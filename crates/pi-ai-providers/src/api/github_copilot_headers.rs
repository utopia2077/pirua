//! Copilot initiator and image-presence request metadata.

use std::collections::BTreeMap;

use pi_ai_core::{InputContent, Message, UserContent};

/// Determines the initiator from the final message; an empty history is user-led.
pub fn infer_copilot_initiator(messages: &[Message]) -> &'static str {
    match messages.last() {
        None | Some(Message::User(_)) => "user",
        Some(Message::Assistant(_) | Message::ToolResult(_)) => "agent",
    }
}

/// Finds image blocks anywhere in user or tool-result messages.
pub fn has_copilot_vision_input(messages: &[Message]) -> bool {
    messages.iter().any(|message| {
        let blocks = match message {
            Message::User(message) => match &message.content {
                UserContent::Text(_) => return false,
                UserContent::Blocks(blocks) => blocks,
            },
            Message::ToolResult(message) => &message.content,
            Message::Assistant(_) => return false,
        };
        blocks
            .iter()
            .any(|block| matches!(block, InputContent::Image(_)))
    })
}

/// Builds dynamic headers, honoring the caller's explicit image-presence flag.
pub fn build_copilot_dynamic_headers(
    messages: &[Message],
    has_images: bool,
) -> BTreeMap<String, String> {
    let mut headers = BTreeMap::from([
        (
            "X-Initiator".to_owned(),
            infer_copilot_initiator(messages).to_owned(),
        ),
        ("Openai-Intent".to_owned(), "conversation-edits".to_owned()),
    ]);
    if has_images {
        headers.insert("Copilot-Vision-Request".to_owned(), "true".to_owned());
    }
    headers
}
