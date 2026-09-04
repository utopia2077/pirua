//! Text extraction from provider content blocks.

use crate::{AssistantContent, InputContent};

/// Borrowed content accepted by text extraction helpers.
pub enum ContentTextInput<'a> {
    /// Plain text, returned unchanged.
    Text(&'a str),
    /// User/tool-result input blocks.
    Input(&'a [InputContent]),
    /// Assistant output blocks.
    Assistant(&'a [AssistantContent]),
}

/// Extracts text blocks and joins them with a newline.
pub fn content_text(content: ContentTextInput<'_>) -> String {
    content_text_with_separator(content, "\n")
}

/// Extracts text blocks in source order and joins them with `separator`.
pub fn content_text_with_separator(content: ContentTextInput<'_>, separator: &str) -> String {
    match content {
        ContentTextInput::Text(text) => text.to_owned(),
        ContentTextInput::Input(blocks) => blocks
            .iter()
            .filter_map(|block| match block {
                InputContent::Text(text) => Some(text.text.as_str()),
                InputContent::Image(_) => None,
            })
            .collect::<Vec<_>>()
            .join(separator),
        ContentTextInput::Assistant(blocks) => blocks
            .iter()
            .filter_map(|block| match block {
                AssistantContent::Text(text) => Some(text.text.as_str()),
                AssistantContent::Thinking(_) | AssistantContent::ToolCall(_) => None,
            })
            .collect::<Vec<_>>()
            .join(separator),
    }
}
