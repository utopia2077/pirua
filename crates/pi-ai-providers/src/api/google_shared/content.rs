use std::fmt;

use pi_ai_core::{
    AssistantContent, AssistantMessage, ImageContent, InputContent, JsonObject, Message, Model,
    ModelInput, SafeUnsigned, ToolCallId, ToolCallIdNormalizer, TranscriptContext, UserContent,
    transform_messages,
};
use serde::Serialize;

use super::{GoogleSharedError, gemini_major_at_least_three, requires_tool_call_id};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum GoogleRole {
    User,
    Model,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct GoogleContent {
    pub role: GoogleRole,
    pub parts: Vec<GooglePart>,
}

/// Finite SDK Part projection used by the shared converter and thought helpers.
/// Serialize-only transient data; it does not claim all SDK Part alternatives.
#[derive(Clone, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GooglePart {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thought: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thought_signature: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inline_data: Option<GoogleInlineData>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub function_call: Option<GoogleFunctionCall>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub function_response: Option<GoogleFunctionResponse>,
}

#[derive(Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GoogleInlineData {
    pub mime_type: String,
    pub data: String,
}

#[derive(Clone, PartialEq, Serialize)]
pub struct GoogleFunctionCall {
    pub name: String,
    pub args: JsonObject,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
}

#[derive(Clone, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum GoogleResponseValue {
    Output(String),
    Error(String),
}

#[derive(Clone, PartialEq, Serialize)]
pub struct GoogleFunctionResponse {
    pub name: String,
    pub response: GoogleResponseValue,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parts: Option<Vec<GooglePart>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
}

macro_rules! redacted_debug {
    ($($name:ident),+ $(,)?) => {$(
        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(concat!(stringify!($name), "([REDACTED])"))
            }
        }
    )+};
}
redacted_debug!(
    GooglePart,
    GoogleInlineData,
    GoogleFunctionCall,
    GoogleFunctionResponse,
    GoogleResponseValue
);

fn image_part(image: &ImageContent) -> GooglePart {
    GooglePart {
        inline_data: Some(GoogleInlineData {
            mime_type: image.mime_type.clone(),
            data: image.data.clone(),
        }),
        ..Default::default()
    }
}
fn input_part(block: &InputContent) -> GooglePart {
    match block {
        InputContent::Text(text) => GooglePart {
            text: Some(text.text.clone()),
            ..Default::default()
        },
        InputContent::Image(image) => image_part(image),
    }
}

fn blank(value: &str) -> bool {
    value.chars().all(|character| {
        matches!(character,
        '\u{0009}'..='\u{000d}' | ' ' | '\u{00a0}' | '\u{1680}' |
        '\u{2000}'..='\u{200a}' | '\u{2028}' | '\u{2029}' | '\u{202f}' |
        '\u{205f}' | '\u{3000}' | '\u{feff}')
    })
}

fn thought_signature(same_provider_model: bool, signature: Option<&str>) -> Option<String> {
    let signature = signature
        .filter(|value| same_provider_model && !value.is_empty() && value.len() % 4 == 0)?;
    let data = signature.trim_end_matches('=');
    if data.is_empty()
        || signature.len() - data.len() > 2
        || !data
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'+' || byte == b'/')
    {
        return None;
    }
    // Source checks alphabet/padding shape only, not decoded bytes or pad bits.
    Some(signature.to_owned())
}

struct IdNormalizer;
impl ToolCallIdNormalizer for IdNormalizer {
    fn normalize_tool_call_id(
        &mut self,
        id: &ToolCallId,
        model: &Model,
        _: &AssistantMessage,
    ) -> ToolCallId {
        if !requires_tool_call_id(model.id.as_str()) {
            return id.clone();
        }
        let normalized: String = id
            .as_str()
            .encode_utf16()
            .take(64)
            .map(|unit| {
                if unit <= 127 {
                    let ch = unit as u8 as char;
                    if ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-') {
                        return ch;
                    }
                }
                '_'
            })
            .collect();
        // Core IDs are nonempty, and every source UTF-16 unit emits one ASCII unit.
        normalized
            .parse()
            .expect("nonempty ASCII normalized tool-call ID")
    }
}

/// Converts strict core messages after the accepted shared transformation.
/// Synthetic time is explicit, though the SDK projection omits timestamps.
/// Rust UTF-8 strings cannot contain the lone surrogates removed upstream.
pub fn convert_messages(
    model: &Model,
    context: &TranscriptContext,
    synthetic_result_timestamp: SafeUnsigned,
) -> Result<Vec<GoogleContent>, GoogleSharedError> {
    let context = pi_ai_core::collapse_system_messages(context.clone());
    let messages = transform_messages(
        &context.messages,
        model,
        synthetic_result_timestamp,
        Some(&mut IdNormalizer),
    )
    .map_err(GoogleSharedError::MessageTransform)?;
    let mut contents: Vec<GoogleContent> = vec![];
    for message in messages {
        match message {
            Message::System(_) => continue,
            Message::User(message) => {
                let parts = match &message.content {
                    UserContent::Text(text) => vec![GooglePart {
                        text: Some(text.clone()),
                        ..Default::default()
                    }],
                    UserContent::Blocks(blocks) => blocks.iter().map(input_part).collect(),
                };
                if !parts.is_empty() {
                    contents.push(GoogleContent {
                        role: GoogleRole::User,
                        parts,
                    });
                }
            }
            Message::Assistant(message) => {
                // This local check intentionally ignores api. The preceding core
                // transform has already applied its own stricter api-aware replay rule.
                let same = message.provider == model.provider && message.model == model.id;
                let mut parts = vec![];
                for block in &message.content {
                    let part = match block {
                        AssistantContent::Text(text) => {
                            let signature = thought_signature(same, text.text_signature.as_deref());
                            if blank(&text.text) && signature.is_none() {
                                continue;
                            }
                            GooglePart {
                                text: Some(text.text.clone()),
                                thought_signature: signature,
                                ..Default::default()
                            }
                        }
                        AssistantContent::Thinking(thinking) => {
                            let signature =
                                thought_signature(same, thinking.thinking_signature.as_deref());
                            if blank(&thinking.thinking) && signature.is_none() {
                                continue;
                            }
                            GooglePart {
                                text: Some(thinking.thinking.clone()),
                                thought: same.then_some(true),
                                thought_signature: signature,
                                ..Default::default()
                            }
                        }
                        AssistantContent::ToolCall(call) => GooglePart {
                            function_call: Some(GoogleFunctionCall {
                                name: call.name.as_str().to_owned(),
                                args: call.arguments.clone(),
                                id: requires_tool_call_id(model.id.as_str())
                                    .then(|| call.id.as_str().to_owned()),
                            }),
                            thought_signature: thought_signature(
                                same,
                                call.thought_signature.as_deref(),
                            ),
                            ..Default::default()
                        },
                    };
                    parts.push(part);
                }
                if !parts.is_empty() {
                    contents.push(GoogleContent {
                        role: GoogleRole::Model,
                        parts,
                    });
                }
            }
            Message::ToolResult(message) => {
                let text = message
                    .content
                    .iter()
                    .filter_map(|block| match block {
                        InputContent::Text(text) => Some(text.text.as_str()),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                let images: Vec<_> = message
                    .content
                    .iter()
                    .filter_map(|block| match block {
                        InputContent::Image(image) if model.input.contains(&ModelInput::Image) => {
                            Some(image_part(image))
                        }
                        _ => None,
                    })
                    .collect();
                let has_images = !images.is_empty();
                // The executable source returns true for ALL non-Gemini models,
                // even though its adjacent comment mentions a separate Claude turn.
                let multimodal = gemini_major_at_least_three(model.id.as_str()).unwrap_or(true);
                let value = if !text.is_empty() {
                    text
                } else if has_images {
                    "(see attached image)".to_owned()
                } else {
                    String::new()
                };
                let part = GooglePart {
                    function_response: Some(GoogleFunctionResponse {
                        name: message.tool_name.as_str().to_owned(),
                        response: if message.is_error {
                            GoogleResponseValue::Error(value)
                        } else {
                            GoogleResponseValue::Output(value)
                        },
                        parts: (has_images && multimodal).then(|| images.clone()),
                        id: requires_tool_call_id(model.id.as_str())
                            .then(|| message.tool_call_id.as_str().to_owned()),
                    }),
                    ..Default::default()
                };
                if let Some(last) = contents.last_mut()
                    && last.role == GoogleRole::User
                    && last
                        .parts
                        .iter()
                        .any(|part| part.function_response.is_some())
                {
                    last.parts.push(part);
                } else {
                    contents.push(GoogleContent {
                        role: GoogleRole::User,
                        parts: vec![part],
                    });
                }
                if has_images && !multimodal {
                    let mut parts = vec![GooglePart {
                        text: Some("Tool result image:".to_owned()),
                        ..Default::default()
                    }];
                    parts.extend(images);
                    contents.push(GoogleContent {
                        role: GoogleRole::User,
                        parts,
                    });
                }
            }
        }
    }
    Ok(contents)
}
