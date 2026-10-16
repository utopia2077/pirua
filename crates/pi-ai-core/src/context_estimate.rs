//! Deterministic context token estimation.

use serde_json::Value as SerdeValue;

use crate::{
    AssistantContent, Context, InputContent, JsonNumber, JsonObject, JsonValue, Message,
    NumericError, SafeUnsigned, StopReason, Tool, Usage, UserContent,
};

const CHARS_PER_TOKEN: u64 = 4;
const ESTIMATED_IMAGE_CHARS: u64 = 4_800;
const UNSERIALIZABLE_CHARS: u64 = 16;

/// Decomposition of one estimated context size.
///
/// ```compile_fail
/// fn requires_serialize<T: serde::Serialize>() {}
/// requires_serialize::<pi_ai_core::ContextUsageEstimate>();
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContextUsageEstimate {
    /// Estimated total context tokens.
    pub tokens: SafeUnsigned,
    /// Tokens reported by the selected assistant checkpoint.
    pub usage_tokens: SafeUnsigned,
    /// Estimated tokens after the selected checkpoint.
    pub trailing_tokens: SafeUnsigned,
    /// Selected assistant-message index, when one exists.
    pub last_usage_index: Option<SafeUnsigned>,
}

/// Borrowed text or input-block content for token estimation.
pub enum TextAndImageContentRef<'a> {
    /// Plain text.
    Text(&'a str),
    /// Text and image blocks.
    Blocks(&'a [InputContent]),
}

/// Borrowed context or message-only input for context estimation.
pub enum ContextEstimateInput<'a> {
    /// Full request context.
    Context(&'a Context),
    /// Message-only transcript.
    Messages(&'a [Message]),
}

/// Returns provider usage tokens, falling back to the four canonical components.
pub fn calculate_context_tokens(usage: &Usage) -> Result<SafeUnsigned, NumericError> {
    if usage.total_tokens.get() != 0 {
        return Ok(usage.total_tokens);
    }
    safe_sum([
        usage.input.get(),
        usage.output.get(),
        usage.cache_read.get(),
        usage.cache_write.get(),
    ])
}

/// Estimates text tokens from JavaScript UTF-16 code-unit length.
pub fn estimate_text_tokens(text: &str) -> Result<SafeUnsigned, NumericError> {
    chars_to_tokens(utf16_len(text)?)
}

/// Estimates combined text/image content after summing its character units.
pub fn estimate_text_and_image_content_tokens(
    content: TextAndImageContentRef<'_>,
) -> Result<SafeUnsigned, NumericError> {
    let chars = match content {
        TextAndImageContentRef::Text(text) => utf16_len(text)?,
        TextAndImageContentRef::Blocks(blocks) => {
            let mut chars = 0_u64;
            for block in blocks {
                let block_chars = match block {
                    InputContent::Text(text) => utf16_len(&text.text)?,
                    InputContent::Image(_) => ESTIMATED_IMAGE_CHARS,
                };
                chars = chars
                    .checked_add(block_chars)
                    .ok_or_else(numeric_overflow)?;
            }
            chars
        }
    };
    chars_to_tokens(chars)
}

/// Estimates one message using its role-specific content rules.
pub fn estimate_message_tokens(message: &Message) -> Result<SafeUnsigned, NumericError> {
    match message {
        Message::System(message) => {
            let prompt = estimate_text_tokens(&crate::get_system_message_text(message))?;
            let tools =
                estimate_tools_tokens(&message.tools_added.iter().flatten().collect::<Vec<_>>())?;
            safe_sum([prompt.get(), tools.get()])
        }
        Message::User(message) => match &message.content {
            UserContent::Text(text) => {
                estimate_text_and_image_content_tokens(TextAndImageContentRef::Text(text))
            }
            UserContent::Blocks(blocks) => {
                estimate_text_and_image_content_tokens(TextAndImageContentRef::Blocks(blocks))
            }
        },
        Message::ToolResult(message) => {
            estimate_text_and_image_content_tokens(TextAndImageContentRef::Blocks(&message.content))
        }
        Message::Assistant(message) => {
            let mut chars = 0_u64;
            for block in &message.content {
                let block_chars = match block {
                    AssistantContent::Text(text) => utf16_len(&text.text)?,
                    AssistantContent::Thinking(thinking) => utf16_len(&thinking.thinking)?,
                    AssistantContent::ToolCall(call) => utf16_len(call.name.as_str())?
                        .checked_add(json_object_utf16_len(&call.arguments)?)
                        .ok_or_else(numeric_overflow)?,
                };
                chars = chars
                    .checked_add(block_chars)
                    .ok_or_else(numeric_overflow)?;
            }
            chars_to_tokens(chars)
        }
    }
}

/// Estimates a message slice or full context with checkpoint reuse.
pub fn estimate_context_tokens(
    input: ContextEstimateInput<'_>,
) -> Result<ContextUsageEstimate, NumericError> {
    match input {
        ContextEstimateInput::Messages(messages) => estimate_messages(messages),
        ContextEstimateInput::Context(context) => {
            let mut estimate = estimate_messages(&context.messages)?;
            let prefix = if estimate.last_usage_index.is_some() {
                safe_zero()
            } else {
                let system = context
                    .system_prompt
                    .as_deref()
                    .map(estimate_text_tokens)
                    .transpose()?
                    .unwrap_or(safe_zero());
                let tools = estimate_tools_tokens(
                    &context
                        .tools
                        .as_deref()
                        .unwrap_or_default()
                        .iter()
                        .collect::<Vec<_>>(),
                )?;
                safe_sum([system.get(), tools.get()])?
            };
            estimate.tokens = safe_sum([estimate.tokens.get(), prefix.get()])?;
            estimate.trailing_tokens = safe_sum([estimate.trailing_tokens.get(), prefix.get()])?;
            Ok(estimate)
        }
    }
}

fn estimate_messages(messages: &[Message]) -> Result<ContextUsageEstimate, NumericError> {
    let usage_info = last_assistant_usage(messages)?;
    if let Some((usage, index)) = usage_info {
        let usage_tokens = calculate_context_tokens(usage)?;
        let trailing_tokens = sum_message_tokens(&messages[index + 1..])?;
        return Ok(ContextUsageEstimate {
            tokens: safe_sum([usage_tokens.get(), trailing_tokens.get()])?,
            usage_tokens,
            trailing_tokens,
            last_usage_index: Some(SafeUnsigned::new(index as u64)?),
        });
    }
    let tokens = sum_message_tokens(messages)?;
    Ok(ContextUsageEstimate {
        tokens,
        usage_tokens: safe_zero(),
        trailing_tokens: tokens,
        last_usage_index: None,
    })
}

fn last_assistant_usage(messages: &[Message]) -> Result<Option<(&Usage, usize)>, NumericError> {
    let mut latest_prefix_timestamp = None::<SafeUnsigned>;
    let mut usage_info = None;
    for (index, message) in messages.iter().enumerate() {
        if let Message::Assistant(assistant) = message {
            let applies =
                latest_prefix_timestamp.is_none_or(|timestamp| assistant.timestamp >= timestamp);
            if applies
                && !matches!(
                    assistant.stop_reason,
                    StopReason::Aborted | StopReason::Error
                )
                && calculate_context_tokens(&assistant.usage)?.get() > 0
            {
                usage_info = Some((&assistant.usage, index));
            }
        }
        let timestamp = message_timestamp(message);
        latest_prefix_timestamp =
            Some(latest_prefix_timestamp.map_or(timestamp, |latest| latest.max(timestamp)));
    }
    Ok(usage_info)
}

fn message_timestamp(message: &Message) -> SafeUnsigned {
    match message {
        Message::System(message) => message.timestamp,
        Message::User(message) => message.timestamp,
        Message::Assistant(message) => message.timestamp,
        Message::ToolResult(message) => message.timestamp,
    }
}

fn sum_message_tokens(messages: &[Message]) -> Result<SafeUnsigned, NumericError> {
    let mut total = 0_u64;
    for message in messages {
        total = total
            .checked_add(estimate_message_tokens(message)?.get())
            .ok_or_else(numeric_overflow)?;
    }
    SafeUnsigned::new(total)
}

fn estimate_tools_tokens(tools: &[&Tool]) -> Result<SafeUnsigned, NumericError> {
    if tools.is_empty() {
        return Ok(safe_zero());
    }
    let chars = match serde_json::to_value(tools) {
        Ok(value) => serde_json_utf16_len(&value)?,
        Err(_) => UNSERIALIZABLE_CHARS,
    };
    chars_to_tokens(chars)
}

fn json_object_utf16_len(object: &JsonObject) -> Result<u64, NumericError> {
    json_value_utf16_len(&JsonValue::Object(object.clone()))
}

fn json_value_utf16_len(value: &JsonValue) -> Result<u64, NumericError> {
    match value {
        JsonValue::Null => Ok(4),
        JsonValue::Bool(true) => Ok(4),
        JsonValue::Bool(false) => Ok(5),
        JsonValue::Number(JsonNumber::Unsigned(value)) => decimal_len(value.get()),
        JsonValue::Number(JsonNumber::Signed(value)) => Ok(value.get().to_string().len() as u64),
        JsonValue::Number(JsonNumber::Float(value)) => {
            Ok(ryu_js::Buffer::new().format_finite(value.get()).len() as u64)
        }
        JsonValue::String(value) => json_string_utf16_len(value),
        JsonValue::Array(values) => {
            let mut length = 2_u64;
            for (index, value) in values.iter().enumerate() {
                if index != 0 {
                    length = length.checked_add(1).ok_or_else(numeric_overflow)?;
                }
                length = length
                    .checked_add(json_value_utf16_len(value)?)
                    .ok_or_else(numeric_overflow)?;
            }
            Ok(length)
        }
        JsonValue::Object(values) => {
            let mut length = 2_u64;
            for (index, (key, value)) in values.as_map().iter().enumerate() {
                if index != 0 {
                    length = length.checked_add(1).ok_or_else(numeric_overflow)?;
                }
                length = length
                    .checked_add(json_string_utf16_len(key)?)
                    .and_then(|length| length.checked_add(1))
                    .and_then(|length| length.checked_add(json_value_utf16_len(value).ok()?))
                    .ok_or_else(numeric_overflow)?;
            }
            Ok(length)
        }
    }
}

fn serde_json_utf16_len(value: &SerdeValue) -> Result<u64, NumericError> {
    match value {
        SerdeValue::Null => Ok(4),
        SerdeValue::Bool(true) => Ok(4),
        SerdeValue::Bool(false) => Ok(5),
        SerdeValue::Number(number) => {
            if let Some(value) = number.as_u64() {
                return decimal_len(value);
            }
            if let Some(value) = number.as_i64() {
                return Ok(value.to_string().len() as u64);
            }
            Ok(ryu_js::Buffer::new()
                .format_finite(number.as_f64().ok_or_else(numeric_overflow)?)
                .len() as u64)
        }
        SerdeValue::String(value) => json_string_utf16_len(value),
        SerdeValue::Array(values) => {
            let mut length = 2_u64;
            for (index, value) in values.iter().enumerate() {
                if index != 0 {
                    length = length.checked_add(1).ok_or_else(numeric_overflow)?;
                }
                length = length
                    .checked_add(serde_json_utf16_len(value)?)
                    .ok_or_else(numeric_overflow)?;
            }
            Ok(length)
        }
        SerdeValue::Object(values) => {
            let mut length = 2_u64;
            for (index, (key, value)) in values.iter().enumerate() {
                if index != 0 {
                    length = length.checked_add(1).ok_or_else(numeric_overflow)?;
                }
                length = length
                    .checked_add(json_string_utf16_len(key)?)
                    .and_then(|length| length.checked_add(1))
                    .and_then(|length| length.checked_add(serde_json_utf16_len(value).ok()?))
                    .ok_or_else(numeric_overflow)?;
            }
            Ok(length)
        }
    }
}

fn json_string_utf16_len(value: &str) -> Result<u64, NumericError> {
    let mut length = 2_u64;
    for character in value.chars() {
        let units = match character {
            '"' | '\\' | '\u{08}' | '\t' | '\n' | '\u{0c}' | '\r' => 2,
            '\u{00}'..='\u{1f}' => 6,
            character => u64::from(character.len_utf16() as u8),
        };
        length = length.checked_add(units).ok_or_else(numeric_overflow)?;
    }
    Ok(length)
}

fn utf16_len(value: &str) -> Result<u64, NumericError> {
    u64::try_from(value.encode_utf16().count()).map_err(|_| numeric_overflow())
}

fn chars_to_tokens(chars: u64) -> Result<SafeUnsigned, NumericError> {
    let rounded = chars
        .checked_add(CHARS_PER_TOKEN - 1)
        .ok_or_else(numeric_overflow)?
        / CHARS_PER_TOKEN;
    SafeUnsigned::new(rounded)
}

fn decimal_len(value: u64) -> Result<u64, NumericError> {
    u64::try_from(value.to_string().len()).map_err(|_| numeric_overflow())
}

fn safe_sum(values: impl IntoIterator<Item = u64>) -> Result<SafeUnsigned, NumericError> {
    let mut total = 0_u64;
    for value in values {
        total = total.checked_add(value).ok_or_else(numeric_overflow)?;
    }
    SafeUnsigned::new(total)
}

fn safe_zero() -> SafeUnsigned {
    SafeUnsigned::new(0).expect("zero is JavaScript-safe")
}

fn numeric_overflow() -> NumericError {
    SafeUnsigned::new(u64::MAX).expect_err("u64::MAX exceeds JavaScript safe range")
}
