//! Compatibility decoding for historical or untyped message values.

use std::fmt;

use pi_ai_core::{Message, SafeUnsigned};
use serde_json::Value;

/// Bounded failure decoding one lax message value.
///
/// ```compile_fail
/// use pi_ai::LaxMessageDecodeError;
///
/// fn cannot_serialize(error: &LaxMessageDecodeError) {
///     let _ = serde_json::to_value(error);
/// }
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct LaxMessageDecodeError {
    index: SafeUnsigned,
}

impl LaxMessageDecodeError {
    /// Returns the zero-based index of the rejected message.
    pub const fn index(&self) -> SafeUnsigned {
        self.index
    }

    fn at(index: usize) -> Self {
        let index = u64::try_from(index)
            .ok()
            .and_then(|value| SafeUnsigned::new(value).ok())
            .unwrap_or(SafeUnsigned::MAX);
        Self { index }
    }
}

impl fmt::Display for LaxMessageDecodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("invalid lax message")
    }
}

impl std::error::Error for LaxMessageDecodeError {}

/// Repairs missing or null content for built-in message roles, then strictly
/// decodes every message without mutating the raw inputs.
pub fn decode_lax_messages(messages: &[Value]) -> Result<Vec<Message>, LaxMessageDecodeError> {
    messages
        .iter()
        .enumerate()
        .map(|(index, source)| {
            let mut value = source.clone();
            if let Value::Object(object) = &mut value
                && object
                    .get("role")
                    .and_then(Value::as_str)
                    .is_some_and(|role| matches!(role, "user" | "assistant" | "toolResult"))
                && object.get("content").is_none_or(Value::is_null)
            {
                object.insert("content".to_owned(), Value::Array(Vec::new()));
            }
            serde_json::from_value(value).map_err(|_| LaxMessageDecodeError::at(index))
        })
        .collect()
}
