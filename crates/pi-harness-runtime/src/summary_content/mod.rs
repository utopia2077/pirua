//! Pure summary preparation on detached, provenance-bearing v3 projection.
//! Callbacks, persistence, retries and provider execution belong to the owner.
mod preparation;
mod prompt;
mod text;
pub use preparation::*;
pub use prompt::*;
pub use text::*;

use pi_agent_schema::*;
use pi_ai_core::{JsonValue, Presence, SafeUnsigned};
use std::collections::BTreeSet;

/// A checked numeric/input/content failure, with no provider or durable side effects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentError {
    /// A total does not fit the durable JavaScript-safe numeric domain.
    NumberRange,
    /// A detached projection or response count is inconsistent.
    InvalidInput,
    /// A settled provider response failed or was aborted.
    ProviderResponse,
}
impl std::fmt::Display for ContentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "summary content {self:?}")
    }
}
impl std::error::Error for ContentError {}
fn number(n: u64) -> Result<SafeUnsigned, ContentError> {
    SafeUnsigned::new(n).map_err(|_| ContentError::NumberRange)
}
fn add(a: u64, b: u64) -> Result<u64, ContentError> {
    a.checked_add(b)
        .filter(|n| *n <= SafeUnsigned::MAX.get())
        .ok_or(ContentError::NumberRange)
}
fn value(message: &AgentMessage) -> Result<serde_json::Value, ContentError> {
    serde_json::to_value(message).map_err(|_| ContentError::InvalidInput)
}

/// Accumulated file sets, represented as normalized durable sorted arrays.
pub fn file_lists(ops: &DurableFileOperations) -> (Vec<String>, Vec<String>) {
    let modified: BTreeSet<_> = ops.written().iter().chain(ops.edited()).cloned().collect();
    let mut read: Vec<_> = ops
        .read()
        .iter()
        .filter(|p| !modified.contains(*p))
        .cloned()
        .collect();
    let mut modified: Vec<_> = modified.into_iter().collect();
    read.sort_by(|a, b| a.encode_utf16().cmp(b.encode_utf16()));
    modified.sort_by(|a, b| a.encode_utf16().cmp(b.encode_utf16()));
    (read, modified)
}
#[derive(Default)]
struct Files {
    read: BTreeSet<String>,
    written: BTreeSet<String>,
    edited: BTreeSet<String>,
}
impl Files {
    fn extend(&mut self, ops: &DurableFileOperations) {
        self.read.extend(ops.read().iter().cloned());
        self.written.extend(ops.written().iter().cloned());
        self.edited.extend(ops.edited().iter().cloned());
    }
    fn details(&mut self, details: &Presence<JsonValue>) {
        let Presence::Value(JsonValue::Object(details)) = details else {
            return;
        };
        for (key, target) in [
            ("readFiles", &mut self.read),
            ("modifiedFiles", &mut self.edited),
        ] {
            if let Some(JsonValue::Array(values)) = details.as_map().get(key) {
                target.extend(values.iter().filter_map(|v| {
                    if let JsonValue::String(s) = v {
                        Some(s.clone())
                    } else {
                        None
                    }
                }));
            }
        }
    }
    fn message(&mut self, message: &AgentMessage) -> Result<(), ContentError> {
        let v = value(message)?;
        if v["role"] != "assistant" {
            return Ok(());
        }
        for block in v["content"].as_array().into_iter().flatten() {
            if block["type"] != "toolCall" {
                continue;
            }
            if let Some(path) = block["arguments"]["path"]
                .as_str()
                .filter(|p| !p.is_empty())
            {
                match block["name"].as_str() {
                    Some("read") => {
                        self.read.insert(path.into());
                    }
                    Some("write") => {
                        self.written.insert(path.into());
                    }
                    Some("edit") => {
                        self.edited.insert(path.into());
                    }
                    _ => {}
                }
            }
        }
        Ok(())
    }
    fn finish(self) -> Result<DurableFileOperations, ContentError> {
        DurableFileOperations::new(
            self.read.into_iter().collect(),
            self.written.into_iter().collect(),
            self.edited.into_iter().collect(),
        )
        .map_err(|_| ContentError::InvalidInput)
    }
}
