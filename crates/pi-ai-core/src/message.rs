//! Provider-neutral message and deferred-response DTOs.

use serde::{Deserialize, Serialize};

use crate::content::{AssistantContent, InputContent};
use crate::diagnostic::AssistantMessageDiagnostic;
use crate::id::{
    ApiId, CustomRole, DeferredId, ModelId, ProviderId, RawStopReason, ResponseId, ToolCallId,
    ToolName,
};
use crate::json::{JsonValue, Presence, SafeUnsigned};
use crate::usage::Usage;
use crate::{ModelThinkingLevel, Tool, ToolName as DeclaredToolName};

/// Text-only system instruction content.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SystemContent {
    /// Plain instruction text.
    Text(String),
    /// Text content blocks.
    Blocks(Vec<crate::TextContent>),
}

/// Literal system role.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum SystemRole {
    /// The system role.
    #[default]
    #[serde(rename = "system")]
    System,
}

/// A named tool removed by a system-message delta.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolReference {
    /// Tool name.
    pub name: DeclaredToolName,
}

/// Named prompt sections, retaining insertion order across serialization.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SystemSections(Vec<(String, Option<String>)>);
impl SystemSections {
    /// Builds sections in object-property order. Duplicate names replace their value.
    pub fn new(entries: Vec<(String, Option<String>)>) -> Self {
        let mut result: Vec<(String, Option<String>)> = Vec::new();
        for (name, value) in entries {
            if let Some((_, old)) = result.iter_mut().find(|(key, _)| key == &name) {
                *old = value;
            } else {
                result.push((name, value));
            }
        }
        result.sort_by_key(|(name, _)| {
            name.parse::<u32>()
                .ok()
                .filter(|index| *index != u32::MAX && index.to_string() == *name)
                .map_or((1, 0), |index| (0, index))
        });
        Self(result)
    }
    /// Iterates in JavaScript object-property order (index keys, then declarations).
    pub fn iter(&self) -> impl Iterator<Item = &(String, Option<String>)> {
        self.0.iter()
    }
}
impl Serialize for SystemSections {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (key, value) in &self.0 {
            map.serialize_entry(key, value)?;
        }
        map.end()
    }
}
impl<'de> Deserialize<'de> for SystemSections {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = SystemSections;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("ordered string or null sections")
            }
            fn visit_map<M: serde::de::MapAccess<'de>>(
                self,
                mut map: M,
            ) -> Result<Self::Value, M::Error> {
                let mut entries = Vec::new();
                while let Some(entry) = map.next_entry::<String, Option<String>>()? {
                    entries.push(entry);
                }
                Ok(SystemSections::new(entries))
            }
        }
        deserializer.deserialize_map(Visitor)
    }
}

/// Instructions and tool declarations anchored at one transcript position.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SystemMessage {
    /// Literal system role.
    pub role: SystemRole,
    /// Base or additional instructions.
    pub content: SystemContent,
    /// Named sections in declaration order; null removes a section.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub sections: Option<SystemSections>,
    /// Full declarations added or replaced at this position.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub tools_added: Option<Vec<Tool>>,
    /// Tool names removed at this position.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub tools_removed: Option<Vec<ToolReference>>,
    /// Unix timestamp in milliseconds.
    pub timestamp: SafeUnsigned,
}

/// One tool call made during another tool execution.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NestedToolCallRecord {
    /// Invocation-local identity.
    pub id: String,
    /// Target tool name.
    pub name: String,
    /// Detached arguments, omitted when over the recording limit.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub arguments: Option<crate::JsonObject>,
    /// UTF-8 JSON size when arguments are omitted.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub arguments_bytes: Option<SafeUnsigned>,
    /// Outcome of the nested call.
    pub status: NestedToolCallStatus,
    /// Monotonic execution duration.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub duration_ms: Option<SafeUnsigned>,
    /// Bounded error text.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub error: Option<String>,
}

/// Nested call execution outcome.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NestedToolCallStatus {
    /// Successful call.
    Ok,
    /// Failed call.
    Error,
    /// Still running when the parent finished.
    Unfinished,
}

/// Bounded nested call transcript; results are not recorded.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NestedToolCalls {
    /// Recorded calls.
    pub calls: Vec<NestedToolCallRecord>,
    /// False when information was omitted or calls did not finish.
    pub complete: bool,
}

/// Closed reason for an assistant response to stop.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum StopReason {
    /// The response has not settled.
    #[serde(rename = "pending")]
    Pending,
    /// The model completed normally.
    #[serde(rename = "stop")]
    Stop,
    /// The response reached a length limit.
    #[serde(rename = "length")]
    Length,
    /// The model requested tool use.
    #[serde(rename = "toolUse")]
    ToolUse,
    /// Provider or runtime failure.
    #[serde(rename = "error")]
    Error,
    /// Caller-owned cancellation.
    #[serde(rename = "aborted")]
    Aborted,
    /// The provider returned a durable deferred handle.
    #[serde(rename = "deferred")]
    Deferred,
}

/// Provider data required to poll or cancel a deferred response.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DeferredHandle {
    /// Provider that owns the deferred response.
    pub provider: ProviderId,
    /// Requested model identity.
    pub model_id: ModelId,
    /// API protocol that created the handle.
    pub api: ApiId,
    /// Provider-native deferred token.
    pub id: DeferredId,
    /// Optional Unix expiry timestamp in milliseconds.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub expires_at: Option<SafeUnsigned>,
    /// Optional suggested delay before polling, in milliseconds.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub poll_after_ms: Option<SafeUnsigned>,
    /// Optional provider conversion data, preserving missing versus null.
    #[serde(default, skip_serializing_if = "Presence::is_missing")]
    pub data: Presence<JsonValue>,
}

/// User-message content represented as text or a content-block array.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum UserContent {
    /// Plain text content.
    Text(String),
    /// Text and image blocks.
    Blocks(Vec<InputContent>),
}

/// Literal user role.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum UserRole {
    /// The `user` role.
    #[default]
    #[serde(rename = "user")]
    User,
}

/// A user message.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UserMessage {
    /// Literal `user` role.
    pub role: UserRole,
    /// User content.
    pub content: UserContent,
    /// Unix timestamp in milliseconds.
    pub timestamp: SafeUnsigned,
}

/// Literal assistant role.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum AssistantRole {
    /// The `assistant` role.
    #[default]
    #[serde(rename = "assistant")]
    Assistant,
}

/// A complete or partial assistant message.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AssistantMessage {
    /// Literal `assistant` role.
    pub role: AssistantRole,
    /// Ordered assistant content blocks.
    pub content: Vec<AssistantContent>,
    /// API protocol used for the request.
    pub api: ApiId,
    /// Provider that handled the request.
    pub provider: ProviderId,
    /// Requested model identity.
    pub model: ModelId,
    /// Concrete response model when it differs from the requested model.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub response_model: Option<ModelId>,
    /// Provider response or message identity.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub response_id: Option<ResponseId>,
    /// Exact provider-native thinking effort used for this response.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub provider_thinking_level: Option<String>,
    /// Pi thinking level requested by the agent loop.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub thinking_level: Option<ModelThinkingLevel>,
    /// Monotonic duration of a response timed by its originating stream.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub duration_ms: Option<SafeUnsigned>,
    /// Redacted provider and runtime diagnostics.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub diagnostics: Option<Vec<AssistantMessageDiagnostic>>,
    /// Provider-reported usage.
    pub usage: Usage,
    /// Current or terminal stop reason.
    pub stop_reason: StopReason,
    /// Deferred response handle when the response continues asynchronously.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub deferred: Option<DeferredHandle>,
    /// Human-readable failure detail.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub error_message: Option<String>,
    /// Open provider-native stop reason.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub raw_stop_reason: Option<RawStopReason>,
    /// Provider indication that the model explicitly ended its turn.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub end_turn: Option<bool>,
    /// Unix timestamp in milliseconds.
    pub timestamp: SafeUnsigned,
}

/// Literal tool-result role.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum ToolResultRole {
    /// The `toolResult` role.
    #[default]
    #[serde(rename = "toolResult")]
    ToolResult,
}

/// A tool result returned to the model.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(
    rename_all = "camelCase",
    deny_unknown_fields,
    bound(serialize = "D: Serialize", deserialize = "D: Deserialize<'de>")
)]
pub struct ToolResultMessage<D = JsonValue> {
    /// Literal `toolResult` role.
    pub role: ToolResultRole,
    /// Tool call this result settles.
    pub tool_call_id: ToolCallId,
    /// Tool name copied from the settled call.
    pub tool_name: ToolName,
    /// Text and image content returned to the model.
    pub content: Vec<InputContent>,
    /// Optional application details, preserving missing versus null.
    #[serde(default, skip_serializing_if = "Presence::is_missing")]
    pub details: Presence<D>,
    /// Usage from the tool execution itself.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub usage: Option<Usage>,
    /// Calls this tool made to other tools; not sent to the model.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub nested_calls: Option<NestedToolCalls>,
    /// Monotonic tool execution duration.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub duration_ms: Option<SafeUnsigned>,
    /// Whether execution failed.
    pub is_error: bool,
    /// Unix timestamp in milliseconds.
    pub timestamp: SafeUnsigned,
}

/// Closed union of built-in provider messages.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Message {
    /// System instructions and tool state delta.
    System(SystemMessage),
    /// User message.
    User(UserMessage),
    /// Assistant message.
    Assistant(Box<AssistantMessage>),
    /// Tool result message with detached JSON details.
    ToolResult(Box<ToolResultMessage>),
}

/// Application message payload carried under a registered non-built-in role.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CustomMessage {
    /// Registered application role.
    pub role: CustomRole,
    /// Detached JSON payload validated by the owning registry.
    pub value: JsonValue,
}
