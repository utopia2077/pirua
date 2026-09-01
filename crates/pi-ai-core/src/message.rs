//! Provider-neutral message and deferred-response DTOs.

use serde::{Deserialize, Serialize};

use crate::content::{AssistantContent, InputContent};
use crate::diagnostic::AssistantMessageDiagnostic;
use crate::id::{
    ApiId, CustomRole, DeferredId, ImagesApiId, ImagesProviderId, ModelId, ProviderId,
    RawStopReason, ResponseId, ToolCallId, ToolName,
};
use crate::json::{JsonValue, Presence, SafeUnsigned};
use crate::usage::Usage;

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
    /// Tools that become available after this result.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub added_tool_names: Option<Vec<ToolName>>,
    /// Whether execution failed.
    pub is_error: bool,
    /// Unix timestamp in milliseconds.
    pub timestamp: SafeUnsigned,
}

/// Closed union of built-in provider messages.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Message {
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

/// Closed terminal reason for image generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ImagesStopReason {
    /// Normal completion.
    #[serde(rename = "stop")]
    Stop,
    /// Provider or runtime failure.
    #[serde(rename = "error")]
    Error,
    /// Caller-owned cancellation.
    #[serde(rename = "aborted")]
    Aborted,
}

/// Complete image-generation result.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AssistantImages {
    /// Image API protocol.
    pub api: ImagesApiId,
    /// Image provider identity.
    pub provider: ImagesProviderId,
    /// Requested model identity.
    pub model: ModelId,
    /// Text and image output blocks.
    pub output: Vec<InputContent>,
    /// Provider response identity.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub response_id: Option<ResponseId>,
    /// Optional reported usage.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub usage: Option<Usage>,
    /// Terminal image-generation reason.
    pub stop_reason: ImagesStopReason,
    /// Human-readable failure detail.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub error_message: Option<String>,
    /// Unix timestamp in milliseconds.
    pub timestamp: SafeUnsigned,
}
