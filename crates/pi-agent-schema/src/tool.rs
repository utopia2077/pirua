//! Callback-free Agent tool DTOs and closed execution discriminants.

use pi_ai_core::{ConstrainedSampling, InputContent, JsonObject, JsonSchema, ToolName, Usage};
use serde::{Deserialize, Serialize};

use crate::serde_support::deserialize_optional_non_null;

/// Queue draining behavior.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum QueueMode {
    /// Drain all eligible queued messages.
    #[serde(rename = "all")]
    All,
    /// Drain only the oldest eligible message.
    #[serde(rename = "one-at-a-time")]
    OneAtATime,
}

/// Tool execution behavior for one batch.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ToolExecutionMode {
    /// Execute and settle calls one by one.
    #[serde(rename = "sequential")]
    Sequential,
    /// Permit effect execution in parallel while settling in source order.
    #[serde(rename = "parallel")]
    Parallel,
}

/// Captured tool replay policy.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum ToolReplay {
    /// Never replay an effect whose outcome is unknown.
    #[default]
    #[serde(rename = "never")]
    Never,
    /// Replay when both captured and current declarations say it is safe.
    #[serde(rename = "safe")]
    Safe,
}

/// Callback-free Agent tool declaration used at schema and registry boundaries.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentTool {
    /// Stable model-visible tool name.
    pub name: ToolName,
    /// Human-readable display label.
    pub label: String,
    /// Model-visible description.
    pub description: String,
    /// Object-only parameter schema.
    pub parameters: JsonSchema,
    /// Optional provider-side constrained-sampling declaration.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub constrained_sampling: Option<ConstrainedSampling>,
    /// Optional per-tool execution mode override.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub execution_mode: Option<ToolExecutionMode>,
    /// Optional recovery replay declaration. Omission remains distinct from `never`.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub replay: Option<ToolReplay>,
}

/// Final or partial callback-free Agent tool result.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentToolResult {
    /// Text and image content returned to the model.
    pub content: Vec<InputContent>,
    /// Arbitrary deterministic details, including JSON null.
    pub details: pi_ai_core::JsonValue,
    /// Optional tool-reported usage.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub usage: Option<Usage>,
    /// Tools introduced by this result.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub added_tool_names: Option<Vec<ToolName>>,
    /// Optional early-termination hint.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub terminate: Option<bool>,
}

/// Effective validated arguments for one tool call.
pub type AgentToolArguments = JsonObject;

/// Partial tool progress uses the same callback-free result shape.
pub type AgentToolUpdate = AgentToolResult;

/// A provider-returned Agent tool-call content block.
pub type AgentToolCall = pi_ai_core::ToolCall;
