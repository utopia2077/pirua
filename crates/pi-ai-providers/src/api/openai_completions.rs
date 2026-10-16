//! Pinned OpenAI 7.19.0 Chat Completions adapter and vendor conversions.

use std::fmt;

use pi_ai_core::{JsonObject, ThinkingBudgets, ThinkingLevel};
use serde::{Deserialize, Serialize};

mod convert;
mod payload;
mod reduce;
mod wire;
pub use convert::{ConvertCompletionsMessagesOptions, ResolvedCompletionsCompat, convert_messages};
pub use wire::OpenAICompletionsApi;

fn optional_non_null<'de, D: serde::Deserializer<'de>, T: Deserialize<'de>>(
    d: D,
) -> Result<Option<T>, D::Error> {
    Option::<T>::deserialize(d)?
        .map(Some)
        .ok_or_else(|| serde::de::Error::custom("expected non-null option"))
}

/// Family fields supplement neutral fields on the nominal resolved request.
#[derive(Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OpenAICompletionsOptions {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_non_null"
    )]
    pub tool_choice: Option<CompletionsToolChoice>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_non_null"
    )]
    pub reasoning_effort: Option<ThinkingLevel>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_non_null"
    )]
    pub thinking_budgets: Option<ThinkingBudgets>,
}
impl fmt::Debug for OpenAICompletionsOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("OpenAICompletionsOptions([REDACTED])")
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CompletionsToolChoiceMode {
    None,
    Auto,
    Required,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CompletionsAllowedMode {
    Auto,
    Required,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompletionsNamedTool {
    pub name: String,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompletionsAllowedTools {
    pub mode: CompletionsAllowedMode,
    pub tools: Vec<JsonObject>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum CompletionsToolSelector {
    Function {
        function: CompletionsNamedTool,
    },
    Custom {
        custom: CompletionsNamedTool,
    },
    AllowedTools {
        allowed_tools: CompletionsAllowedTools,
    },
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum CompletionsToolChoice {
    Mode(CompletionsToolChoiceMode),
    Selector(CompletionsToolSelector),
}

/// Conversion failures contain no transcript, endpoint, schema or provider text.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompletionsError {
    Conversion,
    InvalidWire,
    ResourceLimit,
    MissingFinishReason,
}
impl fmt::Display for CompletionsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Conversion => "Completions conversion failed",
            Self::InvalidWire => "invalid Completions wire value",
            Self::ResourceLimit => "Completions resource limit",
            Self::MissingFinishReason => "Stream ended without finish_reason",
        })
    }
}
impl std::error::Error for CompletionsError {}
