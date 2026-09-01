//! Prompt resources and structural result DTOs.

use pi_ai_core::{JsonValue, Presence, SafeUnsigned, Usage};
use serde::{Deserialize, Serialize};

use crate::config::{CompactionSettings, DurableFileOperations};
use crate::message::AgentMessage;
use crate::serde_support::deserialize_optional_non_null;

/// Generated compaction data ready for a compaction entry.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CompactResult {
    /// Summary text replacing compacted history in future context.
    pub summary: String,
    /// Estimated context tokens before compaction.
    pub tokens_before: SafeUnsigned,
    /// Recent messages stored directly on the compaction entry.
    pub retained_tail: Vec<AgentMessage>,
    /// Optional provider usage.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub usage: Option<Usage>,
    /// Optional implementation details, preserving explicit null.
    #[serde(default, skip_serializing_if = "Presence::is_missing")]
    pub details: Presence<JsonValue>,
}

/// Generated branch summary data ready for a branch-summary entry.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BranchSummaryResult {
    /// Summary text.
    pub summary: String,
    /// Optional provider usage.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub usage: Option<Usage>,
    /// Files read while exploring the branch.
    pub read_files: Vec<String>,
    /// Files modified while exploring the branch.
    pub modified_files: Vec<String>,
}

/// Prepared compaction inputs exposed to the structural decision hook.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CompactionPreparation {
    /// Messages selected for history summarization.
    pub messages_to_summarize: Vec<AgentMessage>,
    /// Prefix messages summarized separately for a split turn.
    pub turn_prefix_messages: Vec<AgentMessage>,
    /// Recent messages retained directly.
    pub retained_tail: Vec<AgentMessage>,
    /// Whether the compaction splits a turn.
    pub is_split_turn: bool,
    /// Estimated context tokens before compaction.
    pub tokens_before: SafeUnsigned,
    /// Optional previous summary.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub previous_summary: Option<String>,
    /// Normalized file operations.
    pub file_ops: DurableFileOperations,
    /// Settings used for preparation.
    pub settings: CompactionSettings,
}

/// Prepared branch inputs exposed to the navigation decision hook.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BranchPreparation {
    /// Messages selected for summarization.
    pub messages: Vec<AgentMessage>,
    /// Normalized file operations.
    pub file_ops: DurableFileOperations,
    /// Estimated selected-message tokens.
    pub total_tokens: SafeUnsigned,
}

/// A model- or application-invocable skill.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Skill {
    /// Stable lookup name.
    pub name: String,
    /// Short model-visible description.
    pub description: String,
    /// Full skill instructions.
    pub content: String,
    /// Source file path.
    pub file_path: String,
    /// Whether model invocation is disabled.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub disable_model_invocation: Option<bool>,
}

/// A named prompt template.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PromptTemplate {
    /// Stable lookup name.
    pub name: String,
    /// Template content.
    pub content: String,
    /// Optional display description.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub description: Option<String>,
}

/// Harness prompt resources.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Resources {
    /// Available prompt templates.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub prompt_templates: Option<Vec<PromptTemplate>>,
    /// Available skills.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub skills: Option<Vec<Skill>>,
}
