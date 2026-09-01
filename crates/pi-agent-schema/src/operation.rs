//! Durable operation acceptance metadata, errors, and terminal lane results.

use pi_ai_core::{JsonObject, JsonValue, Presence, SafeUnsigned};
use serde::{Deserialize, Serialize};

use crate::id::{EntryId, LaneId, OperationErrorCode, OperationId};
use crate::serde_support::{deserialize_optional_non_null, deserialize_required_nullable};

/// Closed durable operation kind.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum OperationKind {
    /// A prompt or resume run.
    #[serde(rename = "run")]
    Run,
    /// Standalone compaction.
    #[serde(rename = "compaction")]
    Compaction,
    /// Tree navigation.
    #[serde(rename = "navigation")]
    Navigation,
}

/// Literal `run` intent kind.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum RunIntentKind {
    /// The `run` value.
    #[default]
    #[serde(rename = "run")]
    Run,
}

/// Accepted run intent.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RunIntent {
    /// Literal intent kind.
    pub kind: RunIntentKind,
    /// Prompt entries born placed during acceptance.
    pub prompt_entry_ids: Vec<EntryId>,
    /// Optional system-prompt override.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub system_prompt_override: Option<String>,
    /// Optional deterministic resume data.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub resume_data: Option<JsonObject>,
}

/// Literal `compaction` intent kind.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum CompactionIntentKind {
    /// The `compaction` value.
    #[default]
    #[serde(rename = "compaction")]
    Compaction,
}

/// Accepted standalone compaction intent.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CompactionIntent {
    /// Literal intent kind.
    pub kind: CompactionIntentKind,
    /// Optional caller instructions.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub custom_instructions: Option<String>,
}

/// Literal `navigation` intent kind.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum NavigationIntentKind {
    /// The `navigation` value.
    #[default]
    #[serde(rename = "navigation")]
    Navigation,
}

/// Accepted navigation intent with independent target and summarize fields.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NavigationIntent {
    /// Literal intent kind.
    pub kind: NavigationIntentKind,
    /// Target entry, or root when null.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub target_id: Option<EntryId>,
    /// Whether admission requested a branch summary.
    pub summarize: bool,
    /// Optional target label.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub label: Option<String>,
    /// Optional summary instructions.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub custom_instructions: Option<String>,
}

/// Closed union of accepted operation intents.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum OperationIntent {
    /// Run intent.
    Run(RunIntent),
    /// Standalone compaction intent.
    Compaction(CompactionIntent),
    /// Navigation intent.
    Navigation(NavigationIntent),
}

/// Acceptance metadata written once for an operation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Operation {
    /// Operation identity.
    pub operation_id: OperationId,
    /// Owning lane.
    pub lane: LaneId,
    /// Lane leaf before acceptance, or root when null.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub source_leaf_id: Option<EntryId>,
    /// Acceptance timestamp in Unix milliseconds.
    pub started_at: SafeUnsigned,
    /// Captured operation intent.
    pub intent: OperationIntent,
}

/// Durable expected operation failure.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OperationError {
    /// Open error code.
    pub code: OperationErrorCode,
    /// Human-readable error message.
    pub message: String,
    /// Optional detached details, preserving missing versus null.
    #[serde(default, skip_serializing_if = "Presence::is_missing")]
    pub details: Presence<JsonValue>,
}

/// Total mutable lane operation state.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LaneState {
    /// Current operation, or null when the lane is idle.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub current_operation_id: Option<OperationId>,
    /// Lane-owned queued entry identities for the next run.
    pub pending_next_run: Vec<EntryId>,
}

macro_rules! outcome_literal {
    ($name:ident, $variant:ident, $wire:literal, $doc:literal) => {
        #[doc = $doc]
        #[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
        pub enum $name {
            /// The sole literal value.
            #[default]
            #[serde(rename = $wire)]
            $variant,
        }
    };
}

outcome_literal!(FailedOutcome, Failed, "failed", "Literal failed outcome.");
outcome_literal!(
    CompletedOutcome,
    Completed,
    "completed",
    "Literal completed outcome."
);
outcome_literal!(
    DeclinedOutcome,
    Declined,
    "declined",
    "Literal declined outcome."
);
outcome_literal!(
    AbortedOutcome,
    Aborted,
    "aborted",
    "Literal aborted outcome."
);
outcome_literal!(RunResultKind, Run, "run", "Literal run operation kind.");
outcome_literal!(
    CompactionResultKind,
    Compaction,
    "compaction",
    "Literal compaction operation kind."
);
outcome_literal!(
    NavigationResultKind,
    Navigation,
    "navigation",
    "Literal navigation operation kind."
);
outcome_literal!(
    AssistantRunCompletion,
    Assistant,
    "assistant",
    "Literal assistant run completion."
);
outcome_literal!(
    TerminatedToolsRunCompletion,
    TerminatedTools,
    "terminated_tools",
    "Literal terminated-tools run completion."
);

/// Failed run result with an optional newest assistant entry.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RunFailedLastResult {
    /// Operation identity.
    pub operation_id: OperationId,
    /// Operation kind.
    pub kind: RunResultKind,
    /// Terminal lane leaf, or root when null.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub leaf_id: Option<EntryId>,
    /// Newest settled assistant for a run, when any.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub final_assistant_entry_id: Option<EntryId>,
    /// Literal failed outcome.
    pub outcome: FailedOutcome,
    /// Terminal failure.
    pub error: OperationError,
}

/// Completed run result with a required final assistant entry.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RunCompletedAssistantLastResult {
    /// Operation identity.
    pub operation_id: OperationId,
    /// Operation kind.
    pub kind: RunResultKind,
    /// Terminal lane leaf, or root when null.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub leaf_id: Option<EntryId>,
    /// Required final assistant entry.
    pub final_assistant_entry_id: EntryId,
    /// Literal completed outcome.
    pub outcome: CompletedOutcome,
    /// Literal assistant run completion.
    pub run_completion: AssistantRunCompletion,
}

/// Completed run result terminated by tools, with no final assistant field.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RunCompletedTerminatedToolsLastResult {
    /// Operation identity.
    pub operation_id: OperationId,
    /// Operation kind.
    pub kind: RunResultKind,
    /// Terminal lane leaf, or root when null.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub leaf_id: Option<EntryId>,
    /// Literal completed outcome.
    pub outcome: CompletedOutcome,
    /// Literal terminated-tools run completion.
    pub run_completion: TerminatedToolsRunCompletion,
}

/// Aborted run result with an optional newest assistant entry.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RunAbortedLastResult {
    /// Operation identity.
    pub operation_id: OperationId,
    /// Operation kind.
    pub kind: RunResultKind,
    /// Terminal lane leaf, or root when null.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub leaf_id: Option<EntryId>,
    /// Newest settled assistant for a run, when any.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub final_assistant_entry_id: Option<EntryId>,
    /// Literal aborted outcome.
    pub outcome: AbortedOutcome,
}

macro_rules! structural_last_result {
    ($name:ident, $kind:ty, $outcome:ty, $doc:literal) => {
        #[doc = $doc]
        #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
        #[serde(rename_all = "camelCase", deny_unknown_fields)]
        pub struct $name {
            /// Operation identity.
            pub operation_id: OperationId,
            /// Structurally fixed operation kind.
            pub kind: $kind,
            /// Terminal lane leaf, or root when null.
            #[serde(deserialize_with = "deserialize_required_nullable")]
            pub leaf_id: Option<EntryId>,
            /// Structurally fixed terminal outcome.
            pub outcome: $outcome,
        }
    };
}

macro_rules! structural_failed_last_result {
    ($name:ident, $kind:ty, $doc:literal) => {
        #[doc = $doc]
        #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
        #[serde(rename_all = "camelCase", deny_unknown_fields)]
        pub struct $name {
            /// Operation identity.
            pub operation_id: OperationId,
            /// Structurally fixed operation kind.
            pub kind: $kind,
            /// Terminal lane leaf, or root when null.
            #[serde(deserialize_with = "deserialize_required_nullable")]
            pub leaf_id: Option<EntryId>,
            /// Literal failed outcome.
            pub outcome: FailedOutcome,
            /// Terminal failure.
            pub error: OperationError,
        }
    };
}

structural_last_result!(
    CompactionCompletedLastResult,
    CompactionResultKind,
    CompletedOutcome,
    "Completed compaction result."
);
structural_failed_last_result!(
    CompactionFailedLastResult,
    CompactionResultKind,
    "Failed compaction result."
);
structural_last_result!(
    CompactionDeclinedLastResult,
    CompactionResultKind,
    DeclinedOutcome,
    "Declined compaction result."
);
structural_last_result!(
    CompactionAbortedLastResult,
    CompactionResultKind,
    AbortedOutcome,
    "Aborted compaction result."
);
structural_last_result!(
    NavigationCompletedLastResult,
    NavigationResultKind,
    CompletedOutcome,
    "Completed navigation result."
);
structural_failed_last_result!(
    NavigationFailedLastResult,
    NavigationResultKind,
    "Failed navigation result."
);
structural_last_result!(
    NavigationDeclinedLastResult,
    NavigationResultKind,
    DeclinedOutcome,
    "Declined navigation result."
);
structural_last_result!(
    NavigationAbortedLastResult,
    NavigationResultKind,
    AbortedOutcome,
    "Aborted navigation result."
);

/// Exactly twelve legal terminal result shapes.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum LaneLastResult {
    /// Run completed with a final assistant entry.
    RunCompletedAssistant(RunCompletedAssistantLastResult),
    /// Run completed through terminating tools.
    RunCompletedTerminatedTools(RunCompletedTerminatedToolsLastResult),
    /// Run failed.
    RunFailed(RunFailedLastResult),
    /// Run aborted.
    RunAborted(RunAbortedLastResult),
    /// Compaction completed.
    CompactionCompleted(CompactionCompletedLastResult),
    /// Compaction failed.
    CompactionFailed(CompactionFailedLastResult),
    /// Compaction declined.
    CompactionDeclined(CompactionDeclinedLastResult),
    /// Compaction aborted.
    CompactionAborted(CompactionAbortedLastResult),
    /// Navigation completed.
    NavigationCompleted(NavigationCompletedLastResult),
    /// Navigation failed.
    NavigationFailed(NavigationFailedLastResult),
    /// Navigation declined.
    NavigationDeclined(NavigationDeclinedLastResult),
    /// Navigation aborted.
    NavigationAborted(NavigationAbortedLastResult),
}
