//! Physical storage-version-one operation-state schema.

use pi_ai_core::{LiteralFalse, LiteralTrue, SafeUnsigned};
use serde::{Deserialize, Serialize};

use crate::config::{
    AgentHarnessStreamOptions, LaneConfiguration, NormalizedRetryPolicy,
    ProviderHeaderClassificationError, ProviderHeaderPolicy, RawAgentHarnessStreamOptions,
    RunSettings,
};
use crate::id::{EntryId, OneBasedSafeUnsigned, StepId, TaskId, UsageId};
use crate::operation::OperationError;
use crate::serde_support::{deserialize_optional_non_null, deserialize_required_nullable};
use crate::tool::ToolReplay;

/// Exhaustive source inventory of physical v1 operation-state paths.
pub const OPERATION_STATE_V1_INVENTORY: &[&str] = &[
    "run.checkpoint",
    "run.assistant.ready",
    "run.assistant.effect_pending",
    "run.assistant.retry_wait",
    "run.tools.planned",
    "run.tools.effect_pending",
    "run.tools.completed",
    "run.compaction.deciding",
    "run.compaction.generating.ready",
    "run.compaction.generating.effect_pending.request_missing",
    "run.compaction.generating.effect_pending.request_present",
    "run.compaction.generating.retry_wait",
    "run.deferred.suspended",
    "run.deferred.effect_pending",
    "run.failure_drain.response",
    "run.failure_drain.structural",
    "compaction.deciding",
    "compaction.generating.ready",
    "compaction.generating.effect_pending.request_missing",
    "compaction.generating.effect_pending.request_present",
    "compaction.generating.retry_wait",
    "navigation.unsummarized.ready_to_commit",
    "navigation.summarized.deciding",
    "navigation.summarized.generating.ready",
    "navigation.summarized.generating.effect_pending.request_missing",
    "navigation.summarized.generating.effect_pending.request_present",
    "navigation.summarized.generating.retry_wait",
];

/// Literal running control status.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum RunningControlStatus {
    /// The `running` value.
    #[default]
    #[serde(rename = "running")]
    Running,
}

/// Exact no-payload running control.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunningControl {
    /// Literal running status.
    pub status: RunningControlStatus,
}

/// Literal cancel-requested control status.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum CancelRequestedControlStatus {
    /// The `cancel_requested` value.
    #[default]
    #[serde(rename = "cancel_requested")]
    CancelRequested,
}

/// Exact durable cancellation marker and drained queues.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CancelRequestedControl {
    /// Literal cancel-requested status.
    pub status: CancelRequestedControlStatus,
    /// Cancellation timestamp in Unix milliseconds.
    pub requested_at: SafeUnsigned,
    /// Drained steering entry identities.
    pub drained_steer: Vec<EntryId>,
    /// Drained follow-up entry identities.
    pub drained_follow_up: Vec<EntryId>,
}

/// Common durable cancellation control with exact closed shapes.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Control {
    /// Ordinary no-payload running control.
    Running(RunningControl),
    /// Durable cancel-requested control.
    CancelRequested(CancelRequestedControl),
}

/// Closed checkpoint continuation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Continuation {
    /// Another assistant generation is required.
    NeedAssistant {
        /// Whether the one allowed overflow recovery has already been used.
        #[serde(rename = "overflowRecoveryUsed")]
        overflow_recovery_used: bool,
    },
    /// The run may finish after inbox processing.
    MayFinish {
        /// Whether the newest assistant response is the final response.
        #[serde(rename = "includeFinalAssistant")]
        include_final_assistant: bool,
    },
}

/// Literal checkpoint phase kind.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum CheckpointPhaseKind {
    /// The `checkpoint` value.
    #[default]
    #[serde(rename = "checkpoint")]
    Checkpoint,
}

/// Durable checkpoint phase.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CheckpointPhase {
    /// Literal checkpoint kind.
    pub kind: CheckpointPhaseKind,
    /// Required next continuation.
    pub continuation: Continuation,
    /// Existing entry that triggered this boundary.
    pub trigger_entry_id: EntryId,
    /// Trigger for which threshold checking already occurred.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub threshold_checked_trigger_entry_id: Option<EntryId>,
    /// Durable one-pass inbox skip marker.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub skip_inbox_once: Option<bool>,
}

/// Entry identities queued inside an open run.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Inbox {
    /// Steering input identities.
    pub steer: Vec<EntryId>,
    /// Follow-up input identities.
    pub follow_up: Vec<EntryId>,
    /// Deferred tree-write identities.
    pub writes: Vec<EntryId>,
}

/// Captured assistant-generation context.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GenerationContext<O = AgentHarnessStreamOptions> {
    /// Generation step identity.
    pub step_id: StepId,
    /// Existing entry that triggered this generation.
    pub trigger_entry_id: EntryId,
    /// Inline lane-configuration snapshot.
    pub configuration: LaneConfiguration,
    /// Inline public stream-option snapshot.
    pub stream_options: O,
    /// Inline normalized retry policy.
    pub retry_policy: NormalizedRetryPolicy,
    /// Whether overflow recovery was already used.
    pub overflow_recovery_used: bool,
}

/// Durable assistant-generation state.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum Generation<O = AgentHarnessStreamOptions> {
    /// Ready to run the pre-request hook and reserve an effect intent.
    Ready {
        /// Captured generation context.
        context: GenerationContext<O>,
        /// Next one-based logical attempt.
        #[serde(rename = "nextAttempt")]
        next_attempt: OneBasedSafeUnsigned,
    },
    /// External effect intent committed but not durably settled.
    EffectPending {
        /// Captured generation context.
        context: GenerationContext<O>,
        /// One-based logical attempt.
        attempt: OneBasedSafeUnsigned,
        /// Reserved response entry identity.
        #[serde(rename = "responseEntryId")]
        response_entry_id: EntryId,
        /// Reserved usage-row identity.
        #[serde(rename = "usageId")]
        usage_id: UsageId,
        /// Intended request output limit.
        #[serde(rename = "intendedOutputLimit")]
        intended_output_limit: SafeUnsigned,
        /// Captured model context window.
        #[serde(rename = "contextWindow")]
        context_window: SafeUnsigned,
    },
    /// Durable retry backoff wait.
    RetryWait {
        /// Captured generation context.
        context: GenerationContext<O>,
        /// Next one-based logical attempt.
        #[serde(rename = "nextAttempt")]
        next_attempt: OneBasedSafeUnsigned,
        /// Earliest retry timestamp in Unix milliseconds.
        #[serde(rename = "notBefore")]
        not_before: SafeUnsigned,
        /// Failure message from the prior attempt.
        #[serde(rename = "errorMessage")]
        error_message: String,
    },
}

/// One source-ordered tool call in a durable batch.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum ToolCallState {
    /// Result identity is reserved but no external intent exists.
    Planned {
        /// Index of the assistant content block.
        #[serde(rename = "sourceIndex")]
        source_index: SafeUnsigned,
        /// Reserved result entry identity.
        #[serde(rename = "resultEntryId")]
        result_entry_id: EntryId,
    },
    /// External tool intent committed but not durably settled.
    EffectPending {
        /// Index of the assistant content block.
        #[serde(rename = "sourceIndex")]
        source_index: SafeUnsigned,
        /// Reserved result entry identity.
        #[serde(rename = "resultEntryId")]
        result_entry_id: EntryId,
        /// Captured replay declaration.
        replay: ToolReplay,
    },
    /// Durable result entry committed.
    Completed {
        /// Index of the assistant content block.
        #[serde(rename = "sourceIndex")]
        source_index: SafeUnsigned,
        /// Committed result entry identity.
        #[serde(rename = "resultEntryId")]
        result_entry_id: EntryId,
        /// Whether this result participates in early termination.
        terminate: bool,
    },
}

/// Durable source-ordered tool batch.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ToolBatch {
    /// Assistant message containing source tool calls.
    pub assistant_entry_id: EntryId,
    /// Producing generation or deferred-fetch configuration.
    pub configuration: LaneConfiguration,
    /// Producing assistant step identity used as the turn identity.
    pub turn_id: StepId,
    /// Source-ordered durable call states.
    pub calls: Vec<ToolCallState>,
}

/// Durable deferred-response state.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum Deferred<O = AgentHarnessStreamOptions> {
    /// Suspended between explicit resume polls.
    Suspended {
        /// Original generation step identity.
        #[serde(rename = "stepId")]
        step_id: StepId,
        /// Newest deferred source entry.
        #[serde(rename = "sourceEntryId")]
        source_entry_id: EntryId,
        /// Count of completed polls.
        poll: SafeUnsigned,
        /// Captured lane configuration.
        configuration: LaneConfiguration,
        /// Captured stream options.
        #[serde(rename = "streamOptions")]
        stream_options: O,
    },
    /// Deferred-fetch intent committed but not durably settled.
    EffectPending {
        /// Original generation step identity.
        #[serde(rename = "stepId")]
        step_id: StepId,
        /// Source deferred entry being polled.
        #[serde(rename = "sourceEntryId")]
        source_entry_id: EntryId,
        /// One-based current poll number.
        poll: OneBasedSafeUnsigned,
        /// Reserved fetch response entry.
        #[serde(rename = "responseEntryId")]
        response_entry_id: EntryId,
        /// Reserved usage row.
        #[serde(rename = "usageId")]
        usage_id: UsageId,
        /// Captured lane configuration.
        configuration: LaneConfiguration,
        /// Captured stream options.
        #[serde(rename = "streamOptions")]
        stream_options: O,
    },
}

/// Structural summary kind.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SummaryKind {
    /// Compaction summary.
    #[serde(rename = "compaction")]
    Compaction,
    /// Branch-navigation summary.
    #[serde(rename = "branch_summary")]
    BranchSummary,
}

/// Optional structural summary reason.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SummaryReason {
    /// Manual standalone compaction.
    #[serde(rename = "manual")]
    Manual,
    /// Automatic threshold compaction.
    #[serde(rename = "threshold")]
    Threshold,
    /// Overflow recovery compaction.
    #[serde(rename = "overflow")]
    Overflow,
}

/// Captured structural summary context.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SummaryContext<O = AgentHarnessStreamOptions> {
    /// Structural task identity.
    pub task_id: TaskId,
    /// Reserved final structural entry identity.
    pub result_entry_id: EntryId,
    /// Summary kind.
    pub kind: SummaryKind,
    /// Inline lane-configuration snapshot.
    pub configuration: LaneConfiguration,
    /// Inline public stream-option snapshot.
    pub stream_options: O,
    /// Inline normalized retry policy.
    pub retry_policy: NormalizedRetryPolicy,
    /// Optional compaction reason.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub reason: Option<SummaryReason>,
}

/// Nested provider-request intent inside a structural attempt.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SummaryRequestIntent {
    /// Zero-based nested request index.
    pub index: SafeUnsigned,
    /// Reserved usage-row identity.
    pub usage_id: UsageId,
}

/// Durable structural summary-generation state.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum SummaryGeneration<O = AgentHarnessStreamOptions> {
    /// Ready to begin a numbered structural attempt.
    Ready {
        /// Captured summary context.
        context: SummaryContext<O>,
        /// Next one-based logical attempt.
        #[serde(rename = "nextAttempt")]
        next_attempt: OneBasedSafeUnsigned,
    },
    /// Structural attempt in progress or uncertain.
    EffectPending {
        /// Captured summary context.
        context: SummaryContext<O>,
        /// One-based logical attempt.
        attempt: OneBasedSafeUnsigned,
        /// Current nested request intent. Absence is distinct from a present object.
        #[serde(
            default,
            deserialize_with = "deserialize_optional_non_null",
            skip_serializing_if = "Option::is_none"
        )]
        request: Option<SummaryRequestIntent>,
        /// Usage rows already settled in this attempt.
        #[serde(rename = "usageIds")]
        usage_ids: Vec<UsageId>,
    },
    /// Durable structural retry backoff wait.
    RetryWait {
        /// Captured summary context.
        context: SummaryContext<O>,
        /// Next one-based logical attempt.
        #[serde(rename = "nextAttempt")]
        next_attempt: OneBasedSafeUnsigned,
        /// Earliest retry timestamp in Unix milliseconds.
        #[serde(rename = "notBefore")]
        not_before: SafeUnsigned,
        /// Failure message from the prior attempt.
        #[serde(rename = "errorMessage")]
        error_message: String,
    },
}

/// Durable structural decision and generation program counter.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum StructuralDecision<O = AgentHarnessStreamOptions> {
    /// Decision hook has not durably selected an outcome.
    Deciding {
        /// Structural task identity.
        #[serde(rename = "taskId")]
        task_id: TaskId,
    },
    /// Decision selected summary generation and cannot rerun.
    Generating {
        /// Structural task identity.
        #[serde(rename = "taskId")]
        task_id: TaskId,
        /// Durable summary generation.
        generation: Box<SummaryGeneration<O>>,
    },
}

/// Literal assistant run-phase kind.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum AssistantPhaseKind {
    /// The `assistant` value.
    #[default]
    #[serde(rename = "assistant")]
    Assistant,
}

/// Assistant generation run phase.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AssistantPhase<O = AgentHarnessStreamOptions> {
    /// Literal assistant kind.
    pub kind: AssistantPhaseKind,
    /// Durable generation state.
    pub generation: Generation<O>,
}

/// Literal tools run-phase kind.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum ToolsPhaseKind {
    /// The `tools` value.
    #[default]
    #[serde(rename = "tools")]
    Tools,
}

/// Tool batch run phase.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolsPhase {
    /// Literal tools kind.
    pub kind: ToolsPhaseKind,
    /// Durable batch.
    pub batch: ToolBatch,
}

/// In-run compaction reason.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum RunCompactionReason {
    /// Proactive threshold compaction.
    #[serde(rename = "threshold")]
    Threshold,
    /// Overflow recovery compaction.
    #[serde(rename = "overflow")]
    Overflow,
}

/// Literal compaction run-phase kind.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum RunCompactionPhaseKind {
    /// The `compaction` value.
    #[default]
    #[serde(rename = "compaction")]
    Compaction,
}

/// In-run structural compaction phase.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RunCompactionPhase<O = AgentHarnessStreamOptions> {
    /// Literal compaction phase kind.
    pub kind: RunCompactionPhaseKind,
    /// Threshold or overflow reason.
    pub reason: RunCompactionReason,
    /// Structural decision state.
    pub structural: StructuralDecision<O>,
    /// Marked checkpoint restored after structural completion or decline.
    pub resume_after: CheckpointPhase,
}

/// Literal deferred run-phase kind.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum DeferredPhaseKind {
    /// The `deferred` value.
    #[default]
    #[serde(rename = "deferred")]
    Deferred,
}

/// Deferred-response run phase.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeferredPhase<O = AgentHarnessStreamOptions> {
    /// Literal deferred kind.
    pub kind: DeferredPhaseKind,
    /// Durable deferred state.
    pub deferred: Deferred<O>,
}

/// Failure provenance.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum FailureProvenance {
    /// Failure originated from a durable response entry.
    Response {
        /// Failed response entry identity.
        #[serde(rename = "entryId")]
        entry_id: EntryId,
    },
    /// Failure originated from structural work.
    Structural {
        /// Structural task identity.
        #[serde(rename = "taskId")]
        task_id: TaskId,
    },
}

/// Literal failure-drain run-phase kind.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum FailureDrainPhaseKind {
    /// The `failure_drain` value.
    #[default]
    #[serde(rename = "failure_drain")]
    FailureDrain,
}

/// Failure-drain run phase.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FailureDrainPhase {
    /// Literal failure-drain kind.
    pub kind: FailureDrainPhaseKind,
    /// Durable operation error.
    pub error: OperationError,
    /// Durable failure provenance.
    pub provenance: FailureProvenance,
}

/// Complete run-phase union.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RunPhase<O = AgentHarnessStreamOptions> {
    /// Checkpoint phase.
    Checkpoint(CheckpointPhase),
    /// Assistant-generation phase.
    Assistant(AssistantPhase<O>),
    /// Tool-batch phase.
    Tools(ToolsPhase),
    /// In-run compaction phase.
    Compaction(RunCompactionPhase<O>),
    /// Deferred-response phase.
    Deferred(DeferredPhase<O>),
    /// Failure-drain phase.
    FailureDrain(FailureDrainPhase),
}

/// Literal run operation-state kind.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum RunStateKind {
    /// The `run` value.
    #[default]
    #[serde(rename = "run")]
    Run,
}

/// Total durable run state.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RunState<O = AgentHarnessStreamOptions> {
    /// Literal operation-state kind.
    pub kind: RunStateKind,
    /// Common cancellation control.
    pub control: Control,
    /// Settings captured atomically at run acceptance.
    pub settings: RunSettings,
    /// Total run phase.
    pub phase: RunPhase<O>,
    /// Durable pending queues and writes.
    pub inbox: Inbox,
    /// Newest settled assistant response in this run, or null.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub latest_assistant_entry_id: Option<EntryId>,
}

/// Literal standalone compaction state kind.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum CompactionStateKind {
    /// The `compaction` value.
    #[default]
    #[serde(rename = "compaction")]
    Compaction,
}

/// Total durable standalone compaction state.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CompactionState<O = AgentHarnessStreamOptions> {
    /// Literal operation-state kind.
    pub kind: CompactionStateKind,
    /// Common cancellation control.
    pub control: Control,
    /// Optional caller instructions.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub custom_instructions: Option<String>,
    /// Structural decision state.
    pub structural: StructuralDecision<O>,
}

/// Literal navigation state kind.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum NavigationStateKind {
    /// The `navigation` value.
    #[default]
    #[serde(rename = "navigation")]
    Navigation,
}

/// Literal ready-to-commit navigation phase kind.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum ReadyToCommitPhaseKind {
    /// The `ready_to_commit` value.
    #[default]
    #[serde(rename = "ready_to_commit")]
    ReadyToCommit,
}

/// Unsummarized navigation phase.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadyToCommitPhase {
    /// Literal ready-to-commit kind.
    pub kind: ReadyToCommitPhaseKind,
}

/// Literal summary navigation phase kind.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum NavigationSummaryPhaseKind {
    /// The `summary` value.
    #[default]
    #[serde(rename = "summary")]
    Summary,
}

/// Summarized navigation phase.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NavigationSummaryPhase<O = AgentHarnessStreamOptions> {
    /// Literal summary kind.
    pub kind: NavigationSummaryPhaseKind,
    /// Structural decision state.
    pub structural: StructuralDecision<O>,
}

/// Structurally unsummarized navigation state.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UnsummarizedNavigationState {
    /// Literal navigation kind.
    pub kind: NavigationStateKind,
    /// Common cancellation control.
    pub control: Control,
    /// Target entry, or root when null.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub target_id: Option<EntryId>,
    /// Optional target label.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub label: Option<String>,
    /// Literal false summarization marker.
    pub summarize: LiteralFalse,
    /// Ready-to-commit phase.
    pub phase: ReadyToCommitPhase,
}

/// Structurally summarized navigation state.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SummarizedNavigationState<O = AgentHarnessStreamOptions> {
    /// Literal navigation kind.
    pub kind: NavigationStateKind,
    /// Common cancellation control.
    pub control: Control,
    /// Non-null target entry.
    pub target_id: EntryId,
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
    /// Literal true summarization marker.
    pub summarize: LiteralTrue,
    /// Structural summary phase.
    pub phase: NavigationSummaryPhase<O>,
}

/// Structurally exclusive navigation state.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum NavigationState<O = AgentHarnessStreamOptions> {
    /// Unsummarized navigation, including root navigation.
    Unsummarized(UnsummarizedNavigationState),
    /// Summarized non-root navigation.
    Summarized(Box<SummarizedNavigationState<O>>),
}

/// Complete physical storage-version-one operation state.
///
/// There is deliberately no finished or migrated-effect-pending variant.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum OperationState<O = AgentHarnessStreamOptions> {
    /// Run operation state.
    Run(Box<RunState<O>>),
    /// Standalone compaction state.
    Compaction(CompactionState<O>),
    /// Navigation operation state.
    Navigation(NavigationState<O>),
}

/// Raw operation state whose nested stream options still require classification.
pub type RawOperationState = OperationState<RawAgentHarnessStreamOptions>;

impl OperationState<RawAgentHarnessStreamOptions> {
    /// Classifies every nested raw stream option under one host policy.
    pub fn into_durable(
        self,
        policy: &ProviderHeaderPolicy,
    ) -> Result<OperationState, ProviderHeaderClassificationError> {
        match self {
            Self::Run(state) => map_run_state(*state, policy)
                .map(Box::new)
                .map(OperationState::Run),
            Self::Compaction(state) => {
                map_compaction_state(state, policy).map(OperationState::Compaction)
            }
            Self::Navigation(state) => {
                map_navigation_state(state, policy).map(OperationState::Navigation)
            }
        }
    }
}

fn map_generation_context(
    context: GenerationContext<RawAgentHarnessStreamOptions>,
    policy: &ProviderHeaderPolicy,
) -> Result<GenerationContext, ProviderHeaderClassificationError> {
    Ok(GenerationContext {
        step_id: context.step_id,
        trigger_entry_id: context.trigger_entry_id,
        configuration: context.configuration,
        stream_options: context.stream_options.into_durable(policy)?,
        retry_policy: context.retry_policy,
        overflow_recovery_used: context.overflow_recovery_used,
    })
}

fn map_generation(
    generation: Generation<RawAgentHarnessStreamOptions>,
    policy: &ProviderHeaderPolicy,
) -> Result<Generation, ProviderHeaderClassificationError> {
    match generation {
        Generation::Ready {
            context,
            next_attempt,
        } => Ok(Generation::Ready {
            context: map_generation_context(context, policy)?,
            next_attempt,
        }),
        Generation::EffectPending {
            context,
            attempt,
            response_entry_id,
            usage_id,
            intended_output_limit,
            context_window,
        } => Ok(Generation::EffectPending {
            context: map_generation_context(context, policy)?,
            attempt,
            response_entry_id,
            usage_id,
            intended_output_limit,
            context_window,
        }),
        Generation::RetryWait {
            context,
            next_attempt,
            not_before,
            error_message,
        } => Ok(Generation::RetryWait {
            context: map_generation_context(context, policy)?,
            next_attempt,
            not_before,
            error_message,
        }),
    }
}

fn map_deferred(
    deferred: Deferred<RawAgentHarnessStreamOptions>,
    policy: &ProviderHeaderPolicy,
) -> Result<Deferred, ProviderHeaderClassificationError> {
    match deferred {
        Deferred::Suspended {
            step_id,
            source_entry_id,
            poll,
            configuration,
            stream_options,
        } => Ok(Deferred::Suspended {
            step_id,
            source_entry_id,
            poll,
            configuration,
            stream_options: stream_options.into_durable(policy)?,
        }),
        Deferred::EffectPending {
            step_id,
            source_entry_id,
            poll,
            response_entry_id,
            usage_id,
            configuration,
            stream_options,
        } => Ok(Deferred::EffectPending {
            step_id,
            source_entry_id,
            poll,
            response_entry_id,
            usage_id,
            configuration,
            stream_options: stream_options.into_durable(policy)?,
        }),
    }
}

fn map_summary_context(
    context: SummaryContext<RawAgentHarnessStreamOptions>,
    policy: &ProviderHeaderPolicy,
) -> Result<SummaryContext, ProviderHeaderClassificationError> {
    Ok(SummaryContext {
        task_id: context.task_id,
        result_entry_id: context.result_entry_id,
        kind: context.kind,
        configuration: context.configuration,
        stream_options: context.stream_options.into_durable(policy)?,
        retry_policy: context.retry_policy,
        reason: context.reason,
    })
}

fn map_summary_generation(
    generation: SummaryGeneration<RawAgentHarnessStreamOptions>,
    policy: &ProviderHeaderPolicy,
) -> Result<SummaryGeneration, ProviderHeaderClassificationError> {
    match generation {
        SummaryGeneration::Ready {
            context,
            next_attempt,
        } => Ok(SummaryGeneration::Ready {
            context: map_summary_context(context, policy)?,
            next_attempt,
        }),
        SummaryGeneration::EffectPending {
            context,
            attempt,
            request,
            usage_ids,
        } => Ok(SummaryGeneration::EffectPending {
            context: map_summary_context(context, policy)?,
            attempt,
            request,
            usage_ids,
        }),
        SummaryGeneration::RetryWait {
            context,
            next_attempt,
            not_before,
            error_message,
        } => Ok(SummaryGeneration::RetryWait {
            context: map_summary_context(context, policy)?,
            next_attempt,
            not_before,
            error_message,
        }),
    }
}

fn map_structural_decision(
    structural: StructuralDecision<RawAgentHarnessStreamOptions>,
    policy: &ProviderHeaderPolicy,
) -> Result<StructuralDecision, ProviderHeaderClassificationError> {
    match structural {
        StructuralDecision::Deciding { task_id } => Ok(StructuralDecision::Deciding { task_id }),
        StructuralDecision::Generating {
            task_id,
            generation,
        } => Ok(StructuralDecision::Generating {
            task_id,
            generation: Box::new(map_summary_generation(*generation, policy)?),
        }),
    }
}

fn map_run_phase(
    phase: RunPhase<RawAgentHarnessStreamOptions>,
    policy: &ProviderHeaderPolicy,
) -> Result<RunPhase, ProviderHeaderClassificationError> {
    match phase {
        RunPhase::Checkpoint(phase) => Ok(RunPhase::Checkpoint(phase)),
        RunPhase::Assistant(phase) => Ok(RunPhase::Assistant(AssistantPhase {
            kind: phase.kind,
            generation: map_generation(phase.generation, policy)?,
        })),
        RunPhase::Tools(phase) => Ok(RunPhase::Tools(phase)),
        RunPhase::Compaction(phase) => Ok(RunPhase::Compaction(RunCompactionPhase {
            kind: phase.kind,
            reason: phase.reason,
            structural: map_structural_decision(phase.structural, policy)?,
            resume_after: phase.resume_after,
        })),
        RunPhase::Deferred(phase) => Ok(RunPhase::Deferred(DeferredPhase {
            kind: phase.kind,
            deferred: map_deferred(phase.deferred, policy)?,
        })),
        RunPhase::FailureDrain(phase) => Ok(RunPhase::FailureDrain(phase)),
    }
}

fn map_run_state(
    state: RunState<RawAgentHarnessStreamOptions>,
    policy: &ProviderHeaderPolicy,
) -> Result<RunState, ProviderHeaderClassificationError> {
    Ok(RunState {
        kind: state.kind,
        control: state.control,
        settings: state.settings,
        phase: map_run_phase(state.phase, policy)?,
        inbox: state.inbox,
        latest_assistant_entry_id: state.latest_assistant_entry_id,
    })
}

fn map_compaction_state(
    state: CompactionState<RawAgentHarnessStreamOptions>,
    policy: &ProviderHeaderPolicy,
) -> Result<CompactionState, ProviderHeaderClassificationError> {
    Ok(CompactionState {
        kind: state.kind,
        control: state.control,
        custom_instructions: state.custom_instructions,
        structural: map_structural_decision(state.structural, policy)?,
    })
}

fn map_navigation_state(
    state: NavigationState<RawAgentHarnessStreamOptions>,
    policy: &ProviderHeaderPolicy,
) -> Result<NavigationState, ProviderHeaderClassificationError> {
    match state {
        NavigationState::Unsummarized(state) => Ok(NavigationState::Unsummarized(state)),
        NavigationState::Summarized(state) => Ok(NavigationState::Summarized(Box::new(
            SummarizedNavigationState {
                kind: state.kind,
                control: state.control,
                target_id: state.target_id,
                label: state.label,
                custom_instructions: state.custom_instructions,
                summarize: state.summarize,
                phase: NavigationSummaryPhase {
                    kind: state.phase.kind,
                    structural: map_structural_decision(state.phase.structural, policy)?,
                },
            },
        ))),
    }
}
