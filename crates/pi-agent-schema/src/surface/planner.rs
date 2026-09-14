//! Process-local planner, effect, action, and effect-boundary signatures.

#![allow(missing_docs)]

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use pi_ai_core::{DeferredHandle, SafeUnsigned};

use crate::surface::BoxFuture;
use crate::{
    AgentHarnessStreamOptions, AgentMessage, AgentToolResult, BranchSummaryResult, CompactResult,
    Deferred, EffectStepId, Entry, EntryId, Generation, GenerationContext, HarnessRuntimeError,
    HookEvent, HookResult, LaneConfiguration, LaneId, LaneState, NormalizedRetryPolicy,
    OneBasedSafeUnsigned, Operation, OperationId, OperationResult, OperationState, Register,
    SessionId, SettledAssistantMessage, SummaryContext, SummaryGeneration, SummaryRequestIntent,
    TaskId, UsageId,
};

/// Fully hydrated current operation and expected register sequence tokens.
#[derive(Clone, Debug, PartialEq)]
pub struct CurrentOperation {
    /// Immutable operation acceptance metadata.
    pub operation: Operation,
    /// Complete current program counter.
    pub state: OperationState,
    /// Expected `op.state` register sequence.
    pub operation_state_seq: SafeUnsigned,
    /// Current total lane state.
    pub lane_state: LaneState,
    /// Expected `lane.state` register sequence.
    pub lane_state_seq: SafeUnsigned,
    /// Current lane leaf.
    pub leaf_id: Option<EntryId>,
    /// Current lane configuration.
    pub configuration: LaneConfiguration,
    /// Expected `lane.config` register sequence.
    pub configuration_seq: SafeUnsigned,
}

/// Closed external-effect kind used in structured correlation keys.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum EffectKind {
    /// Assistant generation request.
    Assistant,
    /// Structural summary composition.
    Summary,
    /// Real tool execution.
    Tool,
    /// Deferred response fetch.
    Deferred,
    /// Best-effort deferred cancellation.
    CancelDeferred,
    /// Awaited hook pipeline.
    Hook,
}

/// Structured derived process-local effect correlation identity.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct EffectKey {
    /// Owning session.
    pub session_id: SessionId,
    /// Owning lane.
    pub lane: LaneId,
    /// Owning operation.
    pub operation_id: OperationId,
    /// Effect family.
    pub kind: EffectKind,
    /// Durable step or task identity.
    pub step_or_task_id: EffectStepId,
    /// Logical attempt in the full safe-number domain; zero is available to
    /// effect kinds without a numbered attempt.
    pub logical_attempt: SafeUnsigned,
    /// Optional source or request index.
    pub item_index: Option<SafeUnsigned>,
}

/// Assistant provider effect plan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PendingProjectionError {
    /// Generation was ready or waiting rather than effect-pending.
    GenerationNotPending,
    /// Summary generation was ready or waiting rather than effect-pending.
    SummaryNotPending,
    /// Deferred state was suspended rather than effect-pending.
    DeferredNotPending,
}

impl std::fmt::Display for PendingProjectionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::GenerationNotPending => formatter.write_str("generation is not effect-pending"),
            Self::SummaryNotPending => {
                formatter.write_str("summary generation is not effect-pending")
            }
            Self::DeferredNotPending => formatter.write_str("deferred state is not effect-pending"),
        }
    }
}

impl std::error::Error for PendingProjectionError {}

/// Owned exact projection of `Generation::EffectPending`.
#[derive(Clone, Debug, PartialEq)]
pub struct PendingGeneration {
    /// Captured generation context.
    pub context: GenerationContext,
    /// One-based logical attempt.
    pub attempt: OneBasedSafeUnsigned,
    /// Reserved response entry.
    pub response_entry_id: EntryId,
    /// Reserved usage row.
    pub usage_id: UsageId,
    /// Intended request output limit.
    pub intended_output_limit: SafeUnsigned,
    /// Captured context window.
    pub context_window: SafeUnsigned,
}

impl TryFrom<Generation> for PendingGeneration {
    type Error = PendingProjectionError;

    fn try_from(generation: Generation) -> Result<Self, Self::Error> {
        match generation {
            Generation::EffectPending {
                context,
                attempt,
                response_entry_id,
                usage_id,
                intended_output_limit,
                context_window,
            } => Ok(Self {
                context,
                attempt,
                response_entry_id,
                usage_id,
                intended_output_limit,
                context_window,
            }),
            Generation::Ready { .. } | Generation::RetryWait { .. } => {
                Err(PendingProjectionError::GenerationNotPending)
            }
        }
    }
}

/// Owned exact projection of `SummaryGeneration::EffectPending`.
#[derive(Clone, Debug, PartialEq)]
pub struct PendingSummaryGeneration {
    /// Captured summary context.
    pub context: SummaryContext,
    /// One-based logical attempt.
    pub attempt: OneBasedSafeUnsigned,
    /// Optional current nested request intent.
    pub request: Option<SummaryRequestIntent>,
    /// Already settled usage-row identities.
    pub usage_ids: Vec<UsageId>,
}

impl TryFrom<SummaryGeneration> for PendingSummaryGeneration {
    type Error = PendingProjectionError;

    fn try_from(generation: SummaryGeneration) -> Result<Self, Self::Error> {
        match generation {
            SummaryGeneration::EffectPending {
                context,
                attempt,
                request,
                usage_ids,
            } => Ok(Self {
                context,
                attempt,
                request,
                usage_ids,
            }),
            SummaryGeneration::Ready { .. } | SummaryGeneration::RetryWait { .. } => {
                Err(PendingProjectionError::SummaryNotPending)
            }
        }
    }
}

/// Owned exact projection of `Deferred::EffectPending`.
#[derive(Clone, Debug, PartialEq)]
pub struct PendingDeferred {
    /// Original generation step.
    pub step_id: crate::StepId,
    /// Deferred source entry.
    pub source_entry_id: EntryId,
    /// One-based poll number.
    pub poll: OneBasedSafeUnsigned,
    /// Reserved response entry.
    pub response_entry_id: EntryId,
    /// Reserved usage row.
    pub usage_id: UsageId,
    /// Captured lane configuration.
    pub configuration: LaneConfiguration,
    /// Captured stream options.
    pub stream_options: AgentHarnessStreamOptions,
}

impl TryFrom<Deferred> for PendingDeferred {
    type Error = PendingProjectionError;

    fn try_from(deferred: Deferred) -> Result<Self, Self::Error> {
        match deferred {
            Deferred::EffectPending {
                step_id,
                source_entry_id,
                poll,
                response_entry_id,
                usage_id,
                configuration,
                stream_options,
            } => Ok(Self {
                step_id,
                source_entry_id,
                poll,
                response_entry_id,
                usage_id,
                configuration,
                stream_options,
            }),
            Deferred::Suspended { .. } => Err(PendingProjectionError::DeferredNotPending),
        }
    }
}

/// Assistant provider effect plan.
#[derive(Clone, Debug, PartialEq)]
pub struct AssistantEffectPlan<C> {
    /// Exact derived correlation key.
    pub key: EffectKey,
    /// Durable effect-pending generation.
    pub generation: PendingGeneration,
    /// Effective curated stream options.
    pub stream_options: AgentHarnessStreamOptions,
    /// Opaque process-local telemetry context.
    pub telemetry_context: C,
}

/// Structural summary effect plan.
#[derive(Clone, Debug, PartialEq)]
pub struct SummaryEffectPlan<C> {
    /// Exact derived correlation key.
    pub key: EffectKey,
    /// Durable effect-pending summary generation.
    pub generation: PendingSummaryGeneration,
    /// Opaque process-local telemetry context.
    pub telemetry_context: C,
}

/// Tool effect plan using persisted effective arguments.
#[derive(Clone, Debug, PartialEq)]
pub struct ToolEffectPlan<C> {
    /// Exact derived correlation key.
    pub key: EffectKey,
    /// Source assistant entry.
    pub assistant_entry_id: EntryId,
    /// Zero-based content source index.
    pub source_index: SafeUnsigned,
    /// Full deterministic `op.tool_args` register key.
    pub args_key: String,
    /// Opaque process-local telemetry context.
    pub telemetry_context: C,
}

/// Deferred-fetch effect plan.
#[derive(Clone, Debug, PartialEq)]
pub struct DeferredEffectPlan<C> {
    /// Exact derived correlation key.
    pub key: EffectKey,
    /// Durable effect-pending deferred state.
    pub deferred: PendingDeferred,
    /// Effective curated stream options.
    pub stream_options: AgentHarnessStreamOptions,
    /// Opaque process-local telemetry context.
    pub telemetry_context: C,
}

/// Best-effort deferred cancellation effect plan.
#[derive(Clone, Debug, PartialEq)]
pub struct CancelDeferredEffectPlan<C> {
    /// Exact derived correlation key.
    pub key: EffectKey,
    /// Newest deferred source entry.
    pub source_entry_id: EntryId,
    /// Provider handle copied from the source response.
    pub handle: DeferredHandle,
    /// Opaque process-local telemetry context.
    pub telemetry_context: C,
}

/// Awaited complete hook-pipeline plan.
#[derive(Clone, Debug, PartialEq)]
pub struct HookEffectPlan<C> {
    /// Exact derived correlation key.
    pub key: EffectKey,
    /// Typed callback-free hook invocation.
    pub event: HookEvent,
    /// Opaque process-local telemetry context.
    pub telemetry_context: C,
}

/// Hook result did not match the dispatched hook event.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HookPairError;

impl std::fmt::Display for HookPairError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("hook result name does not match dispatched hook event")
    }
}

impl std::error::Error for HookPairError {}

/// Validated non-Serde hook effect output with a proven event/result name match.
#[derive(Clone, Debug, PartialEq)]
pub struct HookEffectOutput {
    key: EffectKey,
    result: HookResult,
}

impl HookEffectOutput {
    /// Validates a result against its dispatched plan before producing an output.
    pub fn new<C>(plan: &HookEffectPlan<C>, result: HookResult) -> Result<Self, HookPairError> {
        if plan.event.name() != result.name() {
            return Err(HookPairError);
        }
        Ok(Self {
            key: plan.key.clone(),
            result,
        })
    }

    /// Returns the exact effect key.
    pub const fn key(&self) -> &EffectKey {
        &self.key
    }

    /// Returns the paired hook result.
    pub const fn result(&self) -> &HookResult {
        &self.result
    }
}

/// Complete process-local external-effect plan.
#[derive(Clone, Debug, PartialEq)]
pub enum EffectPlan<C> {
    /// Assistant generation.
    Assistant(AssistantEffectPlan<C>),
    /// Structural summary generation.
    Summary(SummaryEffectPlan<C>),
    /// Tool execution.
    Tool(ToolEffectPlan<C>),
    /// Deferred fetch.
    Deferred(DeferredEffectPlan<C>),
    /// Deferred cancellation.
    CancelDeferred(CancelDeferredEffectPlan<C>),
    /// Awaited hook pipeline.
    Hook(HookEffectPlan<C>),
}

impl<C> EffectPlan<C> {
    /// Returns the exact derived key for this plan.
    pub const fn key(&self) -> &EffectKey {
        match self {
            Self::Assistant(plan) => &plan.key,
            Self::Summary(plan) => &plan.key,
            Self::Tool(plan) => &plan.key,
            Self::Deferred(plan) => &plan.key,
            Self::CancelDeferred(plan) => &plan.key,
            Self::Hook(plan) => &plan.key,
        }
    }
}

/// Successful structural attempt result.
#[derive(Clone, Debug, PartialEq)]
pub enum SummarySuccessResult {
    /// Generated compaction result.
    Compact(CompactResult),
    /// Generated branch-summary result.
    BranchSummary(BranchSummaryResult),
}

/// Three-way structural attempt outcome.
#[derive(Clone, Debug, PartialEq)]
pub enum SummaryAttemptOutcome {
    /// Successful compaction or branch-summary result.
    Success(SummarySuccessResult),
    /// Retryable in-band operation failure.
    Retry(crate::OperationError),
    /// Terminal in-band operation failure.
    Failure(crate::OperationError),
}

/// Raw tool output before source-ordered `after_tool` finalization.
#[derive(Clone, Debug, PartialEq)]
pub struct ToolRawOutput {
    /// Exact effect key.
    pub key: EffectKey,
    /// Raw result.
    pub result: AgentToolResult,
    /// Whether raw execution failed.
    pub is_error: bool,
}

/// Effect output returned in-band by adapters.
#[derive(Clone, Debug, PartialEq)]
pub enum EffectOutput {
    /// Serialized start check found cancellation first.
    NotStarted { key: EffectKey },
    /// Settled assistant response.
    Assistant {
        key: EffectKey,
        message: SettledAssistantMessage,
    },
    /// Settled deferred fetch response.
    Deferred {
        key: EffectKey,
        message: SettledAssistantMessage,
    },
    /// Structural attempt outcome.
    Summary {
        key: EffectKey,
        outcome: SummaryAttemptOutcome,
    },
    /// Raw tool output awaiting source-ordered finalization.
    ToolRaw(ToolRawOutput),
    /// Typed hook-pipeline aggregate.
    Hook(HookEffectOutput),
    /// Best-effort deferred cancellation completed.
    CancelDeferred { key: EffectKey },
}

/// Source-ordered finalized tool settlement.
#[derive(Clone, Debug, PartialEq)]
pub struct ToolSettlementOutput {
    /// Exact effect key.
    pub key: EffectKey,
    /// Finalized result.
    pub result: AgentToolResult,
    /// Final error classification.
    pub is_error: bool,
    /// Final early-termination decision.
    pub terminate: bool,
}

/// Effect output accepted by durable settlement.
#[derive(Clone, Debug, PartialEq)]
pub enum SettlementOutput {
    /// Serialized start check found cancellation first.
    NotStarted { key: EffectKey },
    /// Settled assistant response.
    Assistant {
        key: EffectKey,
        message: SettledAssistantMessage,
    },
    /// Settled deferred fetch response.
    Deferred {
        key: EffectKey,
        message: SettledAssistantMessage,
    },
    /// Structural attempt outcome.
    Summary {
        key: EffectKey,
        outcome: SummaryAttemptOutcome,
    },
    /// Finalized tool output.
    Tool(ToolSettlementOutput),
    /// Typed hook-pipeline aggregate.
    Hook(HookEffectOutput),
    /// Best-effort deferred cancellation completed.
    CancelDeferred { key: EffectKey },
}

/// Settlement commit result and any immediate follow-up dispatch.
#[derive(Clone, Debug, PartialEq)]
pub struct SettlementResult<C> {
    /// Reloaded current operation after settlement.
    pub current: CurrentOperation,
    /// Immediate process-local dispatch prepared by pre-intent settlement.
    pub dispatch: Option<EffectPlan<C>>,
    /// Missing-identity suspension without durable mutation.
    pub suspend: Option<OperationResult>,
    /// Whether this settlement consumed the resume invocation's sole poll permit.
    pub consume_deferred_poll: bool,
}

/// Process-local runtime settings snapshot.
#[derive(Clone, Debug, PartialEq)]
pub struct RuntimeSnapshot {
    /// Monotonic process-local settings revision.
    pub settings_revision: SafeUnsigned,
    /// Current base stream options.
    pub stream_options: AgentHarnessStreamOptions,
    /// Current normalized retry policy.
    pub retry_policy: NormalizedRetryPolicy,
}

/// A loaded planner input value keyed by entry id or deterministic register key.
#[derive(Clone, Debug, PartialEq)]
pub enum PlannerLoadedValue {
    /// Detached committed entry.
    Entry(Entry),
    /// Detached current register.
    Register(Register),
}

/// Zero-or-one deferred poll permit.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum DeferredPollPermit {
    /// No poll may start.
    #[default]
    None,
    /// One poll may start.
    One,
}

/// Complete pure-planner input snapshot.
#[derive(Clone, Debug, PartialEq)]
pub struct PlannerInputs<C> {
    /// Exact process-local running plans without promises or controllers.
    pub running: BTreeMap<EffectKey, EffectPlan<C>>,
    /// Resume invocation poll permit.
    pub deferred_poll: DeferredPollPermit,
    /// Process-local best-effort deferred cancellation targets.
    pub deferred_cancellations: BTreeSet<EntryId>,
    /// Bounded detached entry/register hydration.
    pub loaded: BTreeMap<String, PlannerLoadedValue>,
    /// Runtime settings snapshot.
    pub runtime: RuntimeSnapshot,
    /// Optional already-projected Agent context.
    pub context: Option<Vec<AgentMessage>>,
    /// Injected current Unix timestamp in milliseconds.
    pub now: SafeUnsigned,
}

/// Conditional ordinary transition plan.
#[derive(Clone, Debug, PartialEq)]
pub struct TransitionPlan<C> {
    /// Next complete operation state.
    pub next: OperationState,
    /// Opaque process-local telemetry context.
    pub telemetry_context: C,
    /// Expected configuration sequence when snapshotting configuration.
    pub expected_configuration_seq: Option<SafeUnsigned>,
    /// Expected process-local settings revision when snapshotting settings.
    pub expected_settings_revision: Option<SafeUnsigned>,
}

/// Effect dispatch and optional intent transition.
#[derive(Clone, Debug, PartialEq)]
pub struct DispatchPlan<C> {
    /// Optional complete intent state committed before dispatch.
    pub intent: Option<OperationState>,
    /// Process-local effect plan.
    pub effect: EffectPlan<C>,
    /// Whether committing this intent consumes the single poll permit.
    pub consume_deferred_poll: bool,
}

/// Injected-clock wait plan.
#[derive(Clone, Debug, PartialEq)]
pub struct WaitPlan<C> {
    /// Earliest Unix timestamp in milliseconds.
    pub until: SafeUnsigned,
    /// Opaque process-local telemetry context.
    pub telemetry_context: C,
}

/// Complete pure-planner action.
#[derive(Clone, Debug, PartialEq)]
pub enum Action<C> {
    /// Commit an ordinary total-state transition.
    Transition(TransitionPlan<C>),
    /// Commit optional intent and dispatch an effect.
    Dispatch(Box<DispatchPlan<C>>),
    /// Await one exact already-running effect.
    AwaitEffect(EffectKey),
    /// Wait through the injected effect boundary.
    Wait(WaitPlan<C>),
    /// Return while leaving the durable operation open.
    Suspend(OperationResult),
    /// Commit terminal cleanup and return.
    Finish(OperationResult),
}

/// Conditional transition commit request.
#[derive(Clone, Debug, PartialEq)]
pub struct TransitionCommit<C> {
    /// Expected current operation.
    pub current: CurrentOperation,
    /// Next complete state.
    pub next: OperationState,
    /// Opaque process-local telemetry context.
    pub telemetry_context: C,
    /// Optional expected configuration sequence.
    pub expected_configuration_seq: Option<SafeUnsigned>,
    /// Optional expected settings revision.
    pub expected_settings_revision: Option<SafeUnsigned>,
}

/// Conditional settlement commit request.
#[derive(Clone, Debug, PartialEq)]
pub struct SettlementCommit<C> {
    /// Expected current operation.
    pub current: CurrentOperation,
    /// Exact effect plan being settled.
    pub plan: EffectPlan<C>,
    /// In-band settlement output.
    pub output: SettlementOutput,
    /// Opaque process-local telemetry context.
    pub telemetry_context: C,
}

/// Conditional terminal transaction request.
#[derive(Clone, Debug, PartialEq)]
pub struct TerminalCommit<C> {
    /// Expected current operation.
    pub current: CurrentOperation,
    /// Public result computed from final state.
    pub result: OperationResult,
    /// Opaque process-local telemetry context.
    pub telemetry_context: C,
}

/// One nested summary provider request plan.
#[derive(Clone, Debug, PartialEq)]
pub struct SummaryRequestPlan<C> {
    /// Structural task identity.
    pub task_id: TaskId,
    /// One-based outer attempt.
    pub attempt: OneBasedSafeUnsigned,
    /// Zero-based nested request index.
    pub request_index: SafeUnsigned,
    /// Reserved usage-row identity.
    pub usage_id: UsageId,
    /// Captured lane configuration.
    pub configuration: LaneConfiguration,
    /// Projected Agent messages.
    pub messages: Vec<AgentMessage>,
    /// Opaque process-local telemetry context.
    pub telemetry_context: C,
}

/// Nested summary request output.
#[derive(Clone, Debug, PartialEq)]
pub enum SummaryRequestOutput {
    /// Settled provider response.
    Response(Box<SettledAssistantMessage>),
    /// Cancellation won the serialized effect-start check.
    NotStarted,
}

/// Atomic nested summary usage/state settlement request.
#[derive(Clone, Debug, PartialEq)]
pub struct SummaryRequestSettlement<C> {
    /// Expected current operation.
    pub current: CurrentOperation,
    /// Structural task identity.
    pub task_id: TaskId,
    /// One-based outer attempt.
    pub attempt: OneBasedSafeUnsigned,
    /// Zero-based nested request index.
    pub request_index: SafeUnsigned,
    /// Reserved usage-row identity.
    pub usage_id: UsageId,
    /// Settled response.
    pub response: SettledAssistantMessage,
    /// Opaque process-local telemetry context.
    pub telemetry_context: C,
}

/// Behavior-free effect interpreter boundary.
///
/// Lifecycle rejection uses the outer closed/fault channel; ordinary provider,
/// tool and hook failures remain in-band outputs. A definitive admitted commit
/// success must not be rewritten as a lifecycle failure by a later close.
///
/// Conditional `Ok(None)` means no result was admitted against the supplied
/// ownership/key. It carries no dispatch or consumed permit. The caller reloads
/// under lane discipline to distinguish newer state from external finalization;
/// absence alone does not establish completion or corruption.
pub trait Effects<C>: Send + Sync
where
    C: Clone + Send + Sync + 'static,
{
    /// Conditionally commits an ordinary total-state transition.
    fn commit_transition(
        &self,
        plan: TransitionCommit<C>,
    ) -> BoxFuture<'_, Result<Option<CurrentOperation>, HarnessRuntimeError>>;
    /// Conditionally commits one exact effect settlement.
    fn commit_settlement(
        &self,
        plan: SettlementCommit<C>,
    ) -> BoxFuture<'_, Result<Option<SettlementResult<C>>, HarnessRuntimeError>>;
    /// Conditionally commits terminal cleanup and result publication.
    fn commit_terminal(
        &self,
        plan: TerminalCommit<C>,
    ) -> BoxFuture<'_, Result<Option<CurrentOperation>, HarnessRuntimeError>>;
    /// Revalidates effect start, then executes one external plan in-band.
    fn run(&self, plan: EffectPlan<C>) -> BoxFuture<'_, Result<EffectOutput, HarnessRuntimeError>>;
    /// Runs source-ordered after-tool finalization.
    fn finalize_tool(
        &self,
        plan: ToolEffectPlan<C>,
        output: ToolRawOutput,
    ) -> BoxFuture<'_, Result<ToolSettlementOutput, HarnessRuntimeError>>;
    /// Runs one nested provider request in a structural attempt.
    fn run_summary_request(
        &self,
        plan: SummaryRequestPlan<C>,
    ) -> BoxFuture<'_, Result<SummaryRequestOutput, HarnessRuntimeError>>;
    /// Atomically settles nested summary usage and request state.
    fn settle_summary_request(
        &self,
        plan: SummaryRequestSettlement<C>,
    ) -> BoxFuture<'_, Result<Option<CurrentOperation>, HarnessRuntimeError>>;
    /// Waits using the runtime clock/cancellation boundary.
    fn sleep(&self, delay: Duration, context: C) -> BoxFuture<'_, Result<(), HarnessRuntimeError>>;
}

/// Exact Effects boundary method inventory.
pub const EFFECTS_METHODS: &[&str] = &[
    "commit_transition",
    "commit_settlement",
    "commit_terminal",
    "run",
    "finalize_tool",
    "run_summary_request",
    "settle_summary_request",
    "sleep",
];
