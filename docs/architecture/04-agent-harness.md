# Agent and Harness Design

## Two agent runtimes remain distinct

The workspace supports two surfaces:

1. `pi-agent-loop`: current transient `Agent` and low-level loop compatibility.
2. `pi-harness-core/runtime`: target durable Harness v3.

They share message, tool, stream, and event schema but not state authority.
R4 may reuse the loop's three tool phases; it does not merge the two state
machines.

## Durable model

The authoritative session contains exactly:

```text
entries       immutable conversation tree
registers     current mutable state
usage rows    append-only cost ledger
```

`op.state/{operation_id}` holds one complete `OperationState`. Every transition
overwrites it. A terminal transaction deletes it and records `lane.lastResult`.
No runtime recovery algorithm folds a durable operation history.

## Pure core boundary

`pi-harness-core` owns:

- exhaustive validation over the complete state variants defined solely by
  `pi-agent-schema`;
- `next_action`;
- transition and settlement transaction recipes;
- generation classification and retry decisions;
- tool batch planning and ordering;
- inbox/checkpoint continuation;
- cancellation reconciliation;
- deferred response state;
- compaction/navigation operation states;
- recovery from each effect-pending state;
- terminal cleanup;
- total state migration mappings.

It receives clock, runtime settings, loaded entries/registers, and process-local
running-effect keys as explicit inputs.

## Runtime boundary

`pi-harness-runtime` owns:

- storage and effect adapters;
- per-lane mutation line;
- global settings mutation line;
- provider/tool/hook registries;
- effect dispatch and settlement;
- manual gate;
- process-local `DriveState`;
- abort controllers;
- snapshots/watch/event delivery;
- close, fault, and external finalization;
- public `AgentHarness`/`AgentLane` methods.

Runtime MUST NOT maintain an independently mutable copy of durable operation
state. After a lost compare-and-set, it reloads and replans.

## Planner interfaces

```rust
pub struct CurrentOperation {
    pub operation: Operation,
    pub state: OperationState,
    pub operation_state_seq: Sequence,
    pub lane_state: LaneState,
    pub lane_state_seq: Sequence,
    pub leaf_id: Option<EntryId>,
    pub configuration: LaneConfiguration,
    pub configuration_seq: Sequence,
}

pub enum Action<C> {
    Transition(TransitionPlan<C>),
    Dispatch(DispatchPlan<C>),
    AwaitEffect(EffectKey),
    Wait(WaitPlan<C>),
    Suspend(OperationResult),
    Finish(OperationResult),
}
```

Every `Action` variant has one owner and one settlement path.

`C` is a non-Serde process-local telemetry context. Pure core may clone and
propagate it but never inspect it. Runtime instantiates `C` with the concrete
telemetry context, preserving parentage without a forbidden core-to-telemetry
dependency or an opaque global registry. `EffectPlan<C>`, `DispatchPlan<C>`,
`WaitPlan<C>`, every commit/effect plan carrying telemetry, and `Effects<C>` use
the same parameter.

`EffectKey` is an owned structured value reconstructible solely from current
durable state:

```rust
pub struct EffectKey {
    pub session_id: SessionId,
    pub lane: LaneId,
    pub operation_id: OperationId,
    pub kind: EffectKind,
    pub step_or_task_id: EffectStepId,
    pub logical_attempt: SafeUnsigned,
    pub item_index: Option<SafeUnsigned>,
}
```

It is the idempotency/correlation identity for planning and settlement. Provider
retry creates a later logical attempt. Safe tool replay retains the original
`EffectKey`; the process-local dispatch binding changes. A settlement first
proves that current durable state still expects the exact key. Missing,
duplicate, mismatched, or externally finalized keys commit nothing.

## Effects

```rust
pub trait Effects<C>: Send + Sync
where C: Clone + Send + Sync + 'static {
    fn commit_transition(&self, plan: TransitionCommit<C>)
        -> BoxFuture<'_, Result<Option<CurrentOperation>, HarnessRuntimeError>>;
    fn commit_settlement(&self, plan: SettlementCommit<C>)
        -> BoxFuture<'_, Result<Option<SettlementResult<C>>, HarnessRuntimeError>>;
    fn commit_terminal(&self, plan: TerminalCommit<C>)
        -> BoxFuture<'_, Result<Option<CurrentOperation>, HarnessRuntimeError>>;
    fn run(&self, plan: EffectPlan<C>)
        -> BoxFuture<'_, Result<EffectOutput, HarnessRuntimeError>>;
    fn finalize_tool(&self, plan: ToolEffectPlan<C>, output: ToolRawOutput)
        -> BoxFuture<'_, Result<ToolSettlementOutput, HarnessRuntimeError>>;
    fn run_summary_request(&self, plan: SummaryRequestPlan<C>)
        -> BoxFuture<'_, Result<SummaryRequestOutput, HarnessRuntimeError>>;
    fn settle_summary_request(&self, plan: SummaryRequestSettlement<C>)
        -> BoxFuture<'_, Result<Option<CurrentOperation>, HarnessRuntimeError>>;
    fn sleep(&self, delay: Duration, context: C)
        -> BoxFuture<'_, Result<(), HarnessRuntimeError>>;
}
```

The [S09 process-local ABI contract](../contracts/runtime-binding.md) is the
normative authority for these corrected counter/result domains; the code blocks
here summarize it. It restores full durable-counter representability, the existing
close/fault channel and conditional no-current settlement. Provider/tool/isolated
hook failures remain in-band outputs. A conditional None triggers reload rather
than claiming completion or corruption; an admitted successful commit remains
successful if close occurs later. Actual runtime behavior has separate proofs.

S12 adds one narrow exception to S09's two-category outer error rule: only after
durable operation acceptance, while current state still awaits the exact
`ToolEffectPlan` key for a host-minted live `tool.v1` binding, loss of its peer
**before** the first validated terminal may return the process-local `PeerLost`
category defined below. Validation-only and pre-acceptance calls, including
`before_run`, retain their existing result channels. This is neither an ordinary
tool failure nor an owner close/fault. No S09 durable DTO, counter/result domain,
or Effects method changes with this exception.
[D06](06-extension-protocol.md#durable-identity-and-live-correlation) owns the
binding and recovery semantics; other profiles require their own checked join.

## Effect sandwich

```text
commit durable intent
        ↓
attempt external effect
        ↓
commit output + usage + next total state
```

Core crash after intent and before settlement is an explicitly unknown external
effect. Replay behavior follows the captured policy; exactly once is not
claimed.

Split-turn compaction/branch-summary requests expose three separate manual-gated
crash boundaries:

```text
commit nested request intent
        ↓
run_summary_request
        ↓
settle_summary_request (usage + request-field transition atomically)
```

The pending summary state contains `request_index` and reserved `usage_id`
before dispatch. After a crash, intermediate summary content is never resumed.
The captured structural retry policy either advances to a later numbered attempt
or produces a synthetic terminal failure. Manual and automatic conformance stop
before and after every nested boundary.

## Tool ordering

- Calls are discovered in assistant source order.
- Preflight runs in source order.
- A sequential tool makes the whole batch sequential.
- Parallel effects may finish in any order.
- Raw results are finalized and persisted in source order.
- `tool_execution_end` compatibility events may expose completion order on the
  legacy loop surface; the durable Harness event contract follows its own spec.
- A truncated assistant response executes no tool calls.
- Replacement args are revalidated in Harness v3 even though the old Agent hook
  allowed in-place mutation without revalidation.

## Legacy Agent tool-batch ABI

`pi-agent-schema` defines non-Serde `ToolBatchInput<'a, C>`, phase/result DTOs,
and the object-safe `ToolBatchPhases<C>` port. S14 makes `pi-agent-loop` export:

```rust
pub fn execute_tool_batch<'a, C, P>(
    input: ToolBatchInput<'a, C>,
    phases: &'a P,
) -> BoxFuture<'a, Result<ExecutedToolBatch, P::Error>>
where
    C: Sync + 'a,
    P: ToolBatchPhases<C> + ?Sized + 'a;
```

The port has associated process-local `Binding` and `Error` types and owns
scoped cancellation/progress adaptation. It provides execution-mode lookup,
cancellation observation, prepare/execute/finalize phases, and event emission.
No callback field, JavaScript function, `AbortSignal`, or live handle crosses
the ABI or enters Serde. Native implementations and language-neutral extension
protocol adapters implement the same host-side port.

Target batch finalization and tool-result artifacts follow assistant source
order after source-ordered preparation/dispatch; parallel effects may complete
in any order. The legacy loop wrapper preserves its separate completion-order
`tool_execution_end` event trace. Ordinary validation, blocked, tool, and
isolated-hook failures are in-band tool outcomes; the associated outer error is
reserved for close, invariant, trusted-application, or event-delivery failure,
except for S12's accepted extension invocation that loses its peer before a
validated terminal.
S02 does not place a fake implementation or a panic placeholder in the empty
`pi-agent-loop` crate merely to make the future symbol exist.

## Hook patch and effect safety

JavaScript's per-key `undefined` patch value has no JSON or language-neutral
wire equivalent. The Rust domain therefore uses an explicit metadata patch:

```rust
pub struct MetadataPatch {
    pub set: JsonObject,       // values may include JSON null
    pub delete: Vec<String>,   // sorted, unique, disjoint from set
}
```

Omitting the outer metadata field keeps the map; outer null clears all. Native
JavaScript compatibility maps property `undefined` to `delete`. Protocol peers
send the explicit projection and never depend on object/prototype semantics.

Header-bearing `HookEvent` and `HookResult` roots have raw policy-aware forms.
The conversion takes both the model-header policy and Harness provider-header
policy where required. This is the same fail-closed containing-root rule used
for durable operation state, even though hook values are transient.

Planner effect plans accept only dedicated extracted pending projections:

```text
AssistantEffectPlan → Generation.effect_pending
SummaryEffectPlan   → SummaryGeneration.effect_pending
DeferredEffectPlan  → Deferred.effect_pending
```

`HookEffectOutput::new(plan, result)` validates matching hook names before an
output can enter `EffectOutput` or `SettlementOutput`. Runtime code never relies
on a later string comparison to make an otherwise-invalid safe Rust value legal.

## Public lane interface

The Rust API mirrors upstream names semantically:

```rust
pub type ExpectedResult<T, E> = Result<T, E>;
pub type HarnessCall<T, E> =
    Result<ExpectedResult<T, E>, HarnessRuntimeError>;

#[non_exhaustive]
pub enum HarnessRuntimeError {
    Closed(HarnessClosed),
    Fault(HarnessFault),
    PeerLost,
}

impl AgentLane {
    pub async fn prompt(&self, input: PromptInput)
        -> HarnessCall<RunSuccess, RunExpectedError>;
    pub async fn compact(&self, options: CompactOptions)
        -> HarnessCall<CompactionSuccess, CompactionExpectedError>;
    pub async fn navigate_tree(&self, target: Option<EntryId>, options: NavigateOptions)
        -> HarnessCall<NavigationSuccess, NavigationExpectedError>;
    pub async fn resume(&self)
        -> HarnessCall<ResumeOutcome, ResumeExpectedError>;
    pub async fn abort(&self)
        -> HarnessCall<AbortOutcome, AbortExpectedError>;
    pub async fn steer(&self, message: AgentMessage)
        -> HarnessCall<QueueAdmission, QueueExpectedError>;
    pub async fn follow_up(&self, message: AgentMessage)
        -> HarnessCall<QueueAdmission, QueueExpectedError>;
    pub async fn next_run(&self, message: AgentMessage)
        -> HarnessCall<QueueAdmission, NextRunExpectedError>;
    pub async fn cancel_queued(&self, entry_id: EntryId)
        -> HarnessCall<CancelQueuedOutcome, ClosedExpectedError>;
    pub async fn wait_for_idle(&self) -> Result<(), HarnessRuntimeError>;
    pub async fn peek_action(&self)
        -> Result<Option<ActionInfo>, HarnessRuntimeError>;
    pub async fn execute_action(&self)
        -> Result<Option<ActionInfo>, HarnessRuntimeError>;
    pub async fn run_to_completion(&self) -> Result<(), HarnessRuntimeError>;
}
```

`PeerLost` has no peer-supplied payload and a fixed content-free display value.
For this S12 first route it reports an accepted, still-current tool invocation
without a trustworthy terminal; no synthetic tool result is published or
committed, and the Harness owner is not sealed. Once the first terminal has
been validated and retained, later connection loss does not replace that
output with `PeerLost`. Native tool failures and validated extension error
terminals remain in-band.

The inner error types are exact narrow Part 5 unions, not aliases to one broad
error enum. Setters update typed lane/config registers or process settings
according to the Harness v3 ownership rules and use only the outer runtime
error channel.

`run_when_idle` has one rejected-future channel:

```rust
pub type RunWhenIdleCall = Result<(), RunWhenIdleError>;

#[non_exhaustive]
pub enum RunWhenIdleError {
    Callback(CallbackError),
    Runtime(HarnessRuntimeError),
}
```

The callback arm propagates without faulting the Harness; the runtime arm is
limited to close/fault.

## Events and hooks

- Events are passive projections; listener failure cannot alter execution.
- Hooks are awaited effects with deterministic registration order and typed
  aggregation.
- Hook output becomes durable only when its consumer transaction commits.
- A pre-commit crash may rerun a hook.
- `before_tool` fails closed; ordinary handler failure is isolated.
- Passive subscribers use the currently active extension generation; blocking
  hook pipelines use the captured pipeline revision.

A pipeline revision is an ordered process-local list of authenticated capability
references. Stable handler identity is
`{authenticated_extension_id, handler_id}`; activation generation is not part of
resume-data ownership. The operation persists the hook name, every field from
which the logical `EffectKey` is deterministically derived, stable handler ids
needed by `before_run`/`before_resume`, and each consumer result required for
recovery. `EffectKey` is not an additional register field. A foreign function or
live connection is never durable. If a captured handler is unavailable after
restart, the hook's
documented failure/replay rule applies; a replacement with the same extension,
handler, profile, and schema digest may serve a new live binding.

## Legacy Agent compatibility

`pi-agent-loop` preserves current behavior through a separate façade:

- mutable transcript semantics are represented by a compatibility owner, not by
  the durable Harness;
- listeners settle in registration order;
- `agent_end` listeners are part of idle settlement;
- steering/follow-up semantics and current tool events remain testable;
- proxy SSE reconstruction, error, and abort mapping retain an explicit
  compatibility owner; complete implementation/parity is unwitnessed (U12).

The [legacy conformance denominator](../../conformance/agent-loop/README.md)
retains U1–U14, including the accepted unwitnessed proxy and low-level iterator
boundaries. Its actual Rust-to-Node bridge covers five named rows. The twelve-row
corpus and its causal comparator checks are separate evidence; they do not
establish twelve actual bridge observations or full legacy parity. The
[assembly proof registry](../../conformance/assembly.json) resolves the proxy
suite as an accepted limit and requires its registered owner even though no
proxy runner is available.

Rust-native applications are encouraged to use Harness v3; compatibility does
not constrain the durable schema.
