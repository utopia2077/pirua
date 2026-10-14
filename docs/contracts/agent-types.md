# Agent and Harness type contract

Authority: pinned upstream `853a80d26c90a14c1886f0ebb8ffaae133ca2185` and the accepted architecture.

## Completeness basis

This inventory comes from all 2,941 pinned lines of `packages/agent/docs/
harness.md`, its exact-one selector partition, 129 named declarations plus
anonymous nested unions, supporting pinned Agent/compaction/branch types, and
the Part 8 Types row. Storage version 1 is the first historical schema. Put its
physical definitions in `state::v1`; `state::current` only re-exports them.

`pi-agent-schema` solely owns serialized current/historical state. Pure planning
and mappings belong to `pi-harness-core`; live handles/futures/maps belong to
runtime. S02 declares behavior-free trait signatures but no implementations.

## Module layout

```text
message/
tool/
entry/
register/
transaction/
operation/
state/{v1,current}/
surface/{storage,session,search,planner,lane,harness,agent_loop}/
hook/
event/
snapshot/
error/
```

## Durable entries, usage, and writes

All names are camelCase on JSON. Required nullable values serialize as explicit
null; optional non-null values are omitted.

| Type | Required shape |
|---|---|
| `EntryBase` | `id,parentId:null|string,seq,timestamp,type`, optional `customType` |
| `MessageEntry` | base, `type:"message"`, `message`, optional literal-true `terminate` |
| `CompactionEntry` | base, `type:"compaction"`, summary/retainedTail/tokensBefore/fromHook, optional details/usage |
| `BranchSummaryEntry` | base, `type:"branch_summary"`, fromId/summary/fromHook, optional details/usage |
| `CustomEntry` | base, `type:"custom"`, required customType, presence-aware data |
| `EntryStructure` | exactly id/parentId/seq/timestamp/type/optional customType; no payload |
| `UsageRow` | id/seq/usage/adjustment, optional entryId/details |
| `CommitResult` | firstSeq, ordered seqs, timestamp |
| `Transaction` | ordered writes |

`NewEntry` and `NewUsageRow` are separate types omitting only the upstream `Omit`
fields. `EntryType` is closed: `message|compaction|branch_summary|custom`.

`Write` is exactly:

```text
{kind:"entry",entry}
{kind:"usage",row}
{kind:"register",op:"set",namespace,key,value}
{kind:"register",op:"delete",namespace,key}
```

The set form remains namespace/value coupled; do not collapse it to a namespace
plus arbitrary JSON value.

## Registers

| Namespace | Coupled value |
|---|---|
| `lane.leaf` | `EntryId | null` |
| `lane.config` | `LaneConfiguration` |
| `lane.state` | `LaneState` |
| `lane.lastResult` | `LaneLastResult` |
| `op.meta` | `Operation` |
| `op.state` | `state::v1::OperationState` |
| `op.tool_args` | deterministic JSON object |
| `op.preparation` | `DurableStructuralPreparation` |
| `pending.entry` | `PendingEntry` |
| `fact.name` | string |
| `fact.label` | string |
| `fact.custom` | any JSON value, including null |

`Register` is an enum internally tagged by `namespace`, with `key`, coupled
value, and `seq`. Register absence is deletion; `fact.custom = null` is data.

Supporting DTOs:

- `PendingEntry`: message/custom type, optional customType, presence-aware
  payload.
- `DurableFileOperations`: sorted read/written/edited path arrays.
- Compaction preparation: messagesToSummarize, turnPrefixMessages, retainedTail,
  split-turn flag, tokensBefore, optional previousSummary, fileOps, settings.
- Branch preparation: messages, fileOps, totalTokens.
- `LaneConfiguration`: model `{provider,modelId}`, thinkingLevel, activeToolNames.
- `LaneState`: required nullable currentOperationId, pendingNextRun.

## Scans and cursors

`BranchScan`: optional start/stopAtType/stopAtId/type/customType/order/limit/
cursor. Branch order is closed `newestFirst|oldestFirst`. `EntryScan` and
`UsageScan` use optional seq bounds, `asc|desc`, and limit. `EntryQuery` uses
type/customType/order/limit/cursor. All seq/cursor/limit fields use
`SafeUnsigned`.

## Operation metadata and complete state v1

`Operation` fields are operationId, lane, required nullable sourceLeafId,
startedAt, and intent:

```text
run        {promptEntryIds, systemPromptOverride?, resumeData?}
compaction {customInstructions?}
navigation {targetId:null|string, summarize, label?, customInstructions?}
```

Navigation intent preserves those two independent fields; raw metadata permits
`targetId:null` with either summarize value. Admission validation rejects an
invalid requested operation before persistence. Only the durable navigation
*state* below is structurally split.

There is no finished `OperationState`; terminal state is absence of `op.state`.

### Common control

```text
{status:"running"}
{status:"cancel_requested",requestedAt,drainedSteer,drainedFollowUp}
```

### Run state

`RunState` has `kind:"run"`, control, captured settings, phase, inbox, and
required nullable latestAssistantEntryId.

Run settings: compaction, steeringMode, followUpMode, and toolExecution
`sequential|parallel`. Queue mode is `all|one-at-a-time`.

Run phases:

- checkpoint `{continuation,triggerEntryId,thresholdCheckedTriggerEntryId?,
  skipInboxOnce?}`;
- assistant `{generation}`;
- tools `{batch}`;
- compaction `{reason:"threshold"|"overflow",structural,resumeAfter}`;
- deferred `{deferred}`;
- failure_drain `{error,provenance}`.

Continuation is need-assistant with overflowRecoveryUsed, or may-finish with
includeFinalAssistant. Failure provenance is response entry or structural task.
Inbox has steer, followUp, and writes arrays.

### Assistant generation

`GenerationContext`: stepId, triggerEntryId, captured configuration,
streamOptions, retryPolicy, overflowRecoveryUsed.

`Generation` closed status union:

- ready: context, nextAttempt;
- effect_pending: context, attempt, reserved responseEntryId/usageId,
  intendedOutputLimit, contextWindow;
- retry_wait: context, nextAttempt, notBefore, errorMessage.

Normalized `maxAttempts`, generation `attempt`/`nextAttempt`, and corresponding
summary-generation fields are one-based safe integers. Deferred effect-pending
`poll` is one-based; suspended `poll` counts completed polls and may be zero.
Request/source indexes remain zero-based.

### Tools

`ToolBatch`: assistantEntryId, captured configuration, turnId, calls.
Each call is planned with sourceIndex/resultEntryId; effect_pending additionally
captures replay `never|safe`; completed adds terminate. Structurally distinct
variants prevent fields that TypeScript marks `never` from coexisting.

### Deferred

Suspended deferred state has stepId/sourceEntryId/poll/configuration/
streamOptions. Effect-pending adds reserved responseEntryId/usageId. Poll and ids
are safe integers/open ids as appropriate.

### Structural work

`StructuralDecision`: task id plus deciding, or generating with a summary
generation. Summary context captures task/result id, kind
`compaction|branch_summary`, configuration, stream options, retry policy, and
optional reason `manual|threshold|overflow`.

Summary generation is ready, effect_pending (attempt, optional nested request
index/usageId, usageIds), or retry_wait. Preserve `request` absence separately
from a present object.

`CompactionState`: kind/control/customInstructions?/structural.

Navigation is structurally split:

- unsummarized: nullable targetId, optional label, `summarize:false`,
  ready_to_commit phase;
- summarized: non-null targetId, optional label/instructions, `summarize:true`,
  summary structural phase.

`OperationError` has open code, message, and presence-aware optional details.

### Last result

Base fields: operationId, operation kind, required nullable leafId, optional
finalAssistantEntryId. Variants:

- failed: error and no runCompletion;
- completed: optional runCompletion `assistant|terminated_tools`, no error;
- declined or aborted: neither error nor runCompletion.

Encode these as exactly 12 legal concrete shapes: four run shapes (completed
assistant with required final id, completed terminated-tools without it, failed,
aborted), and completed/failed/declined/aborted for each of compaction and
navigation. A run cannot decline; structural results cannot carry final-
assistant or run-completion fields; only failed results carry error.

## Supporting configuration/resources

- `CompactionSettings`: enabled, reserveTokens, keepRecentTokens.
- `CompactResult`: summary/tokensBefore/retainedTail, optional usage/details.
- `BranchSummaryResult`: summary, optional usage, readFiles, modifiedFiles.
- `Skill`: name/description/content/filePath, optional disableModelInvocation.
- `PromptTemplate`: name/content, optional description.
- `Resources`: optional promptTemplates/skills.
- `RetryPolicy`: enabled/maxRetries/baseDelayMs.
- `NormalizedRetryPolicy`: maxAttempts/baseDelayMs.
- Agent thinking level adds `off` to the AI model-thinking levels.
- Agent tool replay defaults to never but omission and explicit never round-trip
  according to the compatibility fixture.

For the ordered implementation split, C1 includes only `CompactionSettings`,
`RetryPolicy`, `NormalizedRetryPolicy`, Agent thinking/tool/queue configuration,
and captured stream options because durable state names them. `CompactResult`,
`BranchSummaryResult`, `Skill`, `PromptTemplate`, and `Resources` wait for C2.

`AgentHarnessStreamOptions` preserves transport, timeout, retry delay/count,
metadata, cache retention, and deferred bool/window. Its upstream-shaped
`headers` map is exactly `string -> string`, not AI `ProviderHeaders`' distinct
`string -> string|null` suppression map. Explicit null header values fail. The
map can become durable only as host-policy-classified public headers; default
deny, ASCII-case-insensitive duplicate rejection, and the seven credential-name
hard denials are mandatory. Runtime callbacks, cancellation, telemetry handles,
and raw secret headers are not fields.

A classified nonempty header can occur inside generation, deferred, or summary
state. C1 therefore provides policy-aware raw forms/conversions for
`OperationState`, `Register`/register-set, `Write`, and `Transaction` (or an
equivalent containing-root seed). Serializing a valid classified state and then
reopening it with the same policy must succeed; ordinary decode remains
default-deny. A standalone raw-options conversion alone is not sufficient.

## Storage/session/repository traits

Traits are behavior-free and object-safe where the runtime will require dynamic
dispatch. Use boxed futures or the chosen async-trait mechanism consistently.

`Storage` methods:

```text
commit
get_entries
get_register
list_registers
scan_branch
scan_branch_structure
scan_entries
scan_usage
stats
close
```

`StorageError` is a non-serialized/non-exhaustive Rust error with categories:
closed, conflict, corruption, invalid_data, busy, io, unsupported, and backend
with open code. Context is bounded and secret/content-free.

`SessionMetadata`: id, createdAt, storageVersion, required storeGeneration,
optional cwd/parentSessionId/legacyParentSessionPath. The additional generation
resolves the pinned prose/interface conflict; new/imported sessions use zero and
precise rewrite increments atomically.

`SessionRepo` has associated typed create/list options and create/open/list/
delete/fork. `Session` exposes metadata/id generator/view plus internal commit,
entry/register reads, and close. `SessionTree` includes leaf/entry/stats, fact
get/set, global/branch finds, and append message/custom.

`SessionError` is Rust-only/non-exhaustive: storage, schema,
unknown_custom_role, newer_storage_version, migration, not_found, closed, and
invalid_operation.

Fork options are branch (optional entry and before/at position) or whole tree.

## Search types

`SessionSearchService` declares required session search, optional entry search,
sync, synchronous notify, remove, and close. Query contains only text and
optional limit; do not invent unresolved metadata filters. Session hit contains
sessionId/optional score/optional top entry snippet+timestamp. Entry hit contains
sessionId/entryId/timestamp and optional snippet/score.

Search cursor storage keys on `(sessionId,storeGeneration)`; it is a search-
backend detail, not a conversation register.

The Rust projection is object-safe after fixing its associated error type:

```rust
pub trait SessionSearchService: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    fn search_sessions(&self, query: SearchQuery)
        -> BoxFuture<'_, Result<Vec<SessionSearchHit>, Self::Error>>;
    fn search_entries(&self, query: SearchQuery)
        -> Option<BoxFuture<'_, Result<Vec<EntrySearchHit>, Self::Error>>>;
    fn sync(&self) -> BoxFuture<'_, Result<(), Self::Error>>;
    fn notify(&self, session_id: &SessionId);
    fn remove(&self, session_id: SessionId)
        -> BoxFuture<'_, Result<(), Self::Error>>;
    fn close(&self) -> BoxFuture<'_, Result<(), Self::Error>>;
}
```

The outer option represents the upstream optional method. It must not be
collapsed into an empty success or an invented unsupported error. The error
type is intentionally not a frozen public taxonomy.

## Legacy Agent tool-batch phase ABI

S02 declares the non-Serde process-local types and object-safe phase port below;
`pi-agent-loop` implements the free function in S14. This is the Rust export
corresponding to target `executeToolBatch`; JavaScript callbacks and
`AbortSignal` are not part of the ABI.

```rust
pub struct ToolBatchInput<'a, C> {
    pub context: &'a C,
    pub assistant_message: &'a AssistantMessage,
    pub default_mode: ToolExecutionMode,
}

pub struct PreparedToolCall<B> {
    pub tool_call: AgentToolCall,
    pub args: JsonObject,
    pub binding: B,
}

pub enum ToolPreparation<B> {
    Prepared(PreparedToolCall<B>),
    Immediate(ImmediateToolOutcome),
}

pub struct ImmediateToolOutcome {
    pub result: AgentToolResult,
    pub is_error: bool,
    pub terminate: bool,
}

pub struct ExecutedToolOutcome {
    pub result: AgentToolResult,
    pub is_error: bool,
}

pub struct FinalizedToolCall {
    pub tool_call: AgentToolCall,
    pub result: AgentToolResult,
    pub is_error: bool,
    pub terminate: bool,
}

pub struct ExecutedToolBatch {
    pub messages: Vec<ToolResultMessage>,
    pub terminate: bool,
}

pub trait ToolBatchPhases<C>: Send + Sync {
    type Binding: Send + Sync;
    type Error: std::error::Error + Send + Sync + 'static;

    fn execution_mode(&self, tool_name: &str) -> Option<ToolExecutionMode>;
    fn is_cancelled(&self) -> bool;
    fn prepare<'a>(&'a self, context: &'a C,
        assistant: &'a AssistantMessage, call: &'a AgentToolCall)
        -> BoxFuture<'a, Result<ToolPreparation<Self::Binding>, Self::Error>>;
    fn execute<'a>(&'a self, prepared: &'a PreparedToolCall<Self::Binding>)
        -> BoxFuture<'a, Result<ExecutedToolOutcome, Self::Error>>;
    fn finalize<'a>(&'a self, context: &'a C,
        assistant: &'a AssistantMessage,
        prepared: &'a PreparedToolCall<Self::Binding>,
        executed: ExecutedToolOutcome)
        -> BoxFuture<'a, Result<FinalizedToolCall, Self::Error>>;
    fn emit(&self, event: AgentEvent)
        -> BoxFuture<'_, Result<(), Self::Error>>;
}

pub fn execute_tool_batch<'a, C, P>(
    input: ToolBatchInput<'a, C>,
    phases: &'a P,
) -> BoxFuture<'a, Result<ExecutedToolBatch, P::Error>>
where
    C: Sync + 'a,
    P: ToolBatchPhases<C> + ?Sized + 'a;
```

`Binding` retains the selected live tool instance without moving it into schema
or durable state. The phase port is implemented by native runtime code or by a
host adapter to the extension protocol; a foreign extension never supplies a
Rust trait object across a process boundary. The port owns invocation-scoped
cancellation and progress delivery.

The coordinator extracts calls in assistant source order. A sequential tool
forces sequential mode for the batch. Preparation and dispatch start in source
order; blocked, unknown, invalid, and genuine-length calls produce immediate
in-band outcomes and no tool effect. Parallel effects may finish in any order,
but target finalization and result messages remain in source order. `terminate`
is true only for a nonempty batch whose every finalized outcome terminates.
Harness replacement arguments are revalidated. The legacy compatibility
wrapper separately preserves completion-order `tool_execution_end` events.

## Planner/process-local signatures

These types may be declared in schema but are not serde/durable:

- `CurrentOperation` with operation/state and all expected sequence tokens;
- structured derived `EffectKey` (not a persisted field);
- complete `EffectPlan`, `EffectOutput`, `SettlementOutput/Result`, runtime
  snapshot, planner inputs, actions, summary request output;
- `Effects` method signatures for transition/settlement/terminal, run, tool
  finalization, nested summary run/settle, and sleep.

Planner `EffectPlan<C>`, `Action<C>`, and `Effects<C>` use one generic,
process-local telemetry-context parameter. Core treats it as an opaque value to
clone/propagate and keeps no telemetry dependency; runtime supplies the concrete
context. Do not replace it with serialized ids, a global registry, `JsonValue`,
or a `pi-telemetry` edge. `EffectKey` is the structured accepted-design value,
not the pinned document's temporary string alias.

Do not place `LiveEffect`, `DriveState`, promises/futures, abort controllers,
live registries, or tool context handles in schema or serde.

## Public outcomes and expected errors

Define a wire `{ok,value|error}` envelope only for exact JSON/protocol
projections. Native result-bearing Harness methods use two explicit channels:

```rust
pub type ExpectedResult<T, E> = Result<T, E>;
pub type HarnessCall<T, E> =
    Result<ExpectedResult<T, E>, HarnessRuntimeError>;

#[non_exhaustive]
pub enum HarnessRuntimeError {
    Closed(HarnessClosed),
    Fault(HarnessFault),
}
```

`Ok(Ok(value))` is accepted work, including completed, aborted, failed, or
suspended outcomes. `Ok(Err(expected))` is rejection before acceptance.
`Err(Closed)` means close won after acceptance, or a surface without an expected
channel was called after close. `Err(Fault)` is a storage/invariant/trusted-
application failure. Provider, tool, and isolated-hook failures remain in-band.
Method-specific expected-error enums must encode the exact Part 5 subsets; a
single broad `HarnessExpectedError` return type is not a valid public method
signature. Setters, facts, tree appends, and other no-expected-error surfaces
return the outer `Result<T, HarnessRuntimeError>` directly.

Outcomes cover run completed/aborted/failed/deferred/missing identities;
compaction completed/declined/aborted/failed/missing; navigation completed/
declined/aborted/failed/missing; resume adds operation kind and run id.

Count schema alternatives separately from fixture witnesses. Run has 8 concrete
alternatives, compaction 5, navigation 5, and resume 18. Navigation completion's
ordinary optional `summaryEntry` does not create another enum alternative, but
its summarized and unsummarized meanings require separate acceptance witnesses:
6 navigation and 19 resume fixtures before the general optional-field matrix.

Closed expected-error tags and fields:

| Tag | Extra fields |
|---|---|
| `LaneBusy` | lane, operationId, operationKind |
| `MissingIdentities` | lane, tools, models |
| `NoActiveRun`, `NoActiveOperation`, `NothingToResume`, `NothingToCompact` | lane |
| `InvalidMessage`, `InvalidNavigation` | lane, reason |
| `UnknownSkill`, `UnknownTemplate` | name |
| `UnknownTarget` | targetId |
| `LaneExists` | lane |
| `InvalidLane` | lane, reason |
| `Closed` | none |

Harness faults/closed rejection after acceptance are Rust runtime errors, not
durable expected-error DTOs. If close seals admission first, a result-bearing
operation returns its inner expected `Closed`. If acceptance linearizes first,
later close rejects through the outer `HarnessClosed` and leaves durable state
resumable.

## Snapshots

- queued item: entryId/message;
- lane snapshot: lane, transcript, required nullable leaf and operation, queues,
  pending writes, faulted;
- operation snapshot: id/kind/status/startedAt plus optional suspended,
  streamingMessage, runningTools, retry;
- pending write: entryId/type and optional customType/message/data;
- session snapshot: lanes/faulted;
- lane info: name, nullable leaf, nullable operation summary;
- suspended operation: lane/id/kind/reason/startedAt, optional prompt/deferred/
  aborting, required missing tool/model lists.

## Event union

One closed `HarnessEvent` is tagged by exact `type`. It includes:

- run_start/resume/suspend/abort/end;
- fault and handler_error;
- turn_start/end and retry_scheduled/start/end;
- message_start/update/end;
- tool_start/update/end;
- entry_added, write_pending, queue_update, fact_update;
- config_update lane and global property variants;
- compaction_start/end and navigation_start/end;
- lane_created and usage.

The exact schema inventory is 29 tags, 43 structural/53 concrete payload
alternatives, and 45 structural/55 concrete envelope alternatives. The
acceptance fixture set additionally witnesses `navigation_end.summaryEntry`
present and absent, producing 54 payload and 56 envelope representatives. This
extra witness is not a new schema alternative. Other optional fields still need
their own omission/presence/null tests and do not multiply these headline
counts.

Lane events and lane-config events require lane and may carry optional
literal-true recovery. Facts/fault/global config forbid lane/recovery. Usage
carries origin lane in payload but no recovery. Structurally separate event
variants enforce these envelope rules.

## Hooks

All eleven names are closed:

```text
before_run
before_resume
before_run_end
transform_context
before_request
before_payload
after_response
before_tool
after_tool
before_compaction
before_navigation
```

Each invocation also carries lane and runId. Define separate event/result DTOs
and a sealed marker/spec mapping; callback registration is process-local. Preserve
ordered patch fields, before-tool block shape, compaction/navigation decline or
supplied result, and presence-aware optional results.

## Custom message and serde rules

- Built-in Agent messages reuse `pi-ai-core` messages.
- Durable assistant messages reject `pending`, and `error`/`aborted` require
  `errorMessage`, during both encode and decode; safe Rust construction cannot
  skip the durable boundary.
- Custom Agent messages are unwrapped JSON objects with registered open role;
  they do not serialize an extra `custom` tag.
- Decode by inspecting role before selecting built-in/custom; an untagged enum
  may not shadow custom roles.
- Keep deterministic JSON maps and validate custom role schema in session code.
- Required nullable: parent ids, lane leaf, source leaf, nullable target/result/
  snapshot leaves, current operation id.
- Optional nullable JSON/details use `Presence`; plain `Option<Value>` is
  insufficient.
- Literal-true fields reject false.
- Closed discriminants and unknown closed fields reject; ids/custom types/
  custom roles/operation error codes remain open.
- Represent `never` constraints structurally in enums, especially result/error/
  completion and navigation shapes.
- Every state variant, malformed-state rejection, null/absence, and unknown
  discriminant gets a golden or negative fixture.

## Explicit deferrals

- No live storage/session/search/harness implementation.
- No `DriveState`, scheduling, effects, hooks, or event delivery behavior.
- No historical state before v1.
- No invented `migrated_effect_pending`; first v2 migration ADR must define it.
- No unresolved search metadata filter.
