# Runtime effects and durable operation ownership

Authority: pinned upstream `853a80d26c90a14c1886f0ebb8ffaae133ca2185` and the accepted architecture.

## Concrete ownership and coherent reads

The factory implements the existing AgentHarnessFactory with concrete
`RuntimeSession<S,M>` and `ModelsRegistry`, instantiating telemetry with
`Arc<dyn TelemetryContext>`. Its constructor receives the existing Clock,
TaskSpawner, existing IdGenerator and an explicit caller-supplied `Arc<C>` fallback
context; these do not become durable options. There is no C:Default or invented C.
An explicit options.tool_context overrides this fallback. Static values are held
by Arc. A functional system prompt resolves its context for before_run preview and
again per provider request absent an accepted fixed hook override. Static/absent
prompts and accepted overrides need no otherwise-unused context callback. Tool
batches independently resolve one shared context for their own live/restored batch.
Existing summary context uses a caller-supplied SummaryRenderer, the accepted S06
trusted materialization port; the runtime does not invent a summary role/format.
One shared owner holds Session, Models, hooks, tool registry, settings, admission,
retained tasks and the lane map. Every handle of a lane shares its mutation line,
manual gate and drive bindings. Weak backreferences prevent owner/lane cycles.
The main Harness delegates inherited lane methods to that same main lane.

Add `RuntimeStorage: CheckedStorage` beside existing Session backend capabilities:

```rust
fn runtime_attachment(&self) -> &RuntimeAttachment;
fn read_stamp(&self) -> BoxFuture<'_, Result<ReadStamp, StorageError>>;
```

RuntimeAttachment owns a backend-incarnation claim cell. Its non-Clone RAII lease
rejects simultaneous Harness attachments even through separate RuntimeSession
wrappers over the same backend. It is not keyed by logical SessionId and does not
pretend that ordinary Session/Storage clones disappear. ReadStamp is opaque and
compares incarnation plus nonwrapping publication revision. Memory, JSONL and
SQLite expose their actual protected revision, checking the same live/fenced owner
as ordinary reads. Every successful publication and precise rewrite invalidates
earlier stamps; close/reopen cannot reuse an incarnation. No whole-snapshot fallback.
RuntimeSession forwards claim/read_stamp and exposes its actual borrowed codec;
SessionCodec exposes its borrowed header_policy, pairing both at hook invocation.
Frozen Storage/Session method inventories and persistent formats stay unchanged.

Restore brackets lane discovery and its initial snapshot with stamps. Each lane
read brackets the existing three/five root reads plus the existing validator's
exact entry/register hydration with stamps. A changed stamp discards the entire
attempt, including any provisional validation failure. A matching stamp permits
total semantic validation. Retain roots and hydrated maps together, including
leaf/meta tokens outside CurrentOperation. Retry observes admission; per-attempt
reads depend on current named state, with no guarantee against endless external
contention. Restore never reads history, ledger, lastResult or provider context,
and never resolves providers or starts effects. Public context reads are separate.

All Harness mutations use the same lane line, and settings capture takes settings
before lane. Publication uses real RuntimeSession::commit_conditional with all
root and named mutable-register Seq/Absent predicates the recipe depends upon.
Settings revision is checked while holding its line. CAS loss discards pending
durable publication, new dispatch and permit consumption, then reloads. An already
completed live output remains owned through recomputation/retry while its exact key
is still expected; contention does not turn it into an unknown effect or repeat its
execution/finalization. No read-then-unconditional write.
Administrative finalization joins this line; disappearance of an owned operation
stops stale work without recreating registers. Raw external writers remain outside
the public two-history ordering contract, but read stamps and CAS detect contention.

## Pure recipes and complete process-local inputs

Core continues to depend only on schema. It owns next-action selection, acceptance,
transition, settlement and terminal transaction recipes. Runtime supplies explicit
phase-tagged values: session identity, opaque telemetry, clock value, fresh IDs,
full typed request event/model facts, current tool replay/mode/membership, exact
prepared output, retry classification and terminal deletion inventory. No default
telemetry, fake model, fabricated IDs, callback, registry or storage handle enters
core. Data-only schema reexports may name already-owned AI field types; they do
not import behavior or widen serialized values.

The recipe carries exact expected tokens/key, the existing ordered Transaction,
next/current state or terminal result, optional immediate dispatch and ordered
post-commit publication descriptors. Runtime alone maps tokens into concrete
Session CAS and derives committed entry/usage events from actual returned rows.
Wrong family, session, lane, operation, step, attempt or source index produces
no recipe/write/event. Settlement replans against latest state, preserving newer
inbox/control and parallel intents. Source order constrains tool settlement too.

For before_run_end with no follow-up, the recipe returns a terminal continuation
without a durable no-op write. An additive process-local
`SettlementResult<C>.terminal: Option<OperationResult>` transports the approved
result together with its exact `current`; ordinary settlements use None. The
driver then calls the existing commit_terminal with that current/C at a separate
manual boundary. Changed state/control/inbox tokens invalidate the approval and
cause reload/replanning, not an unconditional terminal write. Settlement itself
does not perform the terminal transaction. This revision3 binding is the
derivation of the terminal recipe continuation. Independent code and behavior
checks must cover this continuation. Existing
fields/eight methods remain unchanged, with no hidden approval side map.

Captured pending placement may use a checked, non-Serde PendingPlacement receipt
with private fields. Its constructor privately proves serialized content pairing
using schema's existing facilities. Core owns entry identity, lane ownership,
ordering, conditions and the acceptance recipe; schema exposes no new acceptance
algorithm and core acquires no serialization dependency.

The existing Action enum handles ordinary transition/dispatch/wait/suspend/finish.
A core-owned procedure wrapper additionally represents reconciliation that feeds
an exact synthetic/immediate output into commit_settlement without executing an
external effect. No fake hook, provider call or ordinary transition stands in for
output settlement. Later-slice states have explicit finite unsupported branches;
they cannot return a successful empty transaction or successful suspension.

Two process-local Effects transports close information loss:

* A paired RequestOptions output, additive to EffectOutput/SettlementOutput, owns
  the complete effective AgentHarnessStreamOptions. Its checked constructor takes
  a BeforeRequest HookEffectPlan and copies the exact key. Existing HookResult and
  HookEffectOutput stay unchanged. Clear `X-A` followed by insert `x-a` remains
  lossless through intent and actual request construction.
* ToolClearance is an additive EffectPlan whose key identifies the planned source
  position. Effects.run performs phase1, then returns exact prepared effective args
  and captured replay or an immediate result/error/terminate. Its corresponding
  settlement output carries all payload data directly. No side map substitutes for
  args, replay or result. Live selected bindings/context remain process-local owner
  objects. Successful clearance settlement writes args and pending state atomically
  and returns complete ToolEffectPlan; immediate outcomes write only their result.

Both additions preserve eight Effects methods and all durable field shapes. Their
constructors and settlement joins prove event/state/key pairing. Private names and
module layout remain implementation choices; the implementation must expose a
coherent typed core interface before runtime integration consumes it.

## Every procedure effect belongs to the owner

All procedure commits, provider/tool starts, awaited hooks and timers cross exactly
one existing Effects method. Surface acceptance/tree/config writes stay ungated.
Manual permission comes before admission/start. Actual start reacquires lane,
verifies current key/control/owner and atomically compares/captures the actual
registry registration token, then registers a unique live binding before releasing
the line. Abort-first returns NotStarted without callbacks; start-first
permits the started callback to finish. Missing/replaced registrations at actual
start settle in-band without executing stale bindings; pre-intent identity absence
still suspends unchanged. Failed intent CAS retires the candidate binding. Real
Models dispatch retains its own provider-capture sequencing. Raw tool completions remain live until
source-ordered finalization/settlement, not merely until a provider terminal event.

Wrap the injected TaskSpawner with supervised future ownership. Register before
host spawn; rollback rejection or drop once; allow reentrant inline polling. No
spawn, wake, callbacks or user destructors execute under bookkeeping locks.
Drivers, admitted commits and close run in retained tasks independent of caller
waiters. Returned faults, trusted computation unwinds and premature host task drops
are observed by retained task ownership itself, even after a waiter is dropped.
CommitPermit and mutation ownership survive through definitive publication.
ResponseMetadataReader remains passive and is never used as a completion ticket.

Close seals admission, rejects parked/unresolved local calls, signals live work,
drains admitted commits and their publication, closes Session, and releases the
attachment. It writes no cancellation, synthetic settlement or terminal transaction.
Noncooperative external work cannot delay close indefinitely: retire its binding
and cut cancellable wrapper futures; late output has no authority. Started hooks
are protected against operation abort, not forced to outlive owner close. Fault
retains the first existing HarnessFault and seals all future work. Trusted prompt,
context, projection or conversion defects fault the owner; ordinary provider/tool
and isolated hook failures remain in-band. Full queue/deferred abort machinery is S10.

Durable entry_added and usage events publish after their committed transaction and
before later lane mutation publication. Ephemeral message lifecycle follows pinned
H2447–2459: streamed message_end precedes settlement; synthetic message_start and
message_end also precede settlement. Only entry_added proves durability. These
lifecycle events do not become durable publications if a subsequent CAS loses.
Listener work is passive, isolated, outside the lane lock and never manually gated.
Snapshot-plus-buffer registration is atomic with publication; start flushes ordered
buffered events before live delivery. Session and lane snapshots derive the same
Running/Suspended status from the shared live drive binding. R1/R2
primitives/basic events are required here; S13 retains full-catalog completion.
The public SessionTree facade routes supported writes through owner admission and
the lane line, with existing error signatures and no raw idle-only bypass.

### Registration-bound dispatch

ModelsRegistry owns a non-Serde opaque ProviderRegistration (registry-instance and
committed ProviderRecord identity/epoch) and a non-Clone CapturedTextDispatch. An
outside-line preflight pairs the full observed Model with one registration token;
token-before/model-lookup/token-after must agree, or a helper reads the model from
that exact registration. Provider.models(), auth/adapter getters, clock and host
callbacks never execute under the serialized lane start or registry membership lock.

At actual start, capture_text_dispatch(expected_token, full_model) synchronously
checks registry identity, exact committed record and model.provider using stored data,
then retains that record. It performs no auth, stream initialization, spawn or poll.
A missing/replaced record produces the existing known-model in-band dispatch failure
outside locks; wrong registry/model pairing is an internal fault. Same-Arc re-registration
is a new epoch and cannot pass an old token. Keep displaced tokens alive until locks
are released so their provider destructors cannot run inside the critical section.

After release, the consumed capture follows the existing shared auth/endpoint/header/
lifecycle/adapter/pump path without a second find_record. Capturing first then replacing
from message_start therefore uses the captured old record; replacing first cannot
execute either stale or replacement callbacks for that old intent. Ordinary Models
methods retain their current task-start lookup semantics. The accepted public request
helper keeps RequestInput/run_request; a private captured entry shares its body.
No capability enters a durable DTO, EffectPlan, or a ninth Effects method.

The full Model is an observed snapshot, paired with the registration and rechecked
outside the line when needed. The Provider baseline callback has no publication stamp;
this contract does not claim atomic freshness of arbitrary provider-owned baseline
mutation at the lane capture instant. Registry membership is the atomic boundary.
Do not impose new baseline immutability, cache it globally, invoke the callback under
locks, or widen Provider traits to invent that missing guarantee. This is the existing
snapshot API's limit, not another recovery-policy default. Advice654 records the exact
interface shape and evidence; root adopts its bounded provider-record mechanism.

### External completion and basic observation

A live drive retains its own operation identity. If that operation is legitimately
finalized externally, stop and signal only that drive, discard its uncommitted outputs,
read matching lane.lastResult, and dereference the final assistant entry when present.
Return that durable result without another write or a global owner fault; do not adopt
a newer operation found in the same lane. Missing/mismatched/malformed terminal data
is still a fault. This follows pinned H1964; broader S10 control/queue APIs stay staged.

R1-R4 execution emits its existing run/checkpoint/turn/step/tool/hook/sleep/event_handler spans with
explicit parent contexts and declared safe attributes from the pinned agent schema.
Real Harness provider requests use the existing pi.ai.request name; blocked/synthetic
work creates no provider or phase-two tool span. Parentage follows actual execution,
including retry, safe replay and close/fault settlement. Use existing SpanGuard and
TelemetryContext; no global active span or second telemetry contract. Full S13 catalog,
structural spans/exporters and later public routes are outside this correction.

RuntimeSession's additive commit_conditional_with_telemetry capability accepts a
borrowed explicit context while sharing the same checked publication and one
retrospective pi.session.write observation. Existing commit_conditional keeps its
construction-time context. NotApplied and validation rejection still create no write
span; do not duplicate observation in the runtime or weaken Session validation.
The runtime passes the actual current procedure context. Existing transaction
attributes/outcomes and no-payload rules remain unchanged.

## Actual request and shared tool paths

Requests use ModelsRegistry::stream_simple_with_lifecycle with real registry auth,
endpoint and captured adapter behavior. Materialize Session context, project custom
entries, transform_context, then convert provider messages in their specified order.
Use captured thinking/options and full before_request result; inject owner signal,
clock, supervised spawner and telemetry in RequestRuntime. Do not call retryAssistantCall:
Harness retries are the durable attempts. Payload hooks use the accepted lifecycle
bridge; after_response uses the actual terminal and sanitized passive metadata.
No-handler payload route performs no conversion. With handlers, checked JsonValue
conversion cannot round or silently omit data; an unrepresentable trusted adapter
payload faults the owner before it can bypass the registered hook. Synthetic
reconciliation invokes no response hook. Existing host header classification remains.

Three tool-phase helpers live in pi-agent-loop; runtime consumes them through one
explicitly reviewed internal runtime -> pi-agent-loop dependency. Full legacy Agent
remains S14. This avoids divergent preparation/validation/result behavior while the
two schedulers retain their distinct ordering. Phase1 applies deterministic
prepareArguments, actual argument validation, before_tool and replacement validation;
phase2 executes the bound tool and validates raw result; phase3 applies after_tool
and validates final result. Invalid raw/final data becomes a synthetic tool error,
retaining independently valid usage. Immediate outcomes skip execute and after_tool.
Callbacks/phase ports are process-local, not new durable DTOs.

Runtime resolves tool context once per live/restored batch, uses the same fifth
execute argument for all calls, persists complete effective args unconditionally,
and respects any sequential declaration. Harness clearance/intent/start is in source
order, execution may overlap, and finalize/message/result commits remain in source
order. Legacy compatibility preserves its separate preparation-before-execution and
completion-order hook/end-event behavior. Safe replay requires both captured and
current safe declarations and uses saved args without prepare/before_tool reruns.
Unsafe unknown effects produce interrupted results under reserved IDs. Genuine
length with calls executes none. Final settlement deletes the batch args and folds
the next checkpoint; only a nonempty all-terminating batch ends without an assistant.

## R1-R4 state obligations and recovery completion

R2 accepts normalized prompt/expansion, hook injections and captured pending input
in one transaction; missing identities or zero resulting entries reject before
writes. Generation start captures configuration/settings under both tokens. Ready
executes before_request; intent reserves R/U and output limits; output/usage/leaf/
latestAssistant/classification land in one settlement. May-finish runs before_run_end
and the complete terminal transaction, retaining lane-owned pendingNextRun.

R3 covers ready/pending/retry_wait before/after reopen, captured caps/backoff,
cancel-first normalization, provider-aborted-with-running rejection, ordinary
errors, stop, genuine length and initial deferred suspension. Unknown below-cap
pending advances to a later attempt; cap/cancel reconciliation uses reserved IDs
with synthetic zero usage. Only settled attempts require usage. Response-provenance
failure drain with empty inbox terminates failed; nonempty revival/drain remains
S10. Deferred polling and structural/overflow work remain S10/S11 explicitly.

The [recovery amendment](recovery.md) is the current authority for explicit first
attachment, no-ledger recovery validation and the unknown API fallback. All other
accepted runtime mechanisms stay unchanged.

Empty toolUse follows need_assistant(false): H1289's
tool route, H1317-1320's otherwise continuation and the accepted nonempty termination
guard exclude terminated_tools. Fold zero-call completion into assistant settlement,
using that assistant as trigger and creating no args/results or tool effects.
This derivation retains the Harness continuation and current validation; the legacy
loop keeps its separate zero-call shortcut.

Distinguishing obligations are actual public Memory+faux-Models runs, lossless
request hooks reaching provider input, bounded coherent restore amid unrelated
history/concurrent writes, wrong-key zero writes, dropped admitted waiter/close,
manual/automatic identical durable traces, every generation recovery position,
real context-bound tool calls, saved-args replay, source-ordered finalization and
complete terminal cleanup. Component tests alone do not establish these properties.
