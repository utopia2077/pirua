# Durable conversation runtime

`pi-durable::Durable` is the native conversation facade over one authoritative
`pi-durable-session::Session` and the accepted persistent `Scheduler`. It owns
admission, agent configuration, prompts, generation, local/nested tools, accounting,
compaction and committed agent events. Context normalization and mounted views
remain in their separate `context` and `view` modules. Runtime does not construct
an Agent or CodingSession. Ordinary Coding/MCP/native-extension adapters have their
own binding module and contract; persistent CodeMode execution is a later unit.

The selected source is Pi v1.4.0, exact Git object
`4a080252f06caf63a525d7873c5d94d3c23887e0`. Native types replace JavaScript object
capabilities and mutable Chord proxies. Raw Session records remain the persistence
boundary; no migration, historical import or JSONL path is introduced.

## Opening, initialization and custody

`Durable::open(storage, DurableOptions::new(models)).await` creates the Session,
installs coherent runtime definitions and opens the Scheduler paused. `root`,
`create_conversation`, `conversation`, scoped `commit`, task inspection/control,
usage, `wake` and `close` are public methods. Submitting/waiting/resuming enables
execution. Reads and inspection alone do not invoke handlers. `wait_for_idle`
waits for ordinary work; a background task can be awaited by its actual task ID.

The Session conversation initializer covers every raw, root and fork transaction.
It initializes live, inbox, usage, provider and agent documents, then calls the host's
`conversation_created` callback, before caller configuration/init. Fork copies
exist first. An owned fresh conversation copies its owner's stored agent; a fork
uses its as-of agent copy. Provider identity and usage start fresh. Phase reads do
not lazily repair missing legacy documents.

Five singleton version1 documents are public definitions. `pi.agent` uses
rewindable/asOf semantics. `pi.live`, `pi.inbox`, `pi.usage` and `pi.provider` use
latest/initial semantics. `pi.tool.nested-call` is a task-scoped family retained
until the parent task retires. Builtin task definitions are generation1, tool2 and
compaction1. Missing/incompatible definitions retain the Scheduler's blocking rules.

`Durable` clones are public runtime custody. `downgrade()` returns `WeakDurable`;
its `upgrade()` validates that the runtime still exists and is open. `binding_id()`
is process-local runtime identity. `ToolExecutionApi` and `HookApi` expose the ID
of their actual RuntimeInner, so a binding compares it with its captured weak
runtime before admitting a call. This number grants no peer authority. Connection
retirement and per-invocation liveness remain additional binding checks.

Closing uses the Scheduler's Session close hook. Already admitted backend work
settles independently of a dropped waiter; failure or ambiguous commit is fatal.
No callback may convert that failure into a successful tool result. Snapshot,
observation, record and execution cutoffs remain the underlying Session/Scheduler
contracts. Normal close/reopen recovery is covered; OS/process-crash behavior is
not inferred from that evidence.

## Registry and agent resolution

`RuntimeRegistry::install(Extension)` replaces one named extension, and `uninstall`
removes it. A registration owns tools, ordered prompt sections, seven hooks,
wrappers and generic task definitions. Updates validate the complete candidate
and atomically publish one named generic definition set. Each captured phase sees
one immutable complete snapshot for tools/agent/sections/hooks/definitions.

The `models` and `settings` callbacks are current host providers. A phase resolves
and caches its agent once, using its immutable registry snapshot and stored
configuration. Model/settings accesses prescribed as current by the source remain
current. Configuration supports model, thinking, extension/tool/model-tool
selection, instructions and cwd. Missing selected extensions are ignored; duplicate
selected names collapse while registration order is retained. Invalid wrappers are
reported without executing foreign code under a registry lock.

## Admission and generation

`Conversation::submit(SubmissionDraft)` returns a durable `Submission` receipt.
Inputs support request IDs, user content, busy steer/followUp/reject policy and an
optional custom entry. Writes carry a complete Session `EntryDraft`. Reusing a
request ID returns the original same-type submission even when the later content
changes; a different type rejects. Display-only writes append immediately. Busy
context writes and queued inputs settle at the selected generation boundary.

The boundary applies ordered writes before selected user inputs, handles stale
writes and final resets, then places/settles receipts atomically. Queued withdrawal
removes the exact inbox position. `reset` writes a new head marker and optional
handoff. Submission status/wait/abort use committed records. Wait registers the
commit stream before rereading status, closing the read/register race.

Generation pins cutoff, model, thinking and curated stream configuration before
request dispatch. `timeoutMs` is the declared timeout property. Provider session
identity is read from the persisted ProviderDoc at dispatch, matching tagged
request behavior. Current transient auth is resolved by the existing ModelsRegistry.
Persisted public headers pass `RawModelHeaders` hard denial plus an explicit host
`ModelHeaderPolicy`; the default policy denies headers. Restored configuration is
classified again. Transient auth headers are not written into a document.

Before-request hooks run for each actual attempt against pinned context. Recovery
converts a committed partial to an aborted assistant entry and accounts for its
usage, then repeats the pinned request and hook chain. Partial pacing uses actual
invocation timers. At most one partial commit is in flight; it settles before the
terminal assistant, usage, live clear, receipt or tool-task decision commits.

Retry and deferred polling use persisted absolute deadlines, recheck early wakes,
and read current retry/compaction policy when classifying outcomes. Deferred fetch
and cancellation use the shared AI identity merge contract. Retry, poll, final
answer, tools, yield continuation, cancellation and failure all use durable task
transitions. A yielded continuation keeps its original inputs open until the final
answer. Model calls are limited to declarations actually offered in the transcript.

## Tools, progress and nested calls

A `ToolRegistration` contains its AI declaration, replay policy, callers, execution
mode, output limits, optional structured-output schema, optional argument repair,
and an async `ToolInvocation -> ToolExecutionResult` callback. Initial execution is
repair, validation, ordered before-tool hooks, revalidation, then one committed
execute intent containing detached final arguments and stored replay policy. Effects
begin after that intent. Recovery reruns only when stored and current policies are
both safe. Unsafe/interrupted calls report their durable partial output; no
exactly-once effect claim follows.

`ToolExecutionResult` keeps output, structured_output, details, diagnostics, usage
and control separate. `Presence` preserves missing versus explicit null JSON.
Reported output/details/diagnostics supply final fallbacks before after-tool hooks.
Native owned `after_tool` returns `None` to preserve the result and output
provenance. `Some` is an explicit replacement and clears retained-output provenance,
including a value-equal output. Harness truncation diagnostics are added after
hooks only for retained provenance, or for independently bounded explicit output.

Head/tail bounds preserve whole lines where possible and split long lines only at
scalar UTF8 boundaries. Byte output decodes incrementally, a first BOM is dropped,
and incomplete bytes flush at text/skip/end boundaries. Skipped output is tail-only
and keeps whole-stream dropped counts. Running display output removes invalid
control characters. Final output is bounded independently. Model structured-schema
faults drop the structured value and report the broken extension; nested faults
become error results. A schema-less nested call returns its bounded output value.

One accumulator owns progress. `output` and `diagnostic` mark updates synchronously;
`details(...).await` and `report_progress(operation).await` acknowledge a Session
commit covering the actual change. They never acknowledge merely enqueued work.
The first update captures immediately, later writes coalesce, and pacing uses the
invocation's host timer and wall clock, with an early-timer recheck and a byte-rate
floor. Terminal settlement joins admitted writes. Nested progress=false suppresses
persisted running data while retaining the complete terminal result. An adapter
must observe that disabled option rather than send report_progress operations.

Tool APIs expose actual invocation identity/cancellation, immutable registry,
current models, cached agent, prepared environment, clock, guarded commit/memo,
document reads/watch, task creation/reads/waits, and a bound conversation handle.
Retained tool APIs cannot admit work after execute settles. Bound submissions and
receipt operations retain the same invocation guard. Scoped writes recheck the
invocation on the Session line. Hook APIs expose the corresponding guarded native
phase operations; they are full local capabilities, distinct from the narrower
remote extension profile.

`execute_tool(name, args, NestedToolOptions)` persists key/name/args/task/result
identity. An equal key reattaches only to the same name and arguments; a mismatch
rejects. Default keys follow invocation call order. The task, family member and
live slot commit together. An unsafe caller defaults owned children to
abandon-on-restart. Parent settlement joins underway nested admissions, aborts all
remaining owned nested calls, then commits result/usage/slot cleanup/receipt
atomically. Nested children finish before their parents.

## Usage and compaction

Assistant and tool usage update per-model/per-tool ledgers in the same commit as
their result entry or nested result. Session totals aggregate conversation ledgers.
Forked usage starts fresh. Summarizer spend counts even when a reset makes its
summary write stale. Safe count/finite-cost arithmetic rejects invalid totals.

Manual, threshold blocking/background and overflow compactions use real persistent
compaction tasks. Selection preserves a safe recent boundary and runs the accepted
`context::order_tool_results` helper on the selected contribution prefix. Hooks may
decline or provide a summary. The summarizer uses pinned context/model/thinking/
options and the exact source prompts. Retries persist deadlines. Successful summary
placement uses the selected head and retains raw history; concurrent resets use the
normal stale-write admission rule. Live compaction status and failure are committed.

## Views, events and telemetry boundary

Conversation context, view_state and watch consume the independently owned context
and mounted-view modules. Agent event attachment uses their private atomic current
view/task baseline and complete ordered commit frames, including ops-empty task,
submission and nested-result publications. Translation runs outside owner locks.
Held completing generations are captured at attachment, so later terminal release
does not emit a second turn end. Message and tool deltas use actual mounted ops.

`AgentEventStream` exposes the detached snapshot, serialized `next` or one async
listener, stop/cancel/closed and committed batches. AgentEvent is a transparent
JSON event DTO using the release vocabulary. Public-handle custody is separate
from translator/listener workers: dropping the last public handle releases the
attachment even during a callback. Listener failure ends that stream. At 100 pending
batches, undelivered delivery converges to one newest snapshot. This bounds delivery,
not lossless ingress memory or scheduler latency.

The tagged public TaskGraphView state/watch is not implemented. Native inspect,
get_task and Session record observations provide their documented diagnostic and
record APIs; the scheduler's internal graph is not that public graph view. The
Durable-specific `DURABLE_TELEMETRY_SCHEMA` submit/phase spans and report/phase/
active-task metrics are also not implemented. Hosts can inject actual shared AI
telemetry through `DurableOptions.request_runtime`; its default is noop. The runtime
forwards that RequestRuntime to provider calls, including summary/fetch/cancel.
`on_report` is reporting, not a substitute for schema telemetry.

## Evidence and native comparison

The frozen release runtime corpus executes 18 cases/30 backends. An independent
producer corrected the originally undeclared timeout input to timeoutMs, preserving
its preimage and proving every other case unchanged. Runtime fixture SHA256 is
`690a058107ef2744702d713593f3b6cd24077783cda6f888fb18edffa9f14c6a`.
The validation/revalidation source case explicitly selects sequential execution.
A separate native gate test verifies default parallel model tools: raw entries follow
completion order, while context, provider requests and afterTools retain call order.
The native consumer dispatches only input actions through actual APIs, captures full
requests/hooks/effects/receipts/publications and independently replays mounted and
document operations. It never uses expected values to build native state.

Approved projections cover JSON numeric spelling, atomic Session change membership
and replay-equivalent proxy operations with nonempty structural batches preserved.
The proven integer TypeBox/native-jsonschema diagnosis line projects to the same
path/type metadata while preserving the tool header and original invalid input.
Arbitrary-schema English equivalence is not claimed. Exact request fields use the
native AI DTOs and resolved public headers; no timeout alias or synthetic identity
projection is used. The fixture host has one async execution loop and deterministic
wall/monotonic time/ProviderDoc identities, matching the source host boundaries.
