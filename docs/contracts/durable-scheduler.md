# Generic Durable scheduler contract

The `pi-durable` crate is the generic persistent task owner. Its storage authority
is `pi-durable-session`; it reexports the accepted IDs, TaskRecord, TaskState,
TaskOutcome, JoinPolicy, Error and Transaction. Checkpoints, inputs, results and
memos use detached `serde_json::Value`. No provider, Agent, ordinary tool, Coding
or VM policy runs in this slice.

## Definitions, snapshots and admission

`TaskDefinition::new(kind, version, initial)` builds an immutable definition, with
`phase(name, handler)` and `abort(handler)` builders. Versions are positive. An
initial checkpoint and every runnable checkpoint require a nonempty `phase` string.
Handlers receive `(TaskRecord, Invocation)` and return a Send future with `Result<()>`.
The default abort handler commits an aborted outcome.

`Registry::install(registration, definitions)` atomically replaces one named
registration, rejecting duplicate task kinds across the resulting snapshot.
`uninstall` removes a registration. Existing RegistrySnapshots stay immutable.
Each phase gets a fresh snapshot. A same-version replacement takes the next phase;
a removed or incompatible replacement leaves an active invocation on its original
definition while refreshing its registry snapshot. Each newly resolved missing or
incompatible definition emits one `task_outdated` diagnostic per continuing
invocation; unchanged resolutions do not repeat it. Future reservations block when
no current definition matches. There is no migration API: older installed code
reports `TaskTooOld`, newer incompatible code reports `MigrationFailed`, and absent
code reports `MissingTask`.

`Scheduler::open(Session, Registry, SchedulerOptions)` uses an existing authoritative
Session and creates no conversation. Hosts initialize their conversation separately.
`Scheduler::create_task` and `create_task(&mut Transaction, &TaskDefinition, input,
TaskOptions)` use the same accepted Session TaskCreate path. TaskOptions chooses
an explicit conversation or owner task, background admission, restart abandonment,
trace and links. Task-owned work shares its owner's conversation and cannot be
background. An invocation-bound transaction defaults to its task's conversation
and stamps entries with its task ID.

A newly owned task or conversation requires a final staged owner that is live and
unmarked. Creating child work and deciding its parent's outcome in the same
transaction rejects atomically. Create work while keeping a live checkpoint,
then decide the outcome in a later transaction. Inspection never relaxes this rule.

## Invocation transactions and progress

`Invocation::identity` returns `{ task_id, number, mode }`, with Run or Abort mode.
Numbers distinguish native in-memory invocations; they are not a second persisted
lease field. `registry`, `signal`, `now`, `commit`, `transact`, `memo`,
`memo_or_insert`, `sleep_until`, `get_task`, `owned_tasks`, `outcomes`,
`wait_for_task`, `abort_owned` and `set_timer` bind to that invocation.

`commit(change)` synchronously calls `change(&mut Transaction, &TaskRecord)` on the
Session line and accepts `Result<Option<TaskState>>`. `transact<T>(change)` accepts
`Result<(T, Option<TaskState>)>` and returns T from the same batch. The scheduler
checks the invocation, current running state, normal-close seal and durable abort
mark before invoking user code. It then validates waits/sleeps and stages the task
transition atomically with the caller's other writes. Callbacks cannot perform
external effects, await or recursively call Session. No mutable draft escapes.
Raw Transaction::replace_task is scheduler-only; task consumers return their state
through these lease-checked operations.

Transaction table reads must precede the first table write. The scheduler captures
its graph before user writes and overlays detached `staged_tasks` and
`staged_conversations`; it never relaxes read-before-write. Document acquisition and
mutation retain their accepted Session semantics. A committed child creation waits
for one completed reservation pass when scheduling is enabled, then returns without
waiting for the child outcome. Paused admissions dispatch no handlers.

A phase must durably change its checkpoint, release its invocation into waiting or
sleeping, or decide an outcome. Empty commits, memos and document writes alone are
not checkpoint progress. An unchanged running checkpoint at handler return faults.
An uncaught handler error faults unless a previously committed waiting, sleeping,
completing or terminal state takes precedence. Once an outcome is decided, a later
handler error cannot replace it. Deciding an outcome drops memos and restart count;
terminal task-document retirement belongs to the same accepted Session batch.

First-writer memos distinguish absent from explicit JSON null. Concurrent
`memo_or_insert` candidates serialize through the Session line and return the stored
winner. Ended invocation writes reject; timers disarm and retained host callbacks
check the lease. Dropping a running handler future also invalidates its native lease.

## Ownership, waits and cancellation

The graph joins task ownership with owned conversations. Ordinary ownership walks
stop at background tasks; explicit background conversation abort crosses them.
A completed decision remains `completing` while ordinary work below it is live.
Its handler never reruns. Noncompleted decisions and abort intent cascade to
ordinary descendants. Abort handlers run after those descendants drain.

Waiting persists explicit task IDs and FailFast or AllSettled policy. Targets must
exist; FailFast targets must be directly owned by the waiter. Cycle checks include
both persisted wait edges and implicit owned-work holds, including conversation
ownership. AllSettled waits for every target; FailFast abort-marks remaining live
owned members after a noncompleted outcome. `Invocation::wait_for_task` is an
in-memory wait, creates no persistent edge, and rejects on invocation cancellation.
`outcomes` requires terminal tasks and preserves the requested order.

`abort_task` commits the mark before signaling a run, joins that run and returns
Marked or Terminal. A pending run join rejects with the actual Session failure if
storage fails, even when the handler ignores its signal. Normal abort and normal
close continue to join the handler; a successful Session close is not a failure
substitute for that join. An unavailable task with no invocation or owned live work can
orphan directly, preserving its original abortRequested value. Explicit abort
replaces a restart-abandonment mark; implicit owned cleanup preserves that mark.
`abort_conversation` marks the selected ordinary subtree in one batch and waits for
idle. Default idle/abort traversals exclude background subtrees.

## Recovery, sleep and close

Open reconciles interrupted running tasks to pending and increments consecutive
restart counts, preserving checkpoints, memos and startedAt. Pending, waiting,
sleeping, completing and terminal records retain their meaning. Default restart
limit is five. Reaching the limit parks a run until `retry_task` clears its count;
a state change clears the consecutive count. An abort handler repeatedly interrupted
at its limit faults with `Abort handler of task <id> (<kind>) was cut off <count>
times` and error detail `{ "reason": "restart_limit" }`. A successful decision
commit then emits one `task_faulted` report carrying that message; a failed or
ambiguous commit does not emit the success report. Abandon-on-restart work receives a Restart abort mark before
scheduling; missing definitions remain blocked until registration or explicit abort.

`SchedulerOptions::now` is an injectable millisecond wall clock. Sleeping persists
an absolute finite nonnegative deadline, releases the invocation and rechecks the
clock on wake. `Wake { active, at }` reports runnable work and the earliest future
sleep. `on_wake` replaces the scheduler's local sleep timer. `timer_host` can own
phase/sleep timers; native Tokio timers are the default. An early wake rearms against
the remaining absolute delay. Early/repeated resume does not dispatch a future sleep.

Normal `Scheduler::close` seals new Scheduler and Session admission on its first
poll, signals and joins active handlers, and privately resets surviving running
tasks to pending without incrementing restarts. Checkpoint/memo/startedAt and
admitted abort marks are retained. Both Session and Scheduler close settlement
survive cancellation of the caller's wait. `Session::close_with(join, finalize)`
is a sole-runtime composition seam: first poll seals document observers and Session
admission; its owned worker joins outside the line, drains admitted commits, then
runs a scoped synchronous finalizer through ordinary validation and storage commit.
Internal finalizer writes have no adoption/publication. Previously admitted normal
commits still reach complete commit subscribers before they end. Storage closes
last. First close wins; a failed Session skips the finalizer. Finalizer failure
fails closure, and an ambiguous backend error latches failure for reopen recovery.

## Runtime composition callbacks

SchedulerOptions::settle_outcome is an optional synchronous `(Transaction,
TaskRecord, TaskOutcome) -> Result<()>` callback for scheduler-written Faulted or
Orphaned outcomes at true terminal. It never runs at completing, or for normal
task-owned completed/failed/aborted decisions. `withdraw_inputs(Transaction,
ConversationId)` is an optional callback for conversation abort/cancelled ownership
cascades. Both default to no operation. They run after task candidates are staged:
document operations and known-ID settlements are supported; table rereads reject.
They must be idempotent where a cancelled scope is reconciled repeatedly. Failures
roll back the entire Session batch; an internal scheduler-finalization failure
stops scheduling and seals the Session, preventing handler replay.

SessionOptions::conversation_initializer is an optional synchronous callback over
the actual Transaction and new ConversationRecord. It runs for raw root, new and
fork creation after the row and fork copies are staged, before caller code
continues. Reacquiring an existing reserved root does not rerun it. It may stage
runtime-owned initialization without Session knowing document kinds. Callback
failure rolls back the row/documents and leaves Session usable. Table read rules
remain unchanged. Existing Session::open and with_clock default to no initializer.

## Evidence and practical limits

The source authority is immutable pi-mono commit
`4a080252f06caf63a525d7873c5d94d3c23887e0`. Native fixtures execute all 23 frozen
published 1.4.0 scheduler cases as 41 Memory/SQLite runs through public APIs, never
using expected values to drive execution. Native raw captures remain separate.
Comparisons retain complete records, per-task order and exact atomic batch
membership/count. Owner references must follow their owner's creation batch (or
share the same atomic batch), unless the owner is explicitly present in the input's
initial seeded/existing task set. An existing owner's later update is not treated
as creation. Only independent batch/event order and native representation
(JSON number spelling, typed close error and diagnostic API name) are projected.
Negative controls reject missing/coalesced batches and reversed dependent terminals.

The implementation rebuilds a detached graph on the Session line for each pass;
it does not claim the upstream scheduler's bounded incremental-index performance.
There is one Session/scheduler authority per store, with no distributed lease claim.
Tests cover real native runtime interruption and SQLite reopen; no OS-process kill,
provider effect replay, migration, browser backend or conversation-runtime integration
is claimed by this generic slice.
