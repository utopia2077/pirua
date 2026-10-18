# Durable Session native contract

`pi-durable-session` owns committed records, documents, storage, history, forks and
observation. A later runtime owns admission policy, task definitions/execution,
invocation leases, provider/tool interpretation, context normalization and mounted
conversation-view projection. These authorities do not call Agent or CodingSession.
The selected source is Pi 1.4.0 commit
`4a080252f06caf63a525d7873c5d94d3c23887e0`, principally Durable `types.ts`,
`docs/spec.md`, `storage/{memory,scan,sqlite/*}` and `session/*`, plus the decoded
Chord delta module. JSONL, migrations, browser stores and full Chord are excluded.

## Records and IDs

`ConversationId`, `EntryId`, `TaskId`, `SubmissionId` and `DocumentId` are distinct
positive safe-integer types. They share one global ordered namespace. Root conversation
ID 1 is reserved; allocation begins at 2. Manual stored IDs raise the allocation high
water. An allocation candidate is never reused even if its transaction rolls back or
it is never stored. SQLite reserves each candidate in its own immediate transaction
before returning it. This deliberately strengthens tagged SQLite's implementation,
whose allocation high water is persisted only by a later commit.

`Seq` is a distinct nonnegative safe-integer type. Zero is a valid historical cutoff;
backend commits begin at 1 and strictly increase. It is not an entity ID. `Session::open`
attaches to a store without an implicit write. The higher runtime initializes the
reserved root with idempotent `Transaction::ensure_root_conversation` before accepting
root conversation work. Session-scope document transactions do not need a root record.

`Session::with_options` accepts a clock and an optional synchronous
`conversation_initializer`. It receives the actual transaction and newly staged
conversation after any fork document copies, before caller initialization continues.
It runs for raw, root and fork creation; reacquiring an existing root does not rerun
it. Callback failure rolls back the creation without failing a healthy Session.
Default `open` and `with_clock` have no conversation initializer.

Conversation identity/ancestry/ownership and entries are immutable. Task creation and
ownership and submission identity/request ID remain immutable across replacements.
Task state is the complete tagged pending/running/waiting/sleeping/completing/terminal
variant. Outcomes retain completed/failed/aborted/orphaned/faulted meaning; live memos
and all optional JSON values preserve missing separately from explicit JSON null.
AI messages and telemetry/checkpoint payloads are detached JSON envelopes; typed AI
validation belongs to the consuming runtime. Submitted inputs and passive writes use
different legal state combinations, enforced before Session admission.

## Storage and schema

`Storage: Send + Sync` is a synchronous fault boundary with atomic `commit`, durable
`mint_id`, complete lookups, conjunctive filtered ordered scans, fork-aware entry
visibility/latest-head lookup, conversation-scoped request lookup, exact-address and
incarnation document reads, and close. Returned and retained DTOs are detached.
Continuation cursors are opaque and bound to the scan/query/order. A continuation may
inherit its order, but a conflicting explicit order or a foreign/malformed cursor
rejects with `ErrorKind::Request`. All later operations reject after close.

Memory uses ordered record maps and owner/status/request/address/scope indexes.
SQLite uses `pi_sqlite::Database`, component `durable-session` schema version 1,
separate tables for meta/reservations, global ID ownership, conversation/entry/task/
submission/document records and per-incarnation base/delta revisions. It indexes owner
conjunctions, entry ancestry/head segments, task/submission filters, request IDs,
document scopes/addresses and historical bases. Open checks/initializes schema without
loading documents. Exact document reads select one incarnation's last applicable base
and its tail. Commit validation loads only affected identities/documents/copy sources
and potential address/request conflicts, then writes one atomic SQL transaction.
The local rusqlite dependency enables its existing `fallible_uint` conversion feature;
all dependency versions remain unchanged.

Creation requires a base. Delta versions match the previous base/tail; a base may
start a new stored version, but typed definitions do not migrate or convert it.
One batch may contain at most one content command and one retirement per incarnation.
Content and metadata validate before same-batch retirement. Only one incarnation is
alive at an address. Rewindable revisions remain available; latest-only documents
reject historical content reads and discard obsolete bases/content at retirement.
Definition-free copy reads committed pre-batch state, matches kind/key/history/fork,
and rejects any same-batch source mutation. The backend trusts Session for semantic
record references and ancestry; it does not run definition code. The implementation
also naturally validates supplied raw deltas before commit. Tagged Storage permits
some trusted-input misuse and reports it on materialization; those corrupt-input traces
are explicitly outside common conformance.

## Transaction and failure boundaries

`Session::commit` and `commit_scoped` are async. A callback has type
`FnOnce(&mut Transaction<'_>) -> Result<T> + Send + 'static`, with owned `T: Send + 'static`.
It runs synchronously on an admitted blocking worker. One semaphore serializes all
Session mutation and atomic read/observation acquisition. Dropping an unadmitted wait
runs no callback and writes nothing. After admission, the worker owns the permit and
finishes callback/preparation/storage/adoption even if the caller drops its wait.

The callback reads tables before its first table write. Document reads see staged
changes and repeated acquisition returns the same incarnation ID. `doc` acquires a
scoped document, `document_value` returns a borrowed read-only value, `apply`/`apply_all`
admit exact decoded operations, and `mutate` records scoped edits with harmless scalar
and cancelled append/pop normalization. Preparation never compares only final document
snapshots: positional removals/reinsertion remain nonempty changes even if values
compare equal. A transaction mixing exact and convenience operations preserves the
exact operation sequence. No mutable draft can escape the callback.

Definitions supply kind, version, lifetime/history/fork semantics, optional family key,
initial(seed) and a required fallible checkpoint predicate. Initializers run only for
absent incarnations. Ordinary nonempty changes run checkpoint policy after all batch
validation, using the stored number of deltas since the last base. Creation is a base
and does not call checkpoint policy; empty preparation does not write or publish.
Callback/definition/validation/checkpoint failure before backend admission rolls back
without poisoning a healthy Session. Panics in callback or preparation are contained
as pre-admission validation errors.

Every backend commit or allocation error fails the Session, including an error returned
after the backend has already committed. Unexpected backend read failures also latch
failure; request/typed-definition errors do not. Failure seals admissions, ends
observation and closes storage. It publishes no candidate revision. Future writes and
reads reject. Reopening reads actual durable state; the failed Session never retries.
A successful batch constructs one complete immutable `CommitPublication` and adopts it
without further storage reads or user callbacks. Listener errors cannot undo it.

`TransactionScope` supplies conversation defaults and entry task attribution. The narrow
`replace_task` seam is for trusted runtime code after it validates its invocation lease.
Session checks immutable task fields, stamps first running/terminal lifecycle times,
clears terminal memos and retires every task document atomically at terminal settlement.
New owned work is rejected if the owner's final candidate is completing/terminal or
abort-marked. The scheduler, waits, restarts, abort propagation and lease policy are
outside this crate.

`close()` seals when first polled, ends state/watch delivery at that seal, settles
already admitted commits, and closes storage. A state retains its last pre-seal value
even if an admitted backend commit settles during close. Existing complete commit
subscriptions remain attached through that successful settlement and receive its entire
publication before their stream ends; they have a later cutoff than document states and
watches. Failure still closes the commit stream without publishing a failed candidate.
The close worker also survives a cancelled waiter. `closed()` distinguishes normal
closure from a failed Session.

The execution owner can compose closure through `close_with(join, finalize)`.
Admission seals on the first poll; the owned close worker awaits `join` outside
the mutation line, then validates and commits the synchronous finalizer's writes
without adopting or publishing them. Ordinary admitted commits still publish
before their subscribers close. A failed Session skips the finalizer; finalizer
failure fails close. First close wins. The scheduler uses this seam to join handlers
and reset surviving running tasks without exposing a new public runtime revision.

## Operations and native JSON

`Op` serializes the exact decoded tuple grammar `r/s/d/a/t/p/m`: root replace, path
set/delete, string append/prefix trim, array splice and permutation. `Segment::Key`
and `Segment::Index` distinguish object keys and array indexes. Applying a batch is
atomic on a detached value. Paths reject prototype segments, string array indexes,
sparse growth, missing intermediates and malformed permutations. Array deletes compact
positions; splice indexes/delete counts use JavaScript clamping.

Prefix trimming counts UTF-16 code units. Valid non-BMP strings retain source behavior.
A cut inside a surrogate pair rejects before Session storage admission. Unpaired
surrogates are outside Rust's Unicode scalar JSON domain. The independent source oracle
uses a lossless trace marker for that outside-domain observation, not an extra product
JSON value. No wire dictionary, interning, JS proxy or generic distributed-object host
is implemented.

## History, forks and immutable observation

`history` reads all visible entries with optional inclusive cutoff. `active_entries`
returns the newest head marker first, then visible non-head entries at or after its
`head`. These are raw records, distinct from edited model contributions and normalized
provider context, which belong to the later runtime.

A fork inherits entries through its selected visible entry, including ancestry cutoffs.
As-of documents come from the entry's owning conversation at the final state of that
whole commit, independent of intra-commit entry position. Current-policy documents come
from the requested parent at current committed state. Initial-policy documents remain
absent until acquired. Unknown definitions and keyed members copy without definition
code. Copies have new independent IDs. Task records/task docs and session docs do not
copy. Changing a selected source, or creating/changing a current-policy source in the
fork transaction, rejects before storage admission. `snapshot_as_of` resolves the
incarnation alive at the entry's commit and never follows a replacement address.

Document state/watch acquisition resolves a concrete incarnation and registers baseline
atomically on the Session line. Snapshots are owned JSON; retained observation/publication
values are `Arc<Value>` with no interior mutability. `DocumentState` advances synchronously
with local sequence numbers starting at 0 and provides an exact `StateSubscription`
with atomic baseline/registration. Retirement delivers root null and ends that incarnation.
Close retains the last value. Recreation requires a new acquisition.

`DocumentWatch` retains its baseline before delivery, supports serialized pull `next()`
or one async `start` listener, and advances `.value` immediately before delivery. It
buffers at most 100 pending frames, excluding a callback in flight. Frame 101 replaces
the pending suffix with the newest exact root snapshot; later exact frames continue.
Retirement's null frame settles its listener before `Retired`. Stop/cancel/close prevent
new callback starts, discard pending frames and settle a first-wins terminal reason;
an already-running callback remains host-owned. Listener failure ends only that watch.

`subscribe_commits` returns immutable complete commit publications and drains already
admitted successful commits during normal close before ending. `observe` atomically
captures selected document scopes/addresses plus an optional conversation/active-entry
baseline and registers its stream. Every relevant queued `ObservationFrame` contains
that commit's complete final selected snapshot, including definition-free copy values
and retirement, captured before another commit changes latest-only content. The runtime
may project/mount those frames and prefix document operations later without re-reading
current storage or running foreign callbacks on the Session line. The source mutation
line captures internal immutable data; all host delivery happens afterward.
