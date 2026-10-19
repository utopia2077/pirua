# Durable context and conversation views

`pi_durable::context` and `pi_durable::view` provide committed read models over
`pi_durable_session::Session`. They accept the Session's detached record values and
use `pi_ai::Message` for typed model contributions. Reading or observing a view
creates no document, makes no provider request, and dispatches no task.

## Context

```rust,ignore
let context = pi_durable::context::read_context(
    &session,
    conversation_id,
    optional_visible_entry_id,
).await?;
```

`ContextView` owns its `head: Option<EntryRecord>`, raw active `entries`, per-entry
`contributions: Vec<Vec<pi_ai::Message>>`, and normalized `messages`. The method
returns `pi_durable_session::Result<ContextView>`. An explicit entry cutoff must be
visible from that conversation, including its inherited fork history. One Session
history read captures the immutable cutoff; later commits cannot change the result.
Returned values may be cloned or changed by the caller without altering Session
records or another read.

The newest head marker selects the range beginning at its `head`. Active entries
start with that marker, followed by the range's non-head entries in ID order.
Edits on every raw entry in the selected range count, including older head markers
that are absent from the active entries. The newest edit for a target wins.
Omitted entries contribute no messages; replacements contribute their replacement
messages. Raw entries retain their original committed contents.

Assistant messages with stop reasons `aborted`, `error`, or `deferred` contribute
no provider messages. Tool results follow their assistant in tool-call order; only
the first matching result before the next assistant is used. Orphan results are
omitted. A missing result becomes an error tool result with the call's ID/name,
the assistant timestamp, and `details.reason = "missing_result"`.

`context::order_tool_results(&[pi_ai::Message])` exposes that existing ordering step
as a pure synchronous function returning `pi_durable_session::Result<Vec<Message>>`.
It preserves the other message/system positions and performs no contribution
filtering or baseline-leading transformation. Compaction passes a flattened prefix
of the same captured view's `contributions`, retaining its resolved edits; rereading
context at an earlier entry cutoff would change which later edits apply.

The first system message moves to the front when only user messages precede it,
or when it is the original baseline message. A baseline is the first `pi.system`
entry after the selected head marker, or the transcript's first such entry unless
an earlier entry or replacement edit already carries a system message. Its original
entry/model position identifies it. A value-identical replacement edit has different
provenance and stays positional when an assistant precedes it. Omitting the baseline
does not promote a retained or later system entry into its place.

Stored JSON model envelopes are validated against the accepted `pi_ai::Message`
DTOs while projecting contributions. Invalid contributed envelopes return a validation
error; this read-only module does not alter Session health. The AI DTO's numeric
representation applies, including floating-point usage costs.

## Conversation mounts

```rust,ignore
let views = pi_durable::view::ConversationViews::new(session.clone());
let state = views.state(conversation_id).await?;
let snapshot = state.value().await;
let watch = views.watch(conversation_id).await?;
while let Some(frame) = watch.next().await? {
    // frame.value is an immutable owned revision; frame.ops rebuild it.
}
```

`ConversationView` owns its conversation record, active entries, and `docs`, a map
from document kind to detached JSON value. The mounted kinds are exactly the five
singleton current-version1 documents `pi.agent`, `pi.live`, `pi.inbox`,
`pi.provider`, and `pi.usage`. Absent documents remain absent. Family documents,
third-party kinds, and other scopes do not mount. Unsupported versions reject
acquisition or terminate an existing mount's delivery; no migration is performed.
The view module does not define these documents' runtime fields or initialize them.

A `ConversationViews` manager shares one mount per conversation while a state or
active watch holds it. Public watch clones share custody of their attachment;
dropping the last public clone releases it and stops future deliveries. Background
delivery and monitor tasks retain inner delivery state only, so they cannot keep a
mount attached after public custody ends. An already running callback may finish.
Dropping the last state/watch releases the mount; stopping, cancelling, or failing a
watch also releases that watch's attachment while retaining its last value. A later
acquisition rebuilds from committed Session state. Fork
baselines include inherited entries and copied independent document incarnations;
a fork follows only its own later publications.

Initial hydration and subscription use one atomic Session observation. Before
admitting its first public handle, a new mount awaits the projection driver's
adoption fence and checks its terminal state. If close sealed observation during
an already running baseline read, the closed queue is latched before that fence
acknowledges, and acquisition rejects. Later revisions use each captured immutable publication and its document values, including
copies. They never reread latest storage during delivery. Retirement removes only
the matching mounted incarnation, so retiring an old incarnation cannot remove a
replacement. Previously returned revisions remain stable.

A relevant commit emits at most one `ConversationViewFrame { seq, value, ops }`.
Document operations retain their decoded order and gain the prefix
`["docs", kind]`; a root replacement becomes a set at that prefix. Entry operations
follow document operations. A normal entry appends through an array splice. A head
marker splices out the current prefix and inserts the marker while retaining the
currently mounted non-head suffix at or after its target. As in the tagged mount,
a raw backward head does not restore entries that this mount already dropped. A
fresh mount can expose those entries again. Structural nonempty operations remain
publications even when the final value equals the previous value. Commits that
touch nothing mounted emit no frame.

The native state API awaits adoption: `ConversationViewState::value().await` drains
already queued immutable observation frames before returning `Arc<ConversationView>`.
Its `end()` reports the terminal reason once delivery seals. This is an owned
asynchronous Rust read surface; it does not provide a JavaScript proxy or mutable
shared snapshot.

A watch's `value()` stays at the acquisition revision until its first delivery,
then tracks the most recently delivered frame. `next()` serializes concurrent pull
calls. Alternatively, `start` installs one serialized async listener returning a
Session result; listener and pull modes are mutually exclusive. Listener rejection
or panic ends only that watch as `WatchEnd::ListenerError`; other observers and
Session commits continue. `stop`, `cancel`, `end`, and async `closed` expose explicit
lifetime control. The first terminal reason wins. An already running listener remains
caller-owned after termination.

Each watch retains at most 100 pending delivery frames, excluding a running listener
frame. On overflow, pending delivery frames coalesce into the newest complete
root-replacement snapshot, and later commits can queue behind it. This is bounded
convergence, not a lossless audit log. The shared mount eagerly projects a lossless
Session `ObservationSubscription` off the mutation line, independently of user
listeners. That internal observation ingress is unbounded and can grow if its
projection task is starved. The 100-frame bound therefore describes each watch
delivery queue; it is not an end-to-end retained-frame or memory bound, and does not
claim JavaScript scheduling/resource parity. Session close seals observation independently of already admitted
writes: no subsequent admitted publication enters the mount. Close clears undelivered
watch frames and retains the acquisition/last-delivered watch value and the latest
adopted state value. Document retirement does not close a conversation watch.

## Runtime event input

The runtime's crate-private `ConversationViews::attach_commits` attaches event
translation to this same mount. Its baseline pairs the current immutable view with
an immutable map of all current tasks in the conversation, across task kinds and
statuses. A command on the projection driver adopts queued Session observation
frames, then captures the view/task map and registers attachment delivery together.
Reusing a mount captures its current revisions, including task changes that emitted
no view operations; its original hydration baseline is not reused or reread from
storage.

Each ordered attachment frame carries `before`, `after`, the exact mounted `ops`,
and the complete original `CommitPublication`. Task-only, submission-only and
conversation-attributed task-document changes can therefore produce attachment
frames with empty operations and identical before/after views. A mixed publication
remains complete for the runtime to interpret. Ordinary view watches continue to
emit only nonempty operations. Event translation and event-batch convergence are
owned by the runtime and run outside the mount locks.

Attachment handles share explicit custody, release their mount on last drop or
stop/cancel, and retain their immutable baseline. Pull delivery is serialized.
Close/failure seals attachments at the existing Session observation cutoff, clears
undelivered frames and rejects pending attachment acquisition; an already admitted
commit after that seal does not reenter the attachment. Attachment ingress is
lossless and unbounded, alongside the existing possibly growing Session observation
ingress. The runtime's 100-pending-event-batch bound and the ordinary watch's 100-frame
delivery bound do not bound total pipeline memory. No general coalescing transport
or storage callback API is provided by this seam.

## Verification boundary

The context suite runs the pinned runtime cases
`runtime-context-edits-normalization` and
`runtime-context-baseline-identity-head-edits` through actual public Session writes
on Memory and SQLite. Comparisons decode expected contributions/messages into the
same accepted AI DTO domain, preserving raw records, ordering, head presence,
contribution cardinality, and baseline provenance.

The view suite projects the frozen
`runtime-tool-progress-mounted-view` committed publications into actual Session
document/entry writes and compares all eleven source mount batches, operations,
replayed values, and final view on both backends. The projection reserves IDs for
excluded scheduler/submission records and omits entry task attribution on both
sides. Source sequence gaps from those excluded records are compared as the source
touching-commit list, separately from native per-write sequences. This verifies the
read-model boundary; it does not claim provider, tool-progress credit, or scheduler
execution conformance. Independent native cases exercise real writes, copies,
retirement/recreation, backward heads, immutable cutoffs, close races, listener
isolation, structural no-ops, and 101st-frame convergence.
