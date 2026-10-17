# Current Coding extension binding

The headless Coding SDK uses `pi-coding-runtime::extensions::CodingExtensions`
with the current `coding.v1` profile. This binding targets Pi v1.4.0 at
`4a080252f06caf63a525d7873c5d94d3c23887e0`. It uses the existing length-delimited
protobuf Envelope, authenticated `HostAuthority`, exact capability intersection,
Ready handshake and schema-bound values. It has no dependency on the retired
Harness runtime.

A host first constructs `ApprovedProfile::coding(ProfileApproval)` entries for an
already authenticated peer, with the current session ID as their scope. It then
connects an implementation of `OrderedTransport` through
`pi_extension_protocol::host::HostConnection::connect`. Install an
`Arc<CodingExtensions>` with `CodingSessionBuilder::with_hooks`, build the session,
and call `CodingExtensions::attach` with the Ready connection and host-selected
registrations. Session construction first calls the defaultable
`SessionHooks::bind_session` lifecycle seam. A CodingExtensions object binds once
to the actual runtime instance, using process-local weak identity rather than the
persisted session ID. Reusing it with another builder or attaching it to another
instance is rejected before callback dispatch, including another live manager of
the same persisted session. Ordinary native host hooks keep their shareable default.

Retain the returned public attachment handle. Closing it or dropping its last
public Arc removes its tools and closes its connection immediately, including
while internal hook/tool work is in flight. Internal invocation state cannot retain
that public ownership lease. An unresolved call is retired, and queued late progress
is not published. A first terminal already admitted by structural/native validation
before retirement retains its ownership; it cannot authorize a different or disposed
captured session. Captured tool callbacks and snapshot actions
are invalid after session replacement, session disposal or explicit attachment
close. Attachments never retain a strong CodingSession reference.

`OrderedTransport` supplies an ordered, full-duplex byte channel with one receive
owner. Its receive operation must be safe to cancel because the neutral owner
selects between input and commands. Process creation, transport authentication,
OS isolation and credentials remain explicit host responsibilities. A peer's
manifest name never authenticates it or creates a tool/schema/grant.

The current profile is selected only at core/profile 1.1. Its canonical descriptor
is the checked-in `crates/pi-extension-protocol/schema/current/coding-v1.json`.
The profile digest uses the existing descriptor digest construction, with that
complete JSON descriptor as its descriptor bytes. Each projection additionally
hashes its complete type closure through RFC 8785 canonical JSON. Regenerate with
`python3 crates/pi-extension-protocol/schema/current/generate.py`; protocol tests
independently reconstruct and verify every projection digest. The constructor
rejects the legacy exposure registry rather than silently ignoring it.

The descriptor has 17 methods and 35 projection roots:

| Binding | Method names | Behavior |
| --- | --- | --- |
| Input/context/message hooks | `input`, `before_agent_start`, `context`, `message_start`, `message_end` | Detached native messages; transformations feed the same Agent dispatch, context, events and persisted transcript. Message role changes are rejected. |
| Tool hooks | `before_tool`, `after_tool` | Validated arguments may be explicitly replaced after initial validation. After-hook nullish fallback and content/structured-content invalidation follow the target source in registration order. |
| Session boundary hooks | `turn_end`, `agent_before_settle` | Ordered native drafts; target IDs must belong to the captured active branch, and native message content is validated before admission. The session commits the batch atomically. |
| Other headless hooks | `before_compact`, `before_tree`, `before_session_change`, `cache_warming_decision`, `prepare_loadout` | Closed current DTOs and native result validation; tool description/hiding edits remain within the current loadout. |
| Events | `event` | Source-ordered native SessionEvent delivery with an awaited unit acknowledgement. Model header values are omitted from copied ModelSelect events. |
| Remote tools | `tool.call` | Host-owned declaration plus exact portable progress/result schemas, streamed native AgentToolResult updates and one terminal. Explicit JSON null in raw details/structuredContent remains present. |
| Explicit host action | `session.snapshot` | Read-only current-session snapshot; requires an exact capability and `session.snapshot` permission scoped to the captured session ID. |

Application JSON is confined to native application-content leaves. These leaves
are not accepted as substitute DTO roots: all requests, progress and terminals
pass current structural projections and mandatory native validators. The Coding
adapter uses current AI/Coding deserializers, closed result-field inventories,
role checks, active-branch checks and captured session custody. Arbitrary custom
roles, Harness operation/lane/effect/task fields and unsupported method names are
not current Coding bindings.

Host-issued IDs are odd and never reused. Peer snapshot IDs are even and strictly
increase. The owner bounds request admission with the host's in-flight and
blocking-byte limits, bounds progress with aggregate byte credit, charges actual
Envelope body length. `HostConnection::request` releases credit after a synchronous consumer
acknowledges an update. `request_with_progress_credit` makes Release/Retain explicit.
Coding chooses Retain because ToolUpdate admission queues an asynchronous Agent
subscriber acknowledgement. Its total remote progress admitted per invocation is
therefore finite; exhausting this budget still permits terminal or cancellation,
and later replies cannot write another invocation. The connection's request-rate
limit and finite method, I/O and lifetime
deadlines remain host supplied. Cancellation is serialized with terminals so an
already validated terminal retains ownership; late and duplicate replies to
retired IDs cannot settle another call. Structural/native peer rejection and a
host validator fault remain distinct errors. Protocol/session locks are released
before calling native validators, progress callbacks or host actions.

The old seven-profile descriptor inventory remains in the protocol crate for
its existing framing, schema and cross-language regression tests. Current Coding
never grants those old payloads. The old `echo`/Harness route is replaced by
arbitrary host-approved remote tool declarations through the normal Agent tool
pipeline. Remote provider execution/catalog registration, resource discovery,
passive subscription cursors/resync, telemetry delivery and peer-initiated queue,
abort or mutation actions are not exposed by this current Coding binding. Native
Coding hooks provide boundary drafts; `session.snapshot` is the selected direct
peer action. JS/Jiti loading, CLI/TUI values, provider HTTP authorization and
credential brokering are outside this binding.

The focused verification commands are:

```sh
cargo test -p pi-extension-protocol --all-features --locked
cargo test -p pi-coding-runtime --test extensions --all-features --locked
```

Use the repository's configured build environment when running Cargo. The new
fixtures use real serialized framing, activation and peer transport ports, and
exercise both the neutral connection owner and the assembled Coding pipeline.
