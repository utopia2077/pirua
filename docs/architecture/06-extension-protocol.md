# Language-neutral Extension Protocol

## Core rule

Rust does not call foreign-language functions through an embedded runtime or
FFI callback. It invokes registered capabilities over a bidirectional protocol.
Rust owns durable state, ordering, retry, recovery, authorization, and final
settlement. An extension owns only the external effect it implements.

The protocol can preserve callback behavior that is representable as messages:
typed arguments/results, ordered hook transforms, streaming progress, provider
events, cancellation, host requests, and passive subscriptions. It intentionally
cannot preserve function/object identity, closures, prototypes, shared mutable
objects, cycles, synchronous stack reentrancy, captured Agent/Session objects, or
arbitrary TUI values.

## Trust and deployment boundary

`pi-extension-protocol` accepts an already ordered byte transport plus four
host-established facts:

```text
authenticated peer identity
connection generation
granted capability set
finite ProtocolLimits
```

The manifest's self-declared extension id is metadata, never authentication.
The 15-crate workspace does not claim process launch/termination, transport
authentication, OS sandboxing, environment/network isolation, or secret
brokering. A privileged host must establish those properties before constructing
an endpoint. Closing an endpoint makes its bindings unusable; core correctness
does not depend on killing the foreign process.

## Capability profiles

V1 defines independently negotiated profiles:

```text
tool.v1
hook.v1
provider.v1
event-sink.v1
resource.v1
host-action.v1
telemetry-sink.v1
```

UI component factories are not a core profile. A future declarative UI protocol
may be versioned independently.

Each capability declares:

```text
kind
name
profile major and supported minor range
handler/executor id
schema digest
replay contract
execution mode
```

Internally, capability identity is fully qualified by authenticated extension
identity. Global aliases are configuration, not identity. Conflicting required
aliases fail activation atomically.

## Protocol and schema evolution

The ordered physical protocol is length-delimited Protocol Buffers. Protocol
versions are `{major, minor}`: a major may break compatibility; a minor is
additive only. Capability profiles negotiate separately as
`{name, major, min_minor, max_minor}`. Unsupported required profiles fail the
whole activation; unsupported optional profiles are omitted from the grant.
Different selected profile minors may coexist on one connection.

Within one major version:

- Protobuf field and enum numbers are never reused.
- New fields are presence-aware optional fields or new `oneof` arms with a
  specified absent default.
- Unknown optional fields are ignored. Unknown closed discriminants produce
  `unsupported_variant`; they never alias an existing default.
- Open string identities remain round-trippable.
- A sender emits only the negotiated profile minor. Required information is
  never silently stripped to force a downgrade.

Checked-in canonical `FileDescriptorSet` bytes are the authority for each
selected core-profile minor. Version bytes are the ASCII decimal
`major.minor`; a supported range is never itself a profile version:

```text
SHA-256(profile_name || NUL || ascii_major_dot_minor || NUL || descriptor_bytes)
```

Dynamic tool/payload schemas use SHA-256 over RFC 8785 canonical JSON. Duplicate
keys, non-finite numbers, remote references, unsupported keywords, or values
outside the portable JSON Schema subset reject before hashing. `$ref` may target
only bundled `$defs`. Generated SDKs embed the published digest; generated code
is never a second schema authority.

## Manifest and activation

```text
ExtensionManifest
  declared_extension_id
  extension_version
  protocol_range
  required_capabilities[]
  optional_capabilities[]
  requested_permissions[]
  event_subscriptions[]

CapabilityGrant
  authenticated_extension_id
  selected_protocol
  selected_capabilities[]
  granted_permissions[]
  limits
```

The host-supplied identity/grant is an immutable ceiling. Negotiation computes:

```text
selected capabilities = requested ∩ core-supported ∩ host capability ceiling
selected permissions  = requested ∩ host permission ceiling
```

A required capability or permission outside either ceiling fails activation
atomically with `required_capability_denied`. An optional one is omitted. The
peer cannot widen identity, scope, grants, or limits during handshake.

Activation is:

```text
connected
  → host-authenticated
  → Hello / HelloAck
  → Manifest / CapabilityGrant
  → Ready
  → active
  → draining
  → closed
```

Manifest contents and grants are immutable for one connection generation.
Reload creates active generation `G+1` and places `G` in draining state. `G`
receives no new invocation. A blocking invocation bound to `G` remains valid
through receipt and validation of its first terminal, then enters
`terminal_received` while retaining `{binding, validated_output}`. The binding
retires only after durable settlement commits or core definitively proves that
current state no longer expects that effect. Explicit abandonment, drain
deadline, or connection loss before terminal instead produces `peer_lost`.
Frames after the first terminal are stale, but the retained first output remains
eligible for settlement. “Older generation” alone is never stale. Passive
subscriptions move to `G+1` immediately.

## Durable identity and live correlation

```text
EffectKey {
  session_id, lane, operation_id,
  effect_kind, step_or_task_id,
  logical_attempt, item_index?
}

CapabilityRef {
  authenticated_extension_id,
  kind, name, profile_version { major: u32, minor: u32 },
  schema_digest, handler_id?
}

InvocationBinding {
  effect_key, capability_ref,
  connection_generation, invocation_id
}
```

`EffectKey` is durable and reconstructed from current session state.
`InvocationBinding` is process-local and exists only for one live dispatch.
Settlement requires both the exact live binding and durable state still awaiting
the exact effect key. A mismatch, duplicate terminal, missing operation, or
externally finalized operation commits nothing. Provider retry uses a later
logical attempt. Safe tool replay keeps the original effect key and receives a
new live binding.

After connection loss, drain deadline, or endpoint close, unresolved bindings
that have not entered `terminal_received` produce `peer_lost` and follow their
ordinary durable unknown-effect recovery policy. A retained terminal no longer
depends on the connection. A reconnect never inherits a live binding implicitly.
For the first `tool.v1` runtime route, preterminal `peer_lost` on an accepted,
still-current `ToolEffectPlan` returns D04's process-local, content-free
`HarnessRuntimeError::PeerLost` without publishing or committing a synthetic
tool result or durable terminal. Its `EffectPending` state and saved effective
arguments remain if that exact operation/key is still current; external
finalization follows existing no-longer-expected reconciliation. Before hooks
or replanning, a Safe tool's resume promptly returns `PeerLost` while no
host-approved replacement endpoint exists, releasing any newly acquired drive
reservation. Once a replacement is available, it reuses the exact durable
`EffectKey` with a fresh live binding. A Never tool follows conservative
interrupted recovery without a second external execute. A terminal already in
`terminal_received` remains eligible for checked settlement or definitive
no-longer-expected retirement despite later loss.

## Envelopes

Both peers may send requests after activation:

```text
Request {
  request_id
  parent_request_id?
  capability_ref
  effect_key?
  method
  deadline_after_ms?
  initial_stream_credit_bytes?
  payload
}

Response { request_id, result | error }
StreamItem { request_id, sequence, payload }
StreamEnd  { request_id, result | error }
Cancel     { request_id, reason }
Credit     { request_id, additional_bytes }
GoAway     { reason, retryable }
```

`request_id` is also the invocation id. Unary calls terminate with one
`Response`; streaming calls terminate with one `StreamEnd`. Sequence starts at
zero and is contiguous. A streaming request grants
`initial_stream_credit_bytes` before the producer emits progress; omission means
zero. Each `StreamItem` charges the exact transmitted byte length of its
length-delimited protobuf frame body, excluding only the length prefix.
`Response`, `StreamEnd`, and `Cancel` consume no credit. Credit and increments
are unsigned 64-bit values, use checked addition, and cannot exceed the
negotiated per-invocation maximum. Overflow/excess is `credit_violation` and
closes the malformed generation.

`deadline_after_ms` is an unsigned relative duration, not an epoch. At decode,
the receiver adds it to its own monotonic clock. Omission selects the finite
method default. The effective deadline is the minimum of request, method,
connection, and parent remaining duration, using checked arithmetic capped by
the method limit. Peers never compare wall clocks.

| State | Input | Result |
|---|---|---|
| `open` | contiguous item with sufficient credit | accept and decrement credit |
| `open` | local cancellation | send `Cancel`; enter `cancel_requested` |
| `cancel_requested` | contiguous item with sufficient credit | validate the exact request, sequence and selected payload; decrement credit and advance sequence, suppress progress delivery, remain `cancel_requested` |
| `open` or `cancel_requested` | first terminal | validate and retain binding/output in `terminal_received` |
| `terminal_received` | settlement commits or state no longer expects key | retire binding; enter `retired` |
| `terminal_received` or `retired` | any later item/terminal | stale violation; retained output/state unchanged |
| live | loss/deadline/endpoint close | `abandoned`; durable recovery decides replay |
| `open` or `cancel_requested` | gap, duplicate, credit overrun, invalid payload, or wrong binding | fail invocation and close malformed generation |

After local cancellation the host grants no further progress Credit. Previously
granted credit still charges valid in-flight `StreamItem` frames, which are
validated and discarded without progress publication; the first terminal
remains eligible until the effective deadline or cancellation grace expires.

Terminal payloads contain complete assistant/tool/hook results. Durable
settlement never depends on consuming progress. Cancel/terminal wire races use
arrival order, but current durable Harness cancellation remains authoritative
when settlement is planned. Capacity reserves one control and terminal slot per
negotiated in-flight invocation; progress cannot consume that reserve.
`terminal_received` retains at most one bounded validated terminal per binding.

The first `tool.v1` tool-only route selects exactly one host-approved `echo`
capability and admits at most one live outbound invocation. A grant outside
that shape is not activated by this route; a later multi-tool or parallel
route must define per-capability selection and ownership before widening this
restriction. For this first route, validate a peer-originated Request's
nonzero ID, nonempty method, required `CapabilityRef` shape and authenticated
principal before classifying its method or reserving a denial. The principal
must equal the host-authenticated peer; a wrong principal or contradictory
known profile/kind pairing closes the malformed generation **without a
Response** for every Request. A reference naming a profile selected on this
generation must also use that profile's selected major/minor and canonical
descriptor digest; a mismatch closes without a Response. A structurally
complete reference to an omitted or otherwise ungranted capability starts no
work but remains eligible for a fixed denial. For a known omitted profile with
a canonical descriptor at its claimed major/minor, require that descriptor's
full 32-byte digest; a mismatch closes without a Response. If that claimed
version has no canonical descriptor, the structurally complete reference
remains denial-eligible but has no known method. An unfamiliar optional profile
name is not malformed solely because it is unfamiliar and likewise has no
known method.
Determine method existence only from the referenced profile's descriptor at
its selected minor, or its claimed minor for a coherent known omitted profile.
A known but ungranted peer method reserves one fixed `forbidden` response; an
unknown method reserves `unsupported_method`. Either denial uses reserved
control capacity, performs no Session work and closes the route afterward.
Once that first denial is reserved, later coalesced peer Requests do not change
the already closing route or reserve another response.

If an unrelated outbound tool invocation is still preterminal when its
deadline arrives **after** that denial was reserved, mark only its binding
`peer_lost` and wake its waiter immediately; its deadline alone does not
discard the denial. A first terminal retained earlier remains eligible for
checked settlement. An outbound Request write starts when the sole ordered
writer irrevocably admits its head-of-line frame using a final receiver-local
monotonic sample **below** the invocation deadline, serialized with that
binding's expiry decision. Admission may precede `write_frame` and the first
byte. This host clock sample must finish in bounded time without blocking,
reentering the owner/route, or performing peer/channel I/O; the deadline timer
must wake independently of a pending write. A host unable to provide these
clock properties cannot activate this route. An expired Request that was not
admitted is never sent. An admitted frame cannot be silently dropped or
overtaken because its invocation expires; it may finish only within its
original finite full-frame write deadline. On
full success, attempt the denial next in FIFO order only within its original
finite control-write deadline; if that deadline has already expired, close
without starting the denial write. A failure or timeout of the preceding write,
malformed-generation close, an immediately closing GoAway reason
(`GO_AWAY_CLOSE`, `GO_AWAY_MALFORMED`, or `GO_AWAY_LIMIT`), an invalid GoAway,
or transport loss closes the channel and may prevent the denial attempt. The
route closes after the attempt; this rule never delays the newly lost binding's
`peer_lost` result or permits its late terminal to become a receipt.

For this first route, the general local-cancellation row above has one
pre-admission exception. An ordinary local abort marker does not itself remove
the exact durable `EffectPending` key. If its outbound Request has not been
admitted by the ordered writer, discard that Request, release its exact live
binding, and wake its waiter through a process-local no-write cancellation
path; send no `Cancel` for an ID the peer never saw and publish no external
`ToolRaw` or peer-derived result. If the same full durable `EffectPending` key,
saved arguments and `CancelRequested` control remain current, release this
process-local running effect and let ordinary core cancellation reconciliation
conditionally commit its reserved synthetic interrupted ToolResult without a
UsageRow. The same rule applies when invocation-specific provenance proves the
loop aborted before calling the wrapper's `execute`. A generic cancellation
marker or tool-error text alone does not prove that `execute` was skipped. If
checked current Session state no longer expects the full key, use the exact
no-longer-expected rule below and commit no new result, usage, terminal or
other write. If the Request was admitted or written and the generation is
usable, send one cause-appropriate `Cancel` in FIFO order, grant no new Credit,
and keep the exact binding eligible for its first validated terminal only
until the finite effective deadline or cancellation grace. A previously
reserved closing denial keeps its FIFO position and may close the generation
before the `Cancel` write; immediate-close and write-failure rules still apply.
An already retained first terminal remains subject to checked settlement.
A `Cancel` reserved **before** the first terminal was retained keeps its FIFO
position and original finite control-write deadline after that terminal's
checked settlement or no-longer-expected retirement, even when the binding's
owner-wide slot is released. A terminal retained before local cancellation
creates no `Cancel`. The preterminal exact-key fence's unadmitted-`Cancel`
removal below does not apply after terminal retention.
The first route reserves separate finite control capacity for one local
`Cancel` and one fixed peer-Request denial so either admitted control frame
can retain its original FIFO position and write deadline. A host unable to
reserve both within checked finite ceilings cannot activate this route.
For these control frames, writer admission is the same irrevocable sole-writer
head-of-line decision as for a Request, with a final monotonic sample below
that frame's own original finite control-write deadline.

After checked current Session state **definitively no longer expects the full
durable `EffectKey`**, a different exact-binding transition applies. For a
still-`open` or `cancel_requested` binding, atomically fence its generation and
request ID against later receipt, release its owner-wide live reservation and
wake any exact waiter without `peer_lost`, `ToolRaw`, ToolResult, UsageRow or a
synthetic terminal. This is not local cancellation and sends no `Cancel`. If
the outbound Request was never admitted, remove it and its byte reservation.
Also remove any `Cancel` for this binding that the writer has not admitted,
releasing its control-byte reservation. With a denial already reserved, a
writer-admitted earlier `Cancel` may finish only by its original finite
control-write deadline before the denial reaches the head; its failure or
timeout closes the generation and may preempt the denial. A `Cancel` reserved
after that closing denial is not sent. The fence reserves no new `Cancel`.
Without an already reserved denial, close that generation, including any
admitted but incomplete Request or `Cancel`. With an already reserved denial,
let an admitted Request retain its FIFO position and finish only by its original
finite full-frame write deadline; on success attempt the denial only by its
original finite control-write deadline, then close. If the Request was not
admitted, drop it and attempt that denial by the same original deadline.
Preceding write failure/timeout and the immediate-close reasons above may
preempt the denial. Release of the binding does not cancel the bounded writer
token needed to finish an admitted frame before that denial; a new generation
may use the freed owner slot while the old writer drains.

A first terminal fully validated **before** this fence remains one bounded
`terminal_received` receipt; a definitive no-longer-expected check retires it
through the existing checked path, and a CAS refusal while the key remains
expected retains it. A terminal arriving **after** the fence is stale: close
only its old malformed generation, possibly preempting a pending denial, and
never assign it to a replacement generation or durable key. Receipt retention
and fencing serialize on the owner line. Raw external finalization is observed
only at a later checked boundary; a terminal retained before that observation
still cannot commit against a key that has already disappeared. This
transition never writes or recreates the vanished durable effect.

## Wire values

Protocol values are closed:

```text
null | bool | safe integer | finite float | UTF-8 string | bytes
     | list<WireValue> | map<string, WireValue>
```

Functions, prototypes, symbols, cycles, shared references, process handles,
credentials, streams, and arbitrary language objects are invalid. Domain/wire
conversion lives in `pi-harness-runtime`, which revalidates all data before it
can enter a transaction.

For a `tool.v1` progress or result projection mapped to a Rust tool domain,
validation also requires constructing `AgentToolUpdate`/`AgentToolResult` from
the admitted bounded JSON **before** progress publication or first-terminal
receipt retention. Bind that decoder at registration to the host-approved
principal and tool, selected `CapabilityRef`, profile minor, and exact progress
or result projection ID/digest. A portable schema may accept an integral float
such as WireValue `float_value(1.0)` for a JSON `integer`, while a Rust
`SafeUnsigned` field requires the integer arm. If peer-supplied data passes the
selected schema but cannot construct that correctly bound domain value, close
the malformed generation without publishing that item, retaining a new
terminal receipt for its invocation, or writing its durable result. Previously
published items and retained receipts remain unaffected. An absent/mismatched
host binding or local validator failure unrelated to the peer value is a host
configuration/runtime fault, not malformed peer data. This pre-retention rule
ends at typed construction; the later context-dependent `ResultValidator`
keeps its existing in-band tool-result behavior. Typed peer error terminals use
their closed code/fixed message and do not require `AgentToolResult` conversion.

## Hook execution

`hook.v1` includes all eleven Harness v3 hooks:

| Hook | Aggregation | Replay/failure |
|---|---|---|
| `before_run` | append messages; last system prompt; `resumeData` per stable handler id | once before acceptance |
| `before_resume` | ordered void handlers | every resume; handlers must be idempotent |
| `before_run_end` | latest defined follow-up | may repeat at the same restored finish boundary |
| `transform_context` | sequential message replacement | every request/retry/resume |
| `before_request` | fieldwise stream-option patch | every request/retry/resume |
| `before_payload` | sequential payload replacement | every request with an exact declared payload schema |
| `after_response` | sequential assistant-message replacement | each response unless cancellation wins before it starts |
| `before_tool` | chained validated args; first block stops | invalid/error fails closed |
| `after_tool` | fieldwise result patch | live execution or safe replay |
| `before_compaction` | first decline/result stops | repeats only before a structural source commits |
| `before_navigation` | first decline/result stops | summarized navigation only |

A pipeline revision is a process-local ordered list of `CapabilityRef`s captured
before invocation. Stable handler identity is
`{authenticated_extension_id, handler_id}`; activation generation does not own
resume data. Ordinary handler errors or invalid outputs emit `handler_error`,
keep the prior aggregate, and continue. `before_tool` is the exception and
blocks. Core revalidates every replacement or patch.

A hook result becomes durable only in its consumer transaction, so a pre-commit
crash may rerun it. The effect key includes the logical hook attempt. The
extension must treat the key as an idempotency hint, not an exactly-once
guarantee. `before_payload` activation fails with `unsupported_schema` unless
the provider exposes the exact selected payload profile/digest. `after_response`
receives sanitized headers only; authorization, cookie, and credential-bearing
headers never enter the protocol.

Requiring an exact payload schema is an intentional compatibility restriction:
a raw provider-payload callback without a published profile cannot be projected
remotely.

## Tool execution

```text
commit intent
  → InvokeTool(binding)
  → Progress*
  → terminal raw result
  → source-ordered after_tool pipeline
  → validate
  → commit settlement
```

Progress is transient. The extension cannot append an entry or register. Tool
replay is declared `safe` or `never`; safe replay is at-least-once and keeps its
durable effect key. A `never` tool with unknown outcome settles conservatively as
interrupted. Replacement args and result patches are schema-validated.

## Provider execution and credentials

Provider extensions receive normalized model/context/options/tool DTOs and emit
standard assistant stream events. Rust owns retry attempts, cancellation,
terminal validation, usage settlement, deferred state, and tool planning.
Provider-specific raw payload hooks require a published payload profile/digest.

V1 exposes only `auth_mode = none` to protocol providers. Built-in Rust
providers continue to use the Rust `CredentialStore`. `secret.resolve` is not a
grantable host action; requests return `broker_unavailable`, and a manifest that
requires a core-managed credential fails activation with
`secret_broker_unapproved`. A privileged host may configure its own peer outside
this protocol, but the Rust workspace makes no custody or refresh guarantee for
that secret. No credential, handle, authorization header, or refresh token may
enter protocol payloads, state, passive events, errors, or telemetry.

A future security-owner ADR may add a separately versioned signing/credential
broker. Host-mediated signing or proxying is preferred to raw-secret delivery.

The auth-none rule is an intentional v1 compatibility restriction, not an
implicit promise that credentials will later cross the wire.

## Resource profile

`resource.v1` is core-to-extension and has two unary methods:

| Method | Request | Result | Permission/failure |
|---|---|---|---|
| `resource.list` | `{scope, known_revision?}` | bounded manifests with id/version/kind/digest/metadata | `resource.provide`; invalid schema disables the capability |
| `resource.read` | `{id, version, expected_digest}` | bounded text/bytes/portable value plus digest | `resource.provide`; typed not-found/version/digest failure |

An optional passive `resource.changed` notification requests a refresh; it never
changes an operation snapshot. Rust validates, versions, and snapshots returned
resources before a run consumes them. Remote URLs, filesystem handles, and live
streams are not resource values.

## Host actions and reentrancy

`host-action.v1` is extension-to-core and unary. V1 contains exactly:

| Method | Required grant | Result/failure |
|---|---|---|
| `snapshot.get` | `conversation.read` plus lane/visibility scope | filtered snapshot or `forbidden/not_found` |
| `resource.get` | `resource.read` plus resource scope | snapshot or `forbidden/not_found/version_mismatch` |
| `queue.steer` | `queue.steer` plus lane scope | queue result |
| `queue.follow_up` | `queue.follow_up` plus lane scope | queue result |
| `queue.next_run` | `queue.next_run` plus lane scope | queue result |
| `operation.abort` | `operation.abort` plus lane/operation scope | idempotent abort result |

Filesystem, process, network, UI, prompt, compact, navigate, resume, wait-idle,
run-when-idle, and secret methods are not v1 actions. An unknown method returns
`unsupported_method`; a missing grant fails before work starts.

An extension-originated host action during an invocation MUST name a live parent
on the same authenticated principal and generation. The host maintains an
ancestry/wait graph. Missing, terminal, cross-generation, unrelated, or cyclic
parents return `invalid_parent`. Maximum nesting depth is eight. A child deadline
is the minimum of requested, method, and parent-remaining deadlines; parent
cancellation cascades to descendants.

The lane mutation line is never held while awaiting a peer. Snapshot/resource
reads and independent commits may execute reentrantly. `steer`, `followUp`,
`nextRun`, and `abort` may commit because they do not await parent completion.
Same-lane `prompt`, `resume`, `compact`, `navigate`, `waitForIdle`, and
`runWhenIdle` would depend on their ancestor and return `would_deadlock`.

## Telemetry-sink profile

`telemetry-sink.v1` is core-to-extension and receives content/secret-free typed
records only:

| Method | Request | Terminal result |
|---|---|---|
| `telemetry.emit_batch` | `{schema_version, sequence, records[]}` | `{accepted, dropped}` |
| `telemetry.flush` | `{through_sequence}` | `{flushed_through}` |

It requires `telemetry.receive` plus schema/scope grants. Delivery uses a bounded
passive queue; overflow increments an observable dropped counter and may
coalesce batches. Timeout, malformed acknowledgement, or peer failure disables
the sink and never delays or changes operation settlement. `flush` is best
effort at host close, not a durability boundary.

## Passive events are not `watch()`

Trusted in-process `AgentLane::watch()` preserves the upstream atomic
snapshot-plus-buffer and ordered delivery contract. `event-sink.v1` is an
intentional bounded remote projection:

```text
Subscribe(scope)
  → Snapshot {
      subscription_id, snapshot_revision,
      next_event_sequence, filtered_snapshot
    }
  → Event(sequence = next, ...)*
```

Sequence is scoped to one subscription generation and is not durable across
reconnect. On passive-queue overflow, core discards queued passive events, emits
a reserved `Gap {first_dropped, last_dropped}`, pauses ordinary delivery, and
requires `Resync(subscription_id)`. Resync atomically returns a new filtered
snapshot and sequence baseline. Failure to consume the reserved gap before its
deadline deactivates the subscription. Passive delivery cannot block an
operation or settlement.

## Limits and untrusted peers

Every endpoint receives finite `ProtocolLimits`; no unbounded mode exists. A
fixed pre-handshake frame ceiling applies before payload allocation. Negotiated
limits cover frame bytes, aggregate decoded bytes, value depth/elements, string
and byte lengths, in-flight requests, per-class queue bytes, nesting, request
rate, handshake time, and cancellation grace.

Fixed overload behavior:

- an over-limit length prefix closes before allocation;
- malformed frames/values close the generation;
- a full blocking queue returns `overloaded` without enqueue;
- exhausted progress credit requires the sender to stop;
- event-sink overflow enters gap/resync;
- telemetry-sink overflow drops/coalesces with an observable counter;
- terminal/control slots remain reserved per live binding;
- rate or concurrency excess returns a typed denial without starting work.

Exact production numbers are evidence-dependent configuration. Conformance uses
tiny injected limits and fake clocks to cover fragmentation/slow frames,
allocation bombs, invalid UTF-8/numbers/identifiers, duplicate ids/terminals,
wrong bindings/generations, sequence/credit violations, forged/cyclic ancestry,
both cancel/end orders, every queue exhaustion path, every host-action
permission, crash boundaries, and sentinel-secret redaction.

## Legacy JavaScript host

A Node host may load current JavaScript modules/Jiti extensions and project the
headless subset as a protocol peer. It may emulate in-place mutation within one
handler call and return a validated patch. UI factories, captured mutable
Agent/Session objects, arbitrary `details`, raw credentials, and cross-process
object identity are unsupported. The host, not Rust core, owns JavaScript module
loading and any process sandbox policy.
