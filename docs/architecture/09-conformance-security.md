# Conformance, Verification, and Security

## Conformance authority

Behavior/spec fixtures are authoritative. Generated SDKs, Rust implementations,
and compatibility adapters are implementations of the contract, not parallel
sources of truth.

## Required fixture families

### AI

- every message/content/event variant;
- stream terminal/error/abort behavior;
- cross-provider message transformation;
- tool argument conversion/validation;
- orphan tool result repair;
- thinking/signature replay;
- usage/cost/cache calculation;
- provider retry and overflow classification;
- auth precedence, locked refresh, and failure behavior;
- raw transient-header round trips plus default-deny durable public-header
  classification and credential-header sentinel rejection;
- provider HTTP/stream traces;
- generated model catalog parity for selected provider entries;
- selected image generation outcomes;
- internal scripted/deferred provider behavior needed by offline tests.

Provider breadth follows the [selected scope](../scope.md#selected-provider-scope).
The source inventory remains complete; excluded entries retain an explicit owner
decision and do not count as implemented. Existing accepted shared adapter tests
remain regression obligations. Public Faux compatibility is not an exit claim.

### Harness

- `OperationState + PlannerInputs -> Action`;
- effect output to settlement plan;
- exact transaction write order;
- restore state to next action/suspension;
- every crash boundary around intent/effect/settlement;
- nested summary intent/run/usage-settlement as three independent boundaries;
- every cancellation race order;
- sequential/parallel tool ordering;
- hook aggregation and failure policy;
- automatic/manual drive durable equivalence;
- terminal cleanup and `lane.lastResult`;
- state migrations for every reachable old variant.

### Storage

- atomic rollback;
- shared sequence order;
- duplicate id rejection;
- register set/delete/recreate;
- null versus deletion;
- branch scans and bounds;
- forks and facts;
- usage/stat equivalence;
- JSONL torn-tail and snapshot equivalence;
- SQLite transaction mode, lease fencing, query plans, branch segment chains;
- legacy v3 normalization.

### Extension protocol

- N/N+1 handshake, protocol/profile version negotiation, required/optional
  mismatch, host-ceiling intersection, and schema-digest mismatch;
- manifest atomic activation;
- permission denial for every host action and sanitized header/event projection;
- arbitrary frame fragmentation/coalescing and slow partial frames;
- pre-allocation oversized-length rejection, deep/large value bombs, invalid
  UTF-8, unsafe integers, and non-finite floats;
- authenticated nested requests, forged/missing/cyclic parents, nesting limit,
  deadline and cancellation propagation, and the allowed/deadlocking method
  matrix;
- contiguous sequence, byte credit, wrong binding/generation, duplicate id,
  duplicate terminal, zero/initial/additional/overflow credit, exact frame-byte
  charging, relative monotonic deadlines, and complete terminal-without-progress
  consumption;
- both cancellation/terminal arrival orders;
- old-generation drain acceptance while the exact binding is live, followed by
  stale rejection after settlement/abandonment;
- extension crash and replay policy;
- every control/terminal/blocking/progress/passive queue exhaustion behavior;
- passive gap/resync snapshots and trusted `watch()` non-regression;
- all eleven hook aggregation, failure, replay, and schema rules;
- every resource, host-action, and telemetry-sink method/direction/grant/failure;
- unknown optional fields, unknown closed variants, simultaneous old/new
  capabilities, and canonical digest vectors;
- sentinel-secret redaction and allocation-budget counters;
- SDK parity across Rust and TypeScript, with an executable Python or Go peer.

## Proof-obligation matrix

| Property | Disproving stimulus | Observation |
|---|---|---|
| intent before effect | adapter receives invoke before commit | storage/wire trace |
| settlement at most once | duplicate terminal around `terminal_received` | first retained binding/output settles once, then retires |
| live/durable correlation | right invocation id with wrong effect key/generation | no state mutation; typed violation |
| deterministic hook order | connection timing changes | same aggregate for same pipeline revision |
| total recovery | restart from every operation state | defined next action or corruption |
| conservative migration | migrate an effect-pending old state | uncertainty/ids/replay policy preserved; zero effects |
| stale response safety | reply after binding retirement or with wrong key/generation | no current-state mutation |
| draining generation safety | old generation terminates a still-live binding | one valid settlement before binding retirement |
| bounded memory | peer stops or overloads parser or queues | fixed allocation counters and explicit overload/close |
| storage atomicity | injected failure inside commit | none of the writes visible |
| search non-authority | delete/rebuild index | canonical session unchanged |
| secret redaction | auth/provider failure | no credential in log/telemetry/error detail |
| durable header safety | caller supplies authorization/custom-secret header | acceptance rejects before any operation-state write |
| upstream baseline completeness | enumerate pinned watched Git trees | exactly one route/owner or explicit outside/disposable class |
| upstream drift completeness | synthetic add/delete/modify/rename/selector change | blocking canonical drift item and owner/proof suite |

## Security boundaries

### Assets

- provider credentials and refresh tokens;
- conversation and tool content;
- filesystem/process permissions;
- durable session files;
- extension identities and grants;
- model/provider configuration.

### Trust boundaries

- Rust core versus extension process;
- core versus provider service;
- host versus untrusted workspace/project resources;
- local process versus remote client;
- generated external model feeds versus catalog authority.

## Extension authentication and authorization

Transport authentication precedes protocol messages. A manifest's self-declared
extension id is not authentication.

The host supplies authenticated identity and a scoped grant, including
conversation read, tool/provider registration, snapshot/resource read,
independent queue/control mutations, and telemetry/event access. Core services
validate identity, parent binding, generation, and scope on every request. UI,
filesystem, process, network, and secret authority are not granted by protocol
v1.

Protocol authorization does not sandbox direct OS access. An untrusted peer may
be connected only after an out-of-scope privileged host establishes its
filesystem, environment, network, process, transport-authentication, and
resource isolation. Runtime accepts a pre-authenticated endpoint and does not
claim that merely closing it terminates the process.

## Credentials

- Credentials never enter operation state, session entries, passive events, or
  telemetry.
- Built-in Rust providers obtain credentials through the Rust
  `CredentialStore` and serialize refresh per provider.
- Protocol v1 provider capabilities support only `auth_mode = none`.
- `secret.resolve` is ungrantable and returns `broker_unavailable`; a provider
  requiring a core-managed secret fails activation with
  `secret_broker_unapproved`.
- Errors and diagnostics are sanitized and bounded.

A future credential/signing broker requires a separate security-owner ADR and
profile version. No configuration fallback hatch enables it in v1.

## Limits and overload

Every connection receives finite `ProtocolLimits`; no unbounded mode exists. A
pre-handshake ceiling applies before allocation. Negotiated bounds cover frame
and decoded bytes, value depth/elements, strings, queues by class, in-flight
requests, nesting, request rate, handshake, deadline, and cancellation grace.
Exact production numbers require representative evidence; behavior is fixed:

- oversized length closes before allocation;
- malformed frames/values close the connection generation;
- blocking queue overflow returns `overloaded` without starting work;
- progress stops at exhausted byte credit;
- event-sink overflow enters reserved gap/resync;
- telemetry-sink overflow drops/coalesces with an observable counter;
- one control/terminal slot is reserved per live binding;
- rate/concurrency excess returns a typed denial;
- blocking calls inherit the minimum parent/method/request deadline.

## Review independence

Persistence, protocol, security, concurrency, migration, and public contract
slices require an independent reviewer. Final completion requires an
assembly-level audit asking whether the integrated system still satisfies the
ownership and dependency invariants, not merely whether unit tests pass.
