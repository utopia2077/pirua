# AI domain conformance contract

Authority: pinned upstream `853a80d26c90a14c1886f0ebb8ffaae133ca2185` and the accepted architecture.

## Fixture budget

The accepted minimum is 327 behavior rows. Rows may be table cases rather than
individual Rust test functions, but each fixture must have a stable id under
the suite prefix below. Unit inventories record that id, one source
citation/test case, polarity, owner, and expected observation. Deterministic
aggregation into `conformance/ai-domain/behavior-inventory.json` reads the ten
accepted unit fragments in the order frozen by `final-domain-runtime.md` and
rejects duplicate ids, missing rows, stale output, or a count that differs from
this table.

| Suite | Rows |
|---|---:|
| assistant stream | 22 |
| registry/store/auth/concurrency | 88 |
| tool validation | 30 |
| message transformation | 31 |
| overflow classification | 43 |
| retry classification | 54 |
| session resources | 12 |
| portable helpers | 47 |
| Total | 327 |

The reconciled oracle proposed 342 rows. Exactly 15 `retry-loop.*` rows remain
outside S03, leaving 327 here: 14 belong to the stateless `pi-ai`
compatibility helper in S05, while `retry-loop.overflow-precedence` belongs to
the actual coding-session caller in S15. S03 must not inflate its count by
including S04 HTTP retry, S05 helper behavior, or S15 caller policy.

Direct upstream seed provenance is independently counted:

| Pinned source | Expanded cases | S03 | Deferred |
|---|---:|---:|---:|
| `models-runtime.test.ts` | 39 | 39 | 0 |
| `validation.test.ts` | 9 | 9 | 0 |
| `overflow.test.ts` | 18 | 18 | 0 |
| `retry.test.ts` | 19 | 10 classifier | 9 helper |
| context/lax-transform/text/uuid direct files | 12 | 12 | 0 |
| mixed provider/thinking/telemetry/lazy cases | 14 | 14 | 0 |
| Total | 111 | 102 | 9 |

The larger 327/15 matrix expands patterns, races, negative dimensions, and
first-party causal probes from those seeds; it does not claim 327 upstream test
declarations.

The exact deferred ids are:

```text
retry-loop.policy-absent
retry-loop.policy-disabled
retry-loop.zero-retries
retry-loop.immediate-success
retry-loop.immediate-aborted
retry-loop.immediate-nonretryable
retry-loop.exhaustion-call-count
retry-loop.later-success
retry-loop.aborted-after-scheduled
retry-loop.observer-order
retry-loop.exponential-delays
retry-loop.cancel-during-sleep
retry-loop.finish-only-after-retry
retry-loop.finish-outcomes
retry-loop.overflow-precedence
```

The first 14 ids above are S05-owned. The final id is S15-owned and binds
`packages/coding-agent/src/core/agent-session.ts:2846-2849`; it is not a
behavior of `retryAssistantCall`.

No other row moves out of this domain contract without an explicit reviewed revision and inventory
diff.

## Stream matrix

The 22 stable rows are:

```text
stream.terminal.stop
stream.terminal.length
stream.terminal.tool-use
stream.terminal.deferred
stream.terminal.error
stream.terminal.aborted
stream.result-without-events
stream.queued-fifo
stream.waiting-fifo
stream.setup.unknown-provider
stream.setup.auth
stream.setup.adapter
stream.setup.spawn
stream.cancel.pre-cancelled
stream.cancel-wins
stream.provider-wins
stream.cancel-provider-barrier-race
stream.post-terminal-matrix
stream.last-producer-drop
stream.cancel-live-noncooperative-producer
stream.reentrant-waker-matrix
stream.dropped-waiter-matrix
```

The inventory maps the three pinned public symbols `EventStream<E,R>`,
`AssistantMessageEventStream`, and `create_assistant_message_event_stream` to
their Rust consumer/validated-producer projections. `AssistantStream` is an
unclaimed convenience alias. `stream.post-terminal-matrix` covers all terminal/
terminal and terminal/progress combinations. The waiter matrix covers `next`,
`result`, cancellation, and sleep unregister-on-drop. Missing and empty
terminal error messages are separate negative dimensions.

Additional parameterized witnesses under those 22 ids prove multiple concurrent
result waiters receive owned `R` clones while `E` need not be Clone; 32 producer
clones settle exactly once on final drop; the post-terminal matrix covers all 96
progress/terminal combinations; and linked cancellation covers empty,
pre-cancelled, drop-unregister, and no-strong-cycle cases. Factory import tests
assert the exact `AssistantStreamInit` signature.

## Registry and auth matrix

The contract partitions these 88 rows into the accepted 58-row auth/store
fragment and the exact 30-row registry fragment in `models-registry.md`.
Deterministic aggregation rejects duplicate/missing ids or any count other than
58 + 30.

The registry ids are:

```text
registry.providers.order-replace-delete-clear
registry.models.list-find-failure-isolation
registry.constructor.exclusive-model-store
registry.auth.check-available-filter
registry.refresh.selection-error-cancel-matrix
registry.refresh.cache-before-credential-read
registry.refresh.offline-cache-only
registry.refresh.unconfigured-cache-only
registry.refresh.configured-network-phase
registry.supersede.new-refresh
registry.supersede.replace-provider
registry.supersede.delete-provider
registry.supersede.clear-providers
registry.publication.same-provider-serial
registry.publication.cross-provider-concurrent
registry.publication.two-three-superseded-sequence
registry.publication.drop-prepoll-ticket
registry.publication.drop-admitted-future
registry.publication.intent-matrix
registry.publication.persistence-fence-matrix
registry.restore.policy-reclassification
registry.login.method-error-matrix
registry.login.cancellation-admission
registry.login.quarantine-replace
registry.logout.refresh-and-quarantine-delete
registry.endpoint.precedence-custody
registry.headers.precedence-case-null
registry.dispatch.resolved-text-deferred-roots
registry.dispatch.adapter-capability-matrix
registry.dispatch.setup-error-redaction
```

Concurrency uses barriers/manual polling only. It covers call-time publication
reservation, pre-poll/admitted drop, same/cross-provider order, failure and
reentrant destructors, four supersession causes, before/during/after store
admission, post-spawn global clear ordering, synchronous closed-gate spawn
poll/drop reentry for both success and failure, non-cooperative cancellation,
and checked generation/ticket exhaustion. A successor generation performs no
cache read until an older admitted store effect is definitive. Cache
publication acknowledgement precedes any credential read; offline never
touches credentials; provider-epoch/generation-only supersession leaves
`aborted=false`; replacement admission while the old provider remains publicly
visible cannot allocate new authority for that displaced epoch; the network
phase rereads the model store and catalog OAuth refreshes only expired
credentials.

Endpoint/header fixtures cover credential/public/runtime-sentinel precedence,
missing sentinel resolution, immutable source DTOs, redacted Debug, and the
four text/deferred roots dispatched by `ModelsRegistry`. The stream/request
unit already proves the fifth nominal image root's custody and runtime shape;
S05 owns image auth/registry/dispatch precedence. No S03 registry fixture claims
an image provider runtime.

## Pure behavior matrix

- Validation: the 26 pinned logical inputs plus known/unknown tool, nested
  required path, and nonmutation.
- Transformation: raw lax intake in façade, image placeholders, thinking/text/
  tool signatures, id remapping/collision, dropped failed turns, orphan repair,
  and injected timestamp.
- Overflow: every positive pattern, all exclusions, structural negatives,
  usage thresholds, recoverable length, and overlap dual-classification.
- Retry: every positive marker, each nonretryable marker combined with a
  retryable marker, and structural negatives.
- Helpers: cost tiers, thinking levels, context estimates, deferred tools,
  text, hash, JSON repair/partial parse, UUID monotonicity, cancellation, and
  runtime-port propagation.
- Resources: duplicate object registration, order, optional/exact session id,
  idempotent removal, drop, mutation during snapshot, reentrancy, continuation,
  ordered error aggregation, and removal-plus-failure where removal wins.

The exact remaining 217 stable ids, test bindings, pinned anchors, expected
observations, APIs, and parameterized witness rules are normative in
`final-domain-runtime.md`. Counts are 31 + 30 + 43 + 54 + 20 + 27 + 12. The two
helper fragments aggregate to the fixed 47-row portable-helper suite.
In particular, each of the 43 overflow and 54 retry rows has a fixed source,
test binding, polarity, owner, and expected observation; these are frozen inputs
to implementation. The S03 overlap row proves only that both
classifiers return true. S15 separately owns the actual caller precedence at
pinned `packages/coding-agent/src/core/agent-session.ts:2846-2849`.

## Security and compile probes

- Assert object safety for every dynamic port and shared borrowed-future
  lifetime.
- Compile-fail Serde for endpoints, standalone resolved auth, resolved requests,
  runtime options, streams, resource handles, and mutation leases.
- Scan Display, Debug, terminal messages, model/store serialization, fixtures,
  and telemetry for a unique secret/header/endpoint/provider-cause sentinel.
- Force schema-cache digest collision, equal/different concurrent misses,
  admission/eviction races, poison recovery, and reentrant displaced-entry
  destruction; assert a 16-entry bound and no drop under lock.
- Exercise borrowed structural preflight before owned conversion at schema/
  argument byte and depth-128 boundaries. Prove the fail-only reference closure
  promotes local JSON Pointer, anchor, nested-resource, dynamic, and recursive
  targets into schema frames, follows transitive refs, terminates cycles, and
  rejects non-local refs reached through an originally data-valued location.
  Prove child and reference dispatch follows Draft 4/7/2019/2020 rather than the
  dependency's over-broad Draft 4 child iterator, later-draft keywords stay data
  unless reached by an active ref, and a recognized-path target with a
  trailing-slash relative id is not entered twice. Separately freeze the pinned
  containing-base behavior of a target promoted from a data-valued path and
  prove a direct active external ref on such a target still fails. Compare the
  pinned Draft 4 registry's conservative external-ref crawl with direct offline
  jsonschema construction while proving patterns in that location stay data.
  Count every distinct physical schema-frame pattern value and
  patternProperties key once across repeated references; prove unreferenced
  property/definition/regex names and const/enum/default/examples objects stay
  data. Exercise 65,536-context, reference-scope-depth-128, and one-MiB distinct
  effective-URI fail-closed bounds. Prove duplicate URI/scope interning and a
  4,096-reference graph without per-context string copies;
  exercise compiled-size and backtrack failures without elapsed-time
  assertions; specifically force a patternProperties key-match backtrack
  failure and prove the conservative guard returns the fixed root issue rather
  than accepting jsonschema's swallowed non-match; force DFA-size fallback and
  prove the correct bounded validation result; sort issue output independently of jsonschema hash
  seeding. The 16 published issues are not asserted as a 16-error working-memory
  bound because jsonschema 0.52.1 eagerly fills its internal error vector.
- Assert no HTTP/provider SDK, `async-trait`, runtime, or retry-sleep dependency
  enters S03.
- Run `cargo test --doc -p pi-ai-core --locked --offline`; `--all-targets` does
  not substitute for compile-fail doctests.
- Mutate one terminal reason, generation, credential revision, header case,
  stop reason, or threshold fixture and prove the intended test fails.
- Prove `jsonschema` has no resolver/file/async/runtime features, the direct
  classifier declaration disables `regex` defaults while recording
  jsonschema's unified regex features and mandatory
  uuid-simd/getrandom/ahash/fancy-regex graph, and `ryu-js` has no feature or
  mandatory transitive dependency.
- Prove transform, validation, JSON, UUID, provider, and resource error
  formatting cannot reveal their unique input/source sentinels.
- Exercise streaming JSON at 256 KiB and one byte above for complete and
  incomplete input; over-limit always returns an empty object.
- Exercise reentrant and panicking resource/panic-payload destructors after all
  callbacks and before cleanup-in-progress clears.
- Assert the resource failure list is callback Err/panic failures in snapshot
  order followed by destructor panics in the fixed payload-then-resource
  disposal order, and assert the exact redacted Display/Debug strings.

## S04 boundary

S04 owns HTTP/proxy/timeout, SSE/WebSocket, response bodies/headers, canonical
wire conversion, partial provider stream/tool assembly, provider-request retry,
and lazy API-module construction. It exposes the trusted process-local resource
object for session-scoped transport state. S03 owns no HTTP request or response
type.

## S05 boundary

S05 owns 48 provider-factory files, concrete OAuth flows, images runtime,
generated/static catalog data, faux provider behavior, compat registration,
CLI/developer entrypoints, and provider-feature matrices. It registers S04's
session resource and retains the non-clone S03 handle for the provider
composition lifetime. S03 defines only the provider/auth/image ports needed by
those implementations.

## S12 and S15 resource boundary

Foreign peers never enter `SessionResourceRegistry`. Accepted S12
`resource.v1` is value list/read only and has no session-cleanup lifecycle; a
future profile must close that registered compatibility gap before claiming
foreign cleanup parity. S15 owns the registry instance and invokes cleanup from
coding-session disposal. Protocol frames may stream, but no language stream
object/handle crosses into the registry.

## S05 retry compatibility mapping

Pinned `RetryCallbacks` and `retryAssistantCall` map to a stateless S05 helper in
the `pi-ai` façade. Its 14 stable `retry-loop.*` rows include the nine pinned
expanded helper tests plus added policy/delay/cancellation probes other than
caller overflow precedence. Rust uses trusted process-local
producer/sleeper/observer ports rather than a foreign JavaScript callback
object. Harness never calls the helper and keeps durable retry ownership; S14
independently preserves legacy Agent orchestration. The helper remains absent
from provider-neutral `pi-ai-core`. S15 owns the fifteenth deferred row,
`retry-loop.overflow-precedence`, at the pinned coding-session caller.

## Inventory proof

`conformance/ai-domain/inventory.json` must name all 699 pinned AI export
occurrences, including root and compat duplicates, and one disposition:

```text
implemented | s02-dto | mapped-s04 | mapped-s05 |
rust-nonapplicable | external-schema-projection
```

No entry may be silently absent, and no implementation claim may be satisfied
by an empty public stub. The contract freezes the expected disposition totals as
146 `s02-dto`, 123 `implemented` after S03 closure, 22
`rust-nonapplicable`, 163 `mapped-s04`, 239 `mapped-s05`, and six
`external-schema-projection` rows.
