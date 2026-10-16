# Crate topology

## Dependency and ownership boundaries

The earlier 26-crate model used each synchronization boundary as a Cargo
boundary. That created unnecessary build, dependency, versioning, and release
surface. This design separates two concepts:

- **Crates** are dependency, feature, and ownership boundaries.
- **Modules** are upstream synchronization boundaries.

The original fifteen-crate baseline keeps fine-grained mappings as internal
modules. The standalone Coding session store and shared SQLite infrastructure
add independent owners for the v1.4.0 upgrade, alongside the local CodeMode
executor and MCP client. The inventory below is the
current workspace; the formal upstream baseline remains separately pinned.

## Crate inventory

| # | Crate | Owns | Principal upstream mapping |
|---:|---|---|---|
| 1 | `pi-telemetry` | telemetry contracts, redaction metadata, no-op and memory references | `packages/telemetry`, agent schemas |
| 2 | `pi-extension-protocol` | multi-language capability protocol | coding-agent extensions, Harness hooks |
| 3 | `pi-ai-core` | AI DTOs, streams, Models, auth, stores | `packages/ai/src/types.ts`, `models.ts`, `auth/**` |
| 4 | `pi-ai-transport` | HTTP/SSE/WebSocket/retry/proxy | `packages/ai/src/utils/*transport*`, retry helpers |
| 5 | `pi-ai-providers` | API adapters, provider factories, OAuth, images, catalogs | `packages/ai/src/api/**`, `providers/**`, images |
| 6 | `pi-ai` | public façade, compat, CLI, test and catalog tooling targets | `packages/ai/src/index.ts`, `compat.ts`, scripts |
| 7 | `pi-agent-schema` | shared Agent and Harness v3 schema | `packages/agent/src/types.ts`, Harness Parts 1–5 |
| 8 | `pi-agent-loop` | legacy Agent/loop/tool pipeline/proxy | `agent.ts`, `agent-loop.ts`, `proxy.ts` |
| 9 | `pi-agent-session` | Storage/Session/Repo, Memory, JSONL, migrations | Harness Parts 1–2, S1, Appendix B |
| 10 | `pi-agent-sqlite` | SQLite storage backend | session-backends/sqlite-node, Harness S2 |
| 11 | `pi-harness-core` | pure total-state planner and settlement | Harness Parts 3–4 |
| 12 | `pi-harness-runtime` | interpreter, effects, hooks, events, resources, compaction, tools | Harness Parts 4–5, R1–R12 |
| 13 | `pi-agent-search` | standalone search service | Harness S3 and search projection |
| 14 | `pi-agent` | public façade and conformance/testkit modules | agent root exports and session/testing |
| 15 | `pi-coding-runtime` | headless coding models, tools and composition | selected coding-agent core/server modules |
| 16 | `pi-sqlite` | shared SQLite connection, snapshots and transactions | Rust-native storage infrastructure |
| 17 | `pi-coding-session` | Coding transcript trees, projections and SQLite/Memory stores | v1.4.0 `coding-agent/src/core/session-manager.ts`, `messages.ts` |
| 18 | `pi-codemode` | controlled local JavaScript worker, tool bridge and replay inputs | v1.4.0 `packages/codemode` |
| 19 | `pi-mcp` | neutral MCP client, stdio/HTTP transports and OAuth | v1.4.0 `packages/mcp` |

## Internal synchronization modules

### `pi-ai-core`

```text
types/
stream/
auth/
models/
stores/
transform/
validation/
retry/
overflow/
session_resources/
```

### `pi-ai-providers`

```text
api/anthropic_messages
api/openai/{completions,responses,codex,azure}
api/google/{shared,generative_ai,vertex}
api/{bedrock,mistral,pi_messages}
providers/<upstream-provider-id>
oauth/<upstream-flow>
images/
catalog/
```

### `pi-agent-schema`

```text
message/
tool/
entry/
register/
transaction/
operation/
state/
surface/
hook/
event/
snapshot/
error/
```

### `pi-agent-session`

```text
storage/
session/
tree/
context/
codec/
memory/
jsonl/
legacy_v3/
repo/
migration/
id/
conformance/
```

### `pi-harness-core`

```text
validate/
planner/
transition/
generation/
tool_batch/
deferred/
checkpoint/
inbox/
control/
structural/
navigation/
recovery/
terminal/
migration/
```

### `pi-harness-runtime`

```text
driver/
effects/
lane/
settings/
scheduler/
manual_gate/
registry/
hooks/
events/
watch/
abort/
close/
fault/
restore/
surface/
compaction/
resources/
tools/
telemetry/
```

### `pi-coding-runtime`

```text
models/
credentials/
provider_composition/
tools/
messages/
system_prompt/
settings_snapshot/
session_lifecycle/
legacy_import/
resources/
extensions/
builder/
```

## Dependency graph

Edges below use `consumer → dependency`. “Optional” means a named Cargo feature;
an absent edge is forbidden even if Cargo could technically resolve it.

| Consumer | Required direct internal dependencies | Optional direct internal dependencies |
|---|---|---|
| `pi-telemetry` | none | none |
| `pi-extension-protocol` | none | none |
| `pi-ai-core` | `pi-telemetry` | none |
| `pi-ai-transport` | `pi-ai-core` | none |
| `pi-ai-providers` | `pi-ai-core`, `pi-ai-transport` | none; provider modules use crate features |
| `pi-ai` | `pi-ai-core` | `pi-ai-providers` through the machine-defined `ai-facade-provider-selection` group |
| `pi-agent-schema` | `pi-ai-core` | none |
| `pi-agent-loop` | `pi-ai-core`, `pi-agent-schema`, `pi-telemetry` | `pi-ai-transport` through `proxy` |
| `pi-agent-session` | `pi-agent-schema`, `pi-telemetry` | none; `jsonl` enables only external/file modules |
| `pi-agent-sqlite` | `pi-agent-schema`, `pi-agent-session` | none |
| `pi-harness-core` | `pi-agent-schema` | none |
| `pi-harness-runtime` | `pi-ai-core`, `pi-agent-schema`, `pi-agent-loop`, `pi-agent-session`, `pi-harness-core`, `pi-telemetry` | `pi-extension-protocol` through `extensions` |
| `pi-agent-search` | `pi-agent-schema`, `pi-agent-session` | none; `sqlite-fts` owns its external SQLite dependency |
| `pi-agent` | `pi-ai-core`, `pi-telemetry`, `pi-agent-schema`, `pi-agent-loop`, `pi-agent-session`, `pi-harness-core`, `pi-harness-runtime`, `pi-agent-search` | `pi-agent-sqlite` through `sqlite` |
| `pi-coding-runtime` | `pi-ai`, `pi-agent`, `pi-agent-session`, `pi-harness-runtime`, `pi-telemetry` | `pi-ai-transport` through `remote-catalog`; `pi-ai-providers` through `provider-selection`; `pi-agent-sqlite` through `sqlite-sessions`; `pi-extension-protocol` through `extensions` |
| `pi-sqlite` | none | none |
| `pi-coding-session` | `pi-sqlite` | none |
| `pi-codemode` | none | none |
| `pi-mcp` | none | none |

The test-only internal edge is listed separately from production dependencies:

- `pi-harness-runtime` → `pi-agent-sqlite` (dev-only)

This edge lets the runtime's owning integration tests exercise the real SQLite
backend. It is forbidden as a normal or feature-activated production dependency.
The machine topology checks its exact Cargo dev declaration and includes it
when checking the all-target test graph and cycles.

One valid topological order is:

```text
pi-extension-protocol, pi-telemetry,
pi-ai-core, pi-agent-schema, pi-ai-transport, pi-harness-core,
pi-agent-session, pi-agent-loop, pi-ai-providers,
pi-agent-search, pi-agent-sqlite, pi-harness-runtime,
pi-ai, pi-agent, pi-coding-runtime, pi-sqlite, pi-coding-session, pi-codemode, pi-mcp
```

Default and all-feature graph checks reject a cycle or unlisted internal edge.

The [runtime effect contract](../contracts/runtime-effects.md)
adds runtime -> loop for shared executable tool phases. Each scheduler retains its
own ordering and state authority; core still depends only on schema. This
acyclic edge does not import the full legacy Agent into the Harness runtime.

## Literal schema and migration ownership

| Responsibility | Sole owner | Boundary |
|---|---|---|
| Current and historical serialized `OperationState` definitions, fields, discriminants, and serde rules | `pi-agent-schema` | behavior-free owned DTOs |
| Pure version-to-version `OperationState` mappings | `pi-harness-core::migration` | exhaustive deterministic functions over schema DTOs |
| Migration registry, chain selection, migrate-on-open orchestration, and newer-version refusal | `pi-agent-session::migration` | process-local registry under the backend writer lease |
| Pure validation, `next_action`, transition planning, and settlement planning | `pi-harness-core` | no storage, clock, network, extension, or tool implementation |

`pi-agent` composes the standard migration registry from session-owned registry
APIs and core-owned mapping functions. Composition does not make the façade a
second authority. Backends own only physical format and transaction mechanics.

## Feature and platform boundaries

- `pi-ai-providers` defaults to no built-in provider, OAuth, or image family.
- Machine topology defines `provider-selection` as the feature group
  `provider-*`, `oauth-*`, `images-*`, and `catalog-gen`; it is not itself a
  Cargo feature. It remains the coding-runtime selector group. The facade uses
  the exact `ai-facade-provider-selection` group: seven catalog selectors, six
  provider selectors, images-runtime/images-openrouter, three OAuth selectors,
  all-providers/all-images/all-oauth, native/auth-native, and reserved absent
  catalog-gen/dev-oauth-cli. Implemented runtime compat joins this exact group; dev-catalog-tools stays outside.
  Each implemented member activates its consumer’s optional internal crate edge.
- `wire-*` enables one wire family. `provider-<id>` enables its wire family and
  catalog shard but never OAuth. `oauth-<flow>` and `images-<id>` are independent
  and default-off. `all-*` umbrella features are for explicit binaries/tests,
  never default library features.
- OAuth protocol logic lives in `pi-ai-providers`; browser opening, callback
  listener hosting, and interactive prompts are injected host services.
- `pi-agent-search` never depends on `pi-agent-sqlite` or
  `pi-harness-runtime`. Its optional FTS implementation owns a private external
  SQLite dependency and consumes `SessionRepo` like any other backend.
- `pi-agent` always exposes the lightweight search contract. `search-sqlite`
  forwards `pi-agent-search/sqlite-fts`; `sqlite` forwards the session backend.
- `pi-telemetry` owns contracts, typed schema and sensitivity metadata, no-op
  behavior, and in-memory reference/conformance implementations. It owns no
  network, file, or vendor exporter.

## Pre-authenticated extension endpoint

`pi-extension-protocol` owns wire DTOs, framing, negotiation, generations,
bounded queues, credit, cancellation, and an endpoint over an already ordered
transport. Wire DTOs are projections, not aliases of durable Agent/AI types.
Endpoint construction requires a host-supplied authenticated peer identity,
connection generation, granted capability set, and ordered byte transport.

`pi-harness-runtime::extensions` owns wire/domain conversion, live invocation
bindings, authorization checks, and settlement mapping. The extension protocol
does not claim process launch/termination, transport authentication, OS sandbox,
environment/network isolation, or secret brokering. Those belong to a future
privileged host design. A self-declared manifest id is never authentication, and
runtime correctness never depends on killing a peer process.

## Forbidden dependencies

- `pi-agent-schema` MUST NOT depend on session, runtime, providers, or protocol.
- `pi-harness-core` MUST NOT depend on storage implementations, network, clock,
  extension connections, or tool implementations.
- `pi-agent-session` MUST NOT depend on harness runtime.
- `pi-ai-core` MUST NOT depend on the built-in provider registry.
- `pi-ai-providers` MUST NOT mutate session or operation state.
- Search MUST consume repository reads; repositories MUST NOT depend on search.
- `pi-coding-runtime` MUST NOT define core Agent/Harness schema.
- `pi-extension-protocol` MUST NOT depend on Agent, AI, storage, or Harness
  crates; runtime adapters translate its wire projections.

## Split triggers

A module becomes a separate crate only if at least one holds:

- it requires independently optional heavy dependencies;
- it is independently versioned or consumed;
- it creates an otherwise unavoidable dependency cycle;
- it owns a distinct security or platform boundary;
- it needs a separate conformance implementation;
- it changes under a different authority/lifecycle.

File count and line count are not split triggers.
