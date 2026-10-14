# Architecture

This is the only entry point for the target design. Spokes own independent
semantic boundaries; this hub maps them and does not duplicate their contracts.

Provider product coverage follows the [selected provider scope](../scope.md#selected-provider-scope).
Shared wire coverage does not create an obligation to include additional product factories.

## Design spine

Rust owns all durable agent and AI runtime state. Pure state planning is isolated
from effect execution. Built-in providers and tools implement Rust traits;
third-party or other-language implementations provide the same capabilities over
a versioned extension protocol. Upstream conformance is tracked by contracts and
observable fixtures rather than file-by-file source translation.

```text
pi-mono contract/spec/tests
            │
            ▼
      upstream-map + fixtures
            │
            ▼
  Rust AI runtime ───────► Rust provider/tool effects
            │                         │
            ▼                         │
    Harness pure core ────────────────┤
            │                         │
            ▼                         ▼
 Session/storage authority     Extension protocol
            │                         │
            └──────── Harness runtime ┘
                         │
                         ▼
                Coding headless runtime
```

## Spokes

| Document | Semantic role | Scope | Status |
|---|---|---|---|
| [Principles and scope](01-principles-and-scope.md) | behavior boundary and ownership | whole system | accepted |
| [Crate topology](02-crate-topology.md) | dependency and responsibility design | 15 crates | accepted |
| [AI subsystem](03-ai-subsystem.md) | interfaces and provider architecture | `packages/ai` mirror | accepted |
| [Agent and Harness](04-agent-harness.md) | durable state machine and runtime | Harness v3 + Agent loop | accepted |
| [Session and storage](05-session-storage.md) | persistence and migration contract | Memory/JSONL/SQLite | accepted |
| [Extension protocol](06-extension-protocol.md) | cross-language protocol | tools/hooks/providers/events | accepted |
| [Coding integration](07-coding-integration.md) | inclusion/exclusion boundary | `packages/coding-agent` | accepted |
| [Upstream synchronization](08-upstream-sync.md) | source authority and tracking | pi-mono follow strategy | accepted |
| [Conformance and security](09-conformance-security.md) | proof obligations and trust | all implementations | accepted |

## Decision records

| Decision | Status | Owner |
|---|---|---|
| [Use Rust as the only durable runtime authority](decisions/0001-rust-runtime-authority.md) | accepted | project owner |
| [Follow upstream by semantic authority](decisions/0002-upstream-authority.md) | accepted | project owner |
| [Consolidate the mirror into 15 production crates](decisions/0003-fifteen-crates.md) | accepted | project owner |

## Technical contracts

- [Transport and canonical adapters](../contracts/ai-integration/transport-adapters.md)
  refines transport custody, family option/middleware seams, cancellation
  snapshots and cumulative evidence without replacing the AI architecture.
- [Shared type contract](../contracts/shared-types.md) refines the pinned AI/Harness
  type inventories and their Rust projections.
  These contracts refine implementation detail without replacing upstream authority.

## Authority rules

- The accepted [scope](../scope.md) owns intent and product boundaries.
- This design set owns the target architecture.
- `upstream-map.toml` owns source-to-Rust tracking metadata.
- The future protocol schema owns wire compatibility; generated SDKs are
  projections.

## Explicit deferrals

- Public crate/protocol release compatibility is a separate decision.
- Provider credential brokering must be reviewed before enabling third-party
  provider extensions.
- A complete Rust CLI/TUI is outside this design set.
- Concrete queue, frame, and timeout defaults require runtime evidence.

## Readiness

The core state, AI/provider, session, extension, and upstream-sync boundaries are
accepted for implementation with explicit deferrals. Public release
compatibility is not approved.
