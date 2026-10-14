# ADR-0003: Consolidate the mirror into fifteen production crates

## Context

The initial architecture used 26 crates so every synchronization boundary had a
Cargo boundary. The project owner judged that count excessive and requested a
30–50% reduction.

## Decision

Use 15 production crates, a 42% reduction. Preserve finer upstream boundaries as
internal modules and in `upstream-map.toml`.

## Selected crates

```text
pi-telemetry
pi-extension-protocol
pi-ai-core
pi-ai-transport
pi-ai-providers
pi-ai
pi-agent-schema
pi-agent-loop
pi-agent-session
pi-agent-sqlite
pi-harness-core
pi-harness-runtime
pi-agent-search
pi-agent
pi-coding-runtime
```

## Alternatives

### 26 crates

Maximum isolation and direct mapping, but excessive dependency, release, and
coordination surface.

### 10–12 crates

Lower overhead, but merges session backends, AI wire adapters, pure planning,
runtime effects, and coding integration across materially different dependency
and security boundaries.

### 15 crates

Retains independent boundaries for protocol, provider transport, SQLite,
Harness pure core, runtime effects, search, and coding composition while moving
algorithm/resource/provider subfamilies into modules.

## Consequences

- Crate boundaries no longer encode every upstream directory.
- Internal modules and the upstream map become the synchronization index.
- `pi-harness-runtime` and `pi-coding-runtime` are larger crates with strict
  internal ownership rules.
- A module may split later only under an explicit dependency, versioning,
  security, platform, or conformance trigger.
