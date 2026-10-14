# ADR-0002: Follow pi-mono by semantic authority, not file translation

## Context

Harness v3 is specified but not yet fully implemented. Current harness source
uses a record-log design that the v3 build order explicitly replaces. AI also
contains generated catalog projections and temporary compatibility surfaces.

## Decision

Each upstream surface is classified as target spec, source-and-tests, generator,
generated projection, compatibility, host-only, or disposable scaffold. Rust
owners track the authoritative semantic surface and observable fixtures.

## Rationale

File-to-file translation would port code scheduled for deletion, treat generated
artifacts as independent decisions, and make directory reorganization appear to
be an architecture change.

## Consequences

- The repository maintains one `upstream-map.toml`.
- Harness v3 may be spec-only until an upstream slice lands.
- Generated model data is imported from generator output.
- Rust implementations may differ internally when conformance remains equal.
- Every intentional semantic divergence is recorded explicitly.

## Source

Accepted by the project owner through the requirement to continuously follow
future pi-mono Harness v3 updates.
