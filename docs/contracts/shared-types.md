# Shared type contract

Authority: pinned upstream `853a80d26c90a14c1886f0ebb8ffaae133ca2185` and the accepted architecture.

- [AI DTOs](ai-domain/types.md)
- [Agent/Harness types](agent-types.md)
- [Hook type and error contracts](hook-types.md)

## Frozen cross-cutting decisions

- Rust/Cargo `1.98.0`, edition 2024, resolver 3.
- Open identities are validated non-empty string newtypes; known values are
  constants, never closed enums.
- Closed discriminants and closed durable/security DTO fields reject unknown
  values. Explicit extension maps remain open.
- JSON numbers use `SafeUnsigned`, `SafeSigned`, or `FiniteF64`; no `usize`, NaN,
  or infinity crosses a wire/durable boundary.
- `Presence<T>` distinguishes missing from present JSON `null` when upstream
  uses `field?: JsonValue` or another nullable optional value.
- `LiteralTrue` represents fields typed `?: true`; `false` is invalid.
- Raw provider headers are transient. Durable Harness headers preserve the JSON
  map shape only after host-policy classification as public; default deny.
- `scan_branch_structure` is part of `Storage`.
- `SessionMetadata.storeGeneration` resolves the pinned spec inconsistency and
  is recorded as a divergence.
- Storage/session errors are non-serialized, non-exhaustive Rust categories.
- Storage v1 contains no invented `migrated_effect_pending`; first define it
  with the first v2 migration ADR.
- Generated catalog values require the dated input/source/output provenance;
  schema completeness alone does not prove captured or historical value parity.

## Acceptance shape

S02 is complete only when the export/schema inventory, serde golden fixtures,
unknown-discriminant failures, required-null/optional-absence behavior, numeric
bounds, and default/all-feature workspace gates pass. Traits may be declared but
must not have fake successful implementations.
