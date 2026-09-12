# Generic migration contract

This is SYNTHETIC engine/atomic-store conformance, not a historical schema or
repository-open implementation. Production current version is 1 with no mappings.
The version 1→2→3 fixtures use explicitly SYNTHETIC owned JSON payloads under all
12 current namespaces. They do not represent valid historical OperationState.

The production `MigrationRegistry` acquires `MigrationStore::Lease`, reads actual
stored state, validates the entire remaining consecutive chain, and visits every
register in namespace/key order. A converter can return a value only; the engine
retains exact key/seq envelopes. The lease publishes one complete candidate plus
its version atomically, then the engine rereads the recorded version. Failed
later steps retain completed earlier steps as the next call's resume point.

The next Repository adapter must provide the actual exclusive writer lease,
complete detached reads, source-version/key/token fencing and definitive atomic
value+version publication. It must preserve entries, usage, allocation and other
metadata, and keep ownership until admitted publication finishes even if its
waiting future is dropped. These operations are separate from Storage's ten
public methods. Session publication and codec/header-policy validation remain
Repository/Session responsibilities; there is no Memory-specific adapter here.

Generic values MUST be detached immutable owned data, such as owned JSON or owned
version-aware DTOs. An arbitrary Clone implementation or shared interior-mutable
buffer is not an isolation guarantee. Converters are trusted deterministic pure
mappings; Rust cannot prevent an implementation from capturing arbitrary host
side effects. No time/id/provider/tool/hook capability is supplied. Future
operation mappings need their schema owner's ADR/review and must preserve pending
effect identity/recovery obligations; registering a closure does not authorize
pending-effect rollback to pre-intent. No actual historical mapper ships.

`migration-contract.json` binds literal expected version/visit traces to pinned
Harness lines 866, 2725, 2736, 2739–2748, 2754–2758 and 2763–2768. The synthetic
adapter stages owned canonical state before one publication and records detached
observations only in tests. Tests cover malformed registries, complete-chain
preflight, newer/unknown/equal versions, all-register visitation, failure at every
position of both steps, commit failure, retained resume point, idempotent reopen,
empty register sets, stable immutable domains and content-free diagnostics.
