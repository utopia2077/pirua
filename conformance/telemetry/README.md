# Telemetry conformance matrix

This matrix records the S13-C1C audit against the pinned upstream telemetry
contract at `853a80d26c90a14c1886f0ebb8ffaae133ca2185`. It is source-backed
conformance evidence; it does not invent a callback API behind the frozen Rust
guard ABI.

## Contract mapping

| Contract | Rust witness | Expected invariant |
| --- | --- | --- |
| Detached span identity and parentage | `crates/pi-telemetry/tests/contract.rs:391-429` | Root/child IDs, parent IDs, event order, explicit status, and end order are stable. |
| Detached inputs and snapshots | `crates/pi-telemetry/tests/contract.rs:431-502` | Caller mutation and snapshot mutation never alter recorder state. |
| Schema serialization and duplicate rejection | `crates/pi-telemetry/tests/contract.rs:504-600` | Parsed and exact schema JSON covers all six scalar/list shapes, required/optional fields, escaping, sensitivity, cardinality, values, elementValues, examples, and duplicate span names. Raw schema/value helpers remain separate from remote projection redaction. |
| Public facade | `crates/pi-agent/tests/telemetry_facade.rs:1-33` | The public `pi_agent` surface can construct and inspect a root/child trace. |
| Runtime durable trace equality | `crates/pi-harness-runtime/tests/runtime_telemetry.rs:231-249,753-817` | Telemetry observation does not change durable IDs, transactions, or admitted write publication. |
| Runtime lifecycle/order | `crates/pi-harness-runtime/tests/runtime_telemetry.rs:114-229,418-494,634-703` | Parentage, finish order, retry attempts, close/drop/unwind, and provider failures remain complete. |
| Receipt no-replay under observer failure | `crates/pi-agent-session/tests/session_deferred_observation.rs:111-264,328-373` | Deferred and immediate observations match; the committed snapshot is serialized before a panicking receipt, remains byte-for-byte identical afterward, and the consumed receipt cannot emit/replay a second observation. Observer panics propagate at the existing receipt boundary. |
| Remote queue result isolation | `crates/pi-extension-protocol/tests/profile_live.rs:519-577,618-669` | Overload, malformed ack, flush, and capability errors disable only the telemetry sink and preserve unrelated capability state. |
| Schema freshness | `crates/pi-extension-protocol/schema/verify.sh` | Protocol descriptors, derived tables, activation variants, and Node digests remain fresh. |

## Focused commands

```text
cargo test -p pi-telemetry --locked --offline
cargo test -p pi-agent --test telemetry_facade --features sqlite --locked --offline
cargo test -p pi-agent-session --test session_deferred_observation --locked --offline
cargo test -p pi-harness-runtime --test runtime_telemetry --locked --offline
cargo test -p pi-extension-protocol --test profile_live --locked --offline
sh crates/pi-extension-protocol/schema/verify.sh
```

The telemetry and session commands above include the bounded S13-D1 witness
on top of `f27aa7b`; the unrelated facade/runtime/profile commands retain
their prior receipts from `16143fc` plus `ee48ddc`. The unchanged
`./scripts/verify.sh` remains reserved for S13-D assembly.

## Boundary decision

The matrix accepts sink-local `Result` failure isolation and the new direct
durable snapshot/no-replay witness. It preserves the existing observer-panic
boundary: a custom observer panic propagates to its caller, while the
serialized durable authority remains byte-for-byte unchanged and the consumed
receipt cannot be replayed. Raw schema/value helpers are intentionally distinct
from the remote projection's redaction boundary. Full upstream callback
lifecycle typing and Agent-owned AI/Harness schema constants remain open work.
