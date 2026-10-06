# R11/R12 surface and visibility conformance matrix

This matrix is an audit-only acceptance of the existing Harness v3 surface
owners. It uses real Memory, JSONL, and SQLite fixtures and keeps private
operation/register state out of public projections.

| Surface row | Existing witness | Result |
| --- | --- | --- |
| First attachment and live-state hydration | `crates/pi-harness-runtime/tests/runtime_first_attachment.rs` (12 tests) | Memory and normalized v3 JSONL seed/lease/CAS/reopen behavior passes; rejected/unpolled paths write nothing. |
| Memory/JSONL repository and attachment parity | `crates/pi-agent-session/tests/runtime_capability.rs` (7), `memory_repository.rs` (3), `jsonl_process.rs` (2) | stamps, attachment, reopen, fork/process publication, and writer recovery pass. |
| SQLite durable-prefix and reopen parity | `crates/pi-agent/tests/runtime_sqlite_projection.rs` (2), `runtime_structural_sqlite.rs` (6) | exact visible metadata, expiry refusal, compaction/navigation, and physical reopen preservation pass. |
| Public queue/event/snapshot model | `crates/pi-harness-runtime/tests/r5_r7_public_model.rs` (7), `r8_r10_public_model.rs` (7) | queue consumption, cancellation, reopen, compaction, navigation, and visible reference parity pass. |
| Settings/resources/watch/actions and backend pairs | `crates/pi-harness-runtime` `runtime_extension_profiles` target (216 passed, 4 ignored) | Memory/SQLite × profile pairs cover resources, snapshots, actions, hooks, telemetry, wrong-scope/version/id refusals, and no-write negatives. |
| JSONL runtime provider/hook/telemetry parity | `crates/pi-harness-runtime/tests/runtime_extension_profiles.rs::real_jsonl_provider_hook_telemetry_profile_parity_all_minor_pairs` (1 test; 8 backend cases) | Real `JsonlStorage` version-1 files use checked lane-register writes and the same provider+hook lifecycle as the Memory/SQLite witness; normal completion and telemetry sink-local failure/refusal pass across protocol core/minor pairs. The witness does not claim the full JSONL resource/action/watch or crash matrix. |
| Public/private omission | `runtime_extension_profiles` `runtime_native_host_resource_get_*`, `runtime_native_telemetry_bad_*`, and `crates/pi-extension-protocol/tests/profile_live.rs` | private resource/telemetry faults disable or refuse only the target capability; unrelated state remains usable. |

## Focused commands

```text
cargo test -p pi-harness-runtime --test runtime_first_attachment --locked --offline
cargo test -p pi-harness-runtime --test r5_r7_public_model --locked --offline
cargo test -p pi-harness-runtime --test r8_r10_public_model --locked --offline
cargo test -p pi-agent-session --test runtime_capability --locked --offline
cargo test -p pi-agent-session --test memory_repository --locked --offline
cargo test -p pi-agent-session --test jsonl_process --locked --offline
cargo test -p pi-agent --test runtime_sqlite_projection --features sqlite --locked --offline
cargo test -p pi-agent --test runtime_structural_sqlite --features sqlite --locked --offline
cargo test -p pi-harness-runtime --test runtime_extension_profiles --features extensions --locked --offline
```

All listed commands passed on the current source. The unchanged
`./scripts/verify.sh` remains reserved for S13-D assembly.

## Boundary decision

No new public/private API or backend transaction code is required by this
matrix. S13-B1 is therefore accepted as audit-only evidence. A future change
that needs a new public projection, JSONL backend adapter, or durable
operation/register exposure must reopen the B slice and stop for a new contract.
