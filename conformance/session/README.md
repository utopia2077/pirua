# Reusable session storage contract

`backend.rs` takes a `BackendFactory` and an independent observation sink. Memory
runs it from `pi-agent-session/tests/memory_storage.rs`. Future JSONL/SQLite
adapters implement the same factory and invoke the same logical runners; their
physical crash, lease and query-plan tests remain separate later-slice work.

`storage-contract.json` contains literal input writes and expected query IDs,
bound to the pinned Harness v3 clauses. No production query helper generates
expected results. `model.rs` separately enumerates 4,096 four-action histories
(16,384 steps) over register set/delete, exact-seq/absence conditions, shared-ID
entry/usage inserts with a staged prefix, and empty commits. It independently
tracks allocation/presence/tokens and checks full rollback snapshots.

The `ready` executor is deliberately a synchronous-first-poll assertion for
Memory. Persistent adapters override `BackendFactory::execute` with their
own executor while retaining the identical logical runners/fixtures/model; they must not copy
Memory's physical first-poll claim without implementing it.

Memory lifecycle and pure publication-validation cases additionally cover both
forced CAS contenders, close during a clock call, validator reentry/retry,
complete staged pending/placed validation, unpolled drop, coherent snapshots,
classified-header custody, import integrity and safe numeric exhaustion.

These tests do not implement Session/Repo/context, historical migration, JSONL,
SQLite, or transaction telemetry. Full repository acceptance is solely
`./scripts/verify.sh`; focused package tests are diagnostic evidence.
