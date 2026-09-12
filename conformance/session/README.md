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

The generic Session implementation additionally runs `session-contract.json` from
`session_tree::literal_session_query_contract`, and `context-contract.json` from
`session_context::bounded_compaction_tail_filters_and_explicit_callback_order`.
These independent literal expectations bind to the same pinned Harness document.
The five `session_*` integration targets exercise codecs, all 15 tree and eight
Session methods, conditional append races, late pending/entry exclusion, context,
UUIDv7 follower checking, and actual telemetry composition over CheckedStorage.

Session telemetry is outcome-linked: one `pi.session.write` span is emitted after
an Applied result or Storage mechanism error. Prevalidation errors, staged Rejected
and NotApplied produce no span. Attributes contain only `pi.session.item_count`/`pi.session.item_kinds` and the
first/last committed sequence (omitted for empty transactions). These retrospective
spans do not measure commit duration or reproduce an admission timestamp. Observer
callbacks run after publication, outside storage locks.

`RuntimeSession::new(storage, metadata, codec, ids, telemetry)` validates detached
existing data; repository create/open/version gates and lifetime ownership remain
its caller's responsibility. It never creates main or discovers sessions. The
explicit host ID generator must supply fresh UUIDv7 tails; `id::mint` checks format
and exact follower timestamp but is not an adapter for S03's fallible generator.

Newly authored branch summaries require a source entry with an earlier committed
sequence than the summary, checked against the complete staged publication.
The direct-summary regression rejects self, forward and missing sources without
changing snapshots or emitting write telemetry; preexisting and earlier-in-tx
sources succeed. Imported off-path provenance is preserved without this new-write
check. This is the pre-navigation source constraint in pinned Harness lines631–635,
not interpretation of operation-state ownership.
