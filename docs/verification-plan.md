# Verification

The verification entry point is `bash scripts/verify.sh`, run from the repository
root after the prerequisites in [Development](development.md) are prepared.
Tests run offline. No API key, OAuth login or paid provider call is required.

## Structural checks

The machine [crate topology](../workspace-topology.toml) is checked against Cargo
metadata with default and all features. Checks reject missing or hidden local
packages, renamed/retargeted dependency edges, cycles, unexpected optional feature
activation and provider-selection changes. Documentation links, source ownership,
manifest freshness and proof registration are checked independently.

## Behavior and source evidence

| Boundary | Evidence |
|---|---|
| AI contracts and transport | Native unit/integration cases and retained protocol fixtures |
| Four provider protocols and two OAuth flows | Exact v1.4.0 release/source observations and Rust comparisons |
| Model catalogs | Published archive integrity, source attestation and selected chat-model projection |
| Standalone Agent | Exact-tag event/request/queue/hook observations, native concurrency tests and proxy cases |
| Coding tools and resources | Exact-tag helper observations plus real filesystem/process cases |
| Coding session | Prompt/tool/save/reopen/fork/compact, hook admission, retry, routing and cache tests |
| SQLite and Memory | Logical session projection, transaction rollback and reopen tests |
| MCP | Actual stdio peer, loopback HTTP/OAuth, cancellation, correlation and shutdown tests |
| Local CodeMode | Actual helper processes, capability/output/store limits and controlled delivery |
| Language-neutral extensions | Actual protocol frames, grants, correlation, ordered hooks and native admission |
| ACP application host | Pinned v2 schema, real stdio lifecycle/cancel/replay/queue cases, native provider/auth composition |
| Original TypeScript terminal UI | Retained original tests, source/import audit, stream reducers, real Rust integration and regular/fullscreen PTYs |

The [proof registry](../conformance/assembly.json) defines commands and their
source/test anchors. `check_assembly.py` checks these definitions; it does not
execute them or manufacture a passing receipt. Full Rust tests run their declared
source oracles. Additional standalone oracle and causal commands are documented
beside the corresponding fixtures.

## Build quality

Both default and all-feature profiles must pass formatting, compilation, Clippy,
all target tests and Rust documentation with warnings denied. A source observation
is an expectation, not implementation evidence until the native scenario is run
and compared. Negative tests exercise meaningful boundaries such as stale grants,
invalid drafts, owner collisions, cancellation and transaction failures.

AI and Coding libraries also compile independently with no default features.
The application host deliberately selects native provider features, so workspace
feature unification alone cannot establish that reusable library minimum.

## Limits

Historical parser/transport observations are intentionally retained where current
native tests consume them. They do not become v1.4.0 evidence by changing a label.
Language-level differences and omitted upstream products are explicit in crate
coverage documents and [scope](scope.md). Live service behavior, account access,
production performance and exactly-once external side effects are not established
by this offline suite.

The terminal verification entry point is `bash scripts/verify-tui.sh`, also called
by the full suite. ACP schema validity is separate from lifecycle correctness;
black-box stdio cases exercise both ordinary ACP clients and the negotiated Pi
extensions. Original frontend source provenance is recorded independently of the
native ownership map. Terminal verification uses deterministic native provider
responses and a real subprocess; it does not establish live OAuth account access
or all platform-specific clipboard and image helpers.
