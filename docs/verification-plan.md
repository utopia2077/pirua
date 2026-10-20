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
| Durable Session | Release-backed storage/document/fork observations, native commit/cancellation/failure tests and SQLite reopen |
| Durable scheduler | Release-backed task/ownership/wait/recovery observations, native interruption, cancellation, lifecycle and lease tests |
| Durable conversation runtime and context/views | 18 source cases / 30 release/backend executions; actual native Memory/SQLite generation, tool, progress, recovery, context and view comparisons; separate event and automatic compaction tests |
| Durable native bindings | Direct Coding operations, host-owned MCP discovery/calls, closed durable.v1 grants/hooks and credit after actual Memory/SQLite progress commits |
| Durable persistent CodeMode | Source 21 cases/29 runs with real-worker/document/scripted categories; native real-worker comparisons, journal/count/identity gates, successful store/forks and separate document/protocol equivalents |
| MCP | Actual stdio peer, loopback HTTP/OAuth, cancellation, correlation and shutdown tests |
| Local CodeMode | Actual helper processes, capability/output/store limits and controlled delivery |
| Language-neutral extensions | Actual protocol frames, grants, correlation, ordered hooks and native admission |

The [proof registry](../conformance/assembly.json) defines commands and their
source/test anchors. `check_assembly.py` checks these definitions; it does not
execute them or manufacture a passing receipt. Full Rust tests run their declared
source oracles. Additional standalone oracle and causal commands are documented
beside the corresponding fixtures.

Independent immutable proof floors retain the original Agent enrollment at
`f8fc564` and nineteen accepted Durable/shared commands at `e7fed05`, including
the actual library units. Registry regeneration cannot retire their qualified
identities, command profiles or feature/dependency bindings. Both floors select
Linux obligations independently of the current host; they establish required
definitions, while execution still needs passing test receipts.

The Durable runtime source checks are
`node conformance/durable-v1.4.0/runtime.mjs --check` and
`node conformance/durable-v1.4.0/runtime.mjs --causal`; the full entry point runs
both. They retain the frozen Session/scheduler observations and validate the
runtime's independent release identities. Native integration targets cover
conversation-record observation, context, views, runtime scenarios/fixtures and
bindings; the extension suite also enrolls `durable_host`. The registry also binds
actual private Durable unit tests through
`cargo test --locked --offline -p pi-durable --lib --all-features`, using the
distinct target identity `@lib`.
Its anchors follow the default library source and nested modules with `cfg(test)`
active; custom or disabled library paths/harnesses are outside this bounded model.
Registration checks definitions and bindings; passing execution comes from Cargo.

The [persistent CodeMode source suite](../conformance/durable-v1.4.0/codemode.md)
runs through `node conformance/durable-v1.4.0/codemode.mjs --check` and
`node conformance/durable-v1.4.0/codemode.mjs --causal`, both in the full entry point.
Its 21 cases/29 source/backend runs distinguish 17 real-worker cases/23 runs, one
document case/two runs and three scripted protocol cases/four runs. The actual
native library enrollment contains eight CodeMode units in addition to the ten
existing private Durable units. Its frozen fixture consumer runs the real-worker
subset; separate units exercise native document/protocol equivalents, count and
late-call gates, raw-argument replay validation and the emitted-unawaited-call repair.
See the [native contract](contracts/durable-codemode.md) for exact comparison limits.

## Build quality

Both default and all-feature profiles must pass formatting, compilation, Clippy,
all target tests and Rust documentation with warnings denied. A source observation
is an expectation, not implementation evidence until the native scenario is run
and compared. Negative tests exercise meaningful boundaries such as stale grants,
invalid drafts, owner collisions, cancellation and transaction failures.

## Limits

Historical parser/transport observations are intentionally retained where current
native tests consume them. They do not become v1.4.0 evidence by changing a label.
Language-level differences and omitted upstream products are explicit in crate
coverage documents and [scope](scope.md). Live service behavior, account access,
production performance and exactly-once external side effects are not established
by this offline suite.
Public live TaskGraph mount/watch, Durable-specific telemetry schema spans/events/
metrics remain deferred. Shared AI RequestRuntime
propagation and native reports do not establish that schema's parity. Neither the
runtime corpus nor native Coding/MCP/extension compositions certify every member
of a mixed upstream file, literal JavaScript bindings/tools/environment APIs,
every interleaving, an end-to-end memory bound or OS-crash recovery.
CodeMode's native QuickJS-NG fingerprint and script stacks differ from source
QuickJS-WASI. Typed malformed raw-tool arguments have an explicit journaled native
validation outcome; declarations show JSON schemas rather than compiled TypeScript.
Recorded replay and normal SQLite close/reopen do not guarantee exactly-once unsafe
effects or exclusive cross-process output-path reservation. Source scripted protocol
cases and their separately labeled native equivalents retain distinct evidence.
