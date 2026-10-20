# Development and verification

Run commands from the repository root. The current upstream selection is Pi
v1.4.0; [scope](scope.md) and crate READMEs define the supported APIs.

## Feature and host boundaries

`pi-ai` has empty default features. `provider-openai`, `provider-anthropic`,
`provider-google` and `provider-deepseek` select a builtin catalog and wire.
`oauth-openai-chatgpt` and `oauth-anthropic` select the two subscription login
flows. `native` enables transport; `auth-native` also enables native OAuth hosts.
See [factory features](contracts/ai-integration/factories.md). The standalone
Agent's `proxy` feature enables its injected HTTP proxy stream.

CodingSession accepts a model and stream, or explicitly configured registry-backed
services. The host supplies tool execution context, settings/resources and hooks.
Memory and SQLite share current session semantics; neither imports older data.
MCP clients and local CodeMode workers are independent libraries attached through
session adapters. CodeMode requires a matching `pi-codemode-worker` executable;
the host passes its path explicitly. Credentials, extension grants, transport
configuration and any operating-system sandbox remain host responsibilities.

## Build and test

The checked-in toolchain is Rust 1.98.0 with rustfmt and Clippy. Cache dependencies
before running offline:

```sh
rustup toolchain install 1.98.0 --profile minimal --component rustfmt --component clippy
cargo fetch --locked
cargo fetch --manifest-path crates/pi-extension-protocol/codegen/Cargo.toml --locked
```

Keep build output in one reusable directory and disable incremental/debug output
when disk space matters:

```sh
export CARGO_TARGET_DIR="$PWD/target"
export CARGO_INCREMENTAL=0
export CARGO_PROFILE_DEV_DEBUG=0
export CARGO_PROFILE_TEST_DEBUG=0
export CARGO_BUILD_JOBS=4
cargo check --workspace --all-targets --locked --offline
cargo check --workspace --all-targets --all-features --locked --offline
cargo run -p pi-agent --example offline_agent --locked --offline
cargo run -p pi-coding-runtime --example headless_session --locked --offline
cargo run -p pi-durable --example offline_durable --all-features --locked --offline
```

The examples use local synthetic model responses. After all builds and helper
processes have stopped, `cargo clean --target-dir "$CARGO_TARGET_DIR"` removes
that cache. Do not clean a target directory while another worktree is building.

## Full verification

The suite also uses Bash, Git, Python 3.11 or newer with `protobuf` and
`jsonschema`, Node.js with native TypeScript stripping (tested with 26.8.1), npm,
and `protoc` 3.20.3. Local process tests use a POSIX environment; some lifecycle
probes inspect Linux `/proc`. Prepare a Git object store containing both the
current and historical fixture revisions:

```sh
git clone https://github.com/earendil-works/pi.git upstream/pi-mono
git -C upstream/pi-mono checkout --detach 4a080252f06caf63a525d7873c5d94d3c23887e0
npm ci --prefix upstream/pi-mono --ignore-scripts --no-audit --no-fund
npm ci --prefix conformance/provider-traces/v1.4.0 --ignore-scripts --no-audit --no-fund
node conformance/agent-v1.4.0/setup.mjs --install
node conformance/durable-v1.4.0/setup.mjs --install
```

The source readers use immutable Git objects. A full clone includes the older
fixture baseline; a shallow clone may need an explicit fetch of that history.
The drift analyzer uses the checkout's installed, lock-bound TypeScript parser
and records its identity. Release oracle dependencies have their own lockfiles
and do not use that upstream `node_modules` to execute provider or Agent code. Source oracles neither sign in nor contact
a provider. The exact catalog archive, integrity and source association are
recorded in [catalog provenance](../conformance/catalog/README.md).

The default checkout is `upstream/pi-mono`. An existing checkout can be selected
with `PIRUA_UPSTREAM_ROOT`; additionally configure the local Git setting for tests
that deliberately clear the child environment:

```sh
export PIRUA_UPSTREAM_ROOT=/path/to/pi
git config --local pirua.upstreamRoot /path/to/pi
```

Run the full suite:

```sh
bash scripts/verify.sh
```

The entry point checks source ownership, manifests, assembly definitions, catalog
provenance, documentation and workspace topology, then default/all-feature
compilation, Clippy, tests and Rust documentation with warnings denied. See the
[verification guide](verification-plan.md) for evidence boundaries. Live account
access, paid calls and service availability are not tested.

The Durable Session, scheduler, conversation runtime, native binding and persistent
CodeMode suites can also run independently after their shared setup:

```sh
cargo build -p pi-codemode --bin pi-codemode-worker --locked --offline
cargo test -p pi-durable -p pi-durable-session -p pi-extension-protocol -p pi-codemode --all-features --locked --offline
node conformance/durable-v1.4.0/session.mjs --check
node conformance/durable-v1.4.0/causal-check.mjs
node conformance/durable-v1.4.0/scheduler.mjs --check
node conformance/durable-v1.4.0/scheduler.mjs --causal
node conformance/durable-v1.4.0/runtime.mjs --check
node conformance/durable-v1.4.0/runtime.mjs --causal
node conformance/durable-v1.4.0/codemode.mjs --check
node conformance/durable-v1.4.0/codemode.mjs --causal
```

The [Session oracle](../conformance/durable-v1.4.0/README.md) distinguishes
published-package observations from the native allocation and Unicode contracts.
The [scheduler oracle](../conformance/durable-v1.4.0/scheduler.md) documents its
task scenarios, source identities and recovery evidence limits.
The [runtime oracle](../conformance/durable-v1.4.0/runtime.md) provides 18 cases
and 30 published release/backend executions. Native tests run the corresponding
actual Memory/SQLite runtime scenarios and separately test bindings, automatic
compaction, committed events and attachment lifetimes. Those runtime source
observations do not execute native extension/MCP transport or establish public
TaskGraph or Durable telemetry schema behavior. Persistent CodeMode uses separate
[source evidence](../conformance/durable-v1.4.0/codemode.md), containing
21 cases/29 source/backend runs: 17 real-worker cases/23 runs, one document case/two
runs, and three scripted protocol cases/four runs. The native fixture consumer runs
the real-worker subset with actual Memory/SQLite; separate private units exercise
document policies and native equivalents of the scripted decisions. Source placeholders
are not presented as executed JavaScript. The [CodeMode contract](contracts/durable-codemode.md)
records native worker identity, typed raw-argument variance and replay/effect limits.
The [runtime contract](contracts/durable-runtime.md) and [binding contract](contracts/durable-bindings.md)
record the usable native APIs and remaining limits.

## Upstream drift

The local tool compares immutable commits without fetching or changing the pin:

```sh
python3 scripts/upstream_drift.py
python3 scripts/upstream_drift.py --old FULL_OLD_COMMIT --new FULL_NEW_COMMIT
```

No arguments compares the pin to itself. Exit 2 means classified changes await
review, 3 means invalid or uncertain inputs, and 4 means objects/tools are
unavailable. New paths require intake even when a broad route matches them.
Acknowledgements and bound proof receipts are required before a guarded candidate
patch can be produced. The tool never applies its patch. See the
[drift guide](upstream-drift.md) for the review and receipt formats.

## Assembly consistency

```sh
python3 scripts/check_assembly.py
python3 -m unittest scripts/test_check_assembly.py
```

Assembly checks source-to-owner/proof definitions and current executable test
anchors. They execute zero registered proof commands. Passing definitions,
source observations and passing native test executions are separate evidence.
Historical transport fixtures keep their original provenance; the selected
v1.4.0 suites run separate current-release observations.

Required test identities and Cargo profiles have separate immutable enrollments:
the original Agent floor at `f8fc564` and the Durable floor at `e7fed05`.
The Durable floor binds nineteen commands, including the actual private library
units and shared extension, VM and AI regressions. Regeneration may add evidence;
retiring an enrolled identity, command or feature/dependency binding requires an
explicit reviewed baseline change. Both enrollments select Linux obligations
independently of the discovery host.

## Publication checks

The bounded publication scanner checks repository paths, stored private keys and
known internal metadata. `--history` includes reachable Git objects;
`--self-check` runs isolated rejection controls. Findings report paths, hashes
and categories without matching secret values:

```sh
python3 scripts/check_publication.py --history
python3 scripts/check_publication.py --self-check
```
