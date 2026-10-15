# Development and verification

For a project overview and a runnable local example, start with the
[README](../README.md). This guide covers feature selection, host boundaries,
build prerequisites, and the complete verification workflow. Run commands from
the repository root.

## Feature and host boundaries

The built-in product scope is six text entrances: `openai`, `google`,
`anthropic`, `deepseek`, `openai-codex`, and `openrouter`; the image entrance is
`openrouter-images`. OpenRouter models are retained regardless of their underlying
vendor. Additional shared wire adapters have conformance coverage, which does
not admit the corresponding provider products into this selected scope.

Provider, catalog, image, OAuth, and native transport features are explicit
choices. For example, `pi-ai` exposes `provider-openai`, `images-openrouter`,
`oauth-openai-codex`, and `native`; its default feature set is empty. Provider
selectors include their catalog and wire adapter, while OAuth is independently
selected. `pi-agent` offers `sqlite` and `search-sqlite`; `pi-coding-runtime`
offers selected `provider-*`, image/OAuth selectors, `sqlite-sessions`,
`remote-catalog`, and `extensions`. Inspect each crate's `Cargo.toml` before
selecting an umbrella such as `all-providers` or `compat`.

The current coding builder composes a host-owned `ModelsRegistry`,
`RuntimeFactory`, and session. Detached provider/model composition accepts
injected providers, catalog models, and host-owned credential capabilities;
required capabilities are checked before runtime admission. It does not build
provider transports or refresh credentials. Coding tools use an `ExecutionEnv`,
including a local implementation, so the host controls filesystem and process
access. These APIs provide headless library composition; the developer binaries
in `pi-ai` are catalog and OAuth utilities.

Extension endpoints require host-established authentication, grants, an ordered
channel, and finite limits. The coding adapter's remote tool registration is
currently limited to deterministic `echo`; provider profile registration is
catalog/no-auth only. The host supplies provider HTTP, runtime endpoints,
credentials, process lifecycle, transport authentication, and any OS sandbox or
network isolation. Durable recovery and cancellation do not promise exactly-once
external provider or tool effects. Telemetry supplies contracts and process-local
references; exporters are supplied by the host.

## Build and test

The checked-in toolchain is Rust `1.98.0`, with `rustfmt` and `clippy`. Install that
toolchain and populate Cargo's dependency cache before entering an offline
environment:

```sh
rustup toolchain install 1.98.0 --profile minimal --component rustfmt --component clippy
cargo fetch --locked
cargo fetch --manifest-path crates/pi-extension-protocol/codegen/Cargo.toml --locked
```

Once the dependencies are cached, compiling Rust targets requires no upstream
checkout or live provider credentials:

```sh
cargo check --workspace --all-targets --locked --offline
cargo check --workspace --all-targets --all-features --locked --offline
```

## Full verification

Tests also execute Node/Python peers and pinned upstream source oracles. Complete
verification requires a POSIX environment, Python 3.11 or newer with `protobuf`
and `jsonschema` installed, Bash, Git, Node.js, npm for dependency preparation,
and `protoc` reporting exactly `libprotoc 3.20.3`. Rust tests use POSIX process
groups and utilities including `kill`, `getconf`, and `sha256sum`. Prepare the
ignored repository-relative checkout while network access is available:

```sh
git clone https://github.com/earendil-works/pi.git upstream/pi-mono
git -C upstream/pi-mono checkout --detach 853a80d26c90a14c1886f0ebb8ffaae133ca2185
npm --prefix upstream/pi-mono ci --ignore-scripts
```

The repository-relative checkout above is the default. To use an existing
checkout containing that exact Git object and installed lockfile dependencies,
set `PIRUA_UPSTREAM_ROOT` for script checks. For the full suite, also record the
checkout in this repository's local Git configuration, since some Rust tests
clear their child environment:

```sh
export PIRUA_UPSTREAM_ROOT=/absolute/path/to/pi-mono
git config --local pirua.upstreamRoot /absolute/path/to/pi-mono
```

Scripts prefer the environment setting, then the local Git setting, then
`upstream/pi-mono` resolved relative to this repository. Source conformance reads
immutable pinned Git objects rather than working-tree source files. Prepare npm
dependencies from the pinned revision; the oracles also check installed SDK bytes.

The frozen catalog provenance records Node `v26.8.1`, ICU `78.3`, locale `en-US`,
and the Node executable SHA-256 in
[generated provenance](../conformance/catalog/generated/provenance.json). The
catalog check fingerprints `/usr/bin/node`; a different runtime or SDK build can
fail verification even if it is API compatible. These fixed provenance checks
must be retained when reproducing this snapshot.

After preparing those inputs, run all-feature Rust tests or the complete entry
point from the repository root:

```sh
cargo test --workspace --all-targets --all-features --locked --offline
bash scripts/verify.sh
```

It checks upstream ownership, local baseline drift, assembly consistency,
source/export/behavior inventories, captured catalog regeneration, documentation,
workspace topology, formatting, default and all-feature
builds, Clippy, tests, and generated Rust documentation. Cargo runs offline;
source oracles use captured feeds and synthetic local transports. The checks do
not prove live account access or ongoing provider service compatibility. Catalog
values describe the dated input capture, and the missing historical upstream
text-catalog inputs are explicitly excluded from value-parity claims.

## Upstream drift

The local drift tool compares immutable commits in the prepared Git object
database. With no arguments it compares the pin to itself. A candidate comparison
uses full commit ids and reports responsible owners and proof suites:

```sh
python3 scripts/upstream_drift.py
python3 scripts/upstream_drift.py --old FULL_OLD_COMMIT --new FULL_NEW_COMMIT
```

It returns 2 for classified changes awaiting review, 3 for invalid or uncertain
inputs, and 4 for unavailable objects or tools. Every added path needs explicit
intake, including a path already matched by a broad route. Complete acknowledgements
are required for exit 0. Guarded candidate patches additionally require bound
local proof receipts and complete applicable provenance. The tool performs no
fetch, executes no review-supplied command, and does not apply patches or change
the pin. See [comparison and review](upstream-drift.md) for the exact wire
formats, receipt boundaries and separate production pin-intake requirements.

## Assembly consistency

The pure assembly check resolves pinned paths, public exports, provider scope,
Harness selectors, legacy behavior and coding operations through the canonical
ownership map and [proof registry](../conformance/assembly.json):

```sh
python3 scripts/check_assembly.py
python3 -m unittest scripts/test_check_assembly.py
```

It validates current test targets and evidence anchors without executing the
registered proof commands. Accepted gaps remain visible, including full legacy
proxy/iterator parity, the bounded remote JSONL matrix, historical catalog feed
provenance and host/native certification. The five-row legacy actual-observation
bridge, twelve-row legacy corpus and eighteen-row coding corpus retain separate
denominators. See [upstream assembly consistency](architecture/08-upstream-sync.md#assembly-consistency)
for the exact inventory and evidence boundary.

## Publication checks

An additional publication check scans the index and current tracked/unignored
files for stored private keys, owner paths, private workflow records, and known
internal metadata. Add `--history` to include every reachable Git object and historical
path; `--self-check` runs isolated rejection controls. Findings contain paths,
hashes, and categories, without matching values. This bounded scanner supplements
the conformance entry point:

```sh
python3 scripts/check_publication.py --history
python3 scripts/check_publication.py --self-check
```
