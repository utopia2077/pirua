#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

export CARGO_NET_OFFLINE=true
export RUSTFLAGS="${RUSTFLAGS:+${RUSTFLAGS} }-D warnings"
export RUSTDOCFLAGS="${RUSTDOCFLAGS:+${RUSTDOCFLAGS} }-D warnings"

python3 scripts/generate_upstream_manifests.py --check
python3 scripts/generate_assembly_registry.py --check
export PIRUA_UPSTREAM_ROOT="$(python3 scripts/upstream.py)"
node conformance/agent-v1.4.0/oracle.mjs --check
node conformance/agent-v1.4.0/causal-check.mjs
node conformance/agent-proxy-v1.4.0/oracle.mjs --check
node conformance/coding-tools/source.mjs "$PIRUA_UPSTREAM_ROOT" --check
node conformance/coding-session/source.mjs "$PIRUA_UPSTREAM_ROOT" --check
node conformance/coding-session-runtime-v1.4.0/source.mjs "$PIRUA_UPSTREAM_ROOT" --check
node conformance/coding-tool-adapters-v1.4.0/source.mjs "$PIRUA_UPSTREAM_ROOT" --check
node conformance/durable-v1.4.0/session.mjs --check
node conformance/durable-v1.4.0/causal-check.mjs
node conformance/durable-v1.4.0/scheduler.mjs --check
node conformance/durable-v1.4.0/scheduler.mjs --causal
node conformance/durable-v1.4.0/runtime.mjs --check
node conformance/durable-v1.4.0/runtime.mjs --causal
node conformance/durable-v1.4.0/codemode.mjs --check
node conformance/durable-v1.4.0/codemode.mjs --causal
python3 scripts/upstream_drift.py
python3 scripts/check_assembly.py
python3 -m unittest scripts/test_upstream_drift.py
python3 -m unittest scripts/test_check_assembly.py
python3 conformance/catalog/v1.4.0/check.py --check
python3 conformance/catalog/v1.4.0/check_negative.py
python3 scripts/validate_docs.py
python3 -m unittest scripts/test_validate_docs.py
python3 -m unittest scripts/test_catalog_cli.py
python3 -m unittest scripts/test_check_workspace.py
python3 scripts/check_workspace.py
python3 scripts/check_publication.py --self-check
python3 scripts/check_publication.py
cargo fmt --all -- --check
cargo check -p pi-ai -p pi-coding-runtime --no-default-features --all-targets --locked --offline
cargo check --workspace --all-targets --locked --offline
cargo check --workspace --all-targets --all-features --locked --offline
cargo clippy --workspace --all-targets --locked --offline -- -D warnings
cargo clippy --workspace --all-targets --all-features --locked --offline -- -D warnings
cargo test --workspace --all-targets --locked --offline
cargo test --workspace --all-targets --all-features --locked --offline
cargo test --workspace --doc --locked --offline
cargo test --workspace --doc --all-features --locked --offline
cargo doc --workspace --no-deps --document-private-items --locked --offline
cargo doc --workspace --all-features --no-deps --document-private-items --locked --offline
bash scripts/verify-tui.sh
