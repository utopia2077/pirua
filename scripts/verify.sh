#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

export CARGO_NET_OFFLINE=true
export RUSTFLAGS="${RUSTFLAGS:+${RUSTFLAGS} }-D warnings"
export RUSTDOCFLAGS="${RUSTDOCFLAGS:+${RUSTDOCFLAGS} }-D warnings"

python3 scripts/generate_upstream_manifests.py --check
python3 scripts/generate_ai_conformance.py --check
python3 scripts/generate_selected_catalogs.py --check
python3 scripts/validate_docs.py
python3 -m unittest scripts/test_validate_docs.py
python3 -m unittest scripts/test_generate_ai_conformance.py
python3 -m unittest scripts/test_generate_selected_catalogs.py
python3 -m unittest scripts/test_catalog_cli.py
python3 -m unittest scripts/test_check_workspace.py
python3 scripts/check_workspace.py
cargo fmt --all -- --check
cargo check --workspace --all-targets --locked --offline
cargo check --workspace --all-targets --all-features --locked --offline
cargo clippy --workspace --all-targets --locked --offline -- -D warnings
cargo clippy --workspace --all-targets --all-features --locked --offline -- -D warnings
cargo test --workspace --all-targets --locked --offline
cargo test --workspace --all-targets --all-features --locked --offline
cargo test --doc -p pi-ai-core --locked --offline
cargo doc --workspace --no-deps --document-private-items --locked --offline
cargo doc --workspace --all-features --no-deps --document-private-items --locked --offline
