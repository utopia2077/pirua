#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

cargo build -p pi-acp-server --locked --offline
acp_target_dir="$(cargo metadata --no-deps --format-version 1 --locked --offline | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')"
acp_server_bin="$acp_target_dir/debug/pi-acp-server"

python3 conformance/acp-v2/check_source.py
PIRUA_ACP_TEST_BIN="$acp_server_bin" node --test conformance/acp-v2/stdio-conformance.test.mjs
npm run build
npm run typecheck
PIRUA_TEST_SERVER="$acp_server_bin" npm test
npm test --workspace=@earendil-works/pi-tui
npm run check:extraction --workspace=@pirua/pi-tui-client
python3 packages/pi-tui-client/test/pty-smoke.py "$acp_server_bin" regular
python3 packages/pi-tui-client/test/pty-smoke.py "$acp_server_bin" fullscreen
