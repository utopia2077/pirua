# ACP v2 contract inputs

`source.json` pins the official ACP draft schema, method metadata, and license.
The files are copied byte-for-byte from that revision, not regenerated from a newer
SDK. `schema.json` is the v2 baseline at the selected draft, not the separate set of
experimental features. The Rust endpoint uses `agent-client-protocol-schema`
`=1.12.0` with `unstable_protocol_v2`; this feature alone does not advertise optional
experimental operations to a client.

Run `python3 conformance/acp-v2/check_source.py` from the repository root to check
the pin without network access. Protocol tests validate request/response shapes
against these definitions and separately exercise ordering, cancellation and
insertion semantics. Schema validation cannot establish lifecycle correctness.

The integration contract is [Pi TypeScript TUI over ACP v2](../../docs/design/tui-acp-v2.md).
ACP's Apache-2.0 license is retained in `LICENSE-ACP`.

`stdio-conformance.test.mjs` is an independent client: it imports neither the
production server nor the TypeScript facade. Build `pi-acp-server`, then run:

```sh
PIRUA_ACP_TEST_BIN="$PWD/target/debug/pi-acp-server" \
  node --test conformance/acp-v2/stdio-conformance.test.mjs
```

The tests create temporary SQLite/config directories and explicitly enable the
native fixture provider. They check early insertion, pre/post-insertion
cancellation, duplicate queue identity, restart/replay ordering, forks, complete
live snapshots, retry/final state, malformed requests, batches and active close.
The host's Rust tests separately validate schema fixtures and native composition.
`scripts/verify-tui.sh` combines these cases with the original frontend and PTYs.
