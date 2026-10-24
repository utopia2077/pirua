# Pirua original Pi terminal client

This package runs the original Pi v1.4.0 `InteractiveMode`, components, built-in tool renderers,
editor, themes, images, session tree and HTML export against the Rust `pi-acp-server`.
The sibling `@earendil-works/pi-tui` workspace contains the complete pinned terminal library.
The JavaScript process contains presentation code and an ACP v2 client; provider execution,
shell tools, credentials, queues and session persistence belong to the Rust process.

From the repository root, with Node 22.19.0 or newer:

```sh
npm ci
npm run build
node packages/pi-tui-client/dist/main.js --server ./target/debug/pi-acp-server
```

Use `--server-arg ARG` repeatedly for server options, `--data-dir DIR` for the native session
store, `--cwd DIR`, `--session ID`, `--prompt TEXT`, and `--regular` or `--fullscreen`.
`PIRUA_ACP_SERVER` supplies the default server binary. `PIRUA_UI_DATA_DIR` selects local UI
preferences, themes, keybindings and diagnostics; the default is `~/.local/share/pirua/tui`.
The server uses its configured native provider credentials. The launcher never chooses a
fixture provider automatically.

For a deterministic local run, explicitly select the native fixture provider:

```sh
node packages/pi-tui-client/dist/main.js \
  --server ./target/debug/pi-acp-server \
  --data-dir /tmp/pirua-example \
  --server-arg --config-dir --server-arg /tmp/pirua-example/config \
  --server-arg --fixture-provider
```

The full Pi UI requires the server's `pirua.dev/tui` version 1 snapshot capability. Standard
ACP session updates carry the conversation and tool lifecycle; small renderer metadata
preserves the native content slots and results needed by the original components. A detached
snapshot supplies synchronous read mirrors. Session selection uses server IDs. Queue removal,
labels, configuration changes and tree operations await actual host results.

UI preferences and renderer/widget/dialog callbacks are client-local. `PresentationRegistry`
accepts explicit local presentation callbacks; it does not load third-party JavaScript agent
extensions. The host advertises execution configuration and authentication methods. Logout
removes stored credentials for all host-managed providers; inherited environment credentials
remain active. Local autocomplete, git status and predictive diff reads assume a local stdio
workspace. A remote working directory is outside this launcher's compatibility claim.

`/export` produces a local HTML document from a refreshed native snapshot. JSONL import/export,
external transcript sharing, issue uploads, package installation, a third-party agent extension
loader and writable trust policy are unavailable in this backend scope. Their retained command
handlers report the limitation. Session deletion has no filesystem backdoor. Unsupported
execution settings are omitted from the settings menu; local appearance controls remain.
Persistent global model, thinking and model-scope defaults are outside the present host contract;
normal selection changes are applied to the current native session.

The pinned terminal library retains its optional native clipboard/paste helpers and original
fallbacks. Basic terminal rendering does not require rebuilding native helpers. Image conversion
uses the pinned optional platform implementation from Photon when needed.

Verification:

```sh
npm run typecheck
npm run build
npm test
npm test --workspace=@earendil-works/pi-tui
npm run check:extraction --workspace=@pirua/pi-tui-client
PIRUA_TEST_SERVER="$PWD/target/debug/pi-acp-server" npm test
python3 packages/pi-tui-client/test/pty-smoke.py "$PWD/target/debug/pi-acp-server" regular
python3 packages/pi-tui-client/test/pty-smoke.py "$PWD/target/debug/pi-acp-server" fullscreen
```

Use absolute server paths: npm workspace tests and the PTY launcher change working directory.
If `CARGO_TARGET_DIR` is set, replace `$PWD/target` with the absolute path to that build directory.
The real host test creates temporary stores and explicitly enables the fixture provider; without that
variable it is skipped. The PTY check invokes the built production entry point and verifies
fixture rendering, exit and terminal cleanup.

Upstream: [Pi](https://github.com/earendil-works/pi), version 1.4.0,
commit `4a080252f06caf63a525d7873c5d94d3c23887e0`, MIT license, Mario Zechner and contributors.
`upstream-source.json` records original source hashes, extracted fragments and adapted output
hashes. Original retained test assertions are preserved; imports are relocated, and synthetic
home path literals use `/workspace` to comply with publication rules.
