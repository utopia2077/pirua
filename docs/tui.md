# Original Pi terminal UI with the Rust backend

Pirua retains Pi v1.4.0's TypeScript terminal library, `InteractiveMode`, editor,
components, tool renderers, themes and HTML export. A frontend session facade
connects that interface to `pi-acp-server`. The Rust process runs CodingSession,
providers and tools and owns session history. The frontend does not import or
execute the upstream JavaScript agent backend.

## Build and launch

Use the checked-in Rust toolchain and Node.js **22.19.0 or newer**. From the
repository root:

```sh
cargo build -p pi-acp-server --locked
npm ci
npm run build
node packages/pi-tui-client/dist/main.js --server "$PWD/target/debug/pi-acp-server"
```

If `CARGO_TARGET_DIR` is set, substitute its binary path. An absolute server path
also works when `--cwd` selects another project. The built client resolves its
assets relative to its installed modules, so the current directory can be the
project you want to edit. Keep both built workspaces and their npm dependencies
together; these private packages are currently consumed from the checkout.

The launcher accepts:

| Option | Effect |
| --- | --- |
| `--server PATH` | Native executable; defaults to `PIRUA_ACP_SERVER` or `pi-acp-server` on `PATH` |
| `--server-arg ARG` | Pass one argument to the native host; repeat for flags and values |
| `--cwd DIR` | Absolute project directory for the session and local UI reads |
| `--data-dir DIR` | Native data directory passed to the host |
| `--session ID` | Reopen a stored session by its server ID |
| `--prompt TEXT` | Submit an initial message after the UI starts |
| `--regular`, `--fullscreen` | Select the original terminal layout |

For example, select an explicit catalog model with
`--server-arg --model --server-arg provider/model-id`. `/model` opens the current
catalog in the UI. Model and thinking changes apply to the current native session;
the default-saving shortcuts do not persist global execution defaults.

## Credentials and storage

The host composes the native OpenAI, Google Gemini, Anthropic and DeepSeek
integrations, plus any OpenAI-compatible providers configured in `models.json`
(see below). Existing environment credentials are available through
`OPENAI_API_KEY`, `GEMINI_API_KEY`, `ANTHROPIC_API_KEY` and `DEEPSEEK_API_KEY`.
Use `/login` for a host-advertised API key or supported OAuth interaction.
OpenAI ChatGPT and Anthropic subscription OAuth use the native authentication
implementations. The client receives authentication prompts, not a copy of the
stored credential database.

The default native data directory is `$XDG_DATA_HOME/pirua`, falling back to
`~/.local/share/pirua`. It contains `sessions.sqlite`, `credentials.sqlite` and
the installation identifier used by native authentication. Configuration comes
from `$XDG_CONFIG_HOME/pirua`, falling back to `~/.config/pirua`; the host reads
`settings.json`, `models.json` and native resource directories there. `--config-dir` overrides
that location. `--credentials PATH` explicitly imports the native credential-map
format into the host credential store; it does not search an upstream Pi account
directory. `/logout` removes stored credentials for all host-managed providers.
Inherited environment credentials remain active until removed from the process
environment.

`PIRUA_UI_DATA_DIR` selects the frontend's appearance preferences, themes,
keybindings and diagnostics; its default is `~/.local/share/pirua/tui`. These
settings are independent of native execution settings. Sessions live in the
native store and are selected by ID; the frontend never edits JSONL files.

## OpenAI-compatible gateways

`models.json` in the configuration directory adds providers on the retained
`openai-completions` wire, such as a new-api gateway in front of DeepSeek or vLLM.
It accepts this subset of Pi's `models.json`:

```json
{
  "providers": {
    "gateway": {
      "name": "Gateway",
      "baseUrl": "https://gateway.example/v1",
      "api": "openai-completions",
      "apiKey": "$GATEWAY_API_KEY",
      "compat": { "supportsDeveloperRole": false },
      "models": [
        { "id": "deepseek-v4-flash", "reasoning": true, "contextWindow": 1000000, "maxTokens": 32768 }
      ]
    }
  }
}
```

- A provider needs `baseUrl`, an absolute http or https URL without credentials,
  query, fragment or surrounding whitespace; `api` set to `openai-completions`; and at
  least one model. `name`, `apiKey` and `compat` are optional.
- A model needs `id`; `name`, `reasoning`, `thinkingLevelMap`, `input`, `cost`,
  `contextWindow`, `maxTokens` and `compat` are optional. Unset fields take Pi's
  defaults: the id as name, no reasoning, text input, zero cost, a 128000-token
  context window and 16384 output tokens. Fields may be omitted but not `null`, and
  names may not be empty.
- Model `compat` overrides provider `compat`. `chatTemplateKwargs`,
  `chatTemplateArgs`, `openRouterRouting` and `vercelGatewayRouting` merge per key.
- `apiKey` is a literal or interpolates `$NAME` and `${NAME}` from the environment
  at request time; `$$` and `$!` produce `$` and `!`. An unset or empty variable
  leaves the provider without a key, and a provider without a key refuses prompts,
  so give a server that needs no key any placeholder value. A key stored with
  `/login` takes precedence.
- Provider ids use ASCII letters, digits, `.`, `_` and `-`, and cannot name a
  built-in provider in any letter case. Providers are listed in file order. Other
  upstream fields, such as `headers`, `oauth`, `authHeader`, `modelOverrides`,
  `!command` keys and per-model `api` or `baseUrl`, are rejected, as are JSON comments.
- The host reads the file once at startup; restart it after editing. An invalid file
  stops startup before anything is written, naming the file, provider and model.

Select a configured model with `--server-arg --model --server-arg gateway/deepseek-v4-flash`
or `/model`. DeepSeek reasoning is read whether it arrives as `reasoning_content`
(DeepSeek's API) or `reasoning` (vLLM), and is sent back under the same field. These
settings select how thinking is requested:

| Endpoint | `compat` and model settings |
|---|---|
| Gateway relaying DeepSeek's official API | `{"thinkingFormat":"deepseek","requiresReasoningContentOnAssistantMessages":true,"supportsDeveloperRole":false,"maxTokensField":"max_tokens","supportsStore":false,"supportsStrictMode":true}` with `"reasoning": true` and a built-in model's `thinkingLevelMap`, such as `deepseek-v4-pro`'s `{"minimal":null,"low":null,"medium":null,"high":"high","max":"max"}` |
| vLLM 0.21 serving DeepSeek V4, directly or through a gateway that keeps `chat_template_kwargs` and assistant `reasoning` (new-api does) | `{"thinkingFormat":"chat-template","chatTemplateKwargs":{"thinking":{"$var":"thinking.enabled"},"reasoning_effort":{"$var":"thinking.effort","omitWhenOff":true}},"supportsDeveloperRole":false}` with `"reasoning": true`, the `thinkingLevelMap` above and a `contextWindow` no larger than the server's `max_model_len` |
| Gateway model alias whose parameter override fixes thinking on or off | `{"supportsDeveloperRole":false,"requiresReasoningContentOnAssistantMessages":true}` with `reasoning` matching the alias; new-api applies overrides only with request pass-through off |

vLLM's DeepSeek V4 encoder renders a `developer` message as a user turn, and in
thinking mode without tools drops it before the last user message; hence
`supportsDeveloperRole`. vLLM's DeepSeek V4 chat template maps the `max` and
`xhigh` efforts to its maximum, `none` to thinking off, and every other effort to
high. A gateway must keep reasoning in its own
field; in new-api, leave ThinkingToContent off. The
[recorded streams](../conformance/provider-traces/openai-completions/recorded/README.md)
document both reasoning formats and the vLLM contracts behind these settings.

## Local demonstration without an account

The fixture provider is an explicit verification option. It is never selected
as a fallback for absent credentials. Create a temporary directory and launch:

```sh
demo_dir="$(mktemp -d)"
node packages/pi-tui-client/dist/main.js \
  --server "$PWD/target/debug/pi-acp-server" \
  --data-dir "$demo_dir/data" \
  --server-arg --config-dir --server-arg "$demo_dir/config" \
  --server-arg --fixture-provider \
  --prompt 'hello from the original UI'
```

This streams a deterministic thought and response through the real Rust process.
`fixture:tool` runs a native local bash tool with fixed output. `fixture:block`
waits until cancellation, allowing the normal Escape/cancel interaction to be
exercised. Exit with Ctrl-D from an empty editor. The demo does not contact a
provider; it uses the same protocol, input admission and SQLite paths as the
normal host. The temporary directory remains available for reopening its sessions.

## Supported controls and limits

The original interface supports text/thought/tool streaming, vision input,
steering and follow-up queues, cancellation, model and thinking selection,
session reopening, tree navigation, before/at forks, compaction, session names,
entry labels, native shell execution and local HTML export. `/settings` combines
local appearance choices with the execution options actually advertised by the
host. Resource reload and scoped model selection use explicit native operations.

Themes, clipboard, editor integration and presentation callbacks are client-local.
Path completion, Git status and predictive edit diffs also retain local reads.
This deployment assumes that the UI and server share a working directory on the
same machine. A remote filesystem, remote sandbox and network server deployment
are not part of this launcher.

The host does not provide upstream JavaScript agent extension loading, package
installation, JSONL import/export, external transcript sharing, issue uploads,
session deletion, writable trust policy or persistent global model/thinking
defaults. Commands that require these capabilities explain the limitation.
Renderer and dialog callbacks may be registered explicitly on the frontend;
they do not install or run a JavaScript backend. The existing headless MCP,
CodeMode and language-neutral extension libraries remain available to embedding
hosts; this launcher does not add configuration surfaces for all of them.

## Protocol and source updates

The stdio protocol is newline-delimited JSON-RPC 2.0 using the **ACP v2 draft**
pinned in [source.json](../conformance/acp-v2/source.json). It is not a claim of
stable ACP v2 compatibility. Ordinary ACP clients can use standard lifecycle,
configuration, prompt and content/tool updates. The original Pi UI negotiates
`pirua.dev/tui` version 1 for coherent snapshots, queue identity, tree mutations,
shell execution, scoped models and resources. Standard conversation streaming
does not require these extensions.

An accepted prompt returns its committed message ID before generation finishes.
The session state update reports final completion, cancellation or failure.
Cancelling an uninserted request withdraws that input; cancelling a session stops
its active work. The [design](design/tui-acp-v2.md) specifies these distinct
boundaries, snapshot cursors and replay behavior.

Pi source attribution and original/adapted hashes are recorded in the
[frontend manifest](../packages/pi-tui-client/upstream-source.json). The original
terminal library lives in [pi-tui](../packages/pi-tui). Changes to the ACP pin
require reviewing both endpoints against the new official schema and lifecycle
semantics. Changes to the Pi pin require reviewing the original UI dependency
closure, refreshing source evidence and rerunning extraction and terminal checks.
The Rust ownership map continues to describe the headless port; the frontend
manifest separately records retained TypeScript source.

## Verification

After `cargo fetch --locked` and `npm ci`, run:

```sh
bash scripts/verify-tui.sh
```

This builds the real host, checks pinned ACP artifacts, runs independent stdio
lifecycle tests, builds and typechecks the frontend, runs client and retained
terminal tests, audits the source/import boundary and assets, and exercises both
terminal modes through real PTYs. The integration tests use isolated temporary
data/config directories and explicitly select the deterministic provider.

`bash scripts/verify.sh` includes this entry point after the existing complete
Rust verification. Production provider/auth composition is covered by native
offline tests. Live account login, paid provider calls, platform-specific native
clipboard helpers and non-POSIX terminal behavior are not certified by the
local integration suite. See [Development](development.md) for prerequisites.

## Relationship to Durable

The application host is a new consumer of the accepted Agent/Coding runtime.
It does not depend on Durable crates, task records, scheduling, recovery or
publication APIs. Shared changes provide tracked input admission, canonical
session-history observation and a small initialization correction that keeps
initial tool declarations in canonical request history. Storage schemas and
existing event wire shapes are unchanged. The workspace also contains an independent Durable runtime. A Durable application
host can reuse the generic tools and protocol facilities while owning its own task
and recovery semantics; this Coding-session host does not expose Durable sessions.
