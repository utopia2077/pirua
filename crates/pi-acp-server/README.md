# Native ACP v2 application host

`pi-acp-server` composes the existing Rust `CodingSession`, selected provider
factories, native authentication/transports, SQLite conversations, resources,
settings and local coding tools. The executable reads and writes newline-delimited
JSON-RPC 2.0 on stdin/stdout. Diagnostics use stderr.

Build from the workspace with `cargo build -p pi-acp-server`, then launch
`target/debug/pi-acp-server`. The frontend normally owns this child process and
its pipes. The child does not read the terminal.

The default data directory is `$XDG_DATA_HOME/pirua`, or
`$HOME/.local/share/pirua`. The default configuration directory is
`$XDG_CONFIG_HOME/pirua`, or `$HOME/.config/pirua`. Available options are:

- `--data-dir PATH`: conversation and credential SQLite databases.
- `--config-dir PATH`: settings, resources and `models.json` providers.
- `--credentials PATH`: explicitly import an existing native Pi credential JSON
  object, keyed by provider. Existing stored provider credentials take precedence.
- `--model provider/model`: choose an exact model from the selected captured catalog
  or `models.json`.
- `--fixture-provider`: explicitly select the deterministic offline provider.
- `--fixture-delay-ms N`: delay fixture stream steps, defaulting to 25 ms.

Production includes the accepted OpenAI, Google, Anthropic and DeepSeek provider
scope. It uses their native API-key/OAuth ports. A missing credential produces an
actionable setup error and never selects the fixture provider. The process accepts
provider environment configuration through the existing native authentication
context. Upstream Pi credential paths are not discovered implicitly.

`models.json` in the configuration directory may add providers on the retained
`openai-completions` wire, such as OpenAI-compatible gateways; the accepted subset of
Pi's format is described in [the terminal guide](../../docs/tui.md#openai-compatible-gateways).
Configured providers cannot redefine a built-in provider, and an invalid file stops
startup with the file, provider and model named.

When the client advertises form elicitation, initialization advertises the real
selected authentication methods. `auth/login` calls the existing provider login
port and sends request-scoped `elicitation/create` requests; the input receive
pump continues handling responses and cancellation. `auth/logout` removes stored
credentials for all selected providers. It cannot remove environment values from
the process that launched the server. Credentials never appear in snapshots or
successful authentication responses.
A form the client declines or cancels fails the login with `-32800`.
A form left unanswered when its prompt is abandoned is withdrawn with a `$/cancel_request`
notification.
Each method's `_meta["pirua.dev/auth"]` carries `subscription` and, while the provider has
configured authentication, `status` (`type` and `source`, from local credential knowledge
only). Pi snapshots repeat the list as `authMethods` so a client can refresh it after login
or logout.

The wire contract is the repository's pinned baseline ACP v2 schema, implemented
with `agent-client-protocol-schema` 1.12.0 and its explicit
`unstable_protocol_v2` module. See
[the accepted application design](../../docs/design/tui-acp-v2.md) for method and
DTO details. No v1 lifecycle conversion is provided. Optional MCP setup,
additional directories, session deletion and global settings persistence are not
advertised.

`session/prompt` returns the real persisted user-entry ID after logical insertion.
Its tracked input receipt is independent of the owned generation future. Final
outcome comes from standard identified content, tool, terminal, compaction and
state updates. Cancellation of the prompt request uses the ACP
`requestId` field and only withdraws input before insertion; session cancellation
ends foreground work and withdraws its remaining queued cohort.

Each open session has one projection owner. Its shared observation bridge
serializes detached history commits, runtime callbacks and request barriers.
Snapshots derive canonical messages/context/statistics from the observed history,
and include materialized active entities at the returned epoch/sequence. Full
native assistant partials remain private during token streaming and are attached
to materialized snapshot entities so text/thought slots retain their native
indices. Standard ACP remains useful without Pi capability negotiation.

Resource reload replaces an idle runtime owner from the same SQLite conversation
and preserves session configuration. This applies changed project context files
as well as prompt templates and skills. Fork creates a distinct owner and
conversation, preserving the source owner. Configuration changes apply to the
current session; model/thinking history is retained in that conversation.

The deterministic fixture recognizes normal text and these test inputs:

- `fixture:block`: wait after insertion until cancelled.
- `fixture:before-insert`: wait in input preprocessing until cancelled.
- `fixture:tool`: execute a native local `bash` tool, then return text.
- `fixture:retry`: fail the first stream with a retryable error, then succeed.
- `fixture:length`: finish with the maximum-token stop reason.
- `fixture:error`: finish with a provider error after insertion.
- `fixture:multi-slot`: emit text, thought, text in distinct native slots, then wait.
- `fixture:transform TEXT`: transform admitted text before queueing/insertion.
- `fixture:handled`: handle an input without a retained user message.
- `fixture:context`: answer with the user messages the provider was given, oldest first, as a JSON array.

`cargo test -p pi-acp-server` runs the pinned positive/negative contract fixtures,
real child-process protocol scenarios, injected persistence failures, and real
production provider/auth composition with deterministic transport/interaction
ports. These tests perform no paid provider calls.
