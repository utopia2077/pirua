# Scope

Pirua implements a selected headless surface of Pi v1.4.0
(`4a080252f06caf63a525d7873c5d94d3c23887e0`) as Rust libraries. Upstream source,
contracts and tests establish observable behavior; Rust traits, ownership and
storage layouts are native interfaces. A source path mapped to a Rust owner
identifies responsibility, not a claim that every symbol in that path is ported.
The optional terminal application retains the original TypeScript presentation
code and connects it to those libraries through a Rust ACP v2 application host.

## Selected provider scope

| Provider | Protocol | Authentication |
|---|---|---|
| OpenAI | Responses | API key; optional ChatGPT OAuth |
| Google Gemini | Gemini | API key |
| Anthropic | Messages | API key; optional Anthropic OAuth |
| DeepSeek | OpenAI-compatible Completions | API key |

The catalog is a projection of the corresponding v1.4.0 published package,
restricted to chat models for these four providers. Text and vision input,
streaming, reasoning controls, tool calls and usage are included. Classifier
and generated-image APIs, OpenRouter, old OpenAI Codex, other builtin providers,
dedicated excluded-provider wires and workload-identity federation are excluded.
The [factory contract](contracts/ai-integration/factories.md) lists exact features.

## Agent and Coding

The standalone Agent owns transient execution, events, queues and tool hooks.
CodingSession composes it with model routing, retry, resource/system prompts,
context projection, compaction, branch navigation, usage and cache warming.
SQLite and Memory provide complete current session semantics. Memory is an
in-process store for tests or temporary sessions; its records disappear when the
store is dropped. Neither backend reads JSONL or converts older schemas.

Coding tools include local read, write, edit, bash, find, grep and ls, with
explicit execution environment and output limits. Runtime-independent tool
operations are reusable by a later Durable adapter. MCP provides stdio and
Streamable HTTP clients and explicit OAuth; CodeMode uses one local worker.
The host chooses which tools a session exposes and supplies executable paths,
credentials, transport configuration and process policy.

Language-neutral extensions use authenticated grants, schema-bound messages,
ordered hooks and registered tools. The current Coding profile is distinct from
historical experimental Harness profiles. There is no JS/Jiti loader or legacy
payload adapter. Browser/Cloudflare/SSH execution and host package management
are excluded.

## Terminal application

`pi-acp-server` composes native providers, credentials, coding tools, resources and
SQLite sessions. `packages/pi-tui` retains the upstream terminal library;
`packages/pi-tui-client` adapts the original InteractiveMode, components and
renderers to a read-only frontend mirror. Prompt execution, tools, queue ownership
and session mutation remain in Rust. The two processes speak the pinned ACP v2
draft plus versioned Pi extensions for queues, snapshots, tree operations and shell.

The application supports streaming text/thought/tool output, cancellation, steering
and follow-up, model/thinking selection, session reopening, tree navigation, forks,
compaction, labels, session names and local HTML export. Local UI preferences,
themes, keybindings, clipboard and presentation callbacks remain in TypeScript.
This launch mode assumes a same-machine working directory for completion and
predictive diff reads; it does not establish remote filesystem support.

JSONL import/export, upstream JavaScript backend extensions, package management,
external transcript sharing, issue uploads and persistent default-model editing
are outside this host contract. Unsupported operations report their capability
limit. The native headless ownership map remains separate from the
[frontend source manifest](../packages/pi-tui-client/upstream-source.json).
See [the terminal guide](tui.md) and [protocol design](design/tui-acp-v2.md).

## Durable boundary

The new `packages/durable` runtime is a separate implementation phase after the
Agent/Coding phase. Its intended scope is current documents, tasks, transactions,
publications, scheduler/recovery, SQLite/Memory and persistent local CodeMode
journals, using only the necessary Chord contracts. The old experimental Harness
has been retired; a temporary compatibility layer is not part of this upgrade.

## Verification boundary

Offline tests use synthetic model responses, local subprocesses and loopback
servers. Source comparisons read immutable Git objects or integrity-bound release
artifacts. Historical transport fixtures retain their original provenance and
are distinguished from v1.4.0 differential observations. Live provider accounts,
paid calls, public protocol stability and general upstream-package parity are
not certified. See [verification](verification-plan.md) and the crate READMEs
for the exact native adaptations.
