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
operations are reused by the native Durable adapter. MCP provides stdio and
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

`pi-durable-session` implements committed records, atomic document transactions,
immutable publications, history, definition-free forks and incarnation-bound
observation over Memory and SQLite. Its decoded operation subset follows the
necessary Chord contracts; the full Chord protocol and JavaScript proxy are outside
scope. The [Session contract](contracts/durable-session.md) records native API and
Unicode boundaries, durable ID allocation and the source comparison limits.

Session stores task records without executing handlers. The generic scheduler in
`pi-durable` adds persistent task definitions, invocation leases, ownership and wait
graphs, sleep, cancellation and recovery over this single persistence authority.
Its [contract](contracts/durable-scheduler.md) distinguishes normal close from
interruption and describes the selected source evidence.

The [conversation runtime](contracts/durable-runtime.md) adds initialized builtin
documents, an immutable phase registry, host input receipts/queues, pinned model
generation, retry/deferred recovery, ordinary and keyed nested tools, committed
progress, usage and manual/automatic compaction. It owns its execution policy and
constructs neither Agent nor CodingSession. [Context and views](contracts/durable-views.md)
project committed history and five builtin documents; agent events consume coherent
current view/task baselines and complete publications. Pending watch/event delivery
converges after 100 batches, while lossless internal observation ingress can grow.

[Ordinary native bindings](contracts/durable-bindings.md) call existing CodingTool
operations directly, use a host-owned initialized MCP client, and support the closed
`durable.v1` native extension profile with seven hooks and streaming tools. These
compositions do not claim a literal port of upstream Durable's JavaScript APIs,
tools or environment modules.

[Persistent local CodeMode](contracts/durable-codemode.md) runs the shared native
worker with committed start/fingerprint, arrival-ordered journal, count-before-effect
and every-held-identity gates. It reuses keyed nested tool receipts and commits
success-only rewindable store changes with the decided result. Native QuickJS-NG
identity and script stacks differ from source QuickJS-WASI; replay requires an equal
native fingerprint. Typed raw-argument validation and JSON-schema declaration text
are explicit native variances. Normal SQLite close/reopen and replay policies do not
establish OS-crash recovery or exactly-once external effects. The host supplies the
worker and environment; automatic model-catalog, classifier and image-generation
helpers and remote executors remain excluded.

Public live TaskGraph mount/watch and Durable-specific telemetry schema spans/events/
metrics remain deferred. Shared AI RequestRuntime propagation and native `on_report` are available;
they do not implement that telemetry schema. The old
experimental Harness has been retired; a temporary compatibility layer is not
part of this upgrade.

## Verification boundary

Offline tests use synthetic model responses, local subprocesses and loopback
servers. Source comparisons read immutable Git objects or integrity-bound release
artifacts. Historical transport fixtures retain their original provenance and
are distinguished from v1.4.0 differential observations. Live provider accounts,
paid calls, public protocol stability and general upstream-package parity are
not certified. See [verification](verification-plan.md) and the crate READMEs
for the exact native adaptations.
