# Scope

Pirua implements a selected headless surface of Pi v1.4.0
(`4a080252f06caf63a525d7873c5d94d3c23887e0`) as Rust libraries. Upstream source,
contracts and tests establish observable behavior; Rust traits, ownership and
storage layouts are native interfaces. A source path mapped to a Rust owner
identifies responsibility, not a claim that every symbol in that path is ported.

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
payload adapter. CLI/TUI, HTML export, browser/Cloudflare/SSH execution and host
package management are excluded.

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
interruption and describes the selected source evidence. The conversation runtime
and persistent local CodeMode journals are the remaining Durable implementation. The old
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
