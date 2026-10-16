# AI data at Pi v1.4.0

Authority: Pi tag `v1.4.0`, commit
`4a080252f06caf63a525d7873c5d94d3c23887e0`. `pi-ai-core` owns bounded JSON,
provider-neutral messages, models, options, and process-local request ports.
Required discriminants and finite/safe number admission remain enforced at
serde and constructor boundaries. Unknown fields are rejected on closed DTOs;
open provider/API/model/tool identities still admit nonempty custom strings.

| Rust type | Serialized data |
|---|---|
| `SystemMessage` | `role:"system"`, string/text-block content, optional ordered `sections`, `toolsAdded`, `toolsRemoved`, required timestamp |
| `UserMessage` | `role:"user"`, string or text/image content, timestamp |
| `AssistantMessage` | assistant content, provider/API/model identity, usage/stop state, optional response identity, logical/provider thinking level and `durationMs` |
| `ToolCall` | id, name, object arguments, optional `argumentsError`, namespace and thought signature |
| `ToolResultMessage` | call/tool identity, text/image content, details/usage, error state, optional `nestedCalls` and `durationMs`, timestamp |
| `Context` | optional shorthand prompt/tools and ordered messages |
| `TranscriptContext` | normalized ordered messages; shorthand prompt/tools are folded into the leading system message |
| `DeferredHandle` | provider/model/API identity, provider token, optional expiry/poll interval and presence-aware data |

`SystemSections` preserves declaration order and replaces duplicate values in
their first slot. A section value of `null` removes that named section when
replayed. Tool removal precedes additions in each system message, and replacing
an existing declaration retains its position unless it was removed first.
Built-in `Message` admits system, user, assistant, and tool-result messages.
Custom roles use a distinct registry-aware envelope and cannot shadow those
built-in roles. Vision input stays within text-model requests.

`nestedCalls` records complete/incomplete nested tool activity, with id/name,
optional arguments or byte count, status (`ok`, `error`, `unfinished`), optional
duration/error, and a `complete` flag. `addedToolNames` is removed. Finished
malformed tool arguments retain the entire text in `argumentsError`; validation
rejects that diagnostic before schema validation.

Models retain optional `type:"chat"`, request/image `inputLimits`, prompt-cache
metadata, thinking mappings, and `samplingParamsByThinkingLevel`. Compatibility
is decoded only for the retained Completions, Responses, and Anthropic message
protocols. New capabilities describe anchored system/tool changes, managed
effort, and output-token support. Google selects level/budget controls using the
release's model rules and mappings. Image-generation models, options, contexts,
results, and adapter interfaces are absent.

`ClientIdentity` contains optional user-agent presentation and version, plus
presence-aware attribution. `Presence::Missing` inherits, `Null` disables, and
`Value` replaces attribution. Ordinary optional non-null fields still reject
explicit null. Retry policy's optional `maxAgentDelayMs` caps exponential agent
retry delays at 60 seconds by default, with safe overflow handling.

Detailed callable behavior and ownership are documented in
[the current runtime facade](../ai-integration/runtime-facade.md) and
[transport boundary](../ai-integration/transport-adapters.md). Historical schema
fixtures are selected and updated for this boundary; provider release source
and differential evidence live in the release-specific conformance corpus.
