# AI runtime facade

`pi_ai` reexports the current provider-neutral core. Optional `builtins` and
`catalog` modules expose only feature-selected factories and static catalog
lookup. The development `catalog_gen` binary validates the checked release
catalog offline. There is no AI compatibility namespace or AI-owned OAuth CLI.

Caller-facing `Context` accepts `system_prompt`, `messages`, and `tools`.
Registry dispatch calls `normalize_context`; provider-facing `TextStreamRequest`
and `SimpleStreamRequest` carry `TranscriptContext`. Its ordered message list
contains `Message::System(SystemMessage)` deltas with text, ordered named prompt
sections, full tool additions, and named tool removals. Providers preserve
anchored updates when their model capabilities permit them, otherwise replay
and collapse the current system state. `get_current_tools`,
`get_current_system_message`, `resolve_transcript`, and
`resolve_transcript_tools` expose the shared replay behavior.

`ModelsRegistryOptions.identity` supplies the default calling identity.
`StreamOptionsWire.identity` overrides it field by field, including simple and
captured lifecycle dispatch. `ClientIdentity.attribution` uses `Presence`:
`Missing` inherits, `Null` disables inherited attribution, and `Value` replaces
it. Native detailed User-Agent output reports Pi 1.4.0 and the Rust host runtime.
Model and explicit request headers retain their normal override precedence.

`RequestRuntime` carries cancellation, an explicit `Arc<dyn TelemetrySpan>`,
injected clock/spawner, and an optional `ProviderStreamEventObserver`. The
observer receives parsed provider JSON before assistant normalization; an error
settles the request. Canonical stream settlement attaches `duration_ms` to newly
produced terminal messages while retaining a forwarded existing duration and
avoiding timing older deferred messages.

Finished malformed tool arguments produce empty object arguments plus
`arguments_error`, including the complete received text. Tool validation rejects
that diagnostic before schema validation. `format_tool_validation_feedback(call,
error)` explicitly renders model-visible feedback with the original parse error,
or the shared validator's paths, keywords, native diagnosis and received arguments.
Typed error `Display` and `Debug` remain redacted. Schema diagnosis English follows
the Rust JSON Schema engine rather than TypeBox. Tool results may carry `nested_calls`
and `duration_ms`; assistant messages may carry logical/provider thinking levels.
SDK retry policy includes `max_agent_delay_ms`, defaulting to 60 seconds per
agent retry delay, with JavaScript-safe overflow handling and subscription-limit
retry vetoes.
