# Retained transport adapters

`OpenAIResponsesApi`, `AnthropicMessagesApi`, `GoogleGenAiApi`, and
`OpenAICompletionsApi` implement the four selected wire protocols. The same
configured instances are used by the selected provider factories. Shared
conversions, constrained tool sampling, vision input, cancellation snapshots,
retry/error classification, header ordering, and request/response lifecycle
capabilities remain owned by those adapters and the core/transport contracts.

Explicit `AdapterServices` captures transport, environment values, middleware,
and optional request-scoped handlers. Model endpoints are credential-free
public metadata; transient auth endpoints and credentials are assembled once
at dispatch. The Azure-only runtime endpoint sentinel has been removed.
Payload transformation runs at the provider-defined stage, and response
observation preserves body custody and provider-specific timing. Custom Fetch
retains record-versus-constructed-header semantics before a host is selected.

Parsed provider events reach the request runtime observer before assistant
normalization. Completed malformed tool calls retain their argument diagnostic;
intermediate non-object arguments project to empty objects at the bounded Rust
boundary. Canonical settlement owns one terminal message and its duration.
Native socket/listener behavior is tested with loopback peers, while exact
release protocol comparisons run against offline synthetic SSE.
