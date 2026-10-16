# pi-ai

Rust facade for the selected Pi v1.4.0 AI surface. Default features are empty.
Enable `provider-openai`, `provider-anthropic`, `provider-google`, or
`provider-deepseek` for a builtin provider, or `all-providers` for all four.
`oauth-openai-chatgpt` and `oauth-anthropic` select subscription login flows;
`all-oauth` enables both. `native` enables native transport, and `auth-native`
also supplies native OAuth callback and cryptographic hosts.

The facade reexports `pi-ai-core`, including normalized system/tool transcripts,
client identity, telemetry span request parents, canonical assistant streams,
model/catalog contracts, and object-safe provider interfaces. The optional
`builtins` module explicitly composes configured providers; `catalog` exposes
release-pinned selected models. Construction has no ambient provider discovery.

There are no compatibility aliases, old Codex/OpenRouter builtin providers,
image-generation or classifier APIs, or dedicated excluded-provider wires.
User and tool-result vision input remains supported.

Feature/API details: [selected factories](../../docs/contracts/ai-integration/factories.md).
Release evidence: [offline provider oracle](../../conformance/provider-traces/v1.4.0/README.md)
and [catalog provenance](../../conformance/catalog/README.md).
