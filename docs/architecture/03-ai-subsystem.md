# AI subsystem

`pi-ai-core` owns detached models/messages/transcripts, validation, streams,
registry/auth contracts, retry and client identity. `pi-ai-transport` owns native
HTTP/SSE/WebSocket and related transport adapters. `pi-ai-providers` implements
four selected provider factories and two OAuth flows. `pi-ai` is the public
facade. `pi-telemetry` owns spans, context and process-local telemetry facilities.

Providers are OpenAI Responses, Anthropic Messages, Google Gemini and DeepSeek
Completions. Provider features select catalog and wire support; native transport
and OAuth are separately selected. Default provider features are empty. See the
[factory contract](../contracts/ai-integration/factories.md) for exact features,
services and authentication ownership.

Requests preserve normalized system sections and transcript updates, tool state,
client identity, requested reasoning, provider thinking and usage. Registry-backed
Coding composition resolves the effective model/auth/endpoint for a call instead
of caching a second mutable credential authority. Error feedback intended for the
model is distinct from redacted diagnostics intended for logs.

Catalogs come from the integrity-bound v1.4.0 published AI package and contain only
chat models for the four selected providers. [Catalog evidence](../../conformance/catalog/README.md)
records source association and projection. Target release protocol/OAuth evidence
is documented [beside the fixtures](../../conformance/provider-traces/v1.4.0/README.md).
Retained low-level transport fixtures keep their original source provenance.
