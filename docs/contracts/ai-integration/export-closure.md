# Current AI export boundary

The selected release is Pi `v1.4.0` at
`4a080252f06caf63a525d7873c5d94d3c23887e0`. Builtin IDs and provider features
are limited to `openai`, `anthropic`, `google`, and `deepseek`; implemented wire
IDs are `openai-responses`, `anthropic-messages`, `google-generative-ai`, and
`openai-completions`. Open custom IDs and shared protocol configuration remain
available for callers using these retained wires.

The facade has no `compat` module, legacy wire aliases, image-generation model,
request, options or adapter interface, classifier, old Codex factory/flow,
OpenRouter factory/flow, enterprise workload federation, or dedicated Bedrock,
Vertex, Azure, Mistral, Pi Messages, and Cloudflare wires. There are no migration
aliases. Vision input content and model image input limits remain part of chat.

Historical inventories and fixture directories retain provenance for their
original source revision. They do not activate removed feature profiles. Current
feature manifests, selected-factory source guards, release catalog checks, and
four-wire/OAuth tests define the executable selected boundary.
