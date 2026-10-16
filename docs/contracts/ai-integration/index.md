# AI integration at Pi v1.4.0

The release authority is `v1.4.0` at
`4a080252f06caf63a525d7873c5d94d3c23887e0`. The supported builtin providers are
OpenAI, Anthropic, Google, and DeepSeek. Construction is passive and registration
is explicit. Default features are empty.

`pi-ai-core` owns bounded data, normalized transcripts, authentication/storage
contracts, registry authority, cancellation, stream settlement, and client
identity. `pi-ai-transport` owns explicitly injected network, callback listener,
and cryptographic host effects. `pi-ai-providers` owns four retained protocol
adapters, selected provider factories, catalog decoding, and the two OAuth flows.
`pi-ai` is the facade and explicit builtin composition entry point.
`pi-telemetry` supplies span parents, links, and opaque propagation carriers.

See [factories](factories.md) for feature selection,
[runtime facade](runtime-facade.md) for the callable boundary,
[transport adapters](transport-adapters.md) for request ownership,
[SDK evidence](sdk-evidence.md) for offline release validation, and
[export closure](export-closure.md) for the current exclusions.

Legacy aliases, Faux compatibility, old Codex/OpenRouter builtin factories,
image generation, classifiers, and dedicated excluded-provider wires are
removed. Image inputs remain available in user and tool-result messages.
Historical S03/S04/S05 inventories describe their original pinned source and
are not a declaration of current public feature availability.
