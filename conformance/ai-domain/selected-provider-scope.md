# Historical selected-provider inventory

[selected-provider-scope.json](selected-provider-scope.json) records the earlier
v0.84.4 source classification: six text providers and an OpenRouter image entry.
It is retained with the original export observations and does not define the
current supported providers or runtime features. The former generator that
validated this historical classification has been retired.

Current v1.4.0 scope is **OpenAI, Google Gemini, Anthropic and DeepSeek**, with
ChatGPT and Anthropic OAuth and no generated-image/classifier API. See the
[current scope](../../docs/scope.md#selected-provider-scope),
[current factory contract](../../docs/contracts/ai-integration/factories.md),
[upstream ownership map](../../upstream-map.toml) and
[active proof registry](../assembly.json).

Retained historical source identities and expectations are not relabelled as
v1.4.0 observations. The current protocol and OAuth comparisons execute the
[exact target release](../provider-traces/v1.4.0/README.md) independently.
