# Selected factories and features

Authority: Pi `v1.4.0`, commit `4a080252f06caf63a525d7873c5d94d3c23887e0`.

| Provider feature | Factory | Protocol | API-key environment | Optional OAuth feature |
|---|---|---|---|---|
| `provider-openai` | `openai_provider` | `OpenAIResponsesApi` | `OPENAI_API_KEY` | `oauth-openai-chatgpt` |
| `provider-anthropic` | `anthropic_provider` | `AnthropicMessagesApi` | `ANTHROPIC_API_KEY` and explicit Anthropic auth context | `oauth-anthropic` |
| `provider-google` | `google_provider` | `GoogleGenAiApi` | `GEMINI_API_KEY` | — |
| `provider-deepseek` | `deepseek_provider` | `OpenAICompletionsApi` | `DEEPSEEK_API_KEY` | — |

Every provider feature selects its corresponding catalog and wire. Catalog-only
features are `catalog-openai`, `catalog-anthropic`, `catalog-google`, and
`catalog-deepseek`. Wire-only features are `wire-openai-responses`,
`wire-anthropic-messages`, `wire-google-genai`, and `wire-openai-completions`.
`all-providers` selects the four provider features; `all-oauth` selects the two
OAuth flows. OAuth remains independently selectable. `native` enables the
native transport, and `auth-native` also enables native callback/crypto hosts.
There are no default features.

`BuiltinProviderServices` captures one configured `AdapterServices` for each
selected wire, using `anthropic`, `google`, `openai`, and `completions` fields.
OpenAI and Anthropic additionally use `FactoryAuthServices { spawner, oauth }`.
`BuiltinProviderModules::new` captures those services and retains configured
adapter instances. `builtin_providers` returns the selected provider objects;
`builtin_models` explicitly registers them in a new registry. No construction
path reads configuration, discovers providers, opens a listener, or starts a
network request.

OpenAI offers API key and ChatGPT subscription auth under the single `openai`
provider. ChatGPT uses dynamic public-client registration, an installation UUID,
and direct Responses tokens. Anthropic supports browser callback login and
headless copy-code login. OAuth flows return credentials to the existing store
and registry; they do not create a second credential authority. `OAuthServices`
requires explicit transport, clock, spawner, crypto, callbacks, callback limits,
and response bounds; `device_id` and `agent_name` configure ChatGPT login.

The four catalogs contain 85 chat models. Release identity, data integrity, and
source association are documented in [the catalog corpus](../../../conformance/catalog/README.md).
