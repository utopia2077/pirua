# Selected factories and builtin composition

Authority: pinned upstream `853a80d26c90a14c1886f0ebb8ffaae133ca2185` and the accepted architecture.

## Configured modules and factories

Provider-owned `FactoryAuthServices` contains `spawner: Arc<dyn TaskSpawner>` and
`oauth: Option<OAuthServices>`. All construction and metadata lookup are passive.
Seven public factory names return `Result<Arc<dyn Provider>, ProviderFactoryError>`
(or `Arc<dyn ImagesProvider>` for images):

| Function | Configured typed adapter argument | Additional argument |
|---|---|---|
| anthropic_provider | Arc<AnthropicMessagesApi> | &FactoryAuthServices |
| deepseek_provider | Arc<OpenAICompletionsApi> | none |
| google_provider | Arc<GoogleGenAiApi> | none |
| openai_provider | Arc<OpenAIResponsesApi> | none |
| openai_codex_provider | Arc<OpenAICodexResponsesApi> | &FactoryAuthServices |
| openrouter_provider | Arc<OpenAICompletionsApi> | &FactoryAuthServices |
| openrouter_images_provider | Arc<OpenRouterImagesApi> | &FactoryAuthServices |

Factories reuse accepted core/image composition, key helpers, catalog getters and
lazy wrappers. A private configured text loader retains the supplied adapter Arc.
All selected text lazy capabilities remain false for deferred fetch/cancel; static
refresh/filter capabilities remain absent. No core Provider metadata field is added:
accepted model/request/auth endpoints carry request behavior, while source factory
baseUrl metadata is not an additional dispatch override in these source bodies.

Provider-owned `BuiltinProviderServices` has conditional public fields named
`anthropic`, `google`, `openai` (AdapterServices), `completions` (one AdapterServices
when DeepSeek or OpenRouter text is selected), `codex` (CodexServices), `images`
(ImageAdapterServices), and `auth` (FactoryAuthServices when an OAuth-bearing
factory is selected). `BuiltinProviderModules::new(services)` constructs each
configured concrete adapter once and returns a fallible module bundle. It retains
typed Arcs and exposes corresponding clone-returning accessors, including `codex()`,
plus a borrowed `auth()` accessor where applicable. The shared Completions module
serves both text factories; standalone functions permit distinct configured Arcs.
No Codex limits, wire defaults, or per-request module reconstruction are introduced.

Source OAuth names/subscription/loginLabel remain exact even when the corresponding
flow feature or supplied OAuthServices is absent. In that case a real unavailable
loader fails only on invocation, with a typed redacted error. When both are present,
private loaders construct the accepted actual flow only when loaded. LazyOAuth owns
its existing shared success/failure cache; module/resource lifetime is independent.
API-key/catalog operations never require an unused OAuth host to perform effects.

## Facade composition and catalog reads

`pi-ai::builtins` owns these entrypoints, with source-selected ordering:

- `builtin_providers(&BuiltinProviderModules) -> Result<Vec<Arc<dyn Provider>>, ProviderFactoryError>`.
- `builtin_images_providers(&BuiltinProviderModules) -> Result<Vec<Arc<dyn ImagesProvider>>, ProviderFactoryError>`.
- `async builtin_models(&BuiltinProviderModules, ModelsRegistryOptions) -> Result<ModelsRegistry, BuiltinCompositionError>`.
- `builtin_images_models(&BuiltinProviderModules, ImagesModelsOptions) -> Result<MutableImagesModels, ProviderFactoryError>`.

Construct all provider wrappers before registration. Text uses the accepted
`new_with_child_cancellation` constructor and awaits every set_provider; it cannot
return a registry while registration is still pending. Images use their accepted
synchronous registration. Source order is anthropic, deepseek, google, openai,
openai-codex, openrouter; image namespace contains only openrouter. Partial feature
profiles expose their enabled subset, and all-provider profiles expose exactly six.

Facade catalog getters expose enabled catalog IDs in source order, ordered model
vectors, unknown provider as an empty vector and unknown model as None. Generic
ProviderId/ImagesProviderId remain open; BuiltinProvider is only the selected catalog
identity projection. Preserve the captured model values, negative routing costs,
image52 versus historical50, and no OpenRouter vendor filter. A narrow pure metadata
helper projects captured generated-at to Option<SafeUnsigned>, bound to existing
provenance and without a new date-library or historical-value claim. A new generic
flattenModelCatalog implementation is not required: it is not a retained export
obligation and the accepted complete generator already owns that transformation.

The module bundle and providers retain the same Codex Arc; expose it for later
facade resource registration and owned async close. This unit does not implement
synchronous cleanup callbacks or claim they await physical graceful close.

## Exact feature ownership

Provider factory selectors are exactly:

| Feature | Activations |
|---|---|
| provider-anthropic | wire-anthropic-messages, catalog-anthropic |
| provider-deepseek | wire-openai-completions, catalog-deepseek |
| provider-google | wire-google-genai, catalog-google |
| provider-openai | wire-openai-responses, catalog-openai |
| provider-openai-codex | wire-openai-codex-responses, catalog-openai-codex |
| provider-openrouter | wire-openai-completions, catalog-openrouter |
| images-openrouter | wire-openrouter-images, catalog-openrouter-images |
| all-providers | the six local provider selectors |
| all-images | images-openrouter |
| all-oauth | oauth-anthropic, oauth-openai-codex, oauth-openrouter |

Existing default/catalog/wire/native/OAuth selector activations remain unchanged.
Neither provider nor image selectors activate OAuth or native. all-oauth does not
activate native. Umbrellas are explicit binary/test conveniences, never defaults.

Facade forwarding is exact:

- Each of seven catalog selectors: dep:pi-ai-providers plus its downstream catalog selector.
- Each provider-X: local catalog-X plus pi-ai-providers/provider-X.
- images-runtime: dep:pi-ai-providers; exposes generic image runtime without a wire.
- images-openrouter: images-runtime, local catalog-openrouter-images, and pi-ai-providers/images-openrouter.
- Each selected oauth-X: dep:pi-ai-providers plus pi-ai-providers/oauth-X.
- all-providers/all-images/all-oauth enumerate corresponding local selectors.
- native: dep:pi-ai-providers and pi-ai-providers/native.
- auth-native: native and pi-ai-providers/auth-native.

The exact `ai-facade-provider-selection` machine group controls the optional
pi-ai-to-providers edge. It includes the listed selectors and runtime `compat`,
plus reserved absent catalog-gen/dev-oauth-cli names. There are no wildcards.
Every implemented member activates the matching dependency edge. The
pi-coding-runtime selector group keeps its own exact boundary; dev-catalog-tools
stays outside. Reserved names do not claim implemented developer functionality.
