# Runtime facade and resource composition

Authority: pinned upstream `853a80d26c90a14c1886f0ebb8ffaae133ca2185` and the accepted architecture.

## Owned initialization and API registration

`CompatApiRegistry` owns ordered fresh `Arc<CompatApiBinding>` wrappers. Each
binding snapshots declared API ID and optional source ID; replacement preserves
position. Expose register_api_provider, get_api_provider, get_api_providers,
unregister_api_providers, register_builtin_api_providers and reset_api_providers.
Unregister matches the current source ID only; retained old bindings still reject
mismatched request APIs. Registration remembers the actual current wrapper,
including a preexisting override. Pointer identity is binding identity, not
underlying adapter identity. Replacement invalidates the remembered shortcut
until builtin registration repeats. Never invoke or drop host owners under locks.

`CompatFacade::initialize(registry: CompatApiRegistry,
modules: Arc<BuiltinProviderModules>, shared: CompatSharedApiModules,
options: ModelsRegistryOptions)` returns an owned boxed future yielding
`Result<CompatFacade, BuiltinCompositionError>`. Consume a prepopulated registry,
construct retained lazy modules, remember/register builtins and await accepted
builtin_models before publication. On failure no partially initialized facade is
returned. Retain options.auth_context before consuming options; it is the sole
legacy environment authority. Initialization performs no import-global mutation.

`CompatSharedApiModules` receives configured typed Arcs for Azure Responses,
Google Vertex, Mistral Conversations, Bedrock Converse Stream and Pi Messages.
`CompatApiModules` and `LegacyApiAliases` reuse these and the selected bundle's
Anthropic, Completions, Google, Responses and Codex Arcs. Builtin source order is
anthropic-messages, openai-completions, openai-responses,
openai-codex-responses, azure-openai-responses, google-generative-ai,
google-vertex, mistral-conversations, bedrock-converse-stream, pi-messages.
These are shared wire APIs, not added product factories/catalogs/auth discovery.

## Dispatch and legacy environment lookup

Facade stream and stream_simple return
`Result<AssistantStream, ModelsError>`. Resolve and capture API binding/dispatch
target and check API mismatch synchronously, before host calls. Shortcut requires
current binding identity equal remembered binding, selected provider exists and
any provider model has the requested API. Requested model ID need not be listed.
The shortcut dispatches directly through Provider.adapter, never ModelsRegistry's
auth-resolving stream. A pending environment lookup cannot retarget after later
registry replacement. Complete variants wrap launch and await the terminal result;
in-band assistant failures stay messages.

A nonblank explicit key bypasses environment lookup and preserves its original
bytes; trim is presence testing only. Missing/blank keys use accepted lazy_stream
and async get_env_api_key with captured AuthContext, env and cancellation. Inject
only a nonempty, non-marker result, preserving other auth/request fields. Source
legacy lookup is synchronous; Rust lookup and failures happen in the accepted
lazy setup phase, with inherited safe error/cancellation terminal behavior. This
is an explicit host timing/error normalization, not synchronous lookup parity.
Do not add factory credential-store resolution to these legacy direct paths.

Sixteen raw/simple aliases retain one lazy wrapper per configured module. Raw
calls preserve family typed options via the accepted stream_with_options path;
simple uses accepted ApiAdapter.stream_simple. bedrock_provider_module forwards
raw/simple to the same configured Bedrock Arc and preserves its accepted lazy
global-override/fallback behavior. Aliases add no environment or auth resolution.

Reuse accepted image registry/binding/generator/models types. Image builtin
registration always upserts openrouter-images using one retained CachedImagesApi
and configured loader for the same images Arc; factory image loading stays
PerCallImagesApi. Global image generation adds neither credentials nor environment.
No image unregister/reset/getAll surface is invented. Catalog aliases delegate
accepted getters and preserve dated image52 versus historical50 provenance.

## Actual facade export routes

Each retained source compat occurrence must resolve through an actual
pi_ai::compat route, even when its implementation is provider-owned. Reexport the
accepted image runtime ports/constructors, auth constants/context/env helper and
LazyOAuth, and the existing retry trait/function through compat; planned registry,
alias, image and catalog wrappers supply their corresponding callable routes.
Do not count backend-only probes as facade export proof.

At pi_ai root, images-runtime enables explicit reexports of
CreateImagesProviderOptions, ImagesModels, ImagesProvider, MutableImagesModels,
create_images_models and create_images_provider. compat enables explicit root
reexports of DefaultProviderAuthContext, env_api_key_auth and LazyOAuth. These nine
names have no current core-glob collision. Constructor/type projections replace
source defaultProviderAuthContext and lazyOAuth functions with their accepted
explicit-host Rust construction; no ambient singleton or new auth-runtime feature.
Root retry and OAuth type exports already exist. Source ./oauth type occurrences
may use documented existing compat/core routes; no empty pi_ai::oauth module.
Source ./api image and individual provider/catalog subpaths legitimately retain
their provider-owned Rust routes. Final typed occurrence catalog must prove these
exact routes and features individually, including documented many-to-one mappings.

## Codex close ownership and cleanup scopes

`CodexSessionResources` retains exactly modules.codex() through factory, aliases,
registry and resources. begin_close(session, RequestRuntime) calls accepted close
synchronously before returning its owned future: snapshot detachment and idle
cancellation happen immediately. Polling owns bounded physical close, including
loaned sockets. Retain the Arc through future completion/drop and map errors to
existing SessionResourceError without a new facade-to-transport dependency.
Dropping the future aborts its retired snapshot. A second empty close cannot join
an earlier pending close; new same-session entries belong to later snapshots.
Abort/stats/reset delegate unchanged, including sticky/stat independence.

`register_codex_cleanup_scope(&SessionResourceRegistry,
Arc<OpenAICodexResponsesApi>, RequestRuntime)` yields
`Result<CodexCleanupScope, SessionResourceRegistryError>`. Runtime.spawner is the
explicit task admission owner. The scope retains the core registration handle
and pending observation tickets. Each synchronous cleanup callback starts the
accepted close, publishes a distinct CodexCloseTicket, then admits its owned task.
Do not hold callback-state locks across close, spawn, waking or user-owner drops.
`take_close_tickets` drains handles for those exact attempts; each ticket awaits
`Result<(), Arc<SessionResourceError>>`. Do not overwrite a prior failed/pending
operation with later empty-close success.

Successful task admission means only synchronous cleanup acceptance, not physical
completion. Rejected admission returns a core cleanup error and settles its ticket
failed; dropped close work aborts. An admitted task retains Arc/completion when
scope or ticket observers drop; task-drop settles failure through a guard and
inherited retirement aborts. Direct close callers await their returned future;
scheduled cleanup callers await all recorded tickets when completion matters.
No second aggregation, timeout, retry or cache-retirement algorithm is introduced.

Accepted S03 cleanup removes successful registrations; source JS Set persists.
A retained handle cannot make the callback permanent. Each CodexCleanupScope owns
one registration/cleanup scope. S15 explicitly registers a new scope for subsequent
sessions/cleanup and awaits its tickets as needed. Failed active core registrations
remain retryable, with a separate ticket on each attempt. Core remains sole owner
of ordering, aggregation and panic normalization; its API/behavior is unchanged.

## Features and proof boundaries

The compat feature explicitly enumerates provider-anthropic, provider-deepseek,
provider-google, provider-openai, provider-openai-codex, provider-openrouter,
images-openrouter and five direct provider wire selectors for Azure Responses,
Google Vertex, Mistral Conversations, Bedrock Converse Stream and Pi Messages.
No all-* umbrella in this library feature, native/auth-native/OAuth, excluded
factory/catalog or new dependency/version. Add compat to the exact
ai-facade-provider-selection group together with real dependency activation.
The earlier empty type-only feature remains historical acceptance, not runtime
proof. Partial factory/catalog profiles with compat off remain unchanged.
