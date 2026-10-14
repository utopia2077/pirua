# AI Subsystem

## Target layering

```text
pi-ai-core
  domain DTOs → stream semantics → auth/models runtime
                                  │
                                  ▼
pi-ai-transport              pi-ai-providers
 HTTP/SSE/WS/retry     API adapters → provider factories
                                  │
                                  ▼
                                pi-ai
                           public façade/compat
```

This preserves the upstream distinction between API wire protocols and provider
composition. A provider is identity, catalog, auth, and adapter selection; an
API adapter owns request/response conversion and stream parsing.

## Core data model

Open identities use newtypes over strings:

```rust
pub struct ApiId(pub String);
pub struct ProviderId(pub String);
pub struct ModelId(pub String);
```

Known ids have constants, but parsing MUST accept unknown non-empty ids.

Messages use a closed built-in set plus registered custom roles:

```rust
pub enum Message {
    User(UserMessage),
    Assistant(AssistantMessage),
    ToolResult(ToolResultMessage),
    Custom { role: String, value: JsonValue },
}
```

Custom roles require a runtime schema before they can enter durable session
storage.

## Stream contract

Provider invocation returns synchronously as an `AssistantStream`. Setup,
authentication, lazy adapter construction, and request failures terminate the
stream with an error assistant message; they do not leak as an unrelated task
failure.

```rust
pub struct EventStream<E, R = E> { /* private consumer state */ }
pub type AssistantStream = EventStream<AssistantMessageEvent, AssistantMessage>;
pub type AssistantMessageEventStream = AssistantStream;
```

`AssistantStreamState` is the single settlement owner. Its private transition
is `Open -> Settled`; accepting a validated terminal event atomically enqueues
that event and fixes the independently awaitable result. There is no generic
close/end operation. A producer drop while still open settles one sanitized
setup error so `result()` cannot remain pending forever. Duplicate terminals
and progress after settlement return a typed protocol error while leaving the
first terminal authoritative. Request cancellation is a separate contender on
the same atomic transition, so a non-cooperative producer cannot keep the
result pending. State updates detach registered wakers under lock and wake only
after unlock; dropped wait futures unregister themselves.

The generic public type is consumer-only: `next(&mut self)` and `result(&self)`.
The canonical `create_assistant_message_event_stream` projection returns it
with a cloneable validated `AssistantStreamProducer`. No public generic push/end
or constructor can skip assistant terminal validation. `AssistantStream` is
an extra Rust convenience alias, not a claimed upstream symbol.

Normative requirements:

- `done` and `error` are the only terminal events.
- A terminal event yields exactly one final `AssistantMessage`.
- Its terminal reason equals the nested message `stopReason`; a terminal message
  is never `pending`, and `error`/`aborted` carries a nonempty `errorMessage`.
- Content updates are correlated by `contentIndex`; adjacency is not assumed.
- Cancellation terminates as `aborted`, not a generic transport error.
- Stream setup failures produce zero usage unless valid usage is independently
  available.
- Consumers can await `result()` without consuming all progress events.

## Provider and Models interfaces

```rust
pub trait ApiAdapter: Send + Sync {
    fn api_id(&self) -> &ApiId;
    fn stream(&self, request: TextStreamRequest) -> AssistantStream;
    fn stream_simple(&self, request: SimpleStreamRequest) -> AssistantStream;
    fn fetch_deferred(&self, request: DeferredFetchRequest)
        -> Result<AssistantStream, CapabilityError>;
    fn cancel_deferred<'a>(&'a self, request: DeferredCancelRequest)
        -> BoxFuture<'a, Result<(), ModelsError>>;
}

pub struct ResolvedRequest<M> {
    pub model: M,
    pub endpoint: RequestEndpoint,
}

// Non-Serde; Debug is fully redacted.
pub struct RequestEndpoint(String);

impl RequestEndpoint {
    // Explicit transient boundary used only by transport/provider adapters.
    pub fn expose_for_transport(&self) -> &str;
}

pub trait ImagesApiAdapter: Send + Sync {
    fn generate<'a>(&'a self, request: ImageGenerationRequest)
        -> BoxFuture<'a, AssistantImages>;
}

pub trait ProviderModelsRefresher: Send + Sync {
    fn refresh_models<'a>(&'a self, context: RefreshModelsContext<'a>)
        -> BoxFuture<'a, Result<(), ModelsError>>;
}

pub trait Provider: Send + Sync {
    fn id(&self) -> &ProviderId;
    fn name(&self) -> &str;
    fn auth(&self) -> &ProviderAuth;
    fn models(&self) -> Result<Arc<[Model]>, ModelsError>;
    fn models_refresher(&self) -> Option<&(dyn ProviderModelsRefresher + '_)>;
    fn filter_models(&self, models: &[Model], credential: Option<&Credential>)
        -> Vec<Model>;
    fn adapter(&self, api: &ApiId) -> Option<Arc<dyn ApiAdapter>>;
}

pub struct ModelsRegistry { /* private registry-owned state */ }
```

`ModelsRegistry` is concrete. Dynamic provider and adapter ports are object-safe:
every async method returns an explicit `BoxFuture<'a, T>` whose lifetime is
shared by all borrowed inputs. No `async-trait` transformation is part of the
ABI.

The separate optional refresher is observed before generation allocation or
store/auth access, so static providers are actually skipped rather than called
through a ready-success placeholder.

`RefreshModelsContext<'a>` contains a borrowed, registry-created
`ModelsPublisher`. The publisher is concrete, non-Clone, non-Serde, and redacted.
Its `publish()` method can be awaited repeatedly and returns
`PublicationOutcome::Published | Superseded` for each publication. A
`ModelsPublication` has independent memory `Keep | Set` and persistence
`Keep | Set | Delete` intents; it contains no update closure. The borrow ends
with the provider refresh, preventing a provider from retaining a live
publication authority.

```rust
pub struct RefreshModelsContext<'a> {
    pub publisher: &'a mut ModelsPublisher,
    // credential, stored snapshot, network/force, cancellation
}

impl ModelsPublisher {
    pub fn publish<'a>(
        &'a mut self,
        publication: ModelsPublication,
    ) -> BoxFuture<'a, Result<PublicationOutcome, ModelsError>>;
}
```

The mutable borrow statically permits only one live publication future. Calling
`publish` reserves its monotonic ticket synchronously. Dropping before first
poll cancels that no-effect ticket; after admission, the registry shields and
finishes the ticket before processing the next. Thus invocation order, owner
queue order, persistence order, memory order, and acknowledgement order agree.

## Models concurrency

The runtime preserves these upstream semantics:

- Replacing/removing a provider supersedes its active refresh generation.
- Replacing/removing a provider and refresh publication are serialized by one
  per-provider registry owner.
- Membership mutation order is allocated after successful closed-gate spawn;
  a spawn call still in progress holds no ordering reservation that could block
  synchronous reentry.
- Each admitted provider value has a checked membership epoch. Refresh may
  allocate generation authority only while committed and projected membership
  both retain that same Present epoch; a displaced but still-visible provider
  cannot revive after replacement admission.
- A newly allocated refresh generation closes its predecessor and waits the
  snapshotted tail of older admitted owner work before its first cache read.
- Publication checks provider epoch and generation before and after persistence.
- Cached provider state is restored before network refresh.
- A provider that throws while listing models contributes no models but does not
  invalidate other providers.
- Auth resolution is performed per request, so expiring credentials can refresh
  after long tool phases.
- Only caller cancellation sets `ModelsRefreshResult::aborted`; generation-only
  supersession is reported as neither abortion nor provider failure.
- Provider code computes publication candidates but cannot write registry
  stores. Persistence participates in the provider owner's publication
  sequence, preventing a blocked stale write from overwriting a newer
  generation between two checks.
- One model-store instance is exclusively owned by one live registry. S03 does
  not claim cross-registry or cross-process model-cache fencing; a future shared
  store requires store-level fencing tokens. The credential store retains its
  separate cross-process exclusion requirement.

## Authentication

The credential store owns serialized per-provider mutation. Plain revisioned
CAS is insufficient for rotating OAuth tokens because concurrent callers could
both perform the external refresh before either CAS commits. Rust therefore uses
an exclusive, cancellable mutation lease rather than a callback-bearing
`modify()` interface:

```rust
pub trait CredentialStore: Send + Sync {
    fn read<'a>(&'a self, provider: &'a ProviderId, cancellation: &'a CancellationToken)
        -> BoxFuture<'a, Result<Option<VersionedCredential>, AuthError>>;
    fn list<'a>(&'a self, cancellation: &'a CancellationToken)
        -> BoxFuture<'a, Result<Vec<CredentialInfo>, AuthError>>;
    fn begin_mutation<'a>(
        &'a self,
        provider: &'a ProviderId,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Box<dyn CredentialMutation>, AuthError>>;
    fn reconcile<'a>(
        &'a self,
        provider: &'a ProviderId,
        resolution: CredentialReconciliation,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<VersionedCredential>, AuthError>>;
}

pub trait CredentialMutation: Send {
    fn current(&self) -> Option<&VersionedCredential>;
    fn commit(self: Box<Self>, next: CredentialMutationValue)
        -> BoxFuture<'static, Result<Option<VersionedCredential>, AuthError>>;
}
```

OAuth login, refresh, and `to_auth` remain distinct. Refresh happens while the
exclusive provider lease is held and rechecks its detached current state to
avoid double-refresh. Dropping a lease performs no write. Persistent stores must
provide an equivalent cross-process lease before claiming conformance.

Cancellation is checked before commit linearization. Once commit linearization
starts, the caller shields and drives it to a definitive outcome; cancellation
cannot drop a partially effective future. A normal error proves the old value
remains authoritative. A backend unable to prove post-state returns a distinct
`OutcomeUnknown` category, which forbids ambient fallback or automatic retry.
Credential auth implementations and model stores are non-reentrant leaf ports:
they receive no registry/store capability and must not call back into the same
provider operation through a captured global handle.

Persistent stores durably write an uncertainty intent before a commit that may
lose its acknowledgement. If post-state cannot be proven, that provider remains
quarantined across later reads, mutations, and process restart. Only explicit
host/user reconciliation may clear it: `Replace` with a newly authenticated
credential or `Delete` for logout. Ordinary auth resolution and refresh cannot.
`read` and `begin_mutation` keep returning `OutcomeUnknown` until reconciliation
atomically establishes and records a known state.

## Request options split

Serializable options and process-local runtime services MUST be separate:

```rust
pub struct StreamOptions {
    pub wire: StreamOptionsWire,
    pub runtime: RequestRuntime,
}

pub struct RequestRuntime {
    pub cancellation: CancellationToken,
    pub telemetry: Arc<dyn TelemetryContext>,
    pub clock: Arc<dyn Clock>,
    pub spawner: Arc<dyn TaskSpawner>,
}

pub struct ModelsRequestOverrides {
    pub auth: AuthResolutionOverrides,
    pub headers: ProviderHeaders,
    // Already resolved by provider composition; consumed only by the Azure
    // RuntimeResolved sentinel path.
    pub runtime_endpoint: Option<RequestEndpoint>,
}
```

HTTP transport and middleware are deliberately absent: dependency direction is
`pi-ai-transport -> pi-ai-core`. S04 adapters capture transport and middleware
when constructed. The separately reviewed
[S09 request-lifecycle bridge](../contracts/request-lifecycle.md)
adds an opt-in borrowed adapter capability and observed registry entry points.
A request-local service binding carries payload interception and passive response
observation; RequestRuntime retains its existing shape. Captured middleware keeps
its old policy, and observation-only header projection cannot turn success into
failure. Settled assistant transformation remains a Harness consumer responsibility.
Per-request JavaScript `fetch` and header-transform callbacks
have no portable-core analogue.

Only the corresponding serializable `*OptionsWire` DTO may enter extension RPC
or durable Harness state; no runtime wrapper may cross that boundary.
`ModelsRequestOverrides` is likewise process-local and non-Serde. S03 never
receives Azure resource/deployment configuration or a resolver callback.

Raw `ProviderHeaders = Record<string, string | null>` remain an exact transient
AI DTO because values may be credentials and `null` suppresses defaults. The
separate pinned Harness `AgentHarnessStreamOptions.headers` shape is
`Record<string, string>` and rejects explicit null. Durable Harness construction
accepts only a `PublicProviderHeaders` transparent wrapper produced by its
declarative policy; the default policy accepts none. The seven model-header
hard-deny names and any host-classified secret name can never be marked public.
Session decode validates the captured map before producing typed durable state.
This preserves each wire shape while refusing the unsafe interpretation that
every caller-supplied header is durable.

Pinned upstream also merges `Model.headers` into resolved authentication, so a
plain model-header map is credential-capable. Rust uses transient
`RawModelHeaders`, a pure declarative `ModelHeaderPolicy` allowlist, and a
separate immutable `PublicModelHeaders` wrapper. Raw intake rejects exact and
ASCII-case-insensitive duplicate names. Consuming raw headers with the policy is
the only way to produce the public wrapper; there is no unchecked map
constructor or ordinary `Deserialize`. Core hard-denies, case-insensitively, `authorization`,
`proxy-authorization`, `cookie`, `set-cookie`, `api-key`, `x-api-key`, and
`x-goog-api-key` even when a policy allowlists them. A sentinel-secret test
places its marker under a hard-denied or non-allowlisted name; core does not
guess from value substrings. Ordinary `Model`, `ImagesModel`, and
`ModelsStoreEntry` deserialization uses the default-empty policy and rejects
every nonempty map. Policy-aware `RawModel`, `RawImagesModel`, and
`RawModelsStoreEntry` conversions own external intake and every headerful store
restore while retaining the upstream JSON map shape. Policy changes that reject
a cached header fail closed before publication.
Consequently `Model`, `ImagesModel`, and `ModelsStoreEntry` can serialize only
already-classified public headers; raw credential-bearing headers remain in
`ProviderHeaders`/`ModelAuth` and never enter the model store.

The same durable boundary treats model `baseUrl` as public endpoint metadata,
with one pinned closed sentinel. Raw text-model intake maps the exact empty
string to `ModelBaseUrl::RuntimeResolved` only for
`azure-openai-responses`; the variant serializes back as empty. S05 Azure
composition resolves the endpoint from provider-specific inputs and passes an
already-resolved `RequestEndpoint` into S03 registry assembly before dispatch.
Every other text API and every image API requires `PublicModelBaseUrl`.

Public validation is intentionally structural, not a URL parser: reject literal
`?` or `#`; if the first `//` exists, treat bytes after it through the next `/`,
`?`, or `#` as authority and reject `@` only there. Do not normalize or restrict
scheme, authority, host, port, path, templates, localhost, relative endpoints,
or nonstandard schemes. Accepted bytes serialize exactly. Credential-specific
endpoint selection remains transient `ModelAuth.baseUrl`. Before adapter
dispatch, S03 creates non-Serde, fully redacted `RequestEndpoint` and generic
`ResolvedRequest<M>`. Distinct resolved request roots for text stream, simple
stream, deferred fetch, deferred cancel, and image generation make a raw-model
skip impossible at the signature level. The effective URL is never cloned
back into `Model`, `ImagesModel`, or `ModelsStoreEntry`. Raw/validated model/store,
standalone public-URL, `ModelAuth`, and containing `AuthResult` Debug redact the
complete URL, and validation errors expose category only.

Closed security-sensitive and durable DTOs reject unknown fields. Open identity
strings and explicit flattened extension maps (`OAuthCredentials` extras,
custom JSON data) remain open by contract. Count/time/index/token values use a
finite non-negative JavaScript-safe integer newtype; signed deltas use the signed
equivalent; cost/sampling values use a finite-f64 newtype. Plain `usize` and
NaN/infinity never cross JSON boundaries.

Optional non-null fields reject explicit null; `Presence` is used only where
null and omission are distinct valid states. Open object and header maps reject
duplicate keys. Retry policy additionally caps `maxRetries` one below the safe
integer maximum because normalization adds one attempt.

## Adapter families

`pi-ai-providers` keeps internal modules corresponding to upstream shared wire
families:

- Anthropic Messages;
- OpenAI Completions;
- OpenAI Responses shared core;
- OpenAI Responses, Codex, and Azure variants;
- Google shared conversion, Generative AI, and Vertex;
- Bedrock Converse Stream;
- Mistral Conversations;
- pi-messages;
- image generation.

Provider factories follow the
[owner-selected entries](../scope.md#selected-provider-scope), with one
module per selected source entry. Text and image OpenRouter preserve their
separate source modules and shared ID in separate namespaces. Small factories
are not data-folded away because auth and adapter mapping changes are frequent
and easiest to review under the upstream name. Existing shared wire families
do not imply product registration for excluded factories.

Provider/OAuth/image breadth is feature-isolated inside the consolidated crate:

- no built-in provider, OAuth, or image family is enabled by default;
- `wire-*` selects a shared adapter family;
- `provider-<id>` selects its wire family and catalog shard but not OAuth;
- `oauth-<flow>` and `images-<id>` are independent and default-off;
- provider/OAuth/image `all-*` umbrellas cover only the selected product entries;
  shared-wire test features retain accepted adapter regression coverage;
- `all-*` umbrella features remain permitted only for explicit binaries and tests;
- importing `pi-ai` or `pi-ai-providers` performs no global registration or
  platform action.

Retry authority is non-overlapping: `pi-ai-core` owns pure classification and
policy values, `pi-ai-transport` owns retry within one HTTP transport request,
and `pi-harness-core` owns durable model-attempt/backoff/recovery state.

## Generated model data

The TypeScript generator rules are upstream semantic input; generated TS shards
are projections. The Rust catalog path is:

```text
upstream generator JSON output
  → schema/hash validation
  → Rust catalog representation
  → provider factories
```

The Rust generator MAY later independently implement the normalization rules,
but differential comparison with upstream JSON remains the authority check.

Factory/catalog output follows the selected scope. Preserve the complete source
inventory and record excluded provider projections separately; a smaller product
catalog does not justify losing upstream identity or reporting exclusions as
implemented. OpenRouter routing models are not additionally filtered by vendor.

The pinned upstream commit does not contain the mutable external input snapshot
used to generate all catalog values. The [dated catalog capture](../../conformance/catalog/README.md)
supports reproducible captured-value parity with its source identifiers and hashes.
Generated TypeScript alone remains projection evidence; the absent historical
text inputs are not reconstructed by the new capture.

## Compatibility

`pi-ai` exposes an optional `compat` feature for legacy global registries and
aliases. Core and Harness crates MUST NOT depend on it. Upstream deletion of the
compat entrypoint triggers deprecation or removal review in Rust rather than
permanent adoption.

## Development and build targets remain inside the façade crate

`pi-ai` owns the following non-default targets and modules for the selected
product scope:

```text
bin::catalog_gen
bin::image_catalog_gen
bin::pi_ai_cli
compat
```

The public upstream Faux provider compatibility surface is outside the selected
scope. Harness and AI conformance use internal scripted providers/transports for
text/thinking/tool deltas, abort, usage, deferred operations and invocation-state
observations as required by their tests. These doubles are test infrastructure,
not additional built-in product providers; paid provider APIs are not required.

Catalog generator binaries are build tools and are not linked into the normal
runtime. Their output remains differentially checked against the upstream
generator. The CLI is a façade/composition target and does not introduce state
or provider registrations into `pi-ai-core` imports.
