# Models registry, publication and dispatch

Authority: pinned upstream `853a80d26c90a14c1886f0ebb8ffaae133ca2185` and the accepted architecture.

## Unit boundary

This unit implements the provider-neutral `Provider` and optional catalog-
refresher ports, concrete `ModelsRegistry`, provider membership, dynamic model
publication, refresh phases, public login/logout/status operations, request
auth/header/endpoint assembly, and text/deferred adapter dispatch.

It does not implement HTTP or wire adapters (S04), concrete providers/OAuth/
Azure resolution/image registry or image dispatch (S05), the generic
`createProvider` compatibility builder, or pure cost/thinking/equality helpers
(the final S03 unit). Existing `ImageGenerationRequest` custody remains S03;
S05 constructs and dispatches it after provider-specific image auth.

## Upstream symbol disposition

| Pinned `models.ts` surface | Rust disposition |
|---|---|
| `Models`, `MutableModels` | concrete `ModelsRegistry` |
| `CreateModelsOptions`, `createModels` | `ModelsRegistryOptions`, `ModelsRegistry::new` |
| `Provider`, `RefreshModelsContext` | this unit's object-safe provider/refresher ports |
| `ModelsPublication`, refresh options/result | this unit's value-only runtime types |
| Models request option aliases | `ModelsRequestOverrides` plus existing typed runtime options |
| `ModelsRequestTransforms` | S04 construction-time middleware divergence |
| `CreateProviderOptions`, `createProvider` | final S03 provider-neutral helper unit |
| `hasApi`, cost, thinking, equality helpers | final S03 pure/helper unit |

Root façade exposure needs no source edit because `pi-ai` already reexports
`pi-ai-core`.

## Publication values

All types in this section are non-Serde. Model Debug behavior remains redacted.

```rust
#[derive(Clone, Debug, Default, PartialEq)]
pub enum ModelsMemoryIntent {
    #[default]
    Keep,
    Set(Arc<[Model]>),
}

#[derive(Clone, Debug, Default, PartialEq)]
pub enum ModelsPersistenceIntent {
    #[default]
    Keep,
    Set(ModelsStoreEntry),
    Delete,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ModelsPublication {
    pub memory: ModelsMemoryIntent,
    pub persistence: ModelsPersistenceIntent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PublicationOutcome {
    Published,
    Superseded,
}
```

Memory `Set` is the registry-owned dynamic overlay. An empty slice clears it.
Provider baseline models remain provider-owned. Listing starts with the
baseline snapshot; each dynamic model replaces the first equal `ModelId` in
place or appends. Persistence and memory intents are independent. When both
change, persistence reaches a definitive result before memory is considered.

```rust
pub struct ModelsPublisher { /* private */ }

impl ModelsPublisher {
    pub fn publish<'a>(
        &'a mut self,
        publication: ModelsPublication,
    ) -> BoxFuture<'a, Result<PublicationOutcome, ModelsError>>;
}
```

`ModelsPublisher` is `Send`, non-Clone, non-Serde, and Debug renders exactly
`ModelsPublisher([PROCESS-LOCAL])`. Whether it is `Sync` is not a public
promise. The mutable borrow permits only one live publication future.

## Provider ports

A separate optional capability identifies a static provider before generation,
model-store, credential, or auth work. The accessor is side-effect-free and
stable for one provider instance.

```rust
pub trait ProviderModelsRefresher: Send + Sync {
    fn refresh_models<'a>(
        &'a self,
        context: RefreshModelsContext<'a>,
    ) -> BoxFuture<'a, Result<(), ModelsError>>;
}

pub trait Provider: Send + Sync {
    fn id(&self) -> &ProviderId;
    fn name(&self) -> &str;
    fn auth(&self) -> &ProviderAuth;
    fn models(&self) -> Result<Arc<[Model]>, ModelsError>;

    fn models_refresher(&self) -> Option<&(dyn ProviderModelsRefresher + '_)> {
        None
    }

    fn filter_models(
        &self,
        models: &[Model],
        credential: Option<&Credential>,
    ) -> Vec<Model> {
        models.to_vec()
    }

    fn adapter(&self, api: &ApiId) -> Option<Arc<dyn ApiAdapter>>;
}
```

`models()` returns a detached baseline snapshot and is called with no registry
lock held. Listing catches its `ModelsError` and omits only that provider.
`get_available` instead propagates a selected/checked provider's model error as
`AuthResolutionError::Models`, matching the pinned rejection path.

```rust
pub struct RefreshModelsContext<'a> {
    pub publisher: &'a mut ModelsPublisher,
    pub credential: Option<&'a Credential>,
    pub stored: Option<&'a ModelsStoreEntry>,
    pub allow_network: bool,
    pub force: Option<bool>,
    pub cancellation: &'a CancellationToken,
}
```

All references share one explicit lifetime. The context is non-Serde and Debug
reports only credential/stored presence, booleans, and cancellation state.

## Refresh and request inputs

```rust
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ModelsRefreshOptions {
    pub allow_network: bool,
    pub providers: Option<Vec<ProviderId>>,
    pub force: Option<bool>,
}

impl Default for ModelsRefreshOptions {
    // allow_network=true, providers=None, force=None
}

#[derive(Debug)]
pub struct ModelsRefreshResult {
    pub aborted: bool,
    pub errors: BTreeMap<ProviderId, AuthResolutionError>,
}
```

These types are non-Serde. Selected ids retain input-set semantics; duplicate
ids do not duplicate work. Error keys are deterministic provider ids. Only
caller-token cancellation sets `aborted=true`; generation-only supersession
produces no provider error and leaves `aborted=false` while the caller remains
active.

```rust
#[derive(Default)]
pub struct ModelsRequestOverrides {
    pub auth: AuthResolutionOverrides,
    pub headers: ProviderHeaders,
    pub runtime_endpoint: Option<RequestEndpoint>,
}
```

The request override is non-Clone/non-Serde. Debug exposes only explicit-key,
environment/header-key, minimum-validity, and runtime-endpoint presence. The
runtime endpoint is already resolved by S05/provider composition; portable core
does not receive Azure resource/deployment options or implement the Azure URL
algorithm.

## Registry construction and public API

```rust
pub struct ModelsRegistryOptions {
    pub credentials: Arc<dyn CredentialStore>,
    pub models_store: Box<dyn ModelsStore>,
    pub auth_context: Arc<dyn AuthContext>,
    pub clock: Arc<dyn Clock>,
    pub spawner: Arc<dyn TaskSpawner>,
    pub model_header_policy: ModelHeaderPolicy,
}

pub struct ModelsRegistry { /* Arc<RegistryInner>, private */ }

impl ModelsRegistry {
    pub fn new(options: ModelsRegistryOptions) -> Self;

    pub fn set_provider<'a>(
        &'a self,
        provider: Arc<dyn Provider>,
    ) -> BoxFuture<'a, Result<(), ModelsError>>;
    pub fn delete_provider<'a>(
        &'a self,
        provider: &'a ProviderId,
    ) -> BoxFuture<'a, Result<(), ModelsError>>;
    pub fn clear_providers<'a>(&'a self) -> BoxFuture<'a, Result<(), ModelsError>>;

    pub fn get_providers(&self) -> Vec<Arc<dyn Provider>>;
    pub fn get_provider(&self, provider: &ProviderId) -> Option<Arc<dyn Provider>>;
    pub fn get_models(&self, provider: Option<&ProviderId>) -> Vec<Model>;
    pub fn get_model(&self, provider: &ProviderId, model: &ModelId) -> Option<Model>;

    pub fn refresh<'a>(
        &'a self,
        options: ModelsRefreshOptions,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, ModelsRefreshResult>;

    pub fn check_auth<'a>(
        &'a self,
        provider: &'a ProviderId,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<AuthCheck>, AuthResolutionError>>;
    pub fn get_available<'a>(
        &'a self,
        provider: Option<&'a ProviderId>,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Vec<Model>, AuthResolutionError>>;
    pub fn get_auth<'a>(
        &'a self,
        provider: &'a ProviderId,
        overrides: AuthResolutionOverrides,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<AuthResult>, AuthResolutionError>>;
    pub fn get_model_auth<'a>(
        &'a self,
        model: &'a Model,
        overrides: AuthResolutionOverrides,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<AuthResult>, AuthResolutionError>>;

    pub fn login<'a>(
        &'a self,
        provider: &'a ProviderId,
        auth_type: AuthType,
        interaction: &'a dyn AuthInteraction,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Credential, AuthResolutionError>>;
    pub fn logout<'a>(
        &'a self,
        provider: &'a ProviderId,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), AuthResolutionError>>;
}
```

`ModelsRegistryOptions` is non-Clone/non-Serde because it consumes the boxed
model store. `ModelsRegistry` is non-Clone, `Send + Sync`, non-Serde, and has
fixed Debug `ModelsRegistry([PROCESS-LOCAL])`. Applications share one instance
through `Arc<ModelsRegistry>`.

All membership mutation methods attempt admission on first poll. The caller
first passes a detached command blocked on a private start gate to `TaskSpawner`
with no registry/owner lock held and without reserving global order. A
synchronously polled command can observe only the closed gate. `SpawnError`
signals `Abort` through the gate and has zero generation, membership, or
backend effect.

After `spawn(…) -> Ok(())` returns, the caller enters the global sequencer and
allocates the next checked `u128` admission ticket. Ticket allocation, updating
the sequencer's projected membership state, closing the projected predecessor
generation, and enqueuing the mutation are one admission linearization point.
Only that point is admission. Successful post-spawn admission order, not call
or poll-start order, defines overlapping/reentrant mutation order; a spawn call
that has not returned never blocks a later reentrant admission. Counter
exhaustion signals `Abort` through the already spawned gate and has zero
generation, membership, or backend effect. The gate opens to `Run` only after
the admission transaction commits and affected cancellation handles are
detached; cancellation, wake, and displaced-value drop happen after unlock.

The projected state includes all earlier admitted set/delete/clear intents, so
two overlapping replacements close the correct logical predecessor even when
it is not yet publicly installed. Dropping an admitted mutation future removes
only its waiter; the command remains independently driven. All failures return
a fixed `ModelsErrorCode::ModelSource`; no backend source is copied into Display
or Debug.

## Text and deferred dispatch API

```rust
impl ModelsRegistry {
    pub fn stream(
        &self,
        model: Model,
        context: Context,
        overrides: ModelsRequestOverrides,
        options: StreamOptions,
    ) -> AssistantStream;
    pub fn complete(
        &self,
        model: Model,
        context: Context,
        overrides: ModelsRequestOverrides,
        options: StreamOptions,
    ) -> BoxFuture<'static, AssistantMessage>;
    pub fn stream_simple(
        &self,
        model: Model,
        context: Context,
        overrides: ModelsRequestOverrides,
        options: SimpleStreamOptions,
    ) -> AssistantStream;
    pub fn complete_simple(
        &self,
        model: Model,
        context: Context,
        overrides: ModelsRequestOverrides,
        options: SimpleStreamOptions,
    ) -> BoxFuture<'static, AssistantMessage>;
    pub fn fetch_deferred(
        &self,
        model: Model,
        handle: DeferredHandle,
        overrides: ModelsRequestOverrides,
        options: DeferredFetchOptions,
    ) -> BoxFuture<'static, AssistantMessage>;
    pub fn cancel_deferred(
        &self,
        model: Model,
        handle: DeferredHandle,
        overrides: ModelsRequestOverrides,
        options: DeferredCancelOptions,
    ) -> BoxFuture<'static, Result<(), AuthResolutionError>>;
}
```

No method accepts a raw endpoint string, raw auth result, or adapter callback.
Image registry/generation is S05 and consumes the already frozen
`ImageGenerationRequest`/`ImagesApiAdapter` boundary.

## Membership and owner state

The registry owns one global membership sequencer and one owner cell per
provider id. Each successfully admitted set derives a distinct provider epoch
from its membership admission ticket:

```text
membership table: insertion slots + checked u128 provider epoch
                  + Present|Closing|Tombstoned
provider owner: authority(provider epoch, checked u128 generation) + token
                checked u128 publication tickets/reservations/queue
                dynamic Arc<[Model]> snapshot
```

- Replacement retains the insertion slot and owner cell, closes the projected
  predecessor generation at mutation admission, waits admitted work, then
  swaps the provider to the successor epoch and clears dynamic memory at
  membership linearization.
- Delete closes the generation, waits admitted work, removes the live slot, and
  tombstones the owner. Reinsert creates a distinct owner incarnation and
  appends; an old publisher cannot target it by id.
- Clear admission is globally ordered with set/delete by the successful
  post-spawn ticket. Earlier admitted sets are cleared; later admitted sets
  wait and survive. It closes all projected affected generations, drains each
  without holding two owner locks, then atomically clears the membership table.
- Mutation readiness acknowledges both public membership change and drainage
  of all older admitted effects. Provider/state values and wakers are detached
  under locks and dropped/woken after unlock.

Membership/provider-epoch, generation, and publication counters are checked
`u128`. Exhaustion returns ModelSource before mutation. They never wrap or
reset while any owner, publisher, reservation, admitted command, or stale
capability can survive.

## Refresh phases

Refresh selection snapshots only a committed `Present` provider together with
its provider epoch. It calls `models_refresher()` on that detached provider
before allocating a generation or touching any port; `None` means static and is
ignored. Generation allocation then atomically requires both committed and
projected membership to remain `Present` at that same epoch. A `Closing`,
replaced, deleted, cleared, or merely projected-away epoch is generation-only
supersession: it performs no store/auth/provider work, records no error, and
does not set `aborted`.

For each selected dynamic provider, generation allocation and old-work fencing
are one membership/owner operation: validate the provider epoch, increment the
checked generation, close the old token, and snapshot the tail of every older
admitted owner command. Cancellation is delivered after unlock. The refresh
awaits that tail and rechecks caller cancellation plus both provider-epoch and
generation authority before its first store read. An older reserved-but-
unadmitted publication later observes stale authority and has zero effect.

The exact online sequence is:

```text
validate provider epoch; allocate/cancel generation; snapshot older admitted tail
await older admitted owner commands; recheck caller, provider epoch, and generation
ModelsStore::read
RawModelsStoreEntry::into_store(current policy)
cache provider phase: credential=None, allow_network=false, force=None
credential read and catalog-auth resolution
stop if unconfigured
ModelsStore::read again
RawModelsStoreEntry::into_store(current policy)
network provider phase: effective credential, allow_network=true, caller force
```

The cache phase can await any number of publications and each acknowledgement
precedes credential access. Offline refresh performs only the first read/
validation/cache phase and never touches credential/auth ports. Online
unconfigured refresh completes cache then auth and skips the second store read
and network phase. A second online store read preserves the pinned per-phase
snapshot behavior and observes cache-phase persistence changes.

Catalog auth is distinct from request auth:

- stored OAuth is refreshed under the accepted mutation lease only when
  `clock.now_unix_ms() >= expires`; the five-minute request window is not used;
- the refresh races caller/generation cancellation but has no separate 15-second
  request-auth deadline;
- stored/ambient API-key auth resolves and becomes a synthetic
  `ApiKeyCredential {key, env}` for the provider phase;
- stored type mismatch, quarantine, normal error, and caller cancellation keep
  their accepted channels and never silently fall back.

Different provider phase futures are polled concurrently without a global
publication lock. Caller cancellation has priority over provider readiness,
drops non-cooperative provider work, returns `aborted=true`, and records no
cancellation error. Generation-only cancellation drops provider work and
records neither an error nor `aborted=true`. Dropping the outer refresh future
cancels every generation it created through an RAII guard. Already admitted
publication/store work keeps its definitive ordering; later results are
discarded when stale.

## Publication state machine

Calling `publish()` synchronously reserves the next checked ticket before
returning its future. First-poll task transfer uses the same closed start-gate
rule as membership mutation: the spawner sees a command that cannot act;
successful spawn changes `Reserved` to `Admitted` and opens the gate, while
spawn failure removes the reservation before returning the fixed error.

```text
Reserved
  drop before first poll -> remove ticket, zero effects
  first poll
    stale provider epoch/generation or tombstoned -> Superseded
    spawn/exhaustion failure -> Err, zero effects
    current -> Admitted, independently driven owner command
      wait older same-provider command
      recheck provider epoch and generation before model-store first poll
      persistence Keep | definitive Set/Delete
      recheck provider epoch and generation after persistence
      if current: memory Keep/Set and Published
      if stale: suppress memory and Superseded
```

Dropping an admitted publication future removes only its waiter. The command
continues independently, retains its queue position, and wakes the next ticket
after completion. Store failure leaves memory unchanged, returns a redacted
ModelSource error, and advances the queue. Supersession after a definitive store
mutation returns `Superseded`: persistence may contain the old generation, but
the same-provider fence waits before a newer publication can write, and stale
memory is never applied.

Same-provider ticket, persistence, memory, and acknowledgement order are
identical. Different providers may enter persistence concurrently. The spawner
is called with no registry/owner lock held; `SpawnError` means no task was
admitted.

## Status, login, and logout

`check_auth` reads without refreshing OAuth. A matching stored OAuth credential
returns `AuthCheck {source: "OAuth", type: oauth}` even when expired. API-key
status calls side-effect-free `check` when present and falls back to resolution
only when that optional capability is absent.

`get_available` starts provider checks concurrently, restores insertion order
in its output, passes the detached stored credential to `filter_models`, and
propagates credential/auth/provider-model errors through
`AuthResolutionError`. It does not refresh OAuth. `get_auth` resolves provider-
scoped auth. `get_model_auth` performs the same resolution and then merges the
classified model headers after resolved auth headers without mutating either
input, matching the pinned overload.

Login runs the selected provider method before reading the credential store.
Unknown provider maps Provider; absent API-key/OAuth login maps Auth; API-key
provider failure maps Auth and OAuth failure maps OAuth. The candidate then:

- commits `Set` under a known mutation lease; or
- on `OutcomeUnknown`, explicitly `reconcile(Replace(candidate))` at the
  reserved revision.

Cancellation before login/lease/admission writes nothing and drops
non-cooperative work. Once credential commit/reconciliation admits, it is
shielded and successful login returns the candidate even if cancellation later
arrives, matching the pinned mutation-start behavior.

Logout commits `Delete` for known state or explicitly reconciles `Delete` for
quarantine. It shares the credential FIFO gate with OAuth refresh, so a queued
delete follows a completed rotation and a delete admitted first makes refresh
recheck missing; neither ordering resurrects credentials. Dropped admitted
logout still completes definitively.

## Request assembly and dispatch

Request auth is resolved on every call. Final auth fields are:

```text
api key: explicit request key, otherwise resolved key
environment: resolved environment, then explicit request environment
headers: resolved auth, then classified model, then explicit request
endpoint: credential-selected baseUrl, otherwise public model baseUrl,
          otherwise consumed runtime_endpoint for RuntimeResolved
```

Header collisions are ASCII-case-insensitive. A later entry removes all earlier
spellings and retains its own spelling/value. Explicit `None` suppresses an
earlier value. Source `Model`, `AuthResult`, headers, environment, and overrides
remain unchanged.

`runtime_endpoint` is ignored and dropped for credential/public endpoints. A
runtime-resolved model without a nonempty credential/runtime endpoint returns
ModelValidation before adapter dispatch. `RequestEndpoint::expose_for_transport`
remains the sole URL read boundary.

Stream/simple/fetch create an outer assistant stream synchronously, use the
request's `options.runtime.spawner` to spawn setup, and pump the selected
adapter stream. The registry-level spawner is reserved for membership and
publication commands. Unknown provider, authentication, endpoint, adapter
capability, and spawn failures become fixed in-band setup terminals; source
text is never copied. Caller cancellation settles Aborted and drops non-
cooperative auth/adapter consumption. Complete helpers await the outer result.
Deferred cancellation has no stream channel and returns
`AuthResolutionError`.

## Lock, drop, and reentrancy rules

No registry or owner mutex may be held while calling, polling, cloning, or
dropping provider/store/auth/adapter/spawner capabilities, task futures, wakers,
old provider/snapshot values, or error sources. Candidate/displaced values are
prepared before locks or detached under lock and dropped after unlock.

Lock order is membership then at most one owner. Clear never holds two owner
guards. Provider/store/auth are non-reentrant leaf ports; waker and value
destructors remain reentry-safe. Every waiter unregisters on future drop and is
woken only after lock release.

## Exact 30-row inventory

This unit owns `conformance/ai-models/registry-inventory.json`. The exact ids
and required test bindings are:

```text
registry.providers.order-replace-delete-clear
registry.models.list-find-failure-isolation
registry.constructor.exclusive-model-store
registry.auth.check-available-filter
registry.refresh.selection-error-cancel-matrix
registry.refresh.cache-before-credential-read
registry.refresh.offline-cache-only
registry.refresh.unconfigured-cache-only
registry.refresh.configured-network-phase
registry.supersede.new-refresh
registry.supersede.replace-provider
registry.supersede.delete-provider
registry.supersede.clear-providers
registry.publication.same-provider-serial
registry.publication.cross-provider-concurrent
registry.publication.two-three-superseded-sequence
registry.publication.drop-prepoll-ticket
registry.publication.drop-admitted-future
registry.publication.intent-matrix
registry.publication.persistence-fence-matrix
registry.restore.policy-reclassification
registry.login.method-error-matrix
registry.login.cancellation-admission
registry.login.quarantine-replace
registry.logout.refresh-and-quarantine-delete
registry.endpoint.precedence-custody
registry.headers.precedence-case-null
registry.dispatch.resolved-text-deferred-roots
registry.dispatch.adapter-capability-matrix
registry.dispatch.setup-error-redaction
```

The inventory binds those ids respectively to:

```text
provider_mutations_preserve_order_and_slots
listing_lookup_and_provider_failure_isolation
registry_ports_are_object_safe_and_model_store_is_exclusive
check_auth_and_available_filter_without_oauth_refresh
refresh_selection_error_and_cancellation_matrix
cache_publication_precedes_any_credential_read
offline_refresh_stops_after_cache_without_auth_or_network
unconfigured_refresh_restores_cache_then_skips_network
configured_refresh_runs_cache_then_network_with_effective_credential
new_refresh_fences_old_generation
provider_replacement_fences_old_generation
provider_deletion_fences_old_generation
provider_clear_fences_every_old_generation
same_provider_publications_follow_ticket_order
cross_provider_publications_can_overlap
repeated_publication_two_three_and_superseded_stop_trace
dropping_unpolled_publication_cancels_only_its_ticket
dropping_admitted_publication_finishes_before_next_ticket
memory_and_persistence_intents_are_independent_values
persistence_fence_before_during_after_admission_matrix
restore_reclassifies_raw_headers_under_current_policy
login_api_key_oauth_unknown_and_unsupported_matrix
login_cancellation_before_queue_and_after_commit_admission
explicit_login_reconciles_quarantine_at_reserved_revision
logout_refresh_race_and_quarantine_delete_matrix
endpoint_override_public_runtime_missing_and_custody_matrix
resolved_model_explicit_header_merge_case_and_null_matrix
resolver_builds_four_text_deferred_nominal_request_roots
dispatch_selects_adapter_and_optional_deferred_capabilities
dispatch_setup_failures_are_in_band_cancel_distinct_and_redacted
```

Normative metadata, in the same order, is:

| Id suffix | Source | Polarity | Expected observation |
|---|---|---|---|
| `providers.order-replace-delete-clear` | `models-registry.md:266` | positive | insertion/slot rules plus post-spawn global order and synchronous gate reentry |
| `models.list-find-failure-isolation` | `packages/ai/src/models.ts@853a80d26c90a14c1886f0ebb8ffaae133ca2185:294` | negative | all/single listing and lookup omit only a failing provider |
| `constructor.exclusive-model-store` | `upstream-map.toml:943` | boundary | construction consumes one non-clone model-store capability |
| `auth.check-available-filter` | `packages/ai/src/models.ts@853a80d26c90a14c1886f0ebb8ffaae133ca2185:538` | positive | check is side-effect-free, OAuth is not refreshed, and filtering receives the detached stored credential |
| `refresh.selection-error-cancel-matrix` | `packages/ai/src/models.ts@853a80d26c90a14c1886f0ebb8ffaae133ca2185:386` | boundary | selected dynamic only; caller cancellation sets aborted while generation supersession does not |
| `refresh.cache-before-credential-read` | `upstream-map.toml:903` | race | cache publication acknowledgement precedes the first credential read |
| `refresh.offline-cache-only` | `models-registry.md:414` | boundary | one cache phase, no credential/auth/network work |
| `refresh.unconfigured-cache-only` | `models-registry.md:416` | boundary | cache then auth, no second store read/network phase |
| `refresh.configured-network-phase` | `packages/ai/src/models.ts@853a80d26c90a14c1886f0ebb8ffaae133ca2185:410` | positive | cache then effective credential then fresh-store network phase with force |
| `supersede.new-refresh` | `upstream-map.toml:933` | race | first cache read waits an older admitted write, then observes its definitive state |
| `supersede.replace-provider` | `models-registry.md:381` | race | replacement closes the old epoch; a displaced visible provider cannot allocate successor authority |
| `supersede.delete-provider` | `packages/ai/src/models.ts@853a80d26c90a14c1886f0ebb8ffaae133ca2185:274` | race | delete fences/tombstones old owner; reinsert is a new appended incarnation |
| `supersede.clear-providers` | `packages/ai/src/models.ts@853a80d26c90a14c1886f0ebb8ffaae133ca2185:279` | race | clear fences all earlier owners; a later set survives |
| `publication.same-provider-serial` | `models-registry.md:471` | race | ticket, store, memory, and acknowledgement order are identical |
| `publication.cross-provider-concurrent` | `models-registry.md:472` | race | blocked provider A does not block provider B persistence/memory |
| `publication.two-three-superseded-sequence` | `models-registry.md:459` | race | two Published acknowledgements; three-attempt path stops at Superseded |
| `publication.drop-prepoll-ticket` | `models-registry.md:450` | race | unpolled drop has zero effects and does not strand the next ticket |
| `publication.drop-admitted-future` | `models-registry.md:463` | race | admitted drop finishes independently before the next ticket |
| `publication.intent-matrix` | `models-registry.md:70` | positive | all memory/persistence combinations are independent, persistence first |
| `publication.persistence-fence-matrix` | `models-registry.md:466` | race | before/during/after store admission has exact stale persistence/memory outcome |
| `restore.policy-reclassification` | `upstream-map.toml:873` | negative | current policy rejects cache before provider/credential access or secret exposure |
| `login.method-error-matrix` | `packages/ai/src/models.ts@853a80d26c90a14c1886f0ebb8ffaae133ca2185:565` | boundary | API/OAuth success plus unknown/unsupported/provider error categories |
| `login.cancellation-admission` | `packages/ai/src/models.ts@853a80d26c90a14c1886f0ebb8ffaae133ca2185:574` | race | pre-admission cancellation writes nothing; admitted persistence is definitive success |
| `login.quarantine-replace` | `models-registry.md:496` | negative | explicit login alone reconciles replacement at reserved revision |
| `logout.refresh-and-quarantine-delete` | `models-registry.md:504` | race | known/quarantined delete and refresh ordering never resurrect credentials |
| `endpoint.precedence-custody` | `upstream-map.toml:853` | boundary | credential/public/runtime precedence, missing sentinel error, immutable inputs |
| `headers.precedence-case-null` | `models-registry.md:522` | boundary | auth/model/explicit ASCII-insensitive replacement and explicit-null suppression |
| `dispatch.resolved-text-deferred-roots` | `packages/ai/src/models.ts@853a80d26c90a14c1886f0ebb8ffaae133ca2185:667` | boundary | all four registry-owned roots preserve model/auth/endpoint/options/runtime identity and complete helpers await stream roots |
| `dispatch.adapter-capability-matrix` | `packages/ai/src/models.ts@853a80d26c90a14c1886f0ebb8ffaae133ca2185:762` | boundary | API selection and distinct deferred optional capabilities |
| `dispatch.setup-error-redaction` | `upstream-map.toml:913` | negative | setup failures in-band, cancellation distinct, no source/secret terminal leak |

The final JSON repeats the full 40-hex commit ids and exact line numbers from
the frozen files.

Every row contains one exact pinned/local line, test name, polarity, owner, and
expected observation. Parameterized publication rows include pre/during/after
store admission, failure, dropped waiters, reentrant destructors, checked
counter exhaustion, and no global serialization. No test uses elapsed sleep,
network, credentials, or mutable upstream files.

`provider_mutations_preserve_order_and_slots` includes closed-gate spawners that
synchronously poll once and reenter through task/candidate drop on both `Ok`
and `Err`; an unresolved outer spawn never blocks the reentrant mutation, and
the observed order is the successful post-spawn admission order.
`new_refresh_fences_old_generation` holds an old admitted write after its store
admission, proves the successor performs no store read, releases the write, and
then proves an offline successor reads and restores the definitive new value.
It also proves generation-only supersession leaves `aborted=false`.
`provider_replacement_fences_old_generation` admits replacement while an old
write holds the public swap open, invokes refresh through the still-visible old
provider, and proves epoch validation prevents that displaced refresher from
performing any store, provider, or memory effect before or after replacement.

## Compile obligations

Compile probes require:

- `Arc<dyn Provider>` and `&(dyn ProviderModelsRefresher)` object safety;
- one shared lifetime across refresher/context/publisher/credential/stored/token;
- sequential repeated `publish()` compiles, while two retained publication
  futures fail with `E0499`;
- `ModelsPublisher` is `Send` but not Clone/Serde;
- `ModelsRegistryOptions` is moved into construction and cannot be reused;
- one boxed model store cannot construct two registries (`E0382`);
- `ModelsRegistry: Send + Sync` through `Arc`;
- root imports work through `pi_ai`;
- no adapter overload accepts raw model plus endpoint/auth strings.
