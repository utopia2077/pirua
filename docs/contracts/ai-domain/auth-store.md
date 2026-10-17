# Authentication and portable stores

> Historical fixture contract for the earlier v0.84.4 baseline. Retained
> source/behavior inventories cite these sections; they do not define the current
> v1.4.0 API. See the [current AI architecture](../../architecture/03-ai-subsystem.md)
> and [selected integration contracts](../ai-integration/index.md).

Authority: pinned upstream `853a80d26c90a14c1886f0ebb8ffaae133ca2185` and the accepted architecture.

## Unit boundary

This unit implements object-safe authentication ports, provider-auth
resolution, an exclusive credential mutation protocol, the in-memory
credential store, the portable model-store port, and its in-memory reference.
It does not implement `Provider`, `ModelsRegistry`, publication sequencing,
Azure runtime endpoint resolution, request dispatch, login/logout
orchestration, concrete OAuth/API-key providers, or host environment/file
access.

The following registry unit consumes these ports. An implementer must not
invent the deferred Provider/registry ABI inside this unit.

## Symbol disposition

| Pinned root surface | Rust disposition |
|---|---|
| credential/auth DTOs and `ModelsStoreEntry` | landed in S02 |
| `CredentialStore`, `InMemoryCredentialStore`, auth ports/resolution | this unit |
| `ModelsStore`, `InMemoryModelsStore` | this unit |
| `ModelsError` and its six `ModelsErrorCode` values | landed runtime surface |
| `Models`, `MutableModels`, `Provider`, `createModels`, login/logout | following S03 registry unit |
| `defaultProviderAuthContext` | S05 provider/host assembly; never portable core |
| `envApiKeyAuth`, `lazyOAuth` | S05 provider helpers |

`packages/ai/src/auth/context.ts` and `auth/helpers.ts` therefore map to the
provider-factory surface. Their interfaces remain seeded by `auth/types.ts`,
but their process/filesystem and concrete helper behavior is outside this
unit.

## Runtime-only credential types

All types in this section are non-Serde. Secret-bearing Debug output uses the
existing redacted credential DTO behavior.

```rust
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CredentialRevision(u64);

impl CredentialRevision {
    pub const FIRST: Self;
    pub const fn new(value: u64) -> Option<Self>;
    pub const fn get(self) -> u64;
    pub const fn checked_next(self) -> Option<Self>;
}

#[derive(Clone, Debug, PartialEq)]
pub struct VersionedCredential {
    pub revision: CredentialRevision,
    pub credential: Credential,
}

#[derive(Clone, Debug, PartialEq)]
pub enum CredentialMutationValue {
    Unchanged,
    Set(Credential),
    Delete,
}

#[derive(Clone, Debug, PartialEq)]
pub enum CredentialReconciliation {
    Replace(Credential),
    Delete,
}
```

`CredentialRevision::new(0)` returns `None`; all nonzero values are accepted so
an external persistent implementation can reconstruct durable state without an
unchecked constructor. The first stored credential has revision one. `Set`
always increments even when the value compares equal. `Unchanged` never
increments. Deleting an existing credential increments a retained tombstone
revision; deleting a missing entry is unchanged. A later replacement continues
from the tombstone. Increment past `u64::MAX` returns `RevisionExhausted`
without mutation.

The store's internal never-written state uses prior revision zero solely to
reserve `CredentialRevision::FIRST`; no public `VersionedCredential` exposes
zero.

Tombstone revision and live insertion order are separate. Updating a live
entry preserves its list slot, deletion removes the slot, and replacement
after deletion appends a new slot. Thus `Set(A), Set(B), Delete(A), Set(A)`
lists `B, A` while A's revision continues through its tombstone.

## Credential-store errors

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum CredentialStoreErrorCode {
    Cancelled,
    Failed,
    OutcomeUnknown,
    RevisionExhausted,
    ReconciliationNotRequired,
}

pub struct CredentialStoreError { /* private */ }

impl CredentialStoreError {
    pub const fn new(code: CredentialStoreErrorCode) -> Self;
    pub fn with_source(source: impl Error + Send + Sync + 'static) -> Self;
    pub const fn code(&self) -> CredentialStoreErrorCode;
}
```

`with_source` constructs `Failed`. Display is fixed by code:
`credential store operation cancelled`, `credential store operation failed`,
`credential store outcome unknown`, `credential revision exhausted`, and
`credential reconciliation not required`, in declaration order. Manual Debug
exposes only `code` and `source_present`. Optional
backend detail is reachable only through `Error::source()`. The type is
non-Serde and non-Clone.

## Credential store and lease

```rust
pub trait CredentialStore: Send + Sync {
    fn read<'a>(
        &'a self,
        provider: &'a ProviderId,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<VersionedCredential>, CredentialStoreError>>;

    fn list<'a>(
        &'a self,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Vec<CredentialInfo>, CredentialStoreError>>;

    fn begin_mutation<'a>(
        &'a self,
        provider: &'a ProviderId,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Box<dyn CredentialMutation + 'static>, CredentialStoreError>>;

    fn reconcile<'a>(
        &'a self,
        provider: &'a ProviderId,
        resolution: CredentialReconciliation,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<VersionedCredential>, CredentialStoreError>>;
}

pub trait CredentialMutation: Send {
    fn current(&self) -> Option<&VersionedCredential>;
    fn commit(
        self: Box<Self>,
        next: CredentialMutationValue,
    ) -> BoxFuture<'static, Result<Option<VersionedCredential>, CredentialStoreError>>;
}

pub struct InMemoryCredentialStore { /* private */ }

impl InMemoryCredentialStore {
    pub fn new() -> Self;
}

impl Default for InMemoryCredentialStore { /* new() */ }
```

`begin_mutation` and `reconcile` share one FIFO per-provider gate. Different
providers remain independent. A lease owns `Arc<inner> + ProviderId + detached
current + cloned acquisition token`; it never owns a `MutexGuard`. Dropping a
lease writes nothing.

Acquisition futures have stable waiter ids and explicit queued/granted states.
Gate ownership remains attributable to a granted waiter until that waiter
atomically returns its lease. Dropping or cancelling either a queued waiter or
a granted-but-unreturned waiter hands the gate to the next waiter. Grant versus
cancellation linearizes under the gate mutex. Candidate wakers are cloned
before locking; displaced/removed wakers are detached under lock and dropped or
invoked only after every store lock is released. No credential value, provider
capability, or host callback is cloned, dropped, or invoked under that lock.

The first poll of `commit` is the authoritative cancellation/admission point.
If its cloned token is already cancelled, it returns `Cancelled`, writes
nothing, and releases the gate. Successful admission means later cancellation
loses. The caller then shields and drives the commit to a definitive result.
The in-memory implementation linearizes and completes in that first poll. A
persistent implementation may return Pending after admission only if admitted
work is independently driven; a dropped admitted future must eventually
produce a definitive durable state or the quarantine below. A normal `Failed`
commit proves the prior known state remains authoritative.

## Durable uncertainty and reconciliation

A persistent credential backend atomically reserves the successor revision and
records provider-scoped intent before an effectful mutation whose
acknowledgement may be lost:

```text
Known {revision, credential: Some|None}
Quarantined {
  operationId,
  priorRevision,
  reservedRevision,
  priorPresenceAndOrder,
  intendedAction,
}
```

No quarantined state is valid without `reservedRevision == priorRevision + 1`.
If `priorRevision == u64::MAX`, reservation fails with `RevisionExhausted`
before any mutation. If post-state cannot be proven, the marker remains
authoritative across calls and reopen. `read`, `list`, and `begin_mutation`
return `OutcomeUnknown`; list fails as a whole rather than omitting or
fabricating metadata. Explicit API-key override resolution may skip the
credential store, but it does not inspect or clear quarantine. Normal stored or
ambient auth, refresh, and retry cannot clear quarantine.

`reconcile` is legal only for a quarantined provider; otherwise it returns
`ReconciliationNotRequired`. It waits in the same FIFO gate, performs an
authoritative state read after grant, and holds exclusive ownership through
known-state publication. Cancellation before its first-poll admission returns
`Cancelled` without changing the marker; later cancellation loses. A normal
failure preserves the marker. An uncertain reconciliation atomically replaces
its operation id/action but retains `priorRevision` and `reservedRevision`.

Successful `reconcile(Replace(newly_authenticated))` or `reconcile(Delete)`
establishes the selected known state at the existing reserved revision and
clears quarantine; it does not allocate another revision because no known state
at that revision was observable through the store. Repeated uncertain repairs
reuse that revision. Once a known state is published, a different known value
can never use the same revision. Thus an uncertainty reserved at
`u64::MAX` can be reconciled at `u64::MAX`, after which later effectful
mutations return `RevisionExhausted`.

Reconciled replacement retains the pre-quarantine live slot when one existed
and appends otherwise; reconciled deletion removes any live slot. The marker's
prior-presence/order field makes this deterministic even when the uncertain
effect itself added or removed the backend row.

The in-memory store normally has definitive commits but exposes private
fault-injection only to source-unit tests. A reopening trait fake proves the
persistent contract, including before-effect, after-effect, and repeated
reconciliation uncertainty.

## Leaf authentication errors

`AuthContextError`, `AuthInteractionError`, and `AuthProviderError` are three
distinct non-Serde, non-Clone structs with private optional boxed sources. Each
has exactly:

```rust
pub const fn new() -> Self;
pub fn with_source(source: impl Error + Send + Sync + 'static) -> Self;
```

Their fixed Display messages are respectively `authentication context failed`,
`authentication interaction failed`, and `authentication provider failed`.
Manual Debug exposes only `source_present`; `Error::source()` retains the
optional source. These errors do not classify caller cancellation: the shared
`CancellationToken` and the enclosing operation own that decision.

## Authentication ports

All async trait methods return the workspace `BoxFuture` with one explicit
shared lifetime.

```rust
pub trait AuthContext: Send + Sync {
    fn env<'a>(
        &'a self,
        name: &'a str,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<String>, AuthContextError>>;

    fn file_exists<'a>(
        &'a self,
        path: &'a str,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<bool, AuthContextError>>;
}

pub trait AuthInteraction: Send + Sync {
    fn prompt<'a>(
        &'a self,
        prompt: AuthPrompt,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<String, AuthInteractionError>>;

    fn notify(&self, event: AuthEvent) -> Result<(), AuthInteractionError>;
}

pub struct ApiKeyAuthInput<'a> {
    pub context: &'a dyn AuthContext,
    pub credential: Option<&'a ApiKeyCredential>,
    pub cancellation: &'a CancellationToken,
}

pub trait ApiKeyAuth: Send + Sync {
    fn name(&self) -> &str;
    fn login<'a>(
        &'a self,
        interaction: &'a dyn AuthInteraction,
        cancellation: &'a CancellationToken,
    ) -> Option<BoxFuture<'a, Result<ApiKeyCredential, AuthProviderError>>> {
        None
    }
    fn check<'a>(
        &'a self,
        input: ApiKeyAuthInput<'a>,
    ) -> Option<BoxFuture<'a, Result<Option<AuthCheck>, AuthProviderError>>> {
        None
    }
    fn resolve<'a>(
        &'a self,
        input: ApiKeyAuthInput<'a>,
    ) -> BoxFuture<'a, Result<Option<AuthResult>, AuthProviderError>>;
}

pub trait OAuthAuth: Send + Sync {
    fn name(&self) -> &str;
    fn is_subscription(&self) -> bool { false }
    fn login_label(&self) -> Option<&str> { None }
    fn login<'a>(
        &'a self,
        interaction: &'a dyn AuthInteraction,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<OAuthCredential, AuthProviderError>>;
    fn refresh<'a>(
        &'a self,
        credential: &'a OAuthCredential,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<OAuthCredential, AuthProviderError>>;
    fn to_auth<'a>(
        &'a self,
        credential: &'a OAuthCredential,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<ModelAuth, AuthProviderError>>;
}
```

`ApiKeyAuth::check` and `OAuthAuth::to_auth` are side-effect-free. Concrete
provider auth code is a non-reentrant leaf: it receives no store or registry
handle. The refresh deadline bounds an accidental same-provider reentry, but
reentrant providers are not otherwise supported.

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ProviderAuthError {
    NoCapabilities,
}

#[derive(Clone)]
pub struct ProviderAuth { /* private Arc capabilities */ }

impl ProviderAuth {
    pub fn new(
        api_key: Option<Arc<dyn ApiKeyAuth>>,
        oauth: Option<Arc<dyn OAuthAuth>>,
    ) -> Result<Self, ProviderAuthError>;
    pub fn api_key(&self) -> Option<Arc<dyn ApiKeyAuth>>;
    pub fn oauth(&self) -> Option<Arc<dyn OAuthAuth>>;
}
```

The constructor rejects two absent capabilities. Accessors clone only the Arc
capability. `ProviderAuthError` displays exactly `provider auth requires at
least one capability`. Debug reports booleans `api_key_present` and
`oauth_present`.

## Resolution result and overrides

```rust
#[derive(Default)]
pub struct AuthResolutionOverrides {
    pub api_key: Option<SecretString>,
    pub env: Option<ProviderEnv>,
    pub min_oauth_validity_ms: Option<SafeUnsigned>,
}

#[non_exhaustive]
pub enum AuthResolutionError {
    Cancelled,
    OutcomeUnknown,
    Models(ModelsError),
}

pub fn resolve_provider_auth<'a>(
    provider_id: &'a ProviderId,
    auth: &'a ProviderAuth,
    credentials: &'a dyn CredentialStore,
    context: &'a dyn AuthContext,
    clock: &'a dyn Clock,
    overrides: AuthResolutionOverrides,
    cancellation: &'a CancellationToken,
) -> BoxFuture<'a, Result<Option<AuthResult>, AuthResolutionError>>;
```

All three roots are non-Serde and Debug-redacted. `AuthResolutionError` displays
`authentication resolution cancelled`, `credential outcome unknown`, or the
contained fixed `ModelsError` text. Manual Debug exposes only its category and
the already-redacted `ModelsError`; `Error::source()` returns only the contained
`ModelsError`.

Resolution order is:

1. when an API-key capability exists and an explicit key is present, resolve
   that synthetic credential and return its result, including `None`; this
   branch never reads stored credentials or falls through;
2. otherwise read stored credentials and resolve the stored type;
3. only when storage is known empty, attempt ambient API-key resolution.

An explicit key without an API-key capability proceeds to stored resolution.
A stored type mismatch, stored refresh failure, or store quarantine blocks
ambient fallback. Request environment overlays the base context per field; a
nonempty override wins and an empty override follows pinned `||` fallback.
Stored API-key environment is first overlaid by request environment as raw
values, so an explicit empty stored-field override remains observable to a
provider using nullish credential lookup. Absence and an explicitly supplied
empty map remain distinct through `AuthResolutionOverrides`.

Every pre-commit port future races `cancellation.cancelled()` with simultaneous-
ready priority `caller cancellation -> port result`. This applies to credential
read and mutation acquisition, every explicit/stored/ambient API-key resolve,
and OAuth `to_auth`; dropping an outer API-key future also drops its nested
`AuthContext` waits. On cancellation, resolution signals any supplied linked
token and drops the pending port future before returning
`AuthResolutionError::Cancelled`. A post-result token check closes the gap
between observing port readiness and returning its value.

The specialized refresh race below replaces the two-way rule while refresh is
active. A successfully admitted commit is the sole shielded exception: outer
cancellation is not polled again until commit reaches a definitive result, at
which point cancellation may win before post-commit validity/derivation. Store
quarantine becomes `OutcomeUnknown`. Other store/context/API-key failures
become `Models(Auth)`. OAuth refresh, derivation, timeout, and explicit post-
refresh validity failures become `Models(OAuth)`. Source text is retained only
in error source chains and is never copied into Display, Debug, stream
terminals, or telemetry.

## OAuth refresh state machine

The effective minimum validity is `max(300_000, explicit_or_zero)`. A fresh
clock read triggers refresh when
`now.saturating_add(effective_minimum) >= expires`. A valid token never acquires
a mutation lease. An expired/near-expiry token acquires the lease, rechecks its
authoritative current value, and either uses a newer valid OAuth credential,
returns `None` for missing/mismatched current state, or refreshes once.

After recheck, resolution races three futures with fixed simultaneous-ready
priority: caller cancellation, the 15,000 ms deadline, then provider refresh.
At 14,999 ms a pending provider remains live. At exactly 15,000 ms the deadline
wins if provider completion was not already observed. Cancellation or deadline
cancels the linked refresh token and drops the provider future before releasing
the lease; a non-cooperative provider cannot retain the operation. Caller
cancellation returns `Cancelled`; deadline and provider failure return OAuth.

After provider success, caller cancellation gets one final chance before the
commit future's first poll. The commit's admission check is authoritative;
after successful admission it is shielded even if cancellation arrives. An
explicit caller minimum, including explicit zero, requires the refreshed token
to fall strictly outside the effective refresh window using a new clock read;
the default-only refresh does not reject a provider's short token. Commit
precedes that validity check, matching the pinned rotated-credential behavior.
`to_auth` then runs with caller cancellation, and successful OAuth resolution
uses the exact source string `OAuth`. Cancellation after commit may abort the
request but never rolls the rotated credential back.

## Model store

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ModelsStoreErrorCode {
    Cancelled,
    Failed,
}

pub struct ModelsStoreError { /* private */ }

impl ModelsStoreError {
    pub const fn new(code: ModelsStoreErrorCode) -> Self;
    pub fn with_source(source: impl Error + Send + Sync + 'static) -> Self;
    pub const fn code(&self) -> ModelsStoreErrorCode;
}

pub trait ModelsStore: Send + Sync {
    fn read<'a>(
        &'a self,
        provider: &'a ProviderId,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<RawModelsStoreEntry>, ModelsStoreError>>;
    fn write<'a>(
        &'a self,
        provider: &'a ProviderId,
        entry: ModelsStoreEntry,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), ModelsStoreError>>;
    fn delete<'a>(
        &'a self,
        provider: &'a ProviderId,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), ModelsStoreError>>;
}

pub struct InMemoryModelsStore { /* private */ }

impl InMemoryModelsStore {
    pub fn new() -> Self;
}

impl Default for InMemoryModelsStore { /* new() */ }
```

`with_source` constructs `Failed`; Display is exactly `models store operation
cancelled` or `models store operation failed`, and the remaining redaction/
source rules match the credential store. Reads return detached raw entries so
the later registry reapplies its current header policy. Writes take an owned
validated entry.

Each operation's first poll is its admission point. Cancellation observed
before admission returns `Cancelled` and preserves the old state. Successful
admission makes later cancellation lose. Success exposes one complete new
state; `Failed` proves the complete old state remains authoritative. Reads
observe one detached old-or-new snapshot, never a torn value. `OutcomeUnknown`
is forbidden; a backend unable to finish admitted work definitively is
nonconforming. Dropping an admitted model-store future cannot cancel its
operation: it continues independently to definitive success or `Failed`, and
no partial state is observable. The in-memory implementation completes in one
poll. The store is a non-reentrant leaf and one instance is consumed by one
live registry.

## Conformance inventory

This unit owns `conformance/ai-models/auth-store-inventory.json`, a validated
58-row fragment. The later registry unit adds
`conformance/ai-models/registry-inventory.json` with 30 rows; deterministic
aggregation produces the accepted 88-row registry/store/auth suite without
changing the S03 total of 327.

The 20 credential rows are:

```text
credential.read-missing
credential.set-first-revision
credential.set-equal-increments
credential.unchanged-preserves
credential.delete-existing-tombstone
credential.delete-missing-unchanged
credential.reinsert-revision-order
credential.revision-exhausted
credential.list-metadata-order
credential.detached-values
credential.gate-fifo
credential.gate-cross-provider
credential.gate-waiter-cancel
credential.gate-waiter-drop
credential.gate-granted-drop-handoff
credential.gate-reentrant-waker
credential.lease-drop-no-write
credential.commit-cancel-admission
credential.quarantine-sticky-reopen
credential.reconcile-reserved-revision
```

The eight model-store rows are:

```text
models-store.read-missing
models-store.write-read-detached
models-store.overwrite
models-store.delete
models-store.admission-cancel
models-store.post-admission-cancel
models-store.failure-preserves-old
models-store.atomic-read-write-delete
```

The 30 auth rows are:

```text
auth.provider-capability-validation
auth.port-object-safety-redaction
auth.explicit-api-key-short-circuit
auth.explicit-api-key-none-short-circuit
auth.explicit-api-key-without-capability
auth.stored-api-key-precedence
auth.stored-oauth-precedence
auth.ambient-empty-store
auth.stored-mismatch-blocks-ambient
auth.store-read-failed
auth.store-read-cancelled
auth.store-outcome-unknown
auth.env-overlay-nonempty
auth.env-overlay-empty-fallback
auth.stored-env-request-overlay
auth.api-key-none
auth.api-key-failed
auth.valid-oauth-no-mutation
auth.oauth-default-validity-boundaries
auth.oauth-explicit-validity
auth.oauth-lease-recheck
auth.oauth-32-callers-one-refresh
auth.oauth-refresh-failed-preserves
auth.oauth-timeout-boundaries
auth.api-key-cancel-noncooperative
auth.oauth-cancel-noncooperative
auth.oauth-cancel-before-commit
auth.oauth-commit-shielded
auth.oauth-to-auth-cancel-noncooperative
auth.oauth-to-auth-source-redaction
```

Parameterized witnesses under these ids cover queued/granted waiter drops,
grant/cancel barriers, reentrant waker destructors, refresh versus direct
reconciliation ordering, uncertainty before/after effect and across reopen,
repeated uncertain repair at `u64::MAX`, 32 callers forced through optimistic
reads before one refresh, all error source redaction, and recursive same-
provider refresh timing out without stranding the gate. Every race uses
barriers or manually polled futures, never elapsed sleeps.

Cancellation witnesses keep credential read, mutation acquisition, API-key
resolve, and OAuth `to_auth` permanently pending; cancellation must complete
and a drop probe must fire without manually releasing the port future.
`models-store.post-admission-cancel` parameterizes both token cancellation and
future drop through a manually driven persistent fake, proving a later atomic
old-or-new state.

Login/cancel, public logout/refresh, policy-aware restore, registry cache
ordering, provider generation, publication, endpoint/header precedence, and
resolved dispatch belong exclusively to the later 30-row registry fragment.
