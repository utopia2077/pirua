//! Provider registry, catalog publication, authentication, and text dispatch.

use crate::{ObservedAssistantStream, RequestLifecycleInvocation, RequestLifecycleOptions};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context as TaskContext, Poll, Waker};

use crate::adapter::ApiAdapter;
use crate::auth::{
    ApiKeyCredential, ApiKeyCredentialType, AuthCheck, AuthResult, AuthType, Credential, ModelAuth,
    ModelsErrorCode,
};
use crate::auth_runtime::{
    ApiKeyAuthInput, AuthContext, AuthInteraction, AuthResolutionError, AuthResolutionOverrides,
    ProviderAuth, resolve_provider_auth,
};
use crate::context::Context;
use crate::credential_store::{
    CredentialMutationValue, CredentialReconciliation, CredentialStore, CredentialStoreError,
    CredentialStoreErrorCode, VersionedCredential,
};
use crate::error::{ModelsError, StreamSetupFailure};
use crate::event::AssistantMessageEvent;
use crate::id::{ApiId, ModelId, ProviderId};
use crate::message::{AssistantMessage, DeferredHandle};
use crate::model::{Model, ModelBaseUrl, ModelHeaderPolicy, ModelsStoreEntry};
use crate::models_store::ModelsStore;
use crate::options::ProviderHeaders;
use crate::request::RequestEndpoint;
use crate::request::{
    DeferredCancelRequest, DeferredFetchRequest, ResolvedAuth, ResolvedRequest,
    SimpleStreamRequest, TextStreamRequest,
};
use crate::runtime::{
    BoxFuture, CancellationToken, Clock, DeferredCancelOptions, DeferredFetchOptions,
    SimpleStreamOptions, StreamOptions, TaskSpawner,
};
use crate::stream::{AssistantStream, AssistantStreamInit, create_assistant_message_event_stream};

/// Registry-owned dynamic model-memory intent.
#[derive(Clone, Debug, Default, PartialEq)]
pub enum ModelsMemoryIntent {
    /// Preserve the current dynamic overlay.
    #[default]
    Keep,
    /// Replace the dynamic overlay; an empty slice clears it.
    Set(Arc<[Model]>),
}

/// Provider-selected model-cache persistence intent.
#[derive(Clone, Debug, Default, PartialEq)]
pub enum ModelsPersistenceIntent {
    /// Preserve the current persisted catalog.
    #[default]
    Keep,
    /// Atomically replace the persisted catalog.
    Set(ModelsStoreEntry),
    /// Delete the persisted catalog.
    Delete,
}

/// One ordered model publication.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ModelsPublication {
    /// Dynamic memory update.
    pub memory: ModelsMemoryIntent,
    /// Durable cache update.
    pub persistence: ModelsPersistenceIntent,
}

/// Result of one generation-checked publication.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PublicationOutcome {
    /// Persistence and memory effects were accepted.
    Published,
    /// Provider epoch or refresh generation authority was displaced.
    Superseded,
}

/// Borrow-scoped registry publication capability.
///
/// ```compile_fail
/// fn requires_clone<T: Clone>() {}
/// requires_clone::<pi_ai_core::ModelsPublisher>();
/// ```
///
/// ```compile_fail
/// fn requires_serialize<T: serde::Serialize>() {}
/// requires_serialize::<pi_ai_core::ModelsPublisher>();
/// ```
///
/// ```compile_fail,E0499
/// # async fn overlap(publisher: &mut pi_ai_core::ModelsPublisher) {
/// let first = publisher.publish(pi_ai_core::ModelsPublication::default());
/// let second = publisher.publish(pi_ai_core::ModelsPublication::default());
/// let _ = (first, second);
/// # }
/// ```
///
/// ```
/// # async fn sequential(publisher: &mut pi_ai_core::ModelsPublisher) {
/// let _ = publisher.publish(pi_ai_core::ModelsPublication::default()).await;
/// let _ = publisher.publish(pi_ai_core::ModelsPublication::default()).await;
/// # }
/// ```
pub struct ModelsPublisher {
    inner: Arc<RegistryInner>,
    owner: Arc<ProviderOwner>,
    provider: ProviderId,
    epoch: u128,
    generation: u128,
    cancellation: CancellationToken,
}

impl ModelsPublisher {
    /// Reserves and submits one ordered publication.
    pub fn publish<'a>(
        &'a mut self,
        publication: ModelsPublication,
    ) -> BoxFuture<'a, Result<PublicationOutcome, ModelsError>> {
        Box::pin(PublicationFuture::new(self, publication))
    }
}

impl fmt::Debug for ModelsPublisher {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ModelsPublisher([PROCESS-LOCAL])")
    }
}

/// Optional provider capability for refreshing a dynamic model catalog.
pub trait ProviderModelsRefresher: Send + Sync {
    /// Runs one cache-only or network-enabled refresh phase.
    fn refresh_models<'a>(
        &'a self,
        context: RefreshModelsContext<'a>,
    ) -> BoxFuture<'a, Result<(), ModelsError>>;
}

/// Provider identity, model catalog, authentication, and adapter selection.
pub trait Provider: Send + Sync {
    /// Returns the provider identity.
    fn id(&self) -> &ProviderId;
    /// Returns the human-readable provider name.
    fn name(&self) -> &str;
    /// Returns validated authentication capabilities.
    fn auth(&self) -> &ProviderAuth;
    /// Returns a detached baseline model snapshot.
    fn models(&self) -> Result<Arc<[Model]>, ModelsError>;

    /// Returns the optional stable dynamic-catalog capability.
    fn models_refresher(&self) -> Option<&(dyn ProviderModelsRefresher + '_)> {
        None
    }

    /// Applies provider-specific availability filtering.
    fn filter_models(&self, models: &[Model], _credential: Option<&Credential>) -> Vec<Model> {
        models.to_vec()
    }

    /// Selects the adapter for one model API.
    fn adapter(&self, api: &ApiId) -> Option<Arc<dyn ApiAdapter>>;
}

/// Inputs supplied to one provider model-refresh phase.
pub struct RefreshModelsContext<'a> {
    /// Borrow-scoped ordered publisher.
    pub publisher: &'a mut ModelsPublisher,
    /// Effective configured credential for a network phase.
    pub credential: Option<&'a Credential>,
    /// Detached current cache snapshot.
    pub stored: Option<&'a ModelsStoreEntry>,
    /// Whether network access is allowed in this phase.
    pub allow_network: bool,
    /// Optional provider freshness override.
    pub force: Option<bool>,
    /// Phase cancellation.
    pub cancellation: &'a CancellationToken,
}

impl fmt::Debug for RefreshModelsContext<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RefreshModelsContext")
            .field("publisher", &self.publisher)
            .field("credential_present", &self.credential.is_some())
            .field("stored_present", &self.stored.is_some())
            .field("allow_network", &self.allow_network)
            .field("force", &self.force)
            .field("cancellation", &self.cancellation)
            .finish()
    }
}

/// Provider selection and network policy for one catalog refresh.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ModelsRefreshOptions {
    /// Whether configured providers may access the network.
    pub allow_network: bool,
    /// Optional provider-id selection with set semantics.
    pub providers: Option<Vec<ProviderId>>,
    /// Optional provider freshness override.
    pub force: Option<bool>,
}

impl Default for ModelsRefreshOptions {
    fn default() -> Self {
        Self {
            allow_network: true,
            providers: None,
            force: None,
        }
    }
}

/// Aggregate result of a catalog refresh.
#[derive(Debug)]
pub struct ModelsRefreshResult {
    /// Whether caller cancellation won.
    pub aborted: bool,
    /// Deterministic provider-scoped failures.
    pub errors: BTreeMap<ProviderId, AuthResolutionError>,
}

/// Process-local authentication, header, and runtime-endpoint overrides.
///
/// ```compile_fail
/// fn requires_clone<T: Clone>() {}
/// requires_clone::<pi_ai_core::ModelsRequestOverrides>();
/// ```
///
/// ```compile_fail
/// fn requires_serialize<T: serde::Serialize>() {}
/// requires_serialize::<pi_ai_core::ModelsRequestOverrides>();
/// ```
#[derive(Default)]
pub struct ModelsRequestOverrides {
    /// Authentication overrides.
    pub auth: AuthResolutionOverrides,
    /// Explicit request headers.
    pub headers: ProviderHeaders,
}

impl fmt::Debug for ModelsRequestOverrides {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ModelsRequestOverrides")
            .field("auth", &self.auth)
            .field("headers", &self.headers)
            .finish()
    }
}

/// Consumed process-local capabilities used by one registry.
///
/// ```compile_fail
/// fn requires_clone<T: Clone>() {}
/// requires_clone::<pi_ai_core::ModelsRegistryOptions>();
/// ```
///
/// ```compile_fail
/// fn requires_serialize<T: serde::Serialize>() {}
/// requires_serialize::<pi_ai_core::ModelsRegistryOptions>();
/// ```
///
/// ```compile_fail,E0382
/// # fn probe(options: pi_ai_core::ModelsRegistryOptions) {
/// let _first = pi_ai_core::ModelsRegistry::new(options);
/// let _second = pi_ai_core::ModelsRegistry::new(options);
/// # }
/// ```
///
/// ```compile_fail,E0382
/// # fn make_options(_store: Box<dyn pi_ai_core::ModelsStore>)
/// #     -> pi_ai_core::ModelsRegistryOptions { unimplemented!() }
/// # fn probe(store: Box<dyn pi_ai_core::ModelsStore>) {
/// let _first = make_options(store);
/// let _second = make_options(store);
/// # }
/// ```
pub struct ModelsRegistryOptions {
    /// Default calling application identity, merged with each request.
    pub identity: Option<crate::ClientIdentity>,
    /// Credential storage and per-provider mutation serialization.
    pub credentials: Arc<dyn CredentialStore>,
    /// Exclusively consumed model-cache storage.
    pub models_store: Box<dyn ModelsStore>,
    /// Ambient authentication context.
    pub auth_context: Arc<dyn AuthContext>,
    /// Injected time authority.
    pub clock: Arc<dyn Clock>,
    /// Registry-owned membership/publication task spawner.
    pub spawner: Arc<dyn TaskSpawner>,
    /// Current public model-header policy.
    pub model_header_policy: ModelHeaderPolicy,
}

struct RegistryInner {
    options: ModelsRegistryOptions,
    membership: Mutex<MembershipState>,
}

#[derive(Clone)]
struct Completion<T> {
    shared: Arc<CompletionShared<T>>,
}

struct CompletionShared<T> {
    state: Mutex<CompletionState<T>>,
}

struct CompletionState<T> {
    value: Option<T>,
    next_waiter: u64,
    waiters: BTreeMap<u64, Waker>,
}

impl<T> Completion<T> {
    fn pending() -> Self {
        Self {
            shared: Arc::new(CompletionShared {
                state: Mutex::new(CompletionState {
                    value: None,
                    next_waiter: 0,
                    waiters: BTreeMap::new(),
                }),
            }),
        }
    }

    fn completed(value: T) -> Self {
        Self {
            shared: Arc::new(CompletionShared {
                state: Mutex::new(CompletionState {
                    value: Some(value),
                    next_waiter: 0,
                    waiters: BTreeMap::new(),
                }),
            }),
        }
    }

    fn complete(&self, value: T) {
        let waiters = {
            let mut state = self
                .shared
                .state
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            if state.value.is_some() {
                return;
            }
            state.value = Some(value);
            std::mem::take(&mut state.waiters)
        };
        for waker in waiters.into_values() {
            waker.wake();
        }
    }

    fn wait(&self) -> CompletionFuture<'_, T> {
        CompletionFuture {
            completion: self,
            waiter_id: None,
        }
    }

    #[cfg(test)]
    fn is_complete(&self) -> bool {
        self.shared
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .value
            .is_some()
    }
}

struct CompletionFuture<'a, T> {
    completion: &'a Completion<T>,
    waiter_id: Option<u64>,
}

impl<T: Clone> Future for CompletionFuture<'_, T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, context: &mut TaskContext<'_>) -> Poll<Self::Output> {
        let candidate = context.waker().clone();
        let mut state = self
            .completion
            .shared
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if let Some(value) = state.value.clone() {
            let displaced = self
                .waiter_id
                .take()
                .and_then(|id| state.waiters.remove(&id));
            drop(state);
            drop(displaced);
            return Poll::Ready(value);
        }
        let id = self.waiter_id.unwrap_or_else(|| {
            loop {
                let id = state.next_waiter;
                state.next_waiter = state.next_waiter.wrapping_add(1);
                if !state.waiters.contains_key(&id) {
                    break id;
                }
            }
        });
        self.waiter_id = Some(id);
        let replace = state
            .waiters
            .get(&id)
            .is_none_or(|waker| !waker.will_wake(context.waker()));
        let displaced = replace
            .then(|| state.waiters.insert(id, candidate))
            .flatten();
        drop(state);
        drop(displaced);
        Poll::Pending
    }
}

impl<T> Drop for CompletionFuture<'_, T> {
    fn drop(&mut self) {
        if let Some(id) = self.waiter_id.take() {
            let displaced = self
                .completion
                .shared
                .state
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .waiters
                .remove(&id);
            drop(displaced);
        }
    }
}

struct ProviderRecord {
    id: ProviderId,
    provider: Arc<dyn Provider>,
    owner: Arc<ProviderOwner>,
    epoch: u128,
}

#[derive(Clone)]
struct ProjectedRecord {
    id: ProviderId,
    owner: Arc<ProviderOwner>,
    epoch: u128,
}

struct MembershipState {
    committed: Arc<[Arc<ProviderRecord>]>,
    projected: Vec<ProjectedRecord>,
    next_ticket: u128,
    tail: Completion<()>,
}

struct ProviderOwner {
    state: Mutex<ProviderOwnerState>,
}

struct GenerationAuthority {
    generation: u128,
    cancellation: CancellationToken,
}

struct GenerationLease {
    epoch: u128,
    generation: u128,
    cancellation: CancellationToken,
    cancel_on_drop: bool,
}

impl GenerationLease {
    fn finish(&mut self) {
        self.cancel_on_drop = false;
    }
}

impl Drop for GenerationLease {
    fn drop(&mut self) {
        if self.cancel_on_drop {
            self.cancellation.cancel();
        }
    }
}

struct ProviderOwnerState {
    current_epoch: Option<u128>,
    next_generation: u128,
    active: Option<GenerationAuthority>,
    next_publication_ticket: u128,
    reservations: BTreeSet<u128>,
    tail: Completion<()>,
    dynamic: Arc<[Model]>,
}

impl ProviderOwner {
    fn new() -> Self {
        Self {
            state: Mutex::new(ProviderOwnerState {
                current_epoch: None,
                next_generation: 0,
                active: None,
                next_publication_ticket: 0,
                reservations: BTreeSet::new(),
                tail: Completion::completed(()),
                dynamic: Arc::from([]),
            }),
        }
    }
}

fn allocate_checked(counter: &mut u128) -> Result<u128, ()> {
    let next = counter.checked_add(1).ok_or(())?;
    *counter = next;
    Ok(next)
}

#[derive(Clone)]
enum MutationGate {
    Run(MutationAssignment),
    Abort,
}

#[derive(Clone)]
struct MutationAssignment {
    ticket: u128,
    previous_membership: Completion<()>,
    owner_tails: Vec<Completion<()>>,
    owners: Vec<Arc<ProviderOwner>>,
}

enum MutationKind {
    Set(Arc<dyn Provider>),
    Delete(ProviderId),
    Clear,
}

#[derive(Clone, Copy)]
enum MutationResult {
    Success,
}

enum RefreshProviderResult {
    Complete,
    Superseded,
    CallerCancelled,
    Error(AuthResolutionError),
}

enum PhaseCancelled {
    Caller,
    Generation,
}

#[derive(Clone)]
enum PublicationGate {
    Run(Completion<()>),
    Abort,
}

#[derive(Clone)]
enum PublicationResult {
    Published,
    Superseded,
    Failed(Arc<Mutex<Option<crate::models_store::ModelsStoreError>>>),
}

enum PublicationFutureState {
    Exhausted,
    Reserved(u128),
    Waiting {
        result: Completion<PublicationResult>,
        waiter_id: Option<u64>,
    },
    Complete,
}

struct PublicationFuture<'a> {
    publisher: &'a ModelsPublisher,
    publication: Option<ModelsPublication>,
    state: PublicationFutureState,
}

impl<'a> PublicationFuture<'a> {
    fn new(publisher: &'a ModelsPublisher, publication: ModelsPublication) -> Self {
        let state = {
            let mut owner = publisher
                .owner
                .state
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            match allocate_checked(&mut owner.next_publication_ticket) {
                Ok(ticket) => {
                    owner.reservations.insert(ticket);
                    PublicationFutureState::Reserved(ticket)
                }
                Err(()) => PublicationFutureState::Exhausted,
            }
        };
        Self {
            publisher,
            publication: Some(publication),
            state,
        }
    }

    fn remove_reservation(&self, ticket: u128) {
        self.publisher
            .owner
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .reservations
            .remove(&ticket);
    }

    fn poll_result(
        result: &Completion<PublicationResult>,
        waiter_id: &mut Option<u64>,
        context: &mut TaskContext<'_>,
    ) -> Poll<PublicationResult> {
        let candidate = context.waker().clone();
        let mut state = result
            .shared
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if let Some(value) = state.value.clone() {
            let displaced = waiter_id.take().and_then(|id| state.waiters.remove(&id));
            drop(state);
            drop(displaced);
            return Poll::Ready(value);
        }
        let id = waiter_id.unwrap_or_else(|| {
            loop {
                let id = state.next_waiter;
                state.next_waiter = state.next_waiter.wrapping_add(1);
                if !state.waiters.contains_key(&id) {
                    break id;
                }
            }
        });
        *waiter_id = Some(id);
        let replace = state
            .waiters
            .get(&id)
            .is_none_or(|waker| !waker.will_wake(context.waker()));
        let displaced = replace
            .then(|| state.waiters.insert(id, candidate))
            .flatten();
        drop(state);
        drop(displaced);
        Poll::Pending
    }
}

impl Future for PublicationFuture<'_> {
    type Output = Result<PublicationOutcome, ModelsError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut TaskContext<'_>) -> Poll<Self::Output> {
        loop {
            match &mut self.state {
                PublicationFutureState::Exhausted => {
                    self.state = PublicationFutureState::Complete;
                    return Poll::Ready(Err(ModelsError::new(ModelsErrorCode::ModelSource)));
                }
                PublicationFutureState::Reserved(ticket) => {
                    let ticket = *ticket;
                    if self.publisher.cancellation.is_cancelled() {
                        self.remove_reservation(ticket);
                        self.state = PublicationFutureState::Complete;
                        return Poll::Ready(Ok(PublicationOutcome::Superseded));
                    }
                    let current = {
                        let mut owner = self
                            .publisher
                            .owner
                            .state
                            .lock()
                            .unwrap_or_else(|poison| poison.into_inner());
                        let current = owner.reservations.contains(&ticket)
                            && publication_authority_matches(
                                &owner,
                                self.publisher.epoch,
                                self.publisher.generation,
                            );
                        if !current {
                            owner.reservations.remove(&ticket);
                        }
                        current
                    };
                    if !current {
                        self.state = PublicationFutureState::Complete;
                        return Poll::Ready(Ok(PublicationOutcome::Superseded));
                    }
                    let gate = Completion::pending();
                    let result = Completion::pending();
                    let command_tail = Completion::pending();
                    let task_inner = Arc::clone(&self.publisher.inner);
                    let task_owner = Arc::clone(&self.publisher.owner);
                    let task_provider = self.publisher.provider.clone();
                    let task_gate = gate.clone();
                    let task_result = result.clone();
                    let task_tail = command_tail.clone();
                    let epoch = self.publisher.epoch;
                    let generation = self.publisher.generation;
                    let cancellation = self.publisher.cancellation.clone();
                    let publication = self
                        .publication
                        .take()
                        .expect("reserved publication retains its value");
                    let task = Box::pin(async move {
                        run_publication_command(
                            task_inner,
                            task_owner,
                            task_provider,
                            epoch,
                            generation,
                            cancellation,
                            publication,
                            task_gate,
                            task_result,
                            task_tail,
                        )
                        .await;
                    });
                    if let Err(error) = self.publisher.inner.options.spawner.spawn(task) {
                        self.remove_reservation(ticket);
                        gate.complete(PublicationGate::Abort);
                        self.state = PublicationFutureState::Complete;
                        return Poll::Ready(Err(ModelsError::with_source(
                            ModelsErrorCode::ModelSource,
                            error,
                        )));
                    }
                    let previous = {
                        let mut owner = self
                            .publisher
                            .owner
                            .state
                            .lock()
                            .unwrap_or_else(|poison| poison.into_inner());
                        let reserved = owner.reservations.remove(&ticket);
                        let current = publication_authority_matches(
                            &owner,
                            self.publisher.epoch,
                            self.publisher.generation,
                        );
                        if reserved && current {
                            let previous = owner.tail.clone();
                            owner.tail = command_tail;
                            Some(previous)
                        } else {
                            None
                        }
                    };
                    let Some(previous) = previous else {
                        gate.complete(PublicationGate::Abort);
                        self.state = PublicationFutureState::Complete;
                        return Poll::Ready(Ok(PublicationOutcome::Superseded));
                    };
                    gate.complete(PublicationGate::Run(previous));
                    self.state = PublicationFutureState::Waiting {
                        result,
                        waiter_id: None,
                    };
                }
                PublicationFutureState::Waiting { result, waiter_id } => {
                    let outcome = match Self::poll_result(result, waiter_id, context) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(outcome) => outcome,
                    };
                    self.state = PublicationFutureState::Complete;
                    return Poll::Ready(match outcome {
                        PublicationResult::Published => Ok(PublicationOutcome::Published),
                        PublicationResult::Superseded => Ok(PublicationOutcome::Superseded),
                        PublicationResult::Failed(failure) => {
                            let source = failure
                                .lock()
                                .unwrap_or_else(|poison| poison.into_inner())
                                .take();
                            match source {
                                Some(source) => Err(ModelsError::with_source(
                                    ModelsErrorCode::ModelSource,
                                    source,
                                )),
                                None => Err(ModelsError::new(ModelsErrorCode::ModelSource)),
                            }
                        }
                    });
                }
                PublicationFutureState::Complete => panic!("publication future polled twice"),
            }
        }
    }
}

impl Drop for PublicationFuture<'_> {
    fn drop(&mut self) {
        if let PublicationFutureState::Reserved(ticket) = &self.state {
            self.remove_reservation(*ticket);
            return;
        }
        if let PublicationFutureState::Waiting { result, waiter_id } = &mut self.state
            && let Some(id) = waiter_id.take()
        {
            let displaced = result
                .shared
                .state
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .waiters
                .remove(&id);
            drop(displaced);
        }
    }
}

fn publication_authority_matches(
    owner: &ProviderOwnerState,
    epoch: u128,
    generation: u128,
) -> bool {
    owner.current_epoch == Some(epoch)
        && owner
            .active
            .as_ref()
            .is_some_and(|active| active.generation == generation)
}

/// Concrete provider and model registry.
///
/// ```compile_fail
/// fn requires_clone<T: Clone>() {}
/// requires_clone::<pi_ai_core::ModelsRegistry>();
/// ```
///
/// ```compile_fail
/// fn requires_serialize<T: serde::Serialize>() {}
/// requires_serialize::<pi_ai_core::ModelsRegistry>();
/// ```
pub struct ModelsRegistry {
    inner: Arc<RegistryInner>,
    child_cancellation: bool,
}

/// Opaque identity of one committed registration in one registry incarnation.
#[derive(Clone)]
pub struct ProviderRegistration {
    registry: Weak<RegistryInner>,
    record: Arc<ProviderRecord>,
}
impl ProviderRegistration {
    /// Compares registration allocations, not merely the provider object's Arc.
    pub fn same_registration(&self, other: &Self) -> bool {
        Weak::ptr_eq(&self.registry, &other.registry) && Arc::ptr_eq(&self.record, &other.record)
    }
}
/// One-use authority to dispatch a full text model through a captured registration.
pub struct CapturedTextDispatch {
    registry: Arc<RegistryInner>,
    record: Arc<ProviderRecord>,
    model: Model,
    child_cancellation: bool,
}
impl CapturedTextDispatch {
    /// The complete observed model, fixed independently of later catalog changes.
    pub fn model(&self) -> &Model {
        &self.model
    }
}
/// Failure to bind a request to the expected registration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CaptureDispatchError {
    /// The token or dispatch belongs to a different registry instance.
    WrongRegistry,
    /// No registration remains for this provider identity.
    MissingRegistration,
    /// A different registration, including a re-registration of the same Arc, won.
    ReplacedRegistration,
    /// The supplied model belongs to a different provider.
    ModelProviderMismatch,
}
impl fmt::Display for CaptureDispatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "request capture failed: {self:?}")
    }
}
impl std::error::Error for CaptureDispatchError {}
impl fmt::Debug for ProviderRegistration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ProviderRegistration([PROCESS-LOCAL])")
    }
}
impl fmt::Debug for CapturedTextDispatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CapturedTextDispatch([PROCESS-LOCAL])")
    }
}

impl ModelsRegistry {
    /// Creates an empty registry and consumes its exclusive model store.
    pub fn new(options: ModelsRegistryOptions) -> Self {
        Self::with_cancellation_owner(options, false)
    }

    /// Creates a registry whose dispatched child owns cancellation after handoff.
    ///
    /// Setup cancellation retains the default empty aborted result. Once an
    /// adapter returns its validated stream, its terminal content and usage are
    /// drained even when the original token is cancelled. Trusted adapters must
    /// wire that child to the supplied request token and eventually settle it.
    /// Dropping a consumer does not cancel the independently spawned dispatch.
    pub fn new_with_child_cancellation(options: ModelsRegistryOptions) -> Self {
        Self::with_cancellation_owner(options, true)
    }

    fn with_cancellation_owner(options: ModelsRegistryOptions, child_cancellation: bool) -> Self {
        Self {
            child_cancellation,
            inner: Arc::new(RegistryInner {
                options,
                membership: Mutex::new(MembershipState {
                    committed: Arc::from([]),
                    projected: Vec::new(),
                    next_ticket: 0,
                    tail: Completion::completed(()),
                }),
            }),
        }
    }

    fn committed_snapshot(&self) -> Arc<[Arc<ProviderRecord>]> {
        self.inner
            .membership
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .committed
            .clone()
    }

    fn provider_record(&self, provider: &ProviderId) -> Option<Arc<ProviderRecord>> {
        let snapshot = self.committed_snapshot();
        snapshot
            .iter()
            .find(|record| &record.id == provider)
            .cloned()
    }

    /// Observes committed registration identity without invoking provider code.
    pub fn provider_registration(&self, provider: &ProviderId) -> Option<ProviderRegistration> {
        let membership = self
            .inner
            .membership
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        membership
            .committed
            .iter()
            .find(|record| &record.id == provider)
            .map(|record| ProviderRegistration {
                registry: Arc::downgrade(&self.inner),
                record: Arc::clone(record),
            })
    }

    /// Atomically compares and captures a registration; performs no callbacks or dispatch.
    pub fn capture_text_dispatch(
        &self,
        expected: &ProviderRegistration,
        model: Model,
    ) -> Result<CapturedTextDispatch, CaptureDispatchError> {
        if !Weak::ptr_eq(&expected.registry, &Arc::downgrade(&self.inner)) {
            return Err(CaptureDispatchError::WrongRegistry);
        }
        if model.provider != expected.record.id {
            return Err(CaptureDispatchError::ModelProviderMismatch);
        }
        let record = {
            let membership = self
                .inner
                .membership
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            let current = membership
                .committed
                .iter()
                .find(|record| record.id == expected.record.id)
                .ok_or(CaptureDispatchError::MissingRegistration)?;
            if !Arc::ptr_eq(current, &expected.record) || current.epoch != expected.record.epoch {
                return Err(CaptureDispatchError::ReplacedRegistration);
            }
            Arc::clone(current)
        };
        Ok(CapturedTextDispatch {
            registry: Arc::clone(&self.inner),
            record,
            model,
            child_cancellation: self.child_cancellation,
        })
    }

    /// Consumes captured authority outside caller locks and uses the ordinary request worker.
    pub fn stream_simple_captured_with_lifecycle(
        &self,
        captured: CapturedTextDispatch,
        context: Context,
        overrides: ModelsRequestOverrides,
        options: SimpleStreamOptions,
        lifecycle: RequestLifecycleOptions,
    ) -> Result<ObservedAssistantStream, CaptureDispatchError> {
        if !Arc::ptr_eq(&self.inner, &captured.registry) {
            return Err(CaptureDispatchError::WrongRegistry);
        }
        let (lifecycle, metadata) = RequestLifecycleInvocation::new(lifecycle);
        let stream = start_dispatch_selected(
            captured.registry,
            captured.child_cancellation,
            captured.model,
            overrides,
            DispatchRequest::Simple {
                context,
                options,
                lifecycle: Some(lifecycle),
            },
            Some(captured.record),
        );
        Ok(ObservedAssistantStream { stream, metadata })
    }

    /// Consumes captured provider selection and observes one deferred fetch.
    pub fn fetch_deferred_captured_with_lifecycle(
        &self,
        captured: CapturedTextDispatch,
        handle: DeferredHandle,
        overrides: ModelsRequestOverrides,
        options: DeferredFetchOptions,
        lifecycle: RequestLifecycleOptions,
    ) -> Result<ObservedAssistantStream, CaptureDispatchError> {
        if !Arc::ptr_eq(&self.inner, &captured.registry) {
            return Err(CaptureDispatchError::WrongRegistry);
        }
        let (lifecycle, metadata) = RequestLifecycleInvocation::new(lifecycle);
        let stream = start_dispatch_selected(
            captured.registry,
            captured.child_cancellation,
            captured.model,
            overrides,
            DispatchRequest::Fetch {
                handle,
                options,
                lifecycle: Some(lifecycle),
            },
            Some(captured.record),
        );
        Ok(ObservedAssistantStream { stream, metadata })
    }

    /// Consumes captured provider selection for an owned best-effort cancellation.
    pub fn cancel_deferred_captured(
        &self,
        captured: CapturedTextDispatch,
        handle: DeferredHandle,
        overrides: ModelsRequestOverrides,
        options: DeferredCancelOptions,
    ) -> Result<BoxFuture<'static, Result<(), AuthResolutionError>>, CaptureDispatchError> {
        if !Arc::ptr_eq(&self.inner, &captured.registry) {
            return Err(CaptureDispatchError::WrongRegistry);
        }
        Ok(cancel_deferred_selected(
            captured.registry,
            captured.model,
            handle,
            overrides,
            options,
            Some(captured.record),
        ))
    }

    fn mutate<'a>(&'a self, kind: MutationKind) -> BoxFuture<'a, Result<(), ModelsError>> {
        let intent = match &kind {
            MutationKind::Set(provider) => MutationIntent::Set(provider.id().clone()),
            MutationKind::Delete(provider) => MutationIntent::Delete(provider.clone()),
            MutationKind::Clear => MutationIntent::Clear,
        };
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            let gate = Completion::pending();
            let done = Completion::pending();
            let command_tail = Completion::pending();
            let task_inner = Arc::clone(&inner);
            let task_gate = gate.clone();
            let task_done = done.clone();
            let task_tail = command_tail.clone();
            let task = Box::pin(async move {
                run_membership_command(task_inner, task_gate, task_done, task_tail, kind).await;
            });
            if let Err(error) = inner.options.spawner.spawn(task) {
                gate.complete(MutationGate::Abort);
                return Err(ModelsError::with_source(
                    ModelsErrorCode::ModelSource,
                    error,
                ));
            }

            let fresh_owner = Arc::new(ProviderOwner::new());
            let admitted = admit_mutation(&inner, intent, fresh_owner, command_tail);
            let Ok((assignment, cancelled)) = admitted else {
                gate.complete(MutationGate::Abort);
                return Err(ModelsError::new(ModelsErrorCode::ModelSource));
            };
            for token in cancelled {
                token.cancel();
            }
            gate.complete(MutationGate::Run(assignment));
            match done.wait().await {
                MutationResult::Success => Ok(()),
            }
        })
    }

    /// Inserts or replaces one provider and fences older admitted effects.
    pub fn set_provider<'a>(
        &'a self,
        provider: Arc<dyn Provider>,
    ) -> BoxFuture<'a, Result<(), ModelsError>> {
        self.mutate(MutationKind::Set(provider))
    }

    /// Deletes one provider and fences older admitted effects.
    pub fn delete_provider<'a>(
        &'a self,
        provider: &'a ProviderId,
    ) -> BoxFuture<'a, Result<(), ModelsError>> {
        self.mutate(MutationKind::Delete(provider.clone()))
    }

    /// Clears every provider and fences all older admitted effects.
    pub fn clear_providers<'a>(&'a self) -> BoxFuture<'a, Result<(), ModelsError>> {
        self.mutate(MutationKind::Clear)
    }

    /// Returns providers in insertion order.
    pub fn get_providers(&self) -> Vec<Arc<dyn Provider>> {
        let snapshot = self.committed_snapshot();
        snapshot
            .iter()
            .map(|record| Arc::clone(&record.provider))
            .collect()
    }

    /// Returns one provider by identity.
    pub fn get_provider(&self, provider: &ProviderId) -> Option<Arc<dyn Provider>> {
        let snapshot = self.committed_snapshot();
        snapshot
            .iter()
            .find(|record| &record.id == provider)
            .map(|record| Arc::clone(&record.provider))
    }

    /// Returns merged baseline and dynamic models, omitting only failing providers.
    pub fn get_models(&self, provider: Option<&ProviderId>) -> Vec<Model> {
        let snapshot = self.committed_snapshot();
        let mut output = Vec::new();
        for record in snapshot
            .iter()
            .filter(|record| provider.is_none_or(|provider| &record.id == provider))
        {
            let Ok(baseline) = record.provider.models() else {
                continue;
            };
            let dynamic = {
                record
                    .owner
                    .state
                    .lock()
                    .unwrap_or_else(|poison| poison.into_inner())
                    .dynamic
                    .clone()
            };
            let mut merged = baseline.to_vec();
            for model in dynamic.iter() {
                if let Some(index) = merged.iter().position(|entry| entry.id == model.id) {
                    merged[index] = model.clone();
                } else {
                    merged.push(model.clone());
                }
            }
            output.extend(merged);
        }
        output
    }

    /// Returns one model by provider-scoped identity.
    pub fn get_model(&self, provider: &ProviderId, model: &ModelId) -> Option<Model> {
        self.get_models(Some(provider))
            .into_iter()
            .find(|entry| &entry.id == model)
    }

    /// Refreshes selected dynamic providers.
    pub fn refresh<'a>(
        &'a self,
        options: ModelsRefreshOptions,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, ModelsRefreshResult> {
        let inner = Arc::clone(&self.inner);
        let caller = cancellation.clone();
        Box::pin(async move {
            if caller.is_cancelled() {
                return ModelsRefreshResult {
                    aborted: true,
                    errors: BTreeMap::new(),
                };
            }
            let selected = options
                .providers
                .as_ref()
                .map(|providers| providers.iter().cloned().collect::<BTreeSet<_>>());
            let snapshot = {
                inner
                    .membership
                    .lock()
                    .unwrap_or_else(|poison| poison.into_inner())
                    .committed
                    .clone()
            };
            let mut futures = snapshot
                .iter()
                .filter(|record| {
                    selected
                        .as_ref()
                        .is_none_or(|selected| selected.contains(&record.id))
                })
                .map(|record| {
                    let inner = Arc::clone(&inner);
                    let record = Arc::clone(record);
                    let caller = caller.clone();
                    let allow_network = options.allow_network;
                    let force = options.force;
                    Box::pin(async move {
                        let id = record.id.clone();
                        let result =
                            refresh_provider(inner, record, allow_network, force, caller).await;
                        (id, result)
                    })
                        as BoxFuture<'static, (ProviderId, RefreshProviderResult)>
                })
                .map(Some)
                .collect::<Vec<_>>();
            let mut completed = (0..futures.len()).map(|_| None).collect::<Vec<_>>();
            let results = poll_fn(|context| {
                let mut pending = 0;
                for (index, future) in futures.iter_mut().enumerate() {
                    let Some(active) = future.as_mut() else {
                        continue;
                    };
                    match active.as_mut().poll(context) {
                        Poll::Ready(value) => {
                            completed[index] = Some(value);
                            *future = None;
                        }
                        Poll::Pending => pending += 1,
                    }
                }
                if pending != 0 {
                    Poll::Pending
                } else {
                    let output: Vec<(ProviderId, RefreshProviderResult)> = completed
                        .iter_mut()
                        .map(|value| value.take().expect("completed refresh has a result"))
                        .collect();
                    Poll::Ready(output)
                }
            })
            .await;
            let mut aborted = caller.is_cancelled();
            let mut errors = BTreeMap::new();
            for (provider, result) in results {
                match result {
                    RefreshProviderResult::Complete | RefreshProviderResult::Superseded => {}
                    RefreshProviderResult::CallerCancelled => aborted = true,
                    RefreshProviderResult::Error(error) => {
                        errors.insert(provider, error);
                    }
                }
            }
            ModelsRefreshResult { aborted, errors }
        })
    }

    /// Checks credential availability without refreshing OAuth. An explicit
    /// no-auth provider returns `None` here while its models remain available.
    pub fn check_auth<'a>(
        &'a self,
        provider: &'a ProviderId,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<AuthCheck>, AuthResolutionError>> {
        let record = self.provider_record(provider);
        let inner = Arc::clone(&self.inner);
        let cancellation = cancellation.clone();
        Box::pin(async move {
            if cancellation.is_cancelled() {
                return Err(AuthResolutionError::Cancelled);
            }
            let Some(record) = record else {
                return Ok(None);
            };
            if record.provider.auth().is_no_auth() {
                return Ok(None);
            }
            let credential = read_auth_credential(&inner, &record.id, &cancellation).await?;
            check_provider_auth(&inner, &record, credential.as_ref(), &cancellation).await
        })
    }

    /// Returns currently available models in provider insertion order.
    pub fn get_available<'a>(
        &'a self,
        provider: Option<&'a ProviderId>,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Vec<Model>, AuthResolutionError>> {
        let selected = provider.cloned();
        let snapshot = self.committed_snapshot();
        let inner = Arc::clone(&self.inner);
        let cancellation = cancellation.clone();
        Box::pin(async move {
            if cancellation.is_cancelled() {
                return Err(AuthResolutionError::Cancelled);
            }
            let futures = snapshot
                .iter()
                .filter(|record| {
                    selected
                        .as_ref()
                        .is_none_or(|provider| &record.id == provider)
                })
                .map(|record| {
                    let inner = Arc::clone(&inner);
                    let record = Arc::clone(record);
                    let cancellation = cancellation.clone();
                    Box::pin(async move { available_provider(inner, record, cancellation).await })
                        as BoxFuture<'static, Result<Vec<Model>, AuthResolutionError>>
                })
                .collect();
            let results = join_all(futures).await;
            let mut models = Vec::new();
            for result in results {
                models.extend(result?);
            }
            Ok(models)
        })
    }

    /// Resolves provider-scoped request authentication.
    pub fn get_auth<'a>(
        &'a self,
        provider: &'a ProviderId,
        overrides: AuthResolutionOverrides,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<AuthResult>, AuthResolutionError>> {
        let record = self.provider_record(provider);
        Box::pin(async move {
            if cancellation.is_cancelled() {
                return Err(AuthResolutionError::Cancelled);
            }
            let Some(record) = record else {
                return Ok(None);
            };
            resolve_provider_auth(
                &record.id,
                record.provider.auth(),
                self.inner.options.credentials.as_ref(),
                self.inner.options.auth_context.as_ref(),
                self.inner.options.clock.as_ref(),
                overrides,
                cancellation,
            )
            .await
        })
    }

    /// Resolves model-scoped request authentication and classified headers.
    pub fn get_model_auth<'a>(
        &'a self,
        model: &'a Model,
        overrides: AuthResolutionOverrides,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<AuthResult>, AuthResolutionError>> {
        Box::pin(async move {
            if cancellation.is_cancelled() {
                return Err(AuthResolutionError::Cancelled);
            }
            let Some(record) = self.provider_record(&model.provider) else {
                return Ok(None);
            };
            if record.provider.auth().is_no_auth() && model.headers.is_some() {
                return Err(AuthResolutionError::Models(ModelsError::new(
                    ModelsErrorCode::Auth,
                )));
            }
            let result = resolve_provider_auth(
                &record.id,
                record.provider.auth(),
                self.inner.options.credentials.as_ref(),
                self.inner.options.auth_context.as_ref(),
                self.inner.options.clock.as_ref(),
                overrides,
                cancellation,
            )
            .await?;
            let Some(mut result) = result else {
                return Ok(None);
            };
            if let Some(headers) = &model.headers {
                let model_headers = ProviderHeaders::new(
                    headers
                        .as_map()
                        .iter()
                        .map(|(name, value)| (name.clone(), Some(value.clone())))
                        .collect(),
                );
                result.auth.headers = Some(merge_headers(
                    result.auth.headers.take().unwrap_or_default(),
                    model_headers,
                ));
            }
            Ok(Some(result))
        })
    }

    /// Runs and persists one provider-owned login method.
    pub fn login<'a>(
        &'a self,
        provider: &'a ProviderId,
        auth_type: AuthType,
        interaction: &'a dyn AuthInteraction,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Credential, AuthResolutionError>> {
        let record = self.provider_record(provider);
        Box::pin(async move {
            if cancellation.is_cancelled() {
                return Err(AuthResolutionError::Cancelled);
            }
            let Some(record) = record else {
                return Err(AuthResolutionError::Models(ModelsError::new(
                    ModelsErrorCode::Provider,
                )));
            };
            let candidate = match auth_type {
                AuthType::ApiKey => {
                    let Some(api_key) = record.provider.auth().api_key() else {
                        return Err(AuthResolutionError::Models(ModelsError::new(
                            ModelsErrorCode::Auth,
                        )));
                    };
                    let Some(login) = api_key.login(interaction, cancellation) else {
                        return Err(AuthResolutionError::Models(ModelsError::new(
                            ModelsErrorCode::Auth,
                        )));
                    };
                    Credential::ApiKey(caller_first(cancellation, login).await?.map_err(
                        |error| {
                            AuthResolutionError::Models(ModelsError::with_source(
                                ModelsErrorCode::Auth,
                                error,
                            ))
                        },
                    )?)
                }
                AuthType::OAuth => {
                    let Some(oauth) = record.provider.auth().oauth() else {
                        return Err(AuthResolutionError::Models(ModelsError::new(
                            ModelsErrorCode::Auth,
                        )));
                    };
                    Credential::OAuth(
                        caller_first(cancellation, oauth.login(interaction, cancellation))
                            .await?
                            .map_err(|error| {
                                AuthResolutionError::Models(ModelsError::with_source(
                                    ModelsErrorCode::OAuth,
                                    error,
                                ))
                            })?,
                    )
                }
            };
            persist_login_candidate(
                self.inner.options.credentials.as_ref(),
                &record.id,
                candidate.clone(),
                cancellation,
            )
            .await?;
            Ok(candidate)
        })
    }

    /// Deletes or explicitly reconciles one provider credential.
    pub fn logout<'a>(
        &'a self,
        provider: &'a ProviderId,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), AuthResolutionError>> {
        Box::pin(async move {
            persist_logout(
                self.inner.options.credentials.as_ref(),
                provider,
                cancellation,
            )
            .await
        })
    }

    /// Starts a resolved raw text stream.
    pub fn stream(
        &self,
        model: Model,
        context: Context,
        overrides: ModelsRequestOverrides,
        options: StreamOptions,
    ) -> AssistantStream {
        start_dispatch(
            Arc::clone(&self.inner),
            self.child_cancellation,
            model,
            overrides,
            DispatchRequest::Stream {
                context,
                options,
                lifecycle: None,
            },
        )
    }

    /// Starts a resolved request with isolated payload and response observation.
    pub fn stream_with_lifecycle(
        &self,
        model: Model,
        context: Context,
        overrides: ModelsRequestOverrides,
        options: StreamOptions,
        lifecycle: RequestLifecycleOptions,
    ) -> ObservedAssistantStream {
        let (lifecycle, metadata) = RequestLifecycleInvocation::new(lifecycle);
        let stream = start_dispatch(
            Arc::clone(&self.inner),
            self.child_cancellation,
            model,
            overrides,
            DispatchRequest::Stream {
                context,
                options,
                lifecycle: Some(lifecycle),
            },
        );
        ObservedAssistantStream { stream, metadata }
    }

    /// Completes a resolved raw text request.
    pub fn complete(
        &self,
        model: Model,
        context: Context,
        overrides: ModelsRequestOverrides,
        options: StreamOptions,
    ) -> BoxFuture<'static, AssistantMessage> {
        let stream = self.stream(model, context, overrides, options);
        Box::pin(async move { stream.result().await })
    }

    /// Starts a resolved provider-neutral simple stream.
    pub fn stream_simple(
        &self,
        model: Model,
        context: Context,
        overrides: ModelsRequestOverrides,
        options: SimpleStreamOptions,
    ) -> AssistantStream {
        start_dispatch(
            Arc::clone(&self.inner),
            self.child_cancellation,
            model,
            overrides,
            DispatchRequest::Simple {
                context,
                options,
                lifecycle: None,
            },
        )
    }

    /// Starts a resolved request with isolated payload and response observation.
    pub fn stream_simple_with_lifecycle(
        &self,
        model: Model,
        context: Context,
        overrides: ModelsRequestOverrides,
        options: SimpleStreamOptions,
        lifecycle: RequestLifecycleOptions,
    ) -> ObservedAssistantStream {
        let (lifecycle, metadata) = RequestLifecycleInvocation::new(lifecycle);
        let stream = start_dispatch(
            Arc::clone(&self.inner),
            self.child_cancellation,
            model,
            overrides,
            DispatchRequest::Simple {
                context,
                options,
                lifecycle: Some(lifecycle),
            },
        );
        ObservedAssistantStream { stream, metadata }
    }

    /// Completes a resolved provider-neutral simple request.
    pub fn complete_simple(
        &self,
        model: Model,
        context: Context,
        overrides: ModelsRequestOverrides,
        options: SimpleStreamOptions,
    ) -> BoxFuture<'static, AssistantMessage> {
        let stream = self.stream_simple(model, context, overrides, options);
        Box::pin(async move { stream.result().await })
    }

    /// Fetches and completes one deferred response.
    pub fn fetch_deferred(
        &self,
        model: Model,
        handle: DeferredHandle,
        overrides: ModelsRequestOverrides,
        options: DeferredFetchOptions,
    ) -> BoxFuture<'static, AssistantMessage> {
        let stream = start_dispatch(
            Arc::clone(&self.inner),
            self.child_cancellation,
            model,
            overrides,
            DispatchRequest::Fetch {
                handle,
                options,
                lifecycle: None,
            },
        );
        Box::pin(async move { stream.result().await })
    }

    /// Best-effort cancels one deferred response.
    pub fn cancel_deferred(
        &self,
        model: Model,
        handle: DeferredHandle,
        overrides: ModelsRequestOverrides,
        options: DeferredCancelOptions,
    ) -> BoxFuture<'static, Result<(), AuthResolutionError>> {
        cancel_deferred_selected(
            Arc::clone(&self.inner),
            model,
            handle,
            overrides,
            options,
            None,
        )
    }
}

impl fmt::Debug for ModelsRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ModelsRegistry([PROCESS-LOCAL])")
    }
}

async fn join_all<T>(mut futures: Vec<BoxFuture<'static, T>>) -> Vec<T> {
    let mut futures = futures.drain(..).map(Some).collect::<Vec<_>>();
    let mut values = (0..futures.len()).map(|_| None).collect::<Vec<_>>();
    poll_fn(|context| {
        let mut pending = 0;
        for (index, future) in futures.iter_mut().enumerate() {
            let Some(active) = future.as_mut() else {
                continue;
            };
            match active.as_mut().poll(context) {
                Poll::Ready(value) => {
                    values[index] = Some(value);
                    *future = None;
                }
                Poll::Pending => pending += 1,
            }
        }
        if pending == 0 {
            Poll::Ready(
                values
                    .iter_mut()
                    .map(|value| value.take().expect("completed future has a value"))
                    .collect(),
            )
        } else {
            Poll::Pending
        }
    })
    .await
}

async fn caller_first<T>(
    cancellation: &CancellationToken,
    mut future: BoxFuture<'_, T>,
) -> Result<T, AuthResolutionError> {
    let mut cancelled = Box::pin(cancellation.cancelled());
    let result = poll_fn(|context| {
        if cancelled.as_mut().poll(context).is_ready() || cancellation.is_cancelled() {
            return Poll::Ready(Err(AuthResolutionError::Cancelled));
        }
        match future.as_mut().poll(context) {
            Poll::Ready(_) if cancellation.is_cancelled() => {
                Poll::Ready(Err(AuthResolutionError::Cancelled))
            }
            Poll::Ready(value) => Poll::Ready(Ok(value)),
            Poll::Pending => Poll::Pending,
        }
    })
    .await;
    drop(future);
    result
}

async fn poll_once_then_shield<T>(
    cancellation: &CancellationToken,
    mut future: BoxFuture<'_, T>,
) -> Result<T, AuthResolutionError> {
    if cancellation.is_cancelled() {
        return Err(AuthResolutionError::Cancelled);
    }
    let first = poll_fn(|context| match future.as_mut().poll(context) {
        Poll::Ready(value) => Poll::Ready(Some(value)),
        Poll::Pending => Poll::Ready(None),
    })
    .await;
    match first {
        Some(value) => Ok(value),
        None => Ok(future.await),
    }
}

async fn poll_once_then_shield_phase<T>(
    caller: &CancellationToken,
    generation: &CancellationToken,
    mut future: BoxFuture<'_, T>,
) -> Result<T, PhaseCancelled> {
    if caller.is_cancelled() {
        return Err(PhaseCancelled::Caller);
    }
    if generation.is_cancelled() {
        return Err(PhaseCancelled::Generation);
    }
    let first = poll_fn(|context| match future.as_mut().poll(context) {
        Poll::Ready(value) => Poll::Ready(Some(value)),
        Poll::Pending => Poll::Ready(None),
    })
    .await;
    let value = match first {
        Some(value) => value,
        None => future.await,
    };
    if caller.is_cancelled() {
        Err(PhaseCancelled::Caller)
    } else if generation.is_cancelled() {
        Err(PhaseCancelled::Generation)
    } else {
        Ok(value)
    }
}

async fn read_auth_credential(
    inner: &RegistryInner,
    provider: &ProviderId,
    cancellation: &CancellationToken,
) -> Result<Option<VersionedCredential>, AuthResolutionError> {
    caller_first(
        cancellation,
        inner.options.credentials.read(provider, cancellation),
    )
    .await?
    .map_err(map_credential_error)
}

async fn check_provider_auth(
    inner: &RegistryInner,
    record: &ProviderRecord,
    stored: Option<&VersionedCredential>,
    cancellation: &CancellationToken,
) -> Result<Option<AuthCheck>, AuthResolutionError> {
    if let Some(Credential::OAuth(_)) = stored.map(|stored| &stored.credential) {
        return Ok(record.provider.auth().oauth().map(|_| AuthCheck {
            source: Some("OAuth".to_owned()),
            auth_type: AuthType::OAuth,
        }));
    }
    let Some(api_key) = record.provider.auth().api_key() else {
        return Ok(None);
    };
    let credential = stored.and_then(|stored| match &stored.credential {
        Credential::ApiKey(credential) => Some(credential),
        Credential::OAuth(_) => None,
    });
    if let Some(check) = api_key.check(ApiKeyAuthInput {
        context: inner.options.auth_context.as_ref(),
        credential,
        cancellation,
    }) {
        return caller_first(cancellation, check).await?.map_err(|error| {
            AuthResolutionError::Models(ModelsError::with_source(ModelsErrorCode::Auth, error))
        });
    }
    let resolution = resolve_provider_auth(
        &record.id,
        record.provider.auth(),
        inner.options.credentials.as_ref(),
        inner.options.auth_context.as_ref(),
        inner.options.clock.as_ref(),
        AuthResolutionOverrides::default(),
        cancellation,
    )
    .await?;
    Ok(resolution.map(|result| AuthCheck {
        source: result.source,
        auth_type: AuthType::ApiKey,
    }))
}

fn models_for_record(record: &ProviderRecord) -> Result<Vec<Model>, ModelsError> {
    let baseline = record.provider.models()?;
    let dynamic = {
        record
            .owner
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .dynamic
            .clone()
    };
    let mut models = baseline.to_vec();
    for model in dynamic.iter() {
        if let Some(index) = models.iter().position(|entry| entry.id == model.id) {
            models[index] = model.clone();
        } else {
            models.push(model.clone());
        }
    }
    Ok(models)
}

async fn available_provider(
    inner: Arc<RegistryInner>,
    record: Arc<ProviderRecord>,
    cancellation: CancellationToken,
) -> Result<Vec<Model>, AuthResolutionError> {
    if record.provider.auth().is_no_auth() {
        if cancellation.is_cancelled() {
            return Err(AuthResolutionError::Cancelled);
        }
        let models = models_for_record(&record).map_err(AuthResolutionError::Models)?;
        return Ok(record.provider.filter_models(&models, None));
    }
    let credential = read_auth_credential(&inner, &record.id, &cancellation).await?;
    let auth = check_provider_auth(&inner, &record, credential.as_ref(), &cancellation).await?;
    if auth.is_none() {
        return Ok(Vec::new());
    }
    let models = models_for_record(&record).map_err(AuthResolutionError::Models)?;
    Ok(record.provider.filter_models(
        &models,
        credential.as_ref().map(|stored| &stored.credential),
    ))
}

async fn persist_login_candidate(
    store: &dyn CredentialStore,
    provider: &ProviderId,
    candidate: Credential,
    cancellation: &CancellationToken,
) -> Result<(), AuthResolutionError> {
    let lease =
        match caller_first(cancellation, store.begin_mutation(provider, cancellation)).await? {
            Ok(lease) => lease,
            Err(error) if error.code() == CredentialStoreErrorCode::OutcomeUnknown => {
                poll_once_then_shield(
                    cancellation,
                    store.reconcile(
                        provider,
                        CredentialReconciliation::Replace(candidate),
                        cancellation,
                    ),
                )
                .await?
                .map_err(map_credential_error)?;
                return Ok(());
            }
            Err(error) => return Err(map_credential_error(error)),
        };
    let committed = poll_once_then_shield(
        cancellation,
        lease.commit(CredentialMutationValue::Set(candidate.clone())),
    )
    .await?;
    match committed {
        Ok(_) => Ok(()),
        Err(error) if error.code() == CredentialStoreErrorCode::OutcomeUnknown => {
            store
                .reconcile(
                    provider,
                    CredentialReconciliation::Replace(candidate),
                    &CancellationToken::new(),
                )
                .await
                .map_err(map_credential_error)?;
            Ok(())
        }
        Err(error) => Err(map_credential_error(error)),
    }
}

async fn persist_logout(
    store: &dyn CredentialStore,
    provider: &ProviderId,
    cancellation: &CancellationToken,
) -> Result<(), AuthResolutionError> {
    if cancellation.is_cancelled() {
        return Err(AuthResolutionError::Cancelled);
    }
    let lease =
        match caller_first(cancellation, store.begin_mutation(provider, cancellation)).await? {
            Ok(lease) => lease,
            Err(error) if error.code() == CredentialStoreErrorCode::OutcomeUnknown => {
                poll_once_then_shield(
                    cancellation,
                    store.reconcile(provider, CredentialReconciliation::Delete, cancellation),
                )
                .await?
                .map_err(map_credential_error)?;
                return Ok(());
            }
            Err(error) => return Err(map_credential_error(error)),
        };
    let committed =
        poll_once_then_shield(cancellation, lease.commit(CredentialMutationValue::Delete)).await?;
    match committed {
        Ok(_) => Ok(()),
        Err(error) if error.code() == CredentialStoreErrorCode::OutcomeUnknown => {
            store
                .reconcile(
                    provider,
                    CredentialReconciliation::Delete,
                    &CancellationToken::new(),
                )
                .await
                .map_err(map_credential_error)?;
            Ok(())
        }
        Err(error) => Err(map_credential_error(error)),
    }
}

fn merge_headers(base: ProviderHeaders, later: ProviderHeaders) -> ProviderHeaders {
    let mut merged: Vec<_> = base
        .iter_for_dispatch()
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect();
    for (name, value) in later.iter_for_dispatch() {
        let normalized = name.to_ascii_lowercase();
        merged.retain(|(existing, _)| existing.to_ascii_lowercase() != normalized);
        merged.push((name.clone(), value.clone()));
    }
    ProviderHeaders::from_ordered_entries(merged)
}

#[cfg(test)]
mod ordered_header_tests {
    use super::*;

    #[test]
    fn registry_merge_matches_pinned_order_spelling_and_null_projection() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../conformance/provider-traces/provider-header-order/trace.json"
        ))
        .unwrap();
        for case in fixture["merges"].as_array().unwrap() {
            let base = serde_json::from_str(case["base"].as_str().unwrap()).unwrap();
            let later = serde_json::from_str(case["later"].as_str().unwrap()).unwrap();
            let result = merge_headers(base, later);
            let expected: Vec<(String, Option<String>)> =
                serde_json::from_value(case["entries"].clone()).unwrap();
            assert_eq!(
                result
                    .iter_for_dispatch()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect::<Vec<_>>(),
                expected
            );
            assert_eq!(
                serde_json::to_string(&result).unwrap(),
                case["serialized"].as_str().unwrap()
            );
        }
    }
}

struct AssembledRequest {
    record: Arc<ProviderRecord>,
    model: Model,
    endpoint: RequestEndpoint,
    auth: ResolvedAuth,
}

enum SetupAssemblyError {
    UnknownProvider,
    Authentication(AuthResolutionError),
    Endpoint,
}

impl SetupAssemblyError {
    fn into_auth_error(self) -> AuthResolutionError {
        match self {
            Self::UnknownProvider => {
                AuthResolutionError::Models(ModelsError::new(ModelsErrorCode::Provider))
            }
            Self::Authentication(error) => error,
            Self::Endpoint => {
                AuthResolutionError::Models(ModelsError::new(ModelsErrorCode::ModelValidation))
            }
        }
    }
}

fn find_record(inner: &RegistryInner, provider: &ProviderId) -> Option<Arc<ProviderRecord>> {
    let snapshot = {
        inner
            .membership
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .committed
            .clone()
    };
    snapshot
        .iter()
        .find(|record| &record.id == provider)
        .cloned()
}

fn cancel_deferred_selected(
    inner: Arc<RegistryInner>,
    model: Model,
    handle: DeferredHandle,
    overrides: ModelsRequestOverrides,
    options: DeferredCancelOptions,
    captured: Option<Arc<ProviderRecord>>,
) -> BoxFuture<'static, Result<(), AuthResolutionError>> {
    Box::pin(async move {
        let cancellation = options.runtime.cancellation.clone();
        let assembled = match captured {
            Some(record) => {
                assemble_request_from_record(&inner, record, model, overrides, &cancellation).await
            }
            None => assemble_request(&inner, model, overrides, &cancellation).await,
        }
        .map_err(SetupAssemblyError::into_auth_error)?;
        let Some(adapter) = assembled.record.provider.adapter(&assembled.model.api) else {
            return Err(AuthResolutionError::Models(ModelsError::new(
                ModelsErrorCode::Provider,
            )));
        };
        let request = DeferredCancelRequest {
            resolved: ResolvedRequest {
                model: assembled.model,
                endpoint: assembled.endpoint,
            },
            handle,
            auth: assembled.auth,
            options,
        };
        caller_first(&cancellation, adapter.cancel_deferred(request))
            .await?
            .map_err(AuthResolutionError::Models)
    })
}

async fn assemble_request(
    inner: &RegistryInner,
    model: Model,
    overrides: ModelsRequestOverrides,
    cancellation: &CancellationToken,
) -> Result<AssembledRequest, SetupAssemblyError> {
    let Some(record) = find_record(inner, &model.provider) else {
        return Err(SetupAssemblyError::UnknownProvider);
    };
    assemble_request_from_record(inner, record, model, overrides, cancellation).await
}

async fn assemble_request_from_record(
    inner: &RegistryInner,
    record: Arc<ProviderRecord>,
    model: Model,
    overrides: ModelsRequestOverrides,
    cancellation: &CancellationToken,
) -> Result<AssembledRequest, SetupAssemblyError> {
    if record.provider.auth().is_no_auth() {
        if cancellation.is_cancelled() {
            return Err(SetupAssemblyError::Authentication(
                AuthResolutionError::Cancelled,
            ));
        }
        if overrides.auth.api_key.is_some()
            || overrides.auth.env.is_some()
            || overrides.auth.min_oauth_validity_ms.is_some()
            || model.headers.is_some()
            || !overrides.headers.as_map().is_empty()
        {
            return Err(SetupAssemblyError::Authentication(
                AuthResolutionError::Models(ModelsError::new(ModelsErrorCode::Auth)),
            ));
        }
        let endpoint = match &model.base_url {
            ModelBaseUrl::Public(base_url) => {
                RequestEndpoint::new_resolved(base_url.as_str().to_owned())
                    .map_err(|_| SetupAssemblyError::Endpoint)?
            }
        };
        return Ok(AssembledRequest {
            record,
            model,
            endpoint,
            auth: ResolvedAuth::default(),
        });
    }
    let explicit_api_key = overrides.auth.api_key.clone();
    let explicit_env = overrides.auth.env.clone();
    let resolution = resolve_provider_auth(
        &record.id,
        record.provider.auth(),
        inner.options.credentials.as_ref(),
        inner.options.auth_context.as_ref(),
        inner.options.clock.as_ref(),
        overrides.auth,
        cancellation,
    )
    .await
    .map_err(SetupAssemblyError::Authentication)?
    .ok_or_else(|| {
        SetupAssemblyError::Authentication(AuthResolutionError::Models(ModelsError::new(
            ModelsErrorCode::Auth,
        )))
    })?;
    let AuthResult {
        auth: ModelAuth {
            api_key,
            headers,
            base_url,
        },
        env,
        source: _,
    } = resolution;

    let mut resolved_headers = headers.unwrap_or_default();
    if let Some(model_headers) = &model.headers {
        resolved_headers = merge_headers(
            resolved_headers,
            ProviderHeaders::new(
                model_headers
                    .as_map()
                    .iter()
                    .map(|(name, value)| (name.clone(), Some(value.clone())))
                    .collect(),
            ),
        );
    }
    resolved_headers = merge_headers(resolved_headers, overrides.headers);

    let mut resolved_env = env.map_or_else(BTreeMap::new, |env| env.as_map().clone());
    if let Some(explicit_env) = explicit_env {
        resolved_env.extend(
            explicit_env
                .as_map()
                .iter()
                .map(|(name, value)| (name.clone(), value.clone())),
        );
    }

    let endpoint = if let Some(base_url) = base_url {
        RequestEndpoint::new_resolved(base_url).map_err(|_| SetupAssemblyError::Endpoint)?
    } else {
        match &model.base_url {
            ModelBaseUrl::Public(base_url) => {
                RequestEndpoint::new_resolved(base_url.as_str().to_owned())
                    .map_err(|_| SetupAssemblyError::Endpoint)?
            }
        }
    };
    Ok(AssembledRequest {
        record,
        model,
        endpoint,
        auth: ResolvedAuth {
            api_key: explicit_api_key.or(api_key),
            headers: resolved_headers,
            env: crate::options::ProviderEnv::new(resolved_env),
        },
    })
}

enum DispatchRequest {
    Stream {
        context: Context,
        options: StreamOptions,
        lifecycle: Option<RequestLifecycleInvocation>,
    },
    Simple {
        context: Context,
        options: SimpleStreamOptions,
        lifecycle: Option<RequestLifecycleInvocation>,
    },
    Fetch {
        handle: DeferredHandle,
        options: DeferredFetchOptions,
        lifecycle: Option<RequestLifecycleInvocation>,
    },
}

impl DispatchRequest {
    fn cancellation(&self) -> CancellationToken {
        match self {
            Self::Stream { options, .. } => options.runtime.cancellation.clone(),
            Self::Simple { options, .. } => options.runtime.cancellation.clone(),
            Self::Fetch { options, .. } => options.runtime.cancellation.clone(),
        }
    }

    fn clock(&self) -> Arc<dyn Clock> {
        match self {
            Self::Stream { options, .. } => Arc::clone(&options.runtime.clock),
            Self::Simple { options, .. } => Arc::clone(&options.runtime.clock),
            Self::Fetch { options, .. } => Arc::clone(&options.runtime.clock),
        }
    }

    fn spawner(&self) -> Arc<dyn TaskSpawner> {
        match self {
            Self::Stream { options, .. } => Arc::clone(&options.runtime.spawner),
            Self::Simple { options, .. } => Arc::clone(&options.runtime.spawner),
            Self::Fetch { options, .. } => Arc::clone(&options.runtime.spawner),
        }
    }
}

fn start_dispatch(
    inner: Arc<RegistryInner>,
    child_cancellation: bool,
    model: Model,
    overrides: ModelsRequestOverrides,
    request: DispatchRequest,
) -> AssistantStream {
    start_dispatch_selected(inner, child_cancellation, model, overrides, request, None)
}

fn start_dispatch_selected(
    inner: Arc<RegistryInner>,
    child_cancellation: bool,
    model: Model,
    overrides: ModelsRequestOverrides,
    request: DispatchRequest,
    captured: Option<Arc<ProviderRecord>>,
) -> AssistantStream {
    let mut request = request;
    let identity = match &mut request {
        DispatchRequest::Stream { options, .. } => Some(&mut options.wire.identity),
        DispatchRequest::Simple { options, .. } => Some(&mut options.wire.stream.identity),
        DispatchRequest::Fetch { .. } => None,
    };
    if let Some(identity) = identity
        && (inner.options.identity.is_some() || identity.is_some())
    {
        *identity = Some(crate::merge_client_identity(
            inner.options.identity.as_ref(),
            identity.as_ref(),
        ));
    }
    let cancellation = request.cancellation();
    let outer_cancellation = if child_cancellation {
        CancellationToken::new()
    } else {
        cancellation.clone()
    };
    let (stream, producer) = create_assistant_message_event_stream(AssistantStreamInit {
        api: model.api.clone(),
        provider: model.provider.clone(),
        model: model.id.clone(),
        clock: request.clock(),
        cancellation: outer_cancellation.clone(),
    });
    let spawner = request.spawner();
    let task_producer = producer.clone();
    let task = Box::pin(async move {
        if child_cancellation && cancellation.is_cancelled() {
            outer_cancellation.cancel();
            return;
        }
        let assembly = match captured {
            Some(record) => {
                assemble_request_from_record(&inner, record, model, overrides, &cancellation).await
            }
            None => assemble_request(&inner, model, overrides, &cancellation).await,
        };
        let assembled = match assembly {
            Ok(assembled) => assembled,
            Err(SetupAssemblyError::UnknownProvider) => {
                let _ = task_producer.fail_setup(StreamSetupFailure::UnknownProvider);
                return;
            }
            Err(SetupAssemblyError::Authentication(AuthResolutionError::Cancelled)) => {
                outer_cancellation.cancel();
                return;
            }
            Err(SetupAssemblyError::Authentication(_)) => {
                let _ = task_producer.fail_setup(StreamSetupFailure::Authentication);
                return;
            }
            Err(SetupAssemblyError::Endpoint) => {
                let _ = task_producer.fail_setup(StreamSetupFailure::Adapter);
                return;
            }
        };
        let adapter = assembled.record.provider.adapter(&assembled.model.api);
        if child_cancellation && cancellation.is_cancelled() {
            outer_cancellation.cancel();
            return;
        }
        let Some(adapter) = adapter else {
            let _ = task_producer.fail_setup(StreamSetupFailure::Adapter);
            return;
        };
        let source = match request {
            DispatchRequest::Stream {
                context,
                options,
                lifecycle,
            } => {
                let request = TextStreamRequest {
                    resolved: ResolvedRequest {
                        model: assembled.model,
                        endpoint: assembled.endpoint,
                    },
                    context: crate::normalize_context(context),
                    auth: assembled.auth,
                    options,
                };
                match lifecycle {
                    None => adapter.stream(request),
                    Some(lifecycle) => {
                        let Some(capability) = adapter.lifecycle_adapter() else {
                            let _ = task_producer.fail_setup(StreamSetupFailure::Adapter);
                            return;
                        };
                        capability.stream_with_lifecycle(request, lifecycle)
                    }
                }
            }
            DispatchRequest::Simple {
                context,
                options,
                lifecycle,
            } => {
                let request = SimpleStreamRequest {
                    resolved: ResolvedRequest {
                        model: assembled.model,
                        endpoint: assembled.endpoint,
                    },
                    context: crate::normalize_context(context),
                    auth: assembled.auth,
                    options,
                };
                match lifecycle {
                    None => adapter.stream_simple(request),
                    Some(lifecycle) => {
                        let Some(capability) = adapter.lifecycle_adapter() else {
                            let _ = task_producer.fail_setup(StreamSetupFailure::Adapter);
                            return;
                        };
                        capability.stream_simple_with_lifecycle(request, lifecycle)
                    }
                }
            }
            DispatchRequest::Fetch {
                handle,
                options,
                lifecycle,
            } => {
                let request = DeferredFetchRequest {
                    resolved: ResolvedRequest {
                        model: assembled.model,
                        endpoint: assembled.endpoint,
                    },
                    handle,
                    auth: assembled.auth,
                    options,
                };
                let result = match lifecycle {
                    None => adapter.fetch_deferred(request),
                    Some(lifecycle) => {
                        let Some(capability) = adapter
                            .lifecycle_adapter()
                            .and_then(|capability| capability.deferred_lifecycle())
                        else {
                            let _ = task_producer.fail_setup(StreamSetupFailure::Adapter);
                            return;
                        };
                        capability.fetch_deferred_with_lifecycle(request, lifecycle)
                    }
                };
                match result {
                    Ok(stream) => stream,
                    Err(_) => {
                        if child_cancellation && cancellation.is_cancelled() {
                            outer_cancellation.cancel();
                        } else {
                            let _ = task_producer.fail_setup(StreamSetupFailure::Adapter);
                        }
                        return;
                    }
                }
            }
        };
        // A returned child owns even cancellation triggered inside adapter setup.
        // Its latest usage may never have appeared in a progress event.
        let pump_cancellation = if child_cancellation {
            &outer_cancellation
        } else {
            &cancellation
        };
        pump_stream(source, task_producer, pump_cancellation).await;
    });
    if spawner.spawn(task).is_err() {
        let _ = producer.fail_setup(StreamSetupFailure::Spawn);
    }
    stream
}

async fn pump_stream(
    mut source: AssistantStream,
    producer: crate::stream::AssistantStreamProducer,
    cancellation: &CancellationToken,
) {
    loop {
        let mut cancelled = Box::pin(cancellation.cancelled());
        let mut next = Box::pin(source.next());
        let event = poll_fn(|context| {
            if cancelled.as_mut().poll(context).is_ready() || cancellation.is_cancelled() {
                return Poll::Ready(None);
            }
            match next.as_mut().poll(context) {
                Poll::Ready(event) => Poll::Ready(Some(event)),
                Poll::Pending => Poll::Pending,
            }
        })
        .await;
        drop(next);
        let Some(event) = event else {
            return;
        };
        let Some(event) = event else {
            return;
        };
        let terminal = matches!(
            event,
            AssistantMessageEvent::Done { .. } | AssistantMessageEvent::Error { .. }
        );
        if producer.push(event).is_err() || terminal {
            return;
        }
    }
}

#[derive(Clone)]
enum MutationIntent {
    Set(ProviderId),
    Delete(ProviderId),
    Clear,
}

fn admit_mutation(
    inner: &RegistryInner,
    intent: MutationIntent,
    fresh_owner: Arc<ProviderOwner>,
    command_tail: Completion<()>,
) -> Result<(MutationAssignment, Vec<CancellationToken>), ()> {
    let mut membership = inner
        .membership
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    let ticket = allocate_checked(&mut membership.next_ticket)?;
    let previous_membership = membership.tail.clone();
    let mut owner_tails = Vec::new();
    let mut owners = Vec::new();
    let mut cancelled = Vec::new();

    match intent {
        MutationIntent::Set(id) => {
            let existing = membership
                .projected
                .iter()
                .position(|record| record.id == id);
            let owner = existing
                .map(|index| Arc::clone(&membership.projected[index].owner))
                .unwrap_or(fresh_owner);
            {
                let mut state = owner
                    .state
                    .lock()
                    .unwrap_or_else(|poison| poison.into_inner());
                if let Some(active) = state.active.take() {
                    cancelled.push(active.cancellation);
                }
                state.current_epoch = Some(ticket);
                state.reservations.clear();
                owner_tails.push(state.tail.clone());
            }
            if let Some(index) = existing {
                membership.projected[index] = ProjectedRecord {
                    id,
                    owner: Arc::clone(&owner),
                    epoch: ticket,
                };
            } else {
                membership.projected.push(ProjectedRecord {
                    id,
                    owner: Arc::clone(&owner),
                    epoch: ticket,
                });
            }
            owners.push(owner);
        }
        MutationIntent::Delete(id) => {
            if let Some(index) = membership
                .projected
                .iter()
                .position(|record| record.id == id)
            {
                let record = membership.projected.remove(index);
                {
                    let mut state = record
                        .owner
                        .state
                        .lock()
                        .unwrap_or_else(|poison| poison.into_inner());
                    if let Some(active) = state.active.take() {
                        cancelled.push(active.cancellation);
                    }
                    state.current_epoch = None;
                    state.reservations.clear();
                    owner_tails.push(state.tail.clone());
                }
                owners.push(record.owner);
            }
        }
        MutationIntent::Clear => {
            let projected = std::mem::take(&mut membership.projected);
            for record in projected {
                {
                    let mut state = record
                        .owner
                        .state
                        .lock()
                        .unwrap_or_else(|poison| poison.into_inner());
                    if let Some(active) = state.active.take() {
                        cancelled.push(active.cancellation);
                    }
                    state.current_epoch = None;
                    state.reservations.clear();
                    owner_tails.push(state.tail.clone());
                }
                owners.push(record.owner);
            }
        }
    }

    membership.tail = command_tail;
    drop(membership);
    Ok((
        MutationAssignment {
            ticket,
            previous_membership,
            owner_tails,
            owners,
        },
        cancelled,
    ))
}

async fn run_membership_command(
    inner: Arc<RegistryInner>,
    gate: Completion<MutationGate>,
    done: Completion<MutationResult>,
    command_tail: Completion<()>,
    kind: MutationKind,
) {
    let MutationGate::Run(assignment) = gate.wait().await else {
        return;
    };
    assignment.previous_membership.wait().await;
    for tail in &assignment.owner_tails {
        tail.wait().await;
    }

    let current = {
        inner
            .membership
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .committed
            .clone()
    };
    let atomic_set = matches!(&kind, MutationKind::Set(_));
    let next: Arc<[Arc<ProviderRecord>]> = match kind {
        MutationKind::Set(provider) => {
            let owner = Arc::clone(
                assignment
                    .owners
                    .first()
                    .expect("set admission records one owner"),
            );
            let id = provider.id().clone();
            let record = Arc::new(ProviderRecord {
                id: id.clone(),
                provider,
                owner,
                epoch: assignment.ticket,
            });
            let mut values = current.iter().cloned().collect::<Vec<_>>();
            if let Some(index) = values.iter().position(|entry| entry.id == id) {
                values[index] = record;
            } else {
                values.push(record);
            }
            values.into()
        }
        MutationKind::Delete(id) => current
            .iter()
            .filter(|entry| entry.id != id)
            .cloned()
            .collect::<Vec<_>>()
            .into(),
        MutationKind::Clear => Arc::from([]),
    };

    let set_owner = atomic_set.then(|| {
        Arc::clone(
            assignment
                .owners
                .first()
                .expect("set admission records one owner"),
        )
    });
    let (old, old_dynamic) = {
        let mut membership = inner
            .membership
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let old_dynamic = set_owner.as_ref().map(|owner| {
            let mut state = owner
                .state
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            std::mem::replace(&mut state.dynamic, Arc::from([]))
        });
        (
            std::mem::replace(&mut membership.committed, next),
            old_dynamic,
        )
    };
    drop(old);
    drop(old_dynamic);
    if !atomic_set {
        for owner in assignment.owners {
            let old_dynamic = {
                let mut state = owner
                    .state
                    .lock()
                    .unwrap_or_else(|poison| poison.into_inner());
                std::mem::replace(&mut state.dynamic, Arc::from([]))
            };
            drop(old_dynamic);
        }
    }
    command_tail.complete(());
    done.complete(MutationResult::Success);
}

fn authority_is_current(
    inner: &RegistryInner,
    provider: &ProviderId,
    owner: &ProviderOwner,
    epoch: u128,
    generation: u128,
) -> bool {
    let membership = inner
        .membership
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    let committed = membership.committed.iter().any(|record| {
        &record.id == provider
            && record.epoch == epoch
            && std::ptr::eq(record.owner.as_ref(), owner)
    });
    let projected = membership.projected.iter().any(|record| {
        &record.id == provider
            && record.epoch == epoch
            && std::ptr::eq(record.owner.as_ref(), owner)
    });
    if !committed || !projected {
        return false;
    }
    let owner = owner
        .state
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    publication_authority_matches(&owner, epoch, generation)
}

enum BeginGenerationError {
    Superseded,
    Exhausted,
}

fn begin_generation(
    inner: &RegistryInner,
    record: &ProviderRecord,
) -> Result<(GenerationLease, Completion<()>), BeginGenerationError> {
    let membership = inner
        .membership
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    let committed = membership.committed.iter().any(|candidate| {
        std::ptr::eq(candidate.as_ref(), record)
            && candidate.epoch == record.epoch
            && candidate.id == record.id
    });
    let projected = membership.projected.iter().any(|candidate| {
        candidate.id == record.id
            && candidate.epoch == record.epoch
            && std::ptr::eq(candidate.owner.as_ref(), record.owner.as_ref())
    });
    if !committed || !projected {
        return Err(BeginGenerationError::Superseded);
    }
    let mut owner = record
        .owner
        .state
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    if owner.current_epoch != Some(record.epoch) {
        return Err(BeginGenerationError::Superseded);
    }
    let generation = allocate_checked(&mut owner.next_generation)
        .map_err(|()| BeginGenerationError::Exhausted)?;
    let cancellation = CancellationToken::new();
    let displaced = owner.active.replace(GenerationAuthority {
        generation,
        cancellation: cancellation.clone(),
    });
    owner.reservations.clear();
    let tail = owner.tail.clone();
    drop(owner);
    drop(membership);
    if let Some(displaced) = displaced {
        displaced.cancellation.cancel();
    }
    Ok((
        GenerationLease {
            epoch: record.epoch,
            generation,
            cancellation,
            cancel_on_drop: true,
        },
        tail,
    ))
}

async fn race_phase<T>(
    caller: &CancellationToken,
    generation: &CancellationToken,
    mut future: BoxFuture<'_, T>,
) -> Result<T, PhaseCancelled> {
    let mut caller_wait = Box::pin(caller.cancelled());
    let mut generation_wait = Box::pin(generation.cancelled());
    let result = poll_fn(|context| {
        if caller_wait.as_mut().poll(context).is_ready() || caller.is_cancelled() {
            return Poll::Ready(Err(PhaseCancelled::Caller));
        }
        if generation_wait.as_mut().poll(context).is_ready() || generation.is_cancelled() {
            return Poll::Ready(Err(PhaseCancelled::Generation));
        }
        match future.as_mut().poll(context) {
            Poll::Ready(_value) if caller.is_cancelled() => {
                Poll::Ready(Err(PhaseCancelled::Caller))
            }
            Poll::Ready(_) if generation.is_cancelled() => {
                Poll::Ready(Err(PhaseCancelled::Generation))
            }
            Poll::Ready(value) => Poll::Ready(Ok(value)),
            Poll::Pending => Poll::Pending,
        }
    })
    .await;
    drop(future);
    result
}

async fn race_provider_phase<T>(
    caller: &CancellationToken,
    generation: &CancellationToken,
    mut future: BoxFuture<'_, T>,
) -> Result<T, PhaseCancelled> {
    let mut caller_wait = Box::pin(caller.cancelled());
    let mut generation_wait = Box::pin(generation.cancelled());
    let result = poll_fn(|context| {
        if caller_wait.as_mut().poll(context).is_ready() || caller.is_cancelled() {
            return Poll::Ready(Err(PhaseCancelled::Caller));
        }
        if generation_wait.as_mut().poll(context).is_ready() || generation.is_cancelled() {
            return Poll::Ready(Err(PhaseCancelled::Generation));
        }
        match future.as_mut().poll(context) {
            Poll::Ready(_value) if caller.is_cancelled() => {
                Poll::Ready(Err(PhaseCancelled::Caller))
            }
            Poll::Ready(_) if generation.is_cancelled() => {
                Poll::Ready(Err(PhaseCancelled::Generation))
            }
            Poll::Ready(value) => Poll::Ready(Ok(value)),
            Poll::Pending => Poll::Pending,
        }
    })
    .await;
    drop(future);
    result
}

fn map_phase_cancel(error: PhaseCancelled) -> RefreshProviderResult {
    match error {
        PhaseCancelled::Caller => RefreshProviderResult::CallerCancelled,
        PhaseCancelled::Generation => RefreshProviderResult::Superseded,
    }
}

fn map_credential_error(error: CredentialStoreError) -> AuthResolutionError {
    match error.code() {
        CredentialStoreErrorCode::Cancelled => AuthResolutionError::Cancelled,
        CredentialStoreErrorCode::OutcomeUnknown => AuthResolutionError::OutcomeUnknown,
        CredentialStoreErrorCode::Failed
        | CredentialStoreErrorCode::RevisionExhausted
        | CredentialStoreErrorCode::ReconciliationNotRequired => {
            AuthResolutionError::Models(ModelsError::with_source(ModelsErrorCode::Auth, error))
        }
    }
}

async fn refresh_store_snapshot(
    inner: &RegistryInner,
    provider: &ProviderId,
    caller: &CancellationToken,
    generation: &CancellationToken,
) -> Result<Option<ModelsStoreEntry>, RefreshProviderResult> {
    let linked = CancellationToken::linked(&[caller.clone(), generation.clone()]);
    let raw = race_phase(
        caller,
        generation,
        inner.options.models_store.read(provider, &linked),
    )
    .await
    .map_err(map_phase_cancel)?
    .map_err(|error| {
        RefreshProviderResult::Error(AuthResolutionError::Models(ModelsError::with_source(
            ModelsErrorCode::ModelSource,
            error,
        )))
    })?;
    raw.map(|entry| entry.into_store(&inner.options.model_header_policy))
        .transpose()
        .map_err(|error| {
            RefreshProviderResult::Error(AuthResolutionError::Models(ModelsError::with_source(
                ModelsErrorCode::ModelValidation,
                error,
            )))
        })
}

#[allow(clippy::too_many_arguments)] // The frozen phase context keeps each authority input explicit.
async fn run_refresh_phase(
    inner: &Arc<RegistryInner>,
    record: &Arc<ProviderRecord>,
    lease: &GenerationLease,
    refresher: &dyn ProviderModelsRefresher,
    credential: Option<&Credential>,
    stored: Option<&ModelsStoreEntry>,
    allow_network: bool,
    force: Option<bool>,
    caller: &CancellationToken,
) -> Result<(), RefreshProviderResult> {
    let linked = CancellationToken::linked(&[caller.clone(), lease.cancellation.clone()]);
    let mut publisher = ModelsPublisher {
        inner: Arc::clone(inner),
        owner: Arc::clone(&record.owner),
        provider: record.id.clone(),
        epoch: lease.epoch,
        generation: lease.generation,
        cancellation: linked.clone(),
    };
    let result = race_provider_phase(
        caller,
        &lease.cancellation,
        refresher.refresh_models(RefreshModelsContext {
            publisher: &mut publisher,
            credential,
            stored,
            allow_network,
            force,
            cancellation: &linked,
        }),
    )
    .await
    .map_err(map_phase_cancel)?;
    result.map_err(|error| RefreshProviderResult::Error(AuthResolutionError::Models(error)))
}

async fn resolve_catalog_credential(
    inner: &RegistryInner,
    record: &ProviderRecord,
    caller: &CancellationToken,
    generation: &CancellationToken,
) -> Result<Option<Credential>, RefreshProviderResult> {
    let linked = CancellationToken::linked(&[caller.clone(), generation.clone()]);
    let stored = race_phase(
        caller,
        generation,
        inner.options.credentials.read(&record.id, &linked),
    )
    .await
    .map_err(map_phase_cancel)?
    .map_err(|error| RefreshProviderResult::Error(map_credential_error(error)))?;

    match stored.map(|stored| stored.credential) {
        Some(Credential::OAuth(stored)) => {
            let Some(oauth) = record.provider.auth().oauth() else {
                return Ok(None);
            };
            if inner.options.clock.now_unix_ms() < stored.credentials.expires.get() {
                return Ok(Some(Credential::OAuth(stored)));
            }
            let linked = CancellationToken::linked(&[caller.clone(), generation.clone()]);
            let lease = race_phase(
                caller,
                generation,
                inner
                    .options
                    .credentials
                    .begin_mutation(&record.id, &linked),
            )
            .await
            .map_err(map_phase_cancel)?
            .map_err(|error| RefreshProviderResult::Error(map_credential_error(error)))?;
            let Some(Credential::OAuth(current)) =
                lease.current().map(|current| current.credential.clone())
            else {
                drop(lease);
                return Ok(None);
            };
            if inner.options.clock.now_unix_ms() < current.credentials.expires.get() {
                drop(lease);
                return Ok(Some(Credential::OAuth(current)));
            }
            let linked = CancellationToken::linked(&[caller.clone(), generation.clone()]);
            let refreshed = race_phase(caller, generation, oauth.refresh(&current, &linked))
                .await
                .map_err(map_phase_cancel)?
                .map_err(|error| {
                    RefreshProviderResult::Error(AuthResolutionError::Models(
                        ModelsError::with_source(ModelsErrorCode::OAuth, error),
                    ))
                })?;
            let committed = poll_once_then_shield_phase(
                caller,
                generation,
                lease.commit(CredentialMutationValue::Set(Credential::OAuth(refreshed))),
            )
            .await
            .map_err(map_phase_cancel)?
            .map_err(|error| RefreshProviderResult::Error(map_credential_error(error)))?;
            Ok(committed.and_then(|value| match value.credential {
                Credential::OAuth(credential) => Some(Credential::OAuth(credential)),
                Credential::ApiKey(_) => None,
            }))
        }
        stored => {
            let Some(api_key) = record.provider.auth().api_key() else {
                return Ok(None);
            };
            let stored = stored.and_then(|credential| match credential {
                Credential::ApiKey(credential) => Some(credential),
                Credential::OAuth(_) => None,
            });
            let linked = CancellationToken::linked(&[caller.clone(), generation.clone()]);
            let resolved = race_phase(
                caller,
                generation,
                api_key.resolve(ApiKeyAuthInput {
                    context: inner.options.auth_context.as_ref(),
                    credential: stored.as_ref(),
                    cancellation: &linked,
                }),
            )
            .await
            .map_err(map_phase_cancel)?
            .map_err(|error| {
                RefreshProviderResult::Error(AuthResolutionError::Models(ModelsError::with_source(
                    ModelsErrorCode::Auth,
                    error,
                )))
            })?;
            Ok(resolved.map(|resolved| {
                Credential::ApiKey(ApiKeyCredential {
                    credential_type: ApiKeyCredentialType::ApiKey,
                    key: resolved.auth.api_key,
                    env: resolved.env,
                })
            }))
        }
    }
}

async fn refresh_provider(
    inner: Arc<RegistryInner>,
    record: Arc<ProviderRecord>,
    allow_network: bool,
    force: Option<bool>,
    caller: CancellationToken,
) -> RefreshProviderResult {
    let Some(refresher) = record.provider.models_refresher() else {
        return RefreshProviderResult::Complete;
    };
    let (mut lease, old_tail) = match begin_generation(&inner, &record) {
        Ok(value) => value,
        Err(BeginGenerationError::Superseded) => return RefreshProviderResult::Superseded,
        Err(BeginGenerationError::Exhausted) => {
            return RefreshProviderResult::Error(AuthResolutionError::Models(ModelsError::new(
                ModelsErrorCode::ModelSource,
            )));
        }
    };
    if let Err(error) = race_phase(&caller, &lease.cancellation, Box::pin(old_tail.wait())).await {
        return map_phase_cancel(error);
    }
    if !authority_is_current(
        &inner,
        &record.id,
        &record.owner,
        lease.epoch,
        lease.generation,
    ) {
        return RefreshProviderResult::Superseded;
    }
    let stored =
        match refresh_store_snapshot(&inner, &record.id, &caller, &lease.cancellation).await {
            Ok(stored) => stored,
            Err(error) => return error,
        };
    if let Err(error) = run_refresh_phase(
        &inner,
        &record,
        &lease,
        refresher,
        None,
        stored.as_ref(),
        false,
        None,
        &caller,
    )
    .await
    {
        return error;
    }
    if !allow_network {
        lease.finish();
        return RefreshProviderResult::Complete;
    }
    let credential =
        match resolve_catalog_credential(&inner, &record, &caller, &lease.cancellation).await {
            Ok(Some(credential)) => credential,
            Ok(None) => {
                lease.finish();
                return RefreshProviderResult::Complete;
            }
            Err(error) => return error,
        };
    let stored =
        match refresh_store_snapshot(&inner, &record.id, &caller, &lease.cancellation).await {
            Ok(stored) => stored,
            Err(error) => return error,
        };
    if let Err(error) = run_refresh_phase(
        &inner,
        &record,
        &lease,
        refresher,
        Some(&credential),
        stored.as_ref(),
        true,
        force,
        &caller,
    )
    .await
    {
        return error;
    }
    lease.finish();
    RefreshProviderResult::Complete
}

fn apply_memory_if_current(
    inner: &RegistryInner,
    provider: &ProviderId,
    owner: &ProviderOwner,
    epoch: u128,
    generation: u128,
    memory: ModelsMemoryIntent,
) -> Result<Option<Arc<[Model]>>, ModelsMemoryIntent> {
    let membership = inner
        .membership
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    let committed = membership.committed.iter().any(|record| {
        &record.id == provider
            && record.epoch == epoch
            && std::ptr::eq(record.owner.as_ref(), owner)
    });
    let projected = membership.projected.iter().any(|record| {
        &record.id == provider
            && record.epoch == epoch
            && std::ptr::eq(record.owner.as_ref(), owner)
    });
    if !committed || !projected {
        return Err(memory);
    }
    let mut owner_state = owner
        .state
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    if !publication_authority_matches(&owner_state, epoch, generation) {
        return Err(memory);
    }
    let old = match memory {
        ModelsMemoryIntent::Keep => None,
        ModelsMemoryIntent::Set(models) => {
            Some(std::mem::replace(&mut owner_state.dynamic, models))
        }
    };
    drop(owner_state);
    drop(membership);
    Ok(old)
}

#[allow(clippy::too_many_arguments)]
async fn run_publication_command(
    inner: Arc<RegistryInner>,
    owner: Arc<ProviderOwner>,
    provider: ProviderId,
    epoch: u128,
    generation: u128,
    cancellation: CancellationToken,
    publication: ModelsPublication,
    gate: Completion<PublicationGate>,
    result: Completion<PublicationResult>,
    command_tail: Completion<()>,
) {
    let PublicationGate::Run(previous) = gate.wait().await else {
        return;
    };
    previous.wait().await;
    if cancellation.is_cancelled()
        || !authority_is_current(&inner, &provider, &owner, epoch, generation)
    {
        complete_publication(&result, &command_tail, PublicationResult::Superseded);
        return;
    }

    let ModelsPublication {
        memory,
        persistence,
    } = publication;
    let persisted = match persistence {
        ModelsPersistenceIntent::Keep => Ok(()),
        ModelsPersistenceIntent::Set(entry) => {
            inner
                .options
                .models_store
                .write(&provider, entry, &cancellation)
                .await
        }
        ModelsPersistenceIntent::Delete => {
            inner
                .options
                .models_store
                .delete(&provider, &cancellation)
                .await
        }
    };
    if let Err(error) = persisted {
        let outcome = if cancellation.is_cancelled()
            || !authority_is_current(&inner, &provider, &owner, epoch, generation)
        {
            PublicationResult::Superseded
        } else {
            PublicationResult::Failed(Arc::new(Mutex::new(Some(error))))
        };
        complete_publication(&result, &command_tail, outcome);
        return;
    }

    if cancellation.is_cancelled() {
        complete_publication(&result, &command_tail, PublicationResult::Superseded);
        return;
    }
    let outcome =
        match apply_memory_if_current(&inner, &provider, &owner, epoch, generation, memory) {
            Ok(old) => {
                drop(old);
                PublicationResult::Published
            }
            Err(memory) => {
                drop(memory);
                PublicationResult::Superseded
            }
        };
    complete_publication(&result, &command_tail, outcome);
}

fn complete_publication(
    result: &Completion<PublicationResult>,
    command_tail: &Completion<()>,
    outcome: PublicationResult,
) {
    result.complete(outcome);
    command_tail.complete(());
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::task::{Context, Poll, Wake, Waker};
    use std::time::Duration;

    use crate::{
        ApiAdapter, ApiId, ApiKeyAuth, ApiKeyAuthInput, AuthContext, AuthContextError,
        AuthProviderError, AuthResult, BoxFuture, CancellationToken, Cancelled, Clock,
        InMemoryCredentialStore, InMemoryModelsStore, Model, ModelHeaderPolicy, ModelsError,
        ModelsErrorCode, ModelsPublication, ModelsRegistryOptions, Provider, ProviderAuth,
        ProviderId, SpawnError, TaskSpawner,
    };

    use super::{
        BeginGenerationError, Completion, GenerationAuthority, MembershipState, ModelsPublisher,
        MutationIntent, ProjectedRecord, ProviderOwner, ProviderRecord, PublicationResult,
        RegistryInner, admit_mutation, allocate_checked, begin_generation, complete_publication,
    };

    struct DummyContext;

    impl AuthContext for DummyContext {
        fn env<'a>(
            &'a self,
            _name: &'a str,
            _cancellation: &'a CancellationToken,
        ) -> BoxFuture<'a, Result<Option<String>, AuthContextError>> {
            Box::pin(async { Ok(None) })
        }

        fn file_exists<'a>(
            &'a self,
            _path: &'a str,
            _cancellation: &'a CancellationToken,
        ) -> BoxFuture<'a, Result<bool, AuthContextError>> {
            Box::pin(async { Ok(false) })
        }
    }

    struct DummyClock;

    impl Clock for DummyClock {
        fn now_unix_ms(&self) -> u64 {
            0
        }

        fn sleep<'a>(
            &'a self,
            _duration: Duration,
            _cancellation: &'a CancellationToken,
        ) -> BoxFuture<'a, Result<(), Cancelled>> {
            Box::pin(std::future::pending())
        }
    }

    #[derive(Default)]
    struct CountingSpawner {
        calls: std::sync::atomic::AtomicUsize,
    }

    impl TaskSpawner for CountingSpawner {
        fn spawn(&self, _task: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
            self.calls.fetch_add(1, Ordering::AcqRel);
            Ok(())
        }
    }

    struct DummyApiKey;

    impl ApiKeyAuth for DummyApiKey {
        fn name(&self) -> &str {
            "dummy"
        }

        fn resolve<'a>(
            &'a self,
            _input: ApiKeyAuthInput<'a>,
        ) -> BoxFuture<'a, Result<Option<AuthResult>, AuthProviderError>> {
            Box::pin(async { Ok(None) })
        }
    }

    struct DummyProvider {
        id: ProviderId,
        auth: ProviderAuth,
    }

    impl DummyProvider {
        fn new() -> Self {
            Self {
                id: ProviderId::new("p").expect("id"),
                auth: ProviderAuth::new(Some(Arc::new(DummyApiKey)), None).expect("auth"),
            }
        }
    }

    impl Provider for DummyProvider {
        fn id(&self) -> &ProviderId {
            &self.id
        }

        fn name(&self) -> &str {
            "p"
        }

        fn auth(&self) -> &ProviderAuth {
            &self.auth
        }

        fn models(&self) -> Result<Arc<[Model]>, ModelsError> {
            Ok(Arc::from([]))
        }

        fn adapter(&self, _api: &ApiId) -> Option<Arc<dyn ApiAdapter>> {
            None
        }
    }

    fn registry_inner(spawner: Arc<CountingSpawner>) -> Arc<RegistryInner> {
        Arc::new(RegistryInner {
            options: ModelsRegistryOptions {
                identity: None,
                credentials: Arc::new(InMemoryCredentialStore::new()),
                models_store: Box::new(InMemoryModelsStore::new()),
                auth_context: Arc::new(DummyContext),
                clock: Arc::new(DummyClock),
                spawner,
                model_header_policy: ModelHeaderPolicy::default(),
            },
            membership: std::sync::Mutex::new(MembershipState {
                committed: Arc::from([]),
                projected: Vec::new(),
                next_ticket: 0,
                tail: Completion::completed(()),
            }),
        })
    }

    #[test]
    fn checked_registry_counters_never_wrap_or_mutate_at_exhaustion() {
        for mut counter in [0, u128::MAX - 1, u128::MAX] {
            let prior = counter;
            let result = allocate_checked(&mut counter);
            if prior == u128::MAX {
                assert_eq!(result, Err(()));
                assert_eq!(counter, u128::MAX);
            } else {
                assert_eq!(result, Ok(prior + 1));
                assert_eq!(counter, prior + 1);
            }
        }

        let spawner = Arc::new(CountingSpawner::default());
        let inner = registry_inner(spawner.clone());
        inner
            .membership
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .next_ticket = u128::MAX;
        assert!(
            admit_mutation(
                &inner,
                MutationIntent::Clear,
                Arc::new(ProviderOwner::new()),
                Completion::pending(),
            )
            .is_err()
        );
        assert_eq!(
            inner
                .membership
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .next_ticket,
            u128::MAX
        );

        let owner = Arc::new(ProviderOwner::new());
        {
            let mut state = owner
                .state
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            state.current_epoch = Some(1);
            state.next_generation = u128::MAX;
        }
        let provider = Arc::new(DummyProvider::new());
        let record = Arc::new(ProviderRecord {
            id: provider.id.clone(),
            provider,
            owner: owner.clone(),
            epoch: 1,
        });
        {
            let mut membership = inner
                .membership
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            membership.committed = vec![record.clone()].into();
            membership.projected = vec![ProjectedRecord {
                id: record.id.clone(),
                owner: owner.clone(),
                epoch: 1,
            }];
        }
        assert!(matches!(
            begin_generation(&inner, &record),
            Err(BeginGenerationError::Exhausted)
        ));

        {
            let mut state = owner
                .state
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            state.next_publication_ticket = u128::MAX;
            state.active = Some(GenerationAuthority {
                generation: 1,
                cancellation: CancellationToken::new(),
            });
        }
        let mut publisher = ModelsPublisher {
            inner,
            owner,
            provider: ProviderId::new("p").expect("id"),
            epoch: 1,
            generation: 1,
            cancellation: CancellationToken::new(),
        };
        let mut publication = publisher.publish(ModelsPublication::default());
        assert!(matches!(
            publication
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(Err(error)) if error.code() == ModelsErrorCode::ModelSource
        ));
        assert_eq!(spawner.calls.load(Ordering::Acquire), 0);
    }

    struct ReentrantWake {
        completion: Completion<()>,
        woke: AtomicBool,
    }

    impl Wake for ReentrantWake {
        fn wake(self: Arc<Self>) {
            self.woke.store(true, Ordering::Release);
            self.completion.complete(());
        }
    }

    #[test]
    fn completion_wakes_after_unlock_and_waiter_drop_unregisters() {
        let completion = Completion::pending();
        let wake = Arc::new(ReentrantWake {
            completion: completion.clone(),
            woke: AtomicBool::new(false),
        });
        let waker: Waker = Waker::from(wake.clone());
        let mut future = Box::pin(completion.wait());
        assert!(matches!(
            future.as_mut().poll(&mut Context::from_waker(&waker)),
            Poll::Pending
        ));
        completion.complete(());
        assert!(wake.woke.load(Ordering::Acquire));
        assert!(matches!(
            future
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(())
        ));
    }

    struct PublicationAckWake {
        tail: Completion<()>,
        observed_before_tail: AtomicBool,
    }

    impl Wake for PublicationAckWake {
        fn wake(self: Arc<Self>) {
            assert!(!self.tail.is_complete());
            self.observed_before_tail.store(true, Ordering::Release);
        }
    }

    #[test]
    fn publication_acknowledgement_wakes_before_owner_tail() {
        let result = Completion::pending();
        let tail = Completion::pending();
        let wake = Arc::new(PublicationAckWake {
            tail: tail.clone(),
            observed_before_tail: AtomicBool::new(false),
        });
        let waker = Waker::from(wake.clone());
        let mut waiter = Box::pin(result.wait());
        assert!(matches!(
            waiter.as_mut().poll(&mut Context::from_waker(&waker)),
            Poll::Pending
        ));
        complete_publication(&result, &tail, PublicationResult::Published);
        assert!(wake.observed_before_tail.load(Ordering::Acquire));
        assert!(tail.is_complete());
    }

    struct DropReentryWake {
        completion: Completion<()>,
        dropped: Arc<AtomicBool>,
        wakes: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl Wake for DropReentryWake {
        fn wake(self: Arc<Self>) {
            self.wakes.fetch_add(1, Ordering::AcqRel);
        }
    }

    impl Drop for DropReentryWake {
        fn drop(&mut self) {
            self.completion.complete(());
            self.dropped.store(true, Ordering::Release);
        }
    }

    #[test]
    fn sole_owned_waiter_waker_drop_reenters_and_unregisters() {
        let completion = Completion::pending();
        let dropped = Arc::new(AtomicBool::new(false));
        let wakes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let waker = Waker::from(Arc::new(DropReentryWake {
            completion: completion.clone(),
            dropped: dropped.clone(),
            wakes: wakes.clone(),
        }));
        let mut waiter = Box::pin(completion.wait());
        assert!(matches!(
            waiter.as_mut().poll(&mut Context::from_waker(&waker)),
            Poll::Pending
        ));
        drop(waker);
        drop(waiter);
        assert!(dropped.load(Ordering::Acquire));
        completion.complete(());
        assert_eq!(wakes.load(Ordering::Acquire), 0);
    }
}
