use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::future::{Future, poll_fn};
use std::pin::{Pin, pin};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context as TaskContext, Poll, Waker};
use std::time::Duration;

use pi_ai_core::{
    ApiAdapter, ApiId, ApiKeyAuth, ApiKeyAuthInput, ApiKeyCredential, ApiKeyCredentialType,
    AssistantMessage, AssistantMessageEvent, AssistantRole, AssistantStream, AssistantStreamInit,
    AuthCheck, AuthContext, AuthContextError, AuthInteraction, AuthInteractionError,
    AuthProviderError, AuthResolutionError, AuthResolutionOverrides, AuthResult, AuthType,
    BoxFuture, CancellationToken, Cancelled, CapabilityError, Clock, Context, Credential,
    CredentialInfo, CredentialMutation, CredentialMutationValue, CredentialReconciliation,
    CredentialRevision, CredentialStore, CredentialStoreError, CredentialStoreErrorCode,
    DeferredCancelOptions, DeferredCancelOptionsWire, DeferredCancelRequest, DeferredFetchOptions,
    DeferredFetchOptionsWire, DeferredFetchRequest, DeferredHandle, DeferredId, DoneReason,
    InMemoryCredentialStore, Model, ModelAuth, ModelBaseUrl, ModelHeaderPolicy, ModelId,
    ModelsError, ModelsErrorCode, ModelsMemoryIntent, ModelsPersistenceIntent, ModelsPublication,
    ModelsRefreshOptions, ModelsRegistry, ModelsRegistryOptions, ModelsRequestOverrides,
    ModelsStore, ModelsStoreEntry, ModelsStoreError, OAuthAuth, OAuthCredential,
    OAuthCredentialType, OAuthCredentials, OAuthExtraFields, Provider, ProviderAuth, ProviderEnv,
    ProviderHeaders, ProviderId, ProviderModelsRefresher, PublicationOutcome, RawModel,
    RawModelHeaders, RawModelsStoreEntry, RequestEndpoint, RequestRuntime, ResolvedAuth,
    SafeUnsigned, SecretString, SimpleStreamOptions, SimpleStreamOptionsWire, SimpleStreamRequest,
    SpawnError, StopReason, StreamOptions, StreamOptionsWire, TaskSpawner, TextStreamRequest,
    Usage, VersionedCredential, create_assistant_message_event_stream,
};
use pi_telemetry::{NOOP_TELEMETRY_CONTEXT, TelemetryContext};

const MODEL_FIXTURE: &str =
    include_str!("../../../conformance/schema/ai/models-options.golden.json");
const SECRET_SENTINEL: &str = "REGISTRY_SECRET_SENTINEL_29db";

#[derive(Debug)]
struct SentinelError;

impl fmt::Display for SentinelError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(SECRET_SENTINEL)
    }
}

impl Error for SentinelError {}

fn poll<F: Future + ?Sized>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut TaskContext::from_waker(Waker::noop()))
}

fn ready<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    match poll(future.as_mut()) {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("future unexpectedly pending"),
    }
}

#[derive(Default)]
struct ManualSpawner {
    tasks: Mutex<Vec<BoxFuture<'static, ()>>>,
    calls: AtomicUsize,
    fail: Mutex<bool>,
}

impl ManualSpawner {
    fn drive_once(&self) {
        let tasks = {
            let mut tasks = self.tasks.lock().unwrap_or_else(|p| p.into_inner());
            std::mem::take(&mut *tasks)
        };
        let mut pending = Vec::new();
        for mut task in tasks {
            if poll(task.as_mut()).is_pending() {
                pending.push(task);
            }
        }
        self.tasks
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .extend(pending);
    }

    fn drive(&self) {
        for _ in 0..64 {
            if self
                .tasks
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .is_empty()
            {
                return;
            }
            self.drive_once();
        }
    }

    fn set_fail(&self, fail: bool) {
        *self.fail.lock().unwrap_or_else(|p| p.into_inner()) = fail;
    }
}

impl TaskSpawner for ManualSpawner {
    fn spawn(&self, mut task: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        if *self.fail.lock().unwrap_or_else(|p| p.into_inner()) {
            return Err(SpawnError::with_source(SentinelError));
        }
        if poll(task.as_mut()).is_pending() {
            self.tasks
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push(task);
        }
        Ok(())
    }
}

type Reentry = Box<dyn FnOnce() + Send>;

#[derive(Default)]
struct ReentrantSpawner {
    tasks: Mutex<Vec<BoxFuture<'static, ()>>>,
    callback: Mutex<Option<Reentry>>,
    depth: AtomicUsize,
    fail_outer: std::sync::atomic::AtomicBool,
    calls: AtomicUsize,
}

impl ReentrantSpawner {
    fn drive_once(&self) {
        let tasks = {
            let mut tasks = self.tasks.lock().unwrap_or_else(|p| p.into_inner());
            std::mem::take(&mut *tasks)
        };
        let mut pending = Vec::new();
        for mut task in tasks {
            if poll(task.as_mut()).is_pending() {
                pending.push(task);
            }
        }
        self.tasks
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .extend(pending);
    }
}

impl TaskSpawner for ReentrantSpawner {
    fn spawn(&self, mut task: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        assert!(
            poll(task.as_mut()).is_pending(),
            "closed gate must not run synchronously"
        );
        let depth = self.depth.fetch_add(1, Ordering::AcqRel);
        if depth == 0 {
            let callback = self
                .callback
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .take();
            if let Some(callback) = callback {
                callback();
            }
        }
        self.depth.fetch_sub(1, Ordering::AcqRel);
        if depth == 0 && self.fail_outer.load(Ordering::Acquire) {
            return Err(SpawnError::new());
        }
        self.tasks
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(task);
        Ok(())
    }
}

struct RegistryDropWake {
    registry: Arc<ModelsRegistry>,
    dropped: Arc<std::sync::atomic::AtomicBool>,
    wakes: Arc<AtomicUsize>,
}

impl std::task::Wake for RegistryDropWake {
    fn wake(self: Arc<Self>) {
        self.wakes.fetch_add(1, Ordering::AcqRel);
    }
}

impl Drop for RegistryDropWake {
    fn drop(&mut self) {
        let _ = self.registry.get_models(None);
        self.dropped.store(true, Ordering::Release);
    }
}

fn drive_reentrant<F: Future>(future: F, spawner: &ReentrantSpawner) -> F::Output {
    let mut future = pin!(future);
    for _ in 0..256 {
        if let Poll::Ready(value) = poll(future.as_mut()) {
            return value;
        }
        spawner.drive_once();
    }
    panic!("reentrant executor did not settle")
}

fn drive<F: Future>(future: F, spawner: &ManualSpawner) -> F::Output {
    let mut future = pin!(future);
    for _ in 0..256 {
        if let Poll::Ready(value) = poll(future.as_mut()) {
            return value;
        }
        spawner.drive();
    }
    panic!("manual executor did not settle")
}

struct TestClock {
    now: AtomicU64,
}

impl TestClock {
    fn new(now: u64) -> Self {
        Self {
            now: AtomicU64::new(now),
        }
    }
}

impl Clock for TestClock {
    fn now_unix_ms(&self) -> u64 {
        self.now.load(Ordering::Acquire)
    }

    fn sleep<'a>(
        &'a self,
        _duration: Duration,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), Cancelled>> {
        Box::pin(async move {
            std::future::pending::<()>().await;
            if cancellation.is_cancelled() {
                Err(Cancelled)
            } else {
                Ok(())
            }
        })
    }
}

struct EmptyContext;

impl AuthContext for EmptyContext {
    fn env<'a>(
        &'a self,
        _name: &'a str,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<String>, AuthContextError>> {
        Box::pin(async move {
            if cancellation.is_cancelled() {
                Err(AuthContextError::new())
            } else {
                Ok(None)
            }
        })
    }

    fn file_exists<'a>(
        &'a self,
        _path: &'a str,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<bool, AuthContextError>> {
        Box::pin(async move {
            if cancellation.is_cancelled() {
                Err(AuthContextError::new())
            } else {
                Ok(false)
            }
        })
    }
}

struct TestInteraction;

impl AuthInteraction for TestInteraction {
    fn prompt<'a>(
        &'a self,
        _prompt: pi_ai_core::AuthPrompt,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<String, AuthInteractionError>> {
        Box::pin(async { Ok("answer".to_owned()) })
    }

    fn notify(&self, _event: pi_ai_core::AuthEvent) -> Result<(), AuthInteractionError> {
        Ok(())
    }
}

#[derive(Default)]
struct QuarantineCredentialStore {
    reconciliations: Mutex<Vec<CredentialReconciliation>>,
}

impl CredentialStore for QuarantineCredentialStore {
    fn read<'a>(
        &'a self,
        _provider: &'a ProviderId,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<VersionedCredential>, CredentialStoreError>> {
        Box::pin(async {
            Err(CredentialStoreError::new(
                CredentialStoreErrorCode::OutcomeUnknown,
            ))
        })
    }

    fn list<'a>(
        &'a self,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Vec<CredentialInfo>, CredentialStoreError>> {
        Box::pin(async {
            Err(CredentialStoreError::new(
                CredentialStoreErrorCode::OutcomeUnknown,
            ))
        })
    }

    fn begin_mutation<'a>(
        &'a self,
        _provider: &'a ProviderId,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Box<dyn CredentialMutation>, CredentialStoreError>> {
        Box::pin(async {
            Err(CredentialStoreError::new(
                CredentialStoreErrorCode::OutcomeUnknown,
            ))
        })
    }

    fn reconcile<'a>(
        &'a self,
        _provider: &'a ProviderId,
        resolution: CredentialReconciliation,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<VersionedCredential>, CredentialStoreError>> {
        Box::pin(async move {
            if cancellation.is_cancelled() {
                return Err(CredentialStoreError::new(
                    CredentialStoreErrorCode::Cancelled,
                ));
            }
            self.reconciliations
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push(resolution.clone());
            Ok(match resolution {
                CredentialReconciliation::Replace(credential) => Some(VersionedCredential {
                    revision: CredentialRevision::new(7).expect("reserved revision"),
                    credential,
                }),
                CredentialReconciliation::Delete => None,
            })
        })
    }
}

#[derive(Default)]
struct BlockingQuarantineState {
    reconciliations: Mutex<Vec<CredentialReconciliation>>,
    admitted: std::sync::atomic::AtomicBool,
    released: std::sync::atomic::AtomicBool,
    wakers: Mutex<Vec<Waker>>,
}

impl BlockingQuarantineState {
    fn release(&self) {
        self.released.store(true, Ordering::Release);
        let wakers = {
            let mut wakers = self.wakers.lock().unwrap_or_else(|p| p.into_inner());
            std::mem::take(&mut *wakers)
        };
        for waker in wakers {
            waker.wake();
        }
    }
}

struct BlockingQuarantineStore {
    state: Arc<BlockingQuarantineState>,
}

struct BlockingReconcileFuture {
    state: Arc<BlockingQuarantineState>,
    resolution: Option<CredentialReconciliation>,
    cancellation: CancellationToken,
    admitted: bool,
}

impl Future for BlockingReconcileFuture {
    type Output = Result<Option<VersionedCredential>, CredentialStoreError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut TaskContext<'_>) -> Poll<Self::Output> {
        if !self.admitted {
            if self.cancellation.is_cancelled() {
                return Poll::Ready(Err(CredentialStoreError::new(
                    CredentialStoreErrorCode::Cancelled,
                )));
            }
            self.admitted = true;
            self.state.admitted.store(true, Ordering::Release);
        }
        if !self.state.released.load(Ordering::Acquire) {
            self.state
                .wakers
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push(context.waker().clone());
            return Poll::Pending;
        }
        let resolution = self.resolution.take().expect("resolution");
        self.state
            .reconciliations
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(resolution.clone());
        Poll::Ready(Ok(match resolution {
            CredentialReconciliation::Replace(credential) => Some(VersionedCredential {
                revision: CredentialRevision::new(7).expect("reserved"),
                credential,
            }),
            CredentialReconciliation::Delete => None,
        }))
    }
}

impl CredentialStore for BlockingQuarantineStore {
    fn read<'a>(
        &'a self,
        _provider: &'a ProviderId,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<VersionedCredential>, CredentialStoreError>> {
        Box::pin(async {
            Err(CredentialStoreError::new(
                CredentialStoreErrorCode::OutcomeUnknown,
            ))
        })
    }

    fn list<'a>(
        &'a self,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Vec<CredentialInfo>, CredentialStoreError>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn begin_mutation<'a>(
        &'a self,
        _provider: &'a ProviderId,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Box<dyn CredentialMutation>, CredentialStoreError>> {
        Box::pin(async {
            Err(CredentialStoreError::new(
                CredentialStoreErrorCode::OutcomeUnknown,
            ))
        })
    }

    fn reconcile<'a>(
        &'a self,
        _provider: &'a ProviderId,
        resolution: CredentialReconciliation,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<VersionedCredential>, CredentialStoreError>> {
        Box::pin(BlockingReconcileFuture {
            state: self.state.clone(),
            resolution: Some(resolution),
            cancellation: cancellation.clone(),
            admitted: false,
        })
    }
}

#[derive(Default)]
struct BlockingCredentialState {
    value: Mutex<Option<VersionedCredential>>,
    admitted: std::sync::atomic::AtomicBool,
    released: std::sync::atomic::AtomicBool,
    wakers: Mutex<Vec<Waker>>,
}

impl BlockingCredentialState {
    fn release(&self) {
        self.released.store(true, Ordering::Release);
        let wakers = {
            let mut wakers = self.wakers.lock().unwrap_or_else(|p| p.into_inner());
            std::mem::take(&mut *wakers)
        };
        for waker in wakers {
            waker.wake();
        }
    }
}

struct BlockingCredentialStore {
    state: Arc<BlockingCredentialState>,
}

struct BlockingMutation {
    state: Arc<BlockingCredentialState>,
    current: Option<VersionedCredential>,
    cancellation: CancellationToken,
}

impl CredentialMutation for BlockingMutation {
    fn current(&self) -> Option<&VersionedCredential> {
        self.current.as_ref()
    }

    fn commit(
        self: Box<Self>,
        next: CredentialMutationValue,
    ) -> BoxFuture<'static, Result<Option<VersionedCredential>, CredentialStoreError>> {
        Box::pin(BlockingCommit {
            mutation: Some(*self),
            next: Some(next),
            admitted: false,
        })
    }
}

struct BlockingCommit {
    mutation: Option<BlockingMutation>,
    next: Option<CredentialMutationValue>,
    admitted: bool,
}

impl Future for BlockingCommit {
    type Output = Result<Option<VersionedCredential>, CredentialStoreError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut TaskContext<'_>) -> Poll<Self::Output> {
        if !self.admitted {
            let state = Arc::clone(&self.mutation.as_ref().expect("mutation").state);
            if self
                .mutation
                .as_ref()
                .expect("mutation")
                .cancellation
                .is_cancelled()
            {
                return Poll::Ready(Err(CredentialStoreError::new(
                    CredentialStoreErrorCode::Cancelled,
                )));
            }
            self.admitted = true;
            state.admitted.store(true, Ordering::Release);
        }
        let mutation = self.mutation.as_ref().expect("mutation");
        if !mutation.state.released.load(Ordering::Acquire) {
            mutation
                .state
                .wakers
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push(context.waker().clone());
            return Poll::Pending;
        }
        let mutation = self.mutation.take().expect("mutation");
        let next = self.next.take().expect("next");
        let value = match next {
            CredentialMutationValue::Unchanged => mutation.current,
            CredentialMutationValue::Set(credential) => Some(VersionedCredential {
                revision: mutation
                    .current
                    .as_ref()
                    .and_then(|value| value.revision.checked_next())
                    .unwrap_or(CredentialRevision::FIRST),
                credential,
            }),
            CredentialMutationValue::Delete => None,
        };
        *mutation
            .state
            .value
            .lock()
            .unwrap_or_else(|p| p.into_inner()) = value.clone();
        Poll::Ready(Ok(value))
    }
}

impl CredentialStore for BlockingCredentialStore {
    fn read<'a>(
        &'a self,
        _provider: &'a ProviderId,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<VersionedCredential>, CredentialStoreError>> {
        Box::pin(async move {
            if cancellation.is_cancelled() {
                return Err(CredentialStoreError::new(
                    CredentialStoreErrorCode::Cancelled,
                ));
            }
            Ok(self
                .state
                .value
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .clone())
        })
    }

    fn list<'a>(
        &'a self,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Vec<CredentialInfo>, CredentialStoreError>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn begin_mutation<'a>(
        &'a self,
        _provider: &'a ProviderId,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Box<dyn CredentialMutation>, CredentialStoreError>> {
        let mutation = BlockingMutation {
            state: Arc::clone(&self.state),
            current: self
                .state
                .value
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .clone(),
            cancellation: cancellation.clone(),
        };
        Box::pin(async move { Ok(Box::new(mutation) as Box<dyn CredentialMutation>) })
    }

    fn reconcile<'a>(
        &'a self,
        _provider: &'a ProviderId,
        _resolution: CredentialReconciliation,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<VersionedCredential>, CredentialStoreError>> {
        Box::pin(async {
            Err(CredentialStoreError::new(
                CredentialStoreErrorCode::ReconciliationNotRequired,
            ))
        })
    }
}

struct TestApiKey {
    configured: bool,
    check_calls: AtomicUsize,
    resolve_calls: AtomicUsize,
    login_error: bool,
    auth: Mutex<ModelAuth>,
    env: Mutex<Option<ProviderEnv>>,
}

impl TestApiKey {
    fn configured() -> Self {
        Self {
            configured: true,
            check_calls: AtomicUsize::new(0),
            resolve_calls: AtomicUsize::new(0),
            login_error: false,
            auth: Mutex::new(ModelAuth {
                api_key: Some(SecretString::new("resolved-key")),
                headers: None,
                base_url: None,
            }),
            env: Mutex::new(None),
        }
    }
}

impl ApiKeyAuth for TestApiKey {
    fn name(&self) -> &str {
        "test api key"
    }

    fn login<'a>(
        &'a self,
        _interaction: &'a dyn AuthInteraction,
        _cancellation: &'a CancellationToken,
    ) -> Option<BoxFuture<'a, Result<ApiKeyCredential, AuthProviderError>>> {
        Some(Box::pin(async move {
            if self.login_error {
                Err(AuthProviderError::new())
            } else {
                Ok(ApiKeyCredential {
                    credential_type: ApiKeyCredentialType::ApiKey,
                    key: Some(SecretString::new("login-key")),
                    env: None,
                })
            }
        }))
    }

    fn check<'a>(
        &'a self,
        _input: ApiKeyAuthInput<'a>,
    ) -> Option<BoxFuture<'a, Result<Option<AuthCheck>, AuthProviderError>>> {
        Some(Box::pin(async move {
            self.check_calls.fetch_add(1, Ordering::AcqRel);
            Ok(self.configured.then(|| AuthCheck {
                source: Some("check".to_owned()),
                auth_type: AuthType::ApiKey,
            }))
        }))
    }

    fn resolve<'a>(
        &'a self,
        _input: ApiKeyAuthInput<'a>,
    ) -> BoxFuture<'a, Result<Option<AuthResult>, AuthProviderError>> {
        Box::pin(async move {
            self.resolve_calls.fetch_add(1, Ordering::AcqRel);
            if self.login_error {
                return Err(AuthProviderError::with_source(SentinelError));
            }
            if !self.configured {
                return Ok(None);
            }
            Ok(Some(AuthResult {
                auth: self.auth.lock().unwrap_or_else(|p| p.into_inner()).clone(),
                env: self.env.lock().unwrap_or_else(|p| p.into_inner()).clone(),
                source: Some("api-key".to_owned()),
            }))
        })
    }
}

struct TestOAuth {
    refresh_calls: AtomicUsize,
    login_error: bool,
    refresh_callback: Mutex<Option<Reentry>>,
}

fn oauth_credential(access: &str, expires: u64) -> OAuthCredential {
    OAuthCredential {
        credential_type: OAuthCredentialType::OAuth,
        credentials: OAuthCredentials {
            refresh: SecretString::new("refresh"),
            access: SecretString::new(access),
            expires: SafeUnsigned::new(expires).expect("safe"),
            extra: OAuthExtraFields::new(BTreeMap::new()).expect("extras"),
        },
    }
}

impl OAuthAuth for TestOAuth {
    fn name(&self) -> &str {
        "test oauth"
    }

    fn login<'a>(
        &'a self,
        _interaction: &'a dyn AuthInteraction,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<OAuthCredential, AuthProviderError>> {
        Box::pin(async move {
            if self.login_error {
                Err(AuthProviderError::with_source(SentinelError))
            } else {
                Ok(oauth_credential("login", 1_000))
            }
        })
    }

    fn refresh<'a>(
        &'a self,
        _credential: &'a OAuthCredential,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<OAuthCredential, AuthProviderError>> {
        Box::pin(async move {
            self.refresh_calls.fetch_add(1, Ordering::AcqRel);
            if let Some(callback) = self
                .refresh_callback
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .take()
            {
                callback();
            }
            Ok(oauth_credential("rotated", 1_000))
        })
    }

    fn to_auth<'a>(
        &'a self,
        credential: &'a OAuthCredential,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<ModelAuth, AuthProviderError>> {
        Box::pin(async move {
            Ok(ModelAuth {
                api_key: Some(credential.credentials.access.clone()),
                headers: None,
                base_url: None,
            })
        })
    }
}

#[derive(Default)]
struct BlockingOAuthState {
    started: std::sync::atomic::AtomicBool,
    released: std::sync::atomic::AtomicBool,
    wakers: Mutex<Vec<Waker>>,
}

impl BlockingOAuthState {
    fn release(&self) {
        self.released.store(true, Ordering::Release);
        let wakers = {
            let mut wakers = self.wakers.lock().unwrap_or_else(|p| p.into_inner());
            std::mem::take(&mut *wakers)
        };
        for waker in wakers {
            waker.wake();
        }
    }
}

struct BlockingOAuth {
    state: Arc<BlockingOAuthState>,
}

impl OAuthAuth for BlockingOAuth {
    fn name(&self) -> &str {
        "blocking oauth"
    }

    fn login<'a>(
        &'a self,
        _interaction: &'a dyn AuthInteraction,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<OAuthCredential, AuthProviderError>> {
        Box::pin(async { Ok(oauth_credential("login", 1_000)) })
    }

    fn refresh<'a>(
        &'a self,
        _credential: &'a OAuthCredential,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<OAuthCredential, AuthProviderError>> {
        Box::pin(poll_fn(move |context| {
            self.state.started.store(true, Ordering::Release);
            if self.state.released.load(Ordering::Acquire) {
                Poll::Ready(Ok(oauth_credential("rotated", 1_000)))
            } else {
                self.state
                    .wakers
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .push(context.waker().clone());
                Poll::Pending
            }
        }))
    }

    fn to_auth<'a>(
        &'a self,
        credential: &'a OAuthCredential,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<ModelAuth, AuthProviderError>> {
        Box::pin(async move {
            Ok(ModelAuth {
                api_key: Some(credential.credentials.access.clone()),
                headers: None,
                base_url: None,
            })
        })
    }
}

#[derive(Default)]
struct StoreState {
    entries: Mutex<BTreeMap<ProviderId, ModelsStoreEntry>>,
    reads: AtomicUsize,
    writes: AtomicUsize,
    deletes: AtomicUsize,
}

struct TestModelsStore {
    state: Arc<StoreState>,
}

impl ModelsStore for TestModelsStore {
    fn read<'a>(
        &'a self,
        provider: &'a ProviderId,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<RawModelsStoreEntry>, ModelsStoreError>> {
        Box::pin(async move {
            if cancellation.is_cancelled() {
                return Err(ModelsStoreError::new(
                    pi_ai_core::ModelsStoreErrorCode::Cancelled,
                ));
            }
            self.state.reads.fetch_add(1, Ordering::AcqRel);
            Ok(self
                .state
                .entries
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .get(provider)
                .map(|entry| RawModelsStoreEntry {
                    models: entry.models.iter().map(RawModel::from).collect(),
                    last_modified: entry.last_modified,
                    checked_at: entry.checked_at,
                    etag: entry.etag.clone(),
                }))
        })
    }

    fn write<'a>(
        &'a self,
        provider: &'a ProviderId,
        entry: ModelsStoreEntry,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), ModelsStoreError>> {
        Box::pin(async move {
            if cancellation.is_cancelled() {
                return Err(ModelsStoreError::new(
                    pi_ai_core::ModelsStoreErrorCode::Cancelled,
                ));
            }
            self.state.writes.fetch_add(1, Ordering::AcqRel);
            self.state
                .entries
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .insert(provider.clone(), entry);
            Ok(())
        })
    }

    fn delete<'a>(
        &'a self,
        provider: &'a ProviderId,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), ModelsStoreError>> {
        Box::pin(async move {
            if cancellation.is_cancelled() {
                return Err(ModelsStoreError::new(
                    pi_ai_core::ModelsStoreErrorCode::Cancelled,
                ));
            }
            self.state.deletes.fetch_add(1, Ordering::AcqRel);
            self.state
                .entries
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .remove(provider);
            Ok(())
        })
    }
}

struct TraceModelsStore {
    trace: Arc<Mutex<Vec<&'static str>>>,
}

struct RawModelsStore {
    raw: Mutex<Option<RawModelsStoreEntry>>,
    reads: AtomicUsize,
}

struct FailingModelsStore;

impl ModelsStore for FailingModelsStore {
    fn read<'a>(
        &'a self,
        _provider: &'a ProviderId,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<RawModelsStoreEntry>, ModelsStoreError>> {
        Box::pin(async { Ok(None) })
    }

    fn write<'a>(
        &'a self,
        _provider: &'a ProviderId,
        _entry: ModelsStoreEntry,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), ModelsStoreError>> {
        Box::pin(async { Err(ModelsStoreError::with_source(SentinelError)) })
    }

    fn delete<'a>(
        &'a self,
        _provider: &'a ProviderId,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), ModelsStoreError>> {
        Box::pin(async { Err(ModelsStoreError::with_source(SentinelError)) })
    }
}

struct ReentrantSource {
    callback: Mutex<Option<Reentry>>,
}

impl fmt::Debug for ReentrantSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ReentrantSource([REDACTED])")
    }
}

impl fmt::Display for ReentrantSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(SECRET_SENTINEL)
    }
}

impl Error for ReentrantSource {}

impl Drop for ReentrantSource {
    fn drop(&mut self) {
        if let Some(callback) = self
            .callback
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take()
        {
            callback();
        }
    }
}

struct ReentrantFailingModelsStore {
    callback: Arc<Mutex<Option<Reentry>>>,
}

impl ModelsStore for ReentrantFailingModelsStore {
    fn read<'a>(
        &'a self,
        _provider: &'a ProviderId,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<RawModelsStoreEntry>, ModelsStoreError>> {
        Box::pin(async { Ok(None) })
    }

    fn write<'a>(
        &'a self,
        _provider: &'a ProviderId,
        _entry: ModelsStoreEntry,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), ModelsStoreError>> {
        let callback = self
            .callback
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take();
        Box::pin(async move {
            Err(ModelsStoreError::with_source(ReentrantSource {
                callback: Mutex::new(callback),
            }))
        })
    }

    fn delete<'a>(
        &'a self,
        _provider: &'a ProviderId,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), ModelsStoreError>> {
        Box::pin(async { Ok(()) })
    }
}

impl ModelsStore for RawModelsStore {
    fn read<'a>(
        &'a self,
        _provider: &'a ProviderId,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<RawModelsStoreEntry>, ModelsStoreError>> {
        Box::pin(async move {
            self.reads.fetch_add(1, Ordering::AcqRel);
            Ok(self.raw.lock().unwrap_or_else(|p| p.into_inner()).clone())
        })
    }

    fn write<'a>(
        &'a self,
        _provider: &'a ProviderId,
        _entry: ModelsStoreEntry,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), ModelsStoreError>> {
        Box::pin(async { Ok(()) })
    }

    fn delete<'a>(
        &'a self,
        _provider: &'a ProviderId,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), ModelsStoreError>> {
        Box::pin(async { Ok(()) })
    }
}

impl ModelsStore for TraceModelsStore {
    fn read<'a>(
        &'a self,
        _provider: &'a ProviderId,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<RawModelsStoreEntry>, ModelsStoreError>> {
        Box::pin(async move {
            self.trace
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push("store-read");
            Ok(None)
        })
    }

    fn write<'a>(
        &'a self,
        _provider: &'a ProviderId,
        _entry: ModelsStoreEntry,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), ModelsStoreError>> {
        Box::pin(async move {
            self.trace
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push("store-write");
            Ok(())
        })
    }

    fn delete<'a>(
        &'a self,
        _provider: &'a ProviderId,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), ModelsStoreError>> {
        Box::pin(async { Ok(()) })
    }
}

struct TraceCredentialStore {
    trace: Arc<Mutex<Vec<&'static str>>>,
}

impl CredentialStore for TraceCredentialStore {
    fn read<'a>(
        &'a self,
        _provider: &'a ProviderId,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<VersionedCredential>, CredentialStoreError>> {
        Box::pin(async move {
            self.trace
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push("credential-read");
            Ok(None)
        })
    }

    fn list<'a>(
        &'a self,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Vec<CredentialInfo>, CredentialStoreError>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn begin_mutation<'a>(
        &'a self,
        _provider: &'a ProviderId,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Box<dyn CredentialMutation>, CredentialStoreError>> {
        Box::pin(async { Err(CredentialStoreError::new(CredentialStoreErrorCode::Failed)) })
    }

    fn reconcile<'a>(
        &'a self,
        _provider: &'a ProviderId,
        _resolution: CredentialReconciliation,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<VersionedCredential>, CredentialStoreError>> {
        Box::pin(async {
            Err(CredentialStoreError::new(
                CredentialStoreErrorCode::ReconciliationNotRequired,
            ))
        })
    }
}

struct TraceRefresher {
    trace: Arc<Mutex<Vec<&'static str>>>,
}

impl ProviderModelsRefresher for TraceRefresher {
    fn refresh_models<'a>(
        &'a self,
        context: pi_ai_core::RefreshModelsContext<'a>,
    ) -> BoxFuture<'a, Result<(), ModelsError>> {
        Box::pin(async move {
            let phase = if context.allow_network {
                "network"
            } else {
                "cache"
            };
            self.trace
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push(phase);
            if !context.allow_network {
                assert_eq!(
                    context
                        .publisher
                        .publish(ModelsPublication::default())
                        .await?,
                    PublicationOutcome::Published
                );
                self.trace
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .push("cache-published");
            }
            Ok(())
        })
    }
}

struct TraceProvider {
    inner: TestProvider,
    refresher: TraceRefresher,
}

impl Provider for TraceProvider {
    fn id(&self) -> &ProviderId {
        &self.inner.id
    }
    fn name(&self) -> &str {
        &self.inner.name
    }
    fn auth(&self) -> &ProviderAuth {
        &self.inner.auth
    }
    fn models(&self) -> Result<Arc<[Model]>, ModelsError> {
        self.inner.models()
    }
    fn models_refresher(&self) -> Option<&(dyn ProviderModelsRefresher + '_)> {
        Some(&self.refresher)
    }
    fn adapter(&self, api: &ApiId) -> Option<Arc<dyn ApiAdapter>> {
        self.inner.adapter(api)
    }
}

#[derive(Clone)]
enum BlockingAction {
    Set(ModelsStoreEntry),
    Delete,
}

#[derive(Default)]
struct BlockingStoreControl {
    permits: usize,
    wakers: Vec<Waker>,
}

#[derive(Default)]
struct BlockingStoreState {
    entries: Mutex<BTreeMap<ProviderId, ModelsStoreEntry>>,
    reads: AtomicUsize,
    active: AtomicUsize,
    max_active: AtomicUsize,
    admissions: Mutex<Vec<(ProviderId, &'static str)>>,
    completions: Mutex<Vec<(ProviderId, &'static str)>>,
    control: Mutex<BlockingStoreControl>,
    hold_ack: std::sync::atomic::AtomicBool,
    ack_released: std::sync::atomic::AtomicBool,
    ack_wakers: Mutex<Vec<Waker>>,
}

impl BlockingStoreState {
    fn release(&self, count: usize) {
        let wakers = {
            let mut control = self.control.lock().unwrap_or_else(|p| p.into_inner());
            control.permits = control.permits.saturating_add(count);
            std::mem::take(&mut control.wakers)
        };
        for waker in wakers {
            waker.wake();
        }
    }

    fn hold_ack(&self) {
        self.hold_ack.store(true, Ordering::Release);
        self.ack_released.store(false, Ordering::Release);
    }

    fn release_ack(&self) {
        self.ack_released.store(true, Ordering::Release);
        let wakers = {
            let mut wakers = self.ack_wakers.lock().unwrap_or_else(|p| p.into_inner());
            std::mem::take(&mut *wakers)
        };
        for waker in wakers {
            waker.wake();
        }
    }
}

struct BlockingModelsStore {
    state: Arc<BlockingStoreState>,
}

struct BlockingStoreFuture {
    state: Arc<BlockingStoreState>,
    provider: ProviderId,
    action: Option<BlockingAction>,
    cancellation: CancellationToken,
    admitted: bool,
    effect_applied: bool,
    label: Option<&'static str>,
}

impl Future for BlockingStoreFuture {
    type Output = Result<(), ModelsStoreError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut TaskContext<'_>) -> Poll<Self::Output> {
        if !self.admitted {
            if self.cancellation.is_cancelled() {
                return Poll::Ready(Err(ModelsStoreError::new(
                    pi_ai_core::ModelsStoreErrorCode::Cancelled,
                )));
            }
            self.admitted = true;
            let label = match self.action.as_ref().expect("action") {
                BlockingAction::Set(_) => "set",
                BlockingAction::Delete => "delete",
            };
            self.state
                .admissions
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push((self.provider.clone(), label));
            let active = self.state.active.fetch_add(1, Ordering::AcqRel) + 1;
            self.state.max_active.fetch_max(active, Ordering::AcqRel);
        }
        if self.effect_applied {
            if self.state.hold_ack.load(Ordering::Acquire)
                && !self.state.ack_released.load(Ordering::Acquire)
            {
                self.state
                    .ack_wakers
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .push(context.waker().clone());
                return Poll::Pending;
            }
            self.state.active.fetch_sub(1, Ordering::AcqRel);
            let label = self.label.take().expect("label");
            self.state
                .completions
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push((self.provider.clone(), label));
            return Poll::Ready(Ok(()));
        }
        let effect_permitted = {
            let mut control = self.state.control.lock().unwrap_or_else(|p| p.into_inner());
            if control.permits == 0 {
                control.wakers.push(context.waker().clone());
                false
            } else {
                control.permits -= 1;
                true
            }
        };
        if !effect_permitted {
            return Poll::Pending;
        }
        let action = self.action.take().expect("one completion");
        let label = match &action {
            BlockingAction::Set(_) => "set",
            BlockingAction::Delete => "delete",
        };
        {
            let mut entries = self.state.entries.lock().unwrap_or_else(|p| p.into_inner());
            match action {
                BlockingAction::Set(entry) => {
                    entries.insert(self.provider.clone(), entry);
                }
                BlockingAction::Delete => {
                    entries.remove(&self.provider);
                }
            }
        }
        self.effect_applied = true;
        self.label = Some(label);
        if self.state.hold_ack.load(Ordering::Acquire)
            && !self.state.ack_released.load(Ordering::Acquire)
        {
            self.state
                .ack_wakers
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push(context.waker().clone());
            Poll::Pending
        } else {
            self.state.active.fetch_sub(1, Ordering::AcqRel);
            let label = self.label.take().expect("label");
            self.state
                .completions
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push((self.provider.clone(), label));
            Poll::Ready(Ok(()))
        }
    }
}

impl ModelsStore for BlockingModelsStore {
    fn read<'a>(
        &'a self,
        provider: &'a ProviderId,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<RawModelsStoreEntry>, ModelsStoreError>> {
        Box::pin(async move {
            if cancellation.is_cancelled() {
                return Err(ModelsStoreError::new(
                    pi_ai_core::ModelsStoreErrorCode::Cancelled,
                ));
            }
            self.state.reads.fetch_add(1, Ordering::AcqRel);
            Ok(self
                .state
                .entries
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .get(provider)
                .map(|entry| RawModelsStoreEntry {
                    models: entry.models.iter().map(RawModel::from).collect(),
                    last_modified: entry.last_modified,
                    checked_at: entry.checked_at,
                    etag: entry.etag.clone(),
                }))
        })
    }

    fn write<'a>(
        &'a self,
        provider: &'a ProviderId,
        entry: ModelsStoreEntry,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), ModelsStoreError>> {
        Box::pin(BlockingStoreFuture {
            state: Arc::clone(&self.state),
            provider: provider.clone(),
            action: Some(BlockingAction::Set(entry)),
            cancellation: cancellation.clone(),
            admitted: false,
            effect_applied: false,
            label: None,
        })
    }

    fn delete<'a>(
        &'a self,
        provider: &'a ProviderId,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), ModelsStoreError>> {
        Box::pin(BlockingStoreFuture {
            state: Arc::clone(&self.state),
            provider: provider.clone(),
            action: Some(BlockingAction::Delete),
            cancellation: cancellation.clone(),
            admitted: false,
            effect_applied: false,
            label: None,
        })
    }
}

struct PhaseRecord {
    allow_network: bool,
    credential_present: bool,
    stored_present: bool,
    force: Option<bool>,
}

struct TestRefresher {
    phases: Mutex<Vec<PhaseRecord>>,
    publications: Mutex<Vec<ModelsPublication>>,
    error: bool,
}

#[derive(Default)]
struct ControlledErrorState {
    ready: std::sync::atomic::AtomicBool,
    dropped: std::sync::atomic::AtomicBool,
}

struct ControlledErrorFuture<'a> {
    state: &'a ControlledErrorState,
}

impl Future for ControlledErrorFuture<'_> {
    type Output = Result<(), ModelsError>;

    fn poll(self: Pin<&mut Self>, _context: &mut TaskContext<'_>) -> Poll<Self::Output> {
        if self.state.ready.load(Ordering::Acquire) {
            Poll::Ready(Err(ModelsError::new(ModelsErrorCode::ModelSource)))
        } else {
            Poll::Pending
        }
    }
}

impl Drop for ControlledErrorFuture<'_> {
    fn drop(&mut self) {
        self.state.dropped.store(true, Ordering::Release);
    }
}

struct ControlledErrorRefresher {
    state: Arc<ControlledErrorState>,
}

impl ProviderModelsRefresher for ControlledErrorRefresher {
    fn refresh_models<'a>(
        &'a self,
        _context: pi_ai_core::RefreshModelsContext<'a>,
    ) -> BoxFuture<'a, Result<(), ModelsError>> {
        Box::pin(ControlledErrorFuture { state: &self.state })
    }
}

struct ControlledErrorProvider {
    inner: TestProvider,
    refresher: ControlledErrorRefresher,
}

impl Provider for ControlledErrorProvider {
    fn id(&self) -> &ProviderId {
        &self.inner.id
    }
    fn name(&self) -> &str {
        &self.inner.name
    }
    fn auth(&self) -> &ProviderAuth {
        &self.inner.auth
    }
    fn models(&self) -> Result<Arc<[Model]>, ModelsError> {
        self.inner.models()
    }
    fn models_refresher(&self) -> Option<&(dyn ProviderModelsRefresher + '_)> {
        Some(&self.refresher)
    }
    fn adapter(&self, api: &ApiId) -> Option<Arc<dyn ApiAdapter>> {
        self.inner.adapter(api)
    }
}

impl TestRefresher {
    fn new(publications: Vec<ModelsPublication>) -> Self {
        Self {
            phases: Mutex::new(Vec::new()),
            publications: Mutex::new(publications),
            error: false,
        }
    }
}

impl ProviderModelsRefresher for TestRefresher {
    fn refresh_models<'a>(
        &'a self,
        context: pi_ai_core::RefreshModelsContext<'a>,
    ) -> BoxFuture<'a, Result<(), ModelsError>> {
        Box::pin(async move {
            self.phases
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push(PhaseRecord {
                    allow_network: context.allow_network,
                    credential_present: context.credential.is_some(),
                    stored_present: context.stored.is_some(),
                    force: context.force,
                });
            if self.error {
                return Err(ModelsError::new(ModelsErrorCode::ModelSource));
            }
            let publications = {
                let mut publications = self.publications.lock().unwrap_or_else(|p| p.into_inner());
                std::mem::take(&mut *publications)
            };
            for publication in publications {
                let outcome = context.publisher.publish(publication).await?;
                if outcome == PublicationOutcome::Superseded {
                    break;
                }
            }
            Ok(())
        })
    }
}

#[derive(Default)]
struct AdapterLog {
    roots: Mutex<Vec<String>>,
    models: Mutex<Vec<ModelId>>,
    endpoints: Mutex<Vec<String>>,
    headers: Mutex<Vec<ProviderHeaders>>,
    api_keys: Mutex<Vec<Option<String>>>,
    environments: Mutex<Vec<ProviderEnv>>,
    option_markers: Mutex<Vec<Option<u64>>>,
    runtime_clocks: Mutex<Vec<usize>>,
    runtime_spawners: Mutex<Vec<usize>>,
}

struct TestAdapter {
    api: ApiId,
    log: Arc<AdapterLog>,
    fetch_supported: bool,
    cancel_supported: bool,
}

fn terminal_stream(
    model: Model,
    clock: Arc<dyn Clock>,
    cancellation: CancellationToken,
) -> AssistantStream {
    let (stream, producer) = create_assistant_message_event_stream(AssistantStreamInit {
        api: model.api.clone(),
        provider: model.provider.clone(),
        model: model.id.clone(),
        clock,
        cancellation,
    });
    producer
        .push(AssistantMessageEvent::Done {
            reason: DoneReason::Stop,
            message: Box::new(AssistantMessage {
                role: AssistantRole::Assistant,
                content: Vec::new(),
                api: model.api,
                provider: model.provider,
                model: model.id,
                response_model: None,
                response_id: None,
                diagnostics: None,
                usage: Usage::zero(),
                stop_reason: StopReason::Stop,
                deferred: None,
                error_message: None,
                raw_stop_reason: None,
                end_turn: None,
                timestamp: SafeUnsigned::new(1).expect("safe"),
            }),
        })
        .expect("terminal");
    stream
}

impl TestAdapter {
    fn record(
        &self,
        root: &str,
        resolved: &pi_ai_core::ResolvedRequest<Model>,
        auth: &ResolvedAuth,
        runtime: &RequestRuntime,
        option_marker: Option<SafeUnsigned>,
    ) {
        self.log
            .roots
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(root.to_owned());
        self.log
            .models
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(resolved.model.id.clone());
        self.log
            .endpoints
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(resolved.endpoint.expose_for_transport().to_owned());
        self.log
            .headers
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(auth.headers.clone());
        self.log
            .api_keys
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(
                auth.api_key
                    .as_ref()
                    .map(|value| value.expose_secret().to_owned()),
            );
        self.log
            .environments
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(auth.env.clone());
        self.log
            .option_markers
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(option_marker.map(SafeUnsigned::get));
        self.log
            .runtime_clocks
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(Arc::as_ptr(&runtime.clock) as *const () as usize);
        self.log
            .runtime_spawners
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(Arc::as_ptr(&runtime.spawner) as *const () as usize);
    }
}

impl ApiAdapter for TestAdapter {
    fn api_id(&self) -> &ApiId {
        &self.api
    }

    fn stream(&self, request: TextStreamRequest) -> AssistantStream {
        self.record(
            "stream",
            &request.resolved,
            &request.auth,
            &request.options.runtime,
            request.options.wire.timeout_ms,
        );
        terminal_stream(
            request.resolved.model,
            request.options.runtime.clock,
            request.options.runtime.cancellation,
        )
    }

    fn stream_simple(&self, request: SimpleStreamRequest) -> AssistantStream {
        self.record(
            "simple",
            &request.resolved,
            &request.auth,
            &request.options.runtime,
            request.options.wire.stream.timeout_ms,
        );
        terminal_stream(
            request.resolved.model,
            request.options.runtime.clock,
            request.options.runtime.cancellation,
        )
    }

    fn fetch_deferred(
        &self,
        request: DeferredFetchRequest,
    ) -> Result<AssistantStream, CapabilityError> {
        if !self.fetch_supported {
            return Err(CapabilityError::Unsupported);
        }
        self.record(
            "fetch",
            &request.resolved,
            &request.auth,
            &request.options.runtime,
            request.options.wire.timeout_ms,
        );
        Ok(terminal_stream(
            request.resolved.model,
            request.options.runtime.clock,
            request.options.runtime.cancellation,
        ))
    }

    fn cancel_deferred<'a>(
        &'a self,
        request: DeferredCancelRequest,
    ) -> BoxFuture<'a, Result<(), ModelsError>> {
        Box::pin(async move {
            if !self.cancel_supported {
                return Err(ModelsError::new(ModelsErrorCode::Provider));
            }
            self.record(
                "cancel",
                &request.resolved,
                &request.auth,
                &request.options.runtime,
                request.options.wire.timeout_ms,
            );
            Ok(())
        })
    }
}

struct TestProvider {
    id: ProviderId,
    name: String,
    auth: ProviderAuth,
    models: Mutex<Result<Arc<[Model]>, ModelsErrorCode>>,
    refresher: Option<Arc<TestRefresher>>,
    adapter: Option<Arc<TestAdapter>>,
    filter_calls: AtomicUsize,
    filtered_credentials: Mutex<Vec<Option<Credential>>>,
}

struct DropProbeProvider {
    inner: TestProvider,
    on_drop: Mutex<Option<Reentry>>,
}

impl Provider for DropProbeProvider {
    fn id(&self) -> &ProviderId {
        &self.inner.id
    }
    fn name(&self) -> &str {
        &self.inner.name
    }
    fn auth(&self) -> &ProviderAuth {
        &self.inner.auth
    }
    fn models(&self) -> Result<Arc<[Model]>, ModelsError> {
        self.inner.models()
    }
    fn adapter(&self, api: &ApiId) -> Option<Arc<dyn ApiAdapter>> {
        self.inner.adapter(api)
    }
}

impl Drop for DropProbeProvider {
    fn drop(&mut self) {
        if let Some(callback) = self
            .on_drop
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take()
        {
            callback();
        }
    }
}

impl Provider for TestProvider {
    fn id(&self) -> &ProviderId {
        &self.id
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn auth(&self) -> &ProviderAuth {
        &self.auth
    }

    fn models(&self) -> Result<Arc<[Model]>, ModelsError> {
        match &*self.models.lock().unwrap_or_else(|p| p.into_inner()) {
            Ok(models) => Ok(models.clone()),
            Err(code) => Err(ModelsError::new(*code)),
        }
    }

    fn models_refresher(&self) -> Option<&(dyn ProviderModelsRefresher + '_)> {
        self.refresher
            .as_deref()
            .map(|value| value as &dyn ProviderModelsRefresher)
    }

    fn filter_models(&self, models: &[Model], credential: Option<&Credential>) -> Vec<Model> {
        self.filter_calls.fetch_add(1, Ordering::AcqRel);
        self.filtered_credentials
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(credential.cloned());
        if credential.is_some() {
            models.to_vec()
        } else {
            Vec::new()
        }
    }

    fn adapter(&self, api: &ApiId) -> Option<Arc<dyn ApiAdapter>> {
        self.adapter.as_ref().and_then(|adapter| {
            (adapter.api_id() == api).then(|| Arc::clone(adapter) as Arc<dyn ApiAdapter>)
        })
    }
}

fn model(provider: &str, id: &str) -> Model {
    let value: serde_json::Value = serde_json::from_str(MODEL_FIXTURE).expect("fixture");
    let mut model: Model = serde_json::from_value(value["model"].clone()).expect("model");
    model.provider = ProviderId::new(provider).expect("provider");
    model.id = ModelId::new(id).expect("model id");
    model
}

fn provider(
    id: &str,
    models: Vec<Model>,
    auth: Arc<TestApiKey>,
    refresher: Option<Arc<TestRefresher>>,
    adapter: Option<Arc<TestAdapter>>,
) -> Arc<TestProvider> {
    Arc::new(TestProvider {
        id: ProviderId::new(id).expect("provider"),
        name: id.to_owned(),
        auth: ProviderAuth::new(Some(auth), None).expect("auth"),
        models: Mutex::new(Ok(models.into())),
        refresher,
        adapter,
        filter_calls: AtomicUsize::new(0),
        filtered_credentials: Mutex::new(Vec::new()),
    })
}

fn oauth_provider(
    id: &str,
    oauth: Arc<TestOAuth>,
    refresher: Option<Arc<TestRefresher>>,
) -> Arc<TestProvider> {
    Arc::new(TestProvider {
        id: ProviderId::new(id).expect("provider"),
        name: id.to_owned(),
        auth: ProviderAuth::new(None, Some(oauth)).expect("auth"),
        models: Mutex::new(Ok(Vec::<Model>::new().into())),
        refresher,
        adapter: None,
        filter_calls: AtomicUsize::new(0),
        filtered_credentials: Mutex::new(Vec::new()),
    })
}

struct Fixture {
    registry: ModelsRegistry,
    spawner: Arc<ManualSpawner>,
    store: Arc<StoreState>,
    credentials: Arc<InMemoryCredentialStore>,
    clock: Arc<TestClock>,
}

struct BlockingFixture {
    registry: ModelsRegistry,
    spawner: Arc<ManualSpawner>,
    store: Arc<BlockingStoreState>,
}

struct ReentrantBlockingFixture {
    registry: Arc<ModelsRegistry>,
    spawner: Arc<ReentrantSpawner>,
    store: Arc<BlockingStoreState>,
}

struct BlockingOAuthFixture {
    registry: Arc<ModelsRegistry>,
    spawner: Arc<ManualSpawner>,
    credentials: Arc<BlockingCredentialState>,
    oauth: Arc<TestOAuth>,
}

fn fixture(policy: ModelHeaderPolicy) -> Fixture {
    let spawner = Arc::new(ManualSpawner::default());
    let store = Arc::new(StoreState::default());
    let credentials = Arc::new(InMemoryCredentialStore::new());
    let clock = Arc::new(TestClock::new(100));
    let registry = ModelsRegistry::new(ModelsRegistryOptions {
        credentials: credentials.clone(),
        models_store: Box::new(TestModelsStore {
            state: Arc::clone(&store),
        }),
        auth_context: Arc::new(EmptyContext),
        clock: clock.clone(),
        spawner: spawner.clone(),
        model_header_policy: policy,
    });
    Fixture {
        registry,
        spawner,
        store,
        credentials,
        clock,
    }
}

fn blocking_fixture() -> BlockingFixture {
    let spawner = Arc::new(ManualSpawner::default());
    let store = Arc::new(BlockingStoreState::default());
    let registry = ModelsRegistry::new(ModelsRegistryOptions {
        credentials: Arc::new(InMemoryCredentialStore::new()),
        models_store: Box::new(BlockingModelsStore {
            state: Arc::clone(&store),
        }),
        auth_context: Arc::new(EmptyContext),
        clock: Arc::new(TestClock::new(100)),
        spawner: spawner.clone(),
        model_header_policy: ModelHeaderPolicy::default(),
    });
    BlockingFixture {
        registry,
        spawner,
        store,
    }
}

fn reentrant_blocking_fixture() -> ReentrantBlockingFixture {
    let spawner = Arc::new(ReentrantSpawner::default());
    let store = Arc::new(BlockingStoreState::default());
    let registry = Arc::new(ModelsRegistry::new(ModelsRegistryOptions {
        credentials: Arc::new(InMemoryCredentialStore::new()),
        models_store: Box::new(BlockingModelsStore {
            state: Arc::clone(&store),
        }),
        auth_context: Arc::new(EmptyContext),
        clock: Arc::new(TestClock::new(100)),
        spawner: spawner.clone(),
        model_header_policy: ModelHeaderPolicy::default(),
    }));
    ReentrantBlockingFixture {
        registry,
        spawner,
        store,
    }
}

fn blocking_oauth_fixture() -> BlockingOAuthFixture {
    let spawner = Arc::new(ManualSpawner::default());
    let credentials = Arc::new(BlockingCredentialState::default());
    *credentials.value.lock().unwrap_or_else(|p| p.into_inner()) = Some(VersionedCredential {
        revision: CredentialRevision::FIRST,
        credential: Credential::OAuth(oauth_credential("expired", 100)),
    });
    let oauth = Arc::new(TestOAuth {
        refresh_calls: AtomicUsize::new(0),
        login_error: false,
        refresh_callback: Mutex::new(None),
    });
    let registry = Arc::new(ModelsRegistry::new(ModelsRegistryOptions {
        credentials: Arc::new(BlockingCredentialStore {
            state: credentials.clone(),
        }),
        models_store: Box::new(TestModelsStore {
            state: Arc::new(StoreState::default()),
        }),
        auth_context: Arc::new(EmptyContext),
        clock: Arc::new(TestClock::new(100)),
        spawner: spawner.clone(),
        model_header_policy: ModelHeaderPolicy::default(),
    }));
    drive(
        registry.set_provider(oauth_provider(
            "oauth",
            oauth.clone(),
            Some(Arc::new(TestRefresher::new(Vec::new()))),
        )),
        &spawner,
    )
    .expect("set");
    BlockingOAuthFixture {
        registry,
        spawner,
        credentials,
        oauth,
    }
}

fn set_blocking_provider(fixture: &BlockingFixture, provider: Arc<dyn Provider>) {
    drive(fixture.registry.set_provider(provider), &fixture.spawner).expect("set");
}

fn set_blocking_provider_reentrant(
    fixture: &ReentrantBlockingFixture,
    provider: Arc<dyn Provider>,
) {
    drive_reentrant(fixture.registry.set_provider(provider), &fixture.spawner).expect("set");
}

fn set_provider(fixture: &Fixture, provider: Arc<TestProvider>) {
    drive(
        fixture.registry.set_provider(provider as Arc<dyn Provider>),
        &fixture.spawner,
    )
    .expect("set provider");
}

fn set_credential(fixture: &Fixture, provider: &str) {
    let provider = ProviderId::new(provider).expect("provider");
    let token = CancellationToken::new();
    let lease = ready(fixture.credentials.begin_mutation(&provider, &token)).expect("lease");
    ready(
        lease.commit(CredentialMutationValue::Set(Credential::ApiKey(
            ApiKeyCredential {
                credential_type: ApiKeyCredentialType::ApiKey,
                key: Some(SecretString::new("stored")),
                env: None,
            },
        ))),
    )
    .expect("commit");
}

fn empty_context() -> Context {
    Context {
        system_prompt: None,
        messages: Vec::new(),
        tools: None,
    }
}

fn assert_setup_terminal(result: &AssistantMessage, message: &str) {
    assert_eq!(result.stop_reason, StopReason::Error);
    assert_eq!(result.error_message.as_deref(), Some(message));
    assert!(!format!("{result:?}").contains(SECRET_SENTINEL));
}

fn runtime(spawner: Arc<ManualSpawner>, clock: Arc<TestClock>) -> RequestRuntime {
    let telemetry: Arc<dyn TelemetryContext> = Arc::new(NOOP_TELEMETRY_CONTEXT);
    RequestRuntime {
        cancellation: CancellationToken::new(),
        telemetry,
        clock,
        spawner,
    }
}

fn handle(model: &Model) -> DeferredHandle {
    DeferredHandle {
        provider: model.provider.clone(),
        model_id: model.id.clone(),
        api: model.api.clone(),
        id: DeferredId::new("deferred").expect("id"),
        expires_at: None,
        poll_after_ms: None,
        data: pi_ai_core::Presence::Missing,
    }
}

fn store_entry(model: Model, tag: &str) -> ModelsStoreEntry {
    ModelsStoreEntry {
        models: vec![model],
        last_modified: None,
        checked_at: None,
        etag: Some(tag.to_owned()),
    }
}

struct OutcomeRefresher {
    publications: Mutex<Vec<ModelsPublication>>,
    outcomes: Mutex<Vec<PublicationOutcome>>,
}

impl OutcomeRefresher {
    fn new(publications: Vec<ModelsPublication>) -> Self {
        Self {
            publications: Mutex::new(publications),
            outcomes: Mutex::new(Vec::new()),
        }
    }
}

impl ProviderModelsRefresher for OutcomeRefresher {
    fn refresh_models<'a>(
        &'a self,
        context: pi_ai_core::RefreshModelsContext<'a>,
    ) -> BoxFuture<'a, Result<(), ModelsError>> {
        Box::pin(async move {
            let publications = {
                let mut values = self.publications.lock().unwrap_or_else(|p| p.into_inner());
                std::mem::take(&mut *values)
            };
            for publication in publications {
                let outcome = context.publisher.publish(publication).await?;
                self.outcomes
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .push(outcome);
                if outcome == PublicationOutcome::Superseded {
                    break;
                }
            }
            Ok(())
        })
    }
}

struct OutcomeProvider {
    inner: TestProvider,
    refresher: Arc<OutcomeRefresher>,
}

struct FenceRestoreRefresher {
    calls: AtomicUsize,
    restored: Mutex<Vec<ModelId>>,
}

impl ProviderModelsRefresher for FenceRestoreRefresher {
    fn refresh_models<'a>(
        &'a self,
        context: pi_ai_core::RefreshModelsContext<'a>,
    ) -> BoxFuture<'a, Result<(), ModelsError>> {
        Box::pin(async move {
            let call = self.calls.fetch_add(1, Ordering::AcqRel);
            if call == 0 {
                let _ = context
                    .publisher
                    .publish(ModelsPublication {
                        memory: ModelsMemoryIntent::Keep,
                        persistence: ModelsPersistenceIntent::Set(store_entry(
                            model("p", "definitive"),
                            "definitive",
                        )),
                    })
                    .await?;
            } else if let Some(stored) = context.stored {
                self.restored
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .extend(stored.models.iter().map(|model| model.id.clone()));
                let _ = context
                    .publisher
                    .publish(ModelsPublication {
                        memory: ModelsMemoryIntent::Set(stored.models.clone().into()),
                        persistence: ModelsPersistenceIntent::Keep,
                    })
                    .await?;
            }
            Ok(())
        })
    }
}

struct FenceRestoreProvider {
    inner: TestProvider,
    refresher: Arc<FenceRestoreRefresher>,
}

impl Provider for FenceRestoreProvider {
    fn id(&self) -> &ProviderId {
        &self.inner.id
    }
    fn name(&self) -> &str {
        &self.inner.name
    }
    fn auth(&self) -> &ProviderAuth {
        &self.inner.auth
    }
    fn models(&self) -> Result<Arc<[Model]>, ModelsError> {
        self.inner.models()
    }
    fn models_refresher(&self) -> Option<&(dyn ProviderModelsRefresher + '_)> {
        Some(self.refresher.as_ref())
    }
    fn adapter(&self, api: &ApiId) -> Option<Arc<dyn ApiAdapter>> {
        self.inner.adapter(api)
    }
}

impl Provider for OutcomeProvider {
    fn id(&self) -> &ProviderId {
        &self.inner.id
    }
    fn name(&self) -> &str {
        &self.inner.name
    }
    fn auth(&self) -> &ProviderAuth {
        &self.inner.auth
    }
    fn models(&self) -> Result<Arc<[Model]>, ModelsError> {
        self.inner.models()
    }
    fn models_refresher(&self) -> Option<&(dyn ProviderModelsRefresher + '_)> {
        Some(self.refresher.as_ref())
    }
    fn adapter(&self, api: &ApiId) -> Option<Arc<dyn ApiAdapter>> {
        self.inner.adapter(api)
    }
}

fn outcome_provider(id: &str, refresher: Arc<OutcomeRefresher>) -> Arc<OutcomeProvider> {
    let base = provider(
        id,
        Vec::new(),
        Arc::new(TestApiKey::configured()),
        None,
        None,
    );
    Arc::new(OutcomeProvider {
        inner: Arc::try_unwrap(base).ok().expect("unique provider"),
        refresher,
    })
}

#[test]
fn provider_mutations_preserve_order_and_slots() {
    let fixture = fixture(ModelHeaderPolicy::default());
    let auth = Arc::new(TestApiKey::configured());
    let a = provider("a", vec![model("a", "a1")], auth.clone(), None, None);
    let b = provider("b", vec![model("b", "b1")], auth.clone(), None, None);
    set_provider(&fixture, a);
    set_provider(&fixture, b);
    let replacement = provider("a", vec![model("a", "a2")], auth, None, None);
    set_provider(&fixture, replacement);
    assert_eq!(
        fixture
            .registry
            .get_providers()
            .iter()
            .map(|provider| provider.id().as_str())
            .collect::<Vec<_>>(),
        ["a", "b"]
    );
    drive(
        fixture
            .registry
            .delete_provider(&ProviderId::new("a").expect("id")),
        &fixture.spawner,
    )
    .expect("delete");
    set_provider(
        &fixture,
        provider(
            "a",
            vec![model("a", "a3")],
            Arc::new(TestApiKey::configured()),
            None,
            None,
        ),
    );
    assert_eq!(
        fixture
            .registry
            .get_providers()
            .iter()
            .map(|provider| provider.id().as_str())
            .collect::<Vec<_>>(),
        ["b", "a"]
    );
    drive(fixture.registry.clear_providers(), &fixture.spawner).expect("clear");
    assert!(fixture.registry.get_providers().is_empty());
    fixture.spawner.set_fail(true);
    let error = ready(fixture.registry.set_provider(provider(
        "spawn-fail",
        Vec::new(),
        Arc::new(TestApiKey::configured()),
        None,
        None,
    )))
    .expect_err("spawn failure");
    assert_eq!(error.code(), ModelsErrorCode::ModelSource);
    let spawn = error.source().expect("spawn source retained");
    assert_eq!(spawn.to_string(), "task could not be spawned");
    assert_eq!(
        spawn.source().expect("backend source retained").to_string(),
        SECRET_SENTINEL
    );
    assert!(!format!("{error:?}").contains(SECRET_SENTINEL));
    fixture.spawner.set_fail(false);

    let spawner = Arc::new(ManualSpawner::default());
    spawner.set_fail(true);
    let registry = Arc::new(ModelsRegistry::new(ModelsRegistryOptions {
        credentials: Arc::new(InMemoryCredentialStore::new()),
        models_store: Box::new(TestModelsStore {
            state: Arc::new(StoreState::default()),
        }),
        auth_context: Arc::new(EmptyContext),
        clock: Arc::new(TestClock::new(1)),
        spawner,
        model_header_policy: ModelHeaderPolicy::default(),
    }));
    let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let callback_registry = registry.clone();
    let callback_dropped = dropped.clone();
    let base = provider(
        "drop-probe",
        Vec::new(),
        Arc::new(TestApiKey::configured()),
        None,
        None,
    );
    let drop_provider = Arc::new(DropProbeProvider {
        inner: Arc::try_unwrap(base).ok().expect("unique"),
        on_drop: Mutex::new(Some(Box::new(move || {
            assert!(callback_registry.get_providers().is_empty());
            callback_dropped.store(true, Ordering::Release);
        }))),
    });
    assert!(ready(registry.set_provider(drop_provider)).is_err());
    assert!(dropped.load(Ordering::Acquire));

    for fail_outer in [false, true] {
        let spawner = Arc::new(ReentrantSpawner::default());
        spawner.fail_outer.store(fail_outer, Ordering::Release);
        let registry = Arc::new(ModelsRegistry::new(ModelsRegistryOptions {
            credentials: Arc::new(InMemoryCredentialStore::new()),
            models_store: Box::new(TestModelsStore {
                state: Arc::new(StoreState::default()),
            }),
            auth_context: Arc::new(EmptyContext),
            clock: Arc::new(TestClock::new(1)),
            spawner: spawner.clone(),
            model_header_policy: ModelHeaderPolicy::default(),
        }));
        let callback_registry = registry.clone();
        let survivor = provider(
            "reentrant",
            vec![model("reentrant", "inner")],
            Arc::new(TestApiKey::configured()),
            None,
            None,
        );
        *spawner.callback.lock().unwrap_or_else(|p| p.into_inner()) = Some(Box::new(move || {
            let mut mutation = Box::pin(callback_registry.set_provider(survivor));
            assert!(poll(mutation.as_mut()).is_pending());
            drop(mutation);
        }));
        let outer = provider(
            "outer",
            vec![model("outer", "outer")],
            Arc::new(TestApiKey::configured()),
            None,
            None,
        );
        let result = drive_reentrant(registry.set_provider(outer), &spawner);
        if fail_outer {
            assert!(result.is_err());
        } else {
            result.expect("outer admitted");
        }
        for _ in 0..8 {
            spawner.drive_once();
        }
        let ids = registry
            .get_providers()
            .iter()
            .map(|provider| provider.id().as_str().to_owned())
            .collect::<Vec<_>>();
        if fail_outer {
            assert_eq!(ids, ["reentrant"]);
        } else {
            assert_eq!(ids, ["reentrant", "outer"]);
        }
    }
}

#[test]
fn listing_lookup_and_provider_failure_isolation() {
    let fixture = fixture(ModelHeaderPolicy::default());
    let good = provider(
        "good",
        vec![model("good", "one")],
        Arc::new(TestApiKey::configured()),
        None,
        None,
    );
    let bad = provider(
        "bad",
        Vec::new(),
        Arc::new(TestApiKey::configured()),
        None,
        None,
    );
    *bad.models.lock().unwrap_or_else(|p| p.into_inner()) = Err(ModelsErrorCode::ModelSource);
    set_provider(&fixture, good);
    set_provider(&fixture, bad);
    assert_eq!(fixture.registry.get_models(None).len(), 1);
    assert!(
        fixture
            .registry
            .get_model(
                &ProviderId::new("good").expect("id"),
                &ModelId::new("one").expect("id")
            )
            .is_some()
    );
    assert!(
        fixture
            .registry
            .get_models(Some(&ProviderId::new("bad").expect("id")))
            .is_empty()
    );
}

#[test]
fn check_auth_and_available_filter_without_oauth_refresh() {
    let fixture = fixture(ModelHeaderPolicy::default());
    let auth = Arc::new(TestApiKey::configured());
    let provider = provider("p", vec![model("p", "one")], auth.clone(), None, None);
    set_provider(&fixture, provider.clone());
    set_credential(&fixture, "p");
    let token = CancellationToken::new();
    assert!(
        ready(fixture.registry.check_auth(provider.id(), &token))
            .expect("check")
            .is_some()
    );
    let available = ready(fixture.registry.get_available(None, &token)).expect("available");
    assert_eq!(available.len(), 1);
    assert_eq!(auth.check_calls.load(Ordering::Acquire), 2);
    assert_eq!(provider.filter_calls.load(Ordering::Acquire), 1);
    assert_eq!(
        provider
            .filtered_credentials
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_slice(),
        [Some(Credential::ApiKey(ApiKeyCredential {
            credential_type: ApiKeyCredentialType::ApiKey,
            key: Some(SecretString::new("stored")),
            env: None,
        }))]
    );

    let oauth = Arc::new(TestOAuth {
        refresh_calls: AtomicUsize::new(0),
        login_error: false,
        refresh_callback: Mutex::new(None),
    });
    let oauth_provider = oauth_provider("oauth", oauth.clone(), None);
    set_provider(&fixture, oauth_provider.clone());
    *oauth_provider
        .models
        .lock()
        .unwrap_or_else(|p| p.into_inner()) = Ok(vec![model("oauth", "oauth-model")].into());
    let oauth_id = ProviderId::new("oauth").expect("id");
    let lease = ready(fixture.credentials.begin_mutation(&oauth_id, &token)).expect("lease");
    ready(lease.commit(CredentialMutationValue::Set(Credential::OAuth(
        oauth_credential("expired", 0),
    ))))
    .expect("commit");
    let check = ready(fixture.registry.check_auth(&oauth_id, &token))
        .expect("check")
        .expect("configured");
    assert_eq!(check.auth_type, AuthType::OAuth);
    assert_eq!(oauth.refresh_calls.load(Ordering::Acquire), 0);
}

#[test]
fn offline_refresh_stops_after_cache_without_auth_or_network() {
    {
        let offline_fixture = fixture(ModelHeaderPolicy::default());
        let refresher = Arc::new(TestRefresher::new(Vec::new()));
        let dynamic_provider = provider(
            "p",
            Vec::new(),
            Arc::new(TestApiKey::configured()),
            Some(refresher.clone()),
            None,
        );
        set_provider(&offline_fixture, dynamic_provider);
        let result = drive(
            offline_fixture.registry.refresh(
                ModelsRefreshOptions {
                    allow_network: false,
                    providers: None,
                    force: Some(true),
                },
                &CancellationToken::new(),
            ),
            &offline_fixture.spawner,
        );
        assert!(!result.aborted);
        assert!(result.errors.is_empty());
        let phases = refresher.phases.lock().unwrap_or_else(|p| p.into_inner());
        assert_eq!(phases.len(), 1);
        assert!(!phases[0].allow_network);
        assert!(!phases[0].credential_present);
        assert!(!phases[0].stored_present);
        assert_eq!(offline_fixture.store.reads.load(Ordering::Acquire), 1);
    }

    let fixture = fixture(ModelHeaderPolicy::default());
    let auth = Arc::new(TestApiKey::configured());
    set_provider(
        &fixture,
        provider("static", Vec::new(), auth.clone(), None, None),
    );
    let result = drive(
        fixture.registry.refresh(
            ModelsRefreshOptions {
                allow_network: false,
                providers: Some(vec![ProviderId::new("static").expect("id")]),
                force: None,
            },
            &CancellationToken::new(),
        ),
        &fixture.spawner,
    );
    assert!(!result.aborted && result.errors.is_empty());
    assert_eq!(fixture.store.reads.load(Ordering::Acquire), 0);
    assert_eq!(auth.resolve_calls.load(Ordering::Acquire), 0);
}

#[test]
fn unconfigured_refresh_restores_cache_then_skips_network() {
    let fixture = fixture(ModelHeaderPolicy::default());
    let refresher = Arc::new(TestRefresher::new(Vec::new()));
    let auth = Arc::new(TestApiKey {
        configured: false,
        check_calls: AtomicUsize::new(0),
        resolve_calls: AtomicUsize::new(0),
        login_error: false,
        auth: Mutex::new(ModelAuth::default()),
        env: Mutex::new(None),
    });
    set_provider(
        &fixture,
        provider("p", Vec::new(), auth, Some(refresher.clone()), None),
    );
    let result = drive(
        fixture
            .registry
            .refresh(ModelsRefreshOptions::default(), &CancellationToken::new()),
        &fixture.spawner,
    );
    assert!(result.errors.is_empty());
    assert_eq!(
        refresher
            .phases
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .len(),
        1
    );
    assert_eq!(fixture.store.reads.load(Ordering::Acquire), 1);
}

#[test]
fn configured_refresh_runs_cache_then_network_with_effective_credential() {
    let fixture = fixture(ModelHeaderPolicy::default());
    let refresher = Arc::new(TestRefresher::new(Vec::new()));
    set_provider(
        &fixture,
        provider(
            "p",
            Vec::new(),
            Arc::new(TestApiKey::configured()),
            Some(refresher.clone()),
            None,
        ),
    );
    set_credential(&fixture, "p");
    let result = drive(
        fixture.registry.refresh(
            ModelsRefreshOptions {
                force: Some(true),
                ..ModelsRefreshOptions::default()
            },
            &CancellationToken::new(),
        ),
        &fixture.spawner,
    );
    assert!(result.errors.is_empty());
    let phases = refresher.phases.lock().unwrap_or_else(|p| p.into_inner());
    assert_eq!(phases.len(), 2);
    assert!(!phases[0].allow_network);
    assert!(phases[1].allow_network && phases[1].credential_present);
    assert_eq!(phases[1].force, Some(true));
    assert_eq!(fixture.store.reads.load(Ordering::Acquire), 2);

    let fixture = blocking_oauth_fixture();
    let cancellation = CancellationToken::new();
    let mut refresh = pin!(
        fixture
            .registry
            .refresh(ModelsRefreshOptions::default(), &cancellation)
    );
    assert!(poll(refresh.as_mut()).is_pending());
    assert!(fixture.credentials.admitted.load(Ordering::Acquire));
    cancellation.cancel();
    fixture.credentials.release();
    let result = drive(refresh, &fixture.spawner);
    assert!(result.aborted && result.errors.is_empty());
    assert!(matches!(
        fixture
            .credentials
            .value
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .map(|value| &value.credential),
        Some(Credential::OAuth(credential))
            if credential.credentials.access.expose_secret() == "rotated"
    ));

    let fixture = blocking_oauth_fixture();
    let cancellation = CancellationToken::new();
    let mut refresh = pin!(
        fixture
            .registry
            .refresh(ModelsRefreshOptions::default(), &cancellation)
    );
    assert!(poll(refresh.as_mut()).is_pending());
    assert!(fixture.credentials.admitted.load(Ordering::Acquire));
    let replacement = oauth_provider(
        "oauth",
        Arc::new(TestOAuth {
            refresh_calls: AtomicUsize::new(0),
            login_error: false,
            refresh_callback: Mutex::new(None),
        }),
        None,
    );
    let mut replacement = pin!(fixture.registry.set_provider(replacement));
    assert!(poll(replacement.as_mut()).is_pending());
    fixture.credentials.release();
    let result = drive(refresh, &fixture.spawner);
    assert!(!result.aborted && result.errors.is_empty());
    drive(replacement, &fixture.spawner).expect("replacement");
    assert!(matches!(
        fixture
            .credentials
            .value
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .map(|value| &value.credential),
        Some(Credential::OAuth(credential))
            if credential.credentials.access.expose_secret() == "rotated"
    ));

    let fixture = blocking_oauth_fixture();
    let cancellation = CancellationToken::new();
    let callback_cancellation = cancellation.clone();
    *fixture
        .oauth
        .refresh_callback
        .lock()
        .unwrap_or_else(|p| p.into_inner()) = Some(Box::new(move || {
        callback_cancellation.cancel();
    }));
    let result = drive(
        fixture
            .registry
            .refresh(ModelsRefreshOptions::default(), &cancellation),
        &fixture.spawner,
    );
    assert!(result.aborted && result.errors.is_empty());
    assert!(!fixture.credentials.admitted.load(Ordering::Acquire));

    let fixture = blocking_oauth_fixture();
    let cancellation = CancellationToken::new();
    let callback_registry = fixture.registry.clone();
    *fixture
        .oauth
        .refresh_callback
        .lock()
        .unwrap_or_else(|p| p.into_inner()) = Some(Box::new(move || {
        let replacement = oauth_provider(
            "oauth",
            Arc::new(TestOAuth {
                refresh_calls: AtomicUsize::new(0),
                login_error: false,
                refresh_callback: Mutex::new(None),
            }),
            None,
        );
        let mut mutation = Box::pin(callback_registry.set_provider(replacement));
        assert!(poll(mutation.as_mut()).is_pending());
        drop(mutation);
    }));
    let result = drive(
        fixture
            .registry
            .refresh(ModelsRefreshOptions::default(), &cancellation),
        &fixture.spawner,
    );
    assert!(!result.aborted && result.errors.is_empty());
    assert!(!fixture.credentials.admitted.load(Ordering::Acquire));
}

#[test]
fn memory_and_persistence_intents_are_independent_values() {
    for memory_set in [false, true] {
        for persistence_kind in 0..3 {
            let fixture = fixture(ModelHeaderPolicy::default());
            let provider_id = ProviderId::new("p").expect("id");
            fixture
                .store
                .entries
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .insert(
                    provider_id.clone(),
                    store_entry(model("p", "prior"), "prior"),
                );
            let memory = if memory_set {
                ModelsMemoryIntent::Set(vec![model("p", "dynamic")].into())
            } else {
                ModelsMemoryIntent::Keep
            };
            let persistence = match persistence_kind {
                0 => ModelsPersistenceIntent::Keep,
                1 => {
                    ModelsPersistenceIntent::Set(store_entry(model("p", "persisted"), "persisted"))
                }
                2 => ModelsPersistenceIntent::Delete,
                _ => unreachable!(),
            };
            let refresher = Arc::new(TestRefresher::new(vec![ModelsPublication {
                memory,
                persistence,
            }]));
            set_provider(
                &fixture,
                provider(
                    "p",
                    Vec::new(),
                    Arc::new(TestApiKey::configured()),
                    Some(refresher),
                    None,
                ),
            );
            let result = drive(
                fixture.registry.refresh(
                    ModelsRefreshOptions {
                        allow_network: false,
                        providers: None,
                        force: None,
                    },
                    &CancellationToken::new(),
                ),
                &fixture.spawner,
            );
            assert!(result.errors.is_empty());
            assert_eq!(
                fixture.registry.get_models(None).len(),
                usize::from(memory_set)
            );
            match persistence_kind {
                0 => {
                    assert_eq!(fixture.store.writes.load(Ordering::Acquire), 0);
                    assert_eq!(fixture.store.deletes.load(Ordering::Acquire), 0);
                    assert_eq!(
                        fixture
                            .store
                            .entries
                            .lock()
                            .unwrap_or_else(|p| p.into_inner())[&provider_id]
                            .etag
                            .as_deref(),
                        Some("prior")
                    );
                }
                1 => {
                    assert_eq!(fixture.store.writes.load(Ordering::Acquire), 1);
                    assert_eq!(
                        fixture
                            .store
                            .entries
                            .lock()
                            .unwrap_or_else(|p| p.into_inner())[&provider_id]
                            .etag
                            .as_deref(),
                        Some("persisted")
                    );
                }
                2 => {
                    assert_eq!(fixture.store.deletes.load(Ordering::Acquire), 1);
                    assert!(
                        !fixture
                            .store
                            .entries
                            .lock()
                            .unwrap_or_else(|p| p.into_inner())
                            .contains_key(&provider_id)
                    );
                }
                _ => unreachable!(),
            }
        }
    }

    let overlay = fixture(ModelHeaderPolicy::default());
    let refresher = Arc::new(TestRefresher::new(vec![ModelsPublication {
        memory: ModelsMemoryIntent::Set(vec![model("p", "dynamic")].into()),
        persistence: ModelsPersistenceIntent::Keep,
    }]));
    set_provider(
        &overlay,
        provider(
            "p",
            Vec::new(),
            Arc::new(TestApiKey::configured()),
            Some(refresher.clone()),
            None,
        ),
    );
    let offline = ModelsRefreshOptions {
        allow_network: false,
        providers: None,
        force: None,
    };
    assert!(
        drive(
            overlay
                .registry
                .refresh(offline.clone(), &CancellationToken::new()),
            &overlay.spawner,
        )
        .errors
        .is_empty()
    );
    assert_eq!(overlay.registry.get_models(None).len(), 1);
    refresher
        .publications
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .push(ModelsPublication {
            memory: ModelsMemoryIntent::Set(Arc::from([])),
            persistence: ModelsPersistenceIntent::Keep,
        });
    assert!(
        drive(
            overlay.registry.refresh(offline, &CancellationToken::new()),
            &overlay.spawner,
        )
        .errors
        .is_empty()
    );
    assert!(overlay.registry.get_models(None).is_empty());

    let spawner = Arc::new(ManualSpawner::default());
    let registry = ModelsRegistry::new(ModelsRegistryOptions {
        credentials: Arc::new(InMemoryCredentialStore::new()),
        models_store: Box::new(FailingModelsStore),
        auth_context: Arc::new(EmptyContext),
        clock: Arc::new(TestClock::new(1)),
        spawner: spawner.clone(),
        model_header_policy: ModelHeaderPolicy::default(),
    });
    let refresher = Arc::new(TestRefresher::new(vec![ModelsPublication {
        memory: ModelsMemoryIntent::Set(vec![model("p", "must-not-apply")].into()),
        persistence: ModelsPersistenceIntent::Set(store_entry(model("p", "failed"), "failed")),
    }]));
    drive(
        registry.set_provider(provider(
            "p",
            Vec::new(),
            Arc::new(TestApiKey::configured()),
            Some(refresher),
            None,
        )),
        &spawner,
    )
    .expect("set");
    let result = drive(
        registry.refresh(
            ModelsRefreshOptions {
                allow_network: false,
                providers: None,
                force: None,
            },
            &CancellationToken::new(),
        ),
        &spawner,
    );
    let error = result
        .errors
        .get(&ProviderId::new("p").expect("id"))
        .expect("publication error");
    let AuthResolutionError::Models(error) = error else {
        panic!("models error");
    };
    assert_eq!(error.code(), ModelsErrorCode::ModelSource);
    let store_error = error.source().expect("models-store source retained");
    assert_eq!(store_error.to_string(), "models store operation failed");
    assert_eq!(
        store_error.source().expect("backend source").to_string(),
        SECRET_SENTINEL
    );
    assert!(!format!("{error:?}").contains(SECRET_SENTINEL));
    assert!(registry.get_models(None).is_empty());

    let callback = Arc::new(Mutex::new(None));
    let spawner = Arc::new(ManualSpawner::default());
    let registry = Arc::new(ModelsRegistry::new(ModelsRegistryOptions {
        credentials: Arc::new(InMemoryCredentialStore::new()),
        models_store: Box::new(ReentrantFailingModelsStore {
            callback: callback.clone(),
        }),
        auth_context: Arc::new(EmptyContext),
        clock: Arc::new(TestClock::new(1)),
        spawner: spawner.clone(),
        model_header_policy: ModelHeaderPolicy::default(),
    }));
    let source_dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let callback_registry = registry.clone();
    let callback_dropped = source_dropped.clone();
    *callback.lock().unwrap_or_else(|p| p.into_inner()) = Some(Box::new(move || {
        let _ = callback_registry.get_models(None);
        callback_dropped.store(true, Ordering::Release);
    }));
    drive(
        registry.set_provider(provider(
            "p",
            Vec::new(),
            Arc::new(TestApiKey::configured()),
            Some(Arc::new(TestRefresher::new(vec![ModelsPublication {
                memory: ModelsMemoryIntent::Keep,
                persistence: ModelsPersistenceIntent::Set(store_entry(
                    model("p", "failed"),
                    "failed",
                )),
            }]))),
            None,
        )),
        &spawner,
    )
    .expect("set");
    let result = drive(
        registry.refresh(
            ModelsRefreshOptions {
                allow_network: false,
                providers: None,
                force: None,
            },
            &CancellationToken::new(),
        ),
        &spawner,
    );
    assert_eq!(result.errors.len(), 1);
    drop(result);
    assert!(source_dropped.load(Ordering::Acquire));

    let fixture = fixture(ModelHeaderPolicy::default());
    let refresher = Arc::new(TestRefresher::new(vec![ModelsPublication::default()]));
    set_provider(
        &fixture,
        provider(
            "p",
            Vec::new(),
            Arc::new(TestApiKey::configured()),
            Some(refresher),
            None,
        ),
    );
    fixture.spawner.set_fail(true);
    let result = ready(fixture.registry.refresh(
        ModelsRefreshOptions {
            allow_network: false,
            providers: None,
            force: None,
        },
        &CancellationToken::new(),
    ));
    let error = result
        .errors
        .get(&ProviderId::new("p").expect("id"))
        .expect("publication spawn error");
    let AuthResolutionError::Models(error) = error else {
        panic!("models error");
    };
    let spawn = error.source().expect("spawn source retained");
    assert_eq!(spawn.to_string(), "task could not be spawned");
    assert_eq!(
        spawn.source().expect("backend source").to_string(),
        SECRET_SENTINEL
    );
}

#[test]
fn login_api_key_oauth_unknown_and_unsupported_matrix() {
    let fixture = fixture(ModelHeaderPolicy::default());
    set_provider(
        &fixture,
        provider(
            "p",
            Vec::new(),
            Arc::new(TestApiKey::configured()),
            None,
            None,
        ),
    );
    let token = CancellationToken::new();
    assert!(matches!(
        ready(fixture.registry.login(
            &ProviderId::new("p").expect("id"),
            AuthType::ApiKey,
            &TestInteraction,
            &token
        )),
        Ok(Credential::ApiKey(_))
    ));
    let unknown = ready(fixture.registry.login(
        &ProviderId::new("missing").expect("id"),
        AuthType::ApiKey,
        &TestInteraction,
        &token,
    ))
    .expect_err("unknown");
    assert!(
        matches!(unknown, AuthResolutionError::Models(error) if error.code() == ModelsErrorCode::Provider)
    );
    let unsupported = ready(fixture.registry.login(
        &ProviderId::new("p").expect("id"),
        AuthType::OAuth,
        &TestInteraction,
        &token,
    ))
    .expect_err("unsupported");
    assert!(
        matches!(unsupported, AuthResolutionError::Models(error) if error.code() == ModelsErrorCode::Auth)
    );
    set_provider(
        &fixture,
        oauth_provider(
            "oauth",
            Arc::new(TestOAuth {
                refresh_calls: AtomicUsize::new(0),
                login_error: false,
                refresh_callback: Mutex::new(None),
            }),
            None,
        ),
    );
    assert!(matches!(
        ready(fixture.registry.login(
            &ProviderId::new("oauth").expect("id"),
            AuthType::OAuth,
            &TestInteraction,
            &token,
        )),
        Ok(Credential::OAuth(_))
    ));

    let failing_api = Arc::new(TestApiKey {
        configured: true,
        check_calls: AtomicUsize::new(0),
        resolve_calls: AtomicUsize::new(0),
        login_error: true,
        auth: Mutex::new(ModelAuth::default()),
        env: Mutex::new(None),
    });
    set_provider(
        &fixture,
        provider("api-fail", Vec::new(), failing_api, None, None),
    );
    let error = ready(fixture.registry.login(
        &ProviderId::new("api-fail").expect("id"),
        AuthType::ApiKey,
        &TestInteraction,
        &token,
    ))
    .expect_err("API-key provider failure");
    assert!(matches!(
        error,
        AuthResolutionError::Models(error)
            if error.code() == ModelsErrorCode::Auth && error.source().is_some()
    ));

    set_provider(
        &fixture,
        oauth_provider(
            "oauth-fail",
            Arc::new(TestOAuth {
                refresh_calls: AtomicUsize::new(0),
                login_error: true,
                refresh_callback: Mutex::new(None),
            }),
            None,
        ),
    );
    let error = ready(fixture.registry.login(
        &ProviderId::new("oauth-fail").expect("id"),
        AuthType::OAuth,
        &TestInteraction,
        &token,
    ))
    .expect_err("OAuth provider failure");
    assert!(matches!(
        error,
        AuthResolutionError::Models(error)
            if error.code() == ModelsErrorCode::OAuth && error.source().is_some()
    ));
}

#[test]
fn endpoint_override_public_runtime_missing_and_custody_matrix() {
    let fixture = fixture(ModelHeaderPolicy::default());
    let log = Arc::new(AdapterLog::default());
    let adapter = Arc::new(TestAdapter {
        api: ApiId::new(ApiId::OPENAI_COMPLETIONS).expect("api"),
        log: log.clone(),
        fetch_supported: true,
        cancel_supported: true,
    });
    let auth = Arc::new(TestApiKey::configured());
    set_provider(
        &fixture,
        provider(
            "p",
            vec![model("p", "m")],
            auth.clone(),
            None,
            Some(adapter),
        ),
    );
    let request_spawner = Arc::new(ManualSpawner::default());
    let options = StreamOptions {
        wire: StreamOptionsWire::default(),
        runtime: runtime(request_spawner.clone(), fixture.clock.clone()),
    };
    let stream = fixture.registry.stream(
        model("p", "m"),
        empty_context(),
        ModelsRequestOverrides::default(),
        options,
    );
    let result = drive(stream.result(), &request_spawner);
    assert_eq!(result.stop_reason, StopReason::Stop);
    assert_eq!(
        log.endpoints
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_slice(),
        ["https://example.invalid"]
    );

    auth.auth.lock().unwrap_or_else(|p| p.into_inner()).base_url =
        Some("https://credential.invalid".to_owned());
    let stream = fixture.registry.stream(
        model("p", "m"),
        empty_context(),
        ModelsRequestOverrides {
            runtime_endpoint: Some(
                RequestEndpoint::new_resolved("https://ignored.invalid").expect("endpoint"),
            ),
            ..ModelsRequestOverrides::default()
        },
        StreamOptions {
            wire: StreamOptionsWire::default(),
            runtime: runtime(request_spawner.clone(), fixture.clock.clone()),
        },
    );
    assert_eq!(
        drive(stream.result(), &request_spawner).stop_reason,
        StopReason::Stop
    );
    assert_eq!(
        log.endpoints.lock().unwrap_or_else(|p| p.into_inner())[1],
        "https://credential.invalid"
    );
    auth.auth.lock().unwrap_or_else(|p| p.into_inner()).base_url = None;

    let mut sentinel = model("p", "azure");
    sentinel.api = ApiId::new(ApiId::AZURE_OPENAI_RESPONSES).expect("api");
    sentinel.base_url = ModelBaseUrl::RuntimeResolved;
    let result = fixture.registry.stream(
        sentinel,
        empty_context(),
        ModelsRequestOverrides::default(),
        StreamOptions {
            wire: StreamOptionsWire::default(),
            runtime: runtime(request_spawner.clone(), fixture.clock.clone()),
        },
    );
    let result = drive(result.result(), &request_spawner);
    assert_eq!(result.stop_reason, StopReason::Error);

    let azure_log = Arc::new(AdapterLog::default());
    let azure_adapter = Arc::new(TestAdapter {
        api: ApiId::new(ApiId::AZURE_OPENAI_RESPONSES).expect("api"),
        log: azure_log.clone(),
        fetch_supported: true,
        cancel_supported: true,
    });
    let azure_auth = Arc::new(TestApiKey::configured());
    let mut azure_model = model("azure", "m");
    azure_model.api = ApiId::new(ApiId::AZURE_OPENAI_RESPONSES).expect("api");
    azure_model.base_url = ModelBaseUrl::RuntimeResolved;
    set_provider(
        &fixture,
        provider(
            "azure",
            vec![azure_model.clone()],
            azure_auth,
            None,
            Some(azure_adapter),
        ),
    );
    let stream = fixture.registry.stream(
        azure_model.clone(),
        empty_context(),
        ModelsRequestOverrides {
            runtime_endpoint: Some(
                RequestEndpoint::new_resolved("https://runtime.invalid").expect("endpoint"),
            ),
            ..ModelsRequestOverrides::default()
        },
        StreamOptions {
            wire: StreamOptionsWire::default(),
            runtime: runtime(request_spawner.clone(), fixture.clock.clone()),
        },
    );
    assert_eq!(
        drive(stream.result(), &request_spawner).stop_reason,
        StopReason::Stop
    );
    assert_eq!(
        azure_log
            .endpoints
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_slice(),
        ["https://runtime.invalid"]
    );
    assert!(matches!(
        azure_model.base_url,
        ModelBaseUrl::RuntimeResolved
    ));
}

#[test]
fn resolved_model_explicit_header_merge_case_and_null_matrix() {
    let policy = ModelHeaderPolicy::from_allowlist(vec!["x-test".to_owned()]);
    let fixture = fixture(policy.clone());
    let log = Arc::new(AdapterLog::default());
    let adapter = Arc::new(TestAdapter {
        api: ApiId::new(ApiId::OPENAI_COMPLETIONS).expect("api"),
        log: log.clone(),
        fetch_supported: true,
        cancel_supported: true,
    });
    let auth = Arc::new(TestApiKey::configured());
    auth.auth.lock().unwrap_or_else(|p| p.into_inner()).headers = Some(ProviderHeaders::new(
        BTreeMap::from([("X-Test".to_owned(), Some("auth".to_owned()))]),
    ));
    *auth.env.lock().unwrap_or_else(|p| p.into_inner()) = Some(ProviderEnv::new(BTreeMap::from([
        ("BASE".to_owned(), "base".to_owned()),
        ("REPLACE".to_owned(), "auth".to_owned()),
    ])));
    let mut raw = RawModel::from(&model("p", "m"));
    raw.headers = Some(
        RawModelHeaders::new(BTreeMap::from([("x-test".to_owned(), "model".to_owned())]))
            .expect("headers"),
    );
    let header_model = raw.into_model(&policy).expect("classified model");
    set_provider(
        &fixture,
        provider("p", vec![header_model.clone()], auth, None, Some(adapter)),
    );
    let request_spawner = Arc::new(ManualSpawner::default());
    let stream = fixture.registry.stream(
        header_model.clone(),
        empty_context(),
        ModelsRequestOverrides::default(),
        StreamOptions {
            wire: StreamOptionsWire::default(),
            runtime: runtime(request_spawner.clone(), fixture.clock.clone()),
        },
    );
    let _ = drive(stream.result(), &request_spawner);
    let stream = fixture.registry.stream(
        header_model.clone(),
        empty_context(),
        ModelsRequestOverrides {
            headers: ProviderHeaders::new(BTreeMap::from([(
                "X-TEST".to_owned(),
                Some("explicit".to_owned()),
            )])),
            ..ModelsRequestOverrides::default()
        },
        StreamOptions {
            wire: StreamOptionsWire::default(),
            runtime: runtime(request_spawner.clone(), fixture.clock.clone()),
        },
    );
    let _ = drive(stream.result(), &request_spawner);
    let stream = fixture.registry.stream(
        header_model,
        empty_context(),
        ModelsRequestOverrides {
            auth: AuthResolutionOverrides {
                api_key: Some(SecretString::new("explicit-key")),
                env: Some(ProviderEnv::new(BTreeMap::from([
                    ("EXPLICIT".to_owned(), "yes".to_owned()),
                    ("REPLACE".to_owned(), "explicit".to_owned()),
                ]))),
                min_oauth_validity_ms: None,
            },
            headers: ProviderHeaders::new(BTreeMap::from([("x-TEST".to_owned(), None)])),
            ..ModelsRequestOverrides::default()
        },
        StreamOptions {
            wire: StreamOptionsWire::default(),
            runtime: runtime(request_spawner.clone(), fixture.clock.clone()),
        },
    );
    let _ = drive(stream.result(), &request_spawner);
    let headers = log.headers.lock().unwrap_or_else(|p| p.into_inner());
    assert_eq!(
        headers[0].as_map().get("x-test").and_then(Option::as_deref),
        Some("model")
    );
    assert!(!headers[0].as_map().contains_key("X-Test"));
    assert_eq!(
        headers[1].as_map().get("X-TEST").and_then(Option::as_deref),
        Some("explicit")
    );
    assert_eq!(headers[2].as_map().get("x-TEST"), Some(&None));
    drop(headers);
    assert_eq!(
        log.api_keys.lock().unwrap_or_else(|p| p.into_inner())[2].as_deref(),
        Some("explicit-key")
    );
    let environments = log.environments.lock().unwrap_or_else(|p| p.into_inner());
    assert_eq!(
        environments[2].as_map().get("BASE").map(String::as_str),
        Some("base")
    );
    assert_eq!(
        environments[2].as_map().get("REPLACE").map(String::as_str),
        Some("explicit")
    );
    assert_eq!(
        environments[2].as_map().get("EXPLICIT").map(String::as_str),
        Some("yes")
    );
}

#[test]
fn resolver_builds_four_text_deferred_nominal_request_roots() {
    let fixture = fixture(ModelHeaderPolicy::default());
    let log = Arc::new(AdapterLog::default());
    let adapter = Arc::new(TestAdapter {
        api: ApiId::new(ApiId::OPENAI_COMPLETIONS).expect("api"),
        log: log.clone(),
        fetch_supported: true,
        cancel_supported: true,
    });
    set_provider(
        &fixture,
        provider(
            "p",
            vec![model("p", "m")],
            Arc::new(TestApiKey::configured()),
            None,
            Some(adapter),
        ),
    );
    let request_spawner = Arc::new(ManualSpawner::default());
    let raw_model = model("p", "raw");
    let mut simple_model = raw_model.clone();
    simple_model.id = ModelId::new("simple").expect("id");
    let mut fetch_model = raw_model.clone();
    fetch_model.id = ModelId::new("fetch").expect("id");
    let mut cancel_model = raw_model.clone();
    cancel_model.id = ModelId::new("cancel").expect("id");
    let raw = fixture.registry.complete(
        raw_model,
        empty_context(),
        ModelsRequestOverrides::default(),
        StreamOptions {
            wire: StreamOptionsWire {
                timeout_ms: Some(SafeUnsigned::new(11).expect("safe")),
                ..StreamOptionsWire::default()
            },
            runtime: runtime(request_spawner.clone(), fixture.clock.clone()),
        },
    );
    assert_eq!(drive(raw, &request_spawner).stop_reason, StopReason::Stop);
    let simple = fixture.registry.complete_simple(
        simple_model,
        empty_context(),
        ModelsRequestOverrides::default(),
        SimpleStreamOptions {
            wire: SimpleStreamOptionsWire {
                stream: StreamOptionsWire {
                    timeout_ms: Some(SafeUnsigned::new(22).expect("safe")),
                    ..StreamOptionsWire::default()
                },
                ..SimpleStreamOptionsWire::default()
            },
            runtime: runtime(request_spawner.clone(), fixture.clock.clone()),
        },
    );
    assert_eq!(
        drive(simple, &request_spawner).stop_reason,
        StopReason::Stop
    );
    let fetch = fixture.registry.fetch_deferred(
        fetch_model.clone(),
        handle(&fetch_model),
        ModelsRequestOverrides::default(),
        DeferredFetchOptions {
            wire: DeferredFetchOptionsWire {
                timeout_ms: Some(SafeUnsigned::new(33).expect("safe")),
                ..DeferredFetchOptionsWire::default()
            },
            runtime: runtime(request_spawner.clone(), fixture.clock.clone()),
        },
    );
    assert_eq!(drive(fetch, &request_spawner).stop_reason, StopReason::Stop);
    let cancel = fixture.registry.cancel_deferred(
        cancel_model.clone(),
        handle(&cancel_model),
        ModelsRequestOverrides::default(),
        DeferredCancelOptions {
            wire: DeferredCancelOptionsWire {
                timeout_ms: Some(SafeUnsigned::new(44).expect("safe")),
                ..DeferredCancelOptionsWire::default()
            },
            runtime: runtime(request_spawner.clone(), fixture.clock.clone()),
        },
    );
    assert!(drive(cancel, &request_spawner).is_ok());
    assert_eq!(
        log.roots
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_slice(),
        ["stream", "simple", "fetch", "cancel"]
    );
    assert_eq!(
        log.models
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .map(ModelId::as_str)
            .collect::<Vec<_>>(),
        ["raw", "simple", "fetch", "cancel"]
    );
    assert_eq!(
        log.option_markers
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_slice(),
        [Some(11), Some(22), Some(33), Some(44)]
    );
    assert!(
        log.api_keys
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .all(|key| key.as_deref() == Some("resolved-key"))
    );
    assert!(
        log.endpoints
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .all(|endpoint| endpoint == "https://example.invalid")
    );
    let expected_clock = Arc::as_ptr(&fixture.clock) as *const () as usize;
    let expected_spawner = Arc::as_ptr(&request_spawner) as *const () as usize;
    assert!(
        log.runtime_clocks
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .all(|pointer| *pointer == expected_clock)
    );
    assert!(
        log.runtime_spawners
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .all(|pointer| *pointer == expected_spawner)
    );
}

#[test]
fn dispatch_selects_adapter_and_optional_deferred_capabilities() {
    let fixture = fixture(ModelHeaderPolicy::default());
    let request_spawner = Arc::new(ManualSpawner::default());
    set_provider(
        &fixture,
        provider(
            "p",
            vec![model("p", "m")],
            Arc::new(TestApiKey::configured()),
            None,
            None,
        ),
    );
    let stream = fixture.registry.stream(
        model("p", "m"),
        empty_context(),
        ModelsRequestOverrides::default(),
        StreamOptions {
            wire: StreamOptionsWire::default(),
            runtime: runtime(request_spawner.clone(), fixture.clock.clone()),
        },
    );
    assert_eq!(
        drive(stream.result(), &request_spawner).stop_reason,
        StopReason::Error
    );

    let log = Arc::new(AdapterLog::default());
    let cancel_only = Arc::new(TestAdapter {
        api: ApiId::new(ApiId::OPENAI_COMPLETIONS).expect("api"),
        log: log.clone(),
        fetch_supported: false,
        cancel_supported: true,
    });
    set_provider(
        &fixture,
        provider(
            "p",
            vec![model("p", "m")],
            Arc::new(TestApiKey::configured()),
            None,
            Some(cancel_only),
        ),
    );
    let mut wrong_api = model("p", "wrong-api");
    wrong_api.api = ApiId::new(ApiId::OPENAI_RESPONSES).expect("api");
    let stream = fixture.registry.stream(
        wrong_api,
        empty_context(),
        ModelsRequestOverrides::default(),
        StreamOptions {
            wire: StreamOptionsWire::default(),
            runtime: runtime(request_spawner.clone(), fixture.clock.clone()),
        },
    );
    assert_eq!(
        drive(stream.result(), &request_spawner).stop_reason,
        StopReason::Error
    );
    assert!(
        log.roots
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .is_empty()
    );
    let request_model = model("p", "m");
    let fetched = fixture.registry.fetch_deferred(
        request_model.clone(),
        handle(&request_model),
        ModelsRequestOverrides::default(),
        DeferredFetchOptions {
            wire: DeferredFetchOptionsWire::default(),
            runtime: runtime(request_spawner.clone(), fixture.clock.clone()),
        },
    );
    assert_eq!(
        drive(fetched, &request_spawner).stop_reason,
        StopReason::Error
    );
    assert!(
        drive(
            fixture.registry.cancel_deferred(
                request_model.clone(),
                handle(&request_model),
                ModelsRequestOverrides::default(),
                DeferredCancelOptions {
                    wire: DeferredCancelOptionsWire::default(),
                    runtime: runtime(request_spawner.clone(), fixture.clock.clone()),
                },
            ),
            &request_spawner,
        )
        .is_ok()
    );

    let fetch_only = Arc::new(TestAdapter {
        api: ApiId::new(ApiId::OPENAI_COMPLETIONS).expect("api"),
        log,
        fetch_supported: true,
        cancel_supported: false,
    });
    set_provider(
        &fixture,
        provider(
            "p",
            vec![request_model.clone()],
            Arc::new(TestApiKey::configured()),
            None,
            Some(fetch_only),
        ),
    );
    let fetched = fixture.registry.fetch_deferred(
        request_model.clone(),
        handle(&request_model),
        ModelsRequestOverrides::default(),
        DeferredFetchOptions {
            wire: DeferredFetchOptionsWire::default(),
            runtime: runtime(request_spawner.clone(), fixture.clock.clone()),
        },
    );
    assert_eq!(
        drive(fetched, &request_spawner).stop_reason,
        StopReason::Stop
    );
    let error = drive(
        fixture.registry.cancel_deferred(
            request_model.clone(),
            handle(&request_model),
            ModelsRequestOverrides::default(),
            DeferredCancelOptions {
                wire: DeferredCancelOptionsWire::default(),
                runtime: runtime(request_spawner.clone(), fixture.clock.clone()),
            },
        ),
        &request_spawner,
    )
    .expect_err("cancel unsupported independently");
    assert!(matches!(
        error,
        AuthResolutionError::Models(error) if error.code() == ModelsErrorCode::Provider
    ));
}

#[test]
fn dispatch_setup_failures_are_in_band_cancel_distinct_and_redacted() {
    let fixture = fixture(ModelHeaderPolicy::default());
    let request_spawner = Arc::new(ManualSpawner::default());
    let mut unknown_model = model("missing", "m");
    unknown_model.name = SECRET_SENTINEL.to_owned();
    let stream = fixture.registry.stream(
        unknown_model.clone(),
        empty_context(),
        ModelsRequestOverrides::default(),
        StreamOptions {
            wire: StreamOptionsWire::default(),
            runtime: runtime(request_spawner.clone(), fixture.clock.clone()),
        },
    );
    let result = drive(stream.result(), &request_spawner);
    assert_setup_terminal(&result, "stream provider is unavailable");
    request_spawner.set_fail(true);
    let stream = fixture.registry.stream(
        unknown_model,
        empty_context(),
        ModelsRequestOverrides::default(),
        StreamOptions {
            wire: StreamOptionsWire::default(),
            runtime: runtime(request_spawner.clone(), fixture.clock.clone()),
        },
    );
    let result = ready(stream.result());
    assert_setup_terminal(&result, "stream setup task could not be started");

    request_spawner.set_fail(false);
    let failing_auth = Arc::new(TestApiKey {
        configured: true,
        check_calls: AtomicUsize::new(0),
        resolve_calls: AtomicUsize::new(0),
        login_error: true,
        auth: Mutex::new(ModelAuth::default()),
        env: Mutex::new(None),
    });
    set_provider(
        &fixture,
        provider(
            "auth-fail",
            vec![model("auth-fail", "m")],
            failing_auth,
            None,
            None,
        ),
    );
    let auth_spawner = Arc::new(ManualSpawner::default());
    let stream = fixture.registry.stream(
        model("auth-fail", "m"),
        empty_context(),
        ModelsRequestOverrides::default(),
        StreamOptions {
            wire: StreamOptionsWire::default(),
            runtime: runtime(auth_spawner.clone(), fixture.clock.clone()),
        },
    );
    let result = drive(stream.result(), &auth_spawner);
    assert_setup_terminal(&result, "stream authentication failed");

    let endpoint_log = Arc::new(AdapterLog::default());
    let endpoint_adapter = Arc::new(TestAdapter {
        api: ApiId::new(ApiId::AZURE_OPENAI_RESPONSES).expect("api"),
        log: endpoint_log,
        fetch_supported: true,
        cancel_supported: true,
    });
    let endpoint_auth = Arc::new(TestApiKey::configured());
    endpoint_auth
        .auth
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .api_key = Some(SecretString::new(SECRET_SENTINEL));
    let mut endpoint_model = model("endpoint-fail", "m");
    endpoint_model.api = ApiId::new(ApiId::AZURE_OPENAI_RESPONSES).expect("api");
    endpoint_model.base_url = ModelBaseUrl::RuntimeResolved;
    set_provider(
        &fixture,
        provider(
            "endpoint-fail",
            vec![endpoint_model.clone()],
            endpoint_auth,
            None,
            Some(endpoint_adapter),
        ),
    );
    let endpoint_spawner = Arc::new(ManualSpawner::default());
    let stream = fixture.registry.stream(
        endpoint_model,
        empty_context(),
        ModelsRequestOverrides::default(),
        StreamOptions {
            wire: StreamOptionsWire::default(),
            runtime: runtime(endpoint_spawner.clone(), fixture.clock.clone()),
        },
    );
    let result = drive(stream.result(), &endpoint_spawner);
    assert_setup_terminal(&result, "stream adapter setup failed");

    let missing_adapter_auth = Arc::new(TestApiKey::configured());
    missing_adapter_auth
        .auth
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .api_key = Some(SecretString::new(SECRET_SENTINEL));
    set_provider(
        &fixture,
        provider(
            "missing-adapter",
            vec![model("missing-adapter", "m")],
            missing_adapter_auth,
            None,
            None,
        ),
    );
    let adapter_spawner = Arc::new(ManualSpawner::default());
    let stream = fixture.registry.stream(
        model("missing-adapter", "m"),
        empty_context(),
        ModelsRequestOverrides::default(),
        StreamOptions {
            wire: StreamOptionsWire::default(),
            runtime: runtime(adapter_spawner.clone(), fixture.clock.clone()),
        },
    );
    let result = drive(stream.result(), &adapter_spawner);
    assert_setup_terminal(&result, "stream adapter setup failed");

    let wrong_log = Arc::new(AdapterLog::default());
    let wrong_adapter = Arc::new(TestAdapter {
        api: ApiId::new(ApiId::OPENAI_COMPLETIONS).expect("api"),
        log: wrong_log,
        fetch_supported: true,
        cancel_supported: true,
    });
    let mut wrong_model = model("wrong-adapter", "m");
    wrong_model.api = ApiId::new(ApiId::OPENAI_RESPONSES).expect("api");
    set_provider(
        &fixture,
        provider(
            "wrong-adapter",
            vec![wrong_model.clone()],
            Arc::new(TestApiKey::configured()),
            None,
            Some(wrong_adapter),
        ),
    );
    let stream = fixture.registry.stream(
        wrong_model,
        empty_context(),
        ModelsRequestOverrides::default(),
        StreamOptions {
            wire: StreamOptionsWire::default(),
            runtime: runtime(adapter_spawner.clone(), fixture.clock.clone()),
        },
    );
    let result = drive(stream.result(), &adapter_spawner);
    assert_setup_terminal(&result, "stream adapter setup failed");

    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let telemetry: Arc<dyn TelemetryContext> = Arc::new(NOOP_TELEMETRY_CONTEXT);
    let cancel_spawner = Arc::new(ManualSpawner::default());
    let stream = fixture.registry.stream(
        model("missing", "m"),
        empty_context(),
        ModelsRequestOverrides::default(),
        StreamOptions {
            wire: StreamOptionsWire::default(),
            runtime: RequestRuntime {
                cancellation: cancelled,
                telemetry,
                clock: fixture.clock.clone(),
                spawner: cancel_spawner,
            },
        },
    );
    let result = ready(stream.result());
    assert_eq!(result.stop_reason, StopReason::Aborted);
    assert_eq!(result.error_message.as_deref(), Some("request cancelled"));
}

#[test]
fn refresh_selection_error_and_cancellation_matrix() {
    {
        let fixture = fixture(ModelHeaderPolicy::default());
        let selected = Arc::new(TestRefresher::new(Vec::new()));
        let mut failing_value = TestRefresher::new(Vec::new());
        failing_value.error = true;
        let failing = Arc::new(failing_value);
        set_provider(
            &fixture,
            provider(
                "selected",
                Vec::new(),
                Arc::new(TestApiKey::configured()),
                Some(selected.clone()),
                None,
            ),
        );
        set_provider(
            &fixture,
            provider(
                "failing",
                Vec::new(),
                Arc::new(TestApiKey::configured()),
                Some(failing.clone()),
                None,
            ),
        );
        set_provider(
            &fixture,
            provider(
                "static",
                Vec::new(),
                Arc::new(TestApiKey::configured()),
                None,
                None,
            ),
        );
        let token = CancellationToken::new();
        let result = drive(
            fixture.registry.refresh(
                ModelsRefreshOptions {
                    allow_network: false,
                    providers: Some(vec![
                        ProviderId::new("selected").expect("id"),
                        ProviderId::new("selected").expect("id"),
                        ProviderId::new("failing").expect("id"),
                        ProviderId::new("unknown").expect("id"),
                    ]),
                    force: None,
                },
                &token,
            ),
            &fixture.spawner,
        );
        assert!(!result.aborted);
        assert_eq!(result.errors.len(), 1);
        assert!(
            result
                .errors
                .contains_key(&ProviderId::new("failing").expect("id"))
        );
        assert_eq!(
            selected
                .phases
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .len(),
            1
        );
        assert_eq!(
            failing
                .phases
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .len(),
            1
        );
    }

    let generation_fixture = fixture(ModelHeaderPolicy::default());
    let state = Arc::new(ControlledErrorState::default());
    let base = provider(
        "race",
        Vec::new(),
        Arc::new(TestApiKey::configured()),
        None,
        None,
    );
    drive(
        generation_fixture
            .registry
            .set_provider(Arc::new(ControlledErrorProvider {
                inner: Arc::try_unwrap(base).ok().expect("unique"),
                refresher: ControlledErrorRefresher {
                    state: state.clone(),
                },
            })),
        &generation_fixture.spawner,
    )
    .expect("set");
    let token = CancellationToken::new();
    let mut refresh = pin!(generation_fixture.registry.refresh(
        ModelsRefreshOptions {
            allow_network: false,
            providers: None,
            force: None,
        },
        &token,
    ));
    assert!(poll(refresh.as_mut()).is_pending());
    let replacement = provider(
        "race",
        Vec::new(),
        Arc::new(TestApiKey::configured()),
        None,
        None,
    );
    let mut replacement = pin!(generation_fixture.registry.set_provider(replacement));
    assert!(poll(replacement.as_mut()).is_pending());
    state.ready.store(true, Ordering::Release);
    let result = drive(refresh, &generation_fixture.spawner);
    assert!(!result.aborted && result.errors.is_empty());
    assert!(state.dropped.load(Ordering::Acquire));
    drive(replacement, &generation_fixture.spawner).expect("replacement");

    let fixture = fixture(ModelHeaderPolicy::default());
    let state = Arc::new(ControlledErrorState::default());
    let base = provider(
        "triple-race",
        Vec::new(),
        Arc::new(TestApiKey::configured()),
        None,
        None,
    );
    drive(
        fixture
            .registry
            .set_provider(Arc::new(ControlledErrorProvider {
                inner: Arc::try_unwrap(base).ok().expect("unique"),
                refresher: ControlledErrorRefresher {
                    state: state.clone(),
                },
            })),
        &fixture.spawner,
    )
    .expect("set");
    let cancellation = CancellationToken::new();
    let mut refresh = pin!(fixture.registry.refresh(
        ModelsRefreshOptions {
            allow_network: false,
            providers: None,
            force: None,
        },
        &cancellation,
    ));
    assert!(poll(refresh.as_mut()).is_pending());
    let replacement = provider(
        "triple-race",
        Vec::new(),
        Arc::new(TestApiKey::configured()),
        None,
        None,
    );
    let mut replacement = pin!(fixture.registry.set_provider(replacement));
    assert!(poll(replacement.as_mut()).is_pending());
    state.ready.store(true, Ordering::Release);
    cancellation.cancel();
    let result = drive(refresh, &fixture.spawner);
    assert!(result.aborted && result.errors.is_empty());
    assert!(state.dropped.load(Ordering::Acquire));
    drive(replacement, &fixture.spawner).expect("replacement");

    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let result = ready(
        fixture
            .registry
            .refresh(ModelsRefreshOptions::default(), &cancellation),
    );
    assert!(result.aborted && result.errors.is_empty());
}

#[test]
fn cache_publication_precedes_any_credential_read() {
    let trace = Arc::new(Mutex::new(Vec::new()));
    let spawner = Arc::new(ManualSpawner::default());
    let registry = ModelsRegistry::new(ModelsRegistryOptions {
        credentials: Arc::new(TraceCredentialStore {
            trace: trace.clone(),
        }),
        models_store: Box::new(TraceModelsStore {
            trace: trace.clone(),
        }),
        auth_context: Arc::new(EmptyContext),
        clock: Arc::new(TestClock::new(1)),
        spawner: spawner.clone(),
        model_header_policy: ModelHeaderPolicy::default(),
    });
    let base = provider(
        "p",
        Vec::new(),
        Arc::new(TestApiKey::configured()),
        None,
        None,
    );
    drive(
        registry.set_provider(Arc::new(TraceProvider {
            inner: Arc::try_unwrap(base).ok().expect("unique"),
            refresher: TraceRefresher {
                trace: trace.clone(),
            },
        })),
        &spawner,
    )
    .expect("set");
    let result = drive(
        registry.refresh(ModelsRefreshOptions::default(), &CancellationToken::new()),
        &spawner,
    );
    assert!(result.errors.is_empty());
    let trace = trace.lock().unwrap_or_else(|p| p.into_inner());
    let cache_ack = trace
        .iter()
        .position(|event| *event == "cache-published")
        .expect("cache ack");
    let credential = trace
        .iter()
        .position(|event| *event == "credential-read")
        .expect("credential read");
    assert!(cache_ack < credential, "trace: {trace:?}");
}

#[test]
fn restore_reclassifies_raw_headers_under_current_policy() {
    let raw = serde_json::json!({
        "models": [{
            "id": "m", "name": "m", "api": "openai-completions", "provider": "p",
            "baseUrl": "https://example.invalid", "reasoning": false, "input": ["text"],
            "cost": {"input": 0.0, "output": 0.0, "cacheRead": 0.0, "cacheWrite": 0.0},
            "contextWindow": 1, "maxTokens": 1, "headers": {"x-private": "secret"}
        }]
    });
    let raw: RawModelsStoreEntry = serde_json::from_value(raw).expect("raw");
    assert!(
        raw.clone()
            .into_store(&ModelHeaderPolicy::default())
            .is_err()
    );
    let trace = Arc::new(Mutex::new(Vec::new()));
    let spawner = Arc::new(ManualSpawner::default());
    let registry = ModelsRegistry::new(ModelsRegistryOptions {
        credentials: Arc::new(TraceCredentialStore {
            trace: trace.clone(),
        }),
        models_store: Box::new(RawModelsStore {
            raw: Mutex::new(Some(raw)),
            reads: AtomicUsize::new(0),
        }),
        auth_context: Arc::new(EmptyContext),
        clock: Arc::new(TestClock::new(1)),
        spawner: spawner.clone(),
        model_header_policy: ModelHeaderPolicy::default(),
    });
    let refresher = Arc::new(TestRefresher::new(Vec::new()));
    drive(
        registry.set_provider(provider(
            "p",
            Vec::new(),
            Arc::new(TestApiKey::configured()),
            Some(refresher.clone()),
            None,
        )),
        &spawner,
    )
    .expect("set");
    let result = drive(
        registry.refresh(
            ModelsRefreshOptions {
                allow_network: false,
                providers: None,
                force: None,
            },
            &CancellationToken::new(),
        ),
        &spawner,
    );
    assert!(matches!(
        result.errors.get(&ProviderId::new("p").expect("id")),
        Some(AuthResolutionError::Models(error))
            if error.code() == ModelsErrorCode::ModelValidation
    ));
    assert!(
        refresher
            .phases
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .is_empty()
    );
    assert!(
        !trace
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .contains(&"credential-read")
    );
}

#[test]
fn login_cancellation_before_queue_and_after_commit_admission() {
    let fixture = fixture(ModelHeaderPolicy::default());
    let token = CancellationToken::new();
    token.cancel();
    let result = ready(fixture.registry.login(
        &ProviderId::new("p").expect("id"),
        AuthType::ApiKey,
        &TestInteraction,
        &token,
    ));
    assert!(matches!(result, Err(AuthResolutionError::Cancelled)));

    let spawner = Arc::new(ManualSpawner::default());
    let state = Arc::new(BlockingCredentialState::default());
    let registry = ModelsRegistry::new(ModelsRegistryOptions {
        credentials: Arc::new(BlockingCredentialStore {
            state: state.clone(),
        }),
        models_store: Box::new(TestModelsStore {
            state: Arc::new(StoreState::default()),
        }),
        auth_context: Arc::new(EmptyContext),
        clock: Arc::new(TestClock::new(1)),
        spawner: spawner.clone(),
        model_header_policy: ModelHeaderPolicy::default(),
    });
    drive(
        registry.set_provider(provider(
            "p",
            Vec::new(),
            Arc::new(TestApiKey::configured()),
            None,
            None,
        )),
        &spawner,
    )
    .expect("set");
    let cancellation = CancellationToken::new();
    let provider_id = ProviderId::new("p").expect("id");
    let interaction = TestInteraction;
    let mut login =
        pin!(registry.login(&provider_id, AuthType::ApiKey, &interaction, &cancellation,));
    assert!(poll(login.as_mut()).is_pending());
    assert!(state.admitted.load(Ordering::Acquire));
    cancellation.cancel();
    state.release();
    let credential = match poll(login.as_mut()) {
        Poll::Ready(result) => result.expect("admitted commit is shielded"),
        Poll::Pending => panic!("released commit must settle"),
    };
    assert!(matches!(credential, Credential::ApiKey(_)));
    assert!(
        state
            .value
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .is_some()
    );
}

#[test]
fn explicit_login_reconciles_quarantine_at_reserved_revision() {
    let spawner = Arc::new(ManualSpawner::default());
    let credentials = Arc::new(QuarantineCredentialStore::default());
    let registry = ModelsRegistry::new(ModelsRegistryOptions {
        credentials: credentials.clone(),
        models_store: Box::new(TestModelsStore {
            state: Arc::new(StoreState::default()),
        }),
        auth_context: Arc::new(EmptyContext),
        clock: Arc::new(TestClock::new(1)),
        spawner: spawner.clone(),
        model_header_policy: ModelHeaderPolicy::default(),
    });
    let login_provider = provider(
        "p",
        Vec::new(),
        Arc::new(TestApiKey::configured()),
        None,
        None,
    );
    drive(registry.set_provider(login_provider), &spawner).expect("set");
    let credential = ready(registry.login(
        &ProviderId::new("p").expect("id"),
        AuthType::ApiKey,
        &TestInteraction,
        &CancellationToken::new(),
    ))
    .expect("explicit login reconciles quarantine");
    let reconciliations = credentials
        .reconciliations
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    assert_eq!(reconciliations.len(), 1);
    assert!(matches!(
        (&reconciliations[0], &credential),
        (CredentialReconciliation::Replace(stored), Credential::ApiKey(candidate))
            if stored == &Credential::ApiKey(candidate.clone())
    ));
    drop(reconciliations);

    let spawner = Arc::new(ManualSpawner::default());
    let state = Arc::new(BlockingQuarantineState::default());
    let registry = ModelsRegistry::new(ModelsRegistryOptions {
        credentials: Arc::new(BlockingQuarantineStore {
            state: state.clone(),
        }),
        models_store: Box::new(TestModelsStore {
            state: Arc::new(StoreState::default()),
        }),
        auth_context: Arc::new(EmptyContext),
        clock: Arc::new(TestClock::new(1)),
        spawner: spawner.clone(),
        model_header_policy: ModelHeaderPolicy::default(),
    });
    drive(
        registry.set_provider(provider(
            "p",
            Vec::new(),
            Arc::new(TestApiKey::configured()),
            None,
            None,
        )),
        &spawner,
    )
    .expect("set");
    let provider_id = ProviderId::new("p").expect("id");
    let interaction = TestInteraction;
    let cancellation = CancellationToken::new();
    let mut login =
        pin!(registry.login(&provider_id, AuthType::ApiKey, &interaction, &cancellation,));
    assert!(poll(login.as_mut()).is_pending());
    assert!(state.admitted.load(Ordering::Acquire));
    cancellation.cancel();
    state.release();
    assert!(matches!(
        poll(login.as_mut()),
        Poll::Ready(Ok(Credential::ApiKey(_)))
    ));
    assert!(matches!(
        state
            .reconciliations
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_slice(),
        [CredentialReconciliation::Replace(_)]
    ));
}

#[test]
fn logout_refresh_race_and_quarantine_delete_matrix() {
    let known_fixture = fixture(ModelHeaderPolicy::default());
    set_credential(&known_fixture, "p");
    let id = ProviderId::new("p").expect("id");
    ready(
        known_fixture
            .registry
            .logout(&id, &CancellationToken::new()),
    )
    .expect("logout");
    assert!(
        ready(
            known_fixture
                .credentials
                .read(&id, &CancellationToken::new())
        )
        .expect("read")
        .is_none()
    );

    let spawner = Arc::new(ManualSpawner::default());
    let quarantined = Arc::new(QuarantineCredentialStore::default());
    let registry = ModelsRegistry::new(ModelsRegistryOptions {
        credentials: quarantined.clone(),
        models_store: Box::new(TestModelsStore {
            state: Arc::new(StoreState::default()),
        }),
        auth_context: Arc::new(EmptyContext),
        clock: Arc::new(TestClock::new(1)),
        spawner,
        model_header_policy: ModelHeaderPolicy::default(),
    });
    ready(registry.logout(&id, &CancellationToken::new())).expect("quarantine delete");
    assert!(matches!(
        quarantined
            .reconciliations
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_slice(),
        [CredentialReconciliation::Delete]
    ));

    let state = Arc::new(BlockingQuarantineState::default());
    let registry = ModelsRegistry::new(ModelsRegistryOptions {
        credentials: Arc::new(BlockingQuarantineStore {
            state: state.clone(),
        }),
        models_store: Box::new(TestModelsStore {
            state: Arc::new(StoreState::default()),
        }),
        auth_context: Arc::new(EmptyContext),
        clock: Arc::new(TestClock::new(1)),
        spawner: Arc::new(ManualSpawner::default()),
        model_header_policy: ModelHeaderPolicy::default(),
    });
    let cancellation = CancellationToken::new();
    let mut logout = pin!(registry.logout(&id, &cancellation));
    assert!(poll(logout.as_mut()).is_pending());
    assert!(state.admitted.load(Ordering::Acquire));
    cancellation.cancel();
    state.release();
    assert!(matches!(poll(logout.as_mut()), Poll::Ready(Ok(()))));
    assert!(matches!(
        state
            .reconciliations
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_slice(),
        [CredentialReconciliation::Delete]
    ));

    let spawner = Arc::new(ManualSpawner::default());
    let credentials = Arc::new(InMemoryCredentialStore::new());
    let oauth_state = Arc::new(BlockingOAuthState::default());
    let oauth: Arc<dyn OAuthAuth> = Arc::new(BlockingOAuth {
        state: oauth_state.clone(),
    });
    let registry = ModelsRegistry::new(ModelsRegistryOptions {
        credentials: credentials.clone(),
        models_store: Box::new(TestModelsStore {
            state: Arc::new(StoreState::default()),
        }),
        auth_context: Arc::new(EmptyContext),
        clock: Arc::new(TestClock::new(100)),
        spawner: spawner.clone(),
        model_header_policy: ModelHeaderPolicy::default(),
    });
    let provider = Arc::new(TestProvider {
        id: ProviderId::new("oauth-race").expect("id"),
        name: "oauth-race".to_owned(),
        auth: ProviderAuth::new(None, Some(oauth)).expect("auth"),
        models: Mutex::new(Ok(Vec::<Model>::new().into())),
        refresher: Some(Arc::new(TestRefresher::new(Vec::new()))),
        adapter: None,
        filter_calls: AtomicUsize::new(0),
        filtered_credentials: Mutex::new(Vec::new()),
    });
    drive(registry.set_provider(provider), &spawner).expect("set");
    let race_id = ProviderId::new("oauth-race").expect("id");
    let token = CancellationToken::new();
    let lease = ready(credentials.begin_mutation(&race_id, &token)).expect("lease");
    ready(lease.commit(CredentialMutationValue::Set(Credential::OAuth(
        oauth_credential("expired", 100),
    ))))
    .expect("commit");
    let mut refresh = pin!(registry.refresh(ModelsRefreshOptions::default(), &token));
    assert!(poll(refresh.as_mut()).is_pending());
    assert!(oauth_state.started.load(Ordering::Acquire));
    let mut logout = pin!(registry.logout(&race_id, &token));
    assert!(poll(logout.as_mut()).is_pending());
    oauth_state.release();
    let refresh_result = drive(refresh, &spawner);
    assert!(refresh_result.errors.is_empty());
    drive(logout, &spawner).expect("queued logout follows refresh commit");
    assert!(
        ready(credentials.read(&race_id, &token))
            .expect("read")
            .is_none()
    );

    for refresh_first in [true, false] {
        let fixture = fixture(ModelHeaderPolicy::default());
        let oauth = Arc::new(TestOAuth {
            refresh_calls: AtomicUsize::new(0),
            login_error: false,
            refresh_callback: Mutex::new(None),
        });
        let refresher = Arc::new(TestRefresher::new(Vec::new()));
        set_provider(
            &fixture,
            oauth_provider("oauth", oauth.clone(), Some(refresher)),
        );
        let oauth_id = ProviderId::new("oauth").expect("id");
        let token = CancellationToken::new();
        let lease = ready(fixture.credentials.begin_mutation(&oauth_id, &token)).expect("lease");
        ready(lease.commit(CredentialMutationValue::Set(Credential::OAuth(
            oauth_credential("expired", 100),
        ))))
        .expect("commit");
        if refresh_first {
            let result = drive(
                fixture
                    .registry
                    .refresh(ModelsRefreshOptions::default(), &token),
                &fixture.spawner,
            );
            assert!(result.errors.is_empty());
            assert_eq!(oauth.refresh_calls.load(Ordering::Acquire), 1);
            ready(fixture.registry.logout(&oauth_id, &token)).expect("logout after rotate");
        } else {
            ready(fixture.registry.logout(&oauth_id, &token)).expect("logout first");
            let result = drive(
                fixture
                    .registry
                    .refresh(ModelsRefreshOptions::default(), &token),
                &fixture.spawner,
            );
            assert!(result.errors.is_empty());
            assert_eq!(oauth.refresh_calls.load(Ordering::Acquire), 0);
        }
        assert!(
            ready(fixture.credentials.read(&oauth_id, &token))
                .expect("read")
                .is_none()
        );
    }
}

#[test]
fn new_refresh_fences_old_generation() {
    let fixture = blocking_fixture();
    let refresher = Arc::new(FenceRestoreRefresher {
        calls: AtomicUsize::new(0),
        restored: Mutex::new(Vec::new()),
    });
    let base = provider(
        "p",
        Vec::new(),
        Arc::new(TestApiKey::configured()),
        None,
        None,
    );
    set_blocking_provider(
        &fixture,
        Arc::new(FenceRestoreProvider {
            inner: Arc::try_unwrap(base).ok().expect("unique"),
            refresher: refresher.clone(),
        }),
    );
    let token = CancellationToken::new();
    let mut old_refresh = pin!(fixture.registry.refresh(
        ModelsRefreshOptions {
            allow_network: false,
            providers: None,
            force: None,
        },
        &token,
    ));
    assert!(poll(old_refresh.as_mut()).is_pending());
    fixture.spawner.drive_once();
    assert_eq!(fixture.store.active.load(Ordering::Acquire), 1);
    assert_eq!(fixture.store.reads.load(Ordering::Acquire), 1);

    let mut successor = pin!(fixture.registry.refresh(
        ModelsRefreshOptions {
            allow_network: false,
            providers: None,
            force: None,
        },
        &token,
    ));
    assert!(poll(successor.as_mut()).is_pending());
    fixture.spawner.drive_once();
    assert_eq!(
        fixture.store.reads.load(Ordering::Acquire),
        1,
        "successor must drain the admitted old write before reading cache"
    );

    fixture.store.release(1);
    let mut old_result = None;
    let mut successor_result = None;
    for _ in 0..128 {
        fixture.spawner.drive_once();
        if old_result.is_none()
            && let Poll::Ready(result) = poll(old_refresh.as_mut())
        {
            old_result = Some(result);
        }
        if successor_result.is_none()
            && let Poll::Ready(result) = poll(successor.as_mut())
        {
            successor_result = Some(result);
        }
        if old_result.is_some() && successor_result.is_some() {
            break;
        }
    }
    let old_result = old_result.expect("old refresh settles");
    let successor_result = successor_result.expect("successor settles");
    assert!(!old_result.aborted && old_result.errors.is_empty());
    assert!(!successor_result.aborted && successor_result.errors.is_empty());
    assert_eq!(fixture.store.reads.load(Ordering::Acquire), 2);
    assert_eq!(
        fixture
            .store
            .entries
            .lock()
            .unwrap_or_else(|p| p.into_inner())[&ProviderId::new("p").expect("id")]
            .etag
            .as_deref(),
        Some("definitive")
    );
    assert_eq!(
        refresher
            .restored
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .map(ModelId::as_str)
            .collect::<Vec<_>>(),
        ["definitive"]
    );
    assert_eq!(
        fixture
            .registry
            .get_models(None)
            .iter()
            .map(|model| model.id.as_str())
            .collect::<Vec<_>>(),
        ["definitive"]
    );
}

#[test]
fn provider_replacement_fences_old_generation() {
    let fixture = blocking_fixture();
    let refresher = Arc::new(OutcomeRefresher::new(vec![ModelsPublication {
        memory: ModelsMemoryIntent::Set(vec![model("p", "dynamic")].into()),
        persistence: ModelsPersistenceIntent::Set(store_entry(model("p", "dynamic"), "old")),
    }]));
    set_blocking_provider(&fixture, outcome_provider("p", refresher.clone()));
    let token = CancellationToken::new();
    let mut old_refresh = pin!(fixture.registry.refresh(
        ModelsRefreshOptions {
            allow_network: false,
            providers: None,
            force: None,
        },
        &token,
    ));
    assert!(poll(old_refresh.as_mut()).is_pending());
    fixture.spawner.drive_once();
    assert_eq!(fixture.store.active.load(Ordering::Acquire), 1);

    let replacement = provider(
        "p",
        vec![model("p", "replacement")],
        Arc::new(TestApiKey::configured()),
        None,
        None,
    );
    let mut replacement_future = pin!(fixture.registry.set_provider(replacement));
    assert!(poll(replacement_future.as_mut()).is_pending());
    assert_eq!(
        fixture
            .registry
            .get_provider(&ProviderId::new("p").expect("id"))
            .expect("old remains visible")
            .models()
            .expect("old baseline")
            .len(),
        0
    );
    let reads = fixture.store.reads.load(Ordering::Acquire);
    let displaced = drive(
        fixture.registry.refresh(
            ModelsRefreshOptions {
                allow_network: false,
                providers: Some(vec![ProviderId::new("p").expect("id")]),
                force: None,
            },
            &token,
        ),
        &fixture.spawner,
    );
    assert!(!displaced.aborted && displaced.errors.is_empty());
    assert_eq!(fixture.store.reads.load(Ordering::Acquire), reads);
    assert_eq!(
        refresher
            .outcomes
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .len(),
        0
    );

    fixture.store.release(1);
    let replacement_result = drive(replacement_future, &fixture.spawner);
    replacement_result.expect("replacement drains old write");
    let old_result = drive(old_refresh, &fixture.spawner);
    assert!(!old_result.aborted && old_result.errors.is_empty());
    assert_eq!(
        fixture.registry.get_models(None)[0].id.as_str(),
        "replacement"
    );
}

#[test]
fn provider_deletion_fences_old_generation() {
    let fixture = blocking_fixture();
    set_blocking_provider(
        &fixture,
        outcome_provider(
            "p",
            Arc::new(OutcomeRefresher::new(vec![ModelsPublication {
                memory: ModelsMemoryIntent::Set(vec![model("p", "stale-memory")].into()),
                persistence: ModelsPersistenceIntent::Delete,
            }])),
        ),
    );
    fixture
        .store
        .entries
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .insert(
            ProviderId::new("p").expect("id"),
            store_entry(model("p", "prior"), "prior"),
        );
    let token = CancellationToken::new();
    let mut refresh = pin!(fixture.registry.refresh(
        ModelsRefreshOptions {
            allow_network: false,
            providers: None,
            force: None,
        },
        &token,
    ));
    assert!(poll(refresh.as_mut()).is_pending());
    fixture.spawner.drive_once();
    assert_eq!(fixture.store.active.load(Ordering::Acquire), 1);
    let id = ProviderId::new("p").expect("id");
    let mut delete = pin!(fixture.registry.delete_provider(&id));
    assert!(poll(delete.as_mut()).is_pending());
    assert!(fixture.registry.get_provider(&id).is_some());
    fixture.store.release(1);
    drive(delete, &fixture.spawner).expect("delete drains active publication");
    let refresh = drive(refresh, &fixture.spawner);
    assert!(!refresh.aborted && refresh.errors.is_empty());
    assert!(fixture.registry.get_models(None).is_empty());
    assert!(
        !fixture
            .store
            .entries
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .contains_key(&id)
    );
    set_blocking_provider(
        &fixture,
        provider(
            "p",
            vec![model("p", "new")],
            Arc::new(TestApiKey::configured()),
            None,
            None,
        ),
    );
    assert_eq!(fixture.registry.get_models(None)[0].id.as_str(), "new");
}

#[test]
fn provider_clear_fences_every_old_generation() {
    let fixture = blocking_fixture();
    for id in ["a", "b"] {
        set_blocking_provider(
            &fixture,
            outcome_provider(
                id,
                Arc::new(OutcomeRefresher::new(vec![ModelsPublication {
                    memory: ModelsMemoryIntent::Set(vec![model(id, "dynamic")].into()),
                    persistence: ModelsPersistenceIntent::Set(store_entry(
                        model(id, "dynamic"),
                        id,
                    )),
                }])),
            ),
        );
    }
    let token = CancellationToken::new();
    let mut refresh = pin!(fixture.registry.refresh(
        ModelsRefreshOptions {
            allow_network: false,
            providers: None,
            force: None,
        },
        &token,
    ));
    assert!(poll(refresh.as_mut()).is_pending());
    fixture.spawner.drive_once();
    assert_eq!(fixture.store.active.load(Ordering::Acquire), 2);
    let mut clear = pin!(fixture.registry.clear_providers());
    assert!(poll(clear.as_mut()).is_pending());
    assert_eq!(fixture.registry.get_providers().len(), 2);
    let late_provider = provider(
        "late",
        vec![model("late", "survivor")],
        Arc::new(TestApiKey::configured()),
        None,
        None,
    );
    let mut late = pin!(fixture.registry.set_provider(late_provider));
    assert!(poll(late.as_mut()).is_pending());
    fixture.store.release(2);
    drive(clear, &fixture.spawner).expect("clear drains both owner tails");
    drive(late, &fixture.spawner).expect("later set survives clear");
    let refresh = drive(refresh, &fixture.spawner);
    assert!(!refresh.aborted && refresh.errors.is_empty());
    assert_eq!(
        fixture
            .registry
            .get_providers()
            .iter()
            .map(|provider| provider.id().as_str())
            .collect::<Vec<_>>(),
        ["late"]
    );
}

#[test]
fn same_provider_publications_follow_ticket_order() {
    let fixture = blocking_fixture();
    let first = model("p", "one");
    let second = model("p", "two");
    let refresher = Arc::new(OutcomeRefresher::new(vec![
        ModelsPublication {
            memory: ModelsMemoryIntent::Set(vec![first.clone()].into()),
            persistence: ModelsPersistenceIntent::Set(ModelsStoreEntry {
                models: vec![first],
                last_modified: None,
                checked_at: None,
                etag: Some("one".to_owned()),
            }),
        },
        ModelsPublication {
            memory: ModelsMemoryIntent::Set(vec![second.clone()].into()),
            persistence: ModelsPersistenceIntent::Set(ModelsStoreEntry {
                models: vec![second],
                last_modified: None,
                checked_at: None,
                etag: Some("two".to_owned()),
            }),
        },
    ]));
    set_blocking_provider(&fixture, outcome_provider("p", refresher.clone()));
    let token = CancellationToken::new();
    let mut refresh = pin!(fixture.registry.refresh(
        ModelsRefreshOptions {
            allow_network: false,
            providers: None,
            force: None,
        },
        &token,
    ));
    assert!(poll(refresh.as_mut()).is_pending());
    fixture.spawner.drive_once();
    assert_eq!(fixture.store.active.load(Ordering::Acquire), 1);
    assert_eq!(
        fixture
            .store
            .admissions
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .len(),
        1
    );
    fixture.store.release(1);
    for _ in 0..16 {
        fixture.spawner.drive_once();
        let _ = poll(refresh.as_mut());
        if fixture
            .store
            .admissions
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .len()
            == 2
        {
            break;
        }
    }
    assert_eq!(fixture.store.active.load(Ordering::Acquire), 1);
    assert_eq!(fixture.store.max_active.load(Ordering::Acquire), 1);
    fixture.store.release(1);
    let result = drive(refresh, &fixture.spawner);
    assert!(result.errors.is_empty());
    assert_eq!(
        refresher
            .outcomes
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_slice(),
        [PublicationOutcome::Published, PublicationOutcome::Published]
    );
    assert_eq!(
        fixture
            .store
            .completions
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .len(),
        2
    );
    assert_eq!(fixture.registry.get_models(None)[0].id.as_str(), "two");
}

#[test]
fn cross_provider_publications_can_overlap() {
    let fixture = blocking_fixture();
    for id in ["a", "b"] {
        set_blocking_provider(
            &fixture,
            outcome_provider(
                id,
                Arc::new(OutcomeRefresher::new(vec![ModelsPublication {
                    memory: ModelsMemoryIntent::Set(vec![model(id, "dynamic")].into()),
                    persistence: ModelsPersistenceIntent::Set(store_entry(
                        model(id, "dynamic"),
                        id,
                    )),
                }])),
            ),
        );
    }
    let token = CancellationToken::new();
    let mut refresh = pin!(fixture.registry.refresh(
        ModelsRefreshOptions {
            allow_network: false,
            providers: None,
            force: None,
        },
        &token,
    ));
    assert!(poll(refresh.as_mut()).is_pending());
    fixture.spawner.drive_once();
    assert_eq!(fixture.store.active.load(Ordering::Acquire), 2);
    assert_eq!(fixture.store.max_active.load(Ordering::Acquire), 2);
    fixture.store.release(2);
    let result = drive(refresh, &fixture.spawner);
    assert!(result.errors.is_empty());
    assert_eq!(fixture.registry.get_models(None).len(), 2);
}

#[test]
fn repeated_publication_two_three_and_superseded_stop_trace() {
    let fixture = reentrant_blocking_fixture();
    let refresher = Arc::new(OutcomeRefresher::new(vec![
        ModelsPublication {
            memory: ModelsMemoryIntent::Keep,
            persistence: ModelsPersistenceIntent::Set(store_entry(model("p", "one"), "one")),
        },
        ModelsPublication {
            memory: ModelsMemoryIntent::Keep,
            persistence: ModelsPersistenceIntent::Set(store_entry(model("p", "two"), "two")),
        },
        ModelsPublication {
            memory: ModelsMemoryIntent::Set(vec![model("p", "stale")].into()),
            persistence: ModelsPersistenceIntent::Set(store_entry(model("p", "three"), "three")),
        },
        ModelsPublication {
            memory: ModelsMemoryIntent::Set(vec![model("p", "forbidden")].into()),
            persistence: ModelsPersistenceIntent::Set(store_entry(model("p", "four"), "four")),
        },
    ]));
    drive_reentrant(
        fixture
            .registry
            .set_provider(outcome_provider("p", refresher.clone())),
        &fixture.spawner,
    )
    .expect("set");
    let token = CancellationToken::new();
    let mut refresh = pin!(fixture.registry.refresh(
        ModelsRefreshOptions {
            allow_network: false,
            providers: None,
            force: None,
        },
        &token,
    ));
    assert!(poll(refresh.as_mut()).is_pending());
    fixture.spawner.drive_once();
    assert_eq!(fixture.store.active.load(Ordering::Acquire), 1);
    assert_eq!(
        fixture
            .store
            .admissions
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .len(),
        1
    );

    fixture.store.release(1);
    fixture.spawner.drive_once();
    assert!(poll(refresh.as_mut()).is_pending());
    fixture.spawner.drive_once();
    assert_eq!(
        fixture
            .store
            .admissions
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .len(),
        2
    );

    fixture.store.release(1);
    fixture.spawner.drive_once();
    let callback_registry = fixture.registry.clone();
    *fixture
        .spawner
        .callback
        .lock()
        .unwrap_or_else(|p| p.into_inner()) = Some(Box::new(move || {
        let replacement = provider(
            "p",
            vec![model("p", "replacement")],
            Arc::new(TestApiKey::configured()),
            None,
            None,
        );
        let mut mutation = Box::pin(callback_registry.set_provider(replacement));
        assert!(poll(mutation.as_mut()).is_pending());
        drop(mutation);
    }));
    let result = match poll(refresh.as_mut()) {
        Poll::Ready(result) => result,
        Poll::Pending => drive_reentrant(refresh, &fixture.spawner),
    };
    for _ in 0..8 {
        fixture.spawner.drive_once();
    }
    assert!(!result.aborted && result.errors.is_empty());
    assert_eq!(
        refresher
            .outcomes
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_slice(),
        [
            PublicationOutcome::Published,
            PublicationOutcome::Published,
            PublicationOutcome::Superseded,
        ]
    );
    assert_eq!(
        fixture
            .store
            .admissions
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .len(),
        2
    );
    assert_eq!(fixture.spawner.calls.load(Ordering::Acquire), 5);
    assert_eq!(
        fixture.registry.get_models(None)[0].id.as_str(),
        "replacement"
    );
}

struct DropFirstPublicationRefresher;

impl ProviderModelsRefresher for DropFirstPublicationRefresher {
    fn refresh_models<'a>(
        &'a self,
        context: pi_ai_core::RefreshModelsContext<'a>,
    ) -> BoxFuture<'a, Result<(), ModelsError>> {
        Box::pin(async move {
            let dropped = context.publisher.publish(ModelsPublication {
                memory: ModelsMemoryIntent::Set(vec![model("p", "dropped")].into()),
                persistence: ModelsPersistenceIntent::Keep,
            });
            drop(dropped);
            assert_eq!(
                context
                    .publisher
                    .publish(ModelsPublication {
                        memory: ModelsMemoryIntent::Set(vec![model("p", "kept")].into()),
                        persistence: ModelsPersistenceIntent::Keep,
                    })
                    .await?,
                PublicationOutcome::Published
            );
            Ok(())
        })
    }
}

struct StaleBeforePollRefresher {
    callback: Mutex<Option<Reentry>>,
    outcome: Mutex<Option<PublicationOutcome>>,
}

impl ProviderModelsRefresher for StaleBeforePollRefresher {
    fn refresh_models<'a>(
        &'a self,
        context: pi_ai_core::RefreshModelsContext<'a>,
    ) -> BoxFuture<'a, Result<(), ModelsError>> {
        Box::pin(async move {
            let publication = context.publisher.publish(ModelsPublication {
                memory: ModelsMemoryIntent::Set(vec![model("p", "stale")].into()),
                persistence: ModelsPersistenceIntent::Set(store_entry(
                    model("p", "stale"),
                    "stale",
                )),
            });
            let callback = self
                .callback
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .take()
                .expect("replacement callback");
            callback();
            *self.outcome.lock().unwrap_or_else(|p| p.into_inner()) = Some(publication.await?);
            Ok(())
        })
    }
}

struct StaleBeforePollProvider {
    inner: TestProvider,
    refresher: Arc<StaleBeforePollRefresher>,
}

impl Provider for StaleBeforePollProvider {
    fn id(&self) -> &ProviderId {
        &self.inner.id
    }
    fn name(&self) -> &str {
        &self.inner.name
    }
    fn auth(&self) -> &ProviderAuth {
        &self.inner.auth
    }
    fn models(&self) -> Result<Arc<[Model]>, ModelsError> {
        self.inner.models()
    }
    fn models_refresher(&self) -> Option<&(dyn ProviderModelsRefresher + '_)> {
        Some(self.refresher.as_ref())
    }
    fn adapter(&self, api: &ApiId) -> Option<Arc<dyn ApiAdapter>> {
        self.inner.adapter(api)
    }
}

struct DropAdmittedPublicationRefresher {
    second: Mutex<Option<PublicationOutcome>>,
}

impl ProviderModelsRefresher for DropAdmittedPublicationRefresher {
    fn refresh_models<'a>(
        &'a self,
        context: pi_ai_core::RefreshModelsContext<'a>,
    ) -> BoxFuture<'a, Result<(), ModelsError>> {
        Box::pin(async move {
            let mut first = context.publisher.publish(ModelsPublication {
                memory: ModelsMemoryIntent::Keep,
                persistence: ModelsPersistenceIntent::Set(store_entry(
                    model("p", "first"),
                    "first",
                )),
            });
            poll_fn(|task_context| {
                assert!(first.as_mut().poll(task_context).is_pending());
                Poll::Ready(())
            })
            .await;
            drop(first);
            let second = context
                .publisher
                .publish(ModelsPublication {
                    memory: ModelsMemoryIntent::Set(vec![model("p", "second")].into()),
                    persistence: ModelsPersistenceIntent::Set(store_entry(
                        model("p", "second"),
                        "second",
                    )),
                })
                .await?;
            *self.second.lock().unwrap_or_else(|p| p.into_inner()) = Some(second);
            Ok(())
        })
    }
}

struct DropAdmittedProvider {
    inner: TestProvider,
    refresher: Arc<DropAdmittedPublicationRefresher>,
}

impl Provider for DropAdmittedProvider {
    fn id(&self) -> &ProviderId {
        &self.inner.id
    }
    fn name(&self) -> &str {
        &self.inner.name
    }
    fn auth(&self) -> &ProviderAuth {
        &self.inner.auth
    }
    fn models(&self) -> Result<Arc<[Model]>, ModelsError> {
        self.inner.models()
    }
    fn models_refresher(&self) -> Option<&(dyn ProviderModelsRefresher + '_)> {
        Some(self.refresher.as_ref())
    }
    fn adapter(&self, api: &ApiId) -> Option<Arc<dyn ApiAdapter>> {
        self.inner.adapter(api)
    }
}

struct DropProvider {
    inner: TestProvider,
    refresher: DropFirstPublicationRefresher,
}

impl Provider for DropProvider {
    fn id(&self) -> &ProviderId {
        &self.inner.id
    }
    fn name(&self) -> &str {
        &self.inner.name
    }
    fn auth(&self) -> &ProviderAuth {
        &self.inner.auth
    }
    fn models(&self) -> Result<Arc<[Model]>, ModelsError> {
        self.inner.models()
    }
    fn models_refresher(&self) -> Option<&(dyn ProviderModelsRefresher + '_)> {
        Some(&self.refresher)
    }
    fn adapter(&self, api: &ApiId) -> Option<Arc<dyn ApiAdapter>> {
        self.inner.adapter(api)
    }
}

#[test]
fn dropping_unpolled_publication_cancels_only_its_ticket() {
    let fixture = fixture(ModelHeaderPolicy::default());
    let base = provider(
        "p",
        Vec::new(),
        Arc::new(TestApiKey::configured()),
        None,
        None,
    );
    let inner = Arc::try_unwrap(base).ok().expect("unique provider");
    drive(
        fixture.registry.set_provider(Arc::new(DropProvider {
            inner,
            refresher: DropFirstPublicationRefresher,
        })),
        &fixture.spawner,
    )
    .expect("set");
    let result = drive(
        fixture.registry.refresh(
            ModelsRefreshOptions {
                allow_network: false,
                providers: None,
                force: None,
            },
            &CancellationToken::new(),
        ),
        &fixture.spawner,
    );
    assert!(result.errors.is_empty());
    assert_eq!(fixture.registry.get_models(None)[0].id.as_str(), "kept");

    let fixture = reentrant_blocking_fixture();
    let refresher = Arc::new(StaleBeforePollRefresher {
        callback: Mutex::new(None),
        outcome: Mutex::new(None),
    });
    let base = provider(
        "p",
        Vec::new(),
        Arc::new(TestApiKey::configured()),
        None,
        None,
    );
    drive_reentrant(
        fixture
            .registry
            .set_provider(Arc::new(StaleBeforePollProvider {
                inner: Arc::try_unwrap(base).ok().expect("unique"),
                refresher: refresher.clone(),
            })),
        &fixture.spawner,
    )
    .expect("set");
    let callback_registry = fixture.registry.clone();
    *refresher.callback.lock().unwrap_or_else(|p| p.into_inner()) = Some(Box::new(move || {
        let replacement = provider(
            "p",
            vec![model("p", "replacement")],
            Arc::new(TestApiKey::configured()),
            None,
            None,
        );
        let mut mutation = Box::pin(callback_registry.set_provider(replacement));
        assert!(poll(mutation.as_mut()).is_pending());
        drop(mutation);
    }));
    let result = drive_reentrant(
        fixture.registry.refresh(
            ModelsRefreshOptions {
                allow_network: false,
                providers: None,
                force: None,
            },
            &CancellationToken::new(),
        ),
        &fixture.spawner,
    );
    for _ in 0..8 {
        fixture.spawner.drive_once();
    }
    assert!(!result.aborted && result.errors.is_empty());
    assert_eq!(
        *refresher.outcome.lock().unwrap_or_else(|p| p.into_inner()),
        Some(PublicationOutcome::Superseded)
    );
    assert_eq!(
        fixture.spawner.calls.load(Ordering::Acquire),
        2,
        "stale reservation must not call the publication spawner"
    );
    assert!(
        fixture
            .store
            .admissions
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .is_empty()
    );
}

#[test]
fn dropping_admitted_publication_finishes_before_next_ticket() {
    let fixture = blocking_fixture();
    let refresher = Arc::new(DropAdmittedPublicationRefresher {
        second: Mutex::new(None),
    });
    let base = provider(
        "p",
        Vec::new(),
        Arc::new(TestApiKey::configured()),
        None,
        None,
    );
    set_blocking_provider(
        &fixture,
        Arc::new(DropAdmittedProvider {
            inner: Arc::try_unwrap(base).ok().expect("unique"),
            refresher: refresher.clone(),
        }),
    );
    let token = CancellationToken::new();
    let mut refresh = pin!(fixture.registry.refresh(
        ModelsRefreshOptions {
            allow_network: false,
            providers: None,
            force: None,
        },
        &token,
    ));
    assert!(poll(refresh.as_mut()).is_pending());
    fixture.spawner.drive_once();
    assert_eq!(
        fixture
            .store
            .admissions
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .len(),
        1
    );
    assert_eq!(fixture.store.active.load(Ordering::Acquire), 1);
    fixture.store.release(1);
    for _ in 0..16 {
        fixture.spawner.drive_once();
        let _ = poll(refresh.as_mut());
        if fixture
            .store
            .admissions
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .len()
            == 2
        {
            break;
        }
    }
    assert_eq!(
        fixture
            .store
            .admissions
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .len(),
        2
    );
    assert_eq!(fixture.store.max_active.load(Ordering::Acquire), 1);
    fixture.store.release(1);
    let result = drive(refresh, &fixture.spawner);
    assert!(result.errors.is_empty());
    assert_eq!(
        *refresher.second.lock().unwrap_or_else(|p| p.into_inner()),
        Some(PublicationOutcome::Published)
    );
    assert_eq!(fixture.registry.get_models(None)[0].id.as_str(), "second");

    let fixture = reentrant_blocking_fixture();
    set_blocking_provider_reentrant(
        &fixture,
        outcome_provider(
            "p",
            Arc::new(OutcomeRefresher::new(vec![ModelsPublication {
                memory: ModelsMemoryIntent::Keep,
                persistence: ModelsPersistenceIntent::Set(store_entry(
                    model("p", "admitted"),
                    "admitted",
                )),
            }])),
        ),
    );
    let token = CancellationToken::new();
    let mut refresh = Box::pin(fixture.registry.refresh(
        ModelsRefreshOptions {
            allow_network: false,
            providers: None,
            force: None,
        },
        &token,
    ));
    let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let wakes = Arc::new(AtomicUsize::new(0));
    let waker = Waker::from(Arc::new(RegistryDropWake {
        registry: fixture.registry.clone(),
        dropped: dropped.clone(),
        wakes: wakes.clone(),
    }));
    assert!(matches!(
        refresh.as_mut().poll(&mut TaskContext::from_waker(&waker)),
        Poll::Pending
    ));
    fixture.spawner.drive_once();
    assert_eq!(fixture.store.active.load(Ordering::Acquire), 1);
    drop(waker);
    drop(refresh);
    assert!(dropped.load(Ordering::Acquire));
    fixture.store.release(1);
    for _ in 0..8 {
        fixture.spawner.drive_once();
    }
    assert_eq!(wakes.load(Ordering::Acquire), 0);
}

#[test]
fn persistence_fence_before_during_after_admission_matrix() {
    #[derive(Clone, Copy)]
    enum Stage {
        Before,
        During,
        AfterEffect,
    }
    for stage in [Stage::Before, Stage::During, Stage::AfterEffect] {
        for delete in [false, true] {
            let fixture = blocking_fixture();
            let id = ProviderId::new("p").expect("id");
            fixture
                .store
                .entries
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .insert(id.clone(), store_entry(model("p", "prior"), "prior"));
            if matches!(stage, Stage::AfterEffect) {
                fixture.store.hold_ack();
            }
            let persistence = if delete {
                ModelsPersistenceIntent::Delete
            } else {
                ModelsPersistenceIntent::Set(store_entry(model("p", "persisted"), "persisted"))
            };
            let refresher = Arc::new(OutcomeRefresher::new(vec![ModelsPublication {
                memory: ModelsMemoryIntent::Set(vec![model("p", "memory")].into()),
                persistence,
            }]));
            set_blocking_provider(&fixture, outcome_provider("p", refresher.clone()));
            let token = CancellationToken::new();
            let mut refresh = pin!(fixture.registry.refresh(
                ModelsRefreshOptions {
                    allow_network: false,
                    providers: None,
                    force: None,
                },
                &token,
            ));
            assert!(poll(refresh.as_mut()).is_pending());
            if matches!(stage, Stage::During | Stage::AfterEffect) {
                fixture.spawner.drive_once();
                assert_eq!(fixture.store.active.load(Ordering::Acquire), 1);
            }
            if matches!(stage, Stage::AfterEffect) {
                fixture.store.release(1);
                fixture.spawner.drive_once();
                let persisted = fixture
                    .store
                    .entries
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .get(&id)
                    .cloned();
                assert_eq!(persisted.is_none(), delete);
                if !delete {
                    assert_eq!(
                        persisted.and_then(|entry| entry.etag).as_deref(),
                        Some("persisted")
                    );
                }
            }
            let replacement = provider(
                "p",
                vec![model("p", "replacement")],
                Arc::new(TestApiKey::configured()),
                None,
                None,
            );
            let mut replacement = pin!(fixture.registry.set_provider(replacement));
            assert!(poll(replacement.as_mut()).is_pending());
            if matches!(stage, Stage::During) {
                fixture.store.release(1);
            }
            if matches!(stage, Stage::AfterEffect) {
                fixture.store.release_ack();
            }
            let replacement_result = drive(replacement, &fixture.spawner);
            replacement_result.expect("replacement drains publication");
            let refresh_result = drive(refresh, &fixture.spawner);
            assert!(!refresh_result.aborted && refresh_result.errors.is_empty());
            let admissions = fixture
                .store
                .admissions
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .len();
            match stage {
                Stage::Before => {
                    assert_eq!(admissions, 0);
                    let entry = fixture
                        .store
                        .entries
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .get(&id)
                        .cloned()
                        .expect("prior remains");
                    assert_eq!(entry.etag.as_deref(), Some("prior"));
                }
                Stage::During | Stage::AfterEffect => {
                    assert_eq!(admissions, 1);
                    let persisted = fixture
                        .store
                        .entries
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .get(&id)
                        .cloned();
                    assert_eq!(persisted.is_none(), delete);
                }
            }
            assert_eq!(
                fixture.registry.get_models(None)[0].id.as_str(),
                "replacement"
            );
            assert!(
                fixture
                    .registry
                    .get_model(&id, &ModelId::new("memory").expect("id"))
                    .is_none()
            );
            let outcomes = refresher.outcomes.lock().unwrap_or_else(|p| p.into_inner());
            assert!(
                outcomes.is_empty() || outcomes.as_slice() == [PublicationOutcome::Superseded],
                "stale publication may be dropped by generation cancellation or observe Superseded"
            );
        }
    }
}
