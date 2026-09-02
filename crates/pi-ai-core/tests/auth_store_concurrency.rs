use std::collections::BTreeMap;
use std::future::Future;
use std::pin::{Pin, pin};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex, OnceLock};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::Duration;

use pi_ai_core::{
    ApiKeyAuth, ApiKeyAuthInput, ApiKeyCredential, ApiKeyCredentialType, AuthContext,
    AuthContextError, AuthProviderError, AuthResolutionError, AuthResolutionOverrides, AuthResult,
    BoxFuture, CancellationToken, Cancelled, Clock, Credential, CredentialInfo, CredentialMutation,
    CredentialMutationValue, CredentialReconciliation, CredentialRevision, CredentialStore,
    CredentialStoreError, CredentialStoreErrorCode, InMemoryCredentialStore, InMemoryModelsStore,
    ModelAuth, ModelHeaderPolicy, ModelsErrorCode, ModelsStore, ModelsStoreEntry, ModelsStoreError,
    ModelsStoreErrorCode, OAuthAuth, OAuthCredential, OAuthCredentialType, OAuthCredentials,
    OAuthExtraFields, ProviderAuth, ProviderId, RawModelsStoreEntry, SafeUnsigned, SecretString,
    VersionedCredential, resolve_provider_auth,
};

fn poll_with<F: Future + ?Sized>(future: Pin<&mut F>, waker: &Waker) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(waker))
}

fn poll_noop<F: Future + ?Sized>(future: Pin<&mut F>) -> Poll<F::Output> {
    poll_with(future, Waker::noop())
}

fn ready<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    match poll_noop(future.as_mut()) {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("future must already be ready"),
    }
}

struct ThreadWake(thread::Thread);

impl Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(ThreadWake(thread::current())));
    let mut future = pin!(future);
    loop {
        if let Poll::Ready(value) = poll_with(future.as_mut(), &waker) {
            return value;
        }
        thread::park();
    }
}

fn provider(name: &str) -> ProviderId {
    ProviderId::new(name).expect("nonempty provider")
}

fn api_credential(value: &str) -> Credential {
    Credential::ApiKey(ApiKeyCredential {
        credential_type: ApiKeyCredentialType::ApiKey,
        key: Some(SecretString::new(value)),
        env: None,
    })
}

fn oauth_credential(access: &str, expires: u64) -> OAuthCredential {
    OAuthCredential {
        credential_type: OAuthCredentialType::OAuth,
        credentials: OAuthCredentials {
            refresh: SecretString::new("refresh"),
            access: SecretString::new(access),
            expires: SafeUnsigned::new(expires).expect("safe expiry"),
            extra: OAuthExtraFields::new(BTreeMap::new()).expect("empty extras"),
        },
    }
}

fn set_credential(store: &dyn CredentialStore, provider: &ProviderId, credential: Credential) {
    let token = CancellationToken::new();
    let lease = ready(store.begin_mutation(provider, &token)).expect("lease acquired");
    ready(lease.commit(CredentialMutationValue::Set(credential))).expect("commit succeeds");
}

#[derive(Default)]
struct EmptyContext;

static EMPTY_CONTEXT: EmptyContext = EmptyContext;

impl AuthContext for EmptyContext {
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

struct NeverClock {
    now: u64,
}

impl Clock for NeverClock {
    fn now_unix_ms(&self) -> u64 {
        self.now
    }

    fn sleep<'a>(
        &'a self,
        _duration: Duration,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), Cancelled>> {
        Box::pin(std::future::pending())
    }
}

struct DropReenterWake {
    callback: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    dropped: Arc<AtomicBool>,
    wakes: AtomicUsize,
}

struct OnceCallbackWake {
    callback: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

impl Wake for OnceCallbackWake {
    fn wake(self: Arc<Self>) {
        let callback = self
            .callback
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .take();
        if let Some(callback) = callback {
            callback();
        }
    }
}

impl Wake for DropReenterWake {
    fn wake(self: Arc<Self>) {
        self.wakes.fetch_add(1, Ordering::Relaxed);
    }
}

impl Drop for DropReenterWake {
    fn drop(&mut self) {
        if let Some(callback) = self
            .callback
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .take()
        {
            callback();
        }
        self.dropped.store(true, Ordering::Release);
    }
}

fn drop_reenter_waker(callback: impl FnOnce() + Send + 'static) -> (Waker, Arc<AtomicBool>) {
    let dropped = Arc::new(AtomicBool::new(false));
    (
        Waker::from(Arc::new(DropReenterWake {
            callback: Mutex::new(Some(Box::new(callback))),
            dropped: Arc::clone(&dropped),
            wakes: AtomicUsize::new(0),
        })),
        dropped,
    )
}

#[test]
fn credential_gate_is_fifo_cross_provider_and_hands_off_dropped_waiters() {
    let store = InMemoryCredentialStore::new();
    let a = provider("a");
    let b = provider("b");
    let token = CancellationToken::new();
    let first = ready(store.begin_mutation(&a, &token)).expect("first lease");
    let other = ready(store.begin_mutation(&b, &token)).expect("other provider is independent");

    let mut second = store.begin_mutation(&a, &token);
    let mut third = store.begin_mutation(&a, &token);
    assert!(matches!(poll_noop(second.as_mut()), Poll::Pending));
    assert!(matches!(poll_noop(third.as_mut()), Poll::Pending));
    drop(first);
    assert!(matches!(poll_noop(third.as_mut()), Poll::Pending));
    let second_lease = match poll_noop(second.as_mut()) {
        Poll::Ready(Ok(lease)) => lease,
        _ => panic!("second waiter must receive the first FIFO grant"),
    };
    drop(second_lease);
    let third_lease = match poll_noop(third.as_mut()) {
        Poll::Ready(Ok(lease)) => lease,
        _ => panic!("third waiter must receive the second FIFO grant"),
    };
    drop(third_lease);

    let first = ready(store.begin_mutation(&a, &token)).expect("first lease");
    let mut dropped_queued = store.begin_mutation(&a, &token);
    let mut granted_then_dropped = store.begin_mutation(&a, &token);
    let mut final_waiter = store.begin_mutation(&a, &token);
    assert!(matches!(poll_noop(dropped_queued.as_mut()), Poll::Pending));
    assert!(matches!(
        poll_noop(granted_then_dropped.as_mut()),
        Poll::Pending
    ));
    assert!(matches!(poll_noop(final_waiter.as_mut()), Poll::Pending));
    drop(dropped_queued);
    drop(first);
    drop(granted_then_dropped);
    let final_lease = match poll_noop(final_waiter.as_mut()) {
        Poll::Ready(Ok(lease)) => lease,
        _ => panic!("queued and granted drops must both hand off"),
    };
    drop(final_lease);

    let first = ready(store.begin_mutation(&a, &token)).expect("first lease");
    let mut second = store.begin_mutation(&a, &token);
    let mut third = store.begin_mutation(&a, &token);
    assert!(matches!(poll_noop(second.as_mut()), Poll::Pending));
    assert!(matches!(poll_noop(third.as_mut()), Poll::Pending));
    drop(first);
    drop(second);
    let third = match poll_noop(third.as_mut()) {
        Poll::Ready(Ok(lease)) => lease,
        _ => panic!("granted second waiter drop must hand off to third"),
    };
    drop(third);
    drop(other);

    let first = ready(store.begin_mutation(&a, &token)).expect("first lease");
    let cancelled = CancellationToken::new();
    let mut queued = store.begin_mutation(&a, &cancelled);
    assert!(matches!(poll_noop(queued.as_mut()), Poll::Pending));
    cancelled.cancel();
    assert!(matches!(
        poll_noop(queued.as_mut()),
        Poll::Ready(Err(error)) if error.code() == CredentialStoreErrorCode::Cancelled
    ));
    drop(first);
    assert!(ready(store.begin_mutation(&a, &token)).is_ok());
}

#[test]
fn credential_gate_drops_displaced_reentrant_wakers_after_unlock() {
    let store = Arc::new(InMemoryCredentialStore::new());
    let provider = provider("p");
    let token = CancellationToken::new();
    let first = ready(store.begin_mutation(&provider, &token)).expect("first lease");
    let mut queued = store.begin_mutation(&provider, &token);
    let callback_store = Arc::clone(&store);
    let callback_provider = provider.clone();
    let callback_token = token.clone();
    let (waker, dropped) = drop_reenter_waker(move || {
        let _ = ready(callback_store.read(&callback_provider, &callback_token));
    });
    assert!(matches!(poll_with(queued.as_mut(), &waker), Poll::Pending));
    drop(waker);
    assert!(matches!(poll_noop(queued.as_mut()), Poll::Pending));
    assert!(dropped.load(Ordering::Acquire));
    drop(queued);
    drop(first);
}

#[test]
fn queued_and_granted_cancellation_handoff_without_cancelled_waiter_repoll() {
    let store = InMemoryCredentialStore::new();
    let provider = provider("p");
    let active = CancellationToken::new();

    let first = ready(store.begin_mutation(&provider, &active)).expect("first lease");
    let queued_token = CancellationToken::new();
    let mut cancelled_queued = store.begin_mutation(&provider, &queued_token);
    let mut next = store.begin_mutation(&provider, &active);
    assert!(matches!(
        poll_noop(cancelled_queued.as_mut()),
        Poll::Pending
    ));
    assert!(matches!(poll_noop(next.as_mut()), Poll::Pending));
    queued_token.cancel();
    drop(first);
    let next_lease = match poll_noop(next.as_mut()) {
        Poll::Ready(Ok(lease)) => lease,
        _ => panic!("queued cancellation must hand off without cancelled waiter repoll"),
    };
    drop(next_lease);
    drop(cancelled_queued);

    let first = ready(store.begin_mutation(&provider, &active)).expect("first lease");
    let granted_token = CancellationToken::new();
    let mut cancelled_granted = store.begin_mutation(&provider, &granted_token);
    let mut final_waiter = store.begin_mutation(&provider, &active);
    assert!(matches!(
        poll_noop(cancelled_granted.as_mut()),
        Poll::Pending
    ));
    assert!(matches!(poll_noop(final_waiter.as_mut()), Poll::Pending));
    drop(first);
    granted_token.cancel();
    let final_lease = match poll_noop(final_waiter.as_mut()) {
        Poll::Ready(Ok(lease)) => lease,
        _ => panic!("granted cancellation must hand off without cancelled waiter repoll"),
    };
    drop(final_lease);
    drop(cancelled_granted);
}

#[test]
fn grant_wake_callback_can_cancel_reenter_and_repoll_without_stranding_gate() {
    let store = Arc::new(InMemoryCredentialStore::new());
    let provider = provider("p");
    let active = CancellationToken::new();
    let first = ready(store.begin_mutation(&provider, &active)).expect("first lease");
    let cancelled = CancellationToken::new();
    let mut contender = store.begin_mutation(&provider, &cancelled);
    let mut next = store.begin_mutation(&provider, &active);
    let callback_store = Arc::clone(&store);
    let callback_provider = provider.clone();
    let callback_cancelled = cancelled.clone();
    let callback_active = active.clone();
    let reentered = Arc::new(AtomicBool::new(false));
    let callback_reentered = Arc::clone(&reentered);
    let waker = Waker::from(Arc::new(OnceCallbackWake {
        callback: Mutex::new(Some(Box::new(move || {
            callback_cancelled.cancel();
            let _ = ready(callback_store.read(&callback_provider, &callback_active));
            callback_reentered.store(true, Ordering::Release);
        }))),
    }));
    assert!(matches!(
        poll_with(contender.as_mut(), &waker),
        Poll::Pending
    ));
    assert!(matches!(poll_noop(next.as_mut()), Poll::Pending));
    drop(first);
    assert!(reentered.load(Ordering::Acquire));
    assert!(matches!(
        poll_noop(contender.as_mut()),
        Poll::Ready(Err(error)) if error.code() == CredentialStoreErrorCode::Cancelled
    ));
    let next_lease = match poll_noop(next.as_mut()) {
        Poll::Ready(Ok(lease)) => lease,
        _ => panic!("grant callback cancellation must hand off before repoll"),
    };
    drop(next_lease);
}

struct BarrierStore {
    inner: Arc<InMemoryCredentialStore>,
    reads: Arc<Barrier>,
}

impl CredentialStore for BarrierStore {
    fn read<'a>(
        &'a self,
        provider: &'a ProviderId,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<VersionedCredential>, CredentialStoreError>> {
        Box::pin(async move {
            let value = self.inner.read(provider, cancellation).await?;
            self.reads.wait();
            Ok(value)
        })
    }

    fn list<'a>(
        &'a self,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Vec<CredentialInfo>, CredentialStoreError>> {
        self.inner.list(cancellation)
    }

    fn begin_mutation<'a>(
        &'a self,
        provider: &'a ProviderId,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Box<dyn CredentialMutation + 'static>, CredentialStoreError>> {
        self.inner.begin_mutation(provider, cancellation)
    }

    fn reconcile<'a>(
        &'a self,
        provider: &'a ProviderId,
        resolution: CredentialReconciliation,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<VersionedCredential>, CredentialStoreError>> {
        self.inner.reconcile(provider, resolution, cancellation)
    }
}

struct CountingOAuth {
    refreshes: AtomicUsize,
}

impl OAuthAuth for CountingOAuth {
    fn name(&self) -> &str {
        "counting"
    }

    fn login<'a>(
        &'a self,
        _interaction: &'a dyn pi_ai_core::AuthInteraction,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<OAuthCredential, AuthProviderError>> {
        Box::pin(async { Err(AuthProviderError::new()) })
    }

    fn refresh<'a>(
        &'a self,
        _credential: &'a OAuthCredential,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<OAuthCredential, AuthProviderError>> {
        Box::pin(async move {
            self.refreshes.fetch_add(1, Ordering::AcqRel);
            Ok(oauth_credential("new", 1_000_000))
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

#[test]
fn thirty_two_oauth_callers_refresh_exactly_once() {
    const CALLERS: usize = 32;
    let inner = Arc::new(InMemoryCredentialStore::new());
    let provider = provider("p");
    set_credential(
        inner.as_ref(),
        &provider,
        Credential::OAuth(oauth_credential("old", 0)),
    );
    let inner_for_assert = Arc::clone(&inner);
    let store: Arc<dyn CredentialStore> = Arc::new(BarrierStore {
        inner,
        reads: Arc::new(Barrier::new(CALLERS)),
    });
    let oauth = Arc::new(CountingOAuth {
        refreshes: AtomicUsize::new(0),
    });
    let auth = ProviderAuth::new(None, Some(oauth.clone())).expect("valid auth");
    let context: Arc<dyn AuthContext> = Arc::new(EmptyContext);
    let clock: Arc<dyn Clock> = Arc::new(NeverClock { now: 100 });

    let threads = (0..CALLERS)
        .map(|_| {
            let provider = provider.clone();
            let store = Arc::clone(&store);
            let auth = auth.clone();
            let context = Arc::clone(&context);
            let clock = Arc::clone(&clock);
            thread::spawn(move || {
                let token = CancellationToken::new();
                block_on(resolve_provider_auth(
                    &provider,
                    &auth,
                    store.as_ref(),
                    context.as_ref(),
                    clock.as_ref(),
                    AuthResolutionOverrides::default(),
                    &token,
                ))
                .expect("resolution succeeds")
                .expect("OAuth result")
                .auth
                .api_key
                .expect("key")
                .expose_secret()
                .to_owned()
            })
        })
        .collect::<Vec<_>>();
    let results = threads
        .into_iter()
        .map(|thread| thread.join().expect("caller thread"))
        .collect::<Vec<_>>();
    assert!(results.iter().all(|value| value == "new"));
    assert_eq!(oauth.refreshes.load(Ordering::Acquire), 1);
    let token = CancellationToken::new();
    let stored = ready(inner_for_assert.read(&provider, &token))
        .expect("read succeeds")
        .expect("credential present");
    assert_eq!(stored.revision.get(), 2);
    let lease = ready(inner_for_assert.begin_mutation(&provider, &token))
        .expect("no acquisition remains stranded");
    drop(lease);
    assert_eq!(
        ready(inner_for_assert.read(&provider, &token))
            .expect("read succeeds")
            .expect("credential present")
            .revision
            .get(),
        2
    );
}

struct DropPending<T> {
    dropped: Arc<AtomicBool>,
    marker: std::marker::PhantomData<T>,
}

impl<T> DropPending<T> {
    fn new(dropped: Arc<AtomicBool>) -> Self {
        Self {
            dropped,
            marker: std::marker::PhantomData,
        }
    }
}

impl<T> Future for DropPending<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Pending
    }
}

impl<T> Drop for DropPending<T> {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::Release);
    }
}

struct PendingApiKey {
    dropped: Arc<AtomicBool>,
}

struct PendingReadStore {
    dropped: Arc<AtomicBool>,
}

struct PendingAcquireStore {
    current: VersionedCredential,
    dropped: Arc<AtomicBool>,
}

impl CredentialStore for PendingAcquireStore {
    fn read<'a>(
        &'a self,
        _provider: &'a ProviderId,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<VersionedCredential>, CredentialStoreError>> {
        let current = self.current.clone();
        Box::pin(async move { Ok(Some(current)) })
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
    ) -> BoxFuture<'a, Result<Box<dyn CredentialMutation + 'static>, CredentialStoreError>> {
        Box::pin(DropPending::new(Arc::clone(&self.dropped)))
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

struct PendingContext {
    dropped: Arc<AtomicBool>,
}

impl AuthContext for PendingContext {
    fn env<'a>(
        &'a self,
        _name: &'a str,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<String>, AuthContextError>> {
        Box::pin(DropPending::new(Arc::clone(&self.dropped)))
    }

    fn file_exists<'a>(
        &'a self,
        _path: &'a str,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<bool, AuthContextError>> {
        Box::pin(async { Ok(false) })
    }
}

struct NestedContextApi;

impl ApiKeyAuth for NestedContextApi {
    fn name(&self) -> &str {
        "nested context"
    }

    fn resolve<'a>(
        &'a self,
        input: ApiKeyAuthInput<'a>,
    ) -> BoxFuture<'a, Result<Option<AuthResult>, AuthProviderError>> {
        Box::pin(async move {
            input
                .context
                .env("NESTED", input.cancellation)
                .await
                .map_err(AuthProviderError::with_source)?;
            Ok(None)
        })
    }
}

impl CredentialStore for PendingReadStore {
    fn read<'a>(
        &'a self,
        _provider: &'a ProviderId,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<VersionedCredential>, CredentialStoreError>> {
        Box::pin(DropPending::new(Arc::clone(&self.dropped)))
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
    ) -> BoxFuture<'a, Result<Box<dyn CredentialMutation + 'static>, CredentialStoreError>> {
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

impl ApiKeyAuth for PendingApiKey {
    fn name(&self) -> &str {
        "pending"
    }

    fn resolve<'a>(
        &'a self,
        _input: ApiKeyAuthInput<'a>,
    ) -> BoxFuture<'a, Result<Option<AuthResult>, AuthProviderError>> {
        Box::pin(DropPending::new(Arc::clone(&self.dropped)))
    }
}

enum PendingOAuthPhase {
    Refresh,
    ToAuth,
}

struct PendingOAuth {
    phase: PendingOAuthPhase,
    dropped: Arc<AtomicBool>,
}

struct RefreshThenPendingToAuth {
    dropped: Arc<AtomicBool>,
}

impl OAuthAuth for RefreshThenPendingToAuth {
    fn name(&self) -> &str {
        "refresh then pending derivation"
    }

    fn login<'a>(
        &'a self,
        _interaction: &'a dyn pi_ai_core::AuthInteraction,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<OAuthCredential, AuthProviderError>> {
        Box::pin(async { Err(AuthProviderError::new()) })
    }

    fn refresh<'a>(
        &'a self,
        _credential: &'a OAuthCredential,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<OAuthCredential, AuthProviderError>> {
        Box::pin(async { Ok(oauth_credential("rotated", 1_000_000)) })
    }

    fn to_auth<'a>(
        &'a self,
        _credential: &'a OAuthCredential,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<ModelAuth, AuthProviderError>> {
        Box::pin(DropPending::new(Arc::clone(&self.dropped)))
    }
}

impl OAuthAuth for PendingOAuth {
    fn name(&self) -> &str {
        "pending"
    }

    fn login<'a>(
        &'a self,
        _interaction: &'a dyn pi_ai_core::AuthInteraction,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<OAuthCredential, AuthProviderError>> {
        Box::pin(async { Err(AuthProviderError::new()) })
    }

    fn refresh<'a>(
        &'a self,
        credential: &'a OAuthCredential,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<OAuthCredential, AuthProviderError>> {
        match self.phase {
            PendingOAuthPhase::Refresh => Box::pin(DropPending::new(Arc::clone(&self.dropped))),
            PendingOAuthPhase::ToAuth => {
                let credential = credential.clone();
                Box::pin(async move { Ok(credential) })
            }
        }
    }

    fn to_auth<'a>(
        &'a self,
        credential: &'a OAuthCredential,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<ModelAuth, AuthProviderError>> {
        match self.phase {
            PendingOAuthPhase::Refresh => {
                let credential = credential.clone();
                Box::pin(async move {
                    Ok(ModelAuth {
                        api_key: Some(credential.credentials.access),
                        headers: None,
                        base_url: None,
                    })
                })
            }
            PendingOAuthPhase::ToAuth => Box::pin(DropPending::new(Arc::clone(&self.dropped))),
        }
    }
}

fn assert_cancellation_drops_pending(
    mut future: BoxFuture<'_, Result<Option<AuthResult>, AuthResolutionError>>,
    token: &CancellationToken,
    dropped: &AtomicBool,
) {
    assert!(matches!(poll_noop(future.as_mut()), Poll::Pending));
    token.cancel();
    assert!(matches!(
        poll_noop(future.as_mut()),
        Poll::Ready(Err(AuthResolutionError::Cancelled))
    ));
    assert!(dropped.load(Ordering::Acquire));
}

#[test]
fn cancellation_drops_noncooperative_api_refresh_and_to_auth_futures() {
    let context = EmptyContext;
    let clock = NeverClock { now: 100 };

    let read_dropped = Arc::new(AtomicBool::new(false));
    let read_store = PendingReadStore {
        dropped: Arc::clone(&read_dropped),
    };
    let read_provider = provider("read");
    let read_auth = ProviderAuth::new(
        Some(Arc::new(PendingApiKey {
            dropped: Arc::new(AtomicBool::new(false)),
        })),
        None,
    )
    .expect("valid auth");
    let read_token = CancellationToken::new();
    assert_cancellation_drops_pending(
        resolve_provider_auth(
            &read_provider,
            &read_auth,
            &read_store,
            &context,
            &clock,
            AuthResolutionOverrides::default(),
            &read_token,
        ),
        &read_token,
        &read_dropped,
    );

    for mode in ["explicit", "stored", "ambient"] {
        let api_store = InMemoryCredentialStore::new();
        let api_provider = provider(mode);
        if mode == "stored" {
            set_credential(&api_store, &api_provider, api_credential("stored"));
        }
        let api_dropped = Arc::new(AtomicBool::new(false));
        let api_auth = ProviderAuth::new(
            Some(Arc::new(PendingApiKey {
                dropped: Arc::clone(&api_dropped),
            })),
            None,
        )
        .expect("valid auth");
        let api_token = CancellationToken::new();
        let overrides = if mode == "explicit" {
            AuthResolutionOverrides {
                api_key: Some(SecretString::new("explicit")),
                ..AuthResolutionOverrides::default()
            }
        } else {
            AuthResolutionOverrides::default()
        };
        assert_cancellation_drops_pending(
            resolve_provider_auth(
                &api_provider,
                &api_auth,
                &api_store,
                &context,
                &clock,
                overrides,
                &api_token,
            ),
            &api_token,
            &api_dropped,
        );
    }

    let nested_dropped = Arc::new(AtomicBool::new(false));
    let nested_context = PendingContext {
        dropped: Arc::clone(&nested_dropped),
    };
    let nested_store = InMemoryCredentialStore::new();
    let nested_provider = provider("nested");
    let nested_auth =
        ProviderAuth::new(Some(Arc::new(NestedContextApi)), None).expect("valid auth");
    let nested_token = CancellationToken::new();
    assert_cancellation_drops_pending(
        resolve_provider_auth(
            &nested_provider,
            &nested_auth,
            &nested_store,
            &nested_context,
            &clock,
            AuthResolutionOverrides::default(),
            &nested_token,
        ),
        &nested_token,
        &nested_dropped,
    );

    let acquisition_dropped = Arc::new(AtomicBool::new(false));
    let acquire_store = PendingAcquireStore {
        current: VersionedCredential {
            revision: CredentialRevision::FIRST,
            credential: Credential::OAuth(oauth_credential("old", 0)),
        },
        dropped: Arc::clone(&acquisition_dropped),
    };
    let acquire_provider = provider("acquire");
    let acquire_auth = ProviderAuth::new(
        None,
        Some(Arc::new(CountingOAuth {
            refreshes: AtomicUsize::new(0),
        })),
    )
    .expect("valid auth");
    let acquire_token = CancellationToken::new();
    assert_cancellation_drops_pending(
        resolve_provider_auth(
            &acquire_provider,
            &acquire_auth,
            &acquire_store,
            &context,
            &clock,
            AuthResolutionOverrides::default(),
            &acquire_token,
        ),
        &acquire_token,
        &acquisition_dropped,
    );

    let dropped_outer = Arc::new(AtomicBool::new(false));
    let drop_store = PendingAcquireStore {
        current: VersionedCredential {
            revision: CredentialRevision::FIRST,
            credential: Credential::OAuth(oauth_credential("old", 0)),
        },
        dropped: Arc::clone(&dropped_outer),
    };
    let drop_token = CancellationToken::new();
    let mut dropped_resolution = resolve_provider_auth(
        &acquire_provider,
        &acquire_auth,
        &drop_store,
        &context,
        &clock,
        AuthResolutionOverrides::default(),
        &drop_token,
    );
    assert!(matches!(
        poll_noop(dropped_resolution.as_mut()),
        Poll::Pending
    ));
    drop(dropped_resolution);
    assert!(dropped_outer.load(Ordering::Acquire));

    for (name, phase, expires) in [
        ("refresh", PendingOAuthPhase::Refresh, 0),
        ("to-auth", PendingOAuthPhase::ToAuth, 1_000_000),
    ] {
        let store = InMemoryCredentialStore::new();
        let provider = provider(name);
        set_credential(
            &store,
            &provider,
            Credential::OAuth(oauth_credential("old", expires)),
        );
        let dropped = Arc::new(AtomicBool::new(false));
        let auth = ProviderAuth::new(
            None,
            Some(Arc::new(PendingOAuth {
                phase,
                dropped: Arc::clone(&dropped),
            })),
        )
        .expect("valid auth");
        let token = CancellationToken::new();
        assert_cancellation_drops_pending(
            resolve_provider_auth(
                &provider,
                &auth,
                &store,
                &context,
                &clock,
                AuthResolutionOverrides::default(),
                &token,
            ),
            &token,
            &dropped,
        );
        let active = CancellationToken::new();
        assert!(ready(store.begin_mutation(&provider, &active)).is_ok());
    }
}

#[test]
fn post_commit_to_auth_cancellation_drops_derivation_and_keeps_rotation() {
    let store = InMemoryCredentialStore::new();
    let provider = provider("post-commit");
    set_credential(
        &store,
        &provider,
        Credential::OAuth(oauth_credential("old", 0)),
    );
    let dropped = Arc::new(AtomicBool::new(false));
    let auth = ProviderAuth::new(
        None,
        Some(Arc::new(RefreshThenPendingToAuth {
            dropped: Arc::clone(&dropped),
        })),
    )
    .expect("valid auth");
    let token = CancellationToken::new();
    let mut resolution = resolve_provider_auth(
        &provider,
        &auth,
        &store,
        &EMPTY_CONTEXT,
        &NeverClock { now: 100 },
        AuthResolutionOverrides::default(),
        &token,
    );
    assert!(matches!(poll_noop(resolution.as_mut()), Poll::Pending));
    let active = CancellationToken::new();
    let stored = ready(store.read(&provider, &active))
        .expect("read succeeds")
        .expect("rotation committed before derivation");
    assert_eq!(stored.revision.get(), 2);
    let Credential::OAuth(stored) = stored.credential else {
        panic!("expected OAuth")
    };
    assert_eq!(stored.credentials.access.expose_secret(), "rotated");
    token.cancel();
    assert!(matches!(
        poll_noop(resolution.as_mut()),
        Poll::Ready(Err(AuthResolutionError::Cancelled))
    ));
    assert!(dropped.load(Ordering::Acquire));
    let stored = ready(store.read(&provider, &active))
        .expect("read succeeds")
        .expect("rotation remains");
    assert_eq!(stored.revision.get(), 2);
}

#[derive(Default)]
struct DeadlineState {
    wakers: Vec<Waker>,
}

struct DeadlineClock {
    now: AtomicU64,
    requested_ms: AtomicU64,
    state: Mutex<DeadlineState>,
}

impl DeadlineClock {
    fn new() -> Self {
        Self {
            now: AtomicU64::new(0),
            requested_ms: AtomicU64::new(0),
            state: Mutex::new(DeadlineState::default()),
        }
    }

    fn advance(&self, amount: u64) {
        self.now.fetch_add(amount, Ordering::AcqRel);
        let wakers = std::mem::take(
            &mut self
                .state
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .wakers,
        );
        for waker in wakers {
            waker.wake();
        }
    }
}

struct DeadlineSleep<'a> {
    clock: &'a DeadlineClock,
    deadline: u64,
    cancellation: &'a CancellationToken,
}

impl Future for DeadlineSleep<'_> {
    type Output = Result<(), Cancelled>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if self.cancellation.is_cancelled() {
            return Poll::Ready(Err(Cancelled));
        }
        if self.clock.now.load(Ordering::Acquire) >= self.deadline {
            return Poll::Ready(Ok(()));
        }
        self.clock
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .wakers
            .push(context.waker().clone());
        Poll::Pending
    }
}

impl Clock for DeadlineClock {
    fn now_unix_ms(&self) -> u64 {
        self.now.load(Ordering::Acquire)
    }

    fn sleep<'a>(
        &'a self,
        duration: Duration,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), Cancelled>> {
        let requested = u64::try_from(duration.as_millis()).expect("duration fits u64");
        self.requested_ms.store(requested, Ordering::Release);
        Box::pin(DeadlineSleep {
            clock: self,
            deadline: self.now.load(Ordering::Acquire).saturating_add(requested),
            cancellation,
        })
    }
}

struct ControlledOAuth {
    ready: AtomicBool,
    polls: AtomicUsize,
    dropped: AtomicBool,
    waker: Mutex<Option<Waker>>,
}

impl ControlledOAuth {
    fn new() -> Self {
        Self {
            ready: AtomicBool::new(false),
            polls: AtomicUsize::new(0),
            dropped: AtomicBool::new(false),
            waker: Mutex::new(None),
        }
    }

    fn make_ready(&self) {
        self.ready.store(true, Ordering::Release);
        let waker = self
            .waker
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .take();
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

struct ControlledRefresh<'a> {
    oauth: &'a ControlledOAuth,
}

impl Future for ControlledRefresh<'_> {
    type Output = Result<OAuthCredential, AuthProviderError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.oauth.polls.fetch_add(1, Ordering::AcqRel);
        if self.oauth.ready.load(Ordering::Acquire) {
            Poll::Ready(Ok(oauth_credential("ready", 1_000_000)))
        } else {
            let displaced = self
                .oauth
                .waker
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .replace(context.waker().clone());
            drop(displaced);
            Poll::Pending
        }
    }
}

impl Drop for ControlledRefresh<'_> {
    fn drop(&mut self) {
        self.oauth.dropped.store(true, Ordering::Release);
    }
}

impl OAuthAuth for ControlledOAuth {
    fn name(&self) -> &str {
        "controlled"
    }

    fn login<'a>(
        &'a self,
        _interaction: &'a dyn pi_ai_core::AuthInteraction,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<OAuthCredential, AuthProviderError>> {
        Box::pin(async { Err(AuthProviderError::new()) })
    }

    fn refresh<'a>(
        &'a self,
        _credential: &'a OAuthCredential,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<OAuthCredential, AuthProviderError>> {
        Box::pin(ControlledRefresh { oauth: self })
    }

    fn to_auth<'a>(
        &'a self,
        credential: &'a OAuthCredential,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<ModelAuth, AuthProviderError>> {
        let access = credential.credentials.access.clone();
        Box::pin(async move {
            Ok(ModelAuth {
                api_key: Some(access),
                headers: None,
                base_url: None,
            })
        })
    }
}

fn pending_controlled_resolution<'a>(
    store: &'a InMemoryCredentialStore,
    provider: &'a ProviderId,
    auth: &'a ProviderAuth,
    clock: &'a DeadlineClock,
    token: &'a CancellationToken,
) -> BoxFuture<'a, Result<Option<AuthResult>, AuthResolutionError>> {
    resolve_provider_auth(
        provider,
        auth,
        store,
        &EMPTY_CONTEXT,
        clock,
        AuthResolutionOverrides::default(),
        token,
    )
}

#[test]
fn oauth_three_way_race_prioritizes_caller_then_deadline_then_provider() {
    let provider = provider("deadline-provider");
    let store = InMemoryCredentialStore::new();
    set_credential(
        &store,
        &provider,
        Credential::OAuth(oauth_credential("old", 0)),
    );
    let oauth = Arc::new(ControlledOAuth::new());
    let auth = ProviderAuth::new(None, Some(oauth.clone())).expect("valid auth");
    let clock = DeadlineClock::new();
    let token = CancellationToken::new();
    let mut resolution = pending_controlled_resolution(&store, &provider, &auth, &clock, &token);
    assert!(matches!(poll_noop(resolution.as_mut()), Poll::Pending));
    assert_eq!(oauth.polls.load(Ordering::Acquire), 1);
    oauth.make_ready();
    clock.advance(15_000);
    assert!(matches!(
        poll_noop(resolution.as_mut()),
        Poll::Ready(Err(AuthResolutionError::Models(error)))
            if error.code() == ModelsErrorCode::OAuth
    ));
    assert_eq!(oauth.polls.load(Ordering::Acquire), 1);
    assert!(oauth.dropped.load(Ordering::Acquire));
    assert_eq!(
        ready(store.read(&provider, &CancellationToken::new()))
            .expect("read succeeds")
            .expect("old credential remains")
            .revision
            .get(),
        1
    );

    let second_provider = ProviderId::new("caller-deadline-provider").expect("valid provider");
    let store = InMemoryCredentialStore::new();
    set_credential(
        &store,
        &second_provider,
        Credential::OAuth(oauth_credential("old", 0)),
    );
    let oauth = Arc::new(ControlledOAuth::new());
    let auth = ProviderAuth::new(None, Some(oauth.clone())).expect("valid auth");
    let clock = DeadlineClock::new();
    let token = CancellationToken::new();
    let mut resolution =
        pending_controlled_resolution(&store, &second_provider, &auth, &clock, &token);
    assert!(matches!(poll_noop(resolution.as_mut()), Poll::Pending));
    oauth.make_ready();
    clock.advance(15_000);
    token.cancel();
    assert!(matches!(
        poll_noop(resolution.as_mut()),
        Poll::Ready(Err(AuthResolutionError::Cancelled))
    ));
    assert_eq!(oauth.polls.load(Ordering::Acquire), 1);
    assert!(oauth.dropped.load(Ordering::Acquire));
}

struct RecursiveOAuth {
    provider: ProviderId,
    store: Arc<InMemoryCredentialStore>,
    clock: Arc<DeadlineClock>,
    auth: OnceLock<ProviderAuth>,
    refreshes: AtomicUsize,
}

impl OAuthAuth for RecursiveOAuth {
    fn name(&self) -> &str {
        "recursive"
    }

    fn login<'a>(
        &'a self,
        _interaction: &'a dyn pi_ai_core::AuthInteraction,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<OAuthCredential, AuthProviderError>> {
        Box::pin(async { Err(AuthProviderError::new()) })
    }

    fn refresh<'a>(
        &'a self,
        _credential: &'a OAuthCredential,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<OAuthCredential, AuthProviderError>> {
        Box::pin(async move {
            self.refreshes.fetch_add(1, Ordering::AcqRel);
            let auth = self.auth.get().expect("auth initialized");
            let _ = resolve_provider_auth(
                &self.provider,
                auth,
                self.store.as_ref(),
                &EMPTY_CONTEXT,
                self.clock.as_ref(),
                AuthResolutionOverrides::default(),
                cancellation,
            )
            .await;
            Err(AuthProviderError::new())
        })
    }

    fn to_auth<'a>(
        &'a self,
        _credential: &'a OAuthCredential,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<ModelAuth, AuthProviderError>> {
        Box::pin(async { Ok(ModelAuth::default()) })
    }
}

#[test]
fn recursive_same_provider_refresh_times_out_and_releases_both_gate_claims() {
    let provider = provider("recursive");
    let store = Arc::new(InMemoryCredentialStore::new());
    set_credential(
        store.as_ref(),
        &provider,
        Credential::OAuth(oauth_credential("old", 0)),
    );
    let clock = Arc::new(DeadlineClock::new());
    let oauth = Arc::new(RecursiveOAuth {
        provider: provider.clone(),
        store: Arc::clone(&store),
        clock: Arc::clone(&clock),
        auth: OnceLock::new(),
        refreshes: AtomicUsize::new(0),
    });
    let auth = ProviderAuth::new(None, Some(oauth.clone())).expect("valid auth");
    assert!(oauth.auth.set(auth.clone()).is_ok());
    let token = CancellationToken::new();
    let mut resolution = resolve_provider_auth(
        &provider,
        &auth,
        store.as_ref(),
        &EMPTY_CONTEXT,
        clock.as_ref(),
        AuthResolutionOverrides::default(),
        &token,
    );
    assert!(matches!(poll_noop(resolution.as_mut()), Poll::Pending));
    assert_eq!(oauth.refreshes.load(Ordering::Acquire), 1);
    clock.advance(15_000);
    assert!(matches!(
        poll_noop(resolution.as_mut()),
        Poll::Ready(Err(AuthResolutionError::Models(error)))
            if error.code() == ModelsErrorCode::OAuth
    ));
    let active = CancellationToken::new();
    let lease = ready(store.begin_mutation(&provider, &active))
        .expect("outer and recursive acquisition claims released");
    drop(lease);
    assert_eq!(
        ready(store.read(&provider, &active))
            .expect("read succeeds")
            .expect("old credential remains")
            .revision
            .get(),
        1
    );
}

#[test]
fn oauth_deadline_wins_at_15000_and_drops_noncooperative_refresh() {
    let store = InMemoryCredentialStore::new();
    let provider = provider("p");
    set_credential(
        &store,
        &provider,
        Credential::OAuth(oauth_credential("old", 0)),
    );
    let dropped = Arc::new(AtomicBool::new(false));
    let auth = ProviderAuth::new(
        None,
        Some(Arc::new(PendingOAuth {
            phase: PendingOAuthPhase::Refresh,
            dropped: Arc::clone(&dropped),
        })),
    )
    .expect("valid auth");
    let context = EmptyContext;
    let clock = DeadlineClock::new();
    let token = CancellationToken::new();
    let mut resolution = resolve_provider_auth(
        &provider,
        &auth,
        &store,
        &context,
        &clock,
        AuthResolutionOverrides::default(),
        &token,
    );
    assert!(matches!(poll_noop(resolution.as_mut()), Poll::Pending));
    assert_eq!(clock.requested_ms.load(Ordering::Acquire), 15_000);
    clock.advance(14_999);
    assert!(matches!(poll_noop(resolution.as_mut()), Poll::Pending));
    assert!(!dropped.load(Ordering::Acquire));
    clock.advance(1);
    assert!(matches!(
        poll_noop(resolution.as_mut()),
        Poll::Ready(Err(AuthResolutionError::Models(error)))
            if error.code() == ModelsErrorCode::OAuth
    ));
    assert!(dropped.load(Ordering::Acquire));
    let active = CancellationToken::new();
    assert!(ready(store.begin_mutation(&provider, &active)).is_ok());
}

struct CancelOnSleepDropClock {
    dropped: Arc<AtomicBool>,
    caller: CancellationToken,
}

struct CancelOnSleepDrop<'a> {
    clock: &'a CancelOnSleepDropClock,
}

impl Future for CancelOnSleepDrop<'_> {
    type Output = Result<(), Cancelled>;

    fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Pending
    }
}

impl Drop for CancelOnSleepDrop<'_> {
    fn drop(&mut self) {
        self.clock.dropped.store(true, Ordering::Release);
        self.clock.caller.cancel();
    }
}

impl Clock for CancelOnSleepDropClock {
    fn now_unix_ms(&self) -> u64 {
        100
    }

    fn sleep<'a>(
        &'a self,
        _duration: Duration,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), Cancelled>> {
        Box::pin(CancelOnSleepDrop { clock: self })
    }
}

#[test]
fn cancellation_after_refresh_before_commit_preserves_old_credential() {
    let store = InMemoryCredentialStore::new();
    let provider = provider("p");
    set_credential(
        &store,
        &provider,
        Credential::OAuth(oauth_credential("old", 0)),
    );
    let token = CancellationToken::new();
    let oauth = Arc::new(CountingOAuth {
        refreshes: AtomicUsize::new(0),
    });
    let auth = ProviderAuth::new(None, Some(oauth.clone())).expect("valid auth");
    let deadline_dropped = Arc::new(AtomicBool::new(false));
    let clock = CancelOnSleepDropClock {
        dropped: Arc::clone(&deadline_dropped),
        caller: token.clone(),
    };
    let result = ready(resolve_provider_auth(
        &provider,
        &auth,
        &store,
        &EmptyContext,
        &clock,
        AuthResolutionOverrides::default(),
        &token,
    ));
    assert!(matches!(result, Err(AuthResolutionError::Cancelled)));
    assert_eq!(oauth.refreshes.load(Ordering::Acquire), 1);
    assert!(deadline_dropped.load(Ordering::Acquire));
    let active = CancellationToken::new();
    let stored = ready(store.read(&provider, &active))
        .expect("read succeeds")
        .expect("credential present");
    let Credential::OAuth(stored) = stored.credential else {
        panic!("expected OAuth")
    };
    assert_eq!(stored.credentials.access.expose_secret(), "old");
    assert_eq!(
        ready(store.read(&provider, &active))
            .expect("read succeeds")
            .expect("old credential remains")
            .revision
            .get(),
        1
    );
}

struct ShieldState {
    current: Mutex<VersionedCredential>,
    admitted: AtomicBool,
    finish: AtomicBool,
    waker: Mutex<Option<Waker>>,
}

struct ShieldStore {
    state: Arc<ShieldState>,
}

struct ShieldLease {
    state: Arc<ShieldState>,
    current: VersionedCredential,
    token: CancellationToken,
}

impl CredentialMutation for ShieldLease {
    fn current(&self) -> Option<&VersionedCredential> {
        Some(&self.current)
    }

    fn commit(
        self: Box<Self>,
        next: CredentialMutationValue,
    ) -> BoxFuture<'static, Result<Option<VersionedCredential>, CredentialStoreError>> {
        Box::pin(ShieldCommit {
            lease: Some(*self),
            next: Some(next),
            admitted: false,
        })
    }
}

struct ShieldCommit {
    lease: Option<ShieldLease>,
    next: Option<CredentialMutationValue>,
    admitted: bool,
}

impl Future for ShieldCommit {
    type Output = Result<Option<VersionedCredential>, CredentialStoreError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.admitted {
            let (cancelled, revision, state) = {
                let lease = self.lease.as_ref().expect("lease present");
                (
                    lease.token.is_cancelled(),
                    lease.current.revision,
                    Arc::clone(&lease.state),
                )
            };
            if cancelled {
                return Poll::Ready(Err(CredentialStoreError::new(
                    CredentialStoreErrorCode::Cancelled,
                )));
            }
            let CredentialMutationValue::Set(credential) = self.next.take().expect("value present")
            else {
                panic!("test expects Set")
            };
            let next = VersionedCredential {
                revision: revision.checked_next().expect("revision advances"),
                credential,
            };
            *state
                .current
                .lock()
                .unwrap_or_else(|poison| poison.into_inner()) = next;
            state.admitted.store(true, Ordering::Release);
            self.admitted = true;
        }
        let state = Arc::clone(&self.lease.as_ref().expect("lease present").state);
        if state.finish.load(Ordering::Acquire) {
            return Poll::Ready(Ok(Some(
                state
                    .current
                    .lock()
                    .unwrap_or_else(|poison| poison.into_inner())
                    .clone(),
            )));
        }
        let displaced = state
            .waker
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .replace(context.waker().clone());
        drop(displaced);
        Poll::Pending
    }
}

impl CredentialStore for ShieldStore {
    fn read<'a>(
        &'a self,
        _provider: &'a ProviderId,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<VersionedCredential>, CredentialStoreError>> {
        Box::pin(async move {
            Ok(Some(
                self.state
                    .current
                    .lock()
                    .unwrap_or_else(|poison| poison.into_inner())
                    .clone(),
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
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Box<dyn CredentialMutation + 'static>, CredentialStoreError>> {
        let current = self
            .state
            .current
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone();
        let lease = ShieldLease {
            state: Arc::clone(&self.state),
            current,
            token: cancellation.clone(),
        };
        Box::pin(async move { Ok(Box::new(lease) as Box<dyn CredentialMutation>) })
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

#[test]
fn admitted_commit_is_shielded_but_post_commit_derivation_observes_cancellation() {
    let state = Arc::new(ShieldState {
        current: Mutex::new(VersionedCredential {
            revision: CredentialRevision::FIRST,
            credential: Credential::OAuth(oauth_credential("old", 0)),
        }),
        admitted: AtomicBool::new(false),
        finish: AtomicBool::new(false),
        waker: Mutex::new(None),
    });
    let store = ShieldStore {
        state: Arc::clone(&state),
    };
    let oauth = Arc::new(CountingOAuth {
        refreshes: AtomicUsize::new(0),
    });
    let auth = ProviderAuth::new(None, Some(oauth)).expect("valid auth");
    let provider = provider("p");
    let token = CancellationToken::new();
    let mut resolution = resolve_provider_auth(
        &provider,
        &auth,
        &store,
        &EmptyContext,
        &NeverClock { now: 100 },
        AuthResolutionOverrides::default(),
        &token,
    );
    assert!(matches!(poll_noop(resolution.as_mut()), Poll::Pending));
    assert!(state.admitted.load(Ordering::Acquire));
    token.cancel();
    assert!(matches!(poll_noop(resolution.as_mut()), Poll::Pending));
    state.finish.store(true, Ordering::Release);
    if let Some(waker) = state
        .waker
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .take()
    {
        waker.wake();
    }
    assert!(matches!(
        poll_noop(resolution.as_mut()),
        Poll::Ready(Err(AuthResolutionError::Cancelled))
    ));
    let current = state
        .current
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .clone();
    let Credential::OAuth(current) = current.credential else {
        panic!("expected OAuth")
    };
    assert_eq!(current.credentials.access.expose_secret(), "new");
}

fn raw_entry(id: &str) -> RawModelsStoreEntry {
    serde_json::from_value(serde_json::json!({
        "models": [{
            "id": id,
            "name": id,
            "api": "test-api",
            "provider": "p",
            "baseUrl": "https://example.test/v1",
            "reasoning": false,
            "input": ["text"],
            "cost": {"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0},
            "contextWindow": 1000,
            "maxTokens": 100
        }]
    }))
    .expect("valid raw entry")
}

fn model_entry(id: &str) -> ModelsStoreEntry {
    raw_entry(id)
        .into_store(&ModelHeaderPolicy::default())
        .expect("valid entry")
}

fn complete_raw_entry(generation: &str, number: u64) -> RawModelsStoreEntry {
    serde_json::from_value(serde_json::json!({
        "models": [
            {
                "id": format!("{generation}-one"),
                "name": format!("{generation} name one"),
                "api": "test-api",
                "provider": "p",
                "baseUrl": format!("https://{generation}.example.test/v1"),
                "reasoning": false,
                "input": ["text"],
                "cost": {"input": number, "output": number + 1, "cacheRead": number + 2, "cacheWrite": number + 3},
                "contextWindow": number + 1000,
                "maxTokens": number + 100
            },
            {
                "id": format!("{generation}-two"),
                "name": format!("{generation} name two"),
                "api": "test-api",
                "provider": "p",
                "baseUrl": format!("https://{generation}.example.test/v2"),
                "reasoning": true,
                "input": ["text", "image"],
                "cost": {"input": number + 4, "output": number + 5, "cacheRead": number + 6, "cacheWrite": number + 7},
                "contextWindow": number + 2000,
                "maxTokens": number + 200
            }
        ],
        "lastModified": number + 10,
        "checkedAt": number + 20,
        "etag": format!("\"{generation}-{number}\"")
    }))
    .expect("valid complete raw entry")
}

struct ManualModelState {
    current: Mutex<Option<ModelsStoreEntry>>,
    pending: Mutex<Option<ModelsStoreEntry>>,
    admitted: AtomicBool,
    fail: AtomicBool,
    completion: Mutex<Option<ManualCompletion>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ManualCompletion {
    Success,
    Failed,
}

struct ManualModelsStore {
    state: Arc<ManualModelState>,
}

struct ManualWrite<'a> {
    state: Arc<ManualModelState>,
    entry: Option<ModelsStoreEntry>,
    cancellation: &'a CancellationToken,
    admitted: bool,
}

impl Future for ManualWrite<'_> {
    type Output = Result<(), ModelsStoreError>;

    fn poll(mut self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.admitted {
            if self.cancellation.is_cancelled() {
                return Poll::Ready(Err(ModelsStoreError::new(ModelsStoreErrorCode::Cancelled)));
            }
            if self.state.fail.load(Ordering::Acquire) {
                return Poll::Ready(Err(ModelsStoreError::new(ModelsStoreErrorCode::Failed)));
            }
            let entry = self.entry.take().expect("entry present");
            *self
                .state
                .pending
                .lock()
                .unwrap_or_else(|poison| poison.into_inner()) = Some(entry);
            self.state.admitted.store(true, Ordering::Release);
            self.admitted = true;
        }
        Poll::Pending
    }
}

impl ManualModelsStore {
    fn finish_success(&self) {
        let entry = self
            .state
            .pending
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .take();
        if let Some(entry) = entry {
            *self
                .state
                .current
                .lock()
                .unwrap_or_else(|poison| poison.into_inner()) = Some(entry);
        }
        *self
            .state
            .completion
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) = Some(ManualCompletion::Success);
    }

    fn finish_failed(&self) {
        let discarded = self
            .state
            .pending
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .take();
        drop(discarded);
        *self
            .state
            .completion
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) = Some(ManualCompletion::Failed);
    }
}

impl ModelsStore for ManualModelsStore {
    fn read<'a>(
        &'a self,
        _provider: &'a ProviderId,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<RawModelsStoreEntry>, ModelsStoreError>> {
        let current = self
            .state
            .current
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone();
        Box::pin(async move {
            Ok(current.map(|entry| RawModelsStoreEntry {
                models: entry
                    .models
                    .iter()
                    .map(pi_ai_core::RawModel::from)
                    .collect(),
                last_modified: entry.last_modified,
                checked_at: entry.checked_at,
                etag: entry.etag,
            }))
        })
    }

    fn write<'a>(
        &'a self,
        _provider: &'a ProviderId,
        entry: ModelsStoreEntry,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), ModelsStoreError>> {
        Box::pin(ManualWrite {
            state: Arc::clone(&self.state),
            entry: Some(entry),
            cancellation,
            admitted: false,
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

#[test]
fn dropped_admitted_model_write_finishes_to_one_complete_state() {
    let store = ManualModelsStore {
        state: Arc::new(ManualModelState {
            current: Mutex::new(Some(model_entry("old"))),
            pending: Mutex::new(None),
            admitted: AtomicBool::new(false),
            fail: AtomicBool::new(false),
            completion: Mutex::new(None),
        }),
    };
    let provider = provider("p");
    let token = CancellationToken::new();
    let mut write = store.write(&provider, model_entry("new"), &token);
    assert!(matches!(poll_noop(write.as_mut()), Poll::Pending));
    assert!(store.state.admitted.load(Ordering::Acquire));
    token.cancel();
    assert!(matches!(poll_noop(write.as_mut()), Poll::Pending));
    drop(write);
    assert_eq!(
        ready(store.read(&provider, &CancellationToken::new()))
            .expect("read succeeds")
            .expect("old entry")
            .models[0]
            .id
            .as_str(),
        "old"
    );
    store.finish_success();
    assert_eq!(
        *store
            .state
            .completion
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()),
        Some(ManualCompletion::Success)
    );
    assert_eq!(
        ready(store.read(&provider, &CancellationToken::new()))
            .expect("read succeeds")
            .expect("new entry")
            .models[0]
            .id
            .as_str(),
        "new"
    );

    let failed_store = ManualModelsStore {
        state: Arc::new(ManualModelState {
            current: Mutex::new(Some(model_entry("old"))),
            pending: Mutex::new(None),
            admitted: AtomicBool::new(false),
            fail: AtomicBool::new(false),
            completion: Mutex::new(None),
        }),
    };
    let failed_token = CancellationToken::new();
    let mut failed_write = failed_store.write(&provider, model_entry("new"), &failed_token);
    assert!(matches!(poll_noop(failed_write.as_mut()), Poll::Pending));
    drop(failed_write);
    failed_store.finish_failed();
    assert_eq!(
        *failed_store
            .state
            .completion
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()),
        Some(ManualCompletion::Failed)
    );
    assert_eq!(
        ready(failed_store.read(&provider, &failed_token))
            .expect("read succeeds")
            .expect("old entry remains")
            .models[0]
            .id
            .as_str(),
        "old"
    );
}

#[test]
fn normal_model_store_failure_proves_old_complete_value() {
    let store = ManualModelsStore {
        state: Arc::new(ManualModelState {
            current: Mutex::new(Some(model_entry("old"))),
            pending: Mutex::new(None),
            admitted: AtomicBool::new(false),
            fail: AtomicBool::new(true),
            completion: Mutex::new(None),
        }),
    };
    let provider = provider("p");
    let token = CancellationToken::new();
    assert_eq!(
        ready(store.write(&provider, model_entry("new"), &token))
            .expect_err("backend rejects without mutation")
            .code(),
        ModelsStoreErrorCode::Failed
    );
    assert_eq!(
        ready(store.read(&provider, &token))
            .expect("read succeeds")
            .expect("old entry remains")
            .models[0]
            .id
            .as_str(),
        "old"
    );
}

#[test]
fn in_memory_model_store_reads_only_atomic_old_new_or_deleted_multifield_snapshots() {
    const READERS: usize = 2;
    const WRITES: usize = 200;
    let store = Arc::new(InMemoryModelsStore::new());
    let provider = provider("p");
    let old_raw = complete_raw_entry("old", 10);
    let new_raw = complete_raw_entry("new", 100);
    let old_entry = old_raw
        .clone()
        .into_store(&ModelHeaderPolicy::default())
        .expect("old validates");
    let new_entry = new_raw
        .clone()
        .into_store(&ModelHeaderPolicy::default())
        .expect("new validates");
    ready(store.write(&provider, old_entry.clone(), &CancellationToken::new()))
        .expect("initial write succeeds");
    let barrier = Arc::new(Barrier::new(READERS + 1));
    let readers = (0..READERS)
        .map(|_| {
            let store = Arc::clone(&store);
            let provider = provider.clone();
            let barrier = Arc::clone(&barrier);
            let old_raw = old_raw.clone();
            let new_raw = new_raw.clone();
            thread::spawn(move || {
                let token = CancellationToken::new();
                barrier.wait();
                for _ in 0..(WRITES * 3) {
                    let snapshot = ready(store.read(&provider, &token)).expect("read succeeds");
                    assert!(
                        snapshot.is_none()
                            || snapshot.as_ref() == Some(&old_raw)
                            || snapshot.as_ref() == Some(&new_raw),
                        "read observed a torn multi-field catalog"
                    );
                }
            })
        })
        .collect::<Vec<_>>();
    barrier.wait();
    let token = CancellationToken::new();
    for _ in 0..WRITES {
        ready(store.write(&provider, new_entry.clone(), &token)).expect("new write succeeds");
        ready(store.delete(&provider, &token)).expect("delete succeeds");
        ready(store.write(&provider, old_entry.clone(), &token)).expect("old write succeeds");
    }
    for reader in readers {
        reader.join().expect("reader thread");
    }
    assert_eq!(
        ready(store.read(&provider, &token)).expect("final read succeeds"),
        Some(old_raw)
    );
}
