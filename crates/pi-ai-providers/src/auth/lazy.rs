use pi_ai_core::{
    AuthInteraction, AuthProviderError, BoxFuture, CancellationToken, ModelAuth, OAuthAuth,
    OAuthCredential, TaskSpawner,
};
use std::{
    collections::BTreeMap,
    error::Error,
    fmt,
    future::{Future, poll_fn},
    sync::{Arc, Mutex},
    task::{Poll, Waker},
};

pub trait OAuthLoader: Send + Sync {
    fn load(self: Arc<Self>) -> BoxFuture<'static, Result<Arc<dyn OAuthAuth>, AuthProviderError>>;
}
#[derive(Clone, Debug)]
pub struct LazyOAuthMetadata {
    pub name: String,
    pub is_subscription: bool,
    pub login_label: Option<String>,
}
type Loaded = Result<Arc<dyn OAuthAuth>, Arc<AuthProviderError>>;
#[derive(Default)]
struct State {
    started: bool,
    result: Option<Loaded>,
    next: u128,
    waiters: BTreeMap<u128, Waker>,
}
struct Shared(Mutex<State>);
impl Shared {
    fn finish(&self, result: Loaded) {
        let waiters = {
            let mut state = self.0.lock().unwrap_or_else(|p| p.into_inner());
            if state.result.is_some() {
                return;
            }
            state.result = Some(result);
            std::mem::take(&mut state.waiters)
        };
        for (_, waker) in waiters {
            waker.wake();
        }
    }
}
struct CompletionGuard(Arc<Shared>);
impl Drop for CompletionGuard {
    fn drop(&mut self) {
        self.0.finish(Err(Arc::new(AuthProviderError::new())));
    }
}
struct Waiter {
    shared: Arc<Shared>,
    id: Option<u128>,
}
impl Drop for Waiter {
    fn drop(&mut self) {
        let waker = self.id.and_then(|id| {
            self.shared
                .0
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .waiters
                .remove(&id)
        });
        drop(waker);
    }
}
#[derive(Clone)]
struct SharedLoadError(Arc<AuthProviderError>);
impl fmt::Debug for SharedLoadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("OAuth load failed")
    }
}
impl fmt::Display for SharedLoadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("OAuth load failed")
    }
}
impl Error for SharedLoadError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.0.as_ref())
    }
}
/// One load across all login/refresh/derivation calls, including failed loads.
/// Load lifetime is independent of an individual caller's cancellation.
pub struct LazyOAuth {
    metadata: LazyOAuthMetadata,
    loader: Arc<dyn OAuthLoader>,
    spawner: Arc<dyn TaskSpawner>,
    shared: Arc<Shared>,
}
impl LazyOAuth {
    pub fn new(
        metadata: LazyOAuthMetadata,
        loader: Arc<dyn OAuthLoader>,
        spawner: Arc<dyn TaskSpawner>,
    ) -> Self {
        Self {
            metadata,
            loader,
            spawner,
            shared: Arc::new(Shared(Mutex::new(State::default()))),
        }
    }
    async fn loaded(&self, c: &CancellationToken) -> Result<Arc<dyn OAuthAuth>, AuthProviderError> {
        if c.is_cancelled() {
            return Err(AuthProviderError::new());
        }
        let start = {
            let mut s = self.shared.0.lock().unwrap_or_else(|p| p.into_inner());
            let start = !s.started;
            s.started = true;
            start
        };
        if start {
            let guard = CompletionGuard(self.shared.clone());
            let loader = self.loader.clone();
            let task = Box::pin(async move {
                let result = loader.load().await.map_err(Arc::new);
                guard.0.finish(result);
            });
            // Rejection/drop settles through the completion guard, including a spawner
            // that accepts but later discards its task.
            if self.spawner.spawn(task).is_err() {
                self.shared.finish(Err(Arc::new(AuthProviderError::new())));
            }
        }
        let mut waiter = Waiter {
            shared: self.shared.clone(),
            id: None,
        };
        let mut cancelled = Box::pin(c.cancelled());
        let result = poll_fn(|cx| {
            if cancelled.as_mut().poll(cx).is_ready() {
                return Poll::Ready(Err(Arc::new(AuthProviderError::new())));
            }
            let replacement = cx.waker().clone();
            let mut state = waiter.shared.0.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(result) = &state.result {
                return Poll::Ready(result.clone());
            }
            let id = match waiter.id {
                Some(id) => id,
                None => {
                    let Some(next) = state.next.checked_add(1) else {
                        return Poll::Ready(Err(Arc::new(AuthProviderError::new())));
                    };
                    state.next = next;
                    waiter.id = Some(next);
                    next
                }
            };
            let previous = state.waiters.insert(id, replacement);
            drop(state);
            drop(previous);
            Poll::Pending
        })
        .await;
        result.map_err(|error| AuthProviderError::with_source(SharedLoadError(error)))
    }
}
impl fmt::Debug for LazyOAuth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("LazyOAuth([REDACTED])")
    }
}
impl OAuthAuth for LazyOAuth {
    fn name(&self) -> &str {
        &self.metadata.name
    }
    fn is_subscription(&self) -> bool {
        self.metadata.is_subscription
    }
    fn login_label(&self) -> Option<&str> {
        self.metadata.login_label.as_deref()
    }
    fn login<'a>(
        &'a self,
        i: &'a dyn AuthInteraction,
        c: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<OAuthCredential, AuthProviderError>> {
        Box::pin(async move { self.loaded(c).await?.login(i, c).await })
    }
    fn refresh<'a>(
        &'a self,
        v: &'a OAuthCredential,
        c: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<OAuthCredential, AuthProviderError>> {
        Box::pin(async move { self.loaded(c).await?.refresh(v, c).await })
    }
    fn to_auth<'a>(
        &'a self,
        v: &'a OAuthCredential,
        c: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<ModelAuth, AuthProviderError>> {
        Box::pin(async move { self.loaded(c).await?.to_auth(v, c).await })
    }
}
