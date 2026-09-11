//! Private owned completion: caller drops unregister waiters, not admitted work.
use pi_ai_core::{BoxFuture, ModelsError, ModelsErrorCode};
use std::{
    collections::BTreeMap,
    error::Error,
    fmt,
    future::poll_fn,
    sync::{Arc, Mutex},
    task::{Poll, Waker},
};

pub(super) type SharedResult<T> = Result<T, Arc<ModelsError>>;
struct State<T> {
    result: Option<SharedResult<T>>,
    next: u128,
    waiters: BTreeMap<u128, Waker>,
}
pub(super) struct Completion<T>(Mutex<State<T>>);
impl<T: Clone + Send + 'static> Completion<T> {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self(Mutex::new(State {
            result: None,
            next: 0,
            waiters: BTreeMap::new(),
        })))
    }
    pub(super) fn finish(&self, result: SharedResult<T>) {
        let waiters = {
            let mut state = self.0.lock().unwrap_or_else(|p| p.into_inner());
            if state.result.is_some() {
                drop(state);
                drop(result);
                return;
            }
            state.result = Some(result);
            std::mem::take(&mut state.waiters)
        };
        for (_, waker) in waiters {
            waker.wake();
        }
    }
    pub(super) fn wait(self: &Arc<Self>) -> BoxFuture<'static, Result<T, ModelsError>> {
        let mut waiter = Waiter {
            shared: self.clone(),
            id: None,
        };
        Box::pin(async move {
            poll_fn(|cx| {
                let replacement = cx.waker().clone();
                let mut state = waiter.shared.0.lock().unwrap_or_else(|p| p.into_inner());
                if let Some(result) = &state.result {
                    let result = result.clone();
                    drop(state);
                    drop(replacement);
                    return Poll::Ready(result.map_err(clone_error));
                }
                let id = match waiter.id {
                    Some(id) => id,
                    None => {
                        let Some(next) = state.next.checked_add(1) else {
                            drop(state);
                            drop(replacement);
                            return Poll::Ready(Err(ModelsError::new(
                                ModelsErrorCode::ModelSource,
                            )));
                        };
                        state.next = next;
                        waiter.id = Some(next);
                        next
                    }
                };
                let old = state.waiters.insert(id, replacement);
                drop(state);
                drop(old);
                Poll::Pending
            })
            .await
        })
    }
}
struct Waiter<T> {
    shared: Arc<Completion<T>>,
    id: Option<u128>,
}
impl<T> Drop for Waiter<T> {
    fn drop(&mut self) {
        let old = self.id.and_then(|id| {
            self.shared
                .0
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .waiters
                .remove(&id)
        });
        drop(old);
    }
}
#[derive(Clone)]
struct SharedError(Arc<ModelsError>);
impl fmt::Debug for SharedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ImagesSharedError([REDACTED])")
    }
}
impl fmt::Display for SharedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("image operation failed")
    }
}
impl Error for SharedError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.0.as_ref())
    }
}
fn clone_error(error: Arc<ModelsError>) -> ModelsError {
    ModelsError::with_source(error.code(), SharedError(error))
}
