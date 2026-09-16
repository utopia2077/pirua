//! Result delivery independent of the public future's lifetime.
use pi_agent_schema::{BoxFuture, HarnessRuntimeError};
use pi_ai_core::{CancellationToken, TaskSpawner};
use std::sync::{Arc, Mutex};
struct Complete<T> {
    value: Mutex<Option<T>>,
    done: CancellationToken,
}
struct Finish<T>(Arc<Complete<T>>);
impl<T> Drop for Finish<T> {
    fn drop(&mut self) {
        self.0.done.cancel();
    }
}
/// Transfer work to the real host on first poll. Dropping the waiter cannot drop
/// an admitted task; rejected or discarded host tasks resolve as a runtime fault.
pub fn retained<T: Send + 'static>(
    host: Arc<dyn TaskSpawner>,
    work: BoxFuture<'static, T>,
) -> BoxFuture<'static, Result<T, HarnessRuntimeError>> {
    Box::pin(async move {
        let complete = Arc::new(Complete {
            value: Mutex::new(None),
            done: CancellationToken::new(),
        });
        let finish = Finish(complete.clone());
        host.spawn(Box::pin(async move {
            let result = work.await;
            *finish.0.value.lock().unwrap_or_else(|p| p.into_inner()) = Some(result);
            drop(finish);
        }))
        .map_err(|_| crate::restore::fault("runtime task admission failed"))?;
        complete.done.cancelled().await;
        let result = complete
            .value
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take();
        result.ok_or_else(|| crate::restore::fault("runtime host dropped an admitted task"))
    })
}

/// Isolate trusted computation failures at the actual retained future boundary.
/// Both invocation/poll panics and future-destructor panics become bounded faults.
pub(crate) async fn catch_task<T>(
    mut work: BoxFuture<'static, T>,
) -> Result<T, HarnessRuntimeError> {
    let result = std::future::poll_fn(|cx| {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| work.as_mut().poll(cx))) {
            Ok(poll) => poll.map(Ok),
            Err(_) => {
                std::task::Poll::Ready(Err(crate::restore::fault("trusted runtime task panicked")))
            }
        }
    })
    .await;
    if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(work))).is_err() {
        return Err(crate::restore::fault(
            "trusted runtime task destructor panicked",
        ));
    }
    result
}
