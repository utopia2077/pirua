//! Retained host tasks with independently observable completion and retirement.
use pi_agent_schema::BoxFuture;
use pi_ai_core::{CancellationToken, SpawnError, TaskSpawner};
use std::{
    collections::BTreeMap,
    future::poll_fn,
    sync::{Arc, Mutex},
    task::Poll,
};

/// Supervision wraps the actual injected executor; it never substitutes metadata
/// or an unpolled future for host task ownership.
#[derive(Clone)]
pub struct Supervisor {
    inner: Arc<Inner>,
}
struct Inner {
    host: Arc<dyn TaskSpawner>,
    state: Mutex<State>,
}
#[derive(Default)]
struct State {
    next: u64,
    retired: bool,
    tasks: BTreeMap<u64, CancellationToken>,
}
/// An actual completion ticket, signalled on normal completion, drop or retirement.
#[derive(Clone)]
pub struct TaskTicket {
    done: CancellationToken,
}
impl TaskTicket {
    /// Resolves after the wrapper has released its future.
    pub async fn completed(&self) {
        self.done.cancelled().await;
    }
}
struct Registration {
    owner: Arc<Inner>,
    id: u64,
    done: CancellationToken,
}
impl Drop for Registration {
    fn drop(&mut self) {
        let removed = self
            .owner
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .tasks
            .remove(&self.id);
        drop(removed);
        self.done.cancel();
    }
}
impl Supervisor {
    /// Retains tasks using the caller's actual executor.
    pub fn new(host: Arc<dyn TaskSpawner>) -> Self {
        Self {
            inner: Arc::new(Inner {
                host,
                state: Mutex::new(State::default()),
            }),
        }
    }
    /// Register before spawning, including for hosts that poll inline. The future
    /// is dropped outside bookkeeping locks on spawn rejection or retirement.
    pub fn submit(&self, task: BoxFuture<'static, ()>) -> Result<TaskTicket, SpawnError> {
        let stop = CancellationToken::new();
        let done = CancellationToken::new();
        let id = {
            let mut state = self.inner.state.lock().unwrap_or_else(|p| p.into_inner());
            if state.retired {
                return Err(SpawnError::new());
            }
            let id = state.next;
            state.next = state.next.checked_add(1).ok_or_else(SpawnError::new)?;
            state.tasks.insert(id, stop.clone());
            id
        };
        let registration = Registration {
            owner: self.inner.clone(),
            id,
            done: done.clone(),
        };
        self.inner.host.spawn(Box::pin(async move {
            let mut task = task;
            let mut cancelled = Box::pin(stop.cancelled());
            poll_fn(|cx| {
                if cancelled.as_mut().poll(cx).is_ready() {
                    return Poll::Ready(());
                }
                task.as_mut().poll(cx)
            })
            .await;
            drop(task);
            drop(registration);
        }))?;
        Ok(TaskTicket { done })
    }
    /// Reject new work and signal every current task without awaiting an external
    /// future that may ignore cancellation. No wake occurs under the state lock.
    pub fn retire(&self) {
        let tasks = {
            let mut state = self.inner.state.lock().unwrap_or_else(|p| p.into_inner());
            state.retired = true;
            std::mem::take(&mut state.tasks)
        };
        for (_, signal) in tasks {
            signal.cancel();
        }
    }
}
impl TaskSpawner for Supervisor {
    fn spawn(&self, task: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
        self.submit(task).map(|_| ())
    }
}
