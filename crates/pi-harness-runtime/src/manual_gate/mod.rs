//! Manual permission parking; no executable action or parent-completion lock is held.

use crate::scheduler::Admission;
use pi_agent_schema::{ActionInfo, DriveMode, HarnessRuntimeError};
use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Waker},
};

/// A fixed drive mode and FIFO queue of detached action descriptions.
#[derive(Clone)]
pub struct ManualGate {
    admission: Admission,
    mode: DriveMode,
    queue: Arc<Mutex<VecDeque<Waiter>>>,
}

struct Waiter {
    released: Arc<AtomicBool>,
    action: ActionInfo,
    waker: Option<Waker>,
}

impl ManualGate {
    /// Shares close/fault admission with the owner's mutation lines.
    pub fn new(admission: Admission, mode: DriveMode) -> Self {
        Self {
            admission,
            mode,
            queue: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    /// Creates an inert future. Only its first poll may publish the description.
    pub fn park(&self, action: ActionInfo) -> Park {
        Park {
            gate: self.clone(),
            action: Some(action),
            released: Arc::new(AtomicBool::new(false)),
            signal: Box::pin(self.admission.seal_signal()),
            done: false,
        }
    }

    /// Observes a detached oldest action without releasing it.
    pub fn peek(&self) -> Option<ActionInfo> {
        self.admission
            .while_open(|| {
                self.queue
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .front()
                    .map(|w| w.action.clone())
            })
            .ok()
            .flatten()
    }

    /// Releases one permission without waiting for its action or nested work.
    pub fn release_next(&self) -> Option<ActionInfo> {
        let waiter = self
            .admission
            .while_open(|| {
                let waiter = self
                    .queue
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .pop_front();
                if let Some(waiter) = &waiter {
                    waiter.released.store(true, Ordering::Release);
                }
                waiter
            })
            .ok()
            .flatten();
        waiter.map(|waiter| {
            if let Some(waker) = waiter.waker {
                waker.wake();
            }
            waiter.action
        })
    }
}

/// One parked permission; dropping it removes its action and registered wakers.
pub struct Park {
    gate: ManualGate,
    action: Option<ActionInfo>,
    released: Arc<AtomicBool>,
    signal: Pin<Box<dyn Future<Output = ()> + Send>>,
    done: bool,
}

impl Park {
    fn remove(&self) {
        let removed = {
            let mut queue = self.gate.queue.lock().unwrap_or_else(|p| p.into_inner());
            queue
                .iter()
                .position(|w| Arc::ptr_eq(&w.released, &self.released))
                .and_then(|index| queue.remove(index))
        };
        drop(removed);
    }
}

impl Future for Park {
    type Output = Result<Permission, HarnessRuntimeError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        assert!(!self.done, "park polled after completion");
        let _ = self.signal.as_mut().poll(cx);
        let mut candidate = Some(cx.waker().clone());
        let mut action = self.action.take();
        let result = self.gate.admission.while_open(|| {
            if self.gate.mode == DriveMode::Automatic || self.released.load(Ordering::Acquire) {
                return true;
            }
            let mut queue = self.gate.queue.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(waiter) = queue
                .iter_mut()
                .find(|w| Arc::ptr_eq(&w.released, &self.released))
            {
                std::mem::swap(&mut waiter.waker, &mut candidate);
            } else {
                queue.push_back(Waiter {
                    released: self.released.clone(),
                    action: action.take().expect("first parked poll owns action"),
                    waker: candidate.take(),
                });
            }
            false
        });
        drop(candidate);
        match result {
            Ok(false) => Poll::Pending,
            Ok(true) => {
                self.done = true;
                Poll::Ready(Ok(Permission { _private: () }))
            }
            Err(error) => {
                self.done = true;
                self.remove();
                Poll::Ready(Err(error))
            }
        }
    }
}

impl Drop for Park {
    fn drop(&mut self) {
        if !self.done {
            self.remove();
        }
    }
}

/// A nonduplicable gate result, not storage admission or an operation-abort decision.
#[must_use = "permission authorizes only the next boundary, subject to actual effect admission"]
pub struct Permission {
    _private: (),
}
