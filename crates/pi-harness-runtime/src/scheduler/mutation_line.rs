//! FIFO ownership; a queued head retains its place until it consumes or drops it.

use super::Admission;
use pi_agent_schema::HarnessRuntimeError;
use std::{
    collections::VecDeque,
    future::Future,
    panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};

/// One process-local mutation order, sharing the owner's global admission seal.
#[derive(Clone)]
pub struct MutationLine {
    admission: Admission,
    state: Arc<Mutex<State>>,
}

#[derive(Default)]
struct State {
    held: bool,
    queue: VecDeque<Waiter>,
}

struct Waiter {
    key: Arc<()>,
    waker: Option<Waker>,
}

impl State {
    fn notify_head(&mut self) -> Option<Waker> {
        if self.held {
            None
        } else {
            self.queue.front_mut().and_then(|w| w.waker.take())
        }
    }
}

impl MutationLine {
    /// Creates an independent line under the given admission owner.
    pub fn new(admission: Admission) -> Self {
        Self {
            admission,
            state: Arc::new(Mutex::new(State::default())),
        }
    }

    /// Construction reserves no position. First queued poll establishes FIFO order.
    pub fn acquire(&self) -> Acquire {
        Acquire {
            line: self.clone(),
            key: Arc::new(()),
            signal: Box::pin(self.admission.seal_signal()),
            done: false,
        }
    }
}

/// Cancellation-safe acquisition; dropping a notified head passes its place onward.
pub struct Acquire {
    line: MutationLine,
    key: Arc<()>,
    signal: Pin<Box<dyn Future<Output = ()> + Send>>,
    done: bool,
}

impl Acquire {
    fn remove(&self) {
        let mut state = self.line.state.lock().unwrap_or_else(|p| p.into_inner());
        let removed = state
            .queue
            .iter()
            .position(|w| Arc::ptr_eq(&w.key, &self.key))
            .and_then(|index| state.queue.remove(index));
        let wake = state.notify_head();
        drop(state);
        drop(removed);
        if let Some(wake) = wake {
            wake.wake();
        }
    }
}

impl Future for Acquire {
    type Output = Result<MutationPermit, HarnessRuntimeError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        assert!(!self.done, "acquisition polled after completion");
        // Register before checking the seal. A seal in either window is observed
        // by the locked transition or by this signal; neither can lose the wake.
        let _ = self.signal.as_mut().poll(cx);
        let mut candidate = Some(cx.waker().clone());
        let mut displaced = None;
        let result = self.line.admission.while_open(|| {
            let mut state = self.line.state.lock().unwrap_or_else(|p| p.into_inner());
            let index = state
                .queue
                .iter()
                .position(|w| Arc::ptr_eq(&w.key, &self.key));
            if !state.held && (state.queue.is_empty() || index == Some(0)) {
                displaced = state.queue.pop_front();
                state.held = true;
                true
            } else {
                if let Some(index) = index {
                    std::mem::swap(&mut state.queue[index].waker, &mut candidate);
                } else {
                    state.queue.push_back(Waiter {
                        key: self.key.clone(),
                        waker: candidate.take(),
                    });
                }
                false
            }
        });
        drop(displaced);
        drop(candidate);
        match result {
            Ok(false) => Poll::Pending,
            Ok(true) => {
                self.done = true;
                Poll::Ready(Ok(MutationPermit {
                    line: self.line.clone(),
                    released: false,
                }))
            }
            Err(error) => {
                self.done = true;
                self.remove();
                Poll::Ready(Err(error))
            }
        }
    }
}

impl Drop for Acquire {
    fn drop(&mut self) {
        if !self.done {
            self.remove();
        }
    }
}

/// Exclusive line ownership. This is deliberately not commit admission.
#[must_use = "dropping the permit releases the mutation line"]
pub struct MutationPermit {
    line: MutationLine,
    released: bool,
}

impl MutationPermit {
    /// Releases both lines before invoking either queued head's waker.
    ///
    /// A synchronous waker may reenter either line, so neither notification
    /// can run while the other permit still holds its line. Both notifications
    /// are attempted even if one panics. A panic then propagates; cleanup of
    /// another panic payload can itself panic.
    pub fn release_pair(mut first: Self, mut second: Self) {
        let first_wake = first.release_held();
        let second_wake = second.release_held();
        let first_panic =
            first_wake.and_then(|wake| catch_unwind(AssertUnwindSafe(|| wake.wake())).err());
        let second_panic =
            second_wake.and_then(|wake| catch_unwind(AssertUnwindSafe(|| wake.wake())).err());
        if let Some(panic) = first_panic.or(second_panic) {
            resume_unwind(panic);
        }
    }

    fn release_held(&mut self) -> Option<Waker> {
        debug_assert!(!self.released);
        let mut state = self.line.state.lock().unwrap_or_else(|p| p.into_inner());
        state.held = false;
        let wake = state.notify_head();
        drop(state);
        self.released = true;
        wake
    }
}

impl Drop for MutationPermit {
    fn drop(&mut self) {
        if self.released {
            return;
        }
        let wake = self.release_held();
        if let Some(wake) = wake {
            wake.wake();
        }
    }
}
