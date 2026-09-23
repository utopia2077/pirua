//! Shared close/fault arbitration, acquisition tickets and admitted-commit drain.

use std::{
    any::Any,
    collections::HashMap,
    future::Future,
    panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};

use pi_agent_schema::{HarnessClosed, HarnessFault, HarnessRuntimeError};

use super::mutation_line::{Ticket, retire_ticket};

/// Process-local commit admission shared by all controls of one owner.
#[derive(Clone)]
pub struct Admission {
    pub(crate) inner: Arc<Inner>,
}

pub(crate) struct Inner {
    pub(crate) state: Mutex<State>,
}

pub(crate) struct State {
    seal: Seal,
    commits: usize,
    next_ticket: u64,
    pub(crate) tickets: HashMap<u64, Ticket>,
    sealed: Signal,
    drained: Signal,
}

enum Seal {
    Open,
    Closed,
    Fault(HarnessFault),
}

#[derive(Default)]
struct Signal {
    complete: bool,
    next: u64,
    waiters: HashMap<u64, Waker>,
}

impl Signal {
    fn complete(&mut self, callbacks: &mut Callbacks) {
        if !self.complete {
            self.complete = true;
            callbacks
                .wakes
                .extend(self.waiters.drain().map(|(_, wake)| wake));
        }
    }
}

pub(crate) struct Callbacks {
    pub(crate) drops: Vec<Waker>,
    pub(crate) wakes: Vec<Waker>,
    pub(crate) tickets: Vec<Ticket>,
}

impl Callbacks {
    pub(crate) fn new() -> Self {
        Self {
            drops: Vec::new(),
            wakes: Vec::new(),
            tickets: Vec::new(),
        }
    }

    pub(crate) fn append(&mut self, mut other: Self) {
        self.drops.append(&mut other.drops);
        self.wakes.append(&mut other.wakes);
        self.tickets.append(&mut other.tickets);
    }

    pub(crate) fn dispatch(self) {
        let mut first: Option<Box<dyn Any + Send>> = None;
        for wake in self.drops {
            record_panic(catch_unwind(AssertUnwindSafe(|| drop(wake))), &mut first);
        }
        for wake in self.wakes {
            record_panic(catch_unwind(AssertUnwindSafe(|| wake.wake())), &mut first);
        }
        // Ticket shells contain only scheduler-owned Arcs after their Wakers
        // have been moved into the callback lists above.
        drop(self.tickets);
        if let Some(panic) = first {
            if std::thread::panicking() {
                let _ = catch_unwind(AssertUnwindSafe(|| drop(panic)));
            } else {
                resume_unwind(panic);
            }
        }
    }
}

fn record_panic(result: Result<(), Box<dyn Any + Send>>, first: &mut Option<Box<dyn Any + Send>>) {
    if let Err(panic) = result {
        if first.is_none() {
            *first = Some(panic);
        } else {
            let _ = catch_unwind(AssertUnwindSafe(|| drop(panic)));
        }
    }
}

impl State {
    pub(crate) fn check(&self) -> Result<(), HarnessRuntimeError> {
        match &self.seal {
            Seal::Open => Ok(()),
            Seal::Closed => Err(HarnessRuntimeError::Closed(HarnessClosed)),
            Seal::Fault(fault) => Err(HarnessRuntimeError::Fault(fault.clone())),
        }
    }

    pub(crate) fn ticket_id(&mut self) -> u64 {
        let id = self.next_ticket;
        self.next_ticket = self
            .next_ticket
            .checked_add(1)
            .expect("ticket IDs exhausted");
        id
    }
}

impl Default for Admission {
    fn default() -> Self {
        Self::new()
    }
}

impl Admission {
    /// Starts open, with no admitted commits.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                state: Mutex::new(State {
                    seal: Seal::Open,
                    commits: 0,
                    next_ticket: 0,
                    tickets: HashMap::new(),
                    sealed: Signal::default(),
                    drained: Signal::default(),
                }),
            }),
        }
    }

    /// Admits one commit. Keep its permit through definitive completion and publication.
    pub fn try_admit(&self) -> Result<CommitPermit, HarnessRuntimeError> {
        let mut state = self.inner.state.lock().unwrap_or_else(|p| p.into_inner());
        state.check()?;
        state.commits = state
            .commits
            .checked_add(1)
            .expect("commit count exhausted");
        Ok(CommitPermit {
            admission: self.clone(),
        })
    }

    /// Observes the current seal; a later fault can promote a closed owner.
    pub fn check(&self) -> Result<(), HarnessRuntimeError> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .check()
    }

    /// Rejects future admission without revoking already-admitted commits.
    pub fn seal_close(&self) {
        self.seal(None);
    }

    /// Retains the first fault, including a failure discovered while closing.
    pub fn seal_fault(&self, fault: HarnessFault) {
        self.seal(Some(fault));
    }

    fn seal(&self, mut fault: Option<HarnessFault>) {
        let mut retired = Callbacks::new();
        let mut signals = Callbacks::new();
        let mut previous = None;
        {
            let mut state = self.inner.state.lock().unwrap_or_else(|p| p.into_inner());
            match (&state.seal, fault.as_ref()) {
                (Seal::Open | Seal::Closed, Some(_)) => {
                    previous = Some(std::mem::replace(
                        &mut state.seal,
                        Seal::Fault(fault.take().expect("present fault")),
                    ));
                }
                (Seal::Open, None) => state.seal = Seal::Closed,
                _ => {}
            }
            // Retirement is independent of signal delivery. A panicking drain
            // or earlier sealed Waker cannot leave an acquisition in a queue.
            let ids: Vec<_> = state.tickets.keys().copied().collect();
            for id in ids {
                retire_ticket(&mut state, id, &mut retired, true);
            }
            if state.commits == 0 {
                state.drained.complete(&mut signals);
            }
            state.sealed.complete(&mut signals);
        }
        drop(previous);
        drop(fault);
        // The signals may have been registered before any acquisition. Their
        // callbacks run first, after both completion bits and ticket retirement
        // are committed; each later ticket wake is attempted despite a panic.
        signals.append(retired);
        signals.dispatch();
    }

    /// Resolves only once sealed and all permits have been released.
    pub async fn drained(&self) {
        SignalFuture::new(self.clone(), SignalKind::Drained).await;
    }

    pub(crate) fn seal_signal(&self) -> impl Future<Output = ()> + Send + 'static {
        SignalFuture::new(self.clone(), SignalKind::Sealed)
    }

    // Only internal state transitions belong here: never invoke external code.
    pub(crate) fn while_open<T>(
        &self,
        transition: impl FnOnce() -> T,
    ) -> Result<T, HarnessRuntimeError> {
        let state = self.inner.state.lock().unwrap_or_else(|p| p.into_inner());
        state.check()?;
        Ok(transition())
    }
}

enum SignalKind {
    Sealed,
    Drained,
}

struct SignalFuture {
    admission: Admission,
    kind: SignalKind,
    id: Option<u64>,
}

impl SignalFuture {
    fn new(admission: Admission, kind: SignalKind) -> Self {
        Self {
            admission,
            kind,
            id: None,
        }
    }

    fn signal<'a>(&self, state: &'a mut State) -> &'a mut Signal {
        match self.kind {
            SignalKind::Sealed => &mut state.sealed,
            SignalKind::Drained => &mut state.drained,
        }
    }
}

impl Future for SignalFuture {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let mut candidate = Some(cx.waker().clone());
        let mut old = None;
        let completed;
        let id = self.id;
        let mut next_id = None;
        {
            let mut state = self
                .admission
                .inner
                .state
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            let signal = self.signal(&mut state);
            completed = signal.complete;
            if !completed {
                if let Some(id) = id {
                    old = signal
                        .waiters
                        .insert(id, candidate.take().expect("candidate"));
                } else {
                    let id = signal.next;
                    signal.next = signal.next.checked_add(1).expect("signal IDs exhausted");
                    signal
                        .waiters
                        .insert(id, candidate.take().expect("candidate"));
                    next_id = Some(id);
                }
            }
        }
        if let Some(id) = next_id {
            self.id = Some(id);
        }
        drop(old);
        drop(candidate);
        if completed {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

impl Drop for SignalFuture {
    fn drop(&mut self) {
        let Some(id) = self.id else { return };
        let removed = {
            let mut state = self
                .admission
                .inner
                .state
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            self.signal(&mut state).waiters.remove(&id)
        };
        drop(removed);
    }
}

/// Nonduplicable ownership of one admitted commit, not a background task.
#[must_use = "retain the permit until commit outcome and memory publication are observed"]
pub struct CommitPermit {
    admission: Admission,
}

impl Drop for CommitPermit {
    fn drop(&mut self) {
        let mut callbacks = Callbacks::new();
        {
            let mut state = self
                .admission
                .inner
                .state
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            state.commits -= 1;
            if state.commits == 0 && !matches!(state.seal, Seal::Open) {
                state.drained.complete(&mut callbacks);
            }
        }
        callbacks.dispatch();
    }
}
