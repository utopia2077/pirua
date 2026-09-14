//! Shared close/fault arbitration and admitted-commit drain.

use std::sync::{Arc, Mutex};

use pi_agent_schema::{HarnessClosed, HarnessFault, HarnessRuntimeError};
use pi_ai_core::CancellationToken;

/// Process-local commit admission shared by all controls of one owner.
#[derive(Clone)]
pub struct Admission {
    inner: Arc<Inner>,
}

struct Inner {
    state: Mutex<State>,
    sealed: CancellationToken,
    drained: CancellationToken,
}

struct State {
    seal: Seal,
    commits: usize,
}

enum Seal {
    Open,
    Closed,
    Fault(HarnessFault),
}

impl State {
    fn check(&self) -> Result<(), HarnessRuntimeError> {
        match &self.seal {
            Seal::Open => Ok(()),
            Seal::Closed => Err(HarnessRuntimeError::Closed(HarnessClosed)),
            Seal::Fault(fault) => Err(HarnessRuntimeError::Fault(fault.clone())),
        }
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
                }),
                sealed: CancellationToken::new(),
                drained: CancellationToken::new(),
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
        let mut state = self.inner.state.lock().unwrap_or_else(|p| p.into_inner());
        match (&state.seal, fault.as_ref()) {
            (Seal::Open | Seal::Closed, Some(_)) => {
                state.seal = Seal::Fault(fault.take().expect("present fault"))
            }
            (Seal::Open, None) => state.seal = Seal::Closed,
            _ => {}
        }
        let drained = state.commits == 0;
        drop(state);
        // Fault sources and signal wakers may run arbitrary destructors/callbacks.
        drop(fault);
        if drained {
            self.inner.drained.cancel();
        }
        self.inner.sealed.cancel();
    }

    /// Resolves only once sealed and all permits have been released.
    ///
    /// This does not retain or drive storage futures and does not close Session.
    pub async fn drained(&self) {
        self.inner.drained.cancelled().await;
    }

    pub(crate) fn seal_signal(&self) -> impl Future<Output = ()> + Send + 'static {
        let signal = self.inner.sealed.clone();
        async move {
            signal.cancelled().await;
        }
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

/// Nonduplicable ownership of one admitted commit, not a background task.
#[must_use = "retain the permit until commit outcome and memory publication are observed"]
pub struct CommitPermit {
    admission: Admission,
}

impl Drop for CommitPermit {
    fn drop(&mut self) {
        let mut state = self
            .admission
            .inner
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        state.commits -= 1;
        let drained = state.commits == 0 && !matches!(state.seal, Seal::Open);
        drop(state);
        if drained {
            self.admission.inner.drained.cancel();
        }
    }
}
