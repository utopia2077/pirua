use super::{Inner, SearchError};
use pi_agent_schema::SessionId;
use pi_agent_session::read::RepositoryRead;
use std::{
    collections::{BTreeMap, VecDeque},
    thread::ThreadId,
    time::Instant,
};

#[derive(Clone, Copy)]
pub(super) struct Hint {
    pub deadline: Instant,
    pub barrier: i64,
}

#[derive(Default)]
pub(super) struct State {
    pub active: Option<ThreadId>,
    pub closing: bool,
    pub closed: bool,
    pub queue: BTreeMap<String, Hint>,
    pub hint_admissions: u64,
    pub errors: VecDeque<SearchError>,
}
pub(super) struct Admission<'a, R: RepositoryRead>(&'a Inner<R>);
impl<R: RepositoryRead> Drop for Admission<'_, R> {
    fn drop(&mut self) {
        if let Ok(mut state) = self.0.state.lock() {
            state.active = None;
        }
        self.0.wake.notify_all();
    }
}
impl<R: RepositoryRead> Inner<R> {
    pub(super) fn admit(&self) -> Result<Admission<'_, R>, SearchError> {
        let mut state = self.state.lock().map_err(|_| SearchError::Worker)?;
        if state.closing || state.closed {
            return Err(SearchError::Closed);
        }
        if state.active.is_some() {
            return Err(SearchError::Busy);
        }
        state.active = Some(std::thread::current().id());
        Ok(Admission(self))
    }
    pub(super) fn enqueue(&self, id: &SessionId) {
        // Hint admission precedes its index-only barrier read. Close drains this
        // short operation too; no source content or callback is consumed here.
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        if state.closing || state.closed {
            return;
        }
        let Some(next) = state.hint_admissions.checked_add(1) else {
            return;
        };
        state.hint_admissions = next;
        drop(state);
        self.phase("hint_admitted");
        let progress = self.progress(None);
        if let Ok(mut state) = self.state.lock() {
            state.hint_admissions -= 1;
            if !state.closing && !state.closed {
                match progress {
                    Ok(progress) => {
                        if let Some(deadline) = Instant::now().checked_add(self.options.debounce) {
                            state.queue.insert(
                                id.as_str().to_owned(),
                                Hint {
                                    deadline,
                                    barrier: progress.barrier,
                                },
                            );
                        }
                    }
                    Err(error) => {
                        if state.errors.len() == 16 {
                            state.errors.pop_front();
                        }
                        state.errors.push_back(error);
                    }
                }
            }
        }
        self.wake.notify_all();
    }
    pub(super) fn shutdown(&self, dropping: bool) -> Result<(), SearchError> {
        let current = std::thread::current().id();
        let mut state = self.state.lock().map_err(|_| SearchError::Worker)?;
        if state.closed {
            return Ok(());
        }
        if state.active == Some(current) {
            if dropping {
                state.closing = true;
                state.queue.clear();
                self.wake.notify_all();
            }
            return Err(SearchError::Busy);
        }
        // One external closer owns the join. Others wait for its acknowledgment.
        if state.closing {
            while !state.closed {
                state = self.wake.wait(state).map_err(|_| SearchError::Worker)?;
            }
            return Ok(());
        }
        state.closing = true;
        state.queue.clear();
        self.wake.notify_all();
        while state.active.is_some() || state.hint_admissions != 0 {
            state = self.wake.wait(state).map_err(|_| SearchError::Worker)?;
        }
        drop(state);
        let worker = self.worker.lock().map_err(|_| SearchError::Worker)?.take();
        let result = if let Some(worker) = worker {
            worker.join().map_err(|_| SearchError::Worker)
        } else {
            Ok(())
        };
        let mut state = self.state.lock().map_err(|_| SearchError::Worker)?;
        state.closed = true;
        self.wake.notify_all();
        result
    }
}
impl<R> Inner<R>
where
    R: RepositoryRead + 'static,
    R::ListOptions: Clone + Send + Sync + 'static,
{
    pub(super) fn run_worker(&self) {
        loop {
            let mut state = match self.state.lock() {
                Ok(s) => s,
                Err(_) => return,
            };
            let (id, barrier) = loop {
                if state.closing {
                    return;
                }
                let due = state
                    .queue
                    .iter()
                    .min_by_key(|(_, hint)| hint.deadline)
                    .map(|(id, hint)| (id.clone(), *hint));
                if state.active.is_none()
                    && let Some((id, hint)) = &due
                    && hint.deadline <= Instant::now()
                {
                    state.queue.remove(id);
                    state.active = Some(std::thread::current().id());
                    break (id.clone(), hint.barrier);
                }
                state = match due {
                    Some((_, hint)) if state.active.is_none() => match self.wake.wait_timeout(
                        state,
                        hint.deadline.saturating_duration_since(Instant::now()),
                    ) {
                        Ok((s, _)) => s,
                        Err(_) => return,
                    },
                    _ => match self.wake.wait(state) {
                        Ok(s) => s,
                        Err(_) => return,
                    },
                };
            };
            drop(state);
            let admission = Admission(self);
            // Trusted callbacks can fail or panic; neither strands worker admission.
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let id = SessionId::new(id).map_err(|_| SearchError::InvalidOptions)?;
                self.sweep_at(Some(&id), Some(barrier))
            }))
            .unwrap_or(Err(SearchError::Worker));
            if let Err(error) = result
                && let Ok(mut state) = self.state.lock()
            {
                if state.errors.len() == 16 {
                    state.errors.pop_front();
                }
                state.errors.push_back(error);
            }
            drop(admission);
        }
    }
}
