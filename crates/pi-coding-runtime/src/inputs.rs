//! Exact process-local input receipts, separate from provider messages and legacy events.
use crate::{
    CodingSession, Delivery, InputSource, PromptOptions, SessionError, WeakCodingSession,
    session::lock,
};
pub use pi_agent::InputId;
use pi_agent::{AgentMessage, BoxFuture, CancellationToken, InputAdmission};
use pi_ai::ImageContent;
use std::collections::BTreeMap;
use tokio::sync::oneshot;

#[derive(Clone, Debug)]
pub enum InputResolution {
    Inserted {
        entry_id: String,
        message: AgentMessage,
    },
    Handled,
    Cancelled,
    Rejected {
        message: String,
    },
}
pub struct InputTicket {
    pub id: InputId,
    pub resolution: oneshot::Receiver<InputResolution>,
    session: WeakCodingSession,
}
#[derive(Clone)]
pub struct InputCancellation {
    id: InputId,
    session: WeakCodingSession,
}
impl InputCancellation {
    pub fn cancel(&self) {
        if let Some(session) = self.session.upgrade() {
            session.cancel_input(self.id);
        }
    }
}
impl InputTicket {
    pub fn cancellation_handle(&self) -> InputCancellation {
        InputCancellation {
            id: self.id,
            session: self.session.clone(),
        }
    }
    /// Withdraw this admission if it has not committed. After insertion this is a no-op.
    pub fn cancel(&self) {
        self.cancellation_handle().cancel();
    }
}
pub enum QueueAdmission {
    Handled,
    Queued(InputTicket),
}
#[derive(Clone, Debug)]
pub struct QueuedInput {
    pub id: InputId,
    pub delivery: Delivery,
    pub text: String,
    pub images: Vec<ImageContent>,
    pub source: InputSource,
}
pub(crate) struct TrackedInput {
    pub admission: InputAdmission,
    pub sender: oneshot::Sender<InputResolution>,
    pub queued: Option<QueuedInput>,
    pub owns_run: bool,
}
#[derive(Default)]
pub(crate) struct Inputs {
    pub pending: BTreeMap<InputId, TrackedInput>,
}
impl Drop for Inputs {
    fn drop(&mut self) {
        for (_, input) in std::mem::take(&mut self.pending) {
            input.admission.cancellation.cancel();
            let _ = input.sender.send(InputResolution::Cancelled);
        }
    }
}
impl Inputs {
    pub fn resolve(&mut self, id: InputId, outcome: InputResolution) {
        if let Some(input) = self.pending.remove(&id) {
            let _ = input.sender.send(outcome);
        }
    }
}
pub(crate) struct InputDriverGuard {
    pub session: CodingSession,
    pub id: InputId,
    pub completed: bool,
}
impl Drop for InputDriverGuard {
    fn drop(&mut self) {
        if !self.completed {
            self.session.cancel_input(self.id);
        }
    }
}
impl CodingSession {
    pub(crate) fn reserve_input(&self) -> (InputTicket, InputAdmission) {
        let id = InputId::new();
        let weak = self.downgrade();
        let admission = InputAdmission {
            id,
            cancellation: CancellationToken::new(),
            on_rejected: Some(std::sync::Arc::new(move |error| {
                if let Some(session) = weak.upgrade() {
                    session.reject_input(id, error.to_string());
                }
            })),
        };
        let (sender, resolution) = oneshot::channel();
        lock(&self.inner.inputs).pending.insert(
            admission.id,
            TrackedInput {
                admission: admission.clone(),
                sender,
                queued: None,
                owns_run: false,
            },
        );
        (
            InputTicket {
                id: admission.id,
                resolution,
                session: self.downgrade(),
            },
            admission,
        )
    }
    pub fn tracked_prompt(
        &self,
        text: impl Into<String>,
        options: PromptOptions,
    ) -> (InputTicket, BoxFuture<'static, Result<(), SessionError>>) {
        let text = text.into();
        let session = self.clone();
        let (ticket, admission) = self.reserve_input();
        // Construct outside the async block so dropping an unpolled driver resolves it too.
        let guard = InputDriverGuard {
            session: session.clone(),
            id: ticket.id,
            completed: false,
        };
        let driver = Box::pin(async move {
            let mut guard = guard;
            let id = admission.id;
            let result = session.prompt_input(text, options, Some(admission)).await;
            if let Err(error) = &result {
                session.reject_input(id, error.to_string());
            }
            if result.is_ok() {
                let mut inputs = lock(&session.inner.inputs);
                if inputs.pending.get(&id).is_some_and(|i| i.queued.is_none()) {
                    inputs.resolve(id, InputResolution::Cancelled);
                }
            }
            guard.completed = true;
            result
        });
        (ticket, driver)
    }
    pub(crate) fn cancel_input(&self, id: InputId) {
        self.finish_input(id, InputResolution::Cancelled);
    }
    pub(crate) fn reject_input(&self, id: InputId, message: String) {
        self.finish_input(id, InputResolution::Rejected { message });
    }
    fn finish_input(&self, id: InputId, outcome: InputResolution) {
        let mut inputs = lock(&self.inner.inputs);
        if let Some(input) = inputs.pending.get(&id) {
            let cancelling = matches!(outcome, InputResolution::Cancelled);
            if cancelling || input.queued.is_some() {
                input.admission.cancellation.cancel();
            }
            if cancelling && input.owns_run {
                self.agent().abort();
            }
            self.agent().remove_queued_input(id);
            let mut state = lock(&self.inner.state);
            state.steering.retain(|m| m.id != Some(id));
            state.follow_up.retain(|m| m.id != Some(id));
        }
        inputs.resolve(id, outcome);
    }
    pub(crate) fn resolve_unqueued_inputs(&self, error: Option<&str>) {
        // Queue publication uses Inputs -> Agent ownership too. Keep that
        // boundary across the FIFO snapshot and reconciliation so a new
        // admission cannot be mistaken for an old driver's drained input.
        let mut inputs = lock(&self.inner.inputs);
        let (steering, follow_up) = self.agent().queued_inputs();
        let queued = steering
            .iter()
            .chain(&follow_up)
            .filter_map(|m| m.input.as_ref().map(|i| i.id))
            .collect::<std::collections::BTreeSet<_>>();
        #[cfg(test)]
        tests::after_fifo_snapshot();
        let ids = inputs
            .pending
            .iter()
            .filter(|(id, input)| input.queued.is_some() && !queued.contains(id))
            .map(|(id, _)| *id)
            .collect::<Vec<_>>();
        for id in ids {
            inputs.resolve(
                id,
                if let Some(message) = error {
                    InputResolution::Rejected {
                        message: message.into(),
                    }
                } else {
                    InputResolution::Cancelled
                },
            );
        }
    }
    pub(crate) fn cancel_all_inputs(&self) {
        let mut inputs = lock(&self.inner.inputs);
        let pending = std::mem::take(&mut inputs.pending);
        for (_, input) in pending {
            input.admission.cancellation.cancel();
            let _ = input.sender.send(InputResolution::Cancelled);
        }
    }
    /// Reads the actual Agent queues, not the legacy text-only display mirror.
    pub fn queued_inputs(&self) -> (Vec<QueuedInput>, Vec<QueuedInput>) {
        let (steering, follow_up) = self.agent().queued_inputs();
        let inputs = lock(&self.inner.inputs);
        let read = |messages: Vec<pi_agent::AdmittedMessage>| {
            messages
                .into_iter()
                .filter_map(|m| {
                    m.input
                        .and_then(|a| inputs.pending.get(&a.id).and_then(|i| i.queued.clone()))
                })
                .collect()
        };
        (read(steering), read(follow_up))
    }
    pub fn observe_history(&self) -> Result<pi_coding_session::HistorySubscription, SessionError> {
        self.ensure_live()?;
        Ok(lock(&self.inner.manager).observe_history())
    }
}

#[cfg(test)]
#[path = "../tests/session_support/mod.rs"]
mod test_support;

#[cfg(test)]
mod tests {
    use super::*;
    use std::{cell::RefCell, sync::Arc, time::Duration};

    thread_local! {
        // Pause only this unit test's settlement at the exact stale-snapshot boundary.
        static AFTER_FIFO_SNAPSHOT: RefCell<Option<Box<dyn FnOnce()>>> = RefCell::new(None);
    }
    pub(super) fn after_fifo_snapshot() {
        AFTER_FIFO_SNAPSHOT.with(|hook| {
            if let Some(hook) = hook.take() {
                hook();
            }
        });
    }

    #[test]
    fn settlement_cannot_retire_an_admission_published_after_its_fifo_snapshot() {
        use crate::{CodingSessionBuilder, NoopSessionHooks, QueueMode};
        use pi_coding_session::{MemoryStore, SessionManager};
        use tokio::sync::oneshot::error::TryRecvError;

        for delivery in [Delivery::Steer, Delivery::FollowUp] {
            for mode in [QueueMode::OneAtATime, QueueMode::All] {
                for error in [None, Some("old driver failed")] {
                    let session = test_support::runtime().block_on(async {
                        let (stream, _) = test_support::scripted(vec![]);
                        CodingSessionBuilder::new(
                            SessionManager::create(Arc::new(MemoryStore::new()), "/tmp").unwrap(),
                            test_support::model(),
                            stream,
                        )
                        .with_hooks(Arc::new(NoopSessionHooks))
                        .with_tools(vec![])
                        .build()
                        .await
                        .unwrap()
                    });
                    session.set_steering_mode(mode);
                    session.set_follow_up_mode(mode);
                    let (start, begin) = std::sync::mpsc::channel();
                    let (published, complete) = std::sync::mpsc::channel();
                    let ticket = Arc::new(std::sync::Mutex::new(None));
                    let admitted = std::thread::spawn({
                        let (session, ticket) = (session.clone(), ticket.clone());
                        move || {
                            begin.recv_timeout(Duration::from_secs(5)).unwrap();
                            let queued = test_support::runtime()
                                .block_on(session.queue_tracked("new owner", vec![], delivery))
                                .unwrap();
                            let QueueAdmission::Queued(queued) = queued else {
                                panic!("expected actual queue admission");
                            };
                            *ticket.lock().unwrap() = Some(queued);
                            let _ = published.send(());
                        }
                    });
                    AFTER_FIFO_SNAPSHOT.with(|hook| {
                        let session = session.clone();
                        *hook.borrow_mut() = Some(Box::new(move || {
                            start.send(()).unwrap();
                            // If settlement released admission ownership after taking
                            // its snapshot, force the new queue publication to finish
                            // before it resumes. With the shared lock, publication
                            // waits until settlement returns instead.
                            if let Ok(inputs) = session.inner.inputs.try_lock() {
                                drop(inputs);
                                complete.recv_timeout(Duration::from_secs(5)).unwrap();
                            }
                        }));
                    });
                    session.resolve_unqueued_inputs(error);
                    admitted.join().unwrap();
                    let mut ticket = ticket.lock().unwrap().take().unwrap();
                    assert!(
                        matches!(ticket.resolution.try_recv(), Err(TryRecvError::Empty)),
                        "old settlement retired a different owner's new admission"
                    );
                    let (steering, follow_up) = session.queued_inputs();
                    let actual = if delivery == Delivery::Steer {
                        steering
                    } else {
                        follow_up
                    };
                    assert_eq!(actual.len(), 1);
                    assert_eq!(actual[0].id, ticket.id);
                    test_support::runtime().block_on(async {
                        let cleared = session.clear_queue_tracked().await.unwrap();
                        assert_eq!(cleared.len(), 1);
                        assert_eq!(cleared[0].id, ticket.id);
                        assert!(matches!(
                            ticket.resolution.await.unwrap(),
                            InputResolution::Cancelled
                        ));
                    });
                    assert!(!session.agent().has_queued_messages());
                }
            }
        }
    }
}
