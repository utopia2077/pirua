use crate::{
    graph::Graph,
    scheduler::{Inner, decide_outcome},
    *,
};
use futures_util::FutureExt;
use serde_json::Value;
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::watch;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InvocationMode {
    Run,
    Abort,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InvocationIdentity {
    pub task_id: TaskId,
    pub number: u64,
    pub mode: InvocationMode,
}
pub(crate) struct Lease {
    pub identity: InvocationIdentity,
    pub signal: Cancellation,
    pub active: AtomicBool,
    pub done: watch::Sender<bool>,
    pub timers: Mutex<BTreeMap<u64, Timer>>,
    next_timer: std::sync::atomic::AtomicU64,
}
impl Lease {
    pub fn new(identity: InvocationIdentity) -> Self {
        Self {
            identity,
            signal: Cancellation::new(),
            active: AtomicBool::new(true),
            done: watch::channel(false).0,
            timers: Mutex::new(BTreeMap::new()),
            next_timer: std::sync::atomic::AtomicU64::new(1),
        }
    }
    pub async fn join(&self) {
        let mut done = self.done.subscribe();
        if !*done.borrow() {
            let _ = done.changed().await;
        }
    }
    pub fn check(&self) -> Result<()> {
        if self.active.load(Ordering::SeqCst) {
            Ok(())
        } else {
            Err(Error::request(format!(
                "Task {} invocation has ended",
                self.identity.task_id
            )))
        }
    }
    pub fn end(&self) {
        if self.active.swap(false, Ordering::SeqCst) {
            self.signal.cancel();
            for timer in std::mem::take(&mut *self.timers.lock().unwrap()).into_values() {
                timer.cancel();
            }
        }
    }
}
/// One phase's immutable registry view and a live invocation lease.
#[derive(Clone)]
pub struct Invocation {
    pub(crate) inner: Arc<Inner>,
    pub(crate) lease: Arc<Lease>,
    pub(crate) snapshot: RegistrySnapshot,
    pub(crate) conversation_id: ConversationId,
}
impl Invocation {
    pub fn identity(&self) -> InvocationIdentity {
        self.lease.identity
    }
    pub fn registry(&self) -> RegistrySnapshot {
        self.snapshot.clone()
    }
    pub fn signal(&self) -> Cancellation {
        self.lease.signal.clone()
    }
    pub fn now(&self) -> Result<f64> {
        self.lease.check()?;
        Ok((self.inner.options.now)())
    }
    pub async fn commit(
        &self,
        change: impl FnOnce(&mut Transaction<'_>, &TaskRecord) -> Result<Option<TaskState>>
        + Send
        + 'static,
    ) -> Result<()> {
        self.transact(move |tx, current| Ok(((), change(tx, current)?)))
            .await
    }
    /// Return a receipt together with an optional next state from the same atomic batch.
    pub async fn transact<T: Send + 'static>(
        &self,
        change: impl FnOnce(&mut Transaction<'_>, &TaskRecord) -> Result<(T, Option<TaskState>)>
        + Send
        + 'static,
    ) -> Result<T> {
        self.lease.check()?;
        let inner = self.inner.clone();
        let lease = self.lease.clone();
        let (result,created) = self.inner.session.commit_scoped(session::TransactionScope { conversation_id: Some(self.conversation_id), task_id:Some(lease.identity.task_id) }, move |tx| {
            lease.check()?; inner.check_open()?;
            let mut graph=Graph::read(tx)?;
            let current=graph.tasks.get(&lease.identity.task_id).cloned().ok_or_else(|| Error::request("task does not exist"))?;
            if !matches!(current.state,TaskState::Running{..}) { return Err(Error::request(format!("Task {} is {}",current.id,current.state.status()))); }
            if lease.identity.mode==InvocationMode::Run && current.abort_requested { return Err(Error::request(format!("Task {} has a durable abort mark",current.id))); }
            let (result,next)=change(tx,&current)?;
            let created=tx.staged_tasks().into_iter().any(|task|!graph.tasks.contains_key(&task.id)); graph.overlay(tx);
            if let Some(next)=next {
                match &next {
                    TaskState::Waiting{checkpoint,on,policy} => { validate_checkpoint(checkpoint)?; graph.validate_wait(&current,on,*policy,lease.identity.mode)?; }
                    TaskState::Sleeping{checkpoint,until} => {
                        validate_checkpoint(checkpoint)?;
                        if lease.identity.mode==InvocationMode::Abort { return Err(Error::request(format!("Abort handler of task {} cannot sleep",current.id))); }
                        if !until.is_finite() || *until<0.0 { return Err(Error::request("sleep deadline must be finite nonnegative milliseconds")); }
                    }
                    TaskState::Running{checkpoint} => { validate_checkpoint(checkpoint)?; }
                    TaskState::Terminal{outcome:TaskOutcome::Completed{..}|TaskOutcome::Failed{..}|TaskOutcome::Aborted{..}} => {},
                    _=>return Err(Error::request("handler state must be running, waiting, sleeping or a completed/failed/aborted terminal outcome")),
                }
                let mut candidate=current.clone();
                if next != current.state { candidate.restarts=None; }
                if let TaskState::Terminal{outcome}=next { decide_outcome(tx,&mut graph,&inner,candidate,outcome)?; }
                else { candidate.state=next; tx.replace_task(candidate)?; }
            }
            Ok((result,created))
        }).await?;
        if created
            && self.inner.enabled.load(Ordering::SeqCst)
            && !self.inner.closing.load(Ordering::SeqCst)
        {
            Scheduler {
                inner: self.inner.clone(),
            }
            .pass(&mut Vec::new())
            .await?;
        }
        Ok(result)
    }
    pub async fn memo(&self, name: &str) -> Result<Option<Value>> {
        self.lease.check()?;
        Ok(self
            .inner
            .session
            .task(self.identity().task_id)
            .await?
            .and_then(|task| task.memos.and_then(|memos| memos.get(name).cloned())))
    }
    pub async fn memo_or_insert(&self, name: impl Into<String>, candidate: Value) -> Result<Value> {
        let name = name.into();
        self.transact(move |tx, current| {
            if let Some(winner) = current.memos.as_ref().and_then(|m| m.get(&name)) {
                return Ok((winner.clone(), None));
            }
            let mut record = current.clone();
            record
                .memos
                .get_or_insert_default()
                .insert(name, candidate.clone());
            tx.replace_task(record)?;
            Ok((candidate, None))
        })
        .await
    }
    pub async fn sleep_until(&self, until: f64, checkpoint: Value) -> Result<()> {
        self.commit(move |_, _| Ok(Some(TaskState::Sleeping { checkpoint, until })))
            .await
    }
    pub async fn get_task(&self, id: TaskId) -> Result<Option<TaskRecord>> {
        self.lease.check()?;
        self.inner.session.task(id).await
    }
    pub async fn owned_tasks(&self) -> Result<Vec<TaskRecord>> {
        self.lease.check()?;
        let lease = self.lease.clone();
        self.inner
            .session
            .commit(move |tx| {
                lease.check()?;
                Ok(Graph::read(tx)?
                    .tasks
                    .into_values()
                    .filter(|task| {
                        task.owner == Some(lease.identity.task_id) && !task.state.is_terminal()
                    })
                    .collect())
            })
            .await
    }
    pub async fn outcomes(&self, ids: Vec<TaskId>) -> Result<Vec<TaskOutcome>> {
        self.lease.check()?;
        let lease = self.lease.clone();
        self.inner
            .session
            .commit(move |tx| {
                lease.check()?;
                let graph = Graph::read(tx)?;
                ids.into_iter()
                    .map(|id| match graph.tasks.get(&id).map(|task| &task.state) {
                        Some(TaskState::Terminal { outcome }) => Ok(outcome.clone()),
                        _ => Err(Error::request(format!("Task {id} is not terminal"))),
                    })
                    .collect()
            })
            .await
    }
    /// An in-memory wait; it creates no persisted wait edge.
    pub async fn wait_for_task(&self, id: TaskId) -> Result<TaskRecord> {
        self.lease.check()?;
        let scheduler = Scheduler {
            inner: self.inner.clone(),
        };
        match futures_util::future::select(
            Box::pin(scheduler.wait_for_task(id)),
            Box::pin(self.lease.signal.cancelled()),
        )
        .await
        {
            futures_util::future::Either::Left((result, _)) => result,
            futures_util::future::Either::Right(_) => Err(Error::request("invocation cancelled")),
        }
    }
    pub async fn abort_owned(&self, id: TaskId) -> Result<()> {
        self.lease.check()?;
        let record = self
            .get_task(id)
            .await?
            .ok_or_else(|| Error::request("task does not exist"))?;
        if record.owner != Some(self.identity().task_id) {
            return Err(Error::request("task is not owned by this invocation"));
        }
        if record.state.is_terminal() {
            return Ok(());
        }
        let scheduler = Scheduler {
            inner: self.inner.clone(),
        };
        match futures_util::future::select(
            Box::pin(async {
                scheduler.abort(id, true).await?;
                scheduler.wait_for_task(id).await?;
                Ok(())
            }),
            Box::pin(self.lease.signal.cancelled()),
        )
        .await
        {
            futures_util::future::Either::Left((result, _)) => result,
            futures_util::future::Either::Right(_) => Err(Error::request("invocation cancelled")),
        }
    }
    pub fn set_timer(
        &self,
        callback: impl Fn() + Send + Sync + 'static,
        delay_ms: f64,
    ) -> Result<Timer> {
        self.lease.check()?;
        if !delay_ms.is_finite() {
            return Err(Error::request("timer delay must be finite"));
        }
        let id = self.lease.next_timer.fetch_add(1, Ordering::SeqCst);
        let weak = Arc::downgrade(&self.lease);
        let timer = self.inner.timer(
            Arc::new(move || {
                if let Some(lease) = weak.upgrade()
                    && lease.active.load(Ordering::SeqCst)
                {
                    lease.timers.lock().unwrap().remove(&id);
                    callback();
                }
            }),
            delay_ms,
        );
        // Ending and timer insertion share this mutex, so no newly armed timer escapes cleanup.
        let mut timers = self.lease.timers.lock().unwrap();
        if self.lease.active.load(Ordering::SeqCst) {
            timers.insert(id, timer.clone());
        } else {
            timer.cancel();
        }
        let lease = Arc::downgrade(&self.lease);
        Ok(Timer::new(move || {
            if let Some(lease) = lease.upgrade()
                && let Some(timer) = lease.timers.lock().unwrap().remove(&id)
            {
                timer.cancel();
            }
        }))
    }
}
struct InvocationFinish {
    inner: Arc<Inner>,
    lease: Arc<Lease>,
}
impl Drop for InvocationFinish {
    fn drop(&mut self) {
        self.inner.end(&self.lease);
        self.lease.done.send_replace(true);
    }
}
impl Scheduler {
    pub(crate) async fn run(
        &self,
        _reserved: TaskRecord,
        lease: Arc<Lease>,
        definition: TaskDefinition,
        mut snapshot: RegistrySnapshot,
    ) {
        let _finish = InvocationFinish {
            inner: self.inner.clone(),
            lease: lease.clone(),
        };
        let result = async {
            // None means no outdated resolution has been reported. Some(None)
            // records a missing definition; Some(Some(..)) records its identity.
            let mut reported_definition: Option<Option<TaskDefinition>> = None;
            loop {
                let Some(record) = self.get_task(lease.identity.task_id).await? else {
                    break;
                };
                if self.inner.closing.load(Ordering::SeqCst)
                    || !matches!(record.state, TaskState::Running { .. })
                    || (lease.identity.mode == InvocationMode::Run && record.abort_requested)
                {
                    break;
                }
                let previous_checkpoint = checkpoint(&record.state)
                    .expect("running checkpoint")
                    .clone();
                let invocation = Invocation {
                    inner: self.inner.clone(),
                    lease: lease.clone(),
                    snapshot: snapshot.clone(),
                    conversation_id: record.conversation_id,
                };
                let phase = validate_checkpoint(&previous_checkpoint)?.to_string();
                let handler = if lease.identity.mode == InvocationMode::Abort {
                    Some(definition.inner.abort.clone())
                } else {
                    definition.inner.phases.get(&phase).cloned()
                };
                let failure = match handler {
                    Some(handler) => {
                        match std::panic::AssertUnwindSafe(handler(record.clone(), invocation))
                            .catch_unwind()
                            .await
                        {
                            Ok(result) => result.err(),
                            Err(_) => Some(Error::request("task handler panicked")),
                        }
                    }
                    None => Some(Error::request(format!(
                        "Task {} has no phase {phase}",
                        record.kind
                    ))),
                };
                let inner = self.inner.clone();
                let current_lease = lease.clone();
                let next_snapshot = self.inner.registry.snapshot();
                let replacement = next_snapshot.task(&record.kind);
                let handover = replacement
                    .as_ref()
                    .is_some_and(|next| next.version() == record.version && !definition.same(next));
                let next = self
                    .inner
                    .session
                    .commit(move |tx| {
                        let mut graph = Graph::read(tx)?;
                        let Some(current) =
                            graph.tasks.get(&current_lease.identity.task_id).cloned()
                        else {
                            return Ok(None);
                        };
                        if !matches!(current.state, TaskState::Running { .. })
                            || inner.closing.load(Ordering::SeqCst)
                            || (current_lease.identity.mode == InvocationMode::Run
                                && (current.abort_requested
                                    || graph.cancelled_above(&current).is_some()))
                        {
                            inner.end(&current_lease);
                            return Ok(None);
                        }
                        let fault = if let Some(error) = failure {
                            Some(error)
                        } else if current_lease.identity.mode == InvocationMode::Abort {
                            Some(Error::request(format!(
                                "Abort handler of task {} returned without a terminal outcome",
                                current.id
                            )))
                        } else if checkpoint(&current.state) == Some(&previous_checkpoint) {
                            Some(Error::request(format!(
                                "Task {} phase {phase} returned without durable progress",
                                current.kind
                            )))
                        } else {
                            None
                        };
                        if let Some(error) = fault {
                            inner.end(&current_lease);
                            decide_outcome(
                                tx,
                                &mut graph,
                                &inner,
                                current,
                                TaskOutcome::Faulted {
                                    error: TaskOutcomeError {
                                        message: error.message.clone(),
                                        detail: None,
                                    },
                                },
                            )?;
                            return Ok(Some(Err(error)));
                        }
                        if handover {
                            let mut pending = current;
                            pending.state = TaskState::Pending {
                                checkpoint: checkpoint(&pending.state).unwrap().clone(),
                            };
                            tx.replace_task(pending)?;
                            inner.end(&current_lease);
                            return Ok(None);
                        }
                        Ok(Some(Ok(())))
                    })
                    .await?;
                snapshot = next_snapshot;
                match next {
                    None => break,
                    Some(Err(error)) => {
                        self.inner
                            .report(Some(lease.identity.task_id), "task_faulted", error);
                        break;
                    }
                    Some(Ok(())) => {}
                }
                // A compatible handover ends this invocation above. Otherwise
                // retain the old execution, refresh the phase snapshot, and report
                // each newly resolved unavailable/incompatible definition once.
                if replacement
                    .as_ref()
                    .is_none_or(|next| !definition.same(next))
                {
                    let already_reported = reported_definition.as_ref().is_some_and(|previous| {
                        match (previous, &replacement) {
                            (None, None) => true,
                            (Some(previous), Some(next)) => previous.same(next),
                            _ => false,
                        }
                    });
                    if !already_reported {
                        reported_definition = Some(replacement.clone());
                        self.inner.report(
                            Some(lease.identity.task_id),
                            "task_outdated",
                            Error::request(format!(
                                "Task {} keeps running under its old {} definition",
                                record.id, record.kind
                            )),
                        );
                    }
                }
            }
            Ok::<(), Error>(())
        }
        .await;
        if let Err(error) = result
            && !self.inner.closing.load(Ordering::SeqCst)
        {
            self.inner
                .report(Some(lease.identity.task_id), "task_error", error.clone());
            self.inner.fail_runtime(error).await;
        }
        self.inner.end(&lease);
        lease.done.send_replace(true);
    }
}
