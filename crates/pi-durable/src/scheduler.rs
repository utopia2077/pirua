use crate::{graph::Graph, *};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};
use tokio::sync::{Notify, watch};

pub type SettleOutcome =
    Arc<dyn Fn(&mut Transaction<'_>, &TaskRecord, &TaskOutcome) -> Result<()> + Send + Sync>;
pub type WithdrawInputs =
    Arc<dyn Fn(&mut Transaction<'_>, ConversationId) -> Result<()> + Send + Sync>;
#[derive(Clone)]
pub struct SchedulerOptions {
    pub restart_limit: u64,
    pub now: Arc<dyn Fn() -> f64 + Send + Sync>,
    pub on_wake: Option<Arc<dyn Fn(Wake) + Send + Sync>>,
    pub timer_host: Option<TimerHost>,
    pub settle_outcome: Option<SettleOutcome>,
    pub withdraw_inputs: Option<WithdrawInputs>,
    pub on_report: Option<Arc<dyn Fn(Report) + Send + Sync>>,
}
impl Default for SchedulerOptions {
    fn default() -> Self {
        Self {
            restart_limit: 5,
            now: Arc::new(crate::clock::wall_clock),
            on_wake: None,
            timer_host: None,
            settle_outcome: None,
            withdraw_inputs: None,
            on_report: None,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Wake {
    pub active: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub at: Option<f64>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum AbortDisposition {
    Marked,
    Terminal,
}
#[derive(Clone, Debug)]
pub struct Report {
    pub task_id: Option<TaskId>,
    pub kind: &'static str,
    pub error: Error,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BlockedReason {
    MissingTask,
    TaskTooOld,
    MigrationFailed,
    RestartLimit,
}
impl BlockedReason {
    pub(crate) fn name(&self) -> &'static str {
        match self {
            Self::MissingTask => "missing_task",
            Self::TaskTooOld => "task_too_old",
            Self::MigrationFailed => "migration_failed",
            Self::RestartLimit => "restart_limit",
        }
    }
}
#[derive(Clone, Debug)]
pub enum InspectionState {
    Running,
    Completing,
    Waiting { on: Vec<TaskId> },
    Sleeping { until: f64 },
    Blocked { reason: BlockedReason },
    Ready,
}
#[derive(Clone, Debug)]
pub struct TaskInspection {
    pub record: TaskRecord,
    pub state: InspectionState,
}
#[derive(Clone, Debug)]
pub struct Inspection {
    pub scheduling: &'static str,
    pub tasks: Vec<TaskInspection>,
}
#[derive(Clone)]
pub struct Scheduler {
    pub(crate) inner: Arc<Inner>,
}
pub(crate) struct Inner {
    pub session: Session,
    pub registry: Registry,
    pub options: SchedulerOptions,
    pub enabled: AtomicBool,
    pub closing: AtomicBool,
    pub active: Mutex<BTreeMap<TaskId, Arc<Lease>>>,
    pub next: AtomicU64,
    pub reported_definitions: Mutex<BTreeMap<TaskId, TaskDefinition>>,
    pub notify: Arc<Notify>,
    pub changes: watch::Sender<u64>,
    pub stopped: watch::Sender<bool>,
    pub failure: Mutex<Option<Error>>,
}
impl Scheduler {
    pub async fn open(
        session: Session,
        registry: Registry,
        options: SchedulerOptions,
    ) -> Result<Self> {
        if options.restart_limit == 0 {
            return Err(Error::request("restart limit must be positive"));
        }
        let mut subscription = session.subscribe_commits().await?;
        let scheduler = Self {
            inner: Arc::new(Inner {
                session,
                registry,
                options,
                enabled: AtomicBool::new(false),
                closing: AtomicBool::new(false),
                active: Mutex::new(BTreeMap::new()),
                next: AtomicU64::new(1),
                reported_definitions: Mutex::new(BTreeMap::new()),
                notify: Arc::new(Notify::new()),
                changes: watch::channel(0).0,
                stopped: watch::channel(false).0,
                failure: Mutex::new(None),
            }),
        };
        let limit = scheduler.inner.options.restart_limit;
        let parked = scheduler
            .inner
            .session
            .commit(move |tx| {
                let graph = Graph::read(tx)?;
                let mut parked = Vec::new();
                for mut task in graph.tasks.into_values().filter(|t| !t.state.is_terminal()) {
                    if let TaskState::Running { checkpoint } = &task.state {
                        task.state = TaskState::Pending {
                            checkpoint: checkpoint.clone(),
                        };
                        task.restarts = Some(task.restarts.unwrap_or(0) + 1);
                        if !task.abort_requested && task.restarts.unwrap() >= limit {
                            parked.push(task.clone());
                        }
                        tx.replace_task(task)?;
                    }
                }
                Ok(parked)
            })
            .await?;
        for task in parked {
            scheduler.inner.report(Some(task.id),"task_parked",Error::request(format!("Task {} ({}) was cut off {} times without progress; it is parked until retry_task()",task.id,task.kind,task.restarts.unwrap_or(0))));
        }
        let live = scheduler.all_tasks().await?;
        let abandoned: Vec<_> = live
            .into_iter()
            .filter(|t| !t.state.is_terminal() && t.abandon_on_restart == Some(true))
            .map(|t| t.id)
            .collect();
        let inner = Arc::downgrade(&scheduler.inner);
        tokio::spawn(async move {
            while subscription.next().await.is_some() {
                let Some(inner) = inner.upgrade() else {
                    break;
                };
                inner.changed();
                inner.notify.notify_one();
            }
        });
        let driver = scheduler.clone();
        tokio::spawn(async move {
            driver.drive(abandoned).await;
        });
        Ok(scheduler)
    }
    pub fn resume(&self) {
        if !self.inner.closing.load(Ordering::SeqCst) {
            self.inner.enabled.store(true, Ordering::SeqCst);
            self.inner.notify.notify_one();
        }
    }
    pub async fn commit<T: Send + 'static>(
        &self,
        change: impl FnOnce(&mut Transaction<'_>) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let inner = self.inner.clone();
        self.inner
            .session
            .commit(move |tx| {
                inner.check_open()?;
                change(tx)
            })
            .await
    }
    pub async fn create_task(
        &self,
        definition: &TaskDefinition,
        input: Value,
        options: TaskOptions,
    ) -> Result<TaskId> {
        let definition = definition.clone();
        self.commit(move |tx| create_task(tx, &definition, input, options))
            .await
    }
    pub fn failure(&self) -> Option<Error> {
        self.inner
            .failure
            .lock()
            .unwrap()
            .clone()
            .or_else(|| self.inner.session.failure())
    }
    pub async fn get_task(&self, id: TaskId) -> Result<Option<TaskRecord>> {
        self.inner.session.task(id).await
    }
    pub fn task(&self, id: TaskId) -> TaskHandle {
        TaskHandle {
            scheduler: self.clone(),
            id,
        }
    }
    pub(crate) async fn all_tasks(&self) -> Result<Vec<TaskRecord>> {
        self.inner
            .session
            .commit(|tx| Ok(Graph::read(tx)?.tasks.into_values().collect()))
            .await
    }
    async fn graph(&self) -> Result<Graph> {
        self.inner.session.commit(Graph::read).await
    }
    pub async fn inspect(&self) -> Result<Inspection> {
        let graph = self.graph().await?;
        let snapshot = self.inner.registry.snapshot();
        let now = (self.inner.options.now)();
        let active = self.inner.active.lock().unwrap();
        let tasks = graph
            .tasks
            .values()
            .filter(|task| !task.state.is_terminal())
            .map(|task| {
                let on = graph.waiting_on(task);
                let state = if active.contains_key(&task.id) {
                    InspectionState::Running
                } else if matches!(task.state, TaskState::Completing { .. }) {
                    InspectionState::Completing
                } else if !on.is_empty() {
                    InspectionState::Waiting { on }
                } else if let TaskState::Sleeping { until, .. } = task.state
                    && !task.abort_requested
                    && until > now
                {
                    InspectionState::Sleeping { until }
                } else if let Some(reason) = self.inner.blocked(task, &snapshot, false) {
                    InspectionState::Blocked { reason }
                } else {
                    InspectionState::Ready
                };
                TaskInspection {
                    record: task.clone(),
                    state,
                }
            })
            .collect();
        Ok(Inspection {
            scheduling: if self.inner.closing.load(Ordering::SeqCst) {
                "closing"
            } else if self.inner.enabled.load(Ordering::SeqCst) {
                "running"
            } else {
                "paused"
            },
            tasks,
        })
    }
    pub async fn wake(&self) -> Result<Wake> {
        let graph = self.graph().await?;
        Ok(self.inner.wake(&graph))
    }
    pub async fn wait_for_task(&self, id: TaskId) -> Result<TaskRecord> {
        let mut changed = self.inner.changes.subscribe();
        self.resume();
        loop {
            self.inner.check_open()?;
            let task = self
                .get_task(id)
                .await?
                .ok_or_else(|| Error::request(format!("Task {id} does not exist")))?;
            if task.state.is_terminal() {
                return Ok(task);
            }
            changed
                .changed()
                .await
                .map_err(|_| Error::request("scheduler stopped"))?;
        }
    }
    pub async fn wait_for_idle(&self, conversation: Option<ConversationId>) -> Result<()> {
        let mut changed = self.inner.changes.subscribe();
        self.resume();
        loop {
            self.inner.check_open()?;
            let graph = self.graph().await?;
            if !graph
                .tasks
                .values()
                .any(|t| !t.state.is_terminal() && graph.in_scope(t, conversation, false))
            {
                return Ok(());
            }
            changed
                .changed()
                .await
                .map_err(|_| Error::request("scheduler stopped"))?;
        }
    }
    pub async fn retry_task(&self, id: TaskId) -> Result<()> {
        let limit = self.inner.options.restart_limit;
        self.commit(move |tx| {
            let mut task = tx
                .task(id)?
                .ok_or_else(|| Error::request(format!("Task {id} does not exist")))?;
            if !task.state.is_terminal()
                && !matches!(task.state, TaskState::Completing { .. })
                && !task.abort_requested
                && task.restarts.unwrap_or(0) >= limit
            {
                task.restarts = None;
                tx.replace_task(task)?;
            }
            Ok(())
        })
        .await?;
        self.resume();
        Ok(())
    }
    pub async fn abort_task(&self, id: TaskId) -> Result<AbortDisposition> {
        self.abort(id, false).await
    }
    pub(crate) async fn abort(&self, id: TaskId, keep_restart: bool) -> Result<AbortDisposition> {
        let inner = self.inner.clone();
        let run = self
            .commit(move |tx| {
                let mut graph = Graph::read(tx)?;
                let task = graph
                    .tasks
                    .get(&id)
                    .cloned()
                    .ok_or_else(|| Error::request(format!("Task {id} does not exist")))?;
                if task.state.is_terminal() {
                    return Ok((AbortDisposition::Terminal, None));
                }
                if !inner.active.lock().unwrap().contains_key(&id)
                    && !matches!(task.state, TaskState::Completing { .. })
                    && graph.owned_live(id).is_empty()
                    && !(keep_restart && task.abort_reason == Some(AbortReason::Restart))
                    && let Some(reason) = inner.blocked(&task, &inner.registry.snapshot(), true)
                {
                    decide_outcome(
                        tx,
                        &mut graph,
                        &inner,
                        task,
                        TaskOutcome::Orphaned {
                            reason: reason.name().into(),
                        },
                    )?;
                    return Ok((AbortDisposition::Marked, None));
                }
                if !(keep_restart && task.abort_reason == Some(AbortReason::Restart)) {
                    let mut marked = task.clone();
                    mark(&mut marked, None);
                    tx.replace_task(marked)?;
                }
                Ok((
                    AbortDisposition::Marked,
                    inner
                        .active
                        .lock()
                        .unwrap()
                        .get(&id)
                        .filter(|lease| lease.identity.mode == InvocationMode::Run)
                        .cloned(),
                ))
            })
            .await?;
        self.inner.signal_marks().await?;
        self.inner.notify.notify_one();
        if let Some(run) = run.1 {
            // A handler may ignore its cancellation signal. Storage failure ends
            // abort waits independently; normal close still joins the handler.
            let failed = async {
                match self.inner.session.closed().await {
                    session::SessionEnd::Failed(error) => Err(error),
                    session::SessionEnd::Closed => std::future::pending::<Result<()>>().await,
                }
            };
            if let futures_util::future::Either::Right((failure, _)) =
                futures_util::future::select(Box::pin(run.join()), Box::pin(failed)).await
            {
                failure?;
            }
        }
        Ok(run.0)
    }
    pub async fn abort_conversation(
        &self,
        conversation: ConversationId,
        background: bool,
    ) -> Result<()> {
        let withdraw = self.inner.options.withdraw_inputs.clone();
        let reached = self
            .commit(move |tx| {
                let graph = Graph::read(tx)?;
                let mut reached = Vec::new();
                for task in graph.tasks.values().filter(|task| {
                    !task.state.is_terminal()
                        && graph.in_scope(task, Some(conversation), background)
                }) {
                    let mut marked = task.clone();
                    mark(&mut marked, None);
                    tx.replace_task(marked)?;
                    reached.push(task.id);
                }
                if let Some(withdraw) = withdraw {
                    for id in graph.conversations.keys() {
                        if graph.conversation_in_scope(*id, conversation, background) {
                            withdraw(tx, *id)?;
                        }
                    }
                }
                Ok(reached)
            })
            .await?;
        self.inner.signal_marks().await?;
        self.resume();
        if background {
            for id in reached {
                self.wait_for_task(id).await?;
            }
        }
        self.wait_for_idle(Some(conversation)).await
    }
    pub async fn close(&self) -> Result<()> {
        if self.inner.closing.swap(true, Ordering::SeqCst) {
            let mut stopped = self.inner.stopped.subscribe();
            if !*stopped.borrow() {
                let _ = stopped.changed().await;
            }
            return self
                .inner
                .failure
                .lock()
                .unwrap()
                .clone()
                .map_or(Ok(()), Err);
        }
        self.inner.changed();
        self.inner.notify.notify_one();
        let active: Vec<_> = self
            .inner
            .active
            .lock()
            .unwrap()
            .values()
            .cloned()
            .collect();
        let stopped_ids: Vec<_> = active.iter().map(|lease| lease.identity.task_id).collect();
        let mut closing = Box::pin(self.inner.session.close_with(
            async move {
                for lease in &active {
                    lease.signal.cancel();
                }
                for lease in &active {
                    lease.join().await;
                }
            },
            move |tx| {
                let current: Vec<_> = stopped_ids
                    .into_iter()
                    .map(|id| tx.task(id))
                    .collect::<Result<_>>()?;
                for mut task in current.into_iter().flatten() {
                    if let TaskState::Running { checkpoint } = task.state {
                        task.state = TaskState::Pending { checkpoint };
                        tx.replace_task(task)?;
                    }
                }
                Ok(())
            },
        ));
        // Poll once now: this establishes the Session seal on this close poll.
        // Its owned worker retains join/finalize even when this wait is dropped.
        let immediate = std::future::poll_fn(|cx| {
            std::task::Poll::Ready(match closing.as_mut().poll(cx) {
                std::task::Poll::Ready(result) => Some(result),
                std::task::Poll::Pending => None,
            })
        })
        .await;
        drop(closing);
        let inner = self.inner.clone();
        tokio::spawn(async move {
            let result = match immediate {
                Some(result) => result,
                None => match inner.session.closed().await {
                    session::SessionEnd::Closed => Ok(()),
                    session::SessionEnd::Failed(error) => Err(error),
                },
            };
            if let Err(error) = result {
                *inner.failure.lock().unwrap() = Some(error);
            }
            inner.stopped.send_replace(true);
        });
        let mut stopped = self.inner.stopped.subscribe();
        if !*stopped.borrow() {
            let _ = stopped.changed().await;
        }
        self.inner
            .failure
            .lock()
            .unwrap()
            .clone()
            .map_or(Ok(()), Err)
    }
    async fn drive(self, mut abandoned: Vec<TaskId>) {
        let mut sleep_timer: Option<Timer> = None;
        let mut last_wake = None;
        loop {
            if self.inner.closing.load(Ordering::SeqCst) {
                break;
            }
            let result = self.pass(&mut abandoned).await;
            if let Err(error) = result {
                if !self.inner.closing.load(Ordering::SeqCst) {
                    self.inner.report(None, "scheduler_error", error.clone());
                    self.inner.fail_runtime(error).await;
                }
                break;
            }
            if self.inner.closing.load(Ordering::SeqCst) {
                break;
            }
            if let Ok(graph) = self.graph().await {
                let wake = self.inner.wake(&graph);
                if last_wake.as_ref() != Some(&wake)
                    && let Some(host) = &self.inner.options.on_wake
                {
                    host(wake.clone());
                }
                // A host timer may wake early after a wall-clock adjustment. Always
                // recheck and rearm from the persisted absolute deadline.
                if let Some(timer) = sleep_timer.take() {
                    timer.cancel();
                }
                if self.inner.options.on_wake.is_none()
                    && self.inner.enabled.load(Ordering::SeqCst)
                    && let Some(at) = wake.at
                {
                    let weak = Arc::downgrade(&self.inner);
                    sleep_timer = Some(self.inner.timer(
                        Arc::new(move || {
                            if let Some(inner) = weak.upgrade() {
                                inner.notify.notify_one();
                            }
                        }),
                        at - (self.inner.options.now)(),
                    ));
                }
                last_wake = Some(wake);
            }
            let _ = futures_util::future::select(
                Box::pin(self.inner.notify.notified()),
                Box::pin(self.inner.registry.changed.notified()),
            )
            .await;
        }
        if let Some(timer) = sleep_timer {
            timer.cancel();
        }
    }
    pub(crate) async fn pass(&self, abandoned: &mut Vec<TaskId>) -> Result<()> {
        let inner = self.inner.clone();
        let enabled = inner.enabled.load(Ordering::SeqCst);
        let abandon = if enabled {
            std::mem::take(abandoned)
        } else {
            Vec::new()
        };
        let reservations = self
            .inner
            .session
            .commit(move |tx| {
                inner.check_open()?;
                let mut active = inner.active.lock().unwrap();
                if inner.closing.load(Ordering::SeqCst) {
                    return Ok((Vec::new(), Vec::new()));
                }
                let mut graph = Graph::read(tx)?;
                for id in abandon {
                    if let Some(task) = graph.tasks.get_mut(&id)
                        && !task.abort_requested
                        && !task.state.is_terminal()
                    {
                        mark(task, Some(AbortReason::Restart));
                        tx.replace_task(task.clone())?;
                    }
                }
                reconcile(tx, &mut graph, &inner)?;
                if !enabled {
                    return Ok((Vec::new(), Vec::new()));
                }
                let snapshot = inner.registry.snapshot();
                let mut reservations = Vec::new();
                let mut reports = Vec::new();
                let candidates: Vec<_> = graph
                    .tasks
                    .values()
                    .filter(|task| {
                        !task.state.is_terminal()
                            && !matches!(task.state, TaskState::Completing { .. })
                    })
                    .cloned()
                    .collect();
                for mut task in candidates {
                    if active.contains_key(&task.id) || !graph.waiting_on(&task).is_empty() {
                        continue;
                    }
                    if !task.abort_requested
                        && let TaskState::Sleeping { until, .. } = task.state
                        && until > (inner.options.now)()
                    {
                        continue;
                    }
                    if !task.abort_requested && graph.cancelled_above(&task).is_some() {
                        continue;
                    }
                    let fit = inner.blocked(&task, &snapshot, true);
                    if task.abort_requested
                        && task.restarts.unwrap_or(0) >= inner.options.restart_limit
                    {
                        let task_id = task.id;
                        let message = format!(
                            "Abort handler of task {} ({}) was cut off {} times",
                            task.id,
                            task.kind,
                            task.restarts.unwrap_or(0)
                        );
                        decide_outcome(
                            tx,
                            &mut graph,
                            &inner,
                            task,
                            TaskOutcome::Faulted {
                                error: TaskOutcomeError {
                                    message: message.clone(),
                                    detail: Some(serde_json::json!({"reason":"restart_limit"})),
                                },
                            },
                        )?;
                        reports.push(Report {
                            task_id: Some(task_id),
                            kind: "task_faulted",
                            error: Error::request(message),
                        });
                        continue;
                    }
                    if let Some(reason) = fit {
                        if reason == BlockedReason::MigrationFailed
                            && let Some(definition) = snapshot.task(&task.kind)
                        {
                            let mut reported = inner.reported_definitions.lock().unwrap();
                            if !reported
                                .get(&task.id)
                                .is_some_and(|previous| previous.same(&definition))
                            {
                                reports.push(Report {
                                    task_id: Some(task.id),
                                    kind: "migration_failed",
                                    error: Error::request(format!(
                                        "Task {} version {} has no migration from {}",
                                        task.kind,
                                        definition.version(),
                                        task.version
                                    )),
                                });
                                reported.insert(task.id, definition);
                            }
                        }
                        if task.abort_requested && task.abort_reason.is_none() {
                            decide_outcome(
                                tx,
                                &mut graph,
                                &inner,
                                task,
                                TaskOutcome::Orphaned {
                                    reason: reason.name().into(),
                                },
                            )?;
                        }
                        continue;
                    }
                    let definition = snapshot.task(&task.kind).expect("fitting definition");
                    let mode = if task.abort_requested {
                        InvocationMode::Abort
                    } else {
                        InvocationMode::Run
                    };
                    let state = TaskState::Running {
                        checkpoint: checkpoint(&task.state)
                            .expect("runnable checkpoint")
                            .clone(),
                    };
                    if task.state != state {
                        task.state = state;
                        tx.replace_task(task.clone())?;
                        graph.tasks.insert(task.id, task.clone());
                    }
                    let lease = Arc::new(Lease::new(InvocationIdentity {
                        task_id: task.id,
                        number: inner.next.fetch_add(1, Ordering::SeqCst),
                        mode,
                    }));
                    active.insert(task.id, lease.clone());
                    reservations.push((task, lease, definition, snapshot.clone()));
                }
                Ok((reservations, reports))
            })
            .await?;
        // The decision batch succeeded. Emit its reports before any later read
        // can fail, so an unrelated failure cannot erase this committed receipt.
        for report in reservations.1 {
            self.inner.report(report.task_id, report.kind, report.error);
        }
        self.inner.signal_marks().await?;
        for (record, lease, definition, snapshot) in reservations.0 {
            let scheduler = self.clone();
            tokio::spawn(async move {
                scheduler.run(record, lease, definition, snapshot).await;
            });
        }
        self.inner.changed();
        Ok(())
    }
}
impl Inner {
    pub async fn fail_runtime(&self, error: Error) {
        *self.failure.lock().unwrap() = Some(error);
        self.changed();
        for lease in self.active.lock().unwrap().values() {
            lease.signal.cancel();
        }
        let _ = self.session.close().await;
    }
    pub fn check_open(&self) -> Result<()> {
        if self.closing.load(Ordering::SeqCst) {
            return Err(Error::new(ErrorKind::Closed, "Session is closed"));
        }
        if let Some(error) = self.failure.lock().unwrap().clone() {
            return Err(error);
        }
        Ok(())
    }
    pub fn changed(&self) {
        self.changes
            .send_modify(|version| *version = version.wrapping_add(1));
    }
    pub fn report(&self, task_id: Option<TaskId>, kind: &'static str, error: Error) {
        if let Some(report) = &self.options.on_report {
            report(Report {
                task_id,
                kind,
                error,
            });
        }
    }
    pub fn timer(&self, callback: Arc<dyn Fn() + Send + Sync>, delay: f64) -> Timer {
        self.options.timer_host.as_ref().map_or_else(
            || crate::clock::native_timer(callback.clone(), delay),
            |host| host(callback.clone(), delay),
        )
    }
    pub fn blocked(
        &self,
        task: &TaskRecord,
        snapshot: &RegistrySnapshot,
        for_reservation: bool,
    ) -> Option<BlockedReason> {
        if matches!(
            task.state,
            TaskState::Completing { .. } | TaskState::Terminal { .. }
        ) {
            return None;
        }
        if !task.abort_requested && task.restarts.unwrap_or(0) >= self.options.restart_limit {
            return Some(BlockedReason::RestartLimit);
        }
        if task.abort_requested && task.abort_reason.is_none() && !for_reservation {
            return None;
        }
        match snapshot.task(&task.kind) {
            None => Some(BlockedReason::MissingTask),
            Some(definition) if definition.version() < task.version => {
                Some(BlockedReason::TaskTooOld)
            }
            Some(definition) if definition.version() > task.version => {
                Some(BlockedReason::MigrationFailed)
            }
            _ => None,
        }
    }
    fn wake(&self, graph: &Graph) -> Wake {
        let snapshot = self.registry.snapshot();
        let now = (self.options.now)();
        let mut wake = Wake {
            active: !self.active.lock().unwrap().is_empty(),
            at: None,
        };
        for task in graph
            .tasks
            .values()
            .filter(|task| !task.state.is_terminal())
        {
            if let TaskState::Completing { .. } = task.state {
                if graph.owned_live(task.id).is_empty() {
                    wake.active = true;
                }
                continue;
            }
            if !graph.waiting_on(task).is_empty() {
                continue;
            }
            if !task.abort_requested
                && let TaskState::Sleeping { until, .. } = task.state
                && until > now
            {
                wake.at = Some(wake.at.map_or(until, |at| at.min(until)));
                continue;
            }
            if self.blocked(task, &snapshot, false).is_none() {
                wake.active = true;
            }
        }
        wake
    }
    pub async fn signal_marks(&self) -> Result<()> {
        let active: Vec<_> = self.active.lock().unwrap().values().cloned().collect();
        for lease in active {
            if lease.identity.mode == InvocationMode::Run
                && self
                    .session
                    .task(lease.identity.task_id)
                    .await?
                    .is_some_and(|task| task.abort_requested)
            {
                lease.signal.cancel();
            }
        }
        Ok(())
    }
    pub fn end(&self, lease: &Lease) {
        lease.end();
        let mut active = self.active.lock().unwrap();
        if active
            .get(&lease.identity.task_id)
            .is_some_and(|current| current.identity == lease.identity)
        {
            active.remove(&lease.identity.task_id);
        }
        drop(active);
        self.changed();
        self.notify.notify_one();
    }
}
pub(crate) fn mark(task: &mut TaskRecord, reason: Option<AbortReason>) {
    if !task.abort_requested || (task.abort_reason.is_some() && reason.is_none()) {
        task.abort_requested = true;
        task.abort_reason = reason;
        task.restarts = None;
    }
}
pub(crate) fn decide_outcome(
    tx: &mut Transaction<'_>,
    graph: &mut Graph,
    inner: &Inner,
    mut task: TaskRecord,
    outcome: TaskOutcome,
) -> Result<()> {
    graph.overlay(tx);
    task.state = if graph.owned_live(task.id).is_empty() {
        TaskState::Terminal { outcome }
    } else {
        TaskState::Completing { outcome }
    };
    task.restarts = None;
    task.memos = None;
    tx.replace_task(task.clone())?;
    if let TaskState::Terminal { outcome } = &task.state
        && matches!(
            outcome,
            TaskOutcome::Faulted { .. } | TaskOutcome::Orphaned { .. }
        )
        && let Some(settle) = &inner.options.settle_outcome
    {
        settle(tx, &task, outcome)?;
    }
    graph.tasks.insert(task.id, task);
    Ok(())
}
fn reconcile(tx: &mut Transaction<'_>, graph: &mut Graph, inner: &Inner) -> Result<()> {
    loop {
        let mut updates = BTreeMap::new();
        for task in graph
            .tasks
            .values()
            .filter(|task| !task.state.is_terminal())
        {
            if let Some(reason) = graph.cancelled_above(task) {
                let mut marked = task.clone();
                mark(&mut marked, reason);
                if &marked != task {
                    updates.insert(task.id, marked);
                }
            }
            if let TaskState::Waiting {
                on,
                policy: JoinPolicy::FailFast,
                ..
            } = &task.state
                && on.iter().any(|id| graph.tasks.get(id).is_some_and(failed))
            {
                for id in on {
                    if let Some(member) = graph.tasks.get(id)
                        && !member.state.is_terminal()
                        && !failed(member)
                    {
                        let mut marked = member.clone();
                        mark(&mut marked, None);
                        if &marked != member {
                            updates.insert(*id, marked);
                        }
                    }
                }
            }
            if let TaskState::Completing { outcome } = &task.state
                && graph.owned_live(task.id).is_empty()
            {
                let mut terminal = task.clone();
                terminal.state = TaskState::Terminal {
                    outcome: outcome.clone(),
                };
                updates.insert(task.id, terminal);
            }
        }
        if updates.is_empty() {
            break;
        }
        for (id, task) in updates {
            tx.replace_task(task.clone())?;
            if let TaskState::Terminal { outcome } = &task.state
                && matches!(
                    outcome,
                    TaskOutcome::Faulted { .. } | TaskOutcome::Orphaned { .. }
                )
                && let Some(settle) = &inner.options.settle_outcome
            {
                settle(tx, &task, outcome)?;
            }
            graph.tasks.insert(id, task);
        }
    }
    if let Some(withdraw) = &inner.options.withdraw_inputs {
        for id in graph.conversations.keys() {
            if graph.conversation_cancelled(*id) {
                withdraw(tx, *id)?;
            }
        }
    }
    Ok(())
}
#[derive(Clone)]
pub struct TaskHandle {
    scheduler: Scheduler,
    pub id: TaskId,
}
impl TaskHandle {
    pub async fn get(&self) -> Result<Option<TaskRecord>> {
        self.scheduler.get_task(self.id).await
    }
    pub async fn wait(&self) -> Result<TaskRecord> {
        self.scheduler.wait_for_task(self.id).await
    }
    pub async fn abort(&self) -> Result<AbortDisposition> {
        self.scheduler.abort_task(self.id).await
    }
    pub async fn retry(&self) -> Result<()> {
        self.scheduler.retry_task(self.id).await
    }
}
