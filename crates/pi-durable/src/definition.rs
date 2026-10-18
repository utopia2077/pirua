use crate::*;
use serde_json::Value;
use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
};
use tokio::sync::Notify;

pub type HandlerFuture = Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>>;
pub(crate) type Handler = Arc<dyn Fn(TaskRecord, Invocation) -> HandlerFuture + Send + Sync>;
type Initial = Arc<dyn Fn(&Value) -> Result<Value> + Send + Sync>;

/// A definition is immutable once installed; cloning preserves its identity.
#[derive(Clone)]
pub struct TaskDefinition {
    pub(crate) inner: Arc<Definition>,
}
pub(crate) struct Definition {
    pub kind: String,
    pub version: u32,
    pub initial: Initial,
    pub phases: BTreeMap<String, Handler>,
    pub abort: Handler,
}
impl TaskDefinition {
    pub fn new(
        kind: impl Into<String>,
        version: u32,
        initial: impl Fn(&Value) -> Result<Value> + Send + Sync + 'static,
    ) -> Result<Self> {
        let kind = kind.into();
        if kind.is_empty() || version == 0 {
            return Err(Error::request("task kind and positive version required"));
        }
        Ok(Self {
            inner: Arc::new(Definition {
                kind,
                version,
                initial: Arc::new(initial),
                phases: BTreeMap::new(),
                abort: Arc::new(|_, invocation| {
                    Box::pin(async move {
                        invocation
                            .commit(|_, _| {
                                Ok(Some(TaskState::Terminal {
                                    outcome: TaskOutcome::Aborted {
                                        reason: None,
                                        result: None,
                                    },
                                }))
                            })
                            .await
                    })
                }),
            }),
        })
    }
    pub fn phase<F, Fut>(mut self, name: impl Into<String>, handler: F) -> Self
    where
        F: Fn(TaskRecord, Invocation) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        let definition = Arc::make_mut(&mut self.inner);
        definition.phases.insert(
            name.into(),
            Arc::new(move |task, invocation| Box::pin(handler(task, invocation))),
        );
        self
    }
    pub fn abort<F, Fut>(mut self, handler: F) -> Self
    where
        F: Fn(TaskRecord, Invocation) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        Arc::make_mut(&mut self.inner).abort =
            Arc::new(move |task, invocation| Box::pin(handler(task, invocation)));
        self
    }
    pub fn kind(&self) -> &str {
        &self.inner.kind
    }
    pub fn version(&self) -> u32 {
        self.inner.version
    }
    pub(crate) fn same(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}
impl Clone for Definition {
    fn clone(&self) -> Self {
        Self {
            kind: self.kind.clone(),
            version: self.version,
            initial: self.initial.clone(),
            phases: self.phases.clone(),
            abort: self.abort.clone(),
        }
    }
}

#[derive(Clone, Default)]
pub struct RegistrySnapshot {
    registrations: Arc<BTreeMap<String, Vec<TaskDefinition>>>,
}
impl RegistrySnapshot {
    pub fn task(&self, kind: &str) -> Option<TaskDefinition> {
        self.registrations
            .values()
            .flatten()
            .find(|task| task.kind() == kind)
            .cloned()
    }
    pub fn tasks(&self) -> Vec<TaskDefinition> {
        self.registrations.values().flatten().cloned().collect()
    }
}
#[derive(Clone, Default)]
pub struct Registry {
    current: Arc<Mutex<RegistrySnapshot>>,
    pub(crate) changed: Arc<Notify>,
}
impl Registry {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn snapshot(&self) -> RegistrySnapshot {
        self.current.lock().unwrap().clone()
    }
    pub fn install(
        &self,
        registration: impl Into<String>,
        tasks: Vec<TaskDefinition>,
    ) -> Result<()> {
        let name = registration.into();
        if name.is_empty() {
            return Err(Error::request("registration name required"));
        }
        let mut current = self.current.lock().unwrap();
        let mut next = (*current.registrations).clone();
        next.insert(name, tasks);
        let mut seen = std::collections::BTreeSet::new();
        for task in next.values().flatten() {
            if !seen.insert(task.kind()) {
                return Err(Error::request(format!(
                    "Task {} is already installed",
                    task.kind()
                )));
            }
        }
        *current = RegistrySnapshot {
            registrations: Arc::new(next),
        };
        drop(current);
        self.changed.notify_one();
        Ok(())
    }
    pub fn uninstall(&self, registration: &str) {
        let mut current = self.current.lock().unwrap();
        let mut next = (*current.registrations).clone();
        next.remove(registration);
        *current = RegistrySnapshot {
            registrations: Arc::new(next),
        };
        drop(current);
        self.changed.notify_one();
    }
}

#[derive(Clone, Debug, Default)]
pub struct TaskOptions {
    pub conversation_id: Option<ConversationId>,
    pub owner: Option<TaskId>,
    pub background: bool,
    pub abandon_on_restart: bool,
    pub trace: Option<Value>,
    pub links: Option<Vec<Value>>,
}
/// Create a task inside the caller's atomic Session transaction.
pub fn create_task(
    tx: &mut Transaction<'_>,
    definition: &TaskDefinition,
    input: Value,
    options: TaskOptions,
) -> Result<TaskId> {
    if options.owner.is_some() && options.background {
        return Err(Error::request("task-owned tasks cannot be background"));
    }
    let checkpoint = (definition.inner.initial)(&input)?;
    validate_checkpoint(&checkpoint)?;
    tx.create_task(session::TaskCreate {
        kind: definition.kind().into(),
        version: definition.version(),
        input,
        checkpoint,
        conversation_id: options.conversation_id,
        ownership: options.owner.map_or(
            session::TaskOwnership::Conversation,
            session::TaskOwnership::Task,
        ),
        background: options.background,
        abandon_on_restart: options.abandon_on_restart,
        trace: options.trace,
        links: options.links,
    })
}
pub(crate) fn checkpoint(state: &TaskState) -> Option<&Value> {
    match state {
        TaskState::Pending { checkpoint }
        | TaskState::Running { checkpoint }
        | TaskState::Waiting { checkpoint, .. }
        | TaskState::Sleeping { checkpoint, .. } => Some(checkpoint),
        _ => None,
    }
}
pub(crate) fn validate_checkpoint(checkpoint: &Value) -> Result<&str> {
    checkpoint
        .get("phase")
        .and_then(Value::as_str)
        .filter(|phase| !phase.is_empty())
        .ok_or_else(|| Error::request("checkpoint requires a phase string"))
}
pub(crate) fn completed(outcome: &TaskOutcome) -> bool {
    matches!(outcome, TaskOutcome::Completed { .. })
}
pub(crate) fn failed(record: &TaskRecord) -> bool {
    match &record.state {
        TaskState::Terminal { outcome } | TaskState::Completing { outcome } => !completed(outcome),
        _ => false,
    }
}
