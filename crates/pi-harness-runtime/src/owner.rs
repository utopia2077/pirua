//! Concrete session owner and configured restore-only factory.
#![allow(missing_docs)]
use crate::{
    hooks::HookRegistry,
    manual_gate::ManualGate,
    restore::{HydratedLane, fault, restore_configured},
    scheduler::{Admission, MutationLine, Supervisor},
};
use pi_agent_schema::*;
use pi_agent_session::{
    context::SummaryRenderer,
    session::RuntimeSession,
    storage::{RuntimeLease, RuntimeStorage},
};
use pi_ai_core::{Clock, ModelsRegistry, SafeUnsigned, TaskSpawner};
use pi_telemetry::TelemetryContext;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

pub type Telemetry = Arc<dyn TelemetryContext>;
pub struct RuntimeFactory<C, S: RuntimeStorage, M: SessionMetadataRecord = SessionMetadata> {
    marker: std::marker::PhantomData<fn() -> (S, M)>,
    pub(crate) clock: Arc<dyn Clock>,
    pub(crate) spawner: Arc<dyn TaskSpawner>,
    pub(crate) ids: Arc<dyn IdGenerator>,
    pub(crate) context: Arc<C>,
    pub(crate) summaries: Arc<dyn SummaryRenderer>,
}
impl<C, S: RuntimeStorage, M: SessionMetadataRecord> RuntimeFactory<C, S, M> {
    pub fn new(
        clock: Arc<dyn Clock>,
        spawner: Arc<dyn TaskSpawner>,
        ids: Arc<dyn IdGenerator>,
        context: Arc<C>,
        summaries: Arc<dyn SummaryRenderer>,
    ) -> Self {
        Self {
            marker: std::marker::PhantomData,
            clock,
            spawner,
            ids,
            context,
            summaries,
        }
    }
}
#[derive(Clone)]
pub(crate) struct Settings {
    pub revision: SafeUnsigned,
    pub resources: Resources,
    pub stream_options: AgentHarnessStreamOptions,
    pub retry: RetryPolicy,
    pub run: RunSettings,
}
pub(crate) struct LaneControl {
    pub name: LaneId,
    pub changed: Mutex<pi_ai_core::CancellationToken>,
    pub driving: Mutex<bool>,
    pub reservation: Mutex<Option<OperationId>>,
    pub streaming: Mutex<Option<(EntryId, pi_ai_core::AssistantMessage)>>,
    pub running_tools: Mutex<BTreeMap<EffectKey, RunningToolSnapshot>>,
    pub line: MutationLine,
    pub gate: ManualGate,
    pub hydrated: Mutex<HydratedLane>,
}
pub(crate) struct Owner<C, S: RuntimeStorage, M: SessionMetadataRecord> {
    pub session: Arc<RuntimeSession<S, M>>,
    pub models: Arc<ModelsRegistry>,
    pub clock: Arc<dyn Clock>,
    pub host: Arc<dyn TaskSpawner>,
    pub effects: Supervisor,
    pub ids: Arc<dyn IdGenerator>,
    pub context: ContextSource<C>,
    pub prompt: Option<SystemPromptSource<C>>,
    pub summaries: Arc<dyn SummaryRenderer>,
    pub converter: Option<Arc<dyn ProviderMessageConverter>>,
    pub projectors: BTreeMap<CustomType, Arc<dyn EntryProjector>>,
    pub telemetry: Telemetry,
    pub hooks: HookRegistry,
    pub events: crate::events::EventHub,
    pub admission: Admission,
    pub settings_line: MutationLine,
    pub settings: Mutex<Settings>,
    pub tools: Mutex<Vec<AgentHarnessToolHandle<C>>>,
    pub lanes: Mutex<BTreeMap<LaneId, Arc<LaneControl>>>,
    pub lease: Mutex<Option<RuntimeLease>>,
    pub seed: LaneConfiguration,
    pub drive_mode: DriveMode,
    pub close_started: std::sync::atomic::AtomicBool,
    pub closed: pi_ai_core::CancellationToken,
}
impl<C, S: RuntimeStorage, M: SessionMetadataRecord> Owner<C, S, M> {
    pub fn fail(&self, message: &'static str) -> HarnessRuntimeError {
        self.observe::<()>(Err(fault(message)))
            .expect_err("fault seals owner")
    }
}
pub struct RuntimeLane<C, S: RuntimeStorage, M: SessionMetadataRecord> {
    pub(crate) owner: Arc<Owner<C, S, M>>,
    pub(crate) lane: Arc<LaneControl>,
}
impl<C, S: RuntimeStorage, M: SessionMetadataRecord> Clone for RuntimeLane<C, S, M> {
    fn clone(&self) -> Self {
        Self {
            owner: self.owner.clone(),
            lane: self.lane.clone(),
        }
    }
}
pub struct RuntimeHarness<C, S: RuntimeStorage, M: SessionMetadataRecord> {
    pub(crate) main: RuntimeLane<C, S, M>,
}
impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>
    RuntimeFactory<C, S, M>
{
    pub async fn create_configured(
        &self,
        options: AgentHarnessOptions<C, Telemetry, RuntimeSession<S, M>, ModelsRegistry>,
    ) -> Result<AgentHarnessCreate<RuntimeHarness<C, S, M>>, HarnessRuntimeError> {
        validate_stream_options(
            &options.stream_options,
            options.session.codec().header_policy(),
        )?;
        let admission = Admission::new();
        let lease = options
            .session
            .claim_runtime()
            .map_err(|_| fault("session already attached or unavailable"))?;
        let restored = restore_configured(options.session.as_ref(), &admission).await?;
        let suspended = restored.values().filter_map(suspended).collect();
        let lanes = restored
            .into_iter()
            .map(|(name, hydrated)| {
                let lane = Arc::new(LaneControl {
                    name: name.clone(),
                    changed: Mutex::new(pi_ai_core::CancellationToken::new()),
                    driving: Mutex::new(false),
                    reservation: Mutex::new(None),
                    streaming: Mutex::new(None),
                    running_tools: Mutex::new(BTreeMap::new()),
                    line: MutationLine::new(admission.clone()),
                    gate: ManualGate::new(admission.clone(), options.drive),
                    hydrated: Mutex::new(hydrated),
                });
                (name, lane)
            })
            .collect::<BTreeMap<_, _>>();
        let main = lanes
            .get(&LaneId::new("main").expect("static lane"))
            .expect("restore checked main")
            .clone();
        let effects = Supervisor::new(self.spawner.clone());
        let events = crate::events::EventHub::new(
            Arc::new(effects.clone()),
            options.telemetry_context.clone(),
        );
        let owner = Arc::new(Owner {
            session: options.session,
            models: options.models,
            clock: self.clock.clone(),
            host: self.spawner.clone(),
            effects,
            ids: self.ids.clone(),
            context: match options.tool_context {
                Some(AgentHarnessToolContextSource::Static(value)) => {
                    ContextSource::Static(Arc::new(value))
                }
                Some(AgentHarnessToolContextSource::Provider(provider)) => {
                    ContextSource::Provider(provider)
                }
                None => ContextSource::Static(self.context.clone()),
            },
            prompt: options.system_prompt,
            summaries: self.summaries.clone(),
            converter: options.to_provider_messages,
            projectors: options.entry_projectors,
            telemetry: options.telemetry_context,
            hooks: HookRegistry::new(),
            events,
            settings_line: MutationLine::new(admission.clone()),
            admission,
            settings: Mutex::new(Settings {
                revision: SafeUnsigned::new(0).expect("zero"),
                resources: options.resources,
                stream_options: options.stream_options,
                retry: options.retry,
                run: RunSettings {
                    compaction: options.compaction,
                    steering_mode: options.steering_mode,
                    follow_up_mode: options.follow_up_mode,
                    tool_execution: options.tool_execution,
                },
            }),
            drive_mode: options.drive,
            close_started: std::sync::atomic::AtomicBool::new(false),
            closed: pi_ai_core::CancellationToken::new(),
            seed: LaneConfiguration {
                model: LaneModel {
                    provider: options.model.provider,
                    model_id: options.model.id,
                },
                thinking_level: options.thinking_level.unwrap_or(AgentThinkingLevel::Off),
                active_tool_names: options.active_tool_names.unwrap_or_else(|| {
                    options
                        .tools
                        .iter()
                        .map(|t| t.descriptor().name.clone())
                        .collect()
                }),
            },
            tools: Mutex::new(options.tools),
            lanes: Mutex::new(lanes),
            lease: Mutex::new(Some(lease)),
        });
        Ok(AgentHarnessCreate {
            harness: Arc::new(RuntimeHarness {
                main: RuntimeLane { owner, lane: main },
            }),
            suspended,
        })
    }
}
pub(crate) fn suspended(h: &HydratedLane) -> Option<SuspendedOperation> {
    let current = h.current.as_ref()?;
    let prompt = match &current.operation.intent {
        OperationIntent::Run(intent) => Some(
            intent
                .prompt_entry_ids
                .iter()
                .filter_map(|id| match h.entries.get(id) {
                    Some(Entry::Message(entry)) => Some(entry.message.clone()),
                    _ => None,
                })
                .collect(),
        ),
        _ => None,
    };
    let deferred = match &current.state {
        OperationState::Run(run) => match &run.phase {
            RunPhase::Deferred(DeferredPhase {
                deferred:
                    Deferred::Suspended {
                        source_entry_id, ..
                    },
                ..
            }) => match h.entries.get(source_entry_id) {
                Some(Entry::Message(entry)) => match &entry.message {
                    AgentMessage::BuiltIn(pi_ai_core::Message::Assistant(message)) => {
                        message.deferred.clone()
                    }
                    _ => None,
                },
                _ => None,
            },
            _ => None,
        },
        _ => None,
    };
    let aborting = match &current.state {
        OperationState::Run(run) => match &run.control {
            Control::CancelRequested(control) => {
                let messages = |ids: &[EntryId]| {
                    ids.iter()
                        .filter_map(|id| {
                            match h
                                .registers
                                .get(&(RegisterNamespace::PendingEntry, id.as_str().into()))
                            {
                                Some(Register::PendingEntry { value, .. }) => {
                                    match &value.payload {
                                        pi_ai_core::Presence::Value(payload) => {
                                            serde_json::to_value(payload).ok().and_then(|value| {
                                                serde_json::from_value(value).ok()
                                            })
                                        }
                                        _ => None,
                                    }
                                }
                                _ => None,
                            }
                        })
                        .collect()
                };
                Some(AbortingSnapshot {
                    steer: messages(&control.drained_steer),
                    follow_up: messages(&control.drained_follow_up),
                })
            }
            _ => None,
        },
        _ => None,
    };
    Some(SuspendedOperation {
        lane: current.operation.lane.clone(),
        operation_id: current.operation.operation_id.clone(),
        kind: match current.operation.intent {
            OperationIntent::Run(_) => OperationKind::Run,
            OperationIntent::Compaction(_) => OperationKind::Compaction,
            OperationIntent::Navigation(_) => OperationKind::Navigation,
        },
        reason: if deferred.is_some() {
            SuspendedOperationReason::Deferred
        } else {
            SuspendedOperationReason::Crash
        },
        started_at: current.operation.started_at,
        prompt,
        deferred,
        aborting,
        missing: MissingIdentities {
            models: vec![],
            tools: vec![],
        },
    })
}

impl LaneControl {
    pub(crate) fn announce(&self) {
        let previous = std::mem::replace(
            &mut *self.changed.lock().unwrap_or_else(|p| p.into_inner()),
            pi_ai_core::CancellationToken::new(),
        );
        previous.cancel();
    }
}

pub(crate) enum ContextSource<C> {
    Static(Arc<C>),
    Provider(Arc<dyn ToolContextProvider<C>>),
}
impl<C, S: RuntimeStorage, M: SessionMetadataRecord> Owner<C, S, M> {
    pub(crate) fn observe<T>(
        &self,
        result: Result<T, HarnessRuntimeError>,
    ) -> Result<T, HarnessRuntimeError> {
        match result {
            Err(HarnessRuntimeError::Fault(fault)) => {
                self.admission.seal_fault(fault);
                self.effects.retire();
                Err(self
                    .admission
                    .check()
                    .expect_err("fault retains first owner failure"))
            }
            other => other,
        }
    }
}
impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>
    Owner<C, S, M>
{
    pub(crate) fn retain<T: Send + 'static>(
        self: Arc<Self>,
        work: BoxFuture<'static, T>,
    ) -> BoxFuture<'static, Result<T, HarnessRuntimeError>> {
        Box::pin(async move {
            let observer = self.clone();
            let ownership = TaskOwnership {
                owner: self.clone(),
                completed: false,
                cancellable: false,
            };
            let result = crate::scheduler::retained(
                self.host.clone(),
                Box::pin(async move {
                    let result = crate::scheduler::catch_task(work).await;
                    ownership.complete();
                    observer.observe(result)
                }),
            )
            .await;
            match result {
                Ok(result) => result,
                Err(error) => self.observe(Err(error)),
            }
        })
    }
    pub(crate) fn retain_effect<T: Send + 'static>(
        self: Arc<Self>,
        work: BoxFuture<'static, T>,
    ) -> BoxFuture<'static, Result<T, HarnessRuntimeError>> {
        Box::pin(async move {
            let observer = self.clone();
            let ownership = TaskOwnership {
                owner: self.clone(),
                completed: false,
                cancellable: true,
            };
            let result = crate::scheduler::retained(
                Arc::new(self.effects.clone()),
                Box::pin(async move {
                    let result = crate::scheduler::catch_task(work).await;
                    ownership.complete();
                    observer.observe(result)
                }),
            )
            .await;
            match result {
                Ok(result) => result,
                Err(error) => {
                    self.admission.check()?;
                    self.observe(Err(error))
                }
            }
        })
    }
}

impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>
    Owner<C, S, M>
{
    /// Observe runtime failures in the host-retained task, before result delivery.
    /// The waiter is not responsible for faulting or retiring its owner.
    pub(crate) fn retain_runtime<T: Send + 'static>(
        self: Arc<Self>,
        work: BoxFuture<'static, Result<T, HarnessRuntimeError>>,
    ) -> BoxFuture<'static, Result<T, HarnessRuntimeError>> {
        Box::pin(async move {
            let observer = self.clone();
            self.retain(Box::pin(async move { observer.observe(work.await) }))
                .await?
        })
    }
}

struct TaskOwnership<C, S: RuntimeStorage, M: SessionMetadataRecord> {
    owner: Arc<Owner<C, S, M>>,
    completed: bool,
    cancellable: bool,
}
impl<C, S: RuntimeStorage, M: SessionMetadataRecord> Drop for TaskOwnership<C, S, M> {
    fn drop(&mut self) {
        if !self.completed && (!self.cancellable || self.owner.admission.check().is_ok()) {
            let _ = self.owner.fail("host dropped an admitted runtime task");
        }
    }
}

impl<C, S: RuntimeStorage, M: SessionMetadataRecord> TaskOwnership<C, S, M> {
    fn complete(mut self) {
        self.completed = true;
    }
}

pub(crate) fn validate_stream_options(
    options: &AgentHarnessStreamOptions,
    policy: &ProviderHeaderPolicy,
) -> Result<(), HarnessRuntimeError> {
    let value =
        serde_json::to_value(options).map_err(|_| fault("stream options are not representable"))?;
    let raw: RawAgentHarnessStreamOptions =
        serde_json::from_value(value).map_err(|_| fault("stream options are invalid"))?;
    raw.into_durable(policy)
        .map(|_| ())
        .map_err(|_| fault("stream options violate the Session header policy"))
}
