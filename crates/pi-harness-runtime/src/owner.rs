//! Concrete session owner with explicit first attachment and strict restore.
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
#[cfg(feature = "extensions")]
use std::collections::BTreeSet;
use std::{
    collections::BTreeMap,
    ops::Deref,
    sync::{Arc, Mutex},
};

#[cfg(feature = "extensions")]
use crate::extensions::tool_driver::{EchoRouteTiming, ReadyToolRoute, SingleToolOwner};

#[cfg(feature = "extensions")]
pub(crate) fn matches_ready_echo(local: &AgentTool, route: &ReadyToolRoute) -> bool {
    use pi_extension_protocol::activation::{AgentToolMode, ToolReplay};

    let [granted] = route.grant().tools() else {
        return false;
    };
    let selected = granted.descriptor();
    let mode = match selected.execution_mode {
        AgentToolMode::Sequential => ToolExecutionMode::Sequential,
        AgentToolMode::Parallel => ToolExecutionMode::Parallel,
    };
    let replay = match selected.replay {
        ToolReplay::Safe => pi_agent_schema::ToolReplay::Safe,
        ToolReplay::Never => pi_agent_schema::ToolReplay::Never,
    };
    selected.name == "echo"
        && local.name.as_str() == selected.name
        && local.label == selected.label
        && local.description == selected.description
        && local.constrained_sampling.is_none()
        && local.execution_mode == Some(mode)
        && local.replay == Some(replay)
        && serde_json::to_value(&local.parameters).ok().as_ref() == Some(&selected.parameters)
}

/// One process-local view of native handles and the selected Ready echo route.
/// Mutations of this value are serialized by the owner settings line.
pub(crate) struct ToolRegistry<C> {
    handles: Vec<AgentHarnessToolHandle<C>>,
    handle_names: Vec<pi_ai_core::ToolName>,
    handles_generation: Arc<()>,
    #[cfg(feature = "extensions")]
    echo: Option<RegisteredEcho<C>>,
    #[cfg(feature = "extensions")]
    echo_owner: Option<SingleToolOwner>,
    #[cfg(feature = "extensions")]
    next_epoch: u64,
}

pub(crate) enum InstalledToolOrigin<C> {
    Direct(std::marker::PhantomData<fn() -> C>),
    #[cfg(feature = "extensions")]
    Echo {
        declaration: Arc<AgentTool>,
        route: EchoRouteSnapshot<C>,
    },
}

impl<C> Clone for InstalledToolOrigin<C> {
    fn clone(&self) -> Self {
        match self {
            Self::Direct(marker) => Self::Direct(*marker),
            #[cfg(feature = "extensions")]
            Self::Echo { declaration, route } => Self::Echo {
                declaration: declaration.clone(),
                route: route.clone(),
            },
        }
    }
}

/// Displaced host-owned objects must be released after both registry and
/// settings-line guards. Their destructors may synchronously call back in.
pub(crate) struct ReplacedTools<C> {
    handles: Vec<AgentHarnessToolHandle<C>>,
    #[cfg(feature = "extensions")]
    echo: Option<RegisteredEcho<C>>,
}

impl<C> ReplacedTools<C> {
    pub(crate) fn release(self) {
        drop(self.handles);
        #[cfg(feature = "extensions")]
        drop(self.echo);
    }
}

impl<C> ToolRegistry<C> {
    /// Clone actual objects and installed origin under one registry boundary;
    /// descriptor/context/prepare callbacks run only after this snapshot exists.
    pub(crate) fn selection_snapshot(
        &self,
    ) -> Vec<(AgentHarnessToolHandle<C>, InstalledToolOrigin<C>)> {
        self.handles
            .iter()
            .map(|handle| {
                #[cfg(feature = "extensions")]
                let origin = self.echo_snapshot(handle).map_or(
                    InstalledToolOrigin::Direct(std::marker::PhantomData),
                    |route| InstalledToolOrigin::Echo {
                        declaration: Arc::new(
                            self.echo
                                .as_ref()
                                .expect("snapshot registration")
                                .descriptor
                                .clone(),
                        ),
                        route,
                    },
                );
                #[cfg(not(feature = "extensions"))]
                let origin = InstalledToolOrigin::Direct(std::marker::PhantomData);
                (handle.clone(), origin)
            })
            .collect()
    }
    fn new(handles: Vec<AgentHarnessToolHandle<C>>, descriptors: Vec<AgentTool>) -> Self {
        debug_assert_eq!(handles.len(), descriptors.len());
        Self {
            handles,
            handle_names: descriptors
                .into_iter()
                .map(|descriptor| descriptor.name)
                .collect(),
            handles_generation: Arc::new(()),
            #[cfg(feature = "extensions")]
            echo: None,
            #[cfg(feature = "extensions")]
            echo_owner: None,
            #[cfg(feature = "extensions")]
            next_epoch: 1,
        }
    }

    /// Under settings_line, drain a removed route before publishing new handles.
    pub(crate) fn replace_handles(
        &mut self,
        handles: Vec<AgentHarnessToolHandle<C>>,
        descriptors: Vec<AgentTool>,
    ) -> ReplacedTools<C> {
        debug_assert_eq!(handles.len(), descriptors.len());
        #[cfg(feature = "extensions")]
        let retired_echo = if let Some(registered) = self.echo.as_ref()
            && (descriptors
                .iter()
                .filter(|descriptor| descriptor.name.as_str() == "echo")
                .count()
                != 1
                || !handles
                    .iter()
                    .zip(&descriptors)
                    .any(|(handle, descriptor)| {
                        Arc::ptr_eq(handle, &registered.handle)
                            && *descriptor == registered.descriptor
                    })) {
            let retired = self.echo.take();
            if let Some(registered) = &retired {
                registered.route.stop_new_calls();
            }
            retired
        } else {
            None
        };
        self.handles_generation = Arc::new(());
        self.handle_names = descriptors
            .into_iter()
            .map(|descriptor| descriptor.name)
            .collect();
        ReplacedTools {
            handles: std::mem::replace(&mut self.handles, handles),
            #[cfg(feature = "extensions")]
            echo: retired_echo,
        }
    }

    #[cfg(feature = "extensions")]
    pub(crate) fn register_echo(
        &mut self,
        handle: &AgentHarnessToolHandle<C>,
        descriptor: AgentTool,
        route: &Arc<ReadyToolRoute>,
        timing: EchoRouteTiming,
    ) -> Result<ReplacedTools<C>, crate::extensions::tool_driver::DriverError> {
        use crate::extensions::tool_driver::DriverError;

        if !matches_ready_echo(&descriptor, route)
            || !route.has_echo_domain_projections()
            || !route.is_current()
            || self
                .echo
                .as_ref()
                .is_some_and(|registered| Arc::ptr_eq(&registered.route, route))
            || self
                .echo_owner
                .as_ref()
                .is_some_and(|owner| !owner.same_owner(route.owner()))
        {
            return Err(DriverError::InvalidHost);
        }
        let next_epoch = self
            .next_epoch
            .checked_add(1)
            .ok_or(DriverError::Overloaded)?;
        route.bind_timing(timing)?;
        let retired_echo = self.echo.take();
        if let Some(previous) = &retired_echo {
            previous.route.stop_new_calls();
        }
        let mut handles = Vec::with_capacity(self.handles.len() + 1);
        let mut handle_names = Vec::with_capacity(self.handle_names.len() + 1);
        let mut inserted = false;
        for (current, current_name) in self.handles.iter().zip(&self.handle_names) {
            if current_name.as_str() == "echo" {
                if !inserted {
                    handles.push(handle.clone());
                    handle_names.push(descriptor.name.clone());
                    inserted = true;
                }
            } else {
                handles.push(current.clone());
                handle_names.push(current_name.clone());
            }
        }
        if !inserted {
            handles.push(handle.clone());
            handle_names.push(descriptor.name.clone());
        }
        self.handle_names = handle_names;
        self.handles_generation = Arc::new(());
        self.echo_owner.get_or_insert_with(|| route.owner().clone());
        self.echo = Some(RegisteredEcho {
            handle: handle.clone(),
            descriptor,
            generation: route.generation(),
            epoch: self.next_epoch,
            route: route.clone(),
            timing,
        });
        self.next_epoch = next_epoch;
        Ok(ReplacedTools {
            handles: std::mem::replace(&mut self.handles, handles),
            echo: retired_echo,
        })
    }

    #[cfg(feature = "extensions")]
    pub(crate) fn echo_snapshot(
        &self,
        handle: &AgentHarnessToolHandle<C>,
    ) -> Option<EchoRouteSnapshot<C>> {
        let registered = self.echo.as_ref()?;
        Arc::ptr_eq(handle, &registered.handle).then(|| EchoRouteSnapshot {
            handle: registered.handle.clone(),
            route: registered.route.clone(),
            generation: registered.generation,
            epoch: registered.epoch,
            timing: registered.timing,
        })
    }

    #[cfg(feature = "extensions")]
    pub(crate) fn has_recovered_echo_replacement(&self, original: &AgentTool) -> bool {
        self.echo.as_ref().is_some_and(|registered| {
            registered.descriptor == *original
                && matches_ready_echo(&registered.descriptor, &registered.route)
                && registered.route.has_echo_domain_projections()
                && registered.route.is_current()
        })
    }

    #[cfg(feature = "extensions")]
    pub(crate) fn recovered_echo_snapshot(
        &self,
        original: &AgentTool,
    ) -> Option<EchoRouteSnapshot<C>> {
        let registered = self.echo.as_ref()?;
        self.has_recovered_echo_replacement(original)
            .then(|| EchoRouteSnapshot {
                handle: registered.handle.clone(),
                route: registered.route.clone(),
                generation: registered.generation,
                epoch: registered.epoch,
                timing: registered.timing,
            })
    }

    #[cfg(feature = "extensions")]
    pub(crate) fn echo_is_current(&self, snapshot: &EchoRouteSnapshot<C>) -> bool {
        self.echo.as_ref().is_some_and(|current| {
            current.epoch == snapshot.epoch
                && current.generation == snapshot.generation
                && Arc::ptr_eq(&current.handle, &snapshot.handle)
                && Arc::ptr_eq(&current.route, &snapshot.route)
                && current.timing == snapshot.timing
                && current.route.is_current()
        })
    }

    /// Monotonic route registration generation for callback-free snapshot checks.
    pub(crate) fn route_epoch(&self) -> u64 {
        #[cfg(feature = "extensions")]
        {
            self.next_epoch
        }
        #[cfg(not(feature = "extensions"))]
        {
            0
        }
    }

    pub(crate) fn handles_generation(&self) -> Arc<()> {
        self.handles_generation.clone()
    }

    #[cfg(feature = "extensions")]
    pub(crate) fn has_ready_echo(&self) -> bool {
        self.echo
            .as_ref()
            .is_some_and(|selected| selected.route.is_current())
    }
}

impl<C> Deref for ToolRegistry<C> {
    type Target = Vec<AgentHarnessToolHandle<C>>;
    fn deref(&self) -> &Self::Target {
        &self.handles
    }
}

#[cfg(feature = "extensions")]
struct RegisteredEcho<C> {
    handle: AgentHarnessToolHandle<C>,
    descriptor: AgentTool,
    route: Arc<ReadyToolRoute>,
    generation: u64,
    epoch: u64,
    timing: EchoRouteTiming,
}

#[cfg(feature = "extensions")]
#[derive(Clone)]
pub(crate) struct RecoveredEchoIntent {
    pub(crate) original: AgentTool,
    pub(crate) arguments: crate::tools::CapturedArguments,
}

/// A trusted recovered echo intent could not be registered for this lane.
#[cfg(feature = "extensions")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveredEchoRegistrationError {
    /// A drive currently owns the lane.
    LaneBusy,
    /// The complete pending effect key is no longer current.
    NotCurrent,
    /// The supplied original declaration is outside the supported echo shape.
    InvalidOriginal,
    /// This key already carries a different original declaration.
    ConflictingIntent,
}

/// Candidate only: the later execution wrapper must recheck this under
/// settings_line and claim in the driver before releasing that line.
#[cfg(feature = "extensions")]
pub(crate) struct EchoRouteSnapshot<C> {
    pub(crate) handle: AgentHarnessToolHandle<C>,
    pub(crate) route: Arc<ReadyToolRoute>,
    pub(crate) generation: u64,
    pub(crate) epoch: u64,
    pub(crate) timing: EchoRouteTiming,
}

#[cfg(feature = "extensions")]
impl<C> Clone for EchoRouteSnapshot<C> {
    fn clone(&self) -> Self {
        Self {
            handle: self.handle.clone(),
            route: self.route.clone(),
            generation: self.generation,
            epoch: self.epoch,
            timing: self.timing,
        }
    }
}

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
    #[cfg(feature = "extensions")]
    pub profile_resources: Option<Arc<crate::extensions::profiles::resources::ResourceRevision>>,
    pub stream_options: AgentHarnessStreamOptions,
    pub retry: RetryPolicy,
    pub run: RunSettings,
}
pub(crate) struct LaneControl {
    pub name: LaneId,
    pub changed: Mutex<pi_ai_core::CancellationToken>,
    pub driving: Mutex<bool>,
    pub activity: Mutex<()>,
    pub generation: Mutex<Arc<()>>,
    pub active: Mutex<Option<(OperationId, std::sync::Weak<dyn crate::abort::ActiveDrive>)>>,
    pub restored: std::sync::atomic::AtomicBool,
    pub reservation: Mutex<Option<OperationId>>,
    pub streaming: Mutex<Option<(EntryId, pi_ai_core::AssistantMessage)>>,
    pub running_tools: Mutex<BTreeMap<EffectKey, RunningToolSnapshot>>,
    pub(crate) origin_leases: Mutex<Vec<std::sync::Weak<dyn crate::tools::OriginFence>>>,
    pub line: MutationLine,
    pub gate: ManualGate,
    pub hydrated: Mutex<HydratedLane>,
}
pub(crate) struct SuspensionObservation {
    pub operation_id: OperationId,
    pub summary: Option<PendingSummaryGeneration>,
    pub missing: MissingIdentities,
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
    #[cfg(feature = "extensions")]
    pub(crate) profile_providers: Mutex<
        BTreeMap<pi_ai_core::ProviderId, crate::extensions::profiles::provider::RegisteredProvider>,
    >,
    pub events: crate::events::EventHub,
    pub admission: Admission,
    pub settings_line: MutationLine,
    pub settings: Mutex<Settings>,
    pub tools: Mutex<ToolRegistry<C>>,
    pub(crate) tool_publications: Mutex<BTreeMap<EffectKey, Arc<crate::tools::ToolPublication<C>>>>,
    #[cfg(feature = "extensions")]
    pub(crate) routed_pending: Mutex<BTreeSet<EffectKey>>,
    #[cfg(feature = "extensions")]
    pub(crate) recovered_echo: Mutex<BTreeMap<EffectKey, RecoveredEchoIntent>>,
    pub lanes: Mutex<BTreeMap<LaneId, Arc<LaneControl>>>,
    pub suspensions: Mutex<BTreeMap<LaneId, SuspensionObservation>>,
    pub lease: Mutex<Option<RuntimeLease>>,
    pub seed: LaneConfiguration,
    pub drive_mode: DriveMode,
    pub close_started: std::sync::atomic::AtomicBool,
    pub closed: pi_ai_core::CancellationToken,
}
impl<C, S: RuntimeStorage, M: SessionMetadataRecord> Owner<C, S, M> {
    /// A preadmitted observation may finish through Fault, but an ordinary
    /// close that shuts down its backend retains the Closed read channel.
    pub(crate) fn observed_watch<T>(
        &self,
        result: Result<T, HarnessRuntimeError>,
    ) -> Result<T, HarnessRuntimeError> {
        if result.is_err()
            && let Err(error @ HarnessRuntimeError::Closed(_)) = self.admission.check()
        {
            return self.observed_unlocked(Err(error));
        }
        self.observed_unlocked(result)
    }
    pub(crate) fn observed_unlocked<T>(
        &self,
        result: Result<T, HarnessRuntimeError>,
    ) -> Result<T, HarnessRuntimeError> {
        let result = self.observe(result);
        self.events.flush();
        result
    }
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
        self.create_attached(options, false).await
    }
    /// Explicit caller-authorized first attachment. Only this concrete entry may
    /// initialize missing main configuration; the factory trait remains strict.
    pub async fn create_first_attachment(
        &self,
        options: AgentHarnessOptions<C, Telemetry, RuntimeSession<S, M>, ModelsRegistry>,
    ) -> Result<AgentHarnessCreate<RuntimeHarness<C, S, M>>, HarnessRuntimeError> {
        let factory = Self::new(
            self.clock.clone(),
            self.spawner.clone(),
            self.ids.clone(),
            self.context.clone(),
            self.summaries.clone(),
        );
        // The host task owns the sole lease through seed publication and complete
        // construction, even if the initiating waiter disappears while storage
        // has already admitted the seed. An unpolled call starts no host task.
        crate::scheduler::retained(
            self.spawner.clone(),
            Box::pin(crate::scheduler::catch_task(Box::pin(async move {
                factory.create_attached(options, true).await
            }))),
        )
        .await??
    }
    async fn create_attached(
        &self,
        options: AgentHarnessOptions<C, Telemetry, RuntimeSession<S, M>, ModelsRegistry>,
        first_attachment: bool,
    ) -> Result<AgentHarnessCreate<RuntimeHarness<C, S, M>>, HarnessRuntimeError> {
        validate_stream_options(
            &options.stream_options,
            options.session.codec().header_policy(),
        )?;
        // Capture each application getter once, outside locks. The same pure
        // declarations feed the seed and all later registry replacements.
        let tool_descriptors: Vec<AgentTool> = options
            .tools
            .iter()
            .map(|tool| tool.descriptor().clone())
            .collect();
        // Existing main configuration never replaces this future-lane seed.
        let seed = LaneConfiguration {
            model: LaneModel {
                provider: options.model.provider.clone(),
                model_id: options.model.id.clone(),
            },
            thinking_level: options.thinking_level.unwrap_or(AgentThinkingLevel::Off),
            active_tool_names: options.active_tool_names.clone().unwrap_or_else(|| {
                tool_descriptors
                    .iter()
                    .map(|tool| tool.name.clone())
                    .collect()
            }),
        };
        let admission = Admission::new();
        let lease = options
            .session
            .claim_runtime()
            .map_err(|_| fault("session already attached or unavailable"))?;
        if first_attachment {
            crate::restore::initialize_main(
                options.session.as_ref(),
                &seed,
                options.telemetry_context.as_ref(),
                &admission,
            )
            .await?;
        }
        let restored = restore_configured(options.session.as_ref(), &admission).await?;
        let suspended = restored.values().filter_map(suspended).collect();
        let lanes = restored
            .into_iter()
            .map(|(name, hydrated)| {
                let lane = Arc::new(LaneControl {
                    name: name.clone(),
                    changed: Mutex::new(pi_ai_core::CancellationToken::new()),
                    driving: Mutex::new(false),
                    activity: Mutex::new(()),
                    generation: Mutex::new(Arc::new(())),
                    active: Mutex::new(None),
                    restored: std::sync::atomic::AtomicBool::new(hydrated.current.is_some()),
                    reservation: Mutex::new(None),
                    streaming: Mutex::new(None),
                    running_tools: Mutex::new(BTreeMap::new()),
                    origin_leases: Mutex::new(Vec::new()),
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
            #[cfg(feature = "extensions")]
            profile_providers: Mutex::new(BTreeMap::new()),
            events,
            settings_line: MutationLine::new(admission.clone()),
            admission,
            settings: Mutex::new(Settings {
                revision: SafeUnsigned::new(0).expect("zero"),
                resources: options.resources,
                #[cfg(feature = "extensions")]
                profile_resources: None,
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
            seed,
            tools: Mutex::new(ToolRegistry::new(options.tools, tool_descriptors)),
            tool_publications: Mutex::new(BTreeMap::new()),
            #[cfg(feature = "extensions")]
            routed_pending: Mutex::new(BTreeSet::new()),
            #[cfg(feature = "extensions")]
            recovered_echo: Mutex::new(BTreeMap::new()),
            lanes: Mutex::new(lanes),
            suspensions: Mutex::new(BTreeMap::new()),
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
        _ if pi_harness_core::structural::is_cancelled(current) => Some(AbortingSnapshot {
            steer: vec![],
            follow_up: vec![],
        }),
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
    #[cfg(feature = "extensions")]
    pub(crate) fn recovered_intent_lane_busy(&self) -> bool {
        let _activity = self.activity.lock().unwrap_or_else(|p| p.into_inner());
        let active = self.active.lock().unwrap_or_else(|p| p.into_inner());
        let reservation = self.reservation.lock().unwrap_or_else(|p| p.into_inner());
        let driving = self.driving.lock().unwrap_or_else(|p| p.into_inner());
        active.is_some() || reservation.is_some() || *driving
    }

    pub(crate) fn reserve_drive(
        &self,
        operation: OperationId,
        drive: std::sync::Weak<dyn crate::abort::ActiveDrive>,
    ) -> bool {
        let _activity = self.activity.lock().unwrap_or_else(|p| p.into_inner());
        let mut active = self.active.lock().unwrap_or_else(|p| p.into_inner());
        let mut reservation = self.reservation.lock().unwrap_or_else(|p| p.into_inner());
        let mut driving = self.driving.lock().unwrap_or_else(|p| p.into_inner());
        if *driving {
            return false;
        }
        *self.generation.lock().unwrap_or_else(|p| p.into_inner()) = Arc::new(());
        *active = Some((operation.clone(), drive));
        *reservation = Some(operation);
        *driving = true;
        true
    }
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
                self.events.record_fault();
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
                    let result = observer.observe(result);
                    observer.events.flush();
                    result
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
                    let result = observer.observe(result);
                    observer.events.flush();
                    result
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

impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>
    RuntimeLane<C, S, M>
{
    pub(crate) fn suspended_snapshot(&self, h: &HydratedLane) -> Option<SuspendedOperation> {
        let mut snapshot = suspended(h)?;
        if let Some(observation) = self
            .owner
            .suspensions
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&self.lane.name)
            && observation.operation_id == snapshot.operation_id
        {
            snapshot.reason = SuspendedOperationReason::MissingIdentities;
            snapshot.missing = observation.missing.clone();
        }
        Some(snapshot)
    }
}
