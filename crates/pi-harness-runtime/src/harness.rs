//! Harness-wide settings and public lane ownership.
#[cfg(feature = "extensions")]
use crate::extensions::tool_driver::{DriverError, EchoRouteTiming, ReadyToolRoute};
use crate::owner::*;
use pi_agent_schema::*;
use pi_agent_session::storage::RuntimeStorage;
use std::sync::Arc;

#[cfg(feature = "extensions")]
pub(crate) fn recovered_echo_arguments(
    h: &crate::restore::HydratedLane,
    key: &EffectKey,
    original: &AgentTool,
) -> Result<Option<crate::tools::CapturedArguments>, HarnessRuntimeError> {
    let Some(current) = h.current.as_ref() else {
        return Ok(None);
    };
    let OperationState::Run(run) = &current.state else {
        return Ok(None);
    };
    if !matches!(
        run.control,
        Control::Running(_) | Control::CancelRequested(_)
    ) || current.operation.operation_id != key.operation_id
    {
        return Ok(None);
    }
    let RunPhase::Tools(tools) = &run.phase else {
        return Ok(None);
    };
    let Some(index) = key.item_index else {
        return Ok(None);
    };
    let expected = EffectKey {
        session_id: key.session_id.clone(),
        lane: key.lane.clone(),
        operation_id: current.operation.operation_id.clone(),
        kind: EffectKind::Tool,
        step_or_task_id: EffectStepId::new(tools.batch.turn_id.as_str())
            .map_err(|_| crate::restore::fault("recovered echo turn invalid"))?,
        logical_attempt: pi_ai_core::SafeUnsigned::new(0).expect("zero safe unsigned"),
        item_index: Some(index),
    };
    if expected != *key {
        return Ok(None);
    }
    if run.settings.tool_execution != ToolExecutionMode::Sequential {
        return Err(crate::restore::fault(
            "recovered echo requires sequential execution",
        ));
    }
    let pending = tools.batch.calls.iter().find(|call| {
        matches!(call, ToolCallState::EffectPending { source_index, .. }
            if *source_index == index)
    });
    let Some(ToolCallState::EffectPending {
        replay: ToolReplay::Safe,
        ..
    }) = pending
    else {
        return if pending.is_some() {
            Err(crate::restore::fault("recovered echo replay changed"))
        } else {
            Ok(None)
        };
    };
    let Some(Entry::Message(entry)) = h.entries.get(&tools.batch.assistant_entry_id) else {
        return Err(crate::restore::fault("recovered echo source missing"));
    };
    let AgentMessage::BuiltIn(pi_ai_core::Message::Assistant(assistant)) = &entry.message else {
        return Err(crate::restore::fault("recovered echo source invalid"));
    };
    let Some(pi_ai_core::AssistantContent::ToolCall(call)) =
        assistant.content.get(index.get() as usize)
    else {
        return Err(crate::restore::fault("recovered echo call missing"));
    };
    if call.name != original.name {
        return Err(crate::restore::fault("recovered echo source name changed"));
    }
    let args_key = pi_harness_core::tool_batch::args_key(current, &tools.batch, index);
    let Some(Register::OperationToolArguments {
        key: saved_key,
        value,
        seq,
    }) = h
        .registers
        .get(&(RegisterNamespace::OperationToolArguments, args_key.clone()))
    else {
        return Err(crate::restore::fault("recovered echo arguments missing"));
    };
    if *saved_key != args_key {
        return Err(crate::restore::fault(
            "recovered echo arguments key invalid",
        ));
    }
    Ok(Some(crate::tools::CapturedArguments {
        key: args_key,
        value: value.clone(),
        seq: *seq,
    }))
}

#[cfg(feature = "extensions")]
impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>
    RuntimeHarness<C, S, M>
{
    /// Register a trusted host's original declaration for one current Safe
    /// pending echo effect. The host owns recoverable provenance and native
    /// classification before invoking resume.
    pub async fn register_recovered_echo_pending(
        &self,
        key: EffectKey,
        original: AgentTool,
    ) -> Result<Result<(), RecoveredEchoRegistrationError>, HarnessRuntimeError> {
        use RecoveredEchoRegistrationError as Rejection;
        self.main.owner.admission.check()?;
        if original.name.as_str() != "echo"
            || original.execution_mode != Some(ToolExecutionMode::Sequential)
            || original.replay != Some(ToolReplay::Safe)
            || original.constrained_sampling.is_some()
        {
            return Ok(Err(Rejection::InvalidOriginal));
        }
        if key.session_id != self.main.owner.session.metadata().base_metadata().id {
            return Ok(Err(Rejection::NotCurrent));
        }
        let lane = self
            .main
            .owner
            .lanes
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&key.lane)
            .cloned();
        let Some(lane) = lane else {
            return Ok(Err(Rejection::NotCurrent));
        };
        let view = RuntimeLane {
            owner: self.main.owner.clone(),
            lane: lane.clone(),
        };
        let mut pair =
            crate::tools::PairedPermits::acquire(&self.main.owner.settings_line, &lane.line)
                .await?;
        let busy = lane.recovered_intent_lane_busy();
        if busy {
            pair.release();
            return Ok(Err(Rejection::LaneBusy));
        }
        let loaded = view.load_raw().await;
        let result = match loaded {
            Ok(h)
                if !h.current.as_ref().is_some_and(|current| {
                    matches!(&current.state,
                OperationState::Run(run) if matches!(run.control, Control::Running(_)))
                }) =>
            {
                Ok(Err(Rejection::NotCurrent))
            }
            Ok(h) => match recovered_echo_arguments(&h, &key, &original) {
                Ok(Some(arguments)) => {
                    let mut intents = self
                        .main
                        .owner
                        .recovered_echo
                        .lock()
                        .unwrap_or_else(|p| p.into_inner());
                    if let Some(existing) = intents.get(&key) {
                        if existing.original != original {
                            Ok(Err(Rejection::ConflictingIntent))
                        } else if existing.arguments.key != arguments.key
                            || existing.arguments.value != arguments.value
                            || existing.arguments.seq != arguments.seq
                        {
                            Err(crate::restore::fault("recovered echo arguments changed"))
                        } else {
                            Ok(Ok(()))
                        }
                    } else {
                        intents.insert(
                            key,
                            crate::owner::RecoveredEchoIntent {
                                original,
                                arguments,
                            },
                        );
                        Ok(Ok(()))
                    }
                }
                Ok(None) => Ok(Err(Rejection::NotCurrent)),
                Err(error) => Err(error),
            },
            Err(error) => Err(error),
        };
        pair.release();
        self.main.owner.observed_unlocked(result)
    }
    /// Installs the one host-approved Ready echo route after concrete owner
    /// creation. A later execution wrapper must claim under this settings line.
    pub async fn register_ready_echo_tool(
        &self,
        handle: AgentHarnessToolHandle<C>,
        route: Arc<ReadyToolRoute>,
        timing: EchoRouteTiming,
    ) -> Result<Result<(), DriverError>, HarnessRuntimeError> {
        if let Err(error) = route.check_timing(timing) {
            return Ok(Err(error));
        }
        let descriptor = handle.descriptor().clone();
        let line = self.main.owner.settings_line.acquire().await?;
        let result = self
            .main
            .owner
            .tools
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .register_echo(&handle, descriptor, &route, timing);
        match result {
            Ok(old) => {
                self.main.owner.events.enqueue(
                    HarnessEventType::ConfigUpdate,
                    &self.main.lane.name,
                    HarnessEvent::GlobalConfigUpdate(GlobalConfigUpdateEvent {
                        event_type: Default::default(),
                        property: GlobalConfigProperty::Tools,
                    }),
                );
                drop(line);
                old.release();
                self.main.owner.events.flush();
                Ok(Ok(()))
            }
            Err(error) => {
                drop(line);
                Ok(Err(error))
            }
        }
    }
}

impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>
    AgentHarness<C> for RuntimeHarness<C, S, M>
{
    type Lane = RuntimeLane<C, S, M>;
    fn lane(
        &self,
        name: LaneId,
    ) -> BoxFuture<'_, Result<Option<Arc<Self::Lane>>, HarnessRuntimeError>> {
        Box::pin(async move {
            self.main.owner.admission.check()?;
            Ok(self
                .main
                .owner
                .lanes
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .get(&name)
                .cloned()
                .map(|lane| {
                    Arc::new(RuntimeLane {
                        owner: self.main.owner.clone(),
                        lane,
                    })
                }))
        })
    }
    fn create_lane(
        &self,
        name: LaneId,
        at: Option<EntryId>,
    ) -> BoxFuture<'_, CreateLaneCall<Self::Lane>> {
        Box::pin(self.add_lane(name, at))
    }
    fn lanes(&self) -> BoxFuture<'_, Result<Vec<LaneInfo>, HarnessRuntimeError>> {
        Box::pin(self.list_lanes())
    }
    fn get_tools(
        &self,
    ) -> BoxFuture<'_, Result<Vec<AgentHarnessToolHandle<C>>, HarnessRuntimeError>> {
        Box::pin(async move {
            self.main.owner.admission.check()?;
            Ok(self
                .main
                .owner
                .tools
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .clone())
        })
    }
    fn set_tools(
        &self,
        tools: Vec<AgentHarnessToolHandle<C>>,
    ) -> BoxFuture<'_, Result<(), HarnessRuntimeError>> {
        Box::pin(async move {
            self.main.owner.admission.check()?;
            let descriptors = tools.iter().map(|tool| tool.descriptor().clone()).collect();
            let _line = self.main.owner.settings_line.acquire().await?;
            let old = self
                .main
                .owner
                .tools
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .replace_handles(tools, descriptors);
            self.main.owner.events.enqueue(
                HarnessEventType::ConfigUpdate,
                &self.main.lane.name,
                HarnessEvent::GlobalConfigUpdate(GlobalConfigUpdateEvent {
                    event_type: Default::default(),
                    property: GlobalConfigProperty::Tools,
                }),
            );
            drop(_line);
            old.release();
            self.main.owner.events.flush();
            Ok(())
        })
    }
    fn get_resources(&self) -> BoxFuture<'_, Result<Resources, HarnessRuntimeError>> {
        Box::pin(async move {
            self.main.owner.admission.check()?;
            Ok(self
                .main
                .owner
                .settings
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .resources
                .clone())
        })
    }
    fn set_resources(
        &self,
        resources: Resources,
    ) -> BoxFuture<'_, Result<(), HarnessRuntimeError>> {
        Box::pin(
            self.change_settings(GlobalConfigProperty::Resources, move |s| {
                s.resources = resources
            }),
        )
    }
    fn get_stream_options(
        &self,
    ) -> BoxFuture<'_, Result<AgentHarnessStreamOptions, HarnessRuntimeError>> {
        Box::pin(async move {
            self.main.owner.admission.check()?;
            Ok(self
                .main
                .owner
                .settings
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .stream_options
                .clone())
        })
    }
    fn set_stream_options(
        &self,
        options: AgentHarnessStreamOptions,
    ) -> BoxFuture<'_, Result<(), HarnessRuntimeError>> {
        Box::pin(async move {
            self.main.owner.admission.check()?;
            self.main.owner.observed_unlocked(validate_stream_options(
                &options,
                self.main.owner.session.codec().header_policy(),
            ))?;
            self.change_settings(GlobalConfigProperty::StreamOptions, move |s| {
                s.stream_options = options
            })
            .await
        })
    }
    fn get_retry_policy(&self) -> BoxFuture<'_, Result<RetryPolicy, HarnessRuntimeError>> {
        Box::pin(async move {
            self.main.owner.admission.check()?;
            Ok(self
                .main
                .owner
                .settings
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .retry
                .clone())
        })
    }
    fn set_retry_policy(
        &self,
        policy: RetryPolicy,
    ) -> BoxFuture<'_, Result<(), HarnessRuntimeError>> {
        Box::pin(self.change_settings(GlobalConfigProperty::RetryPolicy, move |s| s.retry = policy))
    }
    fn get_compaction_settings(
        &self,
    ) -> BoxFuture<'_, Result<CompactionSettings, HarnessRuntimeError>> {
        Box::pin(async move {
            self.main.owner.admission.check()?;
            Ok(self
                .main
                .owner
                .settings
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .run
                .compaction
                .clone())
        })
    }
    fn set_compaction_settings(
        &self,
        settings: CompactionSettings,
    ) -> BoxFuture<'_, Result<(), HarnessRuntimeError>> {
        Box::pin(
            self.change_settings(GlobalConfigProperty::CompactionSettings, move |s| {
                s.run.compaction = settings
            }),
        )
    }
    fn get_steering_mode(&self) -> BoxFuture<'_, Result<QueueMode, HarnessRuntimeError>> {
        Box::pin(async move {
            self.main.owner.admission.check()?;
            Ok(self
                .main
                .owner
                .settings
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .run
                .steering_mode)
        })
    }
    fn set_steering_mode(&self, mode: QueueMode) -> BoxFuture<'_, Result<(), HarnessRuntimeError>> {
        Box::pin(
            self.change_settings(GlobalConfigProperty::SteeringMode, move |s| {
                s.run.steering_mode = mode
            }),
        )
    }
    fn get_follow_up_mode(&self) -> BoxFuture<'_, Result<QueueMode, HarnessRuntimeError>> {
        Box::pin(async move {
            self.main.owner.admission.check()?;
            Ok(self
                .main
                .owner
                .settings
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .run
                .follow_up_mode)
        })
    }
    fn set_follow_up_mode(
        &self,
        mode: QueueMode,
    ) -> BoxFuture<'_, Result<(), HarnessRuntimeError>> {
        Box::pin(
            self.change_settings(GlobalConfigProperty::FollowUpMode, move |s| {
                s.run.follow_up_mode = mode
            }),
        )
    }
    fn watch_session(
        &self,
    ) -> BoxFuture<'_, Result<Arc<dyn WatchHandle<SessionSnapshot>>, HarnessRuntimeError>> {
        Box::pin(self.session_watch())
    }
    fn hooks(&self) -> &dyn Hooks {
        &self.main.owner.hooks
    }
    fn events(&self) -> &dyn Events {
        &self.main.owner.events
    }
    fn close(&self) -> BoxFuture<'_, Result<(), HarnessRuntimeError>> {
        Box::pin(self.close_owner())
    }
}
impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>
    RuntimeHarness<C, S, M>
{
    async fn change_settings<F: FnOnce(&mut Settings) + Send>(
        &self,
        property: GlobalConfigProperty,
        change: F,
    ) -> Result<(), HarnessRuntimeError> {
        let _line = self.main.owner.settings_line.acquire().await?;
        let mut next = self
            .main
            .owner
            .settings
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        next.revision = pi_ai_core::SafeUnsigned::new(
            next.revision
                .get()
                .checked_add(1)
                .ok_or_else(|| self.main.owner.fail("settings revision exhausted"))?,
        )
        .map_err(|_| self.main.owner.fail("settings revision exhausted"))?;
        change(&mut next);
        let previous = std::mem::replace(
            &mut *self
                .main
                .owner
                .settings
                .lock()
                .unwrap_or_else(|p| p.into_inner()),
            next,
        );
        drop(previous);
        self.main.owner.events.enqueue(
            HarnessEventType::ConfigUpdate,
            &self.main.lane.name,
            HarnessEvent::GlobalConfigUpdate(GlobalConfigUpdateEvent {
                event_type: Default::default(),
                property,
            }),
        );
        drop(_line);
        self.main.owner.events.flush();
        Ok(())
    }
    #[cfg(feature = "extensions")]
    pub(crate) async fn install_profile_resources(
        &self,
        revision: Arc<crate::extensions::profiles::resources::ResourceRevision>,
    ) -> Result<(), HarnessRuntimeError> {
        self.change_settings(GlobalConfigProperty::Resources, move |settings| {
            settings.resources = revision.resources();
            settings.profile_resources = Some(revision);
        })
        .await
    }
    async fn list_lanes(&self) -> Result<Vec<LaneInfo>, HarnessRuntimeError> {
        let result = self.list_lanes_inner().await;
        self.main.owner.observed_unlocked(result)
    }
    async fn list_lanes_inner(&self) -> Result<Vec<LaneInfo>, HarnessRuntimeError> {
        let _settings = self.main.owner.settings_line.acquire().await?;
        let lanes: Vec<_> = self
            .main
            .owner
            .lanes
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .values()
            .cloned()
            .collect();
        let mut infos = vec![];
        for control in lanes {
            let lane = RuntimeLane {
                owner: self.main.owner.clone(),
                lane: control,
            };
            let _line = lane.lane.line.acquire().await?;
            let h = lane.load().await?;
            let Register::LaneLeaf { value: leaf_id, .. } = h.roots.leaf else {
                return Err(self.main.owner.fail("invalid lane leaf"));
            };
            infos.push(LaneInfo {
                name: lane.lane.name.clone(),
                leaf_id,
                operation: h.current.as_ref().map(|c| LaneOperationInfo {
                    id: c.operation.operation_id.clone(),
                    kind: crate::snapshots::kind(c),
                    status: if *lane.lane.driving.lock().unwrap_or_else(|p| p.into_inner()) {
                        OperationSnapshotStatus::Running
                    } else {
                        OperationSnapshotStatus::Suspended
                    },
                }),
            });
        }
        Ok(infos)
    }
    async fn session_watch(
        &self,
    ) -> Result<Arc<dyn WatchHandle<SessionSnapshot>>, HarnessRuntimeError> {
        let result = self.session_watch_inner().await;
        self.main.owner.observed_watch(result)
    }
    async fn session_watch_inner(
        &self,
    ) -> Result<Arc<dyn WatchHandle<SessionSnapshot>>, HarnessRuntimeError> {
        let _settings = self.main.owner.settings_line.acquire().await?;
        let controls: Vec<_> = self
            .main
            .owner
            .lanes
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .values()
            .cloned()
            .collect();
        let mut permits = Vec::new();
        for control in &controls {
            permits.push(control.line.acquire().await?);
        }
        let mut lanes = vec![];
        for control in controls {
            let lane = RuntimeLane {
                owner: self.main.owner.clone(),
                lane: control,
            };
            let h = crate::restore::hydrate_lane(
                lane.owner.session.as_ref(),
                lane.lane.name.clone(),
                &crate::scheduler::Admission::new(),
            )
            .await?;
            let Register::LaneLeaf { value: leaf_id, .. } = h.roots.leaf.clone() else {
                return Err(self.main.owner.fail("invalid snapshot leaf"));
            };
            let driving = *lane.lane.driving.lock().unwrap_or_else(|p| p.into_inner());
            lanes.push(SessionLaneSnapshot {
                name: lane.lane.name.clone(),
                leaf_id,
                operation: h.current.as_ref().map(|c| LaneOperationInfo {
                    id: c.operation.operation_id.clone(),
                    kind: crate::snapshots::kind(c),
                    status: if pi_harness_core::structural::is_cancelled(c) {
                        OperationSnapshotStatus::Aborting
                    } else if driving {
                        OperationSnapshotStatus::Running
                    } else {
                        OperationSnapshotStatus::Suspended
                    },
                }),
                suspended: if driving {
                    None
                } else {
                    lane.suspended_snapshot(&h)
                },
            });
        }
        Ok(self.main.owner.events.watch(
            SessionSnapshot {
                lanes,
                faulted: false,
            },
            None,
        ))
    }
    async fn close_owner(&self) -> Result<(), HarnessRuntimeError> {
        let owner = self.main.owner.clone();
        if !owner
            .close_started
            .swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            owner.admission.seal_close();
            owner.effects.retire();
            let work = owner.clone();
            let mut completion = CloseGuard {
                owner: owner.clone(),
                completed: false,
            };
            if owner
                .host
                .spawn(Box::pin(async move {
                    work.admission.drained().await;
                    drop(crate::tools::retire_closed_publications(work.as_ref()));
                    if work.session.close().await.is_err() {
                        let _ = work.fail("runtime Session close failed");
                    }
                    let lease = work.lease.lock().unwrap_or_else(|p| p.into_inner()).take();
                    drop(lease);
                    completion.completed = true;
                    drop(completion);
                }))
                .is_err()
            {
                let _ = owner.fail("runtime close task admission failed");
                owner.closed.cancel();
            }
        }
        owner.closed.cancelled().await;
        match owner.admission.check() {
            Err(HarnessRuntimeError::Fault(f)) => Err(HarnessRuntimeError::Fault(f)),
            _ => Ok(()),
        }
    }
}
impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>
    RuntimeHarness<C, S, M>
{
    async fn add_lane(
        &self,
        name: LaneId,
        at: Option<EntryId>,
    ) -> CreateLaneCall<RuntimeLane<C, S, M>> {
        if let Err(error) = self.main.owner.admission.check() {
            return match error {
                HarnessRuntimeError::Closed(_) => {
                    Ok(Err(CreateLaneExpectedError::Closed(ClosedError {
                        tag: Default::default(),
                        message: "Harness is closed".into(),
                    })))
                }
                error => Err(error),
            };
        }

        let owner = self.main.owner.clone();
        owner
            .clone()
            .retain_runtime(Box::pin(async move {
                let _settings = owner.settings_line.acquire().await?;
                if owner
                    .session
                    .get_register(RegisterNamespace::LaneState, name.as_str())
                    .await
                    .map_err(|_| owner.fail("lane existence read failed"))?
                    .is_some()
                {
                    return Ok(Err(CreateLaneExpectedError::LaneExists(LaneExistsError {
                        tag: Default::default(),
                        message: "lane already exists".into(),
                        lane: name,
                    })));
                }
                if let Some(id) = &at
                    && !owner
                        .session
                        .get_entries(std::slice::from_ref(id))
                        .await
                        .map_err(|_| owner.fail("lane anchor read failed"))?
                        .contains_key(id)
                {
                    return Ok(Err(CreateLaneExpectedError::UnknownTarget(
                        UnknownTargetError {
                            tag: Default::default(),
                            message: "lane anchor does not exist".into(),
                            target_id: id.clone(),
                        },
                    )));
                }
                let permit = owner.admission.try_admit()?;
                let conditions = [
                    RegisterNamespace::LaneConfig,
                    RegisterNamespace::LaneLeaf,
                    RegisterNamespace::LaneState,
                ]
                .into_iter()
                .map(|namespace| pi_agent_session::storage::RegisterCondition {
                    register: pi_agent_session::storage::RegisterKey {
                        namespace,
                        key: name.as_str().into(),
                    },
                    expected: pi_agent_session::storage::ExpectedRegister::Absent,
                })
                .collect();
                let result = owner
                    .session
                    .commit_conditional_with_telemetry(
                        pi_agent_session::storage::ConditionalCommit {
                            conditions,
                            transaction: Transaction {
                                writes: vec![
                                    Write::SetRegister(Box::new(RegisterSet::LaneConfig {
                                        key: name.as_str().into(),
                                        value: owner.seed.clone(),
                                    })),
                                    Write::SetRegister(Box::new(RegisterSet::LaneLeaf {
                                        key: name.as_str().into(),
                                        value: at.clone(),
                                    })),
                                    Write::SetRegister(Box::new(RegisterSet::LaneState {
                                        key: name.as_str().into(),
                                        value: LaneState {
                                            current_operation_id: None,
                                            pending_next_run: vec![],
                                        },
                                    })),
                                ],
                            },
                        },
                        owner.telemetry.as_ref(),
                    )
                    .await
                    .map_err(|_| owner.fail("lane creation publication failed"))?;
                if matches!(
                    result,
                    pi_agent_session::storage::ConditionalOutcome::NotApplied
                ) {
                    return Ok(Err(CreateLaneExpectedError::LaneExists(LaneExistsError {
                        tag: Default::default(),
                        message: "lane already exists".into(),
                        lane: name,
                    })));
                }
                let hydrated = crate::restore::hydrate_lane(
                    owner.session.as_ref(),
                    name.clone(),
                    &crate::scheduler::Admission::new(),
                )
                .await?;
                let control = Arc::new(LaneControl {
                    name: name.clone(),
                    changed: std::sync::Mutex::new(pi_ai_core::CancellationToken::new()),
                    driving: std::sync::Mutex::new(false),
                    activity: std::sync::Mutex::new(()),
                    generation: std::sync::Mutex::new(Arc::new(())),
                    active: std::sync::Mutex::new(None),
                    restored: std::sync::atomic::AtomicBool::new(false),
                    reservation: std::sync::Mutex::new(None),
                    streaming: std::sync::Mutex::new(None),
                    running_tools: std::sync::Mutex::new(std::collections::BTreeMap::new()),
                    origin_leases: std::sync::Mutex::new(Vec::new()),
                    line: crate::scheduler::MutationLine::new(owner.admission.clone()),
                    gate: crate::manual_gate::ManualGate::new(
                        owner.admission.clone(),
                        owner.drive_mode,
                    ),
                    hydrated: std::sync::Mutex::new(hydrated),
                });
                let old = owner
                    .lanes
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .insert(name.clone(), control.clone());
                drop(old);
                owner.events.enqueue(
                    HarnessEventType::LaneCreated,
                    &name,
                    HarnessEvent::LaneCreated(LaneCreatedEvent {
                        event_type: Default::default(),
                        lane: name.clone(),
                        recovery: None,
                        at,
                    }),
                );
                drop(permit);
                drop(_settings);
                owner.events.flush();
                Ok(Ok(Arc::new(RuntimeLane {
                    owner: owner.clone(),
                    lane: control,
                })))
            }))
            .await
    }
}
impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>
    AgentHarnessFactory<C, Telemetry> for RuntimeFactory<C, S, M>
{
    type Session = pi_agent_session::session::RuntimeSession<S, M>;
    type Models = pi_ai_core::ModelsRegistry;
    type Harness = RuntimeHarness<C, S, M>;
    fn create(
        &self,
        options: AgentHarnessOptions<C, Telemetry, Self::Session, Self::Models>,
    ) -> BoxFuture<'_, Result<AgentHarnessCreate<Self::Harness>, HarnessRuntimeError>> {
        Box::pin(self.create_configured(options))
    }
}
struct CloseGuard<C, S: RuntimeStorage, M: SessionMetadataRecord> {
    owner: Arc<Owner<C, S, M>>,
    completed: bool,
}
impl<C, S: RuntimeStorage, M: SessionMetadataRecord> Drop for CloseGuard<C, S, M> {
    fn drop(&mut self) {
        if !self.completed {
            let _ = self.owner.fail("host dropped the admitted close task");
        }
        self.owner.closed.cancel();
    }
}
