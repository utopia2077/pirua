//! Harness-wide settings and public lane ownership.
use crate::owner::*;
use pi_agent_schema::*;
use pi_agent_session::storage::RuntimeStorage;
use std::sync::Arc;
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
            let _line = self.main.owner.settings_line.acquire().await?;
            let old = std::mem::replace(
                &mut *self
                    .main
                    .owner
                    .tools
                    .lock()
                    .unwrap_or_else(|p| p.into_inner()),
                tools,
            );
            drop(old);
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
        Box::pin(self.change_settings(move |s| s.resources = resources))
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
            self.main.owner.observe(validate_stream_options(
                &options,
                self.main.owner.session.codec().header_policy(),
            ))?;
            self.change_settings(move |s| s.stream_options = options)
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
        Box::pin(self.change_settings(move |s| s.retry = policy))
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
        Box::pin(self.change_settings(move |s| s.run.compaction = settings))
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
        Box::pin(self.change_settings(move |s| s.run.steering_mode = mode))
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
        Box::pin(self.change_settings(move |s| s.run.follow_up_mode = mode))
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
        Ok(())
    }
    async fn list_lanes(&self) -> Result<Vec<LaneInfo>, HarnessRuntimeError> {
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
            let h = lane.load().await?;
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
                    status: if driving {
                        OperationSnapshotStatus::Running
                    } else {
                        OperationSnapshotStatus::Suspended
                    },
                }),
                suspended: if driving {
                    None
                } else {
                    crate::owner::suspended(&h)
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
                    reservation: std::sync::Mutex::new(None),
                    streaming: std::sync::Mutex::new(None),
                    running_tools: std::sync::Mutex::new(std::collections::BTreeMap::new()),
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
