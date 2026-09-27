//! Shared actual drive cancellation, durable marker and background reconciliation.
use crate::{effects::RuntimeEffects, owner::*, restore::HydratedLane};
use pi_agent_schema::*;
use pi_agent_session::storage::RuntimeStorage;
use pi_ai_core::SafeUnsigned;
use std::{sync::Arc, task::Poll};
pub(crate) trait ActiveDrive: Send + Sync {
    fn kind(&self) -> OperationKind {
        OperationKind::Run
    }
    #[cfg(feature = "extensions")]
    fn profile_actions(
        self: Arc<Self>,
        _key: &pi_extension_protocol::wire::v1_1::EffectKey,
        _registry: pi_extension_protocol::profiles::Registry,
        _max_bytes: usize,
    ) -> Option<Arc<dyn crate::extensions::profiles::connection::HostActions>> {
        None
    }
    fn register_cancel(&self, h: &HydratedLane) -> Result<(), HarnessRuntimeError>;
    fn abort(
        self: Arc<Self>,
        h: HydratedLane,
    ) -> BoxFuture<'static, Result<(), HarnessRuntimeError>>;
}
impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord> ActiveDrive
    for RuntimeEffects<C, S, M>
{
    fn kind(&self) -> OperationKind {
        *self
            .operation_kind
            .lock()
            .unwrap_or_else(|p| p.into_inner())
    }
    #[cfg(feature = "extensions")]
    fn profile_actions(
        self: Arc<Self>,
        wire: &pi_extension_protocol::wire::v1_1::EffectKey,
        registry: pi_extension_protocol::profiles::Registry,
        max_bytes: usize,
    ) -> Option<Arc<dyn crate::extensions::profiles::connection::HostActions>> {
        let key = self
            .running
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .keys()
            .find(|key| crate::extensions::profiles::connection::key_wire(key) == *wire)
            .cloned()?;
        Some(Arc::new(
            crate::extensions::profiles::actions::LaneActions {
                lane: self.lane.clone(),
                resources: self.profile_resources.clone(),
                registry,
                max_bytes,
                scope: crate::extensions::profiles::actions::scope(
                    &self,
                    key.operation_id.clone(),
                    Some(key),
                    true,
                ),
            },
        ))
    }
    fn register_cancel(&self, h: &HydratedLane) -> Result<(), HarnessRuntimeError> {
        self.register_cancel_target(h)
    }
    fn abort(
        self: Arc<Self>,
        h: HydratedLane,
    ) -> BoxFuture<'static, Result<(), HarnessRuntimeError>> {
        Box::pin(async move {
            self.schedule_cancel(&h).await?;
            self.cancellation.cancel();
            self.lane.lane.announce();
            Ok(())
        })
    }
}
impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>
    RuntimeEffects<C, S, M>
{
    pub(crate) fn register_drive(self: &Arc<Self>, operation: &OperationId) {
        let drive: Arc<dyn ActiveDrive> = self.clone();
        let previous = self
            .lane
            .lane
            .active
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .replace((operation.clone(), Arc::downgrade(&drive)));
        drop(previous);
    }
    pub(crate) fn release_drive(self: &Arc<Self>) {
        let this: Arc<dyn ActiveDrive> = self.clone();
        let own = Arc::downgrade(&this);
        let removed = {
            let _activity = self
                .lane
                .lane
                .activity
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            let mut active = self
                .lane
                .lane
                .active
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            if active.as_ref().is_none_or(|(_, drive)| !drive.ptr_eq(&own)) {
                return;
            }
            let removed = active.take();
            self.lane
                .lane
                .reservation
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .take();
            *self
                .lane
                .lane
                .driving
                .lock()
                .unwrap_or_else(|p| p.into_inner()) = false;
            removed
        };
        drop(removed);
    }
    pub(crate) async fn ordinary_permission(
        &self,
        kind: &str,
    ) -> Result<bool, HarnessRuntimeError> {
        let mut cancelled = Box::pin(self.cancellation.cancelled());
        let mut permission = Box::pin(self.permission(kind));
        std::future::poll_fn(|cx| {
            if cancelled.as_mut().poll(cx).is_ready() {
                return Poll::Ready(Ok(false));
            }
            permission.as_mut().poll(cx).map(|r| r.map(|_| true))
        })
        .await
    }
    pub(crate) async fn wait_cancellations(&self) -> Result<(), HarnessRuntimeError> {
        loop {
            let targets = self
                .cancel_targets
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .values()
                .map(|target| (target.plan.key.operation_id.clone(), target.done.clone()))
                .collect::<Vec<_>>();
            if targets.iter().all(|(_, done)| done.is_cancelled()) {
                return Ok(());
            }
            let changed = self
                .lane
                .lane
                .changed
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .clone();
            let h = {
                let _line = self.lane.lane.line.acquire().await?;
                self.lane.load().await?
            };
            if targets.iter().any(|(id, _)| {
                h.current
                    .as_ref()
                    .is_none_or(|c| &c.operation.operation_id != id)
            }) {
                self.retired.cancel();
                return Ok(());
            }
            let mut done = targets
                .iter()
                .map(|(_, done)| Box::pin(done.cancelled()))
                .collect::<Vec<_>>();
            let mut changed = Box::pin(changed.cancelled());
            let mut close = Box::pin(self.lane.owner.admission.seal_signal());
            std::future::poll_fn(|cx| {
                if close.as_mut().poll(cx).is_ready() {
                    return Poll::Ready(self.lane.owner.admission.check());
                }
                if changed.as_mut().poll(cx).is_ready() {
                    return Poll::Ready(Ok(()));
                }
                let mut complete = true;
                // Poll every receipt so each pending task registers its waker.
                for receipt in &mut done {
                    complete &= receipt.as_mut().poll(cx).is_ready();
                }
                if complete {
                    Poll::Ready(Ok(()))
                } else {
                    Poll::Pending
                }
            })
            .await?;
        }
    }
}
impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>
    RuntimeLane<C, S, M>
{
    pub(crate) async fn abort_run(&self) -> AbortCall {
        self.abort_run_checked(
            None,
            #[cfg(feature = "extensions")]
            None,
        )
        .await
    }
    /// Abort only the named current operation. The target comparison and native
    /// abort recipe share the same lane mutation line, including CAS retries.
    #[cfg(feature = "extensions")]
    pub async fn abort_expected_operation(
        &self,
        expected: &OperationId,
    ) -> Result<bool, HarnessRuntimeError> {
        match self.abort_run_checked(Some(expected.clone()), None).await? {
            Ok(_) => Ok(true),
            Err(AbortExpectedError::NoActiveOperation(_)) => Ok(false),
            Err(AbortExpectedError::Closed(_)) => Err(HarnessRuntimeError::Closed(HarnessClosed)),
        }
    }
    #[cfg(feature = "extensions")]
    pub(crate) async fn abort_profile_operation(
        &self,
        expected: &OperationId,
        guard: Arc<crate::extensions::profiles::actions::ActionGuard>,
    ) -> Result<bool, HarnessRuntimeError> {
        match self
            .abort_run_checked(Some(expected.clone()), Some(guard))
            .await?
        {
            Ok(_) => Ok(true),
            Err(AbortExpectedError::NoActiveOperation(_)) => Ok(false),
            Err(AbortExpectedError::Closed(_)) => Err(self
                .owner
                .admission
                .check()
                .err()
                .unwrap_or_else(|| self.owner.fail("closed abort admission"))),
        }
    }
    async fn abort_run_checked(
        &self,
        expected: Option<OperationId>,
        #[cfg(feature = "extensions")] guard: Option<
            Arc<crate::extensions::profiles::actions::ActionGuard>,
        >,
    ) -> AbortCall {
        if let Err(e) = self.owner.admission.check() {
            return match e {
                HarnessRuntimeError::Closed(_) => {
                    Ok(Err(AbortExpectedError::Closed(crate::inbox::closed())))
                }
                e => Err(e),
            };
        }
        let lane = self.clone();
        self.owner
            .clone()
            .retain_runtime(Box::pin(async move {
                let line = lane.lane.line.acquire().await?;
                loop {
                    let h = lane.load().await?;
                    #[cfg(feature = "extensions")]
                    if guard.as_ref().is_some_and(|guard| !guard.check(&h)) {
                        return Ok(Err(AbortExpectedError::NoActiveOperation(
                            NoActiveOperationError {
                                tag: Default::default(),
                                message: "parent is no longer current".into(),
                                lane: lane.lane.name.clone(),
                            },
                        )));
                    }
                    let Some(current) = &h.current else {
                        return Ok(Err(AbortExpectedError::NoActiveOperation(
                            NoActiveOperationError {
                                tag: Default::default(),
                                message: "lane has no open operation".into(),
                                lane: lane.lane.name.clone(),
                            },
                        )));
                    };
                    if expected
                        .as_ref()
                        .is_some_and(|expected| expected != &current.operation.operation_id)
                    {
                        return Ok(Err(AbortExpectedError::NoActiveOperation(
                            NoActiveOperationError {
                                tag: Default::default(),
                                message: "target operation is not current".into(),
                                lane: lane.lane.name.clone(),
                            },
                        )));
                    }
                    let ids = if let OperationState::Run(run) = &current.state {
                        match &run.control {
                            Control::Running(_) => run
                                .inbox
                                .steer
                                .iter()
                                .chain(&run.inbox.follow_up)
                                .cloned()
                                .collect::<Vec<_>>(),
                            Control::CancelRequested(c) => c
                                .drained_steer
                                .iter()
                                .chain(&c.drained_follow_up)
                                .cloned()
                                .collect(),
                        }
                    } else {
                        vec![]
                    };
                    let now = SafeUnsigned::new(lane.owner.clock.now_unix_ms())
                        .map_err(|_| lane.owner.fail("abort clock range failure"))?;
                    let decision = pi_harness_core::control::abort_recipe(
                        current,
                        &lane.facts(&h)?,
                        now,
                        &crate::inbox::placements(&h, &ids)?,
                        &lane.owner.telemetry,
                    )
                    .map_err(|_| lane.owner.fail("abort marker rejected"))?;
                    let Some(recipe) = decision.recipe else {
                        return Ok(Ok(decision.result));
                    };
                    let Some(applied) = lane.apply(recipe).await? else {
                        continue;
                    };
                    let operation = current.operation.operation_id.clone();
                    let activity = lane.lane.activity.lock().unwrap_or_else(|p| p.into_inner());
                    let active = lane
                        .lane
                        .active
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .as_ref()
                        .filter(|(id, _)| id == &operation)
                        .and_then(|(_, drive)| drive.upgrade());
                    let background = if active.is_none() {
                        if *lane.lane.driving.lock().unwrap_or_else(|p| p.into_inner()) {
                            return Err(lane.owner.fail("active run lost drive control"));
                        }
                        *lane
                            .lane
                            .generation
                            .lock()
                            .unwrap_or_else(|p| p.into_inner()) = Arc::new(());
                        *lane.lane.driving.lock().unwrap_or_else(|p| p.into_inner()) = true;
                        *lane
                            .lane
                            .reservation
                            .lock()
                            .unwrap_or_else(|p| p.into_inner()) = Some(operation.clone());
                        let fx = RuntimeEffects::new(
                            lane.clone(),
                            lane.lane
                                .restored
                                .swap(false, std::sync::atomic::Ordering::AcqRel),
                        );
                        *fx.operation_kind.lock().unwrap_or_else(|p| p.into_inner()) =
                            crate::snapshots::kind(current);
                        *fx.original_operation
                            .lock()
                            .unwrap_or_else(|p| p.into_inner()) = Some(current.operation.clone());
                        fx.register_drive(&operation);
                        Some(fx)
                    } else {
                        None
                    };
                    if matches!(current.operation.intent, OperationIntent::Run(_)) {
                        lane.owner.events.enqueue(
                            HarnessEventType::RunAbort,
                            &lane.lane.name,
                            HarnessEvent::RunAbort(RunAbortEvent {
                                event_type: Default::default(),
                                lane: lane.lane.name.clone(),
                                recovery: None,
                                run_id: operation.clone(),
                                steer: decision.result.steer.clone(),
                                follow_up: decision.result.follow_up.clone(),
                            }),
                        );
                    }
                    drop(activity);
                    let active = active
                        .unwrap_or_else(|| background.as_ref().expect("background drive").clone());
                    active.register_cancel(&applied)?;
                    drop(line);
                    active.abort(applied).await?;
                    if let Some(fx) = background {
                        let owner = lane.owner.clone();
                        let host = owner.host.clone();
                        host.spawn(Box::pin(async move {
                            let _guard = crate::driver::DriveGuard::new(fx.lane.lane.clone());
                            fx.trace.begin_kind(
                                &owner.session.metadata().base_metadata().id,
                                &fx.lane.lane.name,
                                &operation,
                                fx.recovery,
                                *fx.operation_kind.lock().unwrap_or_else(|p| p.into_inner()),
                            );
                            let result = crate::driver::drive(fx.clone(), operation).await;
                            match result {
                                Ok(result) => fx.trace.finish(&result),
                                Err(e) => {
                                    let _ = owner.observe::<()>(Err(e));
                                }
                            }
                        }))
                        .map_err(|_| lane.owner.fail("abort reconciliation admission failed"))?;
                    }
                    lane.owner.events.flush();
                    lane.lane.announce();
                    return Ok(Ok(decision.result));
                }
            }))
            .await
    }
}

#[cfg(feature = "extensions")]
impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>
    RuntimeHarness<C, S, M>
{
    /// Compare the expected main-lane operation inside the native abort line.
    pub async fn abort_expected_operation(
        &self,
        expected: &OperationId,
    ) -> Result<bool, HarnessRuntimeError> {
        self.main.abort_expected_operation(expected).await
    }
}
