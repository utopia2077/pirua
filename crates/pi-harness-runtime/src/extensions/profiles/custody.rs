//! One native effect owner retains receipts through its actual checked consumer.
use super::{
    actions::LaneActions,
    connection::{CallScope, ProfileConnection, Reply, key_wire},
    hooks::ScopeAuthority,
};
use crate::effects::RuntimeEffects;
use pi_agent_schema::*;
use pi_agent_session::storage::RuntimeStorage;
use pi_extension_protocol::profile_live::{
    Binding, NativeCustody, NativeObservation, NativeParent, RetireReason, Role,
};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
};
struct Retained {
    connection: Arc<ProfileConnection>,
    binding: Binding,
    operation: OperationId,
    anchor: Option<Arc<AtomicBool>>,
}
#[derive(Default)]
pub(crate) struct Receipts {
    keyed: Mutex<BTreeMap<EffectKey, Vec<Retained>>>,
    validation: Mutex<Vec<Retained>>,
    anchors: Mutex<Vec<(Option<EffectKey>, Arc<AtomicBool>)>>,
}
impl Receipts {
    pub(crate) fn check(&self, key: &EffectKey) -> Result<(), HarnessRuntimeError> {
        for token in self
            .keyed
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(key)
            .into_iter()
            .flatten()
        {
            if token.binding.key() != Some(&key_wire(key))
                || !token.connection.retained(&token.binding)
            {
                return Err(super::fault());
            }
        }
        Ok(())
    }
    pub(crate) fn has_validation(&self, operation: &OperationId) -> bool {
        self.validation
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .any(|token| &token.operation == operation)
    }
    pub(crate) fn check_validation(
        &self,
        operation: &OperationId,
    ) -> Result<(), HarnessRuntimeError> {
        for token in self
            .validation
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .filter(|t| &t.operation == operation)
        {
            if token.binding.key().is_some() || !token.connection.retained(&token.binding) {
                return Err(super::fault());
            }
        }
        Ok(())
    }
    pub(crate) fn invalidate(&self, key: &EffectKey) {
        self.anchors
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .retain(|(bound, active)| {
                if bound.as_ref() == Some(key) {
                    active.store(false, Ordering::Release);
                    false
                } else {
                    true
                }
            });
    }
    pub(crate) fn retire(&self, key: &EffectKey) {
        self.retire_as(key, RetireReason::NoLongerExpected);
    }
    pub(crate) fn settled(&self, key: &EffectKey) {
        self.retire_as(key, RetireReason::Consumed);
    }
    fn retire_as(&self, key: &EffectKey, reason: RetireReason) {
        let tokens = self
            .keyed
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(key)
            .unwrap_or_default();
        for token in tokens {
            if let Some(anchor) = token.anchor {
                anchor.store(false, Ordering::Release);
            }
            token.connection.retire(&token.binding, reason).ok();
        }
        self.invalidate(key);
    }
    pub(crate) fn check_operation(
        &self,
        operation: &OperationId,
    ) -> Result<(), HarnessRuntimeError> {
        let keys: Vec<_> = self
            .keyed
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .keys()
            .filter(|key| &key.operation_id == operation)
            .cloned()
            .collect();
        for key in keys {
            self.check(&key)?;
        }
        self.check_validation(operation)
    }
    pub(crate) fn retire_operation(&self, operation: &OperationId, reason: RetireReason) {
        let keys: Vec<_> = self
            .keyed
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .keys()
            .filter(|key| &key.operation_id == operation)
            .cloned()
            .collect();
        for key in keys {
            self.retire_as(&key, reason);
        }
        self.retire_validation(operation);
    }
    pub(crate) fn retire_validation(&self, operation: &OperationId) {
        let tokens = {
            let mut all = self.validation.lock().unwrap_or_else(|p| p.into_inner());
            let (tokens, remaining) = std::mem::take(&mut *all)
                .into_iter()
                .partition::<Vec<_>, _>(|token| &token.operation == operation);
            *all = remaining;
            tokens
        };
        for token in tokens {
            if let Some(anchor) = token.anchor {
                anchor.store(false, Ordering::Release);
            }
            token
                .connection
                .retire(&token.binding, RetireReason::Consumed)
                .ok();
        }
    }
    pub(crate) fn parent_id(
        &self,
        key: &EffectKey,
        connection: &Arc<ProfileConnection>,
    ) -> Option<u64> {
        self.keyed
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(key)
            .and_then(|tokens| {
                tokens.iter().rev().find(|t| {
                    t.binding.capability().kind == "provider"
                        && Arc::ptr_eq(&t.connection, connection)
                })
            })
            .map(|token| token.binding.id())
    }
}
pub(crate) struct EffectScope<C, S: RuntimeStorage, M: SessionMetadataRecord> {
    pub fx: Arc<RuntimeEffects<C, S, M>>,
    pub accepted: bool,
    pub expected_key: Option<EffectKey>,
}
struct EffectDispatch<C, S: RuntimeStorage, M: SessionMetadataRecord> {
    fx: Weak<RuntimeEffects<C, S, M>>,
    identity: Arc<()>,
    operation: OperationId,
    key: Option<EffectKey>,
    accepted: bool,
    allow_cancelled: bool,
}
impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>
    super::connection::DispatchScope for EffectDispatch<C, S, M>
{
    fn prepare(
        &self,
    ) -> BoxFuture<'_, Result<crate::scheduler::MutationPermit, HarnessRuntimeError>> {
        Box::pin(async move {
            let fx = self.fx.upgrade().ok_or_else(super::fault)?;
            let line = fx.lane.lane.line.acquire().await?;
            let h = fx.lane.load().await?;
            if !Arc::ptr_eq(&fx.identity, &self.identity)
                || fx.retired.is_cancelled()
                || !self.allow_cancelled && fx.cancellation.is_cancelled()
            {
                return Err(super::fault());
            }
            let valid = if self.accepted {
                h.current.as_ref().is_some_and(|current| {
                    current.operation.operation_id == self.operation
                        && (self.allow_cancelled
                            || !pi_harness_core::structural::is_cancelled(current))
                        && self
                            .key
                            .as_ref()
                            .is_none_or(|key| current_key(&fx, &h, key))
                        && fx
                            .original_operation
                            .lock()
                            .unwrap_or_else(|p| p.into_inner())
                            .as_ref()
                            .is_none_or(|original| original == &current.operation)
                })
            } else {
                h.current.is_none()
                    && fx
                        .lane
                        .lane
                        .reservation
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .as_ref()
                        == Some(&self.operation)
            };
            if !valid {
                return Err(super::fault());
            }
            Ok(line)
        })
    }
}
struct Anchor<C, S: RuntimeStorage, M: SessionMetadataRecord> {
    fx: Weak<RuntimeEffects<C, S, M>>,
    identity: Arc<()>,
    active: Arc<AtomicBool>,
    key: Option<EffectKey>,
    parent_request_id: Option<u64>,
    deadline_ms: u64,
    allow_cancelled: bool,
    tool: Option<Arc<dyn NativeCustody>>,
}
impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord> NativeCustody
    for Anchor<C, S, M>
{
    fn observe(
        &self,
        now_ms: u64,
    ) -> Result<NativeObservation, pi_extension_protocol::profile_live::Error> {
        let fx = self
            .fx
            .upgrade()
            .ok_or(pi_extension_protocol::profile_live::Error::Parent)?;
        if !Arc::ptr_eq(&fx.identity, &self.identity)
            || !self.active.load(Ordering::Acquire)
            || now_ms >= self.deadline_ms
            || !self.allow_cancelled && fx.cancellation.is_cancelled()
            || self.key.as_ref().is_some_and(|key| {
                !fx.running
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .contains_key(key)
            })
        {
            return Err(pi_extension_protocol::profile_live::Error::Parent);
        }
        if let Some(tool) = &self.tool {
            let observed = tool.observe(now_ms)?;
            if observed.key != self.key.as_ref().map(key_wire) {
                return Err(pi_extension_protocol::profile_live::Error::Parent);
            }
            return Ok(observed);
        }
        Ok(NativeObservation {
            connection: None,
            key: self.key.as_ref().map(key_wire),
            parent_request_id: self.parent_request_id,
            deadline_ms: self.deadline_ms,
        })
    }
    fn reserve_request_id(
        &self,
        now_ms: u64,
        requested: Option<u64>,
        minimum: u64,
    ) -> Result<Option<u64>, pi_extension_protocol::profile_live::Error> {
        self.observe(now_ms)?;
        match &self.tool {
            Some(tool) => tool.reserve_request_id(now_ms, requested, minimum),
            None => Ok(None),
        }
    }
}
impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord> ScopeAuthority
    for EffectScope<C, S, M>
{
    fn prepare<'a>(
        &'a self,
        event: &'a HookEvent,
        connection: &'a Arc<ProfileConnection>,
    ) -> BoxFuture<'a, Result<CallScope, HarnessRuntimeError>> {
        Box::pin(async move {
            let operation = operation(event);
            let dispatch = Arc::new(EffectDispatch {
                fx: Arc::downgrade(&self.fx),
                identity: self.fx.identity.clone(),
                operation: operation.clone(),
                key: self.expected_key.clone(),
                accepted: self.accepted,
                allow_cancelled: event.name() == HookName::BeforeResume,
            });
            let _line = self.fx.lane.lane.line.acquire().await?;
            let h = self.fx.lane.load().await?;
            if !self.accepted {
                if event.name() != HookName::BeforeRun
                    || h.current.is_some()
                    || self
                        .fx
                        .lane
                        .lane
                        .reservation
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .as_ref()
                        != Some(operation)
                {
                    return Err(super::fault());
                }
                return Ok(CallScope {
                    key: None,
                    role: Role::Preaccept,
                    parent: None,
                    native_parent: None,
                    permit: Some(_line),
                    slot: None,
                    dispatch: Some(dispatch),
                });
            }
            let current = h
                .current
                .as_ref()
                .filter(|current| &current.operation.operation_id == operation)
                .ok_or_else(super::fault)?;
            if event.name() != HookName::BeforeResume
                && pi_harness_core::structural::is_cancelled(current)
            {
                return Err(super::fault());
            }
            let key = self.expected_key.clone();
            if let Some(key) = &key {
                if !current_key(&self.fx, &h, key) {
                    return Err(super::fault());
                }
                let running = self.fx.running.lock().unwrap_or_else(|p| p.into_inner());
                if matches!(
                    running.get(key),
                    Some(EffectPlan::Hook(_) | EffectPlan::ToolClearance(_))
                ) {
                    return Ok(CallScope {
                        key: Some(key.clone()),
                        role: Role::Planned,
                        parent: None,
                        native_parent: None,
                        permit: Some(_line),
                        slot: None,
                        dispatch: Some(dispatch),
                    });
                }
            } else if event.name() != HookName::BeforeResume {
                return Err(super::fault());
            }
            let active = Arc::new(AtomicBool::new(true));
            self.fx
                .profile_receipts
                .anchors
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push((key.clone(), active.clone()));
            let parent_request_id = key
                .as_ref()
                .and_then(|key| self.fx.profile_receipts.parent_id(key, connection));
            let native_deadline = connection.scope_deadline()?;
            let tool = key.as_ref().and_then(|key| {
                self.fx
                    .routed_tools
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .get(key)
                    .and_then(|routed| {
                        let routed = routed.lock().unwrap_or_else(|p| p.into_inner());
                        connection
                            .same_tool_connection(&routed.route)
                            .then(|| {
                                routed.binding.as_ref().and_then(|binding| {
                                    routed
                                        .route
                                        .owner()
                                        .native_profile_parent(binding, native_deadline)
                                        .ok()
                                })
                            })
                            .flatten()
                    })
            });
            let anchor = Anchor {
                fx: Arc::downgrade(&self.fx),
                identity: self.fx.identity.clone(),
                active,
                key: key.clone(),
                parent_request_id,
                deadline_ms: native_deadline,
                allow_cancelled: event.name() == HookName::BeforeResume,
                tool,
            };
            Ok(CallScope {
                role: if key.is_some() {
                    Role::Inline
                } else {
                    Role::Preaccept
                },
                key,
                parent: None,
                native_parent: Some(NativeParent::from_runtime(Arc::new(anchor))),
                permit: Some(_line),
                slot: None,
                dispatch: Some(dispatch),
            })
        })
    }
    fn retain(
        &self,
        event: &HookEvent,
        key: Option<EffectKey>,
        connection: Arc<ProfileConnection>,
        reply: &Reply,
    ) {
        let token = Retained {
            connection,
            binding: reply.binding.clone(),
            operation: operation(event).clone(),
            anchor: None,
        };
        match key {
            Some(key) => self
                .fx
                .profile_receipts
                .keyed
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .entry(key)
                .or_default()
                .push(token),
            None => self
                .fx
                .profile_receipts
                .validation
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push(token),
        }
    }
    fn actions(
        &self,
        connection: &ProfileConnection,
        event: &HookEvent,
        key: Option<EffectKey>,
    ) -> Box<dyn super::connection::HostActions> {
        Box::new(LaneActions {
            lane: self.fx.lane.clone(),
            resources: self.fx.profile_resources.clone(),
            registry: connection.grant().profiles()[0]
                .approved()
                .registry()
                .clone(),
            max_bytes: connection.max_payload_bytes(),
            scope: super::actions::scope(&self.fx, operation(event).clone(), key, self.accepted),
        })
    }
    fn cancellation(&self) -> pi_ai_core::CancellationToken {
        self.fx.cancellation.clone()
    }
}
pub(crate) fn operation(event: &HookEvent) -> &OperationId {
    match event {
        HookEvent::BeforeRun(e) => &e.run_id,
        HookEvent::BeforeResume(e) => match e {
            BeforeResumeEvent::Run(e) => &e.run_id,
            BeforeResumeEvent::Compaction(e) => &e.run_id,
            BeforeResumeEvent::Navigation(e) => &e.run_id,
        },
        HookEvent::BeforeRunEnd(e) => &e.run_id,
        HookEvent::TransformContext(e) => &e.run_id,
        HookEvent::BeforeRequest(e) => &e.run_id,
        HookEvent::BeforePayload(e) => &e.run_id,
        HookEvent::AfterResponse(e) => &e.run_id,
        HookEvent::BeforeTool(e) => &e.run_id,
        HookEvent::AfterTool(e) => &e.run_id,
        HookEvent::BeforeCompaction(e) => &e.run_id,
        HookEvent::BeforeNavigation(e) => &e.run_id,
    }
}

impl Receipts {
    pub(crate) fn retain_provider(
        &self,
        key: &EffectKey,
        connection: Arc<ProfileConnection>,
        reply: &Reply,
    ) {
        self.keyed
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .entry(key.clone())
            .or_default()
            .push(Retained {
                connection,
                binding: reply.binding.clone(),
                operation: key.operation_id.clone(),
                anchor: None,
            });
    }
}
pub(crate) fn current_key<
    C: Send + Sync + 'static,
    S: RuntimeStorage + 'static,
    M: SessionMetadataRecord,
>(
    fx: &RuntimeEffects<C, S, M>,
    h: &crate::restore::HydratedLane,
    key: &EffectKey,
) -> bool {
    let Some(current) = h.current.as_ref() else {
        return false;
    };
    if key.session_id != fx.lane.owner.session.metadata().base_metadata().id
        || key.lane != fx.lane.lane.name
        || key.operation_id != current.operation.operation_id
    {
        return false;
    }
    let running = fx.running.lock().unwrap_or_else(|p| p.into_inner());
    let Some(plan) = running.get(key) else {
        return false;
    };
    match plan {
        EffectPlan::Assistant(plan) => {
            matches!(&current.state,OperationState::Run(run) if matches!(&run.phase,RunPhase::Assistant(phase) if PendingGeneration::try_from(phase.generation.clone()).is_ok_and(|p|p==plan.generation)))
        }
        EffectPlan::Summary(plan) => {
            pi_harness_core::structural::matches(current, plan, &key.session_id)
        }
        EffectPlan::Tool(plan) => {
            pi_harness_core::tool_batch::expects_effect(current, &key.session_id, plan)
        }
        EffectPlan::Deferred(plan) => {
            matches!(&current.state,OperationState::Run(run) if matches!(&run.phase,RunPhase::Deferred(phase) if PendingDeferred::try_from(phase.deferred.clone()).is_ok_and(|p|p==plan.deferred)))
        }
        EffectPlan::ToolClearance(plan) => {
            matches!(&current.state,OperationState::Run(run) if matches!(&run.phase,RunPhase::Tools(tools) if tools.batch.assistant_entry_id==plan.assistant_entry_id && tools.batch.turn_id==plan.turn_id && tools.batch.calls.iter().any(|call|matches!(call,ToolCallState::Planned{source_index,result_entry_id} if *source_index==plan.source_index&&*result_entry_id==plan.result_entry_id))))
        }
        EffectPlan::Hook(plan) => match &plan.event {
            HookEvent::BeforeRequest(_) => {
                matches!(&current.state,OperationState::Run(run) if matches!(&run.phase,RunPhase::Deferred(DeferredPhase{deferred,..}) if match deferred{Deferred::Suspended{step_id,poll,..}=>key.step_or_task_id.as_str()==step_id.as_str()&&key.logical_attempt.get()==poll.get().saturating_add(1),Deferred::EffectPending{step_id,poll,..}=>key.step_or_task_id.as_str()==step_id.as_str()&&key.logical_attempt.get()==poll.get()}))
                    || matches!(&current.state,OperationState::Run(run) if matches!(&run.phase,RunPhase::Assistant(phase) if matches!(&phase.generation,Generation::Ready{context,next_attempt} if context.step_id.as_str()==key.step_or_task_id.as_str()&&next_attempt.get()==key.logical_attempt.get())))
                    || pi_harness_core::structural::decision(current).is_some()
            }
            HookEvent::BeforeRunEnd(_) => {
                matches!(&current.state,OperationState::Run(run) if matches!(&run.phase,RunPhase::Checkpoint(checkpoint) if checkpoint.trigger_entry_id.as_str()==key.step_or_task_id.as_str()&&matches!(checkpoint.continuation,Continuation::MayFinish{..})))
            }
            HookEvent::BeforeCompaction(_) | HookEvent::BeforeNavigation(_) => {
                pi_harness_core::structural::task(current)
                    .is_ok_and(|task| task.as_str() == key.step_or_task_id.as_str())
            }
            _ => false,
        },
        EffectPlan::CancelDeferred(_) => true,
    }
}
