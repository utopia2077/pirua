//! Public acceptance and the pure action procedure.
use crate::{effects::RuntimeEffects, owner::*};
use pi_agent_schema::*;
use pi_agent_session::{backend_support::register_seq, storage::RuntimeStorage};
use pi_ai_core::{JsonValue, Message, Presence, SafeUnsigned, UserContent, UserMessage};
use pi_harness_core::{
    planner::{ProcedureAction, next_action},
    transition::{AcceptanceFacts, acceptance_recipe},
};
use std::sync::Arc;
pub(crate) struct DriveGuard {
    lane: Arc<LaneControl>,
    operation: OperationId,
    generation: Arc<()>,
}
impl DriveGuard {
    pub(crate) fn new(lane: Arc<LaneControl>) -> Self {
        let (operation, generation) = {
            let _activity = lane.activity.lock().unwrap_or_else(|p| p.into_inner());
            (
                lane.reservation
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .clone()
                    .expect("drive reservation"),
                lane.generation
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .clone(),
            )
        };
        Self {
            lane,
            operation,
            generation,
        }
    }
}
impl Drop for DriveGuard {
    fn drop(&mut self) {
        let removed = {
            let _activity = self.lane.activity.lock().unwrap_or_else(|p| p.into_inner());
            if !Arc::ptr_eq(
                &self.generation,
                &self
                    .lane
                    .generation
                    .lock()
                    .unwrap_or_else(|p| p.into_inner()),
            ) {
                return;
            }
            let mut reservation = self
                .lane
                .reservation
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            if reservation.as_ref() != Some(&self.operation) {
                return;
            }
            let mut active = self.lane.active.lock().unwrap_or_else(|p| p.into_inner());
            if active.as_ref().is_some_and(|(id, _)| id != &self.operation) {
                return;
            }
            let removed = active.take();
            reservation.take();
            *self.lane.driving.lock().unwrap_or_else(|p| p.into_inner()) = false;
            removed
        };
        drop(removed);
        self.lane.announce();
    }
}
impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>
    RuntimeLane<C, S, M>
{
    pub(crate) async fn prompt_run(&self, input: PromptInput) -> RunCall {
        if let Err(error) = self.owner.admission.check() {
            return match error {
                HarnessRuntimeError::Closed(_) => Ok(Err(RunExpectedError::Closed(ClosedError {
                    tag: Default::default(),
                    message: "Harness is closed".into(),
                }))),
                error => Err(error),
            };
        }
        let lane = self.clone();
        let result = self
            .owner
            .clone()
            .retain_runtime(Box::pin(async move { lane.accept_prompt(input).await }))
            .await;
        if let Err(HarnessRuntimeError::Fault(fault)) = &result {
            self.owner.admission.seal_fault(fault.clone());
        }
        result
    }
    async fn accept_prompt(&self, input: PromptInput) -> RunCall {
        let effects = RuntimeEffects::new(self.clone(), false);
        let run_id = OperationId::new(self.owner.ids.next(None))
            .map_err(|_| self.owner.fail("operation identity failed"))?;
        let h = {
            let _line = self.lane.line.acquire().await?;
            let h = self.load().await?;
            let _activity = self.lane.activity.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(current) = &h.current {
                return Ok(Err(RunExpectedError::LaneBusy(LaneBusyError {
                    tag: Default::default(),
                    message: "lane already owns an operation".into(),
                    lane: self.lane.name.clone(),
                    operation_id: current.operation.operation_id.clone(),
                    operation_kind: crate::snapshots::kind(current),
                })));
            }
            if *self.lane.driving.lock().unwrap_or_else(|p| p.into_inner()) {
                let operation_id = self
                    .lane
                    .reservation
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .clone();
                let operation_id = operation_id
                    .ok_or_else(|| self.owner.fail("lane reservation identity missing"))?;
                return Ok(Err(RunExpectedError::LaneBusy(LaneBusyError {
                    tag: Default::default(),
                    message: "lane acceptance is reserved".into(),
                    lane: self.lane.name.clone(),
                    operation_id,
                    operation_kind: OperationKind::Run,
                })));
            }
            *self
                .lane
                .generation
                .lock()
                .unwrap_or_else(|p| p.into_inner()) = Arc::new(());
            *self.lane.driving.lock().unwrap_or_else(|p| p.into_inner()) = true;
            *self
                .lane
                .reservation
                .lock()
                .unwrap_or_else(|p| p.into_inner()) = Some(run_id.clone());
            h
        };
        let _drive = DriveGuard::new(self.lane.clone());
        effects.register_drive(&run_id);
        effects.trace.begin(
            &self.owner.session.metadata().base_metadata().id,
            &self.lane.name,
            &run_id,
            false,
        );
        let now = SafeUnsigned::new(self.owner.clock.now_unix_ms())
            .map_err(|_| self.owner.fail("prompt clock range failure"))?;
        let prompt = match input {
            PromptInput::Messages(messages) => messages,
            PromptInput::Text { text, images } => {
                let content = if images.is_empty() {
                    UserContent::Text(text)
                } else {
                    let mut blocks =
                        vec![pi_ai_core::InputContent::Text(pi_ai_core::TextContent {
                            content_type: Default::default(),
                            text,
                            text_signature: None,
                        })];
                    blocks.extend(images.into_iter().map(pi_ai_core::InputContent::Image));
                    UserContent::Blocks(blocks)
                };
                vec![AgentMessage::BuiltIn(Message::User(UserMessage {
                    role: Default::default(),
                    content,
                    timestamp: now,
                }))]
            }
        };
        if prompt.iter().any(|message| {
            self.owner
                .session
                .codec()
                .validate_message(message)
                .is_err()
        }) {
            return Ok(Err(RunExpectedError::InvalidMessage(InvalidMessageError {
                tag: Default::default(),
                message: "invalid prompt message".into(),
                lane: self.lane.name.clone(),
                reason: "invalid prompt message".into(),
            })));
        }
        let resources = self
            .owner
            .settings
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .resources
            .clone();
        // Only the uncommitted preview (including its context resolution) can
        // be cut here. Acceptance publication below keeps retained ownership
        // once admitted, and operation cancellation does not seal the owner.
        let preview = {
            let mut preview = Box::pin(effects.system_prompt(None));
            let mut sealed = Box::pin(self.owner.admission.seal_signal());
            std::future::poll_fn(|cx| {
                if sealed.as_mut().poll(cx).is_ready() {
                    return std::task::Poll::Ready(Err(self
                        .owner
                        .admission
                        .check()
                        .expect_err("sealed preview owner")));
                }
                preview.as_mut().poll(cx)
            })
            .await?
        };
        let output = effects
            .hook(
                HookEvent::BeforeRun(BeforeRunEvent {
                    lane: self.lane.name.clone(),
                    run_id: run_id.clone(),
                    prompt: prompt.clone(),
                    system_prompt: preview,
                    resources,
                }),
                false,
                None,
            )
            .await?;
        let patch = match output.result() {
            Some(HookResult::BeforeRun(result)) => result.clone().unwrap_or_default(),
            _ => return Err(self.owner.fail("invalid before-run aggregate")),
        };
        let mut captured_ids = Vec::new();
        if let Register::LaneState { value, .. } = &h.roots.lane_state {
            captured_ids = value.pending_next_run.clone();
        }
        loop {
            let _settings = self.owner.settings_line.acquire().await?;
            let _line = self.lane.line.acquire().await?;
            let settings = self
                .owner
                .settings
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .clone();
            let current = self.load().await?;
            let Register::LaneState {
                value: lane_state, ..
            } = &current.roots.lane_state
            else {
                return Err(self.owner.fail("acceptance lane state invalid"));
            };
            let Register::LaneLeaf {
                value: source_leaf, ..
            } = &current.roots.leaf
            else {
                return Err(self.owner.fail("acceptance leaf invalid"));
            };
            let Register::LaneConfig {
                value: configuration,
                ..
            } = &current.roots.configuration
            else {
                return Err(self.owner.fail("acceptance configuration invalid"));
            };
            let missing = self.missing(configuration);
            if !missing.models.is_empty() || !missing.tools.is_empty() {
                return Ok(Err(RunExpectedError::MissingIdentities(
                    MissingIdentitiesError {
                        tag: Default::default(),
                        message: "configured identities are missing".into(),
                        lane: self.lane.name.clone(),
                        tools: missing.tools,
                        models: missing.models,
                    },
                )));
            }
            let mut parent = source_leaf.clone();
            let mut captured = Vec::new();
            for id in captured_ids.clone() {
                if !lane_state.pending_next_run.contains(&id) {
                    continue;
                }
                let Some(Register::PendingEntry { value, seq, .. }) = current
                    .registers
                    .get(&(RegisterNamespace::PendingEntry, id.as_str().into()))
                else {
                    return Err(self.owner.fail("captured pending message missing"));
                };
                let Presence::Value(payload) = &value.payload else {
                    return Err(self.owner.fail("captured pending message payload missing"));
                };
                let message: AgentMessage = serde_json::from_value(
                    serde_json::to_value(payload)
                        .map_err(|_| self.owner.fail("pending conversion failed"))?,
                )
                .map_err(|_| self.owner.fail("pending message conversion failed"))?;
                let entry = NewEntry::Message(NewMessageEntry {
                    id: id.clone(),
                    parent_id: parent,
                    entry_type: MessageEntryType::Message,
                    message,
                    terminate: None,
                });
                parent = Some(id);
                captured.push(
                    PendingPlacement::new(entry, value.clone(), *seq)
                        .map_err(|_| self.owner.fail("pending placement mismatch"))?,
                );
            }
            let mut entries =
                |messages: Vec<AgentMessage>| -> Result<Vec<NewMessageEntry>, HarnessRuntimeError> {
                    messages
                        .into_iter()
                        .map(|message| {
                            let id = EntryId::new(self.owner.ids.next(None))
                                .map_err(|_| self.owner.fail("prompt entry identity failed"))?;
                            let entry = NewMessageEntry {
                                id: id.clone(),
                                parent_id: parent.clone(),
                                entry_type: MessageEntryType::Message,
                                message,
                                terminate: None,
                            };
                            parent = Some(id);
                            Ok(entry)
                        })
                        .collect()
                };
            let messages = entries(prompt.clone())?;
            let injections = entries(patch.messages.clone().unwrap_or_default())?;
            let resume_data = match patch.resume_data.clone() {
                Presence::Value(JsonValue::Object(data)) => Some(data),
                Presence::Missing => None,
                _ => return Err(self.owner.fail("before-run resume data must be an object")),
            };
            let operation = Operation {
                operation_id: run_id.clone(),
                lane: self.lane.name.clone(),
                source_leaf_id: source_leaf.clone(),
                started_at: now,
                intent: OperationIntent::Run(RunIntent {
                    kind: Default::default(),
                    prompt_entry_ids: messages.iter().map(|e| e.id.clone()).collect(),
                    system_prompt_override: patch.system_prompt.clone(),
                    resume_data,
                }),
            };
            let facts = AcceptanceFacts {
                operation,
                lane_state: lane_state.clone(),
                lane_seq: register_seq(&current.roots.lane_state),
                leaf_seq: Some(register_seq(&current.roots.leaf)),
                configuration_seq: register_seq(&current.roots.configuration),
                settings_revision: settings.revision,
                settings: settings.run,
                captured,
                messages,
                injections,
                skip_inbox_once: None,
                identities_present: true,
                telemetry_context: effects.trace.context(),
            };
            if facts.messages.is_empty() && facts.injections.is_empty() && facts.captured.is_empty()
            {
                return Ok(Err(RunExpectedError::InvalidMessage(InvalidMessageError {
                    tag: Default::default(),
                    message: "prompt has no entries".into(),
                    lane: self.lane.name.clone(),
                    reason: "prompt has no entries".into(),
                })));
            }
            let recipe = acceptance_recipe(&facts)
                .map_err(|_| self.owner.fail("prompt acceptance recipe rejected"))?;
            if self.apply(recipe).await?.is_none() {
                continue;
            }
            drop(_line);
            drop(_settings);
            effects.events().flush();
            break;
        }
        let result = drive(effects.clone(), run_id.clone()).await?;
        effects.trace.finish(&result);
        let OperationResult::Run(outcome) = result else {
            return Err(self.owner.fail("run returned another operation kind"));
        };
        Ok(Ok(RunSuccess {
            run_id,
            outcome: *outcome,
        }))
    }
    pub(crate) fn missing(&self, configuration: &LaneConfiguration) -> MissingIdentities {
        let bindings = self
            .owner
            .tools
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        let tools = configuration
            .active_tool_names
            .iter()
            .filter(|name| {
                !bindings
                    .iter()
                    .any(|binding| binding.descriptor().name == **name)
            })
            .map(|n| n.as_str().to_owned())
            .collect();
        let models = if self
            .owner
            .models
            .get_model(&configuration.model.provider, &configuration.model.model_id)
            .is_some()
        {
            vec![]
        } else {
            vec![format!(
                "{}/{}",
                configuration.model.provider.as_str(),
                configuration.model.model_id.as_str()
            )]
        };
        MissingIdentities { tools, models }
    }
    pub(crate) async fn skill_run(&self, name: String, additional: Option<String>) -> RunCall {
        if let Err(error) = self.owner.admission.check() {
            return match error {
                HarnessRuntimeError::Closed(_) => Ok(Err(RunExpectedError::Closed(ClosedError {
                    tag: Default::default(),
                    message: "Harness is closed".into(),
                }))),
                error => Err(error),
            };
        }

        let resources = self
            .owner
            .settings
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .resources
            .clone();
        let Some(skill) = resources
            .skills
            .unwrap_or_default()
            .into_iter()
            .find(|s| s.name == name)
        else {
            return Ok(Err(RunExpectedError::UnknownSkill(UnknownSkillError {
                tag: Default::default(),
                message: "unknown skill".into(),
                name,
            })));
        };
        let text = crate::resources::skill(&skill, additional.as_deref());
        self.prompt_run(PromptInput::Text {
            text,
            images: vec![],
        })
        .await
    }
    pub(crate) async fn template_run(&self, name: String, args: Vec<String>) -> RunCall {
        if let Err(error) = self.owner.admission.check() {
            return match error {
                HarnessRuntimeError::Closed(_) => Ok(Err(RunExpectedError::Closed(ClosedError {
                    tag: Default::default(),
                    message: "Harness is closed".into(),
                }))),
                error => Err(error),
            };
        }

        let resources = self
            .owner
            .settings
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .resources
            .clone();
        let Some(template) = resources
            .prompt_templates
            .unwrap_or_default()
            .into_iter()
            .find(|t| t.name == name)
        else {
            return Ok(Err(RunExpectedError::UnknownTemplate(
                UnknownTemplateError {
                    tag: Default::default(),
                    message: "unknown template".into(),
                    name,
                },
            )));
        };
        let text = crate::resources::template(&template.content, &args);
        self.prompt_run(PromptInput::Text {
            text,
            images: vec![],
        })
        .await
    }
    pub(crate) async fn resume_run(&self) -> ResumeCall {
        if let Err(error) = self.owner.admission.check() {
            return match error {
                HarnessRuntimeError::Closed(_) => {
                    Ok(Err(ResumeExpectedError::Closed(ClosedError {
                        tag: Default::default(),
                        message: "Harness is closed".into(),
                    })))
                }
                error => Err(error),
            };
        }

        let lane = self.clone();
        self.owner.clone().retain_runtime(Box::pin(async move {
            let recovery=lane.lane.restored.load(std::sync::atomic::Ordering::Acquire);
            let effects=RuntimeEffects::new(lane.clone(),recovery);
            let registration:Arc<dyn crate::abort::ActiveDrive>=effects.clone();
            let mut current={let _line=lane.lane.line.acquire().await?;let h=lane.load().await?;let Some(current)=h.current else {return Ok(Err(ResumeExpectedError::NothingToResume(NothingToResumeError {tag:Default::default(),message:"lane has no open operation".into(),lane:lane.lane.name.clone()})));};
                if !lane.lane.reserve_drive(current.operation.operation_id.clone(),Arc::downgrade(&registration)) {return Ok(Err(ResumeExpectedError::LaneBusy(LaneBusyError {tag:Default::default(),message:"lane is already driving".into(),lane:lane.lane.name.clone(),operation_id:current.operation.operation_id.clone(),operation_kind:crate::snapshots::kind(&current)})));}
                current};
            let _drive=DriveGuard::new(lane.lane.clone());
            if let OperationState::Run(run)=&current.state && matches!(run.control,Control::Running(_)) && let RunPhase::Deferred(p)=&run.phase {
                let configuration=match &p.deferred {Deferred::Suspended{configuration,..}|Deferred::EffectPending{configuration,..}=>configuration};
                let missing=lane.missing(configuration);
                let precheck_line=lane.lane.line.acquire().await?;
                let latest=lane.load().await?.current;
                match latest {Some(latest) if latest.operation.operation_id==current.operation.operation_id=>current=latest,_=>{drop(precheck_line);let result=externally_completed(&effects,&current.operation.operation_id).await?;return Ok(Ok(resume_outcome(result,&current.operation.operation_id)?));}}
                if matches!(&current.state,OperationState::Run(r) if matches!(r.control,Control::Running(_))) && (!missing.models.is_empty()||!missing.tools.is_empty()) {
                    // Rejection and relinquishing this drive are one lane boundary.
                    // A later abort must select its own reconciliation drive.
                    effects.release_drive();
                    drop(precheck_line);
                    lane.lane.announce();
                    return Ok(Err(ResumeExpectedError::MissingIdentities(MissingIdentitiesError{tag:Default::default(),message:"captured identities are missing".into(),lane:lane.lane.name.clone(),tools:missing.tools,models:missing.models})));
                }
            }
            lane.lane.restored.store(false,std::sync::atomic::Ordering::Release);
            effects.poll_permit.store(true,std::sync::atomic::Ordering::Release);
            effects.trace.begin(&lane.owner.session.metadata().base_metadata().id,&lane.lane.name,&current.operation.operation_id,recovery);
            let cancelled=matches!(&current.state,OperationState::Run(r) if matches!(r.control,Control::CancelRequested(_)));
            if cancelled {let h=lane.load().await?;effects.schedule_cancel(&h).await?;effects.cancellation.cancel();}
            if let OperationIntent::Run(intent)=&current.operation.intent {
                let entries=lane.owner.session.get_entries(&intent.prompt_entry_ids).await.map_err(|_|lane.owner.fail("resume prompt hydration failed"))?;
                let prompt=intent.prompt_entry_ids.iter().map(|id|match entries.get(id) {Some(Entry::Message(entry))=>Ok(entry.message.clone()),_=>Err(lane.owner.fail("resume prompt entry missing"))}).collect::<Result<Vec<_>,_>>()?;
                effects.hook(HookEvent::BeforeResume(BeforeResumeEvent::Run(BeforeResumeRunEvent {lane:lane.lane.name.clone(),run_id:current.operation.operation_id.clone(),kind:Default::default(),prompt,system_prompt_override:intent.system_prompt_override.clone(),resume_data:intent.resume_data.clone().map_or(Presence::Missing,|data|Presence::Value(JsonValue::Object(data)))})),true,None).await?;
            }
            effects.events().enqueue(HarnessEventType::RunResume,&lane.lane.name,HarnessEvent::RunResume(RunResumeEvent {event_type:Default::default(),lane:lane.lane.name.clone(),recovery:recovery.then_some(pi_ai_core::LiteralTrue),run_id:current.operation.operation_id.clone()}));effects.events().flush();
            effects.turn_start(&current);effects.events().flush();
            let result=drive(effects.clone(),current.operation.operation_id.clone()).await?;effects.trace.finish(&result);
            Ok(Ok(resume_outcome(result,&current.operation.operation_id)?))
        })).await
    }
}
fn resume_outcome(
    result: OperationResult,
    operation: &OperationId,
) -> Result<ResumeOutcome, HarnessRuntimeError> {
    let OperationResult::Run(outcome) = result else {
        return Err(crate::restore::fault("resume operation kind unsupported"));
    };
    let mut value = serde_json::to_value(outcome)
        .map_err(|_| crate::restore::fault("resume output conversion failed"))?;
    let map = value
        .as_object_mut()
        .ok_or_else(|| crate::restore::fault("resume output shape invalid"))?;
    map.insert("operation".into(), serde_json::Value::String("run".into()));
    map.insert(
        "runId".into(),
        serde_json::Value::String(operation.as_str().into()),
    );
    serde_json::from_value(value)
        .map_err(|_| crate::restore::fault("resume output conversion failed"))
}

struct PendingEffect {
    plan: EffectPlan<Telemetry>,
    done: pi_ai_core::CancellationToken,
    output: std::sync::Mutex<Option<Result<EffectOutput, HarnessRuntimeError>>>,
}
async fn start<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>(
    fx: &Arc<RuntimeEffects<C, S, M>>,
    mut plan: EffectPlan<Telemetry>,
    pending: &mut std::collections::BTreeMap<EffectKey, Arc<PendingEffect>>,
) -> Result<(), HarnessRuntimeError> {
    if let EffectPlan::Hook(hook) = &mut plan
        && matches!(&hook.event,HookEvent::BeforeRequest(e) if e.step==HookRequestStep::Deferred)
    {
        let loaded = {
            let _line = fx.lane.lane.line.acquire().await?;
            fx.lane.load().await?
        };
        if let Some(current) = loaded
            .current
            .filter(|c| c.operation.operation_id == hook.key.operation_id)
        {
            fx.trace.poll_start(
                &current,
                hook.key.step_or_task_id.as_str(),
                hook.key.logical_attempt.get(),
            );
            fx.start_turn(
                &current.operation.operation_id,
                format!(
                    "{}:poll:{}",
                    hook.key.step_or_task_id.as_str(),
                    hook.key.logical_attempt.get()
                ),
            );
            hook.telemetry_context = fx.trace.context();
            fx.events().flush();
        }
    }
    let started = pi_ai_core::CancellationToken::new();
    fx.start_signals
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .insert(plan.key().clone(), started.clone());
    let slot = Arc::new(PendingEffect {
        plan: plan.clone(),
        done: pi_ai_core::CancellationToken::new(),
        output: std::sync::Mutex::new(None),
    });
    pending.insert(plan.key().clone(), slot.clone());
    let running = fx.clone();
    let ownership = EffectTaskOwnership {
        fx: fx.clone(),
        slot: slot.clone(),
        started: started.clone(),
        completed: false,
    };
    fx.lane
        .owner
        .host
        .spawn(Box::pin(async move {
            let result = running.run(plan).await;
            *slot.output.lock().unwrap_or_else(|p| p.into_inner()) = Some(result);
            ownership.complete();
        }))
        .map_err(|_| fx.lane.owner.fail("driver effect task admission failed"))?;
    wait_owned(fx, &started).await
}
pub(crate) async fn drive<
    C: Send + Sync + 'static,
    S: RuntimeStorage + 'static,
    M: SessionMetadataRecord,
>(
    fx: Arc<RuntimeEffects<C, S, M>>,
    operation_id: OperationId,
) -> Result<OperationResult, HarnessRuntimeError> {
    let mut pending = std::collections::BTreeMap::new();
    let mut immediate_key = None;
    loop {
        let h = {
            let _line = fx.lane.lane.line.acquire().await?;
            fx.lane.load().await?
        };
        if h.current
            .as_ref()
            .is_none_or(|current| current.operation.operation_id != operation_id)
        {
            return externally_completed(&fx, &operation_id).await;
        }
        let current = h.current.clone().expect("matching drive operation");
        fx.trace.sync(&current);
        let inputs = fx.inputs(&h).await?;
        let action = if let Some(key) = immediate_key.take() {
            ProcedureAction::Existing(Action::AwaitEffect(key))
        } else {
            let facts = if pi_harness_core::inbox::select(&current)
                .map_err(|_| fx.lane.owner.fail("inbox selection failed"))?
                .is_some()
            {
                let Some(facts) = fx.prepare_inbox(&h).await? else {
                    continue;
                };
                facts
            } else {
                fx.planning(&h, None, inputs.now).await?
            };
            next_action(&current, &inputs, &facts).map_err(|_| {
                fx.lane
                    .owner
                    .fail("operation planning requires unavailable recovery facts or a later stage")
            })?
        };
        let settlement = match action {
            ProcedureAction::Existing(Action::Transition(plan)) => {
                fx.commit_transition(TransitionCommit {
                    current,
                    next: plan.next,
                    telemetry_context: plan.telemetry_context,
                    expected_configuration_seq: plan.expected_configuration_seq,
                    expected_settings_revision: plan.expected_settings_revision,
                })
                .await?;
                continue;
            }
            ProcedureAction::Existing(Action::Dispatch(plan)) => {
                if plan.intent.is_some() {
                    return Err(fx
                        .lane
                        .owner
                        .fail("direct intent requires a later runtime stage"));
                }
                if matches!(
                    plan.effect,
                    EffectPlan::Hook(_) | EffectPlan::ToolClearance(_)
                ) {
                    immediate_key = Some(plan.effect.key().clone());
                }
                start(&fx, plan.effect, &mut pending).await?;
                continue;
            }
            ProcedureAction::Existing(Action::AwaitEffect(key)) => {
                let slot = pending
                    .remove(&key)
                    .ok_or_else(|| fx.lane.owner.fail("live effect ticket is missing"))?;
                wait_owned(&fx, &slot.done).await?;
                // A completed callback has no settlement/finalization authority
                // once its original operation has ended.
                let latest = {
                    let _line = fx.lane.lane.line.acquire().await?;
                    fx.lane.load().await?
                };
                if latest
                    .current
                    .as_ref()
                    .is_none_or(|current| current.operation.operation_id != operation_id)
                {
                    return externally_completed(&fx, &operation_id).await;
                }
                let output = slot.output.lock().unwrap_or_else(|p| p.into_inner()).take();
                let output = output
                    .ok_or_else(|| fx.lane.owner.fail("effect completed without output"))??;
                let output = match output {
                    EffectOutput::SummarySuspended(output) => {
                        SettlementOutput::SummarySuspended(output)
                    }
                    EffectOutput::Deferred { key, message } => {
                        SettlementOutput::Deferred { key, message }
                    }
                    EffectOutput::Assistant { key, message } => {
                        SettlementOutput::Assistant { key, message }
                    }
                    EffectOutput::Hook(output) => SettlementOutput::Hook(output),
                    EffectOutput::RequestOptions(output) => {
                        SettlementOutput::RequestOptions(output)
                    }
                    EffectOutput::ToolClearance(output) => SettlementOutput::ToolClearance(output),
                    EffectOutput::ToolRaw(output) => {
                        let EffectPlan::Tool(plan) = &slot.plan else {
                            return Err(fx.lane.owner.fail("tool output family mismatch"));
                        };
                        let output = fx.finalize_tool(plan.clone(), output).await?;
                        let latest = {
                            let _line = fx.lane.lane.line.acquire().await?;
                            fx.lane.load().await?
                        };
                        if latest
                            .current
                            .as_ref()
                            .is_none_or(|current| current.operation.operation_id != operation_id)
                        {
                            return externally_completed(&fx, &operation_id).await;
                        }
                        SettlementOutput::Tool(output)
                    }
                    EffectOutput::NotStarted { key } => SettlementOutput::NotStarted { key },
                    _ => {
                        return Err(fx
                            .lane
                            .owner
                            .fail("effect output requires a later runtime stage"));
                    }
                };
                SettlementCommit {
                    current,
                    plan: slot.plan.clone(),
                    output,
                    telemetry_context: fx.trace.context(),
                }
            }
            ProcedureAction::Existing(Action::Wait(plan)) => {
                fx.sleep(
                    std::time::Duration::from_millis(
                        plan.until.get().saturating_sub(inputs.now.get()),
                    ),
                    plan.telemetry_context,
                )
                .await?;
                continue;
            }
            ProcedureAction::Existing(Action::Suspend(result)) => {
                let line = fx.lane.lane.line.acquire().await?;
                let latest = fx.lane.load().await?;
                let Some(latest) = latest
                    .current
                    .filter(|c| c.operation.operation_id == operation_id)
                else {
                    drop(line);
                    return externally_completed(&fx, &operation_id).await;
                };
                if latest.operation_state_seq != current.operation_state_seq
                    || latest.lane_state_seq != current.lane_state_seq
                {
                    drop(line);
                    continue;
                }
                // Publish suspension and relinquish this exact drive under the same
                // lane boundary. A subsequent abort either owns this drive or starts
                // a new reconciliation generation; it cannot lose both owners.
                fx.publish_suspension(&latest, &result);
                fx.release_drive();
                drop(line);
                fx.events().flush();
                fx.lane.lane.announce();
                return Ok(result);
            }
            ProcedureAction::Existing(Action::Finish(result)) => {
                if fx
                    .commit_terminal(TerminalCommit {
                        current,
                        result: result.clone(),
                        telemetry_context: fx.trace.context(),
                    })
                    .await?
                    .is_some()
                {
                    return Ok(result);
                }
                continue;
            }
            ProcedureAction::Reconcile(commit) => *commit,
        };
        if let Some(result) = fx.commit_settlement(settlement).await? {
            if let Some(terminal) = result.terminal {
                if fx
                    .commit_terminal(TerminalCommit {
                        current: result.current,
                        result: terminal.clone(),
                        telemetry_context: fx.trace.context(),
                    })
                    .await?
                    .is_some()
                {
                    return Ok(terminal);
                }
            } else if let Some(plan) = result.dispatch {
                start(&fx, plan, &mut pending).await?;
            } else if let Some(suspend) = result.suspend {
                return Ok(suspend);
            }
        }
    }
}

struct EffectTaskOwnership<
    C: Send + Sync + 'static,
    S: RuntimeStorage + 'static,
    M: SessionMetadataRecord,
> {
    fx: Arc<RuntimeEffects<C, S, M>>,
    slot: Arc<PendingEffect>,
    started: pi_ai_core::CancellationToken,
    completed: bool,
}
impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>
    EffectTaskOwnership<C, S, M>
{
    fn complete(mut self) {
        self.completed = true;
    }
}
impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord> Drop
    for EffectTaskOwnership<C, S, M>
{
    fn drop(&mut self) {
        if !self.completed && self.fx.lane.owner.admission.check().is_ok() {
            let _ = self
                .fx
                .lane
                .owner
                .fail("host dropped an effect completion task");
        }
        self.slot.done.cancel();
        self.started.cancel();
    }
}
async fn wait_owned<
    C: Send + Sync + 'static,
    S: RuntimeStorage + 'static,
    M: SessionMetadataRecord,
>(
    fx: &RuntimeEffects<C, S, M>,
    done: &pi_ai_core::CancellationToken,
) -> Result<(), HarnessRuntimeError> {
    let mut done = Box::pin(done.cancelled());
    let mut sealed = Box::pin(fx.lane.owner.admission.seal_signal());
    std::future::poll_fn(|cx| {
        if sealed.as_mut().poll(cx).is_ready() {
            return std::task::Poll::Ready(fx.lane.owner.admission.check());
        }
        done.as_mut().poll(cx).map(|_| ()).map(Ok)
    })
    .await
}

/// Observe external finalization only at an existing reload/completion boundary.
/// Raw Session writers do not provide a proactive drive wakeup.
async fn externally_completed<
    C: Send + Sync + 'static,
    S: RuntimeStorage + 'static,
    M: SessionMetadataRecord,
>(
    fx: &RuntimeEffects<C, S, M>,
    operation: &OperationId,
) -> Result<OperationResult, HarnessRuntimeError> {
    fx.retired.cancel();
    fx.cancellation.cancel();
    let result = {
        let _line = fx.lane.lane.line.acquire().await?;
        loop {
            fx.lane.owner.admission.check()?;
            let before = fx
                .lane
                .owner
                .session
                .read_stamp()
                .await
                .map_err(|_| fx.lane.owner.fail("external terminal stamp read failed"))?;
            let result = read_external_result(&fx.lane, operation).await;
            let after = fx
                .lane
                .owner
                .session
                .read_stamp()
                .await
                .map_err(|_| fx.lane.owner.fail("external terminal stamp read failed"))?;
            if before == after {
                break fx.lane.owner.observe(result)?;
            }
        }
    };
    let old = fx
        .lane
        .lane
        .streaming
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .take();
    drop(old);
    fx.events().enqueue(
        HarnessEventType::RunEnd,
        &fx.lane.lane.name,
        crate::publication::run_end(fx.lane.lane.name.clone(), operation.clone(), result.clone())?,
    );
    fx.events().flush();
    Ok(result)
}

async fn read_external_result<
    C: Send + Sync + 'static,
    S: RuntimeStorage + 'static,
    M: SessionMetadataRecord,
>(
    lane: &RuntimeLane<C, S, M>,
    operation: &OperationId,
) -> Result<OperationResult, HarnessRuntimeError> {
    use crate::restore::fault;
    let session = &lane.owner.session;
    for namespace in [
        RegisterNamespace::OperationMeta,
        RegisterNamespace::OperationState,
    ] {
        if session
            .get_register(namespace, operation.as_str())
            .await
            .map_err(|_| fault("external operation read failed"))?
            .is_some()
        {
            return Err(fault("external terminal retains operation registers"));
        }
    }
    let Some(Register::LaneLastResult { value, .. }) = session
        .get_register(RegisterNamespace::LaneLastResult, lane.lane.name.as_str())
        .await
        .map_err(|_| fault("external terminal result read failed"))?
    else {
        return Err(fault("external terminal result missing"));
    };
    let (id, leaf, final_id, kind, error) = match value {
        LaneLastResult::RunCompletedAssistant(v) => (
            v.operation_id,
            v.leaf_id,
            Some(v.final_assistant_entry_id),
            PublicOutcomeKind::Completed,
            None,
        ),
        LaneLastResult::RunCompletedTerminatedTools(v) => (
            v.operation_id,
            v.leaf_id,
            None,
            PublicOutcomeKind::Completed,
            None,
        ),
        LaneLastResult::RunAborted(v) => (
            v.operation_id,
            v.leaf_id,
            v.final_assistant_entry_id,
            PublicOutcomeKind::Aborted,
            None,
        ),
        LaneLastResult::RunFailed(v) => (
            v.operation_id,
            v.leaf_id,
            v.final_assistant_entry_id,
            PublicOutcomeKind::Failed,
            Some(v.error),
        ),
        _ => return Err(fault("external terminal operation kind mismatch")),
    };
    if &id != operation {
        return Err(fault("external terminal operation identity mismatch"));
    }
    let leaf = leaf.ok_or_else(|| fault("external run terminal leaf missing"))?;
    let mut ids = vec![leaf.clone()];
    if let Some(id) = &final_id
        && id != &leaf
    {
        ids.push(id.clone());
    }
    let entries = session
        .get_entries(&ids)
        .await
        .map_err(|_| fault("external terminal entry read failed"))?;
    if !entries.contains_key(&leaf) {
        return Err(fault("external terminal leaf entry missing"));
    }
    let final_pair = if let Some(id) = final_id {
        let Some(Entry::Message(entry)) = entries.get(&id) else {
            return Err(fault("external terminal assistant entry missing"));
        };
        let AgentMessage::BuiltIn(Message::Assistant(message)) = &entry.message else {
            return Err(fault("external terminal final entry is not assistant"));
        };
        Some((
            id,
            SettledAssistantMessage::new(message.as_ref().clone())
                .map_err(|_| fault("external terminal assistant is invalid"))?,
        ))
    } else {
        None
    };
    let outcome = match (kind, final_pair) {
        (PublicOutcomeKind::Completed, Some((final_entry_id, final_message))) => {
            RunOutcome::CompletedWithAssistant(RunCompletedWithAssistant {
                kind: Default::default(),
                leaf_id: leaf,
                final_entry_id,
                final_message,
            })
        }
        (PublicOutcomeKind::Completed, None) => {
            RunOutcome::CompletedWithoutAssistant(RunCompletedWithoutAssistant {
                kind: Default::default(),
                leaf_id: leaf,
            })
        }
        (PublicOutcomeKind::Aborted, Some((final_entry_id, final_message))) => {
            RunOutcome::AbortedWithAssistant(RunAbortedWithAssistant {
                kind: Default::default(),
                leaf_id: leaf,
                final_entry_id,
                final_message,
            })
        }
        (PublicOutcomeKind::Aborted, None) => {
            RunOutcome::AbortedWithoutAssistant(RunAbortedWithoutAssistant {
                kind: Default::default(),
                leaf_id: leaf,
            })
        }
        (PublicOutcomeKind::Failed, Some((final_entry_id, final_message))) => {
            RunOutcome::FailedWithAssistant(Box::new(RunFailedWithAssistant {
                kind: Default::default(),
                leaf_id: leaf,
                final_entry_id,
                final_message,
                error: error.expect("failed terminal error"),
            }))
        }
        (PublicOutcomeKind::Failed, None) => {
            RunOutcome::FailedWithoutAssistant(RunFailedWithoutAssistant {
                kind: Default::default(),
                leaf_id: leaf,
                error: error.expect("failed terminal error"),
            })
        }
        _ => return Err(fault("external terminal outcome invalid")),
    };
    Ok(OperationResult::Run(Box::new(outcome)))
}

#[cfg(test)]
mod activity_tests {
    use super::*;
    use std::sync::Mutex;
    fn lane() -> Arc<LaneControl> {
        let cases: serde_json::Value = serde_json::from_str(include_str!(
            "../../../conformance/harness/r1-r4/core/states.json"
        ))
        .unwrap();
        let roots = &cases[0]["roots"];
        let h = crate::restore::HydratedLane {
            roots: pi_harness_core::CurrentRoots {
                lane: LaneId::new("main").unwrap(),
                configuration: serde_json::from_value(roots["configuration"].clone()).unwrap(),
                lane_state: serde_json::from_value(roots["lane_state"].clone()).unwrap(),
                leaf: serde_json::from_value(roots["leaf"].clone()).unwrap(),
                operation_meta: None,
                operation_state: None,
            },
            entries: Default::default(),
            registers: Default::default(),
            current: None,
        };
        let admission = crate::scheduler::Admission::new();
        Arc::new(LaneControl {
            name: LaneId::new("main").unwrap(),
            changed: Mutex::new(CancellationToken::new()),
            driving: Mutex::new(true),
            activity: Mutex::new(()),
            generation: Mutex::new(Arc::new(())),
            active: Mutex::new(None),
            restored: std::sync::atomic::AtomicBool::new(false),
            reservation: Mutex::new(Some(OperationId::new("old").unwrap())),
            streaming: Mutex::new(None),
            running_tools: Mutex::new(Default::default()),
            line: crate::scheduler::MutationLine::new(admission.clone()),
            gate: crate::manual_gate::ManualGate::new(admission, DriveMode::Automatic),
            hydrated: Mutex::new(h),
        })
    }
    use pi_ai_core::CancellationToken;
    #[test]
    fn drive_release_cannot_expose_idle_before_its_metadata_is_clear() {
        let lane = lane();
        let blocked = lane.active.lock().unwrap();
        let other = lane.clone();
        let (started, receive) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            started.send(()).unwrap();
            drop(DriveGuard::new(other));
        });
        receive.recv().unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if !*lane.driving.lock().unwrap() || lane.activity.try_lock().is_err() {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "drop did not reach controlled boundary"
            );
            std::thread::yield_now();
        }
        let still_owned = *lane.driving.lock().unwrap();
        drop(blocked);
        worker.join().unwrap();
        assert!(
            still_owned,
            "old guard exposed idle while it could still erase the next registration"
        );
    }
    struct InertDrive;
    impl crate::abort::ActiveDrive for InertDrive {
        fn register_cancel(
            &self,
            _: &crate::restore::HydratedLane,
        ) -> Result<(), HarnessRuntimeError> {
            Ok(())
        }
        fn abort(
            self: Arc<Self>,
            _: crate::restore::HydratedLane,
        ) -> BoxFuture<'static, Result<(), HarnessRuntimeError>> {
            Box::pin(async { Ok(()) })
        }
    }
    #[test]
    fn reserve_publishes_matching_control_before_exposing_driving() {
        let lane = lane();
        *lane.driving.lock().unwrap() = false;
        *lane.reservation.lock().unwrap() = None;
        let drive: Arc<dyn crate::abort::ActiveDrive> = Arc::new(InertDrive);
        let weak = Arc::downgrade(&drive);
        let blocked = lane.active.lock().unwrap();
        let target = lane.clone();
        let worker = std::thread::spawn(move || {
            target.reserve_drive(OperationId::new("new").unwrap(), weak)
        });
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while lane.activity.try_lock().is_ok() {
            assert!(std::time::Instant::now() < deadline);
            std::thread::yield_now();
        }
        assert!(
            !*lane.driving.lock().unwrap(),
            "reservation exposed without matching control"
        );
        drop(blocked);
        assert!(worker.join().unwrap());
        assert!(*lane.driving.lock().unwrap());
        assert_eq!(
            lane.active.lock().unwrap().as_ref().unwrap().0.as_str(),
            "new"
        );
    }
    #[test]
    fn obsolete_guard_cannot_clear_a_new_generation() {
        let lane = lane();
        let old = DriveGuard::new(lane.clone());
        *lane.reservation.lock().unwrap() = Some(OperationId::new("new").unwrap());
        drop(old);
        assert!(*lane.driving.lock().unwrap());
        assert_eq!(
            lane.reservation.lock().unwrap().as_ref().unwrap().as_str(),
            "new"
        );
    }

    #[test]
    fn obsolete_guard_cannot_clear_a_resumed_generation_of_the_same_operation() {
        let lane = lane();
        let old = DriveGuard::new(lane.clone());
        *lane.generation.lock().unwrap() = Arc::new(());
        drop(old);
        assert!(*lane.driving.lock().unwrap());
        assert_eq!(
            lane.reservation.lock().unwrap().as_ref().unwrap().as_str(),
            "old"
        );
    }
}
