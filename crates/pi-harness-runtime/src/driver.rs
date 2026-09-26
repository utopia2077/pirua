//! Public acceptance and the pure action procedure.
use crate::{effects::RuntimeEffects, owner::*};
use pi_agent_schema::*;
use pi_agent_session::{backend_support::register_seq, storage::RuntimeStorage};
use pi_ai_core::{JsonValue, Message, Presence, SafeUnsigned, UserContent, UserMessage};
use pi_harness_core::{
    planner::{PlanningFacts, ProcedureAction, next_action},
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
        let weak = self
            .lane
            .origin_leases
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        let leases: Vec<_> = weak
            .into_iter()
            .filter_map(|lease| lease.upgrade())
            .filter(|lease| lease.belongs_to_drive(&self.generation))
            .collect();
        for lease in &leases {
            lease.begin_failure(true);
        }
        let removed = (|| {
            let _activity = self.lane.activity.lock().unwrap_or_else(|p| p.into_inner());
            if !Arc::ptr_eq(
                &self.generation,
                &self
                    .lane
                    .generation
                    .lock()
                    .unwrap_or_else(|p| p.into_inner()),
            ) {
                return None;
            }
            let mut reservation = self
                .lane
                .reservation
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            if reservation.as_ref() != Some(&self.operation) {
                return None;
            }
            let mut active = self.lane.active.lock().unwrap_or_else(|p| p.into_inner());
            if active.as_ref().is_some_and(|(id, _)| id != &self.operation) {
                return None;
            }
            let removed = active.take();
            reservation.take();
            *self.lane.driving.lock().unwrap_or_else(|p| p.into_inner()) = false;
            removed
        })();
        drop(removed);
        for lease in &leases {
            lease.finish_failure();
        }
        drop(leases);
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
                    operation_kind: self.reserved_kind(),
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
            let mut origin_release = crate::tools::OriginRelease::default();
            let mut current={let _line=lane.lane.line.acquire().await?;let h=lane.load().await?;let Some(current)=h.current.clone() else {
                let keys: Vec<_> = lane.owner.tool_publications.lock().unwrap_or_else(|p| p.into_inner()).keys().filter(|key| key.lane == lane.lane.name && key.session_id == lane.owner.session.metadata().base_metadata().id).cloned().collect();
                for key in keys { origin_release.append(effects.retire_lost_publication(&h, &key)); }
                return Ok(Err(ResumeExpectedError::NothingToResume(NothingToResumeError {tag:Default::default(),message:"lane has no open operation".into(),lane:lane.lane.name.clone()})));};
                if !lane.lane.reserve_drive(current.operation.operation_id.clone(),Arc::downgrade(&registration)) {return Ok(Err(ResumeExpectedError::LaneBusy(LaneBusyError {tag:Default::default(),message:"lane is already driving".into(),lane:lane.lane.name.clone(),operation_id:current.operation.operation_id.clone(),operation_kind:crate::snapshots::kind(&current)})));}
                *effects.operation_kind.lock().unwrap_or_else(|p|p.into_inner())=crate::snapshots::kind(&current);
                *effects.original_operation.lock().unwrap_or_else(|p|p.into_inner())=Some(current.operation.clone());
                current};
            let _drive=DriveGuard::new(lane.lane.clone());
            effects.wait_tool_publications(&current.operation.operation_id).await?;
            #[cfg(feature = "extensions")]
            {
                let _origin_finishers = effects.origin_return_finishers();
                let mut permits = crate::tools::PairedPermits::acquire(
                    &lane.owner.settings_line,
                    &lane.lane.line,
                ).await?;
                effects.arm_origin_consumers(&mut permits);
                let latest = match lane.load_raw().await {
                    Ok(latest) => latest,
                    Err(error) => {
                        drop(permits);
                        return Err(lane.owner.observed_unlocked::<()>(Err(error)).expect_err("Direct preflight read fault"));
                    }
                };
                let mut recovered_without_route = false;
                let mut recovered_fault = None;
                if let Some(checked) = latest.current.as_ref()
                    && checked.operation.operation_id == current.operation.operation_id
                    && let OperationState::Run(run) = &checked.state
                    && matches!(run.control, Control::Running(_))
                    && let RunPhase::Tools(tools) = &run.phase
                {
                    let step = EffectStepId::new(tools.batch.turn_id.as_str())
                        .map_err(|_| crate::restore::fault("recovered echo turn invalid"))?;
                    for call in &tools.batch.calls {
                        let ToolCallState::EffectPending {
                            source_index,
                            ..
                        } = call else { continue };
                        let key = EffectKey {
                            session_id: lane.owner.session.metadata().base_metadata().id.clone(),
                            lane: lane.lane.name.clone(),
                            operation_id: checked.operation.operation_id.clone(),
                            kind: EffectKind::Tool,
                            step_or_task_id: step.clone(),
                            logical_attempt: SafeUnsigned::new(0).expect("zero safe unsigned"),
                            item_index: Some(*source_index),
                        };
                        let intents = lane.owner.recovered_echo.lock().unwrap_or_else(|p| p.into_inner());
                        let Some(intent) = intents.get(&key) else { continue };
                        match effects.live_recovered_arguments(&latest, &key, &intent.original) {
                            Ok(Some(args)) if args.key == intent.arguments.key
                                && args.value == intent.arguments.value
                                && args.seq == intent.arguments.seq => {
                                if intent.original.replay == Some(ToolReplay::Safe) && !lane.owner.tools.lock().unwrap_or_else(|p| p.into_inner())
                                    .has_recovered_echo_replacement(&intent.original) {
                                    recovered_without_route = true;
                                }
                            }
                            Ok(Some(_)) | Ok(None) => {
                                recovered_fault = Some(crate::restore::fault("recovered echo arguments changed"));
                                break;
                            }
                            Err(error) => { recovered_fault = Some(error); break; }
                        }
                    }
                }
                if let Some(error) = recovered_fault {
                    drop(permits);
                    return Err(lane.owner.observed_unlocked::<()>(Err(error)).expect_err("recovered echo fault"));
                }
                if recovered_without_route {
                    let returned = effects.release_recoverable_drive(&latest, &mut origin_release);
                    if let Err(error) = returned {
                        drop(permits);
                        return Err(lane.owner.observed_unlocked::<()>(Err(error)).expect_err("Direct return fault"));
                    }
                    permits.release();
                    return Err(HarnessRuntimeError::PeerLost);
                }
                if let Some(checked) = latest.current.as_ref()
                    && checked.operation.operation_id == current.operation.operation_id
                    && !lane.owner.tools.lock().unwrap_or_else(|p| p.into_inner()).has_ready_echo()
                    && safe_routed_pending_without_route(
                        checked,
                        &lane.owner.session.metadata().base_metadata().id,
                        &lane.lane.name,
                        &lane.owner.routed_pending.lock().unwrap_or_else(|p| p.into_inner()),
                    )
                {
                    let returned = effects.release_recoverable_drive(&latest, &mut origin_release);
                    if let Err(error) = returned {
                        drop(permits);
                        return Err(lane.owner.observed_unlocked::<()>(Err(error)).expect_err("Direct return fault"));
                    }
                    permits.release();
                    return Err(HarnessRuntimeError::PeerLost);
                }
                permits.release();
            }
            if let OperationState::Run(run)=&current.state && matches!(run.control,Control::Running(_)) && let RunPhase::Deferred(p)=&run.phase {
                let configuration=match &p.deferred {Deferred::Suspended{configuration,..}|Deferred::EffectPending{configuration,..}=>configuration};
                let missing=lane.missing(configuration);
                let precheck_line=lane.lane.line.acquire().await?;
                let latest=lane.load().await?.current;
                match latest {Some(latest) if latest.operation.operation_id==current.operation.operation_id=>current=latest,_=>{drop(precheck_line);let result=externally_completed(&effects,&current.operation.operation_id).await?;return Ok(Ok(resume_outcome(result,&current.operation.operation_id)?));}}
                if matches!(&current.state,OperationState::Run(r) if matches!(r.control,Control::Running(_))) && (!missing.models.is_empty()||!missing.tools.is_empty()) {
                    // Rejection and relinquishing this drive are one lane boundary.
                    // A later abort must select its own reconciliation drive.
                    let origin_release = effects.stage_release_drive();
                    drop(precheck_line);
                    drop(origin_release);
                    lane.lane.announce();
                    return Ok(Err(ResumeExpectedError::MissingIdentities(MissingIdentitiesError{tag:Default::default(),message:"captured identities are missing".into(),lane:lane.lane.name.clone(),tools:missing.tools,models:missing.models})));
                }
            }
            while let Some(configuration)=lane.resume_configuration(&current){
                let missing=lane.missing(&configuration);let line=lane.lane.line.acquire().await?;
                let latest=lane.load().await?.current;
                match latest{Some(latest) if latest.operation.operation_id==current.operation.operation_id=>current=latest,_=>{drop(line);let result=externally_completed(&effects,&current.operation.operation_id).await?;return Ok(Ok(resume_outcome(result,&current.operation.operation_id)?));}}
                if lane.resume_configuration(&current).as_ref()!=Some(&configuration){drop(line);continue}
                if !missing.models.is_empty()||!missing.tools.is_empty(){let origin_release=effects.stage_release_drive();drop(line);drop(origin_release);lane.lane.announce();return Ok(Err(ResumeExpectedError::MissingIdentities(MissingIdentitiesError{tag:Default::default(),message:"captured identities are missing".into(),lane:lane.lane.name.clone(),tools:missing.tools,models:missing.models})))}
                break
            }
            lane.owner.suspensions.lock().unwrap_or_else(|p|p.into_inner()).remove(&lane.lane.name);
            lane.lane.restored.store(false,std::sync::atomic::Ordering::Release);
            effects.poll_permit.store(true,std::sync::atomic::Ordering::Release);
            effects.trace.begin_kind(&lane.owner.session.metadata().base_metadata().id,&lane.lane.name,&current.operation.operation_id,recovery,crate::snapshots::kind(&current));
            let cancelled=pi_harness_core::structural::is_cancelled(&current);
            if cancelled {let h=lane.load().await?;effects.schedule_cancel(&h).await?;effects.cancellation.cancel();}
            if let OperationIntent::Run(intent)=&current.operation.intent {
                let entries=lane.owner.session.get_entries(&intent.prompt_entry_ids).await.map_err(|_|lane.owner.fail("resume prompt hydration failed"))?;
                let prompt=intent.prompt_entry_ids.iter().map(|id|match entries.get(id) {Some(Entry::Message(entry))=>Ok(entry.message.clone()),_=>Err(lane.owner.fail("resume prompt entry missing"))}).collect::<Result<Vec<_>,_>>()?;
                effects.hook(HookEvent::BeforeResume(BeforeResumeEvent::Run(BeforeResumeRunEvent {lane:lane.lane.name.clone(),run_id:current.operation.operation_id.clone(),kind:Default::default(),prompt,system_prompt_override:intent.system_prompt_override.clone(),resume_data:intent.resume_data.clone().map_or(Presence::Missing,|data|Presence::Value(JsonValue::Object(data)))})),true,None).await?;
            }
            if let OperationIntent::Compaction(intent)=&current.operation.intent{effects.hook(HookEvent::BeforeResume(BeforeResumeEvent::Compaction(BeforeResumeCompactionEvent{lane:lane.lane.name.clone(),run_id:current.operation.operation_id.clone(),kind:Default::default(),source_leaf_id:current.operation.source_leaf_id.clone(),custom_instructions:intent.custom_instructions.clone(),resume_data:Presence::Missing})),true,None).await?;}
            if let OperationIntent::Navigation(intent)=&current.operation.intent{effects.hook(HookEvent::BeforeResume(BeforeResumeEvent::Navigation(BeforeResumeNavigationEvent{lane:lane.lane.name.clone(),run_id:current.operation.operation_id.clone(),kind:Default::default(),source_leaf_id:current.operation.source_leaf_id.clone(),target_id:intent.target_id.clone(),summarize:intent.summarize,label:intent.label.clone(),custom_instructions:intent.custom_instructions.clone(),resume_data:Presence::Missing})),true,None).await?;}
            effects.events().enqueue(HarnessEventType::RunResume,&lane.lane.name,HarnessEvent::RunResume(RunResumeEvent {event_type:Default::default(),lane:lane.lane.name.clone(),recovery:recovery.then_some(pi_ai_core::LiteralTrue),run_id:current.operation.operation_id.clone()}));effects.events().flush();
            effects.resume_structure(&current);
            effects.turn_start(&current);effects.events().flush();
            let result=drive(effects.clone(),current.operation.operation_id.clone()).await?;effects.trace.finish(&result);
            Ok(Ok(resume_outcome(result,&current.operation.operation_id)?))
        })).await
    }
}

#[cfg(feature = "extensions")]
fn safe_routed_pending_without_route(
    current: &CurrentOperation,
    session: &SessionId,
    lane: &LaneId,
    known: &std::collections::BTreeSet<EffectKey>,
) -> bool {
    let OperationState::Run(run) = &current.state else {
        return false;
    };
    if !matches!(run.control, Control::Running(_)) {
        return false;
    }
    let RunPhase::Tools(tools) = &run.phase else {
        return false;
    };
    tools.batch.calls.iter().any(|call| {
        let ToolCallState::EffectPending {
            source_index,
            replay: ToolReplay::Safe,
            ..
        } = call
        else {
            return false;
        };
        let Ok(step_or_task_id) = EffectStepId::new(tools.batch.turn_id.as_str()) else {
            return false;
        };
        known.contains(&EffectKey {
            session_id: session.clone(),
            lane: lane.clone(),
            operation_id: current.operation.operation_id.clone(),
            kind: EffectKind::Tool,
            step_or_task_id,
            logical_attempt: SafeUnsigned::new(0).expect("zero safe unsigned"),
            item_index: Some(*source_index),
        })
    })
}
fn resume_outcome(
    result: OperationResult,
    operation: &OperationId,
) -> Result<ResumeOutcome, HarnessRuntimeError> {
    let (kind, value) = match result {
        OperationResult::Run(outcome) => ("run", serde_json::to_value(outcome)),
        OperationResult::Compaction(outcome) => ("compaction", serde_json::to_value(outcome)),
        OperationResult::Navigation(outcome) => ("navigation", serde_json::to_value(outcome)),
    };
    let mut value = value.map_err(|_| crate::restore::fault("resume output conversion failed"))?;
    let map = value
        .as_object_mut()
        .ok_or_else(|| crate::restore::fault("resume output shape invalid"))?;
    map.insert("operation".into(), serde_json::Value::String(kind.into()));
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
    let origin = if let EffectPlan::Tool(tool) = &plan {
        fx.wait_tool_publications(&tool.key.operation_id).await?;
        fx.origin_consumers
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&tool.key)
            .cloned()
    } else {
        None
    };
    if let Some(origin) = &origin {
        origin.transfer_to_work();
    }
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
        origin: origin.clone(),
    };
    fx.lane
        .owner
        .host
        .spawn(Box::pin(async move {
            let result = running.run(plan).await;
            if let Some(origin) = &origin {
                origin.return_to_drive();
            }
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
        fx.wait_tool_publications(&operation_id).await?;
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
        let terminal_source = fx
            .terminal_candidate
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .map(|c| c.source.clone());
        let action = if let Some(source) = terminal_source {
            ProcedureAction::Reconcile(Box::new(source))
        } else if let Some(key) = immediate_key.take() {
            ProcedureAction::Existing(Action::AwaitEffect(key))
        } else {
            let structural_missing = crate::structural::dispatch_configuration(&current)
                .map(|c| fx.lane.missing(c))
                .filter(|m| !m.models.is_empty() || !m.tools.is_empty());
            let facts = if let Some(missing) = structural_missing {
                PlanningFacts::MissingIdentities { missing }
            } else if let Some(facts) = fx.threshold_facts(&h).await? {
                facts
            } else if pi_harness_core::inbox::select(&current)
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
                let dispatch = fx
                    .applied_dispatch
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .take();
                if let Some(dispatch) = dispatch {
                    immediate_key = Some(dispatch.key().clone());
                    start(&fx, dispatch, &mut pending).await?;
                }
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
                    EffectPlan::Hook(_) | EffectPlan::ToolClearance(_) | EffectPlan::Summary(_)
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
                    if matches!(slot.plan, EffectPlan::Tool(_)) {
                        fx.lane.load_raw().await?
                    } else {
                        fx.lane.load().await?
                    }
                };
                if latest
                    .current
                    .as_ref()
                    .is_none_or(|current| current.operation.operation_id != operation_id)
                {
                    return externally_completed(&fx, &operation_id).await;
                }
                let output = slot
                    .output
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .take()
                    .ok_or_else(|| fx.lane.owner.fail("effect completed without output"))?;
                #[cfg(feature = "extensions")]
                if matches!(&slot.plan, EffectPlan::Tool(_)) {
                    if let (EffectPlan::Tool(plan), Ok(EffectOutput::NotStarted { .. })) =
                        (&slot.plan, &output)
                    {
                        fx.classify_skipped_routed_abort(plan).await?;
                    }
                    // An independent owner lifecycle error keeps its original
                    // priority over any private route completion.
                    if matches!(
                        &output,
                        Err(HarnessRuntimeError::Fault(_) | HarnessRuntimeError::Closed(_))
                    ) {
                        return Err(output.expect_err("checked error"));
                    }
                    if let Some(disposition) = fx.routed_disposition(&key) {
                        match disposition {
                            crate::tools::RoutedDisposition::PeerLost => {
                                fx.cleanup_routed(&key, false).await?;
                                let _origin_finishers = fx.origin_return_finishers();
                                let mut release = crate::tools::OriginRelease::default();
                                let mut pair = crate::tools::PairedPermits::acquire(
                                    &fx.lane.owner.settings_line,
                                    &fx.lane.lane.line,
                                )
                                .await?;
                                fx.arm_origin_consumers(&mut pair);
                                let returned = fx.lane.load_raw().await.and_then(|latest| {
                                    fx.release_recoverable_drive(&latest, &mut release)
                                });
                                if let Err(error) = returned {
                                    drop(pair);
                                    return Err(fx
                                        .lane
                                        .owner
                                        .observed_unlocked::<()>(Err(error))
                                        .expect_err("Direct return fault"));
                                }
                                pair.release();
                                drop(release);
                                return Err(HarnessRuntimeError::PeerLost);
                            }
                            crate::tools::RoutedDisposition::HostFault => {
                                fx.cleanup_routed(&key, false).await?;
                                return Err(fx.lane.owner.fail("extension tool host fault"));
                            }
                            crate::tools::RoutedDisposition::UnadmittedLocalAbort => {
                                fx.cleanup_routed(&key, false).await?;
                                continue;
                            }
                            crate::tools::RoutedDisposition::NoLongerExpected => {
                                fx.cleanup_routed(&key, false).await?;
                                let latest = {
                                    let _line = fx.lane.lane.line.acquire().await?;
                                    fx.lane.load_raw().await?
                                };
                                if latest.current.as_ref().is_some_and(|current| {
                                    current.operation.operation_id == operation_id
                                }) {
                                    continue;
                                }
                                return externally_completed(&fx, &operation_id).await;
                            }
                        }
                    }
                }
                let output = output?;
                let output = match output {
                    EffectOutput::Summary { key, outcome } => {
                        SettlementOutput::Summary { key, outcome }
                    }
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
                            fx.lane.load_raw().await?
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
                    EffectOutput::CancelDeferred { .. } => {
                        return Err(fx
                            .lane
                            .owner
                            .fail("cancel output must use its dedicated owner"));
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
                let origin_release = fx.stage_release_drive();
                drop(line);
                drop(origin_release);
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
                    return Ok(fx
                        .terminal_result
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .take()
                        .unwrap_or(result));
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
                    return Ok(fx
                        .terminal_result
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .take()
                        .unwrap_or(terminal));
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
    origin: Option<Arc<dyn crate::tools::OriginFence>>,
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
        if !self.completed
            && let Some(origin) = &self.origin
        {
            origin.begin_failure(false);
        }
        if !self.completed && self.fx.lane.owner.admission.check().is_ok() {
            let _ = self
                .fx
                .lane
                .owner
                .fail("host dropped an effect completion task");
        }
        if !self.completed
            && let Some(origin) = &self.origin
        {
            origin.finish_failure();
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
    fx: &Arc<RuntimeEffects<C, S, M>>,
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
            let original = fx
                .original_operation
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .clone();
            let result = read_external_result(&fx.lane, operation, original.as_ref()).await;
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
    #[cfg(feature = "extensions")]
    {
        fx.cleanup_all_routed().await?;
        fx.lane
            .owner
            .routed_pending
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .retain(|key| {
                key.operation_id != *operation
                    || key.lane != fx.lane.lane.name
                    || key.session_id != fx.lane.owner.session.metadata().base_metadata().id
            });
        fx.lane
            .owner
            .recovered_echo
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .retain(|key, _| {
                key.operation_id != *operation
                    || key.lane != fx.lane.lane.name
                    || key.session_id != fx.lane.owner.session.metadata().base_metadata().id
            });
    }
    fx.release_protected_drive();
    if matches!(result, OperationResult::Run(_)) {
        fx.events().enqueue(
            HarnessEventType::RunEnd,
            &fx.lane.lane.name,
            crate::publication::run_end(
                fx.lane.lane.name.clone(),
                operation.clone(),
                result.clone(),
            )?,
        );
    } else {
        let original = fx
            .original_operation
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
            .ok_or_else(|| fx.lane.owner.fail("external structural original missing"))?;
        let (kind, event) = crate::structural::end_event(
            &original,
            CompactionEventReason::Manual,
            result.clone(),
            fx.recovery,
        )?;
        fx.events().enqueue(kind, &fx.lane.lane.name, event);
    }
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
    original: Option<&Operation>,
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
    if let Some(original) = original
        && !matches!(original.intent, OperationIntent::Run(_))
    {
        return lane.structural_external(original, value).await;
    }
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
            origin_leases: Mutex::new(Vec::new()),
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
    #[cfg(feature = "extensions")]
    #[test]
    fn recovered_registration_checks_activity_before_driving_during_drop() {
        let lane = lane();
        let guard = DriveGuard::new(lane.clone());
        let driving = lane.driving.lock().unwrap();
        let (started, receive) = std::sync::mpsc::channel();
        let other = lane.clone();
        let registration = std::thread::spawn(move || {
            started.send(()).unwrap();
            other.recovered_intent_lane_busy()
        });
        receive.recv().unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if lane.activity.try_lock().is_err() {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "registration waited on driving before acquiring activity"
            );
            std::thread::yield_now();
        }
        let release = std::thread::spawn(move || drop(guard));
        drop(driving);
        assert!(registration.join().unwrap(), "active drive remains busy");
        release.join().unwrap();
        assert!(
            !*lane.driving.lock().unwrap(),
            "Drop releases drive after registration"
        );
        assert!(
            !lane.recovered_intent_lane_busy(),
            "lane is available after Drop"
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
