//! Context and hook binding for the real Models request adapter.
use crate::{effects::RuntimeEffects, owner::Telemetry};
use pi_agent_schema::*;
use pi_agent_session::{context::ContextItem, storage::RuntimeStorage};
use pi_ai_core::{AssistantMessageEvent, RequestRuntime, SafeUnsigned};
use std::sync::Arc;

/// Construct reconciliation data only from explicitly supplied captured identity.
/// This function deliberately supplies no policy for an unavailable durable API.
pub fn synthetic_response(
    api: pi_ai_core::ApiId,
    provider: pi_ai_core::ProviderId,
    model: pi_ai_core::ModelId,
    timestamp: SafeUnsigned,
    aborted: bool,
    message: String,
) -> Result<SettledAssistantMessage, HarnessRuntimeError> {
    SettledAssistantMessage::new(pi_ai_core::AssistantMessage {
        role: Default::default(),
        content: vec![],
        api,
        provider,
        model,
        response_model: None,
        response_id: None,
        diagnostics: None,
        usage: pi_ai_core::Usage::zero(),
        stop_reason: if aborted {
            pi_ai_core::StopReason::Aborted
        } else {
            pi_ai_core::StopReason::Error
        },
        deferred: None,
        error_message: Some(message),
        raw_stop_reason: None,
        end_turn: None,
        timestamp,
    })
    .map_err(|_| crate::restore::fault("synthetic response data is invalid"))
}
impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>
    RuntimeEffects<C, S, M>
{
    /// Recovery uses only captured state and a matching process-local API, never
    /// a fresh registry lookup or the constructor's current model selection.
    pub(crate) fn synthetic_generation(
        &self,
        key: &EffectKey,
        context: &GenerationContext,
        now: SafeUnsigned,
        aborted: bool,
    ) -> Result<SettledAssistantMessage, HarnessRuntimeError> {
        let model = &context.configuration.model;
        let api = self
            .request_bindings
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(key)
            .filter(|binding| {
                binding.model.provider == model.provider && binding.model.id == model.model_id
            })
            .map(|binding| binding.model.api.clone())
            .unwrap_or_else(|| pi_ai_core::ApiId::new("unknown").expect("fixed API identity"));
        synthetic_response(
            api,
            model.provider.clone(),
            model.model_id.clone(),
            now,
            aborted,
            if aborted {
                "Operation aborted"
            } else {
                "Generation outcome unknown at attempt cap"
            }
            .into(),
        )
    }
    pub(crate) async fn request(
        self: &Arc<Self>,
        plan: AssistantEffectPlan<Telemetry>,
    ) -> Result<EffectOutput, HarnessRuntimeError> {
        let candidate = self
            .request_bindings
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&plan.key)
            .cloned();
        let candidate = candidate.ok_or_else(|| {
            self.lane
                .owner
                .fail("post-intent request binding unavailable")
        })?;
        let model = candidate.model.clone();
        let h = self.lane.load().await?;
        let current = h
            .current
            .ok_or_else(|| self.lane.owner.fail("request operation missing"))?;
        let override_prompt = match &current.operation.intent {
            OperationIntent::Run(intent) => intent.system_prompt_override.as_deref(),
            _ => None,
        };
        let system_prompt = self.system_prompt(override_prompt).await?;
        let projectors = self
            .lane
            .owner
            .projectors
            .iter()
            .map(|(key, value)| (key.clone(), value.as_ref()))
            .collect();
        let items = self
            .lane
            .owner
            .session
            .lane_view(self.lane.lane.name.clone())
            .context_items(&projectors)
            .await
            .map_err(|_| {
                self.lane
                    .owner
                    .fail("request context materialization failed")
            })?;
        let mut messages = Vec::new();
        for item in items {
            messages.push(match item {
                ContextItem::Message(message) => message,
                ContextItem::Summary(summary) => self.lane.owner.summaries.render(summary).await?,
            });
        }
        let transformed = self
            .hook_with_key(
                HookEvent::TransformContext(TransformContextEvent {
                    lane: plan.key.lane.clone(),
                    run_id: plan.key.operation_id.clone(),
                    messages: messages.clone(),
                }),
                true,
                None,
                &plan.key,
            )
            .await?;
        {
            let _line = self.lane.lane.line.acquire().await?;
            let h = self.lane.load().await?;
            if h.current
                .as_ref()
                .is_none_or(|current| current.operation.operation_id != plan.key.operation_id)
            {
                // No request has dispatched. Let the owning drive validate the
                // original terminal result before starting another callback.
                return Ok(EffectOutput::NotStarted { key: plan.key });
            }
        }
        if let Some(HookResult::TransformContext(Some(result))) = transformed.result() {
            messages = result.messages.clone();
        }
        let messages = if let Some(converter) = &self.lane.owner.converter {
            converter.convert(messages).await?
        } else {
            messages
                .into_iter()
                .filter_map(|message| match message {
                    AgentMessage::BuiltIn(message) => Some(message),
                    AgentMessage::Custom(_) => None,
                })
                .collect()
        };
        let bindings = self
            .lane
            .owner
            .tools
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        let tools = bindings
            .iter()
            .filter_map(|binding| {
                let d = binding.descriptor();
                if !plan
                    .generation
                    .context
                    .configuration
                    .active_tool_names
                    .contains(&d.name)
                {
                    return None;
                }
                Some(pi_ai_core::Tool {
                    name: d.name.clone(),
                    description: d.description.clone(),
                    parameters: d.parameters.clone(),
                    constrained_sampling: d.constrained_sampling.clone(),
                })
            })
            .collect::<Vec<_>>();
        // The full model was observed outside the lane before request intent.
        // Capture only registry-owned identity here; never call Provider::models.
        let timestamp = SafeUnsigned::new(self.lane.owner.clock.now_unix_ms())
            .map_err(|_| self.lane.owner.fail("stream draft clock range failure"))?;
        let captured = {
            let _settings = self.lane.owner.settings_line.acquire().await?;
            let _line = self.lane.lane.line.acquire().await?;
            let h = self.lane.load().await?;
            if h.current.as_ref().is_none_or(|current| current.operation.operation_id!=plan.key.operation_id||!matches!(&current.state,OperationState::Run(run) if matches!(run.control,Control::Running(_))&&matches!(&run.phase,RunPhase::Assistant(AssistantPhase {generation,..}) if PendingGeneration::try_from(generation.clone()).is_ok_and(|pending|pending==plan.generation)))) {return Ok(EffectOutput::NotStarted {key:plan.key});}
            let captured = self
                .lane
                .owner
                .models
                .capture_text_dispatch(&candidate.registration, model.clone());
            if captured.is_ok() {
                let draft = pi_ai_core::AssistantMessage {
                    role: Default::default(),
                    content: vec![],
                    api: model.api.clone(),
                    provider: model.provider.clone(),
                    model: model.id.clone(),
                    response_model: None,
                    response_id: None,
                    diagnostics: None,
                    usage: pi_ai_core::Usage::zero(),
                    stop_reason: pi_ai_core::StopReason::Pending,
                    deferred: None,
                    error_message: None,
                    raw_stop_reason: None,
                    end_turn: None,
                    timestamp,
                };
                let old = self
                    .lane
                    .lane
                    .streaming
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .replace((plan.generation.response_entry_id.clone(), draft.clone()));
                drop(old);
                self.events().enqueue(
                    HarnessEventType::MessageStart,
                    &plan.key.lane,
                    HarnessEvent::MessageStart(Box::new(MessageStartEvent {
                        event_type: Default::default(),
                        lane: plan.key.lane.clone(),
                        recovery: None,
                        run_id: Some(plan.key.operation_id.clone()),
                        message: LiveAgentMessage::Assistant(Box::new(draft)),
                    })),
                );
            }
            captured
        };
        let captured = match captured {
            Ok(captured) => captured,
            Err(
                pi_ai_core::CaptureDispatchError::MissingRegistration
                | pi_ai_core::CaptureDispatchError::ReplacedRegistration,
            ) => {
                let message = synthetic_response(
                    model.api.clone(),
                    model.provider.clone(),
                    model.id.clone(),
                    timestamp,
                    false,
                    "Provider binding is missing or replaced".into(),
                )?;
                return Ok(EffectOutput::Assistant {
                    key: plan.key,
                    message,
                });
            }
            Err(_) => return Err(self.lane.owner.fail("request registration pairing invalid")),
        };
        self.events().flush();
        let request_span = crate::telemetry::Span::start(
            plan.telemetry_context.clone(),
            "pi.ai.request",
            pi_telemetry::SpanAttributes::from([
                ("pi.ai.operation".into(), crate::telemetry::string("stream")),
                (
                    "pi.ai.provider".into(),
                    crate::telemetry::string(model.provider.as_str()),
                ),
                (
                    "pi.ai.model".into(),
                    crate::telemetry::string(model.id.as_str()),
                ),
                (
                    "pi.ai.api".into(),
                    crate::telemetry::string(model.api.as_str()),
                ),
                (
                    "pi.ai.streaming".into(),
                    pi_telemetry::AttributeValue::Boolean(true),
                ),
            ]),
        );
        let runtime = RequestRuntime {
            cancellation: self.cancellation.clone(),
            telemetry: request_span.clone(),
            clock: self.lane.owner.clock.clone(),
            spawner: Arc::new(self.lane.owner.effects.clone()),
        };
        let input = crate::request::RequestInput {
            models: self.lane.owner.models.clone(),
            model: model.clone(),
            context: pi_ai_core::Context {
                system_prompt: Some(system_prompt),
                messages,
                tools: Some(tools),
            },
            thinking: plan.generation.context.configuration.thinking_level,
            options: plan.stream_options.clone(),
            runtime,
            lane: plan.key.lane.clone(),
            run_id: plan.key.operation_id.clone(),
            session_id: plan.key.session_id.clone(),
            callbacks: Arc::new(RequestHooks {
                fx: self.clone(),
                telemetry: request_span.clone(),
                key: plan.key.clone(),
                projection: RequestProjection::Assistant(plan.generation.clone()),
            }),
        };
        #[cfg(feature = "extensions")]
        let message = if let Some(provider) = candidate.remote {
            crate::request::run_keyed_request(
                input,
                captured,
                &crate::extensions::profiles::provider::Source {
                    fx: self.clone(),
                    key: plan.key.clone(),
                    provider,
                    deferred_source: None,
                },
                None,
            )
            .await?
        } else {
            crate::request::run_captured_request(input, captured).await?
        };
        #[cfg(not(feature = "extensions"))]
        let message = crate::request::run_captured_request(input, captured).await?;
        let reason = match message.as_message().stop_reason {
            pi_ai_core::StopReason::Stop => "stop",
            pi_ai_core::StopReason::Length => "length",
            pi_ai_core::StopReason::ToolUse => "tool_use",
            pi_ai_core::StopReason::Error => "error",
            pi_ai_core::StopReason::Aborted => "aborted",
            pi_ai_core::StopReason::Deferred => "deferred",
            pi_ai_core::StopReason::Pending => "pending",
        };
        request_span.outcome(
            "pi.ai.response.stop_reason",
            reason,
            matches!(
                message.as_message().stop_reason,
                pi_ai_core::StopReason::Error
            ),
        );
        {
            let _line = self.lane.lane.line.acquire().await?;
            let h = self.lane.load().await?;
            if current_request(
                &h,
                &plan.key,
                &RequestProjection::Assistant(plan.generation.clone()),
            ) {
                let old = self
                    .lane
                    .lane
                    .streaming
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .replace((
                        plan.generation.response_entry_id.clone(),
                        message.as_message().clone(),
                    ));
                drop(old);
                self.events().enqueue(
                    HarnessEventType::MessageEnd,
                    &plan.key.lane,
                    HarnessEvent::MessageEnd(Box::new(MessageEndEvent {
                        event_type: Default::default(),
                        lane: plan.key.lane.clone(),
                        recovery: None,
                        run_id: Some(plan.key.operation_id.clone()),
                        message: AgentMessage::BuiltIn(pi_ai_core::Message::Assistant(Box::new(
                            message.as_message().clone(),
                        ))),
                        entry_id: Some(plan.generation.response_entry_id.clone()),
                    })),
                );
            }
        }
        self.events().flush();
        Ok(EffectOutput::Assistant {
            key: plan.key,
            message,
        })
    }
}
pub(crate) struct RequestHooks<C, S: RuntimeStorage, M: SessionMetadataRecord> {
    pub fx: Arc<RuntimeEffects<C, S, M>>,
    pub telemetry: Telemetry,
    pub key: EffectKey,
    pub projection: RequestProjection,
}
impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>
    crate::request::RequestCallbacks for RequestHooks<C, S, M>
{
    fn has_payload_handlers(&self) -> bool {
        !self
            .fx
            .lane
            .owner
            .hooks
            .capture(HookName::BeforePayload)
            .is_empty()
    }
    fn before_payload(
        &self,
        event: BeforePayloadEvent,
    ) -> BoxFuture<'_, Result<Option<pi_ai_core::JsonValue>, HarnessRuntimeError>> {
        Box::pin(async move {
            let result = self
                .fx
                .hook_with_telemetry_key(
                    HookEvent::BeforePayload(Box::new(event)),
                    true,
                    None,
                    self.telemetry.clone(),
                    Some(&self.key),
                )
                .await?;
            match result.result() {
                Some(HookResult::BeforePayload(result)) => {
                    Ok(result.as_ref().map(|r| r.payload.clone()))
                }
                _ => Err(self.fx.lane.owner.fail("payload hook result mismatch")),
            }
        })
    }
    fn after_response(
        &self,
        event: AfterResponseEvent,
    ) -> BoxFuture<'_, Result<SettledAssistantMessage, HarnessRuntimeError>> {
        Box::pin(async move {
            let original = event.message.clone();
            let result = self
                .fx
                .hook_with_telemetry_key(
                    HookEvent::AfterResponse(Box::new(event)),
                    true,
                    None,
                    self.telemetry.clone(),
                    Some(&self.key),
                )
                .await?;
            match result.result() {
                Some(HookResult::AfterResponse(result)) => Ok(result
                    .as_ref()
                    .and_then(|r| r.message.clone())
                    .unwrap_or(original)),
                _ => Err(self.fx.lane.owner.fail("response hook result mismatch")),
            }
        })
    }
    fn stream_event(
        &self,
        event: AssistantMessageEvent,
    ) -> BoxFuture<'_, Result<(), HarnessRuntimeError>> {
        Box::pin(async move {
            self.fx.lane.owner.admission.check()?;
            let _line = self.fx.lane.lane.line.acquire().await?;
            let h = self.fx.lane.load().await?;
            if !current_request(&h, &self.key, &self.projection) {
                return Ok(());
            }
            let partial = match &event {
                AssistantMessageEvent::Start { partial }
                | AssistantMessageEvent::TextStart { partial, .. }
                | AssistantMessageEvent::TextDelta { partial, .. }
                | AssistantMessageEvent::TextEnd { partial, .. }
                | AssistantMessageEvent::ThinkingStart { partial, .. }
                | AssistantMessageEvent::ThinkingDelta { partial, .. }
                | AssistantMessageEvent::ThinkingEnd { partial, .. }
                | AssistantMessageEvent::ToolCallStart { partial, .. }
                | AssistantMessageEvent::ToolCallDelta { partial, .. }
                | AssistantMessageEvent::ToolCallEnd { partial, .. } => partial.clone(),
                AssistantMessageEvent::Done { message, .. } => message.clone(),
                AssistantMessageEvent::Error { error, .. } => error.clone(),
            };
            let old = self
                .fx
                .lane
                .lane
                .streaming
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .replace((self.projection.response_id().clone(), *partial.clone()));
            drop(old);
            self.fx
                .events()
                .with_context(self.telemetry.clone())
                .enqueue(
                    HarnessEventType::MessageUpdate,
                    &self.key.lane,
                    HarnessEvent::MessageUpdate(Box::new(MessageUpdateEvent {
                        event_type: Default::default(),
                        lane: self.key.lane.clone(),
                        recovery: None,
                        run_id: self.key.operation_id.clone(),
                        message: LiveAgentMessage::Assistant(partial),
                        event,
                    })),
                );
            drop(_line);
            self.fx.events().flush();
            Ok(())
        })
    }
}

pub(crate) enum RequestProjection {
    Assistant(PendingGeneration),
    Deferred(PendingDeferred),
}
impl RequestProjection {
    fn response_id(&self) -> &EntryId {
        match self {
            Self::Assistant(p) => &p.response_entry_id,
            Self::Deferred(p) => &p.response_entry_id,
        }
    }
}
pub(crate) fn current_request(
    h: &crate::restore::HydratedLane,
    key: &EffectKey,
    projection: &RequestProjection,
) -> bool {
    h.current.as_ref().is_some_and(|current|current.operation.operation_id==key.operation_id && match (&current.state,projection) {
 (OperationState::Run(run),RequestProjection::Assistant(p))=>matches!(&run.phase,RunPhase::Assistant(AssistantPhase{generation,..}) if PendingGeneration::try_from(generation.clone()).is_ok_and(|current|current==*p)),
 (OperationState::Run(run),RequestProjection::Deferred(p))=>matches!(&run.phase,RunPhase::Deferred(DeferredPhase{deferred,..}) if PendingDeferred::try_from(deferred.clone()).is_ok_and(|current|current==*p)),_=>false})
}
