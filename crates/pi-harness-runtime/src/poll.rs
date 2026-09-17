//! Captured deferred fetches and independent best-effort source cancellation.
use crate::{
    effects::RuntimeEffects,
    owner::Telemetry,
    provider::{RequestHooks, RequestProjection},
    restore::HydratedLane,
};
use pi_agent_schema::*;
use pi_agent_session::storage::RuntimeStorage;
use pi_ai_core::{CancellationToken, RequestRuntime, SafeUnsigned};
use pi_harness_core::planner::PlanningFacts;
use std::sync::Arc;
pub(crate) struct CancelTarget {
    pub plan: CancelDeferredEffectPlan<Telemetry>,
    pub configuration: LaneConfiguration,
    pub options: AgentHarnessStreamOptions,
    pub done: CancellationToken,
    pub started: bool,
}
fn source(
    h: &HydratedLane,
    id: &EntryId,
) -> Result<pi_ai_core::DeferredHandle, HarnessRuntimeError> {
    match h.entries.get(id) {
        Some(Entry::Message(e)) => match &e.message {
            AgentMessage::BuiltIn(pi_ai_core::Message::Assistant(m)) => m
                .deferred
                .clone()
                .ok_or_else(|| crate::restore::fault("deferred source handle absent")),
            _ => Err(crate::restore::fault("deferred source is not assistant")),
        },
        _ => Err(crate::restore::fault("deferred source absent")),
    }
}
impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>
    RuntimeEffects<C, S, M>
{
    pub(crate) async fn deferred_facts(
        &self,
        h: &HydratedLane,
        session_id: SessionId,
        telemetry_context: Telemetry,
        now: SafeUnsigned,
    ) -> Result<PlanningFacts<Telemetry>, HarnessRuntimeError> {
        let current = h
            .current
            .as_ref()
            .ok_or_else(|| self.lane.owner.fail("deferred operation missing"))?;
        let OperationState::Run(run) = &current.state else {
            return Err(self.lane.owner.fail("deferred operation kind invalid"));
        };
        let RunPhase::Deferred(p) = &run.phase else {
            return Err(self.lane.owner.fail("deferred phase invalid"));
        };
        let (configuration, base, poll) = match &p.deferred {
            Deferred::Suspended {
                configuration,
                stream_options,
                poll,
                ..
            } => (configuration, stream_options, poll.get().checked_add(1)),
            Deferred::EffectPending {
                configuration,
                stream_options,
                poll,
                ..
            } => (configuration, stream_options, Some(poll.get())),
        };
        let cancelled = matches!(run.control, Control::CancelRequested(_));
        let pending = PendingDeferred::try_from(p.deferred.clone()).ok();
        let live = pending.as_ref().is_some_and(|pending| {
            self.running
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .values()
                .any(|v| matches!(v,EffectPlan::Deferred(p) if &p.deferred==pending))
        });
        let synthetic = if cancelled && !live {
            pending
                .as_ref()
                .map(|p| {
                    self.synthetic_deferred(
                        p,
                        now,
                        self.deferred_source_api(h, &p.source_entry_id)?,
                    )
                    .map(Box::new)
                })
                .transpose()?
        } else {
            None
        };
        let request =
            if !cancelled && !live && self.poll_permit.load(std::sync::atomic::Ordering::Acquire) {
                let Some(model) = self
                    .lane
                    .owner
                    .models
                    .get_model(&configuration.model.provider, &configuration.model.model_id)
                else {
                    return Ok(PlanningFacts::MissingIdentities {
                        missing: MissingIdentities {
                            tools: vec![],
                            models: vec![format!(
                                "{}/{}",
                                configuration.model.provider.as_str(),
                                configuration.model.model_id.as_str()
                            )],
                        },
                    });
                };
                let mut options = base.clone();
                options.deferred = Some(pi_ai_core::DeferredRequest::Enabled(false));
                Some(Box::new(BeforeRequestEvent {
                    lane: current.operation.lane.clone(),
                    run_id: current.operation.operation_id.clone(),
                    model,
                    step: HookRequestStep::Deferred,
                    attempt: OneBasedSafeUnsigned::new(
                        poll.ok_or_else(|| self.lane.owner.fail("deferred poll exhausted"))?,
                    )
                    .map_err(|_| self.lane.owner.fail("deferred poll exhausted"))?,
                    stream_options: options,
                }))
            } else {
                None
            };
        Ok(PlanningFacts::Deferred {
            session_id,
            telemetry_context,
            request,
            synthetic,
        })
    }
    pub(crate) fn deferred_source_api(
        &self,
        h: &HydratedLane,
        id: &EntryId,
    ) -> Result<pi_ai_core::ApiId, HarnessRuntimeError> {
        match h.entries.get(id) {
            Some(Entry::Message(e)) => match &e.message {
                AgentMessage::BuiltIn(pi_ai_core::Message::Assistant(m)) => Ok(m.api.clone()),
                _ => Err(self.lane.owner.fail("deferred source kind invalid")),
            },
            _ => Err(self.lane.owner.fail("deferred source API absent")),
        }
    }
    pub(crate) fn synthetic_deferred(
        &self,
        p: &PendingDeferred,
        now: SafeUnsigned,
        api: pi_ai_core::ApiId,
    ) -> Result<SettledAssistantMessage, HarnessRuntimeError> {
        crate::provider::synthetic_response(
            api,
            p.configuration.model.provider.clone(),
            p.configuration.model.model_id.clone(),
            now,
            true,
            "Operation aborted".into(),
        )
    }
    pub(crate) async fn fetch(
        self: &Arc<Self>,
        plan: DeferredEffectPlan<Telemetry>,
    ) -> Result<EffectOutput, HarnessRuntimeError> {
        let binding = self
            .request_bindings
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&plan.key)
            .cloned()
            .ok_or_else(|| self.lane.owner.fail("deferred request binding absent"))?;
        let model = binding.model.clone();
        let (captured, handle) = {
            let _line = self.lane.lane.line.acquire().await?;
            let h = self.lane.load().await?;
            if !crate::provider::current_request(&h,&plan.key,&RequestProjection::Deferred(plan.deferred.clone())) || h.current.as_ref().is_some_and(|c|matches!(&c.state,OperationState::Run(r) if matches!(r.control,Control::CancelRequested(_)))){return Ok(EffectOutput::NotStarted{key:plan.key});}
            let handle = source(&h, &plan.deferred.source_entry_id)?;
            let captured = self
                .lane
                .owner
                .models
                .capture_text_dispatch(&binding.registration, model.clone());
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
                    timestamp: SafeUnsigned::new(self.lane.owner.clock.now_unix_ms())
                        .map_err(|_| self.lane.owner.fail("poll clock invalid"))?,
                };
                let previous = self
                    .lane
                    .lane
                    .streaming
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .replace((plan.deferred.response_entry_id.clone(), draft.clone()));
                drop(previous);
                self.events().enqueue(
                    HarnessEventType::MessageStart,
                    &plan.key.lane,
                    HarnessEvent::MessageStart(Box::new(MessageStartEvent {
                        event_type: Default::default(),
                        lane: plan.key.lane.clone(),
                        recovery: self.recovery.then_some(pi_ai_core::LiteralTrue),
                        run_id: Some(plan.key.operation_id.clone()),
                        message: LiveAgentMessage::Assistant(Box::new(draft)),
                    })),
                );
            }
            (captured, handle)
        };
        let captured = match captured {
            Ok(c) => c,
            Err(
                pi_ai_core::CaptureDispatchError::MissingRegistration
                | pi_ai_core::CaptureDispatchError::ReplacedRegistration,
            ) => {
                return Ok(EffectOutput::Deferred {
                    key: plan.key,
                    message: crate::provider::synthetic_response(
                        model.api.clone(),
                        model.provider.clone(),
                        model.id.clone(),
                        SafeUnsigned::new(self.lane.owner.clock.now_unix_ms())
                            .map_err(|_| self.lane.owner.fail("deferred clock failure"))?,
                        false,
                        "Provider binding is missing or replaced".into(),
                    )?,
                });
            }
            Err(_) => return Err(self.lane.owner.fail("deferred captured pairing invalid")),
        };
        self.events().flush();
        let span = crate::telemetry::Span::start(
            plan.telemetry_context.clone(),
            "pi.ai.request",
            pi_telemetry::SpanAttributes::from([
                (
                    "pi.ai.operation".into(),
                    crate::telemetry::string("fetch_deferred"),
                ),
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
            ]),
        );
        let runtime = RequestRuntime {
            cancellation: self.cancellation.clone(),
            telemetry: span.clone(),
            clock: self.lane.owner.clock.clone(),
            spawner: Arc::new(self.lane.owner.effects.clone()),
        };
        let message = crate::request::run_deferred_request(
            crate::request::DeferredRequestInput {
                models: self.lane.owner.models.clone(),
                model,
                handle,
                options: plan.stream_options.clone(),
                runtime,
                lane: plan.key.lane.clone(),
                run_id: plan.key.operation_id.clone(),
                callbacks: Arc::new(RequestHooks {
                    fx: self.clone(),
                    telemetry: span.clone(),
                    key: plan.key.clone(),
                    projection: RequestProjection::Deferred(plan.deferred.clone()),
                }),
            },
            captured,
        )
        .await?;
        span.outcome(
            "pi.ai.request.outcome",
            if matches!(
                message.as_message().stop_reason,
                pi_ai_core::StopReason::Error | pi_ai_core::StopReason::Aborted
            ) {
                "failed"
            } else {
                "succeeded"
            },
            matches!(
                message.as_message().stop_reason,
                pi_ai_core::StopReason::Error
            ),
        );
        {
            let line = self.lane.lane.line.acquire().await?;
            let h = self.lane.load().await?;
            if crate::provider::current_request(
                &h,
                &plan.key,
                &RequestProjection::Deferred(plan.deferred.clone()),
            ) {
                let old = self
                    .lane
                    .lane
                    .streaming
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .replace((
                        plan.deferred.response_entry_id.clone(),
                        message.as_message().clone(),
                    ));
                drop(old);
                self.events().enqueue(
                    HarnessEventType::MessageEnd,
                    &plan.key.lane,
                    HarnessEvent::MessageEnd(Box::new(MessageEndEvent {
                        event_type: Default::default(),
                        lane: plan.key.lane.clone(),
                        recovery: self.recovery.then_some(pi_ai_core::LiteralTrue),
                        run_id: Some(plan.key.operation_id.clone()),
                        message: AgentMessage::BuiltIn(pi_ai_core::Message::Assistant(Box::new(
                            message.as_message().clone(),
                        ))),
                        entry_id: Some(plan.deferred.response_entry_id.clone()),
                    })),
                );
            }
            drop(line);
        }
        self.events().flush();
        Ok(EffectOutput::Deferred {
            key: plan.key,
            message,
        })
    }
    pub(crate) fn register_cancel_target(
        &self,
        h: &HydratedLane,
    ) -> Result<(), HarnessRuntimeError> {
        let Some(current) = &h.current else {
            return Ok(());
        };
        let OperationState::Run(run) = &current.state else {
            return Ok(());
        };
        let RunPhase::Deferred(p) = &run.phase else {
            return Ok(());
        };
        let (step, id, poll, configuration, options) = match &p.deferred {
            Deferred::Suspended {
                step_id,
                source_entry_id,
                poll,
                configuration,
                stream_options,
            } => (
                step_id,
                source_entry_id,
                *poll,
                configuration,
                stream_options,
            ),
            Deferred::EffectPending {
                step_id,
                source_entry_id,
                poll,
                configuration,
                stream_options,
                ..
            } => (
                step_id,
                source_entry_id,
                SafeUnsigned::new(poll.get())
                    .map_err(|_| self.lane.owner.fail("cancel poll invalid"))?,
                configuration,
                stream_options,
            ),
        };
        let handle = source(h, id)?;
        let plan = CancelDeferredEffectPlan {
            key: EffectKey {
                session_id: self
                    .lane
                    .owner
                    .session
                    .metadata()
                    .base_metadata()
                    .id
                    .clone(),
                lane: self.lane.lane.name.clone(),
                operation_id: current.operation.operation_id.clone(),
                kind: EffectKind::CancelDeferred,
                step_or_task_id: EffectStepId::new(step.as_str())
                    .map_err(|_| self.lane.owner.fail("cancel step invalid"))?,
                logical_attempt: poll,
                item_index: None,
            },
            source_entry_id: id.clone(),
            handle,
            telemetry_context: self.trace.context(),
        };
        self.cancel_targets
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .entry(id.clone())
            .or_insert(CancelTarget {
                plan,
                configuration: configuration.clone(),
                options: options.clone(),
                done: CancellationToken::new(),
                started: false,
            });
        Ok(())
    }
    pub(crate) async fn schedule_cancel(
        self: &Arc<Self>,
        h: &HydratedLane,
    ) -> Result<(), HarnessRuntimeError> {
        self.register_cancel_target(h)?;
        let pending = {
            let mut targets = self
                .cancel_targets
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            targets
                .values_mut()
                .filter_map(|target| {
                    if target.started {
                        return None;
                    }
                    target.started = true;
                    Some((
                        target.plan.clone(),
                        target.configuration.clone(),
                        target.options.clone(),
                        target.done.clone(),
                    ))
                })
                .collect::<Vec<_>>()
        };
        for (plan, configuration, options, done) in pending {
            let fx = self.clone();
            let guard = CancelCompletion {
                done,
                fx: self.clone(),
                complete: false,
            };
            self.lane
                .owner
                .host
                .spawn(Box::pin(async move {
                    let mut guard = guard;
                    let result = fx.cancel_source(plan, configuration, options).await;
                    if let Err(error) = result
                        && fx.lane.owner.admission.check().is_ok()
                    {
                        let _ = fx.lane.owner.observe::<()>(Err(error));
                    }
                    guard.complete = true;
                }))
                .map_err(|_| self.lane.owner.fail("deferred cancellation task rejected"))?;
        }
        Ok(())
    }
    async fn cancel_source(
        &self,
        plan: CancelDeferredEffectPlan<Telemetry>,
        configuration: LaneConfiguration,
        options: AgentHarnessStreamOptions,
    ) -> Result<(), HarnessRuntimeError> {
        let active = {
            let mut permission = Box::pin(self.permission("cancel_deferred"));
            let mut retired = Box::pin(self.retired.cancelled());
            std::future::poll_fn(|cx| {
                if retired.as_mut().poll(cx).is_ready() {
                    return std::task::Poll::Ready(Ok(false));
                }
                permission.as_mut().poll(cx).map(|r| r.map(|_| true))
            })
            .await?
        };
        if !active || self.retired.is_cancelled() {
            return Ok(());
        }
        // Registry model callbacks stay outside the lane mutation line.
        let Some(registration) = self
            .lane
            .owner
            .models
            .provider_registration(&configuration.model.provider)
        else {
            return Ok(());
        };
        let model = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.lane
                .owner
                .models
                .get_model(&configuration.model.provider, &configuration.model.model_id)
        }));
        let Ok(Some(model)) = model else {
            return Ok(());
        };
        let captured = {
            let _line = self.lane.lane.line.acquire().await?;
            let h = self.lane.load().await?;
            if h.current.as_ref().is_none_or(|c|c.operation.operation_id!=plan.key.operation_id||!matches!(&c.state,OperationState::Run(r) if matches!(r.control,Control::CancelRequested(_)))){return Ok(());}
            if !self
                .cancel_targets
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .contains_key(&plan.source_entry_id)
            {
                return Ok(());
            }
            self.lane
                .owner
                .models
                .capture_text_dispatch(&registration, model.clone())
        };
        let Ok(captured) = captured else {
            return Ok(());
        };
        let span = crate::telemetry::Span::start(
            plan.telemetry_context,
            "pi.ai.request",
            pi_telemetry::SpanAttributes::from([
                (
                    "pi.ai.operation".into(),
                    crate::telemetry::string("cancel_deferred"),
                ),
                (
                    "pi.ai.provider".into(),
                    crate::telemetry::string(model.provider.as_str()),
                ),
                (
                    "pi.ai.model".into(),
                    crate::telemetry::string(model.id.as_str()),
                ),
            ]),
        );
        let token = CancellationToken::new();
        let runtime = RequestRuntime {
            cancellation: token.clone(),
            telemetry: span.clone(),
            clock: self.lane.owner.clock.clone(),
            spawner: Arc::new(self.lane.owner.effects.clone()),
        };
        let mut request = Box::pin(crate::request::cancel_deferred_request(
            crate::request::DeferredCancelInput {
                models: self.lane.owner.models.clone(),
                model,
                handle: plan.handle,
                options,
                runtime,
            },
            captured,
        ));
        let mut close = Box::pin(self.lane.owner.admission.seal_signal());
        let mut retired = Box::pin(self.retired.cancelled());
        let result = std::future::poll_fn(|cx| {
            if close.as_mut().poll(cx).is_ready() || retired.as_mut().poll(cx).is_ready() {
                token.cancel();
                return std::task::Poll::Ready(Ok(Err(pi_ai_core::AuthResolutionError::Cancelled)));
            }
            request.as_mut().poll(cx)
        })
        .await?;
        span.outcome(
            "pi.ai.request.outcome",
            if result.is_ok() {
                "succeeded"
            } else {
                "failed"
            },
            result.is_err(),
        );
        Ok(())
    }
}
struct CancelCompletion<
    C: Send + Sync + 'static,
    S: RuntimeStorage + 'static,
    M: SessionMetadataRecord,
> {
    done: CancellationToken,
    fx: Arc<RuntimeEffects<C, S, M>>,
    complete: bool,
}
impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord> Drop
    for CancelCompletion<C, S, M>
{
    fn drop(&mut self) {
        if !self.complete && self.fx.lane.owner.admission.check().is_ok() {
            let _ = self
                .fx
                .lane
                .owner
                .fail("host dropped deferred cancellation task");
        }
        self.done.cancel();
    }
}
