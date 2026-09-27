//! Live structural attempts and independently gated nested request ownership.
use crate::{
    effects::RuntimeEffects,
    owner::Telemetry,
    summary_content::{self, SummaryRequestSpec},
};
use pi_agent_schema::*;
use pi_agent_session::storage::RuntimeStorage;
use pi_ai_core::{Model, Presence, ProviderRegistration, SafeUnsigned};
use std::sync::Arc;

async fn nested_settlement_publication(
    settings: &crate::scheduler::MutationLine,
    lane: &crate::scheduler::MutationLine,
) -> Result<crate::publication::HeldPublication, HarnessRuntimeError> {
    // Keep the existing settings-then-lane admission order. This scope begins
    // after both acquisitions and owns the last commit drain handoff.
    let settings = settings.acquire().await?;
    let lane = lane.acquire().await?;
    Ok(crate::publication::HeldPublication::new(settings, lane))
}

#[derive(Clone)]
pub(crate) struct SummaryBinding {
    outer: SummaryEffectPlan<Telemetry>,
    request: SummaryRequestPlan<Telemetry>,
    model: Model,
    registration: ProviderRegistration,
    #[cfg(feature = "extensions")]
    remote: Option<crate::extensions::profiles::provider::RegisteredProvider>,
    options: AgentHarnessStreamOptions,
    spec: SummaryRequestSpec,
}
impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>
    RuntimeEffects<C, S, M>
{
    async fn summary_messages(
        &self,
        outer: &SummaryEffectPlan<Telemetry>,
        mut messages: Vec<AgentMessage>,
    ) -> Result<Vec<pi_ai_core::Message>, HarnessRuntimeError> {
        let transformed = self
            .structural_wait(self.hook_with_key(
                HookEvent::TransformContext(TransformContextEvent {
                    lane: outer.key.lane.clone(),
                    run_id: outer.key.operation_id.clone(),
                    messages: messages.clone(),
                }),
                true,
                None,
                &outer.key,
            ))
            .await?;
        if let Some(HookResult::TransformContext(Some(value))) = transformed.result() {
            messages = value.messages.clone();
        }
        if let Some(converter) = &self.lane.owner.converter {
            self.structural_wait(converter.convert(messages)).await
        } else {
            Ok(messages
                .into_iter()
                .filter_map(|m| {
                    if let AgentMessage::BuiltIn(m) = m {
                        Some(m)
                    } else {
                        None
                    }
                })
                .collect())
        }
    }
    pub(crate) async fn summary(
        self: &Arc<Self>,
        outer: SummaryEffectPlan<Telemetry>,
    ) -> Result<EffectOutput, HarnessRuntimeError> {
        let h = self.lane.load().await?;
        let Some(current) = h.current.as_ref() else {
            return Ok(EffectOutput::NotStarted { key: outer.key });
        };
        if !pi_harness_core::structural::matches(current, &outer, &outer.key.session_id) {
            return Ok(EffectOutput::NotStarted { key: outer.key });
        }
        let inputs = self.inputs(&h).await?;
        let (preparation, _) = pi_harness_core::structural::preparation(current, &inputs)
            .map_err(|_| self.lane.owner.fail("summary preparation unavailable"))?;
        let preparation = preparation.clone();
        let custom = match &current.operation.intent {
            OperationIntent::Compaction(i) => i.custom_instructions.clone(),
            OperationIntent::Navigation(i) => i.custom_instructions.clone(),
            _ => None,
        };
        let compaction = match preparation.clone() {
            DurableStructuralPreparation::Compaction {
                messages_to_summarize,
                turn_prefix_messages,
                retained_tail,
                is_split_turn,
                tokens_before,
                previous_summary,
                file_ops,
                settings,
            } => Some(CompactionPreparation {
                messages_to_summarize,
                turn_prefix_messages,
                retained_tail,
                is_split_turn,
                tokens_before,
                previous_summary,
                file_ops,
                settings,
            }),
            _ => None,
        };
        let branch = match preparation {
            DurableStructuralPreparation::BranchSummary {
                messages,
                file_ops,
                total_tokens,
            } => Some(BranchPreparation {
                messages,
                file_ops,
                total_tokens,
            }),
            _ => None,
        };
        let selections = if let Some(p) = &compaction {
            let split = p.is_split_turn && !p.turn_prefix_messages.is_empty();
            let mut v = vec![];
            if !split || !p.messages_to_summarize.is_empty() {
                v.push((false, p.messages_to_summarize.clone()))
            }
            if split {
                v.push((true, p.turn_prefix_messages.clone()))
            }
            v
        } else {
            let p = branch
                .as_ref()
                .ok_or_else(|| self.lane.owner.fail("summary kind missing"))?;
            if p.messages.is_empty() {
                vec![]
            } else {
                vec![(false, p.messages.clone())]
            }
        };
        let mut responses = vec![];
        for (index, (prefix, messages)) in selections.into_iter().enumerate() {
            let configuration = &outer.generation.context.configuration;
            let missing = self.lane.missing(configuration);
            if !missing.models.is_empty() || !missing.tools.is_empty() {
                return Ok(EffectOutput::SummarySuspended(
                    SummarySuspendedOutput::new(&outer, missing)
                        .ok_or_else(|| self.lane.owner.fail("empty summary suspension"))?,
                ));
            }
            let registration = self
                .lane
                .owner
                .models
                .provider_registration(&configuration.model.provider);
            let model = self
                .lane
                .owner
                .models
                .get_model(&configuration.model.provider, &configuration.model.model_id);
            let (Some(registration), Some(model)) = (registration, model) else {
                return Ok(EffectOutput::SummarySuspended(
                    SummarySuspendedOutput::new(
                        &outer,
                        MissingIdentities {
                            tools: vec![],
                            models: vec![format!(
                                "{}/{}",
                                configuration.model.provider.as_str(),
                                configuration.model.model_id.as_str()
                            )],
                        },
                    )
                    .ok_or_else(|| self.lane.owner.fail("summary model suspension"))?,
                ));
            };
            let converted = self.summary_messages(&outer, messages.clone()).await?;
            let spec = if let Some(p) = &compaction {
                let (history, turn) = if prefix {
                    (&[][..], converted.as_slice())
                } else {
                    (converted.as_slice(), &[][..])
                };
                summary_content::compaction_requests(
                    p,
                    history,
                    turn,
                    &model,
                    custom.as_deref(),
                    configuration.thinking_level,
                )
                .map_err(|_| self.lane.owner.fail("summary prompt construction failed"))?
                .into_iter()
                .nth(index)
                .ok_or_else(|| self.lane.owner.fail("summary request index missing"))?
            } else {
                summary_content::branch_request(
                    branch.as_ref().expect("branch preparation"),
                    &converted,
                    custom.as_deref(),
                    false,
                )
                .map_err(|_| self.lane.owner.fail("branch prompt construction failed"))?
                .ok_or_else(|| self.lane.owner.fail("nonempty branch request missing"))?
            };
            let seeded = crate::request::seed_summary_options(
                outer.generation.context.stream_options.clone(),
            );
            let before = self
                .structural_wait(self.hook_with_key(
                    HookEvent::BeforeRequest(Box::new(BeforeRequestEvent {
                        lane: outer.key.lane.clone(),
                        run_id: outer.key.operation_id.clone(),
                        model: model.clone(),
                        step: if compaction.is_some() {
                            HookRequestStep::Compaction
                        } else {
                            HookRequestStep::BranchSummary
                        },
                        attempt: outer.generation.attempt,
                        stream_options: seeded.clone(),
                    })),
                    true,
                    None,
                    &outer.key,
                ))
                .await?;
            let options = before.request_options().cloned().unwrap_or(seeded);
            let request = SummaryRequestPlan {
                task_id: outer.generation.context.task_id.clone(),
                attempt: outer.generation.attempt,
                request_index: SafeUnsigned::new(index as u64)
                    .map_err(|_| self.lane.owner.fail("summary index invalid"))?,
                usage_id: UsageId::new(self.lane.owner.ids.next(None))
                    .map_err(|_| self.lane.owner.fail("summary usage identity invalid"))?,
                configuration: configuration.clone(),
                messages,
                telemetry_context: outer.telemetry_context.clone(),
            };
            *self
                .summary_binding
                .lock()
                .unwrap_or_else(|p| p.into_inner()) = Some(SummaryBinding {
                outer: outer.clone(),
                request: request.clone(),
                #[cfg(feature = "extensions")]
                remote: self
                    .lane
                    .owner
                    .profile_providers
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .get(&model.provider)
                    .filter(|remote| {
                        remote.registration.same_registration(&registration)
                            && remote.models.contains(&model)
                    })
                    .cloned(),
                model,
                registration,
                options,
                spec,
            });
            if !self.summary_intent(&outer, &request).await? {
                return Ok(EffectOutput::NotStarted { key: outer.key });
            }
            let response = match self.run_summary_request(request.clone()).await? {
                SummaryRequestOutput::Response(r) => *r,
                SummaryRequestOutput::NotStarted => {
                    return Ok(EffectOutput::NotStarted { key: outer.key });
                }
            };
            let h = self.lane.load().await?;
            let Some(current) = h.current else {
                return Ok(EffectOutput::NotStarted { key: outer.key });
            };
            let settlement = SummaryRequestSettlement {
                current,
                task_id: request.task_id,
                attempt: request.attempt,
                request_index: request.request_index,
                usage_id: request.usage_id,
                response: response.clone(),
                telemetry_context: request.telemetry_context,
            };
            if self.settle_summary_request(settlement).await?.is_none() {
                return Ok(EffectOutput::NotStarted { key: outer.key });
            }
            let raw = response.as_message();
            if matches!(
                raw.stop_reason,
                pi_ai_core::StopReason::Error | pi_ai_core::StopReason::Aborted
            ) {
                let error = OperationError {
                    code: OperationErrorCode::new("summary_failed").expect("fixed error code"),
                    message: raw
                        .error_message
                        .clone()
                        .unwrap_or_else(|| "Summary request failed".into()),
                    details: Presence::Missing,
                };
                let retry = pi_ai_core::is_retryable_assistant_error(raw);
                return Ok(EffectOutput::Summary {
                    key: outer.key,
                    outcome: if retry {
                        SummaryAttemptOutcome::Retry(error)
                    } else {
                        SummaryAttemptOutcome::Failure(error)
                    },
                });
            }
            responses.push(response);
        }
        let semantic = if let Some(p) = compaction {
            SummarySuccessResult::Compact(
                summary_content::finish_compaction(&p, &responses)
                    .map_err(|_| self.lane.owner.fail("summary response combination failed"))?,
            )
        } else {
            SummarySuccessResult::BranchSummary(
                summary_content::finish_branch(
                    branch.as_ref().expect("branch preparation"),
                    responses.first(),
                )
                .map_err(|_| self.lane.owner.fail("branch response combination failed"))?,
            )
        };
        Ok(EffectOutput::Summary {
            key: outer.key,
            outcome: SummaryAttemptOutcome::Success(semantic),
        })
    }
    async fn summary_intent(
        &self,
        outer: &SummaryEffectPlan<Telemetry>,
        request: &SummaryRequestPlan<Telemetry>,
    ) -> Result<bool, HarnessRuntimeError> {
        loop {
            if self.retired.is_cancelled() {
                return Ok(false);
            }
            let h = self.lane.load().await?;
            let Some(current) = h.current.as_ref() else {
                return Ok(false);
            };
            let decision = pi_harness_core::structural::nested_intent_recipe(
                current,
                outer,
                request,
                &self.lane.facts(&h)?,
            )
            .map_err(|_| self.lane.owner.fail("nested summary intent invalid"))?;
            let pi_harness_core::planner::RecipeDecision::Apply(recipe) = decision else {
                return Ok(false);
            };
            let next = recipe
                .next
                .ok_or_else(|| self.lane.owner.fail("nested summary next state absent"))?;
            if self
                .commit_transition(TransitionCommit {
                    current: current.clone(),
                    next,
                    telemetry_context: request.telemetry_context.clone(),
                    expected_configuration_seq: None,
                    expected_settings_revision: None,
                })
                .await?
                .is_some()
            {
                return Ok(true);
            }
            // A conditional refusal did not start the request. Replan this same
            // captured intent under the newest lane sequence; the recipe above
            // still decides cancellation, request identity and attempt validity.
        }
    }
    pub(crate) async fn nested_transition(
        &self,
        h: &crate::restore::HydratedLane,
        plan: &TransitionCommit<Telemetry>,
        commit: &mut Option<crate::scheduler::CommitPermit>,
    ) -> Result<Option<Option<CurrentOperation>>, HarnessRuntimeError> {
        let Some(current) = h.current.as_ref() else {
            return Ok(None);
        };
        let binding = self
            .summary_binding
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        let Some(binding) = binding else {
            return Ok(None);
        };
        let mut projected = current.clone();
        projected.state = plan.next.clone();
        if !pi_harness_core::structural::matches(
            &projected,
            &binding.outer,
            &binding.outer.key.session_id,
        ) || pi_harness_core::structural::pending(&projected).is_none_or(|p| {
            p.request
                != Some(SummaryRequestIntent {
                    index: binding.request.request_index,
                    usage_id: binding.request.usage_id.clone(),
                })
        }) {
            return Ok(None);
        }
        if plan.current.operation != current.operation
            || plan.current.operation_state_seq != current.operation_state_seq
            || plan.current.lane_state_seq != current.lane_state_seq
        {
            return Ok(Some(None));
        }
        let decision = pi_harness_core::structural::nested_intent_recipe(
            current,
            &binding.outer,
            &binding.request,
            &self.lane.facts(h)?,
        )
        .map_err(|_| self.lane.owner.fail("nested summary transition invalid"))?;
        let applied = if let pi_harness_core::planner::RecipeDecision::Apply(recipe) = decision {
            if recipe.next.as_ref() != Some(&plan.next) {
                return Ok(Some(None));
            }
            self.lane
                .apply_prepared_scoped(*recipe, None, commit, false)
                .await?
                .and_then(|h| h.current)
        } else {
            None
        };
        Ok(Some(applied))
    }
    pub(crate) async fn nested_request(
        self: &Arc<Self>,
        request: SummaryRequestPlan<Telemetry>,
    ) -> Result<SummaryRequestOutput, HarnessRuntimeError> {
        if !self.ordinary_permission("run_summary_request").await? {
            return Ok(SummaryRequestOutput::NotStarted);
        }
        let binding = self
            .summary_binding
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
            .ok_or_else(|| self.lane.owner.fail("nested request binding missing"))?;
        if binding.request.task_id != request.task_id
            || binding.request.attempt != request.attempt
            || binding.request.request_index != request.request_index
            || binding.request.usage_id != request.usage_id
            || binding.request.configuration != request.configuration
            || binding.request.messages != request.messages
        {
            return Err(self.lane.owner.fail("nested request binding mismatch"));
        }
        let captured = {
            let _line = self.lane.lane.line.acquire().await?;
            let h = self.lane.load().await?;
            let Some(current) = h.current.as_ref() else {
                return Ok(SummaryRequestOutput::NotStarted);
            };
            if !pi_harness_core::structural::matches(
                current,
                &binding.outer,
                &binding.outer.key.session_id,
            ) || pi_harness_core::structural::is_cancelled(current)
                || pi_harness_core::structural::pending(current).is_none_or(|p| {
                    p.request
                        != Some(SummaryRequestIntent {
                            index: request.request_index,
                            usage_id: request.usage_id.clone(),
                        })
                })
            {
                return Ok(SummaryRequestOutput::NotStarted);
            }
            self.lane
                .owner
                .models
                .capture_text_dispatch(&binding.registration, binding.model.clone())
        };
        let now = SafeUnsigned::new(self.lane.owner.clock.now_unix_ms())
            .map_err(|_| self.lane.owner.fail("summary request clock invalid"))?;
        let captured = match captured {
            Ok(c) => c,
            Err(
                pi_ai_core::CaptureDispatchError::MissingRegistration
                | pi_ai_core::CaptureDispatchError::ReplacedRegistration,
            ) => {
                return Ok(SummaryRequestOutput::Response(Box::new(
                    crate::provider::synthetic_response(
                        binding.model.api,
                        binding.model.provider,
                        binding.model.id,
                        now,
                        false,
                        "Provider binding is missing or replaced".into(),
                    )?,
                )));
            }
            Err(_) => return Err(self.lane.owner.fail("summary capture invalid")),
        };
        let span = crate::telemetry::Span::start(
            request.telemetry_context.clone(),
            "pi.ai.request",
            pi_telemetry::SpanAttributes::from([
                ("pi.ai.operation".into(), crate::telemetry::string("stream")),
                (
                    "pi.ai.provider".into(),
                    crate::telemetry::string(binding.model.provider.as_str()),
                ),
                (
                    "pi.ai.model".into(),
                    crate::telemetry::string(binding.model.id.as_str()),
                ),
            ]),
        );
        let input = crate::request::RequestInput {
            models: self.lane.owner.models.clone(),
            model: binding.model,
            context: pi_ai_core::Context {
                system_prompt: Some(binding.spec.system_prompt),
                messages: vec![pi_ai_core::Message::User(pi_ai_core::UserMessage {
                    role: Default::default(),
                    content: pi_ai_core::UserContent::Text(binding.spec.prompt),
                    timestamp: now,
                })],
                tools: None,
            },
            thinking: binding.spec.thinking,
            options: binding.options,
            runtime: pi_ai_core::RequestRuntime {
                cancellation: self.cancellation.clone(),
                telemetry: span.clone(),
                clock: self.lane.owner.clock.clone(),
                spawner: Arc::new(self.lane.owner.effects.clone()),
            },
            lane: binding.outer.key.lane.clone(),
            run_id: binding.outer.key.operation_id.clone(),
            session_id: binding.outer.key.session_id.clone(),
            callbacks: Arc::new(SummaryHooks {
                fx: self.clone(),
                telemetry: span.clone(),
                key: binding.outer.key.clone(),
            }),
        };
        let settings = crate::request::SummaryRequestSettings {
            routing_key: format!(
                "{}:{}:{}:{}",
                binding.outer.key.operation_id.as_str(),
                request.task_id.as_str(),
                request.attempt.get(),
                request.usage_id.as_str()
            ),
            max_tokens: binding.spec.max_tokens,
            logical_prompt_utf16_units: Some(binding.spec.prompt_utf16_units),
        };
        #[cfg(feature = "extensions")]
        let response = if let Some(provider) = binding.remote {
            self.structural_wait(crate::request::run_keyed_request(
                input,
                captured,
                &crate::extensions::profiles::provider::Source {
                    fx: self.clone(),
                    key: binding.outer.key.clone(),
                    provider,
                    deferred_source: None,
                },
                Some(settings),
            ))
            .await?
        } else {
            self.structural_wait(crate::request::run_summary_request(
                input, captured, settings,
            ))
            .await?
        };
        #[cfg(not(feature = "extensions"))]
        let response = self
            .structural_wait(crate::request::run_summary_request(
                input, captured, settings,
            ))
            .await?;
        span.outcome(
            "pi.ai.response.stop_reason",
            match response.as_message().stop_reason {
                pi_ai_core::StopReason::Error => "error",
                pi_ai_core::StopReason::Aborted => "aborted",
                pi_ai_core::StopReason::Length => "length",
                _ => "stop",
            },
            response.as_message().stop_reason == pi_ai_core::StopReason::Error,
        );
        Ok(SummaryRequestOutput::Response(Box::new(response)))
    }
    pub(crate) async fn nested_settlement(
        &self,
        request: SummaryRequestSettlement<Telemetry>,
    ) -> Result<Option<CurrentOperation>, HarnessRuntimeError> {
        self.permission("settle_summary_request").await?;
        let binding = self
            .summary_binding
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
            .ok_or_else(|| self.lane.owner.fail("summary settlement binding missing"))?;
        let mut publication =
            nested_settlement_publication(&self.lane.owner.settings_line, &self.lane.lane.line)
                .await?;
        let h = self.lane.load().await?;
        let Some(current) = h.current.as_ref() else {
            return Ok(None);
        };
        let decision = pi_harness_core::structural::settle_request_recipe(
            current,
            &binding.outer,
            &request,
            &self.lane.facts(&h)?,
        )
        .map_err(|_| self.lane.owner.fail("nested summary settlement invalid"))?;
        let applied = if let pi_harness_core::planner::RecipeDecision::Apply(recipe) = decision {
            self.lane
                .apply_prepared_scoped(*recipe, None, publication.commit_slot(), false)
                .await?
                .and_then(|h| h.current)
        } else {
            None
        };
        publication.release();
        self.events().flush();
        self.lane.lane.announce();
        Ok(applied)
    }
}
struct SummaryHooks<C, S: RuntimeStorage, M: SessionMetadataRecord> {
    fx: Arc<RuntimeEffects<C, S, M>>,
    telemetry: Telemetry,
    key: EffectKey,
}
impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>
    crate::request::RequestCallbacks for SummaryHooks<C, S, M>
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
                Some(HookResult::BeforePayload(value)) => {
                    Ok(value.as_ref().map(|v| v.payload.clone()))
                }
                _ => Err(self.fx.lane.owner.fail("summary payload hook mismatch")),
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
                Some(HookResult::AfterResponse(value)) => Ok(value
                    .as_ref()
                    .and_then(|v| v.message.clone())
                    .unwrap_or(original)),
                _ => Err(self.fx.lane.owner.fail("summary response hook mismatch")),
            }
        })
    }
    fn stream_event(
        &self,
        _event: pi_ai_core::AssistantMessageEvent,
    ) -> BoxFuture<'_, Result<(), HarnessRuntimeError>> {
        Box::pin(async { Ok(()) })
    }
}

impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>
    RuntimeEffects<C, S, M>
{
    pub(crate) fn summary_retry_started(&self, plan: &SummaryEffectPlan<Telemetry>) {
        if plan.generation.attempt.get() > 1 {
            self.events().enqueue(
                HarnessEventType::RetryStart,
                &self.lane.lane.name,
                HarnessEvent::RetryStart(RetryStartEvent {
                    event_type: Default::default(),
                    lane: self.lane.lane.name.clone(),
                    recovery: self.recovery.then_some(pi_ai_core::LiteralTrue),
                    run_id: plan.key.operation_id.clone(),
                    step: plan.generation.context.task_id.as_str().into(),
                    attempt: plan.generation.attempt,
                }),
            );
        }
    }
    pub(crate) fn summary_settled(
        &self,
        previous: &CurrentOperation,
        next: Option<&CurrentOperation>,
        output: &SettlementOutput,
    ) {
        if let Some(StructuralDecision::Generating { generation, .. }) =
            next.and_then(pi_harness_core::structural::decision)
            && let SummaryGeneration::RetryWait {
                context,
                next_attempt,
                not_before,
                error_message,
            } = generation.as_ref()
        {
            self.events().enqueue(
                HarnessEventType::RetryScheduled,
                &self.lane.lane.name,
                HarnessEvent::RetryScheduled(RetryScheduledEvent {
                    event_type: Default::default(),
                    lane: self.lane.lane.name.clone(),
                    recovery: self.recovery.then_some(pi_ai_core::LiteralTrue),
                    run_id: previous.operation.operation_id.clone(),
                    step: context.task_id.as_str().into(),
                    attempt: *next_attempt,
                    max_attempts: context.retry_policy.max_attempts,
                    delay_ms: SafeUnsigned::new(
                        not_before
                            .get()
                            .saturating_sub(self.lane.owner.clock.now_unix_ms()),
                    )
                    .expect("bounded duration"),
                    error_message: error_message.clone(),
                }),
            );
        }
        let SettlementOutput::Summary { outcome, .. } = output else {
            return;
        };
        let Some(p) = pi_harness_core::structural::pending(previous) else {
            return;
        };
        let (value, error) = match outcome {
            SummaryAttemptOutcome::Success(_) => ("succeeded", None),
            SummaryAttemptOutcome::Retry(e) => ("retry", Some(e.message.clone())),
            SummaryAttemptOutcome::Failure(e) => ("failed", Some(e.message.clone())),
        };
        self.trace
            .step_outcome(if pi_harness_core::structural::is_cancelled(previous) {
                "aborted"
            } else {
                value
            });
        if p.attempt.get() > 1 {
            self.events().enqueue(
                HarnessEventType::RetryEnd,
                &self.lane.lane.name,
                HarnessEvent::RetryEnd(RetryEndEvent {
                    event_type: Default::default(),
                    lane: self.lane.lane.name.clone(),
                    recovery: self.recovery.then_some(pi_ai_core::LiteralTrue),
                    run_id: previous.operation.operation_id.clone(),
                    step: p.context.task_id.as_str().into(),
                    attempt: p.attempt,
                    success: value == "succeeded",
                    final_error: error,
                }),
            );
        }
    }
}

#[cfg(test)]
mod nested_settlement_publication_tests {
    use super::nested_settlement_publication;
    use crate::scheduler::{Admission, MutationLine};
    use std::{
        future::Future,
        pin::Pin,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        task::{Context, Poll, Wake, Waker},
    };

    fn poll<F: Future + ?Sized>(future: Pin<&mut F>, waker: &Waker) -> Poll<F::Output> {
        future.poll(&mut Context::from_waker(waker))
    }

    struct DrainProbe {
        settings: MutationLine,
        lane: MutationLine,
        called: Arc<AtomicBool>,
    }
    impl Wake for DrainProbe {
        fn wake(self: Arc<Self>) {
            assert!(
                !self.settings.held_for_test(),
                "summary drain saw held settings"
            );
            assert!(!self.lane.held_for_test(), "summary drain saw held lane");
            self.called.store(true, Ordering::SeqCst);
        }
    }

    #[test]
    fn nested_summary_last_commit_drains_after_both_lines_on_all_exits() {
        for exit in ["applied", "refused", "error", "cancel", "unwind"] {
            let admission = Admission::new();
            let settings = MutationLine::new(admission.clone());
            let lane = MutationLine::new(admission.clone());
            let mut acquire = Box::pin(nested_settlement_publication(&settings, &lane));
            let Poll::Ready(Ok(mut scope)) = poll(acquire.as_mut(), Waker::noop()) else {
                panic!("nested summary lines")
            };
            drop(acquire);
            *scope.commit_slot() = Some(admission.try_admit().unwrap());
            let called = Arc::new(AtomicBool::new(false));
            let waker = Waker::from(Arc::new(DrainProbe {
                settings: settings.clone(),
                lane: lane.clone(),
                called: called.clone(),
            }));
            let mut drained = Box::pin(admission.drained());
            assert!(poll(drained.as_mut(), &waker).is_pending());
            admission.seal_close();
            match exit {
                "applied" | "refused" => scope.release(),
                "error" => {
                    let result: Result<(), ()> = {
                        let _scope = scope;
                        Err(())
                    };
                    assert!(result.is_err());
                }
                "cancel" => {
                    let mut pending = Box::pin(async move {
                        let _scope = scope;
                        std::future::pending::<()>().await;
                    });
                    assert!(poll(pending.as_mut(), Waker::noop()).is_pending());
                    drop(pending);
                }
                "unwind" => {
                    assert!(
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            let _scope = scope;
                            panic!("nested summary caller unwind");
                        }))
                        .is_err()
                    );
                }
                _ => unreachable!(),
            }
            assert!(
                called.load(Ordering::SeqCst),
                "last commit must notify drain"
            );
            assert!(poll(drained.as_mut(), Waker::noop()).is_ready());
        }
    }
}
