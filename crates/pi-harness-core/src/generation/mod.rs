//! Captured assistant generation and atomic settlement recipes.
#![allow(missing_docs)]
use crate::planner::*;
use pi_agent_schema::session_values::*;
use pi_agent_schema::*;

pub(crate) fn plan<C: Clone>(
    current: &CurrentOperation,
    session: &SessionId,
    generation: &Generation,
    context: &C,
) -> Result<AssistantEffectPlan<C>, PlanError> {
    let pending =
        PendingGeneration::try_from(generation.clone()).map_err(|_| PlanError::InvalidCurrent)?;
    Ok(AssistantEffectPlan {
        key: key(
            current,
            session,
            EffectKind::Assistant,
            pending.context.step_id.as_str(),
            pending.attempt.get(),
            None,
        )?,
        stream_options: pending.context.stream_options.clone(),
        generation: pending,
        telemetry_context: context.clone(),
    })
}
pub(crate) fn next<C: Clone>(
    current: &CurrentOperation,
    inputs: &PlannerInputs<C>,
    facts: &PlanningFacts<C>,
    g: &Generation,
) -> Result<ProcedureAction<C>, PlanError> {
    let r = run(current)?;
    match g {
        Generation::Ready {
            context,
            next_attempt,
        } => {
            if cancelled(r) {
                return Ok(ProcedureAction::Existing(Action::Finish(
                    crate::terminal::operation_result(current, inputs)?,
                )));
            }
            if let PlanningFacts::MissingIdentities { missing } = facts {
                return Ok(ProcedureAction::Existing(Action::Suspend(
                    OperationResult::Run(Box::new(RunOutcome::MissingIdentities(
                        RunMissingIdentities {
                            kind: Default::default(),
                            reason: Default::default(),
                            leaf_id: current.leaf_id.clone().ok_or(PlanError::InvalidCurrent)?,
                            missing: missing.clone(),
                        },
                    ))),
                )));
            }
            let PlanningFacts::BeforeRequest {
                session_id,
                telemetry_context,
                event,
            } = facts
            else {
                return Err(PlanError::MissingInput);
            };
            validate_request(current, context, *next_attempt, event)?;
            dispatch_or_await(
                inputs,
                EffectPlan::Hook(HookEffectPlan {
                    key: key(
                        current,
                        session_id,
                        EffectKind::Hook,
                        context.step_id.as_str(),
                        next_attempt.get(),
                        None,
                    )?,
                    event: HookEvent::BeforeRequest(event.clone()),
                    telemetry_context: telemetry_context.clone(),
                }),
            )
        }
        Generation::EffectPending {
            context, attempt, ..
        } => {
            let PlanningFacts::Generation {
                session_id,
                telemetry_context,
                synthetic,
            } = facts
            else {
                return Err(PlanError::MissingInput);
            };
            let effect = EffectPlan::Assistant(plan(current, session_id, g, telemetry_context)?);
            if let Some(live) = inputs.running.get(effect.key()) {
                if !plan_matches(&effect, live) {
                    return Err(PlanError::InvalidInput);
                };
                return Ok(ProcedureAction::Existing(Action::AwaitEffect(
                    effect.key().clone(),
                )));
            }
            if !cancelled(r) && attempt.get() < context.retry_policy.max_attempts.get() {
                return Ok(transition(
                    current,
                    assistant_phase(Generation::Ready {
                        context: context.clone(),
                        next_attempt: increment(*attempt)?,
                    }),
                    telemetry_context,
                ));
            }
            let reason = if cancelled(r) {
                ReconcileReason::CancelledNotStarted
            } else {
                ReconcileReason::UnknownAtCap
            };
            let output = ReconciledOutput::new(
                &effect,
                reason,
                ReconciledValue::Assistant(synthetic.clone().ok_or(PlanError::MissingInput)?),
            )
            .map_err(|_| PlanError::InvalidInput)?;
            Ok(ProcedureAction::Reconcile(Box::new(SettlementCommit {
                current: current.clone(),
                plan: effect,
                output: SettlementOutput::Reconciled(output),
                telemetry_context: telemetry_context.clone(),
            })))
        }
        Generation::RetryWait {
            context,
            next_attempt,
            not_before,
            ..
        } => {
            if cancelled(r) {
                return Ok(ProcedureAction::Existing(Action::Finish(
                    crate::terminal::operation_result(current, inputs)?,
                )));
            }
            let PlanningFacts::Generation {
                telemetry_context, ..
            } = facts
            else {
                return Err(PlanError::MissingInput);
            };
            if inputs.now < *not_before {
                Ok(ProcedureAction::Existing(Action::Wait(WaitPlan {
                    until: *not_before,
                    telemetry_context: telemetry_context.clone(),
                })))
            } else {
                Ok(transition(
                    current,
                    assistant_phase(Generation::Ready {
                        context: context.clone(),
                        next_attempt: *next_attempt,
                    }),
                    telemetry_context,
                ))
            }
        }
    }
}
pub fn increment(attempt: OneBasedSafeUnsigned) -> Result<OneBasedSafeUnsigned, PlanError> {
    OneBasedSafeUnsigned::new(attempt.get().checked_add(1).ok_or(PlanError::NumberRange)?)
        .map_err(|_| PlanError::NumberRange)
}
pub fn retry_deadline(
    now: SafeUnsigned,
    base: SafeUnsigned,
    attempt: OneBasedSafeUnsigned,
) -> Result<SafeUnsigned, PlanError> {
    let max = SafeUnsigned::MAX.get();
    let exponent = attempt.get() - 1;
    let delay = if base.get() == 0 {
        0
    } else if exponent >= 64 {
        max
    } else {
        base.get().saturating_mul(1u64 << exponent).min(max)
    };
    number(now.get().saturating_add(delay).min(max))
}
fn validate_request(
    current: &CurrentOperation,
    g: &GenerationContext,
    attempt: OneBasedSafeUnsigned,
    event: &BeforeRequestEvent,
) -> Result<(), PlanError> {
    if event.lane != current.operation.lane
        || event.run_id != current.operation.operation_id
        || event.step != HookRequestStep::Assistant
        || event.attempt != attempt
        || event.model.provider != g.configuration.model.provider
        || event.model.id != g.configuration.model.model_id
        || event.stream_options != g.stream_options
    {
        return Err(PlanError::InvalidInput);
    }
    Ok(())
}
pub(crate) fn intent<C: Clone>(
    current: &CurrentOperation,
    commit: &SettlementCommit<C>,
    facts: &CommitFacts,
    extra: &SettlementFacts,
) -> Result<RecipeDecision<C>, PlanError> {
    let r = run(current)?;
    let RunPhase::Assistant(AssistantPhase {
        generation: Generation::Ready {
            context,
            next_attempt,
        },
        ..
    }) = &r.phase
    else {
        return Ok(RecipeDecision::NoLongerExpected);
    };
    if cancelled(r) {
        return Ok(RecipeDecision::NoLongerExpected);
    }
    let EffectPlan::Hook(h) = &commit.plan else {
        return Ok(RecipeDecision::NoLongerExpected);
    };
    let HookEvent::BeforeRequest(event) = &h.event else {
        return Ok(RecipeDecision::NoLongerExpected);
    };
    let expected = key(
        current,
        &facts.session_id,
        EffectKind::Hook,
        context.step_id.as_str(),
        next_attempt.get(),
        None,
    )?;
    let SettlementOutput::RequestOptions(output) = &commit.output else {
        return Ok(RecipeDecision::NoLongerExpected);
    };
    if h.key != expected || output.key() != &expected {
        return Ok(RecipeDecision::NoLongerExpected);
    }
    validate_request(current, context, *next_attempt, event)?;
    let SettlementFacts::Request {
        response_entry_id,
        usage_id,
    } = extra
    else {
        return Err(PlanError::MissingInput);
    };
    if response_entry_id.as_str() == usage_id.as_str() || inputs_named(current, response_entry_id) {
        return Err(PlanError::InvalidInput);
    }
    let captured = context.clone();
    let g = Generation::EffectPending {
        context: captured,
        attempt: *next_attempt,
        response_entry_id: response_entry_id.clone(),
        usage_id: usage_id.clone(),
        intended_output_limit: event.model.max_tokens,
        context_window: event.model.context_window,
    };
    let mut effect = plan(current, &facts.session_id, &g, &commit.telemetry_context)?;
    effect.stream_options = output.options().clone();
    let mut next = r.clone();
    next.phase = assistant_phase(g);
    let state = OperationState::Run(Box::new(next));
    let mut result = recipe(current, facts, &commit.telemetry_context);
    result.expected_effect = Some(expected);
    result
        .transaction
        .writes
        .push(state_write(current, state.clone()));
    result.next = Some(state);
    result.dispatch = Some(EffectPlan::Assistant(effect));
    Ok(RecipeDecision::Apply(Box::new(result)))
}
fn inputs_named(current: &CurrentOperation, id: &EntryId) -> bool {
    current.leaf_id.as_ref() == Some(id)
        || current.operation.source_leaf_id.as_ref() == Some(id)
        || match &current.operation.intent {
            OperationIntent::Run(i) => i.prompt_entry_ids.contains(id),
            _ => false,
        }
}
pub(crate) fn settle<C: Clone>(
    current: &CurrentOperation,
    commit: &SettlementCommit<C>,
    inputs: &PlannerInputs<C>,
    facts: &CommitFacts,
    extra: &SettlementFacts,
) -> Result<RecipeDecision<C>, PlanError> {
    let r = run(current)?;
    let RunPhase::Assistant(AssistantPhase {
        generation: g @ Generation::EffectPending { .. },
        ..
    }) = &r.phase
    else {
        return Ok(RecipeDecision::NoLongerExpected);
    };
    let p = plan(current, &facts.session_id, g, &commit.telemetry_context)?;
    let EffectPlan::Assistant(received) = &commit.plan else {
        return Ok(RecipeDecision::NoLongerExpected);
    };
    if received.key != p.key || received.generation != p.generation {
        return Ok(RecipeDecision::NoLongerExpected);
    }
    let synthetic = matches!(&commit.output, SettlementOutput::Reconciled(_));
    let mut message = match &commit.output {
        SettlementOutput::Assistant { key, message } if key == &p.key => {
            message.clone().into_message()
        }
        SettlementOutput::Reconciled(o) if o.key() == &p.key => {
            let valid = match o.reason() {
                ReconcileReason::CancelledNotStarted => cancelled(r),
                ReconcileReason::UnknownAtCap => {
                    !cancelled(r)
                        && p.generation.attempt.get()
                            >= p.generation.context.retry_policy.max_attempts.get()
                }
                ReconcileReason::InterruptedTool => false,
            };
            if !valid {
                return Ok(RecipeDecision::NoLongerExpected);
            }
            let ReconciledValue::Assistant(m) = o.value() else {
                return Ok(RecipeDecision::NoLongerExpected);
            };
            if m.as_message().usage != Usage::zero() || !m.as_message().content.is_empty() {
                return Err(PlanError::InvalidInput);
            };
            m.as_ref().clone().into_message()
        }
        _ => return Ok(RecipeDecision::NoLongerExpected),
    };
    let (retryable, overflow, followers, overflow_preparation) = match extra {
        SettlementFacts::Assistant {
            retryable,
            overflow,
            followers,
        } => (retryable, overflow, followers, None),
        SettlementFacts::Overflow(o) => (
            &o.retryable,
            &o.matching_error,
            &o.followers,
            o.prepared.as_ref(),
        ),
        _ => return Err(PlanError::MissingInput),
    };
    let mut publish_preparation = None;
    if synthetic
        && (message.provider != p.generation.context.configuration.model.provider
            || message.model != p.generation.context.configuration.model.model_id
            || (!cancelled(r) && message.stop_reason != StopReason::Error))
    {
        return Err(PlanError::InvalidInput);
    }
    let id = p.generation.response_entry_id.clone();
    let mut next = r.clone();
    next.latest_assistant_entry_id = Some(id.clone());
    if cancelled(r) {
        message.stop_reason = StopReason::Aborted;
        message.error_message = Some(
            message
                .error_message
                .unwrap_or_else(|| "Operation cancelled".into()),
        );
        next.phase = checkpoint(
            id.clone(),
            Continuation::MayFinish {
                include_final_assistant: true,
            },
        )
    } else if message.stop_reason == StopReason::Aborted {
        return Err(PlanError::InvalidCurrent);
    } else if crate::compaction::is_overflow(
        &message,
        p.generation.intended_output_limit,
        *overflow,
    ) {
        crate::compaction::normalize_overflow(
            &mut message,
            Some(p.generation.intended_output_limit),
        );
        next.phase =
            crate::compaction::overflow_phase(current, &p.generation, &id, overflow_preparation)?;
        if matches!(next.phase, RunPhase::Compaction(_)) {
            publish_preparation = overflow_preparation;
        }
    } else if message.stop_reason == StopReason::Deferred {
        if message.deferred.is_none() {
            return Err(PlanError::InvalidInput);
        }
        next.phase = RunPhase::Deferred(DeferredPhase {
            kind: Default::default(),
            deferred: Deferred::Suspended {
                step_id: p.generation.context.step_id.clone(),
                source_entry_id: id.clone(),
                poll: number(0)?,
                configuration: p.generation.context.configuration.clone(),
                stream_options: p.generation.context.stream_options.clone(),
            },
        })
    } else if message.stop_reason == StopReason::Error {
        let error_message = message
            .error_message
            .clone()
            .ok_or(PlanError::InvalidInput)?;
        if *retryable
            && !synthetic
            && p.generation.attempt.get() < p.generation.context.retry_policy.max_attempts.get()
        {
            next.phase = assistant_phase(Generation::RetryWait {
                context: p.generation.context.clone(),
                next_attempt: increment(p.generation.attempt)?,
                not_before: retry_deadline(
                    inputs.now,
                    p.generation.context.retry_policy.base_delay_ms,
                    p.generation.attempt,
                )?,
                error_message,
            })
        } else {
            next.phase = RunPhase::FailureDrain(FailureDrainPhase {
                kind: Default::default(),
                error: OperationError {
                    code: OperationErrorCode::new("provider_error")
                        .map_err(|_| PlanError::InvalidInput)?,
                    message: error_message,
                    details: Presence::Missing,
                },
                provenance: FailureProvenance::Response {
                    entry_id: id.clone(),
                },
            })
        }
    } else {
        let indexes: Vec<_> = message
            .content
            .iter()
            .enumerate()
            .filter_map(|(i, c)| matches!(c, AssistantContent::ToolCall(_)).then_some(i))
            .map(|i| number(u64::try_from(i).map_err(|_| PlanError::NumberRange)?))
            .collect::<Result<_, _>>()?;
        if indexes.is_empty() {
            if !followers.is_empty() {
                return Err(PlanError::InvalidInput);
            }
            next.phase = checkpoint(
                id.clone(),
                if message.stop_reason == StopReason::ToolUse {
                    Continuation::NeedAssistant {
                        overflow_recovery_used: false,
                    }
                } else {
                    Continuation::MayFinish {
                        include_final_assistant: true,
                    }
                },
            )
        } else {
            let mut ids = std::collections::BTreeSet::new();
            if followers.len() != indexes.len()
                || followers
                    .iter()
                    .zip(&indexes)
                    .any(|((index, result), expected)| {
                        index != expected
                            || result == &id
                            || !ids.insert(result.clone())
                            || inputs_named(current, result)
                    })
            {
                return Err(PlanError::InvalidInput);
            }
            next.phase = RunPhase::Tools(ToolsPhase {
                kind: Default::default(),
                batch: ToolBatch {
                    assistant_entry_id: id.clone(),
                    configuration: p.generation.context.configuration.clone(),
                    turn_id: p.generation.context.step_id.clone(),
                    calls: followers
                        .iter()
                        .map(|(source_index, result_entry_id)| ToolCallState::Planned {
                            source_index: *source_index,
                            result_entry_id: result_entry_id.clone(),
                        })
                        .collect(),
                },
            })
        }
    }
    let mut result = recipe(current, facts, &commit.telemetry_context);
    result.expected_effect = Some(p.key);
    let usage = message.usage.clone();
    let settled = SettledAssistantMessage::new(message).map_err(|_| PlanError::InvalidInput)?;
    result
        .transaction
        .writes
        .push(Write::Entry(NewEntry::Message(NewMessageEntry {
            id: id.clone(),
            parent_id: current.leaf_id.clone(),
            entry_type: Default::default(),
            message: AgentMessage::BuiltIn(Message::Assistant(Box::new(settled.into_message()))),
            terminate: None,
        })));
    result.transaction.writes.push(set(RegisterSet::LaneLeaf {
        key: current.operation.lane.as_str().into(),
        value: Some(id.clone()),
    }));
    result.transaction.writes.push(Write::Usage(NewUsageRow {
        id: p.generation.usage_id.clone(),
        usage,
        entry_id: Some(id.clone()),
        adjustment: false,
        details: Presence::Missing,
    }));
    if let Some(prepared) = publish_preparation {
        crate::compaction::add_preparation(current, prepared, &mut result)?;
    }
    let state = OperationState::Run(Box::new(next));
    result
        .transaction
        .writes
        .push(state_write(current, state.clone()));
    result.next = Some(state);
    result.publication = vec![
        Publication::Entry(id),
        Publication::Usage(p.generation.usage_id),
    ];
    Ok(RecipeDecision::Apply(Box::new(result)))
}
