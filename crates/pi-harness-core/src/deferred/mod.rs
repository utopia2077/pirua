//! Single-permit deferred redemption, exact lineage and atomic settlements.
#![allow(missing_docs)]
use crate::planner::*;
use pi_agent_schema::session_values::*;
use pi_agent_schema::*;

struct Base<'a> {
    step: &'a StepId,
    source: &'a EntryId,
    poll: OneBasedSafeUnsigned,
    config: &'a LaneConfiguration,
    options: &'a AgentHarnessStreamOptions,
}
fn base(d: &Deferred) -> Result<Base<'_>, PlanError> {
    Ok(match d {
        Deferred::Suspended {
            step_id,
            source_entry_id,
            poll,
            configuration,
            stream_options,
        } => Base {
            step: step_id,
            source: source_entry_id,
            poll: OneBasedSafeUnsigned::new(
                poll.get().checked_add(1).ok_or(PlanError::NumberRange)?,
            )
            .map_err(|_| PlanError::NumberRange)?,
            config: configuration,
            options: stream_options,
        },
        Deferred::EffectPending {
            step_id,
            source_entry_id,
            poll,
            configuration,
            stream_options,
            ..
        } => Base {
            step: step_id,
            source: source_entry_id,
            poll: *poll,
            config: configuration,
            options: stream_options,
        },
    })
}
fn state(current: &CurrentOperation) -> Result<&Deferred, PlanError> {
    match &run(current)?.phase {
        RunPhase::Deferred(p) => Ok(&p.deferred),
        _ => Err(PlanError::InvalidCurrent),
    }
}
fn options(base: &AgentHarnessStreamOptions) -> AgentHarnessStreamOptions {
    let mut options = base.clone();
    options.deferred = Some(DeferredRequest::Enabled(false));
    options
}
fn validate_request(
    current: &CurrentOperation,
    b: &Base<'_>,
    e: &BeforeRequestEvent,
) -> Result<(), PlanError> {
    if e.lane != current.operation.lane
        || e.run_id != current.operation.operation_id
        || e.step != HookRequestStep::Deferred
        || e.attempt != b.poll
        || e.model.provider != b.config.model.provider
        || e.model.id != b.config.model.model_id
        || e.stream_options != options(b.options)
    {
        return Err(PlanError::InvalidInput);
    }
    Ok(())
}
pub fn plan<C: Clone>(
    current: &CurrentOperation,
    session: &SessionId,
    context: &C,
) -> Result<DeferredEffectPlan<C>, PlanError> {
    let p = PendingDeferred::try_from(state(current)?.clone())
        .map_err(|_| PlanError::InvalidCurrent)?;
    Ok(DeferredEffectPlan {
        key: key(
            current,
            session,
            EffectKind::Deferred,
            p.step_id.as_str(),
            p.poll.get(),
            None,
        )?,
        stream_options: options(&p.stream_options),
        deferred: p,
        telemetry_context: context.clone(),
    })
}
pub(crate) fn next<C: Clone>(
    current: &CurrentOperation,
    inputs: &PlannerInputs<C>,
    facts: &PlanningFacts<C>,
) -> Result<ProcedureAction<C>, PlanError> {
    let r = run(current)?;
    let d = state(current)?;
    // Old consumers can still inspect a suspended operation with terminal facts.
    if matches!(d, Deferred::Suspended { .. })
        && inputs.deferred_poll == DeferredPollPermit::None
        && !cancelled(r)
    {
        return Ok(ProcedureAction::Existing(Action::Suspend(
            crate::terminal::operation_result(current, inputs)?,
        )));
    }
    if cancelled(r) && matches!(d, Deferred::Suspended { .. }) {
        return Ok(ProcedureAction::Existing(Action::Finish(
            crate::terminal::operation_result(current, inputs)?,
        )));
    }
    let PlanningFacts::Deferred {
        session_id,
        telemetry_context,
        request,
        synthetic,
    } = facts
    else {
        return Err(PlanError::MissingInput);
    };
    if matches!(d, Deferred::EffectPending { .. }) {
        let p = plan(current, session_id, telemetry_context)?;
        let effect = EffectPlan::Deferred(p);
        if let Some(live) = inputs.running.get(effect.key()) {
            if !plan_matches(&effect, live) {
                return Err(PlanError::InvalidInput);
            }
            return Ok(ProcedureAction::Existing(Action::AwaitEffect(
                effect.key().clone(),
            )));
        }
        if cancelled(r) {
            let output = ReconciledOutput::new(
                &effect,
                ReconcileReason::CancelledNotStarted,
                ReconciledValue::Assistant(synthetic.clone().ok_or(PlanError::MissingInput)?),
            )
            .map_err(|_| PlanError::InvalidInput)?;
            return Ok(ProcedureAction::Reconcile(Box::new(SettlementCommit {
                current: current.clone(),
                plan: effect,
                output: SettlementOutput::Reconciled(output),
                telemetry_context: telemetry_context.clone(),
            })));
        }
    }
    if inputs.deferred_poll == DeferredPollPermit::None {
        return Ok(ProcedureAction::Existing(Action::Suspend(
            crate::terminal::operation_result(current, inputs)?,
        )));
    }
    let b = base(d)?;
    let e = request.as_ref().ok_or(PlanError::MissingInput)?;
    validate_request(current, &b, e)?;
    dispatch_or_await(
        inputs,
        EffectPlan::Hook(HookEffectPlan {
            key: key(
                current,
                session_id,
                EffectKind::Hook,
                b.step.as_str(),
                b.poll.get(),
                None,
            )?,
            event: HookEvent::BeforeRequest(e.clone()),
            telemetry_context: telemetry_context.clone(),
        }),
    )
}
/// True only for a successfully applied poll intent recipe. Runtime transfers this
/// to the existing SettlementResult flag, never on a hook or failed conditional write.
pub fn consumes_poll<C>(recipe: &CommitRecipe<C>) -> bool {
    matches!(recipe.dispatch, Some(EffectPlan::Deferred(_)))
}
pub(crate) fn intent<C: Clone>(
    current: &CurrentOperation,
    commit: &SettlementCommit<C>,
    inputs: &PlannerInputs<C>,
    facts: &CommitFacts,
    extra: &SettlementFacts,
) -> Result<RecipeDecision<C>, PlanError> {
    let r = run(current)?;
    let RunPhase::Deferred(p) = &r.phase else {
        return Ok(RecipeDecision::NoLongerExpected);
    };
    if cancelled(r) || inputs.deferred_poll != DeferredPollPermit::One {
        return Ok(RecipeDecision::NoLongerExpected);
    }
    if matches!(p.deferred, Deferred::EffectPending { .. }) {
        let effect =
            EffectPlan::Deferred(plan(current, &facts.session_id, &commit.telemetry_context)?);
        if let Some(live) = inputs.running.get(effect.key()) {
            if !plan_matches(&effect, live) {
                return Err(PlanError::InvalidInput);
            }
            return Ok(RecipeDecision::NoLongerExpected);
        }
    }
    // A hook prepared from a different durable source/poll must never mint an intent.
    if run(&commit.current)?.phase != r.phase {
        return Ok(RecipeDecision::NoLongerExpected);
    }
    let b = base(&p.deferred)?;
    let EffectPlan::Hook(h) = &commit.plan else {
        return Ok(RecipeDecision::NoLongerExpected);
    };
    let HookEvent::BeforeRequest(e) = &h.event else {
        return Ok(RecipeDecision::NoLongerExpected);
    };
    let expected = key(
        current,
        &facts.session_id,
        EffectKind::Hook,
        b.step.as_str(),
        b.poll.get(),
        None,
    )?;
    let SettlementOutput::RequestOptions(o) = &commit.output else {
        return Ok(RecipeDecision::NoLongerExpected);
    };
    if h.key != expected || o.key() != &expected {
        return Ok(RecipeDecision::NoLongerExpected);
    }
    validate_request(current, &b, e)?;
    let SettlementFacts::Request {
        response_entry_id,
        usage_id,
    } = extra
    else {
        return Err(PlanError::MissingInput);
    };
    if response_entry_id.as_str() == usage_id.as_str()
        || inputs.loaded.contains_key(response_entry_id.as_str())
        || response_entry_id == b.source
        || current.leaf_id.as_ref() == Some(response_entry_id)
    {
        return Err(PlanError::InvalidInput);
    }
    if let Deferred::EffectPending {
        response_entry_id: old_r,
        usage_id: old_u,
        ..
    } = &p.deferred
        && (old_r == response_entry_id
            || old_u == usage_id
            || old_r.as_str() == usage_id.as_str()
            || old_u.as_str() == response_entry_id.as_str())
    {
        return Err(PlanError::InvalidInput);
    }
    let mut next = r.clone();
    next.phase = RunPhase::Deferred(DeferredPhase {
        kind: Default::default(),
        deferred: Deferred::EffectPending {
            step_id: b.step.clone(),
            source_entry_id: b.source.clone(),
            poll: b.poll,
            response_entry_id: response_entry_id.clone(),
            usage_id: usage_id.clone(),
            configuration: b.config.clone(),
            stream_options: b.options.clone(),
        },
    });
    let mut next_current = current.clone();
    next_current.state = OperationState::Run(Box::new(next));
    let mut dispatch = plan(&next_current, &facts.session_id, &commit.telemetry_context)?;
    dispatch.stream_options = o.options().clone();
    let mut recipe = recipe(current, facts, &commit.telemetry_context);
    recipe.expected_effect = Some(expected);
    recipe
        .transaction
        .writes
        .push(state_write(current, next_current.state.clone()));
    recipe.next = Some(next_current.state);
    recipe.dispatch = Some(EffectPlan::Deferred(dispatch));
    Ok(RecipeDecision::Apply(Box::new(recipe)))
}
pub(crate) fn settle<C: Clone>(
    current: &CurrentOperation,
    commit: &SettlementCommit<C>,
    inputs: &PlannerInputs<C>,
    facts: &CommitFacts,
    extra: &SettlementFacts,
) -> Result<RecipeDecision<C>, PlanError> {
    let r = run(current)?;
    if !matches!(
        &r.phase,
        RunPhase::Deferred(DeferredPhase {
            deferred: Deferred::EffectPending { .. },
            ..
        })
    ) {
        return Ok(RecipeDecision::NoLongerExpected);
    }
    let p = plan(current, &facts.session_id, &commit.telemetry_context)?;
    let EffectPlan::Deferred(received) = &commit.plan else {
        return Ok(RecipeDecision::NoLongerExpected);
    };
    if p.key != received.key || p.deferred != received.deferred {
        return Ok(RecipeDecision::NoLongerExpected);
    }
    let synthetic = matches!(commit.output, SettlementOutput::Reconciled(_));
    let mut message = match &commit.output {
        SettlementOutput::Deferred { key, message } if key == &p.key => {
            message.clone().into_message()
        }
        SettlementOutput::Reconciled(o)
            if o.key() == &p.key
                && o.reason() == ReconcileReason::CancelledNotStarted
                && cancelled(r) =>
        {
            let ReconciledValue::Assistant(m) = o.value() else {
                return Ok(RecipeDecision::NoLongerExpected);
            };
            if m.as_message().usage != Usage::zero()
                || !m.as_message().content.is_empty()
                || m.as_message().stop_reason != StopReason::Aborted
                || m.as_message().provider != p.deferred.configuration.model.provider
                || m.as_message().model != p.deferred.configuration.model.model_id
            {
                return Err(PlanError::InvalidInput);
            }
            m.as_ref().clone().into_message()
        }
        _ => return Ok(RecipeDecision::NoLongerExpected),
    };
    let (overflow, followers) = match extra {
        SettlementFacts::Deferred {
            overflow,
            followers,
        } => (*overflow, followers),
        SettlementFacts::Assistant {
            overflow,
            followers,
            ..
        } if synthetic => (*overflow, followers),
        _ => return Err(PlanError::MissingInput),
    };
    let source = crate::tool_batch::assistant(inputs, &p.deferred.source_entry_id)?;
    let handle = source.deferred.as_ref().ok_or(PlanError::InvalidCurrent)?;
    if source.stop_reason != StopReason::Deferred {
        return Err(PlanError::InvalidCurrent);
    }
    let id = p.deferred.response_entry_id.clone();
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
        );
    } else {
        if message.stop_reason == StopReason::Aborted {
            return Err(PlanError::InvalidCurrent);
        }
        if overflow && matches!(message.stop_reason, StopReason::Error | StopReason::Length) {
            crate::compaction::normalize_overflow(&mut message, None);
        }
        if message.stop_reason == StopReason::Deferred && message.deferred.as_ref() != Some(handle)
        {
            message.stop_reason = StopReason::Error;
            message.error_message = Some("Deferred response handle mismatch".into());
        }
        if message.stop_reason == StopReason::Deferred {
            next.phase = RunPhase::Deferred(DeferredPhase {
                kind: Default::default(),
                deferred: Deferred::Suspended {
                    step_id: p.deferred.step_id.clone(),
                    source_entry_id: id.clone(),
                    poll: number(p.deferred.poll.get())?,
                    configuration: p.deferred.configuration.clone(),
                    stream_options: p.deferred.stream_options.clone(),
                },
            });
        } else if message.stop_reason == StopReason::Error {
            next.phase = RunPhase::FailureDrain(FailureDrainPhase {
                kind: Default::default(),
                error: OperationError {
                    code: OperationErrorCode::new("provider_error")
                        .map_err(|_| PlanError::InvalidInput)?,
                    message: message
                        .error_message
                        .clone()
                        .ok_or(PlanError::InvalidInput)?,
                    details: Presence::Missing,
                },
                provenance: FailureProvenance::Response {
                    entry_id: id.clone(),
                },
            });
        } else {
            let indexes = message
                .content
                .iter()
                .enumerate()
                .filter_map(|(i, c)| matches!(c, AssistantContent::ToolCall(_)).then_some(i))
                .map(|i| number(u64::try_from(i).map_err(|_| PlanError::NumberRange)?))
                .collect::<Result<Vec<_>, _>>()?;
            let mut seen = std::collections::BTreeSet::new();
            if indexes.len() != followers.len()
                || indexes.iter().zip(followers).any(|(i, (j, id2))| {
                    i != j
                        || id2 == &id
                        || inputs.loaded.contains_key(id2.as_str())
                        || !seen.insert(id2.clone())
                })
            {
                return Err(PlanError::InvalidInput);
            }
            next.phase = if indexes.is_empty() {
                checkpoint(
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
                RunPhase::Tools(ToolsPhase {
                    kind: Default::default(),
                    batch: ToolBatch {
                        assistant_entry_id: id.clone(),
                        configuration: p.deferred.configuration.clone(),
                        turn_id: StepId::new(format!(
                            "{}:poll:{}",
                            p.deferred.step_id.as_str(),
                            p.deferred.poll.get()
                        ))
                        .map_err(|_| PlanError::InvalidInput)?,
                        calls: followers
                            .iter()
                            .map(|(source_index, result_entry_id)| ToolCallState::Planned {
                                source_index: *source_index,
                                result_entry_id: result_entry_id.clone(),
                            })
                            .collect(),
                    },
                })
            };
        }
    }
    let usage = message.usage.clone();
    let message = SettledAssistantMessage::new(message).map_err(|_| PlanError::InvalidInput)?;
    let state = OperationState::Run(Box::new(next));
    let mut result = recipe(current, facts, &commit.telemetry_context);
    result.expected_effect = Some(p.key);
    result.transaction.writes = vec![
        Write::Entry(NewEntry::Message(NewMessageEntry {
            id: id.clone(),
            parent_id: current.leaf_id.clone(),
            entry_type: Default::default(),
            message: AgentMessage::BuiltIn(Message::Assistant(Box::new(message.into_message()))),
            terminate: None,
        })),
        set(RegisterSet::LaneLeaf {
            key: current.operation.lane.as_str().into(),
            value: Some(id.clone()),
        }),
        Write::Usage(NewUsageRow {
            id: p.deferred.usage_id.clone(),
            usage,
            entry_id: Some(id.clone()),
            adjustment: false,
            details: Presence::Missing,
        }),
        state_write(current, state.clone()),
    ];
    result.next = Some(state);
    result.publication = vec![
        Publication::Entry(id),
        Publication::Usage(p.deferred.usage_id),
    ];
    Ok(RecipeDecision::Apply(Box::new(result)))
}
