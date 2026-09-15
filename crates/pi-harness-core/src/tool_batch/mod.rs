//! Source-ordered tool clearance, replay and result settlement.
#![allow(missing_docs)]
use crate::planner::*;
use pi_agent_schema::session_values::*;
use pi_agent_schema::*;

pub fn source(call: &ToolCallState) -> (SafeUnsigned, &EntryId) {
    match call {
        ToolCallState::Planned {
            source_index,
            result_entry_id,
        }
        | ToolCallState::EffectPending {
            source_index,
            result_entry_id,
            ..
        }
        | ToolCallState::Completed {
            source_index,
            result_entry_id,
            ..
        } => (*source_index, result_entry_id),
    }
}
pub fn args_key(current: &CurrentOperation, batch: &ToolBatch, index: SafeUnsigned) -> String {
    format!(
        "{}:{}:{}",
        current.operation.operation_id.as_str(),
        batch.turn_id.as_str(),
        index.get()
    )
}
pub(crate) fn assistant<'a, C>(
    inputs: &'a PlannerInputs<C>,
    id: &EntryId,
) -> Result<&'a AssistantMessage, PlanError> {
    match inputs.loaded.get(id.as_str()) {
        Some(PlannerLoadedValue::Entry(Entry::Message(entry))) => match &entry.message {
            AgentMessage::BuiltIn(Message::Assistant(message)) => Ok(message),
            _ => Err(PlanError::InvalidCurrent),
        },
        _ => Err(PlanError::MissingInput),
    }
}
fn call_at<'a, C>(
    inputs: &'a PlannerInputs<C>,
    batch: &ToolBatch,
    index: SafeUnsigned,
) -> Result<&'a ToolCall, PlanError> {
    let message = assistant(inputs, &batch.assistant_entry_id)?;
    let index = usize::try_from(index.get()).map_err(|_| PlanError::InvalidCurrent)?;
    match message.content.get(index) {
        Some(AssistantContent::ToolCall(call)) => Ok(call),
        _ => Err(PlanError::InvalidCurrent),
    }
}
fn effect<C: Clone>(
    current: &CurrentOperation,
    session: &SessionId,
    batch: &ToolBatch,
    index: SafeUnsigned,
    context: &C,
) -> Result<EffectPlan<C>, PlanError> {
    Ok(EffectPlan::Tool(ToolEffectPlan {
        key: key(
            current,
            session,
            EffectKind::Tool,
            batch.turn_id.as_str(),
            0,
            Some(index),
        )?,
        assistant_entry_id: batch.assistant_entry_id.clone(),
        source_index: index,
        args_key: args_key(current, batch, index),
        telemetry_context: context.clone(),
    }))
}
fn clearance<C: Clone>(
    current: &CurrentOperation,
    session: &SessionId,
    batch: &ToolBatch,
    index: SafeUnsigned,
    result: &EntryId,
    context: &C,
) -> Result<EffectPlan<C>, PlanError> {
    Ok(EffectPlan::ToolClearance(ToolClearancePlan {
        key: key(
            current,
            session,
            EffectKind::Hook,
            batch.turn_id.as_str(),
            0,
            Some(index),
        )?,
        assistant_entry_id: batch.assistant_entry_id.clone(),
        source_index: index,
        turn_id: batch.turn_id.clone(),
        result_entry_id: result.clone(),
        producing_configuration: batch.configuration.clone(),
        telemetry_context: context.clone(),
    }))
}
pub(crate) fn next<C: Clone>(
    current: &CurrentOperation,
    inputs: &PlannerInputs<C>,
    facts: &PlanningFacts<C>,
    batch: &ToolBatch,
) -> Result<ProcedureAction<C>, PlanError> {
    let PlanningFacts::Tools {
        session_id,
        telemetry_context,
        declarations,
        interrupted,
    } = facts
    else {
        return Err(PlanError::MissingInput);
    };
    let r = run(current)?;
    let Some(first) = batch
        .calls
        .iter()
        .position(|c| !matches!(c, ToolCallState::Completed { .. }))
    else {
        let trigger = batch.calls.last().map_or_else(
            || batch.assistant_entry_id.clone(),
            |call| source(call).1.clone(),
        );
        let all_terminate = !batch.calls.is_empty()
            && batch.calls.iter().all(|call| {
                matches!(
                    call,
                    ToolCallState::Completed {
                        terminate: true,
                        ..
                    }
                )
            });
        return Ok(transition(
            current,
            checkpoint(
                trigger,
                if all_terminate {
                    Continuation::MayFinish {
                        include_final_assistant: false,
                    }
                } else {
                    Continuation::NeedAssistant {
                        overflow_recovery_used: false,
                    }
                },
            ),
            telemetry_context,
        ));
    };
    let truncated = assistant(inputs, &batch.assistant_entry_id)?.stop_reason == StopReason::Length;
    let sequential = r.settings.tool_execution == ToolExecutionMode::Sequential
        || batch
            .calls
            .iter()
            .filter_map(|call| declarations.get(&source(call).0))
            .any(|d| d.execution_mode == ToolExecutionMode::Sequential);
    for (position, call) in batch.calls.iter().enumerate().skip(first) {
        let (index, result_id) = source(call);
        let _ = call_at(inputs, batch, index)?;
        let plan = match call {
            ToolCallState::Completed { .. } => continue,
            ToolCallState::Planned { .. } => clearance(
                current,
                session_id,
                batch,
                index,
                result_id,
                telemetry_context,
            )?,
            ToolCallState::EffectPending { .. } => {
                effect(current, session_id, batch, index, telemetry_context)?
            }
        };
        if let Some(live) = inputs.running.get(plan.key()) {
            if !plan_matches(&plan, live) {
                return Err(PlanError::InvalidInput);
            }
            if sequential || matches!(call, ToolCallState::Planned { .. }) {
                if position == first {
                    return Ok(ProcedureAction::Existing(Action::AwaitEffect(
                        plan.key().clone(),
                    )));
                }
                break;
            }
            continue;
        }
        let interrupted_reason = if cancelled(r) {
            Some(ReconcileReason::CancelledNotStarted)
        } else if truncated {
            Some(ReconcileReason::InterruptedTool)
        } else {
            match call {
                ToolCallState::EffectPending { replay, .. }
                    if *replay != ToolReplay::Safe
                        || declarations
                            .get(&index)
                            .is_none_or(|d| d.replay != ToolReplay::Safe) =>
                {
                    Some(ReconcileReason::InterruptedTool)
                }
                _ => None,
            }
        };
        if let Some(reason) = interrupted_reason {
            if position != first {
                break;
            }
            let output = ReconciledOutput::new(
                &plan,
                reason,
                ReconciledValue::Tool {
                    result: interrupted.clone(),
                    is_error: true,
                    terminate: false,
                },
            )
            .map_err(|_| PlanError::InvalidInput)?;
            return Ok(ProcedureAction::Reconcile(Box::new(SettlementCommit {
                current: current.clone(),
                plan,
                output: SettlementOutput::Reconciled(output),
                telemetry_context: telemetry_context.clone(),
            })));
        }
        if matches!(call, ToolCallState::EffectPending { .. }) {
            match inputs.loaded.get(&args_key(current, batch, index)) {
                Some(PlannerLoadedValue::Register(Register::OperationToolArguments {
                    key,
                    ..
                })) if key == &args_key(current, batch, index) => {}
                _ => return Err(PlanError::MissingInput),
            }
        }
        return Ok(dispatch(plan));
    }
    let (index, result) = source(&batch.calls[first]);
    let plan = match &batch.calls[first] {
        ToolCallState::Planned { .. } => {
            clearance(current, session_id, batch, index, result, telemetry_context)?
        }
        _ => effect(current, session_id, batch, index, telemetry_context)?,
    };
    if !inputs.running.contains_key(plan.key()) {
        return Err(PlanError::InvalidCurrent);
    }
    Ok(ProcedureAction::Existing(Action::AwaitEffect(
        plan.key().clone(),
    )))
}
pub(crate) fn settle<C: Clone>(
    current: &CurrentOperation,
    commit: &SettlementCommit<C>,
    inputs: &PlannerInputs<C>,
    facts: &CommitFacts,
    extra: &SettlementFacts,
) -> Result<RecipeDecision<C>, PlanError> {
    let r = run(current)?;
    let RunPhase::Tools(ToolsPhase { batch, .. }) = &r.phase else {
        return Ok(RecipeDecision::NoLongerExpected);
    };
    let Some(index) = commit.plan.key().item_index else {
        return Ok(RecipeDecision::NoLongerExpected);
    };
    let Some(position) = batch.calls.iter().position(|c| source(c).0 == index) else {
        return Ok(RecipeDecision::NoLongerExpected);
    };
    let call = &batch.calls[position];
    let result_id = source(call).1.clone();
    let expected = match call {
        ToolCallState::Planned { .. } => clearance(
            current,
            &facts.session_id,
            batch,
            index,
            &result_id,
            &commit.telemetry_context,
        )?,
        ToolCallState::EffectPending { .. } => effect(
            current,
            &facts.session_id,
            batch,
            index,
            &commit.telemetry_context,
        )?,
        ToolCallState::Completed { .. } => return Ok(RecipeDecision::NoLongerExpected),
    };
    let paired = match (&commit.plan, &expected) {
        (EffectPlan::Tool(a), EffectPlan::Tool(b)) => {
            a.key == b.key
                && a.assistant_entry_id == b.assistant_entry_id
                && a.source_index == b.source_index
                && a.args_key == b.args_key
        }
        (EffectPlan::ToolClearance(a), EffectPlan::ToolClearance(b)) => {
            a.key == b.key
                && a.assistant_entry_id == b.assistant_entry_id
                && a.source_index == b.source_index
                && a.turn_id == b.turn_id
                && a.result_entry_id == b.result_entry_id
                && a.producing_configuration == b.producing_configuration
        }
        _ => false,
    };
    if !paired {
        return Ok(RecipeDecision::NoLongerExpected);
    }
    let SettlementFacts::Tool {
        usage_id,
        declarations,
    } = extra
    else {
        return Err(PlanError::MissingInput);
    };
    let sequential = r.settings.tool_execution == ToolExecutionMode::Sequential
        || batch
            .calls
            .iter()
            .filter_map(|call| declarations.get(&source(call).0))
            .any(|d| d.execution_mode == ToolExecutionMode::Sequential);
    let mut recipe = recipe(current, facts, &commit.telemetry_context);
    recipe.expected_effect = Some(expected.key().clone());
    let mut updated = batch.clone();
    if matches!(call, ToolCallState::EffectPending { .. }) {
        let args = args_key(current, batch, index);
        match inputs.loaded.get(&args) {
            Some(PlannerLoadedValue::Register(Register::OperationToolArguments {
                key,
                seq,
                ..
            })) if key == &args => add_condition(
                &mut recipe.expected,
                condition(RegisterNamespace::OperationToolArguments, &args, Some(*seq)),
            )?,
            _ => return Err(PlanError::MissingInput),
        }
    }
    let (result, is_error, terminate) = match &commit.output {
        SettlementOutput::ToolClearance(o)
            if o.key() == expected.key()
                && o.assistant_entry_id() == &batch.assistant_entry_id
                && o.source_index() == index
                && o.result_entry_id() == &result_id
                && matches!(call, ToolCallState::Planned { .. }) =>
        {
            match o.outcome() {
                ToolClearanceOutcome::Prepared {
                    effective_args,
                    replay,
                } => {
                    if cancelled(r) {
                        return Ok(RecipeDecision::NoLongerExpected);
                    }
                    if batch.calls[..position].iter().any(|c| {
                        matches!(c, ToolCallState::Planned { .. })
                            || (sequential && !matches!(c, ToolCallState::Completed { .. }))
                    }) {
                        return Ok(RecipeDecision::NoLongerExpected);
                    }
                    let args = args_key(current, batch, index);
                    recipe.expected.push(condition(
                        RegisterNamespace::OperationToolArguments,
                        &args,
                        None,
                    ));
                    recipe
                        .transaction
                        .writes
                        .push(set(RegisterSet::OperationToolArguments {
                            key: args,
                            value: effective_args.clone(),
                        }));
                    updated.calls[position] = ToolCallState::EffectPending {
                        source_index: index,
                        result_entry_id: result_id.clone(),
                        replay: *replay,
                    };
                    let mut next = r.clone();
                    next.phase = RunPhase::Tools(ToolsPhase {
                        kind: Default::default(),
                        batch: updated,
                    });
                    let state = OperationState::Run(Box::new(next));
                    recipe
                        .transaction
                        .writes
                        .push(state_write(current, state.clone()));
                    recipe.next = Some(state);
                    recipe.dispatch = Some(effect(
                        current,
                        &facts.session_id,
                        batch,
                        index,
                        &commit.telemetry_context,
                    )?);
                    return Ok(RecipeDecision::Apply(Box::new(recipe)));
                }
                ToolClearanceOutcome::Immediate {
                    result,
                    is_error,
                    terminate,
                } => (result.clone(), *is_error, *terminate),
            }
        }
        SettlementOutput::Tool(o)
            if o.key == *expected.key() && matches!(call, ToolCallState::EffectPending { .. }) =>
        {
            (o.result.clone(), o.is_error, o.terminate)
        }
        SettlementOutput::Reconciled(o) if o.key() == expected.key() => {
            let valid = match o.reason() {
                ReconcileReason::CancelledNotStarted => cancelled(r),
                ReconcileReason::InterruptedTool => {
                    !inputs.running.contains_key(expected.key())
                        && (matches!(call,ToolCallState::EffectPending{replay,..} if *replay!=ToolReplay::Safe||declarations.get(&index).is_none_or(|d|d.replay!=ToolReplay::Safe))
                            || assistant(inputs, &batch.assistant_entry_id)?.stop_reason
                                == StopReason::Length)
                }
                ReconcileReason::UnknownAtCap => false,
            };
            if !valid {
                return Ok(RecipeDecision::NoLongerExpected);
            }
            let ReconciledValue::Tool {
                result,
                is_error,
                terminate,
            } = o.value()
            else {
                return Ok(RecipeDecision::NoLongerExpected);
            };
            if !is_error || *terminate || result.usage.is_some() {
                return Err(PlanError::InvalidInput);
            }
            (result.clone(), *is_error, *terminate)
        }
        _ => return Ok(RecipeDecision::NoLongerExpected),
    };
    if batch.calls[..position]
        .iter()
        .any(|c| !matches!(c, ToolCallState::Completed { .. }))
    {
        return Ok(RecipeDecision::NoLongerExpected);
    }

    if result.usage.is_some() != usage_id.is_some() {
        return Err(PlanError::InvalidInput);
    }
    let source_call = call_at(inputs, batch, index)?;
    let message = ToolResultMessage {
        role: Default::default(),
        tool_call_id: source_call.id.clone(),
        tool_name: source_call.name.clone(),
        content: result.content,
        details: Presence::Value(result.details),
        usage: result.usage.clone(),
        added_tool_names: result.added_tool_names,
        is_error,
        timestamp: inputs.now,
    };
    recipe
        .transaction
        .writes
        .push(Write::Entry(NewEntry::Message(NewMessageEntry {
            id: result_id.clone(),
            parent_id: current.leaf_id.clone(),
            entry_type: Default::default(),
            message: AgentMessage::BuiltIn(Message::ToolResult(Box::new(message))),
            terminate: if terminate { Some(LiteralTrue) } else { None },
        })));
    recipe.transaction.writes.push(set(RegisterSet::LaneLeaf {
        key: current.operation.lane.as_str().into(),
        value: Some(result_id.clone()),
    }));
    recipe
        .publication
        .push(Publication::Entry(result_id.clone()));
    if let (Some(usage), Some(id)) = (result.usage, usage_id) {
        recipe.transaction.writes.push(Write::Usage(NewUsageRow {
            id: id.clone(),
            usage,
            entry_id: Some(result_id.clone()),
            adjustment: false,
            details: Presence::Missing,
        }));
        recipe.publication.push(Publication::Usage(id.clone()))
    }
    updated.calls[position] = ToolCallState::Completed {
        source_index: index,
        result_entry_id: result_id.clone(),
        terminate,
    };
    let mut next = r.clone();
    if updated
        .calls
        .iter()
        .all(|c| matches!(c, ToolCallState::Completed { .. }))
    {
        for c in &updated.calls {
            let args = args_key(current, batch, source(c).0);
            add_condition(
                &mut recipe.expected,
                tool_arguments_condition(inputs, &args)?,
            )?;
            recipe
                .transaction
                .writes
                .push(delete(RegisterNamespace::OperationToolArguments, args))
        }
        let all_terminate = !updated.calls.is_empty()
            && updated.calls.iter().all(|c| {
                matches!(
                    c,
                    ToolCallState::Completed {
                        terminate: true,
                        ..
                    }
                )
            });
        next.phase = checkpoint(
            result_id,
            if all_terminate {
                Continuation::MayFinish {
                    include_final_assistant: false,
                }
            } else {
                Continuation::NeedAssistant {
                    overflow_recovery_used: false,
                }
            },
        )
    } else {
        next.phase = RunPhase::Tools(ToolsPhase {
            kind: Default::default(),
            batch: updated,
        })
    }
    let state = OperationState::Run(Box::new(next));
    recipe
        .transaction
        .writes
        .push(state_write(current, state.clone()));
    recipe.next = Some(state);
    Ok(RecipeDecision::Apply(Box::new(recipe)))
}
