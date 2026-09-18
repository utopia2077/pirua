//! Exact run results and owned-register terminal cleanup.
#![allow(missing_docs)]
use crate::planner::*;
use pi_agent_schema::session_values::*;
use pi_agent_schema::*;

#[derive(Clone, Debug, PartialEq)]
pub struct TerminalFacts {
    pub registers: Vec<RegisterCondition>,
}
pub fn operation_result<C>(
    current: &CurrentOperation,
    inputs: &PlannerInputs<C>,
) -> Result<OperationResult, PlanError> {
    let r = run(current)?;
    let leaf = current.leaf_id.clone().ok_or(PlanError::InvalidCurrent)?;
    let latest = match &r.latest_assistant_entry_id {
        Some(id) => Some((
            id.clone(),
            SettledAssistantMessage::new(crate::tool_batch::assistant(inputs, id)?.clone())
                .map_err(|_| PlanError::InvalidCurrent)?,
        )),
        None => None,
    };
    let outcome = if cancelled(r) {
        match &r.phase {
            RunPhase::Assistant(AssistantPhase {
                generation: Generation::EffectPending { .. },
                ..
            })
            | RunPhase::Tools(_) => return Err(PlanError::InvalidCurrent),
            RunPhase::Deferred(DeferredPhase {
                deferred: Deferred::EffectPending { .. },
                ..
            }) => return Err(PlanError::InvalidCurrent),
            RunPhase::Compaction(_) => return Err(PlanError::LaterSlice),
            _ => {}
        }
        if !r.inbox.writes.is_empty() {
            return Err(PlanError::LaterSlice);
        }
        match latest {
            Some((id, message)) => RunOutcome::AbortedWithAssistant(RunAbortedWithAssistant {
                kind: Default::default(),
                leaf_id: leaf,
                final_entry_id: id,
                final_message: message,
            }),
            None => RunOutcome::AbortedWithoutAssistant(RunAbortedWithoutAssistant {
                kind: Default::default(),
                leaf_id: leaf,
            }),
        }
    } else {
        match &r.phase {
            RunPhase::Checkpoint(p) if inbox_empty(r) => match p.continuation {
                Continuation::MayFinish {
                    include_final_assistant: true,
                } => {
                    let (id, message) = latest.ok_or(PlanError::InvalidCurrent)?;
                    RunOutcome::CompletedWithAssistant(RunCompletedWithAssistant {
                        kind: Default::default(),
                        leaf_id: leaf,
                        final_entry_id: id,
                        final_message: message,
                    })
                }
                Continuation::MayFinish {
                    include_final_assistant: false,
                } => RunOutcome::CompletedWithoutAssistant(RunCompletedWithoutAssistant {
                    kind: Default::default(),
                    leaf_id: leaf,
                }),
                Continuation::NeedAssistant { .. } => return Err(PlanError::InvalidCurrent),
            },
            RunPhase::FailureDrain(p) if inbox_empty(r) => match latest {
                Some((id, message)) => {
                    RunOutcome::FailedWithAssistant(Box::new(RunFailedWithAssistant {
                        kind: Default::default(),
                        leaf_id: leaf,
                        error: p.error.clone(),
                        final_entry_id: id,
                        final_message: message,
                    }))
                }
                None => RunOutcome::FailedWithoutAssistant(RunFailedWithoutAssistant {
                    kind: Default::default(),
                    leaf_id: leaf,
                    error: p.error.clone(),
                }),
            },
            RunPhase::Deferred(DeferredPhase {
                deferred:
                    Deferred::Suspended {
                        source_entry_id, ..
                    }
                    | Deferred::EffectPending {
                        source_entry_id, ..
                    },
                ..
            }) => {
                let message = crate::tool_batch::assistant(inputs, source_entry_id)?;
                RunOutcome::Deferred(RunDeferred {
                    kind: Default::default(),
                    reason: Default::default(),
                    leaf_id: leaf,
                    final_entry_id: source_entry_id.clone(),
                    deferred: message.deferred.clone().ok_or(PlanError::InvalidCurrent)?,
                })
            }
            _ => return Err(PlanError::LaterSlice),
        }
    };
    Ok(OperationResult::Run(Box::new(outcome)))
}
fn last_result(
    current: &CurrentOperation,
    result: &OperationResult,
) -> Result<LaneLastResult, PlanError> {
    let OperationResult::Run(outcome) = result else {
        return crate::structural::last_result(current, result);
    };
    let op = current.operation.operation_id.clone();
    let leaf = current.leaf_id.clone();
    Ok(match outcome.as_ref() {
        RunOutcome::CompletedWithAssistant(o) => {
            LaneLastResult::RunCompletedAssistant(RunCompletedAssistantLastResult {
                operation_id: op,
                kind: Default::default(),
                leaf_id: leaf,
                final_assistant_entry_id: o.final_entry_id.clone(),
                outcome: Default::default(),
                run_completion: Default::default(),
            })
        }
        RunOutcome::CompletedWithoutAssistant(_) => {
            LaneLastResult::RunCompletedTerminatedTools(RunCompletedTerminatedToolsLastResult {
                operation_id: op,
                kind: Default::default(),
                leaf_id: leaf,
                outcome: Default::default(),
                run_completion: Default::default(),
            })
        }
        RunOutcome::AbortedWithAssistant(o) => LaneLastResult::RunAborted(RunAbortedLastResult {
            operation_id: op,
            kind: Default::default(),
            leaf_id: leaf,
            final_assistant_entry_id: Some(o.final_entry_id.clone()),
            outcome: Default::default(),
        }),
        RunOutcome::AbortedWithoutAssistant(_) => {
            LaneLastResult::RunAborted(RunAbortedLastResult {
                operation_id: op,
                kind: Default::default(),
                leaf_id: leaf,
                final_assistant_entry_id: None,
                outcome: Default::default(),
            })
        }
        RunOutcome::FailedWithAssistant(o) => LaneLastResult::RunFailed(RunFailedLastResult {
            operation_id: op,
            kind: Default::default(),
            leaf_id: leaf,
            final_assistant_entry_id: Some(o.final_entry_id.clone()),
            outcome: Default::default(),
            error: o.error.clone(),
        }),
        RunOutcome::FailedWithoutAssistant(o) => LaneLastResult::RunFailed(RunFailedLastResult {
            operation_id: op,
            kind: Default::default(),
            leaf_id: leaf,
            final_assistant_entry_id: None,
            outcome: Default::default(),
            error: o.error.clone(),
        }),
        RunOutcome::Deferred(_) | RunOutcome::MissingIdentities(_) => {
            return Err(PlanError::InvalidInput);
        }
    })
}
pub fn terminal_recipe<C: Clone>(
    current: &CurrentOperation,
    commit: &TerminalCommit<C>,
    inputs: &PlannerInputs<C>,
    facts: &CommitFacts,
    cleanup: &TerminalFacts,
) -> Result<RecipeDecision<C>, PlanError> {
    if !owned(current)
        || commit.current.operation != current.operation
        || commit.current.operation_state_seq != current.operation_state_seq
        || commit.current.lane_state_seq != current.lane_state_seq
    {
        return Ok(RecipeDecision::NoLongerExpected);
    }
    if !matches!(current.state, OperationState::Run(_)) {
        return crate::structural::terminal_recipe(
            current,
            commit,
            facts,
            cleanup,
            &crate::structural::Settlement::default(),
        );
    }
    let result = operation_result(current, inputs)?;
    if result != commit.result {
        return Ok(RecipeDecision::NoLongerExpected);
    }
    finish_recipe(
        current,
        &result,
        facts,
        cleanup,
        vec![],
        &commit.telemetry_context,
    )
}
pub(crate) fn finish_recipe<C: Clone>(
    current: &CurrentOperation,
    result: &OperationResult,
    facts: &CommitFacts,
    cleanup: &TerminalFacts,
    prefix: Vec<Write>,
    telemetry: &C,
) -> Result<RecipeDecision<C>, PlanError> {
    let last = last_result(current, result)?;
    let mut pending: std::collections::BTreeSet<_> = match &current.state {
        OperationState::Run(r) => r
            .inbox
            .steer
            .iter()
            .chain(&r.inbox.follow_up)
            .chain(&r.inbox.writes)
            .cloned()
            .collect(),
        _ => Default::default(),
    };
    if let Control::CancelRequested(c) = crate::structural::control(&current.state) {
        pending.extend(c.drained_steer.iter().chain(&c.drained_follow_up).cloned());
    }
    if pending
        .iter()
        .any(|id| current.lane_state.pending_next_run.contains(id))
    {
        return Err(PlanError::InvalidCurrent);
    }
    let register_prefix = format!("{}:", current.operation.operation_id.as_str());
    let mut seen = std::collections::BTreeSet::new();
    let mut pending_seen = std::collections::BTreeSet::new();
    let mut recipe = recipe(current, facts, telemetry);
    for write in &prefix {
        match write {
            Write::Entry(e) => recipe.publication.push(Publication::Entry(match e {
                NewEntry::Compaction(e) => e.id.clone(),
                NewEntry::BranchSummary(e) => e.id.clone(),
                _ => return Err(PlanError::InvalidInput),
            })),
            Write::Usage(u) => recipe.publication.push(Publication::Usage(u.id.clone())),
            _ => {}
        }
    }
    recipe.transaction.writes = prefix;
    recipe.transaction.writes.push(delete(
        RegisterNamespace::OperationMeta,
        current.operation.operation_id.as_str().into(),
    ));
    recipe.transaction.writes.push(delete(
        RegisterNamespace::OperationState,
        current.operation.operation_id.as_str().into(),
    ));
    for register in &cleanup.registers {
        if register.seq.is_none() || !seen.insert((register.namespace, register.key.clone())) {
            return Err(PlanError::InvalidInput);
        }
        match register.namespace {
            RegisterNamespace::OperationToolArguments | RegisterNamespace::OperationPreparation
                if register.key.starts_with(&register_prefix) => {}
            RegisterNamespace::PendingEntry => {
                let id = EntryId::new(register.key.clone()).map_err(|_| PlanError::InvalidInput)?;
                if !pending.contains(&id) {
                    return Err(PlanError::InvalidInput);
                }
                pending_seen.insert(id);
            }
            _ => return Err(PlanError::InvalidInput),
        }
        recipe.expected.push(register.clone());
        recipe
            .transaction
            .writes
            .push(delete(register.namespace, register.key.clone()));
    }
    if pending_seen != pending {
        return Err(PlanError::MissingInput);
    }
    recipe
        .transaction
        .writes
        .push(set(RegisterSet::LaneLastResult {
            key: current.operation.lane.as_str().into(),
            value: last,
        }));
    let mut lane = current.lane_state.clone();
    lane.current_operation_id = None;
    recipe.transaction.writes.push(set(RegisterSet::LaneState {
        key: current.operation.lane.as_str().into(),
        value: lane,
    }));
    recipe.result = Some(result.clone());
    recipe
        .publication
        .push(Publication::Terminal(Box::new(result.clone())));
    Ok(RecipeDecision::Apply(Box::new(recipe)))
}
pub(crate) fn settle_hook<C: Clone>(
    current: &CurrentOperation,
    commit: &SettlementCommit<C>,
    inputs: &PlannerInputs<C>,
    facts: &CommitFacts,
    extra: &SettlementFacts,
) -> Result<RecipeDecision<C>, PlanError> {
    let r = run(current)?;
    let RunPhase::Checkpoint(p) = &r.phase else {
        return Ok(RecipeDecision::NoLongerExpected);
    };
    if !matches!(p.continuation, Continuation::MayFinish { .. }) || !inbox_empty(r) || cancelled(r)
    {
        return Ok(RecipeDecision::NoLongerExpected);
    }
    let expected = key(
        current,
        &facts.session_id,
        EffectKind::Hook,
        p.trigger_entry_id.as_str(),
        0,
        None,
    )?;
    let EffectPlan::Hook(h) = &commit.plan else {
        return Ok(RecipeDecision::NoLongerExpected);
    };
    let HookEvent::BeforeRunEnd(event) = &h.event else {
        return Ok(RecipeDecision::NoLongerExpected);
    };
    let SettlementOutput::Hook(o) = &commit.output else {
        return Ok(RecipeDecision::NoLongerExpected);
    };
    if h.key != expected
        || o.key() != &expected
        || event.lane != current.operation.lane
        || event.run_id != current.operation.operation_id
    {
        return Ok(RecipeDecision::NoLongerExpected);
    }
    let HookResult::BeforeRunEnd(result) = o.result() else {
        return Ok(RecipeDecision::NoLongerExpected);
    };
    let SettlementFacts::RunEnd { follow_up } = extra else {
        return Err(PlanError::MissingInput);
    };
    let mut recipe = recipe(current, facts, &commit.telemetry_context);
    recipe.expected_effect = Some(expected);
    if let Some(text) = result.as_ref().and_then(|r| r.follow_up.as_ref()) {
        let entry = follow_up.as_ref().ok_or(PlanError::MissingInput)?;
        if entry.parent_id != current.leaf_id {
            return Err(PlanError::InvalidInput);
        }
        let AgentMessage::BuiltIn(Message::User(user)) = &entry.message else {
            return Err(PlanError::InvalidInput);
        };
        if user.content != UserContent::Text(text.clone()) || entry.terminate.is_some() {
            return Err(PlanError::InvalidInput);
        }
        let mut next = r.clone();
        next.phase = checkpoint(
            entry.id.clone(),
            Continuation::NeedAssistant {
                overflow_recovery_used: false,
            },
        );
        recipe
            .transaction
            .writes
            .push(Write::Entry(NewEntry::Message(entry.clone())));
        recipe.transaction.writes.push(set(RegisterSet::LaneLeaf {
            key: current.operation.lane.as_str().into(),
            value: Some(entry.id.clone()),
        }));
        let state = OperationState::Run(Box::new(next));
        recipe
            .transaction
            .writes
            .push(state_write(current, state.clone()));
        recipe.next = Some(state);
        recipe
            .publication
            .push(Publication::Entry(entry.id.clone()));
    } else {
        if follow_up.is_some() {
            return Err(PlanError::InvalidInput);
        }
        return Ok(RecipeDecision::Terminal(Box::new(TerminalCommit {
            current: current.clone(),
            result: operation_result(current, inputs)?,
            telemetry_context: commit.telemetry_context.clone(),
        })));
    }
    Ok(RecipeDecision::Apply(Box::new(recipe)))
}
