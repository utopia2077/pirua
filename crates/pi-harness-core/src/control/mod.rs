//! Orthogonal cancellation marker and exact idempotent abort payloads.
#![allow(missing_docs)]
use crate::planner::*;
use pi_agent_schema::session_values::*;
use pi_agent_schema::*;

#[derive(Clone, Debug, PartialEq)]
pub struct AbortDecision<C> {
    pub result: AbortOutcome,
    pub recipe: Option<CommitRecipe<C>>,
}
/// Payload placements are checked proofs, in drained steer then follow-up order.
/// They are never placed or deleted by this marker transaction.
pub fn abort_recipe<C: Clone>(
    current: &CurrentOperation,
    facts: &CommitFacts,
    now: SafeUnsigned,
    payloads: &[PendingPlacement],
    context: &C,
) -> Result<AbortDecision<C>, PlanError> {
    if !owned(current) {
        return Err(PlanError::InvalidCurrent);
    }
    if !matches!(current.state, OperationState::Run(_)) {
        if !payloads.is_empty() {
            return Err(PlanError::InvalidInput);
        }
        let result = AbortOutcome {
            run_id: current.operation.operation_id.clone(),
            steer: vec![],
            follow_up: vec![],
        };
        if crate::structural::is_cancelled(current) {
            return Ok(AbortDecision {
                result,
                recipe: None,
            });
        }
        let mut state = current.state.clone();
        let c = Control::CancelRequested(CancelRequestedControl {
            status: Default::default(),
            requested_at: now,
            drained_steer: vec![],
            drained_follow_up: vec![],
        });
        match &mut state {
            OperationState::Compaction(s) => s.control = c,
            OperationState::Navigation(NavigationState::Summarized(s)) => s.control = c,
            OperationState::Navigation(NavigationState::Unsummarized(s)) => s.control = c,
            _ => return Err(PlanError::InvalidCurrent),
        }
        let mut r = recipe(current, facts, context);
        r.transaction
            .writes
            .push(state_write(current, state.clone()));
        r.next = Some(state);
        return Ok(AbortDecision {
            result,
            recipe: Some(r),
        });
    }
    let run = run(current)?;
    let (steer, follow_up) = match &run.control {
        Control::Running(_) => (&run.inbox.steer, &run.inbox.follow_up),
        Control::CancelRequested(c) => (&c.drained_steer, &c.drained_follow_up),
    };
    if payloads.len() != steer.len() + follow_up.len() {
        return Err(PlanError::MissingInput);
    }
    let mut messages = Vec::new();
    let mut conditions = Vec::new();
    for (id, p) in steer.iter().chain(follow_up).zip(payloads) {
        let NewEntry::Message(e) = p.entry() else {
            return Err(PlanError::InvalidInput);
        };
        if &e.id != id {
            return Err(PlanError::InvalidInput);
        }
        messages.push(e.message.clone());
        conditions.push(condition(
            RegisterNamespace::PendingEntry,
            id.as_str(),
            Some(p.seq()),
        ));
    }
    let follow_messages = messages.split_off(steer.len());
    let result = AbortOutcome {
        run_id: current.operation.operation_id.clone(),
        steer: messages,
        follow_up: follow_messages,
    };
    if cancelled(run) {
        return Ok(AbortDecision {
            result,
            recipe: None,
        });
    }
    let mut next = run.clone();
    next.control = Control::CancelRequested(CancelRequestedControl {
        status: Default::default(),
        requested_at: now,
        drained_steer: std::mem::take(&mut next.inbox.steer),
        drained_follow_up: std::mem::take(&mut next.inbox.follow_up),
    });
    let state = OperationState::Run(Box::new(next));
    let mut recipe = recipe(current, facts, context);
    for c in conditions {
        add_condition(&mut recipe.expected, c)?;
    }
    recipe
        .transaction
        .writes
        .push(state_write(current, state.clone()));
    recipe.next = Some(state);
    Ok(AbortDecision {
        result,
        recipe: Some(recipe),
    })
}
