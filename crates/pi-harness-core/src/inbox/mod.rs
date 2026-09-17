//! Pure inbox selection and atomic pending-entry placement.
#![allow(missing_docs)]
use crate::planner::*;
use pi_agent_schema::session_values::*;
use pi_agent_schema::*;
use std::collections::BTreeSet;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InboxKind {
    Writes,
    Steer,
    FollowUp,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InboxSelection {
    pub kind: InboxKind,
    pub ids: Vec<EntryId>,
}

pub fn select(current: &CurrentOperation) -> Result<Option<InboxSelection>, PlanError> {
    if !owned(current) {
        return Err(PlanError::InvalidCurrent);
    }
    let r = run(current)?;
    let eligible = if cancelled(r) {
        matches!(
            &r.phase,
            RunPhase::Checkpoint(_)
                | RunPhase::FailureDrain(_)
                | RunPhase::Assistant(AssistantPhase {
                    generation: Generation::Ready { .. } | Generation::RetryWait { .. },
                    ..
                })
                | RunPhase::Deferred(DeferredPhase {
                    deferred: Deferred::Suspended { .. },
                    ..
                })
        )
    } else {
        match &r.phase {
            RunPhase::Checkpoint(p) => p.skip_inbox_once != Some(true),
            RunPhase::FailureDrain(_) => true,
            _ => false,
        }
    };
    if !eligible {
        return Ok(None);
    }
    let selected = if !r.inbox.writes.is_empty() {
        Some((InboxKind::Writes, &r.inbox.writes, QueueMode::All))
    } else if cancelled(r) {
        None
    } else if !r.inbox.steer.is_empty() {
        Some((InboxKind::Steer, &r.inbox.steer, r.settings.steering_mode))
    } else if !r.inbox.follow_up.is_empty()
        && matches!(
            &r.phase,
            RunPhase::FailureDrain(_)
                | RunPhase::Checkpoint(CheckpointPhase {
                    continuation: Continuation::MayFinish { .. },
                    ..
                })
        )
    {
        Some((
            InboxKind::FollowUp,
            &r.inbox.follow_up,
            r.settings.follow_up_mode,
        ))
    } else {
        None
    };
    Ok(selected.map(|(kind, ids, mode)| InboxSelection {
        kind,
        ids: match mode {
            QueueMode::All => ids.clone(),
            QueueMode::OneAtATime => ids[..1].to_vec(),
        },
    }))
}
fn parts(entry: &NewEntry) -> Result<(&EntryId, &Option<EntryId>), PlanError> {
    match entry {
        NewEntry::Message(e) => Ok((&e.id, &e.parent_id)),
        NewEntry::Custom(e) => Ok((&e.id, &e.parent_id)),
        _ => Err(PlanError::InvalidInput),
    }
}
/// Shared prefix: for each selected item, append its entry then delete its payload.
pub fn placement_writes(
    current: &CurrentOperation,
    placements: &[PendingPlacement],
) -> Result<Vec<Write>, PlanError> {
    let selected = select(current)?.ok_or(PlanError::InvalidInput)?;
    if placements.len() != selected.ids.len() {
        return Err(PlanError::InvalidInput);
    }
    let mut parent = current.leaf_id.clone();
    let mut writes = Vec::new();
    let mut seen = BTreeSet::new();
    for (placement, selected_id) in placements.iter().zip(&selected.ids) {
        let (id, actual_parent) = parts(placement.entry())?;
        if id != selected_id
            || actual_parent != &parent
            || !seen.insert(id.clone())
            || parent.as_ref() == Some(id)
        {
            return Err(PlanError::InvalidInput);
        }
        if selected.kind != InboxKind::Writes && !matches!(placement.entry(), NewEntry::Message(_))
        {
            return Err(PlanError::InvalidInput);
        }
        parent = Some(id.clone());
        writes.push(Write::Entry(placement.entry().clone()));
        writes.push(delete(RegisterNamespace::PendingEntry, id.as_str().into()));
    }
    Ok(writes)
}
/// Stored assistant error/aborted/deferred entries are excluded before projection.
pub fn message_projects(message: &AgentMessage) -> bool {
    !matches!(message, AgentMessage::BuiltIn(Message::Assistant(m)) if matches!(m.stop_reason, StopReason::Error | StopReason::Aborted | StopReason::Deferred))
}
pub(crate) fn next_state(
    current: &CurrentOperation,
    placements: &[PendingPlacement],
    projecting: &BTreeSet<EntryId>,
) -> Result<OperationState, PlanError> {
    placement_writes(current, placements)?;
    let selected = select(current)?.ok_or(PlanError::InvalidInput)?;
    if projecting.iter().any(|id| !selected.ids.contains(id)) {
        return Err(PlanError::InvalidInput);
    }
    let mut r = run(current)?.clone();
    let is_cancelled = cancelled(&r);
    if is_cancelled && !projecting.is_empty() {
        return Err(PlanError::InvalidInput);
    }
    if !is_cancelled && selected.kind == InboxKind::Writes {
        for p in placements {
            if let NewEntry::Message(e) = p.entry()
                && projecting.contains(&e.id) != message_projects(&e.message)
            {
                return Err(PlanError::InvalidInput);
            }
        }
    }
    let ids = match selected.kind {
        InboxKind::Writes => &mut r.inbox.writes,
        InboxKind::Steer => &mut r.inbox.steer,
        InboxKind::FollowUp => &mut r.inbox.follow_up,
    };
    ids.retain(|id| !selected.ids.contains(id));
    if !is_cancelled && (selected.kind != InboxKind::Writes || !projecting.is_empty()) {
        let trigger = selected
            .ids
            .last()
            .cloned()
            .ok_or(PlanError::InvalidInput)?;
        r.phase = checkpoint(
            trigger,
            Continuation::NeedAssistant {
                overflow_recovery_used: false,
            },
        );
        if let RunPhase::Checkpoint(p) = &mut r.phase {
            p.skip_inbox_once = Some(true);
        }
    }
    Ok(OperationState::Run(Box::new(r)))
}
pub(crate) fn next<C: Clone>(
    current: &CurrentOperation,
    facts: &PlanningFacts<C>,
) -> Result<ProcedureAction<C>, PlanError> {
    let PlanningFacts::Inbox {
        placements,
        projecting,
        telemetry_context,
        ..
    } = facts
    else {
        return Err(PlanError::MissingInput);
    };
    Ok(ProcedureAction::Existing(Action::Transition(
        TransitionPlan {
            next: next_state(current, placements, projecting)?,
            telemetry_context: telemetry_context.clone(),
            expected_configuration_seq: None,
            expected_settings_revision: None,
        },
    )))
}
pub(crate) fn add_placement<C>(
    current: &CurrentOperation,
    placements: &[PendingPlacement],
    recipe: &mut CommitRecipe<C>,
) -> Result<(), PlanError> {
    recipe
        .transaction
        .writes
        .extend(placement_writes(current, placements)?);
    for p in placements {
        let (id, _) = parts(p.entry())?;
        add_condition(
            &mut recipe.expected,
            condition(RegisterNamespace::PendingEntry, id.as_str(), Some(p.seq())),
        )?;
        recipe.publication.push(Publication::Entry(id.clone()));
    }
    let leaf = parts(placements.last().ok_or(PlanError::InvalidInput)?.entry())?
        .0
        .clone();
    recipe.transaction.writes.push(set(RegisterSet::LaneLeaf {
        key: current.operation.lane.as_str().into(),
        value: Some(leaf),
    }));
    Ok(())
}
