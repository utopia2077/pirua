//! Conditional state transitions and atomic prompt acceptance.
#![allow(missing_docs)]
use crate::planner::*;
use pi_agent_schema::session_values::*;
use pi_agent_schema::*;

pub fn transition_recipe<C: Clone>(
    current: &CurrentOperation,
    commit: &TransitionCommit<C>,
    inputs: &PlannerInputs<C>,
    facts: &CommitFacts,
    planning: &PlanningFacts<C>,
) -> Result<RecipeDecision<C>, PlanError> {
    if !owned(current)
        || commit.current.operation != current.operation
        || commit.current.operation_state_seq != current.operation_state_seq
        || commit.current.lane_state_seq != current.lane_state_seq
    {
        return Ok(RecipeDecision::NoLongerExpected);
    }
    let ProcedureAction::Existing(Action::Transition(expected)) =
        next_action(current, inputs, planning)?
    else {
        return Ok(RecipeDecision::NoLongerExpected);
    };
    if expected.next != commit.next
        || expected.expected_configuration_seq != commit.expected_configuration_seq
        || expected.expected_settings_revision != commit.expected_settings_revision
    {
        return Ok(RecipeDecision::NoLongerExpected);
    }
    let mut recipe = recipe(current, facts, &commit.telemetry_context);
    if let Some(seq) = commit.expected_configuration_seq {
        recipe.expected.push(condition(
            RegisterNamespace::LaneConfig,
            current.operation.lane.as_str(),
            Some(seq),
        ))
    }
    recipe.expected_settings_revision = commit.expected_settings_revision;
    if let OperationState::Run(r) = &current.state
        && let RunPhase::Tools(p) = &r.phase
        && p.batch
            .calls
            .iter()
            .all(|call| matches!(call, ToolCallState::Completed { .. }))
    {
        for call in &p.batch.calls {
            let key =
                crate::tool_batch::args_key(current, &p.batch, crate::tool_batch::source(call).0);
            add_condition(
                &mut recipe.expected,
                tool_arguments_condition(inputs, &key)?,
            )?;
            recipe
                .transaction
                .writes
                .push(delete(RegisterNamespace::OperationToolArguments, key));
        }
    }
    recipe
        .transaction
        .writes
        .push(state_write(current, commit.next.clone()));
    recipe.next = Some(commit.next.clone());
    Ok(RecipeDecision::Apply(Box::new(recipe)))
}
pub type CapturedPending = PendingPlacement;
#[derive(Clone, Debug, PartialEq)]
pub struct AcceptanceFacts<C> {
    pub operation: Operation,
    pub lane_state: LaneState,
    pub lane_seq: SafeUnsigned,
    pub leaf_seq: Option<SafeUnsigned>,
    pub configuration_seq: SafeUnsigned,
    pub settings_revision: SafeUnsigned,
    pub settings: RunSettings,
    pub captured: Vec<CapturedPending>,
    pub messages: Vec<NewMessageEntry>,
    pub injections: Vec<NewMessageEntry>,
    pub skip_inbox_once: Option<bool>,
    pub identities_present: bool,
    pub telemetry_context: C,
}
fn entry_parts(entry: &NewEntry) -> Result<(&EntryId, &Option<EntryId>), PlanError> {
    match entry {
        NewEntry::Message(e) => Ok((&e.id, &e.parent_id)),
        NewEntry::Custom(e) => Ok((&e.id, &e.parent_id)),
        _ => Err(PlanError::InvalidInput),
    }
}
pub fn acceptance_recipe<C: Clone>(
    facts: &AcceptanceFacts<C>,
) -> Result<CommitRecipe<C>, PlanError> {
    if facts.lane_state.current_operation_id.is_some() || !facts.identities_present {
        return Err(PlanError::InvalidInput);
    }
    let OperationIntent::Run(intent) = &facts.operation.intent else {
        return Err(PlanError::LaterSlice);
    };
    if facts.captured.is_empty() && facts.messages.is_empty() && facts.injections.is_empty() {
        return Err(PlanError::InvalidInput);
    }
    let mut ids = std::collections::BTreeSet::new();
    let mut captured_ids = vec![];
    let mut parent = facts.operation.source_leaf_id.clone();
    let mut writes = vec![];
    let mut publication = vec![];
    let lane = facts.operation.lane.as_str();
    let op = facts.operation.operation_id.as_str();
    let mut expected = vec![
        condition(RegisterNamespace::LaneState, lane, Some(facts.lane_seq)),
        condition(RegisterNamespace::LaneLeaf, lane, facts.leaf_seq),
        condition(
            RegisterNamespace::LaneConfig,
            lane,
            Some(facts.configuration_seq),
        ),
        condition(RegisterNamespace::OperationMeta, op, None),
        condition(RegisterNamespace::OperationState, op, None),
    ];
    for captured in &facts.captured {
        let (id, entry_parent) = entry_parts(captured.entry())?;
        if entry_parent != &parent
            || !ids.insert(id.clone())
            || !facts.lane_state.pending_next_run.contains(id)
        {
            return Err(PlanError::InvalidInput);
        }
        captured_ids.push(id.clone());
        parent = Some(id.clone());
        expected.push(condition(
            RegisterNamespace::PendingEntry,
            id.as_str(),
            Some(captured.seq()),
        ));
        writes.push(Write::Entry(captured.entry().clone()));
        publication.push(Publication::Entry(id.clone()));
    }
    let ordered: Vec<_> = facts
        .lane_state
        .pending_next_run
        .iter()
        .filter(|id| ids.contains(*id))
        .cloned()
        .collect();
    if ordered != captured_ids {
        return Err(PlanError::InvalidInput);
    }
    let mut prompt = vec![];
    for (position, entry) in facts.messages.iter().chain(&facts.injections).enumerate() {
        if entry.parent_id != parent || !ids.insert(entry.id.clone()) || entry.terminate.is_some() {
            return Err(PlanError::InvalidInput);
        }
        if let AgentMessage::BuiltIn(Message::Assistant(message)) = &entry.message {
            SettledAssistantMessage::new(message.as_ref().clone())
                .map_err(|_| PlanError::InvalidInput)?;
        }
        parent = Some(entry.id.clone());
        if position < facts.messages.len() {
            prompt.push(entry.id.clone())
        };
        writes.push(Write::Entry(NewEntry::Message(entry.clone())));
        publication.push(Publication::Entry(entry.id.clone()));
    }
    if intent.prompt_entry_ids != prompt {
        return Err(PlanError::InvalidInput);
    }
    for id in &captured_ids {
        writes.push(delete(RegisterNamespace::PendingEntry, id.as_str().into()))
    }
    let trigger = parent.clone().ok_or(PlanError::InvalidInput)?;
    let mut phase = checkpoint(
        trigger,
        Continuation::NeedAssistant {
            overflow_recovery_used: false,
        },
    );
    if let RunPhase::Checkpoint(p) = &mut phase {
        p.skip_inbox_once = facts.skip_inbox_once
    }
    let state = OperationState::Run(Box::new(RunState {
        kind: Default::default(),
        control: Control::Running(Default::default()),
        settings: facts.settings.clone(),
        phase,
        inbox: Default::default(),
        latest_assistant_entry_id: None,
    }));
    writes.push(set(RegisterSet::LaneLeaf {
        key: lane.into(),
        value: parent,
    }));
    writes.push(set(RegisterSet::OperationMeta {
        key: op.into(),
        value: facts.operation.clone(),
    }));
    writes.push(set(RegisterSet::OperationState {
        key: op.into(),
        value: Box::new(state.clone()),
    }));
    let mut lane_state = facts.lane_state.clone();
    lane_state.current_operation_id = Some(facts.operation.operation_id.clone());
    lane_state
        .pending_next_run
        .retain(|id| !captured_ids.contains(id));
    writes.push(set(RegisterSet::LaneState {
        key: lane.into(),
        value: lane_state,
    }));
    Ok(CommitRecipe {
        expected,
        expected_settings_revision: Some(facts.settings_revision),
        expected_effect: None,
        transaction: Transaction { writes },
        next: Some(state),
        dispatch: None,
        result: None,
        publication,
        telemetry_context: facts.telemetry_context.clone(),
    })
}
