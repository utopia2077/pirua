use crate::operation_state_model_support::{Model, n, ready, request, user};
use pi_agent_schema::session_values::*;
use pi_agent_schema::*;
use pi_agent_session::storage::ConditionalOutcome;
use pi_harness_core::{inbox, planner::*, transition::transition_recipe};
pub fn id(s: &str) -> EntryId {
    EntryId::new(s).unwrap()
}
pub fn enqueue(m: &Model, ids: &[&str]) {
    let (mut current, _, _) = m.current();
    let OperationState::Run(r) = &mut current.state else {
        panic!()
    };
    r.settings.steering_mode = QueueMode::OneAtATime;
    let mut writes = vec![];
    for name in ids {
        r.inbox.steer.push(id(name));
        writes.push(Write::SetRegister(Box::new(RegisterSet::PendingEntry {
            key: name.to_string(),
            value: PendingEntry {
                entry_type: PendingEntryType::Message,
                custom_type: None,
                payload: Presence::Value(
                    serde_json::from_value(serde_json::to_value(user()).unwrap()).unwrap(),
                ),
            },
        })));
    }
    writes.push(Write::SetRegister(Box::new(RegisterSet::OperationState {
        key: current.operation.operation_id.as_str().into(),
        value: Box::new(current.state),
    })));
    ready(m.session.commit(Transaction { writes })).unwrap();
}
pub fn selected_recipe(m: &Model) -> CommitRecipe<()> {
    let (c, i, f) = m.current();
    let selected = inbox::select(&c).unwrap().unwrap();
    let mut parent = c.leaf_id.clone();
    let placements = selected
        .ids
        .iter()
        .map(|id| {
            let Register::PendingEntry { value, seq, .. } = m
                .register(RegisterNamespace::PendingEntry, id.as_str())
                .unwrap()
            else {
                panic!()
            };
            let entry = NewEntry::Message(NewMessageEntry {
                id: id.clone(),
                parent_id: parent.clone(),
                entry_type: Default::default(),
                message: user(),
                terminate: None,
            });
            parent = Some(id.clone());
            PendingPlacement::new(entry, value, seq).unwrap()
        })
        .collect();
    let planning = PlanningFacts::Inbox {
        session_id: f.session_id.clone(),
        telemetry_context: (),
        placements,
        projecting: selected.ids.into_iter().collect(),
    };
    let ProcedureAction::Existing(Action::Transition(t)) = next_action(&c, &i, &planning).unwrap()
    else {
        panic!()
    };
    let commit = TransitionCommit {
        current: c.clone(),
        next: t.next,
        telemetry_context: (),
        expected_configuration_seq: t.expected_configuration_seq,
        expected_settings_revision: t.expected_settings_revision,
    };
    match transition_recipe(&c, &commit, &i, &f, &planning).unwrap() {
        RecipeDecision::Apply(r) => *r,
        _ => panic!(),
    }
}
pub fn publish(m: &Model, r: CommitRecipe<()>) -> ConditionalOutcome {
    ready(m.session.commit_conditional(request(r))).unwrap()
}
pub fn cancel(m: &Model) {
    let (c, _, f) = m.current();
    let OperationState::Run(r) = &c.state else {
        panic!()
    };
    let mut parent = c.leaf_id.clone();
    let payloads = r
        .inbox
        .steer
        .iter()
        .chain(&r.inbox.follow_up)
        .map(|id| {
            let Register::PendingEntry { value, seq, .. } = m
                .register(RegisterNamespace::PendingEntry, id.as_str())
                .unwrap()
            else {
                panic!()
            };
            let e = NewEntry::Message(NewMessageEntry {
                id: id.clone(),
                parent_id: parent.clone(),
                entry_type: Default::default(),
                message: user(),
                terminate: None,
            });
            parent = Some(id.clone());
            PendingPlacement::new(e, value, seq).unwrap()
        })
        .collect::<Vec<_>>();
    let r = pi_harness_core::control::abort_recipe(&c, &f, n(200), &payloads, &())
        .unwrap()
        .recipe
        .unwrap();
    assert!(matches!(publish(m, r), ConditionalOutcome::Applied(_)));
}
