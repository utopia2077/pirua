use pi_agent_schema::session_values::*;
use pi_agent_schema::*;
use pi_harness_core::planner::*;
use serde_json::json;
use std::collections::BTreeSet;
pub fn id(s: &str) -> EntryId {
    EntryId::new(s).unwrap()
}
pub fn user() -> AgentMessage {
    serde_json::from_value(json!({"role":"user","content":"queued","timestamp":1})).unwrap()
}
pub fn run_mut(c: &mut CurrentOperation) -> &mut RunState {
    let OperationState::Run(r) = &mut c.state else {
        panic!()
    };
    r
}
pub fn place(
    c: &CurrentOperation,
    ids: &[EntryId],
    message: Option<AgentMessage>,
) -> Vec<PendingPlacement> {
    let mut parent = c.leaf_id.clone();
    ids.iter()
        .enumerate()
        .map(|(i, id)| {
            let entry = if let Some(message) = &message {
                NewEntry::Message(NewMessageEntry {
                    id: id.clone(),
                    parent_id: parent.clone(),
                    entry_type: Default::default(),
                    message: message.clone(),
                    terminate: None,
                })
            } else {
                NewEntry::Custom(NewCustomEntry {
                    id: id.clone(),
                    parent_id: parent.clone(),
                    entry_type: Default::default(),
                    custom_type: CustomType::new("note").unwrap(),
                    data: Presence::Missing,
                })
            };
            let pending = if let Some(message) = &message {
                PendingEntry {
                    entry_type: PendingEntryType::Message,
                    custom_type: None,
                    payload: Presence::Value(
                        serde_json::from_value(serde_json::to_value(message).unwrap()).unwrap(),
                    ),
                }
            } else {
                PendingEntry {
                    entry_type: PendingEntryType::Custom,
                    custom_type: Some(CustomType::new("note").unwrap()),
                    payload: Presence::Missing,
                }
            };
            parent = Some(id.clone());
            PendingPlacement::new(entry, pending, SafeUnsigned::new(20 + i as u64).unwrap())
                .unwrap()
        })
        .collect()
}
pub fn inbox(
    c: &CurrentOperation,
    projecting: bool,
    message: Option<AgentMessage>,
) -> PlanningFacts<&'static str> {
    let selection = pi_harness_core::inbox::select(c).unwrap().unwrap();
    PlanningFacts::Inbox {
        session_id: SessionId::new("session").unwrap(),
        telemetry_context: "test",
        placements: place(c, &selection.ids, message),
        projecting: if projecting {
            selection.ids.into_iter().collect()
        } else {
            BTreeSet::new()
        },
    }
}
pub fn transition(
    c: &CurrentOperation,
    i: &PlannerInputs<&'static str>,
    f: &CommitFacts,
    p: &PlanningFacts<&'static str>,
) -> CommitRecipe<&'static str> {
    let ProcedureAction::Existing(Action::Transition(t)) = next_action(c, i, p).unwrap() else {
        panic!()
    };
    let commit = TransitionCommit {
        current: c.clone(),
        next: t.next,
        telemetry_context: t.telemetry_context,
        expected_configuration_seq: t.expected_configuration_seq,
        expected_settings_revision: t.expected_settings_revision,
    };
    match pi_harness_core::transition::transition_recipe(c, &commit, i, f, p).unwrap() {
        RecipeDecision::Apply(r) => *r,
        _ => panic!(),
    }
}
pub fn deferred_event(c: &CurrentOperation) -> BeforeRequestEvent {
    let OperationState::Run(r) = &c.state else {
        panic!()
    };
    let RunPhase::Deferred(p) = &r.phase else {
        panic!()
    };
    let (config, poll) = match &p.deferred {
        Deferred::Suspended {
            configuration,
            poll,
            ..
        } => (configuration, poll.get() + 1),
        Deferred::EffectPending {
            configuration,
            poll,
            ..
        } => (configuration, poll.get()),
    };
    serde_json::from_value(json!({"lane":c.operation.lane,"runId":c.operation.operation_id,"model":{"id":config.model.model_id,"provider":config.model.provider,"name":"test","api":"test","baseUrl":"https://example.invalid","reasoning":false,"input":["text"],"cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0},"contextWindow":1000,"maxTokens":100},"step":"deferred","attempt":poll,"streamOptions":{"deferred":false}})).unwrap()
}
pub fn deferred_facts(c: &CurrentOperation) -> PlanningFacts<&'static str> {
    PlanningFacts::Deferred {
        session_id: SessionId::new("session").unwrap(),
        telemetry_context: "test",
        request: Some(Box::new(deferred_event(c))),
        synthetic: None,
    }
}
pub fn poll(
    c: &CurrentOperation,
    i: &PlannerInputs<&'static str>,
    f: &CommitFacts,
    response: &str,
    usage: &str,
) -> CommitRecipe<&'static str> {
    let ProcedureAction::Existing(Action::Dispatch(d)) =
        next_action(c, i, &deferred_facts(c)).unwrap()
    else {
        panic!()
    };
    assert!(!d.consume_deferred_poll);
    let EffectPlan::Hook(h) = &d.effect else {
        panic!()
    };
    let o = RequestOptionsOutput::new(
        h,
        AgentHarnessStreamOptions {
            deferred: Some(DeferredRequest::Enabled(false)),
            ..Default::default()
        },
    )
    .unwrap();
    let commit = SettlementCommit {
        current: c.clone(),
        plan: d.effect,
        output: SettlementOutput::RequestOptions(o),
        telemetry_context: "test",
    };
    match settlement_recipe(
        c,
        &commit,
        i,
        f,
        &SettlementFacts::Request {
            response_entry_id: id(response),
            usage_id: UsageId::new(usage).unwrap(),
        },
    )
    .unwrap()
    {
        RecipeDecision::Apply(r) => *r,
        _ => panic!(),
    }
}
