use pi_agent_schema::session_values::*;
use pi_agent_schema::*;
use pi_harness_core::planner::*;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
pub fn n(v: u64) -> SafeUnsigned {
    SafeUnsigned::new(v).unwrap()
}
pub fn fixture(name: &str) -> (CurrentOperation, PlannerInputs<&'static str>, CommitFacts) {
    let cases: Vec<Value> = serde_json::from_str(include_str!(
        "../../../../conformance/harness/r1-r4/core/states.json"
    ))
    .unwrap();
    let case = cases.iter().find(|c| c["name"] == name).unwrap();
    let roots = &case["roots"];
    let current = CurrentOperation {
        operation: serde_json::from_value(roots["operation_meta"]["value"].clone()).unwrap(),
        state: serde_json::from_value(roots["operation_state"]["value"].clone()).unwrap(),
        operation_state_seq: n(10),
        lane_state: serde_json::from_value(roots["lane_state"]["value"].clone()).unwrap(),
        lane_state_seq: n(11),
        leaf_id: Some(EntryId::new("leaf").unwrap()),
        configuration: serde_json::from_value(roots["configuration"]["value"].clone()).unwrap(),
        configuration_seq: n(12),
    };
    let mut loaded = BTreeMap::new();
    for e in case["entries"].as_array().unwrap() {
        loaded.insert(
            e["id"].as_str().unwrap().into(),
            PlannerLoadedValue::Entry(serde_json::from_value(e.clone()).unwrap()),
        );
    }
    for r in case["registers"].as_array().unwrap() {
        loaded.insert(
            r["key"].as_str().unwrap().into(),
            PlannerLoadedValue::Register(serde_json::from_value(r.clone()).unwrap()),
        );
    }
    let inputs = PlannerInputs {
        running: BTreeMap::new(),
        deferred_poll: DeferredPollPermit::None,
        deferred_cancellations: BTreeSet::new(),
        loaded,
        runtime: RuntimeSnapshot {
            settings_revision: n(7),
            stream_options: Default::default(),
            retry_policy: NormalizedRetryPolicy {
                max_attempts: OneBasedSafeUnsigned::new(3).unwrap(),
                base_delay_ms: n(10),
            },
        },
        context: None,
        now: n(50),
    };
    (
        current,
        inputs,
        CommitFacts {
            session_id: SessionId::new("session").unwrap(),
            leaf_seq: Some(n(8)),
            meta_seq: n(9),
        },
    )
}
pub fn model_event(current: &CurrentOperation) -> BeforeRequestEvent {
    let OperationState::Run(r) = &current.state else {
        panic!()
    };
    let RunPhase::Assistant(AssistantPhase {
        generation: Generation::Ready {
            context,
            next_attempt,
        },
        ..
    }) = &r.phase
    else {
        panic!()
    };
    serde_json::from_value(json!({"lane":current.operation.lane,"runId":current.operation.operation_id,"model":{"id":context.configuration.model.model_id,"name":"test","api":"test-api","provider":context.configuration.model.provider,"baseUrl":"https://example.invalid","reasoning":false,"input":["text"],"cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0},"contextWindow":1000,"maxTokens":100},"step":"assistant","attempt":next_attempt,"streamOptions":context.stream_options})).unwrap()
}
pub fn apply<C>(decision: RecipeDecision<C>) -> CommitRecipe<C> {
    match decision {
        RecipeDecision::Apply(r) => *r,
        _ => panic!("expected recipe"),
    }
}
pub fn pending_plan(
    current: &CurrentOperation,
    inputs: &PlannerInputs<&'static str>,
    facts: &CommitFacts,
) -> AssistantEffectPlan<&'static str> {
    let OperationState::Run(r) = &current.state else {
        panic!()
    };
    let RunPhase::Assistant(p) = &r.phase else {
        panic!()
    };
    let pending = PendingGeneration::try_from(p.generation.clone()).unwrap();
    let key = EffectKey {
        session_id: facts.session_id.clone(),
        lane: current.operation.lane.clone(),
        operation_id: current.operation.operation_id.clone(),
        kind: EffectKind::Assistant,
        step_or_task_id: EffectStepId::new(pending.context.step_id.as_str()).unwrap(),
        logical_attempt: n(pending.attempt.get()),
        item_index: None,
    };
    let _ = inputs;
    AssistantEffectPlan {
        key,
        generation: pending.clone(),
        stream_options: pending.context.stream_options,
        telemetry_context: "explicit-C",
    }
}
pub fn response(stop: &str) -> SettledAssistantMessage {
    serde_json::from_value(json!({"role":"assistant","content":[],"api":"explicit-synthetic-api","provider":"captured-provider","model":"captured-model","usage":{"input":0,"output":100,"cacheRead":0,"cacheWrite":0,"totalTokens":100,"cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"total":0}},"stopReason":stop,"errorMessage":if stop=="error"||stop=="aborted"{Some("failure")}else{None},"timestamp":50})).unwrap_or_else(|_|{let mut v=json!({"role":"assistant","content":[],"api":"explicit-synthetic-api","provider":"captured-provider","model":"captured-model","usage":{"input":0,"output":100,"cacheRead":0,"cacheWrite":0,"totalTokens":100,"cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"total":0}},"stopReason":stop,"timestamp":50});if stop=="error"||stop=="aborted"{v["errorMessage"]=json!("failure")}serde_json::from_value(v).unwrap()})
}
pub fn tool_result() -> AgentToolResult {
    serde_json::from_value(json!({"content":[],"details":null})).unwrap()
}
pub fn publish(
    current: &mut CurrentOperation,
    inputs: &mut PlannerInputs<&'static str>,
    recipe: &CommitRecipe<&'static str>,
) {
    for write in &recipe.transaction.writes {
        match write {
            Write::Entry(entry) => {
                let mut value = serde_json::to_value(entry).unwrap();
                value["seq"] = json!(100);
                value["timestamp"] = json!(50);
                let id = value["id"].as_str().unwrap().to_owned();
                inputs.loaded.insert(
                    id,
                    PlannerLoadedValue::Entry(serde_json::from_value(value).unwrap()),
                );
            }
            Write::SetRegister(register) => match register.as_ref() {
                RegisterSet::LaneLeaf { value, .. } => current.leaf_id = value.clone(),
                RegisterSet::OperationState { value, .. } => {
                    current.state = *value.clone();
                    current.operation_state_seq = n(current.operation_state_seq.get() + 1)
                }
                RegisterSet::OperationToolArguments { key, .. } => {
                    let mut v = serde_json::to_value(register).unwrap();
                    v["seq"] = json!(100);
                    inputs.loaded.insert(
                        key.clone(),
                        PlannerLoadedValue::Register(serde_json::from_value(v).unwrap()),
                    );
                }
                _ => {}
            },
            Write::DeleteRegister(r) => {
                inputs.loaded.remove(&r.key);
            }
            Write::Usage(_) => {}
        }
    }
}
pub fn tool_planning(facts: &CommitFacts, sequential: bool) -> PlanningFacts<&'static str> {
    PlanningFacts::Tools {
        session_id: facts.session_id.clone(),
        telemetry_context: "explicit-C",
        declarations: [1, 3, 5]
            .into_iter()
            .map(|i| {
                (
                    n(i),
                    ToolDeclaration {
                        replay: ToolReplay::Safe,
                        execution_mode: if sequential && i == 5 {
                            ToolExecutionMode::Sequential
                        } else {
                            ToolExecutionMode::Parallel
                        },
                    },
                )
            })
            .collect(),
        interrupted: tool_result(),
    }
}
pub fn tool_extra(planning: &PlanningFacts<&'static str>) -> SettlementFacts {
    let PlanningFacts::Tools { declarations, .. } = planning else {
        panic!()
    };
    SettlementFacts::Tool {
        usage_id: None,
        declarations: declarations.clone(),
    }
}
pub fn clear_one(
    current: &CurrentOperation,
    inputs: &PlannerInputs<&'static str>,
    facts: &CommitFacts,
    planning: &PlanningFacts<&'static str>,
) -> CommitRecipe<&'static str> {
    let ProcedureAction::Existing(Action::Dispatch(d)) =
        next_action(current, inputs, planning).unwrap()
    else {
        panic!()
    };
    let EffectPlan::ToolClearance(clear) = d.effect else {
        panic!()
    };
    let receipt = ToolClearanceOutput::new(
        &clear,
        ToolClearanceOutcome::Prepared {
            effective_args: JsonObject::default(),
            replay: ToolReplay::Safe,
        },
    )
    .unwrap();
    apply(
        settlement_recipe(
            current,
            &SettlementCommit {
                current: current.clone(),
                plan: EffectPlan::ToolClearance(clear),
                output: SettlementOutput::ToolClearance(receipt),
                telemetry_context: "explicit-C",
            },
            inputs,
            facts,
            &tool_extra(planning),
        )
        .unwrap(),
    )
}
