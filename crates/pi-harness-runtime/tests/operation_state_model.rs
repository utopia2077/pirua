//! Bounded generated schedules over accepted pure recipes and actual MemorySession.
//! This target does not exercise the public Harness runtime or external callbacks.
mod operation_state_model_support;
use operation_state_model_support::*;
use pi_agent_schema::session_values::*;
use pi_agent_schema::*;
use pi_harness_core::{planner::*, transition::*};
use std::{
    collections::{BTreeMap, BTreeSet},
    panic::{AssertUnwindSafe, catch_unwind},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Attempt {
    Stop,
    Retry,
    Lost,
    ErrorAtCap,
}
fn generations(prefix: Vec<Attempt>, output: &mut Vec<Vec<Attempt>>) {
    let depth = prefix.len() + 1;
    let mut stop = prefix.clone();
    stop.push(Attempt::Stop);
    output.push(stop);
    if depth == 3 {
        let mut error = prefix;
        error.push(Attempt::ErrorAtCap);
        output.push(error)
    } else {
        for action in [Attempt::Retry, Attempt::Lost] {
            let mut next = prefix.clone();
            next.push(action);
            generations(next, output)
        }
    }
}
fn selected(family: &str, seed: usize) -> bool {
    let Ok(value) = std::env::var("PI_OPERATION_MODEL_CASE") else {
        return true;
    };
    let (selected_family, selected_seed) = value
        .split_once(':')
        .expect("replay must be generation:SEED or tools:SEED");
    let selected_seed: usize = selected_seed
        .parse()
        .expect("replay seed must be an integer");
    let bound = match selected_family {
        "generation" => 154,
        "tools" => 97,
        _ => panic!("unknown model replay family"),
    };
    assert!(
        selected_seed < bound,
        "replay seed outside the explored bound"
    );
    family == selected_family && seed == selected_seed
}

fn history(family: &str, seed: usize, actions: impl std::fmt::Debug, run: impl FnOnce(&mut Model)) {
    let mut model = catch_unwind(AssertUnwindSafe(Model::new)).unwrap_or_else(|_| {
        panic!("replay PI_OPERATION_MODEL_CASE={family}:{seed}; actions={actions:?}; failed during configured Session/acceptance setup")
    });
    let result = catch_unwind(AssertUnwindSafe(|| run(&mut model)));
    if let Err(error) = result {
        let detail = error
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| error.downcast_ref::<&str>().copied())
            .unwrap_or("non-string panic");
        panic!(
            "replay PI_OPERATION_MODEL_CASE={family}:{seed}; actions={actions:?}; prefix={:?}; failure={detail}",
            model.trace
        )
    }
}
fn generation_history(model: &mut Model, actions: &[Attempt], axis: usize, race: bool) {
    model.begin_step("step", race);
    for (position, action) in actions.iter().enumerate() {
        let plan = model.intent("generation");
        assert_eq!(plan.generation.attempt.get(), (position + 1) as u64);
        let message = assistant(
            if matches!(action, Attempt::Retry | Attempt::ErrorAtCap) {
                "error"
            } else {
                "stop"
            },
            false,
        );
        let commit = model.assistant_commit(&plan, message);
        let extra = SettlementFacts::Assistant {
            retryable: true,
            overflow: false,
            followers: vec![],
        };
        let mut wrong = commit.clone();
        let EffectPlan::Assistant(p) = &mut wrong.plan else {
            panic!()
        };
        wrong_key(&mut p.key, axis);
        let SettlementOutput::Assistant { key, .. } = &mut wrong.output else {
            panic!()
        };
        *key = p.key.clone();
        model.reject_output(&wrong, &extra, "wrong assistant key");
        if *action == Attempt::Lost {
            let (c, i, f) = model.current();
            let planning = PlanningFacts::Generation {
                session_id: f.session_id.clone(),
                telemetry_context: (),
                synthetic: None,
            };
            let ProcedureAction::Existing(Action::Transition(t)) =
                next_action(&c, &i, &planning).unwrap()
            else {
                panic!("unknown below cap must advance")
            };
            let tx = TransitionCommit {
                current: c.clone(),
                next: t.next,
                telemetry_context: (),
                expected_configuration_seq: t.expected_configuration_seq,
                expected_settings_revision: t.expected_settings_revision,
            };
            let recipe = apply(transition_recipe(&c, &tx, &i, &f, &planning).unwrap());
            model.publish(recipe, "lost binding -> later attempt, no fabricated usage");
            model.reject_output(&commit, &extra, "late output from lost attempt");
            continue;
        }
        model
            .running
            .insert(plan.key.clone(), EffectPlan::Assistant(plan.clone()));
        let (c, i, f) = model.current();
        assert!(
            matches!(next_action(&c,&i,&PlanningFacts::Generation{session_id:f.session_id.clone(),telemetry_context:(),synthetic:None}).unwrap(),ProcedureAction::Existing(Action::AwaitEffect(key)) if key==plan.key)
        );
        let recipe = apply(settlement_recipe(&c, &commit, &i, &f, &extra).unwrap());
        model.record_response(&plan);
        model.publish(recipe, "assistant response + usage + decision");
        model.running.remove(&plan.key);
        model.reject_output(&commit, &extra, "duplicate settled assistant");
        if *action == Attempt::Retry {
            let (c, i, f) = model.current();
            let planning = PlanningFacts::Generation {
                session_id: f.session_id.clone(),
                telemetry_context: (),
                synthetic: None,
            };
            let ProcedureAction::Existing(Action::Wait(wait)) =
                next_action(&c, &i, &planning).unwrap()
            else {
                panic!()
            };
            let before = model.snapshot();
            model.trace.push(format!("wait until {}", wait.until.get()));
            model.now = wait.until;
            assert_eq!(model.snapshot(), before);
            let (c, i, f) = model.current();
            let ProcedureAction::Existing(Action::Transition(t)) =
                next_action(&c, &i, &planning).unwrap()
            else {
                panic!()
            };
            let tx = TransitionCommit {
                current: c.clone(),
                next: t.next,
                telemetry_context: (),
                expected_configuration_seq: t.expected_configuration_seq,
                expected_settings_revision: t.expected_settings_revision,
            };
            model.publish(
                apply(transition_recipe(&c, &tx, &i, &f, &planning).unwrap()),
                "elapsed captured retry -> ready",
            );
        }
    }
    assert_eq!(
        model.reference.attempts,
        (1..=actions.len() as u64).collect::<Vec<_>>()
    );
    model.finish(if actions.last() == Some(&Attempt::ErrorAtCap) {
        End::Failed
    } else {
        End::Assistant
    });
}

#[test]
fn generated_generation_histories_preserve_authority_and_atomic_pairing() {
    let mut outcomes = vec![];
    generations(vec![], &mut outcomes);
    assert_eq!(outcomes.len(), 11);
    let mut executed = 0;
    for (path, actions) in outcomes.iter().enumerate() {
        for axis in 0..7 {
            for race in [false, true] {
                let seed = path * 14 + axis * 2 + usize::from(race);
                if selected("generation", seed) {
                    history("generation", seed, actions, |m| {
                        generation_history(m, actions, axis, race)
                    });
                    executed += 1
                }
            }
        }
    }
    if std::env::var("PI_OPERATION_MODEL_CASE").is_err() {
        assert_eq!(executed, 154)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Event {
    Clear(usize),
    Ready(usize),
    Settle(usize),
}
fn schedules(
    immediate_last: bool,
    cleared: usize,
    ready_mask: u8,
    settled: usize,
    prefix: &mut Vec<Event>,
    output: &mut Vec<Vec<Event>>,
) {
    if settled == 3 {
        output.push(prefix.clone());
        return;
    }
    // Clearance itself stays source ordered. Immediate clearance produces a local
    // receipt; normal clearance publishes intent, then raw readiness can interleave.
    if cleared < 3 {
        prefix.push(Event::Clear(cleared));
        let mask = if immediate_last && cleared == 2 {
            ready_mask | (1 << cleared)
        } else {
            ready_mask
        };
        schedules(immediate_last, cleared + 1, mask, settled, prefix, output);
        prefix.pop();
    }
    for index in settled..cleared {
        if ready_mask & (1 << index) == 0 {
            prefix.push(Event::Ready(index));
            schedules(
                immediate_last,
                cleared,
                ready_mask | (1 << index),
                settled,
                prefix,
                output,
            );
            prefix.pop();
        }
    }
    if settled < cleared && ready_mask & (1 << settled) != 0 {
        prefix.push(Event::Settle(settled));
        schedules(
            immediate_last,
            cleared,
            ready_mask,
            settled + 1,
            prefix,
            output,
        );
        prefix.pop();
    }
}
fn tool_facts() -> PlanningFacts<()> {
    PlanningFacts::Tools {
        session_id: SessionId::new("model-session").unwrap(),
        telemetry_context: (),
        declarations: [1, 3, 5]
            .into_iter()
            .map(|i| {
                (
                    n(i),
                    ToolDeclaration {
                        replay: ToolReplay::Safe,
                        execution_mode: ToolExecutionMode::Parallel,
                    },
                )
            })
            .collect(),
        interrupted: tool_result(false),
    }
}
fn tool_extra(usage: Option<UsageId>) -> SettlementFacts {
    let PlanningFacts::Tools { declarations, .. } = tool_facts() else {
        panic!()
    };
    SettlementFacts::Tool {
        usage_id: usage,
        declarations,
    }
}
fn start_tools(model: &mut Model) {
    model.begin_step("tool-step", false);
    let plan = model.intent("tools");
    let commit = model.assistant_commit(&plan, assistant("toolUse", true));
    let (c, i, f) = model.current();
    let extra = SettlementFacts::Assistant {
        retryable: false,
        overflow: false,
        followers: [1, 3, 5]
            .into_iter()
            .enumerate()
            .map(|(index, source)| (n(source), EntryId::new(format!("result-{index}")).unwrap()))
            .collect(),
    };
    let recipe = apply(settlement_recipe(&c, &commit, &i, &f, &extra).unwrap());
    model.record_response(&plan);
    model.publish(recipe, "complete sparse tool follower plan");
}
fn tool_history(model: &mut Model, events: &[Event], immediate_last: bool, seed: usize) {
    start_tools(model);
    let planning = tool_facts();
    let mut plans: BTreeMap<usize, EffectPlan<()>> = BTreeMap::new();
    let mut immediate = None;
    let mut raw_ready = BTreeSet::new();
    let mut settled = 0;
    for event in events {
        model.trace.push(format!("{event:?}"));
        match *event {
            Event::Clear(index) => {
                let (c, i, f) = model.current();
                let ProcedureAction::Existing(Action::Dispatch(d)) =
                    next_action(&c, &i, &planning).unwrap()
                else {
                    panic!("enabled source clearance must dispatch")
                };
                let EffectPlan::ToolClearance(p) = d.effect else {
                    panic!()
                };
                assert_eq!(p.source_index.get(), (index * 2 + 1) as u64);
                let args = JsonObject::new(BTreeMap::from([(
                    "preparedSlot".into(),
                    JsonValue::String(index.to_string()),
                )]));
                let outcome = if immediate_last && index == 2 {
                    ToolClearanceOutcome::Immediate {
                        result: tool_result(false),
                        is_error: true,
                        terminate: seed.is_multiple_of(2),
                    }
                } else {
                    ToolClearanceOutcome::Prepared {
                        effective_args: args.clone(),
                        replay: ToolReplay::Safe,
                    }
                };
                let output = ToolClearanceOutput::new(&p, outcome).unwrap();
                let plan = EffectPlan::ToolClearance(p);
                let commit = SettlementCommit {
                    current: c.clone(),
                    plan: plan.clone(),
                    output: SettlementOutput::ToolClearance(output),
                    telemetry_context: (),
                };
                if immediate_last && index == 2 {
                    immediate = Some(commit);
                    plans.insert(index, plan.clone());
                    model.running.insert(plan.key().clone(), plan);
                    raw_ready.insert(index);
                } else {
                    let recipe =
                        apply(settlement_recipe(&c, &commit, &i, &f, &tool_extra(None)).unwrap());
                    let dispatch = recipe.dispatch.clone().unwrap();
                    let key = format!("operation:tool-step:{}", index * 2 + 1);
                    model.reference.args.insert(key, args);
                    model.publish(recipe, "saved complete args + pending intent");
                    model
                        .running
                        .insert(dispatch.key().clone(), dispatch.clone());
                    plans.insert(index, dispatch);
                }
            }
            Event::Ready(index) => {
                let before = model.snapshot();
                assert!(plans.contains_key(&index));
                if index == settled && plans.len() == 3 && seed.is_multiple_of(3) {
                    let plan = plans.get(&index).unwrap();
                    model.running.remove(plan.key());
                    let (current, inputs, _) = model.current();
                    let ProcedureAction::Existing(Action::Dispatch(replay)) =
                        next_action(&current, &inputs, &planning).unwrap()
                    else {
                        panic!("safe unknown tool must replay saved intent");
                    };
                    assert_eq!(replay.effect.key(), plan.key());
                    assert!(
                        matches!(replay.effect, EffectPlan::Tool(_)),
                        "saved replay must not clear arguments again"
                    );
                    assert_eq!(
                        model.snapshot(),
                        before,
                        "replay planning must not rewrite saved arguments"
                    );
                    model
                        .trace
                        .push(format!("lose/rebind safe tool {index} using saved args"));
                    model.running.insert(plan.key().clone(), plan.clone());
                }
                assert!(raw_ready.insert(index));
                assert_eq!(model.snapshot(), before, "raw readiness is process-local");
                if index > settled {
                    let p = plans.get(&index).unwrap();
                    let (c, _, _) = model.current();
                    let wrong_order = SettlementCommit {
                        current: c,
                        plan: p.clone(),
                        output: SettlementOutput::Tool(ToolSettlementOutput {
                            key: p.key().clone(),
                            result: tool_result(false),
                            is_error: false,
                            terminate: false,
                        }),
                        telemetry_context: (),
                    };
                    model.reject_output(
                        &wrong_order,
                        &tool_extra(None),
                        "later raw ready cannot settle ahead of source",
                    );
                }
            }
            Event::Settle(index) => {
                assert_eq!(index, settled);
                assert!(raw_ready.remove(&index));
                let plan = plans.get(&index).unwrap();
                let usage = !(immediate_last && index == 2) && (seed + index).is_multiple_of(2);
                let usage_id = usage.then(|| UsageId::new(format!("tool-usage-{index}")).unwrap());
                let extra = tool_extra(usage_id.clone());
                let (c, i, f) = model.current();
                let commit = if immediate_last && index == 2 {
                    immediate.take().unwrap()
                } else {
                    SettlementCommit {
                        current: c.clone(),
                        plan: plan.clone(),
                        output: SettlementOutput::Tool(ToolSettlementOutput {
                            key: plan.key().clone(),
                            result: tool_result(usage),
                            is_error: false,
                            terminate: seed.is_multiple_of(2),
                        }),
                        telemetry_context: (),
                    }
                };
                if let EffectPlan::Tool(p) = &commit.plan {
                    let mut wrong = commit.clone();
                    let EffectPlan::Tool(changed) = &mut wrong.plan else {
                        panic!()
                    };
                    wrong_key(&mut changed.key, (seed + index) % 7);
                    let SettlementOutput::Tool(out) = &mut wrong.output else {
                        panic!()
                    };
                    out.key = changed.key.clone();
                    model.reject_output(&wrong, &extra, "wrong tool key");
                    assert_eq!(p.source_index.get(), (index * 2 + 1) as u64);
                }
                let mut recipe = apply(settlement_recipe(&c, &commit, &i, &f, &extra).unwrap());
                if immediate_last && index == 2 {
                    let key = "operation:tool-step:5".to_owned();
                    let value = JsonObject::default();
                    model.reference.args.insert(key.clone(), value.clone());
                    ready(model.session.commit(Transaction {
                        writes: vec![set(RegisterSet::OperationToolArguments { key, value })],
                    }))
                    .unwrap();
                    model
                        .trace
                        .push("external insertion into observed-absent cleanup key".into());
                    model.check();
                    model.reject_recipe(recipe, "cleanup CAS loses to missing-key insertion");
                    let (c, i, f) = model.current();
                    recipe = apply(settlement_recipe(&c, &commit, &i, &f, &extra).unwrap());
                }
                model.reference.entries.push(format!("result-{index}"));
                if let Some(id) = usage_id {
                    model
                        .reference
                        .usage
                        .insert(id.as_str().into(), format!("result-{index}"));
                }
                model.reference.tool_order.push(index);
                if index == 2 {
                    model.reference.args.clear()
                }
                model.publish(recipe, "ordered tool result + usage + decision");
                model.running.remove(plan.key());
                model.reject_output(&commit, &extra, "duplicate tool receipt");
                settled += 1;
            }
        }
    }
    assert_eq!(settled, 3);
    let (c, _, _) = model.current();
    let OperationState::Run(run) = c.state else {
        panic!()
    };
    let RunPhase::Checkpoint(p) = run.phase else {
        panic!()
    };
    assert_eq!(
        matches!(
            p.continuation,
            Continuation::MayFinish {
                include_final_assistant: false
            }
        ),
        seed.is_multiple_of(2),
        "nonempty all-terminate oracle"
    );
    if !seed.is_multiple_of(2) {
        model.begin_step("final-step", false);
        let plan = model.intent("final");
        let commit = model.assistant_commit(&plan, assistant("stop", false));
        let (c, i, f) = model.current();
        let recipe = apply(
            settlement_recipe(
                &c,
                &commit,
                &i,
                &f,
                &SettlementFacts::Assistant {
                    retryable: false,
                    overflow: false,
                    followers: vec![],
                },
            )
            .unwrap(),
        );
        model.record_response(&plan);
        model.publish(recipe, "final assistant after nonterminating tools");
    }
    model.finish(if seed.is_multiple_of(2) {
        End::Tools
    } else {
        End::Assistant
    });
}
#[test]
fn generated_tool_schedules_preserve_source_order_args_and_cleanup() {
    let mut seed = 0;
    let mut total = 0;
    for immediate_last in [false, true] {
        let mut paths = vec![];
        schedules(immediate_last, 0, 0, 0, &mut vec![], &mut paths);
        assert_eq!(paths.len(), if immediate_last { 26 } else { 71 });
        for events in paths {
            if selected("tools", seed) {
                history("tools", seed, (&events, immediate_last), |m| {
                    tool_history(m, &events, immediate_last, seed)
                });
                total += 1
            }
            seed += 1;
        }
    }
    if std::env::var("PI_OPERATION_MODEL_CASE").is_err() {
        assert_eq!(total, seed);
        assert_eq!(total, 97);
        println!("tool history count={total}")
    }
}
