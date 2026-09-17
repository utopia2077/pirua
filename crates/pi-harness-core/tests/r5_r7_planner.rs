#[allow(dead_code)]
mod r1_r4_support;
mod r5_r7_support;
use pi_agent_schema::session_values::*;
use pi_agent_schema::*;
use pi_harness_core::{inbox::*, planner::*};
use r1_r4_support::{fixture, n, response};
use r5_r7_support::*;

#[test]
fn selection_modes_skip_and_order_survive_state_roundtrip() {
    for mode in [QueueMode::All, QueueMode::OneAtATime] {
        let (mut c, i, f) = fixture("run.checkpoint");
        let r = run_mut(&mut c);
        r.inbox.writes = vec![id("write")];
        r.inbox.steer = vec![id("a"), id("b")];
        r.inbox.follow_up = vec![id("follow")];
        r.settings.steering_mode = mode;
        assert_eq!(select(&c).unwrap().unwrap().kind, InboxKind::Writes);
        let recipe = transition(&c, &i, &f, &inbox(&c, false, None));
        c.state = recipe.next.unwrap();
        c.leaf_id = Some(id("write"));
        let selection = select(&c).unwrap().unwrap();
        assert_eq!(selection.kind, InboxKind::Steer);
        assert_eq!(
            selection.ids.len(),
            if mode == QueueMode::All { 2 } else { 1 }
        );
        let recipe = transition(&c, &i, &f, &inbox(&c, true, Some(user())));
        assert_eq!(
            recipe
                .expected
                .iter()
                .filter(|x| x.namespace == RegisterNamespace::PendingEntry)
                .count(),
            selection.ids.len()
        );
        assert!(matches!(&recipe.transaction.writes[0], Write::Entry(_)));
        assert!(
            matches!(&recipe.transaction.writes[1],Write::DeleteRegister(d) if d.namespace==RegisterNamespace::PendingEntry)
        );
        c.state =
            serde_json::from_value(serde_json::to_value(recipe.next.unwrap()).unwrap()).unwrap();
        assert_eq!(
            select(&c).unwrap(),
            None,
            "reopen must generate before second drain"
        );
        let p = PlanningFacts::Checkpoint {
            session_id: f.session_id.clone(),
            telemetry_context: "test",
            step_id: StepId::new("fresh").unwrap(),
        };
        let next = transition(&c, &i, &f, &p);
        assert_eq!(
            next.expected_settings_revision,
            Some(i.runtime.settings_revision)
        );
        assert!(matches!(
            run_mut(&mut CurrentOperation {
                state: next.next.unwrap(),
                ..c.clone()
            })
            .phase,
            RunPhase::Assistant(_)
        ));
    }
}
#[test]
fn writes_filter_assistants_and_custom_projection_revives_only_context() {
    for reason in ["error", "aborted", "deferred", "stop", "length"] {
        let (mut c, i, f) = fixture("run.failure_drain.response");
        run_mut(&mut c).inbox.writes = vec![id("write")];
        let mut m = response(if reason == "deferred" { "stop" } else { reason }).into_message();
        if reason == "deferred" {
            m.stop_reason = StopReason::Deferred;
            m.deferred = Some(
                serde_json::from_value(
                    serde_json::json!({"id":"h","provider":"p","modelId":"m","api":"a"}),
                )
                .unwrap(),
            );
        }
        let message = AgentMessage::BuiltIn(Message::Assistant(Box::new(m)));
        let projects = message_projects(&message);
        assert_eq!(projects, matches!(reason, "stop" | "length"));
        let recipe = transition(&c, &i, &f, &inbox(&c, projects, Some(message)));
        c.state = recipe.next.unwrap();
        assert_eq!(
            matches!(run_mut(&mut c).phase, RunPhase::Checkpoint(_)),
            projects
        );
    }
    for projects in [false, true] {
        let (mut c, i, f) = fixture("run.failure_drain.response");
        run_mut(&mut c).inbox.writes = vec![id("custom")];
        let original = run_mut(&mut c).phase.clone();
        let recipe = transition(&c, &i, &f, &inbox(&c, projects, None));
        c.state = recipe.next.unwrap();
        if !projects {
            assert_eq!(run_mut(&mut c).phase, original);
        } else {
            assert!(matches!(run_mut(&mut c).phase, RunPhase::Checkpoint(_)));
        }
    }
}
#[test]
fn explicit_steer_is_not_message_write_filter_and_followup_waits_for_generation() {
    let (mut c, i, f) = fixture("run.checkpoint");
    run_mut(&mut c).inbox.follow_up = vec![id("follow")];
    if let RunPhase::Checkpoint(p) = &mut run_mut(&mut c).phase {
        p.continuation = Continuation::NeedAssistant {
            overflow_recovery_used: false,
        };
    }
    assert_eq!(select(&c).unwrap(), None);
    run_mut(&mut c).inbox.steer = vec![id("steer")];
    let message = AgentMessage::BuiltIn(Message::Assistant(Box::new(
        response("error").into_message(),
    )));
    let recipe = transition(&c, &i, &f, &inbox(&c, false, Some(message)));
    c.state = recipe.next.unwrap();
    assert!(
        matches!(&run_mut(&mut c).phase,RunPhase::Checkpoint(p) if p.skip_inbox_once==Some(true))
    );
}
#[test]
fn abort_is_idempotent_retains_payloads_and_cancelled_writes_preserve_every_safe_phase() {
    for name in [
        "run.checkpoint",
        "run.assistant.ready",
        "run.assistant.retry_wait",
        "run.failure_drain.response",
        "run.deferred.suspended",
    ] {
        let (mut c, i, f) = fixture(name);
        let r = run_mut(&mut c);
        r.inbox.steer = vec![id("steer")];
        r.inbox.follow_up = vec![id("follow")];
        r.inbox.writes = vec![id("write")];
        let payloads = place(&c, &[id("steer"), id("follow")], Some(user()));
        let old_phase = run_mut(&mut c).phase.clone();
        let a = pi_harness_core::control::abort_recipe(&c, &f, n(42), &payloads, &"test").unwrap();
        assert_eq!(a.result.steer, vec![user()]);
        let recipe = a.recipe.unwrap();
        assert_eq!(recipe.transaction.writes.len(), 1);
        c.state = recipe.next.unwrap();
        let repeat =
            pi_harness_core::control::abort_recipe(&c, &f, n(900), &payloads, &"test").unwrap();
        assert!(repeat.recipe.is_none());
        assert_eq!(repeat.result, a.result);
        let recipe = transition(&c, &i, &f, &inbox(&c, false, Some(user())));
        c.state = recipe.next.unwrap();
        assert_eq!(run_mut(&mut c).phase, old_phase);
        assert!(run_mut(&mut c).inbox.writes.is_empty());
    }
}
#[test]
fn one_poll_permit_is_consumed_only_by_successful_fresh_intent() {
    let (mut c, mut i, f) = fixture("run.deferred.suspended");
    assert!(matches!(
        next_action(&c, &i, &PlanningFacts::Terminal).unwrap(),
        ProcedureAction::Existing(Action::Suspend(_))
    ));
    i.deferred_poll = DeferredPollPermit::One;
    let recipe = poll(&c, &i, &f, "r1", "u1");
    assert!(pi_harness_core::deferred::consumes_poll(&recipe));
    let effect = recipe.dispatch.clone().unwrap();
    c.state = recipe.next.unwrap();
    i.running.insert(effect.key().clone(), effect.clone());
    assert!(matches!(
        next_action(&c, &i, &deferred_facts(&c)).unwrap(),
        ProcedureAction::Existing(Action::AwaitEffect(_))
    ));
    i.running.clear();
    let replacement = poll(&c, &i, &f, "r2", "u2");
    let EffectPlan::Deferred(old) = effect else {
        panic!()
    };
    let EffectPlan::Deferred(new) = replacement.dispatch.unwrap() else {
        panic!()
    };
    assert_eq!(old.deferred.poll, new.deferred.poll);
    assert_ne!(
        old.deferred.response_entry_id,
        new.deferred.response_entry_id
    );
    assert_ne!(old.deferred.usage_id, new.deferred.usage_id);
    i.deferred_poll = DeferredPollPermit::None;
    assert!(matches!(
        next_action(&c, &i, &deferred_facts(&c)).unwrap(),
        ProcedureAction::Existing(Action::Suspend(_))
    ));
}
#[test]
fn deferred_complete_handle_equality_and_ready_error_usage_are_atomic() {
    for reason in ["pending", "mismatch", "stop", "error", "aborted"] {
        let (mut c, mut i, f) = fixture("run.deferred.suspended");
        i.deferred_poll = DeferredPollPermit::One;
        let recipe = poll(&c, &i, &f, "fresh-r", "fresh-u");
        c.state = recipe.next.unwrap();
        let effect = recipe.dispatch.unwrap();
        let EffectPlan::Deferred(p) = &effect else {
            panic!()
        };
        let source_id = p.deferred.source_entry_id.clone();
        let PlannerLoadedValue::Entry(Entry::Message(source)) = &i.loaded[source_id.as_str()]
        else {
            panic!()
        };
        let AgentMessage::BuiltIn(Message::Assistant(source)) = &source.message else {
            panic!()
        };
        let mut m = response(if matches!(reason, "pending" | "mismatch") {
            "stop"
        } else {
            reason
        })
        .into_message();
        if matches!(reason, "pending" | "mismatch") {
            m.stop_reason = StopReason::Deferred;
            m.deferred = source.deferred.clone();
            if reason == "mismatch" {
                let mut j = serde_json::to_value(m.deferred.as_ref().unwrap()).unwrap();
                j["id"] = serde_json::json!("different");
                m.deferred = Some(serde_json::from_value(j).unwrap());
            }
        }
        let commit = SettlementCommit {
            current: c.clone(),
            plan: effect.clone(),
            output: SettlementOutput::Deferred {
                key: effect.key().clone(),
                message: SettledAssistantMessage::new(m).unwrap(),
            },
            telemetry_context: "test",
        };
        let decision = settlement_recipe(
            &c,
            &commit,
            &i,
            &f,
            &SettlementFacts::Deferred {
                overflow: false,
                followers: vec![],
            },
        );
        if reason == "aborted" {
            assert_eq!(decision.unwrap_err(), PlanError::InvalidCurrent);
            continue;
        }
        let RecipeDecision::Apply(recipe) = decision.unwrap() else {
            panic!()
        };
        assert_eq!(recipe.transaction.writes.len(), 4);
        assert!(
            matches!(&recipe.transaction.writes[2],Write::Usage(u) if u.id.as_str()=="fresh-u" && u.usage.output==n(100))
        );
        c.state = recipe.next.clone().unwrap();
        i.deferred_poll = DeferredPollPermit::None;
        assert!(matches!(&run_mut(&mut c).phase, RunPhase::Deferred(_)) == (reason == "pending"));
        assert!(
            matches!(&run_mut(&mut c).phase, RunPhase::FailureDrain(_))
                == matches!(reason, "error" | "mismatch")
        );
    }
}
#[test]
fn intent_rejects_spent_permit_stale_source_and_reused_unknown_reservations() {
    let (mut c, mut i, f) = fixture("run.deferred.suspended");
    i.deferred_poll = DeferredPollPermit::One;
    let first = poll(&c, &i, &f, "r1", "u1");
    c.state = first.next.unwrap();
    let ProcedureAction::Existing(Action::Dispatch(d)) =
        next_action(&c, &i, &deferred_facts(&c)).unwrap()
    else {
        panic!()
    };
    let EffectPlan::Hook(h) = &d.effect else {
        panic!()
    };
    let output = RequestOptionsOutput::new(h, Default::default()).unwrap();
    let commit = SettlementCommit {
        current: c.clone(),
        plan: d.effect,
        output: SettlementOutput::RequestOptions(output),
        telemetry_context: "test",
    };
    let extra = SettlementFacts::Request {
        response_entry_id: id("r2"),
        usage_id: UsageId::new("u2").unwrap(),
    };
    i.deferred_poll = DeferredPollPermit::None;
    assert!(matches!(
        settlement_recipe(&c, &commit, &i, &f, &extra).unwrap(),
        RecipeDecision::NoLongerExpected
    ));
    i.deferred_poll = DeferredPollPermit::One;
    assert_eq!(
        settlement_recipe(
            &c,
            &commit,
            &i,
            &f,
            &SettlementFacts::Request {
                response_entry_id: id("r1"),
                usage_id: UsageId::new("u2").unwrap()
            }
        )
        .unwrap_err(),
        PlanError::InvalidInput
    );
    let mut stale = c.clone();
    let RunPhase::Deferred(p) = &mut run_mut(&mut stale).phase else {
        panic!()
    };
    let Deferred::EffectPending {
        source_entry_id, ..
    } = &mut p.deferred
    else {
        panic!()
    };
    *source_entry_id = id("new-source");
    assert!(matches!(
        settlement_recipe(&stale, &commit, &i, &f, &extra).unwrap(),
        RecipeDecision::NoLongerExpected
    ));
}
#[test]
fn cancelled_unknown_uses_original_ids_zero_usage_but_live_result_keeps_usage() {
    for live in [false, true] {
        let (mut c, mut i, f) = fixture("run.deferred.suspended");
        i.deferred_poll = DeferredPollPermit::One;
        let recipe = poll(&c, &i, &f, "reserved-r", "reserved-u");
        c.state = recipe.next.unwrap();
        let effect = recipe.dispatch.unwrap();
        let abort = pi_harness_core::control::abort_recipe(&c, &f, n(10), &[], &"test").unwrap();
        c.state = abort.recipe.unwrap().next.unwrap();
        let mut synthetic = response("aborted").into_message();
        synthetic.usage = Usage::zero();
        let EffectPlan::Deferred(p) = &effect else {
            panic!()
        };
        synthetic.provider = p.deferred.configuration.model.provider.clone();
        synthetic.model = p.deferred.configuration.model.model_id.clone();
        let facts = PlanningFacts::Deferred {
            session_id: f.session_id.clone(),
            telemetry_context: "test",
            request: None,
            synthetic: Some(Box::new(SettledAssistantMessage::new(synthetic).unwrap())),
        };
        let commit = if live {
            i.running.insert(effect.key().clone(), effect.clone());
            assert!(matches!(
                next_action(&c, &i, &facts).unwrap(),
                ProcedureAction::Existing(Action::AwaitEffect(_))
            ));
            SettlementCommit {
                current: c.clone(),
                plan: effect.clone(),
                output: SettlementOutput::Deferred {
                    key: effect.key().clone(),
                    message: response("stop"),
                },
                telemetry_context: "test",
            }
        } else {
            let ProcedureAction::Reconcile(commit) = next_action(&c, &i, &facts).unwrap() else {
                panic!()
            };
            *commit
        };
        let RecipeDecision::Apply(recipe) = settlement_recipe(
            &c,
            &commit,
            &i,
            &f,
            &SettlementFacts::Deferred {
                overflow: false,
                followers: vec![],
            },
        )
        .unwrap() else {
            panic!()
        };
        assert!(
            matches!(&recipe.transaction.writes[0],Write::Entry(NewEntry::Message(e)) if e.id.as_str()=="reserved-r" && matches!(&e.message,AgentMessage::BuiltIn(Message::Assistant(m)) if m.stop_reason==StopReason::Aborted))
        );
        assert!(
            matches!(&recipe.transaction.writes[2],Write::Usage(u) if u.id.as_str()=="reserved-u" && u.usage.output==n(if live{100}else{0}))
        );
    }
}
#[test]
fn deferred_ready_tools_have_original_indexes_followers_and_poll_turn() {
    let (mut c, mut i, f) = fixture("run.deferred.suspended");
    i.deferred_poll = DeferredPollPermit::One;
    let recipe = poll(&c, &i, &f, "r", "u");
    c.state = recipe.next.unwrap();
    let effect = recipe.dispatch.unwrap();
    let mut m = serde_json::to_value(response("toolUse")).unwrap();
    m["content"] = serde_json::json!([{"type":"text","text":"prefix"},{"type":"toolCall","id":"call1","name":"tool","arguments":{}},{"type":"text","text":"gap"},{"type":"toolCall","id":"call2","name":"tool","arguments":{}}]);
    let commit = SettlementCommit {
        current: c.clone(),
        plan: effect.clone(),
        output: SettlementOutput::Deferred {
            key: effect.key().clone(),
            message: serde_json::from_value(m).unwrap(),
        },
        telemetry_context: "test",
    };
    let extra = SettlementFacts::Deferred {
        overflow: false,
        followers: vec![(n(1), id("t1")), (n(3), id("t2"))],
    };
    let RecipeDecision::Apply(recipe) = settlement_recipe(&c, &commit, &i, &f, &extra).unwrap()
    else {
        panic!()
    };
    c.state = recipe.next.unwrap();
    let RunPhase::Tools(p) = &run_mut(&mut c).phase else {
        panic!()
    };
    assert_eq!(p.batch.calls.len(), 2);
    assert!(p.batch.turn_id.as_str().ends_with(":poll:1"));
    assert_eq!(
        pi_harness_core::tool_batch::source(&p.batch.calls[1]),
        (n(3), &id("t2"))
    );
}
#[test]
fn planned_cancelled_tools_use_aborted_started_unknown_use_interrupted_live_waits() {
    for name in ["run.tools.planned", "run.tools.effect_pending"] {
        let (mut c, mut i, f) = fixture(name);
        let a = pi_harness_core::control::abort_recipe(&c, &f, n(9), &[], &"test").unwrap();
        c.state = a.recipe.unwrap().next.unwrap();
        let mut interrupted = r1_r4_support::tool_result();
        interrupted.details = JsonValue::String("interrupted".into());
        let mut aborted = interrupted.clone();
        aborted.details = JsonValue::String("aborted".into());
        let planning = PlanningFacts::ToolsCancelled {
            session_id: f.session_id.clone(),
            telemetry_context: "test",
            declarations: Default::default(),
            interrupted,
            aborted,
        };
        let ProcedureAction::Reconcile(commit) = next_action(&c, &i, &planning).unwrap() else {
            panic!()
        };
        let SettlementOutput::Reconciled(o) = &commit.output else {
            panic!()
        };
        let ReconciledValue::Tool { result, .. } = o.value() else {
            panic!()
        };
        assert_eq!(
            result.details,
            JsonValue::String(
                if name.ends_with("planned") {
                    "aborted"
                } else {
                    "interrupted"
                }
                .into()
            )
        );
        i.running
            .insert(commit.plan.key().clone(), commit.plan.clone());
        assert!(matches!(
            next_action(&c, &i, &planning).unwrap(),
            ProcedureAction::Existing(Action::AwaitEffect(_))
        ));
    }
}

#[test]
fn selection_is_bounded_to_checkpoint_failure_or_safe_cancelled_phase() {
    for name in [
        "run.checkpoint",
        "run.assistant.ready",
        "run.assistant.effect_pending",
        "run.assistant.retry_wait",
        "run.tools.planned",
        "run.tools.effect_pending",
        "run.deferred.suspended",
        "run.failure_drain.response",
    ] {
        for cancelled in [false, true] {
            let (mut c, _, f) = fixture(name);
            run_mut(&mut c).inbox.writes = vec![id("write")];
            if cancelled {
                c.state = pi_harness_core::control::abort_recipe(&c, &f, n(2), &[], &"test")
                    .unwrap()
                    .recipe
                    .unwrap()
                    .next
                    .unwrap();
            }
            let expected = matches!(name, "run.checkpoint" | "run.failure_drain.response")
                || cancelled
                    && matches!(
                        name,
                        "run.assistant.ready"
                            | "run.assistant.retry_wait"
                            | "run.deferred.suspended"
                    );
            assert_eq!(
                select(&c).unwrap().is_some(),
                expected,
                "{name}, cancelled={cancelled}"
            );
        }
    }
}

#[test]
fn full_pending_handle_metadata_not_only_id_defines_lineage() {
    for field in [
        "provider",
        "modelId",
        "api",
        "expiresAt",
        "pollAfterMs",
        "data",
    ] {
        let (mut c, mut i, f) = fixture("run.deferred.suspended");
        i.deferred_poll = DeferredPollPermit::One;
        let recipe = poll(&c, &i, &f, "r", "u");
        c.state = recipe.next.unwrap();
        let effect = recipe.dispatch.unwrap();
        let EffectPlan::Deferred(p) = &effect else {
            panic!()
        };
        let PlannerLoadedValue::Entry(Entry::Message(e)) =
            &i.loaded[p.deferred.source_entry_id.as_str()]
        else {
            panic!()
        };
        let AgentMessage::BuiltIn(Message::Assistant(source)) = &e.message else {
            panic!()
        };
        let mut handle = serde_json::to_value(source.deferred.as_ref().unwrap()).unwrap();
        let original_id = handle["id"].clone();
        handle[field] = match field {
            "expiresAt" | "pollAfterMs" => serde_json::json!(20),
            "data" => serde_json::Value::Null,
            _ => serde_json::json!("different"),
        };
        assert_eq!(handle["id"], original_id);
        let mut message = response("stop").into_message();
        message.stop_reason = StopReason::Deferred;
        message.deferred = Some(serde_json::from_value(handle).unwrap());
        let commit = SettlementCommit {
            current: c.clone(),
            plan: effect.clone(),
            output: SettlementOutput::Deferred {
                key: effect.key().clone(),
                message: SettledAssistantMessage::new(message).unwrap(),
            },
            telemetry_context: "test",
        };
        let RecipeDecision::Apply(recipe) = settlement_recipe(
            &c,
            &commit,
            &i,
            &f,
            &SettlementFacts::Deferred {
                overflow: false,
                followers: vec![],
            },
        )
        .unwrap() else {
            panic!()
        };
        c.state = recipe.next.unwrap();
        assert!(
            matches!(run_mut(&mut c).phase, RunPhase::FailureDrain(_)),
            "{field}"
        );
    }
}

#[test]
fn stale_deferred_projection_and_cancel_not_started_are_exact() {
    let (mut c, mut i, f) = fixture("run.deferred.suspended");
    i.deferred_poll = DeferredPollPermit::One;
    let recipe = poll(&c, &i, &f, "r", "u");
    c.state = recipe.next.unwrap();
    let effect = recipe.dispatch.unwrap();
    let mut wrong = effect.clone();
    let EffectPlan::Deferred(p) = &mut wrong else {
        panic!()
    };
    p.deferred.response_entry_id = id("wrong");
    i.running.insert(wrong.key().clone(), wrong.clone());
    assert_eq!(
        next_action(&c, &i, &deferred_facts(&c)).unwrap_err(),
        PlanError::InvalidInput
    );
    i.running.clear();
    let stale = SettlementCommit {
        current: c.clone(),
        plan: wrong.clone(),
        output: SettlementOutput::Deferred {
            key: wrong.key().clone(),
            message: response("stop"),
        },
        telemetry_context: "test",
    };
    assert!(matches!(
        settlement_recipe(
            &c,
            &stale,
            &i,
            &f,
            &SettlementFacts::Deferred {
                overflow: false,
                followers: vec![]
            }
        )
        .unwrap(),
        RecipeDecision::NoLongerExpected
    ));
    c.state = pi_harness_core::control::abort_recipe(&c, &f, n(99), &[], &"test")
        .unwrap()
        .recipe
        .unwrap()
        .next
        .unwrap();
    let EffectPlan::Deferred(p) = &effect else {
        panic!()
    };
    let mut synthetic = response("aborted").into_message();
    synthetic.usage = Usage::zero();
    synthetic.provider = p.deferred.configuration.model.provider.clone();
    synthetic.model = p.deferred.configuration.model.model_id.clone();
    let commit = SettlementCommit {
        current: c.clone(),
        plan: effect.clone(),
        output: SettlementOutput::NotStarted {
            key: effect.key().clone(),
        },
        telemetry_context: "test",
    };
    let RecipeDecision::Apply(recipe) = settlement_recipe(
        &c,
        &commit,
        &i,
        &f,
        &SettlementFacts::NotStarted {
            value: ReconciledValue::Assistant(Box::new(
                SettledAssistantMessage::new(synthetic).unwrap(),
            )),
        },
    )
    .unwrap() else {
        panic!()
    };
    assert!(
        matches!(&recipe.transaction.writes[2], Write::Usage(row) if row.usage == Usage::zero() && row.id.as_str() == "u")
    );
}

#[test]
fn structural_failure_provenance_survives_unprojected_write_then_finishes() {
    let (mut c, i, f) = fixture("run.failure_drain.response");
    let r = run_mut(&mut c);
    let RunPhase::FailureDrain(p) = &mut r.phase else {
        panic!()
    };
    p.provenance = FailureProvenance::Structural {
        task_id: TaskId::new("structural").unwrap(),
    };
    r.inbox.writes = vec![id("custom")];
    let recipe = transition(&c, &i, &f, &inbox(&c, false, None));
    c.state = recipe.next.unwrap();
    assert!(
        matches!(&run_mut(&mut c).phase, RunPhase::FailureDrain(p) if matches!(p.provenance, FailureProvenance::Structural { .. }))
    );
    assert!(matches!(
        next_action(&c, &i, &PlanningFacts::Terminal).unwrap(),
        ProcedureAction::Existing(Action::Finish(_))
    ));
}
