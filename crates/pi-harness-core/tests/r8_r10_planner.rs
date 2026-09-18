mod r8_r10_support;
use pi_agent_schema::session_values::*;
use pi_agent_schema::*;
use pi_harness_core::{compaction, navigation, planner::*, structural};
use r8_r10_support::*;
#[test]
fn nested_intents_and_usage_keep_one_outer_identity_and_reject_wrong_tuples() {
    for kind in ["manual", "run", "navigation"] {
        let (mut c, mut i, f) = fixture(kind, "pending", false);
        let p = structural::plan(&c, &f.session_id, &()).unwrap();
        for axis in 0..7 {
            let mut req = request(&p);
            let mut outer = p.clone();
            match axis {
                0 => outer.key.operation_id = OperationId::new("wrong").unwrap(),
                1 => req.task_id = TaskId::new("wrong").unwrap(),
                2 => req.attempt = OneBasedSafeUnsigned::new(2).unwrap(),
                3 => req.request_index = n(1),
                4 => req.configuration.thinking_level = AgentThinkingLevel::High,
                5 => outer.key.session_id = SessionId::new("wrong").unwrap(),
                _ => req.usage_id = UsageId::new("summary").unwrap(),
            };
            assert!(
                matches!(
                    structural::nested_intent_recipe(&c, &outer, &req, &f).unwrap(),
                    RecipeDecision::NoLongerExpected
                ),
                "{kind}/{axis}"
            )
        }
        i.running
            .insert(p.key.clone(), EffectPlan::Summary(p.clone()));
        for index in 0..2 {
            let mut req = request(&p);
            req.request_index = n(index);
            req.usage_id = UsageId::new(format!("usage{index}")).unwrap();
            let intent = applied(structural::nested_intent_recipe(&c, &p, &req, &f).unwrap());
            c.state = intent.next.unwrap();
            c.operation_state_seq = n(c.operation_state_seq.get() + 1);
            assert!(matches!(
                next_action(&c, &i, &planning(&f)).unwrap(),
                ProcedureAction::Existing(Action::AwaitEffect(_))
            ));
            let mut settle = SummaryRequestSettlement {
                current: c.clone(),
                task_id: req.task_id,
                attempt: req.attempt,
                request_index: req.request_index,
                usage_id: req.usage_id,
                response: response(),
                telemetry_context: (),
            };
            settle.usage_id = UsageId::new("wrong").unwrap();
            assert!(matches!(
                structural::settle_request_recipe(&c, &p, &settle, &f).unwrap(),
                RecipeDecision::NoLongerExpected
            ));
            settle.usage_id = UsageId::new(format!("usage{index}")).unwrap();
            let receipt = applied(structural::settle_request_recipe(&c, &p, &settle, &f).unwrap());
            assert!(matches!(
                &receipt.transaction.writes[..],
                [Write::Usage(_), Write::SetRegister(_)]
            ));
            assert_eq!(receipt.expected[0].seq, Some(c.operation_state_seq));
            c.state = receipt.next.unwrap();
            c.operation_state_seq = n(c.operation_state_seq.get() + 1);
            assert!(matches!(
                structural::settle_request_recipe(&c, &p, &settle, &f).unwrap(),
                RecipeDecision::NoLongerExpected
            ));
        }
        let mut third = request(&p);
        third.request_index = n(2);
        assert!(matches!(
            structural::nested_intent_recipe(&c, &p, &third, &f).unwrap(),
            RecipeDecision::NoLongerExpected
        ));
    }
}
#[test]
fn suspended_carrier_maps_owning_operation_without_write_or_cancellation_alias() {
    for kind in ["manual", "run", "navigation"] {
        let (mut c, _, f) = fixture(kind, "pending", false);
        let p = structural::plan(&c, &f.session_id, &()).unwrap();
        let missing = MissingIdentities {
            tools: vec![],
            models: vec!["p/m".into()],
        };
        let o = SummarySuspendedOutput::new(&p, missing).unwrap();
        let RecipeDecision::Suspend(result) = structural::suspend_recipe(&c, &p, &o, &f).unwrap()
        else {
            panic!()
        };
        assert!(matches!(
            (kind, *result),
            (
                "manual",
                OperationResult::Compaction(CompactionOutcome::MissingIdentities(_))
            ) | ("run", OperationResult::Run(_))
                | (
                    "navigation",
                    OperationResult::Navigation(NavigationOutcome::MissingIdentities(_))
                )
        ));
        cancelled(&mut c);
        assert!(matches!(
            structural::suspend_recipe(&c, &p, &o, &f).unwrap(),
            RecipeDecision::NoLongerExpected
        ));
    }
}
#[test]
fn unknown_attempt_matrix_never_resumes_partial_text_or_duplicates_settled_usage() {
    for cancel in [false, true] {
        for cap in [false, true] {
            for outstanding in [false, true] {
                let (mut c, i, f) = fixture("manual", "pending", outstanding);
                let mut p = structural::pending(&c).unwrap();
                p.usage_ids = if outstanding {
                    vec![]
                } else {
                    vec![UsageId::new("settled").unwrap()]
                };
                if cap {
                    p.attempt = OneBasedSafeUnsigned::new(3).unwrap()
                };
                c.state = structural::replace(
                    &c,
                    StructuralDecision::Generating {
                        task_id: p.context.task_id.clone(),
                        generation: Box::new(SummaryGeneration::EffectPending {
                            context: p.context,
                            attempt: p.attempt,
                            request: p.request,
                            usage_ids: p.usage_ids,
                        }),
                    },
                )
                .unwrap();
                if cancel {
                    cancelled(&mut c)
                };
                let ProcedureAction::Reconcile(commit) =
                    next_action(&c, &i, &planning(&f)).unwrap()
                else {
                    panic!()
                };
                let r = settlement_recipe(
                    &c,
                    &commit,
                    &i,
                    &f,
                    &SettlementFacts::Structural(Default::default()),
                )
                .unwrap();
                if outstanding && (cap || cancel) {
                    let r = applied(r);
                    assert!(
                        matches!(&r.transaction.writes[0],Write::Usage(u) if u.id.as_str()=="usage0"&&u.usage==Usage::zero())
                    );
                    assert!(
                        structural::pending(&CurrentOperation {
                            state: r.next.unwrap(),
                            ..c.clone()
                        })
                        .unwrap()
                        .request
                        .is_none()
                    );
                } else if !cap && !cancel {
                    let r = applied(r);
                    assert!(
                        !r.transaction
                            .writes
                            .iter()
                            .any(|w| matches!(w, Write::Usage(_)))
                    );
                    assert!(
                        matches!(structural::decision(&CurrentOperation{state:r.next.unwrap(),..c.clone()}),Some(StructuralDecision::Generating{generation,..}) if matches!(generation.as_ref(),SummaryGeneration::Ready{next_attempt,..} if next_attempt.get()==2))
                    );
                } else {
                    assert!(matches!(r, RecipeDecision::Terminal(_)));
                }
            }
        }
    }
}
#[test]
fn not_started_has_no_usage_but_started_response_keeps_actual_usage_after_abort() {
    let (mut c, mut i, f) = fixture("manual", "pending", true);
    let p = structural::plan(&c, &f.session_id, &()).unwrap();
    i.running
        .insert(p.key.clone(), EffectPlan::Summary(p.clone()));
    cancelled(&mut c);
    let commit = SettlementCommit {
        current: c.clone(),
        plan: EffectPlan::Summary(p.clone()),
        output: SettlementOutput::NotStarted { key: p.key.clone() },
        telemetry_context: (),
    };
    let r = applied(
        settlement_recipe(
            &c,
            &commit,
            &i,
            &f,
            &SettlementFacts::Structural(Default::default()),
        )
        .unwrap(),
    );
    assert!(
        !r.transaction
            .writes
            .iter()
            .any(|w| matches!(w, Write::Usage(_)))
    );
    let request = SummaryRequestSettlement {
        current: c.clone(),
        task_id: p.generation.context.task_id.clone(),
        attempt: p.generation.attempt,
        request_index: n(0),
        usage_id: UsageId::new("usage0").unwrap(),
        response: response(),
        telemetry_context: (),
    };
    let r = applied(structural::settle_request_recipe(&c, &p, &request, &f).unwrap());
    assert!(
        matches!(&r.transaction.writes[0],Write::Usage(u) if u.usage==response().as_message().usage)
    );
}
#[test]
fn threshold_comparison_priority_and_durable_mark_have_finite_boundaries() {
    for enabled in [false, true] {
        for (tokens, window, reserve, due) in [
            (89, 100, 10, false),
            (90, 100, 10, false),
            (91, 100, 10, true),
            (0, 5, 10, true),
        ] {
            let (mut c, i, f) = base();
            let OperationState::Run(r) = &mut c.state else {
                panic!()
            };
            r.inbox = Default::default();
            r.settings.compaction = CompactionSettings {
                enabled,
                reserve_tokens: n(reserve),
                keep_recent_tokens: n(10),
            };
            let settings = r.settings.compaction.clone();
            assert_eq!(
                compaction::threshold_due(n(tokens), n(window), &settings),
                enabled && due
            );
            let fact = PlanningFacts::Threshold(compaction::Threshold {
                telemetry_context: (),
                context_tokens: n(tokens),
                context_window: n(window),
                prepared: None,
            });
            let ProcedureAction::Existing(Action::Transition(p)) =
                next_action(&c, &i, &fact).unwrap()
            else {
                panic!()
            };
            let commit = TransitionCommit {
                current: c.clone(),
                next: p.next,
                telemetry_context: (),
                expected_configuration_seq: p.expected_configuration_seq,
                expected_settings_revision: p.expected_settings_revision,
            };
            let recipe = applied(
                pi_harness_core::transition::transition_recipe(&c, &commit, &i, &f, &fact).unwrap(),
            );
            c.state = recipe.next.unwrap();
            assert!(next_action(&c, &i, &fact).is_err());
        }
    }
}
#[test]
fn overflow_classifier_does_not_widen_stop_or_preclamp_length() {
    let mut m = response().into_message();
    m.usage.output = n(9);
    for (reason, flag, want) in [
        (StopReason::Stop, true, false),
        (StopReason::Error, true, true),
        (StopReason::Error, false, false),
        (StopReason::Length, false, true),
    ] {
        m.stop_reason = reason;
        assert_eq!(compaction::is_overflow(&m, n(10), flag), want)
    }
    m.stop_reason = StopReason::Length;
    m.usage.output = n(10);
    assert!(!compaction::is_overflow(&m, n(10), true));
}
#[test]
fn navigation_validation_distinguishes_root_current_and_session_target() {
    let mut i = NavigationIntent {
        kind: Default::default(),
        target_id: None,
        summarize: false,
        label: None,
        custom_instructions: None,
    };
    assert!(navigation::validate(&Some(id("source")), &i, None).is_ok());
    i.label = Some("x".into());
    assert_eq!(
        navigation::validate(&Some(id("source")), &i, None),
        Err(navigation::AdmissionError::InvalidNavigation)
    );
    i.label = None;
    i.target_id = Some(id("target"));
    assert_eq!(
        navigation::validate(&Some(id("source")), &i, None),
        Err(navigation::AdmissionError::UnknownTarget)
    );
    assert_eq!(
        navigation::validate(&Some(id("target")), &i, None),
        Err(navigation::AdmissionError::InvalidNavigation)
    );
}
#[test]
fn decision_hook_sources_and_ready_dispatch_preserve_preparation_and_capture_guards() {
    for kind in ["manual", "run", "navigation"] {
        for decline in [false, true] {
            let (c, i, f) = fixture(kind, "deciding", false);
            let ProcedureAction::Existing(Action::Dispatch(d)) =
                next_action(&c, &i, &planning(&f)).unwrap()
            else {
                panic!()
            };
            let EffectPlan::Hook(h) = d.effect else {
                panic!()
            };
            let result = if kind == "navigation" {
                HookResult::BeforeNavigation(
                    decline.then_some(BeforeNavigationResult::DeclineTrue(Default::default())),
                )
            } else {
                HookResult::BeforeCompaction(
                    decline
                        .then(|| Box::new(BeforeCompactionResult::DeclineTrue(Default::default()))),
                )
            };
            let output = HookEffectOutput::new(&h, result).unwrap();
            let commit = SettlementCommit {
                current: c.clone(),
                plan: EffectPlan::Hook(h),
                output: SettlementOutput::Hook(output),
                telemetry_context: (),
            };
            let result = settlement_recipe(
                &c,
                &commit,
                &i,
                &f,
                &SettlementFacts::Structural(structural::Settlement {
                    result_entry_id: Some(id("summary")),
                    ..Default::default()
                }),
            )
            .unwrap();
            if decline {
                assert!(matches!(result, RecipeDecision::Terminal(_)) || kind == "run");
            } else {
                let r = applied(result);
                assert_eq!(
                    r.expected_settings_revision,
                    Some(i.runtime.settings_revision)
                );
                assert!(
                    r.expected
                        .iter()
                        .any(|p| p.namespace == RegisterNamespace::LaneConfig
                            && p.seq == Some(c.configuration_seq))
                );
                assert!(
                    r.expected
                        .iter()
                        .any(|p| p.namespace == RegisterNamespace::OperationPreparation
                            && p.seq == Some(n(6)))
                );
                let mut c = c.clone();
                c.state = r.next.unwrap();
                let ProcedureAction::Existing(Action::Transition(t)) =
                    next_action(&c, &i, &planning(&f)).unwrap()
                else {
                    panic!()
                };
                let r = applied(
                    pi_harness_core::transition::transition_recipe(
                        &c,
                        &TransitionCommit {
                            current: c.clone(),
                            next: t.next,
                            telemetry_context: (),
                            expected_configuration_seq: t.expected_configuration_seq,
                            expected_settings_revision: t.expected_settings_revision,
                        },
                        &i,
                        &f,
                        &planning(&f),
                    )
                    .unwrap(),
                );
                assert!(matches!(r.dispatch, Some(EffectPlan::Summary(_))));
            }
        }
    }
}
#[test]
fn admission_empty_atomic_preparation_and_structural_abort_repeat_are_distinct() {
    let (c, i, f) = fixture("manual", "deciding", false);
    let mut a = structural::Acceptance {
        operation: c.operation.clone(),
        lane_state: LaneState {
            current_operation_id: None,
            pending_next_run: vec![],
        },
        lane_seq: n(4),
        leaf_seq: Some(n(3)),
        source_leaf: c.operation.source_leaf_id.clone(),
        configuration_seq: c.configuration_seq,
        settings_revision: i.runtime.settings_revision,
        identities_present: true,
        task_id: TaskId::new("task").unwrap(),
        preparation: None,
        target: None,
        telemetry_context: (),
    };
    assert!(structural::acceptance_recipe(&a).unwrap().is_none());
    a.preparation = Some(prep());
    let r = structural::acceptance_recipe(&a).unwrap().unwrap();
    assert_eq!(r.transaction.writes.len(), 4);
    assert!(
        matches!(&r.transaction.writes[0],Write::SetRegister(r) if matches!(r.as_ref(),RegisterSet::OperationPreparation{..}))
    );
    assert!(
        r.expected
            .iter()
            .any(|p| p.namespace == RegisterNamespace::OperationPreparation && p.seq.is_none())
    );
    for kind in ["manual", "navigation"] {
        let (mut c, _, _) = fixture(kind, "ready", false);
        let first = pi_harness_core::control::abort_recipe(&c, &f, n(50), &[], &()).unwrap();
        c.state = first.recipe.unwrap().next.unwrap();
        let again = pi_harness_core::control::abort_recipe(&c, &f, n(80), &[], &()).unwrap();
        assert!(again.recipe.is_none());
        assert_eq!(again.result, first.result);
    }
}
#[test]
fn inrun_overflow_and_threshold_decline_keep_different_provenance() {
    let (mut c, i, f) = fixture("run", "deciding", false);
    let OperationState::Run(r) = &mut c.state else {
        panic!()
    };
    let RunPhase::Compaction(p) = &mut r.phase else {
        panic!()
    };
    p.reason = RunCompactionReason::Overflow;
    p.resume_after.continuation = Continuation::NeedAssistant {
        overflow_recovery_used: true,
    };
    let ProcedureAction::Existing(Action::Dispatch(d)) =
        next_action(&c, &i, &planning(&f)).unwrap()
    else {
        panic!()
    };
    let EffectPlan::Hook(h) = d.effect else {
        panic!()
    };
    let output = HookEffectOutput::new(
        &h,
        HookResult::BeforeCompaction(Some(Box::new(BeforeCompactionResult::DeclineTrue(
            Default::default(),
        )))),
    )
    .unwrap();
    let commit = SettlementCommit {
        current: c.clone(),
        plan: EffectPlan::Hook(h),
        output: SettlementOutput::Hook(output),
        telemetry_context: (),
    };
    let r = applied(
        settlement_recipe(
            &c,
            &commit,
            &i,
            &f,
            &SettlementFacts::Structural(Default::default()),
        )
        .unwrap(),
    );
    assert!(
        matches!(r.next,Some(OperationState::Run(r)) if matches!(r.phase,RunPhase::FailureDrain(FailureDrainPhase{provenance:FailureProvenance::Structural{..},..})))
    );
}
#[test]
fn actual_assistant_overflow_recipe_is_atomic_and_second_failure_is_bounded() {
    for used in [false, true] {
        for prepared in [false, true] {
            for cancel in [false, true] {
                let (mut c, mut i, f) = base();
                let cases: serde_json::Value = serde_json::from_str(include_str!(
                    "../../../conformance/harness/r1-r4/core/states.json"
                ))
                .unwrap();
                let case = cases
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|v| v["name"] == "run.assistant.effect_pending")
                    .unwrap();
                c.state = serde_json::from_value(case["roots"]["operation_state"]["value"].clone())
                    .unwrap();
                let OperationState::Run(r) = &mut c.state else {
                    panic!()
                };
                r.settings.compaction = CompactionSettings {
                    enabled: true,
                    reserve_tokens: n(10),
                    keep_recent_tokens: n(10),
                };
                let RunPhase::Assistant(a) = &mut r.phase else {
                    panic!()
                };
                let Generation::EffectPending { context, .. } = &mut a.generation else {
                    panic!()
                };
                context.overflow_recovery_used = used;
                let p = PendingGeneration::try_from(a.generation.clone()).unwrap();
                let effect = AssistantEffectPlan {
                    key: EffectKey {
                        session_id: f.session_id.clone(),
                        lane: c.operation.lane.clone(),
                        operation_id: c.operation.operation_id.clone(),
                        kind: EffectKind::Assistant,
                        step_or_task_id: EffectStepId::new(p.context.step_id.as_str()).unwrap(),
                        logical_attempt: n(p.attempt.get()),
                        item_index: None,
                    },
                    generation: p.clone(),
                    stream_options: p.context.stream_options.clone(),
                    telemetry_context: (),
                };
                i.running
                    .insert(effect.key.clone(), EffectPlan::Assistant(effect.clone()));
                if cancel {
                    cancelled(&mut c)
                }
                let mut m = response().into_message();
                m.stop_reason = StopReason::Error;
                m.error_message = Some("context too large".into());
                let output = SettledAssistantMessage::new(m).unwrap();
                let commit = SettlementCommit {
                    current: c.clone(),
                    plan: EffectPlan::Assistant(effect.clone()),
                    output: SettlementOutput::Assistant {
                        key: effect.key,
                        message: output,
                    },
                    telemetry_context: (),
                };
                let extra = SettlementFacts::Overflow(compaction::Overflow {
                    retryable: true,
                    matching_error: true,
                    prepared: prepared.then(|| compaction::PreparedCompaction {
                        task_id: TaskId::new("overflow-task").unwrap(),
                        preparation: prep(),
                    }),
                    followers: vec![],
                });
                let recipe = applied(settlement_recipe(&c, &commit, &i, &f, &extra).unwrap());
                let has_prep=recipe.transaction.writes.iter().any(|w|matches!(w,Write::SetRegister(r) if matches!(r.as_ref(),RegisterSet::OperationPreparation{..})));
                assert_eq!(has_prep, prepared && !used && !cancel);
                assert!(matches!(&recipe.transaction.writes[0], Write::Entry(_)));
                assert!(matches!(&recipe.transaction.writes[2], Write::Usage(_)));
                let OperationState::Run(next) = recipe.next.unwrap() else {
                    panic!()
                };
                if cancel {
                    assert!(matches!(next.phase, RunPhase::Checkpoint(_)))
                } else if prepared && !used {
                    assert!(
                        matches!(next.phase,RunPhase::Compaction(p) if matches!(p.resume_after.continuation,Continuation::NeedAssistant{overflow_recovery_used:true}))
                    );
                } else {
                    assert!(matches!(
                        next.phase,
                        RunPhase::FailureDrain(FailureDrainPhase {
                            provenance: FailureProvenance::Response { .. },
                            ..
                        })
                    ));
                }
            }
        }
    }
}
#[test]
fn deferred_overflow_uses_poll_failure_table_without_retry_or_compaction() {
    for reason in [StopReason::Error, StopReason::Length, StopReason::Stop] {
        let (mut c, mut i, f) = base();
        let cases: serde_json::Value = serde_json::from_str(include_str!(
            "../../../conformance/harness/r1-r4/core/states.json"
        ))
        .unwrap();
        let case = cases
            .as_array()
            .unwrap()
            .iter()
            .find(|v| v["name"] == "run.deferred.suspended")
            .unwrap();
        c.state =
            serde_json::from_value(case["roots"]["operation_state"]["value"].clone()).unwrap();
        for e in case["entries"].as_array().unwrap() {
            i.loaded.insert(
                e["id"].as_str().unwrap().into(),
                PlannerLoadedValue::Entry(serde_json::from_value(e.clone()).unwrap()),
            );
        }
        let OperationState::Run(r) = &mut c.state else {
            panic!()
        };
        let RunPhase::Deferred(d) = &mut r.phase else {
            panic!()
        };
        let Deferred::Suspended {
            step_id,
            source_entry_id,
            configuration,
            stream_options,
            ..
        } = &d.deferred
        else {
            panic!()
        };
        d.deferred = Deferred::EffectPending {
            step_id: step_id.clone(),
            source_entry_id: source_entry_id.clone(),
            poll: OneBasedSafeUnsigned::new(1).unwrap(),
            response_entry_id: id("poll-r"),
            usage_id: UsageId::new("poll-u").unwrap(),
            configuration: configuration.clone(),
            stream_options: stream_options.clone(),
        };
        let plan = pi_harness_core::deferred::plan(&c, &f.session_id, &()).unwrap();
        let mut m = response().into_message();
        m.stop_reason = reason;
        if reason == StopReason::Error {
            m.error_message = Some("overflow".into())
        };
        let commit = SettlementCommit {
            current: c.clone(),
            plan: EffectPlan::Deferred(plan.clone()),
            output: SettlementOutput::Deferred {
                key: plan.key,
                message: SettledAssistantMessage::new(m).unwrap(),
            },
            telemetry_context: (),
        };
        let r = applied(
            settlement_recipe(
                &c,
                &commit,
                &i,
                &f,
                &SettlementFacts::Deferred {
                    overflow: true,
                    followers: vec![],
                },
            )
            .unwrap(),
        );
        let OperationState::Run(next) = r.next.unwrap() else {
            panic!()
        };
        assert_eq!(
            matches!(next.phase, RunPhase::FailureDrain(_)),
            reason != StopReason::Stop
        );
        assert!(!r.transaction.writes.iter().any(|w|matches!(w,Write::SetRegister(v)if matches!(v.as_ref(),RegisterSet::OperationPreparation{..}))));
    }
}

#[test]
fn overflow_normalization_labels_heuristic_and_preserves_observed_reason() {
    let mut m = response().into_message();
    m.stop_reason = StopReason::Error;
    m.error_message = Some("original context limit".into());
    compaction::normalize_overflow(&mut m, Some(n(100)));
    assert_eq!(
        m.error_message.as_deref(),
        Some("Context overflow heuristic: original context limit")
    );
    m.stop_reason = StopReason::Length;
    m.usage.output = n(9);
    compaction::normalize_overflow(&mut m, Some(n(10)));
    assert_eq!(
        m.error_message.as_deref(),
        Some("Context overflow heuristic: length output 9 below intended limit 10")
    );
}
#[test]
fn hook_and_generated_compaction_publish_same_payload_with_distinct_source_and_continuation() {
    for kind in ["manual", "run"] {
        for hook in [false, true] {
            let (c, mut i, f) = fixture(kind, if hook { "deciding" } else { "pending" }, false);
            let semantic = CompactResult {
                summary: "exact semantic result".into(),
                tokens_before: n(90),
                retained_tail: vec![],
                usage: None,
                details: Presence::Null,
            };
            let entry:Entry=serde_json::from_value(serde_json::json!({"type":"compaction","id":"summary","parentId":"leaf","seq":200,"timestamp":70,"summary":semantic.summary,"tokensBefore":90,"retainedTail":[],"details":null,"fromHook":hook})).unwrap();
            let (plan, output) = if hook {
                let ProcedureAction::Existing(Action::Dispatch(d)) =
                    next_action(&c, &i, &planning(&f)).unwrap()
                else {
                    panic!()
                };
                let EffectPlan::Hook(h) = d.effect else {
                    panic!()
                };
                let o = HookEffectOutput::new(
                    &h,
                    HookResult::BeforeCompaction(Some(Box::new(
                        BeforeCompactionResult::Compaction(Box::new(BeforeCompactionSupply {
                            decline: None,
                            compaction: semantic.clone(),
                        })),
                    ))),
                )
                .unwrap();
                (EffectPlan::Hook(h), SettlementOutput::Hook(o))
            } else {
                let p = structural::plan(&c, &f.session_id, &()).unwrap();
                i.running
                    .insert(p.key.clone(), EffectPlan::Summary(p.clone()));
                (
                    EffectPlan::Summary(p.clone()),
                    SettlementOutput::Summary {
                        key: p.key,
                        outcome: SummaryAttemptOutcome::Success(SummarySuccessResult::Compact(
                            semantic.clone(),
                        )),
                    },
                )
            };
            let commit = SettlementCommit {
                current: c.clone(),
                plan,
                output,
                telemetry_context: (),
            };
            let extra = SettlementFacts::Structural(structural::Settlement {
                prepared_entry: Some(entry.clone()),
                ..Default::default()
            });
            let result = settlement_recipe(&c, &commit, &i, &f, &extra).unwrap();
            if kind == "manual" {
                let RecipeDecision::Terminal(t) = result else {
                    panic!()
                };
                assert_eq!(
                    t.result,
                    structural::completed_result(
                        &c,
                        &SummarySuccessResult::Compact(semantic),
                        &entry,
                        hook
                    )
                    .unwrap()
                );
            } else {
                let r = applied(result);
                assert!(
                    !r.transaction
                        .writes
                        .iter()
                        .any(|w| matches!(w, Write::Usage(_)))
                );
                assert!(
                    matches!(r.next,Some(OperationState::Run(r)) if matches!(&r.phase,RunPhase::Checkpoint(p) if p.threshold_checked_trigger_entry_id==Some(id("leaf"))))
                );
            }
        }
    }
}
#[test]
fn cancelled_inrun_ready_reconciles_to_checkpoint_without_summary_dispatch() {
    let (mut c, i, f) = fixture("run", "ready", false);
    cancelled(&mut c);
    let facts = planning(&f);
    let ProcedureAction::Existing(Action::Transition(t)) = next_action(&c, &i, &facts).unwrap()
    else {
        panic!()
    };
    let result = pi_harness_core::transition::transition_recipe(
        &c,
        &TransitionCommit {
            current: c.clone(),
            next: t.next,
            telemetry_context: (),
            expected_configuration_seq: t.expected_configuration_seq,
            expected_settings_revision: t.expected_settings_revision,
        },
        &i,
        &f,
        &facts,
    );
    let r =
        applied(result.expect("cancelled ready transition must not construct a summary dispatch"));
    assert!(r.dispatch.is_none());
    assert!(
        matches!(r.next,Some(OperationState::Run(r)) if matches!(r.phase,RunPhase::Checkpoint(_)))
    );
}
#[test]
fn prepared_completion_cannot_skip_source_selection_or_complete_ready_work() {
    for (status, from_hook) in [
        ("ready", false),
        ("retry", false),
        ("deciding", false),
        ("pending", true),
    ] {
        let (c, _, _) = fixture("manual", status, false);
        let semantic = CompactResult {
            summary: "result".into(),
            tokens_before: n(90),
            retained_tail: vec![],
            usage: None,
            details: Presence::Missing,
        };
        let e:Entry=serde_json::from_value(serde_json::json!({"type":"compaction","id":"summary","parentId":"leaf","seq":200,"timestamp":70,"summary":"result","tokensBefore":90,"retainedTail":[],"fromHook":from_hook})).unwrap();
        assert!(
            structural::completed_result(
                &c,
                &SummarySuccessResult::Compact(semantic),
                &e,
                from_hook
            )
            .is_err(),
            "{status}/{from_hook}"
        );
    }
}
