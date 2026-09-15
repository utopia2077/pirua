use pi_agent_schema::session_values::*;
use pi_agent_schema::*;
use pi_harness_core::{planner::*, terminal::*, transition::*};
mod r1_r4_support;
use r1_r4_support::*;
use std::collections::BTreeMap;

#[test]
fn checkpoint_captures_configuration_options_retry_and_all_tokens() {
    let (mut current, inputs, facts) = fixture("run.checkpoint");
    if let OperationState::Run(r) = &mut current.state
        && let RunPhase::Checkpoint(p) = &mut r.phase
    {
        p.continuation = Continuation::NeedAssistant {
            overflow_recovery_used: false,
        }
    }

    let planning = PlanningFacts::Checkpoint {
        session_id: facts.session_id.clone(),
        telemetry_context: "explicit-C",
        step_id: StepId::new("fresh-step").unwrap(),
    };
    let ProcedureAction::Existing(Action::Transition(plan)) =
        next_action(&current, &inputs, &planning).unwrap()
    else {
        panic!()
    };
    let commit = TransitionCommit {
        current: current.clone(),
        next: plan.next.clone(),
        telemetry_context: plan.telemetry_context,
        expected_configuration_seq: plan.expected_configuration_seq,
        expected_settings_revision: plan.expected_settings_revision,
    };
    let recipe = apply(transition_recipe(&current, &commit, &inputs, &facts, &planning).unwrap());
    assert_eq!(recipe.transaction.writes.len(), 1);
    assert_eq!(recipe.expected_settings_revision, Some(n(7)));
    assert!(
        recipe
            .expected
            .iter()
            .any(|e| e.namespace == RegisterNamespace::LaneConfig && e.seq == Some(n(12)))
    );
    let OperationState::Run(r) = recipe.next.unwrap() else {
        panic!()
    };
    let RunPhase::Assistant(AssistantPhase {
        generation: Generation::Ready {
            context,
            next_attempt,
        },
        ..
    }) = r.phase
    else {
        panic!()
    };
    assert_eq!(context.step_id.as_str(), "fresh-step");
    assert_eq!(context.configuration, current.configuration);
    assert_eq!(context.retry_policy, inputs.runtime.retry_policy);
    assert_eq!(next_attempt.get(), 1);
    let mut newer = current.clone();
    newer.operation_state_seq = n(20);
    assert_eq!(
        transition_recipe(&newer, &commit, &inputs, &facts, &planning).unwrap(),
        RecipeDecision::NoLongerExpected
    );
}
#[test]
fn full_options_intent_reserves_ids_and_rejects_every_key_axis() {
    let (current, inputs, facts) = fixture("run.assistant.ready");
    let event = model_event(&current);
    let planning = PlanningFacts::BeforeRequest {
        session_id: facts.session_id.clone(),
        telemetry_context: "explicit-C",
        event: Box::new(event),
    };
    let ProcedureAction::Existing(Action::Dispatch(dispatch)) =
        next_action(&current, &inputs, &planning).unwrap()
    else {
        panic!()
    };
    let EffectPlan::Hook(h) = dispatch.effect else {
        panic!()
    };
    let options = AgentHarnessStreamOptions {
        timeout_ms: Some(n(1234)),
        ..Default::default()
    };
    let output = RequestOptionsOutput::new(&h, options.clone()).unwrap();
    let commit = SettlementCommit {
        current: current.clone(),
        plan: EffectPlan::Hook(h),
        output: SettlementOutput::RequestOptions(output),
        telemetry_context: "explicit-C",
    };
    let extra = SettlementFacts::Request {
        response_entry_id: EntryId::new("fresh-R").unwrap(),
        usage_id: UsageId::new("fresh-U").unwrap(),
    };
    let recipe = apply(settlement_recipe(&current, &commit, &inputs, &facts, &extra).unwrap());
    assert_eq!(recipe.transaction.writes.len(), 1);
    let EffectPlan::Assistant(plan) = recipe.dispatch.unwrap() else {
        panic!()
    };
    assert_eq!(plan.stream_options, options);
    assert_eq!(plan.generation.response_entry_id.as_str(), "fresh-R");
    assert_eq!(plan.generation.usage_id.as_str(), "fresh-U");
    assert_eq!(plan.generation.intended_output_limit.get(), 100);
    for axis in 0..7 {
        let mut wrong = commit.clone();
        let EffectPlan::Hook(h) = &mut wrong.plan else {
            panic!()
        };
        match axis {
            0 => h.key.session_id = SessionId::new("wrong").unwrap(),
            1 => h.key.lane = LaneId::new("wrong").unwrap(),
            2 => h.key.operation_id = OperationId::new("wrong").unwrap(),
            3 => h.key.kind = EffectKind::Tool,
            4 => h.key.step_or_task_id = EffectStepId::new("wrong").unwrap(),
            5 => h.key.logical_attempt = n(99),
            _ => h.key.item_index = Some(n(0)),
        }
        assert_eq!(
            settlement_recipe(&current, &wrong, &inputs, &facts, &extra).unwrap(),
            RecipeDecision::NoLongerExpected,
            "axis {axis}"
        );
    }
}
#[test]
fn assistant_settlement_is_atomic_preserves_latest_control_and_inbox() {
    let (current, inputs, facts) = fixture("run.assistant.effect_pending");
    let p = pending_plan(&current, &inputs, &facts);
    let commit = SettlementCommit {
        current: current.clone(),
        plan: EffectPlan::Assistant(p.clone()),
        output: SettlementOutput::Assistant {
            key: p.key,
            message: response("stop"),
        },
        telemetry_context: "explicit-C",
    };
    let extra = SettlementFacts::Assistant {
        retryable: false,
        overflow: false,
        followers: vec![],
    };
    let mut latest = current.clone();
    if let OperationState::Run(r) = &mut latest.state {
        r.inbox.steer.push(EntryId::new("new-steer").unwrap());
        r.control = Control::CancelRequested(CancelRequestedControl {
            status: Default::default(),
            requested_at: n(51),
            drained_steer: vec![],
            drained_follow_up: vec![],
        });
    }
    latest.operation_state_seq = n(20);
    let recipe = apply(settlement_recipe(&latest, &commit, &inputs, &facts, &extra).unwrap());
    assert_eq!(recipe.transaction.writes.len(), 4);
    assert_eq!(recipe.publication.len(), 2);
    let Write::Entry(NewEntry::Message(entry)) = &recipe.transaction.writes[0] else {
        panic!()
    };
    let AgentMessage::BuiltIn(Message::Assistant(m)) = &entry.message else {
        panic!()
    };
    assert_eq!(m.stop_reason, StopReason::Aborted);
    let OperationState::Run(r) = recipe.next.unwrap() else {
        panic!()
    };
    assert_eq!(r.inbox.steer[0].as_str(), "new-steer");
    assert!(matches!(r.control, Control::CancelRequested(_)));
    assert!(matches!(r.phase, RunPhase::Checkpoint(_)));
}
#[test]
fn generation_recovery_live_unknown_cap_retry_and_wrong_aborted() {
    let (mut current, mut inputs, facts) = fixture("run.assistant.effect_pending");
    let p = pending_plan(&current, &inputs, &facts);
    inputs
        .running
        .insert(p.key.clone(), EffectPlan::Assistant(p.clone()));
    let planning = PlanningFacts::Generation {
        session_id: facts.session_id.clone(),
        telemetry_context: "explicit-C",
        synthetic: None,
    };
    assert!(matches!(
        next_action(&current, &inputs, &planning).unwrap(),
        ProcedureAction::Existing(Action::AwaitEffect(_))
    ));
    inputs.running.clear();
    if let OperationState::Run(r) = &mut current.state
        && let RunPhase::Assistant(AssistantPhase {
            generation:
                Generation::EffectPending {
                    context, attempt, ..
                },
            ..
        }) = &mut r.phase
    {
        *attempt = OneBasedSafeUnsigned::new(1).unwrap();
        context.retry_policy.max_attempts = OneBasedSafeUnsigned::new(3).unwrap();
    }

    assert!(matches!(
        next_action(&current, &inputs, &planning).unwrap(),
        ProcedureAction::Existing(Action::Transition(_))
    ));
    let p = pending_plan(&current, &inputs, &facts);
    let commit = SettlementCommit {
        current: current.clone(),
        plan: EffectPlan::Assistant(p.clone()),
        output: SettlementOutput::Assistant {
            key: p.key,
            message: response("aborted"),
        },
        telemetry_context: "explicit-C",
    };
    assert_eq!(
        settlement_recipe(
            &current,
            &commit,
            &inputs,
            &facts,
            &SettlementFacts::Assistant {
                retryable: false,
                overflow: false,
                followers: vec![]
            }
        )
        .unwrap_err(),
        PlanError::InvalidCurrent
    );
    assert_eq!(
        pi_harness_core::recovery::retry_deadline(
            n(50),
            n(10),
            OneBasedSafeUnsigned::new(3).unwrap()
        )
        .unwrap(),
        n(90)
    );
    assert_eq!(
        pi_harness_core::recovery::retry_deadline(
            SafeUnsigned::MAX,
            n(1),
            OneBasedSafeUnsigned::new(1).unwrap()
        ),
        Ok(SafeUnsigned::MAX)
    );
}
#[test]
fn zero_calls_tool_use_requires_another_assistant_without_tool_rows() {
    let (current, inputs, facts) = fixture("run.assistant.effect_pending");
    let p = pending_plan(&current, &inputs, &facts);
    let commit = SettlementCommit {
        current: current.clone(),
        plan: EffectPlan::Assistant(p.clone()),
        output: SettlementOutput::Assistant {
            key: p.key,
            message: response("toolUse"),
        },
        telemetry_context: "explicit-C",
    };
    let recipe = apply(
        settlement_recipe(
            &current,
            &commit,
            &inputs,
            &facts,
            &SettlementFacts::Assistant {
                retryable: false,
                overflow: false,
                followers: vec![],
            },
        )
        .unwrap(),
    );
    let OperationState::Run(r) = recipe.next.unwrap() else {
        panic!()
    };
    assert!(matches!(
        r.phase,
        RunPhase::Checkpoint(CheckpointPhase {
            continuation: Continuation::NeedAssistant {
                overflow_recovery_used: false
            },
            ..
        })
    ));
    assert_eq!(recipe.transaction.writes.len(), 4);
}
#[test]
fn tool_clearance_saves_complete_args_and_source_order_controls_results() {
    let (current, inputs, facts) = fixture("run.tools.planned");
    let planning = PlanningFacts::Tools {
        session_id: facts.session_id.clone(),
        telemetry_context: "explicit-C",
        declarations: BTreeMap::new(),
        interrupted: tool_result(),
    };
    let ProcedureAction::Existing(Action::Dispatch(d)) =
        next_action(&current, &inputs, &planning).unwrap()
    else {
        panic!()
    };
    let EffectPlan::ToolClearance(clear) = d.effect else {
        panic!()
    };
    let args = JsonObject::new(BTreeMap::from([(
        "effective".into(),
        JsonValue::Bool(true),
    )]));
    let receipt = ToolClearanceOutput::new(
        &clear,
        ToolClearanceOutcome::Prepared {
            effective_args: args.clone(),
            replay: ToolReplay::Safe,
        },
    )
    .unwrap();
    let commit = SettlementCommit {
        current: current.clone(),
        plan: EffectPlan::ToolClearance(clear),
        output: SettlementOutput::ToolClearance(receipt),
        telemetry_context: "explicit-C",
    };
    let recipe = apply(
        settlement_recipe(
            &current,
            &commit,
            &inputs,
            &facts,
            &SettlementFacts::Tool {
                usage_id: None,
                declarations: BTreeMap::new(),
            },
        )
        .unwrap(),
    );
    assert_eq!(recipe.transaction.writes.len(), 2);
    let Write::SetRegister(register) = &recipe.transaction.writes[0] else {
        panic!()
    };
    assert!(
        matches!(register.as_ref(),RegisterSet::OperationToolArguments{value,..} if value==&args)
    );
    assert!(matches!(recipe.dispatch, Some(EffectPlan::Tool(_))));
    let mut newer = current.clone();
    newer.state = recipe.next.unwrap();
    assert_eq!(
        settlement_recipe(
            &newer,
            &commit,
            &inputs,
            &facts,
            &SettlementFacts::Tool {
                usage_id: None,
                declarations: BTreeMap::new()
            }
        )
        .unwrap(),
        RecipeDecision::NoLongerExpected
    );
}
#[test]
fn acceptance_rejects_empty_and_places_pending_atomically_preserving_uncaptured() {
    let (current, _, _) = fixture("run.checkpoint");
    let OperationState::Run(r) = current.state else {
        panic!()
    };
    let mut operation = current.operation;
    operation.source_leaf_id = None;
    if let OperationIntent::Run(intent) = &mut operation.intent {
        intent.prompt_entry_ids = vec![]
    };
    let mut facts = AcceptanceFacts {
        operation,
        lane_state: LaneState {
            current_operation_id: None,
            pending_next_run: vec![
                EntryId::new("captured").unwrap(),
                EntryId::new("late").unwrap(),
            ],
        },
        lane_seq: n(1),
        leaf_seq: Some(n(2)),
        configuration_seq: n(3),
        settings_revision: n(4),
        settings: r.settings,
        captured: vec![],
        messages: vec![],
        injections: vec![],
        skip_inbox_once: None,
        identities_present: true,
        telemetry_context: "explicit-C",
    };
    assert_eq!(
        acceptance_recipe(&facts).unwrap_err(),
        PlanError::InvalidInput
    );
    let message = serde_json::json!({"role":"user","content":"queued","timestamp":1});
    facts.captured.push(CapturedPending::new(
        serde_json::from_value(serde_json::json!({"id":"captured","parentId":null,"type":"message","message":message})).unwrap(),
        serde_json::from_value(serde_json::json!({"type":"message","payload":message})).unwrap(), n(5)).unwrap());
    let recipe = acceptance_recipe(&facts).unwrap();
    assert_eq!(recipe.transaction.writes.len(), 6);
    let Write::SetRegister(last) = recipe.transaction.writes.last().unwrap() else {
        panic!()
    };
    let RegisterSet::LaneState { value, .. } = last.as_ref() else {
        panic!()
    };
    assert_eq!(value.pending_next_run, vec![EntryId::new("late").unwrap()]);
    let mut mismatched = facts.captured[0].pending().clone();
    mismatched.payload = Presence::Null;
    assert!(CapturedPending::new(facts.captured[0].entry().clone(), mismatched, n(5)).is_err());
}
#[test]
fn terminal_failure_removes_owned_prefixes_and_preserves_lane_queue() {
    let (current, inputs, facts) = fixture("run.failure_drain.response");
    let result = operation_result(&current, &inputs).unwrap();
    let commit = TerminalCommit {
        current: current.clone(),
        result,
        telemetry_context: "explicit-C",
    };
    let cleanup = TerminalFacts {
        registers: vec![RegisterCondition {
            namespace: RegisterNamespace::OperationToolArguments,
            key: "op:old:99".into(),
            seq: Some(n(5)),
        }],
    };
    let recipe = apply(terminal_recipe(&current, &commit, &inputs, &facts, &cleanup).unwrap());
    assert_eq!(recipe.transaction.writes.len(), 5);
    let Write::SetRegister(last) = recipe.transaction.writes.last().unwrap() else {
        panic!()
    };
    let RegisterSet::LaneState { value, .. } = last.as_ref() else {
        panic!()
    };
    assert_eq!(value.current_operation_id, None);
    assert_eq!(value.pending_next_run, current.lane_state.pending_next_run);
    let mut bad = cleanup;
    bad.registers[0].namespace = RegisterNamespace::PendingEntry;
    bad.registers[0].key = "next".into();
    assert_eq!(
        terminal_recipe(&current, &commit, &inputs, &facts, &bad).unwrap_err(),
        PlanError::InvalidInput
    );
}
#[test]
fn cap_and_cancelled_not_started_reconcile_reserved_ids_without_dispatch() {
    let (mut current, inputs, facts) = fixture("run.assistant.effect_pending");
    let p = pending_plan(&current, &inputs, &facts);
    let mut synthetic = response("error").into_message();
    synthetic.usage = Usage::zero();
    synthetic.provider = p.generation.context.configuration.model.provider.clone();
    synthetic.model = p.generation.context.configuration.model.model_id.clone();
    let synthetic = SettledAssistantMessage::new(synthetic).unwrap();
    let planning = PlanningFacts::Generation {
        session_id: facts.session_id.clone(),
        telemetry_context: "explicit-C",
        synthetic: Some(Box::new(synthetic.clone())),
    };
    let ProcedureAction::Reconcile(commit) = next_action(&current, &inputs, &planning).unwrap()
    else {
        panic!()
    };
    let extra = SettlementFacts::Assistant {
        retryable: true,
        overflow: false,
        followers: vec![],
    };
    let recipe = apply(settlement_recipe(&current, &commit, &inputs, &facts, &extra).unwrap());
    assert!(recipe.dispatch.is_none());
    let Write::Usage(u) = &recipe.transaction.writes[2] else {
        panic!()
    };
    assert_eq!(u.id, p.generation.usage_id);
    assert_eq!(u.usage, Usage::zero());
    let OperationState::Run(r) = recipe.next.unwrap() else {
        panic!()
    };
    assert!(matches!(r.phase, RunPhase::FailureDrain(_)));
    if let OperationState::Run(r) = &mut current.state {
        r.control = Control::CancelRequested(CancelRequestedControl {
            status: Default::default(),
            requested_at: n(50),
            drained_steer: vec![],
            drained_follow_up: vec![],
        })
    }
    let commit = SettlementCommit {
        current: current.clone(),
        plan: EffectPlan::Assistant(p.clone()),
        output: SettlementOutput::NotStarted { key: p.key },
        telemetry_context: "explicit-C",
    };
    let recipe = apply(
        settlement_recipe(
            &current,
            &commit,
            &inputs,
            &facts,
            &SettlementFacts::NotStarted {
                value: ReconciledValue::Assistant(Box::new(synthetic)),
            },
        )
        .unwrap(),
    );
    let Write::Entry(NewEntry::Message(e)) = &recipe.transaction.writes[0] else {
        panic!()
    };
    let AgentMessage::BuiltIn(Message::Assistant(m)) = &e.message else {
        panic!()
    };
    assert_eq!(m.stop_reason, StopReason::Aborted);
}
#[test]
fn retry_error_keeps_captured_base_options_and_retry_wait_is_clock_driven() {
    let (mut current, inputs, facts) = fixture("run.assistant.effect_pending");
    if let OperationState::Run(r) = &mut current.state
        && let RunPhase::Assistant(AssistantPhase {
            generation: Generation::EffectPending { context, .. },
            ..
        }) = &mut r.phase
    {
        context.retry_policy.max_attempts = OneBasedSafeUnsigned::new(3).unwrap();
        context.retry_policy.base_delay_ms = n(10)
    }

    let mut p = pending_plan(&current, &inputs, &facts);
    p.stream_options.timeout_ms = Some(n(999));
    let base = p.generation.context.stream_options.clone();
    let commit = SettlementCommit {
        current: current.clone(),
        plan: EffectPlan::Assistant(p.clone()),
        output: SettlementOutput::Assistant {
            key: p.key,
            message: response("error"),
        },
        telemetry_context: "explicit-C",
    };
    let recipe = apply(
        settlement_recipe(
            &current,
            &commit,
            &inputs,
            &facts,
            &SettlementFacts::Assistant {
                retryable: true,
                overflow: false,
                followers: vec![],
            },
        )
        .unwrap(),
    );
    current.state = recipe.next.unwrap();
    let OperationState::Run(r) = &current.state else {
        panic!()
    };
    let RunPhase::Assistant(AssistantPhase {
        generation:
            Generation::RetryWait {
                context,
                next_attempt,
                not_before,
                ..
            },
        ..
    }) = &r.phase
    else {
        panic!()
    };
    assert_eq!(context.stream_options, base);
    assert_eq!(next_attempt.get(), 2);
    assert_eq!(*not_before, n(60));
    let planning = PlanningFacts::Generation {
        session_id: facts.session_id.clone(),
        telemetry_context: "explicit-C",
        synthetic: None,
    };
    assert!(matches!(
        next_action(&current, &inputs, &planning).unwrap(),
        ProcedureAction::Existing(Action::Wait(_))
    ));
    let mut elapsed = inputs;
    elapsed.now = n(60);
    assert!(matches!(
        next_action(&current, &elapsed, &planning).unwrap(),
        ProcedureAction::Existing(Action::Transition(_))
    ));
    assert_eq!(
        pi_harness_core::recovery::retry_deadline(n(1), n(1), OneBasedSafeUnsigned::MAX).unwrap(),
        SafeUnsigned::MAX
    );
}
#[test]
fn parallel_dispatches_all_but_settles_first_and_final_commit_deletes_all_args() {
    let (mut current, mut inputs, facts) = fixture("run.tools.planned");
    if let OperationState::Run(r) = &mut current.state {
        r.settings.tool_execution = ToolExecutionMode::Parallel
    }
    let planning = tool_planning(&facts, false);
    let mut plans = vec![];
    for index in [1, 3, 5] {
        let recipe = clear_one(&current, &inputs, &facts, &planning);
        let EffectPlan::Tool(p) = recipe.dispatch.clone().unwrap() else {
            panic!()
        };
        assert_eq!(p.source_index, n(index));
        publish(&mut current, &mut inputs, &recipe);
        inputs
            .running
            .insert(p.key.clone(), EffectPlan::Tool(p.clone()));
        plans.push(p);
    }
    assert!(
        matches!(next_action(&current,&inputs,&planning).unwrap(),ProcedureAction::Existing(Action::AwaitEffect(k)) if k==plans[0].key)
    );
    let make = |p: &ToolEffectPlan<&'static str>, current: &CurrentOperation| SettlementCommit {
        current: current.clone(),
        plan: EffectPlan::Tool(p.clone()),
        output: SettlementOutput::Tool(ToolSettlementOutput {
            key: p.key.clone(),
            result: tool_result(),
            is_error: false,
            terminate: true,
        }),
        telemetry_context: "explicit-C",
    };
    assert_eq!(
        settlement_recipe(
            &current,
            &make(&plans[2], &current),
            &inputs,
            &facts,
            &tool_extra(&planning)
        )
        .unwrap(),
        RecipeDecision::NoLongerExpected
    );
    for (i, p) in plans.iter().enumerate() {
        let recipe = apply(
            settlement_recipe(
                &current,
                &make(p, &current),
                &inputs,
                &facts,
                &tool_extra(&planning),
            )
            .unwrap(),
        );
        assert_eq!(
            recipe.publication,
            vec![Publication::Entry(
                EntryId::new(format!("result-{i}")).unwrap()
            )]
        );
        if i == 2 {
            assert_eq!(recipe.transaction.writes.iter().filter(|w|matches!(w,Write::DeleteRegister(r) if r.namespace==RegisterNamespace::OperationToolArguments)).count(),3)
        }
        publish(&mut current, &mut inputs, &recipe);
        inputs.running.remove(&p.key);
    }
    let OperationState::Run(r) = current.state else {
        panic!()
    };
    assert!(matches!(
        r.phase,
        RunPhase::Checkpoint(CheckpointPhase {
            continuation: Continuation::MayFinish {
                include_final_assistant: false
            },
            ..
        })
    ));
    assert!(inputs.loaded.keys().all(|k| !k.starts_with("op:step-1:")));
}
#[test]
fn any_sequential_declaration_blocks_later_clearance_and_saved_safe_replay_skips_it() {
    let (mut current, mut inputs, facts) = fixture("run.tools.planned");
    if let OperationState::Run(r) = &mut current.state {
        r.settings.tool_execution = ToolExecutionMode::Parallel
    }
    let planning = tool_planning(&facts, true);
    let recipe = clear_one(&current, &inputs, &facts, &planning);
    let p = recipe.dispatch.clone().unwrap();
    publish(&mut current, &mut inputs, &recipe);
    inputs.running.insert(p.key().clone(), p.clone());
    assert!(
        matches!(next_action(&current,&inputs,&planning).unwrap(),ProcedureAction::Existing(Action::AwaitEffect(k)) if &k==p.key())
    );
    inputs.running.clear();
    let ProcedureAction::Existing(Action::Dispatch(replay)) =
        next_action(&current, &inputs, &planning).unwrap()
    else {
        panic!()
    };
    assert!(matches!(replay.effect, EffectPlan::Tool(_)));
    assert_eq!(replay.effect.key(), p.key());
    let PlanningFacts::Tools { declarations, .. } = &planning else {
        panic!()
    };
    let mut unsafe_planning = planning.clone();
    if let PlanningFacts::Tools { declarations, .. } = &mut unsafe_planning {
        declarations.get_mut(&n(1)).unwrap().replay = ToolReplay::Never
    }
    let ProcedureAction::Reconcile(commit) =
        next_action(&current, &inputs, &unsafe_planning).unwrap()
    else {
        panic!()
    };
    assert_eq!(
        settlement_recipe(
            &current,
            &commit,
            &inputs,
            &facts,
            &SettlementFacts::Tool {
                usage_id: None,
                declarations: declarations.clone()
            }
        )
        .unwrap(),
        RecipeDecision::NoLongerExpected
    );
    let recipe = apply(
        settlement_recipe(
            &current,
            &commit,
            &inputs,
            &facts,
            &tool_extra(&unsafe_planning),
        )
        .unwrap(),
    );
    assert!(recipe.dispatch.is_none());
}
#[test]
fn before_run_end_terminal_receipt_carries_exact_cas_and_stale_hook_is_dropped() {
    let (current, inputs, facts) = fixture("run.checkpoint");
    let planning = PlanningFacts::Finish {
        session_id: facts.session_id.clone(),
        telemetry_context: "explicit-C",
        event: BeforeRunEndEvent {
            lane: current.operation.lane.clone(),
            run_id: current.operation.operation_id.clone(),
            messages: vec![],
        },
    };
    let ProcedureAction::Existing(Action::Dispatch(d)) =
        next_action(&current, &inputs, &planning).unwrap()
    else {
        panic!()
    };
    let EffectPlan::Hook(h) = d.effect else {
        panic!()
    };
    let o = HookEffectOutput::new(&h, HookResult::BeforeRunEnd(None)).unwrap();
    let commit = SettlementCommit {
        current: current.clone(),
        plan: EffectPlan::Hook(h),
        output: SettlementOutput::Hook(o),
        telemetry_context: "explicit-C",
    };
    let RecipeDecision::Terminal(terminal) = settlement_recipe(
        &current,
        &commit,
        &inputs,
        &facts,
        &SettlementFacts::RunEnd { follow_up: None },
    )
    .unwrap() else {
        panic!()
    };
    assert_eq!(terminal.current, current);
    let mut changed = current.clone();
    changed.operation_state_seq = n(999);
    assert_eq!(
        terminal_recipe(
            &changed,
            &terminal,
            &inputs,
            &facts,
            &TerminalFacts { registers: vec![] }
        )
        .unwrap(),
        RecipeDecision::NoLongerExpected
    );
    if let OperationState::Run(r) = &mut changed.state {
        r.inbox.follow_up.push(EntryId::new("new").unwrap())
    }
    assert_eq!(
        settlement_recipe(
            &changed,
            &commit,
            &inputs,
            &facts,
            &SettlementFacts::RunEnd { follow_up: None }
        )
        .unwrap(),
        RecipeDecision::NoLongerExpected
    );
}
#[test]
fn genuine_length_synthesizes_every_call_and_completed_calls_never_restart() {
    let (mut current, mut inputs, facts) = fixture("run.tools.planned");
    let OperationState::Run(r) = &current.state else {
        panic!()
    };
    let RunPhase::Tools(p) = &r.phase else {
        panic!()
    };
    let id = p.batch.assistant_entry_id.clone();
    let Some(PlannerLoadedValue::Entry(Entry::Message(entry))) = inputs.loaded.get_mut(id.as_str())
    else {
        panic!()
    };
    let AgentMessage::BuiltIn(Message::Assistant(message)) = &mut entry.message else {
        panic!()
    };
    message.stop_reason = StopReason::Length;
    let planning = tool_planning(&facts, false);
    for i in 0..3 {
        let ProcedureAction::Reconcile(commit) = next_action(&current, &inputs, &planning).unwrap()
        else {
            panic!("length must never dispatch")
        };
        let recipe = apply(
            settlement_recipe(&current, &commit, &inputs, &facts, &tool_extra(&planning)).unwrap(),
        );
        let Write::Entry(NewEntry::Message(entry)) = &recipe.transaction.writes[0] else {
            panic!()
        };
        assert_eq!(entry.id.as_str(), format!("result-{i}"));
        let AgentMessage::BuiltIn(Message::ToolResult(m)) = &entry.message else {
            panic!()
        };
        assert!(m.is_error);
        assert!(recipe.dispatch.is_none());
        assert!(!recipe.transaction.writes.iter().any(|w|matches!(w,Write::SetRegister(r) if matches!(r.as_ref(),RegisterSet::OperationToolArguments{..}))));
        publish(&mut current, &mut inputs, &recipe);
        assert_eq!(
            settlement_recipe(&current, &commit, &inputs, &facts, &tool_extra(&planning)).unwrap(),
            RecipeDecision::NoLongerExpected
        );
    }
    let OperationState::Run(r) = current.state else {
        panic!()
    };
    assert!(matches!(
        r.phase,
        RunPhase::Checkpoint(CheckpointPhase {
            continuation: Continuation::NeedAssistant { .. },
            ..
        })
    ));
}
#[test]
fn live_hook_is_awaited_and_wrong_running_family_is_rejected() {
    let (current, mut inputs, facts) = fixture("run.assistant.ready");
    let planning = PlanningFacts::BeforeRequest {
        session_id: facts.session_id.clone(),
        telemetry_context: "explicit-C",
        event: Box::new(model_event(&current)),
    };
    let ProcedureAction::Existing(Action::Dispatch(d)) =
        next_action(&current, &inputs, &planning).unwrap()
    else {
        panic!()
    };
    inputs
        .running
        .insert(d.effect.key().clone(), d.effect.clone());
    assert!(matches!(
        next_action(&current, &inputs, &planning).unwrap(),
        ProcedureAction::Existing(Action::AwaitEffect(_))
    ));
    let key = d.effect.key().clone();
    inputs.running.insert(
        key.clone(),
        EffectPlan::Tool(ToolEffectPlan {
            key,
            assistant_entry_id: EntryId::new("wrong").unwrap(),
            source_index: n(0),
            args_key: "wrong".into(),
            telemetry_context: "explicit-C",
        }),
    );
    assert_eq!(
        next_action(&current, &inputs, &planning).unwrap_err(),
        PlanError::InvalidInput
    );
}
#[test]
fn deferred_initial_suspension_needs_valid_handle_and_keeps_base_options() {
    let (current, mut inputs, facts) = fixture("run.assistant.effect_pending");
    let p = pending_plan(&current, &inputs, &facts);
    let (deferred_current, deferred_inputs, _) = fixture("run.deferred.suspended");
    let OperationState::Run(r) = deferred_current.state else {
        panic!()
    };
    let RunPhase::Deferred(DeferredPhase {
        deferred: Deferred::Suspended {
            source_entry_id, ..
        },
        ..
    }) = r.phase
    else {
        panic!()
    };
    let PlannerLoadedValue::Entry(Entry::Message(e)) = deferred_inputs
        .loaded
        .get(source_entry_id.as_str())
        .unwrap()
    else {
        panic!()
    };
    let AgentMessage::BuiltIn(Message::Assistant(m)) = &e.message else {
        panic!()
    };
    let response = SettledAssistantMessage::new(*m.clone()).unwrap();
    let commit = SettlementCommit {
        current: current.clone(),
        plan: EffectPlan::Assistant(p.clone()),
        output: SettlementOutput::Assistant {
            key: p.key,
            message: response,
        },
        telemetry_context: "explicit-C",
    };
    let recipe = apply(
        settlement_recipe(
            &current,
            &commit,
            &inputs,
            &facts,
            &SettlementFacts::Assistant {
                retryable: false,
                overflow: false,
                followers: vec![],
            },
        )
        .unwrap(),
    );
    let mut changed = current;
    publish(&mut changed, &mut inputs, &recipe);
    assert!(matches!(
        next_action(&changed, &inputs, &PlanningFacts::Terminal).unwrap(),
        ProcedureAction::Existing(Action::Suspend(OperationResult::Run(_)))
    ));
}
#[test]
fn assistant_call_followers_preserve_original_positions_and_reject_incomplete_plan() {
    let (current, inputs, facts) = fixture("run.assistant.effect_pending");
    let p = pending_plan(&current, &inputs, &facts);
    let (_, tool_inputs, _) = fixture("run.tools.planned");
    let PlannerLoadedValue::Entry(Entry::Message(entry)) =
        tool_inputs.loaded.get("entry-response").unwrap()
    else {
        panic!()
    };
    let AgentMessage::BuiltIn(Message::Assistant(m)) = &entry.message else {
        panic!()
    };
    let commit = SettlementCommit {
        current: current.clone(),
        plan: EffectPlan::Assistant(p.clone()),
        output: SettlementOutput::Assistant {
            key: p.key,
            message: SettledAssistantMessage::new(*m.clone()).unwrap(),
        },
        telemetry_context: "explicit-C",
    };
    let followers = vec![
        (n(1), EntryId::new("follower-1").unwrap()),
        (n(3), EntryId::new("follower-3").unwrap()),
        (n(5), EntryId::new("follower-5").unwrap()),
    ];
    let recipe = apply(
        settlement_recipe(
            &current,
            &commit,
            &inputs,
            &facts,
            &SettlementFacts::Assistant {
                retryable: false,
                overflow: false,
                followers: followers.clone(),
            },
        )
        .unwrap(),
    );
    let OperationState::Run(r) = recipe.next.unwrap() else {
        panic!()
    };
    let RunPhase::Tools(p) = r.phase else {
        panic!()
    };
    assert_eq!(
        p.batch
            .calls
            .iter()
            .map(|c| pi_harness_core::tool_batch::source(c).0)
            .collect::<Vec<_>>(),
        vec![n(1), n(3), n(5)]
    );
    for wrong in [
        followers[..2].to_vec(),
        vec![
            (n(0), followers[0].1.clone()),
            followers[1].clone(),
            followers[2].clone(),
        ],
    ] {
        assert_eq!(
            settlement_recipe(
                &current,
                &commit,
                &inputs,
                &facts,
                &SettlementFacts::Assistant {
                    retryable: false,
                    overflow: false,
                    followers: wrong
                }
            )
            .unwrap_err(),
            PlanError::InvalidInput
        )
    }
}
#[test]
fn unfinished_parallel_clearance_cannot_overtake_source_order() {
    let (mut current, mut inputs, facts) = fixture("run.tools.planned");
    if let OperationState::Run(r) = &mut current.state {
        r.settings.tool_execution = ToolExecutionMode::Parallel
    }
    let planning = tool_planning(&facts, false);
    let recipe = clear_one(&current, &inputs, &facts, &planning);
    let first = recipe.dispatch.clone().unwrap();
    publish(&mut current, &mut inputs, &recipe);
    inputs.running.insert(first.key().clone(), first.clone());
    let ProcedureAction::Existing(Action::Dispatch(second)) =
        next_action(&current, &inputs, &planning).unwrap()
    else {
        panic!()
    };
    assert_eq!(second.effect.key().item_index, Some(n(3)));
    inputs
        .running
        .insert(second.effect.key().clone(), second.effect);
    assert!(
        matches!(next_action(&current,&inputs,&planning).unwrap(),ProcedureAction::Existing(Action::AwaitEffect(k)) if &k==first.key())
    );
}
#[test]
fn before_run_end_follow_up_is_born_placed_and_rejects_wrong_text() {
    let (current, inputs, facts) = fixture("run.checkpoint");
    let planning = PlanningFacts::Finish {
        session_id: facts.session_id.clone(),
        telemetry_context: "explicit-C",
        event: BeforeRunEndEvent {
            lane: current.operation.lane.clone(),
            run_id: current.operation.operation_id.clone(),
            messages: vec![],
        },
    };
    let ProcedureAction::Existing(Action::Dispatch(d)) =
        next_action(&current, &inputs, &planning).unwrap()
    else {
        panic!()
    };
    let EffectPlan::Hook(h) = d.effect else {
        panic!()
    };
    let receipt = HookEffectOutput::new(
        &h,
        HookResult::BeforeRunEnd(Some(BeforeRunEndResult {
            follow_up: Some("continue".into()),
        })),
    )
    .unwrap();
    let commit = SettlementCommit {
        current: current.clone(),
        plan: EffectPlan::Hook(h),
        output: SettlementOutput::Hook(receipt),
        telemetry_context: "explicit-C",
    };
    let entry:NewMessageEntry=serde_json::from_value(serde_json::json!({"id":"follow-up","parentId":"leaf","type":"message","message":{"role":"user","content":"continue","timestamp":50}})).unwrap();
    let recipe = apply(
        settlement_recipe(
            &current,
            &commit,
            &inputs,
            &facts,
            &SettlementFacts::RunEnd {
                follow_up: Some(entry.clone()),
            },
        )
        .unwrap(),
    );
    assert_eq!(recipe.transaction.writes.len(), 3);
    let OperationState::Run(r) = recipe.next.unwrap() else {
        panic!()
    };
    assert!(matches!(
        r.phase,
        RunPhase::Checkpoint(CheckpointPhase {
            continuation: Continuation::NeedAssistant { .. },
            ..
        })
    ));
    let mut wrong = entry;
    let AgentMessage::BuiltIn(Message::User(u)) = &mut wrong.message else {
        panic!()
    };
    u.content = UserContent::Text("wrong".into());
    assert_eq!(
        settlement_recipe(
            &current,
            &commit,
            &inputs,
            &facts,
            &SettlementFacts::RunEnd {
                follow_up: Some(wrong)
            }
        )
        .unwrap_err(),
        PlanError::InvalidInput
    );
}
#[test]
fn tool_usage_is_committed_with_its_result_and_requires_explicit_id() {
    let (mut current, mut inputs, facts) = fixture("run.tools.planned");
    let planning = tool_planning(&facts, false);
    let recipe = clear_one(&current, &inputs, &facts, &planning);
    let EffectPlan::Tool(p) = recipe.dispatch.clone().unwrap() else {
        panic!()
    };
    publish(&mut current, &mut inputs, &recipe);
    let mut result = tool_result();
    result.usage = Some(Usage::zero());
    let commit = SettlementCommit {
        current: current.clone(),
        plan: EffectPlan::Tool(p.clone()),
        output: SettlementOutput::Tool(ToolSettlementOutput {
            key: p.key,
            result,
            is_error: false,
            terminate: false,
        }),
        telemetry_context: "explicit-C",
    };
    let mut extra = tool_extra(&planning);
    assert_eq!(
        settlement_recipe(&current, &commit, &inputs, &facts, &extra).unwrap_err(),
        PlanError::InvalidInput
    );
    if let SettlementFacts::Tool { usage_id, .. } = &mut extra {
        *usage_id = Some(UsageId::new("tool-U").unwrap())
    };
    let recipe = apply(settlement_recipe(&current, &commit, &inputs, &facts, &extra).unwrap());
    assert_eq!(recipe.transaction.writes.len(), 4);
    let Write::Usage(u) = &recipe.transaction.writes[2] else {
        panic!()
    };
    assert_eq!(u.id.as_str(), "tool-U");
    assert_eq!(u.entry_id.as_ref().unwrap().as_str(), "result-0");
    assert_eq!(recipe.publication.len(), 2);
}
#[test]
fn restored_empty_or_completed_batch_normalizes_without_reexecuting_tools() {
    for empty in [true, false] {
        let (mut current, mut inputs, facts) = fixture("run.tools.planned");
        let OperationState::Run(r) = &mut current.state else {
            panic!()
        };
        let RunPhase::Tools(p) = &mut r.phase else {
            panic!()
        };
        if empty {
            p.batch.calls.clear();
            let Some(PlannerLoadedValue::Entry(Entry::Message(e))) =
                inputs.loaded.get_mut(p.batch.assistant_entry_id.as_str())
            else {
                panic!()
            };
            let AgentMessage::BuiltIn(Message::Assistant(m)) = &mut e.message else {
                panic!()
            };
            m.content.clear()
        } else {
            for c in &mut p.batch.calls {
                let (index, id) = pi_harness_core::tool_batch::source(c);
                *c = ToolCallState::Completed {
                    source_index: index,
                    result_entry_id: id.clone(),
                    terminate: true,
                }
            }
        }
        let planning = tool_planning(&facts, false);
        let ProcedureAction::Existing(Action::Transition(t)) =
            next_action(&current, &inputs, &planning).unwrap()
        else {
            panic!()
        };
        let commit = TransitionCommit {
            current: current.clone(),
            next: t.next,
            telemetry_context: t.telemetry_context,
            expected_configuration_seq: t.expected_configuration_seq,
            expected_settings_revision: t.expected_settings_revision,
        };
        let recipe =
            apply(transition_recipe(&current, &commit, &inputs, &facts, &planning).unwrap());
        assert!(recipe.dispatch.is_none());
        assert_eq!(recipe.transaction.writes.len(), if empty { 1 } else { 4 });
        let OperationState::Run(r) = recipe.next.unwrap() else {
            panic!()
        };
        let RunPhase::Checkpoint(p) = r.phase else {
            panic!()
        };
        assert_eq!(
            p.continuation,
            if empty {
                Continuation::NeedAssistant {
                    overflow_recovery_used: false,
                }
            } else {
                Continuation::MayFinish {
                    include_final_assistant: false,
                }
            }
        );
    }
}

fn assert_cleanup_conditions(
    recipe: &CommitRecipe<&'static str>,
    observed: &[(u64, Option<SafeUnsigned>)],
) {
    let mut seen = std::collections::BTreeSet::new();
    for condition in &recipe.expected {
        assert!(
            seen.insert((condition.namespace, &condition.key)),
            "duplicate conditional-register identity: {condition:?}"
        );
    }
    let deleted: Vec<_> = recipe
        .transaction
        .writes
        .iter()
        .filter_map(|write| match write {
            Write::DeleteRegister(r)
                if r.namespace == RegisterNamespace::OperationToolArguments =>
            {
                Some(r.key.as_str())
            }
            _ => None,
        })
        .collect();
    let keys: Vec<_> = observed
        .iter()
        .map(|(index, _)| format!("op:step-1:{index}"))
        .collect();
    assert_eq!(
        deleted,
        keys.iter().map(String::as_str).collect::<Vec<_>>(),
        "cleanup remains source ordered"
    );
    for ((_, expected_seq), key) in observed.iter().zip(keys) {
        let condition = recipe
            .expected
            .iter()
            .find(|c| c.namespace == RegisterNamespace::OperationToolArguments && c.key == key)
            .expect("every deleted args key requires an observed Seq/Absent predicate");
        assert_eq!(condition.seq, *expected_seq);
    }
}

#[test]
fn final_tool_cleanup_has_unique_seq_and_absent_predicates() {
    for immediate in [false, true] {
        let (mut current, mut inputs, facts) = fixture("run.tools.planned");
        let planning = tool_planning(&facts, false);
        for _ in 0..2 {
            let clearance = clear_one(&current, &inputs, &facts, &planning);
            let plan = clearance.dispatch.clone().unwrap();
            publish(&mut current, &mut inputs, &clearance);
            let commit = SettlementCommit {
                current: current.clone(),
                plan: plan.clone(),
                output: SettlementOutput::Tool(ToolSettlementOutput {
                    key: plan.key().clone(),
                    result: tool_result(),
                    is_error: false,
                    terminate: false,
                }),
                telemetry_context: "explicit-C",
            };
            let recipe = apply(
                settlement_recipe(&current, &commit, &inputs, &facts, &tool_extra(&planning))
                    .unwrap(),
            );
            publish(&mut current, &mut inputs, &recipe);
        }
        let commit = if immediate {
            let ProcedureAction::Existing(Action::Dispatch(d)) =
                next_action(&current, &inputs, &planning).unwrap()
            else {
                panic!()
            };
            let EffectPlan::ToolClearance(plan) = d.effect else {
                panic!()
            };
            let output = ToolClearanceOutput::new(
                &plan,
                ToolClearanceOutcome::Immediate {
                    result: tool_result(),
                    is_error: true,
                    terminate: false,
                },
            )
            .unwrap();
            SettlementCommit {
                current: current.clone(),
                plan: EffectPlan::ToolClearance(plan),
                output: SettlementOutput::ToolClearance(output),
                telemetry_context: "explicit-C",
            }
        } else {
            let clearance = clear_one(&current, &inputs, &facts, &planning);
            let plan = clearance.dispatch.clone().unwrap();
            publish(&mut current, &mut inputs, &clearance);
            SettlementCommit {
                current: current.clone(),
                plan: plan.clone(),
                output: SettlementOutput::Tool(ToolSettlementOutput {
                    key: plan.key().clone(),
                    result: tool_result(),
                    is_error: false,
                    terminate: false,
                }),
                telemetry_context: "explicit-C",
            }
        };
        let recipe = apply(
            settlement_recipe(&current, &commit, &inputs, &facts, &tool_extra(&planning)).unwrap(),
        );
        assert_cleanup_conditions(
            &recipe,
            &[
                (1, Some(n(100))),
                (3, Some(n(100))),
                (5, if immediate { None } else { Some(n(100)) }),
            ],
        );
    }
}

#[test]
fn restored_completed_cleanup_checks_absence_and_rejects_mismatched_hydration() {
    let (mut current, mut inputs, facts) = fixture("run.tools.planned");
    let OperationState::Run(run) = &mut current.state else {
        panic!()
    };
    let RunPhase::Tools(phase) = &mut run.phase else {
        panic!()
    };
    for call in &mut phase.batch.calls {
        let (index, id) = pi_harness_core::tool_batch::source(call);
        *call = ToolCallState::Completed {
            source_index: index,
            result_entry_id: id.clone(),
            terminate: false,
        };
    }
    for (index, seq) in [(1, 17), (5, 19)] {
        let key = format!("op:step-1:{index}");
        inputs.loaded.insert(
            key.clone(),
            PlannerLoadedValue::Register(Register::OperationToolArguments {
                key,
                value: JsonObject::default(),
                seq: n(seq),
            }),
        );
    }
    inputs.loaded.remove("op:step-1:3");
    let planning = tool_planning(&facts, false);
    let ProcedureAction::Existing(Action::Transition(plan)) =
        next_action(&current, &inputs, &planning).unwrap()
    else {
        panic!()
    };
    let commit = TransitionCommit {
        current: current.clone(),
        next: plan.next,
        telemetry_context: plan.telemetry_context,
        expected_configuration_seq: plan.expected_configuration_seq,
        expected_settings_revision: plan.expected_settings_revision,
    };
    let recipe = apply(transition_recipe(&current, &commit, &inputs, &facts, &planning).unwrap());
    assert_cleanup_conditions(&recipe, &[(1, Some(n(17))), (3, None), (5, Some(n(19)))]);
    inputs.loaded.insert(
        "op:step-1:3".into(),
        PlannerLoadedValue::Register(Register::OperationToolArguments {
            key: "different".into(),
            value: JsonObject::default(),
            seq: n(20),
        }),
    );
    assert_eq!(
        transition_recipe(&current, &commit, &inputs, &facts, &planning).unwrap_err(),
        PlanError::InvalidInput
    );
}

fn role_acceptance_facts(message: AgentMessage, injection: bool) -> AcceptanceFacts<&'static str> {
    let (current, _, _) = fixture("run.checkpoint");
    let OperationState::Run(run) = current.state else {
        panic!()
    };
    let mut operation = current.operation;
    operation.source_leaf_id = None;
    let id = EntryId::new("accepted-message").unwrap();
    let OperationIntent::Run(intent) = &mut operation.intent else {
        panic!()
    };
    intent.prompt_entry_ids = if injection { vec![] } else { vec![id.clone()] };
    let entry = NewMessageEntry {
        id,
        parent_id: None,
        entry_type: Default::default(),
        message,
        terminate: None,
    };
    AcceptanceFacts {
        operation,
        lane_state: LaneState {
            current_operation_id: None,
            pending_next_run: vec![EntryId::new("next").unwrap()],
        },
        lane_seq: n(1),
        leaf_seq: Some(n(2)),
        configuration_seq: n(3),
        settings_revision: n(4),
        settings: run.settings,
        captured: vec![],
        messages: if injection {
            vec![]
        } else {
            vec![entry.clone()]
        },
        injections: if injection { vec![entry] } else { vec![] },
        skip_inbox_once: None,
        identities_present: true,
        telemetry_context: "explicit-C",
    }
}

#[test]
fn acceptance_preserves_every_agent_message_role_for_callers_and_injections() {
    let roles: Vec<AgentMessage> = vec![
        serde_json::from_value(serde_json::json!({"role":"user","content":"user","timestamp":1})).unwrap(),
        serde_json::from_value(serde_json::json!({"role":"note","text":"custom"})).unwrap(),
        AgentMessage::BuiltIn(Message::Assistant(Box::new(response("stop").into_message()))),
        serde_json::from_value(serde_json::json!({"role":"toolResult","toolCallId":"call","toolName":"tool","content":[],"isError":false,"timestamp":1})).unwrap(),
        AgentMessage::BuiltIn(Message::Assistant(Box::new(response("error").into_message()))),
        AgentMessage::BuiltIn(Message::Assistant(Box::new(response("aborted").into_message()))),
    ];
    for message in roles {
        for injection in [false, true] {
            let facts = role_acceptance_facts(message.clone(), injection);
            let recipe = acceptance_recipe(&facts).unwrap();
            let Write::Entry(NewEntry::Message(entry)) = &recipe.transaction.writes[0] else {
                panic!()
            };
            assert_eq!(
                entry.message, message,
                "acceptance must preserve valid content without role rewriting"
            );
            assert_eq!(entry.parent_id, None);
            assert_eq!(entry.terminate, None);
            assert_eq!(recipe.transaction.writes.len(), 5);
            let Write::SetRegister(meta) = &recipe.transaction.writes[2] else {
                panic!()
            };
            let RegisterSet::OperationMeta { value, .. } = meta.as_ref() else {
                panic!()
            };
            let OperationIntent::Run(intent) = &value.intent else {
                panic!()
            };
            assert_eq!(
                intent.prompt_entry_ids,
                if injection {
                    vec![]
                } else {
                    vec![entry.id.clone()]
                }
            );
            assert_eq!(
                recipe.publication,
                vec![Publication::Entry(entry.id.clone())]
            );
            assert_eq!(recipe.expected_settings_revision, Some(n(4)));
            assert!(
                recipe
                    .expected
                    .iter()
                    .any(|c| c.namespace == RegisterNamespace::LaneState && c.seq == Some(n(1)))
            );
            let Write::SetRegister(lane) = recipe.transaction.writes.last().unwrap() else {
                panic!()
            };
            let RegisterSet::LaneState { value, .. } = lane.as_ref() else {
                panic!()
            };
            assert_eq!(value.pending_next_run, facts.lane_state.pending_next_run);
        }
    }
}

#[test]
fn acceptance_rejects_unsettled_assistants_and_retains_metadata_and_chain_guards() {
    for injection in [false, true] {
        for stop in [StopReason::Pending, StopReason::Error, StopReason::Aborted] {
            let mut message = response("stop").into_message();
            message.stop_reason = stop;
            message.error_message = None;
            let facts = role_acceptance_facts(
                AgentMessage::BuiltIn(Message::Assistant(Box::new(message))),
                injection,
            );
            assert_eq!(
                acceptance_recipe(&facts).unwrap_err(),
                PlanError::InvalidInput,
                "invalid assistant must not become an acceptance recipe"
            );
        }
        let message = AgentMessage::BuiltIn(Message::Assistant(Box::new(
            response("stop").into_message(),
        )));
        let base = role_acceptance_facts(message, injection);
        for defect in 0..4 {
            let mut facts = base.clone();
            let entries = if injection {
                &mut facts.injections
            } else {
                &mut facts.messages
            };
            match defect {
                0 => entries[0].parent_id = Some(EntryId::new("wrong-parent").unwrap()),
                1 => entries[0].terminate = Some(LiteralTrue),
                2 => {
                    let mut duplicate = entries[0].clone();
                    duplicate.parent_id = Some(duplicate.id.clone());
                    entries.push(duplicate);
                }
                _ => {
                    let OperationIntent::Run(intent) = &mut facts.operation.intent else {
                        panic!()
                    };
                    intent.prompt_entry_ids =
                        vec![EntryId::new("wrong-prompt-membership").unwrap()];
                }
            }
            assert_eq!(
                acceptance_recipe(&facts).unwrap_err(),
                PlanError::InvalidInput
            );
        }
    }
}
