mod r8_r10_core_support;
use pi_agent_schema::*;
use pi_agent_session::{backend_support::register_seq, storage::*};
use pi_harness_core::{planner::*, structural, terminal::TerminalFacts};
use r8_r10_core_support::*;
use std::sync::atomic::Ordering;
#[test]
fn actual_nested_usage_rejects_wrong_reserved_id_and_stale_state_atomically() {
    let s = Store::new();
    s.accept();
    let outer = s.pending();
    let (c, _, f) = s.current();
    let req = SummaryRequestPlan {
        task_id: outer.generation.context.task_id.clone(),
        attempt: outer.generation.attempt,
        request_index: n(0),
        usage_id: UsageId::new("usage0").unwrap(),
        configuration: c.configuration.clone(),
        messages: vec![],
        telemetry_context: (),
    };
    assert!(matches!(
        s.publish(apply(
            structural::nested_intent_recipe(&c, &outer, &req, &f).unwrap()
        )),
        ConditionalOutcome::Applied(_)
    ));
    let (c, _, f) = s.current();
    let before = ready(s.storage.snapshot()).unwrap();
    let mut settle = SummaryRequestSettlement {
        current: c.clone(),
        task_id: req.task_id,
        attempt: req.attempt,
        request_index: req.request_index,
        usage_id: UsageId::new("wrong-usage").unwrap(),
        response: response(),
        telemetry_context: (),
    };
    let decision = structural::settle_request_recipe(&c, &outer, &settle, &f).unwrap();
    if let RecipeDecision::Apply(r) = decision {
        let _ = s.publish(*r);
    }
    assert_eq!(
        ready(s.storage.snapshot()).unwrap(),
        before,
        "wrong nested usage identity must publish nothing"
    );
    settle.usage_id = UsageId::new("usage0").unwrap();
    let good = apply(structural::settle_request_recipe(&c, &outer, &settle, &f).unwrap());
    assert!(matches!(
        s.publish(good.clone()),
        ConditionalOutcome::Applied(_)
    ));
    let settled = ready(s.storage.snapshot()).unwrap();
    assert!(matches!(s.publish(good), ConditionalOutcome::NotApplied));
    assert_eq!(ready(s.storage.snapshot()).unwrap(), settled);
    assert_eq!(settled.usage.len(), 1);
    assert_eq!(settled.usage[0].id.as_str(), "usage0");
    assert_eq!(settled.usage[0].usage, response().as_message().usage);
}
#[test]
fn actual_prepared_hook_terminal_uses_all_write_ordinal_and_reprepares_after_drift() {
    let s = Store::new();
    s.accept();
    let (c, _, f) = s.current();
    let semantic = CompactResult {
        summary: "kept".into(),
        tokens_before: n(5),
        retained_tail: vec![],
        usage: Some(response().as_message().usage.clone()),
        details: pi_ai_core::Presence::Missing,
    };
    let new = NewEntry::Compaction(NewCompactionEntry {
        id: id("summary"),
        parent_id: Some(id("leaf")),
        entry_type: Default::default(),
        summary: semantic.summary.clone(),
        retained_tail: vec![],
        tokens_before: n(5),
        details: pi_ai_core::Presence::Missing,
        usage: semantic.usage.clone(),
        from_hook: true,
    });
    let stamp = ready(s.session.read_stamp()).unwrap();
    let prepared = ready(s.session.prepare_entries(
        stamp,
        vec![PreparedEntrySlot {
            write_index: 1,
            entry: new.clone(),
        }],
    ))
    .unwrap()
    .unwrap();
    let first = prepared.entries()[0].clone();
    let result = structural::completed_result(
        &c,
        &SummarySuccessResult::Compact(semantic.clone()),
        &first,
        true,
    )
    .unwrap();
    let commit = TerminalCommit {
        current: c.clone(),
        result,
        telemetry_context: (),
    };
    let extra = structural::Settlement {
        prepared_entry: Some(first.clone()),
        hook_usage_id: Some(UsageId::new("hook-cost").unwrap()),
        ..Default::default()
    };
    let cleanup = TerminalFacts {
        registers: vec![pi_harness_core::planner::RegisterCondition {
            namespace: RegisterNamespace::OperationPreparation,
            key: "op:task".into(),
            seq: Some(register_seq(
                &s.register(RegisterNamespace::OperationPreparation, "op:task"),
            )),
        }],
    };
    let r = apply(structural::terminal_recipe(&c, &commit, &f, &cleanup, &extra).unwrap());
    assert!(matches!(
        &r.transaction.writes[..3],
        [Write::Usage(_), Write::Entry(_), Write::SetRegister(_)]
    ));
    ready(s.session.commit(Transaction {
        writes: vec![set(RegisterSet::FactName {
            key: "".into(),
            value: "other mutation".into(),
        })],
    }))
    .unwrap();
    s.time.0.store(200, Ordering::SeqCst);
    let before = ready(s.storage.snapshot()).unwrap();
    assert!(matches!(
        ready(s.session.commit_prepared_conditional_with_telemetry(
            prepared,
            request(r),
            &pi_agent_session::backend_support::NoopTelemetryContext
        ))
        .unwrap(),
        ConditionalOutcome::NotApplied
    ));
    assert_eq!(ready(s.storage.snapshot()).unwrap(), before);
    let prepared = ready(s.session.prepare_entries(
        ready(s.session.read_stamp()).unwrap(),
        vec![PreparedEntrySlot {
            write_index: 1,
            entry: new,
        }],
    ))
    .unwrap()
    .unwrap();
    let final_entry = prepared.entries()[0].clone();
    assert_ne!(first, final_entry);
    let result = structural::completed_result(
        &c,
        &SummarySuccessResult::Compact(semantic),
        &final_entry,
        true,
    )
    .unwrap();
    let commit = TerminalCommit {
        result: result.clone(),
        ..commit
    };
    let extra = structural::Settlement {
        prepared_entry: Some(final_entry.clone()),
        ..extra
    };
    let r = apply(structural::terminal_recipe(&c, &commit, &f, &cleanup, &extra).unwrap());
    assert!(matches!(
        ready(s.session.commit_prepared_conditional_with_telemetry(
            prepared,
            request(r),
            &pi_agent_session::backend_support::NoopTelemetryContext
        ))
        .unwrap(),
        ConditionalOutcome::Applied(_)
    ));
    let actual = ready(s.session.get_entries(&[id("summary")]))
        .unwrap()
        .remove(&id("summary"))
        .unwrap();
    assert_eq!(actual, final_entry);
    let OperationResult::Compaction(CompactionOutcome::Completed(public)) = result else {
        panic!()
    };
    assert_eq!(Entry::Compaction(public.entry), actual);
    assert!(
        ready(
            s.session
                .get_register(RegisterNamespace::OperationMeta, "op")
        )
        .unwrap()
        .is_none()
    );
    assert!(
        ready(
            s.session
                .get_register(RegisterNamespace::OperationPreparation, "op:task")
        )
        .unwrap()
        .is_none()
    );
    let ledger = ready(s.storage.snapshot()).unwrap().usage;
    assert_eq!(ledger.len(), 1);
    assert_eq!(
        ledger[0].entry_id, None,
        "structural hook usage precedes its entry and must not name a future row"
    );
    assert_eq!(ledger[0].usage, response().as_message().usage);
    assert!(!ledger[0].adjustment);
}
#[test]
fn actual_generated_navigation_atomically_moves_labels_and_cleans_without_usage_duplication() {
    let s = Store::new();
    s.accept_navigation();
    let outer = s.pending();
    let (c, _, f) = s.current();
    let semantic = BranchSummaryResult {
        summary: "branch".into(),
        usage: None,
        read_files: vec![],
        modified_files: vec![],
    };
    let new = NewEntry::BranchSummary(NewBranchSummaryEntry {
        id: id("summary"),
        parent_id: Some(id("target")),
        entry_type: Default::default(),
        from_id: id("leaf"),
        summary: "branch".into(),
        details: pi_ai_core::Presence::Value(
            serde_json::from_value(serde_json::json!({"readFiles":[],"modifiedFiles":[]})).unwrap(),
        ),
        usage: None,
        from_hook: false,
    });
    let prepared = ready(s.session.prepare_entries(
        ready(s.session.read_stamp()).unwrap(),
        vec![PreparedEntrySlot {
            write_index: 1,
            entry: new,
        }],
    ))
    .unwrap()
    .unwrap();
    let full = prepared.entries()[0].clone();
    let result = structural::completed_result(
        &c,
        &SummarySuccessResult::BranchSummary(semantic),
        &full,
        false,
    )
    .unwrap();
    let cleanup = TerminalFacts {
        registers: vec![pi_harness_core::planner::RegisterCondition {
            namespace: RegisterNamespace::OperationPreparation,
            key: "op:task".into(),
            seq: Some(register_seq(
                &s.register(RegisterNamespace::OperationPreparation, "op:task"),
            )),
        }],
    };
    let r = apply(
        structural::terminal_recipe(
            &c,
            &TerminalCommit {
                current: c.clone(),
                result: result.clone(),
                telemetry_context: (),
            },
            &f,
            &cleanup,
            &structural::Settlement {
                prepared_entry: Some(full.clone()),
                ..Default::default()
            },
        )
        .unwrap(),
    );
    assert!(
        matches!(&r.transaction.writes[..4],[Write::SetRegister(a),Write::Entry(_),Write::SetRegister(b),Write::SetRegister(label)] if matches!(a.as_ref(),RegisterSet::LaneLeaf{value:Some(id),..} if id.as_str()=="target")&&matches!(b.as_ref(),RegisterSet::LaneLeaf{value:Some(id),..} if id.as_str()=="summary")&&matches!(label.as_ref(),RegisterSet::FactLabel{key,..} if key=="target"))
    );
    assert!(matches!(
        ready(s.session.commit_prepared_conditional_with_telemetry(
            prepared,
            request(r),
            &pi_agent_session::backend_support::NoopTelemetryContext
        ))
        .unwrap(),
        ConditionalOutcome::Applied(_)
    ));
    assert!(ready(s.storage.snapshot()).unwrap().usage.is_empty());
    assert_eq!(
        ready(s.session.get_entries(&[id("summary")])).unwrap()[&id("summary")],
        full
    );
    assert!(
        matches!(s.register(RegisterNamespace::LaneLeaf,"main"),Register::LaneLeaf{value:Some(v),..} if v.as_str()=="summary")
    );
    let _ = outer;
}
#[test]
fn prepared_navigation_with_wrong_all_write_ordinal_refuses_the_complete_transaction() {
    let s = Store::new();
    s.accept_navigation();
    s.pending();
    let (c, _, f) = s.current();
    let semantic = BranchSummaryResult {
        summary: "branch".into(),
        usage: None,
        read_files: vec![],
        modified_files: vec![],
    };
    let entry = NewEntry::BranchSummary(NewBranchSummaryEntry {
        id: id("summary"),
        parent_id: Some(id("target")),
        entry_type: Default::default(),
        from_id: id("leaf"),
        summary: "branch".into(),
        details: pi_ai_core::Presence::Value(
            serde_json::from_value(serde_json::json!({"readFiles":[],"modifiedFiles":[]})).unwrap(),
        ),
        usage: None,
        from_hook: false,
    });
    // Navigation writes the target leaf first, so ordinal zero is deliberately wrong.
    let prepared = ready(s.session.prepare_entries(
        ready(s.session.read_stamp()).unwrap(),
        vec![PreparedEntrySlot {
            write_index: 0,
            entry,
        }],
    ))
    .unwrap()
    .unwrap();
    let full = prepared.entries()[0].clone();
    let result = structural::completed_result(
        &c,
        &SummarySuccessResult::BranchSummary(semantic),
        &full,
        false,
    )
    .unwrap();
    let cleanup = TerminalFacts {
        registers: vec![pi_harness_core::planner::RegisterCondition {
            namespace: RegisterNamespace::OperationPreparation,
            key: "op:task".into(),
            seq: Some(register_seq(
                &s.register(RegisterNamespace::OperationPreparation, "op:task"),
            )),
        }],
    };
    let r = apply(
        structural::terminal_recipe(
            &c,
            &TerminalCommit {
                current: c.clone(),
                result,
                telemetry_context: (),
            },
            &f,
            &cleanup,
            &structural::Settlement {
                prepared_entry: Some(full),
                ..Default::default()
            },
        )
        .unwrap(),
    );
    let before = ready(s.storage.snapshot()).unwrap();
    assert!(
        ready(s.session.commit_prepared_conditional_with_telemetry(
            prepared,
            request(r),
            &pi_agent_session::backend_support::NoopTelemetryContext
        ))
        .is_err()
    );
    assert_eq!(ready(s.storage.snapshot()).unwrap(), before);
}
#[test]
fn actual_unsummarized_root_terminal_preserves_lane_owned_next_run_payload() {
    let s = Store::new();
    let Register::LaneState {
        value: mut lane_state,
        seq: lane_seq,
        ..
    } = s.register(RegisterNamespace::LaneState, "main")
    else {
        panic!()
    };
    lane_state.pending_next_run.push(id("next"));
    ready(s.session.commit(Transaction{writes:vec![set(RegisterSet::PendingEntry{key:"next".into(),value:serde_json::from_value(serde_json::json!({"type":"message","payload":{"role":"user","content":"later","timestamp":1}})).unwrap()}),set(RegisterSet::LaneState{key:"main".into(),value:lane_state.clone()})]})).unwrap();
    let _ = lane_seq;
    let lane_seq = register_seq(&s.register(RegisterNamespace::LaneState, "main"));
    let a = structural::Acceptance {
        operation: Operation {
            operation_id: OperationId::new("op").unwrap(),
            lane: LaneId::new("main").unwrap(),
            source_leaf_id: Some(id("leaf")),
            started_at: n(10),
            intent: OperationIntent::Navigation(NavigationIntent {
                kind: Default::default(),
                target_id: None,
                summarize: false,
                label: None,
                custom_instructions: None,
            }),
        },
        lane_state,
        lane_seq,
        leaf_seq: Some(register_seq(
            &s.register(RegisterNamespace::LaneLeaf, "main"),
        )),
        source_leaf: Some(id("leaf")),
        configuration_seq: register_seq(&s.register(RegisterNamespace::LaneConfig, "main")),
        settings_revision: n(0),
        identities_present: true,
        task_id: TaskId::new("unused").unwrap(),
        preparation: None,
        target: None,
        telemetry_context: (),
    };
    assert!(matches!(
        s.publish(structural::acceptance_recipe(&a).unwrap().unwrap()),
        ConditionalOutcome::Applied(_)
    ));
    let (c, i, f) = s.current();
    let ProcedureAction::Existing(Action::Finish(result)) =
        next_action(&c, &i, &PlanningFacts::Terminal).unwrap()
    else {
        panic!()
    };
    let commit = TerminalCommit {
        current: c.clone(),
        result,
        telemetry_context: (),
    };
    let r = apply(
        pi_harness_core::terminal::terminal_recipe(
            &c,
            &commit,
            &i,
            &f,
            &TerminalFacts { registers: vec![] },
        )
        .unwrap(),
    );
    assert!(matches!(s.publish(r), ConditionalOutcome::Applied(_)));
    assert!(matches!(
        s.register(RegisterNamespace::LaneLeaf, "main"),
        Register::LaneLeaf { value: None, .. }
    ));
    assert!(
        matches!(s.register(RegisterNamespace::LaneState,"main"),Register::LaneState{value:LaneState{current_operation_id:None,pending_next_run},..}if pending_next_run==vec![id("next")])
    );
    assert!(
        ready(
            s.session
                .get_register(RegisterNamespace::PendingEntry, "next")
        )
        .unwrap()
        .is_some()
    );
}

fn review716_failure() -> OperationError {
    OperationError {
        code: OperationErrorCode::new("test_summary_failure").unwrap(),
        message: "summary failed".into(),
        details: pi_ai_core::Presence::Missing,
    }
}
fn review716_store(kind: &str, phase: &str) -> Store {
    let s = Store::new();
    if kind == "unsummarized" {
        let Register::LaneState {
            value: lane_state,
            seq: lane_seq,
            ..
        } = s.register(RegisterNamespace::LaneState, "main")
        else {
            panic!()
        };
        let acceptance = structural::Acceptance {
            operation: Operation {
                operation_id: OperationId::new("op").unwrap(),
                lane: LaneId::new("main").unwrap(),
                source_leaf_id: Some(id("leaf")),
                started_at: n(10),
                intent: OperationIntent::Navigation(NavigationIntent {
                    kind: Default::default(),
                    target_id: None,
                    summarize: false,
                    label: None,
                    custom_instructions: None,
                }),
            },
            lane_state,
            lane_seq,
            leaf_seq: Some(register_seq(
                &s.register(RegisterNamespace::LaneLeaf, "main"),
            )),
            source_leaf: Some(id("leaf")),
            configuration_seq: register_seq(&s.register(RegisterNamespace::LaneConfig, "main")),
            settings_revision: n(0),
            identities_present: true,
            task_id: TaskId::new("unused").unwrap(),
            preparation: None,
            target: None,
            telemetry_context: (),
        };
        assert!(matches!(
            s.publish(structural::acceptance_recipe(&acceptance).unwrap().unwrap()),
            ConditionalOutcome::Applied(_)
        ));
    } else {
        if kind == "compaction" {
            s.accept();
        } else {
            s.accept_navigation();
        }
        if phase != "deciding" {
            let outer = s.pending();
            let (c, mut inputs, facts) = s.current();
            let commit = if phase == "ready" {
                let planning = PlanningFacts::Structural(structural::Planning {
                    session_id: facts.session_id.clone(),
                    telemetry_context: (),
                    result_entry_id: None,
                });
                let ProcedureAction::Reconcile(commit) =
                    next_action(&c, &inputs, &planning).unwrap()
                else {
                    panic!()
                };
                Some(*commit)
            } else if phase == "retry" {
                inputs
                    .running
                    .insert(outer.key.clone(), EffectPlan::Summary(outer.clone()));
                Some(SettlementCommit {
                    current: c.clone(),
                    plan: EffectPlan::Summary(outer.clone()),
                    output: SettlementOutput::Summary {
                        key: outer.key,
                        outcome: SummaryAttemptOutcome::Retry(review716_failure()),
                    },
                    telemetry_context: (),
                })
            } else {
                None
            };
            if let Some(commit) = commit {
                let recipe = apply(
                    settlement_recipe(
                        &c,
                        &commit,
                        &inputs,
                        &facts,
                        &SettlementFacts::Structural(Default::default()),
                    )
                    .unwrap(),
                );
                assert!(matches!(s.publish(recipe), ConditionalOutcome::Applied(_)));
            }
        }
    }
    s
}
fn review716_cleanup(s: &Store, kind: &str) -> TerminalFacts {
    TerminalFacts {
        registers: if kind == "unsummarized" {
            vec![]
        } else {
            vec![pi_harness_core::planner::RegisterCondition {
                namespace: RegisterNamespace::OperationPreparation,
                key: "op:task".into(),
                seq: Some(register_seq(
                    &s.register(RegisterNamespace::OperationPreparation, "op:task"),
                )),
            }]
        },
    }
}
#[test]
fn failed_structural_terminal_requires_pending_and_preserves_every_illegal_prefix() {
    let mut illegal_publications = vec![];
    for kind in ["compaction", "navigation", "unsummarized"] {
        for phase in ["deciding", "ready", "retry", "pending"] {
            if kind == "unsummarized" && phase != "ready" {
                continue;
            }
            let s = review716_store(kind, phase);
            let (c, mut inputs, facts) = s.current();
            let cleanup = review716_cleanup(&s, kind);
            let before = ready(s.storage.snapshot()).unwrap();
            let commit = if phase == "pending" {
                // Positive control is an actual outer failure settlement, not a
                // guessed terminal result or handwritten durable operation state.
                let outer = structural::plan(&c, &facts.session_id, &()).unwrap();
                inputs
                    .running
                    .insert(outer.key.clone(), EffectPlan::Summary(outer.clone()));
                let output = SettlementCommit {
                    current: c.clone(),
                    plan: EffectPlan::Summary(outer.clone()),
                    output: SettlementOutput::Summary {
                        key: outer.key,
                        outcome: SummaryAttemptOutcome::Failure(review716_failure()),
                    },
                    telemetry_context: (),
                };
                let RecipeDecision::Terminal(commit) = settlement_recipe(
                    &c,
                    &output,
                    &inputs,
                    &facts,
                    &SettlementFacts::Structural(Default::default()),
                )
                .unwrap() else {
                    panic!()
                };
                *commit
            } else {
                TerminalCommit {
                    current: c.clone(),
                    result: if kind == "compaction" {
                        OperationResult::Compaction(CompactionOutcome::Failed(CompactionFailed {
                            kind: Default::default(),
                            leaf_id: id("leaf"),
                            error: review716_failure(),
                        }))
                    } else {
                        OperationResult::Navigation(NavigationOutcome::Failed(NavigationFailed {
                            kind: Default::default(),
                            leaf_id: Some(id("leaf")),
                            error: review716_failure(),
                        }))
                    },
                    telemetry_context: (),
                }
            };
            let decision =
                structural::terminal_recipe(&c, &commit, &facts, &cleanup, &Default::default())
                    .unwrap();
            let applied = if let RecipeDecision::Apply(recipe) = decision {
                matches!(s.publish(*recipe), ConditionalOutcome::Applied(_))
            } else {
                false
            };
            let after = ready(s.storage.snapshot()).unwrap();
            if phase == "pending" {
                assert!(applied, "legal {kind} pending failure must publish");
                assert!(
                    ready(
                        s.session
                            .get_register(RegisterNamespace::OperationMeta, "op")
                    )
                    .unwrap()
                    .is_none()
                );
                assert_eq!(before.entries, after.entries);
                assert_eq!(before.usage, after.usage);
            } else if applied || before != after {
                illegal_publications.push(format!("{kind}/{phase}"));
            }
        }
    }
    assert!(
        illegal_publications.is_empty(),
        "Failed terminal changed illegal prefixes: {illegal_publications:?}"
    );
}
#[test]
fn failed_result_never_replaces_cancelled_structural_reconciliation() {
    for (kind, phase) in [
        ("compaction", "deciding"),
        ("compaction", "ready"),
        ("compaction", "retry"),
        ("compaction", "pending"),
        ("navigation", "deciding"),
        ("navigation", "ready"),
        ("navigation", "retry"),
        ("navigation", "pending"),
        ("unsummarized", "ready"),
    ] {
        let s = review716_store(kind, phase);
        let (c, _, facts) = s.current();
        let marker = pi_harness_core::control::abort_recipe(&c, &facts, n(101), &[], &())
            .unwrap()
            .recipe
            .unwrap();
        assert!(matches!(s.publish(marker), ConditionalOutcome::Applied(_)));
        let (c, _, facts) = s.current();
        let failed = if kind == "compaction" {
            OperationResult::Compaction(CompactionOutcome::Failed(CompactionFailed {
                kind: Default::default(),
                leaf_id: id("leaf"),
                error: review716_failure(),
            }))
        } else {
            OperationResult::Navigation(NavigationOutcome::Failed(NavigationFailed {
                kind: Default::default(),
                leaf_id: Some(id("leaf")),
                error: review716_failure(),
            }))
        };
        let cleanup = review716_cleanup(&s, kind);
        let before = ready(s.storage.snapshot()).unwrap();
        assert!(matches!(
            structural::terminal_recipe(
                &c,
                &TerminalCommit {
                    current: c.clone(),
                    result: failed,
                    telemetry_context: ()
                },
                &facts,
                &cleanup,
                &Default::default()
            )
            .unwrap(),
            RecipeDecision::NoLongerExpected
        ));
        assert_eq!(ready(s.storage.snapshot()).unwrap(), before);
        let result = if kind == "compaction" {
            OperationResult::Compaction(CompactionOutcome::Aborted(CompactionAborted {
                kind: Default::default(),
                leaf_id: id("leaf"),
            }))
        } else {
            OperationResult::Navigation(NavigationOutcome::Aborted(NavigationAborted {
                kind: Default::default(),
                leaf_id: Some(id("leaf")),
            }))
        };
        let recipe = apply(
            structural::terminal_recipe(
                &c,
                &TerminalCommit {
                    current: c.clone(),
                    result,
                    telemetry_context: (),
                },
                &facts,
                &cleanup,
                &Default::default(),
            )
            .unwrap(),
        );
        assert!(matches!(s.publish(recipe), ConditionalOutcome::Applied(_)));
        assert!(
            ready(
                s.session
                    .get_register(RegisterNamespace::OperationState, "op")
            )
            .unwrap()
            .is_none()
        );
    }
}
