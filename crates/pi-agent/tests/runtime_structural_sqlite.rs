#![cfg(feature = "sqlite")]
mod runtime_structural_sqlite_support;
use pi_agent_schema::*;
use pi_ai_core::{Message, StopReason};
use runtime_structural_sqlite_support::*;
use std::sync::atomic::Ordering;

fn assert_clean(w: &World, op: &OperationId) {
    assert!(
        w.register(RegisterNamespace::OperationMeta, op.as_str())
            .is_none()
    );
    assert!(
        w.register(RegisterNamespace::OperationState, op.as_str())
            .is_none()
    );
    assert!(
        w.pump
            .drive(w.session.list_registers(
                RegisterNamespace::OperationPreparation,
                Some(&format!("{op}:"))
            ))
            .unwrap()
            .is_empty()
    );
    assert!(matches!(
        w.register(RegisterNamespace::LaneState, "main"),
        Some(Register::LaneState {
            value: LaneState {
                current_operation_id: None,
                ..
            },
            ..
        })
    ));
}
fn terminal_history(navigation: bool, expire: bool) {
    let w = World::new(false);
    let target = w.append("target conversation input");
    let source = w.append("abandoned recent input");
    let h = w.harness.clone();
    let dest = target.clone();
    let mut call = Box::pin(async move {
        if navigation {
            h.navigate_tree(
                Some(dest),
                NavigateOptions {
                    summarize: Some(true),
                    label: Some("selected target".into()),
                    custom_instructions: None,
                },
            )
            .await
            .map(|r| {
                let success = r.expect("valid summarized navigation must be admitted");
                (success.run_id, OperationResult::Navigation(success.outcome))
            })
        } else {
            h.compact(CompactOptions::default()).await.map(|r| {
                let success = r.expect("nonempty manual compaction must be admitted");
                (success.run_id, OperationResult::Compaction(success.outcome))
            })
        }
    });
    w.until_gate(call.as_mut(), |kind| kind == "commit_terminal");
    let (op, _) = w.current();
    assert_eq!(
        w.hooks.compactions.lock().unwrap().len(),
        usize::from(!navigation)
    );
    assert_eq!(
        w.hooks.navigations.lock().unwrap().len(),
        usize::from(navigation)
    );
    let path = w.path();
    // Facts are allowed during structural work; this invalidates old prepared
    // sequence/time metadata while preserving the already-completed hook result.
    w.pump
        .drive(w.harness.session().set_name(Some("allocator drift".into())))
        .unwrap();
    w.time.0.store(9000, Ordering::SeqCst);
    let prefix = database(&path, false).unwrap();
    let events_before = w.events.0.lock().unwrap().len();
    if expire {
        database(&path, true);
    }
    let result = w.finish(call.as_mut());
    if expire {
        assert!(
            matches!(
                result,
                Err(HarnessRuntimeError::Fault(_)) | Err(HarnessRuntimeError::Closed(_))
            ),
            "lost lease must refuse terminal publication: {result:?}"
        );
        assert_eq!(
            database(&path, false).unwrap(),
            prefix,
            "every nonlease table must retain the complete preterminal prefix"
        );
        assert_eq!(
            w.events.0.lock().unwrap().len(),
            events_before,
            "no terminal/entry/usage observation for refused prefix"
        );
        return;
    }
    let (returned_op, result) = result.expect("positive scheduling control must complete");
    assert_eq!(returned_op, op);
    let committed_snapshot = database(&path, false).unwrap();
    assert_eq!(
        committed_snapshot["__revision"].as_u64().unwrap(),
        prefix["__revision"].as_u64().unwrap() + 1,
        "the complete final prefix is one atomic checked commit"
    );
    let entry = match result {
        OperationResult::Compaction(CompactionOutcome::Completed(result)) => {
            assert!(!navigation);
            assert_eq!(result.leaf_id, result.entry.id);
            Entry::Compaction(result.entry)
        }
        OperationResult::Navigation(NavigationOutcome::Completed(result)) => {
            assert!(navigation);
            assert_eq!(result.old_leaf_id, Some(source.clone()));
            let entry = result
                .summary_entry
                .expect("summarized move requires full summary entry");
            assert_eq!(result.new_leaf_id, Some(entry.id.clone()));
            Entry::BranchSummary(entry)
        }
        other => panic!("expected completed structure: {other:?}"),
    };
    let (id, seq, timestamp) = match &entry {
        Entry::Compaction(e) => {
            assert_eq!(e.parent_id, Some(source.clone()));
            assert!(e.from_hook);
            (e.id.clone(), e.seq, e.timestamp)
        }
        Entry::BranchSummary(e) => {
            assert_eq!(e.parent_id, Some(target.clone()));
            assert_eq!(e.from_id, source);
            assert!(e.from_hook);
            (e.id.clone(), e.seq, e.timestamp)
        }
        _ => unreachable!(),
    };
    assert_eq!(
        timestamp,
        n(9000),
        "stale prepared candidate must refresh metadata without replaying hook"
    );
    let stored = w
        .pump
        .drive(w.session.get_entries(std::slice::from_ref(&id)))
        .unwrap();
    assert_eq!(stored[&id], entry);
    assert_eq!(
        w.pump.drive(w.harness.get_leaf_id()).unwrap(),
        Some(id.clone())
    );
    let ledger = ledger(&database(&path, false).unwrap());
    assert_eq!(ledger.len(), 1);
    assert_eq!(ledger[0].usage, usage(2, 1));
    assert_eq!(ledger[0].entry_id, None);
    assert_eq!(ledger[0].seq.get(), prefix["__next_seq"].as_u64().unwrap());
    assert_eq!(
        seq.get(),
        ledger[0].seq.get() + if navigation { 2 } else { 1 },
        "all-write offset includes hook usage and optional target move"
    );
    assert_eq!(
        w.pump.drive(w.session.get_stats()).unwrap().usage,
        usage(2, 1)
    );
    if navigation {
        assert_eq!(
            w.pump.drive(w.harness.session().get_label(target)).unwrap(),
            Some("selected target".into())
        );
    }
    let events = w.events.0.lock().unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e,HarnessEvent::EntryAdded(e)if e.entry==entry))
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e,HarnessEvent::Usage(e)if e.row==ledger[0]))
            .count(),
        1
    );
    match &entry{
        Entry::Compaction(row)=>assert_eq!(events.iter().filter(|e|matches!(e,HarnessEvent::CompactionEndCompleted(e)if &e.entry==row && e.run_id==op)).count(),1),
        Entry::BranchSummary(row)=>assert_eq!(events.iter().filter(|e|matches!(e,HarnessEvent::NavigationEndCompleted(e)if e.summary_entry.as_ref()==Some(row) && e.run_id==op)).count(),1),_=>unreachable!()
    }
    drop(events);
    assert_clean(&w, &op);
    assert_eq!(
        w.hooks.compactions.lock().unwrap().len(),
        usize::from(!navigation)
    );
    assert_eq!(
        w.hooks.navigations.lock().unwrap().len(),
        usize::from(navigation)
    );
    assert_eq!(
        w.adapter.calls.load(Ordering::SeqCst),
        0,
        "hook-supplied structures need no provider"
    );
}
#[test]
fn sqlite_manual_compaction_returns_exact_committed_metadata() {
    terminal_history(false, false)
}
#[test]
fn sqlite_manual_compaction_expired_lease_refuses_full_terminal_prefix() {
    terminal_history(false, true)
}
#[test]
fn sqlite_summarized_navigation_returns_exact_committed_metadata() {
    terminal_history(true, false)
}
#[test]
fn sqlite_summarized_navigation_expired_lease_refuses_full_terminal_prefix() {
    terminal_history(true, true)
}

fn overflow_history(expire: bool) {
    let mut w = World::new(true);
    let h = w.harness.clone();
    let mut call = Box::pin(async move {
        h.prompt(PromptInput::Text {
            text: "overflow prompt".into(),
            images: vec![],
        })
        .await
    });
    w.until_gate(call.as_mut(),|kind|kind=="commit_settlement"&&w.adapter.calls.load(Ordering::SeqCst)==1&&matches!(w.current().1,OperationState::Run(ref r)if matches!(r.phase,RunPhase::Assistant(AssistantPhase{generation:Generation::EffectPending{..},..}))));
    let (op, state) = w.current();
    let OperationState::Run(run) = state else {
        panic!()
    };
    let RunPhase::Assistant(AssistantPhase {
        generation:
            Generation::EffectPending {
                response_entry_id,
                usage_id,
                context,
                ..
            },
        ..
    }) = run.phase
    else {
        panic!()
    };
    let trigger = context.trigger_entry_id;
    let path = w.path();
    let prefix = database(&path, false).unwrap();
    assert!(
        w.pump
            .drive(
                w.session
                    .get_entries(std::slice::from_ref(&response_entry_id))
            )
            .unwrap()
            .is_empty()
    );
    assert!(ledger(&prefix).is_empty());
    assert!(
        w.pump
            .drive(w.session.list_registers(
                RegisterNamespace::OperationPreparation,
                Some(&format!("{op}:"))
            ))
            .unwrap()
            .is_empty()
    );
    if expire {
        database(&path, true);
        w.time.0.store(9000, Ordering::SeqCst);
        let outcome = w.finish(call.as_mut());
        assert!(
            matches!(
                outcome,
                Err(HarnessRuntimeError::Fault(_)) | Err(HarnessRuntimeError::Closed(_))
            ),
            "expired overflow commit must fail closed: {outcome:?}"
        );
        assert_eq!(
            database(&path, false).unwrap(),
            prefix,
            "overflow response/usage/preparation/state must publish all or none"
        );
        return;
    }
    w.pump.drive(w.harness.execute_action()).unwrap();
    w.pump.until(||matches!(w.current().1,OperationState::Run(ref r)if matches!(r.phase,RunPhase::Compaction(_))));
    let after = database(&path, false).unwrap();
    assert_eq!(
        after["__revision"].as_u64().unwrap(),
        prefix["__revision"].as_u64().unwrap() + 1,
        "R/U/preparation/deciding share one SQLite commit"
    );
    let rows = ledger(&after);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, usage_id);
    assert_eq!(rows[0].usage, usage(11, 2));
    assert_eq!(rows[0].entry_id, Some(response_entry_id.clone()));
    let response = w
        .pump
        .drive(
            w.session
                .get_entries(std::slice::from_ref(&response_entry_id)),
        )
        .unwrap()
        .remove(&response_entry_id)
        .unwrap();
    let Entry::Message(response) = response else {
        panic!()
    };
    assert_eq!(response.parent_id, Some(trigger.clone()));
    let AgentMessage::BuiltIn(Message::Assistant(assistant)) = response.message else {
        panic!()
    };
    assert_eq!(assistant.stop_reason, StopReason::Error);
    assert!(
        assistant
            .error_message
            .as_deref()
            .unwrap()
            .contains("maximum context length")
    );
    let (_, state) = w.current();
    let OperationState::Run(run) = state else {
        panic!()
    };
    assert_eq!(
        run.latest_assistant_entry_id,
        Some(response_entry_id.clone())
    );
    let RunPhase::Compaction(compaction) = run.phase else {
        panic!()
    };
    assert_eq!(compaction.reason, RunCompactionReason::Overflow);
    let StructuralDecision::Deciding { task_id } = compaction.structural else {
        panic!()
    };
    let prep = w
        .register(
            RegisterNamespace::OperationPreparation,
            &format!("{op}:{task_id}"),
        )
        .expect("response settlement must publish preparation atomically");
    let mut stored_preparation = match &prep {
        Register::OperationPreparation { value, .. } => serde_json::to_value(value).unwrap(),
        _ => panic!("preparation register"),
    };
    stored_preparation.as_object_mut().unwrap().remove("kind");
    assert!(
        !stored_preparation
            .to_string()
            .contains("OVERFLOW-MUST-NOT-PROJECT")
    );
    let Register::OperationPreparation {
        value:
            DurableStructuralPreparation::Compaction {
                messages_to_summarize,
                retained_tail,
                ..
            },
        ..
    } = prep
    else {
        panic!()
    };
    assert!(
        !serde_json::to_string(&messages_to_summarize)
            .unwrap()
            .contains("OVERFLOW-MUST-NOT-PROJECT")
    );
    assert!(
        !serde_json::to_string(&retained_tail)
            .unwrap()
            .contains("OVERFLOW-MUST-NOT-PROJECT")
    );
    assert!(
        w.hooks.compactions.lock().unwrap().is_empty(),
        "deciding prefix must exist before decision hook runs"
    );
    w.until_gate(call.as_mut(),|kind|kind=="commit_transition"&&matches!(w.current().1,OperationState::Run(ref r)if matches!(r.phase,RunPhase::Checkpoint(CheckpointPhase{continuation:Continuation::NeedAssistant{overflow_recovery_used:true},..}))));
    let checkpoint = w
        .register(RegisterNamespace::OperationState, op.as_str())
        .unwrap();
    let (_, state) = w.current();
    let OperationState::Run(r) = state else {
        panic!()
    };
    let RunPhase::Checkpoint(p) = r.phase else {
        panic!()
    };
    assert_eq!(p.trigger_entry_id, trigger);
    assert!(matches!(
        p.continuation,
        Continuation::NeedAssistant {
            overflow_recovery_used: true
        }
    ));
    let leaf = w.pump.drive(w.harness.get_leaf_id()).unwrap().unwrap();
    let entry = w
        .pump
        .drive(w.session.get_entries(std::slice::from_ref(&leaf)))
        .unwrap()
        .remove(&leaf)
        .unwrap();
    assert!(
        matches!(entry,Entry::Compaction(ref c)if c.parent_id==Some(response_entry_id.clone()))
    );
    let hooks = w.hooks.compactions.lock().unwrap().clone();
    assert_eq!(hooks.len(), 1);
    assert_eq!(
        serde_json::to_value(&hooks[0].preparation).unwrap(),
        stored_preparation,
        "decision hook hydrates exact committed preparation"
    );
    assert_eq!(
        hooks[0].preparation.messages_to_summarize,
        messages_to_summarize
    );
    assert_eq!(hooks[0].preparation.retained_tail, retained_tail);
    let before_reopen = ledger(&database(&path, false).unwrap());
    assert_eq!(before_reopen.len(), 2);
    w.pump.drive(w.harness.close()).unwrap();
    assert!(matches!(
        w.pump.drive(call),
        Err(HarnessRuntimeError::Closed(_))
    ));
    let restored = w.reopen();
    assert_eq!(restored.len(), 1);
    assert_eq!(
        w.register(RegisterNamespace::OperationState, op.as_str()),
        Some(checkpoint)
    );
    assert_eq!(ledger(&database(&path, false).unwrap()), before_reopen);
    assert_eq!(
        w.adapter.calls.load(Ordering::SeqCst),
        1,
        "restore itself must not request"
    );
    let h = w.harness.clone();
    let mut resume = Box::pin(async move { h.resume().await });
    let result = w.finish(resume.as_mut());
    assert!(
        matches!(result, Ok(Ok(_))),
        "resume should complete: {result:?}"
    );
    assert_clean(&w, &op);
    assert!(matches!(
        w.pump.drive(w.harness.get_last_result()).unwrap(),
        Some(LaneLastResult::RunCompletedAssistant(_))
    ));
    let final_rows = ledger(&database(&path, false).unwrap());
    assert_eq!(final_rows.len(), 3);
    assert_eq!(final_rows.iter().filter(|r| r.id == usage_id).count(), 1);
    assert_eq!(
        w.pump.drive(w.session.get_stats()).unwrap().usage,
        usage(17, 4)
    );
    assert_eq!(w.adapter.calls.load(Ordering::SeqCst), 2);
}
#[test]
fn sqlite_first_overflow_is_atomic_and_checkpoint_survives_physical_reopen() {
    overflow_history(false)
}
#[test]
fn sqlite_first_overflow_expired_lease_preserves_every_nonlease_table() {
    overflow_history(true)
}
