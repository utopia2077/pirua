//! Actual ordered publication, with Memory authority behind a RuntimeStorage spy.
mod runtime_r1_r4_support;
mod runtime_r5_r7_support;
use pi_agent_schema::*;
use pi_agent_session::storage::AtomicStorage;
use pi_harness_runtime::owner::RuntimeFactory;
use runtime_r1_r4_support::*;
use runtime_r5_r7_support::{Listener, tracked_session, tracking::TrackedStorage, user};
use std::sync::{Arc, Mutex};
struct ResultHook;
impl HookHandler<BeforeCompaction> for ResultHook {
    fn call(
        &self,
        e: BeforeCompactionEvent,
    ) -> BoxFuture<'_, Result<Option<BeforeCompactionResult>, HandlerError>> {
        Box::pin(async move {
            let mut usage = pi_ai_core::Usage::zero();
            usage.output = n(23);
            usage.total_tokens = n(23);
            Ok(Some(BeforeCompactionResult::Compaction(Box::new(
                BeforeCompactionSupply {
                    decline: None,
                    compaction: CompactResult {
                        summary: "transaction compaction".into(),
                        tokens_before: e.preparation.tokens_before,
                        retained_tail: e.preparation.retained_tail,
                        usage: Some(usage),
                        details: pi_ai_core::Presence::Null,
                    },
                },
            ))))
        })
    }
}
impl HookHandler<BeforeNavigation> for ResultHook {
    fn call(
        &self,
        _: BeforeNavigationEvent,
    ) -> BoxFuture<'_, Result<Option<BeforeNavigationResult>, HandlerError>> {
        Box::pin(async {
            let mut usage = pi_ai_core::Usage::zero();
            usage.output = n(29);
            usage.total_tokens = n(29);
            Ok(Some(BeforeNavigationResult::Summary(
                BeforeNavigationSupply {
                    decline: None,
                    summary: BranchSummaryResult {
                        summary: "transaction branch".into(),
                        read_files: vec!["read".into()],
                        modified_files: vec!["write".into()],
                        usage: Some(usage),
                    },
                },
            )))
        })
    }
}
#[test]
fn hook_usage_entry_move_label_cleanup_is_one_actual_ordered_transaction() {
    for navigation in [false, true] {
        let s = Setup::new();
        let storage = Arc::new(TrackedStorage::new(s.store.clone()));
        let session = tracked_session(&s, storage.clone());
        let f = RuntimeFactory::new(
            s.time.clone(),
            s.pump.clone(),
            s.ids.clone(),
            Arc::new(()),
            Arc::new(Summary),
        );
        let h = s
            .pump
            .drive(f.create(s.options_for(session, DriveMode::Automatic)))
            .unwrap()
            .harness;
        let target = s
            .pump
            .drive(h.session().append_message(user("original target")))
            .unwrap();
        let source = s
            .pump
            .drive(h.session().append_message(user("abandoned source")))
            .unwrap();
        let _compact = h
            .hooks()
            .before_compaction(None, Arc::new(ResultHook))
            .unwrap();
        let _nav = h
            .hooks()
            .before_navigation(None, Arc::new(ResultHook))
            .unwrap();
        let listener = Arc::new(Listener(Mutex::new(vec![])));
        let watch = s.pump.drive(h.watch()).unwrap();
        watch.start(listener.clone());
        storage.transactions.lock().unwrap().clear();
        let entry = if navigation {
            let r = s
                .pump
                .drive(h.navigate_tree(
                    Some(target.clone()),
                    NavigateOptions {
                        summarize: Some(true),
                        label: Some("target-label".into()),
                        ..Default::default()
                    },
                ))
                .unwrap()
                .unwrap();
            let NavigationOutcome::Completed(o) = r.outcome else {
                panic!("navigation outcome")
            };
            let entry = o.summary_entry.unwrap();
            assert_eq!(entry.parent_id, Some(target.clone()));
            assert_eq!(entry.from_id, source);
            Entry::BranchSummary(entry)
        } else {
            let r = s
                .pump
                .drive(h.compact(CompactOptions::default()))
                .unwrap()
                .unwrap();
            let CompactionOutcome::Completed(o) = r.outcome else {
                panic!("compaction outcome")
            };
            assert_eq!(o.entry.parent_id, Some(source));
            Entry::Compaction(o.entry)
        };
        for _ in 0..30 {
            s.pump.tick();
        }
        let transactions = storage.transactions.lock().unwrap();
        let terminal:Vec<_>=transactions.iter().filter(|t|t.writes.iter().any(|w|matches!(w,Write::SetRegister(r)if matches!(r.as_ref(),RegisterSet::LaneLastResult{..})))).collect();
        assert_eq!(terminal.len(), 1);
        let writes = &terminal[0].writes;
        assert!(matches!(&writes[0],Write::Usage(u)if u.entry_id.is_none()&&!u.adjustment));
        let entry_index = if navigation { 2 } else { 1 };
        assert!(matches!(&writes[entry_index], Write::Entry(_)));
        if navigation {
            assert!(
                matches!(&writes[1],Write::SetRegister(r)if matches!(r.as_ref(),RegisterSet::LaneLeaf{value:Some(id),..}if id==&target))
            );
        }
        let entry_id = match &entry {
            Entry::Compaction(e) => &e.id,
            Entry::BranchSummary(e) => &e.id,
            _ => unreachable!(),
        };
        assert!(
            matches!(&writes[entry_index+1],Write::SetRegister(r)if matches!(r.as_ref(),RegisterSet::LaneLeaf{value:Some(id),..}if id==entry_id))
        );
        if navigation {
            assert!(
                matches!(&writes[entry_index+2],Write::SetRegister(r)if matches!(r.as_ref(),RegisterSet::FactLabel{key,value}if key==target.as_str()&&value=="target-label"))
            );
        }
        let snapshot = s.pump.drive(s.store.snapshot()).unwrap();
        assert!(snapshot.entries.contains(&entry));
        assert!(!snapshot.registers.iter().any(|r| matches!(
            r,
            Register::OperationPreparation { .. }
                | Register::OperationState { .. }
                | Register::OperationMeta { .. }
        )));
        let actual_seq = match &entry {
            Entry::Compaction(e) => e.seq,
            Entry::BranchSummary(e) => e.seq,
            _ => unreachable!(),
        };
        assert_eq!(
            actual_seq.get(),
            snapshot.usage.last().unwrap().seq.get() + entry_index as u64
        );
        let delivered = listener.0.lock().unwrap();
        let events: Vec<_> = delivered
            .iter()
            .filter(|e| {
                matches!(
                    e,
                    HarnessEvent::Usage(_)
                        | HarnessEvent::EntryAdded(_)
                        | HarnessEvent::CompactionEndCompleted(_)
                        | HarnessEvent::NavigationEndCompleted(_)
                )
            })
            .collect();
        let usage_at = events
            .iter()
            .position(|e| matches!(e, HarnessEvent::Usage(_)))
            .unwrap();
        let entry_at = events
            .iter()
            .position(|e| matches!(e,HarnessEvent::EntryAdded(e)if e.entry==entry))
            .unwrap();
        assert!(usage_at < entry_at);
        assert_eq!(events.len(), 3);
        match events.last().unwrap() {
            HarnessEvent::CompactionEndCompleted(e) => {
                assert_eq!(Entry::Compaction(e.entry.clone()), entry)
            }
            HarnessEvent::NavigationEndCompleted(e) => assert_eq!(
                Entry::BranchSummary(e.summary_entry.clone().unwrap()),
                entry
            ),
            _ => panic!("typed structural end absent"),
        }
        assert!(s.faux.requests.lock().unwrap().is_empty());
    }
}
#[test]
fn stale_terminal_token_refreshes_returned_and_emitted_full_row_without_hook_replay() {
    let s = Setup::new();
    let tracked = Arc::new(TrackedStorage::new(s.store.clone()));
    let session = tracked_session(&s, tracked.clone());
    let f = RuntimeFactory::new(
        s.time.clone(),
        s.pump.clone(),
        s.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    let h = s
        .pump
        .drive(f.create(s.options_for(session, DriveMode::Manual)))
        .unwrap()
        .harness;
    let other = s
        .pump
        .drive(h.create_lane(LaneId::new("metadata-writer").unwrap(), None))
        .unwrap()
        .unwrap();
    s.pump
        .drive(h.session().append_message(user("history")))
        .unwrap();
    s.pump
        .drive(h.session().append_message(user("tail")))
        .unwrap();
    struct Count(Arc<std::sync::atomic::AtomicUsize>);
    impl HookHandler<BeforeCompaction> for Count {
        fn call(
            &self,
            event: BeforeCompactionEvent,
        ) -> BoxFuture<'_, Result<Option<BeforeCompactionResult>, HandlerError>> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            <ResultHook as HookHandler<BeforeCompaction>>::call(&ResultHook, event)
        }
    }
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let _hook = h
        .hooks()
        .before_compaction(None, Arc::new(Count(calls.clone())))
        .unwrap();
    let watch = s.pump.drive(h.watch()).unwrap();
    let events = Arc::new(Listener(Mutex::new(vec![])));
    watch.start(events.clone());
    let mut compact = Box::pin(h.compact(CompactOptions::default()));
    assert!(poll(compact.as_mut()).is_pending());
    for _ in 0..500 {
        s.pump.tick();
        if let Some(action) = s.pump.drive(h.peek_action()).unwrap() {
            if action.kind == "commit_terminal" {
                break;
            }
            s.pump.drive(h.execute_action()).unwrap();
        }
    }
    assert!(matches!(s.pump.drive(h.peek_action()).unwrap(),Some(a)if a.kind=="commit_terminal"));
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    s.time.0.store(77, std::sync::atomic::Ordering::SeqCst);
    s.pump
        .drive(
            other
                .session()
                .append_message(user("real allocator advances after final row preparation")),
        )
        .unwrap();
    s.pump.drive(h.run_to_completion()).unwrap();
    let result = s.pump.drive(compact).unwrap().unwrap();
    let CompactionOutcome::Completed(outcome) = result.outcome else {
        panic!("stale result not completed")
    };
    for _ in 0..30 {
        s.pump.tick();
    }
    assert_eq!(outcome.entry.timestamp, n(77));
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert!(s.faux.requests.lock().unwrap().is_empty());
    let snapshot = s.pump.drive(s.store.snapshot()).unwrap();
    assert!(
        snapshot
            .entries
            .contains(&Entry::Compaction(outcome.entry.clone()))
    );
    assert_eq!(snapshot.usage.len(), 1);
    let delivered = events.0.lock().unwrap();
    assert_eq!(
        delivered
            .iter()
            .filter(|e| matches!(e, HarnessEvent::CompactionEndCompleted(_)))
            .count(),
        1
    );
    assert!(
        delivered
            .iter()
            .any(|e| matches!(e,HarnessEvent::CompactionEndCompleted(e)if e.entry==outcome.entry))
    );
    assert!(delivered.iter().any(|e|matches!(e,HarnessEvent::EntryAdded(e)if e.entry==Entry::Compaction(outcome.entry.clone()))));
}
