//! Detached preparation preserves real row metadata and serializes admission.
mod runtime_r1_r4_support;
mod runtime_r5_r7_support;
use pi_agent_schema::*;
use pi_agent_session::storage::AtomicStorage;
use pi_ai_core::{CancellationToken, Presence};
use runtime_r1_r4_support::*;
use runtime_r5_r7_support::{factory, prompt, tick_until, user};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
struct ParkProjection {
    entered: CancellationToken,
    release: CancellationToken,
    seen: Mutex<Vec<CustomEntry>>,
    dropped: Arc<AtomicUsize>,
}
struct DropMark(Arc<AtomicUsize>);
impl Drop for DropMark {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
impl EntryProjector for ParkProjection {
    fn project(
        &self,
        entry: CustomEntry,
    ) -> BoxFuture<'_, Result<Option<Vec<AgentMessage>>, HarnessRuntimeError>> {
        Box::pin(async move {
            let _drop = DropMark(self.dropped.clone());
            self.seen.lock().unwrap().push(entry.clone());
            self.entered.cancel();
            self.release.cancelled().await;
            Ok(Some(vec![user(&format!(
                "actual custom seq={} time={}",
                entry.seq.get(),
                entry.timestamp.get()
            ))]))
        })
    }
}
fn projector() -> Arc<ParkProjection> {
    Arc::new(ParkProjection {
        entered: CancellationToken::new(),
        release: CancellationToken::new(),
        seen: Mutex::new(vec![]),
        dropped: Arc::new(AtomicUsize::new(0)),
    })
}
#[test]
fn reserved_preparation_blocks_operations_allows_config_next_run_and_rechecks_metadata() {
    let s = Setup::new();
    let p = projector();
    let mut options = s.options(DriveMode::Automatic);
    options
        .entry_projectors
        .insert(CustomType::new("structure.actual").unwrap(), p.clone());
    let h = s.pump.drive(factory(&s).create(options)).unwrap().harness;
    s.pump
        .drive(h.session().append_message(user("earlier turn")))
        .unwrap();
    let id = s
        .pump
        .drive(
            h.session()
                .append_custom_entry(CustomType::new("structure.actual").unwrap(), Presence::Null),
        )
        .unwrap();
    s.pump
        .drive(h.session().append_message(user("tail turn")))
        .unwrap();
    let actual = s.pump.drive(h.session().get_entry(id)).unwrap().unwrap();
    let mut compact = Box::pin(h.compact(CompactOptions::default()));
    assert!(poll(compact.as_mut()).is_pending());
    tick_until(&s, || p.entered.is_cancelled());
    assert!(
        matches!(s.pump.drive(h.prompt(prompt())).unwrap(),Err(RunExpectedError::LaneBusy(e))if e.operation_kind==OperationKind::Compaction)
    );
    assert!(
        matches!(s.pump.drive(h.navigate_tree(None,NavigateOptions::default())).unwrap(),Err(NavigationExpectedError::LaneBusy(e))if e.operation_kind==OperationKind::Compaction)
    );
    let next = s
        .pump
        .drive(h.next_run(user("next run is preserved")))
        .unwrap()
        .unwrap();
    s.pump
        .drive(h.set_thinking_level(AgentThinkingLevel::High))
        .unwrap();
    let tree = h.session();
    let mut idle = Box::pin(tree.append_message(user("idle append after structure")));
    assert!(poll(idle.as_mut()).is_pending());
    for _ in 0..10 {
        s.pump.tick();
    }
    assert!(poll(idle.as_mut()).is_pending());
    assert!(
        !s.pump
            .drive(s.store.snapshot())
            .unwrap()
            .registers
            .iter()
            .any(|r| matches!(
                r,
                Register::OperationMeta { .. } | Register::OperationState { .. }
            ))
    );
    p.release.cancel();
    let result = s.pump.drive(compact).unwrap().unwrap();
    assert!(matches!(result.outcome, CompactionOutcome::Completed(_)));
    let appended = s.pump.drive(idle).unwrap();
    assert_eq!(s.pump.drive(h.get_leaf_id()).unwrap(), Some(appended));
    assert!(p.seen.lock().unwrap().len() >= 2);
    for seen in p.seen.lock().unwrap().iter() {
        assert_eq!(Entry::Custom(seen.clone()), actual)
    }
    let state = s.pump.drive(h.watch()).unwrap().snapshot();
    assert_eq!(
        s.pump.drive(h.get_thinking_level()).unwrap(),
        AgentThinkingLevel::High
    );
    assert!(
        state
            .queues
            .next_run
            .iter()
            .any(|q| q.entry_id == next.entry_id)
    );
}
#[test]
fn close_retires_pending_preacceptance_projector_with_zero_operation_writes() {
    let s = Setup::new();
    let p = projector();
    let mut options = s.options(DriveMode::Automatic);
    options
        .entry_projectors
        .insert(CustomType::new("structure.actual").unwrap(), p.clone());
    let h = s.pump.drive(factory(&s).create(options)).unwrap().harness;
    s.pump
        .drive(h.session().append_message(user("earlier")))
        .unwrap();
    s.pump
        .drive(
            h.session()
                .append_custom_entry(CustomType::new("structure.actual").unwrap(), Presence::Null),
        )
        .unwrap();
    s.pump
        .drive(h.session().append_message(user("later")))
        .unwrap();
    let before = s.pump.drive(s.store.snapshot()).unwrap();
    let mut call = Box::pin(h.compact(CompactOptions::default()));
    assert!(poll(call.as_mut()).is_pending());
    tick_until(&s, || p.entered.is_cancelled());
    s.pump.drive(h.close()).unwrap();
    assert!(matches!(
        s.pump.drive(call),
        Err(HarnessRuntimeError::Closed(_))
    ));
    assert_eq!(p.dropped.load(Ordering::SeqCst), 1);
    let reopened = s.backing.open(s.time.clone()).unwrap();
    assert_eq!(s.pump.drive(reopened.snapshot()).unwrap(), before);
    assert!(s.faux.requests.lock().unwrap().is_empty());
}
struct ReentrantProjection {
    lane: Mutex<Option<Arc<dyn AgentLane>>>,
    other: Mutex<Option<Arc<dyn AgentLane>>>,
    calls: AtomicUsize,
    config: bool,
}
impl EntryProjector for ReentrantProjection {
    fn project(
        &self,
        _: CustomEntry,
    ) -> BoxFuture<'_, Result<Option<Vec<AgentMessage>>, HarnessRuntimeError>> {
        Box::pin(async move {
            let first = self.calls.fetch_add(1, Ordering::SeqCst) == 0;
            if first {
                let lane = self.lane.lock().unwrap().clone().unwrap();
                let other = self.other.lock().unwrap().clone().unwrap();
                if self.config {
                    lane.set_thinking_level(AgentThinkingLevel::High).await?;
                } else {
                    other
                        .session()
                        .append_message(user("other lane advances actual backend stamp"))
                        .await
                        .unwrap();
                }
            }
            Ok(Some(vec![user(if first {
                "STALE prepared input must never publish"
            } else {
                "FRESH coherent preparation"
            })]))
        })
    }
}
#[test]
fn threshold_stale_config_and_other_lane_write_publish_no_stale_preparation() {
    for config in [true, false] {
        let s = Setup::new();
        let tracked = Arc::new(runtime_r5_r7_support::tracking::TrackedStorage::new(
            s.store.clone(),
        ));
        let session = runtime_r5_r7_support::tracked_session(&s, tracked.clone());
        let projector = Arc::new(ReentrantProjection {
            lane: Mutex::new(None),
            other: Mutex::new(None),
            calls: AtomicUsize::new(0),
            config,
        });
        let mut options = s.options_for(session, DriveMode::Automatic);
        options.compaction.enabled = true;
        options.compaction.reserve_tokens = s.faux.model.context_window;
        options.entry_projectors.insert(
            CustomType::new("threshold.input").unwrap(),
            projector.clone(),
        );
        let f = pi_harness_runtime::owner::RuntimeFactory::new(
            s.time.clone(),
            s.pump.clone(),
            s.ids.clone(),
            Arc::new(()),
            Arc::new(Summary),
        );
        let h = s.pump.drive(f.create(options)).unwrap().harness;
        let other = s
            .pump
            .drive(h.create_lane(LaneId::new("other").unwrap(), None))
            .unwrap()
            .unwrap();
        *projector.lane.lock().unwrap() = Some(h.clone());
        *projector.other.lock().unwrap() = Some(other);
        s.pump
            .drive(
                h.session().append_custom_entry(
                    CustomType::new("threshold.input").unwrap(),
                    Presence::Null,
                ),
            )
            .unwrap();
        s.pump
            .drive(h.session().append_message(user("tail boundary")))
            .unwrap();
        struct Decline;
        impl HookHandler<BeforeCompaction> for Decline {
            fn call(
                &self,
                _: BeforeCompactionEvent,
            ) -> BoxFuture<'_, Result<Option<BeforeCompactionResult>, HandlerError>> {
                Box::pin(async {
                    Ok(Some(
                        BeforeCompactionResult::DeclineTrue(Default::default()),
                    ))
                })
            }
        }
        let _hook = h
            .hooks()
            .before_compaction(None, Arc::new(Decline))
            .unwrap();
        tracked.transactions.lock().unwrap().clear();
        let result = s.pump.drive(h.prompt(prompt())).unwrap().unwrap();
        assert!(matches!(
            result.outcome,
            RunOutcome::CompletedWithAssistant(_)
        ));
        assert!(projector.calls.load(Ordering::SeqCst) >= 2);
        let transactions = tracked.transactions.lock().unwrap();
        let preparations: Vec<_> = transactions
            .iter()
            .flat_map(|tx| &tx.writes)
            .filter_map(|w| match w {
                Write::SetRegister(r) => match r.as_ref() {
                    RegisterSet::OperationPreparation { value, .. } => {
                        Some(serde_json::to_string(value).unwrap())
                    }
                    _ => None,
                },
                _ => None,
            })
            .collect();
        assert!(!preparations.is_empty());
        assert!(preparations.iter().all(|p| !p.contains("STALE")));
        assert!(
            preparations
                .iter()
                .any(|p| p.contains("FRESH coherent preparation"))
        );
    }
}
