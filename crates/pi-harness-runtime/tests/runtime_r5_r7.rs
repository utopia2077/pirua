mod runtime_r1_r4_support;
mod runtime_r5_r7_support;
use pi_agent_schema::*;
use pi_agent_session::storage::AtomicStorage;
use pi_ai_core::{CancellationToken, StopReason};
use runtime_r1_r4_support::*;
use runtime_r5_r7_support::*;
use std::{
    sync::{Arc, Mutex},
    task::Poll,
};
#[test]
fn public_next_queue_cancel_capture_and_adjustment() {
    let s = Setup::new();
    let c = s
        .pump
        .drive(factory(&s).create(s.options(DriveMode::Automatic)))
        .unwrap();
    let events = Arc::new(Listener(Mutex::new(vec![])));
    let watch = s.pump.drive(c.harness.watch()).unwrap();
    watch.start(events.clone());
    let a = s
        .pump
        .drive(c.harness.next_run(user("a")))
        .unwrap()
        .unwrap();
    let b = s
        .pump
        .drive(c.harness.next_run(user("b")))
        .unwrap()
        .unwrap();
    assert!(s.faux.requests.lock().unwrap().is_empty());
    assert_eq!(
        s.pump
            .drive(c.harness.cancel_queued(a.entry_id.clone()))
            .unwrap()
            .unwrap(),
        CancelQueuedOutcome::Cancelled
    );
    assert_eq!(
        s.pump
            .drive(c.harness.cancel_queued(a.entry_id))
            .unwrap()
            .unwrap(),
        CancelQueuedOutcome::NotFound
    );
    s.pump.drive(c.harness.prompt(prompt())).unwrap().unwrap();
    assert_eq!(
        s.pump
            .drive(c.harness.cancel_queued(b.entry_id))
            .unwrap()
            .unwrap(),
        CancelQueuedOutcome::AlreadyConsumed
    );
    let usage = s
        .pump
        .drive(c.harness.record_usage(RecordUsageRequest {
            usage: pi_ai_core::Usage::zero(),
            options: Default::default(),
        }))
        .unwrap()
        .unwrap();
    let snapshot = s.pump.drive(s.store.snapshot()).unwrap();
    assert!(
        snapshot
            .usage
            .iter()
            .any(|row| row.id == usage.usage_id && row.adjustment)
    );
    assert!(
        events
            .0
            .lock()
            .unwrap()
            .iter()
            .any(|e| matches!(e, HarnessEvent::QueueUpdate(_)))
    );
    assert!(
        events
            .0
            .lock()
            .unwrap()
            .iter()
            .any(|e| matches!(e, HarnessEvent::Usage(_)))
    );
}
#[test]
fn public_steer_followup_and_pending_write_drain_after_live_response() {
    let s = Setup::new();
    let hold = CancellationToken::new();
    *s.faux.hold.lock().unwrap() = Some(hold.clone());
    let c = s
        .pump
        .drive(factory(&s).create(s.options(DriveMode::Automatic)))
        .unwrap();
    let mut run = Box::pin(c.harness.prompt(prompt()));
    assert!(poll(run.as_mut()).is_pending());
    tick_until(&s, || !s.faux.requests.lock().unwrap().is_empty());
    let steer = s
        .pump
        .drive(c.harness.steer(user("steer")))
        .unwrap()
        .unwrap();
    let follow = s
        .pump
        .drive(c.harness.follow_up(user("follow")))
        .unwrap()
        .unwrap();
    let write = s
        .pump
        .drive(c.harness.session().append_message(user("write")))
        .unwrap();
    assert!(
        s.pump
            .drive(c.harness.session().get_entry(write.clone()))
            .unwrap()
            .is_none()
    );
    hold.cancel();
    s.pump.drive(run).unwrap().unwrap();
    for id in [steer.entry_id, follow.entry_id, write] {
        assert!(
            s.pump
                .drive(c.harness.session().get_entry(id))
                .unwrap()
                .is_some()
        );
    }
    assert!(s.faux.requests.lock().unwrap().len() >= 3);
}
#[test]
fn public_abort_marker_repeat_and_writes_reconcile_without_timer_fault() {
    let s = Setup::new();
    let hold = CancellationToken::new();
    *s.faux.hold.lock().unwrap() = Some(hold);
    let c = s
        .pump
        .drive(factory(&s).create(s.options(DriveMode::Manual)))
        .unwrap();
    let mut run = Box::pin(c.harness.prompt(prompt()));
    assert!(poll(run.as_mut()).is_pending());
    for _ in 0..40 {
        if current(&s).is_some() {
            break;
        }
        s.pump.drive(c.harness.execute_action()).unwrap();
        s.pump.tick();
    }
    assert!(current(&s).is_some());
    let steer = s
        .pump
        .drive(c.harness.steer(user("drain")))
        .unwrap()
        .unwrap();
    let write = s
        .pump
        .drive(c.harness.session().append_message(user("write")))
        .unwrap();
    let first = s.pump.drive(c.harness.abort()).unwrap().unwrap();
    let snap = s.pump.drive(s.store.snapshot()).unwrap();
    let repeat = s.pump.drive(c.harness.abort()).unwrap().unwrap();
    assert_eq!(first, repeat);
    assert_eq!(snap, s.pump.drive(s.store.snapshot()).unwrap());
    assert_eq!(
        s.pump
            .drive(c.harness.cancel_queued(steer.entry_id.clone()))
            .unwrap()
            .unwrap(),
        CancelQueuedOutcome::NotFound
    );
    for _ in 0..100 {
        if let Poll::Ready(result) = poll(run.as_mut()) {
            assert!(matches!(
                result.unwrap().unwrap().outcome,
                RunOutcome::AbortedWithoutAssistant(_)
            ));
            assert!(
                s.pump
                    .drive(c.harness.session().get_entry(write))
                    .unwrap()
                    .is_some()
            );
            assert!(
                s.pump
                    .drive(
                        s.session
                            .get_register(RegisterNamespace::PendingEntry, steer.entry_id.as_str())
                    )
                    .unwrap()
                    .is_none()
            );
            return;
        }
        s.pump.drive(c.harness.execute_action()).unwrap();
        s.pump.tick();
    }
    panic!("abort did not finish");
}
#[test]
fn public_deferred_one_poll_each_resume_and_same_process_recovery_flag() {
    let s = Setup::new();
    let adapter = DeferredAdapter::install(&s);
    adapter.begin();
    adapter.replies.lock().unwrap().extend([
        adapter.response(StopReason::Deferred),
        adapter.response(StopReason::Deferred),
        adapter.response(StopReason::Stop),
    ]);
    let c = s
        .pump
        .drive(factory(&s).create(s.options(DriveMode::Automatic)))
        .unwrap();
    assert!(matches!(
        s.pump
            .drive(c.harness.prompt(prompt()))
            .unwrap()
            .unwrap()
            .outcome,
        RunOutcome::Deferred(_)
    ));
    let events = Arc::new(Listener(Mutex::new(vec![])));
    let watch = s.pump.drive(c.harness.watch()).unwrap();
    watch.start(events.clone());
    for count in 1..=3 {
        s.pump.drive(c.harness.resume()).unwrap().unwrap();
        assert_eq!(adapter.polls.lock().unwrap().len(), count);
        assert_eq!(adapter.polls.lock().unwrap()[count - 1].1.wait, Some(n(0)));
    }
    assert!(
        events
            .0
            .lock()
            .unwrap()
            .iter()
            .filter_map(|e| match e {
                HarnessEvent::RunResume(e) => Some(e),
                _ => None,
            })
            .all(|e| e.recovery.is_none())
    );
    assert!(s.pump.drive(c.harness.get_last_result()).unwrap().is_some());
}
#[test]
fn abort_live_fetch_starts_cancel_before_fetch_release_and_waits_cancel_receipt() {
    let s = Setup::new();
    let adapter = DeferredAdapter::install(&s);
    adapter.begin();
    let fetch = CancellationToken::new();
    let cancel = CancellationToken::new();
    *adapter.hold_fetch.lock().unwrap() = Some(fetch.clone());
    *adapter.hold_cancel.lock().unwrap() = Some(cancel.clone());
    let c = s
        .pump
        .drive(factory(&s).create(s.options(DriveMode::Automatic)))
        .unwrap();
    s.pump.drive(c.harness.prompt(prompt())).unwrap().unwrap();
    let mut resume = Box::pin(c.harness.resume());
    assert!(poll(resume.as_mut()).is_pending());
    tick_until(&s, || !adapter.polls.lock().unwrap().is_empty());
    s.pump.drive(c.harness.abort()).unwrap().unwrap();
    tick_until(&s, || !adapter.cancels.lock().unwrap().is_empty());
    assert!(!adapter.cancels.lock().unwrap()[0].1.is_cancelled());
    assert!(s.pump.drive(c.harness.get_last_result()).unwrap().is_none());
    s.pump.drive(c.harness.abort()).unwrap().unwrap();
    assert_eq!(adapter.cancels.lock().unwrap().len(), 1);
    cancel.cancel();
    fetch.cancel();
    s.pump.drive(resume).unwrap().unwrap();
    assert!(s.pump.drive(c.harness.get_last_result()).unwrap().is_some());
}
struct AbortDuringClockPoll {
    pump: Arc<Pump>,
    lane: Mutex<Option<Arc<dyn AgentLane>>>,
    fired: std::sync::atomic::AtomicBool,
}
impl pi_ai_core::Clock for AbortDuringClockPoll {
    fn now_unix_ms(&self) -> u64 {
        7
    }
    fn sleep<'a>(
        &'a self,
        _: std::time::Duration,
        token: &'a CancellationToken,
    ) -> pi_ai_core::BoxFuture<'a, Result<(), pi_ai_core::Cancelled>> {
        Box::pin(std::future::poll_fn(move |_| {
            if !self.fired.swap(true, std::sync::atomic::Ordering::AcqRel) {
                let lane = self.lane.lock().unwrap().clone().unwrap();
                self.pump.drive(lane.abort()).unwrap().unwrap();
            }
            if token.is_cancelled() {
                Poll::Ready(Err(pi_ai_core::Cancelled))
            } else {
                Poll::Pending
            }
        }))
    }
}
#[test]
fn abort_between_timer_precheck_and_clock_poll_is_not_fault() {
    let s = Setup::new();
    let clock = Arc::new(AbortDuringClockPoll {
        pump: s.pump.clone(),
        lane: Mutex::new(None),
        fired: std::sync::atomic::AtomicBool::new(false),
    });
    s.faux.responses.lock().unwrap().clear();
    let mut retry = assistant(&s.faux.model, StopReason::Error);
    retry.error_message = Some("429 rate limit exceeded".into());
    s.faux.responses.lock().unwrap().push_back(retry);
    let mut options = s.options(DriveMode::Automatic);
    options.retry.enabled = true;
    options.retry.max_retries = pi_ai_core::MaxRetries::new(2).unwrap();
    let f = pi_harness_runtime::owner::RuntimeFactory::new(
        clock.clone(),
        s.pump.clone(),
        s.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    let c = s.pump.drive(f.create(options)).unwrap();
    *clock.lane.lock().unwrap() = Some(c.harness.clone());
    let result = s.pump.drive(c.harness.prompt(prompt()));
    assert!(clock.fired.load(std::sync::atomic::Ordering::Acquire));
    assert!(
        matches!(
            result,
            Ok(Ok(RunSuccess {
                outcome: RunOutcome::AbortedWithAssistant(_),
                ..
            }))
        ),
        "{result:?}"
    );
    assert!(s.pump.drive(c.harness.get_stream_options()).is_ok());
}
#[test]
fn cancel_provider_model_panic_is_best_effort_without_owner_fault() {
    let s = Setup::new();
    let adapter = DeferredAdapter::install(&s);
    adapter.begin();
    let c = s
        .pump
        .drive(factory(&s).create(s.options(DriveMode::Automatic)))
        .unwrap();
    s.pump.drive(c.harness.prompt(prompt())).unwrap().unwrap();
    adapter
        .panic_models
        .store(true, std::sync::atomic::Ordering::Release);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        s.pump.drive(c.harness.abort()).unwrap().unwrap();
        s.pump.drive(c.harness.wait_for_idle())
    }));
    assert!(
        matches!(result, Ok(Ok(()))),
        "cancel model panic must stay best-effort"
    );
    assert!(s.pump.drive(c.harness.get_stream_options()).is_ok());
}
#[test]
fn external_finalization_retires_held_close_only_cancel_task() {
    let s = Setup::new();
    let adapter = DeferredAdapter::install(&s);
    adapter.begin();
    *adapter.hold_cancel.lock().unwrap() = Some(CancellationToken::new());
    let c = s
        .pump
        .drive(factory(&s).create(s.options(DriveMode::Automatic)))
        .unwrap();
    let run = s.pump.drive(c.harness.prompt(prompt())).unwrap().unwrap();
    s.pump.drive(c.harness.abort()).unwrap().unwrap();
    tick_until(&s, || !adapter.cancels.lock().unwrap().is_empty());
    let token = adapter.cancels.lock().unwrap()[0].1.clone();
    assert!(!token.is_cancelled());
    let leaf = s.pump.drive(c.harness.get_leaf_id()).unwrap().unwrap();
    s.pump
        .drive(s.session.commit(Transaction {
            writes: vec![
                Write::DeleteRegister(RegisterDelete {
                    namespace: RegisterNamespace::OperationMeta,
                    key: run.run_id.as_str().into(),
                }),
                Write::DeleteRegister(RegisterDelete {
                    namespace: RegisterNamespace::OperationState,
                    key: run.run_id.as_str().into(),
                }),
                Write::SetRegister(Box::new(RegisterSet::LaneLastResult {
                    key: "main".into(),
                    value: LaneLastResult::RunAborted(RunAbortedLastResult {
                        operation_id: run.run_id,
                        kind: Default::default(),
                        leaf_id: Some(leaf.clone()),
                        final_assistant_entry_id: Some(leaf),
                        outcome: Default::default(),
                    }),
                })),
                Write::SetRegister(Box::new(RegisterSet::LaneState {
                    key: "main".into(),
                    value: LaneState {
                        current_operation_id: None,
                        pending_next_run: vec![],
                    },
                })),
            ],
        }))
        .unwrap();
    // Public mutation supplies an actual drive wake; raw Session has no proactive wake promise.
    s.pump
        .drive(c.harness.next_run(user("wake")))
        .unwrap()
        .unwrap();
    let expected = s.pump.drive(s.store.snapshot()).unwrap();
    for _ in 0..60 {
        s.pump.tick();
    }
    assert!(
        token.is_cancelled(),
        "retired old cancellation still owns its provider task"
    );
    assert_eq!(s.pump.drive(s.store.snapshot()).unwrap(), expected);
    s.pump.drive(c.harness.wait_for_idle()).unwrap();
}
#[test]
fn external_finalization_removes_unreleased_manual_cancel_permission() {
    let s = Setup::new();
    let adapter = DeferredAdapter::install(&s);
    adapter.begin();
    let c = s
        .pump
        .drive(factory(&s).create(s.options(DriveMode::Manual)))
        .unwrap();
    let mut initial = Box::pin(c.harness.prompt(prompt()));
    assert!(poll(initial.as_mut()).is_pending());
    s.pump.tick();
    s.pump.drive(c.harness.run_to_completion()).unwrap();
    let run = s.pump.drive(initial).unwrap().unwrap();
    s.pump.drive(c.harness.abort()).unwrap().unwrap();
    tick_until(&s, || {
        s.pump
            .drive(c.harness.peek_action())
            .unwrap()
            .is_some_and(|a| a.kind == "cancel_deferred")
    });
    assert!(adapter.cancels.lock().unwrap().is_empty());
    let leaf = s.pump.drive(c.harness.get_leaf_id()).unwrap().unwrap();
    s.pump
        .drive(s.session.commit(Transaction {
            writes: vec![
                Write::DeleteRegister(RegisterDelete {
                    namespace: RegisterNamespace::OperationMeta,
                    key: run.run_id.as_str().into(),
                }),
                Write::DeleteRegister(RegisterDelete {
                    namespace: RegisterNamespace::OperationState,
                    key: run.run_id.as_str().into(),
                }),
                Write::SetRegister(Box::new(RegisterSet::LaneLastResult {
                    key: "main".into(),
                    value: LaneLastResult::RunAborted(RunAbortedLastResult {
                        operation_id: run.run_id,
                        kind: Default::default(),
                        leaf_id: Some(leaf.clone()),
                        final_assistant_entry_id: Some(leaf),
                        outcome: Default::default(),
                    }),
                })),
                Write::SetRegister(Box::new(RegisterSet::LaneState {
                    key: "main".into(),
                    value: LaneState {
                        current_operation_id: None,
                        pending_next_run: vec![],
                    },
                })),
            ],
        }))
        .unwrap();
    s.pump
        .drive(c.harness.next_run(user("wake")))
        .unwrap()
        .unwrap();
    for _ in 0..40 {
        s.pump.tick();
    }
    assert!(
        s.pump.drive(c.harness.peek_action()).unwrap().is_none(),
        "old cancel permission survived retirement"
    );
    assert!(adapter.cancels.lock().unwrap().is_empty());
    s.pump.drive(c.harness.wait_for_idle()).unwrap();
}
