mod runtime_r1_r4_support;
mod runtime_r5_r7_support;
use pi_agent_schema::*;
use pi_ai_core::{CancellationToken, StopReason};
use runtime_r1_r4_support::*;
use runtime_r5_r7_support::*;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
struct AbortOnNow {
    pump: Arc<Pump>,
    lane: Mutex<Option<Arc<dyn AgentLane>>>,
    armed: AtomicBool,
    fired: AtomicUsize,
}
impl pi_ai_core::Clock for AbortOnNow {
    fn now_unix_ms(&self) -> u64 {
        if self.armed.swap(false, Ordering::AcqRel) {
            self.fired.fetch_add(1, Ordering::SeqCst);
            let lane = self.lane.lock().unwrap().clone().unwrap();
            self.pump.drive(lane.abort()).unwrap().unwrap();
        }
        7
    }
    fn sleep<'a>(
        &'a self,
        _: std::time::Duration,
        _: &'a CancellationToken,
    ) -> pi_ai_core::BoxFuture<'a, Result<(), pi_ai_core::Cancelled>> {
        Box::pin(std::future::pending())
    }
}
struct ArmDeferred(Arc<AbortOnNow>);
impl EventListener for ArmDeferred {
    fn on_event(&self, e: HarnessEvent) -> BoxFuture<'_, Result<(), HandlerError>> {
        Box::pin(async move {
            if matches!(e,HarnessEvent::EntryAdded(EntryAddedEvent{entry:Entry::Message(MessageEntry{message:AgentMessage::BuiltIn(pi_ai_core::Message::Assistant(m)),..}),..})if m.stop_reason==StopReason::Deferred)
            {
                self.0.armed.store(true, Ordering::Release);
            }
            Ok(())
        })
    }
}
#[test]
fn abort_during_suspension_planning_keeps_reconciliation_owner() {
    let s = Setup::new();
    let adapter = DeferredAdapter::install(&s);
    adapter.begin();
    let clock = Arc::new(AbortOnNow {
        pump: s.pump.clone(),
        lane: Mutex::new(None),
        armed: AtomicBool::new(false),
        fired: AtomicUsize::new(0),
    });
    let f = pi_harness_runtime::owner::RuntimeFactory::new(
        clock.clone(),
        s.pump.clone(),
        s.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    let c = s
        .pump
        .drive(f.create(s.options(DriveMode::Automatic)))
        .unwrap();
    *clock.lane.lock().unwrap() = Some(c.harness.clone());
    let _listener = c.harness.events().on(
        HarnessEventType::EntryAdded,
        Arc::new(ArmDeferred(clock.clone())),
    );
    let result = s.pump.drive(c.harness.prompt(prompt())).unwrap().unwrap();
    assert_eq!(clock.fired.load(Ordering::SeqCst), 1);
    assert!(
        matches!(result.outcome, RunOutcome::AbortedWithAssistant(_)),
        "stale suspension escaped cancellation: {:?}",
        result.outcome
    );
    assert!(s.pump.drive(c.harness.get_last_result()).unwrap().is_some());
}
#[test]
fn cancelled_unstarted_poll_uses_hydrated_source_api_without_provider() {
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
    s.pump.drive(initial).unwrap().unwrap();
    let mut resume = Box::pin(c.harness.resume());
    assert!(poll(resume.as_mut()).is_pending());
    manual_until(
        &s,
        c.harness.as_ref(),
        || matches!(current(&s),Some(OperationState::Run(r))if matches!(r.phase,RunPhase::Deferred(DeferredPhase{deferred:Deferred::EffectPending{..},..}))),
    );
    s.pump
        .drive(s.registry.delete_provider(&s.faux.model.provider))
        .unwrap();
    s.pump.drive(c.harness.abort()).unwrap().unwrap();
    s.pump.drive(c.harness.run_to_completion()).unwrap();
    s.pump.drive(resume).unwrap().unwrap();
    let leaf = s.pump.drive(c.harness.get_leaf_id()).unwrap().unwrap();
    let Entry::Message(e) = s
        .pump
        .drive(c.harness.session().get_entry(leaf))
        .unwrap()
        .unwrap()
    else {
        panic!()
    };
    let AgentMessage::BuiltIn(pi_ai_core::Message::Assistant(message)) = e.message else {
        panic!()
    };
    assert_eq!(message.api, s.faux.model.api);
    assert_eq!(message.usage, pi_ai_core::Usage::zero());
    assert!(adapter.polls.lock().unwrap().is_empty());
}
#[test]
fn resume_precheck_observes_external_terminal_instead_of_missing_identity() {
    let s = Setup::new();
    let adapter = DeferredAdapter::install(&s);
    adapter.begin();
    let c = s
        .pump
        .drive(factory(&s).create(s.options(DriveMode::Automatic)))
        .unwrap();
    let run = s.pump.drive(c.harness.prompt(prompt())).unwrap().unwrap();
    let leaf = s.pump.drive(c.harness.get_leaf_id()).unwrap().unwrap();
    let pump = s.pump.clone();
    let session = s.session.clone();
    let op = run.run_id;
    *adapter.models_action.lock().unwrap() = Some(Box::new(move || {
        pump.drive(session.commit(Transaction {
            writes: vec![
                Write::DeleteRegister(RegisterDelete {
                    namespace: RegisterNamespace::OperationMeta,
                    key: op.as_str().into(),
                }),
                Write::DeleteRegister(RegisterDelete {
                    namespace: RegisterNamespace::OperationState,
                    key: op.as_str().into(),
                }),
                Write::SetRegister(Box::new(RegisterSet::LaneLastResult {
                    key: "main".into(),
                    value: LaneLastResult::RunAborted(RunAbortedLastResult {
                        operation_id: op.clone(),
                        kind: Default::default(),
                        leaf_id: Some(leaf.clone()),
                        final_assistant_entry_id: Some(leaf.clone()),
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
    }));
    adapter.missing_models.store(true, Ordering::Release);
    let result = s.pump.drive(c.harness.resume()).unwrap();
    assert!(
        result.is_ok(),
        "already-discovered external terminal returned {result:?}"
    );
    assert_eq!(
        serde_json::to_value(result.unwrap()).unwrap()["kind"],
        "aborted"
    );
    assert!(adapter.polls.lock().unwrap().is_empty());
}
struct ResumeAtSuspension {
    pump: Arc<Pump>,
    lane: Mutex<Option<Arc<dyn AgentLane>>>,
    started: AtomicBool,
    waiter: Mutex<Option<BoxFuture<'static, ResumeCall>>>,
    adapter: Arc<DeferredAdapter>,
}
impl EventListener for ResumeAtSuspension {
    fn on_event(&self, event: HarnessEvent) -> BoxFuture<'_, Result<(), HandlerError>> {
        Box::pin(async move {
            if matches!(event, HarnessEvent::RunSuspendDeferred(_))
                && !self.started.swap(true, Ordering::AcqRel)
            {
                let lane = self.lane.lock().unwrap().clone().unwrap();
                let mut waiter: BoxFuture<'static, ResumeCall> =
                    Box::pin(async move { lane.resume().await });
                assert!(poll(waiter.as_mut()).is_pending());
                for _ in 0..100 {
                    if !self.adapter.polls.lock().unwrap().is_empty() {
                        break;
                    }
                    self.pump.tick();
                }
                assert_eq!(self.adapter.polls.lock().unwrap().len(), 1);
                *self.waiter.lock().unwrap() = Some(waiter);
            }
            Ok(())
        })
    }
}
#[test]
fn suspension_listener_can_resume_same_operation_without_old_guard_erasing_control() {
    let s = Setup::new();
    let adapter = DeferredAdapter::install(&s);
    adapter.begin();
    let release = CancellationToken::new();
    *adapter.hold_fetch.lock().unwrap() = Some(release.clone());
    let c = s
        .pump
        .drive(factory(&s).create(s.options(DriveMode::Automatic)))
        .unwrap();
    let listener = Arc::new(ResumeAtSuspension {
        pump: s.pump.clone(),
        lane: Mutex::new(Some(c.harness.clone())),
        started: AtomicBool::new(false),
        waiter: Mutex::new(None),
        adapter: adapter.clone(),
    });
    let _sub = c
        .harness
        .events()
        .on(HarnessEventType::RunSuspend, listener.clone());
    let initial = s.pump.drive(c.harness.prompt(prompt())).unwrap().unwrap();
    assert!(matches!(initial.outcome, RunOutcome::Deferred(_)));
    let snapshot = s.pump.drive(c.harness.watch()).unwrap().snapshot();
    assert_eq!(
        snapshot.operation.unwrap().status,
        OperationSnapshotStatus::Running
    );
    let waiter = listener.waiter.lock().unwrap().take().unwrap();
    s.pump.drive(c.harness.abort()).unwrap().unwrap();
    release.cancel();
    s.pump.drive(waiter).unwrap().unwrap();
    assert!(s.pump.drive(c.harness.get_last_result()).unwrap().is_some());
}

// Poll the queued abort synchronously when the lane owner releases its guard.
// The task is removed from its mutex before polling, so wake callbacks reenter
// the public runtime without holding a fixture lock.
struct WakeQueuedAbort {
    task: Mutex<Option<BoxFuture<'static, ()>>>,
    wakes: AtomicUsize,
}
impl WakeQueuedAbort {
    fn tick(self: &Arc<Self>) {
        let task = self.task.lock().unwrap().take();
        if let Some(mut task) = task {
            let waker = std::task::Waker::from(self.clone());
            if task
                .as_mut()
                .poll(&mut std::task::Context::from_waker(&waker))
                .is_pending()
            {
                *self.task.lock().unwrap() = Some(task);
            }
        }
    }
}
impl std::task::Wake for WakeQueuedAbort {
    fn wake(self: Arc<Self>) {
        self.wakes.fetch_add(1, Ordering::SeqCst);
        self.tick();
    }
}
#[test]
fn missing_identity_rejection_hands_queued_abort_to_a_reconciliation_drive() {
    let s = Setup::new();
    let adapter = DeferredAdapter::install(&s);
    adapter.begin();
    let tracked = Arc::new(tracking::TrackedStorage::new(s.store.clone()));
    let session = tracked_session(&s, tracked.clone());
    let f = pi_harness_runtime::owner::RuntimeFactory::new(
        s.time.clone(),
        s.pump.clone(),
        s.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    let c = s
        .pump
        .drive(f.create(s.options_for(session, DriveMode::Automatic)))
        .unwrap();
    s.pump.drive(c.harness.prompt(prompt())).unwrap().unwrap();
    for _ in 0..10 {
        s.pump.tick();
    }
    assert!(s.pump.tasks.lock().unwrap().is_empty());
    adapter.missing_models.store(true, Ordering::Release);
    let storage = tracked.clone();
    *adapter.models_action.lock().unwrap() = Some(Box::new(move || {
        storage.pause_config_read.store(true, Ordering::Release);
    }));
    let mut resume = Box::pin(c.harness.resume());
    assert!(poll(resume.as_mut()).is_pending());
    tick_until(&s, || tracked.read_entered.is_cancelled());
    let mut abort = Box::pin(c.harness.abort());
    assert!(poll(abort.as_mut()).is_pending());
    let abort_task = s.pump.tasks.lock().unwrap().pop().unwrap();
    let queued = Arc::new(WakeQueuedAbort {
        task: Mutex::new(Some(abort_task)),
        wakes: AtomicUsize::new(0),
    });
    queued.tick();
    assert!(queued.task.lock().unwrap().is_some());
    tracked.release_read.cancel();
    let result = s.pump.drive(resume).unwrap();
    assert!(matches!(
        result,
        Err(ResumeExpectedError::MissingIdentities(_))
    ));
    assert!(queued.wakes.load(Ordering::SeqCst) > 0);
    assert!(queued.task.lock().unwrap().is_none());
    s.pump.drive(abort).unwrap().unwrap();
    for _ in 0..100 {
        s.pump.tick();
    }
    let result = s.pump.drive(c.harness.get_last_result()).unwrap();
    assert!(
        result.is_some(),
        "missing-identity rejection orphaned an accepted abort"
    );
    assert_eq!(
        serde_json::to_value(result.unwrap()).unwrap()["outcome"],
        "aborted"
    );
    assert!(adapter.polls.lock().unwrap().is_empty());
}

#[test]
fn abort_before_missing_identity_reload_keeps_the_existing_reconciliation_drive() {
    let s = Setup::new();
    let adapter = DeferredAdapter::install(&s);
    adapter.begin();
    let c = s
        .pump
        .drive(factory(&s).create(s.options(DriveMode::Automatic)))
        .unwrap();
    s.pump.drive(c.harness.prompt(prompt())).unwrap().unwrap();
    let lane = c.harness.clone();
    let pump = s.pump.clone();
    let fired = Arc::new(AtomicUsize::new(0));
    let count = fired.clone();
    *adapter.models_action.lock().unwrap() = Some(Box::new(move || {
        pump.drive(lane.abort()).unwrap().unwrap();
        count.fetch_add(1, Ordering::SeqCst);
    }));
    adapter.missing_models.store(true, Ordering::Release);
    let result = s.pump.drive(c.harness.resume()).unwrap().unwrap();
    assert_eq!(fired.load(Ordering::SeqCst), 1);
    assert_eq!(serde_json::to_value(result).unwrap()["kind"], "aborted");
    assert!(s.pump.drive(c.harness.get_last_result()).unwrap().is_some());
    assert!(adapter.polls.lock().unwrap().is_empty());
}
