mod runtime_r1_r4_support;
mod runtime_r5_r7_support;
use pi_agent_schema::*;
use runtime_r1_r4_support::*;
use runtime_r5_r7_support::*;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::task::Poll;

struct PausedConfigListener {
    release: AtomicBool,
    received: Mutex<Vec<HarnessEvent>>,
}
impl EventListener for PausedConfigListener {
    fn on_event(&self, event: HarnessEvent) -> BoxFuture<'_, Result<(), HandlerError>> {
        Box::pin(async move {
            std::future::poll_fn(|_| {
                if self.release.load(Ordering::SeqCst) {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            })
            .await;
            self.received.lock().unwrap().push(event);
            Ok(())
        })
    }
}

#[test]
fn dropping_watch_closes_a_delivery_held_by_a_pending_listener() {
    let s = Setup::new();
    let c = s
        .pump
        .drive(factory(&s).create(s.options(DriveMode::Automatic)))
        .unwrap();
    let listener = Arc::new(PausedConfigListener {
        release: AtomicBool::new(false),
        received: Mutex::new(Vec::new()),
    });
    let watch = s.pump.drive(c.harness.watch()).unwrap();
    watch.start(listener.clone());
    s.pump
        .drive(c.harness.set_thinking_level(AgentThinkingLevel::High))
        .unwrap();
    drop(watch);
    s.pump
        .drive(c.harness.set_thinking_level(AgentThinkingLevel::Off))
        .unwrap();
    listener.release.store(true, Ordering::SeqCst);
    for _ in 0..8 {
        s.pump.tick();
    }
    let received = listener.received.lock().unwrap();
    assert_eq!(received.len(), 1);
    assert!(
        matches!(&received[0], HarnessEvent::ThinkingConfigUpdate(event) if event.value == AgentThinkingLevel::High)
    );
}
fn rejected_options() -> AgentHarnessStreamOptions {
    AgentHarnessStreamOptions {
        headers: Some(
            RawProviderHeaders::new([("x-unapproved".into(), "value".into())].into())
                .unwrap()
                .classify(&ProviderHeaderPolicy::from_allowlist([
                    "x-unapproved".into()
                ]))
                .unwrap(),
        ),
        ..Default::default()
    }
}
#[test]
fn fault_observation_preserves_captured_snapshot_and_preadmitted_fault_first_snapshot() {
    let s = Setup::new();
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
    let old = s.pump.drive(c.harness.watch()).unwrap();
    let events = Arc::new(Listener(Mutex::new(vec![])));
    old.start(events.clone());
    tracked.pause_config_read.store(true, Ordering::SeqCst);
    let mut preadmitted = Box::pin(c.harness.watch());
    assert!(poll(preadmitted.as_mut()).is_pending());
    assert!(tracked.read_entered.is_cancelled());
    assert!(matches!(
        s.pump
            .drive(c.harness.set_stream_options(rejected_options())),
        Err(HarnessRuntimeError::Fault(_))
    ));
    tracked.release_read.cancel();
    let acquired = s.pump.drive(preadmitted).unwrap();
    assert!(acquired.snapshot().faulted);
    assert!(!old.snapshot().faulted);
    assert!(matches!(
        s.pump.drive(c.harness.watch()),
        Err(HarnessRuntimeError::Fault(_))
    ));
    for _ in 0..8 {
        s.pump.tick();
    }
    assert_eq!(
        events
            .0
            .lock()
            .unwrap()
            .iter()
            .filter(|e| matches!(e, HarnessEvent::Fault(_)))
            .count(),
        1
    );
    assert!(
        s.pump
            .drive(c.harness.set_stream_options(rejected_options()))
            .is_err()
    );
    assert_eq!(
        events
            .0
            .lock()
            .unwrap()
            .iter()
            .filter(|e| matches!(e, HarnessEvent::Fault(_)))
            .count(),
        1
    );
}
#[test]
fn configuration_and_fact_events_are_typed_and_immediate() {
    let s = Setup::new();
    let c = s
        .pump
        .drive(factory(&s).create(s.options(DriveMode::Automatic)))
        .unwrap();
    let events = Arc::new(Listener(Mutex::new(vec![])));
    let w = s.pump.drive(c.harness.watch()).unwrap();
    w.start(events.clone());
    s.pump
        .drive(c.harness.set_thinking_level(AgentThinkingLevel::High))
        .unwrap();
    s.pump
        .drive(c.harness.set_steering_mode(QueueMode::OneAtATime))
        .unwrap();
    s.pump
        .drive(c.harness.session().set_name(Some("named".into())))
        .unwrap();
    assert!(s.pump.drive(c.harness.get_leaf_id()).unwrap().is_none());
    let e = events.0.lock().unwrap();
    assert!(e.iter().any(|e|matches!(e,HarnessEvent::ThinkingConfigUpdate(e) if e.previous==AgentThinkingLevel::Off && e.value==AgentThinkingLevel::High)));
    assert!(e.iter().any(|e|matches!(e,HarnessEvent::GlobalConfigUpdate(e) if e.property==GlobalConfigProperty::SteeringMode)));
    assert!(
        e.iter().any(
            |e| matches!(e,HarnessEvent::NameFactUpdate(e) if e.name.as_deref()==Some("named"))
        )
    );
}
fn watch_close_race(session_watch: bool) {
    {
        let s = Setup::new();
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
        let old = s.pump.drive(c.harness.watch()).unwrap();
        let events = Arc::new(Listener(Mutex::new(vec![])));
        old.start(events.clone());
        tracked.pause_config_read.store(true, Ordering::SeqCst);
        let h = c.harness.clone();
        let mut watch: BoxFuture<'static, Result<bool, HarnessRuntimeError>> = if session_watch {
            Box::pin(async move { Ok(h.watch_session().await?.snapshot().faulted) })
        } else {
            Box::pin(async move { Ok(h.watch().await?.snapshot().faulted) })
        };
        assert!(poll(watch.as_mut()).is_pending());
        assert!(tracked.read_entered.is_cancelled());
        s.pump.drive(c.harness.close()).unwrap();
        tracked.release_read.cancel();
        let result = s.pump.drive(watch);
        assert!(
            matches!(result, Err(HarnessRuntimeError::Closed(_))),
            "session={session_watch}: {result:?}"
        );
        assert!(matches!(
            s.pump.drive(c.harness.get_stream_options()),
            Err(HarnessRuntimeError::Closed(_))
        ));
        assert!(!old.snapshot().faulted);
        assert!(
            !events
                .0
                .lock()
                .unwrap()
                .iter()
                .any(|e| matches!(e, HarnessEvent::Fault(_)))
        );
    }
}

#[test]
fn preadmitted_lane_watch_close_preserves_closed_channel() {
    watch_close_race(false)
}
#[test]
fn preadmitted_session_watch_close_preserves_closed_channel() {
    watch_close_race(true)
}
