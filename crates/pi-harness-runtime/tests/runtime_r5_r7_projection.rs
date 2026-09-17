mod runtime_r1_r4_support;
mod runtime_r5_r7_support;
use pi_agent_schema::*;
use pi_ai_core::{CancellationToken, Presence};
use runtime_r1_r4_support::*;
use runtime_r5_r7_support::*;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
struct MetadataProjector {
    seen: Mutex<Vec<CustomEntry>>,
    lane: Mutex<Option<Arc<dyn AgentLane>>>,
    other: Mutex<Option<Arc<dyn AgentLane>>>,
    first: AtomicBool,
    time: Arc<Time>,
}
impl EntryProjector for MetadataProjector {
    fn project(
        &self,
        entry: CustomEntry,
    ) -> BoxFuture<'_, Result<Option<Vec<AgentMessage>>, HarnessRuntimeError>> {
        Box::pin(async move {
            self.seen.lock().unwrap().push(entry.clone());
            if !self.first.swap(true, Ordering::AcqRel) {
                let lane = self.lane.lock().unwrap().clone().unwrap();
                let other = self.other.lock().unwrap().clone().unwrap();
                lane.set_thinking_level(AgentThinkingLevel::High).await?;
                self.time.0.store(70, Ordering::SeqCst);
                other
                    .session()
                    .append_message(user("other lane publication"))
                    .await
                    .unwrap();
            }
            Ok((entry.timestamp == n(70)).then(|| {
                vec![user(&format!(
                    "metadata {} {}",
                    entry.seq.get(),
                    entry.timestamp.get()
                ))]
            }))
        })
    }
}
#[test]
fn actual_custom_metadata_reprojects_after_other_lane_write_and_setter_reentry() {
    let s = Setup::new();
    let tracked = Arc::new(tracking::TrackedStorage::new(s.store.clone()));
    let session = tracked_session(&s, tracked.clone());
    let projector = Arc::new(MetadataProjector {
        seen: Mutex::new(vec![]),
        lane: Mutex::new(None),
        other: Mutex::new(None),
        first: AtomicBool::new(false),
        time: s.time.clone(),
    });
    let mut options = s.options_for(session, DriveMode::Automatic);
    options
        .entry_projectors
        .insert(CustomType::new("note").unwrap(), projector.clone());
    let f = pi_harness_runtime::owner::RuntimeFactory::new(
        s.time.clone(),
        s.pump.clone(),
        s.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    let c = s.pump.drive(f.create(options)).unwrap();
    *projector.lane.lock().unwrap() = Some(c.harness.clone());
    let other = s
        .pump
        .drive(c.harness.create_lane(LaneId::new("other").unwrap(), None))
        .unwrap()
        .unwrap();
    *projector.other.lock().unwrap() = Some(other);
    let events = Arc::new(Listener(Mutex::new(vec![])));
    let watch = s.pump.drive(c.harness.watch()).unwrap();
    watch.start(events.clone());
    let hold = CancellationToken::new();
    *s.faux.hold.lock().unwrap() = Some(hold.clone());
    let mut run = Box::pin(c.harness.prompt(prompt()));
    assert!(poll(run.as_mut()).is_pending());
    tick_until(&s, || !s.faux.requests.lock().unwrap().is_empty());
    let id = s
        .pump
        .drive(
            c.harness
                .session()
                .append_custom_entry(CustomType::new("note").unwrap(), Presence::Null),
        )
        .unwrap();
    assert!(
        s.pump
            .drive(c.harness.session().get_entry(id.clone()))
            .unwrap()
            .is_none()
    );
    hold.cancel();
    s.pump.drive(run).unwrap().unwrap();
    let Entry::Custom(committed) = s
        .pump
        .drive(c.harness.session().get_entry(id.clone()))
        .unwrap()
        .unwrap()
    else {
        panic!()
    };
    let seen = projector.seen.lock().unwrap();
    assert!(seen.len() >= 2);
    assert_eq!(seen[0].timestamp, n(7));
    assert_ne!(seen[0].seq, committed.seq);
    assert_eq!(
        seen[1], committed,
        "projector must observe exactly the metadata that commits"
    );
    assert_eq!(committed.timestamp, n(70));
    assert_eq!(
        s.faux.requests.lock().unwrap().len(),
        2,
        "second exact projection must require an assistant"
    );
    let transactions = tracked.transactions.lock().unwrap();
    let applied = transactions
        .iter()
        .filter(|tx| {
            tx.writes
                .iter()
                .any(|w| matches!(w,Write::Entry(NewEntry::Custom(e)) if e.id==id))
        })
        .collect::<Vec<_>>();
    assert_eq!(applied.len(), 1);
    let tx = applied[0];
    assert!(matches!(&tx.writes[0],Write::Entry(NewEntry::Custom(e))if e.id==id));
    assert!(
        matches!(&tx.writes[1],Write::DeleteRegister(r)if r.namespace==RegisterNamespace::PendingEntry && r.key==id.as_str())
    );
    assert!(
        matches!(&tx.writes[2],Write::SetRegister(r)if matches!(r.as_ref(),RegisterSet::LaneLeaf{..}))
    );
    assert!(
        matches!(&tx.writes[3],Write::SetRegister(r)if matches!(r.as_ref(),RegisterSet::OperationState{value,..}if matches!(value.as_ref(),OperationState::Run(run) if matches!(&run.phase,RunPhase::Checkpoint(p) if p.skip_inbox_once==Some(true)))))
    );
    let events = events.0.lock().unwrap();
    let pending = events
        .iter()
        .position(|e| matches!(e,HarnessEvent::WritePending(e)if e.entry_id==id))
        .unwrap();
    let added=events.iter().position(|e|matches!(e,HarnessEvent::EntryAdded(e)if matches!(&e.entry,Entry::Custom(row)if row==&committed))).unwrap();
    assert!(pending < added);
}
#[test]
fn physical_reopen_after_one_consumption_starts_generation_before_next_queue_item() {
    let s = Setup::new();
    let mut options = s.options(DriveMode::Manual);
    options.steering_mode = QueueMode::OneAtATime;
    let c = s.pump.drive(factory(&s).create(options)).unwrap();
    let mut prompt_call = Box::pin(c.harness.prompt(prompt()));
    assert!(poll(prompt_call.as_mut()).is_pending());
    manual_until(
        &s,
        c.harness.as_ref(),
        || matches!(current(&s),Some(OperationState::Run(r))if matches!(r.phase,RunPhase::Assistant(AssistantPhase{generation:Generation::EffectPending{..},..}))),
    );
    let a = s
        .pump
        .drive(c.harness.steer(user("a")))
        .unwrap()
        .unwrap()
        .entry_id;
    let b = s
        .pump
        .drive(c.harness.steer(user("b")))
        .unwrap()
        .unwrap()
        .entry_id;
    s.pump
        .drive(c.harness.set_steering_mode(QueueMode::All))
        .unwrap();
    manual_until(&s, c.harness.as_ref(), || {
        s.pump
            .drive(c.harness.session().get_entry(a.clone()))
            .unwrap()
            .is_some()
    });
    assert!(
        s.pump
            .drive(c.harness.session().get_entry(b.clone()))
            .unwrap()
            .is_none()
    );
    assert_eq!(s.faux.requests.lock().unwrap().len(), 1);
    s.pump.drive(c.harness.close()).unwrap();
    assert!(s.pump.drive(prompt_call).is_err());
    let session = reopen(&s);
    let restored = s
        .pump
        .drive(factory(&s).create(s.options_for(session.clone(), DriveMode::Manual)))
        .unwrap();
    let mut resume = Box::pin(restored.harness.resume());
    assert!(poll(resume.as_mut()).is_pending());
    manual_until(&s, restored.harness.as_ref(), || {
        s.faux.requests.lock().unwrap().len() == 2
    });
    assert!(
        s.pump
            .drive(restored.harness.session().get_entry(b.clone()))
            .unwrap()
            .is_none()
    );
    assert!(
        s.pump
            .drive(session.get_register(RegisterNamespace::PendingEntry, b.as_str()))
            .unwrap()
            .is_some()
    );
    s.pump.drive(restored.harness.run_to_completion()).unwrap();
    s.pump.drive(resume).unwrap().unwrap();
    assert!(
        s.pump
            .drive(restored.harness.session().get_entry(a))
            .unwrap()
            .is_some()
    );
    assert!(
        s.pump
            .drive(restored.harness.session().get_entry(b))
            .unwrap()
            .is_some()
    );
    assert_eq!(s.faux.requests.lock().unwrap().len(), 3);
}
#[test]
fn public_queue_consumer_reloads_changed_pending_payload_before_placement() {
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
    let hold = CancellationToken::new();
    *s.faux.hold.lock().unwrap() = Some(hold.clone());
    let mut run = Box::pin(c.harness.prompt(prompt()));
    assert!(poll(run.as_mut()).is_pending());
    tick_until(&s, || !s.faux.requests.lock().unwrap().is_empty());
    let id = s
        .pump
        .drive(c.harness.steer(user("original")))
        .unwrap()
        .unwrap()
        .entry_id;
    let replacement:PendingEntry=serde_json::from_value(serde_json::json!({"type":"message","payload":{"role":"user","content":"replacement","timestamp":7}})).unwrap();
    *tracked.replace_pending.lock().unwrap() = Some((id.clone(), replacement));
    hold.cancel();
    s.pump.drive(run).unwrap().unwrap();
    assert!(
        tracked.stale_rejections.load(Ordering::SeqCst) > 0,
        "pending token drift must reject the actual first consumer commit"
    );
    let Entry::Message(entry) = s
        .pump
        .drive(c.harness.session().get_entry(id))
        .unwrap()
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(entry.message, user("replacement"));
}
struct HeldProjector {
    entered: CancellationToken,
    release: CancellationToken,
}
impl EntryProjector for HeldProjector {
    fn project(
        &self,
        _: CustomEntry,
    ) -> BoxFuture<'_, Result<Option<Vec<AgentMessage>>, HarnessRuntimeError>> {
        Box::pin(async move {
            self.entered.cancel();
            self.release.cancelled().await;
            Ok(Some(vec![user("projected")]))
        })
    }
}
#[test]
fn close_retires_pending_custom_projection_without_partial_placement() {
    let s = Setup::new();
    let projector = Arc::new(HeldProjector {
        entered: CancellationToken::new(),
        release: CancellationToken::new(),
    });
    let mut options = s.options(DriveMode::Automatic);
    options
        .entry_projectors
        .insert(CustomType::new("held").unwrap(), projector.clone());
    let c = s.pump.drive(factory(&s).create(options)).unwrap();
    let hold = CancellationToken::new();
    *s.faux.hold.lock().unwrap() = Some(hold.clone());
    let mut run = Box::pin(c.harness.prompt(prompt()));
    assert!(poll(run.as_mut()).is_pending());
    tick_until(&s, || !s.faux.requests.lock().unwrap().is_empty());
    let id = s
        .pump
        .drive(
            c.harness
                .session()
                .append_custom_entry(CustomType::new("held").unwrap(), Presence::Missing),
        )
        .unwrap();
    hold.cancel();
    tick_until(&s, || projector.entered.is_cancelled());
    s.pump.drive(c.harness.close()).unwrap();
    for _ in 0..20 {
        s.pump.tick();
    }
    let retired = matches!(
        poll(run.as_mut()),
        std::task::Poll::Ready(Err(HarnessRuntimeError::Closed(_)))
    );
    projector.release.cancel();
    for _ in 0..20 {
        s.pump.tick();
    }
    assert!(
        retired,
        "close left the accepted run awaiting an uncooperative projector"
    );
    let session = reopen(&s);
    assert!(
        s.pump
            .drive(session.get_entries(std::slice::from_ref(&id)))
            .unwrap()
            .is_empty()
    );
    assert!(
        s.pump
            .drive(session.get_register(RegisterNamespace::PendingEntry, id.as_str()))
            .unwrap()
            .is_some()
    );
}
