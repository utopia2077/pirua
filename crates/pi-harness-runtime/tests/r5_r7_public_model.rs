//! Finite public histories; no direct writes to manufacture accepted operations.
mod r5_r7_public_model_support;
mod runtime_r1_r4_support;
#[allow(dead_code)]
mod runtime_telemetry_support;
use pi_agent_schema::*;
use pi_ai_core::CancellationToken;
use r5_r7_public_model_support::*;
use runtime_r1_r4_support::{n, poll as poll_future};

#[test]
fn public_r5_next_run_cancel_and_prompt_order() {
    for mode in [DriveMode::Automatic, DriveMode::Manual] {
        let mut w = World::new(mode, QueueMode::All);
        let h = w.attach(true);
        let mut oracle = QueueReference::default();
        let a = w
            .pump
            .drive(h.next_run(user("next-a")))
            .expect("R5 nextRun must be implemented")
            .unwrap()
            .entry_id;
        oracle.admit(a.clone(), "next-a");
        let b = w
            .pump
            .drive(h.next_run(user("next-b")))
            .unwrap()
            .unwrap()
            .entry_id;
        oracle.admit(b.clone(), "next-b");
        oracle.check(&w.observed());
        assert!(w.adapter.calls.lock().unwrap().streams.is_empty());
        assert_eq!(
            w.pump.drive(h.cancel_queued(b.clone())).unwrap().unwrap(),
            CancelQueuedOutcome::Cancelled
        );
        oracle.cancel(&b);
        assert_eq!(
            w.pump.drive(h.cancel_queued(b)).unwrap().unwrap(),
            CancelQueuedOutcome::NotFound
        );
        w.drive(&h, h.prompt(prompt("prompt"))).unwrap().unwrap();
        oracle.consume(std::slice::from_ref(&a));
        oracle.check(&w.observed());
        assert_eq!(
            w.pump.drive(h.cancel_queued(a)).unwrap().unwrap(),
            CancelQueuedOutcome::AlreadyConsumed
        );
        assert!(w.observed().run().is_none());
        assert!(w.write_observations() > 0);
        w.pump.drive(h.close()).unwrap();
    }
}
#[test]
fn public_r5_bounded_queue_abort_interleavings() {
    for mode in [QueueMode::All, QueueMode::OneAtATime] {
        for consume_first in [false, true] {
            let case = format!(
                "r5-{:?}-{}",
                mode,
                if consume_first {
                    "consume-first"
                } else {
                    "abort-first"
                }
            );
            let mut w = World::new(DriveMode::Manual, mode);
            let hold = CancellationToken::new();
            let mut reply = Reply::stop();
            reply.hold = Some(hold.clone());
            w.adapter.initial.lock().unwrap().push_back(reply);
            let h = w.attach(true);
            let hp = h.clone();
            let mut call = Box::pin(async move { hp.prompt(prompt("start")).await });
            assert!(poll_future(call.as_mut()).is_pending());
            w.until(&h, |_| !w.adapter.calls.lock().unwrap().streams.is_empty());
            let mut oracle = QueueReference::default();
            let a = w
                .pump
                .drive(h.steer(user("steer-a")))
                .expect(&case)
                .unwrap()
                .entry_id;
            oracle.admit(a.clone(), "steer-a");
            let b = w
                .pump
                .drive(h.steer(user("steer-b")))
                .unwrap()
                .unwrap()
                .entry_id;
            oracle.admit(b.clone(), "steer-b");
            let c = w
                .pump
                .drive(h.follow_up(user("follow-c")))
                .unwrap()
                .unwrap()
                .entry_id;
            oracle.admit(c.clone(), "follow-c");
            let next = w
                .pump
                .drive(h.next_run(user("next")))
                .unwrap()
                .unwrap()
                .entry_id;
            oracle.admit(next.clone(), "next");
            oracle.check(&w.observed());
            if consume_first {
                hold.cancel();
                w.until(&h, |view| view.entry_ids().contains(&a));
                let consumed = if mode == QueueMode::All {
                    vec![a.clone(), b.clone()]
                } else {
                    vec![a.clone()]
                };
                oracle.consume(&consumed);
                oracle.check(&w.observed());
                assert_eq!(
                    w.pump.drive(h.cancel_queued(a.clone())).unwrap().unwrap(),
                    CancelQueuedOutcome::AlreadyConsumed
                );
            } else {
                assert_eq!(
                    w.pump.drive(h.cancel_queued(b.clone())).unwrap().unwrap(),
                    CancelQueuedOutcome::Cancelled
                );
                oracle.cancel(&b);
            }
            let aborted = w.pump.drive(h.abort()).expect(&case).unwrap();
            let repeat = w.pump.drive(h.abort()).unwrap().unwrap();
            assert_eq!(repeat, aborted);
            oracle.check(&w.observed());
            assert_eq!(
                w.pump.drive(h.cancel_queued(c.clone())).unwrap().unwrap(),
                CancelQueuedOutcome::NotFound
            );
            assert!(
                w.observed().pending_ids().contains(&c),
                "abort-drained payload must survive marker"
            );
            hold.cancel();
            w.drive(&h, call).unwrap().unwrap();
            w.finish_idle(&h);
            let view = w.observed();
            assert!(view.run().is_none());
            assert_eq!(view.pending_ids(), [next].into());
            assert!(matches!(
                w.pump.drive(h.get_last_result()).unwrap(),
                Some(LaneLastResult::RunAborted(_))
            ));
            w.pump.drive(h.close()).unwrap();
        }
    }
}
#[test]
fn public_r7_one_fetch_per_resume_and_newest_source_cancel() {
    for polls in 0..=2 {
        for abort in [false, true] {
            let mut w = World::new(DriveMode::Automatic, QueueMode::All);
            w.adapter
                .initial
                .lock()
                .unwrap()
                .push_back(Reply::pending());
            for _ in 0..polls {
                w.adapter.polls.lock().unwrap().push_back(Reply::pending());
            }
            if !abort {
                w.adapter.polls.lock().unwrap().push_back(Reply::stop());
            }
            let h = w.attach(true);
            let result = w.drive(&h, h.prompt(prompt("defer"))).unwrap().unwrap();
            assert!(matches!(result.outcome, RunOutcome::Deferred(_)));
            let initial = w.observed();
            let Some(Deferred::Suspended {
                source_entry_id, ..
            }) = initial.deferred()
            else {
                panic!()
            };
            let mut source = source_entry_id.clone();
            for k in 1..=polls {
                w.drive(&h, h.resume())
                    .expect("R7 resume must redeem exactly once")
                    .unwrap();
                let view = w.observed();
                let Some(Deferred::Suspended {
                    source_entry_id,
                    poll,
                    ..
                }) = view.deferred()
                else {
                    panic!()
                };
                assert_eq!(*poll, n(k));
                assert_ne!(*source_entry_id, source);
                source = source_entry_id.clone();
                let calls = w.adapter.calls.lock().unwrap();
                assert_eq!(calls.fetches.len(), k as usize);
                assert!(calls.fetches.iter().all(|h| h == &handle()));
                assert!(calls.wait.iter().all(|wait| *wait == Some(n(0))));
            }
            if abort {
                w.pump
                    .drive(h.abort())
                    .expect("R7 suspended abort must reconcile")
                    .unwrap();
                w.finish_idle(&h);
                let calls = w.adapter.calls.lock().unwrap();
                assert_eq!(calls.cancels, vec![handle()]);
                assert_eq!(calls.fetches.len(), polls as usize);
                assert_eq!(calls.cancel_was_signalled, vec![false]);
                drop(calls);
                assert!(matches!(
                    w.pump.drive(h.get_last_result()).unwrap(),
                    Some(LaneLastResult::RunAborted(_))
                ));
            } else {
                w.drive(&h, h.resume()).expect("R7 ready poll").unwrap();
                assert_eq!(
                    w.adapter.calls.lock().unwrap().fetches.len(),
                    polls as usize + 1
                );
                assert!(matches!(
                    w.pump.drive(h.get_last_result()).unwrap(),
                    Some(LaneLastResult::RunCompletedAssistant(_))
                ));
            }
            assert!(w.observed().run().is_none());
            w.pump.drive(h.close()).unwrap();
        }
    }
}
#[test]
fn public_r7_completed_prefix_close_reopen_matches_uninterrupted_observable_state() {
    let run = |reopen: bool| {
        let mut w = World::new(DriveMode::Automatic, QueueMode::All);
        w.adapter
            .initial
            .lock()
            .unwrap()
            .push_back(Reply::pending());
        w.adapter
            .polls
            .lock()
            .unwrap()
            .extend([Reply::pending(), Reply::stop()]);
        let mut h = w.attach(true);
        w.drive(&h, h.prompt(prompt("same"))).unwrap().unwrap();
        w.drive(&h, h.resume()).unwrap().unwrap();
        let prefix = w.observed();
        assert!(matches!(prefix.deferred(),Some(Deferred::Suspended{poll,..}) if *poll==n(1)));
        if reopen {
            let before = w.session.read_stamp();
            let stamp = w.pump.drive(before).unwrap();
            h = w.reopen(h);
            assert_ne!(stamp, w.pump.drive(w.session.read_stamp()).unwrap());
            assert_eq!(
                prefix,
                w.observed(),
                "reopen must continue the actual completed prefix"
            );
        }
        w.drive(&h, h.resume()).unwrap().unwrap();
        let final_view = w.observed();
        assert!(final_view.run().is_none());
        assert_eq!(w.adapter.calls.lock().unwrap().fetches.len(), 2);
        w.pump.drive(h.close()).unwrap();
        (prefix, final_view)
    };
    let direct = run(false);
    let reopened = run(true);
    assert_eq!(direct, reopened);
}
#[test]
fn public_r7_unknown_poll_reopen_reserves_fresh_ids_at_same_poll() {
    let mut w = World::new(DriveMode::Manual, QueueMode::All);
    w.adapter
        .initial
        .lock()
        .unwrap()
        .push_back(Reply::pending());
    w.adapter.polls.lock().unwrap().push_back(Reply::stop());
    let mut h = w.attach(true);
    w.drive(&h, h.prompt(prompt("unknown"))).unwrap().unwrap();
    let hr = h.clone();
    let mut resume = Box::pin(async move { hr.resume().await });
    assert!(poll_future(resume.as_mut()).is_pending());
    w.until(&h, |view| {
        matches!(view.deferred(), Some(Deferred::EffectPending { .. }))
    });
    let prefix = w.observed();
    let Some(Deferred::EffectPending {
        poll,
        source_entry_id,
        response_entry_id,
        usage_id,
        ..
    }) = prefix.deferred()
    else {
        panic!()
    };
    let old = (
        poll.get(),
        source_entry_id.clone(),
        response_entry_id.clone(),
        usage_id.clone(),
    );
    assert_eq!(
        w.adapter.calls.lock().unwrap().fetches.len(),
        0,
        "stop after intent before external dispatch"
    );
    h = w.reopen(h);
    drop(resume);
    assert_eq!(prefix, w.observed());
    let hr = h.clone();
    let mut resumed = Box::pin(async move { hr.resume().await });
    assert!(poll_future(resumed.as_mut()).is_pending());
    w.until(&h,|view|matches!(view.deferred(),Some(Deferred::EffectPending{response_entry_id,..}) if response_entry_id!=&old.2));
    let new = w.observed();
    let Some(Deferred::EffectPending {
        poll,
        source_entry_id,
        response_entry_id,
        usage_id,
        ..
    }) = new.deferred()
    else {
        panic!()
    };
    assert_eq!(poll.get(), old.0);
    assert_eq!(source_entry_id, &old.1);
    assert_ne!(response_entry_id, &old.2);
    assert_ne!(usage_id, &old.3);
    w.drive(&h, resumed).unwrap().unwrap();
    let final_view = w.observed();
    assert!(!final_view.entry_ids().contains(&old.2));
    assert!(!final_view.usage.iter().any(|u| u.id == old.3));
    assert_eq!(w.adapter.calls.lock().unwrap().fetches.len(), 1);
    assert!(final_view.run().is_none());
    w.pump.drive(h.close()).unwrap();
}
#[test]
fn public_r5_one_item_consumption_reopens_before_next_item() {
    let mut w = World::new(DriveMode::Manual, QueueMode::OneAtATime);
    let hold = CancellationToken::new();
    let mut reply = Reply::stop();
    reply.hold = Some(hold.clone());
    w.adapter.initial.lock().unwrap().push_back(reply);
    let mut h = w.attach(true);
    let hp = h.clone();
    let mut prompt_call = Box::pin(async move { hp.prompt(prompt("skip-prefix")).await });
    assert!(poll_future(prompt_call.as_mut()).is_pending());
    w.until(&h, |_| !w.adapter.calls.lock().unwrap().streams.is_empty());
    let a = w
        .pump
        .drive(h.steer(user("first")))
        .unwrap()
        .unwrap()
        .entry_id;
    let b = w
        .pump
        .drive(h.steer(user("second")))
        .unwrap()
        .unwrap()
        .entry_id;
    hold.cancel();
    w.until(&h, |v| v.entry_ids().contains(&a));
    let prefix = w.observed();
    assert!(prefix.pending_ids().contains(&b));
    assert!(
        matches!(prefix.run().map(|r|&r.phase),Some(RunPhase::Checkpoint(p)) if p.skip_inbox_once==Some(true))
    );
    h = w.reopen(h);
    drop(prompt_call);
    assert_eq!(prefix, w.observed());
    let hr = h.clone();
    let mut resumed = Box::pin(async move { hr.resume().await });
    assert!(poll_future(resumed.as_mut()).is_pending());
    w.until(&h, |view| {
        assert!(
            view.pending_ids().contains(&b),
            "second queue item consumed before required generation"
        );
        w.adapter.calls.lock().unwrap().streams.len() >= 2
    });
    w.drive(&h, resumed).unwrap().unwrap();
    let final_view = w.observed();
    assert!(final_view.entry_ids().contains(&a));
    assert!(final_view.entry_ids().contains(&b));
    assert!(final_view.pending_ids().is_empty());
    assert!(final_view.run().is_none());
    w.pump.drive(h.close()).unwrap();
}

#[test]
fn public_r5_steer_then_follow_up_consumption_preserves_next_run() {
    for mode in [QueueMode::All, QueueMode::OneAtATime] {
        let mut w = World::new(DriveMode::Manual, mode);
        let hold = CancellationToken::new();
        let mut reply = Reply::stop();
        reply.hold = Some(hold.clone());
        w.adapter.initial.lock().unwrap().push_back(reply);
        let h = w.attach(true);
        let hp = h.clone();
        let mut call = Box::pin(async move { hp.prompt(prompt("drain-order")).await });
        assert!(poll_future(call.as_mut()).is_pending());
        w.until(&h, |_| !w.adapter.calls.lock().unwrap().streams.is_empty());
        let mut oracle = QueueReference::default();
        let a = w
            .pump
            .drive(h.steer(user("steer-a")))
            .unwrap()
            .unwrap()
            .entry_id;
        oracle.admit(a.clone(), "steer-a");
        let b = w
            .pump
            .drive(h.steer(user("steer-b")))
            .unwrap()
            .unwrap()
            .entry_id;
        oracle.admit(b.clone(), "steer-b");
        let c = w
            .pump
            .drive(h.follow_up(user("follow-c")))
            .unwrap()
            .unwrap()
            .entry_id;
        oracle.admit(c.clone(), "follow-c");
        let d = w
            .pump
            .drive(h.follow_up(user("follow-d")))
            .unwrap()
            .unwrap()
            .entry_id;
        oracle.admit(d.clone(), "follow-d");
        let next = w
            .pump
            .drive(h.next_run(user("next-run")))
            .unwrap()
            .unwrap()
            .entry_id;
        oracle.admit(next.clone(), "next-run");
        assert_eq!(
            w.pump.drive(h.cancel_queued(d.clone())).unwrap().unwrap(),
            CancelQueuedOutcome::Cancelled
        );
        oracle.cancel(&d);
        oracle.check(&w.observed());
        hold.cancel();
        w.drive(&h, call).unwrap().unwrap();
        oracle.consume(&[a, b, c.clone()]);
        let view = w.observed();
        oracle.check(&view);
        assert_eq!(view.pending_ids(), [next].into());
        assert!(!view.entry_ids().contains(&d));
        assert_eq!(
            w.pump.drive(h.cancel_queued(c)).unwrap().unwrap(),
            CancelQueuedOutcome::AlreadyConsumed
        );
        assert_eq!(
            w.adapter.calls.lock().unwrap().streams.len(),
            if mode == QueueMode::All { 3 } else { 4 }
        );
        assert!(view.run().is_none());
        w.pump.drive(h.close()).unwrap();
    }
}
