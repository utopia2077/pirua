//! Seven finite public structural tests; twenty histories, actual prefix reopen.
mod r8_r10_public_model_support;
mod runtime_r1_r4_support;
use pi_agent_schema::*;
use pi_ai_core::{DeferredRequest, Message};
use r8_r10_public_model_support::*;
use runtime_r1_r4_support::{n, poll};
use std::sync::{Arc, Mutex};

#[test]
fn empty_compaction_preserves_every_public_row() {
    let mut w = World::new();
    let h = w.attach(true);
    let before = w.view();
    let result = w
        .drive(&h, h.compact(CompactOptions::default()))
        .expect("compact must use expected rejection channel");
    assert!(matches!(
        result,
        Err(CompactionExpectedError::NothingToCompact(_))
    ));
    assert_eq!(w.view(), before);
    assert!(w.adapter.calls.lock().unwrap().is_empty());
    assert!(w.log.0.lock().unwrap().is_empty());
    w.pump.drive(h.close()).unwrap();
}
#[test]
fn manual_compaction_sources_match_visible_reference() {
    for source in 0..3 {
        eprintln!("manual-source-{source}");
        let mut w = World::new();
        let h = w.attach(true);
        let old = w.seed(&h, "history ".repeat(100).as_str());
        let queued = w
            .pump
            .drive(h.next_run(user("next operation")))
            .unwrap()
            .unwrap()
            .entry_id;
        let seen = Arc::new(Mutex::new(vec![]));
        compaction_hook(&h, source, seen.clone());
        if source == 2 {
            w.adapter
                .replies
                .lock()
                .unwrap()
                .push_back(reply("generated summary", 11));
        }
        let result = w
            .drive(&h, h.compact(CompactOptions::default()))
            .expect("public compaction implementation")
            .unwrap();
        assert_eq!(seen.lock().unwrap().len(), 1);
        let mut reference = Reference {
            leaf: Some(old.clone()),
            entries: 1,
            pending: vec![queued],
        };
        match result.outcome {
            CompactionOutcome::Declined(_) => {
                assert_eq!(source, 0);
                reference.check(&w, &h, "compaction", "declined");
                w.assert_usage(&[]);
            }
            CompactionOutcome::Completed(c) => {
                assert_ne!(source, 0);
                assert_eq!(c.entry.parent_id, Some(old));
                assert_eq!(c.entry.from_hook, source == 1);
                assert_eq!(c.entry.retained_tail, vec![user(&"history ".repeat(100))]);
                assert_compaction_row(&w, &c.entry);
                reference.entries += 1;
                reference.leaf = Some(c.entry.id);
                reference.check(&w, &h, "compaction", "completed");
                w.assert_usage(&[if source == 1 { 5 } else { 11 }]);
            }
            other => panic!("unexpected {other:?}"),
        }
        assert_eq!(
            w.adapter.calls.lock().unwrap().len(),
            usize::from(source == 2)
        );
        w.assert_structural_only();
        w.assert_lifecycle("compaction");
        w.pump.drive(h.close()).unwrap();
    }
}
#[test]
fn navigation_invalid_inputs_write_nothing() {
    for case in 0..7 {
        eprintln!("invalid-nav-{case}");
        let mut w = World::new();
        let h = w.attach(true);
        let mut options = NavigateOptions::default();
        let mut foreign_keepalive = None;
        let target = match case {
            0 => None,
            1 => {
                w.seed(&h, "source");
                options.label = Some("root label".into());
                None
            }
            2 => {
                let lane = w
                    .pump
                    .drive(h.create_lane(LaneId::new("other").unwrap(), None))
                    .unwrap()
                    .unwrap();
                let target = w
                    .pump
                    .drive(lane.session().append_message(user("target")))
                    .unwrap();
                options.summarize = Some(true);
                Some(target)
            }
            3 => Some(w.seed(&h, "same leaf")),
            4 => {
                w.seed(&h, "source");
                options.summarize = Some(true);
                None
            }
            5 => Some(EntryId::new("absent-target").unwrap()),
            _ => {
                let (repo, foreign, id) = foreign_entry(&w);
                assert!(
                    w.pump
                        .drive(foreign.get_entry(id.clone()))
                        .unwrap()
                        .is_some()
                );
                assert!(
                    w.pump
                        .drive(w.session.get_entry(id.clone()))
                        .unwrap()
                        .is_none()
                );
                foreign_keepalive = Some((repo, foreign, id.clone()));
                Some(id)
            }
        };
        let before = w.view();
        let result = w
            .drive(&h, h.navigate_tree(target, options))
            .expect("navigation rejection is not HarnessFault");
        if case >= 5 {
            assert!(matches!(
                result,
                Err(NavigationExpectedError::UnknownTarget(_))
            ));
        } else {
            assert!(matches!(
                result,
                Err(NavigationExpectedError::InvalidNavigation(_))
            ));
        }
        assert_eq!(w.view(), before);
        assert!(w.adapter.calls.lock().unwrap().is_empty());
        if let Some((_, foreign, id)) = &foreign_keepalive {
            assert!(
                w.pump
                    .drive(foreign.get_entry(id.clone()))
                    .unwrap()
                    .is_some()
            );
            assert!(
                w.pump
                    .drive(w.session.get_entry(id.clone()))
                    .unwrap()
                    .is_none()
            );
        }
        w.pump.drive(h.close()).unwrap();
        drop(foreign_keepalive);
    }
}
#[test]
fn navigation_sources_match_visible_reference() {
    for source in 0..5 {
        eprintln!("navigation-source-{source}");
        let mut w = World::new();
        let h = w.attach(true);
        let target = w.seed(&h, "target ancestor");
        let old = w.seed(&h, "abandoned exploration");
        let pending = w
            .pump
            .drive(h.next_run(user("stay pending")))
            .unwrap()
            .unwrap()
            .entry_id;
        let seen = Arc::new(Mutex::new(vec![]));
        navigation_hook(
            &h,
            if source == 3 {
                0
            } else if source == 2 {
                1
            } else {
                2
            },
            seen.clone(),
        );
        if source == 4 {
            w.adapter
                .replies
                .lock()
                .unwrap()
                .push_back(reply("generated branch", 13));
        }
        let to = if source == 1 {
            None
        } else {
            Some(target.clone())
        };
        let options = NavigateOptions {
            summarize: (source >= 2).then_some(true),
            label: to.as_ref().map(|_| "destination".into()),
            custom_instructions: None,
        };
        let result = w
            .drive(&h, h.navigate_tree(to.clone(), options))
            .expect("public navigation implementation")
            .unwrap();
        let mut reference = Reference {
            leaf: to.clone(),
            entries: 2,
            pending: vec![pending],
        };
        match result.outcome {
            NavigationOutcome::Declined(_) => {
                assert_eq!(source, 3);
                reference.leaf = Some(old.clone());
                reference.check(&w, &h, "navigation", "declined");
                assert_eq!(
                    w.pump.drive(h.session().get_label(target.clone())).unwrap(),
                    None
                );
                w.assert_usage(&[]);
            }
            NavigationOutcome::Completed(c) => {
                assert_ne!(source, 3);
                assert_eq!(c.old_leaf_id, Some(old.clone()));
                if source >= 2 {
                    let entry = c.summary_entry.expect("summarized result");
                    assert_eq!(entry.parent_id, Some(target.clone()));
                    assert_eq!(entry.from_id, old);
                    assert_eq!(entry.from_hook, source == 2);
                    assert_navigation_row(&w, &entry);
                    reference.entries += 1;
                    reference.leaf = Some(entry.id);
                } else {
                    assert!(c.summary_entry.is_none());
                }
                assert_eq!(c.new_leaf_id, reference.leaf);
                reference.check(&w, &h, "navigation", "completed");
                if to.is_some() {
                    assert_eq!(
                        w.pump.drive(h.session().get_label(target.clone())).unwrap(),
                        Some("destination".into())
                    );
                }
                w.assert_usage(if source == 2 {
                    &[7]
                } else if source == 4 {
                    &[13]
                } else {
                    &[]
                });
            }
            other => panic!("unexpected {other:?}"),
        }
        assert_eq!(seen.lock().unwrap().len(), usize::from(source >= 2));
        assert_eq!(
            w.adapter.calls.lock().unwrap().len(),
            usize::from(source == 4)
        );
        w.assert_structural_only();
        w.assert_lifecycle("navigation");
        w.pump.drive(h.close()).unwrap();
    }
}
#[test]
fn empty_abandoned_branch_still_runs_navigation_hook() {
    let mut w = World::new();
    let h = w.attach(true);
    let old = w.seed(&h, "source");
    let lane = w
        .pump
        .drive(h.create_lane(LaneId::new("descendant").unwrap(), Some(old.clone())))
        .unwrap()
        .unwrap();
    let target = w
        .pump
        .drive(lane.session().append_message(user("descendant")))
        .unwrap();
    let seen = Arc::new(Mutex::new(vec![]));
    navigation_hook(&h, 2, seen.clone());
    let result = w
        .drive(
            &h,
            h.navigate_tree(
                Some(target.clone()),
                NavigateOptions {
                    summarize: Some(true),
                    ..Default::default()
                },
            ),
        )
        .expect("empty summarized navigation accepted")
        .unwrap();
    let NavigationOutcome::Completed(c) = result.outcome else {
        panic!("expected completion")
    };
    let e = c.summary_entry.unwrap();
    assert_eq!(seen.lock().unwrap().len(), 1);
    assert!(seen.lock().unwrap()[0].messages.is_empty());
    assert_eq!(e.summary, "No content to summarize");
    assert!(!e.from_hook);
    assert_eq!(e.from_id, old);
    assert_eq!(e.parent_id, Some(target));
    assert!(e.usage.is_none());
    assert_navigation_row(&w, &e);
    Reference {
        leaf: Some(e.id),
        entries: 3,
        pending: vec![],
    }
    .check(&w, &h, "navigation", "completed");
    assert!(w.adapter.calls.lock().unwrap().is_empty());
    w.assert_usage(&[]);
    w.assert_structural_only();
    w.assert_lifecycle("navigation");
    w.pump.drive(h.close()).unwrap();
}
#[test]
fn threshold_checked_prefix_survives_real_reopen() {
    for source in 0..2 {
        eprintln!("threshold-reopen-{source}");
        let mut w = World::new();
        w.settings.enabled = true;
        w.settings.reserve_tokens = n(990);
        let h = w.attach(true);
        let seen = Arc::new(Mutex::new(vec![]));
        compaction_hook(&h, source, seen.clone());
        w.adapter
            .replies
            .lock()
            .unwrap()
            .push_back(reply("done", 1));
        let hp = h.clone();
        let mut call =
            Box::pin(async move { hp.prompt(prompt(&"threshold input ".repeat(100))).await });
        assert!(poll(call.as_mut()).is_pending());
        w.prefix(&h, call.as_mut(), |view| {
            view.operation().is_some_and(|s| {
                s["phase"]["kind"] == "checkpoint"
                    && s["phase"]["thresholdCheckedTriggerEntryId"].is_string()
            })
        });
        assert_eq!(seen.lock().unwrap().len(), 1);
        assert!(w.adapter.calls.lock().unwrap().is_empty());
        let prefix = w.view();
        let h = w.reopen(h);
        assert!(matches!(
            w.pump.drive(call),
            Err(HarnessRuntimeError::Closed(_))
        ));
        assert_eq!(w.view(), prefix);
        compaction_hook(&h, source, seen.clone());
        let result = w
            .drive(&h, h.resume())
            .expect("resume checked threshold")
            .unwrap();
        let json = serde_json::to_value(result).unwrap();
        assert_eq!(json["operation"], "run");
        assert_eq!(json["kind"], "completed");
        assert_eq!(
            seen.lock().unwrap().len(),
            1,
            "same trigger checked twice after reopen"
        );
        assert_eq!(w.adapter.calls.lock().unwrap().len(), 1);
        w.view().terminal(&[]);
        w.assert_usage(if source == 1 { &[5, 1] } else { &[1] });
        w.pump.drive(h.close()).unwrap();
    }
}
#[test]
fn split_summary_reopen_restarts_whole_attempt_and_retains_paid_prefix() {
    let mut w = World::new();
    w.settings.keep_recent_tokens = n(20);
    let h = w.attach(true);
    w.seed(&h, &"old-user ".repeat(40));
    w.pump
        .drive(
            h.session()
                .append_message(AgentMessage::BuiltIn(Message::Assistant(Box::new(reply(
                    &"old-answer ".repeat(40),
                    0,
                ))))),
        )
        .unwrap();
    w.seed(&h, &"new-turn ".repeat(40));
    let tail = AgentMessage::BuiltIn(Message::Assistant(Box::new(reply(&"tail ".repeat(20), 0))));
    w.pump
        .drive(h.session().append_message(tail.clone()))
        .unwrap();
    let pending = w
        .pump
        .drive(h.next_run(user("future run")))
        .unwrap()
        .unwrap()
        .entry_id;
    let seen = Arc::new(Mutex::new(vec![]));
    compaction_hook(&h, 2, seen.clone());
    w.adapter.replies.lock().unwrap().extend([
        reply("abandoned-history-answer", 11),
        reply("fresh-history-answer", 17),
        reply("fresh-prefix-answer", 23),
    ]);
    let hp = h.clone();
    let mut call = Box::pin(async move { hp.compact(CompactOptions::default()).await });
    assert!(poll(call.as_mut()).is_pending());
    w.prefix(&h, call.as_mut(), |view| {
        view.operation().is_some_and(|s| {
            s["structural"]["generation"]["usageIds"]
                .as_array()
                .is_some_and(|a| a.len() == 1)
                && s["structural"]["generation"]["request"].is_null()
        })
    });
    assert_eq!(w.adapter.calls.lock().unwrap().len(), 1);
    w.assert_usage(&[11]);
    let preparation = seen.lock().unwrap()[0].clone();
    assert!(preparation.is_split_turn);
    assert!(!preparation.messages_to_summarize.is_empty());
    assert!(!preparation.turn_prefix_messages.is_empty());
    assert_eq!(preparation.retained_tail, vec![tail]);
    let frozen_preparation = w.view().preparation();
    let prefix = w.view();
    let h = w.reopen(h);
    assert!(matches!(
        w.pump.drive(call),
        Err(HarnessRuntimeError::Closed(_))
    ));
    assert_eq!(w.view(), prefix);
    assert_eq!(w.view().preparation(), frozen_preparation);
    compaction_hook(&h, 2, seen.clone());
    let result = w
        .drive(&h, h.resume())
        .expect("resume split summary")
        .unwrap();
    let json = serde_json::to_value(result).unwrap();
    assert_eq!(json["operation"], "compaction");
    assert_eq!(json["kind"], "completed");
    let e: CompactionEntry = serde_json::from_value(json["entry"].clone()).unwrap();
    assert_compaction_row(&w, &e);
    assert_eq!(e.retained_tail, preparation.retained_tail);
    assert_eq!(e.usage.as_ref().unwrap().input, n(40));
    assert!(e.summary.contains("fresh-history-answer"));
    assert!(e.summary.contains("fresh-prefix-answer"));
    assert!(!e.summary.contains("abandoned-history-answer"));
    assert_eq!(
        seen.lock().unwrap().len(),
        1,
        "generating decision hook replayed"
    );
    w.assert_usage(&[11, 17, 23]);
    let calls = w.adapter.calls.lock().unwrap();
    assert_eq!(calls.len(), 3);
    assert_eq!(
        calls[0].context.system_prompt,
        calls[1].context.system_prompt
    );
    assert_eq!(
        calls[0].context.messages, calls[1].context.messages,
        "request1 resumed instead of whole attempt"
    );
    assert_ne!(calls[1].context.messages, calls[2].context.messages);
    assert!(
        calls
            .iter()
            .all(|c| matches!(c.deferred, Some(DeferredRequest::Enabled(false))))
    );
    drop(calls);
    Reference {
        leaf: Some(e.id),
        entries: 5,
        pending: vec![pending],
    }
    .check(&w, &h, "compaction", "completed");
    w.assert_structural_only();
    w.pump.drive(h.close()).unwrap();
}
