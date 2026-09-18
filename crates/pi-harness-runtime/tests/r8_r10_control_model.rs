//! Independent bounded control/gate histories; no forged operation state.
mod r8_r10_control_model_support;
mod runtime_r1_r4_support;
use pi_agent_schema::*;
use pi_ai_core::{DeferredRequest, JsonValue};
use r8_r10_control_model_support::*;
use runtime_r1_r4_support::n;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
#[derive(Clone, Copy, Debug)]
enum Boundary {
    Intent,
    Dispatch,
    Settlement,
    Paid,
}
#[test]
fn nested_crash_prefixes_distinguish_abandonment_and_synthetic_cap() {
    let cases = [
        ("C-intent-below", Boundary::Intent, false, false),
        ("C-dispatch-below", Boundary::Dispatch, false, false),
        ("C-settlement-below", Boundary::Settlement, false, false),
        ("C-dispatch-cap", Boundary::Dispatch, true, false),
        ("C-settlement-cap", Boundary::Settlement, true, false),
        ("C-paid-between-cap", Boundary::Paid, true, false),
        ("N-dispatch-cap", Boundary::Dispatch, true, true),
    ];
    for (name, boundary, cap, navigation) in cases {
        eprintln!("{name}");
        let mut w = World::new();
        w.retries = if cap { 0 } else { 1 };
        if matches!(boundary, Boundary::Paid) {
            w.settings.keep_recent_tokens = n(20);
        }
        let h = w.attach(true);
        let target = if navigation {
            let target = w.seed(&h, "navigation target");
            w.seed(&h, "abandoned branch");
            Some(target)
        } else {
            if matches!(boundary, Boundary::Paid) {
                seed_split(&w, &h, true)
            } else {
                w.seed(&h, "history for manual compaction");
            }
            None
        };
        let old_leaf = w.pump.drive(h.get_leaf_id()).unwrap();
        let pending = w
            .pump
            .drive(h.next_run(user("next run survives recovery")))
            .unwrap()
            .unwrap()
            .entry_id;
        let count = w.view().entries.len();
        let hook_calls = Arc::new(AtomicUsize::new(0));
        if !navigation {
            install_compaction_hook(&h, false, hook_calls.clone());
        }
        if matches!(boundary, Boundary::Settlement | Boundary::Paid) {
            w.adapter
                .replies
                .lock()
                .unwrap()
                .push_back(reply("discarded-summary-attempt", 11));
        }
        if !cap {
            w.adapter
                .replies
                .lock()
                .unwrap()
                .push_back(reply("fresh-whole-attempt", 17));
        }
        let mut call = start(h.clone(), target);
        w.prefix(&h, call.as_mut(), |view| {
            let Some(state) = view.operation() else {
                return false;
            };
            let g = generation(&state);
            if g["status"] != "effect_pending" || g["attempt"] != 1 {
                return false;
            }
            let paid = g["usageIds"].as_array().map_or(0, Vec::len);
            match boundary {
                Boundary::Intent => {
                    g["request"].is_null() && paid == 0 && gate(&w, &h, "commit_transition")
                }
                Boundary::Dispatch => {
                    g["request"]["index"] == 0 && paid == 0 && gate(&w, &h, "run_summary_request")
                }
                Boundary::Settlement => {
                    g["request"]["index"] == 0
                        && paid == 0
                        && w.adapter.responses.load(Ordering::SeqCst) == 1
                        && gate(&w, &h, "settle_summary_request")
                }
                Boundary::Paid => {
                    g["request"].is_null() && paid == 1 && gate(&w, &h, "commit_transition")
                }
            }
        });
        let prefix = w.view();
        let state = prefix.operation().unwrap();
        let g = generation(&state);
        assert!(g["context"]["taskId"].is_string());
        let reservation = g["request"]["usageId"].as_str().map(str::to_owned);
        let prior_calls = usize::from(matches!(boundary, Boundary::Settlement | Boundary::Paid));
        assert_eq!(w.adapter.calls.lock().unwrap().len(), prior_calls);
        assert_eq!(w.adapter.responses.load(Ordering::SeqCst), prior_calls);
        w.assert_usage(if matches!(boundary, Boundary::Paid) {
            &[11]
        } else {
            &[]
        });
        if matches!(boundary, Boundary::Paid) {
            let p = prefix.preparation().unwrap();
            assert_eq!(p["isSplitTurn"], true);
            assert!(!p["messagesToSummarize"].as_array().unwrap().is_empty());
            assert!(!p["turnPrefixMessages"].as_array().unwrap().is_empty());
        }
        let h = w.reopen(h);
        assert!(matches!(
            w.pump.drive(call),
            Err(HarnessRuntimeError::Closed(_))
        ));
        assert_eq!(w.view(), prefix, "reopen altered committed prefix");
        if !navigation {
            install_compaction_hook(&h, false, hook_calls.clone());
        }
        let outcome = w
            .drive(&h, h.resume())
            .expect("resume must drive structural state")
            .unwrap();
        let json = serde_json::to_value(outcome).unwrap();
        assert_eq!(json["kind"], if cap { "failed" } else { "completed" });
        assert_eq!(
            json["operation"],
            if navigation {
                "navigation"
            } else {
                "compaction"
            }
        );
        w.view().terminal(std::slice::from_ref(&pending));
        assert_eq!(w.view().entries.len(), count + usize::from(!cap));
        if cap {
            assert_eq!(w.pump.drive(h.get_leaf_id()).unwrap(), old_leaf);
            if matches!(boundary, Boundary::Paid) {
                w.assert_usage(&[11]);
            } else {
                w.assert_usage(&[0]);
                assert_eq!(w.paid()[0].id.as_str(), reservation.as_deref().unwrap());
            }
            assert_eq!(w.adapter.calls.lock().unwrap().len(), prior_calls);
        } else {
            w.assert_usage(&[17]);
            if let Some(old) = reservation {
                assert!(w.paid().iter().all(|row| row.id.as_str() != old));
            }
            assert_eq!(w.adapter.calls.lock().unwrap().len(), prior_calls + 1);
        }
        if !navigation {
            assert_eq!(
                hook_calls.load(Ordering::SeqCst),
                1,
                "generating hook replayed"
            );
        }
        assert!(
            w.adapter
                .calls
                .lock()
                .unwrap()
                .iter()
                .all(|r| matches!(r.deferred, Some(DeferredRequest::Enabled(false))))
        );
        assert!(w.paid().iter().all(|r| r.entry_id.is_none()));
        w.assert_structural_only();
        w.pump.drive(h.close()).unwrap();
    }
}
#[test]
fn abort_before_dispatch_and_after_response_have_distinct_usage() {
    for started in [false, true] {
        eprintln!("abort-started-{started}");
        let mut w = World::new();
        let h = w.attach(true);
        let source = w.seed(&h, "manual abort source");
        let pending = w
            .pump
            .drive(h.next_run(user("future run")))
            .unwrap()
            .unwrap()
            .entry_id;
        let hook_calls = Arc::new(AtomicUsize::new(0));
        install_compaction_hook(&h, false, hook_calls.clone());
        if started {
            w.adapter
                .replies
                .lock()
                .unwrap()
                .push_back(reply("returned-before-abort", 11));
        }
        let mut call = start(h.clone(), None);
        w.prefix(&h, call.as_mut(), |v| {
            v.operation().is_some_and(|s| {
                let g = generation(&s);
                g["status"] == "effect_pending"
                    && g["request"]["index"] == 0
                    && g["usageIds"].as_array().unwrap().is_empty()
                    && gate(
                        &w,
                        &h,
                        if started {
                            "settle_summary_request"
                        } else {
                            "run_summary_request"
                        },
                    )
            })
        });
        assert_eq!(w.adapter.calls.lock().unwrap().len(), usize::from(started));
        assert_eq!(
            w.adapter.responses.load(Ordering::SeqCst),
            usize::from(started)
        );
        w.assert_usage(&[]);
        let first = w.pump.drive(h.abort()).expect("public abort").unwrap();
        let marked = w.view();
        assert_eq!(w.pump.drive(h.abort()).unwrap().unwrap(), first);
        assert_eq!(w.view(), marked, "repeat abort published again");
        let outcome = w.drive(&h, call).expect("accepted abort reconciliation");
        assert_eq!(outcome["kind"], "aborted");
        assert_eq!(w.pump.drive(h.get_leaf_id()).unwrap(), Some(source));
        assert_eq!(w.view().entries.len(), 1);
        w.view().terminal(std::slice::from_ref(&pending));
        w.assert_usage(if started { &[11] } else { &[] });
        assert_eq!(w.adapter.calls.lock().unwrap().len(), usize::from(started));
        assert_eq!(hook_calls.load(Ordering::SeqCst), 1);
        w.assert_structural_only();
        w.assert_lifecycle("compaction");
        w.pump.drive(h.close()).unwrap();
    }
}
#[test]
fn paid_split_request_model_loss_suspends_owning_run_without_write() {
    let mut w = World::new();
    w.settings.enabled = true;
    w.settings.reserve_tokens = n(900);
    w.settings.keep_recent_tokens = n(20);
    let h = w.attach(true);
    seed_split(&w, &h, false);
    let hook_calls = Arc::new(AtomicUsize::new(0));
    install_compaction_hook(&h, false, hook_calls.clone());
    w.withdrawal.enabled.store(true, Ordering::SeqCst);
    w.adapter.replies.lock().unwrap().extend([
        reply("abandoned-history", 11),
        reply("fresh-history", 17),
        reply("fresh-prefix", 23),
        reply("ordinary-final", 1),
    ]);
    // A supplied assistant is legal public PromptInput; it is not a forged operation.
    let result = w
        .drive(&h, h.prompt(PromptInput::Messages(vec![tail()])))
        .expect("accepted Run suspension")
        .unwrap();
    assert!(
        matches!(result.outcome, RunOutcome::MissingIdentities(_)),
        "in-run summary must not return standalone compaction outcome"
    );
    assert!(w.withdrawal.hits.load(Ordering::SeqCst) > 0);
    let paid = w
        .withdrawal
        .observed
        .lock()
        .unwrap()
        .clone()
        .expect("real committed usage triggered catalog loss");
    assert_eq!(w.view(), paid, "suspension changed durable state");
    let state = paid.operation().unwrap();
    assert_eq!(state["kind"], "run");
    assert_eq!(state["phase"]["kind"], "compaction");
    let g = generation(&state);
    assert_eq!(g["status"], "effect_pending");
    assert!(g["request"].is_null());
    assert_eq!(g["usageIds"].as_array().unwrap().len(), 1);
    assert_eq!(w.adapter.calls.lock().unwrap().len(), 1);
    w.assert_usage(&[11]);
    let rejected = w
        .pump
        .drive(h.resume())
        .expect("missing identities is expected rejection");
    assert!(matches!(
        rejected,
        Err(ResumeExpectedError::MissingIdentities(_))
    ));
    assert_eq!(w.view(), paid);
    w.withdrawal.enabled.store(false, Ordering::SeqCst);
    let result = w
        .drive(&h, h.resume())
        .expect("catalog-restored resume")
        .unwrap();
    let json = serde_json::to_value(result).unwrap();
    assert_eq!(json["operation"], "run");
    assert_eq!(json["kind"], "completed");
    assert_eq!(hook_calls.load(Ordering::SeqCst), 1);
    w.assert_usage(&[11, 17, 23, 1]);
    w.view().terminal(&[]);
    let calls = w.adapter.calls.lock().unwrap();
    assert_eq!(calls.len(), 4);
    assert_eq!(
        calls[0].context.messages, calls[1].context.messages,
        "resumed partial request2 instead of whole attempt"
    );
    assert_ne!(calls[1].context.messages, calls[2].context.messages);
    drop(calls);
    let entries = w.view().entries;
    let compact = entries
        .iter()
        .find_map(|e| {
            if let Entry::Compaction(e) = e {
                Some(e)
            } else {
                None
            }
        })
        .unwrap();
    assert_eq!(compact.usage.as_ref().unwrap().input, n(40));
    assert!(!compact.summary.contains("abandoned-history"));
    assert!(compact.summary.contains("fresh-history"));
    assert!(compact.summary.contains("fresh-prefix"));
    w.pump.drive(h.close()).unwrap();
}
#[test]
fn terminal_preparation_stale_by_unrelated_write_rebinds_exact_result() {
    for navigation in [false, true] {
        eprintln!("stale-terminal-navigation-{navigation}");
        let mut w = World::new();
        let h = w.attach(true);
        let first = w.seed(&h, "target or original history");
        let target = if navigation {
            w.seed(&h, "branch being summarized");
            Some(first)
        } else {
            None
        };
        let pending = w
            .pump
            .drive(h.next_run(user("next run retained")))
            .unwrap()
            .unwrap()
            .entry_id;
        let calls = Arc::new(AtomicUsize::new(0));
        if navigation {
            install_navigation_hook(&h, calls.clone())
        } else {
            install_compaction_hook(&h, true, calls.clone())
        }
        let mut call = start(h.clone(), target);
        w.prefix(&h, call.as_mut(), |v| {
            v.operation().is_some() && gate(&w, &h, "commit_terminal")
        });
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(w.adapter.calls.lock().unwrap().is_empty());
        w.assert_usage(&[]);
        let before = w.view().entries.len();
        w.time.0.store(99, Ordering::SeqCst);
        w.pump
            .drive(
                w.session
                    .set_custom_fact("terminal-drift".into(), Some(JsonValue::Bool(true))),
            )
            .unwrap();
        let drift = w
            .pump
            .drive(
                w.session
                    .get_register(RegisterNamespace::FactCustom, "terminal-drift"),
            )
            .unwrap()
            .unwrap();
        let drift = serde_json::to_value(drift).unwrap()["seq"]
            .as_u64()
            .unwrap();
        let result = w
            .drive(&h, call)
            .expect("stale terminal reparses without replay");
        assert_eq!(result["kind"], "completed");
        let row = if navigation {
            &result["summaryEntry"]
        } else {
            &result["entry"]
        };
        let id = EntryId::new(row["id"].as_str().unwrap()).unwrap();
        let stored = w
            .pump
            .drive(w.session.get_entry(id.clone()))
            .unwrap()
            .unwrap();
        assert_eq!(serde_json::to_value(&stored).unwrap(), *row);
        assert_eq!(row["timestamp"], 99);
        assert_eq!(row["seq"], drift + if navigation { 3 } else { 2 });
        assert_eq!(row["fromHook"], true);
        assert_eq!(w.view().entries.len(), before + 1);
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "completed hook reran after stale metadata"
        );
        assert_eq!(w.pump.drive(h.get_leaf_id()).unwrap(), Some(id));
        w.view().terminal(std::slice::from_ref(&pending));
        w.assert_usage(&[if navigation { 7 } else { 5 }]);
        assert_eq!(
            w.paid()[0].entry_id,
            None,
            "hook usage cannot link future result"
        );
        let events = w.log.0.lock().unwrap();
        assert!(
            events
                .iter()
                .any(|e| matches!(e,HarnessEvent::EntryAdded(e) if e.entry==stored))
        );
        assert!(events.iter().any(|e| match e {
            HarnessEvent::CompactionEndCompleted(e) =>
                serde_json::to_value(&e.entry).unwrap() == *row,
            HarnessEvent::NavigationEndCompleted(e) =>
                serde_json::to_value(&e.summary_entry).unwrap() == *row,
            _ => false,
        }));
        drop(events);
        let last =
            serde_json::to_value(w.pump.drive(h.get_last_result()).unwrap().unwrap()).unwrap();
        assert_eq!(
            last["kind"],
            if navigation {
                "navigation"
            } else {
                "compaction"
            }
        );
        assert_eq!(last["outcome"], "completed");
        w.assert_structural_only();
        w.assert_lifecycle(if navigation {
            "navigation"
        } else {
            "compaction"
        });
        w.pump.drive(h.close()).unwrap();
    }
}

#[test]
fn post_intent_adapter_loss_is_failed_settlement_not_suspension() {
    let mut w = World::new();
    w.retries = 0;
    let h = w.attach(true);
    let source = w.seed(&h, "adapter-loss source");
    let mut call = start(h.clone(), None);
    w.prefix(&h, call.as_mut(), |v| {
        v.operation().is_some_and(|s| {
            generation(&s)["request"]["index"] == 0 && gate(&w, &h, "run_summary_request")
        })
    });
    let state = w.view().operation().unwrap();
    let reserved = generation(&state)["request"]["usageId"]
        .as_str()
        .unwrap()
        .to_owned();
    assert!(w.adapter.calls.lock().unwrap().is_empty());
    let before = w.lookups.load(Ordering::SeqCst);
    // Registration/model remain present; the real captured dispatcher now sees no API adapter.
    w.unavailable.store(true, Ordering::SeqCst);
    assert!(
        w.models
            .get_model(&w.adapter.model.provider, &w.adapter.model.id)
            .is_some()
    );
    let result = w
        .drive(&h, call)
        .expect("post-intent lookup failure is in-band");
    assert_eq!(result["kind"], "failed");
    assert!(
        w.lookups.load(Ordering::SeqCst) > before,
        "dispatcher never attempted actual adapter lookup"
    );
    assert!(w.adapter.calls.lock().unwrap().is_empty());
    w.assert_usage(&[0]);
    assert_eq!(w.paid()[0].id.as_str(), reserved);
    assert_eq!(w.pump.drive(h.get_leaf_id()).unwrap(), Some(source));
    assert_eq!(w.view().entries.len(), 1);
    w.view().terminal(&[]);
    w.assert_structural_only();
    w.pump.drive(h.close()).unwrap();
}
