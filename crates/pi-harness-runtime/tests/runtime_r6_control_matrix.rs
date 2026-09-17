//! Existing Run phases reached solely through actual public operations.
mod runtime_r1_r4_support;
mod runtime_r6_control_matrix_support;
#[allow(dead_code)]
mod runtime_telemetry_support;
use pi_agent_schema::*;
use pi_ai_core::{CancellationToken, JsonValue, MaxRetries, Presence, StopReason};
use runtime_r1_r4_support::{assistant, n, poll as poll_future};
use runtime_r6_control_matrix_support::*;
use std::sync::{Arc, atomic::Ordering};

fn first_abort(w: &World, h: &Harness, manual: bool) -> (EntryId, EntryId) {
    let steer = w
        .pump
        .drive(h.steer(user("drained-steer")))
        .unwrap()
        .unwrap()
        .entry_id;
    let follow = w
        .pump
        .drive(h.follow_up(user("drained-follow")))
        .unwrap()
        .unwrap()
        .entry_id;
    let next = w
        .pump
        .drive(h.next_run(user("next-run")))
        .unwrap()
        .unwrap()
        .entry_id;
    let write = w
        .pump
        .drive(h.session().append_custom_entry(
            CustomType::new("survives-abort").unwrap(),
            Presence::Value(JsonValue::Bool(true)),
        ))
        .unwrap();
    let before = w.observed();
    assert!(before.pending_ids().contains(&write));
    let result = w.pump.drive(h.abort()).unwrap().unwrap();
    assert_eq!(result.steer, vec![user("drained-steer")]);
    assert_eq!(result.follow_up, vec![user("drained-follow")]);
    if manual {
        let marker = w.observed();
        let run = marker.run().expect("manual reconciliation remains gated");
        assert!(matches!(run.control, Control::CancelRequested(_)));
        assert_eq!(run.phase, before.run().unwrap().phase);
        assert!(marker.pending_ids().contains(&steer) && marker.pending_ids().contains(&follow));
        let repeat = w.pump.drive(h.abort()).unwrap().unwrap();
        assert_eq!(result, repeat);
        assert_eq!(marker, w.observed(), "repeat marker must not publish");
        for _ in 0..4 {
            w.pump.tick();
        }
        assert_eq!(
            marker,
            w.observed(),
            "unreleased reconciliation must not publish"
        );
    }
    (next, write)
}
fn terminal(w: &World, h: &Harness, expected: (EntryId, EntryId)) {
    w.finish_idle(h);
    let view = w.observed();
    assert!(view.run().is_none());
    assert_eq!(view.pending_ids(), [expected.0].into());
    assert!(view.entry_ids().contains(&expected.1));
    assert!(matches!(
        w.pump.drive(h.get_last_result()).unwrap(),
        Some(LaneLastResult::RunAborted(_))
    ));
    assert!(matches!(
        w.pump.drive(h.abort()).unwrap(),
        Err(AbortExpectedError::NoActiveOperation(_))
    ));
}
#[derive(Clone, Copy, Debug)]
enum Phase {
    CheckpointNeed,
    CheckpointMay,
    Ready,
    RequestWaiting,
    RequestRunning,
    PendingLive,
    PendingUnknown,
    RetryWait,
    Failure,
    DeferredSuspended,
    DeferredLive,
    DeferredUnknown,
}
impl Phase {
    fn automatic(self) -> bool {
        matches!(
            self,
            Self::RequestRunning | Self::PendingLive | Self::RetryWait | Self::DeferredLive
        )
    }
}
#[test]
fn public_abort_reconciles_each_existing_non_tool_phase() {
    for phase in [
        Phase::CheckpointNeed,
        Phase::CheckpointMay,
        Phase::Ready,
        Phase::RequestWaiting,
        Phase::RequestRunning,
        Phase::PendingLive,
        Phase::PendingUnknown,
        Phase::RetryWait,
        Phase::Failure,
        Phase::DeferredSuspended,
        Phase::DeferredLive,
        Phase::DeferredUnknown,
    ] {
        eprintln!("CASE {phase:?}");
        let mode = if phase.automatic() {
            DriveMode::Automatic
        } else {
            DriveMode::Manual
        };
        let mut w = World::new(mode, QueueMode::All);
        let hold = CancellationToken::new();
        let hook = Arc::new(HookGate::default());
        let deferred = matches!(
            phase,
            Phase::DeferredSuspended | Phase::DeferredLive | Phase::DeferredUnknown
        );
        let mut reply = if deferred {
            Reply::pending()
        } else {
            Reply::stop()
        };
        if matches!(phase, Phase::PendingLive) {
            reply.hold = Some(hold.clone());
            reply.message.usage.input = n(9);
            reply.message.usage.total_tokens = n(9);
        }
        if matches!(phase, Phase::RetryWait | Phase::Failure) {
            reply.message = assistant(&w.adapter.model, StopReason::Error);
            reply.message.error_message = Some("429 retry fixture".into());
        }
        if matches!(phase, Phase::RetryWait) {
            w.retry.enabled = true;
            w.retry.max_retries = MaxRetries::new(1).unwrap();
        }
        w.adapter.initial.lock().unwrap().push_back(reply);
        let mut h = w.attach(true);
        let _subscription = if matches!(phase, Phase::RequestWaiting | Phase::RequestRunning) {
            Some(h.hooks().before_request(None, hook.clone()).unwrap())
        } else {
            None
        };
        let mut waiter = run_call(h.clone());
        assert!(poll_future(waiter.as_mut()).is_pending());
        if deferred {
            w.drive(&h, waiter).unwrap();
            if matches!(phase, Phase::DeferredSuspended) {
                waiter = Box::pin(async { Ok(()) });
            } else {
                let mut poll = Reply::pending();
                poll.hold = Some(hold.clone());
                w.adapter.polls.lock().unwrap().push_back(poll);
                waiter = resume_call(h.clone());
                assert!(poll_future(waiter.as_mut()).is_pending());
                w.until(&h, |view| {
                    matches!(view.deferred(), Some(Deferred::EffectPending { .. }))
                        && (!matches!(phase, Phase::DeferredLive)
                            || w.adapter.calls.lock().unwrap().fetches.len() == 1)
                });
            }
        } else {
            w.until(&h,|view|{let Some(r)=view.run()else{return false};match phase{
    Phase::CheckpointNeed=>matches!(&r.phase,RunPhase::Checkpoint(p) if matches!(p.continuation,Continuation::NeedAssistant{..})),
    Phase::CheckpointMay=>matches!(&r.phase,RunPhase::Checkpoint(p) if matches!(p.continuation,Continuation::MayFinish{..})),
    Phase::Ready=>matches!(&r.phase,RunPhase::Assistant(AssistantPhase{generation:Generation::Ready{..},..})),
    Phase::RequestWaiting=>matches!(&r.phase,RunPhase::Assistant(AssistantPhase{generation:Generation::Ready{..},..}))&&w.pump.drive(h.peek_action()).unwrap().is_some_and(|a|a.kind=="effect_start"),
    Phase::RequestRunning=>hook.entered.load(Ordering::SeqCst)==1,
    Phase::PendingLive=>w.adapter.calls.lock().unwrap().streams.len()==1,
    Phase::PendingUnknown=>matches!(&r.phase,RunPhase::Assistant(AssistantPhase{generation:Generation::EffectPending{..},..})),
    Phase::RetryWait=>matches!(&r.phase,RunPhase::Assistant(AssistantPhase{generation:Generation::RetryWait{..},..})),
    Phase::Failure=>matches!(r.phase,RunPhase::FailureDrain(_)),_=>false}});
        }
        let before = w.observed();
        let original_reserved = before.run().and_then(|r| match &r.phase {
            RunPhase::Assistant(AssistantPhase {
                generation:
                    Generation::EffectPending {
                        response_entry_id,
                        usage_id,
                        ..
                    },
                ..
            }) => Some((response_entry_id.clone(), usage_id.clone())),
            RunPhase::Deferred(DeferredPhase {
                deferred:
                    Deferred::EffectPending {
                        response_entry_id,
                        usage_id,
                        ..
                    },
                ..
            }) => Some((response_entry_id.clone(), usage_id.clone())),
            _ => None,
        });
        let unknown = matches!(phase, Phase::PendingUnknown | Phase::DeferredUnknown);
        if unknown {
            h = w.reopen(h);
            drop(waiter);
            assert_eq!(before, w.observed());
            waiter = Box::pin(async { Ok(()) });
        }
        let streams = w.adapter.calls.lock().unwrap().streams.len();
        let fetches = w.adapter.calls.lock().unwrap().fetches.len();
        let saved = first_abort(&w, &h, mode == DriveMode::Manual);
        hook.release.cancel();
        hold.cancel();
        w.drive(&h, waiter).unwrap();
        terminal(&w, &h, saved);
        let after = w.observed();
        assert_eq!(
            w.adapter.calls.lock().unwrap().streams.len(),
            streams,
            "new ordinary request after {phase:?} abort"
        );
        assert_eq!(w.adapter.calls.lock().unwrap().fetches.len(), fetches);
        if let Some((response, usage)) = original_reserved {
            assert!(after.entry_ids().contains(&response));
            let row = after
                .usage
                .iter()
                .find(|u| u.id == usage)
                .expect("original reserved usage");
            if unknown {
                assert_eq!(row.usage, pi_ai_core::Usage::zero());
            }
            if matches!(phase, Phase::PendingLive) {
                assert_eq!(row.usage.input, n(9));
            }
        }
        if matches!(phase, Phase::RequestWaiting) {
            assert_eq!(hook.entered.load(Ordering::SeqCst), 0);
        }
        w.pump.drive(h.close()).unwrap();
    }
}

#[test]
fn public_abort_preserves_drained_marker_through_actual_reopen() {
    let mut w = World::new(DriveMode::Manual, QueueMode::All);
    w.adapter
        .initial
        .lock()
        .unwrap()
        .push_back(Reply::pending());
    let mut h = w.attach(true);
    w.drive(&h, h.prompt(prompt("marker"))).unwrap().unwrap();
    let saved = first_abort(&w, &h, true);
    let marker = w.observed();
    h = w.reopen(h);
    assert_eq!(marker, w.observed());
    w.drive(&h, h.resume()).unwrap().unwrap();
    terminal(&w, &h, saved);
    w.pump.drive(h.close()).unwrap();
}

#[test]
fn public_cancelled_tool_matrix_preserves_order_and_replay_rules() {
    for case in [
        "planned",
        "unknown-safe",
        "unknown-never",
        "mixed-completed",
        "mixed-live",
    ] {
        eprintln!("CASE tools-{case}");
        let manual = case != "mixed-live";
        let mut w = World::new(
            if manual {
                DriveMode::Manual
            } else {
                DriveMode::Automatic
            },
            QueueMode::All,
        );
        let replay = if case == "unknown-never" {
            ToolReplay::Never
        } else {
            ToolReplay::Safe
        };
        let tools: Vec<_> = ["a", "b", "c"]
            .iter()
            .map(|n| Tool::new(n, replay))
            .collect();
        w.tools = tools
            .iter()
            .map(|t| t.clone() as AgentHarnessToolHandle<()>)
            .collect();
        if case == "mixed-completed" {
            w.execution = ToolExecutionMode::Sequential;
            tools[0].gate.cancel();
        }
        w.adapter
            .initial
            .lock()
            .unwrap()
            .push_back(tool_reply(&["a", "b", "c"]));
        let mut h = w.attach(true);
        let mut waiter = run_call(h.clone());
        assert!(poll_future(waiter.as_mut()).is_pending());
        w.until(&h, |view| {
            let Some(r) = view.run() else { return false };
            let RunPhase::Tools(p) = &r.phase else {
                return false;
            };
            match case {
                "planned" => p
                    .batch
                    .calls
                    .iter()
                    .all(|c| matches!(c, ToolCallState::Planned { .. })),
                "unknown-safe" | "unknown-never" => {
                    matches!(p.batch.calls[0], ToolCallState::EffectPending { .. })
                }
                "mixed-completed" => matches!(p.batch.calls[0], ToolCallState::Completed { .. }),
                _ => tools.iter().all(|t| t.calls.load(Ordering::SeqCst) == 1),
            }
        });
        let prefix = w.observed();
        let RunPhase::Tools(p) = &prefix.run().unwrap().phase else {
            panic!()
        };
        let reserved: Vec<_> = p
            .batch
            .calls
            .iter()
            .map(|c| match c {
                ToolCallState::Planned {
                    result_entry_id, ..
                }
                | ToolCallState::EffectPending {
                    result_entry_id, ..
                }
                | ToolCallState::Completed {
                    result_entry_id, ..
                } => result_entry_id.clone(),
            })
            .collect();
        if case.starts_with("unknown") {
            assert_eq!(tools[0].calls.load(Ordering::SeqCst), 0);
            h = w.reopen(h);
            drop(waiter);
            assert_eq!(prefix, w.observed());
            waiter = Box::pin(async { Ok(()) });
        }
        if case == "mixed-live" {
            tools[2].gate.cancel();
            for _ in 0..3 {
                w.pump.tick();
            }
            assert!(tool_rows(&w.observed()).is_empty());
        }
        let saved = first_abort(&w, &h, manual);
        for t in tools.iter().rev() {
            t.gate.cancel();
        }
        w.drive(&h, waiter).unwrap();
        terminal(&w, &h, saved);
        let final_view = w.observed();
        let rows = tool_rows(&final_view);
        assert_eq!(rows.len(), 3);
        assert_eq!(
            rows.iter().map(|(e, _)| e.id.clone()).collect::<Vec<_>>(),
            reserved
        );
        for (i, (entry, row)) in rows.iter().enumerate() {
            assert_eq!(row.tool_name.as_str(), ["a", "b", "c"][i]);
            let had_completed = case == "mixed-completed" && i == 0;
            let live = case == "mixed-live" || had_completed;
            if live {
                assert!(!row.is_error);
                assert_eq!(row.usage.as_ref().unwrap().input, n(5));
                assert_eq!(
                    row.details,
                    Presence::Value(JsonValue::String(format!("raw:{}", row.tool_name.as_str())))
                );
                assert_eq!(entry.terminate.is_some(), had_completed);
            } else {
                assert!(row.is_error);
                assert!(row.usage.is_none());
                let text = serde_json::to_string(&row.content).unwrap();
                assert!(
                    text.to_lowercase()
                        .contains(if case.starts_with("unknown") && i == 0 {
                            "interrupted"
                        } else {
                            "aborted"
                        }),
                    "{case} call{i}: {text}"
                );
                assert_eq!(tools[i].calls.load(Ordering::SeqCst), 0);
            }
        }
        w.pump.drive(h.close()).unwrap();
    }
}

#[test]
fn public_post_effect_hooks_obey_both_abort_start_orders() {
    for tool in [false, true] {
        for hook_first in [false, true] {
            eprintln!(
                "CASE {}-{}",
                if tool { "tool" } else { "response" },
                if hook_first {
                    "hook-first"
                } else {
                    "abort-first"
                }
            );
            let mut w = World::new(DriveMode::Automatic, QueueMode::All);
            let hook = Arc::new(HookGate {
                patch: true,
                ..Default::default()
            });
            let gate = CancellationToken::new();
            let t = Tool::new("a", ToolReplay::Safe);
            if tool {
                w.tools = vec![t.clone()];
                w.adapter
                    .initial
                    .lock()
                    .unwrap()
                    .push_back(tool_reply(&["a"]));
            } else {
                let mut r = Reply::stop();
                if !hook_first {
                    r.hold = Some(gate.clone());
                }
                w.adapter.initial.lock().unwrap().push_back(r);
            }
            let h = w.attach(true);
            let _sub = if tool {
                h.hooks().after_tool(None, hook.clone()).unwrap()
            } else {
                h.hooks().after_response(None, hook.clone()).unwrap()
            };
            let mut waiter = run_call(h.clone());
            assert!(poll_future(waiter.as_mut()).is_pending());
            if tool {
                w.until(&h, |_| t.calls.load(Ordering::SeqCst) == 1);
                if hook_first {
                    t.gate.cancel();
                }
            }
            w.until(&h, |_| {
                if hook_first {
                    hook.entered.load(Ordering::SeqCst) == 1
                } else if tool {
                    t.calls.load(Ordering::SeqCst) == 1
                } else {
                    w.adapter.calls.lock().unwrap().streams.len() == 1
                }
            });
            let saved = first_abort(&w, &h, false);
            gate.cancel();
            t.gate.cancel();
            if hook_first {
                for _ in 0..3 {
                    w.pump.tick();
                }
                assert!(
                    w.observed().run().is_some(),
                    "started hook cannot be abandoned"
                );
            }
            hook.release.cancel();
            w.drive(&h, waiter).unwrap();
            terminal(&w, &h, saved);
            assert_eq!(hook.entered.load(Ordering::SeqCst), usize::from(hook_first));
            let view = w.observed();
            if tool {
                let rows = tool_rows(&view);
                assert_eq!(rows.len(), 1);
                assert_eq!(
                    rows[0].1.details,
                    Presence::Value(JsonValue::String(
                        if hook_first {
                            "hook-completed"
                        } else {
                            "raw:a"
                        }
                        .into()
                    ))
                );
                assert_eq!(
                    rows[0].0.terminate,
                    hook_first.then_some(pi_ai_core::LiteralTrue)
                );
            } else {
                let last = view
                    .entries
                    .iter()
                    .filter_map(|e| match e {
                        Entry::Message(e) => match &e.message {
                            AgentMessage::BuiltIn(pi_ai_core::Message::Assistant(m)) => Some(m),
                            _ => None,
                        },
                        _ => None,
                    })
                    .next_back()
                    .unwrap();
                assert_eq!(
                    last.response_id.as_ref().map(|id| id.as_str()),
                    hook_first.then_some("hook-completed")
                );
                assert_eq!(last.stop_reason, StopReason::Aborted);
            }
            w.pump.drive(h.close()).unwrap();
        }
    }
}

#[test]
fn public_abort_while_after_tool_start_is_parked_keeps_raw_but_clears_termination() {
    let mut w = World::new(DriveMode::Manual, QueueMode::All);
    let t = Tool::new("a", ToolReplay::Safe);
    t.gate.cancel();
    w.tools = vec![t.clone()];
    w.adapter
        .initial
        .lock()
        .unwrap()
        .push_back(tool_reply(&["a"]));
    let h = w.attach(true);
    let hook = Arc::new(HookGate {
        patch: true,
        ..Default::default()
    });
    let _subscription = h.hooks().after_tool(None, hook.clone()).unwrap();
    let mut waiter = run_call(h.clone());
    assert!(poll_future(waiter.as_mut()).is_pending());
    // Only after execute and finalize_tool has been released can this be the
    // after_tool hook gate: no before_tool handler remains and calls == 1.
    w.until(&h, |view| {
        t.calls.load(Ordering::SeqCst) == 1
            && tool_rows(view).is_empty()
            && matches!(view.run().map(|run| &run.phase), Some(RunPhase::Tools(_)))
            && w.pump
                .drive(h.peek_action())
                .unwrap()
                .is_some_and(|a| a.kind == "hook")
    });
    assert_eq!(hook.entered.load(Ordering::SeqCst), 0);
    let saved = first_abort(&w, &h, true);
    hook.release.cancel();
    w.drive(&h, waiter).unwrap();
    terminal(&w, &h, saved);
    let view = w.observed();
    let rows = tool_rows(&view);
    assert_eq!(
        hook.entered.load(Ordering::SeqCst),
        0,
        "abort won the serialized after_tool start"
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].1.details,
        Presence::Value(JsonValue::String("raw:a".into()))
    );
    assert_eq!(rows[0].1.usage.as_ref().unwrap().input, n(5));
    assert!(!rows[0].1.is_error);
    assert!(
        rows[0].0.terminate.is_none(),
        "skipped after_tool must use raw terminate:false"
    );
    w.pump.drive(h.close()).unwrap();
}
