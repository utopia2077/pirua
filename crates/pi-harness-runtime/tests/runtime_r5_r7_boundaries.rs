mod runtime_r1_r4_support;
mod runtime_r5_r7_support;
use pi_agent_schema::*;
use pi_agent_session::storage::AtomicStorage;
use pi_ai_core::{CancellationToken, Presence, StopReason};
use runtime_r1_r4_support::*;
use runtime_r5_r7_support::*;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
struct BeforeRunHold {
    entered: CancellationToken,
    release: CancellationToken,
}
impl HookHandler<BeforeRun> for BeforeRunHold {
    fn call(
        &self,
        _: BeforeRunEvent,
    ) -> BoxFuture<'_, Result<Option<BeforeRunResult>, HandlerError>> {
        Box::pin(async move {
            self.entered.cancel();
            self.release.cancelled().await;
            Ok(None)
        })
    }
}
#[test]
fn capture_cancel_and_late_enqueue_serialize_around_actual_before_run_hook() {
    for cancel_before in [false, true] {
        let s = Setup::new();
        let c = s
            .pump
            .drive(factory(&s).create(s.options(DriveMode::Automatic)))
            .unwrap();
        let early = s
            .pump
            .drive(c.harness.next_run(user("early")))
            .unwrap()
            .unwrap()
            .entry_id;
        let hook = Arc::new(BeforeRunHold {
            entered: CancellationToken::new(),
            release: CancellationToken::new(),
        });
        let _registration = c
            .harness
            .hooks()
            .before_run(HandlerId::new("held").unwrap(), hook.clone())
            .unwrap();
        let mut run = Box::pin(c.harness.prompt(prompt()));
        assert!(poll(run.as_mut()).is_pending());
        tick_until(&s, || hook.entered.is_cancelled());
        let late = s
            .pump
            .drive(c.harness.next_run(user("late")))
            .unwrap()
            .unwrap()
            .entry_id;
        if cancel_before {
            assert_eq!(
                s.pump
                    .drive(c.harness.cancel_queued(early.clone()))
                    .unwrap()
                    .unwrap(),
                CancelQueuedOutcome::Cancelled
            );
        }
        hook.release.cancel();
        s.pump.drive(run).unwrap().unwrap();
        assert_eq!(
            s.pump
                .drive(c.harness.cancel_queued(early))
                .unwrap()
                .unwrap(),
            if cancel_before {
                CancelQueuedOutcome::NotFound
            } else {
                CancelQueuedOutcome::AlreadyConsumed
            }
        );
        assert!(
            s.pump
                .drive(c.harness.session().get_entry(late.clone()))
                .unwrap()
                .is_none()
        );
        assert_eq!(
            s.pump
                .drive(c.harness.cancel_queued(late))
                .unwrap()
                .unwrap(),
            CancelQueuedOutcome::Cancelled
        );
    }
}
#[test]
fn queue_expected_errors_and_pending_entry_triad_have_no_partial_writes() {
    let s = Setup::new();
    let c = s
        .pump
        .drive(factory(&s).create(s.options(DriveMode::Automatic)))
        .unwrap();
    let before = s.pump.drive(s.store.snapshot()).unwrap();
    assert!(matches!(
        s.pump.drive(c.harness.steer(user("idle"))).unwrap(),
        Err(QueueExpectedError::NoActiveRun(_))
    ));
    assert!(matches!(
        s.pump.drive(c.harness.follow_up(user("idle"))).unwrap(),
        Err(QueueExpectedError::NoActiveRun(_))
    ));
    let invalid = AgentMessage::BuiltIn(pi_ai_core::Message::Assistant(Box::new(assistant(
        &s.faux.model,
        StopReason::Pending,
    ))));
    assert!(matches!(
        s.pump.drive(c.harness.next_run(invalid)).unwrap(),
        Err(NextRunExpectedError::InvalidMessage(_))
    ));
    assert_eq!(s.pump.drive(s.store.snapshot()).unwrap(), before);
    s.pump.drive(c.harness.close()).unwrap();
    assert!(matches!(
        s.pump.drive(c.harness.next_run(user("closed"))).unwrap(),
        Err(NextRunExpectedError::Closed(_))
    ));
}
#[test]
fn failure_drain_revives_only_projecting_writes_and_keeps_filtered_responses() {
    for kind in ["user", "length", "error", "aborted", "deferred", "custom"] {
        let s = Setup::new();
        let adapter = DeferredAdapter::install(&s);
        *s.faux.responses.lock().unwrap() =
            vec![assistant(&s.faux.model, StopReason::Error)].into();
        let hold = CancellationToken::new();
        *s.faux.hold.lock().unwrap() = Some(hold.clone());
        let c = s
            .pump
            .drive(factory(&s).create(s.options(DriveMode::Automatic)))
            .unwrap();
        let mut run = Box::pin(c.harness.prompt(prompt()));
        assert!(poll(run.as_mut()).is_pending());
        tick_until(&s, || !s.faux.requests.lock().unwrap().is_empty());
        let id = if kind == "custom" {
            s.pump
                .drive(c.harness.session().append_custom_entry(
                    CustomType::new("unprojected").unwrap(),
                    Presence::Missing,
                ))
                .unwrap()
        } else {
            let message = if kind == "user" {
                user("new input")
            } else {
                let reason = match kind {
                    "length" => StopReason::Length,
                    "error" => StopReason::Error,
                    "aborted" => StopReason::Aborted,
                    _ => StopReason::Deferred,
                };
                AgentMessage::BuiltIn(pi_ai_core::Message::Assistant(Box::new(
                    adapter.response(reason),
                )))
            };
            s.pump
                .drive(c.harness.session().append_message(message))
                .unwrap()
        };
        hold.cancel();
        let result = s.pump.drive(run).unwrap().unwrap();
        let projects = matches!(kind, "user" | "length");
        assert_eq!(
            matches!(result.outcome, RunOutcome::CompletedWithAssistant(_)),
            projects,
            "{kind}"
        );
        assert_eq!(
            s.faux.requests.lock().unwrap().len(),
            if projects { 2 } else { 1 },
            "{kind}"
        );
        assert!(
            s.pump
                .drive(c.harness.session().get_entry(id))
                .unwrap()
                .is_some()
        );
    }
}
struct ForbiddenProjector(Arc<AtomicUsize>);
impl EntryProjector for ForbiddenProjector {
    fn project(
        &self,
        _: CustomEntry,
    ) -> BoxFuture<'_, Result<Option<Vec<AgentMessage>>, HarnessRuntimeError>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async {
            Err(HarnessRuntimeError::Fault(HarnessFault::new(
                ErrorContext::new("cancelled projector must not execute").unwrap(),
            )))
        })
    }
}
#[test]
fn cancelled_pending_custom_write_executes_no_projector() {
    let s = Setup::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut options = s.options(DriveMode::Manual);
    options.entry_projectors.insert(
        CustomType::new("note").unwrap(),
        Arc::new(ForbiddenProjector(calls.clone())),
    );
    let c = s.pump.drive(factory(&s).create(options)).unwrap();
    let mut run = Box::pin(c.harness.prompt(prompt()));
    assert!(poll(run.as_mut()).is_pending());
    manual_until(&s, c.harness.as_ref(), || current(&s).is_some());
    s.pump.drive(c.harness.abort()).unwrap().unwrap();
    let id = s
        .pump
        .drive(
            c.harness
                .session()
                .append_custom_entry(CustomType::new("note").unwrap(), Presence::Null),
        )
        .unwrap();
    s.pump.drive(c.harness.run_to_completion()).unwrap();
    s.pump.drive(run).unwrap().unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(
        s.pump
            .drive(c.harness.session().get_entry(id))
            .unwrap()
            .is_some()
    );
}
struct PollOptions {
    seen: Mutex<Vec<BeforeRequestEvent>>,
}
impl HookHandler<BeforeRequest> for PollOptions {
    fn call(
        &self,
        e: BeforeRequestEvent,
    ) -> BoxFuture<'_, Result<Option<BeforeRequestResult>, HandlerError>> {
        Box::pin(async move {
            if e.step == HookRequestStep::Deferred {
                self.seen.lock().unwrap().push(e);
                Ok(Some(BeforeRequestResult {
                    stream_options: Some(
                        serde_json::from_value(serde_json::json!({"timeoutMs":23})).unwrap(),
                    ),
                }))
            } else {
                Ok(None)
            }
        })
    }
}
#[test]
fn polls_capture_original_options_each_invocation_and_use_effective_hook_options() {
    let s = Setup::new();
    let adapter = DeferredAdapter::install(&s);
    adapter.begin();
    adapter.replies.lock().unwrap().extend([
        adapter.response(StopReason::Deferred),
        adapter.response(StopReason::Stop),
    ]);
    let mut options = s.options(DriveMode::Automatic);
    options.stream_options.timeout_ms = Some(n(17));
    let c = s.pump.drive(factory(&s).create(options)).unwrap();
    let hooks = Arc::new(PollOptions {
        seen: Mutex::new(vec![]),
    });
    let _r = c
        .harness
        .hooks()
        .before_request(None, hooks.clone())
        .unwrap();
    s.pump.drive(c.harness.prompt(prompt())).unwrap().unwrap();
    s.pump
        .drive(c.harness.set_stream_options(AgentHarnessStreamOptions {
            timeout_ms: Some(n(99)),
            ..Default::default()
        }))
        .unwrap();
    s.pump.drive(c.harness.resume()).unwrap().unwrap();
    s.pump.drive(c.harness.resume()).unwrap().unwrap();
    let seen = hooks.seen.lock().unwrap();
    assert_eq!(seen.len(), 2);
    for e in seen.iter() {
        assert_eq!(e.stream_options.timeout_ms, Some(n(17)));
        assert_eq!(
            e.stream_options.deferred,
            Some(pi_ai_core::DeferredRequest::Enabled(false))
        );
    }
    assert_eq!(seen[0].attempt.get(), 1);
    assert_eq!(seen[1].attempt.get(), 2);
    for (_, wire) in adapter.polls.lock().unwrap().iter() {
        assert_eq!(wire.timeout_ms, Some(n(23)));
        assert_eq!(wire.wait, Some(n(0)));
    }
}
#[test]
fn deferred_resume_missing_identity_is_expected_and_burns_no_intent_or_permit() {
    let s = Setup::new();
    let adapter = DeferredAdapter::install(&s);
    adapter.begin();
    let c = s
        .pump
        .drive(factory(&s).create(s.options(DriveMode::Automatic)))
        .unwrap();
    s.pump.drive(c.harness.prompt(prompt())).unwrap().unwrap();
    s.pump
        .drive(s.registry.delete_provider(&s.faux.model.provider))
        .unwrap();
    let before = s.pump.drive(s.store.snapshot()).unwrap();
    assert!(matches!(
        s.pump.drive(c.harness.resume()).unwrap(),
        Err(ResumeExpectedError::MissingIdentities(_))
    ));
    assert_eq!(s.pump.drive(s.store.snapshot()).unwrap(), before);
    assert!(adapter.polls.lock().unwrap().is_empty());
    s.pump.drive(c.harness.abort()).unwrap().unwrap();
    s.pump.drive(c.harness.wait_for_idle()).unwrap();
    assert!(s.pump.drive(c.harness.get_stream_options()).is_ok());
}
struct IdleTask {
    entered: CancellationToken,
    release: CancellationToken,
    fail: bool,
}
impl RunWhenIdleTask for IdleTask {
    fn run(self: Box<Self>) -> BoxFuture<'static, Result<(), CallbackError>> {
        Box::pin(async move {
            self.entered.cancel();
            self.release.cancelled().await;
            if self.fail {
                Err(CallbackError::new(
                    ErrorContext::new("ordinary callback error").unwrap(),
                ))
            } else {
                Ok(())
            }
        })
    }
}
#[test]
fn close_waits_started_idle_callback_and_preserves_ordinary_callback_result() {
    for fail in [false, true] {
        let s = Setup::new();
        let c = s
            .pump
            .drive(factory(&s).create(s.options(DriveMode::Automatic)))
            .unwrap();
        let entered = CancellationToken::new();
        let release = CancellationToken::new();
        let mut idle = Box::pin(c.harness.run_when_idle(Box::new(IdleTask {
            entered: entered.clone(),
            release: release.clone(),
            fail,
        })));
        assert!(poll(idle.as_mut()).is_pending());
        tick_until(&s, || entered.is_cancelled());
        let mut close = Box::pin(c.harness.close());
        assert!(poll(close.as_mut()).is_pending());
        for _ in 0..8 {
            s.pump.tick();
        }
        assert!(poll(close.as_mut()).is_pending());
        release.cancel();
        let result = s.pump.drive(idle);
        assert_eq!(matches!(result, Err(RunWhenIdleError::Callback(_))), fail);
        s.pump.drive(close).unwrap();
    }
}
#[test]
fn config_and_global_settings_have_both_serialized_generation_start_orders() {
    for setter_first in [false, true] {
        let s = Setup::new();
        let c = s
            .pump
            .drive(factory(&s).create(s.options(DriveMode::Manual)))
            .unwrap();
        let mut run = Box::pin(c.harness.prompt(prompt()));
        assert!(poll(run.as_mut()).is_pending());
        manual_until(
            &s,
            c.harness.as_ref(),
            || matches!(current(&s),Some(OperationState::Run(r))if matches!(r.phase,RunPhase::Checkpoint(_))),
        );
        let set = || {
            s.pump
                .drive(c.harness.set_thinking_level(AgentThinkingLevel::High))
                .unwrap();
            s.pump
                .drive(c.harness.set_stream_options(AgentHarnessStreamOptions {
                    timeout_ms: Some(n(51)),
                    ..Default::default()
                }))
                .unwrap();
            s.pump
                .drive(c.harness.set_retry_policy(pi_ai_core::RetryPolicy {
                    enabled: true,
                    max_retries: pi_ai_core::MaxRetries::new(2).unwrap(),
                    base_delay_ms: n(4),
                }))
                .unwrap();
        };
        if setter_first {
            set();
        }
        manual_until(
            &s,
            c.harness.as_ref(),
            || matches!(current(&s),Some(OperationState::Run(r))if matches!(r.phase,RunPhase::Assistant(AssistantPhase{generation:Generation::Ready{..},..}))),
        );
        if !setter_first {
            set();
        }
        let Some(OperationState::Run(r)) = current(&s) else {
            panic!()
        };
        let RunPhase::Assistant(AssistantPhase {
            generation: Generation::Ready { context, .. },
            ..
        }) = r.phase
        else {
            panic!()
        };
        assert_eq!(
            context.configuration.thinking_level,
            if setter_first {
                AgentThinkingLevel::High
            } else {
                AgentThinkingLevel::Off
            }
        );
        assert_eq!(
            context.stream_options.timeout_ms,
            if setter_first { Some(n(51)) } else { None }
        );
        assert_eq!(
            context.retry_policy.max_attempts.get(),
            if setter_first { 3 } else { 1 }
        );
        s.pump.drive(c.harness.close()).unwrap();
        assert!(s.pump.drive(run).is_err());
    }
}
#[test]
fn cancel_and_checkpoint_consume_have_both_public_orders() {
    for cancel_first in [false, true] {
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
        let id = s
            .pump
            .drive(c.harness.steer(user("race")))
            .unwrap()
            .unwrap()
            .entry_id;
        if cancel_first {
            assert_eq!(
                s.pump
                    .drive(c.harness.cancel_queued(id.clone()))
                    .unwrap()
                    .unwrap(),
                CancelQueuedOutcome::Cancelled
            );
        }
        hold.cancel();
        s.pump.drive(run).unwrap().unwrap();
        assert_eq!(
            s.pump
                .drive(c.harness.cancel_queued(id.clone()))
                .unwrap()
                .unwrap(),
            if cancel_first {
                CancelQueuedOutcome::NotFound
            } else {
                CancelQueuedOutcome::AlreadyConsumed
            }
        );
        assert_eq!(
            s.pump
                .drive(c.harness.session().get_entry(id))
                .unwrap()
                .is_some(),
            !cancel_first
        );
    }
}
