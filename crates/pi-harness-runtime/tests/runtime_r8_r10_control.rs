//! Owning structural lifecycle at cancellation and process recovery boundaries.
mod runtime_r1_r4_support;
mod runtime_r5_r7_support;
use pi_agent_schema::*;
use runtime_r1_r4_support::*;
use runtime_r5_r7_support::{Listener, current, factory, manual_until, prompt, user};
use std::sync::{Arc, Mutex};
#[test]
fn in_run_structural_cancel_before_hook_still_closes_compaction_lifecycle() {
    let s = Setup::new();
    let mut options = s.options(DriveMode::Manual);
    options.compaction.enabled = true;
    options.compaction.reserve_tokens = s.faux.model.context_window;
    let h = s.pump.drive(factory(&s).create(options)).unwrap().harness;
    s.pump
        .drive(h.session().append_message(user("old context")))
        .unwrap();
    s.pump
        .drive(h.session().append_message(user("latest context")))
        .unwrap();
    let watch = s.pump.drive(h.watch()).unwrap();
    let events = Arc::new(Listener(Mutex::new(vec![])));
    watch.start(events.clone());
    let mut call = Box::pin(h.prompt(prompt()));
    assert!(poll(call.as_mut()).is_pending());
    manual_until(
        &s,
        h.as_ref(),
        || matches!(current(&s),Some(OperationState::Run(r))if matches!(r.phase,RunPhase::Compaction(_))),
    );
    s.pump.drive(h.abort()).unwrap().unwrap();
    s.pump.drive(h.run_to_completion()).unwrap();
    let result = s.pump.drive(call).unwrap().unwrap();
    assert!(matches!(
        result.outcome,
        RunOutcome::AbortedWithoutAssistant(_)
    ));
    for _ in 0..20 {
        s.pump.tick();
    }
    let events = events.0.lock().unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, HarnessEvent::CompactionStart(_)))
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, HarnessEvent::CompactionEndAborted(_)))
            .count(),
        1
    );
    assert!(s.faux.requests.lock().unwrap().is_empty());
}
#[test]
fn physical_structural_resume_reopens_typed_start_without_run_or_message_replay() {
    let s = Setup::new();
    let h = s
        .pump
        .drive(factory(&s).create(s.options(DriveMode::Manual)))
        .unwrap()
        .harness;
    s.pump
        .drive(h.session().append_message(user("history before crash")))
        .unwrap();
    s.pump
        .drive(h.session().append_message(user("tail before crash")))
        .unwrap();
    let mut call = Box::pin(h.compact(CompactOptions::default()));
    assert!(poll(call.as_mut()).is_pending());
    for _ in 0..100 {
        s.pump.tick();
        if s.pump.drive(h.peek_action()).unwrap().is_some() {
            break;
        }
    }
    assert!(matches!(current(&s), Some(OperationState::Compaction(_))));
    s.pump.drive(h.close()).unwrap();
    assert!(matches!(
        s.pump.drive(call),
        Err(HarnessRuntimeError::Closed(_))
    ));
    drop(h);
    let session = runtime_r5_r7_support::reopen(&s);
    let restored = s
        .pump
        .drive(factory(&s).create(s.options_for(session, DriveMode::Automatic)))
        .unwrap();
    assert_eq!(restored.suspended.len(), 1);
    assert_eq!(restored.suspended[0].kind, OperationKind::Compaction);
    let watch = s.pump.drive(restored.harness.watch()).unwrap();
    let events = Arc::new(Listener(Mutex::new(vec![])));
    watch.start(events.clone());
    let result = s.pump.drive(restored.harness.resume()).unwrap().unwrap();
    assert!(matches!(result, ResumeOutcome::CompactionCompleted(_)));
    for _ in 0..30 {
        s.pump.tick();
    }
    let events = events.0.lock().unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e,HarnessEvent::CompactionStart(e)if e.recovery.is_some()))
            .count(),
        1
    );
    assert!(!events.iter().any(|e| matches!(
        e,
        HarnessEvent::RunStart(_)
            | HarnessEvent::MessageStart(_)
            | HarnessEvent::MessageUpdate(_)
            | HarnessEvent::MessageEnd(_)
    )));
}
#[test]
fn known_between_request_suspension_rejects_resume_but_abort_needs_no_model() {
    let s = Setup::new();
    let adapter = runtime_r5_r7_support::DeferredAdapter::install(&s);
    let h = s
        .pump
        .drive(factory(&s).create(s.options(DriveMode::Automatic)))
        .unwrap()
        .harness;
    for text in ["history turn", "split turn"] {
        s.pump
            .drive(h.prompt(PromptInput::Text {
                text: text.into(),
                images: vec![],
            }))
            .unwrap()
            .unwrap();
    }
    struct LoseModel(Arc<runtime_r5_r7_support::DeferredAdapter>);
    impl HookHandler<AfterResponse> for LoseModel {
        fn call(
            &self,
            _: AfterResponseEvent,
        ) -> BoxFuture<'_, Result<Option<AfterResponseResult>, HandlerError>> {
            Box::pin(async move {
                self.0
                    .missing_models
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                Ok(None)
            })
        }
    }
    let _hook = h
        .hooks()
        .after_response(None, Arc::new(LoseModel(adapter)))
        .unwrap();
    let result = s
        .pump
        .drive(h.compact(CompactOptions::default()))
        .unwrap()
        .unwrap();
    assert!(matches!(
        result.outcome,
        CompactionOutcome::MissingIdentities(_)
    ));
    assert_eq!(s.faux.requests.lock().unwrap().len(), 3);
    let before = s
        .pump
        .drive(
            s.session
                .list_registers(RegisterNamespace::OperationState, None),
        )
        .unwrap();
    let resumed = s.pump.drive(h.resume()).unwrap();
    assert!(
        matches!(resumed, Err(ResumeExpectedError::MissingIdentities(_))),
        "known suspension resume: {resumed:?}"
    );
    assert_eq!(
        s.pump
            .drive(
                s.session
                    .list_registers(RegisterNamespace::OperationState, None)
            )
            .unwrap(),
        before
    );
    s.pump.drive(h.abort()).unwrap().unwrap();
    for _ in 0..100 {
        s.pump.tick();
        if current(&s).is_none() {
            break;
        }
    }
    assert!(matches!(
        s.pump.drive(h.get_last_result()).unwrap(),
        Some(LaneLastResult::CompactionAborted(_))
    ));
    assert_eq!(s.faux.requests.lock().unwrap().len(), 3);
}
#[test]
fn unknown_at_cap_and_cancelled_reopen_use_only_existing_nested_reservations() {
    for request_present in [false, true] {
        for cancelled in [false, true] {
            let s = Setup::new();
            let adapter = runtime_r5_r7_support::DeferredAdapter::install(&s);
            let h = s
                .pump
                .drive(factory(&s).create(s.options(DriveMode::Manual)))
                .unwrap()
                .harness;
            s.pump
                .drive(h.session().append_message(user("source history")))
                .unwrap();
            s.pump
                .drive(h.session().append_message(user("source tail")))
                .unwrap();
            let mut call = Box::pin(h.compact(CompactOptions::default()));
            assert!(poll(call.as_mut()).is_pending());
            manual_until(
                &s,
                h.as_ref(),
                || matches!(current(&s),Some(OperationState::Compaction(CompactionState{structural:StructuralDecision::Generating{generation,..},..}))if matches!(generation.as_ref(),SummaryGeneration::EffectPending{request,..}if request.is_some()==request_present)),
            );
            let Some(OperationState::Compaction(c)) = current(&s) else {
                panic!("compaction pending absent")
            };
            let StructuralDecision::Generating { generation, .. } = c.structural else {
                panic!("generation absent")
            };
            let SummaryGeneration::EffectPending { request, .. } = *generation else {
                panic!("nested intent absent")
            };
            let reserved = request.map(|r| r.usage_id);
            assert!(s.faux.requests.lock().unwrap().is_empty());
            if cancelled {
                s.pump.drive(h.abort()).unwrap().unwrap();
            }
            s.pump.drive(h.close()).unwrap();
            assert!(matches!(
                s.pump.drive(call),
                Err(HarnessRuntimeError::Closed(_))
            ));
            drop(h);
            adapter
                .missing_models
                .store(true, std::sync::atomic::Ordering::SeqCst);
            let reopened_store = s.backing.open(s.time.clone()).unwrap();
            let session = Arc::new(
                s.pump
                    .drive(pi_agent_session::session::RuntimeSession::new(
                        reopened_store.clone(),
                        s.session.metadata(),
                        Arc::new(
                            pi_agent_session::codec::SessionCodec::new(
                                SessionCodecOptions::default(),
                                ProviderHeaderPolicy::default(),
                            )
                            .unwrap(),
                        ),
                        s.ids.clone(),
                        Arc::new(pi_telemetry::NOOP_TELEMETRY_CONTEXT),
                    ))
                    .unwrap(),
            );
            let restored = s
                .pump
                .drive(factory(&s).create(s.options_for(session.clone(), DriveMode::Automatic)))
                .unwrap();
            let result = s.pump.drive(restored.harness.resume()).unwrap().unwrap();
            assert_eq!(
                matches!(result, ResumeOutcome::CompactionAborted(_)),
                cancelled
            );
            assert_eq!(
                matches!(result, ResumeOutcome::CompactionFailed(_)),
                !cancelled
            );
            assert!(s.faux.requests.lock().unwrap().is_empty());
            let usage = s
                .pump
                .drive(pi_agent_session::storage::AtomicStorage::snapshot(
                    reopened_store.as_ref(),
                ))
                .unwrap()
                .usage;
            assert_eq!(usage.len(), usize::from(request_present));
            if let Some(id) = reserved {
                assert_eq!(usage[0].id, id);
                assert_eq!(usage[0].usage, pi_ai_core::Usage::zero());
            }
            assert!(
                s.pump
                    .drive(session.list_registers(RegisterNamespace::OperationPreparation, None))
                    .unwrap()
                    .is_empty()
            );
        }
    }
}
#[test]
fn live_unstarted_nested_cancel_has_no_usage_but_missing_postintent_settles_reserved_usage() {
    for cancel in [true, false] {
        let s = Setup::new();
        let h = s
            .pump
            .drive(factory(&s).create(s.options(DriveMode::Manual)))
            .unwrap()
            .harness;
        s.pump
            .drive(h.session().append_message(user("summary history")))
            .unwrap();
        s.pump
            .drive(h.session().append_message(user("summary tail")))
            .unwrap();
        let mut call = Box::pin(h.compact(CompactOptions::default()));
        assert!(poll(call.as_mut()).is_pending());
        manual_until(
            &s,
            h.as_ref(),
            || matches!(current(&s),Some(OperationState::Compaction(CompactionState{structural:StructuralDecision::Generating{generation,..},..}))if matches!(generation.as_ref(),SummaryGeneration::EffectPending{request:Some(_),..})),
        );
        let Some(OperationState::Compaction(c)) = current(&s) else {
            panic!("pending compaction missing")
        };
        let StructuralDecision::Generating { generation, .. } = c.structural else {
            panic!("generation missing")
        };
        let SummaryGeneration::EffectPending {
            request: Some(request),
            ..
        } = *generation
        else {
            panic!("request missing")
        };
        if cancel {
            s.pump.drive(h.abort()).unwrap().unwrap();
        } else {
            s.pump
                .drive(s.registry.delete_provider(&s.faux.model.provider))
                .unwrap();
        }
        s.pump.drive(h.run_to_completion()).unwrap();
        let result = s.pump.drive(call).unwrap().unwrap();
        assert_eq!(
            matches!(result.outcome, CompactionOutcome::Aborted(_)),
            cancel
        );
        assert_eq!(
            matches!(result.outcome, CompactionOutcome::Failed(_)),
            !cancel
        );
        assert!(s.faux.requests.lock().unwrap().is_empty());
        let snapshot = s
            .pump
            .drive(pi_agent_session::storage::AtomicStorage::snapshot(
                s.store.as_ref(),
            ))
            .unwrap();
        assert_eq!(snapshot.usage.len(), usize::from(!cancel));
        if !cancel {
            assert_eq!(snapshot.usage[0].id, request.usage_id);
            assert_eq!(snapshot.usage[0].usage, pi_ai_core::Usage::zero());
        }
    }
}
#[test]
fn live_summary_retry_keeps_failed_usage_and_captured_options_without_decision_replay() {
    let s = Setup::new();
    let mut options = s.options(DriveMode::Automatic);
    options.retry = RetryPolicy {
        enabled: true,
        max_retries: pi_ai_core::MaxRetries::new(1).unwrap(),
        base_delay_ms: n(10),
    };
    options.stream_options.cache_retention = Some(pi_ai_core::CacheRetention::Short);
    options.stream_options.deferred = Some(pi_ai_core::DeferredRequest::Enabled(true));
    let h = s.pump.drive(factory(&s).create(options)).unwrap().harness;
    for text in ["old history", "split boundary"] {
        s.pump
            .drive(h.prompt(PromptInput::Text {
                text: text.into(),
                images: vec![],
            }))
            .unwrap()
            .unwrap();
    }
    let mut failed = assistant(&s.faux.model, pi_ai_core::StopReason::Error);
    failed.error_message = Some("429 rate limit exceeded".into());
    failed.usage.input = n(11);
    failed.usage.total_tokens = n(11);
    let mut history = assistant(&s.faux.model, pi_ai_core::StopReason::Stop);
    history.usage.input = n(17);
    history.usage.total_tokens = n(17);
    let mut prefix = assistant(&s.faux.model, pi_ai_core::StopReason::Stop);
    prefix.usage.input = n(23);
    prefix.usage.total_tokens = n(23);
    s.faux
        .responses
        .lock()
        .unwrap()
        .extend([failed, history, prefix]);
    struct Decision(Arc<std::sync::atomic::AtomicUsize>);
    impl HookHandler<BeforeCompaction> for Decision {
        fn call(
            &self,
            _: BeforeCompactionEvent,
        ) -> BoxFuture<'_, Result<Option<BeforeCompactionResult>, HandlerError>> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Box::pin(async { Ok(None) })
        }
    }
    struct OptionsHook(Arc<Mutex<Vec<u64>>>);
    impl HookHandler<BeforeRequest> for OptionsHook {
        fn call(
            &self,
            event: BeforeRequestEvent,
        ) -> BoxFuture<'_, Result<Option<BeforeRequestResult>, HandlerError>> {
            Box::pin(async move {
                assert_eq!(event.step, HookRequestStep::Compaction);
                assert_eq!(
                    event.stream_options.cache_retention,
                    Some(pi_ai_core::CacheRetention::None)
                );
                assert_eq!(
                    event.stream_options.deferred,
                    Some(pi_ai_core::DeferredRequest::Enabled(true))
                );
                self.0.lock().unwrap().push(event.attempt.get());
                Ok(Some(serde_json::from_value(serde_json::json!({"streamOptions":{"cacheRetention":"long","deferred":true}})).unwrap()))
            })
        }
    }
    let decisions = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let attempts = Arc::new(Mutex::new(vec![]));
    let _decision = h
        .hooks()
        .before_compaction(None, Arc::new(Decision(decisions.clone())))
        .unwrap();
    let _options = h
        .hooks()
        .before_request(None, Arc::new(OptionsHook(attempts.clone())))
        .unwrap();
    let mut call = Box::pin(h.compact(CompactOptions::default()));
    assert!(poll(call.as_mut()).is_pending());
    runtime_r5_r7_support::tick_until(
        &s,
        || matches!(current(&s),Some(OperationState::Compaction(CompactionState{structural:StructuralDecision::Generating{generation,..},..}))if matches!(generation.as_ref(),SummaryGeneration::RetryWait{..})),
    );
    s.pump
        .drive(h.set_retry_policy(RetryPolicy {
            enabled: false,
            max_retries: pi_ai_core::MaxRetries::new(0).unwrap(),
            base_delay_ms: n(0),
        }))
        .unwrap();
    s.pump
        .drive(h.set_stream_options(AgentHarnessStreamOptions::default()))
        .unwrap();
    s.time.0.store(107, std::sync::atomic::Ordering::SeqCst);
    let result = s.pump.drive(call).unwrap().unwrap();
    let CompactionOutcome::Completed(c) = result.outcome else {
        panic!("retry failed")
    };
    assert_eq!(c.entry.usage.unwrap().input, n(40));
    assert_eq!(decisions.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(*attempts.lock().unwrap(), vec![1, 2, 2]);
    let snapshot = s
        .pump
        .drive(pi_agent_session::storage::AtomicStorage::snapshot(
            s.store.as_ref(),
        ))
        .unwrap();
    assert_eq!(
        snapshot
            .usage
            .iter()
            .map(|u| u.usage.input.get())
            .sum::<u64>(),
        51
    );
    assert_eq!(snapshot.usage.len(), 5);
    let requests = s.faux.requests.lock().unwrap();
    assert_eq!(requests.len(), 5);
    for (context, options) in &requests[2..] {
        assert!(context.tools.is_none());
        assert_eq!(
            options.deferred,
            Some(pi_ai_core::DeferredRequest::Enabled(false))
        );
        assert_eq!(
            options.stream.cache_retention,
            Some(pi_ai_core::CacheRetention::Long)
        );
    }
}
