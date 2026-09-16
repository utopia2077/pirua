//! Public resume over physically reopened Memory, preserving captured generation facts.
mod runtime_r1_r4_support;
mod runtime_telemetry_support;
#[path = "runtime_r1_r4_support/tracking.rs"]
mod tracking;
use pi_agent_schema::*;
use pi_agent_session::{
    codec::SessionCodec,
    session::RuntimeSession,
    storage::{AtomicStorage, StorageSnapshot},
};
use pi_ai_core::{
    ApiAdapter, ApiId, Model, ModelsError, Provider, ProviderAuth, ProviderId, StopReason,
};
use pi_harness_runtime::owner::RuntimeFactory;
use runtime_r1_r4_support::*;
use runtime_telemetry_support::{Recorder, text};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

#[derive(Clone, Copy, Debug, PartialEq)]
enum Stage {
    Pending,
    Ready,
    RetryWait,
}
fn parked(stage: Stage, cancelled: bool, retries: u64) -> (Setup, Option<PendingGeneration>) {
    let s = Setup::new();
    if stage == Stage::RetryWait {
        let mut error = assistant(&s.faux.model, StopReason::Error);
        error.error_message = Some("429 retry fixture".into());
        s.faux.responses.lock().unwrap().push_front(error);
    }
    let f = RuntimeFactory::new(
        s.time.clone(),
        s.pump.clone(),
        s.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    let mut options = s.options(DriveMode::Manual);
    options.retry.enabled = retries != 0;
    options.retry.max_retries = pi_ai_core::MaxRetries::new(retries).unwrap();
    let c = s.pump.drive(f.create(options)).unwrap();
    let mut prompt = Box::pin(c.harness.prompt(PromptInput::Text {
        text: "captured recovery".into(),
        images: vec![],
    }));
    assert!(poll(prompt.as_mut()).is_pending());
    let mut saved = None;
    for _ in 0..500 {
        s.pump.tick();
        for register in s
            .pump
            .drive(
                s.session
                    .list_registers(RegisterNamespace::OperationState, None),
            )
            .unwrap()
        {
            if let Register::OperationState { key, mut value, .. } = register
                && let OperationState::Run(run) = value.as_mut()
                && let RunPhase::Assistant(phase) = &run.phase
            {
                let found = matches!(
                    (&phase.generation, stage),
                    (Generation::EffectPending { .. }, Stage::Pending)
                        | (Generation::Ready { .. }, Stage::Ready)
                        | (Generation::RetryWait { .. }, Stage::RetryWait)
                );
                if found {
                    let pending = PendingGeneration::try_from(phase.generation.clone()).ok();
                    if cancelled {
                        run.control = Control::CancelRequested(CancelRequestedControl {
                            status: Default::default(),
                            requested_at: n(7),
                            drained_steer: vec![],
                            drained_follow_up: vec![],
                        });
                        s.pump
                            .drive(s.session.commit(Transaction {
                                writes: vec![Write::SetRegister(Box::new(
                                    RegisterSet::OperationState { key, value },
                                ))],
                            }))
                            .unwrap();
                    }
                    saved = Some(pending);
                    break;
                }
            }
        }
        if saved.is_some() {
            break;
        }
        s.pump.drive(c.harness.execute_action()).unwrap();
    }
    let saved = saved.expect("requested durable generation position");
    s.pump.drive(c.harness.close()).unwrap();
    drop(prompt);
    (s, saved)
}
struct TrapProvider {
    model: Model,
    auth: ProviderAuth,
    armed: Arc<AtomicBool>,
}
impl Provider for TrapProvider {
    fn id(&self) -> &ProviderId {
        &self.model.provider
    }
    fn name(&self) -> &str {
        "recovery trap"
    }
    fn auth(&self) -> &ProviderAuth {
        assert!(!self.armed.load(Ordering::SeqCst), "recovery auth lookup");
        &self.auth
    }
    fn models(&self) -> Result<Arc<[Model]>, ModelsError> {
        assert!(!self.armed.load(Ordering::SeqCst), "recovery model getter");
        Ok(vec![self.model.clone()].into())
    }
    fn adapter(&self, _: &ApiId) -> Option<Arc<dyn ApiAdapter>> {
        panic!("recovery adapter lookup")
    }
}
struct Traps(Arc<AtomicUsize>);
impl HookHandler<BeforeRequest> for Traps {
    fn call(
        &self,
        _: BeforeRequestEvent,
    ) -> BoxFuture<'_, Result<Option<BeforeRequestResult>, HandlerError>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("synthetic before_request")
    }
}
impl HookHandler<BeforePayload> for Traps {
    fn call(
        &self,
        _: BeforePayloadEvent,
    ) -> BoxFuture<'_, Result<Option<BeforePayloadResult>, HandlerError>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("synthetic before_payload")
    }
}
impl HookHandler<AfterResponse> for Traps {
    fn call(
        &self,
        _: AfterResponseEvent,
    ) -> BoxFuture<'_, Result<Option<AfterResponseResult>, HandlerError>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("synthetic after_response")
    }
}
impl HookHandler<TransformContext> for Traps {
    fn call(
        &self,
        _: TransformContextEvent,
    ) -> BoxFuture<'_, Result<Option<TransformContextResult>, HandlerError>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("synthetic transform_context")
    }
}
impl SystemPromptProvider<()> for Traps {
    fn resolve<'a>(&'a self, _: &'a ()) -> BoxFuture<'a, Result<String, HarnessRuntimeError>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("synthetic prompt context")
    }
}
impl ProviderMessageConverter for Traps {
    fn convert(
        &self,
        _: Vec<AgentMessage>,
    ) -> BoxFuture<'_, Result<Vec<pi_ai_core::Message>, HarnessRuntimeError>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("synthetic conversion")
    }
}
struct ResumeCount(AtomicUsize);
impl HookHandler<BeforeResume> for ResumeCount {
    fn call(&self, _: BeforeResumeEvent) -> BoxFuture<'_, Result<(), HandlerError>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(()) })
    }
}
struct Events(Mutex<Vec<HarnessEvent>>);
impl EventListener for Events {
    fn on_event(&self, event: HarnessEvent) -> BoxFuture<'_, Result<(), HandlerError>> {
        self.0.lock().unwrap().push(event);
        Box::pin(async { Ok(()) })
    }
}
fn recover(
    stage: Stage,
    cancelled: bool,
    manual: bool,
    trapped: bool,
    collision: bool,
) -> StorageSnapshot {
    let (s, pending) = parked(
        stage,
        cancelled,
        if cancelled || stage == Stage::RetryWait {
            3
        } else {
            0
        },
    );
    let prior_requests = s.faux.requests.lock().unwrap().len();
    let storage = s.backing.open(s.time.clone()).unwrap();
    let tracked = Arc::new(tracking::TrackedStorage::new(storage.clone()));
    let session = Arc::new(
        s.pump
            .drive(RuntimeSession::new(
                tracked.clone(),
                s.session.metadata(),
                Arc::new(
                    SessionCodec::new(
                        SessionCodecOptions::default(),
                        ProviderHeaderPolicy::from_allowlist(["x-public".into()]),
                    )
                    .unwrap(),
                ),
                s.ids.clone(),
                Arc::new(pi_telemetry::NOOP_TELEMETRY_CONTEXT),
            ))
            .unwrap(),
    );
    if collision {
        session_commit_usage(&s, &session, pending.as_ref().unwrap().usage_id.clone());
    }
    let before = s.pump.drive(storage.snapshot()).unwrap();
    tracked.reads.lock().unwrap().clear(); // RuntimeSession construction is outside Harness restore.
    let armed = Arc::new(AtomicBool::new(false));
    if trapped {
        s.pump
            .drive(s.registry.set_provider(Arc::new(TrapProvider {
                model: s.faux.model.clone(),
                auth: ProviderAuth::new(Some(Arc::new(Auth)), None).unwrap(),
                armed: armed.clone(),
            })))
            .unwrap();
        armed.store(true, Ordering::SeqCst);
    } else {
        s.pump
            .drive(s.registry.delete_provider(&s.faux.model.provider))
            .unwrap();
    }
    let forbidden_calls = Arc::new(AtomicUsize::new(0));
    let recorder = Arc::new(Recorder::default());
    let f = RuntimeFactory::new(
        s.time.clone(),
        s.pump.clone(),
        s.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    let mut options = s.options_for::<(), _>(
        session.clone(),
        if manual {
            DriveMode::Manual
        } else {
            DriveMode::Automatic
        },
    );
    options.model.provider = ProviderId::new("new-provider").unwrap();
    options.model.id = pi_ai_core::ModelId::new("new-model").unwrap();
    options.model.api = ApiId::new("new-api").unwrap();
    options.retry.enabled = true;
    options.retry.max_retries = pi_ai_core::MaxRetries::new(99).unwrap();
    options.system_prompt = Some(SystemPromptSource::Provider(Arc::new(Traps(
        forbidden_calls.clone(),
    ))));
    options.to_provider_messages = Some(Arc::new(Traps(forbidden_calls.clone())));
    options.telemetry_context = recorder.clone();
    let c = s
        .pump
        .drive(f.create(options))
        .expect("bounded restore ignores prior usage collision");
    assert_eq!(before, s.pump.drive(storage.snapshot()).unwrap());
    assert_eq!(c.suspended.len(), 1);
    assert!(
        !tracked
            .reads
            .lock()
            .unwrap()
            .iter()
            .any(|r| matches!(r.as_str(), "scan_usage" | "scan_entries" | "scan_branch"))
    );
    let hooks = c.harness.hooks();
    let _a = hooks
        .before_request(None, Arc::new(Traps(forbidden_calls.clone())))
        .unwrap();
    let _b = hooks
        .before_payload(None, Arc::new(Traps(forbidden_calls.clone())))
        .unwrap();
    let _c = hooks
        .after_response(None, Arc::new(Traps(forbidden_calls.clone())))
        .unwrap();
    let _d = hooks
        .transform_context(None, Arc::new(Traps(forbidden_calls.clone())))
        .unwrap();
    let resumed = Arc::new(ResumeCount(AtomicUsize::new(0)));
    let _e = hooks
        .before_resume(HandlerId::new("resume-count").unwrap(), resumed.clone())
        .unwrap();
    let events = Arc::new(Events(Mutex::new(vec![])));
    let _listener = c
        .harness
        .events()
        .on(HarnessEventType::Usage, events.clone());
    let _entry = c
        .harness
        .events()
        .on(HarnessEventType::EntryAdded, events.clone());
    let mut resume = Box::pin(c.harness.resume());
    let result = if manual {
        let mut result = None;
        for _ in 0..500 {
            if let std::task::Poll::Ready(value) = poll(resume.as_mut()) {
                result = Some(value);
                break;
            }
            s.pump.tick();
            s.pump.drive(c.harness.execute_action()).unwrap();
        }
        result.expect("manual recovery completed")
    } else {
        s.pump.drive(resume)
    };
    for _ in 0..20 {
        s.pump.tick();
    }
    assert_eq!(resumed.0.load(Ordering::SeqCst), 1);
    assert_eq!(
        forbidden_calls.load(Ordering::SeqCst),
        0,
        "isolated hook panics must not hide forbidden invocations"
    );
    assert_eq!(s.faux.requests.lock().unwrap().len(), prior_requests);
    assert!(
        !tracked
            .reads
            .lock()
            .unwrap()
            .iter()
            .any(|r| r == "scan_usage")
    );
    let after = s.pump.drive(storage.snapshot()).unwrap();
    if collision {
        assert!(matches!(result, Err(HarnessRuntimeError::Fault(_))));
        assert_eq!(
            after, before,
            "usage collision must reject entire response/state transaction"
        );
        assert!(events.0.lock().unwrap().is_empty());
        assert!(
            s.pump
                .drive(session.get_entries(&[pending.unwrap().response_entry_id]))
                .unwrap()
                .is_empty()
        );
    } else {
        let success = result.unwrap().unwrap();
        if cancelled {
            assert!(matches!(
                success,
                ResumeOutcome::RunAbortedWithAssistant(_)
                    | ResumeOutcome::RunAbortedWithoutAssistant(_)
            ));
        } else {
            assert!(matches!(success, ResumeOutcome::RunFailedWithAssistant(_)));
        }
        if let Some(pending) = pending {
            let entries = s
                .pump
                .drive(session.get_entries(std::slice::from_ref(&pending.response_entry_id)))
                .unwrap();
            let Entry::Message(entry) = entries.get(&pending.response_entry_id).unwrap() else {
                panic!()
            };
            let AgentMessage::BuiltIn(pi_ai_core::Message::Assistant(message)) = &entry.message
            else {
                panic!()
            };
            assert_eq!(message.api.as_str(), "unknown");
            assert_eq!(
                message.provider,
                pending.context.configuration.model.provider
            );
            assert_eq!(message.model, pending.context.configuration.model.model_id);
            assert!(message.content.is_empty());
            assert_eq!(message.usage, pi_ai_core::Usage::zero());
            assert_eq!(message.timestamp, n(7));
            assert_eq!(
                message.stop_reason,
                if cancelled {
                    StopReason::Aborted
                } else {
                    StopReason::Error
                }
            );
            assert_eq!(after.usage.len(), before.usage.len() + 1);
            let usage = after
                .usage
                .iter()
                .find(|u| u.id == pending.usage_id)
                .unwrap();
            assert_eq!(usage.entry_id, Some(pending.response_entry_id));
            assert_eq!(usage.usage, pi_ai_core::Usage::zero());
            assert_eq!(
                events
                    .0
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|e| matches!(e, HarnessEvent::Usage(_)))
                    .count(),
                1
            );
            assert_eq!(
                events
                    .0
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|e| matches!(e, HarnessEvent::EntryAdded(_)))
                    .count(),
                1
            );
            let rows = recorder.rows();
            assert!(rows.iter().filter(|r| r.name == "pi.harness.step").all(
                |r| r.parent.is_some()
                    && r.status
                        == Some(if cancelled {
                            pi_telemetry::SpanStatus::Ok
                        } else {
                            pi_telemetry::SpanStatus::Error { error: None }
                        })
            ));
            assert!(rows.iter().any(|r| r.name == "pi.harness.step"
                && text(r, "pi.step.outcome").as_deref()
                    == Some(if cancelled { "aborted" } else { "failed" })));
        } else {
            assert_eq!(after.usage, before.usage);
        }
    }
    assert!(
        recorder
            .rows()
            .iter()
            .all(|r| r.name != "pi.ai.request" && r.name != "pi.harness.sleep")
    );
    recorder.assert_finished();
    after
}
fn session_commit_usage(
    s: &Setup,
    session: &RuntimeSession<tracking::TrackedStorage>,
    id: UsageId,
) {
    s.pump
        .drive(session.commit(Transaction {
            writes: vec![Write::Usage(NewUsageRow {
                id,
                usage: pi_ai_core::Usage::zero(),
                entry_id: None,
                adjustment: true,
                details: pi_ai_core::Presence::Missing,
            })],
        }))
        .unwrap();
}
#[test]
fn reopened_pending_cap_uses_captured_ids_without_provider_and_matches_manual() {
    assert_eq!(
        recover(Stage::Pending, false, false, false, false),
        recover(Stage::Pending, false, true, true, false)
    );
}
#[test]
fn reopened_cancelled_pending_below_cap_uses_aborted_zero_usage_without_provider() {
    assert_eq!(
        recover(Stage::Pending, true, false, true, false),
        recover(Stage::Pending, true, true, false, false)
    );
}
#[test]
fn cancelled_ready_and_retry_wait_skip_identity_and_request_work() {
    recover(Stage::Ready, true, false, true, false);
    recover(Stage::RetryWait, true, false, false, false);
}
#[test]
fn usage_collision_restores_but_rejects_synthetic_settlement_atomically() {
    recover(Stage::Pending, false, false, true, true);
}

#[test]
fn below_cap_reopen_advances_captured_budget_despite_new_disabled_retry() {
    let (s, pending) = parked(Stage::Pending, false, 1);
    let pending = pending.unwrap();
    let storage = s.backing.open(s.time.clone()).unwrap();
    let session = Arc::new(
        s.pump
            .drive(RuntimeSession::new(
                storage.clone(),
                s.session.metadata(),
                Arc::new(
                    SessionCodec::new(
                        SessionCodecOptions::default(),
                        ProviderHeaderPolicy::from_allowlist(["x-public".into()]),
                    )
                    .unwrap(),
                ),
                s.ids.clone(),
                Arc::new(pi_telemetry::NOOP_TELEMETRY_CONTEXT),
            ))
            .unwrap(),
    );
    let f = RuntimeFactory::new(
        s.time.clone(),
        s.pump.clone(),
        s.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    let c = s
        .pump
        .drive(f.create(s.options_for::<(), _>(session.clone(), DriveMode::Automatic)))
        .unwrap();
    let result = s.pump.drive(c.harness.resume()).unwrap().unwrap();
    assert!(matches!(
        result,
        ResumeOutcome::RunCompletedWithAssistant(_)
    ));
    assert_eq!(s.faux.requests.lock().unwrap().len(), 1);
    assert!(
        s.pump
            .drive(session.get_entries(&[pending.response_entry_id]))
            .unwrap()
            .is_empty()
    );
    let snapshot = s.pump.drive(storage.snapshot()).unwrap();
    assert_eq!(snapshot.usage.len(), 1);
    assert_ne!(snapshot.usage[0].id, pending.usage_id);
}
#[test]
fn live_at_cap_waits_for_actual_response_instead_of_synthetic_recovery() {
    let s = Setup::new();
    let release = pi_ai_core::CancellationToken::new();
    *s.faux.hold.lock().unwrap() = Some(release.clone());
    let f = RuntimeFactory::new(
        s.time.clone(),
        s.pump.clone(),
        s.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    let c = s
        .pump
        .drive(f.create(s.options(DriveMode::Automatic)))
        .unwrap();
    let mut prompt = Box::pin(c.harness.prompt(PromptInput::Text {
        text: "live at cap".into(),
        images: vec![],
    }));
    assert!(poll(prompt.as_mut()).is_pending());
    for _ in 0..100 {
        s.pump.tick();
    }
    assert_eq!(s.faux.requests.lock().unwrap().len(), 1);
    assert!(s.pump.drive(s.store.snapshot()).unwrap().usage.is_empty());
    assert!(poll(prompt.as_mut()).is_pending());
    release.cancel();
    assert!(matches!(
        s.pump.drive(prompt).unwrap().unwrap().outcome,
        RunOutcome::CompletedWithAssistant(_)
    ));
    let rows = s.pump.drive(s.store.snapshot()).unwrap();
    assert_eq!(rows.usage.len(), 1);
    let entries = s
        .pump
        .drive(
            s.session
                .get_entries(&[rows.usage[0].entry_id.clone().unwrap()]),
        )
        .unwrap();
    assert!(entries.values().any(|entry|matches!(entry,Entry::Message(MessageEntry {message:AgentMessage::BuiltIn(pi_ai_core::Message::Assistant(message)),..}) if message.api==s.faux.model.api && message.stop_reason==StopReason::Stop)));
}
