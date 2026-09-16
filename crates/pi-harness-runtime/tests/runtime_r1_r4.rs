//! Actual public runtime histories over Memory and the real ModelsRegistry.
mod runtime_r1_r4_support;
use pi_agent_schema::*;
use pi_agent_session::storage::AtomicStorage;
use pi_harness_runtime::owner::RuntimeFactory;
use runtime_r1_r4_support::*;
use std::{
    sync::{Arc, Mutex},
    task::Poll,
};
fn factory(s: &Setup) -> RuntimeFactory<(), pi_agent_session::memory::MemoryStorage> {
    RuntimeFactory::new(
        s.time.clone(),
        s.pump.clone(),
        s.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    )
}
#[test]
fn configured_public_create_is_effect_free_and_claims_actual_backend() {
    let s = Setup::new();
    let before = s.pump.drive(s.store.snapshot()).unwrap();
    let f = factory(&s);
    let created = s
        .pump
        .drive(f.create(s.options(DriveMode::Automatic)))
        .unwrap();
    assert!(created.suspended.is_empty());
    assert!(s.faux.requests.lock().unwrap().is_empty());
    assert_eq!(before, s.pump.drive(s.store.snapshot()).unwrap());
    assert!(
        s.pump
            .drive(f.create(s.options(DriveMode::Automatic)))
            .is_err()
    );
    assert_eq!(s.pump.drive(created.harness.lanes()).unwrap().len(), 1);
    s.pump.drive(created.harness.close()).unwrap();
}
#[test]
fn actual_public_prompt_commits_response_usage_and_complete_terminal_cleanup() {
    let s = Setup::new();
    let f = factory(&s);
    let created = s
        .pump
        .drive(f.create(s.options(DriveMode::Automatic)))
        .unwrap();
    let result = s
        .pump
        .drive(created.harness.prompt(PromptInput::Text {
            text: "hello".into(),
            images: vec![],
        }))
        .unwrap()
        .unwrap();
    assert!(matches!(
        result.outcome,
        RunOutcome::CompletedWithAssistant(_)
    ));
    assert_eq!(s.faux.requests.lock().unwrap().len(), 1);
    let snapshot = s.pump.drive(s.store.snapshot()).unwrap();
    assert_eq!(snapshot.entries.len(), 2);
    assert_eq!(snapshot.usage.len(), 1);
    assert!(!snapshot.registers.iter().any(|r| matches!(
        r,
        Register::OperationMeta { .. }
            | Register::OperationState { .. }
            | Register::OperationToolArguments { .. }
            | Register::OperationPreparation { .. }
    )));
    assert!(matches!(
        s.pump.drive(created.harness.get_last_result()).unwrap(),
        Some(LaneLastResult::RunCompletedAssistant(_))
    ));
}
#[test]
fn manual_parking_has_zero_writes_or_provider_effects_and_drop_does_not_cancel_admitted_run() {
    let s = Setup::new();
    let f = factory(&s);
    let created = s
        .pump
        .drive(f.create(s.options(DriveMode::Manual)))
        .unwrap();
    let before = s.pump.drive(s.store.snapshot()).unwrap();
    let mut prompt = Box::pin(created.harness.prompt(PromptInput::Text {
        text: "hello".into(),
        images: vec![],
    }));
    assert!(poll(prompt.as_mut()).is_pending());
    for _ in 0..10 {
        s.pump.tick();
    }
    assert_eq!(
        before,
        s.pump.drive(s.store.snapshot()).unwrap(),
        "parked action must not publish"
    );
    assert!(s.faux.requests.lock().unwrap().is_empty());
    assert!(
        s.pump
            .drive(created.harness.peek_action())
            .unwrap()
            .is_some()
    );
    drop(prompt);
    for _ in 0..500 {
        let _ = s.pump.drive(created.harness.execute_action()).unwrap();
        s.pump.tick();
    }
    assert!(
        s.pump
            .drive(created.harness.get_last_result())
            .unwrap()
            .is_some()
    );
    assert_eq!(s.faux.requests.lock().unwrap().len(), 1);
}
#[test]
fn automatic_and_manual_public_runs_have_identical_durable_trace() {
    let run = |mode| {
        let s = Setup::new();
        let f = factory(&s);
        let c = s.pump.drive(f.create(s.options(mode))).unwrap();
        let mut prompt = Box::pin(c.harness.prompt(PromptInput::Text {
            text: "same".into(),
            images: vec![],
        }));
        for _ in 0..1000 {
            if let Poll::Ready(value) = poll(prompt.as_mut()) {
                value.unwrap().unwrap();
                return s.pump.drive(s.store.snapshot()).unwrap();
            }
            s.pump.tick();
            if mode == DriveMode::Manual {
                let _ = s.pump.drive(c.harness.execute_action()).unwrap();
            }
        }
        panic!("run did not finish");
    };
    assert_eq!(run(DriveMode::Automatic), run(DriveMode::Manual));
}
#[test]
fn close_rejects_parked_effect_without_durable_cancel_or_terminal_write() {
    let s = Setup::new();
    let f = factory(&s);
    let c = s
        .pump
        .drive(f.create(s.options(DriveMode::Manual)))
        .unwrap();
    let mut prompt = Box::pin(c.harness.prompt(PromptInput::Text {
        text: "park".into(),
        images: vec![],
    }));
    assert!(poll(prompt.as_mut()).is_pending());
    for _ in 0..5 {
        s.pump.tick();
    }
    s.pump.drive(c.harness.close()).unwrap();
    assert!(s.pump.drive(prompt).is_err());
    assert!(s.faux.requests.lock().unwrap().is_empty());
}
struct Listener(Mutex<Vec<HarnessEvent>>);
impl EventListener for Listener {
    fn on_event(&self, event: HarnessEvent) -> BoxFuture<'_, Result<(), HandlerError>> {
        Box::pin(async move {
            self.0.lock().unwrap().push(event);
            Ok(())
        })
    }
}
#[test]
fn snapshot_buffer_precedes_later_passive_delivery() {
    let s = Setup::new();
    let f = factory(&s);
    let c = s
        .pump
        .drive(f.create(s.options(DriveMode::Automatic)))
        .unwrap();
    let watch = s.pump.drive(c.harness.watch()).unwrap();
    assert!(watch.snapshot().transcript.is_empty());
    s.pump
        .drive(c.harness.prompt(PromptInput::Text {
            text: "buffer".into(),
            images: vec![],
        }))
        .unwrap()
        .unwrap();
    let listener = Arc::new(Listener(Mutex::new(vec![])));
    watch.start(listener.clone());
    for _ in 0..10 {
        s.pump.tick();
    }
    assert_eq!(
        listener
            .0
            .lock()
            .unwrap()
            .iter()
            .filter(|e| matches!(e, HarnessEvent::EntryAdded(_)))
            .count(),
        2
    );
    assert!(watch.snapshot().transcript.is_empty());
}
struct ContextValue(u64);
struct ContextProvider(std::sync::atomic::AtomicUsize);
impl ToolContextProvider<ContextValue> for ContextProvider {
    fn resolve(&self) -> BoxFuture<'_, Result<ContextValue, HarnessRuntimeError>> {
        Box::pin(async move {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(ContextValue(37))
        })
    }
}
struct Tool {
    descriptor: AgentTool,
    release: pi_ai_core::CancellationToken,
    log: Arc<Mutex<Vec<String>>>,
}
impl AgentHarnessTool<ContextValue> for Tool {
    fn descriptor(&self) -> &AgentTool {
        &self.descriptor
    }
    fn prepare_arguments(
        &self,
        args: pi_ai_core::JsonValue,
    ) -> Result<pi_ai_core::JsonObject, ToolExecutionError> {
        self.log
            .lock()
            .unwrap()
            .push(format!("prepare:{}", self.descriptor.name.as_str()));
        let pi_ai_core::JsonValue::Object(args) = args else {
            panic!("fixture expects object");
        };
        let mut args = args.into_map();
        args.insert("prepared".into(), pi_ai_core::JsonValue::Bool(true));
        Ok(pi_ai_core::JsonObject::new(args))
    }
    fn execute<'a>(
        &'a self,
        _: pi_ai_core::ToolCallId,
        args: pi_ai_core::JsonObject,
        _: &'a dyn Cancellation,
        _: &'a dyn ToolProgressSink,
        context: &'a ContextValue,
    ) -> BoxFuture<'a, Result<AgentToolResult, ToolExecutionError>> {
        Box::pin(async move {
            assert_eq!(context.0, 37);
            assert_eq!(
                args.as_map().get("prepared"),
                Some(&pi_ai_core::JsonValue::Bool(true))
            );
            self.log
                .lock()
                .unwrap()
                .push(format!("start:{}", self.descriptor.name.as_str()));
            self.release.cancelled().await;
            self.log
                .lock()
                .unwrap()
                .push(format!("raw:{}", self.descriptor.name.as_str()));
            let mut usage = pi_ai_core::Usage::zero();
            usage.input = n(3);
            usage.total_tokens = n(3);
            Ok(AgentToolResult {
                content: vec![],
                details: pi_ai_core::JsonValue::String(self.descriptor.name.as_str().into()),
                usage: Some(usage),
                added_tool_names: None,
                terminate: None,
            })
        })
    }
}
struct AfterToolLog(Arc<Mutex<Vec<String>>>);
impl HookHandler<AfterTool> for AfterToolLog {
    fn call(
        &self,
        event: AfterToolEvent,
    ) -> BoxFuture<'_, Result<Option<AfterToolResult>, HandlerError>> {
        Box::pin(async move {
            self.0
                .lock()
                .unwrap()
                .push(format!("after:{}", event.tool_name.as_str()));
            Ok(None)
        })
    }
}
fn tool(
    name: &str,
    log: Arc<Mutex<Vec<String>>>,
    release: pi_ai_core::CancellationToken,
) -> Arc<Tool> {
    Arc::new(Tool {descriptor:AgentTool {name:pi_ai_core::ToolName::new(name).unwrap(),label:name.into(),description:name.into(),parameters:serde_json::from_value(serde_json::json!({"type":"object","properties":{"prepared":{"type":"boolean"}},"additionalProperties":false})).unwrap(),constrained_sampling:None,execution_mode:None,replay:Some(ToolReplay::Safe)},log,release})
}
#[test]
fn actual_parallel_tools_share_one_context_persist_args_and_finalize_in_source_order() {
    use pi_ai_core::{
        AssistantContent, CancellationToken, StopReason, ToolCall, ToolCallId, ToolName,
    };
    let s = Setup::new();
    let log = Arc::new(Mutex::new(vec![]));
    let a = CancellationToken::new();
    let b = CancellationToken::new();
    let mut response = assistant(&s.faux.model, StopReason::ToolUse);
    response.content = ["a", "b"]
        .into_iter()
        .map(|name| {
            AssistantContent::ToolCall(ToolCall {
                content_type: Default::default(),
                id: ToolCallId::new(format!("call-{name}")).unwrap(),
                name: ToolName::new(name).unwrap(),
                arguments: Default::default(),
                thought_signature: None,
                namespace: None,
            })
        })
        .collect();
    *s.faux.responses.lock().unwrap() =
        vec![response, assistant(&s.faux.model, StopReason::Stop)].into();
    let provider = Arc::new(ContextProvider(std::sync::atomic::AtomicUsize::new(0)));
    let f = RuntimeFactory::new(
        s.time.clone(),
        s.pump.clone(),
        s.ids.clone(),
        Arc::new(ContextValue(9)),
        Arc::new(Summary),
    );
    let mut options = s.options(DriveMode::Automatic);
    options.tools = vec![
        tool("a", log.clone(), a.clone()),
        tool("b", log.clone(), b.clone()),
    ];
    options.tool_context = Some(AgentHarnessToolContextSource::Provider(provider.clone()));
    let c = s.pump.drive(f.create(options)).unwrap();
    s.pump
        .drive(c.harness.set_active_tools(vec![
            ToolName::new("a").unwrap(),
            ToolName::new("b").unwrap(),
        ]))
        .unwrap();
    let _subscription = c
        .harness
        .hooks()
        .after_tool(None, Arc::new(AfterToolLog(log.clone())))
        .unwrap();
    let mut prompt = Box::pin(c.harness.prompt(PromptInput::Text {
        text: "tools".into(),
        images: vec![],
    }));
    assert!(poll(prompt.as_mut()).is_pending());
    for _ in 0..150 {
        s.pump.tick();
    }
    assert!(log.lock().unwrap().contains(&"start:a".into()));
    assert!(log.lock().unwrap().contains(&"start:b".into()));
    let snapshot = s.pump.drive(s.store.snapshot()).unwrap();
    assert_eq!(
        snapshot
            .registers
            .iter()
            .filter(|r| matches!(r, Register::OperationToolArguments { .. }))
            .count(),
        2
    );
    b.cancel();
    for _ in 0..50 {
        s.pump.tick();
    }
    assert!(log.lock().unwrap().contains(&"raw:b".into()));
    assert!(!log.lock().unwrap().contains(&"after:b".into()));
    a.cancel();
    s.pump.drive(prompt).unwrap().unwrap();
    let events = log.lock().unwrap();
    assert!(
        events.iter().position(|s| s == "after:a").unwrap()
            < events.iter().position(|s| s == "after:b").unwrap()
    );
    assert_eq!(provider.0.load(std::sync::atomic::Ordering::SeqCst), 1);
    let snapshot = s.pump.drive(s.store.snapshot()).unwrap();
    assert_eq!(snapshot.usage.len(), 4);
    assert!(
        !snapshot
            .registers
            .iter()
            .any(|r| matches!(r, Register::OperationToolArguments { .. }))
    );
}
fn reopen(
    s: &Setup,
) -> Arc<pi_agent_session::session::RuntimeSession<pi_agent_session::memory::MemoryStorage>> {
    let store = s.backing.open(s.time.clone()).unwrap();
    Arc::new(
        s.pump
            .drive(pi_agent_session::session::RuntimeSession::new(
                store,
                s.session.metadata(),
                Arc::new(
                    pi_agent_session::codec::SessionCodec::new(
                        SessionCodecOptions::default(),
                        ProviderHeaderPolicy::from_allowlist(["x-public".into()]),
                    )
                    .unwrap(),
                ),
                s.ids.clone(),
                Arc::new(pi_telemetry::NOOP_TELEMETRY_CONTEXT),
            ))
            .unwrap(),
    )
}
fn tool_response(s: &Setup) -> pi_ai_core::AssistantMessage {
    let mut response = assistant(&s.faux.model, pi_ai_core::StopReason::ToolUse);
    response.content = vec![pi_ai_core::AssistantContent::ToolCall(
        pi_ai_core::ToolCall {
            content_type: Default::default(),
            id: pi_ai_core::ToolCallId::new("call-a").unwrap(),
            name: pi_ai_core::ToolName::new("a").unwrap(),
            arguments: Default::default(),
            thought_signature: None,
            namespace: None,
        },
    )];
    response
}
#[test]
fn safe_reopen_executes_actual_saved_arguments_without_preflight_or_before_tool_again() {
    let s = Setup::new();
    let log = Arc::new(Mutex::new(vec![]));
    let release = pi_ai_core::CancellationToken::new();
    let binding = tool("a", log.clone(), release.clone());
    let context = Arc::new(ContextProvider(std::sync::atomic::AtomicUsize::new(0)));
    *s.faux.responses.lock().unwrap() = vec![
        tool_response(&s),
        assistant(&s.faux.model, pi_ai_core::StopReason::Stop),
    ]
    .into();
    let f = RuntimeFactory::new(
        s.time.clone(),
        s.pump.clone(),
        s.ids.clone(),
        Arc::new(ContextValue(9)),
        Arc::new(Summary),
    );
    let mut options = s.options(DriveMode::Automatic);
    options.tools = vec![binding.clone()];
    options.tool_context = Some(AgentHarnessToolContextSource::Provider(context.clone()));
    let c = s.pump.drive(f.create(options)).unwrap();
    s.pump
        .drive(
            c.harness
                .set_active_tools(vec![pi_ai_core::ToolName::new("a").unwrap()]),
        )
        .unwrap();
    let mut prompt = Box::pin(c.harness.prompt(PromptInput::Text {
        text: "safe".into(),
        images: vec![],
    }));
    assert!(poll(prompt.as_mut()).is_pending());
    for _ in 0..100 {
        s.pump.tick();
    }
    assert!(log.lock().unwrap().contains(&"start:a".into()));
    s.pump.drive(c.harness.close()).unwrap();
    drop(prompt);
    let session = reopen(&s);
    let mut options = s.options(DriveMode::Automatic);
    options.session = session.clone();
    options.tools = vec![binding];
    options.tool_context = Some(AgentHarnessToolContextSource::Provider(context.clone()));
    let restored = s.pump.drive(f.create(options)).unwrap();
    assert_eq!(restored.suspended.len(), 1);
    let before = log.lock().unwrap().clone();
    assert_eq!(
        before.iter().filter(|s| s.as_str() == "prepare:a").count(),
        1
    );
    release.cancel();
    s.pump.drive(restored.harness.resume()).unwrap().unwrap();
    let events = log.lock().unwrap();
    assert_eq!(
        events.iter().filter(|s| s.as_str() == "prepare:a").count(),
        1
    );
    assert_eq!(events.iter().filter(|s| s.as_str() == "start:a").count(), 2);
    assert_eq!(context.0.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert!(
        s.pump
            .drive(session.list_registers(RegisterNamespace::OperationToolArguments, None))
            .unwrap()
            .is_empty()
    );
}
#[test]
fn stale_tool_output_after_external_finalization_writes_nothing_and_never_recreates_operation() {
    let s = Setup::new();
    let log = Arc::new(Mutex::new(vec![]));
    let release = pi_ai_core::CancellationToken::new();
    *s.faux.responses.lock().unwrap() = vec![tool_response(&s)].into();
    let f = RuntimeFactory::new(
        s.time.clone(),
        s.pump.clone(),
        s.ids.clone(),
        Arc::new(ContextValue(37)),
        Arc::new(Summary),
    );
    let mut options = s.options(DriveMode::Automatic);
    options.tools = vec![tool("a", log.clone(), release.clone())];
    let c = s.pump.drive(f.create(options)).unwrap();
    s.pump
        .drive(
            c.harness
                .set_active_tools(vec![pi_ai_core::ToolName::new("a").unwrap()]),
        )
        .unwrap();
    let mut prompt = Box::pin(c.harness.prompt(PromptInput::Text {
        text: "stale".into(),
        images: vec![],
    }));
    assert!(poll(prompt.as_mut()).is_pending());
    for _ in 0..100 {
        s.pump.tick();
    }
    let Some(Register::LaneState { value, .. }) = s
        .pump
        .drive(s.session.get_register(RegisterNamespace::LaneState, "main"))
        .unwrap()
    else {
        panic!("lane state");
    };
    let id = value.current_operation_id.unwrap();
    s.pump
        .drive(s.session.commit(Transaction {
            writes: vec![
                Write::DeleteRegister(RegisterDelete {
                    namespace: RegisterNamespace::OperationMeta,
                    key: id.as_str().into(),
                }),
                Write::DeleteRegister(RegisterDelete {
                    namespace: RegisterNamespace::OperationState,
                    key: id.as_str().into(),
                }),
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
    let before = s.pump.drive(s.store.snapshot()).unwrap();
    release.cancel();
    assert!(s.pump.drive(prompt).is_err());
    assert_eq!(before, s.pump.drive(s.store.snapshot()).unwrap());
    assert_eq!(s.faux.requests.lock().unwrap().len(), 1);
}
#[test]
fn provider_replacement_after_intent_cannot_dispatch_the_replacement_binding() {
    let s = Setup::new();
    let f = factory(&s);
    let c = s
        .pump
        .drive(f.create(s.options(DriveMode::Manual)))
        .unwrap();
    let mut prompt = Box::pin(c.harness.prompt(PromptInput::Text {
        text: "replace".into(),
        images: vec![],
    }));
    assert!(poll(prompt.as_mut()).is_pending());
    let mut pending = false;
    for _ in 0..500 {
        s.pump.tick();
        let registers = s
            .pump
            .drive(
                s.session
                    .list_registers(RegisterNamespace::OperationState, None),
            )
            .unwrap();
        pending=registers.iter().any(|r|matches!(r,Register::OperationState {value,..} if matches!(value.as_ref(),OperationState::Run(run) if matches!(&run.phase,RunPhase::Assistant(AssistantPhase {generation:Generation::EffectPending{..},..})))));
        if pending {
            break;
        }
        let _ = s.pump.drive(c.harness.execute_action()).unwrap();
    }
    assert!(pending);
    let replacement = Arc::new(Faux::new(s.faux.model.clone()));
    s.pump
        .drive(s.registry.set_provider(Arc::new(Product {
            faux: replacement.clone(),
            auth: pi_ai_core::ProviderAuth::new(Some(Arc::new(Auth)), None).unwrap(),
        })))
        .unwrap();
    for _ in 0..1000 {
        s.pump.tick();
        if let Poll::Ready(result) = poll(prompt.as_mut()) {
            result.unwrap().unwrap();
            assert!(
                replacement.requests.lock().unwrap().is_empty(),
                "replacement must fail in-band after intent"
            );
            assert!(s.faux.requests.lock().unwrap().is_empty());
            return;
        }
        let _ = s.pump.drive(c.harness.execute_action()).unwrap();
    }
    panic!("replacement run did not finish");
}
#[test]
fn retry_wait_uses_the_injected_clock_and_captured_policy_then_settles_both_attempts() {
    let s = Setup::new();
    let mut first = assistant(&s.faux.model, pi_ai_core::StopReason::Error);
    first.error_message = Some("429 rate limit exceeded".into());
    *s.faux.responses.lock().unwrap() = vec![
        first,
        assistant(&s.faux.model, pi_ai_core::StopReason::Stop),
    ]
    .into();
    let f = factory(&s);
    let mut options = s.options(DriveMode::Automatic);
    options.retry.enabled = true;
    options.retry.max_retries = pi_ai_core::MaxRetries::new(1).unwrap();
    let c = s.pump.drive(f.create(options)).unwrap();
    let mut prompt = Box::pin(c.harness.prompt(PromptInput::Text {
        text: "retry".into(),
        images: vec![],
    }));
    assert!(poll(prompt.as_mut()).is_pending());
    for _ in 0..100 {
        s.pump.tick();
    }
    assert_eq!(s.faux.requests.lock().unwrap().len(), 1);
    let states = s
        .pump
        .drive(
            s.session
                .list_registers(RegisterNamespace::OperationState, None),
        )
        .unwrap();
    assert!(states.iter().any(|r|matches!(r,Register::OperationState {value,..} if matches!(value.as_ref(),OperationState::Run(run) if matches!(&run.phase,RunPhase::Assistant(AssistantPhase {generation:Generation::RetryWait {not_before,..},..}) if *not_before==n(17))))));
    s.pump
        .drive(c.harness.set_retry_policy(pi_ai_core::RetryPolicy {
            enabled: false,
            max_retries: pi_ai_core::MaxRetries::new(0).unwrap(),
            base_delay_ms: n(999),
        }))
        .unwrap();
    s.time.0.store(17, std::sync::atomic::Ordering::SeqCst);
    s.pump.drive(prompt).unwrap().unwrap();
    assert_eq!(s.faux.requests.lock().unwrap().len(), 2);
    assert_eq!(s.pump.drive(s.store.snapshot()).unwrap().usage.len(), 2);
}
#[test]
fn restore_unknown_generation_below_cap_abandons_old_reservations_without_usage_or_dispatch() {
    let s = Setup::new();
    let f = factory(&s);
    let mut options = s.options(DriveMode::Manual);
    options.retry.enabled = true;
    options.retry.max_retries = pi_ai_core::MaxRetries::new(1).unwrap();
    let c = s.pump.drive(f.create(options)).unwrap();
    let mut prompt = Box::pin(c.harness.prompt(PromptInput::Text {
        text: "unknown".into(),
        images: vec![],
    }));
    assert!(poll(prompt.as_mut()).is_pending());
    let mut reserved = None;
    for _ in 0..500 {
        s.pump.tick();
        for r in s
            .pump
            .drive(
                s.session
                    .list_registers(RegisterNamespace::OperationState, None),
            )
            .unwrap()
        {
            if let Register::OperationState { value, .. } = r
                && let OperationState::Run(run) = *value
                && let RunPhase::Assistant(AssistantPhase {
                    generation:
                        Generation::EffectPending {
                            response_entry_id,
                            usage_id,
                            ..
                        },
                    ..
                }) = run.phase
            {
                reserved = Some((response_entry_id, usage_id));
            }
        }
        if reserved.is_some() {
            break;
        }
        let _ = s.pump.drive(c.harness.execute_action()).unwrap();
    }
    let reserved = reserved.unwrap();
    assert!(s.faux.requests.lock().unwrap().is_empty());
    s.pump.drive(c.harness.close()).unwrap();
    drop(prompt);
    let session = reopen(&s);
    let mut options = s.options(DriveMode::Automatic);
    options.session = session.clone();
    let restored = s.pump.drive(f.create(options)).unwrap();
    assert_eq!(restored.suspended.len(), 1);
    assert!(s.faux.requests.lock().unwrap().is_empty());
    s.pump.drive(restored.harness.resume()).unwrap().unwrap();
    assert_eq!(s.faux.requests.lock().unwrap().len(), 1);
    assert!(
        s.pump
            .drive(session.get_entries(std::slice::from_ref(&reserved.0)))
            .unwrap()
            .is_empty()
    );
    let storage = s.backing.open(s.time.clone()).unwrap();
    let snapshot = s.pump.drive(storage.snapshot()).unwrap();
    assert_eq!(snapshot.usage.len(), 1);
}
#[path = "runtime_r1_r4_support/tracking.rs"]
mod tracking;
#[test]
fn public_restore_is_bounded_by_named_state_and_retries_only_after_actual_publication() {
    let s = Setup::new();
    let writes = (0..10000)
        .map(|index| {
            Write::Entry(NewEntry::Custom(NewCustomEntry {
                id: EntryId::new(format!("unrelated-{index}")).unwrap(),
                parent_id: None,
                entry_type: CustomEntryType::Custom,
                custom_type: CustomType::new("unrelated").unwrap(),
                data: pi_ai_core::Presence::Missing,
            }))
        })
        .collect();
    s.pump
        .drive(s.session.commit(Transaction { writes }))
        .unwrap();
    let tracked = Arc::new(tracking::TrackedStorage::new(s.store.clone()));
    let session = Arc::new(
        s.pump
            .drive(pi_agent_session::session::RuntimeSession::new(
                tracked.clone(),
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
    tracked.reads.lock().unwrap().clear();
    tracked
        .change_and_omit
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let f = RuntimeFactory::new(
        s.time.clone(),
        s.pump.clone(),
        s.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    let options = s.options_for::<(), _>(session, DriveMode::Automatic);
    let c = s.pump.drive(f.create(options)).unwrap();
    assert!(c.suspended.is_empty());
    let reads = tracked.reads.lock().unwrap();
    assert!(!reads.iter().any(|r| matches!(
        r.as_str(),
        "snapshot"
            | "scan_entries"
            | "scan_branch"
            | "scan_branch_structure"
            | "scan_usage"
            | "stats"
    )));
    assert_eq!(
        reads.iter().filter(|r| r.as_str() == "read_stamp").count(),
        4
    );
    assert_eq!(
        reads
            .iter()
            .filter(|r| r.as_str() == "get_register")
            .count(),
        4
    );
    assert!(s.faux.requests.lock().unwrap().is_empty());
}
#[test]
fn stable_configured_restore_error_is_not_retried_or_seeded() {
    let s = Setup::new();
    let tracked = Arc::new(tracking::TrackedStorage::new(s.store.clone()));
    let session = Arc::new(
        s.pump
            .drive(pi_agent_session::session::RuntimeSession::new(
                tracked.clone(),
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
    tracked.reads.lock().unwrap().clear();
    tracked
        .stable_omit
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let before = s.pump.drive(s.store.snapshot()).unwrap();
    let f = RuntimeFactory::new(
        s.time.clone(),
        s.pump.clone(),
        s.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    assert!(
        s.pump
            .drive(f.create(s.options_for::<(), _>(session, DriveMode::Automatic)))
            .is_err()
    );
    assert_eq!(
        tracked
            .reads
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r.as_str() == "read_stamp")
            .count(),
        2
    );
    assert_eq!(before, s.pump.drive(s.store.snapshot()).unwrap());
}
#[test]
fn public_skill_and_template_expansion_matches_pinned_resource_formatting() {
    let s = Setup::new();
    let f = factory(&s);
    let mut options = s.options(DriveMode::Automatic);
    options.resources = Resources {
        skills: Some(vec![Skill {
            name: "review".into(),
            description: "fixture".into(),
            content: "Read this".into(),
            file_path: "C:\\skills\\review\\SKILL.md".into(),
            disable_model_invocation: None,
        }]),
        prompt_templates: Some(vec![PromptTemplate {
            name: "expand".into(),
            content: "$1|$0|$9|${@:2:2}|${@:0:0}|$ARGUMENTS|$@".into(),
            description: None,
        }]),
    };
    let c = s.pump.drive(f.create(options)).unwrap();
    s.pump
        .drive(c.harness.skill("review".into(), Some("Extra".into())))
        .unwrap()
        .unwrap();
    s.pump
        .drive(c.harness.prompt_from_template(
            "expand".into(),
            vec!["one".into(), "two".into(), "three".into()],
        ))
        .unwrap()
        .unwrap();
    let snapshot = s.pump.drive(s.store.snapshot()).unwrap();
    let text: Vec<_> = snapshot
        .entries
        .iter()
        .filter_map(|entry| match entry {
            Entry::Message(MessageEntry {
                message:
                    AgentMessage::BuiltIn(pi_ai_core::Message::User(pi_ai_core::UserMessage {
                        content: pi_ai_core::UserContent::Text(text),
                        ..
                    })),
                ..
            }) => Some(text.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(
        text[0],
        "<skill name=\"review\" location=\"C:\\skills\\review\\SKILL.md\">\nReferences are relative to C:\\skills\\review.\n\nRead this\n</skill>\n\nExtra"
    );
    assert_eq!(text[1], "one|||two three||one two three|one two three");
}
struct PromptProvider(std::sync::atomic::AtomicUsize);
impl SystemPromptProvider<ContextValue> for PromptProvider {
    fn resolve<'a>(
        &'a self,
        context: &'a ContextValue,
    ) -> BoxFuture<'a, Result<String, HarnessRuntimeError>> {
        Box::pin(async move {
            let count = self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(format!("context={} evaluation={count}", context.0))
        })
    }
}
struct FixedPrompt;
impl HookHandler<BeforeRun> for FixedPrompt {
    fn call(
        &self,
        event: BeforeRunEvent,
    ) -> BoxFuture<'_, Result<Option<BeforeRunResult>, HandlerError>> {
        Box::pin(async move {
            assert_eq!(event.system_prompt, "context=37 evaluation=0");
            Ok(Some(BeforeRunResult {
                system_prompt: Some("fixed override".into()),
                ..Default::default()
            }))
        })
    }
}
#[test]
fn functional_prompt_previews_once_then_rechecks_context_unless_fixed_override() {
    for fixed in [false, true] {
        let s = Setup::new();
        let context = Arc::new(ContextProvider(std::sync::atomic::AtomicUsize::new(0)));
        let prompt = Arc::new(PromptProvider(std::sync::atomic::AtomicUsize::new(0)));
        let f = RuntimeFactory::new(
            s.time.clone(),
            s.pump.clone(),
            s.ids.clone(),
            Arc::new(ContextValue(9)),
            Arc::new(Summary),
        );
        let mut options = s.options(DriveMode::Automatic);
        options.tool_context = Some(AgentHarnessToolContextSource::Provider(context.clone()));
        options.system_prompt = Some(SystemPromptSource::Provider(prompt.clone()));
        let c = s.pump.drive(f.create(options)).unwrap();
        let _subscription = if fixed {
            Some(
                c.harness
                    .hooks()
                    .before_run(HandlerId::new("fixed").unwrap(), Arc::new(FixedPrompt))
                    .unwrap(),
            )
        } else {
            None
        };
        s.pump
            .drive(c.harness.prompt(PromptInput::Text {
                text: "context".into(),
                images: vec![],
            }))
            .unwrap()
            .unwrap();
        let expected = if fixed { 1 } else { 2 };
        assert_eq!(
            context.0.load(std::sync::atomic::Ordering::SeqCst),
            expected
        );
        assert_eq!(prompt.0.load(std::sync::atomic::Ordering::SeqCst), expected);
        assert_eq!(
            s.faux.requests.lock().unwrap()[0]
                .0
                .system_prompt
                .as_deref(),
            Some(if fixed {
                "fixed override"
            } else {
                "context=37 evaluation=1"
            })
        );
    }
}
#[derive(Clone)]
struct HeaderHook(AgentHarnessStreamOptionsPatch);
impl HookHandler<BeforeRequest> for HeaderHook {
    fn call(
        &self,
        _: BeforeRequestEvent,
    ) -> BoxFuture<'_, Result<Option<BeforeRequestResult>, HandlerError>> {
        Box::pin(async move {
            Ok(Some(BeforeRequestResult {
                stream_options: Some(self.0.clone()),
            }))
        })
    }
}
struct PayloadHook;
impl HookHandler<BeforePayload> for PayloadHook {
    fn call(
        &self,
        _: BeforePayloadEvent,
    ) -> BoxFuture<'_, Result<Option<BeforePayloadResult>, HandlerError>> {
        Box::pin(async {
            Ok(Some(BeforePayloadResult {
                payload: pi_ai_core::JsonValue::String("actual hook payload".into()),
            }))
        })
    }
}
#[test]
fn complete_clear_recase_before_request_options_and_payload_reach_the_actual_provider() {
    let s = Setup::new();
    let f = factory(&s);
    let policy = ProviderHeaderPolicy::from_allowlist(["x-public".into()]);
    let mut options = s.options(DriveMode::Manual);
    options.stream_options.headers = Some(
        RawProviderHeaders::new([("X-Public".into(), "old".into())].into())
            .unwrap()
            .classify(&policy)
            .unwrap(),
    );
    let c = s.pump.drive(f.create(options)).unwrap();
    let _first = c
        .harness
        .hooks()
        .before_request(
            None,
            Arc::new(HeaderHook(AgentHarnessStreamOptionsPatch {
                headers: pi_ai_core::Presence::Null,
                ..Default::default()
            })),
        )
        .unwrap();
    let _second = c
        .harness
        .hooks()
        .before_request(
            None,
            Arc::new(HeaderHook(AgentHarnessStreamOptionsPatch {
                headers: pi_ai_core::Presence::Value(
                    RawProviderHeaderPatch::new([("x-public".into(), Some("new".into()))].into())
                        .unwrap()
                        .classify(&policy)
                        .unwrap(),
                ),
                timeout_ms: Some(n(123)),
                ..Default::default()
            })),
        )
        .unwrap();
    let _payload = c
        .harness
        .hooks()
        .before_payload(None, Arc::new(PayloadHook))
        .unwrap();
    let mut prompt = Box::pin(c.harness.prompt(PromptInput::Text {
        text: "headers".into(),
        images: vec![],
    }));
    for _ in 0..1000 {
        if let Poll::Ready(result) = poll(prompt.as_mut()) {
            result.unwrap().unwrap();
            let headers = s.faux.headers.lock().unwrap();
            assert_eq!(
                headers[0].as_map().get("x-public"),
                Some(&Some("new".into()))
            );
            assert!(!headers[0].as_map().contains_key("X-Public"));
            assert_eq!(
                s.faux.requests.lock().unwrap()[0].1.stream.timeout_ms,
                Some(n(123))
            );
            assert_eq!(
                *s.faux.payloads.lock().unwrap(),
                vec![serde_json::Value::String("actual hook payload".into())]
            );
            return;
        }
        s.pump.tick();
        let _ = s.pump.drive(c.harness.execute_action()).unwrap();
    }
    panic!("nested payload action did not complete");
}
#[test]
fn acceptance_cas_loss_reloads_and_terminal_preserves_later_lane_owned_pending() {
    let s = Setup::new();
    let tracked = Arc::new(tracking::TrackedStorage::new(s.store.clone()));
    let session = Arc::new(
        s.pump
            .drive(pi_agent_session::session::RuntimeSession::new(
                tracked.clone(),
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
    tracked
        .late_pending
        .store(true, std::sync::atomic::Ordering::SeqCst);
    s.pump
        .drive(c.harness.prompt(PromptInput::Text {
            text: "caller".into(),
            images: vec![],
        }))
        .unwrap_or_else(|error| panic!("{error}"))
        .unwrap();
    let Some(Register::LaneState { value, .. }) = s
        .pump
        .drive(session.get_register(RegisterNamespace::LaneState, "main"))
        .unwrap()
    else {
        panic!("state");
    };
    assert_eq!(
        value.pending_next_run,
        vec![EntryId::new("late-next").unwrap()],
        "stale CAS must preserve newer lane state"
    );
    assert!(
        s.pump
            .drive(session.get_register(RegisterNamespace::PendingEntry, "late-next"))
            .unwrap()
            .is_some()
    );
    assert_eq!(s.faux.requests.lock().unwrap().len(), 1);
    assert_eq!(s.pump.drive(s.store.snapshot()).unwrap().entries.len(), 2);
}
struct PendingBeforeRun(Arc<std::sync::atomic::AtomicBool>);
impl HookHandler<BeforeRun> for PendingBeforeRun {
    fn call(
        &self,
        _: BeforeRunEvent,
    ) -> BoxFuture<'_, Result<Option<BeforeRunResult>, HandlerError>> {
        Box::pin(async move {
            self.0.store(true, std::sync::atomic::Ordering::SeqCst);
            std::future::pending().await
        })
    }
}
#[test]
fn close_retires_noncooperative_started_hook_and_rejects_its_retained_waiter() {
    let s = Setup::new();
    let f = factory(&s);
    let c = s
        .pump
        .drive(f.create(s.options(DriveMode::Automatic)))
        .unwrap();
    let started = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let _handler = c
        .harness
        .hooks()
        .before_run(
            HandlerId::new("pending").unwrap(),
            Arc::new(PendingBeforeRun(started.clone())),
        )
        .unwrap();
    let before = s.pump.drive(s.store.snapshot()).unwrap();
    let mut prompt = Box::pin(c.harness.prompt(PromptInput::Text {
        text: "pending hook".into(),
        images: vec![],
    }));
    assert!(poll(prompt.as_mut()).is_pending());
    for _ in 0..10 {
        s.pump.tick();
    }
    assert!(started.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(before, s.pump.drive(s.store.snapshot()).unwrap());
    s.pump.drive(c.harness.close()).unwrap();
    assert!(matches!(
        s.pump.drive(prompt),
        Err(HarnessRuntimeError::Closed(_))
    ));
}
#[test]
fn close_retires_noncooperative_retry_clock_without_another_attempt() {
    let s = Setup::new();
    let mut response = assistant(&s.faux.model, pi_ai_core::StopReason::Error);
    response.error_message = Some("429 rate limit exceeded".into());
    *s.faux.responses.lock().unwrap() = vec![response].into();
    let f = factory(&s);
    let mut options = s.options(DriveMode::Automatic);
    options.retry.enabled = true;
    options.retry.max_retries = pi_ai_core::MaxRetries::new(1).unwrap();
    let c = s.pump.drive(f.create(options)).unwrap();
    let mut prompt = Box::pin(c.harness.prompt(PromptInput::Text {
        text: "pending clock".into(),
        images: vec![],
    }));
    assert!(poll(prompt.as_mut()).is_pending());
    for _ in 0..100 {
        s.pump.tick();
    }
    assert_eq!(s.faux.requests.lock().unwrap().len(), 1);
    s.pump.drive(c.harness.close()).unwrap();
    assert!(matches!(
        s.pump.drive(prompt),
        Err(HarnessRuntimeError::Closed(_))
    ));
    assert_eq!(s.faux.requests.lock().unwrap().len(), 1);
}
#[test]
fn rejected_host_spawn_has_zero_durable_writes_and_no_provider_dispatch() {
    let s = Setup::new();
    let f = factory(&s);
    let c = s
        .pump
        .drive(f.create(s.options(DriveMode::Automatic)))
        .unwrap();
    let before = s.pump.drive(s.store.snapshot()).unwrap();
    s.pump
        .reject
        .store(true, std::sync::atomic::Ordering::SeqCst);
    assert!(
        s.pump
            .drive(c.harness.prompt(PromptInput::Text {
                text: "reject".into(),
                images: vec![]
            }))
            .is_err()
    );
    assert_eq!(before, s.pump.drive(s.store.snapshot()).unwrap());
    assert!(s.faux.requests.lock().unwrap().is_empty());
}
struct Inject(AgentMessage);
impl HookHandler<BeforeRun> for Inject {
    fn call(
        &self,
        _: BeforeRunEvent,
    ) -> BoxFuture<'_, Result<Option<BeforeRunResult>, HandlerError>> {
        Box::pin(async move {
            Ok(Some(BeforeRunResult {
                messages: Some(vec![self.0.clone()]),
                ..Default::default()
            }))
        })
    }
}
#[test]
fn public_prompt_and_before_run_accept_valid_settled_assistant_and_tool_result_messages() {
    for tool_result in [false, true] {
        for injected in [false, true] {
            let s = Setup::new();
            let message = if tool_result {
                AgentMessage::BuiltIn(pi_ai_core::Message::ToolResult(Box::new(
                    pi_ai_core::ToolResultMessage {
                        role: Default::default(),
                        tool_call_id: pi_ai_core::ToolCallId::new("caller-call").unwrap(),
                        tool_name: pi_ai_core::ToolName::new("caller-tool").unwrap(),
                        content: vec![],
                        details: pi_ai_core::Presence::Missing,
                        usage: None,
                        added_tool_names: None,
                        is_error: false,
                        timestamp: n(7),
                    },
                )))
            } else {
                AgentMessage::BuiltIn(pi_ai_core::Message::Assistant(Box::new(assistant(
                    &s.faux.model,
                    pi_ai_core::StopReason::Stop,
                ))))
            };
            let f = factory(&s);
            let c = s
                .pump
                .drive(f.create(s.options(DriveMode::Automatic)))
                .unwrap();
            let subscription = if injected {
                Some(
                    c.harness
                        .hooks()
                        .before_run(
                            HandlerId::new("inject").unwrap(),
                            Arc::new(Inject(message.clone())),
                        )
                        .unwrap(),
                )
            } else {
                None
            };
            let input = if injected {
                PromptInput::Text {
                    text: "with injection".into(),
                    images: vec![],
                }
            } else {
                PromptInput::Messages(vec![message])
            };
            s.pump.drive(c.harness.prompt(input)).unwrap().unwrap();
            assert_eq!(
                s.pump.drive(s.store.snapshot()).unwrap().entries.len(),
                if injected { 3 } else { 2 }
            );
            drop(subscription);
        }
    }
}
struct EndMark(Arc<std::sync::atomic::AtomicUsize>);
impl HookHandler<BeforeRunEnd> for EndMark {
    fn call(
        &self,
        _: BeforeRunEndEvent,
    ) -> BoxFuture<'_, Result<Option<BeforeRunEndResult>, HandlerError>> {
        Box::pin(async move {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(None)
        })
    }
}
#[test]
fn before_run_end_approval_has_no_noop_write_and_terminal_has_its_own_gate() {
    let s = Setup::new();
    let f = factory(&s);
    let c = s
        .pump
        .drive(f.create(s.options(DriveMode::Manual)))
        .unwrap();
    let ended = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let _handler = c
        .harness
        .hooks()
        .before_run_end(None, Arc::new(EndMark(ended.clone())))
        .unwrap();
    let mut prompt = Box::pin(c.harness.prompt(PromptInput::Text {
        text: "terminal gate".into(),
        images: vec![],
    }));
    assert!(poll(prompt.as_mut()).is_pending());
    for _ in 0..1000 {
        s.pump.tick();
        if ended.load(std::sync::atomic::Ordering::SeqCst) > 0 {
            break;
        }
        let _ = s.pump.drive(c.harness.execute_action()).unwrap();
    }
    assert_eq!(ended.load(std::sync::atomic::Ordering::SeqCst), 1);
    for _ in 0..10 {
        s.pump.tick();
    }
    assert_eq!(
        s.pump.drive(c.harness.peek_action()).unwrap().unwrap().kind,
        "commit_settlement"
    );
    let before = s.pump.drive(s.store.snapshot()).unwrap();
    s.pump.drive(c.harness.execute_action()).unwrap();
    for _ in 0..10 {
        s.pump.tick();
    }
    assert_eq!(
        s.pump.drive(c.harness.peek_action()).unwrap().unwrap().kind,
        "commit_terminal"
    );
    assert_eq!(before, s.pump.drive(s.store.snapshot()).unwrap());
    s.pump.drive(c.harness.execute_action()).unwrap();
    s.pump.drive(prompt).unwrap().unwrap();
    assert!(s.pump.drive(c.harness.get_last_result()).unwrap().is_some());
}
#[test]
fn empty_tool_use_continues_without_tool_effects_and_genuine_length_executes_none() {
    for truncated in [false, true] {
        let s = Setup::new();
        let log = Arc::new(Mutex::new(vec![]));
        let mut response = if truncated {
            tool_response(&s)
        } else {
            assistant(&s.faux.model, pi_ai_core::StopReason::ToolUse)
        };
        if truncated {
            response.stop_reason = pi_ai_core::StopReason::Length;
            response.usage.output = s.faux.model.max_tokens;
            response.usage.total_tokens = s.faux.model.max_tokens;
        }
        *s.faux.responses.lock().unwrap() = vec![
            response,
            assistant(&s.faux.model, pi_ai_core::StopReason::Stop),
        ]
        .into();
        let f = RuntimeFactory::new(
            s.time.clone(),
            s.pump.clone(),
            s.ids.clone(),
            Arc::new(ContextValue(37)),
            Arc::new(Summary),
        );
        let mut options = s.options(DriveMode::Automatic);
        options.tools = vec![tool("a", log.clone(), pi_ai_core::CancellationToken::new())];
        let c = s.pump.drive(f.create(options)).unwrap();
        s.pump
            .drive(
                c.harness
                    .set_active_tools(vec![pi_ai_core::ToolName::new("a").unwrap()]),
            )
            .unwrap();
        s.pump
            .drive(c.harness.prompt(PromptInput::Text {
                text: "edge".into(),
                images: vec![],
            }))
            .unwrap()
            .unwrap();
        assert!(log.lock().unwrap().is_empty());
        assert_eq!(s.faux.requests.lock().unwrap().len(), 2);
        assert!(
            s.pump
                .drive(
                    s.session
                        .list_registers(RegisterNamespace::OperationToolArguments, None)
                )
                .unwrap()
                .is_empty()
        );
    }
}
#[test]
fn first_deferred_suspension_is_durable_and_restore_does_not_poll() {
    let s = Setup::new();
    let mut response = assistant(&s.faux.model, pi_ai_core::StopReason::Deferred);
    response.deferred = Some(pi_ai_core::DeferredHandle {
        provider: s.faux.model.provider.clone(),
        model_id: s.faux.model.id.clone(),
        api: s.faux.model.api.clone(),
        id: pi_ai_core::DeferredId::new("deferred-fixture").unwrap(),
        expires_at: None,
        poll_after_ms: None,
        data: pi_ai_core::Presence::Missing,
    });
    *s.faux.responses.lock().unwrap() = vec![response].into();
    let f = factory(&s);
    let c = s
        .pump
        .drive(f.create(s.options(DriveMode::Automatic)))
        .unwrap();
    assert!(matches!(
        s.pump
            .drive(c.harness.prompt(PromptInput::Text {
                text: "deferred".into(),
                images: vec![]
            }))
            .unwrap()
            .unwrap()
            .outcome,
        RunOutcome::Deferred(_)
    ));
    assert!(s.pump.drive(c.harness.get_last_result()).unwrap().is_none());
    s.pump.drive(c.harness.close()).unwrap();
    let session = reopen(&s);
    let mut options = s.options(DriveMode::Automatic);
    options.session = session;
    let restored = s.pump.drive(f.create(options)).unwrap();
    assert_eq!(
        restored.suspended[0].reason,
        SuspendedOperationReason::Deferred
    );
    assert!(restored.suspended[0].deferred.is_some());
    assert_eq!(restored.suspended[0].prompt.as_ref().unwrap().len(), 1);
    assert_eq!(s.faux.requests.lock().unwrap().len(), 1);
}
#[test]
fn dropped_public_waiter_cannot_hide_stable_storage_fault_from_retained_owner() {
    let s = Setup::new();
    let tracked = Arc::new(tracking::TrackedStorage::new(s.store.clone()));
    let session = Arc::new(
        s.pump
            .drive(pi_agent_session::session::RuntimeSession::new(
                tracked.clone(),
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
    let f = RuntimeFactory::new(
        s.time.clone(),
        s.pump.clone(),
        s.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    let c = s
        .pump
        .drive(f.create(s.options_for::<(), _>(session, DriveMode::Automatic)))
        .unwrap();
    let before = s.pump.drive(s.store.snapshot()).unwrap();
    let mut prompt = Box::pin(c.harness.prompt(PromptInput::Text {
        text: "dropped fault".into(),
        images: vec![],
    }));
    assert!(poll(prompt.as_mut()).is_pending());
    drop(prompt);
    tracked
        .fail_reads
        .store(true, std::sync::atomic::Ordering::SeqCst);
    for _ in 0..10 {
        s.pump.tick();
    }
    tracked
        .fail_reads
        .store(false, std::sync::atomic::Ordering::SeqCst);
    assert!(
        matches!(
            s.pump.drive(c.harness.get_stream_options()),
            Err(HarnessRuntimeError::Fault(_))
        ),
        "retained task must seal owner without its waiter"
    );
    assert_eq!(before, s.pump.drive(s.store.snapshot()).unwrap());
    assert!(s.faux.requests.lock().unwrap().is_empty());
}
fn tracked_session(
    s: &Setup,
    tracked: Arc<tracking::TrackedStorage>,
) -> Arc<pi_agent_session::session::RuntimeSession<tracking::TrackedStorage>> {
    Arc::new(
        s.pump
            .drive(pi_agent_session::session::RuntimeSession::new(
                tracked,
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
    )
}
#[test]
fn tool_settlement_cas_loss_keeps_the_completed_output_and_nonzero_usage() {
    let s = Setup::new();
    let tracked = Arc::new(tracking::TrackedStorage::new(s.store.clone()));
    let session = tracked_session(&s, tracked.clone());
    let release = pi_ai_core::CancellationToken::new();
    release.cancel();
    let log = Arc::new(Mutex::new(vec![]));
    let mut binding = tool("a", log.clone(), release);
    Arc::get_mut(&mut binding).unwrap().descriptor.replay = Some(ToolReplay::Never);
    *s.faux.responses.lock().unwrap() = vec![
        tool_response(&s),
        assistant(&s.faux.model, pi_ai_core::StopReason::Stop),
    ]
    .into();
    let f = RuntimeFactory::new(
        s.time.clone(),
        s.pump.clone(),
        s.ids.clone(),
        Arc::new(ContextValue(37)),
        Arc::new(Summary),
    );
    let mut options = s.options_for(session, DriveMode::Automatic);
    options.tools = vec![binding];
    let c = s.pump.drive(f.create(options)).unwrap();
    s.pump
        .drive(
            c.harness
                .set_active_tools(vec![pi_ai_core::ToolName::new("a").unwrap()]),
        )
        .unwrap();
    tracked
        .late_result
        .store(true, std::sync::atomic::Ordering::SeqCst);
    s.pump
        .drive(c.harness.prompt(PromptInput::Text {
            text: "retain result".into(),
            images: vec![],
        }))
        .unwrap()
        .unwrap();
    let snapshot = s.pump.drive(s.store.snapshot()).unwrap();
    let result = snapshot
        .entries
        .iter()
        .find_map(|entry| match entry {
            Entry::Message(MessageEntry {
                message: AgentMessage::BuiltIn(pi_ai_core::Message::ToolResult(result)),
                ..
            }) => Some(result),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        result.details,
        pi_ai_core::Presence::Value(pi_ai_core::JsonValue::String("a".into())),
        "CAS retry must keep actual raw tool output"
    );
    assert_eq!(result.usage.as_ref().unwrap().input, n(3));
    assert_eq!(snapshot.usage.len(), 3);
    assert_eq!(
        log.lock()
            .unwrap()
            .iter()
            .filter(|s| s.as_str() == "start:a")
            .count(),
        1
    );
}
struct ToolLifecycleTrace(Arc<Mutex<Vec<String>>>);
impl EventListener for ToolLifecycleTrace {
    fn on_event(&self, event: HarnessEvent) -> BoxFuture<'_, Result<(), HandlerError>> {
        Box::pin(async move {
            let tag = match event {
                HarnessEvent::MessageStart(event)
                    if matches!(
                        event.message,
                        LiveAgentMessage::Settled(AgentMessage::BuiltIn(
                            pi_ai_core::Message::ToolResult(_)
                        ))
                    ) =>
                {
                    Some("start")
                }
                HarnessEvent::MessageEnd(event)
                    if matches!(
                        event.message,
                        AgentMessage::BuiltIn(pi_ai_core::Message::ToolResult(_))
                    ) =>
                {
                    Some("end")
                }
                HarnessEvent::EntryAdded(event)
                    if matches!(
                        event.entry,
                        Entry::Message(MessageEntry {
                            message: AgentMessage::BuiltIn(pi_ai_core::Message::ToolResult(_)),
                            ..
                        })
                    ) =>
                {
                    Some("entry")
                }
                _ => None,
            };
            if let Some(tag) = tag {
                self.0.lock().unwrap().push(tag.into());
            }
            Ok(())
        })
    }
}
#[test]
fn synthetic_tool_lifecycle_precedes_the_actual_backend_commit() {
    let s = Setup::new();
    let tracked = Arc::new(tracking::TrackedStorage::new(s.store.clone()));
    let session = tracked_session(&s, tracked.clone());
    let log = Arc::new(Mutex::new(vec![]));
    let mut response = tool_response(&s);
    response.stop_reason = pi_ai_core::StopReason::Length;
    response.usage.output = s.faux.model.max_tokens;
    response.usage.total_tokens = s.faux.model.max_tokens;
    *s.faux.responses.lock().unwrap() = vec![
        response,
        assistant(&s.faux.model, pi_ai_core::StopReason::Stop),
    ]
    .into();
    let f = RuntimeFactory::new(
        s.time.clone(),
        s.pump.clone(),
        s.ids.clone(),
        Arc::new(ContextValue(37)),
        Arc::new(Summary),
    );
    let mut options = s.options_for(session, DriveMode::Automatic);
    options.tools = vec![tool("a", log.clone(), pi_ai_core::CancellationToken::new())];
    let c = s.pump.drive(f.create(options)).unwrap();
    s.pump
        .drive(
            c.harness
                .set_active_tools(vec![pi_ai_core::ToolName::new("a").unwrap()]),
        )
        .unwrap();
    let watch = s.pump.drive(c.harness.watch()).unwrap();
    watch.start(Arc::new(ToolLifecycleTrace(tracked.commit_trace.clone())));
    s.pump
        .drive(c.harness.prompt(PromptInput::Text {
            text: "synthetic lifecycle".into(),
            images: vec![],
        }))
        .unwrap()
        .unwrap();
    for _ in 0..10 {
        s.pump.tick();
    }
    assert_eq!(
        *tracked.commit_trace.lock().unwrap(),
        vec!["start", "end", "commit", "entry"]
    );
    assert!(log.lock().unwrap().is_empty());
}
#[test]
fn host_dropping_an_accepted_unpolled_task_seals_owner_without_public_waiter() {
    let s = Setup::new();
    for _ in 0..10 {
        s.pump.tick();
    }
    let f = factory(&s);
    let c = s
        .pump
        .drive(f.create(s.options(DriveMode::Automatic)))
        .unwrap();
    let mut prompt = Box::pin(c.harness.prompt(PromptInput::Text {
        text: "host drop".into(),
        images: vec![],
    }));
    assert!(poll(prompt.as_mut()).is_pending());
    drop(prompt);
    let tasks = std::mem::take(&mut *s.pump.tasks.lock().unwrap());
    assert!(!tasks.is_empty());
    drop(tasks);
    assert!(
        matches!(
            s.pump.drive(c.harness.get_stream_options()),
            Err(HarnessRuntimeError::Fault(_))
        ),
        "accepted task drop must fault even with no waiter"
    );
    assert!(s.faux.requests.lock().unwrap().is_empty());
}
struct PanickingContext(bool, Arc<std::sync::atomic::AtomicUsize>);
impl ToolContextProvider<ContextValue> for PanickingContext {
    fn resolve(&self) -> BoxFuture<'_, Result<ContextValue, HarnessRuntimeError>> {
        self.1.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        assert!(!self.0, "trusted context construction panic");
        Box::pin(async move {
            self.1.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            panic!("trusted context poll panic")
        })
    }
}
#[test]
fn trusted_context_constructor_and_poll_panic_are_owned_after_waiter_drop() {
    for construct in [false, true] {
        let s = Setup::new();
        let f = RuntimeFactory::new(
            s.time.clone(),
            s.pump.clone(),
            s.ids.clone(),
            Arc::new(ContextValue(37)),
            Arc::new(Summary),
        );
        let mut options = s.options(DriveMode::Automatic);
        let invoked = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        options.tool_context = Some(AgentHarnessToolContextSource::Provider(Arc::new(
            PanickingContext(construct, invoked.clone()),
        )));
        options.system_prompt = Some(SystemPromptSource::Provider(Arc::new(PromptProvider(
            std::sync::atomic::AtomicUsize::new(0),
        ))));
        let c = s.pump.drive(f.create(options)).unwrap();
        let mut prompt = Box::pin(c.harness.prompt(PromptInput::Text {
            text: "panic".into(),
            images: vec![],
        }));
        assert!(poll(prompt.as_mut()).is_pending());
        drop(prompt);
        let polled = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            for _ in 0..10 {
                s.pump.tick();
            }
        }));
        assert!(
            polled.is_ok(),
            "retained boundary must isolate trusted task panic"
        );
        assert_eq!(
            invoked.load(std::sync::atomic::Ordering::SeqCst),
            if construct { 1 } else { 2 }
        );
        assert!(
            matches!(
                s.pump.drive(c.harness.get_stream_options()),
                Err(HarnessRuntimeError::Fault(_))
            ),
            "trusted panic must fault owner without a waiter"
        );
        assert!(s.faux.requests.lock().unwrap().is_empty());
    }
}
#[test]
fn session_and_lane_snapshots_agree_while_real_provider_is_pending() {
    let s = Setup::new();
    let hold = pi_ai_core::CancellationToken::new();
    *s.faux.hold.lock().unwrap() = Some(hold.clone());
    let f = factory(&s);
    let c = s
        .pump
        .drive(f.create(s.options(DriveMode::Automatic)))
        .unwrap();
    let mut prompt = Box::pin(c.harness.prompt(PromptInput::Text {
        text: "snapshot running".into(),
        images: vec![],
    }));
    assert!(poll(prompt.as_mut()).is_pending());
    for _ in 0..100 {
        s.pump.tick();
    }
    assert_eq!(s.faux.requests.lock().unwrap().len(), 1);
    let lane = s.pump.drive(c.harness.watch()).unwrap().snapshot();
    let session = s.pump.drive(c.harness.watch_session()).unwrap().snapshot();
    let info = s.pump.drive(c.harness.lanes()).unwrap();
    assert_eq!(
        lane.operation.as_ref().unwrap().status,
        OperationSnapshotStatus::Running
    );
    assert!(lane.operation.as_ref().unwrap().streaming_message.is_some());
    assert_eq!(
        session.lanes[0].operation.as_ref().unwrap().status,
        OperationSnapshotStatus::Running
    );
    assert!(session.lanes[0].suspended.is_none());
    assert_eq!(
        info[0].operation.as_ref().unwrap().status,
        OperationSnapshotStatus::Running
    );
    hold.cancel();
    s.pump.drive(prompt).unwrap().unwrap();
}
#[test]
fn admitted_commit_survives_waiter_drop_and_close_waits_for_its_definitive_publication() {
    let s = Setup::new();
    let tracked = Arc::new(tracking::TrackedStorage::new(s.store.clone()));
    let session = tracked_session(&s, tracked.clone());
    let f = RuntimeFactory::new(
        s.time.clone(),
        s.pump.clone(),
        s.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    let c = s
        .pump
        .drive(f.create(s.options_for::<(), _>(session, DriveMode::Automatic)))
        .unwrap();
    tracked
        .pause_entry_commit
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let mut prompt = Box::pin(c.harness.prompt(PromptInput::Text {
        text: "admitted".into(),
        images: vec![],
    }));
    assert!(poll(prompt.as_mut()).is_pending());
    for _ in 0..20 {
        s.pump.tick();
    }
    assert!(tracked.commit_entered.is_cancelled());
    drop(prompt);
    let mut close = Box::pin(c.harness.close());
    assert!(poll(close.as_mut()).is_pending());
    for _ in 0..10 {
        s.pump.tick();
    }
    assert!(poll(close.as_mut()).is_pending());
    assert!(s.pump.drive(s.store.snapshot()).unwrap().entries.is_empty());
    tracked.release_commit.cancel();
    s.pump.drive(close).unwrap();
    let reopened = reopen(&s);
    let entries = s
        .pump
        .drive(reopened.find_entries(EntryQuery::default()))
        .unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(
        s.pump
            .drive(reopened.list_registers(RegisterNamespace::OperationState, None))
            .unwrap()
            .len(),
        1
    );
    assert!(
        s.pump
            .drive(reopened.get_register(RegisterNamespace::LaneLastResult, "main"))
            .unwrap()
            .is_none()
    );
    assert!(s.faux.requests.lock().unwrap().is_empty());
}
#[test]
fn unsafe_and_cancelled_pending_tools_reconcile_without_reexecution() {
    for cancelled in [false, true] {
        let s = Setup::new();
        let log = Arc::new(Mutex::new(vec![]));
        let release = pi_ai_core::CancellationToken::new();
        let mut binding = tool("a", log.clone(), release);
        Arc::get_mut(&mut binding).unwrap().descriptor.replay = Some(ToolReplay::Never);
        *s.faux.responses.lock().unwrap() = vec![
            tool_response(&s),
            assistant(&s.faux.model, pi_ai_core::StopReason::Stop),
        ]
        .into();
        let f = RuntimeFactory::new(
            s.time.clone(),
            s.pump.clone(),
            s.ids.clone(),
            Arc::new(ContextValue(37)),
            Arc::new(Summary),
        );
        let mut options = s.options(DriveMode::Automatic);
        options.tools = vec![binding.clone()];
        let c = s.pump.drive(f.create(options)).unwrap();
        s.pump
            .drive(
                c.harness
                    .set_active_tools(vec![pi_ai_core::ToolName::new("a").unwrap()]),
            )
            .unwrap();
        let mut prompt = Box::pin(c.harness.prompt(PromptInput::Text {
            text: "interrupted".into(),
            images: vec![],
        }));
        assert!(poll(prompt.as_mut()).is_pending());
        for _ in 0..100 {
            s.pump.tick();
        }
        assert_eq!(
            log.lock()
                .unwrap()
                .iter()
                .filter(|s| s.as_str() == "start:a")
                .count(),
            1
        );
        if cancelled {
            let register = s
                .pump
                .drive(
                    s.session
                        .list_registers(RegisterNamespace::OperationState, None),
                )
                .unwrap()
                .remove(0);
            let Register::OperationState { key, mut value, .. } = register else {
                panic!("state")
            };
            let OperationState::Run(run) = value.as_mut() else {
                panic!("run")
            };
            run.control = Control::CancelRequested(CancelRequestedControl {
                status: Default::default(),
                requested_at: n(7),
                drained_steer: vec![],
                drained_follow_up: vec![],
            });
            s.pump
                .drive(s.session.commit(Transaction {
                    writes: vec![Write::SetRegister(Box::new(RegisterSet::OperationState {
                        key,
                        value,
                    }))],
                }))
                .unwrap();
        }
        s.pump.drive(c.harness.close()).unwrap();
        drop(prompt);
        let reopened = reopen(&s);
        let mut options = s.options(DriveMode::Automatic);
        options.session = reopened.clone();
        options.tools = vec![binding];
        let restored = s.pump.drive(f.create(options)).unwrap();
        let outcome = s.pump.drive(restored.harness.resume()).unwrap().unwrap();
        assert_eq!(
            log.lock()
                .unwrap()
                .iter()
                .filter(|s| s.as_str() == "start:a")
                .count(),
            1
        );
        assert_eq!(
            log.lock()
                .unwrap()
                .iter()
                .filter(|s| s.as_str() == "prepare:a")
                .count(),
            1
        );
        assert!(
            s.pump
                .drive(reopened.list_registers(RegisterNamespace::OperationToolArguments, None))
                .unwrap()
                .is_empty()
        );
        if cancelled {
            assert!(matches!(outcome, ResumeOutcome::RunAbortedWithAssistant(_)));
            assert_eq!(s.faux.requests.lock().unwrap().len(), 1);
        } else {
            assert!(matches!(
                outcome,
                ResumeOutcome::RunCompletedWithAssistant(_)
            ));
            assert_eq!(s.faux.requests.lock().unwrap().len(), 2);
        }
    }
}
struct SaveResume;
impl HookHandler<BeforeRun> for SaveResume {
    fn call(
        &self,
        _: BeforeRunEvent,
    ) -> BoxFuture<'_, Result<Option<BeforeRunResult>, HandlerError>> {
        Box::pin(async {
            Ok(Some(BeforeRunResult {
                resume_data: pi_ai_core::Presence::Value(pi_ai_core::JsonValue::String(
                    "saved payload".into(),
                )),
                ..Default::default()
            }))
        })
    }
}
struct ReadResume(Arc<std::sync::atomic::AtomicUsize>);
impl HookHandler<BeforeResume> for ReadResume {
    fn call(&self, event: BeforeResumeEvent) -> BoxFuture<'_, Result<(), HandlerError>> {
        Box::pin(async move {
            let BeforeResumeEvent::Run(event) = event else {
                panic!("run resume")
            };
            assert_eq!(
                event.resume_data,
                pi_ai_core::Presence::Value(pi_ai_core::JsonValue::String("saved payload".into()))
            );
            assert_eq!(event.prompt.len(), 1);
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        })
    }
}
#[test]
fn ready_reopen_hydrates_actual_before_resume_prompt_and_per_handler_data() {
    let s = Setup::new();
    let f = factory(&s);
    let c = s
        .pump
        .drive(f.create(s.options(DriveMode::Manual)))
        .unwrap();
    let _save = c
        .harness
        .hooks()
        .before_run(HandlerId::new("memory").unwrap(), Arc::new(SaveResume))
        .unwrap();
    let mut prompt = Box::pin(c.harness.prompt(PromptInput::Text {
        text: "restore ready".into(),
        images: vec![],
    }));
    assert!(poll(prompt.as_mut()).is_pending());
    let mut ready = false;
    for _ in 0..500 {
        s.pump.tick();
        ready=s.pump.drive(s.session.list_registers(RegisterNamespace::OperationState,None)).unwrap().iter().any(|register|matches!(register,Register::OperationState {value,..} if matches!(value.as_ref(),OperationState::Run(run) if matches!(&run.phase,RunPhase::Assistant(AssistantPhase {generation:Generation::Ready{..},..})))));
        if ready {
            break;
        }
        let _ = s.pump.drive(c.harness.execute_action()).unwrap();
    }
    assert!(ready);
    s.pump.drive(c.harness.close()).unwrap();
    drop(prompt);
    let mut options = s.options(DriveMode::Automatic);
    options.session = reopen(&s);
    let restored = s.pump.drive(f.create(options)).unwrap();
    let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let _read = restored
        .harness
        .hooks()
        .before_resume(
            HandlerId::new("memory").unwrap(),
            Arc::new(ReadResume(count.clone())),
        )
        .unwrap();
    s.pump.drive(restored.harness.resume()).unwrap().unwrap();
    assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(s.faux.requests.lock().unwrap().len(), 1);
}
struct Project(Arc<Mutex<Vec<String>>>);
impl EntryProjector for Project {
    fn project(
        &self,
        _: CustomEntry,
    ) -> BoxFuture<'_, Result<Option<Vec<AgentMessage>>, HarnessRuntimeError>> {
        Box::pin(async move {
            self.0.lock().unwrap().push("project".into());
            Ok(Some(vec![user_message("projected")]))
        })
    }
}
fn user_message(text: &str) -> AgentMessage {
    AgentMessage::BuiltIn(pi_ai_core::Message::User(pi_ai_core::UserMessage {
        role: Default::default(),
        content: pi_ai_core::UserContent::Text(text.into()),
        timestamp: n(7),
    }))
}
struct Transform(Arc<Mutex<Vec<String>>>);
impl HookHandler<TransformContext> for Transform {
    fn call(
        &self,
        event: TransformContextEvent,
    ) -> BoxFuture<'_, Result<Option<TransformContextResult>, HandlerError>> {
        Box::pin(async move {
            assert_eq!(
                event.messages,
                vec![user_message("projected"), user_message("caller")]
            );
            self.0.lock().unwrap().push("transform".into());
            Ok(Some(TransformContextResult {
                messages: vec![user_message("transformed")],
            }))
        })
    }
}
struct Convert(Arc<Mutex<Vec<String>>>);
impl ProviderMessageConverter for Convert {
    fn convert(
        &self,
        messages: Vec<AgentMessage>,
    ) -> BoxFuture<'_, Result<Vec<pi_ai_core::Message>, HarnessRuntimeError>> {
        Box::pin(async move {
            assert_eq!(messages, vec![user_message("transformed")]);
            self.0.lock().unwrap().push("convert".into());
            let AgentMessage::BuiltIn(message) = user_message("converted") else {
                unreachable!()
            };
            Ok(vec![message])
        })
    }
}
#[test]
fn real_context_projection_transform_and_conversion_reach_provider_in_order() {
    let s = Setup::new();
    let log = Arc::new(Mutex::new(vec![]));
    let f = factory(&s);
    let mut options = s.options(DriveMode::Automatic);
    options.entry_projectors.insert(
        CustomType::new("note").unwrap(),
        Arc::new(Project(log.clone())),
    );
    options.to_provider_messages = Some(Arc::new(Convert(log.clone())));
    let c = s.pump.drive(f.create(options)).unwrap();
    s.pump
        .drive(c.harness.session().append_custom_entry(
            CustomType::new("note").unwrap(),
            pi_ai_core::Presence::Missing,
        ))
        .unwrap();
    let _hook = c
        .harness
        .hooks()
        .transform_context(None, Arc::new(Transform(log.clone())))
        .unwrap();
    s.pump
        .drive(c.harness.prompt(PromptInput::Text {
            text: "caller".into(),
            images: vec![],
        }))
        .unwrap()
        .unwrap();
    assert_eq!(
        *log.lock().unwrap(),
        vec!["project", "transform", "convert"]
    );
    let AgentMessage::BuiltIn(message) = user_message("converted") else {
        unreachable!()
    };
    assert_eq!(s.faux.requests.lock().unwrap()[0].0.messages, vec![message]);
}
#[test]
fn codec_rejects_pending_or_error_without_message_as_expected_prompt_errors() {
    let s = Setup::new();
    let f = factory(&s);
    let c = s
        .pump
        .drive(f.create(s.options(DriveMode::Automatic)))
        .unwrap();
    let before = s.pump.drive(s.store.snapshot()).unwrap();
    for reason in [
        pi_ai_core::StopReason::Pending,
        pi_ai_core::StopReason::Error,
    ] {
        let mut message = assistant(&s.faux.model, reason);
        message.error_message = None;
        let result = s
            .pump
            .drive(
                c.harness
                    .prompt(PromptInput::Messages(vec![AgentMessage::BuiltIn(
                        pi_ai_core::Message::Assistant(Box::new(message)),
                    )])),
            )
            .unwrap();
        assert!(matches!(result, Err(RunExpectedError::InvalidMessage(_))));
    }
    assert_eq!(before, s.pump.drive(s.store.snapshot()).unwrap());
    assert!(s.pump.drive(c.harness.get_stream_options()).is_ok());
    assert!(s.faux.requests.lock().unwrap().is_empty());
}
#[test]
fn tool_registry_replacement_after_clearance_cannot_execute_either_binding() {
    let s = Setup::new();
    let old_log = Arc::new(Mutex::new(vec![]));
    let new_log = Arc::new(Mutex::new(vec![]));
    let release = pi_ai_core::CancellationToken::new();
    release.cancel();
    *s.faux.responses.lock().unwrap() = vec![
        tool_response(&s),
        assistant(&s.faux.model, pi_ai_core::StopReason::Stop),
    ]
    .into();
    let f = RuntimeFactory::new(
        s.time.clone(),
        s.pump.clone(),
        s.ids.clone(),
        Arc::new(ContextValue(37)),
        Arc::new(Summary),
    );
    let mut options = s.options(DriveMode::Manual);
    options.tools = vec![tool("a", old_log.clone(), release.clone())];
    let c = s.pump.drive(f.create(options)).unwrap();
    s.pump
        .drive(
            c.harness
                .set_active_tools(vec![pi_ai_core::ToolName::new("a").unwrap()]),
        )
        .unwrap();
    let mut prompt = Box::pin(c.harness.prompt(PromptInput::Text {
        text: "replace tool".into(),
        images: vec![],
    }));
    assert!(poll(prompt.as_mut()).is_pending());
    let mut ready = false;
    for _ in 0..1000 {
        s.pump.tick();
        ready = !s
            .pump
            .drive(
                s.session
                    .list_registers(RegisterNamespace::OperationToolArguments, None),
            )
            .unwrap()
            .is_empty();
        if ready {
            break;
        }
        let _ = s.pump.drive(c.harness.execute_action()).unwrap();
    }
    assert!(ready);
    s.pump
        .drive(
            c.harness
                .set_tools(vec![tool("a", new_log.clone(), release)]),
        )
        .unwrap();
    for _ in 0..1000 {
        s.pump.tick();
        if let Poll::Ready(result) = poll(prompt.as_mut()) {
            result.unwrap().unwrap();
            assert_eq!(*old_log.lock().unwrap(), vec!["prepare:a"]);
            assert!(new_log.lock().unwrap().is_empty());
            return;
        }
        let _ = s.pump.drive(c.harness.execute_action()).unwrap();
    }
    panic!("tool replacement did not settle");
}
struct BlockTerminate;
impl HookHandler<BeforeTool> for BlockTerminate {
    fn call(
        &self,
        _: BeforeToolEvent,
    ) -> BoxFuture<'_, Result<Option<BeforeToolResult>, HandlerError>> {
        Box::pin(async {
            Ok(Some(BeforeToolResult {
                args: None,
                block: Some(BeforeToolBlock {
                    reason: "terminate fixture".into(),
                    terminate: Some(true),
                }),
            }))
        })
    }
}
#[test]
fn immediate_terminating_tool_batch_finishes_without_another_assistant_or_execution() {
    let s = Setup::new();
    let log = Arc::new(Mutex::new(vec![]));
    *s.faux.responses.lock().unwrap() = vec![tool_response(&s)].into();
    let f = RuntimeFactory::new(
        s.time.clone(),
        s.pump.clone(),
        s.ids.clone(),
        Arc::new(ContextValue(37)),
        Arc::new(Summary),
    );
    let mut options = s.options(DriveMode::Automatic);
    options.tools = vec![tool("a", log.clone(), pi_ai_core::CancellationToken::new())];
    let c = s.pump.drive(f.create(options)).unwrap();
    s.pump
        .drive(
            c.harness
                .set_active_tools(vec![pi_ai_core::ToolName::new("a").unwrap()]),
        )
        .unwrap();
    let _block = c
        .harness
        .hooks()
        .before_tool(None, Arc::new(BlockTerminate))
        .unwrap();
    let outcome = s
        .pump
        .drive(c.harness.prompt(PromptInput::Text {
            text: "terminate".into(),
            images: vec![],
        }))
        .unwrap()
        .unwrap();
    assert!(matches!(
        outcome.outcome,
        RunOutcome::CompletedWithoutAssistant(_)
    ));
    assert_eq!(*log.lock().unwrap(), vec!["prepare:a"]);
    assert_eq!(s.faux.requests.lock().unwrap().len(), 1);
    assert!(matches!(
        s.pump.drive(c.harness.get_last_result()).unwrap(),
        Some(LaneLastResult::RunCompletedTerminatedTools(_))
    ));
    assert!(
        s.pump
            .drive(
                s.session
                    .list_registers(RegisterNamespace::OperationToolArguments, None)
            )
            .unwrap()
            .is_empty()
    );
}
#[test]
fn terminal_cleanup_deletes_only_the_owned_operation_prefix() {
    let s = Setup::new();
    let f = factory(&s);
    let c = s
        .pump
        .drive(f.create(s.options(DriveMode::Manual)))
        .unwrap();
    let end = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let _hook = c
        .harness
        .hooks()
        .before_run_end(None, Arc::new(EndMark(end.clone())))
        .unwrap();
    let mut prompt = Box::pin(c.harness.prompt(PromptInput::Text {
        text: "prefix".into(),
        images: vec![],
    }));
    assert!(poll(prompt.as_mut()).is_pending());
    for _ in 0..1000 {
        s.pump.tick();
        if end.load(std::sync::atomic::Ordering::SeqCst) > 0 {
            break;
        }
        let _ = s.pump.drive(c.harness.execute_action()).unwrap();
    }
    let Register::OperationMeta { key, .. } = s
        .pump
        .drive(
            s.session
                .list_registers(RegisterNamespace::OperationMeta, None),
        )
        .unwrap()
        .remove(0)
    else {
        panic!("meta")
    };
    let own = format!("{key}:orphan:0");
    let other = "another-operation:orphan:0";
    s.pump
        .drive(
            s.session.commit(Transaction {
                writes: [own.clone(), other.into()]
                    .into_iter()
                    .map(|key| {
                        Write::SetRegister(Box::new(RegisterSet::OperationToolArguments {
                            key,
                            value: Default::default(),
                        }))
                    })
                    .collect(),
            }),
        )
        .unwrap();
    for _ in 0..1000 {
        s.pump.tick();
        if let Poll::Ready(result) = poll(prompt.as_mut()) {
            result.unwrap().unwrap();
            assert!(
                s.pump
                    .drive(
                        s.session
                            .get_register(RegisterNamespace::OperationToolArguments, &own)
                    )
                    .unwrap()
                    .is_none()
            );
            assert!(
                s.pump
                    .drive(
                        s.session
                            .get_register(RegisterNamespace::OperationToolArguments, other)
                    )
                    .unwrap()
                    .is_some()
            );
            return;
        }
        let _ = s.pump.drive(c.harness.execute_action()).unwrap();
    }
    panic!("prefix cleanup did not finish");
}
#[test]
fn ordinary_provider_failure_drains_empty_inbox_but_running_aborted_response_faults() {
    for aborted in [false, true] {
        let s = Setup::new();
        *s.faux.responses.lock().unwrap() = vec![assistant(
            &s.faux.model,
            if aborted {
                pi_ai_core::StopReason::Aborted
            } else {
                pi_ai_core::StopReason::Error
            },
        )]
        .into();
        let f = factory(&s);
        let c = s
            .pump
            .drive(f.create(s.options(DriveMode::Automatic)))
            .unwrap();
        let result = s.pump.drive(c.harness.prompt(PromptInput::Text {
            text: "failure".into(),
            images: vec![],
        }));
        if aborted {
            assert!(matches!(result, Err(HarnessRuntimeError::Fault(_))));
            assert!(s.pump.drive(s.store.snapshot()).unwrap().usage.is_empty());
        } else {
            assert!(matches!(
                result.unwrap().unwrap().outcome,
                RunOutcome::FailedWithAssistant(_)
            ));
            assert_eq!(s.pump.drive(s.store.snapshot()).unwrap().usage.len(), 1);
            assert!(
                s.pump
                    .drive(
                        s.session
                            .list_registers(RegisterNamespace::OperationState, None)
                    )
                    .unwrap()
                    .is_empty()
            );
        }
    }
}
#[test]
fn new_lane_uses_the_immutable_seed_and_tree_writes_route_through_owner() {
    let s = Setup::new();
    let f = factory(&s);
    let c = s
        .pump
        .drive(f.create(s.options(DriveMode::Automatic)))
        .unwrap();
    let watch = s.pump.drive(c.harness.watch_session()).unwrap();
    let listener = Arc::new(Listener(Mutex::new(vec![])));
    watch.start(listener.clone());
    s.pump
        .drive(c.harness.set_thinking_level(AgentThinkingLevel::High))
        .unwrap();
    let lane = s
        .pump
        .drive(c.harness.create_lane(LaneId::new("named").unwrap(), None))
        .unwrap()
        .unwrap();
    assert_eq!(
        s.pump.drive(lane.get_thinking_level()).unwrap(),
        AgentThinkingLevel::Off
    );
    s.pump
        .drive(lane.session().set_name(Some("owned name".into())))
        .unwrap();
    assert_eq!(
        s.pump
            .drive(c.harness.session().get_name())
            .unwrap()
            .as_deref(),
        Some("owned name")
    );
    let entry = s
        .pump
        .drive(lane.session().append_message(user_message("named entry")))
        .unwrap();
    assert_eq!(s.pump.drive(lane.get_leaf_id()).unwrap(), Some(entry));
    assert!(s.pump.drive(c.harness.get_leaf_id()).unwrap().is_none());
    assert_eq!(
        listener
            .0
            .lock()
            .unwrap()
            .iter()
            .filter(|e| matches!(e, HarnessEvent::LaneCreated(_)))
            .count(),
        1
    );
    assert!(
        s.pump
            .drive(c.harness.create_lane(LaneId::new("named").unwrap(), None))
            .unwrap()
            .is_err()
    );
}
#[test]
fn tool_context_panic_is_owned_by_retained_effect_after_public_waiter_drop() {
    let s = Setup::new();
    *s.faux.responses.lock().unwrap() = vec![tool_response(&s)].into();
    let f = RuntimeFactory::new(
        s.time.clone(),
        s.pump.clone(),
        s.ids.clone(),
        Arc::new(ContextValue(37)),
        Arc::new(Summary),
    );
    let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let log = Arc::new(Mutex::new(vec![]));
    let mut options = s.options(DriveMode::Automatic);
    options.tool_context = Some(AgentHarnessToolContextSource::Provider(Arc::new(
        PanickingContext(false, count.clone()),
    )));
    options.tools = vec![tool("a", log.clone(), pi_ai_core::CancellationToken::new())];
    let c = s.pump.drive(f.create(options)).unwrap();
    s.pump
        .drive(
            c.harness
                .set_active_tools(vec![pi_ai_core::ToolName::new("a").unwrap()]),
        )
        .unwrap();
    let mut prompt = Box::pin(c.harness.prompt(PromptInput::Text {
        text: "effect panic".into(),
        images: vec![],
    }));
    assert!(poll(prompt.as_mut()).is_pending());
    drop(prompt);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        for _ in 0..100 {
            s.pump.tick();
        }
    }));
    assert!(result.is_ok());
    assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert!(matches!(
        s.pump.drive(c.harness.get_stream_options()),
        Err(HarnessRuntimeError::Fault(_))
    ));
    assert_eq!(s.faux.requests.lock().unwrap().len(), 1);
    assert!(log.lock().unwrap().is_empty());
}
fn write_label(write: &Write) -> String {
    match write {
        Write::Entry(NewEntry::Message(entry)) => format!(
            "entry:{}",
            match &entry.message {
                AgentMessage::BuiltIn(pi_ai_core::Message::User(_)) => "user",
                AgentMessage::BuiltIn(pi_ai_core::Message::Assistant(_)) => "assistant",
                AgentMessage::BuiltIn(pi_ai_core::Message::ToolResult(_)) => "tool",
                _ => "custom",
            }
        ),
        Write::Usage(_) => "usage".into(),
        Write::SetRegister(register) => match register.as_ref() {
            RegisterSet::LaneLeaf { .. } => "set:lane.leaf".into(),
            RegisterSet::LaneState { .. } => "set:lane.state".into(),
            RegisterSet::LaneLastResult { .. } => "set:lane.lastResult".into(),
            RegisterSet::OperationMeta { .. } => "set:op.meta".into(),
            RegisterSet::OperationState { value, .. } => match value.as_ref() {
                OperationState::Run(run) => match &run.phase {
                    RunPhase::Checkpoint(checkpoint) => format!(
                        "state:checkpoint:{}",
                        match checkpoint.continuation {
                            Continuation::NeedAssistant { .. } => "need_assistant",
                            Continuation::MayFinish { .. } => "may_finish",
                        }
                    ),
                    RunPhase::Assistant(phase) => format!(
                        "state:assistant:{}",
                        match phase.generation {
                            Generation::Ready { .. } => "ready",
                            Generation::EffectPending { .. } => "effect_pending",
                            Generation::RetryWait { .. } => "retry_wait",
                        }
                    ),
                    _ => "other-state".into(),
                },
                _ => "other-state".into(),
            },
            _ => "other-register".into(),
        },
        Write::DeleteRegister(register) => format!(
            "delete:{}",
            match register.namespace {
                RegisterNamespace::OperationMeta => "op.meta",
                RegisterNamespace::OperationState => "op.state",
                _ => "other",
            }
        ),
        _ => "other-write".into(),
    }
}
#[test]
fn both_public_drive_modes_match_the_pinned_exact_transaction_trace_and_ids() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../conformance/harness/r1-r4/runtime/public-traces.json"
    ))
    .unwrap();
    for mode in [DriveMode::Automatic, DriveMode::Manual] {
        let s = Setup::new();
        let tracked = Arc::new(tracking::TrackedStorage::new(s.store.clone()));
        let session = tracked_session(&s, tracked.clone());
        let f = RuntimeFactory::new(
            s.time.clone(),
            s.pump.clone(),
            s.ids.clone(),
            Arc::new(()),
            Arc::new(Summary),
        );
        let c = s
            .pump
            .drive(f.create(s.options_for::<(), _>(session, mode)))
            .unwrap();
        let mut prompt = Box::pin(c.harness.prompt(PromptInput::Text {
            text: "trace".into(),
            images: vec![],
        }));
        let mut finished = false;
        for _ in 0..1000 {
            if let Poll::Ready(result) = poll(prompt.as_mut()) {
                let result = result.unwrap().unwrap();
                assert_eq!(
                    result.run_id.as_str(),
                    fixture["operation_id"].as_str().unwrap()
                );
                finished = true;
                break;
            }
            s.pump.tick();
            if mode == DriveMode::Manual {
                let _ = s.pump.drive(c.harness.execute_action()).unwrap();
            }
        }
        assert!(finished);
        let trace = tracked
            .transactions
            .lock()
            .unwrap()
            .iter()
            .map(|tx| tx.writes.iter().map(write_label).collect::<Vec<_>>())
            .collect::<Vec<_>>();
        assert_eq!(
            serde_json::to_value(trace).unwrap(),
            fixture["configured_no_tool_transactions"]
        );
        let snapshot = s.pump.drive(s.store.snapshot()).unwrap();
        let ids = snapshot
            .entries
            .iter()
            .map(|entry| {
                pi_agent_session::backend_support::entry_structure(entry)
                    .id
                    .as_str()
                    .to_owned()
            })
            .collect::<Vec<_>>();
        assert_eq!(serde_json::to_value(ids).unwrap(), fixture["entry_ids"]);
        assert_eq!(
            serde_json::to_value(
                snapshot
                    .usage
                    .iter()
                    .map(|row| row.id.as_str())
                    .collect::<Vec<_>>()
            )
            .unwrap(),
            fixture["usage_ids"]
        );
    }
}
#[test]
fn initial_options_are_rechecked_against_the_actual_session_header_policy() {
    let s = Setup::new();
    let f = factory(&s);
    let before = s.pump.drive(s.store.snapshot()).unwrap();
    let mut options = s.options::<()>(DriveMode::Automatic);
    options.stream_options.headers = Some(
        RawProviderHeaders::new([("x-another-policy".into(), "public".into())].into())
            .unwrap()
            .classify(&ProviderHeaderPolicy::from_allowlist([
                "x-another-policy".into()
            ]))
            .unwrap(),
    );
    assert!(s.pump.drive(f.create(options)).is_err());
    assert_eq!(before, s.pump.drive(s.store.snapshot()).unwrap());
    assert!(s.faux.requests.lock().unwrap().is_empty());
    assert!(
        s.pump
            .drive(f.create(s.options(DriveMode::Automatic)))
            .is_ok()
    );
}
#[test]
fn one_sequential_tool_declaration_serializes_the_actual_batch() {
    let s = Setup::new();
    let log = Arc::new(Mutex::new(vec![]));
    let a = pi_ai_core::CancellationToken::new();
    let b = pi_ai_core::CancellationToken::new();
    b.cancel();
    let mut response = tool_response(&s);
    let pi_ai_core::AssistantContent::ToolCall(mut second) = response.content[0].clone() else {
        unreachable!()
    };
    second.name = pi_ai_core::ToolName::new("b").unwrap();
    second.id = pi_ai_core::ToolCallId::new("call-b").unwrap();
    response
        .content
        .push(pi_ai_core::AssistantContent::ToolCall(second));
    *s.faux.responses.lock().unwrap() = vec![
        response,
        assistant(&s.faux.model, pi_ai_core::StopReason::Stop),
    ]
    .into();
    let mut second = tool("b", log.clone(), b);
    Arc::get_mut(&mut second).unwrap().descriptor.execution_mode =
        Some(ToolExecutionMode::Sequential);
    let f = RuntimeFactory::new(
        s.time.clone(),
        s.pump.clone(),
        s.ids.clone(),
        Arc::new(ContextValue(37)),
        Arc::new(Summary),
    );
    let mut options = s.options(DriveMode::Automatic);
    options.tools = vec![tool("a", log.clone(), a.clone()), second];
    let c = s.pump.drive(f.create(options)).unwrap();
    s.pump
        .drive(c.harness.set_active_tools(vec![
            pi_ai_core::ToolName::new("a").unwrap(),
            pi_ai_core::ToolName::new("b").unwrap(),
        ]))
        .unwrap();
    let mut prompt = Box::pin(c.harness.prompt(PromptInput::Text {
        text: "sequential".into(),
        images: vec![],
    }));
    assert!(poll(prompt.as_mut()).is_pending());
    for _ in 0..100 {
        s.pump.tick();
    }
    assert!(log.lock().unwrap().contains(&"start:a".into()));
    assert!(!log.lock().unwrap().contains(&"prepare:b".into()));
    a.cancel();
    s.pump.drive(prompt).unwrap().unwrap();
    let log = log.lock().unwrap();
    assert!(
        log.iter().position(|s| s == "raw:a").unwrap()
            < log.iter().position(|s| s == "start:b").unwrap()
    );
}
struct FollowOnce(std::sync::atomic::AtomicUsize);
impl HookHandler<BeforeRunEnd> for FollowOnce {
    fn call(
        &self,
        _: BeforeRunEndEvent,
    ) -> BoxFuture<'_, Result<Option<BeforeRunEndResult>, HandlerError>> {
        Box::pin(async move {
            Ok(
                (self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0).then(|| {
                    BeforeRunEndResult {
                        follow_up: Some("one inline follow-up".into()),
                    }
                }),
            )
        })
    }
}
#[test]
fn before_run_end_follow_up_is_committed_and_driven_in_the_same_operation() {
    let s = Setup::new();
    let f = factory(&s);
    let c = s
        .pump
        .drive(f.create(s.options(DriveMode::Automatic)))
        .unwrap();
    let hook = Arc::new(FollowOnce(std::sync::atomic::AtomicUsize::new(0)));
    let _subscription = c
        .harness
        .hooks()
        .before_run_end(None, hook.clone())
        .unwrap();
    let result = s
        .pump
        .drive(c.harness.prompt(PromptInput::Text {
            text: "first".into(),
            images: vec![],
        }))
        .unwrap()
        .unwrap();
    assert!(matches!(
        result.outcome,
        RunOutcome::CompletedWithAssistant(_)
    ));
    assert_eq!(hook.0.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert_eq!(s.faux.requests.lock().unwrap().len(), 2);
    let snapshot = s.pump.drive(s.store.snapshot()).unwrap();
    assert_eq!(snapshot.entries.len(), 4);
    assert_eq!(snapshot.usage.len(), 2);
    assert!(snapshot.entries.iter().any(|e|matches!(e,Entry::Message(MessageEntry {message:AgentMessage::BuiltIn(pi_ai_core::Message::User(pi_ai_core::UserMessage {content:pi_ai_core::UserContent::Text(text),..})),..}) if text=="one inline follow-up")));
}
#[test]
fn new_calls_after_close_use_the_existing_preacceptance_closed_channels() {
    let s = Setup::new();
    let f = factory(&s);
    let c = s
        .pump
        .drive(f.create(s.options(DriveMode::Automatic)))
        .unwrap();
    s.pump.drive(c.harness.close()).unwrap();
    assert!(matches!(
        s.pump.drive(c.harness.prompt(PromptInput::Text {
            text: "closed".into(),
            images: vec![]
        })),
        Ok(Err(RunExpectedError::Closed(_)))
    ));
    assert!(matches!(
        s.pump.drive(c.harness.skill("unknown".into(), None)),
        Ok(Err(RunExpectedError::Closed(_)))
    ));
    assert!(matches!(
        s.pump
            .drive(c.harness.prompt_from_template("unknown".into(), vec![])),
        Ok(Err(RunExpectedError::Closed(_)))
    ));
    assert!(matches!(
        s.pump.drive(c.harness.resume()),
        Ok(Err(ResumeExpectedError::Closed(_)))
    ));
    assert!(matches!(
        s.pump
            .drive(c.harness.create_lane(LaneId::new("closed").unwrap(), None)),
        Ok(Err(CreateLaneExpectedError::Closed(_)))
    ));
    assert!(s.faux.requests.lock().unwrap().is_empty());
}

struct PendingPrompt(Arc<std::sync::atomic::AtomicBool>);
impl SystemPromptProvider<()> for PendingPrompt {
    fn resolve<'a>(&'a self, _: &'a ()) -> BoxFuture<'a, Result<String, HarnessRuntimeError>> {
        Box::pin(async move {
            self.0.store(true, std::sync::atomic::Ordering::SeqCst);
            std::future::pending().await
        })
    }
}
#[test]
fn close_rejects_waiter_suspended_in_trusted_prompt_preview() {
    let s = Setup::new();
    let f = RuntimeFactory::new(
        s.time.clone(),
        s.pump.clone(),
        s.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    let entered = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut options = s.options(DriveMode::Automatic);
    options.system_prompt = Some(SystemPromptSource::Provider(Arc::new(PendingPrompt(
        entered.clone(),
    ))));
    let c = s.pump.drive(f.create(options)).unwrap();
    let mut prompt = Box::pin(c.harness.prompt(PromptInput::Text {
        text: "pending preview".into(),
        images: vec![],
    }));
    assert!(poll(prompt.as_mut()).is_pending());
    for _ in 0..20 {
        s.pump.tick();
        if entered.load(std::sync::atomic::Ordering::SeqCst) {
            break;
        }
    }
    assert!(entered.load(std::sync::atomic::Ordering::SeqCst));
    s.pump.drive(c.harness.close()).unwrap();
    for _ in 0..20 {
        s.pump.tick();
    }
    let result = poll(prompt.as_mut());
    println!("prompt after close: {result:?}");
    assert!(
        matches!(
            result,
            std::task::Poll::Ready(Err(HarnessRuntimeError::Closed(_)))
        ),
        "close must reject unresolved local prompt waiter without cooperation from trusted preview"
    );
}

struct PendingPreviewContext(Arc<std::sync::atomic::AtomicBool>);
impl ToolContextProvider<()> for PendingPreviewContext {
    fn resolve(&self) -> BoxFuture<'_, Result<(), HarnessRuntimeError>> {
        Box::pin(async move {
            self.0.store(true, std::sync::atomic::Ordering::SeqCst);
            std::future::pending().await
        })
    }
}
#[test]
fn owner_seal_releases_uncommitted_prompt_and_context_previews_without_publication() {
    for context in [false, true] {
        for fault in [false, true] {
            let s = Setup::new();
            let tracked = Arc::new(tracking::TrackedStorage::new(s.store.clone()));
            let f = RuntimeFactory::new(
                s.time.clone(),
                s.pump.clone(),
                s.ids.clone(),
                Arc::new(()),
                Arc::new(Summary),
            );
            let entered = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let mut options =
                s.options_for::<(), _>(tracked_session(&s, tracked.clone()), DriveMode::Automatic);
            options.system_prompt = Some(SystemPromptSource::Provider(Arc::new(PendingPrompt(
                entered.clone(),
            ))));
            if context {
                options.tool_context = Some(AgentHarnessToolContextSource::Provider(Arc::new(
                    PendingPreviewContext(entered.clone()),
                )));
            }
            let c = s.pump.drive(f.create(options)).unwrap();
            let mut prompt = Box::pin(c.harness.prompt(PromptInput::Text {
                text: "pending trusted preview".into(),
                images: vec![],
            }));
            assert!(poll(prompt.as_mut()).is_pending());
            for _ in 0..20 {
                s.pump.tick();
                if entered.load(std::sync::atomic::Ordering::SeqCst) {
                    break;
                }
            }
            assert!(entered.load(std::sync::atomic::Ordering::SeqCst));
            if fault {
                tracked
                    .fail_reads
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                assert!(matches!(
                    s.pump.drive(c.harness.get_leaf_id()),
                    Err(HarnessRuntimeError::Fault(_))
                ));
            } else {
                s.pump.drive(c.harness.close()).unwrap();
            }
            for _ in 0..20 {
                s.pump.tick();
            }
            match poll(prompt.as_mut()) {
                Poll::Ready(Err(HarnessRuntimeError::Fault(_))) if fault => {}
                Poll::Ready(Err(HarnessRuntimeError::Closed(_))) if !fault => {}
                other => panic!(
                    "owner seal must release preview: context={context}, fault={fault}, result={other:?}"
                ),
            }
            assert!(
                tracked.transactions.lock().unwrap().is_empty(),
                "preview must not publish acceptance or terminal state"
            );
            assert!(s.faux.requests.lock().unwrap().is_empty());
        }
    }
}
