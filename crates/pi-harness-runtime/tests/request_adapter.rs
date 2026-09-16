mod request_adapter_support;

use pi_agent_schema::{
    AfterResponseEvent, AgentHarnessStreamOptions, AgentHarnessStreamOptionsPatch,
    AgentThinkingLevel, BeforePayloadEvent, ErrorContext, HarnessClosed, HarnessFault,
    HarnessRuntimeError, LaneId, OperationId, ProviderHeaderPolicy, RawProviderHeaderPatch,
    RawProviderHeaders, SessionId, SettledAssistantMessage,
};
use pi_ai_core::*;
use pi_harness_runtime::hooks::apply_stream_options_patch;
use pi_harness_runtime::request::*;
use request_adapter_support::*;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

fn model() -> Model {
    let v: serde_json::Value = serde_json::from_str(include_str!(
        "../../../conformance/schema/ai/models-options.golden.json"
    ))
    .unwrap();
    serde_json::from_value(v["model"].clone()).unwrap()
}
fn assistant(reason: StopReason) -> AssistantMessage {
    let m = model();
    AssistantMessage {
        role: Default::default(),
        content: vec![],
        api: m.api,
        provider: m.provider,
        model: m.id,
        response_model: None,
        response_id: None,
        diagnostics: None,
        usage: Usage::zero(),
        stop_reason: reason,
        deferred: None,
        error_message: matches!(reason, StopReason::Error | StopReason::Aborted)
            .then(|| "provider fixture error".into()),
        raw_stop_reason: None,
        end_turn: None,
        timestamp: SafeUnsigned::new(7).unwrap(),
    }
}
#[derive(Clone)]
struct Capture {
    wire: SimpleStreamOptionsWire,
    headers: ProviderHeaders,
    context: Context,
    signal: CancellationToken,
    endpoint: String,
    has_auth: bool,
    has_transform: bool,
    invalid_terminal_rejected: bool,
    payload: serde_json::Value,
}
struct Faux {
    model: Model,
    capture: Arc<Mutex<Vec<Capture>>>,
    payload: serde_json::Value,
    head: Option<RequestResponseHead>,
    reason: StopReason,
    cancel_at_terminal: bool,
}
impl ApiAdapter for Faux {
    fn api_id(&self) -> &ApiId {
        &self.model.api
    }
    fn lifecycle_adapter(&self) -> Option<&dyn RequestLifecycleAdapter> {
        Some(self)
    }
    fn stream(&self, _: TextStreamRequest) -> AssistantStream {
        panic!("unexpected raw route")
    }
    fn stream_simple(&self, _: SimpleStreamRequest) -> AssistantStream {
        panic!("unexpected ordinary route")
    }
    fn fetch_deferred(&self, _: DeferredFetchRequest) -> Result<AssistantStream, CapabilityError> {
        Err(CapabilityError::Unsupported)
    }
    fn cancel_deferred<'a>(
        &'a self,
        _: DeferredCancelRequest,
    ) -> BoxFuture<'a, Result<(), ModelsError>> {
        Box::pin(async { Ok(()) })
    }
}
impl RequestLifecycleAdapter for Faux {
    fn stream_with_lifecycle(
        &self,
        _: TextStreamRequest,
        _: RequestLifecycleInvocation,
    ) -> AssistantStream {
        panic!("unexpected raw lifecycle")
    }
    fn stream_simple_with_lifecycle(
        &self,
        request: SimpleStreamRequest,
        lifecycle: RequestLifecycleInvocation,
    ) -> AssistantStream {
        let signal = request.options.runtime.cancellation.clone();
        let (stream, producer) = create_assistant_message_event_stream(AssistantStreamInit {
            api: self.model.api.clone(),
            provider: self.model.provider.clone(),
            model: self.model.id.clone(),
            clock: request.options.runtime.clock.clone(),
            cancellation: signal.clone(),
        });
        let spawner = request.options.runtime.spawner.clone();
        let capture = self.capture.clone();
        let head = self.head.clone();
        let reason = self.reason;
        let cancel = self.cancel_at_terminal;
        let mut payload = self.payload.clone();
        let task_producer = producer.clone();
        let task = Box::pin(async move {
            // Capture actual adapter inputs before the hook, independently of
            // provider outcome normalization and after_response replacement.
            capture.lock().unwrap().push(Capture {
                wire: request.options.wire.clone(),
                headers: request.auth.headers.clone(),
                context: request.context.clone(),
                signal: signal.clone(),
                endpoint: request.resolved.endpoint.expose_for_transport().to_owned(),
                has_auth: request.auth.api_key.is_some(),
                has_transform: lifecycle.payload.is_some(),
                invalid_terminal_rejected: false,
                payload: payload.clone(),
            });
            if let Some(transform) = lifecycle.payload {
                match transform
                    .transform(&request.resolved.model, &mut payload)
                    .await
                {
                    Ok(Some(value)) => payload = value,
                    Ok(None) => (),
                    Err(_) => {
                        task_producer
                            .fail_setup(StreamSetupFailure::Adapter)
                            .unwrap();
                        return;
                    }
                }
            }
            capture.lock().unwrap().last_mut().unwrap().payload = payload;
            task_producer
                .push(AssistantMessageEvent::Start {
                    partial: Box::new(assistant(StopReason::Pending)),
                })
                .unwrap();
            if let Some(head) = head {
                lifecycle.recorder.record(head);
            }
            if cancel {
                signal.cancel();
                return;
            }
            if reason == StopReason::Error {
                task_producer
                    .push(AssistantMessageEvent::Error {
                        reason: ErrorReason::Error,
                        error: Box::new(assistant(reason)),
                    })
                    .unwrap();
            } else if reason == StopReason::Pending {
                let rejected = task_producer
                    .push(AssistantMessageEvent::Done {
                        reason: DoneReason::Stop,
                        message: Box::new(assistant(reason)),
                    })
                    .is_err();
                capture
                    .lock()
                    .unwrap()
                    .last_mut()
                    .unwrap()
                    .invalid_terminal_rejected = rejected;
            } else {
                task_producer
                    .push(AssistantMessageEvent::Done {
                        reason: DoneReason::Stop,
                        message: Box::new(assistant(reason)),
                    })
                    .unwrap();
            }
        });
        if spawner.spawn(task).is_err() {
            producer.fail_setup(StreamSetupFailure::Spawn).unwrap();
        }
        stream
    }
}
struct Auth;
impl ApiKeyAuth for Auth {
    fn name(&self) -> &str {
        "fixture"
    }
    fn resolve<'a>(
        &'a self,
        _: ApiKeyAuthInput<'a>,
    ) -> BoxFuture<'a, Result<Option<AuthResult>, AuthProviderError>> {
        Box::pin(async {
            Ok(Some(AuthResult {
                auth: ModelAuth {
                    api_key: Some(SecretString::new("offline-fixture")),
                    headers: None,
                    base_url: Some("https://resolved.invalid/path".into()),
                },
                env: None,
                source: None,
            }))
        })
    }
}
struct Product {
    model: Model,
    adapter: Arc<Faux>,
    auth: ProviderAuth,
}
impl Provider for Product {
    fn id(&self) -> &ProviderId {
        &self.model.provider
    }
    fn name(&self) -> &str {
        "offline fixture"
    }
    fn auth(&self) -> &ProviderAuth {
        &self.auth
    }
    fn models(&self) -> Result<Arc<[Model]>, ModelsError> {
        Ok(vec![self.model.clone()].into())
    }
    fn adapter(&self, _: &ApiId) -> Option<Arc<dyn ApiAdapter>> {
        Some(self.adapter.clone())
    }
}
#[derive(Default)]
struct Callbacks {
    handlers: bool,
    before: Mutex<Vec<BeforePayloadEvent>>,
    after: Mutex<Vec<AfterResponseEvent>>,
    events: Mutex<Vec<AssistantMessageEvent>>,
    replacement: Option<JsonValue>,
    payload_closed: bool,
    payload_fault: bool,
    payload_panic_construct: bool,
    payload_panic_poll: bool,
    stream_fault: bool,
    after_closed: bool,
    cancel_after_start: Option<CancellationToken>,
    after_started: AtomicBool,
    release_after: AtomicBool,
    patch_response: bool,
    calls: AtomicUsize,
}
impl RequestCallbacks for Callbacks {
    fn has_payload_handlers(&self) -> bool {
        self.handlers
    }
    fn before_payload(
        &self,
        event: BeforePayloadEvent,
    ) -> BoxFuture<'_, Result<Option<JsonValue>, HarnessRuntimeError>> {
        assert!(!self.payload_panic_construct, "fixture constructor panic");
        Box::pin(async move {
            assert!(!self.payload_panic_poll, "fixture polling panic");
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.before.lock().unwrap().push(event);
            if self.payload_closed {
                return Err(HarnessRuntimeError::Closed(HarnessClosed));
            }
            if self.payload_fault {
                return Err(HarnessRuntimeError::Fault(HarnessFault::new(
                    ErrorContext::new("payload callback specific fault").unwrap(),
                )));
            }
            Ok(self.replacement.clone())
        })
    }
    fn after_response(
        &self,
        event: AfterResponseEvent,
    ) -> BoxFuture<'_, Result<SettledAssistantMessage, HarnessRuntimeError>> {
        Box::pin(async move {
            self.after.lock().unwrap().push(event.clone());
            self.after_started.store(true, Ordering::SeqCst);
            if let Some(signal) = &self.cancel_after_start {
                signal.cancel();
                std::future::poll_fn(|_| {
                    if self.release_after.load(Ordering::SeqCst) {
                        std::task::Poll::Ready(())
                    } else {
                        std::task::Poll::Pending
                    }
                })
                .await;
            }
            if self.after_closed {
                return Err(HarnessRuntimeError::Closed(HarnessClosed));
            }
            if self.patch_response {
                let mut m = event.message.into_message();
                m.response_id = Some(ResponseId::new("post-hook").unwrap());
                return Ok(SettledAssistantMessage::new(m).unwrap());
            }
            Ok(event.message)
        })
    }
    fn stream_event(
        &self,
        event: AssistantMessageEvent,
    ) -> BoxFuture<'_, Result<(), HarnessRuntimeError>> {
        Box::pin(async move {
            self.events.lock().unwrap().push(event);
            if self.stream_fault {
                return Err(HarnessRuntimeError::Fault(HarnessFault::new(
                    ErrorContext::new("event callback specific fault").unwrap(),
                )));
            }
            Ok(())
        })
    }
}
struct Fixture {
    executor: Arc<Executor>,
    capture: Arc<Mutex<Vec<Capture>>>,
    input: RequestInput,
}
fn fixture(
    callbacks: Arc<Callbacks>,
    payload: serde_json::Value,
    head: Option<RequestResponseHead>,
    reason: StopReason,
    cancel: bool,
) -> Fixture {
    let executor = Arc::new(Executor::default());
    let capture = Arc::new(Mutex::new(vec![]));
    let model = model();
    let adapter = Arc::new(Faux {
        model: model.clone(),
        capture: capture.clone(),
        payload,
        head,
        reason,
        cancel_at_terminal: cancel,
    });
    let models = Arc::new(ModelsRegistry::new(ModelsRegistryOptions {
        credentials: Arc::new(InMemoryCredentialStore::new()),
        models_store: Box::new(InMemoryModelsStore::new()),
        auth_context: Arc::new(Host),
        clock: Arc::new(TestClock),
        spawner: executor.clone(),
        model_header_policy: Default::default(),
    }));
    executor
        .drive(models.set_provider(Arc::new(Product {
            model: model.clone(),
            adapter,
            auth: ProviderAuth::new(Some(Arc::new(Auth)), None).unwrap(),
        })))
        .unwrap();
    let input = RequestInput {
        models,
        model,
        context: serde_json::from_value(
            serde_json::json!({"systemPrompt":"supplied context", "messages":[]}),
        )
        .unwrap(),
        thinking: AgentThinkingLevel::High,
        options: Default::default(),
        runtime: RequestRuntime::with_noop_telemetry(
            CancellationToken::new(),
            Arc::new(TestClock),
            executor.clone(),
        ),
        lane: LaneId::new("main").unwrap(),
        run_id: OperationId::new("run-1").unwrap(),
        session_id: SessionId::new("session-1").unwrap(),
        callbacks,
    };
    Fixture {
        executor,
        capture,
        input,
    }
}

#[test]
fn complete_clear_recase_options_and_logical_replacement_reach_real_adapter() {
    let callbacks = Arc::new(Callbacks {
        handlers: true,
        replacement: Some(JsonValue::Null),
        patch_response: true,
        ..Default::default()
    });
    let mut f = fixture(
        callbacks.clone(),
        serde_json::json!({"logical":[true, null, 7]}),
        None,
        StopReason::Stop,
        false,
    );
    let policy = ProviderHeaderPolicy::from_allowlist(["x-a".into()]);
    let base = AgentHarnessStreamOptions {
        headers: Some(
            RawProviderHeaders::new([("X-A".into(), "old".into())].into())
                .unwrap()
                .classify(&policy)
                .unwrap(),
        ),
        ..Default::default()
    };
    let cleared = apply_stream_options_patch(
        &base,
        &AgentHarnessStreamOptionsPatch {
            headers: Presence::Null,
            ..Default::default()
        },
        &policy,
    )
    .unwrap();
    let patch = AgentHarnessStreamOptionsPatch {
        headers: Presence::Value(
            RawProviderHeaderPatch::new([("x-a".into(), Some("new".into()))].into())
                .unwrap()
                .classify(&policy)
                .unwrap(),
        ),
        ..Default::default()
    };
    f.input.options = apply_stream_options_patch(&cleared, &patch, &policy).unwrap();
    f.input.options.transport = Some(Transport::WebSocketCached);
    f.input.options.timeout_ms = Some(SafeUnsigned::new(0).unwrap());
    f.input.options.max_retries = Some(SafeUnsigned::new(0).unwrap());
    f.input.options.max_retry_delay_ms = Some(SafeUnsigned::new(123).unwrap());
    f.input.options.cache_retention = Some(CacheRetention::Long);
    f.input.options.metadata = Some(
        serde_json::from_value(serde_json::json!({"null":null,"empty":[],"false":false})).unwrap(),
    );
    f.input.options.deferred =
        Some(serde_json::from_value(serde_json::json!({"window":"24h"})).unwrap());
    let original = f.input.options.clone();
    let context = f.input.context.clone();
    let signal = f.input.runtime.cancellation.clone();
    let result = f.executor.drive(run_request(f.input)).unwrap();
    let capture = f.capture.lock().unwrap();
    assert_eq!(capture.len(), 1);
    let c = &capture[0];
    assert_eq!(
        c.headers.as_map(),
        &[("x-a".to_owned(), Some("new".to_owned()))].into()
    );
    assert_eq!(c.wire.stream.transport, original.transport);
    assert_eq!(c.wire.stream.timeout_ms, original.timeout_ms);
    assert_eq!(c.wire.stream.max_retries, original.max_retries);
    assert_eq!(
        c.wire.stream.max_retry_delay_ms,
        original.max_retry_delay_ms
    );
    assert_eq!(c.wire.stream.metadata, original.metadata);
    assert_eq!(c.wire.stream.cache_retention, original.cache_retention);
    assert_eq!(c.wire.deferred, original.deferred);
    assert_eq!(c.wire.reasoning, Some(ThinkingLevel::High));
    assert_eq!(c.wire.stream.session_id.as_deref(), Some("session-1"));
    assert_eq!(c.context, context);
    assert_eq!(c.payload, serde_json::Value::Null);
    assert!(c.has_transform && c.has_auth);
    assert_eq!(c.endpoint, "https://resolved.invalid/path");
    signal.cancel();
    assert!(c.signal.is_cancelled());
    assert_eq!(callbacks.calls.load(Ordering::SeqCst), 1);
    let before = callbacks.before.lock().unwrap();
    assert_eq!(before[0].run_id.as_str(), "run-1");
    assert_eq!(before[0].lane.as_str(), "main");
    assert_eq!(before[0].model, model());
    assert_eq!(
        serde_json::to_value(&before[0].payload).unwrap(),
        serde_json::json!({"logical":[true,null,7]})
    );
    let events = callbacks.events.lock().unwrap();
    assert!(matches!(&events[0], AssistantMessageEvent::Start { .. }));
    assert!(
        matches!(events.last().unwrap(), AssistantMessageEvent::Done { message, .. } if message.response_id.is_none())
    );
    assert_eq!(
        result.as_message().response_id.as_ref().unwrap().as_str(),
        "post-hook"
    );
}

#[test]
fn unsafe_integer_is_outer_trusted_fault_and_cannot_skip_registered_hook() {
    let callbacks = Arc::new(Callbacks {
        handlers: true,
        ..Default::default()
    });
    let f = fixture(
        callbacks.clone(),
        serde_json::json!({"unsafe":9_007_199_254_740_993u64}),
        None,
        StopReason::Stop,
        false,
    );
    let error = f.executor.drive(run_request(f.input)).unwrap_err();
    assert!(matches!(error, HarnessRuntimeError::Fault(_)));
    assert_eq!(
        error.to_string(),
        "request payload cannot be represented exactly"
    );
    assert_eq!(callbacks.calls.load(Ordering::SeqCst), 0);
    assert!(callbacks.after.lock().unwrap().is_empty());
    assert!(f.capture.lock().unwrap()[0].has_transform);
}

#[test]
fn no_handlers_preserve_wider_payload_and_absent_options() {
    let callbacks = Arc::new(Callbacks::default());
    let payload = serde_json::json!({"unsafe":u64::MAX});
    let mut f = fixture(
        callbacks.clone(),
        payload.clone(),
        None,
        StopReason::Stop,
        false,
    );
    f.input.thinking = AgentThinkingLevel::Off;
    assert_eq!(
        f.executor
            .drive(run_request(f.input))
            .unwrap()
            .as_message()
            .stop_reason,
        StopReason::Stop
    );
    let capture = f.capture.lock().unwrap();
    assert!(!capture[0].has_transform);
    assert_eq!(capture[0].payload, payload);
    assert!(capture[0].wire.stream.metadata.is_none());
    assert!(capture[0].wire.deferred.is_none());
    assert!(capture[0].wire.reasoning.is_none());
    assert!(capture[0].wire.stream.timeout_ms.is_none());
    assert_eq!(callbacks.calls.load(Ordering::SeqCst), 0);
    let after = callbacks.after.lock().unwrap();
    assert!(after[0].headers.is_none() && after[0].status.is_none());
}

#[test]
fn provider_error_remains_in_band_with_sanitized_numeric_metadata() {
    let callbacks = Arc::new(Callbacks::default());
    let number = |n| RequestResponseHeaderValue::Number(FiniteF64::new(n).unwrap());
    let text = |s: &str| RequestResponseHeaderValue::Text(s.into());
    let head = RequestResponseHead {
        status: 429,
        headers: vec![
            ("x-count".into(), number(1e21)),
            ("x-small".into(), number(1e-7)),
            ("x-zero".into(), number(-0.0)),
            ("authorization".into(), text("offline-only")),
            ("X-Duplicate".into(), text("one")),
            ("x-duplicate".into(), text("two")),
            ("x-keep".into(), text("observed")),
        ],
    };
    let f = fixture(
        callbacks.clone(),
        serde_json::json!({}),
        Some(head),
        StopReason::Error,
        false,
    );
    let result = f.executor.drive(run_request(f.input)).unwrap();
    assert_eq!(result.as_message().stop_reason, StopReason::Error);
    assert_eq!(
        result.as_message().error_message.as_deref(),
        Some("provider fixture error")
    );
    let after = callbacks.after.lock().unwrap();
    assert_eq!(after.len(), 1);
    assert_eq!(after[0].status.unwrap().get(), 429);
    let h = after[0].headers.as_ref().unwrap().as_map();
    assert_eq!(h.len(), 4);
    assert_eq!(h["x-count"], "1e+21");
    assert_eq!(h["x-small"], "1e-7");
    assert_eq!(h["x-zero"], "0");
    assert_eq!(h["x-keep"], "observed");
    assert!(matches!(
        callbacks.events.lock().unwrap().last().unwrap(),
        AssistantMessageEvent::Error { .. }
    ));
}

#[test]
fn cancellation_before_response_hook_suppresses_it() {
    let callbacks = Arc::new(Callbacks::default());
    let f = fixture(
        callbacks.clone(),
        serde_json::json!({}),
        None,
        StopReason::Stop,
        true,
    );
    let result = f.executor.drive(run_request(f.input)).unwrap();
    assert_eq!(result.as_message().stop_reason, StopReason::Aborted);
    assert!(callbacks.after.lock().unwrap().is_empty());
}

#[test]
fn started_response_hook_finishes_after_abort_without_early_final_result() {
    let signal = CancellationToken::new();
    let callbacks = Arc::new(Callbacks {
        cancel_after_start: Some(signal.clone()),
        patch_response: true,
        ..Default::default()
    });
    let mut f = fixture(
        callbacks.clone(),
        serde_json::json!({}),
        None,
        StopReason::Stop,
        false,
    );
    f.input.runtime.cancellation = signal.clone();
    let mut future = Box::pin(run_request(f.input));
    for _ in 0..20 {
        assert!(poll(future.as_mut()).is_pending());
        if callbacks.after_started.load(Ordering::SeqCst) {
            break;
        }
        f.executor.tick();
    }
    assert!(callbacks.after_started.load(Ordering::SeqCst));
    assert!(signal.is_cancelled());
    assert!(poll(future.as_mut()).is_pending());
    callbacks.release_after.store(true, Ordering::SeqCst);
    let result = f.executor.drive(future).unwrap();
    assert_eq!(
        result.as_message().response_id.as_ref().unwrap().as_str(),
        "post-hook"
    );
    assert_eq!(result.as_message().stop_reason, StopReason::Stop);
}

#[test]
fn payload_callback_closed_and_specific_fault_survive_models_normalization() {
    for closed in [false, true] {
        let callbacks = Arc::new(Callbacks {
            handlers: true,
            payload_closed: closed,
            payload_fault: !closed,
            ..Default::default()
        });
        let f = fixture(
            callbacks.clone(),
            serde_json::json!({}),
            None,
            StopReason::Stop,
            false,
        );
        let error = f.executor.drive(run_request(f.input)).unwrap_err();
        if closed {
            assert!(matches!(error, HarnessRuntimeError::Closed(_)));
        } else {
            assert_eq!(error.to_string(), "payload callback specific fault");
        }
        assert_eq!(callbacks.calls.load(Ordering::SeqCst), 1);
        assert!(callbacks.after.lock().unwrap().is_empty());
    }
}

#[test]
fn event_and_response_callback_errors_remain_outer() {
    for stream_fault in [false, true] {
        let callbacks = Arc::new(Callbacks {
            stream_fault,
            after_closed: !stream_fault,
            ..Default::default()
        });
        let f = fixture(
            callbacks.clone(),
            serde_json::json!({}),
            None,
            StopReason::Stop,
            false,
        );
        let error = f.executor.drive(run_request(f.input)).unwrap_err();
        if stream_fault {
            assert_eq!(error.to_string(), "event callback specific fault");
            assert!(callbacks.after.lock().unwrap().is_empty());
        } else {
            assert!(matches!(error, HarnessRuntimeError::Closed(_)));
        }
    }
}

#[test]
fn spawn_rejection_is_a_real_settled_setup_error_without_adapter_call() {
    let callbacks = Arc::new(Callbacks::default());
    let f = fixture(
        callbacks.clone(),
        serde_json::json!({}),
        None,
        StopReason::Stop,
        false,
    );
    f.executor.reject.store(true, Ordering::SeqCst);
    let result = f.executor.drive(run_request(f.input)).unwrap();
    assert_eq!(result.as_message().stop_reason, StopReason::Error);
    assert_eq!(result.as_message().api, model().api);
    assert_eq!(result.as_message().provider, model().provider);
    assert_eq!(result.as_message().model, model().id);
    assert!(f.capture.lock().unwrap().is_empty());
    let after = callbacks.after.lock().unwrap();
    assert_eq!(after.len(), 1);
    assert!(after[0].status.is_none() && after[0].headers.is_none());
}

#[test]
fn trusted_payload_callback_unwinds_are_outer_faults() {
    for construction in [false, true] {
        let callbacks = Arc::new(Callbacks {
            handlers: true,
            payload_panic_construct: construction,
            payload_panic_poll: !construction,
            ..Default::default()
        });
        let f = fixture(
            callbacks.clone(),
            serde_json::json!({}),
            None,
            StopReason::Stop,
            false,
        );
        let error = f.executor.drive(run_request(f.input)).unwrap_err();
        assert!(matches!(error, HarnessRuntimeError::Fault(_)));
        assert_eq!(error.to_string(), "trusted request callback panicked");
        assert!(callbacks.after.lock().unwrap().is_empty());
    }
}

#[test]
fn earlier_payload_fault_is_not_replaced_by_terminal_event_callback_fault() {
    let callbacks = Arc::new(Callbacks {
        handlers: true,
        payload_fault: true,
        stream_fault: true,
        ..Default::default()
    });
    let f = fixture(
        callbacks.clone(),
        serde_json::json!({}),
        None,
        StopReason::Stop,
        false,
    );
    assert_eq!(
        f.executor
            .drive(run_request(f.input))
            .unwrap_err()
            .to_string(),
        "payload callback specific fault"
    );
    assert!(callbacks.events.lock().unwrap().is_empty());
}

#[test]
fn empty_metadata_and_every_captured_thinking_level_remain_distinct() {
    let levels = [
        (AgentThinkingLevel::Off, None),
        (AgentThinkingLevel::Minimal, Some(ThinkingLevel::Minimal)),
        (AgentThinkingLevel::Low, Some(ThinkingLevel::Low)),
        (AgentThinkingLevel::Medium, Some(ThinkingLevel::Medium)),
        (AgentThinkingLevel::High, Some(ThinkingLevel::High)),
        (
            AgentThinkingLevel::ExtraHigh,
            Some(ThinkingLevel::ExtraHigh),
        ),
        (AgentThinkingLevel::Max, Some(ThinkingLevel::Max)),
    ];
    for (thinking, expected) in levels {
        let callbacks = Arc::new(Callbacks {
            handlers: true,
            ..Default::default()
        });
        let payload = serde_json::json!([
            null,
            false,
            -9_007_199_254_740_991i64,
            9_007_199_254_740_991u64,
            -0.0,
            0.25
        ]);
        let mut f = fixture(callbacks, payload.clone(), None, StopReason::Stop, false);
        f.input.thinking = thinking;
        f.input.options.metadata = Some(JsonObject::default());
        f.executor.drive(run_request(f.input)).unwrap();
        let c = f.capture.lock().unwrap();
        assert_eq!(c[0].wire.reasoning, expected);
        assert_eq!(c[0].wire.stream.metadata, Some(JsonObject::default()));
        assert_eq!(c[0].payload, payload);
    }
}

#[test]
fn malformed_provider_terminal_settles_in_band_using_known_request_identity() {
    let callbacks = Arc::new(Callbacks::default());
    let f = fixture(
        callbacks.clone(),
        serde_json::json!({}),
        None,
        StopReason::Pending,
        false,
    );
    let result = f.executor.drive(run_request(f.input)).unwrap();
    assert!(f.capture.lock().unwrap()[0].invalid_terminal_rejected);
    assert_eq!(result.as_message().stop_reason, StopReason::Error);
    assert_eq!(result.as_message().api, model().api);
    assert_eq!(result.as_message().provider, model().provider);
    assert_eq!(result.as_message().model, model().id);
    assert_eq!(callbacks.after.lock().unwrap().len(), 1);
}

#[test]
fn already_cancelled_request_starts_no_adapter_or_response_hook() {
    let callbacks = Arc::new(Callbacks {
        handlers: true,
        ..Default::default()
    });
    let f = fixture(
        callbacks.clone(),
        serde_json::json!({}),
        None,
        StopReason::Stop,
        false,
    );
    f.input.runtime.cancellation.cancel();
    let result = f.executor.drive(run_request(f.input)).unwrap();
    assert_eq!(result.as_message().stop_reason, StopReason::Aborted);
    assert!(f.capture.lock().unwrap().is_empty());
    assert_eq!(callbacks.calls.load(Ordering::SeqCst), 0);
    assert!(callbacks.after.lock().unwrap().is_empty());
}
