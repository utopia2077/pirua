//! Deferred helper fixtures adapted from accepted request_adapter at cdfd481.
mod request_deferred_support;

use pi_agent_schema::{
    AfterResponseEvent, AgentHarnessStreamOptions, BeforePayloadEvent, ErrorContext, HarnessClosed,
    HarnessFault, HarnessRuntimeError, LaneId, OperationId, ProviderHeaderPolicy,
    RawProviderHeaders, SettledAssistantMessage,
};
use pi_ai_core::*;
use pi_harness_runtime::request::*;
use request_deferred_support::*;
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
    wire: DeferredFetchOptionsWire,
    headers: ProviderHeaders,
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
    cancels: Arc<Mutex<Vec<CancelCapture>>>,
    cancel_mode: u8,
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
        panic!("unexpected ordinary deferred route")
    }
    fn cancel_deferred<'a>(
        &'a self,
        r: DeferredCancelRequest,
    ) -> BoxFuture<'a, Result<(), ModelsError>> {
        self.cancels.lock().unwrap().push(CancelCapture {
            wire: r.options.wire.clone(),
            headers: r.auth.headers.clone(),
            signal: r.options.runtime.cancellation.clone(),
            handle: r.handle,
            has_auth: r.auth.api_key.is_some(),
        });
        assert_ne!(self.cancel_mode, 1, "cancel constructor fixture panic");
        Box::pin(async move {
            assert_ne!(self.cancel_mode, 2, "cancel poll fixture panic");
            if self.cancel_mode == 3 {
                return Err(ModelsError::new(ModelsErrorCode::Provider));
            }
            if self.cancel_mode == 4 {
                std::future::pending::<()>().await;
            }
            Ok(())
        })
    }
}

impl RequestLifecycleAdapter for Faux {
    fn deferred_lifecycle(&self) -> Option<&dyn DeferredRequestLifecycleAdapter> {
        Some(self)
    }
    fn stream_with_lifecycle(
        &self,
        _: TextStreamRequest,
        _: RequestLifecycleInvocation,
    ) -> AssistantStream {
        panic!("unexpected raw lifecycle")
    }
    fn stream_simple_with_lifecycle(
        &self,
        _: SimpleStreamRequest,
        _: RequestLifecycleInvocation,
    ) -> AssistantStream {
        panic!("unexpected simple lifecycle")
    }
}
impl DeferredRequestLifecycleAdapter for Faux {
    fn fetch_deferred_with_lifecycle(
        &self,
        request: DeferredFetchRequest,
        lifecycle: RequestLifecycleInvocation,
    ) -> Result<AssistantStream, CapabilityError> {
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
                let mut latest = assistant(StopReason::Pending);
                latest.usage.input = SafeUnsigned::new(17).unwrap();
                latest.usage.total_tokens = SafeUnsigned::new(17).unwrap();
                task_producer.publish_cancellation_snapshot(latest).unwrap();
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
        Ok(stream)
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
    handler_queries: AtomicUsize,
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
    attempts: AtomicUsize,
}
impl RequestCallbacks for Callbacks {
    fn has_payload_handlers(&self) -> bool {
        self.handler_queries.fetch_add(1, Ordering::SeqCst);
        self.handlers
    }
    fn before_payload(
        &self,
        event: BeforePayloadEvent,
    ) -> BoxFuture<'_, Result<Option<JsonValue>, HarnessRuntimeError>> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
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
    input: DeferredRequestInput,
    captured: CapturedTextDispatch,
    cancels: Arc<Mutex<Vec<CancelCapture>>>,
}
struct CancelCapture {
    wire: DeferredCancelOptionsWire,
    headers: ProviderHeaders,
    signal: CancellationToken,
    handle: DeferredHandle,
    has_auth: bool,
}
fn handle() -> DeferredHandle {
    let m = model();
    DeferredHandle {
        provider: m.provider,
        model_id: m.id,
        api: m.api,
        id: DeferredId::new("handle").unwrap(),
        expires_at: None,
        poll_after_ms: None,
        data: Presence::Null,
    }
}

fn fixture(
    callbacks: Arc<Callbacks>,
    payload: serde_json::Value,
    head: Option<RequestResponseHead>,
    reason: StopReason,
    cancel: bool,
) -> Fixture {
    fixture_mode(callbacks, payload, head, reason, cancel, 0)
}
fn fixture_mode(
    callbacks: Arc<Callbacks>,
    payload: serde_json::Value,
    head: Option<RequestResponseHead>,
    reason: StopReason,
    cancel: bool,
    cancel_mode: u8,
) -> Fixture {
    let executor = Arc::new(Executor::default());
    let capture = Arc::new(Mutex::new(vec![]));
    let model = model();
    let cancels = Arc::new(Mutex::new(vec![]));
    let adapter = Arc::new(Faux {
        model: model.clone(),
        capture: capture.clone(),
        cancels: cancels.clone(),
        cancel_mode,
        payload,
        head,
        reason,
        cancel_at_terminal: cancel,
    });
    let models = Arc::new(ModelsRegistry::new_with_child_cancellation(
        ModelsRegistryOptions {
            credentials: Arc::new(InMemoryCredentialStore::new()),
            models_store: Box::new(InMemoryModelsStore::new()),
            auth_context: Arc::new(Host),
            clock: Arc::new(TestClock),
            spawner: executor.clone(),
            model_header_policy: Default::default(),
        },
    ));
    executor
        .drive(models.set_provider(Arc::new(Product {
            model: model.clone(),
            adapter,
            auth: ProviderAuth::new(Some(Arc::new(Auth)), None).unwrap(),
        })))
        .unwrap();
    let captured = models
        .capture_text_dispatch(
            &models.provider_registration(&model.provider).unwrap(),
            model.clone(),
        )
        .unwrap();
    let input = DeferredRequestInput {
        models,
        model,
        handle: handle(),
        options: Default::default(),
        runtime: RequestRuntime::with_noop_telemetry(
            CancellationToken::new(),
            Arc::new(TestClock),
            executor.clone(),
        ),
        lane: LaneId::new("main").unwrap(),
        run_id: OperationId::new("run-1").unwrap(),
        callbacks,
    };
    Fixture {
        executor,
        capture,
        input,
        captured,
        cancels,
    }
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
    let error = f
        .executor
        .drive(run_deferred_request(f.input, f.captured))
        .unwrap_err();
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
    let result = f
        .executor
        .drive(run_deferred_request(f.input, f.captured))
        .unwrap();
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
    let result = f
        .executor
        .drive(run_deferred_request(f.input, f.captured))
        .unwrap();
    assert_eq!(result.as_message().stop_reason, StopReason::Aborted);
    assert_eq!(result.as_message().usage.input.get(), 17);
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
    let mut future = Box::pin(run_deferred_request(f.input, f.captured));
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
        let error = f
            .executor
            .drive(run_deferred_request(f.input, f.captured))
            .unwrap_err();
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
        let error = f
            .executor
            .drive(run_deferred_request(f.input, f.captured))
            .unwrap_err();
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
    let result = f
        .executor
        .drive(run_deferred_request(f.input, f.captured))
        .unwrap();
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
        let error = f
            .executor
            .drive(run_deferred_request(f.input, f.captured))
            .unwrap_err();
        assert!(matches!(error, HarnessRuntimeError::Fault(_)));
        assert_eq!(callbacks.attempts.load(Ordering::SeqCst), 1);
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
            .drive(run_deferred_request(f.input, f.captured))
            .unwrap_err()
            .to_string(),
        "payload callback specific fault"
    );
    assert!(callbacks.events.lock().unwrap().is_empty());
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
    let result = f
        .executor
        .drive(run_deferred_request(f.input, f.captured))
        .unwrap();
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
    let result = f
        .executor
        .drive(run_deferred_request(f.input, f.captured))
        .unwrap();
    assert_eq!(result.as_message().stop_reason, StopReason::Aborted);
    assert!(f.capture.lock().unwrap().is_empty());
    assert_eq!(callbacks.calls.load(Ordering::SeqCst), 0);
    assert!(callbacks.after.lock().unwrap().is_empty());
}

fn curated_options() -> AgentHarnessStreamOptions {
    let policy = ProviderHeaderPolicy::from_allowlist(["x-fixture".into()]);
    AgentHarnessStreamOptions {
        headers: Some(
            RawProviderHeaders::new([("X-Fixture".into(), "value".into())].into())
                .unwrap()
                .classify(&policy)
                .unwrap(),
        ),
        timeout_ms: Some(SafeUnsigned::new(321).unwrap()),
        max_retries: Some(SafeUnsigned::new(2).unwrap()),
        max_retry_delay_ms: Some(SafeUnsigned::new(987).unwrap()),
        transport: Some(Transport::WebSocketCached),
        cache_retention: Some(CacheRetention::Long),
        metadata: Some(JsonObject::default()),
        deferred: Some(serde_json::from_value(serde_json::json!({"window":"24h"})).unwrap()),
    }
}
#[test]
fn actual_deferred_helper_mounts_payload_and_only_supported_curated_fields() {
    let callbacks = Arc::new(Callbacks {
        handlers: true,
        replacement: Some(JsonValue::Null),
        patch_response: true,
        ..Default::default()
    });
    let mut f = fixture(
        callbacks.clone(),
        serde_json::json!({"logical":[true,7,null]}),
        None,
        StopReason::Stop,
        false,
    );
    f.input.options = curated_options();
    let original = f.input.options.clone();
    let token = f.input.runtime.cancellation.clone();
    let result = f
        .executor
        .drive(run_deferred_request(f.input, f.captured))
        .unwrap();
    let captures = f.capture.lock().unwrap();
    assert_eq!(captures.len(), 1);
    let c = &captures[0];
    assert_eq!(
        c.wire,
        DeferredFetchOptionsWire {
            timeout_ms: original.timeout_ms,
            max_retries: original.max_retries,
            max_retry_delay_ms: original.max_retry_delay_ms,
            wait: Some(SafeUnsigned::new(0).unwrap())
        }
    );
    assert_eq!(
        c.headers.as_map(),
        &[("X-Fixture".into(), Some("value".into()))].into()
    );
    assert_eq!(c.payload, serde_json::Value::Null);
    assert!(c.has_transform && c.has_auth);
    assert_eq!(c.endpoint, "https://resolved.invalid/path");
    token.cancel();
    assert!(c.signal.is_cancelled());
    assert_eq!(callbacks.calls.load(Ordering::SeqCst), 1);
    let before = callbacks.before.lock().unwrap();
    assert_eq!(before[0].model, model());
    assert_eq!(before[0].lane.as_str(), "main");
    assert_eq!(before[0].run_id.as_str(), "run-1");
    assert_eq!(
        result.as_message().response_id.as_ref().unwrap().as_str(),
        "post-hook"
    );
}
#[test]
fn absent_handlers_leave_wider_payload_and_missing_metadata_unchanged() {
    let callbacks = Arc::new(Callbacks::default());
    let f = fixture(
        callbacks.clone(),
        serde_json::json!({"wide":u64::MAX}),
        None,
        StopReason::Stop,
        false,
    );
    let result = f
        .executor
        .drive(run_deferred_request(f.input, f.captured))
        .unwrap();
    assert_eq!(result.as_message().stop_reason, StopReason::Stop);
    let c = f.capture.lock().unwrap();
    assert!(!c[0].has_transform);
    assert_eq!(c[0].payload, serde_json::json!({"wide":u64::MAX}));
    assert!(c[0].wire.timeout_ms.is_none());
    assert!(callbacks.before.lock().unwrap().is_empty());
    let after = callbacks.after.lock().unwrap();
    assert!(after[0].headers.is_none() && after[0].status.is_none());
}
#[test]
fn captured_fetch_uses_original_provider_after_replacement() {
    let callbacks = Arc::new(Callbacks::default());
    let f = fixture(
        callbacks,
        serde_json::json!({"old":true}),
        None,
        StopReason::Stop,
        false,
    );
    let new_capture = Arc::new(Mutex::new(vec![]));
    let new = Arc::new(Faux {
        model: model(),
        capture: new_capture.clone(),
        cancels: Arc::new(Mutex::new(vec![])),
        cancel_mode: 0,
        payload: serde_json::json!({"new":true}),
        head: None,
        reason: StopReason::Stop,
        cancel_at_terminal: false,
    });
    f.executor
        .drive(f.input.models.set_provider(Arc::new(Product {
            model: model(),
            adapter: new,
            auth: ProviderAuth::new(Some(Arc::new(Auth)), None).unwrap(),
        })))
        .unwrap();
    f.executor
        .drive(run_deferred_request(f.input, f.captured))
        .unwrap();
    assert_eq!(f.capture.lock().unwrap().len(), 1);
    assert!(new_capture.lock().unwrap().is_empty());
}
fn cancel_input(input: DeferredRequestInput) -> DeferredCancelInput {
    DeferredCancelInput {
        models: input.models,
        model: input.model,
        handle: input.handle,
        options: input.options,
        runtime: input.runtime,
    }
}
#[test]
fn actual_cancel_maps_options_and_signal_without_any_callbacks() {
    let callbacks = Arc::new(Callbacks {
        handlers: true,
        payload_panic_construct: true,
        after_closed: true,
        ..Default::default()
    });
    let mut f = fixture(
        callbacks.clone(),
        serde_json::json!({}),
        None,
        StopReason::Stop,
        false,
    );
    f.input.options = curated_options();
    let options = f.input.options.clone();
    let close = f.input.runtime.cancellation.clone();
    let unrelated_operation = CancellationToken::new();
    unrelated_operation.cancel();
    f.executor
        .drive(cancel_deferred_request(cancel_input(f.input), f.captured))
        .unwrap()
        .unwrap();
    let seen = f.cancels.lock().unwrap();
    assert_eq!(seen.len(), 1);
    assert_eq!(
        seen[0].wire,
        DeferredCancelOptionsWire {
            timeout_ms: options.timeout_ms,
            max_retries: options.max_retries,
            max_retry_delay_ms: options.max_retry_delay_ms
        }
    );
    assert_eq!(
        seen[0].headers.as_map(),
        &[("X-Fixture".into(), Some("value".into()))].into()
    );
    assert_eq!(seen[0].handle, handle());
    assert!(seen[0].has_auth);
    assert!(!seen[0].signal.is_cancelled());
    close.cancel();
    assert!(seen[0].signal.is_cancelled());
    assert_eq!(callbacks.handler_queries.load(Ordering::SeqCst), 0);
    assert_eq!(callbacks.calls.load(Ordering::SeqCst), 0);
    assert!(callbacks.before.lock().unwrap().is_empty());
    assert!(callbacks.after.lock().unwrap().is_empty());
    assert!(callbacks.events.lock().unwrap().is_empty());
    assert!(f.capture.lock().unwrap().is_empty());
}
#[test]
fn cancel_provider_constructor_poll_errors_and_close_token_are_inner_only() {
    for mode in [1, 2, 3, 4] {
        let f = fixture_mode(
            Arc::new(Callbacks::default()),
            serde_json::json!({}),
            None,
            StopReason::Stop,
            false,
            mode,
        );
        let token = f.input.runtime.cancellation.clone();
        let mut future = Box::pin(cancel_deferred_request(cancel_input(f.input), f.captured));
        if mode == 4 {
            assert!(poll(future.as_mut()).is_pending());
            token.cancel();
        }
        let result = f
            .executor
            .drive(future)
            .expect("provider outcome must not fault Harness");
        assert_eq!(
            f.cancels.lock().unwrap().len(),
            1,
            "panic must follow actual adapter call"
        );
        if mode == 4 {
            assert!(matches!(result, Err(AuthResolutionError::Cancelled)));
        } else {
            assert!(matches!(result, Err(AuthResolutionError::Models(_))));
        }
    }
}
#[test]
fn capture_or_model_mismatch_is_outer_and_never_dispatches() {
    for cancel in [false, true] {
        for wrong_registry in [false, true] {
            let callbacks = Arc::new(Callbacks::default());
            let mut f = fixture(
                callbacks.clone(),
                serde_json::json!({}),
                None,
                StopReason::Stop,
                false,
            );
            if wrong_registry {
                f.input.models = fixture(
                    Arc::new(Callbacks::default()),
                    serde_json::json!({}),
                    None,
                    StopReason::Stop,
                    false,
                )
                .input
                .models;
            } else {
                f.input.model.max_tokens = SafeUnsigned::new(123).unwrap();
            }
            let outer = if cancel {
                f.executor
                    .drive(cancel_deferred_request(cancel_input(f.input), f.captured))
                    .map(|_| ())
            } else {
                f.executor
                    .drive(run_deferred_request(f.input, f.captured))
                    .map(|_| ())
            };
            assert!(matches!(outer, Err(HarnessRuntimeError::Fault(_))));
            assert!(f.cancels.lock().unwrap().is_empty());
            assert!(f.capture.lock().unwrap().is_empty());
            assert_eq!(callbacks.calls.load(Ordering::SeqCst), 0);
        }
    }
}

#[test]
fn already_cancelled_cancel_signal_never_reaches_adapter_or_callbacks() {
    let callbacks = Arc::new(Callbacks {
        handlers: true,
        payload_panic_construct: true,
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
    assert!(matches!(
        f.executor
            .drive(cancel_deferred_request(cancel_input(f.input), f.captured)),
        Ok(Err(AuthResolutionError::Cancelled))
    ));
    assert!(f.cancels.lock().unwrap().is_empty());
    assert!(f.capture.lock().unwrap().is_empty());
    assert_eq!(callbacks.handler_queries.load(Ordering::SeqCst), 0);
}
