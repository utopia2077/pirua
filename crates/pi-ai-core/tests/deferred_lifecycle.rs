//! Registry-owned deferred dispatch through the real adapter and stream pump.
mod deferred_lifecycle_support;
use deferred_lifecycle_support::*;
use pi_ai_core::*;
use std::{
    future::Future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context as TaskContext, Waker},
};

fn model() -> Model {
    let value: serde_json::Value = serde_json::from_str(include_str!(
        "../../../conformance/schema/ai/models-options.golden.json"
    ))
    .unwrap();
    serde_json::from_value(value["model"].clone()).unwrap()
}
fn handle() -> DeferredHandle {
    let m = model();
    DeferredHandle {
        provider: m.provider,
        model_id: m.id,
        api: m.api,
        id: DeferredId::new("deferred-fixture").unwrap(),
        expires_at: None,
        poll_after_ms: None,
        data: Presence::Null,
    }
}
fn runtime(host: Arc<dyn TaskSpawner>, token: CancellationToken) -> RequestRuntime {
    RequestRuntime::with_noop_telemetry(token, Arc::new(TestClock), host)
}
fn fetch_options(host: Arc<dyn TaskSpawner>, token: CancellationToken) -> DeferredFetchOptions {
    DeferredFetchOptions {
        wire: DeferredFetchOptionsWire {
            wait: Some(SafeUnsigned::new(0).unwrap()),
            ..Default::default()
        },
        runtime: runtime(host, token),
    }
}
fn cancel_options(host: Arc<dyn TaskSpawner>, token: CancellationToken) -> DeferredCancelOptions {
    DeferredCancelOptions {
        wire: Default::default(),
        runtime: runtime(host, token),
    }
}
fn message(m: &Model, usage: u64, reason: StopReason) -> AssistantMessage {
    let mut message:AssistantMessage=serde_json::from_value(serde_json::json!({
        "role":"assistant","content":[],"api":m.api,"provider":m.provider,"model":m.id,
        "usage":{"input":usage,"output":0,"cacheRead":0,"cacheWrite":0,"totalTokens":usage,"cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"total":0}},
        "stopReason":"pending","timestamp":7
    })).unwrap();
    message.stop_reason = reason;
    message
}
#[derive(Clone, Copy, Default)]
enum Mode {
    #[default]
    Ready,
    Held,
    CancelAtHandoff,
}
#[derive(Default)]
struct Config {
    no_lifecycle: bool,
    no_deferred_lifecycle: bool,
    unsupported: bool,
    mode: Mode,
    cancel_error: bool,
    cancel_wait: bool,
    auth_wait: bool,
}
struct Seen {
    endpoint: String,
    auth_present: bool,
    headers: ProviderHeaders,
    handle: DeferredHandle,
}
struct Adapter {
    model: Model,
    config: Config,
    fetches: AtomicUsize,
    ordinary: AtomicUsize,
    queries: AtomicUsize,
    cancels: AtomicUsize,
    payloads: Arc<Mutex<Vec<serde_json::Value>>>,
    seen: Mutex<Vec<Seen>>,
    held: Mutex<Option<AssistantStreamProducer>>,
}
impl Adapter {
    fn start(
        &self,
        r: DeferredFetchRequest,
        lifecycle: Option<RequestLifecycleInvocation>,
    ) -> Result<AssistantStream, CapabilityError> {
        if self.config.unsupported {
            return Err(CapabilityError::Unsupported);
        }
        self.fetches.fetch_add(1, Ordering::SeqCst);
        self.seen.lock().unwrap().push(Seen {
            endpoint: r.resolved.endpoint.expose_for_transport().into(),
            auth_present: r.auth.api_key.is_some(),
            headers: r.auth.headers.clone(),
            handle: r.handle,
        });
        assert_eq!(r.options.wire.wait, Some(SafeUnsigned::new(0).unwrap()));
        let m = r.resolved.model;
        let token = r.options.runtime.cancellation.clone();
        let (stream, producer) = create_assistant_message_event_stream(AssistantStreamInit {
            api: m.api.clone(),
            provider: m.provider.clone(),
            model: m.id.clone(),
            clock: r.options.runtime.clock,
            cancellation: token.clone(),
        });
        if matches!(self.config.mode, Mode::Held | Mode::CancelAtHandoff) {
            producer
                .publish_cancellation_snapshot(message(&m, 17, StopReason::Pending))
                .unwrap();
            *self.held.lock().unwrap() = Some(producer);
            if matches!(self.config.mode, Mode::CancelAtHandoff) {
                token.cancel();
            }
        } else {
            let payloads = self.payloads.clone();
            r.options
                .runtime
                .spawner
                .spawn(Box::pin(async move {
                    let mut payload = serde_json::json!({"original":true});
                    if let Some(lifecycle) = lifecycle {
                        if let Some(transform) = lifecycle.payload {
                            match transform.transform(&m, &mut payload).await {
                                Ok(Some(replacement)) => payload = replacement,
                                Ok(None) => {}
                                Err(_) => {
                                    producer.fail_setup(StreamSetupFailure::Adapter).unwrap();
                                    return;
                                }
                            }
                        }
                        lifecycle.recorder.record(RequestResponseHead {
                            status: 207,
                            headers: vec![(
                                "x-call".into(),
                                RequestResponseHeaderValue::Text(payload.to_string()),
                            )],
                        });
                    }
                    payloads.lock().unwrap().push(payload);
                    producer
                        .push(AssistantMessageEvent::Done {
                            reason: DoneReason::Stop,
                            message: Box::new(message(&m, 13, StopReason::Stop)),
                        })
                        .unwrap();
                }))
                .unwrap();
        }
        Ok(stream)
    }
}
impl RequestLifecycleAdapter for Adapter {
    fn deferred_lifecycle(&self) -> Option<&dyn DeferredRequestLifecycleAdapter> {
        (!self.config.no_deferred_lifecycle).then_some(self)
    }
    fn stream_with_lifecycle(
        &self,
        r: TextStreamRequest,
        _: RequestLifecycleInvocation,
    ) -> AssistantStream {
        self.stream(r)
    }
    fn stream_simple_with_lifecycle(
        &self,
        r: SimpleStreamRequest,
        _: RequestLifecycleInvocation,
    ) -> AssistantStream {
        self.stream_simple(r)
    }
}
impl DeferredRequestLifecycleAdapter for Adapter {
    fn fetch_deferred_with_lifecycle(
        &self,
        r: DeferredFetchRequest,
        l: RequestLifecycleInvocation,
    ) -> Result<AssistantStream, CapabilityError> {
        self.start(r, Some(l))
    }
}
impl ApiAdapter for Adapter {
    fn api_id(&self) -> &ApiId {
        &self.model.api
    }
    fn lifecycle_adapter(&self) -> Option<&dyn RequestLifecycleAdapter> {
        self.queries.fetch_add(1, Ordering::SeqCst);
        (!self.config.no_lifecycle).then_some(self)
    }
    fn stream(&self, _: TextStreamRequest) -> AssistantStream {
        panic!("not text streaming")
    }
    fn stream_simple(&self, _: SimpleStreamRequest) -> AssistantStream {
        panic!("not simple streaming")
    }
    fn fetch_deferred(&self, r: DeferredFetchRequest) -> Result<AssistantStream, CapabilityError> {
        self.ordinary.fetch_add(1, Ordering::SeqCst);
        self.start(r, None)
    }
    fn cancel_deferred<'a>(
        &'a self,
        r: DeferredCancelRequest,
    ) -> BoxFuture<'a, Result<(), ModelsError>> {
        self.cancels.fetch_add(1, Ordering::SeqCst);
        self.seen.lock().unwrap().push(Seen {
            endpoint: r.resolved.endpoint.expose_for_transport().into(),
            auth_present: r.auth.api_key.is_some(),
            headers: r.auth.headers.clone(),
            handle: r.handle,
        });
        Box::pin(async move {
            if self.config.cancel_wait {
                std::future::pending::<()>().await;
            }
            if self.config.cancel_error {
                Err(ModelsError::new(ModelsErrorCode::Provider))
            } else {
                Ok(())
            }
        })
    }
}
struct Auth {
    calls: Arc<AtomicUsize>,
    wait: bool,
}
impl ApiKeyAuth for Auth {
    fn name(&self) -> &str {
        "deferred-fixture"
    }
    fn resolve<'a>(
        &'a self,
        input: ApiKeyAuthInput<'a>,
    ) -> BoxFuture<'a, Result<Option<AuthResult>, AuthProviderError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            if self.wait {
                input.cancellation.cancelled().await;
            }
            Ok(Some(AuthResult {
                auth: ModelAuth {
                    api_key: Some(SecretString::new("fixture-only")),
                    headers: None,
                    base_url: None,
                },
                env: None,
                source: None,
            }))
        })
    }
}
struct Product {
    adapter: Arc<Adapter>,
    auth: ProviderAuth,
    getters: AtomicUsize,
}
impl Provider for Product {
    fn id(&self) -> &ProviderId {
        &self.adapter.model.provider
    }
    fn name(&self) -> &str {
        "deferred-fixture"
    }
    fn models(&self) -> Result<Arc<[Model]>, ModelsError> {
        self.getters.fetch_add(1, Ordering::SeqCst);
        Ok(vec![self.adapter.model.clone()].into())
    }
    fn auth(&self) -> &ProviderAuth {
        self.getters.fetch_add(1, Ordering::SeqCst);
        &self.auth
    }
    fn adapter(&self, _: &ApiId) -> Option<Arc<dyn ApiAdapter>> {
        self.getters.fetch_add(1, Ordering::SeqCst);
        Some(self.adapter.clone())
    }
}
fn product(config: Config) -> (Arc<Product>, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let wait = config.auth_wait;
    (
        Arc::new(Product {
            adapter: Arc::new(Adapter {
                model: model(),
                config,
                fetches: AtomicUsize::new(0),
                ordinary: AtomicUsize::new(0),
                queries: AtomicUsize::new(0),
                cancels: AtomicUsize::new(0),
                payloads: Arc::new(Mutex::new(vec![])),
                seen: Mutex::new(vec![]),
                held: Mutex::new(None),
            }),
            auth: ProviderAuth::new(
                Some(Arc::new(Auth {
                    calls: calls.clone(),
                    wait,
                })),
                None,
            )
            .unwrap(),
            getters: AtomicUsize::new(0),
        }),
        calls,
    )
}
fn registry(e: Arc<Executor>) -> ModelsRegistry {
    ModelsRegistry::new_with_child_cancellation(ModelsRegistryOptions {
        credentials: Arc::new(InMemoryCredentialStore::new()),
        models_store: Box::new(InMemoryModelsStore::new()),
        auth_context: Arc::new(Host),
        clock: Arc::new(TestClock),
        spawner: e,
        model_header_policy: Default::default(),
    })
}
fn capture(r: &ModelsRegistry) -> CapturedTextDispatch {
    r.capture_text_dispatch(
        &r.provider_registration(&model().provider).unwrap(),
        model(),
    )
    .unwrap()
}
fn fetch(
    r: &ModelsRegistry,
    e: Arc<dyn TaskSpawner>,
    token: CancellationToken,
) -> ObservedAssistantStream {
    r.fetch_deferred_captured_with_lifecycle(
        capture(r),
        handle(),
        Default::default(),
        fetch_options(e, token),
        Default::default(),
    )
    .unwrap()
}

#[test]
fn captured_fetch_retains_old_provider_while_ordinary_selects_replacement() {
    let e = Arc::new(Executor::default());
    let r = registry(e.clone());
    let (old, auth) = product(Default::default());
    e.drive(r.set_provider(old.clone())).unwrap();
    let getters = old.getters.load(Ordering::SeqCst);
    let captured = capture(&r);
    assert_eq!(old.getters.load(Ordering::SeqCst), getters);
    assert_eq!(auth.load(Ordering::SeqCst), 0);
    let dispatch = Arc::new(Executor::default());
    let observed = r
        .fetch_deferred_captured_with_lifecycle(
            captured,
            handle(),
            Default::default(),
            fetch_options(dispatch.clone(), CancellationToken::new()),
            Default::default(),
        )
        .unwrap();
    let ordinary = r.fetch_deferred(
        model(),
        handle(),
        Default::default(),
        fetch_options(dispatch.clone(), CancellationToken::new()),
    );
    let (new, _) = product(Default::default());
    e.drive(r.set_provider(new.clone())).unwrap();
    assert_eq!(
        dispatch.drive(observed.stream.result()).usage.input.get(),
        13
    );
    assert_eq!(dispatch.drive(ordinary).stop_reason, StopReason::Stop);
    assert_eq!(old.adapter.fetches.load(Ordering::SeqCst), 1);
    assert_eq!(new.adapter.fetches.load(Ordering::SeqCst), 1);
    assert_eq!(old.adapter.ordinary.load(Ordering::SeqCst), 0);
    assert_eq!(new.adapter.ordinary.load(Ordering::SeqCst), 1);
}
#[test]
fn capture_errors_and_foreign_consumers_do_not_dispatch() {
    let e = Arc::new(Executor::default());
    let r = registry(e.clone());
    let foreign = registry(e.clone());
    let (p, _) = product(Default::default());
    e.drive(r.set_provider(p.clone())).unwrap();
    let registration = r.provider_registration(&model().provider).unwrap();
    assert_eq!(
        foreign
            .fetch_deferred_captured_with_lifecycle(
                capture(&r),
                handle(),
                Default::default(),
                fetch_options(e.clone(), CancellationToken::new()),
                Default::default()
            )
            .unwrap_err(),
        CaptureDispatchError::WrongRegistry
    );
    assert!(matches!(
        foreign.cancel_deferred_captured(
            capture(&r),
            handle(),
            Default::default(),
            cancel_options(e.clone(), CancellationToken::new())
        ),
        Err(CaptureDispatchError::WrongRegistry)
    ));
    let mut wrong = model();
    wrong.provider = ProviderId::new("other").unwrap();
    assert_eq!(
        r.capture_text_dispatch(&registration, wrong).unwrap_err(),
        CaptureDispatchError::ModelProviderMismatch
    );
    assert_eq!(
        foreign
            .capture_text_dispatch(&registration, model())
            .unwrap_err(),
        CaptureDispatchError::WrongRegistry
    );
    e.drive(r.set_provider(p.clone())).unwrap();
    assert_eq!(
        r.capture_text_dispatch(&registration, model()).unwrap_err(),
        CaptureDispatchError::ReplacedRegistration
    );
    let current = r.provider_registration(&model().provider).unwrap();
    e.drive(r.delete_provider(&model().provider)).unwrap();
    assert_eq!(
        r.capture_text_dispatch(&current, model()).unwrap_err(),
        CaptureDispatchError::MissingRegistration
    );
    assert_eq!(p.adapter.fetches.load(Ordering::SeqCst), 0);
    assert_eq!(p.adapter.cancels.load(Ordering::SeqCst), 0);
}
struct Replace {
    value: u64,
    calls: Arc<AtomicUsize>,
    fail: bool,
}
impl RequestPayloadTransform for Replace {
    fn transform<'a>(
        &'a self,
        _: &'a Model,
        payload: &'a mut serde_json::Value,
    ) -> BoxFuture<'a, Result<Option<serde_json::Value>, ModelsError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            *payload = serde_json::json!({"discarded":true});
            if self.fail {
                Err(ModelsError::new(ModelsErrorCode::Provider))
            } else {
                Ok(Some(serde_json::json!({"call":self.value})))
            }
        })
    }
}
#[test]
fn actual_payload_replacement_and_metadata_are_invocation_isolated() {
    let e = Arc::new(Executor::default());
    let r = registry(e.clone());
    let (p, _) = product(Default::default());
    e.drive(r.set_provider(p.clone())).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut streams = vec![];
    for value in [1, 2] {
        streams.push(
            r.fetch_deferred_captured_with_lifecycle(
                capture(&r),
                handle(),
                Default::default(),
                fetch_options(e.clone(), CancellationToken::new()),
                RequestLifecycleOptions {
                    payload: Some(Arc::new(Replace {
                        value,
                        calls: calls.clone(),
                        fail: false,
                    })),
                },
            )
            .unwrap(),
        );
    }
    assert!(streams.iter().all(|s| s.metadata.snapshot().is_none()));
    for (index, s) in streams.iter().enumerate() {
        assert_eq!(e.drive(s.stream.result()).stop_reason, StopReason::Stop);
        let head = s.metadata.snapshot().unwrap();
        assert_eq!(head.status, 207);
        assert!(
            matches!(&head.headers[0].1,RequestResponseHeaderValue::Text(v) if v==&serde_json::json!({"call":index+1}).to_string())
        );
    }
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        *p.adapter.payloads.lock().unwrap(),
        vec![serde_json::json!({"call":1}), serde_json::json!({"call":2})]
    );
}
#[test]
fn unsupported_lifecycle_never_falls_back_even_without_payload() {
    for config in [
        Config {
            no_lifecycle: true,
            ..Default::default()
        },
        Config {
            no_deferred_lifecycle: true,
            ..Default::default()
        },
        Config {
            unsupported: true,
            ..Default::default()
        },
    ] {
        let e = Arc::new(Executor::default());
        let r = registry(e.clone());
        let (p, _) = product(config);
        e.drive(r.set_provider(p.clone())).unwrap();
        let observed = fetch(&r, e.clone(), CancellationToken::new());
        assert_eq!(
            e.drive(observed.stream.result()).stop_reason,
            StopReason::Error
        );
        assert!(observed.metadata.snapshot().is_none());
        assert_eq!(p.adapter.ordinary.load(Ordering::SeqCst), 0);
        assert_eq!(p.adapter.fetches.load(Ordering::SeqCst), 0);
    }
}
#[test]
fn captured_child_cancellation_preserves_usage_in_both_handoff_orders() {
    for mode in [Mode::Held, Mode::CancelAtHandoff] {
        let e = Arc::new(Executor::default());
        let r = registry(e.clone());
        let (p, _) = product(Config {
            mode,
            ..Default::default()
        });
        e.drive(r.set_provider(p.clone())).unwrap();
        let token = CancellationToken::new();
        let observed = fetch(&r, e.clone(), token.clone());
        e.tick();
        assert_eq!(p.adapter.fetches.load(Ordering::SeqCst), 1);
        token.cancel();
        let result = e.drive(observed.stream.result());
        assert_eq!(result.stop_reason, StopReason::Aborted);
        assert_eq!(result.usage.input.get(), 17);
        let mut stream = observed.stream;
        let mut terminals = 0;
        while let Some(event) = e.drive(stream.next()) {
            if matches!(event, AssistantMessageEvent::Error { .. }) {
                terminals += 1;
            }
        }
        assert_eq!(terminals, 1);
    }
}
#[test]
fn captured_cancel_retains_deleted_provider_and_ordinary_selects_new_auth() {
    let e = Arc::new(Executor::default());
    let r = registry(e.clone());
    let (old, old_auth) = product(Default::default());
    e.drive(r.set_provider(old.clone())).unwrap();
    let token = CancellationToken::new();
    let mut m = model();
    m.api = ApiId::new("azure-openai-responses").unwrap();
    m.base_url = ModelBaseUrl::RuntimeResolved;
    let captured = r
        .capture_text_dispatch(&r.provider_registration(&m.provider).unwrap(), m)
        .unwrap();
    let headers: ProviderHeaders =
        serde_json::from_value(serde_json::json!({"x-fixture":"captured"})).unwrap();
    let captured = r
        .cancel_deferred_captured(
            captured,
            handle(),
            ModelsRequestOverrides {
                headers: headers.clone(),
                runtime_endpoint: Some(
                    RequestEndpoint::new_resolved("https://fixture.invalid/cancel").unwrap(),
                ),
                ..Default::default()
            },
            cancel_options(e.clone(), token),
        )
        .unwrap();
    let ordinary = r.cancel_deferred(
        model(),
        handle(),
        Default::default(),
        cancel_options(e.clone(), CancellationToken::new()),
    );
    e.drive(r.delete_provider(&model().provider)).unwrap();
    let (new, new_auth) = product(Default::default());
    e.drive(r.set_provider(new.clone())).unwrap();
    e.drive(captured).unwrap();
    e.drive(ordinary).unwrap();
    assert_eq!(old.adapter.cancels.load(Ordering::SeqCst), 1);
    assert_eq!(new.adapter.cancels.load(Ordering::SeqCst), 1);
    assert_eq!(old_auth.load(Ordering::SeqCst), 1);
    assert_eq!(new_auth.load(Ordering::SeqCst), 1);
    let seen = old.adapter.seen.lock().unwrap();
    assert_eq!(seen[0].endpoint, "https://fixture.invalid/cancel");
    assert!(seen[0].auth_present);
    assert_eq!(seen[0].headers, headers);
    assert_eq!(seen[0].handle, handle());
}
#[test]
fn cancel_provider_error_and_cancellation_preserve_existing_channels() {
    for (wait, error) in [(false, true), (true, false)] {
        let e = Arc::new(Executor::default());
        let r = registry(e.clone());
        let (p, _) = product(Config {
            cancel_wait: wait,
            cancel_error: error,
            ..Default::default()
        });
        e.drive(r.set_provider(p.clone())).unwrap();
        let token = CancellationToken::new();
        let work = r
            .cancel_deferred_captured(
                capture(&r),
                handle(),
                Default::default(),
                cancel_options(e.clone(), token.clone()),
            )
            .unwrap();
        let mut work = std::pin::pin!(work);
        if wait {
            assert!(
                work.as_mut()
                    .poll(&mut TaskContext::from_waker(Waker::noop()))
                    .is_pending()
            );
            token.cancel();
        }
        let result = e.drive(work);
        if error {
            assert!(matches!(result, Err(AuthResolutionError::Models(_))));
        } else {
            assert!(matches!(result, Err(AuthResolutionError::Cancelled)));
        }
    }
}
#[test]
fn rejected_spawn_and_cancelled_auth_never_reach_fetch_adapter() {
    for auth_wait in [false, true] {
        let e = Arc::new(Executor::default());
        let r = registry(e.clone());
        let (p, auth) = product(Config {
            auth_wait,
            ..Default::default()
        });
        e.drive(r.set_provider(p.clone())).unwrap();
        let dispatch = Arc::new(Executor::default());
        dispatch.reject.store(!auth_wait, Ordering::SeqCst);
        let token = CancellationToken::new();
        let observed = fetch(&r, dispatch.clone(), token.clone());
        if auth_wait {
            dispatch.tick();
            assert_eq!(auth.load(Ordering::SeqCst), 1);
            token.cancel();
        }
        assert_eq!(
            dispatch.drive(observed.stream.result()).stop_reason,
            if auth_wait {
                StopReason::Aborted
            } else {
                StopReason::Error
            }
        );
        assert_eq!(p.adapter.fetches.load(Ordering::SeqCst), 0);
    }
}
struct Inline;
impl TaskSpawner for Inline {
    fn spawn(&self, task: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
        assert!(
            std::pin::pin!(task)
                .as_mut()
                .poll(&mut TaskContext::from_waker(Waker::noop()))
                .is_ready()
        );
        Ok(())
    }
}
#[test]
fn inline_host_and_payload_failure_use_the_actual_stream_route() {
    let e = Arc::new(Executor::default());
    let r = registry(e.clone());
    let (p, _) = product(Default::default());
    e.drive(r.set_provider(p.clone())).unwrap();
    let good = fetch(&r, Arc::new(Inline), CancellationToken::new());
    assert_eq!(e.drive(good.stream.result()).usage.input.get(), 13);
    let calls = Arc::new(AtomicUsize::new(0));
    let bad = r
        .fetch_deferred_captured_with_lifecycle(
            capture(&r),
            handle(),
            Default::default(),
            fetch_options(Arc::new(Inline), CancellationToken::new()),
            RequestLifecycleOptions {
                payload: Some(Arc::new(Replace {
                    value: 0,
                    calls: calls.clone(),
                    fail: true,
                })),
            },
        )
        .unwrap();
    assert_eq!(e.drive(bad.stream.result()).stop_reason, StopReason::Error);
    assert!(bad.metadata.snapshot().is_none());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
