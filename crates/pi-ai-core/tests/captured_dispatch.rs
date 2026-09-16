//! Actual ModelsRegistry dispatch; fixture host copied from request_lifecycle tests.
mod captured_dispatch_support;
use captured_dispatch_support::*;
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
    let v: serde_json::Value = serde_json::from_str(include_str!(
        "../../../conformance/schema/ai/models-options.golden.json"
    ))
    .unwrap();
    serde_json::from_value(v["model"].clone()).unwrap()
}
struct Auth {
    calls: Arc<AtomicUsize>,
    wait: bool,
}
impl ApiKeyAuth for Auth {
    fn name(&self) -> &str {
        "captured-fixture"
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
                    api_key: None,
                    headers: None,
                    base_url: None,
                },
                env: None,
                source: None,
            }))
        })
    }
}
struct Adapter {
    model: Model,
    calls: AtomicUsize,
    lifecycle: bool,
    endpoint: Mutex<Option<String>>,
}
impl Adapter {
    fn start(&self, r: SimpleStreamRequest) -> AssistantStream {
        self.calls.fetch_add(1, Ordering::SeqCst);
        *self.endpoint.lock().unwrap() =
            Some(r.resolved.endpoint.expose_for_transport().to_owned());
        let (stream, producer) = create_assistant_message_event_stream(AssistantStreamInit {
            api: r.resolved.model.api.clone(),
            provider: r.resolved.model.provider.clone(),
            model: r.resolved.model.id.clone(),
            clock: r.options.runtime.clock,
            cancellation: r.options.runtime.cancellation,
        });
        let message = AssistantMessage {
            role: Default::default(),
            content: vec![],
            api: r.resolved.model.api,
            provider: r.resolved.model.provider,
            model: r.resolved.model.id,
            response_model: None,
            response_id: None,
            diagnostics: None,
            usage: Usage::zero(),
            stop_reason: StopReason::Stop,
            deferred: None,
            error_message: None,
            raw_stop_reason: None,
            end_turn: None,
            timestamp: SafeUnsigned::new(7).unwrap(),
        };
        producer
            .push(AssistantMessageEvent::Done {
                reason: DoneReason::Stop,
                message: Box::new(message),
            })
            .unwrap();
        stream
    }
}
impl RequestLifecycleAdapter for Adapter {
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
        l: RequestLifecycleInvocation,
    ) -> AssistantStream {
        l.recorder.record(RequestResponseHead {
            status: 200,
            headers: vec![],
        });
        self.start(r)
    }
}
impl ApiAdapter for Adapter {
    fn api_id(&self) -> &ApiId {
        &self.model.api
    }
    fn lifecycle_adapter(&self) -> Option<&dyn RequestLifecycleAdapter> {
        self.lifecycle.then_some(self)
    }
    fn stream(&self, r: TextStreamRequest) -> AssistantStream {
        self.start(SimpleStreamRequest {
            resolved: r.resolved,
            context: r.context,
            auth: r.auth,
            options: SimpleStreamOptions {
                runtime: r.options.runtime,
                wire: SimpleStreamOptionsWire {
                    stream: r.options.wire,
                    ..Default::default()
                },
            },
        })
    }
    fn stream_simple(&self, r: SimpleStreamRequest) -> AssistantStream {
        self.start(r)
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
struct Product {
    adapter: Arc<Adapter>,
    auth: ProviderAuth,
    getters: AtomicUsize,
    missing_api: bool,
    dynamic: Option<Dynamic>,
}
impl Provider for Product {
    fn id(&self) -> &ProviderId {
        &self.adapter.model.provider
    }
    fn name(&self) -> &str {
        "captured-fixture"
    }
    fn models(&self) -> Result<Arc<[Model]>, ModelsError> {
        self.getters.fetch_add(1, Ordering::SeqCst);
        Ok(vec![self.adapter.model.clone()].into())
    }
    fn auth(&self) -> &ProviderAuth {
        self.getters.fetch_add(1, Ordering::SeqCst);
        &self.auth
    }
    fn models_refresher(&self) -> Option<&dyn ProviderModelsRefresher> {
        self.dynamic
            .as_ref()
            .map(|value| value as &dyn ProviderModelsRefresher)
    }
    fn adapter(&self, _: &ApiId) -> Option<Arc<dyn ApiAdapter>> {
        self.getters.fetch_add(1, Ordering::SeqCst);
        (!self.missing_api).then(|| self.adapter.clone() as Arc<dyn ApiAdapter>)
    }
}
fn product(lifecycle: bool, missing_api: bool, wait: bool) -> (Arc<Product>, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    (
        Arc::new(Product {
            adapter: Arc::new(Adapter {
                model: model(),
                calls: AtomicUsize::new(0),
                lifecycle,
                endpoint: Mutex::new(None),
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
            missing_api,
            dynamic: None,
        }),
        calls,
    )
}
fn registry(executor: Arc<Executor>) -> ModelsRegistry {
    ModelsRegistry::new(ModelsRegistryOptions {
        credentials: Arc::new(InMemoryCredentialStore::new()),
        models_store: Box::new(InMemoryModelsStore::new()),
        auth_context: Arc::new(Host),
        clock: Arc::new(TestClock),
        spawner: executor,
        model_header_policy: Default::default(),
    })
}
fn context() -> Context {
    serde_json::from_value(serde_json::json!({"messages":[]})).unwrap()
}
fn options(executor: Arc<dyn TaskSpawner>, token: CancellationToken) -> SimpleStreamOptions {
    SimpleStreamOptions {
        wire: Default::default(),
        runtime: RequestRuntime::with_noop_telemetry(token, Arc::new(TestClock), executor),
    }
}
#[test]
fn capture_retains_old_record_but_ordinary_stream_still_selects_at_task_start() {
    let executor = Arc::new(Executor::default());
    let registry = registry(executor.clone());
    let (old, auth) = product(true, false, false);
    executor.drive(registry.set_provider(old.clone())).unwrap();
    let token = registry
        .provider_registration(&old.adapter.model.provider)
        .unwrap();
    let getters = old.getters.load(Ordering::SeqCst);
    let captured = registry
        .capture_text_dispatch(&token, old.adapter.model.clone())
        .unwrap();
    assert_eq!(old.getters.load(Ordering::SeqCst), getters);
    assert_eq!(auth.load(Ordering::SeqCst), 0);
    let dispatch_executor = Arc::new(Executor::default());
    let captured = registry
        .stream_simple_captured_with_lifecycle(
            captured,
            context(),
            Default::default(),
            options(dispatch_executor.clone(), CancellationToken::new()),
            Default::default(),
        )
        .unwrap();
    let ordinary = registry.stream_simple_with_lifecycle(
        old.adapter.model.clone(),
        context(),
        Default::default(),
        options(dispatch_executor.clone(), CancellationToken::new()),
        Default::default(),
    );
    let (new, _) = product(true, false, false);
    executor.drive(registry.set_provider(new.clone())).unwrap();
    assert_eq!(old.adapter.calls.load(Ordering::SeqCst), 0);
    assert_eq!(new.adapter.calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        dispatch_executor
            .drive(captured.stream.result())
            .stop_reason,
        StopReason::Stop
    );
    assert_eq!(
        dispatch_executor
            .drive(ordinary.stream.result())
            .stop_reason,
        StopReason::Stop
    );
    assert_eq!(old.adapter.calls.load(Ordering::SeqCst), 1);
    assert_eq!(new.adapter.calls.load(Ordering::SeqCst), 1);
    assert_eq!(captured.metadata.snapshot().unwrap().status, 200);
}
#[test]
fn registration_errors_detect_same_arc_aba_missing_foreign_and_model_pairing() {
    let e = Arc::new(Executor::default());
    let r = registry(e.clone());
    let (other, _) = product(true, false, false);
    assert!(
        r.provider_registration(&other.adapter.model.provider)
            .is_none()
    );
    e.drive(r.set_provider(other.clone())).unwrap();
    let token = r
        .provider_registration(&other.adapter.model.provider)
        .unwrap();
    let foreign = registry(e.clone());
    e.drive(foreign.set_provider(other.clone())).unwrap();
    assert!(
        !token.same_registration(
            &foreign
                .provider_registration(&other.adapter.model.provider)
                .unwrap()
        )
    );
    assert_eq!(
        foreign.capture_text_dispatch(&token, model()).unwrap_err(),
        CaptureDispatchError::WrongRegistry
    );
    let mut wrong = model();
    wrong.provider = ProviderId::new("different").unwrap();
    assert_eq!(
        r.capture_text_dispatch(&token, wrong).unwrap_err(),
        CaptureDispatchError::ModelProviderMismatch
    );
    let capture = r.capture_text_dispatch(&token, model()).unwrap();
    assert_eq!(
        foreign
            .stream_simple_captured_with_lifecycle(
                capture,
                context(),
                Default::default(),
                options(e.clone(), CancellationToken::new()),
                Default::default()
            )
            .unwrap_err(),
        CaptureDispatchError::WrongRegistry
    );
    e.drive(r.set_provider(other.clone())).unwrap();
    assert_eq!(
        r.capture_text_dispatch(&token, model()).unwrap_err(),
        CaptureDispatchError::ReplacedRegistration
    );
    let current = r
        .provider_registration(&other.adapter.model.provider)
        .unwrap();
    assert!(!token.same_registration(&current));
    e.drive(r.delete_provider(&other.adapter.model.provider))
        .unwrap();
    assert_eq!(
        r.capture_text_dispatch(&current, model()).unwrap_err(),
        CaptureDispatchError::MissingRegistration
    );
    assert_eq!(other.adapter.calls.load(Ordering::SeqCst), 0);
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
fn captured_dispatch_supports_inline_host_polling_and_runtime_endpoint_model() {
    let e = Arc::new(Executor::default());
    let r = registry(e.clone());
    let (p, _) = product(true, false, false);
    e.drive(r.set_provider(p.clone())).unwrap();
    let token = r.provider_registration(&p.adapter.model.provider).unwrap();
    let mut m = model();
    m.api = ApiId::new("azure-openai-responses").unwrap();
    m.base_url = ModelBaseUrl::RuntimeResolved;
    let captured = r.capture_text_dispatch(&token, m).unwrap();
    let overrides = ModelsRequestOverrides {
        runtime_endpoint: Some(
            RequestEndpoint::new_resolved("https://captured.invalid/runtime").unwrap(),
        ),
        ..Default::default()
    };
    let stream = r
        .stream_simple_captured_with_lifecycle(
            captured,
            context(),
            overrides,
            options(Arc::new(Inline), CancellationToken::new()),
            Default::default(),
        )
        .unwrap();
    assert_eq!(
        e.drive(stream.stream.result()).stop_reason,
        StopReason::Stop
    );
    assert_eq!(
        p.adapter.endpoint.lock().unwrap().as_deref(),
        Some("https://captured.invalid/runtime")
    );
    assert_eq!(p.adapter.calls.load(Ordering::SeqCst), 1);
}
#[test]
fn captured_setup_failures_and_cancellation_do_not_fall_back() {
    for (lifecycle, missing_api) in [(false, false), (true, true)] {
        let e = Arc::new(Executor::default());
        let r = registry(e.clone());
        let (p, _) = product(lifecycle, missing_api, false);
        e.drive(r.set_provider(p.clone())).unwrap();
        let token = r.provider_registration(&p.adapter.model.provider).unwrap();
        let c = r.capture_text_dispatch(&token, model()).unwrap();
        let stream = r
            .stream_simple_captured_with_lifecycle(
                c,
                context(),
                Default::default(),
                options(e.clone(), CancellationToken::new()),
                Default::default(),
            )
            .unwrap();
        assert_eq!(
            e.drive(stream.stream.result()).stop_reason,
            StopReason::Error
        );
        assert_eq!(p.adapter.calls.load(Ordering::SeqCst), 0);
    }
    let e = Arc::new(Executor::default());
    let r = registry(e.clone());
    let (p, auth) = product(true, false, true);
    e.drive(r.set_provider(p.clone())).unwrap();
    let token = r.provider_registration(&p.adapter.model.provider).unwrap();
    let c = r.capture_text_dispatch(&token, model()).unwrap();
    let cancellation = CancellationToken::new();
    let stream = r
        .stream_simple_captured_with_lifecycle(
            c,
            context(),
            Default::default(),
            options(e.clone(), cancellation.clone()),
            Default::default(),
        )
        .unwrap(); // Auth is deliberately pending until cancellation.
    e.tick();
    assert_eq!(auth.load(Ordering::SeqCst), 1);
    cancellation.cancel();
    assert_eq!(
        e.drive(stream.stream.result()).stop_reason,
        StopReason::Aborted
    );
    assert_eq!(p.adapter.calls.load(Ordering::SeqCst), 0);
}

struct Dynamic(Model);
impl ProviderModelsRefresher for Dynamic {
    fn refresh_models<'a>(
        &'a self,
        context: RefreshModelsContext<'a>,
    ) -> BoxFuture<'a, Result<(), ModelsError>> {
        Box::pin(async move {
            context
                .publisher
                .publish(ModelsPublication {
                    memory: ModelsMemoryIntent::Set(vec![self.0.clone()].into()),
                    persistence: ModelsPersistenceIntent::Keep,
                })
                .await?;
            Ok(())
        })
    }
}
#[test]
fn captured_binding_uses_the_full_registry_published_dynamic_model() {
    let e = Arc::new(Executor::default());
    let r = registry(e.clone());
    let (mut p, _) = product(true, false, false);
    let mut dynamic = model();
    dynamic.id = ModelId::new("dynamic-model").unwrap();
    dynamic.max_tokens = SafeUnsigned::new(123).unwrap();
    Arc::get_mut(&mut p).unwrap().dynamic = Some(Dynamic(dynamic.clone()));
    e.drive(r.set_provider(p.clone())).unwrap();
    let result = e.drive(r.refresh(
        ModelsRefreshOptions {
            allow_network: false,
            ..Default::default()
        },
        &CancellationToken::new(),
    ));
    assert!(result.errors.is_empty());
    let observed = r.get_model(&dynamic.provider, &dynamic.id).unwrap();
    assert_eq!(observed, dynamic);
    let token = r.provider_registration(&dynamic.provider).unwrap();
    let captured = r.capture_text_dispatch(&token, observed).unwrap();
    assert_eq!(captured.model(), &dynamic);
    let result = r
        .stream_simple_captured_with_lifecycle(
            captured,
            context(),
            Default::default(),
            options(e.clone(), CancellationToken::new()),
            Default::default(),
        )
        .unwrap();
    assert_eq!(e.drive(result.stream.result()).model, dynamic.id);
    assert_eq!(p.adapter.calls.load(Ordering::SeqCst), 1);
}
