use pi_ai_core::*;
use pi_ai_providers::images::*;
use serde_json::{Value, json};
use std::{
    future::{Future, pending},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
    time::Duration,
};
fn ready<T>(future: impl Future<Output = T>) -> T {
    match Box::pin(future)
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(v) => v,
        Poll::Pending => panic!("unexpected pending"),
    }
}
fn poll<T>(future: &mut BoxFuture<'_, T>) -> Poll<T> {
    future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
}
fn fixture() -> Value {
    serde_json::from_str(include_str!(
        "../../../conformance/provider-traces/images-runtime/trace.json"
    ))
    .unwrap()
}
struct Host(AtomicU64);
impl Host {
    fn new() -> Arc<Self> {
        Arc::new(Self(AtomicU64::new(1700000000000)))
    }
}
impl Clock for Host {
    fn now_unix_ms(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
    fn sleep<'a>(
        &'a self,
        _: Duration,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), Cancelled>> {
        Box::pin(pending())
    }
}
impl TaskSpawner for Host {
    fn spawn(&self, _: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
        panic!("no runtime spawn")
    }
}
#[derive(Default)]
struct Queue(Mutex<Vec<BoxFuture<'static, ()>>>);
impl TaskSpawner for Queue {
    fn spawn(&self, task: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
        self.0.lock().unwrap().push(task);
        Ok(())
    }
}
impl Queue {
    fn take(&self) -> BoxFuture<'static, ()> {
        self.0.lock().unwrap().remove(0)
    }
}
#[derive(Default)]
struct Seen {
    events: Mutex<Vec<String>>,
    calls: Mutex<Vec<Value>>,
}
struct Store {
    inner: InMemoryCredentialStore,
    seen: Arc<Seen>,
}
impl CredentialStore for Store {
    fn read<'a>(
        &'a self,
        id: &'a ProviderId,
        c: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<VersionedCredential>, CredentialStoreError>> {
        assert_eq!(id.as_str(), "openrouter");
        self.seen.events.lock().unwrap().push("read".into());
        self.inner.read(id, c)
    }
    fn list<'a>(
        &'a self,
        c: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Vec<CredentialInfo>, CredentialStoreError>> {
        self.inner.list(c)
    }
    fn begin_mutation<'a>(
        &'a self,
        id: &'a ProviderId,
        c: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Box<dyn CredentialMutation>, CredentialStoreError>> {
        self.inner.begin_mutation(id, c)
    }
    fn reconcile<'a>(
        &'a self,
        id: &'a ProviderId,
        r: CredentialReconciliation,
        c: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<VersionedCredential>, CredentialStoreError>> {
        self.inner.reconcile(id, r, c)
    }
}
struct NoContext;
impl AuthContext for NoContext {
    fn env<'a>(
        &'a self,
        _: &'a str,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<String>, AuthContextError>> {
        Box::pin(async { Ok(None) })
    }
    fn file_exists<'a>(
        &'a self,
        _: &'a str,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<bool, AuthContextError>> {
        Box::pin(async { Ok(false) })
    }
}
struct Auth {
    config: Value,
    seen: Arc<Seen>,
    gate: Arc<AtomicBool>,
}
impl ApiKeyAuth for Auth {
    fn name(&self) -> &str {
        "Synthetic"
    }
    fn resolve<'a>(
        &'a self,
        _: ApiKeyAuthInput<'a>,
    ) -> BoxFuture<'a, Result<Option<AuthResult>, AuthProviderError>> {
        Box::pin(async move {
            self.seen.events.lock().unwrap().push("auth".into());
            if self.config["authPending"] == true {
                std::future::poll_fn(|_| {
                    if self.gate.load(Ordering::SeqCst) {
                        Poll::Ready(())
                    } else {
                        Poll::Pending
                    }
                })
                .await;
            }
            if self.config["authFail"] == true {
                return Err(AuthProviderError::new());
            }
            if self.config["emptyAuth"] == true {
                return Ok(Some(AuthResult {
                    auth: ModelAuth::default(),
                    env: None,
                    source: None,
                }));
            }
            Ok((self.config["auth"]==true).then(||serde_json::from_value(json!({"auth":{"apiKey":"SYNTHETIC_RESOLVED","baseUrl":if self.config["emptyEndpoint"]==true{""}else{"https://credential.invalid/api?synthetic=1"},"headers":{"X-Shared":"auth","X-Keep":"kept","X-Null":"auth"}},"source":"API key","env":{"A":"auth","B":"kept"}})).unwrap()))
        })
    }
}
struct Generator {
    config: Value,
    seen: Arc<Seen>,
    gate: Arc<AtomicBool>,
    label: String,
}
fn output(request: &ImageGenerationRequest, label: &str) -> AssistantImages {
    AssistantImages {
        api: request.resolved.model.api.clone(),
        provider: request.resolved.model.provider.clone(),
        model: request.resolved.model.id.clone(),
        output: vec![InputContent::Text(TextContent {
            content_type: TextContentType::Text,
            text: label.into(),
            text_signature: None,
        })],
        response_id: None,
        usage: None,
        stop_reason: ImagesStopReason::Stop,
        error_message: None,
        timestamp: SafeUnsigned::new(request.options.runtime.clock.now_unix_ms()).unwrap(),
    }
}
impl ImagesGenerator for Generator {
    fn generate<'a>(&'a self, request: ImageGenerationRequest) -> ImagesGenerateCall<'a> {
        self.seen.events.lock().unwrap().push("generate".into());
        assert_eq!(
            request.resolved.model.base_url.as_str(),
            "https://public.invalid/api"
        );
        self.seen.calls.lock().unwrap().push(json!({"endpoint":request.resolved.endpoint.expose_for_transport(),"headers":request.auth.headers.iter_for_dispatch().collect::<Vec<_>>(),"env":request.auth.env,"key":request.auth.api_key.as_ref().map(|v|v.expose_secret()),"context":request.context}));
        if self.config["syncThrow"] == true {
            return Err(ModelsError::new(ModelsErrorCode::Stream));
        }
        Ok(Box::pin(async move {
            if self.config["generationPending"] == true {
                std::future::poll_fn(|_| {
                    if self.gate.load(Ordering::SeqCst) {
                        Poll::Ready(())
                    } else {
                        Poll::Pending
                    }
                })
                .await;
            }
            if self.config["reject"] == true {
                return Err(ModelsError::new(ModelsErrorCode::Stream));
            }
            Ok(output(&request, &self.label))
        }))
    }
}
fn auth_none() -> ProviderAuth {
    ProviderAuth::new(
        Some(Arc::new(Auth {
            config: json!({}),
            seen: Arc::new(Seen::default()),
            gate: Arc::new(AtomicBool::new(false)),
        })),
        None,
    )
    .unwrap()
}
fn model() -> ImagesModel {
    serde_json::from_value(fixture()["model"].clone()).unwrap()
}
fn runtime(host: Arc<Host>) -> ImagesModels {
    create_images_models(ImagesModelsOptions {
        credentials: None,
        auth_context: Arc::new(NoContext),
        clock: host,
    })
}
fn generator(label: &str) -> Arc<dyn ImagesGenerator> {
    Arc::new(Generator {
        config: json!({}),
        seen: Arc::new(Seen::default()),
        gate: Arc::new(AtomicBool::new(false)),
        label: label.into(),
    })
}
fn provider(id: &str, models: Vec<ImagesModel>, name: Option<String>) -> Arc<dyn ImagesProvider> {
    create_images_provider(CreateImagesProviderOptions {
        id: ImagesProviderId::new(id).unwrap(),
        name,
        auth: auth_none(),
        models: models.into(),
        api: generator(id),
        refresh: None,
    })
}
fn request(host: Arc<Host>) -> ImageGenerationRequest {
    let model = model();
    ImageGenerationRequest {
        resolved: ResolvedRequest {
            endpoint: RequestEndpoint::new_resolved(model.base_url.as_str()).unwrap(),
            model,
        },
        context: serde_json::from_value(fixture()["context"].clone()).unwrap(),
        auth: ResolvedAuth::default(),
        options: ImagesOptions {
            wire: ImagesOptionsWire::default(),
            runtime: RequestRuntime::with_noop_telemetry(
                CancellationToken::new(),
                host.clone(),
                host,
            ),
        },
    }
}
#[test]
fn complete_source_generation_cases_preserve_auth_branch_rejection_and_exact_merge() {
    let source = fixture();
    assert_eq!(source["generations"].as_array().unwrap().len(), 16);
    for case in source["generations"].as_array().unwrap() {
        let config = &case["config"];
        let host = Host::new();
        let seen = Arc::new(Seen::default());
        let gate = Arc::new(AtomicBool::new(false));
        let runtime = create_images_models(ImagesModelsOptions {
            credentials: Some(Arc::new(Store {
                inner: InMemoryCredentialStore::new(),
                seen: seen.clone(),
            })),
            auth_context: Arc::new(NoContext),
            clock: host.clone(),
        });
        if config["unknown"] != true {
            runtime.set_provider(create_images_provider(CreateImagesProviderOptions {
                id: ImagesProviderId::new("openrouter").unwrap(),
                name: None,
                auth: ProviderAuth::new(
                    Some(Arc::new(Auth {
                        config: config.clone(),
                        seen: seen.clone(),
                        gate: gate.clone(),
                    })),
                    None,
                )
                .unwrap(),
                models: vec![model()].into(),
                api: Arc::new(Generator {
                    config: config.clone(),
                    seen: seen.clone(),
                    gate: gate.clone(),
                    label: "ok".into(),
                }),
                refresh: None,
            }));
        }
        let request = request(host.clone());
        let token = request.options.runtime.cancellation.clone();
        let overrides = ImagesRequestOverrides {
            api_key: config["options"]
                .get("apiKey")
                .map(|v| SecretString::new(v.as_str().unwrap())),
            env: config["options"]
                .get("env")
                .map(|v| serde_json::from_value(v.clone()).unwrap()),
            headers: config["options"]
                .get("headers")
                .map(|v| serde_json::from_value(v.clone()).unwrap())
                .unwrap_or_default(),
        };
        let mut future = runtime.generate_images(
            request.resolved.model,
            request.context,
            request.options,
            overrides,
        );
        if config["authPending"] == true || config["generationPending"] == true {
            assert!(poll(&mut future).is_pending());
            let required = if config["authPending"] == true {
                "auth"
            } else {
                "generate"
            };
            assert!(
                seen.events.lock().unwrap().iter().any(|v| v == required),
                "branch must be admitted before cancellation"
            );
            host.0.fetch_add(17, Ordering::SeqCst);
            token.cancel();
            if config["generationPending"] == true {
                gate.store(true, Ordering::SeqCst);
            }
        }
        match ready(future) {
            Ok(output) => {
                assert_eq!(case["outcome"]["kind"], "resolved");
                let mut actual = serde_json::to_value(output).unwrap();
                let mut expected = case["outcome"]["value"].clone();
                assert_eq!(
                    actual.get("errorMessage").is_some(),
                    expected.get("errorMessage").is_some()
                );
                actual.as_object_mut().unwrap().remove("errorMessage");
                expected.as_object_mut().unwrap().remove("errorMessage");
                assert_eq!(actual, expected, "{} result", config["id"]);
            }
            Err(error) => {
                assert_eq!(case["outcome"]["kind"], "rejected");
                assert_eq!(error.code(), ModelsErrorCode::Stream);
                assert_eq!(case["outcome"]["message"], "SYNTHETIC_ASYNC_ERROR");
            }
        }
        assert_eq!(
            json!(*seen.events.lock().unwrap()),
            case["events"],
            "{} events",
            config["id"]
        );
        assert_eq!(
            json!(*seen.calls.lock().unwrap()),
            case["calls"],
            "{} calls",
            config["id"]
        );
    }
}
struct BrokenList(Arc<dyn ImagesProvider>);
impl ImagesProvider for BrokenList {
    fn id(&self) -> &ImagesProviderId {
        self.0.id()
    }
    fn name(&self) -> &str {
        self.0.name()
    }
    fn auth(&self) -> &ProviderAuth {
        self.0.auth()
    }
    fn models(&self) -> Result<Arc<[ImagesModel]>, ModelsError> {
        Err(ModelsError::new(ModelsErrorCode::ModelSource))
    }
    fn models_refresher(&self) -> Option<&(dyn ImagesModelsRefresher + '_)> {
        None
    }
    fn generate<'a>(&'a self, r: ImageGenerationRequest) -> ImagesGenerateCall<'a> {
        self.0.generate(r)
    }
}
#[test]
fn source_ordered_membership_best_effort_lists_and_first_match_remain_instance_owned() {
    let source = fixture();
    let runtime = runtime(Host::new());
    let mut first = model();
    first.id = ModelId::new("same").unwrap();
    first.name = "first".into();
    let mut second = first.clone();
    second.name = "second".into();
    let mut b = model();
    b.id = ModelId::new("b").unwrap();
    runtime.set_provider(provider("a", vec![first, second], None));
    runtime.set_provider(provider("b", vec![b], None));
    runtime.set_provider(Arc::new(BrokenList(provider("throw", vec![], None))));
    let ids = || {
        json!(
            runtime
                .get_providers()
                .iter()
                .map(|v| v.id().as_str())
                .collect::<Vec<_>>()
        )
    };
    assert_eq!(ids(), source["listing"]["initial"]);
    assert_eq!(
        json!(
            runtime
                .get_models(None)
                .iter()
                .map(|v| v.id.as_str())
                .collect::<Vec<_>>()
        ),
        source["listing"]["models"]
    );
    assert_eq!(
        runtime
            .get_model(
                &ImagesProviderId::new("a").unwrap(),
                &ModelId::new("same").unwrap()
            )
            .unwrap()
            .name,
        source["listing"]["first"]
    );
    assert!(
        runtime
            .get_models(Some(&ImagesProviderId::new("missing").unwrap()))
            .is_empty()
    );
    runtime.set_provider(provider("a", vec![model()], Some("replacement".into())));
    assert_eq!(ids(), source["listing"]["replaced"]);
    runtime.delete_provider(&ImagesProviderId::new("a").unwrap());
    runtime.set_provider(provider("a", vec![model()], None));
    assert_eq!(ids(), source["listing"]["reinserted"]);
    runtime.clear_providers();
    assert!(runtime.get_providers().is_empty());
}

struct Source {
    model_id: &'static str,
    calls: AtomicUsize,
    gate: Arc<AtomicBool>,
    fail: AtomicBool,
}
impl ImagesModelsSource for Source {
    fn fetch(self: Arc<Self>) -> ImagesModelsFetchCall {
        Ok(Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            std::future::poll_fn(|_| {
                if self.gate.load(Ordering::SeqCst) {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            })
            .await;
            if self.fail.load(Ordering::SeqCst) {
                return Err(ModelsError::new(ModelsErrorCode::ModelSource));
            }
            let mut model = model();
            model.id = ModelId::new(self.model_id).unwrap();
            Ok(vec![model].into())
        }))
    }
}
#[test]
fn shared_refresh_retains_old_models_on_failure_retries_and_outlives_dropped_waiters() {
    let oracle = fixture();
    let queue = Arc::new(Queue::default());
    let gate = Arc::new(AtomicBool::new(false));
    let source = Arc::new(Source {
        model_id: "fresh",
        calls: AtomicUsize::new(0),
        gate: gate.clone(),
        fail: AtomicBool::new(true),
    });
    let provider = create_images_provider(CreateImagesProviderOptions {
        id: ImagesProviderId::new("dynamic").unwrap(),
        name: None,
        auth: auth_none(),
        models: vec![model()].into(),
        api: generator("dynamic"),
        refresh: Some(ImagesRefreshConfig {
            source: source.clone(),
            spawner: queue.clone(),
        }),
    });
    let refresher = provider.models_refresher().unwrap();
    let mut first = refresher.refresh_models();
    let mut second = refresher.refresh_models();
    assert_eq!(queue.0.lock().unwrap().len(), 1);
    let mut task = queue.take();
    assert!(poll(&mut task).is_pending());
    assert!(poll(&mut first).is_pending());
    assert!(poll(&mut second).is_pending());
    assert_eq!(
        source.calls.load(Ordering::SeqCst),
        oracle["refresh"]["fetchesBefore"].as_u64().unwrap() as usize
    );
    let ids = || {
        json!(
            provider
                .models()
                .unwrap()
                .iter()
                .map(|m| m.id.as_str())
                .collect::<Vec<_>>()
        )
    };
    assert_eq!(ids(), oracle["refresh"]["oldWhilePending"]);
    gate.store(true, Ordering::SeqCst);
    ready(task);
    assert_eq!(
        ready(first).unwrap_err().code(),
        ModelsErrorCode::ModelSource
    );
    assert_eq!(
        ready(second).unwrap_err().code(),
        ModelsErrorCode::ModelSource
    );
    assert_eq!(ids(), oracle["refresh"]["oldAfterFailure"]);
    source.fail.store(false, Ordering::SeqCst);
    let next = refresher.refresh_models();
    ready(queue.take());
    ready(next).unwrap();
    assert_eq!(ids(), oracle["refresh"]["afterRetry"]);
    assert_eq!(
        source.calls.load(Ordering::SeqCst),
        oracle["refresh"]["fetches"].as_u64().unwrap() as usize
    );
    let dropped = refresher.refresh_models();
    drop(dropped);
    assert_eq!(queue.0.lock().unwrap().len(), 1);
    ready(queue.take());
    assert_eq!(source.calls.load(Ordering::SeqCst), 3);
}
struct CodeProvider {
    base: Arc<dyn ImagesProvider>,
    code: ModelsErrorCode,
    entered: Arc<AtomicUsize>,
    gate: Arc<AtomicBool>,
}
impl ImagesProvider for CodeProvider {
    fn id(&self) -> &ImagesProviderId {
        self.base.id()
    }
    fn name(&self) -> &str {
        self.base.name()
    }
    fn auth(&self) -> &ProviderAuth {
        self.base.auth()
    }
    fn models(&self) -> Result<Arc<[ImagesModel]>, ModelsError> {
        self.base.models()
    }
    fn models_refresher(&self) -> Option<&(dyn ImagesModelsRefresher + '_)> {
        Some(self)
    }
    fn generate<'a>(&'a self, r: ImageGenerationRequest) -> ImagesGenerateCall<'a> {
        self.base.generate(r)
    }
}
impl ImagesModelsRefresher for CodeProvider {
    fn refresh_models(&self) -> BoxFuture<'_, Result<(), ModelsError>> {
        Box::pin(async move {
            self.entered.fetch_add(1, Ordering::SeqCst);
            std::future::poll_fn(|_| {
                if self.gate.load(Ordering::SeqCst) {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            })
            .await;
            Err(ModelsError::new(self.code))
        })
    }
}
#[test]
fn all_provider_refresh_admits_every_future_and_settles_failures_best_effort() {
    let oracle = fixture();
    let runtime = runtime(Host::new());
    let entered = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(AtomicBool::new(true));
    for (id, code) in [
        ("coded", ModelsErrorCode::OAuth),
        ("plain", ModelsErrorCode::ModelSource),
    ] {
        runtime.set_provider(Arc::new(CodeProvider {
            base: provider(id, vec![], None),
            code,
            entered: entered.clone(),
            gate: gate.clone(),
        }));
    }
    runtime.set_provider(provider("static", vec![], None));
    assert_eq!(
        ready(runtime.refresh(Some(&ImagesProviderId::new("coded").unwrap())))
            .unwrap_err()
            .code(),
        ModelsErrorCode::OAuth
    );
    assert_eq!(oracle["refresh"]["codedCode"], "oauth");
    assert_eq!(
        ready(runtime.refresh(Some(&ImagesProviderId::new("plain").unwrap())))
            .unwrap_err()
            .code(),
        ModelsErrorCode::ModelSource
    );
    entered.store(0, Ordering::SeqCst);
    gate.store(false, Ordering::SeqCst);
    let mut all = runtime.refresh(None);
    assert!(poll(&mut all).is_pending());
    assert_eq!(
        entered.load(Ordering::SeqCst),
        2,
        "all sources entered before either completes"
    );
    gate.store(true, Ordering::SeqCst);
    ready(all).unwrap();
    ready(runtime.refresh(Some(&ImagesProviderId::new("missing").unwrap()))).unwrap();
    assert!(oracle["refresh"]["allSettled"].as_bool().unwrap());
}
struct Loader {
    mode: String,
    events: Arc<Mutex<Vec<String>>>,
    gate: Arc<AtomicBool>,
}
struct LoadedGenerator(Arc<Mutex<Vec<String>>>);
impl ImagesGenerator for LoadedGenerator {
    fn generate<'a>(&'a self, r: ImageGenerationRequest) -> ImagesGenerateCall<'a> {
        self.0.lock().unwrap().push("generate".into());
        Ok(Box::pin(async move { Ok(output(&r, "ok")) }))
    }
}
impl ImagesApiLoader for Loader {
    fn load(self: Arc<Self>) -> BoxFuture<'static, Result<Arc<dyn ImagesGenerator>, ModelsError>> {
        Box::pin(async move {
            self.events
                .lock()
                .unwrap()
                .push(format!("load:{}", self.mode));
            std::future::poll_fn(|_| {
                if self.gate.load(Ordering::SeqCst) {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            })
            .await;
            if self.mode.contains("failure") {
                Err(ModelsError::new(ModelsErrorCode::Stream))
            } else {
                Ok(Arc::new(LoadedGenerator(self.events.clone())) as Arc<dyn ImagesGenerator>)
            }
        })
    }
}
#[test]
fn complete_source_lazy_modes_preserve_per_call_loading_and_cached_success_or_failure() {
    let oracle = fixture();
    for case in oracle["lazy"].as_array().unwrap() {
        let mode = case["mode"].as_str().unwrap();
        let events = Arc::new(Mutex::new(Vec::new()));
        let queue = Arc::new(Queue::default());
        let loader = Arc::new(Loader {
            mode: mode.into(),
            events: events.clone(),
            gate: Arc::new(AtomicBool::new(true)),
        });
        let api: Box<dyn ImagesGenerator> = if mode.starts_with("per-call") {
            Box::new(PerCallImagesApi::new(loader))
        } else {
            Box::new(CachedImagesApi::new(loader, queue.clone()))
        };
        let mut values = Vec::new();
        for _ in 0..2 {
            let mut call = api.generate(request(Host::new())).unwrap();
            let result = match poll(&mut call) {
                Poll::Ready(v) => v,
                Poll::Pending => {
                    ready(queue.take());
                    ready(call)
                }
            };
            values.push(match result {
                Ok(output) => serde_json::to_value(output.stop_reason).unwrap(),
                Err(_) => json!("rejected"),
            });
        }
        assert_eq!(json!(*events.lock().unwrap()), case["events"]);
        assert_eq!(json!(values), case["values"]);
    }
}
#[test]
fn cached_load_survives_caller_drop_and_shared_failure_releases_all_waiters() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let queue = Arc::new(Queue::default());
    let gate = Arc::new(AtomicBool::new(false));
    let api = CachedImagesApi::new(
        Arc::new(Loader {
            mode: "cached-failure".into(),
            events: events.clone(),
            gate: gate.clone(),
        }),
        queue.clone(),
    );
    let mut first = api.generate(request(Host::new())).unwrap();
    assert!(poll(&mut first).is_pending());
    let mut task = queue.take();
    assert!(poll(&mut task).is_pending());
    drop(first);
    let mut second = api.generate(request(Host::new())).unwrap();
    assert!(poll(&mut second).is_pending());
    assert!(queue.0.lock().unwrap().is_empty());
    gate.store(true, Ordering::SeqCst);
    ready(task);
    assert_eq!(ready(second).unwrap().stop_reason, ImagesStopReason::Error);
    assert_eq!(
        ready(api.generate(request(Host::new())).unwrap())
            .unwrap()
            .stop_reason,
        ImagesStopReason::Error
    );
    assert_eq!(events.lock().unwrap().len(), 1);
}
#[test]
fn explicit_registry_replacement_retains_old_binding_checks_and_missing_api_rejects() {
    let oracle = fixture();
    let registry = ImagesApiRegistry::new();
    let api = ImagesApiId::new("synthetic-images").unwrap();
    registry.register(
        ImagesApiBinding::new(api.clone(), generator("old")),
        Some("source-old".into()),
    );
    let old = registry.get(&api).unwrap();
    registry.register(
        ImagesApiBinding::new(api.clone(), generator("new")),
        Some("source-new".into()),
    );
    let old_result = ready(old.generate(request(Host::new())).unwrap()).unwrap();
    assert_eq!(
        serde_json::to_value(old_result).unwrap()["output"][0]["text"],
        oracle["binding"]["old"]
    );
    let new_result = ready(registry.generate(request(Host::new()))).unwrap();
    assert_eq!(
        serde_json::to_value(new_result).unwrap()["output"][0]["text"],
        oracle["binding"]["new"]
    );
    let mut other = request(Host::new());
    other.resolved.model.api = ImagesApiId::new("other").unwrap();
    assert!(old.generate(other).is_err());
    let mut other = request(Host::new());
    other.resolved.model.api = ImagesApiId::new("other").unwrap();
    assert_eq!(
        ready(registry.generate(other)).unwrap_err().code(),
        ModelsErrorCode::Provider
    );
    assert!(
        registry
            .get(&ImagesApiId::new("missing").unwrap())
            .is_none()
    );
}

#[test]
fn replacing_collection_provider_does_not_revoke_old_image_catalog_refresh() {
    let oracle = fixture();
    let queue = Arc::new(Queue::default());
    let gate = Arc::new(AtomicBool::new(false));
    let source = Arc::new(Source {
        model_id: "old-fresh",
        calls: AtomicUsize::new(0),
        gate: gate.clone(),
        fail: AtomicBool::new(false),
    });
    let old = create_images_provider(CreateImagesProviderOptions {
        id: ImagesProviderId::new("replace").unwrap(),
        name: None,
        auth: auth_none(),
        models: vec![model()].into(),
        api: generator("old"),
        refresh: Some(ImagesRefreshConfig {
            source,
            spawner: queue.clone(),
        }),
    });
    let runtime = runtime(Host::new());
    runtime.set_provider(old.clone());
    let id = ImagesProviderId::new("replace").unwrap();
    let mut future = runtime.refresh(Some(&id));
    assert!(poll(&mut future).is_pending());
    let mut worker = queue.take();
    assert!(poll(&mut worker).is_pending());
    let mut new = model();
    new.id = ModelId::new("new-current").unwrap();
    runtime.set_provider(provider("replace", vec![new], None));
    gate.store(true, Ordering::SeqCst);
    ready(worker);
    ready(future).unwrap();
    assert_eq!(
        json!(
            old.models()
                .unwrap()
                .iter()
                .map(|v| v.id.as_str())
                .collect::<Vec<_>>()
        ),
        oracle["refreshReplacement"]["old"]
    );
    assert_eq!(
        json!(
            runtime
                .get_models(Some(&id))
                .iter()
                .map(|v| v.id.as_str())
                .collect::<Vec<_>>()
        ),
        oracle["refreshReplacement"]["current"]
    );
    assert_eq!(oracle["refreshReplacement"]["old"], json!(["old-fresh"]));
}
struct FixedAuth(AuthResult);
impl ApiKeyAuth for FixedAuth {
    fn name(&self) -> &str {
        "Synthetic"
    }
    fn resolve<'a>(
        &'a self,
        _: ApiKeyAuthInput<'a>,
    ) -> BoxFuture<'a, Result<Option<AuthResult>, AuthProviderError>> {
        Box::pin(async move { Ok(Some(self.0.clone())) })
    }
}
#[test]
fn get_model_auth_does_not_add_model_headers_and_unknown_provider_needs_no_auth() {
    let oracle = fixture();
    let host = Host::new();
    let runtime = runtime(host);
    runtime.set_provider(create_images_provider(CreateImagesProviderOptions {
        id: ImagesProviderId::new("openrouter").unwrap(),
        name: None,
        auth: ProviderAuth::new(
            Some(Arc::new(FixedAuth(
                serde_json::from_value(oracle["getAuth"].clone()).unwrap(),
            ))),
            None,
        )
        .unwrap(),
        models: vec![model()].into(),
        api: generator("ok"),
        refresh: None,
    }));
    let mut raw = serde_json::to_value(model()).unwrap();
    raw["headers"] = json!({"X-Model":"model"});
    let model = serde_json::from_value::<RawImagesModel>(raw)
        .unwrap()
        .into_model(&ModelHeaderPolicy::from_allowlist(["X-Model".into()]))
        .unwrap();
    let token = CancellationToken::new();
    let auth = ready(runtime.get_model_auth(&model, AuthResolutionOverrides::default(), &token))
        .unwrap()
        .unwrap();
    assert_eq!(serde_json::to_value(auth).unwrap(), oracle["getAuth"]);
    token.cancel();
    assert!(
        ready(runtime.get_auth(
            &ImagesProviderId::new("missing").unwrap(),
            AuthResolutionOverrides::default(),
            &token
        ))
        .unwrap()
        .is_none()
    );
}
struct RejectSpawner;
impl TaskSpawner for RejectSpawner {
    fn spawn(&self, _: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
        Err(SpawnError::new())
    }
}
#[test]
fn rejected_or_discarded_refresh_load_tasks_settle_without_stranding_waiters() {
    let source = Arc::new(Source {
        model_id: "fresh",
        calls: AtomicUsize::new(0),
        gate: Arc::new(AtomicBool::new(true)),
        fail: AtomicBool::new(false),
    });
    let provider = create_images_provider(CreateImagesProviderOptions {
        id: ImagesProviderId::new("dynamic").unwrap(),
        name: None,
        auth: auth_none(),
        models: vec![model()].into(),
        api: generator("ok"),
        refresh: Some(ImagesRefreshConfig {
            source: source.clone(),
            spawner: Arc::new(RejectSpawner),
        }),
    });
    for _ in 0..2 {
        assert!(ready(provider.models_refresher().unwrap().refresh_models()).is_err());
    }
    assert_eq!(source.calls.load(Ordering::SeqCst), 0);
    let queue = Arc::new(Queue::default());
    let api = CachedImagesApi::new(
        Arc::new(Loader {
            mode: "cached-success".into(),
            events: Arc::new(Mutex::new(Vec::new())),
            gate: Arc::new(AtomicBool::new(true)),
        }),
        queue.clone(),
    );
    let mut future = api.generate(request(Host::new())).unwrap();
    assert!(poll(&mut future).is_pending());
    drop(queue.take());
    assert_eq!(ready(future).unwrap().stop_reason, ImagesStopReason::Error);
}

#[cfg(feature = "wire-openrouter-images")]
mod wire_integration {
    use super::*;
    use pi_ai_providers::{
        api::{runtime::AdapterResponseHead, wire_json::WirePayload},
        images::openrouter::OpenRouterImagesApi,
    };
    use pi_ai_transport::*;
    use std::collections::BTreeMap;
    struct Jitter;
    impl RetryJitter for Jitter {
        fn sample(&self) -> f64 {
            panic!("integration has no retry")
        }
    }
    struct WireAuth(Arc<Seen>);
    impl ApiKeyAuth for WireAuth {
        fn name(&self) -> &str {
            "Synthetic"
        }
        fn resolve<'a>(
            &'a self,
            _: ApiKeyAuthInput<'a>,
        ) -> BoxFuture<'a, Result<Option<AuthResult>, AuthProviderError>> {
            Box::pin(async move {
                self.0.events.lock().unwrap().push("auth".into());
                Ok(Some(serde_json::from_value(json!({"auth":{"apiKey":"SYNTHETIC_WIRE_KEY","baseUrl":"https://credential.invalid/api","headers":{"X-Auth":"auth"}},"source":"API key"})).unwrap()))
            })
        }
    }
    struct Observer(Arc<Seen>);
    impl ImagePayloadMiddleware for Observer {
        fn on_payload<'a>(
            &'a self,
            _: &'a ImageGenerationRequest,
            _: &'a mut WirePayload,
        ) -> BoxFuture<'a, Result<Option<WirePayload>, ModelsError>> {
            Box::pin(async move {
                self.0.events.lock().unwrap().push("payload".into());
                Ok(None)
            })
        }
    }
    impl ImageResponseMiddleware for Observer {
        fn on_response<'a>(
            &'a self,
            r: &'a ImageGenerationRequest,
            _: &'a AdapterResponseHead,
        ) -> BoxFuture<'a, Result<(), ModelsError>> {
            Box::pin(async move {
                self.0.events.lock().unwrap().push("response".into());
                r.options.runtime.cancellation.cancel();
                Ok(())
            })
        }
    }
    struct Body(Option<Vec<u8>>);
    impl ResponseBody for Body {
        fn next<'a>(
            &'a mut self,
            _: &'a RequestRuntime,
        ) -> BoxFuture<'a, Result<Option<Vec<u8>>, TransportError>> {
            Box::pin(async move { Ok(self.0.take()) })
        }
        fn close(&mut self) {}
    }
    struct Fetch {
        seen: Arc<Seen>,
        response: Value,
    }
    impl CustomFetch for Fetch {
        fn fetch<'a>(
            &'a self,
            r: &'a FetchRequest,
            _: &'a RequestRuntime,
        ) -> BoxFuture<'a, Result<HttpResponse, TransportError>> {
            Box::pin(async move {
                self.seen.events.lock().unwrap().push("fetch".into());
                let FetchHeaders::Constructed(headers) = &r.headers else {
                    panic!("SDK Headers")
                };
                let FetchBody::Text(body) = &r.body else {
                    panic!("SDK text body")
                };
                self.seen.calls.lock().unwrap().push(json!({"url":r.endpoint.expose_for_transport(),"headers":headers.iter_for_fetch().collect::<BTreeMap<_,_>>(),"body":body}));
                let mut headers = HttpHeaders::new();
                headers.set("content-type", "application/json")?;
                Ok(HttpResponse {
                    status: 200,
                    status_text: String::new(),
                    headers,
                    body: Box::new(Body(Some(serde_json::to_vec(&self.response).unwrap()))),
                })
            })
        }
    }
    #[test]
    fn complete_runtime_auth_dispatch_to_accepted_wire_preserves_endpoint_and_callback_cancel_success()
     {
        let oracle = fixture();
        let expected = &oracle["integration"];
        let seen = Arc::new(Seen::default());
        let host = Host::new();
        let model: ImagesModel = serde_json::from_value(expected["model"].clone()).unwrap();
        let original = model.clone();
        let mut services = ImageAdapterServices::new(Arc::new(HttpTransport::new_custom(
            Arc::new(Fetch {
                seen: seen.clone(),
                response: expected["response"].clone(),
            }),
            Arc::new(Jitter),
        )));
        let observer = Arc::new(Observer(seen.clone()));
        services.payload = Some(observer.clone());
        services.response = Some(observer);
        let runtime = create_images_models(ImagesModelsOptions {
            credentials: Some(Arc::new(Store {
                inner: InMemoryCredentialStore::new(),
                seen: seen.clone(),
            })),
            auth_context: Arc::new(NoContext),
            clock: host.clone(),
        });
        runtime.set_provider(create_images_provider(CreateImagesProviderOptions {
            id: ImagesProviderId::new("openrouter").unwrap(),
            name: None,
            auth: ProviderAuth::new(Some(Arc::new(WireAuth(seen.clone()))), None).unwrap(),
            models: vec![model.clone()].into(),
            api: images_generator_from_adapter(Arc::new(OpenRouterImagesApi::new(services))),
            refresh: None,
        }));
        let cancellation = CancellationToken::new();
        let result = ready(
            runtime.generate_images(
                model,
                serde_json::from_value(oracle["context"].clone()).unwrap(),
                ImagesOptions {
                    wire: ImagesOptionsWire::default(),
                    runtime: RequestRuntime::with_noop_telemetry(
                        cancellation.clone(),
                        host.clone(),
                        host,
                    ),
                },
                ImagesRequestOverrides {
                    api_key: None,
                    env: None,
                    headers: serde_json::from_value(
                        json!({"Authorization":null,"X-Explicit":"explicit"}),
                    )
                    .unwrap(),
                },
            ),
        )
        .unwrap();
        assert_eq!(
            result,
            serde_json::from_value::<AssistantImages>(expected["output"].clone()).unwrap()
        );
        assert_eq!(json!(*seen.events.lock().unwrap()), expected["events"]);
        assert_eq!(json!(*seen.calls.lock().unwrap()), expected["calls"]);
        assert!(cancellation.is_cancelled());
        assert_eq!(original.base_url.as_str(), expected["originalBaseUrl"]);
        assert_eq!(runtime.get_models(None)[0], original);
    }
}

#[test]
fn configured_loader_retains_one_adapter_and_dropped_waiter_waker_can_reenter() {
    use std::task::Wake;
    let retained = generator("one");
    let loader = Arc::new(ConfiguredImagesApiLoader::new(retained.clone()));
    assert!(Arc::ptr_eq(
        &ready(loader.clone().load()).unwrap(),
        &retained
    ));
    assert!(Arc::ptr_eq(&ready(loader.load()).unwrap(), &retained));
    struct Reenter {
        api: std::sync::Weak<CachedImagesApi>,
        entered: Arc<AtomicBool>,
    }
    // Unlike Waker::noop(), this waker owns the destructor reentry under test.
    #[allow(clippy::manual_noop_waker)]
    impl Wake for Reenter {
        fn wake(self: Arc<Self>) {}
    }
    impl Drop for Reenter {
        fn drop(&mut self) {
            let api = self.api.upgrade().unwrap();
            let mut call = api.generate(request(Host::new())).unwrap();
            assert!(poll(&mut call).is_pending());
            self.entered.store(true, Ordering::SeqCst);
        }
    }
    let queue = Arc::new(Queue::default());
    let api = Arc::new(CachedImagesApi::new(
        Arc::new(Loader {
            mode: "cached-success".into(),
            events: Arc::new(Mutex::new(vec![])),
            gate: Arc::new(AtomicBool::new(false)),
        }),
        queue.clone(),
    ));
    let entered = Arc::new(AtomicBool::new(false));
    let waker = Waker::from(Arc::new(Reenter {
        api: Arc::downgrade(&api),
        entered: entered.clone(),
    }));
    let mut call = api.generate(request(Host::new())).unwrap();
    assert!(
        call.as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    drop(waker);
    drop(call);
    assert!(entered.load(Ordering::SeqCst));
    drop(queue.take());
}

#[test]
fn source_synchronous_refresh_throw_stays_memoized_while_async_rejection_retries() {
    struct LaunchSource {
        calls: AtomicUsize,
        synchronous: bool,
    }
    impl ImagesModelsSource for LaunchSource {
        fn fetch(self: Arc<Self>) -> ImagesModelsFetchCall {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.synchronous {
                Err(ModelsError::new(ModelsErrorCode::ModelSource))
            } else {
                Ok(Box::pin(async {
                    Err(ModelsError::new(ModelsErrorCode::ModelSource))
                }))
            }
        }
    }
    let oracle = fixture();
    for case in oracle["refreshLaunch"].as_array().unwrap() {
        let queue = Arc::new(Queue::default());
        let source = Arc::new(LaunchSource {
            calls: AtomicUsize::new(0),
            synchronous: case["synchronous"].as_bool().unwrap(),
        });
        let provider = create_images_provider(CreateImagesProviderOptions {
            id: ImagesProviderId::new("launch").unwrap(),
            name: None,
            auth: auth_none(),
            models: vec![model()].into(),
            api: generator("ok"),
            refresh: Some(ImagesRefreshConfig {
                source: source.clone(),
                spawner: queue.clone(),
            }),
        });
        let refresh = provider.models_refresher().unwrap();
        let first = refresh.refresh_models();
        assert_eq!(
            queue.0.lock().unwrap().len(),
            usize::from(!source.synchronous)
        );
        if !source.synchronous {
            ready(queue.take());
        }
        assert!(ready(first).is_err());
        let second = refresh.refresh_models();
        let fresh_flight = !queue.0.lock().unwrap().is_empty();
        assert_eq!(!fresh_flight, case["samePromise"].as_bool().unwrap());
        if fresh_flight {
            ready(queue.take());
        }
        assert!(ready(second).is_err());
        assert_eq!(
            source.calls.load(Ordering::SeqCst),
            case["calls"].as_u64().unwrap() as usize
        );
        assert_eq!(
            json!(
                provider
                    .models()
                    .unwrap()
                    .iter()
                    .map(|v| v.id.as_str())
                    .collect::<Vec<_>>()
            ),
            case["models"]
        );
    }
}

struct ReentrantProvider {
    base: Arc<dyn ImagesProvider>,
    label: String,
    refresh: bool,
    action: Mutex<Option<Box<dyn Fn() + Send + Sync>>>,
    events: Arc<Mutex<Vec<String>>>,
}
impl ReentrantProvider {
    fn act(&self) {
        let action = self.action.lock().unwrap().take();
        if let Some(action) = action {
            action();
        }
    }
}
impl ImagesProvider for ReentrantProvider {
    fn id(&self) -> &ImagesProviderId {
        self.base.id()
    }
    fn name(&self) -> &str {
        self.base.name()
    }
    fn auth(&self) -> &ProviderAuth {
        self.base.auth()
    }
    fn models(&self) -> Result<Arc<[ImagesModel]>, ModelsError> {
        if !self.refresh {
            self.events.lock().unwrap().push(self.label.clone());
            self.act();
        }
        self.base.models()
    }
    fn models_refresher(&self) -> Option<&(dyn ImagesModelsRefresher + '_)> {
        self.refresh.then_some(self)
    }
    fn generate<'a>(&'a self, r: ImageGenerationRequest) -> ImagesGenerateCall<'a> {
        self.base.generate(r)
    }
}
impl ImagesModelsRefresher for ReentrantProvider {
    fn refresh_models(&self) -> BoxFuture<'_, Result<(), ModelsError>> {
        self.events
            .lock()
            .unwrap()
            .push(format!("start:{}", self.label));
        self.act();
        Box::pin(async move {
            self.events
                .lock()
                .unwrap()
                .push(format!("finish:{}", self.label));
            Ok(())
        })
    }
}
fn reentrant_provider(
    id: &str,
    label: &str,
    refresh: bool,
    events: Arc<Mutex<Vec<String>>>,
    action: Option<Box<dyn Fn() + Send + Sync>>,
) -> Arc<dyn ImagesProvider> {
    let mut item = model();
    item.id = ModelId::new(label).unwrap();
    Arc::new(ReentrantProvider {
        base: provider(id, vec![item], None),
        label: label.into(),
        refresh,
        events,
        action: Mutex::new(action),
    })
}
#[test]
fn complete_source_live_listing_and_refresh_admission_preserve_bounded_membership_reentry() {
    let source = fixture();
    assert_eq!(source["live"].as_array().unwrap().len(), 8);
    for case in source["live"].as_array().unwrap() {
        let runtime = Arc::new(runtime(Host::new()));
        let weak = Arc::downgrade(&runtime);
        let events = Arc::new(Mutex::new(Vec::new()));
        let callback_events = events.clone();
        let mode = case["mode"].as_str().unwrap().to_owned();
        let refresh = case["refreshMode"].as_bool().unwrap();
        let action = Box::new(move || {
            let runtime = weak.upgrade().unwrap();
            match mode.as_str() {
                "add" | "replace" => runtime.set_provider(reentrant_provider(
                    "b",
                    "b-new",
                    refresh,
                    callback_events.clone(),
                    None,
                )),
                "delete-reinsert" => {
                    runtime.delete_provider(&ImagesProviderId::new("b").unwrap());
                    runtime.set_provider(reentrant_provider(
                        "b",
                        "b-new",
                        refresh,
                        callback_events.clone(),
                        None,
                    ));
                }
                "clear" => {
                    runtime.clear_providers();
                    runtime.set_provider(reentrant_provider(
                        "d",
                        "d",
                        refresh,
                        callback_events.clone(),
                        None,
                    ));
                }
                _ => panic!("fixture mode"),
            }
        });
        runtime.set_provider(reentrant_provider(
            "a",
            "a",
            refresh,
            events.clone(),
            Some(action),
        ));
        if case["mode"] != "add" {
            runtime.set_provider(reentrant_provider(
                "b",
                "b-old",
                refresh,
                events.clone(),
                None,
            ));
            runtime.set_provider(reentrant_provider("c", "c", refresh, events.clone(), None));
        }
        assert_eq!(
            json!(
                runtime
                    .get_providers()
                    .iter()
                    .map(|p| p.id().as_str())
                    .collect::<Vec<_>>()
            ),
            case["snapshot"]
        );
        assert!(events.lock().unwrap().is_empty());
        if refresh {
            ready(runtime.refresh(None)).unwrap();
        } else {
            assert_eq!(
                json!(
                    runtime
                        .get_models(None)
                        .iter()
                        .map(|m| m.id.as_str())
                        .collect::<Vec<_>>()
                ),
                case["models"]
            );
        }
        assert_eq!(
            json!(*events.lock().unwrap()),
            case["events"],
            "{} refresh={refresh}",
            case["mode"]
        );
        assert_eq!(
            json!(
                runtime
                    .get_providers()
                    .iter()
                    .map(|p| p.id().as_str())
                    .collect::<Vec<_>>()
            ),
            case["providers"]
        );
    }
}
struct NestedSource {
    provider: Mutex<Option<std::sync::Weak<dyn ImagesProvider>>>,
    calls: AtomicUsize,
    gates: Mutex<Vec<Arc<AtomicBool>>>,
    children: Mutex<Vec<BoxFuture<'static, Result<(), ModelsError>>>>,
}
impl ImagesModelsSource for NestedSource {
    fn fetch(self: Arc<Self>) -> ImagesModelsFetchCall {
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        let gate = Arc::new(AtomicBool::new(false));
        self.gates.lock().unwrap().push(gate.clone());
        if call == 1 {
            let provider = self
                .provider
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .upgrade()
                .unwrap();
            let mut child: BoxFuture<'static, Result<(), ModelsError>> =
                Box::pin(
                    async move { provider.models_refresher().unwrap().refresh_models().await },
                );
            assert!(poll(&mut child).is_pending());
            self.children.lock().unwrap().push(child);
        }
        Ok(Box::pin(async move {
            std::future::poll_fn(|_| {
                if gate.load(Ordering::SeqCst) {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            })
            .await;
            let mut model = model();
            model.id = ModelId::new(format!("model-{call}")).unwrap();
            Ok(vec![model].into())
        }))
    }
}
#[test]
fn complete_source_nested_refresh_launch_and_completion_clear_current_flight_without_fencing() {
    let oracle = fixture();
    let expected = &oracle["nestedRefresh"];
    let queue = Arc::new(Queue::default());
    let source = Arc::new(NestedSource {
        provider: Mutex::new(None),
        calls: AtomicUsize::new(0),
        gates: Mutex::new(Vec::new()),
        children: Mutex::new(Vec::new()),
    });
    let provider = create_images_provider(CreateImagesProviderOptions {
        id: ImagesProviderId::new("nested").unwrap(),
        name: None,
        auth: auth_none(),
        models: vec![model()].into(),
        api: generator("ok"),
        refresh: Some(ImagesRefreshConfig {
            source: source.clone(),
            spawner: queue.clone(),
        }),
    });
    *source.provider.lock().unwrap() = Some(Arc::downgrade(&provider));
    let refresh = provider.models_refresher().unwrap();
    let outer = refresh.refresh_models();
    let mut joined = refresh.refresh_models();
    assert_eq!(
        source.calls.load(Ordering::SeqCst),
        expected["afterLaunchCalls"].as_u64().unwrap() as usize
    );
    assert_eq!(queue.0.lock().unwrap().len(), 2);
    let child_task = queue.take();
    let outer_task = queue.take();
    source.gates.lock().unwrap()[1].store(true, Ordering::SeqCst);
    ready(child_task);
    ready(source.children.lock().unwrap().remove(0)).unwrap();
    assert!(
        poll(&mut joined).is_pending(),
        "joined the outer visible flight, not the nested child"
    );
    let third = refresh.refresh_models();
    assert_eq!(
        source.calls.load(Ordering::SeqCst),
        expected["afterChildCalls"].as_u64().unwrap() as usize
    );
    source.gates.lock().unwrap()[0].store(true, Ordering::SeqCst);
    ready(outer_task);
    ready(outer).unwrap();
    ready(joined).unwrap();
    let fourth = refresh.refresh_models();
    assert_eq!(
        source.calls.load(Ordering::SeqCst),
        expected["afterOuterCalls"].as_u64().unwrap() as usize
    );
    let third_task = queue.take();
    let fourth_task = queue.take();
    source.gates.lock().unwrap()[3].store(true, Ordering::SeqCst);
    ready(fourth_task);
    ready(fourth).unwrap();
    assert_eq!(
        json!(
            provider
                .models()
                .unwrap()
                .iter()
                .map(|m| m.id.as_str())
                .collect::<Vec<_>>()
        ),
        expected["afterFourth"]
    );
    source.gates.lock().unwrap()[2].store(true, Ordering::SeqCst);
    ready(third_task);
    ready(third).unwrap();
    assert_eq!(
        json!(
            provider
                .models()
                .unwrap()
                .iter()
                .map(|m| m.id.as_str())
                .collect::<Vec<_>>()
        ),
        expected["final"]
    );
}
#[test]
fn simultaneous_other_thread_call_joins_unpublished_launch_without_holding_host_lock() {
    use std::sync::{Condvar, mpsc};
    struct Source {
        calls: AtomicUsize,
        entered: mpsc::Sender<()>,
        release: (Mutex<bool>, Condvar),
    }
    impl ImagesModelsSource for Source {
        fn fetch(self: Arc<Self>) -> ImagesModelsFetchCall {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.entered.send(()).unwrap();
            let mut open = self.release.0.lock().unwrap();
            while !*open {
                open = self.release.1.wait(open).unwrap();
            }
            drop(open);
            Ok(Box::pin(async { Ok(vec![model()].into()) }))
        }
    }
    let (tx, rx) = mpsc::channel();
    let source = Arc::new(Source {
        calls: AtomicUsize::new(0),
        entered: tx,
        release: (Mutex::new(false), Condvar::new()),
    });
    let queue = Arc::new(Queue::default());
    let provider = create_images_provider(CreateImagesProviderOptions {
        id: ImagesProviderId::new("parallel").unwrap(),
        name: None,
        auth: auth_none(),
        models: vec![model()].into(),
        api: generator("ok"),
        refresh: Some(ImagesRefreshConfig {
            source: source.clone(),
            spawner: queue.clone(),
        }),
    });
    let other = provider.clone();
    let thread = std::thread::spawn(move || {
        drop(other.models_refresher().unwrap().refresh_models());
    });
    rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let mut join = provider.models_refresher().unwrap().refresh_models();
    assert!(poll(&mut join).is_pending());
    assert_eq!(source.calls.load(Ordering::SeqCst), 1);
    *source.release.0.lock().unwrap() = true;
    source.release.1.notify_all();
    thread.join().unwrap();
    assert_eq!(queue.0.lock().unwrap().len(), 1);
    ready(queue.take());
    ready(join).unwrap();
}

#[test]
fn displaced_provider_destructors_reenter_listing_outside_membership_locks() {
    struct OnDrop {
        base: Arc<dyn ImagesProvider>,
        runtime: std::sync::Weak<ImagesModels>,
        dropped: Arc<AtomicBool>,
    }
    impl Drop for OnDrop {
        fn drop(&mut self) {
            let runtime = self.runtime.upgrade().unwrap();
            let _ = runtime.get_models(None);
            self.dropped.store(true, Ordering::SeqCst);
        }
    }
    impl ImagesProvider for OnDrop {
        fn id(&self) -> &ImagesProviderId {
            self.base.id()
        }
        fn name(&self) -> &str {
            self.base.name()
        }
        fn auth(&self) -> &ProviderAuth {
            self.base.auth()
        }
        fn models(&self) -> Result<Arc<[ImagesModel]>, ModelsError> {
            self.base.models()
        }
        fn models_refresher(&self) -> Option<&(dyn ImagesModelsRefresher + '_)> {
            None
        }
        fn generate<'a>(&'a self, r: ImageGenerationRequest) -> ImagesGenerateCall<'a> {
            self.base.generate(r)
        }
    }
    for operation in ["replace", "delete", "clear"] {
        let runtime = Arc::new(runtime(Host::new()));
        let dropped = Arc::new(AtomicBool::new(false));
        runtime.set_provider(Arc::new(OnDrop {
            base: provider("a", vec![], None),
            runtime: Arc::downgrade(&runtime),
            dropped: dropped.clone(),
        }));
        match operation {
            "replace" => runtime.set_provider(provider("a", vec![], None)),
            "delete" => runtime.delete_provider(&ImagesProviderId::new("a").unwrap()),
            "clear" => runtime.clear_providers(),
            _ => unreachable!(),
        };
        assert!(dropped.load(Ordering::SeqCst));
    }
}
