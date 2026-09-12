#![allow(dead_code)]
use pi_ai::{builtins::*, compat::*, *};
use pi_ai_providers::{
    api::{
        azure_openai_responses::*, bedrock_converse_stream::*, google_vertex::*,
        mistral_conversations::*, openai_codex_responses::*, pi_messages::*,
        runtime::AdapterServices,
    },
    images::ImageAdapterServices,
};
use pi_ai_transport::*;
use serde_json::{Value, json};
use std::{
    future::Future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context as TaskContext, Poll, Waker},
    time::{Duration, Instant},
};
fn run(f: impl Future<Output = ()>) {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            tokio::time::timeout(Duration::from_secs(10), f)
                .await
                .expect("bounded fixture")
        });
}
struct Host {
    started: Instant,
    spawns: AtomicUsize,
}
impl Host {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            started: Instant::now(),
            spawns: AtomicUsize::new(0),
        })
    }
}
impl Clock for Host {
    fn now_unix_ms(&self) -> u64 {
        1700000000000 + self.started.elapsed().as_millis() as u64
    }
    fn sleep<'a>(
        &'a self,
        d: Duration,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), Cancelled>> {
        Box::pin(async move {
            tokio::time::sleep(d).await;
            Ok(())
        })
    }
}
impl TaskSpawner for Host {
    fn spawn(&self, f: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
        self.spawns.fetch_add(1, Ordering::SeqCst);
        tokio::spawn(f);
        Ok(())
    }
}
impl UuidV7Entropy for Host {
    fn random_bytes(&self) -> Result<[u8; 16], UuidV7EntropyError> {
        Ok([7; 16])
    }
}
impl RetryJitter for Host {
    fn sample(&self) -> f64 {
        0.5
    }
}
impl BedrockConfigProvider for Host {
    fn create_session(&self, _: BedrockConfigKey<'_>) -> Result<BedrockSettings, ModelsError> {
        Ok(BedrockSettings {
            region: BedrockSetting::Value("us-east-1".into()),
            auth_scheme_preference: BedrockSetting::Value(vec![BedrockAuthScheme::Bearer]),
            token: BedrockSetting::Value(BedrockToken {
                token: SecretString::new("SYNTHETIC_BEDROCK"),
                expiration_unix_ms: None,
            }),
            ..Default::default()
        })
    }
}
impl BedrockEntropy for Host {
    fn invocation_bytes(&self) -> Result<[u8; 16], ModelsError> {
        Ok([3; 16])
    }
    fn retry_jitter(&self) -> Result<f64, ModelsError> {
        Ok(0.5)
    }
}
struct Env {
    calls: Mutex<Vec<String>>,
    value: Mutex<Option<String>>,
    blocked: AtomicBool,
    release: tokio::sync::Notify,
}
impl Env {
    fn new(value: Option<&str>) -> Arc<Self> {
        Arc::new(Self {
            calls: Mutex::new(vec![]),
            value: Mutex::new(value.map(str::to_owned)),
            blocked: AtomicBool::new(false),
            release: tokio::sync::Notify::new(),
        })
    }
}
impl AuthContext for Env {
    fn env<'a>(
        &'a self,
        n: &'a str,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<String>, AuthContextError>> {
        self.calls.lock().unwrap().push(n.into());
        Box::pin(async move {
            if self.blocked.load(Ordering::SeqCst) {
                self.release.notified().await;
            }
            Ok(self.value.lock().unwrap().clone())
        })
    }
    fn file_exists<'a>(
        &'a self,
        _: &'a str,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<bool, AuthContextError>> {
        panic!("no ambient files")
    }
}
struct Body(Option<Vec<u8>>);
impl ResponseBody for Body {
    fn next<'a>(
        &'a mut self,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, TransportError>> {
        Box::pin(async { Ok(self.0.take()) })
    }
    fn close(&mut self) {}
}
#[derive(Default)]
struct Peer {
    calls: Mutex<Vec<Value>>,
}
impl CustomFetch for Peer {
    fn fetch<'a>(
        &'a self,
        r: &'a FetchRequest,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<HttpResponse, TransportError>> {
        Box::pin(async move {
            let body = match &r.body {
                FetchBody::Text(s) => serde_json::from_str::<Value>(s).unwrap(),
                FetchBody::Bytes(s) => serde_json::from_slice(s).unwrap(),
                FetchBody::Absent => Value::Null,
            };
            let image = body.get("modalities").is_some();
            self.calls
                .lock()
                .unwrap()
                .push(json!({"endpoint":r.endpoint.expose_for_transport(),"body":body}));
            let mut headers = HttpHeaders::new();
            headers.set("content-type", "application/json").unwrap();
            Ok(HttpResponse {
                status: if image { 200 } else { 400 },
                status_text: "Synthetic".into(),
                headers,
                body: Box::new(Body(Some(
                    if image {
                        json!({"choices":[{"message":{"content":"synthetic-image"}}]}).to_string()
                    } else {
                        json!({"error":{"message":"synthetic fixture failure"}}).to_string()
                    }
                    .into_bytes(),
                ))),
            })
        })
    }
}
fn configured(
    host: Arc<Host>,
    peer: Arc<Peer>,
    ws: Option<Arc<dyn PersistentWebSocketConnector>>,
) -> (Arc<BuiltinProviderModules>, CompatSharedApiModules) {
    let transport =
        Arc::new(HttpTransport::new(peer.clone(), host.clone()).with_custom_fetch(peer));
    let a = AdapterServices::new(transport.clone());
    let selected = Arc::new(
        BuiltinProviderModules::new(BuiltinProviderServices {
            anthropic: a.clone(),
            google: a.clone(),
            openai: a.clone(),
            completions: a.clone(),
            codex: CodexServices {
                adapter: a.clone(),
                websocket: ws,
                request_ids: Arc::new(UuidV7Generator::new(host.clone(), host.clone())),
                date_parser: Arc::new(HttpRetryDateParser),
                signal_presence: CodexSourceSignalPresence::Present,
                compression: None,
                limits: CodexResourceLimits::default(),
            },
            images: ImageAdapterServices::new(transport.clone()),
            auth: FactoryAuthServices {
                spawner: host.clone(),
                oauth: None,
            },
        })
        .unwrap(),
    );
    let mut vertex = GoogleVertexApi::new(a.clone());
    vertex.defaults.project = Some("synthetic-project".into());
    vertex.defaults.location = Some("us-central1".into());
    let shared = CompatSharedApiModules {
        azure: Arc::new(AzureOpenAIResponsesApi::new(a.clone())),
        google_vertex: Arc::new(vertex),
        mistral: Arc::new(MistralApi::new(a.clone())),
        bedrock: Arc::new(BedrockApi::new(
            AdapterServices::new(transport),
            host.clone(),
            host,
        )),
        pi_messages: Arc::new(PiMessagesApi::new(a)),
    };
    (selected, shared)
}
struct FailingStore;
impl CredentialStore for FailingStore {
    fn read<'a>(
        &'a self,
        _: &'a ProviderId,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<VersionedCredential>, CredentialStoreError>> {
        panic!("legacy direct path must not read credentials")
    }
    fn list<'a>(
        &'a self,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Vec<CredentialInfo>, CredentialStoreError>> {
        panic!("no credential list")
    }
    fn begin_mutation<'a>(
        &'a self,
        _: &'a ProviderId,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Box<dyn CredentialMutation>, CredentialStoreError>> {
        panic!("no credential mutation")
    }
    fn reconcile<'a>(
        &'a self,
        _: &'a ProviderId,
        _: CredentialReconciliation,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<VersionedCredential>, CredentialStoreError>> {
        panic!("no reconcile")
    }
}
fn options(host: Arc<Host>, env: Arc<Env>) -> ModelsRegistryOptions {
    ModelsRegistryOptions {
        credentials: Arc::new(FailingStore),
        models_store: Box::new(InMemoryModelsStore::new()),
        auth_context: env,
        clock: host.clone(),
        spawner: host,
        model_header_policy: Default::default(),
    }
}
fn runtime(host: Arc<Host>) -> RequestRuntime {
    RequestRuntime::with_noop_telemetry(CancellationToken::new(), host.clone(), host)
}
const JWT: &str = "e30.eyJodHRwczovL2FwaS5vcGVuYWkuY29tL2F1dGgiOiB7ImNoYXRncHRfYWNjb3VudF9pZCI6ICJzeW50aGV0aWMtYWNjb3VudCJ9fQ.synthetic";
fn request(host: Arc<Host>, api: &str, provider: &str, key: Option<&str>) -> TextStreamRequest {
    let model:Model=serde_json::from_value(json!({"id":"synthetic-unknown-model","name":"Synthetic","api":api,"provider":provider,"baseUrl":"https://synthetic.invalid/v1","reasoning":false,"input":["text"],"cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0},"contextWindow":10000,"maxTokens":1000})).unwrap();
    TextStreamRequest{resolved:ResolvedRequest{model,endpoint:RequestEndpoint::new_resolved("https://synthetic.invalid/v1").unwrap()},context:serde_json::from_value(json!({"messages":[{"role":"user","content":"synthetic","timestamp":7}],"tools":[{"name":"lookup","description":"synthetic","parameters":{"type":"object","properties":{}}}]})).unwrap(),auth:ResolvedAuth{api_key:key.map(SecretString::new),..Default::default()},options:StreamOptions{wire:StreamOptionsWire{max_retries:Some(SafeUnsigned::new(0).unwrap()),..Default::default()},runtime:runtime(host)}}
}
fn simple(r: TextStreamRequest) -> SimpleStreamRequest {
    SimpleStreamRequest {
        resolved: r.resolved,
        context: r.context,
        auth: r.auth,
        options: SimpleStreamOptions {
            wire: SimpleStreamOptionsWire {
                stream: r.options.wire,
                ..Default::default()
            },
            runtime: r.options.runtime,
        },
    }
}
#[derive(Clone)]
struct Fake {
    api: ApiId,
    label: &'static str,
    seen: Arc<Mutex<Vec<Value>>>,
}
impl Fake {
    fn arc(api: &str, label: &'static str, seen: Arc<Mutex<Vec<Value>>>) -> Arc<Self> {
        Arc::new(Self {
            api: api.parse().unwrap(),
            label,
            seen,
        })
    }
}
impl ApiAdapter for Fake {
    fn api_id(&self) -> &ApiId {
        &self.api
    }
    fn stream(&self, r: TextStreamRequest) -> AssistantStream {
        self.seen.lock().unwrap().push(json!({"label":self.label,"key":r.auth.api_key.as_ref().map(|k|k.expose_secret()),"id":r.resolved.model.id.as_str()}));
        let (s, p) = create_assistant_message_event_stream(AssistantStreamInit {
            api: r.resolved.model.api.clone(),
            provider: r.resolved.model.provider.clone(),
            model: r.resolved.model.id.clone(),
            clock: r.options.runtime.clock,
            cancellation: r.options.runtime.cancellation,
        });
        let message:AssistantMessage=serde_json::from_value(json!({"role":"assistant","content":[{"type":"text","text":self.label}],"api":r.resolved.model.api,"provider":r.resolved.model.provider,"model":r.resolved.model.id,"usage":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"totalTokens":0,"cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"total":0}},"stopReason":"stop","timestamp":7})).unwrap();
        p.push(AssistantMessageEvent::Done {
            reason: DoneReason::Stop,
            message: Box::new(message),
        })
        .unwrap();
        s
    }
    fn stream_simple(&self, r: SimpleStreamRequest) -> AssistantStream {
        self.stream(TextStreamRequest {
            resolved: r.resolved,
            context: r.context,
            auth: r.auth,
            options: StreamOptions {
                wire: r.options.wire.stream,
                runtime: r.options.runtime,
            },
        })
    }
    fn fetch_deferred(&self, _: DeferredFetchRequest) -> Result<AssistantStream, CapabilityError> {
        Err(CapabilityError::Unsupported)
    }
    fn cancel_deferred<'a>(
        &'a self,
        _: DeferredCancelRequest,
    ) -> BoxFuture<'a, Result<(), ModelsError>> {
        Box::pin(async { Err(ModelsError::new(ModelsErrorCode::Provider)) })
    }
}
#[test]
fn exact_registration_remembered_override_shortcut_and_binding_snapshot() {
    run(async {
        let host = Host::new();
        let peer = Arc::new(Peer::default());
        let env = Env::new(Some("SYNTHETIC_ENV"));
        let (modules, shared) = configured(host.clone(), peer.clone(), None);
        let seen = Arc::new(Mutex::new(vec![]));
        let registry = CompatApiRegistry::new();
        let id: ApiId = "openai-completions".parse().unwrap();
        registry.register_api_provider(
            Fake::arc(id.as_str(), "before", seen.clone()),
            Some("before".into()),
        );
        let before = registry.get_api_provider(&id).unwrap();
        let facade = CompatFacade::initialize(
            registry,
            modules.clone(),
            shared,
            options(host.clone(), env),
        )
        .await
        .unwrap();
        assert!(Arc::ptr_eq(
            &before,
            &facade.registry().get_api_provider(&id).unwrap()
        ));
        // Source insertion order retains the preexisting entry, then appends missing builtins.
        assert_eq!(
            facade
                .registry()
                .get_api_providers()
                .iter()
                .map(|p| p.api_id().as_str())
                .collect::<Vec<_>>(),
            vec![
                "openai-completions",
                "anthropic-messages",
                "openai-responses",
                "openai-codex-responses",
                "azure-openai-responses",
                "google-generative-ai",
                "google-vertex",
                "mistral-conversations",
                "bedrock-converse-stream",
                "pi-messages"
            ]
        );
        let result = facade
            .complete(request(
                host.clone(),
                id.as_str(),
                "openrouter",
                Some("SYNTHETIC"),
            ))
            .await
            .unwrap();
        assert_eq!(result.stop_reason, StopReason::Error);
        assert_eq!(peer.calls.lock().unwrap().len(), 1);
        assert!(seen.lock().unwrap().is_empty());
        facade.registry().register_api_provider(
            Fake::arc(id.as_str(), "later", seen.clone()),
            Some("later".into()),
        );
        facade
            .complete(request(
                host.clone(),
                id.as_str(),
                "openrouter",
                Some("SYNTHETIC"),
            ))
            .await
            .unwrap();
        assert_eq!(seen.lock().unwrap()[0]["label"], "later");
        facade
            .registry()
            .register_builtin_api_providers(facade.modules());
        facade
            .complete(request(
                host.clone(),
                id.as_str(),
                "openrouter",
                Some("SYNTHETIC"),
            ))
            .await
            .unwrap();
        assert_eq!(peer.calls.lock().unwrap().len(), 2);
        assert!(
            before
                .stream(request(host.clone(), "wrong", "custom", Some("x")))
                .is_err()
        );
        facade.registry().unregister_api_providers("before");
        assert!(facade.registry().get_api_provider(&id).is_some());
        facade.registry().unregister_api_providers("later");
        assert!(facade.registry().get_api_provider(&id).is_none());
        facade.registry().reset_api_providers(facade.modules());
        assert_eq!(
            facade.registry().get_api_providers()[0].api_id().as_str(),
            "anthropic-messages"
        );
        assert!(Arc::ptr_eq(
            &modules.codex(),
            &facade.session_resources().module()
        ));
        assert!(Arc::ptr_eq(&modules.codex(), &facade.modules().codex()));
    });
}
#[test]
fn captured_before_pending_env_explicit_bytes_marker_and_missing_api() {
    run(async {
        let host = Host::new();
        let peer = Arc::new(Peer::default());
        let env = Env::new(Some("SYNTHETIC_ENV"));
        let (modules, shared) = configured(host.clone(), peer, None);
        let seen = Arc::new(Mutex::new(vec![]));
        let facade = CompatFacade::initialize(
            CompatApiRegistry::new(),
            modules,
            shared,
            options(host.clone(), env.clone()),
        )
        .await
        .unwrap();
        let id: ApiId = "openai-completions".parse().unwrap();
        facade
            .registry()
            .register_api_provider(Fake::arc(id.as_str(), "captured", seen.clone()), None);
        assert!(
            facade
                .stream(request(host.clone(), "unknown", "custom", None))
                .is_err()
        );
        assert!(env.calls.lock().unwrap().is_empty());
        facade
            .complete(request(
                host.clone(),
                id.as_str(),
                "custom",
                Some("  explicit  "),
            ))
            .await
            .unwrap();
        assert_eq!(seen.lock().unwrap()[0]["key"], "  explicit  ");
        assert!(env.calls.lock().unwrap().is_empty());
        // Use a selected environment ID, but a replaced binding avoids the remembered shortcut.
        env.blocked.store(true, Ordering::SeqCst);
        let stream = facade
            .stream(request(
                host.clone(),
                id.as_str(),
                "openrouter",
                Some("\u{feff}  "),
            ))
            .unwrap();
        while env.calls.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
        facade
            .registry()
            .register_api_provider(Fake::arc(id.as_str(), "replacement", seen.clone()), None);
        env.blocked.store(false, Ordering::SeqCst);
        env.release.notify_waiters();
        stream.result().await;
        assert_eq!(seen.lock().unwrap().last().unwrap()["label"], "captured");
        assert_eq!(seen.lock().unwrap().last().unwrap()["key"], "SYNTHETIC_ENV");
        *env.value.lock().unwrap() = Some("<authenticated>".into());
        facade
            .complete_simple(simple(request(
                host.clone(),
                id.as_str(),
                "openrouter",
                Some("  "),
            )))
            .await
            .unwrap();
        assert_eq!(seen.lock().unwrap().last().unwrap()["key"], "  ");
        *env.value.lock().unwrap() = None;
        facade
            .complete(request(host, id.as_str(), "openrouter", None))
            .await
            .unwrap();
        assert_eq!(seen.lock().unwrap().last().unwrap()["key"], Value::Null);
    });
}
#[test]
fn all_sixteen_aliases_reach_real_families_with_typed_raw_options() {
    run(async {
        let host = Host::new();
        let peer = Arc::new(Peer::default());
        let (modules, shared) = configured(host.clone(), peer.clone(), None);
        let aliases = LegacyApiAliases::new(Arc::new(CompatApiModules::new(modules, shared)));
        macro_rules! pair {
            ($raw:ident,$simple:ident,$api:literal,$opts:expr) => {{
                let before = peer.calls.lock().unwrap().len();
                let output = aliases
                    .$raw(request(host.clone(), $api, "custom", Some(JWT)), $opts)
                    .result()
                    .await;
                assert_eq!(output.stop_reason, StopReason::Error, "{}", $api);
                assert_eq!(
                    peer.calls.lock().unwrap().len(),
                    before + 1,
                    "{} raw admission",
                    $api
                );
                let raw = peer.calls.lock().unwrap().last().unwrap()["body"].clone();
                assert!(
                    raw.to_string().contains("none") || raw.to_string().contains("NONE"),
                    "{} typed raw field {:?}",
                    $api,
                    raw
                );
                aliases
                    .$simple(simple(request(host.clone(), $api, "custom", Some(JWT))))
                    .result()
                    .await;
                assert_eq!(
                    peer.calls.lock().unwrap().len(),
                    before + 2,
                    "{} simple admission",
                    $api
                );
            }};
        }
        pair!(
            stream_anthropic,
            stream_simple_anthropic,
            "anthropic-messages",
            serde_json::from_value(json!({"toolChoice":"none"})).unwrap()
        );
        pair!(
            stream_azure_openai_responses,
            stream_simple_azure_openai_responses,
            "azure-openai-responses",
            serde_json::from_value(json!({"toolChoice":"none"})).unwrap()
        );
        pair!(
            stream_google,
            stream_simple_google,
            "google-generative-ai",
            serde_json::from_value(json!({"toolChoice":"none"})).unwrap()
        );
        pair!(
            stream_google_vertex,
            stream_simple_google_vertex,
            "google-vertex",
            serde_json::from_value(
                json!({"toolChoice":"none","project":"synthetic-project","location":"us-central1"})
            )
            .unwrap()
        );
        pair!(
            stream_mistral,
            stream_simple_mistral,
            "mistral-conversations",
            serde_json::from_value(json!({"toolChoice":"none"})).unwrap()
        );
        pair!(
            stream_openai_codex_responses,
            stream_simple_openai_codex_responses,
            "openai-codex-responses",
            serde_json::from_value(json!({"toolChoice":"none"})).unwrap()
        );
        pair!(
            stream_openai_completions,
            stream_simple_openai_completions,
            "openai-completions",
            serde_json::from_value(json!({"toolChoice":"none"})).unwrap()
        );
        pair!(
            stream_openai_responses,
            stream_simple_openai_responses,
            "openai-responses",
            serde_json::from_value(json!({"toolChoice":"none"})).unwrap()
        );
        assert_eq!(peer.calls.lock().unwrap().len(), 16);
    });
}
#[test]
fn image_upsert_cached_module_and_no_env_preserve_distinct_registry() {
    run(async {
        let host = Host::new();
        let peer = Arc::new(Peer::default());
        let env = Env::new(Some("SHOULD_NOT_BE_READ"));
        let (modules, shared) = configured(host.clone(), peer.clone(), None);
        let facade = CompatFacade::initialize(
            CompatApiRegistry::new(),
            modules,
            shared,
            options(host.clone(), env.clone()),
        )
        .await
        .unwrap();
        let api: ImagesApiId = "openrouter-images".parse().unwrap();
        let before = facade.images_registry().get(&api).unwrap();
        facade.register_builtin_images_api_providers();
        assert!(!Arc::ptr_eq(
            &before,
            &facade.images_registry().get(&api).unwrap()
        ));
        let spawns_before = host.spawns.load(Ordering::SeqCst);
        for index in 0..2 {
            if index == 1 {
                facade.register_builtin_images_api_providers();
            }
            let model = get_image_models("openrouter").unwrap()[0].clone();
            let request = ImageGenerationRequest {
                resolved: ResolvedRequest {
                    model,
                    endpoint: RequestEndpoint::new_resolved("https://synthetic.invalid/v1")
                        .unwrap(),
                },
                context: serde_json::from_value(
                    json!({"input":[{"type":"text","text":"synthetic"}]}),
                )
                .unwrap(),
                auth: ResolvedAuth {
                    api_key: Some(SecretString::new("SYNTHETIC_IMAGE")),
                    ..Default::default()
                },
                options: ImagesOptions {
                    wire: Default::default(),
                    runtime: runtime(host.clone()),
                },
            };
            assert_eq!(
                facade.generate_images(request).await.unwrap().stop_reason,
                ImagesStopReason::Stop
            );
        }
        assert_eq!(peer.calls.lock().unwrap().len(), 2);
        assert_eq!(host.spawns.load(Ordering::SeqCst), spawns_before + 1);
        assert!(env.calls.lock().unwrap().is_empty());
        assert_eq!(get_image_providers(), vec!["openrouter"]);
        assert!(get_models("excluded").unwrap().is_empty());
    });
}
#[derive(Default)]
struct Queue {
    mode: AtomicUsize,
    tasks: Mutex<Vec<BoxFuture<'static, ()>>>,
}
impl TaskSpawner for Queue {
    fn spawn(&self, f: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
        match self.mode.load(Ordering::SeqCst) {
            1 => Err(SpawnError::new()),
            2 => Ok(()),
            _ => {
                self.tasks.lock().unwrap().push(f);
                return Ok(());
            }
        }
    }
}
impl Queue {
    async fn drive(&self) {
        let tasks = std::mem::take(&mut *self.tasks.lock().unwrap());
        for task in tasks {
            task.await;
        }
    }
}
fn poll<T>(future: &mut std::pin::Pin<Box<impl Future<Output = T> + ?Sized>>) -> Poll<T> {
    future
        .as_mut()
        .poll(&mut TaskContext::from_waker(Waker::noop()))
}
#[test]
fn cleanup_tickets_preserve_each_failure_task_drop_and_two_explicit_scopes() {
    run(async {
        let host = Host::new();
        let peer = Arc::new(Peer::default());
        let (modules, _) = configured(host.clone(), peer, None);
        let registry = SessionResourceRegistry::new();
        let queue = Arc::new(Queue::default());
        let mut rt = runtime(host);
        rt.spawner = queue.clone();
        let scope = register_codex_cleanup_scope(&registry, modules.codex(), rt.clone()).unwrap();
        queue.mode.store(1, Ordering::SeqCst);
        assert!(registry.cleanup(Some("s")).is_err());
        let first = scope.take_close_tickets().pop().unwrap();
        let failed = first.clone().await.unwrap_err();
        assert!(registry.cleanup(Some("s")).is_err());
        let second = scope.take_close_tickets().pop().unwrap();
        let failed2 = second.await.unwrap_err();
        assert!(!Arc::ptr_eq(&failed, &failed2));
        queue.mode.store(0, Ordering::SeqCst);
        registry.cleanup(Some("s")).unwrap();
        let ticket = scope.take_close_tickets().pop().unwrap();
        let mut waiting = Box::pin(ticket.clone());
        assert!(poll(&mut waiting).is_pending());
        queue.drive().await;
        assert!(ticket.await.is_ok());
        assert!(first.await.is_err());
        registry.cleanup(Some("s")).unwrap();
        assert!(scope.take_close_tickets().is_empty());
        let scope2 = register_codex_cleanup_scope(&registry, modules.codex(), rt.clone()).unwrap();
        registry.cleanup(Some("second")).unwrap();
        let ticket = scope2.take_close_tickets().pop().unwrap();
        drop(scope2);
        queue.drive().await;
        assert!(ticket.await.is_ok());
        let scope3 = register_codex_cleanup_scope(&registry, modules.codex(), rt).unwrap();
        queue.mode.store(2, Ordering::SeqCst);
        registry.cleanup(None).unwrap();
        assert!(scope3.take_close_tickets().pop().unwrap().await.is_err());
    });
}
#[test]
fn queued_task_lifetime_survives_scope_and_observer_drop() {
    run(async {
        let host = Host::new();
        let (modules, _) = configured(host.clone(), Arc::new(Peer::default()), None);
        let module = modules.codex();
        let weak = Arc::downgrade(&module);
        let queue = Arc::new(Queue::default());
        let mut rt = runtime(host);
        rt.spawner = queue.clone();
        let registry = SessionResourceRegistry::new();
        let scope = register_codex_cleanup_scope(&registry, module, rt).unwrap();
        registry.cleanup(None).unwrap();
        drop(scope);
        drop(modules);
        assert!(weak.upgrade().is_some());
        queue.drive().await;
        assert!(weak.upgrade().is_none());
    });
}
#[test]
fn bedrock_module_retains_accepted_override_and_fallback() {
    run(async {
        let host = Host::new();
        let peer = Arc::new(Peer::default());
        let (modules, shared) = configured(host.clone(), peer.clone(), None);
        let direct_arc = shared.bedrock.clone();
        let api_modules = Arc::new(CompatApiModules::new(modules, shared));
        let aliases = LegacyApiAliases::new(api_modules.clone());
        let registry = CompatApiRegistry::new();
        registry.register_builtin_api_providers(&api_modules);
        let retained = registry
            .get_api_provider(&"bedrock-converse-stream".parse().unwrap())
            .unwrap();
        assert!(Arc::ptr_eq(&aliases.bedrock_provider_module(), &direct_arc));
        let seen = Arc::new(Mutex::new(vec![]));
        pi_ai_providers::api::lazy::set_bedrock_provider_module(Some(Fake::arc(
            "bedrock-converse-stream",
            "override",
            seen.clone(),
        )));
        retained
            .stream(request(
                host.clone(),
                "bedrock-converse-stream",
                "custom",
                Some("synthetic"),
            ))
            .unwrap()
            .result()
            .await;
        pi_ai_providers::api::lazy::set_bedrock_provider_module(None);
        assert_eq!(seen.lock().unwrap().len(), 1);
        assert!(peer.calls.lock().unwrap().is_empty());
        // Removing the override reaches configured Bedrock's own request path.
        retained
            .stream(request(
                host.clone(),
                "bedrock-converse-stream",
                "custom",
                Some("synthetic"),
            ))
            .unwrap()
            .result()
            .await;
        assert_eq!(peer.calls.lock().unwrap().len(), 1);
        aliases
            .bedrock_provider_module()
            .stream_with_options(
                request(
                    host.clone(),
                    "bedrock-converse-stream",
                    "custom",
                    Some("synthetic"),
                ),
                serde_json::from_value(json!({"requestMetadata":{"typed":"direct"}})).unwrap(),
            )
            .result()
            .await;
        assert_eq!(
            peer.calls.lock().unwrap().last().unwrap()["body"]["requestMetadata"]["typed"],
            "direct"
        );
        aliases
            .bedrock_provider_module()
            .stream_simple(simple(request(
                host.clone(),
                "bedrock-converse-stream",
                "custom",
                Some("synthetic"),
            )))
            .result()
            .await;
        assert_eq!(peer.calls.lock().unwrap().len(), 3);
    });
}
#[cfg(feature = "native")]
mod native_close {
    use super::*;
    use std::{net::TcpListener, sync::mpsc, thread};
    use tokio_tungstenite::tungstenite::{Message, accept};
    fn completed(id: &str) -> Message {
        Message::Text(json!({"type":"response.completed","response":{"id":id,"status":"completed","output":[],"usage":{"input_tokens":17,"output_tokens":0,"total_tokens":17}}}).to_string().into())
    }
    fn req(host: Arc<Host>, port: u16) -> TextStreamRequest {
        let mut r = request(host, "openai-codex-responses", "openai-codex", Some(JWT));
        r.resolved.endpoint =
            RequestEndpoint::new_resolved(format!("http://127.0.0.1:{port}/backend-api")).unwrap();
        r.options.wire.transport = Some(Transport::WebSocketCached);
        r.options.wire.session_id = Some("synthetic-session".into());
        r
    }
    async fn reached(value: &AtomicUsize, count: usize) {
        while value.load(Ordering::SeqCst) < count {
            tokio::task::yield_now().await;
        }
    }
    #[test]
    fn actual_facade_busy_close_and_new_same_session_own_distinct_physical_snapshots() {
        run(async {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let port = listener.local_addr().unwrap().port();
            let reached_count = Arc::new(AtomicUsize::new(0));
            let (release_tx, release_rx) = mpsc::channel();
            let stage = reached_count.clone();
            let server = thread::spawn(move || {
                let mut threads = vec![];
                let mut release = Some(release_rx);
                for i in 0..2 {
                    let (socket, _) = listener.accept().unwrap();
                    socket
                        .set_read_timeout(Some(Duration::from_secs(5)))
                        .unwrap();
                    socket
                        .set_write_timeout(Some(Duration::from_secs(5)))
                        .unwrap();
                    let stage = stage.clone();
                    let release = if i == 0 { release.take() } else { None };
                    threads.push(thread::spawn(move || {
                        let mut ws = accept(socket).unwrap();
                        assert!(ws.read().unwrap().is_text());
                        stage.fetch_add(1, Ordering::SeqCst);
                        if let Some(release) = release {
                            release.recv_timeout(Duration::from_secs(5)).unwrap();
                        }
                        if i == 1 {
                            ws.send(completed("second")).unwrap();
                        }
                        let close = ws.read().unwrap();
                        let Message::Close(frame) = close else {
                            panic!("expected close frame")
                        };
                        let frame = frame.unwrap();
                        assert_eq!(u16::from(frame.code), 1000);
                        assert_eq!(frame.reason, "debug_close");
                        let _ = ws.flush();
                    }));
                }
                for t in threads {
                    t.join().unwrap();
                }
            });
            let host = Host::new();
            let connector =
                Arc::new(Node26WebSocketConnector::new(tokio::runtime::Handle::current()).unwrap());
            let (modules, shared) =
                configured(host.clone(), Arc::new(Peer::default()), Some(connector));
            let facade = CompatFacade::initialize(
                CompatApiRegistry::new(),
                modules.clone(),
                shared,
                options(host.clone(), Env::new(None)),
            )
            .await
            .unwrap();
            let first = facade
                .aliases()
                .stream_openai_codex_responses(req(host.clone(), port), Default::default());
            reached(&reached_count, 1).await;
            let resources = facade.session_resources();
            let mut first_close =
                resources.begin_close(Some("synthetic-session"), runtime(host.clone()));
            assert!(poll(&mut first_close).is_pending());
            assert!(
                resources
                    .begin_close(Some("synthetic-session"), runtime(host.clone()))
                    .await
                    .is_ok()
            );
            let second = facade
                .aliases()
                .stream_simple_openai_codex_responses(simple(req(host.clone(), port)));
            reached(&reached_count, 2).await;
            assert_eq!(second.result().await.stop_reason, StopReason::Stop);
            let registry = SessionResourceRegistry::new();
            let scope =
                register_codex_cleanup_scope(&registry, modules.codex(), runtime(host.clone()))
                    .unwrap();
            registry.cleanup(Some("synthetic-session")).unwrap();
            assert!(scope.take_close_tickets().pop().unwrap().await.is_ok());
            assert!(poll(&mut first_close).is_pending());
            release_tx.send(()).unwrap();
            assert!(first_close.await.is_ok());
            assert_eq!(first.result().await.stop_reason, StopReason::Error);
            let stats = resources.stats("synthetic-session").unwrap();
            assert_eq!(stats.connections_created.get(), 2);
            assert!(Arc::ptr_eq(&modules.codex(), &resources.module()));
            resources.reset_stats(Some("synthetic-session"));
            assert!(resources.stats("synthetic-session").is_none());
            server.join().unwrap();
        });
    }
    #[test]
    fn never_polled_facade_close_drop_aborts_retired_native_socket() {
        run(async {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let port = listener.local_addr().unwrap().port();
            let disconnected = Arc::new(AtomicBool::new(false));
            let peer_closed = disconnected.clone();
            let server = thread::spawn(move || {
                let (socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut ws = accept(socket).unwrap();
                assert!(ws.read().unwrap().is_text());
                ws.send(completed("retired")).unwrap();
                assert!(
                    ws.read().is_err(),
                    "never-polled drop aborts, not graceful close"
                );
                peer_closed.store(true, Ordering::SeqCst);
            });
            let host = Host::new();
            let connector =
                Arc::new(Node26WebSocketConnector::new(tokio::runtime::Handle::current()).unwrap());
            let (modules, shared) =
                configured(host.clone(), Arc::new(Peer::default()), Some(connector));
            let facade = CompatFacade::initialize(
                CompatApiRegistry::new(),
                modules,
                shared,
                options(host.clone(), Env::new(None)),
            )
            .await
            .unwrap();
            assert_eq!(
                facade
                    .aliases()
                    .stream_openai_codex_responses(req(host.clone(), port), Default::default())
                    .result()
                    .await
                    .stop_reason,
                StopReason::Stop
            );
            facade
                .session_resources()
                .reset_stats(Some("synthetic-session"));
            assert!(
                facade
                    .session_resources()
                    .stats("synthetic-session")
                    .is_none()
            );
            assert!(!disconnected.load(Ordering::SeqCst));
            let close = facade
                .session_resources()
                .begin_close(None, runtime(host.clone()));
            assert!(
                facade
                    .session_resources()
                    .begin_close(None, runtime(host))
                    .await
                    .is_ok()
            );
            assert!(!disconnected.load(Ordering::SeqCst));
            drop(close);
            while !disconnected.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
            server.join().unwrap();
        });
    }
}

impl RawHttpClient for Peer {
    fn send_smithy<'a>(
        &'a self,
        r: &'a HttpRequest,
        rt: &'a RequestRuntime,
        _: &'a SmithyRequestProfile,
    ) -> BoxFuture<'a, Result<HttpResponse, TransportError>> {
        self.send(r, rt)
    }
    fn send<'a>(
        &'a self,
        r: &'a HttpRequest,
        rt: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<HttpResponse, TransportError>> {
        Box::pin(async move {
            let logical = FetchRequest {
                endpoint: RequestEndpoint::new_resolved(r.endpoint.expose_for_transport()).unwrap(),
                method: r.method.clone(),
                headers: FetchHeaders::Constructed(r.headers.clone()),
                body: FetchBody::Bytes(r.body.clone()),
                proxy: r.proxy.clone(),
            };
            self.fetch(&logical, rt).await
        })
    }
}
struct ReentrantAdapter {
    inner: Fake,
    registry: std::sync::Weak<CompatApiRegistry>,
    drops: Arc<AtomicUsize>,
}
impl Drop for ReentrantAdapter {
    fn drop(&mut self) {
        if let Some(registry) = self.registry.upgrade() {
            registry.get_api_providers();
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }
}
impl ApiAdapter for ReentrantAdapter {
    fn api_id(&self) -> &ApiId {
        if let Some(registry) = self.registry.upgrade() {
            registry.get_api_providers();
        }
        &self.inner.api
    }
    fn stream(&self, r: TextStreamRequest) -> AssistantStream {
        self.inner.stream(r)
    }
    fn stream_simple(&self, r: SimpleStreamRequest) -> AssistantStream {
        self.inner.stream_simple(r)
    }
    fn fetch_deferred(&self, r: DeferredFetchRequest) -> Result<AssistantStream, CapabilityError> {
        self.inner.fetch_deferred(r)
    }
    fn cancel_deferred<'a>(
        &'a self,
        r: DeferredCancelRequest,
    ) -> BoxFuture<'a, Result<(), ModelsError>> {
        self.inner.cancel_deferred(r)
    }
}
#[test]
fn registry_host_identity_and_displaced_destructor_reenter_outside_lock() {
    let registry = Arc::new(CompatApiRegistry::new());
    let drops = Arc::new(AtomicUsize::new(0));
    let seen = Arc::new(Mutex::new(vec![]));
    registry.register_api_provider(
        Arc::new(ReentrantAdapter {
            inner: (*Fake::arc("custom", "first", seen.clone())).clone(),
            registry: Arc::downgrade(&registry),
            drops: drops.clone(),
        }),
        Some("old".into()),
    );
    registry.register_api_provider(Fake::arc("custom", "next", seen), None);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}
struct ReenterWake {
    ticket: Mutex<Option<CodexCloseTicket>>,
    seen: Arc<AtomicUsize>,
}
impl std::task::Wake for ReenterWake {
    fn wake(self: Arc<Self>) {
        let mut ticket = Box::pin(self.ticket.lock().unwrap().take().unwrap());
        assert!(poll(&mut ticket).is_ready());
        self.seen.fetch_add(1, Ordering::SeqCst);
    }
}
#[test]
fn close_ticket_completion_wakes_reentrant_observer_outside_lock() {
    run(async {
        let host = Host::new();
        let (modules, _) = configured(host.clone(), Arc::new(Peer::default()), None);
        let queue = Arc::new(Queue::default());
        let mut rt = runtime(host);
        rt.spawner = queue.clone();
        let registry = SessionResourceRegistry::new();
        let scope = register_codex_cleanup_scope(&registry, modules.codex(), rt).unwrap();
        registry.cleanup(None).unwrap();
        let mut ticket = Box::pin(scope.take_close_tickets().pop().unwrap());
        let seen = Arc::new(AtomicUsize::new(0));
        let waker = Waker::from(Arc::new(ReenterWake {
            ticket: Mutex::new(Some(ticket.as_ref().get_ref().clone())),
            seen: seen.clone(),
        }));
        assert!(
            ticket
                .as_mut()
                .poll(&mut TaskContext::from_waker(&waker))
                .is_pending()
        );
        drop(waker);
        queue.drive().await;
        assert_eq!(seen.load(Ordering::SeqCst), 1);
        assert!(ticket.await.is_ok());
    });
}
