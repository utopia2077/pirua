#![cfg(all(feature = "all-providers", feature = "all-images"))]
// Compile the exact owning facade body without introducing a reverse dev edge.
#[path = "../../pi-ai/src/builtins.rs"]
mod builtins;
use builtins::{BuiltinProviderModules, BuiltinProviderServices, FactoryAuthServices};
use pi_ai_core::*;
use pi_ai_providers::{
    api::{openai_codex_responses::*, runtime::AdapterServices},
    auth::OAuthServices,
    images::*,
    providers::captured_generated_at,
};
use pi_ai_transport::*;
use serde_json::{Value, json};
use std::{
    future::pending,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
struct Host {
    effects: AtomicUsize,
}
impl pi_ai_core::Clock for Host {
    fn now_unix_ms(&self) -> u64 {
        1700000000000
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
    fn spawn(&self, f: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
        self.effects.fetch_add(1, Ordering::SeqCst);
        tokio::spawn(f);
        Ok(())
    }
}
impl AuthContext for Host {
    fn env<'a>(
        &'a self,
        _: &'a str,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<String>, AuthContextError>> {
        self.effects.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(None) })
    }
    fn file_exists<'a>(
        &'a self,
        _: &'a str,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<bool, AuthContextError>> {
        panic!("no file discovery")
    }
}
impl UuidV7Entropy for Host {
    fn random_bytes(&self) -> Result<[u8; 16], UuidV7EntropyError> {
        self.effects.fetch_add(1, Ordering::SeqCst);
        Ok([1; 16])
    }
}
impl RetryJitter for Host {
    fn sample(&self) -> f64 {
        0.5
    }
}
impl OAuthCrypto for Host {
    fn fill_random(&self, _: &mut [u8]) -> Result<(), AuthHostError> {
        panic!("no OAuth entropy on passive or toAuth path")
    }
    fn sha256(&self, _: &[u8]) -> Result<[u8; 32], AuthHostError> {
        panic!("no OAuth hash on passive or toAuth path")
    }
}
impl CallbackListener for Host {
    fn bind<'a>(
        &'a self,
        _: CallbackBind,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Box<dyn CallbackSession>, AuthHostError>> {
        panic!("no listener on passive or toAuth path")
    }
}
struct Body {
    bytes: Option<Vec<u8>>,
    cancellation: Option<CancellationToken>,
    polls: Arc<AtomicUsize>,
}
impl ResponseBody for Body {
    fn next<'a>(
        &'a mut self,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, TransportError>> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        if let Some(bytes) = self.bytes.take() {
            return Box::pin(async { Ok(Some(bytes)) });
        }
        if let Some(c) = self.cancellation.take() {
            c.cancel();
            return Box::pin(pending());
        }
        Box::pin(async { Ok(None) })
    }
    fn close(&mut self) {}
}
#[derive(Default)]
struct Peer {
    calls: Mutex<Vec<Value>>,
    cancellation: bool,
    polls: Arc<AtomicUsize>,
}
impl CustomFetch for Peer {
    fn fetch<'a>(
        &'a self,
        r: &'a FetchRequest,
        rt: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<HttpResponse, TransportError>> {
        Box::pin(async move {
            let body = match &r.body {
                FetchBody::Text(s) => serde_json::from_str::<Value>(s).unwrap(),
                FetchBody::Bytes(s) => serde_json::from_slice(s).unwrap(),
                FetchBody::Absent => panic!("factory sends body"),
            };
            let endpoint = r.endpoint.expose_for_transport();
            self.calls
                .lock()
                .unwrap()
                .push(json!({"endpoint":endpoint,"body":body}));
            let image = body.get("modalities").is_some();
            let raw = if image {
                json!({"id":"image-1","choices":[{"message":{"content":"synthetic-image","images":[{"image_url":"data:image/png;base64,A"}]}}]}).to_string()
            } else if endpoint.contains("anthropic.com") {
                "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"m1\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[],\"model\":\"synthetic\",\"usage\":{\"input_tokens\":17,\"output_tokens\":0}}}\n\nevent: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":0}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n".into()
            } else if endpoint.contains("generativelanguage") {
                "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"synthetic\"}],\"role\":\"model\"},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":17,\"candidatesTokenCount\":0,\"totalTokenCount\":17}}\n\n".into()
            } else if body.get("input").is_some() {
                format!(
                    "data: {}\n\n",
                    json!({"type":"response.completed","response":{"id":"r1","status":"completed","output":[],"usage":{"input_tokens":17,"output_tokens":0,"total_tokens":17}}})
                )
            } else {
                let chunk = json!({"id":"c1","choices":[],"usage":{"prompt_tokens":17,"completion_tokens":0,"total_tokens":17}});
                if self.cancellation {
                    format!("data: {chunk}\n\n")
                } else {
                    format!(
                        "data: {chunk}\n\ndata: {{\"id\":\"c1\",\"choices\":[{{\"index\":0,\"delta\":{{}},\"finish_reason\":\"stop\"}}]}}\n\ndata: [DONE]\n\n"
                    )
                }
            };
            let mut headers = HttpHeaders::new();
            headers
                .set(
                    "content-type",
                    if image {
                        "application/json"
                    } else {
                        "text/event-stream"
                    },
                )
                .unwrap();
            Ok(HttpResponse {
                status: 200,
                status_text: "OK".into(),
                headers,
                body: Box::new(Body {
                    bytes: Some(raw.into_bytes()),
                    cancellation: self.cancellation.then(|| rt.cancellation.clone()),
                    polls: self.polls.clone(),
                }),
            })
        })
    }
}
fn modules(host: Arc<Host>, peer: Arc<Peer>, oauth: bool) -> BuiltinProviderModules {
    let transport = Arc::new(HttpTransport::new_custom(peer, host.clone()));
    let adapter = AdapterServices::new(transport.clone());
    let oauth = oauth.then(|| OAuthServices {
        transport: transport.clone(),
        clock: host.clone(),
        spawner: host.clone(),
        crypto: host.clone(),
        callbacks: host.clone(),
        callback_host: "127.0.0.1".into(),
        callback_limits: CallbackLimits::default(),
        max_token_response_bytes: 65536,
    });
    BuiltinProviderModules::new(BuiltinProviderServices {
        anthropic: adapter.clone(),
        google: adapter.clone(),
        openai: adapter.clone(),
        completions: adapter.clone(),
        codex: CodexServices {
            adapter,
            websocket: None,
            request_ids: Arc::new(UuidV7Generator::new(host.clone(), host.clone())),
            date_parser: Arc::new(HttpRetryDateParser),
            signal_presence: CodexSourceSignalPresence::Present,
            compression: None,
            limits: CodexResourceLimits::default(),
        },
        images: ImageAdapterServices::new(transport),
        auth: FactoryAuthServices {
            spawner: host,
            oauth,
        },
    })
    .unwrap()
}
fn options(host: Arc<Host>) -> ModelsRegistryOptions {
    ModelsRegistryOptions {
        credentials: Arc::new(InMemoryCredentialStore::new()),
        models_store: Box::new(InMemoryModelsStore::new()),
        auth_context: host.clone(),
        clock: host.clone(),
        spawner: host,
        model_header_policy: ModelHeaderPolicy::default(),
    }
}
fn runtime(host: Arc<Host>) -> RequestRuntime {
    RequestRuntime::with_noop_telemetry(CancellationToken::new(), host.clone(), host)
}
fn oracle() -> Value {
    serde_json::from_str(include_str!(
        "../../../conformance/providers/selected/trace.json"
    ))
    .unwrap()
}
fn host() -> Arc<Host> {
    Arc::new(Host {
        effects: AtomicUsize::new(0),
    })
}
#[test]
fn source_metadata_catalog_order_static_capabilities_and_passive_modules() {
    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
    let host = host();
    let peer = Arc::new(Peer::default());
    let m = modules(host.clone(), peer.clone(), true);
    let providers = builtins::builtin_providers(&m).unwrap();
    let images = builtins::builtin_images_providers(&m).unwrap();
    assert_eq!(host.effects.load(Ordering::SeqCst), 0);
    assert!(peer.calls.lock().unwrap().is_empty());
    assert!(Arc::ptr_eq(&m.codex(), &m.codex()));
    assert!(Arc::ptr_eq(&m.completions(), &m.completions()));
    let fresh = builtins::builtin_providers(&m).unwrap();
    assert!(
        providers
            .iter()
            .zip(fresh)
            .all(|(a, b)| !Arc::ptr_eq(a, &b))
    );
    let expected = oracle();
    for (p, row) in providers
        .iter()
        .zip(expected["observation"]["rows"].as_array().unwrap())
    {
        assert_eq!(p.id().as_str(), row["id"]);
        assert_eq!(p.name(), row["name"]);
        assert!(p.models_refresher().is_none());
        assert_eq!(
            p.auth().api_key().map(|k| k.name().to_owned()),
            row["key"].as_str().map(str::to_owned)
        );
        assert_eq!(p.auth().oauth().map(|o|json!({"name":o.name(),"isSubscription":o.is_subscription(),"loginLabel":o.login_label()})),if row["oauth"].is_null(){None}else{Some(row["oauth"].clone())});
        assert_eq!(
            p.models()
                .unwrap()
                .iter()
                .map(|m| m.id.as_str())
                .collect::<Vec<_>>(),
            row["models"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_str().unwrap())
                .collect::<Vec<_>>()
        );
        assert!(
            p.adapter(&ApiId::new("synthetic-unknown").unwrap())
                .is_none()
        );
        if let Some(key)=p.auth().api_key(){
            let context=KeyEnv(Mutex::new(Vec::new()));
            let c=CancellationToken::new();
            let resolved=key.resolve(ApiKeyAuthInput{context:&context,credential:None,cancellation:&c}).await.unwrap().unwrap();
            assert_eq!(json!(*context.0.lock().unwrap()),row["envCalls"]);
            assert_eq!(resolved.source.as_deref(),row["authSource"].as_str());
        }

    }
    assert_eq!(images.len(), 1);
    assert_eq!(images[0].id().as_str(), "openrouter");
    assert!(images[0].models_refresher().is_none());
    assert_eq!(images[0].models().unwrap().len(), 52);
    let image_row=&expected["observation"]["rows"][6];
    assert_eq!(images[0].auth().api_key().unwrap().name(),image_row["key"].as_str().unwrap());
    let image_oauth=images[0].auth().oauth().unwrap();
    assert_eq!(json!({"name":image_oauth.name(),"isSubscription":image_oauth.is_subscription(),"loginLabel":image_oauth.login_label()}),image_row["oauth"]);
    let codex=m.codex();let weak=Arc::downgrade(&codex);assert_eq!(Arc::strong_count(&codex),3);drop(codex);drop(m);
    assert!(weak.upgrade().is_some());drop(providers);assert!(weak.upgrade().is_none());

    assert_eq!(
        captured_generated_at().unwrap().get(),
        expected["observation"]["generatedAt"].as_u64().unwrap()
    );
 });
}
#[test]
fn awaited_builtin_registration_and_each_selected_factory_reaches_its_wire() {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let host = host();
            let peer = Arc::new(Peer::default());
            let m = modules(host.clone(), peer.clone(), true);
            let mut registry_options = options(host.clone());
            let store=Arc::new(InMemoryCredentialStore::new());
            let credential: Credential=serde_json::from_value(json!({"type":"oauth","access":"e30.eyJodHRwczovL2FwaS5vcGVuYWkuY29tL2F1dGgiOiB7ImNoYXRncHRfYWNjb3VudF9pZCI6ICJzeW50aGV0aWMtYWNjb3VudCJ9fQ.synthetic","refresh":"SYNTHETIC_REFRESH","expires":1700003600000u64})).unwrap();
            store.begin_mutation(&ProviderId::new("openai-codex").unwrap(), &CancellationToken::new()).await.unwrap().commit(CredentialMutationValue::Set(credential)).await.unwrap();
            let key:Credential=serde_json::from_value(json!({"type":"api_key","key":"SYNTHETIC_SHARED_OPENROUTER"})).unwrap();
            store.begin_mutation(&ProviderId::new("openrouter").unwrap(), &CancellationToken::new()).await.unwrap().commit(CredentialMutationValue::Set(key)).await.unwrap();
            registry_options.credentials=store.clone();
            let registry = builtins::builtin_models(&m, registry_options)
                .await
                .unwrap();
            assert_eq!(
                registry
                    .get_providers()
                    .iter()
                    .map(|p| p.id().as_str())
                    .collect::<Vec<_>>(),
                vec![
                    "anthropic",
                    "deepseek",
                    "google",
                    "openai",
                    "openai-codex",
                    "openrouter"
                ]
            );
            for p in registry.get_providers() {
                let model = p.models().unwrap()[0].clone();
                let id = model.id.to_string();
                let result = registry
                    .complete(
                        model,
                        serde_json::from_value(json!({"messages":[{"role":"user","content":"synthetic","timestamp":7}]})).unwrap(),
                        ModelsRequestOverrides {
                            auth: AuthResolutionOverrides {
                                api_key: (!matches!(p.id().as_str(), "openai-codex" | "openrouter")).then(||SecretString::new("e30.eyJodHRwczovL2FwaS5vcGVuYWkuY29tL2F1dGgiOiB7ImNoYXRncHRfYWNjb3VudF9pZCI6ICJzeW50aGV0aWMtYWNjb3VudCJ9fQ.synthetic")),
                                ..Default::default()
                            },
                            ..Default::default()
                        },
                        StreamOptions {
                            wire: StreamOptionsWire::default(),
                            runtime: runtime(host.clone()),
                        },
                    )
                    .await;
                if p.id().as_str()=="openai-codex" && !cfg!(feature="oauth-openai-codex") { assert_eq!(result.stop_reason, StopReason::Error); assert_eq!(peer.calls.lock().unwrap().len(),4); continue; }
                assert_eq!(
                    result.stop_reason,
                    StopReason::Stop,
                    "{}: {:?}; calls={:?}",
                    p.id(),
                    result, peer.calls.lock().unwrap()
                );
                assert_eq!(result.usage.total_tokens.get(), 17, "{}", p.id());
                let calls = peer.calls.lock().unwrap();
                if p.id().as_str() == "google" { assert!(calls.last().unwrap()["endpoint"].as_str().unwrap().contains(&id)); } else { assert_eq!(calls.last().unwrap()["body"]["model"], id); }
            }
            let images = builtins::builtin_images_models(
                &m,
                ImagesModelsOptions {
                    credentials: Some(store),
                    auth_context: host.clone(),
                    clock: host.clone(),
                },
            )
            .unwrap();
            let result = images
                .generate_images(
                    images.get_models(None)[0].clone(),
                    serde_json::from_value(json!({"input":[{"type":"text","text":"synthetic"}]}))
                        .unwrap(),
                    ImagesOptions {
                        wire: ImagesOptionsWire::default(),
                        runtime: runtime(host),
                    },
                    ImagesRequestOverrides {
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            assert_eq!(result.stop_reason, ImagesStopReason::Stop);
            assert_eq!(peer.calls.lock().unwrap().len(), if cfg!(feature="oauth-openai-codex") {7} else {6});
        });
}
#[test]
fn builtin_child_cancellation_retains_usage17_after_real_completions_handoff() {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let host = host();
            let peer = Arc::new(Peer {
                cancellation: true,
                ..Default::default()
            });
            let m = modules(host.clone(), peer.clone(), false);
            let registry = builtins::builtin_models(&m, options(host.clone()))
                .await
                .unwrap();
            let model = registry.get_models(Some(&ProviderId::new("deepseek").unwrap()))[0].clone();
            let rt = runtime(host);
            let original = rt.cancellation.clone();
            let result = registry
                .complete(
                    model,
                    serde_json::from_value(json!({"messages":[{"role":"user","content":"synthetic","timestamp":7}]})).unwrap(),
                    ModelsRequestOverrides {
                        auth: AuthResolutionOverrides {
                            api_key: Some(SecretString::new("e30.eyJodHRwczovL2FwaS5vcGVuYWkuY29tL2F1dGgiOiB7ImNoYXRncHRfYWNjb3VudF9pZCI6ICJzeW50aGV0aWMtYWNjb3VudCJ9fQ.synthetic")),
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                    StreamOptions {
                        wire: StreamOptionsWire::default(),
                        runtime: rt,
                    },
                )
                .await;
            assert_eq!(peer.calls.lock().unwrap().len(), 1);
            assert!(peer.polls.load(Ordering::SeqCst) >= 2);
            assert!(original.is_cancelled());
            assert_eq!(result.stop_reason, StopReason::Aborted);
            assert_eq!(result.usage.total_tokens.get(), 17);
            // Causal control: the same real factory/wire through accepted default
            // registry cancellation settles the source-divergent usage0 fallback.
            let default_host=Arc::new(Host{effects:AtomicUsize::new(0)});let default_peer=Arc::new(Peer{cancellation:true,..Default::default()});
            let default_modules=modules(default_host.clone(),default_peer.clone(),false);
            let legacy=ModelsRegistry::new(options(default_host.clone()));
            let provider=builtins::builtin_providers(&default_modules).unwrap().into_iter().find(|p|p.id().as_str()=="deepseek").unwrap();
            legacy.set_provider(provider).await.unwrap();
            let result=legacy.complete(legacy.get_models(None)[0].clone(),serde_json::from_value(json!({"messages":[{"role":"user","content":"synthetic","timestamp":7}]})).unwrap(),ModelsRequestOverrides{auth:AuthResolutionOverrides{api_key:Some(SecretString::new("SYNTHETIC_CONTROL")),..Default::default()},..Default::default()},StreamOptions{wire:Default::default(),runtime:runtime(default_host)}).await;
            assert_eq!(default_peer.calls.lock().unwrap().len(),1);
            assert_eq!(result.stop_reason,StopReason::Aborted);assert_eq!(result.usage.total_tokens.get(),0);

        });
}
#[test]
fn selected_lazy_capabilities_and_optional_oauth_loads_remain_separate() {
    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
  for supplied in [false,true] {
   let host=host();let peer=Arc::new(Peer::default());let m=modules(host.clone(),peer.clone(),supplied);
   for p in builtins::builtin_providers(&m).unwrap() {
    let model=p.models().unwrap()[0].clone();let adapter=p.adapter(&model.api).unwrap();
    let handle:DeferredHandle=serde_json::from_value(json!({"provider":p.id(),"modelId":model.id,"api":model.api,"id":"synthetic-deferred"})).unwrap();
    assert!(adapter.fetch_deferred(DeferredFetchRequest{resolved:ResolvedRequest{model:model.clone(),endpoint:RequestEndpoint::new_resolved("https://synthetic.invalid").unwrap()},handle:handle.clone(),auth:ResolvedAuth::default(),options:DeferredFetchOptions{wire:Default::default(),runtime:runtime(host.clone())}}).is_err());
    assert!(adapter.cancel_deferred(DeferredCancelRequest{resolved:ResolvedRequest{model,endpoint:RequestEndpoint::new_resolved("https://synthetic.invalid").unwrap()},handle,auth:ResolvedAuth::default(),options:DeferredCancelOptions{wire:Default::default(),runtime:runtime(host.clone())}}).await.is_err());
    assert!(peer.calls.lock().unwrap().is_empty());
    if let Some(oauth)=p.auth().oauth(){
     let credential:OAuthCredential=serde_json::from_value(json!({"type":"oauth","access":"SYNTHETIC_ACCESS","refresh":"SYNTHETIC_REFRESH","expires":1700003600000u64})).unwrap();
     let c=CancellationToken::new();
     let enabled=match p.id().as_str(){"anthropic"=>cfg!(feature="oauth-anthropic"),"openai-codex"=>cfg!(feature="oauth-openai-codex"),"openrouter"=>cfg!(feature="oauth-openrouter"),_=>panic!("unexpected OAuth")};
     let before=host.effects.load(Ordering::SeqCst);
     for _ in 0..2 {let result=oauth.to_auth(&credential,&c).await;assert_eq!(result.is_ok(),supplied&&enabled);if let Ok(v)=result{assert_eq!(v.api_key.unwrap().expose_secret(),"SYNTHETIC_ACCESS");}}
     assert_eq!(host.effects.load(Ordering::SeqCst),before+1,"one LazyOAuth task for both load outcomes");
    }
   }
  }
 });
}

struct RejectSpawner;
impl TaskSpawner for RejectSpawner {
    fn spawn(&self, _: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
        Err(SpawnError::new())
    }
}
#[test]
fn facade_awaits_registration_failure_before_returning_a_registry() {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let host = host();
            let peer = Arc::new(Peer::default());
            let m = modules(host.clone(), peer.clone(), false);
            let mut o = options(host);
            o.spawner = Arc::new(RejectSpawner);
            assert!(matches!(
                builtins::builtin_models(&m, o).await,
                Err(builtins::BuiltinCompositionError::Registration(_))
            ));
            assert!(peer.calls.lock().unwrap().is_empty());
        });
}

struct KeyEnv(Mutex<Vec<String>>);
impl AuthContext for KeyEnv {
    fn env<'a>(
        &'a self,
        name: &'a str,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<String>, AuthContextError>> {
        self.0.lock().unwrap().push(name.into());
        Box::pin(
            async move { Ok((name != "ANTHROPIC_AUTH_TOKEN").then(|| "SYNTHETIC_KEY".into())) },
        )
    }
    fn file_exists<'a>(
        &'a self,
        _: &'a str,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<bool, AuthContextError>> {
        panic!("factory key helpers do not inspect files")
    }
}
