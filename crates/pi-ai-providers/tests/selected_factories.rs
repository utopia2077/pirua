#![cfg(feature = "all-providers")]
// Compile the exact owning facade body without introducing a reverse dev edge.
#[path = "../../pi-ai/src/builtins.rs"]
mod builtins;
use builtins::{BuiltinProviderModules, BuiltinProviderServices, FactoryAuthServices};
use pi_ai_core::*;
use pi_ai_providers::{api::runtime::AdapterServices, auth::OAuthServices};
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
            let raw = if endpoint.contains("anthropic.com") {
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
            headers.set("content-type", "text/event-stream").unwrap();
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
        device_id: None,
        agent_name: None,
        callback_limits: CallbackLimits::default(),
        max_token_response_bytes: 65536,
    });
    BuiltinProviderModules::new(BuiltinProviderServices {
        anthropic: adapter.clone(),
        google: adapter.clone(),
        openai: adapter.clone(),
        completions: adapter.clone(),
        auth: FactoryAuthServices {
            spawner: host,
            oauth,
        },
    })
    .unwrap()
}
fn options(host: Arc<Host>) -> ModelsRegistryOptions {
    ModelsRegistryOptions {
        identity: None,
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
fn host() -> Arc<Host> {
    Arc::new(Host {
        effects: AtomicUsize::new(0),
    })
}

#[test]
fn four_selected_factories_are_passive_and_expose_exact_auth() {
    let host = host();
    let peer = Arc::new(Peer::default());
    let modules = modules(host.clone(), peer.clone(), true);
    let providers = builtins::builtin_providers(&modules).unwrap();
    assert_eq!(
        providers
            .iter()
            .map(|provider| provider.id().as_str())
            .collect::<Vec<_>>(),
        ["anthropic", "deepseek", "google", "openai"]
    );
    assert_eq!(host.effects.load(Ordering::SeqCst), 0);
    assert!(peer.calls.lock().unwrap().is_empty());
    for provider in &providers {
        assert!(provider.auth().api_key().is_some());
        assert_eq!(
            provider.auth().oauth().is_some(),
            matches!(provider.id().as_str(), "anthropic" | "openai")
        );
        assert!(!provider.models().unwrap().is_empty());
    }
    let openai = providers.last().unwrap().auth().oauth().unwrap();
    assert_eq!(openai.name(), "OpenAI (ChatGPT subscription)");
    assert_eq!(openai.login_label(), Some("Sign in with ChatGPT"));
}
#[test]
fn awaited_selected_registration_reaches_all_four_actual_wires() {
    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
  let host=host();let peer=Arc::new(Peer::default());let modules=modules(host.clone(),peer.clone(),false);let registry=builtins::builtin_models(&modules,options(host.clone())).await.unwrap();
  for provider in registry.get_providers(){let model=provider.models().unwrap()[0].clone();let result=registry.complete(model,serde_json::from_value(json!({"messages":[{"role":"user","content":"synthetic","timestamp":7}]})).unwrap(),ModelsRequestOverrides{auth:AuthResolutionOverrides{api_key:Some(SecretString::new("sk-synthetic")),..Default::default()},..Default::default()},StreamOptions{wire:StreamOptionsWire::default(),runtime:runtime(host.clone())}).await;assert_eq!(result.stop_reason,StopReason::Stop,"{}: {:?}",provider.id(),result);assert_eq!(result.usage.total_tokens.get(),17);}
  assert_eq!(peer.calls.lock().unwrap().len(),4);
 });
}
