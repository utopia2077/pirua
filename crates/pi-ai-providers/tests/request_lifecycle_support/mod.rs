use pi_ai_core::*;
use pi_ai_providers::api::{
    anthropic_messages::*, google_genai::*, openai_completions::*, openai_responses::*, runtime::*,
};
use pi_ai_transport::*;
use serde_json::{Value, json};
use std::{
    collections::VecDeque,
    future::{Future, pending},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

pub fn run(f: impl Future<Output = ()>) {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(f);
}
pub struct Host;
impl Clock for Host {
    fn now_unix_ms(&self) -> u64 {
        7
    }
    fn sleep<'a>(
        &'a self,
        duration: Duration,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), Cancelled>> {
        if duration < Duration::from_secs(2) {
            Box::pin(async { Ok(()) })
        } else {
            Box::pin(pending())
        }
    }
}
impl TaskSpawner for Host {
    fn spawn(&self, task: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
        tokio::spawn(task);
        Ok(())
    }
}
impl AuthContext for Host {
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
impl RetryJitter for Host {
    fn sample(&self) -> f64 {
        0.0
    }
}
impl UuidV7Entropy for Host {
    fn random_bytes(&self) -> Result<[u8; 16], UuidV7EntropyError> {
        Ok([1; 16])
    }
}
impl OAuthCrypto for Host {
    fn fill_random(&self, _: &mut [u8]) -> Result<(), AuthHostError> {
        panic!("no OAuth entropy")
    }
    fn sha256(&self, _: &[u8]) -> Result<[u8; 32], AuthHostError> {
        panic!("no OAuth hash")
    }
}
impl CallbackListener for Host {
    fn bind<'a>(
        &'a self,
        _: CallbackBind,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Box<dyn CallbackSession>, AuthHostError>> {
        panic!("no OAuth listener")
    }
}
pub struct Body {
    pub bytes: Option<Vec<u8>>,
}
impl ResponseBody for Body {
    fn next<'a>(
        &'a mut self,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, TransportError>> {
        let bytes = self.bytes.take();
        Box::pin(async { Ok(bytes) })
    }
    fn close(&mut self) {}
}
pub fn response(status: u16, header: &str, body: String) -> HttpResponse {
    let mut headers = HttpHeaders::new();
    headers.set("content-type", "text/event-stream").unwrap();
    headers.set("x-observed", header).unwrap();
    HttpResponse {
        status,
        status_text: "fixture".into(),
        headers,
        body: Box::new(Body {
            bytes: Some(body.into_bytes()),
        }),
    }
}
pub fn body(family: &str) -> String {
    match family {
  "anthropic"=>"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"m1\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[],\"model\":\"synthetic\",\"usage\":{\"input_tokens\":17,\"output_tokens\":0}}}\n\nevent: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":0}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n".into(),
  "google"=>format!("data: {}\n\n",json!({"candidates":[{"content":{"parts":[{"text":"synthetic"}],"role":"model"},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":17,"candidatesTokenCount":0,"totalTokenCount":17}})),
  "openai"=>format!("data: {}\n\n",json!({"type":"response.completed","response":{"id":"r1","status":"completed","output":[],"usage":{"input_tokens":17,"output_tokens":0,"total_tokens":17}}})),
  _=>format!("data: {}\n\ndata: {}\n\ndata: [DONE]\n\n",json!({"id":"c1","choices":[],"usage":{"prompt_tokens":17,"completion_tokens":0,"total_tokens":17}}),json!({"id":"c1","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]})),
 }
}
#[derive(Default)]
pub struct Peer {
    pub calls: Mutex<Vec<Value>>,
    pub script: Mutex<VecDeque<Result<HttpResponse, TransportError>>>,
    pub sequence: AtomicUsize,
}
impl CustomFetch for Peer {
    fn fetch<'a>(
        &'a self,
        request: &'a FetchRequest,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<HttpResponse, TransportError>> {
        Box::pin(async move {
            let payload: Value = match &request.body {
                FetchBody::Text(v) => serde_json::from_str(v).unwrap(),
                FetchBody::Bytes(v) => serde_json::from_slice(v).unwrap(),
                FetchBody::Absent => panic!("expected payload"),
            };
            let endpoint = request.endpoint.expose_for_transport();
            let family = if endpoint.contains("anthropic") {
                "anthropic"
            } else if endpoint.contains("generativelanguage") {
                "google"
            } else if payload.get("input").is_some() {
                "openai"
            } else {
                "deepseek"
            };
            self.calls.lock().unwrap().push(payload);
            if let Some(response) = self.script.lock().unwrap().pop_front() {
                return response;
            }
            let id = self.sequence.fetch_add(1, Ordering::SeqCst);
            Ok(response(200, &format!("head-{id}"), body(family)))
        })
    }
}
impl AnthropicClient for Peer {
    fn create<'a>(
        &'a self,
        _: &'a TextStreamRequest,
        payload: &'a pi_ai_providers::api::wire_json::WirePayload,
        _: AnthropicClientOptions<'a>,
    ) -> BoxFuture<'a, Result<HttpResponse, ProviderAttemptFailure<AnthropicResponseError>>> {
        Box::pin(async move {
            self.calls.lock().unwrap().push(payload.as_value().clone());
            Ok(self.script.lock().unwrap().pop_front().unwrap().unwrap())
        })
    }
}
pub fn runtime() -> RequestRuntime {
    RequestRuntime::with_noop_telemetry(CancellationToken::new(), Arc::new(Host), Arc::new(Host))
}
pub fn context() -> pi_ai_core::Context {
    serde_json::from_value(
        json!({"messages":[{"role":"user","content":"synthetic","timestamp":7}]}),
    )
    .unwrap()
}
pub fn overrides() -> ModelsRequestOverrides {
    ModelsRequestOverrides {
        auth: AuthResolutionOverrides {
            api_key: Some(SecretString::new(
                "e30.eyJodHRwczovL2FwaS5vcGVuYWkuY29tL2F1dGgiOiB7ImNoYXRncHRfYWNjb3VudF9pZCI6ICJzeW50aGV0aWMtYWNjb3VudCJ9fQ.synthetic",
            )),
            ..Default::default()
        },
        ..Default::default()
    }
}
pub fn wire() -> StreamOptionsWire {
    StreamOptionsWire {
        transport: Some(Transport::Sse),
        max_retries: Some(SafeUnsigned::new(0).unwrap()),
        ..Default::default()
    }
}
pub async fn registry(provider: Arc<dyn Provider>) -> (ModelsRegistry, Model) {
    let model = provider.models().unwrap()[0].clone();
    let credentials = Arc::new(InMemoryCredentialStore::new());

    let registry = ModelsRegistry::new(ModelsRegistryOptions {
        identity: None,
        credentials,
        models_store: Box::new(InMemoryModelsStore::new()),
        auth_context: Arc::new(Host),
        clock: Arc::new(Host),
        spawner: Arc::new(Host),
        model_header_policy: Default::default(),
    });
    registry.set_provider(provider).await.unwrap();
    (registry, model)
}
pub fn services(peer: Arc<Peer>) -> AdapterServices {
    AdapterServices::new(Arc::new(HttpTransport::new_custom(peer, Arc::new(Host))))
}

pub fn configured(family: &str, adapter: Arc<dyn ApiAdapter>) -> Arc<dyn Provider> {
    let models = match family {
        "anthropic" => pi_ai_providers::catalog::anthropic_models().unwrap(),
        "google" => pi_ai_providers::catalog::google_models().unwrap(),
        "openai" => pi_ai_providers::catalog::openai_models().unwrap(),
        "deepseek" => pi_ai_providers::catalog::deepseek_models().unwrap(),
        _ => panic!("family"),
    };
    create_provider(CreateProviderOptions {
        id: ProviderId::new(family).unwrap(),
        name: Some(family.into()),
        auth: ProviderAuth::new(
            Some(Arc::new(pi_ai_providers::auth::env_api_key_auth(
                "fixture",
                vec![],
            ))),
            None,
        )
        .unwrap(),
        models,
        dynamic_models: None,
        model_filter: None,
        adapters: vec![adapter],
    })
    .unwrap()
}
pub fn provider(family: &str, services: AdapterServices) -> Arc<dyn Provider> {
    let adapter: Arc<dyn ApiAdapter> = match family {
        "anthropic" => Arc::new(AnthropicMessagesApi::new(services)),
        "google" => Arc::new(GoogleGenAiApi::new(services)),
        "openai" => Arc::new(OpenAIResponsesApi::new(services)),
        "deepseek" => Arc::new(OpenAICompletionsApi::new(services)),
        _ => panic!("family"),
    };
    configured(family, adapter)
}
pub async fn drain(mut stream: AssistantStream) -> (AssistantMessage, Vec<Value>) {
    let result = stream.result().await;
    let mut events = vec![];
    while let Some(event) = stream.next().await {
        events.push(serde_json::to_value(event).unwrap());
    }
    for _ in 0..3 {
        tokio::task::yield_now().await;
    }
    (result, events)
}

pub fn selected_factory(family: &str, services: AdapterServices) -> Arc<dyn Provider> {
    use pi_ai_providers::providers::*;
    let auth = FactoryAuthServices {
        spawner: Arc::new(Host),
        oauth: Some(pi_ai_providers::auth::OAuthServices {
            transport: services.transport.clone(),
            clock: Arc::new(Host),
            spawner: Arc::new(Host),
            crypto: Arc::new(Host),
            callbacks: Arc::new(Host),
            callback_host: "127.0.0.1".into(),
            device_id: None,
            agent_name: None,
            callback_limits: Default::default(),
            max_token_response_bytes: 65536,
        }),
    };
    match family {
        "anthropic" => {
            anthropic_provider(Arc::new(AnthropicMessagesApi::new(services)), &auth).unwrap()
        }
        "google" => google_provider(Arc::new(GoogleGenAiApi::new(services))).unwrap(),
        "openai" => openai_provider(Arc::new(OpenAIResponsesApi::new(services)), &auth).unwrap(),
        "deepseek" => deepseek_provider(Arc::new(OpenAICompletionsApi::new(services))).unwrap(),

        _ => panic!("family"),
    }
}

// Compile the exact private selected-factory composition for custom capability
// controls; no test-only production constructor is added.
#[path = "../../src/providers/error.rs"]
mod factory_error;
use factory_error::ProviderFactoryError;
#[path = "../../src/providers/text.rs"]
mod configured_text;
pub fn configured_wrapper(adapter: Arc<dyn ApiAdapter>) -> Arc<dyn Provider> {
    configured_text::compose(
        "openai",
        "OpenAI",
        "openai-responses",
        adapter,
        ProviderAuth::new(
            Some(Arc::new(pi_ai_providers::auth::env_api_key_auth(
                "fixture",
                vec![],
            ))),
            None,
        )
        .unwrap(),
        pi_ai_providers::catalog::openai_models().unwrap(),
    )
    .unwrap()
}
