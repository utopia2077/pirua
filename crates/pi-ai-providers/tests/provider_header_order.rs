#![cfg(all(
    feature = "wire-pi-messages",
    feature = "wire-mistral-conversations",
    feature = "wire-anthropic-messages",
    feature = "wire-openai-responses",
    feature = "wire-openai-completions"
))]

use pi_ai_core::*;
use pi_ai_providers::api::{
    anthropic_messages::AnthropicMessagesApi, mistral_conversations::MistralApi,
    openai_completions::OpenAICompletionsApi, openai_responses::OpenAIResponsesApi,
    pi_messages::PiMessagesApi, runtime::*,
};
use pi_ai_transport::*;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    future::{Future, pending},
    sync::{Arc, Mutex},
    task::{Context as TaskContext, Poll, Waker},
    time::Duration,
};

struct TestClock;
impl Clock for TestClock {
    fn now_unix_ms(&self) -> u64 {
        7
    }
    fn sleep<'a>(
        &'a self,
        _: Duration,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), Cancelled>> {
        Box::pin(pending())
    }
}
#[derive(Default)]
struct Spawner(Mutex<Vec<BoxFuture<'static, ()>>>);
impl TaskSpawner for Spawner {
    fn spawn(&self, task: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
        self.0.lock().unwrap().push(task);
        Ok(())
    }
}
fn ready<F: Future>(future: F) -> F::Output {
    match Box::pin(future)
        .as_mut()
        .poll(&mut TaskContext::from_waker(Waker::noop()))
    {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("unexpected suspended request"),
    }
}
#[derive(Default)]
struct Observed {
    headers: Mutex<Vec<HttpHeaders>>,
    order: Mutex<Vec<&'static str>>,
}
impl PayloadMiddleware for Observed {
    fn on_payload<'a>(
        &'a self,
        _: &'a TextStreamRequest,
        _: &'a mut JsonValue,
    ) -> BoxFuture<'a, Result<Option<JsonValue>, ModelsError>> {
        self.order.lock().unwrap().push("payload");
        Box::pin(async { Ok(None) })
    }
}
impl ResponseMiddleware for Observed {
    fn on_response<'a>(
        &'a self,
        _: &'a TextStreamRequest,
        _: &'a AdapterResponseHead,
    ) -> BoxFuture<'a, Result<(), ModelsError>> {
        self.order.lock().unwrap().push("response");
        Box::pin(async { Ok(()) })
    }
}
struct EmptyBody;
impl ResponseBody for EmptyBody {
    fn next<'a>(
        &'a mut self,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, TransportError>> {
        Box::pin(async { Ok(None) })
    }
    fn close(&mut self) {}
}
struct Client(Arc<Observed>);
impl RawHttpClient for Client {
    fn send<'a>(
        &'a self,
        request: &'a HttpRequest,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<HttpResponse, TransportError>> {
        self.0.order.lock().unwrap().push("send");
        self.0.headers.lock().unwrap().push(request.headers.clone());
        Box::pin(async {
            let mut headers = HttpHeaders::new();
            headers.set("content-type", "application/json").unwrap();
            Ok(HttpResponse {
                status: 400,
                status_text: "Bad Request".into(),
                headers,
                body: Box::new(EmptyBody),
            })
        })
    }
}
struct NoJitter;
impl RetryJitter for NoJitter {
    fn sample(&self) -> f64 {
        panic!("400 must not retry")
    }
}

#[test]
fn five_family_raw_dispatch_matches_pinned_source_permutations_without_changing_hook_stages() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../conformance/provider-traces/provider-header-order/trace.json"
    ))
    .unwrap();
    assert_eq!(fixture["cases"].as_array().unwrap().len(), 95);
    for case in fixture["cases"].as_array().unwrap() {
        let family = case["family"].as_str().unwrap();
        let observed = Arc::new(Observed::default());
        let mut services = AdapterServices::new(Arc::new(
            HttpTransport::new(Arc::new(Client(observed.clone())), Arc::new(NoJitter))
                .with_fetch_response_source(FetchResponseSource::Preconstructed),
        ));
        services.payload = Some(observed.clone());
        services.response = Some(observed.clone());
        let adapter: Box<dyn ApiAdapter> = match family {
            "pi-messages" => Box::new(PiMessagesApi::new(services)),
            "mistral-conversations" => Box::new(MistralApi::new(services)),
            "anthropic-messages" => Box::new(AnthropicMessagesApi::new(services)),
            "openai-responses" => Box::new(OpenAIResponsesApi::new(services)),
            "openai-completions" => Box::new(OpenAICompletionsApi::new(services)),
            _ => panic!("unexpected fixture family"),
        };
        let provider = match family {
            "anthropic-messages" => "anthropic",
            "mistral-conversations" => "mistral",
            _ => "custom",
        };
        let spawner = Arc::new(Spawner::default());
        let request=TextStreamRequest {
            resolved:ResolvedRequest { model:serde_json::from_value(json!({"id":"header-model","name":"Header model","api":family,"provider":provider,"baseUrl":"https://example.invalid/base","reasoning":false,"input":["text"],"cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0},"contextWindow":4096,"maxTokens":128})).unwrap(),endpoint:RequestEndpoint::new_resolved("https://example.invalid/base").unwrap() },
            context:serde_json::from_value(json!({"messages":[]})).unwrap(),
            auth:ResolvedAuth {api_key:Some(SecretString::new("synthetic-key")),headers:serde_json::from_str(case["raw"].as_str().unwrap()).unwrap(),..Default::default()},
            options:StreamOptions {wire:StreamOptionsWire::default(),runtime:RequestRuntime::with_noop_telemetry(CancellationToken::new(),Arc::new(TestClock),spawner.clone())},
        };
        let stream = adapter.stream(request);
        while let Some(task) = spawner.0.lock().unwrap().pop() {
            ready(task);
        }
        let result = ready(stream.result());
        assert_eq!(result.stop_reason, StopReason::Error, "{case}");
        let requests = observed.headers.lock().unwrap();
        assert_eq!(
            requests.len(),
            case["calls"].as_u64().unwrap() as usize,
            "{case}"
        );
        let actual: BTreeMap<_, _> = requests[0]
            .iter_for_wire()
            .filter(|(name, _)| {
                *name == "x-order" || *name == "accept" || name.bytes().all(|b| b.is_ascii_digit())
            })
            .map(|(name, _)| (name.to_owned(), requests[0].get_for_fetch(name).unwrap()))
            .collect();
        assert_eq!(
            serde_json::to_value(actual).unwrap(),
            case["observed"],
            "{family}: {}",
            case["raw"]
        );
        assert_eq!(
            serde_json::to_value(&*observed.order.lock().unwrap()).unwrap(),
            case["order"],
            "{case}"
        );
    }
}
