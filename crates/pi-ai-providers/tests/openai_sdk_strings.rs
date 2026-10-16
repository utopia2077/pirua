#![cfg(all(feature = "wire-openai-responses", feature = "wire-openai-completions"))]

mod retained_support;
use pi_ai_core::*;
use pi_ai_providers::api::wire_json::WirePayload;
use pi_ai_providers::api::{
    openai_completions::OpenAICompletionsApi, openai_responses::OpenAIResponsesApi, runtime::*,
};
use pi_ai_transport::*;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    future::{Future, pending},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
    time::Duration,
};

#[derive(Default)]
struct Host(Mutex<Vec<BoxFuture<'static, ()>>>);
impl TaskSpawner for Host {
    fn spawn(&self, task: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
        self.0.lock().unwrap().push(task);
        Ok(())
    }
}
impl Clock for Host {
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
impl RetryJitter for Host {
    fn sample(&self) -> f64 {
        panic!("no retry expected")
    }
}
fn ready<F: Future>(f: F) -> F::Output {
    match Box::pin(f)
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(v) => v,
        Poll::Pending => panic!("unexpected blocked stage"),
    }
}
#[derive(Default)]
struct Seen {
    order: Mutex<Vec<&'static str>>,
    requests: Mutex<Vec<Value>>,
    closed: AtomicUsize,
}
struct Body {
    data: Option<Vec<u8>>,
    seen: Arc<Seen>,
}
impl ResponseBody for Body {
    fn next<'a>(
        &'a mut self,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, TransportError>> {
        let next = self.data.take();
        Box::pin(async move { Ok(next) })
    }
    fn close(&mut self) {
        self.seen.closed.fetch_add(1, Ordering::SeqCst);
    }
}
struct Client {
    case: Value,
    seen: Arc<Seen>,
}
impl RawHttpClient for Client {
    fn send<'a>(
        &'a self,
        req: &'a HttpRequest,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<HttpResponse, TransportError>> {
        Box::pin(async move {
            self.seen.order.lock().unwrap().push("send");
            self.seen.requests.lock().unwrap().push(json!({"url":req.endpoint.expose_for_transport(),"headers":req.headers.iter_for_wire().collect::<BTreeMap<_,_>>(),"body":serde_json::from_slice::<Value>(&req.body).unwrap()}));
            let mut headers = HttpHeaders::new();
            headers.append("content-type", self.case["contentType"].as_str().unwrap())?;
            Ok(HttpResponse {
                status: 200,
                status_text: String::new(),
                headers,
                body: Box::new(Body {
                    data: Some(self.case["sse"].as_str().unwrap().as_bytes().to_vec()),
                    seen: self.seen.clone(),
                }),
            })
        })
    }
}
struct Hooks {
    replacement: Option<WirePayload>,
    seen: Arc<Seen>,
}
impl PayloadMiddleware for Hooks {
    fn on_payload<'a>(
        &'a self,
        _: &'a TextStreamRequest,
        _: &'a mut WirePayload,
    ) -> BoxFuture<'a, Result<Option<WirePayload>, ModelsError>> {
        Box::pin(async {
            self.seen.order.lock().unwrap().push("payload");
            Ok(self.replacement.clone())
        })
    }
}
impl ResponseMiddleware for Hooks {
    fn on_response<'a>(
        &'a self,
        _: &'a TextStreamRequest,
        _: &'a AdapterResponseHead,
    ) -> BoxFuture<'a, Result<(), ModelsError>> {
        Box::pin(async {
            self.seen.order.lock().unwrap().push("response");
            Ok(())
        })
    }
}
fn public_snapshot(mut value: Value) -> Value {
    fn clear(value: &mut Value) {
        match value {
            Value::Object(map) => {
                map.remove("diagnostics");
                map.remove("errorMessage");
                for value in map.values_mut() {
                    clear(value);
                }
            }
            Value::Array(values) => {
                for value in values {
                    clear(value);
                }
            }
            Value::Number(number) => {
                *number = serde_json::Number::from_f64(number.as_f64().unwrap()).unwrap()
            }
            _ => {}
        }
    }
    clear(&mut value);
    value
}
fn matrix(completions: bool) {
    let corpus: Value = serde_json::from_str(if completions {
        include_str!("../../../conformance/provider-traces/openai-sdk-strings/completions.json")
    } else {
        include_str!("../../../conformance/provider-traces/openai-sdk-strings/responses.json")
    })
    .unwrap();
    compare(retained_support::fixture(&corpus.to_string()));
}
fn compare(corpus: Value) {
    let mut failures = Vec::new();
    for case in corpus["cases"].as_array().unwrap() {
        let seen = Arc::new(Seen::default());
        let host = Arc::new(Host::default());
        let hooks = Arc::new(Hooks {
            replacement: case.get("replacement").map(|v| WirePayload::new(v.clone())),
            seen: seen.clone(),
        });
        let mut services = AdapterServices::new(Arc::new(
            HttpTransport::new(
                Arc::new(Client {
                    case: case.clone(),
                    seen: seen.clone(),
                }),
                host.clone(),
            )
            .with_fetch_response_source(FetchResponseSource::Preconstructed),
        ));
        services.env = serde_json::from_value(case["ambient"].clone()).unwrap();
        services.payload = Some(hooks.clone());
        services.response = Some(hooks);
        let mut options = case["options"].clone();
        let headers = options
            .as_object_mut()
            .unwrap()
            .remove("headers")
            .unwrap_or(json!({}));
        let env = options
            .as_object_mut()
            .unwrap()
            .remove("env")
            .unwrap_or(json!({}));
        let request = TextStreamRequest {
            resolved: ResolvedRequest {
                model: serde_json::from_value(case["model"].clone()).unwrap(),
                endpoint: RequestEndpoint::new_resolved(case["model"]["baseUrl"].as_str().unwrap())
                    .unwrap(),
            },
            context: serde_json::from_value(case["context"].clone()).unwrap(),
            auth: ResolvedAuth {
                api_key: Some(SecretString::new("synthetic-key")),
                headers: serde_json::from_value(headers).unwrap(),
                env: serde_json::from_value(env).unwrap(),
            },
            options: StreamOptions {
                wire: serde_json::from_value(options).unwrap(),
                runtime: RequestRuntime::with_noop_telemetry(
                    CancellationToken::new(),
                    host.clone(),
                    host.clone(),
                ),
            },
        };
        let adapter: Box<dyn ApiAdapter> = if case["model"]["api"] == "openai-completions" {
            Box::new(OpenAICompletionsApi::new(services))
        } else {
            Box::new(OpenAIResponsesApi::new(services))
        };
        let mut stream = adapter.stream(request);
        ready(host.0.lock().unwrap().pop().unwrap());
        let message = retained_support::message(ready(stream.result()));
        let mut events = Vec::new();
        while let Some(event) = ready(stream.next()) {
            events.push(retained_support::without_duration(
                serde_json::to_value(event).unwrap(),
            ));
        }
        let actual = json!({"requests":*seen.requests.lock().unwrap(),"order":*seen.order.lock().unwrap(),"events":public_snapshot(json!(events)),"retryable":is_retryable_assistant_error(&message),"contextOverflow":is_context_overflow(&message,None)});
        let expected = json!({"requests":case["requests"],"order":case["order"],"events":public_snapshot(case["events"].clone()),"retryable":case["retryable"],"contextOverflow":case["contextOverflow"]});
        if actual != expected {
            failures.push(format!(
                "{}\nactual={actual}\nexpected={expected}",
                case["id"]
            ));
        }
        assert_eq!(
            seen.closed.load(Ordering::SeqCst),
            seen.requests.lock().unwrap().len(),
            "{} close per actual request",
            case["id"]
        );
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
#[test]
fn responses_sdk_string_boundaries_match_full_source() {
    matrix(false);
}
#[test]
fn completions_sdk_string_boundaries_match_full_source() {
    matrix(true);
}

#[test]
fn responses_custom_record_settles_before_validation() {
    compare(retained_support::fixture(include_str!(
        "../../../conformance/provider-traces/openai-sdk-strings/responses-custom-record.json"
    )));
}

#[test]
fn completions_custom_record_settles_before_validation() {
    compare(retained_support::fixture(include_str!(
        "../../../conformance/provider-traces/openai-sdk-strings/completions-custom-record.json"
    )));
}
