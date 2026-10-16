#![cfg(feature = "all-providers")]
mod retained_support;
use pi_ai_core::*;
use pi_ai_providers::api::{
    runtime::{AdapterServices, PayloadMiddleware},
    wire_json::WirePayload,
};
use pi_ai_transport::*;
use serde_json::{Value, json};
use std::{
    future::Future,
    sync::{Arc, Mutex},
    task::{Context as TaskContext, Poll},
    time::Duration,
};
fn ready<T>(future: impl Future<Output = T>) -> T {
    let mut future = Box::pin(future);
    match future
        .as_mut()
        .poll(&mut TaskContext::from_waker(std::task::Waker::noop()))
    {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("offline task pending"),
    }
}
struct Host(Mutex<Vec<BoxFuture<'static, ()>>>);
impl Clock for Host {
    fn now_unix_ms(&self) -> u64 {
        7
    }
    fn sleep<'a>(
        &'a self,
        _: Duration,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), Cancelled>> {
        Box::pin(std::future::pending())
    }
}
impl TaskSpawner for Host {
    fn spawn(&self, future: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
        self.0.lock().unwrap().push(future);
        Ok(())
    }
}
impl RetryJitter for Host {
    fn sample(&self) -> f64 {
        panic!("no jitter")
    }
}
struct Peer {
    terminal: String,
    requests: Mutex<Vec<Value>>,
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
impl CustomFetch for Peer {
    fn fetch<'a>(
        &'a self,
        request: &'a FetchRequest,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<HttpResponse, TransportError>> {
        Box::pin(async move {
            let body = match &request.body {
                FetchBody::Text(body) => serde_json::from_str::<Value>(body).unwrap(),
                FetchBody::Bytes(body) => serde_json::from_slice(body).unwrap(),
                FetchBody::Absent => Value::Null,
            };
            let captured_headers: std::collections::BTreeMap<_, _> = match &request.headers {
                FetchHeaders::Record(record) => record
                    .iter()
                    .map(|(name, value)| (name.to_ascii_lowercase(), value.to_owned()))
                    .collect(),
                FetchHeaders::Constructed(headers) => headers
                    .iter_for_wire()
                    .map(|(name, value)| (name.to_ascii_lowercase(), value.to_owned()))
                    .collect(),
            };
            self.requests.lock().unwrap().push(json!({"url":request.endpoint.expose_for_transport(),"body":body,"headers":captured_headers}));
            let mut headers = HttpHeaders::new();
            headers.set("content-type", "text/event-stream").unwrap();
            Ok(HttpResponse {
                status: 200,
                status_text: "OK".into(),
                headers,
                body: Box::new(Body(Some(self.terminal.as_bytes().to_vec()))),
            })
        })
    }
}
struct Observer {
    events: Mutex<Vec<Value>>,
    fail: bool,
}
impl ProviderStreamEventObserver for Observer {
    fn on_provider_stream_event<'a>(
        &'a self,
        data: &'a Value,
        _: &'a Model,
    ) -> BoxFuture<'a, Result<(), ModelsError>> {
        self.events.lock().unwrap().push(data.clone());
        Box::pin(async move {
            if self.fail {
                Err(ModelsError::new(ModelsErrorCode::Stream))
            } else {
                Ok(())
            }
        })
    }
}
struct Payload(Mutex<Vec<Value>>);
impl PayloadMiddleware for Payload {
    fn on_payload<'a>(
        &'a self,
        _: &'a TextStreamRequest,
        payload: &'a mut WirePayload,
    ) -> BoxFuture<'a, Result<Option<WirePayload>, ModelsError>> {
        self.0.lock().unwrap().push(payload.as_value().clone());
        Box::pin(async { Ok(None) })
    }
}
fn without_duration(mut value: Value) -> Value {
    if let Some(object) = value.as_object_mut() {
        object.remove("durationMs");
    }
    value
}
#[test]
fn four_protocols_match_published_v140_wires_and_terminal_messages() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../conformance/provider-traces/v1.4.0/wire-observations.json"
    ))
    .unwrap();
    assert_eq!(
        fixture["target"],
        "4a080252f06caf63a525d7873c5d94d3c23887e0"
    );
    assert_eq!(fixture["cases"].as_array().unwrap().len(), 32);
    for case in fixture["cases"].as_array().unwrap() {
        for fail in [false, true] {
            let id = case["id"].as_str().unwrap();
            let host = Arc::new(Host(Mutex::new(Vec::new())));
            let peer = Arc::new(Peer {
                terminal: case["terminal"].as_str().unwrap().into(),
                requests: Mutex::new(Vec::new()),
            });
            let payload = Arc::new(Payload(Mutex::new(Vec::new())));
            let mut services = AdapterServices::new(Arc::new(HttpTransport::new_custom(
                peer.clone(),
                host.clone(),
            )));
            services.payload = Some(payload.clone());
            let mut common = case["options"].clone();
            let key = common.as_object_mut().unwrap().remove("apiKey").unwrap();
            let mut family = json!({});
            for name in [
                "reasoningEffort",
                "thinkingEnabled",
                "effort",
                "thinking",
                "serviceTier",
            ] {
                if let Some(value) = common.as_object_mut().unwrap().remove(name) {
                    family[name] = value;
                }
            }
            let observer = Arc::new(Observer {
                events: Mutex::new(Vec::new()),
                fail,
            });
            let mut request = TextStreamRequest {
                resolved: ResolvedRequest {
                    model: serde_json::from_value(case["model"].clone()).unwrap(),
                    endpoint: RequestEndpoint::new_resolved(
                        case["model"]["baseUrl"].as_str().unwrap(),
                    )
                    .unwrap(),
                },
                context: normalize_context(
                    serde_json::from_value(case["context"].clone()).unwrap(),
                ),
                auth: ResolvedAuth {
                    api_key: Some(SecretString::new(key.as_str().unwrap())),
                    ..Default::default()
                },
                options: StreamOptions {
                    wire: serde_json::from_value(common).unwrap(),
                    runtime: RequestRuntime::with_noop_telemetry(
                        CancellationToken::new(),
                        host.clone(),
                        host.clone(),
                    ),
                },
            };
            request.options.runtime.provider_event_observer = Some(observer.clone());
            let mut stream = match request.resolved.model.api.as_str() {
                "openai-responses" => {
                    pi_ai_providers::api::openai_responses::OpenAIResponsesApi::new(services)
                        .stream_with_options(request, serde_json::from_value(family).unwrap())
                }
                "openai-completions" => {
                    pi_ai_providers::api::openai_completions::OpenAICompletionsApi::new(services)
                        .stream_with_options(request, serde_json::from_value(family).unwrap())
                }
                "anthropic-messages" => {
                    pi_ai_providers::api::anthropic_messages::AnthropicMessagesApi::new(services)
                        .stream_with_options(request, serde_json::from_value(family).unwrap())
                }
                "google-generative-ai" => {
                    pi_ai_providers::api::google_genai::GoogleGenAiApi::new(services)
                        .stream_with_options(request, serde_json::from_value(family).unwrap())
                }
                _ => unreachable!(),
            };
            while let Some(task) = host.0.lock().unwrap().pop() {
                ready(task);
            }
            let result = ready(stream.result());
            if fail {
                assert_eq!(
                    result.stop_reason,
                    StopReason::Error,
                    "{id}: observer failure"
                );
                assert_eq!(
                    observer.events.lock().unwrap().len(),
                    1,
                    "{id}: observer called once"
                );
                let mut terminals = 0;
                while let Some(event) = ready(stream.next()) {
                    if matches!(
                        event,
                        AssistantMessageEvent::Error { .. } | AssistantMessageEvent::Done { .. }
                    ) {
                        terminals += 1;
                    }
                }
                assert_eq!(terminals, 1, "{id}: one observer failure terminal");
                continue;
            }
            assert_eq!(
                *payload.0.lock().unwrap(),
                case["payloads"].as_array().unwrap().clone(),
                "{id}: payload"
            );
            let observed = peer.requests.lock().unwrap();
            let expected = &case["requests"][0];
            assert_eq!(observed.len(), 1, "{id}: request");
            assert_eq!(observed[0]["body"], expected["body"], "{id}: wire");
            assert_numeric_json_equal(
                &without_duration(serde_json::to_value(result).unwrap()),
                &case["result"],
                id,
            );
            assert_eq!(
                *observer.events.lock().unwrap(),
                case["providerEvents"].as_array().unwrap().clone(),
                "{id}: provider events"
            );
            let mut events = Vec::new();
            while let Some(event) = ready(stream.next()) {
                events.push(event);
            }
            assert_numeric_json_equal(
                &retained_support::without_duration(json!(events)),
                &case["events"],
                id,
            );
            for name in [
                "accept",
                "content-type",
                "x-stainless-retry-count",
                "x-stainless-timeout",
                "user-agent",
                "authorization",
                "x-api-key",
                "x-goog-api-key",
                "anthropic-beta",
                "anthropic-version",
            ] {
                assert_eq!(
                    observed[0]["headers"][name], expected["headers"][name],
                    "{id}: {name}"
                );
            }
        }
    }
}

#[test]
fn target_release_oracle_reexecutes_without_network() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let output = std::process::Command::new("node")
        .arg(root.join("conformance/provider-traces/v1.4.0/oracle.mjs"))
        .env("NODE_NO_WARNINGS", "1")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn assert_numeric_json_equal(actual: &Value, expected: &Value, id: &str) {
    match (actual, expected) {
        (Value::Number(a), Value::Number(b)) => assert_eq!(a.as_f64(), b.as_f64(), "{id}"),
        (Value::Array(a), Value::Array(b)) => {
            assert_eq!(a.len(), b.len(), "{id}");
            for (a, b) in a.iter().zip(b) {
                assert_numeric_json_equal(a, b, id);
            }
        }
        (Value::Object(a), Value::Object(b)) => {
            assert_eq!(a.len(), b.len(), "{id}: {actual} vs {expected}");
            for (key, b) in b {
                assert_numeric_json_equal(
                    a.get(key).unwrap_or_else(|| panic!("{id}: missing {key}")),
                    b,
                    id,
                );
            }
        }
        _ => assert_eq!(actual, expected, "{id}"),
    }
}
