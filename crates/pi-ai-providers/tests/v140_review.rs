#![cfg(all(feature = "wire-anthropic-messages", feature = "wire-openai-responses"))]

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
    let mut cx = TaskContext::from_waker(std::task::Waker::noop());
    for _ in 0..100 {
        if let Poll::Ready(v) = future.as_mut().poll(&mut cx) {
            return v;
        }
    }
    panic!("unexpected pending");
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
    fn spawn(&self, f: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
        self.0.lock().unwrap().push(f);
        Ok(())
    }
}
impl RetryJitter for Host {
    fn sample(&self) -> f64 {
        0.0
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
struct Peer {
    responses: Vec<Value>,
    requests: Mutex<Vec<Value>>,
}
impl CustomFetch for Peer {
    fn fetch<'a>(
        &'a self,
        request: &'a FetchRequest,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<HttpResponse, TransportError>> {
        Box::pin(async move {
            let body = match &request.body {
                FetchBody::Text(body) => serde_json::from_str(body).unwrap(),
                FetchBody::Bytes(body) => serde_json::from_slice(body).unwrap(),
                FetchBody::Absent => panic!("body expected"),
            };
            let mut requests = self.requests.lock().unwrap();
            requests.push(body);
            let reply = &self.responses[(requests.len() - 1).min(self.responses.len() - 1)];
            let status = reply["status"].as_u64().unwrap() as u16;
            let mut headers = HttpHeaders::new();
            headers
                .set(
                    "content-type",
                    if status == 200 {
                        "text/event-stream"
                    } else {
                        "application/json"
                    },
                )
                .unwrap();
            Ok(HttpResponse {
                status,
                status_text: "fixture".into(),
                headers,
                body: Box::new(Body(Some(
                    reply["body"].as_str().unwrap().as_bytes().to_vec(),
                ))),
            })
        })
    }
}
struct Payload(Mutex<Vec<Value>>, bool);
impl PayloadMiddleware for Payload {
    fn on_payload<'a>(
        &'a self,
        _: &'a TextStreamRequest,
        payload: &'a mut WirePayload,
    ) -> BoxFuture<'a, Result<Option<WirePayload>, ModelsError>> {
        self.0.lock().unwrap().push(payload.as_value().clone());
        Box::pin(async move {
            if self.1 {
                Ok(None)
            } else {
                Err(ModelsError::new(ModelsErrorCode::Stream))
            }
        })
    }
}
#[test]
fn review_regressions_match_exact_v140_adapters() {
    let mut cases: Value = serde_json::from_str(include_str!(
        "../../../conformance/provider-traces/v1.4.0/review-observations.json"
    ))
    .unwrap();
    for c in cases.as_array_mut().unwrap() {
        let host = Arc::new(Host(Mutex::new(Vec::new())));
        let payload = Arc::new(Payload(Mutex::new(Vec::new()), c["retryProbe"] == true));
        let peer = Arc::new(Peer {
            responses: c["responses"].as_array().cloned().unwrap_or_default(),
            requests: Mutex::new(Vec::new()),
        });
        let mut services = AdapterServices::new(Arc::new(HttpTransport::new_custom(
            peer.clone(),
            host.clone(),
        )));
        services.payload = Some(payload.clone());
        let mut common = c["options"].clone();
        let key = common.as_object_mut().unwrap().remove("apiKey").unwrap();
        let mut family = json!({});
        for name in [
            "reasoningEffort",
            "thinkingEnabled",
            "effort",
            "thinking",
            "serviceTier",
        ] {
            if let Some(v) = common.as_object_mut().unwrap().remove(name) {
                family[name] = v;
            }
        }
        let request = TextStreamRequest {
            resolved: ResolvedRequest {
                model: serde_json::from_value(c["model"].clone()).unwrap(),
                endpoint: RequestEndpoint::new_resolved(c["model"]["baseUrl"].as_str().unwrap())
                    .unwrap(),
            },
            context: normalize_context(serde_json::from_value(c["context"].clone()).unwrap()),
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
        let stream = match request.resolved.model.api.as_str() {
            "anthropic-messages" => {
                pi_ai_providers::api::anthropic_messages::AnthropicMessagesApi::new(services)
                    .stream_with_options(request, serde_json::from_value(family).unwrap())
            }
            "openai-responses" => {
                pi_ai_providers::api::openai_responses::OpenAIResponsesApi::new(services)
                    .stream_with_options(request, serde_json::from_value(family).unwrap())
            }
            _ => panic!("unhandled family"),
        };
        while let Some(task) = host.0.lock().unwrap().pop() {
            ready(task);
        }
        let result = ready(stream.result());
        c["actualStopReason"] = json!(result.stop_reason);
        c["actualRequests"] = json!(*peer.requests.lock().unwrap());
        c["actual"] = json!(*payload.0.lock().unwrap());
        c["equal"] = json!(c["actual"] == c["expected"]);
        assert_eq!(
            c["actual"], c["expected"],
            "{} payload callback builds",
            c["id"]
        );
        assert_eq!(
            c["actualRequests"], c["expectedRequests"],
            "{} physical request bodies",
            c["id"]
        );
        assert_eq!(
            c["actualStopReason"], c["expectedStopReason"],
            "{} terminal",
            c["id"]
        );
        assert!(!format!("{result:?}").contains("fixture-sensitive-detail"));
        let expected_diagnostics = c["expectedDiagnostics"].as_array().unwrap();
        let actual_fallback = result
            .diagnostics
            .as_deref()
            .unwrap_or_default()
            .iter()
            .filter(|d| d.diagnostic_type.as_str() == "anthropic_strict_tools_fallback")
            .collect::<Vec<_>>();
        assert_eq!(
            actual_fallback.len(),
            expected_diagnostics.len(),
            "{} fallback diagnostics",
            c["id"]
        );
        for (actual, expected) in actual_fallback.iter().zip(expected_diagnostics) {
            assert_eq!(
                serde_json::to_value(actual.diagnostic_type.clone()).unwrap(),
                expected["type"]
            );
            assert_eq!(
                serde_json::to_value(actual.timestamp).unwrap(),
                expected["timestamp"]
            );
            assert_eq!(
                serde_json::to_value(&actual.details).unwrap(),
                expected["details"]
            );
            // Source raw SDK message is deliberately redacted at the native safe-error boundary.
            let message = &actual.error.as_ref().unwrap().message;
            assert!(
                [
                    "The compiled grammar is too large",
                    "Too many strict tools (",
                    "Schemas contains too many parameters with union types ("
                ]
                .contains(&message.as_str()),
                "{} bounded recognized-limit diagnostic",
                c["id"]
            );
        }
        if c["id"] == "anthropic-failed-tool-result-image" {
            let tail = c["actual"][0]["messages"]
                .as_array()
                .unwrap()
                .iter()
                .find(|m| {
                    m["content"]
                        .as_array()
                        .is_some_and(|b| b.first().is_some_and(|b| b["type"] == "tool_result"))
                })
                .unwrap()["content"]
                .as_array()
                .unwrap();
            assert_eq!(
                tail.iter()
                    .map(|b| b["type"].as_str().unwrap())
                    .collect::<Vec<_>>(),
                ["tool_result", "tool_result", "text", "image"]
            );
            assert_eq!(
                tail.last().unwrap()["cache_control"],
                json!({"type":"ephemeral"})
            );
        }
    }
}

#[test]
fn review_observations_reexecute_exact_release_offline() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let result = std::process::Command::new("node")
        .arg(root.join("conformance/provider-traces/v1.4.0/review-oracle.mjs"))
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert!(String::from_utf8_lossy(&result.stdout).contains("22 cases"));
}
