#![cfg(all(
    feature = "wire-pi-messages",
    feature = "wire-anthropic-messages",
    feature = "wire-mistral-conversations",
    feature = "wire-openai-completions",
    feature = "wire-openai-responses",
    feature = "wire-azure-openai-responses",
    feature = "wire-google-genai",
    feature = "wire-google-vertex"
))]
use pi_ai_core::*;
use pi_ai_providers::api::{
    self,
    runtime::{AdapterResponseHead, AdapterServices, ResponseMiddleware},
};
use pi_ai_transport::*;
use serde_json::{Value, json};
use std::{
    future::Future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
    time::Duration,
};
#[derive(Default)]
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
    fn spawn(&self, t: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
        self.0.lock().unwrap().push(t);
        Ok(())
    }
}
impl RetryJitter for Host {
    fn sample(&self) -> f64 {
        panic!("no retry")
    }
}
fn ready<F: Future>(f: F) -> F::Output {
    match Box::pin(f)
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(v) => v,
        Poll::Pending => panic!("unexpected pending"),
    }
}
#[derive(Default)]
struct Seen {
    order: Mutex<Vec<String>>,
    reads: AtomicUsize,
    closes: AtomicUsize,
    drops: AtomicUsize,
}
struct Body {
    seen: Arc<Seen>,
    absent: bool,
    closed: bool,
    status: u16,
}
impl ResponseBody for Body {
    fn presence(&self) -> ResponseBodyPresence {
        if self.absent {
            ResponseBodyPresence::Absent
        } else {
            ResponseBodyPresence::Present
        }
    }
    fn next<'a>(
        &'a mut self,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, TransportError>> {
        Box::pin(async move {
            assert!(
                !(self.absent && self.status < 300),
                "successful absent body must never be polled"
            );
            if !self.absent {
                self.seen.reads.fetch_add(1, Ordering::SeqCst);
            }
            Ok(None)
        })
    }
    fn close(&mut self) {
        if !self.closed {
            self.closed = true;
            self.seen.closes.fetch_add(1, Ordering::SeqCst);
        }
    }
}
impl Drop for Body {
    fn drop(&mut self) {
        self.close();
        self.seen.drops.fetch_add(1, Ordering::SeqCst);
    }
}
struct Custom {
    seen: Arc<Seen>,
    status: u16,
    absent: bool,
}
impl CustomFetch for Custom {
    fn fetch<'a>(
        &'a self,
        _: &'a FetchRequest,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<HttpResponse, TransportError>> {
        Box::pin(async move {
            self.seen.order.lock().unwrap().push("fetch".into());
            let mut headers = HttpHeaders::new();
            headers.set("content-type", "text/event-stream")?;
            Ok(HttpResponse {
                status: self.status,
                status_text: String::new(),
                headers,
                body: Box::new(Body {
                    seen: self.seen.clone(),
                    absent: self.absent,
                    closed: false,
                    status: self.status,
                }),
            })
        })
    }
}
impl ResponseMiddleware for Seen {
    fn on_response<'a>(
        &'a self,
        _: &'a TextStreamRequest,
        _: &'a AdapterResponseHead,
    ) -> BoxFuture<'a, Result<(), ModelsError>> {
        Box::pin(async move {
            self.order.lock().unwrap().push("response".into());
            Ok(())
        })
    }
}
fn normalized(mut v: Value) -> Value {
    if let Some(object) = v.as_object_mut() {
        if object.get("role").and_then(Value::as_str) == Some("assistant") {
            object.remove("diagnostics");
            if object.contains_key("errorMessage") {
                object.insert("errorMessage".into(), json!("<typed-error>"));
            }
        }
        for value in object.values_mut() {
            *value = normalized(value.take());
        }
    } else if let Some(array) = v.as_array_mut() {
        for value in array {
            *value = normalized(value.take());
        }
    }
    v
}
#[test]
fn actual_custom_fetch_presence_preserves_eight_family_start_retry_and_body_custody_stages() {
    let source: Value = serde_json::from_str(include_str!(
        "../../../conformance/provider-traces/response-body-presence/trace.json"
    ))
    .unwrap();
    assert_eq!(source["cases"].as_array().unwrap().len(), 34);
    for case in source["cases"].as_array().unwrap() {
        let seen = Arc::new(Seen::default());
        let host = Arc::new(Host::default());
        let mut services = AdapterServices::new(Arc::new(HttpTransport::new_custom(
            Arc::new(Custom {
                seen: seen.clone(),
                status: case["status"].as_u64().unwrap() as u16,
                absent: case["absent"].as_bool().unwrap(),
            }),
            host.clone(),
        )));
        services.response = Some(seen.clone());
        let request = TextStreamRequest {
            resolved: ResolvedRequest {
                model: serde_json::from_value(case["model"].clone()).unwrap(),
                endpoint: RequestEndpoint::new_resolved(case["model"]["baseUrl"].as_str().unwrap())
                    .unwrap(),
            },
            context: serde_json::from_value(case["context"].clone()).unwrap(),
            auth: ResolvedAuth {
                api_key: Some(SecretString::new("synthetic-key")),
                ..Default::default()
            },
            options: StreamOptions {
                wire: serde_json::from_value(json!({"maxRetries":2,"timeoutMs":60000})).unwrap(),
                runtime: RequestRuntime::with_noop_telemetry(
                    CancellationToken::new(),
                    host.clone(),
                    host.clone(),
                ),
            },
        };
        let mut stream = match case["family"].as_str().unwrap() {
            "google-generative-ai" => {
                api::google_genai::GoogleGenAiApi::new(services).stream(request)
            }
            "google-vertex" => api::google_vertex::GoogleVertexApi::new(services).stream(request),
            "pi-messages" => api::pi_messages::PiMessagesApi::new(services).stream(request),
            "anthropic-messages" => {
                api::anthropic_messages::AnthropicMessagesApi::new(services).stream(request)
            }
            "mistral-conversations" => {
                api::mistral_conversations::MistralApi::new(services).stream(request)
            }
            "openai-completions" => {
                api::openai_completions::OpenAICompletionsApi::new(services).stream(request)
            }
            "openai-responses" => {
                api::openai_responses::OpenAIResponsesApi::new(services).stream(request)
            }
            "azure-openai-responses" => {
                api::azure_openai_responses::AzureOpenAIResponsesApi::new(services).stream(request)
            }
            _ => unreachable!(),
        };
        ready(host.0.lock().unwrap().pop().unwrap());
        let result = ready(stream.result());
        let mut events = vec![];
        while let Some(event) = ready(stream.next()) {
            let event = json!(event);
            seen.order
                .lock()
                .unwrap()
                .push(event["type"].as_str().unwrap().into());
            events.push(event);
        }
        let label = format!("{} {} {}", case["family"], case["status"], case["absent"]);
        assert_eq!(
            is_retryable_assistant_error(&result),
            case["retryable"].as_bool().unwrap(),
            "{label} retryable"
        );
        assert_eq!(
            serde_json::from_value::<AssistantMessage>(normalized(json!(result))).unwrap(),
            serde_json::from_value::<AssistantMessage>(normalized(case["result"].clone())).unwrap(),
            "{label} result"
        );
        assert_eq!(
            serde_json::from_value::<Vec<AssistantMessageEvent>>(normalized(json!(events)))
                .unwrap(),
            serde_json::from_value::<Vec<AssistantMessageEvent>>(normalized(
                case["events"].clone()
            ))
            .unwrap(),
            "{label} events"
        );
        assert_eq!(
            json!(*seen.order.lock().unwrap()),
            case["order"],
            "{label} stage"
        );
        assert_eq!(
            seen.reads.load(Ordering::SeqCst),
            case["reads"].as_u64().unwrap() as usize,
            "{label} reads"
        );
        assert_eq!(seen.closes.load(Ordering::SeqCst), 1, "{label} close");
        assert_eq!(seen.drops.load(Ordering::SeqCst), 1, "{label} drop");
    }
}
