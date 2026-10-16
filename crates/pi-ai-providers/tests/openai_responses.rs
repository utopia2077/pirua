#![cfg(feature = "wire-openai-responses")]

mod retained_support;
use pi_ai_core::{
    ApiAdapter, AssistantMessageEvent, BoxFuture, CancellationToken, Cancelled, Clock, ModelsError,
    ProviderEnv, ProviderHeaders, RequestEndpoint, RequestRuntime, ResolvedAuth, ResolvedRequest,
    SafeUnsigned, SecretString, SimpleStreamOptions, SimpleStreamOptionsWire, SimpleStreamRequest,
    SpawnError, StopReason, StreamOptions, TaskSpawner, TextStreamRequest, is_context_overflow,
    is_retryable_assistant_error,
};
use pi_ai_providers::api::openai_responses::*;
use pi_ai_providers::api::runtime::{
    AdapterResponseHead, AdapterServices, PayloadMiddleware, ResponseMiddleware,
};
use pi_ai_providers::api::wire_json::WirePayload;
use pi_ai_transport::{
    HttpHeaders, HttpRequest, HttpResponse, HttpTransport, RawHttpClient, ResponseBody,
    RetryJitter, TransportError,
};
use serde_json::{Value, json};
use std::collections::{BTreeMap, VecDeque};
use std::future::{Future, pending};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context as TaskContext, Poll, Waker};
use std::time::Duration;

fn fixture() -> Value {
    retained_support::fixture(include_str!(
        "../../../conformance/provider-traces/openai-responses/trace.json"
    ))
}
fn first() -> Value {
    fixture()["cases"][0].clone()
}
fn ready<F: Future>(future: F) -> F::Output {
    let mut future = Box::pin(future);
    match future
        .as_mut()
        .poll(&mut TaskContext::from_waker(Waker::noop()))
    {
        Poll::Ready(v) => v,
        Poll::Pending => panic!("expected ready"),
    }
}
#[derive(Default)]
struct Spawner {
    tasks: Mutex<Vec<BoxFuture<'static, ()>>>,
    reject: bool,
}
impl TaskSpawner for Spawner {
    fn spawn(&self, task: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
        if self.reject {
            return Err(SpawnError::default());
        }
        self.tasks.lock().unwrap().push(task);
        Ok(())
    }
}
impl Spawner {
    fn take(&self) -> BoxFuture<'static, ()> {
        self.tasks.lock().unwrap().pop().unwrap()
    }
}
#[derive(Default)]
struct ClockProbe {
    sleeps: Mutex<Vec<Duration>>,
    timeout: bool,
}
impl Clock for ClockProbe {
    fn now_unix_ms(&self) -> u64 {
        7
    }
    fn sleep<'a>(
        &'a self,
        duration: Duration,
        _token: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), Cancelled>> {
        self.sleeps.lock().unwrap().push(duration);
        Box::pin(async move {
            if self.timeout || duration < Duration::from_millis(1000) {
                Ok(())
            } else {
                pending().await
            }
        })
    }
}
struct Jitter;
impl RetryJitter for Jitter {
    fn sample(&self) -> f64 {
        0.0
    }
}
#[derive(Default)]
struct Observed {
    requests: Mutex<Vec<Value>>,
    order: Mutex<Vec<&'static str>>,
    closed: AtomicUsize,
    dropped: AtomicUsize,
    models: Mutex<Vec<Value>>,
    response_heads: Mutex<Vec<AdapterResponseHead>>,
}
struct PendingRead(Arc<Observed>);
impl Future for PendingRead {
    type Output = Result<Option<Vec<u8>>, TransportError>;
    fn poll(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        Poll::Pending
    }
}
impl Drop for PendingRead {
    fn drop(&mut self) {
        self.0.dropped.fetch_add(1, Ordering::SeqCst);
    }
}
struct Body {
    chunks: VecDeque<Vec<u8>>,
    block: bool,
    observed: Arc<Observed>,
}
impl ResponseBody for Body {
    fn next<'a>(
        &'a mut self,
        _runtime: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, TransportError>> {
        self.observed.order.lock().unwrap().push("body");
        if self.chunks.is_empty() && self.block {
            return Box::pin(PendingRead(self.observed.clone()));
        }
        let next = self.chunks.pop_front();
        Box::pin(async move { Ok(next) })
    }
    fn close(&mut self) {
        self.observed.closed.fetch_add(1, Ordering::SeqCst);
    }
}
struct Client {
    responses: Mutex<VecDeque<HttpResponse>>,
    observed: Arc<Observed>,
    block: bool,
}
impl RawHttpClient for Client {
    fn send<'a>(
        &'a self,
        request: &'a HttpRequest,
        _runtime: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<HttpResponse, TransportError>> {
        Box::pin(async move {
            self.observed.order.lock().unwrap().push("send");
            self.observed.requests.lock().unwrap().push(json!({"url":request.endpoint.expose_for_transport(),"headers":request.headers.iter_for_wire().collect::<BTreeMap<_,_>>(),"body":serde_json::from_slice::<Value>(&request.body).unwrap_or(Value::Null)}));
            if self.block {
                pending::<()>().await;
            }
            Ok(self
                .responses
                .lock()
                .unwrap()
                .pop_front()
                .expect("one configured attempt"))
        })
    }
}
struct Middleware {
    observed: Arc<Observed>,
    replacement: Option<WirePayload>,
    block_payload: bool,
    block_response: bool,
}
impl PayloadMiddleware for Middleware {
    fn on_payload<'a>(
        &'a self,
        request: &'a TextStreamRequest,
        _payload: &'a mut WirePayload,
    ) -> BoxFuture<'a, Result<Option<WirePayload>, ModelsError>> {
        Box::pin(async move {
            self.observed.order.lock().unwrap().push("payload");
            self.observed
                .models
                .lock()
                .unwrap()
                .push(serde_json::to_value(&request.resolved.model).unwrap());
            if self.block_payload {
                pending::<()>().await;
            }
            Ok(self.replacement.clone())
        })
    }
}
impl ResponseMiddleware for Middleware {
    fn on_response<'a>(
        &'a self,
        _request: &'a TextStreamRequest,
        head: &'a AdapterResponseHead,
    ) -> BoxFuture<'a, Result<(), ModelsError>> {
        Box::pin(async move {
            self.observed.order.lock().unwrap().push("response");
            self.observed
                .response_heads
                .lock()
                .unwrap()
                .push(head.clone());
            if self.block_response {
                pending::<()>().await;
            }
            Ok(())
        })
    }
}
fn setup(
    case: &Value,
    status: u16,
    chunks: Vec<Vec<u8>>,
    block: bool,
) -> (
    OpenAIResponsesApi,
    Arc<Spawner>,
    Arc<Observed>,
    Arc<ClockProbe>,
    TextStreamRequest,
    OpenAIResponsesOptions,
) {
    let observed = Arc::new(Observed::default());
    let mut headers = HttpHeaders::new();
    headers
        .set(
            "content-type",
            case["contentType"].as_str().unwrap_or("text/event-stream"),
        )
        .unwrap();
    let client = Arc::new(Client {
        responses: Mutex::new(VecDeque::from([HttpResponse {
            status,
            status_text: String::new(),
            headers,
            body: Box::new(Body {
                chunks: chunks.into(),
                block,
                observed: observed.clone(),
            }),
        }])),
        observed: observed.clone(),
        block: false,
    });
    let middleware = Arc::new(Middleware {
        observed: observed.clone(),
        replacement: case.get("replacement").map(|v| WirePayload::new(v.clone())),
        block_payload: false,
        block_response: false,
    });
    // Match the oracle's SDK custom-fetch seam, before network defaults/decoding.
    let mut services = AdapterServices::new(Arc::new(
        HttpTransport::new(client, Arc::new(Jitter))
            .with_fetch_response_source(pi_ai_transport::FetchResponseSource::Preconstructed),
    ));
    services.payload = Some(middleware.clone());
    services.response = Some(middleware);
    let spawner = Arc::new(Spawner::default());
    let clock = Arc::new(ClockProbe::default());
    let mut common = case["options"].clone();
    let mut family = json!({});
    for key in [
        "reasoningEffort",
        "reasoningSummary",
        "serviceTier",
        "toolChoice",
    ] {
        if let Some(value) = common.as_object_mut().unwrap().remove(key) {
            family[key] = value;
        }
    }
    let headers = common
        .as_object_mut()
        .unwrap()
        .remove("headers")
        .unwrap_or(json!({}));
    let env = common
        .as_object_mut()
        .unwrap()
        .remove("env")
        .unwrap_or(json!({}));
    common.as_object_mut().unwrap().remove("reasoning");
    let request = TextStreamRequest {
        resolved: ResolvedRequest {
            model: serde_json::from_value(case["model"].clone()).unwrap(),
            endpoint: RequestEndpoint::new_resolved(
                case["endpoint"]
                    .as_str()
                    .unwrap_or(case["model"]["baseUrl"].as_str().unwrap()),
            )
            .unwrap(),
        },
        context: serde_json::from_value(case["context"].clone()).unwrap(),
        auth: ResolvedAuth {
            api_key: if case["noKey"] == true {
                None
            } else {
                Some(SecretString::new("synthetic-key"))
            },
            headers: serde_json::from_value(headers).unwrap(),
            env: serde_json::from_value(env).unwrap(),
        },
        options: StreamOptions {
            wire: serde_json::from_value(common).unwrap(),
            runtime: RequestRuntime::with_noop_telemetry(
                CancellationToken::new(),
                clock.clone(),
                spawner.clone(),
            ),
        },
    };
    (
        OpenAIResponsesApi::new(services),
        spawner,
        observed,
        clock,
        request,
        serde_json::from_value(family).unwrap(),
    )
}

#[test]
fn pinned_wrapper_sdk_requests_and_complete_snapshots_match_52_cases() {
    let fixture = fixture();
    assert_eq!(fixture["cases"].as_array().unwrap().len(), 50);
    for case in fixture["cases"].as_array().unwrap() {
        let (api, spawner, observed, clock, request, options) = setup(
            case,
            case["status"].as_u64().unwrap() as u16,
            case["sse"]
                .as_str()
                .unwrap()
                .as_bytes()
                .chunks(3)
                .map(<[u8]>::to_vec)
                .collect(),
            false,
        );
        let mut stream = if case["simple"] == true {
            api.stream_simple(SimpleStreamRequest {
                resolved: request.resolved,
                context: request.context,
                auth: request.auth,
                options: SimpleStreamOptions {
                    wire: SimpleStreamOptionsWire {
                        stream: request.options.wire,
                        reasoning: Some(
                            serde_json::from_value(case["options"]["reasoning"].clone()).unwrap(),
                        ),
                        ..Default::default()
                    },
                    runtime: request.options.runtime,
                },
            })
        } else {
            api.stream_with_options(request, options)
        };
        ready(spawner.take());
        let result = retained_support::message(ready(stream.result()));
        assert_eq!(
            is_retryable_assistant_error(&result),
            case["retryable"].as_bool().unwrap(),
            "retry {}",
            case["id"]
        );
        assert_eq!(
            is_context_overflow(&result, None),
            case["contextOverflow"].as_bool().unwrap(),
            "overflow {}",
            case["id"]
        );
        let mut events = Vec::new();
        while let Some(event) = ready(stream.next()) {
            events.push(retained_support::without_duration(
                serde_json::to_value(event).unwrap(),
            ));
        }
        // Core costs are finite-f64 while JS has one Number kind. Decode both
        // complete snapshots through the DTO; signature strings remain byte-exact.
        let normalize = |mut events: Value| {
            for event in events.as_array_mut().unwrap() {
                if event["type"] == "error" {
                    event["error"]["errorMessage"] = json!("normalized provider error");
                    event["error"]
                        .as_object_mut()
                        .unwrap()
                        .remove("diagnostics");
                }
            }
            serde_json::from_value::<Vec<AssistantMessageEvent>>(events).unwrap()
        };
        assert_eq!(
            normalize(json!(events)),
            normalize(case["events"].clone()),
            "events {}",
            case["id"]
        );
        assert_eq!(
            json!(*observed.requests.lock().unwrap()),
            case["requests"],
            "request {}",
            case["id"]
        );
        assert_eq!(
            observed
                .order
                .lock()
                .unwrap()
                .iter()
                .copied()
                .filter(|v| *v != "body")
                .collect::<Vec<_>>(),
            case["order"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_str().unwrap())
                .collect::<Vec<_>>()
        );
        assert_eq!(observed.closed.load(Ordering::SeqCst), 1);
        let order = observed.order.lock().unwrap();
        if case["requests"][0]["body"]["stream"] == true
            && case["status"].as_u64().unwrap() < 300
            && let (Some(response), Some(body)) = (
                order.iter().position(|v| *v == "response"),
                order.iter().position(|v| *v == "body"),
            )
        {
            assert!(
                response < body,
                "streaming response middleware precedes body reduction"
            );
        }
        assert_eq!(
            clock.sleeps.lock().unwrap()[0].as_millis(),
            case["options"]["timeoutMs"].as_u64().unwrap_or(600000) as u128
        );
    }
}

#[test]
fn complete_sdk_tool_choice_shapes_and_nullable_fields_are_strict() {
    let choices = [
        json!("auto"),
        json!("none"),
        json!("required"),
        json!({"type":"allowed_tools","mode":"auto","tools":[{"type":"function","name":"x","extra":null}]}),
        json!({"type":"allowed_tools","mode":"required","tools":[]}),
        json!({"type":"function","name":""}),
        json!({"type":"custom","name":"x"}),
        json!({"type":"mcp","server_label":"x"}),
        json!({"type":"mcp","server_label":"x","name":null}),
        json!({"type":"mcp","server_label":"x","name":"tool"}),
        json!({"type":"apply_patch"}),
        json!({"type":"shell"}),
    ];
    for choice in choices.into_iter().chain(
        [
            "file_search",
            "web_search_preview",
            "computer",
            "computer_use_preview",
            "computer_use",
            "web_search_preview_2025_03_11",
            "code_interpreter",
            "mcp",
        ]
        .map(|kind| json!({"type":kind})),
    ) {
        let raw = json!({"toolChoice":choice});
        let parsed: OpenAIResponsesOptions = serde_json::from_value(raw.clone()).unwrap();
        assert_eq!(serde_json::to_value(parsed).unwrap(), raw);
    }
    for raw in [
        json!({"toolChoice":{"type":"image_generation"}}),
        json!({"reasoningEffort":null}),
        json!({"reasoningEffort":"off"}),
        json!({"toolChoice":null}),
        json!({"toolChoice":{"type":"mcp","name":"x"}}),
        json!({"toolChoice":{"type":"shell","extra":false}}),
        json!({"reasoningSummary":"brief"}),
        json!({"serviceTier":"future"}),
        json!({"unknown":false}),
    ] {
        assert!(
            serde_json::from_value::<OpenAIResponsesOptions>(raw.clone()).is_err(),
            "{raw}"
        );
    }
    let raw = json!({"reasoningSummary":null,"serviceTier":null});
    assert_eq!(
        serde_json::to_value(
            serde_json::from_value::<OpenAIResponsesOptions>(raw.clone()).unwrap()
        )
        .unwrap(),
        raw
    );
}

#[test]
fn sdk_decode_stage_finishes_before_response_hook_and_start() {
    let corpus: Value = retained_support::fixture(include_str!(
        "../../../conformance/provider-traces/openai-responses/decode-stage.json"
    ));
    assert_eq!(corpus["cases"].as_array().unwrap().len(), 68);
    for case in corpus["cases"].as_array().unwrap() {
        let mut input = first();
        input["model"] = corpus["model"].clone();
        input["context"] = corpus["context"].clone();
        input["replacement"] = json!({"stream":false});
        input["options"] = json!({"maxRetries":3});
        input["contentType"] = case
            .get("contentType")
            .cloned()
            .unwrap_or(json!("application/json"));
        let (api, spawner, observed, _, request, options) = setup(
            &input,
            200,
            case["body"]
                .as_str()
                .unwrap()
                .as_bytes()
                .chunks(3)
                .map(<[u8]>::to_vec)
                .collect(),
            false,
        );
        let mut stream = api.stream_with_options(request, options);
        ready(spawner.take());
        let result = retained_support::message(ready(stream.result()));
        let mut events = vec![];
        while let Some(event) = ready(stream.next()) {
            events.push(json!(event)["type"].clone());
        }
        assert_eq!(json!(events), case["events"], "{}", case["id"]);
        assert_eq!(
            json!(result.stop_reason),
            case["stopReason"],
            "{}",
            case["id"]
        );
        assert_eq!(
            is_retryable_assistant_error(&result),
            case["retryable"].as_bool().unwrap(),
            "{}",
            case["id"]
        );
        assert_eq!(
            is_context_overflow(&result, None),
            case["contextOverflow"].as_bool().unwrap(),
            "{}",
            case["id"]
        );
        let order: Vec<_> = observed
            .order
            .lock()
            .unwrap()
            .iter()
            .copied()
            .filter(|v| *v != "body")
            .collect();
        assert_eq!(json!(order), case["order"], "{}", case["id"]);
        assert_eq!(observed.requests.lock().unwrap().len(), 1);
        assert_eq!(observed.closed.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn nonstream_decode_cancellation_and_empty_length_skip_hooks_and_retry() {
    let mut case = first();
    case["replacement"] = json!({"stream":false});
    case["contentType"] = json!("application/json");
    case["options"] = json!({"maxRetries":3});
    for skip in [false, true] {
        let (mut api, spawner, observed, _, request, options) = setup(&case, 200, vec![], false);
        let mut headers = HttpHeaders::new();
        headers.set("content-type", "application/json").unwrap();
        if skip {
            headers.set("content-length", "0").unwrap();
        }
        api.services.transport = Arc::new(HttpTransport::new(
            Arc::new(Client {
                responses: Mutex::new(VecDeque::from([HttpResponse {
                    status: 200,
                    status_text: String::new(),
                    headers,
                    body: Box::new(Body {
                        chunks: vec![br#"{"object":"response","output":null}"#.to_vec()].into(),
                        block: true,
                        observed: observed.clone(),
                    }),
                }])),
                observed: observed.clone(),
                block: false,
            }),
            Arc::new(Jitter),
        ));
        let token = request.options.runtime.cancellation.clone();
        let stream = api.stream_with_options(request, options);
        let mut worker = spawner.take();
        if !skip {
            assert!(
                worker
                    .as_mut()
                    .poll(&mut TaskContext::from_waker(Waker::noop()))
                    .is_pending()
            );
            assert!(!observed.order.lock().unwrap().contains(&"response"));
            token.cancel();
        }
        ready(worker);
        assert_eq!(
            ready(stream.result()).stop_reason,
            if skip {
                StopReason::Error
            } else {
                StopReason::Aborted
            }
        );
        assert_eq!(observed.requests.lock().unwrap().len(), 1);
        assert_eq!(observed.closed.load(Ordering::SeqCst), 1);
        assert_eq!(observed.dropped.load(Ordering::SeqCst), usize::from(!skip));
        assert!(!observed.order.lock().unwrap().contains(&"response"));
        if skip {
            assert!(!observed.order.lock().unwrap().contains(&"body"));
        }
    }
}

#[test]
fn malformed_eof_and_classified_http_errors_retain_partial_and_hide_raw_text() {
    for (status, body, retry, overflow) in [
        (
            429,
            r#"{"error":{"message":"limit synthetic-private","code":"rate_limit_exceeded"}}"#,
            true,
            false,
        ),
        (
            400,
            r#"{"error":{"message":"context length exceeded synthetic-private"}}"#,
            false,
            true,
        ),
        (500, "server error synthetic-private", true, false),
        (403, "private synthetic-private", false, false),
        (
            400,
            r#"{"message":"context length exceeded synthetic-private"}"#,
            false,
            false,
        ),
    ] {
        let (api, spawner, observed, _, request, options) =
            setup(&first(), status, vec![body.as_bytes().to_vec()], false);
        let mut stream = api.stream_with_options(request, options);
        ready(spawner.take());
        let result = retained_support::message(ready(stream.result()));
        assert_eq!(
            is_retryable_assistant_error(&result),
            retry,
            "{status} {body}"
        );
        assert_eq!(is_context_overflow(&result, None), overflow);
        assert_eq!(result.stop_reason, StopReason::Error);
        assert!(
            !serde_json::to_string(&result)
                .unwrap()
                .contains("synthetic-private")
        );
        assert!(matches!(
            ready(stream.next()),
            Some(AssistantMessageEvent::Error { .. })
        ));
        assert!(ready(stream.next()).is_none());
        assert!(!observed.order.lock().unwrap().contains(&"response"));
        assert_eq!(observed.closed.load(Ordering::SeqCst), 1);
    }
    let progress = "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"message\",\"id\":\"m\"}}\n\ndata: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"delta\":\"partial\"}\n\n";
    for tail in [
        "",
        "data: {malformed synthetic-private\n\n",
        "data: [DONE]\n\n",
        "data: {\"type\":\"error\",\"code\":\"unknown\",\"message\":\"server error synthetic-private\"}\n\n",
    ] {
        let (api, spawner, observed, _, request, options) = setup(
            &first(),
            200,
            vec![format!("{progress}{tail}").into_bytes()],
            false,
        );
        let stream = api.stream_with_options(request, options);
        ready(spawner.take());
        let result = retained_support::message(ready(stream.result()));
        assert_eq!(result.stop_reason, StopReason::Error);
        assert_eq!(
            serde_json::to_value(&result).unwrap()["content"][0]["text"],
            "partial"
        );
        assert!(
            !serde_json::to_string(&result)
                .unwrap()
                .contains("synthetic-private")
        );
        assert_eq!(observed.closed.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn sdk_falsy_and_empty_json_errors_do_not_invent_overflow() {
    // SDK client.ts errMessage = errJSON ? undefined : errText; APIError.generate
    // selects errorResponse.error. The wrapper's OpenAI API error prefix means
    // Cerebras's anchored no-body matcher does not apply even to empty objects.
    for (body, overflow) in [
        ("false", false),
        ("0", false),
        ("null", false),
        ("\"\"", false),
        ("{}", false),
        ("[]", false),
    ] {
        let (api, spawner, _, _, mut request, options) =
            setup(&first(), 400, vec![body.as_bytes().to_vec()], false);
        request.resolved.model.provider = "cerebras".parse().unwrap();
        let stream = api.stream_with_options(request, options);
        ready(spawner.take());
        assert_eq!(
            is_context_overflow(&ready(stream.result()), None),
            overflow,
            "{body}"
        );
    }
}

struct ReadFailureBody;
impl ResponseBody for ReadFailureBody {
    fn next<'a>(
        &'a mut self,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, TransportError>> {
        Box::pin(async {
            Err(TransportError::new(
                pi_ai_transport::TransportErrorKind::Connection,
            ))
        })
    }
    fn close(&mut self) {}
}

#[test]
fn complete_sdk_errors_and_cap_prefix_classify_final_messages_from_source() {
    let oracle: Value = retained_support::fixture(include_str!(
        "../../../conformance/provider-traces/openai-responses/retry-cap.json"
    ));
    assert_eq!(oracle["cases"].as_array().unwrap().len(), 36);
    for case in oracle["cases"].as_array().unwrap() {
        let (mut api, spawner, observed, _, mut request, options) =
            setup(&first(), 200, vec![], false);
        request.options.wire.max_retries =
            Some(SafeUnsigned::new(u64::from(case["cap"] == true)).unwrap());
        request.options.wire.max_retry_delay_ms = Some(SafeUnsigned::new(1).unwrap());
        let mut headers = HttpHeaders::new();
        for (name, value) in case["headers"].as_object().unwrap() {
            headers.set(name, value.as_str().unwrap()).unwrap();
        }
        let body: Box<dyn ResponseBody> = if case["readFailure"] == true {
            Box::new(ReadFailureBody)
        } else {
            Box::new(Body {
                chunks: vec![case["body"].as_str().unwrap().as_bytes().to_vec()].into(),
                block: false,
                observed: observed.clone(),
            })
        };
        api.services.transport = Arc::new(HttpTransport::new(
            Arc::new(Client {
                responses: Mutex::new(VecDeque::from([HttpResponse {
                    status: case["status"].as_u64().unwrap() as u16,
                    status_text: String::new(),
                    headers,
                    body,
                }])),
                observed: observed.clone(),
                block: false,
            }),
            Arc::new(Jitter),
        ));
        let mut stream = api.stream_with_options(request, options);
        ready(spawner.take());
        let message = retained_support::message(ready(stream.result()));
        assert_eq!(
            (
                is_context_overflow(&message, None),
                is_retryable_assistant_error(&message)
            ),
            (
                case["contextOverflow"].as_bool().unwrap(),
                case["retryable"].as_bool().unwrap()
            ),
            "{}",
            case["id"]
        );
        assert_eq!(
            observed.requests.lock().unwrap().len(),
            case["calls"].as_u64().unwrap() as usize
        );
        assert!(!observed.order.lock().unwrap().contains(&"response"));
        assert!(matches!(
            ready(stream.next()),
            Some(AssistantMessageEvent::Error { .. })
        ));
        assert!(ready(stream.next()).is_none());
        assert_eq!(
            serde_json::to_value(&message).unwrap()["diagnostics"][0]["details"]["status"],
            case["status"]
        );
        assert!(
            !message
                .error_message
                .unwrap()
                .contains(&case["status"].to_string())
        );
    }
}

struct GatedErrorBody {
    bytes: Option<Vec<u8>>,
    eof: Arc<AtomicBool>,
    observed: Arc<Observed>,
}
impl ResponseBody for GatedErrorBody {
    fn next<'a>(
        &'a mut self,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, TransportError>> {
        self.observed.order.lock().unwrap().push("body");
        if let Some(bytes) = self.bytes.take() {
            return Box::pin(async move { Ok(Some(bytes)) });
        }
        Box::pin(std::future::poll_fn(|_| {
            if self.eof.load(Ordering::SeqCst) {
                Poll::Ready(Ok(None))
            } else {
                Poll::Pending
            }
        }))
    }
    fn close(&mut self) {
        self.observed.closed.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn error_body_eof_precedes_retry_and_cancellation_never_starts_successor() {
    for cancel in [false, true] {
        let case = first();
        let (mut api, spawner, observed, _, mut request, options) =
            setup(&case, 200, vec![], false);
        let eof = Arc::new(AtomicBool::new(false));
        let mut headers = HttpHeaders::new();
        headers.set("retry-after-ms", "0").unwrap();
        let first = HttpResponse {
            status: 429,
            status_text: String::new(),
            headers,
            body: Box::new(GatedErrorBody {
                bytes: Some(b"context length exceeded".to_vec()),
                eof: eof.clone(),
                observed: observed.clone(),
            }),
        };
        let second = HttpResponse {
            status: 200,
            status_text: String::new(),
            headers: HttpHeaders::new(),
            body: Box::new(Body {
                chunks: vec![case["sse"].as_str().unwrap().as_bytes().to_vec()].into(),
                block: false,
                observed: observed.clone(),
            }),
        };
        api.services.transport = Arc::new(HttpTransport::new(
            Arc::new(Client {
                responses: Mutex::new(VecDeque::from([first, second])),
                observed: observed.clone(),
                block: false,
            }),
            Arc::new(Jitter),
        ));
        request.options.wire.max_retries = Some(SafeUnsigned::new(1).unwrap());
        let token = request.options.runtime.cancellation.clone();
        let stream = api.stream_with_options(request, options);
        let mut worker = spawner.take();
        assert!(
            worker
                .as_mut()
                .poll(&mut TaskContext::from_waker(Waker::noop()))
                .is_pending()
        );
        assert_eq!(observed.requests.lock().unwrap().len(), 1);
        assert!(!observed.order.lock().unwrap().contains(&"response"));
        assert_eq!(observed.closed.load(Ordering::SeqCst), 0);
        if cancel {
            token.cancel();
        } else {
            eof.store(true, Ordering::SeqCst);
        }
        ready(worker);
        assert_eq!(
            ready(stream.result()).stop_reason,
            if cancel {
                StopReason::Aborted
            } else {
                StopReason::Stop
            }
        );
        assert_eq!(
            observed.requests.lock().unwrap().len(),
            if cancel { 1 } else { 2 }
        );
        assert_eq!(
            observed
                .order
                .lock()
                .unwrap()
                .iter()
                .filter(|v| **v == "payload")
                .count(),
            1
        );
        assert_eq!(
            observed.closed.load(Ordering::SeqCst),
            if cancel { 1 } else { 2 }
        );
    }
}

#[test]
fn affinity_infers_credential_endpoint_without_mutating_public_model_url() {
    let case = first();
    let (api, spawner, observed, _, mut request, options) = setup(
        &case,
        200,
        vec![case["sse"].as_str().unwrap().as_bytes().to_vec()],
        false,
    );
    let model_before = serde_json::to_value(&request.resolved.model).unwrap();
    request.resolved.endpoint =
        RequestEndpoint::new_resolved("https://openrouter.ai/api/v1").unwrap();
    request.options.wire.session_id = Some("credential-session".into());
    assert_eq!(
        serde_json::to_value(&request.resolved.model).unwrap(),
        model_before
    );
    let stream = api.stream_with_options(request, options);
    ready(spawner.take());
    assert_eq!(ready(stream.result()).stop_reason, StopReason::Stop);
    assert_eq!(observed.models.lock().unwrap()[0], model_before);
    let requests = observed.requests.lock().unwrap();
    assert_eq!(requests[0]["url"], "https://openrouter.ai/api/v1/responses");
    assert_eq!(requests[0]["headers"]["x-session-id"], "credential-session");
    assert!(requests[0]["headers"].get("session_id").is_none());
}

#[test]
fn cancellation_after_usage_only_update_preserves_snapshot_and_drops_pending_body() {
    let case = first();
    let (api, spawner, observed, _, request, options) = setup(
        &case,
        200,
        vec![case["sse"].as_str().unwrap().as_bytes().to_vec()],
        true,
    );
    let token = request.options.runtime.cancellation.clone();
    let mut stream = api.stream_with_options(request, options);
    let mut worker = spawner.take();
    assert!(
        worker
            .as_mut()
            .poll(&mut TaskContext::from_waker(Waker::noop()))
            .is_pending()
    );
    token.cancel();
    let result = retained_support::message(ready(stream.result()));
    assert_eq!(result.stop_reason, StopReason::Aborted);
    assert_eq!(result.usage.input.get(), 98);
    assert_eq!(
        serde_json::to_value(&result).unwrap()["content"][0]["text"],
        "hello"
    );
    ready(worker);
    assert_eq!(observed.dropped.load(Ordering::SeqCst), 1);
    assert_eq!(observed.closed.load(Ordering::SeqCst), 1);
    let mut terminals = 0;
    while let Some(event) = ready(stream.next()) {
        if matches!(
            event,
            AssistantMessageEvent::Error { .. } | AssistantMessageEvent::Done { .. }
        ) {
            terminals += 1;
        }
    }
    assert_eq!(terminals, 1);
}

#[test]
fn default_dispatch_and_spawn_api_mismatch_failures_keep_setup_in_band() {
    let case = first();
    let (mut api, spawner, observed, _, request, _) = setup(
        &case,
        200,
        vec![case["sse"].as_str().unwrap().as_bytes().to_vec()],
        false,
    );
    api.defaults = serde_json::from_value(json!({"reasoningEffort":"high"})).unwrap();
    let stream = api.stream(request);
    ready(spawner.take());
    assert_eq!(ready(stream.result()).stop_reason, StopReason::Stop);
    assert_eq!(
        observed.requests.lock().unwrap()[0]["body"]["reasoning"],
        json!({"effort":"high","summary":"auto"})
    );
    for reject_spawn in [false, true] {
        let (api, spawner, observed, _, mut request, options) = setup(&case, 200, vec![], false);
        if reject_spawn {
            request.options.runtime.spawner = Arc::new(Spawner {
                reject: true,
                ..Default::default()
            });
        } else {
            request.resolved.model.api = "other-api".parse().unwrap();
        }
        let mut stream = api.stream_with_options(request, options);
        assert_eq!(ready(stream.result()).stop_reason, StopReason::Error);
        assert!(matches!(
            ready(stream.next()),
            Some(AssistantMessageEvent::Error { .. })
        ));
        assert!(ready(stream.next()).is_none());
        assert!(spawner.tasks.lock().unwrap().is_empty());
        assert!(observed.requests.lock().unwrap().is_empty());
    }
}

#[test]
fn aggregate_input_bound_is_exact_and_body_failures_do_not_replay() {
    let case = first();
    let bytes = case["sse"].as_str().unwrap().as_bytes();
    for budget in [bytes.len(), bytes.len() - 1] {
        let (mut api, spawner, observed, _, mut request, options) = setup(
            &case,
            200,
            bytes.chunks(3).map(<[u8]>::to_vec).collect(),
            false,
        );
        api.max_stream_bytes = budget;
        request.options.wire.max_retries = Some(SafeUnsigned::new(3).unwrap());
        let stream = api.stream_with_options(request, options);
        ready(spawner.take());
        assert_eq!(
            ready(stream.result()).stop_reason,
            if budget == bytes.len() {
                StopReason::Stop
            } else {
                StopReason::Error
            }
        );
        assert_eq!(observed.requests.lock().unwrap().len(), 1);
        assert_eq!(observed.closed.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn cancelled_blocked_send_stays_aborted_and_runs_no_response_middleware() {
    let (mut api, spawner, observed, clock, request, options) = setup(&first(), 200, vec![], false);
    api.services.transport = Arc::new(HttpTransport::new(
        Arc::new(Client {
            responses: Mutex::new(VecDeque::new()),
            observed: observed.clone(),
            block: true,
        }),
        Arc::new(Jitter),
    ));
    let token = request.options.runtime.cancellation.clone();
    let stream = api.stream_with_options(request, options);
    let mut worker = spawner.take();
    assert!(
        worker
            .as_mut()
            .poll(&mut TaskContext::from_waker(Waker::noop()))
            .is_pending()
    );
    token.cancel();
    ready(worker);
    assert_eq!(ready(stream.result()).stop_reason, StopReason::Aborted);
    assert!(!observed.order.lock().unwrap().contains(&"response"));
    assert_eq!(
        *clock.sleeps.lock().unwrap(),
        vec![Duration::from_millis(600000)]
    );
}

#[test]
fn middleware_cancellation_and_auth_preflight_never_send_or_publish_start_early() {
    for response in [false, true] {
        let (mut api, spawner, observed, _, request, options) = setup(&first(), 200, vec![], true);
        let middleware = Arc::new(Middleware {
            observed: observed.clone(),
            replacement: None,
            block_payload: !response,
            block_response: response,
        });
        api.services.payload = Some(middleware.clone());
        api.services.response = Some(middleware);
        let token = request.options.runtime.cancellation.clone();
        let mut stream = api.stream_with_options(request, options);
        let mut worker = spawner.take();
        assert!(
            worker
                .as_mut()
                .poll(&mut TaskContext::from_waker(Waker::noop()))
                .is_pending()
        );
        token.cancel();
        ready(worker);
        assert_eq!(ready(stream.result()).stop_reason, StopReason::Aborted);
        assert!(matches!(
            ready(stream.next()),
            Some(AssistantMessageEvent::Error { .. })
        ));
        assert_eq!(
            observed.requests.lock().unwrap().len(),
            usize::from(response)
        );
        assert_eq!(
            observed.closed.load(Ordering::SeqCst),
            usize::from(response)
        );
    }
    for value in [None, Some(""), Some(" \u{feff}\t")] {
        let (api, spawner, observed, _, mut request, options) = setup(&first(), 200, vec![], false);
        request.auth.api_key = None;
        request.auth.headers = ProviderHeaders::new(BTreeMap::from([(
            "Authorization".to_owned(),
            value.map(str::to_owned),
        )]));
        let stream = api.stream_with_options(request, options);
        ready(spawner.take());
        assert_eq!(ready(stream.result()).stop_reason, StopReason::Error);
        assert!(observed.order.lock().unwrap().is_empty());
    }
}

#[test]
fn sdk_timeout_retry_defaults_and_explicit_overrides_have_one_owner() {
    let case = first();
    let (mut api, spawner, observed, clock, mut request, options) =
        setup(&case, 200, vec![], false);
    api.services.transport = Arc::new(HttpTransport::new(
        Arc::new(Client {
            responses: Mutex::new(VecDeque::new()),
            observed: observed.clone(),
            block: true,
        }),
        Arc::new(Jitter),
    ));
    request.options.runtime.clock = Arc::new(ClockProbe {
        timeout: true,
        ..Default::default()
    });
    let stream = api.stream_with_options(request, options);
    ready(spawner.take());
    let result = retained_support::message(ready(stream.result()));
    assert_eq!(result.stop_reason, StopReason::Error);
    assert!(is_retryable_assistant_error(&result));
    assert_eq!(clock.sleeps.lock().unwrap().len(), 0);
    // Header timer wins before a blocked send, independently of caller cancellation.
    for retries in [0, 1] {
        let (mut api, spawner, observed, clock, mut request, options) =
            setup(&case, 200, vec![], false);
        let response = |status| HttpResponse {
            status,
            status_text: String::new(),
            headers: HttpHeaders::new(),
            body: Box::new(Body {
                chunks: vec![case["sse"].as_str().unwrap().as_bytes().to_vec()].into(),
                block: false,
                observed: observed.clone(),
            }),
        };
        api.services.transport = Arc::new(HttpTransport::new(
            Arc::new(Client {
                responses: Mutex::new(VecDeque::from([response(500), response(200)])),
                observed: observed.clone(),
                block: false,
            }),
            Arc::new(Jitter),
        ));
        request.options.wire.max_retries = Some(SafeUnsigned::new(retries).unwrap());
        let stream = api.stream_with_options(request, options);
        ready(spawner.take());
        assert_eq!(
            ready(stream.result()).stop_reason,
            if retries == 0 {
                StopReason::Error
            } else {
                StopReason::Stop
            }
        );
        assert_eq!(
            observed.requests.lock().unwrap().len(),
            retries as usize + 1
        );
        assert_eq!(observed.closed.load(Ordering::SeqCst), retries as usize + 1);
        assert_eq!(
            clock
                .sleeps
                .lock()
                .unwrap()
                .iter()
                .filter(|v| **v == Duration::from_millis(500))
                .count(),
            retries as usize
        );
        for request in observed.requests.lock().unwrap().iter() {
            assert_eq!(request["headers"]["x-stainless-retry-count"], "0");
        }
    }
}

#[test]
fn sdk_environment_defaults_are_explicit_and_request_headers_override_them() {
    let (mut api, spawner, observed, _, mut request, options) =
        setup(&first(), 200, vec![b"data: [DONE]\n\n".to_vec()], false);
    api.services.env = ProviderEnv::new(BTreeMap::from([
        ("OPENAI_ORG_ID".to_owned(), "org".to_owned()),
        ("OPENAI_PROJECT_ID".to_owned(), "project".to_owned()),
        (
            "OPENAI_CUSTOM_HEADERS".to_owned(),
            "X-Custom: env\nAccept: env\nUser-Agent: env".to_owned(),
        ),
    ]));
    request.auth.headers = ProviderHeaders::new(BTreeMap::from([("X-Custom".to_owned(), None)]));
    let stream = api.stream_with_options(request, options);
    ready(spawner.take());
    assert_eq!(ready(stream.result()).stop_reason, StopReason::Error);
    let requests = observed.requests.lock().unwrap();
    let headers = &requests[0]["headers"];
    assert_eq!(headers["openai-organization"], "org");
    assert_eq!(headers["openai-project"], "project");
    assert_eq!(headers["accept"], "env");
    assert_eq!(headers["user-agent"], pi_ai_core::format_user_agent(None));
    assert!(headers.get("x-custom").is_none());
}

#[test]
fn response_callback_receives_exact_pinned_own_header_record() {
    let fixture: Value = retained_support::fixture(include_str!(
        "../../../conformance/provider-helpers/response-head/headers.json"
    ));
    let case = first();
    let (mut api, spawner, observed, _, request, options) = setup(&case, 200, vec![], false);
    let mut headers = HttpHeaders::new();
    for entry in fixture["raw"].as_array().unwrap() {
        headers
            .append(entry[0].as_str().unwrap(), entry[1].as_str().unwrap())
            .unwrap();
    }
    let response = HttpResponse {
        status: 200,
        status_text: String::new(),
        headers,
        body: Box::new(Body {
            chunks: vec![case["sse"].as_str().unwrap().as_bytes().to_vec()].into(),
            block: false,
            observed: observed.clone(),
        }),
    };
    api.services.transport = Arc::new(HttpTransport::new(
        Arc::new(Client {
            responses: Mutex::new(VecDeque::from([response])),
            observed: observed.clone(),
            block: false,
        }),
        Arc::new(Jitter),
    ));
    let stream = api.stream_with_options(request, options);
    ready(spawner.take());
    assert_eq!(ready(stream.result()).stop_reason, StopReason::Stop);
    let heads = observed.response_heads.lock().unwrap();
    assert_eq!(heads.len(), 1);
    assert_eq!(heads[0].status, 200);
    assert_eq!(
        serde_json::to_value(
            heads[0]
                .headers
                .iter()
                .map(|(key, value)| (key, value.as_str().expect("Fetch string record")))
                .collect::<Vec<_>>()
        )
        .unwrap(),
        fixture["ownRecordEntries"]
    );
}

#[test]
fn sdk_client_default_header_validation_keeps_its_eager_or_lazy_stage() {
    let fixture: Value = retained_support::fixture(include_str!(
        "../../../conformance/provider-traces/openai-responses/header-stage.json"
    ));
    for case in fixture["cases"].as_array().unwrap() {
        let (mut api, spawner, observed, clock, request, options) = setup(case, 200, vec![], false);
        if let Some(custom) = case["clientCustomHeaders"].as_str() {
            api.services.env = ProviderEnv::new(BTreeMap::from([(
                "OPENAI_CUSTOM_HEADERS".to_owned(),
                custom.to_owned(),
            )]));
        }
        let stream = api.stream_with_options(request, options);
        ready(spawner.take());
        let result = retained_support::message(ready(stream.result()));
        assert_eq!(result.stop_reason, StopReason::Error);
        assert!(!is_retryable_assistant_error(&result));
        assert_eq!(
            serde_json::to_value(&*observed.order.lock().unwrap()).unwrap(),
            case["order"]
        );
        assert_eq!(observed.requests.lock().unwrap().len(), 0);
        assert!(clock.sleeps.lock().unwrap().is_empty());
    }
}

#[cfg(feature = "native")]
#[test]
fn native_redirect_chain_preserves_success_trace_and_final_error_body_veto() {
    use pi_ai_transport::NativeHttpClient;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::thread;
    fn receive(socket: &mut TcpStream) -> (String, Vec<u8>) {
        socket
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        socket
            .set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut bytes = Vec::new();
        while !bytes.ends_with(b"\r\n\r\n") {
            let mut byte = [0];
            socket.read_exact(&mut byte).unwrap();
            bytes.push(byte[0]);
            assert!(bytes.len() < 16384);
        }
        let head = String::from_utf8(bytes).unwrap();
        let length = head
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().unwrap())
            })
            .unwrap_or(0);
        let mut body = vec![0; length];
        socket.read_exact(&mut body).unwrap();
        (head, body)
    }
    fn released(socket: &mut TcpStream) {
        match socket.read(&mut [0]) {
            Ok(0) => {}
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
                ) => {}
            other => panic!("socket release must be EOF/reset: {other:?}"),
        }
    }
    for mode in ["success", "error", "cancel-error"] {
        let source = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = source.local_addr().unwrap();
        let target = TcpListener::bind("127.0.0.1:0").unwrap();
        let target_address = target.local_addr().unwrap();
        let source_worker = thread::spawn(move || {
            let (mut socket, _) = source.accept().unwrap();
            let initial = receive(&mut socket);
            socket
                .write_all(
                    b"HTTP/1.1 307 Redirect\r\nLocation: hop\r\nContent-Length: 999\r\n\r\nx",
                )
                .unwrap();
            socket.flush().unwrap();
            released(&mut socket);
            let (mut socket, _) = source.accept().unwrap();
            let relative = receive(&mut socket);
            write!(socket,"HTTP/1.1 302 Redirect\r\nLocation: http://{target_address}/final\r\nContent-Length: 999\r\n\r\nx").unwrap();
            socket.flush().unwrap();
            released(&mut socket);
            (initial, relative)
        });
        let case = first();
        let (mut api, spawner, observed, clock, mut request, options) =
            setup(&case, 200, vec![], false);
        let token = request.options.runtime.cancellation.clone();
        let bytes = if mode == "success" {
            case["sse"].as_str().unwrap().as_bytes().to_vec()
        } else {
            b"billing".to_vec()
        };
        let target_worker = thread::spawn(move || {
            let (mut socket, _) = target.accept().unwrap();
            let request = receive(&mut socket);
            write!(socket,"HTTP/1.1 {} Response\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n",if mode=="success"{200}else{429}).unwrap();
            for chunk in bytes.chunks(3) {
                write!(socket, "{:x}\r\n", chunk.len()).unwrap();
                socket.write_all(chunk).unwrap();
                socket.write_all(b"\r\n").unwrap();
                socket.flush().unwrap();
            }
            if mode == "cancel-error" {
                token.cancel();
            } else {
                socket.write_all(b"0\r\n\r\n").unwrap();
                socket.flush().unwrap();
            }
            released(&mut socket);
            request
        });
        let engine = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        request.resolved.endpoint =
            RequestEndpoint::new_resolved(format!("http://{address}/v1")).unwrap();
        request.auth.headers = ProviderHeaders::new(BTreeMap::from([
            ("cookie".into(), Some("synthetic-cookie".into())),
            ("x-custom".into(), Some("synthetic".into())),
        ]));
        api.services.transport = Arc::new(HttpTransport::new(
            Arc::new(NativeHttpClient::new(engine.handle().clone())),
            Arc::new(Jitter),
        ));
        let mut stream = api.stream_with_options(request, options);
        let worker = spawner.take();
        let (result, events) = engine.block_on(async {
            tokio::time::timeout(Duration::from_secs(5), async {
                let worker = tokio::spawn(worker);
                let mut events = Vec::new();
                while let Some(event) = stream.next().await {
                    events.push(retained_support::untimed(event));
                }
                worker.await.unwrap();
                (retained_support::message(stream.result().await), events)
            })
            .await
            .unwrap()
        });
        let ((initial, body), (relative, replayed)) = source_worker.join().unwrap();
        let (final_head, final_body) = target_worker.join().unwrap();
        assert!(initial.starts_with("POST /v1/responses HTTP/1.1\r\n"));
        assert!(relative.starts_with("POST /v1/hop HTTP/1.1\r\n"));
        assert_eq!(body, replayed);
        assert_eq!(
            serde_json::from_slice::<Value>(&body).unwrap(),
            case["requests"][0]["body"]
        );
        for head in [&initial, &relative] {
            let head = head.to_ascii_lowercase();
            assert!(head.contains("authorization: bearer synthetic-key\r\n"));
            assert!(head.contains("cookie: synthetic-cookie\r\n"));
        }
        let head = final_head.to_ascii_lowercase();
        assert!(head.starts_with("get /final http/1.1\r\n"));
        assert!(final_body.is_empty());
        assert!(!head.contains("authorization:"));
        assert!(!head.contains("cookie:"));
        assert!(!head.contains("content-type:"));
        assert!(head.contains("x-custom: synthetic\r\n"));
        assert_eq!(
            *clock.sleeps.lock().unwrap(),
            vec![Duration::from_millis(600000)]
        );
        if mode == "success" {
            assert_eq!(
                events,
                serde_json::from_value::<Vec<AssistantMessageEvent>>(case["events"].clone())
                    .unwrap()
            );
            assert_eq!(result.stop_reason, StopReason::Stop);
            assert!(observed.order.lock().unwrap().contains(&"response"));
        } else {
            assert_eq!(events.len(), 1);
            assert!(!observed.order.lock().unwrap().contains(&"response"));
            assert_eq!(
                result.stop_reason,
                if mode == "error" {
                    StopReason::Error
                } else {
                    StopReason::Aborted
                }
            );
            assert!(!is_retryable_assistant_error(&result));
            if mode == "error" {
                assert_eq!(
                    serde_json::to_value(&result).unwrap()["diagnostics"][0]["details"]["status"],
                    429
                );
            }
        }
    }
}

#[cfg(feature = "native")]
#[test]
fn native_loopback_request_fragmented_sse_and_abort_release_the_socket() {
    use pi_ai_transport::NativeHttpClient;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;
    for cancel in [false, true] {
        let case = first();
        let bytes = case["sse"].as_str().unwrap().as_bytes().to_vec();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            socket
                .set_write_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut head = Vec::new();
            while !head.ends_with(b"\r\n\r\n") {
                let mut byte = [0];
                socket.read_exact(&mut byte).unwrap();
                head.push(byte[0]);
                assert!(head.len() < 16384);
            }
            let head = String::from_utf8(head).unwrap();
            let length = head
                .lines()
                .find_map(|line| {
                    let (key, value) = line.split_once(':')?;
                    key.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().unwrap())
                })
                .unwrap();
            let mut body = vec![0; length];
            socket.read_exact(&mut body).unwrap();
            socket.write_all(b"HTTP/1.1 200 Synthetic\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n").unwrap();
            for chunk in bytes.chunks(3) {
                write!(socket, "{:x}\r\n", chunk.len()).unwrap();
                socket.write_all(chunk).unwrap();
                socket.write_all(b"\r\n").unwrap();
                socket.flush().unwrap();
            }
            // Standard SDK keeps consuming after response.completed; success
            // needs EOF, while cancellation must release a still-blocked body.
            if !cancel {
                socket.write_all(b"0\r\n\r\n").unwrap();
                socket.flush().unwrap();
            }
            let closed = match socket.read(&mut [0]) {
                Ok(0) => true,
                Err(error) => matches!(
                    error.kind(),
                    std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
                ),
                _ => false,
            };
            (
                head,
                serde_json::from_slice::<Value>(&body).unwrap(),
                closed,
            )
        });
        let engine = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let (mut api, spawner, _, _, mut request, options) = setup(&case, 200, vec![], false);
        request.resolved.endpoint =
            RequestEndpoint::new_resolved(format!("http://{address}/v1/")).unwrap();
        let token = request.options.runtime.cancellation.clone();
        api.services.transport = Arc::new(HttpTransport::new(
            Arc::new(NativeHttpClient::new(engine.handle().clone())),
            Arc::new(Jitter),
        ));
        let mut stream = api.stream_with_options(request, options);
        let task = spawner.take();
        let result = engine.block_on(async {
            tokio::time::timeout(Duration::from_secs(5), async {
                let worker = tokio::spawn(task);
                while let Some(event) = stream.next().await {
                    if cancel && matches!(event, AssistantMessageEvent::TextEnd { .. }) {
                        token.cancel();
                    }
                }
                worker.await.unwrap();
                retained_support::message(stream.result().await)
            })
            .await
            .unwrap()
        });
        let (head, body, closed) = server.join().unwrap();
        assert!(
            closed,
            "socket release requires EOF/reset, not read timeout"
        );
        assert!(head.starts_with("POST /v1/responses HTTP/1.1\r\n"));
        let head = head.to_ascii_lowercase();
        assert!(head.contains("authorization: bearer synthetic-key\r\n"));
        assert!(head.contains("accept: application/json\r\n"));
        assert!(head.contains("content-type: application/json\r\n"));
        assert_eq!(body, case["requests"][0]["body"]);
        assert_eq!(
            serde_json::to_value(&result).unwrap()["content"][0]["text"],
            "hello"
        );
        if cancel {
            assert_eq!(result.stop_reason, StopReason::Aborted);
        } else {
            assert_eq!(result.stop_reason, StopReason::Stop);
            assert_eq!(result.usage.input.get(), 98);
        }
    }
}
