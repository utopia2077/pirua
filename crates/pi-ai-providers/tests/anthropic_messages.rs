#![cfg(feature = "wire-anthropic-messages")]
use pi_ai_core::*;
use pi_ai_providers::api::{anthropic_messages::*, runtime::*};
use pi_ai_transport::*;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, VecDeque},
    future::{Future, pending},
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context as TaskContext, Poll, Waker},
    time::Duration,
};

fn fixture() -> Value {
    serde_json::from_str(include_str!(
        "../../../conformance/provider-traces/anthropic-messages/trace.json"
    ))
    .unwrap()
}
#[derive(Default)]
struct TestClock {
    expired: AtomicBool,
    backoff: AtomicBool,
    waits: Mutex<Vec<Duration>>,
}
impl Clock for TestClock {
    fn now_unix_ms(&self) -> u64 {
        7
    }
    fn sleep<'a>(
        &'a self,
        d: Duration,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), Cancelled>> {
        self.waits.lock().unwrap().push(d);
        Box::pin(std::future::poll_fn(move |_| {
            if self.expired.load(Ordering::SeqCst)
                || (self.backoff.load(Ordering::SeqCst) && d < Duration::from_secs(600))
            {
                Poll::Ready(Ok(()))
            } else {
                Poll::Pending
            }
        }))
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
struct Observed {
    requests: Mutex<Vec<(String, HttpHeaders, Value)>>,
    order: Mutex<Vec<&'static str>>,
    reads: AtomicUsize,
    closes: AtomicUsize,
}
struct Body {
    chunks: VecDeque<Vec<u8>>,
    block: bool,
    gate: Option<Arc<AtomicBool>>,
    observed: Arc<Observed>,
}
impl ResponseBody for Body {
    fn next<'a>(
        &'a mut self,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, TransportError>> {
        self.observed.reads.fetch_add(1, Ordering::SeqCst);
        self.observed.order.lock().unwrap().push("body");
        if self.chunks.is_empty() && self.block {
            return Box::pin(pending());
        }
        if self.chunks.is_empty()
            && let Some(gate) = &self.gate
            && !gate.load(Ordering::SeqCst)
        {
            return Box::pin(std::future::poll_fn(move |_| {
                if gate.load(Ordering::SeqCst) {
                    Poll::Ready(Ok(None))
                } else {
                    Poll::Pending
                }
            }));
        }
        let value = self.chunks.pop_front();
        Box::pin(async move { Ok(value) })
    }
    fn close(&mut self) {
        self.observed.closes.fetch_add(1, Ordering::SeqCst);
    }
}
struct Client {
    responses: Mutex<VecDeque<HttpResponse>>,
    observed: Arc<Observed>,
    fail: Mutex<Option<TransportErrorKind>>,
}
impl RawHttpClient for Client {
    fn send<'a>(
        &'a self,
        request: &'a HttpRequest,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<HttpResponse, TransportError>> {
        Box::pin(async move {
            self.observed.order.lock().unwrap().push("send");
            self.observed.requests.lock().unwrap().push((
                request.endpoint.expose_for_transport().into(),
                request.headers.clone(),
                serde_json::from_slice(&request.body).unwrap(),
            ));
            if let Some(kind) = self.fail.lock().unwrap().take() {
                return Err(TransportError::new(kind));
            }
            Ok(self
                .responses
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected attempt"))
        })
    }
}
struct Jitter;
impl RetryJitter for Jitter {
    fn sample(&self) -> f64 {
        0.0
    }
}
struct Setup {
    api: AnthropicMessagesApi,
    client: Arc<Client>,
    spawner: Arc<Spawner>,
    observed: Arc<Observed>,
    clock: Arc<TestClock>,
    request: TextStreamRequest,
}
fn response(
    status: u16,
    chunks: Vec<Vec<u8>>,
    block: bool,
    observed: Arc<Observed>,
) -> HttpResponse {
    let mut headers = HttpHeaders::new();
    headers.set("content-type", "text/event-stream").unwrap();
    headers.set("request-id", "request-fixture").unwrap();
    HttpResponse {
        status,
        status_text: "Ignored SDK reason".into(),
        headers,
        body: Box::new(Body {
            chunks: chunks.into(),
            block,
            gate: None,
            observed,
        }),
    }
}
fn setup(status: u16, chunks: Vec<Vec<u8>>, block: bool) -> Setup {
    let observed = Arc::new(Observed::default());
    let spawner = Arc::new(Spawner::default());
    let clock = Arc::new(TestClock::default());
    let client = Arc::new(Client {
        fail: Mutex::new(None),
        responses: Mutex::new(VecDeque::from([response(
            status,
            chunks,
            block,
            observed.clone(),
        )])),
        observed: observed.clone(),
    });
    // The source oracle observes custom-fetch arguments and returns a constructed Response.
    let api = AnthropicMessagesApi::new(AdapterServices::new(Arc::new(
        HttpTransport::new(client.clone(), Arc::new(Jitter))
            .with_fetch_response_source(FetchResponseSource::Preconstructed),
    )));
    let f = fixture();
    let request = TextStreamRequest {
        resolved: ResolvedRequest {
            model: serde_json::from_value(f["model"].clone()).unwrap(),
            endpoint: RequestEndpoint::new_resolved("https://gateway.invalid/prefix").unwrap(),
        },
        context: serde_json::from_value(f["context"].clone()).unwrap(),
        auth: ResolvedAuth {
            api_key: Some(SecretString::new("fixture-key")),
            ..Default::default()
        },
        options: StreamOptions {
            wire: StreamOptionsWire::default(),
            runtime: RequestRuntime::with_noop_telemetry(
                CancellationToken::new(),
                clock.clone(),
                spawner.clone(),
            ),
        },
    };
    Setup {
        api,
        client,
        spawner,
        observed,
        clock,
        request,
    }
}
fn poll<F: Future + ?Sized>(f: Pin<&mut F>) -> Poll<F::Output> {
    f.poll(&mut TaskContext::from_waker(Waker::noop()))
}
fn ready<F: Future>(f: F) -> F::Output {
    let mut f = Box::pin(f);
    match poll(f.as_mut()) {
        Poll::Ready(v) => v,
        Poll::Pending => panic!("expected ready"),
    }
}
fn sse(events: &[Value]) -> Vec<u8> {
    events
        .iter()
        .map(|event| {
            format!(
                "event: {}\ndata: {event}\n\n",
                event["type"].as_str().unwrap()
            )
        })
        .collect::<String>()
        .into_bytes()
}
fn ending() -> Vec<Value> {
    vec![
        json!({"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":1}}),
        json!({"type":"message_stop"}),
    ]
}
fn complete(s: Setup, options: AnthropicOptions) -> (AssistantMessage, Vec<Value>) {
    let mut stream = s.api.stream_with_options(s.request, options);
    if !s.spawner.tasks.lock().unwrap().is_empty() {
        ready(s.spawner.take());
    }
    let result = ready(stream.result());
    let mut events = vec![];
    while let Some(event) = ready(stream.next()) {
        events.push(serde_json::to_value(event).unwrap());
    }
    (result, events)
}

struct Hooks {
    observed: Arc<Observed>,
    payload: Mutex<Option<Value>>,
    replace: Option<Value>,
    mutate: bool,
    fail: bool,
    block: bool,
    response_only: bool,
}
impl PayloadMiddleware for Hooks {
    fn on_payload<'a>(
        &'a self,
        _: &'a TextStreamRequest,
        payload: &'a mut JsonValue,
    ) -> BoxFuture<'a, Result<Option<JsonValue>, ModelsError>> {
        Box::pin(async move {
            self.observed.order.lock().unwrap().push("payload");
            *self.payload.lock().unwrap() = Some(json!(payload));
            if self.block && !self.response_only {
                return pending().await;
            }
            if self.fail && !self.response_only {
                return Err(ModelsError::new(ModelsErrorCode::Stream));
            }
            if self.mutate {
                *payload =
                    serde_json::from_value(json!({"model":"mutated","stream":false})).unwrap();
            }
            Ok(self
                .replace
                .as_ref()
                .map(|v| serde_json::from_value(v.clone()).unwrap()))
        })
    }
}
impl ResponseMiddleware for Hooks {
    fn on_response<'a>(
        &'a self,
        _: &'a TextStreamRequest,
        head: &'a AdapterResponseHead,
    ) -> BoxFuture<'a, Result<(), ModelsError>> {
        Box::pin(async move {
            self.observed.order.lock().unwrap().push("response");
            assert_eq!(head.status, 200);
            if self.block && self.response_only {
                return pending().await;
            }
            if self.fail && self.response_only {
                return Err(ModelsError::new(ModelsErrorCode::Stream));
            }
            Ok(())
        })
    }
}
fn hooks(s: &mut Setup) -> Arc<Hooks> {
    let hook = Arc::new(Hooks {
        observed: s.observed.clone(),
        payload: Mutex::new(None),
        replace: None,
        mutate: false,
        fail: false,
        block: false,
        response_only: false,
    });
    s.api.services.payload = Some(hook.clone());
    s.api.services.response = Some(hook.clone());
    hook
}

#[test]
fn installed_sdk_request_and_complete_pinned_event_snapshots() {
    let f = fixture();
    let bytes = sse(f["chunks"].as_array().unwrap());
    let mut s = setup(200, vec![bytes], false);
    let hook = hooks(&mut s);
    let observed = s.observed.clone();
    s.request.options.wire.max_tokens = Some(SafeUnsigned::new(99).unwrap());
    s.request.options.wire.temperature = Some(FiniteF64::new(0.0).unwrap());
    s.request.options.wire.cache_retention = Some(CacheRetention::Long);
    s.request.options.wire.metadata =
        Some(serde_json::from_value(f["options"]["metadata"].clone()).unwrap());
    let options = serde_json::from_value(
        json!({"thinkingEnabled":true,"effort":"xhigh","thinkingDisplay":"summarized"}),
    )
    .unwrap();
    let (result, events) = complete(s, options);
    assert_eq!(
        result,
        serde_json::from_value::<AssistantMessage>(f["result"].clone()).unwrap()
    );
    assert_eq!(
        serde_json::from_value::<Vec<AssistantMessageEvent>>(json!(events)).unwrap(),
        serde_json::from_value::<Vec<AssistantMessageEvent>>(f["events"].clone()).unwrap()
    );
    assert_eq!(
        hook.payload.lock().unwrap().as_ref().unwrap(),
        &f["logical"]
    );
    let requests = observed.requests.lock().unwrap();
    assert_eq!(requests[0].0, f["request"]["url"]);
    assert_eq!(requests[0].2, f["request"]["body"]);
    let headers: BTreeMap<_, _> = requests[0]
        .1
        .iter_for_wire()
        .map(|(k, v)| (k.to_ascii_lowercase(), v.to_owned()))
        .collect();
    assert_eq!(json!(headers), f["request"]["headers"]);
    assert_eq!(
        &observed.order.lock().unwrap()[..4],
        &["payload", "send", "response", "body"]
    );
}

#[test]
fn lf_every_byte_split_and_eof_flush_match_pinned_parser() {
    let f = fixture();
    let bytes = sse(f["chunks"].as_array().unwrap());
    let expected: AssistantMessage = serde_json::from_value(f["result"].clone()).unwrap();
    for split in 0..=bytes.len() {
        let (result, _) = complete(
            setup(
                200,
                vec![bytes[..split].to_vec(), bytes[split..].to_vec()],
                false,
            ),
            AnthropicOptions::default(),
        );
        assert_eq!(result, expected, "split {split}");
    }
    let mut bytes = sse(&ending());
    while bytes.last() == Some(&b'\n') {
        bytes.pop();
    }
    assert_eq!(
        complete(setup(200, vec![bytes], false), AnthropicOptions::default())
            .0
            .stop_reason,
        StopReason::Stop
    );
}

#[test]
fn caller_abort_retains_usage_only_fallback_and_signature_only_updates() {
    let f = fixture();
    let chunks = f["chunks"].as_array().unwrap();
    for end in [1, 6] {
        let s = setup(200, vec![sse(&chunks[..end])], true);
        let token = s.request.options.runtime.cancellation.clone();
        let observed = s.observed.clone();
        let stream = s.api.stream(s.request);
        let mut task = s.spawner.take();
        assert!(poll(task.as_mut()).is_pending());
        token.cancel();
        let result = ready(stream.result());
        assert_eq!(result.stop_reason, StopReason::Aborted);
        assert_eq!(result.model.as_str(), "claude-fable-5");
        assert_eq!(result.response_id.unwrap().as_str(), "msg_fixture");
        assert_eq!(result.usage.input.get(), 12);
        assert_eq!(result.usage.cache_write_1h.unwrap().get(), 4);
        assert_eq!(result.usage.cost.cache_write.get(), 0.000084);
        if end == 6 {
            assert_eq!(json!(result.content)[1]["thinkingSignature"], "sig-tail");
        } else {
            assert!(result.content.is_empty());
        }
        ready(task);
        assert_eq!(observed.closes.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn preserves_approved_missing_model_normalization_but_rejects_present_invalid_models() {
    let base = json!({"type":"message_start","message":{"id":"id","usage":{"input_tokens":12,"output_tokens":0}}});
    let mut events = vec![base.clone()];
    events.extend(ending());
    let result = complete(
        setup(200, vec![sse(&events)], false),
        AnthropicOptions::default(),
    )
    .0;
    assert_eq!(result.model.as_str(), "claude-opus-4-6");
    assert_eq!(result.stop_reason, StopReason::Stop);
    for invalid in [Value::Null, json!(""), json!(1), json!({})] {
        let mut start = base.clone();
        start["message"]["model"] = invalid;
        let mut events = vec![start];
        events.extend(ending());
        assert_eq!(
            complete(
                setup(200, vec![sse(&events)], false),
                AnthropicOptions::default()
            )
            .0
            .stop_reason,
            StopReason::Error
        );
    }
}

#[test]
fn repairs_malformed_sse_json_and_partial_tool_json() {
    let mut bytes = sse(&[
        json!({"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"call","name":"read","input":{}}}),
    ]);
    bytes.extend_from_slice(br#"event: content_block_delta
data: {"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"path\":\"A\H\",\"text\":\"col1\tcol2\"}"}}

"#);
    bytes.extend(sse(&[
        json!({"type":"content_block_stop","index":0}),
        json!({"type":"message_delta","delta":{"stop_reason":"tool_use"}}),
        json!({"type":"message_stop"}),
    ]));
    let result = complete(setup(200, vec![bytes], false), AnthropicOptions::default()).0;
    assert_eq!(result.stop_reason, StopReason::ToolUse);
    assert_eq!(
        json!(result.content)[0]["arguments"],
        json!({"path":"A\\H","text":"col1\tcol2"})
    );
}

#[test]
fn named_filter_unknown_stop_refusal_and_missing_stop_are_source_specific() {
    let mut bytes = sse(&ending());
    bytes.extend(b"event: proxy.stats\ndata: invalid-json\n\n");
    assert_eq!(
        complete(setup(200, vec![bytes], false), AnthropicOptions::default())
            .0
            .stop_reason,
        StopReason::Stop
    );
    let mut bytes = sse(&ending());
    bytes.extend(b"event: error\ndata: prompt is too long\n\n");
    let result = complete(setup(200, vec![bytes], false), AnthropicOptions::default()).0;
    assert!(is_context_overflow(&result, None));
    for reason in ["refusal", "sensitive", "unknown"] {
        let bytes = sse(&[
            json!({"type":"message_delta","delta":{"stop_reason":reason,"stop_details":{"explanation":"server error"}}}),
            json!({"type":"message_stop"}),
        ]);
        let result = complete(setup(200, vec![bytes], false), AnthropicOptions::default()).0;
        assert_eq!(result.stop_reason, StopReason::Error);
        assert_eq!(result.raw_stop_reason.unwrap().as_str(), reason);
    }
    let start = fixture()["chunks"][0].clone();
    let mut events = vec![start];
    events.push(ending()[0].clone());
    let result = complete(
        setup(200, vec![sse(&events)], false),
        AnthropicOptions::default(),
    )
    .0;
    assert_eq!(result.stop_reason, StopReason::Error);
    assert!(is_retryable_assistant_error(&result));
    assert_eq!(result.usage.input.get(), 12);
}

#[test]
fn only_acquisition_times_out_not_successful_sse_body() {
    let s = setup(200, vec![sse(&ending())], true);
    let clock = s.clock.clone();
    let token = s.request.options.runtime.cancellation.clone();
    let stream = s.api.stream(s.request);
    let mut task = s.spawner.take();
    assert!(poll(task.as_mut()).is_pending());
    assert_eq!(clock.waits.lock().unwrap()[0], Duration::from_secs(600));
    clock.expired.store(true, Ordering::SeqCst);
    assert!(
        poll(task.as_mut()).is_pending(),
        "SDK timer must be gone after headers"
    );
    token.cancel();
    ready(task);
    assert_eq!(ready(stream.result()).stop_reason, StopReason::Aborted);
}

#[test]
fn middleware_mutation_replacement_failure_and_cancellation() {
    for replacement in [
        None,
        Some(json!({"model":"replacement","stream":false})),
        Some(Value::Null),
    ] {
        let mut s = setup(200, vec![sse(&ending())], false);
        let observed = s.observed.clone();
        s.api.services.payload = Some(Arc::new(Hooks {
            observed: observed.clone(),
            payload: Mutex::new(None),
            replace: replacement.clone(),
            mutate: true,
            fail: false,
            block: false,
            response_only: false,
        }));
        assert_eq!(
            complete(s, AnthropicOptions::default()).0.stop_reason,
            StopReason::Stop
        );
        let requests = observed.requests.lock().unwrap();
        assert_eq!(requests[0].2["stream"], true);
        assert_eq!(
            requests[0].2.get("model").and_then(Value::as_str),
            if replacement == Some(Value::Null) {
                None
            } else if replacement.is_some() {
                Some("replacement")
            } else {
                Some("mutated")
            }
        );
    }
    for response_only in [false, true] {
        for block in [false, true] {
            let mut s = setup(200, vec![sse(&ending())], true);
            let hook = Arc::new(Hooks {
                observed: s.observed.clone(),
                payload: Mutex::new(None),
                replace: None,
                mutate: false,
                fail: !block,
                block,
                response_only,
            });
            s.api.services.payload = Some(hook.clone());
            s.api.services.response = Some(hook);
            let token = s.request.options.runtime.cancellation.clone();
            let stream = s.api.stream(s.request);
            let mut task = s.spawner.take();
            if block {
                assert!(poll(task.as_mut()).is_pending());
                token.cancel();
            }
            ready(task);
            assert_eq!(
                ready(stream.result()).stop_reason,
                if block {
                    StopReason::Aborted
                } else {
                    StopReason::Error
                }
            );
        }
    }
}

#[test]
fn sdk_body_classifier_matrix_and_retry_cap_preserve_negative_vetoes() {
    let vectors: Value = serde_json::from_str(include_str!(
        "../../../conformance/provider-traces/anthropic-messages/errors.json"
    ))
    .unwrap();
    for case in vectors["cases"].as_array().unwrap() {
        for cap in [false, true] {
            let status = case["status"].as_u64().unwrap() as u16;
            let mut s = setup(
                status,
                vec![case["body"].as_str().unwrap().as_bytes().to_vec()],
                false,
            );
            if cap {
                s.request.options.wire.max_retries = Some(SafeUnsigned::new(1).unwrap());
                s.request.options.wire.max_retry_delay_ms = Some(SafeUnsigned::new(1).unwrap());
                let mut responses = s.client.responses.lock().unwrap();
                responses[0].headers.set("x-should-retry", "true").unwrap();
                responses[0].headers.set("retry-after-ms", "2000").unwrap();
            }
            let result = complete(s, AnthropicOptions::default()).0;
            assert_eq!(result.stop_reason, StopReason::Error);
            assert_eq!(
                is_context_overflow(&result, None),
                case[if cap { "capOverflow" } else { "overflow" }]
                    .as_bool()
                    .unwrap(),
                "{case} cap={cap}"
            );
            assert_eq!(
                is_retryable_assistant_error(&result),
                case[if cap { "capRetryable" } else { "retryable" }]
                    .as_bool()
                    .unwrap(),
                "{case} cap={cap}"
            );
            let diagnostic = json!(result.diagnostics.as_ref().unwrap());
            assert_eq!(diagnostic[0]["details"]["status"], status);
            assert!(!result.error_message.unwrap().contains(&status.to_string()));
        }
    }
}

#[test]
fn complete_failed_body_precedes_retry_and_only_success_head_is_observed() {
    let mut s = setup(503, vec![], false);
    let hook = hooks(&mut s);
    let gate = Arc::new(AtomicBool::new(false));
    {
        let mut responses = s.client.responses.lock().unwrap();
        responses[0].body = Box::new(Body {
            chunks: VecDeque::from([b"{\"message\":\"server error\"}".to_vec()]),
            block: false,
            gate: Some(gate.clone()),
            observed: s.observed.clone(),
        });
        responses.push_back(response(
            200,
            vec![sse(&ending())],
            false,
            s.observed.clone(),
        ));
    }
    s.clock.backoff.store(true, Ordering::SeqCst);
    s.request.options.wire.max_retries = Some(SafeUnsigned::new(1).unwrap());
    let observed = s.observed.clone();
    let stream = s.api.stream(s.request);
    let mut task = s.spawner.take();
    assert!(poll(task.as_mut()).is_pending());
    assert_eq!(observed.requests.lock().unwrap().len(), 1);
    assert!(!observed.order.lock().unwrap().contains(&"response"));
    gate.store(true, Ordering::SeqCst);
    ready(task);
    assert_eq!(ready(stream.result()).stop_reason, StopReason::Stop);
    assert_eq!(observed.requests.lock().unwrap().len(), 2);
    assert_eq!(
        observed
            .order
            .lock()
            .unwrap()
            .iter()
            .filter(|value| **value == "response")
            .count(),
        1
    );
    assert!(hook.payload.lock().unwrap().is_some());
    for (_, headers, _) in observed.requests.lock().unwrap().iter() {
        assert_eq!(headers.get_for_wire("x-stainless-retry-count"), Some("0"));
    }
}

struct SuppliedClient {
    observed: Arc<Observed>,
    calls: AtomicUsize,
    block_body: bool,
    retry_once: bool,
}
impl AnthropicClient for SuppliedClient {
    fn create<'a>(
        &'a self,
        request: &'a TextStreamRequest,
        payload: &'a JsonValue,
        options: AnthropicClientOptions<'a>,
    ) -> BoxFuture<'a, Result<HttpResponse, ProviderAttemptFailure<AnthropicResponseError>>> {
        Box::pin(async move {
            assert_eq!(options.max_retries(), 0);
            assert!(!options.cancellation.is_cancelled());
            let value = json!(payload);
            assert_eq!(value["stream"], true);
            assert_eq!(value["tools"][0]["name"], "read");
            assert_eq!(value["system"][0]["text"], "Be precise");
            assert!(request.auth.api_key.is_none());
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 && self.retry_once {
                let template: AssistantMessage =
                    serde_json::from_value(fixture()["result"].clone()).unwrap();
                return Err(ProviderAttemptFailure::Provider {
                    status: Some(503),
                    headers: Some(HttpHeaders::new()),
                    error: AnthropicResponseError::from_message(
                        Some(503),
                        "503 server error",
                        &template,
                    ),
                });
            }
            Ok(response(
                200,
                vec![sse(&ending())],
                self.block_body,
                self.observed.clone(),
            ))
        })
    }
}
#[test]
fn supplied_client_bypasses_internal_auth_and_keeps_oauth_shaping_off() {
    let mut s = setup(200, vec![], false);
    s.request.auth.api_key = None;
    s.request.resolved.endpoint = RequestEndpoint::new_resolved("not-a-url").unwrap();
    let client = Arc::new(SuppliedClient {
        observed: s.observed.clone(),
        calls: AtomicUsize::new(0),
        block_body: false,
        retry_once: false,
    });
    s.api.client = Some(client.clone());
    let observed = s.observed.clone();
    assert_eq!(
        complete(s, AnthropicOptions::default()).0.stop_reason,
        StopReason::Stop
    );
    assert_eq!(client.calls.load(Ordering::SeqCst), 1);
    assert!(observed.requests.lock().unwrap().is_empty());
}

#[test]
fn supplied_client_noncooperative_body_is_cancelled_and_closed() {
    let mut s = setup(200, vec![], false);
    s.request.auth.api_key = None;
    s.api.client = Some(Arc::new(SuppliedClient {
        observed: s.observed.clone(),
        calls: AtomicUsize::new(0),
        block_body: true,
        retry_once: false,
    }));
    let token = s.request.options.runtime.cancellation.clone();
    let observed = s.observed.clone();
    let stream = s.api.stream(s.request);
    let mut worker = s.spawner.take();
    assert!(poll(worker.as_mut()).is_pending());
    token.cancel();
    assert_eq!(ready(stream.result()).stop_reason, StopReason::Aborted);
    assert!(
        poll(worker.as_mut()).is_ready(),
        "supplied client body must not retain the worker after cancellation"
    );
    assert_eq!(observed.closes.load(Ordering::SeqCst), 1);
}

#[test]
fn supplied_client_uses_only_outer_retry_budget_and_explicit_inner_zero() {
    let mut s = setup(200, vec![], false);
    s.request.auth.api_key = None;
    s.request.options.wire.max_retries = Some(SafeUnsigned::new(1).unwrap());
    s.clock.backoff.store(true, Ordering::SeqCst);
    let client = Arc::new(SuppliedClient {
        observed: s.observed.clone(),
        calls: AtomicUsize::new(0),
        block_body: false,
        retry_once: true,
    });
    s.api.client = Some(client.clone());
    let observed = s.observed.clone();
    let clock = s.clock.clone();
    let result = complete(s, AnthropicOptions::default()).0;
    assert_eq!(result.stop_reason, StopReason::Stop);
    assert_eq!(client.calls.load(Ordering::SeqCst), 2);
    assert!(observed.requests.lock().unwrap().is_empty());
    assert_eq!(
        *clock.waits.lock().unwrap(),
        vec![Duration::from_millis(500)]
    );
}

#[test]
fn raw_integral_decimal_exponents_and_initial_vs_delta_null_semantics() {
    for number in ["0.0", "-0", "2.0", "2e0", "20e-1"] {
        let raw = format!(
            "event: message_start\ndata: {{\"type\":\"message_start\",\"message\":{{\"id\":\"count\",\"model\":\"claude-opus-4-6\",\"usage\":{{\"input_tokens\":12e0,\"output_tokens\":{number},\"cache_creation\":{{\"ephemeral_1h_input_tokens\":0.0}}}}}}}}\n\nevent: content_block_start\ndata: {{\"type\":\"content_block_start\",\"index\":0.0,\"content_block\":{{\"type\":\"text\",\"text\":\"x\"}}}}\n\nevent: content_block_stop\ndata: {{\"type\":\"content_block_stop\",\"index\":0e0}}\n\nevent: message_delta\ndata: {{\"type\":\"message_delta\",\"delta\":{{\"stop_reason\":\"end_turn\"}},\"usage\":{{\"input_tokens\":null,\"output_tokens\":null,\"output_tokens_details\":{{\"thinking_tokens\":{number}}}}}}}\n\nevent: message_stop\ndata: {{\"type\":\"message_stop\"}}\n\n"
        );
        let result = complete(
            setup(200, vec![raw.into_bytes()], false),
            AnthropicOptions::default(),
        )
        .0;
        assert_eq!(result.stop_reason, StopReason::Stop, "{number}");
        assert_eq!(result.usage.input.get(), 12);
        assert_eq!(result.usage.output, result.usage.reasoning.unwrap());
    }
    for value in [-1.0, 0.5, 9007199254740992.0] {
        let events = [
            json!({"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":value}}),
            json!({"type":"message_stop"}),
        ];
        assert_eq!(
            complete(
                setup(200, vec![sse(&events)], false),
                AnthropicOptions::default()
            )
            .0
            .stop_reason,
            StopReason::Error
        );
    }
}

#[test]
fn sdk_request_matrix_auth_betas_cache_thinking_metadata_and_url() {
    let vectors: Value = serde_json::from_str(include_str!(
        "../../../conformance/provider-traces/anthropic-messages/requests.json"
    ))
    .unwrap();
    for case in vectors["cases"].as_array().unwrap() {
        let mut s = setup(200, vec![], false);
        let hook = hooks(&mut s);
        let observed = s.observed.clone();
        // Query-bearing source endpoints enter through transient resolved custody,
        // never through the durable model's structurally public base URL.
        let mut public_model = case["model"].clone();
        public_model["baseUrl"] = json!("https://public.invalid");
        s.request.resolved.model = serde_json::from_value(public_model).unwrap();
        s.request.resolved.endpoint =
            RequestEndpoint::new_resolved(case["model"]["baseUrl"].as_str().unwrap()).unwrap();
        s.request.context = serde_json::from_value(case["context"].clone()).unwrap();
        s.request.auth.api_key = case["options"]
            .get("apiKey")
            .and_then(Value::as_str)
            .map(SecretString::new);
        if let Some(headers) = case["options"].get("headers") {
            s.request.auth.headers = serde_json::from_value(headers.clone()).unwrap();
        }
        if let Some(env) = case["options"].get("env") {
            s.request.auth.env = serde_json::from_value(env.clone()).unwrap();
        }
        let mut raw = case["options"].clone();
        let mut family = serde_json::Map::new();
        for field in [
            "thinkingEnabled",
            "thinkingBudgetTokens",
            "effort",
            "thinkingDisplay",
            "interleavedThinking",
            "toolChoice",
        ] {
            if let Some(value) = raw.as_object_mut().unwrap().remove(field) {
                family.insert(field.into(), value);
            }
        }
        for field in ["apiKey", "env", "headers"] {
            raw.as_object_mut().unwrap().remove(field);
        }
        s.request.options.wire = serde_json::from_value(raw).unwrap();
        let options = serde_json::from_value(Value::Object(family)).unwrap();
        let (result, _) = complete(s, options);
        assert_eq!(result.stop_reason, StopReason::Error, "{}", case["id"]);
        // Middleware's Rust numeric storage variants are not separate JS Number
        // values. Project spelling only; strings/array order/object values stay exact.
        let js_number_values = |value: &Value| {
            serde_json::from_str::<Value>(
                &pi_ai_providers::api::wire_json::stringify_wire_json(value).unwrap(),
            )
            .unwrap()
        };
        assert_eq!(
            hook.payload.lock().unwrap().as_ref().map(js_number_values),
            case.get("logical").map(js_number_values),
            "{} logical",
            case["id"]
        );
        let requests = observed.requests.lock().unwrap();
        if let Some(expected) = case.get("request") {
            assert_eq!(requests.len(), 1, "{}", case["id"]);
            assert_eq!(requests[0].0, expected["url"], "{} URL", case["id"]);
            assert_eq!(requests[0].2, expected["body"], "{} body", case["id"]);
            let headers: BTreeMap<_, _> = requests[0]
                .1
                .iter_for_wire()
                .map(|(key, value)| (key.to_ascii_lowercase(), value.to_owned()))
                .collect();
            assert_eq!(
                json!(headers),
                expected["headers"],
                "{} headers",
                case["id"]
            );
        } else {
            assert!(requests.is_empty(), "{}", case["id"]);
        }
    }
}

#[test]
fn replay_signatures_and_deferred_reference_results_keep_sibling_order_and_cache() {
    let mut s = setup(200, vec![sse(&ending())], false);
    let observed = s.observed.clone();
    let mut assistant = fixture()["result"].clone();
    assistant["model"] = json!("claude-opus-4-6");
    assistant["content"] = json!([
        {"type":"thinking","thinking":"","thinkingSignature":"signed"},
        {"type":"thinking","thinking":"[Reasoning redacted]","thinkingSignature":"opaque","redacted":true},
        {"type":"toolCall","id":"call_1","name":"base","arguments":{}},
        {"type":"toolCall","id":"call_2","name":"base","arguments":{}}]);
    let tools = json!([
        {"name":"base","description":"Base","parameters":{"type":"object","properties":{}}},
        {"name":"late","description":"Late","parameters":{"type":"object","properties":{}}}]);
    s.request.context=serde_json::from_value(json!({"messages":[
        {"role":"user","content":"start","timestamp":7},assistant,
        {"role":"toolResult","toolCallId":"call_1","toolName":"base","content":[{"type":"text","text":"loaded"},{"type":"image","mimeType":"image/png","data":"eA=="}],"addedToolNames":["late","late"],"isError":false,"timestamp":7},
        {"role":"toolResult","toolCallId":"call_2","toolName":"base","content":[{"type":"text","text":"second"}],"isError":true,"timestamp":7}],"tools":tools})).unwrap();
    assert_eq!(
        complete(s, AnthropicOptions::default()).0.stop_reason,
        StopReason::Stop
    );
    let requests = observed.requests.lock().unwrap();
    let wire = &requests[0].2;
    assert_eq!(
        wire["messages"][1]["content"][0],
        json!({"type":"thinking","thinking":"","signature":"signed"})
    );
    assert_eq!(
        wire["messages"][1]["content"][1],
        json!({"type":"redacted_thinking","data":"opaque"})
    );
    assert_eq!(
        wire["messages"][2]["content"],
        json!([
        {"type":"tool_result","tool_use_id":"call_1","content":[{"type":"tool_reference","tool_name":"late"}],"is_error":false},
        {"type":"tool_result","tool_use_id":"call_2","content":"second","is_error":true},
        {"type":"text","text":"loaded"},
        {"type":"image","source":{"type":"base64","media_type":"image/png","data":"eA=="},"cache_control":{"type":"ephemeral"}}])
    );
    assert_eq!(
        wire["tools"][0]["cache_control"],
        json!({"type":"ephemeral"})
    );
    assert!(wire["tools"][0].get("defer_loading").is_none());
    assert_eq!(wire["tools"][1]["defer_loading"], true);
    assert!(wire["tools"][1].get("cache_control").is_none());
}

#[test]
fn empty_signature_compat_and_all_deferred_fallback_match_source() {
    for allow in [false, true] {
        let mut s = setup(200, vec![sse(&ending())], false);
        let observed = s.observed.clone();
        s.request.resolved.model.compat =
            Some(serde_json::from_value(json!({"allowEmptySignature":allow})).unwrap());
        let mut assistant = fixture()["result"].clone();
        assistant["model"] = json!("claude-opus-4-6");
        assistant["content"] = json!([
            {"type":"thinking","thinking":"reason","thinkingSignature":" "},{"type":"toolCall","id":"call","name":"base","arguments":{}}]);
        s.request.context=serde_json::from_value(json!({"messages":[assistant,{"role":"toolResult","toolCallId":"call","toolName":"base","content":[{"type":"text","text":"loaded"}],"addedToolNames":["late"],"isError":false,"timestamp":7}],
            "tools":[{"name":"late","description":"Late","parameters":{"type":"object","properties":{}}}]})).unwrap();
        complete(s, AnthropicOptions::default());
        let requests = observed.requests.lock().unwrap();
        let wire = &requests[0].2;
        assert_eq!(
            wire["messages"][0]["content"][0],
            if allow {
                json!({"type":"thinking","thinking":"reason","signature":""})
            } else {
                json!({"type":"text","text":"reason"})
            }
        );
        assert!(wire["tools"][0].get("defer_loading").is_none());
        assert_eq!(wire["messages"][1]["content"][0]["content"], "loaded");
    }
}

#[test]
fn simple_reasoning_budget_adaptive_custom_mapping_and_raw_option_admission() {
    for adaptive in [false, true] {
        let mut s = setup(200, vec![sse(&ending())], false);
        let observed = s.observed.clone();
        s.request.resolved.model.compat = Some(
            serde_json::from_value(
                json!({"forceAdaptiveThinking":adaptive,"supportsStrictTools":true}),
            )
            .unwrap(),
        );
        s.request.resolved.model.thinking_level_map = Some(ThinkingLevelMap::new(BTreeMap::from(
            [(ModelThinkingLevel::Medium, Some("custom-effort".into()))],
        )));
        let stream = s.api.stream_simple(SimpleStreamRequest {
            resolved: s.request.resolved,
            context: s.request.context,
            auth: s.request.auth,
            options: SimpleStreamOptions {
                wire: SimpleStreamOptionsWire {
                    reasoning: Some(ThinkingLevel::Medium),
                    ..Default::default()
                },
                runtime: s.request.options.runtime,
            },
        });
        ready(s.spawner.take());
        assert_eq!(ready(stream.result()).stop_reason, StopReason::Stop);
        let requests = observed.requests.lock().unwrap();
        let wire = &requests[0].2;
        if adaptive {
            assert_eq!(
                wire["thinking"],
                json!({"type":"adaptive","display":"summarized"})
            );
            assert_eq!(wire["output_config"]["effort"], "custom-effort");
        } else {
            assert_eq!(
                wire["thinking"],
                json!({"type":"enabled","display":"summarized","budget_tokens":8192})
            );
            assert!(wire.get("output_config").is_none());
        }
    }
    for invalid in [
        json!({"thinkingEnabled":null}),
        json!({"thinkingDisplay":"other"}),
        json!({"effort":"other"}),
        json!({"client":{}}),
        json!({"toolChoice":{"type":"tool","name":"read","extra":true}}),
    ] {
        assert!(serde_json::from_value::<AnthropicOptions>(invalid).is_err());
    }
}

#[test]
fn oauth_tool_names_round_trip_without_invented_find_mapping() {
    for name in ["read", "todowrite", "find"] {
        let mut s = setup(
            200,
            vec![sse(&[
                json!({"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"call","name":match name{"read"=>"Read","todowrite"=>"TodoWrite",_=>name},"input":{}}}),
                json!({"type":"content_block_stop","index":0}),
                json!({"type":"message_delta","delta":{"stop_reason":"tool_use"}}),
                json!({"type":"message_stop"}),
            ])],
            false,
        );
        s.request.auth.api_key = Some(SecretString::new("sk-ant-oat-fixture"));
        s.request.context.tools=Some(serde_json::from_value(json!([{"name":name,"description":"tool","parameters":{"type":"object","properties":{}}}])).unwrap());
        let result = complete(s, AnthropicOptions::default()).0;
        assert_eq!(result.stop_reason, StopReason::ToolUse);
        assert_eq!(json!(result.content)[0]["name"], name);
    }
}

#[test]
fn initial_tool_input_finalization_and_duplicate_indices_use_first_active_slot() {
    let events = [
        json!({"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"call","name":"read","input":{"initial":true}}}),
        json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":"second"}}),
        json!({"type":"content_block_stop","index":0}),
        json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":" slot"}}),
        json!({"type":"content_block_stop","index":0}),
        json!({"type":"message_delta","delta":{"stop_reason":"tool_use"}}),
        json!({"type":"message_stop"}),
    ];
    let (result, events) = complete(
        setup(200, vec![sse(&events)], false),
        AnthropicOptions::default(),
    );
    assert_eq!(result.stop_reason, StopReason::ToolUse);
    assert_eq!(
        events[1]["partial"]["content"][0]["arguments"],
        json!({"initial":true})
    );
    assert_eq!(json!(result.content)[0]["arguments"], json!({}));
    assert_eq!(json!(result.content)[1]["text"], "second slot");
}

#[test]
fn sdk_wraps_fetch_rejections_as_connection_errors_but_keeps_timeouts_distinct() {
    for kind in [
        TransportErrorKind::Connection,
        TransportErrorKind::Protocol,
        TransportErrorKind::Timeout,
    ] {
        let s = setup(200, vec![], false);
        *s.client.fail.lock().unwrap() = Some(kind);
        let result = complete(s, AnthropicOptions::default()).0;
        assert_eq!(result.stop_reason, StopReason::Error);
        assert!(is_retryable_assistant_error(&result), "{kind:?}");
    }
}

#[test]
fn limits_setup_spawn_mismatch_and_error_body_cap_fail_closed() {
    let mut s = setup(
        200,
        vec![sse(fixture()["chunks"].as_array().unwrap())],
        false,
    );
    s.api.limits.max_content_blocks = 0;
    let result = complete(s, AnthropicOptions::default()).0;
    assert_eq!(result.stop_reason, StopReason::Error);
    assert_eq!(result.usage.input.get(), 12);
    let mut s = setup(503, vec![b"sensitive body".to_vec()], false);
    s.api.services.max_error_body_bytes = 1;
    let observed = s.observed.clone();
    let result = complete(s, AnthropicOptions::default()).0;
    assert_eq!(result.stop_reason, StopReason::Error);
    assert_eq!(observed.closes.load(Ordering::SeqCst), 1);
    assert!(!json!(result).to_string().contains("sensitive body"));
    let mut s = setup(200, vec![], false);
    s.request.resolved.model.api = "pi-messages".parse().unwrap();
    let observed = s.observed.clone();
    assert_eq!(
        complete(s, AnthropicOptions::default()).0.stop_reason,
        StopReason::Error
    );
    assert!(observed.requests.lock().unwrap().is_empty());
    let mut s = setup(200, vec![], false);
    s.request.options.runtime.spawner = Arc::new(Spawner {
        reject: true,
        ..Default::default()
    });
    assert_eq!(
        complete(s, AnthropicOptions::default()).0.stop_reason,
        StopReason::Error
    );
    let mut s = setup(200, vec![], false);
    s.request.auth.api_key = None;
    assert_eq!(
        complete(s, AnthropicOptions::default()).0.stop_reason,
        StopReason::Error
    );
}

#[test]
fn private_parser_cr_push_boundary_and_named_type_filter_are_not_sdk_semantics() {
    let raw=b"event: message_delta\r\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"}}\r\n\r\n";
    assert_eq!(
        complete(
            setup(200, vec![raw.to_vec()], false),
            AnthropicOptions::default()
        )
        .0
        .stop_reason,
        StopReason::Stop
    );
    let split = raw.iter().position(|byte| *byte == b'\r').unwrap() + 1;
    assert_eq!(
        complete(
            setup(
                200,
                vec![raw[..split].to_vec(), raw[split..].to_vec()],
                false
            ),
            AnthropicOptions::default()
        )
        .0
        .stop_reason,
        StopReason::Error
    );
    for raw in [
        "event: message_delta\ndata: {\"delta\":{\"stop_reason\":\"end_turn\"}}\n\n",
        "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"}}\n\n",
        "event: ignored\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"}}\n\n",
    ] {
        assert_eq!(
            complete(
                setup(200, vec![raw.as_bytes().to_vec()], false),
                AnthropicOptions::default()
            )
            .0
            .stop_reason,
            StopReason::Error
        );
    }
    // The allowlist admits this SSE name, but dispatch and start tracking use
    // the JSON type verbatim; there is no name/type coercion or equality check.
    let raw=b"event: message_start\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"}}";
    assert_eq!(
        complete(
            setup(200, vec![raw.to_vec()], false),
            AnthropicOptions::default()
        )
        .0
        .stop_reason,
        StopReason::Stop
    );
}

#[test]
fn no_body_is_detected_after_success_response_observation_and_start() {
    struct Head(Arc<AtomicUsize>);
    impl ResponseMiddleware for Head {
        fn on_response<'a>(
            &'a self,
            _: &'a TextStreamRequest,
            head: &'a AdapterResponseHead,
        ) -> BoxFuture<'a, Result<(), ModelsError>> {
            Box::pin(async move {
                assert_eq!(head.status, 204);
                self.0.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        }
    }
    let mut s = setup(204, vec![], false);
    let count = Arc::new(AtomicUsize::new(0));
    s.api.services.response = Some(Arc::new(Head(count.clone())));
    let observed = s.observed.clone();
    let (result, events) = complete(s, AnthropicOptions::default());
    assert_eq!(result.stop_reason, StopReason::Error);
    assert_eq!(count.load(Ordering::SeqCst), 1);
    assert_eq!(events[0]["type"], "start");
    assert_eq!(events[1]["type"], "error");
    assert_eq!(observed.reads.load(Ordering::SeqCst), 0);
    assert_eq!(observed.closes.load(Ordering::SeqCst), 1);
}

#[test]
fn callback_header_record_projection_matches_executed_helper_and_preserves_raw() {
    struct Record(Arc<Mutex<Vec<(String, String)>>>);
    impl ResponseMiddleware for Record {
        fn on_response<'a>(
            &'a self,
            _: &'a TextStreamRequest,
            head: &'a AdapterResponseHead,
        ) -> BoxFuture<'a, Result<(), ModelsError>> {
            Box::pin(async move {
                *self.0.lock().unwrap() = head
                    .headers
                    .iter_for_wire()
                    .map(|(name, value)| (name.into(), value.into()))
                    .collect();
                Ok(())
            })
        }
    }
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../conformance/provider-traces/anthropic-messages/headers.json"
    ))
    .unwrap();
    let raw_entries: Vec<(String, String)> =
        serde_json::from_value(fixture["raw"].clone()).unwrap();
    let expected: Vec<(String, String)> =
        serde_json::from_value(fixture["ownRecordEntries"].clone()).unwrap();
    let mut raw = HttpHeaders::new();
    for (name, value) in &raw_entries {
        raw.append(name.clone(), value.clone()).unwrap();
    }
    let before = raw.clone();
    let head = AdapterResponseHead::from_http(200, &raw).unwrap();
    assert_eq!(head.status, 200);
    assert_eq!(
        head.headers
            .iter_for_wire()
            .map(|(k, v)| (k.into(), v.into()))
            .collect::<Vec<(String, String)>>(),
        expected
    );
    assert_eq!(raw, before);
    let mut s = setup(200, vec![sse(&ending())], false);
    s.client.responses.lock().unwrap()[0].headers = raw;
    let captured = Arc::new(Mutex::new(vec![]));
    s.api.services.response = Some(Arc::new(Record(captured.clone())));
    assert_eq!(
        complete(s, AnthropicOptions::default()).0.stop_reason,
        StopReason::Stop
    );
    assert_eq!(*captured.lock().unwrap(), expected);
}

#[test]
fn independent_malformed_raw_lines_classification() {
    let probes: Value = serde_json::from_str(include_str!(
        "../../../conformance/provider-traces/anthropic-messages/parse-review.json"
    ))
    .unwrap();
    let mut mismatches = vec![];
    for case in probes["cases"].as_array().unwrap() {
        let mut s = setup(
            200,
            vec![case["raw"].as_str().unwrap().as_bytes().to_vec()],
            false,
        );
        s.request.options.wire.max_retries = Some(SafeUnsigned::new(1).unwrap());
        let observed = s.observed.clone();
        let result = complete(s, AnthropicOptions::default()).0;
        let actual = json!({"retryable":is_retryable_assistant_error(&result),"overflow":is_context_overflow(&result,None)});
        println!(
            "{} source retry={} overflow={} Rust={actual}",
            case["id"], case["retryable"], case["overflow"]
        );
        assert_eq!(observed.requests.lock().unwrap().len(), 1);
        if actual["retryable"] != case["retryable"] || actual["overflow"] != case["overflow"] {
            mismatches.push(case["id"].clone());
        }
    }
    assert!(
        mismatches.is_empty(),
        "source/Rust differences: {mismatches:?}"
    );
}

#[test]
fn independent_complete_sdk_attempt_caps() {
    let probes: Value = serde_json::from_str(include_str!(
        "../../../conformance/provider-traces/anthropic-messages/parse-review.json"
    ))
    .unwrap();
    for case in probes["cap"].as_array().unwrap() {
        let mut s = setup(
            case["status"].as_u64().unwrap() as u16,
            vec![case["body"].as_str().unwrap().as_bytes().to_vec()],
            false,
        );
        if case["capped"] == true {
            s.request.options.wire.max_retries = Some(SafeUnsigned::new(1).unwrap());
            s.request.options.wire.max_retry_delay_ms = Some(SafeUnsigned::new(1).unwrap());
            let mut responses = s.client.responses.lock().unwrap();
            responses[0].headers.set("x-should-retry", "true").unwrap();
            responses[0].headers.set("retry-after-ms", "2000").unwrap();
        }
        let observed = s.observed.clone();
        let result = complete(s, AnthropicOptions::default()).0;
        assert_eq!(
            is_retryable_assistant_error(&result),
            case["retryable"].as_bool().unwrap(),
            "{case}"
        );
        assert_eq!(
            is_context_overflow(&result, None),
            case["overflow"].as_bool().unwrap(),
            "{case}"
        );
        assert_eq!(observed.requests.lock().unwrap().len(), 1);
        assert_eq!(observed.closes.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn independent_failed_body_cancel_prevents_second_attempt() {
    let mut s = setup(503, vec![b"billing server error".to_vec()], true);
    hooks(&mut s);
    s.request.options.wire.max_retries = Some(SafeUnsigned::new(2).unwrap());
    s.clock.backoff.store(true, Ordering::SeqCst);
    let token = s.request.options.runtime.cancellation.clone();
    let observed = s.observed.clone();
    let stream = s.api.stream(s.request);
    let mut task = s.spawner.take();
    assert!(poll(task.as_mut()).is_pending());
    assert_eq!(observed.requests.lock().unwrap().len(), 1);
    assert!(!observed.order.lock().unwrap().contains(&"response"));
    token.cancel();
    ready(task);
    assert_eq!(ready(stream.result()).stop_reason, StopReason::Aborted);
    assert_eq!(observed.requests.lock().unwrap().len(), 1);
    assert_eq!(observed.closes.load(Ordering::SeqCst), 1);
}

#[test]
fn source_parse_error_raw_context_controls_and_partial_snapshots() {
    let vectors: Value = serde_json::from_str(include_str!(
        "../../../conformance/provider-traces/anthropic-messages/parse-errors.json"
    ))
    .unwrap();
    for case in vectors["cases"].as_array().unwrap() {
        let chunks: Vec<Vec<u8>> = case["chunks"]
            .as_array()
            .unwrap()
            .iter()
            .map(|chunk| chunk.as_str().unwrap().as_bytes().to_vec())
            .collect();
        let mut setup = setup(200, chunks, false);
        setup.request.options.wire.max_retries = Some(SafeUnsigned::new(1).unwrap());
        let observed = setup.observed.clone();
        let (result, events) = complete(setup, AnthropicOptions::default());
        assert_eq!(json!(result.stop_reason), case["stop"], "{}", case["id"]);
        assert_eq!(
            is_retryable_assistant_error(&result),
            case["retryable"].as_bool().unwrap(),
            "{}",
            case["id"]
        );
        assert_eq!(
            is_context_overflow(&result, None),
            case["overflow"].as_bool().unwrap(),
            "{}",
            case["id"]
        );
        assert_eq!(json!(result.content), case["content"], "{}", case["id"]);
        assert_eq!(
            result.usage,
            serde_json::from_value::<Usage>(case["usage"].clone()).unwrap(),
            "{}",
            case["id"]
        );
        assert_eq!(json!(result.model), case["model"]);
        assert_eq!(json!(result.response_id), case["responseId"]);
        let kinds: Vec<_> = events.iter().map(|event| event["type"].clone()).collect();
        assert_eq!(json!(kinds), case["eventTypes"], "{}", case["id"]);
        assert_eq!(
            observed.requests.lock().unwrap().len() as u64,
            case["calls"].as_u64().unwrap()
        );
        assert_eq!(observed.closes.load(Ordering::SeqCst), 1);
        let public =
            json!({"error":result.error_message,"diagnostics":result.diagnostics}).to_string();
        for forbidden in [
            "Could not parse",
            "data=",
            "raw=",
            "ignored:",
            "billing",
            "Unexpected token",
            "\\\\n",
        ] {
            assert!(
                !public.contains(forbidden),
                "{} leaked {forbidden}",
                case["id"]
            );
        }
    }
}

#[test]
fn raw_context_cancellation_and_resource_failures_remain_closed() {
    let mut chunks = vec![sse(&fixture()["chunks"].as_array().unwrap()[..3])];
    chunks.push(b": private-marker prompt is too long\n".to_vec());
    let cancelled_setup = setup(200, chunks, true);
    let token = cancelled_setup.request.options.runtime.cancellation.clone();
    let observed = cancelled_setup.observed.clone();
    let stream = cancelled_setup.api.stream(cancelled_setup.request);
    let mut worker = cancelled_setup.spawner.take();
    assert!(poll(worker.as_mut()).is_pending());
    token.cancel();
    ready(worker);
    let result = ready(stream.result());
    assert_eq!(result.stop_reason, StopReason::Aborted);
    assert_eq!(result.usage.input.get(), 12);
    assert_eq!(result.model.as_str(), "claude-fable-5");
    assert!(!json!(result).to_string().contains("private-marker"));
    assert_eq!(observed.closes.load(Ordering::SeqCst), 1);

    // A carrier/frame limit is not a provider JSON parse failure. Unparsed
    // hostile comments must not be classified after the framer has rejected.
    for frame_limit in [false, true] {
        let mut setup = setup(200, vec![b"event: message_delta\n: private-marker prompt is too long server error\ndata: invalid\n\n".to_vec()], false);
        if frame_limit {
            setup.api.services.sse_limits.max_frame_bytes = 20;
        } else {
            setup.api.limits.max_stream_bytes = 20;
        }
        let observed = setup.observed.clone();
        let result = complete(setup, AnthropicOptions::default()).0;
        assert_eq!(result.stop_reason, StopReason::Error);
        assert!(!is_context_overflow(&result, None));
        assert!(!is_retryable_assistant_error(&result));
        assert!(!json!(result).to_string().contains("private-marker"));
        assert_eq!(observed.closes.load(Ordering::SeqCst), 1);
    }
}

#[cfg(feature = "native")]
mod native {
    use super::*;
    use std::{
        io::{Read, Write},
        net::TcpListener,
        thread,
    };
    #[test]
    fn actual_native_http_fragmented_sse_eof_and_caller_cancellation_release() {
        for cancel in [false, true] {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let f = fixture();
            let events = f["chunks"].as_array().unwrap();
            let bytes = if cancel {
                sse(&events[..3])
            } else {
                sse(events)
            };
            let server = thread::spawn(move || {
                let (mut socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                socket
                    .set_write_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut head = vec![];
                while !head.ends_with(b"\r\n\r\n") {
                    let mut byte = [0];
                    socket.read_exact(&mut byte).unwrap();
                    head.push(byte[0]);
                    assert!(head.len() < 16384);
                }
                assert!(head.starts_with(b"POST /prefix/v1/messages HTTP/1.1\r\n"));
                assert!(
                    head.windows(b"x-byte: \xe9\r\n".len())
                        .any(|w| w == b"x-byte: \xe9\r\n")
                );
                let head: String = head.into_iter().map(char::from).collect();
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
                let body: Value = serde_json::from_slice(&body).unwrap();
                assert_eq!(body["model"], "claude-opus-4-6");
                assert_eq!(body["stream"], true);
                socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n").unwrap();
                for chunk in bytes.chunks(3) {
                    write!(socket, "{:x}\r\n", chunk.len()).unwrap();
                    socket.write_all(chunk).unwrap();
                    socket.write_all(b"\r\n").unwrap();
                    socket.flush().unwrap();
                }
                if !cancel {
                    socket.write_all(b"0\r\n\r\n").unwrap();
                    return;
                }
                let mut byte = [0];
                match socket.read(&mut byte) {
                    Ok(0) => {}
                    Err(error)
                        if matches!(
                            error.kind(),
                            std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
                        ) => {}
                    other => panic!("must release cancelled connection: {other:?}"),
                }
            });
            let engine = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();
            let mut s = setup(200, vec![], false);
            s.request.resolved.endpoint =
                RequestEndpoint::new_resolved(format!("http://{address}/prefix")).unwrap();
            s.request.auth.headers =
                ProviderHeaders::new(BTreeMap::from([("x-byte".into(), Some("é".into()))]));
            s.request.options.runtime.clock = Arc::new(TokioClock::new(engine.handle().clone()));
            s.api.services.transport = Arc::new(HttpTransport::new(
                Arc::new(NativeHttpClient::new(engine.handle().clone())),
                Arc::new(NativeRetryJitter),
            ));
            let token = s.request.options.runtime.cancellation.clone();
            let mut stream = s.api.stream(s.request);
            let worker = s.spawner.take();
            engine.block_on(async {
                let worker = tokio::spawn(worker);
                if cancel {
                    tokio::time::timeout(Duration::from_secs(5), async {
                        while let Some(event) = stream.next().await {
                            if matches!(event, AssistantMessageEvent::TextDelta { .. }) {
                                return;
                            }
                        }
                        panic!("missing text delta")
                    })
                    .await
                    .unwrap();
                    token.cancel();
                }
                let result = tokio::time::timeout(Duration::from_secs(5), stream.result())
                    .await
                    .unwrap();
                assert_eq!(
                    result.stop_reason,
                    if cancel {
                        StopReason::Aborted
                    } else {
                        StopReason::ToolUse
                    }
                );
                assert_eq!(result.model.as_str(), "claude-fable-5");
                assert_eq!(result.usage.input.get(), 12);
                assert_eq!(json!(result.content)[0]["text"], "Hello world☃");
                worker.await.unwrap();
            });
            server.join().unwrap();
        }
    }
}
