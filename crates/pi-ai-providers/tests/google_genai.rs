#![cfg(feature = "wire-google-genai")]
mod retained_support;
use pi_ai_core::*;
use pi_ai_providers::api::wire_json::WirePayload;
use pi_ai_providers::api::{google_genai::*, runtime::*};
use pi_ai_transport::*;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, VecDeque},
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
    time::Duration,
};
fn corpus() -> Value {
    retained_support::fixture(include_str!(
        "../../../conformance/provider-traces/google-genai/trace.json"
    ))
}
fn hex(value: &str) -> Vec<u8> {
    value
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|v| u8::from_str_radix(std::str::from_utf8(v).unwrap(), 16).unwrap())
        .collect()
}
#[derive(Default)]
struct ClockPort {
    sleeps: Mutex<Vec<u64>>,
}
impl Clock for ClockPort {
    fn now_unix_ms(&self) -> u64 {
        7
    }
    fn sleep<'a>(
        &'a self,
        d: Duration,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), Cancelled>> {
        Box::pin(async move {
            self.sleeps.lock().unwrap().push(d.as_millis() as u64);
            Ok(())
        })
    }
}
#[derive(Default)]
struct Spawner(Mutex<Vec<BoxFuture<'static, ()>>>);
impl TaskSpawner for Spawner {
    fn spawn(&self, f: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
        self.0.lock().unwrap().push(f);
        Ok(())
    }
}
impl Spawner {
    fn take(&self) -> BoxFuture<'static, ()> {
        self.0.lock().unwrap().pop().unwrap()
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
    reads: AtomicUsize,
    closes: AtomicUsize,
    drops: AtomicUsize,
    logical: Mutex<Option<Value>>,
}
struct Body {
    chunks: VecDeque<Vec<u8>>,
    blocked: bool,
    closed: bool,
    observed: Arc<Observed>,
}
impl ResponseBody for Body {
    fn next<'a>(
        &'a mut self,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, TransportError>> {
        Box::pin(async move {
            self.observed.reads.fetch_add(1, Ordering::SeqCst);
            if self.chunks.is_empty() && self.blocked {
                std::future::pending().await
            } else {
                Ok(self.chunks.pop_front())
            }
        })
    }
    fn close(&mut self) {
        if !self.closed {
            self.closed = true;
            self.observed.closes.fetch_add(1, Ordering::SeqCst);
        }
    }
}
impl Drop for Body {
    fn drop(&mut self) {
        self.close();
        self.observed.drops.fetch_add(1, Ordering::SeqCst);
    }
}
struct Client {
    responses: Mutex<VecDeque<HttpResponse>>,
    observed: Arc<Observed>,
}
impl RawHttpClient for Client {
    fn send<'a>(
        &'a self,
        r: &'a HttpRequest,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<HttpResponse, TransportError>> {
        Box::pin(async move {
            self.observed.order.lock().unwrap().push("send");
            let headers: BTreeMap<_, _> = r
                .headers
                .iter_for_wire()
                .map(|(k, _)| (k, r.headers.get_for_fetch(k).unwrap()))
                .collect();
            self.observed.requests.lock().unwrap().push(json!({"url":r.endpoint.expose_for_transport(),"method":r.method,"headers":headers,"body":serde_json::from_slice::<Value>(&r.body).unwrap()}));
            Ok(self
                .responses
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected request"))
        })
    }
}
struct Hook {
    observed: Arc<Observed>,
    replacement: Option<Value>,
    block: bool,
    fail: bool,
}
impl PayloadMiddleware for Hook {
    fn on_payload<'a>(
        &'a self,
        _: &'a TextStreamRequest,
        p: &'a mut WirePayload,
    ) -> BoxFuture<'a, Result<Option<WirePayload>, ModelsError>> {
        Box::pin(async move {
            self.observed.order.lock().unwrap().push("payload");
            *self.observed.logical.lock().unwrap() = Some(json!(p.as_value()));
            if self.block {
                return std::future::pending().await;
            }
            if self.fail {
                return Err(ModelsError::new(ModelsErrorCode::Provider));
            }
            Ok(self.replacement.clone().map(WirePayload::new))
        })
    }
}
impl ResponseMiddleware for Hook {
    fn on_response<'a>(
        &'a self,
        _: &'a TextStreamRequest,
        _: &'a AdapterResponseHead,
    ) -> BoxFuture<'a, Result<(), ModelsError>> {
        panic!("Google source never invokes onResponse")
    }
}
struct Setup {
    api: GoogleGenAiApi,
    request: TextStreamRequest,
    spawner: Arc<Spawner>,
    observed: Arc<Observed>,
    clock: Arc<ClockPort>,
}
fn setup(case: &Value, blocked: bool) -> Setup {
    let observed = Arc::new(Observed::default());
    let spawner = Arc::new(Spawner::default());
    let clock = Arc::new(ClockPort::default());
    let mut headers = HttpHeaders::new();
    headers
        .append(
            "content-type",
            case["contentType"].as_str().unwrap_or("text/event-stream"),
        )
        .unwrap();
    if let Some(extra) = case["responseHeaders"].as_object() {
        for (k, v) in extra {
            headers.append(k, v.as_str().unwrap()).unwrap();
        }
    }
    let chunks: Vec<Vec<u8>> = if let Some(values) = case["byteChunks"].as_array() {
        values.iter().map(|v| hex(v.as_str().unwrap())).collect()
    } else {
        case["chunks"].as_array().map(|a|a.iter().map(|v|v.as_str().unwrap().as_bytes().to_vec()).collect()).unwrap_or_else(||vec![b"data: {\"candidates\":[{\"finishReason\":\"STOP\",\"content\":{\"parts\":[{\"text\":\"done\"}]}}]}\n\n".to_vec()])
    };
    let responses = (0..case["requests"].as_array().map_or(1, |a| a.len()))
        .map(|_| HttpResponse {
            status: case["status"].as_u64().unwrap_or(200) as u16,
            status_text: case["statusHex"]
                .as_str()
                .map(|s| hex(s).into_iter().map(char::from).collect())
                .unwrap_or_default(),
            headers: headers.clone(),
            body: Box::new(Body {
                chunks: chunks.clone().into(),
                blocked,
                closed: false,
                observed: observed.clone(),
            }) as Box<dyn ResponseBody>,
        })
        .collect();
    // SDK/custom-fetch argument observation, with already constructed Response
    // fixtures. Native overrides below remain raw-network clients.
    let mut services = AdapterServices::new(Arc::new(
        HttpTransport::new(
            Arc::new(Client {
                responses: Mutex::new(responses),
                observed: observed.clone(),
            }),
            Arc::new(Jitter),
        )
        .with_fetch_response_source(FetchResponseSource::Preconstructed),
    ));
    services.env = serde_json::from_value(case.get("env").cloned().unwrap_or(json!({}))).unwrap();
    let hook = Arc::new(Hook {
        observed: observed.clone(),
        replacement: case.get("replacement").cloned(),
        block: false,
        fail: false,
    });
    services.payload = Some(hook.clone());
    services.response = Some(hook);
    let mut public = case["model"].clone();
    let endpoint = public["baseUrl"].as_str().unwrap().to_owned();
    public["baseUrl"] = json!("https://public.invalid");
    let request = TextStreamRequest {
        resolved: ResolvedRequest {
            model: serde_json::from_value(public).unwrap(),
            endpoint: RequestEndpoint::new_resolved(endpoint).unwrap(),
        },
        context: serde_json::from_value(case["context"].clone()).unwrap(),
        auth: ResolvedAuth {
            api_key: Some(SecretString::new(
                case["key"].as_str().unwrap_or("synthetic-key"),
            )),
            headers: serde_json::from_value(case.get("headers").cloned().unwrap_or(json!({})))
                .unwrap(),
            ..Default::default()
        },
        options: StreamOptions {
            wire: Default::default(),
            runtime: RequestRuntime::with_noop_telemetry(
                CancellationToken::new(),
                clock.clone(),
                spawner.clone(),
            ),
        },
    };
    Setup {
        api: GoogleGenAiApi::new(services).with_tool_call_counter(Arc::new(
            GoogleToolCallCounter::new(SafeUnsigned::new(0).unwrap()),
        )),
        request,
        spawner,
        observed,
        clock,
    }
}
fn poll<F: Future + ?Sized>(f: Pin<&mut F>) -> Poll<F::Output> {
    f.poll(&mut Context::from_waker(Waker::noop()))
}
fn ready<F: Future>(f: F) -> F::Output {
    let mut f = Box::pin(f);
    match poll(f.as_mut()) {
        Poll::Ready(v) => v,
        Poll::Pending => panic!("unexpected pending"),
    }
}
fn execute(mut s: Setup, case: &Value) -> (AssistantMessage, Vec<Value>) {
    let mut options = case["options"].clone();
    let mut family = json!({});
    for key in ["thinking", "toolChoice"] {
        if let Some(value) = options.as_object_mut().unwrap().remove(key) {
            family[key] = value;
        }
    }
    let mut stream = if case["simple"] == true {
        let mut options = case["options"].clone();
        if options.get("toolChoice") == Some(&json!("any")) {
            options.as_object_mut().unwrap().remove("toolChoice");
        }
        s.api.stream_simple(SimpleStreamRequest {
            resolved: s.request.resolved,
            context: s.request.context,
            auth: s.request.auth,
            options: SimpleStreamOptions {
                wire: serde_json::from_value(options).unwrap(),
                runtime: s.request.options.runtime,
            },
        })
    } else {
        s.request.options.wire = serde_json::from_value(options).unwrap();
        s.api
            .stream_with_options(s.request, serde_json::from_value(family).unwrap())
    };
    ready(s.spawner.take());
    let result = retained_support::message(ready(stream.result()));
    let mut events = Vec::new();
    while let Some(e) = ready(stream.next()) {
        events.push(retained_support::without_duration(json!(e)));
    }
    (result, events)
}
fn normalize_events(mut events: Value) -> Vec<AssistantMessageEvent> {
    for event in events.as_array_mut().unwrap() {
        if event["type"] == "error" {
            event["error"]["errorMessage"] = json!("safe error");
            event["error"]
                .as_object_mut()
                .unwrap()
                .remove("diagnostics");
        }
    }
    serde_json::from_value(events).unwrap()
}
fn js_numbers(v: &Value) -> Value {
    serde_json::from_str(&pi_ai_providers::api::wire_json::stringify_wire_json(v).unwrap()).unwrap()
}

#[test]
fn pinned_sdk_requests_and_complete_event_snapshots_match_all_cases() {
    let corpus = corpus();
    assert_eq!(corpus["cases"].as_array().unwrap().len(), 149);
    for case in corpus["cases"].as_array().unwrap() {
        let s = setup(case, false);
        let observed = s.observed.clone();
        let clock = s.clock.clone();
        let (result, events) = execute(s, case);
        assert_eq!(
            json!(*observed.requests.lock().unwrap()),
            case["requests"],
            "{} request",
            case["id"]
        );
        assert_eq!(
            observed.logical.lock().unwrap().as_ref().map(js_numbers),
            case.get("logical").filter(|v| !v.is_null()).map(js_numbers),
            "{} logical",
            case["id"]
        );
        assert_eq!(
            json!(*observed.order.lock().unwrap()),
            case["order"],
            "{} order",
            case["id"]
        );
        assert_eq!(
            json!(*clock.sleeps.lock().unwrap()),
            case["sleeps"],
            "{} sleeps",
            case["id"]
        );
        if case["synchronous"] == true {
            assert_eq!(result.stop_reason, StopReason::Error);
            continue;
        }
        assert_eq!(
            normalize_events(json!(events)),
            normalize_events(case["events"].clone()),
            "{} events",
            case["id"]
        );
        let expected = if case["parserNormalization"] == true {
            (false, false)
        } else {
            (
                case["sourceFlags"]["retryable"].as_bool().unwrap(),
                case["sourceFlags"]["overflow"].as_bool().unwrap(),
            )
        };
        assert_eq!(
            (
                is_retryable_assistant_error(&result),
                is_context_overflow(&result, None)
            ),
            expected,
            "{} classification",
            case["id"]
        );
        assert_eq!(
            observed.closes.load(Ordering::SeqCst),
            observed.requests.lock().unwrap().len(),
            "{} cleanup",
            case["id"]
        );
    }
}

#[test]
fn typed_raw_options_preserve_fractional_dynamic_budget_and_reject_null() {
    for value in [
        json!({}),
        json!({"thinking":{"enabled":false}}),
        json!({"toolChoice":"any","thinking":{"enabled":true,"budgetTokens":-1.5,"level":"LOW"}}),
    ] {
        let options: GoogleOptions = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(js_numbers(&json!(options)), value);
    }
    for value in [
        json!({"thinking":null}),
        json!({"thinking":{}}),
        json!({"thinking":{"enabled":true,"budgetTokens":null}}),
        json!({"thinking":{"enabled":true,"level":"other"}}),
        json!({"toolChoice":null}),
        json!({"fetch":{}}),
    ] {
        assert!(serde_json::from_value::<GoogleOptions>(value).is_err());
    }
}

#[test]
fn sdk_read_boundaries_utf8_and_metadata_errors_match_source() {
    let fixture: Value = retained_support::fixture(include_str!(
        "../../../conformance/provider-traces/google-genai/stream.json"
    ));
    for spec in fixture["cases"].as_array().unwrap() {
        let mut case = spec.clone();
        case["model"] = fixture["model"].clone();
        case["context"] = fixture["context"].clone();
        case["options"] = json!({});
        let expected = &fixture["observations"][spec["observation"].as_u64().unwrap() as usize];
        let s = setup(&case, false);
        let observed = s.observed.clone();
        let (result, events) = execute(s, &case);
        assert_eq!(
            normalize_events(json!(events)),
            normalize_events(expected["events"].clone()),
            "{} events",
            spec["id"]
        );
        assert_eq!(
            (
                is_retryable_assistant_error(&result),
                is_context_overflow(&result, None)
            ),
            (
                expected["retryable"].as_bool().unwrap(),
                expected["overflow"].as_bool().unwrap()
            ),
            "{} flags",
            spec["id"]
        );
        if let Some(reads) = spec["reads"].as_u64() {
            assert_eq!(
                observed.reads.load(Ordering::SeqCst) as u64,
                reads,
                "{} reads",
                spec["id"]
            );
        }
        assert_eq!(observed.closes.load(Ordering::SeqCst), 1);
        assert_eq!(observed.drops.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn metadata_template_operands_and_context_match_source() {
    let fixture: Value = retained_support::fixture(include_str!(
        "../../../conformance/provider-traces/google-genai/metadata.json"
    ));
    for spec in fixture["cases"].as_array().unwrap() {
        let mut case = spec.clone();
        case["model"] = fixture["model"].clone();
        case["context"] = fixture["context"].clone();
        case["options"] = json!({});
        let expected = &fixture["observations"][spec["observation"].as_u64().unwrap() as usize];
        let s = setup(&case, false);
        let observed = s.observed.clone();
        let (result, events) = execute(s, &case);
        assert_eq!(
            normalize_events(json!(events)),
            normalize_events(expected["events"].clone()),
            "{} events",
            spec["id"]
        );
        assert_eq!(
            (
                is_retryable_assistant_error(&result),
                is_context_overflow(&result, None)
            ),
            (
                expected["retryable"].as_bool().unwrap(),
                expected["overflow"].as_bool().unwrap()
            ),
            "{} flags",
            spec["id"]
        );
        if let Some(reads) = spec["reads"].as_u64() {
            assert_eq!(
                observed.reads.load(Ordering::SeqCst) as u64,
                reads,
                "{} reads",
                spec["id"]
            );
        }
        assert_eq!(observed.closes.load(Ordering::SeqCst), 1);
        assert_eq!(observed.drops.load(Ordering::SeqCst), 1);
    }
}

fn custom_case(chunks: Vec<String>) -> Value {
    let mut case = corpus()["cases"][1].clone();
    case["chunks"] = json!(chunks);
    case
}
#[test]
fn default_counter_is_shared_and_given_ids_never_consume_it() {
    let mut ids = Vec::new();
    for provided in [true, false, false] {
        let call = if provided {
            json!({"id":"given","name":"lookup","args":{}})
        } else {
            json!({"name":"lookup","args":{}})
        };
        let case = custom_case(vec![format!(
            "data: {}\n\n",
            json!({"candidates":[{"content":{"parts":[{"functionCall":call}]},"finishReason":"STOP"}]})
        )]);
        let mut s = setup(&case, false);
        s.api = GoogleGenAiApi::new(s.api.services.clone());
        let (result, _) = execute(s, &case);
        let AssistantContent::ToolCall(call) = &result.content[0] else {
            panic!("tool expected")
        };
        ids.push(call.id.as_str().to_owned());
    }
    assert_eq!(ids, ["given", "lookup_7_1", "lookup_7_2"]);
}
#[test]
fn injected_counter_exhaustion_is_nonwrapping_and_given_id_still_works() {
    let counter = Arc::new(GoogleToolCallCounter::new(
        SafeUnsigned::new(SafeUnsigned::MAX.get() - 1).unwrap(),
    ));
    for (provided, success) in [(false, true), (false, false), (true, true)] {
        let mut call = json!({"name":"lookup","args":{}});
        if provided {
            call["id"] = json!("given");
        }
        let case = custom_case(vec![format!(
            "data: {}\n\n",
            json!({"candidates":[{"content":{"parts":[{"functionCall":call}]},"finishReason":"STOP"}]})
        )]);
        let mut s = setup(&case, false);
        s.api = s.api.with_tool_call_counter(counter.clone());
        let (result, _) = execute(s, &case);
        assert_eq!(
            result.stop_reason,
            if success {
                StopReason::ToolUse
            } else {
                StopReason::Error
            }
        );
        if success && !provided {
            assert_eq!(
                json!(result.content)[0]["id"],
                format!("lookup_7_{}", SafeUnsigned::MAX.get())
            );
        }
    }
}
#[test]
fn usage_only_cancellation_snapshot_and_noncooperative_hooks_release() {
    let first = json!({"responseId":"retained","candidates":[{"content":{"parts":[{"text":"partial","thoughtSignature":"c2ln"}]}}]});
    let usage = json!({"responseId":"ignored","usageMetadata":{"promptTokenCount":20,"cachedContentTokenCount":3,"candidatesTokenCount":5,"thoughtsTokenCount":2,"totalTokenCount":27}});
    let case = custom_case(vec![format!("data: {first}\n\ndata: {usage}\n\n")]);
    let s = setup(&case, true);
    let observed = s.observed.clone();
    let token = s.request.options.runtime.cancellation.clone();
    let stream = s.api.stream(s.request);
    let mut worker = s.spawner.take();
    assert!(poll(worker.as_mut()).is_pending());
    token.cancel();
    let result = retained_support::message(ready(stream.result()));
    assert_eq!(result.stop_reason, StopReason::Aborted);
    assert_eq!(result.response_id.as_ref().unwrap().as_str(), "retained");
    assert_eq!(result.usage.input.get(), 17);
    assert_eq!(result.usage.output.get(), 7);
    assert_eq!(
        json!(result.content),
        json!([{"type":"text","text":"partial","textSignature":"c2ln"}])
    );
    ready(worker);
    assert_eq!(observed.closes.load(Ordering::SeqCst), 1);
    assert_eq!(observed.drops.load(Ordering::SeqCst), 1);
    let case = custom_case(vec![]);
    let mut s = setup(&case, true);
    s.api.services.payload = Some(Arc::new(Hook {
        observed: s.observed.clone(),
        replacement: None,
        block: true,
        fail: false,
    }));
    let token = s.request.options.runtime.cancellation.clone();
    let observed = s.observed.clone();
    let stream = s.api.stream(s.request);
    let mut worker = s.spawner.take();
    assert!(poll(worker.as_mut()).is_pending());
    token.cancel();
    ready(worker);
    assert_eq!(ready(stream.result()).stop_reason, StopReason::Aborted);
    assert!(observed.requests.lock().unwrap().is_empty());
}
#[test]
fn neutral_timeout_is_ignored_and_failed_body_finishes_before_retry() {
    let case = custom_case(vec![]);
    let mut s = setup(&case, true);
    s.request.options.wire.timeout_ms = Some(SafeUnsigned::new(1).unwrap());
    let observed = s.observed.clone();
    let clock = s.clock.clone();
    let token = s.request.options.runtime.cancellation.clone();
    let stream = s.api.stream(s.request);
    let mut worker = s.spawner.take();
    assert!(poll(worker.as_mut()).is_pending());
    assert!(clock.sleeps.lock().unwrap().is_empty());
    token.cancel();
    ready(worker);
    assert_eq!(ready(stream.result()).stop_reason, StopReason::Aborted);
    assert_eq!(observed.closes.load(Ordering::SeqCst), 1);
    let mut case = custom_case(vec!["{\"error\":{\"message\":\"server error\"}}".into()]);
    case["status"] = json!(503);
    case["contentType"] = json!("application/json");
    let mut s = setup(&case, true);
    s.request.options.wire.max_retries = Some(SafeUnsigned::new(2).unwrap());
    let token = s.request.options.runtime.cancellation.clone();
    let observed = s.observed.clone();
    let stream = s.api.stream(s.request);
    let mut worker = s.spawner.take();
    assert!(poll(worker.as_mut()).is_pending());
    assert_eq!(observed.requests.lock().unwrap().len(), 1);
    token.cancel();
    ready(worker);
    assert_eq!(ready(stream.result()).stop_reason, StopReason::Aborted);
    assert_eq!(observed.requests.lock().unwrap().len(), 1);
    assert_eq!(observed.closes.load(Ordering::SeqCst), 1);
}
#[test]
fn limits_fail_without_raw_error_text_and_release_owned_body() {
    let raw = format!(
        "data: {}\n\n",
        json!({"candidates":[{"content":{"parts":[{"text":"private-marker"}]},"finishReason":"STOP"}]})
    );
    for limit in ["frame", "buffer", "stream", "blocks"] {
        let case = custom_case(vec![raw.clone()]);
        let mut s = setup(&case, false);
        match limit {
            "frame" => s.api.services.sse_limits.max_frame_bytes = 2,
            "buffer" => s.api.services.sse_limits.max_buffer_bytes = 2,
            "stream" => s.api.limits.max_stream_bytes = 2,
            _ => s.api.limits.max_content_blocks = 0,
        };
        let observed = s.observed.clone();
        let (result, _) = execute(s, &case);
        assert_eq!(result.stop_reason, StopReason::Error);
        assert!(!json!(result).to_string().contains("private-marker"));
        assert_eq!(observed.closes.load(Ordering::SeqCst), 1);
    }
}

#[cfg(feature = "native")]
mod native {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    fn engine() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap()
    }
    async fn request(socket: &mut tokio::net::TcpStream) -> (String, Value) {
        let mut bytes = Vec::new();
        let mut byte = [0];
        while !bytes.ends_with(b"\r\n\r\n") {
            socket.read_exact(&mut byte).await.unwrap();
            bytes.push(byte[0]);
            assert!(bytes.len() < 16384);
        }
        let head = String::from_utf8(bytes).unwrap();
        let length = head
            .lines()
            .find_map(|line| {
                line.to_ascii_lowercase()
                    .strip_prefix("content-length:")
                    .map(|n| n.trim().parse::<usize>().unwrap())
            })
            .unwrap();
        let mut body = vec![0; length];
        socket.read_exact(&mut body).await.unwrap();
        (head, serde_json::from_slice(&body).unwrap())
    }
    fn native_setup(
        case: &Value,
        address: std::net::SocketAddr,
        engine: &tokio::runtime::Runtime,
    ) -> Setup {
        let mut s = setup(case, false);
        s.request.resolved.endpoint =
            RequestEndpoint::new_resolved(format!("http://{address}/v1beta")).unwrap();
        s.request.options.runtime.clock = Arc::new(TokioClock::new(engine.handle().clone()));
        s.api.services.transport = Arc::new(HttpTransport::new(
            Arc::new(NativeHttpClient::new(engine.handle().clone())),
            Arc::new(Jitter),
        ));
        s
    }
    #[test]
    fn native_sdk_wire_fragmented_stream_and_cancel_release() {
        let engine = engine();
        let corpus = corpus();
        for cancel in [false, true] {
            let case = &corpus["cases"][0];
            let data = if cancel {
                format!(
                    "data: {}\n\ndata: {}\n\n",
                    json!({"responseId":"partial-id","usageMetadata":{"promptTokenCount":20,"cachedContentTokenCount":3,"candidatesTokenCount":1,"thoughtsTokenCount":2,"totalTokenCount":23}}),
                    json!({"candidates":[{"content":{"parts":[{"text":"partial"}]}}]})
                )
            } else {
                case["chunks"][0].as_str().unwrap().to_owned()
            };
            let listener = engine
                .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
                .unwrap();
            let address = listener.local_addr().unwrap();
            let expected = case["requests"][0]["body"].clone();
            let server=engine.spawn(async move {let (mut socket,_)=listener.accept().await.unwrap();let (head,body)=request(&mut socket).await;assert!(head.starts_with("POST /v1beta/models/gemini-3-flash-preview:streamGenerateContent?alt=sse HTTP/1.1"));assert!(head.contains("x-goog-api-key: synthetic-key\r\n"));assert_eq!(body,expected);socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n").await.unwrap();for chunk in data.as_bytes().chunks(3){socket.write_all(format!("{:x}\r\n",chunk.len()).as_bytes()).await.unwrap();socket.write_all(chunk).await.unwrap();socket.write_all(b"\r\n").await.unwrap();}if !cancel{socket.write_all(b"0\r\n\r\n").await.unwrap();}let mut byte=[0];assert!(matches!(socket.read(&mut byte).await,Ok(0)|Err(_)),"peer release, not timeout");});
            let s = native_setup(case, address, &engine);
            let token = s.request.options.runtime.cancellation.clone();
            let mut stream = s.api.stream(s.request);
            let worker = s.spawner.take();
            engine.block_on(async {
                tokio::time::timeout(Duration::from_secs(5), async {
                    let worker = tokio::spawn(worker);
                    let mut events = Vec::new();
                    while let Some(event) = stream.next().await {
                        if cancel && matches!(event, AssistantMessageEvent::TextDelta { .. }) {
                            token.cancel();
                        }
                        events.push(retained_support::without_duration(json!(event)));
                    }
                    worker.await.unwrap();
                    let result = retained_support::message(stream.result().await);
                    if cancel {
                        assert_eq!(result.stop_reason, StopReason::Aborted);
                        assert_eq!(result.usage.input.get(), 17);
                        assert_eq!(result.response_id.unwrap().as_str(), "partial-id");
                        assert_eq!(
                            json!(result.content),
                            json!([{"type":"text","text":"partial"}])
                        );
                    } else {
                        for event in &mut events {
                            for key in ["partial", "message", "error"] {
                                if let Some(message) = event.get_mut(key) {
                                    message["timestamp"] = json!(7);
                                }
                            }
                        }
                        assert_eq!(
                            normalize_events(json!(events)),
                            normalize_events(case["events"].clone())
                        );
                    }
                    server.await.unwrap();
                })
                .await
                .unwrap();
            });
        }
    }
    #[test]
    fn native_response_metadata_errors_preserve_explicit_sdk_data_classification() {
        let engine = engine();
        for corpus in [
            include_str!("../../../conformance/provider-traces/google-genai/stream.json"),
            include_str!("../../../conformance/provider-traces/google-genai/metadata.json"),
        ] {
            let fixture: Value = serde_json::from_str(corpus).unwrap();
            for spec in fixture["cases"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|c| c.get("statusHex").is_some())
            {
                let phrase = hex(spec["statusHex"].as_str().unwrap());
                let data = hex(spec["byteChunks"][0].as_str().unwrap());
                let listener = engine
                    .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
                    .unwrap();
                let address = listener.local_addr().unwrap();
                let server = engine.spawn(async move {
                    let (mut socket, _) = listener.accept().await.unwrap();
                    request(&mut socket).await;
                    socket.write_all(b"HTTP/1.1 200 ").await.unwrap();
                    socket.write_all(&phrase).await.unwrap();
                    socket
                        .write_all(
                            format!(
                                "\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\n\r\n",
                                data.len()
                            )
                            .as_bytes(),
                        )
                        .await
                        .unwrap();
                    socket.write_all(&data).await.unwrap();
                    let mut byte = [0];
                    assert!(matches!(socket.read(&mut byte).await, Ok(0) | Err(_)));
                });
                let mut case = spec.clone();
                case["model"] = fixture["model"].clone();
                case["context"] = fixture["context"].clone();
                let s = native_setup(&case, address, &engine);
                let stream = s.api.stream(s.request);
                let worker = s.spawner.take();
                let expected =
                    &fixture["observations"][spec["observation"].as_u64().unwrap() as usize];
                engine.block_on(async {
                    tokio::time::timeout(Duration::from_secs(5), async {
                        let worker = tokio::spawn(worker);
                        let result = retained_support::message(stream.result().await);
                        worker.await.unwrap();
                        assert_eq!(
                            (
                                is_retryable_assistant_error(&result),
                                is_context_overflow(&result, None)
                            ),
                            (
                                expected["retryable"].as_bool().unwrap(),
                                expected["overflow"].as_bool().unwrap()
                            )
                        );
                        assert_eq!(json!(result.content), expected["result"]["content"]);
                        assert!(
                            !json!(result.diagnostics)
                                .to_string()
                                .contains("exception parsing")
                        );
                        server.await.unwrap();
                    })
                    .await
                    .unwrap();
                });
            }
        }
    }
    #[test]
    fn sdk_http_timeout_override_ends_body_without_original_caller_abort_or_retry() {
        let engine = engine();
        let case = custom_case(vec![]);
        let listener = engine
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .unwrap();
        let address = listener.local_addr().unwrap();
        let server=engine.spawn(async move{let (mut socket,_)=listener.accept().await.unwrap();let(head,_)=request(&mut socket).await;assert!(head.contains("x-server-timeout: 1\r\n"));let data=b"data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"partial\"}]}}]}\n\n";socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n").await.unwrap();socket.write_all(format!("{:x}\r\n",data.len()).as_bytes()).await.unwrap();socket.write_all(data).await.unwrap();socket.write_all(b"\r\n").await.unwrap();let mut byte=[0];assert!(matches!(socket.read(&mut byte).await,Ok(0)|Err(_)));assert!(tokio::time::timeout(Duration::from_millis(30),listener.accept()).await.is_err());});
        let mut s = native_setup(&case, address, &engine);
        let token = s.request.options.runtime.cancellation.clone();
        let mut replacement = case["logical"].clone();
        replacement["config"]["httpOptions"] = json!({"timeout":500});
        s.api.services.payload = Some(Arc::new(Hook {
            observed: s.observed.clone(),
            replacement: Some(replacement),
            block: false,
            fail: false,
        }));
        s.request.options.wire.max_retries = Some(SafeUnsigned::new(2).unwrap());
        let stream = s.api.stream(s.request);
        let worker = s.spawner.take();
        engine.block_on(async {
            tokio::time::timeout(Duration::from_secs(5), async {
                let worker = tokio::spawn(worker);
                let result = retained_support::message(stream.result().await);
                worker.await.unwrap();
                assert_eq!(result.stop_reason, StopReason::Error);
                assert!(!token.is_cancelled());
                assert!(!is_retryable_assistant_error(&result));
                assert_eq!(
                    json!(result.content),
                    json!([{"type":"text","text":"partial"}])
                );
                server.await.unwrap();
            })
            .await
            .unwrap();
        });
    }
}
