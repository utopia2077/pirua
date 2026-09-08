#![cfg(feature = "wire-mistral-conversations")]

use pi_ai_core::*;
use pi_ai_providers::api::wire_json::WirePayload;
use pi_ai_providers::api::{mistral_conversations::*, runtime::*};
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
        "../../../conformance/provider-traces/mistral-conversations/trace.json"
    ))
    .unwrap()
}
#[derive(Default)]
struct TestClock {
    expired: AtomicBool,
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
        Box::pin(std::future::poll_fn(|_| {
            if self.expired.load(Ordering::SeqCst) {
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
        let result = self.chunks.pop_front();
        Box::pin(async move { Ok(result) })
    }
    fn close(&mut self) {
        self.observed.closes.fetch_add(1, Ordering::SeqCst);
    }
}
struct Client {
    response: Mutex<Option<HttpResponse>>,
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
            self.observed.requests.lock().unwrap().push((
                r.endpoint.expose_for_transport().into(),
                r.headers.clone(),
                serde_json::from_slice(&r.body).unwrap(),
            ));
            Ok(self
                .response
                .lock()
                .unwrap()
                .take()
                .expect("Mistral never retries"))
        })
    }
}
struct NoJitter;
impl RetryJitter for NoJitter {
    fn sample(&self) -> f64 {
        panic!("no retries")
    }
}
struct Setup {
    api: MistralApi,
    spawner: Arc<Spawner>,
    observed: Arc<Observed>,
    clock: Arc<TestClock>,
    request: TextStreamRequest,
}
fn setup(status: u16, chunks: Vec<Vec<u8>>, block: bool) -> Setup {
    let observed = Arc::new(Observed::default());
    let spawner = Arc::new(Spawner::default());
    let clock = Arc::new(TestClock::default());
    let mut headers = HttpHeaders::new();
    headers.set("x-request-id", "request-1").unwrap();
    let client = Arc::new(Client {
        response: Mutex::new(Some(HttpResponse {
            status,
            status_text: "Forbidden".into(),
            headers,
            body: Box::new(Body {
                chunks: chunks.into(),
                block,
                observed: observed.clone(),
            }),
        })),
        observed: observed.clone(),
    });
    // The source fixture captures SDK custom-fetch arguments, not native defaults.
    let api = MistralApi::new(AdapterServices::new(Arc::new(
        HttpTransport::new(client, Arc::new(NoJitter))
            .with_fetch_response_source(FetchResponseSource::Preconstructed),
    )));
    let request = TextStreamRequest {
        resolved: ResolvedRequest {
            model: serde_json::from_value(fixture()["model"].clone()).unwrap(),
            endpoint: RequestEndpoint::new_resolved("https://api.mistral.ai").unwrap(),
        },
        context: serde_json::from_value(
            json!({"messages":[{"role":"user","content":"hello","timestamp":7}]}),
        )
        .unwrap(),
        auth: ResolvedAuth {
            api_key: Some(SecretString::new("test-key")),
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
fn terminal() -> Value {
    json!({"choices":[{"finish_reason":"stop","delta":{}}]})
}
fn sse(events: &[Value]) -> Vec<u8> {
    events
        .iter()
        .map(|v| format!("data: {v}\r\n\r\n"))
        .collect::<String>()
        .into_bytes()
}
fn complete(s: Setup, options: MistralOptions) -> (AssistantMessage, Vec<Value>) {
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

#[test]
fn pinned_full_event_snapshots_and_every_byte_split() {
    let f = fixture();
    let bytes = sse(f["chunks"].as_array().unwrap());
    let expected: Vec<AssistantMessageEvent> = serde_json::from_value(f["events"].clone()).unwrap();
    for split in 0..=bytes.len() {
        let (result, events) = complete(
            setup(
                200,
                vec![bytes[..split].to_vec(), bytes[split..].to_vec()],
                false,
            ),
            MistralOptions::default(),
        );
        let actual: Vec<AssistantMessageEvent> = serde_json::from_value(json!(events)).unwrap();
        assert_eq!(actual, expected, "split {split}");
        assert_eq!(result.stop_reason, StopReason::ToolUse);
    }
}

#[test]
fn terminal_reason_does_not_end_stream_but_done_stops_hostile_tail() {
    let mut bytes = sse(&[
        terminal(),
        json!({"choices":[{"delta":{"content":"héllo 🌍"}}]}),
    ]);
    bytes.extend_from_slice(b"data: [DONE]\n\ndata: {hostile\n\n");
    let (result, _) = complete(
        setup(200, bytes.chunks(1).map(<[u8]>::to_vec).collect(), false),
        MistralOptions::default(),
    );
    assert_eq!(result.stop_reason, StopReason::Stop);
    assert_eq!(
        json!(result.content),
        json!([{"type":"text","text":"héllo 🌍"}])
    );
}

#[test]
fn eof_trailing_event_and_missing_or_malformed_terminal() {
    let bytes = format!("data: {}", terminal()).into_bytes();
    assert_eq!(
        complete(setup(200, vec![bytes], false), MistralOptions::default())
            .0
            .stop_reason,
        StopReason::Stop
    );
    for raw in [
        "",
        "data: []\n\n",
        "data: {\n\n",
        "data: {}\n\n",
        "data: {\"choices\":[{\"finish_reason\":\"stop\",\"delta\":{\"content\":[null]}}]}\n\n",
        "data: {\"choices\":[]}\n\n",
    ] {
        assert_eq!(
            complete(
                setup(200, vec![raw.as_bytes().to_vec()], false),
                MistralOptions::default()
            )
            .0
            .stop_reason,
            StopReason::Error
        );
    }
}

struct Hooks {
    observed: Arc<Observed>,
    payload: Mutex<Option<Value>>,
    fail: bool,
    block_payload: bool,
    block_response: bool,
}
impl PayloadMiddleware for Hooks {
    fn on_payload<'a>(
        &'a self,
        _: &'a TextStreamRequest,
        p: &'a mut WirePayload,
    ) -> BoxFuture<'a, Result<Option<WirePayload>, ModelsError>> {
        Box::pin(async move {
            self.observed.order.lock().unwrap().push("payload");
            *self.payload.lock().unwrap() = Some(json!(p.as_value()));
            if self.block_payload {
                return pending().await;
            }
            if self.fail {
                return Err(ModelsError::new(ModelsErrorCode::Stream));
            }
            let mut p = json!(p.as_value());
            p["topP"] = json!(0.9);
            p["responseFormat"] = json!({"type":"json_schema","jsonSchema":{"name":"result","schemaDefinition":{"type":"object","properties":{"maxTokens":{"type":"number"}}}}});
            Ok(Some(WirePayload::new(p)))
        })
    }
}
impl ResponseMiddleware for Hooks {
    fn on_response<'a>(
        &'a self,
        _: &'a TextStreamRequest,
        h: &'a AdapterResponseHead,
    ) -> BoxFuture<'a, Result<(), ModelsError>> {
        Box::pin(async move {
            self.observed.order.lock().unwrap().push("response");
            assert_eq!(h.headers.get_str("x-request-id"), Some("request-1"));
            if self.block_response {
                return pending().await;
            }
            Ok(())
        })
    }
}
fn hooks(s: &mut Setup) -> Arc<Hooks> {
    let hooks = Arc::new(Hooks {
        observed: s.observed.clone(),
        payload: Mutex::new(None),
        fail: false,
        block_payload: false,
        block_response: false,
    });
    s.api.services.payload = Some(hooks.clone());
    s.api.services.response = Some(hooks.clone());
    hooks
}

#[test]
fn logical_payload_precedes_finite_wire_remap_headers_and_response_body() {
    let mut s = setup(200, vec![sse(&[terminal()])], false);
    let hooks = hooks(&mut s);
    let observed = s.observed.clone();
    s.request.options.wire.max_tokens = Some(SafeUnsigned::new(123).unwrap());
    s.request.options.wire.session_id = Some("session-1".into());
    s.request.options.wire.sampling_params =
        Some(serde_json::from_value(json!({"randomSeed":999,"maxTokens":888})).unwrap());
    s.request.context=serde_json::from_value(json!({"systemPrompt":"Be precise","messages":[{"role":"user","timestamp":7,"content":[{"type":"text","text":"describe"},{"type":"image","mimeType":"image/png","data":"aGVsbG8="}]}]})).unwrap();
    let options:MistralOptions=serde_json::from_value(json!({"promptMode":"reasoning","reasoningEffort":"high","toolChoice":{"type":"function","function":{"name":"lookup"}}})).unwrap();
    assert_eq!(complete(s, options).0.stop_reason, StopReason::Stop);
    let observed = observed.requests.lock().unwrap();
    let (url, headers, wire) = &observed[0];
    assert_eq!(url, "https://api.mistral.ai/v1/chat/completions");
    assert_eq!(headers.get_for_wire("user-agent"), Some("pi-ai-rust"));
    assert_eq!(headers.get_for_wire("x-affinity"), Some("session-1"));
    assert_eq!(
        headers.get_for_wire("authorization"),
        Some("Bearer test-key")
    );
    assert_eq!(wire["max_tokens"], 123);
    assert_eq!(wire["prompt_mode"], "reasoning");
    assert_eq!(wire["top_p"], 0.9);
    assert!(wire.get("random_seed").is_none());
    assert_eq!(
        wire["response_format"]["json_schema"]["schema"]["properties"]["maxTokens"],
        json!({"type":"number"})
    );
    assert_eq!(
        wire["messages"][1]["content"][1],
        json!({"type":"image_url","image_url":"data:image/png;base64,aGVsbG8="})
    );
    let payload = hooks.payload.lock().unwrap();
    let payload = payload.as_ref().unwrap();
    assert_eq!(payload["maxTokens"], 123);
    assert!(payload.get("max_tokens").is_none());
    assert_eq!(
        &hooks.observed.order.lock().unwrap()[..4],
        &["payload", "send", "response", "body"]
    );
}

#[test]
fn null_affinity_suppression_header_trim_and_base_directory() {
    let mut s = setup(200, vec![sse(&[terminal()])], false);
    let observed = s.observed.clone();
    s.request.resolved.endpoint =
        RequestEndpoint::new_resolved("https://gateway.invalid/prefix///?discard=1#fragment")
            .unwrap();
    s.request.options.wire.session_id = Some("auto".into());
    s.request.auth.headers = ProviderHeaders::new(BTreeMap::from([
        ("Authorization".into(), None),
        ("X-Affinity".into(), None),
        ("User-Agent".into(), Some(" \tcustom\r\n".into())),
    ]));
    assert_eq!(
        complete(s, MistralOptions::default()).0.stop_reason,
        StopReason::Stop
    );
    let requests = observed.requests.lock().unwrap();
    assert_eq!(
        requests[0].0,
        "https://gateway.invalid/prefix/v1/chat/completions"
    );
    assert_eq!(requests[0].1.get_for_wire("authorization"), None);
    assert_eq!(requests[0].1.get_for_wire("x-affinity"), None);
    assert_eq!(requests[0].1.get_for_wire("user-agent"), Some("custom"));
}

#[test]
fn cancellation_retains_content_usage_and_response_identity_without_cooperative_body() {
    let chunks = [
        json!({"id":"retained","choices":[{"delta":{"content":"partial"}}]}),
        json!({"choices":[],"usage":{"prompt_tokens":10,"completion_tokens":4}}),
    ];
    let s = setup(200, vec![sse(&chunks)], true);
    let token = s.request.options.runtime.cancellation.clone();
    let observed = s.observed.clone();
    let stream = s.api.stream(s.request);
    let mut task = s.spawner.take();
    assert!(poll(task.as_mut()).is_pending());
    token.cancel();
    let result = ready(stream.result());
    assert_eq!(result.stop_reason, StopReason::Aborted);
    assert_eq!(result.usage.input.get(), 10);
    assert_eq!(result.response_id.unwrap().as_str(), "retained");
    assert_eq!(
        json!(result.content),
        json!([{"type":"text","text":"partial"}])
    );
    ready(task);
    assert_eq!(observed.closes.load(Ordering::SeqCst), 1);
}

#[test]
fn whole_response_timeout_is_error_with_partial_and_default_sixty_seconds() {
    let s = setup(
        200,
        vec![sse(&[json!({"choices":[{"delta":{"content":"partial"}}]})])],
        true,
    );
    let clock = s.clock.clone();
    let stream = s.api.stream(s.request);
    let mut task = s.spawner.take();
    assert!(poll(task.as_mut()).is_pending());
    assert_eq!(*clock.waits.lock().unwrap(), vec![Duration::from_secs(60)]);
    clock.expired.store(true, Ordering::SeqCst);
    ready(task);
    let result = ready(stream.result());
    assert_eq!(result.stop_reason, StopReason::Error);
    assert!(result.error_message.unwrap().contains("timeout"));
    assert_eq!(result.content.len(), 1);
}

#[test]
fn caller_cancellation_releases_noncooperative_hooks_and_failures_are_sanitized() {
    for response in [false, true] {
        let mut s = setup(200, vec![sse(&[terminal()])], true);
        let hook = Arc::new(Hooks {
            observed: s.observed.clone(),
            payload: Mutex::new(None),
            fail: false,
            block_payload: !response,
            block_response: response,
        });
        s.api.services.payload = Some(hook.clone());
        s.api.services.response = Some(hook);
        let token = s.request.options.runtime.cancellation.clone();
        let stream = s.api.stream(s.request);
        let mut task = s.spawner.take();
        assert!(poll(task.as_mut()).is_pending());
        token.cancel();
        ready(task);
        assert_eq!(ready(stream.result()).stop_reason, StopReason::Aborted);
    }
    let mut s = setup(200, vec![], false);
    s.api.services.payload = Some(Arc::new(Hooks {
        observed: s.observed.clone(),
        payload: Mutex::new(None),
        fail: true,
        block_payload: false,
        block_response: false,
    }));
    assert_eq!(
        complete(s, MistralOptions::default())
            .0
            .error_message
            .as_deref(),
        Some("Mistral middleware failed")
    );
}

#[test]
fn remote_numeric_spelling_alias_nullish_precedence_and_clamping() {
    for spelling in ["0.0", "-0", "3.0", "3e0", "30e-1"] {
        let raw = format!(
            "data: {{\"choices\":[{{\"finish_reason\":\"stop\",\"delta\":{{}}}}],\"usage\":{{\"prompt_tokens\":10.0,\"completion_tokens\":{spelling},\"total_tokens\":0e0,\"promptTokensDetails\":{{\"cachedTokens\":null}},\"prompt_tokens_details\":{{\"cached_tokens\":3e0}}}}}}\n\n"
        );
        let result = complete(
            setup(200, vec![raw.into_bytes()], false),
            MistralOptions::default(),
        )
        .0;
        assert_eq!(result.stop_reason, StopReason::Stop, "{spelling}");
        assert_eq!(result.usage.input.get(), 7);
        assert_eq!(result.usage.cache_read.get(), 3);
    }
    for alias in [
        "promptTokensDetails",
        "prompt_tokens_details",
        "promptTokenDetails",
        "prompt_token_details",
        "numCachedTokens",
        "num_cached_tokens",
    ] {
        let mut event = terminal();
        event["usage"] = json!({"prompt_tokens":10,"completion_tokens":4});
        event["usage"][alias] = match alias {
            "numCachedTokens" | "num_cached_tokens" => json!(20),
            "promptTokensDetails" | "promptTokenDetails" => json!({"cachedTokens":20}),
            _ => json!({"cached_tokens":20}),
        };
        let result = complete(
            setup(200, vec![sse(&[event])], false),
            MistralOptions::default(),
        )
        .0;
        assert_eq!(result.usage.cache_read.get(), 10);
        assert_eq!(result.usage.input.get(), 0);
    }
    for value in [-1.0, 1.5, 9007199254740992.0] {
        let mut event = terminal();
        event["usage"] = json!({"completion_tokens":value});
        assert_eq!(
            complete(
                setup(200, vec![sse(&[event])], false),
                MistralOptions::default()
            )
            .0
            .stop_reason,
            StopReason::Error
        );
    }
}

#[test]
fn raw_stop_reason_classification_and_http_redaction() {
    for reason in [
        "stop",
        "length",
        "model_length",
        "tool_calls",
        "error",
        "unmapped_error",
        "server error prompt is too long",
    ] {
        let event = json!({"choices":[{"finish_reason":reason,"delta":{}}]});
        let result = complete(
            setup(200, vec![sse(&[event])], false),
            MistralOptions::default(),
        )
        .0;
        assert_eq!(result.raw_stop_reason.as_ref().unwrap().as_str(), reason);
        if reason == "server error prompt is too long" {
            assert!(is_context_overflow(&result, None));
            assert!(is_retryable_assistant_error(&result));
        }
    }
    for (body, overflow, retryable) in [
        ("private-sentinel", false, false),
        ("prompt is too long", true, false),
        ("server error", false, true),
    ] {
        let result = complete(
            setup(403, vec![body.as_bytes().to_vec()], false),
            MistralOptions::default(),
        )
        .0;
        assert!(!format!("{result:?}").contains("private-sentinel"));
        assert_eq!(json!(&result.diagnostics)[0]["details"]["status"], 403);
        assert!(!result.error_message.as_ref().unwrap().contains("403"));
        assert_eq!(is_context_overflow(&result, None), overflow);
        assert_eq!(is_retryable_assistant_error(&result), retryable);
    }
}

#[test]
fn final_http_message_preserves_all_source_vetoes_after_utf16_truncation() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../conformance/provider-traces/mistral-conversations/errors.json"
    ))
    .unwrap();
    assert_eq!(fixture["cases"].as_array().unwrap().len(), 16);
    for case in fixture["cases"].as_array().unwrap() {
        let (message, events) = complete(
            setup(
                case["status"].as_u64().unwrap() as u16,
                vec![case["body"].as_str().unwrap().as_bytes().to_vec()],
                false,
            ),
            MistralOptions::default(),
        );
        assert_eq!(
            (
                is_context_overflow(&message, None),
                is_retryable_assistant_error(&message)
            ),
            (
                case["overflow"].as_bool().unwrap(),
                case["retryable"].as_bool().unwrap()
            ),
            "{}",
            case["id"]
        );
        assert_eq!(message.stop_reason, StopReason::Error);
        assert_eq!(events.len(), 1);
        assert_eq!(
            json!(&message.diagnostics)[0]["details"],
            json!({"status":case["status"],"contextOverflow":case["overflow"],"retryable":case["retryable"]})
        );
        assert_eq!(
            message.error_message.as_deref(),
            Some(
                match (
                    case["overflow"].as_bool().unwrap(),
                    case["retryable"].as_bool().unwrap()
                ) {
                    (true, true) => "prompt is too long; server error",
                    (true, false) => "prompt is too long",
                    (false, true) => "server error",
                    (false, false) => "provider request failed",
                }
            )
        );
    }
}

#[test]
fn fetch_no_body_statuses_observe_response_without_start_or_body_poll() {
    for status in [204, 205] {
        let mut s = setup(status, vec![], true);
        hooks(&mut s);
        let observed = s.observed.clone();
        let (message, events) = complete(s, MistralOptions::default());
        assert_eq!(message.stop_reason, StopReason::Error);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["type"], "error");
        assert_eq!(
            *observed.order.lock().unwrap(),
            vec!["payload", "send", "response"]
        );
        assert_eq!(observed.reads.load(Ordering::SeqCst), 0);
        assert_eq!(observed.closes.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn raw_options_closed_shapes_mismatch_setup_spawn_and_timeout_domain() {
    for raw in [
        json!({"promptMode":"other"}),
        json!({"reasoningEffort":null}),
        json!({"toolChoice":{"type":"function","function":{"name":"x","extra":true}}}),
        json!({"extra":1}),
    ] {
        assert!(serde_json::from_value::<MistralOptions>(raw).is_err());
    }
    let mut s = setup(200, vec![], false);
    s.request.resolved.model.api = "pi-messages".parse().unwrap();
    assert_eq!(
        complete(s, MistralOptions::default()).0.stop_reason,
        StopReason::Error
    );
    let mut s = setup(200, vec![], false);
    s.request.auth.api_key = None;
    assert_eq!(
        complete(s, MistralOptions::default()).0.stop_reason,
        StopReason::Error
    );
    let mut s = setup(200, vec![], false);
    s.request.options.runtime.spawner = Arc::new(Spawner {
        reject: true,
        ..Default::default()
    });
    assert_eq!(
        complete(s, MistralOptions::default()).0.stop_reason,
        StopReason::Error
    );
    for (value, expected) in [
        (0, 0),
        (2147483647, 2147483647),
        (2147483648, 1),
        (4294967295, 1),
    ] {
        let mut s = setup(200, vec![sse(&[terminal()])], false);
        s.request.options.wire.timeout_ms = Some(SafeUnsigned::new(value).unwrap());
        let clock = s.clock.clone();
        assert_eq!(
            complete(s, MistralOptions::default()).0.stop_reason,
            StopReason::Stop
        );
        assert_eq!(
            clock.waits.lock().unwrap()[0],
            Duration::from_millis(expected)
        );
    }
    let mut s = setup(200, vec![], false);
    s.request.options.wire.timeout_ms = Some(SafeUnsigned::new(4294967296).unwrap());
    assert_eq!(
        complete(s, MistralOptions::default()).0.stop_reason,
        StopReason::Error
    );
}

#[test]
fn simple_reasoning_selection_base_cap_and_raw_sampling_is_not_spread() {
    for (id, reasoning, effort, prompt) in [
        (
            "mistral-small-2603",
            Some(ThinkingLevel::Medium),
            Some("high"),
            None,
        ),
        ("mistral-small-latest", None, None, None),
        (
            "mistral-medium-3.5",
            Some(ThinkingLevel::Medium),
            Some("high"),
            None,
        ),
        (
            "magistral-medium-latest",
            Some(ThinkingLevel::Medium),
            None,
            Some("reasoning"),
        ),
    ] {
        let mut s = setup(200, vec![sse(&[terminal()])], false);
        s.request.resolved.model.id = id.parse().unwrap();
        let stream = s.api.stream_simple(SimpleStreamRequest {
            resolved: s.request.resolved,
            context: s.request.context,
            auth: s.request.auth,
            options: SimpleStreamOptions {
                wire: SimpleStreamOptionsWire {
                    reasoning,
                    ..Default::default()
                },
                runtime: s.request.options.runtime,
            },
        });
        ready(s.spawner.take());
        assert_eq!(ready(stream.result()).stop_reason, StopReason::Stop);
        let requests = s.observed.requests.lock().unwrap();
        let wire = &requests[0].2;
        assert_eq!(wire["reasoning_effort"].as_str(), effort);
        assert_eq!(wire["prompt_mode"].as_str(), prompt);
        assert_eq!(wire["max_tokens"], 16384);
    }
}

#[test]
fn replay_tools_images_strict_schema_and_collision_safe_foreign_ids() {
    let mut s = setup(200, vec![sse(&[terminal()])], false);
    let observed = s.observed.clone();
    let mut assistant = fixture()["events"].as_array().unwrap().last().unwrap()["message"].clone();
    assistant["provider"] = json!("foreign");
    assistant["model"] = json!("foreign");
    assistant["content"] = json!([
        {"type":"toolCall","id":"abc-123456","name":"lookup","arguments":{"query":"pi"}},
        {"type":"toolCall","id":"abc_123456","name":"lookup","arguments":{}}]);
    s.request.context=serde_json::from_value(json!({"messages":[assistant,
        {"role":"toolResult","toolCallId":"abc-123456","toolName":"lookup","content":[{"type":"text","text":" found "},{"type":"image","data":"aGVsbG8=","mimeType":"image/png"}],"isError":true,"timestamp":7},
        {"role":"toolResult","toolCallId":"abc_123456","toolName":"lookup","content":[],"isError":false,"timestamp":7}],
        "tools":[{"name":"lookup","description":"Look up","parameters":{"type":"object","properties":{"query":{"type":"string"}},"required":["query"]},"constrainedSampling":{"type":"json_schema","strict":"require"}}]})).unwrap();
    assert_eq!(
        complete(s, MistralOptions::default()).0.stop_reason,
        StopReason::Stop
    );
    let requests = observed.requests.lock().unwrap();
    let wire = &requests[0].2;
    let calls = wire["messages"][0]["tool_calls"].as_array().unwrap();
    assert_eq!(calls[0]["id"], "abc123456");
    assert_ne!(calls[0]["id"], calls[1]["id"]);
    assert_eq!(calls[1]["id"].as_str().unwrap().len(), 9);
    assert_eq!(wire["messages"][1]["tool_call_id"], calls[0]["id"]);
    assert_eq!(wire["messages"][2]["tool_call_id"], calls[1]["id"]);
    assert_eq!(
        wire["messages"][1]["content"][0]["text"],
        "[tool error] found"
    );
    assert_eq!(
        wire["messages"][2]["content"][0]["text"],
        "(no tool output)"
    );
    assert_eq!(wire["tools"][0]["function"]["strict"], true);
    assert_eq!(
        wire["tools"][0]["function"]["parameters"]["additionalProperties"],
        false
    );
}

#[test]
fn interleaved_tool_keys_null_id_and_argument_stringify_order() {
    let chunks = [
        json!({"choices":[{"delta":{"tool_calls":[
        {"id":"null","index":0.0,"function":{"name":"a","arguments":"{\"x\":"}},
        {"id":"0","function":{"name":"b","arguments":{"10":"ten","2":"two","z":1.0,"a":2}}}]}}]}),
        json!({"choices":[{"finish_reason":"tool_calls","delta":{"content":"between","tool_calls":[{"id":"changed","index":0e0,"function":{"name":"ignored","arguments":"1}"}}]}}]}),
    ];
    let (result, events) = complete(
        setup(200, vec![sse(&chunks)], false),
        MistralOptions::default(),
    );
    assert_eq!(result.stop_reason, StopReason::ToolUse);
    let content = json!(result.content);
    assert_eq!(content[0]["name"], "a");
    assert_ne!(content[0]["id"], "null");
    assert_ne!(content[0]["id"], "changed");
    assert_eq!(content[0]["arguments"], json!({"x":1}));
    assert_eq!(content[1]["id"], "0");
    assert_eq!(content[2]["text"], "between");
    let ends: Vec<_> = events
        .iter()
        .filter(|e| e["type"] == "toolcall_end")
        .map(|e| e["contentIndex"].clone())
        .collect();
    assert_eq!(ends, vec![json!(0), json!(1)]);
    assert!(
        events
            .iter()
            .any(|e| e["delta"] == "{\"2\":\"two\",\"10\":\"ten\",\"z\":1,\"a\":2}")
    );
}

#[test]
fn direct_model_headers_and_custom_simple_reasoning_map_preserve_source_values() {
    let mut s = setup(200, vec![sse(&[terminal()])], false);
    s.request.resolved.model.headers = Some(
        RawModelHeaders::new(BTreeMap::from([
            ("x-affinity".into(), "model-affinity".into()),
            ("x-model".into(), "public".into()),
        ]))
        .unwrap()
        .classify(&ModelHeaderPolicy::from_allowlist([
            "x-affinity".into(),
            "x-model".into(),
        ]))
        .unwrap(),
    );
    s.request.options.wire.session_id = Some("automatic".into());
    s.request.resolved.model.id = "mistral-small-2603".parse().unwrap();
    s.request.resolved.model.thinking_level_map = Some(ThinkingLevelMap::new(BTreeMap::from([(
        ModelThinkingLevel::Medium,
        Some("custom-effort".into()),
    )])));
    let stream = s.api.stream_simple(SimpleStreamRequest {
        resolved: s.request.resolved,
        context: s.request.context,
        auth: s.request.auth,
        options: SimpleStreamOptions {
            wire: SimpleStreamOptionsWire {
                stream: s.request.options.wire,
                reasoning: Some(ThinkingLevel::Medium),
                ..Default::default()
            },
            runtime: s.request.options.runtime,
        },
    });
    ready(s.spawner.take());
    assert_eq!(ready(stream.result()).stop_reason, StopReason::Stop);
    let requests = s.observed.requests.lock().unwrap();
    assert_eq!(requests[0].1.get_for_wire("x-model"), Some("public"));
    assert_eq!(
        requests[0].1.get_for_wire("x-affinity"),
        Some("model-affinity")
    );
    assert_eq!(requests[0].2["reasoning_effort"], "custom-effort");
}

#[test]
fn cached_token_source_normalization_precedes_final_count_admission() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../conformance/provider-traces/mistral-conversations/cache.json"
    ))
    .unwrap();
    for case in fixture["cases"].as_array().unwrap() {
        let mut event = terminal();
        event["usage"] = case["usage"].clone();
        let result = complete(
            setup(200, vec![sse(&[event])], false),
            MistralOptions::default(),
        )
        .0;
        if case["coreAdmitted"] == true {
            assert_eq!(result.stop_reason, StopReason::Stop, "{}", case["id"]);
            let expected: Usage = serde_json::from_value(case["expected"].clone()).unwrap();
            assert_eq!(result.usage, expected, "{}", case["id"]);
        } else {
            assert_eq!(result.stop_reason, StopReason::Error, "{}", case["id"]);
        }
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

    fn receive(socket: &mut std::net::TcpStream) -> (String, Vec<u8>) {
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
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().unwrap())
            })
            .unwrap_or(0);
        let mut body = vec![0; length];
        socket.read_exact(&mut body).unwrap();
        (head, body)
    }

    fn assert_released(socket: &mut std::net::TcpStream) {
        match socket.read(&mut [0]) {
            Ok(0) => {}
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
                ) => {}
            other => panic!("peer must observe socket release, not timeout: {other:?}"),
        }
    }

    struct RedirectObserver(Mutex<Vec<u16>>);
    impl ResponseMiddleware for RedirectObserver {
        fn on_response<'a>(
            &'a self,
            _: &'a TextStreamRequest,
            head: &'a AdapterResponseHead,
        ) -> BoxFuture<'a, Result<(), ModelsError>> {
            Box::pin(async move {
                self.0.lock().unwrap().push(head.status);
                Ok(())
            })
        }
    }

    #[test]
    fn native_relative_and_cross_origin_redirects_preserve_trace_and_cancel_release() {
        for mode in ["success", "cancel-body", "cancel-hop"] {
            let source = TcpListener::bind("127.0.0.1:0").unwrap();
            let source_address = source.local_addr().unwrap();
            let target = TcpListener::bind("127.0.0.1:0").unwrap();
            let target_address = target.local_addr().unwrap();
            let mut s = setup(200, vec![], false);
            let token = s.request.options.runtime.cancellation.clone();
            let hop_cancel = token.clone();
            let first = thread::spawn(move || {
                let (mut socket, _) = source.accept().unwrap();
                let initial = receive(&mut socket);
                // Incomplete redirect bodies must be closed without waiting for EOF.
                socket.write_all(b"HTTP/1.1 307 Temporary Redirect\r\nLocation: ../../continue\r\nContent-Length: 999\r\n\r\nx").unwrap();
                socket.flush().unwrap();
                assert_released(&mut socket);
                let (mut socket, _) = source.accept().unwrap();
                let relative = receive(&mut socket);
                if mode == "cancel-hop" {
                    hop_cancel.cancel();
                } else {
                    write!(socket,"HTTP/1.1 302 Found\r\nLocation: http://{target_address}/final\r\nContent-Length: 999\r\n\r\nx").unwrap();
                    socket.flush().unwrap();
                }
                assert_released(&mut socket);
                (initial, relative)
            });
            let f = fixture();
            let mut chunks = f["chunks"].as_array().unwrap().clone();
            if mode == "cancel-body" {
                chunks.truncate(1);
            }
            let mut bytes = sse(&chunks);
            if mode == "success" {
                bytes.extend_from_slice(b"data: [DONE]\n\n");
            }
            let last = thread::spawn(move || {
                if mode == "cancel-hop" {
                    return None;
                }
                let (mut socket, _) = target.accept().unwrap();
                let request = receive(&mut socket);
                socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n").unwrap();
                for chunk in bytes.chunks(3) {
                    write!(socket, "{:x}\r\n", chunk.len()).unwrap();
                    socket.write_all(chunk).unwrap();
                    socket.write_all(b"\r\n").unwrap();
                    socket.flush().unwrap();
                }
                // Hold the final HTTP body open: DONE or original cancellation
                // must release it, rather than relying on a server EOF.
                assert_released(&mut socket);
                Some(request)
            });
            let engine = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();
            s.request.resolved.endpoint =
                RequestEndpoint::new_resolved(format!("http://{source_address}")).unwrap();
            s.request.options.wire.session_id = Some("redirect-session".into());
            s.request.options.wire.max_retries = Some(SafeUnsigned::new(9).unwrap());
            s.request.auth.headers = ProviderHeaders::new(BTreeMap::from([
                ("cookie".into(), Some("synthetic-cookie".into())),
                ("x-custom-key".into(), Some("synthetic-custom".into())),
            ]));
            let observer = Arc::new(RedirectObserver(Mutex::new(Vec::new())));
            s.api.services.response = Some(observer.clone());
            s.api.services.transport = Arc::new(HttpTransport::new(
                Arc::new(NativeHttpClient::new(engine.handle().clone())),
                Arc::new(NoJitter),
            ));
            let mut stream = s.api.stream(s.request);
            let task = s.spawner.take();
            let (message, events) = engine.block_on(async {
                tokio::time::timeout(Duration::from_secs(5), async {
                    let worker = tokio::spawn(task);
                    let mut events = Vec::new();
                    while let Some(event) = stream.next().await {
                        if mode == "cancel-body"
                            && matches!(event, AssistantMessageEvent::ThinkingDelta { .. })
                        {
                            token.cancel();
                        }
                        events.push(event);
                    }
                    worker.await.unwrap();
                    (stream.result().await, events)
                })
                .await
                .expect("redirected Mistral call must settle")
            });
            let ((initial_head, initial_body), (relative_head, relative_body)) =
                first.join().unwrap();
            let final_request = last.join().unwrap();
            assert!(initial_head.starts_with("POST /v1/chat/completions HTTP/1.1\r\n"));
            assert!(relative_head.starts_with("POST /continue HTTP/1.1\r\n"));
            assert_eq!(initial_body, relative_body);
            assert_eq!(
                serde_json::from_slice::<Value>(&initial_body).unwrap(),
                json!({"model":"mistral-large-latest","stream":true,"messages":[{"role":"user","content":"hello"}],"prompt_cache_key":"redirect-session"})
            );
            for head in [&initial_head, &relative_head] {
                let head = head.to_ascii_lowercase();
                assert!(head.contains("authorization: bearer test-key\r\n"));
                assert!(head.contains("cookie: synthetic-cookie\r\n"));
                assert!(head.contains("content-type: application/json\r\n"));
                assert!(head.contains("x-affinity: redirect-session\r\n"));
            }
            if let Some((head, body)) = final_request {
                let head = head.to_ascii_lowercase();
                assert!(head.starts_with("get /final http/1.1\r\n"));
                assert!(body.is_empty());
                assert!(!head.contains("authorization:"));
                assert!(!head.contains("cookie:"));
                assert!(!head.contains("content-type:"));
                assert!(head.contains("x-custom-key: synthetic-custom\r\n"));
                assert!(head.contains("x-affinity: redirect-session\r\n"));
            }
            assert_eq!(
                *observer.0.lock().unwrap(),
                if mode == "cancel-hop" {
                    vec![]
                } else {
                    vec![200]
                }
            );
            assert_eq!(
                *s.clock.waits.lock().unwrap(),
                vec![Duration::from_secs(60)]
            );
            if mode == "success" {
                let expected: Vec<AssistantMessageEvent> =
                    serde_json::from_value(f["events"].clone()).unwrap();
                assert_eq!(events, expected);
                assert_eq!(message.stop_reason, StopReason::ToolUse);
            } else {
                assert_eq!(message.stop_reason, StopReason::Aborted);
                assert_eq!(
                    json!(message.content),
                    if mode == "cancel-body" {
                        json!([{"type":"thinking","thinking":"reason"}])
                    } else {
                        json!([])
                    }
                );
                assert_eq!(
                    events
                        .iter()
                        .filter(|event| matches!(
                            event,
                            AssistantMessageEvent::Error { .. }
                                | AssistantMessageEvent::Done { .. }
                        ))
                        .count(),
                    1
                );
            }
        }
    }
    #[test]
    fn real_native_http_fragmentation_eof_and_cancellation_release() {
        for mode in ["done", "eof", "cancel"] {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let mut bytes = sse(&[
                json!({"choices":[{"delta":{"content":"héllo 🌍"}}],"usage":{"prompt_tokens":3,"completion_tokens":2}}),
            ]);
            if mode != "cancel" {
                bytes.extend(sse(&[terminal()]));
            }
            if mode == "done" {
                bytes.extend(b"data: [DONE]\n\n");
            }
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
                assert!(head.starts_with(b"POST /v1/chat/completions HTTP/1.1\r\n"));
                assert!(
                    head.windows(b"x-byte: \xe9\r\n".len())
                        .any(|w| w == b"x-byte: \xe9\r\n")
                );
                let text: String = head.into_iter().map(char::from).collect();
                let length = text
                    .lines()
                    .find_map(|line| {
                        let (k, v) = line.split_once(':')?;
                        k.eq_ignore_ascii_case("content-length")
                            .then(|| v.trim().parse::<usize>().unwrap())
                    })
                    .unwrap();
                let mut body = vec![0; length];
                socket.read_exact(&mut body).unwrap();
                assert_eq!(
                    serde_json::from_slice::<Value>(&body).unwrap()["model"],
                    "mistral-large-latest"
                );
                socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n").unwrap();
                for chunk in bytes.chunks(3) {
                    write!(socket, "{:x}\r\n", chunk.len()).unwrap();
                    socket.write_all(chunk).unwrap();
                    socket.write_all(b"\r\n").unwrap();
                    socket.flush().unwrap();
                }
                if mode == "eof" {
                    socket.write_all(b"0\r\n\r\n").unwrap();
                    return;
                }
                let mut byte = [0];
                match socket.read(&mut byte) {
                    Ok(0) => {}
                    Err(e)
                        if matches!(
                            e.kind(),
                            std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
                        ) => {}
                    other => panic!("socket must release: {other:?}"),
                }
            });
            let engine = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();
            let mut s = setup(200, vec![], false);
            s.request.resolved.endpoint =
                RequestEndpoint::new_resolved(format!("http://{address}")).unwrap();
            s.request.auth.headers =
                ProviderHeaders::new(BTreeMap::from([("x-byte".into(), Some("é".into()))]));
            s.request.options.runtime.clock = Arc::new(TokioClock::new(engine.handle().clone()));
            s.api.services.transport = Arc::new(HttpTransport::new(
                Arc::new(NativeHttpClient::new(engine.handle().clone())),
                Arc::new(NativeRetryJitter),
            ));
            let token = s.request.options.runtime.cancellation.clone();
            let mut stream = s.api.stream(s.request);
            let task = s.spawner.take();
            engine.block_on(async {
                let worker = tokio::spawn(task);
                if mode == "cancel" {
                    loop {
                        if matches!(
                            stream.next().await,
                            Some(AssistantMessageEvent::TextDelta { .. })
                        ) {
                            break;
                        }
                    }
                    token.cancel();
                }
                let result = tokio::time::timeout(Duration::from_secs(5), stream.result())
                    .await
                    .unwrap();
                assert_eq!(
                    result.stop_reason,
                    if mode == "cancel" {
                        StopReason::Aborted
                    } else {
                        StopReason::Stop
                    }
                );
                assert_eq!(result.usage.input.get(), 3);
                assert_eq!(
                    json!(result.content),
                    json!([{"type":"text","text":"héllo 🌍"}])
                );
                worker.await.unwrap();
            });
            server.join().unwrap();
        }
    }
}
