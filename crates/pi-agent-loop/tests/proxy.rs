#![cfg(feature = "proxy")]
use pi_agent_loop::{AgentRequest, ProxyOptions, proxy_stream_fn, stream_proxy};
use pi_ai_core::*;
use pi_ai_transport::*;
use serde_json::{Value, json};
use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

struct FixedClock;
impl Clock for FixedClock {
    fn now_unix_ms(&self) -> u64 {
        7
    }
    fn sleep<'a>(
        &'a self,
        _: Duration,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), Cancelled>> {
        Box::pin(async { panic!("proxy must not schedule sleeps") })
    }
}
struct Spawn(tokio::runtime::Handle);
impl TaskSpawner for Spawn {
    fn spawn(&self, task: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
        self.0.spawn(task);
        Ok(())
    }
}
struct RejectSpawn;
impl TaskSpawner for RejectSpawn {
    fn spawn(&self, _: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
        Err(SpawnError::new())
    }
}
fn runtime(token: CancellationToken) -> RequestRuntime {
    RequestRuntime::with_noop_telemetry(
        token,
        Arc::new(FixedClock),
        Arc::new(Spawn(tokio::runtime::Handle::current())),
    )
}
fn block_on(future: impl std::future::Future<Output = ()>) {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            tokio::time::timeout(Duration::from_secs(5), future)
                .await
                .expect("proxy case must settle")
        });
}
fn model() -> Model {
    serde_json::from_value(json!({"id":"test-model","name":"Test model","api":"openai-responses","provider":"openai","baseUrl":"https://provider.invalid/v1","reasoning":true,"input":["text"],"cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0},"contextWindow":10000,"maxTokens":1000})).unwrap()
}
fn context() -> TranscriptContext {
    normalize_context(Context {
        messages: vec![],
        system_prompt: None,
        tools: None,
    })
}
fn options() -> ProxyOptions {
    ProxyOptions::new(
        SecretString::new("proxy-secret"),
        "https://proxy.invalid/base/",
    )
    .unwrap()
}
fn usage() -> Value {
    json!({"input":5,"output":7,"cacheRead":1,"cacheWrite":2,"totalTokens":15,"cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"total":0}})
}
fn done() -> Value {
    json!({"type":"done","reason":"stop","usage":usage(),"providerThinkingLevel":"high"})
}
fn wire(events: &[Value], trailing: bool) -> Vec<u8> {
    let mut text = events
        .iter()
        .map(|event| format!("data: {event}\n\n"))
        .collect::<String>();
    if !trailing {
        while text.ends_with('\n') {
            text.pop();
        }
    }
    text.into_bytes()
}
enum Read {
    Bytes(Vec<u8>),
    Fail,
    Pending,
}
struct BodyScript {
    reads: VecDeque<Read>,
    closed: Arc<AtomicUsize>,
    dropped: Arc<AtomicBool>,
    read_count: Arc<AtomicUsize>,
}
impl ResponseBody for BodyScript {
    fn next<'a>(
        &'a mut self,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, TransportError>> {
        self.read_count.fetch_add(1, Ordering::SeqCst);
        let next = self.reads.pop_front();
        Box::pin(async move {
            match next {
                Some(Read::Bytes(bytes)) => Ok(Some(bytes)),
                Some(Read::Fail) => Err(TransportError::new(TransportErrorKind::Connection)),
                Some(Read::Pending) => std::future::pending().await,
                None => Ok(None),
            }
        })
    }
    fn close(&mut self) {
        self.closed.fetch_add(1, Ordering::SeqCst);
    }
}
impl Drop for BodyScript {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}
struct Http {
    requests: Mutex<Vec<Value>>,
    response: Mutex<Option<Vec<Read>>>,
    status: u16,
    status_text: String,
    closed: Arc<AtomicUsize>,
    dropped: Arc<AtomicBool>,
    read_count: Arc<AtomicUsize>,
    send_pending: bool,
    cancel_on_response: bool,
    send_entered: AtomicBool,
    send_dropped: Arc<AtomicBool>,
}
impl Http {
    fn new(reads: Vec<Read>) -> Arc<Self> {
        Arc::new(Self {
            requests: Mutex::new(vec![]),
            response: Mutex::new(Some(reads)),
            status: 200,
            status_text: "OK".into(),
            closed: Arc::new(AtomicUsize::new(0)),
            dropped: Arc::new(AtomicBool::new(false)),
            read_count: Arc::new(AtomicUsize::new(0)),
            send_pending: false,
            cancel_on_response: false,
            send_entered: AtomicBool::new(false),
            send_dropped: Arc::new(AtomicBool::new(false)),
        })
    }
}
struct SendGuard(Arc<AtomicBool>);
impl Drop for SendGuard {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}
impl RawHttpClient for Http {
    fn send<'a>(
        &'a self,
        request: &'a HttpRequest,
        runtime: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<HttpResponse, TransportError>> {
        Box::pin(async move {
            self.requests.lock().unwrap().push(json!({"url":request.endpoint.expose_for_transport(),"method":request.method,"headers":request.headers.iter_for_wire().collect::<std::collections::BTreeMap<_,_>>(),"body":serde_json::from_slice::<Value>(&request.body).unwrap()}));
            self.send_entered.store(true, Ordering::SeqCst);
            if self.send_pending {
                let _guard = SendGuard(self.send_dropped.clone());
                std::future::pending::<()>().await;
            }
            if self.cancel_on_response {
                runtime.cancellation.cancel();
            }
            Ok(HttpResponse {
                status: self.status,
                status_text: self.status_text.clone(),
                headers: HttpHeaders::new(),
                body: Box::new(BodyScript {
                    reads: self
                        .response
                        .lock()
                        .unwrap()
                        .take()
                        .expect("one HTTP attempt")
                        .into(),
                    closed: self.closed.clone(),
                    dropped: self.dropped.clone(),
                    read_count: self.read_count.clone(),
                }),
            })
        })
    }
}
async fn collect(mut stream: AssistantStream) -> (Vec<AssistantMessageEvent>, AssistantMessage) {
    let mut events = vec![];
    while let Some(event) = stream.next().await {
        events.push(event);
    }
    let result = stream.result().await;
    (events, result)
}
async fn closed(http: &Http) {
    for _ in 0..100 {
        if http.dropped.load(Ordering::SeqCst) {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(http.dropped.load(Ordering::SeqCst));
    assert_eq!(http.closed.load(Ordering::SeqCst), 1);
}
fn terminal_count(events: &[AssistantMessageEvent]) -> usize {
    events
        .iter()
        .filter(|event| {
            matches!(
                event,
                AssistantMessageEvent::Done { .. } | AssistantMessageEvent::Error { .. }
            )
        })
        .count()
}

#[test]
fn body_allowlist_and_agent_adapter_drop_provider_credentials() {
    block_on(async {
        let http = Http::new(vec![Read::Bytes(wire(&[done()], false))]);
        let mut options = options();
        options.headers =
            Some(serde_json::from_value(json!({"X-Explicit":"yes","remove":null})).unwrap());
        let values = json!({"temperature":0.5,"samplingParams":{"seed":9},"maxTokens":22,"reasoning":"high","cacheRetention":"long","sessionId":"session","metadata":{"task":"test"},"transport":"sse","thinkingBudgets":{"high":19},"maxRetryDelayMs":71,"identity":{"attribution":{"name":"secret-app","url":"https://private.invalid"}},"timeoutMs":33,"maxRetries":4,"websocketConnectTimeoutMs":44,"toolChoice":"none","deferred":true});
        let request_options: SimpleStreamOptionsWire =
            serde_json::from_value(values.clone()).unwrap();
        let token = CancellationToken::new();
        let adapter = proxy_stream_fn(options, http.clone(), runtime(token.clone()));
        let stream = adapter(AgentRequest {
            model: model(),
            context: context(),
            options: request_options,
            api_key: Some(SecretString::new("provider-secret")),
            cancellation: token,
        })
        .await
        .unwrap();
        let (events, result) = collect(stream).await;
        assert_eq!(result.stop_reason, StopReason::Stop);
        assert_eq!(terminal_count(&events), 1);
        closed(&http).await;
        let requests = http.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        let request = &requests[0];
        assert_eq!(request["url"], "https://proxy.invalid/base/api/stream");
        assert_eq!(request["method"], "POST");
        assert_eq!(
            request["headers"],
            json!({"authorization":"Bearer proxy-secret","content-type":"application/json"})
        );
        let body = &request["body"];
        assert_eq!(
            body.as_object()
                .unwrap()
                .keys()
                .cloned()
                .collect::<Vec<_>>(),
            vec!["context", "model", "options"]
        );
        assert_eq!(body["model"], serde_json::to_value(model()).unwrap());
        assert_eq!(body["context"], serde_json::to_value(context()).unwrap());
        let mut expected = values.as_object().unwrap().clone();
        for key in [
            "identity",
            "timeoutMs",
            "maxRetries",
            "websocketConnectTimeoutMs",
            "toolChoice",
            "deferred",
        ] {
            expected.remove(key);
        }
        expected.insert("headers".into(), json!({"X-Explicit":"yes","remove":null}));
        assert_eq!(body["options"], Value::Object(expected));
        let bytes = body.to_string();
        for secret in [
            "proxy-secret",
            "provider-secret",
            "secret-app",
            "private.invalid",
        ] {
            assert!(!bytes.contains(secret));
        }
    });
}

#[test]
fn fragmented_utf8_reconstructs_all_content_and_terminal_metadata() {
    block_on(async {
        let events = vec![
            json!({"type":"start"}),
            json!({"type":"text_start","contentIndex":0}),
            json!({"type":"text_delta","contentIndex":0,"delta":"你好🙂"}),
            json!({"type":"text_end","contentIndex":0,"contentSignature":"text-sig"}),
            json!({"type":"thinking_start","contentIndex":1}),
            json!({"type":"thinking_delta","contentIndex":1,"delta":"consider"}),
            json!({"type":"thinking_end","contentIndex":1,"contentSignature":"thinking-sig"}),
            json!({"type":"toolcall_start","contentIndex":2,"id":"old-id","toolName":"lookup"}),
            json!({"type":"toolcall_delta","contentIndex":2,"delta":"{\"value\":\"你"}),
            json!({"type":"toolcall_delta","contentIndex":2,"delta":"好\"}"}),
            json!({"type":"toolcall_end","contentIndex":2,"toolCall":{"type":"toolCall","id":"final-id","name":"lookup-final","arguments":{"value":"你好"},"namespace":"dynamic_tools","thoughtSignature":"tool-sig"}}),
            json!({"type":"done","reason":"toolUse","usage":usage(),"providerThinkingLevel":"xhigh"}),
        ];
        let http = Http::new(
            wire(&events, false)
                .into_iter()
                .map(|byte| Read::Bytes(vec![byte]))
                .collect(),
        );
        let (observed, result) = collect(stream_proxy(
            model(),
            context(),
            options(),
            http.clone(),
            runtime(CancellationToken::new()),
        ))
        .await;
        assert_eq!(observed.len(), events.len());
        assert_eq!(terminal_count(&observed), 1);
        assert_eq!(result.stop_reason, StopReason::ToolUse);
        assert_eq!(result.provider_thinking_level.as_deref(), Some("xhigh"));
        assert_eq!(
            result.usage,
            serde_json::from_value::<Usage>(usage()).unwrap()
        );
        let data = serde_json::to_value(&result).unwrap();
        assert_eq!(
            data["content"],
            json!([{"type":"text","text":"你好🙂","textSignature":"text-sig"},{"type":"thinking","thinking":"consider","thinkingSignature":"thinking-sig"},{"type":"toolCall","id":"final-id","name":"lookup-final","arguments":{"value":"你好"},"namespace":"dynamic_tools","thoughtSignature":"tool-sig"}])
        );
        let delta = serde_json::to_value(&observed[8]).unwrap();
        assert_eq!(
            delta["partial"]["content"][2]["arguments"],
            json!({"value":"你"})
        );
        assert!(delta["partial"]["content"][2].get("namespace").is_none());
        let end = serde_json::to_value(&observed[10]).unwrap();
        assert_eq!(end["toolCall"], data["content"][2]);
        assert!(data.to_string().find("partialJson").is_none());
        closed(&http).await;
    });
}

#[test]
fn each_data_line_is_independent_and_final_line_is_flushed() {
    block_on(async {
        let bytes=format!("\u{feff}: comment\ndata:{{ignored}}\ndata: \n\nevent: ignored\ndata: {{\"type\":\"future_event\"}}\ndata: {{\"type\":\"start\"}}\r\ndata: {}",done()).into_bytes();
        let http = Http::new(vec![Read::Bytes(bytes)]);
        let (events, result) = collect(stream_proxy(
            model(),
            context(),
            options(),
            http.clone(),
            runtime(CancellationToken::new()),
        ))
        .await;
        assert_eq!(events.len(), 2);
        assert!(matches!(events[0], AssistantMessageEvent::Start { .. }));
        assert_eq!(result.provider_thinking_level.as_deref(), Some("high"));
        closed(&http).await;
    });
}

#[test]
fn eof_keeps_partial_and_emits_exact_incomplete_connection_error() {
    block_on(async {
        let http = Http::new(vec![Read::Bytes(wire(
            &[
                json!({"type":"start"}),
                json!({"type":"text_start","contentIndex":0}),
                json!({"type":"text_delta","contentIndex":0,"delta":"partial"}),
            ],
            true,
        ))]);
        let (events, result) = collect(stream_proxy(
            model(),
            context(),
            options(),
            http.clone(),
            runtime(CancellationToken::new()),
        ))
        .await;
        assert_eq!(terminal_count(&events), 1);
        assert_eq!(result.stop_reason, StopReason::Error);
        assert_eq!(
            result.error_message.as_deref(),
            Some("Connection closed by proxy server before the response completed")
        );
        assert_eq!(
            serde_json::to_value(result.content).unwrap(),
            json!([{"type":"text","text":"partial"}])
        );
        closed(&http).await;
    });
}

#[test]
fn reported_errors_preserve_partial_usage_and_provider_thinking() {
    block_on(async {
        let http = Http::new(vec![Read::Bytes(wire(
            &[
                json!({"type":"text_start","contentIndex":0}),
                json!({"type":"text_delta","contentIndex":0,"delta":"partial"}),
                json!({"type":"error","reason":"aborted","errorMessage":"server cancelled","usage":usage(),"providerThinkingLevel":"native-effort"}),
            ],
            false,
        ))]);
        let (events, result) = collect(stream_proxy(
            model(),
            context(),
            options(),
            http.clone(),
            runtime(CancellationToken::new()),
        ))
        .await;
        assert_eq!(terminal_count(&events), 1);
        assert_eq!(result.stop_reason, StopReason::Aborted);
        assert_eq!(result.error_message.as_deref(), Some("server cancelled"));
        assert_eq!(
            result.provider_thinking_level.as_deref(),
            Some("native-effort")
        );
        assert_eq!(
            result.usage,
            serde_json::from_value::<Usage>(usage()).unwrap()
        );
        closed(&http).await;
    });
}

#[test]
fn http_errors_prefer_json_error_and_fall_back_to_status() {
    block_on(async {
        for (body, expected) in [
            (
                b"{\"error\":\"rate limited\"}".to_vec(),
                "Proxy error: rate limited",
            ),
            (b"not JSON".to_vec(), "Proxy error: 502 Bad Gateway"),
        ] {
            let mut http = Http::new(vec![Read::Bytes(body)]);
            let mutable = Arc::get_mut(&mut http).unwrap();
            mutable.status = 502;
            mutable.status_text = "Bad Gateway".into();
            let (events, result) = collect(stream_proxy(
                model(),
                context(),
                options(),
                http.clone(),
                runtime(CancellationToken::new()),
            ))
            .await;
            assert_eq!(events.len(), 1);
            assert_eq!(result.stop_reason, StopReason::Error);
            assert_eq!(result.error_message.as_deref(), Some(expected));
            closed(&http).await;
        }
    });
}

#[test]
fn malformed_json_and_wrong_content_settle_once_and_close() {
    block_on(async {
        for (bytes, expected) in [
            (b"data: {broken}\n".to_vec(), "Invalid proxy event JSON"),
            (
                wire(
                    &[json!({"type":"text_delta","contentIndex":0,"delta":"x"})],
                    true,
                ),
                "Received text_delta for non-text content",
            ),
            (
                wire(&[json!({"type":"text_start","contentIndex":2})], true),
                "Proxy content index is not contiguous",
            ),
        ] {
            let http = Http::new(vec![Read::Bytes(bytes)]);
            let (events, result) = collect(stream_proxy(
                model(),
                context(),
                options(),
                http.clone(),
                runtime(CancellationToken::new()),
            ))
            .await;
            assert_eq!(terminal_count(&events), 1);
            assert_eq!(result.error_message.as_deref(), Some(expected));
            closed(&http).await;
        }
    });
}

#[test]
fn transport_failure_retains_partial_and_closes() {
    block_on(async {
        let http = Http::new(vec![
            Read::Bytes(wire(
                &[
                    json!({"type":"text_start","contentIndex":0}),
                    json!({"type":"text_delta","contentIndex":0,"delta":"kept"}),
                ],
                true,
            )),
            Read::Fail,
        ]);
        let (events, result) = collect(stream_proxy(
            model(),
            context(),
            options(),
            http.clone(),
            runtime(CancellationToken::new()),
        ))
        .await;
        assert_eq!(terminal_count(&events), 1);
        assert_eq!(result.stop_reason, StopReason::Error);
        assert_eq!(
            result.error_message.as_deref(),
            Some("Transport failure: Connection")
        );
        assert_eq!(
            serde_json::to_value(result.content).unwrap(),
            json!([{"type":"text","text":"kept"}])
        );
        closed(&http).await;
    });
}

#[test]
fn cancellation_retains_observed_partial_and_closes_pending_response() {
    block_on(async {
        let http = Http::new(vec![
            Read::Bytes(wire(
                &[
                    json!({"type":"text_start","contentIndex":0}),
                    json!({"type":"text_delta","contentIndex":0,"delta":"kept"}),
                ],
                true,
            )),
            Read::Pending,
        ]);
        let token = CancellationToken::new();
        let mut stream = stream_proxy(
            model(),
            context(),
            options(),
            http.clone(),
            runtime(token.clone()),
        );
        assert!(matches!(
            stream.next().await,
            Some(AssistantMessageEvent::TextStart { .. })
        ));
        assert!(matches!(
            stream.next().await,
            Some(AssistantMessageEvent::TextDelta { .. })
        ));
        token.cancel();
        let (events, result) = collect(stream).await;
        assert_eq!(terminal_count(&events), 1);
        assert_eq!(result.stop_reason, StopReason::Aborted);
        assert_eq!(
            result.error_message.as_deref(),
            Some(REQUEST_CANCELLED_MESSAGE)
        );
        assert_eq!(
            serde_json::to_value(result.content).unwrap(),
            json!([{"type":"text","text":"kept"}])
        );
        closed(&http).await;
    });
}

#[test]
fn already_cancelled_skips_http_and_pending_acquisition_is_dropped() {
    block_on(async {
        let http = Http::new(vec![]);
        let token = CancellationToken::new();
        token.cancel();
        let (events, result) = collect(stream_proxy(
            model(),
            context(),
            options(),
            http.clone(),
            runtime(token),
        ))
        .await;
        tokio::task::yield_now().await;
        assert_eq!(terminal_count(&events), 1);
        assert_eq!(result.stop_reason, StopReason::Aborted);
        assert!(http.requests.lock().unwrap().is_empty());
        let mut http = Http::new(vec![]);
        Arc::get_mut(&mut http).unwrap().send_pending = true;
        let token = CancellationToken::new();
        let stream = stream_proxy(
            model(),
            context(),
            options(),
            http.clone(),
            runtime(token.clone()),
        );
        while !http.send_entered.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
        token.cancel();
        let (events, result) = collect(stream).await;
        assert_eq!(terminal_count(&events), 1);
        assert_eq!(result.stop_reason, StopReason::Aborted);
        for _ in 0..100 {
            if http.send_dropped.load(Ordering::SeqCst) {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(http.send_dropped.load(Ordering::SeqCst));
        assert_eq!(http.closed.load(Ordering::SeqCst), 0);
    });
}

#[test]
fn first_terminal_wins_and_releases_the_unread_tail() {
    block_on(async {
        let http = Http::new(vec![
            Read::Bytes(wire(&[done()], true)),
            Read::Bytes(b"data: {broken}\n".to_vec()),
            Read::Pending,
        ]);
        let (events, result) = collect(stream_proxy(
            model(),
            context(),
            options(),
            http.clone(),
            runtime(CancellationToken::new()),
        ))
        .await;
        assert_eq!(events.len(), 1);
        assert_eq!(result.stop_reason, StopReason::Stop);
        assert_eq!(http.read_count.load(Ordering::SeqCst), 1);
        closed(&http).await;
    });
}

#[test]
fn cancellation_racing_response_acquisition_still_closes_the_response() {
    block_on(async {
        let mut http = Http::new(vec![Read::Pending]);
        Arc::get_mut(&mut http).unwrap().cancel_on_response = true;
        let (events, result) = collect(stream_proxy(
            model(),
            context(),
            options(),
            http.clone(),
            runtime(CancellationToken::new()),
        ))
        .await;
        assert_eq!(terminal_count(&events), 1);
        assert_eq!(result.stop_reason, StopReason::Aborted);
        closed(&http).await;
        assert_eq!(http.read_count.load(Ordering::SeqCst), 0);
    });
}

#[test]
fn spawn_failure_settles_without_http_and_configuration_is_redacted() {
    block_on(async {
        let http = Http::new(vec![]);
        let mut runtime = runtime(CancellationToken::new());
        runtime.spawner = Arc::new(RejectSpawn);
        let (events, result) = collect(stream_proxy(
            model(),
            context(),
            options(),
            http.clone(),
            runtime,
        ))
        .await;
        assert_eq!(terminal_count(&events), 1);
        assert_eq!(result.stop_reason, StopReason::Error);
        assert_eq!(
            result.error_message.as_deref(),
            Some("stream setup task could not be started")
        );
        assert!(http.requests.lock().unwrap().is_empty());
        assert_eq!(format!("{:?}", options()), "ProxyOptions([REDACTED])");
        for url in [
            "file:///tmp/proxy",
            "https://user:password@proxy.invalid",
            "https://proxy.invalid?route=x",
            "https://proxy.invalid#route",
        ] {
            assert!(ProxyOptions::new(SecretString::new("secret"), url).is_err());
        }
    });
}

#[test]
fn selected_transport_line_bound_is_explicit() {
    block_on(async {
        let http = Http::new(vec![Read::Bytes(vec![
            b'x';
            SseLimits::default().max_frame_bytes
                + 1
        ])]);
        let (events, result) = collect(stream_proxy(
            model(),
            context(),
            options(),
            http.clone(),
            runtime(CancellationToken::new()),
        ))
        .await;
        assert_eq!(terminal_count(&events), 1);
        assert_eq!(
            result.error_message.as_deref(),
            Some("Proxy data line limit exceeded")
        );
        closed(&http).await;
    });
}

fn source_value(mut value: Value) -> Value {
    match &mut value {
        Value::Object(fields) => {
            for (key, item) in fields {
                if key == "durationMs" {
                    if item.is_number() {
                        assert!(item.as_u64().is_some());
                    }
                    *item = json!("<durationMs>");
                } else {
                    *item = source_value(item.take());
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                *item = source_value(item.take());
            }
        }
        Value::Number(number) => {
            if let Some(float) = number.as_f64()
                && float >= 0.0
                && float.fract() == 0.0
                && float < u64::MAX as f64
            {
                value = json!(float as u64);
            }
        }
        _ => {}
    }
    value
}

#[test]
fn target_source_proxy_vectors_match_wire_events_and_full_result() {
    block_on(async {
        let fixture: Value = serde_json::from_str(include_str!(
            "../../../conformance/agent-proxy-v1.4.0/observations.json"
        ))
        .unwrap();
        for case in fixture["observations"].as_array().unwrap() {
            let input = &case["input"];
            let bytes = input["wire"].as_str().unwrap().as_bytes().to_vec();
            let reads = if input["fragmented"].as_bool() == Some(true) {
                bytes
                    .into_iter()
                    .map(|byte| Read::Bytes(vec![byte]))
                    .collect()
            } else {
                vec![Read::Bytes(bytes)]
            };
            let mut http = Http::new(reads);
            let inner = Arc::get_mut(&mut http).unwrap();
            inner.status = input["status"].as_u64().unwrap_or(200) as u16;
            inner.status_text = input["statusText"].as_str().unwrap_or("OK").into();
            let mut options = options();
            let mut wire_options = input.get("options").cloned().unwrap_or_else(|| json!({}));
            let fields = wire_options.as_object_mut().unwrap();
            let headers = fields.remove("headers");
            let provider_key = fields.remove("apiKey");
            options.headers = headers.map(|headers| serde_json::from_value(headers).unwrap());
            let wire_options = serde_json::from_value(wire_options).unwrap();
            let token = CancellationToken::new();
            let adapter = proxy_stream_fn(options, http.clone(), runtime(token.clone()));
            let stream = adapter(AgentRequest {
                model: model(),
                context: context(),
                options: wire_options,
                api_key: provider_key.and_then(|key| key.as_str().map(SecretString::new)),
                cancellation: token,
            })
            .await
            .unwrap();
            let (events, result) = collect(stream).await;
            let events = events
                .into_iter()
                .map(|event| {
                    let mut event = serde_json::to_value(event).unwrap();
                    for key in ["partial", "message", "error"] {
                        event.as_object_mut().unwrap().remove(key);
                    }
                    event
                })
                .collect::<Vec<_>>();
            assert_eq!(
                source_value(json!(events)),
                source_value(case["events"].clone()),
                "{} event payloads",
                case["id"]
            );
            assert_eq!(
                source_value(serde_json::to_value(result).unwrap()),
                source_value(case["result"].clone()),
                "{} result",
                case["id"]
            );
            assert_eq!(
                source_value(http.requests.lock().unwrap()[0].clone()),
                source_value(case["request"].clone()),
                "{} request",
                case["id"]
            );
            closed(&http).await;
        }
    });
}
