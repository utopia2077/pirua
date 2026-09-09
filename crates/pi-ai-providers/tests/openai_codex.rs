#![cfg(feature = "wire-openai-codex-responses")]
use pi_ai_core::*;
use pi_ai_providers::api::{openai_codex_responses::*, runtime::AdapterServices};
use pi_ai_transport::*;
use serde_json::{Value, json};
use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    task::{Context as Cx, Poll, Waker},
    time::Duration,
};
fn poll<F: Future + ?Sized>(f: Pin<&mut F>) -> Poll<F::Output> {
    f.poll(&mut Cx::from_waker(Waker::noop()))
}
struct Sleep {
    done: Arc<AtomicBool>,
}
impl Future for Sleep {
    type Output = Result<(), Cancelled>;
    fn poll(self: Pin<&mut Self>, _: &mut Cx<'_>) -> Poll<Self::Output> {
        if self.done.load(Ordering::SeqCst) {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
}
#[derive(Default)]
struct Clock {
    now: AtomicU64,
    pending: Mutex<Vec<(u64, Arc<AtomicBool>)>>,
    sleeps: Mutex<Vec<u64>>,
}
impl pi_ai_core::Clock for Clock {
    fn now_unix_ms(&self) -> u64 {
        7 + self.now.load(Ordering::SeqCst)
    }
    fn sleep<'a>(
        &'a self,
        d: Duration,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), Cancelled>> {
        let done = Arc::new(AtomicBool::new(false));
        self.pending
            .lock()
            .unwrap()
            .push((d.as_millis() as u64, done.clone()));
        Box::pin(Sleep { done })
    }
}
impl Clock {
    fn advance(&self) {
        let mut pending = self.pending.lock().unwrap();
        pending.retain(|(_, done)| Arc::strong_count(done) > 1 && !done.load(Ordering::SeqCst));
        let i = pending
            .iter()
            .enumerate()
            .min_by_key(|(_, v)| v.0)
            .map(|(i, _)| i)
            .expect("pending operation has a timer");
        let (ms, done) = pending.remove(i);
        self.sleeps.lock().unwrap().push(ms);
        self.now.fetch_add(ms, Ordering::SeqCst);
        done.store(true, Ordering::SeqCst);
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
    fn drive(&self) {
        let tasks = std::mem::take(&mut *self.0.lock().unwrap());
        for mut task in tasks {
            if poll(task.as_mut()).is_pending() {
                self.0.lock().unwrap().push(task);
            }
        }
    }
}
struct Entropy;
impl UuidV7Entropy for Entropy {
    fn random_bytes(&self) -> Result<[u8; 16], UuidV7EntropyError> {
        Ok([0; 16])
    }
}
struct Jitter;
impl RetryJitter for Jitter {
    fn sample(&self) -> f64 {
        panic!("Codex must never use SDK jitter")
    }
}
type Events = Arc<Mutex<VecDeque<Result<WebSocketEvent, TransportError>>>>;
#[derive(Default)]
struct Wire {
    payloads: usize,
    fetch_body_kinds: Vec<&'static str>,
    replacement: Option<Value>,
    states: Vec<Arc<AtomicBool>>,
    exchanges: Vec<Events>,
    http: Vec<(String, HttpHeaders, Vec<u8>)>,
    sent: Vec<Value>,
    sent_raw: Vec<String>,
    connections: Vec<(String, HttpHeaders)>,
    closes: Vec<(u16, String)>,
}
struct Peer {
    spec: Value,
    wire: Arc<Mutex<Wire>>,
    http_index: AtomicU64,
    ws_index: AtomicU64,
    connect_index: AtomicU64,
}
fn full(id: &str) -> Vec<Value> {
    vec![
        json!({"type":"response.created","response":{"id":id}}),
        json!({"type":"response.output_item.added","output_index":0,"item":{"type":"message","id":format!("m-{id}"),"role":"assistant","status":"in_progress","content":[]}}),
        json!({"type":"response.content_part.added","output_index":0,"content_index":0,"part":{"type":"output_text","text":""}}),
        json!({"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":"Hello"}),
        json!({"type":"response.output_item.done","output_index":0,"item":{"type":"message","id":format!("m-{id}"),"role":"assistant","status":"completed","content":[{"type":"output_text","text":"Hello"}]}}),
        json!({"type":"response.completed","response":{"id":id,"status":"completed","end_turn":false,"service_tier":"default","usage":{"input_tokens":20,"output_tokens":3,"total_tokens":23,"input_tokens_details":{"cached_tokens":2}}}}),
    ]
}
fn sse(events: &[Value]) -> String {
    events.iter().map(|e| format!("data: {e}\n\n")).collect()
}
struct Body {
    chunks: VecDeque<Vec<u8>>,
    error: bool,
}
impl ResponseBody for Body {
    fn next<'a>(
        &'a mut self,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, TransportError>> {
        Box::pin(async move {
            if let Some(v) = self.chunks.pop_front() {
                Ok(Some(v))
            } else if self.error {
                Err(TransportError::new(TransportErrorKind::Connection))
            } else {
                Ok(None)
            }
        })
    }
    fn close(&mut self) {
        self.chunks.clear();
    }
}
impl CustomFetch for Peer {
    fn fetch<'a>(
        &'a self,
        r: &'a FetchRequest,
        runtime: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<HttpResponse, TransportError>> {
        Box::pin(async move {
            let FetchHeaders::Constructed(headers) = &r.headers else {
                panic!("Codex source supplies constructed Headers");
            };
            let (kind, body) = match &r.body {
                FetchBody::Text(v) => ("text", v.as_bytes().to_vec()),
                FetchBody::Bytes(v) => ("bytes", v.clone()),
            };
            self.wire.lock().unwrap().fetch_body_kinds.push(kind);
            let physical = HttpRequest {
                endpoint: RequestEndpoint::new_resolved(r.endpoint.expose_for_transport()).unwrap(),
                method: r.method.clone(),
                headers: headers.clone(),
                body,
                proxy: r.proxy.clone(),
            };
            self.send(&physical, runtime).await
        })
    }
}
impl RawHttpClient for Peer {
    fn send<'a>(
        &'a self,
        r: &'a HttpRequest,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<HttpResponse, TransportError>> {
        self.wire.lock().unwrap().http.push((
            r.endpoint.expose_for_transport().to_owned(),
            r.headers.clone(),
            r.body.clone(),
        ));
        let index = self.http_index.fetch_add(1, Ordering::SeqCst) as usize;
        let spec = self.spec["http"][index].clone();
        Box::pin(async move {
            if spec["error"].is_string() {
                return Err(TransportError::new(TransportErrorKind::Connection));
            }
            let payload = spec["body"].as_str().map(str::to_owned).unwrap_or_else(|| {
                sse(spec["events"]
                    .as_array()
                    .unwrap_or(&full(&format!("http{}", index + 1))))
            });
            let chunks = spec["chunks"]
                .as_array()
                .map(|v| {
                    v.iter()
                        .map(|v| v.as_str().unwrap().as_bytes().to_vec())
                        .collect()
                })
                .unwrap_or_else(|| VecDeque::from([payload.into_bytes()]));
            let mut headers = HttpHeaders::new();
            if let Some(fields) = spec["headers"].as_object() {
                for (k, v) in fields {
                    headers.set(k, v.as_str().unwrap()).unwrap();
                }
            } else {
                headers.set("content-type", "text/event-stream").unwrap();
            }
            Ok(HttpResponse {
                status: spec["status"].as_u64().unwrap_or(200) as u16,
                status_text: spec["statusText"].as_str().unwrap_or("").into(),
                headers,
                body: if spec["nullBody"] == true {
                    Box::new(AbsentResponseBody)
                } else {
                    Box::new(Body {
                        chunks,
                        error: spec["readError"].is_string(),
                    })
                },
            })
        })
    }
}
struct Socket {
    peer: Arc<Peer>,
    closed: Arc<AtomicBool>,
    current: Option<Events>,
}
struct Exchange {
    events: Events,
    hold: bool,
}
impl WebSocketExchange for Exchange {
    fn next<'a>(
        &'a mut self,
        rt: &'a RequestRuntime,
        idle: Option<Duration>,
    ) -> BoxFuture<'a, Result<Option<WebSocketEvent>, TransportError>> {
        if self.hold && idle.is_none() {
            return Box::pin(std::future::poll_fn(move |_| {
                self.events
                    .lock()
                    .unwrap()
                    .pop_front()
                    .map_or(Poll::Pending, |event| Poll::Ready(event.map(Some)))
            }));
        }
        Box::pin(async move {
            if let Some(event) = self.events.lock().unwrap().pop_front() {
                event.map(Some)
            } else if self.hold {
                if let Some(idle) = idle {
                    rt.clock.sleep(idle, &rt.cancellation).await.unwrap();
                    Err(TransportError::new(TransportErrorKind::Timeout))
                } else {
                    std::future::pending().await
                }
            } else {
                Ok(None)
            }
        })
    }
}
impl PersistentWebSocketConnector for Peer {
    fn connect<'a>(
        &'a self,
        _: &'a WebSocketRequest,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<Box<dyn PersistentWebSocket>, TransportError>> {
        unreachable!("Arc connector wrapper owns peer")
    }
}
struct Connector(Arc<Peer>);
impl PersistentWebSocketConnector for Connector {
    fn connect<'a>(
        &'a self,
        r: &'a WebSocketRequest,
        rt: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<Box<dyn PersistentWebSocket>, TransportError>> {
        self.0
            .wire
            .lock()
            .unwrap()
            .connections
            .push((r.endpoint.expose_for_transport().into(), r.headers.clone()));
        let index = self.0.connect_index.fetch_add(1, Ordering::SeqCst) as usize;
        Box::pin(async move {
            let config = &self.0.spec["connect"][index];
            if config["hold"] == true {
                rt.clock
                    .sleep(r.connect_timeout.unwrap(), &rt.cancellation)
                    .await
                    .unwrap();
                return Err(TransportError::new(TransportErrorKind::Timeout));
            }
            if config["error"] == true {
                return Err(TransportError::new(TransportErrorKind::Connection));
            }
            let closed = Arc::new(AtomicBool::new(false));
            self.0.wire.lock().unwrap().states.push(closed.clone());
            Ok(Box::new(Socket {
                peer: self.0.clone(),
                closed,
                current: None,
            }) as Box<dyn PersistentWebSocket>)
        })
    }
}
impl PersistentWebSocket for Socket {
    fn state(&self) -> WebSocketState {
        if self.closed.load(Ordering::SeqCst) {
            WebSocketState::Closed
        } else {
            WebSocketState::Open
        }
    }
    fn start_exchange<'a>(
        &'a mut self,
        message: WebSocketMessage,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<Box<dyn WebSocketExchange>, TransportError>> {
        let WebSocketMessage::Text(text) = message else {
            panic!("text")
        };
        {
            let mut wire = self.peer.wire.lock().unwrap();
            wire.sent_raw.push(text.clone());
            wire.sent
                .push(serde_json::from_str(&text).unwrap_or(Value::Null));
        }
        let index = self.peer.ws_index.fetch_add(1, Ordering::SeqCst) as usize;
        let spec = self.peer.spec["ws"][index].clone();
        Box::pin(async move {
            let raw = spec["events"].as_array().cloned().unwrap_or_else(|| {
                if spec["error"] == true || spec["hold"] == true {
                    vec![]
                } else {
                    full(&format!("r{}", index + 1))
                }
            });
            let mut events: VecDeque<_> = raw
                .into_iter()
                .map(|e| {
                    Ok(WebSocketEvent::Message(WebSocketMessage::Text(
                        e.as_str()
                            .map(str::to_owned)
                            .unwrap_or_else(|| e.to_string()),
                    )))
                })
                .collect();
            if spec["error"] == true {
                events.push_back(Err(TransportError::new(TransportErrorKind::Connection)));
            }
            let events = Arc::new(Mutex::new(events));
            self.current = Some(events.clone());
            self.peer
                .wire
                .lock()
                .unwrap()
                .exchanges
                .push(events.clone());
            Ok(Box::new(Exchange {
                events,
                hold: spec["hold"] == true,
            }) as Box<dyn WebSocketExchange>)
        })
    }
    fn shutdown<'a>(
        &'a mut self,
        code: u16,
        reason: String,
        _: &'a RequestRuntime,
        _: Duration,
    ) -> BoxFuture<'a, Result<(), TransportError>> {
        if let Some(events) = &self.current {
            events.lock().unwrap().push_back(Ok(WebSocketEvent::Close {
                code,
                reason: reason.clone(),
            }));
        }
        self.peer.wire.lock().unwrap().closes.push((code, reason));
        if self.peer.spec["shutdownHold"] == true {
            return Box::pin(std::future::pending());
        }
        self.closed.store(true, Ordering::SeqCst);
        Box::pin(async { Ok(()) })
    }
    fn abort(&mut self) {
        self.closed.store(true, Ordering::SeqCst);
    }
}
fn base64(bytes: &[u8]) -> String {
    const ABC: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let n = (u32::from(chunk[0]) << 16)
            | (u32::from(*chunk.get(1).unwrap_or(&0)) << 8)
            | u32::from(*chunk.get(2).unwrap_or(&0));
        out.push(ABC[((n >> 18) & 63) as usize] as char);
        out.push(ABC[((n >> 12) & 63) as usize] as char);
        out.push(if chunk.len() > 1 {
            ABC[((n >> 6) & 63) as usize] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            ABC[(n & 63) as usize] as char
        } else {
            '='
        });
    }
    out
}
fn request(case: &Value, rt: RequestRuntime) -> TextStreamRequest {
    let model: Model = serde_json::from_value(case["model"].clone()).unwrap();
    let mut auth = ResolvedAuth::default();
    if case["noKey"] != true {
        let claim = case.get("account").cloned().unwrap_or(json!("account-a"));
        let mut payload =
            json!({"https://api.openai.com/auth":{"chatgpt_account_id":claim}}).to_string();
        if case["escapeAccount"] == true {
            payload = payload
                .chars()
                .map(|c| {
                    if c as u32 >= 127 {
                        let mut buf = [0; 2];
                        c.encode_utf16(&mut buf)
                            .iter()
                            .map(|u| format!("\\u{u:04x}"))
                            .collect::<String>()
                    } else {
                        c.to_string()
                    }
                })
                .collect();
        }
        auth.api_key = Some(SecretString::new(format!(
            "aaa.{}.bbb",
            base64(payload.as_bytes())
        )));
    }
    let options = case["options"].as_object().cloned().unwrap_or_default();
    if let Some(headers) = case["options"].get("headers") {
        auth.headers = serde_json::from_value(headers.clone()).unwrap();
    }
    let allowed = [
        "transport",
        "sessionId",
        "cacheRetention",
        "timeoutMs",
        "maxRetries",
        "maxRetryDelayMs",
        "maxTokens",
        "temperature",
        "metadata",
        "samplingParams",
        "websocketConnectTimeoutMs",
    ];
    let wire = serde_json::from_value(Value::Object(
        options
            .into_iter()
            .filter(|(k, _)| allowed.contains(&k.as_str()))
            .collect(),
    ))
    .unwrap();
    TextStreamRequest {
        resolved: ResolvedRequest {
            endpoint: RequestEndpoint::new_resolved(case["model"]["baseUrl"].as_str().unwrap())
                .unwrap(),
            model,
        },
        context: serde_json::from_value(case["context"].clone()).unwrap(),
        auth,
        options: StreamOptions { wire, runtime: rt },
    }
}
struct Payload {
    replacement: Option<Value>,
    error: bool,
}
impl pi_ai_providers::api::runtime::PayloadMiddleware for Payload {
    fn on_payload<'a>(
        &'a self,
        _: &'a TextStreamRequest,
        _: &'a mut pi_ai_providers::api::wire_json::WirePayload,
    ) -> BoxFuture<'a, Result<Option<pi_ai_providers::api::wire_json::WirePayload>, ModelsError>>
    {
        Box::pin(async move {
            if self.error {
                Err(ModelsError::new(ModelsErrorCode::Provider))
            } else {
                Ok(self
                    .replacement
                    .clone()
                    .map(pi_ai_providers::api::wire_json::WirePayload::new))
            }
        })
    }
}
struct Hook(AtomicBool);
impl pi_ai_providers::api::runtime::ResponseMiddleware for Hook {
    fn on_response<'a>(
        &'a self,
        _: &'a TextStreamRequest,
        _: &'a pi_ai_providers::api::runtime::AdapterResponseHead,
    ) -> BoxFuture<'a, Result<(), ModelsError>> {
        Box::pin(async move {
            if !self.0.swap(true, Ordering::SeqCst) {
                Err(ModelsError::new(ModelsErrorCode::Provider))
            } else {
                Ok(())
            }
        })
    }
}
fn normalized(mut v: Value) -> Value {
    fn walk(v: &mut Value) {
        match v {
            Value::Object(map) => {
                for key in ["errorMessage", "stack"] {
                    if map.contains_key(key) {
                        map.insert(key.into(), json!("<redacted>"));
                    }
                }
                if let Some(error) = map.get_mut("error").and_then(Value::as_object_mut)
                    && error.contains_key("message")
                {
                    error.insert("message".into(), json!("<redacted>"));
                    error.remove("stack");
                }
                for value in map.values_mut() {
                    walk(value);
                }
            }
            Value::Array(values) => {
                for value in values {
                    walk(value);
                }
            }
            _ => {}
        }
    }
    walk(&mut v);
    serde_json::from_str(&pi_ai_providers::api::wire_json::stringify_wire_json(&v).unwrap())
        .unwrap()
}
#[test]
fn pinned_complete_codex_requests_events_retries_and_error_boundaries() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../conformance/provider-traces/openai-codex-responses/trace.json"
    ))
    .unwrap();
    for case in fixture["cases"].as_array().unwrap() {
        let clock = Arc::new(Clock::default());
        let spawner = Arc::new(Spawner::default());
        let rt = RequestRuntime::with_noop_telemetry(
            CancellationToken::new(),
            clock.clone(),
            spawner.clone(),
        );
        let wire = Arc::new(Mutex::new(Wire::default()));
        let peer = Arc::new(Peer {
            spec: case["config"].clone(),
            wire: wire.clone(),
            http_index: AtomicU64::new(0),
            ws_index: AtomicU64::new(0),
            connect_index: AtomicU64::new(0),
        });
        let transport = Arc::new(
            HttpTransport::new_custom(peer.clone(), Arc::new(Jitter))
                .with_fetch_response_source(FetchResponseSource::Preconstructed),
        );
        let mut services = AdapterServices::new(transport);
        services.payload = Some(Arc::new(Payload {
            replacement: case.get("replacement").cloned(),
            error: case["payloadError"].is_string(),
        }));
        if case["responseError"].is_string() {
            services.response = Some(Arc::new(Hook(AtomicBool::new(false))));
        }
        let api = OpenAICodexResponsesApi::new(CodexServices {
            adapter: services,
            websocket: if case["config"]["noWebSocket"] == true {
                None
            } else {
                Some(Arc::new(Connector(peer)))
            },
            request_ids: Arc::new(UuidV7Generator::new(clock.clone(), Arc::new(Entropy))),
            date_parser: Arc::new(HttpRetryDateParser),
            signal_presence: CodexSourceSignalPresence::Present,
            compression: if case["config"]["compression"] == "missing" {
                None
            } else {
                Some(ZstdRequestLimits {
                    max_input_bytes: 1_048_576,
                    max_output_bytes: if case["config"]["compression"] == "fail" {
                        0
                    } else {
                        1_048_576
                    },
                })
            },
            limits: Default::default(),
        })
        .unwrap();
        let request = request(case, rt);
        let fields = case["options"]
            .as_object()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter(|(k, _)| {
                [
                    "reasoningEffort",
                    "reasoningSummary",
                    "serviceTier",
                    "textVerbosity",
                    "toolChoice",
                ]
                .contains(&k.as_str())
            })
            .collect();
        let mut stream = if case["simple"] == true {
            api.stream_simple(SimpleStreamRequest {
                resolved: request.resolved,
                context: request.context,
                auth: request.auth,
                options: SimpleStreamOptions {
                    wire: serde_json::from_value(case["options"].clone()).unwrap_or_default(),
                    runtime: request.options.runtime,
                },
            })
        } else {
            api.stream_with_options(
                request,
                serde_json::from_value(Value::Object(fields)).unwrap(),
            )
        };
        let mut result = None;
        for _ in 0..24 {
            spawner.drive();
            if let Poll::Ready(r) = poll(Box::pin(stream.result()).as_mut()) {
                result = Some(r);
                break;
            }
            clock.advance();
        }
        let result = result.unwrap_or_else(|| panic!("{} did not settle", case["id"]));
        let mut events = vec![];
        loop {
            match poll(Box::pin(stream.next()).as_mut()) {
                Poll::Ready(Some(e)) => events.push(serde_json::to_value(e).unwrap()),
                Poll::Ready(None) => break,
                Poll::Pending => panic!("settled stream pending"),
            }
        }
        if case["synchronous"] == true {
            assert_eq!(result.stop_reason, StopReason::Error);
        } else {
            assert_eq!(
                normalized(serde_json::to_value(&result).unwrap()),
                normalized(case["result"].clone()),
                "{} result",
                case["id"]
            );
            assert_eq!(
                normalized(json!(events)),
                normalized(case["events"].clone()),
                "{} events",
                case["id"]
            );
        }
        if case["synchronous"] != true {
            assert_eq!(
                json!({"retryable":is_retryable_assistant_error(&result),"overflow":is_context_overflow(&result,None)}),
                case["flags"],
                "{} classifier flags",
                case["id"]
            );
        }
        let source_sleeps = case["trace"]["sleeps"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| {
                let ms = v.as_f64().unwrap();
                if (1.0..=2_147_483_647.0).contains(&ms) {
                    ms.floor() as u64
                } else {
                    1
                }
            })
            .collect::<Vec<_>>();
        assert_eq!(
            *clock.sleeps.lock().unwrap(),
            source_sleeps,
            "{} selected sleep stages",
            case["id"]
        );
        let observed = wire.lock().unwrap();
        assert_eq!(
            observed.fetch_body_kinds,
            case["trace"]["requests"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| if v["compressed"] == true {
                    "bytes"
                } else {
                    "text"
                })
                .collect::<Vec<_>>(),
            "{} actual Fetch input forms",
            case["id"]
        );
        assert_eq!(
            observed.http.len(),
            case["trace"]["requests"].as_array().unwrap().len(),
            "{} HTTP attempts",
            case["id"]
        );
        assert_eq!(
            observed.sent,
            case["trace"]["sent"]
                .as_array()
                .unwrap()
                .iter()
                .map(|r| r["body"].clone())
                .collect::<Vec<_>>(),
            "{} WS bodies",
            case["id"]
        );
        for ((url, headers, bytes), expected) in observed
            .http
            .iter()
            .zip(case["trace"]["requests"].as_array().unwrap())
        {
            assert_eq!(url, expected["url"].as_str().unwrap());
            let mut header_value = serde_json::Map::new();
            for (name, _) in headers.iter_for_wire() {
                header_value.insert(
                    name.to_ascii_lowercase(),
                    json!(headers.get_for_fetch(name).unwrap()),
                );
            }
            header_value.insert("authorization".into(), json!("Bearer <synthetic>"));
            assert_eq!(
                Value::Object(header_value),
                expected["headers"],
                "{} headers",
                case["id"]
            );
            assert_eq!(
                base64(bytes),
                expected["wire"].as_str().unwrap(),
                "{} encoded request",
                case["id"]
            );
            assert_eq!(
                headers.get_for_wire("content-encoding"),
                expected["headers"]["content-encoding"].as_str()
            );
        }
        drop(observed);
        api.abort_websocket_sessions(None);
        drop(std::mem::take(&mut *spawner.0.lock().unwrap()));
    }
}
struct Harness {
    api: OpenAICodexResponsesApi,
    clock: Arc<Clock>,
    spawner: Arc<Spawner>,
    wire: Arc<Mutex<Wire>>,
}
impl Harness {
    fn new(spec: Value) -> Self {
        Self::with_limits(spec, CodexResourceLimits::default())
    }
    fn with_limits(spec: Value, limits: CodexResourceLimits) -> Self {
        let clock = Arc::new(Clock::default());
        let spawner = Arc::new(Spawner::default());
        let wire = Arc::new(Mutex::new(Wire::default()));
        let peer = Arc::new(Peer {
            spec,
            wire: wire.clone(),
            http_index: AtomicU64::new(0),
            ws_index: AtomicU64::new(0),
            connect_index: AtomicU64::new(0),
        });
        let mut services = AdapterServices::new(Arc::new(
            HttpTransport::new_custom(peer.clone(), Arc::new(Jitter))
                .with_fetch_response_source(FetchResponseSource::Preconstructed),
        ));
        services.payload = Some(Arc::new(CapturePayload(wire.clone())));
        let api = OpenAICodexResponsesApi::new(CodexServices {
            adapter: services,
            websocket: Some(Arc::new(Connector(peer))),
            request_ids: Arc::new(UuidV7Generator::new(clock.clone(), Arc::new(Entropy))),
            date_parser: Arc::new(HttpRetryDateParser),
            signal_presence: CodexSourceSignalPresence::Present,
            compression: Some(ZstdRequestLimits {
                max_input_bytes: 1_048_576,
                max_output_bytes: 1_048_576,
            }),
            limits,
        })
        .unwrap();
        Self {
            api,
            clock,
            spawner,
            wire,
        }
    }
    fn runtime(&self) -> RequestRuntime {
        RequestRuntime::with_noop_telemetry(
            CancellationToken::new(),
            self.clock.clone(),
            self.spawner.clone(),
        )
    }
    fn finish(&self, stream: &mut AssistantStream) -> AssistantMessage {
        for _ in 0..24 {
            self.spawner.drive();
            if let Poll::Ready(result) = poll(Box::pin(stream.result()).as_mut()) {
                return result;
            }
            self.clock.advance();
        }
        panic!("bounded completion")
    }
    fn run(&self, case: &Value) -> AssistantMessage {
        let mut stream = self.api.stream(request(case, self.runtime()));
        self.finish(&mut stream)
    }
    fn stats(&self) -> Value {
        self.api.get_openai_codex_websocket_debug_stats("session").map_or(Value::Null,|s|{let mut value=json!({"requests":s.requests,"connectionsCreated":s.connections_created,"connectionsReused":s.connections_reused,"cachedContextRequests":s.cached_context_requests,"storeTrueRequests":s.store_true_requests,"fullContextRequests":s.full_context_requests,"deltaRequests":s.delta_requests,"lastInputItems":s.last_input_items,"websocketFailures":s.websocket_failures,"sseFallbacks":s.sse_fallbacks});if let Some(v)=s.last_delta_input_items{value["lastDeltaInputItems"]=json!(v);}
if let Some(v)=s.last_previous_response_id{value["lastPreviousResponseId"]=json!(v);}
if let Some(v)=s.websocket_fallback_active{value["websocketFallbackActive"]=json!(v);}
if s.last_websocket_error.is_some(){value["lastWebSocketError"]=json!("<redacted>");}value})
    }
}
impl Drop for Harness {
    fn drop(&mut self) {
        self.api.abort_websocket_sessions(None);
        drop(std::mem::take(&mut *self.spawner.0.lock().unwrap()));
    }
}
#[test]
fn pinned_multiturn_full_delta_cache_reset_close_age_and_sticky_sequences() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../conformance/provider-traces/openai-codex-responses/cache.json"
    ))
    .unwrap();
    for source in fixture["cases"].as_array().unwrap() {
        let id = source["id"].as_str().unwrap();
        eprintln!("cache sequence {id}");
        if id == "busy" {
            continue;
        }
        let h = Harness::new(if id == "sticky" {
            json!({"ws":[{"error":true},{},{}]})
        } else {
            source.get("config").cloned().unwrap_or(json!({}))
        });
        let mut first = json!({"model":source["first"]["model"],"context":source["first"]["context"],"options":source["options"]});
        let observed = h.run(&first);
        assert_eq!(
            normalized(json!(observed)),
            normalized(source["first"]["result"].clone()),
            "{id} first"
        );
        match id {
            "closed" => {
                h.wire.lock().unwrap().states[0].store(true, Ordering::SeqCst);
            }
            "reset" => h
                .api
                .reset_openai_codex_websocket_debug_stats(Some("session")),
            "close" => {
                let close = h
                    .api
                    .close_openai_codex_websocket_sessions(Some("session"), h.runtime());
                assert!(matches!(
                    poll(Box::pin(close).as_mut()),
                    Poll::Ready(Ok(()))
                ));
            }
            "age" => {
                h.clock.now.fetch_add(56 * 60 * 1000, Ordering::SeqCst);
            }
            "idle" => {
                h.spawner.drive();
                h.clock.advance();
                h.spawner.drive();
            }
            _ => {}
        }
        first["context"] = source["second"]["context"].clone();
        h.wire.lock().unwrap().replacement = source.get("replacement").cloned();
        if id == "account" {
            first["account"] = json!("account-b");
        }
        let observed = h.run(&first);
        assert_eq!(
            normalized(json!(observed)),
            normalized(source["second"]["result"].clone()),
            "{id} second"
        );
        if id == "sticky" {
            let close = h
                .api
                .close_openai_codex_websocket_sessions(Some("session"), h.runtime());
            assert!(matches!(
                poll(Box::pin(close).as_mut()),
                Poll::Ready(Ok(()))
            ));
            h.api
                .reset_openai_codex_websocket_debug_stats(Some("session"));
            let observed = h.run(&first);
            assert_eq!(
                normalized(json!(observed)),
                normalized(source["third"]["result"].clone())
            );
        }
        let mut expected = source["stats"].clone();
        if expected.get("lastWebSocketError").is_some() {
            expected["lastWebSocketError"] = json!("<redacted>");
        }
        assert_eq!(h.stats(), expected, "{id} stats");
        assert_eq!(
            h.wire.lock().unwrap().sent_raw,
            source["trace"]["sent"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v["json"].as_str().unwrap().to_owned())
                .collect::<Vec<_>>(),
            "{id} exact full/delta wire bytes"
        );
        assert_eq!(
            h.wire.lock().unwrap().sent,
            source["trace"]["sent"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v["body"].clone())
                .collect::<Vec<_>>(),
            "{id} selected bodies"
        );
    }
}
#[test]
fn busy_cache_uses_one_shot_and_original_lease_keeps_its_continuation() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../conformance/provider-traces/openai-codex-responses/cache.json"
    ))
    .unwrap();
    let source = fixture["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["id"] == "busy")
        .unwrap();
    let h = Harness::new(json!({"ws":[{"hold":true},{}]}));
    let case = json!({"model":source["first"]["model"],"context":source["first"]["context"],"options":source["options"]});
    let mut first = h.api.stream(request(&case, h.runtime()));
    h.spawner.drive();
    assert_eq!(h.wire.lock().unwrap().sent.len(), 1);
    let second = h.run(&case);
    assert_eq!(
        normalized(json!(second)),
        normalized(source["second"]["result"].clone())
    );
    let queue = h.wire.lock().unwrap().exchanges[0].clone();
    queue
        .lock()
        .unwrap()
        .extend(full("r1").into_iter().map(|v| {
            Ok(WebSocketEvent::Message(WebSocketMessage::Text(
                v.to_string(),
            )))
        }));
    let first = h.finish(&mut first);
    assert_eq!(
        normalized(json!(first)),
        normalized(source["first"]["result"].clone())
    );
    assert_eq!(h.stats(), source["stats"]);
    assert_eq!(h.wire.lock().unwrap().closes, vec![(1000, "done".into())]);
}
struct MetadataSource(Option<Value>);
impl pi_ai_providers::api::openai_responses_shared::ResponsesEventSource for MetadataSource {
    fn next(
        &mut self,
    ) -> BoxFuture<
        '_,
        Result<Option<Value>, pi_ai_providers::api::openai_responses_shared::ResponsesError>,
    > {
        Box::pin(async move { Ok(self.0.take()) })
    }
}
#[test]
fn end_turn_opt_in_matches_mapper_and_cancellation_snapshot_with_default_off() {
    use pi_ai_providers::api::openai_responses_shared::*;
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../conformance/provider-traces/openai-codex-responses/metadata.json"
    ))
    .unwrap();
    for case in fixture["cases"].as_array().unwrap() {
        let expected = &case["expected"];
        let model:Model=serde_json::from_value(json!({"id":"gpt-5.5","name":"Synthetic Codex","api":"openai-codex-responses","provider":"openai-codex","baseUrl":"https://example.invalid/backend-api","reasoning":true,"input":["text","image"],"cost":{"input":1,"output":2,"cacheRead":3,"cacheWrite":4},"contextWindow":10000,"maxTokens":1000})).unwrap();
        let token = CancellationToken::new();
        let (stream, p) = create_assistant_message_event_stream(AssistantStreamInit {
            api: model.api.clone(),
            provider: model.provider.clone(),
            model: model.id.clone(),
            clock: Arc::new(Clock::default()),
            cancellation: token.clone(),
        });
        let mut output: AssistantMessage = serde_json::from_value(expected.clone()).unwrap();
        output.stop_reason = StopReason::Pending;
        output.usage = Usage::zero();
        output.response_id = None;
        output.raw_stop_reason = None;
        output.end_turn = case["initial"].as_bool();
        let mut source = MetadataSource(Some(case["raw"].clone()));
        let options = OpenAIResponsesStreamOptions {
            preserve_end_turn: case["codex"].as_bool().unwrap(),
            ..Default::default()
        };
        assert!(matches!(
            poll(
                Box::pin(process_responses_stream(
                    &mut source,
                    &mut output,
                    &p,
                    &model,
                    &options,
                    &token
                ))
                .as_mut()
            ),
            Poll::Ready(Ok(()))
        ));
        assert_eq!(
            normalized(json!(output)),
            normalized(expected.clone()),
            "{}",
            case["id"]
        );
        token.cancel();
        let Poll::Ready(cancelled) = poll(Box::pin(stream.result()).as_mut()) else {
            panic!("cancelled")
        };
        assert_eq!(cancelled.end_turn, expected["endTurn"].as_bool());
        assert_eq!(cancelled.usage.input.get(), 17);
    }
}

struct CapturePayload(Arc<Mutex<Wire>>);
impl pi_ai_providers::api::runtime::PayloadMiddleware for CapturePayload {
    fn on_payload<'a>(
        &'a self,
        _: &'a TextStreamRequest,
        _: &'a mut pi_ai_providers::api::wire_json::WirePayload,
    ) -> BoxFuture<'a, Result<Option<pi_ai_providers::api::wire_json::WirePayload>, ModelsError>>
    {
        Box::pin(async move {
            let mut wire = self.0.lock().unwrap();
            wire.payloads += 1;
            Ok(wire
                .replacement
                .clone()
                .map(pi_ai_providers::api::wire_json::WirePayload::new))
        })
    }
}
#[test]
fn finite_json_account_coercion_identity_latin1_and_header_failure_stage_match_source() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../conformance/provider-traces/openai-codex-responses/account.json"
    ))
    .unwrap();
    for source in fixture["cases"].as_array().unwrap() {
        let h = Harness::new(json!({}));
        let turns = if source["id"] == "number-string-distinct" {
            source["output"].as_array().unwrap().clone()
        } else {
            vec![source["first"].clone(), source["second"].clone()]
        };
        for (i, turn) in turns.into_iter().enumerate() {
            let account = if source["id"] == "number-string-distinct" {
                if i == 1 { json!("1") } else { json!(1) }
            } else {
                source["account"].clone()
            };
            let case = json!({"model":turn["model"],"context":turn["context"],"options":source["options"],"account":account,"escapeAccount":source["escapeAccount"]});
            let output = h.run(&case);
            assert_eq!(
                normalized(json!(output)),
                normalized(turn["result"].clone()),
                "{} turn{i}",
                source["id"]
            );
        }
        assert_eq!(h.stats(), source["stats"], "{} stats", source["id"]);
        let wire = h.wire.lock().unwrap();
        assert_eq!(
            wire.payloads,
            source["trace"]["order"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|v| **v == "payload")
                .count(),
            "{} payload stage",
            source["id"]
        );
        assert_eq!(
            wire.connections.len(),
            source["trace"]["connections"].as_array().unwrap().len(),
            "{} connection count",
            source["id"]
        );
        for ((_, headers), expected) in wire
            .connections
            .iter()
            .zip(source["trace"]["connections"].as_array().unwrap())
        {
            assert_eq!(
                headers.get_for_fetch("chatgpt-account-id").as_deref(),
                expected["headers"]["chatgpt-account-id"].as_str(),
                "{} header",
                source["id"]
            );
        }
    }
}

impl Drop for Socket {
    fn drop(&mut self) {
        self.closed.store(true, Ordering::SeqCst);
    }
}
fn cache_case() -> Value {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../conformance/provider-traces/openai-codex-responses/cache.json"
    ))
    .unwrap();
    let source = &fixture["cases"][1];
    json!({"model":source["first"]["model"],"context":source["first"]["context"],"options":source["options"]})
}
#[test]
fn resource_bounds_retire_failed_leases_without_reuse_or_fallback() {
    for limit in ["sessions", "stats", "continuation"] {
        let mut limits = CodexResourceLimits::default();
        match limit {
            "sessions" => limits.max_sessions = 1,
            "stats" => limits.max_stats = 1,
            _ => limits.max_continuation_bytes = 1,
        };
        let h = Harness::with_limits(json!({}), limits);
        let mut case = cache_case();
        let first = h.run(&case);
        if limit == "continuation" {
            assert_eq!(first.stop_reason, StopReason::Error);
            let second = h.run(&case);
            assert_eq!(second.stop_reason, StopReason::Error);
            assert_eq!(h.wire.lock().unwrap().connections.len(), 2);
            assert!(
                h.wire
                    .lock()
                    .unwrap()
                    .states
                    .iter()
                    .all(|s| s.load(Ordering::SeqCst))
            );
        } else {
            assert_eq!(first.stop_reason, StopReason::Stop);
            case["options"]["sessionId"] = json!("second");
            assert_eq!(h.run(&case).stop_reason, StopReason::Error);
            assert_eq!(h.wire.lock().unwrap().sent.len(), 1);
            assert!(h.wire.lock().unwrap().states[1].load(Ordering::SeqCst));
        }
        assert!(h.wire.lock().unwrap().http.is_empty());
    }
    let h = Harness::with_limits(
        json!({"ws":[{"hold":true}]}),
        CodexResourceLimits {
            max_active_leases: 1,
            ..Default::default()
        },
    );
    let case = cache_case();
    let mut first = h.api.stream(request(&case, h.runtime()));
    h.spawner.drive();
    assert_eq!(h.run(&case).stop_reason, StopReason::Error);
    assert_eq!(h.wire.lock().unwrap().connections.len(), 1);
    h.api.abort_websocket_sessions(None);
    assert_eq!(h.finish(&mut first).stop_reason, StopReason::Stop); // Pre-output source fallback remains permitted after explicit emergency cleanup.
}
#[test]
fn dropped_graceful_completion_aborts_and_explicit_busy_close_flushes_without_reset() {
    let h = Harness::new(json!({}));
    let case = cache_case();
    assert_eq!(h.run(&case).stop_reason, StopReason::Stop);
    let close = h
        .api
        .close_openai_codex_websocket_sessions(Some("session"), h.runtime());
    drop(close);
    assert!(h.wire.lock().unwrap().states[0].load(Ordering::SeqCst));
    assert!(h.wire.lock().unwrap().closes.is_empty());
    assert_eq!(h.stats()["requests"], 1);
    assert_eq!(h.run(&case).stop_reason, StopReason::Stop);
    assert_eq!(h.stats()["connectionsReused"], 0);
    let h = Harness::new(json!({"ws":[{"events":[{"type":"codex.rate_limits"}],"hold":true}]}));
    let mut stream = h.api.stream(request(&case, h.runtime()));
    h.spawner.drive();
    let close = h
        .api
        .close_openai_codex_websocket_sessions(None, h.runtime());
    assert!(matches!(
        poll(Box::pin(close).as_mut()),
        Poll::Ready(Ok(()))
    ));
    let result = h.finish(&mut stream);
    assert_eq!(result.stop_reason, StopReason::Error);
    assert_eq!(
        h.wire.lock().unwrap().closes,
        vec![(1000, "debug_close".into())]
    );
    assert!(h.wire.lock().unwrap().http.is_empty());
    assert_eq!(h.stats()["requests"], 1);
}
#[test]
fn last_public_adapter_owner_drop_aborts_active_socket_despite_detached_worker() {
    let h = Harness::new(json!({"ws":[{"hold":true}]}));
    let api = h.api.clone();
    let rt = h.runtime();
    let spawner = h.spawner.clone();
    let wire = h.wire.clone();
    drop(h);
    let mut stream = api.stream(request(&cache_case(), rt));
    spawner.drive();
    assert!(!wire.lock().unwrap().states[0].load(Ordering::SeqCst));
    drop(api);
    spawner.drive();
    let Poll::Ready(result) = poll(Box::pin(stream.result()).as_mut()) else {
        panic!("owner drop must settle")
    };
    assert_eq!(result.stop_reason, StopReason::Error);
    assert!(wire.lock().unwrap().states[0].load(Ordering::SeqCst));
    assert!(wire.lock().unwrap().http.is_empty());
    assert!(matches!(
        poll(Box::pin(stream.next()).as_mut()),
        Poll::Ready(Some(AssistantMessageEvent::Error { .. }))
    ));
}

#[test]
fn graceful_close_bounds_noncooperative_shutdown_and_emergency_abort_drops_owned_socket() {
    for abort in [false, true] {
        let h = Harness::new(json!({"shutdownHold":true}));
        assert_eq!(h.run(&cache_case()).stop_reason, StopReason::Stop);
        let mut close = h
            .api
            .close_openai_codex_websocket_sessions(None, h.runtime());
        assert!(poll(close.as_mut()).is_pending());
        assert!(!h.wire.lock().unwrap().states[0].load(Ordering::SeqCst));
        if abort {
            h.api.abort_websocket_sessions(None);
        } else {
            h.clock.advance();
        }
        let Poll::Ready(Err(error)) = poll(close.as_mut()) else {
            panic!("close must retire")
        };
        assert_eq!(
            error.kind(),
            if abort {
                TransportErrorKind::Cancelled
            } else {
                TransportErrorKind::Timeout
            }
        );
        drop(close);
        assert!(h.wire.lock().unwrap().states[0].load(Ordering::SeqCst));
        assert_eq!(h.stats()["requests"], 1);
    }
}

#[test]
fn finite_payload_spread_keeps_scalar_array_utf16_wire_and_cached_null_failure_custody() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../conformance/provider-traces/openai-codex-responses/payload.json"
    ))
    .unwrap();
    for source in fixture["cases"].as_array().unwrap() {
        let h = Harness::new(source["config"].clone());
        h.wire.lock().unwrap().replacement = Some(source["replacement"].clone());
        let case = json!({"model":source["first"]["model"],"context":source["first"]["context"],"options":source["options"]});
        for stage in ["first", "second"] {
            let output = h.run(&case);
            assert_eq!(
                normalized(json!(output)),
                normalized(source[stage]["result"].clone()),
                "{} {stage}",
                source["id"]
            );
        }
        let mut stats = source["stats"].clone();
        if stats.get("lastWebSocketError").is_some() {
            stats["lastWebSocketError"] = json!("<redacted>");
        }
        assert_eq!(h.stats(), stats, "{} stats", source["id"]);
        assert_eq!(
            h.wire.lock().unwrap().sent_raw,
            source["trace"]["sent"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v["json"].as_str().unwrap().to_owned())
                .collect::<Vec<_>>(),
            "{} raw wire",
            source["id"]
        );
    }
}

#[test]
fn zero_close_grace_expires_before_polling_transport_shutdown() {
    let h = Harness::with_limits(
        json!({}),
        CodexResourceLimits {
            close_grace: Duration::ZERO,
            ..Default::default()
        },
    );
    assert_eq!(h.run(&cache_case()).stop_reason, StopReason::Stop);
    let mut close = h
        .api
        .close_openai_codex_websocket_sessions(None, h.runtime());
    let Poll::Ready(Err(error)) = poll(close.as_mut()) else {
        panic!("zero grace must expire immediately");
    };
    assert_eq!(error.kind(), TransportErrorKind::Timeout);
    assert!(h.wire.lock().unwrap().closes.is_empty());
    assert!(h.wire.lock().unwrap().states[0].load(Ordering::SeqCst));
}

#[test]
fn retained_unpolled_idle_tasks_share_the_explicit_resource_budget() {
    let h = Harness::with_limits(
        json!({}),
        CodexResourceLimits {
            max_active_leases: 2,
            ..Default::default()
        },
    );
    let case = cache_case();
    let first = h.api.stream(request(&case, h.runtime()));
    let mut worker = h.spawner.0.lock().unwrap().pop().unwrap();
    assert!(poll(worker.as_mut()).is_ready());
    assert!(
        matches!(poll(Box::pin(first.result()).as_mut()), Poll::Ready(message) if message.stop_reason==StopReason::Stop)
    );
    // The host deliberately retains the first unpolled idle task. Cancellation
    // must not let an unbounded sequence allocate more retained timer owners.
    let second = h.api.stream(request(&case, h.runtime()));
    let mut worker = h.spawner.0.lock().unwrap().pop().unwrap();
    assert!(poll(worker.as_mut()).is_ready());
    assert!(
        matches!(poll(Box::pin(second.result()).as_mut()), Poll::Ready(message) if message.stop_reason==StopReason::Error)
    );
    assert_eq!(h.spawner.0.lock().unwrap().len(), 1);
    assert!(h.wire.lock().unwrap().http.is_empty());
    assert!(h.wire.lock().unwrap().states[0].load(Ordering::SeqCst));
}

#[test]
fn review_first_completion_start_matches_mapper_stage_and_http_control() {
    let source: Value = serde_json::from_str(include_str!(
        "../../../conformance/provider-traces/openai-codex-responses/review-correction.json"
    ))
    .unwrap();
    assert_eq!(source["cases"].as_array().unwrap().len(), 33);
    for case in source["cases"].as_array().unwrap() {
        let h = Harness::new(case["config"].clone());
        let mut stream = h.api.stream(request(case, h.runtime()));
        let result = h.finish(&mut stream);
        let mut events = vec![];
        while let Poll::Ready(Some(event)) = poll(Box::pin(stream.next()).as_mut()) {
            events.push(json!(event));
        }
        assert_eq!(
            normalized(json!(events)),
            normalized(case["events"].clone()),
            "{} complete event snapshots",
            case["id"]
        );
        assert_eq!(
            normalized(json!(result)),
            normalized(case["result"].clone()),
            "{} final result",
            case["id"]
        );
    }
}

#[test]
fn review_retained_close_futures_preserve_slot_budget_and_drop_recovers_capacity() {
    for selected in [false, true] {
        let h = Harness::with_limits(
            json!({}),
            CodexResourceLimits {
                max_sessions: 1,
                max_active_leases: 2,
                ..Default::default()
            },
        );
        let case = cache_case();
        let mut held = vec![];
        let mut rejected = 0;
        for _ in 0..8 {
            match h.run(&case).stop_reason {
                StopReason::Stop => held.push(h.api.close_openai_codex_websocket_sessions(
                    selected.then_some("session"),
                    h.runtime(),
                )),
                StopReason::Error => rejected += 1,
                other => panic!("unexpected outcome {other:?}"),
            }
            h.spawner.drive(); // Reap cancelled idle work without polling retained closes.
            let live = h
                .wire
                .lock()
                .unwrap()
                .states
                .iter()
                .filter(|s| !s.load(Ordering::SeqCst))
                .count();
            assert!(live <= 2, "retained close slots exceed limit2: {live}");
        }
        assert!(!held.is_empty());
        assert!(rejected > 0);
        assert!(h.wire.lock().unwrap().http.is_empty());
        drop(held);
        h.spawner.drive();
        assert!(
            h.wire
                .lock()
                .unwrap()
                .states
                .iter()
                .all(|s| s.load(Ordering::SeqCst))
        );
        assert_eq!(
            h.run(&case).stop_reason,
            StopReason::Stop,
            "drop restores capacity"
        );
    }
}

#[test]
fn review_all_session_close_charges_each_slot_and_emergency_abort_keeps_retained_charge() {
    let h = Harness::with_limits(
        json!({}),
        CodexResourceLimits {
            max_sessions: 2,
            max_active_leases: 4,
            ..Default::default()
        },
    );
    let a = cache_case();
    let mut b = a.clone();
    b["options"]["sessionId"] = json!("second");
    assert_eq!(h.run(&a).stop_reason, StopReason::Stop);
    assert_eq!(h.run(&b).stop_reason, StopReason::Stop);
    let group = h
        .api
        .close_openai_codex_websocket_sessions(None, h.runtime());
    h.spawner.drive();
    assert_eq!(h.run(&a).stop_reason, StopReason::Stop);
    let single = h
        .api
        .close_openai_codex_websocket_sessions(Some("session"), h.runtime());
    h.spawner.drive();
    assert_eq!(
        h.run(&a).stop_reason,
        StopReason::Error,
        "grouping two slots must retain both charges"
    );
    h.api.abort_websocket_sessions(None);
    h.spawner.drive();
    assert!(
        h.wire
            .lock()
            .unwrap()
            .states
            .iter()
            .all(|s| s.load(Ordering::SeqCst))
    );
    assert_eq!(
        h.run(&a).stop_reason,
        StopReason::Error,
        "unpolled owners retain their charge after emergency IO abort"
    );
    drop(group);
    drop(single);
    h.spawner.drive();
    assert_eq!(h.run(&a).stop_reason, StopReason::Stop);
    assert!(h.wire.lock().unwrap().http.is_empty());
}

#[test]
fn review_busy_and_noncooperative_close_transfer_charges_until_release() {
    let h = Harness::with_limits(
        json!({"ws":[{"events":[{"type":"codex.rate_limits"}],"hold":true}]}),
        CodexResourceLimits {
            max_active_leases: 2,
            ..Default::default()
        },
    );
    let case = cache_case();
    let mut busy = h.api.stream(request(&case, h.runtime()));
    h.spawner.drive();
    let held = h
        .api
        .close_openai_codex_websocket_sessions(Some("session"), h.runtime());
    assert_eq!(h.run(&case).stop_reason, StopReason::Error);
    assert!(h.wire.lock().unwrap().http.is_empty());
    drop(held);
    assert_eq!(h.finish(&mut busy).stop_reason, StopReason::Error);
    h.api
        .reset_openai_codex_websocket_debug_stats(Some("session"));
    assert_eq!(h.run(&case).stop_reason, StopReason::Stop);
    for zero in [false, true] {
        let h = Harness::with_limits(
            json!({"shutdownHold":true}),
            CodexResourceLimits {
                max_active_leases: 2,
                close_grace: if zero {
                    Duration::ZERO
                } else {
                    Duration::from_secs(2)
                },
                ..Default::default()
            },
        );
        assert_eq!(h.run(&case).stop_reason, StopReason::Stop);
        let mut close = h
            .api
            .close_openai_codex_websocket_sessions(None, h.runtime());
        h.spawner.drive();
        if !zero {
            assert!(poll(close.as_mut()).is_pending());
            assert_eq!(h.run(&case).stop_reason, StopReason::Error);
            h.clock.advance();
        }
        let Poll::Ready(Err(error)) = poll(close.as_mut()) else {
            panic!("bounded close expires")
        };
        assert_eq!(error.kind(), TransportErrorKind::Timeout);
        assert_eq!(
            h.run(&case).stop_reason,
            StopReason::Stop,
            "completed retirement restores capacity even before future drop"
        );
    }
}
