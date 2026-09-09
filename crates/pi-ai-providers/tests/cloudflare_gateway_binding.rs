#![cfg(feature = "cloudflare-gateway-binding")]
use pi_ai_core::*;
use pi_ai_providers::api::{cloudflare::*, cloudflare_gateway_binding::*};
use pi_ai_transport::*;
use serde_json::{Value, json};
use std::{
    future::Future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context as Cx, Poll, Waker},
    time::Duration,
};

struct ClockPort;
impl Clock for ClockPort {
    fn now_unix_ms(&self) -> u64 {
        7
    }
    fn sleep<'a>(
        &'a self,
        d: Duration,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), Cancelled>> {
        assert!(
            matches!(d.as_millis(), 60_000 | 600_000),
            "only existing provider timeouts are expected"
        );
        Box::pin(std::future::pending())
    }
}
struct Jitter;
impl RetryJitter for Jitter {
    fn sample(&self) -> f64 {
        panic!("no implicit retry")
    }
}
#[derive(Default)]
struct Spawner(Mutex<Vec<BoxFuture<'static, ()>>>);
impl TaskSpawner for Spawner {
    fn spawn(&self, t: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
        self.0.lock().unwrap().push(t);
        Ok(())
    }
}
fn ready<F: Future>(f: F) -> F::Output {
    match Box::pin(f)
        .as_mut()
        .poll(&mut Cx::from_waker(Waker::noop()))
    {
        Poll::Ready(v) => v,
        Poll::Pending => panic!("unexpected pending"),
    }
}
fn runtime(token: CancellationToken, s: Arc<Spawner>) -> RequestRuntime {
    RequestRuntime::with_noop_telemetry(token, Arc::new(ClockPort), s)
}
#[derive(Default)]
struct Observed {
    order: Mutex<Vec<&'static str>>,
    gateways: Mutex<Vec<String>>,
    data: Mutex<Vec<Capture>>,
    closes: AtomicUsize,
    drops: AtomicUsize,
    future_drops: AtomicUsize,
    gateway_drops: AtomicUsize,
    signals: Mutex<Vec<CancellationToken>>,
}
struct Capture {
    provider: String,
    endpoint: String,
    headers: Vec<(String, String)>,
    query: Value,
}
struct Binding {
    seen: Arc<Observed>,
    bytes: Vec<u8>,
    status: u16,
    block_run: bool,
    block_body: bool,
}
struct Gateway {
    seen: Arc<Observed>,
    bytes: Vec<u8>,
    status: u16,
    block_run: bool,
    block_body: bool,
}
impl Drop for Gateway {
    fn drop(&mut self) {
        self.seen.gateway_drops.fetch_add(1, Ordering::SeqCst);
    }
}
impl AiGatewayBinding for Binding {
    fn gateway(&self, id: &str) -> Result<Box<dyn AiGatewayBindingGateway>, TransportError> {
        self.seen.order.lock().unwrap().push("gateway");
        self.seen.gateways.lock().unwrap().push(id.to_owned());
        Ok(Box::new(Gateway {
            seen: self.seen.clone(),
            bytes: self.bytes.clone(),
            status: self.status,
            block_run: self.block_run,
            block_body: self.block_body,
        }))
    }
}
struct Guard(Arc<Observed>);
impl Drop for Guard {
    fn drop(&mut self) {
        self.0.future_drops.fetch_add(1, Ordering::SeqCst);
    }
}
impl AiGatewayBindingGateway for Gateway {
    fn run<'a>(
        &'a self,
        d: &'a AiGatewayUniversalRequestLike,
        r: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<HttpResponse, TransportError>> {
        self.seen.order.lock().unwrap().push("run");
        self.seen.data.lock().unwrap().push(Capture {
            provider: d.provider.clone(),
            endpoint: d.endpoint.clone(),
            headers: d
                .headers
                .iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
            query: d.query.as_value().clone(),
        });
        self.seen
            .signals
            .lock()
            .unwrap()
            .push(r.cancellation.clone());
        let guard = Guard(self.seen.clone());
        Box::pin(async move {
            let _guard = guard;
            if self.block_run {
                return std::future::pending().await;
            }
            let mut headers = HttpHeaders::new();
            for (k, v) in [
                ("content-type", "text/event-stream"),
                ("content-encoding", "gzip"),
                ("location", "https://unused.invalid"),
                ("x-metadata", "kept"),
            ] {
                headers.append(k, v)?;
            }
            Ok(HttpResponse {
                status: self.status,
                status_text: String::new(),
                headers,
                body: Box::new(Body {
                    seen: self.seen.clone(),
                    bytes: Some(self.bytes.clone()),
                    blocked: self.block_body,
                    closed: false,
                }),
            })
        })
    }
}
struct Body {
    seen: Arc<Observed>,
    bytes: Option<Vec<u8>>,
    blocked: bool,
    closed: bool,
}
impl ResponseBody for Body {
    fn next<'a>(
        &'a mut self,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, TransportError>> {
        Box::pin(async move {
            if self.blocked {
                std::future::pending().await
            } else {
                Ok(self.bytes.take())
            }
        })
    }
    fn close(&mut self) {
        if !self.closed {
            self.closed = true;
            self.bytes = None;
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
fn transport(
    seen: Arc<Observed>,
    base: &str,
    bytes: Vec<u8>,
    status: u16,
    block_run: bool,
    block_body: bool,
) -> Result<HttpTransport, TransportError> {
    transport_named(
        seen,
        base,
        bytes,
        status,
        block_run,
        block_body,
        "configured-name",
    )
}
fn transport_named(
    seen: Arc<Observed>,
    base: &str,
    bytes: Vec<u8>,
    status: u16,
    block_run: bool,
    block_body: bool,
    gateway: &str,
) -> Result<HttpTransport, TransportError> {
    let fetch = create_gateway_binding_fetch(GatewayBindingFetchOptions {
        binding: Arc::new(Binding {
            seen,
            bytes,
            status,
            block_run,
            block_body,
        }),
        base_url: base.into(),
        gateway: gateway.into(),
    })?;
    assert_eq!(format!("{fetch:?}"), "GatewayBindingFetch([REDACTED])");
    Ok(fetch.into_transport(Arc::new(Jitter)))
}
fn record(pairs: &Value) -> FetchHeaderRecord {
    let mut r = FetchHeaderRecord::new();
    for pair in pairs.as_array().unwrap() {
        r.insert(
            pair[0].as_str().unwrap().into(),
            pair[1].as_str().unwrap().into(),
        );
    }
    r
}
fn assert_json(actual: &Value, expected: &Value) {
    match (actual, expected) {
        (Value::Number(a), Value::Number(b)) => {
            if let (Some(a), Some(b)) = (a.as_u64(), b.as_u64()) {
                assert_eq!(a, b);
            } else if let (Some(a), Some(b)) = (a.as_i64(), b.as_i64()) {
                assert_eq!(a, b);
            } else {
                assert_eq!(a.as_f64(), b.as_f64());
            }
        }
        (Value::Object(a), Value::Object(b)) => {
            assert_eq!(a.keys().collect::<Vec<_>>(), b.keys().collect::<Vec<_>>());
            for (k, v) in a {
                assert_json(v, &b[k]);
            }
        }
        (Value::Array(a), Value::Array(b)) => {
            assert_eq!(a.len(), b.len());
            for (a, b) in a.iter().zip(b) {
                assert_json(a, b);
            }
        }
        _ => assert_eq!(actual, expected),
    }
}
#[test]
fn literals_and_complete_binding_source_matrix_match() {
    let source: Value = serde_json::from_str(include_str!(
        "../../../conformance/provider-traces/cloudflare-gateway-binding/trace.json"
    ))
    .unwrap();
    for (name, value) in [
        (
            "CLOUDFLARE_WORKERS_AI_BASE_URL",
            CLOUDFLARE_WORKERS_AI_BASE_URL,
        ),
        (
            "CLOUDFLARE_AI_GATEWAY_COMPAT_BASE_URL",
            CLOUDFLARE_AI_GATEWAY_COMPAT_BASE_URL,
        ),
        (
            "CLOUDFLARE_AI_GATEWAY_OPENAI_BASE_URL",
            CLOUDFLARE_AI_GATEWAY_OPENAI_BASE_URL,
        ),
        (
            "CLOUDFLARE_AI_GATEWAY_ANTHROPIC_BASE_URL",
            CLOUDFLARE_AI_GATEWAY_ANTHROPIC_BASE_URL,
        ),
    ] {
        assert_eq!(source["constants"][name], value);
    }
    assert_eq!(source["sentinel"], CLOUDFLARE_GATEWAY_BINDING_AUTH_SENTINEL);
    assert_eq!(source["cases"].as_array().unwrap().len(), 50);
    for case in source["cases"].as_array().unwrap() {
        let seen = Arc::new(Observed::default());
        let result = transport(
            seen.clone(),
            case["base"]
                .as_str()
                .unwrap_or(source["base"].as_str().unwrap()),
            b"already-decoded".to_vec(),
            case["responseStatus"].as_u64().unwrap_or(200) as u16,
            false,
            false,
        );
        if case["id"] == "invalid-base" {
            assert!(result.is_err());
            assert!(seen.order.lock().unwrap().is_empty());
            continue;
        }
        let transport = result.unwrap();
        let pairs = case.get("headers").cloned().unwrap_or(json!([]));
        let headers = if case["kind"] == "headers" {
            let mut h = HttpHeaders::new();
            for p in pairs.as_array().unwrap() {
                h.append(p[0].as_str().unwrap(), p[1].as_str().unwrap())
                    .unwrap();
            }
            FetchHeaders::Constructed(h)
        } else {
            FetchHeaders::Record(record(&pairs))
        };
        let body = if let Some(bytes) = case["bytes"].as_array() {
            FetchBody::Bytes(bytes.iter().map(|v| v.as_u64().unwrap() as u8).collect())
        } else {
            FetchBody::Text(if let Some(depth) = case["depth"].as_u64() {
                format!(
                    "{}null{}",
                    "[".repeat(depth as usize),
                    "]".repeat(depth as usize)
                )
            } else if let Some(size) = case["size"].as_u64() {
                format!("\"{}\"", "x".repeat(size as usize))
            } else {
                case["body"].as_str().unwrap_or("{}").into()
            })
        };
        let request = FetchRequest {
            endpoint: RequestEndpoint::new_resolved(
                case["url"].as_str().map(str::to_owned).unwrap_or_else(|| {
                    format!(
                        "{}{}",
                        source["base"].as_str().unwrap(),
                        case["path"].as_str().unwrap_or("/openai/responses")
                    )
                }),
            )
            .unwrap(),
            method: case["method"].as_str().unwrap_or("POST").into(),
            headers,
            body,
            proxy: None,
        };
        let token = CancellationToken::new();
        let rt = runtime(token.clone(), Arc::new(Spawner::default()));
        let response = ready(transport.send_fetch_request(
            &request,
            &rt,
            RetryProfile::None,
            TimeoutProfile::None,
        ));
        assert_eq!(
            response.is_ok(),
            case["accepted"].as_bool().unwrap(),
            "{}",
            case["id"]
        );
        assert_eq!(
            json!(*seen.order.lock().unwrap()),
            case["order"],
            "{} effects",
            case["id"]
        );
        if let Ok(mut response) = response {
            let captured = seen.data.lock().unwrap();
            assert_eq!(captured.len(), 1);
            let d = &captured[0];
            assert_eq!(*seen.gateways.lock().unwrap(), ["configured-name"]);
            assert_eq!(d.provider, case["provider"]);
            assert_eq!(d.endpoint, case["endpoint"]);
            assert_eq!(json!(d.headers), case["headerEntries"]);
            assert_eq!(
                d.query
                    .get("negativeZero")
                    .and_then(Value::as_f64)
                    .is_some_and(|value| value == 0.0 && value.is_sign_negative()),
                case["queryNegativeZero"].as_bool().unwrap()
            );
            assert_eq!(
                json!(
                    d.query
                        .as_object()
                        .map(|v| v.keys().cloned().collect::<Vec<_>>())
                        .unwrap_or_default()
                ),
                case["queryKeys"],
                "{} query own keys",
                case["id"]
            );
            if let Some(depth) = case["depth"].as_u64() {
                let mut v = &d.query;
                for _ in 0..depth {
                    v = &v.as_array().unwrap()[0];
                }
                assert!(v.is_null());
            } else if let Some(size) = case["size"].as_u64() {
                assert_eq!(d.query.as_str().unwrap(), "x".repeat(size as usize));
            } else {
                assert_json(&d.query, &case["query"]);
            }
            assert_eq!(
                response.status,
                case["responseStatus"].as_u64().unwrap_or(200) as u16
            );
            assert_eq!(
                response.headers.get_for_wire("content-encoding"),
                Some("gzip")
            );
            assert_eq!(
                ready(read_body_limited(response.body.as_mut(), &rt, 64)).unwrap(),
                b"already-decoded"
            );
            token.cancel();
            assert!(seen.signals.lock().unwrap()[0].is_cancelled());
            drop(response);
            assert_eq!(seen.closes.load(Ordering::SeqCst), 1);
            assert_eq!(seen.drops.load(Ordering::SeqCst), 1);
        } else {
            assert!(seen.data.lock().unwrap().is_empty());
        }
    }
}

#[test]
fn original_cancellation_pending_drop_and_body_release_have_no_fallback() {
    let base = "https://gateway.ai.cloudflare.com/v1/account/gateway";
    let req = FetchRequest {
        endpoint: RequestEndpoint::new_resolved(format!("{base}/openai/responses")).unwrap(),
        method: "POST".into(),
        headers: FetchHeaders::Record(FetchHeaderRecord::new()),
        body: FetchBody::Text("null".into()),
        proxy: None,
    };
    for pre in [false, true] {
        let seen = Arc::new(Observed::default());
        let t = transport(seen.clone(), base, vec![], 200, true, false).unwrap();
        let token = CancellationToken::new();
        let rt = runtime(token.clone(), Arc::new(Spawner::default()));
        if pre {
            token.cancel();
        }
        let mut send = t.send_fetch_request(&req, &rt, RetryProfile::None, TimeoutProfile::None);
        if !pre {
            assert!(
                send.as_mut()
                    .poll(&mut Cx::from_waker(Waker::noop()))
                    .is_pending()
            );
            token.cancel();
        }
        assert_eq!(
            ready(send).unwrap_err().kind(),
            TransportErrorKind::Cancelled
        );
        assert_eq!(seen.future_drops.load(Ordering::SeqCst), usize::from(!pre));
        assert_eq!(seen.gateway_drops.load(Ordering::SeqCst), usize::from(!pre));
    }
    let seen = Arc::new(Observed::default());
    let t = transport(seen.clone(), base, vec![], 200, true, false).unwrap();
    let rt = runtime(CancellationToken::new(), Arc::new(Spawner::default()));
    let mut send = t.send_fetch_request(&req, &rt, RetryProfile::None, TimeoutProfile::None);
    assert!(
        send.as_mut()
            .poll(&mut Cx::from_waker(Waker::noop()))
            .is_pending()
    );
    drop(send);
    assert_eq!(seen.future_drops.load(Ordering::SeqCst), 1);
    assert_eq!(seen.gateway_drops.load(Ordering::SeqCst), 1);
    let seen = Arc::new(Observed::default());
    let t = transport(seen.clone(), base, vec![], 200, false, true).unwrap();
    let token = CancellationToken::new();
    let rt = runtime(token.clone(), Arc::new(Spawner::default()));
    let mut response =
        ready(t.send_fetch_request(&req, &rt, RetryProfile::None, TimeoutProfile::None)).unwrap();
    let mut read = response.body.next(&rt);
    assert!(
        read.as_mut()
            .poll(&mut Cx::from_waker(Waker::noop()))
            .is_pending()
    );
    token.cancel();
    assert_eq!(
        ready(read).unwrap_err().kind(),
        TransportErrorKind::Cancelled
    );
    assert_eq!(seen.closes.load(Ordering::SeqCst), 1);
    drop(response);
    assert_eq!(seen.drops.load(Ordering::SeqCst), 1);
    let physical = HttpRequest {
        endpoint: RequestEndpoint::new_resolved(format!("{base}/openai/responses")).unwrap(),
        method: "POST".into(),
        headers: HttpHeaders::new(),
        body: b"{}".to_vec(),
        proxy: None,
    };
    let rt = runtime(CancellationToken::new(), Arc::new(Spawner::default()));
    assert_eq!(
        ready(t.send(&physical, &rt, RetryProfile::None, TimeoutProfile::None))
            .unwrap_err()
            .kind(),
        TransportErrorKind::UnsupportedOperation
    );
}

#[cfg(any(feature = "wire-pi-messages", feature = "wire-openai-completions"))]
fn compare_composition(source: Value, pi: bool) {
    for case in source["cases"].as_array().unwrap() {
        let seen = Arc::new(Observed::default());
        let s = Arc::new(Spawner::default());
        let token = CancellationToken::new();
        let bytes = if pi {
            let u = json!({"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"totalTokens":0,"cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"total":0}});
            format!(
                "data: {}\n\n",
                json!({"type":"done","reason":"stop","usage":u})
            )
            .into_bytes()
        } else {
            case["sse"].as_str().unwrap().as_bytes().to_vec()
        };
        let t = transport_named(
            seen.clone(),
            "https://gateway.ai.cloudflare.com/v1/account/gateway",
            bytes,
            200,
            false,
            false,
            "gateway",
        )
        .unwrap();
        let mut services = pi_ai_providers::api::runtime::AdapterServices::new(Arc::new(t));
        if let Some(depth) = case["depth"].as_u64() {
            struct Nested(usize);
            impl pi_ai_providers::api::runtime::PayloadMiddleware for Nested {
                fn on_payload<'a>(
                    &'a self,
                    _: &'a TextStreamRequest,
                    _: &'a mut pi_ai_providers::api::wire_json::WirePayload,
                ) -> BoxFuture<
                    'a,
                    Result<Option<pi_ai_providers::api::wire_json::WirePayload>, ModelsError>,
                > {
                    Box::pin(async move {
                        let value = (0..self.0).fold(Value::Null, |v, _| Value::Array(vec![v]));
                        Ok(Some(pi_ai_providers::api::wire_json::WirePayload::new(
                            value,
                        )))
                    })
                }
            }
            services.payload = Some(Arc::new(Nested(depth as usize)));
        }

        let r = TextStreamRequest {
            resolved: ResolvedRequest {
                model: serde_json::from_value(case["model"].clone()).unwrap(),
                endpoint: RequestEndpoint::new_resolved(case["model"]["baseUrl"].as_str().unwrap())
                    .unwrap(),
            },
            context: serde_json::from_value(case["context"].clone()).unwrap(),
            auth: ResolvedAuth {
                api_key: pi.then(|| SecretString::new("synthetic-key")),
                headers: serde_json::from_value(case["headers"].clone()).unwrap(),
                ..Default::default()
            },
            options: StreamOptions {
                wire: Default::default(),
                runtime: runtime(token, s.clone()),
            },
        };
        let mut stream = if pi {
            #[cfg(feature = "wire-pi-messages")]
            {
                pi_ai_providers::api::pi_messages::PiMessagesApi::new(services).stream(r)
            }
            #[cfg(not(feature = "wire-pi-messages"))]
            {
                panic!("Pi feature required")
            }
        } else {
            #[cfg(feature = "wire-openai-completions")]
            {
                pi_ai_providers::api::openai_completions::OpenAICompletionsApi::new(services)
                    .stream(r)
            }
            #[cfg(not(feature = "wire-openai-completions"))]
            {
                panic!("OpenAI feature required")
            }
        };
        ready(s.0.lock().unwrap().pop().unwrap());
        let result = ready(stream.result());
        let mut events = Vec::new();
        while let Some(e) = ready(stream.next()) {
            events.push(e);
        }
        if case["rejected"] == true {
            assert_eq!(result.stop_reason, StopReason::Error);
            assert_eq!(
                is_retryable_assistant_error(&result),
                case["retryable"].as_bool().unwrap(),
                "custom Fetch rejection classification"
            );
            assert!(seen.order.lock().unwrap().is_empty());
            let mut expected_result: AssistantMessage =
                serde_json::from_value(case["result"].clone()).unwrap();
            let mut actual_result = result.clone();
            expected_result.error_message = Some("typed custom Fetch failure".into());
            actual_result.error_message = Some("typed custom Fetch failure".into());
            let diagnostics = result.diagnostics.as_ref().unwrap();
            assert_eq!(diagnostics.len(), 1);
            assert_eq!(
                diagnostics[0].diagnostic_type.as_str(),
                if pi {
                    "pi_messages_failure"
                } else {
                    "openai_completions_failure"
                }
            );
            assert_eq!(
                json!(diagnostics[0].details),
                if pi {
                    json!({"kind":"transport"})
                } else {
                    json!({"kind":"transport","status":null,"retryable":case["retryable"],"contextOverflow":false})
                }
            );
            assert!(expected_result.diagnostics.is_none());
            actual_result.diagnostics = None;
            assert_eq!(actual_result, expected_result);
            let mut expected_events: Vec<AssistantMessageEvent> =
                serde_json::from_value(case["events"].clone()).unwrap();
            for event in events.iter_mut().chain(expected_events.iter_mut()) {
                if let AssistantMessageEvent::Error { error, .. } = event {
                    error.error_message = Some("typed custom Fetch failure".into());
                    error.diagnostics = None;
                }
            }
            assert_eq!(events, expected_events);
            continue;
        }
        assert_eq!(
            result,
            serde_json::from_value::<AssistantMessage>(case["result"].clone()).unwrap(),
            "{} result",
            case["id"]
        );
        assert_eq!(
            events,
            serde_json::from_value::<Vec<AssistantMessageEvent>>(case["events"].clone()).unwrap(),
            "{} events",
            case["id"]
        );
        let data = seen.data.lock().unwrap();
        assert_eq!(data.len(), 1);
        let expected = &case["calls"][1]["data"];
        assert_eq!(data[0].provider, expected["provider"]);
        assert_eq!(data[0].endpoint, expected["endpoint"]);
        if let Some(depth) = case["depth"].as_u64() {
            let mut v = &data[0].query;
            for _ in 0..depth {
                v = &v.as_array().unwrap()[0];
            }
            assert!(v.is_null());
            assert_eq!(expected["queryDepth"], depth);
        } else {
            assert_json(&data[0].query, &expected["query"]);
        }
        let actual = data[0]
            .headers
            .iter()
            .filter(|(k, _)| {
                !k.starts_with("x-stainless-")
                    || matches!(
                        k.as_str(),
                        "x-stainless-timeout" | "x-stainless-retry-count"
                    )
            })
            .cloned()
            .collect::<Vec<_>>();
        let expected = if pi {
            expected["headers"]
                .as_object()
                .unwrap()
                .iter()
                .map(|(k, v)| (k.clone(), v.as_str().unwrap().to_owned()))
                .collect::<Vec<_>>()
        } else {
            serde_json::from_value(expected["headers"].clone()).unwrap()
        };
        assert_eq!(actual, expected, "{} headers", case["id"]);
        assert_eq!(*seen.order.lock().unwrap(), ["gateway", "run"]);
        assert_eq!(
            json!(*seen.gateways.lock().unwrap()),
            json!([case["calls"][0]["id"]])
        );
        assert_eq!(seen.closes.load(Ordering::SeqCst), 1);
    }
}
#[cfg(feature = "wire-pi-messages")]
#[test]
fn complete_pi_and_binding_composition_preserves_original_alias_and_literal_controls() {
    compare_composition(
        serde_json::from_str(include_str!(
            "../../../conformance/provider-traces/cloudflare-gateway-binding/pi.json"
        ))
        .unwrap(),
        true,
    );
}
#[cfg(feature = "wire-openai-completions")]
#[test]
fn complete_openai_sdk_and_binding_composition_preserves_constructed_headers_and_sentinel_removal()
{
    compare_composition(
        serde_json::from_str(include_str!(
            "../../../conformance/provider-traces/cloudflare-gateway-binding/openai.json"
        ))
        .unwrap(),
        false,
    );
}

#[test]
fn binding_unbounded_depth_feature_does_not_change_other_decoder_defaults() {
    let deep = format!("{}null{}", "[".repeat(129), "]".repeat(129));
    assert!(serde_json::from_str::<Value>(&deep).is_err());
    assert!(serde_json::from_str::<JsonValue>(&deep).is_err());
}

#[cfg(all(
    feature = "wire-anthropic-messages",
    feature = "wire-mistral-conversations",
    feature = "wire-google-genai"
))]
#[test]
fn other_source_wrappers_keep_their_existing_custom_rejection_classification() {
    let source: Value = serde_json::from_str(include_str!(
        "../../../conformance/provider-traces/cloudflare-gateway-binding/other-rejections.json"
    ))
    .unwrap();
    for case in source["cases"].as_array().unwrap() {
        let seen = Arc::new(Observed::default());
        let s = Arc::new(Spawner::default());
        let transport = transport(
            seen.clone(),
            "https://gateway.ai.cloudflare.com/v1/account/gateway",
            vec![],
            200,
            false,
            false,
        )
        .unwrap();
        let services = pi_ai_providers::api::runtime::AdapterServices::new(Arc::new(transport));
        let request = TextStreamRequest {
            resolved: ResolvedRequest {
                model: serde_json::from_value(case["model"].clone()).unwrap(),
                endpoint: RequestEndpoint::new_resolved(case["model"]["baseUrl"].as_str().unwrap())
                    .unwrap(),
            },
            context: serde_json::from_value(
                json!({"messages":[{"role":"user","content":"hello","timestamp":7}]}),
            )
            .unwrap(),
            auth: ResolvedAuth {
                api_key: Some(SecretString::new("synthetic-key")),
                ..Default::default()
            },
            options: StreamOptions {
                wire: Default::default(),
                runtime: runtime(CancellationToken::new(), s.clone()),
            },
        };
        let stream = match case["family"].as_str().unwrap() {
            "anthropic-messages" => {
                pi_ai_providers::api::anthropic_messages::AnthropicMessagesApi::new(services)
                    .stream(request)
            }
            "mistral-conversations" => {
                pi_ai_providers::api::mistral_conversations::MistralApi::new(services)
                    .stream(request)
            }
            "google-generative-ai" => {
                pi_ai_providers::api::google_genai::GoogleGenAiApi::new(services).stream(request)
            }
            _ => unreachable!(),
        };
        ready(s.0.lock().unwrap().pop().unwrap());
        let result = ready(stream.result());
        assert_eq!(result.stop_reason, StopReason::Error);
        assert_eq!(
            is_retryable_assistant_error(&result),
            case["retryable"].as_bool().unwrap(),
            "{}",
            case["family"]
        );
        assert!(seen.order.lock().unwrap().is_empty());
    }
}
