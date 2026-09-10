#![cfg(feature = "wire-openrouter-images")]
use pi_ai_core::*;
use pi_ai_providers::{
    api::{runtime::AdapterResponseHead, wire_json::WirePayload},
    images::{openrouter::*, *},
};
use pi_ai_transport::*;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    future::{Future, pending},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
    time::Duration,
};
fn ready<T>(future: impl Future<Output = T>) -> T {
    match Box::pin(future)
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("unexpected pending"),
    }
}
struct Host {
    timeout: u64,
    sleeps: Mutex<Vec<u64>>,
}
impl Clock for Host {
    fn now_unix_ms(&self) -> u64 {
        1700000000000
    }
    fn sleep<'a>(
        &'a self,
        d: Duration,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), Cancelled>> {
        self.sleeps.lock().unwrap().push(d.as_millis() as u64);
        Box::pin(async move {
            if d.as_millis() == u128::from(self.timeout) {
                pending().await
            } else {
                Ok(())
            }
        })
    }
}
impl TaskSpawner for Host {
    fn spawn(&self, _: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
        panic!("image wire does not spawn")
    }
}
impl RetryJitter for Host {
    fn sample(&self) -> f64 {
        0.5
    }
}
#[derive(Default)]
struct Seen {
    calls: Mutex<Vec<Value>>,
    closed: AtomicUsize,
    reads: AtomicUsize,
}
struct Body {
    bytes: Option<Vec<u8>>,
    seen: Arc<Seen>,
    absent: bool,
    failure: bool,
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
        assert!(!self.absent, "absent body must not be read");
        self.seen.reads.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            if self.failure {
                Err(TransportError::new(TransportErrorKind::Closed))
            } else {
                Ok(self.bytes.take())
            }
        })
    }
    fn close(&mut self) {
        self.seen.closed.fetch_add(1, Ordering::SeqCst);
    }
}
struct Fake {
    config: Value,
    response: Value,
    seen: Arc<Seen>,
    count: AtomicUsize,
}
impl CustomFetch for Fake {
    fn fetch<'a>(
        &'a self,
        r: &'a FetchRequest,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<HttpResponse, TransportError>> {
        Box::pin(async move {
            let count = self.count.fetch_add(1, Ordering::SeqCst);
            let (headers, form) = match &r.headers {
                FetchHeaders::Constructed(h) => (
                    h.iter_for_fetch().collect::<BTreeMap<_, _>>(),
                    "constructed",
                ),
                _ => panic!("SDK supplies constructed Headers"),
            };
            let mut call = json!({"kind":"fetch","url":r.endpoint.expose_for_transport(),"method":r.method,"headers":headers,"headerForm":form});
            match &r.body {
                FetchBody::Absent => {
                    call["bodyForm"] = json!("absent");
                }
                FetchBody::Text(v) => {
                    call["bodyForm"] = json!("string");
                    call["body"] = json!(v);
                }
                FetchBody::Bytes(_) => panic!("SDK image JSON is text"),
            };
            self.seen.calls.lock().unwrap().push(call);
            if count < self.config["fetchReject"].as_u64().unwrap_or(0) as usize {
                return Err(TransportError::new(TransportErrorKind::Protocol));
            }
            let status = self.config["statuses"]
                .as_array()
                .map(|v| v[count.min(v.len() - 1)].as_u64().unwrap())
                .or(self.config["status"].as_u64())
                .unwrap_or(200) as u16;
            let raw = self.config["raw"]
                .as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| {
                    serde_json::to_string(self.config.get("response").unwrap_or(&self.response))
                        .unwrap()
                });
            let mut headers = HttpHeaders::new();
            headers.set(
                "content-type",
                self.config["contentType"]
                    .as_str()
                    .unwrap_or("application/json"),
            )?;
            if let Some(h) = self.config["responseHeaders"].as_object() {
                for (k, v) in h {
                    headers.set(k, v.as_str().unwrap())?;
                }
            }
            Ok(HttpResponse {
                status,
                status_text: String::new(),
                headers,
                body: Box::new(Body {
                    bytes: Some(raw.into_bytes()),
                    seen: self.seen.clone(),
                    absent: status == 204,
                    failure: count < self.config["readFailure"].as_u64().unwrap_or(0) as usize,
                }),
            })
        })
    }
}
struct Middleware {
    config: Value,
    seen: Arc<Seen>,
}
impl ImagePayloadMiddleware for Middleware {
    fn on_payload<'a>(
        &'a self,
        r: &'a ImageGenerationRequest,
        p: &'a mut WirePayload,
    ) -> BoxFuture<'a, Result<Option<WirePayload>, ModelsError>> {
        Box::pin(async move {
            self.seen
                .calls
                .lock()
                .unwrap()
                .push(json!({"kind":"payload","value":p.as_value()}));
            if self.config["payloadReject"] == true {
                return Err(ModelsError::new(ModelsErrorCode::Stream));
            }
            if self.config["payloadAbort"] == true {
                r.options.runtime.cancellation.cancel();
            }
            if self.config["mutate"] == true {
                p.as_value_mut()["extra"] = json!({"z":1,"2":"two","1":"one"});
            }
            Ok(self.config.get("payload").cloned().map(WirePayload::new))
        })
    }
}
impl ImageResponseMiddleware for Middleware {
    fn on_response<'a>(
        &'a self,
        r: &'a ImageGenerationRequest,
        h: &'a AdapterResponseHead,
    ) -> BoxFuture<'a, Result<(), ModelsError>> {
        Box::pin(async move {
            let headers = h
                .headers
                .iter()
                .map(|(k, v)| (k.to_owned(), v.as_str().unwrap().to_owned()))
                .collect::<BTreeMap<_, _>>();
            self.seen
                .calls
                .lock()
                .unwrap()
                .push(json!({"kind":"response","status":h.status,"headers":headers}));
            if self.config["responseAbort"] == true {
                r.options.runtime.cancellation.cancel();
            }
            if self.config["responseReject"] == true {
                return Err(ModelsError::new(ModelsErrorCode::Stream));
            }
            Ok(())
        })
    }
}
fn fixture() -> Value {
    serde_json::from_str(include_str!(
        "../../../conformance/provider-traces/openrouter-images/trace.json"
    ))
    .unwrap()
}
fn setup(
    case: &Value,
    base: &Value,
) -> (
    OpenRouterImagesApi,
    ImageGenerationRequest,
    Arc<Seen>,
    Arc<Host>,
) {
    let config = &case["config"];
    let seen = Arc::new(Seen::default());
    let host = Arc::new(Host {
        timeout: config["timeoutMs"].as_u64().unwrap_or(600000),
        sleeps: Mutex::new(Vec::new()),
    });
    let fake = Arc::new(Fake {
        config: config.clone(),
        response: base.clone(),
        seen: seen.clone(),
        count: AtomicUsize::new(0),
    });
    let mut services =
        ImageAdapterServices::new(Arc::new(HttpTransport::new_custom(fake, host.clone())));
    let middleware = Arc::new(Middleware {
        config: config.clone(),
        seen: seen.clone(),
    });
    services.payload = Some(middleware.clone());
    services.response = Some(middleware);
    if let Some(env) = config.get("env") {
        services.env = serde_json::from_value(env.clone()).unwrap();
    }
    let model: RawImagesModel = serde_json::from_value(case["model"].clone()).unwrap();
    let policy = ModelHeaderPolicy::from_allowlist(
        model
            .headers
            .as_ref()
            .map(|h| h.as_map().keys().cloned().collect::<Vec<_>>())
            .unwrap_or_default(),
    );
    let model = model.into_model(&policy).unwrap();
    let endpoint = RequestEndpoint::new_resolved(
        config["endpoint"]
            .as_str()
            .unwrap_or(model.base_url.as_str()),
    )
    .unwrap();
    let token = CancellationToken::new();
    if config["preAbort"] == true {
        token.cancel();
    }
    let request = ImageGenerationRequest {
        resolved: ResolvedRequest { model, endpoint },
        context: serde_json::from_value(case["context"].clone()).unwrap(),
        auth: ResolvedAuth {
            api_key: Some(SecretString::new(
                config["key"].as_str().unwrap_or("SYNTHETIC_IMAGE_KEY"),
            )),
            headers: config
                .get("headers")
                .map(|v| serde_json::from_value(v.clone()).unwrap())
                .unwrap_or_default(),
            env: ProviderEnv::default(),
        },
        options: ImagesOptions {
            runtime: RequestRuntime::with_noop_telemetry(token, host.clone(), host.clone()),
            wire: ImagesOptionsWire {
                timeout_ms: config["timeoutMs"]
                    .as_u64()
                    .map(|v| SafeUnsigned::new(v).unwrap()),
                max_retries: config["maxRetries"]
                    .as_u64()
                    .map(|v| SafeUnsigned::new(v).unwrap()),
                max_retry_delay_ms: config["maxRetryDelayMs"]
                    .as_u64()
                    .map(|v| SafeUnsigned::new(v).unwrap()),
                metadata: None,
            },
        },
    };
    (OpenRouterImagesApi::new(services), request, seen, host)
}
#[test]
fn complete_pinned_image_source_matches_payload_fetch_callback_and_partial_result_stages() {
    let fixture = fixture();
    assert_eq!(fixture["cases"].as_array().unwrap().len(), 49);
    for case in fixture["cases"].as_array().unwrap() {
        let (api, request, seen, _) = setup(case, &fixture["baseResponse"]);
        let result = ready(api.generate(request));
        let mut actual = serde_json::to_value(&result).unwrap();
        let mut expected = case["result"].clone();
        assert_eq!(
            actual.get("errorMessage").is_some(),
            expected.get("errorMessage").is_some(),
            "{} error presence",
            case["id"]
        );
        actual.as_object_mut().unwrap().remove("errorMessage");
        expected.as_object_mut().unwrap().remove("errorMessage");
        assert_eq!(
            serde_json::from_value::<AssistantImages>(actual).unwrap(),
            serde_json::from_value::<AssistantImages>(expected).unwrap(),
            "{} result",
            case["id"]
        );
        assert_eq!(
            json!(*seen.calls.lock().unwrap()),
            case["calls"],
            "{} calls",
            case["id"]
        );
        assert!(!format!("{result:?}").contains("SYNTHETIC_PROVIDER_FAILURE"));
    }
}

#[test]
fn blocked_fetch_cancellation_and_body_admission_release_the_attempt() {
    struct Block;
    impl CustomFetch for Block {
        fn fetch<'a>(
            &'a self,
            _: &'a FetchRequest,
            _: &'a RequestRuntime,
        ) -> BoxFuture<'a, Result<HttpResponse, TransportError>> {
            Box::pin(pending())
        }
    }
    let fixture = fixture();
    let case = &fixture["cases"][0];
    let (mut api, request, seen, host) = setup(case, &fixture["baseResponse"]);
    api.services.transport = Arc::new(HttpTransport::new_custom(Arc::new(Block), host));
    let token = request.options.runtime.cancellation.clone();
    let mut future = api.generate(request);
    assert!(
        future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    token.cancel();
    assert_eq!(ready(future).stop_reason, ImagesStopReason::Aborted);
    assert_eq!(seen.calls.lock().unwrap().len(), 1);
    let (mut api, request, seen, _) = setup(case, &fixture["baseResponse"]);
    api.services.max_response_body_bytes = 1;
    let result = ready(api.generate(request));
    assert_eq!(result.stop_reason, ImagesStopReason::Error);
    assert_eq!(seen.calls.lock().unwrap().len(), 2);
    assert!(seen.closed.load(Ordering::SeqCst) > 0);
}
#[test]
fn callback_cancellation_does_not_add_a_success_veto_and_errors_keep_partial_output() {
    let fixture = fixture();
    let case = fixture["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["id"] == "response-abort")
        .unwrap();
    let (api, request, _, _) = setup(case, &fixture["baseResponse"]);
    let token = request.options.runtime.cancellation.clone();
    let output = ready(api.generate(request));
    assert!(token.is_cancelled());
    assert_eq!(output.stop_reason, ImagesStopReason::Stop);
    assert_eq!(output.usage.unwrap().total_tokens.get(), 25);
    let case = fixture["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["id"] == "partial-images")
        .unwrap();
    let (api, request, _, _) = setup(case, &fixture["baseResponse"]);
    let output = ready(api.generate(request));
    assert_eq!(output.stop_reason, ImagesStopReason::Error);
    assert_eq!(output.output.len(), 2);
    assert!(output.response_id.is_some());
    assert!(output.usage.is_some());
}
#[test]
fn inherited_numeric_admission_is_checked_without_changing_usage_math() {
    let fixture = fixture();
    for admission in fixture["admissionCases"].as_array().unwrap() {
        assert_eq!(admission["result"]["stopReason"], "stop");
        assert!(admission["result"].get("usage").is_some());
        let case = admission;
        let (api, request, seen, _) = setup(case, &fixture["baseResponse"]);
        let result = ready(api.generate(request));
        assert_eq!(result.stop_reason, ImagesStopReason::Error);
        assert_eq!(result.response_id.unwrap().as_str(), "kept");
        assert!(result.usage.is_none());
        assert_eq!(seen.calls.lock().unwrap().len(), 3);
    }
}

#[cfg(feature = "native")]
#[test]
fn native_image_post_decodes_and_closes_peer_while_adapter_stays_alive() {
    use std::{
        io::{Read, Write},
        net::TcpListener,
        thread,
    };
    let engine = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let fixture = fixture();
    let body = serde_json::to_string(&fixture["baseResponse"]).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let peer = thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(4)))
            .unwrap();
        let mut bytes = Vec::new();
        let mut byte = [0];
        while !bytes.ends_with(b"\r\n\r\n") {
            socket.read_exact(&mut byte).unwrap();
            bytes.push(byte[0]);
        }
        let head = String::from_utf8(bytes).unwrap();
        assert!(head.starts_with("POST /api/chat/completions HTTP/1.1\r\n"));
        let length = head
            .lines()
            .find_map(|line| {
                line.to_ascii_lowercase()
                    .strip_prefix("content-length:")
                    .map(|v| v.trim().parse::<usize>().unwrap())
            })
            .unwrap();
        let mut payload = vec![0; length];
        socket.read_exact(&mut payload).unwrap();
        let payload: Value = serde_json::from_slice(&payload).unwrap();
        assert_eq!(payload["modalities"], json!(["image", "text"]));
        write!(socket,"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n{body}",body.len()).unwrap();
        socket.flush().unwrap();
        assert_eq!(
            socket.read(&mut byte).unwrap(),
            0,
            "image body owner physically releases connection"
        );
    });
    let (mut api, mut request, _, host) = setup(&fixture["cases"][0], &fixture["baseResponse"]);
    request.resolved.endpoint =
        RequestEndpoint::new_resolved(format!("http://{address}/api")).unwrap();
    api.services.transport = Arc::new(HttpTransport::new(
        Arc::new(NativeHttpClient::new(engine.handle().clone())),
        host,
    ));
    let output = engine.block_on(async {
        tokio::time::timeout(Duration::from_secs(4), api.generate(request))
            .await
            .unwrap()
    });
    assert_eq!(output.stop_reason, ImagesStopReason::Stop);
    assert_eq!(output.output.len(), 3);
    peer.join().unwrap();
    drop(api);
}

#[test]
fn pending_response_observer_owns_its_cancellation_and_resumes_successfully() {
    use std::sync::atomic::AtomicBool;
    struct Observer(Arc<AtomicBool>);
    impl ImageResponseMiddleware for Observer {
        fn on_response<'a>(
            &'a self,
            _: &'a ImageGenerationRequest,
            _: &'a AdapterResponseHead,
        ) -> BoxFuture<'a, Result<(), ModelsError>> {
            Box::pin(std::future::poll_fn(|_| {
                if self.0.load(Ordering::SeqCst) {
                    Poll::Ready(Ok(()))
                } else {
                    Poll::Pending
                }
            }))
        }
    }
    let fixture = fixture();
    let (mut api, request, _, _) = setup(&fixture["cases"][0], &fixture["baseResponse"]);
    let release = Arc::new(AtomicBool::new(false));
    api.services.response = Some(Arc::new(Observer(release.clone())));
    let token = request.options.runtime.cancellation.clone();
    let mut future = api.generate(request);
    assert!(
        future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    token.cancel();
    assert!(
        future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending(),
        "wire must not acquire callback cancellation ownership"
    );
    release.store(true, Ordering::SeqCst);
    let result = ready(future);
    assert_eq!(result.stop_reason, ImagesStopReason::Stop);
    assert_eq!(result.usage.unwrap().total_tokens.get(), 25);
}
