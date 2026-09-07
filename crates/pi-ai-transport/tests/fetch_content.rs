use pi_ai_core::{
    BoxFuture, CancellationToken, Cancelled, Clock, RequestEndpoint, RequestRuntime, SpawnError,
    TaskSpawner,
};
use pi_ai_transport::*;
use serde_json::{Value, json};
use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
    time::Duration,
};

struct Host;
impl Clock for Host {
    fn now_unix_ms(&self) -> u64 {
        0
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
    fn spawn(&self, _: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
        Err(SpawnError::new())
    }
}
impl RetryJitter for Host {
    fn sample(&self) -> f64 {
        0.0
    }
}
fn runtime() -> RequestRuntime {
    RequestRuntime::with_noop_telemetry(CancellationToken::new(), Arc::new(Host), Arc::new(Host))
}
fn ready<T>(mut f: BoxFuture<'_, T>) -> T {
    match f.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(v) => v,
        Poll::Pending => panic!("unexpected pending"),
    }
}
fn hex(s: &str) -> Vec<u8> {
    s.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|v| u8::from_str_radix(std::str::from_utf8(v).unwrap(), 16).unwrap())
        .collect()
}
fn corpus() -> Value {
    serde_json::from_str(include_str!(
        "../../../conformance/ai-transport/fetch-content/vectors.json"
    ))
    .unwrap()
}
#[derive(Default)]
struct Observed {
    reads: AtomicUsize,
    closed: AtomicUsize,
    requests: Mutex<Vec<Value>>,
}
struct Body {
    chunks: VecDeque<Vec<u8>>,
    observed: Arc<Observed>,
    closed: bool,
    block: bool,
}
impl ResponseBody for Body {
    fn next<'a>(
        &'a mut self,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, TransportError>> {
        Box::pin(async move {
            self.observed.reads.fetch_add(1, Ordering::SeqCst);
            if self.chunks.is_empty() && self.block {
                std::future::pending().await
            } else {
                Ok(self.chunks.pop_front())
            }
        })
    }
    fn close(&mut self) {
        if !self.closed {
            self.closed = true;
            self.observed.closed.fetch_add(1, Ordering::SeqCst);
        }
    }
}
impl Drop for Body {
    fn drop(&mut self) {
        self.close();
    }
}
struct Client {
    responses: Mutex<VecDeque<HttpResponse>>,
    observed: Arc<Observed>,
}
impl RawHttpClient for Client {
    fn send<'a>(
        &'a self,
        request: &'a HttpRequest,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<HttpResponse, TransportError>> {
        Box::pin(async move {
            self.observed.requests.lock().unwrap().push(json!({"method":request.method,"headers":request.headers.iter_for_wire().map(|(name,_)|(name,request.headers.get_for_fetch(name).unwrap())).collect::<std::collections::BTreeMap<_,_>>(),"body":request.body}));
            self.responses
                .lock()
                .unwrap()
                .pop_front()
                .ok_or_else(|| TransportError::new(TransportErrorKind::Connection))
        })
    }
}
fn response(
    status: u16,
    headers: HttpHeaders,
    chunks: Vec<Vec<u8>>,
    observed: &Arc<Observed>,
    block: bool,
) -> HttpResponse {
    HttpResponse {
        status,
        status_text: String::new(),
        headers,
        body: Box::new(Body {
            chunks: chunks.into(),
            observed: observed.clone(),
            closed: false,
            block,
        }),
    }
}
fn transport(responses: Vec<HttpResponse>, observed: &Arc<Observed>) -> HttpTransport {
    HttpTransport::new(
        Arc::new(Client {
            responses: Mutex::new(responses.into()),
            observed: observed.clone(),
        }),
        Arc::new(Host),
    )
}
fn request() -> HttpRequest {
    HttpRequest {
        endpoint: RequestEndpoint::new_resolved("http://example.invalid/case").unwrap(),
        method: "GET".into(),
        headers: HttpHeaders::new(),
        body: vec![],
        proxy: None,
    }
}

#[test]
fn complete_network_content_vectors_match_bundled_undici() {
    let corpus = corpus();
    for case in corpus["cases"].as_array().unwrap() {
        let id = case["id"].as_str().unwrap();
        let observed = Arc::new(Observed::default());
        let mut headers = HttpHeaders::new();
        if let Some(coding) = case["encoding"].as_str() {
            headers.append("content-encoding", coding).unwrap();
        }
        if let Some(location) = case["location"].as_str() {
            headers.append("location", location).unwrap();
        }
        let mut responses = vec![response(
            case["initialStatus"].as_u64().unwrap() as u16,
            headers,
            vec![hex(case["encoded"].as_str().unwrap())],
            &observed,
            false,
        )];
        if id == "redirect-skips" {
            responses.push(response(
                200,
                HttpHeaders::new(),
                vec![hex(corpus["plain"].as_str().unwrap())],
                &observed,
                false,
            ));
        }
        let transport =
            transport(responses, &observed).with_fetch_redirects(if case["redirect"] == "manual" {
                FetchRedirectMode::Manual
            } else {
                FetchRedirectMode::Follow
            });
        let mut request = request();
        request.method = case["method"].as_str().unwrap().into();
        request.body = hex(case["requestBody"].as_str().unwrap());
        for (k, v) in case["requestHeaders"].as_object().unwrap() {
            request.headers.append(k, v.as_str().unwrap()).unwrap();
        }
        let runtime = runtime();
        let acquired = ready(transport.send_fetch(
            &request,
            &runtime,
            RetryProfile::None,
            TimeoutProfile::None,
        ));
        let mut bytes = Vec::new();
        let mut stage = "acquisition";
        if let Ok(mut response) = acquired {
            assert_eq!(json!(response.status), case["status"], "{id}");
            stage = "body";
            loop {
                match ready(response.body.next(&runtime)) {
                    Ok(Some(part)) => bytes.extend(part),
                    Ok(None) => {
                        stage = "done";
                        break;
                    }
                    Err(e) => {
                        assert_eq!(e.kind(), TransportErrorKind::Connection, "{id}");
                        break;
                    }
                }
            }
        }
        assert_eq!(stage, case["stage"].as_str().unwrap(), "{id}");
        assert_eq!(bytes, hex(case["decoded"].as_str().unwrap()), "{id}");
        let captured = observed.requests.lock().unwrap();
        assert_eq!(
            captured.len(),
            case["requests"].as_array().unwrap().len(),
            "{id}"
        );
        for (actual, expected) in captured.iter().zip(case["requests"].as_array().unwrap()) {
            let mut expected_headers = expected["headers"].clone();
            // Host and protocol-specific connection framing are native backend duties.
            expected_headers.as_object_mut().unwrap().remove("host");
            expected_headers
                .as_object_mut()
                .unwrap()
                .remove("connection");
            assert_eq!(actual["headers"], expected_headers, "{id}");
            assert_eq!(actual["method"], expected["method"], "{id}");
        }
        assert_eq!(
            observed.closed.load(Ordering::SeqCst),
            captured.len(),
            "{id}"
        );
    }
}

#[test]
fn fragmented_long_streams_use_buffer_not_cumulative_limits() {
    let corpus: Value = serde_json::from_str(include_str!(
        "../../../conformance/ai-transport/fetch-content/incremental.json"
    ))
    .unwrap();
    for case in corpus["cases"].as_array().unwrap() {
        let id = case["id"].as_str().unwrap();
        let observed = Arc::new(Observed::default());
        let mut headers = HttpHeaders::new();
        headers
            .append("content-encoding", case["encoding"].as_str().unwrap())
            .unwrap();
        let encoded = hex(case["encoded"].as_str().unwrap());
        let transport = transport(
            vec![response(
                200,
                headers,
                encoded.chunks(17).map(<[u8]>::to_vec).collect(),
                &observed,
                false,
            )],
            &observed,
        )
        .with_fetch_content_limits(FetchContentLimits {
            max_pending_bytes: 256,
            output_chunk_bytes: 128,
            ..FetchContentLimits::default()
        });
        let runtime = runtime();
        let request = request();
        let mut response = ready(transport.send_fetch(
            &request,
            &runtime,
            RetryProfile::None,
            TimeoutProfile::None,
        ))
        .unwrap();
        assert_eq!(
            observed.reads.load(Ordering::SeqCst),
            0,
            "acquisition consumed body {id}"
        );
        let pattern = case["pattern"].as_str().unwrap().as_bytes();
        let mut total = 0;
        while let Some(chunk) = ready(response.body.next(&runtime)).unwrap() {
            assert!(chunk.len() <= 128, "{id}");
            for byte in chunk {
                assert_eq!(byte, pattern[total % pattern.len()], "{id} at {total}");
                total += 1;
            }
        }
        assert_eq!(total, case["bytes"].as_u64().unwrap() as usize, "{id}");
        assert_eq!(observed.closed.load(Ordering::SeqCst), 1, "{id}");
        assert_eq!(observed.requests.lock().unwrap().len(), 1);
    }
}

#[test]
fn preconstructed_and_raw_send_bypass_network_defaults_and_decoding() {
    for raw_send in [false, true] {
        let observed = Arc::new(Observed::default());
        let mut headers = HttpHeaders::new();
        headers
            .append("content-encoding", "gzip,gzip,gzip,gzip,gzip,gzip")
            .unwrap();
        let transport = transport(
            vec![response(
                200,
                headers,
                vec![b"already semantic".to_vec()],
                &observed,
                false,
            )],
            &observed,
        )
        .with_fetch_response_source(FetchResponseSource::Preconstructed);
        let runtime = runtime();
        let request = request();
        let mut response = ready(if raw_send {
            transport.send(&request, &runtime, RetryProfile::None, TimeoutProfile::None)
        } else {
            transport.send_fetch(&request, &runtime, RetryProfile::None, TimeoutProfile::None)
        })
        .unwrap();
        assert_eq!(
            ready(Box::pin(read_body_limited(
                response.body.as_mut(),
                &runtime,
                100
            )))
            .unwrap(),
            b"already semantic"
        );
        assert_eq!(observed.requests.lock().unwrap()[0]["headers"], json!({}));
        assert_eq!(observed.closed.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn codec_limits_cancellation_and_drop_release_without_retry() {
    let corpus = corpus();
    for coding in ["gzip", "br", "zstd"] {
        let case = corpus["cases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["id"] == coding)
            .unwrap();
        for (pending, codec) in [(8, 32 * 1024 * 1024), (1024, 1), (1024, 32 * 1024 * 1024)] {
            let observed = Arc::new(Observed::default());
            let mut headers = HttpHeaders::new();
            headers.append("content-encoding", coding).unwrap();
            let transport = transport(
                vec![response(
                    200,
                    headers,
                    vec![hex(case["encoded"].as_str().unwrap())],
                    &observed,
                    false,
                )],
                &observed,
            )
            .with_fetch_content_limits(FetchContentLimits {
                max_pending_bytes: pending,
                output_chunk_bytes: 8,
                max_codec_bytes: codec,
            });
            let runtime = runtime();
            let request = request();
            let acquired = ready(transport.send_fetch(
                &request,
                &runtime,
                RetryProfile::None,
                TimeoutProfile::None,
            ));
            if codec == 1 {
                assert_eq!(
                    acquired.unwrap_err().kind(),
                    TransportErrorKind::LimitExceeded
                );
            } else {
                let mut response = acquired.unwrap();
                if pending == 8 {
                    assert_eq!(
                        ready(response.body.next(&runtime)).unwrap_err().kind(),
                        TransportErrorKind::LimitExceeded
                    );
                } else {
                    assert!(ready(response.body.next(&runtime)).unwrap().is_some());
                    runtime.cancellation.cancel();
                    assert_eq!(
                        ready(response.body.next(&runtime)).unwrap_err().kind(),
                        TransportErrorKind::Cancelled
                    );
                }
            }
            assert_eq!(
                observed.closed.load(Ordering::SeqCst),
                1,
                "{coding}/{pending}/{codec}"
            );
            assert_eq!(observed.requests.lock().unwrap().len(), 1);
        }
    }
    for cancel in [false, true] {
        let observed = Arc::new(Observed::default());
        let mut headers = HttpHeaders::new();
        headers.append("content-encoding", "gzip").unwrap();
        let transport = transport(
            vec![response(200, headers, vec![], &observed, true)],
            &observed,
        );
        let runtime = runtime();
        let request = request();
        let mut response = ready(transport.send_fetch(
            &request,
            &runtime,
            RetryProfile::None,
            TimeoutProfile::None,
        ))
        .unwrap();
        let mut next = response.body.next(&runtime);
        assert!(
            next.as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        if cancel {
            runtime.cancellation.cancel();
            assert_eq!(
                ready(next).unwrap_err().kind(),
                TransportErrorKind::Cancelled
            );
        } else {
            drop(next);
        }
        drop(response);
        assert_eq!(observed.closed.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn codec_working_memory_is_rejected_before_unbounded_window_growth() {
    assert_eq!(zstd_safe::version_number(), 10507);
    let corpus: Value = serde_json::from_str(include_str!(
        "../../../conformance/ai-transport/fetch-content/incremental.json"
    ))
    .unwrap();
    for coding in ["br", "zstd"] {
        let case = corpus["cases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["id"] == format!("{coding}-long"))
            .unwrap();
        let observed = Arc::new(Observed::default());
        let mut headers = HttpHeaders::new();
        headers.append("content-encoding", coding).unwrap();
        let transport = transport(
            vec![response(
                200,
                headers,
                vec![hex(case["encoded"].as_str().unwrap())],
                &observed,
                false,
            )],
            &observed,
        )
        .with_fetch_content_limits(FetchContentLimits {
            max_codec_bytes: 1024 * 1024 + 1024,
            ..FetchContentLimits::default()
        });
        let runtime = runtime();
        let request = request();
        let mut response = ready(transport.send_fetch(
            &request,
            &runtime,
            RetryProfile::None,
            TimeoutProfile::None,
        ))
        .unwrap();
        assert_eq!(
            ready(response.body.next(&runtime)).unwrap_err().kind(),
            TransportErrorKind::LimitExceeded,
            "{coding}"
        );
        assert_eq!(observed.closed.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn held_network_tail_matches_source_eof_or_cancellation_without_extra_polls() {
    let corpus: Value = serde_json::from_str(include_str!(
        "../../../conformance/ai-transport/fetch-content/held-body.json"
    ))
    .unwrap();
    for case in corpus["cases"].as_array().unwrap() {
        let observed = Arc::new(Observed::default());
        let mut headers = HttpHeaders::new();
        headers
            .append("content-encoding", case["coding"].as_str().unwrap())
            .unwrap();
        let transport = transport(
            vec![response(
                200,
                headers,
                vec![hex(case["encoded"].as_str().unwrap())],
                &observed,
                true,
            )],
            &observed,
        );
        let runtime = runtime();
        let request = request();
        let mut response = ready(transport.send_fetch(
            &request,
            &runtime,
            RetryProfile::None,
            TimeoutProfile::None,
        ))
        .unwrap();
        let mut bytes = Vec::new();
        loop {
            let mut next = response.body.next(&runtime);
            match next.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
                Poll::Ready(Ok(Some(chunk))) => bytes.extend(chunk),
                Poll::Ready(Ok(None)) => {
                    assert_eq!(case["stage"], "done");
                    break;
                }
                Poll::Ready(Err(error)) => {
                    assert_eq!(case["stage"], "TypeError");
                    assert_eq!(error.kind(), TransportErrorKind::Connection);
                    break;
                }
                Poll::Pending => {
                    assert_eq!(case["stage"], "AbortError");
                    runtime.cancellation.cancel();
                    assert_eq!(
                        ready(next).unwrap_err().kind(),
                        TransportErrorKind::Cancelled
                    );
                    break;
                }
            }
        }
        assert_eq!(bytes.len(), case["size"].as_u64().unwrap() as usize);
        assert_eq!(observed.closed.load(Ordering::SeqCst), 1);
        if case["stage"] == "done" {
            assert_eq!(
                observed.reads.load(Ordering::SeqCst),
                1,
                "must not poll held upstream after codec EOF"
            );
        }
    }
}

#[test]
fn decoding_stays_outside_acquisition_retry_and_retains_original_runtime() {
    struct Immediate;
    impl Clock for Immediate {
        fn now_unix_ms(&self) -> u64 {
            0
        }
        fn sleep<'a>(
            &'a self,
            _: Duration,
            _: &'a CancellationToken,
        ) -> BoxFuture<'a, Result<(), Cancelled>> {
            Box::pin(async { Ok(()) })
        }
    }
    let corpus = corpus();
    let case = &corpus["cases"][0];
    let observed = Arc::new(Observed::default());
    let mut bad = HttpHeaders::new();
    bad.append("content-encoding", "gzip,gzip,gzip,gzip,gzip,gzip")
        .unwrap();
    let mut good = HttpHeaders::new();
    good.append("content-encoding", "gzip").unwrap();
    let transport = transport(
        vec![
            response(200, bad, vec![], &observed, true),
            response(
                200,
                good,
                vec![hex(case["encoded"].as_str().unwrap())],
                &observed,
                true,
            ),
        ],
        &observed,
    );
    let mut original = runtime();
    original.clock = Arc::new(Immediate);
    let request = request();
    let mut response = ready(transport.send_fetch(
        &request,
        &original,
        RetryProfile::SharedProvider {
            max_retries: 1,
            max_retry_delay_ms: None,
        },
        TimeoutProfile::None,
    ))
    .unwrap();
    assert_eq!(observed.requests.lock().unwrap().len(), 2);
    assert_eq!(observed.reads.load(Ordering::SeqCst), 0);
    assert_eq!(observed.closed.load(Ordering::SeqCst), 1);
    let replacement = runtime();
    assert!(ready(response.body.next(&replacement)).unwrap().is_some());
    original.cancellation.cancel();
    assert_eq!(
        ready(response.body.next(&replacement)).unwrap_err().kind(),
        TransportErrorKind::Cancelled
    );
    assert_eq!(observed.closed.load(Ordering::SeqCst), 2);
    assert_eq!(observed.requests.lock().unwrap().len(), 2);
}

#[test]
fn invalid_output_budgets_and_capacity_overflow_fail_safely() {
    for (pending, output) in [(0, 0), (8, 9), (usize::MAX, usize::MAX)] {
        let observed = Arc::new(Observed::default());
        let mut headers = HttpHeaders::new();
        headers.append("content-encoding", "gzip").unwrap();
        let transport = transport(
            vec![response(200, headers, vec![], &observed, true)],
            &observed,
        )
        .with_fetch_content_limits(FetchContentLimits {
            max_pending_bytes: pending,
            output_chunk_bytes: output,
            ..FetchContentLimits::default()
        });
        let runtime = runtime();
        let request = request();
        let result = ready(transport.send_fetch(
            &request,
            &runtime,
            RetryProfile::None,
            TimeoutProfile::None,
        ));
        let error = match result {
            Err(error) => error,
            Ok(mut response) => ready(response.body.next(&runtime)).unwrap_err(),
        };
        assert_eq!(error.kind(), TransportErrorKind::LimitExceeded);
        assert_eq!(observed.reads.load(Ordering::SeqCst), 0);
        assert_eq!(observed.closed.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn gzip_publication_prefix_is_independent_of_public_chunk_limit() {
    struct GatedBody {
        feeds: VecDeque<Vec<u8>>,
        delivered: usize,
        permit: Arc<AtomicUsize>,
        observed: Arc<Observed>,
        closed: bool,
    }
    impl ResponseBody for GatedBody {
        fn next<'a>(
            &'a mut self,
            _: &'a RequestRuntime,
        ) -> BoxFuture<'a, Result<Option<Vec<u8>>, TransportError>> {
            Box::pin(async move {
                if self.delivered >= self.permit.load(Ordering::SeqCst) {
                    return std::future::pending().await;
                }
                self.observed.reads.fetch_add(1, Ordering::SeqCst);
                self.delivered += 1;
                Ok(self.feeds.pop_front())
            })
        }
        fn close(&mut self) {
            if !self.closed {
                self.closed = true;
                self.observed.closed.fetch_add(1, Ordering::SeqCst);
            }
        }
    }
    impl Drop for GatedBody {
        fn drop(&mut self) {
            self.close();
        }
    }
    let corpus: Value = serde_json::from_str(include_str!(
        "../../../conformance/ai-transport/fetch-content/gzip-boundaries.json"
    ))
    .unwrap();
    let expected_bytes = |runs: &Value| -> Vec<u8> {
        runs.as_array()
            .unwrap()
            .iter()
            .flat_map(|r| {
                std::iter::repeat_n(
                    r["byte"].as_u64().unwrap() as u8,
                    r["count"].as_u64().unwrap() as usize,
                )
            })
            .collect()
    };
    for case in corpus["cases"].as_array().unwrap() {
        for output_chunk_bytes in [17, 128, 16384, 32768] {
            let observed = Arc::new(Observed::default());
            let permit = Arc::new(AtomicUsize::new(0));
            let mut headers = HttpHeaders::new();
            headers.append("content-encoding", "gzip").unwrap();
            let body = GatedBody {
                feeds: case["feeds"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|v| hex(v.as_str().unwrap()))
                    .collect(),
                delivered: 0,
                permit: permit.clone(),
                observed: observed.clone(),
                closed: false,
            };
            let transport = transport(
                vec![HttpResponse {
                    status: 200,
                    status_text: String::new(),
                    headers,
                    body: Box::new(body),
                }],
                &observed,
            )
            .with_fetch_content_limits(FetchContentLimits {
                max_pending_bytes: output_chunk_bytes.max(256),
                output_chunk_bytes,
                max_codec_bytes: 1024 * 1024,
            });
            let runtime = runtime();
            let request = request();
            let mut response = ready(transport.send_fetch(
                &request,
                &runtime,
                RetryProfile::None,
                TimeoutProfile::None,
            ))
            .unwrap();
            let mut bytes = Vec::new();
            for (index, step) in case["steps"].as_array().unwrap().iter().enumerate() {
                permit.store(index + 1, Ordering::SeqCst);
                let mut failed = false;
                loop {
                    let mut next = response.body.next(&runtime);
                    match next.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
                        Poll::Ready(Ok(Some(chunk))) => {
                            assert!(chunk.len() <= output_chunk_bytes);
                            bytes.extend(chunk);
                        }
                        Poll::Ready(Err(error)) => {
                            assert_eq!(error.kind(), TransportErrorKind::Connection);
                            failed = true;
                            break;
                        }
                        Poll::Pending => break,
                        Poll::Ready(Ok(None)) => panic!("gzip cannot end before wire EOF"),
                    }
                }
                assert_eq!(
                    failed,
                    !step["error"].is_null(),
                    "{} step{index} output{output_chunk_bytes}",
                    case["id"]
                );
                assert_eq!(
                    bytes,
                    expected_bytes(&step["runs"]),
                    "{} step{index} output{output_chunk_bytes}",
                    case["id"]
                );
            }
            if case["error"].is_null() {
                permit.store(usize::MAX, Ordering::SeqCst);
                assert!(ready(response.body.next(&runtime)).unwrap().is_none());
            }
            assert_eq!(
                bytes,
                expected_bytes(&case["runs"]),
                "{} output{output_chunk_bytes}",
                case["id"]
            );
            assert_eq!(observed.closed.load(Ordering::SeqCst), 1);
            assert_eq!(observed.requests.lock().unwrap().len(), 1);
        }
    }
}

#[cfg(feature = "native")]
mod native {
    use super::*;
    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
    use tokio_rustls::rustls::{self, pki_types::PrivatePkcs8KeyDer};
    struct WarnLogger(AtomicUsize);
    static LOGGER: WarnLogger = WarnLogger(AtomicUsize::new(0));
    impl log::Log for WarnLogger {
        fn enabled(&self, m: &log::Metadata<'_>) -> bool {
            m.level() <= log::Level::Warn
        }
        fn log(&self, r: &log::Record<'_>) {
            if self.enabled(r.metadata()) && r.target().starts_with("hyper") {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        fn flush(&self) {}
    }
    fn engine() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap()
    }
    fn runtime(native: &tokio::runtime::Runtime) -> RequestRuntime {
        RequestRuntime::with_noop_telemetry(
            CancellationToken::new(),
            Arc::new(TokioClock::new(native.handle().clone())),
            Arc::new(Host),
        )
    }
    async fn head(stream: &mut (impl AsyncRead + Unpin)) -> String {
        let mut bytes = Vec::new();
        let mut byte = [0];
        while !bytes.ends_with(b"\r\n\r\n") {
            stream.read_exact(&mut byte).await.unwrap();
            bytes.push(byte[0]);
            assert!(bytes.len() < 16384);
        }
        String::from_utf8(bytes).unwrap()
    }
    #[test]
    fn real_http_fragmented_codecs_and_postheader_errors_never_retry() {
        let native = engine();
        let corpus = corpus();
        for id in [
            "gzip",
            "deflate",
            "raw",
            "br",
            "zstd",
            "stack-br-gzip-zstd",
            "gzip-corrupt",
            "zstd-truncated",
        ] {
            let case = corpus["cases"]
                .as_array()
                .unwrap()
                .iter()
                .find(|c| c["id"] == id)
                .unwrap();
            let encoded = hex(case["encoded"].as_str().unwrap());
            let coding = case["encoding"].as_str().unwrap().to_owned();
            let listener = native
                .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
                .unwrap();
            let address = listener.local_addr().unwrap();
            let server=native.spawn(async move {
                let (mut stream,_)=listener.accept().await.unwrap();let captured=head(&mut stream).await;
                stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Encoding: {coding}\r\nTransfer-Encoding: chunked\r\n\r\n").as_bytes()).await.unwrap();
                for chunk in encoded.chunks(17) {let frame=format!("{:x}\r\n",chunk.len());stream.write_all(frame.as_bytes()).await.unwrap();stream.write_all(chunk).await.unwrap();stream.write_all(b"\r\n").await.unwrap();}
                stream.write_all(b"0\r\n\r\n").await.unwrap();
                let mut byte=[0];let closed=stream.read(&mut byte).await;assert!(matches!(closed,Ok(0)|Err(_)));
                assert!(tokio::time::timeout(Duration::from_millis(20),listener.accept()).await.is_err());captured
            });
            let transport = HttpTransport::new(
                Arc::new(NativeHttpClient::new(native.handle().clone())),
                Arc::new(Host),
            );
            let runtime = runtime(&native);
            let mut request = request();
            request.endpoint = RequestEndpoint::new_resolved(format!("http://{address}/")).unwrap();
            native.block_on(async {
                tokio::time::timeout(Duration::from_secs(5), async {
                    let mut response = transport
                        .send_fetch(
                            &request,
                            &runtime,
                            RetryProfile::SharedProvider {
                                max_retries: 2,
                                max_retry_delay_ms: None,
                            },
                            TimeoutProfile::None,
                        )
                        .await
                        .unwrap();
                    let result = read_body_limited(response.body.as_mut(), &runtime, 10000).await;
                    if case["stage"] == "done" {
                        assert_eq!(
                            result.unwrap(),
                            hex(case["decoded"].as_str().unwrap()),
                            "{id}"
                        );
                    } else {
                        assert_eq!(
                            result.unwrap_err().kind(),
                            TransportErrorKind::Connection,
                            "{id}"
                        );
                    }
                    drop(response);
                    let captured = server.await.unwrap();
                    assert!(captured.contains("accept-encoding: gzip, deflate\r\n"));
                })
                .await
                .unwrap();
            });
        }
    }
    #[test]
    fn real_tls_http1_and_http2_use_verified_local_trust_and_protocol_defaults() {
        log::set_logger(&LOGGER).unwrap();
        log::set_max_level(log::LevelFilter::Warn);
        let native = engine();
        let case: Value = serde_json::from_str(include_str!(
            "../../../conformance/ai-transport/fetch-content/tls.json"
        ))
        .unwrap();
        for h2 in [false, true] {
            let key = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
            let cert = key.cert.der().clone();
            let mut tls = rustls::ServerConfig::builder_with_provider(Arc::new(
                rustls::crypto::aws_lc_rs::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(
                vec![cert.clone()],
                PrivatePkcs8KeyDer::from(key.signing_key.serialize_der()).into(),
            )
            .unwrap();
            tls.alpn_protocols = vec![if h2 {
                b"h2".to_vec()
            } else {
                b"http/1.1".to_vec()
            }];
            let listener = native
                .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
                .unwrap();
            let address = listener.local_addr().unwrap();
            let encoded = hex(case["encoded"].as_str().unwrap());
            let server=native.spawn(async move {
                let (stream,_)=listener.accept().await.unwrap();let mut stream=tokio_rustls::TlsAcceptor::from(Arc::new(tls)).accept(stream).await.unwrap();
                if h2 {
                    let mut connection=h2::server::handshake(stream).await.unwrap();let (request,mut response)=connection.accept().await.unwrap().unwrap();
                    assert!(request.headers().get("connection").is_none());assert_eq!(request.headers()["accept-encoding"],"br, gzip, deflate, zstd");
                    let mut sender=response.send_response(tokio_tungstenite::tungstenite::http::Response::builder().status(200).header("content-encoding","gzip").body(()).unwrap(),false).unwrap();sender.send_data(encoded.into(),true).unwrap();drop(request);
                    assert!(connection.accept().await.is_none_or(|v|v.is_err()));
                }else {
                    let request=head(&mut stream).await;assert!(request.contains("accept-encoding: br, gzip, deflate, zstd\r\n"));assert!(request.contains("connection: keep-alive\r\n"));
                    stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nContent-Length: {}\r\n\r\n",encoded.len()).as_bytes()).await.unwrap();stream.write_all(&encoded).await.unwrap();let mut byte=[0];assert!(matches!(stream.read(&mut byte).await,Ok(0)|Err(_)));
                }
            });
            let transport = HttpTransport::new(
                Arc::new(
                    NativeHttpClient::with_root_certificates(
                        native.handle().clone(),
                        &[cert.to_vec()],
                    )
                    .unwrap(),
                ),
                Arc::new(Host),
            );
            let runtime = runtime(&native);
            let mut request = request();
            request.endpoint =
                RequestEndpoint::new_resolved(format!("https://{address}/")).unwrap();
            native.block_on(async {
                tokio::time::timeout(Duration::from_secs(5), async {
                    let mut response = transport
                        .send_fetch(&request, &runtime, RetryProfile::None, TimeoutProfile::None)
                        .await
                        .unwrap();
                    assert_eq!(
                        read_body_limited(response.body.as_mut(), &runtime, 10000)
                            .await
                            .unwrap(),
                        hex(case["decoded"].as_str().unwrap())
                    );
                    drop(response);
                    server.await.unwrap();
                })
                .await
                .unwrap();
            });
        }
        assert_eq!(
            LOGGER.0.load(Ordering::SeqCst),
            0,
            "Fetch must not send illegal HTTP/1-only headers through Hyper HTTP/2"
        );
        log::set_max_level(log::LevelFilter::Off);
    }

    #[test]
    fn native_fetch_hop_defaults_are_separate_from_raw_send_and_preconstructed() {
        let native = engine();
        for (raw, preconstructed, method, explicit, expected) in [
            (false, false, "GET", None, Some("keep-alive")),
            (false, false, "HEAD", None, Some("close")),
            (false, false, "GET", Some("close"), Some("close")),
            (true, false, "GET", None, None),
            (false, true, "GET", None, None),
        ] {
            let listener = native
                .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
                .unwrap();
            let address = listener.local_addr().unwrap();
            let server = native.spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                let request = head(&mut stream).await;
                stream
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                    .await
                    .unwrap();
                request
            });
            let transport = HttpTransport::new(
                Arc::new(NativeHttpClient::new(native.handle().clone())),
                Arc::new(Host),
            )
            .with_fetch_response_source(if preconstructed {
                FetchResponseSource::Preconstructed
            } else {
                FetchResponseSource::RawNetwork
            });
            let runtime = runtime(&native);
            let mut request = request();
            request.method = method.into();
            request.endpoint = RequestEndpoint::new_resolved(format!("http://{address}/")).unwrap();
            if let Some(value) = explicit {
                request.headers.append("connection", value).unwrap();
            }
            native.block_on(async {
                tokio::time::timeout(Duration::from_secs(5), async {
                    let response = if raw {
                        transport
                            .send(&request, &runtime, RetryProfile::None, TimeoutProfile::None)
                            .await
                    } else {
                        transport
                            .send_fetch(
                                &request,
                                &runtime,
                                RetryProfile::None,
                                TimeoutProfile::None,
                            )
                            .await
                    }
                    .unwrap();
                    drop(response);
                    let head = server.await.unwrap();
                    let connection = head
                        .lines()
                        .find_map(|line| line.strip_prefix("connection: "));
                    assert_eq!(
                        connection, expected,
                        "raw={raw}, preconstructed={preconstructed}, method={method}"
                    );
                })
                .await
                .unwrap();
            });
        }
    }

    #[test]
    fn real_compressed_pending_body_cancellation_and_drop_release_socket() {
        let native = engine();
        let corpus = corpus();
        for cancel in [false, true] {
            let encoded = hex(corpus["cases"][0]["encoded"].as_str().unwrap());
            let listener = native
                .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
                .unwrap();
            let address = listener.local_addr().unwrap();
            let server=native.spawn(async move {
                let (mut stream,_)=listener.accept().await.unwrap();head(&mut stream).await;
                stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nTransfer-Encoding: chunked\r\n\r\n").await.unwrap();
                stream.write_all(format!("{:x}\r\n",encoded.len()).as_bytes()).await.unwrap();stream.write_all(&encoded).await.unwrap();stream.write_all(b"\r\n").await.unwrap();
                let mut byte=[0];assert!(matches!(stream.read(&mut byte).await,Ok(0)|Err(_)),"closed connection, not timeout");
            });
            let transport = HttpTransport::new(
                Arc::new(NativeHttpClient::new(native.handle().clone())),
                Arc::new(Host),
            );
            let original = runtime(&native);
            let mut request = request();
            request.endpoint = RequestEndpoint::new_resolved(format!("http://{address}/")).unwrap();
            native.block_on(async {
                tokio::time::timeout(Duration::from_secs(5), async {
                    let mut response = transport
                        .send_fetch(
                            &request,
                            &original,
                            RetryProfile::None,
                            TimeoutProfile::None,
                        )
                        .await
                        .unwrap();
                    assert!(response.body.next(&original).await.unwrap().is_some());
                    let mut next = response.body.next(&original);
                    assert!(
                        next.as_mut()
                            .poll(&mut Context::from_waker(Waker::noop()))
                            .is_pending()
                    );
                    if cancel {
                        original.cancellation.cancel();
                        assert_eq!(
                            next.await.unwrap_err().kind(),
                            TransportErrorKind::Cancelled
                        );
                    } else {
                        drop(next);
                    }
                    drop(response);
                    server.await.unwrap();
                })
                .await
                .unwrap();
            });
        }
    }

    #[test]
    fn native_gzip_error_prefix_preserves_source_bytes_at_full_output_boundaries() {
        let native = engine();
        let corpus: Value = serde_json::from_str(include_str!(
            "../../../conformance/ai-transport/fetch-content/gzip-boundaries.json"
        ))
        .unwrap();
        for case in corpus["cases"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|c| c["kind"] == "tail")
        {
            for output_chunk_bytes in [17, 32768] {
                let encoded = hex(case["feeds"][0].as_str().unwrap());
                let listener = native
                    .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
                    .unwrap();
                let address = listener.local_addr().unwrap();
                let server=native.spawn(async move {let (mut stream,_)=listener.accept().await.unwrap();head(&mut stream).await;
                    stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nContent-Length: {}\r\n\r\n",encoded.len()).as_bytes()).await.unwrap();stream.write_all(&encoded).await.unwrap();
                    let mut byte=[0];assert!(matches!(stream.read(&mut byte).await,Ok(0)|Err(_)),"peer closes, never timeout");});
                let transport = HttpTransport::new(
                    Arc::new(NativeHttpClient::new(native.handle().clone())),
                    Arc::new(Host),
                )
                .with_fetch_content_limits(FetchContentLimits {
                    output_chunk_bytes,
                    ..FetchContentLimits::default()
                });
                let runtime = runtime(&native);
                let mut request = request();
                request.endpoint =
                    RequestEndpoint::new_resolved(format!("http://{address}/")).unwrap();
                native.block_on(async {
                    tokio::time::timeout(Duration::from_secs(5), async {
                        let mut response = transport
                            .send_fetch(
                                &request,
                                &runtime,
                                RetryProfile::None,
                                TimeoutProfile::None,
                            )
                            .await
                            .unwrap();
                        let mut bytes = Vec::new();
                        loop {
                            match response.body.next(&runtime).await {
                                Ok(Some(chunk)) => {
                                    assert!(chunk.len() <= output_chunk_bytes);
                                    bytes.extend(chunk);
                                }
                                Err(error) => {
                                    assert_eq!(error.kind(), TransportErrorKind::Connection);
                                    break;
                                }
                                Ok(None) => panic!("gzip garbage must fail"),
                            }
                        }
                        let expected: Vec<u8> = case["runs"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .flat_map(|r| {
                                std::iter::repeat_n(
                                    r["byte"].as_u64().unwrap() as u8,
                                    r["count"].as_u64().unwrap() as usize,
                                )
                            })
                            .collect();
                        assert_eq!(bytes, expected, "{} output{output_chunk_bytes}", case["id"]);
                        drop(response);
                        server.await.unwrap();
                    })
                    .await
                    .unwrap();
                });
            }
        }
    }
}
