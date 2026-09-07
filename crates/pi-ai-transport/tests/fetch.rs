use pi_ai_core::{
    BoxFuture, CancellationToken, Cancelled, Clock, RequestEndpoint, RequestRuntime, SpawnError,
    TaskSpawner,
};
use pi_ai_transport::*;
use std::{
    collections::VecDeque,
    future::{Future, poll_fn},
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
    time::Duration,
};

#[derive(Default)]
struct TestClock {
    now: AtomicU64,
    sleeps: Mutex<Vec<Duration>>,
}
impl Clock for TestClock {
    fn now_unix_ms(&self) -> u64 {
        self.now.load(Ordering::SeqCst)
    }
    fn sleep<'a>(
        &'a self,
        duration: Duration,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), Cancelled>> {
        Box::pin(async move {
            let target = self
                .now_unix_ms()
                .saturating_add(duration.as_millis() as u64);
            self.sleeps.lock().unwrap().push(duration);
            poll_fn(|_| {
                if self.now_unix_ms() >= target {
                    Poll::Ready(Ok(()))
                } else {
                    Poll::Pending
                }
            })
            .await
        })
    }
}
struct NoSpawn;
impl TaskSpawner for NoSpawn {
    fn spawn(&self, _: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
        Err(SpawnError::new())
    }
}
struct ZeroJitter;
impl RetryJitter for ZeroJitter {
    fn sample(&self) -> f64 {
        0.0
    }
}
fn runtime(clock: Arc<TestClock>) -> RequestRuntime {
    RequestRuntime::with_noop_telemetry(CancellationToken::new(), clock, Arc::new(NoSpawn))
}
fn poll<T>(future: Pin<&mut dyn Future<Output = T>>) -> Poll<T> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}
fn ready<T>(mut future: BoxFuture<'_, T>) -> T {
    match poll(future.as_mut()) {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("expected ready"),
    }
}
struct TestBody {
    chunks: VecDeque<Vec<u8>>,
    closed: bool,
    released: Arc<AtomicUsize>,
}
impl ResponseBody for TestBody {
    fn next<'a>(
        &'a mut self,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, TransportError>> {
        Box::pin(async move { Ok(self.chunks.pop_front()) })
    }
    fn close(&mut self) {
        if !self.closed {
            self.closed = true;
            self.released.fetch_add(1, Ordering::SeqCst);
        }
    }
}
impl Drop for TestBody {
    fn drop(&mut self) {
        self.close();
    }
}
fn response(status: u16, headers: &[(&str, &str)]) -> (HttpResponse, Arc<AtomicUsize>) {
    let mut values = HttpHeaders::new();
    for (k, v) in headers {
        values.append(*k, *v).unwrap();
    }
    let released = Arc::new(AtomicUsize::new(0));
    (
        HttpResponse {
            status,
            status_text: String::new(),
            headers: values,
            body: Box::new(TestBody {
                chunks: VecDeque::from([b"ok".to_vec()]),
                closed: false,
                released: released.clone(),
            }),
        },
        released,
    )
}
#[derive(Clone)]
struct Captured {
    url: String,
    method: String,
    headers: HttpHeaders,
    body: Vec<u8>,
}
struct Script {
    steps: Mutex<VecDeque<HttpResponse>>,
    captured: Mutex<Vec<Captured>>,
}
impl RawHttpClient for Script {
    fn send<'a>(
        &'a self,
        request: &'a HttpRequest,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<HttpResponse, TransportError>> {
        Box::pin(async move {
            self.captured.lock().unwrap().push(Captured {
                url: request.endpoint.expose_for_transport().to_owned(),
                method: request.method.clone(),
                headers: request.headers.clone(),
                body: request.body.clone(),
            });
            self.steps
                .lock()
                .unwrap()
                .pop_front()
                .ok_or_else(|| TransportError::new(TransportErrorKind::Protocol))
        })
    }
}
fn transport(responses: Vec<HttpResponse>) -> (HttpTransport, Arc<Script>) {
    let script = Arc::new(Script {
        steps: Mutex::new(responses.into()),
        captured: Mutex::new(Vec::new()),
    });
    (
        HttpTransport::new(script.clone(), Arc::new(ZeroJitter)),
        script,
    )
}
fn request() -> HttpRequest {
    HttpRequest {
        endpoint: RequestEndpoint::new_resolved("http://a.test/start?old=1#original").unwrap(),
        method: "POST".into(),
        headers: HttpHeaders::new(),
        body: b"payload".to_vec(),
        proxy: None,
    }
}
fn shared(max_retries: u64) -> RetryProfile {
    RetryProfile::SharedProvider {
        max_retries,
        max_retry_delay_ms: None,
    }
}

#[test]
fn policy_joined_retry_headers_match_pinned_sdk_helper() {
    for (status, value, expected, calls) in [(400, "true", 400, 1), (503, "false", 200, 2)] {
        let (first, _) = response(
            status,
            &[
                ("x-should-retry", value),
                ("X-Should-Retry", value),
                ("retry-after-ms", "0"),
            ],
        );
        let (last, _) = response(200, &[]);
        let (t, s) = transport(vec![first, last]);
        let runtime = runtime(Arc::new(TestClock::default()));
        let request = request();
        let result = ready(t.send(&request, &runtime, shared(1), TimeoutProfile::None)).unwrap();
        assert_eq!(result.status, expected);
        assert_eq!(s.captured.lock().unwrap().len(), calls);
    }
}
#[test]
fn policy_request_retry_maximum_accepts_initial_success() {
    let (first, _) = response(200, &[]);
    let (t, s) = transport(vec![first]);
    let runtime = runtime(Arc::new(TestClock::default()));
    let request = request();
    assert_eq!(
        ready(t.send(
            &request,
            &runtime,
            shared(9_007_199_254_740_991),
            TimeoutProfile::None
        ))
        .unwrap()
        .status,
        200
    );
    assert_eq!(s.captured.lock().unwrap().len(), 1);
    let captured = s.captured.lock().unwrap();
    assert!(captured[0].url.contains("a.test"));
    assert_eq!(captured[0].method, "POST");
    assert_eq!(captured[0].body, b"payload");
    assert!(captured[0].headers.get_for_wire("x-api-key").is_none());
}

fn credentials(req: &mut HttpRequest) {
    for name in [
        "authorization",
        "proxy-authorization",
        "cookie",
        "host",
        "x-api-key",
    ] {
        req.headers.append(name, "synthetic").unwrap();
    }
    for name in [
        "content-encoding",
        "content-language",
        "content-location",
        "content-type",
        "content-length",
    ] {
        req.headers.append(name, "7").unwrap();
    }
}
#[test]
fn redirect_method_body_and_header_matrix_matches_both_node_oracles() {
    for status in [301, 302, 303, 307, 308] {
        for method in ["POST", "PUT", "HEAD"] {
            let (first, released) = response(status, &[("location", "/final")]);
            let (last, _) = response(200, &[]);
            let (t, s) = transport(vec![first, last]);
            let rt = runtime(Arc::new(TestClock::default()));
            let mut req = request();
            req.method = method.into();
            if method == "HEAD" {
                req.body.clear();
            }
            credentials(&mut req);
            let result =
                ready(t.send_fetch(&req, &rt, RetryProfile::None, TimeoutProfile::None)).unwrap();
            assert_eq!(result.status, 200);
            assert_eq!(released.load(Ordering::SeqCst), 1);
            let seen = s.captured.lock().unwrap();
            assert_eq!(seen.len(), 2);
            let changed = (matches!(status, 301 | 302) && method == "POST")
                || (status == 303 && method != "HEAD");
            assert_eq!(seen[1].method, if changed { "GET" } else { method });
            assert_eq!(
                seen[1].body,
                if changed || method == "HEAD" {
                    b"".as_slice()
                } else {
                    b"payload"
                }
            );
            assert_eq!(seen[1].url, "http://a.test/final#original");
            for name in [
                "authorization",
                "proxy-authorization",
                "cookie",
                "x-api-key",
            ] {
                assert!(seen[1].headers.get_for_wire(name).is_some());
            }
            for name in [
                "content-encoding",
                "content-language",
                "content-location",
                "content-type",
                "content-length",
            ] {
                assert_eq!(seen[1].headers.get_for_wire(name).is_none(), changed);
            }
            assert!(
                seen.iter()
                    .all(|r| r.headers.get_for_wire("host").is_none())
            );
        }
    }
}
#[test]
fn locations_join_decode_resolve_and_inherit_fragments() {
    for (fields, expected) in [
        (
            vec![("location", "../final?q=2")],
            "http://a.test/final?q=2#original",
        ),
        (
            vec![("location", "?new=2#next")],
            "http://a.test/start?new=2#next",
        ),
        (
            vec![("location", "/caf\u{e9}")],
            "http://a.test/caf%EF%BF%BD#original",
        ),
        (
            vec![("location", "/caf\u{c3}\u{a9}")],
            "http://a.test/caf%C3%A9#original",
        ),
        (
            vec![("location", "/one"), ("LOCATION", "/two")],
            "http://a.test/one,%20/two#original",
        ),
        (vec![("location", "")], "http://a.test/start?old=1#original"),
        (
            vec![("location", "/final#")],
            "http://a.test/final#original",
        ),
    ] {
        let (first, _) = response(302, &fields);
        let (last, _) = response(200, &[]);
        let (t, s) = transport(vec![first, last]);
        let rt = runtime(Arc::new(TestClock::default()));
        ready(t.send_fetch(&request(), &rt, RetryProfile::None, TimeoutProfile::None)).unwrap();
        assert_eq!(s.captured.lock().unwrap()[1].url, expected);
    }
}
#[test]
fn origin_comparison_is_normalized_and_preserves_custom_api_keys() {
    for (target, same) in [
        ("http://A.TEST:80/final", true),
        ("http://a.test:81/final", false),
        ("https://a.test/final", false),
        ("http://b.test/final", false),
    ] {
        let (first, _) = response(307, &[("location", target)]);
        let (last, _) = response(200, &[]);
        let (t, s) = transport(vec![first, last]);
        let rt = runtime(Arc::new(TestClock::default()));
        let mut req = request();
        credentials(&mut req);
        ready(t.send_fetch(&req, &rt, RetryProfile::None, TimeoutProfile::None)).unwrap();
        let seen = s.captured.lock().unwrap();
        for name in ["authorization", "proxy-authorization", "cookie"] {
            assert_eq!(seen[1].headers.get_for_wire(name).is_some(), same);
        }
        assert!(seen[1].headers.get_for_wire("x-api-key").is_some());
        assert_eq!(seen[1].body, b"payload");
    }
}
#[test]
fn raw_manual_and_missing_location_keep_original_body() {
    for mode in 0..3 {
        let (first, released) = response(
            302,
            if mode == 2 {
                &[]
            } else {
                &[("location", "ftp://invalid")]
            },
        );
        let (t, s) = transport(vec![first]);
        let rt = runtime(Arc::new(TestClock::default()));
        let req = request();
        let t = t.with_fetch_redirects(FetchRedirectMode::Manual);
        let mut result = if mode == 0 {
            ready(t.send(&req, &rt, RetryProfile::None, TimeoutProfile::None))
        } else {
            ready(t.send_fetch(&req, &rt, RetryProfile::None, TimeoutProfile::None))
        }
        .unwrap();
        assert_eq!(result.status, 302);
        assert_eq!(released.load(Ordering::SeqCst), 0);
        assert_eq!(ready(result.body.next(&rt)).unwrap(), Some(b"ok".to_vec()));
        drop(result);
        assert_eq!(released.load(Ordering::SeqCst), 1);
        assert_eq!(s.captured.lock().unwrap().len(), 1);
    }
    let (first, released) = response(302, &[]);
    let (t, _) = transport(vec![first]);
    let rt = runtime(Arc::new(TestClock::default()));
    let result =
        ready(t.send_fetch(&request(), &rt, RetryProfile::None, TimeoutProfile::None)).unwrap();
    assert_eq!(result.status, 302);
    assert_eq!(released.load(Ordering::SeqCst), 0);
}
#[test]
fn exact_twenty_hop_bound_and_invalid_targets_release_intermediates() {
    for count in [20, 21] {
        let mut responses = Vec::new();
        let mut releases = Vec::new();
        for _ in 0..count {
            let (r, c) = response(307, &[("location", "/loop")]);
            responses.push(r);
            releases.push(c);
        }
        responses.push(response(200, &[]).0);
        let (t, s) = transport(responses);
        let rt = runtime(Arc::new(TestClock::default()));
        let result = ready(t.send_fetch(&request(), &rt, RetryProfile::None, TimeoutProfile::None));
        assert_eq!(result.is_ok(), count == 20);
        assert_eq!(s.captured.lock().unwrap().len(), 21);
        assert!(releases.iter().all(|r| r.load(Ordering::SeqCst) == 1));
    }
    for target in [
        "http://[",
        "ftp://a.test/final",
        "http://synthetic:synthetic@a.test/final",
        "http://a.test:25/final",
    ] {
        let (first, released) = response(302, &[("location", target)]);
        let (t, s) = transport(vec![first]);
        let rt = runtime(Arc::new(TestClock::default()));
        assert!(
            ready(t.send_fetch(&request(), &rt, RetryProfile::None, TimeoutProfile::None)).is_err()
        );
        assert_eq!(released.load(Ordering::SeqCst), 1);
        assert_eq!(s.captured.lock().unwrap().len(), 1);
    }
}
#[test]
fn canonical_request_validation_and_headers_projection() {
    let mut headers = HttpHeaders::new();
    for (k, v) in [
        ("Cookie", " a=1\t"),
        ("COOKIE", "b=2"),
        ("Location", "/one"),
        ("location", "/two"),
        ("set-cookie", "a=1"),
        ("set-cookie", "b=2"),
    ] {
        headers.append(k, v).unwrap();
    }
    assert_eq!(headers.get_for_fetch("CoOkIe").as_deref(), Some("a=1; b=2"));
    assert_eq!(
        headers.get_for_fetch("LOCATION").as_deref(),
        Some("/one, /two")
    );
    assert_eq!(
        headers.get_for_fetch("set-cookie").as_deref(),
        Some("a=1, b=2")
    );
    assert_eq!(headers.get_for_wire("cookie"), Some(" a=1\t"));
    assert_eq!(headers.iter_for_wire().count(), 6);
    for (method, expected) in [
        ("post", "POST"),
        ("pOsT", "POST"),
        ("query", "QUERY"),
        ("patch", "patch"),
        ("pAtCh", "pAtCh"),
        ("GET", "GET"),
        ("head", "HEAD"),
    ] {
        let (t, s) = transport(vec![response(200, &[]).0]);
        let rt = runtime(Arc::new(TestClock::default()));
        let mut req = request();
        req.method = method.into();
        req.body.clear();
        req.headers = headers.clone();
        ready(t.send_fetch(&req, &rt, RetryProfile::None, TimeoutProfile::None)).unwrap();
        let seen = s.captured.lock().unwrap();
        assert_eq!(seen[0].method, expected);
        assert_eq!(seen[0].headers.get_for_wire("cookie"), Some("a=1; b=2"));
        // Expand the former supplied-only count to every Fetch network field.
        // The Node loopback oracle separately retains protocol-owned Host/Connection.
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../conformance/ai-transport/fetch-content/header-projection.json"
        ))
        .unwrap();
        let case = fixture["cases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["method"] == method)
            .unwrap();
        assert_eq!(
            serde_json::json!(
                seen[0]
                    .headers
                    .iter_for_wire()
                    .collect::<std::collections::BTreeMap<_, _>>()
            ),
            case["fetchHeaders"]
        );
    }
    for method in ["", "BAD METHOD", "trAce", "CONNECT", "TRACK", "GET", "HEAD"] {
        let (t, s) = transport(vec![]);
        let rt = runtime(Arc::new(TestClock::default()));
        let mut req = request();
        req.method = method.into();
        assert_eq!(
            ready(t.send_fetch(&req, &rt, RetryProfile::None, TimeoutProfile::None))
                .unwrap_err()
                .kind(),
            TransportErrorKind::InvalidRequest
        );
        assert!(s.captured.lock().unwrap().is_empty());
    }
    let (t, s) = transport(vec![]);
    let rt = runtime(Arc::new(TestClock::default()));
    let mut req = request();
    req.headers.append("x-test", "\u{100}").unwrap();
    assert!(ready(t.send_fetch(&req, &rt, RetryProfile::None, TimeoutProfile::None)).is_err());
    assert!(s.captured.lock().unwrap().is_empty());
}
#[test]
fn retry_restarts_original_headers_body_and_hop_count() {
    let mut steps = Vec::new();
    let mut releases = Vec::new();
    for attempt in 0..2 {
        for _ in 0..20 {
            let (r, c) = response(307, &[("location", "http://b.test/final")]);
            steps.push(r);
            releases.push(c);
        }
        steps.push(
            response(
                if attempt == 0 { 503 } else { 200 },
                &[("retry-after-ms", "0")],
            )
            .0,
        );
    }
    let (t, s) = transport(steps);
    let rt = runtime(Arc::new(TestClock::default()));
    let mut req = request();
    credentials(&mut req);
    let result = ready(t.send_fetch(&req, &rt, shared(1), TimeoutProfile::None)).unwrap();
    assert_eq!(result.status, 200);
    let seen = s.captured.lock().unwrap();
    assert_eq!(seen.len(), 42);
    for index in [0, 21] {
        assert_eq!(seen[index].url, req.endpoint.expose_for_transport());
        assert!(seen[index].headers.get_for_wire("authorization").is_some());
        assert_eq!(seen[index].body, b"payload");
    }
    assert!(seen[1].headers.get_for_wire("authorization").is_none());
    assert!(releases.iter().all(|r| r.load(Ordering::SeqCst) == 1));
}

struct TimedPort {
    calls: AtomicUsize,
    clock: Arc<TestClock>,
    release: Arc<AtomicUsize>,
    pending: bool,
}
impl RawHttpClient for TimedPort {
    fn send<'a>(
        &'a self,
        _: &'a HttpRequest,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<HttpResponse, TransportError>> {
        Box::pin(async move {
            let index = self.calls.fetch_add(1, Ordering::SeqCst);
            if index > 0 && self.pending {
                return std::future::pending().await;
            }
            self.clock.now.fetch_add(6, Ordering::SeqCst);
            Ok(HttpResponse {
                status: 307,
                status_text: String::new(),
                headers: {
                    let mut h = HttpHeaders::new();
                    h.append("location", "/next").unwrap();
                    h
                },
                body: Box::new(TestBody {
                    chunks: VecDeque::new(),
                    closed: false,
                    released: self.release.clone(),
                }),
            })
        })
    }
}
#[test]
fn one_deadline_covers_ready_hops_and_noncooperative_wait_cancel_drop() {
    for pending in [false, true] {
        let clock = Arc::new(TestClock::default());
        let rt = runtime(clock.clone());
        let release = Arc::new(AtomicUsize::new(0));
        let port = Arc::new(TimedPort {
            calls: AtomicUsize::new(0),
            clock: clock.clone(),
            release: release.clone(),
            pending,
        });
        let t = HttpTransport::new(port.clone(), Arc::new(ZeroJitter));
        let req = request();
        let mut future = t.send_fetch(
            &req,
            &rt,
            RetryProfile::None,
            TimeoutProfile::Headers(Duration::from_millis(10)),
        );
        if pending {
            assert!(poll(future.as_mut()).is_pending());
            clock.now.store(10, Ordering::SeqCst);
        }
        assert_eq!(
            ready(future).unwrap_err().kind(),
            TransportErrorKind::Timeout
        );
        assert_eq!(port.calls.load(Ordering::SeqCst), 2);
        assert_eq!(release.load(Ordering::SeqCst), if pending { 1 } else { 2 });
        assert_eq!(clock.sleeps.lock().unwrap().len(), 1);
    }
    for cancel in [false, true] {
        let clock = Arc::new(TestClock::default());
        let rt = runtime(clock.clone());
        let release = Arc::new(AtomicUsize::new(0));
        let port = Arc::new(TimedPort {
            calls: AtomicUsize::new(0),
            clock,
            release: release.clone(),
            pending: true,
        });
        let t = HttpTransport::new(port, Arc::new(ZeroJitter));
        let req = request();
        let mut future = t.send_fetch(&req, &rt, RetryProfile::None, TimeoutProfile::None);
        assert!(poll(future.as_mut()).is_pending());
        if cancel {
            rt.cancellation.cancel();
            assert_eq!(
                ready(future).unwrap_err().kind(),
                TransportErrorKind::Cancelled
            );
        } else {
            drop(future);
        }
        assert_eq!(release.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn sdk_fetch_network_error_retries_but_raw_guards_do_not() {
    for hops in [1, 21] {
        let mut steps = Vec::new();
        for _ in 0..hops {
            steps.push(
                response(
                    307,
                    &[("location", if hops == 1 { "http://[" } else { "/loop" })],
                )
                .0,
            );
        }
        steps.push(response(200, &[]).0);
        let (t, s) = transport(steps);
        let clock = Arc::new(TestClock::default());
        let rt = runtime(clock.clone());
        let req = request();
        let mut future = t.send_fetch(&req, &rt, shared(1), TimeoutProfile::None);
        assert!(poll(future.as_mut()).is_pending());
        clock.now.store(500, Ordering::SeqCst);
        assert_eq!(ready(future).unwrap().status, 200);
        let seen = s.captured.lock().unwrap();
        assert_eq!(seen.len(), hops + 1);
        assert_eq!(seen[hops].url, req.endpoint.expose_for_transport());
        assert_eq!(seen[hops].body, req.body);
    }
    let (t, s) = transport(vec![]);
    let rt = runtime(Arc::new(TestClock::default()));
    let mut req = request();
    req.method = "TRACE".into();
    assert_eq!(
        ready(t.send_fetch(&req, &rt, shared(1), TimeoutProfile::None))
            .unwrap_err()
            .kind(),
        TransportErrorKind::InvalidRequest
    );
    assert!(s.captured.lock().unwrap().is_empty());
}

struct NeverBody;
#[test]
fn initial_fetch_network_failures_retry_without_raw_dispatch() {
    for url in [
        "http://127.0.0.1:25/probe",
        "http://synthetic:synthetic@127.0.0.1/probe",
    ] {
        let (t, s) = transport(vec![]);
        let clock = Arc::new(TestClock::default());
        let rt = runtime(clock.clone());
        let mut req = request();
        req.endpoint = RequestEndpoint::new_resolved(url).unwrap();
        assert_eq!(
            ready(t.send_fetch(&req, &rt, RetryProfile::None, TimeoutProfile::None))
                .unwrap_err()
                .kind(),
            TransportErrorKind::Connection
        );
        let mut future = t.send_fetch(&req, &rt, shared(1), TimeoutProfile::None);
        assert!(poll(future.as_mut()).is_pending());
        assert_eq!(
            *clock.sleeps.lock().unwrap(),
            vec![Duration::from_millis(500)]
        );
        assert!(s.captured.lock().unwrap().is_empty());
        clock.now.store(500, Ordering::SeqCst);
        assert_eq!(
            ready(future).unwrap_err().kind(),
            TransportErrorKind::Connection
        );
        assert_eq!(clock.sleeps.lock().unwrap().len(), 1);
        assert!(s.captured.lock().unwrap().is_empty());
    }
}
#[test]
fn initial_fetch_network_failure_cancellation_wins_before_send_and_in_backoff() {
    for url in [
        "http://127.0.0.1:25/probe",
        "http://synthetic:synthetic@127.0.0.1/probe",
    ] {
        for pre_cancel in [false, true] {
            let (t, s) = transport(vec![]);
            let clock = Arc::new(TestClock::default());
            let rt = runtime(clock.clone());
            let mut req = request();
            req.endpoint = RequestEndpoint::new_resolved(url).unwrap();
            if pre_cancel {
                rt.cancellation.cancel();
            }
            let mut future = t.send_fetch(&req, &rt, shared(1), TimeoutProfile::None);
            if !pre_cancel {
                assert!(poll(future.as_mut()).is_pending());
                rt.cancellation.cancel();
            }
            assert_eq!(
                ready(future).unwrap_err().kind(),
                TransportErrorKind::Cancelled
            );
            assert_eq!(clock.sleeps.lock().unwrap().len(), usize::from(!pre_cancel));
            assert!(s.captured.lock().unwrap().is_empty());
        }
    }
}
#[test]
fn malformed_initial_url_stays_preflight_without_fetch_retry() {
    let (t, s) = transport(vec![]);
    let clock = Arc::new(TestClock::default());
    let rt = runtime(clock.clone());
    let mut req = request();
    req.endpoint = RequestEndpoint::new_resolved("http://[").unwrap();
    assert_eq!(
        ready(t.send_fetch(&req, &rt, shared(1), TimeoutProfile::None))
            .unwrap_err()
            .kind(),
        TransportErrorKind::InvalidRequest
    );
    assert!(clock.sleeps.lock().unwrap().is_empty());
    assert!(s.captured.lock().unwrap().is_empty());
}
impl ResponseBody for NeverBody {
    fn next<'a>(
        &'a mut self,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, TransportError>> {
        Box::pin(std::future::pending())
    }
    fn close(&mut self) {}
}

#[test]
fn final_body_retains_original_runtime_and_whole_chain_deadline() {
    for whole in [false, true] {
        let clock = Arc::new(TestClock::default());
        let rt = runtime(clock.clone());
        let first = response(307, &[("location", "/next")]).0;
        let mut last = response(200, &[]).0;
        last.body = Box::new(NeverBody);
        let (t, s) = transport(vec![first, last]);
        let req = request();
        let timeout = if whole {
            TimeoutProfile::WholeResponse(Duration::from_millis(10))
        } else {
            TimeoutProfile::Headers(Duration::from_millis(10))
        };
        let mut response = ready(t.send_fetch(&req, &rt, shared(1), timeout)).unwrap();
        clock.now.store(10, Ordering::SeqCst);
        let unrelated = runtime(Arc::new(TestClock::default()));
        let mut read = response.body.next(&unrelated);
        if whole {
            assert_eq!(ready(read).unwrap_err().kind(), TransportErrorKind::Timeout);
        } else {
            assert!(poll(read.as_mut()).is_pending());
            unrelated.cancellation.cancel();
            assert!(poll(read.as_mut()).is_pending());
            rt.cancellation.cancel();
            assert_eq!(
                ready(read).unwrap_err().kind(),
                TransportErrorKind::Cancelled
            );
        }
        assert_eq!(s.captured.lock().unwrap().len(), 2);
    }
}

struct RawFailure(TransportErrorKind, AtomicUsize);
impl RawHttpClient for RawFailure {
    fn send<'a>(
        &'a self,
        _: &'a HttpRequest,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<HttpResponse, TransportError>> {
        Box::pin(async move {
            self.1.fetch_add(1, Ordering::SeqCst);
            Err(TransportError::new(self.0))
        })
    }
}
#[test]
fn arbitrary_raw_protocol_and_invalid_request_failures_are_not_network_errors() {
    for kind in [
        TransportErrorKind::Protocol,
        TransportErrorKind::InvalidRequest,
    ] {
        let port = Arc::new(RawFailure(kind, AtomicUsize::new(0)));
        let t = HttpTransport::new(port.clone(), Arc::new(ZeroJitter));
        let rt = runtime(Arc::new(TestClock::default()));
        assert_eq!(
            ready(t.send_fetch(&request(), &rt, shared(1), TimeoutProfile::None))
                .unwrap_err()
                .kind(),
            kind
        );
        assert_eq!(port.1.load(Ordering::SeqCst), 1);
    }
}

#[cfg(feature = "native")]
mod network {
    use super::*;
    use std::{
        io::{Read, Write},
        net::{TcpListener, TcpStream},
        thread,
    };
    fn head(stream: &mut TcpStream) -> (String, Vec<u8>) {
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut bytes = Vec::new();
        let mut byte = [0];
        while !bytes.ends_with(b"\r\n\r\n") {
            stream.read_exact(&mut byte).unwrap();
            bytes.push(byte[0]);
            assert!(bytes.len() < 16384);
        }
        let text = String::from_utf8(bytes).unwrap();
        let length = text
            .lines()
            .find_map(|line| {
                line.to_ascii_lowercase()
                    .strip_prefix("content-length:")
                    .map(|v| v.trim().parse::<usize>().unwrap())
            })
            .unwrap_or(0);
        let mut body = vec![0; length];
        stream.read_exact(&mut body).unwrap();
        (text, body)
    }
    fn engine() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap()
    }
    fn native_runtime(engine: &tokio::runtime::Runtime) -> RequestRuntime {
        RequestRuntime::with_noop_telemetry(
            CancellationToken::new(),
            Arc::new(TokioClock::new(engine.handle().clone())),
            Arc::new(NoSpawn),
        )
    }
    #[test]
    fn production_cross_origin_chain_retries_original_and_preserves_explicit_proxy() {
        // Both logical origins use a single explicitly selected HTTP proxy. No
        // DNS/public endpoints are used. The second attempt must regain original
        // credentials, while each cross-origin hop strips exactly the four fields.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            for index in 0..4 {
                let (mut stream, _) = listener.accept().unwrap();
                let (text, body) = head(&mut stream);
                let lower = text.to_ascii_lowercase();
                assert_eq!(body, b"payload");
                assert!(lower.contains("x-api-key: synthetic\r\n"));
                if index % 2 == 0 {
                    assert!(text.starts_with("POST http://a.test/start?old=1 HTTP/1.1"));
                    assert!(lower.contains("authorization: synthetic\r\n"));
                    assert!(lower.contains("cookie: synthetic\r\n"));
                    stream.write_all(b"HTTP/1.1 307 Redirect\r\nLocation: http://b.test/final\r\nContent-Length: 10000\r\n\r\n").unwrap();
                    let mut byte = [0];
                    assert_eq!(stream.read(&mut byte).unwrap_or(0), 0);
                } else {
                    assert!(text.starts_with("POST http://b.test/final HTTP/1.1"));
                    assert!(!lower.contains("authorization:"));
                    assert!(!lower.contains("cookie:"));
                    stream
                        .write_all(if index == 1 {
                            b"HTTP/1.1 503 Busy\r\nRetry-After-Ms: 0\r\nContent-Length: 0\r\n\r\n"
                                .as_slice()
                        } else {
                            b"HTTP/1.1 200 Fine\r\nContent-Length: 2\r\n\r\nok"
                        })
                        .unwrap();
                }
            }
        });
        let engine = engine();
        let rt = native_runtime(&engine);
        let t = HttpTransport::new(
            Arc::new(NativeHttpClient::new(engine.handle().clone())),
            Arc::new(ZeroJitter),
        );
        let mut req = request();
        for name in [
            "authorization",
            "proxy-authorization",
            "cookie",
            "host",
            "x-api-key",
        ] {
            req.headers.append(name, "synthetic").unwrap();
        }
        req.proxy = Some(ProxyUrl::parse(&format!("http://{address}")).unwrap());
        let mut response = engine
            .block_on(t.send_fetch(
                &req,
                &rt,
                shared(1),
                TimeoutProfile::Headers(Duration::from_secs(5)),
            ))
            .unwrap();
        assert_eq!(response.status, 200);
        assert_eq!(
            engine
                .block_on(read_body_limited(response.body.as_mut(), &rt, 10))
                .unwrap(),
            b"ok"
        );
        server.join().unwrap();
    }
    #[test]
    fn production_relative_post_rewrite_and_body_failure_never_retries() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (mut first, _) = listener.accept().unwrap();
            assert_eq!(head(&mut first).1, b"payload");
            first.write_all(b"HTTP/1.1 303 Redirect\r\nLocation: ../final?next=1\r\nContent-Length: 0\r\n\r\n").unwrap();
            drop(first);
            let (mut last, _) = listener.accept().unwrap();
            let (text, body) = head(&mut last);
            assert!(text.starts_with("GET /final?next=1 HTTP/1.1"));
            assert!(body.is_empty());
            assert!(!text.to_ascii_lowercase().contains("content-type:"));
            last.write_all(b"HTTP/1.1 200 Fine\r\nContent-Length: 100\r\n\r\nshort")
                .unwrap();
        });
        let engine = engine();
        let rt = native_runtime(&engine);
        let t = HttpTransport::new(
            Arc::new(NativeHttpClient::new(engine.handle().clone())),
            Arc::new(ZeroJitter),
        );
        let mut req = request();
        req.endpoint = RequestEndpoint::new_resolved(format!("http://{address}/start")).unwrap();
        req.headers
            .append("content-type", "application/json")
            .unwrap();
        let mut response = engine
            .block_on(t.send_fetch(
                &req,
                &rt,
                shared(1),
                TimeoutProfile::Headers(Duration::from_secs(5)),
            ))
            .unwrap();
        assert!(
            engine
                .block_on(read_body_limited(response.body.as_mut(), &rt, 100))
                .is_err()
        );
        server.join().unwrap();
    }
    #[test]
    fn production_cancel_and_drop_release_pending_redirect_socket() {
        for cancel in [true, false] {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let (tx, rx) = std::sync::mpsc::channel();
            let server = thread::spawn(move || {
                let (mut first, _) = listener.accept().unwrap();
                head(&mut first);
                first
                    .write_all(
                        b"HTTP/1.1 307 Redirect\r\nLocation: /pending\r\nContent-Length: 0\r\n\r\n",
                    )
                    .unwrap();
                drop(first);
                let (mut next, _) = listener.accept().unwrap();
                head(&mut next);
                tx.send(()).unwrap();
                let mut byte = [0];
                assert_eq!(next.read(&mut byte).unwrap_or(0), 0);
            });
            let engine = engine();
            let rt = native_runtime(&engine);
            let t = HttpTransport::new(
                Arc::new(NativeHttpClient::new(engine.handle().clone())),
                Arc::new(ZeroJitter),
            );
            let mut req = request();
            req.endpoint =
                RequestEndpoint::new_resolved(format!("http://{address}/start")).unwrap();
            let mut future = t.send_fetch(&req, &rt, RetryProfile::None, TimeoutProfile::None);
            assert!(poll(future.as_mut()).is_pending());
            let start = std::time::Instant::now();
            loop {
                assert!(start.elapsed() < Duration::from_secs(5));
                assert!(poll(future.as_mut()).is_pending());
                if rx.try_recv().is_ok() {
                    break;
                }
                thread::yield_now();
            }
            if cancel {
                rt.cancellation.cancel();
                assert_eq!(
                    ready(future).unwrap_err().kind(),
                    TransportErrorKind::Cancelled
                );
            } else {
                drop(future);
            }
            server.join().unwrap();
        }
    }
}
