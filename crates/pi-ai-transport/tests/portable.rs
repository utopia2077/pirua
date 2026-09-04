use pi_ai_core::{
    BoxFuture, CancellationToken, Cancelled, Clock, RequestEndpoint, RequestRuntime, SpawnError,
    TaskSpawner,
};
use pi_ai_transport::*;

// Eager collection exists only in test helpers. Adapters use next_frame and stop
// at their terminal before parsing remaining queued frames.
trait TestDrain {
    fn feed(&mut self, chunk: &[u8]) -> Result<Vec<SseFrame>, TransportError>;
    fn finish(&mut self) -> Result<Vec<SseFrame>, TransportError>;
}
impl TestDrain for SseDecoder {
    fn feed(&mut self, chunk: &[u8]) -> Result<Vec<SseFrame>, TransportError> {
        self.push(chunk)?;
        let mut frames = Vec::new();
        while let Some(frame) = self.next_frame()? {
            frames.push(frame);
        }
        Ok(frames)
    }
    fn finish(&mut self) -> Result<Vec<SseFrame>, TransportError> {
        self.end_input();
        let mut frames = Vec::new();
        while let Some(frame) = self.next_frame()? {
            frames.push(frame);
        }
        Ok(frames)
    }
}
use std::{
    collections::{BTreeMap, VecDeque},
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
struct ManualClock {
    now: AtomicU64,
    sleeps: Mutex<Vec<Duration>>,
}
impl Clock for ManualClock {
    fn now_unix_ms(&self) -> u64 {
        self.now.load(Ordering::SeqCst)
    }
    fn sleep<'a>(
        &'a self,
        duration: Duration,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), Cancelled>> {
        Box::pin(async move {
            let until = self.now_unix_ms() + duration.as_millis() as u64;
            self.sleeps.lock().unwrap().push(duration);
            poll_fn(|_| {
                if self.now_unix_ms() >= until {
                    Poll::Ready(Ok(()))
                } else {
                    Poll::Pending
                }
            })
            .await
        })
    }
}
struct Spawner;
impl TaskSpawner for Spawner {
    fn spawn(&self, _: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
        panic!("transport must not spawn through core")
    }
}
struct Jitter(f64);
impl RetryJitter for Jitter {
    fn sample(&self) -> f64 {
        self.0
    }
}
fn runtime(clock: Arc<ManualClock>) -> RequestRuntime {
    RequestRuntime::with_noop_telemetry(CancellationToken::new(), clock, Arc::new(Spawner))
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
fn request() -> HttpRequest {
    HttpRequest {
        endpoint: RequestEndpoint::new_resolved(
            "http://test.invalid/messages?secret=synthetic-secret",
        )
        .unwrap(),
        method: "POST".into(),
        headers: HttpHeaders::new(),
        body: b"synthetic-secret".to_vec(),
        proxy: None,
    }
}
struct Body {
    closed: Arc<AtomicUsize>,
    chunks: VecDeque<Vec<u8>>,
    pending: bool,
}
impl ResponseBody for Body {
    fn next<'a>(
        &'a mut self,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, TransportError>> {
        Box::pin(poll_fn(|_| {
            if self.pending {
                Poll::Pending
            } else {
                Poll::Ready(Ok(self.chunks.pop_front()))
            }
        }))
    }
    fn close(&mut self) {
        self.closed.fetch_add(1, Ordering::SeqCst);
        self.chunks.clear();
    }
}
struct Script {
    responses: Mutex<VecDeque<Result<HttpResponse, TransportError>>>,
    calls: AtomicUsize,
}
impl RawHttpClient for Script {
    fn send<'a>(
        &'a self,
        _: &'a HttpRequest,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<HttpResponse, TransportError>> {
        Box::pin(async {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.responses
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected retry")
        })
    }
}
fn response(
    status: u16,
    headers: &[(&str, &str)],
    pending: bool,
) -> (HttpResponse, Arc<AtomicUsize>) {
    let closed = Arc::new(AtomicUsize::new(0));
    let mut h = HttpHeaders::new();
    for (k, v) in headers {
        h.append(*k, *v).unwrap();
    }
    (
        HttpResponse {
            status,
            headers: h,
            body: Box::new(Body {
                closed: closed.clone(),
                chunks: VecDeque::from([b"first".to_vec()]),
                pending,
            }),
        },
        closed,
    )
}
fn transport(responses: Vec<Result<HttpResponse, TransportError>>) -> (HttpTransport, Arc<Script>) {
    let script = Arc::new(Script {
        responses: Mutex::new(responses.into()),
        calls: AtomicUsize::new(0),
    });
    (
        HttpTransport::new(script.clone(), Arc::new(Jitter(0.0))),
        script,
    )
}
fn shared(max_retries: u64) -> RetryProfile {
    RetryProfile::SharedProvider {
        max_retries,
        max_retry_delay_ms: None,
    }
}

#[test]
fn retries_only_failed_acquisition_and_closes_rejected_body() {
    let clock = Arc::new(ManualClock::default());
    let runtime = runtime(clock.clone());
    let request = request();
    let (first, closed) = response(429, &[("retry-after-ms", "12suffix")], false);
    let (last, _) = response(200, &[], false);
    let (transport, script) = transport(vec![Ok(first), Ok(last)]);
    let mut send = transport.send(&request, &runtime, shared(2), TimeoutProfile::None);
    assert!(poll(send.as_mut()).is_pending());
    assert_eq!(closed.load(Ordering::SeqCst), 1);
    assert_eq!(script.calls.load(Ordering::SeqCst), 1);
    clock.now.store(12, Ordering::SeqCst);
    let mut response = ready(send).unwrap();
    assert_eq!(script.calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        ready(response.body.next(&runtime)).unwrap(),
        Some(b"first".to_vec())
    );
    assert_eq!(ready(response.body.next(&runtime)).unwrap(), None);
    assert_eq!(script.calls.load(Ordering::SeqCst), 2);
}
#[test]
fn success_never_retries_even_with_server_override_and_none_ignores_429() {
    for (status, profile) in [(200, shared(2)), (429, RetryProfile::None)] {
        let runtime = runtime(Arc::new(ManualClock::default()));
        let request = request();
        let (r, _) = response(status, &[("x-should-retry", "true")], false);
        let (transport, script) = transport(vec![Ok(r)]);
        assert_eq!(
            ready(transport.send(&request, &runtime, profile, TimeoutProfile::None))
                .unwrap()
                .status,
            status
        );
        assert_eq!(script.calls.load(Ordering::SeqCst), 1);
    }
}
#[test]
fn veto_and_generic_failure_do_not_retry() {
    let runtime = runtime(Arc::new(ManualClock::default()));
    let request = request();
    let (r, _) = response(503, &[("x-should-retry", "false")], false);
    let (t, s) = transport(vec![Ok(r)]);
    assert_eq!(
        ready(t.send(&request, &runtime, shared(1), TimeoutProfile::None))
            .unwrap()
            .status,
        503
    );
    assert_eq!(s.calls.load(Ordering::SeqCst), 1);
    let (t, s) = transport(vec![Err(TransportError::new(TransportErrorKind::Protocol))]);
    assert_eq!(
        ready(t.send(&request, &runtime, shared(1), TimeoutProfile::None))
            .unwrap_err()
            .kind(),
        TransportErrorKind::Protocol
    );
    assert_eq!(s.calls.load(Ordering::SeqCst), 1);
}
#[test]
fn server_delay_cap_is_inclusive_and_zero_disables_it() {
    for (value, cap, kind) in [
        ("60001", None, Some(TransportErrorKind::RetryDelayExceeded)),
        ("60000", None, None),
        ("60001", Some(0), None),
    ] {
        let clock = Arc::new(ManualClock::default());
        let runtime = runtime(clock.clone());
        let request = request();
        let (r, closed) = response(429, &[("retry-after-ms", value)], false);
        let (last, _) = response(200, &[], false);
        let (t, _) = transport(vec![Ok(r), Ok(last)]);
        let mut send = t.send(
            &request,
            &runtime,
            RetryProfile::SharedProvider {
                max_retries: 1,
                max_retry_delay_ms: cap,
            },
            TimeoutProfile::None,
        );
        if let Some(kind) = kind {
            assert_eq!(ready(send).unwrap_err().kind(), kind);
        } else {
            assert!(poll(send.as_mut()).is_pending());
            clock.now.store(60001, Ordering::SeqCst);
            assert_eq!(ready(send).unwrap().status, 200);
        }
        assert!(closed.load(Ordering::SeqCst) > 0);
    }
}
#[test]
fn backoff_cancel_drops_noncooperative_clock_and_never_sends_again() {
    let clock = Arc::new(ManualClock::default());
    let runtime = runtime(clock.clone());
    let request = request();
    let (r, _) = response(503, &[], false);
    let (t, s) = transport(vec![Ok(r)]);
    let mut send = t.send(&request, &runtime, shared(1), TimeoutProfile::None);
    assert!(poll(send.as_mut()).is_pending());
    assert_eq!(
        *clock.sleeps.lock().unwrap(),
        vec![Duration::from_millis(500)]
    );
    runtime.cancellation.cancel();
    assert_eq!(
        ready(send).unwrap_err().kind(),
        TransportErrorKind::Cancelled
    );
    assert_eq!(s.calls.load(Ordering::SeqCst), 1);
}
struct Stalled {
    drops: Arc<AtomicUsize>,
}
struct DropProbe(Arc<AtomicUsize>);
impl Drop for DropProbe {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
impl RawHttpClient for Stalled {
    fn send<'a>(
        &'a self,
        _: &'a HttpRequest,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<HttpResponse, TransportError>> {
        Box::pin(async {
            let _guard = DropProbe(self.drops.clone());
            std::future::pending().await
        })
    }
}
#[test]
fn cancelled_noncooperative_send_is_dropped_and_cancellation_wins_timeout() {
    let clock = Arc::new(ManualClock::default());
    let runtime = runtime(clock.clone());
    let request = request();
    let drops = Arc::new(AtomicUsize::new(0));
    let t = HttpTransport::new(
        Arc::new(Stalled {
            drops: drops.clone(),
        }),
        Arc::new(Jitter(0.0)),
    );
    let mut send = t.send(
        &request,
        &runtime,
        RetryProfile::None,
        TimeoutProfile::Headers(Duration::from_millis(10)),
    );
    assert!(poll(send.as_mut()).is_pending());
    clock.now.store(10, Ordering::SeqCst);
    runtime.cancellation.cancel();
    assert_eq!(
        ready(send).unwrap_err().kind(),
        TransportErrorKind::Cancelled
    );
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}
#[test]
fn body_cancellation_closes_noncooperative_body_without_retry() {
    let runtime = runtime(Arc::new(ManualClock::default()));
    let unrelated_runtime = RequestRuntime::with_noop_telemetry(
        CancellationToken::new(),
        Arc::new(ManualClock::default()),
        Arc::new(Spawner),
    );
    let request = request();
    let (r, closed) = response(200, &[], true);
    let (t, s) = transport(vec![Ok(r)]);
    let mut response = ready(t.send(&request, &runtime, shared(2), TimeoutProfile::None)).unwrap();
    let mut next = response.body.next(&unrelated_runtime);
    assert!(poll(next.as_mut()).is_pending());
    runtime.cancellation.cancel();
    assert_eq!(
        ready(next).unwrap_err().kind(),
        TransportErrorKind::Cancelled
    );
    assert_eq!(closed.load(Ordering::SeqCst), 1);
    assert_eq!(s.calls.load(Ordering::SeqCst), 1);
}
#[test]
fn headers_timeout_does_not_become_body_timeout_but_whole_timeout_does() {
    for whole in [false, true] {
        let clock = Arc::new(ManualClock::default());
        let runtime = runtime(clock.clone());
        let request = request();
        let (r, closed) = response(200, &[], true);
        let (t, _) = transport(vec![Ok(r)]);
        let timeout = if whole {
            TimeoutProfile::WholeResponse(Duration::from_millis(10))
        } else {
            TimeoutProfile::Headers(Duration::from_millis(10))
        };
        let mut response = ready(t.send(&request, &runtime, RetryProfile::None, timeout)).unwrap();
        clock.now.store(20, Ordering::SeqCst);
        let mut next = response.body.next(&runtime);
        if whole {
            assert_eq!(ready(next).unwrap_err().kind(), TransportErrorKind::Timeout);
            assert_eq!(closed.load(Ordering::SeqCst), 1);
        } else {
            assert!(poll(next.as_mut()).is_pending());
            drop(next);
            response.body.close();
        }
    }
}
#[test]
fn bounded_body_does_not_truncate_and_redaction_has_no_wire_values() {
    let runtime = runtime(Arc::new(ManualClock::default()));
    let (mut r, closed) = response(400, &[("Authorization", "synthetic-secret")], false);
    let error = ready(Box::pin(read_body_limited(r.body.as_mut(), &runtime, 4))).unwrap_err();
    assert_eq!(error.kind(), TransportErrorKind::LimitExceeded);
    assert_eq!(closed.load(Ordering::SeqCst), 1);
    let output = format!(
        "{:?} {:?} {:?} {:?} {}",
        request(),
        r.headers,
        r,
        error,
        error
    );
    assert!(!output.contains("synthetic-secret"));
    assert!(!output.contains("test.invalid"));
}
#[test]
fn proxy_precedence_bypass_ports_and_invalid_credentials_are_redacted() {
    let mut env = ProxyEnvironment {
        scoped: BTreeMap::from([("https_proxy".into(), "scoped.test:90".into())]),
        ambient: BTreeMap::from([("HTTPS_PROXY".into(), "http://ambient.test".into())]),
    };
    assert_eq!(
        env.resolve("https://target.test", false)
            .unwrap()
            .unwrap()
            .expose_for_transport(),
        "https://scoped.test:90/"
    );
    env.scoped.insert("https_proxy".into(), String::new());
    assert_eq!(
        env.resolve("https://target.test", false)
            .unwrap()
            .unwrap()
            .expose_for_transport(),
        "http://ambient.test/"
    );
    env.scoped
        .insert("no_proxy".into(), ".target.test:443 exact.test".into());
    assert!(
        env.resolve("https://sub.target.test", false)
            .unwrap()
            .is_none()
    );
    assert!(env.resolve("https://target.test", false).unwrap().is_some());
    assert!(
        env.resolve("https://sub.target.test:444", false)
            .unwrap()
            .is_some()
    );
    env.scoped
        .insert("HTTPS_PROXY".into(), "socks://secret@proxy.test".into());
    let error = env.resolve("https://target.test", false).unwrap_err();
    assert_eq!(error.kind(), TransportErrorKind::UnsupportedProxy);
    assert!(!format!("{error:?} {error}").contains("secret"));
}
#[test]
fn headers_validate_before_mutation_and_null_policy_remains_caller_owned() {
    let mut h = HttpHeaders::new();
    h.set("AUTHORIZATION", "original").unwrap();
    assert!(h.set("Authorization", "bad\r\nInjected: yes").is_err());
    assert_eq!(h.get_for_wire("authorization"), Some("original"));
    h.append("Set-Cookie", "one").unwrap();
    h.append("set-cookie", "two").unwrap();
    assert_eq!(h.iter_for_wire().count(), 3);
    h.remove("AuThOrIzAtIoN");
    assert_eq!(h.get_for_wire("authorization"), None);
}
#[test]
fn sse_family_profiles_are_not_interchangeable() {
    let mut pi = SseDecoder::new(SseProfile::PiMessages, SseLimits::default());
    assert!(pi.feed(b"data: first\r").unwrap().is_empty());
    let frames = pi.feed(b"\ndata: second\r\n\r\n").unwrap();
    assert_eq!(frames[0].data, "first");
    let mut codex = SseDecoder::new(SseProfile::Codex, SseLimits::default());
    let frames = codex.feed(b"data: first\ndata: second\n\n").unwrap();
    assert_eq!(frames[0].data, "first\nsecond");
    assert!(codex.feed(b"data: unframed").unwrap().is_empty());
    assert!(codex.finish().unwrap().is_empty());
    let mut mistral = SseDecoder::new(SseProfile::Mistral, SseLimits::default());
    assert_eq!(mistral.feed(b"data: [DONE]\r\r").unwrap()[0].data, "[DONE]");
    let mut anthropic = SseDecoder::new(SseProfile::Anthropic, SseLimits::default());
    let frames = anthropic
        .feed(b"event: message_start\ndata:  { }\n\n")
        .unwrap();
    assert_eq!(frames[0].event.as_deref(), Some("message_start"));
    assert_eq!(frames[0].data, " { }");
}
#[test]
fn sse_split_utf8_and_resource_boundaries() {
    let mut pi = SseDecoder::new(SseProfile::PiMessages, SseLimits::default());
    assert!(pi.feed(b"data: \xe4\xbd").unwrap().is_empty());
    assert_eq!(pi.feed(b"\xa0\n\n").unwrap()[0].data, "你");
    let limits = SseLimits {
        max_buffer_bytes: 20,
        max_frame_bytes: 7,
        max_frames_per_feed: 1,
    };
    let mut pi = SseDecoder::new(SseProfile::PiMessages, limits);
    assert_eq!(pi.feed(b"data: x\n\n").unwrap()[0].data, "x");
    assert_eq!(
        pi.feed(b"data: xx\n\n").unwrap_err().kind(),
        TransportErrorKind::LimitExceeded
    );
    assert_eq!(
        pi.feed(b"data: y\n\n").unwrap_err().kind(),
        TransportErrorKind::Closed
    );
    let mut pi = SseDecoder::new(SseProfile::PiMessages, limits);
    assert_eq!(
        pi.feed(b"data: a\n\ndata: b\n\n").unwrap_err().kind(),
        TransportErrorKind::LimitExceeded
    );
}

#[test]
fn regression_leading_bom_and_event_only_error() {
    for profile in [
        SseProfile::PiMessages,
        SseProfile::Codex,
        SseProfile::Mistral,
        SseProfile::Anthropic,
    ] {
        let mut decoder = SseDecoder::new(profile, SseLimits::default());
        assert_eq!(
            decoder.feed(b"\xef\xbb\xbfdata: first\n\n").unwrap().len(),
            1
        );
    }
    let mut decoder = SseDecoder::new(SseProfile::Anthropic, SseLimits::default());
    assert_eq!(decoder.feed(b"event: error\n\n").unwrap().len(), 1);
}
#[test]
fn regression_exact_frame_limit_at_every_split() {
    let bytes = b"data: x\n\n";
    for split in 0..=bytes.len() {
        let mut decoder = SseDecoder::new(
            SseProfile::PiMessages,
            SseLimits {
                max_frame_bytes: 7,
                ..SseLimits::default()
            },
        );
        let mut frames = decoder.feed(&bytes[..split]).unwrap();
        frames.extend(decoder.feed(&bytes[split..]).unwrap());
        assert_eq!(frames.len(), 1);
    }
}
#[test]
fn regression_proxy_port_overflow_cannot_bypass() {
    let env = ProxyEnvironment {
        scoped: BTreeMap::from([
            ("https_proxy".into(), "http://proxy.test".into()),
            ("no_proxy".into(), "target.test:4294967296".into()),
        ]),
        ambient: BTreeMap::new(),
    };
    assert!(env.resolve("https://target.test", false).unwrap().is_some());
}
#[test]
fn regression_nel_is_not_ecmascript_whitespace() {
    let clock = Arc::new(ManualClock::default());
    let runtime = runtime(clock.clone());
    let request = request();
    let (first, _) = response(429, &[("retry-after-ms", "\u{85}12")], false);
    let (t, _) = transport(vec![Ok(first)]);
    let mut send = t.send(&request, &runtime, shared(1), TimeoutProfile::None);
    assert!(poll(send.as_mut()).is_pending());
    assert_eq!(
        *clock.sleeps.lock().unwrap(),
        vec![Duration::from_millis(500)]
    );
}

#[test]
fn regression_terminal_is_visible_before_invalid_tail() {
    let mut decoder = SseDecoder::new(
        SseProfile::PiMessages,
        SseLimits {
            max_frame_bytes: 32,
            max_buffer_bytes: 200,
            max_frames_per_feed: 10,
        },
    );
    decoder.push(b"data: {\"type\":\"done\"}\n\ndata: xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\n\n").unwrap();
    assert_eq!(
        decoder.next_frame().unwrap().unwrap().data,
        "{\"type\":\"done\"}"
    );
    assert_eq!(
        decoder.next_frame().unwrap_err().kind(),
        TransportErrorKind::LimitExceeded
    );
}
#[test]
fn regression_split_bom_is_removed_once_and_colonless_anthropic_is_retained() {
    for profile in [
        SseProfile::PiMessages,
        SseProfile::Codex,
        SseProfile::Mistral,
        SseProfile::Anthropic,
    ] {
        let mut decoder = SseDecoder::new(profile, SseLimits::default());
        let mut frames = Vec::new();
        for byte in b"\xef\xbb\xbfdata: first\n\n\xef\xbb\xbfdata: second\n\n" {
            frames.extend(decoder.feed(&[*byte]).unwrap());
        }
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].data, "first");
    }
    for bytes in [b"event: error\ndata\n\n".as_slice(), b"event: error\n\n"] {
        let mut decoder = SseDecoder::new(SseProfile::Anthropic, SseLimits::default());
        let frame = decoder.feed(bytes).unwrap().remove(0);
        assert_eq!(frame.event.as_deref(), Some("error"));
        assert!(frame.data.is_empty());
    }
}
#[test]
fn regression_every_delimiter_split_preserves_exact_frame_budget() {
    let mixed: [&[u8]; 8] = [
        b"\r\n\r\n",
        b"\r\n\r",
        b"\r\n\n",
        b"\r\r\n",
        b"\n\r\n",
        b"\r\r",
        b"\n\r",
        b"\n\n",
    ];
    for profile in [
        SseProfile::PiMessages,
        SseProfile::Codex,
        SseProfile::Mistral,
        SseProfile::Anthropic,
    ] {
        let endings: &[&[u8]] = match profile {
            SseProfile::PiMessages => &[b"\r\n\r\n", b"\n\n"],
            SseProfile::Codex => &[b"\n\n"],
            _ => &mixed,
        };
        for ending in endings {
            for extra in [false, true] {
                let mut bytes = if extra {
                    b"data: xx".to_vec()
                } else {
                    b"data: x".to_vec()
                };
                bytes.extend_from_slice(ending);
                for split in 0..=bytes.len() {
                    let mut decoder = SseDecoder::new(
                        profile,
                        SseLimits {
                            max_frame_bytes: 7,
                            ..SseLimits::default()
                        },
                    );
                    let result = decoder.feed(&bytes[..split]).and_then(|mut first| {
                        first.extend(decoder.feed(&bytes[split..])?);
                        first.extend(decoder.finish()?);
                        Ok(first)
                    });
                    if extra {
                        assert_eq!(
                            result.unwrap_err().kind(),
                            TransportErrorKind::LimitExceeded
                        );
                    } else {
                        assert_eq!(result.unwrap().len(), 1, "{profile:?} {ending:?} {split}");
                    }
                }
            }
        }
    }
}

#[test]
fn regression_default_retry_dates_preserve_epoch_offsets_and_validate_components() {
    let parser = HttpRetryDateParser;
    for (text, expected) in [
        ("Thu Jan 01 1970 00:00:30 GMT+0000", 30_000.0),
        (
            "Thu Jan 01 1970 01:00:30 GMT+0100 (Synthetic Zone)",
            30_000.0,
        ),
        ("Wed Dec 31 1969 23:00:30 GMT-0100", 30_000.0),
        ("1969-12-31T23:59:59.000Z", -1000.0),
        ("1970-01-01T01:00:30+01:00", 30_000.0),
        ("1969-12-31T23:00:30-01:00", 30_000.0),
        ("1970-01-01", 0.0),
        ("1970", 0.0),
        ("1970-01-01T24:00:00Z", 86_400_000.0),
        ("Thu, 01 Jan 1970 00:00:30 GMT", 30_000.0),
    ] {
        assert_eq!(parser.unix_ms(text), Some(expected), "{text}");
    }
    for invalid in [
        "1970-13-01",
        "1970-02-29",
        "2000-02-30",
        "1970-01-01T00:60:00Z",
        "1970-01-01T00:00:60Z",
        "1970-01-01T24:00:01Z",
        "1970-01-01T00:00:00+24:00",
        "1970-01-01T00:00:00+00:60",
        "1970-01-01T00:00:00",
        "-000000-01-01",
        "Thu Jan 32 1970 00:00:00 GMT+0000",
    ] {
        assert_eq!(parser.unix_ms(invalid), None, "{invalid}");
    }
    assert!(parser.unix_ms("2000-02-29").is_some());
}
#[test]
fn regression_canonical_date_drives_remaining_delay_and_cap() {
    for (now, date, delay, cap) in [
        (0, "Thu Jan 01 1970 00:00:30 GMT+0000", 30_000, false),
        (10_000, "Thu Jan 01 1970 00:00:30 GMT+0000", 20_000, false),
        (0, "Thu Jan 01 1970 00:01:01 GMT+0000", 0, true),
    ] {
        let clock = Arc::new(ManualClock::default());
        clock.now.store(now, Ordering::SeqCst);
        let runtime = runtime(clock.clone());
        let request = request();
        let (first, _) = response(429, &[("retry-after", date)], false);
        let (t, _) = transport(vec![Ok(first)]);
        let mut send = t.send(&request, &runtime, shared(1), TimeoutProfile::None);
        if cap {
            assert_eq!(
                ready(send).unwrap_err().kind(),
                TransportErrorKind::RetryDelayExceeded
            );
        } else {
            assert!(poll(send.as_mut()).is_pending());
            assert_eq!(
                *clock.sleeps.lock().unwrap(),
                vec![Duration::from_millis(delay)]
            );
        }
    }
}
