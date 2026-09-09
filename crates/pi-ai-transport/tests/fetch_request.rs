use pi_ai_core::*;
use pi_ai_transport::*;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    future::Future,
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
        panic!("no timer")
    }
}
impl TaskSpawner for Host {
    fn spawn(&self, _: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
        panic!("no spawn")
    }
}
impl RetryJitter for Host {
    fn sample(&self) -> f64 {
        panic!("no retry")
    }
}
fn rt() -> RequestRuntime {
    RequestRuntime::with_noop_telemetry(CancellationToken::new(), Arc::new(Host), Arc::new(Host))
}
fn ready<F: Future>(f: F) -> F::Output {
    match Box::pin(f)
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(v) => v,
        Poll::Pending => panic!("pending"),
    }
}
struct Body(Option<Vec<u8>>);
impl ResponseBody for Body {
    fn next<'a>(
        &'a mut self,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, TransportError>> {
        Box::pin(async { Ok(self.0.take()) })
    }
    fn close(&mut self) {
        self.0 = None;
    }
}
fn response() -> HttpResponse {
    let mut h = HttpHeaders::new();
    h.append("location", "https://unwanted.invalid").unwrap();
    h.append("content-encoding", "gzip").unwrap();
    HttpResponse {
        status: 302,
        status_text: String::new(),
        headers: h,
        body: Box::new(Body(Some(b"already decoded".to_vec()))),
    }
}
#[derive(Default)]
struct Raw(Mutex<Vec<Value>>);
impl RawHttpClient for Raw {
    fn send<'a>(
        &'a self,
        r: &'a HttpRequest,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<HttpResponse, TransportError>> {
        self.0.lock().unwrap().push(
            json!({"headers":r.headers.iter_for_wire().collect::<BTreeMap<_,_>>(),"body":r.body}),
        );
        Box::pin(async { Ok(response()) })
    }
}
struct Custom {
    calls: AtomicUsize,
    seen: Mutex<Vec<Value>>,
}
impl CustomFetch for Custom {
    fn fetch<'a>(
        &'a self,
        r: &'a FetchRequest,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<HttpResponse, TransportError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let headers = match &r.headers {
            FetchHeaders::Record(v) => {
                json!({"kind":"record","pairs":v.iter().collect::<Vec<_>>()})
            }
            FetchHeaders::Constructed(v) => {
                json!({"kind":"constructed","pairs":v.iter_for_wire().collect::<Vec<_>>()})
            }
        };
        let body = match &r.body {
            FetchBody::Text(v) => json!({"kind":"text","value":v}),
            FetchBody::Bytes(v) => json!({"kind":"bytes","value":v}),
        };
        self.seen.lock().unwrap().push(json!({"endpoint":r.endpoint.expose_for_transport(),"method":r.method,"headers":headers,"body":body}));
        Box::pin(async { Ok(response()) })
    }
}
#[test]
fn network_record_and_body_selection_matches_actual_node_request_constructor() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../conformance/ai-transport/fetch-request/trace.json"
    ))
    .unwrap();
    assert_eq!(fixture["cases"].as_array().unwrap().len(), 15);
    for case in fixture["cases"].as_array().unwrap() {
        let raw = Arc::new(Raw::default());
        let transport = HttpTransport::new(raw.clone(), Arc::new(Host))
            .with_fetch_response_source(FetchResponseSource::Preconstructed)
            .with_fetch_redirects(FetchRedirectMode::Manual);
        let mut headers = FetchHeaderRecord::new();
        for pair in case["headers"].as_array().unwrap() {
            headers.insert(
                pair[0].as_str().unwrap().into(),
                pair[1].as_str().unwrap().into(),
            );
        }
        let body = if let Some(b) = case["bytes"].as_array() {
            FetchBody::Bytes(b.iter().map(|v| v.as_u64().unwrap() as u8).collect())
        } else {
            FetchBody::Text(case["text"].as_str().unwrap().into())
        };
        let request = FetchRequest {
            endpoint: RequestEndpoint::new_resolved("https://example.invalid/path").unwrap(),
            method: case["method"].as_str().unwrap_or("POST").into(),
            headers: FetchHeaders::Record(headers),
            body,
            proxy: None,
        };
        let result = ready(transport.send_fetch_request(
            &request,
            &rt(),
            RetryProfile::None,
            TimeoutProfile::None,
        ));
        assert_eq!(
            result.is_ok(),
            case["accepted"].as_bool().unwrap(),
            "{}",
            case["id"]
        );
        let observed = raw.0.lock().unwrap();
        if result.is_ok() {
            assert_eq!(observed.len(), 1);
            assert_eq!(
                observed[0]["headers"], case["selectedHeaders"],
                "{} headers",
                case["id"]
            );
            assert_eq!(observed[0]["body"], case["selectedBody"]);
        } else {
            assert!(observed.is_empty());
        }
    }
}
#[test]
fn custom_receives_original_values_and_legacy_forms_without_native_fallback_or_decode() {
    let custom = Arc::new(Custom {
        calls: AtomicUsize::new(0),
        seen: Mutex::new(vec![]),
    });
    let transport = HttpTransport::new_custom(custom.clone(), Arc::new(Host))
        .with_fetch_response_source(FetchResponseSource::RawNetwork)
        .with_fetch_redirects(FetchRedirectMode::Follow);
    let mut record = FetchHeaderRecord::new();
    record.insert("x-alias".into(), "lower".into());
    record.insert("X-Alias".into(), "upper".into());
    record.insert("x-text".into(), "😀".into());
    let req = FetchRequest {
        endpoint: RequestEndpoint::new_resolved("not a network URL").unwrap(),
        method: "custom method".into(),
        headers: FetchHeaders::Record(record),
        body: FetchBody::Text("\u{feff}null".into()),
        proxy: None,
    };
    let runtime = rt();
    let mut result = ready(transport.send_fetch_request(
        &req,
        &runtime,
        RetryProfile::None,
        TimeoutProfile::None,
    ))
    .unwrap();
    assert_eq!(result.status, 302);
    assert_eq!(
        ready(read_body_limited(result.body.as_mut(), &runtime, 100)).unwrap(),
        b"already decoded"
    );
    assert_eq!(custom.calls.load(Ordering::SeqCst), 1);
    let seen = custom.seen.lock().unwrap();
    assert_eq!(seen[0]["headers"]["kind"], "record");
    assert_eq!(
        seen[0]["headers"]["pairs"],
        json!([["x-alias", "lower"], ["X-Alias", "upper"], ["x-text", "😀"]])
    );
    assert_eq!(seen[0]["body"]["kind"], "text");
    assert_eq!(seen[0]["body"]["value"], "\u{feff}null");
    drop(seen);
    let mut h = HttpHeaders::new();
    h.append("x", "kept").unwrap();
    let old = HttpRequest {
        endpoint: RequestEndpoint::new_resolved("https://example.invalid").unwrap(),
        method: "POST".into(),
        headers: h,
        body: b"{}".to_vec(),
        proxy: None,
    };
    ready(transport.send_fetch(&old, &runtime, RetryProfile::None, TimeoutProfile::None)).unwrap();
    let seen = custom.seen.lock().unwrap();
    assert_eq!(seen[1]["headers"]["kind"], "constructed");
    assert_eq!(seen[1]["body"]["kind"], "bytes");
    drop(seen);
    assert_eq!(
        ready(transport.send(&old, &runtime, RetryProfile::None, TimeoutProfile::None))
            .unwrap_err()
            .kind(),
        TransportErrorKind::UnsupportedOperation
    );
    assert_eq!(custom.calls.load(Ordering::SeqCst), 2);
}
#[test]
fn record_exact_replacement_numeric_order_and_debug_retain_source_values() {
    let mut record = FetchHeaderRecord::new();
    for (k, v) in [
        ("z", "one"),
        ("2", "two"),
        ("10", "ten"),
        ("01", "leading"),
        ("z", "replacement"),
        ("Z", "alias"),
    ] {
        record.insert(k.into(), v.into());
    }
    assert_eq!(
        record.iter().map(|(k, _)| k).collect::<Vec<_>>(),
        ["2", "10", "z", "01", "Z"]
    );
    assert_eq!(record.get("z"), Some("replacement"));
    assert_eq!(record.get("Z"), Some("alias"));
    assert_eq!(format!("{record:?}"), "FetchHeaderRecord([REDACTED])");
    assert_eq!(
        format!("{:?}", FetchBody::Text("secret".into())),
        "FetchBody([REDACTED])"
    );
}

#[cfg(feature = "native")]
fn peer_eof(socket: &mut std::net::TcpStream) -> std::io::Result<bool> {
    use std::io::Read;
    socket.read(&mut [0]).map(|count| count == 0)
}

#[cfg(feature = "native")]
#[test]
fn physical_release_check_rejects_a_held_open_peer_and_requires_eof() {
    use std::{
        io::{ErrorKind, Write},
        net::{TcpListener, TcpStream},
    };
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let mut peer = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let (mut socket, _) = listener.accept().unwrap();
    socket
        .set_read_timeout(Some(Duration::from_millis(10)))
        .unwrap();
    let error = peer_eof(&mut socket).expect_err("an idle live peer is not EOF");
    assert!(matches!(
        error.kind(),
        ErrorKind::WouldBlock | ErrorKind::TimedOut
    ));
    peer.write_all(b"x").unwrap();
    assert!(
        !peer_eof(&mut socket).unwrap(),
        "the same peer is still able to send"
    );
    drop(peer);
    assert!(
        peer_eof(&mut socket).unwrap(),
        "actual EOF follows peer retirement"
    );
}

#[cfg(feature = "native")]
#[test]
fn logical_native_request_uses_network_defaults_and_actual_socket_ownership() {
    use std::{
        io::{Read, Write},
        net::{TcpListener, TcpStream},
        thread,
    };
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let (released, release_observed) = tokio::sync::oneshot::channel();
    let server = thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(4)))
            .unwrap();
        fn head(socket: &mut TcpStream) -> String {
            let mut b = vec![];
            while !b.ends_with(b"\r\n\r\n") {
                let mut x = [0];
                socket.read_exact(&mut x).unwrap();
                b.push(x[0]);
                assert!(b.len() < 16384);
            }
            String::from_utf8(b).unwrap()
        }
        let head = head(&mut socket);
        assert!(head.starts_with("POST /logical HTTP/1.1\r\n"));
        for line in [
            "accept: */*\r\n",
            "content-type: text/plain;charset=UTF-8\r\n",
            "content-length: 5\r\n",
            "x-alias: lower, upper\r\n",
        ] {
            assert!(head.contains(line), "missing {line}");
        }
        let mut body = [0; 5];
        socket.read_exact(&mut body).unwrap();
        assert_eq!(&body, b"hello");
        socket
            .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok")
            .unwrap();
        assert!(peer_eof(&mut socket).expect("read failed before physical EOF"));
        released.send(()).unwrap();
    });
    let engine = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let client = Arc::new(NativeHttpClient::new(engine.handle().clone()));
    let transport = HttpTransport::new(client, Arc::new(Host));
    let mut headers = FetchHeaderRecord::new();
    headers.insert("x-alias".into(), "lower".into());
    headers.insert("X-Alias".into(), "upper".into());
    let request = FetchRequest {
        endpoint: RequestEndpoint::new_resolved(format!("http://{address}/logical")).unwrap(),
        method: "post".into(),
        headers: FetchHeaders::Record(headers),
        body: FetchBody::Text("hello".into()),
        proxy: None,
    };
    let runtime = rt();
    engine.block_on(async {
        let mut response = transport
            .send_fetch_request(&request, &runtime, RetryProfile::None, TimeoutProfile::None)
            .await
            .unwrap();
        assert_eq!(
            read_body_limited(response.body.as_mut(), &runtime, 16)
                .await
                .unwrap(),
            b"ok"
        );
        // Require physical EOF while response, transport, and runtime are alive.
        tokio::time::timeout(Duration::from_secs(5), release_observed)
            .await
            .expect("peer did not observe EOF while owners remained alive")
            .expect("peer failed its physical EOF check");
        assert_eq!(response.status, 200);
        assert!(response.body.next(&runtime).await.unwrap().is_none());
        drop(response);
    });
    server.join().unwrap();
}

struct RejectCustom(Option<TransportErrorKind>);
struct FailedBody;
impl ResponseBody for FailedBody {
    fn next<'a>(
        &'a mut self,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, TransportError>> {
        Box::pin(async { Err(TransportError::new(TransportErrorKind::Protocol)) })
    }
    fn close(&mut self) {}
}
impl CustomFetch for RejectCustom {
    fn fetch<'a>(
        &'a self,
        _: &'a FetchRequest,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<HttpResponse, TransportError>> {
        Box::pin(async move {
            if let Some(kind) = self.0 {
                return Err(TransportError::new(kind));
            }
            Ok(HttpResponse {
                status: 200,
                status_text: String::new(),
                headers: HttpHeaders::new(),
                body: Box::new(FailedBody),
            })
        })
    }
}
#[test]
fn only_custom_send_rejection_is_annotated_without_changing_kind_or_later_body_errors() {
    let request = FetchRequest {
        endpoint: RequestEndpoint::new_resolved("https://example.invalid").unwrap(),
        method: "POST".into(),
        headers: FetchHeaders::Record(FetchHeaderRecord::new()),
        body: FetchBody::Text("{}".into()),
        proxy: None,
    };
    let runtime = rt();
    for kind in [
        TransportErrorKind::InvalidRequest,
        TransportErrorKind::Protocol,
        TransportErrorKind::Timeout,
        TransportErrorKind::Cancelled,
    ] {
        let transport =
            HttpTransport::new_custom(Arc::new(RejectCustom(Some(kind))), Arc::new(Host));
        let error = ready(transport.send_fetch_request(
            &request,
            &runtime,
            RetryProfile::None,
            TimeoutProfile::None,
        ))
        .unwrap_err();
        assert_eq!(error.kind(), kind);
        assert!(error.is_custom_fetch_rejection());
        assert!(!TransportError::new(kind).is_custom_fetch_rejection());
    }
    let transport = HttpTransport::new_custom(Arc::new(RejectCustom(None)), Arc::new(Host));
    let mut response = ready(transport.send_fetch_request(
        &request,
        &runtime,
        RetryProfile::None,
        TimeoutProfile::None,
    ))
    .unwrap();
    let error = ready(response.body.next(&runtime)).unwrap_err();
    assert_eq!(error.kind(), TransportErrorKind::Protocol);
    assert!(!error.is_custom_fetch_rejection());
    let token = runtime.cancellation.clone();
    token.cancel();
    let error = ready(transport.send_fetch_request(
        &request,
        &runtime,
        RetryProfile::None,
        TimeoutProfile::None,
    ))
    .unwrap_err();
    assert_eq!(error.kind(), TransportErrorKind::Cancelled);
    assert!(!error.is_custom_fetch_rejection());
}
