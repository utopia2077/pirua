use pi_ai_core::*;
use pi_ai_transport::*;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    future::{Future, pending},
    sync::{Arc, Mutex},
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
        Box::pin(pending())
    }
}
impl TaskSpawner for Host {
    fn spawn(&self, _: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
        panic!("no tasks")
    }
}
impl RetryJitter for Host {
    fn sample(&self) -> f64 {
        panic!("no retry")
    }
}
fn runtime() -> RequestRuntime {
    RequestRuntime::with_noop_telemetry(CancellationToken::new(), Arc::new(Host), Arc::new(Host))
}
fn ready<T>(future: impl Future<Output = T>) -> T {
    match Box::pin(future)
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(v) => v,
        Poll::Pending => panic!("pending"),
    }
}
struct Body;
impl ResponseBody for Body {
    fn next<'a>(
        &'a mut self,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, TransportError>> {
        Box::pin(async { Ok(None) })
    }
    fn close(&mut self) {}
}
fn response() -> HttpResponse {
    HttpResponse {
        status: 200,
        status_text: String::new(),
        headers: HttpHeaders::new(),
        body: Box::new(Body),
    }
}
struct Custom(Mutex<Vec<&'static str>>);
impl CustomFetch for Custom {
    fn fetch<'a>(
        &'a self,
        r: &'a FetchRequest,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<HttpResponse, TransportError>> {
        self.0.lock().unwrap().push(match r.body {
            FetchBody::Absent => "absent",
            FetchBody::Text(_) => "text",
            FetchBody::Bytes(_) => "bytes",
        });
        Box::pin(async { Ok(response()) })
    }
}
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
fn request(body: FetchBody) -> FetchRequest {
    FetchRequest {
        endpoint: RequestEndpoint::new_resolved("https://synthetic.invalid/").unwrap(),
        method: "POST".into(),
        headers: FetchHeaders::Record(FetchHeaderRecord::new()),
        body,
        proxy: None,
    }
}
#[test]
fn absent_empty_text_and_empty_bytes_retain_distinct_custom_forms_and_source_network_headers() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../conformance/provider-traces/openrouter-images/trace.json"
    ))
    .unwrap();
    let custom = Arc::new(Custom(Mutex::new(vec![])));
    let transport = HttpTransport::new_custom(custom.clone(), Arc::new(Host));
    let raw = Arc::new(Raw(Mutex::new(vec![])));
    let network = HttpTransport::new(raw.clone(), Arc::new(Host))
        .with_fetch_response_source(FetchResponseSource::Preconstructed)
        .with_fetch_redirects(FetchRedirectMode::Manual);
    for body in [
        FetchBody::Absent,
        FetchBody::Text(String::new()),
        FetchBody::Bytes(vec![]),
    ] {
        let r = request(body);
        ready(transport.send_fetch_request(
            &r,
            &runtime(),
            RetryProfile::None,
            TimeoutProfile::None,
        ))
        .unwrap();
        ready(network.send_fetch_request(&r, &runtime(), RetryProfile::None, TimeoutProfile::None))
            .unwrap();
    }
    assert_eq!(*custom.0.lock().unwrap(), ["absent", "text", "bytes"]);
    for (case, observed) in fixture["fetchPresence"]
        .as_array()
        .unwrap()
        .iter()
        .zip(raw.0.lock().unwrap().iter())
    {
        assert_eq!(observed["headers"]["content-type"], case["contentType"]);
        assert_eq!(observed["body"], json!([]));
    }
    assert_eq!(fixture["fetchPresence"][0]["bodyPresent"], false);
    assert_eq!(fixture["fetchPresence"][1]["bodyPresent"], true);
    assert_eq!(fixture["fetchPresence"][2]["bodyPresent"], true);
    // The established empty-byte GET convention remains, while Text is rejected.
    for (body, accepted) in [
        (FetchBody::Absent, true),
        (FetchBody::Bytes(vec![]), true),
        (FetchBody::Text(String::new()), false),
    ] {
        let mut r = request(body);
        r.method = "GET".into();
        assert_eq!(
            ready(network.send_fetch_request(
                &r,
                &runtime(),
                RetryProfile::None,
                TimeoutProfile::None
            ))
            .is_ok(),
            accepted
        );
    }
}
#[cfg(feature = "native")]
#[test]
fn native_empty_body_forms_match_source_content_type_and_physically_close() {
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
    for (body, has_type) in [
        (FetchBody::Absent, false),
        (FetchBody::Text(String::new()), true),
        (FetchBody::Bytes(vec![]), false),
    ] {
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
            let head = String::from_utf8(bytes).unwrap().to_ascii_lowercase();
            assert!(head.starts_with("post / http/1.1\r\n"));
            assert_eq!(
                head.contains("content-type: text/plain;charset=utf-8"),
                has_type
            );
            write!(
                socket,
                "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n"
            )
            .unwrap();
            socket.flush().unwrap();
            assert_eq!(socket.read(&mut byte).unwrap(), 0);
        });
        let transport = HttpTransport::new(
            Arc::new(NativeHttpClient::new(engine.handle().clone())),
            Arc::new(Host),
        );
        let mut r = request(body);
        r.endpoint = RequestEndpoint::new_resolved(format!("http://{address}/")).unwrap();
        engine.block_on(async {
            let response = tokio::time::timeout(
                Duration::from_secs(4),
                transport.send_fetch_request(
                    &r,
                    &runtime(),
                    RetryProfile::None,
                    TimeoutProfile::None,
                ),
            )
            .await
            .unwrap()
            .unwrap();
            drop(response);
        });
        peer.join().unwrap();
    }
}
