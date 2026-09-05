#![cfg(feature = "native")]

use pi_ai_core::{
    BoxFuture, CancellationToken, RequestEndpoint, RequestRuntime, SpawnError, TaskSpawner,
};
use pi_ai_transport::*;
use std::{
    future::Future,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::{Arc, mpsc},
    task::{Context, Poll, Wake, Waker},
    thread,
    time::{Duration, Instant},
};
use tokio::runtime::Runtime;
use tokio_rustls::rustls::{self, pki_types::PrivatePkcs8KeyDer};
use tokio_tungstenite::tungstenite::{self, Message};

struct NoSpawn;
impl TaskSpawner for NoSpawn {
    fn spawn(&self, _: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
        Err(SpawnError::new())
    }
}
fn runtime(native: &Runtime) -> RequestRuntime {
    RequestRuntime::with_noop_telemetry(
        CancellationToken::new(),
        Arc::new(TokioClock::new(native.handle().clone())),
        Arc::new(NoSpawn),
    )
}
fn engine() -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
}
struct ThreadWake(thread::Thread);
impl Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}
/// The consumer does not enter Tokio. The backend must drive its own I/O.
fn outside_tokio<T>(future: impl Future<Output = T>) -> T {
    let mut future = Box::pin(future);
    let waker = Waker::from(Arc::new(ThreadWake(thread::current())));
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match future.as_mut().poll(&mut Context::from_waker(&waker)) {
            Poll::Ready(value) => return value,
            Poll::Pending => {
                assert!(Instant::now() < deadline, "I/O did not finish");
                thread::park_timeout(Duration::from_millis(20));
            }
        }
    }
}
fn listener() -> (TcpListener, String) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    (listener, address)
}
fn read_head(stream: &mut TcpStream) -> String {
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut bytes = Vec::new();
    let mut b = [0];
    while !bytes.ends_with(b"\r\n\r\n") {
        stream.read_exact(&mut b).unwrap();
        bytes.push(b[0]);
        assert!(bytes.len() < 16384);
    }
    String::from_utf8(bytes).unwrap()
}
fn http_request(address: &str) -> HttpRequest {
    HttpRequest {
        endpoint: RequestEndpoint::new_resolved(format!("http://{address}/messages")).unwrap(),
        method: "POST".into(),
        headers: HttpHeaders::new(),
        body: b"hello".to_vec(),
        proxy: None,
    }
}
fn ws_request(endpoint: String) -> WebSocketRequest {
    WebSocketRequest {
        endpoint: RequestEndpoint::new_resolved(endpoint).unwrap(),
        headers: HttpHeaders::new(),
        proxy: None,
        connect_timeout: Some(Duration::from_secs(2)),
        max_message_bytes: 1024,
        max_frame_bytes: 1024,
    }
}

#[test]
fn real_http_sse_bytes_stream_outside_tokio_and_connection_releases() {
    let (listener, address) = listener();
    let (closed_tx, closed_rx) = mpsc::channel();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let head = read_head(&mut stream);
        assert!(head.starts_with("POST /messages HTTP/1.1"));
        let mut body = [0; 5];
        stream.read_exact(&mut body).unwrap();
        assert_eq!(&body, b"hello");
        stream.write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nContent-Type: text/event-stream\r\n\r\n9\r\ndata: x\n\n\r\n0\r\n\r\n").unwrap();
        let mut b = [0];
        closed_tx
            .send(stream.read(&mut b).unwrap_or(0) == 0)
            .unwrap();
    });
    let native = engine();
    let runtime = runtime(&native);
    let transport = HttpTransport::new(
        Arc::new(NativeHttpClient::new(native.handle().clone())),
        Arc::new(NativeRetryJitter),
    );
    let request = http_request(&address);
    let mut response =
        outside_tokio(transport.send(&request, &runtime, RetryProfile::None, TimeoutProfile::None))
            .unwrap();
    assert_eq!(response.status, 200);
    let bytes = outside_tokio(read_body_limited(response.body.as_mut(), &runtime, 1024)).unwrap();
    let mut sse = SseDecoder::new(SseProfile::PiMessages, SseLimits::default());
    sse.push(&bytes).unwrap();
    assert_eq!(sse.next_frame().unwrap().unwrap().data, "x");
    drop(response);
    assert!(closed_rx.recv_timeout(Duration::from_secs(3)).unwrap());
    server.join().unwrap();
}
#[test]
fn real_http_explicit_proxy_uses_absolute_target() {
    let (listener, address) = listener();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let head = read_head(&mut stream);
        assert!(head.starts_with("POST http://origin.invalid/messages HTTP/1.1"));
        assert!(
            head.to_ascii_lowercase()
                .contains("proxy-authorization: basic ")
        );
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
            .unwrap();
    });
    let native = engine();
    let runtime = runtime(&native);
    let client = NativeHttpClient::new(native.handle().clone());
    let mut request = http_request("origin.invalid");
    request.proxy = Some(ProxyUrl::parse(&format!("http://synthetic:password@{address}")).unwrap());
    let mut response = outside_tokio(client.send(&request, &runtime)).unwrap();
    assert_eq!(
        outside_tokio(read_body_limited(response.body.as_mut(), &runtime, 10)).unwrap(),
        b"ok"
    );
    server.join().unwrap();
}
#[test]
fn native_client_does_not_follow_redirects_or_retry_http_failures() {
    for status in ["302 Found", "503 Service Unavailable"] {
        let (listener, address) = listener();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            read_head(&mut stream);
            stream.write_all(format!("HTTP/1.1 {status}\r\nLocation: http://127.0.0.1:1/forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").as_bytes()).unwrap();
        });
        let native = engine();
        let runtime = runtime(&native);
        let client = NativeHttpClient::new(native.handle().clone());
        let request = http_request(&address);
        let response = outside_tokio(client.send(&request, &runtime)).unwrap();
        assert_eq!(
            response.status,
            if status.starts_with("302") { 302 } else { 503 }
        );
        server.join().unwrap();
    }
}
#[test]
fn native_body_cancellation_releases_pending_socket() {
    let (listener, address) = listener();
    let (tx, rx) = mpsc::channel();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        read_head(&mut stream);
        let mut body = [0; 5];
        stream.read_exact(&mut body).unwrap();
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n")
            .unwrap();
        let mut b = [0];
        tx.send(stream.read(&mut b).unwrap_or(0) == 0).unwrap();
    });
    let native = engine();
    let runtime = runtime(&native);
    let transport = HttpTransport::new(
        Arc::new(NativeHttpClient::new(native.handle().clone())),
        Arc::new(NativeRetryJitter),
    );
    let request = http_request(&address);
    let mut response =
        outside_tokio(transport.send(&request, &runtime, RetryProfile::None, TimeoutProfile::None))
            .unwrap();
    let token = runtime.cancellation.clone();
    let cancellation = thread::spawn(move || {
        thread::sleep(Duration::from_millis(25));
        token.cancel();
    });
    assert_eq!(
        outside_tokio(response.body.next(&runtime))
            .unwrap_err()
            .kind(),
        TransportErrorKind::Cancelled
    );
    assert!(rx.recv_timeout(Duration::from_secs(3)).unwrap());
    cancellation.join().unwrap();
    server.join().unwrap();
}
#[test]
fn real_websocket_frames_and_close_outside_tokio() {
    let (listener, address) = listener();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut ws = tungstenite::accept(stream).unwrap();
        assert_eq!(ws.read().unwrap().into_text().unwrap(), "hello");
        ws.send(Message::Text("world".into())).unwrap();
        assert!(matches!(ws.read().unwrap(), Message::Close(_)));
    });
    let native = engine();
    let runtime = runtime(&native);
    let connector = NativeWebSocketConnector::new(native.handle().clone()).unwrap();
    let request = ws_request(format!("ws://{address}/responses"));
    let mut connection = outside_tokio(connector.connect(&request, &runtime)).unwrap();
    outside_tokio(connection.send(WebSocketFrame::Text("hello".into()), &runtime)).unwrap();
    assert!(
        matches!(outside_tokio(connection.next(&runtime,Some(Duration::from_secs(2)))).unwrap(),Some(WebSocketFrame::Text(s))if s=="world")
    );
    outside_tokio(connection.send(
        WebSocketFrame::Close {
            code: Some(1000),
            reason: "done".into(),
        },
        &runtime,
    ))
    .unwrap();
    server.join().unwrap();
}
#[test]
fn websocket_http_proxy_connect_auth_and_frames() {
    let (listener, address) = listener();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let head = read_head(&mut stream);
        assert!(head.starts_with("CONNECT origin.invalid:80 HTTP/1.1"));
        assert!(head.contains("Proxy-Authorization: Basic c3ludGhldGljOnBhc3N3b3Jk"));
        stream
            .write_all(b"HTTP/1.1 200 Connection established\r\n\r\n")
            .unwrap();
        let mut ws = tungstenite::accept(stream).unwrap();
        assert_eq!(ws.read().unwrap().into_text().unwrap(), "through proxy");
        ws.send(Message::Text("ok".into())).unwrap();
    });
    let native = engine();
    let runtime = runtime(&native);
    let connector = NativeWebSocketConnector::new(native.handle().clone()).unwrap();
    let mut request = ws_request("ws://origin.invalid/responses".into());
    request.proxy = Some(ProxyUrl::parse(&format!("http://synthetic:password@{address}")).unwrap());
    let mut connection = outside_tokio(connector.connect(&request, &runtime)).unwrap();
    outside_tokio(connection.send(WebSocketFrame::Text("through proxy".into()), &runtime)).unwrap();
    assert!(
        matches!(outside_tokio(connection.next(&runtime,None)).unwrap(),Some(WebSocketFrame::Text(s))if s=="ok")
    );
    connection.close();
    server.join().unwrap();
}
#[test]
fn websocket_idle_timeout_and_frame_limit_close_resources() {
    for limited in [false, true] {
        let (listener, address) = listener();
        let (tx, rx) = mpsc::channel();
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut ws = tungstenite::accept(stream).unwrap();
            if limited {
                ws.send(Message::Text("12345".into())).unwrap();
            }
            tx.send(ws.read().is_err()).unwrap();
        });
        let native = engine();
        let runtime = runtime(&native);
        let connector = NativeWebSocketConnector::new(native.handle().clone()).unwrap();
        let mut request = ws_request(format!("ws://{address}"));
        if limited {
            request.max_message_bytes = 4;
            request.max_frame_bytes = 4;
        }
        let mut connection = outside_tokio(connector.connect(&request, &runtime)).unwrap();
        let error =
            outside_tokio(connection.next(&runtime, Some(Duration::from_millis(50)))).unwrap_err();
        assert_eq!(
            error.kind(),
            if limited {
                TransportErrorKind::LimitExceeded
            } else {
                TransportErrorKind::Timeout
            }
        );
        assert!(rx.recv_timeout(Duration::from_secs(3)).unwrap());
        server.join().unwrap();
    }
}

fn tls_material() -> (
    Vec<u8>,
    Arc<rustls::ServerConfig>,
    Arc<rustls::ClientConfig>,
) {
    let key = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into(), "origin.invalid".into()])
        .unwrap();
    let cert = key.cert.der().clone();
    let server = rustls::ServerConfig::builder_with_provider(Arc::new(
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
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert.clone()).unwrap();
    let client = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth();
    (cert.to_vec(), Arc::new(server), Arc::new(client))
}
fn generic_head(stream: &mut impl Read) -> std::io::Result<String> {
    let mut bytes = Vec::new();
    let mut byte = [0];
    while !bytes.ends_with(b"\r\n\r\n") {
        stream.read_exact(&mut byte)?;
        bytes.push(byte[0]);
        assert!(bytes.len() < 16_384);
    }
    Ok(String::from_utf8(bytes).unwrap())
}

#[test]
fn https_verifies_certificates_and_explicit_private_roots_work() {
    for trusted in [false, true] {
        let (cert, server_tls, _) = tls_material();
        let (listener, address) = listener();
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut tls = rustls::StreamOwned::new(
                rustls::ServerConnection::new(server_tls).unwrap(),
                stream,
            );
            let result = generic_head(&mut tls);
            if trusted {
                assert!(result.unwrap().starts_with("POST /messages HTTP/1.1"));
                let mut bytes = [0; 5];
                tls.read_exact(&mut bytes).unwrap();
                tls.write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok",
                )
                .unwrap();
                tls.flush().unwrap();
            } else {
                assert!(result.is_err());
            }
        });
        let native = engine();
        let runtime = runtime(&native);
        let client = if trusted {
            NativeHttpClient::with_root_certificates(native.handle().clone(), &[cert]).unwrap()
        } else {
            NativeHttpClient::new(native.handle().clone())
        };
        let mut request = http_request(&address);
        request.endpoint =
            RequestEndpoint::new_resolved(format!("https://{address}/messages")).unwrap();
        let response = outside_tokio(client.send(&request, &runtime));
        if trusted {
            let mut response = response.unwrap();
            assert_eq!(
                outside_tokio(read_body_limited(response.body.as_mut(), &runtime, 10)).unwrap(),
                b"ok"
            );
        } else {
            assert_eq!(response.unwrap_err().kind(), TransportErrorKind::Connection);
        }
        server.join().unwrap();
    }
}

#[test]
#[expect(
    clippy::result_large_err,
    reason = "Tungstenite's required server callback returns its upstream HTTP response error by value"
)]
fn secure_websocket_through_https_proxy_preserves_both_tls_layers() {
    let (_, server_tls, client_tls) = tls_material();
    let (listener, address) = listener();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut proxy_tls = rustls::StreamOwned::new(
            rustls::ServerConnection::new(server_tls.clone()).unwrap(),
            stream,
        );
        assert!(
            generic_head(&mut proxy_tls)
                .unwrap()
                .starts_with("CONNECT origin.invalid:443 HTTP/1.1")
        );
        proxy_tls
            .write_all(b"HTTP/1.1 200 Connection established\r\n\r\n")
            .unwrap();
        proxy_tls.flush().unwrap();
        let origin_tls = rustls::StreamOwned::new(
            rustls::ServerConnection::new(server_tls).unwrap(),
            proxy_tls,
        );
        let mut ws = tungstenite::accept_hdr(
            origin_tls,
            |request: &tungstenite::handshake::server::Request, response| {
                assert!(!request.headers().contains_key("proxy-authorization"));
                Ok(response)
            },
        )
        .unwrap();
        assert_eq!(ws.read().unwrap().into_text().unwrap(), "nested TLS");
        ws.send(Message::Text("ok".into())).unwrap();
    });
    let native = engine();
    let runtime = runtime(&native);
    let connector = NativeWebSocketConnector::with_tls_config(native.handle().clone(), client_tls);
    let mut request = ws_request("wss://origin.invalid/responses".into());
    request.proxy =
        Some(ProxyUrl::parse(&format!("https://synthetic:password@{address}")).unwrap());
    let mut connection = outside_tokio(connector.connect(&request, &runtime)).unwrap();
    outside_tokio(connection.send(WebSocketFrame::Text("nested TLS".into()), &runtime)).unwrap();
    assert!(
        matches!(outside_tokio(connection.next(&runtime,None)).unwrap(),Some(WebSocketFrame::Text(s)) if s=="ok")
    );
    connection.close();
    server.join().unwrap();
}

#[test]
fn native_retry_wrapper_repeats_only_the_requested_number_of_attempts() {
    let (listener, address) = listener();
    let server = thread::spawn(move || {
        for retry in [true, false] {
            let (mut stream, _) = listener.accept().unwrap();
            read_head(&mut stream);
            let mut body = [0; 5];
            stream.read_exact(&mut body).unwrap();
            assert_eq!(&body, b"hello");
            if retry {
                stream.write_all(b"HTTP/1.1 429 Too Many Requests\r\nretry-after-ms: 0\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").unwrap();
            } else {
                stream
                    .write_all(
                        b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok",
                    )
                    .unwrap();
            }
        }
        listener.set_nonblocking(true).unwrap();
        assert!(listener.accept().is_err());
    });
    let native = engine();
    let runtime = runtime(&native);
    let transport = HttpTransport::new(
        Arc::new(NativeHttpClient::new(native.handle().clone())),
        Arc::new(NativeRetryJitter),
    );
    let request = http_request(&address);
    let mut response = outside_tokio(transport.send(
        &request,
        &runtime,
        RetryProfile::SharedProvider {
            max_retries: 1,
            max_retry_delay_ms: None,
        },
        TimeoutProfile::None,
    ))
    .unwrap();
    assert_eq!(
        outside_tokio(read_body_limited(response.body.as_mut(), &runtime, 10)).unwrap(),
        b"ok"
    );
    server.join().unwrap();
}

#[test]
fn stalled_websocket_handshake_timeout_releases_tcp_connection() {
    let (listener, address) = listener();
    let (tx, rx) = mpsc::channel();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        read_head(&mut stream);
        let mut byte = [0];
        tx.send(stream.read(&mut byte).unwrap_or(0) == 0).unwrap();
    });
    let native = engine();
    let runtime = runtime(&native);
    let connector = NativeWebSocketConnector::new(native.handle().clone()).unwrap();
    let mut request = ws_request(format!("ws://{address}"));
    request.connect_timeout = Some(Duration::from_millis(50));
    let error = outside_tokio(connector.connect(&request, &runtime))
        .err()
        .unwrap();
    assert_eq!(error.kind(), TransportErrorKind::Timeout);
    assert!(rx.recv_timeout(Duration::from_secs(3)).unwrap());
    server.join().unwrap();
}

#[test]
fn websocket_outgoing_exact_limit_and_overflow_preserve_connection() {
    let (listener, address) = listener();
    let (tx, rx) = mpsc::channel();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut socket = tungstenite::accept(stream).unwrap();
        assert_eq!(socket.read().unwrap().into_text().unwrap(), "1234");
        assert_eq!(socket.read().unwrap().into_text().unwrap(), "1234");
        tx.send(socket.read().is_err()).unwrap();
    });
    let native = engine();
    let runtime = runtime(&native);
    let connector = NativeWebSocketConnector::new(native.handle().clone()).unwrap();
    let mut request = ws_request(format!("ws://{address}"));
    request.max_message_bytes = 4;
    let mut connection = outside_tokio(connector.connect(&request, &runtime)).unwrap();
    outside_tokio(connection.send(WebSocketFrame::Text("1234".into()), &runtime)).unwrap();
    let error =
        outside_tokio(connection.send(WebSocketFrame::Text("12345".into()), &runtime)).unwrap_err();
    assert_eq!(error.kind(), TransportErrorKind::LimitExceeded);
    outside_tokio(connection.send(WebSocketFrame::Text("1234".into()), &runtime)).unwrap();
    connection.close();
    assert!(rx.recv_timeout(Duration::from_secs(3)).unwrap());
    server.join().unwrap();
}

#[test]
fn raw_response_headers_preserve_opaque_byte_values() {
    let (listener, address) = listener();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        read_head(&mut stream);
        stream.write_all(b"HTTP/1.1 200 OK\r\nX-Opaque: \xff\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").unwrap();
    });
    let native = engine();
    let runtime = runtime(&native);
    let client = NativeHttpClient::new(native.handle().clone());
    let request = http_request(&address);
    let response = outside_tokio(client.send(&request, &runtime)).unwrap();
    assert_eq!(response.headers.get_for_wire("x-opaque"), Some("\u{ff}"));
    server.join().unwrap();
}

#[test]
fn invalid_websocket_controls_preserve_connection_and_emit_no_bytes() {
    let mut invalid = vec![
        WebSocketFrame::Ping(vec![0; 126]),
        WebSocketFrame::Pong(vec![0; 126]),
        WebSocketFrame::Close {
            code: Some(1000),
            reason: "x".repeat(124),
        },
        WebSocketFrame::Close {
            code: Some(1000),
            reason: "é".repeat(62),
        },
        WebSocketFrame::Close {
            code: None,
            reason: "must not disappear".into(),
        },
    ];
    for code in [
        0,
        999,
        1004,
        1005,
        1006,
        1014,
        1015,
        1016,
        2999,
        5000,
        u16::MAX,
    ] {
        invalid.push(WebSocketFrame::Close {
            code: Some(code),
            reason: String::new(),
        });
    }
    let count = invalid.len();
    let (listener, address) = listener();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut socket = tungstenite::accept(stream).unwrap();
        for _ in 0..count {
            assert_eq!(socket.read().unwrap().into_text().unwrap(), "still usable");
            socket.send(Message::Text("ok".into())).unwrap();
        }
    });
    let native = engine();
    let runtime = runtime(&native);
    let connector = NativeWebSocketConnector::new(native.handle().clone()).unwrap();
    let request = ws_request(format!("ws://{address}"));
    let mut connection = outside_tokio(connector.connect(&request, &runtime)).unwrap();
    for frame in invalid {
        assert!(outside_tokio(connection.send(frame, &runtime)).is_err());
        outside_tokio(connection.send(WebSocketFrame::Text("still usable".into()), &runtime))
            .unwrap();
        assert!(
            matches!(outside_tokio(connection.next(&runtime,None)).unwrap(),Some(WebSocketFrame::Text(s))if s=="ok")
        );
    }
    connection.close();
    server.join().unwrap();
}

#[test]
fn exact_control_frame_boundaries_are_valid_on_wire() {
    let (listener, address) = listener();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut socket = tungstenite::accept(stream).unwrap();
        assert!(matches!(socket.read().unwrap(),Message::Ping(b)if b.len()==125));
        assert!(matches!(socket.read().unwrap(),Message::Pong(b)if b.len()==125));
        assert!(
            matches!(socket.read().unwrap(),Message::Close(Some(c))if c.reason.len()==123&&u16::from(c.code)==1000)
        );
    });
    let native = engine();
    let runtime = runtime(&native);
    let connector = NativeWebSocketConnector::new(native.handle().clone()).unwrap();
    let request = ws_request(format!("ws://{address}"));
    let mut connection = outside_tokio(connector.connect(&request, &runtime)).unwrap();
    outside_tokio(connection.send(WebSocketFrame::Ping(vec![0; 125]), &runtime)).unwrap();
    outside_tokio(connection.send(WebSocketFrame::Pong(vec![0; 125]), &runtime)).unwrap();
    assert!(
        matches!(outside_tokio(connection.next(&runtime,Some(Duration::from_secs(2)))).unwrap(),Some(WebSocketFrame::Pong(b))if b.len()==125)
    );
    outside_tokio(connection.send(
        WebSocketFrame::Close {
            code: Some(1000),
            reason: format!("{}x", "é".repeat(61)),
        },
        &runtime,
    ))
    .unwrap();
    server.join().unwrap();
}

#[test]
fn peer_close_is_replied_to_before_physical_release() {
    for code in [
        None,
        Some(tungstenite::protocol::frame::coding::CloseCode::Normal),
    ] {
        let (listener, address) = listener();
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut socket = tungstenite::accept(stream).unwrap();
            socket
                .send(Message::Close(code.map(|code| {
                    tungstenite::protocol::CloseFrame {
                        code,
                        reason: "done".into(),
                    }
                })))
                .unwrap();
            assert!(matches!(socket.read().unwrap(), Message::Close(_)));
        });
        let native = engine();
        let runtime = runtime(&native);
        let connector = NativeWebSocketConnector::new(native.handle().clone()).unwrap();
        let request = ws_request(format!("ws://{address}"));
        let mut connection = outside_tokio(connector.connect(&request, &runtime)).unwrap();
        assert!(matches!(
            outside_tokio(connection.next(&runtime, Some(Duration::from_secs(2)))).unwrap(),
            Some(WebSocketFrame::Close { .. })
        ));
        assert!(
            outside_tokio(connection.next(&runtime, None))
                .unwrap()
                .is_none()
        );
        server.join().unwrap();
    }
}

#[test]
fn http_request_header_uses_fetch_byte_string_projection() {
    let (listener, address) = listener();
    let (tx, rx) = mpsc::channel();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut head = Vec::new();
        let mut byte = [0];
        while !head.ends_with(b"\r\n\r\n") {
            stream.read_exact(&mut byte).unwrap();
            head.push(byte[0]);
        }
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .unwrap();
        tx.send(head).unwrap();
    });
    let native = engine();
    let runtime = runtime(&native);
    let client = NativeHttpClient::new(native.handle().clone());
    let mut request = http_request(&address);
    request.headers.set("x-latin", "é").unwrap();
    outside_tokio(client.send(&request, &runtime)).unwrap();
    let head = rx.recv_timeout(Duration::from_secs(2)).unwrap();
    assert!(
        head.windows(b"x-latin: \xe9\r\n".len())
            .any(|w| w == b"x-latin: \xe9\r\n")
    );
    server.join().unwrap();
}

#[test]
#[expect(
    clippy::result_large_err,
    reason = "Tungstenite server callback requires its HTTP response error by value"
)]
fn websocket_request_header_uses_fetch_byte_string_projection() {
    let (listener, address) = listener();
    let (tx, rx) = mpsc::channel();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let _socket = tungstenite::accept_hdr(
            stream,
            move |request: &tungstenite::handshake::server::Request, response| {
                tx.send(request.headers()["x-latin"].as_bytes().to_vec())
                    .unwrap();
                Ok(response)
            },
        )
        .unwrap();
    });
    let native = engine();
    let runtime = runtime(&native);
    let connector = NativeWebSocketConnector::new(native.handle().clone()).unwrap();
    let mut request = ws_request(format!("ws://{address}"));
    request.headers.set("x-latin", "é").unwrap();
    outside_tokio(connector.connect(&request, &runtime)).unwrap();
    assert_eq!(rx.recv_timeout(Duration::from_secs(2)).unwrap(), [0xe9]);
    server.join().unwrap();
}

#[test]
fn headers_above_latin1_are_rejected_before_connect() {
    let (listener, address) = listener();
    listener.set_nonblocking(true).unwrap();
    let native = engine();
    let runtime = runtime(&native);
    let mut request = http_request(&address);
    request.headers.set("x-invalid", "Ā").unwrap();
    let client = NativeHttpClient::new(native.handle().clone());
    let error = outside_tokio(client.send(&request, &runtime)).unwrap_err();
    assert_eq!(error.kind(), TransportErrorKind::InvalidRequest);
    assert!(listener.accept().is_err());
    let connector = NativeWebSocketConnector::new(native.handle().clone()).unwrap();
    let mut request = ws_request(format!("ws://{address}"));
    request.headers.set("x-invalid", "Ā").unwrap();
    let error = outside_tokio(connector.connect(&request, &runtime))
        .err()
        .unwrap();
    assert_eq!(error.kind(), TransportErrorKind::InvalidRequest);
    assert!(listener.accept().is_err());
    assert!(!format!("{error:?} {error}").contains('Ā'));
}

#[test]
fn http1_status_text_preserves_canonical_custom_empty_and_unknown_phrases() {
    for (version, status, phrase) in [
        ("HTTP/1.1", 429, "Too Many Requests"),
        ("HTTP/1.1", 429, ""),
        ("HTTP/1.1", 429, "Custom throttle"),
        ("HTTP/1.0", 200, "STATUS_TEXT_SYNTHETIC_SECRET"),
        ("HTTP/1.1", 599, "Unknown custom"),
        ("HTTP/1.1", 599, ""),
    ] {
        let (listener, address) = listener();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            read_head(&mut stream);
            let mut body = [0; 5];
            stream.read_exact(&mut body).unwrap();
            stream.write_all(format!("{version} {status} {phrase}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").as_bytes()).unwrap();
        });
        let native = engine();
        let runtime = runtime(&native);
        let client = NativeHttpClient::new(native.handle().clone());
        let request = http_request(&address);
        let response = outside_tokio(client.send(&request, &runtime)).unwrap();
        assert_eq!(response.status, status);
        assert_eq!(response.status_text, phrase);
        assert!(!format!("{response:?}").contains("STATUS_TEXT_SYNTHETIC_SECRET"));
        drop(response);
        server.join().unwrap();
    }
}

#[test]
fn http2_status_text_is_empty_after_real_alpn_negotiation() {
    let native = engine();
    let (cert, server_tls, _) = tls_material();
    let mut server_tls = (*server_tls).clone();
    server_tls.alpn_protocols = vec![b"h2".to_vec()];
    let listener = native
        .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
        .unwrap();
    let address = listener.local_addr().unwrap();
    let server = native.spawn(async move {
        tokio::time::timeout(Duration::from_secs(5), async move {
            let (stream, _) = listener.accept().await.unwrap();
            let stream = tokio_rustls::TlsAcceptor::from(Arc::new(server_tls))
                .accept(stream)
                .await
                .unwrap();
            assert_eq!(stream.get_ref().1.alpn_protocol(), Some(b"h2".as_slice()));
            let mut connection = h2::server::handshake(stream).await.unwrap();
            let (request, mut respond) = connection.accept().await.unwrap().unwrap();
            assert_eq!(request.version(), tungstenite::http::Version::HTTP_2);
            drop(request);
            respond
                .send_response(
                    tungstenite::http::Response::builder()
                        .status(429)
                        .body(())
                        .unwrap(),
                    true,
                )
                .unwrap();
            // Drive the queued response until the client releases its connection.
            assert!(
                connection
                    .accept()
                    .await
                    .is_none_or(|result| result.is_err()),
                "unexpected second request"
            );
        })
        .await
        .unwrap();
    });
    let runtime = runtime(&native);
    let client =
        NativeHttpClient::with_root_certificates(native.handle().clone(), &[cert]).unwrap();
    let mut request = http_request(&address.to_string());
    request.endpoint =
        RequestEndpoint::new_resolved(format!("https://{address}/messages")).unwrap();
    request.body.clear();
    let response = outside_tokio(client.send(&request, &runtime)).unwrap();
    assert_eq!(response.status, 429);
    assert_eq!(response.status_text, "");
    drop(response);
    native.block_on(server).unwrap();
}

#[test]
fn http1_status_text_recovers_raw_high_bytes_utf8_and_informational_heads() {
    for phrase in [
        b"Rate limited \xe9\xff".as_slice(),
        b"Rate limited \xc3\xa9",
        b"\xef\xbb\xbfRate limited",
    ] {
        for informational in [false, true] {
            let (listener, address) = listener();
            let wire_phrase = phrase.to_vec();
            let server = thread::spawn(move || {
                let (mut socket, _) = listener.accept().unwrap();
                read_head(&mut socket);
                let mut body = [0; 5];
                socket.read_exact(&mut body).unwrap();
                let mut bytes = Vec::new();
                if informational {
                    bytes.extend_from_slice(
                        b"HTTP/1.1 103 Early Hints\r\nLink: </synthetic>\r\n\r\n",
                    );
                }
                bytes.extend_from_slice(b"HTTP/1.1 429 ");
                bytes.extend_from_slice(&wire_phrase);
                bytes.extend_from_slice(b"\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok");
                for byte in bytes {
                    socket.write_all(&[byte]).unwrap();
                }
            });
            let native = engine();
            let runtime = runtime(&native);
            let client = NativeHttpClient::new(native.handle().clone());
            let request = http_request(&address);
            let mut response = outside_tokio(client.send(&request, &runtime)).unwrap();
            assert_eq!(response.status, 429);
            assert_eq!(
                response.status_text,
                phrase.iter().copied().map(char::from).collect::<String>()
            );
            assert_eq!(
                outside_tokio(read_body_limited(response.body.as_mut(), &runtime, 10)).unwrap(),
                b"ok"
            );
            server.join().unwrap();
        }
    }
}

#[test]
fn raw_response_head_budget_is_exact_and_does_not_count_body() {
    let head = b"HTTP/1.1 200 Bounded\r\nContent-Length: 2048\r\nConnection: close\r\n\r\n";
    for exact in [false, true] {
        let (listener, address) = listener();
        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            read_head(&mut socket);
            let mut request_body = [0; 5];
            socket.read_exact(&mut request_body).unwrap();
            let mut response = head.to_vec();
            response.extend_from_slice(&vec![b'x'; 2048]);
            socket.write_all(&response).unwrap();
        });
        let native = engine();
        let runtime = runtime(&native);
        let client = NativeHttpClient::new(native.handle().clone())
            .with_response_head_limit(head.len() - usize::from(!exact));
        let request = http_request(&address);
        let result = outside_tokio(client.send(&request, &runtime));
        if exact {
            let mut response = result.unwrap();
            assert_eq!(response.status_text, "Bounded");
            assert_eq!(
                outside_tokio(read_body_limited(response.body.as_mut(), &runtime, 2048)).unwrap(),
                vec![b'x'; 2048]
            );
        } else {
            assert_eq!(
                result.unwrap_err().kind(),
                TransportErrorKind::LimitExceeded
            );
        }
        server.join().unwrap();
    }
}

#[test]
fn http_status_observation_survives_https_connect_proxy_and_both_tls_layers() {
    let (cert, server_tls, _) = tls_material();
    let (listener, address) = listener();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut proxy = rustls::StreamOwned::new(
            rustls::ServerConnection::new(server_tls.clone()).unwrap(),
            stream,
        );
        let head = generic_head(&mut proxy).unwrap();
        assert!(head.starts_with("CONNECT origin.invalid:443 HTTP/1.1"));
        assert!(head.contains("Proxy-Authorization: Basic "));
        proxy
            .write_all(b"HTTP/1.1 200 Connection established\r\n\r\n")
            .unwrap();
        proxy.flush().unwrap();
        let mut origin =
            rustls::StreamOwned::new(rustls::ServerConnection::new(server_tls).unwrap(), proxy);
        let head = generic_head(&mut origin).unwrap();
        assert!(head.starts_with("POST /messages HTTP/1.1"));
        assert!(!head.to_ascii_lowercase().contains("proxy-authorization"));
        let mut body = [0; 5];
        origin.read_exact(&mut body).unwrap();
        origin.write_all(b"HTTP/1.1 429 Rate limited \xe9\xff\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok").unwrap();
        origin.flush().unwrap();
    });
    let native = engine();
    let runtime = runtime(&native);
    let client =
        NativeHttpClient::with_root_certificates(native.handle().clone(), &[cert]).unwrap();
    let mut request = http_request("origin.invalid");
    request.endpoint = RequestEndpoint::new_resolved("https://origin.invalid/messages").unwrap();
    request.proxy =
        Some(ProxyUrl::parse(&format!("https://synthetic:password@{address}")).unwrap());
    let mut response = outside_tokio(client.send(&request, &runtime)).unwrap();
    assert_eq!(response.status_text, "Rate limited éÿ");
    assert_eq!(
        outside_tokio(read_body_limited(response.body.as_mut(), &runtime, 10)).unwrap(),
        b"ok"
    );
    server.join().unwrap();
}

#[test]
fn cancellation_releases_socket_during_partial_raw_head_observation() {
    let (listener, address) = listener();
    let (tx, rx) = mpsc::channel();
    let server = thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        read_head(&mut socket);
        let mut body = [0; 5];
        socket.read_exact(&mut body).unwrap();
        socket.write_all(b"HTTP/1.1 429 Rate limited ").unwrap();
        tx.send(()).unwrap();
        let mut byte = [0];
        assert_eq!(socket.read(&mut byte).unwrap_or(0), 0);
    });
    let native = engine();
    let runtime = runtime(&native);
    let token = runtime.cancellation.clone();
    let canceller = thread::spawn(move || {
        rx.recv_timeout(Duration::from_secs(3)).unwrap();
        token.cancel();
    });
    let client = NativeHttpClient::new(native.handle().clone());
    let request = http_request(&address);
    let error = outside_tokio(client.send(&request, &runtime)).unwrap_err();
    assert_eq!(error.kind(), TransportErrorKind::Cancelled);
    server.join().unwrap();
    canceller.join().unwrap();
}

#[test]
fn leading_empty_lines_preserve_status_and_cannot_bypass_head_budget() {
    for prefix in [b"\r\n".as_slice(), b"\n", b"\r\n\n\r\n"] {
        for informational in [false, true] {
            let mut head = prefix.to_vec();
            if informational {
                head.extend_from_slice(b"HTTP/1.1 103 Early\r\n\r\n");
                head.extend_from_slice(prefix);
            }
            head.extend_from_slice(
                b"HTTP/1.1 200 Custom\r\nContent-Length: 2\r\nConnection: close\r\n\r\n",
            );
            for limit in [2, head.len() - 1, head.len()] {
                let (listener, address) = listener();
                let server_head = head.clone();
                let server = thread::spawn(move || {
                    let (mut socket, _) = listener.accept().unwrap();
                    read_head(&mut socket);
                    let mut body = [0; 5];
                    socket.read_exact(&mut body).unwrap();
                    let mut bytes = server_head;
                    bytes.extend_from_slice(b"ok");
                    socket.write_all(&bytes).unwrap();
                });
                let native = engine();
                let runtime = runtime(&native);
                let client =
                    NativeHttpClient::new(native.handle().clone()).with_response_head_limit(limit);
                let request = http_request(&address);
                let result = outside_tokio(client.send(&request, &runtime));
                if limit == head.len() {
                    let mut response = result.unwrap();
                    assert_eq!(response.status, 200);
                    assert_eq!(response.status_text, "Custom");
                    assert_eq!(
                        outside_tokio(read_body_limited(response.body.as_mut(), &runtime, 2))
                            .unwrap(),
                        b"ok"
                    );
                } else {
                    assert_eq!(
                        result.unwrap_err().kind(),
                        TransportErrorKind::LimitExceeded
                    );
                }
                server.join().unwrap();
            }
        }
    }
}
