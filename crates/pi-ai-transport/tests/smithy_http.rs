use pi_ai_core::{
    BoxFuture, CancellationToken, Cancelled, Clock, RequestEndpoint, RequestRuntime, SpawnError,
    TaskSpawner,
};
use pi_ai_transport::*;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
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
        panic!("no implicit Smithy timeout")
    }
}
impl TaskSpawner for Host {
    fn spawn(&self, _: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
        Err(SpawnError::new())
    }
}
impl RetryJitter for Host {
    fn sample(&self) -> f64 {
        panic!("no Smithy retry")
    }
}
fn runtime() -> RequestRuntime {
    RequestRuntime::with_noop_telemetry(CancellationToken::new(), Arc::new(Host), Arc::new(Host))
}
fn request(url: String) -> HttpRequest {
    HttpRequest {
        endpoint: RequestEndpoint::new_resolved(url).unwrap(),
        method: "POST".into(),
        headers: HttpHeaders::new(),
        body: b"{}".to_vec(),
        proxy: None,
    }
}
struct Raw(AtomicUsize);
impl RawHttpClient for Raw {
    fn send<'a>(
        &'a self,
        _: &'a HttpRequest,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<HttpResponse, TransportError>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Err(TransportError::new(TransportErrorKind::Connection)) })
    }
}
struct PendingSmithy {
    calls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
}
struct PendingGuard(Arc<AtomicUsize>);
impl Drop for PendingGuard {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
impl RawHttpClient for PendingSmithy {
    fn send<'a>(
        &'a self,
        _: &'a HttpRequest,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<HttpResponse, TransportError>> {
        panic!("not generic send")
    }
    fn send_smithy<'a>(
        &'a self,
        _: &'a HttpRequest,
        _: &'a RequestRuntime,
        _: &'a SmithyRequestProfile,
    ) -> BoxFuture<'a, Result<HttpResponse, TransportError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let guard = PendingGuard(self.drops.clone());
        Box::pin(async move {
            let _guard = guard;
            std::future::pending().await
        })
    }
}
#[test]
fn already_cancelled_skips_even_port_construction_and_blocked_attempt_is_dropped() {
    for before in [false, true] {
        let calls = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let transport = HttpTransport::new(
            Arc::new(PendingSmithy {
                calls: calls.clone(),
                drops: drops.clone(),
            }),
            Arc::new(Host),
        );
        let runtime = runtime();
        if before {
            runtime.cancellation.cancel();
        }
        let req = request("http://unused.invalid".into());
        let profile = SmithyRequestProfile::Http1 {
            body_admission: SmithyBodyAdmission::Immediate,
            connection: SmithyH1Connection::KeepAlive,
        };
        let mut op = transport.send_smithy(&req, &runtime, &profile);
        if !before {
            assert!(
                op.as_mut()
                    .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
                    .is_pending()
            );
            runtime.cancellation.cancel();
        }
        assert!(
            matches!(op.as_mut().poll(&mut std::task::Context::from_waker(std::task::Waker::noop())),std::task::Poll::Ready(Err(error)) if error.kind()==TransportErrorKind::Cancelled)
        );
        assert_eq!(calls.load(Ordering::SeqCst), usize::from(!before));
        assert_eq!(drops.load(Ordering::SeqCst), usize::from(!before));
    }
}
#[test]
fn unsupported_profile_never_delegates_and_cancellation_wins_before_default_error() {
    let raw = Arc::new(Raw(AtomicUsize::new(0)));
    let transport = HttpTransport::new(raw.clone(), Arc::new(Host));
    let runtime = runtime();
    let req = request("http://unused.invalid".into());
    let mut op = transport.send_smithy(
        &req,
        &runtime,
        &SmithyRequestProfile::Http1 {
            body_admission: SmithyBodyAdmission::Immediate,
            connection: SmithyH1Connection::KeepAlive,
        },
    );
    let result = op
        .as_mut()
        .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()));
    assert!(
        matches!(result,std::task::Poll::Ready(Err(e)) if e.kind()==TransportErrorKind::UnsupportedProfile)
    );
    runtime.cancellation.cancel();
    let mut op = transport.send_smithy(
        &req,
        &runtime,
        &SmithyRequestProfile::Http1 {
            body_admission: SmithyBodyAdmission::Immediate,
            connection: SmithyH1Connection::KeepAlive,
        },
    );
    assert!(
        matches!(op.as_mut().poll(&mut std::task::Context::from_waker(std::task::Waker::noop())),std::task::Poll::Ready(Err(e)) if e.kind()==TransportErrorKind::Cancelled)
    );
    assert_eq!(raw.0.load(Ordering::SeqCst), 0);
}

#[cfg(feature = "native")]
mod native {
    use super::*;
    use hyper::{Response, body::Bytes};
    use std::{
        io::{Read, Write},
        net::{TcpListener, TcpStream},
        thread,
    };
    fn engine() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap()
    }
    fn rt(engine: &tokio::runtime::Runtime) -> RequestRuntime {
        RequestRuntime::with_noop_telemetry(
            CancellationToken::new(),
            Arc::new(TokioClock::new(engine.handle().clone())),
            Arc::new(Host),
        )
    }
    fn head(socket: &mut TcpStream) -> String {
        socket
            .set_read_timeout(Some(Duration::from_secs(4)))
            .unwrap();
        let mut out = vec![];
        while !out.ends_with(b"\r\n\r\n") {
            let mut b = [0];
            socket.read_exact(&mut b).unwrap();
            out.push(b[0]);
            assert!(out.len() < 16384);
        }
        String::from_utf8(out).unwrap()
    }
    fn closed(socket: &mut TcpStream) {
        match socket.read(&mut [0]) {
            Ok(0) => {}
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
                ) => {}
            other => panic!("not released: {other:?}"),
        }
    }
    fn fields() -> Vec<(&'static str, &'static str)> {
        vec![
            ("x-z", "one"),
            ("set-cookie", "a=1"),
            ("x-a", "two"),
            ("set-cookie", "b=2"),
            ("x-z", "three"),
            ("cookie", "a=1"),
            ("cookie", "b=2"),
            ("x-byte", "\u{a0}kept\u{a0}"),
        ]
    }
    fn profile(
        h2: bool,
        authority: String,
        admission: SmithyBodyAdmission,
        connection: SmithyH1Connection,
    ) -> SmithyRequestProfile {
        if h2 {
            SmithyRequestProfile::Http2 {
                pseudo_headers: SmithyH2PseudoHeaders {
                    authority,
                    scheme: None,
                },
                body_admission: admission,
            }
        } else {
            SmithyRequestProfile::Http1 {
                body_admission: admission,
                connection,
            }
        }
    }
    #[test]
    fn source_record_selects_independent_body_and_connection_controls() {
        let source: serde_json::Value = serde_json::from_str(include_str!(
            "../../../conformance/ai-transport/smithy-http/trace.json"
        ))
        .unwrap();
        for case in source["expect"].as_array().unwrap() {
            let h2 = case["protocol"] == "h2";
            let immediate = case["admittedBeforeContinue"].as_bool().unwrap();
            let expected_connection = case["connection"]
                .as_str()
                .unwrap_or("keep-alive")
                .to_owned();
            let e = engine();
            let runtime = rt(&e);
            e.block_on(async {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let address = listener.local_addr().unwrap();
                let expected = expected_connection.clone();
                let server = tokio::spawn(async move {
                    let (mut socket, _) = listener.accept().await.unwrap();
                    if h2 {
                        let mut conn = h2::server::handshake(socket).await.unwrap();
                        let (req, mut send) = conn.accept().await.unwrap().unwrap();
                        let driver =
                            tokio::spawn(async move { while conn.accept().await.is_some() {} });
                        let mut body = req.into_body();
                        if !immediate {
                            assert!(
                                tokio::time::timeout(Duration::from_millis(20), body.data())
                                    .await
                                    .is_err()
                            );
                            send.send_informational(
                                Response::builder().status(100).body(()).unwrap(),
                            )
                            .unwrap();
                        }
                        assert_eq!(&body.data().await.unwrap().unwrap()[..], b"{}");
                        let response = send
                            .send_response(Response::builder().status(200).body(()).unwrap(), true)
                            .unwrap();
                        drop(response);
                        drop(send);
                        drop(body);
                        driver.await.unwrap();
                    } else {
                        use tokio::io::{AsyncReadExt, AsyncWriteExt};
                        let mut head = vec![];
                        while !head.ends_with(b"\r\n\r\n") {
                            let mut b = [0];
                            socket.read_exact(&mut b).await.unwrap();
                            head.push(b[0]);
                        }
                        let head = String::from_utf8(head).unwrap();
                        assert!(head.contains(&format!("connection: {expected}\r\n")));
                        let mut bytes = [0; 2];
                        if !immediate {
                            assert!(
                                tokio::time::timeout(
                                    Duration::from_millis(20),
                                    socket.read_exact(&mut bytes)
                                )
                                .await
                                .is_err()
                            );
                            socket
                                .write_all(b"HTTP/1.1 100 Continue\r\n\r\n")
                                .await
                                .unwrap();
                        }
                        socket.read_exact(&mut bytes).await.unwrap();
                        assert_eq!(&bytes, b"{}");
                        socket
                            .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
                            .await
                            .unwrap();
                        assert_eq!(socket.read(&mut [0]).await.unwrap_or(0), 0);
                    }
                });
                let mut req = request(format!("http://{address}/"));
                if case["name"] == "dual" {
                    req.headers.append("Expect", "").unwrap();
                }
                req.headers
                    .append(
                        if case["name"] == "eXpEcT" {
                            "eXpEcT"
                        } else {
                            "expect"
                        },
                        "100-continue",
                    )
                    .unwrap();
                let profile = profile(
                    h2,
                    address.to_string(),
                    if immediate {
                        SmithyBodyAdmission::Immediate
                    } else {
                        SmithyBodyAdmission::WaitForContinue
                    },
                    if expected_connection == "close" {
                        SmithyH1Connection::Close
                    } else {
                        SmithyH1Connection::KeepAlive
                    },
                );
                let client = NativeHttpClient::new(e.handle().clone());
                let mut response = tokio::time::timeout(
                    Duration::from_secs(3),
                    client.send_smithy(&req, &runtime, &profile),
                )
                .await
                .unwrap()
                .unwrap();
                response.body.close();
                drop(response);
                tokio::time::timeout(Duration::from_secs(3), server)
                    .await
                    .unwrap()
                    .unwrap();
            });
        }
    }

    struct GraceClock {
        entered: tokio::sync::Notify,
        release: tokio::sync::Notify,
        calls: AtomicUsize,
    }
    impl Clock for GraceClock {
        fn now_unix_ms(&self) -> u64 {
            0
        }
        fn sleep<'a>(
            &'a self,
            d: Duration,
            _: &'a CancellationToken,
        ) -> BoxFuture<'a, Result<(), Cancelled>> {
            assert_eq!(d, Duration::from_millis(6000));
            self.calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                self.entered.notify_one();
                self.release.notified().await;
                Ok(())
            })
        }
    }
    #[test]
    fn no_continue_uses_only_explicit_6000ms_grace_and_then_sends() {
        for h2 in [false, true] {
            let e = engine();
            let clock = Arc::new(GraceClock {
                entered: tokio::sync::Notify::new(),
                release: tokio::sync::Notify::new(),
                calls: AtomicUsize::new(0),
            });
            let runtime = RequestRuntime::with_noop_telemetry(
                CancellationToken::new(),
                clock.clone(),
                Arc::new(Host),
            );
            e.block_on(async {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let address = listener.local_addr().unwrap();
                let (seen_tx, mut seen_rx) = tokio::sync::oneshot::channel();
                let server = tokio::spawn(async move {
                    let (mut socket, _) = listener.accept().await.unwrap();
                    if h2 {
                        let mut conn = h2::server::handshake(socket).await.unwrap();
                        let (req, mut send) = conn.accept().await.unwrap().unwrap();
                        let driver =
                            tokio::spawn(async move { while conn.accept().await.is_some() {} });
                        seen_tx.send(()).unwrap();
                        let mut body = req.into_body();
                        assert_eq!(&body.data().await.unwrap().unwrap()[..], b"{}");
                        let sent = send
                            .send_response(Response::builder().status(200).body(()).unwrap(), true)
                            .unwrap();
                        drop(sent);
                        drop(send);
                        drop(body);
                        driver.await.unwrap();
                    } else {
                        use tokio::io::{AsyncReadExt, AsyncWriteExt};
                        let mut head = vec![];
                        while !head.ends_with(b"\r\n\r\n") {
                            let mut b = [0];
                            socket.read_exact(&mut b).await.unwrap();
                            head.push(b[0]);
                        }
                        seen_tx.send(()).unwrap();
                        let mut b = [0; 2];
                        socket.read_exact(&mut b).await.unwrap();
                        assert_eq!(&b, b"{}");
                        socket
                            .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
                            .await
                            .unwrap();
                        assert_eq!(socket.read(&mut [0]).await.unwrap_or(0), 0);
                    }
                });
                let mut req = request(format!("http://{address}/"));
                req.headers.set("expect", "100-continue").unwrap();
                let profile = profile(
                    h2,
                    address.to_string(),
                    SmithyBodyAdmission::WaitForContinue,
                    SmithyH1Connection::Close,
                );
                let client = NativeHttpClient::new(e.handle().clone());
                let mut send = client.send_smithy(&req, &runtime, &profile);
                std::future::poll_fn(|cx| {
                    assert!(send.as_mut().poll(cx).is_pending());
                    std::pin::Pin::new(&mut seen_rx).poll(cx)
                })
                .await
                .unwrap();
                let mut entered = Box::pin(clock.entered.notified());
                std::future::poll_fn(|cx| {
                    assert!(send.as_mut().poll(cx).is_pending());
                    entered.as_mut().poll(cx)
                })
                .await;
                assert_eq!(clock.calls.load(Ordering::SeqCst), 1);
                clock.release.notify_one();
                let mut response = tokio::time::timeout(Duration::from_secs(3), send)
                    .await
                    .unwrap()
                    .unwrap();
                response.body.close();
                drop(response);
                server.await.unwrap();
            });
        }
    }

    #[test]
    fn cancellation_and_pending_operation_drop_release_h1_and_h2() {
        for h2 in [false, true] {
            for after_head in [false, true] {
                for cancel in [false, true] {
                    let e = engine();
                    let runtime = rt(&e);
                    e.block_on(async {
                        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                        let address = listener.local_addr().unwrap();
                        let (seen_tx, mut seen_rx) = tokio::sync::oneshot::channel();
                        let server = tokio::spawn(async move {
                            let (mut socket, _) = listener.accept().await.unwrap();
                            if h2 {
                                let mut conn = h2::server::handshake(socket).await.unwrap();
                                let (req, mut send) = conn.accept().await.unwrap().unwrap();
                                let held = if after_head {
                                    Some(
                                        send.send_response(
                                            Response::builder().status(200).body(()).unwrap(),
                                            false,
                                        )
                                        .unwrap(),
                                    )
                                } else {
                                    None
                                };
                                seen_tx.send(()).unwrap();
                                tokio::time::timeout(Duration::from_secs(3), async {
                                    while conn.accept().await.is_some() {}
                                })
                                .await
                                .unwrap();
                                drop(held);
                                drop(send);
                                drop(req);
                            } else {
                                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                                let mut head = vec![];
                                while !head.ends_with(b"\r\n\r\n") {
                                    let mut b = [0];
                                    socket.read_exact(&mut b).await.unwrap();
                                    head.push(b[0]);
                                }
                                if after_head {
                                    socket
                                        .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 9\r\n\r\n")
                                        .await
                                        .unwrap();
                                }
                                seen_tx.send(()).unwrap();
                                let mut bytes = [0; 32];
                                tokio::time::timeout(Duration::from_secs(3), async {
                                    loop {
                                        match socket.read(&mut bytes).await {
                                            Ok(0) | Err(_) => break,
                                            Ok(_) => {}
                                        }
                                    }
                                })
                                .await
                                .unwrap();
                            }
                        });
                        let transport = HttpTransport::new(
                            Arc::new(NativeHttpClient::new(e.handle().clone())),
                            Arc::new(Host),
                        );
                        let req = request(format!("http://{address}/"));
                        let profile = if h2 {
                            SmithyRequestProfile::Http2 {
                                pseudo_headers: SmithyH2PseudoHeaders {
                                    authority: address.to_string(),
                                    scheme: None,
                                },
                                body_admission: SmithyBodyAdmission::Immediate,
                            }
                        } else {
                            SmithyRequestProfile::Http1 {
                                body_admission: SmithyBodyAdmission::Immediate,
                                connection: SmithyH1Connection::KeepAlive,
                            }
                        };
                        if after_head {
                            let mut response = transport
                                .send_smithy(&req, &runtime, &profile)
                                .await
                                .unwrap();
                            seen_rx.await.unwrap();
                            let mut read = response.body.next(&runtime);
                            assert!(
                                read.as_mut()
                                    .poll(&mut std::task::Context::from_waker(
                                        std::task::Waker::noop()
                                    ))
                                    .is_pending()
                            );
                            if cancel {
                                runtime.cancellation.cancel();
                                assert_eq!(
                                    read.await.unwrap_err().kind(),
                                    TransportErrorKind::Cancelled
                                );
                            } else {
                                drop(read);
                            }
                            drop(response);
                        } else {
                            let mut pending = transport.send_smithy(&req, &runtime, &profile);
                            std::future::poll_fn(|cx| {
                                assert!(pending.as_mut().poll(cx).is_pending());
                                std::pin::Pin::new(&mut seen_rx).poll(cx)
                            })
                            .await
                            .unwrap();
                            if cancel {
                                runtime.cancellation.cancel();
                                assert_eq!(
                                    pending.await.unwrap_err().kind(),
                                    TransportErrorKind::Cancelled
                                );
                            } else {
                                drop(pending);
                            }
                        }
                        server.await.unwrap();
                    });
                }
            }
        }
    }
    #[test]
    fn h1_forced_request_aliases_no_accept_and_ordered_raw_fields() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            let head = head(&mut socket).to_ascii_lowercase();
            assert!(!head.contains("\r\naccept:"));
            assert!(head.contains("connection: close\r\n"));
            assert!(head.contains("x-alias: upper\r\n"));
            assert!(!head.contains("x-alias: lower"));
            let mut body = [0; 2];
            socket.read_exact(&mut body).unwrap();
            assert_eq!(&body, b"{}");
            socket.write_all(b"HTTP/1.1 200 Fine\r\nx-z: one \t\r\nset-cookie: a=1\r\nx-a: two\r\nset-cookie: b=2\r\nx-z: three\r\ncookie: a=1\r\ncookie: b=2\r\nx-byte: \xa0kept\xa0\r\ncontent-length: 2\r\n\r\nok").unwrap();
            closed(&mut socket);
        });
        let e = engine();
        let runtime = rt(&e);
        let client = NativeHttpClient::new(e.handle().clone());
        let mut req = request(format!("http://{address}/test"));
        req.headers.append("x-alias", "lower").unwrap();
        req.headers.append("X-Alias", "upper").unwrap();
        req.headers.set("Connection", "close").unwrap();
        e.block_on(async {
            let mut response = client
                .send_smithy(
                    &req,
                    &runtime,
                    &SmithyRequestProfile::Http1 {
                        body_admission: SmithyBodyAdmission::Immediate,
                        connection: SmithyH1Connection::KeepAlive,
                    },
                )
                .await
                .unwrap();
            let mut expected = fields();
            expected.push(("content-length", "2"));
            assert_eq!(
                response.headers.iter_for_wire().collect::<Vec<_>>(),
                expected
            );
            assert_eq!(
                read_body_limited(response.body.as_mut(), &runtime, 16)
                    .await
                    .unwrap(),
                b"ok"
            );
        });
        server.join().unwrap();
    }
    #[test]
    fn source_rejected_lf_and_folded_h1_fields_never_publish_a_head() {
        let source: serde_json::Value = serde_json::from_str(include_str!(
            "../../../conformance/ai-transport/smithy-http/trace.json"
        ))
        .unwrap();
        for case in source["admission"].as_array().unwrap() {
            let wire = case["head"].as_str().unwrap().to_owned();
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let server = thread::spawn(move || {
                let (mut socket, _) = listener.accept().unwrap();
                let _ = head(&mut socket);
                socket.write_all(wire.as_bytes()).unwrap();
                socket.shutdown(std::net::Shutdown::Write).unwrap();
                let mut bytes = [0; 1024];
                while let Ok(n) = socket.read(&mut bytes) {
                    if n == 0 {
                        break;
                    }
                }
            });
            let e = engine();
            let runtime = rt(&e);
            let client = NativeHttpClient::new(e.handle().clone());
            let req = request(format!("http://{address}/"));
            let profile = profile(
                false,
                address.to_string(),
                SmithyBodyAdmission::Immediate,
                SmithyH1Connection::KeepAlive,
            );
            e.block_on(async {
                assert_eq!(
                    client
                        .send_smithy(&req, &runtime, &profile)
                        .await
                        .unwrap_err()
                        .kind(),
                    TransportErrorKind::Connection
                );
            });
            server.join().unwrap();
        }
    }

    #[test]
    fn forced_h1_tls_never_selects_h2_and_h2_never_falls_back_to_cleartext_h1() {
        use tokio_rustls::rustls::{self, pki_types::PrivatePkcs8KeyDer};
        let e = engine();
        let runtime = rt(&e);
        e.block_on(async {
            let key = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
            let cert = key.cert.der().clone();
            let mut config = rustls::ServerConfig::builder_with_provider(Arc::new(
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
            config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                let (socket, _) = listener.accept().await.unwrap();
                let mut tls = tokio_rustls::TlsAcceptor::from(Arc::new(config))
                    .accept(socket)
                    .await
                    .unwrap();
                assert_eq!(
                    tls.get_ref().1.alpn_protocol(),
                    Some(b"http/1.1".as_slice())
                );
                let mut head = vec![];
                while !head.ends_with(b"\r\n\r\n") {
                    let mut b = [0];
                    tls.read_exact(&mut b).await.unwrap();
                    head.push(b[0]);
                }
                assert!(head.starts_with(b"POST /test HTTP/1.1\r\n"));
                let mut body = [0; 2];
                tls.read_exact(&mut body).await.unwrap();
                tls.write_all(b"HTTP/1.1 200 Fine\r\ncontent-length: 0\r\n\r\n")
                    .await
                    .unwrap();
                let _ = tls.read(&mut [0]).await;
            });
            let client =
                NativeHttpClient::with_root_certificates(e.handle().clone(), &[cert.to_vec()])
                    .unwrap();
            let req = request(format!("https://{address}/test"));
            let profile = profile(
                false,
                address.to_string(),
                SmithyBodyAdmission::Immediate,
                SmithyH1Connection::KeepAlive,
            );
            let mut response = client.send_smithy(&req, &runtime, &profile).await.unwrap();
            response.body.close();
            drop(response);
            server.await.unwrap();
        });
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(3)))
                .unwrap();
            let mut preface = [0; 24];
            socket.read_exact(&mut preface).unwrap();
            assert_eq!(&preface, b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n");
            socket
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
                .unwrap();
            socket.shutdown(std::net::Shutdown::Write).unwrap();
            let mut buf = [0; 1024];
            while let Ok(n) = socket.read(&mut buf) {
                if n == 0 {
                    break;
                }
            }
            listener.set_nonblocking(true).unwrap();
            assert!(listener.accept().is_err(), "no H1 successor connection");
        });
        let client = NativeHttpClient::new(e.handle().clone());
        let req = request(format!("http://{address}/"));
        let profile = profile(
            true,
            address.to_string(),
            SmithyBodyAdmission::Immediate,
            SmithyH1Connection::KeepAlive,
        );
        e.block_on(async {
            assert_eq!(
                client
                    .send_smithy(&req, &runtime, &profile)
                    .await
                    .unwrap_err()
                    .kind(),
                TransportErrorKind::Connection
            );
        });
        server.join().unwrap();
    }

    #[test]
    fn h2_illegal_pseudoheaders_connections_and_singletons_fail_without_wire_effect() {
        let e = engine();
        let runtime = rt(&e);
        let client = NativeHttpClient::new(e.handle().clone());
        for (authority, scheme, headers) in [
            ("bad authority", None, vec![]),
            ("valid.invalid", Some("bad scheme"), vec![]),
            ("valid.invalid", None, vec![("connection", "close")]),
            ("valid.invalid", None, vec![("te", "gzip")]),
            (
                "valid.invalid",
                None,
                vec![("content-type", "one"), ("Content-Type", "two")],
            ),
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let mut req = request(format!("http://{}/", listener.local_addr().unwrap()));
            for (k, v) in headers {
                req.headers.append(k, v).unwrap();
            }
            let profile = SmithyRequestProfile::Http2 {
                pseudo_headers: SmithyH2PseudoHeaders {
                    authority: authority.into(),
                    scheme: scheme.map(str::to_owned),
                },
                body_admission: SmithyBodyAdmission::Immediate,
            };
            e.block_on(async {
                assert_eq!(
                    client
                        .send_smithy(&req, &runtime, &profile)
                        .await
                        .unwrap_err()
                        .kind(),
                    TransportErrorKind::InvalidRequest
                );
            });
            assert!(listener.accept().is_err());
            assert!(!format!("{profile:?}").contains(authority));
        }
    }

    #[test]
    fn empty_continuation_envelopes_are_charged_at_the_exact_wire_limit() {
        for frames in [6usize, 7] {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let server = thread::spawn(move || {
                let (mut socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(3)))
                    .unwrap();
                let mut preface = [0; 24];
                socket.read_exact(&mut preface).unwrap();
                loop {
                    let mut h = [0; 9];
                    socket.read_exact(&mut h).unwrap();
                    let n =
                        (usize::from(h[0]) << 16) | (usize::from(h[1]) << 8) | usize::from(h[2]);
                    let mut p = vec![0; n];
                    socket.read_exact(&mut p).unwrap();
                    if h[3] == 1 && h[8] == 1 {
                        break;
                    }
                }
                let mut response = vec![0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 0, 0, 1, 0x88];
                for index in 0..frames {
                    response.extend_from_slice(&[
                        0,
                        0,
                        0,
                        9,
                        if index + 1 == frames { 4 } else { 0 },
                        0,
                        0,
                        0,
                        1,
                    ]);
                }
                socket.write_all(&response).unwrap();
                let mut bytes = [0; 1024];
                while let Ok(n) = socket.read(&mut bytes) {
                    if n == 0 {
                        break;
                    }
                }
            });
            let e = engine();
            let runtime = rt(&e);
            let client = NativeHttpClient::new(e.handle().clone()).with_response_head_limit(64);
            let req = request(format!("http://{address}/"));
            let profile = profile(
                true,
                address.to_string(),
                SmithyBodyAdmission::Immediate,
                SmithyH1Connection::KeepAlive,
            );
            e.block_on(async {
                let result = client.send_smithy(&req, &runtime, &profile).await;
                if frames == 6 {
                    let mut response = result.unwrap();
                    assert_eq!(response.status, 200);
                    response.body.close();
                } else {
                    assert_eq!(
                        result.unwrap_err().kind(),
                        TransportErrorKind::LimitExceeded
                    );
                }
            });
            server.join().unwrap();
        }
    }

    #[test]
    fn peer_waiting_for_head_goaway_can_deliver_body_without_consumer_polling() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (seen_tx, seen_rx) = tokio::sync::oneshot::channel();
        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(3)))
                .unwrap();
            let mut preface = [0; 24];
            socket.read_exact(&mut preface).unwrap();
            let read_frame = |socket: &mut TcpStream| {
                let mut h = [0; 9];
                socket.read_exact(&mut h).unwrap();
                let n = (usize::from(h[0]) << 16) | (usize::from(h[1]) << 8) | usize::from(h[2]);
                let mut body = vec![0; n];
                socket.read_exact(&mut body).unwrap();
                (h, body)
            };
            loop {
                let (h, _) = read_frame(&mut socket);
                if h[3] == 1 && h[8] == 1 {
                    break;
                }
            }
            socket
                .write_all(&[0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 1, 1, 4, 0, 0, 0, 1, 0x88])
                .unwrap();
            loop {
                let (h, body) = read_frame(&mut socket);
                if h[3] == 7 {
                    assert_eq!(&h[4..], &[0, 0, 0, 0, 0]);
                    assert_eq!(body, [0; 8]);
                    break;
                }
            }
            seen_tx.send(()).unwrap();
            socket
                .write_all(&[0, 0, 2, 0, 1, 0, 0, 0, 1, b'o', b'k'])
                .unwrap();
            let mut bytes = [0; 1024];
            while let Ok(n) = socket.read(&mut bytes) {
                if n == 0 {
                    break;
                }
            }
        });
        let e = engine();
        let runtime = rt(&e);
        let client = NativeHttpClient::new(e.handle().clone());
        let req = request(format!("http://{address}/"));
        let profile = profile(
            true,
            address.to_string(),
            SmithyBodyAdmission::Immediate,
            SmithyH1Connection::KeepAlive,
        );
        e.block_on(async {
            let mut response = client.send_smithy(&req, &runtime, &profile).await.unwrap();
            tokio::time::timeout(Duration::from_secs(3), seen_rx)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                read_body_limited(response.body.as_mut(), &runtime, 16)
                    .await
                    .unwrap(),
                b"ok"
            );
        });
        server.join().unwrap();
    }

    #[test]
    fn post_head_push_metadata_has_exact_byte_and_block_bounds_and_releases_socket() {
        // Each valid promise is 19 wire bytes, with four tiny HPACK pseudoheaders.
        for (limit, promises, accepted) in [
            (247, 13, true),
            (247, 14, false),
            (65536, 128, true),
            (65536, 129, false),
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let server = thread::spawn(move || {
                let (mut socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(3)))
                    .unwrap();
                let mut preface = [0; 24];
                socket.read_exact(&mut preface).unwrap();
                let frame = |socket: &mut TcpStream| {
                    let mut h = [0; 9];
                    socket.read_exact(&mut h).unwrap();
                    let n =
                        (usize::from(h[0]) << 16) | (usize::from(h[1]) << 8) | usize::from(h[2]);
                    let mut body = vec![0; n];
                    socket.read_exact(&mut body).unwrap();
                    h
                };
                while frame(&mut socket)[3] != 1 {}
                socket
                    .write_all(&[0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 1, 1, 4, 0, 0, 0, 1, 0x88])
                    .unwrap();
                // Causal head boundary: promises cannot arrive during acquisition.
                while frame(&mut socket)[3] != 7 {}
                let mut wire = Vec::new();
                for id in 1..=promises {
                    wire.extend_from_slice(&[0, 0, 10, 5, 4, 0, 0, 0, 1]);
                    wire.extend_from_slice(&(id * 2u32).to_be_bytes());
                    wire.extend_from_slice(&[0x82, 0x86, 0x84, 0x01, 0x01, b'x']);
                }
                wire.extend_from_slice(&[0, 0, 2, 0, 0, 0, 0, 0, 1, b'o', b'k']);
                if accepted {
                    // More DATA than the metadata budget remains a normal stream.
                    for _ in 0..2 {
                        wire.extend_from_slice(&[0, 64, 0, 0, 0, 0, 0, 0, 1]);
                        wire.extend_from_slice(&[b'd'; 16384]);
                    }
                }
                socket.write_all(&wire).unwrap();
                let mut bytes = [0; 1024];
                loop {
                    match socket.read(&mut bytes) {
                        Ok(0) => break,
                        Ok(_) => {}
                        Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => break,
                        Err(e) => panic!("socket owner was not released: {e}"),
                    }
                }
            });
            let e = engine();
            let runtime = rt(&e);
            let client = NativeHttpClient::new(e.handle().clone()).with_response_head_limit(limit);
            let req = request(format!("http://{address}/"));
            let profile = profile(
                true,
                address.to_string(),
                SmithyBodyAdmission::Immediate,
                SmithyH1Connection::KeepAlive,
            );
            e.block_on(async {
                let mut response = client.send_smithy(&req, &runtime, &profile).await.unwrap();
                let result =
                    tokio::time::timeout(Duration::from_secs(3), response.body.next(&runtime))
                        .await
                        .unwrap();
                if accepted {
                    assert_eq!(result.unwrap().unwrap(), b"ok");
                    let mut received = 0;
                    while received < 32768 {
                        let chunk = response.body.next(&runtime).await.unwrap().unwrap();
                        assert!(chunk.iter().all(|b| *b == b'd'));
                        received += chunk.len();
                    }
                    assert_eq!(received, 32768);
                    response.body.close();
                } else {
                    assert_eq!(
                        result.unwrap_err().kind(),
                        TransportErrorKind::LimitExceeded
                    );
                    // Keep the outer response alive: the failed read must retire IO itself.
                    server.join().unwrap();
                    assert!(response.body.next(&runtime).await.unwrap().is_none());
                    return;
                }
                server.join().unwrap();
            });
        }
    }

    #[test]
    fn runtime_loss_releases_response_and_pending_control_owners() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(3)))
                .unwrap();
            let mut preface = [0; 24];
            socket.read_exact(&mut preface).unwrap();
            loop {
                let mut h = [0; 9];
                socket.read_exact(&mut h).unwrap();
                let size = (usize::from(h[0]) << 16) | (usize::from(h[1]) << 8) | usize::from(h[2]);
                let mut p = vec![0; size];
                socket.read_exact(&mut p).unwrap();
                if h[3] == 1 && h[8] == 1 {
                    break;
                }
            }
            socket
                .write_all(&[0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 1, 1, 4, 0, 0, 0, 1, 0x88])
                .unwrap();
            let mut bytes = [0; 1024];
            loop {
                match socket.read(&mut bytes) {
                    Ok(0) => break,
                    Ok(_) => {}
                    Err(e)
                        if matches!(
                            e.kind(),
                            std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
                        ) =>
                    {
                        break;
                    }
                    other => panic!("runtime left IO alive: {other:?}"),
                }
            }
        });
        let e = engine();
        let runtime = rt(&e);
        let client = NativeHttpClient::new(e.handle().clone());
        let req = request(format!("http://{address}/"));
        let profile = profile(
            true,
            address.to_string(),
            SmithyBodyAdmission::Immediate,
            SmithyH1Connection::KeepAlive,
        );
        let mut response = e
            .block_on(client.send_smithy(&req, &runtime, &profile))
            .unwrap();
        drop(e);
        let mut read = response.body.next(&runtime);
        let mut result = None;
        for _ in 0..1000 {
            if let std::task::Poll::Ready(value) = read
                .as_mut()
                .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
            {
                result = Some(value);
                break;
            }
            thread::yield_now();
        }
        assert_eq!(
            result
                .expect("closed runtime settles the join")
                .unwrap_err()
                .kind(),
            TransportErrorKind::RuntimeUnavailable
        );
        drop(read);
        drop(response);
        server.join().unwrap();
    }

    #[test]
    fn compressed_informational_heads_have_cumulative_decoded_and_block_limits() {
        for blocks_only in [false, true] {
            let e = engine();
            let runtime = rt(&e);
            e.block_on(async {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let address = listener.local_addr().unwrap();
                let server = tokio::spawn(async move {
                    let (socket, _) = listener.accept().await.unwrap();
                    let mut conn = h2::server::handshake(socket).await.unwrap();
                    let (req, mut send) = conn.accept().await.unwrap().unwrap();
                    for _ in 0..if blocks_only { 129 } else { 4 } {
                        let mut head = Response::builder().status(100).body(()).unwrap();
                        if !blocks_only {
                            head.headers_mut().insert(
                                "x-repeated",
                                hyper::header::HeaderValue::from_str(&"x".repeat(1280)).unwrap(),
                            );
                        }
                        send.send_informational(head).unwrap();
                    }
                    let response = send
                        .send_response(Response::builder().status(200).body(()).unwrap(), true)
                        .unwrap();
                    drop(response);
                    drop(send);
                    drop(req);
                    while conn.accept().await.is_some() {}
                });
                let client = NativeHttpClient::new(e.handle().clone())
                    .with_response_head_limit(if blocks_only { 65536 } else { 4096 });
                let req = request(format!("http://{address}/"));
                let profile = profile(
                    true,
                    address.to_string(),
                    SmithyBodyAdmission::Immediate,
                    SmithyH1Connection::KeepAlive,
                );
                assert_eq!(
                    tokio::time::timeout(
                        Duration::from_secs(3),
                        client.send_smithy(&req, &runtime, &profile)
                    )
                    .await
                    .unwrap()
                    .unwrap_err()
                    .kind(),
                    TransportErrorKind::LimitExceeded
                );
                tokio::time::timeout(Duration::from_secs(3), server)
                    .await
                    .unwrap()
                    .unwrap();
            });
        }
    }
    #[test]
    fn h2_prior_knowledge_order_aliases_latin1_and_body_release() {
        let e = engine();
        let runtime = rt(&e);
        e.block_on(async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (socket, _) = listener.accept().await.unwrap();
                let mut conn = h2::server::handshake(socket).await.unwrap();
                let (req, mut send) = conn.accept().await.unwrap().unwrap();
                let driver = tokio::spawn(async move { while conn.accept().await.is_some() {} });
                assert_eq!(req.uri().authority().unwrap().as_str(), "synthetic.invalid");
                assert!(!req.headers().contains_key("accept"));
                assert!(!req.headers().contains_key("host"));
                assert_eq!(
                    req.headers()
                        .get_all("x-alias")
                        .iter()
                        .map(|v| v.to_str().unwrap())
                        .collect::<Vec<_>>(),
                    ["lower", "upper"]
                );
                let mut body = req.into_body();
                let bytes = body.data().await.unwrap().unwrap();
                assert_eq!(&bytes[..], b"{}");
                body.flow_control().release_capacity(bytes.len()).unwrap();
                let mut response = Response::builder().status(200).body(()).unwrap();
                for (k, v) in fields() {
                    response.headers_mut().append(
                        hyper::header::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                        hyper::header::HeaderValue::from_bytes(
                            &v.chars().map(|c| c as u8).collect::<Vec<_>>(),
                        )
                        .unwrap(),
                    );
                }
                response.headers_mut().append(
                    "x-invalid",
                    hyper::header::HeaderValue::from_static(" trailing "),
                );
                for i in (0..80).rev() {
                    response.headers_mut().append(
                        hyper::header::HeaderName::from_bytes(format!("x-grow-{i}").as_bytes())
                            .unwrap(),
                        hyper::header::HeaderValue::from_static("v"),
                    );
                }
                let mut stream = send.send_response(response, false).unwrap();
                stream.send_data(Bytes::from_static(b"ok"), true).unwrap();
                drop(stream);
                drop(send);
                drop(body);
                tokio::time::timeout(Duration::from_secs(3), driver)
                    .await
                    .unwrap()
                    .unwrap();
            });
            let client = NativeHttpClient::new(e.handle().clone());
            let mut req = request(format!("http://{address}/test"));
            req.headers.append("x-alias", "lower").unwrap();
            req.headers.append("X-Alias", "upper").unwrap();
            let profile = SmithyRequestProfile::Http2 {
                pseudo_headers: SmithyH2PseudoHeaders {
                    authority: "synthetic.invalid".into(),
                    scheme: None,
                },
                body_admission: SmithyBodyAdmission::Immediate,
            };
            let mut response = client.send_smithy(&req, &runtime, &profile).await.unwrap();
            assert_eq!(
                response
                    .headers
                    .iter_for_wire()
                    .take(8)
                    .map(|(k, _)| k)
                    .collect::<Vec<_>>(),
                [
                    "x-z",
                    "x-z",
                    "set-cookie",
                    "set-cookie",
                    "x-a",
                    "cookie",
                    "cookie",
                    "x-byte"
                ]
            );
            assert_eq!(
                response.headers.get_for_wire("x-byte"),
                Some("\u{a0}kept\u{a0}")
            );
            assert!(response.headers.get_for_wire("x-invalid").is_none());
            let source: serde_json::Value = serde_json::from_str(include_str!(
                "../../../conformance/ai-transport/smithy-http/trace.json"
            ))
            .unwrap();
            let expected: Vec<_> = source["cases"][1]["entries"]
                .as_array()
                .unwrap()
                .iter()
                .filter_map(|p| p[0].as_str().filter(|k| k.starts_with("x-grow-")))
                .collect();
            assert_eq!(
                response
                    .headers
                    .iter_for_wire()
                    .filter_map(|(k, _)| k.starts_with("x-grow-").then_some(k))
                    .collect::<Vec<_>>(),
                expected
            );
            assert_eq!(
                read_body_limited(response.body.as_mut(), &runtime, 16)
                    .await
                    .unwrap(),
                b"ok"
            );
            drop(response);
            server.await.unwrap();
        });
    }
    #[test]
    fn h1_expect_continue_or_final_admits_body_at_source_stage() {
        for final_first in [false, true] {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let server = thread::spawn(move || {
                let (mut socket, _) = listener.accept().unwrap();
                let _ = head(&mut socket);
                socket
                    .set_read_timeout(Some(Duration::from_millis(50)))
                    .unwrap();
                assert!(socket.read(&mut [0; 2]).is_err(), "body must wait for100");
                socket
                    .set_read_timeout(Some(Duration::from_secs(4)))
                    .unwrap();
                if final_first {
                    socket
                        .write_all(b"HTTP/1.1 417 No\r\ncontent-length: 0\r\n\r\n")
                        .unwrap();
                } else {
                    socket.write_all(b"HTTP/1.1 100 Continue\r\n\r\n").unwrap();
                    let mut body = [0; 2];
                    socket.read_exact(&mut body).unwrap();
                    assert_eq!(&body, b"{}");
                    socket
                        .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
                        .unwrap();
                }
                closed(&mut socket);
            });
            let e = engine();
            let runtime = rt(&e);
            let client = NativeHttpClient::new(e.handle().clone());
            let mut req = request(format!("http://{address}/"));
            req.headers.set("expect", "100-continue").unwrap();
            e.block_on(async {
                let mut response = tokio::time::timeout(
                    Duration::from_secs(3),
                    client.send_smithy(
                        &req,
                        &runtime,
                        &SmithyRequestProfile::Http1 {
                            body_admission: SmithyBodyAdmission::WaitForContinue,
                            connection: SmithyH1Connection::Close,
                        },
                    ),
                )
                .await
                .unwrap()
                .unwrap();
                assert_eq!(response.status, if final_first { 417 } else { 200 });
                response.body.close();
            });
            server.join().unwrap();
        }
    }
    #[test]
    fn h2_expect_continue_or_final_does_not_deadlock_driver() {
        for final_first in [false, true] {
            let e = engine();
            let runtime = rt(&e);
            e.block_on(async {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let address = listener.local_addr().unwrap();
                let server = tokio::spawn(async move {
                    let (socket, _) = listener.accept().await.unwrap();
                    let mut conn = h2::server::handshake(socket).await.unwrap();
                    let (req, mut send) = conn.accept().await.unwrap().unwrap();
                    let driver =
                        tokio::spawn(async move { while conn.accept().await.is_some() {} });
                    let mut body = req.into_body();
                    assert!(
                        tokio::time::timeout(Duration::from_millis(50), body.data())
                            .await
                            .is_err()
                    );
                    if !final_first {
                        send.send_informational(Response::builder().status(100).body(()).unwrap())
                            .unwrap();
                        let bytes = tokio::time::timeout(Duration::from_secs(2), body.data())
                            .await
                            .unwrap()
                            .unwrap()
                            .unwrap();
                        assert_eq!(&bytes[..], b"{}");
                    }
                    let stream = send
                        .send_response(
                            Response::builder()
                                .status(if final_first { 417 } else { 200 })
                                .body(())
                                .unwrap(),
                            true,
                        )
                        .unwrap();
                    drop(stream);
                    drop(body);
                    drop(send);
                    tokio::time::timeout(Duration::from_secs(3), driver)
                        .await
                        .unwrap()
                        .unwrap();
                });
                let client = NativeHttpClient::new(e.handle().clone());
                let mut req = request(format!("http://{address}/"));
                req.headers.set("expect", "100-continue").unwrap();
                let profile = SmithyRequestProfile::Http2 {
                    pseudo_headers: SmithyH2PseudoHeaders {
                        authority: address.to_string(),
                        scheme: None,
                    },
                    body_admission: SmithyBodyAdmission::WaitForContinue,
                };
                let mut response = tokio::time::timeout(
                    Duration::from_secs(3),
                    client.send_smithy(&req, &runtime, &profile),
                )
                .await
                .unwrap()
                .unwrap();
                assert_eq!(response.status, if final_first { 417 } else { 200 });
                response.body.close();
                drop(response);
                server.await.unwrap();
            });
        }
    }
}
