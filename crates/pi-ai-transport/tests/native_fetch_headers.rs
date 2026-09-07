use pi_ai_transport::{HttpHeaders, TransportErrorKind};

fn fields(values: &[(&str, &str)]) -> HttpHeaders {
    let mut result = HttpHeaders::new();
    for (name, value) in values {
        result.append(*name, *value).unwrap();
    }
    result
}
#[test]
fn explicit_views_preserve_raw_and_mutations_do_not_retrim_observed_values() {
    let raw = fields(&[("X-Test", "a"), ("x-test", "b"), ("set-cookie", "c")]);
    let semantic = fields(&[("set-cookie", "c \t"), ("x-test", "a "), ("x-test", "b\t")]);
    let mut value = raw.clone().with_fetch_response_fields(semantic).unwrap();
    assert_eq!(
        value.iter_for_wire().collect::<Vec<_>>(),
        raw.iter_for_wire().collect::<Vec<_>>()
    );
    assert_eq!(value.get_for_fetch("X-Test").as_deref(), Some("a , b\t"));
    assert_ne!(value, raw);
    assert_eq!(value, value.clone());
    value.append("X-Test", " c ").unwrap();
    assert_eq!(value.get_for_fetch("x-test").as_deref(), Some("a , b\t, c"));
    assert_eq!(value.get_for_fetch("set-cookie").as_deref(), Some("c \t"));
    value.set("X-Test", " d ").unwrap();
    assert_eq!(value.get_for_fetch("x-test").as_deref(), Some("d"));
    assert_eq!(value.get_for_wire("x-test"), Some(" d "));
    value.remove("SET-COOKIE");
    assert!(value.get_for_fetch("set-cookie").is_none());
    let before = value.clone();
    assert!(value.append("bad name", "secret").is_err());
    assert_eq!(value, before);
    assert!(!format!("{value:?}").contains(" d "));
}
#[test]
fn view_admission_checks_names_cardinality_and_byte_strings() {
    let raw = fields(&[("a", "one"), ("a", "two")]);
    for invalid in [
        fields(&[("a", "one")]),
        fields(&[("a", "one"), ("b", "two")]),
        fields(&[("a", "one"), ("a", "😀")]),
    ] {
        assert_eq!(
            raw.clone()
                .with_fetch_response_fields(invalid)
                .unwrap_err()
                .kind(),
            TransportErrorKind::Protocol
        );
    }
    assert_eq!(
        fields(&[("a", " x "), ("a", " y\t")])
            .get_for_fetch("a")
            .as_deref(),
        Some("x, y")
    );
}

#[cfg(feature = "native")]
mod native {
    use super::*;
    use pi_ai_core::*;
    use pi_ai_transport::*;
    use serde_json::{Value, json};
    use std::{
        io::{Read, Write},
        net::{TcpListener, TcpStream},
        sync::Arc,
        time::Duration,
    };
    struct Host;
    impl Clock for Host {
        fn now_unix_ms(&self) -> u64 {
            0
        }
        fn sleep<'a>(
            &'a self,
            d: Duration,
            _: &'a CancellationToken,
        ) -> BoxFuture<'a, Result<(), Cancelled>> {
            Box::pin(async move {
                if d.is_zero() {
                    Ok(())
                } else {
                    std::future::pending().await
                }
            })
        }
    }
    impl RetryJitter for Host {
        fn sample(&self) -> f64 {
            panic!("source uses explicit zero delay")
        }
    }
    impl TaskSpawner for Host {
        fn spawn(&self, _: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
            Err(SpawnError::new())
        }
    }
    fn runtime() -> RequestRuntime {
        RequestRuntime::with_noop_telemetry(
            CancellationToken::new(),
            Arc::new(Host),
            Arc::new(Host),
        )
    }
    fn request(endpoint: String) -> HttpRequest {
        HttpRequest {
            endpoint: RequestEndpoint::new_resolved(endpoint).unwrap(),
            method: "GET".into(),
            headers: HttpHeaders::new(),
            body: vec![],
            proxy: None,
        }
    }
    fn hex(text: &str) -> Vec<u8> {
        text.as_bytes()
            .as_chunks::<2>()
            .0
            .iter()
            .map(|p| u8::from_str_radix(std::str::from_utf8(p).unwrap(), 16).unwrap())
            .collect()
    }
    fn read_request(socket: &mut TcpStream) {
        socket
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        socket
            .set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut bytes = vec![];
        while !bytes.ends_with(b"\r\n\r\n") {
            let mut byte = [0];
            socket.read_exact(&mut byte).unwrap();
            bytes.push(byte[0]);
            assert!(bytes.len() < 16384);
        }
    }
    fn closed(socket: &mut TcpStream) {
        match socket.read(&mut [0]) {
            Ok(0) => {}
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
                ) => {}
            other => panic!("expected EOF/reset: {other:?}"),
        }
    }
    fn exchange(head: Vec<u8>, fetch: bool, limit: usize) -> Result<HttpHeaders, TransportError> {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            read_request(&mut socket);
            socket.write_all(&head).unwrap();
            closed(&mut socket);
        });
        let engine = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let client = NativeHttpClient::new(engine.handle().clone()).with_response_head_limit(limit);
        let runtime = runtime();
        let request = request(format!("http://{address}/"));
        let result = engine.block_on(async {
            tokio::time::timeout(Duration::from_secs(5), async {
                let mut response = if fetch {
                    client.send_fetch_hop(&request, &runtime).await
                } else {
                    client.send(&request, &runtime).await
                }?;
                let headers = response.headers.clone();
                response.body.close();
                Ok(headers)
            })
            .await
            .unwrap()
        });
        server.join().unwrap();
        result
    }
    #[test]
    fn network_matrix_preserves_each_source_value_and_raw_send_stays_unchanged() {
        let corpus: Value = serde_json::from_str(include_str!(
            "../../../conformance/ai-transport/native-fetch-headers/headers.json"
        ))
        .unwrap();
        for row in corpus["rows"].as_array().unwrap() {
            let mut head = "HTTP/1.1 200 OK\r\n".to_owned();
            for name in ["X-Test", "Cookie", "Set-Cookie"] {
                for value in row["values"].as_array().unwrap() {
                    head.push_str(&format!("{name}: {}\r\n", value.as_str().unwrap()));
                }
            }
            head.push_str("Content-Length: 0\r\n\r\n");
            let raw = exchange(head.as_bytes().to_vec(), false, 65536).unwrap();
            let fetched = exchange(head.as_bytes().to_vec(), true, 65536).unwrap();
            assert_eq!(
                raw.iter_for_wire().collect::<Vec<_>>(),
                fetched.iter_for_wire().collect::<Vec<_>>()
            );
            for pair in row["network"]["get"].as_array().unwrap() {
                assert_eq!(
                    json!(fetched.get_for_fetch(pair[0].as_str().unwrap())),
                    pair[1]
                );
            }
            for pair in row["preconstructed"]["get"].as_array().unwrap() {
                assert_eq!(json!(raw.get_for_fetch(pair[0].as_str().unwrap())), pair[1]);
            }
        }
    }
    #[test]
    fn source_physical_forms_are_not_repaired_and_informational_bytes_stay_charged() {
        let corpus: Value = serde_json::from_str(include_str!(
            "../../../conformance/ai-transport/native-fetch-headers/physical.json"
        ))
        .unwrap();
        for row in corpus["cases"].as_array().unwrap() {
            let head = hex(row["head"].as_str().unwrap());
            let result = exchange(head.clone(), true, head.len());
            assert_eq!(
                result.is_ok(),
                row["accepted"].as_bool().unwrap(),
                "{}",
                row["id"]
            );
            if let Ok(headers) = result {
                for pair in row["entries"].as_array().unwrap() {
                    assert_eq!(
                        json!(headers.get_for_fetch(pair[0].as_str().unwrap())),
                        pair[1],
                        "{}",
                        row["id"]
                    );
                }
                assert_eq!(
                    exchange(head.clone(), true, head.len() - 1)
                        .unwrap_err()
                        .kind(),
                    TransportErrorKind::LimitExceeded
                );
            }
        }
        // Hyper's previously accepted LF raw profile is deliberately unchanged.
        let lf = corpus["cases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["id"] == "lf")
            .unwrap();
        assert!(exchange(hex(lf["head"].as_str().unwrap()), false, 65536).is_ok());
    }
    #[test]
    fn partial_head_cancellation_and_dropped_acquisition_release_same_socket() {
        for cancel in [false, true] {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let (sent, received) = std::sync::mpsc::channel();
            let server = std::thread::spawn(move || {
                let (mut socket, _) = listener.accept().unwrap();
                read_request(&mut socket);
                socket
                    .write_all(b"HTTP/1.1 200 OK\r\nX-Test: unfinished ")
                    .unwrap();
                sent.send(()).unwrap();
                closed(&mut socket);
            });
            let engine = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();
            let client = NativeHttpClient::new(engine.handle().clone());
            let runtime = runtime();
            let token = runtime.cancellation.clone();
            let request = request(format!("http://{address}/"));
            let worker =
                engine.spawn(async move { client.send_fetch_hop(&request, &runtime).await });
            received.recv_timeout(Duration::from_secs(5)).unwrap();
            if cancel {
                token.cancel();
                assert_eq!(
                    engine.block_on(worker).unwrap().unwrap_err().kind(),
                    TransportErrorKind::Cancelled
                );
            } else {
                worker.abort();
                assert!(engine.block_on(worker).unwrap_err().is_cancelled());
            }
            server.join().unwrap();
        }
    }
    #[test]
    fn tls_h2_admission_is_observed_independently_from_http1() {
        use tokio_rustls::rustls::{self, pki_types::PrivatePkcs8KeyDer};
        let corpus: Value = serde_json::from_str(include_str!(
            "../../../conformance/ai-transport/native-fetch-headers/h2.json"
        ))
        .unwrap();
        for (index, source_fields) in corpus["fields"].as_array().unwrap().iter().enumerate() {
            for fetch in [false, true] {
                let engine = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .build()
                    .unwrap();
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
                config.alpn_protocols = vec![b"h2".to_vec()];
                let listener = engine
                    .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
                    .unwrap();
                let address = listener.local_addr().unwrap();
                let source_fields = source_fields.clone();
                let server = engine.spawn(async move {
                    let (stream, _) = listener.accept().await.unwrap();
                    let stream = tokio_rustls::TlsAcceptor::from(Arc::new(config))
                        .accept(stream)
                        .await
                        .unwrap();
                    assert_eq!(stream.get_ref().1.alpn_protocol(), Some(b"h2".as_slice()));
                    let mut connection = h2::server::handshake(stream).await.unwrap();
                    let (_, mut sender) = connection.accept().await.unwrap().unwrap();
                    let mut response = hyper::Response::builder().status(200).body(()).unwrap();
                    for (name, value) in source_fields.as_object().unwrap() {
                        let values = if let Some(values) = value.as_array() {
                            values.clone()
                        } else {
                            vec![value.clone()]
                        };
                        for value in values {
                            let bytes = value
                                .as_str()
                                .unwrap()
                                .chars()
                                .map(|c| u8::try_from(u32::from(c)).unwrap())
                                .collect::<Vec<_>>();
                            response.headers_mut().append(
                                hyper::header::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                                hyper::header::HeaderValue::from_bytes(&bytes).unwrap(),
                            );
                        }
                    }
                    sender.send_response(response, true).unwrap();
                    assert!(connection.accept().await.is_none_or(|v| v.is_err()));
                });
                let client = NativeHttpClient::with_root_certificates(
                    engine.handle().clone(),
                    &[cert.to_vec()],
                )
                .unwrap();
                let runtime = runtime();
                let request = request(format!("https://{address}/"));
                engine.block_on(async {
                    tokio::time::timeout(Duration::from_secs(5), async {
                        let mut response = if fetch {
                            client.send_fetch_hop(&request, &runtime).await
                        } else {
                            client.send(&request, &runtime).await
                        }
                        .unwrap();
                        assert_eq!(response.status_text, "");
                        if !fetch {
                            for (name, expected) in corpus["fields"][index].as_object().unwrap() {
                                let actual = response
                                    .headers
                                    .iter_for_wire()
                                    .filter(|(k, _)| *k == name)
                                    .map(|(_, v)| json!(v))
                                    .collect::<Vec<_>>();
                                let expected = expected
                                    .as_array()
                                    .cloned()
                                    .unwrap_or_else(|| vec![expected.clone()]);
                                assert_eq!(actual, expected, "raw H2 remains unchanged");
                            }
                        }
                        let mut entries = Vec::new();
                        let names = std::collections::BTreeSet::from_iter(
                            response.headers.iter_for_fetch().map(|(k, _)| k.to_owned()),
                        );
                        for name in names {
                            if name == "set-cookie" {
                                for (k, v) in response
                                    .headers
                                    .iter_for_fetch()
                                    .filter(|(k, _)| *k == name)
                                {
                                    entries.push(json!([k, v]));
                                }
                            } else {
                                entries.push(json!([
                                    name,
                                    response.headers.get_for_fetch(&name).unwrap()
                                ]));
                            }
                        }
                        response.body.close();
                        drop(response);
                        server.await.unwrap();
                        if fetch {
                            assert_eq!(
                                json!(entries),
                                corpus["rows"][index]["entries"],
                                "h2 source case{index}"
                            );
                        }
                    })
                    .await
                    .unwrap()
                });
            }
        }
    }
    #[test]
    fn strict_retry_header_values_survive_cloning_into_the_full_attempt_owner() {
        let corpus: Value = serde_json::from_str(include_str!(
            "../../../conformance/ai-transport/native-fetch-headers/retry.json"
        ))
        .unwrap();
        for case in corpus["cases"].as_array().unwrap() {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let address = listener.local_addr().unwrap();
            let status = case["status"].as_u64().unwrap();
            let value = case["value"].as_str().unwrap().to_owned();
            let expected = case["calls"].as_u64().unwrap() as usize;
            let server = std::thread::spawn(move || {
                let mut calls = 0;
                let deadline = std::time::Instant::now() + Duration::from_secs(3);
                while calls < expected && std::time::Instant::now() < deadline {
                    let (mut socket, _) = match listener.accept() {
                        Ok(v) => v,
                        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                            std::thread::sleep(Duration::from_millis(1));
                            continue;
                        }
                        Err(e) => panic!("accept: {e}"),
                    };
                    read_request(&mut socket);
                    calls += 1;
                    write!(socket,"HTTP/1.1 {} Synthetic\r\nx-should-retry: {value}\r\nretry-after-ms: 0\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",if calls==1{status}else{200}).unwrap();
                    closed(&mut socket);
                }
                calls
            });
            let engine = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();
            let transport = HttpTransport::new(
                Arc::new(NativeHttpClient::new(engine.handle().clone())),
                Arc::new(Host),
            )
            .with_fetch_response_source(if case["network"] == true {
                FetchResponseSource::RawNetwork
            } else {
                FetchResponseSource::Preconstructed
            });
            let runtime = runtime();
            let request = request(format!("http://{address}/"));
            let seen = std::sync::Mutex::new(Vec::new());
            let result = engine.block_on(async {
                transport
                    .retry_provider_request(
                        &runtime,
                        RetryProfile::SharedProvider {
                            max_retries: 1,
                            max_retry_delay_ms: None,
                        },
                        || {
                            Box::pin(async {
                                let mut response = transport
                                    .send_fetch(
                                        &request,
                                        &runtime,
                                        RetryProfile::None,
                                        TimeoutProfile::None,
                                    )
                                    .await
                                    .map_err(|_| ProviderAttemptFailure::Other(0u16))?;
                                read_body_limited(response.body.as_mut(), &runtime, 1024)
                                    .await
                                    .map_err(|_| ProviderAttemptFailure::Other(0u16))?;
                                if response.status == 200 {
                                    Ok(())
                                } else {
                                    let headers = response.headers.clone();
                                    seen.lock()
                                        .unwrap()
                                        .push(headers.get_for_fetch("x-should-retry").unwrap());
                                    Err(ProviderAttemptFailure::Provider {
                                        status: Some(response.status),
                                        headers: Some(headers),
                                        error: response.status,
                                    })
                                }
                            })
                        },
                    )
                    .await
            });
            assert_eq!(server.join().unwrap(), expected);
            assert_eq!(result.is_ok(), case["success"].as_bool().unwrap());
            assert_eq!(json!(*seen.lock().unwrap()), json!([case["header"]]));
        }
    }
}
