#![cfg(feature = "native")]
use pi_ai_core::*;
use pi_ai_transport::*;
use std::{sync::Arc, time::Duration};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
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
        panic!("no transport retries")
    }
}
impl TaskSpawner for Host {
    fn spawn(&self, _: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
        Err(SpawnError::new())
    }
}
fn runtime() -> RequestRuntime {
    RequestRuntime::with_noop_telemetry(CancellationToken::new(), Arc::new(Host), Arc::new(Host))
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
fn profile() -> SmithyRequestProfile {
    SmithyRequestProfile::Http1 {
        connection: SmithyH1Connection::Close,
        body_admission: SmithyBodyAdmission::Immediate,
    }
}
#[test]
fn source_head_and_body_error_classes_preserve_default_profile() {
    let engine = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    engine.block_on(async {
        for (id, bytes, body, expected) in [
            (
                "refused",
                b"".as_slice(),
                false,
                SmithyRetryDisposition::Transient,
            ),
            (
                "reset-before-head",
                b"".as_slice(),
                false,
                SmithyRetryDisposition::Transient,
            ),
            (
                "malformed-head",
                b"NOT HTTP\r\n\r\n".as_slice(),
                false,
                SmithyRetryDisposition::NotRetryable,
            ),
            (
                "truncated-body",
                b"HTTP/1.1 200 OK\r\nContent-Length: 9\r\n\r\na".as_slice(),
                true,
                SmithyRetryDisposition::Transient,
            ),
            (
                "malformed-chunk",
                b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\nNO\r\n".as_slice(),
                true,
                SmithyRetryDisposition::Transient,
            ),
        ] {
            for smithy in [false, true] {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let address = listener.local_addr().unwrap();
                let server = if id == "refused" {
                    drop(listener);
                    None
                } else {
                    Some(tokio::spawn(async move {
                        let (mut stream, _) = listener.accept().await.unwrap();
                        let mut input = [0; 2048];
                        assert!(stream.read(&mut input).await.unwrap() > 0);
                        stream.write_all(bytes).await.unwrap();
                        stream.shutdown().await.unwrap();
                    }))
                };
                let client = NativeHttpClient::new(engine.handle().clone());
                let req = request(format!("http://{address}/"));
                let rt = runtime();
                let result = if smithy {
                    client.send_smithy(&req, &rt, &profile()).await
                } else {
                    client.send(&req, &rt).await
                };
                let error = if body {
                    let mut response = result.unwrap();
                    loop {
                        match response.body.next(&rt).await {
                            Ok(Some(_)) => {}
                            Ok(None) => panic!("{id} should fail"),
                            Err(e) => break e,
                        }
                    }
                } else {
                    result.unwrap_err()
                };
                assert_eq!(error.kind(), TransportErrorKind::Connection, "{id}");
                assert_eq!(
                    error.smithy_retry(),
                    smithy.then_some(expected),
                    "{id} smithy={smithy}"
                );
                let expected_assistant = match id {
                    "refused" => ["429", "500", "502", "503", "504", "524"]
                        .iter()
                        .any(|p| address.to_string().contains(p)),
                    "reset-before-head" => true,
                    _ => false,
                };
                assert_eq!(
                    error.smithy_assistant_retry(),
                    smithy.then_some(expected_assistant),
                    "{id} assistant smithy={smithy}"
                );
                if let Some(server) = server {
                    server.await.unwrap();
                }
            }
        }
    });
}
#[test]
fn source_h2_remote_refusal_differs_from_protocol_error() {
    let engine = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    engine.block_on(async {
        for (reason, expected) in [
            (
                h2::Reason::REFUSED_STREAM,
                SmithyRetryDisposition::Transient,
            ),
            (
                h2::Reason::PROTOCOL_ERROR,
                SmithyRetryDisposition::NotRetryable,
            ),
        ] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                let mut conn = h2::server::handshake(stream).await.unwrap();
                let (_, mut send) = conn.accept().await.unwrap().unwrap();
                send.send_reset(reason);
                while conn.accept().await.is_some() {}
            });
            let client = NativeHttpClient::new(engine.handle().clone());
            let req = request(format!("http://{address}/"));
            let error = client
                .send_smithy(
                    &req,
                    &runtime(),
                    &SmithyRequestProfile::Http2 {
                        pseudo_headers: SmithyH2PseudoHeaders {
                            authority: address.to_string(),
                            scheme: None,
                        },
                        body_admission: SmithyBodyAdmission::Immediate,
                    },
                )
                .await
                .unwrap_err();
            assert_eq!(error.kind(), TransportErrorKind::Connection);
            assert_eq!(error.smithy_retry(), Some(expected));
            assert_eq!(error.smithy_assistant_retry(), Some(false));
            server.abort();
        }
    });
}
#[test]
fn source_untrusted_certificate_is_nonretryable_only_on_explicit_smithy() {
    use tokio_rustls::rustls::{self, pki_types::PrivatePkcs8KeyDer};
    let engine = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    engine.block_on(async {
        for smithy in [false, true] {
            let key = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
            let config = rustls::ServerConfig::builder_with_provider(Arc::new(
                rustls::crypto::aws_lc_rs::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(
                vec![key.cert.der().clone()],
                PrivatePkcs8KeyDer::from(key.signing_key.serialize_der()).into(),
            )
            .unwrap();
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                let _ = tokio_rustls::TlsAcceptor::from(Arc::new(config))
                    .accept(stream)
                    .await;
            });
            let client =
                NativeHttpClient::with_root_certificates(engine.handle().clone(), &[]).unwrap();
            let req = request(format!("https://{address}/"));
            let rt = runtime();
            let error = if smithy {
                client.send_smithy(&req, &rt, &profile()).await
            } else {
                client.send(&req, &rt).await
            }
            .unwrap_err();
            assert_eq!(
                error.smithy_retry(),
                smithy.then_some(SmithyRetryDisposition::NotRetryable)
            );
            server.await.unwrap();
        }
    });
}

#[test]
fn single_destination_refused_error_retains_source_port_classifier_fact() {
    let engine = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    engine.block_on(async {
        for (port, expected) in [
            (40001, false),
            (42901, true),
            (50001, true),
            (50201, true),
            (50301, true),
            (50401, true),
            (52401, true),
        ] {
            let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
                .await
                .unwrap();
            let address = listener.local_addr().unwrap();
            drop(listener);
            let client = NativeHttpClient::new(engine.handle().clone());
            let error = client
                .send_smithy(
                    &request(format!("http://{address}/")),
                    &runtime(),
                    &profile(),
                )
                .await
                .unwrap_err();
            assert_eq!(
                error.smithy_retry(),
                Some(SmithyRetryDisposition::Transient)
            );
            assert_eq!(
                error.smithy_assistant_retry(),
                Some(expected),
                "port {port}"
            );
            assert!(!format!("{error:?}").contains(&port.to_string()));
        }
    });
}
