#![cfg(feature = "auth-native")]
use pi_ai_core::CancellationToken;
use pi_ai_transport::*;
use std::{sync::Arc, time::Duration};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
}
fn bind() -> CallbackBind {
    CallbackBind {
        host: "127.0.0.1".into(),
        port: 0,
        limits: CallbackLimits::default(),
    }
}
fn response() -> CallbackResponse {
    CallbackResponse {
        status: 200,
        content_type: "text/html; charset=utf-8".into(),
        no_store: true,
        body: "synthetic-complete".into(),
    }
}
#[test]
fn auth_host_native_callback_retains_reply_and_acknowledges_close() {
    let rt = runtime();
    rt.block_on(async {
        let service = NativeCallbackListener::new(rt.handle().clone());
        let c = CancellationToken::new();
        let mut session = service.bind(bind(), &c).await.unwrap();
        let addr = session.local_addr();
        let mut peer = tokio::net::TcpStream::connect(addr).await.unwrap();
        peer.write_all(
            b"GET /oauth/callback/fixture?code=synthetic HTTP/1.1\r\nHost: localhost\r\n\r\n",
        )
        .await
        .unwrap();
        let request = session.next(&c).await.unwrap();
        assert_eq!(request.method, "GET");
        assert_eq!(request.target, "/oauth/callback/fixture?code=synthetic");
        assert!(!format!("{request:?}").contains("synthetic"));
        let mut byte = [0];
        assert!(
            tokio::time::timeout(Duration::from_millis(30), peer.read(&mut byte))
                .await
                .is_err()
        );
        request.reply.send(response(), &c).await.unwrap();
        let mut wire = Vec::new();
        peer.read_to_end(&mut wire).await.unwrap();
        let wire = String::from_utf8(wire).unwrap();
        assert!(wire.starts_with("HTTP/1.1 200"));
        assert!(wire.contains("Cache-Control: no-store"));
        assert!(wire.ends_with("synthetic-complete"));
        let mut held = tokio::net::TcpStream::connect(addr).await.unwrap();
        held.write_all(b"GET /held HTTP/1.1\r\n\r\n").await.unwrap();
        let held_reply = session.next(&c).await.unwrap().reply;
        session.close().await.unwrap();
        assert!(tokio::net::TcpStream::connect(addr).await.is_err());
        assert_eq!(held.read(&mut byte).await.unwrap(), 0);
        assert!(held_reply.send(response(), &c).await.is_err());
        session.close().await.unwrap();
    });
}
#[test]
fn auth_host_native_drop_cancellation_limits_and_invalid_response() {
    let rt = runtime();
    rt.block_on(async{
 let service=NativeCallbackListener::new(rt.handle().clone());let c=CancellationToken::new();let mut session=service.bind(bind(),&c).await.unwrap();let addr=session.local_addr();
 let cancelled=CancellationToken::new();cancelled.cancel();assert!(matches!(session.next(&cancelled).await,Err(e) if e.kind()==AuthHostErrorKind::Cancelled));
 let mut peer=tokio::net::TcpStream::connect(addr).await.unwrap();peer.write_all(b"GET /fixture HTTP/1.1\r\n\r\n").await.unwrap();let request=session.next(&c).await.unwrap();let mut bad=response();bad.content_type="text/html\r\nInjected: value".into();assert_eq!(request.reply.send(bad,&c).await.unwrap_err().kind(),AuthHostErrorKind::InvalidInput);
 let mut bytes=Vec::new();peer.read_to_end(&mut bytes).await.unwrap();assert!(bytes.is_empty());
 let mut partial=tokio::net::TcpStream::connect(addr).await.unwrap();partial.write_all(b"GET /partial").await.unwrap();drop(session);
 let mut bytes=Vec::new();let result=tokio::time::timeout(Duration::from_secs(2),partial.read_to_end(&mut bytes)).await.unwrap();assert!(result.is_ok() || result.unwrap_err().kind()==std::io::ErrorKind::ConnectionReset);
 let mut config=bind();config.limits.max_connections=0;assert!(service.bind(config,&c).await.is_err());assert!(service.bind(bind(),&cancelled).await.is_err());
});
}
struct Fixed(NativeOAuthCrypto);
impl OAuthCrypto for Fixed {
    fn fill_random(&self, b: &mut [u8]) -> Result<(), AuthHostError> {
        for (i, v) in b.iter_mut().enumerate() {
            *v = i as u8;
        }
        Ok(())
    }
    fn sha256(&self, b: &[u8]) -> Result<[u8; 32], AuthHostError> {
        self.0.sha256(b)
    }
}
#[test]
fn auth_host_pkce_matches_complete_pinned_webcrypto_and_native_entropy() {
    let pkce = generate_pkce(&Fixed(NativeOAuthCrypto)).unwrap();
    assert_eq!(
        pkce.verifier.expose_secret(),
        "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8"
    );
    assert_eq!(
        pkce.challenge,
        "6oZqdX5MOLq_qBJ8vppAnT4fk6AP8UiP9zX8-Rev_9A"
    );
    let native = Arc::new(NativeOAuthCrypto);
    let a = generate_pkce(native.as_ref()).unwrap();
    let b = generate_pkce(native.as_ref()).unwrap();
    assert_eq!(a.verifier.expose_secret().len(), 43);
    assert_eq!(a.challenge.len(), 43);
    assert_ne!(a.verifier.expose_secret(), b.verifier.expose_secret());
    assert!(!format!("{a:?}").contains(a.verifier.expose_secret()));
}

#[test]
fn auth_host_native_bounded_connections_body_and_receive_drop() {
    let rt = runtime();
    rt.block_on(async {
        let service = NativeCallbackListener::new(rt.handle().clone());
        let c = CancellationToken::new();
        let mut config = bind();
        config.limits.max_connections = 1;
        config.limits.max_response_bytes = 4;
        let mut session = service.bind(config, &c).await.unwrap();
        let addr = session.local_addr();
        let mut wait = session.next(&c);
        std::future::poll_fn(|cx| {
            assert!(wait.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        drop(wait);
        let mut peer = tokio::net::TcpStream::connect(addr).await.unwrap();
        peer.write_all(b"POST /fixture HTTP/1.1\r\n\r\n")
            .await
            .unwrap();
        let request = session.next(&c).await.unwrap();
        assert_eq!(request.method, "POST");
        let mut excess = tokio::net::TcpStream::connect(addr).await.unwrap();
        let mut byte = [0];
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), excess.read(&mut byte))
                .await
                .unwrap()
                .unwrap(),
            0
        );
        assert_eq!(
            request.reply.send(response(), &c).await.unwrap_err().kind(),
            AuthHostErrorKind::LimitExceeded
        );
        assert_eq!(peer.read(&mut byte).await.unwrap(), 0);
        session.close().await.unwrap();
        let mut duplicate = bind();
        duplicate.port = addr.port();
        let mut rebound = service.bind(duplicate, &c).await.unwrap();
        let mut conflict = bind();
        conflict.port = rebound.local_addr().port();
        assert_eq!(
            service.bind(conflict, &c).await.err().unwrap().kind(),
            AuthHostErrorKind::Io
        );
        rebound.close().await.unwrap();
    });
}
#[test]
fn auth_host_crypto_failure_is_typed_and_redacted() {
    struct Failed;
    impl OAuthCrypto for Failed {
        fn fill_random(&self, _: &mut [u8]) -> Result<(), AuthHostError> {
            Err(AuthHostError::with_source(
                AuthHostErrorKind::Crypto,
                std::io::Error::other("synthetic-private-detail"),
            ))
        }
        fn sha256(&self, _: &[u8]) -> Result<[u8; 32], AuthHostError> {
            panic!("hash must not run after entropy failure")
        }
    }
    let err = generate_pkce(&Failed).err().unwrap();
    assert_eq!(err.kind(), AuthHostErrorKind::Crypto);
    assert!(!format!("{err:?} {err}").contains("synthetic-private-detail"));
    assert!(std::error::Error::source(&err).is_some());
}
