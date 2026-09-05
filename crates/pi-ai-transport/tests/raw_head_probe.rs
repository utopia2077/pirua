#![cfg(feature = "native")]
//! Minimal same-connection proof before replacing the native HTTP driver.
use http_body_util::Full;
use hyper::{Request, body::Bytes};
use hyper_util::rt::TokioIo;
use std::{
    io::Write,
    net::TcpListener,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    thread,
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

struct Observe<S> {
    inner: S,
    line: Arc<Mutex<Vec<u8>>>,
}
impl<S: AsyncRead + Unpin> AsyncRead for Observe<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        let before = buf.filled().len();
        let result = Pin::new(&mut this.inner).poll_read(cx, buf);
        if matches!(result, Poll::Ready(Ok(()))) {
            let mut line = this.line.lock().unwrap();
            for byte in &buf.filled()[before..] {
                if line.ends_with(b"\n") {
                    break;
                }
                assert!(line.len() < 8192);
                line.push(*byte);
            }
        }
        result
    }
}
impl<S: AsyncWrite + Unpin> AsyncWrite for Observe<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, bytes)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}
#[test]
fn original_reason_is_observable_on_the_same_io_before_httparse_discards_it() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        let mut bytes = Vec::new();
        let mut byte = [0];
        while !bytes.ends_with(b"\r\n\r\n") {
            std::io::Read::read_exact(&mut socket, &mut byte).unwrap();
            bytes.push(byte[0]);
        }
        socket.write_all(b"HTTP/1.1 429 Rate limited \xe9\xff\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").unwrap();
    });
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let line = Arc::new(Mutex::new(Vec::new()));
        let io = Observe {
            inner: tokio::net::TcpStream::connect(address).await.unwrap(),
            line: line.clone(),
        };
        let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(io))
            .await
            .unwrap();
        let driver = tokio::spawn(connection);
        let response = sender
            .send_request(
                Request::builder()
                    .uri("/")
                    .header("host", "localhost")
                    .body(Full::new(Bytes::new()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), 429);
        assert_eq!(
            response
                .extensions()
                .get::<hyper::ext::ReasonPhrase>()
                .unwrap()
                .as_bytes(),
            b""
        );
        assert_eq!(
            &*line.lock().unwrap(),
            b"HTTP/1.1 429 Rate limited \xe9\xff\r\n"
        );
        drop(response);
        drop(sender);
        driver.await.unwrap().unwrap();
    });
    server.join().unwrap();
}
