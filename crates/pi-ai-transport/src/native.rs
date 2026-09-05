//! Optional Tokio/Rustls implementation. Work is driven on the explicitly
//! supplied Tokio handle, even when the consumer future is polled elsewhere.
use crate::{
    RetryJitter, TransportError, TransportErrorKind, WebSocketConnection, WebSocketConnector,
    WebSocketFrame, WebSocketRequest,
};
use base64::{Engine, engine::general_purpose::STANDARD};
use futures_util::{SinkExt, StreamExt};
use pi_ai_core::{BoxFuture, CancellationToken, Cancelled, Clock, RequestRuntime};
use std::{
    future::Future,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpStream,
    runtime::Handle,
    task::JoinHandle,
};
use tokio_rustls::{
    TlsConnector,
    rustls::{self, pki_types::ServerName},
};
use tokio_tungstenite::{
    Connector, MaybeTlsStream, WebSocketStream, client_async_tls_with_config,
    tungstenite::{
        self, Message,
        client::IntoClientRequest,
        protocol::{CloseFrame, WebSocketConfig, frame::coding::CloseCode},
    },
};
use url::Url;

struct AbortTask<T>(JoinHandle<T>);
impl<T> Drop for AbortTask<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}
async fn on_runtime<T: Send + 'static>(
    handle: &Handle,
    future: impl Future<Output = Result<T, TransportError>> + Send + 'static,
) -> Result<T, TransportError> {
    let mut task = AbortTask(handle.spawn(future));
    (&mut task.0)
        .await
        .map_err(|_| TransportError::new(TransportErrorKind::RuntimeUnavailable))?
}

/// Real host clock. Timer tasks abort on drop and cannot outlive a cancelled wait.
#[derive(Clone)]
pub struct TokioClock {
    handle: Handle,
}
impl TokioClock {
    pub fn new(handle: Handle) -> Self {
        Self { handle }
    }
}
impl Clock for TokioClock {
    fn now_unix_ms(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .min(u128::from(u64::MAX)) as u64
    }
    fn sleep<'a>(
        &'a self,
        duration: Duration,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), Cancelled>> {
        Box::pin(async move {
            crate::policy::cancel_wait(
                Box::pin(on_runtime(&self.handle, async move {
                    tokio::time::sleep(duration).await;
                    Ok(())
                })),
                cancellation,
            )
            .await
            .map_err(|_| Cancelled)
        })
    }
}
/// TLS backend entropy; failures are surfaced as invalid jitter by policy.
pub struct NativeRetryJitter;
impl RetryJitter for NativeRetryJitter {
    fn sample(&self) -> f64 {
        let mut bytes = [0; 8];
        if rustls::crypto::aws_lc_rs::default_provider()
            .secure_random
            .fill(&mut bytes)
            .is_err()
        {
            return f64::NAN;
        }
        (u64::from_le_bytes(bytes) >> 11) as f64 / 9_007_199_254_740_992.0
    }
}

#[path = "native_http.rs"]
mod native_http;
pub use native_http::NativeHttpClient;

fn latin1_header_bytes(value: &str) -> Result<Vec<u8>, TransportError> {
    value
        .chars()
        .map(|c| {
            u8::try_from(u32::from(c))
                .map_err(|_| TransportError::new(TransportErrorKind::InvalidRequest))
        })
        .collect()
}

trait SocketIo: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin> SocketIo for T {}
type Socket = WebSocketStream<MaybeTlsStream<Box<dyn SocketIo>>>;

/// Instance-owned connector; no global session cache or registration side effects.
#[derive(Clone)]
pub struct NativeWebSocketConnector {
    handle: Handle,
    tls: Arc<rustls::ClientConfig>,
}
impl NativeWebSocketConnector {
    pub fn new(handle: Handle) -> Result<Self, TransportError> {
        let roots = rustls::RootCertStore {
            roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
        };
        let tls = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(|_| TransportError::new(TransportErrorKind::Protocol))?
        .with_root_certificates(roots)
        .with_no_client_auth();
        Ok(Self {
            handle,
            tls: Arc::new(tls),
        })
    }
    /// Explicit host TLS configuration for private PKI; never disables verification.
    pub fn with_tls_config(handle: Handle, tls: Arc<rustls::ClientConfig>) -> Self {
        Self { handle, tls }
    }
}
impl WebSocketConnector for NativeWebSocketConnector {
    fn connect<'a>(
        &'a self,
        request: &'a WebSocketRequest,
        runtime: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<Box<dyn WebSocketConnection>, TransportError>> {
        Box::pin(async move {
            let endpoint = request.endpoint.expose_for_transport().to_owned();
            let headers = request.headers.clone();
            let proxy = request.proxy.clone();
            let tls = self.tls.clone();
            let handle = self.handle.clone();
            let max_message = request.max_message_bytes;
            let max_frame = request.max_frame_bytes;
            let operation = Box::pin(on_runtime(&self.handle, async move {
                let url = Url::parse(&endpoint)
                    .map_err(|_| TransportError::new(TransportErrorKind::InvalidRequest))?;
                if !matches!(url.scheme(), "ws" | "wss") || max_message == 0 || max_frame == 0 {
                    return Err(TransportError::new(TransportErrorKind::InvalidRequest));
                }
                let mut handshake_request = endpoint.into_client_request().map_err(ws_error)?;
                for (name, value) in headers.iter_for_wire() {
                    let mut value =
                        tungstenite::http::HeaderValue::from_bytes(&latin1_header_bytes(value)?)
                            .map_err(|_| TransportError::new(TransportErrorKind::InvalidRequest))?;
                    value.set_sensitive(true);
                    handshake_request.headers_mut().insert(
                        tungstenite::http::HeaderName::from_bytes(name.as_bytes())
                            .map_err(|_| TransportError::new(TransportErrorKind::InvalidRequest))?,
                        value,
                    );
                }
                let host = url
                    .host_str()
                    .ok_or_else(|| TransportError::new(TransportErrorKind::InvalidRequest))?;
                let port = url
                    .port_or_known_default()
                    .ok_or_else(|| TransportError::new(TransportErrorKind::InvalidRequest))?;
                let socket: Box<dyn SocketIo> = if let Some(proxy) = proxy {
                    let proxy = Url::parse(proxy.expose_for_transport())
                        .map_err(|_| TransportError::new(TransportErrorKind::InvalidProxy))?;
                    let proxy_host = proxy
                        .host_str()
                        .ok_or_else(|| TransportError::new(TransportErrorKind::InvalidProxy))?;
                    let proxy_port = proxy
                        .port_or_known_default()
                        .ok_or_else(|| TransportError::new(TransportErrorKind::InvalidProxy))?;
                    let tcp = TcpStream::connect((unbracket(proxy_host), proxy_port))
                        .await
                        .map_err(io_error)?;
                    let mut stream: Box<dyn SocketIo> = if proxy.scheme() == "https" {
                        let name = ServerName::try_from(unbracket(proxy_host).to_owned())
                            .map_err(|_| TransportError::new(TransportErrorKind::InvalidProxy))?;
                        Box::new(
                            TlsConnector::from(tls.clone())
                                .connect(name, tcp)
                                .await
                                .map_err(io_error)?,
                        )
                    } else {
                        Box::new(tcp)
                    };
                    let authority = format!("{host}:{port}");
                    let auth = if proxy.username().is_empty() && proxy.password().is_none() {
                        String::new()
                    } else {
                        format!(
                            "Proxy-Authorization: Basic {}\r\n",
                            STANDARD.encode(format!(
                                "{}:{}",
                                decode_userinfo(proxy.username())?,
                                decode_userinfo(proxy.password().unwrap_or(""))?
                            ))
                        )
                    };
                    stream
                        .write_all(
                            format!(
                                "CONNECT {authority} HTTP/1.1\r\nHost: {authority}\r\n{auth}\r\n"
                            )
                            .as_bytes(),
                        )
                        .await
                        .map_err(io_error)?;
                    // Read only the CONNECT head; consuming tunneled bytes would
                    // corrupt the origin TLS/WS handshake. Bound hostile proxies.
                    let mut head = Vec::new();
                    let mut byte = [0];
                    while !head.ends_with(b"\r\n\r\n") {
                        if head.len() == 16_384 {
                            return Err(TransportError::new(TransportErrorKind::LimitExceeded));
                        }
                        stream.read_exact(&mut byte).await.map_err(io_error)?;
                        head.push(byte[0]);
                    }
                    let status = std::str::from_utf8(&head)
                        .ok()
                        .and_then(|s| s.lines().next())
                        .and_then(|s| s.split_whitespace().nth(1));
                    if status != Some("200") {
                        return Err(TransportError::new(TransportErrorKind::Connection));
                    }
                    stream
                } else {
                    Box::new(
                        TcpStream::connect((unbracket(host), port))
                            .await
                            .map_err(io_error)?,
                    )
                };
                let config = WebSocketConfig::default()
                    .max_message_size(Some(max_message))
                    .max_frame_size(Some(max_frame));
                let (socket, _) = client_async_tls_with_config(
                    handshake_request,
                    socket,
                    Some(config),
                    Some(Connector::Rustls(tls)),
                )
                .await
                .map_err(ws_error)?;
                Ok(Box::new(NativeWebSocket {
                    handle,
                    socket: Some(socket),
                    max_message_bytes: max_message,
                }) as Box<dyn WebSocketConnection>)
            }));
            crate::policy::wait_with_timeout(operation, runtime, request.connect_timeout).await
        })
    }
}
fn unbracket(host: &str) -> &str {
    host.strip_prefix('[')
        .and_then(|s| s.strip_suffix(']'))
        .unwrap_or(host)
}
fn decode_userinfo(value: &str) -> Result<String, TransportError> {
    let mut out = Vec::new();
    let mut chars = value.bytes();
    while let Some(c) = chars.next() {
        if c == b'%' {
            let a = chars.next().and_then(|b| (b as char).to_digit(16));
            let b = chars.next().and_then(|b| (b as char).to_digit(16));
            match (a, b) {
                (Some(a), Some(b)) => out.push((a * 16 + b) as u8),
                _ => return Err(TransportError::new(TransportErrorKind::InvalidProxy)),
            }
        } else {
            out.push(c)
        }
    }
    String::from_utf8(out).map_err(|_| TransportError::new(TransportErrorKind::InvalidProxy))
}
fn io_error(_: std::io::Error) -> TransportError {
    TransportError::new(TransportErrorKind::Connection)
}
fn ws_error(error: tungstenite::Error) -> TransportError {
    TransportError::new(match error {
        tungstenite::Error::Capacity(_) => TransportErrorKind::LimitExceeded,
        tungstenite::Error::ConnectionClosed | tungstenite::Error::AlreadyClosed => {
            TransportErrorKind::Closed
        }
        tungstenite::Error::Io(_) | tungstenite::Error::Tls(_) => TransportErrorKind::Connection,
        _ => TransportErrorKind::Protocol,
    })
}
struct NativeWebSocket {
    handle: Handle,
    socket: Option<Socket>,
    max_message_bytes: usize,
}
impl WebSocketConnection for NativeWebSocket {
    fn send<'a>(
        &'a mut self,
        frame: WebSocketFrame,
        runtime: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<(), TransportError>> {
        Box::pin(async move {
            if self.socket.is_none() {
                return Err(TransportError::new(TransportErrorKind::Closed));
            }
            // Validate local input before taking the live resource or writing
            // bytes. Tungstenite constructs outgoing control frames unchecked.
            let payload_len = match &frame {
                WebSocketFrame::Text(value) => value.len(),
                WebSocketFrame::Binary(value) => value.len(),
                WebSocketFrame::Ping(value) | WebSocketFrame::Pong(value) => {
                    if value.len() > 125 {
                        return Err(TransportError::new(TransportErrorKind::LimitExceeded));
                    }
                    value.len()
                }
                WebSocketFrame::Close { code, reason } => {
                    if code.is_none() && !reason.is_empty()
                        || code.is_some_and(|code| !CloseCode::from(code).is_allowed())
                    {
                        return Err(TransportError::new(TransportErrorKind::InvalidRequest));
                    }
                    if code.is_some() && reason.len() > 123 {
                        return Err(TransportError::new(TransportErrorKind::LimitExceeded));
                    }
                    reason.len() + if code.is_some() { 2 } else { 0 }
                }
            };
            if payload_len > self.max_message_bytes {
                return Err(TransportError::new(TransportErrorKind::LimitExceeded));
            }
            let mut socket = self
                .socket
                .take()
                .ok_or_else(|| TransportError::new(TransportErrorKind::Closed))?;
            let msg = match frame {
                WebSocketFrame::Text(s) => Message::Text(s.into()),
                WebSocketFrame::Binary(b) => Message::Binary(b.into()),
                WebSocketFrame::Ping(b) => Message::Ping(b.into()),
                WebSocketFrame::Pong(b) => Message::Pong(b.into()),
                WebSocketFrame::Close { code, reason } => {
                    Message::Close(code.map(|code| CloseFrame {
                        code: CloseCode::from(code),
                        reason: reason.into(),
                    }))
                }
            };
            let closing = matches!(msg, Message::Close(_));
            let socket = crate::policy::cancel_wait(
                Box::pin(on_runtime(&self.handle, async move {
                    socket.send(msg).await.map_err(ws_error)?;
                    Ok(socket)
                })),
                &runtime.cancellation,
            )
            .await?;
            if !closing {
                self.socket = Some(socket);
            }
            Ok(())
        })
    }
    fn next<'a>(
        &'a mut self,
        runtime: &'a RequestRuntime,
        idle_timeout: Option<Duration>,
    ) -> BoxFuture<'a, Result<Option<WebSocketFrame>, TransportError>> {
        Box::pin(async move {
            let Some(mut socket) = self.socket.take() else {
                return Ok(None);
            };
            let operation = Box::pin(on_runtime(&self.handle, async move {
                loop {
                    let frame = match socket.next().await.transpose().map_err(ws_error)? {
                        None => None,
                        Some(Message::Text(s)) => Some(WebSocketFrame::Text(s.to_string())),
                        Some(Message::Binary(b)) => Some(WebSocketFrame::Binary(b.to_vec())),
                        Some(Message::Ping(b)) => {
                            socket.flush().await.map_err(ws_error)?;
                            Some(WebSocketFrame::Ping(b.to_vec()))
                        }
                        Some(Message::Pong(b)) => Some(WebSocketFrame::Pong(b.to_vec())),
                        Some(Message::Close(c)) => {
                            // Reading Close queues its mandatory reply. Flush
                            // while still inside this operation's cancellation
                            // and idle deadline race, then release the socket.
                            socket.flush().await.map_err(ws_error)?;
                            Some(WebSocketFrame::Close {
                                code: c.as_ref().map(|c| u16::from(c.code)),
                                reason: c.map(|c| c.reason.to_string()).unwrap_or_default(),
                            })
                        }
                        Some(Message::Frame(_)) => continue,
                    };
                    return Ok((socket, frame));
                }
            }));
            let (socket, frame) =
                crate::policy::wait_with_timeout(operation, runtime, idle_timeout).await?;
            if !matches!(frame, None | Some(WebSocketFrame::Close { .. })) {
                self.socket = Some(socket);
            }
            Ok(frame)
        })
    }
    fn close(&mut self) {
        self.socket = None;
    }
}

#[cfg(test)]
mod control_tests {
    use super::*;
    use pi_ai_core::{SpawnError, TaskSpawner};
    use std::{
        future::poll_fn,
        pin::Pin,
        task::{Context, Poll},
    };
    use tokio::io::{DuplexStream, ReadBuf};
    use tokio::sync::Notify;
    use tungstenite::protocol::Role;

    struct NoSpawn;
    impl TaskSpawner for NoSpawn {
        fn spawn(&self, _: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
            Err(SpawnError::new())
        }
    }
    struct BlockWrites {
        inner: DuplexStream,
        attempted: Arc<Notify>,
    }
    impl AsyncRead for BlockWrites {
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
        }
    }
    impl AsyncWrite for BlockWrites {
        fn poll_write(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            _: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            self.attempted.notify_one();
            Poll::Pending
        }
        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Pending
        }
        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }
    #[test]
    fn peer_close_reply_flush_remains_cancelled_and_deadline_bounded() {
        let native = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        native.block_on(async {
            for cancelled in [true, false] {
                let runtime = RequestRuntime::with_noop_telemetry(
                    CancellationToken::new(),
                    Arc::new(TokioClock::new(native.handle().clone())),
                    Arc::new(NoSpawn),
                );
                let (client, mut peer) = tokio::io::duplex(16);
                let attempted = Arc::new(Notify::new());
                let stream: Box<dyn SocketIo> = Box::new(BlockWrites {
                    inner: client,
                    attempted: attempted.clone(),
                });
                let socket = WebSocketStream::from_raw_socket(
                    MaybeTlsStream::Plain(stream),
                    Role::Client,
                    None,
                )
                .await;
                let mut connection = NativeWebSocket {
                    handle: native.handle().clone(),
                    socket: Some(socket),
                    max_message_bytes: 1024,
                };
                // A valid unmasked empty peer Close. Outbound reply writes stall.
                peer.write_all(&[0x88, 0]).await.unwrap();
                let timeout = if cancelled {
                    None
                } else {
                    Some(Duration::from_millis(50))
                };
                let mut next = connection.next(&runtime, timeout);
                let mut notice = Box::pin(attempted.notified());
                tokio::time::timeout(
                    Duration::from_secs(2),
                    poll_fn(|cx| {
                        assert!(
                            next.as_mut().poll(cx).is_pending(),
                            "Close must not escape before reply flush"
                        );
                        notice.as_mut().poll(cx)
                    }),
                )
                .await
                .unwrap();
                if cancelled {
                    runtime.cancellation.cancel();
                }
                let error = tokio::time::timeout(Duration::from_secs(2), next)
                    .await
                    .unwrap()
                    .unwrap_err();
                assert_eq!(
                    error.kind(),
                    if cancelled {
                        TransportErrorKind::Cancelled
                    } else {
                        TransportErrorKind::Timeout
                    }
                );
                assert!(connection.socket.is_none());
                let mut bytes = Vec::new();
                tokio::time::timeout(Duration::from_secs(2), peer.read_to_end(&mut bytes))
                    .await
                    .unwrap()
                    .unwrap();
                assert!(bytes.is_empty());
            }
        });
    }
}
