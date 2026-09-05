//! Single-request Hyper driver with bounded pre-parser HTTP/1 head observation.
use super::{
    AbortTask, SocketIo, decode_userinfo, io_error, latin1_header_bytes, on_runtime, unbracket,
};
use crate::{
    HttpHeaders, HttpRequest, HttpResponse, RawHttpClient, ResponseBody, TransportError,
    TransportErrorKind,
};
use base64::{Engine, engine::general_purpose::STANDARD};
use http_body_util::{BodyExt, Full};
use hyper::{
    Request,
    body::{Bytes, Incoming},
    header::{AUTHORIZATION, HOST, HeaderValue, PROXY_AUTHORIZATION},
};
use hyper_util::rt::{TokioExecutor, TokioIo};
use pi_ai_core::{BoxFuture, RequestRuntime};
use rustls_platform_verifier::BuilderVerifierExt;
use std::{
    io,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf},
    net::TcpStream,
    runtime::Handle,
};
use tokio_rustls::{
    TlsConnector,
    rustls::{
        self,
        pki_types::{CertificateDer, ServerName},
    },
};
use url::Url;

#[derive(Clone)]
pub struct NativeHttpClient {
    handle: Handle,
    roots: Option<Vec<Vec<u8>>>,
    head_limit: usize,
}
impl NativeHttpClient {
    pub fn new(handle: Handle) -> Self {
        Self {
            handle,
            roots: None,
            head_limit: 65_536,
        }
    }
    /// Explicit private trust store; certificate/hostname verification stays enabled.
    pub fn with_root_certificates(
        handle: Handle,
        certificates: &[Vec<u8>],
    ) -> Result<Self, TransportError> {
        let mut roots = rustls::RootCertStore::empty();
        for cert in certificates {
            roots
                .add(CertificateDer::from(cert.clone()))
                .map_err(|_| invalid())?;
        }
        Ok(Self {
            handle,
            roots: Some(certificates.to_vec()),
            head_limit: 65_536,
        })
    }
    /// Caps observed HTTP/1 heads, cumulatively including informational heads.
    /// Body bytes do not count. Zero rejects every HTTP/1 response head.
    pub fn with_response_head_limit(mut self, bytes: usize) -> Self {
        self.head_limit = bytes;
        self
    }
}
impl RawHttpClient for NativeHttpClient {
    fn send<'a>(
        &'a self,
        request: &'a HttpRequest,
        runtime: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<HttpResponse, TransportError>> {
        Box::pin(async move {
            let endpoint = request.endpoint.expose_for_transport().to_owned();
            let method = request.method.clone();
            let headers = request.headers.clone();
            let body = request.body.clone();
            let proxy = request.proxy.clone();
            let roots = self.roots.clone();
            let handle = self.handle.clone();
            let head_limit = self.head_limit;
            crate::policy::cancel_wait(
                Box::pin(on_runtime(&self.handle, async move {
                    let mut url = Url::parse(&endpoint).map_err(|_| invalid())?;
                    if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
                        return Err(invalid());
                    }
                    let origin_auth = basic_auth(&url)?;
                    url.set_username("").map_err(|_| invalid())?;
                    url.set_password(None).map_err(|_| invalid())?;
                    url.set_fragment(None);
                    let host = url.host_str().ok_or_else(invalid)?.to_owned();
                    let port = url.port_or_known_default().ok_or_else(invalid)?;
                    let mut wire = Request::builder()
                        .method(
                            hyper::Method::from_bytes(method.as_bytes()).map_err(|_| invalid())?,
                        )
                        .body(Full::new(Bytes::from(body)))
                        .map_err(|_| invalid())?;
                    for (name, value) in headers.iter_for_wire() {
                        let mut value = HeaderValue::from_bytes(&latin1_header_bytes(value)?)
                            .map_err(|_| invalid())?;
                        value.set_sensitive(true);
                        wire.headers_mut().append(
                            hyper::header::HeaderName::from_bytes(name.as_bytes())
                                .map_err(|_| invalid())?,
                            value,
                        );
                    }
                    if !wire.headers().contains_key(HOST) {
                        wire.headers_mut().insert(
                            HOST,
                            HeaderValue::from_str(&match url.port() {
                                Some(port) => format!("{host}:{port}"),
                                None => host.clone(),
                            })
                            .map_err(|_| invalid())?,
                        );
                    }
                    if !wire.headers().contains_key("accept") {
                        wire.headers_mut()
                            .insert("accept", HeaderValue::from_static("*/*"));
                    }
                    if let Some(auth) = origin_auth
                        && !wire.headers().contains_key(AUTHORIZATION)
                    {
                        wire.headers_mut().insert(AUTHORIZATION, auth);
                    }
                    let mut absolute = false;
                    let mut stream: Box<dyn SocketIo> = if let Some(proxy) = proxy {
                        let proxy =
                            Url::parse(proxy.expose_for_transport()).map_err(|_| invalid())?;
                        let proxy_host = proxy.host_str().ok_or_else(invalid)?;
                        let proxy_port = proxy.port_or_known_default().ok_or_else(invalid)?;
                        let tcp = TcpStream::connect((unbracket(proxy_host), proxy_port))
                            .await
                            .map_err(io_error)?;
                        tcp.set_nodelay(true).map_err(io_error)?;
                        let mut stream: Box<dyn SocketIo> = Box::new(tcp);
                        if proxy.scheme() == "https" {
                            stream = tls(stream, proxy_host, &roots, false).await?.0;
                        }
                        let auth = basic_auth(&proxy)?;
                        if url.scheme() == "https" {
                            tunnel(&mut stream, &format!("{host}:{port}"), auth.as_ref()).await?;
                        } else {
                            absolute = true;
                            if let Some(auth) = auth {
                                wire.headers_mut().insert(PROXY_AUTHORIZATION, auth);
                            }
                        }
                        stream
                    } else {
                        let tcp = TcpStream::connect((unbracket(&host), port))
                            .await
                            .map_err(io_error)?;
                        tcp.set_nodelay(true).map_err(io_error)?;
                        Box::new(tcp)
                    };
                    let mut h2 = false;
                    if url.scheme() == "https" {
                        let secured = tls(stream, &host, &roots, true).await?;
                        stream = secured.0;
                        h2 = secured.1;
                    }
                    let uri = if absolute || h2 {
                        url.as_str().to_owned()
                    } else {
                        let mut path = url.path().to_owned();
                        if let Some(query) = url.query() {
                            path.push('?');
                            path.push_str(query);
                        }
                        path
                    };
                    *wire.uri_mut() = uri.parse().map_err(|_| invalid())?;
                    let capture = Arc::new(Mutex::new(HeadCapture::new(head_limit)));
                    let (response, driver) = if h2 {
                        let (mut sender, connection) = hyper::client::conn::http2::handshake(
                            TokioExecutor::new(),
                            TokioIo::new(stream),
                        )
                        .await
                        .map_err(|_| connection_error())?;
                        let driver = AbortTask(handle.spawn(async move {
                            let _ = connection.await;
                        }));
                        (
                            sender
                                .send_request(wire)
                                .await
                                .map_err(|_| connection_error())?,
                            driver,
                        )
                    } else {
                        let io = ObserveHead {
                            inner: stream,
                            capture: capture.clone(),
                        };
                        let (mut sender, connection) =
                            hyper::client::conn::http1::handshake(TokioIo::new(io))
                                .await
                                .map_err(|_| connection_error())?;
                        let driver = AbortTask(handle.spawn(async move {
                            let _ = connection.await;
                        }));
                        let response = sender.send_request(wire).await.map_err(|_| {
                            if capture.lock().unwrap_or_else(|p| p.into_inner()).exceeded {
                                TransportError::new(TransportErrorKind::LimitExceeded)
                            } else {
                                connection_error()
                            }
                        })?;
                        (response, driver)
                    };
                    let status = response.status().as_u16();
                    let status_text = if h2 {
                        String::new()
                    } else {
                        let bytes = capture
                            .lock()
                            .unwrap_or_else(|p| p.into_inner())
                            .phrase
                            .take()
                            .ok_or_else(|| TransportError::new(TransportErrorKind::Protocol))?;
                        bytes.into_iter().map(char::from).collect()
                    };
                    let mut headers = HttpHeaders::new();
                    for (name, value) in response.headers() {
                        headers.append(
                            name.as_str(),
                            value
                                .as_bytes()
                                .iter()
                                .copied()
                                .map(char::from)
                                .collect::<String>(),
                        )?;
                    }
                    Ok(HttpResponse {
                        status,
                        status_text,
                        headers,
                        body: Box::new(NativeBody {
                            handle,
                            body: Some(response.into_body()),
                            driver: Some(driver),
                        }),
                    })
                })),
                &runtime.cancellation,
            )
            .await
        })
    }
}
fn invalid() -> TransportError {
    TransportError::new(TransportErrorKind::InvalidRequest)
}
fn connection_error() -> TransportError {
    TransportError::new(TransportErrorKind::Connection)
}
fn basic_auth(url: &Url) -> Result<Option<HeaderValue>, TransportError> {
    if url.username().is_empty() && url.password().is_none() {
        return Ok(None);
    }
    let credentials = format!(
        "{}:{}",
        decode_userinfo(url.username())?,
        decode_userinfo(url.password().unwrap_or(""))?
    );
    let mut header = HeaderValue::from_str(&format!("Basic {}", STANDARD.encode(credentials)))
        .map_err(|_| invalid())?;
    header.set_sensitive(true);
    Ok(Some(header))
}
async fn tls(
    stream: Box<dyn SocketIo>,
    host: &str,
    roots: &Option<Vec<Vec<u8>>>,
    offer_h2: bool,
) -> Result<(Box<dyn SocketIo>, bool), TransportError> {
    let builder = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|_| connection_error())?;
    let mut config = if let Some(certificates) = roots {
        let mut store = rustls::RootCertStore::empty();
        for cert in certificates {
            store
                .add(CertificateDer::from(cert.clone()))
                .map_err(|_| invalid())?;
        }
        builder.with_root_certificates(store).with_no_client_auth()
    } else {
        builder
            .with_platform_verifier()
            .map_err(|_| connection_error())?
            .with_no_client_auth()
    };
    config.alpn_protocols = if offer_h2 {
        vec![b"h2".to_vec(), b"http/1.1".to_vec()]
    } else {
        vec![b"http/1.1".to_vec()]
    };
    let name = ServerName::try_from(unbracket(host).to_owned()).map_err(|_| invalid())?;
    let stream = TlsConnector::from(Arc::new(config))
        .connect(name, stream)
        .await
        .map_err(io_error)?;
    let h2 = stream.get_ref().1.alpn_protocol() == Some(b"h2");
    Ok((Box::new(stream), h2))
}
async fn tunnel(
    stream: &mut Box<dyn SocketIo>,
    authority: &str,
    auth: Option<&HeaderValue>,
) -> Result<(), TransportError> {
    let mut request = format!("CONNECT {authority} HTTP/1.1\r\nHost: {authority}\r\n").into_bytes();
    if let Some(auth) = auth {
        request.extend_from_slice(b"Proxy-Authorization: ");
        request.extend_from_slice(auth.as_bytes());
        request.extend_from_slice(b"\r\n");
    }
    request.extend_from_slice(b"\r\n");
    stream.write_all(&request).await.map_err(io_error)?;
    stream.flush().await.map_err(io_error)?;
    let mut head = Vec::new();
    let mut byte = [0];
    while !head.ends_with(b"\r\n\r\n") {
        if head.len() == 16_384 {
            return Err(TransportError::new(TransportErrorKind::LimitExceeded));
        }
        stream.read_exact(&mut byte).await.map_err(io_error)?;
        head.push(byte[0]);
    }
    if head.get(9..12) != Some(b"200") {
        return Err(connection_error());
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum HeadPhase {
    AwaitingStatus,
    Headers { informational: bool },
    Complete,
}
struct HeadCapture {
    line: Vec<u8>,
    phase: HeadPhase,
    pending_phrase: Vec<u8>,
    total: usize,
    limit: usize,
    phrase: Option<Vec<u8>>,
    exceeded: bool,
}
impl HeadCapture {
    fn new(limit: usize) -> Self {
        Self {
            line: Vec::new(),
            phase: HeadPhase::AwaitingStatus,
            pending_phrase: Vec::new(),
            total: 0,
            limit,
            phrase: None,
            exceeded: false,
        }
    }
    fn observe(&mut self, bytes: &[u8]) -> io::Result<()> {
        for &byte in bytes {
            if matches!(self.phase, HeadPhase::Complete) {
                break;
            }
            if self.total >= self.limit {
                self.exceeded = true;
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "response head limit",
                ));
            }
            // Prefix, status and header bytes all belong to the same budget.
            self.total += 1;
            self.line.push(byte);
            if byte != b'\n' {
                continue;
            }
            let line = &self.line[..self.line.len() - 1];
            let line = line.strip_suffix(b"\r").unwrap_or(line);
            match self.phase {
                HeadPhase::AwaitingStatus if line.is_empty() => {
                    // httparse::skip_empty_lines accepts repeated LF and CRLF,
                    // also before the next response after an informational head.
                }
                HeadPhase::AwaitingStatus => {
                    // Hyper keeps responsibility for status/header validation.
                    // These offsets follow its default status-line delimiters.
                    let code = line
                        .get(9..12)
                        .and_then(|bytes| std::str::from_utf8(bytes).ok())
                        .and_then(|text| text.parse::<u16>().ok());
                    self.pending_phrase = if line.get(12) == Some(&b' ') {
                        line[13..].to_vec()
                    } else {
                        Vec::new()
                    };
                    self.phase = HeadPhase::Headers {
                        informational: code
                            .is_some_and(|code| (100..200).contains(&code) && code != 101),
                    };
                }
                HeadPhase::Headers { informational } if line.is_empty() => {
                    if informational {
                        self.pending_phrase.clear();
                        self.phase = HeadPhase::AwaitingStatus;
                    } else {
                        self.phrase = Some(std::mem::take(&mut self.pending_phrase));
                        self.phase = HeadPhase::Complete;
                    }
                }
                HeadPhase::Headers { .. } | HeadPhase::Complete => {}
            }
            self.line.clear();
        }
        Ok(())
    }
}
struct ObserveHead<S> {
    inner: S,
    capture: Arc<Mutex<HeadCapture>>,
}
impl<S: AsyncRead + Unpin> AsyncRead for ObserveHead<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let before = buf.filled().len();
        let result = Pin::new(&mut this.inner).poll_read(cx, buf);
        if matches!(result, Poll::Ready(Ok(())))
            && let Err(error) = this
                .capture
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .observe(&buf.filled()[before..])
        {
            return Poll::Ready(Err(error));
        }
        result
    }
}
impl<S: AsyncWrite + Unpin> AsyncWrite for ObserveHead<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, bytes)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}
struct NativeBody {
    handle: Handle,
    body: Option<Incoming>,
    driver: Option<AbortTask<()>>,
}
impl ResponseBody for NativeBody {
    fn next<'a>(
        &'a mut self,
        runtime: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, TransportError>> {
        Box::pin(async move {
            let Some(mut body) = self.body.take() else {
                return Ok(None);
            };
            let driver = self.driver.take();
            let (body, driver, chunk) = crate::policy::cancel_wait(
                Box::pin(on_runtime(&self.handle, async move {
                    let chunk = loop {
                        match body.frame().await {
                            Some(Ok(frame)) => {
                                if let Ok(data) = frame.into_data() {
                                    break Some(data.to_vec());
                                }
                            }
                            Some(Err(_)) => return Err(connection_error()),
                            None => break None,
                        }
                    };
                    Ok((body, driver, chunk))
                })),
                &runtime.cancellation,
            )
            .await?;
            if chunk.is_some() {
                self.body = Some(body);
                self.driver = driver;
            }
            Ok(chunk)
        })
    }
    fn close(&mut self) {
        self.body = None;
        self.driver = None;
    }
}

#[cfg(test)]
mod head_tests {
    use super::{HeadCapture, HeadPhase};

    #[test]
    fn prefixes_status_and_headers_are_distinct_states() {
        let mut capture = HeadCapture::new(128);
        for byte in b"\r\n\n\r\n" {
            capture.observe(&[*byte]).unwrap();
            assert!(matches!(capture.phase, HeadPhase::AwaitingStatus));
            assert!(capture.phrase.is_none());
        }
        capture.observe(b"HTTP/1.1 200 Custom\r").unwrap();
        assert!(matches!(capture.phase, HeadPhase::AwaitingStatus));
        capture.observe(b"\n").unwrap();
        assert!(matches!(
            capture.phase,
            HeadPhase::Headers {
                informational: false
            }
        ));
        assert!(capture.phrase.is_none());
        capture.observe(b"X-Test: yes\r\n\r").unwrap();
        assert!(matches!(capture.phase, HeadPhase::Headers { .. }));
        assert!(capture.phrase.is_none());
        capture.observe(b"\nbody\n\n").unwrap();
        assert!(matches!(capture.phase, HeadPhase::Complete));
        assert_eq!(capture.phrase.as_deref(), Some(b"Custom".as_slice()));
    }

    #[test]
    fn all_prefix_and_head_splits_preserve_metadata_and_cumulative_budget() {
        for prefix in [b"".as_slice(), b"\n", b"\r\n", b"\n\r\n\r\n\n"] {
            for ending in [b"\n".as_slice(), b"\r\n"] {
                for informational in [false, true] {
                    let mut head = prefix.to_vec();
                    if informational {
                        for status in [b"HTTP/1.1 103 Early".as_slice(), b"HTTP/1.1 100 Continue"] {
                            head.extend_from_slice(status);
                            head.extend_from_slice(ending);
                            head.extend_from_slice(ending);
                            head.extend_from_slice(prefix);
                        }
                    }
                    head.extend_from_slice(b"HTTP/1.1 200 Custom \xe9\xff");
                    head.extend_from_slice(ending);
                    head.extend_from_slice(b"Content-Length: 2");
                    head.extend_from_slice(ending);
                    head.extend_from_slice(ending);
                    for split in 0..=head.len() {
                        let mut capture = HeadCapture::new(head.len());
                        capture.observe(&head[..split]).unwrap();
                        capture.observe(&head[split..]).unwrap();
                        capture.observe(b"ok\n\n").unwrap();
                        assert_eq!(capture.total, head.len());
                        assert_eq!(
                            capture.phrase.as_deref(),
                            Some(b"Custom \xe9\xff".as_slice())
                        );
                        assert!(matches!(capture.phase, HeadPhase::Complete));
                        let mut short = HeadCapture::new(head.len() - 1);
                        let result = short
                            .observe(&head[..split])
                            .and_then(|()| short.observe(&head[split..]));
                        assert!(result.is_err());
                        assert!(short.exceeded);
                        assert!(short.phrase.is_none());
                        assert!(!matches!(short.phase, HeadPhase::Complete));
                    }
                    let mut bytewise = HeadCapture::new(head.len());
                    for byte in &head {
                        bytewise.observe(&[*byte]).unwrap();
                    }
                    assert_eq!(
                        bytewise.phrase.as_deref(),
                        Some(b"Custom \xe9\xff".as_slice())
                    );
                    assert_eq!(bytewise.total, head.len());
                }
            }
        }
    }
}
