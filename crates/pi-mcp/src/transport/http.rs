use super::{SseParser, Transport, TransportEvent};
use crate::{
    BoxFuture, CancellationToken, DEFAULT_MAX_MESSAGE_BYTES, Error, JsonRpcId, Result,
    validate_message,
};
use reqwest::{
    Client, Method, Response,
    header::{HeaderMap, HeaderValue},
};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::mpsc;
use url::Url;

#[derive(Clone)]
pub struct UnauthorizedContext {
    pub server_url: Url,
    pub status: u16,
    pub challenge: Option<String>,
    pub rejected_token: Option<String>,
}
/// Credentials are supplied by the host. Recovery is invoked at most once for
/// each HTTP dispatch; concurrent OAuth refreshes are coordinated by OAuthProvider.
pub trait AuthProvider: Send + Sync {
    /// Server-scoped providers reject a mismatched transport before any token is
    /// read or sent. Generic host providers may intentionally support many URLs.
    fn validate_server(&self, _server: &Url) -> Result<()> {
        Ok(())
    }
    fn token(&self) -> BoxFuture<'_, Result<Option<String>>>;
    fn on_unauthorized(&self, context: UnauthorizedContext) -> BoxFuture<'_, Result<()>>;
}
#[derive(Clone, Debug)]
pub struct ReconnectOptions {
    pub initial_delay: Duration,
    pub max_delay: Duration,
    pub max_retries: usize,
}
impl Default for ReconnectOptions {
    fn default() -> Self {
        Self {
            initial_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(30),
            max_retries: 5,
        }
    }
}
#[derive(Clone)]
pub struct HttpOptions {
    pub url: Url,
    pub headers: HeaderMap,
    /// Optional custom network/TLS/proxy client. Hosts must disable credential
    /// forwarding across redirects; the default client disables redirects entirely.
    pub client: Option<Client>,
    pub open_get_stream: bool,
    pub max_message_bytes: usize,
    pub auth: Option<Arc<dyn AuthProvider>>,
    pub reconnect: ReconnectOptions,
}
impl HttpOptions {
    pub fn new(url: Url) -> Self {
        Self {
            url,
            headers: HeaderMap::new(),
            client: None,
            open_get_stream: true,
            max_message_bytes: DEFAULT_MAX_MESSAGE_BYTES,
            auth: None,
            reconnect: ReconnectOptions::default(),
        }
    }
}
struct State {
    session: Option<String>,
    protocol: Option<String>,
    last_token: Option<String>,
}
struct Inner {
    options: HttpOptions,
    client: Client,
    state: Mutex<State>,
    started: AtomicBool,
    closed: AtomicBool,
    get_started: AtomicBool,
    stop: CancellationToken,
    events: Mutex<Option<mpsc::UnboundedSender<TransportEvent>>>,
}
#[derive(Clone)]
pub struct StreamableHttpTransport(Arc<Inner>, Arc<()>);
impl StreamableHttpTransport {
    pub fn new(options: HttpOptions) -> Result<Self> {
        if !["https", "http"].contains(&options.url.scheme())
            || !options.url.username().is_empty()
            || options.url.password().is_some()
        {
            return Err(Error::invalid(
                "MCP HTTP URL must use http(s) without userinfo",
            ));
        }
        if let Some(auth) = &options.auth {
            auth.validate_server(&options.url)?;
        }
        let client = match &options.client {
            Some(client) => client.clone(),
            None => Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
        };
        Ok(Self(
            Arc::new(Inner {
                options,
                client,
                state: Mutex::new(State {
                    session: None,
                    protocol: None,
                    last_token: None,
                }),
                started: AtomicBool::new(false),
                closed: AtomicBool::new(false),
                get_started: AtomicBool::new(false),
                stop: CancellationToken::new(),
                events: Mutex::new(None),
            }),
            Arc::new(()),
        ))
    }
    pub fn session_id(&self) -> Option<String> {
        self.0.state.lock().unwrap().session.clone()
    }
}
impl Drop for StreamableHttpTransport {
    fn drop(&mut self) {
        if Arc::strong_count(&self.1) == 1 && !self.0.closed.swap(true, Ordering::AcqRel) {
            self.0.stop.cancel();
            self.0.emit(TransportEvent::Closed);
        }
    }
}
impl Inner {
    fn emit(&self, event: TransportEvent) {
        if let Some(events) = self.events.lock().unwrap().as_ref() {
            let _ = events.send(event);
        }
    }
    fn headers(&self, token: Option<&str>, extra: &HeaderMap) -> Result<HeaderMap> {
        let mut headers = self.options.headers.clone();
        headers.extend(extra.clone());
        let state = self.state.lock().unwrap();
        for (name, value) in [
            ("mcp-session-id", state.session.as_deref()),
            ("mcp-protocol-version", state.protocol.as_deref()),
        ] {
            if let Some(value) = value {
                headers.insert(
                    name,
                    HeaderValue::from_str(value)
                        .map_err(|_| Error::invalid("invalid MCP session/protocol header"))?,
                );
            }
        }
        if let Some(token) = token {
            let mut value = HeaderValue::from_str(&format!("Bearer {token}"))
                .map_err(|_| Error::invalid("invalid bearer token header"))?;
            value.set_sensitive(true);
            headers.insert("authorization", value);
        }
        Ok(headers)
    }
    async fn authorized_fetch(
        &self,
        method: Method,
        extra: HeaderMap,
        body: Option<Vec<u8>>,
        cancellation: &CancellationToken,
    ) -> Result<Response> {
        for attempt in 0..2 {
            if self.stop.is_cancelled() {
                return Err(Error::Closed);
            }
            let token = if let Some(auth) = &self.options.auth {
                tokio::select! { _ = self.stop.cancelled() => return Err(Error::Closed), token = auth.token() => token? }
            } else {
                None
            };
            if cancellation.is_cancelled() {
                return Err(Error::Cancelled);
            }
            self.state.lock().unwrap().last_token = token.clone();
            let mut request = self
                .client
                .request(method.clone(), self.options.url.clone())
                .headers(self.headers(token.as_deref(), &extra)?);
            if let Some(body) = &body {
                request = request.body(body.clone());
            }
            let response = tokio::select! {
                _ = self.stop.cancelled() => return Err(Error::Closed),
                _ = cancellation.cancelled() => return Err(Error::Cancelled),
                response = request.send() => response?,
            };
            let status = response.status().as_u16();
            let challenge = response
                .headers()
                .get("www-authenticate")
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned);
            let needs_auth = status == 401
                || (status == 403
                    && crate::oauth::parse_www_authenticate(challenge.as_deref())
                        .error
                        .as_deref()
                        == Some("insufficient_scope"));
            if attempt == 0
                && needs_auth
                && let Some(auth) = &self.options.auth
            {
                drop(response);
                // Cancellation only prevents another dispatch; recovery can be a
                // shared rotating-token refresh needed by other callers.
                tokio::select! {
                    _ = self.stop.cancelled() => return Err(Error::Closed),
                    result = auth.on_unauthorized(UnauthorizedContext { server_url: self.options.url.clone(), status, challenge, rejected_token: token }) => result?,
                }
            } else {
                return Ok(response);
            }
        }
        unreachable!()
    }
    async fn check_response(&self, response: Response) -> Result<Response> {
        if response.status().is_success() {
            if let Some(session) = response
                .headers()
                .get("mcp-session-id")
                .and_then(|value| value.to_str().ok())
                .filter(|value| !value.is_empty())
            {
                self.state.lock().unwrap().session = Some(session.into());
            }
            return Ok(response);
        }
        let status = response.status().as_u16();
        if status == 401 {
            return Err(Error::AuthRequired {
                challenge: response
                    .headers()
                    .get("www-authenticate")
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_owned),
            });
        }
        if status == 404 && self.state.lock().unwrap().session.is_some() {
            return Err(Error::SessionExpired);
        }
        let bytes = read_bounded(response, 8192, &self.stop, &self.stop)
            .await
            .unwrap_or_default();
        let body = String::from_utf8_lossy(&bytes).chars().take(500).collect();
        Err(Error::Http { status, body })
    }
    async fn open_stream(
        &self,
        last_id: Option<&str>,
        cancellation: &CancellationToken,
    ) -> Result<Option<Response>> {
        let mut headers = HeaderMap::new();
        headers.insert("accept", HeaderValue::from_static("text/event-stream"));
        if let Some(id) = last_id {
            headers.insert(
                "last-event-id",
                HeaderValue::from_str(id).map_err(|_| Error::invalid("invalid event ID header"))?,
            );
        }
        let response = self
            .authorized_fetch(Method::GET, headers, None, cancellation)
            .await?;
        if response.status().as_u16() == 405 {
            return Ok(None);
        }
        let response = self.check_response(response).await?;
        if content_type(&response).as_deref() != Some("text/event-stream") {
            return Err(Error::invalid("GET response must be text/event-stream"));
        }
        Ok(Some(response))
    }
    fn delay(&self, attempt: usize, server: Option<u64>) -> Duration {
        server.map(Duration::from_millis).unwrap_or_else(|| {
            self.options
                .reconnect
                .initial_delay
                .saturating_mul(2_u32.saturating_pow(attempt.min(31) as u32))
                .min(self.options.reconnect.max_delay)
        })
    }
    async fn sleep(&self, delay: Duration, cancellation: &CancellationToken) -> bool {
        tokio::select! { _ = self.stop.cancelled() => false, _ = cancellation.cancelled() => false, _ = tokio::time::sleep(delay) => true }
    }
    async fn consume(
        &self,
        mut response: Response,
        cursor: &mut Cursor,
        request_id: Option<&JsonRpcId>,
        cancellation: &CancellationToken,
    ) -> Result<bool> {
        let mut parser = SseParser::new(self.options.max_message_bytes);
        loop {
            let chunk = tokio::select! {
                _ = self.stop.cancelled() => return Err(Error::Closed),
                _ = cancellation.cancelled() => return Err(Error::Cancelled),
                chunk = response.chunk() => chunk?,
            };
            let ended = chunk.is_none();
            let events = match chunk {
                Some(chunk) => parser.push(&chunk)?,
                None => parser.finish()?,
            };
            if parser.last_event_id.is_some() {
                cursor.last_id = parser.last_event_id.clone();
            }
            if parser.retry_ms.is_some() {
                cursor.retry = parser.retry_ms;
            }
            for event in events {
                cursor.received = true;
                if event.data.trim().is_empty()
                    || event.event.as_deref().is_some_and(|kind| kind != "message")
                {
                    continue;
                }
                match serde_json::from_str::<Value>(&event.data)
                    .map_err(Error::from)
                    .and_then(|message| {
                        validate_message(&message)?;
                        Ok(message)
                    }) {
                    Ok(message) => {
                        let answered = message.get("method").is_none()
                            && request_id.is_some_and(|id| {
                                serde_json::from_value::<JsonRpcId>(message["id"].clone())
                                    .ok()
                                    .as_ref()
                                    == Some(id)
                            });
                        self.emit(TransportEvent::Message(message));
                        if answered {
                            return Ok(true);
                        }
                    }
                    Err(error) => self.emit(TransportEvent::Error(error)),
                }
            }
            if ended {
                return Ok(false);
            }
        }
    }
    async fn response_stream(
        self: Arc<Self>,
        response: Response,
        id: JsonRpcId,
        cancellation: CancellationToken,
    ) {
        let mut cursor = Cursor::default();
        let mut response = Some(response);
        let mut attempts = 0;
        let failure = loop {
            let mut failure = None;
            if let Some(stream) = response.take() {
                match self
                    .consume(stream, &mut cursor, Some(&id), &cancellation)
                    .await
                {
                    Ok(true) => return,
                    Ok(false) => {}
                    Err(Error::Cancelled | Error::Closed) => return,
                    Err(error) => failure = Some(error),
                }
            }
            if failure.as_ref().is_some_and(|error| !error.retryable()) || cursor.last_id.is_none()
            {
                break failure;
            }
            if cursor.received {
                attempts = 0;
                cursor.received = false;
            }
            if attempts >= self.options.reconnect.max_retries {
                break failure;
            }
            if !self
                .sleep(self.delay(attempts, cursor.retry), &cancellation)
                .await
            {
                return;
            }
            attempts += 1;
            match self
                .open_stream(cursor.last_id.as_deref(), &cancellation)
                .await
            {
                Ok(Some(stream)) => response = Some(stream),
                Ok(None) => break Some(Error::invalid("server cannot resume response stream")),
                Err(Error::Cancelled | Error::Closed) => return,
                Err(error) if error.retryable() => {}
                Err(error) => break Some(error),
            }
        };
        self.emit(TransportEvent::Message(json!({"jsonrpc":"2.0","id":id,"error":{"code":-32603,"message":format!("MCP response stream failed: {}", failure.map_or_else(|| "stream ended without a response".into(), |error| error.to_string()))}})));
    }
    async fn get_stream(self: Arc<Self>) {
        let mut cursor = Cursor::default();
        let mut attempts = 0;
        while !self.stop.is_cancelled() {
            let opened = tokio::time::Instant::now();
            let result = match self
                .open_stream(cursor.last_id.as_deref(), &self.stop)
                .await
            {
                Ok(None) => return,
                Ok(Some(stream)) => self
                    .consume(stream, &mut cursor, None, &self.stop)
                    .await
                    .map(|_| ()),
                Err(error) => Err(error),
            };
            if self.stop.is_cancelled() {
                return;
            }
            if let Err(error) = result
                && !error.retryable()
            {
                self.emit(TransportEvent::Error(error));
                return;
            }
            if cursor.received || opened.elapsed() > self.options.reconnect.max_delay {
                attempts = 0;
            }
            cursor.received = false;
            if attempts >= self.options.reconnect.max_retries {
                self.emit(TransportEvent::Error(Error::invalid(
                    "server GET stream could not be reopened",
                )));
                return;
            }
            if !self
                .sleep(self.delay(attempts, cursor.retry), &self.stop)
                .await
            {
                return;
            }
            attempts += 1;
        }
    }
}
#[derive(Default)]
struct Cursor {
    last_id: Option<String>,
    retry: Option<u64>,
    received: bool,
}
fn content_type(response: &Response) -> Option<String> {
    response
        .headers()
        .get("content-type")?
        .to_str()
        .ok()?
        .split(';')
        .next()
        .map(|value| value.trim().to_ascii_lowercase())
}
pub(crate) async fn read_bounded(
    mut response: Response,
    max: usize,
    stop: &CancellationToken,
    cancellation: &CancellationToken,
) -> Result<Vec<u8>> {
    let mut data = Vec::new();
    loop {
        let chunk = tokio::select! { _ = stop.cancelled() => return Err(Error::Closed), _ = cancellation.cancelled() => return Err(Error::Cancelled), chunk = response.chunk() => chunk? };
        let Some(chunk) = chunk else {
            return Ok(data);
        };
        if data.len().saturating_add(chunk.len()) > max {
            return Err(Error::invalid("HTTP message exceeds byte limit"));
        }
        data.extend_from_slice(&chunk);
    }
}
impl Transport for StreamableHttpTransport {
    fn start(&self) -> BoxFuture<'_, Result<mpsc::UnboundedReceiver<TransportEvent>>> {
        Box::pin(async {
            if self.0.closed.load(Ordering::Acquire) {
                return Err(Error::Closed);
            }
            if self.0.started.swap(true, Ordering::AcqRel) {
                return Err(Error::invalid("HTTP transport already started"));
            }
            let (events, receiver) = mpsc::unbounded_channel();
            *self.0.events.lock().unwrap() = Some(events);
            Ok(receiver)
        })
    }
    fn set_protocol_version(&self, version: &str) {
        self.0.state.lock().unwrap().protocol = Some(version.into());
    }
    fn send(&self, message: Value, cancellation: CancellationToken) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            if !self.0.started.load(Ordering::Acquire) || self.0.closed.load(Ordering::Acquire) {
                return Err(Error::Closed);
            }
            validate_message(&message)?;
            let mut headers = HeaderMap::new();
            headers.insert(
                "accept",
                HeaderValue::from_static("application/json, text/event-stream"),
            );
            headers.insert("content-type", HeaderValue::from_static("application/json"));
            let response = self
                .0
                .authorized_fetch(
                    Method::POST,
                    headers,
                    Some(serde_json::to_vec(&message)?),
                    &cancellation,
                )
                .await?;
            let response = self.0.check_response(response).await?;
            let request_id = if message.get("method").is_some() {
                message
                    .get("id")
                    .map(|id| serde_json::from_value::<JsonRpcId>(id.clone()).unwrap())
            } else {
                None
            };
            let Some(id) = request_id else {
                drop(response);
                if message["method"] == "notifications/initialized"
                    && self.0.options.open_get_stream
                    && !self.0.get_started.swap(true, Ordering::AcqRel)
                {
                    tokio::spawn(self.0.clone().get_stream());
                }
                return Ok(());
            };
            if [202, 204].contains(&response.status().as_u16()) {
                return Err(Error::Http {
                    status: response.status().as_u16(),
                    body: "server accepted request without a response".into(),
                });
            }
            match content_type(&response).as_deref() {
                Some("application/json") => {
                    let bytes = read_bounded(
                        response,
                        self.0.options.max_message_bytes,
                        &self.0.stop,
                        &cancellation,
                    )
                    .await?;
                    let message = serde_json::from_slice(&bytes)?;
                    validate_message(&message)?;
                    self.0.emit(TransportEvent::Message(message));
                }
                Some("text/event-stream") => {
                    tokio::spawn(self.0.clone().response_stream(response, id, cancellation));
                }
                _ => return Err(Error::invalid("unsupported MCP response content type")),
            }
            Ok(())
        })
    }
    fn close(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async {
            if self.0.closed.swap(true, Ordering::AcqRel) {
                return Ok(());
            }
            self.0.stop.cancel();
            // Settlement and close notification precede best-effort DELETE.
            self.0.emit(TransportEvent::Closed);
            let (session, token) = {
                let state = self.0.state.lock().unwrap();
                (state.session.clone(), state.last_token.clone())
            };
            if session.is_some()
                && let Ok(headers) = self.0.headers(token.as_deref(), &HeaderMap::new())
            {
                let _ = self
                    .0
                    .client
                    .delete(self.0.options.url.clone())
                    .headers(headers)
                    .timeout(Duration::from_secs(1))
                    .send()
                    .await;
            }
            Ok(())
        })
    }
}
