use crate::auth::OAuthServices;
#[cfg(any(feature = "oauth-anthropic", feature = "oauth-openai-codex"))]
use crate::auth::checked_oauth_expiry;
use pi_ai_core::{AuthProviderError, CancellationToken, RequestEndpoint, SecretString};
#[cfg(any(feature = "oauth-anthropic", feature = "oauth-openai-codex"))]
use pi_ai_core::{OAuthCredential, OAuthCredentialType, OAuthCredentials, OAuthExtraFields};
use pi_ai_transport::{
    FetchBody, FetchHeaderRecord, FetchHeaders, FetchRequest, RetryProfile, TimeoutProfile,
    TransportErrorKind, read_body_limited,
};
use serde_json::Value;
use std::{error::Error, fmt, future::poll_fn, sync::Arc, task::Poll, time::Duration};
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum OAuthFlowErrorKind {
    Cancelled,
    Timeout,
    InvalidResponse,
    MissingCode,
    MissingState,
    StateMismatch,
    Exchange,
    Host,
    UnknownMethod,
    DeviceDisabled,
    DeviceDenied,
    AuthorizationDenied,
    DeviceTimeout,
    DeviceSlowDownTimeout,
}
#[derive(Clone)]
pub struct OAuthFlowError {
    kind: OAuthFlowErrorKind,
    status: Option<u16>,
    source: Option<Arc<dyn Error + Send + Sync>>,
    callback_message: Option<SecretString>,
}
impl OAuthFlowError {
    pub fn kind(&self) -> OAuthFlowErrorKind {
        self.kind
    }
    pub fn status(&self) -> Option<u16> {
        self.status
    }
    pub(super) fn new(kind: OAuthFlowErrorKind) -> Self {
        Self {
            kind,
            status: None,
            source: None,
            callback_message: None,
        }
    }
    pub(super) fn http(kind: OAuthFlowErrorKind, status: u16) -> Self {
        Self {
            kind,
            status: Some(status),
            source: None,
            callback_message: None,
        }
    }
    pub(super) fn source(
        kind: OAuthFlowErrorKind,
        error: impl Error + Send + Sync + 'static,
    ) -> Self {
        Self {
            kind,
            status: None,
            source: Some(Arc::new(error)),
            callback_message: None,
        }
    }
    #[cfg(feature = "oauth-openrouter")]
    pub(super) fn with_callback_message(mut self, message: String) -> Self {
        self.callback_message = Some(SecretString::new(message));
        self
    }
    #[cfg(feature = "oauth-openrouter")]
    pub(super) fn callback_message(&self) -> String {
        self.callback_message
            .as_ref()
            .map(|m| m.expose_secret().to_owned())
            .unwrap_or_else(|| self.to_string())
    }
}
impl fmt::Debug for OAuthFlowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OAuthFlowError")
            .field("kind", &self.kind)
            .field("status", &self.status)
            .field("callback_message_present", &self.callback_message.is_some())
            .finish()
    }
}
impl fmt::Display for OAuthFlowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self.kind {
            OAuthFlowErrorKind::Cancelled => "Login cancelled",
            OAuthFlowErrorKind::Timeout => "OAuth operation timed out",
            OAuthFlowErrorKind::InvalidResponse => "OAuth response admission failed",
            OAuthFlowErrorKind::MissingCode => "Missing authorization code",
            OAuthFlowErrorKind::MissingState => "Missing OAuth state",
            OAuthFlowErrorKind::StateMismatch => "OAuth state mismatch",
            OAuthFlowErrorKind::Exchange => "OAuth token exchange failed",
            OAuthFlowErrorKind::Host => "OAuth host operation failed",
            OAuthFlowErrorKind::UnknownMethod => "Unknown OAuth login method",
            OAuthFlowErrorKind::DeviceDisabled => "Device code login is not enabled",
            OAuthFlowErrorKind::DeviceDenied => "Device authorization failed",
            OAuthFlowErrorKind::AuthorizationDenied => "OAuth authorization denied",
            OAuthFlowErrorKind::DeviceTimeout => "Device flow timed out",
            OAuthFlowErrorKind::DeviceSlowDownTimeout => "Device flow timed out after slow_down",
        })
    }
}
impl Error for OAuthFlowError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source.as_deref().map(|s| s as _)
    }
}
pub(super) type Result<T> = std::result::Result<T, OAuthFlowError>;
pub(super) fn provider(error: OAuthFlowError) -> AuthProviderError {
    AuthProviderError::with_source(error)
}
pub(super) fn host(error: impl Error + Send + Sync + 'static) -> OAuthFlowError {
    let kind = if (&error as &dyn Error)
        .downcast_ref::<pi_ai_transport::AuthHostError>()
        .is_some_and(|error| error.kind() == pi_ai_transport::AuthHostErrorKind::Cancelled)
    {
        OAuthFlowErrorKind::Cancelled
    } else {
        OAuthFlowErrorKind::Host
    };
    OAuthFlowError::source(kind, error)
}
pub(super) fn invalid() -> OAuthFlowError {
    OAuthFlowError::new(OAuthFlowErrorKind::InvalidResponse)
}
pub(super) fn cancelled() -> OAuthFlowError {
    OAuthFlowError::new(OAuthFlowErrorKind::Cancelled)
}
pub(super) struct CancelOnDrop(pub CancellationToken);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}
/// A source Promise checkpoint; prevents synchronous fake HTTP from completing
/// an exchange before browser listener/manual cleanup has run.
pub(super) async fn checkpoint() {
    let mut yielded = false;
    poll_fn(|cx| {
        if yielded {
            Poll::Ready(())
        } else {
            yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    })
    .await
}
pub(super) struct TokenResponse {
    pub status: u16,
    pub body: Vec<u8>,
}
impl TokenResponse {
    pub fn ok(&self) -> bool {
        (200..300).contains(&self.status)
    }
    pub fn json(&self) -> Result<Value> {
        serde_json::from_slice(&self.body).map_err(|_| invalid())
    }
}
#[derive(Clone, Copy)]
pub(super) enum TokenRead {
    #[cfg(feature = "oauth-anthropic")]
    Anthropic,
    #[cfg(feature = "oauth-openai-codex")]
    Codex,
    #[cfg(feature = "oauth-openai-codex")]
    CodexPoll,
    #[cfg(feature = "oauth-openai-codex")]
    CodexDeviceStart,
    #[cfg(feature = "oauth-openrouter")]
    OpenRouter,
}
impl TokenRead {
    fn skip(self, status: u16) -> bool {
        match self {
            #[cfg(feature = "oauth-openai-codex")]
            Self::CodexPoll => matches!(status, 403 | 404),
            #[cfg(feature = "oauth-openai-codex")]
            Self::CodexDeviceStart => status == 404,
            _ => {
                let _ = status;
                false
            }
        }
    }
    fn error_body_best_effort(self) -> bool {
        match self {
            #[cfg(feature = "oauth-anthropic")]
            Self::Anthropic => false,
            #[cfg(feature = "oauth-openai-codex")]
            Self::Codex | Self::CodexPoll | Self::CodexDeviceStart => true,
            #[cfg(feature = "oauth-openrouter")]
            Self::OpenRouter => true,
        }
    }
}
pub(super) async fn post(
    s: &OAuthServices,
    url: &str,
    headers: &[(&str, &str)],
    body: String,
    c: &CancellationToken,
    timeout: Option<Duration>,
    read: TokenRead,
) -> Result<TokenResponse> {
    if s.max_token_response_bytes == 0 {
        return Err(invalid());
    }
    let mut record = FetchHeaderRecord::new();
    for (key, value) in headers {
        record.insert((*key).into(), (*value).into());
    }
    let request = FetchRequest {
        endpoint: RequestEndpoint::new_resolved(url).map_err(host)?,
        method: "POST".into(),
        headers: FetchHeaders::Record(record),
        body: FetchBody::Text(body),
        proxy: None,
    };
    let runtime = s.runtime(c.clone());
    let map = |error: pi_ai_transport::TransportError| {
        let kind = match error.kind() {
            TransportErrorKind::Cancelled => OAuthFlowErrorKind::Cancelled,
            TransportErrorKind::Timeout => OAuthFlowErrorKind::Timeout,
            _ => OAuthFlowErrorKind::Exchange,
        };
        OAuthFlowError::source(kind, error)
    };
    let mut response = s
        .transport
        .send_fetch_request(
            &request,
            &runtime,
            RetryProfile::None,
            timeout.map_or(TimeoutProfile::None, TimeoutProfile::WholeResponse),
        )
        .await
        .map_err(map)?;
    checkpoint().await;
    let body = if read.skip(response.status) {
        response.body.close();
        Vec::new()
    } else {
        match read_body_limited(response.body.as_mut(), &runtime, s.max_token_response_bytes).await
        {
            Ok(body) => body,
            Err(_) if !(200..300).contains(&response.status) && read.error_body_best_effort() => {
                Vec::new()
            }
            Err(error) => {
                #[cfg(feature = "oauth-openrouter")]
                if matches!(read, TokenRead::OpenRouter) && (200..300).contains(&response.status) {
                    // response.json() rejects at this provider's admission stage.
                    // Its outer catch still gives caller cancellation and timeout priority.
                    if c.is_cancelled() || error.kind() == TransportErrorKind::Cancelled {
                        return Err(OAuthFlowError::source(OAuthFlowErrorKind::Cancelled, error));
                    }
                    if error.kind() != TransportErrorKind::Timeout {
                        return Err(OAuthFlowError::source(
                            OAuthFlowErrorKind::InvalidResponse,
                            error,
                        )
                        .with_callback_message("OpenRouter OAuth returned invalid JSON".into()));
                    }
                }
                return Err(map(error));
            }
        }
    };
    Ok(TokenResponse {
        status: response.status,
        body,
    })
}
#[cfg(any(feature = "oauth-anthropic", feature = "oauth-openai-codex"))]
pub(super) fn token(value: &Value, now: u64, margin: u64) -> Result<OAuthCredential> {
    let access = value
        .get("access_token")
        .and_then(Value::as_str)
        .ok_or_else(invalid)?;
    let refresh = value
        .get("refresh_token")
        .and_then(Value::as_str)
        .ok_or_else(invalid)?;
    let expires = value
        .get("expires_in")
        .and_then(Value::as_f64)
        .ok_or_else(invalid)?;
    Ok(OAuthCredential {
        credential_type: OAuthCredentialType::OAuth,
        credentials: OAuthCredentials {
            access: SecretString::new(access),
            refresh: SecretString::new(refresh),
            expires: checked_oauth_expiry(now, expires, margin).map_err(|_| invalid())?,
            extra: OAuthExtraFields::default(),
        },
    })
}
pub(super) fn ecma_space(c: char) -> bool {
    matches!(c,'\u{0009}'..='\u{000d}'|'\u{0020}'|'\u{00a0}'|'\u{1680}'|'\u{2000}'..='\u{200a}'|'\u{2028}'|'\u{2029}'|'\u{202f}'|'\u{205f}'|'\u{3000}'|'\u{feff}')
}
pub(super) fn query(url: &url::Url, name: &str) -> Option<String> {
    url.query_pairs()
        .find(|(key, _)| key == name)
        .map(|(_, v)| v.into_owned())
}
pub(super) fn parse_input(input: &str, hash_state: bool) -> (Option<String>, Option<String>) {
    let value = input.trim_matches(ecma_space);
    if value.is_empty() {
        return (None, None);
    }
    if let Ok(url) = url::Url::parse(value) {
        return (query(&url, "code"), query(&url, "state"));
    }
    if hash_state && value.contains('#') {
        let mut parts = value.split('#');
        return (
            parts.next().map(str::to_owned),
            parts.next().map(str::to_owned),
        );
    }
    if value.contains("code=") {
        let pairs: Vec<_> = url::form_urlencoded::parse(value.as_bytes()).collect();
        return (
            pairs
                .iter()
                .find(|(k, _)| k == "code")
                .map(|(_, v)| v.to_string()),
            pairs
                .iter()
                .find(|(k, _)| k == "state")
                .map(|(_, v)| v.to_string()),
        );
    }
    (Some(value.into()), None)
}
pub(super) fn callback_url(target: &str) -> Result<url::Url> {
    url::Url::parse("http://localhost")
        .map_err(host)?
        .join(target)
        .map_err(host)
}
pub(super) fn form(pairs: &[(&str, &str)]) -> String {
    url::form_urlencoded::Serializer::new(String::new())
        .extend_pairs(pairs.iter().copied())
        .finish()
}

/// Pending writes for callbacks that do not settle authentication. A queued
/// response cannot pause the manual input, token exchange or login deadline.
pub(super) struct Responses<'a> {
    pending:
        Vec<pi_ai_core::BoxFuture<'a, std::result::Result<(), pi_ai_transport::AuthHostError>>>,
    limit: usize,
}
impl<'a> Responses<'a> {
    pub fn new(limit: usize) -> Self {
        Self {
            pending: Vec::new(),
            limit,
        }
    }
    pub async fn start(
        &mut self,
        reply: Box<dyn pi_ai_transport::CallbackReply>,
        response: pi_ai_transport::CallbackResponse,
        c: &'a CancellationToken,
    ) -> Result<()> {
        let mut pending = reply.send(response, c);
        if poll_fn(|cx| Poll::Ready(pending.as_mut().poll(cx)))
            .await
            .is_pending()
        {
            if self.pending.len() == self.limit {
                return Err(host(pi_ai_transport::AuthHostError::new(
                    pi_ai_transport::AuthHostErrorKind::LimitExceeded,
                )));
            }
            self.pending.push(pending);
        }
        Ok(())
    }
    pub fn poll(&mut self, cx: &mut std::task::Context<'_>) {
        let mut index = 0;
        while index < self.pending.len() {
            if self.pending[index].as_mut().poll(cx).is_ready() {
                drop(self.pending.remove(index));
            } else {
                index += 1;
            }
        }
    }
}

/// Browser flow cancellation owns this race even for a host that never wakes
/// or observes the supplied cancellation token while acknowledging a reply.
#[cfg(any(feature = "oauth-anthropic", feature = "oauth-openai-codex"))]
pub(super) async fn acknowledge(
    reply: Box<dyn pi_ai_transport::CallbackReply>,
    response: pi_ai_transport::CallbackResponse,
    c: &CancellationToken,
) -> Result<()> {
    use std::future::Future;
    let mut pending = reply.send(response, c);
    let mut cancelled_future = Box::pin(c.cancelled());
    poll_fn(|cx| {
        if cancelled_future.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err(cancelled()));
        }
        pending.as_mut().poll(cx).map(|result| result.map_err(host))
    })
    .await
}
