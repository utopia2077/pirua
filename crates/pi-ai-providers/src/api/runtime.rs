//! Construction-time services shared by canonical adapters.
//!
//! Middleware is a trusted Rust capability with full transient request access,
//! including credentials and cancellation. It cannot cross a durable or foreign
//! protocol boundary. Each adapter invokes it at that provider's logical payload
//! stage, which may precede final HTTP serialization.

use std::collections::BTreeSet;
use std::fmt;
#[cfg(any(
    feature = "wire-pi-messages",
    feature = "wire-mistral-conversations",
    feature = "wire-anthropic-messages"
))]
use std::future::{Future, poll_fn};
use std::sync::Arc;
#[cfg(any(
    feature = "wire-pi-messages",
    feature = "wire-mistral-conversations",
    feature = "wire-anthropic-messages"
))]
use std::task::Poll;

use pi_ai_core::{BoxFuture, JsonValue, ModelsError, ProviderEnv, TextStreamRequest};
use pi_ai_transport::{HttpHeaders, HttpTransport, SseLimits, TransportError, TransportErrorKind};

#[cfg(any(
    feature = "wire-pi-messages",
    feature = "wire-mistral-conversations",
    feature = "wire-anthropic-messages"
))]
use pi_ai_core::{CancellationToken, Cancelled};

/// Optional mutation and replacement of one provider's logical request payload.
pub trait PayloadMiddleware: Send + Sync {
    /// `None` preserves the potentially mutated payload; `Some` replaces it.
    fn on_payload<'a>(
        &'a self,
        request: &'a TextStreamRequest,
        payload: &'a mut JsonValue,
    ) -> BoxFuture<'a, Result<Option<JsonValue>, ModelsError>>;
}

/// Transient response metadata. Header values are available only to trusted code.
#[derive(Clone, Debug)]
pub struct AdapterResponseHead {
    pub status: u16,
    pub headers: HttpHeaders,
}

impl AdapterResponseHead {
    /// Projects `headersToRecord(Fetch Headers)` without changing raw transport
    /// headers. Ordinary duplicates use Fetch joining, Cookie uses semicolons,
    /// and the last Set-Cookie entry wins. Names and stable own-record order
    /// follow Headers.entries then JavaScript object-key enumeration.
    /// `__proto__` creates no own string entry in the pinned plain-object helper;
    /// this projection does not emulate or expose JavaScript prototype values.
    pub fn from_http(status: u16, raw: &HttpHeaders) -> Result<Self, TransportError> {
        let names: BTreeSet<String> = raw
            .iter_for_wire()
            .map(|(name, _)| name.to_ascii_lowercase())
            .filter(|name| name != "__proto__")
            .collect();
        let mut names: Vec<String> = names.into_iter().collect();
        names.sort_by_key(|name| {
            name.parse::<u32>()
                .ok()
                .filter(|index| *index != u32::MAX && index.to_string() == name.as_str())
                .map_or((1, 0), |index| (0, index))
        });
        let mut headers = HttpHeaders::new();
        for name in names {
            let value = if name == "set-cookie" {
                raw.iter_for_wire()
                    .filter(|(key, _)| key.eq_ignore_ascii_case(&name))
                    .last()
                    .map(|(_, value)| value.trim_matches([' ', '\t']).to_owned())
            } else {
                raw.get_for_fetch(&name)
            }
            .ok_or_else(|| TransportError::new(TransportErrorKind::Protocol))?;
            headers.append(name, value)?;
        }
        Ok(Self { status, headers })
    }
}

/// Observation of received HTTP headers before provider body reduction.
pub trait ResponseMiddleware: Send + Sync {
    fn on_response<'a>(
        &'a self,
        request: &'a TextStreamRequest,
        response: &'a AdapterResponseHead,
    ) -> BoxFuture<'a, Result<(), ModelsError>>;
}

/// Reusable, non-Serde services explicitly supplied at adapter construction.
#[derive(Clone)]
pub struct AdapterServices {
    pub transport: Arc<HttpTransport>,
    pub payload: Option<Arc<dyn PayloadMiddleware>>,
    pub response: Option<Arc<dyn ResponseMiddleware>>,
    /// Explicit ambient environment snapshot; never reads the process environment.
    pub env: ProviderEnv,
    pub sse_limits: SseLimits,
    pub max_error_body_bytes: usize,
}

impl AdapterServices {
    /// Creates passive defaults; no host discovery, registration, or I/O occurs.
    pub fn new(transport: Arc<HttpTransport>) -> Self {
        Self {
            transport,
            payload: None,
            response: None,
            env: ProviderEnv::default(),
            sse_limits: SseLimits::default(),
            max_error_body_bytes: 262_144,
        }
    }
}

impl fmt::Debug for AdapterServices {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AdapterServices([REDACTED])")
    }
}

/// Drops a non-cooperative operation when the original caller cancels.
#[cfg(any(
    feature = "wire-pi-messages",
    feature = "wire-mistral-conversations",
    feature = "wire-anthropic-messages"
))]
pub(crate) async fn cancellable<T>(
    mut operation: BoxFuture<'_, T>,
    cancellation: &CancellationToken,
) -> Result<T, Cancelled> {
    let mut cancelled = Box::pin(cancellation.cancelled());
    poll_fn(|cx| {
        if cancelled.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err(Cancelled));
        }
        let result = operation.as_mut().poll(cx);
        if cancellation.is_cancelled() {
            Poll::Ready(Err(Cancelled))
        } else {
            result.map(Ok)
        }
    })
    .await
}
