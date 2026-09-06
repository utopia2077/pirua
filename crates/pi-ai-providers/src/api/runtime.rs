//! Construction-time services shared by canonical adapters.
//!
//! Middleware is a trusted Rust capability with full transient request access,
//! including credentials and cancellation. It cannot cross a durable or foreign
//! protocol boundary. Each adapter invokes it at that provider's logical payload
//! stage, which may precede final HTTP serialization.

use std::fmt;
#[cfg(any(feature = "wire-pi-messages", feature = "wire-mistral-conversations"))]
use std::future::{Future, poll_fn};
use std::sync::Arc;
#[cfg(any(feature = "wire-pi-messages", feature = "wire-mistral-conversations"))]
use std::task::Poll;

use pi_ai_core::{BoxFuture, JsonValue, ModelsError, ProviderEnv, TextStreamRequest};
use pi_ai_transport::{HttpHeaders, HttpTransport, SseLimits};

#[cfg(any(feature = "wire-pi-messages", feature = "wire-mistral-conversations"))]
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
#[cfg(any(feature = "wire-pi-messages", feature = "wire-mistral-conversations"))]
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
