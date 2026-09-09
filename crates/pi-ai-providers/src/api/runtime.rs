//! Construction-time services shared by canonical adapters.
//!
//! Middleware is a trusted Rust capability with full transient request access,
//! including credentials and cancellation. It cannot cross a durable or foreign
//! protocol boundary. Each adapter invokes it at that provider's logical payload
//! stage, which may precede final HTTP serialization.

use std::collections::BTreeSet;
use std::fmt;
#[path = "response_record.rs"]
mod response_record;
pub use response_record::{ResponseHeaderRecord, ResponseHeaderValue};
#[cfg(any(
    feature = "wire-pi-messages",
    feature = "wire-mistral-conversations",
    feature = "wire-anthropic-messages",
    feature = "wire-openai-responses",
    feature = "wire-azure-openai-responses",
    feature = "wire-openai-completions",
    feature = "wire-google-genai",
    feature = "wire-openai-codex-responses",
    feature = "wire-google-vertex",
    feature = "wire-bedrock-converse-stream"
))]
use std::future::{Future, poll_fn};
use std::sync::Arc;
#[cfg(any(
    feature = "wire-pi-messages",
    feature = "wire-mistral-conversations",
    feature = "wire-anthropic-messages",
    feature = "wire-openai-responses",
    feature = "wire-azure-openai-responses",
    feature = "wire-openai-completions",
    feature = "wire-google-genai",
    feature = "wire-openai-codex-responses",
    feature = "wire-google-vertex",
    feature = "wire-bedrock-converse-stream"
))]
use std::task::Poll;

use super::wire_json::WirePayload;
use pi_ai_core::{BoxFuture, ModelsError, ProviderEnv, TextStreamRequest};
use pi_ai_transport::{HttpHeaders, HttpTransport, SseLimits, TransportError, TransportErrorKind};

#[cfg(any(
    feature = "wire-pi-messages",
    feature = "wire-mistral-conversations",
    feature = "wire-anthropic-messages",
    feature = "wire-openai-responses",
    feature = "wire-azure-openai-responses",
    feature = "wire-openai-completions",
    feature = "wire-google-genai",
    feature = "wire-openai-codex-responses",
    feature = "wire-google-vertex",
    feature = "wire-bedrock-converse-stream"
))]
use pi_ai_core::{CancellationToken, Cancelled};

/// Optional mutation and replacement of one provider's logical request payload.
pub trait PayloadMiddleware<P = WirePayload>: Send + Sync {
    /// `None` preserves the potentially mutated payload; `Some` replaces it.
    fn on_payload<'a>(
        &'a self,
        request: &'a TextStreamRequest,
        payload: &'a mut P,
    ) -> BoxFuture<'a, Result<Option<P>, ModelsError>>;
}

/// Transient response metadata. Header values are available only to trusted code.
#[derive(Clone, Debug)]
pub struct AdapterResponseHead {
    pub status: u16,
    pub headers: ResponseHeaderRecord,
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
            .iter_for_fetch()
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
        let mut headers = ResponseHeaderRecord::new();
        for name in names {
            let value = if name == "set-cookie" {
                raw.iter_for_fetch()
                    .filter(|(key, _)| key.eq_ignore_ascii_case(&name))
                    .last()
                    .map(|(_, value)| value.to_owned())
            } else {
                raw.get_for_fetch(&name)
            }
            .ok_or_else(|| TransportError::new(TransportErrorKind::Protocol))?;
            if value.chars().any(|c| u32::from(c) > 255) {
                return Err(TransportError::new(TransportErrorKind::Protocol));
            }
            headers.insert_inherited(name, ResponseHeaderValue::Text(value))?;
        }
        // This is an already projected record, not a new Headers construction.
        // Keep the same admitted values through both explicit accessors.
        Ok(Self { status, headers })
    }

    /// Node's admitted fields followed by Smithy's plain-record projection.
    /// Physical headers stay strict; only this record contains numeric :status.
    pub fn from_smithy(
        status: u16,
        admitted: &HttpHeaders,
        protocol: pi_ai_transport::SmithyProtocol,
    ) -> Result<Self, TransportError> {
        let mut headers = ResponseHeaderRecord::new();
        if protocol == pi_ai_transport::SmithyProtocol::Http2 {
            headers.insert(
                ":status".into(),
                ResponseHeaderValue::Number(
                    pi_ai_core::FiniteF64::new(f64::from(status)).expect("u16 is finite"),
                ),
            )?;
        }
        for (name, value) in admitted.iter_for_wire() {
            // getTransformedHeaders assigns strings to a plain object.
            if name == "__proto__" {
                continue;
            }
            let value = if let Some(old) = headers.get_str(name) {
                if protocol.is_single_value_header(name) {
                    continue;
                }
                format!(
                    "{old}{}{value}",
                    match name {
                        "cookie" => "; ",
                        "set-cookie" => ",",
                        _ => ", ",
                    }
                )
            } else {
                value.to_owned()
            };
            headers.insert(name.into(), ResponseHeaderValue::Text(value))?;
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
pub struct AdapterServices<P = WirePayload> {
    pub transport: Arc<HttpTransport>,
    pub payload: Option<Arc<dyn PayloadMiddleware<P>>>,
    pub response: Option<Arc<dyn ResponseMiddleware>>,
    /// Explicit ambient environment snapshot; never reads the process environment.
    pub env: ProviderEnv,
    pub sse_limits: SseLimits,
    pub max_error_body_bytes: usize,
}

impl<P> AdapterServices<P> {
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

impl<P> Clone for AdapterServices<P> {
    fn clone(&self) -> Self {
        Self {
            transport: self.transport.clone(),
            payload: self.payload.clone(),
            response: self.response.clone(),
            env: self.env.clone(),
            sse_limits: self.sse_limits,
            max_error_body_bytes: self.max_error_body_bytes,
        }
    }
}
impl<P> fmt::Debug for AdapterServices<P> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AdapterServices([REDACTED])")
    }
}

/// Drops a non-cooperative operation when the original caller cancels.
#[cfg(any(
    feature = "wire-pi-messages",
    feature = "wire-mistral-conversations",
    feature = "wire-anthropic-messages",
    feature = "wire-openai-responses",
    feature = "wire-azure-openai-responses",
    feature = "wire-openai-completions",
    feature = "wire-google-genai",
    feature = "wire-openai-codex-responses",
    feature = "wire-google-vertex",
    feature = "wire-bedrock-converse-stream"
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
