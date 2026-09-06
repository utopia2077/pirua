//! Retry ownership around a complete SDK-equivalent operation.
//!
//! The caller disables lower HTTP retries and constructs provider failures only
//! after reading/classifying unsuccessful response bodies. Successful streaming
//! bodies leave this helper; their subsequent failures cannot restart an attempt.

use std::fmt;

use pi_ai_core::{BoxFuture, RequestRuntime};

use crate::policy::{cancel_wait, shared_retryable};
use crate::{HttpHeaders, HttpTransport, RetryProfile, TransportError, TransportErrorKind};

/// Family-owned failure plus the exact optional metadata used by shared retry.
/// `Provider` explicitly represents the SDK's status/headers-bearing error shape,
/// even when either property has no value. `Other` is never retried.
pub enum ProviderAttemptFailure<E> {
    Provider {
        status: Option<u16>,
        headers: Option<HttpHeaders>,
        error: E,
    },
    Other(E),
}

impl<E> fmt::Debug for ProviderAttemptFailure<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Provider { status, .. } => formatter
                .debug_struct("ProviderAttemptFailure")
                .field("status", status)
                .field("detail", &"[REDACTED]")
                .finish(),
            Self::Other(_) => formatter.write_str("ProviderAttemptFailure::Other([REDACTED])"),
        }
    }
}
impl<E> fmt::Display for ProviderAttemptFailure<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Provider { .. } => "provider attempt failed",
            Self::Other(_) => "request operation failed",
        })
    }
}

/// Failure of the complete retry operation. Policy errors retain the preceding
/// opaque family value; transport never formats, serializes or classifies it.
pub enum ProviderRetryError<E> {
    Attempt(ProviderAttemptFailure<E>),
    Cancelled {
        previous: Option<ProviderAttemptFailure<E>>,
    },
    Policy {
        error: TransportError,
        previous: Option<ProviderAttemptFailure<E>>,
    },
}

impl<E> fmt::Debug for ProviderRetryError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Attempt(error) => formatter
                .debug_tuple("ProviderRetryError::Attempt")
                .field(error)
                .finish(),
            Self::Cancelled { .. } => {
                formatter.write_str("ProviderRetryError::Cancelled([REDACTED])")
            }
            Self::Policy { error, .. } => formatter
                .debug_struct("ProviderRetryError::Policy")
                .field("kind", &error.kind())
                .field("previous", &"[REDACTED]")
                .finish(),
        }
    }
}
impl<E> fmt::Display for ProviderRetryError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Attempt(_) => "provider attempt failed",
            Self::Cancelled { .. } => "provider request cancelled",
            Self::Policy { .. } => "provider retry policy failed",
        })
    }
}
impl<E> std::error::Error for ProviderRetryError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Policy { error, .. } => Some(error),
            _ => None,
        }
    }
}

impl HttpTransport {
    /// Executes one complete attempt at a time under the pinned shared retry
    /// policy. The operation may borrow request/model state for the whole call;
    /// it need not be Clone and its opaque failure needs no Debug/Error bound.
    ///
    /// Original cancellation wins before invocation, while blocked in an
    /// attempt/backoff, and again after an attempt becomes ready. Dropping this
    /// future drops the active attempt or sleep. No provider code runs in locks.
    pub fn retry_provider_request<'a, T, E, F>(
        &'a self,
        runtime: &'a RequestRuntime,
        profile: RetryProfile,
        mut attempt: F,
    ) -> BoxFuture<'a, Result<T, ProviderRetryError<E>>>
    where
        T: Send + 'a,
        E: Send + 'a,
        F: FnMut() -> BoxFuture<'a, Result<T, ProviderAttemptFailure<E>>> + Send + 'a,
    {
        Box::pin(async move {
            let (max_retries, cap) = match profile {
                RetryProfile::None => (0, None),
                RetryProfile::SharedProvider {
                    max_retries,
                    max_retry_delay_ms,
                } => (max_retries, max_retry_delay_ms),
            };
            if runtime.cancellation.is_cancelled() {
                return Err(ProviderRetryError::Cancelled { previous: None });
            }
            if max_retries > 9_007_199_254_740_991 {
                return Err(ProviderRetryError::Policy {
                    error: TransportError::new(TransportErrorKind::InvalidRequest),
                    previous: None,
                });
            }
            let mut retry_index = 0;
            loop {
                if runtime.cancellation.is_cancelled() {
                    return Err(ProviderRetryError::Cancelled { previous: None });
                }
                // Result is nested because cancellation belongs to the owner;
                // the caller's failure type remains opaque and untouched.
                let result = cancel_wait(
                    Box::pin(async { Ok(attempt().await) }),
                    &runtime.cancellation,
                )
                .await;
                let failure = match result {
                    Err(_) => return Err(ProviderRetryError::Cancelled { previous: None }),
                    Ok(Ok(value)) => return Ok(value),
                    Ok(Err(failure)) => failure,
                };
                if retry_index >= max_retries {
                    return Err(ProviderRetryError::Attempt(failure));
                }
                let headers = match &failure {
                    ProviderAttemptFailure::Other(_) => {
                        return Err(ProviderRetryError::Attempt(failure));
                    }
                    ProviderAttemptFailure::Provider {
                        status, headers, ..
                    } => {
                        let headers = match retry_headers(headers.as_ref()) {
                            Ok(headers) => headers,
                            Err(error) => {
                                return Err(ProviderRetryError::Policy {
                                    error,
                                    previous: Some(failure),
                                });
                            }
                        };
                        if !shared_retryable(*status, &headers) {
                            return Err(ProviderRetryError::Attempt(failure));
                        }
                        headers
                    }
                };
                let delay =
                    match self.retry_delay(&headers, retry_index, cap, runtime.clock.now_unix_ms())
                    {
                        Ok(delay) => delay,
                        Err(error) => {
                            if runtime.cancellation.is_cancelled() {
                                return Err(ProviderRetryError::Cancelled {
                                    previous: Some(failure),
                                });
                            }
                            return Err(ProviderRetryError::Policy {
                                error,
                                previous: Some(failure),
                            });
                        }
                    };
                let slept = cancel_wait(
                    Box::pin(async {
                        runtime
                            .clock
                            .sleep(delay, &runtime.cancellation)
                            .await
                            .map_err(|_| TransportError::new(TransportErrorKind::Cancelled))
                    }),
                    &runtime.cancellation,
                )
                .await;
                if slept.is_err() {
                    return Err(ProviderRetryError::Cancelled {
                        previous: Some(failure),
                    });
                }
                // The raw retry budget includes MAX_SAFE_INTEGER. This private
                // zero-based counter reaches at most that budget; no durable
                // one-based maxAttempts value is constructed here.
                retry_index += 1;
            }
        })
    }
}

fn retry_headers(raw: Option<&HttpHeaders>) -> Result<HttpHeaders, TransportError> {
    let mut result = HttpHeaders::new();
    if let Some(raw) = raw {
        for name in ["x-should-retry", "retry-after-ms", "retry-after"] {
            if let Some(value) = raw.get_for_fetch(name) {
                result.set(name, value)?;
            }
        }
    }
    Ok(result)
}
