use std::fmt;

use pi_ai_core::{BoxFuture, RequestRuntime, SafeUnsigned};
use pi_ai_transport::{
    HttpHeaders, HttpTransport, ProviderAttemptFailure, ProviderRetryError, RetryProfile,
};

/// Retry knobs for one complete Google SDK-equivalent attempt. Cancellation and
/// timing come from the original borrowed request runtime, not an invented token.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct GoogleRetryOptions {
    pub max_retries: Option<SafeUnsigned>,
    pub max_retry_delay_ms: Option<SafeUnsigned>,
}

/// Explicit SDK error-shape projection. ApiError represents an Error with a
/// status property but no headers property; the helper adds absent headers before
/// shared retry. Provider represents an already valid status/headers-bearing Error.
/// Other retains non-Error values and invalid/missing provider property shapes.
/// No Debug, Display, Clone, Error or serialization bound is imposed on E.
pub enum GoogleRequestFailure<E> {
    ApiError {
        status: Option<u16>,
        error: E,
    },
    Provider {
        status: Option<u16>,
        headers: Option<HttpHeaders>,
        error: E,
    },
    Other(E),
}
impl<E> fmt::Debug for GoogleRequestFailure<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("GoogleRequestFailure([REDACTED])")
    }
}

/// Delegates the complete operation to the sole accepted retry owner. Each
/// attempt must disable SDK/lower HTTP retry and finish error-body processing
/// before returning failure. Successful streaming bodies are outside this loop.
/// Opaque E survives final/cap/cancel failure; active noncooperative futures are
/// dropped on original caller cancellation by the transport owner.
pub fn retry_google_request<'a, T, E, F>(
    transport: &'a HttpTransport,
    runtime: &'a RequestRuntime,
    options: GoogleRetryOptions,
    mut request: F,
) -> BoxFuture<'a, Result<T, ProviderRetryError<E>>>
where
    T: Send + 'a,
    E: Send + 'a,
    F: FnMut() -> BoxFuture<'a, Result<T, GoogleRequestFailure<E>>> + Send + 'a,
{
    transport.retry_provider_request(
        runtime,
        RetryProfile::SharedProvider {
            max_retries: options.max_retries.map_or(0, SafeUnsigned::get),
            max_retry_delay_ms: options.max_retry_delay_ms.map(SafeUnsigned::get),
        },
        move || {
            let attempt = request();
            Box::pin(async move {
                attempt.await.map_err(|failure| match failure {
                    GoogleRequestFailure::ApiError { status, error } => {
                        ProviderAttemptFailure::Provider {
                            status,
                            headers: None,
                            error,
                        }
                    }
                    GoogleRequestFailure::Provider {
                        status,
                        headers,
                        error,
                    } => ProviderAttemptFailure::Provider {
                        status,
                        headers,
                        error,
                    },
                    GoogleRequestFailure::Other(error) => ProviderAttemptFailure::Other(error),
                })
            })
        },
    )
}
