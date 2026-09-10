use pi_ai_core::{
    AuthProviderError, CancellationToken, Clock, RequestRuntime, SafeUnsigned, TaskSpawner,
};
use pi_ai_transport::{CallbackLimits, CallbackListener, HttpTransport, OAuthCrypto};
use std::{fmt, sync::Arc};
/// Captured host dependencies. Token endpoints and timeout policy belong to each flow.
#[derive(Clone)]
pub struct OAuthServices {
    pub transport: Arc<HttpTransport>,
    pub clock: Arc<dyn Clock>,
    pub spawner: Arc<dyn TaskSpawner>,
    pub crypto: Arc<dyn OAuthCrypto>,
    pub callbacks: Arc<dyn CallbackListener>,
    pub callback_host: String,
    pub callback_limits: CallbackLimits,
    pub max_token_response_bytes: usize,
}
impl OAuthServices {
    pub fn runtime(&self, cancellation: CancellationToken) -> RequestRuntime {
        RequestRuntime::with_noop_telemetry(cancellation, self.clock.clone(), self.spawner.clone())
    }
}
impl fmt::Debug for OAuthServices {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("OAuthServices([REDACTED])")
    }
}
/// Inherited credential admission, using the source f64 arithmetic before checked conversion.
pub fn checked_oauth_expiry(
    now_ms: u64,
    expires_seconds: f64,
    margin_ms: u64,
) -> Result<SafeUnsigned, AuthProviderError> {
    if now_ms > 9_007_199_254_740_991 || margin_ms > 9_007_199_254_740_991 {
        return Err(AuthProviderError::new());
    }
    checked_auth_integer(now_ms as f64 + expires_seconds * 1000.0 - margin_ms as f64)
}
/// Fractional interval events remain outside inherited SafeUnsigned admission.
pub fn checked_auth_integer(value: f64) -> Result<SafeUnsigned, AuthProviderError> {
    if !value.is_finite() || value < 0.0 || value.fract() != 0.0 || value > 9_007_199_254_740_991.0
    {
        return Err(AuthProviderError::new());
    }
    SafeUnsigned::new(value as u64).map_err(AuthProviderError::with_source)
}
