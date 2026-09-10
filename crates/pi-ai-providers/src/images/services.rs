//! Trusted construction-time capabilities for image requests.
use crate::api::{runtime::AdapterResponseHead, wire_json::WirePayload};
use pi_ai_core::{BoxFuture, ImageGenerationRequest, ModelsError, ProviderEnv};
use pi_ai_transport::HttpTransport;
use std::{fmt, sync::Arc};

/// Mutates an image payload or replaces it with any supported finite JSON value.
pub trait ImagePayloadMiddleware: Send + Sync {
    fn on_payload<'a>(
        &'a self,
        request: &'a ImageGenerationRequest,
        payload: &'a mut WirePayload,
    ) -> BoxFuture<'a, Result<Option<WirePayload>, ModelsError>>;
}

/// Observes successful SDK response decoding before image reduction.
pub trait ImageResponseMiddleware: Send + Sync {
    fn on_response<'a>(
        &'a self,
        request: &'a ImageGenerationRequest,
        response: &'a AdapterResponseHead,
    ) -> BoxFuture<'a, Result<(), ModelsError>>;
}

/// Non-Serde, passive services. Environment is explicitly supplied, never discovered.
/// Middleware is awaited at its source stage; it owns its own cancellation policy.
#[derive(Clone)]
pub struct ImageAdapterServices {
    pub transport: Arc<HttpTransport>,
    pub payload: Option<Arc<dyn ImagePayloadMiddleware>>,
    pub response: Option<Arc<dyn ImageResponseMiddleware>>,
    pub env: ProviderEnv,
    /// Bound on one buffered successful response, including embedded image data.
    pub max_response_body_bytes: usize,
    /// Bound on one unsuccessful HTTP response body, read before retry decisions.
    pub max_error_body_bytes: usize,
}
impl ImageAdapterServices {
    pub fn new(transport: Arc<HttpTransport>) -> Self {
        Self {
            transport,
            payload: None,
            response: None,
            env: ProviderEnv::default(),
            max_response_body_bytes: 16 * 1024 * 1024,
            max_error_body_bytes: 262_144,
        }
    }
}
impl fmt::Debug for ImageAdapterServices {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ImageAdapterServices([REDACTED])")
    }
}
