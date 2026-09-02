//! Non-serializable resolved request capabilities.

use std::fmt;

use crate::auth::SecretString;
use crate::context::{Context, ImagesContext};
use crate::error::RequestEndpointError;
use crate::message::DeferredHandle;
use crate::model::{ImagesModel, Model};
use crate::options::{ProviderEnv, ProviderHeaders};
use crate::runtime::{
    DeferredCancelOptions, DeferredFetchOptions, ImagesOptions, SimpleStreamOptions, StreamOptions,
};

/// An already-resolved request endpoint exposed only to transport adapters.
///
/// The endpoint is wholly redacted in Debug output and intentionally does not
/// implement Serde, Display, AsRef, or Deref.
///
/// ```compile_fail
/// fn requires_serialize<T: serde::Serialize>() {}
/// requires_serialize::<pi_ai_core::RequestEndpoint>();
/// ```
///
/// ```compile_fail
/// fn requires_deserialize<T: serde::de::DeserializeOwned>() {}
/// requires_deserialize::<pi_ai_core::RequestEndpoint>();
/// ```
pub struct RequestEndpoint(String);

impl RequestEndpoint {
    /// Creates a transient endpoint from an explicitly already-resolved URL.
    pub fn new_resolved(value: impl Into<String>) -> Result<Self, RequestEndpointError> {
        let value = value.into();
        if value.is_empty() {
            Err(RequestEndpointError::Empty)
        } else {
            Ok(Self(value))
        }
    }

    /// Explicitly exposes the resolved URL to a transport or provider adapter.
    pub fn expose_for_transport(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for RequestEndpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RequestEndpoint([REDACTED])")
    }
}

/// A model paired with its transient resolved request endpoint.
///
/// ```compile_fail
/// fn requires_serialize<T: serde::Serialize>() {}
/// requires_serialize::<pi_ai_core::ResolvedRequest<pi_ai_core::Model>>();
/// ```
///
/// ```compile_fail
/// fn requires_deserialize<T: serde::de::DeserializeOwned>() {}
/// requires_deserialize::<pi_ai_core::ResolvedRequest<pi_ai_core::Model>>();
/// ```
pub struct ResolvedRequest<M> {
    /// Validated source model, unchanged by endpoint resolution.
    pub model: M,
    /// Transient effective endpoint.
    pub endpoint: RequestEndpoint,
}

impl<M> fmt::Debug for ResolvedRequest<M> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ResolvedRequest([REDACTED])")
    }
}

/// Transient authentication resolved for one outbound request.
///
/// Debug output reveals only API-key presence plus header/environment keys.
/// The type intentionally does not implement Serde.
///
/// ```compile_fail
/// fn requires_serialize<T: serde::Serialize>() {}
/// requires_serialize::<pi_ai_core::ResolvedAuth>();
/// ```
///
/// ```compile_fail
/// fn requires_deserialize<T: serde::de::DeserializeOwned>() {}
/// requires_deserialize::<pi_ai_core::ResolvedAuth>();
/// ```
#[derive(Clone, Default, Eq, PartialEq)]
pub struct ResolvedAuth {
    /// Optional provider API key or bearer token.
    pub api_key: Option<SecretString>,
    /// Resolved transient request headers.
    pub headers: ProviderHeaders,
    /// Resolved provider-scoped environment.
    pub env: ProviderEnv,
}

impl fmt::Debug for ResolvedAuth {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let header_keys = self.headers.as_map().keys().collect::<Vec<_>>();
        let env_keys = self.env.as_map().keys().collect::<Vec<_>>();
        formatter
            .debug_struct("ResolvedAuth")
            .field("api_key_present", &self.api_key.is_some())
            .field("header_keys", &header_keys)
            .field("env_keys", &env_keys)
            .finish()
    }
}

/// Nominal resolved raw text-stream request root.
///
/// ```compile_fail
/// fn requires_serialize<T: serde::Serialize>() {}
/// requires_serialize::<pi_ai_core::TextStreamRequest>();
/// ```
///
/// ```compile_fail
/// fn requires_deserialize<T: serde::de::DeserializeOwned>() {}
/// requires_deserialize::<pi_ai_core::TextStreamRequest>();
/// ```
pub struct TextStreamRequest {
    /// Validated model and transient endpoint.
    pub resolved: ResolvedRequest<Model>,
    /// Provider-neutral request context.
    pub context: Context,
    /// Transient resolved authentication.
    pub auth: ResolvedAuth,
    /// Raw-stream wire values and process-local runtime.
    pub options: StreamOptions,
}

/// Nominal resolved simple text-stream request root.
///
/// ```compile_fail
/// fn requires_serialize<T: serde::Serialize>() {}
/// requires_serialize::<pi_ai_core::SimpleStreamRequest>();
/// ```
///
/// ```compile_fail
/// fn requires_deserialize<T: serde::de::DeserializeOwned>() {}
/// requires_deserialize::<pi_ai_core::SimpleStreamRequest>();
/// ```
pub struct SimpleStreamRequest {
    /// Validated model and transient endpoint.
    pub resolved: ResolvedRequest<Model>,
    /// Provider-neutral request context.
    pub context: Context,
    /// Transient resolved authentication.
    pub auth: ResolvedAuth,
    /// Simple-stream wire values and process-local runtime.
    pub options: SimpleStreamOptions,
}

/// Nominal resolved deferred-response fetch request root.
///
/// ```compile_fail
/// fn requires_serialize<T: serde::Serialize>() {}
/// requires_serialize::<pi_ai_core::DeferredFetchRequest>();
/// ```
///
/// ```compile_fail
/// fn requires_deserialize<T: serde::de::DeserializeOwned>() {}
/// requires_deserialize::<pi_ai_core::DeferredFetchRequest>();
/// ```
pub struct DeferredFetchRequest {
    /// Validated model and transient endpoint.
    pub resolved: ResolvedRequest<Model>,
    /// Provider-native deferred handle.
    pub handle: DeferredHandle,
    /// Transient resolved authentication.
    pub auth: ResolvedAuth,
    /// Deferred-fetch wire values and process-local runtime.
    pub options: DeferredFetchOptions,
}

/// Nominal resolved deferred-response cancellation request root.
///
/// ```compile_fail
/// fn requires_serialize<T: serde::Serialize>() {}
/// requires_serialize::<pi_ai_core::DeferredCancelRequest>();
/// ```
///
/// ```compile_fail
/// fn requires_deserialize<T: serde::de::DeserializeOwned>() {}
/// requires_deserialize::<pi_ai_core::DeferredCancelRequest>();
/// ```
pub struct DeferredCancelRequest {
    /// Validated model and transient endpoint.
    pub resolved: ResolvedRequest<Model>,
    /// Provider-native deferred handle.
    pub handle: DeferredHandle,
    /// Transient resolved authentication.
    pub auth: ResolvedAuth,
    /// Deferred-cancel wire values and process-local runtime.
    pub options: DeferredCancelOptions,
}

/// Nominal resolved image-generation request root.
///
/// ```compile_fail
/// fn requires_serialize<T: serde::Serialize>() {}
/// requires_serialize::<pi_ai_core::ImageGenerationRequest>();
/// ```
///
/// ```compile_fail
/// fn requires_deserialize<T: serde::de::DeserializeOwned>() {}
/// requires_deserialize::<pi_ai_core::ImageGenerationRequest>();
/// ```
pub struct ImageGenerationRequest {
    /// Validated image model and transient endpoint.
    pub resolved: ResolvedRequest<ImagesModel>,
    /// Provider-neutral image request context.
    pub context: ImagesContext,
    /// Transient resolved authentication.
    pub auth: ResolvedAuth,
    /// Image wire values and process-local runtime.
    pub options: ImagesOptions,
}

macro_rules! redacted_request_debug {
    ($($name:ident),+ $(,)?) => {
        $(
            impl fmt::Debug for $name {
                fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                    formatter.write_str(concat!(stringify!($name), "([REDACTED])"))
                }
            }
        )+
    };
}

redacted_request_debug!(
    TextStreamRequest,
    SimpleStreamRequest,
    DeferredFetchRequest,
    DeferredCancelRequest,
    ImageGenerationRequest,
);
