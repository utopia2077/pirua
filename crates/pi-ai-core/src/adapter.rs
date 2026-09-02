//! Provider-neutral adapter boundaries without transport dependencies.

use crate::error::{CapabilityError, ModelsError};
use crate::id::ApiId;
use crate::message::AssistantImages;
use crate::request::{
    DeferredCancelRequest, DeferredFetchRequest, ImageGenerationRequest, SimpleStreamRequest,
    TextStreamRequest,
};
use crate::runtime::BoxFuture;
use crate::stream::AssistantStream;

/// Object-safe text and deferred-response adapter.
///
/// Stream methods return synchronously. Authentication, adapter setup, spawn,
/// and request failures after dispatch are represented by an in-band error
/// terminal rather than escaping from an unrelated setup task.
pub trait ApiAdapter: Send + Sync {
    /// Returns the API protocol implemented by this adapter.
    fn api_id(&self) -> &ApiId;

    /// Starts a raw provider stream.
    fn stream(&self, request: TextStreamRequest) -> AssistantStream;

    /// Starts a provider-neutral simple stream.
    fn stream_simple(&self, request: SimpleStreamRequest) -> AssistantStream;

    /// Fetches a deferred response when supported.
    fn fetch_deferred(
        &self,
        request: DeferredFetchRequest,
    ) -> Result<AssistantStream, CapabilityError>;

    /// Best-effort cancellation of a deferred response.
    fn cancel_deferred<'a>(
        &'a self,
        request: DeferredCancelRequest,
    ) -> BoxFuture<'a, Result<(), ModelsError>>;
}

/// Object-safe image-generation adapter.
pub trait ImagesApiAdapter: Send + Sync {
    /// Generates images and represents provider failure in the returned result DTO.
    fn generate<'a>(&'a self, request: ImageGenerationRequest) -> BoxFuture<'a, AssistantImages>;
}
