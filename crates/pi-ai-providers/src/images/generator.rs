//! Typed distinction between a synchronous launch rejection and rejected promise.
use pi_ai_core::{
    AssistantImages, BoxFuture, ImageGenerationRequest, ImagesApiAdapter, ModelsError,
};
use std::{fmt, sync::Arc};

/// The outer result is launch failure; the future's result is asynchronous failure.
pub type ImagesGenerateCall<'a> =
    Result<BoxFuture<'a, Result<AssistantImages, ModelsError>>, ModelsError>;
pub trait ImagesGenerator: Send + Sync {
    fn generate<'a>(&'a self, request: ImageGenerationRequest) -> ImagesGenerateCall<'a>;
}
/// Adapts an existing in-band wire adapter without changing its core signature.
pub fn images_generator_from_adapter(
    adapter: Arc<dyn ImagesApiAdapter>,
) -> Arc<dyn ImagesGenerator> {
    Arc::new(AdapterGenerator(adapter))
}
struct AdapterGenerator(Arc<dyn ImagesApiAdapter>);
impl ImagesGenerator for AdapterGenerator {
    fn generate<'a>(&'a self, request: ImageGenerationRequest) -> ImagesGenerateCall<'a> {
        Ok(Box::pin(async move { Ok(self.0.generate(request).await) }))
    }
}
impl fmt::Debug for AdapterGenerator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ImagesAdapterGenerator([REDACTED])")
    }
}
