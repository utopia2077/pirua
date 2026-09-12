//! Explicit cached image builtin composition, without environment/auth resolution.
use pi_ai_core::{AssistantImages, BoxFuture, ImageGenerationRequest, ImagesApiId, ModelsError};
use pi_ai_providers::{images::*, providers::BuiltinProviderModules};
use std::{fmt, sync::Arc};
pub struct CompatImages {
    registry: ImagesApiRegistry,
    cached: Arc<CachedImagesApi>,
}
impl CompatImages {
    pub fn new(modules: &BuiltinProviderModules) -> Self {
        Self {
            registry: ImagesApiRegistry::new(),
            cached: Arc::new(CachedImagesApi::new(
                Arc::new(ConfiguredImagesApiLoader::new(
                    images_generator_from_adapter(modules.images()),
                )),
                modules.auth().spawner.clone(),
            )),
        }
    }
    pub fn registry(&self) -> &ImagesApiRegistry {
        &self.registry
    }
    pub fn register_builtin_images_api_providers(&self) {
        self.registry.register(
            ImagesApiBinding::new(
                ImagesApiId::new("openrouter-images").expect("constant API"),
                self.cached.clone(),
            ),
            None,
        );
    }
    pub fn generate_images_openrouter(
        &self,
        request: ImageGenerationRequest,
    ) -> ImagesGenerateCall<'_> {
        self.cached.generate(request)
    }
    pub fn generate_images(
        &self,
        request: ImageGenerationRequest,
    ) -> BoxFuture<'_, Result<AssistantImages, ModelsError>> {
        self.registry.generate(request)
    }
}
impl fmt::Debug for CompatImages {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CompatImages([REDACTED])")
    }
}
