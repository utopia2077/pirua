//! Explicit instance-owned counterpart of the legacy image API registry.
use super::generator::{ImagesGenerateCall, ImagesGenerator, images_generator_from_adapter};
use pi_ai_core::{
    AssistantImages, BoxFuture, ImageGenerationRequest, ImagesApiAdapter, ImagesApiId, ModelsError,
    ModelsErrorCode,
};
use std::{
    fmt,
    sync::{Arc, Mutex},
};

pub struct ImagesApiBinding {
    api: ImagesApiId,
    generator: Arc<dyn ImagesGenerator>,
}
impl ImagesApiBinding {
    pub fn new(api: ImagesApiId, generator: Arc<dyn ImagesGenerator>) -> Self {
        Self { api, generator }
    }
    pub fn from_adapter(api: ImagesApiId, adapter: Arc<dyn ImagesApiAdapter>) -> Self {
        Self::new(api, images_generator_from_adapter(adapter))
    }
    pub fn api_id(&self) -> &ImagesApiId {
        &self.api
    }
    pub fn generate<'a>(&'a self, request: ImageGenerationRequest) -> ImagesGenerateCall<'a> {
        if request.resolved.model.api != self.api {
            return Err(ModelsError::new(ModelsErrorCode::Provider));
        }
        self.generator.generate(request)
    }
}
impl fmt::Debug for ImagesApiBinding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ImagesApiBinding([REDACTED])")
    }
}
struct Entry {
    binding: Arc<ImagesApiBinding>,
    _source_id: Option<String>,
}
#[derive(Default)]
pub struct ImagesApiRegistry {
    entries: Mutex<Vec<Entry>>,
}
impl ImagesApiRegistry {
    pub fn new() -> Self {
        Self::default()
    }
    /// Upserts even built-ins. Retained old bindings keep their own identity checks.
    pub fn register(&self, binding: ImagesApiBinding, source_id: Option<String>) {
        let entry = Entry {
            binding: Arc::new(binding),
            _source_id: source_id,
        };
        let displaced = {
            let mut entries = self.entries.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(old) = entries
                .iter_mut()
                .find(|v| v.binding.api == entry.binding.api)
            {
                Some(std::mem::replace(old, entry))
            } else {
                entries.push(entry);
                None
            }
        };
        drop(displaced);
    }
    pub fn get(&self, api: &ImagesApiId) -> Option<Arc<ImagesApiBinding>> {
        self.entries
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .find(|v| &v.binding.api == api)
            .map(|v| v.binding.clone())
    }
    /// Global-style dispatch does not resolve auth; missing registration rejects.
    pub fn generate<'a>(
        &'a self,
        request: ImageGenerationRequest,
    ) -> BoxFuture<'a, Result<AssistantImages, ModelsError>> {
        Box::pin(async move {
            let binding = self
                .get(&request.resolved.model.api)
                .ok_or_else(|| ModelsError::new(ModelsErrorCode::Provider))?;
            binding.generate(request)?.await
        })
    }
}
impl fmt::Debug for ImagesApiRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ImagesApiRegistry([REDACTED])")
    }
}
