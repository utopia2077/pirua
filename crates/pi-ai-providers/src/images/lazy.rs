//! Two source wrappers: per-call forwarding and cached legacy load/catch ownership.
use super::{
    generator::{ImagesGenerateCall, ImagesGenerator},
    models::ImageFailure,
    runtime_shared::Completion,
};
use pi_ai_core::{BoxFuture, ImageGenerationRequest, ModelsError, ModelsErrorCode, TaskSpawner};
use std::{
    fmt,
    sync::{Arc, Mutex},
};

pub trait ImagesApiLoader: Send + Sync {
    fn load(self: Arc<Self>) -> BoxFuture<'static, Result<Arc<dyn ImagesGenerator>, ModelsError>>;
}
/// Passive module loader retaining one configured generator/adapter instance.
pub struct ConfiguredImagesApiLoader {
    api: Arc<dyn ImagesGenerator>,
}
impl ConfiguredImagesApiLoader {
    pub fn new(api: Arc<dyn ImagesGenerator>) -> Self {
        Self { api }
    }
}
impl ImagesApiLoader for ConfiguredImagesApiLoader {
    fn load(self: Arc<Self>) -> BoxFuture<'static, Result<Arc<dyn ImagesGenerator>, ModelsError>> {
        Box::pin(async move { Ok(self.api.clone()) })
    }
}
impl fmt::Debug for ConfiguredImagesApiLoader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ConfiguredImagesApiLoader([REDACTED])")
    }
}
/// Provider-facing wrapper: invokes its loader for every call and propagates rejection.
pub struct PerCallImagesApi {
    loader: Arc<dyn ImagesApiLoader>,
}
impl PerCallImagesApi {
    pub fn new(loader: Arc<dyn ImagesApiLoader>) -> Self {
        Self { loader }
    }
}
impl ImagesGenerator for PerCallImagesApi {
    fn generate<'a>(&'a self, request: ImageGenerationRequest) -> ImagesGenerateCall<'a> {
        let loader = self.loader.clone();
        Ok(Box::pin(async move {
            let api = loader.load().await?;
            api.generate(request)?.await
        }))
    }
}
impl fmt::Debug for PerCallImagesApi {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PerCallImagesApi([REDACTED])")
    }
}
type Loaded = Arc<dyn ImagesGenerator>;
/// Legacy wrapper caches one owned load, including failure, and catches each
/// load/generation rejection into an image error. Dropped callers do not cancel load.
pub struct CachedImagesApi {
    loader: Arc<dyn ImagesApiLoader>,
    spawner: Arc<dyn TaskSpawner>,
    load: Mutex<Option<Arc<Completion<Loaded>>>>,
}
impl CachedImagesApi {
    pub fn new(loader: Arc<dyn ImagesApiLoader>, spawner: Arc<dyn TaskSpawner>) -> Self {
        Self {
            loader,
            spawner,
            load: Mutex::new(None),
        }
    }
    fn loaded(&self) -> BoxFuture<'static, Result<Loaded, ModelsError>> {
        let (completion, start) = {
            let mut slot = self.load.lock().unwrap_or_else(|p| p.into_inner());
            match &*slot {
                Some(v) => (v.clone(), false),
                None => {
                    let v = Completion::new();
                    *slot = Some(v.clone());
                    (v, true)
                }
            }
        };
        if start {
            let guard = LoadGuard(completion.clone());
            let loader = self.loader.clone();
            if self
                .spawner
                .spawn(Box::pin(async move {
                    guard.0.finish(loader.load().await.map_err(Arc::new));
                }))
                .is_err()
            {
                completion.finish(Err(Arc::new(ModelsError::new(ModelsErrorCode::Stream))));
            }
        }
        completion.wait()
    }
}
struct LoadGuard(Arc<Completion<Loaded>>);
impl Drop for LoadGuard {
    fn drop(&mut self) {
        self.0
            .finish(Err(Arc::new(ModelsError::new(ModelsErrorCode::Stream))));
    }
}
impl ImagesGenerator for CachedImagesApi {
    fn generate<'a>(&'a self, request: ImageGenerationRequest) -> ImagesGenerateCall<'a> {
        Ok(Box::pin(async move {
            let failure = ImageFailure::new(
                &request.resolved.model,
                request.options.runtime.clock.clone(),
            );
            let result = async {
                let api = self.loaded().await?;
                api.generate(request)?.await
            }
            .await;
            Ok(result.unwrap_or_else(|_| failure.output()))
        }))
    }
}
impl fmt::Debug for CachedImagesApi {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CachedImagesApi([REDACTED])")
    }
}
