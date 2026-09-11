//! Image providers own their static/current catalogs and optional single-flight refresh.
use super::{
    generator::{ImagesGenerateCall, ImagesGenerator},
    runtime_shared::{Completion, SharedResult},
};
use pi_ai_core::{
    BoxFuture, ImageGenerationRequest, ImagesModel, ImagesProviderId, ModelsError, ModelsErrorCode,
    ProviderAuth, TaskSpawner,
};
use std::{
    fmt,
    sync::{Arc, Mutex},
    thread::ThreadId,
};

pub trait ImagesModelsRefresher: Send + Sync {
    fn refresh_models(&self) -> BoxFuture<'_, Result<(), ModelsError>>;
}
pub trait ImagesProvider: Send + Sync {
    fn id(&self) -> &ImagesProviderId;
    fn name(&self) -> &str;
    fn auth(&self) -> &ProviderAuth;
    fn models(&self) -> Result<Arc<[ImagesModel]>, ModelsError>;
    fn models_refresher(&self) -> Option<&(dyn ImagesModelsRefresher + '_)>;
    fn generate<'a>(&'a self, request: ImageGenerationRequest) -> ImagesGenerateCall<'a>;
}
/// Launch failure models a synchronous source callback throw; a future error
/// models Promise rejection after the source callback returns.
pub type ImagesModelsFetchCall =
    Result<BoxFuture<'static, Result<Arc<[ImagesModel]>, ModelsError>>, ModelsError>;
pub trait ImagesModelsSource: Send + Sync {
    fn fetch(self: Arc<Self>) -> ImagesModelsFetchCall;
}
/// Work ownership independent of any refresh waiter. Static providers need none.
pub struct ImagesRefreshConfig {
    pub source: Arc<dyn ImagesModelsSource>,
    pub spawner: Arc<dyn TaskSpawner>,
}
pub struct CreateImagesProviderOptions {
    pub id: ImagesProviderId,
    pub name: Option<String>,
    pub auth: ProviderAuth,
    pub models: Arc<[ImagesModel]>,
    pub api: Arc<dyn ImagesGenerator>,
    pub refresh: Option<ImagesRefreshConfig>,
}
impl fmt::Debug for CreateImagesProviderOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CreateImagesProviderOptions([REDACTED])")
    }
}
struct LaunchReservation {
    thread: ThreadId,
    completion: Arc<Completion<()>>,
}
struct Catalog {
    models: Arc<[ImagesModel]>,
    inflight: Option<Arc<Completion<()>>>,
    launching: Option<LaunchReservation>,
}
struct ProviderImpl {
    id: ImagesProviderId,
    name: String,
    auth: ProviderAuth,
    api: Arc<dyn ImagesGenerator>,
    catalog: Arc<Mutex<Catalog>>,
    refresh: Option<ImagesRefreshConfig>,
}
/// Creates a passive provider; no catalog fetch or other host action occurs.
pub fn create_images_provider(options: CreateImagesProviderOptions) -> Arc<dyn ImagesProvider> {
    Arc::new(ProviderImpl {
        name: options
            .name
            .unwrap_or_else(|| options.id.as_str().to_owned()),
        id: options.id,
        auth: options.auth,
        api: options.api,
        catalog: Arc::new(Mutex::new(Catalog {
            models: options.models,
            inflight: None,
            launching: None,
        })),
        refresh: options.refresh,
    })
}
impl ImagesProvider for ProviderImpl {
    fn id(&self) -> &ImagesProviderId {
        &self.id
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn auth(&self) -> &ProviderAuth {
        &self.auth
    }
    fn models(&self) -> Result<Arc<[ImagesModel]>, ModelsError> {
        Ok(self
            .catalog
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .models
            .clone())
    }
    fn models_refresher(&self) -> Option<&(dyn ImagesModelsRefresher + '_)> {
        self.refresh
            .as_ref()
            .map(|_| self as &dyn ImagesModelsRefresher)
    }
    fn generate<'a>(&'a self, request: ImageGenerationRequest) -> ImagesGenerateCall<'a> {
        self.api.generate(request)
    }
}
// Only other Rust threads join this unpublished launch. Same-thread source
// callbacks see just inflight, matching the pinned callback-before-??= ordering.
struct LaunchGuard {
    catalog: Arc<Mutex<Catalog>>,
    owner: bool,
}
impl Drop for LaunchGuard {
    fn drop(&mut self) {
        if self.owner {
            let old = self
                .catalog
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .launching
                .take();
            drop(old);
        }
    }
}
struct RefreshGuard {
    catalog: Arc<Mutex<Catalog>>,
    completion: Arc<Completion<()>>,
    finished: bool,
}
impl RefreshGuard {
    fn cache_launch_failure(&mut self, error: ModelsError) {
        // Pinned ??= installs the already-rejected promise after the IIFE's
        // synchronous finally cleared its slot. Keep this failed flight cached.
        self.finished = true;
        self.completion.finish(Err(Arc::new(error)));
    }
    fn finish(&mut self, result: Result<Arc<[ImagesModel]>, ModelsError>) {
        let result: SharedResult<Arc<[ImagesModel]>> = result.map_err(Arc::new);
        let (displaced, flight) = {
            let mut catalog = self.catalog.lock().unwrap_or_else(|p| p.into_inner());
            let displaced = if let Ok(models) = &result {
                Some(std::mem::replace(&mut catalog.models, models.clone()))
            } else {
                None
            };
            // Source finally clears the current slot unconditionally, even when
            // nested launch/completion has installed a different flight.
            let flight = catalog.inflight.take();
            (displaced, flight)
        };
        self.finished = true;
        drop(displaced);
        drop(flight);
        self.completion.finish(result.map(|_| ()));
    }
}
impl Drop for RefreshGuard {
    fn drop(&mut self) {
        if !self.finished {
            self.finish(Err(ModelsError::new(ModelsErrorCode::ModelSource)));
        }
    }
}
impl ImagesModelsRefresher for ProviderImpl {
    fn refresh_models(&self) -> BoxFuture<'_, Result<(), ModelsError>> {
        let Some(config) = &self.refresh else {
            return Box::pin(async { Ok(()) });
        };
        let thread = std::thread::current().id();
        let (completion, start, owner) = {
            let mut catalog = self.catalog.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(flight) = &catalog.inflight {
                (flight.clone(), false, false)
            } else if let Some(launch) = &catalog.launching
                && launch.thread != thread
            {
                (launch.completion.clone(), false, false)
            } else {
                let completion = Completion::new();
                let owner = catalog.launching.is_none();
                if owner {
                    catalog.launching = Some(LaunchReservation {
                        thread,
                        completion: completion.clone(),
                    });
                }
                (completion, true, owner)
            }
        };
        if start {
            let launch = LaunchGuard {
                catalog: self.catalog.clone(),
                owner,
            };
            let mut guard = RefreshGuard {
                catalog: self.catalog.clone(),
                completion: completion.clone(),
                finished: false,
            };
            // This host call precedes visible flight assignment. Bounded callback
            // reentry can launch another source call before the outer one returns.
            let call = config.source.clone().fetch();
            let displaced = {
                let mut catalog = self.catalog.lock().unwrap_or_else(|p| p.into_inner());
                catalog.inflight.replace(completion.clone())
            };
            drop(displaced);
            drop(launch);
            match call {
                Err(error) => guard.cache_launch_failure(error),
                Ok(future) => {
                    // Publish before a possibly eager spawner polls the returned
                    // future. Each completion clears the CURRENT source-visible
                    // flight, including a nested or newer flight: no generation fence.
                    let _ = config.spawner.spawn(Box::pin(async move {
                        guard.finish(future.await);
                    }));
                }
            }
        }
        completion.wait()
    }
}

#[cfg(test)]
mod disposal_tests {
    use super::*;
    use std::{
        error::Error,
        sync::{
            Weak,
            atomic::{AtomicBool, Ordering},
        },
    };
    struct Probe {
        catalog: Weak<Mutex<Catalog>>,
        observed: Arc<AtomicBool>,
    }
    impl fmt::Debug for Probe {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("Probe")
        }
    }
    impl fmt::Display for Probe {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("probe")
        }
    }
    impl Error for Probe {}
    impl Drop for Probe {
        fn drop(&mut self) {
            let catalog = self.catalog.upgrade().unwrap();
            let state = catalog
                .try_lock()
                .expect("cached error must dispose after catalog unlock");
            assert!(state.inflight.is_none());
            self.observed.store(true, Ordering::SeqCst);
        }
    }
    #[test]
    fn clearing_a_nested_cached_failure_drops_its_last_error_owner_outside_catalog_lock() {
        let catalog = Arc::new(Mutex::new(Catalog {
            models: Arc::from([]),
            inflight: None,
            launching: None,
        }));
        let observed = Arc::new(AtomicBool::new(false));
        let cached = Completion::new();
        cached.finish(Err(Arc::new(ModelsError::with_source(
            ModelsErrorCode::ModelSource,
            Probe {
                catalog: Arc::downgrade(&catalog),
                observed: observed.clone(),
            },
        ))));
        catalog.lock().unwrap().inflight = Some(cached);
        let mut older = RefreshGuard {
            catalog,
            completion: Completion::new(),
            finished: false,
        };
        older.finish(Ok(Arc::from([])));
        assert!(observed.load(Ordering::SeqCst));
    }
}
