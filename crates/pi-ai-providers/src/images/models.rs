//! Ordered image providers, shared auth resolution and source-specific error ownership.
use super::provider::ImagesProvider;
use pi_ai_core::*;
use std::{
    fmt,
    future::poll_fn,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::Poll,
};

/// Explicit host capabilities; absent credentials create a private memory store.
pub struct ImagesModelsOptions {
    pub credentials: Option<Arc<dyn CredentialStore>>,
    pub auth_context: Arc<dyn AuthContext>,
    pub clock: Arc<dyn Clock>,
}
impl fmt::Debug for ImagesModelsOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ImagesModelsOptions([REDACTED])")
    }
}
/// Source image generation forwards only key/env/signal into auth resolution.
#[derive(Default)]
pub struct ImagesRequestOverrides {
    pub api_key: Option<SecretString>,
    pub env: Option<ProviderEnv>,
    pub headers: ProviderHeaders,
}
impl fmt::Debug for ImagesRequestOverrides {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ImagesRequestOverrides([REDACTED])")
    }
}
struct Entry {
    id: ImagesProviderId,
    provider: Arc<dyn ImagesProvider>,
}
#[derive(Default)]
struct Membership {
    slots: Vec<Option<Entry>>,
    cursors: usize,
}
impl Membership {
    fn compact(&mut self) {
        if self.cursors == 0 {
            self.slots.retain(Option::is_some);
        }
    }
}
struct LiveProviders<'a> {
    models: &'a ImagesModels,
    next: usize,
}
impl Iterator for LiveProviders<'_> {
    type Item = Arc<dyn ImagesProvider>;
    fn next(&mut self) -> Option<Self::Item> {
        let state = self
            .models
            .providers
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        while let Some(slot) = state.slots.get(self.next) {
            self.next += 1;
            if let Some(entry) = slot {
                return Some(entry.provider.clone());
            }
        }
        None
    }
}
impl Drop for LiveProviders<'_> {
    fn drop(&mut self) {
        let mut state = self
            .models
            .providers
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        state.cursors -= 1;
        state.compact(); // Only empty tombstones are removed; no provider is dropped here.
    }
}
pub struct ImagesModels {
    providers: Mutex<Membership>,
    credentials: Arc<dyn CredentialStore>,
    auth_context: Arc<dyn AuthContext>,
    clock: Arc<dyn Clock>,
}
/// Source's mutable view of the same concrete, instance-owned collection.
pub type MutableImagesModels = ImagesModels;
pub fn create_images_models(options: ImagesModelsOptions) -> MutableImagesModels {
    ImagesModels::new(options)
}
impl ImagesModels {
    pub fn new(options: ImagesModelsOptions) -> Self {
        Self {
            providers: Mutex::new(Membership::default()),
            credentials: options
                .credentials
                .unwrap_or_else(|| Arc::new(InMemoryCredentialStore::new())),
            auth_context: options.auth_context,
            clock: options.clock,
        }
    }
    pub fn set_provider(&self, provider: Arc<dyn ImagesProvider>) {
        let entry = Entry {
            id: provider.id().clone(),
            provider,
        };
        let displaced = {
            let mut state = self.providers.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(old) = state.slots.iter_mut().flatten().find(|v| v.id == entry.id) {
                Some(std::mem::replace(old, entry))
            } else {
                state.slots.push(Some(entry));
                None
            }
        };
        drop(displaced);
    }
    pub fn delete_provider(&self, id: &ImagesProviderId) {
        let displaced = {
            let mut state = self.providers.lock().unwrap_or_else(|p| p.into_inner());
            let displaced = state
                .slots
                .iter_mut()
                .find(|v| v.as_ref().is_some_and(|v| &v.id == id))
                .and_then(Option::take);
            state.compact();
            displaced
        };
        drop(displaced);
    }
    pub fn clear_providers(&self) {
        let displaced = {
            let mut state = self.providers.lock().unwrap_or_else(|p| p.into_inner());
            let displaced: Vec<_> = state.slots.iter_mut().filter_map(Option::take).collect();
            state.compact();
            displaced
        };
        drop(displaced);
    }
    /// Snapshot, unlike the live Map iteration used by model listing/refresh admission.
    pub fn get_providers(&self) -> Vec<Arc<dyn ImagesProvider>> {
        self.providers
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .slots
            .iter()
            .flatten()
            .map(|v| v.provider.clone())
            .collect()
    }
    pub fn get_provider(&self, id: &ImagesProviderId) -> Option<Arc<dyn ImagesProvider>> {
        self.providers
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .slots
            .iter()
            .flatten()
            .find(|v| &v.id == id)
            .map(|v| v.provider.clone())
    }
    fn live_providers(&self) -> LiveProviders<'_> {
        self.providers
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .cursors += 1;
        LiveProviders {
            models: self,
            next: 0,
        }
    }
    pub fn get_models(&self, provider: Option<&ImagesProviderId>) -> Vec<ImagesModel> {
        if let Some(id) = provider {
            return self
                .get_provider(id)
                .and_then(|p| p.models().ok())
                .map_or_else(Vec::new, |v| v.to_vec());
        }
        let mut models = Vec::new();
        for provider in self.live_providers() {
            if let Ok(next) = provider.models() {
                models.extend(next.iter().cloned());
            }
        }
        models
    }
    pub fn get_model(&self, provider: &ImagesProviderId, id: &ModelId) -> Option<ImagesModel> {
        self.get_models(Some(provider))
            .into_iter()
            .find(|v| &v.id == id)
    }
    pub fn refresh<'a>(
        &'a self,
        provider: Option<&'a ImagesProviderId>,
    ) -> BoxFuture<'a, Result<(), ModelsError>> {
        Box::pin(async move {
            if let Some(id) = provider {
                let Some(provider) = self.get_provider(id) else {
                    return Ok(());
                };
                return match provider.models_refresher() {
                    Some(refresh) => refresh.refresh_models().await,
                    None => Ok(()),
                };
            }
            // Array.from(values, mapper) invokes the mapper during live iteration.
            // Pin each provider with its borrowed returned future, then stop at a
            // barrier so no returned future is polled until admission is complete.
            type PendingRefresh = Option<BoxFuture<'static, Result<(), ModelsError>>>;
            let mut admitted: Option<Vec<PendingRefresh>> = None;
            poll_fn(|cx| {
                if admitted.is_none() {
                    let gate = Arc::new(AtomicBool::new(false));
                    let mut calls = Vec::new();
                    for provider in self.live_providers() {
                        let gate = gate.clone();
                        let mut call: BoxFuture<'static, Result<(), ModelsError>> =
                            Box::pin(async move {
                                let future =
                                    provider.models_refresher().map(|v| v.refresh_models());
                                poll_fn(|_| {
                                    if gate.load(Ordering::Acquire) {
                                        Poll::Ready(())
                                    } else {
                                        Poll::Pending
                                    }
                                })
                                .await;
                                match future {
                                    Some(future) => future.await,
                                    None => Ok(()),
                                }
                            });
                        // The closed private barrier guarantees Pending; polling
                        // here invokes only source mapper/refresh launch code.
                        let initial = call.as_mut().poll(cx);
                        calls.push(if initial.is_pending() {
                            Some(call)
                        } else {
                            None
                        });
                    }
                    gate.store(true, Ordering::Release);
                    admitted = Some(calls);
                }
                let mut done = true;
                if let Some(calls) = &mut admitted {
                    for item in calls {
                        if let Some(future) = item {
                            if future.as_mut().poll(cx).is_ready() {
                                *item = None;
                            } else {
                                done = false;
                            }
                        }
                    }
                }
                if done { Poll::Ready(()) } else { Poll::Pending }
            })
            .await;
            Ok(())
        })
    }
    pub fn get_auth<'a>(
        &'a self,
        provider: &'a ImagesProviderId,
        overrides: AuthResolutionOverrides,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<AuthResult>, AuthResolutionError>> {
        Box::pin(async move {
            let Some(provider) = self.get_provider(provider) else {
                return Ok(None);
            };
            self.resolve(provider.as_ref(), overrides, cancellation)
                .await
        })
    }
    pub fn get_model_auth<'a>(
        &'a self,
        model: &'a ImagesModel,
        overrides: AuthResolutionOverrides,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<AuthResult>, AuthResolutionError>> {
        self.get_auth(&model.provider, overrides, cancellation)
    }
    async fn resolve(
        &self,
        provider: &dyn ImagesProvider,
        overrides: AuthResolutionOverrides,
        cancellation: &CancellationToken,
    ) -> Result<Option<AuthResult>, AuthResolutionError> {
        let id = ProviderId::new(provider.id().as_str()).map_err(|_| {
            AuthResolutionError::Models(ModelsError::new(ModelsErrorCode::Provider))
        })?;
        resolve_provider_auth(
            &id,
            provider.auth(),
            self.credentials.as_ref(),
            self.auth_context.as_ref(),
            self.clock.as_ref(),
            overrides,
            cancellation,
        )
        .await
    }
    /// Setup/launch errors and auth-path rejected futures become in-band image
    /// errors. With no resolved auth, only asynchronous rejection propagates.
    pub fn generate_images<'a>(
        &'a self,
        model: ImagesModel,
        context: ImagesContext,
        options: ImagesOptions,
        overrides: ImagesRequestOverrides,
    ) -> BoxFuture<'a, Result<AssistantImages, ModelsError>> {
        Box::pin(async move {
            let failure = ImageFailure::new(&model, options.runtime.clock.clone());
            let Some(provider) = self.get_provider(&model.provider) else {
                return Ok(failure.output());
            };
            let resolution = match self
                .resolve(
                    provider.as_ref(),
                    AuthResolutionOverrides {
                        api_key: overrides.api_key.clone(),
                        env: overrides.env.clone(),
                        min_oauth_validity_ms: None,
                    },
                    &options.runtime.cancellation,
                )
                .await
            {
                Ok(v) => v,
                Err(_) => return Ok(failure.output()),
            };
            let has_auth = resolution.is_some();
            let mut auth = ResolvedAuth {
                api_key: overrides.api_key,
                headers: overrides.headers,
                env: overrides.env.clone().unwrap_or_default(),
            };
            let mut endpoint = None;
            if let Some(resolution) = resolution {
                endpoint = resolution.auth.base_url.filter(|v| !v.is_empty());
                auth.api_key = auth.api_key.or(resolution.auth.api_key);
                auth.headers = match merge_image_headers(
                    resolution.auth.headers.unwrap_or_default(),
                    auth.headers,
                ) {
                    Ok(v) => v,
                    Err(_) => return Ok(failure.output()),
                };
                let mut env = resolution.env.unwrap_or_default().as_map().clone();
                if let Some(explicit) = overrides.env {
                    env.extend(explicit.as_map().clone());
                }
                auth.env = ProviderEnv::new(env);
            }
            let endpoint = match RequestEndpoint::new_resolved(
                endpoint.unwrap_or_else(|| model.base_url.as_str().to_owned()),
            ) {
                Ok(v) => v,
                Err(_) => return Ok(failure.output()),
            };
            let request = ImageGenerationRequest {
                resolved: ResolvedRequest { model, endpoint },
                context,
                auth,
                options,
            };
            let call = match provider.generate(request) {
                Ok(call) => call,
                Err(_) => return Ok(failure.output()),
            };
            if has_auth {
                Ok(call.await.unwrap_or_else(|_| failure.output()))
            } else {
                call.await
            }
        })
    }
}
impl fmt::Debug for ImagesModels {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ImagesModels([REDACTED])")
    }
}
/// JS object spread retains exact-key slots and distinct case spellings.
fn merge_image_headers(
    base: ProviderHeaders,
    later: ProviderHeaders,
) -> Result<ProviderHeaders, ModelsError> {
    let mut entries: Vec<_> = base
        .iter_for_dispatch()
        .map(|(k, v)| (k.to_owned(), v.clone()))
        .collect();
    for (k, v) in later.iter_for_dispatch() {
        if let Some((_, old)) = entries.iter_mut().find(|(name, _)| name == k) {
            *old = v.clone();
        } else {
            entries.push((k.to_owned(), v.clone()));
        }
    }
    let entries = entries
        .into_iter()
        .map(|(k, v)| {
            (
                k,
                v.map_or(serde_json::Value::Null, serde_json::Value::String),
            )
        })
        .collect();
    serde_json::from_value(serde_json::Value::Object(entries))
        .map_err(|_| ModelsError::new(ModelsErrorCode::Auth))
}
pub(super) struct ImageFailure {
    api: ImagesApiId,
    provider: ImagesProviderId,
    model: ModelId,
    clock: Arc<dyn Clock>,
}
impl ImageFailure {
    pub(super) fn new(model: &ImagesModel, clock: Arc<dyn Clock>) -> Self {
        Self {
            api: model.api.clone(),
            provider: model.provider.clone(),
            model: model.id.clone(),
            clock,
        }
    }
    pub(super) fn output(&self) -> AssistantImages {
        AssistantImages {
            api: self.api.clone(),
            provider: self.provider.clone(),
            model: self.model.clone(),
            output: Vec::new(),
            response_id: None,
            usage: None,
            stop_reason: ImagesStopReason::Error,
            error_message: Some("image generation failed".to_owned()),
            timestamp: SafeUnsigned::new(self.clock.now_unix_ms()).unwrap_or(SafeUnsigned::MAX),
        }
    }
}

#[cfg(test)]
mod cursor_tests {
    use super::*;
    use crate::images::{
        CreateImagesProviderOptions, ImagesGenerateCall, ImagesGenerator, create_images_provider,
    };
    struct Host;
    impl Clock for Host {
        fn now_unix_ms(&self) -> u64 {
            0
        }
        fn sleep<'a>(
            &'a self,
            _: std::time::Duration,
            _: &'a CancellationToken,
        ) -> BoxFuture<'a, Result<(), Cancelled>> {
            Box::pin(std::future::pending())
        }
    }
    impl AuthContext for Host {
        fn env<'a>(
            &'a self,
            _: &'a str,
            _: &'a CancellationToken,
        ) -> BoxFuture<'a, Result<Option<String>, AuthContextError>> {
            Box::pin(async { Ok(None) })
        }
        fn file_exists<'a>(
            &'a self,
            _: &'a str,
            _: &'a CancellationToken,
        ) -> BoxFuture<'a, Result<bool, AuthContextError>> {
            Box::pin(async { Ok(false) })
        }
    }
    impl ImagesGenerator for Host {
        fn generate<'a>(&'a self, _: ImageGenerationRequest) -> ImagesGenerateCall<'a> {
            Err(ModelsError::new(ModelsErrorCode::Stream))
        }
    }
    fn provider(id: &str) -> Arc<dyn ImagesProvider> {
        create_images_provider(CreateImagesProviderOptions {
            id: ImagesProviderId::new(id).unwrap(),
            name: None,
            auth: ProviderAuth::new(
                Some(Arc::new(crate::auth::env_api_key_auth("Synthetic", vec![]))),
                None,
            )
            .unwrap(),
            models: Arc::from([]),
            api: Arc::new(Host),
            refresh: None,
        })
    }
    #[test]
    fn nested_live_cursor_disposal_preserves_slots_then_compacts_only_after_last_cursor() {
        let models = ImagesModels::new(ImagesModelsOptions {
            credentials: None,
            auth_context: Arc::new(Host),
            clock: Arc::new(Host),
        });
        for id in ["a", "b", "c"] {
            models.set_provider(provider(id));
        }
        let mut outer = models.live_providers();
        let mut inner = models.live_providers();
        assert_eq!(outer.next().unwrap().id().as_str(), "a");
        models.delete_provider(&ImagesProviderId::new("a").unwrap());
        models.delete_provider(&ImagesProviderId::new("b").unwrap());
        assert_eq!(models.providers.lock().unwrap().slots.len(), 3);
        drop(outer);
        assert_eq!(models.providers.lock().unwrap().cursors, 1);
        assert_eq!(inner.next().unwrap().id().as_str(), "c");
        models.clear_providers();
        models.set_provider(provider("d"));
        assert_eq!(models.providers.lock().unwrap().slots.len(), 4);
        assert_eq!(inner.next().unwrap().id().as_str(), "d");
        drop(inner);
        assert_eq!(models.providers.lock().unwrap().slots.len(), 1);
        assert_eq!(models.providers.lock().unwrap().cursors, 0);
        let unconsumed = models.live_providers();
        models.delete_provider(&ImagesProviderId::new("d").unwrap());
        assert_eq!(models.providers.lock().unwrap().slots.len(), 1);
        drop(unconsumed);
        assert!(models.providers.lock().unwrap().slots.is_empty());
    }
}
