use super::ProviderFactoryError;
use crate::api::lazy::{LazyApi, LazyApiCapabilities, LazyApiLoader, lazy_api, lazy_stream};
use pi_ai_core::{
    ApiAdapter, ApiId, AssistantStream, AssistantStreamInit, BoxFuture, CapabilityError,
    CreateProviderOptions, DeferredCancelRequest, DeferredFetchRequest, Model, ModelsError,
    ModelsErrorCode, Provider, ProviderAuth, ProviderId, RequestLifecycleAdapter,
    RequestLifecycleInvocation, RequestRuntime, SimpleStreamRequest, TextStreamRequest,
    create_provider,
};
use std::sync::Arc;
struct ConfiguredApi(Arc<dyn ApiAdapter>);
impl LazyApiLoader for ConfiguredApi {
    fn load(self: Arc<Self>) -> BoxFuture<'static, Result<Arc<dyn ApiAdapter>, ModelsError>> {
        Box::pin(async move { Ok(self.0.clone()) })
    }
}
pub(super) fn compose(
    id: &str,
    name: &str,
    api: &str,
    adapter: Arc<dyn ApiAdapter>,
    auth: ProviderAuth,
    models: Vec<Model>,
) -> Result<Arc<dyn Provider>, ProviderFactoryError> {
    let configured = Arc::new(ConfiguredApi(adapter));
    let ordinary = lazy_api(
        ApiId::new(api).map_err(ProviderFactoryError::from_source)?,
        configured.clone(),
        LazyApiCapabilities::default(),
    );
    let adapter = ConfiguredAdapter {
        ordinary,
        configured,
    };
    create_provider(CreateProviderOptions {
        id: ProviderId::new(id).map_err(ProviderFactoryError::from_source)?,
        name: Some(name.into()),
        auth,
        models,
        dynamic_models: None,
        model_filter: None,
        adapters: vec![Arc::new(adapter)],
    })
    .map_err(ProviderFactoryError::from_source)
}

// The public lazy factories retain their existing advertised defaults. Only this
// explicitly configured selected text composition forwards the optional port.
struct ConfiguredAdapter {
    ordinary: LazyApi,
    configured: Arc<ConfiguredApi>,
}
impl ApiAdapter for ConfiguredAdapter {
    fn api_id(&self) -> &ApiId {
        self.ordinary.api_id()
    }
    fn stream(&self, request: TextStreamRequest) -> AssistantStream {
        self.ordinary.stream(request)
    }
    fn stream_simple(&self, request: SimpleStreamRequest) -> AssistantStream {
        self.ordinary.stream_simple(request)
    }
    fn fetch_deferred(
        &self,
        request: DeferredFetchRequest,
    ) -> Result<AssistantStream, CapabilityError> {
        self.ordinary.fetch_deferred(request)
    }
    fn cancel_deferred<'a>(
        &'a self,
        request: DeferredCancelRequest,
    ) -> BoxFuture<'a, Result<(), ModelsError>> {
        self.ordinary.cancel_deferred(request)
    }
    fn lifecycle_adapter(&self) -> Option<&dyn RequestLifecycleAdapter> {
        self.configured
            .0
            .lifecycle_adapter()
            .map(|_| self as &dyn RequestLifecycleAdapter)
    }
}
fn lifecycle_init(model: &Model, runtime: &RequestRuntime) -> AssistantStreamInit {
    AssistantStreamInit {
        api: model.api.clone(),
        provider: model.provider.clone(),
        model: model.id.clone(),
        clock: runtime.clock.clone(),
        cancellation: runtime.cancellation.clone(),
    }
}
impl RequestLifecycleAdapter for ConfiguredAdapter {
    fn stream_with_lifecycle(
        &self,
        request: TextStreamRequest,
        lifecycle: RequestLifecycleInvocation,
    ) -> AssistantStream {
        let init = lifecycle_init(&request.resolved.model, &request.options.runtime);
        let spawner = request.options.runtime.spawner.clone();
        let configured = self.configured.clone();
        lazy_stream(init, spawner.as_ref(), || {
            Box::pin(async move {
                let concrete = configured.load().await?;
                let capability = concrete
                    .lifecycle_adapter()
                    .ok_or_else(|| ModelsError::new(ModelsErrorCode::Provider))?;
                Ok(capability.stream_with_lifecycle(request, lifecycle))
            })
        })
    }
    fn stream_simple_with_lifecycle(
        &self,
        request: SimpleStreamRequest,
        lifecycle: RequestLifecycleInvocation,
    ) -> AssistantStream {
        let init = lifecycle_init(&request.resolved.model, &request.options.runtime);
        let spawner = request.options.runtime.spawner.clone();
        let configured = self.configured.clone();
        lazy_stream(init, spawner.as_ref(), || {
            Box::pin(async move {
                let concrete = configured.load().await?;
                let capability = concrete
                    .lifecycle_adapter()
                    .ok_or_else(|| ModelsError::new(ModelsErrorCode::Provider))?;
                Ok(capability.stream_simple_with_lifecycle(request, lifecycle))
            })
        })
    }
}
