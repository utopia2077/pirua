use super::ProviderFactoryError;
use crate::api::lazy::{LazyApiCapabilities, LazyApiLoader, lazy_api};
use pi_ai_core::{
    ApiAdapter, ApiId, BoxFuture, CreateProviderOptions, Model, ModelsError, Provider,
    ProviderAuth, ProviderId, create_provider,
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
    let adapter = lazy_api(
        ApiId::new(api).map_err(ProviderFactoryError::from_source)?,
        Arc::new(ConfiguredApi(adapter)),
        LazyApiCapabilities::default(),
    );
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
