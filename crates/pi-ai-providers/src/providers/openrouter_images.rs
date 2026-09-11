use super::{FactoryAuthServices, ProviderFactoryError};
use crate::images::{
    ConfiguredImagesApiLoader, CreateImagesProviderOptions, ImagesProvider, PerCallImagesApi,
    create_images_provider, images_generator_from_adapter, openrouter::OpenRouterImagesApi,
};
use pi_ai_core::{ImagesProviderId, ProviderAuth};
use std::sync::Arc;
/// Constructs the image namespace's OpenRouter entry, sharing its auth provider ID.
pub fn openrouter_images_provider(
    adapter: Arc<OpenRouterImagesApi>,
    services: &FactoryAuthServices,
) -> Result<Arc<dyn ImagesProvider>, ProviderFactoryError> {
    let auth = ProviderAuth::new(
        Some(Arc::new(crate::auth::env_api_key_auth(
            "OpenRouter API key",
            vec!["OPENROUTER_API_KEY".into()],
        ))),
        Some(super::oauth::openrouter(services)),
    )
    .map_err(ProviderFactoryError::from_source)?;
    let api = Arc::new(PerCallImagesApi::new(Arc::new(
        ConfiguredImagesApiLoader::new(images_generator_from_adapter(adapter)),
    )));
    Ok(create_images_provider(CreateImagesProviderOptions {
        id: ImagesProviderId::new("openrouter").map_err(ProviderFactoryError::from_source)?,
        name: Some("OpenRouter".into()),
        auth,
        models: crate::catalog::openrouter_image_models()
            .map_err(ProviderFactoryError::from_source)?
            .into(),
        api,
        refresh: None,
    }))
}
