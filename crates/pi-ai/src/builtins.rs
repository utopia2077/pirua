//! Explicit builtin registration. Construction never mutates a global registry.
use pi_ai_core::{ModelsError, ModelsRegistry, ModelsRegistryOptions, Provider};
pub use pi_ai_providers::providers::{
    BuiltinProviderModules, BuiltinProviderServices, FactoryAuthServices, ProviderFactoryError,
};
use std::{error::Error, fmt, sync::Arc};
/// Factory or awaited registry-admission failure.
#[derive(Debug)]
#[non_exhaustive]
pub enum BuiltinCompositionError {
    Factory(ProviderFactoryError),
    Registration(ModelsError),
}
impl fmt::Display for BuiltinCompositionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("builtin composition failed")
    }
}
impl Error for BuiltinCompositionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(match self {
            Self::Factory(e) => e,
            Self::Registration(e) => e,
        })
    }
}
pub fn builtin_providers(
    modules: &BuiltinProviderModules,
) -> Result<Vec<Arc<dyn Provider>>, ProviderFactoryError> {
    let _ = modules;
    Ok(vec![
        #[cfg(feature = "provider-anthropic")]
        pi_ai_providers::providers::anthropic_provider(modules.anthropic(), modules.auth())?,
        #[cfg(feature = "provider-deepseek")]
        pi_ai_providers::providers::deepseek_provider(modules.completions())?,
        #[cfg(feature = "provider-google")]
        pi_ai_providers::providers::google_provider(modules.google())?,
        #[cfg(feature = "provider-openai")]
        pi_ai_providers::providers::openai_provider(modules.openai(), modules.auth())?,
    ])
}
pub async fn builtin_models(
    modules: &BuiltinProviderModules,
    options: ModelsRegistryOptions,
) -> Result<ModelsRegistry, BuiltinCompositionError> {
    let providers = builtin_providers(modules).map_err(BuiltinCompositionError::Factory)?;
    let models = ModelsRegistry::new_with_child_cancellation(options);
    for provider in providers {
        models
            .set_provider(provider)
            .await
            .map_err(BuiltinCompositionError::Registration)?;
    }
    Ok(models)
}
