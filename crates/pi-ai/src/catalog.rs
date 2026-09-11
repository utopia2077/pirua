//! Pure views of the enabled, dated captured catalogs.
use pi_ai_core::{ImagesModel, Model, SafeUnsigned};
pub use pi_ai_providers::catalog::CatalogError;
/// Selected catalog identity; generic core provider IDs remain open.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BuiltinProvider {
    Anthropic,
    DeepSeek,
    Google,
    OpenAI,
    OpenAICodex,
    OpenRouter,
}
impl BuiltinProvider {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Anthropic => "anthropic",
            Self::DeepSeek => "deepseek",
            Self::Google => "google",
            Self::OpenAI => "openai",
            Self::OpenAICodex => "openai-codex",
            Self::OpenRouter => "openrouter",
        }
    }
}
pub fn get_builtin_providers() -> Vec<BuiltinProvider> {
    vec![
        #[cfg(feature = "catalog-anthropic")]
        BuiltinProvider::Anthropic,
        #[cfg(feature = "catalog-deepseek")]
        BuiltinProvider::DeepSeek,
        #[cfg(feature = "catalog-google")]
        BuiltinProvider::Google,
        #[cfg(feature = "catalog-openai")]
        BuiltinProvider::OpenAI,
        #[cfg(feature = "catalog-openai-codex")]
        BuiltinProvider::OpenAICodex,
        #[cfg(feature = "catalog-openrouter")]
        BuiltinProvider::OpenRouter,
    ]
}
pub fn get_builtin_models(provider: &str) -> Result<Vec<Model>, CatalogError> {
    match provider {
        #[cfg(feature = "catalog-anthropic")]
        "anthropic" => pi_ai_providers::catalog::anthropic_models(),
        #[cfg(feature = "catalog-deepseek")]
        "deepseek" => pi_ai_providers::catalog::deepseek_models(),
        #[cfg(feature = "catalog-google")]
        "google" => pi_ai_providers::catalog::google_models(),
        #[cfg(feature = "catalog-openai")]
        "openai" => pi_ai_providers::catalog::openai_models(),
        #[cfg(feature = "catalog-openai-codex")]
        "openai-codex" => pi_ai_providers::catalog::openai_codex_models(),
        #[cfg(feature = "catalog-openrouter")]
        "openrouter" => pi_ai_providers::catalog::openrouter_models(),
        _ => Ok(Vec::new()),
    }
}
pub fn get_builtin_model(provider: &str, id: &str) -> Result<Option<Model>, CatalogError> {
    Ok(get_builtin_models(provider)?
        .into_iter()
        .find(|m| m.id.as_str() == id))
}
pub fn get_builtin_images_providers() -> Vec<&'static str> {
    vec![
        #[cfg(feature = "catalog-openrouter-images")]
        "openrouter",
    ]
}
pub fn get_builtin_images_models(provider: &str) -> Result<Vec<ImagesModel>, CatalogError> {
    match provider {
        #[cfg(feature = "catalog-openrouter-images")]
        "openrouter" => pi_ai_providers::catalog::openrouter_image_models(),
        _ => Ok(Vec::new()),
    }
}
pub fn get_builtin_images_model(
    provider: &str,
    id: &str,
) -> Result<Option<ImagesModel>, CatalogError> {
    Ok(get_builtin_images_models(provider)?
        .into_iter()
        .find(|m| m.id.as_str() == id))
}
/// Generation clock of the captured input set, not historical Git-time data.
pub fn get_builtin_model_data_generated_at() -> Option<SafeUnsigned> {
    pi_ai_providers::providers::captured_generated_at()
}
