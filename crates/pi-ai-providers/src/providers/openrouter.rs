use super::ProviderFactoryError;
use crate::api::openai_completions::OpenAICompletionsApi;
use pi_ai_core::{Provider, ProviderAuth};
use std::sync::Arc;
/// Constructs a static selected provider around the retained configured adapter.
pub fn openrouter_provider(
    adapter: Arc<OpenAICompletionsApi>,
    auth: &super::FactoryAuthServices,
) -> Result<Arc<dyn Provider>, ProviderFactoryError> {
    let auth = ProviderAuth::new(
        Some(Arc::new(crate::auth::env_api_key_auth(
            "OpenRouter API key",
            vec!["OPENROUTER_API_KEY".into()],
        ))),
        Some(super::oauth::openrouter(auth)),
    )
    .map_err(ProviderFactoryError::from_source)?;
    super::text::compose(
        "openrouter",
        "OpenRouter",
        "openai-completions",
        adapter,
        auth,
        crate::catalog::openrouter_models().map_err(ProviderFactoryError::from_source)?,
    )
}
