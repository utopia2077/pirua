use super::ProviderFactoryError;
use crate::api::openai_responses::OpenAIResponsesApi;
use pi_ai_core::{Provider, ProviderAuth};
use std::sync::Arc;
/// Constructs a static selected provider around the retained configured adapter.
pub fn openai_provider(
    adapter: Arc<OpenAIResponsesApi>,
    services: &super::FactoryAuthServices,
) -> Result<Arc<dyn Provider>, ProviderFactoryError> {
    let auth = ProviderAuth::new(
        Some(Arc::new(crate::auth::env_api_key_auth(
            "OpenAI API key",
            vec!["OPENAI_API_KEY".into()],
        ))),
        Some(super::oauth::openai_chatgpt(services)),
    )
    .map_err(ProviderFactoryError::from_source)?;
    super::text::compose(
        "openai",
        "OpenAI",
        "openai-responses",
        adapter,
        auth,
        crate::catalog::openai_models().map_err(ProviderFactoryError::from_source)?,
    )
}
