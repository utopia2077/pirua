use super::ProviderFactoryError;
use crate::api::openai_codex_responses::OpenAICodexResponsesApi;
use pi_ai_core::{Provider, ProviderAuth};
use std::sync::Arc;
/// Constructs a static selected provider around the retained configured adapter.
pub fn openai_codex_provider(
    adapter: Arc<OpenAICodexResponsesApi>,
    auth: &super::FactoryAuthServices,
) -> Result<Arc<dyn Provider>, ProviderFactoryError> {
    let auth = ProviderAuth::new(None, Some(super::oauth::openai_codex(auth)))
        .map_err(ProviderFactoryError::from_source)?;
    super::text::compose(
        "openai-codex",
        "OpenAI Codex",
        "openai-codex-responses",
        adapter,
        auth,
        crate::catalog::openai_codex_models().map_err(ProviderFactoryError::from_source)?,
    )
}
