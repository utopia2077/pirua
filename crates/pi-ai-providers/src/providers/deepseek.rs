use super::ProviderFactoryError;
use crate::api::openai_completions::OpenAICompletionsApi;
use pi_ai_core::{Provider, ProviderAuth};
use std::sync::Arc;
/// Constructs a static selected provider around the retained configured adapter.
pub fn deepseek_provider(
    adapter: Arc<OpenAICompletionsApi>,
) -> Result<Arc<dyn Provider>, ProviderFactoryError> {
    let auth = ProviderAuth::new(
        Some(Arc::new(crate::auth::env_api_key_auth(
            "DeepSeek API key",
            vec!["DEEPSEEK_API_KEY".into()],
        ))),
        None,
    )
    .map_err(ProviderFactoryError::from_source)?;
    super::text::compose(
        "deepseek",
        "DeepSeek",
        "openai-completions",
        adapter,
        auth,
        crate::catalog::deepseek_models().map_err(ProviderFactoryError::from_source)?,
    )
}
