use super::ProviderFactoryError;
use crate::api::google_genai::GoogleGenAiApi;
use pi_ai_core::{Provider, ProviderAuth};
use std::sync::Arc;
/// Constructs a static selected provider around the retained configured adapter.
pub fn google_provider(
    adapter: Arc<GoogleGenAiApi>,
) -> Result<Arc<dyn Provider>, ProviderFactoryError> {
    let auth = ProviderAuth::new(
        Some(Arc::new(crate::auth::env_api_key_auth(
            "Gemini API key",
            vec!["GEMINI_API_KEY".into()],
        ))),
        None,
    )
    .map_err(ProviderFactoryError::from_source)?;
    super::text::compose(
        "google",
        "Google",
        "google-generative-ai",
        adapter,
        auth,
        crate::catalog::google_models().map_err(ProviderFactoryError::from_source)?,
    )
}
