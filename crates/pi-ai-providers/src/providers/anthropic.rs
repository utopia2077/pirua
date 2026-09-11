use super::ProviderFactoryError;
use crate::api::anthropic_messages::AnthropicMessagesApi;
use pi_ai_core::{Provider, ProviderAuth};
use std::sync::Arc;
/// Constructs a static selected provider around the retained configured adapter.
pub fn anthropic_provider(
    adapter: Arc<AnthropicMessagesApi>,
    auth: &super::FactoryAuthServices,
) -> Result<Arc<dyn Provider>, ProviderFactoryError> {
    let auth = ProviderAuth::new(
        Some(Arc::new(crate::auth::anthropic_api_key_auth())),
        Some(super::oauth::anthropic(auth)),
    )
    .map_err(ProviderFactoryError::from_source)?;
    super::text::compose(
        "anthropic",
        "Anthropic",
        "anthropic-messages",
        adapter,
        auth,
        crate::catalog::anthropic_models().map_err(ProviderFactoryError::from_source)?,
    )
}
