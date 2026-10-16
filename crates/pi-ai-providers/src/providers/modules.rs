use super::ProviderFactoryError;
use crate::auth::OAuthServices;
use pi_ai_core::TaskSpawner;
use std::{fmt, sync::Arc};
/// Host capabilities captured without invoking them.
#[derive(Clone)]
pub struct FactoryAuthServices {
    pub spawner: Arc<dyn TaskSpawner>,
    pub oauth: Option<OAuthServices>,
}
impl fmt::Debug for FactoryAuthServices {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("FactoryAuthServices([REDACTED])")
    }
}
pub struct BuiltinProviderServices {
    #[cfg(feature = "provider-anthropic")]
    pub anthropic: crate::api::runtime::AdapterServices,
    #[cfg(feature = "provider-google")]
    pub google: crate::api::runtime::AdapterServices,
    #[cfg(feature = "provider-openai")]
    pub openai: crate::api::runtime::AdapterServices,
    #[cfg(feature = "provider-deepseek")]
    pub completions: crate::api::runtime::AdapterServices,
    #[cfg(any(feature = "provider-anthropic", feature = "provider-openai"))]
    pub auth: FactoryAuthServices,
}
pub struct BuiltinProviderModules {
    #[cfg(feature = "provider-anthropic")]
    anthropic: Arc<crate::api::anthropic_messages::AnthropicMessagesApi>,
    #[cfg(feature = "provider-google")]
    google: Arc<crate::api::google_genai::GoogleGenAiApi>,
    #[cfg(feature = "provider-openai")]
    openai: Arc<crate::api::openai_responses::OpenAIResponsesApi>,
    #[cfg(feature = "provider-deepseek")]
    completions: Arc<crate::api::openai_completions::OpenAICompletionsApi>,
    #[cfg(any(feature = "provider-anthropic", feature = "provider-openai"))]
    auth: FactoryAuthServices,
}
impl BuiltinProviderModules {
    pub fn new(services: BuiltinProviderServices) -> Result<Self, ProviderFactoryError> {
        let _ = &services;
        Ok(Self {
            #[cfg(feature = "provider-anthropic")]
            anthropic: Arc::new(crate::api::anthropic_messages::AnthropicMessagesApi::new(
                services.anthropic,
            )),
            #[cfg(feature = "provider-google")]
            google: Arc::new(crate::api::google_genai::GoogleGenAiApi::new(
                services.google,
            )),
            #[cfg(feature = "provider-openai")]
            openai: Arc::new(crate::api::openai_responses::OpenAIResponsesApi::new(
                services.openai,
            )),
            #[cfg(feature = "provider-deepseek")]
            completions: Arc::new(crate::api::openai_completions::OpenAICompletionsApi::new(
                services.completions,
            )),
            #[cfg(any(feature = "provider-anthropic", feature = "provider-openai"))]
            auth: services.auth,
        })
    }
    #[cfg(feature = "provider-anthropic")]
    pub fn anthropic(&self) -> Arc<crate::api::anthropic_messages::AnthropicMessagesApi> {
        self.anthropic.clone()
    }
    #[cfg(feature = "provider-google")]
    pub fn google(&self) -> Arc<crate::api::google_genai::GoogleGenAiApi> {
        self.google.clone()
    }
    #[cfg(feature = "provider-openai")]
    pub fn openai(&self) -> Arc<crate::api::openai_responses::OpenAIResponsesApi> {
        self.openai.clone()
    }
    #[cfg(feature = "provider-deepseek")]
    pub fn completions(&self) -> Arc<crate::api::openai_completions::OpenAICompletionsApi> {
        self.completions.clone()
    }
    #[cfg(any(feature = "provider-anthropic", feature = "provider-openai"))]
    pub fn auth(&self) -> &FactoryAuthServices {
        &self.auth
    }
}
impl fmt::Debug for BuiltinProviderModules {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("BuiltinProviderModules([REDACTED])")
    }
}
