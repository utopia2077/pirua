use super::ProviderFactoryError;
use crate::auth::OAuthServices;
use pi_ai_core::TaskSpawner;
use std::{fmt, sync::Arc};
/// Explicit host capabilities, captured without accessing them.
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
/// Only the enabled factories require service fields.
pub struct BuiltinProviderServices {
    #[cfg(feature = "provider-anthropic")]
    pub anthropic: crate::api::runtime::AdapterServices,
    #[cfg(feature = "provider-google")]
    pub google: crate::api::runtime::AdapterServices,
    #[cfg(feature = "provider-openai")]
    pub openai: crate::api::runtime::AdapterServices,
    #[cfg(any(feature = "provider-deepseek", feature = "provider-openrouter"))]
    pub completions: crate::api::runtime::AdapterServices,
    #[cfg(feature = "provider-openai-codex")]
    pub codex: crate::api::openai_codex_responses::CodexServices,
    #[cfg(feature = "images-openrouter")]
    pub images: crate::images::ImageAdapterServices,
    #[cfg(any(
        feature = "provider-anthropic",
        feature = "provider-openai-codex",
        feature = "provider-openrouter",
        feature = "images-openrouter"
    ))]
    pub auth: FactoryAuthServices,
}
/// Retains each configured concrete module exactly once.
pub struct BuiltinProviderModules {
    #[cfg(feature = "provider-anthropic")]
    anthropic: Arc<crate::api::anthropic_messages::AnthropicMessagesApi>,
    #[cfg(feature = "provider-google")]
    google: Arc<crate::api::google_genai::GoogleGenAiApi>,
    #[cfg(feature = "provider-openai")]
    openai: Arc<crate::api::openai_responses::OpenAIResponsesApi>,
    #[cfg(any(feature = "provider-deepseek", feature = "provider-openrouter"))]
    completions: Arc<crate::api::openai_completions::OpenAICompletionsApi>,
    #[cfg(feature = "provider-openai-codex")]
    codex: Arc<crate::api::openai_codex_responses::OpenAICodexResponsesApi>,
    #[cfg(feature = "images-openrouter")]
    images: Arc<crate::images::openrouter::OpenRouterImagesApi>,
    #[cfg(any(
        feature = "provider-anthropic",
        feature = "provider-openai-codex",
        feature = "provider-openrouter",
        feature = "images-openrouter"
    ))]
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
            #[cfg(any(feature = "provider-deepseek", feature = "provider-openrouter"))]
            completions: Arc::new(crate::api::openai_completions::OpenAICompletionsApi::new(
                services.completions,
            )),
            #[cfg(feature = "provider-openai-codex")]
            codex: Arc::new(
                crate::api::openai_codex_responses::OpenAICodexResponsesApi::new(services.codex)
                    .map_err(ProviderFactoryError::from_source)?,
            ),
            #[cfg(feature = "images-openrouter")]
            images: Arc::new(crate::images::openrouter::OpenRouterImagesApi::new(
                services.images,
            )),
            #[cfg(any(
                feature = "provider-anthropic",
                feature = "provider-openai-codex",
                feature = "provider-openrouter",
                feature = "images-openrouter"
            ))]
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
    #[cfg(any(feature = "provider-deepseek", feature = "provider-openrouter"))]
    pub fn completions(&self) -> Arc<crate::api::openai_completions::OpenAICompletionsApi> {
        self.completions.clone()
    }
    #[cfg(feature = "provider-openai-codex")]
    pub fn codex(&self) -> Arc<crate::api::openai_codex_responses::OpenAICodexResponsesApi> {
        self.codex.clone()
    }
    #[cfg(feature = "images-openrouter")]
    pub fn images(&self) -> Arc<crate::images::openrouter::OpenRouterImagesApi> {
        self.images.clone()
    }
    #[cfg(any(
        feature = "provider-anthropic",
        feature = "provider-openai-codex",
        feature = "provider-openrouter",
        feature = "images-openrouter"
    ))]
    pub fn auth(&self) -> &FactoryAuthServices {
        &self.auth
    }
}
impl fmt::Debug for BuiltinProviderModules {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("BuiltinProviderModules([REDACTED])")
    }
}
