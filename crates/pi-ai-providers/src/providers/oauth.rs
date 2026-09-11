use super::FactoryAuthServices;
use crate::auth::{LazyOAuth, LazyOAuthMetadata, OAuthLoader, OAuthServices};
use pi_ai_core::{AuthProviderError, BoxFuture, OAuthAuth};
use std::sync::Arc;
#[derive(Debug)]
struct UnavailableOAuth;
impl std::fmt::Display for UnavailableOAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("OAuth flow is not configured")
    }
}
impl std::error::Error for UnavailableOAuth {}
macro_rules! flow {
    ($loader:ident, $function:ident, $feature:literal, $ty:ident, $name:literal, $subscription:expr, $label:expr) => {
        struct $loader(Option<OAuthServices>);
        impl OAuthLoader for $loader {
            fn load(
                self: Arc<Self>,
            ) -> BoxFuture<'static, Result<Arc<dyn OAuthAuth>, AuthProviderError>> {
                Box::pin(async move {
                    #[cfg(feature = $feature)]
                    if let Some(services) = &self.0 {
                        return Ok(Arc::new(crate::auth::oauth::$ty::new(services.clone()))
                            as Arc<dyn OAuthAuth>);
                    }
                    let _ = &self.0;
                    Err(AuthProviderError::with_source(UnavailableOAuth))
                })
            }
        }
        pub(super) fn $function(services: &FactoryAuthServices) -> Arc<dyn OAuthAuth> {
            Arc::new(LazyOAuth::new(
                LazyOAuthMetadata {
                    name: $name.into(),
                    is_subscription: $subscription,
                    login_label: $label,
                },
                Arc::new($loader(services.oauth.clone())),
                services.spawner.clone(),
            ))
        }
    };
}
#[cfg(feature = "provider-anthropic")]
flow!(
    AnthropicLoader,
    anthropic,
    "oauth-anthropic",
    AnthropicOAuth,
    "Anthropic (Claude Pro/Max)",
    true,
    None
);
#[cfg(feature = "provider-openai-codex")]
flow!(
    CodexLoader,
    openai_codex,
    "oauth-openai-codex",
    OpenAICodexOAuth,
    "OpenAI (ChatGPT Plus/Pro)",
    true,
    None
);
#[cfg(any(feature = "provider-openrouter", feature = "images-openrouter"))]
flow!(
    OpenRouterLoader,
    openrouter,
    "oauth-openrouter",
    OpenRouterOAuth,
    "OpenRouter OAuth",
    false,
    Some("Sign in with OpenRouter".into())
);
