//! Selected OAuth flows. All host effects come from explicit OAuthServices.
#[cfg(feature = "oauth-anthropic")]
mod anthropic;
#[cfg(any(feature = "oauth-anthropic", feature = "oauth-openai-codex"))]
mod browser;
#[cfg(feature = "oauth-openai-codex")]
mod codex;
mod common;
#[cfg(feature = "oauth-openrouter")]
mod openrouter;
mod page;
#[cfg(feature = "oauth-anthropic")]
pub use anthropic::AnthropicOAuth;
#[cfg(feature = "oauth-openai-codex")]
pub use codex::OpenAICodexOAuth;
pub use common::{OAuthFlowError, OAuthFlowErrorKind};
#[cfg(feature = "oauth-openrouter")]
pub use openrouter::OpenRouterOAuth;
