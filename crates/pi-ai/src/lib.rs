//! Private AI-facade crate for the Rust harness workspace.

mod lax_messages;

pub use lax_messages::*;
pub use pi_ai_core::*;

#[path = "retry.rs"]
mod assistant_retry;
pub use assistant_retry::*;

#[cfg(feature = "compat")]
pub mod compat;
#[cfg(feature = "compat")]
pub use compat::{
    OAuthAuthInfo, OAuthDeviceCodeInfo, OAuthLoginCallbacks, OAuthPrompt, OAuthSelectOption,
    OAuthSelectPrompt,
};

#[cfg(any(
    feature = "catalog-anthropic",
    feature = "catalog-deepseek",
    feature = "catalog-google",
    feature = "catalog-openai",
    feature = "catalog-openai-codex",
    feature = "catalog-openrouter",
    feature = "catalog-openrouter-images",
    feature = "provider-anthropic",
    feature = "provider-deepseek",
    feature = "provider-google",
    feature = "provider-openai",
    feature = "provider-openai-codex",
    feature = "provider-openrouter",
    feature = "images-runtime",
    feature = "images-openrouter",
    feature = "oauth-anthropic",
    feature = "oauth-openai-codex",
    feature = "oauth-openrouter",
    feature = "all-providers",
    feature = "all-images",
    feature = "all-oauth",
    feature = "native",
    feature = "auth-native"
))]
pub mod builtins;
#[cfg(any(
    feature = "catalog-anthropic",
    feature = "catalog-deepseek",
    feature = "catalog-google",
    feature = "catalog-openai",
    feature = "catalog-openai-codex",
    feature = "catalog-openrouter",
    feature = "catalog-openrouter-images",
    feature = "provider-anthropic",
    feature = "provider-deepseek",
    feature = "provider-google",
    feature = "provider-openai",
    feature = "provider-openai-codex",
    feature = "provider-openrouter",
    feature = "images-runtime",
    feature = "images-openrouter",
    feature = "oauth-anthropic",
    feature = "oauth-openai-codex",
    feature = "oauth-openrouter",
    feature = "all-providers",
    feature = "all-images",
    feature = "all-oauth",
    feature = "native",
    feature = "auth-native"
))]
pub mod catalog;
#[cfg(feature = "images-runtime")]
pub mod images {
    pub use pi_ai_providers::images::*;
}
