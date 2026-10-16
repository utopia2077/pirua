//! Provider-neutral AI facade and explicit selected provider composition.
mod lax_messages;
pub use lax_messages::*;
pub use pi_ai_core::*;
#[path = "retry.rs"]
mod assistant_retry;
pub use assistant_retry::*;
#[cfg(any(
    feature = "all-providers",
    feature = "all-oauth",
    feature = "catalog-anthropic",
    feature = "catalog-deepseek",
    feature = "catalog-google",
    feature = "catalog-openai",
    feature = "provider-anthropic",
    feature = "provider-deepseek",
    feature = "provider-google",
    feature = "provider-openai",
    feature = "oauth-anthropic",
    feature = "oauth-openai-chatgpt",
    feature = "native"
))]
pub mod builtins;
#[cfg(any(
    feature = "all-providers",
    feature = "catalog-anthropic",
    feature = "catalog-deepseek",
    feature = "catalog-google",
    feature = "catalog-openai",
    feature = "provider-anthropic",
    feature = "provider-deepseek",
    feature = "provider-google",
    feature = "provider-openai"
))]
pub mod catalog;
