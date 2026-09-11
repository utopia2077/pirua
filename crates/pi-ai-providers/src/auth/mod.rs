//! Explicit provider authentication composition; imports and construction have no host effects.
mod context;
mod helpers;
mod lazy;
mod services;
pub use context::*;
pub use helpers::*;
pub use lazy::*;
pub use services::*;

#[cfg(any(
    feature = "oauth-anthropic",
    feature = "oauth-openai-codex",
    feature = "oauth-openrouter"
))]
pub mod oauth;
