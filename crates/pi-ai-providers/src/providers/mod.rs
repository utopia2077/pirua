//! Passive selected factories and explicitly configured module custody.
mod error;
mod modules;
pub use error::*;
pub use modules::*;
#[cfg(feature = "provider-anthropic")]
mod anthropic;
#[cfg(any(
    feature = "provider-anthropic",
    feature = "provider-openai-codex",
    feature = "provider-openrouter",
    feature = "images-openrouter"
))]
mod oauth;
#[cfg(any(
    feature = "provider-anthropic",
    feature = "provider-deepseek",
    feature = "provider-google",
    feature = "provider-openai",
    feature = "provider-openai-codex",
    feature = "provider-openrouter"
))]
mod text;
#[cfg(feature = "provider-anthropic")]
pub use anthropic::*;
#[cfg(feature = "provider-deepseek")]
mod deepseek;
#[cfg(feature = "provider-deepseek")]
pub use deepseek::*;
#[cfg(feature = "provider-google")]
mod google;
#[cfg(feature = "provider-google")]
pub use google::*;
#[cfg(feature = "provider-openai")]
mod openai;
#[cfg(feature = "provider-openai")]
pub use openai::*;
#[cfg(feature = "provider-openai-codex")]
mod openai_codex;
#[cfg(feature = "provider-openai-codex")]
pub use openai_codex::*;
#[cfg(feature = "provider-openrouter")]
mod openrouter;
#[cfg(feature = "provider-openrouter")]
pub use openrouter::*;
#[cfg(feature = "images-openrouter")]
mod openrouter_images;
#[cfg(feature = "images-openrouter")]
pub use openrouter_images::*;
mod metadata;
pub use metadata::*;
