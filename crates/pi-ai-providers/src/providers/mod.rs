//! Passive selected factories and explicitly configured module custody.
mod error;
mod metadata;
mod modules;
#[cfg(any(feature = "provider-anthropic", feature = "provider-openai"))]
mod oauth;
#[cfg(feature = "all-providers")]
mod text;
#[cfg(all(
    not(feature = "all-providers"),
    any(
        feature = "provider-anthropic",
        feature = "provider-deepseek",
        feature = "provider-google",
        feature = "provider-openai"
    )
))]
mod text;
pub use error::*;
pub use metadata::*;
pub use modules::*;
#[cfg(feature = "provider-anthropic")]
mod anthropic;
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
