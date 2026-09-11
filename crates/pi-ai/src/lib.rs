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
