//! Selected OAuth flows; host effects use explicit services.
#[cfg(feature = "oauth-anthropic")]
mod anthropic;
#[cfg(feature = "oauth-anthropic")]
mod browser;
#[cfg(feature = "oauth-openai-chatgpt")]
mod chatgpt;
mod common;
mod page;
#[cfg(feature = "oauth-anthropic")]
pub use anthropic::AnthropicOAuth;
#[cfg(feature = "oauth-openai-chatgpt")]
pub use chatgpt::OpenAIChatGPTOAuth;
pub use common::{OAuthFlowError, OAuthFlowErrorKind};
