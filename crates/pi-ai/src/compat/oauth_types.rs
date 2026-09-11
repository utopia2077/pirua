//! Pinned extension OAuth declarations, using trusted Rust callback capabilities.

use pi_ai_core::{AuthInteractionError, BoxFuture, CancellationToken, SafeUnsigned};
use std::fmt;

/// The inherited credential value, with its existing serialization and custody rules.
pub use pi_ai_core::OAuthCredentials;

/// Legacy extension text prompt. Empty, omitted and explicitly false remain distinct.
#[derive(Clone, Eq, PartialEq)]
pub struct OAuthPrompt {
    pub message: String,
    pub placeholder: Option<String>,
    pub allow_empty: Option<bool>,
}

/// Transient authorization link and optional instructions.
#[derive(Clone, Eq, PartialEq)]
pub struct OAuthAuthInfo {
    pub url: String,
    pub instructions: Option<String>,
}

/// Transient device-code notification, with inherited safe-integer time admission.
#[derive(Clone, Eq, PartialEq)]
pub struct OAuthDeviceCodeInfo {
    pub user_code: String,
    pub verification_uri: String,
    pub interval_seconds: Option<SafeUnsigned>,
    pub expires_in_seconds: Option<SafeUnsigned>,
}

/// A selectable value and label. The source has no description field.
#[derive(Clone, Eq, PartialEq)]
pub struct OAuthSelectOption {
    pub id: String,
    pub label: String,
}

/// Select prompt; options retain their supplied order, including repeated values.
#[derive(Clone, Eq, PartialEq)]
pub struct OAuthSelectPrompt {
    pub message: String,
    pub options: Vec<OAuthSelectOption>,
}

/// Optional synchronous progress capability, distinct from absence.
pub trait OAuthProgressCallback: Send + Sync {
    fn notify(&self, message: &str) -> Result<(), AuthInteractionError>;
}

/// Optional manual-code capability, whose presence can be inspected without calling it.
pub trait OAuthManualCodeInputCallback: Send + Sync {
    fn input<'a>(&'a self) -> BoxFuture<'a, Result<String, AuthInteractionError>>;
}

/// Trusted, process-local extension callbacks. No flow or cancellation policy is added.
/// Synchronous source exceptions and asynchronous rejections use the existing typed error.
pub trait OAuthLoginCallbacks: Send + Sync {
    fn on_auth(&self, info: OAuthAuthInfo) -> Result<(), AuthInteractionError>;
    fn on_device_code(&self, info: OAuthDeviceCodeInfo) -> Result<(), AuthInteractionError>;
    fn on_prompt<'a>(
        &'a self,
        prompt: OAuthPrompt,
    ) -> BoxFuture<'a, Result<String, AuthInteractionError>>;
    fn on_select<'a>(
        &'a self,
        prompt: OAuthSelectPrompt,
    ) -> BoxFuture<'a, Result<Option<String>, AuthInteractionError>>;
    fn progress(&self) -> Option<&(dyn OAuthProgressCallback + '_)> {
        None
    }
    fn manual_code_input(&self) -> Option<&(dyn OAuthManualCodeInputCallback + '_)> {
        None
    }
    fn signal(&self) -> Option<&CancellationToken> {
        None
    }
}

macro_rules! redacted_debug {
    ($($name:ident),* $(,)?) => {$ (
        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(concat!(stringify!($name), "([REDACTED])"))
            }
        }
    )*};
}
redacted_debug!(
    OAuthPrompt,
    OAuthAuthInfo,
    OAuthDeviceCodeInfo,
    OAuthSelectOption,
    OAuthSelectPrompt
);

macro_rules! redacted_port_debug {
    ($($name:ident),* $(,)?) => {$ (
        impl fmt::Debug for dyn $name + '_ {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(concat!(stringify!($name), "([REDACTED])"))
            }
        }
    )*};
}
redacted_port_debug!(
    OAuthLoginCallbacks,
    OAuthProgressCallback,
    OAuthManualCodeInputCallback
);
