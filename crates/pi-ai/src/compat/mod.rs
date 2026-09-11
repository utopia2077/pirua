//! Optional process-local compatibility declarations; importing them performs no effects.
//!
//! Callback inputs deliberately do not define a serialized extension protocol.
//! ```
//! use pi_ai::compat::{OAuthPrompt, OAuthLoginCallbacks};
//! let prompt = OAuthPrompt { message: String::new(), placeholder: None, allow_empty: None };
//! assert!(prompt.allow_empty.is_none());
//! fn accept_port(_: &dyn OAuthLoginCallbacks) {}
//! ```
//! ```compile_fail
//! use pi_ai::compat::OAuthPrompt;
//! let prompt = OAuthPrompt { message: String::new(), placeholder: None, allow_empty: None };
//! let _ = serde_json::to_value(prompt);
//! ```
//! ```compile_fail
//! use pi_ai::compat::OAuthAuthInfo;
//! let _: OAuthAuthInfo = serde_json::from_str("{}").unwrap();
//! ```
//! ```compile_fail
//! use pi_ai::compat::OAuthLoginCallbacks;
//! fn serialize(callbacks: &dyn OAuthLoginCallbacks) {
//!     let _ = serde_json::to_value(callbacks);
//! }
//! ```

mod oauth_types;
pub use oauth_types::*;
