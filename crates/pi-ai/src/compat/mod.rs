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

mod facade;
mod images;
mod legacy_api_aliases;
mod registry;
mod session_resources;
pub use crate::catalog::{
    BuiltinProvider, get_builtin_images_model as get_image_model,
    get_builtin_images_models as get_image_models,
    get_builtin_images_providers as get_image_providers, get_builtin_model as get_model,
    get_builtin_models as get_models, get_builtin_providers as get_providers,
};
pub use crate::{RetryCallbacks, retry_assistant_call};
pub use facade::*;
pub use images::*;
pub use legacy_api_aliases::*;
pub use pi_ai_providers::auth::{
    ANTHROPIC_API_KEY_ENV, ANTHROPIC_AUTH_TOKEN_ENV, ANTHROPIC_OAUTH_TOKEN_ENV,
    DefaultProviderAuthContext, LazyOAuth, LazyOAuthMetadata, OAuthLoader, env_api_key_auth,
    find_env_keys, get_env_api_key,
};
pub use pi_ai_providers::images::{
    CreateImagesProviderOptions, ImagesApiBinding, ImagesApiRegistry, ImagesGenerateCall,
    ImagesGenerator, ImagesModels, ImagesModelsOptions, ImagesProvider, ImagesRequestOverrides,
    MutableImagesModels, create_images_models, create_images_provider,
};
pub use registry::*;
pub use session_resources::*;
