//! Provider-neutral AI data and portable runtime contracts.
//!
//! This crate owns serialization-stable provider-neutral data together with
//! executor-neutral stream, cancellation, request-custody, and adapter ports.

#![deny(missing_docs)]

pub mod adapter;
pub mod auth;
pub mod auth_runtime;
pub mod content;
pub mod context;
pub mod context_estimate;
pub mod credential_store;
pub mod deferred_tools;
pub mod diagnostic;
pub mod error;
pub mod event;
pub mod hash;
pub mod id;
pub mod json;
pub mod json_parse;
pub mod message;
pub mod message_transform;
pub mod model;
pub mod models_helpers;
pub mod models_registry;
pub mod models_store;
pub mod options;
pub mod overflow;
pub mod request;
pub mod retry;
pub mod runtime;
pub mod session_resources;
pub mod stream;
pub mod text;
pub mod tool;
pub mod usage;
pub mod uuid;
pub mod validation;

pub use adapter::*;
pub use auth::*;
pub use auth_runtime::*;
pub use content::*;
pub use context::*;
pub use context_estimate::*;
pub use credential_store::*;
pub use deferred_tools::*;
pub use diagnostic::*;
pub use error::*;
pub use event::*;
pub use hash::*;
pub use id::*;
pub use json::*;
pub use json_parse::*;
pub use message::*;
pub use message_transform::*;
pub use model::*;
pub use models_helpers::*;
pub use models_registry::*;
pub use models_store::*;
pub use options::*;
pub use overflow::*;
pub use request::*;
pub use retry::*;
pub use runtime::*;
pub use session_resources::*;
pub use stream::*;
pub use text::*;
pub use tool::*;
pub use usage::*;
pub use uuid::*;
pub use validation::*;
