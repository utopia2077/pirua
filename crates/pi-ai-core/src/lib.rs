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
pub mod credential_store;
pub mod diagnostic;
pub mod error;
pub mod event;
pub mod id;
pub mod json;
pub mod message;
pub mod model;
pub mod models_registry;
pub mod models_store;
pub mod options;
pub mod request;
pub mod runtime;
pub mod stream;
pub mod tool;
pub mod usage;

pub use adapter::*;
pub use auth::*;
pub use auth_runtime::*;
pub use content::*;
pub use context::*;
pub use credential_store::*;
pub use diagnostic::*;
pub use error::*;
pub use event::*;
pub use id::*;
pub use json::*;
pub use message::*;
pub use model::*;
pub use models_registry::*;
pub use models_store::*;
pub use options::*;
pub use request::*;
pub use runtime::*;
pub use stream::*;
pub use tool::*;
pub use usage::*;
