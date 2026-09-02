//! Provider-neutral AI data and portable runtime contracts.
//!
//! This crate owns serialization-stable provider-neutral data together with
//! executor-neutral stream, cancellation, request-custody, and adapter ports.

#![deny(missing_docs)]

pub mod adapter;
pub mod auth;
pub mod content;
pub mod context;
pub mod diagnostic;
pub mod error;
pub mod event;
pub mod id;
pub mod json;
pub mod message;
pub mod model;
pub mod options;
pub mod request;
pub mod runtime;
pub mod stream;
pub mod tool;
pub mod usage;

pub use adapter::*;
pub use auth::*;
pub use content::*;
pub use context::*;
pub use diagnostic::*;
pub use error::*;
pub use event::*;
pub use id::*;
pub use json::*;
pub use message::*;
pub use model::*;
pub use options::*;
pub use request::*;
pub use runtime::*;
pub use stream::*;
pub use tool::*;
pub use usage::*;
