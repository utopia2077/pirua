//! Behavior-free AI data-transfer objects for the Rust harness workspace.
//!
//! This crate owns serialization-stable provider-neutral data. Provider
//! adapters, streams, credential stores, generated catalogs, and other runtime
//! behavior deliberately live in later crates and slices.

#![deny(missing_docs)]

pub mod auth;
pub mod content;
pub mod context;
pub mod diagnostic;
pub mod event;
pub mod id;
pub mod json;
pub mod message;
pub mod model;
pub mod options;
pub mod tool;
pub mod usage;

pub use auth::*;
pub use content::*;
pub use context::*;
pub use diagnostic::*;
pub use event::*;
pub use id::*;
pub use json::*;
pub use message::*;
pub use model::*;
pub use options::*;
pub use tool::*;
pub use usage::*;
