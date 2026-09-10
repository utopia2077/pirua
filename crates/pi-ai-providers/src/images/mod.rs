//! Explicit image services and opt-in image wire adapters. No registration or I/O on import.
pub mod services;
pub use services::{ImageAdapterServices, ImagePayloadMiddleware, ImageResponseMiddleware};

#[cfg(feature = "wire-openrouter-images")]
pub mod openrouter;
#[cfg(feature = "wire-openrouter-images")]
mod wire_request;
#[cfg(feature = "wire-openrouter-images")]
mod wire_response;
