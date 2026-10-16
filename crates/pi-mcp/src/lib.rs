//! A transport-neutral MCP client. Results are MCP data, independent of runtimes.
//!
//! The protocol surface follows Pi v1.4.0 (commit
//! `4a080252f06caf63a525d7873c5d94d3c23887e0`). See the crate README for boundaries.
#![doc = include_str!("../README.md")]

mod cancel;
mod client;
mod error;
pub mod oauth;
pub mod protocol;
pub mod transport;

pub use cancel::CancellationToken;
pub use client::{ClientEvent, ClientOptions, McpClient, RequestHandler, RequestOptions};
pub use error::{Error, Result};
pub use protocol::*;

/// Object-safe asynchronous extension point used by transports, handlers and stores.
pub type BoxFuture<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;
