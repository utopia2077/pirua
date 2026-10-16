//! Framing and I/O only; request settlement belongs to `McpClient`.
pub(crate) mod http;
mod memory;
mod sse;
mod stdio;

pub use http::{
    AuthProvider, HttpOptions, ReconnectOptions, StreamableHttpTransport, UnauthorizedContext,
};
pub use memory::{MemoryPeer, MemoryTransport, memory_transport_pair};
pub use sse::{SseEvent, SseParser};
pub use stdio::{StdioOptions, StdioTransport};

use crate::{BoxFuture, CancellationToken, Error, Result};
use serde_json::Value;
use tokio::sync::mpsc;

#[derive(Clone, Debug)]
pub enum TransportEvent {
    Message(Value),
    Error(Error),
    Closed,
}

/// A single-use connection. `start` is called once before any sends. A transport
/// must honor cancellation before dispatch/retry and emit exactly one close event.
pub trait Transport: Send + Sync {
    fn start(&self) -> BoxFuture<'_, Result<mpsc::UnboundedReceiver<TransportEvent>>>;
    fn send(&self, message: Value, cancellation: CancellationToken) -> BoxFuture<'_, Result<()>>;
    fn set_protocol_version(&self, _version: &str) {}
    fn close(&self) -> BoxFuture<'_, Result<()>>;
}
