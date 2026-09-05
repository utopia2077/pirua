use crate::{HttpHeaders, ProxyUrl, TransportError};
use pi_ai_core::{BoxFuture, RequestEndpoint, RequestRuntime};
use std::{fmt, time::Duration};

/// Physical frame; no provider continuation, replay, or assistant settlement.
pub enum WebSocketFrame {
    Text(String),
    Binary(Vec<u8>),
    Ping(Vec<u8>),
    Pong(Vec<u8>),
    Close { code: Option<u16>, reason: String },
}
impl fmt::Debug for WebSocketFrame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("WebSocketFrame([REDACTED])")
    }
}
pub struct WebSocketRequest {
    pub endpoint: RequestEndpoint,
    pub headers: HttpHeaders,
    pub proxy: Option<ProxyUrl>,
    /// None disables deadline; Some(Duration::ZERO) expires immediately.
    pub connect_timeout: Option<Duration>,
    pub max_message_bytes: usize,
    pub max_frame_bytes: usize,
}
impl fmt::Debug for WebSocketRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("WebSocketRequest([REDACTED])")
    }
}
pub trait WebSocketConnection: Send {
    /// Invalid local input is rejected without writing or retiring the socket.
    /// Ping/Pong allow 125 bytes; Close reserves two bytes for a validated status
    /// code and permits no reason without a code.
    fn send<'a>(
        &'a mut self,
        frame: WebSocketFrame,
        runtime: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<(), TransportError>>;
    /// Flushes a peer Close reply within caller cancellation/idle bounds before
    /// returning the Close event and releasing the physical connection.
    fn next<'a>(
        &'a mut self,
        runtime: &'a RequestRuntime,
        idle_timeout: Option<Duration>,
    ) -> BoxFuture<'a, Result<Option<WebSocketFrame>, TransportError>>;
    /// Synchronous connection release; drop has the same resource effect.
    fn close(&mut self);
}
pub trait WebSocketConnector: Send + Sync {
    fn connect<'a>(
        &'a self,
        request: &'a WebSocketRequest,
        runtime: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<Box<dyn WebSocketConnection>, TransportError>>;
}
