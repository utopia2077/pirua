//! Application-level persistent socket capability. Physical Ping/Pong are private.
use crate::{TransportError, WebSocketRequest};
use pi_ai_core::{BoxFuture, RequestRuntime};
use std::{fmt, time::Duration};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WebSocketState {
    Open,
    Closing,
    Closed,
}

#[derive(Clone, Eq, PartialEq)]
pub enum WebSocketMessage {
    Text(String),
    Binary(Vec<u8>),
}
impl fmt::Debug for WebSocketMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("WebSocketMessage([REDACTED])")
    }
}
#[derive(Clone, Eq, PartialEq)]
pub enum WebSocketEvent {
    Message(WebSocketMessage),
    /// Observed code may include reserved 1005; this is not wire-send permission.
    Close {
        code: u16,
        reason: String,
    },
}
impl fmt::Debug for WebSocketEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("WebSocketEvent([REDACTED])")
    }
}
pub trait PersistentWebSocketConnector: Send + Sync {
    fn connect<'a>(
        &'a self,
        request: &'a WebSocketRequest,
        runtime: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<Box<dyn PersistentWebSocket>, TransportError>>;
}
pub trait PersistentWebSocket: Send {
    fn state(&self) -> WebSocketState;
    /// Atomically admits one message and attaches its receiver. An abandoned
    /// admitted send retires the connection; overlapping exchanges do no IO.
    fn start_exchange<'a>(
        &'a mut self,
        message: WebSocketMessage,
        runtime: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<Box<dyn WebSocketExchange>, TransportError>>;
    /// Flushes a validated Close and waits for physical closure under an explicit
    /// grace. Zero expires immediately. Drop/cancel retires pending IO.
    fn shutdown<'a>(
        &'a mut self,
        code: u16,
        reason: String,
        runtime: &'a RequestRuntime,
        close_grace: Duration,
    ) -> BoxFuture<'a, Result<(), TransportError>>;
    /// Immediate release. Dropping the connection has the same resource effect.
    fn abort(&mut self);
}
pub trait WebSocketExchange: Send {
    /// Dropping only this wait unregisters its waiter, not its receiver/socket.
    /// Dropping the receiver detaches its epoch and discards its queued messages.
    fn next<'a>(
        &'a mut self,
        runtime: &'a RequestRuntime,
        idle_timeout: Option<Duration>,
    ) -> BoxFuture<'a, Result<Option<WebSocketEvent>, TransportError>>;
}
