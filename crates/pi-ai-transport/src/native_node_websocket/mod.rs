//! Node26/Undici8.10 application-message profile, separate from generic WS.
mod codec;
mod driver;
mod handshake;
use super::{NativeWebSocketConnector, SocketIo};
use crate::{
    PersistentWebSocket, PersistentWebSocketConnector, TransportError, TransportErrorKind as Kind,
    WebSocketEvent, WebSocketExchange, WebSocketMessage, WebSocketRequest, WebSocketState,
};
use pi_ai_core::{BoxFuture, RequestRuntime};
use std::{
    collections::VecDeque,
    fmt,
    future::poll_fn,
    sync::{Arc, Mutex},
    task::{Poll, Waker},
    time::Duration,
};
use tokio::{
    runtime::Handle,
    sync::oneshot,
    task::{AbortHandle, JoinHandle},
};

/// Independent local resource budgets, not Node's unbounded defaults.
#[derive(Clone, Copy, Debug)]
pub struct NodeWebSocketLimits {
    pub max_handshake_bytes: usize,
    pub read_buffer_bytes: usize,
    pub max_fragments: usize,
    pub max_queue_messages: usize,
    pub max_queue_bytes: usize,
    pub max_pending_commands: usize,
    pub max_pending_command_bytes: usize,
    pub max_outbound_bytes: usize,
    pub control_reserve_bytes: usize,
    pub peer_close_grace: Duration,
}
impl Default for NodeWebSocketLimits {
    fn default() -> Self {
        Self {
            max_handshake_bytes: 65536,
            read_buffer_bytes: 8192,
            max_fragments: 4096,
            max_queue_messages: 128,
            max_queue_bytes: 16 * 1024 * 1024,
            max_pending_commands: 2,
            max_pending_command_bytes: 16 * 1024 * 1024 + 14,
            max_outbound_bytes: 16 * 1024 * 1024 + 14,
            control_reserve_bytes: 4096,
            peer_close_grace: Duration::from_secs(5),
        }
    }
}
impl NodeWebSocketLimits {
    fn validate(self) -> Result<Self, TransportError> {
        if self.max_handshake_bytes == 0
            || self.read_buffer_bytes == 0
            || self.read_buffer_bytes > 65536
            || self.max_fragments == 0
            || self.max_queue_messages == 0
            || self.max_queue_bytes == 0
            || self.max_pending_commands == 0
            || self.max_pending_command_bytes == 0
            || self.max_outbound_bytes == 0
            || self.control_reserve_bytes < 131
        {
            Err(fail(Kind::InvalidRequest))
        } else {
            Ok(self)
        }
    }
}
#[derive(Clone)]
pub struct Node26WebSocketConnector {
    base: NativeWebSocketConnector,
    limits: NodeWebSocketLimits,
}
impl fmt::Debug for Node26WebSocketConnector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Node26WebSocketConnector([REDACTED])")
    }
}
impl Node26WebSocketConnector {
    pub fn new(handle: Handle) -> Result<Self, TransportError> {
        Ok(Self {
            base: NativeWebSocketConnector::new(handle)?,
            limits: NodeWebSocketLimits::default(),
        })
    }
    pub fn with_tls_config(handle: Handle, tls: Arc<tokio_rustls::rustls::ClientConfig>) -> Self {
        Self {
            base: NativeWebSocketConnector::with_tls_config(handle, tls),
            limits: NodeWebSocketLimits::default(),
        }
    }
    pub fn with_limits(mut self, limits: NodeWebSocketLimits) -> Result<Self, TransportError> {
        self.limits = limits.validate()?;
        Ok(self)
    }
}
fn fail(kind: Kind) -> TransportError {
    TransportError::new(kind)
}
impl PersistentWebSocketConnector for Node26WebSocketConnector {
    fn connect<'a>(
        &'a self,
        request: &'a WebSocketRequest,
        runtime: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<Box<dyn PersistentWebSocket>, TransportError>> {
        Box::pin(async move {
            if request.max_message_bytes == 0 || request.max_frame_bytes == 0 {
                return Err(fail(Kind::InvalidRequest));
            }
            // Admit borrowed input before cloning secrets into the runtime task.
            // A new Node Headers construction uses request fields, not an attached
            // native Fetch response observation view.
            let raw_endpoint = request.endpoint.expose_for_transport();
            let mut charge = raw_endpoint.len();
            if let Some(proxy) = &request.proxy {
                charge = charge
                    .checked_add(proxy.expose_for_transport().len())
                    .ok_or_else(|| fail(Kind::LimitExceeded))?;
            }
            if charge > self.limits.max_handshake_bytes {
                return Err(fail(Kind::LimitExceeded));
            }
            for (name, value) in request.headers.iter_for_wire() {
                charge = charge
                    .checked_add(name.len())
                    .and_then(|n| n.checked_add(4))
                    .filter(|n| *n <= self.limits.max_handshake_bytes)
                    .ok_or_else(|| fail(Kind::LimitExceeded))?;
                let room = self.limits.max_handshake_bytes - charge;
                // A Latin1 scalar takes at most two UTF8 bytes. Reject oversized
                // raw inputs without walking their entire value or remaining map.
                if value.len() > room.saturating_mul(2) {
                    return Err(fail(Kind::LimitExceeded));
                }
                if value.chars().any(|c| u32::from(c) > 255) {
                    return Err(fail(Kind::InvalidRequest));
                }
                charge = charge
                    .checked_add(value.chars().count())
                    .filter(|n| *n <= self.limits.max_handshake_bytes)
                    .ok_or_else(|| fail(Kind::LimitExceeded))?;
            }
            if charge > self.limits.max_handshake_bytes {
                return Err(fail(Kind::LimitExceeded));
            }
            let endpoint = raw_endpoint.to_owned();
            let mut headers = crate::HttpHeaders::new();
            for (name, value) in request.headers.iter_for_wire() {
                headers.append(name, value)?;
            }
            let proxy = request.proxy.clone();
            let tls = self.base.tls.clone();
            let limits = self.limits;
            let acquired = super::on_runtime(&self.base.handle, async move {
                handshake::connect(endpoint, headers, proxy, tls, limits).await
            });
            let (socket, tail, extension) = crate::policy::wait_with_timeout(
                Box::pin(acquired),
                runtime,
                request.connect_timeout,
            )
            .await?;
            let mut codec = codec::Codec::new(
                extension.as_deref(),
                request.max_frame_bytes,
                request.max_message_bytes,
                limits,
            )?;
            codec.push(&tail)?;
            let shared = Arc::new(Shared::new(limits));
            let task = driver::spawn(
                &self.base.handle,
                socket,
                codec,
                shared.clone(),
                runtime.clock.clone(),
                limits,
            );
            Ok(Box::new(Connection {
                shared,
                task,
                max_message: request.max_message_bytes,
            }) as Box<dyn PersistentWebSocket>)
        })
    }
}

struct Command {
    bytes: Vec<u8>,
    ack: oneshot::Sender<()>,
    close: bool,
}
struct Epoch {
    id: u64,
    queue: VecDeque<WebSocketEvent>,
    bytes: usize,
    terminal: Option<Result<WebSocketEvent, Kind>>,
    waiter: Option<Waker>,
}
struct Inner {
    state: WebSocketState,
    epoch: Option<Epoch>,
    generation: u64,
    commands: VecDeque<Command>,
    command_bytes: usize,
    command_count: usize,
    driver_waiter: Option<Waker>,
    close_waiter: Option<Waker>,
    closure: Option<Result<(), Kind>>,
    aborted: bool,
}
struct Shared {
    inner: Mutex<Inner>,
    limits: NodeWebSocketLimits,
}
impl Shared {
    fn new(limits: NodeWebSocketLimits) -> Self {
        Self {
            limits,
            inner: Mutex::new(Inner {
                state: WebSocketState::Open,
                epoch: None,
                generation: 0,
                commands: VecDeque::new(),
                command_bytes: 0,
                command_count: 0,
                driver_waiter: None,
                close_waiter: None,
                closure: None,
                aborted: false,
            }),
        }
    }
    fn state(&self) -> WebSocketState {
        self.inner.lock().unwrap_or_else(|p| p.into_inner()).state
    }
    fn wake(waker: Option<Waker>) {
        if let Some(w) = waker {
            w.wake();
        }
    }
    fn finish(&self, terminal: Result<WebSocketEvent, Kind>) {
        let (waiter, closed, commands, driver) = {
            let mut state = self.inner.lock().unwrap_or_else(|p| p.into_inner());
            if state.closure.is_some() {
                return;
            }
            state.state = WebSocketState::Closed;
            state.closure = Some(terminal.as_ref().map(|_| ()).map_err(|e| *e));
            let waiter = if let Some(epoch) = &mut state.epoch {
                epoch.terminal = Some(terminal);
                epoch.waiter.take()
            } else {
                None
            };
            state.command_bytes = 0;
            state.command_count = 0;
            (
                waiter,
                state.close_waiter.take(),
                std::mem::take(&mut state.commands),
                state.driver_waiter.take(),
            )
        };
        drop(commands);
        drop(driver);
        Self::wake(waiter);
        Self::wake(closed);
    }
    fn abort(&self) {
        let (epoch, commands, driver, closed) = {
            let mut state = self.inner.lock().unwrap_or_else(|p| p.into_inner());
            state.aborted = true;
            state.state = WebSocketState::Closed;
            state.closure = Some(Err(Kind::Closed));
            state.command_bytes = 0;
            state.command_count = 0;
            (
                state.epoch.take(),
                std::mem::take(&mut state.commands),
                state.driver_waiter.take(),
                state.close_waiter.take(),
            )
        };
        let waiter = epoch.and_then(|mut e| e.waiter.take());
        drop(commands);
        Self::wake(waiter);
        Self::wake(driver);
        Self::wake(closed);
    }
    fn detach(&self, id: u64) {
        let old = {
            let mut state = self.inner.lock().unwrap_or_else(|p| p.into_inner());
            if state.epoch.as_ref().is_some_and(|e| e.id == id) {
                state.epoch.take()
            } else {
                None
            }
        };
        drop(old);
    }
    fn publish(&self, message: WebSocketMessage) -> Result<(), TransportError> {
        let length = message_len(&message);
        let event = WebSocketEvent::Message(message);
        let waiter = {
            let mut state = self.inner.lock().unwrap_or_else(|p| p.into_inner());
            if state.state != WebSocketState::Open {
                return Ok(());
            }
            let Some(epoch) = &mut state.epoch else {
                return Ok(());
            };
            if epoch.queue.len() >= self.limits.max_queue_messages
                || length > self.limits.max_queue_bytes.saturating_sub(epoch.bytes)
            {
                return Err(fail(Kind::LimitExceeded));
            }
            epoch.queue.push_back(event);
            epoch.bytes += length;
            epoch.waiter.take()
        };
        Self::wake(waiter);
        Ok(())
    }
}
struct Connection {
    shared: Arc<Shared>,
    task: JoinHandle<()>,
    max_message: usize,
}
impl Drop for Connection {
    fn drop(&mut self) {
        self.shared.abort();
        self.task.abort();
    }
}
struct Admission {
    shared: Arc<Shared>,
    task: AbortHandle,
    armed: bool,
}
impl Drop for Admission {
    fn drop(&mut self) {
        if self.armed {
            self.shared.abort();
            self.task.abort();
        }
    }
}
fn message_len(message: &WebSocketMessage) -> usize {
    match message {
        WebSocketMessage::Text(s) => s.len(),
        WebSocketMessage::Binary(b) => b.len(),
    }
}
impl PersistentWebSocket for Connection {
    fn state(&self) -> WebSocketState {
        self.shared.state()
    }
    fn start_exchange<'a>(
        &'a mut self,
        message: WebSocketMessage,
        runtime: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<Box<dyn WebSocketExchange>, TransportError>> {
        Box::pin(async move {
            if runtime.cancellation.is_cancelled() {
                return Err(fail(Kind::Cancelled));
            }
            let size = message_len(&message);
            let limits = self.shared.limits;
            if size > self.max_message
                || size
                    .checked_add(if size < 126 {
                        6
                    } else if size <= 65535 {
                        8
                    } else {
                        14
                    })
                    .is_none_or(|n| {
                        n > limits.max_pending_command_bytes || n > limits.max_outbound_bytes
                    })
            {
                return Err(fail(Kind::LimitExceeded));
            }
            let bytes = match message {
                WebSocketMessage::Text(s) => codec::encode(1, s.as_bytes())?,
                WebSocketMessage::Binary(b) => codec::encode(2, &b)?,
            };
            let (tx, rx) = oneshot::channel();
            let (id, waker) = {
                let mut state = self.shared.inner.lock().unwrap_or_else(|p| p.into_inner());
                if state.state != WebSocketState::Open {
                    return Err(fail(Kind::Closed));
                }
                if state.epoch.is_some() {
                    return Err(fail(Kind::InvalidRequest));
                }
                if state.command_count >= limits.max_pending_commands
                    || bytes.len()
                        > limits
                            .max_pending_command_bytes
                            .saturating_sub(state.command_bytes)
                    || bytes.len()
                        > limits
                            .max_outbound_bytes
                            .saturating_sub(state.command_bytes)
                {
                    return Err(fail(Kind::LimitExceeded));
                }
                let id = state
                    .generation
                    .checked_add(1)
                    .ok_or_else(|| fail(Kind::LimitExceeded))?;
                state.generation = id;
                state.epoch = Some(Epoch {
                    id,
                    queue: VecDeque::new(),
                    bytes: 0,
                    terminal: None,
                    waiter: None,
                });
                state.command_bytes += bytes.len();
                state.command_count += 1;
                state.commands.push_back(Command {
                    bytes,
                    ack: tx,
                    close: false,
                });
                (id, state.driver_waiter.take())
            };
            let mut guard = Admission {
                shared: self.shared.clone(),
                task: self.task.abort_handle(),
                armed: true,
            };
            Shared::wake(waker);
            crate::policy::cancel_wait(
                Box::pin(async move { rx.await.map_err(|_| fail(Kind::Closed)) }),
                &runtime.cancellation,
            )
            .await?;
            guard.armed = false;
            Ok(Box::new(Receiver {
                shared: self.shared.clone(),
                task: self.task.abort_handle(),
                id,
            }) as Box<dyn WebSocketExchange>)
        })
    }
    fn shutdown<'a>(
        &'a mut self,
        code: u16,
        reason: String,
        runtime: &'a RequestRuntime,
        close_grace: Duration,
    ) -> BoxFuture<'a, Result<(), TransportError>> {
        Box::pin(async move {
            if code != 1000 && !(3000..=4999).contains(&code) || reason.len() > 123 {
                return Err(fail(Kind::InvalidRequest));
            }
            let mut guard = Admission {
                shared: self.shared.clone(),
                task: self.task.abort_handle(),
                armed: true,
            };
            if runtime.cancellation.is_cancelled() {
                return Err(fail(Kind::Cancelled));
            }
            if close_grace.is_zero() {
                return Err(fail(Kind::Timeout));
            }
            let mut body = code.to_be_bytes().to_vec();
            body.extend_from_slice(reason.as_bytes());
            let bytes = codec::encode(8, &body)?;
            let (tx, rx) = oneshot::channel();
            let (admitted, waker) = {
                let mut state = self.shared.inner.lock().unwrap_or_else(|p| p.into_inner());
                if state.state == WebSocketState::Open {
                    state.state = WebSocketState::Closing;
                    state.commands.push_back(Command {
                        bytes,
                        ack: tx,
                        close: true,
                    });
                    (true, state.driver_waiter.take())
                } else {
                    (false, None)
                }
            };
            Shared::wake(waker);
            let shared = self.shared.clone();
            let operation = Box::pin(async move {
                if admitted {
                    rx.await.map_err(|_| fail(Kind::Closed))?;
                }
                poll_fn(|cx| {
                    let new = cx.waker().clone();
                    let (result, old) = {
                        let mut state = shared.inner.lock().unwrap_or_else(|p| p.into_inner());
                        if let Some(result) = state.closure {
                            (Poll::Ready(result.map_err(fail)), None)
                        } else {
                            (Poll::Pending, state.close_waiter.replace(new))
                        }
                    };
                    drop(old);
                    result
                })
                .await
            });
            let result =
                crate::policy::wait_with_timeout(operation, runtime, Some(close_grace)).await;
            if result.is_ok() {
                guard.armed = false;
            }
            result
        })
    }
    fn abort(&mut self) {
        self.shared.abort();
        self.task.abort();
    }
}
struct Receiver {
    shared: Arc<Shared>,
    task: AbortHandle,
    id: u64,
}
impl Drop for Receiver {
    fn drop(&mut self) {
        self.shared.detach(self.id);
    }
}
struct Waiter {
    shared: Arc<Shared>,
    id: u64,
}
impl Drop for Waiter {
    fn drop(&mut self) {
        let old = {
            let mut state = self.shared.inner.lock().unwrap_or_else(|p| p.into_inner());
            state
                .epoch
                .as_mut()
                .filter(|e| e.id == self.id)
                .and_then(|e| e.waiter.take())
        };
        drop(old);
    }
}
impl WebSocketExchange for Receiver {
    fn next<'a>(
        &'a mut self,
        runtime: &'a RequestRuntime,
        idle_timeout: Option<Duration>,
    ) -> BoxFuture<'a, Result<Option<WebSocketEvent>, TransportError>> {
        Box::pin(async move {
            let _guard = Waiter {
                shared: self.shared.clone(),
                id: self.id,
            };
            let mut operation = Box::pin(poll_fn(|cx| {
                let new = cx.waker().clone();
                let (result, old) = {
                    let mut state = self.shared.inner.lock().unwrap_or_else(|p| p.into_inner());
                    let closed = state.state == WebSocketState::Closed;
                    if state.aborted {
                        (Poll::Ready(Err(fail(Kind::Closed))), None)
                    } else if let Some(epoch) = state.epoch.as_mut().filter(|e| e.id == self.id) {
                        if let Some(event) = epoch.queue.pop_front() {
                            if let WebSocketEvent::Message(m) = &event {
                                epoch.bytes -= message_len(m);
                            }
                            (Poll::Ready(Ok(Some(event))), None)
                        } else if let Some(terminal) = epoch.terminal.take() {
                            (Poll::Ready(terminal.map(Some).map_err(fail)), None)
                        } else if closed {
                            (Poll::Ready(Ok(None)), None)
                        } else {
                            (Poll::Pending, epoch.waiter.replace(new))
                        }
                    } else {
                        (Poll::Ready(Ok(None)), None)
                    }
                };
                drop(old);
                result
            }));
            if runtime.cancellation.is_cancelled() {
                self.shared.abort();
                self.task.abort();
                return Err(fail(Kind::Cancelled));
            }
            // The source starts its idle timer only when the application queue
            // is empty; zero disables it. A queued event needs no clock call.
            let first = poll_fn(|cx| Poll::Ready(operation.as_mut().poll(cx))).await;
            if let Poll::Ready(result) = first {
                if runtime.cancellation.is_cancelled() {
                    self.shared.abort();
                    self.task.abort();
                    return Err(fail(Kind::Cancelled));
                }
                return result;
            }
            let result = crate::policy::wait_with_timeout(
                operation,
                runtime,
                idle_timeout.filter(|v| !v.is_zero()),
            )
            .await;
            if result
                .as_ref()
                .is_err_and(|e| matches!(e.kind(), Kind::Cancelled | Kind::Timeout))
            {
                self.shared.abort();
                self.task.abort();
            }
            result
        })
    }
}
