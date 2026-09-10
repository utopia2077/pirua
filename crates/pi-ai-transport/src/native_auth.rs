//! Explicit native callback service. Provider code owns all OAuth decisions.
use crate::{
    AuthHostError, AuthHostErrorKind, CallbackBind, CallbackListener, CallbackReply,
    CallbackRequest, CallbackResponse, CallbackSession, OAuthCrypto,
};
use pi_ai_core::{BoxFuture, CancellationToken};
use ring::rand::SecureRandom;
use std::{
    future::{Future, poll_fn},
    net::SocketAddr,
    task::Poll,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    runtime::Handle,
    sync::{mpsc, oneshot},
    task::{JoinHandle, JoinSet},
};

#[derive(Default)]
pub struct NativeOAuthCrypto;
impl OAuthCrypto for NativeOAuthCrypto {
    fn fill_random(&self, bytes: &mut [u8]) -> Result<(), AuthHostError> {
        ring::rand::SystemRandom::new()
            .fill(bytes)
            .map_err(|_| AuthHostError::new(AuthHostErrorKind::Crypto))
    }
    fn sha256(&self, bytes: &[u8]) -> Result<[u8; 32], AuthHostError> {
        let digest = ring::digest::digest(&ring::digest::SHA256, bytes);
        let mut out = [0; 32];
        out.copy_from_slice(digest.as_ref());
        Ok(out)
    }
}
pub struct NativeCallbackListener {
    handle: Handle,
}
impl NativeCallbackListener {
    pub fn new(handle: Handle) -> Self {
        Self { handle }
    }
}
struct AbortTask<T>(JoinHandle<T>);
impl<T> Drop for AbortTask<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}
fn io(error: std::io::Error) -> AuthHostError {
    AuthHostError::with_source(AuthHostErrorKind::Io, error)
}
async fn cancel<T>(
    future: impl Future<Output = Result<T, AuthHostError>>,
    c: &CancellationToken,
) -> Result<T, AuthHostError> {
    let mut future = Box::pin(future);
    let mut cancelled = Box::pin(c.cancelled());
    poll_fn(|cx| {
        if cancelled.as_mut().poll(cx).is_ready() {
            Poll::Ready(Err(AuthHostError::new(AuthHostErrorKind::Cancelled)))
        } else {
            future.as_mut().poll(cx)
        }
    })
    .await
}
struct NativeSession {
    addr: SocketAddr,
    requests: mpsc::Receiver<CallbackRequest>,
    closed: CancellationToken,
    task: Option<JoinHandle<()>>,
    close_result: Option<Result<(), AuthHostError>>,
}
impl Drop for NativeSession {
    fn drop(&mut self) {
        self.closed.cancel();
        self.requests.close(); /* supervisor drains all connections before ending */
    }
}
impl CallbackSession for NativeSession {
    fn local_addr(&self) -> SocketAddr {
        self.addr
    }
    fn next<'a>(
        &'a mut self,
        c: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<CallbackRequest, AuthHostError>> {
        Box::pin(async move {
            cancel(
                async {
                    self.requests
                        .recv()
                        .await
                        .ok_or_else(|| AuthHostError::new(AuthHostErrorKind::Closed))
                },
                c,
            )
            .await
        })
    }
    fn close<'a>(&'a mut self) -> BoxFuture<'a, Result<(), AuthHostError>> {
        Box::pin(async move {
            self.closed.cancel();
            self.requests.close();
            while self.requests.try_recv().is_ok() {}
            if let Some(result) = &self.close_result {
                return result.clone();
            }
            let result = if let Some(task) = self.task.as_mut() {
                task.await
                    .map_err(|e| AuthHostError::with_source(AuthHostErrorKind::Runtime, e))
            } else {
                Ok(())
            };
            self.task = None;
            self.close_result = Some(result.clone());
            result
        })
    }
}
impl CallbackListener for NativeCallbackListener {
    fn bind<'a>(
        &'a self,
        config: CallbackBind,
        c: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Box<dyn CallbackSession>, AuthHostError>> {
        Box::pin(async move {
            let limits = config.limits;
            if limits.max_connections == 0
                || limits.max_connections > 1024
                || limits.max_request_head_bytes < 16
                || limits.max_request_head_bytes > 1_048_576
                || limits.max_response_bytes == 0
                || limits.max_response_bytes > 16_777_216
            {
                return Err(AuthHostError::new(AuthHostErrorKind::InvalidInput));
            }
            if c.is_cancelled() {
                return Err(AuthHostError::new(AuthHostErrorKind::Cancelled));
            }
            let handle = self.handle.clone();
            let mut task = AbortTask(self.handle.spawn(async move {
                let listener = TcpListener::bind((config.host.as_str(), config.port))
                    .await
                    .map_err(io)?;
                let addr = listener.local_addr().map_err(io)?;
                let (sender, requests) = mpsc::channel(limits.max_connections);
                let closed = CancellationToken::new();
                let task = handle.spawn(supervise(listener, sender, limits, closed.clone()));
                Ok(Box::new(NativeSession {
                    addr,
                    requests,
                    closed,
                    task: Some(task),
                    close_result: None,
                }) as Box<dyn CallbackSession>)
            }));
            cancel(
                async {
                    (&mut task.0)
                        .await
                        .map_err(|e| AuthHostError::with_source(AuthHostErrorKind::Runtime, e))?
                },
                c,
            )
            .await
        })
    }
}
async fn supervise(
    listener: TcpListener,
    sender: mpsc::Sender<CallbackRequest>,
    limits: crate::CallbackLimits,
    closed: CancellationToken,
) {
    let mut connections = JoinSet::new();
    loop {
        enum Event {
            Closed,
            Accepted(std::io::Result<(TcpStream, SocketAddr)>),
            Finished,
        }
        let event = {
            let mut accept = Box::pin(listener.accept());
            let mut finished = (!connections.is_empty()).then(|| Box::pin(connections.join_next()));
            let mut cancelled = Box::pin(closed.cancelled());
            poll_fn(|cx| {
                if cancelled.as_mut().poll(cx).is_ready() {
                    return Poll::Ready(Event::Closed);
                }
                if finished
                    .as_mut()
                    .is_some_and(|future| future.as_mut().poll(cx).is_ready())
                {
                    return Poll::Ready(Event::Finished);
                }
                accept.as_mut().poll(cx).map(Event::Accepted)
            })
            .await
        };
        match event {
            Event::Closed => break,
            Event::Finished => {}
            Event::Accepted(Err(_)) => break,
            Event::Accepted(Ok((socket, _))) => {
                if connections.len() >= limits.max_connections {
                    drop(socket);
                    continue;
                }
                let sender = sender.clone();
                let closed = closed.clone();
                connections.spawn(async move {
                    let _ = cancel(serve(socket, sender, limits), &closed).await;
                });
            }
        }
    }
    drop(listener);
    drop(sender);
    connections.abort_all();
    while connections.join_next().await.is_some() {}
}
type ReplyMessage = (
    CallbackResponse,
    CancellationToken,
    oneshot::Sender<Result<(), AuthHostError>>,
);
struct NativeReply {
    response: Option<oneshot::Sender<ReplyMessage>>,
}
impl CallbackReply for NativeReply {
    fn send<'a>(
        mut self: Box<Self>,
        response: CallbackResponse,
        c: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), AuthHostError>> {
        Box::pin(async move {
            if c.is_cancelled() {
                return Err(AuthHostError::new(AuthHostErrorKind::Cancelled));
            }
            let (ack, done) = oneshot::channel();
            self.response
                .take()
                .ok_or_else(|| AuthHostError::new(AuthHostErrorKind::Closed))?
                .send((response, c.clone(), ack))
                .map_err(|_| AuthHostError::new(AuthHostErrorKind::Closed))?;
            cancel(
                async {
                    done.await
                        .map_err(|_| AuthHostError::new(AuthHostErrorKind::Closed))?
                },
                c,
            )
            .await
        })
    }
}
async fn serve(
    mut socket: TcpStream,
    sender: mpsc::Sender<CallbackRequest>,
    limits: crate::CallbackLimits,
) -> Result<(), AuthHostError> {
    let mut head = Vec::new();
    while !head.ends_with(b"\r\n\r\n") {
        if head.len() == limits.max_request_head_bytes {
            return Err(AuthHostError::new(AuthHostErrorKind::LimitExceeded));
        }
        let mut byte = [0];
        socket.read_exact(&mut byte).await.map_err(io)?;
        head.push(byte[0]);
    }
    let line = std::str::from_utf8(&head)
        .map_err(|_| AuthHostError::new(AuthHostErrorKind::InvalidInput))?
        .split("\r\n")
        .next()
        .unwrap_or("");
    let mut fields = line.split(' ');
    let method = fields.next().unwrap_or("");
    let target = fields.next().unwrap_or("");
    let version = fields.next().unwrap_or("");
    if method.is_empty()
        || target.is_empty()
        || fields.next().is_some()
        || !matches!(version, "HTTP/1.0" | "HTTP/1.1")
        || !method.bytes().all(|b| b.is_ascii_uppercase())
    {
        return Err(AuthHostError::new(AuthHostErrorKind::InvalidInput));
    }
    let (response, wait) = oneshot::channel();
    sender
        .send(CallbackRequest {
            method: method.into(),
            target: target.into(),
            reply: Box::new(NativeReply {
                response: Some(response),
            }),
        })
        .await
        .map_err(|_| AuthHostError::new(AuthHostErrorKind::Closed))?;
    let (response, cancellation, ack) = wait
        .await
        .map_err(|_| AuthHostError::new(AuthHostErrorKind::Closed))?;
    let result = cancel(
        write_response(&mut socket, response, limits.max_response_bytes),
        &cancellation,
    )
    .await;
    let _ = ack.send(result.clone());
    result
}
async fn write_response(
    socket: &mut TcpStream,
    response: CallbackResponse,
    max: usize,
) -> Result<(), AuthHostError> {
    if !(100..=599).contains(&response.status)
        || response.content_type.is_empty()
        || response.content_type.bytes().any(|b| b < 32 || b == 127)
        || response.content_type.len() > 1024
    {
        return Err(AuthHostError::new(AuthHostErrorKind::InvalidInput));
    }
    if response.body.len() > max {
        return Err(AuthHostError::new(AuthHostErrorKind::LimitExceeded));
    }
    let cache = if response.no_store {
        "Cache-Control: no-store\r\n"
    } else {
        ""
    };
    let head = format!(
        "HTTP/1.1 {} Response\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n{}\r\n",
        response.status,
        response.content_type,
        response.body.len(),
        cache
    );
    socket.write_all(head.as_bytes()).await.map_err(io)?;
    socket
        .write_all(response.body.as_bytes())
        .await
        .map_err(io)?;
    socket.shutdown().await.map_err(io)
}

#[cfg(test)]
mod close_failure_tests {
    use super::*;
    #[test]
    fn auth_host_repeated_close_preserves_supervisor_failure() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let (_, requests) = mpsc::channel(1);
            let task = runtime.spawn(std::future::pending());
            task.abort();
            let mut session = NativeSession {
                addr: "127.0.0.1:1".parse().unwrap(),
                requests,
                closed: CancellationToken::new(),
                task: Some(task),
                close_result: None,
            };
            for _ in 0..2 {
                assert_eq!(
                    session.close().await.unwrap_err().kind(),
                    AuthHostErrorKind::Runtime
                );
            }
        });
    }
}
