use super::{
    Command, NodeWebSocketLimits, Shared, SocketIo,
    codec::{self, Codec, Input},
    fail,
};
use crate::{TransportError, TransportErrorKind as Kind, WebSocketEvent, WebSocketState};
use pi_ai_core::{BoxFuture, CancellationToken, Clock};
use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::{io::ReadBuf, runtime::Handle, task::JoinHandle};

struct Write {
    bytes: Vec<u8>,
    offset: usize,
    ack: Option<tokio::sync::oneshot::Sender<()>>,
    control: bool,
}
struct Driver {
    socket: Box<dyn SocketIo>,
    codec: Codec,
    shared: Arc<Shared>,
    limits: NodeWebSocketLimits,
    clock: Arc<dyn Clock>,
    writes: VecDeque<Write>,
    control_bytes: usize,
    read_buffer: Vec<u8>,
    close_sent: bool,
    peer_close: Option<(u16, String)>,
    deadline: Option<BoxFuture<'static, ()>>,
    finished: bool,
    eof: bool,
    #[cfg(test)]
    inflate_gate: Option<Arc<InflateGate>>,
}
#[cfg(test)]
struct InflateGate {
    released: std::sync::atomic::AtomicBool,
    entered: std::sync::atomic::AtomicBool,
}
pub(super) fn spawn(
    handle: &Handle,
    socket: Box<dyn SocketIo>,
    codec: Codec,
    shared: Arc<Shared>,
    clock: Arc<dyn Clock>,
    limits: NodeWebSocketLimits,
) -> JoinHandle<()> {
    handle.spawn(Driver {
        socket,
        codec,
        shared,
        limits,
        clock,
        writes: VecDeque::new(),
        control_bytes: 0,
        read_buffer: vec![0; limits.read_buffer_bytes],
        close_sent: false,
        peer_close: None,
        deadline: None,
        finished: false,
        eof: false,
        #[cfg(test)]
        inflate_gate: None,
    })
}
impl Drop for Driver {
    fn drop(&mut self) {
        if !self.finished {
            self.shared.finish(Err(Kind::RuntimeUnavailable));
        }
    }
}
impl Driver {
    fn control(
        &mut self,
        opcode: u8,
        payload: &[u8],
        ack: Option<tokio::sync::oneshot::Sender<()>>,
    ) -> Result<(), TransportError> {
        let bytes = codec::encode(opcode, payload)?;
        let ceiling = if opcode == 8 {
            self.limits.control_reserve_bytes
        } else {
            self.limits.control_reserve_bytes.saturating_sub(131)
        };
        if bytes.len() > ceiling.saturating_sub(self.control_bytes) {
            return Err(fail(Kind::LimitExceeded));
        }
        self.control_bytes += bytes.len();
        self.writes.push_back(Write {
            bytes,
            offset: 0,
            ack,
            control: true,
        });
        Ok(())
    }
    fn command(&mut self, command: Command) -> Result<(), TransportError> {
        if command.close {
            if self.close_sent {
                let _ = command.ack.send(());
                return Ok(());
            }
            if command.bytes.len()
                > self
                    .limits
                    .control_reserve_bytes
                    .saturating_sub(self.control_bytes)
            {
                return Err(fail(Kind::LimitExceeded));
            }
            self.close_sent = true;
            self.control_bytes += command.bytes.len();
            self.writes.push_back(Write {
                bytes: command.bytes,
                offset: 0,
                ack: Some(command.ack),
                control: true,
            });
        } else {
            self.writes.push_back(Write {
                bytes: command.bytes,
                offset: 0,
                ack: None,
                control: false,
            });
            // Node send accepts into its send queue, not after socket flush.
            // Later IO failure belongs to the already attached receiver.
            let _ = command.ack.send(());
        }
        Ok(())
    }
    fn finish(&mut self, result: Result<WebSocketEvent, Kind>) -> Poll<()> {
        self.finished = true;
        self.shared.finish(result);
        Poll::Ready(())
    }
    fn read(&mut self, cx: &mut Context<'_>) -> Result<bool, TransportError> {
        let room = if self.peer_close.is_some() {
            self.read_buffer.len()
        } else {
            self.codec.read_capacity().min(self.read_buffer.len())
        };
        if room == 0 {
            return Ok(false);
        }
        let mut buffer = ReadBuf::new(&mut self.read_buffer[..room]);
        match Pin::new(&mut *self.socket).poll_read(cx, &mut buffer) {
            Poll::Ready(Ok(())) => {
                if buffer.filled().is_empty() {
                    self.eof = true;
                } else if self.peer_close.is_none() {
                    self.codec.push(buffer.filled())?;
                }
                Ok(true)
            }
            Poll::Ready(Err(_)) => Err(fail(Kind::Connection)),
            Poll::Pending => Ok(false),
        }
    }
    fn step(&mut self, cx: &mut Context<'_>) -> Result<bool, TransportError> {
        // Register the driver before checking commands. Cloning and disposing
        // wakers is deliberately outside the shared lock.
        let new = cx.waker().clone();
        let (aborted, commands, old) = {
            let mut state = self.shared.inner.lock().unwrap_or_else(|p| p.into_inner());
            (
                state.aborted,
                std::mem::take(&mut state.commands),
                state.driver_waiter.replace(new),
            )
        };
        drop(old);
        if aborted {
            return Err(fail(Kind::Closed));
        }
        for command in commands {
            self.command(command)?;
        }
        if let Some(deadline) = &mut self.deadline
            && deadline.as_mut().poll(cx).is_ready()
        {
            return Err(fail(Kind::Timeout));
        }
        let mut progressed = false;
        if let Some(write) = self.writes.front_mut() {
            if write.offset < write.bytes.len() {
                let end = write
                    .offset
                    .saturating_add(self.limits.read_buffer_bytes)
                    .min(write.bytes.len());
                match Pin::new(&mut *self.socket).poll_write(cx, &write.bytes[write.offset..end]) {
                    Poll::Ready(Ok(0)) => return Err(fail(Kind::Connection)),
                    Poll::Ready(Ok(n)) => {
                        write.offset += n;
                        progressed = true;
                    }
                    Poll::Ready(Err(_)) => return Err(fail(Kind::Connection)),
                    Poll::Pending => {}
                }
            }
            if write.offset == write.bytes.len() {
                match Pin::new(&mut *self.socket).poll_flush(cx) {
                    Poll::Ready(Ok(())) => {
                        let write = self.writes.pop_front().expect("front just observed");
                        if write.control {
                            self.control_bytes -= write.bytes.len();
                        } else {
                            let mut state =
                                self.shared.inner.lock().unwrap_or_else(|p| p.into_inner());
                            state.command_bytes =
                                state.command_bytes.saturating_sub(write.bytes.len());
                            state.command_count = state.command_count.saturating_sub(1);
                        }
                        if let Some(ack) = write.ack {
                            let _ = ack.send(());
                        }
                        progressed = true;
                    }
                    Poll::Ready(Err(_)) => return Err(fail(Kind::Connection)),
                    Poll::Pending => {}
                }
            }
        }
        let read_first = self.codec.in_flight();
        if read_first && !self.eof {
            progressed |= self.read(cx)?;
        }
        // Node can observe physical closure before its asynchronous inflater
        // publishes. An owned in-flight stage must not publish after that point.
        if self.eof && self.codec.in_flight() {
            return Ok(true);
        }
        #[cfg(test)]
        if self.codec.in_flight()
            && let Some(gate) = &self.inflate_gate
            && !gate.released.load(std::sync::atomic::Ordering::Acquire)
        {
            gate.entered
                .store(true, std::sync::atomic::Ordering::Release);
            return Ok(false);
        }
        // A bounded number of frames per poll; controls do not enter or wake the
        // application queue. A stalled writer never prevents this read path.
        if self.peer_close.is_none() {
            for index in 0..32 {
                let Some(input) = self.codec.next()? else {
                    break;
                };
                progressed = true;
                match input {
                    Input::Progress | Input::Pong => {}
                    Input::Message(message) => self.shared.publish(message)?,
                    Input::Ping(bytes) => self.control(10, &bytes, None)?,
                    Input::Close {
                        code,
                        reason,
                        reply,
                    } => {
                        {
                            let mut state =
                                self.shared.inner.lock().unwrap_or_else(|p| p.into_inner());
                            if state.state != WebSocketState::Closed {
                                state.state = WebSocketState::Closing;
                            }
                        }
                        if !self.close_sent {
                            self.control(8, &reply, None)?;
                            self.close_sent = true;
                        }
                        self.peer_close = Some((code, reason));
                        let clock = self.clock.clone();
                        let duration = self.limits.peer_close_grace;
                        self.deadline = Some(Box::pin(async move {
                            let _ = clock.sleep(duration, &CancellationToken::new()).await;
                        }));
                        break;
                    }
                }
                if self.codec.in_flight() {
                    break;
                }
                if index == 31 {
                    cx.waker().wake_by_ref();
                    return Ok(false);
                }
            }
        }
        if self.eof {
            if self.peer_close.is_none() || self.writes.is_empty() {
                return Ok(true);
            }
            if progressed {
                cx.waker().wake_by_ref();
            }
            return Ok(false);
        }
        if !read_first {
            progressed |= self.read(cx)?;
        }
        if progressed {
            cx.waker().wake_by_ref();
        }
        Ok(false)
    }
}
impl Future for Driver {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        match this.step(cx) {
            Ok(true) => {
                let terminal = this
                    .peer_close
                    .take()
                    .map(|(code, reason)| WebSocketEvent::Close { code, reason })
                    .ok_or(Kind::Connection);
                this.finish(terminal)
            }
            Ok(false) => Poll::Pending,
            Err(error) => this.finish(Err(error.kind())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{NodeWebSocketLimits, TokioClock, WebSocketMessage};
    use base64::{Engine, engine::general_purpose::STANDARD};
    use std::{
        io::Write,
        net::{Shutdown, TcpListener},
        sync::{
            atomic::{AtomicBool, Ordering},
            mpsc,
        },
        time::Duration,
    };

    #[test]
    fn physical_eof_and_inflight_completion_have_two_causal_orders() {
        let native = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../conformance/ai-transport/node-websocket/trace.json"
        ))
        .unwrap();
        let case = fixture["cases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|v| v["id"] == "compressed")
            .unwrap();
        let frame = STANDARD
            .decode(case["frames"][0].as_str().unwrap())
            .unwrap();
        for closed_first in [true, false] {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let (go, going) = mpsc::channel();
            let frame = frame.clone();
            let server = std::thread::spawn(move || {
                let (mut peer, _) = listener.accept().unwrap();
                peer.write_all(&[vec![129, 16], b"already-admitted".to_vec(), frame].concat())
                    .unwrap();
                going.recv_timeout(Duration::from_secs(3)).unwrap();
                peer.shutdown(Shutdown::Write).unwrap();
                let mut b = vec![];
                let _ = std::io::Read::read_to_end(&mut peer, &mut b);
            });
            let socket = native
                .block_on(tokio::net::TcpStream::connect(address))
                .unwrap();
            let limits = NodeWebSocketLimits::default();
            let shared = Arc::new(Shared::new(limits));
            {
                let mut state = shared.inner.lock().unwrap();
                state.epoch = Some(super::super::Epoch {
                    id: 1,
                    queue: VecDeque::new(),
                    bytes: 0,
                    terminal: None,
                    waiter: None,
                });
            }
            let codec = Codec::new(Some("permessage-deflate"), 65536, 65536, limits).unwrap();
            let gate = Arc::new(InflateGate {
                released: AtomicBool::new(false),
                entered: AtomicBool::new(false),
            });
            let task = native.handle().spawn(Driver {
                socket: Box::new(socket),
                codec,
                shared: shared.clone(),
                limits,
                clock: Arc::new(TokioClock::new(native.handle().clone())),
                writes: VecDeque::new(),
                control_bytes: 0,
                read_buffer: vec![0; 8192],
                close_sent: false,
                peer_close: None,
                deadline: None,
                finished: false,
                eof: false,
                inflate_gate: Some(gate.clone()),
            });
            native.block_on(async {
                tokio::time::timeout(Duration::from_secs(2), async {
                    while !gate.entered.load(Ordering::Acquire) {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
            });
            if closed_first {
                go.send(()).unwrap();
            } else {
                gate.released.store(true, Ordering::Release);
                let w = shared.inner.lock().unwrap().driver_waiter.take();
                Shared::wake(w);
                native.block_on(async {
                    tokio::time::timeout(Duration::from_secs(2), async {
                        while shared
                            .inner
                            .lock()
                            .unwrap()
                            .epoch
                            .as_ref()
                            .unwrap()
                            .queue
                            .len()
                            != 2
                        {
                            tokio::task::yield_now().await;
                        }
                    })
                    .await
                    .unwrap();
                });
                go.send(()).unwrap();
            }
            native.block_on(async {
                tokio::time::timeout(Duration::from_secs(2), task)
                    .await
                    .unwrap()
                    .unwrap();
            });
            gate.released.store(true, Ordering::Release);
            let state = shared.inner.lock().unwrap();
            let epoch = state.epoch.as_ref().unwrap();
            assert_eq!(state.state, WebSocketState::Closed);
            assert_eq!(epoch.queue.len(), if closed_first { 1 } else { 2 });
            assert_eq!(
                epoch.queue.front(),
                Some(&WebSocketEvent::Message(WebSocketMessage::Text(
                    "already-admitted".into()
                )))
            );
            assert!(matches!(epoch.terminal, Some(Err(Kind::Connection))));
            drop(state);
            server.join().unwrap();
        }
    }
}
