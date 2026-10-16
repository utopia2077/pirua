use super::{Transport, TransportEvent};
use crate::{BoxFuture, CancellationToken, Error, Result};
use serde_json::Value;
use std::sync::{
    Mutex,
    atomic::{AtomicBool, Ordering},
};
use tokio::sync::mpsc;

pub struct MemoryTransport {
    started: AtomicBool,
    receiver: Mutex<Option<mpsc::UnboundedReceiver<TransportEvent>>>,
    events: mpsc::UnboundedSender<TransportEvent>,
    outgoing: Mutex<Option<mpsc::UnboundedSender<Value>>>,
}
/// Server endpoint for offline consumer tests. It is not a production MCP server.
pub struct MemoryPeer {
    pub incoming: mpsc::UnboundedReceiver<Value>,
    pub outgoing: mpsc::UnboundedSender<TransportEvent>,
}
pub fn memory_transport_pair() -> (MemoryTransport, MemoryPeer) {
    let (events, receiver) = mpsc::unbounded_channel();
    let (outgoing, incoming) = mpsc::unbounded_channel();
    (
        MemoryTransport {
            started: AtomicBool::new(false),
            receiver: Mutex::new(Some(receiver)),
            events: events.clone(),
            outgoing: Mutex::new(Some(outgoing)),
        },
        MemoryPeer {
            incoming,
            outgoing: events,
        },
    )
}
impl Transport for MemoryTransport {
    fn start(&self) -> BoxFuture<'_, Result<mpsc::UnboundedReceiver<TransportEvent>>> {
        Box::pin(async {
            if self.outgoing.lock().unwrap().is_none() {
                return Err(Error::Closed);
            }
            self.started.store(true, Ordering::Release);
            self.receiver
                .lock()
                .unwrap()
                .take()
                .ok_or_else(|| Error::invalid("transport already started"))
        })
    }
    fn send(&self, message: Value, cancellation: CancellationToken) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            if !self.started.load(Ordering::Acquire) {
                return Err(Error::Closed);
            }
            if cancellation.is_cancelled() {
                return Err(Error::Cancelled);
            }
            self.outgoing
                .lock()
                .unwrap()
                .as_ref()
                .ok_or(Error::Closed)?
                .send(message)
                .map_err(|_| Error::Closed)
        })
    }
    fn close(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async {
            if self.outgoing.lock().unwrap().take().is_some() {
                let _ = self.events.send(TransportEvent::Closed);
            }
            Ok(())
        })
    }
}
impl Drop for MemoryPeer {
    fn drop(&mut self) {
        let _ = self.outgoing.send(TransportEvent::Closed);
    }
}
