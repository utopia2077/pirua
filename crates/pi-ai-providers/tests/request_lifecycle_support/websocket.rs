use super::*;
use std::sync::atomic::AtomicBool;

#[derive(Default)]
pub struct WsPeer {
    pub connects: AtomicUsize,
    pub sent: Mutex<Vec<Value>>,
    pub closed: AtomicUsize,
    /// First exchange produces the pinned pre-start limit error.
    pub retry_first: AtomicBool,
    /// First exchange fails before start, permitting the existing SSE fallback.
    pub fail_first: AtomicBool,
    pub hold: AtomicBool,
}
struct Connector(Arc<WsPeer>);
impl WsPeer {
    pub fn connector(self: &Arc<Self>) -> Arc<dyn PersistentWebSocketConnector> {
        Arc::new(Connector(self.clone()))
    }
}
impl PersistentWebSocketConnector for Connector {
    fn connect<'a>(
        &'a self,
        _: &'a WebSocketRequest,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<Box<dyn PersistentWebSocket>, TransportError>> {
        Box::pin(async move {
            self.0.connects.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new(Socket {
                peer: self.0.clone(),
                closed: false,
            }) as Box<dyn PersistentWebSocket>)
        })
    }
}
struct Socket {
    peer: Arc<WsPeer>,
    closed: bool,
}
impl Socket {
    fn close(&mut self) {
        if !self.closed {
            self.closed = true;
            self.peer.closed.fetch_add(1, Ordering::SeqCst);
        }
    }
}
impl Drop for Socket {
    fn drop(&mut self) {
        self.close();
    }
}
impl PersistentWebSocket for Socket {
    fn state(&self) -> WebSocketState {
        if self.closed {
            WebSocketState::Closed
        } else {
            WebSocketState::Open
        }
    }
    fn start_exchange<'a>(
        &'a mut self,
        message: WebSocketMessage,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<Box<dyn WebSocketExchange>, TransportError>> {
        Box::pin(async move {
            let WebSocketMessage::Text(raw) = message else {
                panic!("text")
            };
            let value = serde_json::from_str(&raw).unwrap();
            self.peer.sent.lock().unwrap().push(value);
            let item = if self.peer.fail_first.swap(false, Ordering::SeqCst) {
                Err(TransportError::new(TransportErrorKind::Connection))
            } else if self.peer.retry_first.swap(false, Ordering::SeqCst) {
                Ok(
                    json!({"type":"error","code":"websocket_connection_limit_reached","message":"retry"}),
                )
            } else {
                let index = self.peer.sent.lock().unwrap().len();
                Ok(
                    json!({"type":"response.completed","response":{"id":format!("r{index}"),"status":"completed","output":[],"usage":{"input_tokens":17,"output_tokens":0,"total_tokens":17}}}),
                )
            };
            Ok(Box::new(Exchange {
                item: Some(item),
                hold: self.peer.hold.load(Ordering::SeqCst),
            }) as Box<dyn WebSocketExchange>)
        })
    }
    fn shutdown<'a>(
        &'a mut self,
        _: u16,
        _: String,
        _: &'a RequestRuntime,
        _: Duration,
    ) -> BoxFuture<'a, Result<(), TransportError>> {
        self.close();
        Box::pin(async { Ok(()) })
    }
    fn abort(&mut self) {
        self.close();
    }
}
struct Exchange {
    item: Option<Result<Value, TransportError>>,
    hold: bool,
}
impl WebSocketExchange for Exchange {
    fn next<'a>(
        &'a mut self,
        _: &'a RequestRuntime,
        _: Option<Duration>,
    ) -> BoxFuture<'a, Result<Option<WebSocketEvent>, TransportError>> {
        if self.hold {
            return Box::pin(pending());
        }
        let item = self.item.take();
        Box::pin(async move {
            item.map(|v| v.map(|v| WebSocketEvent::Message(WebSocketMessage::Text(v.to_string()))))
                .transpose()
        })
    }
}
