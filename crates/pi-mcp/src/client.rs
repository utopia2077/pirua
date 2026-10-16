use crate::{
    transport::{Transport, TransportEvent},
    *,
};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicU8, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::sync::{broadcast, mpsc, oneshot};

#[derive(Clone, Debug)]
pub struct ClientOptions {
    pub implementation: Implementation,
    pub capabilities: Object,
    pub protocol_version: String,
    pub request_timeout: Duration,
    /// `Some([])` advertises roots support with an empty list.
    pub roots: Option<Vec<Root>>,
}
impl ClientOptions {
    pub fn new(name: impl Into<String>, version: impl Into<String>) -> Self {
        Self {
            implementation: Implementation {
                name: name.into(),
                version: version.into(),
                title: None,
            },
            capabilities: Object::new(),
            protocol_version: LATEST_PROTOCOL_VERSION.into(),
            request_timeout: Duration::from_secs(30),
            roots: None,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct RequestOptions {
    pub cancellation: CancellationToken,
    /// Zero disables the request timer. Progress renews the inactivity timeout.
    pub timeout: Option<Duration>,
    pub progress: Option<mpsc::UnboundedSender<ProgressNotification>>,
}
#[derive(Clone, Debug)]
pub enum ClientEvent {
    Notification { method: String, params: Value },
    Error(Error),
    Closed,
}
pub type RequestHandler =
    Arc<dyn Fn(Value, CancellationToken) -> BoxFuture<'static, Result<Value>> + Send + Sync>;

struct Pending {
    result: oneshot::Sender<Result<Value>>,
    progress: Option<mpsc::UnboundedSender<ProgressNotification>>,
    renew: mpsc::UnboundedSender<()>,
    dispatch: CancellationToken,
}
struct Inner {
    transport: Arc<dyn Transport>,
    options: ClientOptions,
    // 0 connecting, 1 connected, 2 closed. A client/transport is single-use.
    state: AtomicU8,
    next_id: AtomicU64,
    pending: Mutex<HashMap<JsonRpcId, Pending>>,
    incoming: Mutex<HashMap<JsonRpcId, CancellationToken>>,
    handlers: Mutex<HashMap<String, RequestHandler>>,
    initialized: Mutex<Option<InitializeResult>>,
    events: broadcast::Sender<ClientEvent>,
}
impl Inner {
    fn finish(&self, id: &JsonRpcId, value: Result<Value>) {
        if let Some(pending) = self.pending.lock().unwrap().remove(id) {
            pending.dispatch.cancel();
            let _ = pending.result.send(value);
        } else {
            let _ = self.events.send(ClientEvent::Error(Error::invalid(
                "response for unknown or settled request",
            )));
        }
    }
    fn mark_closed(&self) {
        // Admission and closure share this lock. Once closed is published,
        // no new incoming token can appear after the cancellation drain.
        let mut incoming = self.incoming.lock().unwrap();
        if self.state.swap(2, Ordering::AcqRel) == 2 {
            return;
        }
        for (_, pending) in self.pending.lock().unwrap().drain() {
            pending.dispatch.cancel();
            let _ = pending.result.send(Err(Error::Closed));
        }
        for (_, token) in incoming.drain() {
            token.cancel();
        }
        let _ = self.events.send(ClientEvent::Closed);
    }
    fn handle(self: &Arc<Self>, message: Value) {
        if self.state.load(Ordering::Acquire) == 2 {
            return;
        }
        if let Err(error) = validate_message(&message) {
            let _ = self.events.send(ClientEvent::Error(error));
            return;
        }
        let id = message
            .get("id")
            .map(|id| serde_json::from_value::<JsonRpcId>(id.clone()).unwrap());
        let method = message.get("method").and_then(Value::as_str);
        match (method, id) {
            (None, Some(id)) => {
                let result = match message.get("error") {
                    Some(error) => Err(Error::Protocol {
                        code: error["code"].as_i64().unwrap(),
                        message: error["message"].as_str().unwrap().into(),
                        data: error.get("data").cloned(),
                    }),
                    None => Ok(message["result"].clone()),
                };
                self.finish(&id, result);
            }
            (Some(method), None) => {
                let params = message.get("params").cloned().unwrap_or(Value::Null);
                if method == "notifications/progress" {
                    if let Ok(progress) =
                        serde_json::from_value::<ProgressNotification>(params.clone())
                        && let Some(pending) =
                            self.pending.lock().unwrap().get(&progress.progress_token)
                        && let Some(sender) = &pending.progress
                    {
                        let _ = pending.renew.send(());
                        let _ = sender.send(progress);
                    }
                } else if method == "notifications/cancelled"
                    && let Ok(id) = serde_json::from_value::<JsonRpcId>(params["requestId"].clone())
                    && let Some(token) = self.incoming.lock().unwrap().get(&id)
                {
                    token.cancel();
                }
                let _ = self.events.send(ClientEvent::Notification {
                    method: method.into(),
                    params,
                });
            }
            (Some(method), Some(id)) => {
                let handler = self.handlers.lock().unwrap().get(method).cloned();
                let token = CancellationToken::new();
                {
                    let mut incoming = self.incoming.lock().unwrap();
                    if self.state.load(Ordering::Acquire) == 2 {
                        return;
                    }
                    if incoming.contains_key(&id) {
                        let _ = self.events.send(ClientEvent::Error(Error::invalid(
                            "duplicate incoming request ID",
                        )));
                        return;
                    }
                    incoming.insert(id.clone(), token.clone());
                }
                let inner = Arc::clone(self);
                let method = method.to_owned();
                tokio::spawn(async move {
                    // Admission may precede close while this task's first poll
                    // follows it. Do not construct or poll that host handler.
                    if token.is_cancelled() || inner.state.load(Ordering::Acquire) == 2 {
                        inner.incoming.lock().unwrap().remove(&id);
                        return;
                    }
                    let result = match handler {
                        Some(handler) => tokio::select! {
                            biased;
                            _ = token.cancelled() => Err(Error::Cancelled),
                            value = async { handler(message.get("params").cloned().unwrap_or(Value::Null), token.clone()).await } => value,
                        },
                        None => Err(Error::Protocol {
                            code: -32601,
                            message: format!("Method not found: {method}"),
                            data: None,
                        }),
                    };
                    inner.incoming.lock().unwrap().remove(&id);
                    if token.is_cancelled() || inner.state.load(Ordering::Acquire) == 2 {
                        return;
                    }
                    let reply = match result {
                        Ok(result) => json!({"jsonrpc":"2.0","id":id,"result":result}),
                        Err(Error::Protocol {
                            code,
                            message,
                            data,
                        }) => {
                            json!({"jsonrpc":"2.0","id":id,"error":{"code":code,"message":message,"data":data}})
                        }
                        Err(error) => {
                            json!({"jsonrpc":"2.0","id":id,"error":{"code":-32603,"message":error.to_string()}})
                        }
                    };
                    if let Err(error) = inner.transport.send(reply, token).await {
                        let _ = inner.events.send(ClientEvent::Error(error));
                    }
                });
            }
            _ => unreachable!(),
        }
    }
}

/// Cloneable connection handle. Call `close()` to await transport shutdown.
/// Dropping the last handle also initiates shutdown when a Tokio runtime exists.
#[derive(Clone)]
pub struct McpClient(Arc<Inner>, Arc<()>);
impl McpClient {
    pub async fn connect(options: ClientOptions, transport: Arc<dyn Transport>) -> Result<Self> {
        if !SUPPORTED_PROTOCOL_VERSIONS.contains(&options.protocol_version.as_str()) {
            return Err(Error::invalid("unsupported requested protocol version"));
        }
        let (events, _) = broadcast::channel(256);
        let client = Self(
            Arc::new(Inner {
                transport: Arc::clone(&transport),
                options,
                state: AtomicU8::new(0),
                next_id: AtomicU64::new(1),
                pending: Mutex::new(HashMap::new()),
                incoming: Mutex::new(HashMap::new()),
                handlers: Mutex::new(HashMap::new()),
                initialized: Mutex::new(None),
                events,
            }),
            Arc::new(()),
        );
        client.set_request_handler("ping", Arc::new(|_, _| Box::pin(async { Ok(json!({})) })));
        if let Some(roots) = &client.0.options.roots {
            let roots = roots.clone();
            client.set_request_handler(
                "roots/list",
                Arc::new(move |_, _| {
                    let roots = roots.clone();
                    Box::pin(async move { Ok(json!({"roots":roots})) })
                }),
            );
        }
        let result = async {
            let mut incoming = transport.start().await?;
            let weak = Arc::downgrade(&client.0);
            tokio::spawn(async move {
                while let Some(event) = incoming.recv().await {
                    let Some(inner) = weak.upgrade() else { break; };
                    match event {
                        TransportEvent::Message(message) => inner.handle(message),
                        TransportEvent::Error(error) => { let _ = inner.events.send(ClientEvent::Error(error)); },
                        TransportEvent::Closed => { inner.mark_closed(); return; },
                    }
                }
                if let Some(inner) = weak.upgrade() { inner.mark_closed(); }
            });
            let mut capabilities = client.0.options.capabilities.clone();
            if client.0.options.roots.is_some() { capabilities.entry("roots").or_insert(json!({})); }
            let params = json!({"protocolVersion":client.0.options.protocol_version,"capabilities":capabilities,"clientInfo":client.0.options.implementation});
            let result = client.request_inner("initialize", Some(params), RequestOptions::default(), true).await?;
            let result: InitializeResult = serde_json::from_value(result)?;
            if !SUPPORTED_PROTOCOL_VERSIONS.contains(&result.protocol_version.as_str()) { return Err(Error::invalid(format!("server selected unsupported version {}", result.protocol_version))); }
            transport.set_protocol_version(&result.protocol_version);
            transport.send(json!({"jsonrpc":"2.0","method":"notifications/initialized"}), CancellationToken::new()).await?;
            *client.0.initialized.lock().unwrap() = Some(result);
            client.0.state.compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire).map_err(|_| Error::Closed)?;
            Ok(())
        }.await;
        if let Err(error) = result {
            let _ = client.close().await;
            return Err(error);
        }
        Ok(client)
    }
    pub fn initialization(&self) -> InitializeResult {
        self.0.initialized.lock().unwrap().as_ref().unwrap().clone()
    }
    pub fn is_connected(&self) -> bool {
        self.0.state.load(Ordering::Acquire) == 1
    }
    pub fn subscribe(&self) -> broadcast::Receiver<ClientEvent> {
        self.0.events.subscribe()
    }
    pub fn set_request_handler(&self, method: impl Into<String>, handler: RequestHandler) {
        self.0
            .handlers
            .lock()
            .unwrap()
            .insert(method.into(), handler);
    }
    pub fn remove_request_handler(&self, method: &str) {
        self.0.handlers.lock().unwrap().remove(method);
    }
    pub async fn close(&self) -> Result<()> {
        self.0.mark_closed();
        self.0.transport.close().await
    }
    pub async fn request(
        &self,
        method: &str,
        params: Option<Value>,
        options: RequestOptions,
    ) -> Result<Value> {
        self.request_inner(method, params, options, false).await
    }
    async fn request_inner(
        &self,
        method: &str,
        mut params: Option<Value>,
        options: RequestOptions,
        connecting: bool,
    ) -> Result<Value> {
        let state = self.0.state.load(Ordering::Acquire);
        if state != 1 && !(connecting && state == 0) {
            return Err(Error::Closed);
        }
        if options.cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        if params.as_ref().is_some_and(|value| !value.is_object()) {
            return Err(Error::invalid("request params must be an object"));
        }
        let id = JsonRpcId::from(self.0.next_id.fetch_add(1, Ordering::Relaxed));
        if options.progress.is_some() {
            let value = params.get_or_insert_with(|| json!({}));
            if !value["_meta"].is_object() {
                value["_meta"] = json!({});
            }
            value["_meta"]["progressToken"] = serde_json::to_value(&id)?;
        }
        let mut message = json!({"jsonrpc":"2.0","id":id,"method":method});
        if let Some(params) = params {
            message["params"] = params;
        }
        let (sender, mut result) = oneshot::channel();
        let (renew, mut renewed) = mpsc::unbounded_channel();
        let dispatch = CancellationToken::new();
        self.0.pending.lock().unwrap().insert(
            id.clone(),
            Pending {
                result: sender,
                progress: options.progress,
                renew,
                dispatch: dispatch.clone(),
            },
        );
        let mut guard = PendingGuard {
            inner: Arc::downgrade(&self.0),
            id: id.clone(),
            cancellable: method != "initialize",
            armed: true,
        };
        // Closure may race insertion. Close must still own all outstanding requests.
        if self.0.state.load(Ordering::Acquire) == 2 {
            self.0.finish(&id, Err(Error::Closed));
        }
        let weak = Arc::downgrade(&self.0);
        tokio::spawn(async move {
            if let Some(inner) = weak.upgrade()
                && let Err(error) = inner.transport.send(message, dispatch).await
            {
                // A settlement may have canceled the send; do not emit a spurious
                // unknown-response error for that expected send termination.
                let pending = inner.pending.lock().unwrap().remove(&id);
                if let Some(pending) = pending {
                    pending.dispatch.cancel();
                    let _ = pending.result.send(Err(error));
                }
            }
        });
        let timeout = options.timeout.unwrap_or(self.0.options.request_timeout);
        let timer = tokio::time::sleep(timeout);
        tokio::pin!(timer);
        let value = loop {
            tokio::select! {
                biased;
                value = &mut result => break value.unwrap_or(Err(Error::Closed)),
                _ = options.cancellation.cancelled() => break Err(Error::Cancelled),
                _ = &mut timer, if !timeout.is_zero() => break Err(Error::Timeout(timeout)),
                Some(()) = renewed.recv() => timer.as_mut().reset(tokio::time::Instant::now() + timeout),
            }
        };
        if !matches!(value, Err(Error::Cancelled | Error::Timeout(_))) {
            guard.armed = false;
        }
        value
    }
    pub async fn notify(&self, method: &str, params: Option<Value>) -> Result<()> {
        if !self.is_connected() {
            return Err(Error::Closed);
        }
        let mut message = json!({"jsonrpc":"2.0","method":method});
        if let Some(params) = params {
            message["params"] = params;
        }
        self.0
            .transport
            .send(message, CancellationToken::new())
            .await
    }
    pub async fn ping(&self, options: RequestOptions) -> Result<()> {
        self.request("ping", None, options).await.map(|_| ())
    }
    pub async fn call_tool(
        &self,
        name: &str,
        arguments: Value,
        options: RequestOptions,
    ) -> Result<CallToolResult> {
        if !arguments.is_object() {
            return Err(Error::invalid("tool arguments must be an object"));
        }
        Ok(serde_json::from_value(
            self.request(
                "tools/call",
                Some(json!({"name":name,"arguments":arguments})),
                options,
            )
            .await?,
        )?)
    }
    pub async fn list_tools(&self, options: RequestOptions) -> Result<Vec<Tool>> {
        self.list_all("tools/list", "tools", options).await
    }
    pub async fn list_resources(&self, options: RequestOptions) -> Result<Vec<Resource>> {
        self.list_all("resources/list", "resources", options).await
    }
    pub async fn list_resource_templates(
        &self,
        options: RequestOptions,
    ) -> Result<Vec<ResourceTemplate>> {
        self.list_all("resources/templates/list", "resourceTemplates", options)
            .await
    }
    pub async fn list_resources_page(
        &self,
        cursor: Option<&str>,
        options: RequestOptions,
    ) -> Result<ListPage<Resource>> {
        self.list_page("resources/list", "resources", cursor, options)
            .await
    }
    pub async fn list_resource_templates_page(
        &self,
        cursor: Option<&str>,
        options: RequestOptions,
    ) -> Result<ListPage<ResourceTemplate>> {
        self.list_page(
            "resources/templates/list",
            "resourceTemplates",
            cursor,
            options,
        )
        .await
    }
    async fn list_all<T: DeserializeOwned>(
        &self,
        method: &str,
        key: &str,
        options: RequestOptions,
    ) -> Result<Vec<T>> {
        let mut cursor = None;
        let mut seen = HashSet::new();
        let mut result = Vec::new();
        for _ in 0..1000 {
            let page = self
                .list_page(method, key, cursor.as_deref(), options.clone())
                .await?;
            result.extend(page.items);
            let Some(next) = page.next_cursor else {
                return Ok(result);
            };
            if !seen.insert(next.clone()) {
                return Err(Error::invalid("repeated pagination cursor"));
            }
            cursor = Some(next);
        }
        Err(Error::invalid("pagination exceeds 1000 pages"))
    }
    async fn list_page<T: DeserializeOwned>(
        &self,
        method: &str,
        key: &str,
        cursor: Option<&str>,
        options: RequestOptions,
    ) -> Result<ListPage<T>> {
        let value = self
            .request(
                method,
                cursor.map(|cursor| json!({"cursor":cursor})),
                options,
            )
            .await?;
        let mut items = value
            .get(key)
            .and_then(Value::as_array)
            .cloned()
            .ok_or_else(|| Error::invalid(format!("{method} result missing {key}")))?;
        if key == "resources" || key == "resourceTemplates" {
            let uri = if key == "resources" {
                "uri"
            } else {
                "uriTemplate"
            };
            for item in &mut items {
                if !item.is_object() {
                    return Err(Error::invalid("list item must be an object"));
                }
                if item.get("name").is_none() {
                    item["name"] = item[uri].clone();
                }
            }
        }
        let cursor = match value.get("nextCursor") {
            None | Some(Value::Null) => None,
            Some(Value::String(value)) if value.is_empty() => None,
            Some(Value::String(value)) => Some(value.clone()),
            _ => return Err(Error::invalid("pagination cursor must be a string")),
        };
        Ok(ListPage {
            items: serde_json::from_value(Value::Array(items))?,
            next_cursor: cursor,
        })
    }
    pub async fn read_resource(
        &self,
        uri: &str,
        options: RequestOptions,
    ) -> Result<ReadResourceResult> {
        let result: ReadResourceResult = serde_json::from_value(
            self.request("resources/read", Some(json!({"uri":uri})), options)
                .await?,
        )?;
        if result
            .contents
            .iter()
            .any(|item| item.text.is_none() && item.blob.is_none())
        {
            return Err(Error::invalid("resource must have text or blob"));
        }
        Ok(result)
    }
}
impl Drop for McpClient {
    fn drop(&mut self) {
        // Count public handles independently of background work holding Inner.
        if Arc::strong_count(&self.1) == 1 {
            self.0.mark_closed();
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                let transport = self.0.transport.clone();
                runtime.spawn(async move {
                    let _ = transport.close().await;
                });
            }
        }
    }
}
struct PendingGuard {
    inner: Weak<Inner>,
    id: JsonRpcId,
    cancellable: bool,
    armed: bool,
}
impl Drop for PendingGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let Some(inner) = self.inner.upgrade() else {
            return;
        };
        if let Some(pending) = inner.pending.lock().unwrap().remove(&self.id) {
            pending.dispatch.cancel();
            if self.cancellable
                && inner.state.load(Ordering::Acquire) != 2
                && let Ok(runtime) = tokio::runtime::Handle::try_current()
            {
                let transport = inner.transport.clone();
                let id = self.id.clone();
                runtime.spawn(async move { let _ = transport.send(json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":id,"reason":"Request cancelled or timed out"}}), CancellationToken::new()).await; });
            }
        }
    }
}
