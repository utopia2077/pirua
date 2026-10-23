//! One connection actor, owned session drivers and one stdout writer.
use crate::{
    composition::{AppError, Application},
    projection::{self, Projection},
    protocol::*,
};
use futures_util::future::join_all;
use pi_ai::{
    AuthEvent, AuthInteraction, AuthInteractionError, AuthPrompt, BoxFuture, CancellationToken,
};
use pi_coding_runtime::*;
use pi_coding_session::{EntryContent, SessionManager};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader},
    sync::{mpsc, oneshot},
    task::JoinSet,
};
type Reply = oneshot::Sender<RpcResult>;
#[derive(Clone)]
struct Output {
    sender: mpsc::UnboundedSender<Value>,
    queued: Arc<std::sync::atomic::AtomicUsize>,
    closed: CancellationToken,
}
impl Output {
    fn send(&self, value: Value) -> Result<(), mpsc::error::SendError<Value>> {
        use std::sync::atomic::Ordering;
        if self.closed.is_cancelled() {
            return Err(mpsc::error::SendError(value));
        }
        if self.queued.fetch_add(1, Ordering::AcqRel) >= 4096 {
            self.queued.fetch_sub(1, Ordering::AcqRel);
            self.closed.cancel();
            return Err(mpsc::error::SendError(value));
        }
        match self.sender.send(value) {
            Ok(()) => Ok(()),
            Err(e) => {
                self.queued.fetch_sub(1, Ordering::AcqRel);
                self.closed.cancel();
                Err(e)
            }
        }
    }
}
type Reverse = Arc<Mutex<BTreeMap<String, oneshot::Sender<Value>>>>;

struct Call {
    id: Value,
    method: String,
    params: Value,
    reply: Reply,
}
enum Event {
    Call(Call),
    Ready(Box<Event>),
    History(String, HistoryChange),
    Runtime(String, SessionEvent),
    Input(String, String, Option<String>, InputResolution),
    Driver(String, Result<OperationResult, String>),
    Queue(
        String,
        String,
        Result<pi_coding_runtime::QueueAdmission, RpcError>,
        Reply,
    ),
    Cleared(
        String,
        Result<Vec<pi_coding_runtime::QueuedInput>, String>,
        Option<Reply>,
    ),
    Shutdown(oneshot::Sender<()>),
}
enum OperationResult {
    Prompt,
    Compact,
    Mutation(Value),
    Fork(CodingSession, LoadedResources, Box<NavigationResult>),
    Bash(BashResult),
}
struct Pending {
    session_id: String,
    cancel: InputCancellation,
    reply: Reply,
}
struct Owner {
    session: CodingSession,
    history: Arc<Mutex<HistorySubscription>>,
    projection: Projection,
    subscription: u64,
    busy: bool,
    cancelling: bool,
    closing: bool,
    pending_inputs: usize,
    pending_queue: usize,
    needs_rebuild: bool,
    queue_cancellations: BTreeMap<String, CancellationToken>,
    operation_reply: Option<Reply>,
    operation_request: Option<String>,
    cancel_replies: Vec<Reply>,
    close_replies: Vec<Reply>,
    admission: Arc<tokio::sync::Mutex<()>>,
}
struct Host {
    application: Arc<Application>,
    contracts: Contracts,
    tx: mpsc::UnboundedSender<Event>,
    rx: mpsc::UnboundedReceiver<Event>,
    output: Output,
    reverse: Reverse,
    initialized: bool,
    pi: bool,
    elicitation: bool,
    owners: BTreeMap<String, Owner>,
    pending: BTreeMap<String, Pending>,
    tasks: JoinSet<()>,
    auth: BTreeMap<String, CancellationToken>,
}

pub async fn serve<R, W>(application: Application, input: R, output: W) -> Result<(), AppError>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let (writer_sender, mut writer_rx) = mpsc::unbounded_channel::<Value>();
    let queued = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let closed = CancellationToken::new();
    let writer_tx = Output {
        sender: writer_sender,
        queued: queued.clone(),
        closed: closed.clone(),
    };
    let writer_closed = closed.clone();
    let writer = tokio::spawn(async move {
        let result = async move {
            let mut output = output;
            while let Some(value) = writer_rx.recv().await {
                queued.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
                let mut line = serde_json::to_vec(&value)?;
                line.push(b'\n');
                tokio::time::timeout(std::time::Duration::from_secs(5), async {
                    output.write_all(&line).await?;
                    output.flush().await
                })
                .await
                .map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::TimedOut, "Protocol output stalled")
                })??;
            }
            Ok::<_, AppError>(())
        }
        .await;
        if result.is_err() {
            writer_closed.cancel();
        }
        result
    });
    let (tx, rx) = mpsc::unbounded_channel();
    let reverse = Arc::new(Mutex::new(BTreeMap::new()));
    let host = Host {
        application: Arc::new(application),
        contracts: Contracts::new(),
        tx: tx.clone(),
        rx,
        output: writer_tx.clone(),
        reverse: reverse.clone(),
        initialized: false,
        pi: false,
        elicitation: false,
        owners: BTreeMap::new(),
        pending: BTreeMap::new(),
        tasks: JoinSet::new(),
        auth: BTreeMap::new(),
    };
    let actor = tokio::spawn(host.run());
    let mut lines = BufReader::new(input).lines();
    let mut responses = JoinSet::new();
    loop {
        let line = tokio::select! {line=lines.next_line()=>line?,_=tokio::signal::ctrl_c()=>None,_=closed.cancelled()=>None};
        let Some(line) = line else { break };
        match serde_json::from_str::<Value>(&line) {
            Err(_) => {
                let _ = writer_tx.send(response(
                    Value::Null,
                    Err(RpcError::new(-32700, "Parse error")),
                ));
            }
            Ok(Value::Array(items)) => {
                if items.is_empty() {
                    let _ = writer_tx.send(response(
                        Value::Null,
                        Err(RpcError::new(-32600, "Empty batch")),
                    ));
                    continue;
                }
                let waits = items
                    .into_iter()
                    .filter_map(|item| route(item, &tx, &reverse))
                    .collect::<Vec<_>>();
                if !waits.is_empty() {
                    let output = writer_tx.clone();
                    responses.spawn(async move {
                        let values = join_all(waits).await;
                        let _ = output.send(Value::Array(values));
                    });
                }
            }
            Ok(value) => {
                if let Some(wait) = route(value, &tx, &reverse) {
                    let output = writer_tx.clone();
                    responses.spawn(async move {
                        let _ = output.send(wait.await);
                    });
                }
            }
        }
        while responses.try_join_next().is_some() {}
    }
    let (done, wait) = oneshot::channel();
    let _ = tx.send(Event::Shutdown(done));
    let _ = wait.await;
    let _ = actor.await;
    while responses.join_next().await.is_some() {}
    reverse.lock().unwrap().clear();
    drop(tx);
    drop(writer_tx);
    writer.await??;
    Ok(())
}
fn route(
    value: Value,
    tx: &mpsc::UnboundedSender<Event>,
    reverse: &Reverse,
) -> Option<BoxFuture<'static, Value>> {
    if value.is_object()
        && value.get("method").is_none()
        && value.get("id").is_some()
        && (value.get("result").is_some() || value.get("error").is_some())
    {
        if let Some(wait) = reverse.lock().unwrap().remove(&value["id"].to_string()) {
            let _ = wait.send(value);
        }
        return None;
    }
    let valid = value.is_object()
        && value["jsonrpc"] == "2.0"
        && value["method"].is_string()
        && value
            .get("params")
            .is_none_or(|v| v.is_object() || v.is_array())
        && value
            .get("id")
            .is_none_or(|v| v.is_null() || v.is_string() || v.is_number());
    if !valid {
        return Some(Box::pin(async move {
            response(Value::Null, Err(RpcError::new(-32600, "Invalid Request")))
        }));
    }
    let id = value.get("id").cloned();
    let method = value["method"].as_str().unwrap().to_owned();
    let params = value.get("params").cloned().unwrap_or_else(|| json!({}));
    let (reply, wait) = oneshot::channel();
    let _ = tx.send(Event::Call(Call {
        id: id.clone().unwrap_or(Value::Null),
        method,
        params,
        reply,
    }));
    id.map(|id| {
        Box::pin(async move {
            response(
                id,
                wait.await.unwrap_or_else(|_| {
                    Err(RpcError::new(-32603, "Application connection closed"))
                }),
            )
        }) as BoxFuture<'static, Value>
    })
}
fn barrier_session(event: &Event) -> Option<&str> {
    match event {
        Event::Call(call) => call.params.get("sessionId").and_then(Value::as_str),
        Event::Input(id, ..)
        | Event::Driver(id, ..)
        | Event::Queue(id, ..)
        | Event::Cleared(id, ..) => Some(id),
        _ => None,
    }
}
fn observe(
    tx: &mpsc::UnboundedSender<Event>,
    id: &str,
    history: &Arc<Mutex<HistorySubscription>>,
    event: Event,
) {
    let mut history = history.lock().unwrap_or_else(|e| e.into_inner());
    while let Ok(change) = history.changes.try_recv() {
        let _ = tx.send(Event::History(id.into(), change));
    }
    let _ = tx.send(event);
}
impl Host {
    async fn run(mut self) {
        while let Some(event) = self.rx.recv().await {
            let event = match event {
                Event::Ready(event) => *event,
                event => {
                    if let Some(id) = barrier_session(&event).map(str::to_owned)
                        && let Some(owner) = self.owners.get(&id)
                    {
                        let history = owner.history.clone();
                        observe(&self.tx, &id, &history, Event::Ready(Box::new(event)));
                        continue;
                    }
                    event
                }
            };
            match event {
                Event::Ready(_) => unreachable!("one observation barrier"),
                Event::History(id, change) => self.history(&id, change),
                Event::Call(call) => self.call(call).await,
                Event::Runtime(id, event) => {
                    if let Some(owner) = self.owners.get_mut(&id) {
                        let before = owner.projection.activity.clone();
                        let updates = owner.projection.event(event);
                        for update in updates {
                            let _ = self
                                .output
                                .send(owner.projection.standard(&id, update, self.pi));
                        }
                        if self.pi && owner.projection.activity != before {
                            let activity = owner.projection.activity.clone();
                            let _ = self.output.send(owner.projection.changed(
                                &id,
                                json!({"activity":activity}),
                                None,
                                None,
                            ));
                        }
                    }
                }
                Event::Input(id, submission, request, resolution) => {
                    self.input(id, submission, request, resolution)
                }
                Event::Driver(id, result) => self.finished(&id, result).await,
                Event::Queue(id, request, result, reply) => {
                    self.queue_finished(&id, &request, result, reply)
                }
                Event::Cleared(id, result, reply) => {
                    if let Some(owner) = self.owners.get_mut(&id) {
                        match result {
                            Ok(removed) => {
                                if let Some(reply) = reply {
                                    let _=reply.send(Ok(json!({"removed":removed.iter().map(projection::queued).collect::<Vec<_>>()})));
                                }
                            }
                            Err(error) => {
                                if let Some(reply) = reply {
                                    let _ = reply.send(Err(RpcError::failed(error)));
                                }
                            }
                        }
                        let _ = owner;
                        self.publish_fields(&id, &["queue"]);
                        self.maybe_close(&id).await;
                    }
                }
                Event::Shutdown(done) => {
                    for pending in self.pending.values() {
                        pending.cancel.cancel();
                    }
                    for owner in self.owners.values() {
                        owner.session.abort();
                        owner.session.abort_bash();
                        for token in owner.queue_cancellations.values() {
                            token.cancel();
                        }
                    }
                    for token in self.auth.values() {
                        token.cancel();
                    }
                    for owner in self.owners.values() {
                        let _ = owner.session.clear_queue_tracked().await;
                        let _ = owner.session.dispose().await;
                    }
                    self.reverse.lock().unwrap().clear();
                    while self.tasks.join_next().await.is_some() {}
                    self.owners.clear();
                    self.pending.clear();
                    let _ = done.send(());
                    break;
                }
            }
            let ids = self.owners.keys().cloned().collect::<Vec<_>>();
            for id in ids {
                self.maybe_close(&id).await;
                self.continue_queue(&id);
            }
            while self.tasks.try_join_next().is_some() {}
        }
    }
    fn history(&mut self, id: &str, change: HistoryChange) {
        let Some(owner) = self.owners.get_mut(id) else {
            return;
        };
        let mut updates = Vec::new();
        let history_change = match change {
            HistoryChange::Committed {
                session_id: _,
                base_revision,
                revision,
                appended,
                leaf_id,
            } => {
                for entry in &appended {
                    if let EntryContent::Message { message } = &entry.content {
                        if message["role"] == "user" {
                            updates.extend(projection::message_updates(
                                &entry.id,
                                message,
                                "end",
                                Some(&entry.id),
                            ));
                        } else if message["role"] == "assistant"
                            && !owner.projection.assistant_id.is_empty()
                        {
                            let native = owner.projection.assistant_id.clone();
                            updates.extend(projection::message_updates(
                                &native,
                                message,
                                "end",
                                Some(&entry.id),
                            ));
                        }
                    }
                }
                owner.projection.history.entries.extend(appended.clone());
                owner.projection.history.revision = revision;
                owner.projection.history.leaf_id = leaf_id.clone();
                json!({"kind":"append","baseRevision":base_revision,"revision":revision,"appended":appended,"leafId":leaf_id})
            }
            HistoryChange::Replaced { snapshot } => {
                owner.projection.history = snapshot.clone();
                json!({"kind":"replace","history":snapshot})
            }
        };
        for update in updates {
            let _ = self
                .output
                .send(owner.projection.standard(id, update, self.pi));
        }
        if self.pi {
            let context = pi_coding_session::project(&owner.projection.history)
                .expect("observed canonical history");
            let _ = self.output.send(owner.projection.changed(
                id,
                json!({"messages":context.messages,"contextEntries":context.entries}),
                Some(history_change),
                None,
            ));
        }
    }
    async fn register(
        &mut self,
        session: CodingSession,
        loaded: LoadedResources,
    ) -> Result<String, RpcError> {
        let observed = session.observe_history().map_err(RpcError::failed)?;
        let initial = observed.snapshot.clone();
        let history = Arc::new(Mutex::new(observed));
        let callback_history = history.clone();
        let id = session.session_id();
        let tx = self.tx.clone();
        let event_id = id.clone();
        let subscription = session.subscribe(Arc::new(move |event| {
            observe(
                &tx,
                &event_id,
                &callback_history,
                Event::Runtime(event_id.clone(), event),
            );
            Box::pin(async { Ok(()) })
        }));
        let mut settings = projection::display_settings(&self.application.settings);
        settings["thinkingLevel"] = json!(session.thinking_level());
        settings["activeTools"] = json!(session.active_tool_names());
        settings["compaction"]["enabled"] = json!(session.auto_compaction_enabled());
        settings["retry"]["enabled"] = json!(session.auto_retry_enabled());
        settings["steeringMode"] = json!(session.agent().steering_mode());
        settings["followUpMode"] = json!(session.agent().follow_up_mode());
        let mut projection = Projection::new(initial, &loaded, settings);
        projection.view = Projection::capture_view(&session, &self.application.models);
        self.owners.insert(
            id.clone(),
            Owner {
                session,
                history,
                projection,
                subscription,
                busy: false,
                cancelling: false,
                closing: false,
                pending_inputs: 0,
                pending_queue: 0,
                needs_rebuild: false,
                queue_cancellations: BTreeMap::new(),
                operation_reply: None,
                operation_request: None,
                cancel_replies: vec![],
                close_replies: vec![],
                admission: Arc::new(tokio::sync::Mutex::new(())),
            },
        );
        Ok(id)
    }
    fn publish_fields(&mut self, id: &str, fields: &[&str]) {
        if !self.pi {
            return;
        }
        if let Some(owner) = self.owners.get_mut(id) {
            let captured = Projection::capture_view(&owner.session, &self.application.models);
            for key in fields {
                if let Some(value) = captured.get(*key) {
                    owner.projection.view[*key] = value.clone();
                }
            }
            let snapshot = owner
                .projection
                .snapshot(&owner.session, &self.application.models);
            let changes = fields
                .iter()
                .map(|key| ((*key).to_owned(), snapshot[*key].clone()))
                .collect::<serde_json::Map<_, _>>();
            let _ = self.output.send(owner.projection.changed(
                id,
                Value::Object(changes),
                None,
                None,
            ));
        }
    }
    fn standard(&mut self, id: &str, update: Value) {
        if let Some(owner) = self.owners.get_mut(id) {
            let _ = self
                .output
                .send(owner.projection.standard(id, update, self.pi));
        }
    }
    fn set_state(&mut self, id: &str, running: bool, reason: Option<String>) {
        if let Some(owner) = self.owners.get_mut(id) {
            let update = owner.projection.state_update(running, reason);
            let _ = self
                .output
                .send(owner.projection.standard(id, update, self.pi));
        }
        self.publish_fields(id, &["state", "activity"]);
    }
    fn require_idle(&self, id: &str, params: &Value) -> Result<(), RpcError> {
        let owner = self
            .owners
            .get(id)
            .ok_or_else(|| RpcError::params("Session is not open; call session/resume"))?;
        if owner.busy
            || owner.cancelling
            || owner.closing
            || owner.pending_queue > 0
            || owner.projection.state != "idle"
        {
            return Err(RpcError::app("busy", "Session foreground work is active"));
        }
        if let Some(v) = params.get("expectedRevision") {
            let expected = v
                .as_u64()
                .ok_or_else(|| RpcError::params("expectedRevision must be an unsigned integer"))?;
            let revision = owner.session.snapshot().revision;
            if expected != revision {
                let mut e = RpcError::app("stale_revision", "Session history changed");
                e.data = Some(json!({"kind":"stale_revision","revision":revision}));
                return Err(e);
            }
        }
        Ok(())
    }
    fn own_ticket(
        &mut self,
        id: &str,
        mut ticket: InputTicket,
        request: Option<String>,
        reply: Option<Reply>,
    ) {
        let submission = ticket.id.to_string();
        let cancellation = ticket.cancellation_handle();
        if let Some(request) = &request {
            self.pending.insert(
                request.clone(),
                Pending {
                    session_id: id.to_owned(),
                    cancel: cancellation,
                    reply: reply.expect("prompt responder"),
                },
            );
        }
        if let Some(owner) = self.owners.get_mut(id) {
            owner.pending_inputs += 1;
        }
        let tx = self.tx.clone();
        let id = id.to_owned();
        self.tasks.spawn(async move {
            let resolution = (&mut ticket.resolution)
                .await
                .unwrap_or(InputResolution::Cancelled);
            let _ = tx.send(Event::Input(id, submission, request, resolution));
        });
    }
    fn start_driver(&mut self, id: &str, future: BoxFuture<'static, Result<(), SessionError>>) {
        let tx = self.tx.clone();
        let id = id.to_owned();
        self.tasks.spawn(async move {
            let result = future
                .await
                .map(|_| OperationResult::Prompt)
                .map_err(|e| e.to_string());
            let _ = tx.send(Event::Driver(id, result));
        });
    }
    async fn call(&mut self, call: Call) {
        let Call {
            id,
            method,
            params,
            reply,
        } = call;
        let key = id.to_string();
        if method == "$/cancel_request" {
            if let Err(error) = self
                .contracts
                .validate("CancelRequestNotification", &params)
            {
                let _ = reply.send(Err(error));
                return;
            }
            if let Some(request) = params.get("requestId") {
                let request = request.to_string();
                if let Some(p) = self.pending.get(&request) {
                    p.cancel.cancel();
                }
                if let Some(token) = self.auth.get(&request) {
                    token.cancel();
                }
                for owner in self.owners.values() {
                    if let Some(token) = owner.queue_cancellations.get(&request) {
                        token.cancel();
                    }
                    if owner.operation_request.as_ref() == Some(&request) {
                        owner.session.abort();
                        owner.session.abort_bash();
                    }
                }
            }
            let _ = reply.send(Ok(json!({})));
            return;
        }
        if method == "initialize" {
            let result = (|| {
                self.contracts.validate("InitializeRequest", &params)?;
                let version = params["protocolVersion"].as_u64().unwrap_or(0);
                if version < 2 {
                    let mut e = RpcError::new(-32022, "Only ACP protocol version 2 is supported");
                    e.data = Some(json!({"requested":version,"supported":[2]}));
                    return Err(e);
                }
                if self.initialized {
                    return Err(RpcError::new(-32600, "Connection is already initialized"));
                }
                self.initialized = true;
                self.pi = params["capabilities"]["_meta"][TUI]["version"] == 1;
                self.elicitation = params["capabilities"]["elicitation"]["form"].is_object();
                let methods = self.auth_methods();
                Ok(
                    json!({"protocolVersion":2,"info":{"name":"pi-acp-server","version":env!("CARGO_PKG_VERSION")},"capabilities":{"session":{"prompt":{"image":{}}},"_meta":{TUI:{"version":1,"features":["snapshot","queue","tree","shell","scopedModels","resources"]}}},"authMethods":methods}),
                )
            })();
            let _ = reply.send(result);
            return;
        }
        if !self.initialized {
            let _ = reply.send(Err(RpcError::new(
                -32600,
                "Initialize the connection before calling session methods",
            )));
            return;
        }
        let result = self.dispatch(&method, &params, &key, reply).await;
        if let Err((error, reply)) = result {
            let _ = reply.send(Err(error));
        }
    }
    async fn dispatch(
        &mut self,
        method: &str,
        params: &Value,
        key: &str,
        reply: Reply,
    ) -> Result<(), (RpcError, Reply)> {
        // Keep the responder until admission succeeds; long operations transfer it
        // into the owner, leaving the connection input reader independent.
        let mut reply = Some(reply);
        let result = self.dispatch_value(method, params, key, &mut reply).await;
        match result {
            Ok(Some(value)) => {
                let _ = reply.take().unwrap().send(Ok(value));
                Ok(())
            }
            Ok(None) => Ok(()),
            Err(error) => Err((
                error,
                reply.take().expect("failed admission retains responder"),
            )),
        }
    }
    async fn dispatch_value(
        &mut self,
        method: &str,
        params: &Value,
        key: &str,
        reply: &mut Option<Reply>,
    ) -> Result<Option<Value>, RpcError> {
        if method.starts_with("_pirua/") && !self.pi {
            return Err(RpcError::app(
                "unsupported",
                "Negotiate pirua.dev/tui version 1 first",
            ));
        }
        match method {
            "session/new" => {
                self.contracts.validate("NewSessionRequest", params)?;
                supported_lifecycle(params)?;
                let cwd = string(params, "cwd")?;
                let manager = SessionManager::create(self.application.store.clone(), cwd)
                    .map_err(RpcError::failed)?;
                let (session, loaded) = self
                    .application
                    .build(manager)
                    .await
                    .map_err(RpcError::failed)?;
                let id = self.register(session, loaded).await?;
                Ok(Some(self.setup_result(&id, true)))
            }
            "session/list" => {
                self.contracts.validate("ListSessionsRequest", params)?;
                if params.get("cursor").is_some_and(|v| !v.is_null()) {
                    return Err(RpcError::params("Pagination cursor is not supported"));
                }
                let cwd = params.get("cwd").and_then(Value::as_str);
                let sessions = self.application.store.list(cwd).map_err(RpcError::failed)?;
                Ok(Some(
                    json!({"sessions":sessions.into_iter().map(|s|json!({"sessionId":s.header.id,"cwd":s.header.cwd,"title":s.name,"updatedAt":timestamp(s.last_timestamp)})).collect::<Vec<_>>()}),
                ))
            }
            "session/resume" => {
                self.contracts.validate("ResumeSessionRequest", params)?;
                supported_lifecycle(params)?;
                if params
                    .get("replayFrom")
                    .is_some_and(|r| !r.is_null() && r["type"] != "start")
                {
                    return Err(RpcError::params("Unsupported replay cursor"));
                }
                let id = string(params, "sessionId")?.to_owned();
                let cwd = string(params, "cwd")?;
                if !self.owners.contains_key(&id) {
                    let manager = SessionManager::open(self.application.store.clone(), &id)
                        .map_err(RpcError::failed)?;
                    if manager.header().cwd != cwd {
                        return Err(RpcError::params("cwd must match the persisted session"));
                    }
                    let (session, loaded) = self
                        .application
                        .build(manager)
                        .await
                        .map_err(RpcError::failed)?;
                    self.register(session, loaded).await?;
                } else if self.owners[&id].session.snapshot().header.cwd != cwd {
                    return Err(RpcError::params("cwd must match the persisted session"));
                }
                if params["replayFrom"]["type"] == "start" {
                    let updates = projection::replay(&self.owners[&id].projection.history);
                    for update in updates {
                        self.standard(&id, update);
                    }
                }
                let owner = self.owners.get_mut(&id).unwrap();
                let mut state =
                    json!({"sessionUpdate":"state_update","state":owner.projection.state});
                if let Some(reason) = &owner.projection.stop_reason {
                    state["stopReason"] = json!(reason);
                    if reason == "error" {
                        state["error"] = RpcError::failed(
                            owner
                                .projection
                                .last_error
                                .as_deref()
                                .unwrap_or("Native session failed"),
                        )
                        .value();
                    }
                }
                self.standard(&id, state);
                Ok(Some(self.setup_result(&id, false)))
            }
            "auth/login" => {
                self.contracts.validate("LoginAuthRequest", params)?;
                let method_id = string(params, "methodId")?;
                if !self
                    .auth_methods()
                    .iter()
                    .any(|m| m["methodId"] == method_id)
                {
                    return Err(RpcError::params("Authentication method was not advertised"));
                }
                let (provider, kind) = method_id
                    .split_once(':')
                    .ok_or_else(|| RpcError::params("Invalid authentication method"))?;
                let provider = pi_ai::ProviderId::new(provider).map_err(RpcError::failed)?;
                let auth_type = if kind == "oauth" {
                    pi_ai::AuthType::OAuth
                } else {
                    pi_ai::AuthType::ApiKey
                };
                let registry = self.application.registry.clone().ok_or_else(|| {
                    RpcError::app("unsupported", "Fixture provider has no authentication")
                })?;
                let token = CancellationToken::new();
                self.auth.insert(key.into(), token.clone());
                let interaction = Interaction {
                    output: self.output.clone(),
                    reverse: self.reverse.clone(),
                    request: serde_json::from_str(key).unwrap(),
                    next: std::sync::atomic::AtomicU64::new(0),
                };
                let reply = reply.take().unwrap();
                self.tasks.spawn(async move {
                    let result = registry
                        .login(&provider, auth_type, &interaction, &token)
                        .await
                        .map(|_| json!({}))
                        .map_err(RpcError::failed);
                    let _ = reply.send(result);
                });
                Ok(None)
            }
            "auth/logout" => {
                self.contracts.validate("LogoutAuthRequest", params)?;
                if self.auth_methods().is_empty() {
                    return Err(RpcError::app(
                        "unsupported",
                        "Authentication was not advertised for this connection",
                    ));
                }
                let registry = self.application.registry.clone().ok_or_else(|| {
                    RpcError::app("unsupported", "Fixture provider has no authentication")
                })?;
                let token = CancellationToken::new();
                self.auth.insert(key.into(), token.clone());
                let reply = reply.take().unwrap();
                self.tasks.spawn(async move {
                    let mut result = Ok(json!({}));
                    for p in registry.get_providers() {
                        if let Err(e) = registry.logout(p.id(), &token).await {
                            result = Err(RpcError::failed(e));
                            break;
                        }
                    }
                    let _ = reply.send(result);
                });
                Ok(None)
            }
            _ => {
                let supported = matches!(
                    method,
                    "session/prompt"
                        | "session/cancel"
                        | "session/close"
                        | "session/set_config_option"
                        | "_pirua/session/snapshot"
                        | "_pirua/session/queue"
                        | "_pirua/session/clear_queue"
                        | "_pirua/session/navigate"
                        | "_pirua/session/fork"
                        | "_pirua/session/label"
                        | "_pirua/session/rename"
                        | "_pirua/session/bash"
                        | "_pirua/session/scoped_models"
                        | "_pirua/session/reload"
                );
                if !supported {
                    return Err(RpcError::new(-32601, format!("Unknown method {method}")));
                }
                let id = string(params, "sessionId")?.to_owned();
                if !self.owners.contains_key(&id) {
                    return Err(RpcError::params("Session is not open; call session/resume"));
                }
                match method {
                    "_pirua/session/snapshot" => Ok(Some(
                        self.owners[&id]
                            .projection
                            .snapshot(&self.owners[&id].session, &self.application.models),
                    )),
                    "session/prompt" => {
                        self.contracts.validate("PromptRequest", params)?;
                        self.require_idle(&id, params)?;
                        if let Some(registry) = &self.application.registry {
                            let model = self.owners[&id].session.model();
                            if registry
                                .check_auth(&model.provider, &CancellationToken::new())
                                .await
                                .map_err(RpcError::failed)?
                                .is_none()
                            {
                                return Err(RpcError::new(
                                    -32000,
                                    format!(
                                        "Credentials for {} are missing. Set its API key environment variable, use --credentials PATH, or authenticate with auth/login.",
                                        model.provider
                                    ),
                                ));
                            }
                        }
                        let (text, images) = content(&params["prompt"])?;
                        self.owners.get_mut(&id).unwrap().busy = true;
                        self.set_state(&id, true, None);
                        if text == "/compact" || text.starts_with("/compact ") {
                            let live = self.owners.get_mut(&id).unwrap().projection.id("command");
                            self.standard(&id,json!({"sessionUpdate":"user_message","messageId":live,"content":params["prompt"],"_meta":{RENDER:{"phase":"end","nativeMessageId":live,"message":{"role":"user","content":text,"timestamp":self.application.runtime.clock.now_unix_ms()}}}}));
                            let _ = reply.take().unwrap().send(Ok(json!({"messageId":live})));
                            let session = self.owners[&id].session.clone();
                            let tx = self.tx.clone();
                            let instructions = text.strip_prefix("/compact ").map(str::to_owned);
                            self.tasks.spawn(async move {
                                let result = session
                                    .compact(instructions)
                                    .await
                                    .map(|_| OperationResult::Compact)
                                    .map_err(|e| e.to_string());
                                let _ = tx.send(Event::Driver(id, result));
                            });
                            Ok(None)
                        } else {
                            let (ticket, driver) = self.owners[&id].session.tracked_prompt(
                                text,
                                PromptOptions {
                                    images,
                                    ..Default::default()
                                },
                            );
                            self.own_ticket(&id, ticket, Some(key.into()), reply.take());
                            self.start_driver(&id, driver);
                            Ok(None)
                        }
                    }
                    "session/cancel" | "session/close" => {
                        let contract = if method == "session/cancel" {
                            "CancelSessionNotification"
                        } else {
                            "CloseSessionRequest"
                        };
                        self.contracts.validate(contract, params)?;
                        let owner = self.owners.get_mut(&id).unwrap();
                        if method == "session/close" {
                            owner.closing = true;
                            owner.close_replies.push(reply.take().unwrap());
                        } else {
                            owner.cancel_replies.push(reply.take().unwrap());
                        }
                        if owner.busy || owner.pending_queue > 0 || owner.pending_inputs > 0 {
                            owner.cancelling = true;
                            // Runtime abort reaches installed operations. Admission can
                            // still be waiting in input hooks before that operation exists.
                            // The ticket handle is a no-op once insertion has committed.
                            for pending in self.pending.values().filter(|p| p.session_id == id) {
                                pending.cancel.cancel();
                            }
                            owner.session.abort();
                            owner.session.abort_bash();
                            for token in owner.queue_cancellations.values() {
                                token.cancel();
                            }
                            let session = owner.session.clone();
                            let tx = self.tx.clone();
                            let event_id = id.clone();
                            self.tasks.spawn(async move {
                                let result = session
                                    .clear_queue_tracked()
                                    .await
                                    .map_err(|e| e.to_string());
                                let _ = tx.send(Event::Cleared(event_id, result, None));
                            });
                        }
                        self.maybe_close(&id).await;
                        Ok(None)
                    }
                    "_pirua/session/queue" => {
                        if self.owners[&id].cancelling || self.owners[&id].closing {
                            return Err(RpcError::app("busy", "Cancellation is settling"));
                        }
                        let delivery = match string(params, "delivery")? {
                            "steer" => Delivery::Steer,
                            "followUp" => Delivery::FollowUp,
                            _ => {
                                return Err(RpcError::params("delivery must be steer or followUp"));
                            }
                        };
                        self.contracts.validate(
                            "PromptRequest",
                            &json!({"sessionId":id,"prompt":params["prompt"]}),
                        )?;
                        let (text, images) = content(&params["prompt"])?;
                        let owner = self.owners.get_mut(&id).unwrap();
                        owner.pending_queue += 1;
                        let token = CancellationToken::new();
                        owner.queue_cancellations.insert(key.into(), token.clone());
                        let session = owner.session.clone();
                        let gate = owner.admission.clone();
                        let tx = self.tx.clone();
                        let request = key.to_owned();
                        let reply = reply.take().unwrap();
                        self.tasks.spawn(async move{let admission=async{let _gate=gate.lock().await;session.queue_tracked(text,images,delivery).await.map_err(RpcError::failed)};let result=tokio::select!{biased;_=token.cancelled()=>Err(RpcError::cancelled()),result=admission=>result};let _=tx.send(Event::Queue(id,request,result,reply));});
                        Ok(None)
                    }
                    "_pirua/session/clear_queue" => {
                        let owner = &self.owners[&id];
                        let session = owner.session.clone();
                        let tx = self.tx.clone();
                        let reply = reply.take();
                        self.tasks.spawn(async move {
                            let result = session
                                .clear_queue_tracked()
                                .await
                                .map_err(|e| e.to_string());
                            let _ = tx.send(Event::Cleared(id, result, reply));
                        });
                        Ok(None)
                    }
                    "session/set_config_option" => {
                        self.contracts
                            .validate("SetSessionConfigOptionRequest", params)?;
                        self.require_idle(&id, params)?;
                        let config = string(params, "configId")?;
                        let kind = string(params, "type")?;
                        if !matches!(kind, "id" | "boolean") {
                            return Err(RpcError::app(
                                "unsupported",
                                "Unsupported configuration value type",
                            ));
                        }
                        let session = self.owners[&id].session.clone();
                        let advertised = projection::config(&session, &self.application.models);
                        let option = advertised
                            .as_array()
                            .and_then(|options| {
                                options.iter().find(|option| option["configId"] == config)
                            })
                            .ok_or_else(|| {
                                RpcError::app("unsupported", "Unsupported configuration option")
                            })?;
                        let expected_kind = match option["type"].as_str() {
                            Some("select") => "id",
                            Some("boolean") => "boolean",
                            _ => {
                                return Err(RpcError::app(
                                    "unsupported",
                                    "Unsupported configuration option kind",
                                ));
                            }
                        };
                        if kind != expected_kind {
                            return Err(RpcError::params(
                                "Configuration value type does not match the advertised option",
                            ));
                        }
                        let value = params["value"].clone();
                        if kind == "id"
                            && !option["options"].as_array().is_some_and(|options| {
                                options.iter().any(|option| option["value"] == value)
                            })
                        {
                            return Err(RpcError::params(
                                "Configuration value is not an advertised selection",
                            ));
                        }
                        let models = self.application.models.clone();
                        let config = config.to_owned();
                        let operation = async move {
                            match config.as_str() {
                                "model" => {
                                    let reference =
                                        value.as_str().ok_or("model value must be an ID")?;
                                    let model = models
                                        .into_iter()
                                        .find(|m| format!("{}/{}", m.provider, m.id) == reference)
                                        .ok_or("Unknown model selection")?;
                                    session.set_model(model).await.map_err(|e| e.to_string())?;
                                }
                                "thinkingLevel" => session
                                    .set_thinking_level(
                                        serde_json::from_value(value).map_err(|e| e.to_string())?,
                                    )
                                    .await
                                    .map_err(|e| e.to_string())?,
                                "steeringMode" => session.set_steering_mode(
                                    serde_json::from_value(value).map_err(|e| e.to_string())?,
                                ),
                                "followUpMode" => session.set_follow_up_mode(
                                    serde_json::from_value(value).map_err(|e| e.to_string())?,
                                ),
                                "autoCompaction" => session.set_auto_compaction_enabled(
                                    value.as_bool().ok_or("autoCompaction requires a boolean")?,
                                ),
                                "autoRetry" => session.set_auto_retry_enabled(
                                    value.as_bool().ok_or("autoRetry requires a boolean")?,
                                ),
                                _ => return Err("Unsupported configuration option".into()),
                            };
                            Ok(OperationResult::Mutation(json!({"configuration":true})))
                        };
                        self.mutation(&id, key, reply.take().unwrap(), Box::pin(operation));
                        Ok(None)
                    }
                    "_pirua/session/bash" => {
                        self.require_idle(&id, params)?;
                        let command = string(params, "command")?.to_owned();
                        let exclude = optional_bool(params, "excludeFromContext", false)?;
                        let owner = self.owners.get_mut(&id).unwrap();
                        let terminal = owner.projection.id("terminal");
                        owner.projection.activity["bashRunning"] = json!(true);
                        let session = owner.session.clone();
                        self.standard(&id,json!({"sessionUpdate":"terminal_update","terminalId":terminal,"command":command,"cwd":session.snapshot().header.cwd}));
                        let operation = async move {
                            session
                                .execute_bash(
                                    command,
                                    None,
                                    ExecuteBashOptions {
                                        exclude_from_context: exclude,
                                        id: Some(terminal),
                                        ..Default::default()
                                    },
                                )
                                .await
                                .map(OperationResult::Bash)
                                .map_err(|e| e.to_string())
                        };
                        self.mutation(&id, key, reply.take().unwrap(), Box::pin(operation));
                        Ok(None)
                    }
                    "_pirua/session/navigate" => {
                        self.require_idle(&id, params)?;
                        let target = string(params, "targetId")?.to_owned();
                        let options = NavigateOptions {
                            summarize: optional_bool(params, "summarize", false)?,
                            custom_instructions: params
                                .get("customInstructions")
                                .map(|v| {
                                    v.as_str().map(str::to_owned).ok_or_else(|| {
                                        RpcError::params("customInstructions must be a string")
                                    })
                                })
                                .transpose()?,
                            replace_instructions: optional_bool(
                                params,
                                "replaceInstructions",
                                false,
                            )?,
                            label: params
                                .get("label")
                                .map(|v| {
                                    v.as_str()
                                        .map(str::to_owned)
                                        .ok_or_else(|| RpcError::params("label must be a string"))
                                })
                                .transpose()?,
                        };
                        let session = self.owners[&id].session.clone();
                        self.mutation(&id,key,reply.take().unwrap(),Box::pin(async move{let r=session.navigate_tree(&target,options).await.map_err(|e|e.to_string())?;Ok(OperationResult::Mutation(json!({"sessionId":session.session_id(),"cancelled":r.cancelled,"aborted":r.aborted,"editorText":r.editor_text})))}));
                        Ok(None)
                    }
                    "_pirua/session/fork" => {
                        self.require_idle(&id, params)?;
                        let entry = string(params, "entryId")?.to_owned();
                        let position = string(params, "position")?.to_owned();
                        if position != "before" && position != "at" {
                            return Err(RpcError::params("position must be before or at"));
                        }
                        let app = self.application.clone();
                        let source = self.owners[&id].session.snapshot();
                        if !source.entries.iter().any(|e| e.id == entry) {
                            return Err(RpcError::params("Fork entry does not exist"));
                        }
                        self.mutation(
                            &id,
                            key,
                            reply.take().unwrap(),
                            Box::pin(async move {
                                let manager =
                                    SessionManager::open(app.store.clone(), &source.header.id)
                                        .map_err(|e| e.to_string())?;
                                let selected =
                                    source.entries.iter().find(|e| e.id == entry).unwrap();
                                let mut navigation = NavigationResult::default();
                                let fork = if position == "before" {
                                    let EntryContent::Message { message } = &selected.content
                                    else {
                                        return Err("before requires a user entry".into());
                                    };
                                    if message["role"] != "user" {
                                        return Err("before requires a user entry".into());
                                    }
                                    navigation.editor_text = Some(native_text(message));
                                    if let Some(parent) = &selected.parent_id {
                                        manager.fork(parent).map_err(|e| e.to_string())?
                                    } else {
                                        SessionManager::create(
                                            app.store.clone(),
                                            &source.header.cwd,
                                        )
                                        .map_err(|e| e.to_string())?
                                    }
                                } else {
                                    manager.fork(&entry).map_err(|e| e.to_string())?
                                };
                                let (session, loaded) =
                                    app.build(fork).await.map_err(|e| e.to_string())?;
                                Ok(OperationResult::Fork(session, loaded, Box::new(navigation)))
                            }),
                        );
                        Ok(None)
                    }
                    "_pirua/session/label" => {
                        self.require_idle(&id, params)?;
                        let entry = string(params, "entryId")?.to_owned();
                        let label = match params.get("label") {
                            Some(Value::Null) => None,
                            Some(Value::String(s)) => Some(s.clone()),
                            _ => return Err(RpcError::params("label must be a string or null")),
                        };
                        let session = self.owners[&id].session.clone();
                        self.mutation(
                            &id,
                            key,
                            reply.take().unwrap(),
                            Box::pin(async move {
                                session
                                    .set_label(&entry, label)
                                    .await
                                    .map_err(|e| e.to_string())?;
                                Ok(OperationResult::Mutation(json!({})))
                            }),
                        );
                        Ok(None)
                    }
                    "_pirua/session/rename" => {
                        self.require_idle(&id, params)?;
                        let name = string(params, "name")?.to_owned();
                        let session = self.owners[&id].session.clone();
                        self.mutation(
                            &id,
                            key,
                            reply.take().unwrap(),
                            Box::pin(async move {
                                session.set_name(&name).await.map_err(|e| e.to_string())?;
                                Ok(OperationResult::Mutation(json!({"rename":true})))
                            }),
                        );
                        Ok(None)
                    }
                    "_pirua/session/scoped_models" => {
                        self.require_idle(&id, params)?;
                        let models = params["models"]
                            .as_array()
                            .ok_or_else(|| RpcError::params("models must be an array"))?;
                        let mut scoped = Vec::new();
                        for selection in models {
                            let p = string(selection, "provider")?;
                            let m = string(selection, "modelId")?;
                            let model = self
                                .application
                                .models
                                .iter()
                                .find(|v| v.provider.as_str() == p && v.id.as_str() == m)
                                .cloned()
                                .ok_or_else(|| RpcError::params("Unknown scoped model"))?;
                            let thinking = selection
                                .get("thinkingLevel")
                                .map(|v| {
                                    serde_json::from_value(v.clone())
                                        .map_err(|e| RpcError::params(e.to_string()))
                                })
                                .transpose()?;
                            scoped.push(ScopedModel {
                                model,
                                thinking_level: thinking,
                            });
                        }
                        self.owners[&id]
                            .session
                            .set_scoped_models(scoped)
                            .map_err(RpcError::failed)?;
                        self.publish_fields(&id, &["scopedModels"]);
                        Ok(Some(
                            json!({"snapshot":self.owners[&id].projection.snapshot(&self.owners[&id].session,&self.application.models)}),
                        ))
                    }
                    "_pirua/session/reload" => {
                        self.require_idle(&id, params)?;
                        self.rebuild(&id).await?;
                        let owner = &self.owners[&id];
                        let snapshot = owner
                            .projection
                            .snapshot(&owner.session, &self.application.models);
                        Ok(Some(
                            json!({"diagnostics":snapshot["resources"]["diagnostics"],"snapshot":snapshot}),
                        ))
                    }
                    _ => Err(RpcError::new(-32601, "Unknown method")),
                }
            }
        }
    }
    fn mutation(
        &mut self,
        id: &str,
        key: &str,
        reply: Reply,
        operation: BoxFuture<'static, Result<OperationResult, String>>,
    ) {
        let owner = self.owners.get_mut(id).unwrap();
        owner.busy = true;
        owner.operation_reply = Some(reply);
        owner.operation_request = Some(key.into());
        self.set_state(id, true, None);
        let tx = self.tx.clone();
        let id = id.to_owned();
        self.tasks.spawn(async move {
            let result = operation.await;
            let _ = tx.send(Event::Driver(id, result));
        });
    }
    fn input(
        &mut self,
        id: String,
        submission: String,
        request: Option<String>,
        resolution: InputResolution,
    ) {
        let pending = request.as_ref().and_then(|key| self.pending.remove(key));
        let Some(owner) = self.owners.get_mut(&id) else {
            return;
        };
        owner.pending_inputs = owner.pending_inputs.saturating_sub(1);
        if pending.is_some() && matches!(&resolution, InputResolution::Cancelled) {
            // Only an actual uninserted prompt cancellation owns this outcome.
            // An RPC can remain pending briefly after insertion already won;
            // cancelling its ticket then must not affect the foreground run.
            owner.cancelling = true;
            owner.projection.stop_reason = Some("cancelled".into());
            owner.projection.last_error = None;
        }
        if matches!(&resolution, InputResolution::Rejected { .. }) {
            owner.needs_rebuild = true;
        }
        let (mut update, result) = match resolution {
            InputResolution::Inserted {
                entry_id,
                message: _,
            } => (
                json!({"submissionId":submission,"outcome":"inserted","messageId":entry_id}),
                Ok(json!({"messageId":entry_id})),
            ),
            InputResolution::Handled => (
                json!({"submissionId":submission,"outcome":"handled"}),
                Err(RpcError::app(
                    "operation_failed",
                    "Input was handled without a retained user message",
                )),
            ),
            InputResolution::Cancelled => (
                json!({"submissionId":submission,"outcome":"cancelled"}),
                Err(RpcError::cancelled()),
            ),
            InputResolution::Rejected { message } => (
                json!({"submissionId":submission,"outcome":"rejected","error":message}),
                Err(RpcError::failed(message)),
            ),
        };
        if let Some(pending) = pending {
            let _ = pending.reply.send(result);
        }
        if self.pi {
            let _ = self.output.send(owner.projection.changed(
                &id,
                json!({}),
                None,
                Some(update.take()),
            ));
        }
        self.publish_fields(&id, &["queue"]);
    }
    fn queue_finished(
        &mut self,
        id: &str,
        key: &str,
        result: Result<pi_coding_runtime::QueueAdmission, RpcError>,
        reply: Reply,
    ) {
        let Some(owner) = self.owners.get_mut(id) else {
            let _ = reply.send(Err(RpcError::cancelled()));
            return;
        };
        owner.pending_queue = owner.pending_queue.saturating_sub(1);
        owner.queue_cancellations.remove(key);
        match result {
            Ok(pi_coding_runtime::QueueAdmission::Handled) => {
                let _ = reply.send(Ok(json!({"disposition":"handled"})));
            }
            Ok(pi_coding_runtime::QueueAdmission::Queued(ticket)) => {
                let submission = ticket.id.to_string();
                self.own_ticket(id, ticket, None, None);
                let _ = reply.send(Ok(
                    json!({"disposition":"queued","submissionId":submission}),
                ));
            }
            Err(error) => {
                let _ = reply.send(Err(error));
            }
        }
        self.publish_fields(id, &["queue"]);
        self.continue_queue(id);
    }
    fn continue_queue(&mut self, id: &str) {
        let Some(owner) = self.owners.get_mut(id) else {
            return;
        };
        if owner.busy || owner.cancelling || owner.closing || owner.pending_queue > 0 {
            return;
        }
        let (a, b) = owner.session.queued_inputs();
        if a.is_empty() && b.is_empty() {
            return;
        }
        owner.busy = true;
        owner.projection.entities.clear();
        owner.projection.native_partials.clear();
        owner.projection.entity_order.clear();
        let session = owner.session.clone();
        self.publish_fields(id, &["liveEntities"]);
        self.set_state(id, true, None);
        self.start_driver(id, Box::pin(async move { session.continue_run().await }));
    }
    async fn finished(&mut self, id: &str, result: Result<OperationResult, String>) {
        let Some(owner) = self.owners.get_mut(id) else {
            return;
        };
        owner.busy = false;
        owner.operation_request = None;
        owner.projection.activity["bashRunning"] = json!(false);
        let reply = owner.operation_reply.take();
        let cancelled = owner.cancelling;
        let mut reply_value: Option<RpcResult> = None;
        let mut include_snapshot = false;
        let mut fork = None;
        match result {
            Err(error) => {
                owner.cancelling = true;
                let session = owner.session.clone();
                let tx = self.tx.clone();
                let event_id = id.to_owned();
                self.tasks.spawn(async move {
                    let result = session
                        .clear_queue_tracked()
                        .await
                        .map_err(|e| e.to_string());
                    let _ = tx.send(Event::Cleared(event_id, result, None));
                });
                owner.projection.last_error = Some(error.clone());
                owner.projection.stop_reason =
                    Some(if cancelled { "cancelled" } else { "error" }.into());
                reply_value = Some(Err(if cancelled {
                    RpcError::cancelled()
                } else {
                    RpcError::failed(error)
                }));
            }
            Ok(OperationResult::Prompt | OperationResult::Compact) => {}
            Ok(OperationResult::Mutation(mut value)) => {
                if value["configuration"] == true {
                    let config = projection::config(&owner.session, &self.application.models);
                    let _ = self.output.send(owner.projection.standard(
                        id,
                        json!({"sessionUpdate":"config_option_update","configOptions":config}),
                        self.pi,
                    ));
                    value = json!({"configOptions":config});
                } else if value["rename"] == true {
                    value = json!({});
                } else {
                    include_snapshot = true;
                }
                if value["aborted"] == true {
                    owner.projection.stop_reason = Some("cancelled".into());
                }
                reply_value = Some(Ok(value));
            }
            Ok(OperationResult::Bash(result)) => {
                let terminal = owner
                    .projection
                    .entities
                    .keys()
                    .rev()
                    .find(|(kind, _)| kind == "terminal_update")
                    .map(|(_, id)| id.clone());
                if let Some(terminal) = terminal {
                    let _=self.output.send(owner.projection.standard(id,json!({"sessionUpdate":"terminal_update","terminalId":terminal,"output":{"data":base64::Engine::encode(&base64::engine::general_purpose::STANDARD,result.output.as_bytes())},"exitStatus":{"exitCode":result.exit_code.filter(|code|*code>=0)}}),self.pi));
                }
                if result.cancelled {
                    owner.projection.stop_reason = Some("cancelled".into());
                }
                reply_value = Some(Ok(serde_json::to_value(result).unwrap()));
            }
            Ok(OperationResult::Fork(session, loaded, navigation)) => {
                fork = Some((session, loaded, navigation));
            }
        }
        let reason = if cancelled || owner.projection.last_outcome == ActivityOutcome::Aborted {
            Some("cancelled".into())
        } else if owner.projection.last_outcome == ActivityOutcome::Error {
            Some("error".into())
        } else {
            owner
                .projection
                .stop_reason
                .clone()
                .or(Some("end_turn".into()))
        };
        owner.projection.stop_reason = reason;
        owner.projection.settings["thinkingLevel"] = json!(owner.session.thinking_level());
        owner.projection.settings["compaction"]["enabled"] =
            json!(owner.session.auto_compaction_enabled());
        owner.projection.settings["retry"]["enabled"] = json!(owner.session.auto_retry_enabled());
        owner.projection.settings["steeringMode"] = json!(owner.session.agent().steering_mode());
        owner.projection.settings["followUpMode"] = json!(owner.session.agent().follow_up_mode());
        self.settle_projection(id);
        self.publish_fields(
            id,
            &[
                "model",
                "routedModel",
                "thinkingLevel",
                "availableThinkingLevels",
                "configOptions",
                "messages",
                "contextEntries",
                "queue",
                "activity",
                "stats",
                "contextUsage",
                "settings",
                "liveEntities",
            ],
        );
        if include_snapshot
            && let Some(Ok(value)) = &mut reply_value
            && let Some(owner) = self.owners.get(id)
        {
            value["snapshot"] = owner
                .projection
                .snapshot(&owner.session, &self.application.models);
        }
        if let Some((session, loaded, navigation)) = fork {
            reply_value = Some(match self.register(session, loaded).await {
                Ok(fork_id) => {
                    let owner = &self.owners[&fork_id];
                    Ok(
                        json!({"sessionId":fork_id,"cancelled":navigation.cancelled,"aborted":navigation.aborted,"editorText":navigation.editor_text,"snapshot":owner.projection.snapshot(&owner.session,&self.application.models)}),
                    )
                }
                Err(error) => Err(error),
            });
        }
        if let Some(reply) = reply {
            let _ = reply.send(reply_value.unwrap_or_else(|| Ok(json!({}))));
        }
        self.maybe_close(id).await;
        self.continue_queue(id);
    }
    fn settle_projection(&mut self, id: &str) {
        let Some(owner) = self.owners.get_mut(id) else {
            return;
        };
        if owner.busy || owner.pending_inputs > 0 || owner.pending_queue > 0 {
            return;
        }
        let (steering, follow) = owner.session.queued_inputs();
        if !steering.is_empty() || !follow.is_empty() {
            return;
        }
        let cancelling = owner.cancelling;
        let reason = if cancelling && owner.projection.stop_reason.as_deref() != Some("error") {
            Some("cancelled".into())
        } else {
            owner.projection.stop_reason.clone()
        };
        owner.cancelling = false;
        if owner.projection.state != "idle" || cancelling {
            owner.projection.entities.clear();
            owner.projection.native_partials.clear();
            owner.projection.entity_order.clear();
            self.set_state(id, false, reason);
            self.publish_fields(id, &["liveEntities"]);
        }
    }
    async fn maybe_close(&mut self, id: &str) {
        let Some(owner) = self.owners.get_mut(id) else {
            return;
        };
        if owner.busy || owner.pending_inputs > 0 || owner.pending_queue > 0 {
            return;
        }
        self.settle_projection(id);
        let rebuild = self
            .owners
            .get(id)
            .is_some_and(|owner| owner.needs_rebuild && !owner.closing);
        if rebuild
            && let Err(error) = self.rebuild(id).await
            && let Some(owner) = self.owners.get_mut(id)
        {
            owner.closing = true;
            owner.projection.last_error = Some(error.message);
        }
        let owner = self.owners.get_mut(id).unwrap();
        for reply in owner.cancel_replies.drain(..) {
            let _ = reply.send(Ok(json!({})));
        }
        if owner.closing {
            let owner = self.owners.remove(id).unwrap();
            owner.session.unsubscribe(owner.subscription);
            let result = owner
                .session
                .dispose()
                .await
                .map(|_| json!({}))
                .map_err(RpcError::failed);
            for reply in owner.close_replies {
                let _ = reply.send(result.clone());
            }
        }
    }
    async fn rebuild(&mut self, id: &str) -> Result<(), RpcError> {
        let owner = self
            .owners
            .get(id)
            .ok_or_else(|| RpcError::params("Session is closed"))?;
        let settings = owner.projection.settings.clone();
        let source = owner.session.clone();
        let scoped = source.scoped_models();
        let steering = source.agent().steering_mode();
        let follow = source.agent().follow_up_mode();
        let compact = source.auto_compaction_enabled();
        let retry = source.auto_retry_enabled();
        let manager =
            SessionManager::open(self.application.store.clone(), id).map_err(RpcError::failed)?;
        let (replacement, loaded) = self
            .application
            .build_with_settings(manager, &settings)
            .await
            .map_err(RpcError::failed)?;
        replacement
            .set_scoped_models(scoped)
            .map_err(RpcError::failed)?;
        replacement.set_steering_mode(steering);
        replacement.set_follow_up_mode(follow);
        replacement.set_auto_compaction_enabled(compact);
        replacement.set_auto_retry_enabled(retry);
        let mut previous = self.owners.remove(id).expect("owned session");
        previous.session.unsubscribe(previous.subscription);
        previous.session.dispose().await.map_err(RpcError::failed)?;
        let actual = self.register(replacement, loaded).await?;
        debug_assert_eq!(actual, id);
        let owner = self.owners.get_mut(id).unwrap();
        owner.projection.settings = settings;
        owner.cancel_replies = std::mem::take(&mut previous.cancel_replies);
        if self.pi {
            let snapshot = owner
                .projection
                .snapshot(&owner.session, &self.application.models);
            let mut changes = snapshot.as_object().unwrap().clone();
            for key in ["sessionId", "epoch", "sequence", "history"] {
                changes.remove(key);
            }
            let history = owner.projection.history.clone();
            let _ = self.output.send(owner.projection.changed(
                id,
                Value::Object(changes),
                Some(json!({"kind":"replace","history":history})),
                None,
            ));
        }
        Ok(())
    }
    fn setup_result(&self, id: &str, new: bool) -> Value {
        let owner = &self.owners[id];
        let mut value = json!({"configOptions":projection::config(&owner.session,&self.application.models),"availableCommands":[{"name":"compact","description":"Compact the native session context","input":{"hint":"optional instructions"}}]});
        if new {
            value["sessionId"] = json!(id);
        }
        value
    }
    fn auth_methods(&self) -> Vec<Value> {
        if !self.elicitation {
            return vec![];
        }
        let Some(registry) = &self.application.registry else {
            return vec![];
        };
        let mut out = Vec::new();
        for p in registry.get_providers() {
            if let Some(api) = p.auth().api_key() {
                out.push(json!({"type":"agent","methodId":format!("{}:apiKey",p.id()),"name":api.name()}));
            }
            if let Some(oauth) = p.auth().oauth() {
                out.push(json!({"type":"agent","methodId":format!("{}:oauth",p.id()),"name":oauth.name()}));
            }
        }
        out
    }
}
fn supported_lifecycle(params: &Value) -> Result<(), RpcError> {
    for key in ["additionalDirectories", "mcpServers"] {
        if params
            .get(key)
            .and_then(Value::as_array)
            .is_some_and(|v| !v.is_empty())
        {
            return Err(RpcError::app(
                "unsupported",
                format!("{key} is not supported by this host"),
            ));
        }
    }
    Ok(())
}
fn native_text(message: &Value) -> String {
    message["content"]
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| {
            message["content"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|c| c["text"].as_str())
                .collect::<Vec<_>>()
                .join("\n")
        })
}
fn timestamp(milliseconds: u64) -> String {
    // Civil date conversion for the positive Unix millisecond timestamps in native storage.
    let seconds = milliseconds / 1000;
    let days = (seconds / 86400) as i64;
    let z = days + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let mut year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{:03}Z",
        seconds / 3600 % 24,
        seconds / 60 % 60,
        seconds % 60,
        milliseconds % 1000
    )
}
struct Interaction {
    output: Output,
    reverse: Reverse,
    request: Value,
    next: std::sync::atomic::AtomicU64,
}
impl AuthInteraction for Interaction {
    fn prompt<'a>(
        &'a self,
        prompt: AuthPrompt,
        c: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<String, AuthInteractionError>> {
        Box::pin(async move {
            let value = serde_json::to_value(&prompt).map_err(AuthInteractionError::with_source)?;
            let index = self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let id = json!(format!("auth:{}:{index}", self.request));
            let key = id.to_string();
            let mut property = json!({"type":"string","title":value["message"]});
            if value["type"] == "select" {
                property["enum"] = json!(
                    value["options"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .filter_map(|v| v["id"].as_str())
                        .collect::<Vec<_>>()
                );
            }
            let params = json!({"mode":"form","message":value["message"],"requestId":self.request,"requestedSchema":{"type":"object","properties":{"value":property},"required":["value"]},"_meta":{"pirua.dev/auth":{"secret":value["type"]=="secret"}}});
            let (tx, rx) = oneshot::channel();
            self.reverse.lock().unwrap().insert(key.clone(), tx);
            self.output
                .send(
                    json!({"jsonrpc":"2.0","id":id,"method":"elicitation/create","params":params}),
                )
                .map_err(|_| AuthInteractionError::new())?;
            let result = tokio::select! {r=rx=>r.map_err(|_|AuthInteractionError::new()),_=c.cancelled()=>Err(AuthInteractionError::new())};
            self.reverse.lock().unwrap().remove(&key);
            let result = result?;
            if result["result"]["action"] != "accept" {
                return Err(AuthInteractionError::new());
            }
            result["result"]["content"]["value"]
                .as_str()
                .map(str::to_owned)
                .ok_or_else(AuthInteractionError::new)
        })
    }
    fn notify(&self, event: AuthEvent) -> Result<(), AuthInteractionError> {
        // Authentication instructions are request-scoped form elicitations; they
        // contain no credentials and the receive pump remains independently live.
        let value = serde_json::to_value(event).map_err(AuthInteractionError::with_source)?;
        let index = self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let id = json!(format!("auth-info:{}:{index}", self.request));
        let message = if let Some(url) = value["url"].as_str() {
            format!(
                "{}\n{}",
                value["instructions"]
                    .as_str()
                    .unwrap_or("Open this authorization URL"),
                url
            )
        } else {
            value["message"]
                .as_str()
                .unwrap_or("Authentication in progress")
                .to_owned()
        };
        self.output.send(json!({"jsonrpc":"2.0","id":id,"method":"elicitation/create","params":{"mode":"form","requestId":self.request,"message":message,"requestedSchema":{"type":"object","properties":{}}}})).map_err(|_|AuthInteractionError::new())?;
        Ok(())
    }
}
