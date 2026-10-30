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
    /// An `auth/login` or `auth/logout` task ended: its request id and generation, and
    /// the advertised methods as the credentials now stand.
    Auth(String, u64, Vec<Value>),
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
    rebuild_failed: bool,
    queue_cancellations: BTreeMap<String, CancellationToken>,
    operation_reply: Option<Reply>,
    operation_request: Option<(String, CancellationToken)>,
    cancel_replies: Vec<Reply>,
    close_replies: Vec<Reply>,
    /// `session/resume` requests received while closing, and closes that follow them; each runs
    /// again, in order, once the owner is gone.
    resumes: Vec<Call>,
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
    /// The client extends its context from `historyChange.contextAppended`; it does not need
    /// the whole projected context sent again with every append.
    context_append: bool,
    elicitation: bool,
    owners: BTreeMap<String, Owner>,
    pending: BTreeMap<String, Pending>,
    tasks: JoinSet<()>,
    /// Cancellation of each running `auth/login` or `auth/logout`, by request id. The
    /// generation tells a request id's earlier run from its current one.
    auth: BTreeMap<String, (u64, CancellationToken)>,
    auth_generation: u64,
    /// Advertised authentication methods; empty unless elicitation was negotiated.
    auth_methods: Vec<Value>,
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
        context_append: false,
        elicitation: false,
        owners: BTreeMap::new(),
        pending: BTreeMap::new(),
        tasks: JoinSet::new(),
        auth: BTreeMap::new(),
        auth_generation: 0,
        auth_methods: vec![],
    };
    let actor = tokio::spawn(host.run());
    let mut lines = BufReader::new(input).split(b'\n');
    let mut responses = JoinSet::new();
    let mut read = Ok(());
    loop {
        let line = tokio::select! {line=lines.next_segment()=>line,_=tokio::signal::ctrl_c()=>Ok(None),_=closed.cancelled()=>Ok(None)};
        // Bytes, not text: invalid UTF-8 is a parse error; a read error still shuts down below.
        let Some(line) = line.unwrap_or_else(|error| {
            read = Err(error);
            None
        }) else {
            break;
        };
        let line = line.strip_suffix(b"\r").unwrap_or(&line);
        match serde_json::from_slice::<Value>(line) {
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
    let written = writer.await?;
    read?;
    written
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
                // A cancel names no session, but its target may sit one barrier hop behind.
                // Each event is deferred at most once, to the tail: every earlier call either
                // ran on receipt or was requeued before this cancel arrived, so one hop of its
                // own keeps the cancel behind all of them.
                Event::Call(call) if call.method == "$/cancel_request" => {
                    let _ = self.tx.send(Event::Ready(Box::new(Event::Call(call))));
                    continue;
                }
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
                Event::Auth(key, generation, methods) => {
                    self.auth_finished(&key, generation, methods)
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
                    for (_, token) in self.auth.values() {
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
        // The context an append adds, when the client can extend its own with it.
        let mut extension = None;
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
                if self.context_append {
                    extension = projection::context_extension(
                        owner.projection.history.leaf_id.as_deref(),
                        &appended,
                        leaf_id.as_deref(),
                    );
                }
                owner.projection.history.entries.extend(appended.clone());
                owner.projection.history.revision = revision;
                owner.projection.history.leaf_id = leaf_id.clone();
                let mut change = json!({"kind":"append","baseRevision":base_revision,"revision":revision,"appended":appended,"leafId":leaf_id});
                if let Some(context) = &extension {
                    change["contextAppended"] = json!(context);
                }
                change
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
            // Whole, unless the append only extends what the client already holds.
            let changes = if extension.is_some() {
                json!({})
            } else {
                let context = projection::project_context(&owner.projection.history);
                json!({"messages":context.messages,"contextEntries":context.entries})
            };
            let _ = self.output.send(owner.projection.changed(
                id,
                changes,
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
        let owner = self.prepare(session, loaded).await?;
        let id = owner.session.session_id();
        self.owners.insert(id.clone(), owner);
        Ok(id)
    }
    /// Every fallible step of a new owner, before the connection can see it. A session
    /// that cannot become an owner is disposed here rather than leaked.
    async fn prepare(
        &self,
        session: CodingSession,
        loaded: LoadedResources,
    ) -> Result<Owner, RpcError> {
        #[cfg(test)]
        tests::before_observation(&session).await;
        let observed = match session.observe_history() {
            Ok(observed) => observed,
            Err(error) => {
                let _ = session.dispose().await;
                return Err(RpcError::failed(error));
            }
        };
        let initial = observed.snapshot.clone();
        let history = Arc::new(Mutex::new(observed));
        let callback_history = history.clone();
        let tx = self.tx.clone();
        let event_id = session.session_id();
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
        projection.auth_methods = Value::Array(self.auth_methods.clone());
        Ok(Owner {
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
            rebuild_failed: false,
            queue_cancellations: BTreeMap::new(),
            operation_reply: None,
            operation_request: None,
            cancel_replies: vec![],
            close_replies: vec![],
            resumes: vec![],
            admission: Arc::new(tokio::sync::Mutex::new(())),
        })
    }
    fn publish_fields(&mut self, id: &str, fields: &[&str]) {
        if !self.pi {
            return;
        }
        if let Some(owner) = self.owners.get_mut(id) {
            let changes = owner
                .projection
                .fields(&owner.session, &self.application.models, fields);
            let _ = self
                .output
                .send(owner.projection.changed(id, changes, None, None));
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
    /// Idle foreground admission on current live context.
    async fn admit(&mut self, id: &str, params: &Value) -> Result<(), RpcError> {
        self.require_idle(id, params)?;
        self.require_current(id).await
    }
    /// A rejected input leaves provisional live context that admits nothing more. Replace it
    /// first; when that fails again the request fails with the cause and nothing is admitted.
    async fn require_current(&mut self, id: &str) -> Result<(), RpcError> {
        let owner = self
            .owners
            .get(id)
            .ok_or_else(|| RpcError::params("Session is not open; call session/resume"))?;
        if !owner.needs_rebuild {
            return Ok(());
        }
        if owner.busy || owner.pending_inputs > 0 || owner.pending_queue > 0 {
            return Err(RpcError::app("busy", "Session foreground work is active"));
        }
        self.renew(id).await
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
                if let Some((_, token)) = self.auth.get(&request) {
                    token.cancel();
                }
                for owner in self.owners.values() {
                    if let Some(token) = owner.queue_cancellations.get(&request) {
                        token.cancel();
                    }
                    if let Some((operation, token)) = &owner.operation_request
                        && *operation == request
                    {
                        token.cancel();
                    }
                }
            }
            let _ = reply.send(Ok(json!({})));
            return;
        }
        if method == "initialize" {
            let negotiated = (|| {
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
                self.context_append =
                    self.pi && params["capabilities"]["_meta"][TUI]["contextAppend"] == true;
                self.elicitation = params["capabilities"]["elicitation"]["form"].is_object();
                Ok(())
            })();
            let result = match negotiated {
                Ok(()) => {
                    if self.elicitation
                        && let Some(registry) = &self.application.registry
                    {
                        self.auth_methods = auth_methods(registry).await;
                    }
                    Ok(
                        json!({"protocolVersion":2,"info":{"name":"pi-acp-server","version":env!("CARGO_PKG_VERSION")},"capabilities":{"session":{"prompt":{"image":{}}},"_meta":{TUI:{"version":1,"features":["snapshot","queue","tree","shell","scopedModels","resources"]}}},"authMethods":self.auth_methods}),
                    )
                }
                Err(error) => Err(error),
            };
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
                if let Some(owner) = self.owners.get_mut(&id)
                    && owner.closing
                {
                    // About to be removed: answering with it would hand out a session that is
                    // gone. Resume again once the close has finished, reopening from storage.
                    owner.resumes.push(Call {
                        id: serde_json::from_str(key).unwrap(),
                        method: method.into(),
                        params: params.clone(),
                        reply: reply.take().unwrap(),
                    });
                    return Ok(None);
                }
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
                if !self.auth_methods.iter().any(|m| m["methodId"] == method_id) {
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
                let (generation, token) = self.begin_auth(key);
                let interaction = Interaction {
                    output: self.output.clone(),
                    reverse: self.reverse.clone(),
                    request: serde_json::from_str(key).unwrap(),
                    next: std::sync::atomic::AtomicU64::new(0),
                    login: token.clone(),
                };
                let reply = reply.take().unwrap();
                let (tx, key) = (self.tx.clone(), key.to_owned());
                self.tasks.spawn(async move {
                    let result = registry
                        .login(&provider, auth_type, &interaction, &token)
                        .await
                        .map(|_| json!({}))
                        .map_err(auth_error);
                    // Settled before the reply, so whatever the client asks next sees it.
                    let _ = tx.send(Event::Auth(key, generation, auth_methods(&registry).await));
                    let _ = reply.send(result);
                });
                Ok(None)
            }
            "auth/logout" => {
                self.contracts.validate("LogoutAuthRequest", params)?;
                if self.auth_methods.is_empty() {
                    return Err(RpcError::app(
                        "unsupported",
                        "Authentication was not advertised for this connection",
                    ));
                }
                let registry = self.application.registry.clone().ok_or_else(|| {
                    RpcError::app("unsupported", "Fixture provider has no authentication")
                })?;
                let (generation, token) = self.begin_auth(key);
                let reply = reply.take().unwrap();
                let (tx, key) = (self.tx.clone(), key.to_owned());
                self.tasks.spawn(async move {
                    let mut result = Ok(json!({}));
                    for p in registry.get_providers() {
                        if let Err(e) = registry.logout(p.id(), &token).await {
                            result = Err(auth_error(e));
                            break;
                        }
                    }
                    let _ = tx.send(Event::Auth(key, generation, auth_methods(&registry).await));
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
                        self.admit(&id, params).await?;
                        // Local reads only (the credential store and the environment): `check_auth`
                        // never refreshes OAuth or reaches the network, so this await is brief and
                        // keeps admission ordered before the run starts.
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
                        if method == "session/close" && !owner.resumes.is_empty() {
                            // A resume ahead of it reopens the session: close that one, not this.
                            owner.resumes.push(Call {
                                id: serde_json::from_str(key).unwrap(),
                                method: method.into(),
                                params: params.clone(),
                                reply: reply.take().unwrap(),
                            });
                            return Ok(None);
                        }
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
                            if let Some((_, token)) = &owner.operation_request {
                                token.cancel();
                            }
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
                        self.require_current(&id).await?;
                        let owner = self.owners.get_mut(&id).unwrap();
                        owner.pending_queue += 1;
                        let token = CancellationToken::new();
                        owner.queue_cancellations.insert(key.into(), token.clone());
                        let session = owner.session.clone();
                        let gate = owner.admission.clone();
                        let tx = self.tx.clone();
                        let request = key.to_owned();
                        let reply = reply.take().unwrap();
                        self.tasks.spawn(async move{let admission=async{let _gate=gate.lock().await;session.queue_tracked(text,images,delivery).await.map_err(queue_error)};let result=tokio::select!{biased;_=token.cancelled()=>Err(RpcError::cancelled()),result=admission=>result};let _=tx.send(Event::Queue(id,request,result,reply));});
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
                        self.admit(&id, params).await?;
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
                        self.admit(&id, params).await?;
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
                        self.admit(&id, params).await?;
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
                        self.admit(&id, params).await?;
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
                        self.admit(&id, params).await?;
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
                        self.admit(&id, params).await?;
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
                        self.admit(&id, params).await?;
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
                        self.renew(&id).await?;
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
        let token = CancellationToken::new();
        owner.busy = true;
        owner.operation_reply = Some(reply);
        owner.operation_request = Some((key.into(), token.clone()));
        let session = owner.session.clone();
        self.set_state(id, true, None);
        let tx = self.tx.clone();
        let id = id.to_owned();
        self.tasks.spawn(async move {
            // Polled first, the operation installs its abort target (e.g. the shell process)
            // before a cancellation that preceded this task can look for it.
            let mut operation = operation;
            let result = tokio::select! {
                biased;
                result = &mut operation => result,
                _ = token.cancelled() => {
                    session.abort();
                    session.abort_bash();
                    operation.await
                }
            };
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
        // The context only changes with the history, which announced each change already.
        let fields = [
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
        ]
        .into_iter()
        .filter(|field| !(self.context_append && matches!(*field, "messages" | "contextEntries")))
        .collect::<Vec<_>>();
        self.publish_fields(id, &fields);
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
        // One automatic attempt per rejected input. After a failure only foreground
        // admission retries, so a persistent fault cannot repeat on every connection event.
        let rebuild = self
            .owners
            .get(id)
            .is_some_and(|owner| owner.needs_rebuild && !owner.rebuild_failed && !owner.closing);
        if rebuild {
            let _ = self.renew(id).await;
        }
        let Some(owner) = self.owners.get_mut(id) else {
            return;
        };
        for reply in owner.cancel_replies.drain(..) {
            let _ = reply.send(Ok(json!({})));
        }
        if owner.closing
            && let Some(owner) = self.owners.remove(id)
        {
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
            for resume in owner.resumes {
                let _ = self.tx.send(Event::Call(resume));
            }
        }
    }
    /// Rebuild that keeps a stale owner registered on failure: the cause is recorded and
    /// published as its error state, and only foreground admission retries.
    async fn renew(&mut self, id: &str) -> Result<(), RpcError> {
        let result = self.rebuild(id).await;
        if let Err(error) = &result
            && let Some(owner) = self.owners.get_mut(id)
            && owner.needs_rebuild
        {
            owner.rebuild_failed = true;
            owner.projection.last_error = Some(error.message.clone());
            self.set_state(id, false, Some("error".into()));
        }
        result
    }
    /// Replaces the owner's live context from storage. Every fallible step concerns only
    /// the replacement and precedes any change to the current owner, which a failure
    /// leaves registered as it was.
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
        if let Err(error) = replacement.set_scoped_models(scoped) {
            let _ = replacement.dispose().await;
            return Err(RpcError::failed(error));
        }
        replacement.set_steering_mode(steering);
        replacement.set_follow_up_mode(follow);
        replacement.set_auto_compaction_enabled(compact);
        replacement.set_auto_retry_enabled(retry);
        let mut next = self.prepare(replacement, loaded).await?;
        debug_assert_eq!(next.session.session_id(), id);
        next.projection.settings = settings;
        if let Some(previous) = self.owners.get_mut(id) {
            next.cancel_replies = std::mem::take(&mut previous.cancel_replies);
            next.close_replies = std::mem::take(&mut previous.close_replies);
        }
        if let Some(previous) = self.owners.insert(id.to_owned(), next) {
            previous.session.unsubscribe(previous.subscription);
            // Disposal closes admission before its first await, so no result can reopen the
            // stale context, and the replacement already holds the identity and the owed
            // replies. A failed cleanup is therefore not a failed rebuild.
            let _ = previous.session.dispose().await;
        }
        if self.pi
            && let Some(owner) = self.owners.get_mut(id)
        {
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
    /// Registers the cancellation of one `auth/login` or `auth/logout` request.
    fn begin_auth(&mut self, key: &str) -> (u64, CancellationToken) {
        self.auth_generation += 1;
        let token = CancellationToken::new();
        self.auth
            .insert(key.to_owned(), (self.auth_generation, token.clone()));
        (self.auth_generation, token)
    }
    /// The task ended: forget its cancellation (a request id reused since keeps the new
    /// run's) and hand every open session the methods as the credentials now stand.
    fn auth_finished(&mut self, key: &str, generation: u64, methods: Vec<Value>) {
        if self
            .auth
            .get(key)
            .is_some_and(|(current, _)| *current == generation)
        {
            self.auth.remove(key);
        }
        for owner in self.owners.values_mut() {
            owner.projection.auth_methods = Value::Array(methods.clone());
        }
        self.auth_methods = methods;
    }
}
/// The authentication methods this registry offers. A provider's `status` is present only
/// while its credentials are configured, and `subscription` is the provider's OAuth flag,
/// as in the original Pi login selector. Both come from local knowledge: `check_auth`
/// reads the credential store and ambient environment and never refreshes over the network.
async fn auth_methods(registry: &pi_ai::ModelsRegistry) -> Vec<Value> {
    let never = CancellationToken::new();
    let mut out = Vec::new();
    for p in registry.get_providers() {
        let (api, oauth) = (p.auth().api_key(), p.auth().oauth());
        if api.is_none() && oauth.is_none() {
            continue;
        }
        let mut meta = json!({"subscription":oauth.as_ref().is_some_and(|o|o.is_subscription())});
        if let Ok(Some(check)) = registry.check_auth(p.id(), &never).await {
            meta["status"] =
                json!({"type":check.auth_type,"source":check.source.unwrap_or_default()});
        }
        let meta = json!({"pirua.dev/auth":meta});
        if let Some(api) = api {
            out.push(json!({"type":"agent","methodId":format!("{}:apiKey",p.id()),"name":api.name(),"_meta":meta}));
        }
        if let Some(oauth) = oauth {
            out.push(json!({"type":"agent","methodId":format!("{}:oauth",p.id()),"name":oauth.name(),"_meta":meta}));
        }
    }
    out
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
    /// The token of the `auth/login` this interaction serves.
    login: CancellationToken,
}
/// An `elicitation/create` the client has not answered. Dropping it, because the prompt was
/// dropped or cancelled, withdraws the reply waiter and tells the client to dismiss the form.
struct Outstanding<'a> {
    interaction: &'a Interaction,
    id: Value,
    key: String,
}
impl Drop for Outstanding<'_> {
    fn drop(&mut self) {
        let Interaction {
            reverse, output, ..
        } = self.interaction;
        // An answered or connection-cleared entry is already gone: nothing to withdraw.
        let pending = reverse
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.key)
            .is_some();
        if pending {
            let _ = output.send(
                json!({"jsonrpc":"2.0","method":"$/cancel_request","params":{"requestId":self.id}}),
            );
        }
    }
}
/// A queue admission withdrawn before insertion is a protocol cancellation.
fn queue_error(error: SessionError) -> RpcError {
    match error {
        SessionError::Cancelled => RpcError::cancelled(),
        error => RpcError::failed(error),
    }
}
fn auth_error(error: pi_ai::AuthResolutionError) -> RpcError {
    match error {
        pi_ai::AuthResolutionError::Cancelled => RpcError::cancelled(),
        error => RpcError::failed(error),
    }
}
/// The readable form of an authentication event for a client that ignores `_meta`: every
/// piece of data the user must act on appears in the text.
fn auth_message(event: &Value) -> String {
    let text = |key: &str| event[key].as_str();
    match event["type"].as_str() {
        Some("auth_url") if text("url").is_some() => format!(
            "{}\n{}",
            text("instructions").unwrap_or("Open this authorization URL"),
            text("url").unwrap()
        ),
        Some("device_code") => {
            let mut message = format!(
                "Open {} and enter code: {}",
                text("verificationUri").unwrap_or_default(),
                text("userCode").unwrap_or_default()
            );
            if let Some(seconds) = event["expiresInSeconds"].as_u64() {
                message.push_str(&format!("\nThe code expires in {seconds} seconds."));
            }
            message
        }
        Some("info") => {
            let mut message = text("message").unwrap_or_default().to_owned();
            for link in event["links"].as_array().into_iter().flatten() {
                match link["label"].as_str() {
                    Some(label) => message.push_str(&format!(
                        "\n{label}: {}",
                        link["url"].as_str().unwrap_or_default()
                    )),
                    None => {
                        message.push_str(&format!("\n{}", link["url"].as_str().unwrap_or_default()))
                    }
                }
            }
            message
        }
        _ => text("message")
            .unwrap_or("Authentication in progress")
            .to_owned(),
    }
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
            if self
                .output
                .send(
                    json!({"jsonrpc":"2.0","id":id,"method":"elicitation/create","params":params}),
                )
                .is_err()
            {
                self.reverse.lock().unwrap().remove(&key);
                return Err(AuthInteractionError::new());
            }
            // The form is now on the client's screen.
            let _outstanding = Outstanding {
                interaction: self,
                id,
                key,
            };
            let result = tokio::select! {r=rx=>r.map_err(|_|AuthInteractionError::new()),_=c.cancelled()=>Err(AuthInteractionError::new())};
            let result = result?;
            if matches!(
                result["result"]["action"].as_str(),
                Some("decline" | "cancel")
            ) || result["error"]["code"] == -32800
            {
                // The user walked away from the login. Cancelling its own token is how the
                // operation learns that: error types do not classify cancellation.
                self.login.cancel();
                return Err(AuthInteractionError::new());
            }
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
        let message = auth_message(&value);
        self.output.send(json!({"jsonrpc":"2.0","id":id,"method":"elicitation/create","params":{"mode":"form","requestId":self.request,"message":message,"requestedSchema":{"type":"object","properties":{}},"_meta":{"pirua.dev/auth":{"event":value}}}})).map_err(|_|AuthInteractionError::new())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pi_coding_session::{Error, SessionCommit, SessionInfo, SessionSnapshot, SessionStore};
    use std::{
        cell::Cell,
        path::PathBuf,
        sync::atomic::{AtomicBool, AtomicUsize, Ordering},
        time::Duration,
    };

    thread_local! {
        // Makes this unit test's next owner unobservable: its session is disposed
        // between build and history observation.
        static UNOBSERVABLE: Cell<bool> = const { Cell::new(false) };
    }
    pub(super) async fn before_observation(session: &CodingSession) {
        if UNOBSERVABLE.with(|armed| armed.replace(false)) {
            let _ = session.dispose().await;
        }
    }

    /// Storage faults switched by a test. A failing user-message commit is the canonical
    /// rejected input; a failing load keeps the replacement from opening.
    #[derive(Default)]
    struct Faults {
        user_commit: AtomicBool,
        load: AtomicBool,
        loads: AtomicUsize,
        on_commit_failure: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    }
    struct Faulty {
        inner: Arc<dyn SessionStore>,
        faults: Arc<Faults>,
    }
    impl SessionStore for Faulty {
        fn create(&self, snapshot: &SessionSnapshot) -> Result<(), Error> {
            self.inner.create(snapshot)
        }
        fn load(&self, id: &str) -> Result<SessionSnapshot, Error> {
            self.faults.loads.fetch_add(1, Ordering::SeqCst);
            if self.faults.load.load(Ordering::SeqCst) {
                return Err(Error::Invalid("synthetic load failure".into()));
            }
            self.inner.load(id)
        }
        fn commit(&self, id: &str, commit: &SessionCommit) -> Result<u64, Error> {
            let user = commit.append.iter().any(|entry| {
                matches!(&entry.content, EntryContent::Message { message } if message["role"] == "user")
            });
            if user && self.faults.user_commit.swap(false, Ordering::SeqCst) {
                if let Some(hook) = self.faults.on_commit_failure.lock().unwrap().take() {
                    hook();
                }
                return Err(Error::Invalid("synthetic user commit failure".into()));
            }
            self.inner.commit(id, commit)
        }
        fn list(&self, cwd: Option<&str>) -> Result<Vec<SessionInfo>, Error> {
            self.inner.list(cwd)
        }
    }

    struct Root(PathBuf);
    impl Root {
        fn new() -> Self {
            static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            Self(std::env::temp_dir().join(format!(
                "pirua-acp-unit-{}-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            )))
        }
        async fn application(&self) -> Application {
            Application::new(crate::Options {
                data_dir: self.0.join("data"),
                config_dir: self.0.join("config"),
                credentials: None,
                model: None,
                fixture_provider: true,
                fixture_delay_ms: 20,
            })
            .await
            .unwrap()
        }
        async fn faulty(&self) -> (Application, Arc<Faults>) {
            let mut application = self.application().await;
            let faults = Arc::new(Faults::default());
            application.store = Arc::new(Faulty {
                inner: application.store.clone(),
                faults: faults.clone(),
            });
            (application, faults)
        }
    }
    impl Drop for Root {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    fn call(
        tx: &mpsc::UnboundedSender<Event>,
        id: Value,
        method: &str,
        params: Value,
    ) -> oneshot::Receiver<RpcResult> {
        let (reply, wait) = oneshot::channel();
        let _ = tx.send(Event::Call(Call {
            id,
            method: method.into(),
            params,
            reply,
        }));
        wait
    }
    /// A Pi connection actor with one open fixture session. On the current-thread test
    /// runtime the actor runs only while the test awaits, so frames sent back to back are
    /// all queued before the actor receives the first of them.
    async fn open(
        root: &Root,
    ) -> (
        mpsc::UnboundedSender<Event>,
        mpsc::UnboundedReceiver<Value>,
        String,
    ) {
        open_with(root, root.application().await).await
    }
    async fn open_with(
        root: &Root,
        application: Application,
    ) -> (
        mpsc::UnboundedSender<Event>,
        mpsc::UnboundedReceiver<Value>,
        String,
    ) {
        open_as(root, application, false).await
    }
    /// `context_append`: the client extends its context from appends, as the TUI does.
    async fn open_as(
        root: &Root,
        application: Application,
        context_append: bool,
    ) -> (
        mpsc::UnboundedSender<Event>,
        mpsc::UnboundedReceiver<Value>,
        String,
    ) {
        let (sender, output) = mpsc::unbounded_channel();
        let (tx, rx) = mpsc::unbounded_channel();
        let host = Host {
            application: Arc::new(application),
            contracts: Contracts::new(),
            tx: tx.clone(),
            rx,
            output: Output {
                sender,
                queued: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                closed: CancellationToken::new(),
            },
            reverse: Reverse::default(),
            initialized: false,
            pi: false,
            context_append: false,
            elicitation: false,
            owners: BTreeMap::new(),
            pending: BTreeMap::new(),
            tasks: JoinSet::new(),
            auth: BTreeMap::new(),
            auth_generation: 0,
            auth_methods: vec![],
        };
        tokio::spawn(host.run());
        call(&tx, json!(1), "initialize", json!({"protocolVersion":2,"info":{"name":"unit","version":"1"},"capabilities":{"_meta":{TUI:{"version":1,"contextAppend":context_append}}}}))
            .await
            .unwrap()
            .unwrap();
        let session = call(
            &tx,
            json!(2),
            "session/new",
            json!({"cwd":root.0.to_string_lossy()}),
        )
        .await
        .unwrap()
        .unwrap();
        (
            tx,
            output,
            session["sessionId"].as_str().unwrap().to_owned(),
        )
    }
    async fn shutdown(tx: &mpsc::UnboundedSender<Event>) {
        let (done, wait) = oneshot::channel();
        let _ = tx.send(Event::Shutdown(done));
        let _ = wait.await;
    }

    #[tokio::test]
    async fn request_cancel_reaches_a_shell_call_deferred_by_the_history_barrier() {
        let root = Root::new();
        let (tx, _output, s) = open(&root).await;
        // Queued together: the barrier defers the shell call; the cancel names no session.
        let shell = call(
            &tx,
            json!(10),
            "_pirua/session/bash",
            json!({"sessionId":s,"command":"sleep 2; printf should-not-run"}),
        );
        drop(call(
            &tx,
            Value::Null,
            "$/cancel_request",
            json!({"requestId":10}),
        ));
        let result = shell.await.unwrap().unwrap();
        assert_eq!(result["cancelled"], true, "{result}");
        assert!(
            !result["output"]
                .as_str()
                .unwrap()
                .contains("should-not-run")
        );
        shutdown(&tx).await;
    }

    #[tokio::test]
    async fn request_cancel_reaches_a_prompt_deferred_by_the_history_barrier() {
        let root = Root::new();
        let (tx, _output, s) = open(&root).await;
        let prompt = call(
            &tx,
            json!(11),
            "session/prompt",
            json!({"sessionId":s,"prompt":[{"type":"text","text":"fixture:block"}]}),
        );
        drop(call(
            &tx,
            Value::Null,
            "$/cancel_request",
            json!({"requestId":11}),
        ));
        let error = prompt
            .await
            .unwrap()
            .expect_err("the cancel was received before the prompt could be inserted");
        assert_eq!(error.code, -32800, "{error:?}");
        shutdown(&tx).await;
    }

    #[tokio::test]
    async fn session_cancel_reaches_a_shell_call_whose_task_has_not_started() {
        let root = Root::new();
        let (tx, _output, s) = open(&root).await;
        let shell = call(
            &tx,
            json!(12),
            "_pirua/session/bash",
            json!({"sessionId":s,"command":"sleep 2; printf should-not-run"}),
        );
        let cancel = call(&tx, json!(13), "session/cancel", json!({"sessionId":s}));
        let result = shell.await.unwrap().unwrap();
        assert_eq!(result["cancelled"], true, "{result}");
        assert!(
            !result["output"]
                .as_str()
                .unwrap()
                .contains("should-not-run")
        );
        cancel.await.unwrap().unwrap();
        shutdown(&tx).await;
    }

    fn prompt(
        tx: &mpsc::UnboundedSender<Event>,
        id: u64,
        s: &str,
        text: &str,
    ) -> oneshot::Receiver<RpcResult> {
        call(
            tx,
            json!(id),
            "session/prompt",
            json!({"sessionId":s,"prompt":[{"type":"text","text":text}]}),
        )
    }
    /// Waits for the standard error state naming `cause` and the Pi state change after it.
    async fn published_error(output: &mut mpsc::UnboundedReceiver<Value>, s: &str, cause: &str) {
        let published = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let frame = output.recv().await?;
                let update = &frame["params"]["update"];
                if frame["params"]["sessionId"] == s
                    && update["sessionUpdate"] == "state_update"
                    && update["state"] == "idle"
                    && update["stopReason"] == "error"
                    && update["error"]["message"]
                        .as_str()
                        .is_some_and(|message| message.contains(cause))
                {
                    return output.recv().await;
                }
            }
        })
        .await;
        let Ok(Some(changed)) = published else {
            panic!("the rebuild failure {cause:?} must reach the client");
        };
        assert_eq!(changed["method"], "_pirua/session/changed", "{changed}");
        assert_eq!(changed["params"]["changes"]["state"], "idle", "{changed}");
    }
    async fn snapshot(tx: &mpsc::UnboundedSender<Event>, id: u64, s: &str) -> Value {
        call(
            tx,
            json!(id),
            "_pirua/session/snapshot",
            json!({"sessionId":s}),
        )
        .await
        .unwrap()
        .expect("the owner stays registered")
    }
    /// Waits for the turn that ends after this call to settle; returns every frame up to its end.
    async fn settled(output: &mut mpsc::UnboundedReceiver<Value>, s: &str) -> Vec<Value> {
        let mut frames = vec![];
        tokio::time::timeout(Duration::from_secs(5), async {
            while let Some(frame) = output.recv().await {
                let update = &frame["params"]["update"];
                let end = frame["params"]["sessionId"] == s
                    && update["sessionUpdate"] == "state_update"
                    && update["state"] == "idle"
                    && update["stopReason"] == "end_turn";
                frames.push(frame);
                if end {
                    return;
                }
            }
        })
        .await
        .expect("the turn settles");
        frames
    }
    /// Prompts the fixture provider to report the user messages in the context of its
    /// request: the live context the next turn runs on, not the stored history.
    async fn provider_context(
        tx: &mpsc::UnboundedSender<Event>,
        output: &mut mpsc::UnboundedReceiver<Value>,
        id: u64,
        s: &str,
    ) -> Vec<String> {
        let receipt = prompt(tx, id, s, "fixture:context")
            .await
            .unwrap()
            .expect("the session admits the prompt");
        assert!(receipt["messageId"].is_string(), "{receipt}");
        settled(output, s).await;
        let messages = snapshot(tx, id + 1, s).await["messages"].clone();
        let answer = messages
            .as_array()
            .unwrap()
            .iter()
            .rev()
            .find(|message| message["role"] == "assistant")
            .expect("the provider answered");
        let text = answer["content"]
            .as_array()
            .unwrap()
            .iter()
            .rev()
            .find_map(|block| block["text"].as_str())
            .unwrap();
        serde_json::from_str(text.strip_prefix("Fixture response: ").unwrap()).unwrap()
    }

    #[tokio::test]
    async fn a_rebuild_that_cannot_open_keeps_the_owner_and_retries_at_admission() {
        let root = Root::new();
        let (application, faults) = root.faulty().await;
        let (tx, mut output, s) = open_with(&root, application).await;
        let stale = epoch(&tx, 9, &s).await.unwrap();
        faults.user_commit.store(true, Ordering::SeqCst);
        faults.load.store(true, Ordering::SeqCst);
        let error = prompt(&tx, 10, &s, "rejected first")
            .await
            .unwrap()
            .expect_err("the user commit fails");
        assert!(
            error.message.contains("synthetic user commit failure"),
            "{error:?}"
        );
        published_error(&mut output, &s, "synthetic load failure").await;
        let attempted = faults.loads.load(Ordering::SeqCst);
        // Registered and observable as the same stale owner; later connection events do not
        // retry the rebuild.
        for id in [11, 12] {
            let owner = snapshot(&tx, id, &s).await;
            assert_eq!(owner["state"], "idle");
            assert_eq!(owner["epoch"], stale);
        }
        assert_eq!(faults.loads.load(Ordering::SeqCst), attempted);
        // Each foreground admission retries once and fails with the cause, admitting nothing.
        let admissions = [
            (
                "session/prompt",
                json!({"sessionId":s,"prompt":[{"type":"text","text":"stale prompt"}]}),
            ),
            (
                "_pirua/session/queue",
                json!({"sessionId":s,"delivery":"followUp","prompt":[{"type":"text","text":"stale queue"}]}),
            ),
            (
                "_pirua/session/bash",
                json!({"sessionId":s,"command":"printf stale-shell"}),
            ),
        ];
        for (index, (method, params)) in admissions.into_iter().enumerate() {
            let error = call(&tx, json!(13 + index), method, params)
                .await
                .unwrap()
                .expect_err("stale live context admits nothing");
            assert!(
                error.message.contains("synthetic load failure"),
                "{method}: {error:?}"
            );
            assert_eq!(
                faults.loads.load(Ordering::SeqCst),
                attempted + 1 + index,
                "{method}"
            );
        }
        faults.load.store(false, Ordering::SeqCst);
        // The recovered turn runs on a rebuilt owner, whose context holds neither the failed
        // input nor anything refused since.
        let context = provider_context(&tx, &mut output, 20, &s).await;
        assert_eq!(context, ["fixture:context"], "the provider saw stale input");
        assert_ne!(epoch(&tx, 22, &s).await.unwrap(), stale);
        shutdown(&tx).await;
    }

    #[tokio::test]
    async fn a_replacement_that_cannot_be_observed_leaves_the_owner_and_answers_owed_replies() {
        let root = Root::new();
        let (application, faults) = root.faulty().await;
        let (tx, mut output, s) = open_with(&root, application).await;
        let stale = epoch(&tx, 29, &s).await.unwrap();
        // Received at the failing commit itself: behind the prompt, ahead of its rejection,
        // so this reply is still owed when the settled owner rebuilds.
        let (reply, cancelled) = oneshot::channel();
        let (sender, session) = (tx.clone(), s.clone());
        *faults.on_commit_failure.lock().unwrap() = Some(Box::new(move || {
            let _ = sender.send(Event::Call(Call {
                id: json!(30),
                method: "session/cancel".into(),
                params: json!({"sessionId":session}),
                reply,
            }));
        }));
        faults.user_commit.store(true, Ordering::SeqCst);
        UNOBSERVABLE.with(|armed| armed.set(true));
        let error = prompt(&tx, 31, &s, "rejected first")
            .await
            .unwrap()
            .expect_err("the user commit fails");
        assert!(
            error.message.contains("synthetic user commit failure"),
            "{error:?}"
        );
        tokio::time::timeout(Duration::from_secs(5), cancelled)
            .await
            .expect("the cancel settles")
            .expect("the owed cancel reply is answered")
            .unwrap();
        published_error(&mut output, &s, "CodingSession is disposed").await;
        assert!(
            !UNOBSERVABLE.with(Cell::get),
            "the rebuild reached observation"
        );
        assert_eq!(snapshot(&tx, 32, &s).await["epoch"], stale);
        call(
            &tx,
            json!(33),
            "session/new",
            json!({"cwd":root.0.to_string_lossy()}),
        )
        .await
        .unwrap()
        .expect("the connection keeps serving");
        let context = provider_context(&tx, &mut output, 34, &s).await;
        assert_eq!(context, ["fixture:context"], "the provider saw stale input");
        assert_ne!(epoch(&tx, 36, &s).await.unwrap(), stale);
        shutdown(&tx).await;
    }

    #[tokio::test]
    async fn a_stale_owner_still_answers_cancel_and_close() {
        let root = Root::new();
        let (application, faults) = root.faulty().await;
        let (tx, mut output, s) = open_with(&root, application).await;
        faults.user_commit.store(true, Ordering::SeqCst);
        faults.load.store(true, Ordering::SeqCst);
        prompt(&tx, 40, &s, "rejected first")
            .await
            .unwrap()
            .expect_err("the user commit fails");
        published_error(&mut output, &s, "synthetic load failure").await;
        call(&tx, json!(41), "session/cancel", json!({"sessionId":s}))
            .await
            .unwrap()
            .expect("cancel is answered");
        call(&tx, json!(42), "session/close", json!({"sessionId":s}))
            .await
            .unwrap()
            .expect("close is answered");
        let error = call(
            &tx,
            json!(43),
            "_pirua/session/snapshot",
            json!({"sessionId":s}),
        )
        .await
        .unwrap()
        .expect_err("the closed owner is gone");
        assert_eq!(error.code, -32602, "{error:?}");
        shutdown(&tx).await;
    }

    /// The activation epoch of the session's current owner: new for every owner.
    async fn epoch(tx: &mpsc::UnboundedSender<Event>, id: u64, s: &str) -> RpcResult {
        let snapshot = call(
            tx,
            json!(id),
            "_pirua/session/snapshot",
            json!({"sessionId":s}),
        );
        snapshot
            .await
            .unwrap()
            .map(|snapshot| snapshot["epoch"].clone())
    }

    #[tokio::test]
    async fn resuming_a_closing_session_waits_for_the_close_and_reopens_it() {
        let root = Root::new();
        let (tx, _output, s) = open(&root).await;
        let before = epoch(&tx, 50, &s).await.expect("the session is open");
        // Queued together: the close finds the prompt running and cannot finish at once,
        // so the resume arrives while the owner is closing.
        let running = prompt(&tx, 51, &s, "fixture:block");
        let close = call(&tx, json!(52), "session/close", json!({"sessionId":s}));
        let resume = call(
            &tx,
            json!(53),
            "session/resume",
            json!({"sessionId":s,"cwd":root.0.to_string_lossy()}),
        );
        tokio::time::timeout(Duration::from_secs(5), resume)
            .await
            .expect("the resume is answered once the close has finished")
            .unwrap()
            .expect("the resume reopens the session");
        close.await.unwrap().expect("the close is answered");
        let _ = running.await;
        // The resume did not hand out the closing owner: a new one serves the session.
        let after = epoch(&tx, 54, &s)
            .await
            .expect("the resumed session stays open");
        assert_ne!(after, before, "the closing owner was handed out");
        let receipt = prompt(&tx, 55, &s, "again")
            .await
            .unwrap()
            .expect("the reopened session admits a prompt");
        assert!(receipt["messageId"].is_string(), "{receipt}");
        shutdown(&tx).await;
    }

    #[tokio::test]
    async fn a_close_after_a_parked_resume_closes_the_reopened_session() {
        let root = Root::new();
        let (tx, _output, s) = open(&root).await;
        let cwd = root.0.to_string_lossy().to_string();
        let running = prompt(&tx, 60, &s, "fixture:block");
        let first = call(&tx, json!(61), "session/close", json!({"sessionId":s}));
        let resume = call(
            &tx,
            json!(62),
            "session/resume",
            json!({"sessionId":s,"cwd":cwd}),
        );
        let second = call(&tx, json!(63), "session/close", json!({"sessionId":s}));
        first.await.unwrap().expect("the first close is answered");
        resume
            .await
            .unwrap()
            .expect("the parked resume reopens the session");
        tokio::time::timeout(Duration::from_secs(5), second)
            .await
            .expect("the second close is answered")
            .unwrap()
            .expect("the second close succeeds");
        let _ = running.await;
        // Requests take effect in the order sent: the last one closed the reopened session.
        assert!(
            epoch(&tx, 64, &s).await.is_err(),
            "the reopened session must be closed"
        );
        shutdown(&tx).await;
    }

    /// What a client sees of `turns` prompts: the frames that announced history appends, in
    /// order, grouped by turn, and the whole projections the server made meanwhile.
    struct Growth {
        turns: Vec<Vec<Value>>,
        /// Frames of any kind that carried the whole context.
        whole: usize,
        projections: usize,
        context: (Vec<Value>, Vec<Value>),
        snapshot: Value,
    }
    async fn grow(root: &Root, context_append: bool, turns: usize) -> Growth {
        let (tx, mut output, s) = open_as(root, root.application().await, context_append).await;
        let held = snapshot(&tx, 100, &s).await;
        let mut context = (
            held["messages"].as_array().unwrap().clone(),
            held["contextEntries"].as_array().unwrap().clone(),
        );
        let before = projection::PROJECTIONS.with(Cell::get);
        let mut grown = vec![];
        let mut whole = 0;
        for turn in 0..turns {
            let text = format!("turn {turn} {}", "padding ".repeat(64));
            prompt(&tx, 101 + turn as u64, &s, &text)
                .await
                .unwrap()
                .unwrap();
            let frames = settled(&mut output, &s).await;
            whole += frames
                .iter()
                .filter(|frame| {
                    frame["method"] == "_pirua/session/changed"
                        && frame["params"]["changes"].get("messages").is_some()
                })
                .count();
            let appends = frames
                .into_iter()
                .filter(|frame| {
                    frame["method"] == "_pirua/session/changed"
                        && frame["params"]["historyChange"]["kind"] == "append"
                })
                .collect::<Vec<_>>();
            for frame in &appends {
                // What a client does with the frame.
                let params = &frame["params"];
                match params["historyChange"]["contextAppended"].as_array() {
                    Some(added) => {
                        for entry in added {
                            context
                                .0
                                .extend(entry["messages"].as_array().unwrap().clone());
                            context.1.push(entry.clone());
                        }
                    }
                    None => {
                        context.0 = params["changes"]["messages"].as_array().unwrap().clone();
                        context.1 = params["changes"]["contextEntries"]
                            .as_array()
                            .unwrap()
                            .clone();
                    }
                }
            }
            grown.push(appends);
        }
        let projections = projection::PROJECTIONS.with(Cell::get) - before;
        let snapshot = snapshot(&tx, 200, &s).await;
        shutdown(&tx).await;
        Growth {
            turns: grown,
            whole,
            projections,
            context,
            snapshot,
        }
    }
    fn bytes(frames: &[Value]) -> usize {
        frames.iter().map(|frame| frame.to_string().len()).sum()
    }

    #[tokio::test]
    async fn an_append_costs_the_same_however_long_the_session_has_run() {
        let root = Root::new();
        let turns = 6;
        let extending = grow(&root, true, turns).await;
        for frame in extending.turns.iter().flatten() {
            let (changes, change) = (
                &frame["params"]["changes"],
                &frame["params"]["historyChange"],
            );
            assert!(changes.get("messages").is_none() && changes.get("contextEntries").is_none());
            // The entries the append adds to the context, each beside its source entry.
            let added = change["contextAppended"].as_array().unwrap();
            let appended = change["appended"].as_array().unwrap();
            assert_eq!(added.len(), appended.len());
            for (entry, source) in added.iter().zip(appended) {
                assert_eq!(entry["sourceEntry"], *source);
            }
        }
        // From the second turn on (the first also carries the session's setup) the frames of
        // each turn are the same size, and following them yields the context a snapshot holds.
        let (second, last) = (
            bytes(&extending.turns[1]),
            bytes(&extending.turns[turns - 1]),
        );
        assert!(
            last <= second + second / 50,
            "{second} bytes grew to {last}"
        );
        assert_eq!(extending.whole, 0, "no frame needs the whole context");
        assert_eq!(
            extending.context.0,
            *extending.snapshot["messages"].as_array().unwrap()
        );
        assert_eq!(
            extending.context.1,
            *extending.snapshot["contextEntries"].as_array().unwrap()
        );
        assert!(extending.context.0.len() >= 2 * turns);
        // Whole histories projected: once per turn, for its statistics.
        assert!(
            extending.projections <= turns,
            "{} projections for {turns} turns",
            extending.projections
        );

        // A client that does not say it extends its context gets all of it with every append.
        let whole = grow(&root, false, turns).await;
        let (second, last) = (bytes(&whole.turns[1]), bytes(&whole.turns[turns - 1]));
        // Every earlier turn is sent again, on top of what the second turn carried.
        let added = whole.turns[1]
            .iter()
            .map(|frame| {
                frame["params"]["historyChange"]["appended"]
                    .to_string()
                    .len()
            })
            .sum::<usize>();
        assert!(
            last >= second + (turns - 2) * added,
            "{second} bytes grew only to {last} ({added} added per turn)"
        );
        for frame in whole.turns.iter().flatten() {
            assert!(frame["params"]["changes"]["messages"].is_array());
            assert!(
                frame["params"]["historyChange"]
                    .get("contextAppended")
                    .is_none()
            );
        }
        assert!(whole.whole >= 3 * turns, "{} whole contexts", whole.whole);
        assert_eq!(
            whole.context.0,
            *whole.snapshot["messages"].as_array().unwrap()
        );
        assert!(
            whole.projections >= 2 * turns,
            "{} projections for {turns} turns",
            whole.projections
        );
    }

    struct Input(mpsc::UnboundedReceiver<std::io::Result<Vec<u8>>>);
    impl AsyncRead for Input {
        fn poll_read(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            self.0.poll_recv(cx).map(|item| match item {
                Some(Ok(bytes)) => {
                    buf.put_slice(&bytes);
                    Ok(())
                }
                Some(Err(error)) => Err(error),
                None => Ok(()),
            })
        }
    }

    #[tokio::test]
    async fn input_read_error_shuts_down_and_settles_requests_before_returning() {
        let root = Root::new();
        let (lines, input) = mpsc::unbounded_channel();
        let (output, server) = tokio::io::duplex(1 << 16);
        let served = tokio::spawn(serve(root.application().await, Input(input), server));
        let mut output = BufReader::new(output).lines();
        let send = |value: Value| lines.send(Ok(format!("{value}\n").into_bytes())).unwrap();
        send(
            json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":2,"info":{"name":"unit","version":"1"},"capabilities":{}}}),
        );
        send(
            json!({"jsonrpc":"2.0","id":2,"method":"session/new","params":{"cwd":root.0.to_string_lossy()}}),
        );
        let s = loop {
            let frame: Value =
                serde_json::from_str(&output.next_line().await.unwrap().unwrap()).unwrap();
            if frame["id"] == 2 {
                break frame["result"]["sessionId"].as_str().unwrap().to_owned();
            }
        };
        send(
            json!({"jsonrpc":"2.0","id":3,"method":"session/prompt","params":{"sessionId":s,"prompt":[{"type":"text","text":"fixture:before-insert"}]}}),
        );
        lines
            .send(Err(std::io::Error::other("synthetic read failure")))
            .unwrap();
        let mut frames = vec![];
        let closed = tokio::time::timeout(Duration::from_secs(10), async {
            while let Some(line) = output.next_line().await.unwrap() {
                frames.push(serde_json::from_str::<Value>(&line).unwrap());
            }
        })
        .await;
        assert!(
            closed.is_ok(),
            "output must close after shutdown: {frames:?}"
        );
        assert!(
            frames
                .iter()
                .any(|v| v["id"] == 3 && v["error"]["code"] == -32603),
            "the in-flight prompt must be settled: {frames:?}"
        );
        let error = served
            .await
            .unwrap()
            .expect_err("the read error is returned");
        assert_eq!(error.to_string(), "synthetic read failure");
    }

    fn interaction() -> (
        Interaction,
        mpsc::UnboundedReceiver<Value>,
        Reverse,
        CancellationToken,
    ) {
        let (sender, received) = mpsc::unbounded_channel();
        let reverse = Reverse::default();
        let login = CancellationToken::new();
        let interaction = Interaction {
            output: Output {
                sender,
                queued: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                closed: CancellationToken::new(),
            },
            reverse: reverse.clone(),
            request: json!(7),
            next: std::sync::atomic::AtomicU64::new(0),
            login: login.clone(),
        };
        (interaction, received, reverse, login)
    }
    fn secret() -> AuthPrompt {
        AuthPrompt::Secret {
            message: "Enter the synthetic key".into(),
            placeholder: None,
        }
    }
    /// What the receive pump does with a client's answer to a reverse request.
    fn answer(reverse: &Reverse, id: &Value, result: Value) {
        let (tx, _rx) = mpsc::unbounded_channel();
        let frame = json!({"jsonrpc":"2.0","id":id,"result":result});
        assert!(route(frame, &tx, reverse).is_none());
    }

    #[test]
    fn a_withdrawn_queue_admission_is_a_protocol_cancellation() {
        assert_eq!(queue_error(SessionError::Cancelled).code, -32800);
        let failed = queue_error(SessionError::Invalid("queued start rejected".into()));
        assert_ne!(failed.code, -32800);
        assert_eq!(failed.message, "queued start rejected");
    }
    #[test]
    fn auth_notifications_preserve_each_non_secret_event_without_waiting() {
        let (interaction, mut received, reverse, _) = interaction();
        for (event, shown) in [
            (
                json!({"type":"auth_url","url":"https://example.invalid/authorize","instructions":"Open the link"}),
                vec!["Open the link", "https://example.invalid/authorize"],
            ),
            // A client that ignores `_meta` must still be able to finish the flow.
            (
                json!({"type":"device_code","userCode":"SYNTHETIC","verificationUri":"https://example.invalid/device","intervalSeconds":5,"expiresInSeconds":60}),
                vec!["SYNTHETIC", "https://example.invalid/device", "60"],
            ),
            (
                json!({"type":"info","message":"Read the instructions","links":[{"label":"Help","url":"https://example.invalid/help"},{"url":"https://example.invalid/bare"}]}),
                vec![
                    "Read the instructions",
                    "Help",
                    "https://example.invalid/help",
                    "https://example.invalid/bare",
                ],
            ),
            (
                json!({"type":"progress","message":"Waiting for synthetic authorization"}),
                vec!["Waiting for synthetic authorization"],
            ),
        ] {
            interaction
                .notify(serde_json::from_value(event.clone()).unwrap())
                .unwrap();
            let frame = received
                .try_recv()
                .expect("notify must emit without a client reply");
            assert_eq!(frame["method"], "elicitation/create");
            assert_eq!(frame["params"]["mode"], "form");
            assert_eq!(frame["params"]["requestId"], 7);
            assert_eq!(frame["params"]["requestedSchema"]["properties"], json!({}));
            let message = frame["params"]["message"].as_str().unwrap();
            for part in shown {
                assert!(message.contains(part), "{part} missing from {message:?}");
            }
            assert_eq!(frame["params"]["_meta"]["pirua.dev/auth"]["event"], event);
            assert!(reverse.lock().unwrap().is_empty());
        }
    }

    #[tokio::test]
    async fn a_dropped_auth_prompt_withdraws_its_form_from_the_client() {
        use futures_util::FutureExt;
        let (interaction, mut received, reverse, _) = interaction();
        let token = CancellationToken::new();
        let mut prompt = interaction.prompt(secret(), &token);
        assert!((&mut prompt).now_or_never().is_none());
        let form = received.try_recv().expect("the form is sent");
        assert_eq!(form["method"], "elicitation/create");
        assert_eq!(reverse.lock().unwrap().len(), 1);
        // The browser callback won: the manual-code prompt future is dropped unanswered.
        drop(prompt);
        assert!(
            reverse.lock().unwrap().is_empty(),
            "the waiter must not leak"
        );
        let cancel = received
            .try_recv()
            .expect("the client is told to dismiss it");
        assert_eq!(
            cancel,
            json!({"jsonrpc":"2.0","method":"$/cancel_request","params":{"requestId":form["id"]}})
        );
        Contracts::new()
            .validate("CancelRequestNotification", &cancel["params"])
            .unwrap();
        assert!(received.try_recv().is_err());
    }

    #[tokio::test]
    async fn a_cancelled_auth_prompt_withdraws_its_form_from_the_client() {
        use futures_util::FutureExt;
        let (interaction, mut received, reverse, _) = interaction();
        let token = CancellationToken::new();
        let mut prompt = interaction.prompt(secret(), &token);
        assert!((&mut prompt).now_or_never().is_none());
        let form = received.try_recv().unwrap();
        token.cancel();
        assert!(prompt.await.is_err());
        assert!(reverse.lock().unwrap().is_empty());
        let cancel = received
            .try_recv()
            .expect("the client is told to dismiss it");
        assert_eq!(cancel["method"], "$/cancel_request");
        assert_eq!(cancel["params"]["requestId"], form["id"]);
    }

    #[tokio::test]
    async fn an_answered_auth_prompt_sends_no_cancellation() {
        use futures_util::FutureExt;
        let (interaction, mut received, reverse, login) = interaction();
        let token = CancellationToken::new();
        let mut prompt = interaction.prompt(secret(), &token);
        assert!((&mut prompt).now_or_never().is_none());
        let form = received.try_recv().unwrap();
        answer(
            &reverse,
            &form["id"],
            json!({"action":"accept","content":{"value":"synthetic"}}),
        );
        assert_eq!(prompt.await.unwrap(), "synthetic");
        assert!(received.try_recv().is_err(), "nothing is left to cancel");
        assert!(!login.is_cancelled());
    }

    #[tokio::test]
    async fn declining_or_cancelling_an_auth_form_cancels_the_login() {
        use futures_util::FutureExt;
        for reply in [
            json!({"result":{"action":"decline"}}),
            json!({"result":{"action":"cancel"}}),
            json!({"error":{"code":-32800,"message":"Request cancelled"}}),
        ] {
            let (interaction, mut received, reverse, login) = interaction();
            let token = CancellationToken::new();
            let mut prompt = interaction.prompt(secret(), &token);
            assert!((&mut prompt).now_or_never().is_none());
            let form = received.try_recv().unwrap();
            let (tx, _rx) = mpsc::unbounded_channel();
            let mut frame = json!({"jsonrpc":"2.0","id":form["id"]});
            frame
                .as_object_mut()
                .unwrap()
                .extend(reply.as_object().unwrap().clone());
            assert!(route(frame, &tx, &reverse).is_none());
            assert!(prompt.await.is_err());
            assert!(login.is_cancelled(), "{reply}: the login must be cancelled");
            assert!(received.try_recv().is_err());
        }
        // Other failures stay failures.
        for reply in [
            json!({"result":{"action":"accept"}}),
            json!({"error":{"code":-32601,"message":"Method not found"}}),
        ] {
            let (interaction, mut received, reverse, login) = interaction();
            let token = CancellationToken::new();
            let mut prompt = interaction.prompt(secret(), &token);
            assert!((&mut prompt).now_or_never().is_none());
            let form = received.try_recv().unwrap();
            let (tx, _rx) = mpsc::unbounded_channel();
            let mut frame = json!({"jsonrpc":"2.0","id":form["id"]});
            frame
                .as_object_mut()
                .unwrap()
                .extend(reply.as_object().unwrap().clone());
            assert!(route(frame, &tx, &reverse).is_none());
            assert!(prompt.await.is_err());
            assert!(!login.is_cancelled(), "{reply}: not a cancellation");
        }
    }

    /// A host that has negotiated form elicitation over the production registry, with
    /// nothing running: the test plays the actor loop.
    async fn authenticating_host(root: &Root) -> (Host, mpsc::UnboundedReceiver<Value>) {
        let application = Application::new(crate::Options {
            data_dir: root.0.join("data"),
            config_dir: root.0.join("config"),
            credentials: None,
            model: None,
            fixture_provider: false,
            fixture_delay_ms: 20,
        })
        .await
        .unwrap();
        let (sender, output) = mpsc::unbounded_channel();
        let (tx, rx) = mpsc::unbounded_channel();
        let mut host = Host {
            application: Arc::new(application),
            contracts: Contracts::new(),
            tx,
            rx,
            output: Output {
                sender,
                queued: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                closed: CancellationToken::new(),
            },
            reverse: Reverse::default(),
            initialized: false,
            pi: false,
            context_append: false,
            elicitation: false,
            owners: BTreeMap::new(),
            pending: BTreeMap::new(),
            tasks: JoinSet::new(),
            auth: BTreeMap::new(),
            auth_generation: 0,
            auth_methods: vec![],
        };
        let (reply, wait) = oneshot::channel();
        host.call(Call {
            id: json!(1),
            method: "initialize".into(),
            params: json!({"protocolVersion":2,"info":{"name":"unit","version":"1"},"capabilities":{"elicitation":{"form":{}}}}),
            reply,
        })
        .await;
        assert!(
            !wait.await.unwrap().unwrap()["authMethods"]
                .as_array()
                .unwrap()
                .is_empty()
        );
        (host, output)
    }

    #[tokio::test]
    async fn a_finished_auth_request_leaves_no_cancellation_behind() {
        let root = Root::new();
        let (mut host, mut output) = authenticating_host(&root).await;
        for (id, method, params) in [
            (2, "auth/logout", json!({})),
            (3, "auth/login", json!({"methodId":"deepseek:apiKey"})),
        ] {
            let (reply, wait) = oneshot::channel();
            host.call(Call {
                id: json!(id),
                method: method.into(),
                params,
                reply,
            })
            .await;
            assert!(host.auth.contains_key(&id.to_string()));
            if method == "auth/login" {
                let form = loop {
                    let frame = output.recv().await.unwrap();
                    if frame["method"] == "elicitation/create" {
                        break frame;
                    }
                };
                answer(&host.reverse, &form["id"], json!({"action":"decline"}));
            }
            // The task settles the connection before it replies.
            let Some(Event::Auth(key, generation, methods)) = host.rx.recv().await else {
                panic!("the finished task must report to the actor");
            };
            assert_eq!(key, id.to_string());
            host.auth_finished(&key, generation, methods);
            assert!(host.auth.is_empty(), "request {id} left its token");
            let reply = wait.await.unwrap();
            if method == "auth/login" {
                assert_eq!(reply.unwrap_err().code, -32800);
            } else {
                assert_eq!(reply.unwrap(), json!({}));
            }
        }
    }

    #[tokio::test]
    async fn a_stored_oauth_credential_is_reported_as_the_providers_status() {
        let root = Root::new();
        std::fs::create_dir_all(&root.0).unwrap();
        let credentials = root.0.join("credentials.json");
        std::fs::write(&credentials, json!({"anthropic":{"type":"oauth","refresh":"synthetic-refresh","access":"synthetic-access","expires":4102444800000u64}}).to_string()).unwrap();
        let application = Application::new(crate::Options {
            data_dir: root.0.join("data"),
            config_dir: root.0.join("config"),
            credentials: Some(credentials),
            model: None,
            fixture_provider: false,
            fixture_delay_ms: 20,
        })
        .await
        .unwrap();
        let methods = auth_methods(application.registry.as_ref().unwrap()).await;
        // Provider-level, like the original selector: both of its methods show the OAuth in use.
        for id in ["anthropic:oauth", "anthropic:apiKey"] {
            let meta =
                &methods.iter().find(|m| m["methodId"] == id).unwrap()["_meta"]["pirua.dev/auth"];
            assert_eq!(meta["subscription"], true, "{id}");
            assert_eq!(
                meta["status"],
                json!({"type":"oauth","source":"OAuth"}),
                "{id}"
            );
        }
        assert!(!json!(methods).to_string().contains("synthetic-"));
    }

    #[tokio::test]
    async fn a_reused_request_id_keeps_its_current_cancellation() {
        let root = Root::new();
        let (mut host, _output) = authenticating_host(&root).await;
        let (earlier, _) = host.begin_auth("5");
        let (current, token) = host.begin_auth("5");
        // The earlier run ends after its id was reused: the current run stays cancellable.
        host.auth_finished("5", earlier, vec![]);
        assert!(host.auth.contains_key("5"));
        assert!(!token.is_cancelled());
        host.auth_finished("5", current, vec![]);
        assert!(host.auth.is_empty());
    }
}
