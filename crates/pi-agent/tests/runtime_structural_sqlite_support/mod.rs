//! Actual public structural SQLite fixture; no copied state machine or fake commit.
use pi_agent_schema::*;
use pi_agent_session::{
    context::{ContextSummary, SummaryRenderer},
    memory::StorageClock,
};
use pi_agent_sqlite::{
    SqliteOptions, SqliteStorage,
    repo::{SqliteSession, SqliteSessionMetadata, SqliteSessionRepo},
};
use pi_ai_core::*;
use pi_harness_runtime::owner::{RuntimeFactory, RuntimeHarness};
use std::{
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    task::{Context as TaskContext, Poll, Waker},
    time::Duration,
};
pub fn n(v: u64) -> SafeUnsigned {
    SafeUnsigned::new(v).unwrap()
}
pub fn poll<F: Future + ?Sized>(f: Pin<&mut F>) -> Poll<F::Output> {
    f.poll(&mut TaskContext::from_waker(Waker::noop()))
}
#[derive(Default)]
pub struct Pump(Mutex<Vec<pi_ai_core::BoxFuture<'static, ()>>>);
impl TaskSpawner for Pump {
    fn spawn(&self, f: pi_ai_core::BoxFuture<'static, ()>) -> Result<(), SpawnError> {
        self.0.lock().unwrap().push(f);
        Ok(())
    }
}
impl Pump {
    pub fn tick(&self) {
        let tasks = std::mem::take(&mut *self.0.lock().unwrap());
        for mut f in tasks {
            if poll(f.as_mut()).is_pending() {
                self.0.lock().unwrap().push(f);
            }
        }
    }
    pub fn drive<F: Future>(&self, f: F) -> F::Output {
        let mut f = std::pin::pin!(f);
        for _ in 0..2000 {
            if let Poll::Ready(v) = poll(f.as_mut()) {
                return v;
            }
            self.tick();
        }
        panic!("public fixture did not complete")
    }
    pub fn until(&self, condition: impl Fn() -> bool) {
        for _ in 0..2000 {
            if condition() {
                return;
            }
            self.tick();
        }
        panic!("public boundary not reached")
    }
}
pub struct Time(pub AtomicU64);
impl Clock for Time {
    fn now_unix_ms(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
    fn sleep<'a>(
        &'a self,
        _: Duration,
        c: &'a CancellationToken,
    ) -> pi_ai_core::BoxFuture<'a, Result<(), Cancelled>> {
        Box::pin(async move {
            c.cancelled().await;
            Err(Cancelled)
        })
    }
}
impl StorageClock for Time {
    fn now(&self) -> Result<SafeUnsigned, StorageError> {
        Ok(n(self.0.load(Ordering::SeqCst)))
    }
}
#[derive(Default)]
struct Ids(AtomicU64);
impl IdGenerator for Ids {
    fn next(&self, _: Option<SafeUnsigned>) -> String {
        format!("structural-{}", self.0.fetch_add(1, Ordering::SeqCst))
    }
}
struct Host;
impl AuthContext for Host {
    fn env<'a>(
        &'a self,
        _: &'a str,
        _: &'a CancellationToken,
    ) -> pi_ai_core::BoxFuture<'a, Result<Option<String>, AuthContextError>> {
        Box::pin(async { Ok(None) })
    }
    fn file_exists<'a>(
        &'a self,
        _: &'a str,
        _: &'a CancellationToken,
    ) -> pi_ai_core::BoxFuture<'a, Result<bool, AuthContextError>> {
        Box::pin(async { Ok(false) })
    }
}
struct Auth;
impl ApiKeyAuth for Auth {
    fn name(&self) -> &str {
        "offline-projection"
    }
    fn resolve<'a>(
        &'a self,
        _: ApiKeyAuthInput<'a>,
    ) -> pi_ai_core::BoxFuture<'a, Result<Option<AuthResult>, AuthProviderError>> {
        Box::pin(async {
            Ok(Some(AuthResult {
                auth: ModelAuth {
                    api_key: Some(SecretString::new("fixture")),
                    headers: None,
                    base_url: None,
                },
                env: None,
                source: None,
            }))
        })
    }
}
struct Summary;
impl SummaryRenderer for Summary {
    fn render(
        &self,
        summary: ContextSummary,
    ) -> pi_agent_schema::BoxFuture<'_, Result<AgentMessage, HarnessRuntimeError>> {
        Box::pin(async move { Ok(user(&summary.text)) })
    }
}
fn model() -> Model {
    let v: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../conformance/schema/ai/models-options.golden.json"
    ))
    .unwrap();
    serde_json::from_value(v["model"].clone()).unwrap()
}
pub struct Adapter {
    model: Model,
    pub calls: AtomicUsize,
    pub overflow_first: bool,
}
impl ApiAdapter for Adapter {
    fn api_id(&self) -> &ApiId {
        &self.model.api
    }
    fn lifecycle_adapter(&self) -> Option<&dyn RequestLifecycleAdapter> {
        Some(self)
    }
    fn stream(&self, _: TextStreamRequest) -> AssistantStream {
        panic!("unexpected raw request")
    }
    fn stream_simple(&self, _: SimpleStreamRequest) -> AssistantStream {
        panic!("expected lifecycle request")
    }
    fn fetch_deferred(&self, _: DeferredFetchRequest) -> Result<AssistantStream, CapabilityError> {
        Err(CapabilityError::Unsupported)
    }
    fn cancel_deferred<'a>(
        &'a self,
        _: DeferredCancelRequest,
    ) -> pi_ai_core::BoxFuture<'a, Result<(), ModelsError>> {
        Box::pin(async { Ok(()) })
    }
}
impl RequestLifecycleAdapter for Adapter {
    fn stream_with_lifecycle(
        &self,
        _: TextStreamRequest,
        _: RequestLifecycleInvocation,
    ) -> AssistantStream {
        panic!("unexpected raw lifecycle")
    }
    fn stream_simple_with_lifecycle(
        &self,
        r: SimpleStreamRequest,
        _: RequestLifecycleInvocation,
    ) -> AssistantStream {
        let overflow = self.calls.fetch_add(1, Ordering::SeqCst) == 0 && self.overflow_first;
        let model = r.resolved.model;
        let token = r.options.runtime.cancellation.clone();
        let (stream, producer) = create_assistant_message_event_stream(AssistantStreamInit {
            api: model.api.clone(),
            provider: model.provider.clone(),
            model: model.id.clone(),
            clock: r.options.runtime.clock,
            cancellation: token.clone(),
        });
        r.options
            .runtime
            .spawner
            .spawn(Box::pin(async move {
                if token.is_cancelled() {
                    return;
                }
                let message = AssistantMessage {
                    role: Default::default(),
                    content: vec![AssistantContent::Text(TextContent {
                        text: if overflow {
                            "OVERFLOW-MUST-NOT-PROJECT"
                        } else {
                            "answer"
                        }
                        .into(),
                        content_type: Default::default(),
                        text_signature: None,
                    })],
                    api: model.api,
                    provider: model.provider,
                    model: model.id,
                    response_model: None,
                    response_id: None,
                    diagnostics: None,
                    usage: if overflow { usage(11, 2) } else { usage(4, 1) },
                    stop_reason: if overflow {
                        StopReason::Error
                    } else {
                        StopReason::Stop
                    },
                    deferred: None,
                    error_message: overflow.then(|| "maximum context length exceeded".into()),
                    raw_stop_reason: None,
                    end_turn: None,
                    timestamp: n(7),
                };
                if overflow {
                    producer
                        .push(AssistantMessageEvent::Error {
                            reason: ErrorReason::Error,
                            error: Box::new(message),
                        })
                        .unwrap();
                } else {
                    producer
                        .push(AssistantMessageEvent::Done {
                            reason: DoneReason::Stop,
                            message: Box::new(message),
                        })
                        .unwrap();
                }
            }))
            .unwrap();
        stream
    }
}
struct Product {
    adapter: Arc<Adapter>,
    auth: ProviderAuth,
}
impl Provider for Product {
    fn id(&self) -> &ProviderId {
        &self.adapter.model.provider
    }
    fn name(&self) -> &str {
        "sqlite-structural"
    }
    fn models(&self) -> Result<Arc<[Model]>, ModelsError> {
        Ok(vec![self.adapter.model.clone()].into())
    }
    fn auth(&self) -> &ProviderAuth {
        &self.auth
    }
    fn adapter(&self, _: &ApiId) -> Option<Arc<dyn ApiAdapter>> {
        Some(self.adapter.clone())
    }
}
pub struct Dir(pub PathBuf);
impl Dir {
    fn new() -> Self {
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "harness-sqlite-structural-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
pub type Harness = RuntimeHarness<(), SqliteStorage, SqliteSessionMetadata>;
pub struct World {
    pub pump: Arc<Pump>,
    pub time: Arc<Time>,
    pub session: Arc<SqliteSession>,
    pub harness: Arc<Harness>,
    pub adapter: Arc<Adapter>,
    pub events: Arc<EventLog>,
    pub hooks: Arc<StructuralHooks>,
    repo: SqliteSessionRepo,
    models: Arc<ModelsRegistry>,
    ids: Arc<Ids>,
    _dir: Dir,
    _listeners: Vec<Arc<dyn EventSubscription>>,
    _handlers: Vec<Arc<dyn HookSubscription>>,
}
impl World {
    pub fn new(overflow_first: bool) -> Self {
        let dir = Dir::new();
        let pump = Arc::new(Pump::default());
        let time = Arc::new(Time(AtomicU64::new(7)));
        let ids = Arc::new(Ids::default());
        let repo = SqliteSessionRepo::new(
            &dir.0,
            SessionCodecOptions::default(),
            ProviderHeaderPolicy::default(),
            ids.clone(),
            time.clone(),
            Arc::new(pi_telemetry::NoopTelemetryContext),
            SqliteOptions {
                lease_ttl_ms: n(120000),
                heartbeat_ms: n(60000),
                ..Default::default()
            },
        )
        .unwrap();
        let session = pump
            .drive(repo.create(SessionCreateOptions {
                id: Some(SessionId::new("sqlite-structural").unwrap()),
                ..Default::default()
            }))
            .unwrap();
        let models = Arc::new(ModelsRegistry::new_with_child_cancellation(
            ModelsRegistryOptions {
                credentials: Arc::new(InMemoryCredentialStore::new()),
                models_store: Box::new(InMemoryModelsStore::new()),
                auth_context: Arc::new(Host),
                clock: time.clone(),
                spawner: pump.clone(),
                model_header_policy: Default::default(),
            },
        ));
        let adapter = Arc::new(Adapter {
            model: model(),
            calls: AtomicUsize::new(0),
            overflow_first,
        });
        pump.drive(models.set_provider(Arc::new(Product {
            adapter: adapter.clone(),
            auth: ProviderAuth::new(Some(Arc::new(Auth)), None).unwrap(),
        })))
        .unwrap();
        let factory = RuntimeFactory::new(
            time.clone(),
            pump.clone(),
            ids.clone(),
            Arc::new(()),
            Arc::new(Summary),
        );
        let harness = pump
            .drive(factory.create_first_attachment(options(session.clone(), models.clone())))
            .unwrap()
            .harness;
        let events = Arc::new(EventLog::default());
        let hooks = Arc::new(StructuralHooks::default());
        let listeners = [
            HarnessEventType::EntryAdded,
            HarnessEventType::Usage,
            HarnessEventType::CompactionEnd,
            HarnessEventType::NavigationEnd,
        ]
        .into_iter()
        .map(|tag| harness.events().on(tag, events.clone()))
        .collect();
        let handlers = vec![
            harness
                .hooks()
                .before_compaction(None, hooks.clone())
                .unwrap(),
            harness
                .hooks()
                .before_navigation(None, hooks.clone())
                .unwrap(),
        ];
        Self {
            pump,
            time,
            session,
            harness,
            adapter,
            events,
            hooks,
            repo,
            models,
            ids,
            _dir: dir,
            _listeners: listeners,
            _handlers: handlers,
        }
    }
    pub fn append(&self, text: &str) -> EntryId {
        self.pump
            .drive(self.harness.session().append_message(user(text)))
            .unwrap()
    }
    pub fn register(&self, namespace: RegisterNamespace, key: &str) -> Option<Register> {
        self.pump
            .drive(self.session.get_register(namespace, key))
            .unwrap()
    }
    pub fn current(&self) -> (OperationId, OperationState) {
        let Some(Register::LaneState { value, .. }) =
            self.register(RegisterNamespace::LaneState, "main")
        else {
            panic!("lane state")
        };
        let id = value.current_operation_id.expect("active public operation");
        let Some(Register::OperationState { value, .. }) =
            self.register(RegisterNamespace::OperationState, id.as_str())
        else {
            panic!("operation state")
        };
        (id, *value)
    }
    pub fn path(&self) -> PathBuf {
        self.session.metadata().path
    }
    pub fn until_gate<F: Future>(&self, call: Pin<&mut F>, predicate: impl Fn(&str) -> bool)
    where
        F::Output: std::fmt::Debug,
    {
        let mut call = call;
        for _ in 0..500 {
            if let Poll::Ready(result) = poll(call.as_mut()) {
                panic!("operation ended before required public gate: {result:?}")
            }
            self.pump.tick();
            if let Some(action) = self.pump.drive(self.harness.peek_action()).unwrap() {
                if predicate(&action.kind) {
                    return;
                }
                self.pump.drive(self.harness.execute_action()).unwrap();
            }
        }
        panic!("required public gate not reached")
    }
    pub fn finish<F: Future>(&self, call: Pin<&mut F>) -> F::Output {
        let mut call = call;
        for _ in 0..500 {
            if let Poll::Ready(result) = poll(call.as_mut()) {
                return result;
            }
            self.pump.tick();
            if self
                .pump
                .drive(self.harness.peek_action())
                .is_ok_and(|a| a.is_some())
            {
                let _ = self.pump.drive(self.harness.execute_action());
            }
        }
        panic!("public operation did not reach terminal")
    }
    pub fn reopen(&mut self) -> Vec<SuspendedOperation> {
        self.pump.drive(self.harness.close()).unwrap();
        self.pump.drive(self.session.close()).unwrap();
        self.session = self.pump.drive(self.repo.open_path(self.path())).unwrap();
        let factory = RuntimeFactory::new(
            self.time.clone(),
            self.pump.clone(),
            self.ids.clone(),
            Arc::new(()),
            Arc::new(Summary),
        );
        let restored = self
            .pump
            .drive(factory.create(options(self.session.clone(), self.models.clone())))
            .unwrap();
        self.harness = restored.harness;
        restored.suspended
    }
}
fn options(
    session: Arc<SqliteSession>,
    models: Arc<ModelsRegistry>,
) -> AgentHarnessOptions<(), Arc<dyn pi_telemetry::TelemetryContext>, SqliteSession, ModelsRegistry>
{
    AgentHarnessOptions {
        session,
        models,
        model: model(),
        thinking_level: None,
        active_tool_names: None,
        tools: vec![],
        tool_context: None,
        system_prompt: None,
        resources: Default::default(),
        stream_options: Default::default(),
        retry: RetryPolicy {
            enabled: false,
            max_retries: MaxRetries::new(0).unwrap(),
            base_delay_ms: n(1),
        },
        compaction: CompactionSettings {
            enabled: false,
            reserve_tokens: n(100),
            keep_recent_tokens: n(1),
        },
        steering_mode: QueueMode::All,
        follow_up_mode: QueueMode::All,
        tool_execution: ToolExecutionMode::Parallel,
        drive: DriveMode::Manual,
        to_provider_messages: None,
        entry_projectors: Default::default(),
        telemetry_context: Arc::new(pi_telemetry::NoopTelemetryContext),
    }
}
impl Drop for World {
    fn drop(&mut self) {
        let _ = self.pump.drive(self.harness.close());
        let _ = self.pump.drive(self.session.close());
    }
}
pub fn user(text: &str) -> AgentMessage {
    serde_json::from_value(serde_json::json!({"role":"user","content":text,"timestamp":7})).unwrap()
}
pub fn usage(input: u64, output: u64) -> Usage {
    let mut u = Usage::zero();
    u.input = n(input);
    u.output = n(output);
    u.total_tokens = n(input + output);
    u
}
#[derive(Default)]
pub struct EventLog(pub Mutex<Vec<HarnessEvent>>);
impl EventListener for EventLog {
    fn on_event(
        &self,
        event: HarnessEvent,
    ) -> pi_agent_schema::BoxFuture<'_, Result<(), HandlerError>> {
        self.0.lock().unwrap().push(event);
        Box::pin(async { Ok(()) })
    }
}
#[derive(Default)]
pub struct StructuralHooks {
    pub compactions: Mutex<Vec<BeforeCompactionEvent>>,
    pub navigations: Mutex<Vec<BeforeNavigationEvent>>,
}
impl HookHandler<BeforeCompaction> for StructuralHooks {
    fn call(
        &self,
        event: BeforeCompactionEvent,
    ) -> pi_agent_schema::BoxFuture<'_, Result<Option<BeforeCompactionResult>, HandlerError>> {
        self.compactions.lock().unwrap().push(event.clone());
        Box::pin(async move {
            Ok(Some(BeforeCompactionResult::Compaction(Box::new(
                BeforeCompactionSupply {
                    decline: None,
                    compaction: CompactResult {
                        summary: "hook compaction".into(),
                        tokens_before: event.preparation.tokens_before,
                        retained_tail: event.preparation.retained_tail,
                        usage: Some(usage(2, 1)),
                        details: Presence::Missing,
                    },
                },
            ))))
        })
    }
}
impl HookHandler<BeforeNavigation> for StructuralHooks {
    fn call(
        &self,
        event: BeforeNavigationEvent,
    ) -> pi_agent_schema::BoxFuture<'_, Result<Option<BeforeNavigationResult>, HandlerError>> {
        self.navigations.lock().unwrap().push(event);
        Box::pin(async {
            Ok(Some(BeforeNavigationResult::Summary(
                BeforeNavigationSupply {
                    decline: None,
                    summary: BranchSummaryResult {
                        summary: "hook branch".into(),
                        usage: Some(usage(2, 1)),
                        read_files: vec![],
                        modified_files: vec![],
                    },
                },
            )))
        })
    }
}
const SQL_HELPER: &str = r#"
import sqlite3,json,sys,pathlib
path=pathlib.Path(sys.argv[1])
if sys.argv[2]=='expire':
 db=sqlite3.connect(str(path)); cursor=db.execute('UPDATE writer_lease SET expires_at_ms=0 WHERE singleton=1'); assert cursor.rowcount==1; db.commit()
else:
 db=sqlite3.connect(path.as_uri()+'?mode=ro',uri=True); db.execute('BEGIN')
 names=[r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name!='writer_lease' ORDER BY name")]
 result={name:sorted([list(row) for row in db.execute('SELECT * FROM "'+name.replace('"','""')+'"')],key=repr) for name in names}
 result['__revision'],result['__next_seq']=db.execute('SELECT revision,next_seq FROM session WHERE singleton=1').fetchone()
 print(json.dumps(result,sort_keys=True,default=lambda value:{'bytes':value.hex()})); db.rollback()
db.close()
"#;
pub fn database(path: &Path, expire: bool) -> Option<serde_json::Value> {
    let output = std::process::Command::new("python3")
        .arg("-c")
        .arg(SQL_HELPER)
        .arg(path)
        .arg(if expire { "expire" } else { "read" })
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "private sqlite helper failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    if expire {
        None
    } else {
        Some(serde_json::from_slice(&output.stdout).unwrap())
    }
}

pub fn ledger(snapshot: &serde_json::Value) -> Vec<UsageRow> {
    snapshot["usage_ledger"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| UsageRow {
            id: UsageId::new(row[0].as_str().unwrap()).unwrap(),
            seq: n(row[1].as_u64().unwrap()),
            entry_id: row[2].as_str().map(|s| EntryId::new(s).unwrap()),
            adjustment: row[3].as_u64().unwrap() != 0,
            usage: serde_json::from_str(row[4].as_str().unwrap()).unwrap(),
            details: match row[5].as_str() {
                None => Presence::Missing,
                Some("null") => Presence::Null,
                Some(value) => Presence::Value(serde_json::from_str(value).unwrap()),
            },
        })
        .collect()
}
