//! Actual SQLite/public Harness fixture; no copied production state algorithm.
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
        format!("projection-{}", self.0.fetch_add(1, Ordering::SeqCst))
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
        _: ContextSummary,
    ) -> pi_agent_schema::BoxFuture<'_, Result<AgentMessage, HarnessRuntimeError>> {
        Box::pin(async { panic!("no structural operation in projection proof") })
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
    pub release_first: CancellationToken,
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
        let first = self.calls.fetch_add(1, Ordering::SeqCst) == 0;
        let release = self.release_first.clone();
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
                if first {
                    std::future::poll_fn(|cx| {
                        if token.is_cancelled() {
                            return Poll::Ready(());
                        }
                        let mut wait = Box::pin(release.cancelled());
                        wait.as_mut().poll(cx).map(|_| ())
                    })
                    .await;
                }
                if token.is_cancelled() {
                    return;
                }
                let message = AssistantMessage {
                    role: Default::default(),
                    content: vec![],
                    api: model.api,
                    provider: model.provider,
                    model: model.id,
                    response_model: None,
                    response_id: None,
                    diagnostics: None,
                    usage: Usage::zero(),
                    stop_reason: StopReason::Stop,
                    deferred: None,
                    error_message: None,
                    raw_stop_reason: None,
                    end_turn: None,
                    timestamp: n(7),
                };
                producer
                    .push(AssistantMessageEvent::Done {
                        reason: DoneReason::Stop,
                        message: Box::new(message),
                    })
                    .unwrap();
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
        "sqlite-projection"
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
pub struct Projector {
    pub rows: Mutex<Vec<CustomEntry>>,
    pub release: CancellationToken,
}
impl Default for Projector {
    fn default() -> Self {
        Self {
            rows: Mutex::new(vec![]),
            release: CancellationToken::new(),
        }
    }
}
impl EntryProjector for Projector {
    fn project(
        &self,
        row: CustomEntry,
    ) -> pi_agent_schema::BoxFuture<'_, Result<Option<Vec<AgentMessage>>, HarnessRuntimeError>>
    {
        self.rows.lock().unwrap().push(row.clone());
        Box::pin(async move {
            self.release.cancelled().await;
            Ok(Some(vec![serde_json::from_value(serde_json::json!({"role":"user","content":format!("{}:{}:{}",row.id,row.seq.get(),row.timestamp.get()),"timestamp":7})).unwrap()]))
        })
    }
}
pub struct Dir(pub PathBuf);
impl Dir {
    fn new() -> Self {
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "harness-sqlite-projection-{}-{}",
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
    pub projector: Arc<Projector>,
    _repo: SqliteSessionRepo,
    _dir: Dir,
}
impl World {
    pub fn new() -> Self {
        let dir = Dir::new();
        let pump = Arc::new(Pump::default());
        let time = Arc::new(Time(AtomicU64::new(7)));
        let ids = Arc::new(Ids::default());
        let telemetry: Arc<dyn pi_telemetry::TelemetryContext> =
            Arc::new(pi_telemetry::NoopTelemetryContext);
        let repo = SqliteSessionRepo::new(
            &dir.0,
            SessionCodecOptions::default(),
            ProviderHeaderPolicy::default(),
            ids.clone(),
            time.clone(),
            telemetry.clone(),
            SqliteOptions {
                lease_ttl_ms: n(120000),
                heartbeat_ms: n(60000),
                ..Default::default()
            },
        )
        .unwrap();
        let session = pump
            .drive(repo.create(SessionCreateOptions {
                id: Some(SessionId::new("sqlite-projection").unwrap()),
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
            release_first: CancellationToken::new(),
        });
        pump.drive(models.set_provider(Arc::new(Product {
            adapter: adapter.clone(),
            auth: ProviderAuth::new(Some(Arc::new(Auth)), None).unwrap(),
        })))
        .unwrap();
        let projector = Arc::new(Projector::default());
        let options = AgentHarnessOptions {
            session: session.clone(),
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
                reserve_tokens: n(0),
                keep_recent_tokens: n(0),
            },
            steering_mode: QueueMode::All,
            follow_up_mode: QueueMode::All,
            tool_execution: ToolExecutionMode::Parallel,
            drive: DriveMode::Automatic,
            to_provider_messages: None,
            entry_projectors: [(
                CustomType::new("metadata-sensitive").unwrap(),
                projector.clone() as Arc<dyn EntryProjector>,
            )]
            .into(),
            telemetry_context: telemetry,
        };
        let factory = RuntimeFactory::new(
            time.clone(),
            pump.clone(),
            ids,
            Arc::new(()),
            Arc::new(Summary),
        );
        let harness = pump
            .drive(factory.create_first_attachment(options))
            .unwrap()
            .harness;
        Self {
            pump,
            time,
            session,
            harness,
            adapter,
            projector,
            _repo: repo,
            _dir: dir,
        }
    }
}
impl Drop for World {
    fn drop(&mut self) {
        let _ = self.pump.drive(self.harness.close());
        let _ = self.pump.drive(self.session.close());
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
