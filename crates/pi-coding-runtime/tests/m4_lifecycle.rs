use pi_agent::schema::{
    AgentLane, AgentMessage, BranchForkOptions, BranchScan, CommitResult, CompactionSettings,
    Entry, EntryId, EntryMap, EntryScan, EntryStructure, ForkOptions, HarnessRuntimeError,
    LaneConfiguration, LaneModel, LaneState, ProviderHeaderPolicy, Register, RegisterNamespace,
    RegisterSet, Session, SessionCodecOptions, SessionCreateOptions, SessionForkRequest,
    SessionMetadata, SessionRepo, SessionStats, SessionTree, Storage, StorageError, Transaction,
    UsageRow, UsageScan, Write,
};
use pi_agent_session::{
    codec::SessionCodec,
    context::SummaryRenderer,
    jsonl::{JsonlOptions, JsonlStorage},
    memory::{MemoryBacking, MemoryStorage, StorageClock},
    repo::{
        MemorySessionRepo,
        jsonl::{JsonlSession, JsonlSessionMetadata, JsonlSessionRepo},
    },
    session::RuntimeSession,
    storage::{
        AtomicStorage, CheckedCommitError, CheckedStorage, ConditionalCommit, ConditionalOutcome,
        PublicationValidator, ReadStamp, RuntimeAttachment, RuntimeStorage,
    },
};

use pi_ai::{
    ApiAdapter, ApiId, ApiKeyAuth, ApiKeyAuthInput, AuthProviderError, AuthResult, BoxFuture,
    Clock, Message, Model, ModelBaseUrl, ModelCost, ModelId, ModelInput, ModelsError,
    ModelsRegistry, ModelsRegistryOptions, Provider, ProviderAuth, ProviderId, PublicModelBaseUrl,
    SafeUnsigned, StreamOptionsWire, TaskSpawner, ToolName, UserContent, UserMessage,
};
use pi_coding_runtime::{
    CodingHarnessBuilder, CodingHarnessError, CodingSessionLifecycle, CodingSettingsSnapshot,
    ExecutionEnv, LegacyImportSource, LocalExecutionEnv, ModelRef, Resources, ThinkingLevel,
};
use pi_harness_runtime::owner::RuntimeFactory;
use pi_telemetry::NOOP_TELEMETRY_CONTEXT;
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

struct FailOnceStorage {
    inner: Arc<MemoryStorage>,
    fail_close: std::sync::atomic::AtomicBool,
}
impl FailOnceStorage {
    fn new(inner: Arc<MemoryStorage>) -> Self {
        Self {
            inner,
            fail_close: std::sync::atomic::AtomicBool::new(true),
        }
    }
}
impl Storage for FailOnceStorage {
    fn commit(
        &self,
        transaction: Transaction,
    ) -> BoxFuture<'_, Result<CommitResult, StorageError>> {
        self.inner.commit(transaction)
    }
    fn get_entries<'a>(
        &'a self,
        ids: &'a [EntryId],
    ) -> BoxFuture<'a, Result<EntryMap, StorageError>> {
        self.inner.get_entries(ids)
    }
    fn get_register<'a>(
        &'a self,
        namespace: RegisterNamespace,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Option<Register>, StorageError>> {
        self.inner.get_register(namespace, key)
    }
    fn list_registers<'a>(
        &'a self,
        namespace: RegisterNamespace,
        prefix: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Vec<Register>, StorageError>> {
        self.inner.list_registers(namespace, prefix)
    }
    fn scan_branch(&self, query: BranchScan) -> BoxFuture<'_, Result<Vec<Entry>, StorageError>> {
        self.inner.scan_branch(query)
    }
    fn scan_branch_structure(
        &self,
        query: BranchScan,
    ) -> BoxFuture<'_, Result<Vec<EntryStructure>, StorageError>> {
        self.inner.scan_branch_structure(query)
    }
    fn scan_entries(&self, query: EntryScan) -> BoxFuture<'_, Result<Vec<Entry>, StorageError>> {
        self.inner.scan_entries(query)
    }
    fn scan_usage(&self, query: UsageScan) -> BoxFuture<'_, Result<Vec<UsageRow>, StorageError>> {
        self.inner.scan_usage(query)
    }
    fn stats(&self) -> BoxFuture<'_, Result<SessionStats, StorageError>> {
        self.inner.stats()
    }
    fn close(&self) -> BoxFuture<'_, Result<(), StorageError>> {
        if self.fail_close.swap(false, Ordering::SeqCst) {
            Box::pin(async {
                Err(StorageError::Io(
                    pi_agent::schema::ErrorContext::new("injected close failure").unwrap(),
                ))
            })
        } else {
            self.inner.close()
        }
    }
}
impl AtomicStorage for FailOnceStorage {
    fn commit_conditional(
        &self,
        request: ConditionalCommit,
    ) -> BoxFuture<'_, Result<ConditionalOutcome, StorageError>> {
        self.inner.commit_conditional(request)
    }
    fn snapshot(
        &self,
    ) -> BoxFuture<'_, Result<pi_agent_session::storage::StorageSnapshot, StorageError>> {
        self.inner.snapshot()
    }
}
impl CheckedStorage for FailOnceStorage {
    fn commit_checked<'a, V: PublicationValidator + ?Sized>(
        &'a self,
        request: ConditionalCommit,
        validator: &'a V,
    ) -> BoxFuture<'a, Result<ConditionalOutcome, CheckedCommitError<V::Error>>> {
        self.inner.commit_checked(request, validator)
    }
}
impl RuntimeStorage for FailOnceStorage {
    fn runtime_attachment(&self) -> &RuntimeAttachment {
        self.inner.runtime_attachment()
    }
    fn read_stamp(&self) -> BoxFuture<'_, Result<ReadStamp, StorageError>> {
        self.inner.read_stamp()
    }
}

#[derive(Default)]
struct Ids(AtomicUsize);
impl pi_agent::schema::IdGenerator for Ids {
    fn next(&self, _: Option<SafeUnsigned>) -> String {
        format!("m4-id-{}", self.0.fetch_add(1, Ordering::SeqCst))
    }
}

struct ClockPort;
impl Clock for ClockPort {
    fn now_unix_ms(&self) -> u64 {
        7
    }
    fn sleep<'a>(
        &'a self,
        _: Duration,
        cancellation: &'a pi_ai::CancellationToken,
    ) -> pi_ai::BoxFuture<'a, Result<(), pi_ai::Cancelled>> {
        Box::pin(async move {
            if cancellation.is_cancelled() {
                Err(pi_ai::Cancelled)
            } else {
                Ok(())
            }
        })
    }
}
impl StorageClock for ClockPort {
    fn now(&self) -> Result<SafeUnsigned, pi_agent::schema::StorageError> {
        Ok(SafeUnsigned::new(7).expect("bounded fixture time"))
    }
}

struct TokioSpawner;
impl TaskSpawner for TokioSpawner {
    fn spawn(&self, task: pi_ai::BoxFuture<'static, ()>) -> Result<(), pi_ai::SpawnError> {
        tokio::spawn(task);
        Ok(())
    }
}

struct AuthHost;
impl pi_ai::AuthContext for AuthHost {
    fn env<'a>(
        &'a self,
        _: &'a str,
        _: &'a pi_ai::CancellationToken,
    ) -> pi_ai::BoxFuture<'a, Result<Option<String>, pi_ai::AuthContextError>> {
        Box::pin(async { Ok(None) })
    }
    fn file_exists<'a>(
        &'a self,
        _: &'a str,
        _: &'a pi_ai::CancellationToken,
    ) -> pi_ai::BoxFuture<'a, Result<bool, pi_ai::AuthContextError>> {
        Box::pin(async { Ok(false) })
    }
}

struct Summary;
impl SummaryRenderer for Summary {
    fn render(
        &self,
        summary: pi_agent_session::context::ContextSummary,
    ) -> pi_agent::schema::BoxFuture<'_, Result<AgentMessage, HarnessRuntimeError>> {
        Box::pin(async move {
            Ok(AgentMessage::BuiltIn(Message::User(UserMessage {
                role: Default::default(),
                content: UserContent::Text(summary.text),
                timestamp: summary.timestamp,
            })))
        })
    }
}

struct NoopApiKey;
impl ApiKeyAuth for NoopApiKey {
    fn name(&self) -> &str {
        "m4"
    }
    fn resolve<'a>(
        &'a self,
        _: ApiKeyAuthInput<'a>,
    ) -> BoxFuture<'a, Result<Option<AuthResult>, AuthProviderError>> {
        Box::pin(async { Ok(None) })
    }
}

struct FixtureProvider {
    id: ProviderId,
    models: Vec<Model>,
    auth: ProviderAuth,
}
impl Provider for FixtureProvider {
    fn id(&self) -> &ProviderId {
        &self.id
    }
    fn name(&self) -> &str {
        self.id.as_str()
    }
    fn auth(&self) -> &ProviderAuth {
        &self.auth
    }
    fn models(&self) -> Result<Arc<[Model]>, ModelsError> {
        Ok(self.models.clone().into())
    }
    fn adapter(&self, _: &ApiId) -> Option<Arc<dyn ApiAdapter>> {
        None
    }
}

fn model(id: &str) -> Model {
    Model {
        id: ModelId::new(id).expect("model id"),
        name: id.to_owned(),
        api: ApiId::new(ApiId::OPENAI_COMPLETIONS).expect("api"),
        provider: ProviderId::new("openai").expect("provider"),
        base_url: ModelBaseUrl::Public(
            PublicModelBaseUrl::new("https://example.invalid").expect("url"),
        ),
        reasoning: false,
        thinking_level_map: None,
        input: vec![ModelInput::Text],
        cost: ModelCost {
            input: 0.0.try_into().expect("finite"),
            output: 0.0.try_into().expect("finite"),
            cache_read: 0.0.try_into().expect("finite"),
            cache_write: 0.0.try_into().expect("finite"),
            tiers: None,
        },
        context_window: SafeUnsigned::new(1024).expect("safe"),
        max_tokens: SafeUnsigned::new(128).expect("safe"),
        sampling_params: None,
        headers: None,
        compat: None,
    }
}

async fn registry_for(
    clock: Arc<ClockPort>,
    spawner: Arc<TokioSpawner>,
    first: Model,
    second: Model,
) -> Arc<ModelsRegistry> {
    let registry = Arc::new(ModelsRegistry::new(ModelsRegistryOptions {
        credentials: Arc::new(pi_ai::InMemoryCredentialStore::new()),
        models_store: Box::new(pi_ai::InMemoryModelsStore::new()),
        auth_context: Arc::new(AuthHost),
        clock,
        spawner,
        model_header_policy: Default::default(),
    }));
    registry
        .set_provider(Arc::new(FixtureProvider {
            id: first.provider.clone(),
            models: vec![first, second],
            auth: ProviderAuth::new(Some(Arc::new(NoopApiKey)), None).expect("auth"),
        }))
        .await
        .expect("provider publication");
    registry
}

fn settings_for(model: &Model) -> CodingSettingsSnapshot {
    CodingSettingsSnapshot::new(
        ModelRef::new(model.provider.clone(), model.id.clone()),
        ThinkingLevel::Off,
        vec![ToolName::new("read").expect("tool")],
        pi_ai::RetryPolicy {
            enabled: false,
            max_retries: pi_ai::MaxRetries::new(0).expect("retries"),
            base_delay_ms: SafeUnsigned::new(0).expect("delay"),
        },
        CompactionSettings {
            enabled: false,
            reserve_tokens: SafeUnsigned::new(0).expect("reserve"),
            keep_recent_tokens: SafeUnsigned::new(0).expect("tail"),
        },
        StreamOptionsWire::default(),
        pi_coding_runtime::QueueMode::All,
        pi_coding_runtime::QueueMode::All,
    )
}

struct Fixture {
    registry: Arc<ModelsRegistry>,
    repo: MemorySessionRepo,
    factory: Arc<RuntimeFactory<pi_coding_runtime::ExecutionToolContext, MemoryStorage>>,
    session: Arc<pi_agent_session::repo::MemorySession>,
    destination: Arc<pi_agent_session::repo::MemorySession>,
    env: Arc<dyn ExecutionEnv>,
    settings: CodingSettingsSnapshot,
}

impl Fixture {
    async fn new() -> Self {
        let clock = Arc::new(ClockPort);
        let spawner = Arc::new(TokioSpawner);
        let ids = Arc::new(Ids::default());
        let first = model("m4-first");
        let second = model("m4-second");
        let registry = registry_for(clock.clone(), spawner.clone(), first.clone(), second).await;
        let repo = MemorySessionRepo::new(
            SessionCodecOptions::default(),
            ProviderHeaderPolicy::default(),
            ids.clone(),
            clock.clone(),
            Arc::new(NOOP_TELEMETRY_CONTEXT),
        )
        .expect("memory repository");
        let session = repo
            .create(SessionCreateOptions {
                id: Some("m4-session".parse().expect("session id")),
                ..Default::default()
            })
            .await
            .expect("session");
        let destination = repo
            .create(SessionCreateOptions {
                id: Some("m4-destination".parse().expect("session id")),
                ..Default::default()
            })
            .await
            .expect("destination session");
        let env: Arc<dyn ExecutionEnv> = Arc::new(LocalExecutionEnv::new(std::env::temp_dir()));
        let factory = Arc::new(RuntimeFactory::new(
            clock.clone(),
            spawner.clone(),
            ids.clone(),
            Arc::new(pi_coding_runtime::ExecutionToolContext { env: env.clone() }),
            Arc::new(Summary),
        ));
        let settings = settings_for(&first);
        Self {
            registry,
            repo,
            factory,
            session,
            destination,
            env,
            settings,
        }
    }

    fn builder(&self) -> CodingHarnessBuilder<MemoryStorage> {
        self.builder_for(self.session.clone())
    }

    fn builder_for(
        &self,
        session: Arc<pi_agent_session::repo::MemorySession>,
    ) -> CodingHarnessBuilder<MemoryStorage> {
        CodingHarnessBuilder::new(
            self.factory.clone(),
            session,
            self.registry.clone(),
            self.env.clone(),
            Resources::default(),
            self.settings.clone(),
        )
    }
}

#[test]
fn builder_first_attachment_and_lifecycle_replacement_use_one_runtime() {
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let fixture = Fixture::new().await;
        let mut lifecycle = CodingSessionLifecycle::new();
        lifecycle
            .attach(fixture.builder().first_attachment())
            .await
            .expect("first attach");
        assert!(
            fixture.session.claim_runtime().is_err(),
            "one runtime claim is active"
        );
        assert_eq!(
            lifecycle
                .current()
                .unwrap()
                .harness()
                .get_active_tools()
                .await
                .unwrap(),
            vec![ToolName::new("read").unwrap()]
        );
        assert!(matches!(
            fixture.builder().build().await,
            Err(CodingHarnessError::Runtime(_))
        ));

        if let Err(error) = lifecycle
            .replace(
                fixture
                    .builder_for(fixture.destination.clone())
                    .first_attachment(),
            )
            .await
        {
            panic!("replacement failed: {error}");
        }
        let current = lifecycle.current().expect("replacement remains attached");
        assert_eq!(
            current
                .harness()
                .get_model()
                .await
                .unwrap()
                .unwrap()
                .id
                .as_str(),
            "m4-first"
        );
        current
            .switch_model(
                ProviderId::new("openai").unwrap(),
                ModelId::new("m4-second").unwrap(),
            )
            .await
            .expect("model switch");
        assert_eq!(
            current
                .harness()
                .get_model()
                .await
                .unwrap()
                .unwrap()
                .id
                .as_str(),
            "m4-second"
        );
        lifecycle.close().await.expect("close");
        assert!(
            fixture.session.claim_runtime().is_ok(),
            "close releases owner"
        );
    });
}

#[test]
fn lifecycle_retains_owner_when_teardown_reports_storage_failure() {
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let fixture = Fixture::new().await;
        let mut lifecycle = CodingSessionLifecycle::new();
        lifecycle
            .attach(fixture.builder().first_attachment())
            .await
            .expect("first attach");
        // Inject a deterministic failure into the teardown path by closing the
        // backend behind the active owner. The lifecycle must retain the
        // owner/lease handle so a host can inspect or recover it.
        fixture.session.close().await.expect("injected close");
        assert!(
            lifecycle.close().await.is_err(),
            "teardown must report failure"
        );
        assert!(
            lifecycle.current().is_some(),
            "failed teardown must retain the active owner for recovery"
        );
        assert!(
            fixture.session.claim_runtime().is_err(),
            "failed close must retain the runtime lease"
        );
        drop(lifecycle);
        assert!(
            fixture.session.claim_runtime().is_ok(),
            "dropping the recovered owner releases the retained lease"
        );
    });
}

#[test]
fn concurrent_first_attachment_contenders_have_one_winner_and_one_failed_claim() {
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let fixture = Fixture::new().await;
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let first = fixture.builder().first_attachment();
        let second = fixture.builder().first_attachment();
        let left_barrier = barrier.clone();
        let right_barrier = barrier;
        let left = tokio::spawn(async move {
            left_barrier.wait().await;
            first.build().await
        });
        let right = tokio::spawn(async move {
            right_barrier.wait().await;
            second.build().await
        });
        let left = left.await.expect("left contender");
        let right = right.await.expect("right contender");
        let mut winners = Vec::new();
        if let Ok(value) = left {
            winners.push(value);
        }
        if let Ok(value) = right {
            winners.push(value);
        }
        assert_eq!(winners.len(), 1, "exactly one first attachment wins");
        assert!(
            fixture.session.claim_runtime().is_err(),
            "winning owner retains the one-runtime claim"
        );
        drop(winners);
        assert!(
            fixture.session.claim_runtime().is_ok(),
            "dropping the winner releases the claim after the failed contender"
        );
    });
}

#[test]
fn fork_destination_publishes_through_lifecycle_and_preserves_source() {
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let fixture = Fixture::new().await;
        let mut lifecycle = CodingSessionLifecycle::new();
        lifecycle
            .attach(fixture.builder().first_attachment())
            .await
            .expect("source attach");
        let source_leaf = fixture.session.get_leaf_id().await.expect("source leaf");
        let fork = fixture
            .repo
            .fork(
                fixture.session.metadata(),
                SessionForkRequest {
                    fork: ForkOptions::Branch(BranchForkOptions::default()),
                    create: SessionCreateOptions {
                        id: Some("m4-fork".parse().expect("fork id")),
                        ..Default::default()
                    },
                },
            )
            .await
            .expect("fork destination");
        assert_eq!(
            fork.metadata().parent_session_id,
            Some("m4-session".parse().expect("parent id"))
        );
        assert_eq!(
            fixture.session.get_leaf_id().await.expect("source leaf"),
            source_leaf
        );
        lifecycle
            .attach_fork(fixture.builder_for(fork.clone()).first_attachment())
            .await
            .expect("fork attach");
        assert!(
            fork.get_register(pi_agent::schema::RegisterNamespace::LaneConfig, "main")
                .await
                .expect("fork config")
                .is_some()
        );
        lifecycle.close().await.expect("close fork");
    });
}

#[test]
fn close_failure_keeps_claim_busy_until_session_recovery_and_owner_drop() {
    #[derive(Default)]
    struct TrackingTaskSpawner(std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>);
    impl TaskSpawner for TrackingTaskSpawner {
        fn spawn(&self, task: pi_ai::BoxFuture<'static, ()>) -> Result<(), pi_ai::SpawnError> {
            let handle = tokio::spawn(async move {
                let mut task = task;
                task.as_mut().await;
                // Release every task-captured owner before reporting completion.
                drop(task);
            });
            self.0.lock().expect("fixture task tracker").push(handle);
            Ok(())
        }
    }
    impl TrackingTaskSpawner {
        async fn join(&self) -> usize {
            let mut joined = 0;
            loop {
                let tasks = std::mem::take(&mut *self.0.lock().expect("fixture task tracker"));
                if tasks.is_empty() {
                    return joined;
                }
                joined += tasks.len();
                // Await outside the lock, then include any tasks these admitted.
                for task in tasks {
                    task.await.expect("admitted host task completes");
                }
            }
        }
    }

    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let clock = Arc::new(ClockPort);
        let spawner = Arc::new(TokioSpawner);
        let host_tasks = Arc::new(TrackingTaskSpawner::default());
        let ids = Arc::new(Ids::default());
        let first = model("m4-first");
        let registry = registry_for(
            clock.clone(),
            spawner.clone(),
            first.clone(),
            model("m4-second"),
        )
        .await;
        let backing = Arc::new(MemoryBacking::new());
        let raw = backing.open(clock.clone()).expect("memory storage");
        raw.commit(Transaction {
            writes: vec![
                Write::SetRegister(Box::new(RegisterSet::LaneConfig {
                    key: "main".into(),
                    value: LaneConfiguration {
                        model: LaneModel {
                            provider: first.provider.clone(),
                            model_id: first.id.clone(),
                        },
                        thinking_level: pi_agent::schema::AgentThinkingLevel::Off,
                        active_tool_names: vec![ToolName::new("read").unwrap()],
                    },
                })),
                Write::SetRegister(Box::new(RegisterSet::LaneLeaf {
                    key: "main".into(),
                    value: None,
                })),
                Write::SetRegister(Box::new(RegisterSet::LaneState {
                    key: "main".into(),
                    value: LaneState {
                        current_operation_id: None,
                        pending_next_run: vec![],
                    },
                })),
            ],
        })
        .await
        .expect("seed backend");
        let failing = Arc::new(FailOnceStorage::new(raw));
        let session = Arc::new(
            RuntimeSession::new(
                failing,
                SessionMetadata {
                    id: "m4-close-recovery".parse().unwrap(),
                    created_at: SafeUnsigned::new(7).unwrap(),
                    storage_version: SafeUnsigned::new(1).unwrap(),
                    store_generation: SafeUnsigned::new(0).unwrap(),
                    cwd: None,
                    parent_session_id: None,
                    legacy_parent_session_path: None,
                },
                Arc::new(
                    SessionCodec::new(
                        SessionCodecOptions::default(),
                        ProviderHeaderPolicy::default(),
                    )
                    .unwrap(),
                ),
                ids.clone(),
                Arc::new(NOOP_TELEMETRY_CONTEXT),
            )
            .await
            .expect("session"),
        );
        let env: Arc<dyn ExecutionEnv> = Arc::new(LocalExecutionEnv::new(std::env::temp_dir()));
        let factory: Arc<RuntimeFactory<pi_coding_runtime::ExecutionToolContext, FailOnceStorage>> =
            Arc::new(RuntimeFactory::new(
                clock,
                host_tasks.clone(),
                ids,
                Arc::new(pi_coding_runtime::ExecutionToolContext { env: env.clone() }),
                Arc::new(Summary),
            ));
        let builder = CodingHarnessBuilder::new(
            factory,
            session.clone(),
            registry,
            env,
            Resources::default(),
            settings_for(&first),
        );
        let mut lifecycle: CodingSessionLifecycle<FailOnceStorage> = CodingSessionLifecycle::new();
        lifecycle.attach(builder).await.expect("attach");
        assert!(lifecycle.close().await.is_err(), "injected close failure");
        assert!(
            session.claim_runtime().is_err(),
            "failed close retains lease"
        );
        // Close-result notification can precede destruction of its owner clones.
        assert!(
            host_tasks.join().await > 0,
            "fixture joins admitted host tasks"
        );
        assert!(
            session.claim_runtime().is_err(),
            "failed close retains lease after host tasks complete"
        );
        session.close().await.expect("successful recovery close");
        assert!(
            session.claim_runtime().is_err(),
            "session recovery retains lease while lifecycle owns harness"
        );
        drop(lifecycle);
        assert!(
            session.claim_runtime().is_ok(),
            "recovered owner releases lease"
        );
    });
}

#[test]
fn builder_rejects_stream_fields_outside_the_curated_harness_surface() {
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let fixture = Fixture::new().await;
        let mut settings = fixture.settings.clone();
        settings.stream.temperature = Some(1.0.try_into().expect("finite temperature"));
        let builder = CodingHarnessBuilder::new(
            fixture.factory.clone(),
            fixture.session.clone(),
            fixture.registry.clone(),
            fixture.env.clone(),
            Resources::default(),
            settings,
        )
        .first_attachment();
        assert!(matches!(
            builder.build().await,
            Err(CodingHarnessError::InvalidStreamOptions)
        ));
        assert!(
            fixture.session.claim_runtime().is_ok(),
            "rejected options do not claim"
        );
    });
}

#[test]
fn configured_restart_reopens_jsonl_on_a_new_backend_incarnation() {
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let root = unique_dir("m4-restart");
        std::fs::create_dir_all(&root).expect("root");
        let clock = Arc::new(ClockPort);
        let spawner = Arc::new(TokioSpawner);
        let ids = Arc::new(Ids::default());
        let first = model("m4-first");
        let registry = registry_for(
            clock.clone(),
            spawner.clone(),
            first.clone(),
            model("m4-second"),
        )
        .await;
        let repo = JsonlSessionRepo::new(
            &root,
            SessionCodecOptions::default(),
            ProviderHeaderPolicy::default(),
            ids.clone(),
            clock.clone(),
            Arc::new(NOOP_TELEMETRY_CONTEXT),
            JsonlOptions::default(),
        )
        .expect("jsonl repo");
        let session = repo
            .create(SessionCreateOptions {
                id: Some("m4-restart".parse().expect("id")),
                ..Default::default()
            })
            .await
            .expect("session");
        let env: Arc<dyn ExecutionEnv> = Arc::new(LocalExecutionEnv::new(&root));
        let factory: Arc<
            RuntimeFactory<
                pi_coding_runtime::ExecutionToolContext,
                JsonlStorage,
                JsonlSessionMetadata,
            >,
        > = Arc::new(RuntimeFactory::new(
            clock.clone(),
            spawner.clone(),
            ids,
            Arc::new(pi_coding_runtime::ExecutionToolContext { env: env.clone() }),
            Arc::new(Summary),
        ));
        let settings = settings_for(&first);
        let builder = |session: Arc<JsonlSession>| {
            CodingHarnessBuilder::new(
                factory.clone(),
                session,
                registry.clone(),
                env.clone(),
                Resources::default(),
                settings.clone(),
            )
        };
        let metadata = session.metadata();
        let mut lifecycle: CodingSessionLifecycle<JsonlStorage, JsonlSessionMetadata> =
            CodingSessionLifecycle::new();
        lifecycle
            .attach(builder(session.clone()).first_attachment())
            .await
            .expect("first attach");
        lifecycle
            .current()
            .expect("current")
            .switch_model(
                ProviderId::new("openai").unwrap(),
                ModelId::new("m4-second").unwrap(),
            )
            .await
            .expect("durable model switch");
        assert_eq!(
            lifecycle
                .current()
                .expect("current")
                .harness()
                .get_model()
                .await
                .unwrap()
                .unwrap()
                .id
                .as_str(),
            "m4-second"
        );
        lifecycle.close().await.expect("close first backend");
        let reopened = repo.open(metadata).await.expect("reopen");
        assert!(
            !Arc::ptr_eq(&session, &reopened),
            "reopen allocates a new backend incarnation"
        );
        lifecycle
            .restart(builder(reopened.clone()))
            .await
            .expect("configured restart");
        assert_eq!(
            lifecycle
                .current()
                .expect("current")
                .session()
                .metadata()
                .base
                .id,
            reopened.metadata().base.id
        );
        assert_eq!(
            lifecycle
                .current()
                .expect("current")
                .harness()
                .get_model()
                .await
                .unwrap()
                .unwrap()
                .id
                .as_str(),
            "m4-second"
        );
        lifecycle.close().await.expect("close reopened backend");
        std::fs::remove_dir_all(root).expect("cleanup");
    });
}

#[test]
fn active_run_transport_requires_a_host_supplied_provider_adapter() {
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let fixture = Fixture::new().await;
        let provider = fixture
            .registry
            .get_provider(&ProviderId::new("openai").unwrap())
            .expect("provider");
        let model = fixture
            .registry
            .get_model(
                &ProviderId::new("openai").unwrap(),
                &ModelId::new("m4-first").unwrap(),
            )
            .expect("model");
        assert!(
            provider.adapter(&model.api).is_none(),
            "the M4 fixture refuses active-run execution without a host adapter"
        );
    });
}

#[test]
fn external_legacy_import_preserves_source_until_first_checked_write() {
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let root = unique_dir("m4-import");
        std::fs::create_dir_all(&root).expect("root");
        let path = root.join("legacy.jsonl");
        let source = legacy_v3_with_usage();
        std::fs::write(&path, &source).expect("fixture");
        let ids = Arc::new(Ids::default());
        let repo = JsonlSessionRepo::new(
            &root,
            SessionCodecOptions::default(),
            ProviderHeaderPolicy::default(),
            ids,
            Arc::new(ClockPort),
            Arc::new(NOOP_TELEMETRY_CONTEXT),
            JsonlOptions::default(),
        )
        .expect("jsonl repo");
        let imported = LegacyImportSource::new(&path)
            .open(&repo)
            .await
            .expect("open");
        assert_eq!(std::fs::read(&path).expect("read source"), source);
        assert_eq!(imported.metadata().base.id.as_str(), "process");
        assert_eq!(imported.metadata().base.cwd.as_deref(), Some("source"));
        let imported_leaf = imported.get_leaf_id().await.expect("normalized leaf");
        assert!(imported_leaf.is_some());
        assert!(
            imported_leaf
                .as_ref()
                .is_some_and(|leaf| leaf.as_str() != "assistant"),
            "legacy adoption remints the source identity"
        );
        assert_eq!(
            imported
                .get_stats()
                .await
                .expect("normalized stats")
                .usage
                .total_tokens,
            SafeUnsigned::new(99).unwrap()
        );

        let clock = Arc::new(ClockPort);
        let spawner = Arc::new(TokioSpawner);
        let ids = Arc::new(Ids::default());
        let first = model("m4-first");
        let registry = registry_for(
            clock.clone(),
            spawner.clone(),
            first.clone(),
            model("m4-second"),
        )
        .await;
        let env: Arc<dyn ExecutionEnv> = Arc::new(LocalExecutionEnv::new(&root));
        let factory: Arc<
            RuntimeFactory<
                pi_coding_runtime::ExecutionToolContext,
                JsonlStorage,
                JsonlSessionMetadata,
            >,
        > = Arc::new(RuntimeFactory::new(
            clock,
            spawner,
            ids,
            Arc::new(pi_coding_runtime::ExecutionToolContext { env: env.clone() }),
            Arc::new(Summary),
        ));
        let builder = CodingHarnessBuilder::new(
            factory,
            imported.clone(),
            registry,
            env,
            Resources::default(),
            settings_for(&first),
        )
        .first_attachment();
        let mut lifecycle: CodingSessionLifecycle<JsonlStorage, JsonlSessionMetadata> =
            CodingSessionLifecycle::new();
        lifecycle
            .attach_import(builder)
            .await
            .expect("import attach");
        assert_eq!(
            lifecycle
                .current()
                .expect("current")
                .session()
                .get_leaf_id()
                .await
                .expect("leaf"),
            imported_leaf
        );
        let rows: Vec<serde_json::Value> = std::fs::read(&path)
            .expect("converted bytes")
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_slice(line).expect("jsonl row"))
            .collect();
        assert_eq!(
            rows.first().and_then(|row| row.get("v")),
            Some(&serde_json::json!(4))
        );
        assert!(rows.iter().any(
            |row| row.get("details").and_then(|details| details.get("source"))
                == Some(&serde_json::json!("v3-import"))
        ));
        lifecycle.close().await.expect("close import");
        std::fs::remove_dir_all(root).expect("cleanup");
    });
}

fn legacy_v3_with_usage() -> Vec<u8> {
    let rows = [
        serde_json::json!({
            "type": "session",
            "version": 3,
            "id": "process",
            "timestamp": "1970-01-01T00:00:00.001Z",
            "cwd": "source"
        }),
        serde_json::json!({
            "type": "custom",
            "id": "old",
            "parentId": null,
            "timestamp": "1970-01-01T00:00:00.002Z",
            "customType": "note",
            "data": {"opaque": "old"}
        }),
        serde_json::json!({
            "type": "message",
            "id": "assistant",
            "parentId": "old",
            "timestamp": "1970-01-01T00:00:00.003Z",
            "message": {
                "role": "assistant",
                "content": [{"type": "text", "text": "answer", "textSignature": "keep"}],
                "api": "a",
                "provider": "p",
                "model": "m",
                "usage": {
                    "input": 1,
                    "output": 2,
                    "cacheRead": 3,
                    "cacheWrite": 4,
                    "totalTokens": 99,
                    "cost": {"input": 0.25, "output": 0.5, "cacheRead": 0.75, "cacheWrite": 1, "total": -0.125}
                },
                "stopReason": "stop",
                "timestamp": 78
            }
        }),
    ];
    let mut source = Vec::new();
    for row in rows {
        source.extend(serde_json::to_vec(&row).expect("legacy row"));
        source.push(b'\n');
    }
    source
}

fn unique_dir(prefix: &str) -> PathBuf {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    std::env::temp_dir().join(format!(
        "{prefix}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::SeqCst)
    ))
}
