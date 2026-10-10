use pi_agent::schema::{
    AgentLane, AgentMessage, CompactionSettings, HarnessRuntimeError, ProviderHeaderPolicy,
    SessionCodecOptions, SessionCreateOptions, SessionRepo, SessionTree,
};
use pi_agent_session::{
    context::SummaryRenderer,
    jsonl::JsonlOptions,
    memory::{MemoryStorage, StorageClock},
    repo::{MemorySessionRepo, jsonl::JsonlSessionRepo},
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

struct Fixture {
    registry: Arc<ModelsRegistry>,
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
        let provider = Arc::new(FixtureProvider {
            id: first.provider.clone(),
            models: vec![first.clone(), second],
            auth: ProviderAuth::new(Some(Arc::new(NoopApiKey)), None).expect("auth"),
        });
        let registry = Arc::new(ModelsRegistry::new(ModelsRegistryOptions {
            credentials: Arc::new(pi_ai::InMemoryCredentialStore::new()),
            models_store: Box::new(pi_ai::InMemoryModelsStore::new()),
            auth_context: Arc::new(AuthHost),
            clock: clock.clone(),
            spawner: spawner.clone(),
            model_header_policy: Default::default(),
        }));
        registry
            .set_provider(provider)
            .await
            .expect("provider publication");
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
        let settings = CodingSettingsSnapshot::new(
            ModelRef::new(first.provider, first.id),
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
        );
        Self {
            registry,
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
fn external_legacy_import_preserves_source_until_first_checked_write() {
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let root = unique_dir("m4-import");
        std::fs::create_dir_all(&root).expect("root");
        let path = root.join("legacy.jsonl");
        let source =
            include_bytes!("../../../conformance/session/jsonl/repository-process-v3.jsonl");
        std::fs::write(&path, source).expect("fixture");
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
        imported
            .set_name(Some("first checked write".into()))
            .await
            .expect("first checked write converts");
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
        std::fs::remove_dir_all(root).expect("cleanup");
    });
}

fn unique_dir(prefix: &str) -> PathBuf {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    std::env::temp_dir().join(format!(
        "{prefix}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::SeqCst)
    ))
}
