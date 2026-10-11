#![cfg(feature = "extensions")]

use pi_agent::schema::{
    AgentHarness, AgentLane, AgentMessage, HarnessRuntimeError, IdGenerator, SessionCreateOptions,
    SessionRepo, SessionTree, StorageError,
};
use pi_agent_session::{
    context::SummaryRenderer,
    memory::{MemoryStorage, StorageClock},
    repo::MemorySessionRepo,
};
use pi_ai::{
    ApiAdapter, ApiId, Clock, Message, Model, ModelBaseUrl, ModelCost, ModelId, ModelInput,
    ModelsError, ModelsRegistry, ModelsRegistryOptions, Provider, ProviderAuth, ProviderId,
    PublicModelBaseUrl, SafeUnsigned, StreamOptionsWire, TaskSpawner, ToolName, UserContent,
    UserMessage,
};
use pi_coding_runtime::{
    CodingExtensionAdapter, CodingHarnessBuilder, CodingSettingsSnapshot, CompactionSettings,
    EchoConflictPolicy, ExecutionEnv, ExtensionRegistration, LocalExecutionEnv, ModelRef,
    QueueMode, Resources, ThinkingLevel,
};
use pi_extension_protocol::{
    activation::HostAuthority,
    profiles::{ApprovedProfile, Exposure, ProfileApproval},
    wire::{framing, v1_1},
};
use pi_harness_runtime::{
    extensions::{
        profiles::connection::ProfileConnection,
        tool_driver::EchoRouteTiming,
        transport::{ChannelError, MonotonicClock, OrderedByteChannel, PumpConfig},
    },
    owner::RuntimeFactory,
};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    num::{NonZeroU64, NonZeroUsize},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    task::Poll,
    time::Duration,
};

struct Ids(AtomicU64);
impl Default for Ids {
    fn default() -> Self {
        Self(AtomicU64::new(0))
    }
}
impl IdGenerator for Ids {
    fn next(&self, _: Option<SafeUnsigned>) -> String {
        format!("m5-{}", self.0.fetch_add(1, Ordering::SeqCst))
    }
}

struct AppClock;
impl Clock for AppClock {
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
impl StorageClock for AppClock {
    fn now(&self) -> Result<SafeUnsigned, StorageError> {
        Ok(SafeUnsigned::new(7).expect("bounded clock"))
    }
}

struct Spawner;
impl TaskSpawner for Spawner {
    fn spawn(&self, task: pi_ai::BoxFuture<'static, ()>) -> Result<(), pi_ai::SpawnError> {
        tokio::spawn(task);
        Ok(())
    }
}

struct AuthContext;
impl pi_ai::AuthContext for AuthContext {
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

struct FixtureProvider {
    id: ProviderId,
    models: Vec<Model>,
    model_calls: Arc<AtomicU64>,
}
impl Provider for FixtureProvider {
    fn id(&self) -> &ProviderId {
        &self.id
    }
    fn name(&self) -> &str {
        "m5-fixture"
    }
    fn auth(&self) -> &ProviderAuth {
        static AUTH: std::sync::OnceLock<ProviderAuth> = std::sync::OnceLock::new();
        AUTH.get_or_init(ProviderAuth::no_auth)
    }
    fn models(&self) -> Result<Arc<[Model]>, ModelsError> {
        self.model_calls.fetch_add(1, Ordering::SeqCst);
        Ok(self.models.clone().into())
    }
    fn adapter(&self, _: &ApiId) -> Option<Arc<dyn ApiAdapter>> {
        None
    }
}

fn model(id: &str) -> Model {
    Model {
        id: ModelId::new(id).expect("model ID"),
        name: id.to_owned(),
        api: ApiId::new(ApiId::OPENAI_COMPLETIONS).expect("API"),
        provider: ProviderId::new("openai").expect("provider"),
        base_url: ModelBaseUrl::Public(
            PublicModelBaseUrl::new("https://example.invalid").expect("public endpoint"),
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
        context_window: SafeUnsigned::new(1024).expect("context"),
        max_tokens: SafeUnsigned::new(128).expect("max"),
        sampling_params: None,
        headers: None,
        compat: None,
    }
}

fn settings(model: &Model) -> CodingSettingsSnapshot {
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
        QueueMode::All,
        QueueMode::All,
    )
}

fn profile(kind: &str, name: &str) -> ApprovedProfile {
    ApprovedProfile::new(ProfileApproval {
        principal: "peer-a".into(),
        kind: kind.into(),
        name: name.into(),
        profile: format!("{kind}.v1"),
        handler: format!("{kind}-{name}-handler"),
        minors: BTreeSet::from([0]),
        replay: if kind == "provider" { 2 } else { 1 },
        execution: if kind == "provider" { 2 } else { 1 },
        scope: "main".into(),
        permissions: BTreeSet::new(),
        exposure: Exposure::new(BTreeMap::new(), BTreeSet::new()).expect("exposure"),
    })
    .expect("approved profile")
}

struct Mono(AtomicU64);
impl MonotonicClock for Mono {
    fn now_ms(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
    fn sleep_until(&self, deadline_ms: u64) -> pi_ai::BoxFuture<'_, ()> {
        Box::pin(std::future::poll_fn(move |_| {
            if self.now_ms() >= deadline_ms {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }))
    }
}

struct Peer {
    input: Mutex<VecDeque<u8>>,
    writes: Mutex<Vec<Vec<u8>>>,
    closed: AtomicBool,
}
impl Peer {
    fn new(profiles: &[ApprovedProfile]) -> Arc<Self> {
        let supported_profiles = profiles
            .iter()
            .map(|profile| v1_1::ProfileRange {
                name: profile.profile().into(),
                major: 1,
                min_minor: 0,
                max_minor: 0,
            })
            .collect();
        let capabilities = profiles
            .iter()
            .map(|profile| v1_1::CapabilityDeclaration {
                kind: profile.kind().into(),
                name: profile.name().into(),
                profile: Some(v1_1::ProfileRange {
                    name: profile.profile().into(),
                    major: 1,
                    min_minor: 0,
                    max_minor: 0,
                }),
                handler_id: profile.handler().into(),
                replay: profile.replay(),
                execution: profile.execution(),
                required: true,
                supported_descriptor_digests: vec![v1_1::SupportedProfileDigest {
                    minor: 0,
                    descriptor_digest_sha256:
                        pi_extension_protocol::descriptor::canonical_profile_digest(
                            profile.profile(),
                            1,
                            0,
                        )
                        .expect("profile digest")
                        .to_vec(),
                }],
                ..Default::default()
            })
            .collect();
        let peer = Arc::new(Self {
            input: Mutex::new(VecDeque::new()),
            writes: Mutex::new(Vec::new()),
            closed: AtomicBool::new(false),
        });
        peer.push(v1_1::envelope::Frame::Hello(v1_1::Hello {
            protocol: Some(v1_1::VersionRange {
                major: 1,
                min_minor: 0,
                max_minor: 0,
            }),
            supported_profiles,
        }));
        peer.push(v1_1::envelope::Frame::Manifest(v1_1::Manifest {
            declared_extension_id: "peer-a".into(),
            extension_version: "m5".into(),
            protocol: Some(v1_1::VersionRange {
                major: 1,
                min_minor: 0,
                max_minor: 0,
            }),
            capabilities,
            permissions: vec![],
            ..Default::default()
        }));
        peer.push(v1_1::envelope::Frame::Ready(v1_1::Ready {}));
        peer
    }

    fn push(&self, frame: v1_1::envelope::Frame) {
        let encoded = framing::encode(
            &v1_1::Envelope { frame: Some(frame) },
            NonZeroUsize::new(8192).expect("frame bound"),
        )
        .expect("encode handshake");
        self.input.lock().expect("peer input").extend(encoded.bytes);
    }
}
impl OrderedByteChannel for Peer {
    fn max_read_scratch_bytes(&self) -> usize {
        8192
    }
    fn read_chunk<'a>(
        &'a self,
        buffer: &'a mut [u8],
    ) -> pi_ai::BoxFuture<'a, Result<Option<usize>, ChannelError>> {
        Box::pin(async move {
            loop {
                if self.closed.load(Ordering::SeqCst) {
                    return Ok(None);
                }
                let count = {
                    let mut input = self.input.lock().expect("peer input");
                    if input.is_empty() {
                        None
                    } else {
                        let count = buffer.len().min(input.len());
                        for destination in buffer.iter_mut().take(count) {
                            *destination = input.pop_front().expect("counted byte");
                        }
                        Some(count)
                    }
                };
                if let Some(count) = count {
                    return Ok(Some(count));
                }
                tokio::task::yield_now().await;
            }
        })
    }
    fn write_frame(
        &self,
        frame: pi_extension_protocol::wire::framing::EncodedFrame,
    ) -> pi_ai::BoxFuture<'_, Result<(), ChannelError>> {
        self.writes.lock().expect("peer writes").push(frame.bytes);
        Box::pin(async { Ok(()) })
    }
    fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
    }
}

struct Fixture {
    model: Model,
    factory: Arc<RuntimeFactory<pi_coding_runtime::ExecutionToolContext, MemoryStorage>>,
    session: Arc<pi_agent_session::repo::MemorySession>,
    registry: Arc<ModelsRegistry>,
    environment: Arc<dyn ExecutionEnv>,
    provider_model_calls: Arc<AtomicU64>,
}

async fn fixture() -> Fixture {
    let clock = Arc::new(AppClock);
    let spawner = Arc::new(Spawner);
    let ids = Arc::new(Ids::default());
    let model = model("m5-model");
    let provider_model_calls = Arc::new(AtomicU64::new(0));
    let registry = Arc::new(ModelsRegistry::new(ModelsRegistryOptions {
        credentials: Arc::new(pi_ai::InMemoryCredentialStore::new()),
        models_store: Box::new(pi_ai::InMemoryModelsStore::new()),
        auth_context: Arc::new(AuthContext),
        clock: clock.clone(),
        spawner: spawner.clone(),
        model_header_policy: Default::default(),
    }));
    registry
        .set_provider(Arc::new(FixtureProvider {
            id: model.provider.clone(),
            models: vec![model.clone()],
            model_calls: provider_model_calls.clone(),
        }))
        .await
        .expect("initial model catalog");
    let repo = MemorySessionRepo::new(
        Default::default(),
        Default::default(),
        ids.clone(),
        clock.clone(),
        Arc::new(pi_telemetry::NOOP_TELEMETRY_CONTEXT),
    )
    .expect("session repo");
    let session = repo
        .create(SessionCreateOptions {
            id: Some("m5-session".parse().expect("session ID")),
            ..Default::default()
        })
        .await
        .expect("session");
    let environment: Arc<dyn ExecutionEnv> = Arc::new(LocalExecutionEnv::new(std::env::temp_dir()));
    let factory = Arc::new(RuntimeFactory::new(
        clock,
        spawner,
        ids,
        Arc::new(pi_coding_runtime::ExecutionToolContext {
            env: environment.clone(),
        }),
        Arc::new(Summary),
    ));
    Fixture {
        model,
        factory,
        session,
        registry,
        environment,
        provider_model_calls,
    }
}

fn host(profiles: Vec<ApprovedProfile>) -> HostAuthority {
    HostAuthority::new(
        "peer-a".into(),
        7,
        0,
        0,
        v1_1::ProtocolLimits {
            pre_handshake_frame_bytes: 8192,
            frame_bytes: 8192,
            aggregate_decoded_bytes: 8192,
            value_depth: 32,
            value_elements: 1024,
            string_bytes: 4096,
            bytes_length: 4096,
            in_flight_requests: 8,
            blocking_queue_bytes: 4096,
            progress_queue_bytes: 4096,
            passive_queue_bytes: 4096,
            telemetry_queue_bytes: 4096,
            nesting_depth: 8,
            requests_per_second: 32,
            handshake_ms: 1000,
            cancellation_grace_ms: 100,
            max_stream_credit_bytes: 4096,
        },
        vec![],
        vec![],
    )
    .expect("host")
    .with_profiles(profiles)
    .expect("profiles")
}

async fn coding_harness(
    fixture: &Fixture,
) -> pi_coding_runtime::CodingHarness<pi_agent_session::memory::MemoryStorage> {
    CodingHarnessBuilder::new(
        fixture.factory.clone(),
        fixture.session.clone(),
        fixture.registry.clone(),
        fixture.environment.clone(),
        Resources::default(),
        settings(&fixture.model),
    )
    .first_attachment()
    .build()
    .await
    .expect("coding harness")
}

fn reservation() -> pi_extension_protocol::profile_live::Reservations {
    pi_extension_protocol::profile_live::Reservations {
        blocking_bytes: 4096,
        control_bytes: 65_536,
        terminal_bytes: 65_536,
        progress_bytes: 4096,
        passive_bytes: 4096,
        telemetry_bytes: 4096,
    }
}

fn config() -> PumpConfig {
    PumpConfig {
        max_read_chunk: NonZeroUsize::new(1024).expect("read bound"),
        write_timeout_ms: NonZeroU64::new(1000).expect("write timeout"),
    }
}

fn capability(connection: &ProfileConnection, kind: &str, name: &str) -> v1_1::CapabilityRef {
    connection
        .grant()
        .profiles()
        .iter()
        .find(|profile| profile.reference().kind == kind && profile.reference().name == name)
        .expect("granted capability")
        .reference()
        .clone()
}

#[test]
fn assembled_missing_grant_refuses_before_harness_registration() {
    tokio::runtime::Runtime::new()
        .expect("runtime")
        .block_on(async {
            let fixture = fixture().await;
            let harness = coding_harness(&fixture).await;
            let provider_calls_before = fixture.provider_model_calls.load(Ordering::SeqCst);
            let stats_before = fixture.session.get_stats().await.expect("stats before");
            let leaf_before = fixture.session.get_leaf_id().await.expect("leaf before");
            let tool_invocations = AtomicU64::new(0);
            let peer = Peer::new(&[]);
            let adapter = CodingExtensionAdapter::connect_service(
                &harness,
                pi_coding_runtime::ExtensionServiceInput {
                    host: host(vec![]),
                    channel: peer.clone(),
                    clock: Arc::new(Mono(AtomicU64::new(0))),
                    config: config(),
                    reservations: reservation(),
                    remaining_ms: 10_000,
                },
            )
            .await
            .expect("ready service");
            let missing = v1_1::CapabilityRef {
                authenticated_extension_id: "peer-a".into(),
                kind: "hook".into(),
                name: "before_run".into(),
                profile: Some(v1_1::ProfileVersion {
                    name: "hook.v1".into(),
                    major: 1,
                    minor: 0,
                }),
                handler_id: Some("missing".into()),
                ..Default::default()
            };
            let result = harness
                .attach_extensions(adapter, vec![ExtensionRegistration::hook(missing)])
                .await;
            assert!(matches!(
                result,
                Err(pi_coding_runtime::CodingExtensionError::MissingGrant { .. })
            ));
            assert_eq!(
                harness
                    .harness()
                    .registered_hook_count(pi_agent::schema::HookName::BeforeRun),
                0
            );
            assert_eq!(
                fixture.provider_model_calls.load(Ordering::SeqCst),
                provider_calls_before,
                "missing grant must not call the provider catalog"
            );
            assert_eq!(
                fixture.session.get_stats().await.expect("stats after"),
                stats_before,
                "missing grant must not publish a session event"
            );
            assert_eq!(
                fixture.session.get_leaf_id().await.expect("leaf after"),
                leaf_before,
                "missing grant must not add a durable entry"
            );
            assert_eq!(tool_invocations.load(Ordering::SeqCst), 0);
            assert!(peer.writes.lock().expect("writes").len() >= 2);
            harness.harness().close().await.expect("close harness");
        });
}

#[test]
fn assembled_ordered_provider_and_hook_registration_is_deterministic() {
    tokio::runtime::Runtime::new()
        .expect("runtime")
        .block_on(async {
            let fixture = fixture().await;
            let harness = coding_harness(&fixture).await;
            let selected = vec![profile("hook", "before_run"), profile("provider", "openai")];
            let peer = Peer::new(&selected);
            let adapter = CodingExtensionAdapter::connect_service(
                &harness,
                pi_coding_runtime::ExtensionServiceInput {
                    host: host(selected),
                    channel: peer,
                    clock: Arc::new(Mono(AtomicU64::new(0))),
                    config: config(),
                    reservations: reservation(),
                    remaining_ms: 10_000,
                },
            )
            .await
            .expect("ready service");
            let cap_hook = capability(adapter.connection(), "hook", "before_run");
            let cap_provider = capability(adapter.connection(), "provider", "openai");
            let mut alternate = fixture.model.clone();
            alternate.id = pi_ai::ModelId::new("m5-selected").expect("selected model");
            let attachment = harness
                .attach_extensions(
                    adapter,
                    vec![
                        ExtensionRegistration::provider(
                            cap_provider,
                            vec![fixture.model.clone(), alternate.clone()],
                        ),
                        ExtensionRegistration::hook(cap_hook),
                    ],
                )
                .await
                .expect("attach ordered profiles");
            assert_eq!(
                attachment.registration_order(),
                &[
                    pi_coding_runtime::ExtensionRegistrationKind::Provider,
                    pi_coding_runtime::ExtensionRegistrationKind::Hook
                ]
            );
            assert_eq!(
                harness
                    .harness()
                    .registered_hook_count(pi_agent::schema::HookName::BeforeRun),
                1
            );
            assert!(
                fixture
                    .registry
                    .get_provider(&fixture.model.provider)
                    .expect("provider")
                    .auth()
                    .is_no_auth()
            );
            harness
                .switch_model(alternate.provider.clone(), alternate.id.clone())
                .await
                .expect("select catalog model");
            assert_eq!(
                harness
                    .harness()
                    .get_model()
                    .await
                    .expect("model")
                    .expect("selected")
                    .id,
                alternate.id
            );
            attachment.close();
            harness.harness().close().await.expect("close harness");
        });
}

#[test]
fn assembled_late_hook_refusal_rolls_back_prior_provider_mutation() {
    tokio::runtime::Runtime::new()
        .expect("runtime")
        .block_on(async {
            let fixture = fixture().await;
            let harness = coding_harness(&fixture).await;
            let selected = vec![profile("hook", "before_run"), profile("provider", "openai")];
            let peer = Peer::new(&selected);
            let adapter = CodingExtensionAdapter::connect_service(
                &harness,
                pi_coding_runtime::ExtensionServiceInput {
                    host: host(selected),
                    channel: peer,
                    clock: Arc::new(Mono(AtomicU64::new(0))),
                    config: config(),
                    reservations: reservation(),
                    remaining_ms: 10_000,
                },
            )
            .await
            .expect("ready service");
            eprintln!(
                "before provider={}",
                fixture
                    .registry
                    .get_provider(&fixture.model.provider)
                    .expect("initial provider")
                    .name()
            );
            let connection = adapter.connection().clone();
            let hook = capability(&connection, "hook", "before_run");
            let provider = capability(&connection, "provider", "openai");
            let first = harness
                .attach_extensions(adapter, vec![ExtensionRegistration::hook(hook.clone())])
                .await
                .expect("first hook");
            let mut alternate = fixture.model.clone();
            alternate.id = pi_ai::ModelId::new("m5-alternate").expect("alternate model");
            let result = harness
                .attach_extensions(
                    CodingExtensionAdapter::from_connection(connection),
                    vec![
                        ExtensionRegistration::provider(provider, vec![alternate]),
                        ExtensionRegistration::hook(hook),
                    ],
                )
                .await;
            assert!(matches!(
                result,
                Err(pi_coding_runtime::CodingExtensionError::Hook(_))
            ));
            let restored = fixture.registry.get_models(Some(&fixture.model.provider));
            assert_eq!(restored.len(), 1);
            assert_eq!(restored[0].id, fixture.model.id);
            first.close();
            harness.harness().close().await.expect("close harness");
        });
}

#[test]
fn assembled_provider_auth_and_runtime_endpoint_inputs_are_refused_without_side_effects() {
    tokio::runtime::Runtime::new()
        .expect("runtime")
        .block_on(async {
            let fixture = fixture().await;
            let harness = coding_harness(&fixture).await;
            let selected = vec![profile("provider", "openai")];
            let peer = Peer::new(&selected);
            let adapter = CodingExtensionAdapter::connect_service(
                &harness,
                pi_coding_runtime::ExtensionServiceInput {
                    host: host(selected),
                    channel: peer,
                    clock: Arc::new(Mono(AtomicU64::new(0))),
                    config: config(),
                    reservations: reservation(),
                    remaining_ms: 10_000,
                },
            )
            .await
            .expect("ready service");
            let cap = capability(adapter.connection(), "provider", "openai");
            let mut invalid = fixture.model.clone();
            invalid.headers = Some(Default::default());
            let result = harness
                .attach_extensions(
                    adapter,
                    vec![ExtensionRegistration::provider(cap, vec![invalid])],
                )
                .await;
            assert!(matches!(
                result,
                Err(pi_coding_runtime::CodingExtensionError::InvalidRegistration(_))
            ));
            assert_eq!(fixture.registry.get_models(None).len(), 1);
            harness.harness().close().await.expect("close harness");
        });
}

#[test]
fn assembled_tool_refuses_without_bounded_echo_route_before_registration() {
    tokio::runtime::Runtime::new()
        .expect("runtime")
        .block_on(async {
            let fixture = fixture().await;
            let harness = coding_harness(&fixture).await;
            assert_eq!(
                CodingExtensionAdapter::echo_conflict_policy(),
                EchoConflictPolicy::RejectExisting
            );
            let peer = Peer::new(&[]);
            let adapter = CodingExtensionAdapter::connect_service(
                &harness,
                pi_coding_runtime::ExtensionServiceInput {
                    host: host(vec![]),
                    channel: peer,
                    clock: Arc::new(Mono(AtomicU64::new(0))),
                    config: config(),
                    reservations: reservation(),
                    remaining_ms: 10_000,
                },
            )
            .await
            .expect("ready service");
            let result = harness
                .attach_extensions(
                    adapter,
                    vec![ExtensionRegistration::echo_tool(
                        pi_coding_runtime::create_tools()
                            .into_iter()
                            .next()
                            .expect("coding tool"),
                        EchoRouteTiming {
                            method_limit_ms: NonZeroU64::new(1000).expect("method limit"),
                            connection_deadline_ms: 10_000,
                        },
                    )],
                )
                .await;
            assert!(matches!(
                result,
                Err(
                    pi_coding_runtime::CodingExtensionError::InvalidRegistration(
                        "echo registration requires a tool connection"
                    )
                )
            ));
            assert_eq!(
                harness
                    .harness()
                    .registered_hook_count(pi_agent::schema::HookName::BeforeRun),
                0
            );
            harness.harness().close().await.expect("close harness");
        });
}

#[test]
fn assembled_close_drops_generation_and_preserves_no_live_binding() {
    tokio::runtime::Runtime::new()
        .expect("runtime")
        .block_on(async {
            let fixture = fixture().await;
            let harness = coding_harness(&fixture).await;
            let selected = vec![profile("provider", "openai")];
            let peer = Peer::new(&selected);
            let adapter = CodingExtensionAdapter::connect_service(
                &harness,
                pi_coding_runtime::ExtensionServiceInput {
                    host: host(selected),
                    channel: peer,
                    clock: Arc::new(Mono(AtomicU64::new(0))),
                    config: config(),
                    reservations: reservation(),
                    remaining_ms: 10_000,
                },
            )
            .await
            .expect("ready service");
            let cap = capability(adapter.connection(), "provider", "openai");
            let attachment = harness
                .attach_extensions(
                    adapter,
                    vec![ExtensionRegistration::provider(
                        cap,
                        vec![fixture.model.clone()],
                    )],
                )
                .await
                .expect("attach");
            assert!(attachment.connection().current());
            let connection = attachment.connection().clone();
            attachment.close();
            assert!(!connection.current());
            assert_eq!(connection.grant().wire().connection_generation, 7);
            harness.harness().close().await.expect("close harness");
        });
}

#[test]
fn assembled_close_allows_replacement_generation_without_inheriting_binding() {
    tokio::runtime::Runtime::new()
        .expect("runtime")
        .block_on(async {
            let fixture = fixture().await;
            let harness = coding_harness(&fixture).await;
            let selected = vec![profile("provider", "openai")];
            let first_peer = Peer::new(&selected);
            let first = CodingExtensionAdapter::connect_service(
                &harness,
                pi_coding_runtime::ExtensionServiceInput {
                    host: host(selected.clone()),
                    channel: first_peer,
                    clock: Arc::new(Mono(AtomicU64::new(0))),
                    config: config(),
                    reservations: reservation(),
                    remaining_ms: 10_000,
                },
            )
            .await
            .expect("first service");
            let first_connection = first.connection().clone();
            let first_attachment = harness
                .attach_extensions(
                    first,
                    vec![ExtensionRegistration::provider(
                        capability(&first_connection, "provider", "openai"),
                        vec![fixture.model.clone()],
                    )],
                )
                .await
                .expect("first attachment");
            first_attachment.close();
            assert!(!first_connection.current());

            let second_peer = Peer::new(&selected);
            let second = CodingExtensionAdapter::connect_service(
                &harness,
                pi_coding_runtime::ExtensionServiceInput {
                    host: host(selected),
                    channel: second_peer,
                    clock: Arc::new(Mono(AtomicU64::new(0))),
                    config: config(),
                    reservations: reservation(),
                    remaining_ms: 10_000,
                },
            )
            .await
            .expect("replacement service");
            let second_connection = second.connection().clone();
            assert!(second_connection.current());
            assert!(
                !first_connection
                    .connection_identity()
                    .same_connection(&second_connection.connection_identity())
            );
            let second_attachment = harness
                .attach_extensions(
                    second,
                    vec![ExtensionRegistration::provider(
                        capability(&second_connection, "provider", "openai"),
                        vec![fixture.model.clone()],
                    )],
                )
                .await
                .expect("replacement attachment");
            second_attachment.close();
            harness.harness().close().await.expect("close harness");
        });
}

#[test]
fn mutation_restoration_witness_rejects_ungranted_provider_then_restores_positive_path() {
    tokio::runtime::Runtime::new()
        .expect("runtime")
        .block_on(async {
            let fixture = fixture().await;
            let harness = coding_harness(&fixture).await;
            let selected = vec![profile("provider", "openai")];
            let peer = Peer::new(&selected);
            let adapter = CodingExtensionAdapter::connect_service(
                &harness,
                pi_coding_runtime::ExtensionServiceInput {
                    host: host(selected),
                    channel: peer,
                    clock: Arc::new(Mono(AtomicU64::new(0))),
                    config: config(),
                    reservations: reservation(),
                    remaining_ms: 10_000,
                },
            )
            .await
            .expect("ready service");
            let cap = capability(adapter.connection(), "provider", "openai");
            let mut mutated = cap.clone();
            mutated.name = "missing".into();
            let rejected = harness
                .attach_extensions(
                    CodingExtensionAdapter::from_connection(adapter.connection().clone()),
                    vec![ExtensionRegistration::provider(
                        mutated,
                        vec![fixture.model.clone()],
                    )],
                )
                .await;
            assert!(matches!(
                rejected,
                Err(pi_coding_runtime::CodingExtensionError::MissingGrant { .. })
            ));
            let restored = harness
                .attach_extensions(
                    CodingExtensionAdapter::from_connection(adapter.connection().clone()),
                    vec![ExtensionRegistration::provider(
                        cap,
                        vec![fixture.model.clone()],
                    )],
                )
                .await
                .expect("restored grant");
            assert_eq!(restored.registration_order().len(), 1);
            restored.close();
            harness.harness().close().await.expect("close harness");
        });
}
