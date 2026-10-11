use pi_agent::schema::EntryQuery;
use pi_agent::schema::{
    AgentHarness, AgentLane, AgentMessage, EventListener, HandlerError, HarnessEvent,
    HarnessEventType, HarnessRuntimeError, IdGenerator, PromptInput, SessionCreateOptions,
    SessionRepo,
};
use pi_agent_session::{
    context::SummaryRenderer,
    memory::{MemoryStorage, StorageClock},
    repo::{MemorySession, MemorySessionRepo},
};
use pi_ai::{
    ApiAdapter, ApiId, ApiKeyAuth, ApiKeyAuthInput, AssistantMessage, AssistantMessageEvent,
    AssistantStream, AssistantStreamInit, AuthContext, AuthContextError, AuthProviderError,
    AuthResult, BoxFuture, CapabilityError, Clock, DeferredCancelRequest, DeferredFetchRequest,
    DoneReason, Message, Model, ModelBaseUrl, ModelCost, ModelId, ModelInput, ModelsError,
    ModelsRegistry, ModelsRegistryOptions, Provider, ProviderAuth, ProviderId, PublicModelBaseUrl,
    RequestLifecycleAdapter, RequestLifecycleInvocation, SafeUnsigned, SimpleStreamRequest,
    TaskSpawner, TextStreamRequest, ToolName, Usage, UserContent,
    create_assistant_message_event_stream,
};
use pi_coding_runtime::{
    CodingHarnessBuilder, CodingHarnessError, CodingRuntimeError, CodingSettingsSnapshot,
    ConfiguredModelOverlay, CredentialInputs, ExecutionEnv, LocalExecutionEnv, ModelAvailability,
    ModelRef, ProviderComposition, ProviderCompositionInputs, QueueMode, Resources, ThinkingLevel,
};
use pi_harness_runtime::owner::RuntimeFactory;
use pi_telemetry::NOOP_TELEMETRY_CONTEXT;
use serde_json::{Value, json};
use std::{
    io::Write,
    process::{Command, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

const AUTHORITY: &str = "853a80d26c90a14c1886f0ebb8ffaae133ca2185";

struct FixtureClock;
impl Clock for FixtureClock {
    fn now_unix_ms(&self) -> u64 {
        7
    }
    fn sleep<'a>(
        &'a self,
        _: Duration,
        cancellation: &'a pi_ai::CancellationToken,
    ) -> BoxFuture<'a, Result<(), pi_ai::Cancelled>> {
        Box::pin(async move {
            if cancellation.is_cancelled() {
                Err(pi_ai::Cancelled)
            } else {
                Ok(())
            }
        })
    }
}
impl StorageClock for FixtureClock {
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

#[derive(Default)]
struct FixtureIds(std::sync::atomic::AtomicUsize);
impl IdGenerator for FixtureIds {
    fn next(&self, _: Option<SafeUnsigned>) -> String {
        format!(
            "m6-id-{}",
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
        )
    }
}

struct FixtureAuth;
impl ApiKeyAuth for FixtureAuth {
    fn name(&self) -> &str {
        "m6-offline"
    }
    fn resolve<'a>(
        &'a self,
        _: ApiKeyAuthInput<'a>,
    ) -> BoxFuture<'a, Result<Option<AuthResult>, AuthProviderError>> {
        Box::pin(async { Ok(None) })
    }
}

struct FixtureAuthContext;
impl AuthContext for FixtureAuthContext {
    fn env<'a>(
        &'a self,
        _: &'a str,
        _: &'a pi_ai::CancellationToken,
    ) -> BoxFuture<'a, Result<Option<String>, AuthContextError>> {
        Box::pin(async { Ok(None) })
    }

    fn file_exists<'a>(
        &'a self,
        _: &'a str,
        _: &'a pi_ai::CancellationToken,
    ) -> BoxFuture<'a, Result<bool, AuthContextError>> {
        Box::pin(async { Ok(false) })
    }
}

struct FixtureProvider {
    id: ProviderId,
    models: Vec<Model>,
    auth: ProviderAuth,
    adapter: Arc<StopAdapter>,
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
    fn adapter(&self, api: &ApiId) -> Option<Arc<dyn ApiAdapter>> {
        (api == self.adapter.api_id()).then(|| self.adapter.clone() as Arc<dyn ApiAdapter>)
    }
}

struct StopAdapter {
    model: Model,
    calls: AtomicUsize,
}

impl StopAdapter {
    fn new(model: &Model) -> Arc<Self> {
        Arc::new(Self {
            model: model.clone(),
            calls: AtomicUsize::new(0),
        })
    }

    fn start(
        &self,
        clock: Arc<dyn Clock>,
        cancellation: pi_ai::CancellationToken,
    ) -> AssistantStream {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let response = AssistantMessage {
            role: Default::default(),
            content: Vec::new(),
            api: self.model.api.clone(),
            provider: self.model.provider.clone(),
            model: self.model.id.clone(),
            response_model: None,
            response_id: None,
            diagnostics: None,
            usage: Usage::zero(),
            stop_reason: pi_ai::StopReason::Stop,
            deferred: None,
            error_message: None,
            raw_stop_reason: None,
            end_turn: None,
            timestamp: SafeUnsigned::new(7).expect("timestamp"),
        };
        let (stream, producer) = create_assistant_message_event_stream(AssistantStreamInit {
            api: self.model.api.clone(),
            provider: self.model.provider.clone(),
            model: self.model.id.clone(),
            clock,
            cancellation,
        });
        tokio::spawn(async move {
            producer
                .push(AssistantMessageEvent::Done {
                    reason: DoneReason::Stop,
                    message: Box::new(response),
                })
                .expect("stop response");
        });
        stream
    }
}

impl ApiAdapter for StopAdapter {
    fn api_id(&self) -> &ApiId {
        &self.model.api
    }

    fn stream(&self, request: TextStreamRequest) -> AssistantStream {
        self.start(
            request.options.runtime.clock.clone(),
            request.options.runtime.cancellation.clone(),
        )
    }

    fn stream_simple(&self, request: SimpleStreamRequest) -> AssistantStream {
        self.start(
            request.options.runtime.clock.clone(),
            request.options.runtime.cancellation.clone(),
        )
    }

    fn fetch_deferred(&self, _: DeferredFetchRequest) -> Result<AssistantStream, CapabilityError> {
        Err(CapabilityError::Unsupported)
    }

    fn cancel_deferred<'a>(
        &'a self,
        _: DeferredCancelRequest,
    ) -> BoxFuture<'a, Result<(), ModelsError>> {
        Box::pin(async { Ok(()) })
    }

    fn lifecycle_adapter(&self) -> Option<&dyn RequestLifecycleAdapter> {
        Some(self)
    }
}

impl RequestLifecycleAdapter for StopAdapter {
    fn stream_with_lifecycle(
        &self,
        request: TextStreamRequest,
        _: RequestLifecycleInvocation,
    ) -> AssistantStream {
        self.stream(request)
    }

    fn stream_simple_with_lifecycle(
        &self,
        request: SimpleStreamRequest,
        _: RequestLifecycleInvocation,
    ) -> AssistantStream {
        self.stream_simple(request)
    }
}

struct Summary;
impl SummaryRenderer for Summary {
    fn render(
        &self,
        summary: pi_agent_session::context::ContextSummary,
    ) -> pi_agent::schema::BoxFuture<'_, Result<AgentMessage, HarnessRuntimeError>> {
        Box::pin(async move {
            Ok(AgentMessage::BuiltIn(Message::User(pi_ai::UserMessage {
                role: Default::default(),
                content: UserContent::Text(summary.text),
                timestamp: summary.timestamp,
            })))
        })
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
        context_window: SafeUnsigned::new(1024).expect("context"),
        max_tokens: SafeUnsigned::new(128).expect("tokens"),
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
        pi_coding_runtime::CompactionSettings {
            enabled: false,
            reserve_tokens: SafeUnsigned::new(0).expect("reserve"),
            keep_recent_tokens: SafeUnsigned::new(0).expect("tail"),
        },
        pi_ai::StreamOptionsWire::default(),
        QueueMode::All,
        QueueMode::All,
    )
}

struct Fixture {
    model: Model,
    adapter: Arc<StopAdapter>,
    factory: Arc<RuntimeFactory<pi_coding_runtime::ExecutionToolContext, MemoryStorage>>,
    session: Arc<MemorySession>,
    registry: Arc<ModelsRegistry>,
    environment: Arc<dyn ExecutionEnv>,
}

async fn fixture() -> Fixture {
    let clock = Arc::new(FixtureClock);
    let spawner = Arc::new(TokioSpawner);
    let ids = Arc::new(FixtureIds::default());
    let selected = model("offline-model");
    let adapter = StopAdapter::new(&selected);
    let registry = Arc::new(ModelsRegistry::new(ModelsRegistryOptions {
        credentials: Arc::new(pi_ai::InMemoryCredentialStore::new()),
        models_store: Box::new(pi_ai::InMemoryModelsStore::new()),
        auth_context: Arc::new(FixtureAuthContext),
        clock: clock.clone(),
        spawner: spawner.clone(),
        model_header_policy: Default::default(),
    }));
    registry
        .set_provider(Arc::new(FixtureProvider {
            id: selected.provider.clone(),
            models: vec![selected.clone()],
            auth: ProviderAuth::new(Some(Arc::new(FixtureAuth)), None).expect("auth"),
            adapter: adapter.clone(),
        }))
        .await
        .expect("provider publication");
    let repo = MemorySessionRepo::new(
        Default::default(),
        Default::default(),
        ids.clone(),
        clock.clone(),
        Arc::new(NOOP_TELEMETRY_CONTEXT),
    )
    .expect("repo");
    let session = repo
        .create(SessionCreateOptions {
            id: Some("m6-session".parse().expect("session id")),
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
        model: selected,
        adapter,
        factory,
        session,
        registry,
        environment,
    }
}

fn envelope(case_id: &str, input: Value, outcome: Value, observations: Value) -> Value {
    json!({ "schemaVersion": 1, "authority": AUTHORITY, "caseId": case_id, "input": input, "events": [], "messages": [], "outcome": outcome, "observations": observations })
}

fn side_effects() -> Value {
    json!({ "provider_calls": 0, "tool_invocations": 0, "session_publication_events": 0, "durable_entries": 0 })
}

fn observed_effects(observation: &AssemblyObservation) -> Value {
    json!({
        "provider_calls": observation.provider_calls,
        "tool_invocations": 0,
        "session_publication_events": 0,
        "durable_entries": observation.durable_entries,
        "event_count": observation.event_types.len(),
        "event_types": observation.event_types,
    })
}

fn composition_for(model: &Model, availability: bool) -> ProviderComposition {
    ProviderComposition::new(ProviderCompositionInputs {
        providers: Vec::new(),
        models: Vec::new(),
        credentials: CredentialInputs::empty(),
    })
    .expect("empty composition")
    .with_model_overlays([ConfiguredModelOverlay::new(model.clone()).with_alias("offline")])
    .expect("overlay")
    .with_availability([ModelAvailability {
        provider: model.provider.clone(),
        model_id: model.id.clone(),
        available: availability,
    }])
}

#[derive(Default)]
struct EventCount(Mutex<Vec<String>>);

impl EventListener for EventCount {
    fn on_event(
        &self,
        event: HarnessEvent,
    ) -> pi_agent::schema::BoxFuture<'_, Result<(), HandlerError>> {
        let mut events = self.0.lock().expect("event count");
        let value = serde_json::to_value(&event).expect("event JSON");
        events.push(
            value
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_owned(),
        );
        Box::pin(async { Ok(()) })
    }
}

#[derive(Debug)]
struct AssemblyObservation {
    provider_calls: usize,
    event_types: Vec<String>,
    durable_entries: usize,
}

async fn assembled(
    fixture: &Fixture,
    case_id: &str,
    composition: Arc<ProviderComposition>,
) -> Result<AssemblyObservation, CodingHarnessError> {
    let resources = if case_id == "prompt.system_resources" {
        Resources {
            skills: Some(vec![pi_coding_runtime::Skill {
                name: "visible".into(),
                description: "fixture".into(),
                content: "detached".into(),
                file_path: "/fixture/SKILL.md".into(),
                disable_model_invocation: None,
            }]),
            prompt_templates: Some(vec![]),
        }
    } else {
        Resources::default()
    };
    let mut builder = CodingHarnessBuilder::new(
        fixture.factory.clone(),
        fixture.session.clone(),
        fixture.registry.clone(),
        fixture.environment.clone(),
        resources,
        settings(&fixture.model),
    )
    .first_attachment();
    builder = builder.with_provider_composition(composition);
    let harness = builder.build().await?;
    let event_count = Arc::new(EventCount::default());
    let mut subscriptions = Vec::new();
    for event_type in [
        HarnessEventType::RunStart,
        HarnessEventType::RunEnd,
        HarnessEventType::TurnStart,
        HarnessEventType::TurnEnd,
        HarnessEventType::MessageStart,
        HarnessEventType::MessageEnd,
        HarnessEventType::EntryAdded,
        HarnessEventType::WritePending,
        HarnessEventType::Usage,
        HarnessEventType::ConfigUpdate,
    ] {
        subscriptions.push(
            harness
                .harness()
                .events()
                .on(event_type, event_count.clone()),
        );
    }
    let run = harness
        .harness()
        .prompt(PromptInput::Text {
            text: format!("m6 assembled operation: {case_id}"),
            images: Vec::new(),
        })
        .await;
    match run {
        Ok(result) => assert!(
            result.is_ok(),
            "{case_id}: assembled prompt rejected: {result:?}"
        ),
        Err(error) => panic!("{case_id}: assembled prompt outer result failed: {error:?}"),
    }
    harness
        .harness()
        .wait_for_idle()
        .await
        .map_err(CodingHarnessError::Runtime)?;
    let durable_entries = harness
        .harness()
        .session()
        .find_entries(EntryQuery::default())
        .await
        .expect("session entries")
        .len();
    // Keep subscriptions alive until the terminal observation has drained.
    let _ = subscriptions;
    Ok(AssemblyObservation {
        provider_calls: fixture.adapter.calls.load(Ordering::SeqCst),
        event_types: event_count.0.lock().expect("event count").clone(),
        durable_entries,
    })
}

async fn actual(case_id: &str) -> Value {
    let fixture = fixture().await;
    let effects = side_effects();
    match case_id {
        "model.credentials_catalog" => {
            let unavailable = composition_for(&fixture.model, false);
            assert!(
                unavailable
                    .resolve_reference("openai:offline-model")
                    .is_err()
            );
            let available = composition_for(&fixture.model, true);
            let selected = available
                .resolve_selection("offline")
                .expect("available overlay");
            assert_eq!(selected.model.id.as_str(), "offline-model");
            let observation = assembled(&fixture, case_id, Arc::new(available))
                .await
                .expect("assembled model row");
            let mut observations = observed_effects(&observation);
            observations["model"] = json!({"provider":"openai","id":"offline-model","source":"overlay","available":true});
            observations["availability_transition"] = json!(["unavailable", "available"]);
            observations["credential_boundary"] =
                json!("host-owned credential inputs retained without reads");
            envelope(
                case_id,
                json!({"kind":"assemble","operation":"resolve_overlay","reference":"openai:offline-model"}),
                json!({"kind":"resolved","error":null,"terminal":true}),
                observations,
            )
        }
        "prompt.system_resources" => {
            let observation = assembled(
                &fixture,
                case_id,
                Arc::new(composition_for(&fixture.model, true)),
            )
            .await
            .expect("resources row");
            let mut observations = observed_effects(&observation);
            observations["resources_attached"] = json!(true);
            envelope(
                case_id,
                json!({"kind":"assemble","operation":"system_resources","resources":{"skills":["visible"],"prompt_templates":["build"]}}),
                json!({"kind":"assembled","error":null,"terminal":true}),
                observations,
            )
        }
        "prompt.settings_snapshot" => {
            let observation = assembled(
                &fixture,
                case_id,
                Arc::new(composition_for(&fixture.model, true)),
            )
            .await
            .expect("settings row");
            let mut observations = observed_effects(&observation);
            observations["settings_detached"] = json!(true);
            envelope(
                case_id,
                json!({"kind":"assemble","operation":"settings_snapshot","thinking_level":"off","active_tools":["read"]}),
                json!({"kind":"assembled","error":null,"terminal":true}),
                observations,
            )
        }
        "capability.required_prestart_failure" => {
            let empty =
                ProviderComposition::new(ProviderCompositionInputs::default()).expect("empty");
            let result = assembled(&fixture, case_id, Arc::new(empty)).await;
            match result {
                Err(CodingHarnessError::Composition(CodingRuntimeError::MissingCapability {
                    capability,
                })) => {
                    assert_eq!(capability, "models.resolve");
                }
                other => panic!("unexpected pre-start result: {other:?}"),
            }
            envelope(
                case_id,
                json!({"kind":"assemble","operation":"preflight","capability":"models.resolve"}),
                json!({"kind":"error","error":{"type":"MissingCapability","capability":"models.resolve"},"terminal":true}),
                effects,
            )
        }
        other => {
            let observation = assembled(
                &fixture,
                other,
                Arc::new(composition_for(&fixture.model, true)),
            )
            .await
            .unwrap_or_else(|error| panic!("{other}: assembled operation failed: {error}"));
            envelope(
                other,
                json!({"kind":"assemble","operation":other}),
                json!({"kind":"boundary","error":null,"terminal":true}),
                observed_effects(&observation),
            )
        }
    }
}

fn compare_with_node(case_id: &str, actual: Value) {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut child = Command::new("/usr/bin/node")
        .current_dir(root)
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("NODE_NO_WARNINGS", "1")
        .arg("conformance/coding-runtime/compare-actual.mjs")
        .arg(case_id)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("node comparator");
    child
        .stdin
        .take()
        .expect("stdin")
        .write_all(&serde_json::to_vec(&actual).expect("json"))
        .expect("write actual");
    let output = child.wait_with_output().expect("wait comparator");
    assert!(
        output.status.success(),
        "{case_id}: comparator failed\nstdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn assembled_coding_runtime_rows_compare_through_node() {
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    for case_id in [
        "model.credentials_catalog",
        "prompt.system_resources",
        "prompt.settings_snapshot",
        "capability.required_prestart_failure",
    ] {
        compare_with_node(case_id, runtime.block_on(actual(case_id)));
    }
}
