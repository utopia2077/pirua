use pi_agent::schema::{
    AgentHarness, AgentLane, AgentMessage, EntryQuery, EventListener, HandlerError, HarnessEvent,
    HarnessEventType, HarnessRuntimeError, IdGenerator, PromptInput, RunOutcome,
    SessionCreateOptions, SessionRepo, SessionTree,
};
use pi_agent_session::{
    context::SummaryRenderer,
    memory::{MemoryStorage, StorageClock},
    repo::{MemorySession, MemorySessionRepo},
};
use pi_ai::{
    ApiAdapter, ApiId, AssistantContent, AssistantMessage, AssistantMessageEvent, AssistantStream,
    AssistantStreamInit, AuthContext, AuthContextError, BoxFuture, CapabilityError, Clock,
    DeferredCancelRequest, DeferredFetchRequest, DoneReason, Message, Model, ModelBaseUrl,
    ModelCost, ModelId, ModelInput, ModelsError, ModelsRegistry, ModelsRegistryOptions, Provider,
    ProviderAuth, ProviderId, PublicModelBaseUrl, RequestLifecycleAdapter,
    RequestLifecycleInvocation, RequestResponseHead, SafeUnsigned, SimpleStreamOptions,
    SimpleStreamOptionsWire, SimpleStreamRequest, StreamSetupFailure, TaskSpawner, TextContent,
    TextStreamRequest, ToolName, Usage, UserContent, create_assistant_message_event_stream,
};
use pi_coding_runtime::{
    CodingHarnessBuilder, CodingHarnessError, CodingMessageConverter, CodingRuntimeError,
    CodingSettingsSnapshot, ConfiguredModelOverlay, CredentialInputs, ExecutionEnv,
    LocalExecutionEnv, ModelAvailability, ModelRef, ProviderComposition, ProviderCompositionInputs,
    QueueMode, Resources, ThinkingLevel,
};
use pi_harness_runtime::owner::RuntimeFactory;
use pi_telemetry::NOOP_TELEMETRY_CONTEXT;
use serde_json::{Value, json};
use std::{
    collections::VecDeque,
    io::Write,
    process::{Command, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

const AUTHORITY: &str = "853a80d26c90a14c1886f0ebb8ffaae133ca2185";

mod coding_runtime_m6b_support;
mod coding_runtime_m6c_support;

struct FixtureClock {
    now: std::sync::atomic::AtomicU64,
    sleeps: Mutex<Vec<u64>>,
}
impl FixtureClock {
    fn new() -> Self {
        Self {
            now: std::sync::atomic::AtomicU64::new(7),
            sleeps: Mutex::new(Vec::new()),
        }
    }
}
impl Clock for FixtureClock {
    fn now_unix_ms(&self) -> u64 {
        self.now.load(Ordering::SeqCst)
    }
    fn sleep<'a>(
        &'a self,
        duration: Duration,
        cancellation: &'a pi_ai::CancellationToken,
    ) -> BoxFuture<'a, Result<(), pi_ai::Cancelled>> {
        Box::pin(async move {
            if cancellation.is_cancelled() {
                Err(pi_ai::Cancelled)
            } else {
                let elapsed = u64::try_from(duration.as_millis()).expect("bounded sleep");
                self.sleeps.lock().expect("clock sleeps").push(elapsed);
                self.now.fetch_add(elapsed, Ordering::SeqCst);
                Ok(())
            }
        })
    }
}
impl StorageClock for FixtureClock {
    fn now(&self) -> Result<SafeUnsigned, pi_agent::schema::StorageError> {
        Ok(SafeUnsigned::new(self.now_unix_ms()).expect("bounded fixture time"))
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
    resolved_models: Mutex<Vec<Model>>,
    responses: Mutex<VecDeque<AssistantMessage>>,
    contexts: Mutex<Vec<pi_ai::Context>>,
    emitted_responses: Arc<Mutex<Vec<AssistantMessage>>>,
    lifecycle: Arc<Mutex<Vec<String>>>,
    hold: Mutex<Option<pi_ai::CancellationToken>>,
    active_streams: Arc<AtomicUsize>,
}

impl StopAdapter {
    fn new(model: &Model) -> Arc<Self> {
        let response = AssistantMessage {
            role: Default::default(),
            content: vec![AssistantContent::Text(TextContent {
                content_type: Default::default(),
                text: "deterministic terminal".into(),
                text_signature: None,
            })],
            api: model.api.clone(),
            provider: model.provider.clone(),
            model: model.id.clone(),
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
        Arc::new(Self {
            model: model.clone(),
            calls: AtomicUsize::new(0),
            resolved_models: Mutex::new(Vec::new()),
            responses: Mutex::new(VecDeque::from([response.clone(), response])),
            contexts: Mutex::new(Vec::new()),
            emitted_responses: Arc::new(Mutex::new(Vec::new())),
            lifecycle: Arc::new(Mutex::new(Vec::new())),
            hold: Mutex::new(None),
            active_streams: Arc::new(AtomicUsize::new(0)),
        })
    }

    fn last_resolved_model(&self) -> Option<Model> {
        self.resolved_models
            .lock()
            .expect("resolved models")
            .last()
            .cloned()
    }

    fn start(
        &self,
        request: SimpleStreamRequest,
        lifecycle: RequestLifecycleInvocation,
    ) -> AssistantStream {
        self.resolved_models
            .lock()
            .expect("resolved models")
            .push(request.resolved.model.clone());
        let request_number = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        self.contexts
            .lock()
            .expect("contexts")
            .push(request.context.clone());
        self.lifecycle
            .lock()
            .expect("lifecycle")
            .push(format!("dispatch:{request_number}"));
        let response = self
            .responses
            .lock()
            .expect("responses")
            .pop_front()
            .unwrap_or_else(|| {
                coding_runtime_m6b_support::assistant(
                    &self.model,
                    pi_ai::StopReason::Stop,
                    "deterministic terminal",
                )
            });
        let (stream, producer) = create_assistant_message_event_stream(AssistantStreamInit {
            api: self.model.api.clone(),
            provider: self.model.provider.clone(),
            model: self.model.id.clone(),
            clock: request.options.runtime.clock.clone(),
            cancellation: request.options.runtime.cancellation.clone(),
        });
        let model = self.model.clone();
        let lifecycle_trace = self.lifecycle.clone();
        let hold = self.hold.lock().expect("hold").take();
        let cancellation = request.options.runtime.cancellation.clone();
        let active_streams = self.active_streams.clone();
        let emitted_responses = self.emitted_responses.clone();
        active_streams.fetch_add(1, Ordering::SeqCst);
        request
            .options
            .runtime
            .spawner
            .spawn(Box::pin(async move {
                let _cleanup = coding_runtime_m6b_support::StreamCleanup(active_streams);
                let mut payload = serde_json::json!({"m6":"assembled"});
                if let Some(transform) = lifecycle.payload {
                    match transform.transform(&model, &mut payload).await {
                        Ok(Some(value)) => payload = value,
                        Ok(None) => {}
                        Err(_) => {
                            let _ = producer.fail_setup(StreamSetupFailure::Adapter);
                            return;
                        }
                    }
                }
                let _ = payload;
                lifecycle_trace
                    .lock()
                    .expect("lifecycle")
                    .push(format!("payload:{request_number}"));
                if let Some(hold) = hold {
                    let mut partial = response.clone();
                    partial.stop_reason = pi_ai::StopReason::Pending;
                    partial.error_message = None;
                    producer
                        .push(AssistantMessageEvent::Start {
                            partial: Box::new(partial),
                        })
                        .expect("stream start");
                    while !hold.is_cancelled() && !cancellation.is_cancelled() {
                        tokio::task::yield_now().await;
                    }
                    if cancellation.is_cancelled() {
                        lifecycle_trace
                            .lock()
                            .expect("lifecycle")
                            .push(format!("cancelled:{request_number}"));
                        return;
                    }
                }
                lifecycle.recorder.record(RequestResponseHead {
                    status: 200,
                    headers: Vec::new(),
                });
                lifecycle_trace
                    .lock()
                    .expect("lifecycle")
                    .push(format!("response:{request_number}:200"));
                emitted_responses
                    .lock()
                    .expect("emitted responses")
                    .push(response.clone());
                let event = match response.stop_reason {
                    pi_ai::StopReason::Error => AssistantMessageEvent::Error {
                        reason: pi_ai::ErrorReason::Error,
                        error: Box::new(response),
                    },
                    pi_ai::StopReason::Aborted => AssistantMessageEvent::Error {
                        reason: pi_ai::ErrorReason::Aborted,
                        error: Box::new(response),
                    },
                    reason => AssistantMessageEvent::Done {
                        reason: match reason {
                            pi_ai::StopReason::ToolUse => DoneReason::ToolUse,
                            pi_ai::StopReason::Length => DoneReason::Length,
                            _ => DoneReason::Stop,
                        },
                        message: Box::new(response),
                    },
                };
                let reason = match &event {
                    AssistantMessageEvent::Done { message, .. } => message.stop_reason,
                    AssistantMessageEvent::Error { error, .. } => error.stop_reason,
                    _ => unreachable!(),
                };
                lifecycle_trace.lock().expect("lifecycle").push(format!(
                    "terminal:{request_number}:{}",
                    serde_json::to_value(reason)
                        .expect("reason")
                        .as_str()
                        .expect("reason string")
                ));
                producer.push(event).expect("stop response");
            }))
            .expect("fixture task spawn");
        stream
    }
}

impl ApiAdapter for StopAdapter {
    fn api_id(&self) -> &ApiId {
        &self.model.api
    }
    fn stream(&self, _: TextStreamRequest) -> AssistantStream {
        panic!("raw stream route is not used")
    }
    fn stream_simple(&self, _: SimpleStreamRequest) -> AssistantStream {
        panic!("raw simple stream route is not used")
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
        lifecycle: RequestLifecycleInvocation,
    ) -> AssistantStream {
        self.start(
            SimpleStreamRequest {
                resolved: request.resolved,
                context: request.context,
                auth: request.auth,
                options: SimpleStreamOptions {
                    wire: SimpleStreamOptionsWire {
                        stream: request.options.wire,
                        ..Default::default()
                    },
                    runtime: request.options.runtime,
                },
            },
            lifecycle,
        )
    }
    fn stream_simple_with_lifecycle(
        &self,
        request: SimpleStreamRequest,
        lifecycle: RequestLifecycleInvocation,
    ) -> AssistantStream {
        self.start(request, lifecycle)
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
    clock: Arc<FixtureClock>,
    repo: Arc<MemorySessionRepo>,
}

async fn fixture() -> Fixture {
    fixture_with_environment(Arc::new(LocalExecutionEnv::new(std::env::temp_dir()))).await
}

async fn fixture_with_environment(environment: Arc<dyn ExecutionEnv>) -> Fixture {
    let clock = Arc::new(FixtureClock::new());
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
            auth: ProviderAuth::no_auth(),
            adapter: adapter.clone(),
        }))
        .await
        .expect("provider publication");
    let repo = Arc::new(
        MemorySessionRepo::new(
            Default::default(),
            Default::default(),
            ids.clone(),
            clock.clone(),
            Arc::new(NOOP_TELEMETRY_CONTEXT),
        )
        .expect("repo"),
    );
    let session = repo
        .create(SessionCreateOptions {
            id: Some("m6-session".parse().expect("session id")),
            ..Default::default()
        })
        .await
        .expect("session");
    let factory = Arc::new(RuntimeFactory::new(
        clock.clone(),
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
        clock,
        repo,
    }
}

fn envelope(case_id: &str, input: Value, outcome: Value, observations: Value) -> Value {
    envelope_with_events(case_id, input, json!([]), outcome, observations)
}

fn envelope_with_events(
    case_id: &str,
    input: Value,
    events: Value,
    outcome: Value,
    observations: Value,
) -> Value {
    json!({ "schemaVersion": 1, "authority": AUTHORITY, "caseId": case_id, "input": input, "events": events, "messages": [], "outcome": outcome, "observations": observations })
}

fn observed_effects(observation: &AssemblyObservation) -> Value {
    json!({
        "provider_calls": observation.provider_calls,
        "tool_invocations": observation.tool_invocations,
        "session_publication_events": observation.session_publication_events,
        "durable_entries": observation.durable_entries,
        "terminal_kind": observation.terminal_kind,
        "runtime_model_match": observation.runtime_model_match,
        "resources_match": observation.resources_match,
        "settings_match": observation.settings_match,
    })
}

fn same_validated_model_metadata(left: &Model, right: &Model) -> bool {
    left.provider == right.provider
        && left.id == right.id
        && left.name == right.name
        && left.base_url == right.base_url
        && left == right
}

fn model_observation(model: &Model) -> Value {
    let base_url = match &model.base_url {
        ModelBaseUrl::Public(url) => url.as_str(),
        ModelBaseUrl::RuntimeResolved => "",
    };
    json!({
        "provider": model.provider,
        "id": model.id,
        "name": model.name,
        "base_url": base_url,
        "source": "overlay",
        "available": true,
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
struct EventCount(AtomicUsize);

impl EventListener for EventCount {
    fn on_event(
        &self,
        _event: HarnessEvent,
    ) -> pi_agent::schema::BoxFuture<'_, Result<(), HandlerError>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(()) })
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct EffectSnapshot {
    provider_calls: usize,
    tool_invocations: usize,
    session_publication_events: usize,
    durable_entries: usize,
    stats_message_count: usize,
    stats_usage_total: usize,
}

impl EffectSnapshot {
    fn delta(self, before: Self) -> Self {
        Self {
            provider_calls: self.provider_calls.saturating_sub(before.provider_calls),
            tool_invocations: self
                .tool_invocations
                .saturating_sub(before.tool_invocations),
            session_publication_events: self
                .session_publication_events
                .saturating_sub(before.session_publication_events),
            durable_entries: self.durable_entries.saturating_sub(before.durable_entries),
            stats_message_count: self
                .stats_message_count
                .saturating_sub(before.stats_message_count),
            stats_usage_total: self
                .stats_usage_total
                .saturating_sub(before.stats_usage_total),
        }
    }
}

async fn effect_snapshot(
    fixture: &Fixture,
    publication_events: &EventCount,
    tool_invocations: &EventCount,
) -> EffectSnapshot {
    let durable_entries = fixture
        .session
        .find_entries(EntryQuery::default())
        .await
        .expect("session entries")
        .len();
    let stats = fixture.session.get_stats().await.expect("session stats");
    EffectSnapshot {
        provider_calls: fixture.adapter.calls.load(Ordering::SeqCst),
        tool_invocations: tool_invocations.0.load(Ordering::SeqCst),
        session_publication_events: publication_events.0.load(Ordering::SeqCst),
        durable_entries,
        stats_message_count: stats.message_count.get() as usize,
        stats_usage_total: stats.usage.total_tokens.get() as usize,
    }
}

#[derive(Debug)]
struct AssemblyObservation {
    provider_calls: usize,
    durable_entries: usize,
    session_publication_events: usize,
    tool_invocations: usize,
    terminal_kind: String,
    request_model: Model,
    runtime_model_match: bool,
    resources_match: bool,
    settings_match: bool,
}

async fn assembled(
    fixture: &Fixture,
    case_id: &str,
    composition: Arc<ProviderComposition>,
    model_override: Option<Model>,
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
            prompt_templates: Some(vec![pi_coding_runtime::PromptTemplate {
                name: "build".into(),
                content: "fixture prompt".into(),
                description: Some("fixture".into()),
            }]),
        }
    } else {
        Resources::default()
    };
    let expected_resources = resources.clone();
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
    builder = builder.with_message_converter(Arc::new(
        CodingMessageConverter::new().expect("coding converter"),
    ));
    if let Some(model) = model_override.clone() {
        builder = builder.with_model_override(model);
    }
    if case_id == "prompt.system_resources" {
        builder = builder.with_system_prompt("m6 assembled system prompt");
    }
    let expected_model = model_override.as_ref().unwrap_or(&fixture.model);
    let harness = builder.build().await?;
    if let Some(overlay) = model_override.as_ref() {
        let host_model = fixture
            .registry
            .get_model(&overlay.provider, &overlay.id)
            .expect("host registry model");
        assert_eq!(host_model.provider, overlay.provider);
        assert_eq!(host_model.id, overlay.id);
        assert_ne!(
            host_model.name, overlay.name,
            "{case_id}: host and detached model names must differ"
        );
        assert_ne!(
            host_model.base_url, overlay.base_url,
            "{case_id}: host and detached public base URLs must differ"
        );
        assert!(
            !same_validated_model_metadata(&host_model, overlay),
            "{case_id}: host registry metadata must differ from the detached overlay"
        );
    }
    let publication_events = Arc::new(EventCount::default());
    let tool_invocations = Arc::new(EventCount::default());
    let subscriptions = vec![
        harness
            .harness()
            .events()
            .on(HarnessEventType::EntryAdded, publication_events.clone()),
        harness
            .harness()
            .events()
            .on(HarnessEventType::ToolStart, tool_invocations.clone()),
    ];
    let before_effects = effect_snapshot(fixture, &publication_events, &tool_invocations).await;
    let run = harness
        .harness()
        .prompt(PromptInput::Text {
            text: format!("m6 assembled operation: {case_id}"),
            images: Vec::new(),
        })
        .await;
    let terminal_kind = match run {
        Ok(Ok(success)) => match success.outcome {
            RunOutcome::CompletedWithAssistant(_) | RunOutcome::CompletedWithoutAssistant(_) => {
                "completed"
            }
            RunOutcome::AbortedWithAssistant(_) | RunOutcome::AbortedWithoutAssistant(_) => {
                "aborted"
            }
            RunOutcome::FailedWithAssistant(_) | RunOutcome::FailedWithoutAssistant(_) => "failed",
            RunOutcome::Deferred(_) | RunOutcome::MissingIdentities(_) => "suspended",
        },
        Ok(Err(result)) => panic!("{case_id}: assembled prompt rejected: {result:?}"),
        Err(error) => panic!("{case_id}: assembled prompt outer result failed: {error:?}"),
    }
    .to_owned();
    harness
        .harness()
        .wait_for_idle()
        .await
        .map_err(CodingHarnessError::Runtime)?;
    let host_model_identity_match = harness
        .harness()
        .get_model()
        .await
        .map_err(CodingHarnessError::Runtime)?
        .is_some_and(|model| {
            model.provider == expected_model.provider && model.id == expected_model.id
        });
    let runtime_model_match = host_model_identity_match
        && fixture
            .adapter
            .last_resolved_model()
            .is_some_and(|request_model| {
                same_validated_model_metadata(&request_model, expected_model)
            });
    let request_model = fixture
        .adapter
        .last_resolved_model()
        .unwrap_or_else(|| panic!("{case_id}: adapter must observe a resolved request model"));
    assert!(
        same_validated_model_metadata(&request_model, expected_model),
        "{case_id}: provider request model must equal the selected validated model metadata"
    );
    let resources_match = serde_json::to_value(
        harness
            .harness()
            .get_resources()
            .await
            .map_err(CodingHarnessError::Runtime)?,
    )
    .expect("resources JSON")
        == serde_json::to_value(&expected_resources).expect("expected resources JSON");
    let expected_settings = settings(&fixture.model);
    let settings_match = serde_json::to_value((
        harness
            .harness()
            .get_thinking_level()
            .await
            .map_err(CodingHarnessError::Runtime)?,
        harness
            .harness()
            .get_active_tools()
            .await
            .map_err(CodingHarnessError::Runtime)?,
        harness
            .harness()
            .get_retry_policy()
            .await
            .map_err(CodingHarnessError::Runtime)?,
        harness
            .harness()
            .get_compaction_settings()
            .await
            .map_err(CodingHarnessError::Runtime)?,
        harness
            .harness()
            .get_steering_mode()
            .await
            .map_err(CodingHarnessError::Runtime)?,
        harness
            .harness()
            .get_follow_up_mode()
            .await
            .map_err(CodingHarnessError::Runtime)?,
    ))
    .expect("settings JSON")
        == serde_json::to_value((
            expected_settings.thinking_level,
            expected_settings.active_tools,
            expected_settings.retry,
            expected_settings.compaction,
            expected_settings.steering_mode,
            expected_settings.follow_up_mode,
        ))
        .expect("expected settings JSON");
    let after_effects = effect_snapshot(fixture, &publication_events, &tool_invocations).await;
    let effects = after_effects.delta(before_effects);
    harness
        .harness()
        .close()
        .await
        .map_err(CodingHarnessError::Runtime)?;
    // Stop receiving after close before taking the detached event snapshot.
    drop(subscriptions);
    Ok(AssemblyObservation {
        provider_calls: effects.provider_calls,
        durable_entries: effects.durable_entries,
        session_publication_events: effects.session_publication_events,
        tool_invocations: effects.tool_invocations,
        terminal_kind,
        request_model,
        runtime_model_match,
        resources_match,
        settings_match,
    })
}

async fn actual(case_id: &str) -> Value {
    if coding_runtime_m6c_support::ROWS.contains(&case_id) {
        return coding_runtime_m6c_support::actual_operation(case_id).await;
    }
    if coding_runtime_m6b_support::ROWS.contains(&case_id) {
        return coding_runtime_m6b_support::actual_operation(case_id).await;
    }
    let fixture = fixture().await;
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
            let mut selected_model = selected.model.clone();
            selected_model.name = "offline-model-detached-overlay".to_owned();
            selected_model.base_url = ModelBaseUrl::Public(
                PublicModelBaseUrl::new("https://overlay.example.invalid/v1").expect("overlay URL"),
            );
            assert!(
                !same_validated_model_metadata(&fixture.model, &selected_model),
                "detached overlay must differ from the host registry metadata"
            );
            let available = composition_for(&selected_model, true);
            let selected_model = available
                .resolve_selection("offline")
                .expect("distinguishable available overlay")
                .model
                .clone();
            let observation =
                assembled(&fixture, case_id, Arc::new(available), Some(selected_model))
                    .await
                    .expect("assembled model row");
            let mut observations = observed_effects(&observation);
            observations["model"] = model_observation(&observation.request_model);
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
                None,
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
                None,
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
            let publication_events = EventCount::default();
            let tool_invocations = EventCount::default();
            let before_effects =
                effect_snapshot(&fixture, &publication_events, &tool_invocations).await;
            let empty =
                ProviderComposition::new(ProviderCompositionInputs::default()).expect("empty");
            let result = assembled(&fixture, case_id, Arc::new(empty), None).await;
            let after_effects =
                effect_snapshot(&fixture, &publication_events, &tool_invocations).await;
            let effects = after_effects.delta(before_effects);
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
                json!({
                    "provider_calls": effects.provider_calls,
                    "tool_invocations": effects.tool_invocations,
                    "session_publication_events": effects.session_publication_events,
                    "durable_entries": effects.durable_entries,
                }),
            )
        }
        other => panic!("unimplemented conformance row: {other}"),
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
        "{case_id}: comparator failed\nactual={}\nstdout={}\nstderr={}",
        serde_json::to_string_pretty(&actual).expect("actual json"),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn assembled_coding_runtime_rows_compare_through_node() {
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    let mut compared = 0;
    for case_id in [
        "model.credentials_catalog",
        "prompt.system_resources",
        "prompt.settings_snapshot",
        "tools.validate_progress_settle",
        "tools.coding_operations",
        "queue.steer_during_turn",
        "queue.follow_up_after_turn",
        "retry.overflow_precedence",
        "retry.provider_recovery",
        "abort.active_stream_cleanup",
        "compaction.context_projection",
        "branch.fork_atomic_publish",
        "restart.recover_session",
        "legacy_import.format3",
        "model_switch.next_run",
        "extension.participation_ordering",
        "lifecycle.builder_replace",
        "capability.required_prestart_failure",
    ] {
        #[cfg(not(feature = "extensions"))]
        if case_id == "extension.participation_ordering" {
            println!(
                "extension.participation_ordering requires the extensions feature; no participation evidence in this build"
            );
            continue;
        }
        compare_with_node(case_id, runtime.block_on(actual(case_id)));
        compared += 1;
        println!("assembled row compared: {case_id}");
    }
    let applicable = if cfg!(feature = "extensions") { 18 } else { 17 };
    assert_eq!(
        compared, applicable,
        "every applicable frozen row must compare"
    );
    println!("assembled coding-runtime conformance: {compared} applicable rows compared");
}

#[test]
fn coding_runtime_causal_corpus_runs_through_node() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let output = Command::new("/usr/bin/node")
        .current_dir(root)
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("NODE_NO_WARNINGS", "1")
        .arg("conformance/coding-runtime/causal.mjs")
        .output()
        .expect("run full Node causal self-test");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "Node causal self-test failed: status={}\nstdout={stdout}\nstderr={stderr}",
        output.status
    );
    for marker in [
        "causal self-test passed: 18 positives, 18 one-change negatives, fixture bytes restored",
        "causal corpus: 18 positives, 18 negatives, 0 pending; fixture and manifest bytes restored",
        "nested message probes passed: 4 semantic and same-mutated expected/actual rejections",
        "prior-row lifecycle field probes passed: 32 semantic and same-mutated expected/actual rejections",
        "foundation closure probes passed: 380 semantic and same-mutated expected/actual rejections across all four foundation rows",
    ] {
        assert!(
            stdout.contains(marker),
            "missing causal success marker {marker:?}\nstdout={stdout}\nstderr={stderr}"
        );
    }
    print!("{stdout}");
}
