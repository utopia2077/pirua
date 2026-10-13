use pi_agent::schema::{
    AgentHarness, AgentHarnessTool, AgentLane, AgentMessage, AgentTool, AgentToolResult,
    Cancellation, HarnessRuntimeError, IdGenerator, PromptInput, Session, SessionCreateOptions,
    SessionRepo, ToolExecutionError, ToolProgressSink,
};
use pi_agent_session::{
    context::SummaryRenderer,
    memory::{MemoryStorage, StorageClock},
    repo::{MemorySession, MemorySessionRepo},
};
use pi_ai::{
    ApiAdapter, ApiId, AssistantContent, AssistantMessage, AssistantMessageEvent, AssistantStream,
    AssistantStreamInit, AuthContext, AuthContextError, BoxFuture, CapabilityError, Clock,
    DeferredCancelRequest, DeferredFetchRequest, DoneReason, JsonObject, JsonValue, Model,
    ModelBaseUrl, ModelCost, ModelId, ModelInput, ModelsError, ModelsRegistry,
    ModelsRegistryOptions, Provider, ProviderAuth, ProviderId, PublicModelBaseUrl,
    RequestLifecycleAdapter, RequestLifecycleInvocation, RequestResponseHead, SafeUnsigned,
    SimpleStreamRequest, TaskSpawner, TextStreamRequest, ToolCall, ToolCallId, ToolName, Usage,
    create_assistant_message_event_stream,
};
use pi_coding_runtime::{
    CodingHarness, CodingHarnessBuilder, CodingSettingsSnapshot, EnvError, ExecutionEnv,
    ExecutionToolContext, FileInfo, ModelRef, ProcessOptions, ProcessOutput, QueueMode, Resources,
    ThinkingLevel,
};
use pi_harness_runtime::owner::RuntimeFactory;
use pi_telemetry::NOOP_TELEMETRY_CONTEXT;
use serde_json::json;
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

struct TestClock;
impl Clock for TestClock {
    fn now_unix_ms(&self) -> u64 {
        7
    }
    fn sleep<'a>(
        &'a self,
        _: Duration,
        _: &'a pi_ai::CancellationToken,
    ) -> BoxFuture<'a, Result<(), pi_ai::Cancelled>> {
        Box::pin(async { Ok(()) })
    }
}
impl StorageClock for TestClock {
    fn now(&self) -> Result<SafeUnsigned, pi_agent::schema::StorageError> {
        Ok(SafeUnsigned::new(7).unwrap())
    }
}
struct Spawner;
impl TaskSpawner for Spawner {
    fn spawn(&self, task: BoxFuture<'static, ()>) -> Result<(), pi_ai::SpawnError> {
        tokio::spawn(task);
        Ok(())
    }
}
#[derive(Default)]
struct Ids(AtomicUsize);
impl IdGenerator for Ids {
    fn next(&self, _: Option<SafeUnsigned>) -> String {
        format!("shell-id-{}", self.0.fetch_add(1, Ordering::SeqCst))
    }
}
struct Auth;
impl AuthContext for Auth {
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
struct Summary;
impl SummaryRenderer for Summary {
    fn render(
        &self,
        _: pi_agent_session::context::ContextSummary,
    ) -> BoxFuture<'_, Result<AgentMessage, HarnessRuntimeError>> {
        panic!("compaction disabled")
    }
}
struct TestProvider {
    id: ProviderId,
    models: Vec<Model>,
    auth: ProviderAuth,
    adapter: Arc<Adapter>,
}
impl Provider for TestProvider {
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
struct Adapter {
    api: ApiId,
    responses: Mutex<VecDeque<Vec<AssistantContent>>>,
}
impl Adapter {
    fn shells(&self) {
        self.responses.lock().unwrap().extend([
            vec![
                call("bash", json!({"command":"echo current", "timeout":2.5})),
                call(
                    "powershell",
                    json!({"command":"Write-Output current", "timeout":3}),
                ),
            ],
            Vec::new(),
        ]);
    }
    fn start(
        &self,
        request: SimpleStreamRequest,
        lifecycle: RequestLifecycleInvocation,
    ) -> AssistantStream {
        let model = request.resolved.model;
        let content = self
            .responses
            .lock()
            .unwrap()
            .pop_front()
            .expect("queued response");
        let uses_tools = !content.is_empty();
        let message = AssistantMessage {
            role: Default::default(),
            content,
            api: model.api.clone(),
            provider: model.provider.clone(),
            model: model.id.clone(),
            response_model: None,
            response_id: None,
            diagnostics: None,
            usage: Usage::zero(),
            stop_reason: if uses_tools {
                pi_ai::StopReason::ToolUse
            } else {
                pi_ai::StopReason::Stop
            },
            deferred: None,
            error_message: None,
            raw_stop_reason: None,
            end_turn: None,
            timestamp: SafeUnsigned::new(7).unwrap(),
        };
        let (stream, producer) = create_assistant_message_event_stream(AssistantStreamInit {
            api: model.api,
            provider: model.provider,
            model: model.id,
            clock: request.options.runtime.clock,
            cancellation: request.options.runtime.cancellation,
        });
        lifecycle.recorder.record(RequestResponseHead {
            status: 200,
            headers: Vec::new(),
        });
        producer
            .push(AssistantMessageEvent::Done {
                reason: if uses_tools {
                    DoneReason::ToolUse
                } else {
                    DoneReason::Stop
                },
                message: Box::new(message),
            })
            .unwrap();
        stream
    }
}
impl ApiAdapter for Adapter {
    fn api_id(&self) -> &ApiId {
        &self.api
    }
    fn stream(&self, _: TextStreamRequest) -> AssistantStream {
        panic!("lifecycle route required")
    }
    fn stream_simple(&self, _: SimpleStreamRequest) -> AssistantStream {
        panic!("lifecycle route required")
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
impl RequestLifecycleAdapter for Adapter {
    fn stream_with_lifecycle(
        &self,
        _: TextStreamRequest,
        _: RequestLifecycleInvocation,
    ) -> AssistantStream {
        panic!("simple route required")
    }
    fn stream_simple_with_lifecycle(
        &self,
        request: SimpleStreamRequest,
        lifecycle: RequestLifecycleInvocation,
    ) -> AssistantStream {
        self.start(request, lifecycle)
    }
}
fn call(name: &str, arguments: serde_json::Value) -> AssistantContent {
    AssistantContent::ToolCall(ToolCall {
        content_type: Default::default(),
        id: ToolCallId::new(format!("shell-{name}")).unwrap(),
        name: ToolName::new(name).unwrap(),
        arguments: serde_json::from_value(arguments).unwrap(),
        thought_signature: None,
        namespace: None,
    })
}
fn model(provider: &str, id: &str) -> Model {
    Model {
        id: ModelId::new(id).unwrap(),
        name: id.into(),
        api: ApiId::new(ApiId::OPENAI_COMPLETIONS).unwrap(),
        provider: ProviderId::new(provider).unwrap(),
        base_url: ModelBaseUrl::Public(PublicModelBaseUrl::new("https://example.invalid").unwrap()),
        reasoning: true,
        thinking_level_map: None,
        input: vec![ModelInput::Text],
        cost: ModelCost {
            input: 0.0.try_into().unwrap(),
            output: 0.0.try_into().unwrap(),
            cache_read: 0.0.try_into().unwrap(),
            cache_write: 0.0.try_into().unwrap(),
            tiers: None,
        },
        context_window: SafeUnsigned::new(4096).unwrap(),
        max_tokens: SafeUnsigned::new(128).unwrap(),
        sampling_params: None,
        headers: None,
        compat: None,
    }
}
#[derive(Clone, Debug)]
struct HostCall {
    shell: String,
    command: String,
    options: ProcessOptions,
}
#[derive(Default)]
struct Host {
    calls: Mutex<Vec<HostCall>>,
}
impl ExecutionEnv for Host {
    fn absolute_path<'a>(&'a self, _: &'a str) -> BoxFuture<'a, Result<String, EnvError>> {
        panic!("shell does not resolve paths")
    }
    fn exists<'a>(&'a self, _: &'a str) -> BoxFuture<'a, Result<bool, EnvError>> {
        panic!("unused filesystem")
    }
    fn read_binary_file<'a>(&'a self, _: &'a str) -> BoxFuture<'a, Result<Vec<u8>, EnvError>> {
        panic!("unused filesystem")
    }
    fn read_text_file<'a>(&'a self, _: &'a str) -> BoxFuture<'a, Result<String, EnvError>> {
        panic!("unused filesystem")
    }
    fn write_file<'a>(&'a self, _: &'a str, _: &'a [u8]) -> BoxFuture<'a, Result<(), EnvError>> {
        panic!("unused filesystem")
    }
    fn list_dir<'a>(&'a self, _: &'a str) -> BoxFuture<'a, Result<Vec<FileInfo>, EnvError>> {
        panic!("unused filesystem")
    }
    fn file_info<'a>(&'a self, _: &'a str) -> BoxFuture<'a, Result<FileInfo, EnvError>> {
        panic!("unused filesystem")
    }
    fn canonical_path<'a>(&'a self, _: &'a str) -> BoxFuture<'a, Result<String, EnvError>> {
        panic!("unused filesystem")
    }
    fn exec<'a>(
        &'a self,
        shell: &'a str,
        command: &'a str,
        options: ProcessOptions,
    ) -> BoxFuture<'a, Result<ProcessOutput, EnvError>> {
        self.calls.lock().unwrap().push(HostCall {
            shell: shell.into(),
            command: command.into(),
            options,
        });
        Box::pin(async {
            Ok(ProcessOutput {
                stdout: "host shell output".into(),
                stderr: String::new(),
                merged: None,
                exit_code: 0,
            })
        })
    }
    fn create_temp_file<'a>(
        &'a self,
        _: &'a str,
        _: &'a [u8],
    ) -> BoxFuture<'a, Result<String, EnvError>> {
        panic!("host output fits")
    }
}
struct Fixture {
    first: Model,
    second: Model,
    adapter: Arc<Adapter>,
    host: Arc<Host>,
    repo: MemorySessionRepo,
    session: Arc<MemorySession>,
    registry: Arc<ModelsRegistry>,
    factory: Arc<RuntimeFactory<ExecutionToolContext, MemoryStorage>>,
}
impl Fixture {
    async fn new() -> Self {
        let clock = Arc::new(TestClock);
        let spawner = Arc::new(Spawner);
        let ids = Arc::new(Ids::default());
        let first = model("openai", "shell-first");
        let second = model("deepseek", "shell-second");
        let adapter = Arc::new(Adapter {
            api: first.api.clone(),
            responses: Mutex::new(VecDeque::new()),
        });
        let registry = Arc::new(ModelsRegistry::new(ModelsRegistryOptions {
            credentials: Arc::new(pi_ai::InMemoryCredentialStore::new()),
            models_store: Box::new(pi_ai::InMemoryModelsStore::new()),
            auth_context: Arc::new(Auth),
            clock: clock.clone(),
            spawner: spawner.clone(),
            model_header_policy: Default::default(),
        }));
        for selected in [&first, &second] {
            registry
                .set_provider(Arc::new(TestProvider {
                    id: selected.provider.clone(),
                    models: vec![selected.clone()],
                    auth: ProviderAuth::no_auth(),
                    adapter: adapter.clone(),
                }))
                .await
                .unwrap();
        }
        let repo = MemorySessionRepo::new(
            Default::default(),
            Default::default(),
            ids.clone(),
            clock.clone(),
            Arc::new(NOOP_TELEMETRY_CONTEXT),
        )
        .unwrap();
        let session = repo
            .create(SessionCreateOptions {
                id: Some("shell-session".parse().unwrap()),
                ..Default::default()
            })
            .await
            .unwrap();
        let host = Arc::new(Host::default());
        let factory = Arc::new(RuntimeFactory::new(
            clock,
            spawner,
            ids,
            Arc::new(ExecutionToolContext { env: host.clone() }),
            Arc::new(Summary),
        ));
        Self {
            first,
            second,
            adapter,
            host,
            repo,
            session,
            registry,
            factory,
        }
    }
    fn builder(&self, session: Arc<MemorySession>) -> CodingHarnessBuilder<MemoryStorage> {
        CodingHarnessBuilder::new(
            self.factory.clone(),
            session,
            self.registry.clone(),
            self.host.clone(),
            Resources::default(),
            CodingSettingsSnapshot::new(
                ModelRef::new(self.first.provider.clone(), self.first.id.clone()),
                ThinkingLevel::Low,
                vec![
                    ToolName::new("bash").unwrap(),
                    ToolName::new("powershell").unwrap(),
                ],
                pi_ai::RetryPolicy {
                    enabled: false,
                    max_retries: pi_ai::MaxRetries::new(0).unwrap(),
                    base_delay_ms: SafeUnsigned::new(0).unwrap(),
                },
                pi_coding_runtime::CompactionSettings {
                    enabled: false,
                    reserve_tokens: SafeUnsigned::new(0).unwrap(),
                    keep_recent_tokens: SafeUnsigned::new(0).unwrap(),
                },
                Default::default(),
                QueueMode::All,
                QueueMode::All,
            ),
        )
    }
    async fn run_shells(
        &self,
        coding: &CodingHarness<MemoryStorage>,
        selected: &Model,
        thinking: &str,
    ) {
        self.adapter.shells();
        coding
            .harness()
            .prompt(PromptInput::Text {
                text: "run both shells".into(),
                images: Vec::new(),
            })
            .await
            .unwrap()
            .unwrap();
        coding.harness().wait_for_idle().await.unwrap();
        let calls = std::mem::take(&mut *self.host.calls.lock().unwrap());
        assert_eq!(calls.len(), 2, "both shell bindings must reach the host");
        let expected = identity(coding.session().metadata().id.as_str(), selected, thinking);
        for host in &calls {
            assert_eq!(
                host.options.env, expected,
                "actual {} host-call identity",
                host.shell
            );
            assert_eq!(host.options.cwd, None);
            assert!(
                !host
                    .options
                    .abort_signal
                    .as_ref()
                    .unwrap()
                    .load(Ordering::SeqCst)
            );
        }
        assert_eq!(calls[0].shell, "bash");
        assert_eq!(calls[0].command, "echo current");
        assert_eq!(calls[0].options.timeout, Some(Duration::from_millis(2500)));
        assert_eq!(calls[1].shell, "powershell");
        assert_eq!(
            calls[1].command,
            "try { [Console]::OutputEncoding=[System.Text.Encoding]::UTF8 } catch {}\nWrite-Output current"
        );
        assert_eq!(calls[1].options.timeout, Some(Duration::from_secs(3)));
    }
}
fn identity(session: &str, model: &Model, thinking: &str) -> BTreeMap<String, String> {
    [
        ("PI_SESSION_ID", session),
        ("PI_SESSION_FILE", ""),
        ("PI_PROVIDER", model.provider.as_str()),
        ("PI_MODEL", model.id.as_str()),
        ("PI_REASONING_LEVEL", thinking),
    ]
    .into_iter()
    .map(|(key, value)| (key.into(), value.into()))
    .collect()
}
#[test]
fn assembled_shell_context_tracks_initial_direct_and_public_model_changes() {
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let fixture = Fixture::new().await;
        let coding = fixture
            .builder(fixture.session.clone())
            .first_attachment()
            .build()
            .await
            .unwrap();
        fixture.run_shells(&coding, &fixture.first, "low").await;
        coding
            .harness()
            .set_model(fixture.second.clone())
            .await
            .unwrap();
        coding
            .harness()
            .set_thinking_level(ThinkingLevel::High)
            .await
            .unwrap();
        fixture.run_shells(&coding, &fixture.second, "high").await;
        coding
            .switch_model(fixture.first.provider.clone(), fixture.first.id.clone())
            .await
            .unwrap();
        coding
            .harness()
            .set_thinking_level(ThinkingLevel::Off)
            .await
            .unwrap();
        fixture.run_shells(&coding, &fixture.first, "off").await;
        coding.harness().close().await.unwrap();
        let weak = Arc::downgrade(coding.harness());
        drop(coding);
        assert!(
            weak.upgrade().is_none(),
            "tool context must not retain the Harness owner"
        );
    });
}
#[test]
fn assembled_shell_context_restores_authority_on_a_new_session_backend() {
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let fixture = Fixture::new().await;
        let coding = fixture
            .builder(fixture.session.clone())
            .first_attachment()
            .build()
            .await
            .unwrap();
        coding
            .switch_model(fixture.second.provider.clone(), fixture.second.id.clone())
            .await
            .unwrap();
        coding
            .harness()
            .set_thinking_level(ThinkingLevel::Medium)
            .await
            .unwrap();
        let metadata = coding.session().metadata();
        coding.harness().close().await.unwrap();
        coding.session().close().await.unwrap();
        drop(coding);
        let reopened = fixture.repo.open(metadata).await.unwrap();
        assert!(!Arc::ptr_eq(&fixture.session, &reopened));
        let restored = fixture.builder(reopened.clone()).build().await.unwrap();
        fixture
            .run_shells(&restored, &fixture.second, "medium")
            .await;
        restored.harness().close().await.unwrap();
        reopened.close().await.unwrap();
    });
}

struct OverrideTool {
    descriptor: AgentTool,
    abort: Arc<std::sync::atomic::AtomicBool>,
}
impl AgentHarnessTool<ExecutionToolContext> for OverrideTool {
    fn descriptor(&self) -> &AgentTool {
        &self.descriptor
    }
    fn prepare_arguments(&self, args: JsonValue) -> Result<JsonObject, ToolExecutionError> {
        let JsonValue::Object(args) = args else {
            panic!("object arguments")
        };
        Ok(args)
    }
    fn execute<'a>(
        &'a self,
        _: ToolCallId,
        _: JsonObject,
        _: &'a dyn Cancellation,
        _: &'a dyn ToolProgressSink,
        ctx: &'a ExecutionToolContext,
    ) -> BoxFuture<'a, Result<AgentToolResult, ToolExecutionError>> {
        Box::pin(async move {
            let mut env: BTreeMap<String, String> = [
                "PI_SESSION_ID",
                "PI_SESSION_FILE",
                "PI_PROVIDER",
                "PI_MODEL",
                "PI_REASONING_LEVEL",
            ]
            .into_iter()
            .map(|key| (key.into(), "stale".into()))
            .collect();
            env.insert("HOST_KEEP".into(), "host-value".into());
            let output = ctx
                .env
                .exec(
                    "host-shell",
                    "host-command",
                    ProcessOptions {
                        cwd: Some("host-cwd".into()),
                        timeout: Some(Duration::from_millis(1234)),
                        env,
                        abort_signal: Some(self.abort.clone()),
                    },
                )
                .await
                .unwrap();
            assert_eq!(output.stdout, "host shell output");
            Ok(AgentToolResult {
                content: Vec::new(),
                details: JsonValue::Null,
                usage: None,
                added_tool_names: None,
                terminate: None,
            })
        })
    }
}
#[test]
fn assembled_context_overrides_stale_pi_entries_and_preserves_host_options() {
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let fixture = Fixture::new().await;
        let abort = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let descriptor = AgentTool {
            name: ToolName::new("override").unwrap(),
            label: "override".into(),
            description: "host option witness".into(),
            parameters: serde_json::from_value(json!({"type":"object","properties":{}})).unwrap(),
            constrained_sampling: None,
            execution_mode: None,
            replay: None,
        };
        let tool = Arc::new(OverrideTool {
            descriptor,
            abort: abort.clone(),
        });
        let coding = fixture
            .builder(fixture.session.clone())
            .with_tools(vec![tool])
            .first_attachment()
            .build()
            .await
            .unwrap();
        coding
            .harness()
            .set_active_tools(vec![ToolName::new("override").unwrap()])
            .await
            .unwrap();
        fixture
            .adapter
            .responses
            .lock()
            .unwrap()
            .extend([vec![call("override", json!({}))], Vec::new()]);
        coding
            .harness()
            .prompt(PromptInput::Text {
                text: "run override".into(),
                images: Vec::new(),
            })
            .await
            .unwrap()
            .unwrap();
        coding.harness().wait_for_idle().await.unwrap();
        let calls = fixture.host.calls.lock().unwrap().clone();
        assert_eq!(calls.len(), 1);
        let mut expected = identity("shell-session", &fixture.first, "low");
        expected.insert("HOST_KEEP".into(), "host-value".into());
        assert_eq!(calls[0].options.env, expected);
        assert_eq!(calls[0].shell, "host-shell");
        assert_eq!(calls[0].command, "host-command");
        assert_eq!(calls[0].options.cwd.as_deref(), Some("host-cwd"));
        assert_eq!(calls[0].options.timeout, Some(Duration::from_millis(1234)));
        assert!(Arc::ptr_eq(
            calls[0].options.abort_signal.as_ref().unwrap(),
            &abort
        ));
        coding.harness().close().await.unwrap();
        assert!(
            coding
                .harness()
                .prompt(PromptInput::Text {
                    text: "closed".into(),
                    images: Vec::new()
                })
                .await
                .unwrap()
                .is_err()
        );
        assert_eq!(
            fixture.host.calls.lock().unwrap().len(),
            1,
            "closed owner cannot dispatch a stale context"
        );
        let weak = Arc::downgrade(coding.harness());
        drop(coding);
        assert!(weak.upgrade().is_none());
        let host = Arc::downgrade(&fixture.host);
        drop(fixture);
        assert!(
            host.upgrade().is_none(),
            "context must not leak the host environment"
        );
    });
}
