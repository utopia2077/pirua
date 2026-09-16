#![allow(dead_code)]
use pi_agent_schema::*;
use pi_agent_session::{
    codec::SessionCodec,
    memory::{MemoryBacking, MemoryStorage, StorageClock},
    session::RuntimeSession,
};
use pi_ai_core::*;
use pi_telemetry::NOOP_TELEMETRY_CONTEXT;
use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    task::{Context as TaskContext, Poll, Waker},
    time::Duration,
};
pub fn n(value: u64) -> SafeUnsigned {
    SafeUnsigned::new(value).unwrap()
}
pub fn poll<F: Future + ?Sized>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut TaskContext::from_waker(Waker::noop()))
}
#[derive(Default)]
pub struct Pump {
    pub tasks: Mutex<Vec<pi_ai_core::BoxFuture<'static, ()>>>,
    pub reject: AtomicBool,
}
impl TaskSpawner for Pump {
    fn spawn(&self, task: pi_ai_core::BoxFuture<'static, ()>) -> Result<(), SpawnError> {
        if self.reject.load(Ordering::SeqCst) {
            return Err(SpawnError::new());
        }
        self.tasks.lock().unwrap().push(task);
        Ok(())
    }
}
impl Pump {
    pub fn tick(&self) {
        let tasks = std::mem::take(&mut *self.tasks.lock().unwrap());
        for mut task in tasks {
            if poll(task.as_mut()).is_pending() {
                self.tasks.lock().unwrap().push(task);
            }
        }
    }
    pub fn drive<F: Future>(&self, future: F) -> F::Output {
        let mut future = std::pin::pin!(future);
        for _ in 0..20000 {
            if let Poll::Ready(result) = poll(future.as_mut()) {
                return result;
            }
            self.tick();
        }
        panic!("retained pump did not complete");
    }
}
pub struct Time(pub AtomicU64);
impl Clock for Time {
    fn now_unix_ms(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
    fn sleep<'a>(
        &'a self,
        delay: Duration,
        cancellation: &'a CancellationToken,
    ) -> pi_ai_core::BoxFuture<'a, Result<(), Cancelled>> {
        let until = self.now_unix_ms() + delay.as_millis() as u64;
        Box::pin(std::future::poll_fn(move |_| {
            if cancellation.is_cancelled() {
                Poll::Ready(Err(Cancelled))
            } else if self.now_unix_ms() >= until {
                Poll::Ready(Ok(()))
            } else {
                Poll::Pending
            }
        }))
    }
}
impl StorageClock for Time {
    fn now(&self) -> Result<SafeUnsigned, StorageError> {
        Ok(n(self.now_unix_ms()))
    }
}
#[derive(Default)]
pub struct Ids(AtomicU64);
impl IdGenerator for Ids {
    fn next(&self, _: Option<SafeUnsigned>) -> String {
        format!("runtime-id-{}", self.0.fetch_add(1, Ordering::SeqCst))
    }
}
pub struct Host;
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
pub struct Auth;
impl ApiKeyAuth for Auth {
    fn name(&self) -> &str {
        "offline"
    }
    fn resolve<'a>(
        &'a self,
        _: ApiKeyAuthInput<'a>,
    ) -> pi_ai_core::BoxFuture<'a, Result<Option<AuthResult>, AuthProviderError>> {
        Box::pin(async {
            Ok(Some(AuthResult {
                auth: ModelAuth {
                    api_key: Some(SecretString::new("offline-fixture")),
                    headers: None,
                    base_url: None,
                },
                env: None,
                source: None,
            }))
        })
    }
}
pub fn model() -> Model {
    let value: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../conformance/schema/ai/models-options.golden.json"
    ))
    .unwrap();
    serde_json::from_value(value["model"].clone()).unwrap()
}
pub fn assistant(model: &Model, reason: StopReason) -> AssistantMessage {
    AssistantMessage {
        role: Default::default(),
        content: vec![],
        api: model.api.clone(),
        provider: model.provider.clone(),
        model: model.id.clone(),
        response_model: None,
        response_id: None,
        diagnostics: None,
        usage: Usage::zero(),
        stop_reason: reason,
        deferred: None,
        error_message: matches!(reason, StopReason::Error | StopReason::Aborted)
            .then(|| "ordinary fixture failure".into()),
        raw_stop_reason: None,
        end_turn: None,
        timestamp: n(7),
    }
}
pub struct Faux {
    pub model: Model,
    pub responses: Mutex<VecDeque<AssistantMessage>>,
    pub requests: Mutex<Vec<(Context, SimpleStreamOptionsWire)>>,
    pub headers: Mutex<Vec<ProviderHeaders>>,
    pub payloads: Arc<Mutex<Vec<serde_json::Value>>>,
    pub hold: Mutex<Option<CancellationToken>>,
}
impl Faux {
    pub fn new(model: Model) -> Self {
        Self {
            responses: Mutex::new(vec![assistant(&model, StopReason::Stop)].into()),
            model,
            requests: Mutex::new(vec![]),
            headers: Mutex::new(vec![]),
            payloads: Arc::new(Mutex::new(vec![])),
            hold: Mutex::new(None),
        }
    }
    fn start(
        &self,
        request: SimpleStreamRequest,
        lifecycle: RequestLifecycleInvocation,
    ) -> AssistantStream {
        self.requests
            .lock()
            .unwrap()
            .push((request.context.clone(), request.options.wire.clone()));
        self.headers
            .lock()
            .unwrap()
            .push(request.auth.headers.clone());
        let response = self
            .responses
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| assistant(&self.model, StopReason::Stop));
        let (stream, producer) = create_assistant_message_event_stream(AssistantStreamInit {
            api: self.model.api.clone(),
            provider: self.model.provider.clone(),
            model: self.model.id.clone(),
            clock: request.options.runtime.clock.clone(),
            cancellation: request.options.runtime.cancellation.clone(),
        });
        let model = self.model.clone();
        let payloads = self.payloads.clone();
        let hold = self.hold.lock().unwrap().clone();
        request
            .options
            .runtime
            .spawner
            .spawn(Box::pin(async move {
                let mut payload = serde_json::json!({"request":"actual faux input"});
                if let Some(transform) = lifecycle.payload {
                    match transform.transform(&model, &mut payload).await {
                        Ok(Some(value)) => payload = value,
                        Ok(None) => (),
                        Err(_) => {
                            let _ = producer.fail_setup(StreamSetupFailure::Adapter);
                            return;
                        }
                    }
                }
                payloads.lock().unwrap().push(payload);
                if let Some(hold) = hold {
                    hold.cancelled().await;
                }
                lifecycle.recorder.record(RequestResponseHead {
                    status: 200,
                    headers: vec![],
                });
                let event = match response.stop_reason {
                    StopReason::Error => AssistantMessageEvent::Error {
                        reason: ErrorReason::Error,
                        error: Box::new(response),
                    },
                    StopReason::Aborted => AssistantMessageEvent::Error {
                        reason: ErrorReason::Aborted,
                        error: Box::new(response),
                    },
                    reason => AssistantMessageEvent::Done {
                        reason: match reason {
                            StopReason::ToolUse => DoneReason::ToolUse,
                            StopReason::Length => DoneReason::Length,
                            StopReason::Deferred => DoneReason::Deferred,
                            _ => DoneReason::Stop,
                        },
                        message: Box::new(response),
                    },
                };
                producer.push(event).unwrap();
            }))
            .unwrap();
        stream
    }
}
impl RequestLifecycleAdapter for Faux {
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
impl ApiAdapter for Faux {
    fn api_id(&self) -> &ApiId {
        &self.model.api
    }
    fn lifecycle_adapter(&self) -> Option<&dyn RequestLifecycleAdapter> {
        Some(self)
    }
    fn stream(&self, request: TextStreamRequest) -> AssistantStream {
        self.stream_with_lifecycle(
            request,
            RequestLifecycleInvocation::new(Default::default()).0,
        )
    }
    fn stream_simple(&self, request: SimpleStreamRequest) -> AssistantStream {
        self.start(
            request,
            RequestLifecycleInvocation::new(Default::default()).0,
        )
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
pub struct Product {
    pub faux: Arc<Faux>,
    pub auth: ProviderAuth,
}
impl Provider for Product {
    fn id(&self) -> &ProviderId {
        &self.faux.model.provider
    }
    fn name(&self) -> &str {
        "offline-faux"
    }
    fn auth(&self) -> &ProviderAuth {
        &self.auth
    }
    fn models(&self) -> Result<Arc<[Model]>, ModelsError> {
        Ok(vec![self.faux.model.clone()].into())
    }
    fn adapter(&self, api: &ApiId) -> Option<Arc<dyn ApiAdapter>> {
        (api == &self.faux.model.api).then(|| self.faux.clone() as Arc<dyn ApiAdapter>)
    }
}
pub struct Summary;
impl pi_agent_session::context::SummaryRenderer for Summary {
    fn render(
        &self,
        summary: pi_agent_session::context::ContextSummary,
    ) -> pi_agent_schema::BoxFuture<'_, Result<AgentMessage, HarnessRuntimeError>> {
        Box::pin(async move {
            Ok(AgentMessage::BuiltIn(Message::User(UserMessage {
                role: Default::default(),
                content: UserContent::Text(summary.text),
                timestamp: summary.timestamp,
            })))
        })
    }
}
pub struct Setup {
    pub pump: Arc<Pump>,
    pub time: Arc<Time>,
    pub ids: Arc<Ids>,
    pub backing: Arc<MemoryBacking>,
    pub store: Arc<MemoryStorage>,
    pub session: Arc<RuntimeSession<MemoryStorage>>,
    pub registry: Arc<ModelsRegistry>,
    pub faux: Arc<Faux>,
}
impl Setup {
    pub fn new() -> Self {
        let pump = Arc::new(Pump::default());
        let time = Arc::new(Time(AtomicU64::new(7)));
        let ids = Arc::new(Ids::default());
        let model = model();
        let backing = Arc::new(MemoryBacking::new());
        let store = backing.open(time.clone()).unwrap();
        let session = Arc::new(
            pump.drive(RuntimeSession::new(
                store.clone(),
                SessionMetadata {
                    id: SessionId::new("runtime-session").unwrap(),
                    created_at: n(7),
                    storage_version: n(1),
                    store_generation: n(0),
                    cwd: None,
                    parent_session_id: None,
                    legacy_parent_session_path: None,
                },
                Arc::new(
                    SessionCodec::new(
                        SessionCodecOptions::default(),
                        ProviderHeaderPolicy::from_allowlist(["x-public".into()]),
                    )
                    .unwrap(),
                ),
                ids.clone(),
                Arc::new(NOOP_TELEMETRY_CONTEXT),
            ))
            .unwrap(),
        );
        pump.drive(session.commit(Transaction {
            writes: vec![
                Write::SetRegister(Box::new(RegisterSet::LaneConfig {
                    key: "main".into(),
                    value: LaneConfiguration {
                        model: LaneModel {
                            provider: model.provider.clone(),
                            model_id: model.id.clone(),
                        },
                        thinking_level: AgentThinkingLevel::Off,
                        active_tool_names: vec![],
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
        }))
        .unwrap();
        let registry = Arc::new(ModelsRegistry::new(ModelsRegistryOptions {
            credentials: Arc::new(InMemoryCredentialStore::new()),
            models_store: Box::new(InMemoryModelsStore::new()),
            auth_context: Arc::new(Host),
            clock: time.clone(),
            spawner: pump.clone(),
            model_header_policy: Default::default(),
        }));
        let faux = Arc::new(Faux::new(model));
        pump.drive(registry.set_provider(Arc::new(Product {
            faux: faux.clone(),
            auth: ProviderAuth::new(Some(Arc::new(Auth)), None).unwrap(),
        })))
        .unwrap();
        Self {
            pump,
            time,
            ids,
            backing,
            store,
            session,
            registry,
            faux,
        }
    }
    pub fn options<C>(
        &self,
        drive: DriveMode,
    ) -> AgentHarnessOptions<
        C,
        Arc<dyn pi_telemetry::TelemetryContext>,
        RuntimeSession<MemoryStorage>,
        ModelsRegistry,
    > {
        self.options_for(self.session.clone(), drive)
    }
    pub fn options_for<C, S: pi_agent_session::storage::RuntimeStorage + 'static>(
        &self,
        session: Arc<RuntimeSession<S>>,
        drive: DriveMode,
    ) -> AgentHarnessOptions<
        C,
        Arc<dyn pi_telemetry::TelemetryContext>,
        RuntimeSession<S>,
        ModelsRegistry,
    > {
        AgentHarnessOptions {
            session,
            models: self.registry.clone(),
            model: self.faux.model.clone(),
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
                base_delay_ms: n(10),
            },
            compaction: CompactionSettings {
                enabled: false,
                reserve_tokens: n(0),
                keep_recent_tokens: n(0),
            },
            steering_mode: QueueMode::All,
            follow_up_mode: QueueMode::All,
            tool_execution: ToolExecutionMode::Parallel,
            drive,
            to_provider_messages: None,
            entry_projectors: Default::default(),
            telemetry_context: Arc::new(NOOP_TELEMETRY_CONTEXT),
        }
    }
}
