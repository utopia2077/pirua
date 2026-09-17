#![allow(dead_code)]
//! Actual public R6 host adapted from frozen model689 test-only fixtures.
use crate::runtime_r1_r4_support::{
    Auth, Host, Ids, Pump, Summary, Time, assistant, model, n, poll,
};
use crate::runtime_telemetry_support::Recorder;
use pi_agent_schema::*;
use pi_agent_session::{
    memory::MemoryStorage,
    repo::{MemorySession, MemorySessionRepo},
};
use pi_ai_core::*;
use pi_harness_runtime::owner::{RuntimeFactory, RuntimeHarness};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    future::Future,
    sync::{Arc, Mutex, atomic::AtomicU64},
    task::Poll,
};

pub type Harness = RuntimeHarness<(), MemoryStorage, SessionMetadata>;
pub fn user(text: &str) -> AgentMessage {
    serde_json::from_value(serde_json::json!({"role":"user","content":text,"timestamp":7})).unwrap()
}
pub fn prompt(text: &str) -> PromptInput {
    PromptInput::Text {
        text: text.into(),
        images: vec![],
    }
}
pub fn handle() -> DeferredHandle {
    let m = model();
    DeferredHandle {
        provider: m.provider,
        model_id: m.id,
        api: m.api,
        id: DeferredId::new("public-model-handle").unwrap(),
        expires_at: Some(n(99999)),
        poll_after_ms: Some(n(50)),
        data: Presence::Value(JsonValue::String("complete-metadata".into())),
    }
}
pub fn pending_message() -> AssistantMessage {
    let mut m = assistant(&model(), StopReason::Deferred);
    m.deferred = Some(handle());
    m.usage.input = n(3);
    m.usage.total_tokens = n(3);
    m
}
#[derive(Clone)]
pub struct Reply {
    pub message: AssistantMessage,
    pub hold: Option<CancellationToken>,
}
impl Reply {
    pub fn stop() -> Self {
        Self {
            message: assistant(&model(), StopReason::Stop),
            hold: None,
        }
    }
    pub fn pending() -> Self {
        Self {
            message: pending_message(),
            hold: None,
        }
    }
}
#[derive(Default)]
pub struct Calls {
    pub streams: Vec<Context>,
    pub fetches: Vec<DeferredHandle>,
    pub cancels: Vec<DeferredHandle>,
    pub wait: Vec<Option<SafeUnsigned>>,
    pub cancel_was_signalled: Vec<bool>,
}
pub struct Adapter {
    pub model: Model,
    pub initial: Mutex<VecDeque<Reply>>,
    pub polls: Mutex<VecDeque<Reply>>,
    pub calls: Mutex<Calls>,
}
impl Adapter {
    fn start(
        &self,
        runtime: RequestRuntime,
        reply: Reply,
        lifecycle: RequestLifecycleInvocation,
    ) -> AssistantStream {
        let (stream, producer) = create_assistant_message_event_stream(AssistantStreamInit {
            api: self.model.api.clone(),
            provider: self.model.provider.clone(),
            model: self.model.id.clone(),
            clock: runtime.clock.clone(),
            cancellation: runtime.cancellation.clone(),
        });
        let m = self.model.clone();
        runtime
            .spawner
            .spawn(Box::pin(async move {
                let mut payload = serde_json::json!({"deferred-model":true});
                if let Some(p) = lifecycle.payload {
                    match p.transform(&m, &mut payload).await {
                        Ok(Some(v)) => payload = v,
                        Ok(None) => {}
                        Err(_) => {
                            let _ = producer.fail_setup(StreamSetupFailure::Adapter);
                            return;
                        }
                    }
                }
                let _ = payload;
                if let Some(gate) = reply.hold {
                    let mut partial = reply.message.clone();
                    partial.stop_reason = StopReason::Pending;
                    partial.error_message = None;
                    let _ = producer.publish_cancellation_snapshot(partial);
                    std::future::poll_fn(|_| {
                        if gate.is_cancelled() || runtime.cancellation.is_cancelled() {
                            Poll::Ready(())
                        } else {
                            Poll::Pending
                        }
                    })
                    .await;
                }
                if runtime.cancellation.is_cancelled() {
                    return;
                }
                lifecycle.recorder.record(RequestResponseHead {
                    status: 200,
                    headers: vec![],
                });
                let event = match reply.message.stop_reason {
                    StopReason::Error => AssistantMessageEvent::Error {
                        reason: ErrorReason::Error,
                        error: Box::new(reply.message),
                    },
                    StopReason::Aborted => AssistantMessageEvent::Error {
                        reason: ErrorReason::Aborted,
                        error: Box::new(reply.message),
                    },
                    reason => AssistantMessageEvent::Done {
                        reason: match reason {
                            StopReason::Deferred => DoneReason::Deferred,
                            StopReason::ToolUse => DoneReason::ToolUse,
                            StopReason::Length => DoneReason::Length,
                            _ => DoneReason::Stop,
                        },
                        message: Box::new(reply.message),
                    },
                };
                let _ = producer.push(event);
            }))
            .unwrap();
        stream
    }
}
impl RequestLifecycleAdapter for Adapter {
    fn deferred_lifecycle(&self) -> Option<&dyn DeferredRequestLifecycleAdapter> {
        Some(self)
    }
    fn stream_with_lifecycle(
        &self,
        _: TextStreamRequest,
        _: RequestLifecycleInvocation,
    ) -> AssistantStream {
        panic!("Harness must use simple request")
    }
    fn stream_simple_with_lifecycle(
        &self,
        r: SimpleStreamRequest,
        l: RequestLifecycleInvocation,
    ) -> AssistantStream {
        self.calls.lock().unwrap().streams.push(r.context);
        let reply = self
            .initial
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(Reply::stop);
        self.start(r.options.runtime, reply, l)
    }
}
impl DeferredRequestLifecycleAdapter for Adapter {
    fn fetch_deferred_with_lifecycle(
        &self,
        r: DeferredFetchRequest,
        l: RequestLifecycleInvocation,
    ) -> Result<AssistantStream, CapabilityError> {
        let mut c = self.calls.lock().unwrap();
        c.fetches.push(r.handle);
        c.wait.push(r.options.wire.wait);
        drop(c);
        let reply = self
            .polls
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected extra provider poll");
        Ok(self.start(r.options.runtime, reply, l))
    }
}
impl ApiAdapter for Adapter {
    fn api_id(&self) -> &ApiId {
        &self.model.api
    }
    fn lifecycle_adapter(&self) -> Option<&dyn RequestLifecycleAdapter> {
        Some(self)
    }
    fn stream(&self, _: TextStreamRequest) -> AssistantStream {
        panic!("unexpected ordinary stream")
    }
    fn stream_simple(&self, _: SimpleStreamRequest) -> AssistantStream {
        panic!("lifecycle bridge required")
    }
    fn fetch_deferred(&self, _: DeferredFetchRequest) -> Result<AssistantStream, CapabilityError> {
        panic!("lifecycle bridge required for poll")
    }
    fn cancel_deferred<'a>(
        &'a self,
        r: DeferredCancelRequest,
    ) -> pi_ai_core::BoxFuture<'a, Result<(), ModelsError>> {
        let mut c = self.calls.lock().unwrap();
        c.cancels.push(r.handle);
        c.cancel_was_signalled
            .push(r.options.runtime.cancellation.is_cancelled());
        Box::pin(async { Ok(()) })
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
        "public-model"
    }
    fn auth(&self) -> &ProviderAuth {
        &self.auth
    }
    fn models(&self) -> Result<Arc<[Model]>, ModelsError> {
        Ok(vec![self.adapter.model.clone()].into())
    }
    fn adapter(&self, api: &ApiId) -> Option<Arc<dyn ApiAdapter>> {
        (api == &self.adapter.model.api).then(|| self.adapter.clone() as Arc<dyn ApiAdapter>)
    }
}
#[derive(Default)]
struct EventLog(Mutex<Vec<HarnessEvent>>);
impl EventListener for EventLog {
    fn on_event(
        &self,
        event: HarnessEvent,
    ) -> pi_agent_schema::BoxFuture<'_, Result<(), HandlerError>> {
        Box::pin(async move {
            self.0.lock().unwrap().push(event);
            Ok(())
        })
    }
}
#[derive(Clone, Debug, PartialEq)]
pub struct Observed {
    pub entries: Vec<Entry>,
    pub registers: Vec<Register>,
    pub usage: Vec<UsageRow>,
    pub stats: SessionStats,
}
impl Observed {
    pub fn entry_ids(&self) -> BTreeSet<EntryId> {
        self.entries
            .iter()
            .map(|e| match e {
                Entry::Message(e) => e.id.clone(),
                Entry::Custom(e) => e.id.clone(),
                Entry::Compaction(e) => e.id.clone(),
                Entry::BranchSummary(e) => e.id.clone(),
            })
            .collect()
    }
    pub fn pending_ids(&self) -> BTreeSet<EntryId> {
        self.registers
            .iter()
            .filter_map(|r| match r {
                Register::PendingEntry { key, .. } => Some(EntryId::new(key.clone()).unwrap()),
                _ => None,
            })
            .collect()
    }
    pub fn run(&self) -> Option<&RunState> {
        self.registers.iter().find_map(|r| match r {
            Register::OperationState { value, .. } => match value.as_ref() {
                OperationState::Run(r) => Some(r.as_ref()),
                _ => None,
            },
            _ => None,
        })
    }
    pub fn deferred(&self) -> Option<&Deferred> {
        self.run().and_then(|r| match &r.phase {
            RunPhase::Deferred(p) => Some(&p.deferred),
            _ => None,
        })
    }
    pub fn exclusive(&self) {
        let placed = self.entry_ids();
        let pending = self.pending_ids();
        assert!(placed.is_disjoint(&pending), "register/entry coexistence");
        let mut u = BTreeSet::new();
        for row in &self.usage {
            assert!(u.insert(row.id.clone()), "duplicate Usage event/row");
        }
    }
}
pub struct World {
    pub pump: Arc<Pump>,
    pub time: Arc<Time>,
    pub ids: Arc<Ids>,
    pub repo: MemorySessionRepo,
    pub session: Arc<MemorySession>,
    pub adapter: Arc<Adapter>,
    pub models: Arc<ModelsRegistry>,
    pub recorder: Recorder,
    events: Arc<EventLog>,
    subscriptions: Vec<Arc<dyn EventSubscription>>,
    pub mode: DriveMode,
    pub queue: QueueMode,
    pub tools: Vec<AgentHarnessToolHandle<()>>,
    pub execution: ToolExecutionMode,
    pub retry: RetryPolicy,
}
impl World {
    pub fn new(mode: DriveMode, queue: QueueMode) -> Self {
        let pump = Arc::new(Pump::default());
        let time = Arc::new(Time(AtomicU64::new(7)));
        let ids = Arc::new(Ids::default());
        let recorder = Recorder::default();
        let repo = MemorySessionRepo::new(
            SessionCodecOptions::default(),
            ProviderHeaderPolicy::default(),
            ids.clone(),
            time.clone(),
            Arc::new(recorder.clone()),
        )
        .unwrap();
        let session = pump
            .drive(repo.create(SessionCreateOptions {
                id: Some(SessionId::new("public-model").unwrap()),
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
            initial: Mutex::new(VecDeque::new()),
            polls: Mutex::new(VecDeque::new()),
            calls: Mutex::new(Calls::default()),
        });
        pump.drive(models.set_provider(Arc::new(Product {
            adapter: adapter.clone(),
            auth: ProviderAuth::new(Some(Arc::new(Auth)), None).unwrap(),
        })))
        .unwrap();
        Self {
            pump,
            time,
            ids,
            repo,
            session,
            adapter,
            models,
            recorder,
            events: Arc::new(EventLog::default()),
            subscriptions: vec![],
            mode,
            queue,
            tools: vec![],
            execution: ToolExecutionMode::Parallel,
            retry: RetryPolicy {
                enabled: false,
                max_retries: MaxRetries::new(0).unwrap(),
                base_delay_ms: n(1000),
            },
        }
    }
    pub fn attach(&mut self, first: bool) -> Arc<Harness> {
        let factory = RuntimeFactory::new(
            self.time.clone(),
            self.pump.clone(),
            self.ids.clone(),
            Arc::new(()),
            Arc::new(Summary),
        );
        let options = AgentHarnessOptions {
            session: self.session.clone(),
            models: self.models.clone(),
            model: self.adapter.model.clone(),
            thinking_level: None,
            active_tool_names: Some(
                self.tools
                    .iter()
                    .map(|t| t.descriptor().name.clone())
                    .collect(),
            ),
            tools: self.tools.clone(),
            tool_context: Some(AgentHarnessToolContextSource::Static(())),
            system_prompt: None,
            resources: Default::default(),
            stream_options: Default::default(),
            retry: self.retry.clone(),
            compaction: CompactionSettings {
                enabled: false,
                reserve_tokens: n(0),
                keep_recent_tokens: n(0),
            },
            steering_mode: self.queue,
            follow_up_mode: self.queue,
            tool_execution: self.execution,
            drive: self.mode,
            to_provider_messages: None,
            entry_projectors: Default::default(),
            telemetry_context: Arc::new(self.recorder.clone())
                as Arc<dyn pi_telemetry::TelemetryContext>,
        };
        let created = if first {
            self.pump.drive(factory.create_first_attachment(options))
        } else {
            self.pump.drive(factory.create_configured(options))
        }
        .expect("public factory");
        for tag in [
            HarnessEventType::Usage,
            HarnessEventType::RunResume,
            HarnessEventType::RunSuspend,
            HarnessEventType::EntryAdded,
        ] {
            self.subscriptions
                .push(created.harness.events().on(tag, self.events.clone()));
        }
        created.harness
    }
    pub fn reopen(&mut self, h: Arc<Harness>) -> Arc<Harness> {
        let metadata = self.session.metadata();
        self.pump.drive(h.close()).unwrap();
        drop(h);
        self.session = self
            .pump
            .drive(self.repo.open(metadata))
            .expect("actual MemoryRepo reopen");
        self.attach(false)
    }
    pub fn observed(&self) -> Observed {
        let entries = self
            .pump
            .drive(self.session.find_entries(EntryQuery {
                order: Some(ScanOrder::Ascending),
                ..Default::default()
            }))
            .unwrap();
        let mut registers = vec![];
        for ns in [
            RegisterNamespace::LaneLeaf,
            RegisterNamespace::LaneConfig,
            RegisterNamespace::LaneState,
            RegisterNamespace::LaneLastResult,
            RegisterNamespace::OperationMeta,
            RegisterNamespace::OperationState,
            RegisterNamespace::OperationToolArguments,
            RegisterNamespace::OperationPreparation,
            RegisterNamespace::PendingEntry,
            RegisterNamespace::FactName,
            RegisterNamespace::FactLabel,
            RegisterNamespace::FactCustom,
        ] {
            registers.extend(
                self.pump
                    .drive(self.session.list_registers(ns, None))
                    .unwrap(),
            );
        }
        let usage = self
            .events
            .0
            .lock()
            .unwrap()
            .iter()
            .filter_map(|e| match e {
                HarnessEvent::Usage(u) => Some(u.row.clone()),
                _ => None,
            })
            .collect();
        let stats = self.pump.drive(self.session.get_stats()).unwrap();
        let view = Observed {
            entries,
            registers,
            usage,
            stats,
        };
        view.exclusive();
        view
    }
    pub fn drive<T>(&self, h: &Harness, future: impl Future<Output = T>) -> T {
        let mut future = std::pin::pin!(future);
        for _ in 0..4000 {
            if let Poll::Ready(v) = poll(future.as_mut()) {
                self.observed();
                return v;
            }
            self.pump.tick();
            self.observed();
            if self.mode == DriveMode::Manual {
                self.pump.drive(h.execute_action()).unwrap();
            }
        }
        panic!("public model did not terminate")
    }
    pub fn until(&self, h: &Harness, mut pred: impl FnMut(&Observed) -> bool) {
        for _ in 0..1000 {
            self.pump.tick();
            let view = self.observed();
            if pred(&view) {
                return;
            }
            if self.mode == DriveMode::Manual {
                self.pump.drive(h.execute_action()).unwrap();
            }
        }
        panic!("public prefix boundary not reached")
    }
    pub fn finish_idle(&self, h: &Harness) {
        self.drive(h, h.wait_for_idle()).unwrap();
    }
    pub fn write_observations(&self) -> usize {
        self.recorder
            .rows()
            .iter()
            .filter(|row| row.name == "pi.session.write")
            .count()
    }
}
#[derive(Default)]
pub struct QueueReference {
    pub pending: BTreeMap<EntryId, String>,
    pub placed: Vec<EntryId>,
}
impl QueueReference {
    pub fn admit(&mut self, id: EntryId, text: &str) {
        assert!(self.pending.insert(id, text.into()).is_none());
    }
    pub fn cancel(&mut self, id: &EntryId) {
        assert!(self.pending.remove(id).is_some());
    }
    pub fn consume(&mut self, ids: &[EntryId]) {
        for id in ids {
            assert!(self.pending.remove(id).is_some());
            self.placed.push(id.clone());
        }
    }
    pub fn check(&self, view: &Observed) {
        let placed = view.entry_ids();
        let pending = view.pending_ids();
        for id in self.pending.keys() {
            assert!(pending.contains(id));
            assert!(!placed.contains(id));
        }
        for id in &self.placed {
            assert!(placed.contains(id));
            assert!(!pending.contains(id));
        }
        let order: Vec<_> = view
            .entries
            .iter()
            .filter_map(|e| {
                let id = match e {
                    Entry::Message(e) => &e.id,
                    _ => return None,
                };
                self.placed.contains(id).then_some(id.clone())
            })
            .collect();
        assert_eq!(order, self.placed);
    }
}

pub struct HookGate {
    pub entered: std::sync::atomic::AtomicUsize,
    pub release: CancellationToken,
    pub patch: bool,
}
impl Default for HookGate {
    fn default() -> Self {
        Self {
            entered: std::sync::atomic::AtomicUsize::new(0),
            release: CancellationToken::new(),
            patch: false,
        }
    }
}
impl HookHandler<BeforeRequest> for HookGate {
    fn call(
        &self,
        _: BeforeRequestEvent,
    ) -> pi_agent_schema::BoxFuture<'_, Result<Option<BeforeRequestResult>, HandlerError>> {
        self.entered
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Box::pin(async move {
            self.release.cancelled().await;
            Ok(None)
        })
    }
}
impl HookHandler<AfterResponse> for HookGate {
    fn call(
        &self,
        event: AfterResponseEvent,
    ) -> pi_agent_schema::BoxFuture<'_, Result<Option<AfterResponseResult>, HandlerError>> {
        self.entered
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Box::pin(async move {
            self.release.cancelled().await;
            if self.patch {
                let mut m = event.message.into_message();
                m.response_id = Some(ResponseId::new("hook-completed").unwrap());
                Ok(Some(AfterResponseResult {
                    message: Some(SettledAssistantMessage::new(m).unwrap()),
                }))
            } else {
                Ok(None)
            }
        })
    }
}
impl HookHandler<AfterTool> for HookGate {
    fn call(
        &self,
        _: AfterToolEvent,
    ) -> pi_agent_schema::BoxFuture<'_, Result<Option<AfterToolResult>, HandlerError>> {
        self.entered
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Box::pin(async move {
            self.release.cancelled().await;
            Ok(self.patch.then(|| AfterToolResult {
                details: Presence::Value(JsonValue::String("hook-completed".into())),
                ..Default::default()
            }))
        })
    }
}
pub struct Tool {
    descriptor: AgentTool,
    pub gate: CancellationToken,
    pub calls: std::sync::atomic::AtomicUsize,
    pub cancellations: std::sync::atomic::AtomicUsize,
}
impl Tool {
    pub fn new(name: &str, replay: ToolReplay) -> Arc<Self> {
        Arc::new(Self{descriptor:AgentTool{name:ToolName::new(name).unwrap(),label:name.into(),description:name.into(),parameters:serde_json::from_value(serde_json::json!({"type":"object","properties":{},"additionalProperties":false})).unwrap(),constrained_sampling:None,execution_mode:Some(ToolExecutionMode::Parallel),replay:Some(replay)},gate:CancellationToken::new(),calls:std::sync::atomic::AtomicUsize::new(0),cancellations:std::sync::atomic::AtomicUsize::new(0)})
    }
}
impl AgentHarnessTool<()> for Tool {
    fn descriptor(&self) -> &AgentTool {
        &self.descriptor
    }
    fn prepare_arguments(&self, args: JsonValue) -> Result<JsonObject, ToolExecutionError> {
        let JsonValue::Object(args) = args else {
            panic!("fixture object arguments")
        };
        Ok(args)
    }
    fn execute<'a>(
        &'a self,
        _: ToolCallId,
        _: JsonObject,
        cancellation: &'a dyn Cancellation,
        _: &'a dyn ToolProgressSink,
        _: &'a (),
    ) -> pi_agent_schema::BoxFuture<'a, Result<AgentToolResult, ToolExecutionError>> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Box::pin(async move {
            self.gate.cancelled().await;
            if cancellation.is_cancelled() {
                self.cancellations
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            let mut usage = Usage::zero();
            usage.input = n(5);
            usage.total_tokens = n(5);
            Ok(AgentToolResult {
                content: vec![InputContent::Text(TextContent {
                    content_type: Default::default(),
                    text: format!("raw:{}", self.descriptor.name.as_str()),
                    text_signature: None,
                })],
                details: JsonValue::String(format!("raw:{}", self.descriptor.name.as_str())),
                usage: Some(usage),
                added_tool_names: None,
                terminate: Some(true),
            })
        })
    }
}
pub fn tool_reply(names: &[&str]) -> Reply {
    let mut reply = Reply::stop();
    reply.message.stop_reason = StopReason::ToolUse;
    reply.message.content = names
        .iter()
        .map(|name| {
            AssistantContent::ToolCall(ToolCall {
                content_type: Default::default(),
                id: ToolCallId::new(format!("call-{name}")).unwrap(),
                name: ToolName::new(*name).unwrap(),
                arguments: JsonObject::default(),
                thought_signature: None,
                namespace: None,
            })
        })
        .collect();
    reply
}
pub fn tool_rows(view: &Observed) -> Vec<(&MessageEntry, &ToolResultMessage)> {
    view.entries
        .iter()
        .filter_map(|e| match e {
            Entry::Message(e) => match &e.message {
                AgentMessage::BuiltIn(Message::ToolResult(r)) => Some((e, r.as_ref())),
                _ => None,
            },
            _ => None,
        })
        .collect()
}
pub fn run_call(
    h: Arc<Harness>,
) -> pi_agent_schema::BoxFuture<'static, Result<(), HarnessRuntimeError>> {
    Box::pin(async move {
        h.prompt(prompt("control-matrix")).await.map(|r| {
            r.expect("public prefix accepted");
        })
    })
}
pub fn resume_call(
    h: Arc<Harness>,
) -> pi_agent_schema::BoxFuture<'static, Result<(), HarnessRuntimeError>> {
    Box::pin(async move {
        h.resume().await.map(|r| {
            r.expect("public resume accepted");
        })
    })
}
