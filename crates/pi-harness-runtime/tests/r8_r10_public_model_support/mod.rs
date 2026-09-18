//! Public structural fixture and small observable reference; no core recipes.
use crate::runtime_r1_r4_support::{
    Auth, Host, Ids, Pump, Summary, Time, assistant, model, n, poll,
};
use pi_agent_schema::*;
use pi_agent_session::{
    memory::MemoryStorage,
    repo::{MemorySession, MemorySessionRepo},
};
use pi_ai_core::*;
use pi_harness_runtime::owner::{RuntimeFactory, RuntimeHarness};
use pi_telemetry::NOOP_TELEMETRY_CONTEXT;
use std::{
    collections::{BTreeSet, VecDeque},
    future::Future,
    sync::{Arc, Mutex, atomic::AtomicU64},
    task::Poll,
};

pub type Harness = RuntimeHarness<(), MemoryStorage, SessionMetadata>;
pub fn user(text: &str) -> AgentMessage {
    serde_json::from_value(serde_json::json!({"role":"user","content":text,"timestamp":7})).unwrap()
}
pub fn reply(text: &str, usage: u64) -> AssistantMessage {
    let mut m = assistant(&model(), StopReason::Stop);
    m.content = vec![AssistantContent::Text(TextContent {
        content_type: Default::default(),
        text: text.into(),
        text_signature: None,
    })];
    m.usage.input = n(usage);
    m.usage.total_tokens = n(usage);
    m
}
pub fn prompt(text: &str) -> PromptInput {
    PromptInput::Text {
        text: text.into(),
        images: vec![],
    }
}
pub fn usage(value: u64) -> Usage {
    let mut u = Usage::zero();
    u.input = n(value);
    u.total_tokens = n(value);
    u
}
#[derive(Clone, Debug)]
pub struct Request {
    pub context: Context,
    pub deferred: Option<DeferredRequest>,
}
pub struct Adapter {
    pub model: Model,
    pub replies: Mutex<VecDeque<AssistantMessage>>,
    pub calls: Mutex<Vec<Request>>,
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
        r: SimpleStreamRequest,
        l: RequestLifecycleInvocation,
    ) -> AssistantStream {
        self.calls.lock().unwrap().push(Request {
            context: r.context,
            deferred: r.options.wire.deferred,
        });
        let message = self
            .replies
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected provider request");
        let rt = r.options.runtime;
        let (stream, producer) = create_assistant_message_event_stream(AssistantStreamInit {
            api: self.model.api.clone(),
            provider: self.model.provider.clone(),
            model: self.model.id.clone(),
            clock: rt.clock.clone(),
            cancellation: rt.cancellation.clone(),
        });
        let m = self.model.clone();
        rt.spawner
            .spawn(Box::pin(async move {
                let mut payload = serde_json::json!({"structural-model":true});
                if let Some(transform) = l.payload {
                    match transform.transform(&m, &mut payload).await {
                        Ok(Some(v)) => payload = v,
                        Ok(None) => {}
                        Err(_) => {
                            let _ = producer.fail_setup(StreamSetupFailure::Adapter);
                            return;
                        }
                    }
                }
                let _ = payload;
                l.recorder.record(RequestResponseHead {
                    status: 200,
                    headers: vec![],
                });
                let _ = producer.push(AssistantMessageEvent::Done {
                    reason: DoneReason::Stop,
                    message: Box::new(message),
                });
            }))
            .unwrap();
        stream
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
        panic!("captured lifecycle route required")
    }
    fn stream_simple(&self, _: SimpleStreamRequest) -> AssistantStream {
        panic!("captured lifecycle route required")
    }
    fn fetch_deferred(&self, _: DeferredFetchRequest) -> Result<AssistantStream, CapabilityError> {
        panic!("structural requests may not defer")
    }
    fn cancel_deferred<'a>(
        &'a self,
        _: DeferredCancelRequest,
    ) -> pi_ai_core::BoxFuture<'a, Result<(), ModelsError>> {
        panic!("structural requests may not defer")
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
        "structural-public-model"
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
pub struct Log(pub Mutex<Vec<HarnessEvent>>);
impl EventListener for Log {
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
pub struct View {
    pub entries: Vec<Entry>,
    pub registers: Vec<Register>,
    pub stats: SessionStats,
}
impl View {
    pub fn operation(&self) -> Option<serde_json::Value> {
        self.registers.iter().find_map(|r| match r {
            Register::OperationState { value, .. } => Some(serde_json::to_value(value).unwrap()),
            _ => None,
        })
    }
    pub fn preparation(&self) -> Option<serde_json::Value> {
        self.registers.iter().find_map(|r| match r {
            Register::OperationPreparation { value, .. } => {
                Some(serde_json::to_value(value).unwrap())
            }
            _ => None,
        })
    }
    pub fn terminal(&self, pending: &[EntryId]) {
        assert!(
            self.registers.iter().all(|r| !matches!(
                r,
                Register::OperationMeta { .. }
                    | Register::OperationState { .. }
                    | Register::OperationPreparation { .. }
                    | Register::OperationToolArguments { .. }
            )),
            "operation registers leaked"
        );
        let state = self
            .registers
            .iter()
            .find_map(|r| match r {
                Register::LaneState { key, value, .. } if key == "main" => Some(value),
                _ => None,
            })
            .unwrap();
        assert_eq!(state.current_operation_id, None);
        assert_eq!(state.pending_next_run, pending);
        let ids: BTreeSet<_> = self
            .registers
            .iter()
            .filter_map(|r| match r {
                Register::PendingEntry { key, .. } => Some(key.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(ids, pending.iter().map(|i| i.as_str()).collect());
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
    pub log: Arc<Log>,
    subscriptions: Vec<Arc<dyn EventSubscription>>,
    pub settings: CompactionSettings,
}
impl World {
    pub fn new() -> Self {
        let pump = Arc::new(Pump::default());
        let time = Arc::new(Time(AtomicU64::new(7)));
        let ids = Arc::new(Ids::default());
        let repo = MemorySessionRepo::new(
            SessionCodecOptions::default(),
            ProviderHeaderPolicy::default(),
            ids.clone(),
            time.clone(),
            Arc::new(NOOP_TELEMETRY_CONTEXT),
        )
        .unwrap();
        let session = pump
            .drive(repo.create(SessionCreateOptions {
                id: Some(SessionId::new("structural-model").unwrap()),
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
        let mut m = model();
        m.context_window = n(1000);
        m.max_tokens = n(100);
        let adapter = Arc::new(Adapter {
            model: m,
            replies: Mutex::new(VecDeque::new()),
            calls: Mutex::new(vec![]),
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
            log: Arc::new(Log::default()),
            subscriptions: vec![],
            settings: CompactionSettings {
                enabled: false,
                reserve_tokens: n(100),
                keep_recent_tokens: n(0),
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
            active_tool_names: None,
            tools: vec![],
            tool_context: None,
            system_prompt: None,
            resources: Default::default(),
            stream_options: Default::default(),
            retry: RetryPolicy {
                enabled: true,
                max_retries: MaxRetries::new(2).unwrap(),
                base_delay_ms: n(0),
            },
            compaction: self.settings.clone(),
            steering_mode: QueueMode::All,
            follow_up_mode: QueueMode::All,
            tool_execution: ToolExecutionMode::Parallel,
            drive: DriveMode::Manual,
            to_provider_messages: None,
            entry_projectors: Default::default(),
            telemetry_context: Arc::new(NOOP_TELEMETRY_CONTEXT)
                as Arc<dyn pi_telemetry::TelemetryContext>,
        };
        let made = if first {
            self.pump.drive(factory.create_first_attachment(options))
        } else {
            self.pump.drive(factory.create_configured(options))
        }
        .expect("public runtime attachment");
        for tag in [
            HarnessEventType::Usage,
            HarnessEventType::EntryAdded,
            HarnessEventType::CompactionStart,
            HarnessEventType::CompactionEnd,
            HarnessEventType::NavigationStart,
            HarnessEventType::NavigationEnd,
            HarnessEventType::RunStart,
            HarnessEventType::RunEnd,
            HarnessEventType::MessageStart,
            HarnessEventType::MessageUpdate,
            HarnessEventType::MessageEnd,
        ] {
            self.subscriptions
                .push(made.harness.events().on(tag, self.log.clone()));
        }
        made.harness
    }
    pub fn reopen(&mut self, h: Arc<Harness>) -> Arc<Harness> {
        let metadata = self.session.metadata();
        self.pump.drive(h.close()).unwrap();
        drop(h);
        self.session = self
            .pump
            .drive(self.repo.open(metadata))
            .expect("physical MemorySessionRepo reopen");
        self.attach(false)
    }
    pub fn view(&self) -> View {
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
        View {
            entries,
            registers,
            stats: self.pump.drive(self.session.get_stats()).unwrap(),
        }
    }
    pub fn drive<T>(&self, h: &Harness, future: impl Future<Output = T>) -> T {
        let mut future = std::pin::pin!(future);
        for _ in 0..5000 {
            if let Poll::Ready(v) = poll(future.as_mut()) {
                return v;
            }
            self.pump.tick();
            self.pump.drive(h.execute_action()).unwrap();
        }
        panic!("bounded public drive exhausted")
    }
    pub fn prefix<T: std::fmt::Debug>(
        &self,
        h: &Harness,
        future: std::pin::Pin<&mut impl Future<Output = T>>,
        mut stop: impl FnMut(&View) -> bool,
    ) {
        let mut future = future;
        for _ in 0..5000 {
            if let Poll::Ready(v) = poll(future.as_mut()) {
                panic!("public call ended before required durable prefix: {v:?}");
            }
            self.pump.tick();
            if stop(&self.view()) {
                return;
            }
            self.pump.drive(h.execute_action()).unwrap();
        }
        panic!("required durable prefix not reached")
    }
    pub fn seed(&self, h: &Harness, text: &str) -> EntryId {
        self.pump
            .drive(h.session().append_message(user(text)))
            .unwrap()
    }
    pub fn paid(&self) -> Vec<UsageRow> {
        self.log
            .0
            .lock()
            .unwrap()
            .iter()
            .filter_map(|e| match e {
                HarnessEvent::Usage(e) => Some(e.row.clone()),
                _ => None,
            })
            .collect()
    }
    pub fn assert_usage(&self, expected: &[u64]) {
        let rows = self.paid();
        assert_eq!(
            rows.iter().map(|r| r.usage.input.get()).collect::<Vec<_>>(),
            expected
        );
        let ids: BTreeSet<_> = rows.iter().map(|r| r.id.clone()).collect();
        assert_eq!(ids.len(), rows.len());
        assert_eq!(
            self.view().stats.usage.input.get(),
            expected.iter().sum::<u64>()
        );
    }
    pub fn assert_structural_only(&self) {
        for event in self.log.0.lock().unwrap().iter() {
            let value = serde_json::to_value(event).unwrap();
            let tag = value["type"].as_str().unwrap();
            assert!(
                !matches!(
                    tag,
                    "run_start" | "run_end" | "message_start" | "message_update" | "message_end"
                ),
                "structural lifecycle leak: {tag}"
            );
        }
    }
    pub fn assert_lifecycle(&self, kind: &str) {
        let events = self.log.0.lock().unwrap();
        let count = |tag: &str| {
            events
                .iter()
                .filter(|e| serde_json::to_value(e).unwrap()["type"] == tag)
                .count()
        };
        assert_eq!(count(&format!("{kind}_start")), 1);
        assert_eq!(count(&format!("{kind}_end")), 1);
    }
}

struct Handler<S, F>(F, std::marker::PhantomData<fn() -> S>);
impl<S: HookSpec, F: Fn(S::Event) -> Result<S::Result, HandlerError> + Send + Sync> HookHandler<S>
    for Handler<S, F>
{
    fn call(
        &self,
        event: S::Event,
    ) -> pi_agent_schema::BoxFuture<'_, Result<S::Result, HandlerError>> {
        let result = (self.0)(event);
        Box::pin(async move { result })
    }
}
pub fn handler<S: HookSpec>(
    f: impl Fn(S::Event) -> Result<S::Result, HandlerError> + Send + Sync + 'static,
) -> Arc<dyn HookHandler<S>> {
    Arc::new(Handler(f, std::marker::PhantomData))
}
/// Reference stores only visible effects: declined keeps leaf/count, append adds one,
/// navigation optionally appends one. No durable phase transition algorithm.
pub struct Reference {
    pub leaf: Option<EntryId>,
    pub entries: usize,
    pub pending: Vec<EntryId>,
}
impl Reference {
    pub fn check(&self, w: &World, h: &Harness, kind: &str, outcome: &str) {
        let view = w.view();
        assert_eq!(view.entries.len(), self.entries);
        assert_eq!(w.pump.drive(h.get_leaf_id()).unwrap(), self.leaf);
        view.terminal(&self.pending);
        let last = w
            .pump
            .drive(h.get_last_result())
            .unwrap()
            .expect("durable last result");
        let json = serde_json::to_value(last).unwrap();
        assert_eq!(json["kind"], kind);
        assert_eq!(json["outcome"], outcome);
        assert_eq!(json["leafId"], serde_json::to_value(&self.leaf).unwrap());
    }
}
pub fn compaction_hook(h: &Harness, mode: u8, seen: Arc<Mutex<Vec<CompactionPreparation>>>) {
    h.hooks()
        .before_compaction(
            None,
            handler::<BeforeCompaction>(move |event| {
                seen.lock().unwrap().push(event.preparation.clone());
                Ok(match mode {
                    0 => Some(BeforeCompactionResult::DeclineTrue(Default::default())),
                    1 => Some(BeforeCompactionResult::Compaction(Box::new(
                        BeforeCompactionSupply {
                            decline: None,
                            compaction: CompactResult {
                                summary: "hook-summary".into(),
                                tokens_before: event.preparation.tokens_before,
                                retained_tail: event.preparation.retained_tail,
                                usage: Some(usage(5)),
                                details: Presence::Missing,
                            },
                        },
                    ))),
                    _ => None,
                })
            }),
        )
        .unwrap();
}
pub fn navigation_hook(h: &Harness, mode: u8, seen: Arc<Mutex<Vec<BranchPreparation>>>) {
    h.hooks()
        .before_navigation(
            None,
            handler::<BeforeNavigation>(move |event| {
                seen.lock().unwrap().push(event.preparation);
                Ok(match mode {
                    0 => Some(BeforeNavigationResult::DeclineTrue(Default::default())),
                    1 => Some(BeforeNavigationResult::Summary(BeforeNavigationSupply {
                        decline: None,
                        summary: BranchSummaryResult {
                            summary: "hook-branch".into(),
                            usage: Some(usage(7)),
                            read_files: vec![],
                            modified_files: vec![],
                        },
                    })),
                    _ => None,
                })
            }),
        )
        .unwrap();
}
pub fn assert_compaction_row(w: &World, e: &CompactionEntry) {
    assert_eq!(
        w.pump.drive(w.session.get_entry(e.id.clone())).unwrap(),
        Some(Entry::Compaction(e.clone()))
    );
    assert!(
        w.log
            .0
            .lock()
            .unwrap()
            .iter()
            .any(|v| matches!(v,HarnessEvent::CompactionEndCompleted(x) if x.entry==*e))
    );
    assert!(
        w.log.0.lock().unwrap().iter().any(
            |v| matches!(v,HarnessEvent::EntryAdded(x) if x.entry==Entry::Compaction(e.clone()))
        )
    );
}
pub fn assert_navigation_row(w: &World, e: &BranchSummaryEntry) {
    assert_eq!(
        w.pump.drive(w.session.get_entry(e.id.clone())).unwrap(),
        Some(Entry::BranchSummary(e.clone()))
    );
    assert!(w.log.0.lock().unwrap().iter().any(
        |v| matches!(v,HarnessEvent::NavigationEndCompleted(x) if x.summary_entry.as_ref()==Some(e))
    ));
}

/// Foreign-only UUIDv7 port: the raw Session facade validates canonical IDs.
#[derive(Default)]
struct ForeignIds(AtomicU64);
impl IdGenerator for ForeignIds {
    fn next(&self, timestamp: Option<SafeUnsigned>) -> String {
        let timestamp = timestamp.map_or(7, SafeUnsigned::get);
        let counter = self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
        format!(
            "{:08x}-{:04x}-7000-8000-{:012x}",
            timestamp >> 16,
            timestamp & 0xffff,
            counter
        )
    }
}
pub fn foreign_entry(w: &World) -> (MemorySessionRepo, Arc<MemorySession>, EntryId) {
    let repo = MemorySessionRepo::new(
        SessionCodecOptions::default(),
        ProviderHeaderPolicy::default(),
        Arc::new(ForeignIds::default()),
        w.time.clone(),
        Arc::new(NOOP_TELEMETRY_CONTEXT),
    )
    .unwrap();
    let session = w
        .pump
        .drive(repo.create(SessionCreateOptions {
            id: Some(SessionId::new("foreign").unwrap()),
            ..Default::default()
        }))
        .unwrap();
    let id = w
        .pump
        .drive(session.append_message(user("foreign only")))
        .unwrap();
    (repo, session, id)
}
