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
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
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
    pub responses: Arc<AtomicUsize>,
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
        let responses = self.responses.clone();
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
                responses.fetch_add(1, Ordering::SeqCst);
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
    unavailable: Arc<AtomicBool>,
    lookups: Arc<AtomicUsize>,
    withdrawal: Arc<Withdrawal>,
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
        let session = self.withdrawal.session.lock().unwrap().clone();
        if self.withdrawal.enabled.load(Ordering::SeqCst)
            && immediate(session.get_stats()).unwrap().usage.input.get() >= 11
        {
            self.withdrawal.hits.fetch_add(1, Ordering::SeqCst);
            let mut snapshot = self.withdrawal.observed.lock().unwrap();
            if snapshot.is_none() {
                *snapshot = Some(snapshot_session(&session));
            }
            return Ok(vec![].into());
        }
        Ok(vec![self.adapter.model.clone()].into())
    }
    fn adapter(&self, api: &ApiId) -> Option<Arc<dyn ApiAdapter>> {
        self.lookups.fetch_add(1, Ordering::SeqCst);
        (api == &self.adapter.model.api && !self.unavailable.load(Ordering::SeqCst))
            .then(|| self.adapter.clone() as Arc<dyn ApiAdapter>)
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
    pub retries: u64,
    pub withdrawal: Arc<Withdrawal>,
    pub unavailable: Arc<AtomicBool>,
    pub lookups: Arc<AtomicUsize>,
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
        let unavailable = Arc::new(AtomicBool::new(false));
        let lookups = Arc::new(AtomicUsize::new(0));
        let withdrawal = Arc::new(Withdrawal {
            enabled: AtomicBool::new(false),
            session: Mutex::new(session.clone()),
            observed: Mutex::new(None),
            hits: AtomicUsize::new(0),
        });
        let adapter = Arc::new(Adapter {
            model: m,
            replies: Mutex::new(VecDeque::new()),
            calls: Mutex::new(vec![]),
            responses: Arc::new(AtomicUsize::new(0)),
        });
        pump.drive(models.set_provider(Arc::new(Product {
            unavailable: unavailable.clone(),
            lookups: lookups.clone(),
            withdrawal: withdrawal.clone(),
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
            retries: 1,
            unavailable,
            lookups,
            withdrawal,
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
                max_retries: MaxRetries::new(self.retries).unwrap(),
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
        *self.withdrawal.session.lock().unwrap() = self.session.clone();
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
    pub fn prefix<T: std::fmt::Debug, F: Future<Output = T> + ?Sized>(
        &self,
        h: &Harness,
        future: std::pin::Pin<&mut F>,
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

pub struct Withdrawal {
    pub enabled: AtomicBool,
    pub session: Mutex<Arc<MemorySession>>,
    pub observed: Mutex<Option<View>>,
    pub hits: AtomicUsize,
}
fn immediate<T>(f: impl Future<Output = T>) -> T {
    let mut f = std::pin::pin!(f);
    match poll(f.as_mut()) {
        Poll::Ready(v) => v,
        Poll::Pending => panic!("actual Memory read unexpectedly yielded"),
    }
}
fn snapshot_session(s: &MemorySession) -> View {
    let entries = immediate(s.find_entries(EntryQuery {
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
        registers.extend(immediate(s.list_registers(ns, None)).unwrap());
    }
    View {
        entries,
        registers,
        stats: immediate(s.get_stats()).unwrap(),
    }
}
pub fn generation(s: &serde_json::Value) -> &serde_json::Value {
    match s["kind"].as_str() {
        Some("run") => &s["phase"]["structural"]["generation"],
        Some("navigation") => &s["phase"]["structural"]["generation"],
        _ => &s["structural"]["generation"],
    }
}
pub fn gate(w: &World, h: &Harness, kind: &str) -> bool {
    w.pump
        .drive(h.peek_action())
        .unwrap()
        .is_some_and(|a| a.kind == kind)
}
pub fn install_compaction_hook(h: &Harness, supply: bool, calls: Arc<AtomicUsize>) {
    h.hooks()
        .before_compaction(
            None,
            handler::<BeforeCompaction>(move |event| {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(supply.then(|| {
                    BeforeCompactionResult::Compaction(Box::new(BeforeCompactionSupply {
                        decline: None,
                        compaction: CompactResult {
                            summary: "hook output".into(),
                            tokens_before: event.preparation.tokens_before,
                            retained_tail: event.preparation.retained_tail,
                            usage: Some(usage(5)),
                            details: Presence::Missing,
                        },
                    }))
                }))
            }),
        )
        .unwrap();
}
pub fn install_navigation_hook(h: &Harness, calls: Arc<AtomicUsize>) {
    h.hooks()
        .before_navigation(
            None,
            handler::<BeforeNavigation>(move |_| {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(Some(BeforeNavigationResult::Summary(
                    BeforeNavigationSupply {
                        decline: None,
                        summary: BranchSummaryResult {
                            summary: "hook branch".into(),
                            usage: Some(usage(7)),
                            read_files: vec![],
                            modified_files: vec![],
                        },
                    },
                )))
            }),
        )
        .unwrap();
}
pub fn start(
    h: Arc<Harness>,
    target: Option<EntryId>,
) -> pi_agent_schema::BoxFuture<'static, Result<serde_json::Value, HarnessRuntimeError>> {
    Box::pin(async move {
        match target {
            Some(id) => Ok(serde_json::to_value(
                h.navigate_tree(
                    Some(id),
                    NavigateOptions {
                        summarize: Some(true),
                        ..Default::default()
                    },
                )
                .await?
                .expect("accepted navigation"),
            )
            .unwrap()),
            None => Ok(serde_json::to_value(
                h.compact(CompactOptions::default())
                    .await?
                    .expect("accepted compaction"),
            )
            .unwrap()),
        }
    })
}
pub fn seed_split(w: &World, h: &Harness, with_tail: bool) {
    w.seed(h, &"old-user ".repeat(40));
    w.pump
        .drive(
            h.session()
                .append_message(AgentMessage::BuiltIn(Message::Assistant(Box::new(reply(
                    &"old-answer ".repeat(40),
                    0,
                ))))),
        )
        .unwrap();
    w.seed(h, &"new-turn ".repeat(40));
    if with_tail {
        w.pump.drive(h.session().append_message(tail())).unwrap();
    }
}
pub fn tail() -> AgentMessage {
    AgentMessage::BuiltIn(Message::Assistant(Box::new(reply(&"tail ".repeat(20), 0))))
}
