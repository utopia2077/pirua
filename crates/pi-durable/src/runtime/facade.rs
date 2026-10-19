use super::*;
use crate::{session::*, *};
use pi_ai::{BoxFuture, CancellationToken, ModelsRegistry, RequestRuntime};
use serde_json::{Value, json};
use std::sync::{Arc, OnceLock, Weak};
use std::time::Duration;
pub type ModelsProvider = Arc<dyn Fn() -> Arc<ModelsRegistry> + Send + Sync>;
pub type SettingsProvider = Arc<dyn Fn() -> DurableSettings + Send + Sync>;
#[derive(Clone)]
pub struct EnvTarget {
    pub conversation_id: ConversationId,
    pub cwd: Option<String>,
    pub session: Session,
}
pub type EnvProvider = Arc<
    dyn Fn(EnvTarget) -> RuntimeFuture<Option<pi_coding_runtime::tools::ExecutionToolContext>>
        + Send
        + Sync,
>;
#[derive(Clone)]
pub struct DurableOptions {
    pub models: ModelsProvider,
    pub registry: RuntimeRegistry,
    pub settings: SettingsProvider,
    pub request_runtime: Arc<dyn Fn(CancellationToken) -> RequestRuntime + Send + Sync>,
    pub env: Option<EnvProvider>,
    pub conversation_created: Option<ConversationInitializer>,
    pub provider_session_id: Arc<dyn Fn() -> Result<String> + Send + Sync>,
    /// Process-local monotonic milliseconds, used only for elapsed tool duration.
    pub monotonic_now: Arc<dyn Fn() -> f64 + Send + Sync>,
    pub header_policy: pi_ai::ModelHeaderPolicy,
    pub scheduler: SchedulerOptions,
}
impl DurableOptions {
    pub fn new(models: Arc<ModelsRegistry>) -> Self {
        Self {
            models: Arc::new(move || models.clone()),
            registry: RuntimeRegistry::new(),
            settings: Arc::new(DurableSettings::default),
            request_runtime: Arc::new(|c| {
                RequestRuntime::with_noop_telemetry(
                    c,
                    Arc::new(NativeClock),
                    Arc::new(NativeSpawner),
                )
            }),
            env: None,
            conversation_created: None,
            provider_session_id: Arc::new(session_id),
            monotonic_now: {
                let started = std::time::Instant::now();
                Arc::new(move || started.elapsed().as_secs_f64() * 1000.0)
            },
            header_policy: Default::default(),
            scheduler: Default::default(),
        }
    }
}
struct NativeClock;
impl pi_ai::Clock for NativeClock {
    fn now_unix_ms(&self) -> u64 {
        crate::clock::wall_clock() as u64
    }
    fn sleep<'a>(
        &'a self,
        d: Duration,
        c: &'a CancellationToken,
    ) -> BoxFuture<'a, std::result::Result<(), pi_ai::Cancelled>> {
        Box::pin(async move {
            tokio::select! {_=tokio::time::sleep(d)=>Ok(()),_=c.cancelled()=>Err(pi_ai::Cancelled)}
        })
    }
}
struct NativeSpawner;
impl pi_ai::TaskSpawner for NativeSpawner {
    fn spawn(&self, f: BoxFuture<'static, ()>) -> std::result::Result<(), pi_ai::SpawnError> {
        tokio::spawn(f);
        Ok(())
    }
}
pub(crate) fn session_id() -> Result<String> {
    use std::io::Read;
    let mut bytes = [0u8; 16];
    std::fs::File::open("/dev/urandom")
        .and_then(|mut f| f.read_exact(&mut bytes))
        .map_err(|e| Error::request(format!("provider session identity entropy: {e}")))?;
    let now = crate::clock::wall_clock() as u64;
    bytes[..6].copy_from_slice(&now.to_be_bytes()[2..]);
    bytes[6] = (bytes[6] & 15) | 0x70;
    bytes[8] = (bytes[8] & 63) | 0x80;
    let h = bytes.iter().map(|b| format!("{b:02x}")).collect::<String>();
    Ok(format!(
        "{}-{}-{}-{}-{}",
        &h[..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..]
    ))
}
pub(crate) struct RuntimeInner {
    pub session: Session,
    pub scheduler: OnceLock<Scheduler>,
    pub options: DurableOptions,
    pub views: crate::view::ConversationViews,
    pub binding_id: u64,
}
impl RuntimeInner {
    pub fn scheduler(&self) -> &Scheduler {
        self.scheduler.get().expect("opened runtime")
    }
    pub fn settings(&self) -> DurableSettings {
        (self.options.settings)()
    }
    pub fn report(&self, error: Error, task: Option<TaskId>) {
        if let Some(f) = &self.options.scheduler.on_report {
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                f(Report {
                    task_id: task,
                    kind: "extension_error",
                    error,
                })
            }));
        }
    }
    pub fn check(&self) -> Result<()> {
        if let Some(e) = self.session.failure() {
            return Err(e);
        }
        if let Some(s) = self.scheduler.get() {
            s.inner.check_open()?;
        }
        Ok(())
    }
}
#[derive(Clone)]
pub struct Durable {
    pub(crate) inner: Arc<RuntimeInner>,
}
/// Weak custody for adapters: callbacks cannot keep a retired runtime alive.
#[derive(Clone)]
pub struct WeakDurable {
    inner: Weak<RuntimeInner>,
    binding_id: u64,
}
impl WeakDurable {
    pub fn binding_id(&self) -> u64 {
        self.binding_id
    }
    pub fn upgrade(&self) -> Result<Durable> {
        let inner = self
            .inner
            .upgrade()
            .ok_or_else(|| Error::request("Durable runtime retired"))?;
        inner.check()?;
        Ok(Durable { inner })
    }
}
impl Durable {
    pub async fn open(storage: Arc<dyn Storage>, options: DurableOptions) -> Result<Self> {
        let initializer_options = options.clone();
        let session = Session::with_options(
            storage,
            SessionOptions {
                now: options.scheduler.now.clone(),
                conversation_initializer: Some(Arc::new(move |tx, record| {
                    for d in [live_doc(), inbox_doc(), usage_doc()] {
                        doc(tx, &d, record.id)?;
                    }
                    let provider =
                        provider_definition(Some((initializer_options.provider_session_id)()?));
                    doc(tx, &provider, record.id)?;
                    if record.parent.is_none() {
                        let (id, mut agent) = doc(tx, &agent_doc(), record.id)?;
                        if let Some(owner) = &record.owner {
                            agent = doc(tx, &agent_doc(), owner.conversation_id)?.1;
                        }
                        save(tx, id, agent)?;
                    }
                    if let Some(callback) = &initializer_options.conversation_created {
                        callback(tx, record)?;
                    }
                    Ok(())
                })),
            },
        )?;
        let inner = Arc::new(RuntimeInner {
            views: crate::view::ConversationViews::new(session.clone()),
            session: session.clone(),
            scheduler: OnceLock::new(),
            options: options.clone(),
            binding_id: {
                static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
                NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            },
        });
        let registry = Registry::new();
        options
            .registry
            .bind(registry.clone(), Arc::downgrade(&inner))?;
        let mut scheduling = options.scheduler.clone();
        scheduling.withdraw_inputs =
            Some(Arc::new(|tx, c| super::inbox::withdraw(tx, c, "aborted")));
        scheduling.settle_outcome = Some(Arc::new(settle_scheduler));
        let scheduler = Scheduler::open(session, registry, scheduling).await?;
        inner
            .scheduler
            .set(scheduler)
            .map_err(|_| Error::request("runtime already opened"))?;
        Ok(Self { inner })
    }
    pub fn binding_id(&self) -> u64 {
        self.inner.binding_id
    }
    pub fn downgrade(&self) -> WeakDurable {
        WeakDurable {
            inner: Arc::downgrade(&self.inner),
            binding_id: self.inner.binding_id,
        }
    }
    pub fn check_open(&self) -> Result<()> {
        self.inner.check()
    }
    pub fn session(&self) -> Session {
        self.inner.session.clone()
    }
    pub fn registry(&self) -> RuntimeRegistry {
        self.inner.options.registry.clone()
    }
    pub fn resume(&self) {
        self.inner.scheduler().resume()
    }
    pub async fn root(&self, options: ConversationOptions) -> Result<Conversation> {
        let id = self
            .inner
            .scheduler()
            .commit(move |tx| {
                let existing = tx.conversation(ROOT_CONVERSATION_ID)?.is_some();
                let c = tx.ensure_root_conversation()?;
                if !existing {
                    apply_options(tx, c.id, options)?;
                }
                Ok(c.id)
            })
            .await?;
        Ok(Conversation {
            id,
            host: self.clone(),
        })
    }
    pub async fn conversation(&self, id: ConversationId) -> Result<Option<Conversation>> {
        Ok(self
            .inner
            .session
            .conversation(id)
            .await?
            .map(|_| Conversation {
                id,
                host: self.clone(),
            }))
    }
    pub async fn create_conversation(
        &self,
        options: ConversationCreateOptions,
    ) -> Result<Conversation> {
        let id = self
            .inner
            .scheduler()
            .commit(move |tx| {
                let c = tx.create_conversation(options.ownership)?;
                apply_options(tx, c.id, options.options)?;
                Ok(c.id)
            })
            .await?;
        Ok(Conversation {
            id,
            host: self.clone(),
        })
    }
    pub async fn commit<T: Send + 'static>(
        &self,
        change: impl FnOnce(&mut Transaction<'_>) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        self.inner.scheduler().commit(change).await
    }
    pub async fn wake(&self) -> Result<Wake> {
        self.inner.scheduler().wake().await
    }
    pub async fn inspect(&self) -> Result<DurableInspection> {
        let scheduling = self.inner.scheduler().inspect().await?;
        let mut submissions = Vec::new();
        let mut cursor = None;
        loop {
            let p = self
                .inner
                .session
                .scan_submissions(SubmissionQuery::default(), 256, cursor)
                .await?;
            submissions.extend(p.items.into_iter().filter(|s| !s.state.settled()));
            cursor = p.next;
            if cursor.is_none() {
                break;
            }
        }
        Ok(DurableInspection {
            scheduling,
            submissions,
        })
    }
    pub async fn get_task(&self, id: TaskId) -> Result<Option<TaskRecord>> {
        self.inner.scheduler().get_task(id).await
    }
    pub async fn wait_for_task(&self, id: TaskId) -> Result<TaskRecord> {
        self.resume();
        self.inner.scheduler().wait_for_task(id).await
    }
    pub async fn abort_task(&self, id: TaskId) -> Result<AbortDisposition> {
        self.inner.scheduler().abort_task(id).await
    }
    pub async fn retry_task(&self, id: TaskId) -> Result<()> {
        self.inner.scheduler().retry_task(id).await
    }
    pub async fn wait_for_idle(&self) -> Result<()> {
        self.resume();
        self.inner.scheduler().wait_for_idle(None).await
    }
    pub async fn close(&self) -> Result<()> {
        self.inner.scheduler().close().await
    }
    pub async fn submission(&self, id: SubmissionId) -> Result<Option<Submission>> {
        Ok(self
            .inner
            .session
            .submission(id)
            .await?
            .map(|_| Submission {
                id,
                host: self.clone(),
                guard: None,
            }))
    }
    pub async fn abort_submission(
        &self,
        id: SubmissionId,
        conversation: Option<ConversationId>,
    ) -> Result<SubmissionAbort> {
        self.inner
            .scheduler()
            .commit(move |tx| abort_receipt(tx, id, conversation))
            .await
    }
    pub async fn usage(&self) -> Result<UsageState> {
        let mut total = UsageState::default();
        let mut cursor = None;
        loop {
            let p = self
                .inner
                .session
                .scan_conversations(ConversationQuery::default(), 256, cursor)
                .await?;
            for c in p.items {
                if let Some(value) = self
                    .inner
                    .session
                    .snapshot(usage_doc(), address("pi.usage", c.id))
                    .await?
                {
                    let state: UsageState = decode(value)?;
                    for (target, source) in [
                        (&mut total.models, state.models),
                        (&mut total.tools, state.tools),
                    ] {
                        for (k, v) in source {
                            if let Some(a) = target.get_mut(&k) {
                                add_usage(a, &v)?
                            } else {
                                target.insert(k, v);
                            }
                        }
                    }
                }
            }
            cursor = p.next;
            if cursor.is_none() {
                break;
            }
        }
        Ok(total)
    }
}
pub struct DurableInspection {
    pub scheduling: Inspection,
    pub submissions: Vec<SubmissionRecord>,
}
fn apply_options(
    tx: &mut Transaction<'_>,
    c: ConversationId,
    options: ConversationOptions,
) -> Result<()> {
    if let Some(change) = options.agent {
        super::registry::configure(tx, c, change)?;
    }
    if let Some(init) = options.init {
        init(tx, c)?;
    }
    Ok(())
}
#[derive(Clone)]
pub struct Conversation {
    pub id: ConversationId,
    pub(crate) host: Durable,
}
impl Conversation {
    pub async fn agent(&self) -> Result<Agent> {
        let snapshot = self.host.registry().snapshot();
        let value = self
            .host
            .inner
            .session
            .snapshot(agent_doc(), address("pi.agent", self.id))
            .await?
            .unwrap_or(json!({}));
        Ok(super::registry::resolve(
            decode(value)?,
            &snapshot,
            &self.host.inner.settings(),
            |e| self.host.inner.report(e, None),
        ))
    }
    pub async fn configure(&self, change: AgentChange) -> Result<()> {
        let c = self.id;
        self.host
            .commit(move |tx| super::registry::configure(tx, c, change))
            .await
    }
    pub async fn submit(&self, draft: SubmissionDraft) -> Result<Submission> {
        self.host.resume();
        let c = self.id;
        let host = self.host.inner.clone();
        let snapshot = host.options.registry.snapshot();
        let id = self
            .host
            .commit(move |tx| super::inbox::admit(tx, c, draft, &host, &snapshot))
            .await?;
        Ok(Submission {
            id,
            host: self.host.clone(),
            guard: None,
        })
    }
    pub async fn reset(&self, handoff: Option<String>) -> Result<()> {
        let mut entry = EntryDraft::new("pi.reset");
        entry.head = Some(EntryHead::Own);
        if let Some(content) = handoff {
            entry.model = Some(vec![
                json!({"role":"user","content":content,"timestamp":timestamp((self.host.inner.options.scheduler.now)())?}),
            ]);
        }
        self.submit(SubmissionDraft::Write {
            request_id: None,
            entry,
        })
        .await?;
        Ok(())
    }
    pub async fn compact(&self, instructions: Option<String>) -> Result<TaskId> {
        self.host.resume();
        let c = self.id;
        let snapshot = self.host.registry().snapshot();
        let owner = Arc::downgrade(&self.host.inner);
        self.host
            .commit(move |tx| {
                super::compaction::create(
                    tx,
                    c,
                    CompactionReason::Manual,
                    instructions,
                    None,
                    owner,
                    snapshot,
                )
            })
            .await
    }
    pub async fn commit<T: Send + 'static>(
        &self,
        change: impl FnOnce(&mut Transaction<'_>) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        self.host
            .inner
            .session
            .commit_scoped(
                TransactionScope {
                    conversation_id: Some(self.id),
                    task_id: None,
                },
                change,
            )
            .await
    }
    pub async fn context(&self, at: Option<EntryId>) -> Result<crate::context::ContextView> {
        crate::context::read_context(&self.host.inner.session, self.id, at).await
    }
    pub async fn entries(
        &self,
        mut query: EntryQuery,
        limit: usize,
        cursor: Option<Cursor>,
    ) -> Result<Page<EntryRecord>> {
        query.conversation_id = self.id;
        self.host
            .inner
            .session
            .scan_entries(query, limit, cursor)
            .await
    }
    pub async fn fork(
        &self,
        at: EntryId,
        options: ConversationCreateOptions,
    ) -> Result<Conversation> {
        let parent = self.id;
        let id = self
            .host
            .commit(move |tx| {
                let c = tx.fork_conversation(parent, at, options.ownership)?;
                apply_options(tx, c.id, options.options)?;
                Ok(c.id)
            })
            .await?;
        Ok(Conversation {
            id,
            host: self.host.clone(),
        })
    }
    pub async fn abort(&self, background: bool) -> Result<()> {
        self.host.resume();
        self.host
            .inner
            .scheduler()
            .abort_conversation(self.id, background)
            .await
    }
    pub async fn wait_for_idle(&self) -> Result<()> {
        self.host.resume();
        self.host
            .inner
            .scheduler()
            .wait_for_idle(Some(self.id))
            .await
    }
    pub async fn view_state(&self) -> Result<crate::view::ConversationViewState> {
        self.host.inner.views.state(self.id).await
    }
    pub async fn watch_events(&self) -> Result<AgentEventStream> {
        super::events::attach(&self.host.inner, self.id).await
    }
    pub async fn watch(&self) -> Result<crate::view::ConversationViewWatch> {
        self.host.inner.views.watch(self.id).await
    }
}
#[derive(Clone)]
pub struct Submission {
    pub id: SubmissionId,
    pub(crate) host: Durable,
    pub(crate) guard: Option<ToolExecutionApi>,
}
impl Submission {
    pub async fn status(&self) -> Result<SubmissionRecord> {
        if let Some(guard) = &self.guard {
            guard.check_live()?;
        }
        self.host
            .inner
            .session
            .submission(self.id)
            .await?
            .ok_or_else(|| Error::request("submission does not exist"))
    }
    pub async fn wait(&self) -> Result<SubmissionRecord> {
        self.host.resume();
        let mut commits = self.host.inner.session.subscribe_commits().await?;
        loop {
            let s = self.status().await?;
            if s.state.settled() {
                return Ok(s);
            }
            let next = if let Some(guard) = &self.guard {
                let signal = guard.signal();
                tokio::select! { next = commits.next() => next, _ = signal.cancelled() => return Err(Error::request("invocation cancelled")) }
            } else {
                commits.next().await
            };
            if next.is_none() {
                return Err(self
                    .host
                    .inner
                    .session
                    .failure()
                    .unwrap_or_else(|| Error::new(ErrorKind::Closed, "Session is closed")));
            }
        }
    }
    pub async fn abort(&self) -> Result<SubmissionAbort> {
        if let Some(guard) = &self.guard {
            let id = self.id;
            guard.commit(move |tx| abort_receipt(tx, id, None)).await
        } else {
            self.host.abort_submission(self.id, None).await
        }
    }
}
pub(crate) fn abort_receipt(
    tx: &mut Transaction<'_>,
    id: SubmissionId,
    conversation: Option<ConversationId>,
) -> Result<SubmissionAbort> {
    let Some(s) = tx.submission(id)? else {
        return Ok(SubmissionAbort::NotFound);
    };
    if conversation.is_some_and(|c| c != s.conversation_id) {
        return Ok(SubmissionAbort::NotFound);
    }
    match s.state {
        SubmissionState::Queued => {
            tx.settle_submission(
                id,
                SubmissionSettlement::Unanswered {
                    reason: "aborted".into(),
                    detail: None,
                },
            )?;
            super::inbox::remove(tx, s.conversation_id, id)?;
            Ok(SubmissionAbort::Aborted)
        }
        SubmissionState::Placed { .. } => Ok(SubmissionAbort::AlreadyPlaced),
        _ => Ok(SubmissionAbort::Settled),
    }
}
#[derive(Clone)]
pub(crate) struct Phase {
    pub host: Arc<RuntimeInner>,
    pub invocation: Invocation,
    pub snapshot: RuntimeRegistrySnapshot,
    pub conversation: ConversationId,
    pub agent: Arc<tokio::sync::OnceCell<Agent>>,
    pub cancellation: CancellationToken,
}
impl Phase {
    pub fn new(
        owner: &Weak<RuntimeInner>,
        invocation: Invocation,
        snapshot: RuntimeRegistrySnapshot,
        c: ConversationId,
    ) -> Result<Self> {
        let host = owner
            .upgrade()
            .ok_or_else(|| Error::request("runtime ended"))?;
        let cancellation = CancellationToken::new();
        let signal = invocation.signal();
        let token = cancellation.clone();
        tokio::spawn(async move {
            signal.cancelled().await;
            token.cancel()
        });
        Ok(Self {
            host,
            invocation,
            snapshot,
            conversation: c,
            agent: Default::default(),
            cancellation,
        })
    }
    pub fn check(&self) -> Result<()> {
        self.invocation.now()?;
        self.host.check()?;
        if self.cancellation.is_cancelled() {
            return Err(Error::request("invocation cancelled"));
        }
        Ok(())
    }
    pub fn settings(&self) -> DurableSettings {
        self.host.settings()
    }
    pub fn now(&self) -> Result<f64> {
        self.invocation.now()
    }
    pub fn api(&self) -> HookApi {
        HookApi {
            phase: self.clone(),
        }
    }
    pub fn models(&self) -> Result<Arc<ModelsRegistry>> {
        self.check()?;
        Ok((self.host.options.models)())
    }
    pub async fn agent(&self) -> Result<Agent> {
        self.check()?;
        self.agent
            .get_or_try_init(|| async {
                let v = self
                    .host
                    .session
                    .snapshot(agent_doc(), address("pi.agent", self.conversation))
                    .await?
                    .unwrap_or(json!({}));
                Ok(super::registry::resolve(
                    decode(v)?,
                    &self.snapshot,
                    &self.settings(),
                    |e| self.report(e),
                ))
            })
            .await
            .cloned()
    }
    pub async fn hooks(&self) -> Result<Vec<Hooks>> {
        Ok(self
            .agent()
            .await?
            .extensions
            .into_iter()
            .map(|e| e.hooks)
            .collect())
    }
    pub fn report(&self, e: Error) {
        self.host
            .report(e, Some(self.invocation.identity().task_id))
    }
    pub async fn env(&self) -> Result<Option<pi_coding_runtime::tools::ExecutionToolContext>> {
        self.check()?;
        if let Some(build) = &self.host.options.env {
            let agent = self.agent().await?;
            build(EnvTarget {
                conversation_id: self.conversation,
                cwd: agent.state.cwd,
                session: self.host.session.clone(),
            })
            .await
        } else {
            Ok(None)
        }
    }
    pub fn request_runtime(&self) -> RequestRuntime {
        (self.host.options.request_runtime)(self.cancellation.clone())
    }
    pub async fn provider_session_id(&self) -> Result<String> {
        self.check()?;
        let v = self
            .host
            .session
            .snapshot(provider_doc(), address("pi.provider", self.conversation))
            .await?
            .ok_or_else(|| Error::validation("missing provider identity"))?;
        v["sessionId"]
            .as_str()
            .map(str::to_owned)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| Error::validation("invalid provider identity"))
    }
}
impl HookApi {
    pub fn binding_id(&self) -> u64 {
        self.phase.host.binding_id
    }
    pub fn task_id(&self) -> TaskId {
        self.phase.invocation.identity().task_id
    }
    pub fn conversation_id(&self) -> ConversationId {
        self.phase.conversation
    }
    pub fn check_live(&self) -> Result<()> {
        self.phase.check()
    }
    pub fn signal(&self) -> CancellationToken {
        self.phase.cancellation.clone()
    }
    pub fn models(&self) -> Result<Arc<ModelsRegistry>> {
        self.phase.models()
    }
    pub fn registry(&self) -> RuntimeRegistrySnapshot {
        self.phase.snapshot.clone()
    }
    pub fn now(&self) -> Result<f64> {
        self.phase.check()?;
        self.phase.now()
    }
    pub fn settings(&self) -> Result<DurableSettings> {
        self.phase.check()?;
        Ok(self.phase.settings())
    }
    pub async fn agent(&self) -> Result<Agent> {
        self.phase.check()?;
        self.phase.agent().await
    }
    pub async fn env(&self) -> Result<Option<pi_coding_runtime::tools::ExecutionToolContext>> {
        self.phase.check()?;
        self.phase.env().await
    }
    pub async fn commit<T: Send + 'static>(
        &self,
        change: impl FnOnce(&mut Transaction<'_>) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        self.phase
            .invocation
            .transact(move |tx, _| Ok((change(tx)?, None)))
            .await
    }
    pub async fn create_task(
        &self,
        definition: TaskDefinition,
        input: Value,
        mut options: TaskOptions,
    ) -> Result<TaskId> {
        options.conversation_id = Some(self.phase.conversation);
        self.commit(move |tx| crate::create_task(tx, &definition, input, options))
            .await
    }
    pub async fn get_task(&self, id: TaskId) -> Result<Option<TaskRecord>> {
        self.phase.invocation.get_task(id).await
    }
    pub async fn wait_for_task(&self, id: TaskId) -> Result<TaskRecord> {
        self.phase.invocation.wait_for_task(id).await
    }
    pub async fn abort_owned(&self, id: TaskId) -> Result<()> {
        self.phase.invocation.abort_owned(id).await
    }
    pub fn set_timer(
        &self,
        callback: impl Fn() + Send + Sync + 'static,
        delay_ms: f64,
    ) -> Result<Timer> {
        self.phase.invocation.set_timer(callback, delay_ms)
    }
    pub async fn memo(&self, name: &str) -> Result<Option<Value>> {
        self.phase.check()?;
        self.phase.invocation.memo(name).await
    }
    pub async fn memo_or_insert(&self, name: String, candidate: Value) -> Result<Value> {
        self.phase.check()?;
        self.phase.invocation.memo_or_insert(name, candidate).await
    }
    pub async fn snapshot(
        &self,
        definition: DocDefinition,
        address: DocumentAddress,
    ) -> Result<Option<Value>> {
        self.phase.check()?;
        self.phase.host.session.snapshot(definition, address).await
    }
    pub async fn snapshot_as_of(
        &self,
        definition: DocDefinition,
        address: DocumentAddress,
        at: EntryId,
    ) -> Result<Option<Value>> {
        self.phase.check()?;
        self.phase
            .host
            .session
            .snapshot_as_of(definition, address, at)
            .await
    }
}
fn settle_scheduler(
    tx: &mut Transaction<'_>,
    task: &TaskRecord,
    outcome: &TaskOutcome,
) -> Result<()> {
    if !["pi.generation", "pi.tool", "pi.compaction"].contains(&task.kind.as_str()) {
        return Ok(());
    }
    let (id, mut live) = doc(tx, &live_doc(), task.conversation_id)?;
    match task.kind.as_str() {
        "pi.tool" => {
            if let Some(s) = slot_mut(&mut live, task.id) {
                finish_slot(s)
            }
            remove_nested(&mut live, task.id)
        }
        "pi.compaction" => super::compaction::fail_status(
            &mut live,
            task.id,
            match outcome {
                TaskOutcome::Faulted { error, .. } => error.message.clone(),
                TaskOutcome::Orphaned { reason } => reason.clone(),
                _ => String::new(),
            },
        ),
        _ => {
            if live.pointer("/run/taskId") == Some(&json!(task.id)) {
                convert_partial(tx, task.conversation_id, &live)?;
                let (reason, detail) = match outcome {
                    TaskOutcome::Faulted { error, .. } => {
                        ("faulted".into(), Some(json!(error.message)))
                    }
                    TaskOutcome::Orphaned { reason } => (reason.clone(), None),
                    _ => ("faulted".into(), None),
                };
                end_run(
                    tx,
                    &mut live,
                    task.id,
                    SubmissionSettlement::Unanswered { reason, detail },
                )?;
            }
        }
    }
    save(tx, id, live)
}
