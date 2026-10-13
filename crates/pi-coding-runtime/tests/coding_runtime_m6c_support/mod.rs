//! Fresh lifecycle observations; repositories and storage retain their existing authority.
use super::*;
use pi_agent::schema::{
    BranchForkOptions, Entry, ForkOptions, ProviderHeaderPolicy, Register, RegisterNamespace,
    Session, SessionCodecOptions, SessionForkRequest, SessionMetadataRecord,
};
use pi_agent_session::{
    jsonl::{JsonlOptions, JsonlStorage},
    repo::jsonl::{JsonlSession, JsonlSessionMetadata, JsonlSessionRepo},
    session::RuntimeSession,
    storage::RuntimeStorage,
};
use pi_coding_runtime::{CodingHarness, CodingSessionLifecycle, LegacyImportSource};
use std::path::{Path, PathBuf};

#[cfg(feature = "extensions")]
mod extensions;

pub const ROWS: &[&str] = &[
    "branch.fork_atomic_publish",
    "restart.recover_session",
    "legacy_import.format3",
    "model_switch.next_run",
    "extension.participation_ordering",
    "lifecycle.builder_replace",
];

#[derive(Default)]
struct Trace {
    events: Mutex<Vec<HarnessEvent>>,
    subscriptions: Mutex<Vec<Arc<dyn pi_agent::schema::EventSubscription>>>,
}
impl EventListener for Trace {
    fn on_event(&self, event: HarnessEvent) -> BoxFuture<'_, Result<(), HandlerError>> {
        self.events.lock().unwrap().push(event);
        Box::pin(async { Ok(()) })
    }
}
impl Trace {
    fn attach<S: RuntimeStorage, M: SessionMetadataRecord>(
        self: &Arc<Self>,
        harness: &CodingHarness<S, M>,
    ) {
        for kind in [
            HarnessEventType::RunStart,
            HarnessEventType::TurnStart,
            HarnessEventType::TurnEnd,
            HarnessEventType::MessageEnd,
            HarnessEventType::EntryAdded,
            HarnessEventType::RunEnd,
            HarnessEventType::ConfigUpdate,
            HarnessEventType::RunAbort,
            HarnessEventType::ToolStart,
            HarnessEventType::ToolUpdate,
            HarnessEventType::ToolEnd,
            HarnessEventType::QueueUpdate,
        ] {
            self.subscriptions
                .lock()
                .unwrap()
                .push(harness.harness().events().on(kind, self.clone()));
        }
    }
    fn json(&self) -> Value {
        json!(self.events.lock().unwrap().iter().enumerate().map(|(index, event)| {
            let raw = serde_json::to_value(event).unwrap();
            let mut item = json!({"index":index,"type":raw["type"]});
            let payload = match event {
                HarnessEvent::EntryAdded(e) => Some(json!({"entry":entry_json(&e.entry)})),
                HarnessEvent::MessageEnd(e) => Some(json!({"entry_id":e.entry_id,"message":message_json(&e.message)})),
                HarnessEvent::GlobalConfigUpdate(e) => Some(json!({"property":e.property})),
                HarnessEvent::ModelConfigUpdate(e) => Some(json!({"property":e.property,"value":e.value,"previous":e.previous})),
                HarnessEvent::ActiveToolsConfigUpdate(e) => Some(json!({"property":e.property,"value":e.value,"previous":e.previous})),
                HarnessEvent::RunEndCompletedWithFinal(e) => Some(json!({"run_id":e.run_id,"outcome":e.outcome,"leaf_id":e.leaf_id})),
                HarnessEvent::RunEndCompletedWithoutFinal(e) => Some(json!({"run_id":e.run_id,"outcome":e.outcome,"leaf_id":e.leaf_id})),
                HarnessEvent::RunEndAbortedWithFinal(e) => Some(json!({"run_id":e.run_id,"outcome":e.outcome,"leaf_id":e.leaf_id})),
                HarnessEvent::RunEndAbortedWithoutFinal(e) => Some(json!({"run_id":e.run_id,"outcome":e.outcome,"leaf_id":e.leaf_id})),
                HarnessEvent::RunAbort(e) => Some(json!({"run_id":e.run_id,"steer":e.steer.iter().map(message_json).collect::<Vec<_>>(),"follow_up":e.follow_up.iter().map(message_json).collect::<Vec<_>>()})),
                HarnessEvent::ToolStart(e) => Some(json!({"tool_call_id":e.tool_call_id,"tool_name":e.tool_name,"args":e.args})),
                HarnessEvent::ToolUpdate(e) => Some(json!({"tool_call_id":e.tool_call_id,"tool_name":e.tool_name,"partial_result":e.partial_result})),
                HarnessEvent::ToolEnd(e) => Some(json!({"tool_call_id":e.tool_call_id,"tool_name":e.tool_name,"result":e.result,"is_error":e.is_error,"terminate":e.terminate})),
                HarnessEvent::QueueUpdate(e) => Some(json!({"steer":queued_json(&e.steer),"follow_up":queued_json(&e.follow_up),"next_run":queued_json(&e.next_run)})),
                _ => None,
            };
            if let Some(payload) = payload { item["payload"] = payload; }
            item
        }).collect::<Vec<_>>())
    }
    fn base(&self, fixture: &Fixture, durable_entries: usize) -> Value {
        let events = self.json();
        let events = events.as_array().unwrap();
        let publications = events.iter().filter(|e| e["type"] == "entry_added").count();
        assert_eq!(
            publications, durable_entries,
            "actual publications must match session deltas"
        );
        json!({"provider_calls":fixture.adapter.calls.load(Ordering::SeqCst),
            "tool_invocations":events.iter().filter(|e| e["type"] == "tool_start").count(),
            "session_publication_events":publications,"durable_entries":durable_entries,
            "event_count":events.len(),"event_types":events.iter().map(|e| e["type"].clone()).collect::<Vec<_>>(),
            "terminal_kind":"completed","provider_requests":requests(fixture)})
    }
}
fn message_json(message: &AgentMessage) -> Value {
    let raw = serde_json::to_value(message).unwrap();
    let mut value = json!({"role":raw["role"],"content":raw["content"]});
    for (source, target) in [
        ("stopReason", "stop_reason"),
        ("errorMessage", "error_message"),
        ("toolCallId", "tool_call_id"),
        ("toolName", "tool_name"),
        ("isError", "is_error"),
    ] {
        if let Some(field) = raw.get(source) {
            value[target] = field.clone();
        }
    }
    value
}
fn queued_json(items: &[pi_agent::schema::QueuedItem]) -> Value {
    json!(
        items
            .iter()
            .map(|item| json!({"entry_id":item.entry_id,"message":message_json(&item.message)}))
            .collect::<Vec<_>>()
    )
}
fn entry_json(entry: &Entry) -> Value {
    let raw = serde_json::to_value(entry).unwrap();
    let mut value =
        json!({"type":raw["type"],"id":raw["id"],"parent_id":raw["parentId"],"seq":raw["seq"]});
    if let Entry::Message(e) = entry {
        value["message"] = message_json(&e.message);
    }
    if let Entry::Custom(e) = entry {
        value["custom_type"] = json!(e.custom_type);
        value["data"] = json!(e.data);
    }
    value
}
fn context_json(context: &pi_ai::Context) -> Value {
    json!({"system_prompt":context.system_prompt,"messages":context.messages.iter().map(|message| {
        message_json(&serde_json::from_value::<AgentMessage>(serde_json::to_value(message).unwrap()).unwrap())
    }).collect::<Vec<_>>(),"tools":context.tools.as_ref().map(|ts| ts.iter().map(|t| t.name.to_string()).collect::<Vec<_>>()).unwrap_or_default()})
}
fn requests(fixture: &Fixture) -> Value {
    let models = fixture.adapter.resolved_models.lock().unwrap();
    let contexts = fixture.adapter.contexts.lock().unwrap();
    assert_eq!(models.len(), contexts.len());
    json!(models.iter().zip(contexts.iter()).map(|(model, context)| json!({
        "model":{"provider":model.provider,"id":model.id,"name":model.name},"context":context_json(context)
    })).collect::<Vec<_>>())
}
async fn snapshot<S: RuntimeStorage + 'static, M: SessionMetadataRecord>(
    session: &RuntimeSession<S, M>,
) -> Value {
    let metadata = session.metadata();
    let base = metadata.base_metadata();
    let config = match session
        .get_register(RegisterNamespace::LaneConfig, "main")
        .await
        .unwrap()
    {
        Some(Register::LaneConfig { value, .. }) => json!(value),
        None => Value::Null,
        other => panic!("config: {other:?}"),
    };
    let state = match session
        .get_register(RegisterNamespace::LaneState, "main")
        .await
        .unwrap()
    {
        Some(Register::LaneState { value, .. }) => json!(value),
        None => Value::Null,
        other => panic!("state: {other:?}"),
    };
    let mut entries = session.find_entries(EntryQuery::default()).await.unwrap();
    entries.sort_by_key(|entry| {
        serde_json::to_value(entry).unwrap()["seq"]
            .as_u64()
            .unwrap()
    });
    let stats = session.get_stats().await.unwrap();
    json!({"session_id":base.id,"parent_session_id":base.parent_session_id,"cwd":base.cwd,
        "leaf_id":session.get_leaf_id().await.unwrap(),"entries":entries.iter().map(entry_json).collect::<Vec<_>>(),
        "lane_config":config,"lane_state":state,"message_count":stats.message_count,"usage_total":stats.usage.total_tokens})
}
fn builder(f: &Fixture, session: Arc<MemorySession>) -> CodingHarnessBuilder<MemoryStorage> {
    CodingHarnessBuilder::new(
        f.factory.clone(),
        session,
        f.registry.clone(),
        f.environment.clone(),
        Resources::default(),
        settings(&f.model),
    )
    .with_message_converter(Arc::new(CodingMessageConverter::new().unwrap()))
}
async fn prompt(harness: &impl AgentLane, text: &str) {
    let run = tokio::time::timeout(
        Duration::from_secs(10),
        harness.prompt(PromptInput::Text {
            text: text.into(),
            images: vec![],
        }),
    )
    .await
    .unwrap_or_else(|_| panic!("{text}: lifecycle prompt timed out"))
    .unwrap_or_else(|error| panic!("{text}: {error:?}"))
    .unwrap_or_else(|error| panic!("{text}: {error:?}"));
    assert!(
        matches!(
            run.outcome,
            RunOutcome::CompletedWithAssistant(_) | RunOutcome::CompletedWithoutAssistant(_)
        ),
        "real prompt: {:?}",
        run.outcome
    );
    harness.wait_for_idle().await.unwrap();
}
fn result(case: &str, trace: &Trace, obs: Value) -> Value {
    trace.subscriptions.lock().unwrap().clear();
    envelope_with_events(
        case,
        json!({"kind":"lifecycle","operation":case.split('.').nth(1).unwrap()}),
        trace.json(),
        json!({"kind":"completed","error":null,"terminal":true}),
        obs,
    )
}
fn entries_len(snapshot: &Value) -> usize {
    snapshot["entries"].as_array().unwrap().len()
}

pub async fn actual_operation(case: &str) -> Value {
    match case {
        "branch.fork_atomic_publish" => fork().await,
        "restart.recover_session" => restart().await,
        "legacy_import.format3" => import().await,
        "model_switch.next_run" => switch().await,
        "lifecycle.builder_replace" => replace().await,
        #[cfg(feature = "extensions")]
        "extension.participation_ordering" => extensions::participation().await,
        other => panic!("feature-required or unknown M6C row: {other}"),
    }
}
async fn fork() -> Value {
    let f = fixture().await;
    let trace = Arc::new(Trace::default());
    let mut life = CodingSessionLifecycle::new();
    trace.attach(
        life.attach(builder(&f, f.session.clone()).first_attachment())
            .await
            .unwrap(),
    );
    prompt(life.current_harness().unwrap().as_ref(), "fork source").await;
    let source = snapshot(f.session.as_ref()).await;
    let metadata = f.session.metadata();
    let destination = f
        .repo
        .fork(
            metadata.clone(),
            SessionForkRequest {
                fork: ForkOptions::Branch(BranchForkOptions::default()),
                create: SessionCreateOptions {
                    id: Some("m6-fork".parse().unwrap()),
                    ..Default::default()
                },
            },
        )
        .await
        .unwrap();
    let published = snapshot(destination.as_ref()).await;
    let source_after_fork = snapshot(f.session.as_ref()).await;
    assert_eq!(source, source_after_fork, "fork must preserve source");
    assert_eq!(source["entries"], published["entries"]);
    assert_eq!(source["leaf_id"], published["leaf_id"]);
    assert_eq!(source["lane_config"], published["lane_config"]);
    trace.attach(
        life.attach_fork(builder(&f, destination.clone()))
            .await
            .unwrap(),
    );
    let attached = snapshot(destination.as_ref()).await;
    prompt(life.current_harness().unwrap().as_ref(), "fork continued").await;
    let continued = snapshot(destination.as_ref()).await;
    let source_reopened = f.repo.open(metadata).await.unwrap();
    let source_after_continue = snapshot(source_reopened.as_ref()).await;
    assert_eq!(
        source, source_after_continue,
        "destination writes must not alter source"
    );
    let mut obs = trace.base(
        &f,
        entries_len(&source) + entries_len(&continued) - entries_len(&published),
    );
    obs["fork"] = json!({"source":source,"published":published,"attached":attached,"continued":continued,"source_after_continue":source_after_continue});
    obs["fixture_boundary"] = json!(
        "MemorySessionRepo owns atomic destination publication; host file export is unverified"
    );
    life.close().await.unwrap();
    source_reopened.close().await.unwrap();
    result("branch.fork_atomic_publish", &trace, obs)
}

struct FileFixture {
    root: PathBuf,
    repo: JsonlSessionRepo,
    factory: Arc<
        RuntimeFactory<pi_coding_runtime::ExecutionToolContext, JsonlStorage, JsonlSessionMetadata>,
    >,
}
impl FileFixture {
    fn new(f: &Fixture, label: &str) -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let root = std::env::temp_dir().join(format!(
            "coding-m6c-{label}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&root).unwrap();
        let ids = Arc::new(FixtureIds::default());
        let repo = JsonlSessionRepo::new(
            &root,
            SessionCodecOptions::default(),
            ProviderHeaderPolicy::default(),
            ids.clone(),
            f.clock.clone(),
            Arc::new(NOOP_TELEMETRY_CONTEXT),
            JsonlOptions::default(),
        )
        .unwrap();
        let factory = Arc::new(RuntimeFactory::new(
            f.clock.clone(),
            Arc::new(TokioSpawner),
            ids,
            Arc::new(pi_coding_runtime::ExecutionToolContext {
                env: f.environment.clone(),
            }),
            Arc::new(Summary),
        ));
        Self {
            root,
            repo,
            factory,
        }
    }
    fn builder(
        &self,
        f: &Fixture,
        session: Arc<JsonlSession>,
    ) -> CodingHarnessBuilder<JsonlStorage, JsonlSessionMetadata> {
        CodingHarnessBuilder::new(
            self.factory.clone(),
            session,
            f.registry.clone(),
            f.environment.clone(),
            Resources::default(),
            settings(&f.model),
        )
        .with_message_converter(Arc::new(CodingMessageConverter::new().unwrap()))
    }
}
impl Drop for FileFixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.root).unwrap();
    }
}
async fn restart() -> Value {
    let f = fixture().await;
    let file = FileFixture::new(&f, "restart");
    let original = file
        .repo
        .create(SessionCreateOptions {
            id: Some("m6-restart".parse().unwrap()),
            ..Default::default()
        })
        .await
        .unwrap();
    let trace = Arc::new(Trace::default());
    let mut life = CodingSessionLifecycle::new();
    trace.attach(
        life.attach(file.builder(&f, original.clone()).first_attachment())
            .await
            .unwrap(),
    );
    prompt(life.current_harness().unwrap().as_ref(), "restart before").await;
    let before = snapshot(original.as_ref()).await;
    let metadata = original.metadata();
    life.close().await.unwrap();
    let reopened = file.repo.open(metadata).await.unwrap();
    let new_backend = !Arc::ptr_eq(&original, &reopened);
    assert!(new_backend);
    let reopened_snapshot = snapshot(reopened.as_ref()).await;
    assert_eq!(before, reopened_snapshot);
    trace.attach(
        life.restart(file.builder(&f, reopened.clone()))
            .await
            .unwrap(),
    );
    let recovered = snapshot(reopened.as_ref()).await;
    assert_eq!(before, recovered);
    prompt(life.current_harness().unwrap().as_ref(), "restart after").await;
    let after = snapshot(reopened.as_ref()).await;
    let mut obs = trace.base(&f, entries_len(&after));
    obs["restart"] = json!({"before_close":before,"reopened":reopened_snapshot,"recovered":recovered,"continued":after,"new_backend":new_backend});
    obs["fixture_boundary"] = json!(
        "host-selected temporary JSONL path; JsonlSessionRepo owns reopen and recovery; crash/power-loss is unverified"
    );
    life.close().await.unwrap();
    result("restart.recover_session", &trace, obs)
}
fn legacy_bytes() -> Vec<u8> {
    // The source format and preserved nonzero usage follow repo/jsonl legacy-v3 adoption.
    let rows = [
        json!({"type":"session","version":3,"id":"process","timestamp":"1970-01-01T00:00:00.001Z","cwd":"source"}),
        json!({"type":"custom","id":"old","parentId":null,"timestamp":"1970-01-01T00:00:00.002Z","customType":"note","data":{"opaque":"old"}}),
        json!({"type":"message","id":"assistant","parentId":"old","timestamp":"1970-01-01T00:00:00.003Z","message":{
            "role":"assistant","content":[{"type":"text","text":"legacy answer","textSignature":"keep"}],"api":"a","provider":"p","model":"m",
            "usage":{"input":1,"output":2,"cacheRead":3,"cacheWrite":4,"totalTokens":99,"cost":{"input":0.25,"output":0.5,"cacheRead":0.75,"cacheWrite":1,"total":-0.125}},"stopReason":"stop","timestamp":78}}),
    ];
    let mut bytes = Vec::new();
    for row in rows {
        bytes.extend(serde_json::to_vec(&row).unwrap());
        bytes.push(b'\n');
    }
    bytes
}
fn jsonl(path: &Path) -> Vec<Value> {
    std::fs::read(path)
        .unwrap()
        .split(|b| *b == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).unwrap())
        .collect()
}
async fn import() -> Value {
    let f = fixture().await;
    let file = FileFixture::new(&f, "import");
    let path = file.root.join("legacy.jsonl");
    let bytes = legacy_bytes();
    std::fs::write(&path, &bytes).unwrap();
    let imported = LegacyImportSource::new(&path)
        .open(&file.repo)
        .await
        .unwrap();
    let source_preserved = std::fs::read(&path).unwrap() == bytes;
    assert!(source_preserved);
    let opened = snapshot(imported.as_ref()).await;
    assert_eq!(opened["usage_total"], 99);
    assert_ne!(opened["leaf_id"], "assistant");
    let trace = Arc::new(Trace::default());
    let mut life = CodingSessionLifecycle::new();
    let attached = life
        .attach_import(file.builder(&f, imported.clone()).first_attachment())
        .await
        .unwrap();
    trace.attach(attached);
    let adopted = snapshot(imported.as_ref()).await;
    let rows = jsonl(&path);
    assert_eq!(rows[0]["v"], 4);
    let provenance = rows
        .iter()
        .filter_map(|r| r.get("details"))
        .filter(|d| d["source"] == "v3-import")
        .cloned()
        .collect::<Vec<_>>();
    assert!(!provenance.is_empty());
    prompt(life.current_harness().unwrap().as_ref(), "import continued").await;
    let continued = snapshot(imported.as_ref()).await;
    let mut obs = trace.base(&f, entries_len(&continued) - entries_len(&opened));
    obs["legacy_import"] = json!({"source_version":3,"source_preserved_before_write":source_preserved,"opened":opened,"adopted":adopted,"continued":continued,"persisted_version":rows[0]["v"],"provenance":provenance});
    obs["fixture_boundary"] = json!(
        "host supplies external JSONL path; Rust opens in place and adopts on first checked write; upstream copy-to-active-directory is unimplemented"
    );
    life.close().await.unwrap();
    result("legacy_import.format3", &trace, obs)
}
async fn switch() -> Value {
    let f = fixture().await;
    let alternate = model("offline-next");
    f.registry
        .set_provider(Arc::new(FixtureProvider {
            id: f.model.provider.clone(),
            models: vec![f.model.clone(), alternate.clone()],
            auth: ProviderAuth::no_auth(),
            adapter: f.adapter.clone(),
        }))
        .await
        .unwrap();
    let h = builder(&f, f.session.clone())
        .first_attachment()
        .with_model_override({
            let mut overlay = f.model.clone();
            overlay.name = "initial overlay".into();
            overlay
        })
        .build()
        .await
        .unwrap();
    let trace = Arc::new(Trace::default());
    trace.attach(&h);
    prompt(h.harness().as_ref(), "model before").await;
    let before = snapshot(f.session.as_ref()).await;
    h.switch_model(alternate.provider.clone(), alternate.id.clone())
        .await
        .unwrap();
    let switched = snapshot(f.session.as_ref()).await;
    prompt(h.harness().as_ref(), "model after").await;
    let after = snapshot(f.session.as_ref()).await;
    let model_readback = h.harness().get_model().await.unwrap().unwrap();
    assert_eq!(model_readback, alternate);
    let observed = f.adapter.resolved_models.lock().unwrap().clone();
    assert_eq!(observed.len(), 2);
    assert_eq!(
        observed[1], alternate,
        "switch must clear attachment overlay and reach next real request"
    );
    let mut obs = trace.base(&f, entries_len(&after));
    obs["model_switch"] = json!({"before":before,"switched":switched,"continued":after,"readback":{"provider":model_readback.provider,"id":model_readback.id,"name":model_readback.name}});
    obs["fixture_boundary"] = json!(
        "host registry owns model catalog and no-auth adapter; live provider reconfiguration is unverified"
    );
    h.harness().close().await.unwrap();
    result("model_switch.next_run", &trace, obs)
}
async fn replace() -> Value {
    let f = fixture().await;
    let destination = f
        .repo
        .create(SessionCreateOptions {
            id: Some("m6-replacement".parse().unwrap()),
            ..Default::default()
        })
        .await
        .unwrap();
    let trace = Arc::new(Trace::default());
    let mut life = CodingSessionLifecycle::new();
    trace.attach(
        life.attach(builder(&f, f.session.clone()).first_attachment())
            .await
            .unwrap(),
    );
    let old = life.current_harness().unwrap().clone();
    let release = pi_ai::CancellationToken::new();
    *f.adapter.hold.lock().unwrap() = Some(release);
    let operation = tokio::spawn(async move {
        old.prompt(PromptInput::Text {
            text: "replace active".into(),
            images: vec![],
        })
        .await
    });
    tokio::time::timeout(Duration::from_secs(10), async {
        while f.adapter.active_streams.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let busy_claim = f.session.claim_runtime().is_err();
    assert!(busy_claim);
    let before = snapshot(f.session.as_ref()).await;
    let replacement = life
        .replace(builder(&f, destination.clone()).first_attachment())
        .await
        .unwrap();
    trace.attach(replacement);
    let cancelled = operation.await.unwrap().unwrap().unwrap();
    assert!(matches!(
        cancelled.outcome,
        RunOutcome::AbortedWithAssistant(_) | RunOutcome::AbortedWithoutAssistant(_)
    ));
    let released_claim = f.session.claim_runtime().is_ok();
    assert!(released_claim);
    let destination_busy = destination.claim_runtime().is_err();
    assert!(destination_busy);
    let reopened = f.repo.open(f.session.metadata()).await.unwrap();
    let settled = snapshot(reopened.as_ref()).await;
    prompt(
        life.current_harness().unwrap().as_ref(),
        "replace continued",
    )
    .await;
    let after = snapshot(destination.as_ref()).await;
    let mut cleanup_register_counts = serde_json::Map::new();
    for namespace in [
        RegisterNamespace::OperationMeta,
        RegisterNamespace::OperationState,
        RegisterNamespace::OperationToolArguments,
        RegisterNamespace::OperationPreparation,
        RegisterNamespace::PendingEntry,
    ] {
        let name = serde_json::to_value(namespace)
            .unwrap()
            .as_str()
            .unwrap()
            .to_owned();
        let registers = reopened.list_registers(namespace, None).await.unwrap();
        assert!(registers.is_empty(), "old owner must settle {name}");
        cleanup_register_counts.insert(name, json!(registers.len()));
    }
    let mut obs = trace.base(&f, entries_len(&settled) + entries_len(&after));
    obs["replacement"] = json!({"active_source":before,"settled_source":settled,"continued_destination":after,"source_claim_busy":busy_claim,"source_claim_released":released_claim,"destination_claim_busy":destination_busy,
        "adapter_lifecycle":f.adapter.lifecycle.lock().unwrap().clone(),"active_streams":f.adapter.active_streams.load(Ordering::SeqCst),"cleanup_register_counts":cleanup_register_counts});
    obs["fixture_boundary"] = json!(
        "CodingSessionLifecycle sequences abort/idle/close/attach; MemorySession owns leases; host process teardown is unverified"
    );
    life.close().await.unwrap();
    reopened.close().await.unwrap();
    result("lifecycle.builder_replace", &trace, obs)
}
