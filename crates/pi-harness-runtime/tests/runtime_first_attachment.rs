//! Explicit first attachment through real Session repositories and CAS authority.
mod runtime_first_attachment_support;
mod runtime_r1_r4_support;
use pi_agent_schema::*;
use pi_agent_session::{
    codec::SessionCodec, repo::MemorySessionRepo, session::RuntimeSession, storage::AtomicStorage,
};
use pi_harness_runtime::owner::RuntimeFactory;
use runtime_first_attachment_support::*;
use runtime_r1_r4_support::*;
use std::sync::{Arc, atomic::Ordering};
fn factory<S: pi_agent_session::storage::RuntimeStorage, M: SessionMetadataRecord>(
    s: &Setup,
    ids: Arc<dyn IdGenerator>,
) -> RuntimeFactory<(), S, M> {
    RuntimeFactory::new(
        s.time.clone(),
        s.pump.clone(),
        ids,
        Arc::new(()),
        Arc::new(Summary),
    )
}
fn repo(s: &Setup, ids: Arc<CountingIds>) -> MemorySessionRepo {
    MemorySessionRepo::new(
        SessionCodecOptions::default(),
        ProviderHeaderPolicy::from_allowlist(["x-public".into()]),
        ids,
        s.time.clone(),
        Arc::new(pi_telemetry::NOOP_TELEMETRY_CONTEXT),
    )
    .unwrap()
}
fn create_options(id: &str) -> SessionCreateOptions {
    SessionCreateOptions {
        id: Some(SessionId::new(id).unwrap()),
        ..Default::default()
    }
}
fn config(model_id: &str) -> LaneConfiguration {
    LaneConfiguration {
        model: LaneModel {
            provider: model().provider,
            model_id: pi_ai_core::ModelId::new(model_id).unwrap(),
        },
        thinking_level: AgentThinkingLevel::Off,
        active_tool_names: vec![],
    }
}
fn config_of<S: pi_agent_session::storage::RuntimeStorage + 'static, M: SessionMetadataRecord>(
    s: &Setup,
    session: &RuntimeSession<S, M>,
) -> Option<LaneConfiguration> {
    s.pump
        .drive(session.get_register(RegisterNamespace::LaneConfig, "main"))
        .unwrap()
        .map(|r| match r {
            Register::LaneConfig { value, .. } => value,
            _ => panic!("config kind"),
        })
}
fn unconfigured(s: &Setup) -> (Arc<ProbeStorage>, Arc<RuntimeSession<ProbeStorage>>) {
    s.pump
        .drive(s.session.commit(Transaction {
            writes: vec![Write::DeleteRegister(RegisterDelete {
                namespace: RegisterNamespace::LaneConfig,
                key: "main".into(),
            })],
        }))
        .unwrap();
    let probe = Arc::new(ProbeStorage::new(s.store.clone()));
    let session = Arc::new(
        s.pump
            .drive(RuntimeSession::new(
                probe.clone(),
                s.session.metadata(),
                Arc::new(
                    SessionCodec::new(
                        SessionCodecOptions::default(),
                        ProviderHeaderPolicy::from_allowlist(["x-public".into()]),
                    )
                    .unwrap(),
                ),
                s.ids.clone(),
                Arc::new(pi_telemetry::NOOP_TELEMETRY_CONTEXT),
            ))
            .unwrap(),
    );
    probe.reads.lock().unwrap().clear();
    (probe, session)
}
#[test]
fn fresh_memory_repo_requires_explicit_first_attachment_then_runs_real_prompt() {
    let s = Setup::new();
    let ids = Arc::new(CountingIds::default());
    let repo = repo(&s, ids.clone());
    let session = s.pump.drive(repo.create(create_options("fresh"))).unwrap();
    let f = factory(&s, ids.clone());
    assert!(
        s.pump
            .drive(f.create(s.options_for::<(), _>(session.clone(), DriveMode::Automatic)))
            .is_err()
    );
    assert!(
        s.pump
            .drive(
                f.create_configured(s.options_for::<(), _>(session.clone(), DriveMode::Automatic))
            )
            .is_err()
    );
    assert!(config_of(&s, &session).is_none());
    let before = s.pump.drive(session.get_leaf_id()).unwrap();
    let c =
        s.pump
            .drive(f.create_first_attachment(
                s.options_for::<(), _>(session.clone(), DriveMode::Automatic),
            ))
            .unwrap();
    assert_eq!(ids.0.load(Ordering::SeqCst), 0);
    assert_eq!(s.pump.drive(session.get_leaf_id()).unwrap(), before);
    assert!(s.faux.requests.lock().unwrap().is_empty());
    let result = s
        .pump
        .drive(c.harness.prompt(PromptInput::Text {
            text: "first real prompt".into(),
            images: vec![],
        }))
        .unwrap()
        .unwrap();
    assert_eq!(
        result.run_id.as_str(),
        "00000000-0007-7000-8000-000000000000"
    );
    assert_eq!(s.faux.requests.lock().unwrap().len(), 1);
}
#[test]
fn initialization_writes_only_main_config_with_exact_absent_state_leaf_guards() {
    let s = Setup::new();
    let (probe, session) = unconfigured(&s);
    let f = factory(&s, s.ids.clone());
    let before = s.pump.drive(s.store.snapshot()).unwrap();
    let telemetry = Arc::new(SeedTelemetry(std::sync::Mutex::new(vec![])));
    let mut options = s.options_for::<(), _>(session, DriveMode::Automatic);
    options.telemetry_context = telemetry.clone();
    let c = s.pump.drive(f.create_first_attachment(options)).unwrap();
    let spans = telemetry.0.lock().unwrap();
    assert_eq!(spans.len(), 1);
    assert_eq!(spans[0].name, "pi.session.write");
    assert!(
        matches!(spans[0].attributes.get("pi.session.item_count"),Some(pi_telemetry::AttributeValue::Number(value)) if value.get()==1.0)
    );
    drop(spans);
    let commits = probe.commits.lock().unwrap();
    assert_eq!(commits.len(), 1);
    let request = &commits[0];
    assert_eq!(request.transaction.writes.len(), 1);
    assert_eq!(request.conditions.len(), 3);
    assert!(
        request
            .conditions
            .iter()
            .any(|c| c.register.namespace == RegisterNamespace::LaneConfig
                && c.expected == pi_agent_session::storage::ExpectedRegister::Absent)
    );
    for ns in [RegisterNamespace::LaneState, RegisterNamespace::LaneLeaf] {
        let root = before
            .registers
            .iter()
            .find(|r| pi_agent_session::backend_support::register_key(r).namespace == ns)
            .unwrap();
        assert!(request.conditions.iter().any(|c| c.register.namespace == ns
            && c.expected
                == pi_agent_session::storage::ExpectedRegister::Seq(
                    pi_agent_session::backend_support::register_seq(root)
                )));
    }
    assert!(
        !probe
            .reads
            .lock()
            .unwrap()
            .iter()
            .any(|name| name.starts_with("scan") || matches!(*name, "stats" | "snapshot"))
    );
    drop(commits);
    drop(c);
}
fn compete(order: usize) -> (LaneConfiguration, usize, usize) {
    let s = Setup::new();
    let (probe, session) = unconfigured(&s);
    let winner = config("competing-model");
    *probe.competitor.lock().unwrap() = Some(winner.clone());
    probe.race.store(order, Ordering::SeqCst);
    let f = factory(&s, s.ids.clone());
    let _c =
        s.pump
            .drive(f.create_first_attachment(
                s.options_for::<(), _>(session.clone(), DriveMode::Automatic),
            ))
            .unwrap();
    let actual = config_of(&s, &session).unwrap();
    let calls = probe.commits.lock().unwrap().len();
    (actual, probe.applied.load(Ordering::SeqCst), calls)
}
#[test]
fn competing_configuration_wins_when_published_before_seed_cas() {
    let (actual, applied, calls) = compete(2);
    assert_eq!(
        actual,
        config("competing-model"),
        "Absent guard must preserve the competing configuration"
    );
    assert_eq!(applied, 0);
    assert_eq!(calls, 1);
}
#[test]
fn seed_first_and_competing_later_publication_preserve_the_later_configuration() {
    let (actual, applied, calls) = compete(3);
    assert_eq!(actual, config("competing-model"));
    assert_eq!(applied, 1);
    assert_eq!(calls, 1);
}
#[test]
fn changed_stamp_discards_stale_absence_and_changed_leaf_loses_cas_then_retries() {
    let (actual, applied, calls) = compete(1);
    assert_eq!(actual, config("competing-model"));
    assert_eq!((applied, calls), (0, 0));
    let s = Setup::new();
    let (probe, session) = unconfigured(&s);
    probe.race.store(4, Ordering::SeqCst);
    let f = factory(&s, s.ids.clone());
    let _c = s
        .pump
        .drive(f.create_first_attachment(s.options_for::<(), _>(session, DriveMode::Automatic)))
        .unwrap();
    assert_eq!(probe.applied.load(Ordering::SeqCst), 1);
    assert_eq!(probe.commits.lock().unwrap().len(), 2);
}
#[test]
fn applied_seed_survives_later_restore_failure_and_retry_never_overwrites_it() {
    let s = Setup::new();
    let (probe, session) = unconfigured(&s);
    probe.fail_discovery.store(true, Ordering::SeqCst);
    let f = factory(&s, s.ids.clone());
    let options = s.options_for::<(), _>(session.clone(), DriveMode::Automatic);
    let expected = LaneConfiguration {
        model: LaneModel {
            provider: options.model.provider.clone(),
            model_id: options.model.id.clone(),
        },
        thinking_level: AgentThinkingLevel::Off,
        active_tool_names: vec![],
    };
    assert!(s.pump.drive(f.create_first_attachment(options)).is_err());
    assert_eq!(config_of(&s, &session), Some(expected.clone()));
    assert_eq!(probe.applied.load(Ordering::SeqCst), 1);
    probe.fail_discovery.store(false, Ordering::SeqCst);
    let mut options = s.options_for::<(), _>(session.clone(), DriveMode::Automatic);
    options.model.id = pi_ai_core::ModelId::new("new-seed").unwrap();
    let _c = s.pump.drive(f.create_first_attachment(options)).unwrap();
    assert_eq!(config_of(&s, &session), Some(expected));
    assert_eq!(probe.applied.load(Ordering::SeqCst), 1);
}
#[test]
fn dropped_waiter_keeps_the_runtime_lease_until_the_admitted_seed_completes() {
    let s = Setup::new();
    let (probe, session) = unconfigured(&s);
    probe.pause.store(true, Ordering::SeqCst);
    let f = factory(&s, s.ids.clone());
    let mut first = Box::pin(
        f.create_first_attachment(s.options_for::<(), _>(session.clone(), DriveMode::Automatic)),
    );
    assert!(poll(first.as_mut()).is_pending());
    for _ in 0..10 {
        s.pump.tick();
        if probe.entered.is_cancelled() {
            break;
        }
    }
    assert!(probe.entered.is_cancelled());
    drop(first);
    assert!(
        s.pump
            .drive(f.create_first_attachment(
                s.options_for::<(), _>(session.clone(), DriveMode::Automatic)
            ))
            .is_err(),
        "dropped waiter must not release the retained lease"
    );
    assert!(config_of(&s, &session).is_none());
    probe.release.cancel();
    for _ in 0..20 {
        s.pump.tick();
    }
    assert_eq!(probe.applied.load(Ordering::SeqCst), 1);
    let _c = s
        .pump
        .drive(f.create_configured(s.options_for::<(), _>(session, DriveMode::Automatic)))
        .unwrap();
    assert_eq!(probe.applied.load(Ordering::SeqCst), 1);
}

#[test]
fn malformed_basic_roots_invalid_options_and_storage_failure_never_seed() {
    for case in [
        "missing-state",
        "missing-leaf",
        "active",
        "wrong-state",
        "options",
        "commit",
    ] {
        let s = Setup::new();
        let (probe, session) = unconfigured(&s);
        let writes = match case {
            "missing-state" => vec![Write::DeleteRegister(RegisterDelete {
                namespace: RegisterNamespace::LaneState,
                key: "main".into(),
            })],
            "missing-leaf" => vec![Write::DeleteRegister(RegisterDelete {
                namespace: RegisterNamespace::LaneLeaf,
                key: "main".into(),
            })],
            "active" => vec![Write::SetRegister(Box::new(RegisterSet::LaneState {
                key: "main".into(),
                value: LaneState {
                    current_operation_id: Some(OperationId::new("active").unwrap()),
                    pending_next_run: vec![],
                },
            }))],
            _ => vec![],
        };
        if !writes.is_empty() {
            s.pump
                .drive(s.session.commit(Transaction { writes }))
                .unwrap();
        }
        probe
            .wrong_state
            .store(case == "wrong-state", Ordering::SeqCst);
        probe.fail_commit.store(case == "commit", Ordering::SeqCst);
        let mut options = s.options_for::<(), _>(session.clone(), DriveMode::Automatic);
        if case == "options" {
            options.stream_options.headers = Some(
                RawProviderHeaders::new([("not-public".into(), "value".into())].into())
                    .unwrap()
                    .classify(&ProviderHeaderPolicy::from_allowlist(["not-public".into()]))
                    .unwrap(),
            );
        }
        let before = s.pump.drive(s.store.snapshot()).unwrap();
        let f = factory(&s, s.ids.clone());
        assert!(
            s.pump.drive(f.create_first_attachment(options)).is_err(),
            "must reject {case}"
        );
        assert_eq!(
            s.pump.drive(s.store.snapshot()).unwrap(),
            before,
            "must not seed {case}"
        );
        assert_eq!(probe.applied.load(Ordering::SeqCst), 0);
        assert!(
            session.claim_runtime().is_ok(),
            "error must release runtime lease"
        );
    }
}
#[test]
fn unpolled_and_host_rejected_first_attachment_have_no_seed_or_lease() {
    let s = Setup::new();
    let (probe, session) = unconfigured(&s);
    let f = factory(&s, s.ids.clone());
    drop(f.create_first_attachment(s.options_for::<(), _>(session.clone(), DriveMode::Automatic)));
    assert!(session.claim_runtime().is_ok());
    assert_eq!(probe.applied.load(Ordering::SeqCst), 0);
    s.pump.reject.store(true, Ordering::SeqCst);
    assert!(
        s.pump
            .drive(f.create_first_attachment(
                s.options_for::<(), _>(session.clone(), DriveMode::Automatic)
            ))
            .is_err()
    );
    s.pump.reject.store(false, Ordering::SeqCst);
    assert!(session.claim_runtime().is_ok());
    assert!(config_of(&s, &session).is_none());
}
#[test]
fn actual_unconfigured_memory_fork_retains_its_leaf_and_entries() {
    let s = Setup::new();
    let ids = Arc::new(CountingIds::default());
    let repo = repo(&s, ids.clone());
    let source = s.pump.drive(repo.create(create_options("source"))).unwrap();
    let leaf = s
        .pump
        .drive(
            source.append_message(AgentMessage::BuiltIn(pi_ai_core::Message::User(
                pi_ai_core::UserMessage {
                    role: Default::default(),
                    content: pi_ai_core::UserContent::Text("fork content".into()),
                    timestamp: n(7),
                },
            ))),
        )
        .unwrap();
    let fork = s
        .pump
        .drive(repo.fork(
            source.metadata(),
            SessionForkRequest {
                fork: ForkOptions::Branch(BranchForkOptions::default()),
                create: create_options("fork"),
            },
        ))
        .unwrap();
    assert!(config_of(&s, &fork).is_none());
    let entry = s.pump.drive(fork.get_entry(leaf.clone())).unwrap();
    let count = ids.0.load(Ordering::SeqCst);
    let f = factory(&s, ids.clone());
    let _c = s
        .pump
        .drive(
            f.create_first_attachment(s.options_for::<(), _>(fork.clone(), DriveMode::Automatic)),
        )
        .unwrap();
    assert_eq!(ids.0.load(Ordering::SeqCst), count);
    assert_eq!(
        s.pump.drive(fork.get_leaf_id()).unwrap(),
        Some(leaf.clone())
    );
    assert_eq!(s.pump.drive(fork.get_entry(leaf)).unwrap(), entry);
    assert!(config_of(&s, &source).is_none());
}
struct CountTool {
    descriptor: AgentTool,
    reads: std::sync::atomic::AtomicUsize,
}
impl AgentHarnessTool<()> for CountTool {
    fn prepare_arguments(
        &self,
        _: pi_ai_core::JsonValue,
    ) -> Result<pi_ai_core::JsonObject, ToolExecutionError> {
        panic!("constructor must not prepare tools")
    }
    fn descriptor(&self) -> &AgentTool {
        self.reads.fetch_add(1, Ordering::SeqCst);
        &self.descriptor
    }
    fn execute<'a>(
        &'a self,
        _: pi_ai_core::ToolCallId,
        _: pi_ai_core::JsonObject,
        _: &'a dyn Cancellation,
        _: &'a dyn ToolProgressSink,
        _: &'a (),
    ) -> BoxFuture<'a, Result<AgentToolResult, ToolExecutionError>> {
        panic!("constructor must not execute tools")
    }
}
struct ForbiddenContext;
impl ToolContextProvider<()> for ForbiddenContext {
    fn resolve(&self) -> BoxFuture<'_, Result<(), HarnessRuntimeError>> {
        panic!("constructor must not resolve context")
    }
}
struct ForbiddenPrompt;
impl SystemPromptProvider<()> for ForbiddenPrompt {
    fn resolve<'a>(&'a self, _: &'a ()) -> BoxFuture<'a, Result<String, HarnessRuntimeError>> {
        panic!("constructor must not resolve prompt")
    }
}
struct CountProduct {
    base: Product,
    reads: std::sync::atomic::AtomicUsize,
}
impl pi_ai_core::Provider for CountProduct {
    fn id(&self) -> &pi_ai_core::ProviderId {
        pi_ai_core::Provider::id(&self.base)
    }
    fn name(&self) -> &str {
        pi_ai_core::Provider::name(&self.base)
    }
    fn auth(&self) -> &pi_ai_core::ProviderAuth {
        pi_ai_core::Provider::auth(&self.base)
    }
    fn models(&self) -> Result<Arc<[pi_ai_core::Model]>, pi_ai_core::ModelsError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        pi_ai_core::Provider::models(&self.base)
    }
    fn adapter(&self, id: &pi_ai_core::ApiId) -> Option<Arc<dyn pi_ai_core::ApiAdapter>> {
        pi_ai_core::Provider::adapter(&self.base, id)
    }
}
#[test]
fn existing_config_is_preserved_and_future_lanes_use_one_original_seed_capture() {
    let s = Setup::new();
    let product = Arc::new(CountProduct {
        base: Product {
            faux: s.faux.clone(),
            auth: pi_ai_core::ProviderAuth::new(Some(Arc::new(Auth)), None).unwrap(),
        },
        reads: std::sync::atomic::AtomicUsize::new(0),
    });
    s.pump
        .drive(s.registry.set_provider(product.clone()))
        .unwrap();
    product.reads.store(0, Ordering::SeqCst);
    let original = config_of(&s, &s.session).unwrap();
    let f = factory(&s, s.ids.clone());
    let tool = Arc::new(CountTool {
        descriptor: AgentTool {
            name: pi_ai_core::ToolName::new("seed-tool").unwrap(),
            label: "tool".into(),
            description: "tool".into(),
            parameters: serde_json::from_value(serde_json::json!({"type":"object"})).unwrap(),
            constrained_sampling: None,
            replay: None,
            execution_mode: None,
        },
        reads: std::sync::atomic::AtomicUsize::new(0),
    });
    for _ in 0..2 {
        let mut options = s.options_for::<(), _>(s.session.clone(), DriveMode::Automatic);
        options.model.id = pi_ai_core::ModelId::new("future-lane-seed").unwrap();
        options.tools = vec![tool.clone()];
        options.tool_context = Some(AgentHarnessToolContextSource::Provider(Arc::new(
            ForbiddenContext,
        )));
        options.system_prompt = Some(SystemPromptSource::Provider(Arc::new(ForbiddenPrompt)));
        let before = s.pump.drive(s.store.snapshot()).unwrap();
        let reads = tool.reads.load(Ordering::SeqCst);
        let c = s.pump.drive(f.create_first_attachment(options)).unwrap();
        assert_eq!(s.pump.drive(s.store.snapshot()).unwrap(), before);
        assert_eq!(tool.reads.load(Ordering::SeqCst), reads + 1);
        assert_eq!(
            product.reads.load(Ordering::SeqCst),
            0,
            "constructor must not query provider models"
        );
        drop(c);
    }
    let mut options = s.options_for::<(), _>(s.session.clone(), DriveMode::Automatic);
    options.model.id = pi_ai_core::ModelId::new("future-lane-seed").unwrap();
    options.thinking_level = Some(AgentThinkingLevel::High);
    options.tools = vec![tool.clone()];
    let c = s.pump.drive(f.create_first_attachment(options)).unwrap();
    let reads = tool.reads.load(Ordering::SeqCst);
    assert_eq!(config_of(&s, &s.session), Some(original));
    assert!(s.pump.drive(c.harness.get_model()).unwrap().is_some());
    assert!(
        product.reads.load(Ordering::SeqCst) > 0,
        "explicit get_model must be distinguishable from create"
    );
    s.pump
        .drive(c.harness.set_thinking_level(AgentThinkingLevel::Off))
        .unwrap();
    s.pump
        .drive(c.harness.create_lane(LaneId::new("later").unwrap(), None))
        .unwrap()
        .unwrap();
    let register = s
        .pump
        .drive(
            s.session
                .get_register(RegisterNamespace::LaneConfig, "later"),
        )
        .unwrap()
        .unwrap();
    let Register::LaneConfig { value, .. } = register else {
        unreachable!()
    };
    assert_eq!(value.model.model_id.as_str(), "future-lane-seed");
    assert_eq!(value.thinking_level, AgentThinkingLevel::High);
    assert_eq!(
        value.active_tool_names,
        vec![pi_ai_core::ToolName::new("seed-tool").unwrap()]
    );
    assert_eq!(tool.reads.load(Ordering::SeqCst), reads);
    assert!(s.faux.requests.lock().unwrap().is_empty());
}
fn jsonl_options(
    s: &Setup,
    session: Arc<pi_agent_session::repo::jsonl::JsonlSession>,
) -> AgentHarnessOptions<
    (),
    Arc<dyn pi_telemetry::TelemetryContext>,
    pi_agent_session::repo::jsonl::JsonlSession,
    pi_ai_core::ModelsRegistry,
> {
    let o = s.options::<()>(DriveMode::Automatic);
    AgentHarnessOptions {
        session,
        models: o.models,
        model: o.model,
        thinking_level: o.thinking_level,
        active_tool_names: o.active_tool_names,
        tools: o.tools,
        tool_context: o.tool_context,
        system_prompt: o.system_prompt,
        resources: o.resources,
        stream_options: o.stream_options,
        retry: o.retry,
        compaction: o.compaction,
        steering_mode: o.steering_mode,
        follow_up_mode: o.follow_up_mode,
        tool_execution: o.tool_execution,
        drive: o.drive,
        to_provider_messages: o.to_provider_messages,
        entry_projectors: o.entry_projectors,
        telemetry_context: o.telemetry_context,
    }
}
#[test]
fn normalized_v3_jsonl_first_seed_is_the_actual_conversion_write_and_preserves_leaf() {
    let s = Setup::new();
    let ids = Arc::new(CountingIds::default());
    let dir = std::env::temp_dir().join(format!("pi-first-attachment669-{}", std::process::id()));
    std::fs::create_dir(&dir).unwrap();
    let path = dir.join("legacy.jsonl");
    let original = include_bytes!("../../../conformance/session/jsonl/repository-process-v3.jsonl");
    std::fs::write(&path, original).unwrap();
    let repo = pi_agent_session::repo::jsonl::JsonlSessionRepo::new(
        &dir,
        SessionCodecOptions::default(),
        ProviderHeaderPolicy::default(),
        ids.clone(),
        s.time.clone(),
        Arc::new(pi_telemetry::NOOP_TELEMETRY_CONTEXT),
        pi_agent_session::jsonl::JsonlOptions::default(),
    )
    .unwrap();
    let session = s.pump.drive(repo.open_path(&path)).unwrap();
    let leaf = s.pump.drive(session.get_leaf_id()).unwrap().unwrap();
    let entry = s.pump.drive(session.get_entry(leaf.clone())).unwrap();
    assert!(config_of(&s, &session).is_none());
    let f = factory(&s, ids.clone());
    assert!(
        s.pump
            .drive(f.create_configured(jsonl_options(&s, session.clone())))
            .is_err()
    );
    assert_eq!(std::fs::read(&path).unwrap(), original);
    let c = s
        .pump
        .drive(f.create_first_attachment(jsonl_options(&s, session.clone())))
        .unwrap();
    assert_eq!(ids.0.load(Ordering::SeqCst), 0);
    assert_eq!(
        s.pump.drive(session.get_leaf_id()).unwrap(),
        Some(leaf.clone())
    );
    assert_eq!(s.pump.drive(session.get_entry(leaf)).unwrap(), entry);
    let bytes = std::fs::read(&path).unwrap();
    let rows: Vec<serde_json::Value> = bytes
        .split(|b| *b == b'\n')
        .filter(|l| !l.is_empty())
        .map(|l| serde_json::from_slice(l).unwrap())
        .collect();
    assert_eq!(rows[0]["v"], 4);
    assert_eq!(
        rows.len() as u64,
        rows[0]["snapshotItems"].as_u64().unwrap() + 2,
        "one seed frame follows the complete normalized snapshot"
    );
    assert_eq!(rows.last().unwrap()["kind"], "register");
    assert_eq!(rows.last().unwrap()["namespace"], "lane.config");
    assert_eq!(rows.last().unwrap()["key"], "main");
    assert_eq!(rows.last().unwrap()["seq"], rows[0]["nextSeq"]);
    assert!(config_of(&s, &session).is_some());
    s.pump.drive(c.harness.close()).unwrap();
    drop(c);
    drop(session);
    drop(repo);
    std::fs::remove_dir_all(dir).unwrap();
}

struct SeedTelemetry(std::sync::Mutex<Vec<pi_telemetry::SpanOptions>>);
impl pi_telemetry::TelemetryContext for SeedTelemetry {
    fn start_span(&self, options: pi_telemetry::SpanOptions) -> pi_telemetry::SpanGuard {
        self.0.lock().unwrap().push(options);
        pi_telemetry::SpanGuard::new(pi_telemetry::NoopTelemetrySpan)
    }
}
