use super::*;

use pi_agent::schema::{
    AgentMessage, BoxFuture, EventListener, HandlerError, HarnessEvent, HarnessEventType,
};
use pi_ai::{
    AssistantContent, AssistantMessage, Message, StopReason, ToolCall, ToolCallId, ToolCallType,
    ToolName, UserMessage,
};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use pi_coding_runtime::{
    CodingHarnessBuilder, EnvError, ExecutionEnv, ExecutionToolContext, FileInfo, ProcessOptions,
    ProcessOutput, ProcessedImage,
};

pub const ROWS: &[&str] = &[
    "tools.validate_progress_settle",
    "tools.coding_operations",
    "queue.steer_during_turn",
    "queue.follow_up_after_turn",
    "retry.overflow_precedence",
    "retry.provider_recovery",
    "abort.active_stream_cleanup",
    "compaction.context_projection",
];

pub struct StreamCleanup(pub Arc<AtomicUsize>);
impl Drop for StreamCleanup {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

pub fn assistant(model: &Model, reason: StopReason, text: &str) -> AssistantMessage {
    AssistantMessage {
        role: Default::default(),
        content: if text.is_empty() {
            Vec::new()
        } else {
            vec![AssistantContent::Text(TextContent {
                content_type: Default::default(),
                text: text.into(),
                text_signature: None,
            })]
        },
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
            .then(|| text.to_owned()),
        raw_stop_reason: None,
        end_turn: None,
        timestamp: SafeUnsigned::new(7).expect("timestamp"),
    }
}

fn tool_response(model: &Model, name: &str, id: &str, arguments: Value) -> AssistantMessage {
    AssistantMessage {
        role: Default::default(),
        content: vec![AssistantContent::ToolCall(ToolCall {
            content_type: ToolCallType::ToolCall,
            id: ToolCallId::new(id).expect("tool call ID"),
            name: ToolName::new(name).expect("tool name"),
            arguments: serde_json::from_value(arguments).expect("tool arguments"),
            thought_signature: None,
            namespace: None,
        })],
        api: model.api.clone(),
        provider: model.provider.clone(),
        model: model.id.clone(),
        response_model: None,
        response_id: None,
        diagnostics: None,
        usage: Usage::zero(),
        stop_reason: StopReason::ToolUse,
        deferred: None,
        error_message: None,
        raw_stop_reason: None,
        end_turn: None,
        timestamp: SafeUnsigned::new(7).expect("timestamp"),
    }
}

#[derive(Default)]
struct MemoryEnv {
    files: Mutex<BTreeMap<String, Vec<u8>>>,
    calls: Mutex<Vec<String>>,
}

impl MemoryEnv {
    fn seeded() -> Arc<Self> {
        let env = Arc::new(Self::default());
        env.files
            .lock()
            .expect("files")
            .insert("fixture.txt".into(), b"alpha\nbeta\n".to_vec());
        env
    }
}

impl ExecutionEnv for MemoryEnv {
    fn absolute_path<'a>(&'a self, path: &'a str) -> BoxFuture<'a, Result<String, EnvError>> {
        Box::pin(async move { Ok(path.to_owned()) })
    }
    fn exists<'a>(&'a self, path: &'a str) -> BoxFuture<'a, Result<bool, EnvError>> {
        Box::pin(async move { Ok(self.files.lock().expect("files").contains_key(path)) })
    }
    fn read_binary_file<'a>(&'a self, path: &'a str) -> BoxFuture<'a, Result<Vec<u8>, EnvError>> {
        Box::pin(async move {
            Ok(self
                .files
                .lock()
                .expect("files")
                .get(path)
                .cloned()
                .unwrap_or_default())
        })
    }
    fn process_image<'a>(
        &'a self,
        _: &'a [u8],
        _: &'a str,
        _: bool,
    ) -> BoxFuture<'a, Result<Option<ProcessedImage>, EnvError>> {
        Box::pin(async { Ok(None) })
    }
    fn read_text_file<'a>(&'a self, path: &'a str) -> BoxFuture<'a, Result<String, EnvError>> {
        Box::pin(async move {
            let bytes = self.read_binary_file(path).await?;
            Ok(String::from_utf8(bytes).expect("fixture UTF-8"))
        })
    }
    fn write_file<'a>(
        &'a self,
        path: &'a str,
        content: &'a [u8],
    ) -> BoxFuture<'a, Result<(), EnvError>> {
        Box::pin(async move {
            self.calls
                .lock()
                .expect("calls")
                .push(format!("write:{path}"));
            self.files
                .lock()
                .expect("files")
                .insert(path.to_owned(), content.to_vec());
            Ok(())
        })
    }
    fn list_dir<'a>(&'a self, _: &'a str) -> BoxFuture<'a, Result<Vec<FileInfo>, EnvError>> {
        Box::pin(async { Ok(Vec::new()) })
    }
    fn file_info<'a>(&'a self, path: &'a str) -> BoxFuture<'a, Result<FileInfo, EnvError>> {
        Box::pin(async move {
            Ok(FileInfo {
                name: path.to_owned(),
                path: path.to_owned(),
                is_dir: false,
            })
        })
    }
    fn canonical_path<'a>(&'a self, path: &'a str) -> BoxFuture<'a, Result<String, EnvError>> {
        Box::pin(async move { Ok(path.to_owned()) })
    }
    fn exec<'a>(
        &'a self,
        _: &'a str,
        command: &'a str,
        _: ProcessOptions,
    ) -> BoxFuture<'a, Result<ProcessOutput, EnvError>> {
        Box::pin(async move {
            self.calls
                .lock()
                .expect("calls")
                .push(format!("exec:{command}"));
            Ok(ProcessOutput {
                stdout: "deterministic shell\n".into(),
                stderr: String::new(),
                merged: None,
                exit_code: 0,
            })
        })
    }
    fn create_temp_file<'a>(
        &'a self,
        prefix: &'a str,
        content: &'a [u8],
    ) -> BoxFuture<'a, Result<String, EnvError>> {
        Box::pin(async move {
            let path = format!("{prefix}.tmp");
            self.files
                .lock()
                .expect("files")
                .insert(path.clone(), content.to_vec());
            Ok(path)
        })
    }
}

struct TraceListener {
    name: &'static str,
    events: Arc<Mutex<Vec<String>>>,
}
impl EventListener for TraceListener {
    fn on_event(&self, _: HarnessEvent) -> BoxFuture<'_, Result<(), HandlerError>> {
        let name = self.name.to_owned();
        let events = self.events.clone();
        Box::pin(async move {
            events.lock().expect("events").push(name);
            Ok(())
        })
    }
}

fn trace_type(event_type: HarnessEventType) -> &'static str {
    match event_type {
        HarnessEventType::RunStart => "run_start",
        HarnessEventType::RunResume => "run_resume",
        HarnessEventType::RunSuspend => "run_suspend",
        HarnessEventType::RunAbort => "run_abort",
        HarnessEventType::RunEnd => "run_end",
        HarnessEventType::Fault => "fault",
        HarnessEventType::HandlerError => "handler_error",
        HarnessEventType::TurnStart => "turn_start",
        HarnessEventType::TurnEnd => "turn_end",
        HarnessEventType::RetryScheduled => "retry_scheduled",
        HarnessEventType::RetryStart => "retry_start",
        HarnessEventType::RetryEnd => "retry_end",
        HarnessEventType::MessageStart => "message_start",
        HarnessEventType::MessageUpdate => "message_update",
        HarnessEventType::MessageEnd => "message_end",
        HarnessEventType::ToolStart => "tool_start",
        HarnessEventType::ToolUpdate => "tool_update",
        HarnessEventType::ToolEnd => "tool_end",
        HarnessEventType::EntryAdded => "entry_added",
        HarnessEventType::WritePending => "write_pending",
        HarnessEventType::QueueUpdate => "queue_update",
        HarnessEventType::FactUpdate => "fact_update",
        HarnessEventType::ConfigUpdate => "config_update",
        HarnessEventType::CompactionStart => "compaction_start",
        HarnessEventType::CompactionEnd => "compaction_end",
        HarnessEventType::NavigationStart => "navigation_start",
        HarnessEventType::NavigationEnd => "navigation_end",
        HarnessEventType::LaneCreated => "lane_created",
        HarnessEventType::Usage => "usage",
    }
}

fn event_types_for(case_id: &str) -> &'static [HarnessEventType] {
    match case_id {
        "tools.validate_progress_settle" | "tools.coding_operations" => &[
            HarnessEventType::RunStart,
            HarnessEventType::TurnStart,
            HarnessEventType::MessageStart,
            HarnessEventType::MessageEnd,
            HarnessEventType::ToolStart,
            HarnessEventType::ToolUpdate,
            HarnessEventType::ToolEnd,
            HarnessEventType::EntryAdded,
            HarnessEventType::WritePending,
            HarnessEventType::TurnEnd,
            HarnessEventType::RunEnd,
            HarnessEventType::Usage,
        ],
        "queue.steer_during_turn" | "queue.follow_up_after_turn" => &[
            HarnessEventType::RunStart,
            HarnessEventType::TurnStart,
            HarnessEventType::QueueUpdate,
            HarnessEventType::EntryAdded,
            HarnessEventType::MessageStart,
            HarnessEventType::MessageEnd,
            HarnessEventType::TurnEnd,
            HarnessEventType::RunEnd,
            HarnessEventType::Usage,
        ],
        "retry.overflow_precedence" | "retry.provider_recovery" => &[
            HarnessEventType::RunStart,
            HarnessEventType::TurnStart,
            HarnessEventType::RetryScheduled,
            HarnessEventType::RetryStart,
            HarnessEventType::RetryEnd,
            HarnessEventType::CompactionStart,
            HarnessEventType::CompactionEnd,
            HarnessEventType::MessageStart,
            HarnessEventType::MessageEnd,
            HarnessEventType::EntryAdded,
            HarnessEventType::TurnEnd,
            HarnessEventType::RunEnd,
            HarnessEventType::Usage,
        ],
        "abort.active_stream_cleanup" => &[
            HarnessEventType::RunStart,
            HarnessEventType::TurnStart,
            HarnessEventType::RunAbort,
            HarnessEventType::RunEnd,
            HarnessEventType::EntryAdded,
            HarnessEventType::WritePending,
        ],
        "compaction.context_projection" => &[
            HarnessEventType::RunStart,
            HarnessEventType::TurnStart,
            HarnessEventType::MessageStart,
            HarnessEventType::MessageEnd,
            HarnessEventType::EntryAdded,
            HarnessEventType::TurnEnd,
            HarnessEventType::RunEnd,
            HarnessEventType::Usage,
            HarnessEventType::CompactionStart,
            HarnessEventType::CompactionEnd,
        ],
        _ => &[],
    }
}

fn observe(
    harness: &Arc<
        pi_harness_runtime::owner::RuntimeHarness<
            ExecutionToolContext,
            MemoryStorage,
            pi_agent::schema::SessionMetadata,
        >,
    >,
    case_id: &str,
) -> (
    Arc<EventCount>,
    Arc<EventCount>,
    Arc<Mutex<Vec<String>>>,
    Vec<Arc<dyn pi_agent::schema::EventSubscription>>,
) {
    let publication_events = Arc::new(EventCount::default());
    let tool_invocations = Arc::new(EventCount::default());
    let events = Arc::new(Mutex::new(Vec::new()));
    let mut subscriptions = Vec::new();
    subscriptions.push(
        harness
            .events()
            .on(HarnessEventType::EntryAdded, publication_events.clone()),
    );
    subscriptions.push(
        harness
            .events()
            .on(HarnessEventType::ToolStart, tool_invocations.clone()),
    );
    for event_type in event_types_for(case_id) {
        subscriptions.push(harness.events().on(
            *event_type,
            Arc::new(TraceListener {
                name: trace_type(*event_type),
                events: events.clone(),
            }),
        ));
    }
    (publication_events, tool_invocations, events, subscriptions)
}

fn event_json(events: &Arc<Mutex<Vec<String>>>) -> Value {
    json!(
        events
            .lock()
            .expect("events")
            .iter()
            .enumerate()
            .map(|(index, ty)| json!({
                "index": index,
                "type": ty,
            }))
            .collect::<Vec<_>>()
    )
}

fn add_stats(observations: &mut Value, effects: &EffectSnapshot) {
    observations["session_stats_delta"] = json!({
        "message_count": effects.stats_message_count,
        "usage_total": effects.stats_usage_total,
    });
}

fn user(text: &str) -> AgentMessage {
    AgentMessage::BuiltIn(Message::User(UserMessage {
        role: Default::default(),
        content: pi_ai::UserContent::Text(text.to_owned()),
        timestamp: SafeUnsigned::new(7).expect("timestamp"),
    }))
}

fn settings_with(
    model: &Model,
    tools: Vec<ToolName>,
    retry: pi_ai::RetryPolicy,
    compaction: pi_coding_runtime::CompactionSettings,
) -> CodingSettingsSnapshot {
    CodingSettingsSnapshot::new(
        ModelRef::new(model.provider.clone(), model.id.clone()),
        ThinkingLevel::Off,
        tools,
        retry,
        compaction,
        pi_ai::StreamOptionsWire::default(),
        QueueMode::All,
        QueueMode::All,
    )
}

async fn build(
    fixture: &Fixture,
    settings: CodingSettingsSnapshot,
    env: Arc<dyn ExecutionEnv>,
) -> pi_coding_runtime::CodingHarness<MemoryStorage> {
    let composition = Arc::new(composition_for(&fixture.model, true));
    CodingHarnessBuilder::new(
        fixture.factory.clone(),
        fixture.session.clone(),
        fixture.registry.clone(),
        env,
        Resources::default(),
        settings,
    )
    .first_attachment()
    .with_provider_composition(composition)
    .with_message_converter(Arc::new(CodingMessageConverter::new().expect("converter")))
    .build()
    .await
    .expect("assembled M6-B harness")
}

fn terminal(outcome: &RunOutcome) -> bool {
    matches!(
        outcome,
        RunOutcome::CompletedWithAssistant(_)
            | RunOutcome::CompletedWithoutAssistant(_)
            | RunOutcome::AbortedWithAssistant(_)
            | RunOutcome::AbortedWithoutAssistant(_)
            | RunOutcome::FailedWithAssistant(_)
            | RunOutcome::FailedWithoutAssistant(_)
            | RunOutcome::Deferred(_)
            | RunOutcome::MissingIdentities(_)
    )
}

pub async fn actual_operation(case_id: &str) -> Value {
    match case_id {
        "tools.validate_progress_settle" => tools_validate().await,
        "tools.coding_operations" => tools_coding().await,
        "queue.steer_during_turn" => queue_operation(case_id, false).await,
        "queue.follow_up_after_turn" => queue_operation(case_id, true).await,
        "retry.overflow_precedence" => retry_overflow().await,
        "retry.provider_recovery" => retry_recovery().await,
        "abort.active_stream_cleanup" => abort_stream().await,
        "compaction.context_projection" => compaction_projection().await,
        _ => panic!("unknown M6-B row {case_id}"),
    }
}

async fn tools_validate() -> Value {
    let env = MemoryEnv::seeded();
    let fixture = super::fixture_with_environment(env.clone()).await;
    let tool_model = fixture.model.clone();
    *fixture.adapter.responses.lock().expect("responses") = VecDeque::from([
        tool_response(
            &tool_model,
            "read",
            "read-call",
            json!({"path":"fixture.txt"}),
        ),
        assistant(&tool_model, StopReason::Stop, "read settled"),
    ]);
    let harness = build(
        &fixture,
        settings_with(
            &fixture.model,
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
        ),
        env.clone(),
    )
    .await;
    let (publication, tools, events, _subs) =
        observe(harness.harness(), "tools.validate_progress_settle");
    let before = effect_snapshot(&fixture, &publication, &tools).await;
    let result = harness
        .harness()
        .prompt(PromptInput::Text {
            text: "read fixture".into(),
            images: Vec::new(),
        })
        .await;
    let run_result = result.expect("prompt outer").expect("prompt accepted");
    assert!(terminal(&run_result.outcome));
    let after = effect_snapshot(&fixture, &publication, &tools)
        .await
        .delta(before);
    let mut obs = json!({
        "provider_calls": after.provider_calls, "tool_invocations": after.tool_invocations,
        "session_publication_events": after.session_publication_events, "durable_entries": after.durable_entries,
        "event_count": events.lock().expect("events").len(), "event_types": events.lock().expect("events").clone(),
        "terminal_kind": "completed", "tool_progress": true, "tool_settled": true, "validation": "read",
        "environment_calls": env.calls.lock().expect("calls").clone(),
    });
    add_stats(&mut obs, &after);
    harness.harness().close().await.expect("close");
    envelope_with_events(
        case_id(),
        json!({"kind":"operation","operation":"validate_progress_settle","tool":"read"}),
        event_json(&events),
        json!({"kind":"completed","error":null,"terminal":true}),
        obs,
    )
}

fn case_id() -> &'static str {
    "tools.validate_progress_settle"
}

async fn tools_coding() -> Value {
    let env = MemoryEnv::seeded();
    let fixture = super::fixture_with_environment(env.clone()).await;
    let model = fixture.model.clone();
    *fixture.adapter.responses.lock().expect("responses") = VecDeque::from([
        tool_response(
            &model,
            "write",
            "write-call",
            json!({"path":"out.txt","content":"written"}),
        ),
        tool_response(
            &model,
            "bash",
            "bash-call",
            json!({"command":"printf coding"}),
        ),
        assistant(&model, StopReason::Stop, "tools settled"),
    ]);
    let tools = vec![
        ToolName::new("write").expect("write"),
        ToolName::new("bash").expect("bash"),
    ];
    let harness = build(
        &fixture,
        settings_with(
            &model,
            tools,
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
        ),
        env.clone(),
    )
    .await;
    let (publication, tool_count, events, _subs) =
        observe(harness.harness(), "tools.coding_operations");
    let before = effect_snapshot(&fixture, &publication, &tool_count).await;
    let result = harness
        .harness()
        .prompt(PromptInput::Text {
            text: "perform coding operations".into(),
            images: Vec::new(),
        })
        .await;
    let run_result = result.expect("prompt outer").expect("prompt accepted");
    assert!(terminal(&run_result.outcome));
    let after = effect_snapshot(&fixture, &publication, &tool_count)
        .await
        .delta(before);
    let mut obs = json!({
        "provider_calls":after.provider_calls,"tool_invocations":after.tool_invocations,
        "session_publication_events":after.session_publication_events,"durable_entries":after.durable_entries,
        "event_count":events.lock().expect("events").len(),"event_types":events.lock().expect("events").clone(),
        "terminal_kind":"completed","tool_names":["write","bash"],
        "environment_calls":env.calls.lock().expect("calls").clone(),
        "file_written":String::from_utf8(env.files.lock().expect("files").get("out.txt").cloned().expect("out")).expect("UTF8"),
        "shell_output":"deterministic shell\n",
    });
    add_stats(&mut obs, &after);
    harness.harness().close().await.expect("close");
    envelope_with_events(
        "tools.coding_operations",
        json!({"kind":"operation","operation":"coding_operations","tools":["write","bash"]}),
        event_json(&events),
        json!({"kind":"completed","error":null,"terminal":true}),
        obs,
    )
}

async fn queue_operation(case_id: &str, follow_up: bool) -> Value {
    let fixture = super::fixture().await;
    let hold = pi_ai::CancellationToken::new();
    *fixture.adapter.hold.lock().expect("hold") = Some(hold.clone());
    let model = fixture.model.clone();
    *fixture.adapter.responses.lock().expect("responses") = VecDeque::from([
        assistant(&model, StopReason::Stop, "held turn"),
        assistant(&model, StopReason::Stop, "queued turn"),
    ]);
    let harness = build(
        &fixture,
        settings_with(
            &model,
            vec![ToolName::new("read").expect("read")],
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
        ),
        fixture.environment.clone(),
    )
    .await;
    let (publication, tools, events, _subs) = observe(harness.harness(), case_id);
    let lane = harness.harness().clone();
    let first = tokio::spawn(async move {
        lane.prompt(PromptInput::Text {
            text: "active turn".into(),
            images: Vec::new(),
        })
        .await
    });
    for _ in 0..100_000 {
        if fixture.adapter.calls.load(Ordering::SeqCst) > 0 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(
        fixture.adapter.calls.load(Ordering::SeqCst) > 0,
        "queue operation must reach provider"
    );
    let queued = if follow_up {
        harness
            .harness()
            .follow_up(user("follow-up after active turn"))
            .await
            .expect("follow outer")
            .expect("follow accepted")
    } else {
        harness
            .harness()
            .steer(user("steer during active turn"))
            .await
            .expect("steer outer")
            .expect("steer accepted")
    };
    hold.cancel();
    let first_result = first
        .await
        .expect("first join")
        .expect("first outer")
        .expect("first accepted");
    assert!(terminal(&first_result.outcome));
    harness.harness().wait_for_idle().await.expect("idle");
    let after = effect_snapshot(&fixture, &publication, &tools).await;
    let mut obs = json!({
        "provider_calls":after.provider_calls,"tool_invocations":after.tool_invocations,
        "session_publication_events":after.session_publication_events,"durable_entries":after.durable_entries,
        "event_count":events.lock().expect("events").len(),"event_types":events.lock().expect("events").clone(),
        "terminal_kind":"completed","queue_entry_id":queued.entry_id,
        "queue_order":if follow_up {"follow_up"} else {"steer"},"idle_after":true,
    });
    add_stats(&mut obs, &after);
    harness.harness().close().await.expect("close");
    envelope_with_events(
        case_id,
        json!({"kind":"operation","operation":if follow_up {"follow_up_after_turn"} else {"steer_during_turn"},"queue":if follow_up {"follow_up"} else {"steer"}}),
        event_json(&events),
        json!({"kind":"completed","error":null,"terminal":true}),
        obs,
    )
}

async fn retry_recovery() -> Value {
    let fixture = super::fixture().await;
    let model = fixture.model.clone();
    *fixture.adapter.responses.lock().expect("responses") = VecDeque::from([
        assistant(&model, StopReason::Error, "503 service unavailable"),
        assistant(&model, StopReason::Stop, "recovered"),
    ]);
    let retry = pi_ai::RetryPolicy {
        enabled: true,
        max_retries: pi_ai::MaxRetries::new(1).expect("retry"),
        base_delay_ms: SafeUnsigned::new(0).expect("delay"),
    };
    let harness = build(
        &fixture,
        settings_with(
            &model,
            vec![],
            retry,
            pi_coding_runtime::CompactionSettings {
                enabled: false,
                reserve_tokens: SafeUnsigned::new(0).expect("reserve"),
                keep_recent_tokens: SafeUnsigned::new(0).expect("tail"),
            },
        ),
        fixture.environment.clone(),
    )
    .await;
    let (publication, tools, events, _subs) = observe(harness.harness(), "retry.provider_recovery");
    let before = effect_snapshot(&fixture, &publication, &tools).await;
    let result = harness
        .harness()
        .prompt(PromptInput::Text {
            text: "recover".into(),
            images: Vec::new(),
        })
        .await;
    let run_result = result.expect("outer").expect("accepted");
    assert!(terminal(&run_result.outcome));
    let after = effect_snapshot(&fixture, &publication, &tools)
        .await
        .delta(before);
    let mut obs = json!({"provider_calls":after.provider_calls,"tool_invocations":after.tool_invocations,"session_publication_events":after.session_publication_events,"durable_entries":after.durable_entries,"event_count":events.lock().expect("events").len(),"event_types":events.lock().expect("events").clone(),"terminal_kind":"completed","retry_attempts":1,"provider_recovery":true,"request_count":fixture.adapter.calls.load(Ordering::SeqCst)});
    add_stats(&mut obs, &after);
    harness.harness().close().await.expect("close");
    envelope_with_events(
        "retry.provider_recovery",
        json!({"kind":"operation","operation":"provider_recovery","max_retries":1}),
        event_json(&events),
        json!({"kind":"completed","error":null,"terminal":true}),
        obs,
    )
}

async fn retry_overflow() -> Value {
    let fixture = super::fixture().await;
    let model = fixture.model.clone();
    *fixture.adapter.responses.lock().expect("responses") = VecDeque::from([
        assistant(
            &model,
            StopReason::Error,
            "request exceeds the context window",
        ),
        assistant(&model, StopReason::Stop, "compaction summary"),
        assistant(&model, StopReason::Stop, "overflow recovered"),
    ]);
    let retry = pi_ai::RetryPolicy {
        enabled: true,
        max_retries: pi_ai::MaxRetries::new(1).expect("retry"),
        base_delay_ms: SafeUnsigned::new(0).expect("delay"),
    };
    let compact = pi_coding_runtime::CompactionSettings {
        enabled: true,
        reserve_tokens: SafeUnsigned::new(0).expect("reserve"),
        keep_recent_tokens: SafeUnsigned::new(0).expect("tail"),
    };
    let harness = build(
        &fixture,
        settings_with(&model, vec![], retry, compact),
        fixture.environment.clone(),
    )
    .await;
    let (publication, tools, events, _subs) =
        observe(harness.harness(), "retry.overflow_precedence");
    let before = effect_snapshot(&fixture, &publication, &tools).await;
    let result = harness
        .harness()
        .prompt(PromptInput::Text {
            text: "overflow".into(),
            images: Vec::new(),
        })
        .await;
    let run_result = result.expect("outer").expect("accepted");
    assert!(terminal(&run_result.outcome));
    let after = effect_snapshot(&fixture, &publication, &tools)
        .await
        .delta(before);
    let mut obs = json!({"provider_calls":after.provider_calls,"tool_invocations":after.tool_invocations,"session_publication_events":after.session_publication_events,"durable_entries":after.durable_entries,"event_count":events.lock().expect("events").len(),"event_types":events.lock().expect("events").clone(),"terminal_kind":"completed","overflow_classification":"context_overflow","retry_suppressed":true,"compaction_triggered":true,"request_count":fixture.adapter.calls.load(Ordering::SeqCst)});
    add_stats(&mut obs, &after);
    harness.harness().close().await.expect("close");
    envelope_with_events(
        "retry.overflow_precedence",
        json!({"kind":"operation","operation":"overflow_precedence","retry_enabled":true}),
        event_json(&events),
        json!({"kind":"completed","error":null,"terminal":true}),
        obs,
    )
}

async fn abort_stream() -> Value {
    let fixture = super::fixture().await;
    let hold = pi_ai::CancellationToken::new();
    *fixture.adapter.hold.lock().expect("hold") = Some(hold);
    let harness = build(
        &fixture,
        settings(&fixture.model),
        fixture.environment.clone(),
    )
    .await;
    let (publication, tools, events, _subs) =
        observe(harness.harness(), "abort.active_stream_cleanup");
    let lane = harness.harness().clone();
    let run = tokio::spawn(async move {
        lane.prompt(PromptInput::Text {
            text: "abort active".into(),
            images: Vec::new(),
        })
        .await
    });
    for _ in 0..100_000 {
        if fixture.adapter.calls.load(Ordering::SeqCst) > 0 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(
        fixture.adapter.calls.load(Ordering::SeqCst) > 0,
        "abort operation must reach provider"
    );
    let abort = harness
        .harness()
        .abort()
        .await
        .expect("abort outer")
        .expect("abort accepted");
    let result = run.await.expect("join").expect("outer").expect("accepted");
    harness.harness().wait_for_idle().await.expect("idle");
    let after = effect_snapshot(&fixture, &publication, &tools).await;
    let mut obs = json!({"provider_calls":after.provider_calls,"tool_invocations":after.tool_invocations,"session_publication_events":after.session_publication_events,"durable_entries":after.durable_entries,"event_count":events.lock().expect("events").len(),"event_types":events.lock().expect("events").clone(),"terminal_kind":"aborted","abort_outcome":format!("{abort:?}"),"post_abort_idle":true,"active_streams":fixture.adapter.active_streams.load(Ordering::SeqCst)});
    add_stats(&mut obs, &after);
    assert!(terminal(&result.outcome));
    assert_eq!(fixture.adapter.active_streams.load(Ordering::SeqCst), 0);
    harness.harness().close().await.expect("close");
    envelope_with_events(
        "abort.active_stream_cleanup",
        json!({"kind":"operation","operation":"abort_active_stream"}),
        event_json(&events),
        json!({"kind":"aborted","error":null,"terminal":true}),
        obs,
    )
}

async fn compaction_projection() -> Value {
    let fixture = super::fixture().await;
    let model = fixture.model.clone();
    *fixture.adapter.responses.lock().expect("responses") = VecDeque::from([
        assistant(&model, StopReason::Stop, "turn one"),
        assistant(&model, StopReason::Stop, "turn two"),
        assistant(&model, StopReason::Stop, "summary"),
    ]);
    let compact = pi_coding_runtime::CompactionSettings {
        enabled: true,
        reserve_tokens: SafeUnsigned::new(0).expect("reserve"),
        keep_recent_tokens: SafeUnsigned::new(0).expect("tail"),
    };
    let harness = build(
        &fixture,
        settings_with(
            &model,
            vec![],
            pi_ai::RetryPolicy {
                enabled: false,
                max_retries: pi_ai::MaxRetries::new(0).expect("retry"),
                base_delay_ms: SafeUnsigned::new(0).expect("delay"),
            },
            compact,
        ),
        fixture.environment.clone(),
    )
    .await;
    let (publication, tools, events, _subs) =
        observe(harness.harness(), "compaction.context_projection");
    let before = effect_snapshot(&fixture, &publication, &tools).await;
    for text in ["history one", "history two"] {
        let result = harness
            .harness()
            .prompt(PromptInput::Text {
                text: text.into(),
                images: Vec::new(),
            })
            .await;
        let run_result = result.expect("outer").expect("accepted");
        assert!(terminal(&run_result.outcome));
    }
    let before_compact = fixture
        .session
        .find_entries(EntryQuery::default())
        .await
        .expect("entries")
        .len();
    let compacted = harness
        .harness()
        .compact(pi_agent::schema::CompactOptions {
            custom_instructions: Some("project context".into()),
        })
        .await
        .expect("compact outer")
        .expect("compact accepted");
    assert!(matches!(
        compacted.outcome,
        pi_agent::schema::CompactionOutcome::Completed(_)
    ));
    harness.harness().wait_for_idle().await.expect("idle");
    let after = effect_snapshot(&fixture, &publication, &tools)
        .await
        .delta(before);
    let after_entries = fixture
        .session
        .find_entries(EntryQuery::default())
        .await
        .expect("entries")
        .len();
    let contexts = fixture.adapter.contexts.lock().expect("contexts");
    let mut obs = json!({"provider_calls":after.provider_calls,"tool_invocations":after.tool_invocations,"session_publication_events":after.session_publication_events,"durable_entries":after.durable_entries,"event_count":events.lock().expect("events").len(),"event_types":events.lock().expect("events").clone(),"terminal_kind":"completed","context_projection":{"before_messages":contexts.first().map(|c|c.messages.len()).unwrap_or(0),"summary_messages":contexts.last().map(|c|c.messages.len()).unwrap_or(0)},"session_entries_before":before_compact,"session_entries_after":after_entries,"compaction_published":after_entries > before_compact});
    add_stats(&mut obs, &after);
    harness.harness().close().await.expect("close");
    envelope_with_events(
        "compaction.context_projection",
        json!({"kind":"operation","operation":"context_projection","custom_instructions":"project context"}),
        event_json(&events),
        json!({"kind":"completed","error":null,"terminal":true}),
        obs,
    )
}
