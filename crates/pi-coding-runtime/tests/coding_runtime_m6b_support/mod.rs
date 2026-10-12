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
    ProcessOutput,
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
    calls: Mutex<Vec<Value>>,
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
    fn record(&self, method: &str, args: Value, result: Value, error: Option<&str>) {
        self.calls
            .lock()
            .expect("calls")
            .push(json!({"method":method,"args":args,"result":result,"error":error}));
    }
    fn read(&self, method: &str, path: &str) -> Result<Vec<u8>, EnvError> {
        let value = self.files.lock().expect("files").get(path).cloned();
        match value {
            Some(bytes) => {
                self.record(
                    method,
                    json!({"path":path}),
                    json!(String::from_utf8(bytes.clone()).expect("fixture UTF8")),
                    None,
                );
                Ok(bytes)
            }
            None => {
                self.record(
                    method,
                    json!({"path":path}),
                    Value::Null,
                    Some("file missing"),
                );
                Err(EnvError {
                    message: "file missing".into(),
                })
            }
        }
    }
}
fn file_json(file: &FileInfo) -> Value {
    json!({"name":file.name,"path":file.path,"is_dir":file.is_dir})
}
impl ExecutionEnv for MemoryEnv {
    fn absolute_path<'a>(&'a self, path: &'a str) -> BoxFuture<'a, Result<String, EnvError>> {
        Box::pin(async move {
            self.record("absolute_path", json!({"path":path}), json!(path), None);
            Ok(path.to_owned())
        })
    }
    fn exists<'a>(&'a self, path: &'a str) -> BoxFuture<'a, Result<bool, EnvError>> {
        Box::pin(async move {
            let exists = self.files.lock().expect("files").contains_key(path);
            self.record("exists", json!({"path":path}), json!(exists), None);
            Ok(exists)
        })
    }
    fn read_binary_file<'a>(&'a self, path: &'a str) -> BoxFuture<'a, Result<Vec<u8>, EnvError>> {
        Box::pin(async move { self.read("read_binary_file", path) })
    }
    fn read_text_file<'a>(&'a self, path: &'a str) -> BoxFuture<'a, Result<String, EnvError>> {
        Box::pin(async move {
            self.read("read_text_file", path)
                .map(|bytes| String::from_utf8(bytes).expect("fixture UTF8"))
        })
    }
    fn write_file<'a>(
        &'a self,
        path: &'a str,
        content: &'a [u8],
    ) -> BoxFuture<'a, Result<(), EnvError>> {
        Box::pin(async move {
            self.files
                .lock()
                .expect("files")
                .insert(path.to_owned(), content.to_vec());
            self.record("write_file", json!({"path":path,"content":String::from_utf8(content.to_vec()).expect("fixture UTF8")}), Value::Null, None);
            Ok(())
        })
    }
    fn list_dir<'a>(&'a self, path: &'a str) -> BoxFuture<'a, Result<Vec<FileInfo>, EnvError>> {
        Box::pin(async move {
            let files = self.files.lock().expect("files");
            let result = files
                .keys()
                .map(|name| FileInfo {
                    name: name.clone(),
                    path: name.clone(),
                    is_dir: false,
                })
                .collect::<Vec<_>>();
            self.record(
                "list_dir",
                json!({"path":path}),
                json!(result.iter().map(file_json).collect::<Vec<_>>()),
                None,
            );
            Ok(result)
        })
    }
    fn file_info<'a>(&'a self, path: &'a str) -> BoxFuture<'a, Result<FileInfo, EnvError>> {
        Box::pin(async move {
            let result = FileInfo {
                name: path.to_owned(),
                path: path.to_owned(),
                is_dir: path == ".",
            };
            self.record("file_info", json!({"path":path}), file_json(&result), None);
            Ok(result)
        })
    }
    fn canonical_path<'a>(&'a self, path: &'a str) -> BoxFuture<'a, Result<String, EnvError>> {
        Box::pin(async move {
            self.record("canonical_path", json!({"path":path}), json!(path), None);
            Ok(path.to_owned())
        })
    }
    fn exec<'a>(
        &'a self,
        shell: &'a str,
        command: &'a str,
        options: ProcessOptions,
    ) -> BoxFuture<'a, Result<ProcessOutput, EnvError>> {
        Box::pin(async move {
            let output = ProcessOutput {
                stdout: "deterministic shell\n".into(),
                stderr: String::new(),
                merged: None,
                exit_code: 0,
            };
            self.record("exec",json!({"shell":shell,"command":command,"cwd":options.cwd,"timeout_ms":options.timeout.map(|t|t.as_millis() as u64),"env":options.env,"abort_signal":options.abort_signal.as_ref().map(|a|a.load(Ordering::SeqCst))}),json!({"stdout":output.stdout,"stderr":output.stderr,"merged":output.merged,"exit_code":output.exit_code}),None);
            Ok(output)
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
            self.record("create_temp_file",json!({"prefix":prefix,"content":String::from_utf8(content.to_vec()).expect("fixture UTF8")}),json!(path),None);
            Ok(path)
        })
    }
}

struct TraceListener {
    events: Arc<Mutex<Vec<HarnessEvent>>>,
}
impl EventListener for TraceListener {
    fn on_event(&self, event: HarnessEvent) -> BoxFuture<'_, Result<(), HandlerError>> {
        // Keep the concrete payload. Type-only order is supplemental evidence.
        self.events.lock().expect("events").push(event);
        Box::pin(async { Ok(()) })
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
    Arc<Mutex<Vec<HarnessEvent>>>,
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
                events: events.clone(),
            }),
        ));
    }
    (publication_events, tool_invocations, events, subscriptions)
}

fn message_json(message: &AgentMessage) -> Value {
    let value = serde_json::to_value(message).expect("message JSON");
    let mut result = json!({"role":value["role"],"content":value["content"]});
    for (source, target) in [
        ("stopReason", "stop_reason"),
        ("errorMessage", "error_message"),
        ("toolCallId", "tool_call_id"),
        ("toolName", "tool_name"),
        ("isError", "is_error"),
    ] {
        if let Some(value) = value.get(source) {
            result[target] = value.clone();
        }
    }
    result
}
fn queued_json(items: &[pi_agent::schema::QueuedItem]) -> Value {
    json!(
        items
            .iter()
            .map(|item| json!({"entry_id":item.entry_id,"message":message_json(&item.message)}))
            .collect::<Vec<_>>()
    )
}
fn compaction_json(entry: &pi_agent::schema::CompactionEntry) -> Value {
    json!({"id":entry.id,"parent_id":entry.parent_id,"seq":entry.seq,"timestamp":entry.timestamp,"summary":entry.summary,"retained_tail":entry.retained_tail.iter().map(message_json).collect::<Vec<_>>(),"tokens_before":entry.tokens_before,"usage":entry.usage,"from_hook":entry.from_hook})
}
fn payload_json(event: &HarnessEvent) -> Option<Value> {
    Some(match event {
        HarnessEvent::ToolStart(e) => {
            json!({"tool_call_id":e.tool_call_id,"tool_name":e.tool_name,"args":e.args})
        }
        HarnessEvent::ToolUpdate(e) => {
            json!({"tool_call_id":e.tool_call_id,"tool_name":e.tool_name,"partial_result":e.partial_result})
        }
        HarnessEvent::ToolEnd(e) => {
            json!({"tool_call_id":e.tool_call_id,"tool_name":e.tool_name,"result":e.result,"is_error":e.is_error,"terminate":e.terminate})
        }
        HarnessEvent::QueueUpdate(e) => {
            json!({"steer":queued_json(&e.steer),"follow_up":queued_json(&e.follow_up),"next_run":queued_json(&e.next_run)})
        }
        HarnessEvent::RetryScheduled(e) => {
            json!({"step":e.step,"attempt":e.attempt,"max_attempts":e.max_attempts,"delay_ms":e.delay_ms,"error_message":e.error_message})
        }
        HarnessEvent::MessageEnd(e) => {
            json!({"entry_id":e.entry_id,"message":message_json(&e.message)})
        }
        HarnessEvent::CompactionStart(e) => json!({"reason":e.reason}),
        HarnessEvent::CompactionEndCompleted(e) => {
            json!({"reason":e.reason,"outcome":e.outcome,"entry":compaction_json(&e.entry),"from_hook":e.from_hook})
        }
        HarnessEvent::RunAbort(e) => {
            json!({"run_id":e.run_id,"steer":e.steer.iter().map(message_json).collect::<Vec<_>>(),"follow_up":e.follow_up.iter().map(message_json).collect::<Vec<_>>()})
        }
        HarnessEvent::RunEndCompletedWithFinal(e) => {
            json!({"run_id":e.run_id,"outcome":e.outcome,"leaf_id":e.leaf_id})
        }
        HarnessEvent::RunEndCompletedWithoutFinal(e) => {
            json!({"run_id":e.run_id,"outcome":e.outcome,"leaf_id":e.leaf_id})
        }
        HarnessEvent::RunEndAbortedWithFinal(e) => {
            json!({"run_id":e.run_id,"outcome":e.outcome,"leaf_id":e.leaf_id})
        }
        HarnessEvent::RunEndAbortedWithoutFinal(e) => {
            json!({"run_id":e.run_id,"outcome":e.outcome,"leaf_id":e.leaf_id})
        }
        _ => return None,
    })
}
fn event_json(events: &Arc<Mutex<Vec<HarnessEvent>>>) -> Value {
    json!(
        events
            .lock()
            .expect("events")
            .iter()
            .enumerate()
            .map(|(index, event)| {
                let value = serde_json::to_value(event).expect("event JSON");
                let mut result = json!({"index":index,"type":value["type"]});
                if let Some(payload) = payload_json(event) {
                    result["payload"] = payload;
                }
                result
            })
            .collect::<Vec<_>>()
    )
}
fn event_names(events: &Arc<Mutex<Vec<HarnessEvent>>>) -> Vec<String> {
    events
        .lock()
        .expect("events")
        .iter()
        .map(|event| {
            serde_json::to_value(event).expect("event JSON")["type"]
                .as_str()
                .expect("event type")
                .to_owned()
        })
        .collect()
}
fn event_payloads(events: &Arc<Mutex<Vec<HarnessEvent>>>, kind: &str) -> Vec<Value> {
    event_json(events)
        .as_array()
        .expect("events array")
        .iter()
        .filter(|e| e["type"] == kind)
        .map(|e| e["payload"].clone())
        .collect()
}
fn terminal_kind(outcome: &RunOutcome) -> &'static str {
    match outcome {
        RunOutcome::CompletedWithAssistant(_) | RunOutcome::CompletedWithoutAssistant(_) => {
            "completed"
        }
        RunOutcome::AbortedWithAssistant(_) | RunOutcome::AbortedWithoutAssistant(_) => "aborted",
        RunOutcome::FailedWithAssistant(_) | RunOutcome::FailedWithoutAssistant(_) => "failed",
        _ => "suspended",
    }
}
fn base_obs(after: &EffectSnapshot, events: &Arc<Mutex<Vec<HarnessEvent>>>, kind: &str) -> Value {
    let mut result = json!({"provider_calls":after.provider_calls,"tool_invocations":after.tool_invocations,"session_publication_events":after.session_publication_events,"durable_entries":after.durable_entries,"event_count":events.lock().expect("events").len(),"event_types":event_names(events),"terminal_kind":kind});
    add_stats(&mut result, after);
    result
}
fn provider_tool_calls(fixture: &Fixture) -> Vec<Value> {
    fixture
        .adapter
        .emitted_responses
        .lock()
        .expect("emitted responses")
        .iter()
        .flat_map(|message| message.content.iter())
        .filter_map(|content| match content {
            AssistantContent::ToolCall(call) => {
                Some(json!({"tool_call_id":call.id,"tool_name":call.name,"args":call.arguments}))
            }
            _ => None,
        })
        .collect()
}
fn context_json(context: &pi_ai::Context) -> Value {
    json!({"system_prompt":context.system_prompt,"messages":context.messages.iter().map(|message|message_json(&AgentMessage::BuiltIn(message.clone()))).collect::<Vec<_>>(),"tools":context.tools.as_ref().map(|tools|tools.iter().map(|tool|tool.name.to_string()).collect::<Vec<_>>()).unwrap_or_default()})
}
async fn lane_json(harness: &impl AgentLane) -> Value {
    let watch = harness.watch().await.expect("lane watch");
    let snapshot = watch.snapshot();
    watch.unsubscribe();
    json!({"operation":snapshot.operation.as_ref().map(|op|json!({"id":op.id,"kind":op.kind,"status":op.status,"running_tools":op.running_tools.len()})),"steer":queued_json(&snapshot.queues.steer),"follow_up":queued_json(&snapshot.queues.follow_up),"next_run":queued_json(&snapshot.queues.next_run),"pending_writes":snapshot.pending_writes.len(),"faulted":snapshot.faulted,"leaf_id":snapshot.leaf_id})
}
async fn cleanup_json(fixture: &Fixture, harness: &impl AgentLane) -> Value {
    use pi_agent::schema::Session;
    let lane = lane_json(harness).await;
    let mut registers = serde_json::Map::new();
    for namespace in [
        pi_agent::schema::RegisterNamespace::OperationMeta,
        pi_agent::schema::RegisterNamespace::OperationState,
        pi_agent::schema::RegisterNamespace::OperationToolArguments,
        pi_agent::schema::RegisterNamespace::OperationPreparation,
        pi_agent::schema::RegisterNamespace::PendingEntry,
    ] {
        let name = serde_json::to_value(namespace)
            .expect("namespace")
            .as_str()
            .expect("namespace string")
            .to_owned();
        let rows = fixture
            .session
            .list_registers(namespace, None)
            .await
            .expect("register rows");
        assert!(rows.is_empty(), "cleanup must clear {name}");
        registers.insert(name, json!(rows.len()));
    }
    assert!(lane["operation"].is_null());
    for queue in ["steer", "follow_up", "next_run"] {
        assert!(lane[queue].as_array().expect("queue").is_empty());
    }
    assert_eq!(lane["pending_writes"], 0);
    json!({"lane":lane,"register_counts":registers})
}
async fn compactions(fixture: &Fixture, events: &Arc<Mutex<Vec<HarnessEvent>>>) -> Vec<Value> {
    let entries = fixture
        .session
        .find_entries(EntryQuery::default())
        .await
        .expect("entries");
    let projected = entries
        .iter()
        .filter_map(|entry| match entry {
            pi_agent::schema::Entry::Compaction(c) => Some(compaction_json(c)),
            _ => None,
        })
        .collect::<Vec<_>>();
    let emitted = event_payloads(events, "compaction_end");
    assert_eq!(
        projected,
        emitted
            .iter()
            .map(|event| event["entry"].clone())
            .collect::<Vec<_>>(),
        "event and durable compaction entry must match"
    );
    projected
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

fn no_retry() -> pi_ai::RetryPolicy {
    pi_ai::RetryPolicy {
        enabled: false,
        max_retries: pi_ai::MaxRetries::new(0).expect("retries"),
        base_delay_ms: SafeUnsigned::new(0).expect("delay"),
    }
}
fn compaction_settings(enabled: bool) -> pi_coding_runtime::CompactionSettings {
    pi_coding_runtime::CompactionSettings {
        enabled,
        reserve_tokens: SafeUnsigned::new(0).expect("reserve"),
        keep_recent_tokens: SafeUnsigned::new(0).expect("tail"),
    }
}
async fn prompt(harness: &impl AgentLane, text: &str) -> RunOutcome {
    let result = harness
        .prompt(PromptInput::Text {
            text: text.into(),
            images: Vec::new(),
        })
        .await
        .expect("prompt outer")
        .expect("prompt accepted");
    harness.wait_for_idle().await.expect("idle");
    result.outcome
}
fn tool_evidence(fixture: &Fixture, events: &Arc<Mutex<Vec<HarnessEvent>>>) -> Value {
    let calls = provider_tool_calls(fixture);
    let starts = event_payloads(events, "tool_start");
    let updates = event_payloads(events, "tool_update");
    let ends = event_payloads(events, "tool_end");
    assert_eq!(
        calls, starts,
        "provider calls must reach real ToolStart with their arguments"
    );
    assert_eq!(calls.len(), ends.len());
    for (call, end) in calls.iter().zip(&ends) {
        assert_eq!(call["tool_call_id"], end["tool_call_id"]);
        assert_eq!(call["tool_name"], end["tool_name"]);
        assert_eq!(end["is_error"], false);
        assert_eq!(end["terminate"], false);
        assert!(
            end["result"]["content"]
                .as_array()
                .is_some_and(|content| !content.is_empty())
        );
    }
    json!({"provider_calls":calls,"starts":starts,"updates":updates,"ends":ends})
}
async fn tools_validate() -> Value {
    let case_id = "tools.validate_progress_settle";
    let env = MemoryEnv::seeded();
    let fixture = super::fixture_with_environment(env.clone()).await;
    *fixture.adapter.responses.lock().expect("responses") = VecDeque::from([
        tool_response(
            &fixture.model,
            "read",
            "read-call",
            json!({"path":"fixture.txt"}),
        ),
        assistant(&fixture.model, StopReason::Stop, "read settled"),
    ]);
    let harness = build(
        &fixture,
        settings_with(
            &fixture.model,
            vec![ToolName::new("read").expect("read")],
            no_retry(),
            compaction_settings(false),
        ),
        env.clone(),
    )
    .await;
    let (publication, tools, events, _subs) = observe(harness.harness(), case_id);
    let before = effect_snapshot(&fixture, &publication, &tools).await;
    let outcome = prompt(harness.harness().as_ref(), "read fixture").await;
    assert_eq!(terminal_kind(&outcome), "completed");
    let after = effect_snapshot(&fixture, &publication, &tools)
        .await
        .delta(before);
    let evidence = tool_evidence(&fixture, &events);
    assert_eq!(evidence["updates"].as_array().expect("updates").len(), 1);
    let mut obs = base_obs(&after, &events, terminal_kind(&outcome));
    obs["tool_evidence"] = evidence;
    obs["environment_calls"] = json!(env.calls.lock().expect("calls").clone());
    harness.harness().close().await.expect("close");
    envelope_with_events(
        case_id,
        json!({"kind":"operation","operation":"validate_progress_settle","tool":"read"}),
        event_json(&events),
        json!({"kind":terminal_kind(&outcome),"error":null,"terminal":terminal(&outcome)}),
        obs,
    )
}
async fn tools_coding() -> Value {
    let case_id = "tools.coding_operations";
    let env = MemoryEnv::seeded();
    let fixture = super::fixture_with_environment(env.clone()).await;
    let requests = [
        ("read", json!({"path":"fixture.txt"})),
        ("write", json!({"path":"out.txt","content":"written\n"})),
        (
            "edit",
            json!({"path":"out.txt","edits":[{"oldText":"written","newText":"revised"}]}),
        ),
        (
            "grep",
            json!({"path":".","pattern":"revised","literal":true}),
        ),
        ("find", json!({"path":".","pattern":"*.txt"})),
        ("ls", json!({"path":"."})),
        ("bash", json!({"command":"printf coding"})),
    ];
    let responses = requests
        .iter()
        .map(|(name, args)| {
            tool_response(&fixture.model, name, &format!("{name}-call"), args.clone())
        })
        .chain([assistant(&fixture.model, StopReason::Stop, "tools settled")])
        .collect();
    *fixture.adapter.responses.lock().expect("responses") = responses;
    let active_tools = requests
        .iter()
        .map(|(name, _)| ToolName::new(*name).expect("tool"))
        .collect();
    let harness = build(
        &fixture,
        settings_with(
            &fixture.model,
            active_tools,
            no_retry(),
            compaction_settings(false),
        ),
        env.clone(),
    )
    .await;
    let (publication, tools, events, _subs) = observe(harness.harness(), case_id);
    let before = effect_snapshot(&fixture, &publication, &tools).await;
    let outcome = prompt(harness.harness().as_ref(), "perform coding operations").await;
    assert_eq!(terminal_kind(&outcome), "completed");
    let after = effect_snapshot(&fixture, &publication, &tools)
        .await
        .delta(before);
    let evidence = tool_evidence(&fixture, &events);
    let names = evidence["starts"]
        .as_array()
        .expect("starts")
        .iter()
        .map(|e| e["tool_name"].clone())
        .collect::<Vec<_>>();
    assert_eq!(
        names,
        json!(["read", "write", "edit", "grep", "find", "ls", "bash"])
            .as_array()
            .expect("names")
            .clone()
    );
    let mut obs = base_obs(&after, &events, terminal_kind(&outcome));
    obs["tool_names"] = json!(names);
    obs["tool_evidence"] = evidence;
    obs["environment_calls"] = json!(env.calls.lock().expect("calls").clone());
    obs["files_after"] = json!(
        env.files
            .lock()
            .expect("files")
            .iter()
            .map(|(path, bytes)| (
                path.clone(),
                String::from_utf8(bytes.clone()).expect("UTF8")
            ))
            .collect::<BTreeMap<_, _>>()
    );
    harness.harness().close().await.expect("close");
    envelope_with_events(
        case_id,
        json!({"kind":"operation","operation":"coding_operations","tools":names}),
        event_json(&events),
        json!({"kind":terminal_kind(&outcome),"error":null,"terminal":terminal(&outcome)}),
        obs,
    )
}
async fn await_active(fixture: &Fixture) {
    for _ in 0..100_000 {
        if fixture.adapter.active_streams.load(Ordering::SeqCst) > 0
            && fixture
                .adapter
                .lifecycle
                .lock()
                .expect("lifecycle")
                .iter()
                .any(|e| e == "payload:1")
        {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("held operation did not reach active provider stream");
}
async fn queue_operation(case_id: &str, follow_up: bool) -> Value {
    let fixture = super::fixture().await;
    let hold = pi_ai::CancellationToken::new();
    *fixture.adapter.hold.lock().expect("hold") = Some(hold.clone());
    *fixture.adapter.responses.lock().expect("responses") = VecDeque::from([
        assistant(&fixture.model, StopReason::Stop, "held turn"),
        assistant(&fixture.model, StopReason::Stop, "queued turn"),
    ]);
    let harness = build(
        &fixture,
        settings_with(
            &fixture.model,
            vec![],
            no_retry(),
            compaction_settings(false),
        ),
        fixture.environment.clone(),
    )
    .await;
    let (publication, tools, events, _subs) = observe(harness.harness(), case_id);
    let before = effect_snapshot(&fixture, &publication, &tools).await;
    let lane = harness.harness().clone();
    let first = tokio::spawn(async move {
        lane.prompt(PromptInput::Text {
            text: "active turn".into(),
            images: Vec::new(),
        })
        .await
    });
    await_active(&fixture).await;
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
    let snapshot = lane_json(harness.harness().as_ref()).await;
    assert!(snapshot["operation"].is_object());
    let placement = ["steer", "follow_up", "next_run"]
        .into_iter()
        .find(|key| {
            snapshot[*key]
                .as_array()
                .expect("queue")
                .iter()
                .any(|item| item["entry_id"] == json!(queued.entry_id))
        })
        .expect("queued entry in public lane snapshot");
    let item = snapshot[placement]
        .as_array()
        .expect("queue")
        .iter()
        .find(|item| item["entry_id"] == json!(queued.entry_id))
        .expect("queued item")
        .clone();
    let updates = event_payloads(&events, "queue_update");
    assert!(
        updates
            .iter()
            .any(|update| update[placement].as_array().expect("queue").contains(&item)),
        "QueueUpdate must contain admitted message"
    );
    hold.cancel();
    let first_result = first
        .await
        .expect("join")
        .expect("prompt outer")
        .expect("prompt accepted");
    harness.harness().wait_for_idle().await.expect("idle");
    let cleanup = cleanup_json(&fixture, harness.harness().as_ref()).await;
    let after = effect_snapshot(&fixture, &publication, &tools)
        .await
        .delta(before);
    let contexts = fixture
        .adapter
        .contexts
        .lock()
        .expect("contexts")
        .iter()
        .map(context_json)
        .collect::<Vec<_>>();
    assert!(
        contexts.last().expect("second context")["messages"]
            .as_array()
            .expect("messages")
            .contains(&item["message"])
    );
    let mut obs = base_obs(&after, &events, terminal_kind(&first_result.outcome));
    obs["queued_result"] = json!({"entry_id":queued.entry_id});
    obs["queue_order"] = json!(placement);
    obs["queued_item"] = item;
    obs["queue_snapshot"] = snapshot;
    obs["provider_contexts"] = json!(contexts);
    obs["cleanup"] = cleanup;
    harness.harness().close().await.expect("close");
    envelope_with_events(
        case_id,
        json!({"kind":"operation","operation":case_id.strip_prefix("queue.").expect("queue row"),"queue":placement}),
        event_json(&events),
        json!({"kind":terminal_kind(&first_result.outcome),"error":null,"terminal":terminal(&first_result.outcome)}),
        obs,
    )
}
fn clock_json(fixture: &Fixture, before: u64) -> Value {
    json!({"before_ms":before,"after_ms":fixture.clock.now_unix_ms(),"sleeps_ms":fixture.clock.sleeps.lock().expect("sleeps").clone()})
}
async fn retry_recovery() -> Value {
    let case_id = "retry.provider_recovery";
    let fixture = super::fixture().await;
    *fixture.adapter.responses.lock().expect("responses") = VecDeque::from([
        assistant(&fixture.model, StopReason::Error, "503 service unavailable"),
        assistant(&fixture.model, StopReason::Stop, "recovered"),
    ]);
    let retry = pi_ai::RetryPolicy {
        enabled: true,
        max_retries: pi_ai::MaxRetries::new(1).expect("retry"),
        base_delay_ms: SafeUnsigned::new(17).expect("delay"),
    };
    let harness = build(
        &fixture,
        settings_with(&fixture.model, vec![], retry, compaction_settings(false)),
        fixture.environment.clone(),
    )
    .await;
    let (publication, tools, events, _subs) = observe(harness.harness(), case_id);
    let before = effect_snapshot(&fixture, &publication, &tools).await;
    let clock_before = fixture.clock.now_unix_ms();
    let outcome = prompt(harness.harness().as_ref(), "recover").await;
    assert_eq!(terminal_kind(&outcome), "completed");
    let after = effect_snapshot(&fixture, &publication, &tools)
        .await
        .delta(before);
    let scheduled = event_payloads(&events, "retry_scheduled");
    assert_eq!(scheduled.len(), 1);
    assert_eq!(scheduled[0]["error_message"], "503 service unavailable");
    assert!(event_payloads(&events, "retry_start").is_empty());
    assert!(event_payloads(&events, "retry_end").is_empty());
    let mut obs = base_obs(&after, &events, terminal_kind(&outcome));
    obs["retry_scheduled"] = json!(scheduled);
    obs["clock"] = clock_json(&fixture, clock_before);
    obs["adapter_lifecycle"] = json!(fixture.adapter.lifecycle.lock().expect("lifecycle").clone());
    obs["provider_responses"] = json!(
        fixture
            .adapter
            .emitted_responses
            .lock()
            .expect("responses")
            .iter()
            .map(
                |message| message_json(&AgentMessage::BuiltIn(Message::Assistant(Box::new(
                    message.clone()
                ))))
            )
            .collect::<Vec<_>>()
    );
    obs["cleanup"] = cleanup_json(&fixture, harness.harness().as_ref()).await;
    harness.harness().close().await.expect("close");
    envelope_with_events(
        case_id,
        json!({"kind":"operation","operation":"provider_recovery","max_retries":1}),
        event_json(&events),
        json!({"kind":terminal_kind(&outcome),"error":null,"terminal":terminal(&outcome)}),
        obs,
    )
}
async fn retry_overflow() -> Value {
    let case_id = "retry.overflow_precedence";
    let fixture = super::fixture().await;
    *fixture.adapter.responses.lock().expect("responses") = VecDeque::from([
        assistant(
            &fixture.model,
            StopReason::Error,
            "request exceeds the context window",
        ),
        assistant(&fixture.model, StopReason::Stop, "compaction summary"),
        assistant(&fixture.model, StopReason::Stop, "overflow recovered"),
    ]);
    let retry = pi_ai::RetryPolicy {
        enabled: true,
        max_retries: pi_ai::MaxRetries::new(1).expect("retry"),
        base_delay_ms: SafeUnsigned::new(17).expect("delay"),
    };
    let harness = build(
        &fixture,
        settings_with(&fixture.model, vec![], retry, compaction_settings(true)),
        fixture.environment.clone(),
    )
    .await;
    let (publication, tools, events, _subs) = observe(harness.harness(), case_id);
    let before = effect_snapshot(&fixture, &publication, &tools).await;
    let clock_before = fixture.clock.now_unix_ms();
    let outcome = prompt(harness.harness().as_ref(), "overflow").await;
    assert_eq!(terminal_kind(&outcome), "completed");
    let after = effect_snapshot(&fixture, &publication, &tools)
        .await
        .delta(before);
    let responses = fixture
        .adapter
        .emitted_responses
        .lock()
        .expect("responses")
        .clone();
    let first = responses.first().expect("overflow response");
    assert!(pi_ai::is_context_overflow(
        first,
        Some(fixture.model.context_window)
    ));
    let scheduled = event_payloads(&events, "retry_scheduled");
    assert!(
        scheduled.is_empty(),
        "overflow recovery must precede ordinary retry"
    );
    let starts = event_payloads(&events, "compaction_start");
    assert_eq!(starts.len(), 1);
    assert_eq!(starts[0]["reason"], "overflow");
    let entries = compactions(&fixture, &events).await;
    assert_eq!(entries.len(), 1);
    let contexts = fixture.adapter.contexts.lock().expect("contexts").clone();
    assert_eq!(contexts.len(), 3);
    assert!(
        contexts[1]
            .tools
            .as_ref()
            .is_none_or(|tools| tools.is_empty())
    );
    assert_eq!(
        responses[1]
            .content
            .iter()
            .filter_map(|c| match c {
                AssistantContent::Text(t) => Some(t.text.clone()),
                _ => None,
            })
            .collect::<String>(),
        entries[0]["summary"].as_str().expect("summary")
    );
    let mut obs = base_obs(&after, &events, terminal_kind(&outcome));
    obs["overflow_response"] = message_json(&AgentMessage::BuiltIn(Message::Assistant(Box::new(
        first.clone(),
    ))));
    obs["retry_scheduled"] = json!(scheduled);
    obs["clock"] = clock_json(&fixture, clock_before);
    obs["provider_contexts"] = json!(contexts.iter().map(context_json).collect::<Vec<_>>());
    obs["summary_request"] = context_json(&contexts[1]);
    obs["compaction_entries"] = json!(entries);
    obs["adapter_lifecycle"] = json!(fixture.adapter.lifecycle.lock().expect("lifecycle").clone());
    harness.harness().close().await.expect("close");
    envelope_with_events(
        case_id,
        json!({"kind":"operation","operation":"overflow_precedence","retry_enabled":true}),
        event_json(&events),
        json!({"kind":terminal_kind(&outcome),"error":null,"terminal":terminal(&outcome)}),
        obs,
    )
}
async fn abort_stream() -> Value {
    let case_id = "abort.active_stream_cleanup";
    let fixture = super::fixture().await;
    *fixture.adapter.hold.lock().expect("hold") = Some(pi_ai::CancellationToken::new());
    let harness = build(
        &fixture,
        settings(&fixture.model),
        fixture.environment.clone(),
    )
    .await;
    let (publication, tools, events, _subs) = observe(harness.harness(), case_id);
    let before = effect_snapshot(&fixture, &publication, &tools).await;
    let lane = harness.harness().clone();
    let run = tokio::spawn(async move {
        lane.prompt(PromptInput::Text {
            text: "abort active".into(),
            images: Vec::new(),
        })
        .await
    });
    await_active(&fixture).await;
    let active = lane_json(harness.harness().as_ref()).await;
    assert!(active["operation"].is_object());
    let abort = harness
        .harness()
        .abort()
        .await
        .expect("abort outer")
        .expect("abort accepted");
    let result = run
        .await
        .expect("join")
        .expect("prompt outer")
        .expect("prompt accepted");
    harness.harness().wait_for_idle().await.expect("idle");
    let lifecycle = fixture.adapter.lifecycle.lock().expect("lifecycle").clone();
    assert!(
        lifecycle.iter().any(|event| event == "cancelled:1"),
        "held adapter must observe runtime cancellation"
    );
    assert!(
        !lifecycle.iter().any(|event| event.starts_with("terminal:")),
        "cancelled held stream emits no provider terminal"
    );
    assert_eq!(terminal_kind(&result.outcome), "aborted");
    let cleanup = cleanup_json(&fixture, harness.harness().as_ref()).await;
    let after = effect_snapshot(&fixture, &publication, &tools)
        .await
        .delta(before);
    let mut obs = base_obs(&after, &events, terminal_kind(&result.outcome));
    obs["abort_outcome"] = json!({"run_id":abort.run_id,"steer":abort.steer.iter().map(message_json).collect::<Vec<_>>(),"follow_up":abort.follow_up.iter().map(message_json).collect::<Vec<_>>()});
    obs["active_snapshot"] = active;
    obs["adapter_lifecycle"] = json!(lifecycle);
    obs["cleanup"] = cleanup;
    obs["active_streams"] = json!(fixture.adapter.active_streams.load(Ordering::SeqCst));
    assert_eq!(obs["active_streams"], 0);
    harness.harness().close().await.expect("close");
    envelope_with_events(
        case_id,
        json!({"kind":"operation","operation":"abort_active_stream"}),
        event_json(&events),
        json!({"kind":terminal_kind(&result.outcome),"error":null,"terminal":terminal(&result.outcome)}),
        obs,
    )
}
async fn compaction_projection() -> Value {
    let case_id = "compaction.context_projection";
    let fixture = super::fixture().await;
    *fixture.adapter.responses.lock().expect("responses") = VecDeque::from([
        assistant(&fixture.model, StopReason::Stop, "turn one"),
        assistant(&fixture.model, StopReason::Stop, "turn two"),
        assistant(&fixture.model, StopReason::Stop, "summary"),
        assistant(&fixture.model, StopReason::Stop, "turn summary"),
        assistant(&fixture.model, StopReason::Stop, "after compaction"),
    ]);
    let harness = build(
        &fixture,
        settings_with(
            &fixture.model,
            vec![],
            no_retry(),
            compaction_settings(true),
        ),
        fixture.environment.clone(),
    )
    .await;
    let (publication, tools, events, _subs) = observe(harness.harness(), case_id);
    let before = effect_snapshot(&fixture, &publication, &tools).await;
    for text in ["history one", "history two"] {
        assert_eq!(
            terminal_kind(&prompt(harness.harness().as_ref(), text).await),
            "completed"
        );
    }
    let before_entries = fixture
        .session
        .find_entries(EntryQuery::default())
        .await
        .expect("entries");
    let before_leaf = harness.harness().get_leaf_id().await.expect("leaf");
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
    let entries = compactions(&fixture, &events).await;
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["parent_id"], json!(before_leaf));
    let after_entries = fixture
        .session
        .find_entries(EntryQuery::default())
        .await
        .expect("entries");
    let outcome = prompt(harness.harness().as_ref(), "continue compacted").await;
    assert_eq!(terminal_kind(&outcome), "completed");
    let contexts = fixture.adapter.contexts.lock().expect("contexts").clone();
    assert_eq!(contexts.len(), 5);
    let continued = context_json(&contexts[4]);
    assert!(
        continued["messages"]
            .as_array()
            .expect("messages")
            .iter()
            .any(|message| message["content"] == entries[0]["summary"]),
        "new request must project actual durable summary"
    );
    let after = effect_snapshot(&fixture, &publication, &tools)
        .await
        .delta(before);
    let mut obs = base_obs(&after, &events, terminal_kind(&outcome));
    obs["summary_requests"] = json!(contexts[2..4].iter().map(context_json).collect::<Vec<_>>());
    obs["summary_responses"] = json!(
        fixture.adapter.emitted_responses.lock().expect("responses")[2..4]
            .iter()
            .map(
                |message| message_json(&AgentMessage::BuiltIn(Message::Assistant(Box::new(
                    message.clone()
                ))))
            )
            .collect::<Vec<_>>()
    );
    obs["continued_request"] = continued;
    obs["compaction_entries"] = json!(entries);
    obs["session_entries_before"] = json!(before_entries.len());
    obs["session_entries_after"] = json!(after_entries.len());
    obs["before_compaction_leaf"] = json!(before_leaf);
    harness.harness().close().await.expect("close");
    envelope_with_events(
        case_id,
        json!({"kind":"operation","operation":"context_projection","custom_instructions":"project context"}),
        event_json(&events),
        json!({"kind":terminal_kind(&outcome),"error":null,"terminal":terminal(&outcome)}),
        obs,
    )
}
