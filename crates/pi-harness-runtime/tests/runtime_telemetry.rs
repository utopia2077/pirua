//! Real public interpreter telemetry, explicit parent IDs and guard completion.
mod runtime_r1_r4_support;
mod runtime_telemetry_support;
#[path = "runtime_r1_r4_support/tracking.rs"]
mod tracking;
use pi_agent_schema::*;
use pi_agent_session::storage::AtomicStorage;
use pi_harness_runtime::owner::RuntimeFactory;
use pi_telemetry::{AttributeValue, SpanStatus};
use runtime_r1_r4_support::*;
use runtime_telemetry_support::*;
use std::sync::{Arc, Mutex};
fn factory(s: &Setup) -> RuntimeFactory<(), pi_agent_session::memory::MemoryStorage> {
    RuntimeFactory::new(
        s.time.clone(),
        s.pump.clone(),
        s.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    )
}
struct RecordingTelemetry(std::sync::Mutex<Vec<String>>);
impl pi_telemetry::TelemetryContext for RecordingTelemetry {
    fn start_span(&self, options: pi_telemetry::SpanOptions) -> pi_telemetry::SpanGuard {
        self.0.lock().unwrap().push(options.name);
        pi_telemetry::SpanGuard::new(pi_telemetry::NoopTelemetrySpan)
    }
}
#[test]
fn successful_public_run_emits_basic_harness_operation_span() {
    let s = Setup::new();
    let f = RuntimeFactory::new(
        s.time.clone(),
        s.pump.clone(),
        s.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    let telemetry = Arc::new(RecordingTelemetry(std::sync::Mutex::new(vec![])));
    let mut options = s.options(DriveMode::Automatic);
    options.telemetry_context = telemetry.clone();
    let c = s.pump.drive(f.create(options)).unwrap();
    s.pump
        .drive(c.harness.prompt(PromptInput::Text {
            text: "basic telemetry".into(),
            images: vec![],
        }))
        .unwrap()
        .unwrap();
    let spans = telemetry.0.lock().unwrap();
    println!("Harness context spans: {spans:?}");
    assert!(
        spans.iter().any(|name| name == "pi.harness.run"),
        "R2 basic run telemetry must exist before full S13 telemetry completion"
    );
}
struct Hooks;
impl HookHandler<BeforeRun> for Hooks {
    fn call(
        &self,
        _: BeforeRunEvent,
    ) -> BoxFuture<'_, Result<Option<BeforeRunResult>, HandlerError>> {
        Box::pin(async { Ok(None) })
    }
}
impl HookHandler<BeforeRequest> for Hooks {
    fn call(
        &self,
        _: BeforeRequestEvent,
    ) -> BoxFuture<'_, Result<Option<BeforeRequestResult>, HandlerError>> {
        Box::pin(async { Ok(None) })
    }
}
impl HookHandler<BeforePayload> for Hooks {
    fn call(
        &self,
        _: BeforePayloadEvent,
    ) -> BoxFuture<'_, Result<Option<BeforePayloadResult>, HandlerError>> {
        Box::pin(async {
            Ok(Some(BeforePayloadResult {
                payload: pi_ai_core::JsonValue::String("PRIVATE_PROVIDER_PAYLOAD".into()),
            }))
        })
    }
}
impl HookHandler<AfterResponse> for Hooks {
    fn call(
        &self,
        _: AfterResponseEvent,
    ) -> BoxFuture<'_, Result<Option<AfterResponseResult>, HandlerError>> {
        Box::pin(async { Ok(None) })
    }
}
impl HookHandler<BeforeRunEnd> for Hooks {
    fn call(
        &self,
        _: BeforeRunEndEvent,
    ) -> BoxFuture<'_, Result<Option<BeforeRunEndResult>, HandlerError>> {
        Box::pin(async { Ok(None) })
    }
}
struct Passive(bool);
impl EventListener for Passive {
    fn on_event(&self, _: HarnessEvent) -> BoxFuture<'_, Result<(), HandlerError>> {
        Box::pin(async move {
            if self.0 {
                panic!("PRIVATE_LISTENER_PAYLOAD")
            }
            Ok(())
        })
    }
}
#[test]
fn real_no_tool_parent_tree_finishes_once_and_records_no_payload_values() {
    let s = Setup::new();
    let recorder = Arc::new(Recorder::default());
    let mut options = s.options(DriveMode::Automatic);
    options.telemetry_context = recorder.clone();
    options.system_prompt = Some(SystemPromptSource::Static("PRIVATE_SYSTEM_PROMPT".into()));
    options.stream_options.headers = Some(
        RawProviderHeaders::new([("x-public".into(), "PRIVATE_HEADER".into())].into())
            .unwrap()
            .classify(&ProviderHeaderPolicy::from_allowlist(["x-public".into()]))
            .unwrap(),
    );
    let c = s.pump.drive(factory(&s).create(options)).unwrap();
    let buffered = s.pump.drive(c.harness.watch()).unwrap();
    let hooks = c.harness.hooks();
    let _a = hooks
        .before_run(HandlerId::new("before-run").unwrap(), Arc::new(Hooks))
        .unwrap();
    let _b = hooks.before_request(None, Arc::new(Hooks)).unwrap();
    let _c = hooks.before_payload(None, Arc::new(Hooks)).unwrap();
    let _d = hooks.after_response(None, Arc::new(Hooks)).unwrap();
    let _e = hooks.before_run_end(None, Arc::new(Hooks)).unwrap();
    let _listener = c
        .harness
        .events()
        .on(HarnessEventType::EntryAdded, Arc::new(Passive(true)));
    s.pump
        .drive(c.harness.prompt(PromptInput::Text {
            text: "PRIVATE_USER_PROMPT".into(),
            images: vec![],
        }))
        .unwrap()
        .unwrap();
    for _ in 0..20 {
        s.pump.tick();
    }
    recorder.assert_finished();
    let rows = recorder.rows();
    let run = rows
        .iter()
        .position(|r| r.name == "pi.harness.run")
        .unwrap();
    assert_eq!(
        text(&rows[run], "pi.operation.outcome").as_deref(),
        Some("completed")
    );
    assert_eq!(
        rows.iter().filter(|r| r.name == "pi.session.write").count(),
        5
    );
    for name in [
        "pi.harness.checkpoint",
        "pi.harness.turn",
        "pi.harness.step",
        "pi.ai.request",
        "pi.harness.hook",
        "pi.harness.event_handler",
    ] {
        assert!(rows.iter().any(|r| r.name == name), "missing {name}");
    }
    for row in &rows {
        if row.name == "pi.harness.run" {
            assert!(row.parent.is_none());
        } else {
            assert!(row.parent.is_some(), "{} must have real parent", row.name);
        }
        let parent = row.parent.map(|id| rows[id].name.as_str());
        match row.name.as_str() {
            "pi.harness.checkpoint" | "pi.harness.turn" => {
                assert_eq!(parent, Some("pi.harness.run"))
            }
            "pi.harness.step" => assert_eq!(parent, Some("pi.harness.turn")),
            "pi.ai.request" => assert_eq!(parent, Some("pi.harness.step")),
            "pi.harness.hook" => match text(row, "pi.hook.name").as_deref() {
                Some("before_payload" | "after_response") => {
                    assert_eq!(parent, Some("pi.ai.request"))
                }
                Some("before_run") => assert_eq!(parent, Some("pi.harness.run")),
                Some("before_run_end") => assert_eq!(parent, Some("pi.harness.checkpoint")),
                _ => assert_eq!(parent, Some("pi.harness.step")),
            },
            "pi.harness.event_handler" => {
                assert!(matches!(row.status, Some(SpanStatus::Error { .. })))
            }
            _ => {}
        }
        for (key, value) in &row.attrs {
            assert!(
                !key.contains("payload") && !key.contains("prompt") && !key.contains("message")
            );
            match value {
                AttributeValue::String(s) => assert!(!s.contains("PRIVATE_")),
                AttributeValue::Strings(v) => assert!(!v.iter().any(|s| s.contains("PRIVATE_"))),
                _ => {}
            }
        }
    }
    // A buffer that has not started cannot keep completed interpreter guards
    // alive. Its later callbacks run under the explicit external owner context.
    let before_delivery = rows.len();
    buffered.start(Arc::new(Passive(false)));
    for _ in 0..20 {
        s.pump.tick();
    }
    recorder.assert_finished();
    let after = recorder.rows();
    assert!(after.len() > before_delivery);
    for row in &after[before_delivery..] {
        assert_eq!(row.name, "pi.harness.event_handler");
        assert!(
            row.parent.is_none(),
            "finished interpreter scopes cannot own later callbacks"
        );
        assert_eq!(row.status, Some(SpanStatus::Ok));
    }
}
#[test]
fn recording_and_noop_preserve_exact_durable_ids_and_transactions() {
    fn run(record: bool) -> (pi_agent_session::storage::StorageSnapshot, Vec<Transaction>) {
        let s = Setup::new();
        let tracked = Arc::new(tracking::TrackedStorage::new(s.store.clone()));
        let mut options =
            s.options_for::<(), _>(tracked_session(&s, tracked.clone()), DriveMode::Automatic);
        if record {
            options.telemetry_context = Arc::new(Recorder::default());
        }
        let f = RuntimeFactory::new(
            s.time.clone(),
            s.pump.clone(),
            s.ids.clone(),
            Arc::new(()),
            Arc::new(Summary),
        );
        let c = s.pump.drive(f.create(options)).unwrap();
        s.pump
            .drive(c.harness.prompt(PromptInput::Text {
                text: "same".into(),
                images: vec![],
            }))
            .unwrap()
            .unwrap();
        let transactions = tracked.transactions.lock().unwrap().clone();
        assert_eq!(transactions.len(), 5);
        (s.pump.drive(s.store.snapshot()).unwrap(), transactions)
    }
    assert_eq!(run(false), run(true));
}
#[test]
fn retry_records_distinct_attempt_children_and_elapsed_sleep() {
    let s = Setup::new();
    let recorder = Arc::new(Recorder::default());
    let mut first = assistant(&s.faux.model, pi_ai_core::StopReason::Error);
    first.error_message = Some("429 PRIVATE_ERROR".into());
    *s.faux.responses.lock().unwrap() = vec![
        first,
        assistant(&s.faux.model, pi_ai_core::StopReason::Stop),
    ]
    .into();
    let mut options = s.options(DriveMode::Automatic);
    options.telemetry_context = recorder.clone();
    options.retry.enabled = true;
    options.retry.max_retries = pi_ai_core::MaxRetries::new(1).unwrap();
    let c = s.pump.drive(factory(&s).create(options)).unwrap();
    let mut call = Box::pin(c.harness.prompt(PromptInput::Text {
        text: "retry".into(),
        images: vec![],
    }));
    assert!(poll(call.as_mut()).is_pending());
    for _ in 0..100 {
        s.pump.tick();
    }
    s.time.0.store(17, std::sync::atomic::Ordering::SeqCst);
    s.pump.drive(call).unwrap().unwrap();
    for _ in 0..20 {
        s.pump.tick();
    }
    recorder.assert_finished();
    let rows = recorder.rows();
    let steps: Vec<_> = rows
        .iter()
        .filter(|r| r.name == "pi.harness.step")
        .collect();
    assert_eq!(steps.len(), 2);
    for (i, step) in steps.iter().enumerate() {
        assert!(
            matches!(step.attrs.get("pi.step.attempt"),Some(AttributeValue::Number(n)) if n.get()==(i+1) as f64)
        );
    }
    assert_eq!(text(steps[0], "pi.step.outcome").as_deref(), Some("retry"));
    assert_eq!(
        text(steps[1], "pi.step.outcome").as_deref(),
        Some("succeeded")
    );
    let sleep = rows.iter().find(|r| r.name == "pi.harness.sleep").unwrap();
    assert_eq!(text(sleep, "pi.sleep.outcome").as_deref(), Some("elapsed"));
    assert_eq!(rows[sleep.parent.unwrap()].name, "pi.harness.turn");
}

struct ContextValue(u64);
struct Tool {
    descriptor: AgentTool,
    release: pi_ai_core::CancellationToken,
    log: Arc<Mutex<Vec<String>>>,
}
impl AgentHarnessTool<ContextValue> for Tool {
    fn descriptor(&self) -> &AgentTool {
        &self.descriptor
    }
    fn prepare_arguments(
        &self,
        args: pi_ai_core::JsonValue,
    ) -> Result<pi_ai_core::JsonObject, ToolExecutionError> {
        self.log
            .lock()
            .unwrap()
            .push(format!("prepare:{}", self.descriptor.name.as_str()));
        let pi_ai_core::JsonValue::Object(args) = args else {
            panic!("fixture expects object");
        };
        let mut args = args.into_map();
        args.insert("prepared".into(), pi_ai_core::JsonValue::Bool(true));
        Ok(pi_ai_core::JsonObject::new(args))
    }
    fn execute<'a>(
        &'a self,
        _: pi_ai_core::ToolCallId,
        args: pi_ai_core::JsonObject,
        _: &'a dyn Cancellation,
        _: &'a dyn ToolProgressSink,
        context: &'a ContextValue,
    ) -> BoxFuture<'a, Result<AgentToolResult, ToolExecutionError>> {
        Box::pin(async move {
            assert_eq!(context.0, 37);
            assert_eq!(
                args.as_map().get("prepared"),
                Some(&pi_ai_core::JsonValue::Bool(true))
            );
            self.log
                .lock()
                .unwrap()
                .push(format!("start:{}", self.descriptor.name.as_str()));
            self.release.cancelled().await;
            self.log
                .lock()
                .unwrap()
                .push(format!("raw:{}", self.descriptor.name.as_str()));
            let mut usage = pi_ai_core::Usage::zero();
            usage.input = n(3);
            usage.total_tokens = n(3);
            Ok(AgentToolResult {
                content: vec![],
                details: pi_ai_core::JsonValue::String("PRIVATE_TOOL_RESULT".into()),
                usage: Some(usage),
                added_tool_names: None,
                terminate: None,
            })
        })
    }
}
fn tool(
    name: &str,
    log: Arc<Mutex<Vec<String>>>,
    release: pi_ai_core::CancellationToken,
) -> Arc<Tool> {
    Arc::new(Tool {descriptor:AgentTool {name:pi_ai_core::ToolName::new(name).unwrap(),label:name.into(),description:name.into(),parameters:serde_json::from_value(serde_json::json!({"type":"object","properties":{"prepared":{"type":"boolean"}},"additionalProperties":false})).unwrap(),constrained_sampling:None,execution_mode:None,replay:Some(ToolReplay::Safe)},log,release})
}

fn reopen(
    s: &Setup,
) -> Arc<pi_agent_session::session::RuntimeSession<pi_agent_session::memory::MemoryStorage>> {
    let store = s.backing.open(s.time.clone()).unwrap();
    Arc::new(
        s.pump
            .drive(pi_agent_session::session::RuntimeSession::new(
                store,
                s.session.metadata(),
                Arc::new(
                    pi_agent_session::codec::SessionCodec::new(
                        SessionCodecOptions::default(),
                        ProviderHeaderPolicy::from_allowlist(["x-public".into()]),
                    )
                    .unwrap(),
                ),
                s.ids.clone(),
                Arc::new(pi_telemetry::NOOP_TELEMETRY_CONTEXT),
            ))
            .unwrap(),
    )
}
fn tool_response(s: &Setup) -> pi_ai_core::AssistantMessage {
    let mut response = assistant(&s.faux.model, pi_ai_core::StopReason::ToolUse);
    response.content = vec![pi_ai_core::AssistantContent::ToolCall(
        pi_ai_core::ToolCall {
            content_type: Default::default(),
            id: pi_ai_core::ToolCallId::new("call-a").unwrap(),
            name: pi_ai_core::ToolName::new("a").unwrap(),
            arguments: Default::default(),
            thought_signature: None,
            namespace: None,
        },
    )];
    response
}
#[test]
fn parallel_phase_two_spans_share_turn_parent_and_finish_in_actual_completion_order() {
    let s = Setup::new();
    let log = Arc::new(Mutex::new(vec![]));
    let a = pi_ai_core::CancellationToken::new();
    let b = pi_ai_core::CancellationToken::new();
    let mut response = tool_response(&s);
    if let pi_ai_core::AssistantContent::ToolCall(call) = &response.content[0] {
        let mut call = call.clone();
        call.id = pi_ai_core::ToolCallId::new("call-b").unwrap();
        call.name = pi_ai_core::ToolName::new("b").unwrap();
        response
            .content
            .push(pi_ai_core::AssistantContent::ToolCall(call));
    }
    *s.faux.responses.lock().unwrap() = vec![
        response,
        assistant(&s.faux.model, pi_ai_core::StopReason::Stop),
    ]
    .into();
    let recorder = Arc::new(Recorder::default());
    let mut options = s.options(DriveMode::Automatic);
    options.telemetry_context = recorder.clone();
    options.tools = vec![
        tool("a", log.clone(), a.clone()),
        tool("b", log.clone(), b.clone()),
    ];
    let f = RuntimeFactory::new(
        s.time.clone(),
        s.pump.clone(),
        s.ids.clone(),
        Arc::new(ContextValue(37)),
        Arc::new(Summary),
    );
    let c = s.pump.drive(f.create(options)).unwrap();
    s.pump
        .drive(c.harness.set_active_tools(vec![
            pi_ai_core::ToolName::new("a").unwrap(),
            pi_ai_core::ToolName::new("b").unwrap(),
        ]))
        .unwrap();
    let mut call = Box::pin(c.harness.prompt(PromptInput::Text {
        text: "parallel".into(),
        images: vec![],
    }));
    assert!(poll(call.as_mut()).is_pending());
    for _ in 0..150 {
        s.pump.tick();
    }
    let rows = recorder.rows();
    let tools: Vec<_> = rows
        .iter()
        .filter(|r| r.name == "pi.harness.tool")
        .collect();
    assert_eq!(tools.len(), 2);
    assert_eq!(tools[0].parent, tools[1].parent);
    assert_eq!(rows[tools[0].parent.unwrap()].name, "pi.harness.turn");
    assert_eq!(tools[0].finishes, 0);
    assert_eq!(tools[1].finishes, 0);
    b.cancel();
    for _ in 0..20 {
        s.pump.tick();
    }
    let rows = recorder.rows();
    let tools: Vec<_> = rows
        .iter()
        .filter(|r| r.name == "pi.harness.tool")
        .collect();
    assert_eq!(tools[0].finishes, 0);
    assert_eq!(tools[1].finishes, 1);
    a.cancel();
    s.pump.drive(call).unwrap().unwrap();
    for _ in 0..20 {
        s.pump.tick();
    }
    recorder.assert_finished();
}
#[test]
fn safe_replay_creates_one_raw_span_and_unsafe_reconciliation_creates_none() {
    for safe in [true, false] {
        let s = Setup::new();
        let log = Arc::new(Mutex::new(vec![]));
        let release = pi_ai_core::CancellationToken::new();
        let mut binding = tool("a", log.clone(), release.clone());
        if !safe {
            Arc::get_mut(&mut binding).unwrap().descriptor.replay = Some(ToolReplay::Never);
        }
        *s.faux.responses.lock().unwrap() = vec![
            tool_response(&s),
            assistant(&s.faux.model, pi_ai_core::StopReason::Stop),
        ]
        .into();
        let first = Arc::new(Recorder::default());
        let mut options = s.options(DriveMode::Automatic);
        options.telemetry_context = first.clone();
        options.tools = vec![binding.clone()];
        let f = RuntimeFactory::new(
            s.time.clone(),
            s.pump.clone(),
            s.ids.clone(),
            Arc::new(ContextValue(37)),
            Arc::new(Summary),
        );
        let c = s.pump.drive(f.create(options)).unwrap();
        s.pump
            .drive(
                c.harness
                    .set_active_tools(vec![pi_ai_core::ToolName::new("a").unwrap()]),
            )
            .unwrap();
        let mut call = Box::pin(c.harness.prompt(PromptInput::Text {
            text: "replay".into(),
            images: vec![],
        }));
        assert!(poll(call.as_mut()).is_pending());
        for _ in 0..100 {
            s.pump.tick();
        }
        assert!(log.lock().unwrap().contains(&"start:a".into()));
        s.pump.drive(c.harness.close()).unwrap();
        drop(call);
        for _ in 0..20 {
            s.pump.tick();
        }
        first.assert_finished();
        let record = Arc::new(Recorder::default());
        let mut options = s.options(DriveMode::Automatic);
        options.session = reopen(&s);
        options.telemetry_context = record.clone();
        options.tools = vec![binding];
        let restored = s.pump.drive(f.create(options)).unwrap();
        release.cancel();
        s.pump.drive(restored.harness.resume()).unwrap().unwrap();
        for _ in 0..20 {
            s.pump.tick();
        }
        record.assert_finished();
        let rows = record.rows();
        let tools: Vec<_> = rows
            .iter()
            .filter(|r| r.name == "pi.harness.tool")
            .collect();
        assert_eq!(tools.len(), usize::from(safe));
        if safe {
            assert_eq!(
                tools[0].attrs.get("pi.tool.recovery"),
                Some(&AttributeValue::Boolean(true))
            );
            assert_eq!(text(tools[0], "pi.tool.replay").as_deref(), Some("safe"));
        }
        assert_eq!(
            rows.iter().filter(|r| r.name == "pi.ai.request").count(),
            1,
            "only the later real assistant gets a request span"
        );
    }
}
#[test]
fn malformed_tool_arguments_settle_without_raw_execution_span() {
    let s = Setup::new();
    let log = Arc::new(Mutex::new(vec![]));
    let mut response = tool_response(&s);
    if let pi_ai_core::AssistantContent::ToolCall(call) = &mut response.content[0] {
        call.arguments = pi_ai_core::JsonObject::new(
            [("PRIVATE_ARGUMENT".into(), pi_ai_core::JsonValue::Bool(true))].into(),
        );
    }
    *s.faux.responses.lock().unwrap() = vec![
        response,
        assistant(&s.faux.model, pi_ai_core::StopReason::Stop),
    ]
    .into();
    let recorder = Arc::new(Recorder::default());
    let mut options = s.options(DriveMode::Automatic);
    options.telemetry_context = recorder.clone();
    options.tools = vec![tool("a", log.clone(), pi_ai_core::CancellationToken::new())];
    let f = RuntimeFactory::new(
        s.time.clone(),
        s.pump.clone(),
        s.ids.clone(),
        Arc::new(ContextValue(37)),
        Arc::new(Summary),
    );
    let c = s.pump.drive(f.create(options)).unwrap();
    s.pump
        .drive(
            c.harness
                .set_active_tools(vec![pi_ai_core::ToolName::new("a").unwrap()]),
        )
        .unwrap();
    s.pump
        .drive(c.harness.prompt(PromptInput::Text {
            text: "bad args".into(),
            images: vec![],
        }))
        .unwrap()
        .unwrap();
    for _ in 0..20 {
        s.pump.tick();
    }
    recorder.assert_finished();
    assert!(!recorder.rows().iter().any(|r| r.name == "pi.harness.tool"));
    assert!(!log.lock().unwrap().contains(&"start:a".into()));
}
struct PendingListener;
impl EventListener for PendingListener {
    fn on_event(&self, _: HarnessEvent) -> BoxFuture<'_, Result<(), HandlerError>> {
        Box::pin(std::future::pending())
    }
}
struct PanicPrompt;
impl SystemPromptProvider<()> for PanicPrompt {
    fn resolve<'a>(&'a self, _: &'a ()) -> BoxFuture<'a, Result<String, HarnessRuntimeError>> {
        Box::pin(async { panic!("PRIVATE_TRUSTED_ERROR") })
    }
}
#[test]
fn close_host_drop_and_trusted_unwind_finish_owned_guards_without_public_waiter() {
    for mode in ["close", "host-drop", "unwind"] {
        let s = Setup::new();
        let record = Arc::new(Recorder::default());
        let hold = pi_ai_core::CancellationToken::new();
        *s.faux.hold.lock().unwrap() = Some(hold);
        let mut options = s.options(DriveMode::Automatic);
        options.telemetry_context = record.clone();
        if mode == "unwind" {
            options.system_prompt = Some(SystemPromptSource::Provider(Arc::new(PanicPrompt)));
        }
        let c = s.pump.drive(factory(&s).create(options)).unwrap();
        let _listener = c
            .harness
            .events()
            .on(HarnessEventType::RunStart, Arc::new(PendingListener));
        let mut call = Box::pin(c.harness.prompt(PromptInput::Text {
            text: "lifetime".into(),
            images: vec![],
        }));
        assert!(poll(call.as_mut()).is_pending());
        drop(call);
        for _ in 0..60 {
            s.pump.tick();
        }
        if mode == "close" {
            s.pump.drive(c.harness.close()).unwrap();
        } else if mode == "host-drop" {
            let tasks = std::mem::take(&mut *s.pump.tasks.lock().unwrap());
            assert!(!tasks.is_empty());
            drop(tasks);
        }
        for _ in 0..30 {
            s.pump.tick();
        }
        record.assert_finished();
        assert!(record.rows().iter().any(|r| r.name == "pi.harness.run"));
        if mode != "close" {
            assert!(matches!(
                s.pump.drive(c.harness.get_stream_options()),
                Err(HarnessRuntimeError::Fault(_))
            ));
        }
    }
}

fn tracked_session(
    s: &Setup,
    tracked: Arc<tracking::TrackedStorage>,
) -> Arc<pi_agent_session::session::RuntimeSession<tracking::TrackedStorage>> {
    Arc::new(
        s.pump
            .drive(pi_agent_session::session::RuntimeSession::new(
                tracked,
                s.session.metadata(),
                Arc::new(
                    pi_agent_session::codec::SessionCodec::new(
                        SessionCodecOptions::default(),
                        ProviderHeaderPolicy::default(),
                    )
                    .unwrap(),
                ),
                s.ids.clone(),
                Arc::new(pi_telemetry::NOOP_TELEMETRY_CONTEXT),
            ))
            .unwrap(),
    )
}

#[test]
fn typed_provider_failure_finishes_run_attempt_and_request_with_error_status() {
    let s = Setup::new();
    let mut message = assistant(&s.faux.model, pi_ai_core::StopReason::Error);
    message.error_message = Some("PRIVATE_UNRETRYABLE_FAILURE".into());
    *s.faux.responses.lock().unwrap() = vec![message].into();
    let recorder = Arc::new(Recorder::default());
    let mut options = s.options(DriveMode::Automatic);
    options.telemetry_context = recorder.clone();
    options.retry.enabled = false;
    let c = s.pump.drive(factory(&s).create(options)).unwrap();
    let result = s
        .pump
        .drive(c.harness.prompt(PromptInput::Text {
            text: "failed".into(),
            images: vec![],
        }))
        .unwrap()
        .unwrap();
    assert!(matches!(result.outcome, RunOutcome::FailedWithAssistant(_)));
    for _ in 0..20 {
        s.pump.tick();
    }
    recorder.assert_finished();
    let rows = recorder.rows();
    for (name, key, value) in [
        ("pi.harness.run", "pi.operation.outcome", "failed"),
        ("pi.harness.step", "pi.step.outcome", "failed"),
        ("pi.ai.request", "pi.ai.response.stop_reason", "error"),
    ] {
        let row = rows.iter().find(|r| r.name == name).unwrap();
        assert_eq!(text(row, key).as_deref(), Some(value));
        assert!(matches!(row.status, Some(SpanStatus::Error { .. })));
    }
    assert!(
        rows.iter()
            .any(|r| text(r, "pi.checkpoint.kind").as_deref() == Some("failure_drain"))
    );
}

struct CountEntries(std::sync::atomic::AtomicUsize);
impl EventListener for CountEntries {
    fn on_event(&self, _: HarnessEvent) -> BoxFuture<'_, Result<(), HandlerError>> {
        Box::pin(async move {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        })
    }
}
#[test]
fn close_drains_admitted_write_observation_before_releasing_event_contexts() {
    for fault in [false, true] {
        let s = Setup::new();
        let tracked = Arc::new(tracking::TrackedStorage::new(s.store.clone()));
        let recorder = Arc::new(Recorder::default());
        let mut options =
            s.options_for::<(), _>(tracked_session(&s, tracked.clone()), DriveMode::Automatic);
        options.telemetry_context = recorder.clone();
        let f = RuntimeFactory::new(
            s.time.clone(),
            s.pump.clone(),
            s.ids.clone(),
            Arc::new(()),
            Arc::new(Summary),
        );
        let c = s.pump.drive(f.create(options)).unwrap();
        let other = s
            .pump
            .drive(c.harness.create_lane(LaneId::new("other").unwrap(), None))
            .unwrap()
            .unwrap();
        let writes_before = recorder
            .rows()
            .iter()
            .filter(|r| r.name == "pi.session.write")
            .count();
        let entries = Arc::new(CountEntries(std::sync::atomic::AtomicUsize::new(0)));
        let _subscription = c
            .harness
            .events()
            .on(HarnessEventType::EntryAdded, entries.clone());
        tracked
            .pause_entry_commit
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let mut call = Box::pin(c.harness.prompt(PromptInput::Text {
            text: "admitted".into(),
            images: vec![],
        }));
        assert!(poll(call.as_mut()).is_pending());
        drop(call);
        for _ in 0..30 {
            s.pump.tick();
            if tracked.commit_entered.is_cancelled() {
                break;
            }
        }
        assert!(tracked.commit_entered.is_cancelled());
        let mut close = None;
        if fault {
            tracked
                .fail_reads
                .store(true, std::sync::atomic::Ordering::SeqCst);
            assert!(matches!(
                s.pump.drive(other.get_leaf_id()),
                Err(HarnessRuntimeError::Fault(_))
            ));
            tracked
                .fail_reads
                .store(false, std::sync::atomic::Ordering::SeqCst);
        } else {
            let mut future = Box::pin(c.harness.close());
            assert!(poll(future.as_mut()).is_pending());
            close = Some(future);
        }
        tracked.release_commit.cancel();
        if let Some(close) = close {
            s.pump.drive(close).unwrap();
        }
        for _ in 0..40 {
            s.pump.tick();
        }
        assert_eq!(
            entries.0.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "admitted entry publication must reach listener: fault={fault}"
        );
        recorder.assert_finished();
        assert_eq!(
            recorder
                .rows()
                .iter()
                .filter(|r| r.name == "pi.session.write")
                .count(),
            writes_before + 1
        );
    }
}
