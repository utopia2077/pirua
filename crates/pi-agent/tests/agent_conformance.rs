use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pi_agent::{Agent, AgentOptions};
use pi_agent_loop::{
    LegacyAgentError, LegacyCancellation, LegacyContext, LegacyConvertFn, LegacyLoopConfig,
    LegacyLoopControl, LegacyStreamFn, LegacyTool, run_agent_loop, run_agent_loop_continue,
    run_agent_loop_with_tools, run_agent_loop_with_tools_and_control,
};
use pi_agent_schema::{
    AgentEvent, AgentEventSink, AgentHarnessTool, AgentMessage, AgentTool, AgentToolResult,
    BoxFuture, Cancellation, ErrorContext, HarnessRuntimeError, SettledAssistantMessage,
    ToolExecutionError, ToolProgressSink,
};
use pi_ai_core::{InputContent, JsonObject, JsonValue, Message, Model, TextContent, ToolCallId};
use serde_json::Value;

const AUTHORITY: &str = "853a80d26c90a14c1886f0ebb8ffaae133ca2185";
const MANIFEST: &str = include_str!("../../../conformance/agent-loop/manifest.json");
const FIXTURES: &str = include_str!("../../../conformance/agent-loop/fixtures.json");

#[derive(Clone, Default)]
struct Events(Arc<Mutex<Vec<AgentEvent>>>);

impl AgentEventSink for Events {
    fn emit(&self, event: AgentEvent) -> BoxFuture<'_, Result<(), HarnessRuntimeError>> {
        self.0.lock().unwrap().push(event);
        Box::pin(async { Ok(()) })
    }
}

fn model() -> Model {
    serde_json::from_value(serde_json::json!({
        "id": "model",
        "name": "Model",
        "api": "api",
        "provider": "provider",
        "baseUrl": "https://example.invalid",
        "reasoning": false,
        "input": ["text"],
        "cost": {"input": 0.0, "output": 0.0, "cacheRead": 0.0, "cacheWrite": 0.0},
        "contextWindow": 1000,
        "maxTokens": 100
    }))
    .expect("valid model fixture")
}

fn user(text: &str) -> AgentMessage {
    serde_json::from_value(serde_json::json!({
        "role": "user",
        "content": text,
        "timestamp": 0
    }))
    .expect("valid user fixture")
}

fn assistant(text: &str, stop_reason: &str) -> SettledAssistantMessage {
    let mut value = serde_json::json!({
        "role": "assistant",
        "content": [{"type": "text", "text": text}],
        "api": "api",
        "provider": "provider",
        "model": "model",
        "usage": {
            "input": 0,
            "output": 0,
            "cacheRead": 0,
            "cacheWrite": 0,
            "totalTokens": 0,
            "cost": {"input": 0.0, "output": 0.0, "cacheRead": 0.0, "cacheWrite": 0.0, "total": 0.0}
        },
        "stopReason": stop_reason,
        "timestamp": 0
    });
    if stop_reason == "error" || stop_reason == "aborted" {
        value["errorMessage"] = serde_json::Value::String(text.to_owned());
    }
    serde_json::from_value(value).expect("valid assistant fixture")
}

fn tool_assistant(stop_reason: &str, calls: &[(&str, &str)]) -> SettledAssistantMessage {
    let content: Vec<_> = calls
        .iter()
        .map(|(id, value)| {
            serde_json::json!({
                "type": "toolCall",
                "id": *id,
                "name": "echo",
                "arguments": {"value": *value}
            })
        })
        .collect();
    serde_json::from_value(serde_json::json!({
        "role": "assistant",
        "content": content,
        "api": "api",
        "provider": "provider",
        "model": "model",
        "usage": {
            "input": 0,
            "output": 0,
            "cacheRead": 0,
            "cacheWrite": 0,
            "totalTokens": 0,
            "cost": {"input": 0.0, "output": 0.0, "cacheRead": 0.0, "cacheWrite": 0.0, "total": 0.0}
        },
        "stopReason": stop_reason,
        "timestamp": 0
    }))
    .expect("valid tool assistant fixture")
}

fn converter() -> LegacyConvertFn {
    Arc::new(|messages| {
        Ok(messages
            .into_iter()
            .filter_map(|message| match message {
                AgentMessage::BuiltIn(message) => Some(message),
                AgentMessage::Custom(_) => None,
            })
            .collect())
    })
}

fn config(stream: LegacyStreamFn) -> LegacyLoopConfig {
    LegacyLoopConfig {
        model: model(),
        transform_context: None,
        convert_to_provider: converter(),
        stream,
        cancellation: LegacyCancellation::new() as Arc<dyn Cancellation>,
    }
}

fn event_types(events: &[AgentEvent]) -> Vec<&'static str> {
    events
        .iter()
        .map(|event| match event {
            AgentEvent::AgentStart => "agent_start",
            AgentEvent::AgentEnd { .. } => "agent_end",
            AgentEvent::TurnStart => "turn_start",
            AgentEvent::TurnEnd { .. } => "turn_end",
            AgentEvent::MessageStart { .. } => "message_start",
            AgentEvent::MessageUpdate { .. } => "message_update",
            AgentEvent::MessageEnd { .. } => "message_end",
            AgentEvent::ToolExecutionStart { .. } => "tool_execution_start",
            AgentEvent::ToolExecutionUpdate { .. } => "tool_execution_update",
            AgentEvent::ToolExecutionEnd { .. } => "tool_execution_end",
        })
        .collect()
}

fn fixture_row(case_id: &str) -> Value {
    let fixtures: Value = serde_json::from_str(FIXTURES).expect("fixtures JSON");
    fixtures["rows"]
        .as_array()
        .expect("fixture rows")
        .iter()
        .find(|row| row["caseId"] == case_id)
        .cloned()
        .unwrap_or_else(|| panic!("fixture row {case_id}"))
}

fn fixture_event_types(case_id: &str) -> Vec<String> {
    fixture_row(case_id)["events"]
        .as_array()
        .expect("fixture events")
        .iter()
        .map(|event| event["type"].as_str().expect("event type").to_owned())
        .collect()
}

fn echo_descriptor() -> AgentTool {
    serde_json::from_value(serde_json::json!({
        "name": "echo",
        "label": "Echo",
        "description": "Echo",
        "parameters": {
            "type": "object",
            "properties": {"value": {"type": "string"}},
            "required": ["value"],
            "additionalProperties": false
        }
    }))
    .expect("valid echo descriptor")
}

struct EchoTool {
    descriptor: AgentTool,
    delay_ms: u64,
    slow_value: Option<String>,
    seen: Arc<Mutex<Vec<String>>>,
}

impl AgentHarnessTool<LegacyContext> for EchoTool {
    fn descriptor(&self) -> &AgentTool {
        &self.descriptor
    }

    fn prepare_arguments(&self, args: JsonValue) -> Result<JsonObject, ToolExecutionError> {
        let JsonValue::Object(args) = args else {
            return Err(ToolExecutionError::new(
                ErrorContext::new("arguments must be an object").expect("static error"),
            ));
        };
        Ok(args)
    }

    fn execute<'a>(
        &'a self,
        _tool_call_id: ToolCallId,
        params: JsonObject,
        _cancellation: &'a dyn Cancellation,
        _progress: &'a dyn ToolProgressSink,
        _context: &'a LegacyContext,
    ) -> BoxFuture<'a, Result<AgentToolResult, ToolExecutionError>> {
        Box::pin(async move {
            let value = params
                .as_map()
                .get("value")
                .and_then(|value| match value {
                    JsonValue::String(value) => Some(value.as_str()),
                    _ => None,
                })
                .unwrap_or_default()
                .to_owned();
            self.seen.lock().unwrap().push(value.clone());
            if self.delay_ms > 0 && self.slow_value.as_deref() == Some(value.as_str()) {
                tokio::time::sleep(Duration::from_millis(self.delay_ms)).await;
            }
            Ok(AgentToolResult {
                content: vec![InputContent::Text(TextContent {
                    content_type: Default::default(),
                    text: format!("echo:{value}"),
                    text_signature: None,
                })],
                details: JsonValue::Object(params),
                usage: None,
                added_tool_names: None,
                terminate: None,
            })
        })
    }
}

#[derive(Default)]
struct QueueControl {
    steering: Mutex<VecDeque<AgentMessage>>,
    follow_up: Mutex<VecDeque<AgentMessage>>,
}

impl LegacyLoopControl for QueueControl {
    fn poll_steering(&self) -> BoxFuture<'_, Vec<AgentMessage>> {
        let messages = self.steering.lock().unwrap().drain(..).collect::<Vec<_>>();
        Box::pin(async move { messages })
    }

    fn poll_follow_up(&self) -> BoxFuture<'_, Vec<AgentMessage>> {
        let messages = self.follow_up.lock().unwrap().drain(..).collect::<Vec<_>>();
        Box::pin(async move { messages })
    }
}

#[test]
fn corpus_manifest_has_exact_denominator_statuses_and_one_negative_each() {
    let manifest: Value = serde_json::from_str(MANIFEST).expect("manifest JSON");
    assert_eq!(manifest["schemaVersion"], 1);
    assert_eq!(manifest["authority"], AUTHORITY);
    let expected = [
        "single_turn.stop",
        "single_turn.transform_context",
        "tool.single_call",
        "tool.parallel_completion_source",
        "tool.sequential_override",
        "tool.length_truncated",
        "tool.block_terminate",
        "queue.steer_after_tools",
        "queue.follow_up_continue",
        "abort.active_stream",
        "provider.throw",
        "continue.existing_context",
    ];
    let rows = manifest["positiveRows"].as_array().expect("positive rows");
    assert_eq!(rows.len(), expected.len());
    for (row, expected_id) in rows.iter().zip(expected) {
        assert_eq!(row["id"], expected_id);
        assert!(
            row["sourceReferences"]
                .as_array()
                .is_some_and(|refs| !refs.is_empty())
        );
        assert!(matches!(
            row["status"].as_str(),
            Some("source-confirmed" | "test-unwitnessed" | "non-applicable")
        ));
    }
    assert_eq!(
        manifest["negatives"].as_array().expect("negatives").len(),
        12
    );
    assert_eq!(
        manifest["denominator"]
            .as_array()
            .expect("denominator")
            .len(),
        14
    );
    let statuses = rows
        .iter()
        .map(|row| (row["id"].as_str().unwrap(), row["status"].as_str().unwrap()))
        .collect::<Vec<_>>();
    assert!(statuses.contains(&("tool.single_call", "test-unwitnessed")));
    assert!(statuses.contains(&("tool.block_terminate", "test-unwitnessed")));
    assert!(statuses.contains(&("queue.follow_up_continue", "test-unwitnessed")));
    assert!(statuses.contains(&("abort.active_stream", "test-unwitnessed")));
    let denominator = manifest["denominator"].as_array().unwrap();
    assert!(
        denominator
            .iter()
            .any(|row| row["id"] == "U12" && row["status"] == "test-unwitnessed")
    );
    assert!(
        denominator
            .iter()
            .any(|row| row["id"] == "U13" && row["status"] == "test-unwitnessed")
    );
    assert!(
        denominator
            .iter()
            .any(|row| row["id"] == "U14" && row["status"] == "non-applicable")
    );
}

#[test]
fn low_level_witnesses_match_source_pinned_lifecycle_and_boundaries() {
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let stream: LegacyStreamFn =
            Arc::new(|_, _| Box::pin(async { Ok(assistant("hello", "stop")) }));
        let sink = Events::default();
        run_agent_loop(
            vec![user("hello")],
            LegacyContext::default(),
            config(stream),
            Arc::new(sink.clone()),
        )
        .await
        .expect("single turn succeeds");
        let expected = fixture_event_types("single_turn.stop");
        assert_eq!(event_types(&sink.0.lock().unwrap()), expected);

        let order = Arc::new(Mutex::new(Vec::<&'static str>::new()));
        let transform_order = order.clone();
        let transform: pi_agent_loop::LegacyTransformFn = Arc::new(move |messages| {
            transform_order.lock().unwrap().push("transform");
            Box::pin(async move { Ok(messages) })
        });
        let convert_order = order.clone();
        let converter: LegacyConvertFn = Arc::new(move |messages| {
            convert_order.lock().unwrap().push("convert");
            Ok(messages
                .into_iter()
                .filter_map(|message| match message {
                    AgentMessage::BuiltIn(message) => Some(message),
                    AgentMessage::Custom(_) => None,
                })
                .collect())
        });
        let transform_sink = Events::default();
        let transform_config = LegacyLoopConfig {
            model: model(),
            transform_context: Some(transform),
            convert_to_provider: converter,
            stream: Arc::new(|_, _| Box::pin(async { Ok(assistant("ok", "stop")) })),
            cancellation: LegacyCancellation::new() as Arc<dyn Cancellation>,
        };
        run_agent_loop(
            vec![user("hello")],
            LegacyContext::default(),
            transform_config,
            Arc::new(transform_sink),
        )
        .await
        .expect("transform turn succeeds");
        assert_eq!(&*order.lock().unwrap(), &["transform", "convert"]);

        let provider_sink = Events::default();
        let provider_stream: LegacyStreamFn = Arc::new(|_, _| {
            Box::pin(async {
                Err(LegacyAgentError::Provider(
                    "synthetic provider failure".to_owned(),
                ))
            })
        });
        let produced = run_agent_loop(
            vec![user("provider throws")],
            LegacyContext::default(),
            config(provider_stream),
            Arc::new(provider_sink.clone()),
        )
        .await
        .expect("provider throw is in-band");
        let AgentMessage::BuiltIn(Message::Assistant(message)) = produced.last().unwrap() else {
            panic!("provider throw must produce an assistant");
        };
        assert_eq!(message.stop_reason, pi_ai_core::StopReason::Error);
        assert!(
            event_types(&provider_sink.0.lock().unwrap()).ends_with(&["turn_end", "agent_end"])
        );

        let continue_sink = Events::default();
        run_agent_loop_continue(
            LegacyContext {
                messages: vec![user("existing")],
                ..LegacyContext::default()
            },
            config(Arc::new(|_, _| {
                Box::pin(async { Ok(assistant("continued", "stop")) })
            })),
            Arc::new(continue_sink.clone()),
        )
        .await
        .expect("continuation succeeds");
        assert_eq!(
            event_types(&continue_sink.0.lock().unwrap()),
            fixture_event_types("continue.existing_context")
        );
        assert_eq!(
            continue_sink
                .0
                .lock()
                .unwrap()
                .iter()
                .filter(|event| matches!(event, AgentEvent::MessageStart { .. }))
                .count(),
            1,
            "continuation has no replayed user message event"
        );
    });
}

#[test]
fn low_level_tool_order_truncation_and_steering_witnesses_are_causal() {
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let tool: LegacyTool = Arc::new(EchoTool {
            descriptor: echo_descriptor(),
            delay_ms: 20,
            slow_value: Some("first".to_owned()),
            seen: seen.clone(),
        });
        let calls = Arc::new(AtomicUsize::new(0));
        let stream: LegacyStreamFn = {
            let calls = calls.clone();
            Arc::new(move |_, _| {
                let call = calls.fetch_add(1, Ordering::SeqCst);
                Box::pin(async move {
                    if call == 0 {
                        Ok(tool_assistant("toolUse", &[("tool-1", "first"), ("tool-2", "second")]))
                    } else {
                        Ok(assistant("done", "stop"))
                    }
                })
            })
        };
        let sink = Events::default();
        run_agent_loop_with_tools(
            vec![user("parallel")],
            LegacyContext {
                tools: vec![echo_descriptor()],
                ..LegacyContext::default()
            },
            config(stream),
            vec![tool],
            Arc::new(sink.clone()),
        )
        .await
        .expect("parallel tools succeed");
        let (end_ids, result_ids) = {
            let event_guard = sink.0.lock().unwrap();
            let end_ids = event_guard
                .iter()
                .filter_map(|event| match event {
                    AgentEvent::ToolExecutionEnd { tool_call_id, .. } => {
                        Some(tool_call_id.to_string())
                    }
                    _ => None,
                })
                .collect::<Vec<_>>();
            let result_ids = event_guard
                .iter()
                .filter_map(|event| match event {
                    AgentEvent::MessageEnd {
                        message: AgentMessage::BuiltIn(Message::ToolResult(result)),
                    } => Some(result.tool_call_id.to_string()),
                    _ => None,
                })
                .collect::<Vec<_>>();
            (end_ids, result_ids)
        };
        assert_eq!(end_ids, vec!["tool-2".to_owned(), "tool-1".to_owned()]);
        assert_eq!(result_ids, vec!["tool-1".to_owned(), "tool-2".to_owned()]);
        assert_eq!(fixture_row("tool.parallel_completion_source")["observations"]["completionOrder"], serde_json::json!(["call-2", "call-1"]));
        assert_eq!(fixture_row("tool.parallel_completion_source")["observations"]["sourceOrder"], serde_json::json!(["call-1", "call-2"]));

        let seen = Arc::new(Mutex::new(Vec::new()));
        let tool: LegacyTool = Arc::new(EchoTool {
            descriptor: echo_descriptor(),
            delay_ms: 0,
            slow_value: None,
            seen: seen.clone(),
        });
        let calls = Arc::new(AtomicUsize::new(0));
        let stream: LegacyStreamFn = {
            let calls = calls.clone();
            Arc::new(move |_, _| {
                let call = calls.fetch_add(1, Ordering::SeqCst);
                Box::pin(async move {
                    if call == 0 {
                        Ok(tool_assistant("length", &[("tool-1", "cut")]))
                    } else {
                        Ok(assistant("done", "stop"))
                    }
                })
            })
        };
        run_agent_loop_with_tools(
            vec![user("truncated")],
            LegacyContext {
                tools: vec![echo_descriptor()],
                ..LegacyContext::default()
            },
            config(stream),
            vec![tool],
            Arc::new(Events::default()),
        )
        .await
        .expect("length truncation is in-band");
        assert!(seen.lock().unwrap().is_empty());

        let tool: LegacyTool = Arc::new(EchoTool {
            descriptor: echo_descriptor(),
            delay_ms: 0,
            slow_value: None,
            seen: Arc::new(Mutex::new(Vec::new())),
        });
        let calls = Arc::new(AtomicUsize::new(0));
        let stream: LegacyStreamFn = {
            let calls = calls.clone();
            Arc::new(move |request, _| {
                let call = calls.fetch_add(1, Ordering::SeqCst);
                Box::pin(async move {
                    if call == 0 {
                        Ok(tool_assistant("toolUse", &[("tool-1", "first")]))
                    } else {
                        assert!(request.messages.iter().any(|message| {
                            matches!(message, Message::User(user) if matches!(&user.content, pi_ai_core::UserContent::Text(text) if text == "steer"))
                        }));
                        Ok(assistant("done", "stop"))
                    }
                })
            })
        };
        let control = Arc::new(QueueControl {
            steering: Mutex::new(VecDeque::from([user("steer")])),
            follow_up: Mutex::new(VecDeque::new()),
        });
        let sink = Events::default();
        run_agent_loop_with_tools_and_control(
            vec![user("start")],
            LegacyContext {
                tools: vec![echo_descriptor()],
                ..LegacyContext::default()
            },
            config(stream),
            vec![tool],
            Some(control),
            Arc::new(sink.clone()),
        )
        .await
        .expect("steering boundary succeeds");
        let events = sink.0.lock().unwrap();
        let turn_end = events
            .iter()
            .position(|event| matches!(event, AgentEvent::TurnEnd { tool_results, .. } if !tool_results.is_empty()))
            .expect("tool turn end");
        let steer = events
            .iter()
            .position(|event| matches!(event, AgentEvent::MessageStart { message: pi_agent_schema::LiveAgentMessage::Settled(AgentMessage::BuiltIn(Message::User(user))) } if matches!(&user.content, pi_ai_core::UserContent::Text(text) if text == "steer")))
            .expect("steering message");
        assert!(turn_end < steer);
    });
}

#[test]
fn public_facade_witness_remains_process_local_and_offline() {
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let options = AgentOptions {
            model: model(),
            system_prompt: Some("system".to_owned()),
            stream: Arc::new(|_, _| Box::pin(async { Ok(assistant("done", "stop")) })),
            convert_to_provider: converter(),
            transform_context: None,
            tools: Vec::new(),
            hooks: None,
        };
        let agent = Agent::new(options);
        agent
            .prompt(vec![user("hello")])
            .await
            .expect("facade prompt");
        let state = agent.state();
        assert!(!state.is_streaming);
        assert_eq!(state.messages.len(), 2);
        agent.reset().expect("idle reset");
        assert!(agent.state().messages.is_empty());
    });
}
