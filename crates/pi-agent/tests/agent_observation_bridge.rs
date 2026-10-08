use std::collections::BTreeSet;
use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pi_agent_loop::{
    LegacyCancellation, LegacyContext, LegacyConvertFn, LegacyLoopConfig, LegacyStreamFn,
    LegacyTool, LegacyTransformFn, run_agent_loop, run_agent_loop_continue,
    run_agent_loop_with_tools,
};
use pi_agent_schema::{
    AgentEvent, AgentEventSink, AgentHarnessTool, AgentMessage, AgentTool, AgentToolResult,
    BoxFuture, Cancellation, ErrorContext, HarnessRuntimeError, LiveAgentMessage,
    SettledAssistantMessage, ToolExecutionError, ToolProgressSink,
};
use pi_ai_core::{InputContent, JsonObject, JsonValue, Message, Model, TextContent};
use serde_json::{Map, Value, json};

const AUTHORITY: &str = "853a80d26c90a14c1886f0ebb8ffaae133ca2185";

#[derive(Clone, Default)]
struct Events(Arc<Mutex<Vec<AgentEvent>>>);

impl AgentEventSink for Events {
    fn emit(&self, event: AgentEvent) -> BoxFuture<'_, Result<(), HarnessRuntimeError>> {
        self.0.lock().unwrap().push(event);
        Box::pin(async { Ok(()) })
    }
}

fn model() -> Model {
    serde_json::from_value(json!({
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
    serde_json::from_value(json!({
        "role": "user",
        "content": text,
        "timestamp": 0
    }))
    .expect("valid user fixture")
}

fn assistant(text: &str, stop_reason: &str) -> SettledAssistantMessage {
    let mut value = json!({
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
    if matches!(stop_reason, "error" | "aborted") {
        value["errorMessage"] = Value::String(text.to_owned());
    }
    serde_json::from_value(value).expect("valid assistant fixture")
}

fn tool_assistant(stop_reason: &str, calls: &[(&str, &str)]) -> SettledAssistantMessage {
    let content: Vec<_> = calls
        .iter()
        .map(|(id, value)| {
            json!({
                "type": "toolCall",
                "id": *id,
                "name": "echo",
                "arguments": {"value": *value}
            })
        })
        .collect();
    serde_json::from_value(json!({
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

fn echo_descriptor() -> AgentTool {
    serde_json::from_value(json!({
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
    executed_ids: Arc<Mutex<Vec<String>>>,
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
        tool_call_id: pi_ai_core::ToolCallId,
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
            self.executed_ids
                .lock()
                .unwrap()
                .push(tool_call_id.to_string());
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
struct ProjectionState {
    next_user: usize,
    next_assistant: usize,
    open_user: Option<String>,
    open_assistant: Option<String>,
}

fn stop_reason(value: pi_ai_core::StopReason) -> &'static str {
    match value {
        pi_ai_core::StopReason::Pending => "pending",
        pi_ai_core::StopReason::Stop => "stop",
        pi_ai_core::StopReason::Length => "length",
        pi_ai_core::StopReason::ToolUse => "toolUse",
        pi_ai_core::StopReason::Error => "error",
        pi_ai_core::StopReason::Aborted => "aborted",
        pi_ai_core::StopReason::Deferred => "deferred",
    }
}

fn role_message_start(
    message: &LiveAgentMessage,
    state: &mut ProjectionState,
) -> Map<String, Value> {
    let mut result = Map::new();
    match message {
        LiveAgentMessage::Assistant(_) => {
            let id = format!("assistant-{}", state.next_assistant);
            state.next_assistant += 1;
            state.open_assistant = Some(id.clone());
            result.insert("role".to_owned(), Value::String("assistant".to_owned()));
            result.insert("messageId".to_owned(), Value::String(id));
        }
        LiveAgentMessage::Settled(AgentMessage::BuiltIn(Message::User(_))) => {
            let id = format!("user-{}", state.next_user);
            state.next_user += 1;
            state.open_user = Some(id.clone());
            result.insert("role".to_owned(), Value::String("user".to_owned()));
            result.insert("messageId".to_owned(), Value::String(id));
        }
        LiveAgentMessage::Settled(AgentMessage::BuiltIn(Message::Assistant(_))) => {
            let id = format!("assistant-{}", state.next_assistant);
            state.next_assistant += 1;
            state.open_assistant = Some(id.clone());
            result.insert("role".to_owned(), Value::String("assistant".to_owned()));
            result.insert("messageId".to_owned(), Value::String(id));
        }
        LiveAgentMessage::Settled(AgentMessage::BuiltIn(Message::ToolResult(message))) => {
            result.insert("role".to_owned(), Value::String("toolResult".to_owned()));
            result.insert(
                "toolCallId".to_owned(),
                Value::String(message.tool_call_id.to_string()),
            );
            result.insert(
                "toolName".to_owned(),
                Value::String(message.tool_name.to_string()),
            );
        }
        LiveAgentMessage::Settled(AgentMessage::Custom(_)) => {
            panic!("selected bridge rows do not contain custom message events")
        }
    }
    result
}

fn role_message_end(message: &AgentMessage, state: &mut ProjectionState) -> Map<String, Value> {
    let mut result = Map::new();
    match message {
        AgentMessage::BuiltIn(Message::User(_)) => {
            result.insert("role".to_owned(), Value::String("user".to_owned()));
            result.insert(
                "messageId".to_owned(),
                Value::String(
                    state
                        .open_user
                        .take()
                        .expect("user message end has a matching start"),
                ),
            );
        }
        AgentMessage::BuiltIn(Message::Assistant(message)) => {
            result.insert("role".to_owned(), Value::String("assistant".to_owned()));
            result.insert(
                "messageId".to_owned(),
                Value::String(
                    state
                        .open_assistant
                        .take()
                        .expect("assistant message end has a matching start"),
                ),
            );
            result.insert(
                "stopReason".to_owned(),
                Value::String(stop_reason(message.stop_reason).to_owned()),
            );
            if matches!(
                message.stop_reason,
                pi_ai_core::StopReason::Error | pi_ai_core::StopReason::Aborted
            ) {
                result.insert(
                    "errorCategory".to_owned(),
                    Value::String(
                        if message.stop_reason == pi_ai_core::StopReason::Aborted {
                            "aborted"
                        } else {
                            "provider"
                        }
                        .to_owned(),
                    ),
                );
            }
        }
        AgentMessage::BuiltIn(Message::ToolResult(message)) => {
            result.insert("role".to_owned(), Value::String("toolResult".to_owned()));
            result.insert(
                "toolCallId".to_owned(),
                Value::String(message.tool_call_id.to_string()),
            );
            result.insert(
                "toolName".to_owned(),
                Value::String(message.tool_name.to_string()),
            );
        }
        AgentMessage::Custom(_) => panic!("selected bridge rows do not contain custom messages"),
    }
    result
}

fn project_events(
    events: &[AgentEvent],
    executed_ids: &BTreeSet<String>,
    immediate_reason: Option<&str>,
) -> Vec<Value> {
    let mut state = ProjectionState::default();
    events
        .iter()
        .enumerate()
        .map(|(index, event)| {
            let mut result = Map::new();
            result.insert("index".to_owned(), json!(index));
            match event {
                AgentEvent::AgentStart => {
                    result.insert("type".to_owned(), json!("agent_start"));
                }
                AgentEvent::AgentEnd { .. } => {
                    result.insert("type".to_owned(), json!("agent_end"));
                }
                AgentEvent::TurnStart => {
                    result.insert("type".to_owned(), json!("turn_start"));
                }
                AgentEvent::TurnEnd { tool_results, .. } => {
                    result.insert("type".to_owned(), json!("turn_end"));
                    result.insert(
                        "toolResults".to_owned(),
                        Value::Array(
                            tool_results
                                .iter()
                                .map(|result| Value::String(result.tool_call_id.to_string()))
                                .collect(),
                        ),
                    );
                }
                AgentEvent::MessageStart { message } => {
                    result.insert("type".to_owned(), json!("message_start"));
                    result.extend(role_message_start(message, &mut state));
                }
                AgentEvent::MessageUpdate { message, .. } => {
                    result.insert("type".to_owned(), json!("message_update"));
                    result.extend(role_message_start(message, &mut state));
                }
                AgentEvent::MessageEnd { message } => {
                    result.insert("type".to_owned(), json!("message_end"));
                    result.extend(role_message_end(message, &mut state));
                }
                AgentEvent::ToolExecutionStart {
                    tool_call_id,
                    tool_name,
                    ..
                } => {
                    result.insert("type".to_owned(), json!("tool_execution_start"));
                    result.insert("toolCallId".to_owned(), json!(tool_call_id.to_string()));
                    result.insert("toolName".to_owned(), json!(tool_name.to_string()));
                }
                AgentEvent::ToolExecutionUpdate {
                    tool_call_id,
                    tool_name,
                    ..
                } => {
                    result.insert("type".to_owned(), json!("tool_execution_update"));
                    result.insert("toolCallId".to_owned(), json!(tool_call_id.to_string()));
                    result.insert("toolName".to_owned(), json!(tool_name.to_string()));
                    result.insert("progress".to_owned(), json!("updated"));
                }
                AgentEvent::ToolExecutionEnd {
                    tool_call_id,
                    tool_name,
                    result: tool_result,
                    is_error,
                } => {
                    result.insert("type".to_owned(), json!("tool_execution_end"));
                    result.insert("toolCallId".to_owned(), json!(tool_call_id.to_string()));
                    result.insert("toolName".to_owned(), json!(tool_name.to_string()));
                    result.insert("isError".to_owned(), json!(is_error));
                    result.insert(
                        "executed".to_owned(),
                        json!(executed_ids.contains(&tool_call_id.to_string())),
                    );
                    if let Some(reason) = immediate_reason {
                        result.insert("immediate".to_owned(), json!(reason));
                    }
                    if let Some(terminate) = tool_result.terminate {
                        result.insert("terminate".to_owned(), json!(terminate));
                    }
                }
            }
            Value::Object(result)
        })
        .collect()
}

fn tool_orders(events: &[AgentEvent]) -> (Vec<String>, Vec<String>) {
    let completion = events
        .iter()
        .filter_map(|event| match event {
            AgentEvent::ToolExecutionEnd { tool_call_id, .. } => Some(tool_call_id.to_string()),
            _ => None,
        })
        .collect();
    let source = events
        .iter()
        .filter_map(|event| match event {
            AgentEvent::MessageEnd {
                message: AgentMessage::BuiltIn(Message::ToolResult(result)),
            } => Some(result.tool_call_id.to_string()),
            _ => None,
        })
        .collect();
    (completion, source)
}

fn truncation_reason(events: &[AgentEvent]) -> Option<String> {
    events.iter().find_map(|event| {
        let AgentEvent::ToolExecutionEnd {
            result, is_error, ..
        } = event
        else {
            return None;
        };
        if !is_error {
            return None;
        }
        let has_length_error = result.content.iter().any(|content| match content {
            InputContent::Text(text) => text.text.contains("output token limit"),
            InputContent::Image(_) => false,
        });
        has_length_error.then(|| "length".to_owned())
    })
}

fn actual_observation(
    case_id: &str,
    events: &[AgentEvent],
    executed_ids: &BTreeSet<String>,
    observations: Value,
    immediate_reason: Option<&str>,
) -> Value {
    let projected = project_events(events, executed_ids, immediate_reason);
    let stop_reason = projected
        .iter()
        .rev()
        .find_map(|event| {
            (event["type"] == "message_end" && event["role"] == "assistant")
                .then(|| event["stopReason"].clone())
        })
        .expect("actual trace has an assistant terminal message");
    json!({
        "schemaVersion": 1,
        "authority": AUTHORITY,
        "caseId": case_id,
        "events": projected,
        "outcome": {"stopReason": stop_reason, "errorCategory": null},
        "observations": observations,
    })
}

fn run_single_turn() -> Value {
    let calls = Arc::new(AtomicUsize::new(0));
    let provider_calls = calls.clone();
    let stream: LegacyStreamFn = Arc::new(move |_, _| {
        provider_calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(assistant("hello from model", "stop")) })
    });
    let events = Events::default();
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(run_agent_loop(
            vec![user("hello")],
            LegacyContext::default(),
            config(stream),
            Arc::new(events.clone()),
        ))
        .expect("single-turn loop succeeds");
    actual_observation(
        "single_turn.stop",
        &events.0.lock().unwrap(),
        &BTreeSet::new(),
        json!({"providerCalls": calls.load(Ordering::SeqCst)}),
        None,
    )
}

fn run_transform() -> Value {
    let order = Arc::new(Mutex::new(Vec::<&'static str>::new()));
    let transform_order = order.clone();
    let transform: LegacyTransformFn = Arc::new(move |messages| {
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
    let calls = Arc::new(AtomicUsize::new(0));
    let provider_calls = calls.clone();
    let stream: LegacyStreamFn = Arc::new(move |_, _| {
        provider_calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(assistant("transformed", "stop")) })
    });
    let events = Events::default();
    let config = LegacyLoopConfig {
        model: model(),
        transform_context: Some(transform),
        convert_to_provider: converter,
        stream,
        cancellation: LegacyCancellation::new() as Arc<dyn Cancellation>,
    };
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(run_agent_loop(
            vec![user("hello")],
            LegacyContext::default(),
            config,
            Arc::new(events.clone()),
        ))
        .expect("transform loop succeeds");
    actual_observation(
        "single_turn.transform_context",
        &events.0.lock().unwrap(),
        &BTreeSet::new(),
        json!({
            "transformBeforeConvert": *order.lock().unwrap() == ["transform", "convert"],
            "providerCalls": calls.load(Ordering::SeqCst),
        }),
        None,
    )
}

fn run_parallel() -> Value {
    let executed_ids = Arc::new(Mutex::new(Vec::new()));
    let tool: LegacyTool = Arc::new(EchoTool {
        descriptor: echo_descriptor(),
        delay_ms: 20,
        slow_value: Some("first".to_owned()),
        executed_ids: executed_ids.clone(),
    });
    let calls = Arc::new(AtomicUsize::new(0));
    let provider_calls = calls.clone();
    let stream: LegacyStreamFn = Arc::new(move |_, _| {
        let call = provider_calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            if call == 0 {
                Ok(tool_assistant(
                    "toolUse",
                    &[("call-1", "first"), ("call-2", "second")],
                ))
            } else {
                Ok(assistant("done", "stop"))
            }
        })
    });
    let events = Events::default();
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(run_agent_loop_with_tools(
            vec![user("parallel")],
            LegacyContext {
                tools: vec![echo_descriptor()],
                ..LegacyContext::default()
            },
            config(stream),
            vec![tool],
            Arc::new(events.clone()),
        ))
        .expect("parallel loop succeeds");
    let executed: BTreeSet<_> = executed_ids.lock().unwrap().iter().cloned().collect();
    let event_guard = events.0.lock().unwrap();
    let (completion_order, source_order) = tool_orders(&event_guard);
    actual_observation(
        "tool.parallel_completion_source",
        &event_guard,
        &executed,
        json!({
            "completionMayDiffer": completion_order != source_order,
            "completionOrder": completion_order,
            "sourceOrder": source_order,
        }),
        None,
    )
}

fn run_truncated() -> Value {
    let executed_ids = Arc::new(Mutex::new(Vec::new()));
    let tool: LegacyTool = Arc::new(EchoTool {
        descriptor: echo_descriptor(),
        delay_ms: 0,
        slow_value: None,
        executed_ids: executed_ids.clone(),
    });
    let calls = Arc::new(AtomicUsize::new(0));
    let provider_calls = calls.clone();
    let stream: LegacyStreamFn = Arc::new(move |_, _| {
        let call = provider_calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            if call == 0 {
                Ok(tool_assistant("length", &[("call-1", "cut")]))
            } else {
                Ok(assistant("done", "stop"))
            }
        })
    });
    let events = Events::default();
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(run_agent_loop_with_tools(
            vec![user("truncated")],
            LegacyContext {
                tools: vec![echo_descriptor()],
                ..LegacyContext::default()
            },
            config(stream),
            vec![tool],
            Arc::new(events.clone()),
        ))
        .expect("truncated loop succeeds");
    let executed: BTreeSet<_> = executed_ids.lock().unwrap().iter().cloned().collect();
    let event_guard = events.0.lock().unwrap();
    let immediate_reason = truncation_reason(&event_guard);
    actual_observation(
        "tool.length_truncated",
        &event_guard,
        &executed,
        json!({
            "toolExecuted": !executed.is_empty(),
            "immediateReason": immediate_reason.clone().expect("length immediate result"),
        }),
        immediate_reason.as_deref(),
    )
}

fn run_continue() -> Value {
    let calls = Arc::new(AtomicUsize::new(0));
    let provider_calls = calls.clone();
    let stream: LegacyStreamFn = Arc::new(move |_, _| {
        provider_calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(assistant("continued", "stop")) })
    });
    let events = Events::default();
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(run_agent_loop_continue(
            LegacyContext {
                messages: vec![user("existing")],
                ..LegacyContext::default()
            },
            config(stream),
            Arc::new(events.clone()),
        ))
        .expect("continuation succeeds");
    let user_events = events
        .0
        .lock()
        .unwrap()
        .iter()
        .filter(|event| {
            matches!(
                event,
                AgentEvent::MessageStart {
                    message: LiveAgentMessage::Settled(AgentMessage::BuiltIn(Message::User(_)))
                }
            )
        })
        .count();
    actual_observation(
        "continue.existing_context",
        &events.0.lock().unwrap(),
        &BTreeSet::new(),
        json!({
            "replayedInput": false,
            "userMessageEvents": user_events,
            "providerCalls": calls.load(Ordering::SeqCst),
        }),
        None,
    )
}

fn compare_with_node(case_id: &str, actual: Value) {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let node = if Path::new("/usr/bin/node").exists() {
        "/usr/bin/node"
    } else {
        "node"
    };
    let mut child = Command::new(node)
        .current_dir(&root)
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("NODE_NO_WARNINGS", "1")
        .arg("conformance/agent-loop/compare-actual.mjs")
        .arg(case_id)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap_or_else(|error| panic!("{case_id}: failed to spawn Node comparator: {error}"));
    let bytes = serde_json::to_vec(&actual).expect("actual observation JSON");
    child
        .stdin
        .take()
        .expect("Node stdin")
        .write_all(&bytes)
        .unwrap_or_else(|error| panic!("{case_id}: failed to write actual observation: {error}"));
    let output = child
        .wait_with_output()
        .unwrap_or_else(|error| panic!("{case_id}: failed to wait for Node comparator: {error}"));
    assert!(
        output.status.success(),
        "{case_id}: Node comparator failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn actual_rust_agent_observations_compare_through_node_for_representable_rows() {
    let cases = [
        ("single_turn.stop", run_single_turn as fn() -> Value),
        (
            "single_turn.transform_context",
            run_transform as fn() -> Value,
        ),
        (
            "tool.parallel_completion_source",
            run_parallel as fn() -> Value,
        ),
        ("tool.length_truncated", run_truncated as fn() -> Value),
        ("continue.existing_context", run_continue as fn() -> Value),
    ];
    for (case_id, run) in cases {
        let actual = run();
        compare_with_node(case_id, actual);
    }
}
