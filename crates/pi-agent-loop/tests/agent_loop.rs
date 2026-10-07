use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use pi_agent_loop::{
    LegacyAgentError, LegacyCancellation, LegacyContext, LegacyConvertFn, LegacyLoopConfig,
    LegacyLoopControl, LegacyStreamFn, LegacyTool, LegacyTransformFn, run_agent_loop,
    run_agent_loop_continue, run_agent_loop_with_tools, run_agent_loop_with_tools_and_control,
    run_agent_loop_with_tools_and_mode,
};
use pi_agent_schema::{
    AgentEvent, AgentEventSink, AgentHarnessTool, AgentMessage, AgentTool, AgentToolResult,
    BoxFuture, Cancellation, ErrorContext, HarnessRuntimeError, SettledAssistantMessage,
    ToolExecutionError, ToolProgressSink,
};
use pi_ai_core::{InputContent, JsonObject, JsonValue, Message, Model, TextContent};

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
    serde_json::from_value(serde_json::json!({
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
    }))
    .expect("valid assistant fixture")
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

fn echo_descriptor(execution_mode: Option<&str>) -> AgentTool {
    let mut value = serde_json::json!({
        "name": "echo",
        "label": "Echo",
        "description": "Echo",
        "parameters": {
            "type": "object",
            "properties": {"value": {"type": "string"}},
            "required": ["value"],
            "additionalProperties": false
        }
    });
    if let Some(execution_mode) = execution_mode {
        value["executionMode"] = serde_json::Value::String(execution_mode.to_owned());
    }
    serde_json::from_value(value).expect("valid tool descriptor")
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
        _tool_call_id: pi_ai_core::ToolCallId,
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
                tokio::time::sleep(std::time::Duration::from_millis(self.delay_ms)).await;
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

fn config(
    stream: LegacyStreamFn,
    transform_context: Option<LegacyTransformFn>,
    convert_to_provider: LegacyConvertFn,
) -> LegacyLoopConfig {
    LegacyLoopConfig {
        model: model(),
        transform_context,
        convert_to_provider,
        stream,
        cancellation: LegacyCancellation::new() as Arc<dyn Cancellation>,
    }
}

fn identity_converter() -> LegacyConvertFn {
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

fn event_names(events: &[AgentEvent]) -> Vec<&'static str> {
    events
        .iter()
        .map(|event| match event {
            AgentEvent::AgentStart => "agent_start",
            AgentEvent::TurnStart => "turn_start",
            AgentEvent::MessageStart { .. } => "message_start",
            AgentEvent::MessageEnd { .. } => "message_end",
            AgentEvent::TurnEnd { .. } => "turn_end",
            AgentEvent::AgentEnd { .. } => "agent_end",
            _ => "other",
        })
        .collect()
}

fn require_trace(actual: &[&str], expected: &[&str]) -> Result<(), String> {
    if actual == expected {
        Ok(())
    } else {
        Err(format!(
            "trace mismatch: actual={actual:?}, expected={expected:?}"
        ))
    }
}

#[test]
fn prompt_emits_legacy_lifecycle_and_returns_prompt_plus_assistant() {
    let stream: LegacyStreamFn =
        Arc::new(|_, _| Box::pin(async { Ok(assistant("hello", "stop")) }));
    let sink = Events::default();
    let produced = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(run_agent_loop(
            vec![user("hi")],
            LegacyContext::default(),
            config(stream, None, identity_converter()),
            Arc::new(sink.clone()),
        ))
        .expect("loop succeeds");

    assert_eq!(produced.len(), 2);
    let events = sink.0.lock().unwrap();
    let names = event_names(&events);
    require_trace(
        &names,
        &[
            "agent_start",
            "turn_start",
            "message_start",
            "message_end",
            "message_start",
            "message_end",
            "turn_end",
            "agent_end",
        ],
    )
    .expect("normal lifecycle trace");
}

#[test]
fn transform_runs_before_conversion() {
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
    let stream: LegacyStreamFn = Arc::new(|_, _| Box::pin(async { Ok(assistant("ok", "stop")) }));

    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(run_agent_loop(
            vec![user("hi")],
            LegacyContext::default(),
            config(stream, Some(transform), converter),
            Arc::new(Events::default()),
        ))
        .expect("loop succeeds");

    assert_eq!(&*order.lock().unwrap(), &["transform", "convert"]);
}

#[test]
fn provider_failure_is_terminalized_as_error_assistant() {
    let stream: LegacyStreamFn = Arc::new(|_, _| {
        Box::pin(async {
            Err(LegacyAgentError::Provider(
                "synthetic provider failure".to_owned(),
            ))
        })
    });
    let sink = Events::default();
    let produced = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(run_agent_loop(
            vec![user("hi")],
            LegacyContext::default(),
            config(stream, None, identity_converter()),
            Arc::new(sink.clone()),
        ))
        .expect("provider failure is an in-band assistant result");

    let AgentMessage::BuiltIn(Message::Assistant(message)) = produced.last().unwrap() else {
        panic!("expected assistant failure");
    };
    assert_eq!(message.stop_reason, pi_ai_core::StopReason::Error);
    assert_eq!(
        message.error_message.as_deref(),
        Some("legacy provider failed: synthetic provider failure")
    );
    assert!(event_names(&sink.0.lock().unwrap()).ends_with(&["turn_end", "agent_end"]));
}

#[test]
fn continuation_rejects_assistant_and_causal_event_swap_is_detectable() {
    let stream: LegacyStreamFn = Arc::new(|_, _| Box::pin(async { Ok(assistant("ok", "stop")) }));
    let sink = Events::default();
    let context = LegacyContext {
        messages: vec![user("resume")],
        ..LegacyContext::default()
    };
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(run_agent_loop_continue(
            context,
            config(stream, None, identity_converter()),
            Arc::new(sink.clone()),
        ))
        .expect("continuation succeeds");

    let names = event_names(&sink.0.lock().unwrap());
    assert_eq!(names[0..2], ["agent_start", "turn_start"]);
    let expected = [
        "agent_start",
        "turn_start",
        "message_start",
        "message_end",
        "turn_end",
        "agent_end",
    ];
    require_trace(&names, &expected).expect("continuation trace");
    let mut mutated = names.clone();
    mutated.swap(0, 1);
    assert!(
        require_trace(&mutated, &expected).is_err(),
        "causal event-order mutation must be rejected"
    );

    let assistant_message = AgentMessage::BuiltIn(Message::Assistant(Box::new(
        assistant("already done", "stop").into_message(),
    )));
    let assistant_context = LegacyContext {
        messages: vec![assistant_message],
        ..LegacyContext::default()
    };
    let error = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(run_agent_loop_continue(
            assistant_context,
            config(
                Arc::new(|_, _| Box::pin(async { Ok(assistant("unused", "stop")) })),
                None,
                identity_converter(),
            ),
            Arc::new(Events::default()),
        ))
        .expect_err("assistant context must be rejected");
    assert!(matches!(error, LegacyAgentError::InvalidContext(_)));
}

#[test]
fn tool_batch_keeps_completion_end_order_and_source_result_order() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let tool: LegacyTool = Arc::new(EchoTool {
        descriptor: echo_descriptor(None),
        delay_ms: 20,
        slow_value: Some("first".to_owned()),
        seen: seen.clone(),
    });
    let call_index = Arc::new(AtomicUsize::new(0));
    let stream: LegacyStreamFn = {
        let call_index = call_index.clone();
        Arc::new(move |_, _| {
            let index = call_index.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                if index == 0 {
                    Ok(tool_assistant(
                        "toolUse",
                        &[("tool-1", "first"), ("tool-2", "second")],
                    ))
                } else {
                    Ok(assistant("done", "stop"))
                }
            })
        })
    };
    let sink = Events::default();
    let produced = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(run_agent_loop_with_tools(
            vec![user("run tools")],
            LegacyContext {
                tools: vec![echo_descriptor(None)],
                ..LegacyContext::default()
            },
            config(stream, None, identity_converter()),
            vec![tool],
            Arc::new(sink.clone()),
        ))
        .expect("tool loop succeeds");

    assert_eq!(
        produced
            .iter()
            .filter(|m| matches!(m, AgentMessage::BuiltIn(Message::ToolResult(_))))
            .count(),
        2
    );
    let events = sink.0.lock().unwrap();
    let end_ids: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            AgentEvent::ToolExecutionEnd { tool_call_id, .. } => Some(tool_call_id.as_str()),
            _ => None,
        })
        .collect();
    let result_ids: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            AgentEvent::MessageEnd {
                message: AgentMessage::BuiltIn(Message::ToolResult(message)),
            } => Some(message.tool_call_id.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(end_ids, ["tool-2", "tool-1"]);
    assert_eq!(result_ids, ["tool-1", "tool-2"]);
    assert_eq!(
        &*seen.lock().unwrap(),
        &["first".to_owned(), "second".to_owned()]
    );
}

#[test]
fn length_truncated_tool_calls_never_execute_and_still_pair_results() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let tool: LegacyTool = Arc::new(EchoTool {
        descriptor: echo_descriptor(None),
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
    let sink = Events::default();
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(run_agent_loop_with_tools(
            vec![user("truncated")],
            LegacyContext {
                tools: vec![echo_descriptor(None)],
                ..LegacyContext::default()
            },
            config(stream, None, identity_converter()),
            vec![tool],
            Arc::new(sink.clone()),
        ))
        .expect("length result is in-band");

    assert!(seen.lock().unwrap().is_empty());
    let events = sink.0.lock().unwrap();
    let end = events.iter().find_map(|event| match event {
        AgentEvent::ToolExecutionEnd {
            result, is_error, ..
        } => Some((result.clone(), *is_error)),
        _ => None,
    });
    assert!(end.map(|(_, is_error)| is_error).unwrap_or(false));
}

#[test]
fn explicit_sequential_mode_serializes_tool_execution() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let tool: LegacyTool = Arc::new(EchoTool {
        descriptor: echo_descriptor(None),
        delay_ms: 10,
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
                    Ok(tool_assistant(
                        "toolUse",
                        &[("tool-1", "first"), ("tool-2", "second")],
                    ))
                } else {
                    Ok(assistant("done", "stop"))
                }
            })
        })
    };
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(run_agent_loop_with_tools_and_mode(
            vec![user("run sequentially")],
            LegacyContext {
                tools: vec![echo_descriptor(None)],
                ..LegacyContext::default()
            },
            config(stream, None, identity_converter()),
            vec![tool],
            pi_agent_schema::ToolExecutionMode::Sequential,
            Arc::new(Events::default()),
        ))
        .expect("sequential loop succeeds");

    assert_eq!(
        &*seen.lock().unwrap(),
        &["first".to_owned(), "second".to_owned()]
    );
}

#[test]
fn steering_queue_is_delivered_after_tool_turn_before_next_provider_call() {
    let tool: LegacyTool = Arc::new(EchoTool {
        descriptor: echo_descriptor(None),
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
                        matches!(
                            message,
                            Message::User(user)
                                if matches!(&user.content, pi_ai_core::UserContent::Text(text) if text == "steer")
                        )
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
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(run_agent_loop_with_tools_and_control(
            vec![user("start")],
            LegacyContext {
                tools: vec![echo_descriptor(None)],
                ..LegacyContext::default()
            },
            config(stream, None, identity_converter()),
            vec![tool],
            Some(control),
            Arc::new(sink.clone()),
        ))
        .expect("steering loop succeeds");

    let events = sink.0.lock().unwrap();
    let turn_end_index = events
        .iter()
        .position(|event| matches!(event, AgentEvent::TurnEnd { tool_results, .. } if !tool_results.is_empty()))
        .expect("tool turn end");
    let steering_index = events
        .iter()
        .position(|event| {
            matches!(
                event,
                AgentEvent::MessageStart {
                    message: pi_agent_schema::LiveAgentMessage::Settled(
                        AgentMessage::BuiltIn(Message::User(user))
                    )
                } if matches!(&user.content, pi_ai_core::UserContent::Text(text) if text == "steer")
            )
        })
        .expect("steering message");
    assert!(turn_end_index < steering_index);
}
