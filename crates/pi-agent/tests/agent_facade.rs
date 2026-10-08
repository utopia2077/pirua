use std::sync::{Arc, Mutex};
use std::time::Duration;

use pi_agent::{
    Agent, AgentListener, AgentOptions, LegacyAgentError, LegacyContext, LegacyConvertFn,
    LegacyHooksHandle, LegacyStreamFn, LegacyTool, ToolPhaseError, ToolPhaseHooks,
};
use pi_agent_schema::{
    AgentEvent, AgentHarnessTool, AgentMessage, AgentTool, AgentToolResult, BeforeToolResult,
    BoxFuture, Cancellation, ErrorContext, ExecutedToolOutcome, QueueMode, SettledAssistantMessage,
    ToolExecutionError, ToolProgressSink,
};
use pi_ai_core::{InputContent, JsonObject, JsonValue, Message, Model, TextContent, ToolCallId};

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
    .expect("valid model")
}

fn user(text: &str) -> AgentMessage {
    serde_json::from_value(serde_json::json!({
        "role": "user",
        "content": text,
        "timestamp": 0
    }))
    .expect("valid user")
}

fn assistant(text: &str) -> pi_agent_schema::SettledAssistantMessage {
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
        "stopReason": "stop",
        "timestamp": 0
    }))
    .expect("valid assistant")
}

fn tool_assistant() -> SettledAssistantMessage {
    serde_json::from_value(serde_json::json!({
        "role": "assistant",
        "content": [{
            "type": "toolCall",
            "id": "call-1",
            "name": "echo",
            "arguments": {"value": "from-tool"}
        }],
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
        "stopReason": "toolUse",
        "timestamp": 0
    }))
    .expect("valid tool assistant")
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

fn options(stream: LegacyStreamFn) -> AgentOptions {
    AgentOptions {
        model: model(),
        system_prompt: Some("system".to_owned()),
        stream,
        convert_to_provider: converter(),
        transform_context: None,
        tools: Vec::new(),
        hooks: None,
    }
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
                    JsonValue::String(value) => Some(value.clone()),
                    _ => None,
                })
                .unwrap_or_default();
            self.seen.lock().unwrap().push(value.clone());
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

struct RecordingHooks {
    events: Arc<Mutex<Vec<&'static str>>>,
}

impl ToolPhaseHooks<LegacyContext> for RecordingHooks {
    fn before<'a>(
        &'a self,
        _call: &'a pi_ai_core::ToolCall,
        _args: &'a JsonObject,
        _context: &'a LegacyContext,
        _cancellation: &'a dyn Cancellation,
    ) -> BoxFuture<'a, Result<Option<BeforeToolResult>, ToolPhaseError>> {
        let events = self.events.clone();
        Box::pin(async move {
            events.lock().unwrap().push("before");
            Ok(None)
        })
    }

    fn after<'a>(
        &'a self,
        _call: &'a pi_ai_core::ToolCall,
        _args: &'a JsonObject,
        _outcome: &'a ExecutedToolOutcome,
        _context: &'a LegacyContext,
        _cancellation: &'a dyn Cancellation,
    ) -> BoxFuture<'a, Result<Option<pi_agent_schema::AfterToolResult>, ToolPhaseError>> {
        let events = self.events.clone();
        Box::pin(async move {
            events.lock().unwrap().push("after");
            Ok(None)
        })
    }
}

fn options_with_tool_and_hooks(
    stream: LegacyStreamFn,
    tool: LegacyTool,
    hooks: LegacyHooksHandle,
) -> AgentOptions {
    AgentOptions {
        tools: vec![tool],
        hooks: Some(hooks),
        ..options(stream)
    }
}

#[test]
fn prompt_reduces_state_and_listener_order_is_awaited() {
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let listener_log = Arc::new(Mutex::new(Vec::<&'static str>::new()));
        let first_log = listener_log.clone();
        let first: AgentListener = Arc::new(move |event, _| {
            let first_log = first_log.clone();
            Box::pin(async move {
                if matches!(event, AgentEvent::AgentEnd { .. }) {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                first_log.lock().unwrap().push("first");
                Ok(())
            })
        });
        let second_log = listener_log.clone();
        let second: AgentListener = Arc::new(move |_event, _| {
            let second_log = second_log.clone();
            Box::pin(async move {
                second_log.lock().unwrap().push("second");
                Ok(())
            })
        });
        let stream: LegacyStreamFn = Arc::new(|_, _| Box::pin(async { Ok(assistant("done")) }));
        let agent = Agent::new(options(stream));
        agent.subscribe(first);
        agent.subscribe(second);

        agent
            .prompt(vec![user("hello")])
            .await
            .expect("prompt succeeds");
        let state = agent.state();
        assert!(!state.is_streaming);
        assert_eq!(state.messages.len(), 2);
        let log = listener_log.lock().unwrap();
        assert_eq!(log.len(), 16);
        assert!(log.chunks(2).all(|chunk| chunk == ["first", "second"]));
    });
}

#[test]
fn abort_propagates_signal_and_reset_clears_transcript() {
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let stream: LegacyStreamFn = Arc::new(|_, cancellation| {
            Box::pin(async move {
                cancellation.cancelled().await;
                Err(LegacyAgentError::Provider("cancelled".to_owned()))
            })
        });
        let agent = Agent::new(options(stream));
        let running = {
            let agent = agent.clone();
            tokio::spawn(async move { agent.prompt(vec![user("wait")]).await })
        };
        let idle_waiter = {
            let agent = agent.clone();
            tokio::spawn(async move { agent.wait_for_idle().await })
        };
        for _ in 0..1000 {
            if agent.state().is_streaming {
                break;
            }
            tokio::task::yield_now().await;
        }
        if !agent.state().is_streaming {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert!(agent.state().is_streaming);
        agent.abort();
        running
            .await
            .expect("task joins")
            .expect("abort is in-band");
        idle_waiter.await.expect("idle waiter joins");
        assert!(!agent.state().is_streaming);
        assert!(agent.state().messages.iter().any(|message| {
            matches!(
                message,
                AgentMessage::BuiltIn(Message::Assistant(message))
                    if message.stop_reason == pi_ai_core::StopReason::Aborted
            )
        }));
        agent.reset().expect("idle reset succeeds");
        assert!(agent.state().messages.is_empty());
    });
}

#[test]
fn reentrancy_is_rejected_while_a_run_is_active() {
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let started = Arc::new(tokio::sync::Notify::new());
        let stream: LegacyStreamFn = {
            let started = started.clone();
            Arc::new(move |_, cancellation| {
                started.notify_one();
                Box::pin(async move {
                    cancellation.cancelled().await;
                    Err(LegacyAgentError::Provider("cancelled".to_owned()))
                })
            })
        };
        let agent = Agent::new(options(stream));
        let running = {
            let agent = agent.clone();
            tokio::spawn(async move { agent.prompt(vec![user("first")]).await })
        };
        tokio::time::timeout(Duration::from_secs(1), started.notified())
            .await
            .expect("first run must enter the provider stream");
        let error = agent
            .prompt(vec![user("second")])
            .await
            .expect_err("second prompt must be rejected");
        assert!(matches!(error, LegacyAgentError::InvalidContext(_)));
        agent.abort();
        running
            .await
            .expect("task joins")
            .expect("abort is in-band");
    });
}

#[test]
fn follow_up_all_mode_delivers_all_queued_messages_on_next_turn() {
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let stream: LegacyStreamFn = {
            let calls = calls.clone();
            Arc::new(move |request, _| {
                let call = calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Box::pin(async move {
                    if call == 0 {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    } else {
                        let users = request
                            .messages
                            .iter()
                            .filter_map(|message| match message {
                                Message::User(user) => match &user.content {
                                    pi_ai_core::UserContent::Text(text) => Some(text.clone()),
                                    _ => None,
                                },
                                _ => None,
                            })
                            .collect::<Vec<_>>();
                        assert!(users.iter().any(|text| text == "one"));
                        assert!(users.iter().any(|text| text == "two"));
                    }
                    Ok(assistant("done"))
                })
            })
        };
        let agent = Agent::new(options(stream));
        agent.set_follow_up_mode(QueueMode::All);
        let running = {
            let agent = agent.clone();
            tokio::spawn(async move { agent.prompt(vec![user("start")]).await })
        };
        for _ in 0..1000 {
            if agent.state().is_streaming {
                break;
            }
            tokio::task::yield_now().await;
        }
        agent.follow_up(user("one")).expect("active follow-up");
        agent.follow_up(user("two")).expect("active follow-up");
        running.await.expect("task joins").expect("prompt succeeds");
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    });
}

#[test]
fn public_hooks_reach_legacy_tool_prepare_and_finalize() {
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let stream: LegacyStreamFn = {
            let calls = calls.clone();
            Arc::new(move |_, _| {
                let call = calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Box::pin(async move {
                    if call == 0 {
                        Ok(tool_assistant())
                    } else {
                        Ok(assistant("done"))
                    }
                })
            })
        };
        let seen = Arc::new(Mutex::new(Vec::new()));
        let tool: LegacyTool = Arc::new(EchoTool {
            descriptor: echo_descriptor(),
            seen: seen.clone(),
        });
        let hook_events = Arc::new(Mutex::new(Vec::new()));
        let hooks: LegacyHooksHandle = Arc::new(RecordingHooks {
            events: hook_events.clone(),
        });
        let agent = Agent::new(options_with_tool_and_hooks(stream, tool, hooks));

        agent
            .prompt(vec![user("run tool")])
            .await
            .expect("prompt succeeds");

        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert_eq!(&*seen.lock().unwrap(), &["from-tool".to_owned()]);
        assert_eq!(&*hook_events.lock().unwrap(), &["before", "after"]);
        assert_eq!(agent.state().messages.len(), 4);
    });
}
