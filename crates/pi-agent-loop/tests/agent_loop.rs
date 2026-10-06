use std::sync::{Arc, Mutex};

use pi_agent_loop::{
    LegacyAgentError, LegacyCancellation, LegacyContext, LegacyConvertFn, LegacyLoopConfig,
    LegacyStreamFn, LegacyTransformFn, run_agent_loop, run_agent_loop_continue,
};
use pi_agent_schema::{
    AgentEvent, AgentEventSink, AgentMessage, BoxFuture, Cancellation, HarnessRuntimeError,
    SettledAssistantMessage,
};
use pi_ai_core::{Message, Model};

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
