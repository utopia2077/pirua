#![allow(dead_code)]
use pi_agent::*;
use pi_ai::*;
use serde_json::{Value, json};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};
pub fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}
pub fn model() -> Model {
    serde_json::from_value(json!({"id":"fixture","name":"Fixture","api":"openai-responses","provider":"openai","baseUrl":"https://fixture.invalid/v1","reasoning":false,"input":["text","image"],"cost":{"input":1,"output":2,"cacheRead":0.1,"cacheWrite":1.25},"contextWindow":100000,"maxTokens":10000})).unwrap()
}
struct FixtureClock;
impl Clock for FixtureClock {
    fn now_unix_ms(&self) -> u64 {
        7
    }
    fn sleep<'a>(
        &'a self,
        _: std::time::Duration,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), Cancelled>> {
        Box::pin(async { Ok(()) })
    }
}
pub fn response(request: AgentRequest, value: Value) -> AssistantStream {
    let (stream, producer) = create_assistant_message_event_stream(AssistantStreamInit {
        api: request.model.api.clone(),
        provider: request.model.provider.clone(),
        model: request.model.id.clone(),
        clock: Arc::new(FixtureClock),
        cancellation: request.cancellation,
    });
    let mut message = failure_message(&request.model, &AgentError::Callback(String::new()), false);
    message.stop_reason =
        serde_json::from_value(value.get("stopReason").cloned().unwrap_or(json!("stop"))).unwrap();
    message.error_message = value["errorMessage"].as_str().map(str::to_owned);
    message.content = serde_json::from_value(
        value
            .get("content")
            .cloned()
            .unwrap_or(json!([{ "type":"text","text":"ok"}])),
    )
    .unwrap();
    if let Some(usage) = value.get("usage") {
        message.usage = serde_json::from_value(usage.clone()).unwrap();
    }
    let reason = match message.stop_reason {
        StopReason::ToolUse => Some(DoneReason::ToolUse),
        StopReason::Length => Some(DoneReason::Length),
        StopReason::Stop => Some(DoneReason::Stop),
        _ => None,
    };
    if let Some(reason) = reason {
        producer
            .push(AssistantMessageEvent::Done {
                reason,
                message: Box::new(message),
            })
            .unwrap();
    } else {
        producer
            .push(AssistantMessageEvent::Error {
                reason: if message.stop_reason == StopReason::Aborted {
                    ErrorReason::Aborted
                } else {
                    ErrorReason::Error
                },
                error: Box::new(message),
            })
            .unwrap();
    }
    stream
}
pub fn scripted(values: Vec<Value>) -> (StreamFn, Arc<Mutex<Vec<AgentRequest>>>) {
    let values = Arc::new(Mutex::new(VecDeque::from(values)));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let capture = requests.clone();
    let stream = Arc::new(move |request: AgentRequest| {
        let values = values.clone();
        let capture = capture.clone();
        Box::pin(async move {
            capture.lock().unwrap().push(request.clone());
            let value = values
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected provider call");
            Ok(response(request, value))
        }) as pi_ai::BoxFuture<'static, Result<AssistantStream, AgentError>>
    });
    (stream, requests)
}
pub fn tool(name: &str, execute: ToolExecute) -> AgentTool {
    AgentTool::new(serde_json::from_value(json!({"name":name,"description":name,"parameters":{"type":"object","properties":{"path":{"type":"string"}},"additionalProperties":true}})).unwrap(),execute)
}
pub fn text_result(text: &str) -> AgentToolResult {
    AgentToolResult {
        content: vec![InputContent::Text(TextContent {
            content_type: Default::default(),
            text: text.into(),
            text_signature: None,
        })],
        ..Default::default()
    }
}
pub fn usage(input: u64) -> Value {
    json!({"input":input,"output":1,"cacheRead":0,"cacheWrite":0,"totalTokens":input+1,"cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"total":0}})
}
