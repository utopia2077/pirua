//! Representation bridge for the frozen source oracle. No expected scheduler
//! decisions are computed here: complete observed events/requests/timelines are
//! compared with the independent release fixture.
use pi_agent::*;
use pi_ai_core::{self as ai, *};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use std::time::Duration;

macro_rules! value {
    ($item:expr) => {
        serde_json::to_value($item).expect("serializable public DTO")
    };
}
pub(crate) use value;

pub fn normalize(mut value: Value) -> Value {
    // The source faux model accepts an empty endpoint. Rust's typed endpoint is
    // nonempty, so only these two injected-stream model identities use an inert
    // reserved URI. Real provider/model/endpoint values are never rewritten.
    let synthetic_model = value.get("api").is_some_and(|item| item == "oracle")
        && value.get("provider").is_some_and(|item| item == "oracle")
        && value
            .get("id")
            .is_some_and(|item| item == "oracle-model" || item == "replacement")
        && value
            .get("baseUrl")
            .is_some_and(|item| item == "https://oracle.invalid");
    match &mut value {
        Value::Object(fields) => {
            for (key, item) in fields {
                if key == "baseUrl" && synthetic_model {
                    *item = json!("");
                } else if key == "durationMs" {
                    if item.is_number() {
                        assert!(item.as_u64().is_some(), "nonnegative integer duration");
                    }
                    *item = json!("<durationMs>");
                } else if key == "timestamp"
                    && item.as_u64().is_some_and(|time| time >= 1_000_000_000_000)
                {
                    *item = json!(1700000000000_u64);
                } else if key == "rejections" {
                    for error in item.as_object_mut().unwrap().values_mut() {
                        match error.as_str().unwrap() {
                            "Busy"
                            | "Agent is already processing a prompt. Use steer() or followUp() to queue messages, or wait for completion."
                            | "Agent is already processing. Wait for completion before continuing."
                            | "Agent is already processing. Wait for completion before resetting." => {
                                *error = json!("Busy")
                            }
                            other => panic!("unexpected busy rejection: {other}"),
                        }
                    }
                } else {
                    *item = normalize(item.take());
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                *item = normalize(item.take());
            }
        }
        Value::Number(number) => {
            if let Some(float) = number.as_f64()
                && float >= 0.0
                && float.fract() == 0.0
                && float < u64::MAX as f64
            {
                value = json!(float as u64);
            }
        }
        _ => {}
    }
    value
}
#[derive(Clone)]
pub struct Record(Arc<Mutex<Value>>, Arc<Mutex<Value>>);
impl Record {
    pub fn new() -> Self {
        Self(
            Arc::new(Mutex::new(json!({"requests":[],"events":[],"timeline":[]}))),
            Arc::new(Mutex::new(json!({}))),
        )
    }
    pub fn register_config(&self, config: &AgentLoopConfig) {
        let mut meta = self.1.lock().unwrap();
        let agent = meta["agent"].as_bool().unwrap_or(false);
        let execution = meta["execution"].as_bool().unwrap_or(false);
        let mut callbacks = json!({"convertToLlm":"<callback>"});
        for (name, present) in [
            ("finishTurn", config.finish_turn.is_some()),
            ("prepareRequest", config.prepare_request.is_some()),
            ("prepareNextTurn", config.prepare_next_turn.is_some()),
            (
                "getSteeringMessages",
                agent || config.get_steering_messages.is_some(),
            ),
            (
                "getFollowUpMessages",
                agent || config.get_follow_up_messages.is_some(),
            ),
            ("beforeToolCall", config.tool_hooks.before.is_some()),
            ("afterToolCall", config.tool_hooks.after.is_some()),
            ("transformContext", config.transform_context.is_some()),
            ("getApiKey", config.get_api_key.is_some()),
        ] {
            if present {
                callbacks[name] = json!("<callback>");
            }
        }
        if execution {
            callbacks["toolExecution"] = value!(config.tool_execution);
        }
        meta["callbacks"] = callbacks;
    }
    pub fn set(&self, key: &str, item: Value) {
        self.0.lock().unwrap()[key] = normalize(item);
    }
    pub fn push(&self, key: &str, item: Value) -> usize {
        let mut data = self.0.lock().unwrap();
        let list = data[key].as_array_mut().unwrap();
        let index = list.len();
        list.push(normalize(item));
        index
    }
    pub fn mark(&self, kind: &str, fields: Value) {
        let mut fields = fields.as_object().unwrap().clone();
        fields.insert("kind".into(), json!(kind));
        self.push("timeline", Value::Object(fields));
    }
    pub fn event(&self, event: &AgentEvent) {
        let index = self.push("events", value!(event));
        self.mark("event", json!({"index":index}));
    }
    pub fn event_count(&self) -> usize {
        self.0.lock().unwrap()["events"].as_array().unwrap().len()
    }
    pub fn events(&self) -> Vec<Value> {
        self.0.lock().unwrap()["events"].as_array().unwrap().clone()
    }
    pub fn sink(&self) -> AgentEventSink {
        let rec = self.clone();
        Arc::new(move |event, _| {
            let rec = rec.clone();
            Box::pin(async move {
                rec.event(&event);
                Ok(())
            })
        })
    }
    pub fn finish(&self) -> Value {
        self.0.lock().unwrap().clone()
    }
}
#[derive(Clone)]
pub struct Gate(Arc<tokio::sync::Semaphore>);
impl Gate {
    pub fn new() -> Self {
        Self(Arc::new(tokio::sync::Semaphore::new(0)))
    }
    pub fn release(&self) {
        self.0.add_permits(1);
    }
    pub async fn wait(&self) {
        self.0.acquire().await.unwrap().forget();
    }
}
struct Clock;
impl ai::Clock for Clock {
    fn now_unix_ms(&self) -> u64 {
        1700000000000
    }
    fn sleep<'a>(
        &'a self,
        _: Duration,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), Cancelled>> {
        Box::pin(async { panic!("faux provider must not sleep") })
    }
}
pub fn model(id: &str) -> Model {
    serde_json::from_value(json!({"id":id,"name":id,"api":"oracle","provider":"oracle","baseUrl":"https://oracle.invalid",
    "reasoning":true,"input":["text","image"],"cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0},"contextWindow":10000,"maxTokens":1000})).unwrap()
}
pub fn user(text: &str) -> AgentMessage {
    serde_json::from_value(json!({"role":"user","content":text,"timestamp":11})).unwrap()
}
pub fn system(text: &str) -> AgentMessage {
    serde_json::from_value(json!({"role":"system","content":text,"timestamp":12})).unwrap()
}
pub fn usage() -> Value {
    json!({"input":1,"output":2,"cacheRead":0,"cacheWrite":0,"totalTokens":3,"cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"total":0}})
}
pub fn call(id: &str, name: &str, args: Value) -> ToolCall {
    serde_json::from_value(json!({"type":"toolCall","id":id,"name":name,"arguments":args})).unwrap()
}
pub fn assistant(content: Value, stop: &str) -> AssistantMessage {
    serde_json::from_value(json!({"role":"assistant","content":content,"api":"oracle","provider":"oracle","model":"oracle-model","usage":usage(),"stopReason":stop,"timestamp":13})).unwrap()
}
pub fn done() -> AssistantMessage {
    assistant(json!([{"type":"text","text":"done"}]), "stop")
}
pub fn calls(calls: &[ToolCall]) -> AssistantMessage {
    assistant(value!(calls), "toolUse")
}
pub fn text_result(text: &str) -> AgentToolResult {
    serde_json::from_value(json!({"content":[{"type":"text","text":text}],"details":{"text":text}}))
        .unwrap()
}
pub fn schema() -> Value {
    json!({"type":"object","properties":{"value":{"type":"string"}},"required":["value"],"additionalProperties":false})
}
pub fn tool(name: &str, execute: ToolExecute) -> AgentTool {
    AgentTool::new(serde_json::from_value(json!({"name":name,"description":format!("Tool {name}"),"parameters":{"type":"object","properties":{},"additionalProperties":false}})).unwrap(),execute)
}
pub fn echo(name: &str, text: &str) -> AgentTool {
    let text = text.to_owned();
    tool(
        name,
        Arc::new(move |_| {
            let text = text.clone();
            Box::pin(async move { Ok(text_result(&text)) })
        }),
    )
}
pub fn tool_value(tool: &AgentTool) -> Value {
    let mut result = value!(&tool.declaration);
    result["label"] = json!(tool.label);
    result["execute"] = json!("<callback>");
    if let Some(mode) = tool.execution_mode {
        result["executionMode"] = value!(mode);
    }
    if tool.prepare_arguments.is_some() {
        result["prepareArguments"] = json!("<callback>");
    }
    if let Some(schema) = &tool.output_schema {
        result["outputSchema"] = value!(schema);
    }
    result
}
pub fn context_value(context: &AgentContext) -> Value {
    json!({"messages":value!(&context.messages),"tools":context.tools.iter().map(tool_value).collect::<Vec<_>>()})
}
pub fn turn_value(turn: &AgentTurnContext) -> Value {
    json!({"message":value!(&turn.message),"toolResults":value!(&turn.tool_results),"context":context_value(&turn.context),"newMessages":value!(&turn.new_messages)})
}
pub fn request_value(request: &PrepareRequestContext) -> Value {
    json!({"context":context_value(&request.context),"model":value!(&request.model),"thinkingLevel":value!(request.thinking_level)})
}
pub fn outcome_value(outcome: &AgentToolCallOutcome) -> Value {
    let mut item = json!({"toolCall":value!(&outcome.tool_call),"result":value!(&outcome.result),"isError":outcome.is_error});
    if let Some(duration) = outcome.duration_ms {
        item["durationMs"] = json!(duration);
    }
    item
}

/// Controller-approved variance for exactly the plain JSON Schema `value:{}`
/// rejection in the validation matrix. Native jsonschema diagnostics retain the
/// path and keyword rather than TypeBox's sentence. Validate those semantics and
/// the original argument rendering before mapping this one sentence. Every other
/// error and every other outcome field remains subject to strict equality.
pub fn normalize_native_schema_invalid_diagnostic(outcome: &mut Value) {
    assert_eq!(outcome["toolCall"]["id"], "schema-invalid");
    assert_eq!(outcome["isError"], true);
    assert!(outcome.get("durationMs").is_none());
    let feedback = outcome["result"]["content"][0]["text"].as_str().unwrap();
    let rest = feedback
        .strip_prefix("Validation failed for tool \"echo\":\n  - value [type]: ")
        .expect("native diagnostic must retain exact tool/path/type keyword");
    let (diagnosis, arguments) = rest
        .split_once("\n\nReceived arguments:\n")
        .expect("native diagnostic must retain original arguments");
    assert!(!diagnosis.trim().is_empty(), "nonempty native diagnosis");
    assert!(
        !diagnosis.contains('\n'),
        "this input has exactly one schema issue"
    );
    assert_eq!(
        arguments,
        serde_json::to_string_pretty(&json!({"value":{}})).unwrap()
    );
    outcome["result"]["content"][0]["text"] = json!(
        "Validation failed for tool \"echo\":\n  - value: must be string\n\nReceived arguments:\n{\n  \"value\": {}\n}"
    );
}
pub fn state_value(agent: &Agent) -> Value {
    let state = agent.state();
    let mut item = json!({"systemPrompt":state.system_prompt,"model":value!(&state.model),"thinkingLevel":value!(state.thinking_level),
        "tools":state.tools.iter().map(tool_value).collect::<Vec<_>>(),"messages":value!(state.messages),"isStreaming":state.is_streaming,"pendingToolCalls":state.pending_tool_calls});
    if let Some(message) = state.streaming_message {
        item["streamingMessage"] = value!(message);
    }
    if let Some(error) = state.error_message {
        item["errorMessage"] = json!(error);
    }
    item
}
/// TS spreads its config into stream options; Rust separates runtime callback
/// handles from serialized provider options. Callback metadata is read from the
/// actual final config by register_config, never from a source-fixture answer.
pub fn config(
    rec: &Record,
    responses: Vec<AssistantMessage>,
    streamed: bool,
    agent: bool,
    execution: Option<ToolExecutionMode>,
) -> AgentLoopConfig {
    *rec.1.lock().unwrap() = json!({"agent":agent,"execution":execution.is_some()});
    let rec = rec.clone();
    let counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let stream: StreamFn = Arc::new(move |request| {
        let index = counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let rec = rec.clone();
        let message = responses
            .get(index)
            .expect("unexpected extra provider request")
            .clone();
        Box::pin(async move {
            let mut options = value!(&request.options);
            options["model"] = value!(&request.model);
            for (key, value) in rec.1.lock().unwrap()["callbacks"]
                .as_object()
                .expect("register actual config before run")
            {
                options[key] = value.clone();
            }
            if agent {
                options["signal"] = json!({"aborted":request.cancellation.is_cancelled()});
            }
            if let Some(key) = &request.api_key {
                options["apiKey"] = json!(key.expose_secret());
            }
            rec.push("requests",json!({"model":value!(&request.model),"context":value!(&request.context),"options":options}));
            rec.mark("request", json!({"index":index}));
            let (stream, producer) = create_assistant_message_event_stream(AssistantStreamInit {
                api: request.model.api,
                provider: request.model.provider,
                model: request.model.id,
                clock: Arc::new(Clock),
                cancellation: request.cancellation,
            });
            if streamed {
                let mut partial = message.clone();
                partial.content.clear();
                producer
                    .push(AssistantMessageEvent::Start {
                        partial: Box::new(partial),
                    })
                    .unwrap();
                producer
                    .push(AssistantMessageEvent::TextDelta {
                        content_index: SafeUnsigned::new(0).unwrap(),
                        delta: "done".into(),
                        partial: Box::new(message.clone()),
                    })
                    .unwrap();
            }
            let reason = value!(message.stop_reason);
            let event = if reason == "error" || reason == "aborted" {
                json!({"type":"error","reason":reason,"error":value!(message)})
            } else {
                json!({"type":"done","reason":reason,"message":value!(message)})
            };
            producer
                .push(serde_json::from_value(event).unwrap())
                .unwrap();
            Ok(stream)
        })
    });
    AgentLoopConfig::new(model("oracle-model"), stream)
}
pub async fn run_loop(
    rec: &Record,
    config: AgentLoopConfig,
    context: AgentContext,
    prompts: Vec<AgentMessage>,
    sink: Option<AgentEventSink>,
) {
    rec.register_config(&config);
    let messages = run_agent_loop(prompts, context, config, sink.unwrap_or_else(|| rec.sink()))
        .await
        .unwrap();
    rec.set("messages", value!(messages));
}
pub fn fixture(id: &str) -> Value {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../../conformance/agent-v1.4.0/observations.json"
    ))
    .unwrap();
    let mut case = fixture["observations"]
        .as_array()
        .unwrap()
        .iter()
        .find(|case| case["id"] == id)
        .unwrap()
        .clone();
    case.as_object_mut().unwrap().remove("id");
    case.as_object_mut().unwrap().remove("input");
    normalize(case)
}
pub fn compare(id: &str, actual: Value) {
    let expected = fixture(id);
    let actual = normalize(actual);
    if actual != expected {
        let event_order = |case: &Value| {
            case["events"].as_array().unwrap().iter().map(|event| json!({"type":event["type"],"call":event.get("toolCallId"),"role":event.get("message").and_then(|message|message.get("role"))})).collect::<Vec<_>>()
        };
        eprintln!("actual event order: {}", json!(event_order(&actual)));
        eprintln!("source event order: {}", json!(event_order(&expected)));
        fn first(actual: &Value, expected: &Value, path: String) -> Option<String> {
            match (actual, expected) {
                (Value::Object(a), Value::Object(e)) => {
                    for key in a.keys().chain(e.keys()) {
                        if a.get(key) != e.get(key) {
                            return first(
                                a.get(key).unwrap_or(&Value::Null),
                                e.get(key).unwrap_or(&Value::Null),
                                format!("{path}/{key}"),
                            );
                        }
                    }
                }
                (Value::Array(a), Value::Array(e)) => {
                    if a.len() != e.len() {
                        return Some(format!(
                            "{path} length actual={} source={}",
                            a.len(),
                            e.len()
                        ));
                    }
                    for (i, (a, e)) in a.iter().zip(e).enumerate() {
                        if a != e {
                            return first(a, e, format!("{path}/{i}"));
                        }
                    }
                }
                _ => return Some(format!("{path}: actual={actual} source={expected}")),
            }
            None
        }
        panic!(
            "{id}: {}",
            first(&actual, &expected, String::new())
                .unwrap_or_else(|| "object field presence differs".into())
        );
    }
}
