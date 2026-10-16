//! A complete prompt/tool/response run with an injected, offline model stream.
use pi_agent::{
    Agent, AgentEvent, AgentOptions, AgentTool, AgentToolResult, BoxFuture, CancellationToken,
};
use pi_ai_core::{
    AssistantMessage, AssistantMessageEvent, AssistantStreamInit, Cancelled, Clock, DoneReason,
    InputContent, Message, Model, TextContent, create_assistant_message_event_stream,
};
use serde_json::json;
use std::{sync::Arc, time::Duration};

struct ExampleClock;
impl Clock for ExampleClock {
    fn now_unix_ms(&self) -> u64 {
        1
    }
    fn sleep<'a>(
        &'a self,
        _: Duration,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), Cancelled>> {
        Box::pin(async { Ok(()) })
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tokio::runtime::Builder::new_current_thread().enable_all().build()?.block_on(async {
        let model: Model = serde_json::from_value(json!({
            "id":"offline", "name":"Offline example", "api":"openai-responses", "provider":"openai",
            "baseUrl":"https://example.invalid/v1", "reasoning":false, "input":["text"],
            "cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0}, "contextWindow":10000,"maxTokens":1000
        }))?;
        let stream = Arc::new(|request: pi_agent::AgentRequest| -> BoxFuture<'static, Result<_, pi_agent::AgentError>> {
            Box::pin(async move {
                let finished_tool = request.context.messages.iter().any(|message| matches!(message, Message::ToolResult(_)));
                let (stream, producer) = create_assistant_message_event_stream(AssistantStreamInit {
                    api: request.model.api.clone(), provider: request.model.provider.clone(), model: request.model.id.clone(),
                    clock: Arc::new(ExampleClock), cancellation: request.cancellation,
                });
                let content = if finished_tool { json!([{"type":"text","text":"The local tool reports: ready."}]) }
                    else { json!([{"type":"toolCall","id":"status-1","name":"status","arguments":{}}]) };
                let message: AssistantMessage = serde_json::from_value(json!({
                    "role":"assistant", "content":content, "api":request.model.api, "provider":request.model.provider,
                    "model":request.model.id, "usage":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"totalTokens":0,
                        "cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"total":0}},
                    "stopReason":if finished_tool {"stop"} else {"toolUse"}, "timestamp":1
                })).expect("static example response");
                producer.push(AssistantMessageEvent::Done {
                    reason: if finished_tool { DoneReason::Stop } else { DoneReason::ToolUse }, message: Box::new(message),
                }).expect("one terminal event");
                Ok(stream)
            })
        });
        let mut options = AgentOptions::new(model, stream);
        options.initial_state.system_prompt = Some("Use the local status tool to answer.".into());
        options.initial_state.tools.push(AgentTool::new(
            serde_json::from_value(json!({"name":"status","description":"Read local status","parameters":{"type":"object","properties":{},"additionalProperties":false}}))?,
            Arc::new(|_| Box::pin(async { Ok(AgentToolResult {
                content: vec![InputContent::Text(TextContent { content_type: Default::default(), text: "ready".into(), text_signature: None })],
                ..Default::default()
            }) })),
        ));
        let agent = Agent::new(options);
        let subscription = agent.subscribe(Arc::new(|event, _| Box::pin(async move {
            if let AgentEvent::ToolExecutionEnd { tool_name, is_error, .. } = event {
                println!("tool {tool_name}: {}", if is_error { "failed" } else { "completed" });
            }
            Ok(())
        })));
        agent.prompt_text("Check the status.", Vec::new()).await?;
        agent.unsubscribe(subscription);
        println!("Agent settled with {} transcript messages.", agent.state().messages.len());
        Ok::<_, Box<dyn std::error::Error>>(())
    })
}
