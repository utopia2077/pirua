//! Explicit deterministic native provider, selected only by --fixture-provider.
use pi_agent::{AgentError, StreamFn};
use pi_ai::*;
use pi_coding_runtime::*;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};
pub fn model() -> Model {
    serde_json::from_value(json!({"id":"fixture","name":"Deterministic fixture","api":"openai-responses","provider":"openai","baseUrl":"https://example.invalid/v1","reasoning":true,"input":["text","image"],"cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0},"contextWindow":100000,"maxTokens":1000})).expect("fixture model")
}
pub struct Hooks;
impl SessionHooks for Hooks {
    fn input(
        &self,
        event: InputEvent,
        c: CancellationToken,
    ) -> BoxFuture<'_, Result<InputResult, AgentError>> {
        Box::pin(async move {
            if event.text.contains("fixture:before-insert") {
                c.cancelled().await;
                return Err(AgentError::Callback("Input cancelled".into()));
            }
            if event.text.contains("fixture:handled") {
                return Ok(InputResult::Handled);
            }
            if let Some(text) = event.text.strip_prefix("fixture:transform ") {
                return Ok(InputResult::Transform {
                    text: format!("Transformed: {text}"),
                    images: None,
                });
            }
            Ok(InputResult::Continue)
        })
    }
    fn before_compact(
        &self,
        p: CompactionPreparation,
        _: CompactionReason,
        _: CancellationToken,
    ) -> BoxFuture<'_, Result<BeforeCompactionResult, AgentError>> {
        Box::pin(async move {
            Ok(BeforeCompactionResult {
                cancel: false,
                compaction: Some(CompactionResult {
                    summary: "Fixture compaction summary".into(),
                    first_kept_entry_id: Some(p.first_kept_entry_id),
                    tokens_before: p.tokens_before,
                    details: Some(p.file_ops.details()),
                    usage: None,
                }),
            })
        })
    }
}
pub fn stream(delay_ms: u64) -> StreamFn {
    let attempts = Arc::new(Mutex::new(BTreeMap::<String, usize>::new()));
    Arc::new(move |request| {
        let attempts = attempts.clone();
        Box::pin(async move {
            let context = serde_json::to_value(request.context.into_messages()).expect("context");
            let user = context
                .as_array()
                .and_then(|a| a.iter().rev().find(|m| m["role"] == "user"))
                .cloned()
                .unwrap_or(Value::Null);
            let text = if let Some(t) = user["content"].as_str() {
                t.to_owned()
            } else {
                user["content"]
                    .as_array()
                    .map(|a| {
                        a.iter()
                            .filter_map(|b| b["text"].as_str())
                            .collect::<Vec<_>>()
                            .join("\n")
                    })
                    .unwrap_or_default()
            };
            let tool_done = context
                .as_array()
                .and_then(|a| a.last())
                .is_some_and(|m| m["role"] == "toolResult");
            let attempt = {
                let mut a = attempts.lock().unwrap();
                let n = a.entry(text.clone()).or_default();
                *n += 1;
                *n
            };
            let clock = Arc::new(pi_ai_transport::TokioClock::new(
                tokio::runtime::Handle::current(),
            ));
            let (stream, producer) = create_assistant_message_event_stream(AssistantStreamInit {
                api: request.model.api.clone(),
                provider: request.model.provider.clone(),
                model: request.model.id.clone(),
                clock,
                cancellation: request.cancellation.clone(),
            });
            tokio::spawn(async move {
                let mut message = pi_agent::failure_message(
                    &request.model,
                    &AgentError::Callback(String::new()),
                    false,
                );
                message.error_message = None;
                message.stop_reason = StopReason::Pending;
                message.content = vec![];
                let _ = producer.push(AssistantMessageEvent::Start {
                    partial: Box::new(message.clone()),
                });
                let cancellation = request.cancellation;
                if text.contains("fixture:block") {
                    cancellation.cancelled().await;
                } else {
                    tokio::select! {_=cancellation.cancelled()=>{},_=tokio::time::sleep(Duration::from_millis(delay_ms))=>{}}
                }
                if cancellation.is_cancelled() {
                    let message = pi_agent::failure_message(
                        &request.model,
                        &AgentError::Callback("Fixture cancelled".into()),
                        true,
                    );
                    let _ = producer.push(AssistantMessageEvent::Error {
                        reason: ErrorReason::Aborted,
                        error: Box::new(message),
                    });
                    return;
                }
                if text.contains("fixture:retry") && attempt == 1 {
                    let message = pi_agent::failure_message(
                        &request.model,
                        &AgentError::Callback("429 rate limit fixture, retry".into()),
                        false,
                    );
                    let _ = producer.push(AssistantMessageEvent::Error {
                        reason: ErrorReason::Error,
                        error: Box::new(message),
                    });
                    return;
                }
                if text.contains("fixture:error") {
                    let message = pi_agent::failure_message(
                        &request.model,
                        &AgentError::Callback("Fixture provider failure".into()),
                        false,
                    );
                    let _ = producer.push(AssistantMessageEvent::Error {
                        reason: ErrorReason::Error,
                        error: Box::new(message),
                    });
                    return;
                }
                if text.contains("fixture:tool") && !tool_done {
                    message.content=serde_json::from_value(json!([{"type":"toolCall","id":"fixture-bash","name":"bash","arguments":{"command":"printf 'fixture tool output\\n'"}}])).unwrap();
                    message.stop_reason = StopReason::ToolUse;
                    let _ = producer.push(AssistantMessageEvent::Done {
                        reason: DoneReason::ToolUse,
                        message: Box::new(message),
                    });
                    return;
                }
                if text.contains("fixture:multi-slot") {
                    message.content =
                        serde_json::from_value(json!([{"type":"text","text":"first"}])).unwrap();
                    let _ = producer.push(AssistantMessageEvent::TextDelta {
                        content_index: SafeUnsigned::new(0).unwrap(),
                        delta: "first".into(),
                        partial: Box::new(message.clone()),
                    });
                    message.content.push(
                        serde_json::from_value(json!({"type":"thinking","thinking":"middle"}))
                            .unwrap(),
                    );
                    let _ = producer.push(AssistantMessageEvent::ThinkingDelta {
                        content_index: SafeUnsigned::new(1).unwrap(),
                        delta: "middle".into(),
                        partial: Box::new(message.clone()),
                    });
                    message.content.push(
                        serde_json::from_value(json!({"type":"text","text":"last"})).unwrap(),
                    );
                    let _ = producer.push(AssistantMessageEvent::TextDelta {
                        content_index: SafeUnsigned::new(2).unwrap(),
                        delta: "last".into(),
                        partial: Box::new(message.clone()),
                    });
                    cancellation.cancelled().await;
                    message.stop_reason = StopReason::Aborted;
                    message.error_message = Some("Fixture cancelled".into());
                    let _ = producer.push(AssistantMessageEvent::Error {
                        reason: ErrorReason::Aborted,
                        error: Box::new(message),
                    });
                    return;
                }
                message.content = serde_json::from_value(
                    json!([{"type":"thinking","thinking":"Checking the fixture."}]),
                )
                .unwrap();
                let _ = producer.push(AssistantMessageEvent::ThinkingDelta {
                    content_index: SafeUnsigned::new(0).unwrap(),
                    delta: "Checking the fixture.".into(),
                    partial: Box::new(message.clone()),
                });
                let reply = format!("Fixture response: {text}");
                for (index, part) in ["Fixture response: ", text.as_str()].iter().enumerate() {
                    if index == 0 {
                        message.content.push(
                            serde_json::from_value(json!({"type":"text","text":part})).unwrap(),
                        );
                    } else {
                        message.content[1] =
                            serde_json::from_value(json!({"type":"text","text":reply})).unwrap();
                    }
                    let _ = producer.push(AssistantMessageEvent::TextDelta {
                        content_index: SafeUnsigned::new(1).unwrap(),
                        delta: (*part).into(),
                        partial: Box::new(message.clone()),
                    });
                    tokio::select! {_=cancellation.cancelled()=>{},_=tokio::time::sleep(Duration::from_millis(delay_ms))=>{}}
                    if cancellation.is_cancelled() {
                        let message = pi_agent::failure_message(
                            &request.model,
                            &AgentError::Callback("Fixture cancelled".into()),
                            true,
                        );
                        let _ = producer.push(AssistantMessageEvent::Error {
                            reason: ErrorReason::Aborted,
                            error: Box::new(message),
                        });
                        return;
                    }
                }
                let length = text.contains("fixture:length");
                message.stop_reason = if length {
                    StopReason::Length
                } else {
                    StopReason::Stop
                };
                let _ = producer.push(AssistantMessageEvent::Done {
                    reason: if length {
                        DoneReason::Length
                    } else {
                        DoneReason::Stop
                    },
                    message: Box::new(message),
                });
            });
            Ok(stream)
        })
    })
}
