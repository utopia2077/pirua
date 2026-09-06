use pi_ai_core::*;
use serde_json::{Value, json};

use super::{Failure, MistralLimits, classified_message, classify, payload::derive_id};
use crate::api::wire_json::{project_wire_safe_unsigned, stringify_wire_json};

#[derive(Eq, PartialEq)]
enum Key {
    Index(SafeUnsigned),
    Id(String),
}
struct ToolState {
    key: Key,
    index: usize,
    arguments: String,
}
pub(super) struct Reducer {
    pub partial: AssistantMessage,
    current: Option<usize>,
    tools: Vec<ToolState>,
    limits: MistralLimits,
}
impl Reducer {
    pub fn new(partial: AssistantMessage, limits: MistralLimits) -> Self {
        Self {
            partial,
            current: None,
            tools: vec![],
            limits,
        }
    }
    pub fn emit(
        &self,
        producer: &AssistantStreamProducer,
        event: AssistantMessageEvent,
    ) -> Result<(), Failure> {
        self.snapshot(producer)?;
        producer.push(event).map_err(|_| Failure::Cancelled)
    }
    fn snapshot(&self, producer: &AssistantStreamProducer) -> Result<(), Failure> {
        producer
            .publish_cancellation_snapshot(self.partial.clone())
            .map_err(|_| Failure::Cancelled)
    }
    fn index(index: usize) -> SafeUnsigned {
        SafeUnsigned::new(index as u64).expect("bounded block index")
    }
    fn room(&self) -> Result<(), Failure> {
        if self.partial.content.len() >= self.limits.max_content_blocks {
            Err(Failure::Limit)
        } else {
            Ok(())
        }
    }
    fn end_current(&mut self, producer: &AssistantStreamProducer) -> Result<(), Failure> {
        if let Some(index) = self.current.take() {
            let content_index = Self::index(index);
            let partial = Box::new(self.partial.clone());
            let event = match &self.partial.content[index] {
                AssistantContent::Text(text) => AssistantMessageEvent::TextEnd {
                    content_index,
                    content: text.text.clone(),
                    partial,
                },
                AssistantContent::Thinking(thinking) => AssistantMessageEvent::ThinkingEnd {
                    content_index,
                    content: thinking.thinking.clone(),
                    partial,
                },
                _ => return Err(Failure::Protocol),
            };
            self.emit(producer, event)?;
        }
        Ok(())
    }
    fn text(
        &mut self,
        producer: &AssistantStreamProducer,
        delta: &str,
        thinking: bool,
    ) -> Result<(), Failure> {
        if thinking && delta.is_empty() {
            return Ok(());
        }
        let matches = self.current.is_some_and(|index| {
            matches!(
                (&self.partial.content[index], thinking),
                (AssistantContent::Text(_), false) | (AssistantContent::Thinking(_), true)
            )
        });
        if !matches {
            self.end_current(producer)?;
            self.room()?;
            let index = self.partial.content.len();
            self.current = Some(index);
            self.partial.content.push(if thinking {
                AssistantContent::Thinking(ThinkingContent {
                    content_type: ThinkingContentType::Thinking,
                    thinking: String::new(),
                    thinking_signature: None,
                    redacted: None,
                })
            } else {
                AssistantContent::Text(TextContent {
                    content_type: TextContentType::Text,
                    text: String::new(),
                    text_signature: None,
                })
            });
            let partial = Box::new(self.partial.clone());
            let content_index = Self::index(index);
            self.emit(
                producer,
                if thinking {
                    AssistantMessageEvent::ThinkingStart {
                        content_index,
                        partial,
                    }
                } else {
                    AssistantMessageEvent::TextStart {
                        content_index,
                        partial,
                    }
                },
            )?;
        }
        let index = self.current.ok_or(Failure::Protocol)?;
        match &mut self.partial.content[index] {
            AssistantContent::Text(text) => text.text.push_str(delta),
            AssistantContent::Thinking(thinking) => thinking.thinking.push_str(delta),
            _ => return Err(Failure::Protocol),
        }
        let partial = Box::new(self.partial.clone());
        let content_index = Self::index(index);
        self.emit(
            producer,
            if thinking {
                AssistantMessageEvent::ThinkingDelta {
                    content_index,
                    delta: delta.into(),
                    partial,
                }
            } else {
                AssistantMessageEvent::TextDelta {
                    content_index,
                    delta: delta.into(),
                    partial,
                }
            },
        )
    }
    pub fn consume(
        &mut self,
        model: &Model,
        producer: &AssistantStreamProducer,
        text: &str,
    ) -> Result<(), Failure> {
        let chunk: Value = serde_json::from_str(text).map_err(|_| Failure::Protocol)?;
        let choices = chunk
            .as_object()
            .and_then(|chunk| chunk.get("choices"))
            .and_then(Value::as_array)
            .ok_or(Failure::Protocol)?;
        if self.partial.response_id.is_none()
            && let Some(id) = chunk
                .get("id")
                .and_then(Value::as_str)
                .filter(|id| !id.is_empty())
        {
            self.partial.response_id = Some(id.parse().map_err(|_| Failure::Protocol)?);
        }
        if let Some(usage) = chunk
            .get("usage")
            .filter(|v| !v.is_null() && **v != Value::Bool(false))
        {
            let prompt = numeric_or_zero(usage.get("prompt_tokens"))?;
            let output = numeric_or_zero(usage.get("completion_tokens"))?;
            let cached = [
                usage.pointer("/promptTokensDetails/cachedTokens"),
                usage.pointer("/prompt_tokens_details/cached_tokens"),
                usage.pointer("/promptTokenDetails/cachedTokens"),
                usage.pointer("/prompt_token_details/cached_tokens"),
                usage.get("numCachedTokens"),
                usage.get("num_cached_tokens"),
            ]
            .into_iter()
            .flatten()
            .find(|v| !v.is_null())
            .and_then(Value::as_f64)
            .filter(|n| n.is_finite())
            .unwrap_or(0.0);
            let cached = prompt.min(cached.max(0.0));
            let mut usage_result = Usage::zero();
            usage_result.input = count((prompt - cached).max(0.0))?;
            usage_result.output = count(output)?;
            usage_result.cache_read = count(cached)?;
            let total = numeric_or_zero(usage.get("total_tokens"))?;
            usage_result.total_tokens = count(if total == 0.0 {
                (usage_result.input.get() as f64 + output) + cached
            } else {
                total
            })?;
            calculate_cost(model, &mut usage_result).map_err(|_| Failure::Protocol)?;
            self.partial.usage = usage_result;
        }
        self.snapshot(producer)?;
        let Some(choice) = choices.first().filter(|value| {
            !value.is_null()
                && **value != Value::Bool(false)
                && value.as_f64() != Some(0.0)
                && value.as_str() != Some("")
        }) else {
            return Ok(());
        };
        if let Some(reason) = choice.get("finish_reason").filter(|v| !v.is_null()) {
            let reason = reason.as_str().ok_or(Failure::Protocol)?;
            if !reason.is_empty() {
                self.partial.raw_stop_reason = Some(reason.parse().map_err(|_| Failure::Protocol)?);
                self.partial.stop_reason = match reason {
                    "stop" => StopReason::Stop,
                    "length" | "model_length" => StopReason::Length,
                    "tool_calls" => StopReason::ToolUse,
                    _ => StopReason::Error,
                };
                if self.partial.stop_reason == StopReason::Error {
                    let (overflow, retryable) =
                        classify(&format!("Provider stopped with: {reason}"), &self.partial);
                    self.partial.error_message =
                        Some(classified_message(overflow, retryable).into());
                }
            }
        }
        self.snapshot(producer)?;
        let delta = choice
            .get("delta")
            .and_then(Value::as_object)
            .ok_or(Failure::Protocol)?;
        if let Some(content) = delta.get("content").filter(|v| !v.is_null()) {
            if let Some(text) = content.as_str() {
                self.text(producer, text, false)?;
            } else {
                for item in content.as_array().ok_or(Failure::Protocol)? {
                    if item.is_null() {
                        return Err(Failure::Protocol);
                    }
                    if let Some(text) = item.as_str() {
                        self.text(producer, text, false)?;
                    } else {
                        match item.get("type").and_then(Value::as_str) {
                            Some("text") => {
                                self.text(producer, nullable_text(item.get("text"))?, false)?
                            }
                            Some("thinking") => {
                                let mut text = String::new();
                                if let Some(parts) = item.get("thinking").filter(|v| !v.is_null()) {
                                    for part in parts.as_array().ok_or(Failure::Protocol)? {
                                        if part.is_null() {
                                            return Err(Failure::Protocol);
                                        }
                                        text.push_str(nullable_text(part.get("text"))?);
                                    }
                                }
                                self.text(producer, &text, true)?;
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
        if let Some(calls) = delta
            .get("tool_calls")
            .filter(|v| !v.is_null() && **v != Value::Bool(false))
        {
            for call in calls.as_array().ok_or(Failure::Protocol)? {
                self.tool(producer, call)?;
            }
        }
        Ok(())
    }
    fn tool(&mut self, producer: &AssistantStreamProducer, call: &Value) -> Result<(), Failure> {
        self.end_current(producer)?;
        let index = call
            .get("index")
            .filter(|v| !v.is_null())
            .map(project_wire_safe_unsigned)
            .transpose()
            .map_err(|_| Failure::Protocol)?;
        let id = call
            .get("id")
            .filter(|v| !v.is_null())
            .map(|v| v.as_str().ok_or(Failure::Protocol))
            .transpose()?;
        let id = id
            .filter(|id| !id.is_empty() && *id != "null")
            .map(str::to_owned)
            .unwrap_or_else(|| {
                derive_id(
                    &format!("toolcall:{}", index.map_or(0, SafeUnsigned::get)),
                    0,
                )
            });
        let key = index.map_or_else(|| Key::Id(id.clone()), Key::Index);
        let function = call
            .get("function")
            .and_then(Value::as_object)
            .ok_or(Failure::Protocol)?;
        let position = match self.tools.iter().position(|state| state.key == key) {
            Some(position) => position,
            None => {
                self.room()?;
                let name = function
                    .get("name")
                    .and_then(Value::as_str)
                    .ok_or(Failure::Protocol)?;
                let index = self.partial.content.len();
                self.partial
                    .content
                    .push(AssistantContent::ToolCall(ToolCall {
                        content_type: ToolCallType::ToolCall,
                        id: id.parse().map_err(|_| Failure::Protocol)?,
                        name: name.parse().map_err(|_| Failure::Protocol)?,
                        arguments: JsonObject::default(),
                        thought_signature: None,
                        namespace: None,
                    }));
                self.tools.push(ToolState {
                    key,
                    index,
                    arguments: String::new(),
                });
                self.emit(
                    producer,
                    AssistantMessageEvent::ToolCallStart {
                        content_index: Self::index(index),
                        partial: Box::new(self.partial.clone()),
                    },
                )?;
                self.tools.len() - 1
            }
        };
        let arguments = function.get("arguments");
        let delta = if let Some(text) = arguments.and_then(Value::as_str) {
            text.into()
        } else {
            stringify_wire_json(
                arguments
                    .filter(|v| {
                        !v.is_null() && **v != Value::Bool(false) && v.as_f64() != Some(0.0)
                    })
                    .unwrap_or(&json!({})),
            )
            .map_err(|_| Failure::Protocol)?
        };
        let state = &mut self.tools[position];
        if delta.len()
            > self
                .limits
                .max_tool_input_bytes
                .saturating_sub(state.arguments.len())
        {
            return Err(Failure::Limit);
        }
        state.arguments.push_str(&delta);
        let index = state.index;
        let parsed = parse_streaming_json(Some(&state.arguments));
        let arguments = match parsed {
            JsonValue::Object(object) => object,
            _ => return Err(Failure::Protocol),
        };
        let AssistantContent::ToolCall(block) = &mut self.partial.content[index] else {
            return Err(Failure::Protocol);
        };
        block.arguments = arguments;
        self.emit(
            producer,
            AssistantMessageEvent::ToolCallDelta {
                content_index: Self::index(index),
                delta,
                partial: Box::new(self.partial.clone()),
            },
        )
    }
    pub fn finish(&mut self, producer: &AssistantStreamProducer) -> Result<(), Failure> {
        self.end_current(producer)?;
        for state in &self.tools {
            let AssistantContent::ToolCall(call) = &self.partial.content[state.index] else {
                return Err(Failure::Protocol);
            };
            self.emit(
                producer,
                AssistantMessageEvent::ToolCallEnd {
                    content_index: Self::index(state.index),
                    tool_call: call.clone(),
                    partial: Box::new(self.partial.clone()),
                },
            )?;
        }
        Ok(())
    }
}
fn count(number: f64) -> Result<SafeUnsigned, Failure> {
    project_wire_safe_unsigned(&json!(number)).map_err(|_| Failure::Protocol)
}
fn numeric_or_zero(value: Option<&Value>) -> Result<f64, Failure> {
    match value {
        None | Some(Value::Null) | Some(Value::Bool(false)) => Ok(0.0),
        Some(Value::String(value)) if value.is_empty() => Ok(0.0),
        Some(value) => value
            .as_f64()
            .filter(|n| n.is_finite())
            .ok_or(Failure::Protocol),
    }
}
fn nullable_text(value: Option<&Value>) -> Result<&str, Failure> {
    match value {
        None | Some(Value::Null) => Ok(""),
        Some(value) => value.as_str().ok_or(Failure::Protocol),
    }
}
