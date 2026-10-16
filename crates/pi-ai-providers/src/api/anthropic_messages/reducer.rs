use super::{
    AnthropicLimits, AnthropicResponseError, Failure, compat, payload::inbound_name, truthy,
};
use crate::api::wire_json::project_wire_safe_unsigned;
use pi_ai_core::*;
use pi_ai_transport::SseFrame;
use serde_json::{Value, json};

struct BlockState {
    index: Option<SafeUnsigned>,
    tool_json: Option<String>,
}
pub(super) struct Reducer {
    pub partial: AssistantMessage,
    usage_model: Model,
    blocks: Vec<BlockState>,
    saw_start: bool,
    saw_stop: bool,
    limits: AnthropicLimits,
}
impl Reducer {
    pub fn new(partial: AssistantMessage, model: Model, limits: AnthropicLimits) -> Self {
        Self {
            partial,
            usage_model: model,
            blocks: vec![],
            saw_start: false,
            saw_stop: false,
            limits,
        }
    }
    pub fn snapshot(&self, producer: &AssistantStreamProducer) -> Result<(), Failure> {
        producer
            .publish_cancellation_snapshot(self.partial.clone())
            .map_err(|_| Failure::Cancelled)
    }
    pub fn emit(
        &self,
        producer: &AssistantStreamProducer,
        event: AssistantMessageEvent,
    ) -> Result<(), Failure> {
        self.snapshot(producer)?;
        producer.push(event).map_err(|_| Failure::Cancelled)
    }
    fn index(index: usize) -> SafeUnsigned {
        SafeUnsigned::new(index as u64).expect("bounded content index")
    }
    fn update_total(&mut self) -> Result<(), Failure> {
        let usage = &mut self.partial.usage;
        usage.total_tokens = count(&json!(
            ((usage.input.get() as f64 + usage.output.get() as f64)
                + usage.cache_read.get() as f64)
                + usage.cache_write.get() as f64
        ))?;
        calculate_cost(&self.usage_model, usage).map_err(|_| Failure::Protocol)?;
        Ok(())
    }
    pub fn consume(
        &mut self,
        request: &TextStreamRequest,
        producer: &AssistantStreamProducer,
        oauth: bool,
        frame: &SseFrame,
    ) -> Result<(), Failure> {
        let event_name = frame.event.as_deref();
        let data = &frame.data;
        if event_name == Some("error") {
            return Err(Failure::Classified(AnthropicResponseError::from_message(
                None,
                data,
                &self.partial,
            )));
        }
        if !matches!(
            event_name,
            Some(
                "message_start"
                    | "message_delta"
                    | "message_stop"
                    | "content_block_start"
                    | "content_block_delta"
                    | "content_block_stop"
            )
        ) {
            return Ok(());
        }
        let raw = frame.raw_lines_for_adapter().ok_or(Failure::Protocol)?;
        let parse_failure = |detail: &str| {
            // The source embeds every raw line, including comments/ignored
            // fields, in this closed provider parser error path. Join with the
            // literal backslash-n source separator, not a newline. Classify
            // before discarding context; never publish raw text or V8 prose.
            Failure::Classified(AnthropicResponseError::from_message(
                None,
                &format!(
                    "Could not parse Anthropic SSE event {}: {detail}; data={data}; raw={}",
                    event_name.unwrap_or(""),
                    raw.join("\\n")
                ),
                &self.partial,
            ))
        };
        let event: Value =
            parse_json_with_repair(data).map_err(|error| parse_failure(&error.to_string()))?;
        if event.is_null() {
            // Source event.type access is inside the same parser try/catch.
            return Err(parse_failure("invalid Anthropic SSE event"));
        }
        match event.get("type").and_then(Value::as_str) {
            Some("message_start") => {
                self.saw_start = true;
                let message = event
                    .get("message")
                    .and_then(Value::as_object)
                    .ok_or(Failure::Protocol)?;
                self.partial.response_id = Some(
                    message
                        .get("id")
                        .and_then(Value::as_str)
                        .ok_or(Failure::Protocol)?
                        .parse()
                        .map_err(|_| Failure::Protocol)?,
                );
                // Explicit controller normalization for incomplete upstream fixtures:
                // absence keeps current identity; a present invalid model rejects.
                if let Some(model) = message.get("model") {
                    self.partial.model = model
                        .as_str()
                        .ok_or(Failure::Protocol)?
                        .parse()
                        .map_err(|_| Failure::Protocol)?;
                }
                let model = &request.resolved.model;
                self.usage_model = model.clone();
                if self.partial.model != model.id
                    && let Some(fallback) = compat(model)?
                        .allowed_fallback_models
                        .as_deref()
                        .unwrap_or_default()
                        .iter()
                        .find(|fallback| {
                            fallback.provider == model.provider
                                && fallback.model == self.partial.model
                        })
                {
                    self.usage_model.id = self.partial.model.clone();
                    self.usage_model.cost = fallback.cost.clone();
                }
                let usage = message
                    .get("usage")
                    .filter(|value| !value.is_null())
                    .ok_or(Failure::Protocol)?;
                self.partial.usage.input = initial_count(usage.get("input_tokens"))?;
                self.partial.usage.output = initial_count(usage.get("output_tokens"))?;
                self.partial.usage.cache_read =
                    initial_count(usage.get("cache_read_input_tokens"))?;
                self.partial.usage.cache_write =
                    initial_count(usage.get("cache_creation_input_tokens"))?;
                self.partial.usage.cache_write_1h = Some(initial_count(
                    usage.pointer("/cache_creation/ephemeral_1h_input_tokens"),
                )?);
                self.update_total()?;
            }
            Some("message_stop") => self.saw_stop = true,
            Some("content_block_start") => {
                let block = event
                    .get("content_block")
                    .filter(|value| !value.is_null())
                    .ok_or(Failure::Protocol)?;
                let kind = block.get("type").and_then(Value::as_str);
                if !matches!(
                    kind,
                    Some("text" | "thinking" | "redacted_thinking" | "tool_use")
                ) {
                    return Ok(());
                }
                if self.blocks.len() >= self.limits.max_content_blocks {
                    return Err(Failure::Limit);
                }
                let index = count(event.get("index").ok_or(Failure::Protocol)?)?;
                let content = match kind {
                    Some("text") => AssistantContent::Text(TextContent {
                        content_type: TextContentType::Text,
                        text: nullable_text(block.get("text"))?.into(),
                        text_signature: None,
                    }),
                    Some("thinking") => AssistantContent::Thinking(ThinkingContent {
                        content_type: ThinkingContentType::Thinking,
                        thinking: nullable_text(block.get("thinking"))?.into(),
                        thinking_signature: Some(nullable_text(block.get("signature"))?.into()),
                        redacted: None,
                    }),
                    Some("redacted_thinking") => AssistantContent::Thinking(ThinkingContent {
                        content_type: ThinkingContentType::Thinking,
                        thinking: "[Reasoning redacted]".into(),
                        thinking_signature: Some(
                            block
                                .get("data")
                                .and_then(Value::as_str)
                                .ok_or(Failure::Protocol)?
                                .into(),
                        ),
                        redacted: Some(true),
                    }),
                    Some("tool_use") => AssistantContent::ToolCall(ToolCall {
                        content_type: ToolCallType::ToolCall,
                        id: block
                            .get("id")
                            .and_then(Value::as_str)
                            .ok_or(Failure::Protocol)?
                            .parse()
                            .map_err(|_| Failure::Protocol)?,
                        name: inbound_name(
                            block
                                .get("name")
                                .and_then(Value::as_str)
                                .ok_or(Failure::Protocol)?,
                            &request.context,
                            oauth,
                        )
                        .parse()
                        .map_err(|_| Failure::Protocol)?,
                        arguments: serde_json::from_value(
                            block
                                .get("input")
                                .filter(|value| !value.is_null())
                                .cloned()
                                .unwrap_or(json!({})),
                        )
                        .map_err(|_| Failure::Protocol)?,
                        arguments_error: None,
                        thought_signature: None,
                        namespace: None,
                    }),
                    _ => return Err(Failure::Protocol),
                };
                self.partial.content.push(content);
                self.blocks.push(BlockState {
                    index: Some(index),
                    tool_json: (kind == Some("tool_use")).then(String::new),
                });
                let content_index = Self::index(self.blocks.len() - 1);
                let partial = Box::new(self.partial.clone());
                let event = match kind {
                    Some("text") => AssistantMessageEvent::TextStart {
                        content_index,
                        partial,
                    },
                    Some("tool_use") => AssistantMessageEvent::ToolCallStart {
                        content_index,
                        partial,
                    },
                    _ => AssistantMessageEvent::ThinkingStart {
                        content_index,
                        partial,
                    },
                };
                self.emit(producer, event)?;
            }
            Some("content_block_delta") => {
                let index = count(event.get("index").ok_or(Failure::Protocol)?)?;
                let Some(position) = self
                    .blocks
                    .iter()
                    .position(|block| block.index == Some(index))
                else {
                    return Ok(());
                };
                let delta = event
                    .get("delta")
                    .filter(|value| !value.is_null())
                    .ok_or(Failure::Protocol)?;
                let kind = delta.get("type").and_then(Value::as_str);
                let content_index = Self::index(position);
                let outgoing = match (kind, &mut self.partial.content[position]) {
                    (Some("text_delta"), AssistantContent::Text(block)) => {
                        let text = required_text(delta.get("text"))?;
                        block.text.push_str(text);
                        Some((false, text.to_owned()))
                    }
                    (Some("thinking_delta"), AssistantContent::Thinking(block)) => {
                        let text = required_text(delta.get("thinking"))?;
                        block.thinking.push_str(text);
                        Some((true, text.to_owned()))
                    }
                    (Some("signature_delta"), AssistantContent::Thinking(block)) => {
                        block
                            .thinking_signature
                            .get_or_insert_default()
                            .push_str(required_text(delta.get("signature"))?);
                        None
                    }
                    (Some("input_json_delta"), AssistantContent::ToolCall(block)) => {
                        let delta = required_text(delta.get("partial_json"))?;
                        let buffer = self.blocks[position]
                            .tool_json
                            .as_mut()
                            .ok_or(Failure::Protocol)?;
                        if delta.len()
                            > self
                                .limits
                                .max_tool_input_bytes
                                .saturating_sub(buffer.len())
                        {
                            return Err(Failure::Limit);
                        }
                        buffer.push_str(delta);
                        block.arguments = arguments(buffer)?;
                        self.emit(
                            producer,
                            AssistantMessageEvent::ToolCallDelta {
                                content_index,
                                delta: delta.into(),
                                partial: Box::new(self.partial.clone()),
                            },
                        )?;
                        None
                    }
                    _ => None,
                };
                if let Some((thinking, delta)) = outgoing {
                    let partial = Box::new(self.partial.clone());
                    self.emit(
                        producer,
                        if thinking {
                            AssistantMessageEvent::ThinkingDelta {
                                content_index,
                                delta,
                                partial,
                            }
                        } else {
                            AssistantMessageEvent::TextDelta {
                                content_index,
                                delta,
                                partial,
                            }
                        },
                    )?;
                }
            }
            Some("content_block_stop") => {
                let index = count(event.get("index").ok_or(Failure::Protocol)?)?;
                if let Some(position) = self
                    .blocks
                    .iter()
                    .position(|block| block.index == Some(index))
                {
                    self.blocks[position].index = None;
                    if let AssistantContent::ToolCall(block) = &mut self.partial.content[position] {
                        let parsed = pi_ai_core::parse_tool_call_arguments(
                            self.blocks[position].tool_json.take().as_deref(),
                        );
                        block.arguments = parsed.arguments;
                        block.arguments_error = parsed.arguments_error;
                    }
                    let content_index = Self::index(position);
                    let partial = Box::new(self.partial.clone());
                    let event = match &self.partial.content[position] {
                        AssistantContent::Text(block) => AssistantMessageEvent::TextEnd {
                            content_index,
                            content: block.text.clone(),
                            partial,
                        },
                        AssistantContent::Thinking(block) => AssistantMessageEvent::ThinkingEnd {
                            content_index,
                            content: block.thinking.clone(),
                            partial,
                        },
                        AssistantContent::ToolCall(block) => AssistantMessageEvent::ToolCallEnd {
                            content_index,
                            tool_call: block.clone(),
                            partial,
                        },
                    };
                    self.emit(producer, event)?;
                }
            }
            Some("message_delta") => {
                let delta = event
                    .get("delta")
                    .filter(|value| !value.is_null())
                    .ok_or(Failure::Protocol)?;
                if let Some(reason) = delta.get("stop_reason").filter(|value| truthy(value)) {
                    let reason = required_text(Some(reason))?;
                    self.partial.raw_stop_reason =
                        Some(reason.parse().map_err(|_| Failure::Protocol)?);
                    self.partial.stop_reason = match reason {
                        "end_turn" | "pause_turn" | "stop_sequence" => StopReason::Stop,
                        "max_tokens" => StopReason::Length,
                        "tool_use" => StopReason::ToolUse,
                        "refusal" | "sensitive" => {
                            let detail = if reason == "refusal" {
                                delta
                                    .pointer("/stop_details/explanation")
                                    .and_then(Value::as_str)
                                    .filter(|s| !s.is_empty())
                                    .unwrap_or("The model refused to complete the request")
                            } else {
                                "Provider stopped with: sensitive"
                            };
                            self.partial.error_message = Some(
                                AnthropicResponseError::from_message(None, detail, &self.partial)
                                    .safe_message(false),
                            );
                            StopReason::Error
                        }
                        _ => {
                            return Err(Failure::Classified(AnthropicResponseError::from_message(
                                None,
                                &format!("Unhandled stop reason: {reason}"),
                                &self.partial,
                            )));
                        }
                    };
                }
                if let Some(usage) = event.get("usage").filter(|value| truthy(value)) {
                    for (field, target) in [
                        ("input_tokens", &mut self.partial.usage.input),
                        ("output_tokens", &mut self.partial.usage.output),
                        (
                            "cache_read_input_tokens",
                            &mut self.partial.usage.cache_read,
                        ),
                        (
                            "cache_creation_input_tokens",
                            &mut self.partial.usage.cache_write,
                        ),
                    ] {
                        if let Some(value) = usage.get(field).filter(|value| !value.is_null()) {
                            *target = count(value)?;
                        }
                    }
                    if let Some(value) = usage
                        .pointer("/output_tokens_details/thinking_tokens")
                        .filter(|value| !value.is_null())
                    {
                        self.partial.usage.reasoning = Some(count(value)?);
                    }
                }
                self.update_total()?;
            }
            _ => {}
        }
        self.snapshot(producer)
    }
    pub fn finish(&self) -> Result<DoneReason, Failure> {
        if self.saw_start && !self.saw_stop {
            return Err(Failure::Classified(AnthropicResponseError::from_message(
                None,
                "Anthropic stream ended before message_stop",
                &self.partial,
            )));
        }
        match self.partial.stop_reason {
            StopReason::Stop => Ok(DoneReason::Stop),
            StopReason::Length => Ok(DoneReason::Length),
            StopReason::ToolUse => Ok(DoneReason::ToolUse),
            StopReason::Error => Err(Failure::Classified(AnthropicResponseError::from_message(
                None,
                self.partial
                    .error_message
                    .as_deref()
                    .unwrap_or("provider request failed"),
                &self.partial,
            ))),
            _ => Err(Failure::MissingTerminal),
        }
    }
}
fn count(value: &Value) -> Result<SafeUnsigned, Failure> {
    project_wire_safe_unsigned(value).map_err(|_| Failure::Protocol)
}
fn initial_count(value: Option<&Value>) -> Result<SafeUnsigned, Failure> {
    value
        .filter(|value| truthy(value))
        .map(count)
        .unwrap_or(Ok(SafeUnsigned::new(0).expect("zero count")))
}
fn required_text(value: Option<&Value>) -> Result<&str, Failure> {
    value.and_then(Value::as_str).ok_or(Failure::Protocol)
}
fn nullable_text(value: Option<&Value>) -> Result<&str, Failure> {
    match value {
        None | Some(Value::Null) => Ok(""),
        Some(value) => required_text(Some(value)),
    }
}
fn arguments(buffer: &str) -> Result<JsonObject, Failure> {
    match parse_streaming_json(Some(buffer)) {
        JsonValue::Object(object) => Ok(object),
        _ => Ok(JsonObject::default()),
    }
}
