use super::convert::valid_detail;
use super::{CompletionsError, ResolvedCompletionsCompat};
use crate::api::constrained_sampling::{
    GrammarToolInputJsonBuffer, append_grammar_tool_input_json_delta,
};
use crate::api::wire_json::{project_wire_safe_unsigned, stringify_wire_json};
use pi_ai_core::{
    AssistantMessage, AssistantMessageEvent, AssistantStreamProducer, Model, SafeUnsigned, Usage,
    calculate_cost, parse_streaming_json,
};
use serde_json::{Value, json};
use std::collections::BTreeMap;

struct ToolState {
    index: Option<f64>,
    args: String,
    custom: Option<(String, GrammarToolInputJsonBuffer)>,
}
pub(super) struct Reducer {
    pub value: Value,
    text: Option<usize>,
    thinking: Option<usize>,
    details: Option<Vec<Value>>,
    tools: BTreeMap<usize, ToolState>,
    by_index: Vec<(f64, usize)>,
    by_id: BTreeMap<String, usize>,
    queued: Vec<Value>,
    queue_bytes: usize,
    finish: bool,
}
fn number(v: &Value) -> Result<f64, CompletionsError> {
    v.as_f64()
        .filter(|n| n.is_finite())
        .ok_or(CompletionsError::InvalidWire)
}
fn count(n: f64) -> Result<SafeUnsigned, CompletionsError> {
    project_wire_safe_unsigned(&json!(n)).map_err(|_| CompletionsError::InvalidWire)
}
fn usage(raw: &Value, model: &Model) -> Result<Usage, CompletionsError> {
    let zero = json!(0);
    let val = |v: &Value| {
        if v.is_null() {
            number(&zero)
        } else {
            number(v)
        }
    };
    let prompt = val(&raw["prompt_tokens"])?;
    let read = [
        &raw["prompt_tokens_details"]["cached_tokens"],
        &raw["prompt_cache_hit_tokens"],
        &raw["cached_tokens"],
    ]
    .into_iter()
    .find(|v| !v.is_null())
    .unwrap_or(&zero);
    let read = number(read)?;
    let write = val(&raw["prompt_tokens_details"]["cache_write_tokens"])?;
    let input = (prompt - read - write).max(0.0);
    let output = val(&raw["completion_tokens"])?;
    let mut usage = Usage::zero();
    usage.input = count(input)?;
    usage.output = count(output)?;
    usage.cache_read = count(read)?;
    usage.cache_write = count(write)?;
    usage.reasoning = Some(count(val(
        &raw["completion_tokens_details"]["reasoning_tokens"],
    )?)?);
    usage.total_tokens = count(input + output + read + write)?;
    calculate_cost(model, &mut usage).map_err(|_| CompletionsError::InvalidWire)?;
    Ok(usage)
}
impl Reducer {
    pub fn new(output: &AssistantMessage) -> Self {
        Self {
            value: json!(output),
            text: None,
            thinking: None,
            details: None,
            tools: BTreeMap::new(),
            by_index: Vec::new(),
            by_id: BTreeMap::new(),
            queued: Vec::new(),
            queue_bytes: 0,
            finish: false,
        }
    }
    fn blocks(&mut self) -> &mut Vec<Value> {
        self.value["content"]
            .as_array_mut()
            .expect("owned content array")
    }
    fn snapshot(&self) -> Value {
        self.value.clone()
    }
    pub fn partial(&self) -> Result<AssistantMessage, CompletionsError> {
        serde_json::from_value(self.value.clone()).map_err(|_| CompletionsError::InvalidWire)
    }
    pub fn safe_partial(&self) -> Result<AssistantMessage, CompletionsError> {
        let mut value = self.value.clone();
        if let Some(blocks) = value["content"].as_array_mut()
            && let Some(i) = blocks.iter().position(|b| {
                b["type"] == "toolCall"
                    && (b["id"].as_str().is_none_or(str::is_empty)
                        || b["name"].as_str().is_none_or(str::is_empty))
            })
        {
            blocks.truncate(i);
        }
        serde_json::from_value(value).map_err(|_| CompletionsError::InvalidWire)
    }
    fn publish(
        &mut self,
        mut event: Value,
        producer: &AssistantStreamProducer,
    ) -> Result<(), CompletionsError> {
        event["partial"] = self.snapshot();
        self.queue_bytes = self
            .queue_bytes
            .checked_add(
                stringify_wire_json(&event)
                    .map_err(|_| CompletionsError::ResourceLimit)?
                    .len(),
            )
            .filter(|n| *n <= 1024 * 1024)
            .ok_or(CompletionsError::ResourceLimit)?;
        self.queued.push(event);
        self.flush(producer)
    }
    fn flush(&mut self, producer: &AssistantStreamProducer) -> Result<(), CompletionsError> {
        let blocks = self.value["content"].as_array().expect("owned array");
        if blocks.iter().any(|b| {
            b["type"] == "toolCall"
                && (b["id"].as_str().is_none_or(str::is_empty)
                    || b["name"].as_str().is_none_or(str::is_empty))
        }) {
            return Ok(());
        }
        for mut event in self.queued.drain(..) {
            if let Some(parts) = event["partial"]["content"].as_array_mut() {
                for (i, part) in parts.iter_mut().enumerate() {
                    if part["type"] == "toolCall" {
                        for key in ["id", "name"] {
                            if part[key].as_str().is_none_or(str::is_empty) {
                                part[key] = blocks[i][key].clone();
                            }
                        }
                    }
                }
            }
            let event: AssistantMessageEvent =
                serde_json::from_value(event).map_err(|_| CompletionsError::InvalidWire)?;
            producer
                .push(event)
                .map_err(|_| CompletionsError::InvalidWire)?;
        }
        self.queue_bytes = 0;
        producer
            .publish_cancellation_snapshot(self.partial()?)
            .map_err(|_| CompletionsError::InvalidWire)?;
        Ok(())
    }
    fn ensure(
        &mut self,
        thinking: bool,
        signature: &str,
        producer: &AssistantStreamProducer,
    ) -> Result<usize, CompletionsError> {
        if let Some(i) = if thinking { self.thinking } else { self.text } {
            return Ok(i);
        }
        let i = self.blocks().len();
        self.blocks().push(if thinking {
            json!({"type":"thinking","thinking":"","thinkingSignature":signature})
        } else {
            json!({"type":"text","text":""})
        });
        if thinking {
            self.thinking = Some(i);
        } else {
            self.text = Some(i);
        }
        self.publish(
            json!({"type":if thinking {"thinking_start"} else {"text_start"},"contentIndex":i}),
            producer,
        )?;
        Ok(i)
    }
    fn apply_details(&mut self) -> Result<(), CompletionsError> {
        if let (Some(i), Some(details)) = (self.thinking, &self.details) {
            self.value["content"][i]["thinkingSignature"] = json!(
                stringify_wire_json(&json!(details))
                    .map_err(|_| CompletionsError::ResourceLimit)?
            );
        }
        Ok(())
    }
    pub fn caught_partial(&mut self) -> Result<AssistantMessage, CompletionsError> {
        self.apply_details()?;
        self.safe_partial()
    }
    /// Cancellation state is independent of whether queued progress has usable
    /// tool identities. Preserve all global fields and only trim the content tail.
    fn cancellation_snapshot(
        &mut self,
        producer: &AssistantStreamProducer,
    ) -> Result<(), CompletionsError> {
        let signature = self
            .thinking
            .map(|i| self.value["content"][i]["thinkingSignature"].clone());
        let partial = self.caught_partial();
        if let (Some(i), Some(signature)) = (self.thinking, signature) {
            self.value["content"][i]["thinkingSignature"] = signature;
        }
        producer
            .publish_cancellation_snapshot(partial?)
            .map_err(|_| CompletionsError::InvalidWire)
    }
    pub fn chunk(
        &mut self,
        chunk: &Value,
        model: &Model,
        grammar: &BTreeMap<String, String>,
        producer: &AssistantStreamProducer,
    ) -> Result<(), CompletionsError> {
        if !chunk.is_object() && !chunk.is_array() {
            return Ok(());
        }
        if chunk
            .get("id")
            .is_some_and(|v| !v.is_null() && !v.is_string())
        {
            return Err(CompletionsError::InvalidWire);
        }
        if self.value.get("responseId").is_none()
            && let Some(id) = chunk
                .get("id")
                .filter(|v| v.as_str().is_some_and(|s| !s.is_empty()))
        {
            self.value["responseId"] = id.clone();
        }
        if let Some(m) = chunk["model"]
            .as_str()
            .filter(|m| !m.is_empty() && *m != model.id.as_str())
            && self.value.get("responseModel").is_none()
        {
            self.value["responseModel"] = json!(m);
        }
        if let Some(raw) = chunk.get("usage").filter(|v| !v.is_null()) {
            self.value["usage"] = json!(usage(raw, model)?);
        }
        let Some(choice) = chunk["choices"].as_array().and_then(|a| a.first()) else {
            self.flush(producer)?;
            self.cancellation_snapshot(producer)?;
            return Ok(());
        };
        if choice
            .get("finish_reason")
            .is_some_and(|v| !v.is_null() && !v.is_string())
        {
            return Err(CompletionsError::InvalidWire);
        }
        if chunk.get("usage").is_none_or(Value::is_null)
            && let Some(raw) = choice.get("usage").filter(|v| !v.is_null())
        {
            self.value["usage"] = json!(usage(raw, model)?);
        }
        if let Some(reason) = choice["finish_reason"].as_str().filter(|s| !s.is_empty()) {
            self.finish = true;
            self.value["rawStopReason"] = json!(reason);
            self.value["stopReason"] = json!(match reason {
                "stop" | "end" => "stop",
                "length" => "length",
                "function_call" | "tool_calls" => "toolUse",
                _ => "error",
            });
            if self.value["stopReason"] == "error" {
                self.value["errorMessage"] = json!(format!("Provider finish_reason: {reason}"));
            }
        }
        let delta = &choice["delta"];
        if delta
            .get("content")
            .is_some_and(|v| !v.is_null() && !v.is_string())
        {
            return Err(CompletionsError::InvalidWire);
        }
        if let Some(text) = delta["content"].as_str().filter(|s| !s.is_empty()) {
            let i = self.ensure(false, "", producer)?;
            let old = self.value["content"][i]["text"]
                .as_str()
                .unwrap_or_default();
            self.value["content"][i]["text"] = json!(format!("{old}{text}"));
            self.publish(
                json!({"type":"text_delta","contentIndex":i,"delta":text}),
                producer,
            )?;
        }
        for field in ["reasoning_content", "reasoning", "reasoning_text"] {
            if let Some(text) = delta[field].as_str().filter(|s| !s.is_empty()) {
                let signature = if model.provider.as_str() == "opencode-go" && field == "reasoning"
                {
                    "reasoning_content"
                } else {
                    field
                };
                let i = self.ensure(true, signature, producer)?;
                let old = self.value["content"][i]["thinking"]
                    .as_str()
                    .unwrap_or_default();
                self.value["content"][i]["thinking"] = json!(format!("{old}{text}"));
                self.publish(
                    json!({"type":"thinking_delta","contentIndex":i,"delta":text}),
                    producer,
                )?;
                break;
            }
        }
        if let Some(tools) = delta.get("tool_calls").filter(|v| !v.is_null()) {
            for tool in tools.as_array().ok_or(CompletionsError::InvalidWire)? {
                self.tool(tool, grammar, producer)?;
            }
        }
        if let Some(details) = delta["reasoning_details"].as_array() {
            for detail in details.iter().filter(|v| valid_detail(v)) {
                self.ensure(true, "", producer)?;
                let details = self.details.get_or_insert_with(Vec::new);
                let field = match detail["type"].as_str() {
                    Some("reasoning.text") => Some("text"),
                    Some("reasoning.summary") => Some("summary"),
                    _ => None,
                };
                if let Some(last) = details
                    .last_mut()
                    .filter(|last| last["type"] == detail["type"])
                    && let Some(field) = field
                {
                    last[field] = json!(format!(
                        "{}{}",
                        last[field].as_str().unwrap_or_default(),
                        detail[field].as_str().unwrap_or_default()
                    ));
                    for key in ["signature", "id", "format", "index"] {
                        if key == "signature" && field != "text" {
                            continue;
                        }
                        let missing = if matches!(key, "signature" | "format") {
                            last.get(key).is_none_or(|v| v.is_null() || v == "")
                        } else {
                            last.get(key).is_none_or(Value::is_null)
                        };
                        if missing {
                            if let Some(v) = detail.get(key) {
                                last[key] = v.clone();
                            } else if let Some(object) = last.as_object_mut() {
                                // Assignment from undefined removes null/empty metadata at stringify.
                                object.shift_remove(key);
                            }
                        }
                    }
                } else {
                    details.push(detail.clone());
                }
            }
        }
        // Signatures remain absent from ordinary deltas, but cancellation needs final replay metadata.
        self.flush(producer)?;
        self.cancellation_snapshot(producer)
    }
    fn tool(
        &mut self,
        delta: &Value,
        grammar: &BTreeMap<String, String>,
        producer: &AssistantStreamProducer,
    ) -> Result<(), CompletionsError> {
        let index = delta
            .get("index")
            .filter(|v| v.is_number())
            .map(number)
            .transpose()?;
        let id = delta["id"].as_str().filter(|s| !s.is_empty());
        let name = delta["function"]["name"]
            .as_str()
            .or_else(|| delta["custom"]["name"].as_str())
            .unwrap_or_default();
        let existing = index
            .and_then(|n| self.by_index.iter().find(|(k, _)| *k == n).map(|(_, i)| *i))
            .or_else(|| id.and_then(|id| self.by_id.get(id).copied()));
        let custom = delta.get("custom").is_some_and(|v| !v.is_null())
            && delta.get("function").is_none_or(Value::is_null);
        let i = if let Some(i) = existing {
            i
        } else {
            let i = self.blocks().len();
            let property = grammar
                .get(name)
                .cloned()
                .unwrap_or_else(|| "input".to_owned());
            let args = if custom {
                json!({property.clone():""})
            } else {
                json!({})
            };
            self.blocks().push(
                json!({"type":"toolCall","id":id.unwrap_or_default(),"name":name,"arguments":args}),
            );
            self.tools.insert(
                i,
                ToolState {
                    index,
                    args: String::new(),
                    custom: custom.then(|| (property, GrammarToolInputJsonBuffer::default())),
                },
            );
            if let Some(n) = index {
                self.by_index.push((n, i));
            }
            if let Some(id) = id {
                self.by_id.insert(id.to_owned(), i);
            }
            self.publish(json!({"type":"toolcall_start","contentIndex":i}), producer)?;
            i
        };
        let state = self
            .tools
            .get_mut(&i)
            .ok_or(CompletionsError::InvalidWire)?;
        if state.index.is_none()
            && let Some(n) = index
        {
            state.index = Some(n);
            self.by_index.push((n, i));
        }
        if let Some(id) = id {
            self.by_id.insert(id.to_owned(), i);
            if self.value["content"][i]["id"] == "" {
                self.value["content"][i]["id"] = json!(id);
            }
        }
        if self.value["content"][i]["name"] == "" && !name.is_empty() {
            self.value["content"][i]["name"] = json!(name);
        }
        if custom && state.custom.is_none() {
            let name = self.value["content"][i]["name"]
                .as_str()
                .unwrap_or_default();
            let property = grammar
                .get(name)
                .cloned()
                .unwrap_or_else(|| "input".to_owned());
            self.value["content"][i]["arguments"] = json!({property.clone():""});
            state.custom = Some((property, GrammarToolInputJsonBuffer::default()));
            state.args.clear();
        }
        let mut text = String::new();
        if let Some(args) = delta["function"]["arguments"]
            .as_str()
            .filter(|s| !s.is_empty())
        {
            if args.len() > 262144usize.saturating_sub(state.args.len()) {
                return Err(CompletionsError::ResourceLimit);
            }
            state.args.push_str(args);
            text = args.to_owned();
            self.value["content"][i]["arguments"] = match parse_streaming_json(Some(&state.args)) {
                pi_ai_core::JsonValue::Object(arguments) => json!(arguments),
                _ => json!({}),
            };
        } else if let Some(input) = delta["custom"]["input"].as_str().filter(|s| !s.is_empty())
            && let Some((property, buffer)) = &mut state.custom
        {
            if input.len() > 262144usize.saturating_sub(buffer.input.len()) {
                return Err(CompletionsError::ResourceLimit);
            }
            let next = format!("{}{input}", buffer.input);
            text = append_grammar_tool_input_json_delta(buffer, property, &next, false)
                .map_err(|_| CompletionsError::Conversion)?
                .unwrap_or_default();
            self.value["content"][i]["arguments"] = json!({property.clone():next});
        }
        self.publish(
            json!({"type":"toolcall_delta","contentIndex":i,"delta":text}),
            producer,
        )
    }
    pub fn finish(
        &mut self,
        compat: &ResolvedCompletionsCompat,
        producer: &AssistantStreamProducer,
    ) -> Result<(), CompletionsError> {
        self.partial()?;
        for i in 0..self.blocks().len() {
            if let Some(state) = self.tools.get_mut(&i) {
                if let Some((property, buffer)) = &mut state.custom {
                    let input = buffer.input.clone();
                    if let Some(delta) =
                        append_grammar_tool_input_json_delta(buffer, property, &input, true)
                            .map_err(|_| CompletionsError::Conversion)?
                    {
                        self.publish(
                            json!({"type":"toolcall_delta","contentIndex":i,"delta":delta}),
                            producer,
                        )?;
                    }
                } else {
                    let parsed = pi_ai_core::parse_tool_call_arguments(Some(&state.args));
                    self.value["content"][i]["arguments"] = json!(parsed.arguments);
                    if let Some(error) = parsed.arguments_error {
                        self.value["content"][i]["argumentsError"] = json!(error);
                    }
                }
                self.publish(json!({"type":"toolcall_end","contentIndex":i,"toolCall":self.value["content"][i]}),producer)?;
            } else if Some(i) == self.thinking {
                self.apply_details()?;
                self.publish(json!({"type":"thinking_end","contentIndex":i,"content":self.value["content"][i]["thinking"]}),producer)?;
            } else {
                self.publish(json!({"type":"text_end","contentIndex":i,"content":self.value["content"][i]["text"]}),producer)?;
            }
        }
        if !self.finish && !compat.flag("supportsFinishReason") {
            self.value["stopReason"] = json!(if self.tools.is_empty() {
                "stop"
            } else {
                "toolUse"
            });
        }
        if (compat.flag("supportsFinishReason") && !self.finish)
            || self.value["stopReason"] == "pending"
        {
            return Err(CompletionsError::MissingFinishReason);
        }
        Ok(())
    }
}
