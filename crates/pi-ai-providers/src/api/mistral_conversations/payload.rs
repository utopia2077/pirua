use std::collections::BTreeMap;

use pi_ai_core::*;
use serde_json::{Map, Value, json};

use super::{Failure, MistralOptions, ecma_whitespace};
use crate::api::constrained_sampling::{
    get_wire_json_schema_tool_parameters, resolve_json_schema_strict_sampling,
};
use crate::api::wire_json::stringify_wire_json;

pub(super) fn cache_session(request: &TextStreamRequest) -> Option<&str> {
    request
        .options
        .wire
        .session_id
        .as_deref()
        .filter(|id| !id.is_empty())
        .filter(|_| request.options.wire.cache_retention != Some(CacheRetention::None))
}

pub(super) fn derive_id(id: &str, attempt: u64) -> String {
    let normalized: String = id.chars().filter(char::is_ascii_alphanumeric).collect();
    if attempt == 0 && normalized.len() == 9 {
        return normalized;
    }
    let seed = if normalized.is_empty() {
        id
    } else {
        &normalized
    };
    let hash = short_hash(&if attempt == 0 {
        seed.to_owned()
    } else {
        format!("{seed}:{attempt}")
    });
    hash.chars()
        .filter(char::is_ascii_alphanumeric)
        .take(9)
        .collect()
}
#[derive(Default)]
struct IdNormalizer {
    ids: BTreeMap<String, String>,
    owners: BTreeMap<String, String>,
}
impl ToolCallIdNormalizer for IdNormalizer {
    fn normalize_tool_call_id(
        &mut self,
        id: &ToolCallId,
        _: &Model,
        _: &AssistantMessage,
    ) -> ToolCallId {
        if let Some(mapped) = self.ids.get(id.as_str()) {
            return mapped.parse().expect("normalized ID");
        }
        for attempt in 0..=u64::MAX {
            let candidate = derive_id(id.as_str(), attempt);
            if !self.owners.contains_key(&candidate) {
                self.owners
                    .insert(candidate.clone(), id.as_str().to_owned());
                self.ids.insert(id.as_str().to_owned(), candidate.clone());
                return candidate.parse().expect("nonempty hash");
            }
        }
        // The input resource bounds make exhaustive occupancy unreachable.
        unreachable!("more unique calls than available hash candidates")
    }
}

pub(super) fn build(
    request: &TextStreamRequest,
    options: &MistralOptions,
) -> Result<Value, Failure> {
    let model = &request.resolved.model;
    let messages = transform_messages(
        &request.context.messages,
        model,
        SafeUnsigned::new(request.options.runtime.clock.now_unix_ms()).unwrap_or(SafeUnsigned::MAX),
        Some(&mut IdNormalizer::default()),
    )
    .map_err(|_| Failure::Protocol)?;
    let images = model.input.contains(&ModelInput::Image);
    let mut chat = Vec::new();
    if let Some(system) = &request.context.system_prompt
        && !system.is_empty()
    {
        chat.push(json!({"role":"system", "content": system}));
    }
    for message in &messages {
        match message {
            Message::User(message) => match &message.content {
                UserContent::Text(text) => chat.push(json!({"role":"user","content":text})),
                UserContent::Blocks(blocks) => {
                    let content = input_parts(blocks, images);
                    if !content.is_empty() {
                        chat.push(json!({"role":"user","content":content}));
                    } else if blocks
                        .iter()
                        .any(|part| matches!(part, InputContent::Image(_)))
                        && !images
                    {
                        chat.push(json!({"role":"user","content":"(image omitted: model does not support images)"}));
                    }
                }
            },
            Message::Assistant(message) => {
                let mut content = vec![];
                let mut calls = vec![];
                for block in &message.content {
                    match block {
                        AssistantContent::Text(text) if !text.text.trim_matches(ecma_whitespace).is_empty() =>
                            content.push(json!({"type":"text","text":text.text})),
                        AssistantContent::Thinking(thinking) if !thinking.thinking.trim_matches(ecma_whitespace).is_empty() =>
                            content.push(json!({"type":"thinking","thinking":[{"type":"text","text":thinking.thinking}]})),
                        AssistantContent::ToolCall(call) => calls.push(json!({"id":call.id,"type":"function",
                            "function":{"name":call.name,"arguments": stringify_wire_json(&serde_json::to_value(&call.arguments).map_err(|_| Failure::Protocol)?)
                                .map_err(|_| Failure::Protocol)?},"index":0})),
                        _ => {}
                    }
                }
                if !content.is_empty() || !calls.is_empty() {
                    let mut result = json!({"role":"assistant","prefix":false});
                    if !content.is_empty() {
                        result["content"] = json!(content);
                    }
                    if !calls.is_empty() {
                        result["toolCalls"] = json!(calls);
                    }
                    chat.push(result);
                }
            }
            Message::ToolResult(message) => {
                let text = message
                    .content
                    .iter()
                    .filter_map(|part| match part {
                        InputContent::Text(text) => Some(text.text.as_str()),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                let has_images = message
                    .content
                    .iter()
                    .any(|part| matches!(part, InputContent::Image(_)));
                let trimmed = text.trim_matches(ecma_whitespace);
                let prefix = if message.is_error {
                    "[tool error] "
                } else {
                    ""
                };
                let text = if !trimmed.is_empty() {
                    format!(
                        "{prefix}{trimmed}{}",
                        if has_images && !images {
                            "\n[tool image omitted: model does not support images]"
                        } else {
                            ""
                        }
                    )
                } else if has_images {
                    format!(
                        "{prefix}{}",
                        if images {
                            "(see attached image)"
                        } else {
                            "(image omitted: model does not support images)"
                        }
                    )
                } else {
                    format!("{prefix}(no tool output)")
                };
                let mut content = vec![json!({"type":"text","text":text})];
                if images {
                    content.extend(message.content.iter().filter_map(|part| match part {
                        InputContent::Image(image) => Some(json!({"type":"image_url","imageUrl":format!("data:{};base64,{}",image.mime_type,image.data)})), _=>None }));
                }
                chat.push(json!({"role":"tool","toolCallId":message.tool_call_id,"name":message.tool_name,"content":content}));
            }
        }
    }
    let mut result = json!({"model":model.id,"stream":true,"messages":chat});
    if let Some(tools) = &request.context.tools
        && !tools.is_empty()
    {
        let tools = tools.iter().map(|tool| {
            let strict = resolve_json_schema_strict_sampling(tool,true).map_err(|_| Failure::Protocol)?;
            let parameters = get_wire_json_schema_tool_parameters(tool,strict).map_err(|_| Failure::Protocol)?;
            Ok(json!({"type":"function","function":{"name":tool.name,"description":tool.description,
                "parameters":parameters.as_value(),"strict":strict.unwrap_or(false)}}))
        }).collect::<Result<Vec<_>,Failure>>()?;
        result["tools"] = json!(tools);
    }
    if let Some(temperature) = request.options.wire.temperature {
        result["temperature"] = json!(temperature);
    }
    if let Some(max) = request.options.wire.max_tokens {
        result["maxTokens"] = json!(max);
    }
    if let Some(choice) = &options.tool_choice {
        result["toolChoice"] = json!(choice);
    }
    if let Some(prompt) = options.prompt_mode {
        result["promptMode"] = json!(prompt);
    }
    if let Some(effort) = options.reasoning_effort {
        result["reasoningEffort"] = json!(effort);
    }
    if let Some(session) = cache_session(request) {
        result["promptCacheKey"] = json!(session);
    }
    // Pinned raw builder deliberately does not spread samplingParams.
    Ok(result)
}

fn input_parts(parts: &[InputContent], images: bool) -> Vec<Value> {
    parts.iter().filter_map(|part| match part {
        InputContent::Text(text) => Some(json!({"type":"text","text":text.text})),
        InputContent::Image(image) if images => Some(json!({"type":"image_url","imageUrl":format!("data:{};base64,{}", image.mime_type,image.data)})),
        _ => None,
    }).collect()
}

fn remap(record: &mut Map<String, Value>, keys: &[(&str, &str)]) {
    for (source, target) in keys {
        if let Some(value) = record.get(*source).cloned() {
            record.insert((*target).into(), value);
            record.shift_remove(*source);
        }
    }
}
pub(super) fn to_wire(mut payload: Value) -> Result<Value, Failure> {
    let object = payload.as_object_mut().ok_or(Failure::Protocol)?;
    remap(
        object,
        &[
            ("topP", "top_p"),
            ("maxTokens", "max_tokens"),
            ("randomSeed", "random_seed"),
            ("responseFormat", "response_format"),
            ("toolChoice", "tool_choice"),
            ("presencePenalty", "presence_penalty"),
            ("frequencyPenalty", "frequency_penalty"),
            ("parallelToolCalls", "parallel_tool_calls"),
            ("reasoningEffort", "reasoning_effort"),
            ("promptMode", "prompt_mode"),
            ("promptCacheKey", "prompt_cache_key"),
            ("safePrompt", "safe_prompt"),
        ],
    );
    for message in object
        .get_mut("messages")
        .and_then(Value::as_array_mut)
        .ok_or(Failure::Protocol)?
    {
        let message = message.as_object_mut().ok_or(Failure::Protocol)?;
        remap(
            message,
            &[("toolCalls", "tool_calls"), ("toolCallId", "tool_call_id")],
        );
        if let Some(content) = message.get_mut("content").and_then(Value::as_array_mut) {
            for chunk in content {
                let chunk = chunk.as_object_mut().ok_or(Failure::Protocol)?;
                remap(
                    chunk,
                    &[
                        ("imageUrl", "image_url"),
                        ("documentUrl", "document_url"),
                        ("documentName", "document_name"),
                        ("fileId", "file_id"),
                        ("referenceIds", "reference_ids"),
                        ("inputAudio", "input_audio"),
                    ],
                );
            }
        }
    }
    if let Some(format) = object
        .get_mut("response_format")
        .and_then(Value::as_object_mut)
    {
        remap(format, &[("jsonSchema", "json_schema")]);
        if let Some(schema) = format.get_mut("json_schema").and_then(Value::as_object_mut) {
            remap(schema, &[("schemaDefinition", "schema")]);
        }
    }
    Ok(payload)
}
