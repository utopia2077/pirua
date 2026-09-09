use super::{
    AnthropicOptions, AnthropicThinkingDisplay, AnthropicToolChoice, Failure, compat,
    ecma_whitespace,
};
use crate::api::{
    constrained_sampling::{
        get_wire_json_schema_tool_parameters, resolve_json_schema_strict_sampling,
    },
    runtime::AdapterServices,
};
use pi_ai_core::*;
use serde_json::{Value, json};
use std::collections::BTreeSet;

const CLAUDE_CODE_TOOLS: &[&str] = &[
    "Read",
    "Write",
    "Edit",
    "Bash",
    "Grep",
    "Glob",
    "AskUserQuestion",
    "EnterPlanMode",
    "ExitPlanMode",
    "KillShell",
    "NotebookEdit",
    "Skill",
    "Task",
    "TaskOutput",
    "TodoWrite",
    "WebFetch",
    "WebSearch",
];
pub(super) fn outbound_name(name: &str, oauth: bool) -> String {
    if oauth {
        let lower = name.to_lowercase();
        if let Some(canonical) = CLAUDE_CODE_TOOLS
            .iter()
            .find(|value| value.to_lowercase() == lower)
        {
            return (*canonical).into();
        }
    }
    name.into()
}
pub(super) fn inbound_name(name: &str, context: &Context, oauth: bool) -> String {
    if oauth {
        let lower = name.to_lowercase();
        if let Some(tool) = context
            .tools
            .as_deref()
            .unwrap_or_default()
            .iter()
            .find(|tool| tool.name.as_str().to_lowercase() == lower)
        {
            return tool.name.as_str().into();
        }
    }
    name.into()
}
struct NameNormalizer(bool);
impl ToolNameNormalizer for NameNormalizer {
    fn normalize_tool_name(&self, name: &ToolName) -> ToolName {
        outbound_name(name.as_str(), self.0)
            .parse()
            .expect("nonempty normalized tool name")
    }
}
struct IdNormalizer;
impl ToolCallIdNormalizer for IdNormalizer {
    fn normalize_tool_call_id(
        &mut self,
        id: &ToolCallId,
        _: &Model,
        _: &AssistantMessage,
    ) -> ToolCallId {
        // The source regex is non-Unicode: each invalid UTF-16 code unit maps to _.
        let normalized: String = id
            .as_str()
            .encode_utf16()
            .take(64)
            .map(|unit| {
                if unit <= 127 && ((unit as u8).is_ascii_alphanumeric() || matches!(unit, 45 | 95))
                {
                    char::from(unit as u8)
                } else {
                    '_'
                }
            })
            .collect();
        normalized.parse().expect("nonempty normalized tool ID")
    }
}

pub(super) fn retention(request: &TextStreamRequest, services: &AdapterServices) -> CacheRetention {
    request.options.wire.cache_retention.unwrap_or_else(|| {
        let value = request
            .auth
            .env
            .as_map()
            .get("PI_CACHE_RETENTION")
            .filter(|v| !v.is_empty())
            .or_else(|| services.env.as_map().get("PI_CACHE_RETENTION"));
        if value.is_some_and(|v| v == "long") {
            CacheRetention::Long
        } else {
            CacheRetention::Short
        }
    })
}
fn cache_control(
    request: &TextStreamRequest,
    services: &AdapterServices,
    c: &AnthropicMessagesCompat,
) -> Option<Value> {
    let retention = retention(request, services);
    if retention == CacheRetention::None {
        return None;
    }
    let mut cache = json!({"type":"ephemeral"});
    if retention == CacheRetention::Long && c.supports_long_cache_retention.unwrap_or(true) {
        cache["ttl"] = json!("1h");
    }
    Some(cache)
}
fn add_cache(value: &mut Value, cache: Option<&Value>) {
    if let Some(cache) = cache {
        value["cache_control"] = cache.clone();
    }
}

pub(super) fn default_tool_references(model: &Model) -> bool {
    if model.provider.as_str() != "anthropic" || model.id.as_str().contains("haiku") {
        return false;
    }
    let mut pieces = model.id.as_str().split('-');
    if pieces.next() != Some("claude")
        || !matches!(pieces.next(), Some("opus" | "sonnet" | "fable"))
    {
        return false;
    }
    let Some(major) = pieces
        .next()
        .filter(|s| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()))
    else {
        return false;
    };
    let major = major.trim_start_matches('0');
    if major.len() > 1 || major.as_bytes().first().is_some_and(|digit| *digit > b'4') {
        return true;
    }
    if major != "4" {
        return false;
    }
    let minor = pieces
        .next()
        .filter(|s| !s.is_empty() && s.len() < 8 && s.bytes().all(|b| b.is_ascii_digit()))
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(0);
    minor >= 5
}

pub(super) fn build(
    request: &TextStreamRequest,
    options: &AnthropicOptions,
    mapped_effort: Option<&str>,
    services: &AdapterServices,
    oauth: bool,
) -> Result<Value, Failure> {
    let model = &request.resolved.model;
    let c = compat(model)?;
    let cache = cache_control(request, services, &c);
    let mut context = request.context.clone();
    context.messages = transform_messages(
        &context.messages,
        model,
        SafeUnsigned::new(request.options.runtime.clock.now_unix_ms()).unwrap_or(SafeUnsigned::MAX),
        Some(&mut IdNormalizer),
    )
    .map_err(|_| Failure::Protocol)?;
    let normalizer = NameNormalizer(oauth);
    let mut split = split_deferred_tools(
        &context,
        c.supports_tool_references
            .unwrap_or_else(|| default_tool_references(model)),
        Some(&normalizer),
    );
    if split.immediate.is_empty() && !split.deferred.is_empty() {
        split.immediate = split.deferred.drain(..).map(|entry| entry.tool).collect();
    }
    let deferred_names: BTreeSet<String> = split
        .deferred
        .iter()
        .map(|entry| entry.normalized_name.as_str().into())
        .collect();
    let mut result = json!({"model":model.id,"messages":messages(&context.messages,oauth,cache.as_ref(),c.allow_empty_signature.unwrap_or(false),&deferred_names)?,
        "max_tokens":request.options.wire.max_tokens.unwrap_or(model.max_tokens),"stream":true});
    let mut system = vec![];
    if oauth {
        let mut identity = json!({"type":"text","text":"You are Claude Code, Anthropic's official CLI for Claude."});
        add_cache(&mut identity, cache.as_ref());
        system.push(identity);
    }
    if let Some(text) = &context.system_prompt
        && !text.is_empty()
    {
        let mut block = json!({"type":"text","text":text});
        add_cache(&mut block, cache.as_ref());
        system.push(block);
    }
    if !system.is_empty() {
        result["system"] = json!(system);
    }
    if options.thinking_enabled != Some(true)
        && c.supports_temperature.unwrap_or(true)
        && let Some(temperature) = request.options.wire.temperature
    {
        result["temperature"] = json!(temperature);
    }
    let mut tools = convert_tools(
        &split.immediate,
        oauth,
        &c,
        cache
            .as_ref()
            .filter(|_| c.supports_cache_control_on_tools.unwrap_or(true)),
        false,
    )?;
    let deferred: Vec<Tool> = split.deferred.into_iter().map(|entry| entry.tool).collect();
    tools.extend(convert_tools(&deferred, oauth, &c, None, true)?);
    if !tools.is_empty() {
        result["tools"] = json!(tools);
    }
    if model.reasoning {
        if options.thinking_enabled == Some(true) {
            let display = options
                .thinking_display
                .unwrap_or(AnthropicThinkingDisplay::Summarized);
            if c.force_adaptive_thinking == Some(true) {
                result["thinking"] = json!({"type":"adaptive","display":display});
                let effort = mapped_effort.map(str::to_owned).or_else(|| {
                    options.effort.map(|effort| {
                        serde_json::to_value(effort)
                            .expect("closed effort")
                            .as_str()
                            .expect("string effort")
                            .into()
                    })
                });
                if let Some(effort) = effort.filter(|s| !s.is_empty()) {
                    result["output_config"] = json!({"effort":effort});
                }
            } else {
                let budget = options
                    .thinking_budget_tokens
                    .filter(|value| value.get() != 0)
                    .map_or(1024, SafeUnsigned::get);
                result["thinking"] =
                    json!({"type":"enabled","budget_tokens":budget,"display":display});
            }
        } else if options.thinking_enabled == Some(false)
            && !model
                .thinking_level_map
                .as_ref()
                .is_some_and(|map| map.as_map().get(&ModelThinkingLevel::Off) == Some(&None))
        {
            result["thinking"] = json!({"type":"disabled"});
        }
    }
    if let Some(JsonValue::String(user_id)) = request
        .options
        .wire
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.as_map().get("user_id"))
    {
        result["metadata"] = json!({"user_id":user_id});
    }
    if let Some(choice) = &options.tool_choice {
        result["tool_choice"] = match choice {
            AnthropicToolChoice::Mode(mode) => json!({"type":mode}),
            AnthropicToolChoice::Named { .. } => json!(choice),
        };
    }
    if let Some(fallbacks) = &c.allowed_fallback_models
        && !fallbacks.is_empty()
    {
        result["fallbacks"] = json!(
            fallbacks
                .iter()
                .map(|fallback| json!({"model":fallback.model}))
                .collect::<Vec<_>>()
        );
    }
    // Unlike OpenAI, this pinned builder does not spread samplingParams.
    Ok(result)
}

fn input_block(block: &InputContent) -> Value {
    match block {
        InputContent::Text(text) => json!({"type":"text","text":text.text}),
        InputContent::Image(image) => {
            json!({"type":"image","source":{"type":"base64","media_type":image.mime_type,"data":image.data}})
        }
    }
}
fn convert_content(content: &[InputContent]) -> Value {
    if !content
        .iter()
        .any(|block| matches!(block, InputContent::Image(_)))
    {
        return json!(
            content
                .iter()
                .filter_map(|block| match block {
                    InputContent::Text(text) => Some(text.text.as_str()),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join("\n")
        );
    }
    let mut blocks: Vec<Value> = content.iter().map(input_block).collect();
    if !content
        .iter()
        .any(|block| matches!(block, InputContent::Text(_)))
    {
        blocks.insert(0, json!({"type":"text","text":"(see attached image)"}));
    }
    json!(blocks)
}

fn messages(
    messages: &[Message],
    oauth: bool,
    cache: Option<&Value>,
    allow_empty: bool,
    deferred: &BTreeSet<String>,
) -> Result<Vec<Value>, Failure> {
    let mut result = Vec::new();
    let mut loaded = BTreeSet::new();
    let mut i = 0;
    while i < messages.len() {
        match &messages[i] {
            Message::User(message) => match &message.content {
                UserContent::Text(text) if !text.trim_matches(ecma_whitespace).is_empty() => {
                    result.push(json!({"role":"user","content":text}))
                }
                UserContent::Blocks(blocks) => {
                    let content: Vec<Value> = blocks
                        .iter()
                        .filter(|block| match block {
                            InputContent::Text(text) => {
                                !text.text.trim_matches(ecma_whitespace).is_empty()
                            }
                            _ => true,
                        })
                        .map(input_block)
                        .collect();
                    if !content.is_empty() {
                        result.push(json!({"role":"user","content":content}));
                    }
                }
                _ => {}
            },
            Message::Assistant(message) => {
                let mut blocks = vec![];
                for block in &message.content {
                    match block {
                        AssistantContent::Text(text)=>{if !text.text.trim_matches(ecma_whitespace).is_empty(){blocks.push(json!({"type":"text","text":text.text}));}},
                        AssistantContent::Thinking(thinking)=>{
                            if thinking.redacted==Some(true) {let signature=thinking.thinking_signature.as_ref().ok_or(Failure::Protocol)?;blocks.push(json!({"type":"redacted_thinking","data":signature}));continue;}
                            let signature=thinking.thinking_signature.as_deref().filter(|s|!s.trim_matches(ecma_whitespace).is_empty());
                            if thinking.thinking.trim_matches(ecma_whitespace).is_empty() && signature.is_none() {continue;}
                            if signature.is_some() || allow_empty {blocks.push(json!({"type":"thinking","thinking":thinking.thinking,"signature":signature.unwrap_or("")}));}
                            else {blocks.push(json!({"type":"text","text":thinking.thinking}));}
                        },
                        AssistantContent::ToolCall(call)=>blocks.push(json!({"type":"tool_use","id":call.id,"name":outbound_name(call.name.as_str(),oauth),"input":call.arguments})),
                    }
                }
                if !blocks.is_empty() {
                    result.push(json!({"role":"assistant","content":blocks}));
                }
            }
            Message::ToolResult(_) => {
                let mut results = vec![];
                let mut siblings = vec![];
                while let Some(Message::ToolResult(message)) = messages.get(i) {
                    let mut references = vec![];
                    for name in message.added_tool_names.as_deref().unwrap_or_default() {
                        let name = outbound_name(name.as_str(), oauth);
                        if deferred.contains(&name) && loaded.insert(name.clone()) {
                            references.push(json!({"type":"tool_reference","tool_name":name}));
                        }
                    }
                    let content = convert_content(&message.content);
                    let content = if references.is_empty() {
                        content
                    } else {
                        match content {
                            Value::String(text) => {
                                siblings.push(json!({"type":"text","text":text}))
                            }
                            Value::Array(blocks) => siblings.extend(blocks),
                            _ => return Err(Failure::Protocol),
                        }
                        json!(references)
                    };
                    results.push(json!({"type":"tool_result","tool_use_id":message.tool_call_id,"content":content,"is_error":message.is_error}));
                    i += 1;
                }
                results.extend(siblings);
                result.push(json!({"role":"user","content":results}));
                continue;
            }
        }
        i += 1;
    }
    if let Some(cache) = cache
        && let Some(last) = result.last_mut()
        && last["role"] == "user"
    {
        if let Some(text) = last["content"].as_str() {
            last["content"] = json!([{"type":"text","text":text,"cache_control":cache}]);
        } else if let Some(last_block) = last["content"]
            .as_array_mut()
            .and_then(|blocks| blocks.last_mut())
            && matches!(
                last_block["type"].as_str(),
                Some("text" | "image" | "tool_result")
            )
        {
            add_cache(last_block, Some(cache));
        }
    }
    Ok(result)
}

fn convert_tools(
    tools: &[Tool],
    oauth: bool,
    c: &AnthropicMessagesCompat,
    cache: Option<&Value>,
    deferred: bool,
) -> Result<Vec<Value>, Failure> {
    tools.iter().enumerate().map(|(index,tool)|{
        let strict=resolve_json_schema_strict_sampling(tool,c.supports_strict_tools.unwrap_or(false)).map_err(|_|Failure::Protocol)?;
        let parameters=get_wire_json_schema_tool_parameters(tool,strict).map_err(|_|Failure::Protocol)?;
        let parameters=parameters.into_value();
        let mut schema=if strict==Some(true){parameters.clone()}else{json!({})};
        schema["type"]=json!("object");schema["properties"]=parameters.get("properties").filter(|v|!v.is_null()).cloned().unwrap_or(json!({}));
        schema["required"]=parameters.get("required").filter(|v|!v.is_null()).cloned().unwrap_or(json!([]));
        let mut result=json!({"name":outbound_name(tool.name.as_str(),oauth),"description":tool.description,"input_schema":schema});
        if c.supports_eager_tool_input_streaming.unwrap_or(true) {result["eager_input_streaming"]=json!(true);}
        if strict==Some(true){result["strict"]=json!(true);}
        if deferred{result["defer_loading"]=json!(true);}
        if index+1==tools.len(){add_cache(&mut result,cache);}
        Ok(result)
    }).collect()
}
