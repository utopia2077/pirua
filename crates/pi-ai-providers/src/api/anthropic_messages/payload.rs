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
use std::collections::BTreeMap;

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
pub(super) fn inbound_name(name: &str, context: &TranscriptContext, oauth: bool) -> String {
    if oauth {
        let lower = name.to_lowercase();
        if let Some(tool) = pi_ai_core::get_current_tools(&context.messages)
            .as_slice()
            .iter()
            .find(|tool| tool.name.as_str().to_lowercase() == lower)
        {
            return tool.name.as_str().into();
        }
    }
    name.into()
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

pub(super) struct BuiltParams {
    pub params: Value,
    pub has_preferred_strict_tools: bool,
}
struct StrictTools {
    disable_preferred: bool,
    has_preferred: bool,
}
fn prefers_strict(tool: &Tool) -> bool {
    matches!(
        tool.constrained_sampling,
        Some(ConstrainedSampling::Config(
            ConstrainedSamplingConfig::JsonSchema {
                strict: JsonSchemaStrictness::Prefer
            }
        ))
    )
}

pub(super) fn build(
    request: &TextStreamRequest,
    options: &AnthropicOptions,
    mapped_effort: Option<&str>,
    services: &AdapterServices,
    oauth: bool,
    disable_preferred_strict_tools: bool,
) -> Result<BuiltParams, Failure> {
    let model = &request.resolved.model;
    let c = compat(model)?;
    let cache = cache_control(request, services, &c);
    let mut strict_tools = StrictTools {
        disable_preferred: disable_preferred_strict_tools,
        has_preferred: false,
    };
    let mut context = resolve_transcript(
        request.context.clone(),
        c.supports_mid_convo_system_messages.unwrap_or(false),
    );
    context.messages = transform_messages(
        &context.messages,
        model,
        SafeUnsigned::new(request.options.runtime.clock.now_unix_ms()).unwrap_or(SafeUnsigned::MAX),
        Some(&mut IdNormalizer),
    )
    .map_err(|_| Failure::Protocol)?;
    let initial = get_initial_system_message(&context.messages);
    let initial_tools = initial
        .and_then(|system| system.tools_added.as_deref())
        .unwrap_or_default();
    let native = c.supports_mid_convo_system_messages == Some(true)
        && c.supports_mid_convo_tool_changes == Some(true)
        && !initial_tools.is_empty();
    let conversation = if initial.is_some() {
        &context.messages[1..]
    } else {
        context.messages.as_slice()
    };
    let (converted_messages, historical_efforts) = messages(
        conversation,
        oauth,
        cache.as_ref(),
        c.allow_empty_signature.unwrap_or(false),
        native.then_some(&c),
        &model.provider,
        &mut strict_tools,
    )?;
    let mut converted = Vec::new();
    let thinking_disabled = model.reasoning
        && options.thinking_enabled == Some(false)
        && model
            .thinking_level_map
            .as_ref()
            .is_none_or(|map| map.as_map().get(&ModelThinkingLevel::Off) != Some(&None));
    if c.supports_mid_convo_effort == Some(true) && !thinking_disabled {
        let active = mapped_effort
            .map(str::to_owned)
            .or_else(|| {
                options.effort.map(|effort| {
                    serde_json::to_value(effort)
                        .expect("effort")
                        .as_str()
                        .expect("string effort")
                        .to_owned()
                })
            })
            .unwrap_or_else(|| "high".into());
        for (index, message) in converted_messages.into_iter().enumerate() {
            if let Some(effort) = historical_efforts.get(&index) {
                converted
                    .push(json!({"role":"system","content":[],"output_config":{"effort":effort}}));
            }
            converted.push(message);
        }
        converted.push(json!({"role":"system","content":[],"output_config":{"effort":active}}));
    } else {
        converted = converted_messages;
    }
    let mut result = json!({"model":model.id,"messages":converted,
        "max_tokens":request.options.wire.max_tokens.unwrap_or(model.max_tokens),"stream":true});
    let betas = beta_features(request, options, &c, oauth, native);
    if !betas.is_empty() {
        result["betas"] = json!(betas);
    }
    let mut system = vec![];
    if oauth {
        let mut identity = json!({"type":"text","text":"You are Claude Code, Anthropic's official CLI for Claude."});
        add_cache(&mut identity, cache.as_ref());
        system.push(identity);
    }
    let text = initial.map(get_system_message_text).unwrap_or_default();
    if !text.is_empty() {
        let mut block = json!({"type":"text","text":text});
        add_cache(&mut block, cache.as_ref());
        system.push(block);
    }
    if !system.is_empty() {
        result["system"] = json!(system);
    }
    if options.thinking_enabled != Some(true)
        && c.supports_mid_convo_effort != Some(true)
        && c.supports_temperature.unwrap_or(true)
        && let Some(temperature) = request.options.wire.temperature
    {
        result["temperature"] = json!(temperature);
    }
    let tools = if native {
        initial_tools.to_vec()
    } else {
        get_current_tools(&context.messages)
    };
    let mut tools = convert_tools(
        &tools,
        oauth,
        &c,
        cache
            .as_ref()
            .filter(|_| c.supports_cache_control_on_tools.unwrap_or(true)),
        false,
        &mut strict_tools,
    )?;
    if native {
        tools.push(json!({"name":"__pi_deferred_placeholder__","description":"Reserved placeholder. Never available. Never call this.","input_schema":{"type":"object","properties":{},"required":[]},"defer_loading":true}));
    }
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
    if c.supports_mid_convo_effort == Some(true) && !thinking_disabled {
        result["thinking"] = json!({"type":"adaptive","display":options.thinking_display.unwrap_or(AnthropicThinkingDisplay::Summarized),"block_binding":{"prefix_mismatch_behavior":"drop_block"}});
        result["output_config"] = json!({"effort":"high"});
    }
    // Anthropic does not spread samplingParams.
    Ok(BuiltParams {
        params: result,
        has_preferred_strict_tools: strict_tools.has_preferred,
    })
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
    native: Option<&AnthropicMessagesCompat>,
    provider: &ProviderId,
    strict_tools: &mut StrictTools,
) -> Result<(Vec<Value>, BTreeMap<usize, String>), Failure> {
    let mut result = Vec::new();
    let mut historical_efforts = BTreeMap::new();
    let mut pending = Vec::new();
    let mut i = 0;
    while i < messages.len() {
        match &messages[i] {
            Message::System(system) => {
                let mut blocks = Vec::new();
                let text = render_system_message_update(system);
                if !text.is_empty() {
                    blocks.push(json!({"type":"text","text":text}));
                }
                if let Some(c) = native {
                    let added = system.tools_added.as_deref().unwrap_or_default();
                    for removed in system.tools_removed.iter().flatten() {
                        if !added.iter().any(|tool| tool.name == removed.name) {
                            blocks.push(json!({"type":"tool_removal","tool":{"type":"tool_reference","name":outbound_name(removed.name.as_str(),oauth)}}));
                        }
                    }
                    for definition in convert_tools(added, oauth, c, None, false, strict_tools)? {
                        blocks.push(json!({"type":"tool_addition","tool":{"type":"tool_definition","definition":definition}}));
                    }
                }
                if !blocks.is_empty() {
                    pending.push(json!({"role":"system","content":blocks}));
                }
            }

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
                result.append(&mut pending);
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
                    if message.provider == *provider
                        && message.api.as_str() == "anthropic-messages"
                        && let Some(effort) = &message.provider_thinking_level
                        && matches!(effort.as_str(), "low" | "medium" | "high" | "xhigh" | "max")
                    {
                        historical_efforts.insert(result.len(), effort.clone());
                    }
                    result.push(json!({"role":"assistant","content":blocks}));
                }
            }
            Message::ToolResult(_) => {
                let mut results = vec![];
                let mut error_images = vec![];

                while let Some(Message::ToolResult(message)) = messages.get(i) {
                    let content = if message.is_error
                        && message
                            .content
                            .iter()
                            .any(|block| matches!(block, InputContent::Image(_)))
                    {
                        let mut text = Vec::new();
                        let mut previous_placeholder = false;
                        error_images.push(json!({"type":"text","text":format!("Images from the error result of tool call {} ({}):", message.tool_call_id, message.tool_name)}));
                        for block in &message.content {
                            match block {
                                InputContent::Image(_) => {
                                    if !previous_placeholder {
                                        text.push(
                                            "(image attached after the tool results)".to_owned(),
                                        );
                                    }
                                    previous_placeholder = true;
                                    error_images.push(input_block(block));
                                }
                                InputContent::Text(block) => {
                                    text.push(block.text.clone());
                                    previous_placeholder =
                                        block.text == "(image attached after the tool results)";
                                }
                            }
                        }
                        json!(text.join("\n"))
                    } else {
                        convert_content(&message.content)
                    };
                    results.push(json!({"type":"tool_result","tool_use_id":message.tool_call_id,"content":content,"is_error":message.is_error}));
                    i += 1;
                }

                results.extend(error_images);
                result.push(json!({"role":"user","content":results}));
                continue;
            }
        }
        i += 1;
    }
    result.append(&mut pending);
    if let Some(cache) = cache
        && let Some(last) = result.last_mut()
        && (last["role"] == "user" || last["role"] == "system")
    {
        if let Some(text) = last["content"].as_str() {
            last["content"] = json!([{"type":"text","text":text,"cache_control":cache}]);
        } else if let Some(last_block) = last["content"]
            .as_array_mut()
            .and_then(|blocks| blocks.last_mut())
            && matches!(
                last_block["type"].as_str(),
                Some("text" | "image" | "tool_result" | "tool_addition" | "tool_removal")
            )
        {
            add_cache(last_block, Some(cache));
        }
    }
    Ok((result, historical_efforts))
}

fn convert_tools(
    tools: &[Tool],
    oauth: bool,
    c: &AnthropicMessagesCompat,
    cache: Option<&Value>,
    deferred: bool,
    strict_tools: &mut StrictTools,
) -> Result<Vec<Value>, Failure> {
    tools.iter().enumerate().map(|(index,tool)|{
        let strict=if strict_tools.disable_preferred && prefers_strict(tool) {None} else {
            resolve_json_schema_strict_sampling(tool,c.supports_strict_tools.unwrap_or(false)).map_err(|_|Failure::Protocol)?
        };
        strict_tools.has_preferred |= strict == Some(true) && prefers_strict(tool);
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

pub(super) fn beta_features(
    request: &TextStreamRequest,
    options: &AnthropicOptions,
    c: &AnthropicMessagesCompat,
    oauth: bool,
    native: bool,
) -> Vec<String> {
    let mut configured = None;
    if let Some(headers) = &request.resolved.model.headers {
        for (name, value) in headers.as_map() {
            if name.eq_ignore_ascii_case("anthropic-beta") {
                configured = Some(Some(value.as_str()));
            }
        }
    }
    for (name, value) in request.auth.headers.iter_for_dispatch() {
        if name.eq_ignore_ascii_case("anthropic-beta") {
            configured = Some(value.as_deref());
        }
    }
    if let Some(configured) = configured {
        let mut features = Vec::new();
        for feature in configured
            .unwrap_or_default()
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            if !features.iter().any(|old| old == feature) {
                features.push(feature.to_owned());
            }
        }
        return features;
    }
    let mut features = Vec::new();
    if oauth {
        features.extend(["claude-code-20250219", "oauth-2025-04-20"].map(str::to_owned));
    }
    if !get_current_tools(&request.context.messages).is_empty()
        && !c.supports_eager_tool_input_streaming.unwrap_or(true)
    {
        features.push("fine-grained-tool-streaming-2025-05-14".into());
    }
    if request.resolved.model.reasoning
        && options.thinking_enabled == Some(true)
        && options.interleaved_thinking.unwrap_or(true)
        && c.force_adaptive_thinking != Some(true)
    {
        features.push("interleaved-thinking-2025-05-14".into());
    }
    if c.allowed_fallback_models
        .as_ref()
        .is_some_and(|models| !models.is_empty())
    {
        features.push("server-side-fallback-2026-07-01".into());
    }
    if c.supports_mid_convo_effort == Some(true) {
        features.extend(
            [
                "mid-conversation-output-config-2026-07-01",
                "thinking-binding-controls-2026-08-01",
            ]
            .map(str::to_owned),
        );
    }
    if native {
        features.push("inline-tools-2026-09-15".into());
    }
    features
}
