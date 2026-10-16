use std::collections::BTreeMap;

use pi_ai_core::{
    AssistantContent, AssistantMessage, InputContent, JsonValue, Message, Model, ModelInput,
    SafeUnsigned, Tool, ToolCallId, ToolCallIdNormalizer, TranscriptContext, short_hash,
    transform_messages,
};
use serde_json::{Value, json};

use super::CompletionsError;
use crate::api::constrained_sampling::{
    get_grammar_tool_input, get_wire_json_schema_tool_parameters,
    resolve_grammar_constrained_sampling, resolve_json_schema_strict_sampling,
};
use crate::api::wire_json::stringify_wire_json;

/// Resolved source compatibility. Created from public model settings and the transient endpoint.
#[derive(Clone)]
pub struct ResolvedCompletionsCompat(pub(super) Value);
impl std::fmt::Debug for ResolvedCompletionsCompat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ResolvedCompletionsCompat([REDACTED])")
    }
}
impl ResolvedCompletionsCompat {
    pub fn resolve(model: &Model, endpoint: &pi_ai_core::RequestEndpoint) -> Self {
        let p = model.provider.as_str();
        let u = endpoint.expose_for_transport();
        let zai = p == "zai"
            || p == "zai-coding-cn"
            || u.contains("api.z.ai")
            || u.contains("open.bigmodel.cn");
        let together =
            p == "together" || u.contains("api.together.ai") || u.contains("api.together.xyz");
        let moon = p == "moonshotai" || p == "moonshotai-cn" || u.contains("api.moonshot.");
        let router = p == "openrouter" || u.contains("openrouter.ai");
        let workers = p == "cloudflare-workers-ai" || u.contains("api.cloudflare.com");
        let gateway = p == "cloudflare-ai-gateway" || u.contains("gateway.ai.cloudflare.com");
        let nvidia = p == "nvidia" || u.contains("integrate.api.nvidia.com");
        let ant = p == "ant-ling" || u.contains("api.ant-ling.com");
        let deep = p == "deepseek" || u.to_ascii_lowercase().contains("deepseek.com");
        let grok = p == "xai" || u.contains("api.x.ai");
        let nonstandard = nvidia
            || p == "cerebras"
            || u.contains("cerebras.ai")
            || grok
            || together
            || u.contains("chutes.ai")
            || deep
            || zai
            || moon
            || p == "opencode"
            || u.contains("opencode.ai")
            || workers
            || gateway
            || ant;
        let max =
            u.contains("chutes.ai") || deep || moon || gateway || together || nvidia || ant || zai;
        let mut value = json!({
            "supportsStore":!nonstandard,
            "supportsDeveloperRole": (router && (model.id.as_str().starts_with("anthropic/") || model.id.as_str().starts_with("openai/"))) || (!nonstandard && !router),
            "supportsReasoningEffort": !(grok || zai || moon || together || gateway || nvidia || ant),
            "supportsUsageInStreaming":true,"supportsFinishReason":true,
            "maxTokensField":if max {"max_tokens"} else {"max_completion_tokens"},
            "requiresToolResultName":false,"requiresAssistantAfterToolResult":false,"requiresThinkingAsText":false,
            "requiresReasoningContentOnAssistantMessages":deep,
            "thinkingFormat":if deep {"deepseek"} else if zai {"zai"} else if together {"together"} else if ant {"ant-ling"} else if router {"openrouter"} else {"openai"},
            "openRouterRouting":{},"vercelGatewayRouting":{},"chatTemplateKwargs":{},"chatTemplateArgs":{},
            "zaiToolStream":false,"supportsThinkingTokenBudget":false,"supportsStrictMode":false,
            "supportsOpenAIGrammarTools":false,"sendSessionAffinityHeaders":false,
            "sessionAffinityFormat":if router {"openrouter"} else {"openai"},
            "supportsLongCacheRetention":!(together || workers || gateway || nvidia || ant)
        });
        if p == "openrouter" && model.id.as_str().starts_with("anthropic/") {
            value["cacheControlFormat"] = json!("anthropic");
        }
        if let Some(compat) = &model.compat {
            for (k, v) in compat.0.as_map() {
                if !matches!(v, JsonValue::Null) {
                    value[k] = json!(v);
                }
            }
        }
        Self(value)
    }
    pub(super) fn flag(&self, key: &str) -> bool {
        self.0[key].as_bool().unwrap_or(false)
    }
    pub(super) fn text(&self, key: &str) -> &str {
        self.0[key].as_str().unwrap_or("")
    }
}

#[derive(Default)]
pub struct ConvertCompletionsMessagesOptions<'a> {
    pub grammar_tool_input_properties: Option<&'a BTreeMap<String, String>>,
    /// Only repairs need a timestamp; it never appears in wire messages.
    pub synthetic_result_timestamp: Option<SafeUnsigned>,
}

struct Normalizer {
    invalid: bool,
}
impl ToolCallIdNormalizer for Normalizer {
    fn normalize_tool_call_id(
        &mut self,
        id: &ToolCallId,
        model: &Model,
        _source: &AssistantMessage,
    ) -> ToolCallId {
        let raw = id.as_str();
        let normalized = if let Some((call, item)) = raw.split_once('|') {
            let sanitize = |s: &str| {
                s.encode_utf16()
                    .map(|c| {
                        if matches!(c, 48..=57 | 65..=90 | 97..=122 | 45 | 95) {
                            char::from_u32(u32::from(c)).unwrap_or('_')
                        } else {
                            '_'
                        }
                    })
                    .collect::<String>()
            };
            let call = sanitize(call);
            let item = sanitize(item);
            let joined = if item.is_empty() {
                call.clone()
            } else {
                format!("{call}_{item}")
            };
            if joined.len() <= 40 {
                joined
            } else {
                let hash = short_hash(raw);
                let hash = &hash[..hash.len().min(8)];
                format!("{}_{}", &call[..call.len().min(40 - hash.len() - 1)], hash)
            }
        } else if model.provider.as_str() == "openai" {
            String::from_utf16_lossy(&raw.encode_utf16().take(40).collect::<Vec<_>>())
        } else {
            raw.to_owned()
        };
        ToolCallId::new(normalized).unwrap_or_else(|_| {
            self.invalid = true;
            id.clone()
        })
    }
}
pub(super) fn nonblank(s: &str) -> bool {
    s.chars().any(|c| !matches!(c, '\u{9}'..='\u{d}' | ' ' | '\u{a0}' | '\u{1680}' | '\u{2000}'..='\u{200a}' | '\u{2028}' | '\u{2029}' | '\u{202f}' | '\u{205f}' | '\u{3000}' | '\u{feff}'))
}
pub(super) fn valid_detail(v: &Value) -> bool {
    let Some(o) = v.as_object() else { return false };
    if o.get("id").is_some_and(|v| !v.is_null() && !v.is_string())
        || o.get("format").is_some_and(|v| !v.is_string())
        || o.get("index").is_some_and(|v| !v.is_number())
    {
        return false;
    }
    match v["type"].as_str() {
        Some("reasoning.summary") => v["summary"].is_string(),
        Some("reasoning.encrypted") => v["data"].is_string(),
        Some("reasoning.text") => {
            v["text"].is_string()
                && o.get("signature")
                    .is_none_or(|v| v.is_null() || v.is_string())
        }
        _ => false,
    }
}
fn details(signature: Option<&str>) -> Option<Vec<Value>> {
    let Value::Array(values) = serde_json::from_str(signature?).ok()? else {
        return None;
    };
    (!values.is_empty() && values.iter().all(valid_detail)).then_some(values)
}
fn image_wire(v: &pi_ai_core::ImageContent) -> Value {
    json!({"type":"image_url","image_url":{"url":format!("data:{};base64,{}",v.mime_type,v.data)}})
}
fn input_wire(v: &InputContent) -> Value {
    match v {
        InputContent::Text(t) => json!({"type":"text","text":t.text}),
        InputContent::Image(i) => image_wire(i),
    }
}

pub(super) fn convert_tools(
    tools: &[Tool],
    compat: &ResolvedCompletionsCompat,
) -> Result<Vec<Value>, CompletionsError> {
    tools.iter().map(|tool| {
        if let Some(g) = resolve_grammar_constrained_sampling(tool, compat.flag("supportsOpenAIGrammarTools")).map_err(|_| CompletionsError::Conversion)? {
            return Ok(json!({"type":"custom","custom":{"name":tool.name,"description":tool.description,"format":{"type":"grammar","grammar":{"syntax":g.format,"definition":g.definition}}}}));
        }
        let strict = resolve_json_schema_strict_sampling(tool, compat.flag("supportsStrictMode")).map_err(|_| CompletionsError::Conversion)?;
        let params = get_wire_json_schema_tool_parameters(tool, strict).map_err(|_| CompletionsError::Conversion)?;
        let mut value = json!({"type":"function","function":{"name":tool.name,"description":tool.description,"parameters":params.as_value()}});
        if compat.flag("supportsStrictMode") {value["function"]["strict"] = json!(strict.unwrap_or(false));}
        Ok(value)
    }).collect()
}

/// Converts transcript after the accepted source repair and tool-id normalization.
pub fn convert_messages(
    model: &Model,
    context: &TranscriptContext,
    compat: &ResolvedCompletionsCompat,
    options: &ConvertCompletionsMessagesOptions<'_>,
) -> Result<Vec<Value>, CompletionsError> {
    let mut normalizer = Normalizer { invalid: false };
    let context = pi_ai_core::resolve_transcript(
        context.clone(),
        compat.flag("supportsMidConvoSystemMessages"),
    );
    let transformed = transform_messages(
        &context.messages,
        model,
        options
            .synthetic_result_timestamp
            .unwrap_or(SafeUnsigned::new(0).expect("constant")),
        Some(&mut normalizer),
    )
    .map_err(|_| CompletionsError::Conversion)?;
    if normalizer.invalid {
        return Err(CompletionsError::Conversion);
    }
    let mut params = Vec::new();
    let support = if compat.flag("supportsMidConvoSystemMessages")
        && compat.flag("supportsMidConvoToolAdditions")
    {
        pi_ai_core::TranscriptToolSupport::Additions
    } else {
        pi_ai_core::TranscriptToolSupport::None
    };
    let placement = pi_ai_core::resolve_transcript_tools(&context.messages, support);
    let mut system_index = 0;
    let mut last_tool = false;
    let mut i = 0;
    while i < transformed.len() {
        let msg = &transformed[i];
        if last_tool
            && compat.flag("requiresAssistantAfterToolResult")
            && matches!(msg, Message::User(_))
        {
            params.push(json!({"role":"assistant","content":"I have processed the tool results."}));
        }
        match msg {
            Message::System(system) => {
                let added = if system_index >= placement.folded_system_messages {
                    system.tools_added.as_deref().unwrap_or_default()
                } else {
                    &[]
                };
                system_index += 1;
                if !added.is_empty() {
                    params.push(json!({"role":"system","tools":convert_tools(added,compat)?}));
                }
                let text = if i == 0 {
                    pi_ai_core::get_system_message_text(system)
                } else {
                    pi_ai_core::render_system_message_update(system)
                };
                if !text.is_empty() {
                    params.push(json!({"role":if model.reasoning&&compat.flag("supportsDeveloperRole"){"developer"}else{"system"},"content":text}));
                }
            }
            Message::User(user) => {
                let content = match &user.content {
                    pi_ai_core::UserContent::Text(t) => json!(t),
                    pi_ai_core::UserContent::Blocks(blocks) => {
                        if blocks.is_empty() {
                            i += 1;
                            continue;
                        }
                        json!(blocks.iter().map(input_wire).collect::<Vec<_>>())
                    }
                };
                params.push(json!({"role":"user","content":content}));
                last_tool = false;
            }
            Message::Assistant(a) => {
                let text: Vec<_> = a
                    .content
                    .iter()
                    .filter_map(|b| match b {
                        AssistantContent::Text(t) if nonblank(&t.text) => {
                            Some(json!({"type":"text","text":t.text}))
                        }
                        _ => None,
                    })
                    .collect();
                let joined = text
                    .iter()
                    .filter_map(|v| v["text"].as_str())
                    .collect::<String>();
                let thinking: Vec<_> = a
                    .content
                    .iter()
                    .filter_map(|b| match b {
                        AssistantContent::Thinking(t) => Some(t),
                        _ => None,
                    })
                    .collect();
                let calls: Vec<_> = a
                    .content
                    .iter()
                    .filter_map(|b| match b {
                        AssistantContent::ToolCall(t) => Some(t),
                        _ => None,
                    })
                    .collect();
                let legacy: Vec<_> = calls
                    .iter()
                    .filter_map(|t| {
                        serde_json::from_str::<Value>(t.thought_signature.as_deref()?).ok()
                    })
                    .filter(|v| {
                        valid_detail(v)
                            && v["type"] == "reasoning.encrypted"
                            && v["id"].as_str().is_some_and(|s| !s.is_empty())
                            && v["data"].as_str().is_some_and(|s| !s.is_empty())
                    })
                    .collect();
                let preserved = thinking
                    .iter()
                    .find_map(|t| details(t.thinking_signature.as_deref()))
                    .or_else(|| (!legacy.is_empty()).then_some(legacy));
                let thinking: Vec<_> = thinking
                    .into_iter()
                    .filter(|t| nonblank(&t.thinking))
                    .collect();
                let mut wire = json!({"role":"assistant","content":if compat.flag("requiresAssistantAfterToolResult") {json!("")} else {Value::Null}});
                if !thinking.is_empty() && compat.flag("requiresThinkingAsText") {
                    let mut parts = vec![
                        json!({"type":"text","text":thinking.iter().map(|t| t.thinking.as_str()).collect::<Vec<_>>().join("\n\n")}),
                    ];
                    parts.extend(text);
                    wire["content"] = json!(parts);
                } else {
                    if !joined.is_empty() {
                        wire["content"] = json!(joined);
                    }
                    if !thinking.is_empty()
                        && preserved.is_none()
                        && let Some(mut signature) = thinking[0].thinking_signature.as_deref()
                    {
                        if model.provider.as_str() == "opencode-go" && signature == "reasoning" {
                            signature = "reasoning_content";
                        }
                        if matches!(
                            signature,
                            "reasoning" | "reasoning_content" | "reasoning_text"
                        ) {
                            wire[signature] = json!(
                                thinking
                                    .iter()
                                    .map(|t| t.thinking.as_str())
                                    .collect::<Vec<_>>()
                                    .join("\n")
                            );
                        }
                    }
                }
                if !calls.is_empty() {
                    wire["tool_calls"] = Value::Array(calls.into_iter().map(|t| {
                        if let Some(property) = options.grammar_tool_input_properties.and_then(|m|m.get(t.name.as_str())) {
                            let input = get_grammar_tool_input(t.name.as_str(), &t.arguments, property).map_err(|_|CompletionsError::Conversion)?;
                            Ok(json!({"id":t.id,"type":"custom","custom":{"name":t.name,"input":input}}))
                        } else {Ok(json!({"id":t.id,"type":"function","function":{"name":t.name,"arguments":stringify_wire_json(&json!(t.arguments)).map_err(|_|CompletionsError::ResourceLimit)?}}))}
                    }).collect::<Result<Vec<_>,CompletionsError>>()?);
                }
                if let Some(details) = preserved {
                    wire["reasoning_details"] = json!(details);
                }
                if compat.flag("requiresReasoningContentOnAssistantMessages")
                    && model.reasoning
                    && wire.get("reasoning_content").is_none()
                {
                    wire["reasoning_content"] = json!("");
                }
                let has = wire["content"].as_str().is_some_and(|s| !s.is_empty())
                    || wire["content"].as_array().is_some_and(|a| !a.is_empty());
                if !has && wire.get("tool_calls").is_none() {
                    i += 1;
                    continue;
                }
                params.push(wire);
                last_tool = false;
            }
            Message::ToolResult(_) => {
                let mut images = Vec::new();

                while let Some(Message::ToolResult(t)) = transformed.get(i) {
                    let text = t
                        .content
                        .iter()
                        .filter_map(|v| match v {
                            InputContent::Text(t) => Some(t.text.as_str()),
                            _ => None,
                        })
                        .collect::<Vec<_>>()
                        .join("\n");
                    let has_images = t
                        .content
                        .iter()
                        .any(|v| matches!(v, InputContent::Image(_)));
                    let result = if !text.trim().is_empty() {
                        text.as_str()
                    } else if has_images {
                        "(see attached image)"
                    } else {
                        "(no tool output)"
                    };
                    let mut wire =
                        json!({"role":"tool","content":result,"tool_call_id":t.tool_call_id});
                    if compat.flag("requiresToolResultName") {
                        wire["name"] = json!(t.tool_name);
                    }
                    params.push(wire);
                    if model.input.contains(&ModelInput::Image) {
                        images.extend(t.content.iter().filter_map(|b| match b {
                            InputContent::Image(v) => Some(image_wire(v)),
                            _ => None,
                        }));
                    }
                    i += 1;
                }
                last_tool = images.is_empty();
                if !images.is_empty() {
                    if compat.flag("requiresAssistantAfterToolResult") {
                        params.push(json!({"role":"assistant","content":"I have processed the tool results."}));
                    }
                    let mut content =
                        vec![json!({"type":"text","text":"Attached image(s) from tool result:"})];
                    content.extend(images);
                    params.push(json!({"role":"user","content":content}));
                }
                continue;
            }
        }
        i += 1;
    }
    Ok(params)
}
