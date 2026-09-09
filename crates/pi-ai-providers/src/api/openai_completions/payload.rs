use super::convert::convert_tools;
use super::{
    CompletionsError, ConvertCompletionsMessagesOptions, OpenAICompletionsOptions,
    ResolvedCompletionsCompat, convert_messages,
};
use crate::api::openai_prompt_cache::clamp_openai_prompt_cache_key;
use crate::api::simple_options::{clamp_thinking_budget_to_answer_room, thinking_budget_for_level};
use pi_ai_core::{
    CacheRetention, Message, Model, ModelThinkingLevel, TextStreamRequest, ThinkingLevel,
};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

pub(super) fn model_level(level: ThinkingLevel) -> ModelThinkingLevel {
    match level {
        ThinkingLevel::Minimal => ModelThinkingLevel::Minimal,
        ThinkingLevel::Low => ModelThinkingLevel::Low,
        ThinkingLevel::Medium => ModelThinkingLevel::Medium,
        ThinkingLevel::High => ModelThinkingLevel::High,
        ThinkingLevel::ExtraHigh => ModelThinkingLevel::ExtraHigh,
        ThinkingLevel::Max => ModelThinkingLevel::Max,
    }
}
fn mapped(model: &Model, effort: Option<ThinkingLevel>) -> Option<Option<&str>> {
    model
        .thinking_level_map
        .as_ref()?
        .as_map()
        .get(&effort.map(model_level).unwrap_or(ModelThinkingLevel::Off))
        .map(|v| v.as_deref())
}
fn mapped_or(model: &Model, effort: Option<ThinkingLevel>, fallback: Value) -> Value {
    mapped(model, effort)
        .flatten()
        .map_or(fallback, |s| json!(s))
}
fn template_values(
    model: &Model,
    options: &OpenAICompletionsOptions,
    values: &Value,
    budget: Option<u64>,
) -> Option<Value> {
    let mut result = serde_json::Map::new();
    for (key, value) in values.as_object()? {
        if !value.is_object() {
            result.insert(key.clone(), value.clone());
            continue;
        }
        if options.reasoning_effort.is_none() && value["omitWhenOff"] == true {
            continue;
        }
        let resolved = match value["$var"].as_str() {
            Some("thinking.enabled") => Some(json!(options.reasoning_effort.is_some())),
            Some("thinking.budget") => budget.map(|b| json!(b)),
            _ => match mapped(model, options.reasoning_effort) {
                Some(Some(s)) => Some(json!(s)),
                Some(None) => None,
                None => options.reasoning_effort.map(|e| json!(e)),
            },
        };
        if let Some(v) = resolved {
            result.insert(key.clone(), v);
        }
    }
    (!result.is_empty()).then_some(Value::Object(result))
}
fn cache_text(message: &mut Value, control: &Value) -> bool {
    let Some(content) = message.get_mut("content") else {
        return false;
    };
    if let Some(text) = content.as_str() {
        if text.is_empty() {
            return false;
        }
        *content = json!([{"type":"text","text":text,"cache_control":control}]);
        return true;
    }
    if let Some(parts) = content.as_array_mut() {
        for part in parts.iter_mut().rev() {
            if part["type"] == "text" {
                part["cache_control"] = control.clone();
                return true;
            }
        }
    }
    false
}
pub(super) fn build(
    request: &TextStreamRequest,
    options: &OpenAICompletionsOptions,
    compat: &ResolvedCompletionsCompat,
    cache: CacheRetention,
    grammar: &BTreeMap<String, String>,
) -> Result<Value, CompletionsError> {
    let model = &request.resolved.model;
    let wire = &request.options.wire;
    let mut messages = convert_messages(
        model,
        &request.context,
        compat,
        &ConvertCompletionsMessagesOptions {
            grammar_tool_input_properties: Some(grammar),
            ..Default::default()
        },
    )?;
    // The source reserves messages before stream and later optional fields.
    let mut params = json!({"model":model.id,"messages":Value::Null,"stream":true});
    let long = cache == CacheRetention::Long && compat.flag("supportsLongCacheRetention");
    if ((request
        .resolved
        .endpoint
        .expose_for_transport()
        .contains("api.openai.com")
        && cache != CacheRetention::None)
        || long)
        && let Some(key) = clamp_openai_prompt_cache_key(wire.session_id.as_deref())
    {
        params["prompt_cache_key"] = json!(key);
    }
    if long {
        params["prompt_cache_retention"] = json!("24h");
    }
    if compat.flag("supportsUsageInStreaming") {
        params["stream_options"] = json!({"include_usage":true});
    }
    if compat.flag("supportsStore") {
        params["store"] = json!(false);
    }
    if let Some(max) = wire.max_tokens.filter(|v| v.get() != 0) {
        params[compat.text("maxTokensField")] = json!(max);
    }
    if let Some(temp) = wire.temperature {
        params["temperature"] = json!(temp);
    }
    let mut deferred = BTreeSet::new();
    if compat.text("deferredToolsMode") == "kimi" {
        for msg in &request.context.messages {
            if let Message::ToolResult(t) = msg {
                deferred.extend(
                    t.added_tool_names
                        .as_deref()
                        .unwrap_or_default()
                        .iter()
                        .cloned(),
                );
            }
        }
    }
    let active: Vec<_> = request
        .context
        .tools
        .as_deref()
        .unwrap_or_default()
        .iter()
        .filter(|t| !deferred.contains(&t.name))
        .cloned()
        .collect();
    if !active.is_empty() {
        params["tools"] = json!(convert_tools(&active,compat)?);
        if compat.flag("zaiToolStream") {params["tool_stream"] = json!(true);}
    } else if request.context.messages.iter().any(|m|matches!(m,Message::ToolResult(_)) || matches!(m,Message::Assistant(a) if a.content.iter().any(|b|matches!(b,pi_ai_core::AssistantContent::ToolCall(_))))) {params["tools"] = json!([]);}
    if compat.text("cacheControlFormat") == "anthropic" && cache != CacheRetention::None {
        let mut control = json!({"type":"ephemeral"});
        if long {
            control["ttl"] = json!("1h");
        }
        if let Some(m) = messages
            .iter_mut()
            .find(|m| matches!(m["role"].as_str(), Some("system" | "developer")))
        {
            cache_text(m, &control);
        }
        if let Some(t) = params
            .get_mut("tools")
            .and_then(Value::as_array_mut)
            .and_then(|t| t.last_mut())
        {
            t["cache_control"] = control.clone();
        }
        for m in messages.iter_mut().rev() {
            if matches!(m["role"].as_str(), Some("user" | "assistant" | "tool"))
                && cache_text(m, &control)
            {
                break;
            }
        }
    }
    params["messages"] = json!(messages);
    if let Some(choice) = &options.tool_choice {
        params["tool_choice"] = json!(choice);
    }
    let effort = options.reasoning_effort;
    let enabled = effort.is_some();
    let budget = effort
        .filter(|_| model.reasoning)
        .map(|level| {
            clamp_thinking_budget_to_answer_room(
                thinking_budget_for_level(level, options.thinking_budgets.as_ref()),
                wire.max_tokens
                    .filter(|v| v.get() != 0)
                    .unwrap_or(model.max_tokens),
            )
        })
        .filter(|v| v.get() > 0)
        .map(|v| v.get());
    if model.reasoning {
        let format = compat.text("thinkingFormat");
        let supports = compat.flag("supportsReasoningEffort");
        let requested = effort.map(|v| json!(v)).unwrap_or(Value::Null);
        let normal_effort = mapped_or(model, effort, requested.clone());
        match format {
            "zai" => {
                params["thinking"] = if enabled {
                    json!({"type":"enabled","clear_thinking":false})
                } else {
                    json!({"type":"disabled"})
                };
                if enabled && supports && mapped(model, effort) != Some(None) {
                    params["reasoning_effort"] = normal_effort;
                }
            }
            "qwen" => {
                params["enable_thinking"] = json!(enabled);
                if enabled && supports {
                    params["reasoning_effort"] = normal_effort;
                }
            }
            "qwen-chat-template" => {
                params["chat_template_kwargs"] =
                    json!({"enable_thinking":enabled,"preserve_thinking":true})
            }
            "chat-template" | "baseten" => {
                let (key, source) = if format == "baseten" {
                    ("chat_template_args", "chatTemplateArgs")
                } else {
                    ("chat_template_kwargs", "chatTemplateKwargs")
                };
                if let Some(value) = template_values(model, options, &compat.0[source], budget) {
                    params[key] = value;
                }
                if format == "baseten"
                    && supports
                    && normal_effort.is_string()
                    && mapped(model, effort) != Some(None)
                {
                    params["reasoning_effort"] = normal_effort;
                }
            }
            "deepseek" => {
                if enabled {
                    params["thinking"] = json!({"type":"enabled"});
                } else if mapped(model, None) != Some(None) {
                    params["thinking"] = json!({"type":"disabled"});
                }
                if enabled && supports {
                    params["reasoning_effort"] = normal_effort;
                }
            }
            "openrouter" => {
                if enabled {
                    params["reasoning"] = json!({"effort":normal_effort});
                } else if mapped(model, None) != Some(None) {
                    params["reasoning"] = json!({"effort":mapped_or(model,None,json!("none"))});
                }
            }
            "ant-ling" if enabled => {
                if let Some(Some(value)) = mapped(model, effort) {
                    params["reasoning"] = json!({"effort":value});
                }
            }
            "together" => {
                params["reasoning"] = json!({"enabled":enabled});
                if enabled && supports {
                    params["reasoning_effort"] = normal_effort;
                }
            }
            "string-thinking" => {
                if enabled {
                    params["thinking"] = normal_effort;
                } else if mapped(model, None) != Some(None) {
                    params["thinking"] = mapped_or(model, None, json!("none"));
                }
            }
            _ => {
                if enabled && supports {
                    params["reasoning_effort"] = normal_effort;
                } else if supports && let Some(Some(value)) = mapped(model, None) {
                    params["reasoning_effort"] = json!(value);
                }
            }
        }
    }
    let field = compat.text("thinkingTokenBudgetField");
    if let Some(budget) = budget {
        if !field.is_empty() {
            params[field] = json!(budget);
        } else if compat.flag("supportsThinkingTokenBudget") {
            params["thinking_token_budget"] = json!(budget);
        }
    }
    if let Some(compat) = &model.compat {
        if let Some(v) = compat.0.as_map().get("openRouterRouting") {
            params["provider"] = json!(v);
        }
        if let Some(v) = compat.0.as_map().get("vercelGatewayRouting") {
            let v = json!(v);
            let mut gateway = json!({});
            for key in ["only", "order"] {
                if let Some(v) = v.get(key) {
                    gateway[key] = v.clone();
                }
            }
            if gateway.as_object().is_some_and(|v| !v.is_empty()) {
                params["providerOptions"] = json!({"gateway":gateway});
            }
        }
    }
    if let Some(sampling) = &wire.sampling_params {
        for (k, v) in sampling.as_map() {
            params[k] = json!(v);
        }
    }
    Ok(params)
}
