use super::{
    BedrockOptions, BedrockThinkingDisplay, BedrockToolChoice, BedrockToolMode,
    payload::{BedrockObject, BedrockValue as V},
    sdk,
};
use crate::api::constrained_sampling::{
    get_wire_json_schema_tool_parameters, resolve_json_schema_strict_sampling,
};
use pi_ai_core::*;
use serde_json::{Value, json};
fn invalid() -> ModelsError {
    ModelsError::new(ModelsErrorCode::Stream)
}
pub(super) fn whitespace(c: char) -> bool {
    matches!(c,'\u{0009}'..='\u{000d}'|'\u{0020}'|'\u{00a0}'|'\u{1680}'|'\u{2000}'..='\u{200a}'|'\u{2028}'|'\u{2029}'|'\u{202f}'|'\u{205f}'|'\u{3000}'|'\u{feff}')
}
pub(super) fn env<'a>(
    request: &'a TextStreamRequest,
    ambient: &'a ProviderEnv,
    key: &str,
) -> Option<&'a str> {
    request
        .auth
        .env
        .as_map()
        .get(key)
        .filter(|v| !v.is_empty())
        .or_else(|| ambient.as_map().get(key).filter(|v| !v.is_empty()))
        .map(String::as_str)
}
fn j(v: Value) -> V {
    V::from_json(v)
}
fn o<const N: usize>(fields: [(&str, V); N]) -> V {
    let mut object = BedrockObject::new();
    for (k, v) in fields {
        object.insert(k.into(), v);
    }
    V::Object(object)
}
fn candidates(model: &Model) -> Vec<String> {
    [model.id.as_str(), &model.name]
        .into_iter()
        .flat_map(|v| {
            let lower = v.to_lowercase();
            let mut normalized = String::new();
            let mut last = false;
            for c in lower.chars() {
                let sep = whitespace(c) || matches!(c, '_' | '.' | ':');
                if !sep {
                    normalized.push(c);
                } else if !last {
                    normalized.push('-');
                }
                last = sep;
            }
            [lower, normalized]
        })
        .collect()
}
pub(super) fn claude(model: &Model) -> bool {
    let id = model.id.as_str().to_lowercase();
    let name = model.name.to_lowercase();
    id.contains("anthropic.claude") || id.contains("anthropic/claude") || name.contains("claude")
}
pub(super) fn adaptive(model: &Model) -> bool {
    candidates(model).iter().any(|s| {
        [
            "opus-4-6",
            "opus-4-7",
            "opus-4-8",
            "opus-5",
            "sonnet-4-6",
            "sonnet-5",
            "fable-5",
        ]
        .iter()
        .any(|m| s.contains(m))
    })
}
fn caching(request: &TextStreamRequest, ambient: &ProviderEnv) -> bool {
    let c = candidates(&request.resolved.model);
    if !c.iter().any(|s| s.contains("claude")) {
        return env(request, ambient, "AWS_BEDROCK_FORCE_CACHE") == Some("1");
    }
    c.iter().any(|s| {
        [
            "fable-5",
            "opus-5",
            "sonnet-5",
            "-4-",
            "claude-3-7-sonnet",
            "claude-3-5-haiku",
        ]
        .iter()
        .any(|m| s.contains(m))
    })
}
fn cache(retention: CacheRetention) -> V {
    if retention == CacheRetention::Long {
        j(json!({"cachePoint":{"type":"default","ttl":"1h"}}))
    } else {
        j(json!({"cachePoint":{"type":"default"}}))
    }
}
fn text(value: &str, required: bool) -> Option<V> {
    if value.trim_matches(whitespace).is_empty() {
        required.then(|| j(json!({"text":"<empty>"})))
    } else {
        Some(j(json!({"text":value})))
    }
}
fn image(image: &ImageContent) -> Result<V, ModelsError> {
    let format = match image.mime_type.as_str() {
        "image/jpeg" | "image/jpg" => "jpeg",
        "image/png" => "png",
        "image/gif" => "gif",
        "image/webp" => "webp",
        _ => return Err(invalid()),
    };
    Ok(o([
        (
            "source",
            o([("bytes", V::Bytes(sdk::base64_decode(&image.data, true)?))]),
        ),
        ("format", j(json!(format))),
    ]))
}
fn inputs(content: &[InputContent], required: bool) -> Result<Vec<V>, ModelsError> {
    let mut out = Vec::new();
    for c in content {
        match c {
            InputContent::Text(t) => {
                if let Some(v) = text(&t.text, false) {
                    out.push(v);
                }
            }
            InputContent::Image(i) => out.push(o([("image", image(i)?)])),
        }
    }
    if out.is_empty() && required {
        out.push(j(json!({"text":"<empty>"})));
    }
    Ok(out)
}
fn sanitize_document(value: Value) -> Value {
    match value {
        Value::Object(v) => Value::Object(
            v.into_iter()
                .filter(|(k, _)| !k.is_empty())
                .map(|(k, v)| (k, sanitize_document(v)))
                .collect(),
        ),
        Value::Array(v) => Value::Array(v.into_iter().map(sanitize_document).collect()),
        other => other,
    }
}
struct Normalize;
impl ToolCallIdNormalizer for Normalize {
    fn normalize_tool_call_id(
        &mut self,
        id: &ToolCallId,
        _: &Model,
        _: &AssistantMessage,
    ) -> ToolCallId {
        id.as_str()
            .encode_utf16()
            .map(|n| {
                if n < 128 && (n as u8).is_ascii_alphanumeric() || n == 45 || n == 95 {
                    n as u8 as char
                } else {
                    '_'
                }
            })
            .take(64)
            .collect::<String>()
            .parse()
            .expect("nonempty normalized identifier")
    }
}
pub(super) fn payload(
    request: &TextStreamRequest,
    options: &BedrockOptions,
    ambient: &ProviderEnv,
) -> Result<V, ModelsError> {
    let model = &request.resolved.model;
    let retention = request.options.wire.cache_retention.unwrap_or(
        if env(request, ambient, "PI_CACHE_RETENTION") == Some("long") {
            CacheRetention::Long
        } else {
            CacheRetention::Short
        },
    );
    let cache_enabled = retention != CacheRetention::None && caching(request, ambient);
    let transformed = transform_messages(
        &request.context.messages,
        model,
        SafeUnsigned::new(request.options.runtime.clock.now_unix_ms()).map_err(|_| invalid())?,
        Some(&mut Normalize),
    )
    .map_err(|_| invalid())?;
    let mut messages = Vec::new();
    let mut i = 0;
    while i < transformed.len() {
        match &transformed[i] {
            Message::User(m) => {
                let content = match &m.content {
                    UserContent::Text(t) => vec![text(t, true).expect("required text")],
                    UserContent::Blocks(b) => inputs(b, true)?,
                };
                messages.push(o([
                    ("role", j(json!("user"))),
                    ("content", V::Array(content)),
                ]));
            }
            Message::Assistant(m) => {
                let mut content = Vec::new();
                for block in &m.content {
                    match block {
                        AssistantContent::Text(t) => {
                            if let Some(t) = text(&t.text, false) {
                                content.push(t);
                            }
                        }
                        AssistantContent::ToolCall(t) => content.push(o([(
                            "toolUse",
                            o([
                                ("toolUseId", j(json!(t.id))),
                                ("name", j(json!(t.name))),
                                (
                                    "input",
                                    j(sanitize_document(
                                        serde_json::to_value(&t.arguments)
                                            .map_err(|_| invalid())?,
                                    )),
                                ),
                            ]),
                        )])),
                        AssistantContent::Thinking(t) => {
                            if t.redacted == Some(true) {
                                if let Some(signature) = &t.thinking_signature
                                    && let Ok(bytes) = sdk::base64_decode(signature, true)
                                    && !bytes.is_empty()
                                {
                                    content.push(o([(
                                        "reasoningContent",
                                        o([("redactedContent", V::Bytes(bytes))]),
                                    )]));
                                }
                                continue;
                            }
                            if t.thinking.trim_matches(whitespace).is_empty() {
                                continue;
                            }
                            let signature = t
                                .thinking_signature
                                .as_ref()
                                .filter(|s| !s.trim_matches(whitespace).is_empty());
                            if claude(model) && signature.is_none() {
                                content.push(j(json!({"text":t.thinking})));
                            } else {
                                let mut reasoning = json!({"text":t.thinking});
                                if claude(model) {
                                    reasoning["signature"] = json!(signature);
                                }
                                content.push(j(
                                    json!({"reasoningContent":{"reasoningText":reasoning}}),
                                ));
                            }
                        }
                    }
                }
                if !content.is_empty() {
                    messages.push(o([
                        ("role", j(json!("assistant"))),
                        ("content", V::Array(content)),
                    ]));
                }
            }
            Message::ToolResult(_) => {
                let mut content = Vec::new();
                while let Some(Message::ToolResult(m)) = transformed.get(i) {
                    content.push(o([(
                        "toolResult",
                        o([
                            ("toolUseId", j(json!(m.tool_call_id))),
                            ("content", V::Array(inputs(&m.content, true)?)),
                            (
                                "status",
                                j(json!(if m.is_error { "error" } else { "success" })),
                            ),
                        ]),
                    )]));
                    i += 1;
                }
                messages.push(o([
                    ("role", j(json!("user"))),
                    ("content", V::Array(content)),
                ]));
                continue;
            }
        }
        i += 1;
    }
    if cache_enabled
        && let Some(last) = messages.last_mut()
        && last.get("role").and_then(V::as_str) == Some("user")
        && let Some(content) = last.get_mut("content").and_then(V::as_array_mut)
    {
        content.push(cache(retention));
    }
    let mut out = BedrockObject::new();
    out.insert("modelId".into(), j(json!(model.id)));
    out.insert("messages".into(), V::Array(messages));
    if let Some(system) = request
        .context
        .system_prompt
        .as_ref()
        .filter(|v| !v.is_empty())
    {
        let mut blocks = vec![j(json!({"text":system}))];
        if cache_enabled {
            blocks.push(cache(retention));
        }
        out.insert("system".into(), V::Array(blocks));
    }
    let mut inference = serde_json::Map::new();
    if let Some(max) = request
        .options
        .wire
        .max_tokens
        .or_else(|| claude(model).then_some(model.max_tokens))
    {
        inference.insert("maxTokens".into(), json!(max));
    }
    if let Some(temp) = request.options.wire.temperature {
        inference.insert("temperature".into(), json!(temp));
    }
    out.insert("inferenceConfig".into(), j(Value::Object(inference)));
    if let Some(source_tools) = request
        .context
        .tools
        .as_ref()
        .filter(|tools| !tools.is_empty())
        && !matches!(
            options.tool_choice,
            Some(BedrockToolChoice::Mode(BedrockToolMode::None))
        )
    {
        let supports = match model
            .compat
            .as_ref()
            .map(|v| v.decode_for_api(&model.api))
            .transpose()
            .map_err(|_| invalid())?
        {
            Some(TypedModelCompat::Bedrock(v)) => v.supports_strict_mode.unwrap_or(false),
            _ => false,
        };
        let mut tools = Vec::new();
        for tool in source_tools {
            let strict =
                resolve_json_schema_strict_sampling(tool, supports).map_err(|_| invalid())?;
            let schema = get_wire_json_schema_tool_parameters(tool, strict)
                .map_err(|_| invalid())?
                .into_value();
            let mut spec = json!({"name":tool.name,"description":tool.description,"inputSchema":{"json":schema}});
            if strict == Some(true) {
                spec["strict"] = json!(true);
            }
            tools.push(json!({"toolSpec":spec}));
        }
        let mut tool = json!({"tools":tools});
        match &options.tool_choice {
            Some(BedrockToolChoice::Mode(BedrockToolMode::Auto)) => {
                tool["toolChoice"] = json!({"auto":{}})
            }
            Some(BedrockToolChoice::Mode(BedrockToolMode::Any)) => {
                tool["toolChoice"] = json!({"any":{}})
            }
            Some(BedrockToolChoice::Tool(t)) => {
                tool["toolChoice"] = json!({"tool":{"name":t.name}})
            }
            _ => {}
        }
        out.insert("toolConfig".into(), j(tool));
    }
    if let Some(fields) = thinking(request, options, ambient) {
        out.insert("additionalModelRequestFields".into(), j(fields));
    }
    if let Some(metadata) = &options.request_metadata {
        out.insert(
            "requestMetadata".into(),
            o_dynamic(
                metadata
                    .iter()
                    .map(|(k, v)| (k.to_owned(), V::String(v.to_owned()))),
            ),
        );
    }
    Ok(V::Object(out))
}
fn o_dynamic(values: impl Iterator<Item = (String, V)>) -> V {
    let mut out = BedrockObject::new();
    for (k, v) in values {
        out.insert(k, v);
    }
    V::Object(out)
}
fn thinking(
    request: &TextStreamRequest,
    options: &BedrockOptions,
    ambient: &ProviderEnv,
) -> Option<Value> {
    let model = &request.resolved.model;
    let level = options.reasoning?;
    if !model.reasoning || !claude(model) {
        return None;
    }
    let region = options
        .region
        .as_deref()
        .filter(|s| !s.is_empty())
        .or_else(|| env(request, ambient, "AWS_REGION"))
        .or_else(|| env(request, ambient, "AWS_DEFAULT_REGION"));
    let gov = region.is_some_and(|r| r.to_lowercase().starts_with("us-gov-"))
        || model.id.as_str().to_lowercase().starts_with("us-gov.")
        || model
            .id
            .as_str()
            .to_lowercase()
            .starts_with("arn:aws-us-gov:");
    let mut fields = if adaptive(model) {
        let native = level == ThinkingLevel::ExtraHigh
            && candidates(model).iter().any(|s| {
                ["opus-4-7", "opus-4-8", "opus-5", "sonnet-5", "fable-5"]
                    .iter()
                    .any(|p| s.contains(p))
            });
        let mapped = model
            .thinking_level_map
            .as_ref()
            .and_then(|m| {
                m.as_map().get(&match level {
                    ThinkingLevel::Minimal => ModelThinkingLevel::Minimal,
                    ThinkingLevel::Low => ModelThinkingLevel::Low,
                    ThinkingLevel::Medium => ModelThinkingLevel::Medium,
                    ThinkingLevel::High => ModelThinkingLevel::High,
                    ThinkingLevel::ExtraHigh => ModelThinkingLevel::ExtraHigh,
                    ThinkingLevel::Max => ModelThinkingLevel::Max,
                })
            })
            .and_then(|s| s.as_deref());
        let effort = if native {
            "xhigh"
        } else {
            mapped.unwrap_or(match level {
                ThinkingLevel::Minimal | ThinkingLevel::Low => "low",
                ThinkingLevel::Medium => "medium",
                _ => "high",
            })
        };
        json!({"thinking":{"type":"adaptive"},"output_config":{"effort":effort}})
    } else {
        let budget = crate::api::simple_options::thinking_budget_for_level(
            level,
            options.thinking_budgets.as_ref(),
        );
        json!({"thinking":{"type":"enabled","budget_tokens":budget}})
    };
    if !gov {
        fields["thinking"]["display"] = json!(
            options
                .thinking_display
                .unwrap_or(BedrockThinkingDisplay::Summarized)
        );
    }
    if !adaptive(model) && options.interleaved_thinking.unwrap_or(true) {
        fields["anthropic_beta"] = json!(["interleaved-thinking-2025-05-14"]);
    }
    Some(fields)
}
