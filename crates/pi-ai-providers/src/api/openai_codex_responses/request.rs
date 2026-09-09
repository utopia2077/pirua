use super::*;
pub(super) struct Account(Value);
impl Account {
    pub fn header(&self) -> Result<String, Failure> {
        fn stringify(value: &Value, in_array: bool) -> Result<String, Failure> {
            Ok(match value {
                Value::Null => {
                    if in_array {
                        String::new()
                    } else {
                        "null".into()
                    }
                }
                Value::String(s) => s.clone(),
                Value::Bool(v) => v.to_string(),
                Value::Number(n) => stringify_wire_json(&Value::Number(n.clone()))
                    .map_err(|_| Failure::setup("account_number"))?,
                Value::Array(values) => values
                    .iter()
                    .map(|v| stringify(v, true))
                    .collect::<Result<Vec<_>, _>>()?
                    .join(","),
                Value::Object(fields) => {
                    if fields.contains_key("toString") {
                        return Err(Failure::setup("account_coercion"));
                    }
                    "[object Object]".into()
                }
            })
        }
        stringify(&self.0, false)
    }
    pub fn key(&self, fresh: impl FnOnce() -> Result<u64, Failure>) -> Result<String, Failure> {
        Ok(match &self.0 {
            Value::String(s) => format!("s:{s}"),
            Value::Bool(v) => format!("b:{v}"),
            Value::Number(_) => format!("n:{}", self.header()?),
            _ => format!("o:{}", fresh()?),
        })
    }
}
pub(super) fn key(r: &TextStreamRequest) -> Result<&str, Failure> {
    r.auth
        .api_key
        .as_ref()
        .map(SecretString::expose_secret)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| Failure::setup("missing_auth"))
}
pub(super) fn account(r: &TextStreamRequest, limit: usize) -> Result<Account, Failure> {
    let token = key(r)?;
    if token.len() > limit {
        return Err(Failure::Limit);
    }
    let mut parts = token.split('.');
    parts.next();
    let payload = parts.next().ok_or_else(|| Failure::setup("account"))?;
    if parts.next().is_none() || parts.next().is_some() {
        return Err(Failure::setup("account"));
    }
    let mut sextets = Vec::new();
    let mut padding = 0;
    for b in payload.bytes() {
        if matches!(b, b' ' | b'\t' | b'\n' | b'\r' | 12) {
            continue;
        }
        if b == b'=' {
            padding += 1;
            continue;
        }
        if padding > 0 {
            return Err(Failure::setup("account"));
        }
        let value = match b {
            b'A'..=b'Z' => b - b'A',
            b'a'..=b'z' => b - b'a' + 26,
            b'0'..=b'9' => b - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            _ => return Err(Failure::setup("account")),
        };
        sextets.push(value);
    }
    if sextets.len() % 4 == 1 || padding > 2 || (padding > 0 && (sextets.len() + padding) % 4 != 0)
    {
        return Err(Failure::setup("account"));
    }
    let mut bits = 0u32;
    let mut count = 0;
    let mut decoded = String::new();
    for sextet in sextets {
        bits = (bits << 6) | u32::from(sextet);
        count += 6;
        if count >= 8 {
            count -= 8;
            decoded.push(char::from(((bits >> count) & 255) as u8));
        }
    }
    let value: Value = serde_json::from_str(&decoded).map_err(|_| Failure::setup("account"))?;
    let account = &value["https://api.openai.com/auth"]["chatgpt_account_id"];
    let truthy = match account {
        Value::Null => false,
        Value::Bool(v) => *v,
        Value::String(s) => !s.is_empty(),
        Value::Number(n) => n.as_f64().is_some_and(|n| n != 0.0),
        _ => true,
    };
    if !truthy {
        return Err(Failure::setup("account"));
    }
    Ok(Account(account.clone()))
}
pub(super) fn compat(model: &Model, key: &str, default: bool) -> bool {
    match model.compat.as_ref().and_then(|v| v.0.as_map().get(key)) {
        Some(JsonValue::Bool(v)) => *v,
        _ => default,
    }
}
pub(super) fn body(
    r: &TextStreamRequest,
    o: &OpenAICodexResponsesOptions,
    session: Option<&str>,
    grammar: &BTreeMap<String, String>,
) -> Result<WirePayload, Failure> {
    let m = &r.resolved.model;
    let mode = if compat(m, "supportsAdditionalTools", false) {
        Some(ResponsesDeferredToolsMode::AdditionalTools)
    } else if compat(m, "supportsToolSearch", false) {
        Some(ResponsesDeferredToolsMode::ToolSearch)
    } else {
        None
    };
    let placement = split_deferred_tools(&r.context, mode.is_some(), None);
    let deferred = placement
        .deferred
        .into_iter()
        .map(|v| (v.normalized_name.as_str().to_owned(), v.tool))
        .collect();
    let tools = ConvertResponsesToolsOptions {
        strict: Presence::Null,
        supports_strict_mode: Some(compat(m, "supportsStrictMode", true)),
        supports_openai_grammar_tools: Some(compat(m, "supportsOpenAIGrammarTools", false)),
        ..Default::default()
    };
    let input = convert_responses_messages(
        m,
        &r.context,
        &BTreeSet::from([
            "openai".to_owned(),
            "openai-codex".to_owned(),
            "opencode".to_owned(),
        ]),
        &ConvertResponsesMessagesOptions {
            include_system_prompt: Some(false),
            grammar_tool_input_properties: Some(grammar),
            deferred_tools: Some(&deferred),
            deferred_tools_mode: mode,
            tool_options: Some(&tools),
            ..Default::default()
        },
    )?;
    let instructions = r
        .context
        .system_prompt
        .as_deref()
        .filter(|s| !s.is_empty())
        .unwrap_or("You are a helpful assistant.");
    let mut body = json!({"model":m.id,"store":false,"stream":true,"instructions":instructions,"input":input,"text":{"verbosity":o.text_verbosity.unwrap_or(CodexTextVerbosity::Low)},"include":["reasoning.encrypted_content"]});
    if let Some(key) = clamp_openai_prompt_cache_key(session) {
        body["prompt_cache_key"] = json!(key);
    }
    body["tool_choice"] = json!(o.tool_choice.unwrap_or(CodexToolChoice::Auto));
    body["parallel_tool_calls"] = json!(true);
    if let Some(t) = r.options.wire.temperature {
        body["temperature"] = json!(t);
    }
    if !o.service_tier.is_missing() {
        body["service_tier"] = json!(o.service_tier);
    }
    if !placement.immediate.is_empty() {
        body["tools"] = json!(convert_responses_tools(&placement.immediate, &tools)?);
    }
    if let Some(e) = o.reasoning_effort {
        let level = match e {
            CodexReasoningEffort::None => ModelThinkingLevel::Off,
            CodexReasoningEffort::Minimal => ModelThinkingLevel::Minimal,
            CodexReasoningEffort::Low => ModelThinkingLevel::Low,
            CodexReasoningEffort::Medium => ModelThinkingLevel::Medium,
            CodexReasoningEffort::High => ModelThinkingLevel::High,
            CodexReasoningEffort::ExtraHigh => ModelThinkingLevel::ExtraHigh,
            CodexReasoningEffort::Max => ModelThinkingLevel::Max,
        };
        let effort = m
            .thinking_level_map
            .as_ref()
            .and_then(|v| v.as_map().get(&level))
            .and_then(Option::as_ref)
            .map_or_else(|| json!(e), |s| json!(s));
        body["reasoning"] = json!({"effort":effort,"summary":match o.reasoning_summary{Presence::Value(s)=>s,_=>CodexReasoningSummary::Auto}});
    }
    Ok(WirePayload::new(body))
}
pub(super) fn endpoint(raw: &str) -> String {
    let raw = if raw.trim_matches(|c| matches!(c, '\u{9}'..='\u{d}' | ' ' | '\u{a0}' | '\u{1680}' | '\u{2000}'..='\u{200a}' | '\u{2028}' | '\u{2029}' | '\u{202f}' | '\u{205f}' | '\u{3000}' | '\u{feff}')).is_empty() {
        "https://chatgpt.com/backend-api"
    } else {
        raw
    };
    let base = raw.trim_end_matches('/');
    if base.ends_with("/codex/responses") {
        base.to_owned()
    } else if base.ends_with("/codex") {
        format!("{base}/responses")
    } else {
        format!("{base}/codex/responses")
    }
}
pub(super) fn headers(
    r: &TextStreamRequest,
    account: &str,
    affinity: Option<&str>,
    ws: bool,
) -> Result<HttpHeaders, Failure> {
    let mut h = HttpHeaders::new();
    if let Some(headers) = &r.resolved.model.headers {
        for (k, v) in headers.as_map() {
            if v.chars().any(|c| u32::from(c) > 255) {
                return Err(Failure::setup("header"));
            }
            h.append(k, v.trim_matches([' ', '\t', '\r', '\n']))?;
        }
    }
    for (k, v) in r.auth.headers.iter_for_dispatch() {
        if let Some(v) = v {
            set(&mut h, k, v)?;
        } else {
            h.remove(k);
        }
    }
    set(
        &mut h,
        "authorization",
        &format!(
            "Bearer {}",
            r.auth
                .api_key
                .as_ref()
                .ok_or_else(|| Failure::setup("missing_auth"))?
                .expose_secret()
        ),
    )?;
    set(&mut h, "chatgpt-account-id", account)?;
    set(&mut h, "originator", "pi")?;
    set(&mut h, "user-agent", "pi-ai-rust")?;
    if ws {
        h.remove("accept");
        h.remove("content-type");
        set(&mut h, "openai-beta", "responses_websockets=2026-02-06")?;
    } else {
        set(&mut h, "openai-beta", "responses=experimental")?;
        set(&mut h, "accept", "text/event-stream")?;
        set(&mut h, "content-type", "application/json")?;
    }
    if let Some(s) = affinity.filter(|s| !s.is_empty()) {
        set(&mut h, "session-id", s)?;
        set(&mut h, "x-client-request-id", s)?;
    }
    Ok(h)
}

fn set(h: &mut HttpHeaders, key: &str, value: &str) -> Result<(), Failure> {
    if value.chars().any(|c| u32::from(c) > 255) {
        return Err(Failure::setup("header"));
    }
    h.set(key, value.trim_matches([' ', '\t', '\r', '\n']))?;
    Ok(())
}
