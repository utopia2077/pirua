use super::{AnthropicOptions, Failure, compat, ecma_whitespace, payload};
use crate::api::{
    github_copilot_headers::{build_copilot_dynamic_headers, has_copilot_vision_input},
    runtime::AdapterServices,
    wire_json::stringify_wire_json,
};
use pi_ai_core::*;
use pi_ai_transport::{HttpHeaders, HttpRequest, TransportError};
use serde_json::Value;
use std::collections::BTreeSet;
use std::time::Duration;
use url::Url;

pub(super) fn is_oauth(request: &TextStreamRequest) -> bool {
    request.resolved.model.provider.as_str() != "github-copilot"
        && request
            .auth
            .api_key
            .as_ref()
            .is_some_and(|key| key.expose_secret().contains("sk-ant-oat"))
}
pub(super) fn assert_auth(request: &TextStreamRequest) -> Result<(), Failure> {
    if request
        .auth
        .api_key
        .as_ref()
        .is_some_and(|key| !key.expose_secret().is_empty())
    {
        return Ok(());
    }
    let present = request.auth.headers.as_map().iter().any(|(name, value)| {
        ["authorization", "x-api-key", "cf-aig-authorization"]
            .iter()
            .any(|expected| name.eq_ignore_ascii_case(expected))
            && value
                .as_ref()
                .is_some_and(|value| !value.trim_matches(ecma_whitespace).is_empty())
    });
    if present {
        Ok(())
    } else {
        Err(Failure::Authentication)
    }
}

// Exact object spread replacement preserves each first insertion slot. SDK
// buildHeaders later processes case variants independently in that order.
fn assign(target: &mut Vec<(String, Option<String>)>, name: &str, value: Option<String>) {
    if let Some((_, old)) = target.iter_mut().find(|(key, _)| key == name) {
        *old = value;
    } else {
        target.push((name.into(), value));
    }
}
fn set(
    headers: &mut HttpHeaders,
    nulls: &mut BTreeSet<String>,
    name: &str,
    value: Option<&str>,
) -> Result<(), Failure> {
    let mut probe = HttpHeaders::new();
    probe.set(name, "").map_err(|_| Failure::Protocol)?;
    if let Some(value) = value {
        if value.chars().any(|c| u32::from(c) > 255) {
            return Err(Failure::Protocol);
        }
        headers
            .set(name, value.trim_matches([' ', '\t', '\r', '\n']))
            .map_err(|_| Failure::Protocol)?;
        nulls.remove(&name.to_ascii_lowercase());
    } else {
        headers.remove(name);
        nulls.insert(name.to_ascii_lowercase());
    }
    Ok(())
}

pub(super) fn build(
    request: &TextStreamRequest,
    options: &AnthropicOptions,
    services: &AdapterServices,
    payload: &Value,
) -> Result<HttpRequest, Failure> {
    let model = &request.resolved.model;
    let c = compat(model)?;
    let oauth = is_oauth(request);
    let mut betas = vec![];
    if request
        .context
        .tools
        .as_ref()
        .is_some_and(|tools| !tools.is_empty())
        && !c.supports_eager_tool_input_streaming.unwrap_or(true)
    {
        betas.push("fine-grained-tool-streaming-2025-05-14");
    }
    if options.interleaved_thinking.unwrap_or(true) && c.force_adaptive_thinking != Some(true) {
        betas.push("interleaved-thinking-2025-05-14");
    }
    if c.allowed_fallback_models
        .as_ref()
        .is_some_and(|models| !models.is_empty())
    {
        betas.push("server-side-fallback-2026-07-01");
    }
    let mut defaults = vec![
        ("User-Agent".into(), Some("pi-ai-rust".into())),
        ("accept".into(), Some("application/json".into())),
        (
            "anthropic-dangerous-direct-browser-access".into(),
            Some("true".into()),
        ),
    ];
    if oauth {
        betas.splice(0..0, ["claude-code-20250219", "oauth-2025-04-20"]);
    }
    if !betas.is_empty() {
        assign(&mut defaults, "anthropic-beta", Some(betas.join(",")));
    }
    if oauth {
        assign(
            &mut defaults,
            "user-agent",
            Some("claude-cli/2.1.75".into()),
        );
        assign(&mut defaults, "x-app", Some("cli".into()));
    }
    if !oauth
        && model.provider.as_str() != "github-copilot"
        && c.send_session_affinity_headers == Some(true)
        && payload::retention(request, services) != CacheRetention::None
        && let Some(session) = request
            .options
            .wire
            .session_id
            .as_ref()
            .filter(|value| !value.is_empty())
    {
        assign(&mut defaults, "x-session-affinity", Some(session.clone()));
    }
    if let Some(headers) = &model.headers {
        for (name, value) in headers.as_map() {
            assign(&mut defaults, name, Some(value.clone()));
        }
    }
    if model.provider.as_str() == "github-copilot" {
        for (name, value) in build_copilot_dynamic_headers(
            &request.context.messages,
            has_copilot_vision_input(&request.context.messages),
        ) {
            assign(&mut defaults, &name, Some(value));
        }
    }
    for (name, value) in request.auth.headers.as_map() {
        assign(&mut defaults, name, value.clone());
    }
    let mut headers = HttpHeaders::new();
    let mut nulls = BTreeSet::new();
    // Portable identity normalization is explicit; protocol version and retry
    // metadata retain the SDK's concrete values. No process OS/runtime discovery.
    for (name, value) in [
        ("accept", "application/json"),
        ("user-agent", "pi-ai-rust"),
        ("x-stainless-retry-count", "0"),
        ("anthropic-dangerous-direct-browser-access", "true"),
        ("anthropic-version", "2023-06-01"),
    ] {
        set(&mut headers, &mut nulls, name, Some(value))?;
    }
    let timeout = request
        .options
        .wire
        .timeout_ms
        .map_or(600_000, SafeUnsigned::get);
    if timeout != 0 {
        set(
            &mut headers,
            &mut nulls,
            "x-stainless-timeout",
            Some(&(timeout / 1000).to_string()),
        )?;
    }
    if let Some(key) = &request.auth.api_key {
        if oauth || model.provider.as_str() == "github-copilot" {
            set(
                &mut headers,
                &mut nulls,
                "Authorization",
                Some(&format!("Bearer {}", key.expose_secret())),
            )?;
        } else {
            set(
                &mut headers,
                &mut nulls,
                "X-Api-Key",
                Some(key.expose_secret()),
            )?;
        }
    }
    for (name, value) in defaults {
        set(&mut headers, &mut nulls, &name, value.as_deref())?;
    }
    // The SDK's JSON encoder body headers are later than client default headers.
    set(
        &mut headers,
        &mut nulls,
        "content-type",
        Some("application/json"),
    )?;
    if !headers
        .get_for_wire("x-api-key")
        .is_some_and(|v| !v.is_empty())
        && !headers
            .get_for_wire("authorization")
            .is_some_and(|v| !v.is_empty())
        && !nulls.contains("x-api-key")
        && !nulls.contains("authorization")
    {
        return Err(Failure::Authentication);
    }
    let base = request.resolved.endpoint.expose_for_transport();
    let combined = format!(
        "{base}{}",
        if base.ends_with('/') {
            "v1/messages"
        } else {
            "/v1/messages"
        }
    );
    let mut url = Url::parse(&combined).map_err(|_| Failure::Protocol)?;
    let mut query: Vec<(String, String)> = vec![];
    for (name, value) in url.query_pairs() {
        if let Some((_, old)) = query.iter_mut().find(|(key, _)| key == name.as_ref()) {
            *old = value.into_owned();
        } else {
            query.push((name.into_owned(), value.into_owned()));
        }
    }
    if !query.is_empty() {
        // Object.fromEntries followed by Object.entries orders array-index keys
        // numerically before the other first-insertion slots.
        query.sort_by_key(|(name, _)| {
            name.parse::<u32>()
                .ok()
                .filter(|index| *index != u32::MAX && index.to_string() == name.as_str())
                .map_or((1, 0), |index| (0, index))
        });
        let query = query
            .into_iter()
            .map(|(key, value)| format!("{}={}", encode_component(&key), encode_component(&value)))
            .collect::<Vec<_>>()
            .join("&");
        url.set_query(Some(&query));
    }
    Ok(HttpRequest {
        endpoint: RequestEndpoint::new_resolved(url.as_str()).map_err(|_| Failure::Protocol)?,
        method: "POST".into(),
        headers,
        body: stringify_wire_json(payload)
            .map_err(|_| Failure::Protocol)?
            .into_bytes(),
        proxy: None,
    })
}
fn encode_component(value: &str) -> String {
    let mut result = String::new();
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric()
            || matches!(
                byte,
                b'-' | b'_' | b'.' | b'!' | b'~' | b'*' | b'\'' | b'(' | b')'
            )
        {
            result.push(char::from(byte));
        } else {
            result.push_str(&format!("%{byte:02X}"));
        }
    }
    result
}
pub(super) fn timeout(request: &TextStreamRequest) -> Duration {
    let ms = request
        .options
        .wire
        .timeout_ms
        .map_or(600_000, SafeUnsigned::get);
    // SDK validates a nonnegative integer, then Node setTimeout clamps 0 and
    // delays outside signed 32-bit range to one millisecond.
    Duration::from_millis(if ms == 0 || ms > i32::MAX as u64 {
        1
    } else {
        ms
    })
}
pub(super) fn force_stream(payload: Value) -> Result<Value, Failure> {
    let mut result = match payload {
        Value::Object(object) => object,
        Value::Array(values) => values
            .into_iter()
            .enumerate()
            .map(|(i, value)| (i.to_string(), value))
            .collect(),
        Value::String(text) => {
            let mut object = serde_json::Map::new();
            for (index, unit) in text.encode_utf16().enumerate() {
                let value = char::from_u32(u32::from(unit)).ok_or(Failure::Protocol)?;
                object.insert(index.to_string(), Value::String(value.to_string()));
            }
            object
        }
        _ => serde_json::Map::new(),
    };
    result.insert("stream".into(), Value::Bool(true));
    Ok(Value::Object(result))
}

impl From<TransportError> for Failure {
    fn from(_: TransportError) -> Self {
        Failure::Protocol
    }
}
