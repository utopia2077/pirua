use super::{ImageAdapterServices, openrouter::Failure};
use crate::api::{
    openai_sdk_strings::{custom_headers, read_env},
    wire_json::stringify_wire_json,
};
use pi_ai_core::{ImageGenerationRequest, InputContent, ModelInput, RequestEndpoint};
use pi_ai_transport::{FetchBody, FetchHeaders, FetchRequest, HttpHeaders};
use serde_json::{Value, json};
use url::Url;

pub(super) fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(v) => *v,
        Value::Number(v) => v.as_f64() != Some(0.0),
        Value::String(v) => !v.is_empty(),
        _ => true,
    }
}
pub(super) fn payload(request: &ImageGenerationRequest) -> Value {
    let content: Vec<Value> = request.context.input.iter().map(|item| match item {
        InputContent::Text(item) => json!({"type":"text","text":item.text}),
        InputContent::Image(item) => json!({"type":"image_url","image_url":{"url":format!("data:{};base64,{}",item.mime_type,item.data)}}),
    }).collect();
    json!({"model":request.resolved.model.id.as_str(),"messages":[{"role":"user","content":content}],"stream":false,"modalities":if request.resolved.model.output.contains(&ModelInput::Text) {vec!["image","text"]} else {vec!["image"]}})
}
fn set(headers: &mut HttpHeaders, name: &str, value: &str) -> Result<(), Failure> {
    if value.chars().any(|c| u32::from(c) > 255) {
        return Err(Failure::Headers);
    }
    headers
        .set(name, value.trim_matches([' ', '\t', '\r', '\n']))
        .map_err(|_| Failure::Headers)
}
/// Preserve exact spread replacement/position before null removal and SDK casing.
pub(super) fn default_headers(
    request: &ImageGenerationRequest,
    services: &ImageAdapterServices,
) -> Result<Vec<(String, String)>, Failure> {
    let mut entries: Vec<(String, Option<String>)> = request
        .resolved
        .model
        .headers
        .as_ref()
        .map(|h| {
            h.as_map()
                .iter()
                .map(|(k, v)| (k.clone(), Some(v.clone())))
                .collect()
        })
        .unwrap_or_default();
    for (name, value) in request.auth.headers.iter_for_dispatch() {
        if let Some((_, old)) = entries.iter_mut().find(|(key, _)| key == name) {
            *old = value.clone();
        } else {
            entries.push((name.to_owned(), value.clone()));
        }
    }
    entries.sort_by_key(|(name, _)| {
        name.parse::<u32>()
            .ok()
            .filter(|v| *v != u32::MAX && v.to_string() == *name)
            .map_or((1, 0), |v| (0, v))
    });
    let own: Vec<_> = entries
        .into_iter()
        .filter(|(key, _)| key != "__proto__")
        .filter_map(|(k, v)| v.map(|v| (k, v)))
        .collect();
    if let Some(raw) = read_env(&services.env, "OPENAI_CUSTOM_HEADERS") {
        // SDK construction's buildHeaders is earlier than onPayload.
        let mut headers = HttpHeaders::new();
        for (name, value) in custom_headers(raw) {
            set(&mut headers, name, value)?;
        }
        for (name, value) in &own {
            set(&mut headers, name, value)?;
        }
        Ok(headers
            .iter_for_wire()
            .map(|(k, v)| (k.to_owned(), v.to_owned()))
            .collect())
    } else {
        Ok(own)
    }
}
fn endpoint(base: &str) -> Result<RequestEndpoint, Failure> {
    let mut url = Url::parse(&format!(
        "{base}{}",
        if base.ends_with('/') {
            "chat/completions"
        } else {
            "/chat/completions"
        }
    ))
    .map_err(|_| Failure::Endpoint)?;
    let mut pairs: Vec<(String, String)> = Vec::new();
    for (k, v) in url.query_pairs() {
        if let Some((_, old)) = pairs.iter_mut().find(|(name, _)| name == &k) {
            *old = v.into_owned();
        } else {
            pairs.push((k.into_owned(), v.into_owned()));
        }
    }
    if !pairs.is_empty() {
        pairs.sort_by_key(|(name, _)| {
            name.parse::<u32>()
                .ok()
                .filter(|v| *v != u32::MAX && v.to_string() == *name)
                .map_or((1, 0), |v| (0, v))
        });
        let encode = |s: &str| {
            s.bytes()
                .map(|b| {
                    if b.is_ascii_alphanumeric() || b"-._~".contains(&b) {
                        char::from(b).to_string()
                    } else {
                        format!("%{b:02X}")
                    }
                })
                .collect::<String>()
        };
        url.set_query(Some(
            &pairs
                .iter()
                .map(|(k, v)| format!("{}={}", encode(k), encode(v)))
                .collect::<Vec<_>>()
                .join("&"),
        ));
    }
    RequestEndpoint::new_resolved(url.as_str()).map_err(|_| Failure::Endpoint)
}
pub(super) fn build(
    request: &ImageGenerationRequest,
    services: &ImageAdapterServices,
    key: &str,
    defaults: Vec<(String, String)>,
    value: Value,
) -> Result<(FetchRequest, bool), Failure> {
    // ChatCompletion.create reads body.stream before building its request.
    if value.is_null() {
        return Err(Failure::Payload);
    }
    let streaming = value.get("stream").is_some_and(truthy);
    let endpoint = endpoint(request.resolved.endpoint.expose_for_transport())?;
    let mut headers = HttpHeaders::new();
    for (name, value) in [
        ("accept", "application/json"),
        ("user-agent", "pi-ai-rust"),
        ("x-stainless-retry-count", "0"),
    ] {
        set(&mut headers, name, value)?;
    }
    if let Some(ms) = request.options.wire.timeout_ms.filter(|v| v.get() != 0) {
        set(
            &mut headers,
            "x-stainless-timeout",
            &(ms.get() / 1000).to_string(),
        )?;
    }
    for (env, name) in [
        ("OPENAI_ORG_ID", "openai-organization"),
        ("OPENAI_PROJECT_ID", "openai-project"),
    ] {
        if let Some(value) = read_env(&services.env, env) {
            set(&mut headers, name, value)?;
        }
    }
    set(&mut headers, "authorization", &format!("Bearer {key}"))?;
    for (name, value) in defaults {
        set(&mut headers, &name, &value)?;
    }
    let body = if truthy(&value) {
        set(&mut headers, "content-type", "application/json")?;
        FetchBody::Text(stringify_wire_json(&value).map_err(|_| Failure::Payload)?)
    } else {
        FetchBody::Absent
    };
    Ok((
        FetchRequest {
            endpoint,
            method: "POST".to_owned(),
            headers: FetchHeaders::Constructed(headers),
            body,
            proxy: None,
        },
        streaming,
    ))
}
