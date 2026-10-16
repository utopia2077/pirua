//! Private @google/genai2.21.0 GenerateContent projection, not its NextGen client.
//! Shared by Google SDK consumers, without selecting a provider or discovering ADC.
use super::wire_json::stringify_wire_json;
use pi_ai_core::{ProviderEnv, RequestEndpoint, TextStreamRequest};
use pi_ai_transport::{HttpHeaders, HttpRequest, SseLimits};
use serde_json::{Value, json};
use std::{fmt, time::Duration};

pub(crate) enum Error {
    Local,
    Limit,
    Message {
        status: Option<u16>,
        api: bool,
        message: String,
    },
}
impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("GoogleSdkError([REDACTED])")
    }
}
pub(crate) fn message(message: impl Into<String>) -> Error {
    Error::Message {
        status: None,
        api: false,
        message: message.into(),
    }
}
pub(crate) fn truthy(v: &Value) -> bool {
    match v {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().is_some_and(|n| n != 0.0),
        Value::String(s) => !s.is_empty(),
        _ => true,
    }
}
pub(crate) fn whitespace(c: char) -> bool {
    matches!(c,'\u{9}'..='\u{d}'|' '| '\u{a0}'|'\u{1680}'|'\u{2000}'..='\u{200a}'|'\u{2028}'|'\u{2029}'|'\u{202f}'|'\u{205f}'|'\u{3000}'|'\u{feff}')
}
fn present<'a>(v: &'a Value, key: &str) -> Option<&'a Value> {
    v.get(key).filter(|v| !v.is_null())
}
fn object() -> Value {
    json!({})
}
fn copy(v: &Value, keys: &[&str]) -> Value {
    let mut out = object();
    for key in keys {
        if let Some(value) = present(v, key) {
            out[*key] = value.clone();
        }
    }
    out
}
fn reject(v: &Value, keys: &[&str]) -> Result<(), Error> {
    if keys.iter().any(|key| v.get(*key).is_some()) {
        Err(Error::Local)
    } else {
        Ok(())
    }
}
fn array_map(
    v: &Value,
    mut convert: impl FnMut(&Value) -> Result<Value, Error>,
) -> Result<Value, Error> {
    if let Some(values) = v.as_array() {
        Ok(Value::Array(
            values.iter().map(&mut convert).collect::<Result<_, _>>()?,
        ))
    } else {
        Ok(v.clone())
    }
}
fn sdk_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null => "null".into(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => ryu_js::Buffer::new()
            .format(n.as_f64().unwrap_or(f64::NAN))
            .to_owned(),
        Value::Array(a) => a
            .iter()
            .map(|v| {
                if v.is_null() {
                    String::new()
                } else {
                    sdk_string(v)
                }
            })
            .collect::<Vec<_>>()
            .join(","),
        Value::Object(_) => "[object Object]".into(),
    }
}
pub(crate) fn number(v: &Value) -> f64 {
    match v {
        Value::Number(n) => n.as_f64().unwrap_or(f64::NAN),
        Value::Null => 0.0,
        Value::Bool(b) => f64::from(u8::from(*b)),
        _ => {
            let text = sdk_string(v);
            let text = text.trim_matches(whitespace);
            if text.is_empty() {
                return 0.0;
            }
            for (prefix, radix) in [
                ("0x", 16),
                ("0X", 16),
                ("0b", 2),
                ("0B", 2),
                ("0o", 8),
                ("0O", 8),
            ] {
                if let Some(digits) = text.strip_prefix(prefix) {
                    if digits.is_empty() {
                        return f64::NAN;
                    }
                    let mut result = 0.0;
                    for c in digits.chars() {
                        let Some(d) = c.to_digit(radix) else {
                            return f64::NAN;
                        };
                        result = result * f64::from(radix) + f64::from(d);
                    }
                    return result;
                }
            }
            text.parse().unwrap_or(f64::NAN)
        }
    }
}
pub(crate) fn cloud_mode(env: &ProviderEnv) -> bool {
    env.as_map()
        .get("GOOGLE_GENAI_USE_ENTERPRISE")
        .or_else(|| env.as_map().get("GOOGLE_GENAI_USE_VERTEXAI"))
        .is_some_and(|v| v.trim_matches(whitespace).eq_ignore_ascii_case("true"))
}

fn part(v: &Value, cloud: bool) -> Result<Value, Error> {
    let mut out = object();
    for key in [
        "mediaResolution",
        "codeExecutionResult",
        "executableCode",
        "fileData",
        "functionCall",
        "functionResponse",
        "inlineData",
        "text",
        "thought",
        "thoughtSignature",
        "videoMetadata",
        "toolCall",
        "toolResponse",
        "partMetadata",
    ] {
        if cloud && matches!(key, "toolCall" | "toolResponse" | "partMetadata") {
            reject(v, &[key])?;
            continue;
        }
        if let Some(value) = present(v, key) {
            out[key] = if cloud {
                value.clone()
            } else {
                match key {
                    "fileData" => {
                        reject(value, &["displayName"])?;
                        copy(value, &["fileUri", "mimeType"])
                    }
                    "inlineData" => {
                        reject(value, &["displayName"])?;
                        copy(value, &["data", "mimeType"])
                    }
                    "functionCall" => {
                        reject(value, &["partialArgs", "willContinue"])?;
                        copy(value, &["id", "args", "name"])
                    }
                    _ => value.clone(),
                }
            };
        }
    }
    Ok(out)
}
fn content(v: &Value, cloud: bool) -> Result<Value, Error> {
    let mut out = object();
    if let Some(parts) = present(v, "parts") {
        out["parts"] = array_map(parts, |p| part(p, cloud))?;
    }
    if let Some(role) = present(v, "role") {
        out["role"] = role.clone();
    }
    Ok(out)
}
fn parts(v: &Value) -> Result<Value, Error> {
    let values = if let Some(values) = v.as_array() {
        if values.is_empty() {
            return Err(Error::Local);
        }
        values.clone()
    } else {
        vec![v.clone()]
    };
    Ok(Value::Array(
        values
            .into_iter()
            .map(|v| match v {
                Value::String(s) => Ok(json!({"text":s})),
                Value::Object(_) | Value::Array(_) => Ok(v),
                _ => Err(Error::Local),
            })
            .collect::<Result<_, _>>()?,
    ))
}
fn is_content(v: &Value) -> bool {
    v.get("parts").is_some_and(Value::is_array)
}
fn t_content(v: &Value) -> Result<Value, Error> {
    if is_content(v) {
        Ok(v.clone())
    } else {
        Ok(json!({"role":"user","parts":parts(v)?}))
    }
}
fn contents(v: &Value, cloud: bool) -> Result<Value, Error> {
    let values = if let Some(values) = v.as_array() {
        if values.is_empty() {
            return Err(Error::Local);
        }
        if values
            .iter()
            .any(|v| is_content(v) != is_content(&values[0]))
        {
            return Err(Error::Local);
        }
        if is_content(&values[0]) {
            values.clone()
        } else {
            if values
                .iter()
                .any(|v| v.get("functionCall").is_some() || v.get("functionResponse").is_some())
            {
                return Err(Error::Local);
            }
            vec![t_content(v)?]
        }
    } else {
        if v.get("functionCall").is_some() || v.get("functionResponse").is_some() {
            return Err(Error::Local);
        }
        vec![t_content(v)?]
    };
    Ok(Value::Array(
        values
            .iter()
            .map(|v| content(v, cloud))
            .collect::<Result<_, _>>()?,
    ))
}
fn schema(v: &Value, depth: usize) -> Result<Value, Error> {
    if depth > 128 {
        return Err(Error::Limit);
    }
    let mut value = v;
    let mut out = object();
    if v.get("type").is_some_and(truthy) && v.get("anyOf").is_some_and(truthy) {
        return Err(Error::Local);
    }
    if let Some(a) = v
        .get("anyOf")
        .and_then(Value::as_array)
        .filter(|a| a.len() == 2)
    {
        if a[0]["type"] == "null" {
            out["nullable"] = json!(true);
            value = &a[1];
        } else if a[1]["type"] == "null" {
            out["nullable"] = json!(true);
            value = &a[0];
        }
    }
    let type_name = |v: &Value| -> Result<Value, Error> {
        let s = v.as_str().ok_or(Error::Local)?.to_uppercase();
        Ok(json!(if [
            "TYPE_UNSPECIFIED",
            "STRING",
            "NUMBER",
            "INTEGER",
            "BOOLEAN",
            "ARRAY",
            "OBJECT",
            "NULL"
        ]
        .contains(&s.as_str())
        {
            s
        } else {
            "TYPE_UNSPECIFIED".into()
        }))
    };
    if let Some(types) = value.get("type").and_then(Value::as_array) {
        if types.iter().any(|v| v == "null") {
            out["nullable"] = json!(true);
        }
        let types: Vec<_> = types.iter().filter(|v| *v != "null").collect();
        if types.len() == 1 {
            out["type"] = type_name(types[0])?;
        } else {
            out["anyOf"] = Value::Array(
                types
                    .into_iter()
                    .map(|v| Ok(json!({"type":type_name(v)?})))
                    .collect::<Result<_, Error>>()?,
            );
        }
    }
    for (key, val) in value.as_object().ok_or(Error::Local)? {
        if val.is_null() {
            continue;
        }
        match key.as_str() {
            "type" => {
                if val == "null" {
                    return Err(Error::Local);
                }
                if !val.is_array() {
                    out[key] = type_name(val)?;
                }
            }
            "items" => out[key] = schema(val, depth + 1)?,
            "anyOf" => {
                let mut a = Vec::new();
                for item in val.as_array().ok_or(Error::Local)? {
                    if item["type"] == "null" {
                        out["nullable"] = json!(true);
                    } else {
                        a.push(schema(item, depth + 1)?);
                    }
                }
                out[key] = Value::Array(a);
            }
            "properties" => {
                let mut properties = object();
                for (name, p) in val.as_object().ok_or(Error::Local)? {
                    if name != "__proto__" {
                        properties[name] = schema(p, depth + 1)?;
                    }
                }
                out[key] = properties;
            }
            "additionalProperties" | "__proto__" => {}
            _ => out[key] = val.clone(),
        }
    }
    Ok(out)
}
fn tool(v: &Value, cloud: bool) -> Result<Value, Error> {
    let mut v = v.clone();
    if v.get("functionDeclarations").is_some_and(truthy) {
        for declaration in v["functionDeclarations"]
            .as_array_mut()
            .ok_or(Error::Local)?
        {
            for (field, json_field) in [
                ("parameters", "parametersJsonSchema"),
                ("response", "responseJsonSchema"),
            ] {
                if declaration.get(field).is_some_and(truthy) {
                    if declaration[field].get("$schema").is_some() {
                        if !declaration.get(json_field).is_some_and(truthy) {
                            declaration[json_field] = declaration[field].clone();
                            declaration
                                .as_object_mut()
                                .ok_or(Error::Local)?
                                .remove(field);
                        }
                    } else {
                        declaration[field] = schema(&declaration[field], 0)?;
                    }
                }
            }
        }
    }
    reject(
        &v,
        if cloud {
            &["fileSearch", "mcpServers"]
        } else {
            &["retrieval", "enterpriseWebSearch", "parallelAiSearch"]
        },
    )?;
    let mut out = object();
    for key in [
        "retrieval",
        "computerUse",
        "fileSearch",
        "googleSearch",
        "googleMaps",
        "codeExecution",
        "enterpriseWebSearch",
        "functionDeclarations",
        "googleSearchRetrieval",
        "parallelAiSearch",
        "urlContext",
        "mcpServers",
    ] {
        if let Some(value) = present(&v, key) {
            out[key] = match key {
                "functionDeclarations" if cloud => array_map(value, |v| {
                    reject(v, &["behavior"])?;
                    Ok(copy(
                        v,
                        &[
                            "description",
                            "name",
                            "parameters",
                            "parametersJsonSchema",
                            "response",
                            "responseJsonSchema",
                        ],
                    ))
                })?,
                "googleSearch" if !cloud => {
                    reject(value, &["blockingConfidence", "excludeDomains"])?;
                    copy(value, &["searchTypes", "timeRangeFilter"])
                }
                "googleMaps" if !cloud => {
                    let mut map = copy(value, &["enableWidget"]);
                    if let Some(auth) = present(value, "authConfig") {
                        reject(
                            auth,
                            &[
                                "apiKeyConfig",
                                "authType",
                                "googleServiceAccountConfig",
                                "httpBasicAuthConfig",
                                "oauthConfig",
                                "oidcConfig",
                            ],
                        )?;
                        map["authConfig"] = copy(auth, &["apiKey"]);
                    }
                    map
                }
                _ => value.clone(),
            };
        }
    }
    Ok(out)
}
fn deep_merge(target: &mut Value, source: &Value, depth: usize) -> Result<(), Error> {
    if depth > 128 {
        return Err(Error::Limit);
    }
    if let Some(src) = source.as_object() {
        for (key, value) in src {
            if key == "__proto__" {
                continue;
            }
            if target.get(key).is_some_and(Value::is_object) && value.is_object() {
                deep_merge(&mut target[key], value, depth + 1)?;
            } else {
                target[key] = value.clone();
            }
        }
    }
    Ok(())
}

pub(crate) struct Prepared {
    pub request: HttpRequest,
    pub timeout: Option<Duration>,
    pub cloud: bool,
}

pub(crate) fn prepare(
    request: &TextStreamRequest,
    params: &Value,
    ambient: &ProviderEnv,
) -> Result<Prepared, Error> {
    let cloud = cloud_mode(ambient);

    let key = request
        .auth
        .api_key
        .as_ref()
        .ok_or(Error::Local)?
        .expose_secret();
    let key = Some(key);

    let model = params
        .get("model")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or(Error::Local)?;
    if model.contains("..") || model.contains('?') || model.contains('&') {
        return Err(Error::Local);
    }
    let model_path = if cloud {
        if model.starts_with("publishers/")
            || model.starts_with("projects/")
            || model.starts_with("models/")
        {
            model.into()
        } else if let Some((publisher, rest)) = model.split_once('/') {
            format!(
                "publishers/{publisher}/models/{}",
                rest.split('/').next().unwrap_or("")
            )
        } else {
            format!("publishers/google/models/{model}")
        }
    } else if model.starts_with("models/") || model.starts_with("tunedModels/") {
        model.into()
    } else {
        format!("models/{model}")
    };
    let mut config = params.get("config").cloned().unwrap_or(Value::Null);
    if config.get("responseSchema").is_some_and(truthy)
        && !config.get("responseJsonSchema").is_some_and(truthy)
        && config["responseSchema"].get("$schema").is_some()
    {
        config["responseJsonSchema"] = config["responseSchema"].clone();
        config
            .as_object_mut()
            .ok_or(Error::Local)?
            .remove("responseSchema");
    }
    let mut body = object();
    if let Some(c) = present(params, "contents") {
        body["contents"] = contents(c, cloud)?;
    }
    if !config.is_null() {
        reject(
            &config,
            if cloud {
                &["enableEnhancedCivicAnswers"]
            } else {
                &[
                    "routingConfig",
                    "modelSelectionConfig",
                    "labels",
                    "audioTimestamp",
                    "modelArmorConfig",
                ]
            },
        )?;
        let mut generation = copy(
            &config,
            &[
                "temperature",
                "topP",
                "topK",
                "candidateCount",
                "maxOutputTokens",
                "stopSequences",
                "responseLogprobs",
                "logprobs",
                "presencePenalty",
                "frequencyPenalty",
                "seed",
                "responseMimeType",
            ],
        );
        if let Some(s) = present(&config, "systemInstruction") {
            body["systemInstruction"] = content(&t_content(s)?, cloud)?;
        }
        if let Some(s) = present(&config, "responseSchema") {
            generation["responseSchema"] = schema(s, 0)?;
        }
        for field in ["responseJsonSchema", "routingConfig"] {
            if let Some(v) = present(&config, field) {
                generation[field] = v.clone();
            }
        }
        if let Some(v) = present(&config, "modelSelectionConfig") {
            generation["modelConfig"] = v.clone();
        }
        if let Some(v) = present(&config, "safetySettings") {
            body["safetySettings"] = array_map(v, |v| {
                if cloud {
                    Ok(v.clone())
                } else {
                    reject(v, &["method"])?;
                    Ok(copy(v, &["category", "threshold"]))
                }
            })?;
        }
        if let Some(v) = present(&config, "tools") {
            let values = v.as_array().ok_or(Error::Local)?;
            if values.iter().any(|v| !v.is_object()) {
                return Err(Error::Local);
            }
            body["tools"] = Value::Array(
                values
                    .iter()
                    .map(|v| tool(v, cloud))
                    .collect::<Result<_, _>>()?,
            );
        }
        if let Some(v) = present(&config, "toolConfig") {
            if cloud {
                reject(v, &["includeServerSideToolInvocations"])?;
            }
            let mut t = copy(v, &["retrievalConfig"]);
            if let Some(f) = present(v, "functionCallingConfig") {
                t["functionCallingConfig"] = if cloud {
                    f.clone()
                } else {
                    reject(f, &["streamFunctionCallArguments"])?;
                    copy(f, &["allowedFunctionNames", "mode"])
                };
            }
            if !cloud && let Some(x) = present(v, "includeServerSideToolInvocations") {
                t["includeServerSideToolInvocations"] = x.clone();
            }
            body["toolConfig"] = t;
        }
        for field in ["labels", "modelArmorConfig", "serviceTier"] {
            if let Some(v) = present(&config, field) {
                body[field] = v.clone();
            }
        }
        if let Some(v) = present(&config, "cachedContent") {
            let s = v.as_str().ok_or(Error::Local)?;

            body["cachedContent"] = json!(if (cloud && s.starts_with("projects/"))
                || s.starts_with("cachedContents/")
            {
                s.to_owned()
            } else {
                format!("cachedContents/{s}")
            });
        }
        for field in [
            "responseModalities",
            "mediaResolution",
            "audioTimestamp",
            "thinkingConfig",
            "enableEnhancedCivicAnswers",
        ] {
            if let Some(v) = present(&config, field) {
                generation[field] = v.clone();
            }
        }
        if let Some(v) = present(&config, "speechConfig") {
            generation["speechConfig"] = match v {
                Value::String(s) => json!({"voiceConfig":{"prebuiltVoiceConfig":{"voiceName":s}}}),
                Value::Object(_) | Value::Array(_) => v.clone(),
                _ => return Err(Error::Local),
            };
        }
        if let Some(v) = present(&config, "imageConfig") {
            if !cloud {
                reject(
                    v,
                    &[
                        "personGeneration",
                        "prominentPeople",
                        "outputMimeType",
                        "outputCompressionQuality",
                        "imageOutputOptions",
                    ],
                )?;
            }
            let mut image = copy(
                v,
                &[
                    "aspectRatio",
                    "imageSize",
                    "personGeneration",
                    "prominentPeople",
                ],
            );
            if let Some(x) = present(v, "outputMimeType") {
                image["imageOutputOptions"] = json!({"mimeType":x});
            }
            if let Some(x) = present(v, "outputCompressionQuality") {
                if image.get("imageOutputOptions").is_none() {
                    image["imageOutputOptions"] = object();
                }
                image["imageOutputOptions"]["compressionQuality"] = x.clone();
            }
            if let Some(x) = present(v, "imageOutputOptions") {
                if image.get("imageOutputOptions").is_none() {
                    image["imageOutputOptions"] = x.clone();
                } else {
                    deep_merge(&mut image["imageOutputOptions"], x, 0)?;
                }
            }
            generation["imageConfig"] = image;
        }
        body["generationConfig"] = generation;
    }
    let http = config.get("httpOptions").cloned().unwrap_or(Value::Null);
    if let Some(extra) = present(&http, "extraBody") {
        deep_merge(&mut body, extra, 0)?;
    }

    let default_base = request.resolved.endpoint.expose_for_transport();
    let base = http
        .get("baseUrl")
        .and_then(Value::as_str)
        .unwrap_or(default_base);

    let default_version = "";
    let version = http
        .get("apiVersion")
        .and_then(Value::as_str)
        .unwrap_or(default_version);

    let base = base.strip_suffix('/').unwrap_or(base);
    let path = if version.is_empty() {
        format!("{base}/{model_path}:streamGenerateContent?alt=sse")
    } else {
        format!("{base}/{version}/{model_path}:streamGenerateContent?alt=sse")
    };
    let mut url = url::Url::parse(&path).map_err(|_| Error::Local)?;
    let alt = url
        .query_pairs()
        .find(|(k, _)| k == "alt")
        .map(|(_, v)| v.into_owned());
    if alt.as_deref() != Some("sse") {
        let pairs: Vec<_> = url
            .query_pairs()
            .filter(|(k, _)| k != "alt")
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();
        url.query_pairs_mut()
            .clear()
            .extend_pairs(pairs)
            .append_pair("alt", "sse");
    }
    let mut supplied = object();
    for (name, value) in pi_ai_core::get_identity_headers(
        request.resolved.model.provider.as_str(),
        request.resolved.endpoint.expose_for_transport(),
        request.options.wire.identity.as_ref(),
    ) {
        supplied[&name] = json!(value);
    }
    if let Some(headers) = &request.resolved.model.headers {
        for (name, value) in headers.as_map() {
            supplied[name] = json!(value);
        }
    }
    for (name, value) in request.auth.headers.iter_for_dispatch() {
        supplied[name] = json!(value);
    }
    let mut headers = object();
    headers["User-Agent"] = json!("google-genai-sdk/2.21.0 pi-ai-rust");
    headers["x-goog-api-client"] = headers["User-Agent"].clone();
    headers["Content-Type"] = json!("application/json");
    for (name, value) in supplied.as_object().ok_or(Error::Local)? {
        if !value.is_null() {
            headers[name] = value.clone();
        }
    }
    let mut extra_headers = http.get("headers").cloned().unwrap_or(object());
    if config
        .get("tools")
        .and_then(Value::as_array)
        .is_some_and(|a| a.iter().any(|v| v.get("inputSchema").is_some()))
    {
        if extra_headers.as_object().is_none_or(|o| o.is_empty()) {
            extra_headers = json!({"User-Agent":"google-genai-sdk/2.21.0 pi-ai-rust","x-goog-api-client":"google-genai-sdk/2.21.0 pi-ai-rust","Content-Type":"application/json"});
        }
        let existing = extra_headers
            .get("x-goog-api-client")
            .filter(|v| !v.is_null())
            .map(sdk_string)
            .unwrap_or_default();
        extra_headers["x-goog-api-client"] =
            json!(format!("{existing} mcp_used/unknown").trim_start_matches(whitespace));
    }
    if let Some(values) = extra_headers.as_object() {
        for (name, value) in values {
            headers[name] = value.clone();
        }
    }
    let mut wire_headers = HttpHeaders::new();
    for (name, value) in headers.as_object().ok_or(Error::Local)? {
        wire_headers
            .append(
                name,
                sdk_string(value).trim_matches([' ', '\t', '\r', '\n']),
            )
            .map_err(|_| Error::Local)?;
    }
    let timeout_number = http
        .get("timeout")
        .and_then(Value::as_f64)
        .filter(|n| *n > 0.0);
    if let Some(ms) = timeout_number {
        wire_headers
            .append(
                "X-Server-Timeout",
                ryu_js::Buffer::new().format((ms / 1000.0).ceil()),
            )
            .map_err(|_| Error::Local)?;
    }
    if key.is_some_and(|key| key.starts_with("auth_tokens/")) {
        return Err(Error::Local);
    }
    if let Some(key) = key
        && wire_headers.get_for_wire("x-goog-api-key").is_none()
    {
        wire_headers
            .append("x-goog-api-key", key)
            .map_err(|_| Error::Local)?;
    }
    let timeout = timeout_number.map(|ms| {
        Duration::from_millis(if ms >= 2147483648.0 || ms < 1.0 {
            1
        } else {
            ms as u64
        })
    });
    Ok(Prepared {
        request: HttpRequest {
            endpoint: RequestEndpoint::new_resolved(url.as_str()).map_err(|_| Error::Local)?,
            method: "POST".into(),
            headers: wire_headers,
            body: stringify_wire_json(&body)
                .map_err(|_| Error::Limit)?
                .into_bytes(),
            proxy: None,
        },
        timeout,
        cloud,
    })
}

pub(crate) fn http_error(
    status: u16,
    status_text: &str,
    headers: &HttpHeaders,
    bytes: &[u8],
) -> Error {
    let text = String::from_utf8_lossy(bytes);
    let text = text.strip_prefix('\u{feff}').unwrap_or(&text);
    let body = if headers
        .get_for_fetch("content-type")
        .is_some_and(|v| v.contains("application/json"))
    {
        match serde_json::from_str::<Value>(text) {
            Ok(v) => v,
            Err(_) => return Error::Local,
        }
    } else {
        let raw: Result<Vec<u8>, _> = status_text
            .chars()
            .map(|c| u8::try_from(c as u32))
            .collect();
        let Ok(raw) = raw else {
            return Error::Local;
        };
        json!({"error":{"message":text,"code":status,"status":String::from_utf8_lossy(&raw)}})
    };
    match stringify_wire_json(&body) {
        Ok(message) => Error::Message {
            status: Some(status),
            api: (400..600).contains(&status),
            message,
        },
        Err(_) => Error::Limit,
    }
}
pub(crate) fn frame_metadata(status_text: &str, data: &str) -> Result<(), Error> {
    let raw: Vec<u8> = status_text
        .chars()
        .map(|c| u8::try_from(c as u32).map_err(|_| Error::Local))
        .collect::<Result<_, _>>()?;
    // Node's fixed WebIDL ByteString template includes the first offending
    // UTF-16 index and code unit. Those bounded wire-derived operands can change
    // provider classifiers, independently of the data's billing/overflow text.
    if let Some((index, unit)) = String::from_utf8_lossy(&raw)
        .encode_utf16()
        .enumerate()
        .find(|(_, unit)| *unit > 255)
    {
        return Err(message(format!(
            "exception parsing stream chunk {data}. TypeError: Cannot convert argument to a ByteString because the character at index {index} has a value of {unit} which is greater than 255."
        )));
    }
    Ok(())
}

/// Google SDK couples a whole-read JSON error probe to its delimiter framing.
/// This is not the generic SSE line decoder. Pending input/frame limits remain
/// explicit; transient JSON parsing and returned frame ownership are separate.
pub(crate) struct Decoder {
    buffer: String,
    utf8: Vec<u8>,
    bom: bool,
    limits: SseLimits,
    emitted: usize,
    failed: bool,
}
impl Decoder {
    pub fn new(limits: SseLimits) -> Self {
        Self {
            buffer: String::new(),
            utf8: Vec::new(),
            bom: true,
            limits,
            emitted: 0,
            failed: false,
        }
    }
    pub fn push(&mut self, bytes: &[u8]) -> Result<(), Error> {
        if self.failed {
            return Err(Error::Local);
        }
        let result = self.push_inner(bytes);
        if result.is_err() {
            self.clear();
            self.failed = true;
        }
        result
    }
    fn push_inner(&mut self, bytes: &[u8]) -> Result<(), Error> {
        if bytes.len()
            > self
                .limits
                .max_buffer_bytes
                .saturating_sub(self.buffer.len() + self.utf8.len())
        {
            return Err(Error::Limit);
        }
        let mut raw = std::mem::take(&mut self.utf8);
        raw.try_reserve_exact(bytes.len())
            .map_err(|_| Error::Limit)?;
        raw.extend_from_slice(bytes);
        let mut chunk = String::new();
        let mut at = 0;
        let append = |chunk: &mut String, text: &str| -> Result<(), Error> {
            if text.len()
                > self
                    .limits
                    .max_buffer_bytes
                    .saturating_sub(self.buffer.len() + chunk.len())
            {
                return Err(Error::Limit);
            }
            chunk
                .try_reserve_exact(text.len())
                .map_err(|_| Error::Limit)?;
            chunk.push_str(text);
            Ok(())
        };
        while at < raw.len() {
            match std::str::from_utf8(&raw[at..]) {
                Ok(s) => {
                    append(&mut chunk, s)?;
                    break;
                }
                Err(e) => {
                    let valid = &raw[at..at + e.valid_up_to()];
                    append(
                        &mut chunk,
                        std::str::from_utf8(valid).map_err(|_| Error::Local)?,
                    )?;
                    at += e.valid_up_to();
                    if let Some(len) = e.error_len() {
                        append(&mut chunk, "\u{fffd}")?;
                        at += len;
                    } else {
                        self.utf8.extend_from_slice(&raw[at..]);
                        break;
                    }
                }
            }
        }
        if self.bom && !chunk.is_empty() {
            if chunk.starts_with('\u{feff}') {
                chunk.drain(..3);
            }
            self.bom = false;
        }
        if chunk.len()
            > self
                .limits
                .max_buffer_bytes
                .saturating_sub(self.buffer.len() + self.utf8.len())
        {
            return Err(Error::Limit);
        }
        if let Ok(value) = serde_json::from_str::<Value>(&chunk)
            && let Some(error) = value.get("error")
        {
            let code = error.get("code").map(number);
            if code.is_some_and(|n| (400.0..600.0).contains(&n)) {
                let status = code.filter(|n| n.fract() == 0.0).map(|n| n as u16);
                let label = error
                    .get("status")
                    .map(sdk_string)
                    .unwrap_or("undefined".into());
                return Err(Error::Message {
                    status,
                    api: true,
                    message: format!(
                        "got status: {label}. {}",
                        stringify_wire_json(&value).map_err(|_| Error::Limit)?
                    ),
                });
            }
        }
        self.buffer
            .try_reserve_exact(chunk.len())
            .map_err(|_| Error::Limit)?;
        self.buffer.push_str(&chunk);
        self.emitted = 0;
        Ok(())
    }
    pub fn next(&mut self) -> Result<Option<String>, Error> {
        if self.failed {
            return Err(Error::Local);
        }
        let result = self.next_inner();
        if result.is_err() {
            self.clear();
            self.failed = true;
        }
        result
    }
    fn next_inner(&mut self) -> Result<Option<String>, Error> {
        loop {
            let delimiter = ["\n\n", "\r\r", "\r\n\r\n"]
                .iter()
                .filter_map(|d| self.buffer.find(d).map(|at| (at, d.len())))
                .min_by_key(|(at, _)| *at);
            let Some((at, len)) = delimiter else {
                if self.buffer.len() > self.limits.max_frame_bytes {
                    return Err(Error::Limit);
                }
                return Ok(None);
            };
            if at > self.limits.max_frame_bytes {
                return Err(Error::Limit);
            }
            let event = self.buffer[..at]
                .trim_matches(whitespace)
                .strip_prefix("data:")
                .map(|v| v.trim_matches(whitespace).to_owned());
            self.buffer.drain(..at + len);
            if event.is_some() {
                if self.emitted >= self.limits.max_frames_per_feed {
                    return Err(Error::Limit);
                }
                self.emitted += 1;
                return Ok(event);
            }
        }
    }
    pub fn finish(&mut self) -> Result<(), Error> {
        let incomplete = !self.buffer.trim_matches(whitespace).is_empty();
        self.clear();
        if incomplete {
            Err(message("Incomplete JSON segment at the end"))
        } else {
            Ok(())
        }
    }
    fn clear(&mut self) {
        self.buffer = String::new();
        self.utf8 = Vec::new();
    }
}

pub(crate) fn response(value: Value, cloud: bool) -> Result<Value, Error> {
    let mut out = copy(
        &value,
        if cloud {
            &[
                "candidates",
                "createTime",
                "modelVersion",
                "promptFeedback",
                "responseId",
                "usageMetadata",
            ]
        } else {
            &[
                "modelVersion",
                "promptFeedback",
                "responseId",
                "usageMetadata",
                "modelStatus",
            ]
        },
    );
    if !cloud && let Some(candidates) = present(&value, "candidates") {
        out["candidates"] = array_map(candidates, |candidate| {
            let mut out = copy(
                candidate,
                &[
                    "content",
                    "tokenCount",
                    "finishReason",
                    "groundingMetadata",
                    "avgLogprobs",
                    "index",
                    "logprobsResult",
                    "safetyRatings",
                    "urlContextMetadata",
                ],
            );
            if let Some(citation) = present(candidate, "citationMetadata") {
                let mut c = object();
                if let Some(s) = present(citation, "citationSources") {
                    c["citations"] = s.clone();
                }
                out["citationMetadata"] = c;
            }
            Ok(out)
        })?;
    }
    Ok(out)
}

#[cfg(test)]
mod metadata_tests {
    use super::*;

    #[test]
    fn fixed_bytestring_template_matches_executed_utf16_operands() {
        let corpus: Value = serde_json::from_str(include_str!(
            "../../../../conformance/provider-traces/google-genai/metadata.json"
        ))
        .unwrap();
        for case in corpus["cases"].as_array().unwrap() {
            let hex = case["statusHex"].as_str().unwrap();
            let phrase: String = (0..hex.len())
                .step_by(2)
                .map(|at| char::from(u8::from_str_radix(&hex[at..at + 2], 16).unwrap()))
                .collect();
            let data = case["data"].as_str().unwrap();
            match frame_metadata(&phrase, data) {
                Ok(()) => assert!(case["constructorError"].is_null()),
                Err(Error::Message { message, .. }) => assert_eq!(
                    message,
                    format!(
                        "exception parsing stream chunk {data}. {}",
                        case["constructorError"].as_str().unwrap()
                    ),
                    "{}",
                    case["id"]
                ),
                Err(_) => panic!("unexpected metadata failure"),
            }
        }
    }
}
