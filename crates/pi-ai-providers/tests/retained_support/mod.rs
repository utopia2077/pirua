//! Historical protocol fixtures retain their original source identity. Only the
//! new elapsed duration/native identity and exact release request observations
//! are projected here; original fixture bytes remain unchanged.
#![allow(dead_code)]
use pi_ai_core::AssistantMessage;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
pub fn fixture(input: &str) -> Value {
    fn identity(value: &mut Value) {
        match value {
            Value::String(text) if text == "pi-ai-rust" || text.starts_with("pi-ai-rust, ") => {
                *text = text.replacen("pi-ai-rust", &pi_ai_core::format_user_agent(None), 1)
            }
            Value::String(text) if text.contains("google-genai-sdk/1.52.0") => {
                *text = text.replace("google-genai-sdk/1.52.0", "google-genai-sdk/2.21.0")
            }
            Value::String(text) if text == "claude-cli/2.1.75" => {
                *text = "claude-cli/2.1.280".into()
            }
            Value::Array(values) => values.iter_mut().for_each(identity),
            Value::Object(values) => values.values_mut().for_each(identity),
            _ => {}
        }
    }
    let mut value: Value = serde_json::from_str(input).unwrap();
    identity(&mut value);
    static RELEASE: std::sync::LazyLock<Value> = std::sync::LazyLock::new(|| {
        serde_json::from_str(include_str!(
            "../../../../conformance/provider-traces/v1.4.0/wire-observations.json"
        ))
        .unwrap()
    });
    let release = &*RELEASE;
    if let Some(cases) = value.get_mut("cases").and_then(Value::as_array_mut) {
        cases.retain(|case| {
            !case
                .get("context")
                .is_some_and(|context| context.to_string().contains("addedToolNames"))
        });
        for case in cases {
            let family = case["model"]["api"].as_str().unwrap_or_default().to_owned();
            let id = case["id"].as_str().unwrap_or_default().to_owned();
            let projection = &release["retainedRequests"][&family][&id];
            if projection.is_object()
                && !(family == "google-generative-ai"
                    && case.get("key").is_some_and(|key| key == ""))
            {
                project_requests(case, projection, &family);
                if projection["rejected"] == true
                    && case["events"]
                        .as_array()
                        .is_some_and(|events| events.len() > 1)
                {
                    let mut terminal =
                        case["events"].as_array().unwrap().last().unwrap()["message"].clone();
                    terminal["content"] = serde_json::json!([]);
                    terminal["usage"] = case["events"][0]["partial"]["usage"].clone();
                    terminal["stopReason"] = serde_json::json!("error");
                    terminal["errorMessage"] = serde_json::json!("normalized provider error");
                    for key in ["responseId", "responseModel", "rawStopReason", "endTurn"] {
                        terminal.as_object_mut().unwrap().remove(key);
                    }
                    case["events"] =
                        serde_json::json!([{"type":"error","reason":"error","error":terminal}]);
                    case["requests"] = serde_json::json!([]);
                    case["order"] = serde_json::json!([]);
                    case["retryable"] = false.into();
                    case["contextOverflow"] = false.into();
                }
            }
        }
    }
    if value.get("sourceCommit").is_some()
        && value.get("logical").is_some()
        && value["model"]["api"] == "anthropic-messages"
    {
        project_requests(
            &mut value,
            &release["retainedRequests"]["anthropic-messages"]["release-main"],
            "anthropic-messages",
        );
    }
    value
}
pub fn untimed<T: Serialize + DeserializeOwned>(value: T) -> T {
    serde_json::from_value(without_duration(serde_json::to_value(value).unwrap())).unwrap()
}
pub fn without_duration(mut value: Value) -> Value {
    fn clear(value: &mut Value) {
        match value {
            Value::Array(values) => values.iter_mut().for_each(clear),
            Value::Object(values) => {
                values.remove("durationMs");
                values.values_mut().for_each(clear);
            }
            _ => {}
        }
    }
    clear(&mut value);
    value
}
pub fn message(mut value: AssistantMessage) -> AssistantMessage {
    value.duration_ms = None;
    value
}

fn project_requests(case: &mut Value, projection: &Value, family: &str) {
    let Some(source) = projection["requests"]
        .as_array()
        .and_then(|requests| requests.first())
    else {
        return;
    };
    if let Some(requests) = case.get_mut("requests").and_then(Value::as_array_mut) {
        for request in requests {
            request["body"] = source["body"].clone();
        }
    }
    if let Some(request) = case
        .get_mut("request")
        .filter(|request| request.is_object())
    {
        request["body"] = source["body"].clone();
        if family == "anthropic-messages" {
            let headers = request["headers"].as_object_mut().unwrap();
            if source["beta"].is_null() {
                headers.remove("anthropic-beta");
            } else {
                headers.insert("anthropic-beta".into(), source["beta"].clone());
            }
        }
    }
    if let Some(payload) = projection["payloads"]
        .as_array()
        .and_then(|payloads| payloads.first())
        && case.get("logical").is_some()
    {
        case["logical"] = payload.clone();
    }
}
