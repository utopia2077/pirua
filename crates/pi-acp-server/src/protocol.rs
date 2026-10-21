//! Pinned baseline validation, JSON-RPC envelopes and content admission.
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc};

pub const TUI: &str = "pirua.dev/tui";
pub const RENDER: &str = "pirua.dev/render";
pub const CURSOR: &str = "pirua.dev/projection";
pub type RpcResult = Result<Value, RpcError>;
#[derive(Clone, Debug)]
pub struct RpcError {
    pub code: i64,
    pub message: String,
    pub data: Option<Value>,
}
impl RpcError {
    pub fn new(code: i64, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            data: None,
        }
    }
    pub fn params(message: impl Into<String>) -> Self {
        Self::new(-32602, message)
    }
    pub fn app(kind: &str, message: impl Into<String>) -> Self {
        Self {
            code: -32050,
            message: message.into(),
            data: Some(json!({"kind":kind})),
        }
    }
    pub fn failed(error: impl std::fmt::Display) -> Self {
        Self::app("operation_failed", error.to_string())
    }
    pub fn cancelled() -> Self {
        Self::new(-32800, "Request cancelled before insertion")
    }
    pub fn value(&self) -> Value {
        let mut v = json!({"code":self.code,"message":self.message});
        if let Some(data) = &self.data {
            v["data"] = data.clone();
        }
        v
    }
}
pub fn response(id: Value, result: RpcResult) -> Value {
    match result {
        Ok(result) => json!({"jsonrpc":"2.0","id":id,"result":result}),
        Err(error) => json!({"jsonrpc":"2.0","id":id,"error":error.value()}),
    }
}
pub fn notification(method: &str, params: Value) -> Value {
    json!({"jsonrpc":"2.0","method":method,"params":params})
}
pub fn string<'a>(v: &'a Value, key: &str) -> Result<&'a str, RpcError> {
    v.get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| RpcError::params(format!("{key} must be a string")))
}
pub fn optional_bool(v: &Value, key: &str, default: bool) -> Result<bool, RpcError> {
    match v.get(key) {
        None => Ok(default),
        Some(Value::Bool(b)) => Ok(*b),
        _ => Err(RpcError::params(format!("{key} must be a boolean"))),
    }
}

/// Validation uses the exact baseline schema, not every unstable feature in the crate.
pub struct Contracts {
    validators: BTreeMap<String, Arc<jsonschema::Validator>>,
}
impl Contracts {
    pub fn new() -> Self {
        let schema: Value =
            serde_json::from_str(include_str!("../../../conformance/acp-v2/schema.json"))
                .expect("pinned schema");
        let mut validators = BTreeMap::new();
        for (name, def) in schema["$defs"].as_object().expect("definitions") {
            if def.get("x-method").and_then(Value::as_str).is_some() {
                let root = json!({"$schema":schema["$schema"],"$defs":schema["$defs"],"$ref":format!("#/$defs/{name}")});
                validators.insert(
                    name.clone(),
                    Arc::new(jsonschema::validator_for(&root).expect("pinned contract")),
                );
            }
        }
        Self { validators }
    }
    pub fn validate(&self, name: &str, value: &Value) -> Result<(), RpcError> {
        self.validators
            .get(name)
            .expect("known contract")
            .validate(value)
            .map_err(|e| RpcError::params(e.to_string()))
    }
}
impl Default for Contracts {
    fn default() -> Self {
        Self::new()
    }
}

pub fn content(prompt: &Value) -> Result<(String, Vec<pi_ai::ImageContent>), RpcError> {
    // The official v2 DTO also preserves future open variants. Baseline validation
    // rejects malformed recognized variants before the application reads them.
    let blocks = prompt
        .as_array()
        .ok_or_else(|| RpcError::params("prompt must be an array"))?;
    let mut text = Vec::new();
    let mut images = Vec::new();
    for block in blocks {
        let _: agent_client_protocol_schema::v2::ContentBlock =
            serde_json::from_value(block.clone()).map_err(RpcError::failed)?;
        match block["type"].as_str() {
            Some("text") => text.push(string(block, "text")?.to_owned()),
            Some("resource_link") => text.push(format!(
                "[Context: {}]({})",
                string(block, "name")?,
                string(block, "uri")?
            )),
            Some("image") => {
                let data = string(block, "data")?;
                let mime = string(block, "mimeType")?;
                images.push(
                    serde_json::from_value(json!({"type":"image","data":data,"mimeType":mime}))
                        .map_err(|e| RpcError::params(e.to_string()))?,
                );
            }
            Some("audio" | "resource") => {
                return Err(RpcError::app(
                    "unsupported",
                    "This host supports text, image and resource-link prompts",
                ));
            }
            Some(_) => {}
            None => return Err(RpcError::params("Content block requires type")),
        }
    }
    Ok((text.join("\n"), images))
}
