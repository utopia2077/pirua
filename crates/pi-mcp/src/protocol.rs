use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

pub const LATEST_PROTOCOL_VERSION: &str = "2025-11-25";
pub const SUPPORTED_PROTOCOL_VERSIONS: &[&str] = &[
    LATEST_PROTOCOL_VERSION,
    "2025-06-18",
    "2025-03-26",
    "2024-11-05",
];
pub const DEFAULT_MAX_MESSAGE_BYTES: usize = 16 * 1024 * 1024;
pub type Object = Map<String, Value>;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum JsonRpcId {
    String(String),
    Number(serde_json::Number),
}

fn normalized_id(number: &serde_json::Number) -> serde_json::Number {
    if number.is_f64()
        && let Some(value) = number.as_f64()
        && value.fract() == 0.0
    {
        // Preserve exact large integer IDs; only normalize integral floats that
        // fit without a saturating cast at the u64/i64 bounds.
        if value >= 0.0 && value < u64::MAX as f64 {
            return serde_json::Number::from(value as u64);
        }
        if value < 0.0 && value >= i64::MIN as f64 {
            return serde_json::Number::from(value as i64);
        }
    }
    number.clone()
}
impl PartialEq for JsonRpcId {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::String(a), Self::String(b)) => a == b,
            (Self::Number(a), Self::Number(b)) => normalized_id(a) == normalized_id(b),
            _ => false,
        }
    }
}
impl Eq for JsonRpcId {}
impl std::hash::Hash for JsonRpcId {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Self::String(value) => {
                0_u8.hash(state);
                value.hash(state);
            }
            Self::Number(value) => {
                1_u8.hash(state);
                normalized_id(value).hash(state);
            }
        }
    }
}

impl From<u64> for JsonRpcId {
    fn from(value: u64) -> Self {
        Self::Number(value.into())
    }
}

/// Validate one message. Batches and null IDs are deliberately unsupported.
pub fn validate_message(value: &Value) -> Result<()> {
    let object = value
        .as_object()
        .ok_or_else(|| Error::invalid("JSON-RPC object required"))?;
    if object.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return Err(Error::invalid("jsonrpc must be 2.0"));
    }
    let id = object.get("id");
    if let Some(id) = id {
        serde_json::from_value::<JsonRpcId>(id.clone())?;
    }
    if object.get("method").is_some_and(Value::is_string) {
        return Ok(());
    }
    if id.is_none() {
        return Err(Error::invalid("response ID missing"));
    }
    match (object.get("result"), object.get("error")) {
        (Some(_), None) => Ok(()),
        (None, Some(e))
            if e.get("code").is_some_and(Value::is_i64)
                && e.get("message").is_some_and(Value::is_string) =>
        {
            Ok(())
        }
        _ => Err(Error::invalid("invalid JSON-RPC response")),
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Implementation {
    pub name: String,
    pub version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Root {
    pub uri: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct InitializeResult {
    pub protocol_version: String,
    pub capabilities: Object,
    pub server_info: Implementation,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instructions: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ProgressNotification {
    pub progress_token: JsonRpcId,
    pub progress: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// Unknown optional protocol fields are preserved in `extra` for host adapters.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Tool {
    pub name: String,
    pub input_schema: Object,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(flatten)]
    pub extra: Object,
}

/// MCP content stays detached JSON: all block fields, annotations and extensions
/// survive transport. `kind` enables adapters to select supported block types.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ContentBlock {
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(flatten)]
    pub fields: Object,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CallToolResult {
    #[serde(default)]
    pub content: Vec<ContentBlock>,
    /// `Some(Value::Null)` is distinct from absent structured content.
    #[serde(
        default,
        deserialize_with = "present_value",
        skip_serializing_if = "Option::is_none"
    )]
    pub structured_content: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_error: Option<bool>,
    #[serde(flatten)]
    pub extra: Object,
}
fn present_value<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<Value>, D::Error> {
    Value::deserialize(deserializer).map(Some)
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Resource {
    pub uri: String,
    pub name: String,
    #[serde(flatten)]
    pub extra: Object,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ResourceTemplate {
    pub uri_template: String,
    pub name: String,
    #[serde(flatten)]
    pub extra: Object,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ResourceContents {
    pub uri: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blob: Option<String>,
    #[serde(flatten)]
    pub extra: Object,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ReadResourceResult {
    pub contents: Vec<ResourceContents>,
    #[serde(flatten)]
    pub extra: Object,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ListPage<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<String>,
}
