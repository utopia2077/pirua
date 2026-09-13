use super::{LegacyError, parse_iso_timestamp};
use pi_agent_schema::session_values::{JsonObject, SafeUnsigned};
use serde_json::{Map, Value};

pub(super) struct Header {
    pub id: String,
    pub timestamp: SafeUnsigned,
    pub cwd: String,
    pub parent: Option<String>,
}
pub(super) struct Node {
    pub id: String,
    pub parent: Option<String>,
    pub timestamp: SafeUnsigned,
    pub value: Value,
    pub kind: Kind,
}
#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum Kind {
    Message,
    Custom,
    CustomMessage,
    Compaction,
    BranchSummary,
    Label,
    SessionInfo,
    Discard,
}
impl Kind {
    pub fn retained(self) -> bool {
        matches!(
            self,
            Self::Message
                | Self::Custom
                | Self::CustomMessage
                | Self::Compaction
                | Self::BranchSummary
        )
    }
}
fn object(bytes: &[u8]) -> Result<Value, LegacyError> {
    let object: JsonObject = serde_json::from_slice(bytes).map_err(|_| LegacyError::InvalidData)?;
    serde_json::to_value(object).map_err(|_| LegacyError::InvalidData)
}
pub(super) fn string(value: &Value, key: &str) -> Result<String, LegacyError> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or(LegacyError::InvalidData)
}
pub(super) fn optional_string(value: &Value, key: &str) -> Result<Option<String>, LegacyError> {
    match value.get(key) {
        None => Ok(None),
        Some(Value::String(s)) => Ok(Some(s.clone())),
        _ => Err(LegacyError::InvalidData),
    }
}
fn only(map: &Map<String, Value>, keys: &[&str]) -> Result<(), LegacyError> {
    if map.keys().all(|key| keys.contains(&key.as_str())) {
        Ok(())
    } else {
        Err(LegacyError::InvalidData)
    }
}
pub(super) fn parse(source: &[u8]) -> Result<(Header, Vec<Node>), LegacyError> {
    let mut lines: Vec<_> = source.split(|b| *b == b'\n').collect();
    while lines
        .last()
        .is_some_and(|line| line.iter().all(u8::is_ascii_whitespace))
    {
        lines.pop();
    }
    let first = lines.first().ok_or(LegacyError::InvalidData)?;
    let value = object(first)?;
    if value.get("type").and_then(Value::as_str) != Some("session")
        || value.get("version").and_then(Value::as_u64) != Some(3)
    {
        return Err(LegacyError::InvalidData);
    }
    only(
        value.as_object().ok_or(LegacyError::InvalidData)?,
        &["type", "version", "id", "timestamp", "cwd", "parentSession"],
    )?;
    let header = Header {
        id: string(&value, "id")?,
        timestamp: parse_iso_timestamp(&string(&value, "timestamp")?)?,
        cwd: string(&value, "cwd")?,
        parent: optional_string(&value, "parentSession")?,
    };
    if header.id.is_empty() {
        return Err(LegacyError::InvalidData);
    }
    let mut nodes = Vec::with_capacity(lines.len() - 1);
    for line in &lines[1..] {
        let value = object(line)?;
        let tag = string(&value, "type")?;
        let (kind, extras): (Kind, &[&str]) = match tag.as_str() {
            "message" => (Kind::Message, &["message"]),
            "custom" => (Kind::Custom, &["customType", "data"]),
            "custom_message" => (
                Kind::CustomMessage,
                &["customType", "content", "display", "details"],
            ),
            "compaction" => (
                Kind::Compaction,
                &[
                    "summary",
                    "firstKeptEntryId",
                    "tokensBefore",
                    "details",
                    "usage",
                    "fromHook",
                ],
            ),
            "branch_summary" => (
                Kind::BranchSummary,
                &["fromId", "summary", "details", "usage", "fromHook"],
            ),
            "label" => (Kind::Label, &["targetId", "label"]),
            "session_info" => (Kind::SessionInfo, &["name"]),
            "model_change" | "thinking_level_change" | "active_tools_change" => {
                (Kind::Discard, &[])
            }
            _ => return Err(LegacyError::InvalidData),
        };
        let id = string(&value, "id")?;
        if id.is_empty() {
            return Err(LegacyError::Ancestry);
        }
        let parent = match value.get("parentId") {
            Some(Value::Null) => None,
            Some(Value::String(s)) if !s.is_empty() => Some(s.clone()),
            _ => return Err(LegacyError::Ancestry),
        };
        let timestamp = parse_iso_timestamp(&string(&value, "timestamp")?)?;
        if kind != Kind::Discard {
            let mut keys = vec!["type", "id", "parentId", "timestamp"];
            keys.extend_from_slice(extras);
            only(value.as_object().ok_or(LegacyError::InvalidData)?, &keys)?;
        }
        nodes.push(Node {
            id,
            parent,
            timestamp,
            value,
            kind,
        });
    }
    Ok((header, nodes))
}
