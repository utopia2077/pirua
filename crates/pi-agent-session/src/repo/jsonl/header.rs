//! Header-only format discovery. No node normalization or current body admission.
use super::*;
use serde::Deserialize;
use serde_json::Value;
fn first(source: &[u8]) -> Result<Value, SessionError> {
    let line = source.split(|b| *b == b'\n').next().ok_or_else(damaged)?;
    let object: JsonObject = serde_json::from_slice(line).map_err(|_| damaged())?;
    serde_json::to_value(object).map_err(|_| damaged())
}
pub(super) fn is_legacy(source: &[u8]) -> Result<bool, SessionError> {
    let value = first(source)?;
    if value.get("v") == Some(&Value::from(4)) {
        return Ok(false);
    }
    if value.get("type") == Some(&Value::from("session"))
        && value.get("version") == Some(&Value::from(3))
    {
        return Ok(true);
    }
    Err(storage(StorageError::Unsupported(context(
        "unsupported session file format",
    ))))
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct LegacyHeader {
    #[serde(rename = "type")]
    kind: String,
    version: SafeUnsigned,
    id: SessionId,
    timestamp: String,
    cwd: String,
    #[serde(default)]
    parent_session: Presence<String>,
}
pub(super) fn read(claim: &mut FileClaim) -> Result<SessionMetadata, SessionError> {
    let source = claim.read_source().map_err(storage)?;
    if !is_legacy(&source)? {
        return Ok(claim.read_header().map_err(storage)?.metadata);
    }
    let value: LegacyHeader = serde_json::from_value(first(&source)?).map_err(|_| damaged())?;
    if value.kind != "session" || value.version.get() != 3 {
        return Err(damaged());
    }
    let parent = match value.parent_session {
        Presence::Missing => None,
        Presence::Value(value) => Some(value),
        Presence::Null => return Err(damaged()),
    };
    Ok(SessionMetadata {
        id: value.id,
        created_at: legacy_v3::parse_iso_timestamp(&value.timestamp).map_err(legacy_error)?,
        storage_version: SafeUnsigned::new(1).expect("one"),
        store_generation: SafeUnsigned::new(0).expect("zero"),
        cwd: Some(value.cwd),
        parent_session_id: None,
        legacy_parent_session_path: parent,
    })
}
