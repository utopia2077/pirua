use super::SearchError;
use pi_agent_schema::Entry;

/// Explicit, stable text policy. Callbacks run outside SQL and queue locks.
/// A changed policy must use a different identity and a separately rebuilt index.
pub trait TextProjector: Send + Sync {
    fn identity(&self) -> &str;
    fn project(&self, entry: &Entry) -> Result<Option<String>, SearchError>;
}
/// Message content strings or text blocks only; includes every settled status.
pub struct DefaultTextProjector;
impl TextProjector for DefaultTextProjector {
    fn identity(&self) -> &str {
        "message-content-text-v1"
    }
    fn project(&self, entry: &Entry) -> Result<Option<String>, SearchError> {
        let Entry::Message(entry) = entry else {
            return Ok(None);
        };
        let value = serde_json::to_value(&entry.message).map_err(|_| SearchError::Projection)?;
        let text = match value.get("content") {
            Some(serde_json::Value::String(text)) => text.clone(),
            Some(serde_json::Value::Array(blocks)) => blocks
                .iter()
                .filter(|b| b.get("type").and_then(|t| t.as_str()) == Some("text"))
                .filter_map(|b| b.get("text").and_then(|t| t.as_str()))
                .collect::<Vec<_>>()
                .join("\n"),
            _ => String::new(),
        };
        Ok((!text.is_empty()).then_some(text))
    }
}
