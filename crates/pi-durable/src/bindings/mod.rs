//! Ordinary tools and authenticated native extensions for Durable.
//!
//! These adapters use the runtime's validation, committed intent, invocation
//! custody, progress and finalizer. Hosts retain environments, MCP clients and
//! credentials. No Agent or CodingSession is constructed.
pub mod coding;
pub mod extensions;
pub mod mcp;

use crate::{Error, Result};
use pi_ai::{InputContent, JsonValue, Presence};

fn json(value: serde_json::Value) -> Result<JsonValue> {
    serde_json::from_value(value).map_err(Error::from)
}
fn text(value: impl Into<String>) -> InputContent {
    InputContent::Text(pi_ai::TextContent {
        content_type: Default::default(),
        text: value.into(),
        text_signature: None,
    })
}
fn presence(value: Option<JsonValue>) -> Presence<JsonValue> {
    value.map_or(Presence::Missing, |v| match v {
        JsonValue::Null => Presence::Null,
        other => Presence::Value(other),
    })
}
