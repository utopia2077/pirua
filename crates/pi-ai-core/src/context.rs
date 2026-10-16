//! Provider-neutral request contexts.

use serde::{Deserialize, Serialize};

use crate::message::Message;
use crate::tool::Tool;

/// Text-model request context.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Context {
    /// Optional system prompt.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub system_prompt: Option<String>,
    /// Ordered provider messages.
    pub messages: Vec<Message>,
    /// Optional available tool declarations.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub tools: Option<Vec<Tool>>,
}
