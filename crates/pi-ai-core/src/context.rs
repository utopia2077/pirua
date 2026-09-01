//! Provider-neutral request contexts.

use serde::{Deserialize, Serialize};

use crate::content::InputContent;
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

/// Image-generation request context.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImagesContext {
    /// Text and image prompt blocks.
    pub input: Vec<InputContent>,
}
