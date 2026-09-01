//! Provider-neutral message content blocks.

use serde::{Deserialize, Serialize};

use crate::id::{ToolCallId, ToolName};
use crate::json::{JsonObject, LiteralOne};

/// Optional phase carried by a version-one text signature.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum TextSignaturePhase {
    /// Intermediate commentary content.
    #[serde(rename = "commentary")]
    Commentary,
    /// Final-answer content.
    #[serde(rename = "final_answer")]
    FinalAnswer,
}

/// Structured metadata encoded inside a text signature string.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TextSignatureV1 {
    /// Signature schema version, always `1`.
    pub v: LiteralOne,
    /// Provider-defined signature identity.
    pub id: String,
    /// Optional provider output phase.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub phase: Option<TextSignaturePhase>,
}

/// Literal tag for a text content block.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum TextContentType {
    /// The `text` content tag.
    #[default]
    #[serde(rename = "text")]
    Text,
}

/// A text content block.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TextContent {
    /// Literal `text` discriminator.
    #[serde(rename = "type")]
    pub content_type: TextContentType,
    /// Text visible to the model or user.
    pub text: String,
    /// Opaque legacy signature or encoded [`TextSignatureV1`].
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub text_signature: Option<String>,
}

/// Literal tag for a thinking content block.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum ThinkingContentType {
    /// The `thinking` content tag.
    #[default]
    #[serde(rename = "thinking")]
    Thinking,
}

/// A model reasoning or thinking content block.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ThinkingContent {
    /// Literal `thinking` discriminator.
    #[serde(rename = "type")]
    pub content_type: ThinkingContentType,
    /// Provider-returned thinking text.
    pub thinking: String,
    /// Opaque replay metadata or encrypted reasoning payload.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub thinking_signature: Option<String>,
    /// Whether safety filters redacted the visible thinking text.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub redacted: Option<bool>,
}

/// Literal tag for an image content block.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum ImageContentType {
    /// The `image` content tag.
    #[default]
    #[serde(rename = "image")]
    Image,
}

/// A base64-convention image content block.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ImageContent {
    /// Literal `image` discriminator.
    #[serde(rename = "type")]
    pub content_type: ImageContentType,
    /// Base64-encoded image data by convention.
    pub data: String,
    /// Media type such as `image/png`.
    pub mime_type: String,
}

/// Literal tag for a tool-call content block.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum ToolCallType {
    /// The `toolCall` content tag.
    #[default]
    #[serde(rename = "toolCall")]
    ToolCall,
}

/// A model-requested tool call.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ToolCall {
    /// Literal `toolCall` discriminator.
    #[serde(rename = "type")]
    pub content_type: ToolCallType,
    /// Provider or client-generated call identity.
    pub id: ToolCallId,
    /// Target tool name.
    pub name: ToolName,
    /// Object-only tool arguments.
    pub arguments: JsonObject,
    /// Optional Google thought signature.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub thought_signature: Option<String>,
    /// Optional OpenAI Responses tool namespace.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub namespace: Option<String>,
}

/// A block accepted as user or tool-result content.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum InputContent {
    /// Text input.
    Text(TextContent),
    /// Image input.
    Image(ImageContent),
}

/// A block returned in an assistant message.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum AssistantContent {
    /// Text output.
    Text(TextContent),
    /// Thinking output.
    Thinking(ThinkingContent),
    /// Tool-call output.
    ToolCall(ToolCall),
}
