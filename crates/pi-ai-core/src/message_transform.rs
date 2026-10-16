//! Deterministic provider-neutral message transformation.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use crate::content::{AssistantContent, InputContent, TextContent, TextContentType, ToolCall};
use crate::id::ToolCallId;
use crate::json::{Presence, SafeUnsigned};
use crate::message::{
    AssistantMessage, Message, StopReason, ToolResultMessage, ToolResultRole, UserContent,
};
use crate::model::{Model, ModelInput};

const NON_VISION_USER_IMAGE_PLACEHOLDER: &str = "(image omitted: model does not support images)";
const NON_VISION_TOOL_IMAGE_PLACEHOLDER: &str =
    "(tool image omitted: model does not support images)";
const MISSING_TOOL_RESULT_TEXT: &str = "No result provided";

/// Trusted request-local policy for normalizing foreign tool-call identities.
///
/// The policy is borrowed only for one [`transform_messages`] call. It is not
/// stored, serialized, or part of the foreign-extension protocol.
///
/// ```compile_fail
/// use pi_ai_core::ToolCallIdNormalizer;
///
/// fn cannot_serialize(policy: &mut dyn ToolCallIdNormalizer) {
///     let _ = serde_json::to_value(policy);
/// }
/// ```
pub trait ToolCallIdNormalizer {
    /// Returns the target identity for one source tool call.
    fn normalize_tool_call_id(
        &mut self,
        source_id: &ToolCallId,
        target_model: &Model,
        source_message: &AssistantMessage,
    ) -> ToolCallId;
}

/// Failure transforming a strict message transcript.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum MessageTransformError {
    /// Two distinct source identities would become one final identity.
    NormalizedToolCallIdCollision,
}

impl fmt::Display for MessageTransformError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NormalizedToolCallIdCollision => {
                formatter.write_str("normalized tool-call identity collision")
            }
        }
    }
}

impl std::error::Error for MessageTransformError {}

/// Transforms strict messages for a target model.
///
/// The first pass downgrades unsupported images, strips non-portable replay
/// metadata, and normalizes foreign tool-call identities. The second pass
/// removes failed assistant turns and repairs orphaned tool calls. Synthetic
/// result timestamps are supplied by the caller so replay is deterministic.
pub fn transform_messages(
    messages: &[Message],
    target_model: &Model,
    synthetic_result_timestamp: SafeUnsigned,
    mut normalizer: Option<&mut dyn ToolCallIdNormalizer>,
) -> Result<Vec<Message>, MessageTransformError> {
    let supports_images = target_model.input.contains(&ModelInput::Image);
    let mut changed_tool_call_ids = BTreeMap::<ToolCallId, ToolCallId>::new();
    let mut final_id_owners = BTreeMap::<ToolCallId, ToolCallId>::new();
    let mut transformed = Vec::with_capacity(messages.len());

    for message in messages {
        match message {
            Message::System(system) => transformed.push(Message::System(system.clone())),
            Message::User(source) => {
                let mut message = source.clone();
                if !supports_images && let UserContent::Blocks(content) = &source.content {
                    message.content = UserContent::Blocks(replace_images_with_placeholder(
                        content,
                        NON_VISION_USER_IMAGE_PLACEHOLDER,
                    ));
                }
                transformed.push(Message::User(message));
            }
            Message::ToolResult(source) => {
                let mut message = (**source).clone();
                if !supports_images {
                    message.content = replace_images_with_placeholder(
                        &source.content,
                        NON_VISION_TOOL_IMAGE_PLACEHOLDER,
                    );
                }
                if let Some(normalized) = changed_tool_call_ids.get(&source.tool_call_id) {
                    message.tool_call_id = normalized.clone();
                }
                transformed.push(Message::ToolResult(Box::new(message)));
            }
            Message::Assistant(source) => {
                let same_model = source.provider == target_model.provider
                    && source.api == target_model.api
                    && source.model == target_model.id;
                let mut message = (**source).clone();
                let mut content = Vec::with_capacity(source.content.len());

                for block in &source.content {
                    match block {
                        AssistantContent::Thinking(thinking) => {
                            if thinking.redacted == Some(true) {
                                if same_model {
                                    content.push(block.clone());
                                }
                            } else if same_model
                                && thinking
                                    .thinking_signature
                                    .as_deref()
                                    .is_some_and(|signature| !signature.is_empty())
                            {
                                content.push(block.clone());
                            } else if !is_ecmascript_blank(&thinking.thinking) {
                                if same_model {
                                    content.push(block.clone());
                                } else {
                                    content.push(AssistantContent::Text(TextContent {
                                        content_type: TextContentType::Text,
                                        text: thinking.thinking.clone(),
                                        text_signature: None,
                                    }));
                                }
                            }
                        }
                        AssistantContent::Text(text) => {
                            if same_model {
                                content.push(block.clone());
                            } else {
                                content.push(AssistantContent::Text(TextContent {
                                    content_type: TextContentType::Text,
                                    text: text.text.clone(),
                                    text_signature: None,
                                }));
                            }
                        }
                        AssistantContent::ToolCall(source_call) => {
                            let mut tool_call = source_call.clone();
                            if !same_model
                                && source_call
                                    .thought_signature
                                    .as_deref()
                                    .is_some_and(|signature| !signature.is_empty())
                            {
                                tool_call.thought_signature = None;
                            }

                            let final_id = if same_model {
                                source_call.id.clone()
                            } else if let Some(policy) = normalizer.as_deref_mut() {
                                policy.normalize_tool_call_id(&source_call.id, target_model, source)
                            } else {
                                source_call.id.clone()
                            };

                            if final_id_owners
                                .get(&final_id)
                                .is_some_and(|owner| owner != &source_call.id)
                            {
                                return Err(MessageTransformError::NormalizedToolCallIdCollision);
                            }
                            final_id_owners
                                .entry(final_id.clone())
                                .or_insert_with(|| source_call.id.clone());

                            if final_id != source_call.id {
                                changed_tool_call_ids
                                    .insert(source_call.id.clone(), final_id.clone());
                                tool_call.id = final_id;
                            }
                            content.push(AssistantContent::ToolCall(tool_call));
                        }
                    }
                }

                message.content = content;
                transformed.push(Message::Assistant(Box::new(message)));
            }
        }
    }

    let mut result = Vec::with_capacity(transformed.len());
    let mut pending_tool_calls = Vec::<ToolCall>::new();
    let mut existing_tool_result_ids = BTreeSet::<ToolCallId>::new();

    for message in transformed {
        match message {
            Message::System(system) => result.push(Message::System(system)),
            Message::Assistant(assistant) => {
                flush_pending_tool_calls(
                    &mut result,
                    &mut pending_tool_calls,
                    &mut existing_tool_result_ids,
                    synthetic_result_timestamp,
                );
                if matches!(
                    assistant.stop_reason,
                    StopReason::Error | StopReason::Aborted
                ) {
                    continue;
                }
                let tool_calls: Vec<ToolCall> = assistant
                    .content
                    .iter()
                    .filter_map(|block| match block {
                        AssistantContent::ToolCall(tool_call) => Some(tool_call.clone()),
                        AssistantContent::Text(_) | AssistantContent::Thinking(_) => None,
                    })
                    .collect();
                if !tool_calls.is_empty() {
                    pending_tool_calls = tool_calls;
                    existing_tool_result_ids.clear();
                }
                result.push(Message::Assistant(assistant));
            }
            Message::ToolResult(tool_result) => {
                existing_tool_result_ids.insert(tool_result.tool_call_id.clone());
                result.push(Message::ToolResult(tool_result));
            }
            Message::User(user) => {
                flush_pending_tool_calls(
                    &mut result,
                    &mut pending_tool_calls,
                    &mut existing_tool_result_ids,
                    synthetic_result_timestamp,
                );
                result.push(Message::User(user));
            }
        }
    }

    flush_pending_tool_calls(
        &mut result,
        &mut pending_tool_calls,
        &mut existing_tool_result_ids,
        synthetic_result_timestamp,
    );
    Ok(result)
}

fn replace_images_with_placeholder(
    content: &[InputContent],
    placeholder: &str,
) -> Vec<InputContent> {
    let mut result = Vec::with_capacity(content.len());
    let mut previous_was_placeholder = false;

    for block in content {
        match block {
            InputContent::Image(_) => {
                if !previous_was_placeholder {
                    result.push(InputContent::Text(TextContent {
                        content_type: TextContentType::Text,
                        text: placeholder.to_owned(),
                        text_signature: None,
                    }));
                }
                previous_was_placeholder = true;
            }
            InputContent::Text(text) => {
                result.push(block.clone());
                previous_was_placeholder = text.text == placeholder;
            }
        }
    }

    result
}

fn flush_pending_tool_calls(
    result: &mut Vec<Message>,
    pending_tool_calls: &mut Vec<ToolCall>,
    existing_tool_result_ids: &mut BTreeSet<ToolCallId>,
    timestamp: SafeUnsigned,
) {
    if pending_tool_calls.is_empty() {
        return;
    }

    for tool_call in pending_tool_calls.drain(..) {
        if !existing_tool_result_ids.contains(&tool_call.id) {
            result.push(Message::ToolResult(Box::new(ToolResultMessage {
                role: ToolResultRole::ToolResult,
                tool_call_id: tool_call.id,
                tool_name: tool_call.name,
                content: vec![InputContent::Text(TextContent {
                    content_type: TextContentType::Text,
                    text: MISSING_TOOL_RESULT_TEXT.to_owned(),
                    text_signature: None,
                })],
                details: Presence::Missing,
                usage: None,
                nested_calls: None,
                duration_ms: None,
                is_error: true,
                timestamp,
            })));
        }
    }
    existing_tool_result_ids.clear();
}

fn is_ecmascript_blank(value: &str) -> bool {
    value.chars().all(|character| {
        matches!(
            character,
            '\u{0009}' | '\u{000b}' | '\u{000c}' | '\u{0020}' | '\u{00a0}' | '\u{1680}' | '\u{2000}'
                ..='\u{200a}'
                    | '\u{2028}'
                    | '\u{2029}'
                    | '\u{202f}'
                    | '\u{205f}'
                    | '\u{3000}'
                    | '\u{feff}'
                    | '\n'
                    | '\r'
        )
    })
}
