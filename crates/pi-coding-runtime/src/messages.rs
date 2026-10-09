//! Coding-message projection over existing Agent and AI contracts.
//!
//! Policy is translated from pi-mono's coding-agent/core/messages.ts at
//! `853a80d26c90a14c1886f0ebb8ffaae133ca2185`. The existing Session codec owns
//! the four coding-role schemas; this module does not define another registry.

use pi_agent::schema::{
    AgentMessage, BoxFuture, CustomAgentMessage, ErrorContext, HarnessFault, HarnessRuntimeError,
    ProviderHeaderPolicy, ProviderMessageConverter,
};
use pi_agent_session::{codec::SessionCodec, legacy_v3::coding_message_options};
use pi_ai::{InputContent, Message, SafeUnsigned, TextContent, UserContent, UserMessage};
use serde::de::DeserializeOwned;

/// Exact prefix applied to a compaction summary.
pub const COMPACTION_SUMMARY_PREFIX: &str = "The conversation history before this point was compacted into the following summary:\n\n<summary>\n";
/// Exact suffix applied to a compaction summary, including the leading newline.
pub const COMPACTION_SUMMARY_SUFFIX: &str = "\n</summary>";
/// Exact prefix applied to a branch summary.
pub const BRANCH_SUMMARY_PREFIX: &str =
    "The following is a summary of a branch that this conversation came back from:\n\n<summary>\n";
/// Exact suffix applied to a branch summary.
pub const BRANCH_SUMMARY_SUFFIX: &str = "</summary>";

/// Source-compatible provider-message converter for the coding message roles.
///
/// Built-ins retain their complete content and metadata. Unknown custom roles
/// are omitted, as in the source converter. Known coding messages are validated
/// by the existing offline Session codec before projection, including excluded
/// bash executions.
pub struct CodingMessageConverter {
    codec: SessionCodec,
}

impl Default for CodingMessageConverter {
    fn default() -> Self {
        Self::new().expect("static coding message schemas compile")
    }
}

impl CodingMessageConverter {
    /// Compiles the existing four coding-role schemas without external lookup.
    pub fn new() -> Result<Self, HarnessRuntimeError> {
        let codec = SessionCodec::new(coding_message_options(), ProviderHeaderPolicy::default())
            .map_err(|_| invalid_message())?;
        Ok(Self { codec })
    }

    /// Projects one Agent message into zero or one provider-neutral message.
    pub fn project(&self, message: AgentMessage) -> Result<Option<Message>, HarnessRuntimeError> {
        if let AgentMessage::Custom(custom) = &message
            && !matches!(
                custom.role().as_str(),
                "bashExecution" | "custom" | "branchSummary" | "compactionSummary"
            )
        {
            return Ok(None);
        }
        self.codec
            .validate_message(&message)
            .map_err(|_| invalid_message())?;
        let custom = match message {
            AgentMessage::BuiltIn(message) => return Ok(Some(message)),
            AgentMessage::Custom(custom) => custom,
        };
        let content = match custom.role().as_str() {
            "bashExecution" => {
                if optional_field::<bool>(&custom, "excludeFromContext")? == Some(true) {
                    return Ok(None);
                }
                UserContent::Blocks(vec![text_block(bash_execution_to_text(&custom)?)])
            }
            "custom" => match field::<UserContent>(&custom, "content")? {
                UserContent::Text(text) => UserContent::Blocks(vec![text_block(text)]),
                blocks @ UserContent::Blocks(_) => blocks,
            },
            "branchSummary" => UserContent::Blocks(vec![text_block(format!(
                "{BRANCH_SUMMARY_PREFIX}{}{BRANCH_SUMMARY_SUFFIX}",
                field::<String>(&custom, "summary")?
            ))]),
            "compactionSummary" => UserContent::Blocks(vec![text_block(format!(
                "{COMPACTION_SUMMARY_PREFIX}{}{COMPACTION_SUMMARY_SUFFIX}",
                field::<String>(&custom, "summary")?
            ))]),
            _ => return Ok(None),
        };
        Ok(Some(Message::User(UserMessage {
            role: Default::default(),
            content,
            timestamp: field::<SafeUnsigned>(&custom, "timestamp")?,
        })))
    }

    /// Converts ordered detached messages using the same public projector.
    pub fn convert_messages(
        &self,
        messages: Vec<AgentMessage>,
    ) -> Result<Vec<Message>, HarnessRuntimeError> {
        messages
            .into_iter()
            .filter_map(|message| self.project(message).transpose())
            .collect()
    }
}

impl ProviderMessageConverter for CodingMessageConverter {
    fn convert(
        &self,
        messages: Vec<AgentMessage>,
    ) -> BoxFuture<'_, Result<Vec<Message>, HarnessRuntimeError>> {
        Box::pin(async move { self.convert_messages(messages) })
    }
}

fn bash_execution_to_text(custom: &CustomAgentMessage) -> Result<String, HarnessRuntimeError> {
    let command: String = field(custom, "command")?;
    let output: String = field(custom, "output")?;
    let mut text = format!("Ran `{command}`\n");
    if output.is_empty() {
        text.push_str("(no output)");
    } else {
        text.push_str(&format!("```\n{output}\n```"));
    }
    if field::<bool>(custom, "cancelled")? {
        text.push_str("\n\n(command cancelled)");
    } else if let Some(exit_code) = optional_field::<i64>(custom, "exitCode")?
        && exit_code != 0
    {
        text.push_str(&format!("\n\nCommand exited with code {exit_code}"));
    }
    if field::<bool>(custom, "truncated")?
        && let Some(path) = optional_field::<String>(custom, "fullOutputPath")?
        && !path.is_empty()
    {
        text.push_str(&format!("\n\n[Output truncated. Full output: {path}]"));
    }
    Ok(text)
}

fn text_block(text: String) -> InputContent {
    InputContent::Text(TextContent {
        content_type: Default::default(),
        text,
        text_signature: None,
    })
}

fn field<T: DeserializeOwned>(
    custom: &CustomAgentMessage,
    name: &str,
) -> Result<T, HarnessRuntimeError> {
    let value = custom
        .fields()
        .as_map()
        .get(name)
        .ok_or_else(invalid_message)?;
    let value = serde_json::to_value(value).map_err(|_| invalid_message())?;
    serde_json::from_value(value).map_err(|_| invalid_message())
}

fn optional_field<T: DeserializeOwned>(
    custom: &CustomAgentMessage,
    name: &str,
) -> Result<Option<T>, HarnessRuntimeError> {
    if custom.fields().as_map().contains_key(name) {
        field(custom, name).map(Some)
    } else {
        Ok(None)
    }
}

fn invalid_message() -> HarnessRuntimeError {
    HarnessRuntimeError::Fault(HarnessFault::new(
        ErrorContext::new("coding message schema validation failed")
            .expect("static bounded context"),
    ))
}
