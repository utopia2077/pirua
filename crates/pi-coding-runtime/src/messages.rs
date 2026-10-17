//! Coding transcript admission and conversion to provider messages.
use pi_agent::{AgentError, AgentMessage};
use pi_ai::{Message, SafeUnsigned, UserContent, UserMessage};
use serde_json::{Value, json};

pub const COMPACTION_SUMMARY_PREFIX: &str = "The conversation history before this point was compacted into the following summary:\n\n<summary>\n";
pub const COMPACTION_SUMMARY_SUFFIX: &str = "\n</summary>";
pub const BRANCH_SUMMARY_PREFIX: &str =
    "The following is a summary of a branch that this conversation came back from:\n\n<summary>\n";
pub const BRANCH_SUMMARY_SUFFIX: &str = "</summary>";

pub fn content_text(content: &Value, separator: &str) -> String {
    if let Some(text) = content.as_str() {
        return text.to_owned();
    }
    content
        .as_array()
        .map(|blocks| {
            blocks
                .iter()
                .filter(|b| b["type"] == "text")
                .filter_map(|b| b["text"].as_str())
                .collect::<Vec<_>>()
                .join(separator)
        })
        .unwrap_or_default()
}
pub fn bash_execution_to_text(message: &Value) -> String {
    let mut text = format!(
        "Ran `{}`\n",
        message["command"].as_str().unwrap_or_default()
    );
    let output = message["output"].as_str().unwrap_or_default();
    if output.is_empty() {
        text.push_str("(no output)");
    } else {
        text.push_str(&format!("```\n{output}\n```"));
    }
    if message["cancelled"] == true {
        text.push_str("\n\n(command cancelled)");
    } else if let Some(code) = message["exitCode"].as_i64().filter(|c| *c != 0) {
        text.push_str(&format!("\n\nCommand exited with code {code}"));
    }
    if message["truncated"] == true
        && let Some(path) = message["fullOutputPath"].as_str()
    {
        text.push_str(&format!("\n\n[Output truncated. Full output: {path}]"));
    }
    text
}
pub fn convert_to_llm(messages: Vec<AgentMessage>) -> Result<Vec<Message>, AgentError> {
    messages.into_iter().filter_map(|message| {
        if let AgentMessage::BuiltIn(message) = message { return Some(Ok(message)); }
        let value = match serde_json::to_value(&message) { Ok(v) => v, Err(e) => return Some(Err(AgentError::InvalidContext(e.to_string()))) };
        let content = match message.role() {
            "bashExecution" if value["excludeFromContext"] == true => return None,
            "bashExecution" => json!([{"type":"text","text":bash_execution_to_text(&value)}]),
            "custom" => if value["content"].is_string() { json!([{"type":"text","text":value["content"]}]) } else { value["content"].clone() },
            "branchSummary" => json!([{"type":"text","text":format!("{BRANCH_SUMMARY_PREFIX}{}{BRANCH_SUMMARY_SUFFIX}",value["summary"].as_str().unwrap_or_default())}]),
            "compactionSummary" => json!([{"type":"text","text":format!("{COMPACTION_SUMMARY_PREFIX}{}{COMPACTION_SUMMARY_SUFFIX}",value["summary"].as_str().unwrap_or_default())}]),
            _ => return None,
        };
        Some(serde_json::from_value(json!({"role":"user","content":content,"timestamp":value["timestamp"]})).map_err(|e| AgentError::InvalidContext(e.to_string())))
    }).collect()
}
pub fn user_message(text: impl Into<String>, images: Vec<pi_ai::ImageContent>) -> AgentMessage {
    let mut content = vec![pi_ai::InputContent::Text(pi_ai::TextContent {
        content_type: Default::default(),
        text: text.into(),
        text_signature: None,
    })];
    content.extend(images.into_iter().map(pi_ai::InputContent::Image));
    AgentMessage::BuiltIn(Message::User(UserMessage {
        role: Default::default(),
        content: UserContent::Blocks(content),
        timestamp: SafeUnsigned::new(now()).expect("bounded clock"),
    }))
}
pub(crate) fn now() -> u64 {
    u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
    .min(pi_ai::JS_SAFE_INTEGER_MAX)
}
pub(crate) fn decode(value: Value) -> Result<AgentMessage, AgentError> {
    let invalid = || AgentError::InvalidContext("invalid saved Coding message".into());
    match value["role"].as_str() {
        Some("custom") => {
            if value["customType"].as_str().is_none_or(str::is_empty)
                || !value["display"].is_boolean()
            {
                return Err(invalid());
            }
            if !value["content"].is_string() {
                serde_json::from_value::<Vec<pi_ai::InputContent>>(value["content"].clone())
                    .map_err(|_| invalid())?;
            }
            serde_json::from_value::<SafeUnsigned>(value["timestamp"].clone())
                .map_err(|_| invalid())?;
        }
        Some("bashExecution") => {
            if !value["command"].is_string()
                || !value["output"].is_string()
                || !value["cancelled"].is_boolean()
                || !value["truncated"].is_boolean()
                || value
                    .get("exitCode")
                    .is_some_and(|code| code.as_i64().is_none())
                || value
                    .get("fullOutputPath")
                    .is_some_and(|path| !path.is_string())
                || value
                    .get("excludeFromContext")
                    .is_some_and(|excluded| !excluded.is_boolean())
            {
                return Err(invalid());
            }
        }
        Some("branchSummary" | "compactionSummary") => {
            if !value["summary"].is_string() {
                return Err(invalid());
            }
            if value["role"] == "branchSummary" {
                if !value["fromId"].is_null() && !value["fromId"].is_string() {
                    return Err(invalid());
                }
            } else {
                serde_json::from_value::<SafeUnsigned>(value["tokensBefore"].clone())
                    .map_err(|_| invalid())?;
            }
        }
        Some("system" | "user" | "assistant" | "toolResult") => {}
        _ => return Err(invalid()),
    }
    if matches!(
        value["role"].as_str(),
        Some("bashExecution" | "branchSummary" | "compactionSummary")
    ) {
        serde_json::from_value::<SafeUnsigned>(value["timestamp"].clone())
            .map_err(|_| invalid())?;
    }
    serde_json::from_value(value)
        .map_err(|e| AgentError::InvalidContext(format!("invalid saved Coding message: {e}")))
}

pub(crate) fn validate(message: AgentMessage) -> Result<AgentMessage, AgentError> {
    decode(
        serde_json::to_value(message)
            .map_err(|error| AgentError::InvalidContext(error.to_string()))?,
    )
}
