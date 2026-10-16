//! Normalized transcript and replay of anchored system and tool changes.

use crate::{
    Context, Message, SafeUnsigned, SystemContent, SystemMessage, SystemRole, Tool, ToolReference,
};

/// Provider-facing context. Only normalization produces this type.
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
#[non_exhaustive]
pub struct TranscriptContext {
    /// Ordered transcript messages.
    pub messages: Vec<Message>,
}
impl TranscriptContext {
    /// Ordered normalized messages.
    pub fn messages(&self) -> &[Message] {
        &self.messages
    }
    /// Takes ownership of the normalized transcript.
    pub fn into_messages(self) -> Vec<Message> {
        self.messages
    }
}

/// Builds a leading system message when prompt or tools are nonempty.
pub fn create_initial_system_message(
    prompt: Option<&str>,
    tools: Option<&[Tool]>,
) -> Option<SystemMessage> {
    let has_tools = tools.is_some_and(|tools| !tools.is_empty());
    if prompt.is_none_or(str::is_empty) && !has_tools {
        return None;
    }
    Some(SystemMessage {
        role: SystemRole::System,
        content: SystemContent::Text(prompt.unwrap_or_default().to_owned()),
        sections: None,
        tools_added: has_tools.then(|| tools.unwrap().to_vec()),
        tools_removed: None,
        timestamp: SafeUnsigned::new(0).expect("zero is safe"),
    })
}

/// Folds public shorthand into a leading system message.
pub fn normalize_context(context: Context) -> TranscriptContext {
    let initial =
        create_initial_system_message(context.system_prompt.as_deref(), context.tools.as_deref());
    let mut messages = context.messages;
    if let Some(initial) = initial {
        messages.insert(0, Message::System(initial));
    }
    TranscriptContext { messages }
}

/// Leading system message, if one is present.
pub fn get_initial_system_message(messages: &[Message]) -> Option<&SystemMessage> {
    match messages.first() {
        Some(Message::System(system)) => Some(system),
        _ => None,
    }
}

/// Text carried directly by a system message.
pub fn system_content_text(content: &SystemContent) -> String {
    match content {
        SystemContent::Text(text) => text.clone(),
        SystemContent::Blocks(blocks) => blocks
            .iter()
            .map(|block| block.text.as_str())
            .collect::<Vec<_>>()
            .join("\n"),
    }
}

/// Renders instruction text and named sections verbatim, separated by blank lines.
pub fn get_system_message_text(message: &SystemMessage) -> String {
    let mut parts = vec![system_content_text(&message.content)];
    if let Some(sections) = &message.sections {
        parts.extend(sections.iter().filter_map(|(_, value)| value.clone()));
    }
    parts
        .into_iter()
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("\n\n")
}

/// Resolves tools after every removal/addition, preserving declaration order.
pub fn get_current_tools(messages: &[Message]) -> Vec<Tool> {
    let mut tools: Vec<Tool> = Vec::new();
    for message in messages {
        let Message::System(system) = message else {
            continue;
        };
        for removed in system.tools_removed.iter().flatten() {
            tools.retain(|tool| tool.name != removed.name);
        }
        for added in system.tools_added.iter().flatten() {
            if let Some(existing) = tools.iter_mut().find(|tool| tool.name == added.name) {
                *existing = added.clone();
            } else {
                tools.push(added.clone());
            }
        }
    }
    tools
}

/// Replays instructions, named sections and tools into one system message.
pub fn get_current_system_message(messages: &[Message]) -> Option<SystemMessage> {
    let mut content = Vec::new();
    let mut sections: Vec<(String, Option<String>)> = Vec::new();
    let mut timestamp = None;
    for message in messages {
        let Message::System(system) = message else {
            continue;
        };
        timestamp.get_or_insert(system.timestamp);
        let text = system_content_text(&system.content);
        if !text.is_empty() {
            content.push(text);
        }
        for (name, value) in system.sections.iter().flat_map(|sections| sections.iter()) {
            if value.is_none() {
                sections.retain(|(key, _)| key != name);
            } else if let Some((_, existing)) = sections.iter_mut().find(|(key, _)| key == name) {
                *existing = value.clone();
            } else {
                sections.push((name.clone(), value.clone()));
            }
        }
    }
    let tools = get_current_tools(messages);
    let timestamp = timestamp?;
    Some(SystemMessage {
        role: SystemRole::System,
        content: SystemContent::Text(content.join("\n\n")),
        sections: (!sections.is_empty()).then(|| crate::SystemSections::new(sections)),
        tools_added: (!tools.is_empty()).then_some(tools),
        tools_removed: None,
        timestamp,
    })
}

/// Current rendered prompt after replaying all system deltas.
pub fn get_current_system_prompt(messages: &[Message]) -> String {
    get_current_system_message(messages)
        .as_ref()
        .map(get_system_message_text)
        .unwrap_or_default()
}

/// Collapses system changes for protocols without mid-conversation instructions.
pub fn collapse_system_messages(context: TranscriptContext) -> TranscriptContext {
    let head = get_current_system_message(&context.messages);
    let mut messages = context
        .messages
        .into_iter()
        .filter(|message| !matches!(message, Message::System(_)))
        .collect::<Vec<_>>();
    if let Some(head) = head {
        messages.insert(0, Message::System(head));
    }
    TranscriptContext { messages }
}

/// Keeps anchored system messages when supported, otherwise collapses them.
pub fn resolve_transcript(
    context: TranscriptContext,
    supports_mid_conversation: bool,
) -> TranscriptContext {
    if supports_mid_conversation {
        context
    } else {
        collapse_system_messages(context)
    }
}

/// Declarations added or removed between two tool states.
#[derive(Clone, Debug, PartialEq)]
pub struct ToolStateChanges {
    /// New or replaced declarations.
    pub tools_added: Vec<Tool>,
    /// Removed or replaced names.
    pub tools_removed: Vec<ToolReference>,
}
/// Compares model-facing declarations; replacement is removal followed by addition.
pub fn get_tool_state_changes(previous: &[Tool], current: &[Tool]) -> ToolStateChanges {
    ToolStateChanges {
        tools_added: current
            .iter()
            .filter(|tool| previous.iter().find(|old| old.name == tool.name) != Some(*tool))
            .cloned()
            .collect(),
        tools_removed: previous
            .iter()
            .filter(|tool| current.iter().find(|new| new.name == tool.name) != Some(*tool))
            .map(|tool| ToolReference {
                name: tool.name.clone(),
            })
            .collect(),
    }
}

/// Distinct declarations in first-declaration order, taking the latest definition.
pub fn get_declared_tools(messages: &[Message]) -> Vec<Tool> {
    let mut tools: Vec<Tool> = Vec::new();
    for message in messages {
        if let Message::System(system) = message {
            for tool in system.tools_added.iter().flatten() {
                if let Some(existing) = tools.iter_mut().find(|existing| existing.name == tool.name)
                {
                    *existing = tool.clone();
                } else {
                    tools.push(tool.clone());
                }
            }
        }
    }
    tools
}

/// Renders a later instruction delta, identifying section replacements/removals.
pub fn render_system_message_update(message: &SystemMessage) -> String {
    let mut parts = Vec::new();
    let text = system_content_text(&message.content);
    if !text.is_empty() {
        parts.push(text);
    }
    for (name, value) in message.sections.iter().flat_map(|sections| sections.iter()) {
        parts.push(match value {
            None => format!("Removed system prompt section \"{name}\"."),
            Some(value) => format!("Updated system prompt section \"{name}\":\n\n{value}"),
        });
    }
    parts.join("\n\n")
}

/// Tool changes a provider can represent at anchored message positions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TranscriptToolSupport {
    /// All tools appear at request level.
    None,
    /// Only additions can be anchored.
    Additions,
    /// Additions and redefinitions are anchored; removals are announced in text.
    Changes,
}
/// Split between request-level tools and later anchored declarations.
#[derive(Clone, Debug, PartialEq)]
pub struct TranscriptTools {
    /// Request-level definitions.
    pub request_tools: Vec<Tool>,
    /// Leading system messages whose tool state is already represented above.
    pub folded_system_messages: usize,
}
/// Splits declarations while folding through the last change the provider cannot express.
pub fn resolve_transcript_tools(
    messages: &[Message],
    support: TranscriptToolSupport,
) -> TranscriptTools {
    let systems = messages
        .iter()
        .filter_map(|message| {
            if let Message::System(system) = message {
                Some(system)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    if support == TranscriptToolSupport::None {
        return TranscriptTools {
            request_tools: get_current_tools(messages),
            folded_system_messages: systems.len(),
        };
    }
    if support == TranscriptToolSupport::Changes {
        return TranscriptTools {
            request_tools: Vec::new(),
            folded_system_messages: 0,
        };
    }
    let mut declared = std::collections::BTreeSet::new();
    let mut folded = usize::from(get_initial_system_message(messages).is_some());
    for (index, system) in systems.iter().enumerate() {
        if system
            .tools_removed
            .as_ref()
            .is_some_and(|tools| !tools.is_empty())
        {
            folded = index + 1;
        }
        for tool in system.tools_added.iter().flatten() {
            if !declared.insert(tool.name.clone()) {
                folded = index + 1;
            }
        }
    }
    let prefix = systems
        .into_iter()
        .take(folded)
        .map(|system| Message::System(system.clone()))
        .collect::<Vec<_>>();
    TranscriptTools {
        request_tools: get_current_tools(&prefix),
        folded_system_messages: folded,
    }
}

impl<'de> serde::Deserialize<'de> for TranscriptContext {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        <Context as serde::Deserialize>::deserialize(deserializer).map(normalize_context)
    }
}
