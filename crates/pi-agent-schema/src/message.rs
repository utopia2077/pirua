use std::collections::BTreeMap;

use pi_ai_core::{Message, ModelThinkingLevel};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

/// Builtin messages retain typed AI validation. Custom messages have open roles.
/// Assistant drafts are valid events; persistence owners choose their admission rule.
#[derive(Clone, Debug, PartialEq)]
pub enum AgentMessage {
    BuiltIn(Message),
    Custom(CustomMessage),
}

#[derive(Clone, Debug, PartialEq)]
pub struct CustomMessage {
    role: String,
    fields: BTreeMap<String, Value>,
}
impl CustomMessage {
    pub fn new(
        role: impl Into<String>,
        fields: BTreeMap<String, Value>,
    ) -> Result<Self, crate::AgentError> {
        let role = role.into();
        if role.is_empty()
            || matches!(
                role.as_str(),
                "system" | "user" | "assistant" | "toolResult"
            )
            || fields.contains_key("role")
        {
            return Err(crate::AgentError::InvalidContext(
                "invalid custom message role or fields".into(),
            ));
        }
        Ok(Self { role, fields })
    }
    pub fn role(&self) -> &str {
        &self.role
    }
    pub fn fields(&self) -> &BTreeMap<String, Value> {
        &self.fields
    }
}
impl AgentMessage {
    pub fn role(&self) -> &str {
        match self {
            Self::BuiltIn(Message::System(_)) => "system",
            Self::BuiltIn(Message::User(_)) => "user",
            Self::BuiltIn(Message::Assistant(_)) => "assistant",
            Self::BuiltIn(Message::ToolResult(_)) => "toolResult",
            Self::Custom(message) => message.role(),
        }
    }
    pub fn as_builtin(&self) -> Option<&Message> {
        match self {
            Self::BuiltIn(message) => Some(message),
            Self::Custom(_) => None,
        }
    }
}
impl From<Message> for AgentMessage {
    fn from(message: Message) -> Self {
        Self::BuiltIn(message)
    }
}
impl Serialize for AgentMessage {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        match self {
            Self::BuiltIn(message) => message.serialize(serializer),
            Self::Custom(message) => {
                let mut map = serializer.serialize_map(Some(message.fields.len() + 1))?;
                map.serialize_entry("role", &message.role)?;
                for (key, value) in &message.fields {
                    map.serialize_entry(key, value)?;
                }
                map.end()
            }
        }
    }
}
impl<'de> Deserialize<'de> for AgentMessage {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::Error;
        let mut fields = BTreeMap::<String, Value>::deserialize(deserializer)?;
        let role = fields
            .get("role")
            .and_then(Value::as_str)
            .ok_or_else(|| D::Error::custom("message role must be a string"))?
            .to_owned();
        if matches!(
            role.as_str(),
            "system" | "user" | "assistant" | "toolResult"
        ) {
            return serde_json::from_value(Value::Object(fields.into_iter().collect()))
                .map(Self::BuiltIn)
                .map_err(D::Error::custom);
        }
        fields.remove("role");
        CustomMessage::new(role, fields)
            .map(Self::Custom)
            .map_err(D::Error::custom)
    }
}

/// Agent includes off while provider reasoning options encode off as absent.
pub type ThinkingLevel = ModelThinkingLevel;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum QueueMode {
    All,
    #[default]
    OneAtATime,
}

/// Explicit native equivalent of a runtime mutating a message during an awaited
/// start/end listener. Replacements retain the original role.
pub type MessageTransform = std::sync::Arc<
    dyn Fn(
            AgentMessage,
            crate::CancellationToken,
        ) -> crate::BoxFuture<'static, Result<AgentMessage, crate::AgentError>>
        + Send
        + Sync,
>;

#[derive(Clone, Default)]
pub struct MessageHooks {
    pub start: Option<MessageTransform>,
    pub end: Option<MessageTransform>,
}

impl MessageHooks {
    pub async fn start(
        &self,
        message: AgentMessage,
        token: crate::CancellationToken,
    ) -> Result<AgentMessage, crate::AgentError> {
        Self::apply(&self.start, message, token).await
    }
    pub async fn end(
        &self,
        message: AgentMessage,
        token: crate::CancellationToken,
    ) -> Result<AgentMessage, crate::AgentError> {
        Self::apply(&self.end, message, token).await
    }
    async fn apply(
        hook: &Option<MessageTransform>,
        message: AgentMessage,
        token: crate::CancellationToken,
    ) -> Result<AgentMessage, crate::AgentError> {
        let role = message.role().to_owned();
        let replacement = if let Some(hook) = hook {
            hook(message, token).await?
        } else {
            message
        };
        if replacement.role() != role {
            return Err(crate::AgentError::InvalidContext(
                "Message replacement must retain its role".into(),
            ));
        }
        Ok(replacement)
    }
}
