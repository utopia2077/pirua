//! Durable built-in and custom Agent message envelopes.

use std::collections::BTreeMap;
use std::fmt;

use pi_ai_core::{CustomRole, JsonObject, JsonValue, Message, StopReason};
use serde::de::{self, MapAccess, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::serde_support::from_json_value;

/// Error constructing a custom Agent message.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CustomAgentMessageError {
    /// Detached fields attempted to supply the structural role field.
    RoleField,
}

impl fmt::Display for CustomAgentMessageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("custom message fields must not contain role")
    }
}

impl std::error::Error for CustomAgentMessageError {}

/// Error validating a durable built-in Agent message.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DurableAgentMessageError {
    /// A durable assistant message still has the pending stop reason.
    PendingAssistant,
    /// Error and aborted assistant messages require an error message.
    MissingErrorMessage,
}

impl fmt::Display for DurableAgentMessageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PendingAssistant => {
                formatter.write_str("durable assistant messages cannot be pending")
            }
            Self::MissingErrorMessage => formatter
                .write_str("durable error and aborted assistant messages require errorMessage"),
        }
    }
}

impl std::error::Error for DurableAgentMessageError {}

/// A custom Agent message with an unwrapped open role and detached fields.
#[derive(Clone, Debug, PartialEq)]
pub struct CustomAgentMessage {
    role: CustomRole,
    fields: JsonObject,
}

impl CustomAgentMessage {
    /// Creates a custom message. A `role` field in `fields` is rejected.
    pub fn new(role: CustomRole, fields: JsonObject) -> Result<Self, CustomAgentMessageError> {
        if fields.as_map().contains_key("role") {
            return Err(CustomAgentMessageError::RoleField);
        }
        Ok(Self { role, fields })
    }

    /// Returns the registered custom role.
    pub const fn role(&self) -> &CustomRole {
        &self.role
    }

    /// Returns the detached custom fields excluding `role`.
    pub const fn fields(&self) -> &JsonObject {
        &self.fields
    }
}

impl Serialize for CustomAgentMessage {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.fields.as_map().len() + 1))?;
        map.serialize_entry("role", &self.role)?;
        for (key, value) in self.fields.as_map() {
            map.serialize_entry(key, value)?;
        }
        map.end()
    }
}

/// A settled durable Agent message.
///
/// Decoding inspects `role` before selecting the built-in or custom shape, so
/// an open custom role cannot be shadowed by an untagged built-in enum.
#[derive(Clone, Debug, PartialEq)]
pub enum AgentMessage {
    /// A built-in provider-neutral message.
    BuiltIn(Message),
    /// An application-defined message with a registered open role.
    Custom(CustomAgentMessage),
}

impl Serialize for AgentMessage {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::BuiltIn(message) => {
                validate_builtin(message).map_err(serde::ser::Error::custom)?;
                message.serialize(serializer)
            }
            Self::Custom(message) => message.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for AgentMessage {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct AgentMessageVisitor;

        impl<'de> Visitor<'de> for AgentMessageVisitor {
            type Value = AgentMessage;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("an Agent message object with a nonempty role")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut fields = BTreeMap::<String, JsonValue>::new();
                while let Some((key, value)) = map.next_entry()? {
                    if fields.insert(key, value).is_some() {
                        return Err(de::Error::custom("duplicate Agent message field"));
                    }
                }
                let role = match fields.get("role") {
                    Some(JsonValue::String(role)) => role.clone(),
                    Some(_) => {
                        return Err(de::Error::custom("Agent message role must be a string"));
                    }
                    None => return Err(de::Error::missing_field("role")),
                };

                if matches!(role.as_str(), "user" | "assistant" | "toolResult") {
                    let message: Message =
                        from_json_value(JsonValue::Object(JsonObject::new(fields)))?;
                    validate_builtin(&message).map_err(de::Error::custom)?;
                    return Ok(AgentMessage::BuiltIn(message));
                }

                fields.remove("role");
                let role = CustomRole::new(role).map_err(de::Error::custom)?;
                Ok(AgentMessage::Custom(CustomAgentMessage {
                    role,
                    fields: JsonObject::new(fields),
                }))
            }
        }

        deserializer.deserialize_map(AgentMessageVisitor)
    }
}

fn validate_builtin(message: &Message) -> Result<(), DurableAgentMessageError> {
    let Message::Assistant(assistant) = message else {
        return Ok(());
    };
    if assistant.stop_reason == StopReason::Pending {
        return Err(DurableAgentMessageError::PendingAssistant);
    }
    if matches!(
        assistant.stop_reason,
        StopReason::Error | StopReason::Aborted
    ) && assistant.error_message.is_none()
    {
        return Err(DurableAgentMessageError::MissingErrorMessage);
    }
    Ok(())
}
