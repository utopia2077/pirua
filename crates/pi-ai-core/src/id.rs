//! Validated open string identities and known upstream constants.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

/// Error returned when an open identity is empty or otherwise reserved.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IdError {
    message: &'static str,
}

impl IdError {
    const fn empty() -> Self {
        Self {
            message: "identity must be non-empty",
        }
    }

    const fn built_in_role() -> Self {
        Self {
            message: "custom role cannot shadow a built-in role",
        }
    }
}

impl fmt::Display for IdError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message)
    }
}

impl std::error::Error for IdError {}

macro_rules! open_id {
    ($(#[$meta:meta])* $name:ident, $description:literal) => {
        $(#[$meta])*
        #[doc = $description]
        #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(String);

        impl $name {
            /// Creates an identity after validating that it is not empty.
            pub fn new(value: impl Into<String>) -> Result<Self, IdError> {
                let value = value.into();
                if value.is_empty() {
                    Err(IdError::empty())
                } else {
                    Ok(Self(value))
                }
            }

            /// Returns the wire string.
            pub fn as_str(&self) -> &str {
                &self.0
            }

            /// Consumes the identity and returns the wire string.
            pub fn into_string(self) -> String {
                self.0
            }
        }

        impl TryFrom<String> for $name {
            type Error = IdError;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl FromStr for $name {
            type Err = IdError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::new(value)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.serialize_str(&self.0)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                Self::new(String::deserialize(deserializer)?).map_err(de::Error::custom)
            }
        }
    };
}

open_id!(ApiId, "An open provider API protocol identity.");
open_id!(ProviderId, "An open text-model provider identity.");
open_id!(ModelId, "An open provider-scoped model identity.");
open_id!(ToolName, "An open tool name.");
open_id!(ToolCallId, "An open tool-call identity.");
open_id!(DiagnosticType, "An open diagnostic category.");
open_id!(RawStopReason, "An open provider-native stop reason.");
open_id!(
    DeferredId,
    "An open provider token for a deferred response."
);
open_id!(ResponseId, "An open provider response identity.");

/// An application-defined message role that cannot shadow a built-in role.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CustomRole(String);

impl CustomRole {
    /// Creates a validated custom role.
    pub fn new(value: impl Into<String>) -> Result<Self, IdError> {
        let value = value.into();
        if value.is_empty() {
            return Err(IdError::empty());
        }
        if matches!(value.as_str(), "user" | "assistant" | "toolResult") {
            return Err(IdError::built_in_role());
        }
        Ok(Self(value))
    }

    /// Returns the wire role string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Serialize for CustomRole {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for CustomRole {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(de::Error::custom)
    }
}

impl ApiId {
    /// Selected protocol implementations.
    pub const KNOWN: &[&str] = &[
        Self::OPENAI_COMPLETIONS,
        Self::OPENAI_RESPONSES,
        Self::ANTHROPIC_MESSAGES,
        Self::GOOGLE_GENERATIVE_AI,
    ];
    /// OpenAI-compatible Chat Completions API.
    pub const OPENAI_COMPLETIONS: &str = "openai-completions";
    /// OpenAI Responses API.
    pub const OPENAI_RESPONSES: &str = "openai-responses";
    /// Anthropic Messages API.
    pub const ANTHROPIC_MESSAGES: &str = "anthropic-messages";
    /// Google Generative AI API.
    pub const GOOGLE_GENERATIVE_AI: &str = "google-generative-ai";
}
impl ProviderId {
    /// Selected builtin providers; custom identities remain open.
    pub const KNOWN: &[&str] = &[Self::OPENAI, Self::ANTHROPIC, Self::GOOGLE, Self::DEEPSEEK];
    /// OpenAI.
    pub const OPENAI: &str = "openai";
    /// Anthropic.
    pub const ANTHROPIC: &str = "anthropic";
    /// Google.
    pub const GOOGLE: &str = "google";
    /// DeepSeek.
    pub const DEEPSEEK: &str = "deepseek";
}

#[cfg(test)]
mod tests {
    use super::{ApiId, CustomRole};

    #[test]
    fn open_ids_accept_future_values_but_reject_empty_values() {
        assert_eq!(
            ApiId::new("future-api").map(|id| id.into_string()),
            Ok("future-api".to_owned())
        );
        assert!(ApiId::new("").is_err());
        assert_eq!(
            ApiId::new("   ").map(|id| id.into_string()),
            Ok("   ".to_owned())
        );
    }

    #[test]
    fn custom_roles_cannot_shadow_builtin_roles() {
        assert!(CustomRole::new("artifact").is_ok());
        assert!(CustomRole::new("user").is_err());
        assert!(CustomRole::new("assistant").is_err());
        assert!(CustomRole::new("toolResult").is_err());
    }
}
