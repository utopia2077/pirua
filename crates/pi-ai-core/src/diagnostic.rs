//! Redacted assistant-message diagnostic data.

use serde::{Deserialize, Serialize};

use crate::id::DiagnosticType;
use crate::json::{JsonNumber, JsonObject, SafeUnsigned};

/// A diagnostic error code represented by either a string or a safe JSON number.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DiagnosticCode {
    /// A string error code.
    String(String),
    /// A numeric error code.
    Number(JsonNumber),
}

/// Redacted information extracted from an error.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DiagnosticErrorInfo {
    /// Optional runtime error name.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub name: Option<String>,
    /// Human-readable error message.
    pub message: String,
    /// Optional redacted stack trace.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub stack: Option<String>,
    /// Optional provider or runtime error code.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub code: Option<DiagnosticCode>,
}

/// A diagnostic attached to an assistant message.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AssistantMessageDiagnostic {
    /// Open diagnostic category.
    #[serde(rename = "type")]
    pub diagnostic_type: DiagnosticType,
    /// Unix timestamp in milliseconds.
    pub timestamp: SafeUnsigned,
    /// Optional redacted error information.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub error: Option<DiagnosticErrorInfo>,
    /// Optional object-only diagnostic details.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub details: Option<JsonObject>,
}
