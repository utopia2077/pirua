//! Serializable provider-neutral options and transient request maps.

use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

use crate::json::{FiniteF64, JS_SAFE_INTEGER_MAX, JsonObject, NumericError, SafeUnsigned};
use crate::model::ThinkingLevel;

/// Preferred provider transport.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Transport {
    /// Server-sent events.
    #[serde(rename = "sse")]
    Sse,
    /// A fresh WebSocket connection.
    #[serde(rename = "websocket")]
    WebSocket,
    /// A cached WebSocket connection.
    #[serde(rename = "websocket-cached")]
    WebSocketCached,
    /// Provider-selected transport.
    #[serde(rename = "auto")]
    Auto,
}

/// Prompt-cache retention preference.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum CacheRetention {
    /// Disable prompt caching where possible.
    #[serde(rename = "none")]
    None,
    /// Short provider-default retention.
    #[serde(rename = "short")]
    Short,
    /// Long provider-specific retention.
    #[serde(rename = "long")]
    Long,
}

/// Provider-neutral tool-selection preference.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ToolChoice {
    /// Let the provider choose whether to call a tool.
    #[serde(rename = "auto")]
    Auto,
    /// Disable tool calls.
    #[serde(rename = "none")]
    None,
}

/// Provider deferred-response window.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum DeferredWindow {
    /// Fifteen minutes.
    #[serde(rename = "15m")]
    FifteenMinutes,
    /// One hour.
    #[serde(rename = "1h")]
    OneHour,
    /// Twenty-four hours.
    #[serde(rename = "24h")]
    TwentyFourHours,
}

/// Object form of a deferred-response request.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeferredOptions {
    /// Optional requested deferred window.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub window: Option<DeferredWindow>,
}

/// Boolean or object form of a deferred-response request.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DeferredRequest {
    /// Boolean enable or disable value.
    Enabled(bool),
    /// Window-bearing object form.
    Options(DeferredOptions),
}

/// Token budgets for provider-neutral thinking levels.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ThinkingBudgets {
    /// Minimal-thinking token budget.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub minimal: Option<SafeUnsigned>,
    /// Low-thinking token budget.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub low: Option<SafeUnsigned>,
    /// Medium-thinking token budget.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub medium: Option<SafeUnsigned>,
    /// High-thinking token budget.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub high: Option<SafeUnsigned>,
}

/// Raw provider headers, including explicit null values that suppress defaults.
///
/// # Durability and secrecy
///
/// This map is transient request data. Header values can contain credentials,
/// so it must never enter durable Harness state. Durable public headers require
/// a separate host-policy-classified wrapper whose default policy denies every
/// header.
#[derive(Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ProviderHeaders(
    #[serde(deserialize_with = "crate::json::deserialize_btree_map_reject_duplicates")]
    BTreeMap<String, Option<String>>,
);

impl ProviderHeaders {
    /// Creates a raw transient header map.
    pub fn new(headers: BTreeMap<String, Option<String>>) -> Self {
        Self(headers)
    }

    /// Returns the raw headers for transient dispatch.
    pub const fn as_map(&self) -> &BTreeMap<String, Option<String>> {
        &self.0
    }
}

impl fmt::Debug for ProviderHeaders {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let keys: Vec<&str> = self.0.keys().map(String::as_str).collect();
        formatter
            .debug_struct("ProviderHeaders")
            .field("keys", &keys)
            .finish()
    }
}

/// Provider-scoped environment overrides.
///
/// Values are transient configuration and may contain secrets. Debug output
/// therefore reports keys only.
#[derive(Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ProviderEnv(
    #[serde(deserialize_with = "crate::json::deserialize_btree_map_reject_duplicates")]
    BTreeMap<String, String>,
);

impl ProviderEnv {
    /// Creates provider-scoped environment overrides.
    pub fn new(values: BTreeMap<String, String>) -> Self {
        Self(values)
    }

    /// Returns the transient values.
    pub const fn as_map(&self) -> &BTreeMap<String, String> {
        &self.0
    }
}

impl fmt::Debug for ProviderEnv {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let keys: Vec<&str> = self.0.keys().map(String::as_str).collect();
        formatter
            .debug_struct("ProviderEnv")
            .field("keys", &keys)
            .finish()
    }
}

/// Serializable provider-neutral stream options.
///
/// This shape excludes cancellation, telemetry, HTTP clients, callbacks,
/// credentials, environment values, and raw headers, making it suitable for a
/// durable boundary once the owning surface applies its own public-data policy.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StreamOptionsWire {
    /// Provider request timeout in milliseconds.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub timeout_ms: Option<SafeUnsigned>,
    /// Maximum provider-client retry attempts.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub max_retries: Option<SafeUnsigned>,
    /// Maximum provider-requested retry delay in milliseconds.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub max_retry_delay_ms: Option<SafeUnsigned>,
    /// Sampling temperature.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub temperature: Option<FiniteF64>,
    /// Arbitrary JSON sampling parameters.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub sampling_params: Option<JsonObject>,
    /// Maximum output tokens.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub max_tokens: Option<SafeUnsigned>,
    /// Preferred transport.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub transport: Option<Transport>,
    /// Prompt-cache retention preference.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub cache_retention: Option<CacheRetention>,
    /// Provider session identity.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub session_id: Option<String>,
    /// WebSocket connection timeout in milliseconds.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub websocket_connect_timeout_ms: Option<SafeUnsigned>,
    /// Provider-readable detached metadata.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub metadata: Option<JsonObject>,
    /// Provider-neutral tool selection.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub tool_choice: Option<ToolChoice>,
    /// Requested thinking level.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub reasoning: Option<ThinkingLevel>,
    /// Optional deferred-response request.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub deferred: Option<DeferredRequest>,
    /// Custom thinking budgets.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub thinking_budgets: Option<ThinkingBudgets>,
}

/// Serializable image-generation options.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ImagesOptionsWire {
    /// Provider-readable detached metadata.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub metadata: Option<JsonObject>,
}

/// Retry count whose normalized `maxRetries + 1` attempt count remains safe.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct MaxRetries(SafeUnsigned);

impl MaxRetries {
    /// Largest accepted retry count.
    pub const MAX_VALUE: u64 = JS_SAFE_INTEGER_MAX - 1;

    /// Creates a retry count that can safely be incremented during normalization.
    pub fn new(value: u64) -> Result<Self, NumericError> {
        let value = SafeUnsigned::new(value)?;
        if value.get() <= Self::MAX_VALUE {
            Ok(Self(value))
        } else {
            Err(NumericError::retry_count_too_large())
        }
    }

    /// Returns the public `maxRetries` value.
    pub const fn get(self) -> u64 {
        self.0.get()
    }

    /// Returns normalized `maxAttempts = maxRetries + 1` without overflow.
    pub const fn max_attempts(self) -> u64 {
        self.get() + 1
    }
}

impl Serialize for MaxRetries {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(self.get())
    }
}

impl<'de> Deserialize<'de> for MaxRetries {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = SafeUnsigned::deserialize(deserializer)?;
        Self::new(value.get()).map_err(de::Error::custom)
    }
}

/// Bounded retry policy shared with the Agent and Harness schemas.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RetryPolicy {
    /// Whether retries are enabled.
    pub enabled: bool,
    /// Maximum retry attempts after the initial call.
    pub max_retries: MaxRetries,
    /// Base exponential-backoff delay in milliseconds.
    pub base_delay_ms: SafeUnsigned,
}
