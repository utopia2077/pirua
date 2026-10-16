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
#[derive(Clone, Default, Eq, PartialEq)]
pub struct ProviderHeaders {
    values: BTreeMap<String, Option<String>>,
    order: Vec<String>,
}

impl ProviderHeaders {
    /// Creates a raw transient header map from its existing deterministic order.
    /// A BTreeMap cannot recover insertion history that was already discarded.
    pub fn new(headers: BTreeMap<String, Option<String>>) -> Self {
        let mut result = Self {
            order: headers.keys().cloned().collect(),
            values: headers,
        };
        result.sort_numeric_keys();
        result
    }

    /// Returns exact-key lookup data. Header-sensitive dispatch uses the ordered
    /// projection instead of this map's lexical iteration order.
    pub const fn as_map(&self) -> &BTreeMap<String, Option<String>> {
        &self.values
    }

    /// Borrows exact header names and nullable values in JavaScript own-key
    /// order: array-index keys first, then other keys in insertion order.
    pub fn iter_for_dispatch(&self) -> impl Iterator<Item = (&String, &Option<String>)> {
        self.order.iter().map(|name| {
            (
                name,
                self.values
                    .get(name)
                    .expect("private order index names existing keys"),
            )
        })
    }

    fn sort_numeric_keys(&mut self) {
        self.order.sort_by_key(|name| {
            name.parse::<u32>()
                .ok()
                .filter(|index| *index != u32::MAX && index.to_string() == *name)
                .map_or((1, 0), |index| (0, index))
        });
    }

    // Registry composition owns these entries. Exact replacement keeps its
    // first own-key slot; raw object decoding below separately rejects duplicates.
    pub(crate) fn from_ordered_entries(entries: Vec<(String, Option<String>)>) -> Self {
        let mut result = Self::default();
        for (name, value) in entries {
            if !result.values.contains_key(&name) {
                result.order.push(name.clone());
            }
            result.values.insert(name, value);
        }
        result.sort_numeric_keys();
        result
    }
}

impl Serialize for ProviderHeaders {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(self.values.len()))?;
        for (name, value) in self.iter_for_dispatch() {
            map.serialize_entry(name, value)?;
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for ProviderHeaders {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct HeaderVisitor;
        impl<'de> de::Visitor<'de> for HeaderVisitor {
            type Value = ProviderHeaders;
            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a map without duplicate keys")
            }
            fn visit_map<A: de::MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut result = ProviderHeaders::default();
                while let Some((name, value)) = map.next_entry::<String, Option<String>>()? {
                    if result.values.insert(name.clone(), value).is_some() {
                        return Err(de::Error::custom("duplicate map key"));
                    }
                    result.order.push(name);
                }
                result.sort_numeric_keys();
                Ok(result)
            }
        }
        deserializer.deserialize_map(HeaderVisitor)
    }
}

impl fmt::Debug for ProviderHeaders {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let keys: Vec<&str> = self.values.keys().map(String::as_str).collect();
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
    /// Explicit calling-application identity.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub identity: Option<crate::ClientIdentity>,
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
}

/// Serializable provider-neutral simple-stream options.
///
/// The common raw-stream fields serialize at the same object level as the four
/// fields that exist only on simple requests.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SimpleStreamOptionsWire {
    /// Common raw-stream options.
    #[serde(flatten)]
    pub stream: StreamOptionsWire,
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

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SimpleStreamOptionsWireDef {
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null"
    )]
    identity: Option<crate::ClientIdentity>,
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null"
    )]
    timeout_ms: Option<SafeUnsigned>,
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null"
    )]
    max_retries: Option<SafeUnsigned>,
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null"
    )]
    max_retry_delay_ms: Option<SafeUnsigned>,
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null"
    )]
    temperature: Option<FiniteF64>,
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null"
    )]
    sampling_params: Option<JsonObject>,
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null"
    )]
    max_tokens: Option<SafeUnsigned>,
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null"
    )]
    transport: Option<Transport>,
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null"
    )]
    cache_retention: Option<CacheRetention>,
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null"
    )]
    session_id: Option<String>,
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null"
    )]
    websocket_connect_timeout_ms: Option<SafeUnsigned>,
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null"
    )]
    metadata: Option<JsonObject>,
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null"
    )]
    tool_choice: Option<ToolChoice>,
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null"
    )]
    reasoning: Option<ThinkingLevel>,
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null"
    )]
    deferred: Option<DeferredRequest>,
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null"
    )]
    thinking_budgets: Option<ThinkingBudgets>,
}

impl<'de> Deserialize<'de> for SimpleStreamOptionsWire {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = SimpleStreamOptionsWireDef::deserialize(deserializer)?;
        Ok(Self {
            stream: StreamOptionsWire {
                identity: value.identity,
                timeout_ms: value.timeout_ms,
                max_retries: value.max_retries,
                max_retry_delay_ms: value.max_retry_delay_ms,
                temperature: value.temperature,
                sampling_params: value.sampling_params,
                max_tokens: value.max_tokens,
                transport: value.transport,
                cache_retention: value.cache_retention,
                session_id: value.session_id,
                websocket_connect_timeout_ms: value.websocket_connect_timeout_ms,
                metadata: value.metadata,
            },
            tool_choice: value.tool_choice,
            reasoning: value.reasoning,
            deferred: value.deferred,
            thinking_budgets: value.thinking_budgets,
        })
    }
}

/// Serializable deferred-response fetch options.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DeferredFetchOptionsWire {
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
    /// Maximum provider long-poll duration in milliseconds.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub wait: Option<SafeUnsigned>,
}

/// Serializable best-effort deferred-response cancellation options.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DeferredCancelOptionsWire {
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
    /// Maximum agent retry backoff, defaulting to sixty seconds.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub max_agent_delay_ms: Option<SafeUnsigned>,
}
