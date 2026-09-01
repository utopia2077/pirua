//! Captured configuration and structural preparation DTOs.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use pi_ai_core::{
    CacheRetention, DeferredRequest, JsonObject, ModelId, ModelThinkingLevel, ProviderId,
    SafeUnsigned, ToolName, Transport,
};
use serde::de::{self, MapAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::OneBasedSafeUnsigned;
use crate::message::AgentMessage;
use crate::serde_support::deserialize_optional_non_null;
use crate::tool::{QueueMode, ToolExecutionMode};

/// The Agent thinking level. It aliases the model-declared level, including `off`.
pub type AgentThinkingLevel = ModelThinkingLevel;

/// Public retry policy accepted by the Agent and Harness surface.
pub use pi_ai_core::RetryPolicy;

const HARD_DENIED_PROVIDER_HEADERS: [&str; 7] = [
    "authorization",
    "proxy-authorization",
    "cookie",
    "set-cookie",
    "api-key",
    "x-api-key",
    "x-goog-api-key",
];

/// Pure declarative policy for public Harness header names.
///
/// Matching is ASCII-case-insensitive and the default allowlist is empty.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ProviderHeaderPolicy {
    allowlist: BTreeSet<String>,
}

impl ProviderHeaderPolicy {
    /// Creates a policy from names explicitly classified as public by the host.
    pub fn from_allowlist(names: impl IntoIterator<Item = String>) -> Self {
        Self {
            allowlist: names
                .into_iter()
                .map(|name| name.to_ascii_lowercase())
                .collect(),
        }
    }

    fn allows(&self, name: &str) -> bool {
        self.allowlist.contains(&name.to_ascii_lowercase())
    }
}

/// Error classifying transient Harness headers for durable state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProviderHeaderClassificationError {
    /// Raw input repeated a header under exact or ASCII-case-insensitive spelling.
    Duplicate(String),
    /// Core rejected a credential-capable header name regardless of host policy.
    HardDenied(String),
    /// The host policy did not classify a header as public.
    PolicyDenied(String),
}

impl fmt::Display for ProviderHeaderClassificationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Duplicate(name) => write!(formatter, "duplicate Harness header {name:?}"),
            Self::HardDenied(name) => {
                write!(formatter, "Harness header {name:?} is credential-capable")
            }
            Self::PolicyDenied(name) => {
                write!(
                    formatter,
                    "Harness header {name:?} was not classified public"
                )
            }
        }
    }
}

impl std::error::Error for ProviderHeaderClassificationError {}

/// Transient, unclassified Harness header intake.
///
/// Values may contain credentials. Debug output reports keys only. Unlike AI
/// `ProviderHeaders`, this pinned Harness shape accepts only string values.
#[derive(Clone, Default, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct RawProviderHeaders(BTreeMap<String, String>);

impl RawProviderHeaders {
    /// Creates raw headers while rejecting case-insensitive duplicates.
    pub fn new(
        values: BTreeMap<String, String>,
    ) -> Result<Self, ProviderHeaderClassificationError> {
        let mut normalized = BTreeSet::new();
        for name in values.keys() {
            if !normalized.insert(name.to_ascii_lowercase()) {
                return Err(ProviderHeaderClassificationError::Duplicate(name.clone()));
            }
        }
        Ok(Self(values))
    }

    /// Classifies every header through hard denial and the injected host policy.
    pub fn classify(
        self,
        policy: &ProviderHeaderPolicy,
    ) -> Result<PublicProviderHeaders, ProviderHeaderClassificationError> {
        for name in self.0.keys() {
            let normalized = name.to_ascii_lowercase();
            if HARD_DENIED_PROVIDER_HEADERS.contains(&normalized.as_str()) {
                return Err(ProviderHeaderClassificationError::HardDenied(name.clone()));
            }
            if !policy.allows(name) {
                return Err(ProviderHeaderClassificationError::PolicyDenied(
                    name.clone(),
                ));
            }
        }
        Ok(PublicProviderHeaders(self.0))
    }

    /// Returns transient values for host-policy inspection.
    pub const fn as_map(&self) -> &BTreeMap<String, String> {
        &self.0
    }
}

impl<'de> Deserialize<'de> for RawProviderHeaders {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct HeaderVisitor;

        impl<'de> Visitor<'de> for HeaderVisitor {
            type Value = RawProviderHeaders;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(
                    "a string-valued Harness header map without case-insensitive duplicates",
                )
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut values = BTreeMap::new();
                let mut normalized = BTreeSet::new();
                while let Some((name, value)) = map.next_entry::<String, String>()? {
                    if !normalized.insert(name.to_ascii_lowercase()) {
                        return Err(de::Error::custom(format!(
                            "duplicate Harness header {name:?}"
                        )));
                    }
                    values.insert(name, value);
                }
                Ok(RawProviderHeaders(values))
            }
        }

        deserializer.deserialize_map(HeaderVisitor)
    }
}

impl fmt::Debug for RawProviderHeaders {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        debug_header_keys("RawProviderHeaders", &self.0, formatter)
    }
}

/// Host-policy-classified public Harness headers safe for durable state.
///
/// Nonempty values can be produced only by [`RawProviderHeaders::classify`].
/// This immutable wrapper intentionally has no `Deserialize` implementation or
/// unchecked public constructor.
#[derive(Clone, Default, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct PublicProviderHeaders(BTreeMap<String, String>);

impl PublicProviderHeaders {
    /// Returns the classified public headers.
    pub const fn as_map(&self) -> &BTreeMap<String, String> {
        &self.0
    }
}

impl fmt::Debug for PublicProviderHeaders {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        debug_header_keys("PublicProviderHeaders", &self.0, formatter)
    }
}

fn debug_header_keys(
    name: &str,
    headers: &BTreeMap<String, String>,
    formatter: &mut fmt::Formatter<'_>,
) -> fmt::Result {
    let keys: Vec<&str> = headers.keys().map(String::as_str).collect();
    formatter.debug_tuple(name).field(&keys).finish()
}

/// Transient raw form of [`AgentHarnessStreamOptions`].
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RawAgentHarnessStreamOptions {
    /// Preferred provider transport.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub transport: Option<Transport>,
    /// Provider request timeout in milliseconds.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub timeout_ms: Option<SafeUnsigned>,
    /// Maximum provider-client retry attempts.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub max_retries: Option<SafeUnsigned>,
    /// Cap for provider-requested retry delays in milliseconds.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub max_retry_delay_ms: Option<SafeUnsigned>,
    /// Transient unclassified string-valued headers.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub headers: Option<RawProviderHeaders>,
    /// Provider-readable deterministic metadata.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub metadata: Option<JsonObject>,
    /// Provider cache-retention hint.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub cache_retention: Option<CacheRetention>,
    /// Optional deferred-response request, in boolean or window form.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub deferred: Option<DeferredRequest>,
}

impl RawAgentHarnessStreamOptions {
    /// Consumes raw intake and classifies headers under the supplied host policy.
    pub fn into_durable(
        self,
        policy: &ProviderHeaderPolicy,
    ) -> Result<AgentHarnessStreamOptions, ProviderHeaderClassificationError> {
        let headers = self
            .headers
            .map(|headers| headers.classify(policy))
            .transpose()?;
        Ok(AgentHarnessStreamOptions {
            transport: self.transport,
            timeout_ms: self.timeout_ms,
            max_retries: self.max_retries,
            max_retry_delay_ms: self.max_retry_delay_ms,
            headers,
            metadata: self.metadata,
            cache_retention: self.cache_retention,
            deferred: self.deferred,
        })
    }
}

/// Curated, durable Harness stream options with classified public headers.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct AgentHarnessStreamOptions {
    /// Preferred provider transport.
    pub transport: Option<Transport>,
    /// Provider request timeout in milliseconds.
    pub timeout_ms: Option<SafeUnsigned>,
    /// Maximum provider-client retry attempts.
    pub max_retries: Option<SafeUnsigned>,
    /// Cap for provider-requested retry delays in milliseconds.
    pub max_retry_delay_ms: Option<SafeUnsigned>,
    /// Host-policy-classified public headers.
    pub headers: Option<PublicProviderHeaders>,
    /// Provider-readable deterministic metadata.
    pub metadata: Option<JsonObject>,
    /// Provider cache-retention hint.
    pub cache_retention: Option<CacheRetention>,
    /// Optional deferred-response request.
    pub deferred: Option<DeferredRequest>,
}

impl Serialize for AgentHarnessStreamOptions {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        RawAgentHarnessStreamOptions {
            transport: self.transport,
            timeout_ms: self.timeout_ms,
            max_retries: self.max_retries,
            max_retry_delay_ms: self.max_retry_delay_ms,
            headers: self
                .headers
                .as_ref()
                .map(|headers| RawProviderHeaders(headers.0.clone())),
            metadata: self.metadata.clone(),
            cache_retention: self.cache_retention,
            deferred: self.deferred.clone(),
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for AgentHarnessStreamOptions {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        RawAgentHarnessStreamOptions::deserialize(deserializer)?
            .into_durable(&ProviderHeaderPolicy::default())
            .map_err(de::Error::custom)
    }
}

/// Compaction thresholds and retained-tail settings.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CompactionSettings {
    /// Whether automatic compaction checks are enabled.
    pub enabled: bool,
    /// Tokens reserved for summary prompt and output.
    pub reserve_tokens: SafeUnsigned,
    /// Approximate recent-context tokens retained after compaction.
    pub keep_recent_tokens: SafeUnsigned,
}

/// Captured run settings.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RunSettings {
    /// Captured compaction settings.
    pub compaction: CompactionSettings,
    /// Steering queue drain mode.
    pub steering_mode: QueueMode,
    /// Follow-up queue drain mode.
    pub follow_up_mode: QueueMode,
    /// Tool batch execution mode.
    pub tool_execution: ToolExecutionMode,
}

/// Durable model identity captured in a lane configuration.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LaneModel {
    /// Provider identity.
    pub provider: ProviderId,
    /// Provider-scoped model identity.
    pub model_id: ModelId,
}

/// Total durable lane configuration.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LaneConfiguration {
    /// Captured provider/model identity.
    pub model: LaneModel,
    /// Captured Agent thinking level, including `off`.
    pub thinking_level: AgentThinkingLevel,
    /// Active model-visible tool names.
    pub active_tool_names: Vec<ToolName>,
}

/// Retry settings normalized to an exact maximum attempt count.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NormalizedRetryPolicy {
    /// Total allowed attempts, including the first attempt.
    pub max_attempts: OneBasedSafeUnsigned,
    /// Base exponential-backoff delay in milliseconds.
    pub base_delay_ms: SafeUnsigned,
}

/// Sorted durable file-operation paths used by structural work.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DurableFileOperations {
    read: Vec<String>,
    written: Vec<String>,
    edited: Vec<String>,
}

/// Error constructing normalized durable file-operation paths.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DurableFileOperationsError {
    /// One or more path arrays were not strictly sorted and unique.
    Unsorted,
}

impl fmt::Display for DurableFileOperationsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("durable file-operation arrays must be strictly sorted")
    }
}

impl std::error::Error for DurableFileOperationsError {}

impl DurableFileOperations {
    /// Creates validated, strictly sorted file-operation path arrays.
    pub fn new(
        read: Vec<String>,
        written: Vec<String>,
        edited: Vec<String>,
    ) -> Result<Self, DurableFileOperationsError> {
        if !strictly_sorted(&read) || !strictly_sorted(&written) || !strictly_sorted(&edited) {
            return Err(DurableFileOperationsError::Unsorted);
        }
        Ok(Self {
            read,
            written,
            edited,
        })
    }

    /// Returns read paths.
    pub fn read(&self) -> &[String] {
        &self.read
    }

    /// Returns written paths.
    pub fn written(&self) -> &[String] {
        &self.written
    }

    /// Returns edited paths.
    pub fn edited(&self) -> &[String] {
        &self.edited
    }
}

impl<'de> Deserialize<'de> for DurableFileOperations {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            read: Vec<String>,
            written: Vec<String>,
            edited: Vec<String>,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::new(wire.read, wire.written, wire.edited).map_err(de::Error::custom)
    }
}

fn strictly_sorted(values: &[String]) -> bool {
    values.windows(2).all(|pair| pair[0] < pair[1])
}

/// Preparation persisted before a structural decision hook.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum DurableStructuralPreparation {
    /// Prepared compaction inputs.
    Compaction {
        /// Messages selected for summarization.
        #[serde(rename = "messagesToSummarize")]
        messages_to_summarize: Vec<AgentMessage>,
        /// Prefix messages summarized separately for a split turn.
        #[serde(rename = "turnPrefixMessages")]
        turn_prefix_messages: Vec<AgentMessage>,
        /// Recent messages retained directly on the compaction entry.
        #[serde(rename = "retainedTail")]
        retained_tail: Vec<AgentMessage>,
        /// Whether the compaction splits a turn.
        #[serde(rename = "isSplitTurn")]
        is_split_turn: bool,
        /// Estimated context tokens before compaction.
        #[serde(rename = "tokensBefore")]
        tokens_before: SafeUnsigned,
        /// Previous summary used for iterative compaction.
        #[serde(
            rename = "previousSummary",
            default,
            deserialize_with = "deserialize_optional_non_null",
            skip_serializing_if = "Option::is_none"
        )]
        previous_summary: Option<String>,
        /// Normalized file operations.
        #[serde(rename = "fileOps")]
        file_ops: DurableFileOperations,
        /// Captured compaction settings.
        settings: CompactionSettings,
    },
    /// Prepared branch-summary inputs.
    BranchSummary {
        /// Messages selected for the branch summary.
        messages: Vec<AgentMessage>,
        /// Normalized file operations.
        #[serde(rename = "fileOps")]
        file_ops: DurableFileOperations,
        /// Estimated selected-message tokens.
        #[serde(rename = "totalTokens")]
        total_tokens: SafeUnsigned,
    },
}
