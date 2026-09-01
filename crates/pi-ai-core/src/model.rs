//! Model, pricing, compatibility, routing, and catalog-store schemas.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use serde::de::DeserializeOwned;
use serde::de::{MapAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de, ser};

use crate::id::{ApiId, ImagesApiId, ImagesProviderId, ModelId, ProviderId};
use crate::json::{FiniteF64, JsonNumber, JsonObject, Presence, SafeUnsigned};

const HARD_DENIED_MODEL_HEADERS: [&str; 7] = [
    "authorization",
    "proxy-authorization",
    "cookie",
    "set-cookie",
    "api-key",
    "x-api-key",
    "x-goog-api-key",
];

/// Pure declarative policy for public model-header names.
///
/// Names are matched ASCII-case-insensitively. The default policy has an empty
/// allowlist and therefore classifies no nonempty header map as public.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ModelHeaderPolicy {
    allowlist: BTreeSet<String>,
}

impl ModelHeaderPolicy {
    /// Creates a policy from public header names.
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

/// Error classifying transient model headers for durable storage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ModelHeaderClassificationError {
    /// Raw input repeated a header under exact or ASCII-case-insensitive spelling.
    Duplicate(String),
    /// Core rejected a credential-capable header name regardless of host policy.
    HardDenied(String),
    /// The injected host policy did not classify a header as public.
    PolicyDenied(String),
}

impl fmt::Display for ModelHeaderClassificationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Duplicate(name) => write!(formatter, "duplicate model header {name:?}"),
            Self::HardDenied(name) => {
                write!(formatter, "model header {name:?} is credential-capable")
            }
            Self::PolicyDenied(name) => {
                write!(formatter, "model header {name:?} was not classified public")
            }
        }
    }
}

impl std::error::Error for ModelHeaderClassificationError {}

/// Transient, unclassified model-header intake.
///
/// Values may contain credentials. Debug output reports keys only.
#[derive(Clone, Default, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct RawModelHeaders(BTreeMap<String, String>);

impl RawModelHeaders {
    /// Creates transient unclassified model headers.
    pub fn new(values: BTreeMap<String, String>) -> Result<Self, ModelHeaderClassificationError> {
        let mut normalized = BTreeSet::new();
        for name in values.keys() {
            if !normalized.insert(name.to_ascii_lowercase()) {
                return Err(ModelHeaderClassificationError::Duplicate(name.clone()));
            }
        }
        Ok(Self(values))
    }

    /// Classifies every header through core hard-denial and an injected policy.
    pub fn classify(
        self,
        policy: &ModelHeaderPolicy,
    ) -> Result<PublicModelHeaders, ModelHeaderClassificationError> {
        for name in self.0.keys() {
            let normalized = name.to_ascii_lowercase();
            if HARD_DENIED_MODEL_HEADERS.contains(&normalized.as_str()) {
                return Err(ModelHeaderClassificationError::HardDenied(name.clone()));
            }
            if !policy.allows(name) {
                return Err(ModelHeaderClassificationError::PolicyDenied(name.clone()));
            }
        }
        Ok(PublicModelHeaders(self.0))
    }

    /// Returns transient raw values for policy inspection.
    pub const fn as_map(&self) -> &BTreeMap<String, String> {
        &self.0
    }
}

impl<'de> Deserialize<'de> for RawModelHeaders {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct RawModelHeadersVisitor;

        impl<'de> Visitor<'de> for RawModelHeadersVisitor {
            type Value = RawModelHeaders;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a model-header map without case-insensitive duplicates")
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
                            "duplicate model header {name:?}"
                        )));
                    }
                    values.insert(name, value);
                }
                Ok(RawModelHeaders(values))
            }
        }

        deserializer.deserialize_map(RawModelHeadersVisitor)
    }
}

impl fmt::Debug for RawModelHeaders {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        debug_header_keys("RawModelHeaders", &self.0, formatter)
    }
}

/// Host-policy-classified model headers safe for model storage.
///
/// Nonempty values can be produced only by [`RawModelHeaders::classify`]. The
/// type has no `Deserialize` implementation or unchecked public constructor.
#[derive(Clone, Default, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct PublicModelHeaders(BTreeMap<String, String>);

impl PublicModelHeaders {
    /// Returns the classified public headers.
    pub const fn as_map(&self) -> &BTreeMap<String, String> {
        &self.0
    }
}

impl fmt::Debug for PublicModelHeaders {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        debug_header_keys("PublicModelHeaders", &self.0, formatter)
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

/// Error validating public model endpoint metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PublicModelBaseUrlError {
    /// The endpoint string was empty.
    Empty,
    /// The URL authority contained user information before `@`.
    UserInfo,
    /// The endpoint contained a query component.
    Query,
    /// The endpoint contained a fragment component.
    Fragment,
}

impl fmt::Display for PublicModelBaseUrlError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => formatter.write_str("model baseUrl must be nonempty"),
            Self::UserInfo => formatter.write_str("model baseUrl must not contain URL userinfo"),
            Self::Query => formatter.write_str("model baseUrl must not contain a query"),
            Self::Fragment => formatter.write_str("model baseUrl must not contain a fragment"),
        }
    }
}

impl std::error::Error for PublicModelBaseUrlError {}

/// Public, credential-free model endpoint metadata.
///
/// Validation is intentionally structural: it rejects only empty strings and
/// credential-capable URL components. It does not constrain scheme, host, port,
/// or path and preserves accepted bytes exactly.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct PublicModelBaseUrl(String);

impl PublicModelBaseUrl {
    /// Validates public model endpoint metadata.
    pub fn new(value: impl Into<String>) -> Result<Self, PublicModelBaseUrlError> {
        let value = value.into();
        if value.is_empty() {
            return Err(PublicModelBaseUrlError::Empty);
        }
        if value.contains('?') {
            return Err(PublicModelBaseUrlError::Query);
        }
        if value.contains('#') {
            return Err(PublicModelBaseUrlError::Fragment);
        }
        let authority_start = value.find("//").map(|index| index + 2);
        if let Some(start) = authority_start {
            let end = value[start..]
                .find(['/', '?', '#'])
                .map_or(value.len(), |offset| start + offset);
            let authority = &value[start..end];
            if authority.contains('@') {
                return Err(PublicModelBaseUrlError::UserInfo);
            }
        }
        Ok(Self(value))
    }

    /// Returns the accepted wire string.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the wrapper and returns the accepted wire string.
    pub fn into_string(self) -> String {
        self.0
    }
}

impl fmt::Debug for PublicModelBaseUrl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PublicModelBaseUrl([REDACTED])")
    }
}

/// Stored text-model endpoint metadata.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ModelBaseUrl {
    /// Public, credential-free endpoint metadata.
    Public(PublicModelBaseUrl),
    /// Azure endpoint resolved from runtime request configuration.
    RuntimeResolved,
}

impl ModelBaseUrl {
    fn wire_value(&self) -> &str {
        match self {
            Self::Public(value) => value.as_str(),
            Self::RuntimeResolved => "",
        }
    }
}

impl Serialize for ModelBaseUrl {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.wire_value())
    }
}

impl fmt::Debug for ModelBaseUrl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ModelBaseUrl([REDACTED])")
    }
}

/// Model input or output modality.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ModelInput {
    /// Text content.
    #[serde(rename = "text")]
    Text,
    /// Image content.
    #[serde(rename = "image")]
    Image,
}

/// Provider-neutral thinking level used for requests.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum ThinkingLevel {
    /// Minimal thinking.
    #[serde(rename = "minimal")]
    Minimal,
    /// Low thinking.
    #[serde(rename = "low")]
    Low,
    /// Medium thinking.
    #[serde(rename = "medium")]
    Medium,
    /// High thinking.
    #[serde(rename = "high")]
    High,
    /// Extra-high thinking.
    #[serde(rename = "xhigh")]
    ExtraHigh,
    /// Unbounded maximum thinking where supported.
    #[serde(rename = "max")]
    Max,
}

/// Model-declared thinking level, including the disabled state.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum ModelThinkingLevel {
    /// Thinking disabled.
    #[serde(rename = "off")]
    Off,
    /// Minimal thinking.
    #[serde(rename = "minimal")]
    Minimal,
    /// Low thinking.
    #[serde(rename = "low")]
    Low,
    /// Medium thinking.
    #[serde(rename = "medium")]
    Medium,
    /// High thinking.
    #[serde(rename = "high")]
    High,
    /// Extra-high thinking.
    #[serde(rename = "xhigh")]
    ExtraHigh,
    /// Unbounded maximum thinking where supported.
    #[serde(rename = "max")]
    Max,
}

/// Partial mapping from public thinking levels to provider-specific values.
///
/// A present null value explicitly marks a level as unsupported.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ThinkingLevelMap(
    #[serde(deserialize_with = "crate::json::deserialize_btree_map_reject_duplicates")]
    BTreeMap<ModelThinkingLevel, Option<String>>,
);

impl ThinkingLevelMap {
    /// Creates a deterministic thinking-level map.
    pub fn new(values: BTreeMap<ModelThinkingLevel, Option<String>>) -> Self {
        Self(values)
    }

    /// Returns the mapped levels.
    pub const fn as_map(&self) -> &BTreeMap<ModelThinkingLevel, Option<String>> {
        &self.0
    }
}

/// Per-million-token model cost rates.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ModelCostRates {
    /// Input-token rate.
    pub input: FiniteF64,
    /// Output-token rate.
    pub output: FiniteF64,
    /// Cache-read rate.
    pub cache_read: FiniteF64,
    /// Cache-write rate.
    pub cache_write: FiniteF64,
}

/// Request-wide pricing tier selected above an input threshold.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ModelCostTier {
    /// Input-token rate.
    pub input: FiniteF64,
    /// Output-token rate.
    pub output: FiniteF64,
    /// Cache-read rate.
    pub cache_read: FiniteF64,
    /// Cache-write rate.
    pub cache_write: FiniteF64,
    /// Exclusive lower input-token threshold for this tier.
    pub input_tokens_above: SafeUnsigned,
}

/// Model pricing and optional request-wide tiers.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ModelCost {
    /// Input-token rate.
    pub input: FiniteF64,
    /// Output-token rate.
    pub output: FiniteF64,
    /// Cache-read rate.
    pub cache_read: FiniteF64,
    /// Cache-write rate.
    pub cache_write: FiniteF64,
    /// Optional tiered rates.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub tiers: Option<Vec<ModelCostTier>>,
}

/// Variable substituted into an OpenAI-compatible chat-template value.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ChatTemplateVariableName {
    /// Whether thinking is enabled.
    #[serde(rename = "thinking.enabled")]
    Enabled,
    /// Requested thinking effort.
    #[serde(rename = "thinking.effort")]
    Effort,
    /// Requested thinking budget.
    #[serde(rename = "thinking.budget")]
    Budget,
}

/// A chat-template variable reference.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ChatTemplateVariable {
    /// Variable name, serialized as `$var`.
    #[serde(rename = "$var")]
    pub variable: ChatTemplateVariableName,
    /// Whether to omit the argument when thinking is disabled.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub omit_when_off: Option<bool>,
}

/// Value accepted by OpenAI-compatible chat-template arguments.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ChatTemplateKwargValue {
    /// String value.
    String(String),
    /// Safe numeric value.
    Number(JsonNumber),
    /// Boolean value.
    Bool(bool),
    /// Explicit null.
    Null,
    /// Runtime-substituted thinking value.
    Variable(ChatTemplateVariable),
}

/// Top-level request field used to cap reasoning tokens.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ThinkingTokenBudgetField {
    /// vLLM field.
    #[serde(rename = "thinking_token_budget")]
    ThinkingTokenBudget,
    /// Qwen, DashScope, and SGLang field.
    #[serde(rename = "thinking_budget")]
    ThinkingBudget,
    /// llama.cpp field.
    #[serde(rename = "thinking_budget_tokens")]
    ThinkingBudgetTokens,
}

/// OpenAI-compatible maximum-token request field.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum MaxTokensField {
    /// Modern maximum-completion-token field.
    #[serde(rename = "max_completion_tokens")]
    MaxCompletionTokens,
    /// Legacy maximum-token field.
    #[serde(rename = "max_tokens")]
    MaxTokens,
}

/// OpenAI-compatible thinking request format.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ThinkingFormat {
    /// OpenAI reasoning effort.
    #[serde(rename = "openai")]
    OpenAi,
    /// OpenRouter reasoning object.
    #[serde(rename = "openrouter")]
    OpenRouter,
    /// DeepSeek thinking object.
    #[serde(rename = "deepseek")]
    DeepSeek,
    /// Together thinking object.
    #[serde(rename = "together")]
    Together,
    /// Baseten chat-template arguments.
    #[serde(rename = "baseten")]
    Baseten,
    /// Z.AI thinking object.
    #[serde(rename = "zai")]
    Zai,
    /// Qwen top-level enable flag.
    #[serde(rename = "qwen")]
    Qwen,
    /// Configurable chat-template keyword arguments.
    #[serde(rename = "chat-template")]
    ChatTemplate,
    /// Qwen chat-template keyword arguments.
    #[serde(rename = "qwen-chat-template")]
    QwenChatTemplate,
    /// Top-level string thinking field.
    #[serde(rename = "string-thinking")]
    StringThinking,
    /// Ant Ling mapped reasoning object.
    #[serde(rename = "ant-ling")]
    AntLing,
}

/// Provider data-collection preference for OpenRouter routing.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum OpenRouterDataCollection {
    /// Deny providers that collect request data.
    #[serde(rename = "deny")]
    Deny,
    /// Allow providers that may collect request data.
    #[serde(rename = "allow")]
    Allow,
}

/// String or number accepted by OpenRouter price fields.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum StringOrNumber {
    /// String-form price.
    String(String),
    /// Numeric price.
    Number(JsonNumber),
}

/// OpenRouter sort object.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpenRouterSortOptions {
    /// Sorting metric.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub by: Option<String>,
    /// Optional partition strategy, preserving explicit null.
    #[serde(default, skip_serializing_if = "Presence::is_missing")]
    pub partition: Presence<String>,
}

/// OpenRouter sorting strategy.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum OpenRouterSort {
    /// Named sorting strategy.
    Name(String),
    /// Structured sorting strategy.
    Options(OpenRouterSortOptions),
}

/// OpenRouter maximum prices per unit.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpenRouterMaxPrice {
    /// Maximum prompt-token price.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub prompt: Option<StringOrNumber>,
    /// Maximum completion-token price.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub completion: Option<StringOrNumber>,
    /// Maximum image price.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub image: Option<StringOrNumber>,
    /// Maximum audio-unit price.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub audio: Option<StringOrNumber>,
    /// Maximum request price.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub request: Option<StringOrNumber>,
}

/// Percentile-specific throughput or latency bounds.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpenRouterPercentiles {
    /// 50th percentile bound.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub p50: Option<JsonNumber>,
    /// 75th percentile bound.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub p75: Option<JsonNumber>,
    /// 90th percentile bound.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub p90: Option<JsonNumber>,
    /// 99th percentile bound.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub p99: Option<JsonNumber>,
}

/// Single p50 number or percentile-specific bounds.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum OpenRouterMetricPreference {
    /// Single p50 bound.
    Number(JsonNumber),
    /// Percentile-specific bounds.
    Percentiles(OpenRouterPercentiles),
}

/// OpenRouter upstream-provider routing preferences.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpenRouterRouting {
    /// Whether backup providers may serve the request.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub allow_fallbacks: Option<bool>,
    /// Whether every routed provider must support all request parameters.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub require_parameters: Option<bool>,
    /// Data-collection policy.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub data_collection: Option<OpenRouterDataCollection>,
    /// Zero-data-retention requirement.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub zdr: Option<bool>,
    /// Text-distillation requirement.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub enforce_distillable_text: Option<bool>,
    /// Ordered provider preference.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub order: Option<Vec<String>>,
    /// Exclusive provider allowlist.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub only: Option<Vec<String>>,
    /// Provider denylist.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub ignore: Option<Vec<String>>,
    /// Allowed quantization levels.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub quantizations: Option<Vec<String>>,
    /// Sorting strategy.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub sort: Option<OpenRouterSort>,
    /// Maximum prices.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub max_price: Option<OpenRouterMaxPrice>,
    /// Preferred minimum throughput.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub preferred_min_throughput: Option<OpenRouterMetricPreference>,
    /// Preferred maximum latency.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub preferred_max_latency: Option<OpenRouterMetricPreference>,
}

/// Vercel AI Gateway provider routing preferences.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VercelGatewayRouting {
    /// Exclusive provider list.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub only: Option<Vec<String>>,
    /// Ordered provider preference.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub order: Option<Vec<String>>,
}

/// Session-affinity header format.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SessionAffinityFormat {
    /// OpenAI session and client-request headers.
    #[serde(rename = "openai")]
    OpenAi,
    /// OpenAI client-request headers without `session_id`.
    #[serde(rename = "openai-nosession")]
    OpenAiNoSession,
    /// OpenRouter session header.
    #[serde(rename = "openrouter")]
    OpenRouter,
}

/// OpenAI-compatible completions behavior overrides.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OpenAiCompletionsCompat {
    /// Whether the provider accepts `store`.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub supports_store: Option<bool>,
    /// Whether the provider accepts the developer role.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub supports_developer_role: Option<bool>,
    /// Whether the provider accepts reasoning effort.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub supports_reasoning_effort: Option<bool>,
    /// Whether streaming responses report usage.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub supports_usage_in_streaming: Option<bool>,
    /// Whether streams report a finish reason.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub supports_finish_reason: Option<bool>,
    /// Maximum-token request field.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub max_tokens_field: Option<MaxTokensField>,
    /// Whether tool results require a name.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub requires_tool_result_name: Option<bool>,
    /// Whether a user message after tool results needs an assistant separator.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub requires_assistant_after_tool_result: Option<bool>,
    /// Whether thinking blocks must become text.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub requires_thinking_as_text: Option<bool>,
    /// Whether replayed assistants require empty reasoning content.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub requires_reasoning_content_on_assistant_messages: Option<bool>,
    /// Provider thinking format.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub thinking_format: Option<ThinkingFormat>,
    /// Configurable chat-template keyword arguments.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_btree_map_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub chat_template_kwargs: Option<BTreeMap<String, ChatTemplateKwargValue>>,
    /// Baseten chat-template arguments.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_btree_map_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub chat_template_args: Option<BTreeMap<String, ChatTemplateKwargValue>>,
    /// OpenRouter routing preferences.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub open_router_routing: Option<OpenRouterRouting>,
    /// Vercel routing preferences.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub vercel_gateway_routing: Option<VercelGatewayRouting>,
    /// Whether Z.AI accepts streamed tool deltas.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub zai_tool_stream: Option<bool>,
    /// Reasoning-token budget field.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub thinking_token_budget_field: Option<ThinkingTokenBudgetField>,
    /// Legacy vLLM reasoning-budget capability flag.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub supports_thinking_token_budget: Option<bool>,
    /// Whether OpenAI grammar tools are supported.
    #[serde(
        rename = "supportsOpenAIGrammarTools",
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub supports_open_ai_grammar_tools: Option<bool>,
    /// Whether strict JSON-schema tools are supported.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub supports_strict_mode: Option<bool>,
    /// Anthropic-style prompt cache control.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub cache_control_format: Option<AnthropicCacheControlFormat>,
    /// Whether session-affinity headers are sent.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub send_session_affinity_headers: Option<bool>,
    /// Provider-specific deferred-tool serialization.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub deferred_tools_mode: Option<KimiDeferredToolsMode>,
    /// Session-affinity header format.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub session_affinity_format: Option<SessionAffinityFormat>,
    /// Whether long cache retention is supported.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub supports_long_cache_retention: Option<bool>,
}

/// Literal Anthropic cache-control format.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum AnthropicCacheControlFormat {
    /// Anthropic-compatible cache-control markers.
    #[serde(rename = "anthropic")]
    Anthropic,
}

/// Literal Kimi deferred-tool mode.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum KimiDeferredToolsMode {
    /// Kimi deferred-tool serialization.
    #[serde(rename = "kimi")]
    Kimi,
}

/// OpenAI Responses behavior overrides.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OpenAiResponsesCompat {
    /// Whether the developer role is supported.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub supports_developer_role: Option<bool>,
    /// Session-affinity format.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub session_affinity_format: Option<SessionAffinityFormat>,
    /// Whether long cache retention is supported.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub supports_long_cache_retention: Option<bool>,
    /// Whether strict function tools are supported.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub supports_strict_mode: Option<bool>,
    /// Whether grammar tools are supported.
    #[serde(
        rename = "supportsOpenAIGrammarTools",
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub supports_open_ai_grammar_tools: Option<bool>,
    /// Whether message-anchored additional tools are supported.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub supports_additional_tools: Option<bool>,
    /// Whether client-executed tool search is supported.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub supports_tool_search: Option<bool>,
    /// Whether explicit prompt cache mode is supported.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub supports_explicit_prompt_cache_mode: Option<bool>,
}

/// Anthropic server-side fallback target and pricing.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AnthropicAllowedFallbackModel {
    /// Fallback provider.
    pub provider: ProviderId,
    /// Fallback model identity.
    pub model: ModelId,
    /// Local pricing metadata.
    pub cost: ModelCost,
}

/// Anthropic Messages behavior overrides.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AnthropicMessagesCompat {
    /// Whether eager tool input streaming is supported.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub supports_eager_tool_input_streaming: Option<bool>,
    /// Whether long cache retention is supported.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub supports_long_cache_retention: Option<bool>,
    /// Whether session-affinity headers are sent.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub send_session_affinity_headers: Option<bool>,
    /// Whether cache-control markers on tools are supported.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub supports_cache_control_on_tools: Option<bool>,
    /// Whether the temperature field is supported.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub supports_temperature: Option<bool>,
    /// Whether adaptive thinking is forced.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub force_adaptive_thinking: Option<bool>,
    /// Whether empty thinking signatures are replayed.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub allow_empty_signature: Option<bool>,
    /// Whether strict Anthropic tools are supported.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub supports_strict_tools: Option<bool>,
    /// Allowed server-side fallback targets.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub allowed_fallback_models: Option<Vec<AnthropicAllowedFallbackModel>>,
    /// Whether deferred tool references are supported.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub supports_tool_references: Option<bool>,
}

/// Amazon Bedrock behavior overrides.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BedrockCompat {
    /// Whether strict tool schemas are supported.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub supports_strict_mode: Option<bool>,
}

/// Raw untagged compatibility object carried by a model.
///
/// This avoids ambiguous untagged-enum decoding. Call [`Self::decode_for_api`]
/// to validate and select the typed shape using the enclosing model API.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ModelCompat(pub JsonObject);

/// Typed compatibility value selected using an enclosing API identity.
#[derive(Clone, Debug, PartialEq)]
pub enum TypedModelCompat {
    /// OpenAI-compatible completions settings.
    OpenAiCompletions(Box<OpenAiCompletionsCompat>),
    /// OpenAI Responses-family settings.
    OpenAiResponses(OpenAiResponsesCompat),
    /// Anthropic Messages settings.
    AnthropicMessages(Box<AnthropicMessagesCompat>),
    /// Amazon Bedrock settings.
    Bedrock(BedrockCompat),
}

/// Error validating a raw model compatibility object.
#[derive(Debug)]
pub enum ModelCompatError {
    /// The API does not define a compatibility object.
    UnsupportedApi(ApiId),
    /// The compatibility object does not match the API-specific shape.
    Invalid(serde_json::Error),
}

impl fmt::Display for ModelCompatError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedApi(api) => write!(
                formatter,
                "API {api} does not accept model compatibility data"
            ),
            Self::Invalid(error) => write!(formatter, "invalid model compatibility data: {error}"),
        }
    }
}

impl std::error::Error for ModelCompatError {}

impl ModelCompat {
    fn decode<T>(&self) -> Result<T, ModelCompatError>
    where
        T: DeserializeOwned,
    {
        let value = serde_json::to_value(&self.0).map_err(ModelCompatError::Invalid)?;
        serde_json::from_value(value).map_err(ModelCompatError::Invalid)
    }

    /// Selects and validates the compatibility shape for an enclosing API.
    pub fn decode_for_api(&self, api: &ApiId) -> Result<TypedModelCompat, ModelCompatError> {
        match api.as_str() {
            ApiId::OPENAI_COMPLETIONS => self
                .decode()
                .map(Box::new)
                .map(TypedModelCompat::OpenAiCompletions),
            ApiId::OPENAI_RESPONSES
            | ApiId::AZURE_OPENAI_RESPONSES
            | ApiId::OPENAI_CODEX_RESPONSES => self.decode().map(TypedModelCompat::OpenAiResponses),
            ApiId::ANTHROPIC_MESSAGES => self
                .decode()
                .map(Box::new)
                .map(TypedModelCompat::AnthropicMessages),
            ApiId::BEDROCK_CONVERSE_STREAM => self.decode().map(TypedModelCompat::Bedrock),
            _ => Err(ModelCompatError::UnsupportedApi(api.clone())),
        }
    }
}

/// Transient raw text-model intake with unclassified headers.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RawModel {
    /// Provider-scoped model identity.
    pub id: ModelId,
    /// Human-readable model name.
    pub name: String,
    /// API protocol.
    pub api: ApiId,
    /// Provider identity.
    pub provider: ProviderId,
    /// Provider base URL.
    pub base_url: String,
    /// Whether the model supports thinking.
    pub reasoning: bool,
    /// Optional provider-specific thinking-level mapping.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub thinking_level_map: Option<ThinkingLevelMap>,
    /// Accepted input modalities.
    pub input: Vec<ModelInput>,
    /// Pricing metadata.
    pub cost: ModelCost,
    /// Context-window tokens.
    pub context_window: SafeUnsigned,
    /// Maximum output tokens.
    pub max_tokens: SafeUnsigned,
    /// Default arbitrary sampling parameters.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub sampling_params: Option<JsonObject>,
    /// Transient unclassified model headers.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub headers: Option<RawModelHeaders>,
    /// Raw API-selected compatibility object.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub compat: Option<ModelCompat>,
}

impl fmt::Debug for RawModel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RawModel")
            .field("id", &self.id)
            .field("name", &self.name)
            .field("api", &self.api)
            .field("provider", &self.provider)
            .field("base_url", &"[REDACTED]")
            .field("reasoning", &self.reasoning)
            .field("thinking_level_map", &self.thinking_level_map)
            .field("input", &self.input)
            .field("cost", &self.cost)
            .field("context_window", &self.context_window)
            .field("max_tokens", &self.max_tokens)
            .field("sampling_params", &self.sampling_params)
            .field("headers", &self.headers)
            .field("compat", &self.compat)
            .finish()
    }
}

/// Error converting raw text-model intake into a validated storable model.
#[derive(Debug)]
pub enum ModelValidationError {
    /// API-specific compatibility validation failed.
    Compat(ModelCompatError),
    /// Public model endpoint validation failed.
    BaseUrl(PublicModelBaseUrlError),
    /// Runtime-resolved endpoint sentinel was paired with a non-Azure API.
    RuntimeResolvedApiMismatch,
    /// Model-header classification failed.
    Headers(ModelHeaderClassificationError),
}

impl fmt::Display for ModelValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Compat(error) => error.fmt(formatter),
            Self::BaseUrl(error) => error.fmt(formatter),
            Self::RuntimeResolvedApiMismatch => {
                formatter.write_str("runtime-resolved baseUrl requires azure-openai-responses")
            }
            Self::Headers(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for ModelValidationError {}

impl RawModel {
    /// Consumes raw intake and classifies headers under the supplied policy.
    pub fn into_model(self, policy: &ModelHeaderPolicy) -> Result<Model, ModelValidationError> {
        if let Some(compat) = &self.compat {
            compat
                .decode_for_api(&self.api)
                .map_err(ModelValidationError::Compat)?;
        }
        let headers = self
            .headers
            .map(|headers| headers.classify(policy))
            .transpose()
            .map_err(ModelValidationError::Headers)?;
        let base_url = if self.base_url.is_empty() {
            if self.api.as_str() == ApiId::AZURE_OPENAI_RESPONSES {
                ModelBaseUrl::RuntimeResolved
            } else {
                return Err(ModelValidationError::BaseUrl(
                    PublicModelBaseUrlError::Empty,
                ));
            }
        } else {
            ModelBaseUrl::Public(
                PublicModelBaseUrl::new(self.base_url).map_err(ModelValidationError::BaseUrl)?,
            )
        };
        Ok(Model {
            id: self.id,
            name: self.name,
            api: self.api,
            provider: self.provider,
            base_url,
            reasoning: self.reasoning,
            thinking_level_map: self.thinking_level_map,
            input: self.input,
            cost: self.cost,
            context_window: self.context_window,
            max_tokens: self.max_tokens,
            sampling_params: self.sampling_params,
            headers,
            compat: self.compat,
        })
    }
}

/// Validated provider-neutral text model safe for model storage.
#[derive(Clone, PartialEq)]
pub struct Model {
    /// Provider-scoped model identity.
    pub id: ModelId,
    /// Human-readable model name.
    pub name: String,
    /// API protocol.
    pub api: ApiId,
    /// Provider identity.
    pub provider: ProviderId,
    /// Provider base URL.
    pub base_url: ModelBaseUrl,
    /// Whether the model supports thinking.
    pub reasoning: bool,
    /// Optional provider-specific thinking-level mapping.
    // serde-non-null: delegated-to-raw-model
    pub thinking_level_map: Option<ThinkingLevelMap>,
    /// Accepted input modalities.
    pub input: Vec<ModelInput>,
    /// Pricing metadata.
    pub cost: ModelCost,
    /// Context-window tokens.
    pub context_window: SafeUnsigned,
    /// Maximum output tokens.
    pub max_tokens: SafeUnsigned,
    /// Default arbitrary sampling parameters.
    // serde-non-null: delegated-to-raw-model
    pub sampling_params: Option<JsonObject>,
    /// Host-policy-classified public model headers.
    // serde-non-null: delegated-to-raw-model
    pub headers: Option<PublicModelHeaders>,
    /// Raw API-selected compatibility object, validated by model serde.
    // serde-non-null: delegated-to-raw-model
    pub compat: Option<ModelCompat>,
}

impl Model {
    /// Validates and selects this model's optional compatibility object.
    pub fn typed_compat(&self) -> Result<Option<TypedModelCompat>, ModelCompatError> {
        self.compat
            .as_ref()
            .map(|compat| compat.decode_for_api(&self.api))
            .transpose()
    }

    fn validate_base_url(&self) -> Result<(), ModelValidationError> {
        if matches!(self.base_url, ModelBaseUrl::RuntimeResolved)
            && self.api.as_str() != ApiId::AZURE_OPENAI_RESPONSES
        {
            return Err(ModelValidationError::RuntimeResolvedApiMismatch);
        }
        Ok(())
    }
}

impl fmt::Debug for Model {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Model")
            .field("id", &self.id)
            .field("name", &self.name)
            .field("api", &self.api)
            .field("provider", &self.provider)
            .field("base_url", &"[REDACTED]")
            .field("reasoning", &self.reasoning)
            .field("thinking_level_map", &self.thinking_level_map)
            .field("input", &self.input)
            .field("cost", &self.cost)
            .field("context_window", &self.context_window)
            .field("max_tokens", &self.max_tokens)
            .field("sampling_params", &self.sampling_params)
            .field("headers", &self.headers)
            .field("compat", &self.compat)
            .finish()
    }
}

impl From<&Model> for RawModel {
    fn from(model: &Model) -> Self {
        Self {
            id: model.id.clone(),
            name: model.name.clone(),
            api: model.api.clone(),
            provider: model.provider.clone(),
            base_url: model.base_url.wire_value().to_owned(),
            reasoning: model.reasoning,
            thinking_level_map: model.thinking_level_map.clone(),
            input: model.input.clone(),
            cost: model.cost.clone(),
            context_window: model.context_window,
            max_tokens: model.max_tokens,
            sampling_params: model.sampling_params.clone(),
            headers: model
                .headers
                .as_ref()
                .map(|headers| RawModelHeaders(headers.0.clone())),
            compat: model.compat.clone(),
        }
    }
}

impl Serialize for Model {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.typed_compat().map_err(ser::Error::custom)?;
        self.validate_base_url().map_err(ser::Error::custom)?;
        RawModel::from(self).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Model {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        RawModel::deserialize(deserializer)?
            .into_model(&ModelHeaderPolicy::default())
            .map_err(de::Error::custom)
    }
}

/// Transient raw image-model intake with unclassified headers.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RawImagesModel {
    /// Provider-scoped model identity.
    pub id: ModelId,
    /// Human-readable model name.
    pub name: String,
    /// Image API protocol.
    pub api: ImagesApiId,
    /// Image provider identity.
    pub provider: ImagesProviderId,
    /// Provider base URL.
    pub base_url: String,
    /// Accepted input modalities.
    pub input: Vec<ModelInput>,
    /// Produced output modalities.
    pub output: Vec<ModelInput>,
    /// Pricing metadata.
    pub cost: ModelCost,
    /// Default arbitrary sampling parameters.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub sampling_params: Option<JsonObject>,
    /// Transient unclassified model headers.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub headers: Option<RawModelHeaders>,
}

impl fmt::Debug for RawImagesModel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RawImagesModel")
            .field("id", &self.id)
            .field("name", &self.name)
            .field("api", &self.api)
            .field("provider", &self.provider)
            .field("base_url", &"[REDACTED]")
            .field("input", &self.input)
            .field("output", &self.output)
            .field("cost", &self.cost)
            .field("sampling_params", &self.sampling_params)
            .field("headers", &self.headers)
            .finish()
    }
}

impl RawImagesModel {
    /// Consumes raw intake and classifies headers under the supplied policy.
    pub fn into_model(
        self,
        policy: &ModelHeaderPolicy,
    ) -> Result<ImagesModel, ModelValidationError> {
        let headers = self
            .headers
            .map(|headers| headers.classify(policy))
            .transpose()
            .map_err(ModelValidationError::Headers)?;
        let base_url =
            PublicModelBaseUrl::new(self.base_url).map_err(ModelValidationError::BaseUrl)?;
        Ok(ImagesModel {
            id: self.id,
            name: self.name,
            api: self.api,
            provider: self.provider,
            base_url,
            input: self.input,
            output: self.output,
            cost: self.cost,
            sampling_params: self.sampling_params,
            headers,
        })
    }
}

/// Validated image-generation model safe for model storage.
#[derive(Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ImagesModel {
    /// Provider-scoped model identity.
    pub id: ModelId,
    /// Human-readable model name.
    pub name: String,
    /// Image API protocol.
    pub api: ImagesApiId,
    /// Image provider identity.
    pub provider: ImagesProviderId,
    /// Provider base URL.
    pub base_url: PublicModelBaseUrl,
    /// Accepted input modalities.
    pub input: Vec<ModelInput>,
    /// Produced output modalities.
    pub output: Vec<ModelInput>,
    /// Pricing metadata.
    pub cost: ModelCost,
    /// Default arbitrary sampling parameters.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub sampling_params: Option<JsonObject>,
    /// Host-policy-classified public model headers.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub headers: Option<PublicModelHeaders>,
}

impl fmt::Debug for ImagesModel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ImagesModel")
            .field("id", &self.id)
            .field("name", &self.name)
            .field("api", &self.api)
            .field("provider", &self.provider)
            .field("base_url", &"[REDACTED]")
            .field("input", &self.input)
            .field("output", &self.output)
            .field("cost", &self.cost)
            .field("sampling_params", &self.sampling_params)
            .field("headers", &self.headers)
            .finish()
    }
}

impl From<&ImagesModel> for RawImagesModel {
    fn from(model: &ImagesModel) -> Self {
        Self {
            id: model.id.clone(),
            name: model.name.clone(),
            api: model.api.clone(),
            provider: model.provider.clone(),
            base_url: model.base_url.as_str().to_owned(),
            input: model.input.clone(),
            output: model.output.clone(),
            cost: model.cost.clone(),
            sampling_params: model.sampling_params.clone(),
            headers: model
                .headers
                .as_ref()
                .map(|headers| RawModelHeaders(headers.0.clone())),
        }
    }
}

impl<'de> Deserialize<'de> for ImagesModel {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        RawImagesModel::deserialize(deserializer)?
            .into_model(&ModelHeaderPolicy::default())
            .map_err(de::Error::custom)
    }
}

/// Transient raw model-store intake used for policy-aware restore.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RawModelsStoreEntry {
    /// Last-known raw models.
    pub models: Vec<RawModel>,
    /// Remote catalog `Last-Modified` timestamp.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub last_modified: Option<SafeUnsigned>,
    /// Unix timestamp of the last completed remote check.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub checked_at: Option<SafeUnsigned>,
    /// Opaque remote `ETag`, including quotes.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub etag: Option<String>,
}

impl RawModelsStoreEntry {
    /// Consumes raw store data and reclassifies every model under the current policy.
    pub fn into_store(
        self,
        policy: &ModelHeaderPolicy,
    ) -> Result<ModelsStoreEntry, ModelValidationError> {
        let models = self
            .models
            .into_iter()
            .map(|model| model.into_model(policy))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ModelsStoreEntry {
            models,
            last_modified: self.last_modified,
            checked_at: self.checked_at,
            etag: self.etag,
        })
    }
}

/// Persisted last-known model catalog for one provider.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ModelsStoreEntry {
    /// Last-known models.
    pub models: Vec<Model>,
    /// Remote catalog `Last-Modified` timestamp.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub last_modified: Option<SafeUnsigned>,
    /// Unix timestamp of the last completed remote check.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub checked_at: Option<SafeUnsigned>,
    /// Opaque remote `ETag`, including quotes.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub etag: Option<String>,
}
