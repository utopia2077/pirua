//! Closed hook names, typed invocation/result DTOs, and sealed spec mapping.

#![allow(missing_docs)]

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use pi_ai_core::{
    CacheRetention, DeferredRequest, JsonObject, JsonValue, Model, ModelHeaderPolicy,
    ModelValidationError, Presence, RawModel, SafeUnsigned, ToolCallId, ToolName, Transport, Usage,
};
use serde::de::{self, MapAccess, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::serde_support::{
    deserialize_optional_non_null, deserialize_required_nullable, from_json_value,
};
use crate::{
    AgentHarnessStreamOptions, AgentMessage, BranchPreparation, BranchSummaryResult, CompactResult,
    CompactionPreparation, EntryId, LaneId, OperationId, ProviderHeaderClassificationError,
    ProviderHeaderPolicy, RawAgentHarnessStreamOptions, Resources, SettledAssistantMessage,
};

/// Exact closed hook-name inventory.
pub const HOOK_NAMES: &[&str] = &[
    "before_run",
    "before_resume",
    "before_run_end",
    "transform_context",
    "before_request",
    "before_payload",
    "after_response",
    "before_tool",
    "after_tool",
    "before_compaction",
    "before_navigation",
];

/// Closed Harness hook name.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum HookName {
    #[serde(rename = "before_run")]
    BeforeRun,
    #[serde(rename = "before_resume")]
    BeforeResume,
    #[serde(rename = "before_run_end")]
    BeforeRunEnd,
    #[serde(rename = "transform_context")]
    TransformContext,
    #[serde(rename = "before_request")]
    BeforeRequest,
    #[serde(rename = "before_payload")]
    BeforePayload,
    #[serde(rename = "after_response")]
    AfterResponse,
    #[serde(rename = "before_tool")]
    BeforeTool,
    #[serde(rename = "after_tool")]
    AfterTool,
    #[serde(rename = "before_compaction")]
    BeforeCompaction,
    #[serde(rename = "before_navigation")]
    BeforeNavigation,
}

/// Classified header patch where JSON null deletes a key. Debug output exposes keys only.
#[derive(Clone, Default, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct ProviderHeaderPatch(BTreeMap<String, Option<String>>);

impl ProviderHeaderPatch {
    /// Returns the deterministic patch entries.
    pub const fn as_map(&self) -> &BTreeMap<String, Option<String>> {
        &self.0
    }
}

impl fmt::Debug for ProviderHeaderPatch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let keys: Vec<&str> = self.0.keys().map(String::as_str).collect();
        formatter
            .debug_tuple("ProviderHeaderPatch")
            .field(&keys)
            .finish()
    }
}

/// Transient unclassified header patch used for policy-aware hook decode.
#[derive(Clone, Default, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct RawProviderHeaderPatch(BTreeMap<String, Option<String>>);

impl RawProviderHeaderPatch {
    /// Creates raw patch intake while rejecting ASCII-case-insensitive duplicates.
    pub fn new(
        values: BTreeMap<String, Option<String>>,
    ) -> Result<Self, ProviderHeaderClassificationError> {
        Self::from_entries(values)
    }

    /// Creates raw patch intake from entries while retaining exact-duplicate detection.
    pub fn from_entries(
        entries: impl IntoIterator<Item = (String, Option<String>)>,
    ) -> Result<Self, ProviderHeaderClassificationError> {
        let mut values = BTreeMap::new();
        let mut normalized = BTreeSet::new();
        for (name, value) in entries {
            if values.contains_key(&name) || !normalized.insert(name.to_ascii_lowercase()) {
                return Err(ProviderHeaderClassificationError::Duplicate(name.clone()));
            }
            values.insert(name, value);
        }
        Ok(Self(values))
    }

    /// Classifies every patch key under the existing Harness header policy.
    pub fn classify(
        self,
        policy: &crate::ProviderHeaderPolicy,
    ) -> Result<ProviderHeaderPatch, crate::ProviderHeaderClassificationError> {
        let classified_names = self
            .0
            .iter()
            .map(|(name, value)| (name.clone(), value.clone().unwrap_or_default()))
            .collect();
        crate::RawProviderHeaders::new(classified_names)?.classify(policy)?;
        Ok(ProviderHeaderPatch(self.0))
    }
}

impl fmt::Debug for RawProviderHeaderPatch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let keys: Vec<&str> = self.0.keys().map(String::as_str).collect();
        formatter
            .debug_tuple("RawProviderHeaderPatch")
            .field(&keys)
            .finish()
    }
}

impl<'de> Deserialize<'de> for RawProviderHeaderPatch {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct PatchVisitor;

        impl<'de> Visitor<'de> for PatchVisitor {
            type Value = RawProviderHeaderPatch;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a header patch without case-insensitive duplicate names")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut values = BTreeMap::new();
                let mut normalized = BTreeSet::new();
                while let Some((name, value)) = map.next_entry::<String, Option<String>>()? {
                    if !normalized.insert(name.to_ascii_lowercase()) {
                        return Err(de::Error::custom("duplicate Harness header patch name"));
                    }
                    values.insert(name, value);
                }
                RawProviderHeaderPatch::new(values).map_err(de::Error::custom)
            }
        }

        deserializer.deserialize_map(PatchVisitor)
    }
}

/// Invalid language-neutral metadata patch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetadataPatchError {
    /// Delete keys were not strictly sorted and unique.
    UnsortedDelete,
    /// A key appeared in both set and delete operations.
    Overlap,
}

impl fmt::Display for MetadataPatchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsortedDelete => {
                formatter.write_str("metadata delete keys must be strictly sorted")
            }
            Self::Overlap => formatter.write_str("metadata set and delete keys must be disjoint"),
        }
    }
}

impl std::error::Error for MetadataPatchError {}

/// Language-neutral metadata set/delete patch.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MetadataPatch {
    set: JsonObject,
    delete: Vec<String>,
}

impl MetadataPatch {
    /// Creates a patch after enforcing sorted/unique/disjoint delete operations.
    pub fn new(set: JsonObject, delete: Vec<String>) -> Result<Self, MetadataPatchError> {
        if !delete.windows(2).all(|pair| pair[0] < pair[1]) {
            return Err(MetadataPatchError::UnsortedDelete);
        }
        if delete.iter().any(|key| set.as_map().contains_key(key)) {
            return Err(MetadataPatchError::Overlap);
        }
        Ok(Self { set, delete })
    }

    /// Maps JavaScript property values: `Some(value)` sets, `None` deletes.
    pub fn from_javascript_properties(values: BTreeMap<String, Option<JsonValue>>) -> Self {
        let mut set = BTreeMap::new();
        let mut delete = Vec::new();
        for (key, value) in values {
            match value {
                Some(value) => {
                    set.insert(key, value);
                }
                None => delete.push(key),
            }
        }
        Self {
            set: JsonObject::new(set),
            delete,
        }
    }

    /// Returns metadata values to set. JSON null remains data.
    pub const fn set(&self) -> &JsonObject {
        &self.set
    }

    /// Returns strictly sorted unique delete keys.
    pub fn delete(&self) -> &[String] {
        &self.delete
    }
}

impl<'de> Deserialize<'de> for MetadataPatch {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            set: JsonObject,
            delete: Vec<String>,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::new(wire.set, wire.delete).map_err(de::Error::custom)
    }
}

/// Curated per-request stream-option patch.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentHarnessStreamOptionsPatch {
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub transport: Option<Transport>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub timeout_ms: Option<SafeUnsigned>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub max_retries: Option<SafeUnsigned>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub max_retry_delay_ms: Option<SafeUnsigned>,
    /// Missing keeps headers, null clears all headers, and a map patches keys.
    #[serde(default, skip_serializing_if = "Presence::is_missing")]
    pub headers: Presence<ProviderHeaderPatch>,
    /// Missing keeps metadata, null clears it, and a value applies set/delete operations.
    #[serde(default, skip_serializing_if = "Presence::is_missing")]
    pub metadata: Presence<MetadataPatch>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub cache_retention: Option<CacheRetention>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub deferred: Option<DeferredRequest>,
}

/// Transient raw stream-option patch for policy-aware hook result intake.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RawAgentHarnessStreamOptionsPatch {
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub transport: Option<Transport>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub timeout_ms: Option<SafeUnsigned>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub max_retries: Option<SafeUnsigned>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub max_retry_delay_ms: Option<SafeUnsigned>,
    #[serde(default, skip_serializing_if = "Presence::is_missing")]
    pub headers: Presence<RawProviderHeaderPatch>,
    #[serde(default, skip_serializing_if = "Presence::is_missing")]
    pub metadata: Presence<MetadataPatch>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub cache_retention: Option<CacheRetention>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub deferred: Option<DeferredRequest>,
}

impl RawAgentHarnessStreamOptionsPatch {
    /// Consumes raw patch intake and classifies every non-null header value.
    pub fn into_durable(
        self,
        policy: &crate::ProviderHeaderPolicy,
    ) -> Result<AgentHarnessStreamOptionsPatch, crate::ProviderHeaderClassificationError> {
        let headers = match self.headers {
            Presence::Missing => Presence::Missing,
            Presence::Null => Presence::Null,
            Presence::Value(headers) => Presence::Value(headers.classify(policy)?),
        };
        Ok(AgentHarnessStreamOptionsPatch {
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

impl<'de> Deserialize<'de> for AgentHarnessStreamOptionsPatch {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        RawAgentHarnessStreamOptionsPatch::deserialize(deserializer)?
            .into_durable(&crate::ProviderHeaderPolicy::default())
            .map_err(de::Error::custom)
    }
}

/// Hook request step.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum HookRequestStep {
    #[serde(rename = "assistant")]
    Assistant,
    #[serde(rename = "deferred")]
    Deferred,
    #[serde(rename = "compaction")]
    Compaction,
    #[serde(rename = "branch_summary")]
    BranchSummary,
}

/// Before-run hook invocation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BeforeRunEvent {
    pub lane: LaneId,
    pub run_id: OperationId,
    pub prompt: Vec<AgentMessage>,
    pub system_prompt: String,
    pub resources: Resources,
}

/// Before-run aggregate patch.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BeforeRunResult {
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub messages: Option<Vec<AgentMessage>>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub system_prompt: Option<String>,
    #[serde(default, skip_serializing_if = "Presence::is_missing")]
    pub resume_data: Presence<JsonValue>,
}

/// Literal prepared resume kind.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum PreparedResumeKind {
    #[serde(rename = "run")]
    Run,
    #[serde(rename = "compaction")]
    Compaction,
    #[serde(rename = "navigation")]
    Navigation,
}

/// Prepared run resume invocation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BeforeResumeRunEvent {
    pub lane: LaneId,
    pub run_id: OperationId,
    pub kind: RunPreparedKind,
    pub prompt: Vec<AgentMessage>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub system_prompt_override: Option<String>,
    #[serde(default, skip_serializing_if = "Presence::is_missing")]
    pub resume_data: Presence<JsonValue>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum RunPreparedKind {
    #[default]
    #[serde(rename = "run")]
    Run,
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum CompactionPreparedKind {
    #[default]
    #[serde(rename = "compaction")]
    Compaction,
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum NavigationPreparedKind {
    #[default]
    #[serde(rename = "navigation")]
    Navigation,
}

/// Prepared compaction resume invocation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BeforeResumeCompactionEvent {
    pub lane: LaneId,
    pub run_id: OperationId,
    pub kind: CompactionPreparedKind,
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub source_leaf_id: Option<EntryId>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub custom_instructions: Option<String>,
    #[serde(default, skip_serializing_if = "Presence::is_missing")]
    pub resume_data: Presence<JsonValue>,
}

/// Prepared navigation resume invocation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BeforeResumeNavigationEvent {
    pub lane: LaneId,
    pub run_id: OperationId,
    pub kind: NavigationPreparedKind,
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub source_leaf_id: Option<EntryId>,
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub target_id: Option<EntryId>,
    pub summarize: bool,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub label: Option<String>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub custom_instructions: Option<String>,
    #[serde(default, skip_serializing_if = "Presence::is_missing")]
    pub resume_data: Presence<JsonValue>,
}

/// Structurally exclusive before-resume invocation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum BeforeResumeEvent {
    Run(BeforeResumeRunEvent),
    Compaction(BeforeResumeCompactionEvent),
    Navigation(BeforeResumeNavigationEvent),
}

/// Before-run-end invocation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BeforeRunEndEvent {
    pub lane: LaneId,
    pub run_id: OperationId,
    pub messages: Vec<AgentMessage>,
}
/// Before-run-end aggregate patch.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BeforeRunEndResult {
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub follow_up: Option<String>,
}

/// Transform-context invocation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TransformContextEvent {
    pub lane: LaneId,
    pub run_id: OperationId,
    pub messages: Vec<AgentMessage>,
}
/// Transform-context replacement.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TransformContextResult {
    pub messages: Vec<AgentMessage>,
}

/// Before-request invocation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BeforeRequestEvent {
    pub lane: LaneId,
    pub run_id: OperationId,
    pub model: Model,
    pub step: HookRequestStep,
    pub attempt: crate::OneBasedSafeUnsigned,
    pub stream_options: AgentHarnessStreamOptions,
}
/// Before-request option patch.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BeforeRequestResult {
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub stream_options: Option<AgentHarnessStreamOptionsPatch>,
}

/// Raw before-request result whose stream patch still requires header classification.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RawBeforeRequestResult {
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub stream_options: Option<RawAgentHarnessStreamOptionsPatch>,
}

/// Before-payload invocation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BeforePayloadEvent {
    pub lane: LaneId,
    pub run_id: OperationId,
    pub model: Model,
    pub payload: JsonValue,
}
/// Before-payload replacement.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BeforePayloadResult {
    pub payload: JsonValue,
}

const HARD_DENIED_RESPONSE_HEADERS: [&str; 7] = [
    "authorization",
    "proxy-authorization",
    "cookie",
    "set-cookie",
    "api-key",
    "x-api-key",
    "x-goog-api-key",
];

/// Invalid response-header construction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ResponseHeaderError {
    /// Exact or ASCII-case-insensitive duplicate header name.
    Duplicate(String),
    /// Credential-capable response header name.
    Credential(String),
}

impl fmt::Display for ResponseHeaderError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Duplicate(name) => write!(formatter, "duplicate response header {name:?}"),
            Self::Credential(name) => {
                write!(formatter, "credential-capable response header {name:?}")
            }
        }
    }
}

impl std::error::Error for ResponseHeaderError {}

/// Sanitized string-valued response headers exposed to the response hook.
#[derive(Clone, Default, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct ResponseHeaders(BTreeMap<String, String>);

impl ResponseHeaders {
    /// Validates a runtime response-header map.
    pub fn new(values: BTreeMap<String, String>) -> Result<Self, ResponseHeaderError> {
        Self::from_entries(values)
    }

    /// Validates response-header entries while retaining exact-duplicate detection.
    pub fn from_entries(
        entries: impl IntoIterator<Item = (String, String)>,
    ) -> Result<Self, ResponseHeaderError> {
        let mut values = BTreeMap::new();
        let mut normalized = BTreeSet::new();
        for (name, value) in entries {
            let normalized_name = name.to_ascii_lowercase();
            if values.contains_key(&name) || !normalized.insert(normalized_name.clone()) {
                return Err(ResponseHeaderError::Duplicate(name.clone()));
            }
            if HARD_DENIED_RESPONSE_HEADERS.contains(&normalized_name.as_str()) {
                return Err(ResponseHeaderError::Credential(name.clone()));
            }
            values.insert(name, value);
        }
        Ok(Self(values))
    }

    /// Sanitizes a valid response by dropping unsafe headers; an empty result is omitted.
    pub fn sanitize(values: BTreeMap<String, String>) -> Option<Self> {
        Self::sanitize_entries(values)
    }

    /// Sanitizes response-header entries while retaining duplicate detection.
    pub fn sanitize_entries(entries: impl IntoIterator<Item = (String, String)>) -> Option<Self> {
        let mut accepted = BTreeMap::<String, (String, String)>::new();
        let mut duplicates = BTreeSet::new();
        for (name, value) in entries {
            let normalized_name = name.to_ascii_lowercase();
            if HARD_DENIED_RESPONSE_HEADERS.contains(&normalized_name.as_str()) {
                continue;
            }
            if duplicates.contains(&normalized_name) {
                continue;
            }
            if accepted.remove(&normalized_name).is_some() {
                duplicates.insert(normalized_name);
                continue;
            }
            accepted.insert(normalized_name, (name, value));
        }
        let sanitized = accepted.into_values().collect::<BTreeMap<String, String>>();
        (!sanitized.is_empty()).then_some(Self(sanitized))
    }

    /// Returns deterministic string-valued response headers.
    pub const fn as_map(&self) -> &BTreeMap<String, String> {
        &self.0
    }
}

impl fmt::Debug for ResponseHeaders {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let keys: Vec<&str> = self.0.keys().map(String::as_str).collect();
        formatter
            .debug_tuple("ResponseHeaders")
            .field(&keys)
            .finish()
    }
}

impl<'de> Deserialize<'de> for ResponseHeaders {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ResponseHeaderVisitor;
        impl<'de> Visitor<'de> for ResponseHeaderVisitor {
            type Value = ResponseHeaders;
            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a string-valued response-header map without duplicate keys")
            }
            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut values = BTreeMap::new();
                while let Some((name, value)) = map.next_entry::<String, String>()? {
                    if values.insert(name, value).is_some() {
                        return Err(de::Error::custom("duplicate response header name"));
                    }
                }
                ResponseHeaders::new(values).map_err(de::Error::custom)
            }
        }
        deserializer.deserialize_map(ResponseHeaderVisitor)
    }
}

/// After-response invocation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AfterResponseEvent {
    pub lane: LaneId,
    pub run_id: OperationId,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub status: Option<SafeUnsigned>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub headers: Option<ResponseHeaders>,
    pub message: SettledAssistantMessage,
}
/// After-response message replacement.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AfterResponseResult {
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub message: Option<SettledAssistantMessage>,
}

/// Before-tool invocation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BeforeToolEvent {
    pub lane: LaneId,
    pub run_id: OperationId,
    pub tool_call_id: ToolCallId,
    pub tool_name: ToolName,
    pub args: JsonObject,
}
/// Before-tool fail-closed block.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BeforeToolBlock {
    pub reason: String,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub terminate: Option<bool>,
}
/// Before-tool argument/block patch.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BeforeToolResult {
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub args: Option<JsonObject>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub block: Option<BeforeToolBlock>,
}

/// After-tool invocation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AfterToolEvent {
    pub lane: LaneId,
    pub run_id: OperationId,
    pub tool_call_id: ToolCallId,
    pub tool_name: ToolName,
    pub args: JsonObject,
    pub content: Vec<pi_ai_core::InputContent>,
    #[serde(default, skip_serializing_if = "Presence::is_missing")]
    pub details: Presence<JsonValue>,
    pub is_error: bool,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub usage: Option<Usage>,
}
/// After-tool field-by-field patch.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AfterToolResult {
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub content: Option<Vec<pi_ai_core::InputContent>>,
    #[serde(default, skip_serializing_if = "Presence::is_missing")]
    pub details: Presence<JsonValue>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub is_error: Option<bool>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub usage: Option<Usage>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub terminate: Option<bool>,
}

/// Structural decision reason.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum HookCompactionReason {
    #[serde(rename = "manual")]
    Manual,
    #[serde(rename = "threshold")]
    Threshold,
    #[serde(rename = "overflow")]
    Overflow,
}
/// Before-compaction invocation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BeforeCompactionEvent {
    pub lane: LaneId,
    pub run_id: OperationId,
    pub reason: HookCompactionReason,
    pub preparation: CompactionPreparation,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub custom_instructions: Option<String>,
}
/// Before-compaction decline/result selection.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum BeforeCompactionResult {
    Continue(BeforeCompactionEmpty),
    DeclineFalse(BeforeCompactionContinue),
    DeclineTrue(BeforeCompactionDecline),
    Compaction(Box<BeforeCompactionSupply>),
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BeforeCompactionEmpty {}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BeforeCompactionContinue {
    pub decline: pi_ai_core::LiteralFalse,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BeforeCompactionDecline {
    pub decline: pi_ai_core::LiteralTrue,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BeforeCompactionSupply {
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub decline: Option<pi_ai_core::LiteralFalse>,
    pub compaction: CompactResult,
}

/// Before-navigation invocation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BeforeNavigationEvent {
    pub lane: LaneId,
    pub run_id: OperationId,
    pub target_id: EntryId,
    pub preparation: BranchPreparation,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub custom_instructions: Option<String>,
}
/// Before-navigation decline/result selection.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum BeforeNavigationResult {
    Continue(BeforeNavigationEmpty),
    DeclineFalse(BeforeNavigationContinue),
    DeclineTrue(BeforeNavigationDecline),
    Summary(BeforeNavigationSupply),
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BeforeNavigationEmpty {}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BeforeNavigationContinue {
    pub decline: pi_ai_core::LiteralFalse,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BeforeNavigationDecline {
    pub decline: pi_ai_core::LiteralTrue,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BeforeNavigationSupply {
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub decline: Option<pi_ai_core::LiteralFalse>,
    pub summary: BranchSummaryResult,
}

mod sealed {
    pub trait Sealed {}
}

/// Sealed compile-time mapping from a hook marker to its event and result DTO.
pub trait HookSpec: sealed::Sealed + Send + Sync + 'static {
    /// Invocation DTO.
    type Event: Clone + Send + Sync + 'static;
    /// Hook-specific handler result, including absence where permitted.
    type Result: Clone + Send + Sync + 'static;
    /// Closed hook name.
    const NAME: HookName;
}

macro_rules! hook_spec {
    ($marker:ident, $event:ty, $result:ty, $name:ident) => {
        #[doc = concat!("Typed marker for `", stringify!($name), "`.")]
        #[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
        pub struct $marker;
        impl sealed::Sealed for $marker {}
        impl HookSpec for $marker {
            type Event = $event;
            type Result = $result;
            const NAME: HookName = HookName::$name;
        }
    };
}

hook_spec!(
    BeforeRun,
    BeforeRunEvent,
    Option<BeforeRunResult>,
    BeforeRun
);
hook_spec!(BeforeResume, BeforeResumeEvent, (), BeforeResume);
hook_spec!(
    BeforeRunEnd,
    BeforeRunEndEvent,
    Option<BeforeRunEndResult>,
    BeforeRunEnd
);
hook_spec!(
    TransformContext,
    TransformContextEvent,
    Option<TransformContextResult>,
    TransformContext
);
hook_spec!(
    BeforeRequest,
    BeforeRequestEvent,
    Option<BeforeRequestResult>,
    BeforeRequest
);
hook_spec!(
    BeforePayload,
    BeforePayloadEvent,
    Option<BeforePayloadResult>,
    BeforePayload
);
hook_spec!(
    AfterResponse,
    AfterResponseEvent,
    Option<AfterResponseResult>,
    AfterResponse
);
hook_spec!(
    BeforeTool,
    BeforeToolEvent,
    Option<BeforeToolResult>,
    BeforeTool
);
hook_spec!(
    AfterTool,
    AfterToolEvent,
    Option<AfterToolResult>,
    AfterTool
);
hook_spec!(
    BeforeCompaction,
    BeforeCompactionEvent,
    Option<BeforeCompactionResult>,
    BeforeCompaction
);
hook_spec!(
    BeforeNavigation,
    BeforeNavigationEvent,
    Option<BeforeNavigationResult>,
    BeforeNavigation
);

/// Callback-free hook event union for protocol/process-local routing.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "name",
    content = "event",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum HookEvent {
    BeforeRun(BeforeRunEvent),
    BeforeResume(BeforeResumeEvent),
    BeforeRunEnd(BeforeRunEndEvent),
    TransformContext(TransformContextEvent),
    BeforeRequest(Box<BeforeRequestEvent>),
    BeforePayload(Box<BeforePayloadEvent>),
    AfterResponse(Box<AfterResponseEvent>),
    BeforeTool(BeforeToolEvent),
    AfterTool(AfterToolEvent),
    BeforeCompaction(Box<BeforeCompactionEvent>),
    BeforeNavigation(Box<BeforeNavigationEvent>),
}

impl HookEvent {
    /// Returns the closed dispatched hook name.
    pub const fn name(&self) -> HookName {
        match self {
            Self::BeforeRun(_) => HookName::BeforeRun,
            Self::BeforeResume(_) => HookName::BeforeResume,
            Self::BeforeRunEnd(_) => HookName::BeforeRunEnd,
            Self::TransformContext(_) => HookName::TransformContext,
            Self::BeforeRequest(_) => HookName::BeforeRequest,
            Self::BeforePayload(_) => HookName::BeforePayload,
            Self::AfterResponse(_) => HookName::AfterResponse,
            Self::BeforeTool(_) => HookName::BeforeTool,
            Self::AfterTool(_) => HookName::AfterTool,
            Self::BeforeCompaction(_) => HookName::BeforeCompaction,
            Self::BeforeNavigation(_) => HookName::BeforeNavigation,
        }
    }
}

/// Error classifying a complete raw hook containing root.
#[derive(Debug)]
pub enum HookPolicyError {
    /// Model endpoint/header validation failed.
    Model(ModelValidationError),
    /// Harness provider-header classification failed.
    Provider(ProviderHeaderClassificationError),
}

impl fmt::Display for HookPolicyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Model(error) => error.fmt(formatter),
            Self::Provider(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for HookPolicyError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Model(error) => Some(error),
            Self::Provider(error) => Some(error),
        }
    }
}

/// Raw before-request event whose model/options still require policy classification.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RawBeforeRequestEvent {
    pub lane: LaneId,
    pub run_id: OperationId,
    pub model: RawModel,
    pub step: HookRequestStep,
    pub attempt: crate::OneBasedSafeUnsigned,
    pub stream_options: RawAgentHarnessStreamOptions,
}

/// Raw before-payload event whose model still requires policy classification.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RawBeforePayloadEvent {
    pub lane: LaneId,
    pub run_id: OperationId,
    pub model: RawModel,
    pub payload: JsonValue,
}

/// Complete raw hook-event seed for policy-aware containing-root conversion.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "name",
    content = "event",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum RawHookEvent {
    BeforeRun(BeforeRunEvent),
    BeforeResume(BeforeResumeEvent),
    BeforeRunEnd(BeforeRunEndEvent),
    TransformContext(TransformContextEvent),
    BeforeRequest(Box<RawBeforeRequestEvent>),
    BeforePayload(Box<RawBeforePayloadEvent>),
    AfterResponse(Box<AfterResponseEvent>),
    BeforeTool(BeforeToolEvent),
    AfterTool(AfterToolEvent),
    BeforeCompaction(Box<BeforeCompactionEvent>),
    BeforeNavigation(Box<BeforeNavigationEvent>),
}

impl RawHookEvent {
    /// Classifies every nested model and Harness header location.
    pub fn into_durable(
        self,
        model_policy: &ModelHeaderPolicy,
        provider_policy: &ProviderHeaderPolicy,
    ) -> Result<HookEvent, HookPolicyError> {
        Ok(match self {
            Self::BeforeRun(event) => HookEvent::BeforeRun(event),
            Self::BeforeResume(event) => HookEvent::BeforeResume(event),
            Self::BeforeRunEnd(event) => HookEvent::BeforeRunEnd(event),
            Self::TransformContext(event) => HookEvent::TransformContext(event),
            Self::BeforeRequest(event) => HookEvent::BeforeRequest(Box::new(BeforeRequestEvent {
                lane: event.lane,
                run_id: event.run_id,
                model: event
                    .model
                    .into_model(model_policy)
                    .map_err(HookPolicyError::Model)?,
                step: event.step,
                attempt: event.attempt,
                stream_options: event
                    .stream_options
                    .into_durable(provider_policy)
                    .map_err(HookPolicyError::Provider)?,
            })),
            Self::BeforePayload(event) => HookEvent::BeforePayload(Box::new(BeforePayloadEvent {
                lane: event.lane,
                run_id: event.run_id,
                model: event
                    .model
                    .into_model(model_policy)
                    .map_err(HookPolicyError::Model)?,
                payload: event.payload,
            })),
            Self::AfterResponse(event) => HookEvent::AfterResponse(event),
            Self::BeforeTool(event) => HookEvent::BeforeTool(event),
            Self::AfterTool(event) => HookEvent::AfterTool(event),
            Self::BeforeCompaction(event) => HookEvent::BeforeCompaction(event),
            Self::BeforeNavigation(event) => HookEvent::BeforeNavigation(event),
        })
    }
}

/// Callback-free hook result union for routing after typed handler execution.
#[derive(Clone, Debug, PartialEq)]
pub enum HookResult {
    BeforeRun(Option<BeforeRunResult>),
    BeforeResume,
    BeforeRunEnd(Option<BeforeRunEndResult>),
    TransformContext(Option<TransformContextResult>),
    BeforeRequest(Option<BeforeRequestResult>),
    BeforePayload(Option<BeforePayloadResult>),
    AfterResponse(Option<Box<AfterResponseResult>>),
    BeforeTool(Option<BeforeToolResult>),
    AfterTool(Option<AfterToolResult>),
    BeforeCompaction(Option<Box<BeforeCompactionResult>>),
    BeforeNavigation(Option<BeforeNavigationResult>),
}

impl HookResult {
    /// Returns the closed hook name this result belongs to.
    pub const fn name(&self) -> HookName {
        match self {
            Self::BeforeRun(_) => HookName::BeforeRun,
            Self::BeforeResume => HookName::BeforeResume,
            Self::BeforeRunEnd(_) => HookName::BeforeRunEnd,
            Self::TransformContext(_) => HookName::TransformContext,
            Self::BeforeRequest(_) => HookName::BeforeRequest,
            Self::BeforePayload(_) => HookName::BeforePayload,
            Self::AfterResponse(_) => HookName::AfterResponse,
            Self::BeforeTool(_) => HookName::BeforeTool,
            Self::AfterTool(_) => HookName::AfterTool,
            Self::BeforeCompaction(_) => HookName::BeforeCompaction,
            Self::BeforeNavigation(_) => HookName::BeforeNavigation,
        }
    }
}

fn serialize_optional_result<S, T>(
    name: HookName,
    result: &Option<T>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
    T: Serialize,
{
    let mut map = serializer.serialize_map(Some(if result.is_some() { 2 } else { 1 }))?;
    map.serialize_entry("name", &name)?;
    if let Some(result) = result {
        map.serialize_entry("result", result)?;
    }
    map.end()
}

impl Serialize for HookResult {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::BeforeRun(result) => {
                serialize_optional_result(HookName::BeforeRun, result, serializer)
            }
            Self::BeforeResume => {
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("name", &HookName::BeforeResume)?;
                map.end()
            }
            Self::BeforeRunEnd(result) => {
                serialize_optional_result(HookName::BeforeRunEnd, result, serializer)
            }
            Self::TransformContext(result) => {
                serialize_optional_result(HookName::TransformContext, result, serializer)
            }
            Self::BeforeRequest(result) => {
                serialize_optional_result(HookName::BeforeRequest, result, serializer)
            }
            Self::BeforePayload(result) => {
                serialize_optional_result(HookName::BeforePayload, result, serializer)
            }
            Self::AfterResponse(result) => {
                serialize_optional_result(HookName::AfterResponse, result, serializer)
            }
            Self::BeforeTool(result) => {
                serialize_optional_result(HookName::BeforeTool, result, serializer)
            }
            Self::AfterTool(result) => {
                serialize_optional_result(HookName::AfterTool, result, serializer)
            }
            Self::BeforeCompaction(result) => {
                serialize_optional_result(HookName::BeforeCompaction, result, serializer)
            }
            Self::BeforeNavigation(result) => {
                serialize_optional_result(HookName::BeforeNavigation, result, serializer)
            }
        }
    }
}

fn decode_optional_result<T, E>(result: Presence<JsonValue>) -> Result<Option<T>, E>
where
    T: serde::de::DeserializeOwned,
    E: de::Error,
{
    match result {
        Presence::Missing => Ok(None),
        Presence::Null => Err(E::custom("explicit null hook result is invalid")),
        Presence::Value(result) => from_json_value(result).map(Some),
    }
}

impl<'de> Deserialize<'de> for HookResult {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            name: HookName,
            #[serde(default)]
            result: Presence<JsonValue>,
        }

        let wire = Wire::deserialize(deserializer)?;
        match wire.name {
            HookName::BeforeRun => decode_optional_result(wire.result).map(Self::BeforeRun),
            HookName::BeforeResume => {
                if !wire.result.is_missing() {
                    Err(de::Error::custom("before_resume has no result field"))
                } else {
                    Ok(Self::BeforeResume)
                }
            }
            HookName::BeforeRunEnd => decode_optional_result(wire.result).map(Self::BeforeRunEnd),
            HookName::TransformContext => {
                decode_optional_result(wire.result).map(Self::TransformContext)
            }
            HookName::BeforeRequest => decode_optional_result(wire.result).map(Self::BeforeRequest),
            HookName::BeforePayload => decode_optional_result(wire.result).map(Self::BeforePayload),
            HookName::AfterResponse => {
                decode_optional_result(wire.result).map(|result: Option<AfterResponseResult>| {
                    Self::AfterResponse(result.map(Box::new))
                })
            }
            HookName::BeforeTool => decode_optional_result(wire.result).map(Self::BeforeTool),
            HookName::AfterTool => decode_optional_result(wire.result).map(Self::AfterTool),
            HookName::BeforeCompaction => {
                decode_optional_result(wire.result).map(|result: Option<BeforeCompactionResult>| {
                    Self::BeforeCompaction(result.map(Box::new))
                })
            }
            HookName::BeforeNavigation => {
                decode_optional_result(wire.result).map(Self::BeforeNavigation)
            }
        }
    }
}

/// Complete raw hook-result seed for policy-aware containing-root conversion.
#[derive(Clone, Debug, PartialEq)]
pub enum RawHookResult {
    BeforeRun(Option<BeforeRunResult>),
    BeforeResume,
    BeforeRunEnd(Option<BeforeRunEndResult>),
    TransformContext(Option<TransformContextResult>),
    BeforeRequest(Option<RawBeforeRequestResult>),
    BeforePayload(Option<BeforePayloadResult>),
    AfterResponse(Option<Box<AfterResponseResult>>),
    BeforeTool(Option<BeforeToolResult>),
    AfterTool(Option<AfterToolResult>),
    BeforeCompaction(Option<Box<BeforeCompactionResult>>),
    BeforeNavigation(Option<BeforeNavigationResult>),
}

impl RawHookResult {
    /// Classifies nested Harness header patches.
    pub fn into_durable(
        self,
        provider_policy: &ProviderHeaderPolicy,
    ) -> Result<HookResult, ProviderHeaderClassificationError> {
        Ok(match self {
            Self::BeforeRun(result) => HookResult::BeforeRun(result),
            Self::BeforeResume => HookResult::BeforeResume,
            Self::BeforeRunEnd(result) => HookResult::BeforeRunEnd(result),
            Self::TransformContext(result) => HookResult::TransformContext(result),
            Self::BeforeRequest(result) => HookResult::BeforeRequest(
                result
                    .map(|result| {
                        Ok(BeforeRequestResult {
                            stream_options: result
                                .stream_options
                                .map(|options| options.into_durable(provider_policy))
                                .transpose()?,
                        })
                    })
                    .transpose()?,
            ),
            Self::BeforePayload(result) => HookResult::BeforePayload(result),
            Self::AfterResponse(result) => HookResult::AfterResponse(result),
            Self::BeforeTool(result) => HookResult::BeforeTool(result),
            Self::AfterTool(result) => HookResult::AfterTool(result),
            Self::BeforeCompaction(result) => HookResult::BeforeCompaction(result),
            Self::BeforeNavigation(result) => HookResult::BeforeNavigation(result),
        })
    }
}

impl Serialize for RawHookResult {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::BeforeRun(result) => {
                serialize_optional_result(HookName::BeforeRun, result, serializer)
            }
            Self::BeforeResume => {
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("name", &HookName::BeforeResume)?;
                map.end()
            }
            Self::BeforeRunEnd(result) => {
                serialize_optional_result(HookName::BeforeRunEnd, result, serializer)
            }
            Self::TransformContext(result) => {
                serialize_optional_result(HookName::TransformContext, result, serializer)
            }
            Self::BeforeRequest(result) => {
                serialize_optional_result(HookName::BeforeRequest, result, serializer)
            }
            Self::BeforePayload(result) => {
                serialize_optional_result(HookName::BeforePayload, result, serializer)
            }
            Self::AfterResponse(result) => {
                serialize_optional_result(HookName::AfterResponse, result, serializer)
            }
            Self::BeforeTool(result) => {
                serialize_optional_result(HookName::BeforeTool, result, serializer)
            }
            Self::AfterTool(result) => {
                serialize_optional_result(HookName::AfterTool, result, serializer)
            }
            Self::BeforeCompaction(result) => {
                serialize_optional_result(HookName::BeforeCompaction, result, serializer)
            }
            Self::BeforeNavigation(result) => {
                serialize_optional_result(HookName::BeforeNavigation, result, serializer)
            }
        }
    }
}

impl<'de> Deserialize<'de> for RawHookResult {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            name: HookName,
            #[serde(default)]
            result: Presence<JsonValue>,
        }

        let wire = Wire::deserialize(deserializer)?;
        match wire.name {
            HookName::BeforeRun => decode_optional_result(wire.result).map(Self::BeforeRun),
            HookName::BeforeResume => {
                if wire.result.is_missing() {
                    Ok(Self::BeforeResume)
                } else {
                    Err(de::Error::custom("before_resume has no result field"))
                }
            }
            HookName::BeforeRunEnd => decode_optional_result(wire.result).map(Self::BeforeRunEnd),
            HookName::TransformContext => {
                decode_optional_result(wire.result).map(Self::TransformContext)
            }
            HookName::BeforeRequest => decode_optional_result(wire.result).map(Self::BeforeRequest),
            HookName::BeforePayload => decode_optional_result(wire.result).map(Self::BeforePayload),
            HookName::AfterResponse => {
                decode_optional_result(wire.result).map(|result: Option<AfterResponseResult>| {
                    Self::AfterResponse(result.map(Box::new))
                })
            }
            HookName::BeforeTool => decode_optional_result(wire.result).map(Self::BeforeTool),
            HookName::AfterTool => decode_optional_result(wire.result).map(Self::AfterTool),
            HookName::BeforeCompaction => {
                decode_optional_result(wire.result).map(|result: Option<BeforeCompactionResult>| {
                    Self::BeforeCompaction(result.map(Box::new))
                })
            }
            HookName::BeforeNavigation => {
                decode_optional_result(wire.result).map(Self::BeforeNavigation)
            }
        }
    }
}
