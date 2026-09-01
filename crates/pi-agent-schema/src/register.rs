//! Typed durable register namespaces and namespace/value coupling.

use pi_ai_core::{JsonObject, JsonValue, Presence, SafeUnsigned};
use serde::{Deserialize, Serialize};

use crate::config::{
    DurableStructuralPreparation, LaneConfiguration, ProviderHeaderClassificationError,
    ProviderHeaderPolicy,
};
use crate::entry::EntryType;
use crate::id::{CustomType, EntryId};
use crate::operation::{LaneLastResult, LaneState, Operation};
use crate::serde_support::{deserialize_optional_non_null, deserialize_required_nullable};
use crate::state::v1::{OperationState, RawOperationState};

/// Exhaustive register namespace inventory.
pub const REGISTER_NAMESPACES: &[&str] = &[
    "lane.leaf",
    "lane.config",
    "lane.state",
    "lane.lastResult",
    "op.meta",
    "op.state",
    "op.tool_args",
    "op.preparation",
    "pending.entry",
    "fact.name",
    "fact.label",
    "fact.custom",
];

/// Closed register namespace.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum RegisterNamespace {
    /// Lane leaf pointer.
    #[serde(rename = "lane.leaf")]
    LaneLeaf,
    /// Total lane configuration.
    #[serde(rename = "lane.config")]
    LaneConfig,
    /// Total lane state.
    #[serde(rename = "lane.state")]
    LaneState,
    /// Latest terminal lane result.
    #[serde(rename = "lane.lastResult")]
    LaneLastResult,
    /// Operation acceptance metadata.
    #[serde(rename = "op.meta")]
    OperationMeta,
    /// Total physical v1 operation state.
    #[serde(rename = "op.state")]
    OperationState,
    /// Effective tool arguments.
    #[serde(rename = "op.tool_args")]
    OperationToolArguments,
    /// Durable structural preparation.
    #[serde(rename = "op.preparation")]
    OperationPreparation,
    /// Unplaced pending entry content.
    #[serde(rename = "pending.entry")]
    PendingEntry,
    /// Session name fact.
    #[serde(rename = "fact.name")]
    FactName,
    /// Entry label fact.
    #[serde(rename = "fact.label")]
    FactLabel,
    /// Application-defined fact.
    #[serde(rename = "fact.custom")]
    FactCustom,
}

/// Pending entry type.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum PendingEntryType {
    /// Pending Agent message.
    #[serde(rename = "message")]
    Message,
    /// Pending custom entry.
    #[serde(rename = "custom")]
    Custom,
}

/// Unplaced durable entry content.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PendingEntry {
    /// Pending message or custom entry type.
    #[serde(rename = "type")]
    pub entry_type: PendingEntryType,
    /// Optional open custom type.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub custom_type: Option<CustomType>,
    /// Optional content, preserving missing versus null.
    #[serde(default, skip_serializing_if = "Presence::is_missing")]
    pub payload: Presence<JsonValue>,
}

/// Current typed register value, including its last-write sequence.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "namespace", deny_unknown_fields)]
pub enum Register<S = OperationState> {
    /// Lane leaf entry or root.
    #[serde(rename = "lane.leaf")]
    LaneLeaf {
        /// Lane name.
        key: String,
        /// Entry identity or null for root.
        #[serde(deserialize_with = "deserialize_required_nullable")]
        value: Option<EntryId>,
        /// Last-write sequence.
        seq: SafeUnsigned,
    },
    /// Total lane configuration.
    #[serde(rename = "lane.config")]
    LaneConfig {
        /// Lane name.
        key: String,
        /// Total configuration.
        value: LaneConfiguration,
        /// Last-write sequence.
        seq: SafeUnsigned,
    },
    /// Total lane state.
    #[serde(rename = "lane.state")]
    LaneState {
        /// Lane name.
        key: String,
        /// Total lane state.
        value: LaneState,
        /// Last-write sequence.
        seq: SafeUnsigned,
    },
    /// Latest terminal lane result.
    #[serde(rename = "lane.lastResult")]
    LaneLastResult {
        /// Lane name.
        key: String,
        /// Latest terminal result.
        value: LaneLastResult,
        /// Last-write sequence.
        seq: SafeUnsigned,
    },
    /// Write-once operation acceptance metadata.
    #[serde(rename = "op.meta")]
    OperationMeta {
        /// Operation identity string.
        key: String,
        /// Acceptance metadata.
        value: Operation,
        /// Last-write sequence.
        seq: SafeUnsigned,
    },
    /// Total physical v1 operation state.
    #[serde(rename = "op.state")]
    OperationState {
        /// Operation identity string.
        key: String,
        /// Total program counter.
        value: Box<S>,
        /// Last-write sequence.
        seq: SafeUnsigned,
    },
    /// Deterministic effective tool arguments.
    #[serde(rename = "op.tool_args")]
    OperationToolArguments {
        /// Deterministic operation/step/source-index key.
        key: String,
        /// Effective object-only arguments.
        value: JsonObject,
        /// Last-write sequence.
        seq: SafeUnsigned,
    },
    /// Durable structural preparation.
    #[serde(rename = "op.preparation")]
    OperationPreparation {
        /// Deterministic operation/task key.
        key: String,
        /// Normalized structural preparation.
        value: DurableStructuralPreparation,
        /// Last-write sequence.
        seq: SafeUnsigned,
    },
    /// Unplaced pending entry.
    #[serde(rename = "pending.entry")]
    PendingEntry {
        /// Reserved entry identity string.
        key: String,
        /// Pending content.
        value: PendingEntry,
        /// Last-write sequence.
        seq: SafeUnsigned,
    },
    /// Session name fact.
    #[serde(rename = "fact.name")]
    FactName {
        /// Empty string by convention.
        key: String,
        /// Session name.
        value: String,
        /// Last-write sequence.
        seq: SafeUnsigned,
    },
    /// Entry label fact.
    #[serde(rename = "fact.label")]
    FactLabel {
        /// Labeled entry identity string.
        key: String,
        /// Label.
        value: String,
        /// Last-write sequence.
        seq: SafeUnsigned,
    },
    /// Application-defined fact. JSON null is a value, not deletion.
    #[serde(rename = "fact.custom")]
    FactCustom {
        /// Application fact key.
        key: String,
        /// Arbitrary deterministic JSON, including null.
        value: JsonValue,
        /// Last-write sequence.
        seq: SafeUnsigned,
    },
}

/// Namespace/value-coupled register set before storage assigns a sequence.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "namespace", deny_unknown_fields)]
pub enum RegisterSet<S = OperationState> {
    /// Lane leaf entry or root.
    #[serde(rename = "lane.leaf")]
    LaneLeaf {
        /// Lane name.
        key: String,
        /// Entry identity or null for root.
        #[serde(deserialize_with = "deserialize_required_nullable")]
        value: Option<EntryId>,
    },
    /// Total lane configuration.
    #[serde(rename = "lane.config")]
    LaneConfig {
        /// Lane name.
        key: String,
        /// Total configuration.
        value: LaneConfiguration,
    },
    /// Total lane state.
    #[serde(rename = "lane.state")]
    LaneState {
        /// Lane name.
        key: String,
        /// Total lane state.
        value: LaneState,
    },
    /// Latest terminal lane result.
    #[serde(rename = "lane.lastResult")]
    LaneLastResult {
        /// Lane name.
        key: String,
        /// Latest terminal result.
        value: LaneLastResult,
    },
    /// Write-once operation metadata.
    #[serde(rename = "op.meta")]
    OperationMeta {
        /// Operation identity string.
        key: String,
        /// Acceptance metadata.
        value: Operation,
    },
    /// Total physical v1 operation state.
    #[serde(rename = "op.state")]
    OperationState {
        /// Operation identity string.
        key: String,
        /// Total program counter.
        value: Box<S>,
    },
    /// Deterministic effective tool arguments.
    #[serde(rename = "op.tool_args")]
    OperationToolArguments {
        /// Deterministic operation/step/source-index key.
        key: String,
        /// Effective object-only arguments.
        value: JsonObject,
    },
    /// Durable structural preparation.
    #[serde(rename = "op.preparation")]
    OperationPreparation {
        /// Deterministic operation/task key.
        key: String,
        /// Normalized preparation.
        value: DurableStructuralPreparation,
    },
    /// Unplaced pending entry.
    #[serde(rename = "pending.entry")]
    PendingEntry {
        /// Reserved entry identity string.
        key: String,
        /// Pending content.
        value: PendingEntry,
    },
    /// Session name fact.
    #[serde(rename = "fact.name")]
    FactName {
        /// Empty string by convention.
        key: String,
        /// Session name.
        value: String,
    },
    /// Entry label fact.
    #[serde(rename = "fact.label")]
    FactLabel {
        /// Labeled entry identity string.
        key: String,
        /// Label.
        value: String,
    },
    /// Application-defined fact. JSON null remains data.
    #[serde(rename = "fact.custom")]
    FactCustom {
        /// Application fact key.
        key: String,
        /// Arbitrary deterministic JSON, including null.
        value: JsonValue,
    },
}

/// Returns the structural entry type carried by pending content.
pub const fn pending_entry_type(value: &PendingEntry) -> EntryType {
    match value.entry_type {
        PendingEntryType::Message => EntryType::Message,
        PendingEntryType::Custom => EntryType::Custom,
    }
}

/// Register root whose operation state still contains raw stream options.
pub type RawRegister = Register<RawOperationState>;

/// Register-set root whose operation state still contains raw stream options.
pub type RawRegisterSet = RegisterSet<RawOperationState>;

impl Register<RawOperationState> {
    /// Classifies nested raw operation-state headers under one host policy.
    pub fn into_durable(
        self,
        policy: &ProviderHeaderPolicy,
    ) -> Result<Register, ProviderHeaderClassificationError> {
        Ok(match self {
            Self::LaneLeaf { key, value, seq } => Register::LaneLeaf { key, value, seq },
            Self::LaneConfig { key, value, seq } => Register::LaneConfig { key, value, seq },
            Self::LaneState { key, value, seq } => Register::LaneState { key, value, seq },
            Self::LaneLastResult { key, value, seq } => {
                Register::LaneLastResult { key, value, seq }
            }
            Self::OperationMeta { key, value, seq } => Register::OperationMeta { key, value, seq },
            Self::OperationState { key, value, seq } => Register::OperationState {
                key,
                value: Box::new((*value).into_durable(policy)?),
                seq,
            },
            Self::OperationToolArguments { key, value, seq } => {
                Register::OperationToolArguments { key, value, seq }
            }
            Self::OperationPreparation { key, value, seq } => {
                Register::OperationPreparation { key, value, seq }
            }
            Self::PendingEntry { key, value, seq } => Register::PendingEntry { key, value, seq },
            Self::FactName { key, value, seq } => Register::FactName { key, value, seq },
            Self::FactLabel { key, value, seq } => Register::FactLabel { key, value, seq },
            Self::FactCustom { key, value, seq } => Register::FactCustom { key, value, seq },
        })
    }
}

impl RegisterSet<RawOperationState> {
    /// Classifies nested raw operation-state headers under one host policy.
    pub fn into_durable(
        self,
        policy: &ProviderHeaderPolicy,
    ) -> Result<RegisterSet, ProviderHeaderClassificationError> {
        Ok(match self {
            Self::LaneLeaf { key, value } => RegisterSet::LaneLeaf { key, value },
            Self::LaneConfig { key, value } => RegisterSet::LaneConfig { key, value },
            Self::LaneState { key, value } => RegisterSet::LaneState { key, value },
            Self::LaneLastResult { key, value } => RegisterSet::LaneLastResult { key, value },
            Self::OperationMeta { key, value } => RegisterSet::OperationMeta { key, value },
            Self::OperationState { key, value } => RegisterSet::OperationState {
                key,
                value: Box::new((*value).into_durable(policy)?),
            },
            Self::OperationToolArguments { key, value } => {
                RegisterSet::OperationToolArguments { key, value }
            }
            Self::OperationPreparation { key, value } => {
                RegisterSet::OperationPreparation { key, value }
            }
            Self::PendingEntry { key, value } => RegisterSet::PendingEntry { key, value },
            Self::FactName { key, value } => RegisterSet::FactName { key, value },
            Self::FactLabel { key, value } => RegisterSet::FactLabel { key, value },
            Self::FactCustom { key, value } => RegisterSet::FactCustom { key, value },
        })
    }
}
