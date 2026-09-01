//! Storage scans, cursors, queries, and session statistics.

use pi_ai_core::{SafeUnsigned, Usage};
use serde::{Deserialize, Serialize};

use crate::serde_support::deserialize_optional_non_null;
use crate::{CustomType, EntryId, EntryType};

/// Exclusive sequence cursor for entry queries.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EntryCursor {
    /// Last observed storage sequence.
    pub seq: SafeUnsigned,
}

/// Branch scan order.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum BranchOrder {
    /// Newest entry first.
    #[serde(rename = "newestFirst")]
    NewestFirst,
    /// Oldest entry first.
    #[serde(rename = "oldestFirst")]
    OldestFirst,
}

/// Session-wide scan order.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ScanOrder {
    /// Ascending sequence order.
    #[serde(rename = "asc")]
    Ascending,
    /// Descending sequence order.
    #[serde(rename = "desc")]
    Descending,
}

/// Query for one branch path.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BranchScan {
    /// Starting entry. The session view may supply its lane leaf when omitted.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub start: Option<EntryId>,
    /// Inclusive stop entry type.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub stop_at_type: Option<EntryType>,
    /// Inclusive stop entry identity.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub stop_at_id: Option<EntryId>,
    /// Entry-type filter.
    #[serde(
        rename = "type",
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub entry_type: Option<EntryType>,
    /// Open custom-type filter.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub custom_type: Option<CustomType>,
    /// Result order.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub order: Option<BranchOrder>,
    /// Maximum returned entries.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub limit: Option<SafeUnsigned>,
    /// Exclusive sequence cursor.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub cursor: Option<EntryCursor>,
}

/// Session-wide committed-entry scan.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EntryScan {
    /// Entry-type filter.
    #[serde(
        rename = "type",
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub entry_type: Option<EntryType>,
    /// Open custom-type filter.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub custom_type: Option<CustomType>,
    /// Inclusive lower sequence bound.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub from_seq: Option<SafeUnsigned>,
    /// Inclusive upper sequence bound.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub to_seq: Option<SafeUnsigned>,
    /// Result order.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub order: Option<ScanOrder>,
    /// Maximum returned entries.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub limit: Option<SafeUnsigned>,
}

/// Append-only usage-ledger scan.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UsageScan {
    /// Inclusive lower sequence bound.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub from_seq: Option<SafeUnsigned>,
    /// Inclusive upper sequence bound.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub to_seq: Option<SafeUnsigned>,
    /// Result order.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub order: Option<ScanOrder>,
    /// Maximum returned rows.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub limit: Option<SafeUnsigned>,
}

/// SessionTree global entry query.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EntryQuery {
    /// Entry-type filter.
    #[serde(
        rename = "type",
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub entry_type: Option<EntryType>,
    /// Open custom-type filter.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub custom_type: Option<CustomType>,
    /// Result order.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub order: Option<ScanOrder>,
    /// Maximum returned entries.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub limit: Option<SafeUnsigned>,
    /// Exclusive sequence cursor.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub cursor: Option<EntryCursor>,
}

/// Maintained session statistics projection.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SessionStats {
    /// Count of message entries only.
    pub message_count: SafeUnsigned,
    /// Sum of the append-only usage ledger.
    pub usage: Usage,
}
