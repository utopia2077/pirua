//! Immutable conversation entries and append-only usage rows.

use pi_ai_core::{JsonValue, LiteralTrue, Presence, SafeUnsigned, Usage};
use serde::{Deserialize, Serialize};

use crate::id::{CustomType, EntryId, UsageId};
use crate::message::AgentMessage;
use crate::serde_support::{deserialize_optional_non_null, deserialize_required_nullable};

/// Closed durable entry type.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum EntryType {
    /// A durable Agent message.
    #[serde(rename = "message")]
    Message,
    /// A compaction checkpoint.
    #[serde(rename = "compaction")]
    Compaction,
    /// A summary attached after branch navigation.
    #[serde(rename = "branch_summary")]
    BranchSummary,
    /// Application-defined durable data.
    #[serde(rename = "custom")]
    Custom,
}

/// Placement fields shared by every committed entry.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EntryBase {
    /// Entry identity.
    pub id: EntryId,
    /// Parent entry, or root when null.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub parent_id: Option<EntryId>,
    /// Storage-assigned sequence.
    pub seq: SafeUnsigned,
    /// Storage-assigned Unix timestamp in milliseconds.
    pub timestamp: SafeUnsigned,
    /// Closed structural entry type.
    #[serde(rename = "type")]
    pub entry_type: EntryType,
    /// Open custom-entry type when applicable.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub custom_type: Option<CustomType>,
}

/// Literal `message` entry type.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum MessageEntryType {
    /// The `message` value.
    #[default]
    #[serde(rename = "message")]
    Message,
}

/// A committed message entry.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MessageEntry {
    /// Entry identity.
    pub id: EntryId,
    /// Parent entry, or root when null.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub parent_id: Option<EntryId>,
    /// Storage-assigned sequence.
    pub seq: SafeUnsigned,
    /// Storage-assigned Unix timestamp in milliseconds.
    pub timestamp: SafeUnsigned,
    /// Literal entry type.
    #[serde(rename = "type")]
    pub entry_type: MessageEntryType,
    /// Settled built-in or custom Agent message.
    pub message: AgentMessage,
    /// Optional literal-true orchestration termination marker.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub terminate: Option<LiteralTrue>,
}

/// Literal `compaction` entry type.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum CompactionEntryType {
    /// The `compaction` value.
    #[default]
    #[serde(rename = "compaction")]
    Compaction,
}

/// A committed compaction checkpoint.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CompactionEntry {
    /// Entry identity.
    pub id: EntryId,
    /// Parent entry, or root when null.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub parent_id: Option<EntryId>,
    /// Storage-assigned sequence.
    pub seq: SafeUnsigned,
    /// Storage-assigned Unix timestamp in milliseconds.
    pub timestamp: SafeUnsigned,
    /// Literal entry type.
    #[serde(rename = "type")]
    pub entry_type: CompactionEntryType,
    /// Generated or hook-supplied summary.
    pub summary: String,
    /// Retained recent messages, including an explicitly empty tail.
    pub retained_tail: Vec<AgentMessage>,
    /// Estimated tokens before compaction.
    pub tokens_before: SafeUnsigned,
    /// Optional details, preserving missing versus null.
    #[serde(default, skip_serializing_if = "Presence::is_missing")]
    pub details: Presence<JsonValue>,
    /// Optional display usage.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub usage: Option<Usage>,
    /// Whether a hook supplied the compaction result.
    pub from_hook: bool,
}

/// Literal `branch_summary` entry type.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum BranchSummaryEntryType {
    /// The `branch_summary` value.
    #[default]
    #[serde(rename = "branch_summary")]
    BranchSummary,
}

/// A committed branch-summary entry.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BranchSummaryEntry {
    /// Entry identity.
    pub id: EntryId,
    /// Parent entry, or root when null.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub parent_id: Option<EntryId>,
    /// Storage-assigned sequence.
    pub seq: SafeUnsigned,
    /// Storage-assigned Unix timestamp in milliseconds.
    pub timestamp: SafeUnsigned,
    /// Literal entry type.
    #[serde(rename = "type")]
    pub entry_type: BranchSummaryEntryType,
    /// Producing operation's pre-navigation source leaf.
    pub from_id: EntryId,
    /// Generated or hook-supplied summary.
    pub summary: String,
    /// Optional details, preserving missing versus null.
    #[serde(default, skip_serializing_if = "Presence::is_missing")]
    pub details: Presence<JsonValue>,
    /// Optional display usage.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub usage: Option<Usage>,
    /// Whether a hook supplied the branch summary.
    pub from_hook: bool,
}

/// Literal `custom` entry type.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum CustomEntryType {
    /// The `custom` value.
    #[default]
    #[serde(rename = "custom")]
    Custom,
}

/// A committed custom entry.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CustomEntry {
    /// Entry identity.
    pub id: EntryId,
    /// Parent entry, or root when null.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub parent_id: Option<EntryId>,
    /// Storage-assigned sequence.
    pub seq: SafeUnsigned,
    /// Storage-assigned Unix timestamp in milliseconds.
    pub timestamp: SafeUnsigned,
    /// Literal entry type.
    #[serde(rename = "type")]
    pub entry_type: CustomEntryType,
    /// Open application custom type.
    pub custom_type: CustomType,
    /// Optional application data, preserving missing versus null.
    #[serde(default, skip_serializing_if = "Presence::is_missing")]
    pub data: Presence<JsonValue>,
}

/// Closed union of committed durable entries.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Entry {
    /// Message entry.
    Message(MessageEntry),
    /// Compaction entry.
    Compaction(CompactionEntry),
    /// Branch-summary entry.
    BranchSummary(BranchSummaryEntry),
    /// Custom entry.
    Custom(CustomEntry),
}

/// Placement-only entry projection without payload fields.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EntryStructure {
    /// Entry identity.
    pub id: EntryId,
    /// Parent entry, or root when null.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub parent_id: Option<EntryId>,
    /// Storage-assigned sequence.
    pub seq: SafeUnsigned,
    /// Storage-assigned Unix timestamp in milliseconds.
    pub timestamp: SafeUnsigned,
    /// Closed entry type.
    #[serde(rename = "type")]
    pub entry_type: EntryType,
    /// Open custom-entry type when applicable.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub custom_type: Option<CustomType>,
}

/// A new message entry before storage assigns sequence and timestamp.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NewMessageEntry {
    /// Reserved entry identity.
    pub id: EntryId,
    /// Parent entry, or root when null.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub parent_id: Option<EntryId>,
    /// Literal entry type.
    #[serde(rename = "type")]
    pub entry_type: MessageEntryType,
    /// Settled Agent message.
    pub message: AgentMessage,
    /// Optional literal-true orchestration termination marker.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub terminate: Option<LiteralTrue>,
}

/// A new compaction entry before storage assigns sequence and timestamp.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NewCompactionEntry {
    /// Reserved entry identity.
    pub id: EntryId,
    /// Parent entry, or root when null.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub parent_id: Option<EntryId>,
    /// Literal entry type.
    #[serde(rename = "type")]
    pub entry_type: CompactionEntryType,
    /// Summary text.
    pub summary: String,
    /// Retained recent messages.
    pub retained_tail: Vec<AgentMessage>,
    /// Estimated tokens before compaction.
    pub tokens_before: SafeUnsigned,
    /// Optional details, preserving missing versus null.
    #[serde(default, skip_serializing_if = "Presence::is_missing")]
    pub details: Presence<JsonValue>,
    /// Optional display usage.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub usage: Option<Usage>,
    /// Whether a hook supplied the result.
    pub from_hook: bool,
}

/// A new branch-summary entry before storage assigns sequence and timestamp.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NewBranchSummaryEntry {
    /// Reserved entry identity.
    pub id: EntryId,
    /// Parent entry, or root when null.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub parent_id: Option<EntryId>,
    /// Literal entry type.
    #[serde(rename = "type")]
    pub entry_type: BranchSummaryEntryType,
    /// Producing operation's pre-navigation source leaf.
    pub from_id: EntryId,
    /// Summary text.
    pub summary: String,
    /// Optional details, preserving missing versus null.
    #[serde(default, skip_serializing_if = "Presence::is_missing")]
    pub details: Presence<JsonValue>,
    /// Optional display usage.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub usage: Option<Usage>,
    /// Whether a hook supplied the result.
    pub from_hook: bool,
}

/// A new custom entry before storage assigns sequence and timestamp.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NewCustomEntry {
    /// Reserved entry identity.
    pub id: EntryId,
    /// Parent entry, or root when null.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub parent_id: Option<EntryId>,
    /// Literal entry type.
    #[serde(rename = "type")]
    pub entry_type: CustomEntryType,
    /// Open application custom type.
    pub custom_type: CustomType,
    /// Optional application data, preserving missing versus null.
    #[serde(default, skip_serializing_if = "Presence::is_missing")]
    pub data: Presence<JsonValue>,
}

/// Closed union of new entries. It omits only storage-assigned fields.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum NewEntry {
    /// New message entry.
    Message(NewMessageEntry),
    /// New compaction entry.
    Compaction(NewCompactionEntry),
    /// New branch-summary entry.
    BranchSummary(NewBranchSummaryEntry),
    /// New custom entry.
    Custom(NewCustomEntry),
}

/// Append-only committed usage-ledger row.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UsageRow {
    /// Usage-row identity.
    pub id: UsageId,
    /// Storage-assigned sequence.
    pub seq: SafeUnsigned,
    /// Provider, tool, or adjustment usage.
    pub usage: Usage,
    /// Entry associated with this cost, when any.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub entry_id: Option<EntryId>,
    /// Whether this row is caller-supplied reconciliation.
    pub adjustment: bool,
    /// Optional adjustment details, preserving missing versus null.
    #[serde(default, skip_serializing_if = "Presence::is_missing")]
    pub details: Presence<JsonValue>,
}

/// New usage row before storage assigns its sequence.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NewUsageRow {
    /// Reserved usage-row identity.
    pub id: UsageId,
    /// Provider, tool, or adjustment usage.
    pub usage: Usage,
    /// Entry associated with this cost, when any.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub entry_id: Option<EntryId>,
    /// Whether this row is caller-supplied reconciliation.
    pub adjustment: bool,
    /// Optional adjustment details, preserving missing versus null.
    #[serde(default, skip_serializing_if = "Presence::is_missing")]
    pub details: Presence<JsonValue>,
}
