//! Public lane, operation, queue, and session snapshots.

use pi_ai_core::{AssistantMessage, DeferredHandle, JsonValue};
use serde::{Deserialize, Serialize};

use crate::serde_support::{deserialize_optional_non_null, deserialize_required_nullable};
use crate::{
    AgentMessage, AgentToolResult, CustomType, Entry, EntryId, EntryType, LaneId,
    MissingIdentities, OperationId, OperationKind, RetryInfo,
};

/// One queued message and its reserved entry identity.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct QueuedItem {
    /// Reserved entry identity.
    pub entry_id: EntryId,
    /// Settled queued message.
    pub message: AgentMessage,
}

/// Literal pending message write type.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum PendingMessageWriteType {
    /// Pending message.
    #[default]
    #[serde(rename = "message")]
    Message,
}

/// Literal pending custom write type.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum PendingCustomWriteType {
    /// Pending custom entry.
    #[default]
    #[serde(rename = "custom")]
    Custom,
}

/// Pending message snapshot.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PendingMessageWrite {
    /// Reserved entry identity.
    pub entry_id: EntryId,
    /// Literal message type.
    #[serde(rename = "type")]
    pub entry_type: PendingMessageWriteType,
    /// Pending message payload.
    pub message: AgentMessage,
}

/// Pending custom-entry snapshot.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PendingCustomWrite {
    /// Reserved entry identity.
    pub entry_id: EntryId,
    /// Literal custom type.
    #[serde(rename = "type")]
    pub entry_type: PendingCustomWriteType,
    /// Optional open custom subtype.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub custom_type: Option<CustomType>,
    /// Optional application data, preserving missing versus null.
    #[serde(default, skip_serializing_if = "pi_ai_core::Presence::is_missing")]
    pub data: pi_ai_core::Presence<JsonValue>,
}

/// Structurally exclusive pending write snapshot.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PendingWrite {
    /// Pending message.
    Message(PendingMessageWrite),
    /// Pending custom entry.
    Custom(PendingCustomWrite),
}

/// Process-local running tool projection.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RunningToolSnapshot {
    /// Provider tool-call identity.
    pub tool_call_id: pi_ai_core::ToolCallId,
    /// Tool name.
    pub tool_name: pi_ai_core::ToolName,
    /// Effective arguments.
    pub args: JsonValue,
    /// Optional partial result.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub partial_result: Option<AgentToolResult>,
}

/// Public operation status.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum OperationSnapshotStatus {
    /// Ordinary execution.
    #[serde(rename = "running")]
    Running,
    /// Awaiting explicit resume or missing identities.
    #[serde(rename = "suspended")]
    Suspended,
    /// Durable cancellation is reconciling.
    #[serde(rename = "aborting")]
    Aborting,
}

/// Why an open operation is exposed as suspended.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SuspendedOperationReason {
    /// Process loss left an open operation.
    #[serde(rename = "crash")]
    Crash,
    /// A provider deferred response awaits another poll permit.
    #[serde(rename = "deferred")]
    Deferred,
    /// Captured identities are unavailable.
    #[serde(rename = "missing_identities")]
    MissingIdentities,
}

/// Drained payloads retained through abort reconciliation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AbortingSnapshot {
    /// Drained steering messages.
    pub steer: Vec<AgentMessage>,
    /// Drained follow-up messages.
    pub follow_up: Vec<AgentMessage>,
}

/// Descriptor for one open suspended operation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SuspendedOperation {
    /// Owning lane.
    pub lane: LaneId,
    /// Durable operation identity.
    pub operation_id: OperationId,
    /// Operation kind.
    pub kind: OperationKind,
    /// Suspension reason.
    pub reason: SuspendedOperationReason,
    /// Acceptance timestamp in Unix milliseconds.
    pub started_at: pi_ai_core::SafeUnsigned,
    /// Optional normalized prompt.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub prompt: Option<Vec<AgentMessage>>,
    /// Optional deferred handle.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub deferred: Option<DeferredHandle>,
    /// Optional abort-drained payloads.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub aborting: Option<AbortingSnapshot>,
    /// Required complete missing-identity lists.
    pub missing: MissingIdentities,
}

/// Active operation projection inside a lane snapshot.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OperationSnapshot {
    /// Durable operation identity.
    pub id: OperationId,
    /// Operation kind.
    pub kind: OperationKind,
    /// Derived runtime status.
    pub status: OperationSnapshotStatus,
    /// Acceptance timestamp in Unix milliseconds.
    pub started_at: pi_ai_core::SafeUnsigned,
    /// Optional suspension descriptor.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub suspended: Option<SuspendedOperation>,
    /// Optional process-local assistant draft.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub streaming_message: Option<AssistantMessage>,
    /// Process-local running tools.
    pub running_tools: Vec<RunningToolSnapshot>,
    /// Optional durable retry wait projection.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub retry: Option<RetryInfo>,
}

/// All three lane queues.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LaneQueuesSnapshot {
    /// Steering queue.
    pub steer: Vec<QueuedItem>,
    /// Follow-up queue.
    pub follow_up: Vec<QueuedItem>,
    /// Next-run queue.
    pub next_run: Vec<QueuedItem>,
}

/// Complete public lane snapshot.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LaneSnapshot {
    /// Lane name.
    pub lane: LaneId,
    /// Context-window transcript including its boundary compaction.
    pub transcript: Vec<Entry>,
    /// Current leaf, or root when null.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub leaf_id: Option<EntryId>,
    /// Active operation, or null when idle.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub operation: Option<OperationSnapshot>,
    /// Pending queues.
    pub queues: LaneQueuesSnapshot,
    /// Pending deferred tree writes.
    pub pending_writes: Vec<PendingWrite>,
    /// Whether the owning Harness is faulted.
    pub faulted: bool,
}

/// Compact active-operation projection for lane inventory.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LaneOperationInfo {
    /// Durable operation identity.
    pub id: OperationId,
    /// Operation kind.
    pub kind: OperationKind,
    /// Derived runtime status.
    pub status: OperationSnapshotStatus,
}

/// Public lane inventory item.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LaneInfo {
    /// Lane name.
    pub name: LaneId,
    /// Current leaf, or root when null.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub leaf_id: Option<EntryId>,
    /// Current operation summary, or null when idle.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub operation: Option<LaneOperationInfo>,
}

/// One lane in a session-wide snapshot.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SessionLaneSnapshot {
    /// Lane name.
    pub name: LaneId,
    /// Current leaf, or root when null.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub leaf_id: Option<EntryId>,
    /// Current operation summary, or null when idle.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub operation: Option<LaneOperationInfo>,
    /// Optional suspension descriptor.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub suspended: Option<SuspendedOperation>,
}

/// Harness-wide session snapshot.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionSnapshot {
    /// Lane inventory.
    pub lanes: Vec<SessionLaneSnapshot>,
    /// Whether the Harness is faulted.
    pub faulted: bool,
}

/// Closed pending-write type inventory.
pub const PENDING_WRITE_TYPES: &[EntryType] = &[EntryType::Message, EntryType::Custom];
