//! Closed passive Harness event union.

#![allow(missing_docs)]

use pi_ai_core::{
    AssistantMessageEvent, DeferredHandle, JsonValue, LiteralTrue, Presence, SafeUnsigned,
    ToolCallId, ToolName, ToolResultMessage, Usage,
};
use serde::{Deserialize, Serialize};

use crate::serde_support::{deserialize_optional_non_null, deserialize_required_nullable};
use crate::{
    AgentMessage, AgentThinkingLevel, AgentToolResult, BranchSummaryEntry, CompactionEntry, Entry,
    EntryId, EntryType, LaneId, LaneModel, LiveAgentMessage, MissingIdentities, OperationError,
    OperationId, QueuedItem, SettledAssistantMessage, UsageRow,
};

macro_rules! event_type {
    ($name:ident, $variant:ident, $wire:literal) => {
        #[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
        pub enum $name {
            #[default]
            #[serde(rename = $wire)]
            $variant,
        }
    };
}

event_type!(RunStartEventType, RunStart, "run_start");
event_type!(RunResumeEventType, RunResume, "run_resume");
event_type!(RunSuspendEventType, RunSuspend, "run_suspend");
event_type!(RunAbortEventType, RunAbort, "run_abort");
event_type!(RunEndEventType, RunEnd, "run_end");
event_type!(FaultEventType, Fault, "fault");
event_type!(HandlerErrorEventType, HandlerError, "handler_error");
event_type!(TurnStartEventType, TurnStart, "turn_start");
event_type!(TurnEndEventType, TurnEnd, "turn_end");
event_type!(RetryScheduledEventType, RetryScheduled, "retry_scheduled");
event_type!(RetryStartEventType, RetryStart, "retry_start");
event_type!(RetryEndEventType, RetryEnd, "retry_end");
event_type!(MessageStartEventType, MessageStart, "message_start");
event_type!(MessageUpdateEventType, MessageUpdate, "message_update");
event_type!(MessageEndEventType, MessageEnd, "message_end");
event_type!(ToolStartEventType, ToolStart, "tool_start");
event_type!(ToolUpdateEventType, ToolUpdate, "tool_update");
event_type!(ToolEndEventType, ToolEnd, "tool_end");
event_type!(EntryAddedEventType, EntryAdded, "entry_added");
event_type!(WritePendingEventType, WritePending, "write_pending");
event_type!(QueueUpdateEventType, QueueUpdate, "queue_update");
event_type!(FactUpdateEventType, FactUpdate, "fact_update");
event_type!(ConfigUpdateEventType, ConfigUpdate, "config_update");
event_type!(
    CompactionStartEventType,
    CompactionStart,
    "compaction_start"
);
event_type!(CompactionEndEventType, CompactionEnd, "compaction_end");
event_type!(
    NavigationStartEventType,
    NavigationStart,
    "navigation_start"
);
event_type!(NavigationEndEventType, NavigationEnd, "navigation_end");
event_type!(LaneCreatedEventType, LaneCreated, "lane_created");
event_type!(UsageEventType, Usage, "usage");

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum DeferredEventReason {
    #[default]
    #[serde(rename = "deferred")]
    Deferred,
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum MissingIdentitiesEventReason {
    #[default]
    #[serde(rename = "missing_identities")]
    MissingIdentities,
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum CompletedEventOutcome {
    #[default]
    #[serde(rename = "completed")]
    Completed,
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum AbortedEventOutcome {
    #[default]
    #[serde(rename = "aborted")]
    Aborted,
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum FailedEventOutcome {
    #[default]
    #[serde(rename = "failed")]
    Failed,
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum DeclinedEventOutcome {
    #[default]
    #[serde(rename = "declined")]
    Declined,
}

macro_rules! simple_lane_event {
    ($name:ident, $event_ty:ty, { $($field:ident : $field_ty:ty),* $(,)? }) => {
        #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
        #[serde(rename_all = "camelCase", deny_unknown_fields)]
        pub struct $name {
            #[serde(rename = "type")]
            pub event_type: $event_ty,
            pub lane: LaneId,
            #[serde(default, deserialize_with = "deserialize_optional_non_null", skip_serializing_if = "Option::is_none")]
            pub recovery: Option<LiteralTrue>,
            $(pub $field: $field_ty,)*
        }
    };
}

simple_lane_event!(RunStartEvent, RunStartEventType, { run_id: OperationId });
simple_lane_event!(RunResumeEvent, RunResumeEventType, { run_id: OperationId });

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RunSuspendDeferredEvent {
    #[serde(rename = "type")]
    pub event_type: RunSuspendEventType,
    pub lane: LaneId,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub recovery: Option<LiteralTrue>,
    pub run_id: OperationId,
    pub reason: DeferredEventReason,
    pub deferred: DeferredHandle,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RunSuspendMissingEvent {
    #[serde(rename = "type")]
    pub event_type: RunSuspendEventType,
    pub lane: LaneId,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub recovery: Option<LiteralTrue>,
    pub run_id: OperationId,
    pub reason: MissingIdentitiesEventReason,
    pub missing: MissingIdentities,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RunAbortEvent {
    #[serde(rename = "type")]
    pub event_type: RunAbortEventType,
    pub lane: LaneId,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub recovery: Option<LiteralTrue>,
    pub run_id: OperationId,
    pub steer: Vec<AgentMessage>,
    pub follow_up: Vec<AgentMessage>,
}

macro_rules! run_end_without_final {
    ($name:ident, $outcome:ty $(, $extra:ident : $extra_ty:ty)?) => {
        #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
        #[serde(rename_all = "camelCase", deny_unknown_fields)]
        pub struct $name {
            #[serde(rename = "type")] pub event_type: RunEndEventType,
            pub lane: LaneId,
            #[serde(default, deserialize_with = "deserialize_optional_non_null", skip_serializing_if = "Option::is_none")] pub recovery: Option<LiteralTrue>,
            pub run_id: OperationId,
            #[serde(deserialize_with = "deserialize_required_nullable")] pub leaf_id: Option<EntryId>,
            pub outcome: $outcome,
            $(pub $extra: $extra_ty,)?
        }
    };
}

macro_rules! run_end_with_final {
    ($name:ident, $outcome:ty $(, $extra:ident : $extra_ty:ty)?) => {
        #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
        #[serde(rename_all = "camelCase", deny_unknown_fields)]
        pub struct $name {
            #[serde(rename = "type")] pub event_type: RunEndEventType,
            pub lane: LaneId,
            #[serde(default, deserialize_with = "deserialize_optional_non_null", skip_serializing_if = "Option::is_none")] pub recovery: Option<LiteralTrue>,
            pub run_id: OperationId,
            #[serde(deserialize_with = "deserialize_required_nullable")] pub leaf_id: Option<EntryId>,
            pub outcome: $outcome,
            $(pub $extra: $extra_ty,)?
            pub final_entry_id: EntryId,
            pub final_message: SettledAssistantMessage,
        }
    };
}

run_end_without_final!(RunEndCompletedWithoutFinalEvent, CompletedEventOutcome);
run_end_with_final!(RunEndCompletedWithFinalEvent, CompletedEventOutcome);
run_end_without_final!(RunEndAbortedWithoutFinalEvent, AbortedEventOutcome);
run_end_with_final!(RunEndAbortedWithFinalEvent, AbortedEventOutcome);
run_end_without_final!(RunEndFailedWithoutFinalEvent, FailedEventOutcome, error: OperationError);
run_end_with_final!(RunEndFailedWithFinalEvent, FailedEventOutcome, error: OperationError);

simple_lane_event!(TurnStartEvent, TurnStartEventType, { run_id: OperationId, turn_id: String });

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TurnEndEvent {
    #[serde(rename = "type")]
    pub event_type: TurnEndEventType,
    pub lane: LaneId,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub recovery: Option<LiteralTrue>,
    pub run_id: OperationId,
    pub turn_id: String,
    pub message: SettledAssistantMessage,
    pub tool_results: Vec<ToolResultMessage>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RetryScheduledEvent {
    #[serde(rename = "type")]
    pub event_type: RetryScheduledEventType,
    pub lane: LaneId,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub recovery: Option<LiteralTrue>,
    pub run_id: OperationId,
    pub step: String,
    pub attempt: crate::OneBasedSafeUnsigned,
    pub max_attempts: crate::OneBasedSafeUnsigned,
    pub delay_ms: SafeUnsigned,
    pub error_message: String,
}

simple_lane_event!(RetryStartEvent, RetryStartEventType, { run_id: OperationId, step: String, attempt: crate::OneBasedSafeUnsigned });

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RetryEndEvent {
    #[serde(rename = "type")]
    pub event_type: RetryEndEventType,
    pub lane: LaneId,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub recovery: Option<LiteralTrue>,
    pub run_id: OperationId,
    pub step: String,
    pub attempt: crate::OneBasedSafeUnsigned,
    pub success: bool,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub final_error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MessageStartEvent {
    #[serde(rename = "type")]
    pub event_type: MessageStartEventType,
    pub lane: LaneId,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub recovery: Option<LiteralTrue>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub run_id: Option<OperationId>,
    pub message: LiveAgentMessage,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MessageUpdateEvent {
    #[serde(rename = "type")]
    pub event_type: MessageUpdateEventType,
    pub lane: LaneId,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub recovery: Option<LiteralTrue>,
    pub run_id: OperationId,
    pub message: LiveAgentMessage,
    pub event: AssistantMessageEvent,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MessageEndEvent {
    #[serde(rename = "type")]
    pub event_type: MessageEndEventType,
    pub lane: LaneId,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub recovery: Option<LiteralTrue>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub run_id: Option<OperationId>,
    pub message: AgentMessage,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub entry_id: Option<EntryId>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ToolStartEvent {
    #[serde(rename = "type")]
    pub event_type: ToolStartEventType,
    pub lane: LaneId,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub recovery: Option<LiteralTrue>,
    pub run_id: OperationId,
    pub turn_id: String,
    pub tool_call_id: ToolCallId,
    pub tool_name: ToolName,
    pub args: JsonValue,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ToolUpdateEvent {
    #[serde(rename = "type")]
    pub event_type: ToolUpdateEventType,
    pub lane: LaneId,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub recovery: Option<LiteralTrue>,
    pub run_id: OperationId,
    pub turn_id: String,
    pub tool_call_id: ToolCallId,
    pub tool_name: ToolName,
    pub partial_result: AgentToolResult,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ToolEndEvent {
    #[serde(rename = "type")]
    pub event_type: ToolEndEventType,
    pub lane: LaneId,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub recovery: Option<LiteralTrue>,
    pub run_id: OperationId,
    pub turn_id: String,
    pub tool_call_id: ToolCallId,
    pub tool_name: ToolName,
    pub result: AgentToolResult,
    pub is_error: bool,
    pub terminate: bool,
}

simple_lane_event!(EntryAddedEvent, EntryAddedEventType, { entry: Entry });
simple_lane_event!(WritePendingEvent, WritePendingEventType, { run_id: OperationId, entry_id: EntryId, entry_type: EntryType });

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct QueueUpdateEvent {
    #[serde(rename = "type")]
    pub event_type: QueueUpdateEventType,
    pub lane: LaneId,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub recovery: Option<LiteralTrue>,
    pub steer: Vec<QueuedItem>,
    pub follow_up: Vec<QueuedItem>,
    pub next_run: Vec<QueuedItem>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum ModelConfigProperty {
    #[default]
    #[serde(rename = "model")]
    Model,
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum ThinkingConfigProperty {
    #[default]
    #[serde(rename = "thinkingLevel")]
    ThinkingLevel,
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum ActiveToolsConfigProperty {
    #[default]
    #[serde(rename = "activeTools")]
    ActiveTools,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ModelConfigUpdateEvent {
    #[serde(rename = "type")]
    pub event_type: ConfigUpdateEventType,
    pub lane: LaneId,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub recovery: Option<LiteralTrue>,
    pub property: ModelConfigProperty,
    pub value: LaneModel,
    pub previous: JsonValue,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ThinkingConfigUpdateEvent {
    #[serde(rename = "type")]
    pub event_type: ConfigUpdateEventType,
    pub lane: LaneId,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub recovery: Option<LiteralTrue>,
    pub property: ThinkingConfigProperty,
    pub value: AgentThinkingLevel,
    pub previous: AgentThinkingLevel,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ActiveToolsConfigUpdateEvent {
    #[serde(rename = "type")]
    pub event_type: ConfigUpdateEventType,
    pub lane: LaneId,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub recovery: Option<LiteralTrue>,
    pub property: ActiveToolsConfigProperty,
    pub value: Vec<ToolName>,
    pub previous: Vec<ToolName>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum CompactionEventReason {
    #[serde(rename = "manual")]
    Manual,
    #[serde(rename = "threshold")]
    Threshold,
    #[serde(rename = "overflow")]
    Overflow,
}

simple_lane_event!(CompactionStartEvent, CompactionStartEventType, { run_id: OperationId, reason: CompactionEventReason });

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CompactionEndCompletedEvent {
    #[serde(rename = "type")]
    pub event_type: CompactionEndEventType,
    pub lane: LaneId,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub recovery: Option<LiteralTrue>,
    pub run_id: OperationId,
    pub reason: CompactionEventReason,
    pub outcome: CompletedEventOutcome,
    pub entry: CompactionEntry,
    pub from_hook: bool,
}

macro_rules! compaction_end_simple {
    ($name:ident, $outcome:ty $(, $extra:ident : $extra_ty:ty)?) => {
        #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
        #[serde(rename_all = "camelCase", deny_unknown_fields)]
        pub struct $name {
            #[serde(rename = "type")] pub event_type: CompactionEndEventType,
            pub lane: LaneId,
            #[serde(default, deserialize_with = "deserialize_optional_non_null", skip_serializing_if = "Option::is_none")] pub recovery: Option<LiteralTrue>,
            pub run_id: OperationId,
            pub reason: CompactionEventReason,
            pub outcome: $outcome,
            $(pub $extra: $extra_ty,)?
        }
    };
}
compaction_end_simple!(CompactionEndDeclinedEvent, DeclinedEventOutcome);
compaction_end_simple!(CompactionEndAbortedEvent, AbortedEventOutcome);
compaction_end_simple!(CompactionEndFailedEvent, FailedEventOutcome, error: OperationError);

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NavigationStartEvent {
    #[serde(rename = "type")]
    pub event_type: NavigationStartEventType,
    pub lane: LaneId,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub recovery: Option<LiteralTrue>,
    pub run_id: OperationId,
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub target_id: Option<EntryId>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NavigationEndCompletedEvent {
    #[serde(rename = "type")]
    pub event_type: NavigationEndEventType,
    pub lane: LaneId,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub recovery: Option<LiteralTrue>,
    pub run_id: OperationId,
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub old_leaf_id: Option<EntryId>,
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub new_leaf_id: Option<EntryId>,
    pub outcome: CompletedEventOutcome,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub summary_entry: Option<BranchSummaryEntry>,
}

macro_rules! navigation_end_simple {
    ($name:ident, $outcome:ty $(, $extra:ident : $extra_ty:ty)?) => {
        #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
        #[serde(rename_all = "camelCase", deny_unknown_fields)]
        pub struct $name {
            #[serde(rename = "type")] pub event_type: NavigationEndEventType,
            pub lane: LaneId,
            #[serde(default, deserialize_with = "deserialize_optional_non_null", skip_serializing_if = "Option::is_none")] pub recovery: Option<LiteralTrue>,
            pub run_id: OperationId,
            #[serde(deserialize_with = "deserialize_required_nullable")] pub old_leaf_id: Option<EntryId>,
            #[serde(deserialize_with = "deserialize_required_nullable")] pub new_leaf_id: Option<EntryId>,
            pub outcome: $outcome,
            $(pub $extra: $extra_ty,)?
        }
    };
}
navigation_end_simple!(NavigationEndDeclinedEvent, DeclinedEventOutcome);
navigation_end_simple!(NavigationEndAbortedEvent, AbortedEventOutcome);
navigation_end_simple!(NavigationEndFailedEvent, FailedEventOutcome, error: OperationError);

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LaneCreatedEvent {
    #[serde(rename = "type")]
    pub event_type: LaneCreatedEventType,
    pub lane: LaneId,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub recovery: Option<LiteralTrue>,
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub at: Option<EntryId>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum HookHandlerKind {
    #[default]
    #[serde(rename = "hook")]
    Hook,
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum EventHandlerKind {
    #[default]
    #[serde(rename = "event")]
    Event,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LaneHookHandlerErrorEvent {
    #[serde(rename = "type")]
    pub event_type: HandlerErrorEventType,
    pub lane: LaneId,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub recovery: Option<LiteralTrue>,
    pub error: String,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub stack: Option<String>,
    pub kind: HookHandlerKind,
    pub hook: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LaneEventHandlerErrorEvent {
    #[serde(rename = "type")]
    pub event_type: HandlerErrorEventType,
    pub lane: LaneId,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub recovery: Option<LiteralTrue>,
    pub error: String,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub stack: Option<String>,
    pub kind: EventHandlerKind,
    pub event: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FaultEvent {
    #[serde(rename = "type")]
    pub event_type: FaultEventType,
    pub code: String,
    pub message: String,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum NameFact {
    #[default]
    #[serde(rename = "name")]
    Name,
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum LabelFact {
    #[default]
    #[serde(rename = "label")]
    Label,
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum CustomFact {
    #[default]
    #[serde(rename = "custom")]
    Custom,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NameFactUpdateEvent {
    #[serde(rename = "type")]
    pub event_type: FactUpdateEventType,
    pub fact: NameFact,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub name: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LabelFactUpdateEvent {
    #[serde(rename = "type")]
    pub event_type: FactUpdateEventType,
    pub fact: LabelFact,
    pub target_id: EntryId,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub label: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CustomFactUpdateEvent {
    #[serde(rename = "type")]
    pub event_type: FactUpdateEventType,
    pub fact: CustomFact,
    pub key: String,
    #[serde(default, skip_serializing_if = "Presence::is_missing")]
    pub value: Presence<JsonValue>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum GlobalConfigProperty {
    #[serde(rename = "tools")]
    Tools,
    #[serde(rename = "resources")]
    Resources,
    #[serde(rename = "streamOptions")]
    StreamOptions,
    #[serde(rename = "retryPolicy")]
    RetryPolicy,
    #[serde(rename = "compactionSettings")]
    CompactionSettings,
    #[serde(rename = "steeringMode")]
    SteeringMode,
    #[serde(rename = "followUpMode")]
    FollowUpMode,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GlobalConfigUpdateEvent {
    #[serde(rename = "type")]
    pub event_type: ConfigUpdateEventType,
    pub property: GlobalConfigProperty,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GlobalHookHandlerErrorEvent {
    #[serde(rename = "type")]
    pub event_type: HandlerErrorEventType,
    pub error: String,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub stack: Option<String>,
    pub kind: HookHandlerKind,
    pub hook: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GlobalEventHandlerErrorEvent {
    #[serde(rename = "type")]
    pub event_type: HandlerErrorEventType,
    pub error: String,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub stack: Option<String>,
    pub kind: EventHandlerKind,
    pub event: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UsageEvent {
    #[serde(rename = "type")]
    pub event_type: UsageEventType,
    pub lane: LaneId,
    pub row: UsageRow,
    pub totals: Usage,
}

/// Complete closed Harness event union. Concrete variants encode lane and recovery rules structurally.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum HarnessEvent {
    RunStart(RunStartEvent),
    RunResume(RunResumeEvent),
    RunSuspendDeferred(RunSuspendDeferredEvent),
    RunSuspendMissing(RunSuspendMissingEvent),
    RunAbort(RunAbortEvent),
    RunEndCompletedWithFinal(Box<RunEndCompletedWithFinalEvent>),
    RunEndCompletedWithoutFinal(RunEndCompletedWithoutFinalEvent),
    RunEndAbortedWithFinal(Box<RunEndAbortedWithFinalEvent>),
    RunEndAbortedWithoutFinal(RunEndAbortedWithoutFinalEvent),
    RunEndFailedWithFinal(Box<RunEndFailedWithFinalEvent>),
    RunEndFailedWithoutFinal(RunEndFailedWithoutFinalEvent),
    TurnStart(TurnStartEvent),
    TurnEnd(Box<TurnEndEvent>),
    RetryScheduled(RetryScheduledEvent),
    RetryStart(RetryStartEvent),
    RetryEnd(RetryEndEvent),
    MessageStart(Box<MessageStartEvent>),
    MessageUpdate(Box<MessageUpdateEvent>),
    MessageEnd(Box<MessageEndEvent>),
    ToolStart(ToolStartEvent),
    ToolUpdate(ToolUpdateEvent),
    ToolEnd(ToolEndEvent),
    EntryAdded(EntryAddedEvent),
    WritePending(WritePendingEvent),
    QueueUpdate(QueueUpdateEvent),
    ModelConfigUpdate(ModelConfigUpdateEvent),
    ThinkingConfigUpdate(ThinkingConfigUpdateEvent),
    ActiveToolsConfigUpdate(ActiveToolsConfigUpdateEvent),
    CompactionStart(CompactionStartEvent),
    CompactionEndCompleted(Box<CompactionEndCompletedEvent>),
    CompactionEndDeclined(CompactionEndDeclinedEvent),
    CompactionEndAborted(CompactionEndAbortedEvent),
    CompactionEndFailed(CompactionEndFailedEvent),
    NavigationStart(NavigationStartEvent),
    NavigationEndCompleted(Box<NavigationEndCompletedEvent>),
    NavigationEndDeclined(NavigationEndDeclinedEvent),
    NavigationEndAborted(NavigationEndAbortedEvent),
    NavigationEndFailed(NavigationEndFailedEvent),
    LaneCreated(LaneCreatedEvent),
    LaneHookHandlerError(LaneHookHandlerErrorEvent),
    LaneEventHandlerError(LaneEventHandlerErrorEvent),
    Fault(FaultEvent),
    NameFactUpdate(NameFactUpdateEvent),
    LabelFactUpdate(LabelFactUpdateEvent),
    CustomFactUpdate(CustomFactUpdateEvent),
    GlobalConfigUpdate(GlobalConfigUpdateEvent),
    GlobalHookHandlerError(GlobalHookHandlerErrorEvent),
    GlobalEventHandlerError(GlobalEventHandlerErrorEvent),
    Usage(UsageEvent),
}

/// Exact unique event discriminant inventory.
pub const HARNESS_EVENT_TYPES: &[&str] = &[
    "run_start",
    "run_resume",
    "run_suspend",
    "run_abort",
    "run_end",
    "fault",
    "handler_error",
    "turn_start",
    "turn_end",
    "retry_scheduled",
    "retry_start",
    "retry_end",
    "message_start",
    "message_update",
    "message_end",
    "tool_start",
    "tool_update",
    "tool_end",
    "entry_added",
    "write_pending",
    "queue_update",
    "fact_update",
    "config_update",
    "compaction_start",
    "compaction_end",
    "navigation_start",
    "navigation_end",
    "lane_created",
    "usage",
];

/// Closed unique Harness event type used for listener registration.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum HarnessEventType {
    RunStart,
    RunResume,
    RunSuspend,
    RunAbort,
    RunEnd,
    Fault,
    HandlerError,
    TurnStart,
    TurnEnd,
    RetryScheduled,
    RetryStart,
    RetryEnd,
    MessageStart,
    MessageUpdate,
    MessageEnd,
    ToolStart,
    ToolUpdate,
    ToolEnd,
    EntryAdded,
    WritePending,
    QueueUpdate,
    FactUpdate,
    ConfigUpdate,
    CompactionStart,
    CompactionEnd,
    NavigationStart,
    NavigationEnd,
    LaneCreated,
    Usage,
}
