//! Behavior-free lane surface signatures.

use std::sync::Arc;

use pi_ai_core::{ImageContent, JsonValue, Model, Presence, ToolName};
use serde::{Deserialize, Serialize};

use crate::surface::BoxFuture;
use crate::{
    AbortCall, AgentMessage, AgentThinkingLevel, CallbackError, CancelQueuedCall, CompactOptions,
    CompactionCall, EntryId, HandlerError, HarnessEvent, HarnessRuntimeError, LaneId,
    LaneLastResult, LaneSnapshot, NavigateOptions, NavigationCall, NextRunCall, QueueCall,
    RecordUsageCall, RecordUsageRequest, ResumeCall, RunCall, SessionTree,
};

/// Normalized native prompt input.
#[derive(Clone, Debug, PartialEq)]
pub enum PromptInput {
    /// Text plus optional images.
    Text {
        /// User text.
        text: String,
        /// User image blocks.
        images: Vec<ImageContent>,
    },
    /// One or more already-normalized Agent messages.
    Messages(Vec<AgentMessage>),
}

/// Manual-drive action information safe for public projection.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActionInfo {
    /// Open action kind.
    pub kind: String,
    /// Human-readable description.
    pub description: String,
    /// Optional detached details, preserving explicit null.
    #[serde(default, skip_serializing_if = "Presence::is_missing")]
    pub details: Presence<JsonValue>,
}

/// Process-local callback admitted under a lane idle reservation.
pub trait RunWhenIdleTask: Send {
    /// Runs the callback after its reservation starts.
    fn run(self: Box<Self>) -> BoxFuture<'static, Result<(), CallbackError>>;
}

/// One rejected-future channel for `run_when_idle`.
#[non_exhaustive]
pub enum RunWhenIdleError {
    /// The admitted callback rejected without faulting the Harness.
    Callback(CallbackError),
    /// Close or Harness fault prevented/completed the call.
    Runtime(HarnessRuntimeError),
}

impl std::fmt::Debug for RunWhenIdleError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let category = match self {
            Self::Callback(_) => "callback",
            Self::Runtime(_) => "runtime",
        };
        formatter
            .debug_struct("RunWhenIdleError")
            .field("category", &category)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Display for RunWhenIdleError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Callback(error) => error.fmt(formatter),
            Self::Runtime(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for RunWhenIdleError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Callback(error) => Some(error),
            Self::Runtime(error) => Some(error),
        }
    }
}

/// Public single-channel run-when-idle result.
pub type RunWhenIdleCall = Result<(), RunWhenIdleError>;

/// Passive Harness event listener.
pub trait EventListener: Send + Sync {
    /// Receives an isolated event payload.
    fn on_event(&self, event: HarnessEvent) -> BoxFuture<'_, Result<(), HandlerError>>;
}

/// Snapshot-first subscription handle.
pub trait WatchHandle<T>: Send + Sync
where
    T: Clone + Send + Sync + 'static,
{
    /// Returns the atomically captured snapshot.
    fn snapshot(&self) -> T;
    /// Starts ordered buffer flush followed by live delivery.
    fn start(&self, listener: Arc<dyn EventListener>);
    /// Drops the watcher and buffered events.
    fn unsubscribe(&self);
}

/// Behavior-free public lane surface.
pub trait AgentLane: Send + Sync {
    /// Lane name.
    fn name(&self) -> &LaneId;
    /// Current leaf, or root when null.
    fn get_leaf_id(&self) -> BoxFuture<'_, Result<Option<EntryId>, HarnessRuntimeError>>;
    /// Most recent terminal durable result.
    fn get_last_result(&self)
    -> BoxFuture<'_, Result<Option<LaneLastResult>, HarnessRuntimeError>>;
    /// Accepts and drives a normalized prompt.
    fn prompt(&self, input: PromptInput) -> BoxFuture<'_, RunCall>;
    /// Expands a named skill before prompt acceptance.
    fn skill(
        &self,
        name: String,
        additional_instructions: Option<String>,
    ) -> BoxFuture<'_, RunCall>;
    /// Expands a named prompt template before prompt acceptance.
    fn prompt_from_template(&self, name: String, args: Vec<String>) -> BoxFuture<'_, RunCall>;
    /// Starts standalone compaction.
    fn compact(&self, options: CompactOptions) -> BoxFuture<'_, CompactionCall>;
    /// Starts tree navigation.
    fn navigate_tree(
        &self,
        target_id: Option<EntryId>,
        options: NavigateOptions,
    ) -> BoxFuture<'_, NavigationCall>;
    /// Resumes an open operation.
    fn resume(&self) -> BoxFuture<'_, ResumeCall>;
    /// Durably requests operation cancellation.
    fn abort(&self) -> BoxFuture<'_, AbortCall>;
    /// Queues steering input for an active run.
    fn steer(&self, message: AgentMessage) -> BoxFuture<'_, QueueCall>;
    /// Queues follow-up input for an active run.
    fn follow_up(&self, message: AgentMessage) -> BoxFuture<'_, QueueCall>;
    /// Queues lane-owned input for the next run.
    fn next_run(&self, message: AgentMessage) -> BoxFuture<'_, NextRunCall>;
    /// Cancels one queued entry idempotently.
    fn cancel_queued(&self, entry_id: EntryId) -> BoxFuture<'_, CancelQueuedCall>;
    /// Records a caller-supplied usage adjustment.
    fn record_usage(&self, request: RecordUsageRequest) -> BoxFuture<'_, RecordUsageCall>;
    /// Waits for all earlier admitted lane jobs and operation ownership to become idle.
    fn wait_for_idle(&self) -> BoxFuture<'_, Result<(), HarnessRuntimeError>>;
    /// Runs a process-local callback under an idle admission reservation.
    fn run_when_idle(&self, task: Box<dyn RunWhenIdleTask>) -> BoxFuture<'_, RunWhenIdleCall>;
    /// Peeks the stable next manual-drive action.
    fn peek_action(&self) -> BoxFuture<'_, Result<Option<ActionInfo>, HarnessRuntimeError>>;
    /// Releases exactly one manual-drive action.
    fn execute_action(&self) -> BoxFuture<'_, Result<Option<ActionInfo>, HarnessRuntimeError>>;
    /// Drives until suspension or terminal settlement.
    fn run_to_completion(&self) -> BoxFuture<'_, Result<(), HarnessRuntimeError>>;
    /// Resolves the current durable provider/model identity when registered.
    fn get_model(&self) -> BoxFuture<'_, Result<Option<Model>, HarnessRuntimeError>>;
    /// Replaces total lane model configuration.
    fn set_model(&self, model: Model) -> BoxFuture<'_, Result<(), HarnessRuntimeError>>;
    /// Reads the current thinking level.
    fn get_thinking_level(&self) -> BoxFuture<'_, Result<AgentThinkingLevel, HarnessRuntimeError>>;
    /// Replaces the current thinking level.
    fn set_thinking_level(
        &self,
        level: AgentThinkingLevel,
    ) -> BoxFuture<'_, Result<(), HarnessRuntimeError>>;
    /// Reads active model-visible tools.
    fn get_active_tools(&self) -> BoxFuture<'_, Result<Vec<ToolName>, HarnessRuntimeError>>;
    /// Replaces active model-visible tools.
    fn set_active_tools(
        &self,
        names: Vec<ToolName>,
    ) -> BoxFuture<'_, Result<(), HarnessRuntimeError>>;
    /// Returns this lane's session-tree view.
    fn session(&self) -> Arc<dyn SessionTree>;
    /// Creates a snapshot-first lane watch handle.
    fn watch(
        &self,
    ) -> BoxFuture<'_, Result<Arc<dyn WatchHandle<LaneSnapshot>>, HarnessRuntimeError>>;
}

/// Exact public AgentLane method/property inventory.
pub const AGENT_LANE_METHODS: &[&str] = &[
    "name",
    "get_leaf_id",
    "get_last_result",
    "prompt",
    "skill",
    "prompt_from_template",
    "compact",
    "navigate_tree",
    "resume",
    "abort",
    "steer",
    "follow_up",
    "next_run",
    "cancel_queued",
    "record_usage",
    "wait_for_idle",
    "run_when_idle",
    "peek_action",
    "execute_action",
    "run_to_completion",
    "get_model",
    "set_model",
    "get_thinking_level",
    "set_thinking_level",
    "get_active_tools",
    "set_active_tools",
    "session",
    "watch",
];
