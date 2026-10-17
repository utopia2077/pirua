//! Detached, language-neutral Coding hook boundaries. Callbacks never hold session locks.
use pi_agent::{
    AfterToolCallContext, AfterToolCallResult, AgentError, AgentEvent, AgentMessage,
    BeforeToolCallContext, BeforeToolCallResult, BoxFuture, CancellationToken,
};
use pi_ai::{AssistantMessage, ImageContent, Model, ModelThinkingLevel, ToolResultMessage, Usage};
use pi_coding_session::{ContentReplacement, SessionEntry, SessionSnapshot};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MessagePhase {
    Start,
    End,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum InputSource {
    Interactive,
    Extension,
    Rpc,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Delivery {
    Steer,
    FollowUp,
    NextTurn,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InputEvent {
    pub text: String,
    pub images: Vec<ImageContent>,
    pub source: InputSource,
    pub delivery: Option<Delivery>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(
    tag = "action",
    rename_all = "lowercase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum InputResult {
    Continue,
    Handled,
    Transform {
        text: String,
        images: Option<Vec<ImageContent>>,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BeforeAgentStart {
    pub message: AgentMessage,
    pub system_prompt: String,
    pub active_tools: Vec<String>,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct BeforeAgentStartResult {
    pub messages: Vec<AgentMessage>,
    pub system_prompt: Option<String>,
    pub active_tools: Option<Vec<String>>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BoundaryKind {
    TurnEnd,
    AgentBeforeSettle,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "snake_case",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum SessionBoundaryDraft {
    Custom {
        custom_type: String,
        data: Option<Value>,
    },
    CustomMessage {
        custom_type: String,
        content: Value,
        display: bool,
        details: Option<Value>,
    },
    ContextEdit {
        target_id: String,
        replacement: Option<ContentReplacement>,
    },
    Compaction {
        summary: String,
        first_kept_entry_id: Option<String>,
        details: Option<Value>,
        usage: Option<Usage>,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BoundaryContext {
    pub kind: BoundaryKind,
    pub turn_index: usize,
    pub message: Option<AssistantMessage>,
    pub tool_results: Vec<ToolResultMessage>,
    pub message_entry_id: Option<String>,
    pub tool_result_entry_ids: Vec<String>,
    pub snapshot: SessionSnapshot,
    pub context_messages: Vec<AgentMessage>,
    pub pending_messages: Vec<AgentMessage>,
    pub can_continue: bool,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct BoundaryResult {
    pub entries: Vec<SessionBoundaryDraft>,
    pub continue_run: bool,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CompactionReason {
    Manual,
    Threshold,
    Overflow,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SummaryResult {
    pub summary: String,
    pub details: Option<Value>,
    pub usage: Option<Usage>,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct BeforeCompactionResult {
    pub cancel: bool,
    pub compaction: Option<crate::CompactionResult>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TreePreparation {
    pub target_id: String,
    pub old_leaf_id: Option<String>,
    pub common_ancestor_id: Option<String>,
    pub entries_to_summarize: Vec<SessionEntry>,
    pub summarize: bool,
    pub custom_instructions: Option<String>,
    pub replace_instructions: bool,
    pub label: Option<String>,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct BeforeTreeResult {
    pub cancel: bool,
    pub summary: Option<SummaryResult>,
    pub custom_instructions: Option<String>,
    pub replace_instructions: Option<bool>,
    pub label: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum SessionEvent {
    Agent(AgentEvent),
    AgentEnd {
        messages: Vec<AgentMessage>,
        will_retry: bool,
    },
    BashExecutionUpdate {
        id: Option<String>,
        delta: String,
    },
    NestedTool {
        parent_tool_call_id: String,
        event: AgentEvent,
    },
    EntryAppended {
        entry: SessionEntry,
    },
    QueueUpdate {
        steering: Vec<String>,
        follow_up: Vec<String>,
    },
    AgentSettled {
        outcome: ActivityOutcome,
    },
    SessionStart {
        session_id: String,
    },
    SessionSwitch {
        previous_session_id: String,
        session_id: String,
    },
    SessionFork {
        previous_session_id: String,
        session_id: String,
    },
    SessionInfoChanged {
        name: Option<String>,
    },
    SessionTree {
        old_leaf_id: Option<String>,
        new_leaf_id: Option<String>,
        summary_entry: Option<SessionEntry>,
    },
    ModelSelect {
        model: Box<Model>,
        previous_model: Box<Model>,
    },
    ThinkingLevelSelect {
        level: ModelThinkingLevel,
    },
    CompactionStart {
        reason: CompactionReason,
        will_retry: bool,
    },
    CompactionEnd {
        reason: CompactionReason,
        result: Option<crate::CompactionResult>,
        aborted: bool,
        will_retry: bool,
        error: Option<String>,
    },
    SessionCompact {
        entry: SessionEntry,
        reason: CompactionReason,
        from_hook: bool,
    },
    AutoRetryStart {
        attempt: u64,
        max_attempts: u64,
        delay_ms: u64,
        error_message: String,
    },
    AutoRetryEnd {
        success: bool,
        attempt: u64,
        final_error: Option<String>,
    },
    CacheWarmed {
        entry: SessionEntry,
    },
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ActivityOutcome {
    Completed,
    Aborted,
    Error,
}
pub type SessionEventSink = std::sync::Arc<
    dyn Fn(SessionEvent) -> BoxFuture<'static, Result<(), AgentError>> + Send + Sync,
>;

/// Override only the hooks needed by the host. Errors are observable by the caller.
/// `event` runs before public subscribers; finalized message events precede persistence.
#[derive(Clone, Debug)]
pub struct ToolLoadout {
    pub declared: Vec<pi_agent::AgentTool>,
    pub callable: Vec<pi_agent::AgentTool>,
    pub registered: Vec<crate::RegisteredTool>,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct ToolLoadoutChanges {
    pub descriptions: std::collections::BTreeMap<String, String>,
    pub hidden_declarations: Vec<String>,
}
pub trait SessionHooks: Send + Sync {
    /// Called once during construction, before any other session hook. The weak
    /// handle identifies the runtime instance; implementations may retain it for
    /// custody checks. The Agent has not been initialized at this point.
    /// Default host hooks remain shareable across independent sessions.
    fn bind_session(&self, _session: crate::WeakCodingSession) -> Result<(), AgentError> {
        Ok(())
    }
    fn prepare_loadout(
        &self,
        _loadout: ToolLoadout,
        _signal: CancellationToken,
    ) -> BoxFuture<'_, Result<ToolLoadoutChanges, AgentError>> {
        Box::pin(async { Ok(ToolLoadoutChanges::default()) })
    }
    fn input(
        &self,
        _event: InputEvent,
        _signal: CancellationToken,
    ) -> BoxFuture<'_, Result<InputResult, AgentError>> {
        Box::pin(async { Ok(InputResult::Continue) })
    }
    fn before_agent_start(
        &self,
        _event: BeforeAgentStart,
        _signal: CancellationToken,
    ) -> BoxFuture<'_, Result<BeforeAgentStartResult, AgentError>> {
        Box::pin(async { Ok(BeforeAgentStartResult::default()) })
    }
    fn context(
        &self,
        messages: Vec<AgentMessage>,
        _signal: CancellationToken,
    ) -> BoxFuture<'_, Result<Vec<AgentMessage>, AgentError>> {
        Box::pin(async move { Ok(messages) })
    }
    fn transform_message(
        &self,
        _phase: MessagePhase,
        message: AgentMessage,
        _signal: CancellationToken,
    ) -> BoxFuture<'_, Result<AgentMessage, AgentError>> {
        Box::pin(async move { Ok(message) })
    }
    fn before_tool(
        &self,
        _event: BeforeToolCallContext,
        _parent_id: Option<String>,
        _signal: CancellationToken,
    ) -> BoxFuture<'_, Result<Option<BeforeToolCallResult>, AgentError>> {
        Box::pin(async { Ok(None) })
    }
    fn after_tool(
        &self,
        _event: AfterToolCallContext,
        _parent_id: Option<String>,
        _signal: CancellationToken,
    ) -> BoxFuture<'_, Result<Option<AfterToolCallResult>, AgentError>> {
        Box::pin(async { Ok(None) })
    }
    fn boundary(
        &self,
        _event: BoundaryContext,
        _signal: CancellationToken,
    ) -> BoxFuture<'_, Result<BoundaryResult, AgentError>> {
        Box::pin(async { Ok(BoundaryResult::default()) })
    }
    fn before_compact(
        &self,
        _preparation: crate::CompactionPreparation,
        _reason: CompactionReason,
        _signal: CancellationToken,
    ) -> BoxFuture<'_, Result<BeforeCompactionResult, AgentError>> {
        Box::pin(async { Ok(BeforeCompactionResult::default()) })
    }
    fn before_tree(
        &self,
        _preparation: TreePreparation,
        _signal: CancellationToken,
    ) -> BoxFuture<'_, Result<BeforeTreeResult, AgentError>> {
        Box::pin(async { Ok(BeforeTreeResult::default()) })
    }
    fn before_session_change(
        &self,
        _reason: SessionChangeReason,
        _snapshot: SessionSnapshot,
        _signal: CancellationToken,
    ) -> BoxFuture<'_, Result<bool, AgentError>> {
        Box::pin(async { Ok(false) })
    }
    fn cache_warming_decision(
        &self,
        decision: crate::CacheWarmingDecision,
    ) -> BoxFuture<'_, Result<crate::CacheWarmingAction, AgentError>> {
        Box::pin(async move { Ok(decision.action) })
    }
    fn event(&self, _event: SessionEvent) -> BoxFuture<'_, Result<(), AgentError>> {
        Box::pin(async { Ok(()) })
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SessionChangeReason {
    New,
    Switch,
    Fork,
}
#[derive(Default, Serialize, Deserialize)]
pub struct NoopSessionHooks;
impl SessionHooks for NoopSessionHooks {}
