use std::sync::Arc;

use pi_ai_core::{AssistantMessageEvent, JsonObject, ToolResultMessage};
use serde::{Deserialize, Serialize};

use crate::{AgentError, AgentMessage, AgentToolResult, BoxFuture, CancellationToken};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
pub enum AgentEvent {
    AgentStart,
    AgentEnd {
        messages: Vec<AgentMessage>,
    },
    TurnStart,
    TurnEnd {
        message: AgentMessage,
        tool_results: Vec<ToolResultMessage>,
    },
    MessageStart {
        message: AgentMessage,
    },
    MessageUpdate {
        message: AgentMessage,
        assistant_message_event: AssistantMessageEvent,
    },
    MessageEnd {
        message: AgentMessage,
    },
    ToolExecutionStart {
        tool_call_id: String,
        tool_name: String,
        args: JsonObject,
    },
    ToolExecutionUpdate {
        tool_call_id: String,
        tool_name: String,
        args: JsonObject,
        partial_result: AgentToolResult,
    },
    ToolExecutionEnd {
        tool_call_id: String,
        tool_name: String,
        result: AgentToolResult,
        is_error: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        duration_ms: Option<u64>,
    },
}

/// Acknowledgement is an event barrier, including AgentEnd.
pub type AgentEventSink = Arc<
    dyn Fn(AgentEvent, CancellationToken) -> BoxFuture<'static, Result<(), AgentError>>
        + Send
        + Sync,
>;
