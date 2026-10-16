use std::{fmt, sync::Arc};

use pi_ai_core::{
    AssistantMessage, InputContent, JsonObject, JsonSchema, JsonValue, Tool, ToolCall, Usage,
};
use serde::{Deserialize, Serialize};

use crate::{AgentError, AgentMessage, BoxFuture, CancellationToken};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ToolExecutionMode {
    Sequential,
    #[default]
    Parallel,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ToolReplay {
    #[default]
    Never,
    Safe,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentToolResult {
    pub content: Vec<InputContent>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<JsonValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub structured_content: Option<JsonValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<Usage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_error: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminate: Option<bool>,
}

pub type ToolUpdate = Arc<dyn Fn(AgentToolResult) + Send + Sync>;
pub type ToolExecute = Arc<
    dyn Fn(ToolInvocation) -> BoxFuture<'static, Result<AgentToolResult, AgentError>> + Send + Sync,
>;
pub type PrepareArguments = Arc<dyn Fn(JsonObject) -> Result<JsonObject, AgentError> + Send + Sync>;

#[derive(Clone)]
pub struct ToolInvocation {
    pub call_id: String,
    pub arguments: JsonObject,
    pub cancellation: CancellationToken,
    pub on_update: ToolUpdate,
}

/// Executable capability. The callback has no dependency on Agent scheduler state.
#[derive(Clone)]
pub struct AgentTool {
    pub declaration: Tool,
    pub label: String,
    pub output_schema: Option<JsonSchema>,
    pub execution_mode: Option<ToolExecutionMode>,
    pub replay: ToolReplay,
    pub prepare_arguments: Option<PrepareArguments>,
    pub execute: ToolExecute,
}
impl AgentTool {
    pub fn new(declaration: Tool, execute: ToolExecute) -> Self {
        Self {
            label: declaration.name.to_string(),
            declaration,
            execute,
            output_schema: None,
            execution_mode: None,
            replay: ToolReplay::Never,
            prepare_arguments: None,
        }
    }
}
impl fmt::Debug for AgentTool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AgentTool")
            .field("declaration", &self.declaration)
            .field("label", &self.label)
            .field("execution_mode", &self.execution_mode)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug, Default)]
pub struct AgentContext {
    pub messages: Vec<AgentMessage>,
    pub tools: Vec<AgentTool>,
}

#[derive(Clone, Debug, Default)]
pub struct BeforeToolCallResult {
    /// Explicit Rust equivalent of mutating the already-validated args in a hook.
    /// Applied without another validation pass, matching the target callback order.
    pub args: Option<JsonObject>,
    pub block: bool,
    pub reason: Option<String>,
    pub terminate: bool,
}

#[derive(Clone, Debug, Default)]
pub struct AfterToolCallResult {
    pub content: Option<Vec<InputContent>>,
    pub details: Option<JsonValue>,
    pub structured_content: Option<JsonValue>,
    pub usage: Option<Usage>,
    pub is_error: Option<bool>,
    pub terminate: Option<bool>,
}

#[derive(Clone, Debug)]
pub struct BeforeToolCallContext {
    pub assistant_message: AssistantMessage,
    pub tool_call: ToolCall,
    pub args: JsonObject,
    pub context: AgentContext,
}

#[derive(Clone, Debug)]
pub struct AfterToolCallContext {
    pub before: BeforeToolCallContext,
    pub result: AgentToolResult,
    pub is_error: bool,
}
pub type BeforeToolCall = Arc<
    dyn Fn(
            BeforeToolCallContext,
            CancellationToken,
        ) -> BoxFuture<'static, Result<Option<BeforeToolCallResult>, AgentError>>
        + Send
        + Sync,
>;
pub type AfterToolCall = Arc<
    dyn Fn(
            AfterToolCallContext,
            CancellationToken,
        ) -> BoxFuture<'static, Result<Option<AfterToolCallResult>, AgentError>>
        + Send
        + Sync,
>;

#[derive(Clone, Default)]
pub struct ToolCallHooks {
    pub before: Option<BeforeToolCall>,
    pub after: Option<AfterToolCall>,
}

#[derive(Clone, Debug)]
pub struct AgentToolCallOutcome {
    pub tool_call: ToolCall,
    pub result: AgentToolResult,
    pub is_error: bool,
    pub duration_ms: Option<u64>,
}
