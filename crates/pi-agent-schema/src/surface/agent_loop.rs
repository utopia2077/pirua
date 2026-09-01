//! Behavior-free legacy Agent-loop building-block signatures.

use std::sync::Arc;

use pi_ai_core::{
    AssistantMessage, AssistantMessageEvent, JsonObject, JsonValue, Message, Model, ToolCallId,
    ToolName, ToolResultMessage,
};

use crate::surface::BoxFuture;
use crate::{
    AgentHarnessStreamOptions, AgentMessage, AgentThinkingLevel, AgentTool, AgentToolCall,
    AgentToolResult, Cancellation, HarnessRuntimeError, LiveAgentMessage, ResponseHeaders,
    SettledAssistantMessage, ToolExecutionMode,
};

/// Process-local low-level Agent event passed to the phase port.
#[derive(Clone, Debug, PartialEq)]
pub enum AgentEvent {
    /// Agent run started.
    AgentStart,
    /// Agent run ended after listener settlement.
    AgentEnd {
        /// Messages produced by the invocation.
        messages: Vec<AgentMessage>,
    },
    /// Assistant turn started.
    TurnStart,
    /// Assistant turn and its tool-result artifacts ended.
    TurnEnd {
        /// Final assistant message.
        message: AgentMessage,
        /// Source-ordered tool result messages.
        tool_results: Vec<ToolResultMessage>,
    },
    /// Message lifecycle started.
    MessageStart {
        /// Starting message.
        message: LiveAgentMessage,
    },
    /// Assistant stream updated.
    MessageUpdate {
        /// Partial Agent message.
        message: LiveAgentMessage,
        /// Provider-neutral assistant stream event.
        assistant_message_event: AssistantMessageEvent,
    },
    /// Message lifecycle ended.
    MessageEnd {
        /// Final message.
        message: AgentMessage,
    },
    /// Tool execution started.
    ToolExecutionStart {
        /// Tool-call identity.
        tool_call_id: ToolCallId,
        /// Tool name.
        tool_name: ToolName,
        /// Effective arguments.
        args: JsonValue,
    },
    /// Tool execution reported progress.
    ToolExecutionUpdate {
        /// Tool-call identity.
        tool_call_id: ToolCallId,
        /// Tool name.
        tool_name: ToolName,
        /// Effective arguments.
        args: JsonValue,
        /// Partial result.
        partial_result: AgentToolResult,
    },
    /// Tool execution ended.
    ToolExecutionEnd {
        /// Tool-call identity.
        tool_call_id: ToolCallId,
        /// Tool name.
        tool_name: ToolName,
        /// Final result.
        result: AgentToolResult,
        /// Whether execution failed.
        is_error: bool,
    },
}

/// Borrowed context and assistant message for one legacy tool batch.
#[derive(Clone, Copy, Debug)]
pub struct ToolBatchInput<'a, C> {
    /// Process-local tool context snapshot.
    pub context: &'a C,
    /// Assistant message containing source calls.
    pub assistant_message: &'a AssistantMessage,
    /// Default execution mode before per-tool overrides.
    pub default_mode: ToolExecutionMode,
}

/// Exact ToolBatchInput field inventory.
pub const TOOL_BATCH_INPUT_FIELDS: &[&str] = &["context", "assistant_message", "default_mode"];

/// Prepared real tool call retaining its process-local selected binding.
#[derive(Debug)]
pub struct PreparedToolCall<B> {
    /// Source tool call.
    pub tool_call: AgentToolCall,
    /// Validated effective arguments.
    pub args: JsonObject,
    /// Process-local selected tool binding.
    pub binding: B,
}

/// Exact PreparedToolCall field inventory.
pub const PREPARED_TOOL_CALL_FIELDS: &[&str] = &["tool_call", "args", "binding"];

/// Source-ordered preparation result.
#[derive(Debug)]
pub enum ToolPreparation<B> {
    /// A real tool effect may execute.
    Prepared(PreparedToolCall<B>),
    /// An in-band result requires no tool effect.
    Immediate(ImmediateToolOutcome),
}

/// In-band result produced without executing a tool effect.
#[derive(Clone, Debug, PartialEq)]
pub struct ImmediateToolOutcome {
    /// Tool result.
    pub result: AgentToolResult,
    /// Whether the result represents failure.
    pub is_error: bool,
    /// Early-termination decision.
    pub terminate: bool,
}

/// Exact ImmediateToolOutcome field inventory.
pub const IMMEDIATE_TOOL_OUTCOME_FIELDS: &[&str] = &["result", "is_error", "terminate"];

/// Raw phase-two execution outcome.
#[derive(Clone, Debug, PartialEq)]
pub struct ExecutedToolOutcome {
    /// Raw tool result.
    pub result: AgentToolResult,
    /// Whether execution failed.
    pub is_error: bool,
}

/// Exact ExecutedToolOutcome field inventory.
pub const EXECUTED_TOOL_OUTCOME_FIELDS: &[&str] = &["result", "is_error"];

/// Source-ordered finalized tool call.
#[derive(Clone, Debug, PartialEq)]
pub struct FinalizedToolCall {
    /// Source tool call.
    pub tool_call: AgentToolCall,
    /// Final patched result.
    pub result: AgentToolResult,
    /// Final failure classification.
    pub is_error: bool,
    /// Final early-termination decision.
    pub terminate: bool,
}

/// Exact FinalizedToolCall field inventory.
pub const FINALIZED_TOOL_CALL_FIELDS: &[&str] = &["tool_call", "result", "is_error", "terminate"];

/// Source-ordered target artifacts for a completed tool batch.
#[derive(Clone, Debug, PartialEq)]
pub struct ExecutedToolBatch {
    /// Source-ordered tool-result messages.
    pub messages: Vec<ToolResultMessage>,
    /// True only for a nonempty batch whose every finalized result terminates.
    pub terminate: bool,
}

/// Exact ExecutedToolBatch field inventory.
pub const EXECUTED_TOOL_BATCH_FIELDS: &[&str] = &["messages", "terminate"];

/// Object-safe process-local phases used by the S14 tool-batch coordinator.
pub trait ToolBatchPhases<C>: Send + Sync {
    /// Selected live tool binding retained between phases.
    type Binding: Send + Sync;
    /// Outer close/invariant/application/event-delivery failure.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Returns a per-tool execution-mode override.
    fn execution_mode(&self, tool_name: &str) -> Option<ToolExecutionMode>;
    /// Observes invocation-scoped cancellation.
    fn is_cancelled(&self) -> bool;
    /// Prepares one source call and either retains a binding or returns an immediate outcome.
    fn prepare<'a>(
        &'a self,
        context: &'a C,
        assistant: &'a AssistantMessage,
        call: &'a AgentToolCall,
    ) -> BoxFuture<'a, Result<ToolPreparation<Self::Binding>, Self::Error>>;
    /// Executes one prepared real tool effect.
    fn execute<'a>(
        &'a self,
        prepared: &'a PreparedToolCall<Self::Binding>,
    ) -> BoxFuture<'a, Result<ExecutedToolOutcome, Self::Error>>;
    /// Finalizes one executed result in assistant source order.
    fn finalize<'a>(
        &'a self,
        context: &'a C,
        assistant: &'a AssistantMessage,
        prepared: &'a PreparedToolCall<Self::Binding>,
        executed: ExecutedToolOutcome,
    ) -> BoxFuture<'a, Result<FinalizedToolCall, Self::Error>>;
    /// Emits one low-level compatibility event.
    fn emit(&self, event: AgentEvent) -> BoxFuture<'_, Result<(), Self::Error>>;
}

/// Exact ToolBatchPhases method inventory.
pub const TOOL_BATCH_PHASE_METHODS: &[&str] = &[
    "execution_mode",
    "is_cancelled",
    "prepare",
    "execute",
    "finalize",
    "emit",
];

/// Accepted S14 free-function signature; C2 deliberately provides no implementation.
pub const EXECUTE_TOOL_BATCH_SIGNATURE: &str = "pub fn execute_tool_batch<'a, C, P>(input: ToolBatchInput<'a, C>, phases: &'a P) -> BoxFuture<'a, Result<ExecutedToolBatch, P::Error>> where C: Sync + 'a, P: ToolBatchPhases<C> + ?Sized + 'a;";

/// Low-level Agent event sink used by assistant streaming and compatibility wrappers.
pub trait AgentEventSink: Send + Sync {
    /// Emits one event and awaits listener settlement.
    fn emit(&self, event: AgentEvent) -> BoxFuture<'_, Result<(), HarnessRuntimeError>>;
}

/// Exact AgentEventSink method inventory.
pub const AGENT_EVENT_SINK_METHODS: &[&str] = &["emit"];

/// Process-local Agent-context transform used before provider conversion.
pub trait AgentContextTransform: Send + Sync {
    /// Transforms projected Agent messages.
    fn transform<'a>(
        &'a self,
        messages: Vec<AgentMessage>,
        cancellation: &'a dyn Cancellation,
    ) -> BoxFuture<'a, Result<Vec<AgentMessage>, HarnessRuntimeError>>;
}

/// Exact AgentContextTransform method inventory.
pub const AGENT_CONTEXT_TRANSFORM_METHODS: &[&str] = &["transform"];

/// Process-local provider-payload transform.
pub trait ProviderPayloadTransform: Send + Sync {
    /// Replaces or preserves a provider-specific payload.
    fn transform<'a>(
        &'a self,
        payload: JsonValue,
        model: &'a Model,
    ) -> BoxFuture<'a, Result<Option<JsonValue>, HarnessRuntimeError>>;
}

/// Exact ProviderPayloadTransform method inventory.
pub const PROVIDER_PAYLOAD_TRANSFORM_METHODS: &[&str] = &["transform"];

/// Metadata accompanying a settled provider response transform.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ResponseMetadata {
    /// Optional HTTP-like status.
    pub status: Option<pi_ai_core::SafeUnsigned>,
    /// Optional string-valued response headers.
    pub headers: Option<ResponseHeaders>,
}

/// Process-local settled-response transform.
pub trait AssistantResponseTransform: Send + Sync {
    /// Transforms one settled response before message-end publication.
    fn transform(
        &self,
        message: SettledAssistantMessage,
        metadata: ResponseMetadata,
    ) -> BoxFuture<'_, Result<SettledAssistantMessage, HarnessRuntimeError>>;
}

/// Exact AssistantResponseTransform method inventory.
pub const ASSISTANT_RESPONSE_TRANSFORM_METHODS: &[&str] = &["transform"];

/// Process-local provider-message converter required by low-level assistant streaming.
pub trait StreamProviderMessageConverter: Send + Sync {
    /// Converts Agent messages to provider-neutral messages.
    fn convert(
        &self,
        messages: Vec<AgentMessage>,
    ) -> BoxFuture<'_, Result<Vec<Message>, HarnessRuntimeError>>;
}

/// Exact stream provider-message converter method inventory.
pub const STREAM_PROVIDER_MESSAGE_CONVERTER_METHODS: &[&str] = &["convert"];

/// Complete process-local assistant-stream configuration.
pub struct StreamAssistantConfig<C, M>
where
    M: Send + Sync + ?Sized,
{
    /// Requested model.
    pub model: Model,
    /// Requested thinking level.
    pub thinking_level: AgentThinkingLevel,
    /// Optional system prompt.
    pub system_prompt: Option<String>,
    /// Callback-free tool declarations.
    pub tools: Vec<AgentTool>,
    /// Optional Agent-context transform.
    pub transform_context: Option<Arc<dyn AgentContextTransform>>,
    /// Provider-message converter.
    pub to_provider_messages: Arc<dyn StreamProviderMessageConverter>,
    /// Runtime Models service handle.
    pub models: Arc<M>,
    /// Optional curated request options.
    pub stream_options: Option<AgentHarnessStreamOptions>,
    /// Optional provider-payload transform.
    pub transform_payload: Option<Arc<dyn ProviderPayloadTransform>>,
    /// Optional settled-response transform.
    pub transform_response: Option<Arc<dyn AssistantResponseTransform>>,
    /// Opaque process-local telemetry context.
    pub telemetry_context: C,
    /// Runtime-owned cancellation observation.
    pub cancellation: Arc<dyn Cancellation>,
}

/// Exact StreamAssistantConfig field inventory.
pub const STREAM_ASSISTANT_CONFIG_FIELDS: &[&str] = &[
    "model",
    "thinking_level",
    "system_prompt",
    "tools",
    "transform_context",
    "to_provider_messages",
    "models",
    "stream_options",
    "transform_payload",
    "transform_response",
    "telemetry_context",
    "cancellation",
];

/// Behavior-free low-level assistant-streaming port.
pub trait StreamAssistant<C, M>: Send + Sync
where
    M: Send + Sync + ?Sized,
{
    /// Streams and returns one settled assistant message without throwing expected request failures.
    fn stream(
        &self,
        messages: Vec<AgentMessage>,
        config: StreamAssistantConfig<C, M>,
        events: Arc<dyn AgentEventSink>,
    ) -> BoxFuture<'_, Result<SettledAssistantMessage, HarnessRuntimeError>>;
}

/// Exact StreamAssistant method inventory.
pub const STREAM_ASSISTANT_METHODS: &[&str] = &["stream"];
