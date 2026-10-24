use std::sync::Arc;

use pi_agent_schema::*;
use pi_ai_core::{
    AssistantMessage, AssistantStream, Model, SecretString, SimpleStreamOptionsWire,
    ToolResultMessage, TranscriptContext,
};

#[derive(Clone, Debug)]
pub struct AgentRequest {
    pub model: Model,
    pub context: TranscriptContext,
    pub options: SimpleStreamOptionsWire,
    pub api_key: Option<SecretString>,
    pub cancellation: CancellationToken,
}

pub type StreamFn = Arc<
    dyn Fn(AgentRequest) -> BoxFuture<'static, Result<AssistantStream, AgentError>> + Send + Sync,
>;
pub type ConvertToLlm = Arc<
    dyn Fn(Vec<AgentMessage>) -> BoxFuture<'static, Result<Vec<pi_ai_core::Message>, AgentError>>
        + Send
        + Sync,
>;
pub type TransformContext = Arc<
    dyn Fn(
            Vec<AgentMessage>,
            CancellationToken,
        ) -> BoxFuture<'static, Result<Vec<AgentMessage>, AgentError>>
        + Send
        + Sync,
>;
pub type GetApiKey = Arc<
    dyn Fn(String) -> BoxFuture<'static, Result<Option<SecretString>, AgentError>> + Send + Sync,
>;
pub type PollMessages =
    Arc<dyn Fn() -> BoxFuture<'static, Result<Vec<AgentMessage>, AgentError>> + Send + Sync>;
/// Optional injected monotonic clock; elapsed time never uses the wall clock.
pub type MonotonicClock = Arc<dyn Fn() -> std::time::Duration + Send + Sync>;

#[derive(Clone, Debug)]
pub struct AgentTurnContext {
    pub message: AssistantMessage,
    pub tool_results: Vec<ToolResultMessage>,
    pub context: AgentContext,
    pub new_messages: Vec<AgentMessage>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AgentTurnDecision {
    Continue,
    End,
}
#[derive(Clone, Debug, Default)]
pub struct AgentRequestUpdate {
    pub context: Option<AgentContext>,
    pub model: Option<Model>,
    pub thinking_level: Option<ThinkingLevel>,
}
#[derive(Clone, Debug, Default)]
pub struct AgentLoopTurnUpdate {
    pub request: AgentRequestUpdate,
    pub messages: Vec<AgentMessage>,
}
#[derive(Clone, Debug)]
pub struct PrepareRequestContext {
    pub context: AgentContext,
    pub model: Model,
    pub thinking_level: ThinkingLevel,
}

pub type FinishTurn = Arc<
    dyn Fn(
            AgentTurnContext,
            CancellationToken,
        ) -> BoxFuture<'static, Result<Option<AgentTurnDecision>, AgentError>>
        + Send
        + Sync,
>;
pub type PrepareRequest = Arc<
    dyn Fn(
            PrepareRequestContext,
            CancellationToken,
        ) -> BoxFuture<'static, Result<Option<AgentRequestUpdate>, AgentError>>
        + Send
        + Sync,
>;
pub type PrepareNextTurn = Arc<
    dyn Fn(
            AgentTurnContext,
            CancellationToken,
        ) -> BoxFuture<'static, Result<Option<AgentLoopTurnUpdate>, AgentError>>
        + Send
        + Sync,
>;

#[derive(Clone)]
pub struct AgentLoopConfig {
    pub model: Model,
    pub options: SimpleStreamOptionsWire,
    pub api_key: Option<SecretString>,
    pub get_api_key: Option<GetApiKey>,
    pub stream: StreamFn,
    pub convert_to_llm: ConvertToLlm,
    pub transform_context: Option<TransformContext>,
    pub cancellation: CancellationToken,
    pub tool_execution: ToolExecutionMode,
    pub tool_hooks: ToolCallHooks,
    pub message_hooks: MessageHooks,
    pub monotonic_clock: Option<MonotonicClock>,
    pub finish_turn: Option<FinishTurn>,
    pub prepare_request: Option<PrepareRequest>,
    pub prepare_next_turn: Option<PrepareNextTurn>,
    pub get_steering_messages: Option<PollMessages>,
    pub get_follow_up_messages: Option<PollMessages>,
}
impl AgentLoopConfig {
    pub fn new(model: Model, stream: StreamFn) -> Self {
        Self {
            model,
            stream,
            options: SimpleStreamOptionsWire::default(),
            api_key: None,
            get_api_key: None,
            convert_to_llm: Arc::new(|messages| {
                Box::pin(async move {
                    Ok(messages
                        .into_iter()
                        .filter_map(|message| match message {
                            AgentMessage::BuiltIn(message) => Some(message),
                            _ => None,
                        })
                        .collect())
                })
            }),
            transform_context: None,
            cancellation: CancellationToken::new(),
            tool_execution: ToolExecutionMode::Parallel,
            tool_hooks: ToolCallHooks::default(),
            message_hooks: MessageHooks::default(),
            monotonic_clock: None,
            finish_turn: None,
            prepare_request: None,
            prepare_next_turn: None,
            get_steering_messages: None,
            get_follow_up_messages: None,
        }
    }
}

/// Transient admission sidecars used by the stateful Agent. Legacy loop callers
/// continue using the ordinary message polls and event sink.
pub type PollInputs = std::sync::Arc<
    dyn Fn() -> BoxFuture<'static, Result<Vec<crate::AdmittedMessage>, AgentError>> + Send + Sync,
>;
#[derive(Clone, Default)]
pub struct InputLoopOptions {
    pub steering: Option<PollInputs>,
    pub follow_up: Option<PollInputs>,
    pub events: Option<crate::InputEventSink>,
}
