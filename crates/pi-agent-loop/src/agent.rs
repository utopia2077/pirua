//! Low-level, transient legacy Agent loop compatibility.
//!
//! This module deliberately owns only process-local execution.  Durable
//! Harness state, recovery registers, and usage ledgers remain outside this
//! crate.

use std::fmt;
use std::future::poll_fn;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Poll, Waker};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::{ToolPhaseHooks, ToolResultValidator};
use pi_agent_schema::{
    AgentEvent, AgentEventSink, AgentHarnessToolHandle, AgentMessage, AgentTool, BoxFuture,
    Cancellation, HarnessRuntimeError, LiveAgentMessage, SettledAssistantMessage, ToolBatchInput,
    ToolBatchPhases, ToolExecutionMode, ToolPreparation,
};
use pi_ai_core::{
    AssistantContent, AssistantMessage, Message, Model, SafeUnsigned, StopReason, Usage,
};

/// A provider request after the legacy transform/conversion boundary.
#[derive(Clone, Debug)]
pub struct LegacyRequest {
    /// Requested model.
    pub model: Model,
    /// System prompt supplied to the provider.
    pub system_prompt: Option<String>,
    /// Provider-neutral messages after conversion.
    pub messages: Vec<Message>,
    /// Callback-free tool declarations.
    pub tools: Vec<AgentTool>,
}

/// The provider/model callback used by the low-level loop.
pub type LegacyStreamFn = Arc<
    dyn Fn(
            LegacyRequest,
            Arc<dyn Cancellation>,
        ) -> BoxFuture<'static, Result<SettledAssistantMessage, LegacyAgentError>>
        + Send
        + Sync,
>;

/// Synchronous conversion from process-local Agent messages to provider
/// messages.
pub type LegacyConvertFn =
    Arc<dyn Fn(Vec<AgentMessage>) -> Result<Vec<Message>, LegacyAgentError> + Send + Sync>;

/// Optional process-local context transform.  The transform always runs before
/// [`LegacyConvertFn`].
pub type LegacyTransformFn = Arc<
    dyn Fn(Vec<AgentMessage>) -> BoxFuture<'static, Result<Vec<AgentMessage>, LegacyAgentError>>
        + Send
        + Sync,
>;

/// The mutable in-memory context passed between legacy loop turns.
#[derive(Clone, Debug, Default)]
pub struct LegacyContext {
    /// System prompt.
    pub system_prompt: Option<String>,
    /// Current process-local transcript.
    pub messages: Vec<AgentMessage>,
    /// Callback-free tool declarations.
    pub tools: Vec<AgentTool>,
}

/// Process-local executable tool port for the legacy loop.
///
/// The descriptor remains callback-free and can be copied into provider
/// requests. The executable binding is retained only in memory for one loop
/// invocation and is never serialized or attached to durable Harness state.
pub type LegacyTool = AgentHarnessToolHandle<LegacyContext>;

/// Queue and stop hooks used by the transient legacy loop.
pub trait LegacyLoopControl: Send + Sync {
    /// Polls messages that steer the next assistant turn.
    fn poll_steering(&self) -> BoxFuture<'_, Vec<AgentMessage>>;
    /// Polls messages to follow up a turn that would otherwise stop.
    fn poll_follow_up(&self) -> BoxFuture<'_, Vec<AgentMessage>>;
    /// Allows the caller to stop before follow-up polling.
    fn should_stop_after_turn(&self) -> bool {
        false
    }
}

/// Configuration for one low-level legacy loop invocation.
pub struct LegacyLoopConfig {
    /// Requested model.
    pub model: Model,
    /// Optional context transform.
    pub transform_context: Option<LegacyTransformFn>,
    /// Provider conversion callback.
    pub convert_to_provider: LegacyConvertFn,
    /// Provider/model stream callback.
    pub stream: LegacyStreamFn,
    /// Invocation-local cancellation token.
    pub cancellation: Arc<dyn Cancellation>,
}

impl fmt::Debug for LegacyLoopConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LegacyLoopConfig")
            .field("model", &self.model)
            .field("transform_context", &self.transform_context.is_some())
            .field("cancellation", &"<process-local>")
            .finish_non_exhaustive()
    }
}

/// A low-level loop failure. Provider failures are normally represented by a
/// settled assistant message; this error is reserved for conversion and event
/// delivery failures.
#[derive(Debug)]
pub enum LegacyAgentError {
    /// Context conversion failed.
    Conversion(String),
    /// Provider callback failed before it could return a settled message.
    Provider(String),
    /// Event delivery failed.
    Event(String),
    /// The continuation precondition was not met.
    InvalidContext(String),
    /// Tool preparation, execution, finalization, or cancellation failure.
    Tool(String),
}

impl fmt::Display for LegacyAgentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Conversion(message) => write!(formatter, "legacy conversion failed: {message}"),
            Self::Provider(message) => write!(formatter, "legacy provider failed: {message}"),
            Self::Event(message) => write!(formatter, "legacy event delivery failed: {message}"),
            Self::InvalidContext(message) => write!(formatter, "invalid legacy context: {message}"),
            Self::Tool(message) => write!(formatter, "legacy tool failed: {message}"),
        }
    }
}

impl std::error::Error for LegacyAgentError {}

/// A process-local cancellation token suitable for `LegacyLoopConfig`.
#[derive(Debug, Default)]
pub struct LegacyCancellation {
    cancelled: AtomicBool,
    waiter: Mutex<Option<Waker>>,
}

impl LegacyCancellation {
    /// Creates a clear cancellation token.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Pulls the cancellation signal.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        if let Some(waker) = self.waiter.lock().unwrap_or_else(|e| e.into_inner()).take() {
            waker.wake();
        }
    }
}

impl Cancellation for LegacyCancellation {
    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    fn cancelled(&self) -> BoxFuture<'_, ()> {
        Box::pin(poll_fn(|cx| {
            if self.is_cancelled() {
                Poll::Ready(())
            } else {
                *self.waiter.lock().unwrap_or_else(|e| e.into_inner()) = Some(cx.waker().clone());
                Poll::Pending
            }
        }))
    }
}

/// Runs a prompt invocation and returns only messages produced by that
/// invocation, matching the upstream `agentLoop` contract.
pub fn run_agent_loop(
    prompts: Vec<AgentMessage>,
    context: LegacyContext,
    config: LegacyLoopConfig,
    sink: Arc<dyn AgentEventSink>,
) -> BoxFuture<'static, Result<Vec<AgentMessage>, LegacyAgentError>> {
    run_agent_loop_with_tools_and_control_and_mode(
        prompts,
        context,
        config,
        Vec::new(),
        None,
        ToolExecutionMode::Parallel,
        sink,
    )
}

/// Runs a legacy prompt with executable process-local tools.
pub fn run_agent_loop_with_tools(
    prompts: Vec<AgentMessage>,
    context: LegacyContext,
    config: LegacyLoopConfig,
    tools: Vec<LegacyTool>,
    sink: Arc<dyn AgentEventSink>,
) -> BoxFuture<'static, Result<Vec<AgentMessage>, LegacyAgentError>> {
    run_agent_loop_with_tools_and_control_and_mode(
        prompts,
        context,
        config,
        tools,
        None,
        ToolExecutionMode::Parallel,
        sink,
    )
}

/// Runs a legacy prompt with executable tools and process-local queue hooks.
pub fn run_agent_loop_with_tools_and_control(
    prompts: Vec<AgentMessage>,
    context: LegacyContext,
    config: LegacyLoopConfig,
    tools: Vec<LegacyTool>,
    control: Option<Arc<dyn LegacyLoopControl>>,
    sink: Arc<dyn AgentEventSink>,
) -> BoxFuture<'static, Result<Vec<AgentMessage>, LegacyAgentError>> {
    run_agent_loop_with_tools_and_control_and_mode(
        prompts,
        context,
        config,
        tools,
        control,
        ToolExecutionMode::Parallel,
        sink,
    )
}

/// Runs a legacy prompt with an explicit default tool execution mode.
pub fn run_agent_loop_with_tools_and_mode(
    prompts: Vec<AgentMessage>,
    context: LegacyContext,
    config: LegacyLoopConfig,
    tools: Vec<LegacyTool>,
    default_mode: ToolExecutionMode,
    sink: Arc<dyn AgentEventSink>,
) -> BoxFuture<'static, Result<Vec<AgentMessage>, LegacyAgentError>> {
    run_agent_loop_with_tools_and_control_and_mode(
        prompts,
        context,
        config,
        tools,
        None,
        default_mode,
        sink,
    )
}

fn run_agent_loop_with_tools_and_control_and_mode(
    prompts: Vec<AgentMessage>,
    context: LegacyContext,
    config: LegacyLoopConfig,
    tools: Vec<LegacyTool>,
    control: Option<Arc<dyn LegacyLoopControl>>,
    default_mode: ToolExecutionMode,
    sink: Arc<dyn AgentEventSink>,
) -> BoxFuture<'static, Result<Vec<AgentMessage>, LegacyAgentError>> {
    Box::pin(async move {
        let mut current = LegacyContext {
            messages: context
                .messages
                .into_iter()
                .chain(prompts.iter().cloned())
                .collect(),
            ..context
        };
        let mut produced = prompts;
        emit(&sink, AgentEvent::AgentStart).await?;
        emit(&sink, AgentEvent::TurnStart).await?;
        for prompt in &produced {
            emit(
                &sink,
                AgentEvent::MessageStart {
                    message: LiveAgentMessage::Settled(prompt.clone()),
                },
            )
            .await?;
            emit(
                &sink,
                AgentEvent::MessageEnd {
                    message: prompt.clone(),
                },
            )
            .await?;
        }

        loop {
            if config.cancellation.is_cancelled() {
                let assistant =
                    synthetic_failure(&config.model, "Operation aborted".to_owned(), true);
                let assistant_message =
                    AgentMessage::BuiltIn(Message::Assistant(Box::new(assistant)));
                current.messages.push(assistant_message.clone());
                produced.push(assistant_message.clone());
                emit_message(&sink, assistant_message.clone()).await?;
                emit(
                    &sink,
                    AgentEvent::TurnEnd {
                        message: assistant_message,
                        tool_results: Vec::new(),
                    },
                )
                .await?;
                break;
            }
            let assistant = stream_one(&mut current, &config).await?;
            let stop_reason = assistant.stop_reason;
            let assistant_message = AgentMessage::BuiltIn(Message::Assistant(Box::new(assistant)));
            current.messages.push(assistant_message.clone());
            produced.push(assistant_message.clone());
            emit_message(&sink, assistant_message.clone()).await?;

            let mut tool_results = Vec::new();
            let mut terminate = false;
            let has_tool_calls = match &assistant_message {
                AgentMessage::BuiltIn(Message::Assistant(message)) => message
                    .content
                    .iter()
                    .any(|content| matches!(content, AssistantContent::ToolCall(_))),
                _ => false,
            };
            if has_tool_calls && matches!(stop_reason, StopReason::ToolUse | StopReason::Length) {
                let AgentMessage::BuiltIn(Message::Assistant(assistant)) = &assistant_message
                else {
                    unreachable!("assistant message shape is fixed")
                };
                let phases = LegacyPhases {
                    tools: tools.clone(),
                    context: current.clone(),
                    sink: sink.clone(),
                    cancellation: config.cancellation.clone(),
                };
                let batch = crate::execute_legacy_tool_batch(
                    ToolBatchInput {
                        context: &current,
                        assistant_message: assistant,
                        default_mode,
                    },
                    &phases,
                )
                .await?;
                terminate = batch.terminate;
                tool_results = batch.messages;
                for result in &tool_results {
                    let message =
                        AgentMessage::BuiltIn(Message::ToolResult(Box::new(result.clone())));
                    current.messages.push(message.clone());
                    produced.push(message);
                }
            }
            let turn_message = current
                .messages
                .iter()
                .rev()
                .find_map(|message| match message {
                    AgentMessage::BuiltIn(Message::Assistant(_)) => Some(message.clone()),
                    _ => None,
                })
                .expect("assistant appended");
            emit(
                &sink,
                AgentEvent::TurnEnd {
                    message: turn_message,
                    tool_results,
                },
            )
            .await?;
            if terminate
                || config.cancellation.is_cancelled()
                || matches!(stop_reason, StopReason::Error | StopReason::Aborted)
            {
                break;
            }
            if has_tool_calls && matches!(stop_reason, StopReason::ToolUse | StopReason::Length) {
                emit(&sink, AgentEvent::TurnStart).await?;
                if let Some(control) = &control {
                    let mut steering = control.poll_steering().await;
                    append_control_messages(&mut current, &mut produced, &sink, &mut steering)
                        .await?;
                }
                continue;
            }
            let Some(control) = &control else {
                break;
            };
            if control.should_stop_after_turn() {
                break;
            }
            let mut follow_up = control.poll_follow_up().await;
            if follow_up.is_empty() {
                break;
            }
            emit(&sink, AgentEvent::TurnStart).await?;
            append_control_messages(&mut current, &mut produced, &sink, &mut follow_up).await?;
        }
        emit(
            &sink,
            AgentEvent::AgentEnd {
                messages: produced.clone(),
            },
        )
        .await?;
        Ok(produced)
    })
}

/// Continues an existing context without adding a new prompt.
pub fn run_agent_loop_continue(
    context: LegacyContext,
    config: LegacyLoopConfig,
    sink: Arc<dyn AgentEventSink>,
) -> BoxFuture<'static, Result<Vec<AgentMessage>, LegacyAgentError>> {
    Box::pin(async move {
        let last = context.messages.last().ok_or_else(|| {
            LegacyAgentError::InvalidContext("cannot continue an empty context".to_owned())
        })?;
        if matches!(last, AgentMessage::BuiltIn(Message::Assistant(_))) {
            return Err(LegacyAgentError::InvalidContext(
                "cannot continue from an assistant message".to_owned(),
            ));
        }

        let mut current = context;
        emit(&sink, AgentEvent::AgentStart).await?;
        emit(&sink, AgentEvent::TurnStart).await?;
        let assistant = stream_one(&mut current, &config).await?;
        let assistant_message = AgentMessage::BuiltIn(Message::Assistant(Box::new(assistant)));
        current.messages.push(assistant_message.clone());
        emit(
            &sink,
            AgentEvent::MessageStart {
                message: LiveAgentMessage::Settled(assistant_message.clone()),
            },
        )
        .await?;
        emit(
            &sink,
            AgentEvent::MessageEnd {
                message: assistant_message.clone(),
            },
        )
        .await?;
        emit(
            &sink,
            AgentEvent::TurnEnd {
                message: assistant_message,
                tool_results: Vec::new(),
            },
        )
        .await?;
        emit(
            &sink,
            AgentEvent::AgentEnd {
                messages: vec![
                    current
                        .messages
                        .last()
                        .cloned()
                        .expect("assistant appended"),
                ],
            },
        )
        .await?;
        Ok(vec![
            current
                .messages
                .last()
                .cloned()
                .expect("assistant appended"),
        ])
    })
}

async fn stream_one(
    context: &mut LegacyContext,
    config: &LegacyLoopConfig,
) -> Result<AssistantMessage, LegacyAgentError> {
    let transformed = if let Some(transform) = &config.transform_context {
        transform(context.messages.clone()).await?
    } else {
        context.messages.clone()
    };
    let provider_messages = (config.convert_to_provider)(transformed)?;
    let request = LegacyRequest {
        model: config.model.clone(),
        system_prompt: context.system_prompt.clone(),
        messages: provider_messages,
        tools: context.tools.clone(),
    };
    match (config.stream)(request, config.cancellation.clone()).await {
        Ok(message) => Ok(message.into_message()),
        Err(error) => Ok(synthetic_failure(
            &config.model,
            error.to_string(),
            config.cancellation.is_cancelled(),
        )),
    }
}

async fn emit_message(
    sink: &Arc<dyn AgentEventSink>,
    message: AgentMessage,
) -> Result<(), LegacyAgentError> {
    emit(
        sink,
        AgentEvent::MessageStart {
            message: LiveAgentMessage::Settled(message.clone()),
        },
    )
    .await?;
    emit(sink, AgentEvent::MessageEnd { message }).await
}

async fn append_control_messages(
    context: &mut LegacyContext,
    produced: &mut Vec<AgentMessage>,
    sink: &Arc<dyn AgentEventSink>,
    messages: &mut Vec<AgentMessage>,
) -> Result<(), LegacyAgentError> {
    for message in messages.drain(..) {
        context.messages.push(message.clone());
        produced.push(message.clone());
        emit_message(sink, message).await?;
    }
    Ok(())
}

struct LegacyPhases {
    tools: Vec<LegacyTool>,
    context: LegacyContext,
    sink: Arc<dyn AgentEventSink>,
    cancellation: Arc<dyn Cancellation>,
}

impl ToolBatchPhases<LegacyContext> for LegacyPhases {
    type Binding = LegacyTool;
    type Error = LegacyAgentError;

    fn execution_mode(&self, tool_name: &str) -> Option<ToolExecutionMode> {
        self.tools
            .iter()
            .find(|tool| tool.descriptor().name.as_str() == tool_name)
            .and_then(|tool| tool.descriptor().execution_mode)
    }

    fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }

    fn prepare<'a>(
        &'a self,
        context: &'a LegacyContext,
        _assistant: &'a AssistantMessage,
        call: &'a pi_ai_core::ToolCall,
    ) -> BoxFuture<'a, Result<ToolPreparation<Self::Binding>, Self::Error>> {
        let binding = self
            .tools
            .iter()
            .find(|tool| tool.descriptor().name == call.name)
            .cloned();
        Box::pin(async move {
            crate::prepare_tool_call(
                call,
                binding,
                context,
                self.cancellation.as_ref(),
                &LegacyHooks,
                false,
            )
            .await
            .map_err(|error| LegacyAgentError::Tool(error.to_string()))
        })
    }

    fn execute<'a>(
        &'a self,
        prepared: &'a pi_agent_schema::PreparedToolCall<Self::Binding>,
    ) -> BoxFuture<'a, Result<pi_agent_schema::ExecutedToolOutcome, Self::Error>> {
        Box::pin(async move {
            crate::execute_tool_call(
                prepared,
                &self.context,
                self.cancellation.as_ref(),
                self.sink.as_ref(),
                &LegacyValidator,
            )
            .await
            .map_err(|error| LegacyAgentError::Tool(error.to_string()))
        })
    }

    fn finalize<'a>(
        &'a self,
        context: &'a LegacyContext,
        _assistant: &'a AssistantMessage,
        prepared: &'a pi_agent_schema::PreparedToolCall<Self::Binding>,
        executed: pi_agent_schema::ExecutedToolOutcome,
    ) -> BoxFuture<'a, Result<pi_agent_schema::FinalizedToolCall, Self::Error>> {
        Box::pin(async move {
            crate::finalize_tool_call(
                prepared,
                executed,
                context,
                self.cancellation.as_ref(),
                &LegacyHooks,
                &LegacyValidator,
                !self.cancellation.is_cancelled(),
            )
            .await
            .map_err(|error| LegacyAgentError::Tool(error.to_string()))
        })
    }

    fn emit(&self, event: AgentEvent) -> BoxFuture<'_, Result<(), Self::Error>> {
        Box::pin(async move {
            self.sink
                .emit(event)
                .await
                .map_err(|error| LegacyAgentError::Event(error.to_string()))
        })
    }
}

struct LegacyHooks;

impl ToolPhaseHooks<LegacyContext> for LegacyHooks {
    fn before<'a>(
        &'a self,
        _call: &'a pi_ai_core::ToolCall,
        _args: &'a pi_ai_core::JsonObject,
        _context: &'a LegacyContext,
        _cancellation: &'a dyn Cancellation,
    ) -> BoxFuture<'a, Result<Option<pi_agent_schema::BeforeToolResult>, crate::ToolPhaseError>>
    {
        Box::pin(async { Ok(None) })
    }

    fn after<'a>(
        &'a self,
        _call: &'a pi_ai_core::ToolCall,
        _args: &'a pi_ai_core::JsonObject,
        _outcome: &'a pi_agent_schema::ExecutedToolOutcome,
        _context: &'a LegacyContext,
        _cancellation: &'a dyn Cancellation,
    ) -> BoxFuture<'a, Result<Option<pi_agent_schema::AfterToolResult>, crate::ToolPhaseError>>
    {
        Box::pin(async { Ok(None) })
    }
}

struct LegacyValidator;

impl ToolResultValidator for LegacyValidator {
    fn validate(
        &self,
        _result: &pi_agent_schema::AgentToolResult,
    ) -> Result<(), pi_agent_schema::ToolExecutionError> {
        Ok(())
    }

    fn validate_usage(&self, _usage: &Usage) -> Result<(), pi_agent_schema::ToolExecutionError> {
        Ok(())
    }
}

fn synthetic_failure(model: &Model, message: String, cancelled: bool) -> AssistantMessage {
    AssistantMessage {
        role: Default::default(),
        content: vec![AssistantContent::Text(pi_ai_core::TextContent {
            content_type: Default::default(),
            text: String::new(),
            text_signature: None,
        })],
        api: model.api.clone(),
        provider: model.provider.clone(),
        model: model.id.clone(),
        response_model: None,
        response_id: None,
        diagnostics: None,
        usage: Usage::zero(),
        stop_reason: if cancelled {
            StopReason::Aborted
        } else {
            StopReason::Error
        },
        deferred: None,
        error_message: Some(message),
        raw_stop_reason: None,
        end_turn: None,
        timestamp: timestamp(),
    }
}

fn timestamp() -> SafeUnsigned {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default();
    SafeUnsigned::new(u64::try_from(millis).unwrap_or(u64::MAX))
        .unwrap_or_else(|_| SafeUnsigned::new(0).expect("zero is safe"))
}

async fn emit(sink: &Arc<dyn AgentEventSink>, event: AgentEvent) -> Result<(), LegacyAgentError> {
    sink.emit(event)
        .await
        .map_err(|error: HarnessRuntimeError| LegacyAgentError::Event(error.to_string()))
}
