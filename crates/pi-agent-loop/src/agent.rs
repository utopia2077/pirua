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

use pi_agent_schema::{
    AgentEvent, AgentEventSink, AgentMessage, AgentTool, BoxFuture, Cancellation,
    HarnessRuntimeError, LiveAgentMessage, SettledAssistantMessage,
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
}

impl fmt::Display for LegacyAgentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Conversion(message) => write!(formatter, "legacy conversion failed: {message}"),
            Self::Provider(message) => write!(formatter, "legacy provider failed: {message}"),
            Self::Event(message) => write!(formatter, "legacy event delivery failed: {message}"),
            Self::InvalidContext(message) => write!(formatter, "invalid legacy context: {message}"),
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

        let assistant = stream_one(&mut current, &config).await?;
        let assistant_message = AgentMessage::BuiltIn(Message::Assistant(Box::new(assistant)));
        current.messages.push(assistant_message.clone());
        produced.push(assistant_message.clone());
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
