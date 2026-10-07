//! Process-local public compatibility owner for the legacy Agent loop.

use std::collections::VecDeque;
use std::fmt;
use std::sync::{Arc, Mutex};

use pi_agent_loop::{
    LegacyAgentError, LegacyCancellation, LegacyContext, LegacyConvertFn, LegacyLoopConfig,
    LegacyLoopControl, LegacyStreamFn, LegacyTool, LegacyTransformFn,
    run_agent_loop_with_tools_and_control,
};
use pi_agent_schema::{
    AgentEvent, AgentEventSink, AgentMessage, BoxFuture, Cancellation, LiveAgentMessage, QueueMode,
};
use pi_ai_core::{Message, Model, ToolCallId};
use tokio::sync::Notify;

/// Listener callback used by the public compatibility owner.
pub type AgentListener = Arc<
    dyn Fn(AgentEvent, Arc<dyn Cancellation>) -> BoxFuture<'static, Result<(), LegacyAgentError>>
        + Send
        + Sync,
>;

/// Public construction options for the process-local Agent owner.
#[derive(Clone)]
pub struct AgentOptions {
    /// Requested model.
    pub model: Model,
    /// Optional system prompt.
    pub system_prompt: Option<String>,
    /// Provider/model stream callback.
    pub stream: LegacyStreamFn,
    /// Agent-to-provider message conversion callback.
    pub convert_to_provider: LegacyConvertFn,
    /// Optional context transform.
    pub transform_context: Option<LegacyTransformFn>,
    /// Executable process-local tools.
    pub tools: Vec<LegacyTool>,
}

impl fmt::Debug for AgentOptions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AgentOptions")
            .field("model", &self.model)
            .field("system_prompt", &self.system_prompt)
            .field("tools", &self.tools.len())
            .finish_non_exhaustive()
    }
}

/// Mutable in-memory Agent state exposed by the compatibility façade.
#[derive(Clone, Debug)]
pub struct AgentState {
    /// Current system prompt.
    pub system_prompt: Option<String>,
    /// Current model.
    pub model: Model,
    /// Settled transcript.
    pub messages: Vec<AgentMessage>,
    /// Whether a run is active.
    pub is_streaming: bool,
    /// Current partial message when one is observable.
    pub streaming_message: Option<AgentMessage>,
    /// Active tool-call identities.
    pub pending_tool_calls: Vec<ToolCallId>,
    /// Last assistant error text.
    pub error_message: Option<String>,
}

struct QueueState {
    steering: VecDeque<AgentMessage>,
    follow_up: VecDeque<AgentMessage>,
    steering_mode: QueueMode,
    follow_up_mode: QueueMode,
}

struct ActiveRun {
    cancellation: Arc<LegacyCancellation>,
    done: Arc<Notify>,
}

struct Inner {
    options: AgentOptions,
    state: AgentState,
    queues: QueueState,
    listeners: Vec<(usize, AgentListener)>,
    next_listener_id: usize,
    active: Option<ActiveRun>,
}

/// Process-local, mutable legacy Agent owner.
#[derive(Clone)]
pub struct Agent {
    inner: Arc<Mutex<Inner>>,
}

impl fmt::Debug for Agent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Agent")
            .field("state", &self.state())
            .finish()
    }
}

impl Agent {
    /// Creates a process-local Agent owner.
    pub fn new(options: AgentOptions) -> Self {
        let state = AgentState {
            system_prompt: options.system_prompt.clone(),
            model: options.model.clone(),
            messages: Vec::new(),
            is_streaming: false,
            streaming_message: None,
            pending_tool_calls: Vec::new(),
            error_message: None,
        };
        Self {
            inner: Arc::new(Mutex::new(Inner {
                options,
                state,
                queues: QueueState {
                    steering: VecDeque::new(),
                    follow_up: VecDeque::new(),
                    steering_mode: QueueMode::OneAtATime,
                    follow_up_mode: QueueMode::OneAtATime,
                },
                listeners: Vec::new(),
                next_listener_id: 0,
                active: None,
            })),
        }
    }

    /// Returns a snapshot of the current process-local state.
    pub fn state(&self) -> AgentState {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .state
            .clone()
    }

    /// Subscribes a listener. Listener promises settle in registration order.
    pub fn subscribe(&self, listener: AgentListener) -> usize {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let id = inner.next_listener_id;
        inner.next_listener_id += 1;
        inner.listeners.push((id, listener));
        id
    }

    /// Removes a listener by its subscription identifier.
    pub fn unsubscribe(&self, id: usize) -> bool {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let before = inner.listeners.len();
        inner.listeners.retain(|(candidate, _)| *candidate != id);
        before != inner.listeners.len()
    }

    /// Sets the steering queue drain mode.
    pub fn set_steering_mode(&self, mode: QueueMode) {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .queues
            .steering_mode = mode;
    }

    /// Sets the follow-up queue drain mode.
    pub fn set_follow_up_mode(&self, mode: QueueMode) {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .queues
            .follow_up_mode = mode;
    }

    /// Queues a steering message for the next assistant turn.
    pub fn steer(&self, message: AgentMessage) -> Result<(), LegacyAgentError> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if inner.active.is_none() {
            return Err(LegacyAgentError::InvalidContext(
                "steering requires an active run".to_owned(),
            ));
        }
        inner.queues.steering.push_back(message);
        Ok(())
    }

    /// Queues a follow-up message after the current run would stop.
    pub fn follow_up(&self, message: AgentMessage) -> Result<(), LegacyAgentError> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if inner.active.is_none() {
            return Err(LegacyAgentError::InvalidContext(
                "follow-up requires an active run".to_owned(),
            ));
        }
        inner.queues.follow_up.push_back(message);
        Ok(())
    }

    /// Aborts the current process-local run.
    pub fn abort(&self) {
        if let Some(active) = &self.inner.lock().unwrap_or_else(|e| e.into_inner()).active {
            active.cancellation.cancel();
        }
    }

    /// Resolves after the active run and all `agent_end` listeners settle.
    pub async fn wait_for_idle(&self) {
        loop {
            let notify = self
                .inner
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .active
                .as_ref()
                .map(|active| active.done.clone());
            let Some(notify) = notify else {
                return;
            };
            notify.notified().await;
        }
    }

    /// Clears transcript, queues, and transient state while idle.
    pub fn reset(&self) -> Result<(), LegacyAgentError> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if inner.active.is_some() {
            return Err(LegacyAgentError::InvalidContext(
                "Agent is already processing".to_owned(),
            ));
        }
        inner.state.messages.clear();
        inner.state.streaming_message = None;
        inner.state.pending_tool_calls.clear();
        inner.state.error_message = None;
        inner.queues.steering.clear();
        inner.queues.follow_up.clear();
        Ok(())
    }

    /// Starts a new prompt run.
    pub async fn prompt(&self, prompts: Vec<AgentMessage>) -> Result<(), LegacyAgentError> {
        self.start_run(prompts, false).await
    }

    /// Continues from the current transcript without adding a prompt.
    pub async fn continue_run(&self) -> Result<(), LegacyAgentError> {
        let last = self.state().messages.last().cloned().ok_or_else(|| {
            LegacyAgentError::InvalidContext("no messages to continue from".to_owned())
        })?;
        if matches!(last, AgentMessage::BuiltIn(Message::Assistant(_))) {
            return Err(LegacyAgentError::InvalidContext(
                "cannot continue from an assistant message".to_owned(),
            ));
        }
        self.start_run(Vec::new(), true).await
    }

    async fn start_run(
        &self,
        prompts: Vec<AgentMessage>,
        continuation: bool,
    ) -> Result<(), LegacyAgentError> {
        let (options, context, cancellation, done, control, sink) = {
            let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            if inner.active.is_some() {
                return Err(LegacyAgentError::InvalidContext(
                    "Agent is already processing".to_owned(),
                ));
            }
            let cancellation = LegacyCancellation::new();
            let done = Arc::new(Notify::new());
            inner.state.is_streaming = true;
            inner.state.streaming_message = None;
            inner.state.error_message = None;
            inner.state.pending_tool_calls.clear();
            inner.active = Some(ActiveRun {
                cancellation: cancellation.clone(),
                done: done.clone(),
            });
            let context = LegacyContext {
                system_prompt: inner.state.system_prompt.clone(),
                messages: inner.state.messages.clone(),
                tools: inner
                    .options
                    .tools
                    .iter()
                    .map(|tool| tool.descriptor().clone())
                    .collect(),
            };
            let control = Arc::new(QueueControl {
                inner: self.inner.clone(),
            });
            let sink = Arc::new(FacadeSink {
                inner: self.inner.clone(),
                cancellation: cancellation.clone(),
            });
            (
                inner.options.clone(),
                context,
                cancellation,
                done,
                control,
                sink,
            )
        };
        let config = LegacyLoopConfig {
            model: options.model,
            transform_context: options.transform_context,
            convert_to_provider: options.convert_to_provider,
            stream: options.stream,
            cancellation: cancellation.clone(),
        };
        let result = if continuation {
            run_agent_loop_with_tools_and_control(
                prompts,
                context,
                config,
                options.tools,
                Some(control),
                sink,
            )
            .await
            .map(|_| ())
        } else {
            run_agent_loop_with_tools_and_control(
                prompts,
                context,
                config,
                options.tools,
                Some(control),
                sink,
            )
            .await
            .map(|_| ())
        };
        {
            let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            inner.state.is_streaming = false;
            inner.state.streaming_message = None;
            inner.state.pending_tool_calls.clear();
            inner.active = None;
        }
        done.notify_waiters();
        result
    }
}

struct QueueControl {
    inner: Arc<Mutex<Inner>>,
}

impl LegacyLoopControl for QueueControl {
    fn poll_steering(&self) -> BoxFuture<'_, Vec<AgentMessage>> {
        let messages = {
            let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            let mode = inner.queues.steering_mode;
            drain_queue(&mut inner.queues.steering, mode)
        };
        Box::pin(async move { messages })
    }

    fn poll_follow_up(&self) -> BoxFuture<'_, Vec<AgentMessage>> {
        let messages = {
            let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            let mode = inner.queues.follow_up_mode;
            drain_queue(&mut inner.queues.follow_up, mode)
        };
        Box::pin(async move { messages })
    }
}

fn drain_queue(queue: &mut VecDeque<AgentMessage>, mode: QueueMode) -> Vec<AgentMessage> {
    match mode {
        QueueMode::All => queue.drain(..).collect(),
        QueueMode::OneAtATime => queue.pop_front().into_iter().collect(),
    }
}

struct FacadeSink {
    inner: Arc<Mutex<Inner>>,
    cancellation: Arc<LegacyCancellation>,
}

impl AgentEventSink for FacadeSink {
    fn emit(
        &self,
        event: AgentEvent,
    ) -> BoxFuture<'_, Result<(), pi_agent_schema::HarnessRuntimeError>> {
        let listeners = {
            let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            reduce_state(&mut inner.state, &event);
            inner
                .listeners
                .iter()
                .map(|(_, listener)| listener.clone())
                .collect::<Vec<_>>()
        };
        let event_for_listeners = event.clone();
        let cancellation: Arc<dyn Cancellation> = self.cancellation.clone();
        Box::pin(async move {
            for listener in listeners {
                listener(event_for_listeners.clone(), cancellation.clone())
                    .await
                    .map_err(|error| {
                        pi_agent_schema::HarnessRuntimeError::Fault(
                            pi_agent_schema::HarnessFault::new(
                                pi_agent_schema::ErrorContext::new(error.to_string())
                                    .expect("legacy error is nonempty"),
                            ),
                        )
                    })?;
            }
            Ok(())
        })
    }
}

fn reduce_state(state: &mut AgentState, event: &AgentEvent) {
    match event {
        AgentEvent::MessageStart { message } | AgentEvent::MessageUpdate { message, .. } => {
            state.streaming_message = match message {
                LiveAgentMessage::Settled(message) => Some(message.clone()),
                LiveAgentMessage::Assistant(message) => {
                    Some(AgentMessage::BuiltIn(Message::Assistant(message.clone())))
                }
            };
        }
        AgentEvent::MessageEnd { message } => {
            state.streaming_message = None;
            state.messages.push(message.clone());
        }
        AgentEvent::ToolExecutionStart { tool_call_id, .. } => {
            state.pending_tool_calls.push(tool_call_id.clone());
        }
        AgentEvent::ToolExecutionEnd { tool_call_id, .. } => {
            state.pending_tool_calls.retain(|id| id != tool_call_id);
        }
        AgentEvent::TurnEnd { message, .. } => {
            if let AgentMessage::BuiltIn(Message::Assistant(message)) = message {
                state.error_message = message.error_message.clone();
            }
        }
        AgentEvent::AgentEnd { .. } => state.streaming_message = None,
        AgentEvent::ToolExecutionUpdate { .. } | AgentEvent::AgentStart | AgentEvent::TurnStart => {
        }
    }
}
