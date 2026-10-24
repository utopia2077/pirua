use futures_util::FutureExt;
use pi_agent_loop::*;
use pi_ai_core::{
    ImageContent, InputContent, Message, Model, SafeUnsigned, TextContent, UserContent,
    UserMessage, create_initial_system_message, get_current_system_message,
    get_current_system_prompt,
};
use std::{
    collections::{BTreeSet, VecDeque},
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Clone, Default)]
pub struct AgentInitialState {
    pub system_prompt: Option<String>,
    pub messages: Vec<AgentMessage>,
    pub tools: Vec<AgentTool>,
    pub thinking_level: Option<ThinkingLevel>,
}

pub struct AgentOptions {
    pub initial_state: AgentInitialState,
    pub loop_config: AgentLoopConfig,
    pub steering_mode: QueueMode,
    pub follow_up_mode: QueueMode,
}
impl AgentOptions {
    pub fn new(model: Model, stream: StreamFn) -> Self {
        Self {
            initial_state: AgentInitialState::default(),
            loop_config: AgentLoopConfig::new(model, stream),
            steering_mode: QueueMode::default(),
            follow_up_mode: QueueMode::default(),
        }
    }
}

/// Detached snapshot. Mutation methods on Agent replace future runtime inputs.
#[derive(Clone, Debug)]
pub struct AgentState {
    pub system_prompt: String,
    pub model: Model,
    pub thinking_level: ThinkingLevel,
    pub tools: Vec<AgentTool>,
    pub messages: Vec<AgentMessage>,
    pub is_streaming: bool,
    pub streaming_message: Option<AgentMessage>,
    pub pending_tool_calls: BTreeSet<String>,
    pub error_message: Option<String>,
}
struct Queue {
    messages: VecDeque<AdmittedMessage>,
    mode: QueueMode,
}
impl Queue {
    fn peek(&self) -> Vec<AgentMessage> {
        let count = if self.mode == QueueMode::All {
            self.messages.len()
        } else {
            1
        };
        self.messages
            .iter()
            .take(count)
            .map(|m| m.message.clone())
            .collect()
    }
    fn drain(&mut self) -> Vec<AdmittedMessage> {
        let count = if self.mode == QueueMode::All {
            self.messages.len()
        } else {
            self.messages.len().min(1)
        };
        self.messages.drain(..count).collect()
    }
}
struct ActiveRun {
    cancellation: CancellationToken,
    completed: tokio::sync::watch::Receiver<bool>,
}
struct State {
    state: AgentState,
    steering: Queue,
    follow_up: Queue,
    active: Option<ActiveRun>,
}
struct Inner {
    state: Mutex<State>,
    config: Mutex<AgentLoopConfig>,
    listeners: Mutex<Vec<(u64, InputEventSink)>>,
    listener_id: std::sync::atomic::AtomicU64,
}

#[derive(Clone)]
pub struct Agent {
    inner: Arc<Inner>,
}

/// Dropping a prompt future cancels its token and releases run ownership.
struct RunGuard {
    agent: Agent,
    completion: tokio::sync::watch::Sender<bool>,
    cancel_on_drop: bool,
}
impl Drop for RunGuard {
    fn drop(&mut self) {
        let active = {
            let mut state = self
                .agent
                .inner
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let active = state.active.take();
            state.state.is_streaming = false;
            state.state.streaming_message = None;
            state.state.pending_tool_calls.clear();
            active
        };
        if self.cancel_on_drop
            && let Some(active) = active
        {
            active.cancellation.cancel();
        }
        let _ = self.completion.send(true);
    }
}

impl Agent {
    pub fn new(mut options: AgentOptions) -> Self {
        options
            .loop_config
            .options
            .stream
            .transport
            .get_or_insert(pi_ai_core::Transport::Auto);
        let AgentInitialState {
            system_prompt,
            mut messages,
            tools,
            thinking_level,
        } = options.initial_state;
        if messages
            .first()
            .is_none_or(|message| message.role() != "system")
        {
            let declarations = tools
                .iter()
                .map(|tool| tool.declaration.clone())
                .collect::<Vec<_>>();
            if let Some(initial) =
                create_initial_system_message(system_prompt.as_deref(), Some(&declarations))
            {
                messages.insert(0, AgentMessage::BuiltIn(Message::System(initial)));
            }
        }
        let state = AgentState {
            system_prompt: String::new(),
            model: options.loop_config.model.clone(),
            thinking_level: thinking_level
                .unwrap_or_else(|| requested_thinking(options.loop_config.options.reasoning)),
            tools,
            messages,
            is_streaming: false,
            streaming_message: None,
            pending_tool_calls: BTreeSet::new(),
            error_message: None,
        };
        Self {
            inner: Arc::new(Inner {
                config: Mutex::new(options.loop_config),
                state: Mutex::new(State {
                    state,
                    steering: Queue {
                        messages: VecDeque::new(),
                        mode: options.steering_mode,
                    },
                    follow_up: Queue {
                        messages: VecDeque::new(),
                        mode: options.follow_up_mode,
                    },
                    active: None,
                }),
                listeners: Mutex::new(Vec::new()),
                listener_id: std::sync::atomic::AtomicU64::new(1),
            }),
        }
    }

    pub fn state(&self) -> AgentState {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .state
            .clone();
        let messages = state
            .messages
            .iter()
            .filter_map(|message| message.as_builtin().cloned())
            .collect::<Vec<_>>();
        state.system_prompt = get_current_system_prompt(&messages);
        state
    }
    /// Snapshot configuration for subsequent runs, including the current model and
    /// requested thinking level. Active runs retain the snapshot they started with.
    pub fn configuration(&self) -> AgentLoopConfig {
        let state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let mut config = self
            .inner
            .config
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        config.model = state.state.model.clone();
        config.options.reasoning = provider_thinking(state.state.thinking_level);
        config
    }

    /// Replace configuration for future runs. Agent owns its fresh cancellation
    /// token and queues; their low-level config fields are replaced at run entry.
    pub fn set_configuration(&self, config: AgentLoopConfig) {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state.state.model = config.model.clone();
        state.state.thinking_level = requested_thinking(config.options.reasoning);
        *self
            .inner
            .config
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = config;
    }

    pub fn set_model(&self, model: Model) {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .state
            .model = model;
    }
    pub fn set_thinking_level(&self, level: ThinkingLevel) {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .state
            .thinking_level = level;
    }
    pub fn set_tools(&self, tools: Vec<AgentTool>) {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .state
            .tools = tools;
    }
    pub fn replace_messages(&self, messages: Vec<AgentMessage>) {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .state
            .messages = messages;
    }
    pub fn append_message(&self, message: AgentMessage) {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .state
            .messages
            .push(message);
    }

    pub fn subscribe(&self, listener: AgentEventSink) -> u64 {
        self.subscribe_inputs(Arc::new(move |event, _, signal| listener(event, signal)))
    }
    /// Observe exact transient admission identity without changing AgentEvent serialization.
    pub fn subscribe_inputs(&self, listener: InputEventSink) -> u64 {
        let id = self
            .inner
            .listener_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.inner
            .listeners
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .push((id, listener));
        id
    }
    pub fn unsubscribe(&self, id: u64) {
        self.inner
            .listeners
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .retain(|(registered, _)| *registered != id);
    }

    pub fn steer(&self, message: AgentMessage) {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .steering
            .messages
            .push_back(message.into());
    }
    pub fn follow_up(&self, message: AgentMessage) {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .follow_up
            .messages
            .push_back(message.into());
    }
    pub fn steer_input(&self, message: AdmittedMessage) {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .steering
            .messages
            .push_back(message);
    }
    pub fn follow_up_input(&self, message: AdmittedMessage) {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .follow_up
            .messages
            .push_back(message);
    }
    /// Returns the actual FIFO contents, including sidecars, independently of queue mode.
    pub fn queued_inputs(&self) -> (Vec<AdmittedMessage>, Vec<AdmittedMessage>) {
        let state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        (
            state.steering.messages.iter().cloned().collect(),
            state.follow_up.messages.iter().cloned().collect(),
        )
    }
    /// Withdraw only records that have not been drained into an executing loop.
    pub fn take_queued_inputs(&self) -> (Vec<AdmittedMessage>, Vec<AdmittedMessage>) {
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        (
            state.steering.messages.drain(..).collect(),
            state.follow_up.messages.drain(..).collect(),
        )
    }
    pub fn remove_queued_input(&self, id: InputId) -> bool {
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        let State {
            steering,
            follow_up,
            ..
        } = &mut *state;
        for queue in [steering, follow_up] {
            if let Some(index) = queue
                .messages
                .iter()
                .position(|m| m.input.as_ref().is_some_and(|i| i.id == id))
            {
                queue.messages.remove(index);
                return true;
            }
        }
        false
    }
    pub fn set_steering_mode(&self, mode: QueueMode) {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .steering
            .mode = mode;
    }
    pub fn set_follow_up_mode(&self, mode: QueueMode) {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .follow_up
            .mode = mode;
    }
    pub fn steering_mode(&self) -> QueueMode {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .steering
            .mode
    }
    pub fn follow_up_mode(&self) -> QueueMode {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .follow_up
            .mode
    }
    pub fn clear_steering_queue(&self) {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .steering
            .messages
            .clear();
    }
    pub fn clear_follow_up_queue(&self) {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .follow_up
            .messages
            .clear();
    }
    pub fn clear_all_queues(&self) {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state.steering.messages.clear();
        state.follow_up.messages.clear();
    }
    pub fn has_queued_messages(&self) -> bool {
        let state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        !state.steering.messages.is_empty() || !state.follow_up.messages.is_empty()
    }
    pub fn peek_queued_messages(&self) -> Vec<AgentMessage> {
        let state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if state.steering.messages.is_empty() {
            state.follow_up.peek()
        } else {
            state.steering.peek()
        }
    }
    pub fn signal(&self) -> Option<CancellationToken> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .active
            .as_ref()
            .map(|run| run.cancellation.clone())
    }
    pub fn abort(&self) {
        if let Some(signal) = self.signal() {
            signal.cancel();
        }
    }
    pub async fn wait_for_idle(&self) {
        let receiver = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .active
            .as_ref()
            .map(|run| run.completed.clone());
        if let Some(mut receiver) = receiver {
            while !*receiver.borrow_and_update() {
                if receiver.changed().await.is_err() {
                    break;
                }
            }
        }
    }
    pub fn reset(&self) -> Result<(), AgentError> {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if state.active.is_some() {
            return Err(AgentError::Busy);
        }
        let messages = state
            .state
            .messages
            .iter()
            .filter_map(|message| message.as_builtin().cloned())
            .collect::<Vec<_>>();
        state.state.messages = get_current_system_message(&messages)
            .map(|message| AgentMessage::BuiltIn(Message::System(message)))
            .into_iter()
            .collect();
        state.state.error_message = None;
        state.state.streaming_message = None;
        state.state.pending_tool_calls.clear();
        state.steering.messages.clear();
        state.follow_up.messages.clear();
        Ok(())
    }

    pub async fn prompt(
        &self,
        messages: Vec<AgentMessage>,
    ) -> Result<Vec<AgentMessage>, AgentError> {
        self.run(Some(messages.into_iter().map(Into::into).collect()))
            .await
    }
    pub async fn prompt_inputs(
        &self,
        messages: Vec<AdmittedMessage>,
    ) -> Result<Vec<AgentMessage>, AgentError> {
        self.run(Some(messages)).await
    }
    pub async fn prompt_text(
        &self,
        text: impl Into<String>,
        images: Vec<ImageContent>,
    ) -> Result<Vec<AgentMessage>, AgentError> {
        let mut content = vec![InputContent::Text(TextContent {
            content_type: Default::default(),
            text: text.into(),
            text_signature: None,
        })];
        content.extend(images.into_iter().map(InputContent::Image));
        let timestamp = u64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
        )
        .unwrap_or(u64::MAX)
        .min(pi_ai_core::JS_SAFE_INTEGER_MAX);
        self.prompt(vec![AgentMessage::BuiltIn(Message::User(UserMessage {
            role: Default::default(),
            content: UserContent::Blocks(content),
            timestamp: SafeUnsigned::new(timestamp).expect("safe clock"),
        }))])
        .await
    }
    pub async fn continue_run(&self) -> Result<Vec<AgentMessage>, AgentError> {
        self.run(None).await
    }

    async fn run(
        &self,
        mut prompts: Option<Vec<AdmittedMessage>>,
    ) -> Result<Vec<AgentMessage>, AgentError> {
        let mut skip_initial = false;
        let cancellation = CancellationToken::new();
        let (sender, receiver) = tokio::sync::watch::channel(false);
        let (context, config) = {
            let mut state = self
                .inner
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if state.active.is_some() {
                return Err(AgentError::Busy);
            }
            if prompts.is_none() {
                let no_history = state.state.messages.is_empty()
                    || state
                        .state
                        .messages
                        .iter()
                        .all(|message| message.role() == "system");
                if no_history
                    || state
                        .state
                        .messages
                        .last()
                        .is_some_and(|message| message.role() == "assistant")
                {
                    let steering = state.steering.drain();
                    if !steering.is_empty() {
                        prompts = Some(steering);
                        skip_initial = true;
                    } else {
                        let follow = state.follow_up.drain();
                        if follow.is_empty() {
                            return Err(AgentError::InvalidContext(
                                if no_history {
                                    "No messages to continue from"
                                } else {
                                    "Cannot continue from message role: assistant"
                                }
                                .into(),
                            ));
                        }
                        prompts = Some(follow);
                    }
                }
            }
            let mut config = self
                .inner
                .config
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .clone();
            config.model = state.state.model.clone();
            config.options.reasoning = provider_thinking(state.state.thinking_level);
            config.cancellation = cancellation.clone();
            let context = AgentContext {
                messages: state.state.messages.clone(),
                tools: state.state.tools.clone(),
            };
            state.state.is_streaming = true;
            state.state.streaming_message = None;
            state.state.error_message = None;
            state.active = Some(ActiveRun {
                cancellation: cancellation.clone(),
                completed: receiver,
            });
            (context, config)
        };
        let mut guard = RunGuard {
            agent: self.clone(),
            completion: sender,
            cancel_on_drop: true,
        };
        let skip = Arc::new(std::sync::atomic::AtomicBool::new(skip_initial));
        let agent = self.clone();
        let steering: Option<PollInputs> = Some(Arc::new(move || {
            let (agent, skip) = (agent.clone(), skip.clone());
            Box::pin(async move {
                if skip.swap(false, std::sync::atomic::Ordering::Relaxed) {
                    return Ok(Vec::new());
                }
                Ok(agent
                    .inner
                    .state
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .steering
                    .drain())
            })
        }));
        let agent = self.clone();
        let follow_up: Option<PollInputs> = Some(Arc::new(move || {
            let agent = agent.clone();
            Box::pin(async move {
                Ok(agent
                    .inner
                    .state
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .follow_up
                    .drain())
            })
        }));
        let agent = self.clone();
        let sink: AgentEventSink = Arc::new(move |event, cancellation| {
            let agent = agent.clone();
            Box::pin(async move { agent.process_event(event, cancellation).await })
        });
        let agent = self.clone();
        let inputs = InputLoopOptions {
            steering,
            follow_up,
            events: Some(Arc::new(move |event, input, cancellation| {
                let agent = agent.clone();
                Box::pin(async move { agent.process_input_event(event, input, cancellation).await })
            })),
        };
        let message_hooks = config.message_hooks.clone();
        let result = std::panic::AssertUnwindSafe(async {
            if let Some(prompts) = prompts {
                run_agent_loop_with_inputs(prompts, context, config, sink, inputs).await
            } else {
                run_agent_loop_continue_with_inputs(context, config, sink, inputs).await
            }
        })
        .catch_unwind()
        .await
        .unwrap_or_else(|_| Err(AgentError::Callback("Agent callback panicked".into())));
        match result {
            Ok(messages) => {
                guard.cancel_on_drop = false;
                Ok(messages)
            }
            Err(error) => {
                let message = AgentMessage::BuiltIn(Message::Assistant(Box::new(failure_message(
                    &self.state().model,
                    &error,
                    cancellation.is_cancelled(),
                ))));
                let message = message_hooks.start(message, cancellation.clone()).await?;
                self.process_event(
                    AgentEvent::MessageStart {
                        message: message.clone(),
                    },
                    cancellation.clone(),
                )
                .await?;
                let message = message_hooks.end(message, cancellation.clone()).await?;
                for event in [
                    AgentEvent::MessageEnd {
                        message: message.clone(),
                    },
                    AgentEvent::TurnEnd {
                        message: message.clone(),
                        tool_results: Vec::new(),
                    },
                    AgentEvent::AgentEnd {
                        messages: vec![message.clone()],
                    },
                ] {
                    self.process_event(event, cancellation.clone()).await?;
                }
                guard.cancel_on_drop = false;
                Ok(vec![message])
            }
        }
    }

    async fn process_event(
        &self,
        event: AgentEvent,
        cancellation: CancellationToken,
    ) -> Result<(), AgentError> {
        self.process_input_event(event, None, cancellation).await
    }
    async fn process_input_event(
        &self,
        event: AgentEvent,
        input: Option<InputId>,
        cancellation: CancellationToken,
    ) -> Result<(), AgentError> {
        {
            let mut state = self
                .inner
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            match &event {
                AgentEvent::MessageStart { message }
                | AgentEvent::MessageUpdate { message, .. } => {
                    state.state.streaming_message = Some(message.clone())
                }
                AgentEvent::MessageEnd { message } => {
                    state.state.streaming_message = None;
                    state.state.messages.push(message.clone());
                }
                AgentEvent::ToolExecutionStart { tool_call_id, .. } => {
                    state.state.pending_tool_calls.insert(tool_call_id.clone());
                }
                AgentEvent::ToolExecutionEnd { tool_call_id, .. } => {
                    state.state.pending_tool_calls.remove(tool_call_id);
                }
                AgentEvent::TurnEnd {
                    message: AgentMessage::BuiltIn(Message::Assistant(message)),
                    ..
                } => {
                    if let Some(error) = &message.error_message {
                        state.state.error_message = Some(error.clone());
                    }
                }
                AgentEvent::AgentEnd { .. } => state.state.streaming_message = None,
                _ => {}
            }
        }
        let listeners = self
            .inner
            .listeners
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        for (_, listener) in listeners {
            listener(event.clone(), input, cancellation.clone()).await?;
        }
        Ok(())
    }
}
