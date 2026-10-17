//! Saved headless coding sessions over the transient Agent.
use crate::{
    BuildSystemPromptOptions, CompactionSettings, ModelRuntime, Resources,
    builder::CodingSessionBuilder, hooks::*, messages,
};
use pi_agent::*;
use pi_ai::{
    AssistantMessage, Message, Model, ModelThinkingLevel, SafeUnsigned, StopReason, Usage,
};
use pi_coding_session::{
    ContentReplacement, EntryContent, SessionEntry, SessionManager, SessionProjection,
    SessionSnapshot,
};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc, Mutex, OnceLock, Weak,
        atomic::{AtomicU64, Ordering},
    },
};

#[derive(Debug)]
pub enum SessionError {
    Agent(AgentError),
    Storage(pi_coding_session::Error),
    Invalid(String),
    Disposed,
}
impl std::fmt::Display for SessionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Agent(e) => e.fmt(f),
            Self::Storage(e) => e.fmt(f),
            Self::Invalid(m) => f.write_str(m),
            Self::Disposed => f.write_str("CodingSession is disposed"),
        }
    }
}
impl std::error::Error for SessionError {}
impl From<AgentError> for SessionError {
    fn from(e: AgentError) -> Self {
        Self::Agent(e)
    }
}
impl From<pi_coding_session::Error> for SessionError {
    fn from(e: pi_coding_session::Error) -> Self {
        Self::Storage(e)
    }
}
impl From<serde_json::Error> for SessionError {
    fn from(e: serde_json::Error) -> Self {
        Self::Invalid(e.to_string())
    }
}
pub(crate) fn agent_error(error: SessionError) -> AgentError {
    AgentError::Callback(error.to_string())
}
pub(crate) fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ToolExposure {
    #[default]
    Direct,
    ModelOnly,
    CodeMode,
    Deferred,
    Hidden,
}
#[derive(Clone, Debug)]
pub struct RegisteredTool {
    pub tool: AgentTool,
    pub exposure: ToolExposure,
    pub source: Option<String>,
    pub default_active: bool,
    pub namespace: Option<String>,
    pub prompt_snippet: Option<String>,
    pub prompt_guidelines: Vec<String>,
}
impl RegisteredTool {
    pub fn direct(tool: AgentTool) -> Self {
        Self {
            tool,
            exposure: ToolExposure::Direct,
            source: None,
            default_active: true,
            namespace: None,
            prompt_snippet: None,
            prompt_guidelines: Vec::new(),
        }
    }
}
#[derive(Clone, Debug)]
pub struct ToolInfo {
    pub declaration: pi_ai::Tool,
    pub exposure: ToolExposure,
    pub source: Option<String>,
    pub namespace: Option<String>,
}
#[derive(Clone, Debug)]
pub struct PromptOptions {
    pub images: Vec<pi_ai::ImageContent>,
    pub delivery: Option<Delivery>,
    pub source: InputSource,
    pub expand_prompt_templates: bool,
}
impl Default for PromptOptions {
    fn default() -> Self {
        Self {
            images: Vec::new(),
            delivery: None,
            source: InputSource::Interactive,
            expand_prompt_templates: true,
        }
    }
}
#[derive(Clone, Debug, Default)]
pub struct NavigateOptions {
    pub summarize: bool,
    pub custom_instructions: Option<String>,
    pub replace_instructions: bool,
    pub label: Option<String>,
}
#[derive(Clone, Debug, Default)]
pub struct NavigationResult {
    pub editor_text: Option<String>,
    pub cancelled: bool,
    pub aborted: bool,
    pub summary_entry: Option<SessionEntry>,
}
#[derive(Clone, Debug)]
pub struct ScopedModel {
    pub model: Model,
    pub thinking_level: Option<ModelThinkingLevel>,
}
#[derive(Clone, Debug, Default)]
pub struct SessionStats {
    pub user_messages: usize,
    pub assistant_messages: usize,
    pub tool_calls: usize,
    pub tool_results: usize,
    pub total_messages: usize,
    pub usage: Option<Usage>,
    pub context_usage: Option<crate::ContextUsage>,
}

pub(crate) struct RuntimeState {
    pub active: bool,
    pub disposed: bool,
    pub disposing: bool,
    pub operation: Option<CancellationToken>,
    pub outcome: ActivityOutcome,
    pub last_error: Option<String>,
    pub steering: Vec<String>,
    pub follow_up: Vec<String>,
    pub pending_custom: Vec<AgentMessage>,
    pub pending_bash: Vec<AgentMessage>,
    pub bash: BTreeMap<u64, CancellationToken>,
    pub next_bash: u64,
    pub next_turn: Vec<AgentMessage>,
    pub tools: Vec<RegisteredTool>,
    pub active_tools: Vec<String>,
    pub pending_tools: Vec<String>,
    pub prompt_options: BuildSystemPromptOptions,
    pub hidden_declarations: BTreeSet<String>,
    pub descriptions: BTreeMap<String, String>,
    pub run_prompt: Option<String>,
    pub resources: Resources,
    pub compaction: CompactionSettings,
    pub retry: pi_ai::RetryPolicy,
    pub retry_attempt: u64,
    pub overflow_attempted: bool,
    pub failed: Option<AssistantMessage>,
    pub routed: Option<(Model, ModelThinkingLevel)>,
    pub scoped_models: Vec<ScopedModel>,
    pub uses_default_tools: bool,
    pub local_environment: bool,
    pub image_auto_resize: bool,
    pub block_images: bool,
    pub turn_index: usize,
    pub nested: BTreeMap<String, (Vec<pi_ai::NestedToolCallRecord>, Option<Usage>, bool)>,
    pub nested_next: BTreeMap<String, u64>,
    pub nested_roots: BTreeMap<String, String>,
    pub nested_holds: BTreeSet<String>,
}
pub(crate) struct Inner {
    pub manager: Mutex<SessionManager>,
    pub state: Mutex<RuntimeState>,
    pub agent: OnceLock<Agent>,
    pub hooks: Arc<dyn SessionHooks>,
    pub models: Arc<dyn ModelRuntime>,
    pub listeners: Mutex<Vec<(u64, SessionEventSink)>>,
    pub next_listener: AtomicU64,
    pub navigation_generation: AtomicU64,
    pub idle: tokio::sync::Notify,
    pub gate: Arc<tokio::sync::Mutex<()>>,
    pub stream: StreamFn,
    pub summary_ids: pi_ai::UuidV7Generator,
    pub warmer: Mutex<crate::cache_warmer::Warmer>,
    pub nested_gate: Arc<tokio::sync::Mutex<()>>,
    pub execution_env: Mutex<Arc<dyn crate::ExecutionEnv>>,
}
#[derive(Clone)]
pub struct CodingSession {
    pub(crate) inner: Arc<Inner>,
}
fn upgrade(weak: &Weak<Inner>) -> Result<CodingSession, AgentError> {
    weak.upgrade()
        .map(|inner| CodingSession { inner })
        .ok_or_else(|| AgentError::Callback("CodingSession is disposed".into()))
}
impl CodingSession {
    pub(crate) async fn build(mut b: CodingSessionBuilder) -> Result<Self, SessionError> {
        if b.manager.entries().is_empty() {
            b.manager.set_model(
                b.config.model.provider.to_string(),
                b.config.model.id.to_string(),
            )?;
            let level = pi_ai::clamp_thinking_level(
                &b.config.model,
                b.thinking_level
                    .or_else(|| b.settings.as_ref().map(|s| s.thinking_level))
                    .unwrap_or_else(crate::default_thinking_level),
            );
            b.manager.set_thinking_level(
                serde_json::to_value(level)?
                    .as_str()
                    .expect("thinking")
                    .to_owned(),
            )?;
        }
        let projection = b.manager.projection();
        let saved_model =
            crate::branch_model_selection(&b.manager.get_branch(), b.model_runtime.as_ref())
                .and_then(|(p, id)| b.model_runtime.get_model(&p, &id));
        if let Some(model) = saved_model {
            b.config.model = model;
        }
        let settings = b.settings.take();
        let thinking = b
            .thinking_level
            .or_else(|| {
                projection
                    .thinking_level
                    .parse::<String>()
                    .ok()
                    .and_then(|s| serde_json::from_value::<ModelThinkingLevel>(json!(s)).ok())
            })
            .or_else(|| settings.as_ref().map(|s| s.thinking_level))
            .unwrap_or_else(crate::default_thinking_level);
        let thinking = pi_ai::clamp_thinking_level(&b.config.model, thinking);
        let initial = projection
            .messages
            .iter()
            .cloned()
            .map(messages::decode)
            .collect::<Result<Vec<_>, _>>()?;
        let builtin = initial
            .iter()
            .filter_map(|m| m.as_builtin().cloned())
            .collect::<Vec<_>>();
        let restored = pi_ai::get_current_tools(&builtin)
            .into_iter()
            .map(|t| t.name.to_string())
            .collect::<Vec<_>>();
        let requested = settings
            .as_ref()
            .map(|s| s.active_tools.iter().map(ToString::to_string).collect())
            .unwrap_or_else(|| vec!["read".into(), "bash".into(), "edit".into(), "write".into()]);
        let active = if builtin.iter().any(|m| matches!(m, Message::System(_))) {
            restored
        } else {
            requested
        };
        let registered = b
            .tools
            .iter()
            .map(|t| t.tool.declaration.name.to_string())
            .collect::<BTreeSet<_>>();
        let pending_tools = active
            .iter()
            .filter(|name| !registered.contains(*name))
            .cloned()
            .collect();
        let active_tools = active
            .into_iter()
            .filter(|name| registered.contains(name))
            .collect::<Vec<_>>();
        b.prompt_options.resources = b.resources.clone();
        let retry = settings
            .as_ref()
            .map(|s| s.retry.clone())
            .unwrap_or(pi_ai::RetryPolicy {
                enabled: true,
                max_retries: pi_ai::MaxRetries::new(3).expect("default retries"),
                base_delay_ms: SafeUnsigned::new(2000).expect("default delay"),
                max_agent_delay_ms: Some(SafeUnsigned::new(60000).expect("default cap")),
            });
        if let Some(settings) = &settings {
            b.config.options.stream = settings.stream.clone();
        }
        let inner = Arc::new(Inner {
            manager: Mutex::new(b.manager),
            state: Mutex::new(RuntimeState {
                active: false,
                disposed: false,
                disposing: false,
                operation: None,
                outcome: ActivityOutcome::Completed,
                last_error: None,
                steering: Vec::new(),
                follow_up: Vec::new(),
                pending_custom: Vec::new(),
                pending_bash: Vec::new(),
                bash: BTreeMap::new(),
                next_bash: 0,
                next_turn: Vec::new(),
                tools: b.tools,
                active_tools,
                pending_tools,
                prompt_options: b.prompt_options,
                hidden_declarations: BTreeSet::new(),
                descriptions: BTreeMap::new(),
                run_prompt: None,
                resources: b.resources,
                compaction: settings
                    .as_ref()
                    .map(|s| s.compaction.clone())
                    .unwrap_or_default(),
                retry,
                retry_attempt: 0,
                overflow_attempted: false,
                failed: None,
                routed: None,
                scoped_models: Vec::new(),
                uses_default_tools: b.uses_default_tools,
                local_environment: b.local_environment,
                image_auto_resize: b.image_auto_resize,
                block_images: b.block_images,
                turn_index: 0,
                nested: BTreeMap::new(),
                nested_next: BTreeMap::new(),
                nested_roots: BTreeMap::new(),
                nested_holds: BTreeSet::new(),
            }),
            agent: OnceLock::new(),
            hooks: b.hooks,
            models: b.model_runtime,
            listeners: Mutex::new(Vec::new()),
            next_listener: AtomicU64::new(1),
            navigation_generation: AtomicU64::new(0),
            idle: tokio::sync::Notify::new(),
            gate: Arc::new(tokio::sync::Mutex::new(())),
            stream: b.config.stream.clone(),
            summary_ids: crate::compaction::summary_ids(),
            warmer: Mutex::new(crate::cache_warmer::Warmer::new(b.cache_warming)),
            nested_gate: Arc::new(tokio::sync::Mutex::new(())),
            execution_env: Mutex::new(b.execution_env),
        });
        let session = Self { inner };
        session.inner.hooks.bind_session(session.downgrade())?;
        let mut config = b.config;
        config
            .options
            .stream
            .session_id
            .get_or_insert_with(|| session.session_id());
        let previous_prepare = config.prepare_request.clone();
        let weak = Arc::downgrade(&session.inner);
        config.prepare_request = Some(Arc::new(move |request, signal| {
            let weak = weak.clone();
            let previous = previous_prepare.clone();
            Box::pin(async move {
                upgrade(&weak)?
                    .prepare_request(request, signal, previous)
                    .await
            })
        }));
        let previous_finish = config.finish_turn.clone();
        let weak = Arc::downgrade(&session.inner);
        config.finish_turn = Some(Arc::new(move |turn, signal| {
            let weak = weak.clone();
            let previous = previous_finish.clone();
            Box::pin(async move {
                let s = upgrade(&weak)?;
                let more = s
                    .boundary(BoundaryKind::TurnEnd, Some(turn.clone()), signal.clone())
                    .await
                    .map_err(agent_error)?;
                let prior = if let Some(h) = previous {
                    h(turn, signal).await?
                } else {
                    None
                };
                Ok(if prior == Some(AgentTurnDecision::End) {
                    prior
                } else if more || prior == Some(AgentTurnDecision::Continue) {
                    Some(AgentTurnDecision::Continue)
                } else {
                    None
                })
            })
        }));
        let previous_next = config.prepare_next_turn.clone();
        let weak = Arc::downgrade(&session.inner);
        config.prepare_next_turn = Some(Arc::new(move |turn, signal| {
            let weak = weak.clone();
            let previous = previous_next.clone();
            Box::pin(async move {
                let s = upgrade(&weak)?;
                let prior = if let Some(h) = previous {
                    h(turn, signal.clone()).await?
                } else {
                    None
                };
                s.flush_custom().await.map_err(agent_error)?;
                s.refresh_loadout(signal).await.map_err(agent_error)?;
                let patch = s.prompt_patch().map_err(agent_error)?;
                let context = AgentContext {
                    messages: s.messages().map_err(agent_error)?,
                    tools: s.active_executable_tools(),
                };
                Ok(Some(AgentLoopTurnUpdate {
                    request: AgentRequestUpdate {
                        context: Some(context),
                        model: Some(s.model()),
                        thinking_level: Some(s.thinking_level()),
                    },
                    messages: prior
                        .into_iter()
                        .flat_map(|p| p.messages)
                        .chain(patch)
                        .collect(),
                }))
            })
        }));
        let previous_transform = config.transform_context.clone();
        let weak = Arc::downgrade(&session.inner);
        config.transform_context = Some(Arc::new(move |messages, signal| {
            let weak = weak.clone();
            let previous = previous_transform.clone();
            Box::pin(async move {
                let s = upgrade(&weak)?;
                let messages = s.inner.hooks.context(messages, signal.clone()).await?;
                let messages = if let Some(p) = previous {
                    p(messages, signal).await?
                } else {
                    messages
                };
                s.project_request_messages(messages)
            })
        }));
        let weak = Arc::downgrade(&session.inner);
        config.convert_to_llm = Arc::new(move |messages| {
            let weak = weak.clone();
            Box::pin(async move {
                let session = upgrade(&weak)?;
                let mut messages = crate::messages::convert_to_llm(messages)?;
                if lock(&session.inner.state).block_images {
                    for message in &mut messages {
                        match message {
                            Message::User(user) => {
                                if let pi_ai::UserContent::Blocks(blocks) = &mut user.content {
                                    replace_blocked_images(blocks);
                                }
                            }
                            Message::ToolResult(result) => {
                                replace_blocked_images(&mut result.content)
                            }
                            _ => {}
                        }
                    }
                }
                Ok(messages)
            })
        });
        let previous_hooks = config.message_hooks.clone();
        let weak = Arc::downgrade(&session.inner);
        config.message_hooks.start = Some(Arc::new(move |mut message, signal| {
            let weak = weak.clone();
            let prior = previous_hooks.clone();
            Box::pin(async move {
                let s = upgrade(&weak)?;
                if let AgentMessage::BuiltIn(Message::ToolResult(result)) = &mut message
                    && let Some((calls, usage, complete)) = lock(&s.inner.state)
                        .nested
                        .remove(result.tool_call_id.as_str())
                {
                    if !calls.is_empty() {
                        let complete = complete
                            && calls
                                .iter()
                                .all(|c| c.status != pi_ai::NestedToolCallStatus::Unfinished);
                        result.nested_calls = Some(pi_ai::NestedToolCalls { calls, complete });
                    }
                    result.usage = combine_usage(result.usage.take(), usage);
                }
                let message = prior.start(message, signal.clone()).await?;
                messages::validate(
                    s.inner
                        .hooks
                        .transform_message(MessagePhase::Start, message, signal)
                        .await?,
                )
            })
        }));
        let previous_hooks = config.message_hooks.clone();
        let original_end = previous_hooks.end;
        let weak = Arc::downgrade(&session.inner);
        config.message_hooks.end = Some(Arc::new(move |message, signal| {
            let weak = weak.clone();
            let prior = original_end.clone();
            Box::pin(async move {
                let s = upgrade(&weak)?;
                let message = if let Some(p) = prior {
                    p(message, signal.clone()).await?
                } else {
                    message
                };
                messages::validate(
                    s.inner
                        .hooks
                        .transform_message(MessagePhase::End, message, signal)
                        .await?,
                )
            })
        }));
        config.tool_hooks = session.tool_hooks(None, config.tool_hooks.clone());
        let original_stream = config.stream.clone();
        let weak = Arc::downgrade(&session.inner);
        config.stream = Arc::new(move |request| {
            let original = original_stream.clone();
            let weak = weak.clone();
            Box::pin(async move {
                let mut request = request;
                if let Ok(s) = upgrade(&weak)
                    && let Some(key) = s
                        .inner
                        .models
                        .api_key(request.model.clone(), request.cancellation.clone())
                        .await?
                    && !key.expose_secret().is_empty()
                {
                    request.api_key = Some(key);
                }
                if let Ok(s) = upgrade(&weak) {
                    s.start_cache_warming(request.clone());
                }
                original(request).await
            })
        });
        let mut options = AgentOptions::new(config.model.clone(), config.stream.clone());
        options.loop_config = config;
        options.initial_state = AgentInitialState {
            messages: initial,
            tools: session.active_executable_tools(),
            thinking_level: Some(thinking),
            system_prompt: None,
        };
        if let Some(settings) = settings {
            options.steering_mode = settings.steering_mode;
            options.follow_up_mode = settings.follow_up_mode;
        }
        let agent = Agent::new(options);
        session
            .inner
            .agent
            .set(agent.clone())
            .map_err(|_| SessionError::Invalid("Agent initialized twice".into()))?;
        let weak = Arc::downgrade(&session.inner);
        agent.subscribe(Arc::new(move |event, signal| {
            let weak = weak.clone();
            Box::pin(async move {
                upgrade(&weak)?
                    .handle_agent_event(event, signal)
                    .await
                    .map_err(agent_error)
            })
        }));
        session
            .emit(SessionEvent::SessionStart {
                session_id: session.session_id(),
            })
            .await?;
        Ok(session)
    }
    pub fn agent(&self) -> &Agent {
        self.inner.agent.get().expect("Agent initialized")
    }
    pub fn snapshot(&self) -> SessionSnapshot {
        lock(&self.inner.manager).snapshot().clone()
    }
    pub fn projection(&self) -> SessionProjection {
        lock(&self.inner.manager).projection()
    }
    pub fn session_id(&self) -> String {
        lock(&self.inner.manager).header().id.clone()
    }
    pub fn session_name(&self) -> Option<String> {
        lock(&self.inner.manager)
            .entries()
            .iter()
            .rev()
            .find_map(|e| {
                if let EntryContent::SessionInfo { name } = &e.content {
                    Some(name.clone())
                } else {
                    None
                }
            })
    }
    pub fn messages(&self) -> Result<Vec<AgentMessage>, SessionError> {
        self.projection()
            .messages
            .into_iter()
            .map(messages::decode)
            .collect::<Result<Vec<_>, _>>()
            .map_err(Into::into)
    }
    pub fn model(&self) -> Model {
        self.agent().state().model
    }
    pub fn thinking_level(&self) -> ModelThinkingLevel {
        self.agent().state().thinking_level
    }
    pub fn routed_model(&self) -> Option<(Model, ModelThinkingLevel)> {
        lock(&self.inner.state).routed.clone()
    }
    pub fn is_streaming(&self) -> bool {
        lock(&self.inner.state).active
    }
    pub fn is_idle(&self) -> bool {
        let s = lock(&self.inner.state);
        !s.active && s.operation.is_none()
    }
    pub fn is_disposed(&self) -> bool {
        lock(&self.inner.state).disposed
    }
    pub fn subscribe(&self, sink: SessionEventSink) -> u64 {
        let id = self.inner.next_listener.fetch_add(1, Ordering::Relaxed);
        lock(&self.inner.listeners).push((id, sink));
        id
    }
    pub fn unsubscribe(&self, id: u64) {
        lock(&self.inner.listeners).retain(|(i, _)| *i != id);
    }
    pub(crate) async fn emit(&self, event: SessionEvent) -> Result<(), SessionError> {
        self.inner.hooks.event(event.clone()).await?;
        let listeners = lock(&self.inner.listeners).clone();
        for (_, listener) in listeners {
            listener(event.clone()).await?;
        }
        Ok(())
    }
    pub(crate) fn refresh(&self) -> Result<(), SessionError> {
        self.agent().replace_messages(self.messages()?);
        Ok(())
    }
    pub(crate) fn ensure_live(&self) -> Result<(), SessionError> {
        let state = lock(&self.inner.state);
        if state.disposed || state.disposing {
            Err(SessionError::Disposed)
        } else {
            Ok(())
        }
    }
    pub(crate) fn idle_guard(&self) -> Result<tokio::sync::OwnedMutexGuard<()>, SessionError> {
        self.ensure_live()?;
        let guard = self
            .inner
            .gate
            .clone()
            .try_lock_owned()
            .map_err(|_| SessionError::Agent(AgentError::Busy))?;
        self.ensure_live()?;
        Ok(guard)
    }
    pub fn active_tool_names(&self) -> Vec<String> {
        lock(&self.inner.state).active_tools.clone()
    }
    fn active_executable_tools(&self) -> Vec<AgentTool> {
        let s = lock(&self.inner.state);
        s.active_tools
            .iter()
            .filter_map(|n| {
                s.tools
                    .iter()
                    .find(|t| t.tool.declaration.name.as_str() == n)
                    .map(|t| {
                        let mut tool = t.tool.clone();
                        if let Some(description) = s.descriptions.get(n) {
                            tool.declaration.description = description.clone();
                        }
                        tool
                    })
            })
            .collect()
    }
    pub fn callable_tools(&self) -> Vec<AgentTool> {
        let s = lock(&self.inner.state);
        s.tools
            .iter()
            .filter(|t| {
                matches!(t.exposure, ToolExposure::CodeMode | ToolExposure::Deferred)
                    || (t.exposure == ToolExposure::Direct
                        && s.active_tools
                            .iter()
                            .any(|n| n == t.tool.declaration.name.as_str()))
            })
            .map(|t| t.tool.clone())
            .collect()
    }
    pub fn callable_tool_names(&self) -> Vec<String> {
        self.callable_tools()
            .iter()
            .map(|t| t.declaration.name.to_string())
            .collect()
    }
    pub fn all_tools(&self) -> Vec<ToolInfo> {
        lock(&self.inner.state)
            .tools
            .iter()
            .map(|t| ToolInfo {
                declaration: t.tool.declaration.clone(),
                exposure: t.exposure,
                source: t.source.clone(),
                namespace: t.namespace.clone(),
            })
            .collect()
    }
    pub fn set_active_tools(&self, names: Vec<String>) -> Result<(), SessionError> {
        self.ensure_live()?;
        {
            let mut s = lock(&self.inner.state);
            let mut seen = BTreeSet::new();
            let active = names
                .into_iter()
                .filter(|n| {
                    seen.insert(n.clone())
                        && s.tools.iter().any(|t| {
                            t.tool.declaration.name.as_str() == n
                                && t.exposure != ToolExposure::Hidden
                        })
                })
                .collect::<Vec<_>>();
            if s.active_tools.iter().any(|n| !active.contains(n)) {
                s.pending_tools.clear();
            }
            s.active_tools = active;
        }
        self.agent().set_tools(self.active_executable_tools());
        Ok(())
    }
    pub fn register_tool(&self, tool: RegisteredTool) -> Result<(), SessionError> {
        self.ensure_live()?;
        let name = tool.tool.declaration.name.to_string();
        let activate = tool.default_active
            && matches!(
                tool.exposure,
                ToolExposure::Direct | ToolExposure::ModelOnly
            );
        let names = {
            let mut s = lock(&self.inner.state);
            if let Some(t) = s
                .tools
                .iter_mut()
                .find(|t| t.tool.declaration.name.as_str() == name)
            {
                *t = tool;
            } else {
                s.tools.push(tool);
            }
            if (activate || s.pending_tools.contains(&name)) && !s.active_tools.contains(&name) {
                s.active_tools.push(name.clone());
            }
            s.pending_tools.retain(|n| n != &name);
            s.active_tools.clone()
        };
        self.set_active_tools(names)
    }
    pub fn unregister_tool(&self, name: &str) -> Result<(), SessionError> {
        self.ensure_live()?;
        {
            let mut s = lock(&self.inner.state);
            s.tools.retain(|t| t.tool.declaration.name.as_str() != name);
            s.active_tools.retain(|n| n != name);
        }
        self.agent().set_tools(self.active_executable_tools());
        Ok(())
    }
    pub fn system_prompt(&self) -> Result<String, SessionError> {
        let s = lock(&self.inner.state);
        if let Some(p) = &s.run_prompt {
            return Ok(p.clone());
        }
        let mut options = s.prompt_options.clone();
        options.selected_tools = Some(s.active_tools.clone());
        for t in &s.tools {
            let name = t.tool.declaration.name.to_string();
            if let Some(snippet) = &t.prompt_snippet {
                options.tool_snippets.insert(name.clone(), snippet.clone());
            }
            if !t.prompt_guidelines.is_empty() {
                options
                    .tool_guidelines
                    .insert(name, t.prompt_guidelines.clone());
            }
        }
        crate::build_system_prompt(&options).map_err(|e| SessionError::Invalid(e.to_string()))
    }
    pub(crate) fn prompt_patch(&self) -> Result<Option<AgentMessage>, SessionError> {
        let options = {
            let s = lock(&self.inner.state);
            let mut options = s.prompt_options.clone();
            options.force_system_prompt = None;
            options.selected_tools = Some(s.active_tools.clone());
            options.hidden_tools = s.hidden_declarations.iter().cloned().collect();
            for tool in &s.tools {
                let name = tool.tool.declaration.name.to_string();
                if let Some(snippet) = &tool.prompt_snippet {
                    options.tool_snippets.insert(name.clone(), snippet.clone());
                }
                if !tool.prompt_guidelines.is_empty() {
                    options
                        .tool_guidelines
                        .insert(name, tool.prompt_guidelines.clone());
                }
            }
            options
        };
        let messages = self
            .messages()?
            .into_iter()
            .filter_map(|m| m.as_builtin().cloned())
            .collect::<Vec<_>>();
        let old = pi_ai::get_current_system_message(&messages);
        let old_sections = old
            .and_then(|m| m.sections)
            .unwrap_or_else(|| pi_ai::SystemSections::new(Vec::new()));
        let sections = crate::build_system_prompt_sections(&options)
            .map_err(|e| SessionError::Invalid(e.to_string()))?;
        Ok(
            crate::diff_system_prompt_sections(&old_sections, &sections).map(|sections| {
                AgentMessage::BuiltIn(Message::System(pi_ai::SystemMessage {
                    role: Default::default(),
                    content: pi_ai::SystemContent::Text(String::new()),
                    sections: Some(sections),
                    tools_added: None,
                    tools_removed: None,
                    timestamp: SafeUnsigned::new(messages::now()).expect("clock"),
                }))
            }),
        )
    }
    pub(crate) async fn refresh_loadout(
        &self,
        signal: CancellationToken,
    ) -> Result<(), SessionError> {
        let loadout = ToolLoadout {
            declared: self.active_executable_tools(),
            callable: self.callable_tools(),
            registered: lock(&self.inner.state).tools.clone(),
        };
        let changes = self.inner.hooks.prepare_loadout(loadout, signal).await?;
        {
            let mut state = lock(&self.inner.state);
            state.descriptions = changes.descriptions;
            state.hidden_declarations = changes.hidden_declarations.into_iter().collect();
        }
        self.agent().set_tools(self.active_executable_tools());
        Ok(())
    }
    fn project_request_messages(
        &self,
        messages: Vec<AgentMessage>,
    ) -> Result<Vec<AgentMessage>, AgentError> {
        let (hidden, forced) = {
            let state = lock(&self.inner.state);
            (
                state.hidden_declarations.clone(),
                state
                    .run_prompt
                    .clone()
                    .or_else(|| state.prompt_options.force_system_prompt.clone()),
            )
        };
        let messages = messages
            .into_iter()
            .map(|mut message| {
                if let AgentMessage::BuiltIn(Message::System(system)) = &mut message {
                    if let Some(tools) = &mut system.tools_added {
                        tools.retain(|t| !hidden.contains(t.name.as_str()));
                        if tools.is_empty() {
                            system.tools_added = None;
                        }
                    }
                    if let Some(tools) = &mut system.tools_removed {
                        tools.retain(|t| !hidden.contains(t.name.as_str()));
                        if tools.is_empty() {
                            system.tools_removed = None;
                        }
                    }
                }
                message
            })
            .collect::<Vec<_>>();
        if let Some(forced) = forced {
            let typed = messages
                .iter()
                .filter_map(|m| m.as_builtin().cloned())
                .collect::<Vec<_>>();
            let current = pi_ai::get_current_system_message(&typed);
            let mut head = pi_ai::create_initial_system_message(
                Some(&forced),
                Some(&pi_ai::get_current_tools(&typed)),
            )
            .unwrap_or(pi_ai::SystemMessage {
                role: Default::default(),
                content: pi_ai::SystemContent::Text(forced),
                sections: None,
                tools_added: None,
                tools_removed: None,
                timestamp: SafeUnsigned::new(0).expect("zero"),
            });
            if let Some(current) = current {
                head.timestamp = current.timestamp;
            }
            Ok(
                std::iter::once(AgentMessage::BuiltIn(Message::System(head)))
                    .chain(messages.into_iter().filter(|m| m.role() != "system"))
                    .collect(),
            )
        } else {
            Ok(messages)
        }
    }
    pub(crate) fn tool_hooks(&self, parent: Option<String>, prior: ToolCallHooks) -> ToolCallHooks {
        let weak = Arc::downgrade(&self.inner);
        let before_parent = parent.clone();
        let before = prior.before;
        let weak_after = weak.clone();
        let after = prior.after;
        ToolCallHooks {
            before: Some(Arc::new(move |context, signal| {
                let weak = weak.clone();
                let parent = before_parent.clone();
                let prior = before.clone();
                Box::pin(async move {
                    let s = upgrade(&weak)?;
                    let own = s
                        .inner
                        .hooks
                        .before_tool(context.clone(), parent, signal.clone())
                        .await?;
                    if own.is_some() {
                        Ok(own)
                    } else if let Some(p) = prior {
                        p(context, signal).await
                    } else {
                        Ok(None)
                    }
                })
            })),
            after: Some(Arc::new(move |context, signal| {
                let weak = weak_after.clone();
                let parent = parent.clone();
                let prior = after.clone();
                Box::pin(async move {
                    let s = upgrade(&weak)?;
                    let own = s
                        .inner
                        .hooks
                        .after_tool(context.clone(), parent, signal.clone())
                        .await?;
                    let patch = if own.is_some() {
                        own
                    } else if let Some(p) = prior {
                        p(context.clone(), signal).await?
                    } else {
                        None
                    };
                    s.normalize_tool_result(&context, patch).await
                })
            })),
        }
    }
    async fn normalize_image_content(
        &self,
        content: Vec<pi_ai::InputContent>,
    ) -> Result<(Vec<pi_ai::InputContent>, Vec<String>), AgentError> {
        use base64::Engine;
        let model = self
            .routed_model()
            .map(|(model, _)| model)
            .unwrap_or_else(|| self.model());
        let auto_resize = lock(&self.inner.state).image_auto_resize;
        let options = crate::ReadToolOptions {
            auto_resize,
            resize: model
                .input_limits
                .as_ref()
                .and_then(|limits| limits.images.as_ref())
                .and_then(|images| images.resize.clone()),
            supports_images: None,
        };
        let environment = lock(&self.inner.execution_env).clone();
        let mut normalized = Vec::new();
        let mut hints = Vec::new();
        for block in content {
            match block {
                pi_ai::InputContent::Image(image) => {
                    let decoded = base64::engine::general_purpose::STANDARD.decode(&image.data);
                    match decoded {
                        Ok(bytes) => match environment
                            .process_image(&bytes, &image.mime_type, &options)
                            .await
                        {
                            Ok(Some(processed)) => {
                                normalized.push(pi_ai::InputContent::Image(pi_ai::ImageContent {
                                    content_type: Default::default(),
                                    data: processed.data,
                                    mime_type: processed.mime_type,
                                }));
                                hints.extend(processed.hints);
                            }
                            Ok(None) => hints
                                .push("Image omitted: the image could not be processed.".into()),
                            Err(error) => hints.push(format!("Image omitted: {error}")),
                        },
                        Err(_) => hints.push("Image omitted: invalid base64 image data.".into()),
                    }
                }
                other => normalized.push(other),
            }
        }
        Ok((normalized, hints))
    }
    pub(crate) async fn normalize_prompt_message(
        &self,
        message: AgentMessage,
    ) -> Result<AgentMessage, SessionError> {
        if let AgentMessage::BuiltIn(Message::User(mut user)) = message {
            if let pi_ai::UserContent::Blocks(blocks) = user.content {
                let (mut content, hints) = self.normalize_image_content(blocks).await?;
                if !hints.is_empty() {
                    if let Some(pi_ai::InputContent::Text(text)) = content.first_mut() {
                        text.text.push_str(&format!("\n\n{}", hints.join("\n")));
                    } else {
                        content.insert(
                            0,
                            pi_ai::InputContent::Text(pi_ai::TextContent {
                                content_type: Default::default(),
                                text: hints.join("\n"),
                                text_signature: None,
                            }),
                        );
                    }
                }
                user.content = pi_ai::UserContent::Blocks(content);
            }
            Ok(AgentMessage::BuiltIn(Message::User(user)))
        } else {
            Ok(message)
        }
    }
    async fn normalize_tool_result(
        &self,
        context: &AfterToolCallContext,
        patch: Option<AfterToolCallResult>,
    ) -> Result<Option<AfterToolCallResult>, AgentError> {
        let original = patch
            .as_ref()
            .and_then(|p| p.content.clone())
            .unwrap_or_else(|| context.result.content.clone());
        if !original
            .iter()
            .any(|block| matches!(block, pi_ai::InputContent::Image(_)))
        {
            return Ok(patch);
        }
        let (mut content, hints) = self.normalize_image_content(original.clone()).await?;
        content.extend(hints.into_iter().map(|text| {
            pi_ai::InputContent::Text(pi_ai::TextContent {
                content_type: Default::default(),
                text,
                text_signature: None,
            })
        }));
        if content == original {
            return Ok(patch);
        }
        let from_hook = patch.is_some();
        let mut patch = patch.unwrap_or_default();
        patch.content = Some(content);
        if !from_hook {
            patch.structured_content = context.result.structured_content.clone();
        }
        Ok(Some(patch))
    }
    pub(crate) fn rebind_cwd(&self) -> Result<(), SessionError> {
        let cwd = lock(&self.inner.manager).header().cwd.clone();
        let (local, defaults) = {
            let mut state = lock(&self.inner.state);
            state.prompt_options.cwd = cwd.clone();
            (state.local_environment, state.uses_default_tools)
        };
        if local {
            *lock(&self.inner.execution_env) = Arc::new(crate::LocalExecutionEnv::new(cwd));
        }
        if defaults {
            let context = crate::ExecutionToolContext {
                env: lock(&self.inner.execution_env).clone(),
            };
            let mut state = lock(&self.inner.state);
            state
                .tools
                .retain(|t| t.source.as_deref() != Some("builtin"));
            state
                .tools
                .extend(crate::create_tools(context).into_iter().map(|tool| {
                    let mut registered = RegisteredTool::direct(tool);
                    registered.source = Some("builtin".into());
                    registered
                }));
        }
        self.agent().set_tools(self.active_executable_tools());
        Ok(())
    }
    async fn handle_agent_event(
        &self,
        event: AgentEvent,
        _signal: CancellationToken,
    ) -> Result<(), SessionError> {
        if matches!(event, AgentEvent::AgentStart) {
            lock(&self.inner.state).turn_index = 0;
        }
        if let AgentEvent::MessageStart { message } = &event
            && message.role() == "user"
        {
            let value = serde_json::to_value(message)?;
            let text = messages::content_text(&value["content"], "");
            let changed = {
                let mut s = lock(&self.inner.state);
                s.overflow_attempted = false;
                if let Some(i) = s.steering.iter().position(|m| m == &text) {
                    s.steering.remove(i);
                    true
                } else if let Some(i) = s.follow_up.iter().position(|m| m == &text) {
                    s.follow_up.remove(i);
                    true
                } else {
                    false
                }
            };
            if changed {
                self.emit_queue().await?;
            }
        }
        if let AgentEvent::AgentEnd { messages } = &event {
            let (policy, attempt, aborted) = {
                let state = lock(&self.inner.state);
                (
                    state.retry.clone(),
                    state.retry_attempt,
                    state
                        .operation
                        .as_ref()
                        .is_some_and(CancellationToken::is_cancelled),
                )
            };
            let will_retry = !aborted
                && policy.enabled
                && attempt < policy.max_retries.get()
                && messages
                    .iter()
                    .rev()
                    .find_map(|message| {
                        if let AgentMessage::BuiltIn(Message::Assistant(message)) = message {
                            Some(
                                pi_ai::is_retryable_assistant_error(message)
                                    && !pi_ai::is_context_overflow(
                                        message,
                                        Some(self.model().context_window),
                                    ),
                            )
                        } else {
                            None
                        }
                    })
                    .unwrap_or(false);
            self.emit(SessionEvent::AgentEnd {
                messages: messages.clone(),
                will_retry,
            })
            .await?;
        } else {
            self.emit(SessionEvent::Agent(event.clone())).await?;
        }
        if let AgentEvent::MessageEnd { message } = &event {
            self.persist_message(message)?;
            if let AgentMessage::BuiltIn(Message::Assistant(m)) = message {
                let retry = {
                    let mut s = lock(&self.inner.state);
                    s.outcome = match m.stop_reason {
                        StopReason::Error => ActivityOutcome::Error,
                        StopReason::Aborted => ActivityOutcome::Aborted,
                        _ => ActivityOutcome::Completed,
                    };
                    if m.stop_reason != StopReason::Error && s.retry_attempt > 0 {
                        let n = s.retry_attempt;
                        s.retry_attempt = 0;
                        Some(n)
                    } else {
                        None
                    }
                };
                if let Some(attempt) = retry {
                    self.emit(SessionEvent::AutoRetryEnd {
                        success: true,
                        attempt,
                        final_error: None,
                    })
                    .await?;
                }
            }
        }
        if matches!(event, AgentEvent::TurnEnd { .. }) {
            lock(&self.inner.state).turn_index += 1;
            self.flush_custom().await?;
        }
        if matches!(event, AgentEvent::AgentEnd { .. }) {
            self.flush_bash().await?;
            lock(&self.inner.state).nested.clear();
        }
        Ok(())
    }
    pub(crate) fn persist_message(&self, message: &AgentMessage) -> Result<String, SessionError> {
        let value = serde_json::to_value(message)?;
        messages::decode(value.clone())?;
        let mut manager = lock(&self.inner.manager);
        if message.role() == "custom" {
            Ok(manager.append(EntryContent::CustomMessage {
                custom_type: value["customType"]
                    .as_str()
                    .ok_or_else(|| SessionError::Invalid("custom message type missing".into()))?
                    .into(),
                content: value["content"].clone(),
                display: value["display"].as_bool().unwrap_or(false),
                details: value.get("details").cloned(),
            })?)
        } else if matches!(
            message.role(),
            "system" | "user" | "assistant" | "toolResult" | "bashExecution"
        ) {
            Ok(manager.append_message(value)?)
        } else {
            Err(SessionError::Invalid(
                "unsupported message admission".into(),
            ))
        }
    }
    pub(crate) async fn prepare_request(
        &self,
        request: PrepareRequestContext,
        signal: CancellationToken,
        previous: Option<PrepareRequest>,
    ) -> Result<Option<AgentRequestUpdate>, AgentError> {
        let (projection, branch) = {
            let m = lock(&self.inner.manager);
            (m.projection(), m.get_branch())
        };
        let canonical = AgentContext {
            messages: projection
                .messages
                .clone()
                .into_iter()
                .map(messages::decode)
                .collect::<Result<Vec<_>, _>>()?,
            tools: self.active_executable_tools(),
        };
        let prior = if let Some(p) = previous {
            p(
                PrepareRequestContext {
                    context: canonical.clone(),
                    model: self.model(),
                    thinking_level: self.thinking_level(),
                },
                signal.clone(),
            )
            .await?
        } else {
            None
        };
        let context = prior
            .as_ref()
            .and_then(|p| p.context.clone())
            .unwrap_or(canonical);
        let selected = prior
            .as_ref()
            .and_then(|p| p.model.clone())
            .unwrap_or_else(|| self.model());
        let thinking = prior
            .as_ref()
            .and_then(|p| p.thinking_level)
            .unwrap_or_else(|| self.thinking_level());
        let virtual_model = self.inner.models.is_virtual(&selected);
        let failed = lock(&self.inner.state).failed.take();
        let value_messages = context
            .messages
            .iter()
            .map(serde_json::to_value)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| AgentError::InvalidContext(e.to_string()))?;
        let last = value_messages
            .iter()
            .rposition(|m| m["role"] == "assistant")
            .map(|i| i + 1)
            .unwrap_or(0);
        let reason = if failed.is_some() {
            crate::ModelRouteReason::Retry
        } else if value_messages[last..].iter().any(|m| m["role"] == "user") {
            crate::ModelRouteReason::User
        } else {
            crate::ModelRouteReason::Continuation
        };
        let route_state = crate::branch_virtual_model_state(
            &branch,
            selected.provider.as_str(),
            selected.id.as_str(),
        );
        let failed = failed.and_then(|message| {
            self.inner
                .models
                .get_model(message.provider.as_str(), message.model.as_str())
                .filter(|model| !self.inner.models.is_virtual(model))
                .map(|model| crate::RoutedFailure {
                    model,
                    thinking_level: message.thinking_level,
                    message,
                })
        });
        let route = if virtual_model {
            self.inner
                .models
                .resolve(crate::ModelRouteRequest {
                    selected_model: selected.clone(),
                    messages: crate::convert_to_llm(context.messages.clone())?,
                    thinking_level: thinking,
                    reason,
                    failed,
                    previous: self
                        .route_previous(&crate::convert_to_llm(context.messages.clone())?),
                    state: route_state.clone(),
                    cancellation: signal.clone(),
                })
                .await?
        } else {
            crate::ModelRoute {
                model: selected.clone(),
                thinking_level: thinking,
                state: None,
            }
        };
        if self.inner.models.is_virtual(&route.model) {
            return Err(AgentError::InvalidContext(format!(
                "Virtual model {}/{} must route to a physical model",
                selected.provider, selected.id
            )));
        }
        if virtual_model
            && route.state != route_state
            && let Some(state) = route.state.clone()
        {
            self.append_custom_entries(vec![(
                crate::VIRTUAL_MODEL_STATE_ENTRY.into(),
                Some(json!({"provider":selected.provider,"modelId":selected.id,"state":state})),
            )])
            .await
            .map_err(agent_error)?;
        }
        lock(&self.inner.state).routed =
            virtual_model.then(|| (route.model.clone(), route.thinking_level));
        let settings = lock(&self.inner.state).compaction.clone();
        let mut context = context;
        if route.model.context_window.get() > 0
            && crate::should_compact(
                crate::estimate_projected_context_tokens(&projection, &branch).tokens,
                route.model.context_window.get(),
                &settings,
            )
        {
            self.auto_compact(CompactionReason::Threshold, false, signal.clone())
                .await
                .map_err(agent_error)?;
            context.messages = self.messages().map_err(agent_error)?;
        }
        let _ = request;
        Ok(Some(AgentRequestUpdate {
            context: Some(context),
            model: Some(route.model),
            thinking_level: Some(route.thinking_level),
        }))
    }
    pub(crate) fn route_previous(&self, messages: &[Message]) -> Option<crate::RoutedResponse> {
        messages.iter().rev().find_map(|message| {
            if let Message::Assistant(message) = message {
                if matches!(message.stop_reason, StopReason::Error | StopReason::Aborted) {
                    None
                } else {
                    self.inner
                        .models
                        .get_model(message.provider.as_str(), message.model.as_str())
                        .filter(|model| !self.inner.models.is_virtual(model))
                        .map(|model| crate::RoutedResponse {
                            model,
                            thinking_level: message.thinking_level,
                        })
                }
            } else {
                None
            }
        })
    }
    pub(crate) async fn emit_queue(&self) -> Result<(), SessionError> {
        let (steering, follow_up) = {
            let s = lock(&self.inner.state);
            (s.steering.clone(), s.follow_up.clone())
        };
        self.emit(SessionEvent::QueueUpdate {
            steering,
            follow_up,
        })
        .await
    }
    pub(crate) async fn flush_custom(&self) -> Result<(), SessionError> {
        loop {
            let message = lock(&self.inner.state).pending_custom.first().cloned();
            let Some(message) = message else {
                return Ok(());
            };
            self.persist_message(&message)?;
            lock(&self.inner.state).pending_custom.remove(0);
            self.refresh()?;
        }
    }
    pub async fn append_custom_entry(
        &self,
        custom_type: impl Into<String>,
        data: Option<Value>,
    ) -> Result<SessionEntry, SessionError> {
        Ok(self
            .append_custom_entries(vec![(custom_type.into(), data)])
            .await?
            .remove(0))
    }
    /// Append adapter state in a single canonical store transaction. Empty batches are no-ops.
    pub async fn append_custom_entries(
        &self,
        entries: Vec<(String, Option<Value>)>,
    ) -> Result<Vec<SessionEntry>, SessionError> {
        self.ensure_live()?;
        if entries.is_empty() {
            return Ok(Vec::new());
        }
        let entries = self.commit_contents(
            entries
                .into_iter()
                .map(|(custom_type, data)| EntryContent::Custom { custom_type, data })
                .collect(),
        )?;
        for entry in &entries {
            self.emit(SessionEvent::EntryAppended {
                entry: entry.clone(),
            })
            .await?;
        }
        Ok(entries)
    }
    pub(crate) fn commit_contents(
        &self,
        contents: Vec<EntryContent>,
    ) -> Result<Vec<SessionEntry>, SessionError> {
        commit_manager_contents(&mut lock(&self.inner.manager), contents)
    }
    pub fn branch_cursor(&self) -> SessionBranchCursor {
        let manager = lock(&self.inner.manager);
        SessionBranchCursor {
            session_id: manager.header().id.clone(),
            navigation_generation: self.inner.navigation_generation.load(Ordering::Acquire),
            leaf_id: manager.leaf_id().map(str::to_owned),
        }
    }
    /// Commit script deltas after same-branch appends, rejecting any navigation away and back.
    pub async fn append_custom_entries_if_cursor(
        &self,
        cursor: &SessionBranchCursor,
        entries: Vec<(String, Option<Value>)>,
    ) -> Result<Vec<SessionEntry>, SessionError> {
        self.ensure_live()?;
        let appended = {
            let mut manager = lock(&self.inner.manager);
            if manager.header().id != cursor.session_id
                || self.inner.navigation_generation.load(Ordering::Acquire)
                    != cursor.navigation_generation
            {
                return Err(SessionError::Invalid(
                    "Session navigation changed while the tool was executing".into(),
                ));
            }
            commit_manager_contents(
                &mut manager,
                entries
                    .into_iter()
                    .map(|(custom_type, data)| EntryContent::Custom { custom_type, data })
                    .collect(),
            )?
        };
        for entry in &appended {
            self.emit(SessionEvent::EntryAppended {
                entry: entry.clone(),
            })
            .await?;
        }
        Ok(appended)
    }
    /// Commit adapter state only while its captured canonical session and leaf still match.
    pub async fn append_custom_entries_if_session_branch(
        &self,
        session_id: &str,
        leaf_id: Option<&str>,
        entries: Vec<(String, Option<Value>)>,
    ) -> Result<Vec<SessionEntry>, SessionError> {
        self.ensure_live()?;
        let appended = {
            let mut manager = lock(&self.inner.manager);
            if manager.header().id != session_id || manager.leaf_id() != leaf_id {
                return Err(SessionError::Invalid(
                    "Session branch changed while the tool was executing".into(),
                ));
            }
            commit_manager_contents(
                &mut manager,
                entries
                    .into_iter()
                    .map(|(custom_type, data)| EntryContent::Custom { custom_type, data })
                    .collect(),
            )?
        };
        for entry in &appended {
            self.emit(SessionEvent::EntryAppended {
                entry: entry.clone(),
            })
            .await?;
        }
        Ok(appended)
    }
    /// Commit successful script writes after legitimate appends on the same branch.
    pub async fn append_custom_entries_if_session_ancestor(
        &self,
        session_id: &str,
        ancestor_leaf_id: Option<&str>,
        entries: Vec<(String, Option<Value>)>,
    ) -> Result<Vec<SessionEntry>, SessionError> {
        self.ensure_live()?;
        let appended = {
            let mut manager = lock(&self.inner.manager);
            let same_branch = ancestor_leaf_id
                .is_none_or(|id| manager.get_branch().iter().any(|entry| entry.id == id));
            if manager.header().id != session_id || !same_branch {
                return Err(SessionError::Invalid(
                    "Session branch changed while the tool was executing".into(),
                ));
            }
            commit_manager_contents(
                &mut manager,
                entries
                    .into_iter()
                    .map(|(custom_type, data)| EntryContent::Custom { custom_type, data })
                    .collect(),
            )?
        };
        for entry in &appended {
            self.emit(SessionEvent::EntryAppended {
                entry: entry.clone(),
            })
            .await?;
        }
        Ok(appended)
    }
    /// Atomically replace one authenticated adapter's tools without overwriting another owner.
    pub fn replace_tools_for_source(
        &self,
        source: &str,
        replacements: Vec<RegisteredTool>,
    ) -> Result<(), SessionError> {
        self.ensure_live()?;
        {
            let mut state = lock(&self.inner.state);
            let mut names = BTreeSet::new();
            for tool in &replacements {
                let name = tool.tool.declaration.name.to_string();
                if tool.source.as_deref() != Some(source) || !names.insert(name.clone()) {
                    return Err(SessionError::Invalid(
                        "Invalid adapter tool source or duplicate name".into(),
                    ));
                }
                if state.tools.iter().any(|existing| {
                    existing.tool.declaration.name.as_str() == name
                        && existing.source.as_deref() != Some(source)
                }) {
                    return Err(SessionError::Invalid(format!(
                        "Tool {name} belongs to another source"
                    )));
                }
            }
            let prior = state
                .tools
                .iter()
                .filter(|t| t.source.as_deref() == Some(source))
                .map(|t| t.tool.declaration.name.to_string())
                .collect::<BTreeSet<_>>();
            state.tools.retain(|t| t.source.as_deref() != Some(source));
            state
                .active_tools
                .retain(|name| !prior.contains(name) || names.contains(name));
            for tool in replacements {
                let name = tool.tool.declaration.name.to_string();
                if (state.pending_tools.contains(&name)
                    || (!prior.contains(&name)
                        && tool.default_active
                        && matches!(
                            tool.exposure,
                            ToolExposure::Direct | ToolExposure::ModelOnly
                        )))
                    && !state.active_tools.contains(&name)
                {
                    state.active_tools.push(name.clone());
                }
                state.pending_tools.retain(|pending| pending != &name);
                state.tools.push(tool);
            }
            let active_allowed = state
                .tools
                .iter()
                .filter(|tool| tool.exposure != ToolExposure::Hidden)
                .map(|tool| tool.tool.declaration.name.to_string())
                .collect::<BTreeSet<_>>();
            state
                .active_tools
                .retain(|name| active_allowed.contains(name));
        }
        self.agent().set_tools(self.active_executable_tools());
        Ok(())
    }
    pub async fn set_name(&self, name: &str) -> Result<(), SessionError> {
        self.ensure_live()?;
        lock(&self.inner.manager).set_name(name)?;
        self.emit(SessionEvent::SessionInfoChanged {
            name: self.session_name(),
        })
        .await
    }
    pub async fn set_label(&self, id: &str, label: Option<String>) -> Result<(), SessionError> {
        self.ensure_live()?;
        let entry = {
            let mut m = lock(&self.inner.manager);
            let id = m.set_label(id, label)?;
            m.get_entry(&id).expect("committed label").clone()
        };
        self.emit(SessionEvent::EntryAppended { entry }).await
    }
    pub fn label(&self, id: &str) -> Option<String> {
        lock(&self.inner.manager).label(id).map(str::to_owned)
    }
    pub async fn edit_context(
        &self,
        id: &str,
        replacement: Option<ContentReplacement>,
    ) -> Result<(), SessionError> {
        let _guard = self.idle_guard()?;
        self.commit_boundary_drafts(vec![SessionBoundaryDraft::ContextEdit {
            target_id: id.to_owned(),
            replacement,
        }])
        .await?;
        Ok(())
    }
    pub fn context_usage(&self) -> Option<crate::ContextUsage> {
        let window = self
            .routed_model()
            .map(|(m, _)| m.context_window.get())
            .unwrap_or_else(|| self.model().context_window.get());
        let m = lock(&self.inner.manager);
        crate::context_usage::context_usage(&m.projection(), &m.get_branch(), window)
    }
    pub fn stats(&self) -> SessionStats {
        let snapshot = self.snapshot();
        let mut stats = SessionStats::default();
        for e in snapshot.entries {
            let usage = match e.content {
                EntryContent::Usage { usage, .. } => serde_json::from_value(usage).ok(),
                EntryContent::Compaction { usage, .. }
                | EntryContent::BranchSummary { usage, .. } => {
                    usage.and_then(|u| serde_json::from_value(u).ok())
                }
                EntryContent::Message { message } => {
                    stats.total_messages += 1;
                    match message["role"].as_str() {
                        Some("user") => stats.user_messages += 1,
                        Some("assistant") => {
                            stats.assistant_messages += 1;
                            stats.tool_calls += message["content"]
                                .as_array()
                                .map(|b| b.iter().filter(|b| b["type"] == "toolCall").count())
                                .unwrap_or(0);
                        }
                        Some("toolResult") => stats.tool_results += 1,
                        _ => {}
                    }
                    message
                        .get("usage")
                        .cloned()
                        .and_then(|u| serde_json::from_value(u).ok())
                }
                _ => None,
            };
            stats.usage = combine_usage(stats.usage, usage);
        }
        stats.context_usage = self.context_usage();
        stats
    }
}
pub(crate) fn combine_usage(a: Option<Usage>, b: Option<Usage>) -> Option<Usage> {
    match (a, b) {
        (None, b) => b,
        (a, None) => a,
        (Some(a), Some(b)) => {
            let mut a = serde_json::to_value(a).expect("usage");
            let b = serde_json::to_value(b).expect("usage");
            for key in [
                "input",
                "output",
                "cacheRead",
                "cacheWrite",
                "totalTokens",
                "cacheWrite1h",
                "reasoning",
            ] {
                if a.get(key).is_some() || b.get(key).is_some() {
                    a[key] = json!(
                        (a[key].as_u64().unwrap_or(0) + b[key].as_u64().unwrap_or(0))
                            .min(pi_ai::JS_SAFE_INTEGER_MAX)
                    );
                }
            }
            for key in ["input", "output", "cacheRead", "cacheWrite", "total"] {
                a["cost"][key] = json!(
                    a["cost"][key].as_f64().unwrap_or(0.0) + b["cost"][key].as_f64().unwrap_or(0.0)
                );
            }
            serde_json::from_value(a).ok()
        }
    }
}

impl CodingSession {
    /// Invoke a callable tool through the same validation, hooks, cancellation and progress pipeline.
    /// Nested calls share their top-level result's bounded record and usage total.
    pub async fn execute_tool(
        &self,
        parent_tool_call_id: impl Into<String>,
        name: impl Into<String>,
        arguments: pi_ai::JsonObject,
        cancellation: CancellationToken,
        on_update: Option<ToolUpdateSink>,
    ) -> Result<AgentToolCallOutcome, SessionError> {
        self.ensure_live()?;
        let parent = parent_tool_call_id.into();
        let name = name.into();
        let tools = self.callable_tools();
        let (id, root, holds) = {
            let mut s = lock(&self.inner.state);
            if s.disposed || s.disposing {
                return Err(SessionError::Disposed);
            }

            let next = s.nested_next.entry(parent.clone()).or_insert(1);
            let id = format!("{parent}/{next}");
            *next += 1;
            let root = s
                .nested_roots
                .get(&parent)
                .cloned()
                .unwrap_or_else(|| parent.clone());
            s.nested_roots.insert(id.clone(), root.clone());
            let holds = s.nested_holds.contains(&parent);
            (id, root, holds)
        };
        let _scope = NestedScopeGuard {
            inner: Arc::downgrade(&self.inner),
            id: id.clone(),
        };
        let call = pi_ai::ToolCall {
            content_type: Default::default(),
            id: id
                .clone()
                .try_into()
                .map_err(|_| SessionError::Invalid("invalid nested tool call id".into()))?,
            name: name
                .clone()
                .try_into()
                .map_err(|_| SessionError::Invalid("invalid tool name".into()))?,
            arguments: arguments.clone(),
            arguments_error: None,
            namespace: None,
            thought_signature: None,
        };
        let bytes = serde_json::to_vec(&arguments)?.len();
        let recorded = {
            let mut s = lock(&self.inner.state);
            let record = s
                .nested
                .entry(root.clone())
                .or_insert((Vec::new(), None, true));
            if record.0.len() >= 256 {
                record.2 = false;
                false
            } else {
                let used = record
                    .0
                    .iter()
                    .filter_map(|r| r.arguments.as_ref())
                    .map(|a| serde_json::to_vec(a).map(|v| v.len()).unwrap_or(0))
                    .sum::<usize>();
                let omitted = bytes > 8192 || used + bytes > 32768;
                if omitted {
                    record.2 = false;
                }
                record.0.push(pi_ai::NestedToolCallRecord {
                    id: id.clone(),
                    name: name.clone(),
                    arguments: (!omitted).then_some(arguments.clone()),
                    arguments_bytes: omitted
                        .then(|| SafeUnsigned::new(bytes as u64).expect("bounded arguments")),
                    status: pi_ai::NestedToolCallStatus::Unfinished,
                    duration_ms: None,
                    error: None,
                });
                true
            }
        };
        self.emit(SessionEvent::NestedTool {
            parent_tool_call_id: parent.clone(),
            event: AgentEvent::ToolExecutionStart {
                tool_call_id: id.clone(),
                tool_name: name.clone(),
                args: arguments.clone(),
            },
        })
        .await?;
        let exclusive = !holds
            && (self.agent().configuration().tool_execution == ToolExecutionMode::Sequential
                || tools.iter().any(|t| {
                    t.declaration.name.as_str() == name
                        && t.execution_mode == Some(ToolExecutionMode::Sequential)
                }));
        let _guard = if exclusive {
            Some(self.inner.nested_gate.clone().lock_owned().await)
        } else {
            None
        };
        if holds || exclusive {
            lock(&self.inner.state).nested_holds.insert(id.clone());
        }
        let outcome = if let Some(assistant) = self.last_assistant() {
            let weak = Arc::downgrade(&self.inner);
            let update_parent = parent.clone();
            let update_id = id.clone();
            let update_name = name.clone();
            let update_args = arguments.clone();
            run_tool_call(
                call.clone(),
                RunToolCallOptions {
                    tools,
                    assistant_message: assistant,
                    context: AgentContext {
                        messages: self.agent().state().messages,
                        tools: self.active_executable_tools(),
                    },
                    cancellation,
                    hooks: self.tool_hooks(Some(parent.clone()), ToolCallHooks::default()),
                    on_update: Some(Arc::new(move |partial| {
                        let weak = weak.clone();
                        let listener = on_update.clone();
                        let parent = update_parent.clone();
                        let id = update_id.clone();
                        let name = update_name.clone();
                        let args = update_args.clone();
                        Box::pin(async move {
                            if let Some(listener) = listener {
                                listener(partial.clone()).await?;
                            }
                            upgrade(&weak)?
                                .emit(SessionEvent::NestedTool {
                                    parent_tool_call_id: parent,
                                    event: AgentEvent::ToolExecutionUpdate {
                                        tool_call_id: id,
                                        tool_name: name,
                                        args,
                                        partial_result: partial,
                                    },
                                })
                                .await
                                .map_err(agent_error)
                        })
                    })),
                    monotonic_clock: self.agent().configuration().monotonic_clock,
                },
            )
            .await?
        } else {
            AgentToolCallOutcome {
                tool_call: call,
                result: AgentToolResult {
                    content: vec![pi_ai::InputContent::Text(pi_ai::TextContent {
                        content_type: Default::default(),
                        text: "No assistant message issued this call".into(),
                        text_signature: None,
                    })],
                    ..Default::default()
                },
                is_error: true,
                duration_ms: None,
            }
        };
        {
            let mut s = lock(&self.inner.state);
            if let Some(record) = s.nested.get_mut(&root) {
                if recorded && let Some(call) = record.0.iter_mut().find(|c| c.id == id) {
                    call.status = if outcome.is_error {
                        pi_ai::NestedToolCallStatus::Error
                    } else {
                        pi_ai::NestedToolCallStatus::Ok
                    };
                    call.duration_ms = outcome.duration_ms.and_then(|v| SafeUnsigned::new(v).ok());
                    if outcome.is_error {
                        let content = serde_json::to_value(&outcome.result.content)?;
                        let text = messages::content_text(&content, "\n");
                        if !text.is_empty() {
                            call.error = Some(text.chars().take(500).collect());
                        }
                    }
                }
                record.1 = combine_usage(record.1.take(), outcome.result.usage.clone());
            }
        }
        self.emit(SessionEvent::NestedTool {
            parent_tool_call_id: parent,
            event: AgentEvent::ToolExecutionEnd {
                tool_call_id: id,
                tool_name: name,
                result: outcome.result.clone(),
                is_error: outcome.is_error,
                duration_ms: outcome.duration_ms,
            },
        })
        .await?;
        Ok(outcome)
    }
}

struct NestedScopeGuard {
    inner: Weak<Inner>,
    id: String,
}
impl Drop for NestedScopeGuard {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.upgrade() {
            let mut state = lock(&inner.state);
            state.nested_holds.remove(&self.id);
            state.nested_roots.remove(&self.id);
            state.nested_next.remove(&self.id);
        }
    }
}

/// Weak session capability for tools/adapters that are themselves registered on a session.
#[derive(Clone)]
pub struct WeakCodingSession {
    inner: Weak<Inner>,
}
impl WeakCodingSession {
    /// Process-local runtime identity, independent of persisted session IDs.
    pub fn same_instance(&self, other: &Self) -> bool {
        self.inner.ptr_eq(&other.inner)
    }
    pub fn upgrade(&self) -> Option<CodingSession> {
        self.inner.upgrade().map(|inner| CodingSession { inner })
    }
}
impl CodingSession {
    pub fn downgrade(&self) -> WeakCodingSession {
        WeakCodingSession {
            inner: Arc::downgrade(&self.inner),
        }
    }
}

fn commit_manager_contents(
    manager: &mut SessionManager,
    contents: Vec<EntryContent>,
) -> Result<Vec<SessionEntry>, SessionError> {
    if contents.is_empty() {
        return Ok(Vec::new());
    }
    let snapshot = manager.snapshot().clone();
    let store = Arc::new(pi_coding_session::MemoryStore::new());
    pi_coding_session::SessionStore::create(store.as_ref(), &snapshot)?;
    let mut preview = SessionManager::open(store, &snapshot.header.id)?;
    for content in contents {
        preview.append(content)?;
    }
    let entries = preview.entries()[snapshot.entries.len()..].to_vec();
    manager.commit(entries.clone(), preview.leaf_id().map(str::to_owned))?;
    Ok(entries)
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SessionBranchCursor {
    pub session_id: String,
    pub navigation_generation: u64,
    pub leaf_id: Option<String>,
}

fn replace_blocked_images(blocks: &mut Vec<pi_ai::InputContent>) {
    if !blocks
        .iter()
        .any(|block| matches!(block, pi_ai::InputContent::Image(_)))
    {
        return;
    }

    let mut replaced = Vec::new();
    for block in std::mem::take(blocks) {
        let block = if matches!(block, pi_ai::InputContent::Image(_)) {
            pi_ai::InputContent::Text(pi_ai::TextContent {
                content_type: Default::default(),
                text: "Image reading is disabled.".into(),
                text_signature: None,
            })
        } else {
            block
        };
        let duplicate = matches!(&block,pi_ai::InputContent::Text(text)if text.text=="Image reading is disabled.")
            && matches!(replaced.last(),Some(pi_ai::InputContent::Text(text))if text.text=="Image reading is disabled.");
        if !duplicate {
            replaced.push(block);
        }
    }
    *blocks = replaced;
}
