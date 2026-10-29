//! Prompt, queue, abort, saved-session selection and recovery lifecycle.
use crate::{hooks::*, messages, session::*};
use pi_agent::*;
use pi_ai::{AssistantMessage, Message, Model, ModelThinkingLevel};
use pi_coding_session::{EntryContent, SessionEntry, SessionManager, SessionStore};
use serde_json::{Value, json};
use std::sync::{Arc, atomic::Ordering};

struct ActivityGuard {
    session: CodingSession,
    signal: CancellationToken,
    completed: bool,
}
impl Drop for ActivityGuard {
    fn drop(&mut self) {
        if !self.completed {
            self.signal.cancel();
            self.session.agent().abort();
            // Legacy MessageEnd dispatch may have exposed a provisional message
            // before its commit. A dropped owner must not retain that context.
            let _ = self.session.refresh();
        }
        {
            let mut s = lock(&self.session.inner.state);
            s.active = false;
            s.operation = None;
            s.run_prompt = None;
        }
        self.session.resolve_unqueued_inputs(None);
        self.session.inner.idle.notify_waiters();
    }
}
pub(crate) struct OperationGuard {
    session: CodingSession,
    signal: CancellationToken,
    completed: bool,
}
impl OperationGuard {
    pub fn new(session: &CodingSession, signal: CancellationToken) -> Result<Self, SessionError> {
        let mut state = lock(&session.inner.state);
        if state.disposed || state.disposing {
            return Err(SessionError::Disposed);
        }
        state.operation = Some(signal.clone());
        drop(state);
        Ok(Self {
            session: session.clone(),
            signal,
            completed: false,
        })
    }
    pub fn complete(&mut self) {
        self.completed = true;
    }
}
impl Drop for OperationGuard {
    fn drop(&mut self) {
        if !self.completed {
            self.signal.cancel();
        }
        lock(&self.session.inner.state).operation = None;
        self.session.inner.idle.notify_waiters();
    }
}
impl CodingSession {
    pub async fn prompt(
        &self,
        text: impl Into<String>,
        options: PromptOptions,
    ) -> Result<(), SessionError> {
        self.prompt_input(text.into(), options, None).await
    }
    pub(crate) async fn prompt_input(
        &self,
        text: String,
        options: PromptOptions,
        admission: Option<InputAdmission>,
    ) -> Result<(), SessionError> {
        self.ensure_live()?;
        let signal = admission
            .as_ref()
            .map(|i| i.cancellation.clone())
            .unwrap_or_else(CancellationToken::new);
        let input = InputEvent {
            text,
            images: options.images,
            source: options.source,
            delivery: options.delivery,
        };
        let (text, images) = match self
            .inner
            .hooks
            .input(input.clone(), signal.clone())
            .await?
        {
            InputResult::Handled => {
                if let Some(admission) = &admission {
                    lock(&self.inner.inputs).resolve(admission.id, crate::InputResolution::Handled);
                }
                return Ok(());
            }
            InputResult::Continue => (input.text, input.images),
            InputResult::Transform { text, images } => (text, images.unwrap_or(input.images)),
        };
        let text = if options.expand_prompt_templates {
            self.expand_input(&text)
        } else {
            text
        };
        if signal.is_cancelled() {
            return Ok(());
        }
        if self.is_streaming() {
            return match options.delivery {
                Some(Delivery::Steer) => self
                    .queue_message_input(text, images, true, admission, input.source)
                    .await
                    .map(|_| ()),
                Some(Delivery::FollowUp) => self
                    .queue_message_input(text, images, false, admission, input.source)
                    .await
                    .map(|_| ()),
                _ => Err(SessionError::Invalid(
                    "Agent is streaming; choose steer or follow-up delivery".into(),
                )),
            };
        }
        self.run_prompt_input(messages::user_message(text, images), signal, admission)
            .await
    }
    async fn run_prompt(
        &self,
        message: AgentMessage,
        signal: CancellationToken,
    ) -> Result<(), SessionError> {
        self.run_prompt_input(message, signal, None).await
    }
    async fn run_prompt_input(
        &self,
        message: AgentMessage,
        signal: CancellationToken,
        admission: Option<InputAdmission>,
    ) -> Result<(), SessionError> {
        let _gate = self.idle_guard()?;
        if let Some(input) = &admission {
            let mut inputs = lock(&self.inner.inputs);
            let Some(tracked) = inputs.pending.get_mut(&input.id) else {
                return Ok(());
            };
            tracked.owns_run = true;
        }
        {
            let mut s = lock(&self.inner.state);
            if s.disposed || s.disposing {
                return Err(SessionError::Disposed);
            }
            s.active = true;
            s.operation = Some(signal.clone());
            s.last_error = None;
            s.persistence_error = None;
            s.outcome = ActivityOutcome::Completed;
            s.pending_tools.clear();
        }
        let mut activity = ActivityGuard {
            session: self.clone(),
            signal: signal.clone(),
            completed: false,
        };
        let result = self
            .run_prompt_inner(message, signal.clone(), admission)
            .await;
        let flush = self.flush_bash().await.and(self.flush_custom().await);
        let result = result.and(flush);
        if let Err(error) = &result {
            let mut s = lock(&self.inner.state);
            s.last_error = Some(error.to_string());
            s.outcome = ActivityOutcome::Error;
        }
        if signal.is_cancelled() {
            lock(&self.inner.state).outcome = ActivityOutcome::Aborted;
        }
        let retry = {
            let mut s = lock(&self.inner.state);
            let n = s.retry_attempt;
            s.retry_attempt = 0;
            s.failed = None;
            n
        };
        if retry > 0 {
            self.emit(SessionEvent::AutoRetryEnd {
                success: false,
                attempt: retry,
                final_error: Some(if signal.is_cancelled() {
                    "Retry cancelled".into()
                } else {
                    "Retry exhausted".into()
                }),
            })
            .await?;
        }
        self.on_cache_settled();
        self.resolve_unqueued_inputs(result.as_ref().err().map(|e| e.to_string()).as_deref());
        let outcome = lock(&self.inner.state).outcome;
        if let Err(error) = self.emit(SessionEvent::AgentSettled { outcome }).await {
            lock(&self.inner.state).last_error = Some(error.to_string());
            return Err(error);
        }
        activity.completed = true;
        result
    }
    async fn run_prompt_inner(
        &self,
        message: AgentMessage,
        signal: CancellationToken,
        admission: Option<InputAdmission>,
    ) -> Result<(), SessionError> {
        if let Some(last) = self.last_assistant() {
            self.check_compaction(&last, false, signal.clone()).await?;
        }
        self.flush_bash().await?;
        self.flush_custom().await?;
        let hook = self
            .inner
            .hooks
            .before_agent_start(
                BeforeAgentStart {
                    message: message.clone(),
                    system_prompt: self.system_prompt()?,
                    active_tools: self.active_tool_names(),
                },
                signal.clone(),
            )
            .await?;
        if let Some(tools) = hook.active_tools {
            self.set_active_tools(tools)?;
        }
        lock(&self.inner.state).run_prompt = hook.system_prompt;
        self.refresh_loadout(signal.clone()).await?;
        let mut starting = Vec::new();
        if let Some(patch) = self.prompt_patch()? {
            starting.push(patch.into());
        }
        starting.push(AdmittedMessage {
            message: self.normalize_prompt_message(message).await?,
            input: admission,
        });
        starting.extend(
            std::mem::take(&mut lock(&self.inner.state).next_turn)
                .into_iter()
                .map(Into::into),
        );
        starting.extend(hook.messages.into_iter().map(Into::into));
        if signal.is_cancelled() {
            return Ok(());
        }
        let config = self.agent().configuration();
        let model = self.model();
        let api_key = self.inner.models.api_key(model, signal.clone()).await?;
        let mut config = config;
        if api_key.is_some() {
            config.api_key = api_key;
        }
        self.agent().set_configuration(config);
        self.flush_bash().await?;
        self.flush_custom().await?;
        if signal.is_cancelled() {
            return Ok(());
        }
        let mut result = self.agent().prompt_inputs(starting).await;
        loop {
            if let Some(error) = lock(&self.inner.state).persistence_error.take() {
                self.refresh()?;
                return Err(SessionError::Invalid(error));
            }
            result?;
            if signal.is_cancelled() {
                break;
            }
            let mut more = false;
            if let Some(last) = self.last_assistant() {
                let limits = self
                    .model_for_message(&last)
                    .unwrap_or_else(|| self.model());
                if pi_ai::is_retryable_assistant_error(&last)
                    && !pi_ai::is_context_overflow(&last, Some(limits.context_window))
                {
                    more = self.prepare_retry(&last, signal.clone()).await?;
                }
                if !more {
                    more = self.check_compaction(&last, true, signal.clone()).await?;
                }
            }
            if !more {
                more = self.agent().has_queued_messages();
            }
            if !more {
                more = self
                    .boundary(BoundaryKind::AgentBeforeSettle, None, signal.clone())
                    .await?;
            }
            if !more || signal.is_cancelled() {
                break;
            }
            self.refresh()?;
            result = self.agent().continue_run().await;
        }
        Ok(())
    }
    pub async fn continue_run(&self) -> Result<(), SessionError> {
        let _gate = self.idle_guard()?;
        let signal = CancellationToken::new();
        {
            let mut s = lock(&self.inner.state);
            if s.disposed || s.disposing {
                return Err(SessionError::Disposed);
            }
            s.active = true;
            s.operation = Some(signal.clone());
            s.last_error = None;
            s.persistence_error = None;
        }
        let mut activity = ActivityGuard {
            session: self.clone(),
            signal,
            completed: false,
        };
        self.refresh()?;
        let mut result = self
            .agent()
            .continue_run()
            .await
            .map(|_| ())
            .map_err(SessionError::from);
        if let Some(error) = lock(&self.inner.state).persistence_error.take() {
            self.refresh()?;
            result = Err(SessionError::Invalid(error));
        }
        self.resolve_unqueued_inputs(result.as_ref().err().map(|e| e.to_string()).as_deref());
        if let Err(e) = &result {
            lock(&self.inner.state).last_error = Some(e.to_string());
        }
        activity.completed = true;
        result
    }
    pub async fn steer(
        &self,
        text: impl Into<String>,
        images: Vec<pi_ai::ImageContent>,
    ) -> Result<(), SessionError> {
        self.ensure_live()?;
        self.queue_input(text.into(), images, true).await
    }
    pub async fn follow_up(
        &self,
        text: impl Into<String>,
        images: Vec<pi_ai::ImageContent>,
    ) -> Result<(), SessionError> {
        self.ensure_live()?;
        self.queue_input(text.into(), images, false).await
    }
    async fn queue_input(
        &self,
        text: String,
        images: Vec<pi_ai::ImageContent>,
        steer: bool,
    ) -> Result<(), SessionError> {
        let signal = lock(&self.inner.state)
            .operation
            .clone()
            .unwrap_or_else(CancellationToken::new);
        let input = InputEvent {
            text,
            images,
            source: InputSource::Interactive,
            delivery: Some(if steer {
                Delivery::Steer
            } else {
                Delivery::FollowUp
            }),
        };
        let (text, images) = match self.inner.hooks.input(input.clone(), signal).await? {
            InputResult::Handled => return Ok(()),
            InputResult::Continue => (input.text, input.images),
            InputResult::Transform { text, images } => (text, images.unwrap_or(input.images)),
        };
        self.queue_message(self.expand_input(&text), images, steer)
            .await
    }
    async fn queue_message(
        &self,
        text: String,
        images: Vec<pi_ai::ImageContent>,
        steer: bool,
    ) -> Result<(), SessionError> {
        self.queue_message_input(text, images, steer, None, InputSource::Interactive)
            .await
            .map(|_| ())
    }
    /// Whether the input entered the FIFO. A tracked input withdrawn since its reservation
    /// (cancellation clears every pending receipt) is left out, and its receipt says so.
    async fn queue_message_input(
        &self,
        text: String,
        images: Vec<pi_ai::ImageContent>,
        steer: bool,
        admission: Option<InputAdmission>,
        source: InputSource,
    ) -> Result<bool, SessionError> {
        let message = messages::user_message(text.clone(), images.clone());
        let display_id = admission.as_ref().map(|a| a.id);
        {
            let mut inputs = lock(&self.inner.inputs);
            if let Some(admission) = &admission {
                let Some(input) = inputs.pending.get_mut(&admission.id) else {
                    return Ok(false);
                };
                if admission.cancellation.is_cancelled() {
                    return Ok(false);
                }
                input.queued = Some(crate::QueuedInput {
                    id: admission.id,
                    delivery: if steer {
                        Delivery::Steer
                    } else {
                        Delivery::FollowUp
                    },
                    text: text.clone(),
                    images,
                    source,
                });
            }
            // Publish the display record before the FIFO can be drained. The
            // input lock also orders cancellation against this admission.
            let mut state = lock(&self.inner.state);
            let display = QueueDisplay {
                id: display_id,
                text,
            };
            if steer {
                state.steering.push(display);
            } else {
                state.follow_up.push(display);
            }
            drop(state);
            let message = AdmittedMessage {
                message,
                input: admission,
            };
            if steer {
                self.agent().steer_input(message);
            } else {
                self.agent().follow_up_input(message);
            }
        }
        self.emit_queue().await?;
        Ok(true)
    }
    pub async fn queue_tracked(
        &self,
        text: impl Into<String>,
        images: Vec<pi_ai::ImageContent>,
        delivery: Delivery,
    ) -> Result<crate::QueueAdmission, SessionError> {
        self.ensure_live()?;
        if !matches!(delivery, Delivery::Steer | Delivery::FollowUp) {
            return Err(SessionError::Invalid(
                "Queue delivery must be steer or follow-up".into(),
            ));
        }
        let (ticket, admission) = self.reserve_input();
        let mut guard = crate::inputs::InputDriverGuard {
            session: self.clone(),
            id: ticket.id,
            completed: false,
        };
        let result = async {
            let input = InputEvent {
                text: text.into(),
                images,
                source: InputSource::Interactive,
                delivery: Some(delivery),
            };
            let (text, images) = match self
                .inner
                .hooks
                .input(input.clone(), admission.cancellation.clone())
                .await?
            {
                InputResult::Handled => {
                    lock(&self.inner.inputs).resolve(ticket.id, crate::InputResolution::Handled);
                    return Ok(false);
                }
                InputResult::Continue => (input.text, input.images),
                InputResult::Transform { text, images } => (text, images.unwrap_or(input.images)),
            };
            self.ensure_live()?;
            let queued = self
                .queue_message_input(
                    self.expand_input(&text),
                    images,
                    delivery == Delivery::Steer,
                    Some(admission),
                    input.source,
                )
                .await?;
            // Withdrawn while the input hook ran: the receipt already holds the outcome.
            if !queued {
                return Err(SessionError::Cancelled);
            }
            Ok::<_, SessionError>(true)
        }
        .await;
        guard.completed = true;
        match result {
            Ok(true) => Ok(crate::QueueAdmission::Queued(ticket)),
            Ok(false) => Ok(crate::QueueAdmission::Handled),
            // The live driver can resolve this exact input while queue
            // notification awaits acknowledgement; its receipt must survive.
            Err(error) => self.fail_queue_admission(ticket, error),
        }
    }
    fn clear_queue_tracked_inner(&self) -> (Vec<crate::QueuedInput>, Vec<String>) {
        let (steering, follow_up) = self.agent().take_queued_inputs();
        let mut inputs = lock(&self.inner.inputs);
        let mut removed = Vec::new();
        let mut untracked = Vec::new();
        for message in steering.into_iter().chain(follow_up) {
            if let Some(admission) = message.input {
                if let Some(queued) = inputs
                    .pending
                    .get(&admission.id)
                    .and_then(|i| i.queued.clone())
                {
                    removed.push(queued);
                }
                admission.cancellation.cancel();
                inputs.resolve(admission.id, crate::InputResolution::Cancelled);
            } else if let Ok(value) = serde_json::to_value(&message.message) {
                untracked.push(messages::content_text(&value["content"], ""));
            }
        }
        (removed, untracked)
    }
    pub async fn clear_queue_tracked(&self) -> Result<Vec<crate::QueuedInput>, SessionError> {
        self.ensure_live()?;
        let (removed, untracked) = self.clear_queue_tracked_inner();
        {
            let mut state = lock(&self.inner.state);
            for input in &removed {
                let queue = if input.delivery == Delivery::Steer {
                    &mut state.steering
                } else {
                    &mut state.follow_up
                };
                if let Some(index) = queue
                    .iter()
                    .position(|display| display.id == Some(input.id))
                {
                    queue.remove(index);
                }
            }
            for text in untracked {
                if let Some(index) = state
                    .steering
                    .iter()
                    .position(|m| m.id.is_none() && m.text == text)
                {
                    state.steering.remove(index);
                } else if let Some(index) = state
                    .follow_up
                    .iter()
                    .position(|m| m.id.is_none() && m.text == text)
                {
                    state.follow_up.remove(index);
                }
            }
        }
        self.emit_queue().await?;
        Ok(removed)
    }
    pub async fn clear_queue(&self) -> Result<(Vec<String>, Vec<String>), SessionError> {
        let _ = self.clear_queue_tracked_inner();
        let queues = {
            let mut s = lock(&self.inner.state);
            (
                std::mem::take(&mut s.steering)
                    .into_iter()
                    .map(|m| m.text)
                    .collect(),
                std::mem::take(&mut s.follow_up)
                    .into_iter()
                    .map(|m| m.text)
                    .collect(),
            )
        };
        self.emit_queue().await?;
        Ok(queues)
    }
    pub fn pending_message_count(&self) -> usize {
        let s = lock(&self.inner.state);
        s.steering.len() + s.follow_up.len()
    }
    pub fn steering_messages(&self) -> Vec<String> {
        lock(&self.inner.state)
            .steering
            .iter()
            .map(|m| m.text.clone())
            .collect()
    }
    pub fn follow_up_messages(&self) -> Vec<String> {
        lock(&self.inner.state)
            .follow_up
            .iter()
            .map(|m| m.text.clone())
            .collect()
    }
    pub fn set_steering_mode(&self, mode: QueueMode) {
        self.agent().set_steering_mode(mode);
    }
    pub fn set_follow_up_mode(&self, mode: QueueMode) {
        self.agent().set_follow_up_mode(mode);
    }
    pub async fn send_custom_message(
        &self,
        custom_type: impl Into<String>,
        content: Value,
        display: bool,
        details: Option<Value>,
        delivery: Option<Delivery>,
        trigger_turn: bool,
    ) -> Result<(), SessionError> {
        self.ensure_live()?;
        if !content.is_string() && !content.is_array() {
            return Err(SessionError::Invalid(
                "custom message content must be a string or blocks".into(),
            ));
        }
        let value = json!({"role":"custom","customType":custom_type.into(),"content":content,"display":display,"timestamp":messages::now()});
        let mut value = value;
        if let Some(details) = details {
            value["details"] = details;
        }
        let message = messages::decode(value)?;
        if delivery == Some(Delivery::NextTurn) {
            lock(&self.inner.state).next_turn.push(message);
            return Ok(());
        }
        if trigger_turn {
            if self.is_streaming() {
                if delivery == Some(Delivery::FollowUp) {
                    self.agent().follow_up(message);
                } else {
                    self.agent().steer(message);
                }
                return Ok(());
            }
            return self.run_prompt(message, CancellationToken::new()).await;
        }
        if self.is_streaming() {
            lock(&self.inner.state).pending_custom.push(message);
            Ok(())
        } else {
            let _guard = self.idle_guard()?;
            self.persist_message(&message)?;
            self.refresh()
        }
    }
    pub fn abort(&self) {
        if let Some(signal) = lock(&self.inner.state).operation.clone() {
            signal.cancel();
        }
        self.agent().abort();
    }
    pub async fn wait_for_idle(&self) -> Result<(), SessionError> {
        loop {
            let notified = self.inner.idle.notified();
            if self.is_idle() {
                break;
            }
            notified.await;
        }
        if let Some(error) = lock(&self.inner.state).last_error.clone() {
            return Err(SessionError::Invalid(error));
        }
        Ok(())
    }
    pub async fn dispose(&self) -> Result<(), SessionError> {
        {
            let mut state = lock(&self.inner.state);
            if state.disposed {
                return Ok(());
            }
            // Admission closes before cancellation or any cleanup await. A dropped
            // disposal future leaves the owner closed; another caller can finish it.
            state.disposing = true;
        }
        self.cancel_all_inputs();
        self.agent().clear_all_queues();
        self.abort_bash();
        self.abort();
        self.cancel_cache_warming();
        // The existing exclusive gate also settles operations admitted just before
        // shutdown, including those still preparing a run. It serializes disposers.
        let _gate = self.inner.gate.clone().lock_owned().await;
        let _ = self.wait_for_idle().await;
        self.wait_for_bash().await;
        let listeners = std::mem::take(&mut *lock(&self.inner.listeners));
        drop(listeners);
        {
            let mut state = lock(&self.inner.state);
            state.disposed = true;
            state.disposing = false;
        }
        Ok(())
    }
    pub(crate) fn last_assistant(&self) -> Option<AssistantMessage> {
        self.agent()
            .state()
            .messages
            .into_iter()
            .rev()
            .find_map(|m| {
                if let AgentMessage::BuiltIn(Message::Assistant(m)) = m {
                    Some(*m)
                } else {
                    None
                }
            })
    }
    fn model_for_message(&self, message: &AssistantMessage) -> Option<Model> {
        let selected = self.model();
        if self.inner.models.is_virtual(&selected) {
            self.inner
                .models
                .get_model(message.provider.as_str(), message.model.as_str())
        } else {
            (selected.provider == message.provider && selected.id == message.model)
                .then_some(selected)
        }
    }
    async fn prepare_retry(
        &self,
        message: &AssistantMessage,
        signal: CancellationToken,
    ) -> Result<bool, SessionError> {
        let (attempt, policy) = {
            let mut s = lock(&self.inner.state);
            if !s.retry.enabled || s.retry_attempt >= s.retry.max_retries.get() {
                return Ok(false);
            }
            s.retry_attempt += 1;
            (s.retry_attempt, s.retry.clone())
        };
        let delay = policy
            .base_delay_ms
            .get()
            .saturating_mul(
                1u64.checked_shl((attempt - 1).min(63) as u32)
                    .unwrap_or(u64::MAX),
            )
            .min(policy.max_agent_delay_ms.map(|v| v.get()).unwrap_or(60000));
        self.emit(SessionEvent::AutoRetryStart {
            attempt,
            max_attempts: policy.max_retries.get(),
            delay_ms: delay,
            error_message: message
                .error_message
                .clone()
                .unwrap_or_else(|| "Unknown error".into()),
        })
        .await?;
        self.omit_recovery_attempt(message).await?;
        lock(&self.inner.state).failed = Some(message.clone());
        Ok(sleep_abortable(delay, &signal).await)
    }
    pub(crate) async fn omit_recovery_attempt(
        &self,
        message: &AssistantMessage,
    ) -> Result<(), SessionError> {
        let projection = self.projection();
        let value = serde_json::to_value(message)?;
        let mut ids = Vec::new();
        let mut found = false;
        let calls = message
            .content
            .iter()
            .filter_map(|b| {
                if let pi_ai::AssistantContent::ToolCall(c) = b {
                    Some(c.id.as_str())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        'entries: for e in &projection.entries {
            for m in &e.messages {
                if !found && *m == value {
                    ids.push(e.source_entry.id.clone());
                    found = true;
                } else if found && m["role"] == "assistant" {
                    break 'entries;
                } else if found
                    && m["role"] == "toolResult"
                    && m["toolCallId"]
                        .as_str()
                        .is_some_and(|id| calls.contains(&id))
                {
                    ids.push(e.source_entry.id.clone());
                }
            }
        }
        if !found {
            return Err(SessionError::Invalid(
                "Cannot persist recovery omission: assistant has no source entry".into(),
            ));
        }
        let entries = self.commit_contents(
            ids.into_iter()
                .map(|target_id| EntryContent::ContextEdit {
                    target_id,
                    replacement: None,
                })
                .collect(),
        )?;
        self.refresh()?;
        for entry in entries {
            self.emit(SessionEvent::EntryAppended { entry }).await?;
        }
        Ok(())
    }
    async fn check_compaction(
        &self,
        message: &AssistantMessage,
        post_run: bool,
        signal: CancellationToken,
    ) -> Result<bool, SessionError> {
        let settings = lock(&self.inner.state).compaction.clone();
        if !settings.enabled || (post_run && message.stop_reason == pi_ai::StopReason::Aborted) {
            return Ok(false);
        }
        let message_model = self.model_for_message(message);
        let model = message_model.clone().unwrap_or_else(|| self.model());
        let snapshot = self.snapshot();
        let branch = crate::compaction::branch_path(&snapshot, snapshot.leaf_id.as_deref());
        let latest = branch
            .iter()
            .rev()
            .find(|e| matches!(e.content, EntryContent::Compaction { .. }));
        if latest.is_some_and(|entry| message.timestamp.get() <= entry.timestamp) {
            return Ok(false);
        }
        let projection = self.projection();
        let value = serde_json::to_value(message)?;
        let assistant_id = find_message_entry(&snapshot, &value);
        let position = assistant_id
            .as_ref()
            .and_then(|id| branch.iter().position(|e| e.id == *id));
        let after = position.map(|i| &branch[i + 1..]).unwrap_or_default();
        let projected = assistant_id.as_ref().is_none_or(|id| {
            projection.entries.iter().any(|e| {
                e.source_entry.id == *id && e.messages.iter().any(|m| m["role"] == "assistant")
            })
        });
        let edited = after
            .iter()
            .any(|e| matches!(e.content, EntryContent::ContextEdit { .. }));
        let last_edit=after.iter().rev().find(|e|matches!(&e.content,EntryContent::ContextEdit{target_id,..} if Some(target_id)==assistant_id.as_ref()));
        let retained = assistant_id.is_none()
            || (!after
                .iter()
                .any(|e| matches!(e.content, EntryContent::Compaction { .. }))
                && !last_edit.is_some_and(|e| {
                    matches!(
                        e.content,
                        EntryContent::ContextEdit {
                            replacement: None,
                            ..
                        }
                    )
                }));
        let explicit = message.stop_reason == pi_ai::StopReason::Error
            && pi_ai::is_context_overflow(message, None);
        let overflow = message_model.is_some()
            && ((explicit && retained)
                || (projected
                    && !edited
                    && pi_ai::is_context_overflow(message, Some(model.context_window))));
        let length = message_model.is_some()
            && projected
            && pi_ai::is_recoverable_length(message, model.max_tokens);
        if overflow || length {
            let retry = message.stop_reason != pi_ai::StopReason::Stop;
            if retry {
                let attempted = {
                    let mut state = lock(&self.inner.state);
                    let old = state.overflow_attempted;
                    state.overflow_attempted = true;
                    old
                };
                if attempted {
                    self.emit(SessionEvent::CompactionEnd{reason:CompactionReason::Overflow,result:None,aborted:false,will_retry:false,error:Some(if overflow{"Context overflow recovery failed after one compact-and-retry attempt. Try reducing context or switching to a larger-context model."}else{"Truncated response recovery failed after one compact-and-retry attempt."}.into())}).await?;
                    return Ok(false);
                }
                self.omit_recovery_attempt(message).await?;
            }
            let compacted = self
                .auto_compact(CompactionReason::Overflow, retry, signal)
                .await?;
            if compacted && retry {
                lock(&self.inner.state).failed = Some(message.clone());
            }
            return Ok(compacted && (retry || self.agent().has_queued_messages()));
        }
        let tokens = crate::estimate_projected_context_tokens(&projection, &branch).tokens;
        if crate::should_compact(tokens, model.context_window.get(), &settings) {
            let compacted = self
                .auto_compact(CompactionReason::Threshold, false, signal)
                .await?;
            return Ok(compacted && self.agent().has_queued_messages());
        }
        Ok(false)
    }
    pub(crate) async fn auto_compact(
        &self,
        reason: CompactionReason,
        retry: bool,
        signal: CancellationToken,
    ) -> Result<bool, SessionError> {
        match self.compact_internal(None, reason, retry, signal).await {
            Ok(result) => Ok(result.is_some()),
            Err(error @ SessionError::Storage(_)) => Err(error),
            Err(_) => Ok(false),
        }
    }
    pub async fn set_model(&self, model: Model) -> Result<(), SessionError> {
        self.ensure_live()?;
        let old = self.model();
        if old.provider == model.provider && old.id == model.id {
            return Ok(());
        }
        let level = pi_ai::clamp_thinking_level(&model, self.thinking_level());
        self.commit_contents(vec![
            EntryContent::ModelChange {
                provider: model.provider.to_string(),
                model_id: model.id.to_string(),
            },
            EntryContent::ThinkingLevelChange {
                thinking_level: thinking_name(level),
            },
        ])?;
        self.agent().set_model(model.clone());
        self.agent().set_thinking_level(level);
        lock(&self.inner.state).routed = None;
        self.cancel_cache_warming();
        self.emit(SessionEvent::ModelSelect {
            model: Box::new(model),
            previous_model: Box::new(old),
        })
        .await
    }
    pub async fn set_thinking_level(&self, level: ModelThinkingLevel) -> Result<(), SessionError> {
        self.ensure_live()?;
        let level = pi_ai::clamp_thinking_level(&self.model(), level);
        if level == self.thinking_level() {
            return Ok(());
        }
        lock(&self.inner.manager).set_thinking_level(thinking_name(level))?;
        self.agent().set_thinking_level(level);
        self.cancel_cache_warming();
        self.emit(SessionEvent::ThinkingLevelSelect { level }).await
    }
    pub async fn cycle_model(&self, backward: bool) -> Result<Option<Model>, SessionError> {
        let scope = lock(&self.inner.state).scoped_models.clone();
        let models = if scope.is_empty() {
            self.inner.models.models()
        } else {
            scope.iter().map(|s| s.model.clone()).collect()
        };
        if models.is_empty() {
            return Ok(None);
        }
        let old = self.model();
        let index = models
            .iter()
            .position(|m| m.provider == old.provider && m.id == old.id)
            .unwrap_or(0);
        let index = if backward {
            (index + models.len() - 1) % models.len()
        } else {
            (index + 1) % models.len()
        };
        let model = models[index].clone();
        self.set_model(model.clone()).await?;
        if let Some(level) = scope
            .iter()
            .find(|s| s.model.provider == model.provider && s.model.id == model.id)
            .and_then(|s| s.thinking_level)
        {
            self.set_thinking_level(level).await?;
        }
        Ok(Some(model))
    }
    pub fn scoped_models(&self) -> Vec<ScopedModel> {
        lock(&self.inner.state).scoped_models.clone()
    }
    pub fn set_scoped_models(&self, models: Vec<ScopedModel>) -> Result<(), SessionError> {
        self.ensure_live()?;
        lock(&self.inner.state).scoped_models = models;
        Ok(())
    }
    pub fn available_thinking_levels(&self) -> Vec<ModelThinkingLevel> {
        pi_ai::get_supported_thinking_levels(&self.model())
    }
    pub fn supports_thinking(&self) -> bool {
        self.model().reasoning
    }
    pub async fn cycle_thinking_level(&self) -> Result<Option<ModelThinkingLevel>, SessionError> {
        if !self.supports_thinking() {
            return Ok(None);
        }
        let levels = self.available_thinking_levels();
        if levels.is_empty() {
            return Ok(None);
        }
        let current = levels
            .iter()
            .position(|level| *level == self.thinking_level())
            .map(|i| i + 1)
            .unwrap_or(0)
            % levels.len();
        let level = levels[current];
        self.set_thinking_level(level).await?;
        Ok(Some(level))
    }
    pub fn tree(&self) -> Vec<pi_coding_session::TreeNode> {
        lock(&self.inner.manager).tree()
    }
    pub fn user_messages_for_forking(&self) -> Vec<(String, String)> {
        self.snapshot()
            .entries
            .iter()
            .filter_map(|e| match &e.content {
                EntryContent::Message { message } if message["role"] == "user" => {
                    let text = messages::content_text(&message["content"], "");
                    (!text.is_empty()).then(|| (e.id.clone(), text))
                }
                _ => None,
            })
            .collect()
    }
    pub fn last_assistant_text(&self) -> Option<String> {
        let message =
            self.messages()
                .ok()?
                .into_iter()
                .rev()
                .find_map(|message| match message {
                    AgentMessage::BuiltIn(Message::Assistant(message))
                        if message.stop_reason != pi_ai::StopReason::Aborted
                            || !message.content.is_empty() =>
                    {
                        Some(message)
                    }
                    _ => None,
                })?;
        let value = serde_json::to_value(message).ok()?;
        let text = messages::content_text(&value["content"], "")
            .trim()
            .to_owned();
        (!text.is_empty()).then_some(text)
    }
    pub fn retry_attempt(&self) -> u64 {
        lock(&self.inner.state).retry_attempt
    }
    pub fn is_retrying(&self) -> bool {
        self.retry_attempt() > 0
    }
    pub fn auto_retry_enabled(&self) -> bool {
        lock(&self.inner.state).retry.enabled
    }
    pub fn auto_compaction_enabled(&self) -> bool {
        lock(&self.inner.state).compaction.enabled
    }
    pub fn is_compacting(&self) -> bool {
        let state = lock(&self.inner.state);
        !state.active && state.operation.is_some()
    }
    pub fn abort_retry(&self) {
        if let Some(signal) = lock(&self.inner.state).operation.clone() {
            signal.cancel();
        }
    }
    pub fn abort_branch_summary(&self) {
        self.abort_compaction();
    }
    fn expand_input(&self, text: &str) -> String {
        let resources = lock(&self.inner.state).resources.clone();
        let expanded = if let Some(command) = text.strip_prefix("/skill:") {
            let (name, args) = command.split_once(' ').unwrap_or((command, ""));
            if let Some(skill) = resources
                .skills
                .as_ref()
                .and_then(|skills| skills.iter().find(|s| s.name == name))
            {
                let body = crate::parse_frontmatter(&skill.content)
                    .map(|(_, body)| body)
                    .unwrap_or_else(|_| skill.content.clone());
                let directory = std::path::Path::new(&skill.file_path)
                    .parent()
                    .map(|p| p.to_string_lossy().into_owned())
                    .unwrap_or_default();
                let block = format!(
                    "<skill name=\"{}\" location=\"{}\">\nReferences are relative to {}.\n\n{}\n</skill>",
                    skill.name,
                    skill.file_path,
                    directory,
                    body.trim()
                );
                if args.trim().is_empty() {
                    block
                } else {
                    format!("{block}\n\n{}", args.trim())
                }
            } else {
                text.to_owned()
            }
        } else {
            text.to_owned()
        };
        crate::expand_prompt_template(
            &expanded,
            resources.prompt_templates.as_deref().unwrap_or_default(),
        )
    }
    pub fn set_resources(&self, resources: crate::Resources) -> Result<(), SessionError> {
        self.ensure_live()?;
        let mut state = lock(&self.inner.state);
        state.resources = resources.clone();
        state.prompt_options.resources = resources;
        Ok(())
    }
    pub fn set_compaction_settings(
        &self,
        settings: crate::CompactionSettings,
    ) -> Result<(), SessionError> {
        self.ensure_live()?;
        lock(&self.inner.state).compaction = settings;
        Ok(())
    }
    pub fn set_auto_compaction_enabled(&self, enabled: bool) {
        lock(&self.inner.state).compaction.enabled = enabled;
    }
    pub fn set_auto_retry_enabled(&self, enabled: bool) {
        lock(&self.inner.state).retry.enabled = enabled;
    }
    pub(crate) async fn boundary(
        &self,
        kind: BoundaryKind,
        turn: Option<AgentTurnContext>,
        signal: CancellationToken,
    ) -> Result<bool, SessionError> {
        let snapshot = self.snapshot();
        let messages = self.messages()?;
        let pending = self
            .agent()
            .peek_queued_messages()
            .into_iter()
            .chain(lock(&self.inner.state).pending_custom.clone())
            .collect::<Vec<_>>();
        let context_can_continue = can_continue(&messages, &pending)?;
        let message = turn.as_ref().map(|t| t.message.clone());
        let message_entry_id = message
            .as_ref()
            .and_then(|m| find_message_entry(&snapshot, &serde_json::to_value(m).ok()?));
        let tool_results = turn
            .as_ref()
            .map(|t| t.tool_results.clone())
            .unwrap_or_default();
        let tool_result_entry_ids = tool_results
            .iter()
            .filter_map(|m| find_message_entry(&snapshot, &serde_json::to_value(m).ok()?))
            .collect();
        let turn_index = lock(&self.inner.state).turn_index;
        let result = self
            .inner
            .hooks
            .boundary(
                BoundaryContext {
                    kind,
                    turn_index,
                    message,
                    tool_results,
                    message_entry_id,
                    tool_result_entry_ids,
                    snapshot,
                    context_messages: messages,
                    pending_messages: pending,
                    can_continue: context_can_continue,
                },
                signal.clone(),
            )
            .await?;
        self.commit_boundary_drafts(result.entries).await?;
        self.flush_custom().await?;
        if signal.is_cancelled() {
            return Ok(false);
        }
        let pending = self.agent().peek_queued_messages();
        if result.continue_run && !can_continue(&self.messages()?, &pending)? {
            return Err(SessionError::Invalid(
                "Boundary requested continuation without runnable model context".into(),
            ));
        }
        Ok(result.continue_run || self.agent().has_queued_messages())
    }
    pub async fn preview_boundary_drafts(
        &self,
        drafts: &[SessionBoundaryDraft],
    ) -> Result<pi_coding_session::SessionProjection, SessionError> {
        self.ensure_live()?;
        let mut manager = self.preview_manager()?;
        apply_drafts(&mut manager, drafts)?;
        Ok(manager.projection())
    }
    pub async fn commit_boundary_drafts(
        &self,
        drafts: Vec<SessionBoundaryDraft>,
    ) -> Result<Vec<SessionEntry>, SessionError> {
        self.ensure_live()?;
        if drafts.is_empty() {
            return Ok(Vec::new());
        }
        let (entries, revision) = {
            let manager = lock(&self.inner.manager);
            let snapshot = manager.snapshot().clone();
            let mut preview = preview_manager_from(&snapshot)?;
            apply_drafts(&mut preview, &drafts)?;
            (
                preview.entries()[snapshot.entries.len()..].to_vec(),
                snapshot.revision,
            )
        };
        {
            let mut m = lock(&self.inner.manager);
            if m.snapshot().revision != revision {
                return Err(SessionError::Invalid(
                    "Session changed while preparing boundary drafts".into(),
                ));
            }
            let leaf = entries.last().map(|e| e.id.clone());
            m.commit(entries.clone(), leaf)?;
        }
        self.refresh()?;
        for entry in &entries {
            self.emit(SessionEvent::EntryAppended {
                entry: entry.clone(),
            })
            .await?;
        }
        Ok(entries)
    }
    fn preview_manager(&self) -> Result<SessionManager, SessionError> {
        preview_manager_from(&self.snapshot())
    }
    pub async fn navigate_tree(
        &self,
        target_id: &str,
        options: NavigateOptions,
    ) -> Result<NavigationResult, SessionError> {
        let _guard = self.idle_guard()?;
        let signal = CancellationToken::new();
        let mut operation = OperationGuard::new(self, signal.clone())?;
        let result = self.navigate_inner(target_id, options, signal).await;
        operation.complete();
        result
    }
    async fn navigate_inner(
        &self,
        target_id: &str,
        options: NavigateOptions,
        signal: CancellationToken,
    ) -> Result<NavigationResult, SessionError> {
        let snapshot = self.snapshot();
        if snapshot.leaf_id.as_deref() == Some(target_id) {
            let _manager = lock(&self.inner.manager);
            self.inner
                .navigation_generation
                .fetch_add(1, Ordering::AcqRel);
            return Ok(NavigationResult::default());
        }
        let target = snapshot
            .entries
            .iter()
            .find(|e| e.id == target_id)
            .ok_or_else(|| SessionError::Invalid(format!("Entry {target_id} not found")))?
            .clone();
        let (entries, common) = crate::collect_branch_entries(&snapshot, target_id);
        let preparation = TreePreparation {
            target_id: target_id.into(),
            old_leaf_id: snapshot.leaf_id.clone(),
            common_ancestor_id: common,
            entries_to_summarize: entries.clone(),
            summarize: options.summarize,
            custom_instructions: options.custom_instructions.clone(),
            replace_instructions: options.replace_instructions,
            label: options.label.clone(),
        };
        let hook = self
            .inner
            .hooks
            .before_tree(preparation, signal.clone())
            .await?;
        if hook.cancel {
            return Ok(NavigationResult {
                cancelled: true,
                ..Default::default()
            });
        }
        let from_hook = options.summarize && hook.summary.is_some();
        let mut summary = if options.summarize {
            hook.summary
        } else {
            None
        };
        if options.summarize && summary.is_none() && !entries.is_empty() {
            summary = Some(
                self.summarize_branch(
                    &entries,
                    hook.custom_instructions.or(options.custom_instructions),
                    hook.replace_instructions
                        .unwrap_or(options.replace_instructions),
                    signal.clone(),
                )
                .await?,
            );
        }
        if signal.is_cancelled() {
            return Ok(NavigationResult {
                cancelled: true,
                aborted: true,
                ..Default::default()
            });
        }
        let (leaf, editor) = match &target.content {
            EntryContent::Message { message } if message["role"] == "user" => (
                target.parent_id.clone(),
                Some(messages::content_text(&message["content"], "")),
            ),
            EntryContent::CustomMessage { content, .. } => (
                target.parent_id.clone(),
                Some(messages::content_text(content, "")),
            ),
            _ => (Some(target_id.into()), None),
        };
        let label = hook.label.or(options.label);
        let summary_entry = {
            let mut manager = lock(&self.inner.manager);
            let before = manager.snapshot().clone();
            let mut preview = preview_manager_from(&before)?;
            let summary_id = if let Some(summary) = summary.filter(|s| !s.summary.is_empty()) {
                let id = preview.branch_with_summary(
                    leaf.as_deref(),
                    summary.summary,
                    summary.details,
                    summary.usage.map(serde_json::to_value).transpose()?,
                )?;
                if let Some(label) = label {
                    preview.set_label(&id, Some(label))?;
                }
                Some(id)
            } else {
                preview.branch(leaf.as_deref())?;
                if let Some(label) = label {
                    preview.set_label(target_id, Some(label))?;
                }
                None
            };
            let mut appended = preview.entries()[before.entries.len()..].to_vec();
            for entry in &mut appended {
                if let EntryContent::BranchSummary {
                    from_hook: origin, ..
                } = &mut entry.content
                {
                    *origin = Some(from_hook);
                }
            }
            let summary_entry =
                summary_id.and_then(|id| appended.iter().find(|entry| entry.id == id).cloned());
            manager.commit(appended, preview.leaf_id().map(str::to_owned))?;
            self.inner
                .navigation_generation
                .fetch_add(1, Ordering::AcqRel);
            summary_entry
        };
        self.restore_selection()?;
        self.refresh()?;
        self.restore_tools()?;
        self.cancel_cache_warming();
        self.emit(SessionEvent::SessionTree {
            old_leaf_id: snapshot.leaf_id,
            new_leaf_id: self.snapshot().leaf_id,
            summary_entry: summary_entry.clone(),
        })
        .await?;
        Ok(NavigationResult {
            editor_text: editor,
            summary_entry,
            ..Default::default()
        })
    }
    pub async fn replace_session(&self, manager: SessionManager) -> Result<bool, SessionError> {
        let _guard = self.idle_guard()?;
        if self
            .inner
            .hooks
            .before_session_change(
                SessionChangeReason::Switch,
                self.snapshot(),
                CancellationToken::new(),
            )
            .await?
        {
            return Ok(false);
        }
        let previous_session_id = self.session_id();
        let projection = manager.projection();
        for value in projection.messages {
            messages::decode(value)?;
        }
        {
            let mut current = lock(&self.inner.manager);
            current.replace_with(manager);
            self.inner
                .navigation_generation
                .fetch_add(1, Ordering::AcqRel);
        }
        self.clear_transient_state();
        self.rebind_cwd()?;
        self.restore_selection()?;
        self.refresh()?;
        self.restore_tools()?;
        self.cancel_cache_warming();
        self.emit(SessionEvent::SessionSwitch {
            previous_session_id,
            session_id: self.session_id(),
        })
        .await?;
        Ok(true)
    }
    pub async fn new_session(
        &self,
        store: Arc<dyn SessionStore>,
        cwd: impl Into<String>,
    ) -> Result<bool, SessionError> {
        let _guard = self.idle_guard()?;
        if self
            .inner
            .hooks
            .before_session_change(
                SessionChangeReason::New,
                self.snapshot(),
                CancellationToken::new(),
            )
            .await?
        {
            return Ok(false);
        }
        let previous_session_id = self.session_id();
        let mut manager = SessionManager::create(store, cwd)?;
        let model = self.model();
        manager.set_model(model.provider.to_string(), model.id.to_string())?;
        manager.set_thinking_level(thinking_name(self.thinking_level()))?;
        {
            let mut current = lock(&self.inner.manager);
            current.replace_with(manager);
            self.inner
                .navigation_generation
                .fetch_add(1, Ordering::AcqRel);
        }
        self.clear_transient_state();
        self.rebind_cwd()?;
        self.refresh()?;
        self.cancel_cache_warming();
        self.emit(SessionEvent::SessionSwitch {
            previous_session_id,
            session_id: self.session_id(),
        })
        .await?;
        self.emit(SessionEvent::SessionStart {
            session_id: self.session_id(),
        })
        .await?;
        Ok(true)
    }
    pub async fn fork(&self, entry_id: &str) -> Result<NavigationResult, SessionError> {
        let _guard = self.idle_guard()?;
        if self
            .inner
            .hooks
            .before_session_change(
                SessionChangeReason::Fork,
                self.snapshot(),
                CancellationToken::new(),
            )
            .await?
        {
            return Ok(NavigationResult {
                cancelled: true,
                ..Default::default()
            });
        }
        let previous_session_id = self.session_id();
        let (manager, editor) = {
            let m = lock(&self.inner.manager);
            let target = m
                .get_entry(entry_id)
                .ok_or_else(|| SessionError::Invalid(format!("Entry {entry_id} not found")))?;
            let editor = if let EntryContent::Message { message } = &target.content {
                if message["role"] == "user" {
                    Some(messages::content_text(&message["content"], ""))
                } else {
                    None
                }
            } else {
                None
            };
            let mut manager = m.fork(entry_id)?;
            if editor.is_some() {
                let parent = manager
                    .get_entry(entry_id)
                    .and_then(|e| e.parent_id.clone());
                manager.branch(parent.as_deref())?;
            }
            (manager, editor)
        };
        {
            let mut current = lock(&self.inner.manager);
            current.replace_with(manager);
            self.inner
                .navigation_generation
                .fetch_add(1, Ordering::AcqRel);
        }
        self.clear_transient_state();
        self.rebind_cwd()?;
        self.restore_selection()?;
        self.refresh()?;
        self.restore_tools()?;
        self.cancel_cache_warming();
        self.emit(SessionEvent::SessionFork {
            previous_session_id,
            session_id: self.session_id(),
        })
        .await?;
        Ok(NavigationResult {
            editor_text: editor,
            ..Default::default()
        })
    }
    fn clear_transient_state(&self) {
        self.cancel_all_inputs();
        self.agent().clear_all_queues();
        let mut state = lock(&self.inner.state);
        state.steering.clear();
        state.follow_up.clear();
        state.pending_custom.clear();
        state.pending_bash.clear();
        state.next_turn.clear();
        state.failed = None;
        state.routed = None;
        state.nested.clear();
        state.nested_roots.clear();
        state.nested_holds.clear();
        state.nested_next.clear();
        state.last_error = None;
        state.retry_attempt = 0;
    }
    fn restore_selection(&self) -> Result<(), SessionError> {
        let p = self.projection();
        let branch = lock(&self.inner.manager).get_branch();
        if let Some((provider, id)) =
            crate::branch_model_selection(&branch, self.inner.models.as_ref())
            && let Some(model) = self.inner.models.get_model(&provider, &id)
        {
            self.agent().set_model(model);
        }
        if let Ok(level) = serde_json::from_value(json!(p.thinking_level)) {
            self.agent()
                .set_thinking_level(pi_ai::clamp_thinking_level(&self.model(), level));
        }
        lock(&self.inner.state).routed = None;
        Ok(())
    }
    fn restore_tools(&self) -> Result<(), SessionError> {
        let messages = self
            .messages()?
            .into_iter()
            .filter_map(|m| m.as_builtin().cloned())
            .collect::<Vec<_>>();
        if !messages
            .iter()
            .any(|message| matches!(message, Message::System(_)))
        {
            return Ok(());
        }
        let names = pi_ai::get_current_tools(&messages)
            .iter()
            .map(|t| t.name.to_string())
            .collect::<Vec<_>>();
        let known = lock(&self.inner.state)
            .tools
            .iter()
            .map(|t| t.tool.declaration.name.to_string())
            .collect::<Vec<_>>();
        self.set_active_tools(
            names
                .iter()
                .filter(|n| known.contains(n))
                .cloned()
                .collect(),
        )?;
        lock(&self.inner.state).pending_tools =
            names.into_iter().filter(|n| !known.contains(n)).collect();
        Ok(())
    }
}
fn thinking_name(level: ModelThinkingLevel) -> String {
    serde_json::to_value(level)
        .expect("thinking")
        .as_str()
        .expect("thinking string")
        .into()
}
fn find_message_entry(
    snapshot: &pi_coding_session::SessionSnapshot,
    message: &Value,
) -> Option<String> {
    snapshot
        .entries
        .iter()
        .rev()
        .find_map(|e| match &e.content {
            EntryContent::Message { message: m } if m == message => Some(e.id.clone()),
            _ => None,
        })
}
fn can_continue(messages: &[AgentMessage], pending: &[AgentMessage]) -> Result<bool, SessionError> {
    let llm = crate::convert_to_llm(messages.to_vec())?;
    Ok(!pending.is_empty()
        || (llm.iter().any(|m| !matches!(m, Message::System(_)))
            && !matches!(llm.last(), Some(Message::Assistant(_)))))
}
fn preview_manager_from(
    snapshot: &pi_coding_session::SessionSnapshot,
) -> Result<SessionManager, SessionError> {
    let store = Arc::new(pi_coding_session::MemoryStore::new());
    store.create(snapshot)?;
    Ok(SessionManager::open(store, &snapshot.header.id)?)
}
fn apply_drafts(
    manager: &mut SessionManager,
    drafts: &[SessionBoundaryDraft],
) -> Result<(), SessionError> {
    for draft in drafts {
        match draft {
            SessionBoundaryDraft::Custom { custom_type, data } => {
                manager.append(EntryContent::Custom {
                    custom_type: custom_type.clone(),
                    data: data.clone(),
                })?;
            }
            SessionBoundaryDraft::CustomMessage {
                custom_type,
                content,
                display,
                details,
            } => {
                if !content.is_string() && !content.is_array() {
                    return Err(SessionError::Invalid(
                        "Invalid custom message content".into(),
                    ));
                }
                manager.append(EntryContent::CustomMessage {
                    custom_type: custom_type.clone(),
                    content: content.clone(),
                    display: *display,
                    details: details.clone(),
                })?;
            }
            SessionBoundaryDraft::ContextEdit {
                target_id,
                replacement,
            } => {
                manager.edit_context(target_id, replacement.clone())?;
            }
            SessionBoundaryDraft::Compaction {
                summary,
                first_kept_entry_id,
                details,
                usage,
            } => {
                let tokens = crate::estimate_projected_context_tokens(
                    &manager.projection(),
                    &manager.get_branch(),
                )
                .tokens;
                let typed = manager
                    .projection()
                    .messages
                    .into_iter()
                    .map(messages::decode)
                    .collect::<Result<Vec<_>, _>>()?;
                let llm = crate::convert_to_llm(typed)?;
                let system = pi_ai::get_current_system_message(&llm)
                    .map(|m| serde_json::to_value(Message::System(m)))
                    .transpose()?;
                manager.append_compaction(pi_coding_session::CompactionOptions {
                    summary: summary.clone(),
                    first_kept_entry_id: first_kept_entry_id.clone(),
                    tokens_before: tokens,
                    details: details.clone(),
                    from_hook: Some(true),
                    usage: usage.as_ref().map(serde_json::to_value).transpose()?,
                    system_message: system,
                })?;
            }
        }
        // Storage accepts detached JSON. Coding admission additionally requires every
        // visible message to satisfy the native Agent/AI contract before publication.
        for message in manager.projection().messages {
            messages::decode(message)?;
        }
    }
    Ok(())
}
pub(crate) async fn sleep_abortable(delay: u64, signal: &CancellationToken) -> bool {
    tokio::select! {_ = signal.cancelled()=>false,_ = tokio::time::sleep(std::time::Duration::from_millis(delay))=>true}
}
