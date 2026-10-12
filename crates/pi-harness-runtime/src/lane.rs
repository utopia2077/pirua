//! Public lane routing through the shared concrete owner.
#![allow(missing_docs)]
use crate::owner::*;
use pi_agent_schema::*;
use pi_agent_session::storage::RuntimeStorage;
use pi_ai_core::{Model, ToolName};
use std::sync::Arc;
impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord> AgentLane
    for RuntimeLane<C, S, M>
{
    fn name(&self) -> &LaneId {
        &self.lane.name
    }
    fn get_leaf_id(&self) -> BoxFuture<'_, Result<Option<EntryId>, HarnessRuntimeError>> {
        Box::pin(async move {
            let result = async {
                let _line = self.lane.line.acquire().await?;
                let value = self.load().await?;
                let Register::LaneLeaf { value, .. } = value.roots.leaf else {
                    return Err(self.owner.fail("invalid lane leaf"));
                };
                Ok(value)
            }
            .await;
            self.owner.observed_unlocked(result)
        })
    }
    fn get_last_result(
        &self,
    ) -> BoxFuture<'_, Result<Option<LaneLastResult>, HarnessRuntimeError>> {
        Box::pin(async move {
            let result = async {
                self.owner.admission.check()?;
                let result = self
                    .owner
                    .session
                    .get_register(RegisterNamespace::LaneLastResult, self.lane.name.as_str())
                    .await
                    .map_err(|_| self.owner.fail("last result read failed"))?;
                match result {
                    None => Ok(None),
                    Some(Register::LaneLastResult { value, .. }) => Ok(Some(value)),
                    _ => Err(self.owner.fail("invalid last result kind")),
                }
            }
            .await;
            self.owner.observed_unlocked(result)
        })
    }
    fn prompt(&self, input: PromptInput) -> BoxFuture<'_, RunCall> {
        Box::pin(self.prompt_run(input))
    }
    fn skill(
        &self,
        name: String,
        additional_instructions: Option<String>,
    ) -> BoxFuture<'_, RunCall> {
        Box::pin(self.skill_run(name, additional_instructions))
    }
    fn prompt_from_template(&self, name: String, args: Vec<String>) -> BoxFuture<'_, RunCall> {
        Box::pin(self.template_run(name, args))
    }
    fn compact(&self, options: CompactOptions) -> BoxFuture<'_, CompactionCall> {
        Box::pin(self.compact_call(options))
    }
    fn navigate_tree(
        &self,
        target_id: Option<EntryId>,
        options: NavigateOptions,
    ) -> BoxFuture<'_, NavigationCall> {
        Box::pin(self.navigation_call(target_id, options))
    }
    fn resume(&self) -> BoxFuture<'_, ResumeCall> {
        Box::pin(self.resume_run())
    }
    fn abort(&self) -> BoxFuture<'_, AbortCall> {
        Box::pin(self.abort_run())
    }
    fn steer(&self, message: AgentMessage) -> BoxFuture<'_, QueueCall> {
        Box::pin(self.queue_input(message, crate::inbox::QueueKind::Steer))
    }
    fn follow_up(&self, message: AgentMessage) -> BoxFuture<'_, QueueCall> {
        Box::pin(self.queue_input(message, crate::inbox::QueueKind::FollowUp))
    }
    fn next_run(&self, message: AgentMessage) -> BoxFuture<'_, NextRunCall> {
        Box::pin(self.next_input(message))
    }
    fn cancel_queued(&self, entry_id: EntryId) -> BoxFuture<'_, CancelQueuedCall> {
        Box::pin(self.cancel_input(entry_id))
    }
    fn record_usage(&self, request: RecordUsageRequest) -> BoxFuture<'_, RecordUsageCall> {
        Box::pin(self.adjustment(request))
    }
    fn wait_for_idle(&self) -> BoxFuture<'_, Result<(), HarnessRuntimeError>> {
        Box::pin(self.idle())
    }
    fn run_when_idle(&self, task: Box<dyn RunWhenIdleTask>) -> BoxFuture<'_, RunWhenIdleCall> {
        Box::pin(self.when_idle(task))
    }
    fn peek_action(&self) -> BoxFuture<'_, Result<Option<ActionInfo>, HarnessRuntimeError>> {
        Box::pin(async move {
            self.owner.admission.check()?;
            Ok(self.lane.gate.peek())
        })
    }
    fn execute_action(&self) -> BoxFuture<'_, Result<Option<ActionInfo>, HarnessRuntimeError>> {
        Box::pin(async move {
            self.owner.admission.check()?;
            Ok(self.lane.gate.release_next())
        })
    }
    fn run_to_completion(&self) -> BoxFuture<'_, Result<(), HarnessRuntimeError>> {
        Box::pin(self.manual_completion())
    }
    fn get_model(&self) -> BoxFuture<'_, Result<Option<Model>, HarnessRuntimeError>> {
        Box::pin(async move {
            let config = self.read_configuration().await?;
            Ok(self
                .owner
                .models
                .get_model(&config.model.provider, &config.model.model_id))
        })
    }
    fn set_model(&self, model: Model) -> BoxFuture<'_, Result<(), HarnessRuntimeError>> {
        Box::pin(self.update_configuration("model", move |configuration| {
            configuration.model = LaneModel {
                provider: model.provider.clone(),
                model_id: model.id.clone(),
            }
        }))
    }
    fn get_thinking_level(&self) -> BoxFuture<'_, Result<AgentThinkingLevel, HarnessRuntimeError>> {
        Box::pin(async move { Ok(self.read_configuration().await?.thinking_level) })
    }
    fn set_thinking_level(
        &self,
        level: AgentThinkingLevel,
    ) -> BoxFuture<'_, Result<(), HarnessRuntimeError>> {
        Box::pin(
            self.update_configuration("thinkingLevel", move |configuration| {
                configuration.thinking_level = level
            }),
        )
    }
    fn get_active_tools(&self) -> BoxFuture<'_, Result<Vec<ToolName>, HarnessRuntimeError>> {
        Box::pin(async move { Ok(self.read_configuration().await?.active_tool_names) })
    }
    fn set_active_tools(
        &self,
        names: Vec<ToolName>,
    ) -> BoxFuture<'_, Result<(), HarnessRuntimeError>> {
        Box::pin(
            self.update_configuration("activeTools", move |configuration| {
                configuration.active_tool_names = names.clone()
            }),
        )
    }
    fn session(&self) -> Arc<dyn SessionTree> {
        Arc::new(crate::tree::TreeView(self.clone()))
    }
    fn watch(
        &self,
    ) -> BoxFuture<'_, Result<Arc<dyn WatchHandle<LaneSnapshot>>, HarnessRuntimeError>> {
        Box::pin(self.watch_lane())
    }
}
impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord> AgentLane
    for RuntimeHarness<C, S, M>
{
    fn name(&self) -> &LaneId {
        self.main.name()
    }
    fn get_leaf_id(&self) -> BoxFuture<'_, Result<Option<EntryId>, HarnessRuntimeError>> {
        self.main.get_leaf_id()
    }
    fn get_last_result(
        &self,
    ) -> BoxFuture<'_, Result<Option<LaneLastResult>, HarnessRuntimeError>> {
        self.main.get_last_result()
    }
    fn prompt(&self, input: PromptInput) -> BoxFuture<'_, RunCall> {
        self.main.prompt(input)
    }
    fn skill(
        &self,
        name: String,
        additional_instructions: Option<String>,
    ) -> BoxFuture<'_, RunCall> {
        self.main.skill(name, additional_instructions)
    }
    fn prompt_from_template(&self, name: String, args: Vec<String>) -> BoxFuture<'_, RunCall> {
        self.main.prompt_from_template(name, args)
    }
    fn compact(&self, options: CompactOptions) -> BoxFuture<'_, CompactionCall> {
        self.main.compact(options)
    }
    fn navigate_tree(
        &self,
        target_id: Option<EntryId>,
        options: NavigateOptions,
    ) -> BoxFuture<'_, NavigationCall> {
        self.main.navigate_tree(target_id, options)
    }
    fn resume(&self) -> BoxFuture<'_, ResumeCall> {
        self.main.resume()
    }
    fn abort(&self) -> BoxFuture<'_, AbortCall> {
        self.main.abort()
    }
    fn steer(&self, message: AgentMessage) -> BoxFuture<'_, QueueCall> {
        self.main.steer(message)
    }
    fn follow_up(&self, message: AgentMessage) -> BoxFuture<'_, QueueCall> {
        self.main.follow_up(message)
    }
    fn next_run(&self, message: AgentMessage) -> BoxFuture<'_, NextRunCall> {
        self.main.next_run(message)
    }
    fn cancel_queued(&self, entry_id: EntryId) -> BoxFuture<'_, CancelQueuedCall> {
        self.main.cancel_queued(entry_id)
    }
    fn record_usage(&self, request: RecordUsageRequest) -> BoxFuture<'_, RecordUsageCall> {
        self.main.record_usage(request)
    }
    fn wait_for_idle(&self) -> BoxFuture<'_, Result<(), HarnessRuntimeError>> {
        self.main.wait_for_idle()
    }
    fn run_when_idle(&self, task: Box<dyn RunWhenIdleTask>) -> BoxFuture<'_, RunWhenIdleCall> {
        self.main.run_when_idle(task)
    }
    fn peek_action(&self) -> BoxFuture<'_, Result<Option<ActionInfo>, HarnessRuntimeError>> {
        self.main.peek_action()
    }
    fn execute_action(&self) -> BoxFuture<'_, Result<Option<ActionInfo>, HarnessRuntimeError>> {
        self.main.execute_action()
    }
    fn run_to_completion(&self) -> BoxFuture<'_, Result<(), HarnessRuntimeError>> {
        self.main.run_to_completion()
    }
    fn get_model(&self) -> BoxFuture<'_, Result<Option<Model>, HarnessRuntimeError>> {
        self.main.get_model()
    }
    fn set_model(&self, model: Model) -> BoxFuture<'_, Result<(), HarnessRuntimeError>> {
        self.main.set_model(model)
    }
    fn get_thinking_level(&self) -> BoxFuture<'_, Result<AgentThinkingLevel, HarnessRuntimeError>> {
        self.main.get_thinking_level()
    }
    fn set_thinking_level(
        &self,
        level: AgentThinkingLevel,
    ) -> BoxFuture<'_, Result<(), HarnessRuntimeError>> {
        self.main.set_thinking_level(level)
    }
    fn get_active_tools(&self) -> BoxFuture<'_, Result<Vec<ToolName>, HarnessRuntimeError>> {
        self.main.get_active_tools()
    }
    fn set_active_tools(
        &self,
        names: Vec<ToolName>,
    ) -> BoxFuture<'_, Result<(), HarnessRuntimeError>> {
        self.main.set_active_tools(names)
    }
    fn session(&self) -> Arc<dyn SessionTree> {
        self.main.session()
    }
    fn watch(
        &self,
    ) -> BoxFuture<'_, Result<Arc<dyn WatchHandle<LaneSnapshot>>, HarnessRuntimeError>> {
        self.main.watch()
    }
}
impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>
    RuntimeLane<C, S, M>
{
    async fn read_configuration(&self) -> Result<LaneConfiguration, HarnessRuntimeError> {
        let result = async {
            let _line = self.lane.line.acquire().await?;
            let h = self.load().await?;
            let Register::LaneConfig { value, .. } = h.roots.configuration else {
                return Err(self.owner.fail("invalid lane configuration"));
            };
            Ok(value)
        }
        .await;
        self.owner.observed_unlocked(result)
    }
    async fn update_configuration<F: Fn(&mut LaneConfiguration) + Send + Sync + 'static>(
        &self,
        property: &'static str,
        update: F,
    ) -> Result<(), HarnessRuntimeError> {
        let lane = self.clone();
        self.owner
            .clone()
            .retain_runtime(Box::pin(async move {
                let _settings = lane.owner.settings_line.acquire().await?;
                let _line = lane.lane.line.acquire().await?;
                loop {
                    let h = lane.load().await?;
                    let Register::LaneConfig {
                        value: mut configuration,
                        ..
                    } = h.roots.configuration.clone()
                    else {
                        return Err(lane.owner.fail("invalid lane configuration"));
                    };
                    let previous = configuration.clone();
                    update(&mut configuration);
                    let effective = configuration.clone();
                    let permit = lane.owner.admission.try_admit()?;
                    let result = lane
                        .owner
                        .session
                        .commit_conditional_with_telemetry(
                            pi_agent_session::storage::ConditionalCommit {
                                conditions: vec![crate::publication::observed(
                                    &h.roots.configuration,
                                )],
                                transaction: Transaction {
                                    writes: vec![Write::SetRegister(Box::new(
                                        RegisterSet::LaneConfig {
                                            key: lane.lane.name.as_str().into(),
                                            value: configuration,
                                        },
                                    ))],
                                },
                            },
                            lane.owner.telemetry.as_ref(),
                        )
                        .await
                        .map_err(|_| lane.owner.fail("configuration commit failed"))?;
                    drop(permit);
                    if matches!(
                        result,
                        pi_agent_session::storage::ConditionalOutcome::Applied(_)
                    ) {
                        if property == "model" {
                            lane.owner.clear_request_model_snapshot(&lane.lane.name);
                        }
                        let event = match property {
                            "model" => HarnessEvent::ModelConfigUpdate(ModelConfigUpdateEvent {
                                event_type: Default::default(),
                                lane: lane.lane.name.clone(),
                                recovery: None,
                                property: Default::default(),
                                value: effective.model,
                                previous: serde_json::from_value(
                                    serde_json::to_value(previous.model).map_err(|_| {
                                        lane.owner.fail("model event encoding failed")
                                    })?,
                                )
                                .map_err(|_| lane.owner.fail("model event encoding failed"))?,
                            }),
                            "thinkingLevel" => {
                                HarnessEvent::ThinkingConfigUpdate(ThinkingConfigUpdateEvent {
                                    event_type: Default::default(),
                                    lane: lane.lane.name.clone(),
                                    recovery: None,
                                    property: Default::default(),
                                    value: effective.thinking_level,
                                    previous: previous.thinking_level,
                                })
                            }
                            _ => HarnessEvent::ActiveToolsConfigUpdate(
                                ActiveToolsConfigUpdateEvent {
                                    event_type: Default::default(),
                                    lane: lane.lane.name.clone(),
                                    recovery: None,
                                    property: Default::default(),
                                    value: effective.active_tool_names,
                                    previous: previous.active_tool_names,
                                },
                            ),
                        };
                        lane.owner.events.enqueue(
                            HarnessEventType::ConfigUpdate,
                            &lane.lane.name,
                            event,
                        );
                        drop(_line);
                        drop(_settings);
                        lane.owner.events.flush();
                        lane.lane.announce();
                        return Ok(());
                    }
                }
            }))
            .await
    }
    async fn idle(&self) -> Result<(), HarnessRuntimeError> {
        loop {
            self.owner.admission.check()?;
            let changed = self
                .lane
                .changed
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .clone();
            {
                let _line = self.lane.line.acquire().await?;
                if self.load().await?.current.is_none()
                    && !*self.lane.driving.lock().unwrap_or_else(|p| p.into_inner())
                {
                    return Ok(());
                }
            }
            let mut close = Box::pin(self.owner.admission.seal_signal());
            let mut changed = Box::pin(changed.cancelled());
            std::future::poll_fn(|cx| {
                if close.as_mut().poll(cx).is_ready() {
                    return std::task::Poll::Ready(());
                }
                changed.as_mut().poll(cx).map(|_| ())
            })
            .await;
        }
    }
    async fn when_idle(&self, task: Box<dyn RunWhenIdleTask>) -> RunWhenIdleCall {
        let lane = self.clone();
        self.owner
            .clone()
            .retain(Box::pin(async move {
                let callback_permit = loop {
                    lane.idle().await.map_err(RunWhenIdleError::Runtime)?;
                    let _line = lane
                        .lane
                        .line
                        .acquire()
                        .await
                        .map_err(RunWhenIdleError::Runtime)?;
                    let occupied = lane
                        .load()
                        .await
                        .map_err(RunWhenIdleError::Runtime)?
                        .current
                        .is_some();
                    let _activity = lane.lane.activity.lock().unwrap_or_else(|p| p.into_inner());
                    if occupied || *lane.lane.driving.lock().unwrap_or_else(|p| p.into_inner()) {
                        continue;
                    }
                    let reservation =
                        OperationId::new(lane.owner.ids.next(None)).map_err(|_| {
                            RunWhenIdleError::Runtime(
                                lane.owner.fail("idle reservation identity failed"),
                            )
                        })?;
                    *lane
                        .lane
                        .generation
                        .lock()
                        .unwrap_or_else(|p| p.into_inner()) = Arc::new(());
                    *lane.lane.driving.lock().unwrap_or_else(|p| p.into_inner()) = true;
                    *lane
                        .lane
                        .reservation
                        .lock()
                        .unwrap_or_else(|p| p.into_inner()) = Some(reservation);
                    break lane
                        .owner
                        .admission
                        .try_admit()
                        .map_err(RunWhenIdleError::Runtime)?;
                };
                let _callback_permit = callback_permit;
                let _reservation = crate::driver::DriveGuard::new(lane.lane.clone());
                task.run().await.map_err(RunWhenIdleError::Callback)
            }))
            .await
            .map_err(RunWhenIdleError::Runtime)?
    }
    async fn manual_completion(&self) -> Result<(), HarnessRuntimeError> {
        loop {
            self.owner.admission.check()?;
            let changed = self
                .lane
                .changed
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .clone();
            if self.lane.gate.release_next().is_some() {
                continue;
            }
            if !*self.lane.driving.lock().unwrap_or_else(|p| p.into_inner()) {
                return Ok(());
            }
            let mut close = Box::pin(self.owner.admission.seal_signal());
            let mut changed = Box::pin(changed.cancelled());
            std::future::poll_fn(|cx| {
                if close.as_mut().poll(cx).is_ready() {
                    return std::task::Poll::Ready(());
                }
                changed.as_mut().poll(cx).map(|_| ())
            })
            .await;
        }
    }
}
