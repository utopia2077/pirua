//! Concrete effect interpreter; procedures receive detached facts and this catalog.
use crate::owner::*;
use pi_agent_schema::*;
use pi_agent_session::{
    backend_support::{register_key, register_seq},
    storage::RuntimeStorage,
};
use pi_ai_core::{CancellationToken, SafeUnsigned};
use pi_harness_core::planner::*;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
    time::Duration,
};

pub(crate) struct RuntimeEffects<C, S: RuntimeStorage, M: SessionMetadataRecord> {
    pub lane: RuntimeLane<C, S, M>,
    pub recovery: bool,
    pub trace: crate::telemetry::Trace,
    this: std::sync::Weak<Self>,
    pub running: Mutex<BTreeMap<EffectKey, EffectPlan<Telemetry>>>,
    pub started_turns: Mutex<BTreeSet<String>>,
    pub cancellation: CancellationToken,
    pub retired: CancellationToken,
    pub inbox_preparation: Mutex<Option<crate::checkpoint::InboxPreparation>>,
    pub poll_permit: std::sync::atomic::AtomicBool,
    pub cancel_targets: Mutex<BTreeMap<EntryId, crate::poll::CancelTarget>>,
    pub executed_tools: Mutex<BTreeSet<EffectKey>>,
    pub request_bindings: Mutex<BTreeMap<EffectKey, RequestBinding>>,
    pub start_signals: Mutex<BTreeMap<EffectKey, CancellationToken>>,
    pub context_line: crate::scheduler::MutationLine,
    pub contexts: Mutex<BTreeMap<StepId, Arc<C>>>,
    pub bindings: Mutex<BTreeMap<(StepId, SafeUnsigned), AgentHarnessToolHandle<C>>>,
}
impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>
    RuntimeEffects<C, S, M>
{
    pub fn new(lane: RuntimeLane<C, S, M>, recovery: bool) -> Arc<Self> {
        Arc::new_cyclic(|this| Self {
            trace: crate::telemetry::Trace::new(lane.owner.telemetry.clone()),
            recovery,
            this: this.clone(),
            context_line: crate::scheduler::MutationLine::new(lane.owner.admission.clone()),
            contexts: Mutex::new(BTreeMap::new()),
            bindings: Mutex::new(BTreeMap::new()),
            lane,
            running: Mutex::new(BTreeMap::new()),
            started_turns: Mutex::new(BTreeSet::new()),
            start_signals: Mutex::new(BTreeMap::new()),
            request_bindings: Mutex::new(BTreeMap::new()),
            executed_tools: Mutex::new(BTreeSet::new()),
            cancellation: CancellationToken::new(),
            retired: CancellationToken::new(),
            inbox_preparation: Mutex::new(None),
            poll_permit: std::sync::atomic::AtomicBool::new(false),
            cancel_targets: Mutex::new(BTreeMap::new()),
        })
    }
    pub(crate) async fn permission(&self, kind: &str) -> Result<(), HarnessRuntimeError> {
        let mut park = Box::pin(self.lane.lane.gate.park(ActionInfo {
            kind: kind.into(),
            description: kind.into(),
            details: pi_ai_core::Presence::Missing,
        }));
        let mut announced = false;
        let _permission = std::future::poll_fn(|cx| {
            let result = park.as_mut().poll(cx);
            if !announced {
                announced = true;
                self.lane.lane.announce();
            }
            result
        })
        .await?;
        Ok(())
    }
    pub(crate) async fn inputs(
        &self,
        h: &crate::restore::HydratedLane,
    ) -> Result<PlannerInputs<Telemetry>, HarnessRuntimeError> {
        let settings = self
            .lane
            .owner
            .settings
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        let attempts = if settings.retry.enabled {
            settings.retry.max_retries.max_attempts()
        } else {
            1
        };
        let mut loaded: BTreeMap<_, _> = h
            .entries
            .iter()
            .map(|(k, v)| (k.as_str().into(), PlannerLoadedValue::Entry(v.clone())))
            .chain(h.registers.iter().map(|((_, key), value)| {
                (key.clone(), PlannerLoadedValue::Register(value.clone()))
            }))
            .collect();
        if let Some(CurrentOperation {
            state: OperationState::Run(run),
            operation,
            ..
        }) = &h.current
            && let RunPhase::Tools(tools) = &run.phase
        {
            for call in &tools.batch.calls {
                let index = match call {
                    ToolCallState::Planned { source_index, .. }
                    | ToolCallState::EffectPending { source_index, .. }
                    | ToolCallState::Completed { source_index, .. } => source_index,
                };
                let key = format!(
                    "{}:{}:{}",
                    operation.operation_id.as_str(),
                    tools.batch.turn_id.as_str(),
                    index.get()
                );
                if let Some(register) = self
                    .lane
                    .owner
                    .session
                    .get_register(RegisterNamespace::OperationToolArguments, &key)
                    .await
                    .map_err(|_| self.lane.owner.fail("tool cleanup arguments read failed"))?
                {
                    loaded.insert(key, PlannerLoadedValue::Register(register));
                }
            }
        }
        Ok(PlannerInputs {
            running: self
                .running
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .clone(),
            deferred_poll: if self.poll_permit.load(std::sync::atomic::Ordering::Acquire) {
                DeferredPollPermit::One
            } else {
                DeferredPollPermit::None
            },
            deferred_cancellations: BTreeSet::new(),
            loaded,
            runtime: RuntimeSnapshot {
                settings_revision: settings.revision,
                stream_options: settings.stream_options,
                retry_policy: NormalizedRetryPolicy {
                    max_attempts: OneBasedSafeUnsigned::new(attempts)
                        .map_err(|_| self.lane.owner.fail("retry policy range failure"))?,
                    base_delay_ms: settings.retry.base_delay_ms,
                },
            },
            context: None,
            now: SafeUnsigned::new(self.lane.owner.clock.now_unix_ms())
                .map_err(|_| self.lane.owner.fail("runtime clock range failure"))?,
        })
    }
    pub(crate) async fn planning(
        &self,
        h: &crate::restore::HydratedLane,
        step: Option<StepId>,
        now: SafeUnsigned,
    ) -> Result<PlanningFacts<Telemetry>, HarnessRuntimeError> {
        let current = h
            .current
            .as_ref()
            .ok_or_else(|| self.lane.owner.fail("planner operation disappeared"))?;
        let session_id = self
            .lane
            .owner
            .session
            .metadata()
            .base_metadata()
            .id
            .clone();
        let telemetry_context = self.trace.context();
        Ok(
            match required_facts(current).map_err(|_| {
                self.lane
                    .owner
                    .fail("operation requires a later runtime stage")
            })? {
                RequiredFacts::Checkpoint => PlanningFacts::Checkpoint {
                    session_id,
                    telemetry_context,
                    step_id: match step {
                        Some(step) => step,
                        None => StepId::new(self.lane.owner.ids.next(None))
                            .map_err(|_| self.lane.owner.fail("step identity failed"))?,
                    },
                },
                RequiredFacts::BeforeRequest => {
                    let OperationState::Run(run) = &current.state else {
                        return Err(self.lane.owner.fail("invalid request operation"));
                    };
                    let RunPhase::Assistant(AssistantPhase {
                        generation:
                            Generation::Ready {
                                context,
                                next_attempt,
                            },
                        ..
                    }) = &run.phase
                    else {
                        return Err(self.lane.owner.fail("invalid ready request"));
                    };
                    let missing = self.lane.missing(&context.configuration);
                    if !missing.models.is_empty() || !missing.tools.is_empty() {
                        return Ok(PlanningFacts::MissingIdentities { missing });
                    }
                    match self.lane.owner.models.get_model(
                        &context.configuration.model.provider,
                        &context.configuration.model.model_id,
                    ) {
                        Some(model) => PlanningFacts::BeforeRequest {
                            session_id,
                            telemetry_context,
                            event: Box::new(BeforeRequestEvent {
                                lane: current.operation.lane.clone(),
                                run_id: current.operation.operation_id.clone(),
                                model,
                                step: HookRequestStep::Assistant,
                                attempt: *next_attempt,
                                stream_options: context.stream_options.clone(),
                            }),
                        },
                        None => PlanningFacts::MissingIdentities {
                            missing: MissingIdentities {
                                tools: vec![],
                                models: vec![format!(
                                    "{}/{}",
                                    context.configuration.model.provider.as_str(),
                                    context.configuration.model.model_id.as_str()
                                )],
                            },
                        },
                    }
                }
                RequiredFacts::Generation => {
                    let synthetic = match &current.state {
                        OperationState::Run(run) => match &run.phase {
                            RunPhase::Assistant(AssistantPhase {
                                generation:
                                    Generation::EffectPending {
                                        context, attempt, ..
                                    },
                                ..
                            }) if !matches!(run.control, Control::Running(_))
                                || attempt.get() >= context.retry_policy.max_attempts.get() =>
                            {
                                let key = EffectKey {
                                    session_id: session_id.clone(),
                                    lane: current.operation.lane.clone(),
                                    operation_id: current.operation.operation_id.clone(),
                                    kind: EffectKind::Assistant,
                                    step_or_task_id: EffectStepId::new(context.step_id.as_str())
                                        .map_err(|_| {
                                            self.lane.owner.fail("generation step invalid")
                                        })?,
                                    logical_attempt: SafeUnsigned::new(attempt.get()).map_err(
                                        |_| self.lane.owner.fail("generation attempt invalid"),
                                    )?,
                                    item_index: None,
                                };
                                Some(Box::new(self.synthetic_generation(
                                    &key,
                                    context,
                                    now,
                                    !matches!(run.control, Control::Running(_)),
                                )?))
                            }
                            _ => None,
                        },
                        _ => None,
                    };
                    PlanningFacts::Generation {
                        session_id,
                        telemetry_context,
                        synthetic,
                    }
                }
                RequiredFacts::Finish => PlanningFacts::Finish {
                    session_id,
                    telemetry_context,
                    event: BeforeRunEndEvent {
                        lane: current.operation.lane.clone(),
                        run_id: current.operation.operation_id.clone(),
                        messages: self.run_messages(current).await?,
                    },
                },
                RequiredFacts::Terminal => PlanningFacts::Terminal,
                RequiredFacts::DeferredSource => {
                    self.deferred_facts(h, session_id, telemetry_context, now)
                        .await?
                }
                RequiredFacts::Tools => {
                    self.tool_facts(current, session_id, telemetry_context)
                        .await?
                }
            },
        )
    }
    pub(crate) async fn run_messages(
        &self,
        current: &CurrentOperation,
    ) -> Result<Vec<AgentMessage>, HarnessRuntimeError> {
        let mut entries = self
            .lane
            .owner
            .session
            .lane_view(self.lane.lane.name.clone())
            .find_entries_on_branch(BranchScan {
                order: Some(BranchOrder::NewestFirst),
                ..Default::default()
            })
            .await
            .map_err(|_| self.lane.owner.fail("run message read failed"))?;
        let mut messages = Vec::new();
        for entry in entries.drain(..) {
            if current.operation.source_leaf_id.as_ref()
                == Some(&pi_agent_session::backend_support::entry_structure(&entry).id)
            {
                break;
            }
            if let Entry::Message(e) = entry {
                messages.push(e.message);
            }
        }
        messages.reverse();
        Ok(messages)
    }
    async fn transition(
        &self,
        plan: TransitionCommit<Telemetry>,
    ) -> Result<Option<CurrentOperation>, HarnessRuntimeError> {
        self.permission("commit_transition").await?;
        let _settings = self.lane.owner.settings_line.acquire().await?;
        let _line = self.lane.lane.line.acquire().await?;
        let h = self.lane.load().await?;
        let Some(current) = &h.current else {
            return Ok(None);
        };
        if pi_harness_core::inbox::select(current)
            .map_err(|_| self.lane.owner.fail("inbox selection failed"))?
            .is_some()
        {
            let prepared = self
                .inbox_preparation
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .take();
            let Some(prepared) = prepared else {
                return Ok(None);
            };
            if current.operation != prepared.current.operation
                || current.operation_state_seq != prepared.current.operation_state_seq
                || current.lane_state_seq != prepared.current.lane_state_seq
            {
                return Ok(None);
            }
            let inputs = self.inputs(&h).await?;
            let decision = pi_harness_core::transition::transition_recipe(
                current,
                &plan,
                &inputs,
                &self.lane.facts(&h)?,
                &prepared.facts,
            )
            .map_err(|_| self.lane.owner.fail("inbox recipe rejected"))?;
            let result = match decision {
                RecipeDecision::Apply(recipe) => self
                    .lane
                    .apply_prepared(*recipe, prepared.entries)
                    .await?
                    .and_then(|h| h.current),
                _ => None,
            };
            drop(_line);
            drop(_settings);
            self.events().flush();
            self.lane.lane.announce();
            return Ok(result);
        }
        let step = match &plan.next {
            OperationState::Run(run) => match &run.phase {
                RunPhase::Assistant(AssistantPhase {
                    generation: Generation::Ready { context, .. },
                    ..
                }) => Some(context.step_id.clone()),
                _ => None,
            },
            _ => None,
        };
        let inputs = self.inputs(&h).await?;
        let planning = self.planning(&h, step, inputs.now).await?;
        let decision = pi_harness_core::transition::transition_recipe(
            current,
            &plan,
            &inputs,
            &self.lane.facts(&h)?,
            &planning,
        )
        .map_err(|_| self.lane.owner.fail("transition recipe rejected"))?;
        let result = match decision {
            RecipeDecision::Apply(recipe) => {
                self.lane.apply(*recipe).await?.and_then(|h| h.current)
            }
            RecipeDecision::NoLongerExpected => None,
            RecipeDecision::Terminal(_) => {
                return Err(self.lane.owner.fail("unexpected terminal transition"));
            }
        };
        if let Some(next) = &result
            && matches!(&current.state,OperationState::Run(run) if matches!(run.phase,RunPhase::Checkpoint(_)))
        {
            self.turn_start(next);
        }
        drop(_line);
        drop(_settings);
        self.events().flush();
        self.lane.lane.announce();
        Ok(result)
    }
    async fn settlement(
        &self,
        plan: SettlementCommit<Telemetry>,
    ) -> Result<Option<SettlementResult<Telemetry>>, HarnessRuntimeError> {
        self.permission("commit_settlement").await?;
        loop {
            let mut retired_requests = Vec::new();
            let mut settings = Some(self.lane.owner.settings_line.acquire().await?);
            let mut line = Some(self.lane.lane.line.acquire().await?);
            let h = self.lane.load().await?;
            let Some(current) = &h.current else {
                retired_requests.extend(self.retire_effect(plan.plan.key()));
                return Ok(None);
            };
            if current.operation != plan.current.operation
                || matches!(&plan.plan,EffectPlan::Deferred(p) if pi_harness_core::deferred::plan(current,&plan.plan.key().session_id,&plan.telemetry_context).map_or(true,|expected|expected.deferred!=p.deferred || expected.key!=p.key))
            {
                retired_requests.extend(self.retire_effect(plan.plan.key()));
                return Ok(None);
            }
            let inputs = self.inputs(&h).await?;
            if matches!(plan.output, SettlementOutput::NotStarted { .. })
                && (matches!(plan.plan, EffectPlan::Hook(_))
                    || matches!(&current.state,OperationState::Run(run) if matches!(run.control,Control::Running(_))))
            {
                retired_requests.extend(self.retire_effect(plan.plan.key()));
                return Ok(None);
            }
            let facts = self.settlement_facts(&plan, inputs.now, &h).await?;
            let decision =
                settlement_recipe(current, &plan, &inputs, &self.lane.facts(&h)?, &facts)
                    .map_err(|_| self.lane.owner.fail("settlement recipe rejected"))?;
            let result = match decision {
                RecipeDecision::NoLongerExpected => None,
                RecipeDecision::Terminal(terminal) => Some(SettlementResult {
                    terminal: Some(terminal.result),
                    current: terminal.current,
                    dispatch: None,
                    suspend: None,
                    consume_deferred_poll: false,
                }),
                RecipeDecision::Apply(recipe) => {
                    let mut dispatch = recipe.dispatch.clone();
                    let consume_deferred_poll = pi_harness_core::deferred::consumes_poll(&recipe);
                    let lifecycle = self.lane.precommit_messages(
                        &recipe,
                        &current.operation.operation_id,
                        self.recovery,
                    );
                    if !lifecycle.is_empty() {
                        drop(line.take());
                        drop(settings.take());
                        for (kind, event) in lifecycle {
                            self.events().enqueue(kind, &self.lane.lane.name, event);
                        }
                        self.events().flush();
                        settings = Some(self.lane.owner.settings_line.acquire().await?);
                        line = Some(self.lane.lane.line.acquire().await?);
                    }
                    let Some(applied) = self.lane.apply(*recipe).await? else {
                        // Keep the actual output/binding on CAS loss. The next
                        // iteration replans it against newer inbox/control roots.
                        continue;
                    };
                    if consume_deferred_poll {
                        drop(line.take());
                        drop(settings.take());
                        self.poll_permit
                            .store(false, std::sync::atomic::Ordering::Release);
                        if let Some(current) = &applied.current {
                            self.trace.sync(current);
                            self.turn_start(current);
                        }
                        if let Some(EffectPlan::Deferred(plan)) = &mut dispatch {
                            plan.telemetry_context = self.trace.context();
                        }
                    }
                    let assistant = match &plan.output {
                        SettlementOutput::Assistant { message, .. }
                        | SettlementOutput::Deferred { message, .. } => Some(message),
                        SettlementOutput::Reconciled(output) => match output.value() {
                            ReconciledValue::Assistant(message) => Some(message.as_ref()),
                            _ => None,
                        },
                        SettlementOutput::NotStarted { .. } => match &facts {
                            SettlementFacts::NotStarted {
                                value: ReconciledValue::Assistant(message),
                            } => Some(message.as_ref()),
                            _ => None,
                        },
                        _ => None,
                    };
                    if let Some(message) = assistant {
                        let retry = applied.current.as_ref().is_some_and(|c| matches!(&c.state,OperationState::Run(r) if matches!(&r.phase,RunPhase::Assistant(AssistantPhase {generation:Generation::RetryWait {..},..}))));
                        let outcome = if retry {
                            "retry"
                        } else {
                            match message.as_message().stop_reason {
                                pi_ai_core::StopReason::Error => "failed",
                                pi_ai_core::StopReason::Aborted => "aborted",
                                pi_ai_core::StopReason::Deferred => "deferred",
                                _ => "succeeded",
                            }
                        };
                        self.trace.step_outcome(outcome);
                    }
                    self.after_settlement(current, &applied).await?;
                    let candidate = self
                        .request_bindings
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .remove(plan.plan.key());
                    if let Some(candidate) = candidate {
                        if let Some(effect @ (EffectPlan::Assistant(_) | EffectPlan::Deferred(_))) =
                            &dispatch
                        {
                            retired_requests.extend(
                                self.request_bindings
                                    .lock()
                                    .unwrap_or_else(|p| p.into_inner())
                                    .insert(effect.key().clone(), candidate),
                            );
                        } else {
                            retired_requests.push(candidate);
                        }
                    }
                    applied.current.map(|current| SettlementResult {
                        terminal: None,
                        current,
                        dispatch,
                        suspend: None,
                        consume_deferred_poll,
                    })
                }
            };
            retired_requests.extend(self.retire_effect(plan.plan.key()));
            drop(line);
            drop(settings);
            drop(retired_requests);
            self.events().flush();
            self.lane.lane.announce();
            return Ok(result);
        }
    }
    fn retire_effect(&self, key: &EffectKey) -> Option<RequestBinding> {
        let old = self
            .running
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(key);
        drop(old);
        let retired_request = self
            .request_bindings
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(key);
        let old = self
            .lane
            .lane
            .running_tools
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(key);
        drop(old);
        retired_request
    }
    async fn terminal(
        &self,
        plan: TerminalCommit<Telemetry>,
    ) -> Result<Option<CurrentOperation>, HarnessRuntimeError> {
        self.wait_cancellations().await?;
        if self.retired.is_cancelled() {
            return Ok(None);
        }
        self.permission("commit_terminal").await?;
        let _settings = self.lane.owner.settings_line.acquire().await?;
        let _line = self.lane.lane.line.acquire().await?;
        let h = self.lane.load().await?;
        let Some(current) = &h.current else {
            return Ok(None);
        };
        let inputs = self.inputs(&h).await?;
        let prefix = format!("{}:", current.operation.operation_id.as_str());
        let mut registers = Vec::new();
        for namespace in [
            RegisterNamespace::OperationToolArguments,
            RegisterNamespace::OperationPreparation,
        ] {
            for register in self
                .lane
                .owner
                .session
                .list_registers(namespace, Some(&prefix))
                .await
                .map_err(|_| self.lane.owner.fail("terminal cleanup inventory failed"))?
            {
                let key = register_key(&register);
                registers.push(pi_harness_core::planner::RegisterCondition {
                    namespace: key.namespace,
                    key: key.key,
                    seq: Some(register_seq(&register)),
                });
            }
        }
        let mut owned_pending = BTreeSet::new();
        if let OperationState::Run(run) = &current.state {
            owned_pending.extend(
                run.inbox
                    .steer
                    .iter()
                    .chain(&run.inbox.follow_up)
                    .chain(&run.inbox.writes)
                    .map(|id| id.as_str().to_owned()),
            );
            if let Control::CancelRequested(control) = &run.control {
                owned_pending.extend(
                    control
                        .drained_steer
                        .iter()
                        .chain(&control.drained_follow_up)
                        .map(|id| id.as_str().to_owned()),
                );
            }
        }
        for ((namespace, key), register) in &h.registers {
            if *namespace == RegisterNamespace::PendingEntry && owned_pending.contains(key) {
                registers.push(pi_harness_core::planner::RegisterCondition {
                    namespace: *namespace,
                    key: key.clone(),
                    seq: Some(register_seq(register)),
                });
            }
        }
        let decision = pi_harness_core::terminal::terminal_recipe(
            current,
            &plan,
            &inputs,
            &self.lane.facts(&h)?,
            &pi_harness_core::terminal::TerminalFacts { registers },
        )
        .map_err(|_| self.lane.owner.fail("terminal recipe rejected"))?;
        let result = match decision {
            RecipeDecision::Apply(recipe) => {
                if self.lane.apply(*recipe).await?.is_some() {
                    Some(current.clone())
                } else {
                    None
                }
            }
            RecipeDecision::NoLongerExpected => None,
            _ => return Err(self.lane.owner.fail("invalid terminal recipe")),
        };
        drop(_line);
        drop(_settings);
        self.events().flush();
        self.lane.lane.announce();
        Ok(result)
    }
}
impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>
    Effects<Telemetry> for RuntimeEffects<C, S, M>
{
    fn commit_transition(
        &self,
        plan: TransitionCommit<Telemetry>,
    ) -> BoxFuture<'_, Result<Option<CurrentOperation>, HarnessRuntimeError>> {
        let this = self.this.upgrade().expect("live Effects owner");
        Box::pin(async move {
            this.lane
                .owner
                .clone()
                .retain_runtime(Box::pin(async move { this.transition(plan).await }))
                .await
        })
    }
    fn commit_settlement(
        &self,
        plan: SettlementCommit<Telemetry>,
    ) -> BoxFuture<'_, Result<Option<SettlementResult<Telemetry>>, HarnessRuntimeError>> {
        let this = self.this.upgrade().expect("live Effects owner");
        Box::pin(async move {
            this.lane
                .owner
                .clone()
                .retain_runtime(Box::pin(async move { this.settlement(plan).await }))
                .await
        })
    }
    fn commit_terminal(
        &self,
        plan: TerminalCommit<Telemetry>,
    ) -> BoxFuture<'_, Result<Option<CurrentOperation>, HarnessRuntimeError>> {
        let this = self.this.upgrade().expect("live Effects owner");
        Box::pin(async move {
            this.lane
                .owner
                .clone()
                .retain_runtime(Box::pin(async move { this.terminal(plan).await }))
                .await
        })
    }
    fn run(
        &self,
        plan: EffectPlan<Telemetry>,
    ) -> BoxFuture<'_, Result<EffectOutput, HarnessRuntimeError>> {
        let this = self.this.upgrade().expect("live Effects owner");
        Box::pin(async move {
            this.lane
                .owner
                .clone()
                .retain_effect(Box::pin(async move {
                    this.lane.owner.observe(this.execute(plan).await)
                }))
                .await?
        })
    }
    fn finalize_tool(
        &self,
        plan: ToolEffectPlan<Telemetry>,
        output: ToolRawOutput,
    ) -> BoxFuture<'_, Result<ToolSettlementOutput, HarnessRuntimeError>> {
        let this = self.this.upgrade().expect("live Effects owner");
        Box::pin(async move {
            this.lane
                .owner
                .clone()
                .retain_effect(Box::pin(async move {
                    this.lane.owner.observe(this.finalize(plan, output).await)
                }))
                .await?
        })
    }
    fn run_summary_request(
        &self,
        _plan: SummaryRequestPlan<Telemetry>,
    ) -> BoxFuture<'_, Result<SummaryRequestOutput, HarnessRuntimeError>> {
        Box::pin(async move {
            Err(self
                .lane
                .owner
                .fail("summary requests require a later runtime stage"))
        })
    }
    fn settle_summary_request(
        &self,
        _plan: SummaryRequestSettlement<Telemetry>,
    ) -> BoxFuture<'_, Result<Option<CurrentOperation>, HarnessRuntimeError>> {
        Box::pin(async move {
            Err(self
                .lane
                .owner
                .fail("summary settlement requires a later runtime stage"))
        })
    }
    fn sleep(
        &self,
        delay: Duration,
        context: Telemetry,
    ) -> BoxFuture<'_, Result<(), HarnessRuntimeError>> {
        Box::pin(async move {
            if !self.ordinary_permission("sleep").await? {
                return Ok(());
            }
            let operation = self
                .lane
                .lane
                .reservation
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .clone()
                .expect("sleep owns operation");
            let span = crate::telemetry::Span::start(
                context,
                "pi.harness.sleep",
                pi_telemetry::SpanAttributes::from([
                    (
                        "pi.operation.id".into(),
                        crate::telemetry::string(operation.as_str()),
                    ),
                    (
                        "pi.sleep.delay_ms".into(),
                        crate::telemetry::number(delay.as_millis().min(u64::MAX as u128) as u64),
                    ),
                ]),
            );
            span.outcome("pi.sleep.outcome", "aborted", true);
            let mut close = Box::pin(self.lane.owner.admission.seal_signal());
            let mut sleep = self.lane.owner.clock.sleep(delay, &self.cancellation);
            let mut aborted = Box::pin(self.cancellation.cancelled());
            let result = std::future::poll_fn(|cx| {
                if close.as_mut().poll(cx).is_ready() {
                    return std::task::Poll::Ready(Err(self
                        .lane
                        .owner
                        .admission
                        .check()
                        .err()
                        .unwrap_or_else(|| self.lane.owner.fail("timer lost owner"))));
                }
                if aborted.as_mut().poll(cx).is_ready() {
                    return std::task::Poll::Ready(Ok(()));
                }
                sleep.as_mut().poll(cx).map(|result| match result {
                    Ok(()) => Ok(()),
                    Err(_) => {
                        self.lane.owner.admission.check()?;
                        if self.cancellation.is_cancelled() {
                            Ok(())
                        } else {
                            Err(self
                                .lane
                                .owner
                                .fail("operation timer cancelled without cancellation"))
                        }
                    }
                })
            })
            .await;
            span.outcome(
                "pi.sleep.outcome",
                if result.is_ok() && !self.cancellation.is_cancelled() {
                    "elapsed"
                } else {
                    "aborted"
                },
                result.is_err(),
            );
            result
        })
    }
}
pub(crate) struct TokenCancellation(pub CancellationToken);
impl Cancellation for TokenCancellation {
    fn is_cancelled(&self) -> bool {
        self.0.is_cancelled()
    }
    fn cancelled(&self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            self.0.cancelled().await;
        })
    }
}
struct ErrorSink(crate::events::EventHub);
impl crate::hooks::HookErrorSink for ErrorSink {
    fn report(&self, event: LaneHookHandlerErrorEvent) {
        let lane = event.lane.clone();
        self.0.enqueue(
            HarnessEventType::HandlerError,
            &lane,
            HarnessEvent::LaneHookHandlerError(event),
        );
        self.0.flush();
    }
}
impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>
    RuntimeEffects<C, S, M>
{
    pub(crate) async fn context(&self) -> Result<Arc<C>, HarnessRuntimeError> {
        match &self.lane.owner.context {
            ContextSource::Static(context) => Ok(context.clone()),
            ContextSource::Provider(provider) => provider.resolve().await.map(Arc::new),
        }
    }
    pub(crate) async fn system_prompt(
        &self,
        override_prompt: Option<&str>,
    ) -> Result<String, HarnessRuntimeError> {
        if let Some(value) = override_prompt {
            return Ok(value.to_owned());
        }
        match &self.lane.owner.prompt {
            None => Ok(String::new()),
            Some(SystemPromptSource::Static(value)) => Ok(value.clone()),
            Some(SystemPromptSource::Provider(provider)) => {
                provider.resolve(self.context().await?.as_ref()).await
            }
        }
    }
    pub(crate) async fn hook(
        &self,
        event: HookEvent,
        accepted: bool,
        tool: Option<&pi_ai_core::Tool>,
    ) -> Result<crate::hooks::HookPipelineOutput, HarnessRuntimeError> {
        self.hook_with_telemetry(event, accepted, tool, self.trace.context())
            .await
    }
    pub(crate) async fn hook_with_telemetry(
        &self,
        event: HookEvent,
        accepted: bool,
        tool: Option<&pi_ai_core::Tool>,
        telemetry: Telemetry,
    ) -> Result<crate::hooks::HookPipelineOutput, HarnessRuntimeError> {
        if accepted && !matches!(event, HookEvent::BeforeResume(_)) {
            let _ = self.ordinary_permission("hook").await?;
        } else {
            self.permission("hook").await?;
        }
        if accepted {
            let run_id = match &event {
                HookEvent::BeforePayload(e) => Some(&e.run_id),
                HookEvent::AfterResponse(e) => Some(&e.run_id),
                HookEvent::BeforeTool(e) => Some(&e.run_id),
                HookEvent::AfterTool(e) => Some(&e.run_id),
                HookEvent::TransformContext(e) => Some(&e.run_id),
                _ => None,
            };
            if let Some(run_id) = run_id {
                let _line = self.lane.lane.line.acquire().await?;
                let h = self.lane.load().await?;
                // Hydration failures remain faults. A successfully hydrated different
                // operation is only loss of this unstarted callback's authority;
                // the owning drive validates the original operation's lastResult.
                let current = h
                    .current
                    .as_ref()
                    .filter(|current| &current.operation.operation_id == run_id);
                let cancelled = current.is_some_and(|current| matches!(&current.state,OperationState::Run(run) if !matches!(run.control,Control::Running(_))));
                if current.is_none() || cancelled {
                    // Cancellation can invoke stream fallback callbacks: release
                    // the mutation line before cancelling only this old drive.
                    drop(_line);
                    self.cancellation.cancel();
                    let result = match event {
                        HookEvent::BeforePayload(_) => HookResult::BeforePayload(None),
                        HookEvent::AfterResponse(_) => HookResult::AfterResponse(None),
                        HookEvent::BeforeTool(_) => HookResult::BeforeTool(None),
                        // This serialized boundary proves the hook never started. Keep the
                        // raw output, but do not let an earlier run_after=true preserve
                        // its termination request. Started hooks never take this branch.
                        HookEvent::AfterTool(_) => HookResult::AfterTool(Some(AfterToolResult {
                            terminate: Some(false),
                            ..Default::default()
                        })),
                        HookEvent::TransformContext(_) => HookResult::TransformContext(None),
                        HookEvent::BeforeResume(_) => HookResult::BeforeResume,
                        _ => return Err(self.lane.owner.fail("invalid skipped hook family")),
                    };
                    return crate::hooks::HookPipelineOutput::from_result(result.name(), result);
                }
            }
        }
        self.started_hook_with_telemetry(event, accepted, tool, telemetry)
            .await
    }
    async fn started_hook_with_telemetry(
        &self,
        event: HookEvent,
        accepted: bool,
        tool: Option<&pi_ai_core::Tool>,
        telemetry: Telemetry,
    ) -> Result<crate::hooks::HookPipelineOutput, HarnessRuntimeError> {
        self.lane.owner.admission.check()?;
        let saved = match &event {
            HookEvent::BeforeResume(BeforeResumeEvent::Run(e)) => match &e.resume_data {
                pi_ai_core::Presence::Value(pi_ai_core::JsonValue::Object(data)) => data.clone(),
                _ => Default::default(),
            },
            _ => Default::default(),
        };
        let cancellation = TokenCancellation(self.cancellation.clone());
        let codec = self.lane.owner.session.codec();
        let context = crate::hooks::StartedHookContext {
            codec,
            header_policy: codec.header_policy(),
            tool,
            cancellation: &cancellation,
            telemetry: telemetry.as_ref(),
            operation_accepted: accepted,
            recovery: self.recovery,
            resume_data: &saved,
            errors: &ErrorSink(self.events().clone()),
        };
        let captured = self.lane.owner.hooks.capture(event.name());
        let mut invocation = Box::pin(captured.run_started(event, &context));
        let mut close = Box::pin(self.lane.owner.admission.seal_signal());
        std::future::poll_fn(|cx| {
            if close.as_mut().poll(cx).is_ready() {
                return std::task::Poll::Ready(Err(self
                    .lane
                    .owner
                    .admission
                    .check()
                    .err()
                    .unwrap_or_else(|| self.lane.owner.fail("hook lost owner"))));
            }
            invocation.as_mut().poll(cx)
        })
        .await
    }
}
impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>
    RuntimeEffects<C, S, M>
{
    async fn settlement_facts(
        &self,
        commit: &SettlementCommit<Telemetry>,
        now: SafeUnsigned,
        h: &crate::restore::HydratedLane,
    ) -> Result<SettlementFacts, HarnessRuntimeError> {
        Ok(match &commit.output {
            SettlementOutput::RequestOptions(_) => SettlementFacts::Request {
                response_entry_id: EntryId::new(self.lane.owner.ids.next(None))
                    .map_err(|_| self.lane.owner.fail("response identity failed"))?,
                usage_id: UsageId::new(self.lane.owner.ids.next(None))
                    .map_err(|_| self.lane.owner.fail("usage identity failed"))?,
            },
            SettlementOutput::Assistant { message, .. }
            | SettlementOutput::Deferred { message, .. } => {
                let raw = message.as_message();
                let mut followers = Vec::new();
                for (index, content) in raw.content.iter().enumerate() {
                    if matches!(content, pi_ai_core::AssistantContent::ToolCall(_)) {
                        followers.push((
                            SafeUnsigned::new(index as u64).map_err(|_| {
                                self.lane.owner.fail("tool source index range failure")
                            })?,
                            EntryId::new(self.lane.owner.ids.next(Some(raw.timestamp)))
                                .map_err(|_| self.lane.owner.fail("tool result identity failed"))?,
                        ));
                    }
                }
                let window = match &commit.plan {
                    EffectPlan::Assistant(plan) => Some(plan.generation.context_window),
                    _ => None,
                };
                if matches!(commit.plan, EffectPlan::Deferred(_)) {
                    SettlementFacts::Deferred {
                        overflow: pi_ai_core::is_context_overflow(raw, None),
                        followers,
                    }
                } else {
                    SettlementFacts::Assistant {
                        retryable: pi_ai_core::is_retryable_assistant_error(raw),
                        overflow: pi_ai_core::is_context_overflow(raw, window),
                        followers,
                    }
                }
            }
            SettlementOutput::Hook(output) => match output.result() {
                HookResult::BeforeRunEnd(result) => {
                    let follow_up = if let Some(text) =
                        result.as_ref().and_then(|r| r.follow_up.as_ref())
                    {
                        Some(NewMessageEntry {
                            id: EntryId::new(self.lane.owner.ids.next(None))
                                .map_err(|_| self.lane.owner.fail("follow-up identity failed"))?,
                            parent_id: commit.current.leaf_id.clone(),
                            entry_type: MessageEntryType::Message,
                            message: AgentMessage::BuiltIn(pi_ai_core::Message::User(
                                pi_ai_core::UserMessage {
                                    role: Default::default(),
                                    content: pi_ai_core::UserContent::Text(text.clone()),
                                    timestamp: SafeUnsigned::new(
                                        self.lane.owner.clock.now_unix_ms(),
                                    )
                                    .map_err(|_| {
                                        self.lane.owner.fail("follow-up clock range failure")
                                    })?,
                                },
                            )),
                            terminate: None,
                        })
                    } else {
                        None
                    };
                    SettlementFacts::RunEnd { follow_up }
                }
                _ => {
                    return Err(self
                        .lane
                        .owner
                        .fail("hook settlement requires a later runtime stage"));
                }
            },
            SettlementOutput::Tool(output) => SettlementFacts::Tool {
                usage_id: output
                    .result
                    .usage
                    .as_ref()
                    .map(|_| UsageId::new(self.lane.owner.ids.next(None)))
                    .transpose()
                    .map_err(|_| self.lane.owner.fail("tool usage identity failed"))?,
                declarations: self.declarations(&commit.current).await?,
            },
            SettlementOutput::ToolClearance(output) => {
                let usage = match output.outcome() {
                    ToolClearanceOutcome::Immediate { result, .. } => result.usage.as_ref(),
                    _ => None,
                };
                SettlementFacts::Tool {
                    usage_id: usage
                        .map(|_| UsageId::new(self.lane.owner.ids.next(None)))
                        .transpose()
                        .map_err(|_| self.lane.owner.fail("tool usage identity failed"))?,
                    declarations: self.declarations(&commit.current).await?,
                }
            }
            SettlementOutput::Reconciled(output) => match output.value() {
                ReconciledValue::Assistant(_) => SettlementFacts::Assistant {
                    retryable: false,
                    overflow: false,
                    followers: vec![],
                },
                ReconciledValue::Tool { result, .. } => SettlementFacts::Tool {
                    usage_id: result
                        .usage
                        .as_ref()
                        .map(|_| UsageId::new(self.lane.owner.ids.next(None)))
                        .transpose()
                        .map_err(|_| self.lane.owner.fail("tool usage identity failed"))?,
                    declarations: self.declarations(&commit.current).await?,
                },
            },
            SettlementOutput::NotStarted { .. } => match &commit.plan {
                EffectPlan::Tool(_) | EffectPlan::ToolClearance(_) => SettlementFacts::NotStarted {
                    value: ReconciledValue::Tool {
                        result: pi_agent_loop::tool_error("Operation aborted"),
                        is_error: true,
                        terminate: false,
                    },
                },
                EffectPlan::Deferred(plan) => SettlementFacts::NotStarted {
                    value: ReconciledValue::Assistant(Box::new(self.synthetic_deferred(
                        &plan.deferred,
                        now,
                        self.deferred_source_api(h, &plan.deferred.source_entry_id)?,
                    )?)),
                },
                EffectPlan::Assistant(plan) => SettlementFacts::NotStarted {
                    value: ReconciledValue::Assistant(Box::new(self.synthetic_generation(
                        &plan.key,
                        &plan.generation.context,
                        now,
                        true,
                    )?)),
                },
                _ => {
                    return Err(self
                        .lane
                        .owner
                        .fail("cancelled settlement requires a later stage"));
                }
            },
            _ => {
                return Err(self
                    .lane
                    .owner
                    .fail("settlement requires a later runtime stage"));
            }
        })
    }
}
impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>
    RuntimeEffects<C, S, M>
{
    pub(crate) async fn begin(
        &self,
        plan: &EffectPlan<Telemetry>,
    ) -> Result<bool, HarnessRuntimeError> {
        if !self.ordinary_permission("effect_start").await? {
            return Ok(false);
        }
        // Provider model snapshots are host callbacks, so observe them before locks.
        let candidate = if let EffectPlan::Hook(HookEffectPlan {
            event: HookEvent::BeforeRequest(event),
            ..
        }) = plan
        {
            let Some(before) = self
                .lane
                .owner
                .models
                .provider_registration(&event.model.provider)
            else {
                return Ok(false);
            };
            let Some(model) = self
                .lane
                .owner
                .models
                .get_model(&event.model.provider, &event.model.id)
            else {
                return Ok(false);
            };
            let Some(after) = self
                .lane
                .owner
                .models
                .provider_registration(&event.model.provider)
            else {
                return Ok(false);
            };
            if model != event.model || !before.same_registration(&after) {
                return Ok(false);
            }
            Some(RequestBinding {
                model,
                registration: before,
            })
        } else {
            None
        };
        // Declared before guards: even early returns drop these after the lane unlocks.
        let mut observed_registration = None;
        let mut displaced_request = None;
        let _settings = self.lane.owner.settings_line.acquire().await?;
        let _line = self.lane.lane.line.acquire().await?;
        let h = self.lane.load().await?;
        let Some(current) = h.current else {
            return Ok(false);
        };
        let key = plan.key();
        if key.session_id != self.lane.owner.session.metadata().base_metadata().id
            || key.lane != self.lane.lane.name
            || key.operation_id != current.operation.operation_id
        {
            return Ok(false);
        }
        let OperationState::Run(run) = &current.state else {
            return Ok(false);
        };
        if !matches!(run.control, Control::Running(_)) {
            drop(_line);
            drop(_settings);
            self.cancellation.cancel();
            return Ok(false);
        }
        let expected=match (plan,&run.phase) {
            (EffectPlan::Deferred(plan),RunPhase::Deferred(DeferredPhase{deferred,..}))=>PendingDeferred::try_from(deferred.clone()).is_ok_and(|p|p==plan.deferred),
            (EffectPlan::Hook(plan),RunPhase::Deferred(DeferredPhase{deferred,..}))=>matches!(plan.event,HookEvent::BeforeRequest(_)) && match deferred {Deferred::Suspended{step_id,poll,..}=>key.step_or_task_id.as_str()==step_id.as_str() && key.logical_attempt.get()==poll.get().saturating_add(1),Deferred::EffectPending{step_id,poll,..}=>key.step_or_task_id.as_str()==step_id.as_str() && key.logical_attempt.get()==poll.get()},
            (EffectPlan::Assistant(plan),RunPhase::Assistant(AssistantPhase {generation,..}))=>PendingGeneration::try_from(generation.clone()).is_ok_and(|pending|pending==plan.generation),
            (EffectPlan::Hook(plan),RunPhase::Assistant(AssistantPhase {generation:Generation::Ready {context,next_attempt},..}))=>matches!(plan.event,HookEvent::BeforeRequest(_))&&key.step_or_task_id.as_str()==context.step_id.as_str()&&key.logical_attempt.get()==next_attempt.get()&&key.kind==EffectKind::Hook,
            (EffectPlan::Hook(plan),RunPhase::Checkpoint(checkpoint))=>matches!(plan.event,HookEvent::BeforeRunEnd(_))&&key.step_or_task_id.as_str()==checkpoint.trigger_entry_id.as_str()&&matches!(checkpoint.continuation,Continuation::MayFinish {..}),
            (EffectPlan::Tool(plan),RunPhase::Tools(tools))=>plan.assistant_entry_id==tools.batch.assistant_entry_id&&key.step_or_task_id.as_str()==tools.batch.turn_id.as_str()&&tools.batch.calls.iter().any(|call|matches!(call,ToolCallState::EffectPending {source_index,..} if *source_index==plan.source_index))&&plan.args_key==pi_harness_core::tool_batch::args_key(&current,&tools.batch,plan.source_index),
            (EffectPlan::ToolClearance(plan),RunPhase::Tools(tools))=>plan.assistant_entry_id==tools.batch.assistant_entry_id&&plan.turn_id==tools.batch.turn_id&&tools.batch.calls.iter().any(|call|matches!(call,ToolCallState::Planned {source_index,result_entry_id} if *source_index==plan.source_index&&*result_entry_id==plan.result_entry_id)),
            _=>false,
        };
        if !expected {
            return Ok(false);
        }
        if let Some(candidate) = &candidate {
            observed_registration = self
                .lane
                .owner
                .models
                .provider_registration(&candidate.model.provider);
            if observed_registration
                .as_ref()
                .is_none_or(|current| !current.same_registration(&candidate.registration))
            {
                return Ok(false);
            }
            displaced_request = self
                .request_bindings
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .insert(key.clone(), candidate.clone());
        }
        let previous = self
            .running
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(key.clone(), plan.clone());
        if previous.is_some() {
            return Err(self.lane.owner.fail("duplicate actual effect start"));
        }
        drop(previous);
        let signal = self
            .start_signals
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(key);
        drop(_line);
        drop(_settings);
        drop(observed_registration);
        drop(displaced_request);
        if let Some(signal) = signal {
            signal.cancel();
        }
        Ok(true)
    }
    pub(crate) async fn execute(
        self: &Arc<Self>,
        plan: EffectPlan<Telemetry>,
    ) -> Result<EffectOutput, HarnessRuntimeError> {
        if !self.begin(&plan).await? {
            return Ok(EffectOutput::NotStarted {
                key: plan.key().clone(),
            });
        }
        match plan {
            EffectPlan::Hook(plan) => {
                let output = self
                    .started_hook_with_telemetry(
                        plan.event.clone(),
                        true,
                        None,
                        plan.telemetry_context.clone(),
                    )
                    .await?;
                if let Some(options) = output.request_options() {
                    Ok(EffectOutput::RequestOptions(
                        RequestOptionsOutput::new(&plan, options.clone()).map_err(|_| {
                            self.lane.owner.fail("request options receipt mismatch")
                        })?,
                    ))
                } else {
                    Ok(EffectOutput::Hook(
                        HookEffectOutput::new(
                            &plan,
                            output
                                .result()
                                .cloned()
                                .ok_or_else(|| self.lane.owner.fail("hook result missing"))?,
                        )
                        .map_err(|_| self.lane.owner.fail("hook result mismatch"))?,
                    ))
                }
            }
            EffectPlan::Assistant(plan) => self.request(plan).await,
            EffectPlan::Deferred(plan) => self.fetch(plan).await,
            EffectPlan::ToolClearance(plan) => self.clearance(plan).await,
            EffectPlan::Tool(plan) => self.execute_tool(plan).await,
            _ => Err(self
                .lane
                .owner
                .fail("effect requires a later runtime stage")),
        }
    }
}

#[derive(Clone)]
pub(crate) struct RequestBinding {
    pub model: pi_ai_core::Model,
    pub registration: pi_ai_core::ProviderRegistration,
}
impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>
    RuntimeEffects<C, S, M>
{
    pub(crate) fn turn_start(&self, current: &CurrentOperation) {
        let turn = match &current.state {
            OperationState::Run(run) => match &run.phase {
                RunPhase::Assistant(AssistantPhase { generation, .. }) => Some(match generation {
                    Generation::Ready { context, .. }
                    | Generation::EffectPending { context, .. }
                    | Generation::RetryWait { context, .. } => context.step_id.as_str().to_owned(),
                }),
                RunPhase::Tools(tools) => Some(tools.batch.turn_id.as_str().to_owned()),
                RunPhase::Deferred(DeferredPhase {
                    deferred: Deferred::EffectPending { step_id, poll, .. },
                    ..
                }) => Some(format!("{}:poll:{}", step_id.as_str(), poll.get())),
                _ => None,
            },
            _ => None,
        };
        if let Some(turn) = turn {
            self.start_turn(&current.operation.operation_id, turn);
        }
    }
    pub(crate) fn start_turn(&self, operation: &OperationId, turn: String) {
        if !self
            .started_turns
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(turn.clone())
        {
            return;
        }
        self.events().enqueue(
            HarnessEventType::TurnStart,
            &self.lane.lane.name,
            HarnessEvent::TurnStart(TurnStartEvent {
                event_type: Default::default(),
                lane: self.lane.lane.name.clone(),
                recovery: self.recovery.then_some(pi_ai_core::LiteralTrue),
                run_id: operation.clone(),
                turn_id: turn,
            }),
        );
    }
    pub(crate) fn publish_suspension(&self, current: &CurrentOperation, result: &OperationResult) {
        let event = match result {
            OperationResult::Run(outcome) => match outcome.as_ref() {
                RunOutcome::Deferred(outcome) => {
                    Some(HarnessEvent::RunSuspendDeferred(RunSuspendDeferredEvent {
                        event_type: Default::default(),
                        lane: self.lane.lane.name.clone(),
                        recovery: self.recovery.then_some(pi_ai_core::LiteralTrue),
                        run_id: current.operation.operation_id.clone(),
                        reason: Default::default(),
                        deferred: outcome.deferred.clone(),
                    }))
                }
                RunOutcome::MissingIdentities(outcome) => {
                    Some(HarnessEvent::RunSuspendMissing(RunSuspendMissingEvent {
                        event_type: Default::default(),
                        lane: self.lane.lane.name.clone(),
                        recovery: self.recovery.then_some(pi_ai_core::LiteralTrue),
                        run_id: current.operation.operation_id.clone(),
                        reason: Default::default(),
                        missing: outcome.missing.clone(),
                    }))
                }
                _ => None,
            },
            _ => None,
        };
        if let Some(event) = event {
            self.events()
                .enqueue(HarnessEventType::RunSuspend, &self.lane.lane.name, event);
        }
    }
    async fn after_settlement(
        &self,
        previous: &CurrentOperation,
        applied: &crate::restore::HydratedLane,
    ) -> Result<(), HarnessRuntimeError> {
        let Some(current) = &applied.current else {
            return Ok(());
        };
        let (OperationState::Run(before), OperationState::Run(after)) =
            (&previous.state, &current.state)
        else {
            return Ok(());
        };
        if let RunPhase::Assistant(AssistantPhase {
            generation:
                Generation::RetryWait {
                    context,
                    next_attempt,
                    not_before,
                    error_message,
                },
            ..
        }) = &after.phase
        {
            self.events().enqueue(
                HarnessEventType::RetryScheduled,
                &self.lane.lane.name,
                HarnessEvent::RetryScheduled(RetryScheduledEvent {
                    event_type: Default::default(),
                    lane: self.lane.lane.name.clone(),
                    recovery: self.recovery.then_some(pi_ai_core::LiteralTrue),
                    run_id: current.operation.operation_id.clone(),
                    step: context.step_id.as_str().into(),
                    attempt: *next_attempt,
                    max_attempts: context.retry_policy.max_attempts,
                    delay_ms: SafeUnsigned::new(
                        not_before
                            .get()
                            .saturating_sub(self.lane.owner.clock.now_unix_ms()),
                    )
                    .map_err(|_| self.lane.owner.fail("retry delay range failure"))?,
                    error_message: error_message.clone(),
                }),
            );
        }
        if matches!(
            after.phase,
            RunPhase::Assistant(_)
                | RunPhase::Tools(_)
                | RunPhase::Deferred(DeferredPhase {
                    deferred: Deferred::EffectPending { .. },
                    ..
                })
        ) {
            return Ok(());
        }
        let (turn, assistant_id, result_ids) = match &before.phase {
            RunPhase::Assistant(AssistantPhase {
                generation:
                    Generation::EffectPending {
                        context,
                        response_entry_id,
                        ..
                    },
                ..
            }) => (context.step_id.clone(), response_entry_id.clone(), vec![]),
            RunPhase::Deferred(DeferredPhase {
                deferred:
                    Deferred::EffectPending {
                        step_id,
                        poll,
                        response_entry_id,
                        ..
                    },
                ..
            }) => (
                StepId::new(format!("{}:poll:{}", step_id.as_str(), poll.get()))
                    .map_err(|_| self.lane.owner.fail("poll turn invalid"))?,
                response_entry_id.clone(),
                vec![],
            ),
            RunPhase::Tools(tools) => (
                tools.batch.turn_id.clone(),
                tools.batch.assistant_entry_id.clone(),
                tools
                    .batch
                    .calls
                    .iter()
                    .map(|call| pi_harness_core::tool_batch::source(call).1.clone())
                    .collect::<Vec<_>>(),
            ),
            _ => return Ok(()),
        };
        let ids = std::iter::once(assistant_id.clone())
            .chain(result_ids.iter().cloned())
            .collect::<Vec<_>>();
        let entries = self
            .lane
            .owner
            .session
            .get_entries(&ids)
            .await
            .map_err(|_| self.lane.owner.fail("turn closure hydration failed"))?;
        let Some(Entry::Message(entry)) = entries.get(&assistant_id) else {
            return Err(self.lane.owner.fail("turn assistant is absent"));
        };
        let AgentMessage::BuiltIn(pi_ai_core::Message::Assistant(message)) = &entry.message else {
            return Err(self.lane.owner.fail("turn assistant has wrong kind"));
        };
        let message = SettledAssistantMessage::new(*message.clone())
            .map_err(|_| self.lane.owner.fail("turn assistant is unsettled"))?;
        let tool_results = result_ids
            .iter()
            .map(|id| match entries.get(id) {
                Some(Entry::Message(entry)) => match &entry.message {
                    AgentMessage::BuiltIn(pi_ai_core::Message::ToolResult(result)) => {
                        Ok(*result.clone())
                    }
                    _ => Err(self.lane.owner.fail("turn tool result has wrong kind")),
                },
                _ => Err(self.lane.owner.fail("turn tool result absent")),
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.events().enqueue(
            HarnessEventType::TurnEnd,
            &self.lane.lane.name,
            HarnessEvent::TurnEnd(Box::new(TurnEndEvent {
                event_type: Default::default(),
                lane: self.lane.lane.name.clone(),
                recovery: self.recovery.then_some(pi_ai_core::LiteralTrue),
                run_id: current.operation.operation_id.clone(),
                turn_id: turn.as_str().into(),
                message,
                tool_results,
            })),
        );
        Ok(())
    }
}

impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>
    RuntimeEffects<C, S, M>
{
    pub(crate) fn events(&self) -> crate::events::EventHub {
        self.lane.owner.events.with_context(self.trace.context())
    }
}
