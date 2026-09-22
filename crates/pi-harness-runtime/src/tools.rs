//! Shared phase consumers with actual bindings, durable arguments and batch context.
use crate::{
    effects::{RuntimeEffects, TokenCancellation},
    owner::Telemetry,
};
use pi_agent_schema::*;
use pi_agent_session::storage::RuntimeStorage;
use pi_ai_core::{JsonObject, Presence, SafeUnsigned};
use pi_harness_core::planner::{PlanningFacts, ToolDeclaration};
use std::{collections::BTreeMap, sync::Arc};

fn saved_arguments_match(register: Option<&Register>, key: &str, args: &JsonObject) -> bool {
    matches!(register, Some(Register::OperationToolArguments { key: saved_key, value, .. })
        if saved_key == key && value == args)
}

impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>
    RuntimeEffects<C, S, M>
{
    pub(crate) async fn declarations(
        &self,
        current: &CurrentOperation,
    ) -> Result<BTreeMap<SafeUnsigned, ToolDeclaration>, HarnessRuntimeError> {
        let OperationState::Run(run) = &current.state else {
            return Err(self.lane.owner.fail("tool operation kind invalid"));
        };
        let RunPhase::Tools(tools) = &run.phase else {
            return Ok(BTreeMap::new());
        };
        let entries = self
            .lane
            .owner
            .session
            .get_entries(std::slice::from_ref(&tools.batch.assistant_entry_id))
            .await
            .map_err(|_| self.lane.owner.fail("tool source read failed"))?;
        let Some(Entry::Message(entry)) = entries.get(&tools.batch.assistant_entry_id) else {
            return Err(self.lane.owner.fail("tool source absent"));
        };
        let AgentMessage::BuiltIn(pi_ai_core::Message::Assistant(assistant)) = &entry.message
        else {
            return Err(self.lane.owner.fail("tool source kind invalid"));
        };
        let bindings = self
            .lane
            .owner
            .tools
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        let mut declarations = BTreeMap::new();
        for call in &tools.batch.calls {
            let (index, _) = pi_harness_core::tool_batch::source(call);
            let Some(pi_ai_core::AssistantContent::ToolCall(call)) =
                assistant.content.get(index.get() as usize)
            else {
                return Err(self.lane.owner.fail("tool source index invalid"));
            };
            if let Some(binding) = bindings
                .iter()
                .find(|binding| binding.descriptor().name == call.name)
            {
                let descriptor = binding.descriptor();
                declarations.insert(
                    index,
                    ToolDeclaration {
                        replay: descriptor.replay.unwrap_or_default(),
                        execution_mode: descriptor
                            .execution_mode
                            .unwrap_or(run.settings.tool_execution),
                    },
                );
            }
        }
        Ok(declarations)
    }
    pub(crate) async fn tool_facts(
        &self,
        current: &CurrentOperation,
        session_id: SessionId,
        telemetry_context: Telemetry,
    ) -> Result<PlanningFacts<Telemetry>, HarnessRuntimeError> {
        let declarations = self.declarations(current).await?;
        if let OperationState::Run(run) = &current.state
            && let RunPhase::Tools(tools) = &run.phase
            && let Some(ToolCallState::Planned { source_index, .. }) = tools
                .batch
                .calls
                .iter()
                .find(|call| !matches!(call, ToolCallState::Completed { .. }))
            && !declarations.contains_key(source_index)
            && matches!(run.control, Control::Running(_))
        {
            let call = self
                .source_call(&tools.batch.assistant_entry_id, *source_index)
                .await?;
            return Ok(PlanningFacts::MissingIdentities {
                missing: MissingIdentities {
                    tools: vec![call.name.as_str().into()],
                    models: vec![],
                },
            });
        }
        if matches!(&current.state,OperationState::Run(run) if matches!(run.control,Control::CancelRequested(_)))
        {
            return Ok(PlanningFacts::ToolsCancelled {
                session_id,
                telemetry_context,
                declarations,
                interrupted: pi_agent_loop::tool_error("Tool execution was interrupted"),
                aborted: pi_agent_loop::tool_error("Operation aborted"),
            });
        }
        Ok(PlanningFacts::Tools {
            session_id,
            telemetry_context,
            declarations,
            interrupted: pi_agent_loop::tool_error("Tool execution was interrupted"),
        })
    }
    async fn batch_context(&self, turn: &StepId) -> Result<Arc<C>, HarnessRuntimeError> {
        let _line = self.context_line.acquire().await?;
        if let Some(context) = self
            .contexts
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(turn)
            .cloned()
        {
            return Ok(context);
        }
        let context = self.context().await?;
        let old = self
            .contexts
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(turn.clone(), context.clone());
        drop(old);
        Ok(context)
    }
    async fn source_call(
        &self,
        id: &EntryId,
        index: SafeUnsigned,
    ) -> Result<AgentToolCall, HarnessRuntimeError> {
        let entries = self
            .lane
            .owner
            .session
            .get_entries(std::slice::from_ref(id))
            .await
            .map_err(|_| self.lane.owner.fail("tool source read failed"))?;
        let Some(Entry::Message(entry)) = entries.get(id) else {
            return Err(self.lane.owner.fail("tool source missing"));
        };
        let AgentMessage::BuiltIn(pi_ai_core::Message::Assistant(assistant)) = &entry.message
        else {
            return Err(self.lane.owner.fail("tool source kind invalid"));
        };
        let Some(pi_ai_core::AssistantContent::ToolCall(call)) =
            assistant.content.get(index.get() as usize)
        else {
            return Err(self.lane.owner.fail("tool source index invalid"));
        };
        Ok(call.clone())
    }
    pub(crate) async fn clearance(
        self: &Arc<Self>,
        plan: ToolClearancePlan<Telemetry>,
    ) -> Result<EffectOutput, HarnessRuntimeError> {
        let call = self
            .source_call(&plan.assistant_entry_id, plan.source_index)
            .await?;
        let context = self.batch_context(&plan.turn_id).await?;
        let bindings = self
            .lane
            .owner
            .tools
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        let binding = bindings.into_iter().find(|binding| {
            binding.descriptor().name == call.name
                && plan
                    .producing_configuration
                    .active_tool_names
                    .contains(&call.name)
        });
        let hooks = ToolHooks {
            fx: self.clone(),
            key: plan.key.clone(),
            binding: binding.clone(),
        };
        let output = pi_agent_loop::prepare_tool_call(
            &call,
            binding,
            context.as_ref(),
            &TokenCancellation(self.cancellation.clone()),
            &hooks,
            false,
        )
        .await?;
        let outcome = match output {
            ToolPreparation::Prepared(prepared) => {
                let replay = prepared.binding.descriptor().replay.unwrap_or_default();
                let old = self
                    .bindings
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .insert((plan.turn_id.clone(), plan.source_index), prepared.binding);
                drop(old);
                ToolClearanceOutcome::Prepared {
                    effective_args: prepared.args,
                    replay,
                }
            }
            ToolPreparation::Immediate(output) => ToolClearanceOutcome::Immediate {
                result: output.result,
                is_error: output.is_error,
                terminate: output.terminate,
            },
        };
        Ok(EffectOutput::ToolClearance(
            ToolClearanceOutput::new(&plan, outcome)
                .map_err(|_| self.lane.owner.fail("tool clearance pairing failed"))?,
        ))
    }
    async fn tool_is_current(
        &self,
        plan: &ToolEffectPlan<Telemetry>,
    ) -> Result<bool, HarnessRuntimeError> {
        let _line = self.lane.lane.line.acquire().await?;
        let h = self.lane.load().await?;
        Ok(h.current.as_ref().is_some_and(|current| {
            pi_harness_core::tool_batch::expects_effect(
                current,
                &self.lane.owner.session.metadata().base_metadata().id,
                plan,
            )
        }))
    }
    async fn prepared(
        &self,
        plan: &ToolEffectPlan<Telemetry>,
        check_current: bool,
    ) -> Result<Option<(PreparedToolCall<AgentHarnessToolHandle<C>>, Arc<C>)>, HarnessRuntimeError>
    {
        let call = self
            .source_call(&plan.assistant_entry_id, plan.source_index)
            .await?;
        let turn = StepId::new(plan.key.step_or_task_id.as_str())
            .map_err(|_| self.lane.owner.fail("tool turn identity invalid"))?;
        let context = self.batch_context(&turn).await?;
        let current = self
            .lane
            .owner
            .tools
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        let candidate = self
            .bindings
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&(turn.clone(), plan.source_index))
            .cloned();
        let actual = current
            .into_iter()
            .find(|binding| binding.descriptor().name == call.name);
        let actual = if check_current {
            actual
        } else {
            candidate.clone().or(actual)
        };
        let Some(actual) = actual else {
            return Ok(None);
        };
        let binding = if let Some(candidate) = candidate {
            if check_current && !Arc::ptr_eq(&candidate, &actual) {
                return Ok(None);
            }
            candidate
        } else {
            if actual.descriptor().replay != Some(ToolReplay::Safe) {
                return Ok(None);
            }
            actual
        };
        let register = self
            .lane
            .owner
            .session
            .get_register(RegisterNamespace::OperationToolArguments, &plan.args_key)
            .await
            .map_err(|_| self.lane.owner.fail("saved tool arguments read failed"))?;
        let Some(Register::OperationToolArguments {
            key, value: args, ..
        }) = register
        else {
            if !self.tool_is_current(plan).await? {
                return Ok(None);
            }
            return Err(self.lane.owner.fail("saved tool arguments absent"));
        };
        if key != plan.args_key {
            return Err(self.lane.owner.fail("saved tool arguments key mismatch"));
        }
        Ok(Some((
            PreparedToolCall {
                tool_call: call,
                args,
                binding,
            },
            context,
        )))
    }
    pub(crate) async fn execute_tool(
        self: &Arc<Self>,
        plan: ToolEffectPlan<Telemetry>,
    ) -> Result<EffectOutput, HarnessRuntimeError> {
        let Some((prepared, context)) = self.prepared(&plan, true).await? else {
            return Ok(EffectOutput::ToolRaw(ToolRawOutput {
                key: plan.key,
                result: pi_agent_loop::tool_error("Tool binding is missing or replaced"),
                is_error: true,
            }));
        };
        let replay = {
            let _settings = self.lane.owner.settings_line.acquire().await?;
            let _line = self.lane.lane.line.acquire().await?;
            let h = self.lane.load().await?;
            let expected=h.current.as_ref().is_some_and(|current| {
                pi_harness_core::tool_batch::expects_effect(
                    current,
                    &self.lane.owner.session.metadata().base_metadata().id,
                    &plan,
                ) && matches!(&current.state, OperationState::Run(run) if matches!(run.control, Control::Running(_)))
            });
            if !expected {
                return Ok(EffectOutput::NotStarted { key: plan.key });
            }
            // Telemetry uses the already-validated captured declaration; it
            // does not invoke another application descriptor callback.
            let replay = h
                .current
                .as_ref()
                .and_then(|current| match &current.state {
                    OperationState::Run(run) => match &run.phase {
                        RunPhase::Tools(tools) => {
                            tools.batch.calls.iter().find_map(|call| match call {
                                ToolCallState::EffectPending {
                                    source_index,
                                    replay,
                                    ..
                                } if *source_index == plan.source_index => Some(*replay),
                                _ => None,
                            })
                        }
                        _ => None,
                    },
                    _ => None,
                })
                .expect("validated pending tool");
            let bindings = self
                .lane
                .owner
                .tools
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .clone();
            if !bindings
                .iter()
                .any(|binding| Arc::ptr_eq(binding, &prepared.binding))
            {
                return Ok(EffectOutput::ToolRaw(ToolRawOutput {
                    key: plan.key,
                    result: pi_agent_loop::tool_error("Tool binding is missing or replaced"),
                    is_error: true,
                }));
            }
            let register = self
                .lane
                .owner
                .session
                .get_register(RegisterNamespace::OperationToolArguments, &plan.args_key)
                .await
                .map_err(|_| self.lane.owner.fail("tool start arguments read failed"))?;
            if !saved_arguments_match(register.as_ref(), &plan.args_key, &prepared.args) {
                return Ok(EffectOutput::NotStarted { key: plan.key });
            }
            let turn = StepId::new(plan.key.step_or_task_id.as_str())
                .map_err(|_| self.lane.owner.fail("tool turn identity invalid"))?;
            let old = self
                .bindings
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .insert((turn, plan.source_index), prepared.binding.clone());
            drop(old);
            self.executed_tools
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .insert(plan.key.clone());
            let snapshot = RunningToolSnapshot {
                tool_call_id: prepared.tool_call.id.clone(),
                tool_name: prepared.tool_call.name.clone(),
                args: pi_ai_core::JsonValue::Object(prepared.args.clone()),
                partial_result: None,
            };
            let old = self
                .lane
                .lane
                .running_tools
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .insert(plan.key.clone(), snapshot);
            drop(old);
            self.events().enqueue(
                HarnessEventType::ToolStart,
                &plan.key.lane,
                HarnessEvent::ToolStart(ToolStartEvent {
                    event_type: Default::default(),
                    lane: plan.key.lane.clone(),
                    recovery: self.recovery.then_some(pi_ai_core::LiteralTrue),
                    run_id: plan.key.operation_id.clone(),
                    turn_id: plan.key.step_or_task_id.as_str().into(),
                    tool_call_id: prepared.tool_call.id.clone(),
                    tool_name: prepared.tool_call.name.clone(),
                    args: pi_ai_core::JsonValue::Object(prepared.args.clone()),
                }),
            );
            replay
        };
        self.events().flush();
        let validator = ResultValidator {
            fx: self.as_ref(),
            call: &prepared.tool_call,
        };
        let mut attributes = crate::telemetry::correlation(&plan.key.lane, &plan.key.operation_id);
        attributes.insert(
            "pi.turn.id".into(),
            crate::telemetry::string(plan.key.step_or_task_id.as_str()),
        );
        attributes.insert(
            "pi.tool.name".into(),
            crate::telemetry::string(prepared.tool_call.name.as_str()),
        );
        attributes.insert(
            "pi.tool.call_id".into(),
            crate::telemetry::string(prepared.tool_call.id.as_str()),
        );
        attributes.insert(
            "pi.tool.replay".into(),
            crate::telemetry::string(if replay == ToolReplay::Safe {
                "safe"
            } else {
                "never"
            }),
        );
        attributes.insert(
            "pi.tool.recovery".into(),
            pi_telemetry::AttributeValue::Boolean(self.recovery),
        );
        let span = crate::telemetry::Span::start(
            plan.telemetry_context.clone(),
            "pi.harness.tool",
            attributes,
        );
        let events = ToolEvents {
            fx: self.clone(),
            plan: plan.clone(),
            args: prepared.args.clone(),
            telemetry: span.clone(),
        };
        let output = pi_agent_loop::execute_tool_call(
            &prepared,
            context.as_ref(),
            &TokenCancellation(self.cancellation.clone()),
            &events,
            &validator,
        )
        .await?;
        span.end(
            pi_telemetry::SpanAttributes::from([(
                "pi.tool.is_error".into(),
                pi_telemetry::AttributeValue::Boolean(output.is_error),
            )]),
            output.is_error,
        );
        // The tool may have yielded while another writer finalized the
        // operation or changed its saved effective arguments. Use the register
        // from this same stamped hydration: a separate read could observe an
        // external terminal's args deletion after the pending-state sample.
        let _line = self.lane.lane.line.acquire().await?;
        let h = self.lane.load().await?;
        let expected = h.current.as_ref().is_some_and(|current| {
            pi_harness_core::tool_batch::expects_effect(
                current,
                &self.lane.owner.session.metadata().base_metadata().id,
                &plan,
            )
        });
        if !expected {
            return Ok(EffectOutput::NotStarted { key: plan.key });
        }
        let register = h.registers.get(&(
            RegisterNamespace::OperationToolArguments,
            plan.args_key.clone(),
        ));
        if !saved_arguments_match(register, &plan.args_key, &prepared.args) {
            return Err(self
                .lane
                .owner
                .fail("saved tool arguments changed during execution"));
        }
        Ok(EffectOutput::ToolRaw(ToolRawOutput {
            key: plan.key,
            result: output.result,
            is_error: output.is_error,
        }))
    }
    pub(crate) async fn finalize(
        self: &Arc<Self>,
        plan: ToolEffectPlan<Telemetry>,
        output: ToolRawOutput,
    ) -> Result<ToolSettlementOutput, HarnessRuntimeError> {
        self.permission("finalize_tool").await?;
        if output.key != plan.key {
            return Err(self.lane.owner.fail("tool finalization key mismatch"));
        }
        // The manual gate may outlive a legitimate terminal transaction, which
        // removes saved arguments. Recheck authority before touching those facts.
        let prepared = if self.tool_is_current(&plan).await? {
            self.prepared(&plan, false).await?
        } else {
            None
        };
        let Some((prepared, context)) = prepared else {
            return Ok(ToolSettlementOutput {
                key: plan.key,
                result: output.result,
                is_error: output.is_error,
                terminate: false,
            });
        };
        let hooks = ToolHooks {
            fx: self.clone(),
            key: plan.key.clone(),
            binding: Some(prepared.binding.clone()),
        };
        let validator = ResultValidator {
            fx: self.as_ref(),
            call: &prepared.tool_call,
        };
        let run_after = !self.cancellation.is_cancelled()
            && self
                .executed_tools
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .contains(&plan.key);
        let result = pi_agent_loop::finalize_tool_call(
            &prepared,
            ExecutedToolOutcome {
                result: output.result,
                is_error: output.is_error,
            },
            context.as_ref(),
            &TokenCancellation(self.cancellation.clone()),
            &hooks,
            &validator,
            run_after,
        )
        .await?;
        if self
            .executed_tools
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .contains(&plan.key)
        {
            self.events().enqueue(
                HarnessEventType::ToolEnd,
                &plan.key.lane,
                HarnessEvent::ToolEnd(ToolEndEvent {
                    event_type: Default::default(),
                    lane: plan.key.lane.clone(),
                    recovery: self.recovery.then_some(pi_ai_core::LiteralTrue),
                    run_id: plan.key.operation_id.clone(),
                    turn_id: plan.key.step_or_task_id.as_str().into(),
                    tool_call_id: prepared.tool_call.id.clone(),
                    tool_name: prepared.tool_call.name.clone(),
                    result: result.result.clone(),
                    is_error: result.is_error,
                    terminate: result.terminate,
                }),
            );
            self.events().flush();
        }
        Ok(ToolSettlementOutput {
            key: plan.key,
            result: result.result,
            is_error: result.is_error,
            terminate: result.terminate,
        })
    }
}
struct ToolHooks<C, S: RuntimeStorage, M: SessionMetadataRecord> {
    fx: Arc<RuntimeEffects<C, S, M>>,
    key: EffectKey,
    binding: Option<AgentHarnessToolHandle<C>>,
}
impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>
    pi_agent_loop::ToolPhaseHooks<C> for ToolHooks<C, S, M>
{
    fn before<'a>(
        &'a self,
        call: &'a AgentToolCall,
        args: &'a JsonObject,
        _context: &'a C,
        _cancellation: &'a dyn Cancellation,
    ) -> BoxFuture<'a, Result<Option<BeforeToolResult>, pi_agent_loop::ToolPhaseError>> {
        Box::pin(async move {
            let tool = self.binding.as_ref().map(|binding| {
                let d = binding.descriptor();
                pi_ai_core::Tool {
                    name: d.name.clone(),
                    description: d.description.clone(),
                    parameters: d.parameters.clone(),
                    constrained_sampling: d.constrained_sampling.clone(),
                }
            });
            let result = self
                .fx
                .hook(
                    HookEvent::BeforeTool(BeforeToolEvent {
                        lane: self.key.lane.clone(),
                        run_id: self.key.operation_id.clone(),
                        tool_call_id: call.id.clone(),
                        tool_name: call.name.clone(),
                        args: args.clone(),
                    }),
                    true,
                    tool.as_ref(),
                )
                .await
                .map_err(pi_agent_loop::ToolPhaseError::Runtime)?;
            match result.result() {
                Some(HookResult::BeforeTool(result)) => Ok(result.clone()),
                _ => Err(pi_agent_loop::ToolPhaseError::Runtime(
                    self.fx.lane.owner.fail("invalid before-tool result"),
                )),
            }
        })
    }
    fn after<'a>(
        &'a self,
        call: &'a AgentToolCall,
        args: &'a JsonObject,
        outcome: &'a ExecutedToolOutcome,
        _context: &'a C,
        _cancellation: &'a dyn Cancellation,
    ) -> BoxFuture<'a, Result<Option<AfterToolResult>, pi_agent_loop::ToolPhaseError>> {
        Box::pin(async move {
            let result = self
                .fx
                .hook(
                    HookEvent::AfterTool(AfterToolEvent {
                        lane: self.key.lane.clone(),
                        run_id: self.key.operation_id.clone(),
                        tool_call_id: call.id.clone(),
                        tool_name: call.name.clone(),
                        args: args.clone(),
                        content: outcome.result.content.clone(),
                        details: Presence::Value(outcome.result.details.clone()),
                        is_error: outcome.is_error,
                        usage: outcome.result.usage.clone(),
                    }),
                    true,
                    None,
                )
                .await
                .map_err(pi_agent_loop::ToolPhaseError::Runtime)?;
            match result.result() {
                Some(HookResult::AfterTool(result)) => Ok(result.clone()),
                _ => Err(pi_agent_loop::ToolPhaseError::Runtime(
                    self.fx.lane.owner.fail("invalid after-tool result"),
                )),
            }
        })
    }
}
struct ResultValidator<'a, C, S: RuntimeStorage, M: SessionMetadataRecord> {
    fx: &'a RuntimeEffects<C, S, M>,
    call: &'a AgentToolCall,
}
fn invalid_result() -> ToolExecutionError {
    ToolExecutionError::new(ErrorContext::new("tool result is invalid").expect("static context"))
}
impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>
    pi_agent_loop::ToolResultValidator for ResultValidator<'_, C, S, M>
{
    fn validate(&self, result: &AgentToolResult) -> Result<(), ToolExecutionError> {
        let message = AgentMessage::BuiltIn(pi_ai_core::Message::ToolResult(Box::new(
            pi_ai_core::ToolResultMessage {
                role: Default::default(),
                tool_call_id: self.call.id.clone(),
                tool_name: self.call.name.clone(),
                content: result.content.clone(),
                details: Presence::Value(result.details.clone()),
                usage: result.usage.clone(),
                added_tool_names: result.added_tool_names.clone(),
                is_error: false,
                timestamp: SafeUnsigned::new(self.fx.lane.owner.clock.now_unix_ms())
                    .map_err(|_| invalid_result())?,
            },
        )));
        self.fx
            .lane
            .owner
            .session
            .codec()
            .validate_message(&message)
            .map_err(|_| invalid_result())
    }
    fn validate_usage(&self, usage: &pi_ai_core::Usage) -> Result<(), ToolExecutionError> {
        let bytes = serde_json::to_vec(usage).map_err(|_| invalid_result())?;
        let _: pi_ai_core::Usage = serde_json::from_slice(&bytes).map_err(|_| invalid_result())?;
        Ok(())
    }
}
struct ToolEvents<C, S: RuntimeStorage, M: SessionMetadataRecord> {
    fx: Arc<RuntimeEffects<C, S, M>>,
    telemetry: Telemetry,
    plan: ToolEffectPlan<Telemetry>,
    args: JsonObject,
}
impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord> AgentEventSink
    for ToolEvents<C, S, M>
{
    fn emit(&self, event: AgentEvent) -> BoxFuture<'_, Result<(), HarnessRuntimeError>> {
        Box::pin(async move {
            self.fx.lane.owner.admission.check()?;
            if let AgentEvent::ToolExecutionUpdate {
                tool_call_id,
                tool_name,
                partial_result,
                ..
            } = event
            {
                let line = self.fx.lane.lane.line.acquire().await?;
                let h = self.fx.lane.load().await?;
                let expected = h.current.as_ref().is_some_and(|current| {
                    pi_harness_core::tool_batch::expects_effect(
                        current,
                        &self.fx.lane.owner.session.metadata().base_metadata().id,
                        &self.plan,
                    )
                });
                if !expected {
                    return Ok(());
                }
                let register = h.registers.get(&(
                    RegisterNamespace::OperationToolArguments,
                    self.plan.args_key.clone(),
                ));
                if !saved_arguments_match(register, &self.plan.args_key, &self.args) {
                    return Ok(());
                }
                let previous = {
                    let mut tools = self
                        .fx
                        .lane
                        .lane
                        .running_tools
                        .lock()
                        .unwrap_or_else(|p| p.into_inner());
                    tools.get_mut(&self.plan.key).and_then(|snapshot| {
                        snapshot.partial_result.replace(partial_result.clone())
                    })
                };
                drop(previous);
                self.fx
                    .events()
                    .with_context(self.telemetry.clone())
                    .enqueue(
                        HarnessEventType::ToolUpdate,
                        &self.plan.key.lane,
                        HarnessEvent::ToolUpdate(ToolUpdateEvent {
                            event_type: Default::default(),
                            lane: self.plan.key.lane.clone(),
                            recovery: None,
                            run_id: self.plan.key.operation_id.clone(),
                            turn_id: self.plan.key.step_or_task_id.as_str().into(),
                            tool_call_id,
                            tool_name,
                            partial_result,
                        }),
                    );
                drop(line);
                self.fx.events().flush();
            }
            Ok(())
        })
    }
}

#[cfg(test)]
mod exact_plan_tests {
    use super::*;

    #[test]
    fn saved_arguments_require_register_key_and_value_identity() {
        let args = JsonObject::default();
        let register = Register::OperationToolArguments {
            key: "expected".into(),
            value: args.clone(),
            seq: SafeUnsigned::new(1).unwrap(),
        };
        assert!(saved_arguments_match(Some(&register), "expected", &args));
        assert!(!saved_arguments_match(Some(&register), "other", &args));
        let different = JsonObject::new(BTreeMap::from([(
            "changed".into(),
            pi_ai_core::JsonValue::Bool(true),
        )]));
        assert!(!saved_arguments_match(
            Some(&register),
            "expected",
            &different
        ));
        assert!(!saved_arguments_match(None, "expected", &args));
    }
}
