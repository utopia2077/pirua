//! Pure operation planning with explicit detached inputs.
#![allow(missing_docs)]
use pi_agent_schema::session_values::*;
use pi_agent_schema::*;
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PlanError {
    MissingInput,
    InvalidInput,
    InvalidCurrent,
    LaterSlice,
    NumberRange,
}
impl std::fmt::Display for PlanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "pure planner {:?}", self)
    }
}
impl std::error::Error for PlanError {}
#[derive(Clone, Debug, PartialEq)]
// Preserve direct matching of the existing Action ABI without another allocation.
#[allow(clippy::large_enum_variant)]
pub enum ProcedureAction<C> {
    Existing(Action<C>),
    Reconcile(Box<SettlementCommit<C>>),
}
#[derive(Clone, Debug, PartialEq)]
pub struct RegisterCondition {
    pub namespace: RegisterNamespace,
    pub key: String,
    pub seq: Option<SafeUnsigned>,
}
#[derive(Clone, Debug, PartialEq)]
pub enum Publication {
    Entry(EntryId),
    Usage(UsageId),
    Terminal(Box<OperationResult>),
}
#[derive(Clone, Debug, PartialEq)]
pub struct CommitRecipe<C> {
    pub expected: Vec<RegisterCondition>,
    pub expected_settings_revision: Option<SafeUnsigned>,
    pub expected_effect: Option<EffectKey>,
    pub transaction: Transaction,
    pub next: Option<OperationState>,
    pub dispatch: Option<EffectPlan<C>>,
    pub result: Option<OperationResult>,
    pub publication: Vec<Publication>,
    pub telemetry_context: C,
}
#[derive(Clone, Debug, PartialEq)]
pub enum RecipeDecision<C> {
    NoLongerExpected,
    Apply(Box<CommitRecipe<C>>),
    Terminal(Box<TerminalCommit<C>>),
}
#[derive(Clone, Debug, PartialEq)]
pub struct CommitFacts {
    pub session_id: SessionId,
    pub leaf_seq: Option<SafeUnsigned>,
    pub meta_seq: SafeUnsigned,
}
#[derive(Clone, Debug, PartialEq)]
pub struct ToolDeclaration {
    pub replay: ToolReplay,
    pub execution_mode: ToolExecutionMode,
}
#[derive(Clone, Debug, PartialEq)]
pub enum PlanningFacts<C> {
    Inbox {
        session_id: SessionId,
        telemetry_context: C,
        placements: Vec<PendingPlacement>,
        projecting: std::collections::BTreeSet<EntryId>,
    },
    Deferred {
        session_id: SessionId,
        telemetry_context: C,
        request: Option<Box<BeforeRequestEvent>>,
        synthetic: Option<Box<SettledAssistantMessage>>,
    },
    Checkpoint {
        session_id: SessionId,
        telemetry_context: C,
        step_id: StepId,
    },
    BeforeRequest {
        session_id: SessionId,
        telemetry_context: C,
        event: Box<BeforeRequestEvent>,
    },
    MissingIdentities {
        missing: MissingIdentities,
    },
    Generation {
        session_id: SessionId,
        telemetry_context: C,
        synthetic: Option<Box<SettledAssistantMessage>>,
    },
    ToolsCancelled {
        session_id: SessionId,
        telemetry_context: C,
        declarations: BTreeMap<SafeUnsigned, ToolDeclaration>,
        interrupted: AgentToolResult,
        aborted: AgentToolResult,
    },
    Tools {
        session_id: SessionId,
        telemetry_context: C,
        declarations: BTreeMap<SafeUnsigned, ToolDeclaration>,
        interrupted: AgentToolResult,
    },
    Finish {
        session_id: SessionId,
        telemetry_context: C,
        event: BeforeRunEndEvent,
    },
    Terminal,
}
#[derive(Clone, Debug, PartialEq)]
pub enum SettlementFacts {
    Deferred {
        overflow: bool,
        followers: Vec<(SafeUnsigned, EntryId)>,
    },
    Request {
        response_entry_id: EntryId,
        usage_id: UsageId,
    },
    Assistant {
        retryable: bool,
        overflow: bool,
        followers: Vec<(SafeUnsigned, EntryId)>,
    },
    Tool {
        usage_id: Option<UsageId>,
        declarations: BTreeMap<SafeUnsigned, ToolDeclaration>,
    },
    NotStarted {
        value: ReconciledValue,
    },
    RunEnd {
        follow_up: Option<NewMessageEntry>,
    },
}
pub(crate) fn number(v: u64) -> Result<SafeUnsigned, PlanError> {
    SafeUnsigned::new(v).map_err(|_| PlanError::NumberRange)
}
pub(crate) fn run(current: &CurrentOperation) -> Result<&RunState, PlanError> {
    match &current.state {
        OperationState::Run(r) => Ok(r),
        OperationState::Compaction(_) | OperationState::Navigation(_) => Err(PlanError::LaterSlice),
    }
}
pub(crate) fn owned(current: &CurrentOperation) -> bool {
    current.lane_state.current_operation_id.as_ref() == Some(&current.operation.operation_id)
}
pub(crate) fn cancelled(r: &RunState) -> bool {
    matches!(r.control, Control::CancelRequested(_))
}
pub(crate) fn inbox_empty(r: &RunState) -> bool {
    r.inbox.steer.is_empty() && r.inbox.follow_up.is_empty() && r.inbox.writes.is_empty()
}
pub(crate) fn key(
    current: &CurrentOperation,
    session: &SessionId,
    kind: EffectKind,
    step: &str,
    attempt: u64,
    index: Option<SafeUnsigned>,
) -> Result<EffectKey, PlanError> {
    Ok(EffectKey {
        session_id: session.clone(),
        lane: current.operation.lane.clone(),
        operation_id: current.operation.operation_id.clone(),
        kind,
        step_or_task_id: EffectStepId::new(step).map_err(|_| PlanError::InvalidCurrent)?,
        logical_attempt: number(attempt)?,
        item_index: index,
    })
}
pub(crate) fn state_write(current: &CurrentOperation, next: OperationState) -> Write {
    Write::SetRegister(Box::new(RegisterSet::OperationState {
        key: current.operation.operation_id.as_str().into(),
        value: Box::new(next),
    }))
}
pub(crate) fn set(register: RegisterSet) -> Write {
    Write::SetRegister(Box::new(register))
}
pub(crate) fn delete(namespace: RegisterNamespace, key: String) -> Write {
    Write::DeleteRegister(RegisterDelete { namespace, key })
}
pub(crate) fn condition(
    namespace: RegisterNamespace,
    key: &str,
    seq: Option<SafeUnsigned>,
) -> RegisterCondition {
    RegisterCondition {
        namespace,
        key: key.into(),
        seq,
    }
}
/// Preserve the first predicate's position while rejecting contradictory facts.
pub(crate) fn add_condition(
    expected: &mut Vec<RegisterCondition>,
    candidate: RegisterCondition,
) -> Result<(), PlanError> {
    if let Some(existing) = expected
        .iter()
        .find(|existing| existing.namespace == candidate.namespace && existing.key == candidate.key)
    {
        if existing.seq != candidate.seq {
            return Err(PlanError::InvalidInput);
        }
    } else {
        expected.push(candidate);
    }
    Ok(())
}

/// Cleanup inputs include a point read for every deterministic batch-args key.
/// Missing map entries therefore mean observed absence, never omitted hydration.
pub(crate) fn tool_arguments_condition<C>(
    inputs: &PlannerInputs<C>,
    key: &str,
) -> Result<RegisterCondition, PlanError> {
    let seq = match inputs.loaded.get(key) {
        None => None,
        Some(PlannerLoadedValue::Register(Register::OperationToolArguments {
            key: loaded_key,
            seq,
            ..
        })) if loaded_key == key => Some(*seq),
        Some(_) => return Err(PlanError::InvalidInput),
    };
    Ok(condition(
        RegisterNamespace::OperationToolArguments,
        key,
        seq,
    ))
}

pub(crate) fn recipe<C: Clone>(
    current: &CurrentOperation,
    facts: &CommitFacts,
    context: &C,
) -> CommitRecipe<C> {
    CommitRecipe {
        expected: vec![
            condition(
                RegisterNamespace::OperationState,
                current.operation.operation_id.as_str(),
                Some(current.operation_state_seq),
            ),
            condition(
                RegisterNamespace::OperationMeta,
                current.operation.operation_id.as_str(),
                Some(facts.meta_seq),
            ),
            condition(
                RegisterNamespace::LaneState,
                current.operation.lane.as_str(),
                Some(current.lane_state_seq),
            ),
            condition(
                RegisterNamespace::LaneLeaf,
                current.operation.lane.as_str(),
                facts.leaf_seq,
            ),
        ],
        expected_settings_revision: None,
        expected_effect: None,
        transaction: Transaction { writes: vec![] },
        next: None,
        dispatch: None,
        result: None,
        publication: vec![],
        telemetry_context: context.clone(),
    }
}
pub(crate) fn transition<C: Clone>(
    current: &CurrentOperation,
    phase: RunPhase,
    context: &C,
) -> ProcedureAction<C> {
    let mut r = match &current.state {
        OperationState::Run(r) => r.clone(),
        _ => unreachable!("caller checked run"),
    };
    r.phase = phase;
    ProcedureAction::Existing(Action::Transition(TransitionPlan {
        next: OperationState::Run(r),
        telemetry_context: context.clone(),
        expected_configuration_seq: None,
        expected_settings_revision: None,
    }))
}
pub(crate) fn dispatch<C>(effect: EffectPlan<C>) -> ProcedureAction<C> {
    ProcedureAction::Existing(Action::Dispatch(Box::new(DispatchPlan {
        intent: None,
        effect,
        consume_deferred_poll: false,
    })))
}
pub(crate) fn plan_matches<C>(expected: &EffectPlan<C>, actual: &EffectPlan<C>) -> bool {
    if expected.key() != actual.key() {
        return false;
    }
    match (expected, actual) {
        (EffectPlan::Assistant(a), EffectPlan::Assistant(b)) => a.generation == b.generation,
        (EffectPlan::Deferred(a), EffectPlan::Deferred(b)) => a.deferred == b.deferred,
        (EffectPlan::Tool(a), EffectPlan::Tool(b)) => {
            a.assistant_entry_id == b.assistant_entry_id
                && a.source_index == b.source_index
                && a.args_key == b.args_key
        }
        (EffectPlan::ToolClearance(a), EffectPlan::ToolClearance(b)) => {
            a.assistant_entry_id == b.assistant_entry_id
                && a.source_index == b.source_index
                && a.turn_id == b.turn_id
                && a.result_entry_id == b.result_entry_id
                && a.producing_configuration == b.producing_configuration
        }
        (EffectPlan::Hook(a), EffectPlan::Hook(b)) => a.event == b.event,
        _ => false,
    }
}
pub(crate) fn dispatch_or_await<C>(
    inputs: &PlannerInputs<C>,
    effect: EffectPlan<C>,
) -> Result<ProcedureAction<C>, PlanError> {
    if let Some(live) = inputs.running.get(effect.key()) {
        if !plan_matches(&effect, live) {
            return Err(PlanError::InvalidInput);
        }
        Ok(ProcedureAction::Existing(Action::AwaitEffect(
            effect.key().clone(),
        )))
    } else {
        Ok(dispatch(effect))
    }
}
pub(crate) fn checkpoint(trigger: EntryId, continuation: Continuation) -> RunPhase {
    RunPhase::Checkpoint(CheckpointPhase {
        kind: Default::default(),
        continuation,
        trigger_entry_id: trigger,
        threshold_checked_trigger_entry_id: None,
        skip_inbox_once: None,
    })
}
pub(crate) fn assistant_phase(generation: Generation) -> RunPhase {
    RunPhase::Assistant(AssistantPhase {
        kind: Default::default(),
        generation,
    })
}

pub fn next_action<C: Clone>(
    current: &CurrentOperation,
    inputs: &PlannerInputs<C>,
    facts: &PlanningFacts<C>,
) -> Result<ProcedureAction<C>, PlanError> {
    if !owned(current) {
        return Err(PlanError::InvalidCurrent);
    }
    let r = run(current)?;
    if crate::inbox::select(current)?.is_some() {
        return crate::inbox::next(current, facts);
    }
    if let PlanningFacts::MissingIdentities { missing } = facts {
        let safe = match &r.phase {
            RunPhase::Assistant(AssistantPhase {
                generation: Generation::Ready { .. },
                ..
            }) => true,
            RunPhase::Deferred(p) => inputs.deferred_poll == DeferredPollPermit::One
                && !inputs.running.values().any(|effect| matches!(effect, EffectPlan::Deferred(live)
                    if PendingDeferred::try_from(p.deferred.clone()).is_ok_and(|pending| pending == live.deferred))),
            RunPhase::Tools(p) => p
                .batch
                .calls
                .iter()
                .find(|c| !matches!(c, ToolCallState::Completed { .. }))
                .is_some_and(|c| matches!(c, ToolCallState::Planned { .. })),
            _ => false,
        };
        if safe && !cancelled(r) {
            return Ok(ProcedureAction::Existing(Action::Suspend(
                OperationResult::Run(Box::new(RunOutcome::MissingIdentities(
                    RunMissingIdentities {
                        kind: Default::default(),
                        reason: Default::default(),
                        leaf_id: current.leaf_id.clone().ok_or(PlanError::InvalidCurrent)?,
                        missing: missing.clone(),
                    },
                ))),
            )));
        }
    }

    match &r.phase {
        RunPhase::Checkpoint(p) => {
            if cancelled(r) {
                if !r.inbox.writes.is_empty() {
                    return Err(PlanError::LaterSlice);
                }
                return Ok(ProcedureAction::Existing(Action::Finish(
                    crate::terminal::operation_result(current, inputs)?,
                )));
            }
            match &p.continuation {
                Continuation::NeedAssistant {
                    overflow_recovery_used,
                } => {
                    let PlanningFacts::Checkpoint {
                        telemetry_context,
                        step_id,
                        ..
                    } = facts
                    else {
                        return Err(PlanError::MissingInput);
                    };
                    let g = Generation::Ready {
                        context: GenerationContext {
                            step_id: step_id.clone(),
                            trigger_entry_id: p.trigger_entry_id.clone(),
                            configuration: current.configuration.clone(),
                            stream_options: inputs.runtime.stream_options.clone(),
                            retry_policy: inputs.runtime.retry_policy.clone(),
                            overflow_recovery_used: *overflow_recovery_used,
                        },
                        next_attempt: OneBasedSafeUnsigned::new(1)
                            .map_err(|_| PlanError::NumberRange)?,
                    };
                    let mut action = transition(current, assistant_phase(g), telemetry_context);
                    if let ProcedureAction::Existing(Action::Transition(t)) = &mut action {
                        t.expected_configuration_seq = Some(current.configuration_seq);
                        t.expected_settings_revision = Some(inputs.runtime.settings_revision)
                    }
                    Ok(action)
                }
                Continuation::MayFinish { .. } => {
                    if !inbox_empty(r) {
                        return Err(PlanError::LaterSlice);
                    }
                    let PlanningFacts::Finish {
                        session_id,
                        telemetry_context,
                        event,
                    } = facts
                    else {
                        return Err(PlanError::MissingInput);
                    };
                    if event.lane != current.operation.lane
                        || event.run_id != current.operation.operation_id
                    {
                        return Err(PlanError::InvalidInput);
                    };
                    dispatch_or_await(
                        inputs,
                        EffectPlan::Hook(HookEffectPlan {
                            key: key(
                                current,
                                session_id,
                                EffectKind::Hook,
                                p.trigger_entry_id.as_str(),
                                0,
                                None,
                            )?,
                            event: HookEvent::BeforeRunEnd(event.clone()),
                            telemetry_context: telemetry_context.clone(),
                        }),
                    )
                }
            }
        }
        RunPhase::Assistant(p) => crate::generation::next(current, inputs, facts, &p.generation),
        RunPhase::Tools(p) => crate::tool_batch::next(current, inputs, facts, &p.batch),
        RunPhase::FailureDrain(_) => {
            if !inbox_empty(r) {
                return Err(PlanError::LaterSlice);
            }
            Ok(ProcedureAction::Existing(Action::Finish(
                crate::terminal::operation_result(current, inputs)?,
            )))
        }
        RunPhase::Deferred(_) => crate::deferred::next(current, inputs, facts),
        RunPhase::Compaction(_) => Err(PlanError::LaterSlice),
    }
}
pub fn settlement_recipe<C: Clone>(
    current: &CurrentOperation,
    commit: &SettlementCommit<C>,
    inputs: &PlannerInputs<C>,
    facts: &CommitFacts,
    output_facts: &SettlementFacts,
) -> Result<RecipeDecision<C>, PlanError> {
    if !owned(current)
        || commit.current.operation != current.operation
        || commit.plan.key().session_id != facts.session_id
        || commit.plan.key().lane != current.operation.lane
        || commit.plan.key().operation_id != current.operation.operation_id
    {
        return Ok(RecipeDecision::NoLongerExpected);
    }
    if let SettlementOutput::NotStarted { key: output_key } = &commit.output {
        if output_key != commit.plan.key() || !cancelled(run(current)?) {
            return Ok(RecipeDecision::NoLongerExpected);
        }
        if matches!(commit.plan, EffectPlan::Hook(_)) {
            return Ok(RecipeDecision::NoLongerExpected);
        }
        let SettlementFacts::NotStarted { value } = output_facts else {
            return Err(PlanError::MissingInput);
        };
        let output = ReconciledOutput::new(
            &commit.plan,
            ReconcileReason::CancelledNotStarted,
            value.clone(),
        )
        .map_err(|_| PlanError::InvalidInput)?;
        let extra = match value {
            ReconciledValue::Assistant(_) => SettlementFacts::Assistant {
                retryable: false,
                overflow: false,
                followers: vec![],
            },
            ReconciledValue::Tool { .. } => SettlementFacts::Tool {
                usage_id: None,
                declarations: BTreeMap::new(),
            },
        };
        let normalized = SettlementCommit {
            current: commit.current.clone(),
            plan: commit.plan.clone(),
            output: SettlementOutput::Reconciled(output),
            telemetry_context: commit.telemetry_context.clone(),
        };
        return settlement_recipe(current, &normalized, inputs, facts, &extra);
    }
    match &commit.plan {
        EffectPlan::Assistant(_) => {
            crate::generation::settle(current, commit, inputs, facts, output_facts)
        }
        EffectPlan::Hook(h) => match &h.event {
            HookEvent::BeforeRequest(_) => {
                if matches!(run(current)?.phase, RunPhase::Deferred(_)) {
                    crate::deferred::intent(current, commit, inputs, facts, output_facts)
                } else {
                    crate::generation::intent(current, commit, facts, output_facts)
                }
            }
            HookEvent::BeforeRunEnd(_) => {
                crate::terminal::settle_hook(current, commit, inputs, facts, output_facts)
            }
            _ => Err(PlanError::LaterSlice),
        },
        EffectPlan::Tool(_) | EffectPlan::ToolClearance(_) => {
            crate::tool_batch::settle(current, commit, inputs, facts, output_facts)
        }
        EffectPlan::Deferred(_) => {
            crate::deferred::settle(current, commit, inputs, facts, output_facts)
        }
        EffectPlan::CancelDeferred(_) => Ok(RecipeDecision::NoLongerExpected),
        EffectPlan::Summary(_) => Err(PlanError::LaterSlice),
    }
}

/// Finite phase inventory for runtime fact materialization. Later slices are explicit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequiredFacts {
    Checkpoint,
    BeforeRequest,
    Generation,
    Tools,
    Finish,
    Terminal,
    DeferredSource,
}
pub fn required_facts(current: &CurrentOperation) -> Result<RequiredFacts, PlanError> {
    let r = run(current)?;
    Ok(match &r.phase {
        RunPhase::Checkpoint(p) => {
            if cancelled(r) {
                RequiredFacts::Terminal
            } else {
                match p.continuation {
                    Continuation::NeedAssistant { .. } => RequiredFacts::Checkpoint,
                    Continuation::MayFinish { .. } => RequiredFacts::Finish,
                }
            }
        }
        RunPhase::Assistant(p) => match p.generation {
            Generation::Ready { .. } if !cancelled(r) => RequiredFacts::BeforeRequest,
            Generation::Ready { .. } | Generation::RetryWait { .. } if cancelled(r) => {
                RequiredFacts::Terminal
            }
            _ => RequiredFacts::Generation,
        },
        RunPhase::Tools(_) => RequiredFacts::Tools,
        RunPhase::FailureDrain(_) => RequiredFacts::Terminal,
        RunPhase::Deferred(_) => RequiredFacts::DeferredSource,
        RunPhase::Compaction(_) => return Err(PlanError::LaterSlice),
    })
}

#[cfg(test)]
mod condition_tests {
    use super::*;

    #[test]
    fn repeated_register_facts_merge_only_when_the_expectations_agree() {
        let one = number(1).unwrap();
        let two = number(2).unwrap();
        for first in [None, Some(one), Some(two)] {
            for second in [None, Some(one), Some(two)] {
                let mut expected = vec![condition(
                    RegisterNamespace::OperationToolArguments,
                    "op:step:1",
                    first,
                )];
                let original = expected.clone();
                let result = add_condition(
                    &mut expected,
                    condition(
                        RegisterNamespace::OperationToolArguments,
                        "op:step:1",
                        second,
                    ),
                );
                if first == second {
                    assert_eq!(result, Ok(()));
                } else {
                    assert_eq!(result, Err(PlanError::InvalidInput));
                }
                assert_eq!(
                    expected, original,
                    "duplicate/conflict must not replace prior facts"
                );
            }
        }
        let mut expected = vec![condition(
            RegisterNamespace::OperationToolArguments,
            "same",
            Some(one),
        )];
        add_condition(
            &mut expected,
            condition(RegisterNamespace::OperationPreparation, "same", None),
        )
        .unwrap();
        add_condition(
            &mut expected,
            condition(RegisterNamespace::OperationToolArguments, "other", None),
        )
        .unwrap();
        assert_eq!(
            expected.len(),
            3,
            "identity includes both namespace and key"
        );
    }
}
