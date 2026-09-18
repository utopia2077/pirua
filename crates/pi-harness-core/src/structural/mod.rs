//! Shared pure structural ownership, nested requests, and frozen preparation.
use crate::planner::*;
use pi_agent_schema::session_values::*;
use pi_agent_schema::*;

#[derive(Clone, Debug, PartialEq)]
/// Detached identity and runtime context for structural planning.
pub struct Planning<C> {
    /// Owning Session identity.
    pub session_id: SessionId,
    /// Opaque process-local observation context.
    pub telemetry_context: C,
    /// Reserved only when a deciding hook selects generation.
    pub result_entry_id: Option<EntryId>,
}
#[derive(Clone, Debug, Default, PartialEq)]
/// Caller-supplied reservation and exact prepared result facts.
pub struct Settlement {
    /// Optional fresh structural result reservation.
    pub result_entry_id: Option<EntryId>,
    /// Backend-prepared full row; never inferred from state counters.
    pub prepared_entry: Option<Entry>,
    /// Commit-minted hook usage, never a generated-attempt aggregate row.
    pub hook_usage_id: Option<UsageId>,
}
/// Borrow cancellation control without changing the owning operation kind.
pub fn control(state: &OperationState) -> &Control {
    match state {
        OperationState::Run(r) => &r.control,
        OperationState::Compaction(c) => &c.control,
        OperationState::Navigation(NavigationState::Summarized(n)) => &n.control,
        OperationState::Navigation(NavigationState::Unsummarized(n)) => &n.control,
    }
}
/// Whether cancellation is already durable for this operation.
pub fn is_cancelled(current: &CurrentOperation) -> bool {
    matches!(control(&current.state), Control::CancelRequested(_))
}
/// Borrow the shared structural decision from any owning operation.
pub fn decision(current: &CurrentOperation) -> Option<&StructuralDecision> {
    match &current.state {
        OperationState::Compaction(c) => Some(&c.structural),
        OperationState::Run(r) => match &r.phase {
            RunPhase::Compaction(c) => Some(&c.structural),
            _ => None,
        },
        OperationState::Navigation(NavigationState::Summarized(n)) => Some(&n.phase.structural),
        _ => None,
    }
}
/// Replace only the structural decision, preserving control and continuation.
pub fn replace(
    current: &CurrentOperation,
    value: StructuralDecision,
) -> Result<OperationState, PlanError> {
    let mut state = current.state.clone();
    match &mut state {
        OperationState::Compaction(c) => c.structural = value,
        OperationState::Run(r) => match &mut r.phase {
            RunPhase::Compaction(c) => c.structural = value,
            _ => return Err(PlanError::InvalidCurrent),
        },
        OperationState::Navigation(NavigationState::Summarized(n)) => n.phase.structural = value,
        _ => return Err(PlanError::InvalidCurrent),
    }
    Ok(state)
}
/// Return the current structural task identity.
pub fn task(current: &CurrentOperation) -> Result<&TaskId, PlanError> {
    match decision(current).ok_or(PlanError::InvalidCurrent)? {
        StructuralDecision::Deciding { task_id }
        | StructuralDecision::Generating { task_id, .. } => Ok(task_id),
    }
}
/// Project only an effect-pending summary generation.
pub fn pending(current: &CurrentOperation) -> Option<PendingSummaryGeneration> {
    match decision(current)? {
        StructuralDecision::Generating { generation, .. } => {
            PendingSummaryGeneration::try_from(generation.as_ref().clone()).ok()
        }
        _ => None,
    }
}
/// Bind the current pending attempt to its complete outer effect identity.
pub fn plan<C: Clone>(
    current: &CurrentOperation,
    session: &SessionId,
    telemetry: &C,
) -> Result<SummaryEffectPlan<C>, PlanError> {
    let generation = pending(current).ok_or(PlanError::InvalidCurrent)?;
    Ok(SummaryEffectPlan {
        key: key(
            current,
            session,
            EffectKind::Summary,
            generation.context.task_id.as_str(),
            generation.attempt.get(),
            None,
        )?,
        generation,
        telemetry_context: telemetry.clone(),
    })
}
/// Mutable nested fields and op.state sequence do not change outer attempt identity.
pub fn matches<C>(
    current: &CurrentOperation,
    outer: &SummaryEffectPlan<C>,
    session: &SessionId,
) -> bool {
    owned(current)
        && outer.key.session_id == *session
        && outer.key.operation_id == current.operation.operation_id
        && outer.key.lane == current.operation.lane
        && outer.key.kind == EffectKind::Summary
        && outer.key.item_index.is_none()
        && pending(current).is_some_and(|p| {
            p.context == outer.generation.context
                && p.attempt == outer.generation.attempt
                && outer.key.step_or_task_id.as_str() == p.context.task_id.as_str()
                && outer.key.logical_attempt.get() == p.attempt.get()
        })
}
/// Return the deterministic operation/task preparation register key.
pub fn preparation_key(current: &CurrentOperation) -> Result<String, PlanError> {
    Ok(format!(
        "{}:{}",
        current.operation.operation_id.as_str(),
        task(current)?.as_str()
    ))
}
/// Read the already-hydrated preparation and its observed sequence token.
pub fn preparation<'a, C>(
    current: &CurrentOperation,
    inputs: &'a PlannerInputs<C>,
) -> Result<(&'a DurableStructuralPreparation, SafeUnsigned), PlanError> {
    let k = preparation_key(current)?;
    match inputs.loaded.get(&k) {
        Some(PlannerLoadedValue::Register(Register::OperationPreparation { key, value, seq }))
            if key == &k =>
        {
            Ok((value, *seq))
        }
        _ => Err(PlanError::MissingInput),
    }
}
fn request_matches<C>(
    current: &CurrentOperation,
    outer: &SummaryEffectPlan<C>,
    request: &SummaryRequestPlan<C>,
    session: &SessionId,
) -> bool {
    matches(current, outer, session)
        && request.task_id == outer.generation.context.task_id
        && request.attempt == outer.generation.attempt
        && request.configuration == outer.generation.context.configuration
}
/// Conditionally reserve exactly the next nested request without executing it.
pub fn nested_intent_recipe<C: Clone>(
    current: &CurrentOperation,
    outer: &SummaryEffectPlan<C>,
    request: &SummaryRequestPlan<C>,
    facts: &CommitFacts,
) -> Result<RecipeDecision<C>, PlanError> {
    if !request_matches(current, outer, request, &facts.session_id) || is_cancelled(current) {
        return Ok(RecipeDecision::NoLongerExpected);
    }
    let mut p = pending(current).ok_or(PlanError::InvalidCurrent)?;
    if p.request.is_some()
        || request.request_index.get() > 1
        || request.request_index.get() != p.usage_ids.len() as u64
        || p.usage_ids.contains(&request.usage_id)
        || request.usage_id.as_str() == p.context.result_entry_id.as_str()
    {
        return Ok(RecipeDecision::NoLongerExpected);
    }
    p.request = Some(SummaryRequestIntent {
        index: request.request_index,
        usage_id: request.usage_id.clone(),
    });
    let next = pending_state(current, p)?;
    let mut r = recipe(current, facts, &request.telemetry_context);
    r.expected_effect = Some(outer.key.clone());
    r.transaction
        .writes
        .push(state_write(current, next.clone()));
    r.next = Some(next);
    Ok(RecipeDecision::Apply(Box::new(r)))
}
fn pending_state(
    current: &CurrentOperation,
    p: PendingSummaryGeneration,
) -> Result<OperationState, PlanError> {
    replace(
        current,
        StructuralDecision::Generating {
            task_id: p.context.task_id.clone(),
            generation: Box::new(SummaryGeneration::EffectPending {
                context: p.context,
                attempt: p.attempt,
                request: p.request,
                usage_ids: p.usage_ids,
            }),
        },
    )
}
/// Atomically settle the exact nested usage reservation and advance its state.
pub fn settle_request_recipe<C: Clone>(
    current: &CurrentOperation,
    outer: &SummaryEffectPlan<C>,
    request: &SummaryRequestSettlement<C>,
    facts: &CommitFacts,
) -> Result<RecipeDecision<C>, PlanError> {
    if !matches(current, outer, &facts.session_id)
        || request.current.operation != current.operation
        || request.task_id != outer.generation.context.task_id
        || request.attempt != outer.generation.attempt
    {
        return Ok(RecipeDecision::NoLongerExpected);
    }
    let mut p = pending(current).ok_or(PlanError::InvalidCurrent)?;
    if p.request.as_ref()
        != Some(&SummaryRequestIntent {
            index: request.request_index,
            usage_id: request.usage_id.clone(),
        })
        || request.request_index.get() != p.usage_ids.len() as u64
        || request.request_index.get() > 1
    {
        return Ok(RecipeDecision::NoLongerExpected);
    }
    let message = request.response.as_message();
    if message.stop_reason == StopReason::Aborted && !is_cancelled(current) {
        return Err(PlanError::InvalidCurrent);
    }
    if message.stop_reason == StopReason::Deferred {
        return Err(PlanError::InvalidInput);
    }
    p.request = None;
    p.usage_ids.push(request.usage_id.clone());
    let next = pending_state(current, p)?;
    let mut r = recipe(current, facts, &request.telemetry_context);
    r.expected_effect = Some(outer.key.clone());
    r.transaction
        .writes
        .push(usage(request.usage_id.clone(), None, message.usage.clone()));
    r.publication
        .push(Publication::Usage(request.usage_id.clone()));
    r.transaction
        .writes
        .push(state_write(current, next.clone()));
    r.next = Some(next);
    Ok(RecipeDecision::Apply(Box::new(r)))
}
pub(crate) fn usage(id: UsageId, entry_id: Option<EntryId>, usage: Usage) -> Write {
    Write::Usage(NewUsageRow {
        id,
        entry_id,
        adjustment: false,
        usage,
        details: Presence::Missing,
    })
}
/// Construct a missing-identity result for the owning operation intent.
pub fn suspension(
    current: &CurrentOperation,
    missing: MissingIdentities,
) -> Result<OperationResult, PlanError> {
    if missing.models.is_empty() && missing.tools.is_empty() {
        return Err(PlanError::InvalidInput);
    }
    Ok(match &current.operation.intent {
        OperationIntent::Run(_) => OperationResult::Run(Box::new(RunOutcome::MissingIdentities(
            RunMissingIdentities {
                kind: Default::default(),
                reason: Default::default(),
                leaf_id: current.leaf_id.clone().ok_or(PlanError::InvalidCurrent)?,
                missing,
            },
        ))),
        OperationIntent::Compaction(_) => OperationResult::Compaction(
            CompactionOutcome::MissingIdentities(CompactionMissingIdentities {
                kind: Default::default(),
                reason: Default::default(),
                leaf_id: current.leaf_id.clone().ok_or(PlanError::InvalidCurrent)?,
                missing,
            }),
        ),
        OperationIntent::Navigation(_) => OperationResult::Navigation(
            NavigationOutcome::MissingIdentities(NavigationMissingIdentities {
                kind: Default::default(),
                reason: Default::default(),
                leaf_id: current.leaf_id.clone(),
                missing,
            }),
        ),
    })
}
/// Validate an exact-key, Running, between-request suspension without writing.
pub fn suspend_recipe<C>(
    current: &CurrentOperation,
    outer: &SummaryEffectPlan<C>,
    output: &SummarySuspendedOutput,
    facts: &CommitFacts,
) -> Result<RecipeDecision<C>, PlanError> {
    if !matches(current, outer, &facts.session_id)
        || output.key() != &outer.key
        || is_cancelled(current)
        || pending(current).is_none_or(|p| p.request.is_some())
    {
        return Ok(RecipeDecision::NoLongerExpected);
    }
    Ok(RecipeDecision::Suspend(Box::new(suspension(
        current,
        output.missing().clone(),
    )?)))
}

pub(crate) fn transition<C: Clone>(
    current: &CurrentOperation,
    next: OperationState,
    context: &C,
) -> ProcedureAction<C> {
    let _ = current;
    ProcedureAction::Existing(Action::Transition(TransitionPlan {
        next,
        telemetry_context: context.clone(),
        expected_configuration_seq: None,
        expected_settings_revision: None,
    }))
}
fn context_generation(g: &SummaryGeneration) -> (&SummaryContext, OneBasedSafeUnsigned) {
    match g {
        SummaryGeneration::Ready {
            context,
            next_attempt,
        }
        | SummaryGeneration::RetryWait {
            context,
            next_attempt,
            ..
        } => (context, *next_attempt),
        SummaryGeneration::EffectPending {
            context, attempt, ..
        } => (context, *attempt),
    }
}
/// Choose the next structural action from explicit durable and live facts.
pub fn next<C: Clone>(
    current: &CurrentOperation,
    inputs: &PlannerInputs<C>,
    facts: &PlanningFacts<C>,
) -> Result<ProcedureAction<C>, PlanError> {
    if !owned(current) {
        return Err(PlanError::InvalidCurrent);
    }
    if matches!(
        &current.state,
        OperationState::Navigation(NavigationState::Unsummarized(_))
    ) {
        return Ok(ProcedureAction::Existing(Action::Finish(
            crate::navigation::unsummarized_result(current)?,
        )));
    }
    let d = decision(current).ok_or(PlanError::InvalidCurrent)?;
    if let PlanningFacts::MissingIdentities { missing } = facts {
        let safe = match d {
            StructuralDecision::Deciding { .. } => true,
            StructuralDecision::Generating { generation, .. } => {
                matches!(generation.as_ref(), SummaryGeneration::Ready { .. })
            }
        };
        if safe && !is_cancelled(current) {
            return Ok(ProcedureAction::Existing(Action::Suspend(suspension(
                current,
                missing.clone(),
            )?)));
        }
    }
    let PlanningFacts::Structural(p) = facts else {
        return Err(PlanError::MissingInput);
    };
    match d {
        StructuralDecision::Deciding { task_id } => {
            if is_cancelled(current) {
                return cancelled_action(current, &p.telemetry_context);
            }
            let event = decision_event(current, inputs)?;
            dispatch_or_await(
                inputs,
                EffectPlan::Hook(HookEffectPlan {
                    key: key(
                        current,
                        &p.session_id,
                        EffectKind::Hook,
                        task_id.as_str(),
                        0,
                        None,
                    )?,
                    event,
                    telemetry_context: p.telemetry_context.clone(),
                }),
            )
        }
        StructuralDecision::Generating {
            task_id,
            generation,
        } => {
            let (context, attempt) = context_generation(generation);
            match generation.as_ref() {
                SummaryGeneration::Ready { .. } => {
                    if is_cancelled(current) {
                        return cancelled_action(current, &p.telemetry_context);
                    }
                    let next = replace(
                        current,
                        StructuralDecision::Generating {
                            task_id: task_id.clone(),
                            generation: Box::new(SummaryGeneration::EffectPending {
                                context: context.clone(),
                                attempt,
                                request: None,
                                usage_ids: vec![],
                            }),
                        },
                    )?;
                    Ok(transition(current, next, &p.telemetry_context))
                }
                SummaryGeneration::RetryWait { not_before, .. } => {
                    if is_cancelled(current) {
                        return cancelled_action(current, &p.telemetry_context);
                    }
                    if inputs.now < *not_before {
                        return Ok(ProcedureAction::Existing(Action::Wait(WaitPlan {
                            until: *not_before,
                            telemetry_context: p.telemetry_context.clone(),
                        })));
                    }
                    Ok(transition(
                        current,
                        replace(
                            current,
                            StructuralDecision::Generating {
                                task_id: task_id.clone(),
                                generation: Box::new(SummaryGeneration::Ready {
                                    context: context.clone(),
                                    next_attempt: attempt,
                                }),
                            },
                        )?,
                        &p.telemetry_context,
                    ))
                }
                SummaryGeneration::EffectPending { .. } => {
                    let effect = plan(current, &p.session_id, &p.telemetry_context)?;
                    if let Some(live) = inputs.running.get(&effect.key) {
                        let EffectPlan::Summary(live) = live else {
                            return Err(PlanError::InvalidInput);
                        };
                        if !matches(current, live, &p.session_id) {
                            return Err(PlanError::InvalidInput);
                        }
                        return Ok(ProcedureAction::Existing(Action::AwaitEffect(effect.key)));
                    }
                    // A freshly committed intent is dispatched by its transition consumer.
                    // An observed pending attempt with no live owner is always uncertain.
                    let error = OperationError {
                        code: OperationErrorCode::new("summary_interrupted")
                            .map_err(|_| PlanError::InvalidInput)?,
                        message: "Summary attempt was interrupted".into(),
                        details: Presence::Missing,
                    };
                    let outcome = if !is_cancelled(current)
                        && attempt.get() < context.retry_policy.max_attempts.get()
                    {
                        SummaryAttemptOutcome::Retry(error)
                    } else {
                        SummaryAttemptOutcome::Failure(error)
                    };
                    Ok(ProcedureAction::Reconcile(Box::new(SettlementCommit {
                        current: current.clone(),
                        plan: EffectPlan::Summary(effect.clone()),
                        output: SettlementOutput::Summary {
                            key: effect.key,
                            outcome,
                        },
                        telemetry_context: p.telemetry_context.clone(),
                    })))
                }
            }
        }
    }
}
fn cancelled_action<C: Clone>(
    current: &CurrentOperation,
    context: &C,
) -> Result<ProcedureAction<C>, PlanError> {
    if let OperationState::Run(r) = &current.state {
        let mut next = r.clone();
        let trigger = match &r.phase {
            RunPhase::Compaction(c) => c.resume_after.trigger_entry_id.clone(),
            _ => return Err(PlanError::InvalidCurrent),
        };
        next.phase = checkpoint(
            trigger,
            Continuation::MayFinish {
                include_final_assistant: r.latest_assistant_entry_id.is_some(),
            },
        );
        Ok(transition(current, OperationState::Run(next), context))
    } else {
        Ok(ProcedureAction::Existing(Action::Finish(failed_result(
            current, None, true,
        )?)))
    }
}
/// Build the decision hook from immutable hydrated preparation.
pub fn decision_event<C>(
    current: &CurrentOperation,
    inputs: &PlannerInputs<C>,
) -> Result<HookEvent, PlanError> {
    let (p, _) = preparation(current, inputs)?;
    match (p, &current.state) {
        (
            DurableStructuralPreparation::Compaction {
                messages_to_summarize,
                turn_prefix_messages,
                retained_tail,
                is_split_turn,
                tokens_before,
                previous_summary,
                file_ops,
                settings,
            },
            state,
        ) => {
            let (reason, custom_instructions) = match state {
                OperationState::Compaction(c) => {
                    (HookCompactionReason::Manual, c.custom_instructions.clone())
                }
                OperationState::Run(r) => match &r.phase {
                    RunPhase::Compaction(c) => (
                        match c.reason {
                            RunCompactionReason::Threshold => HookCompactionReason::Threshold,
                            RunCompactionReason::Overflow => HookCompactionReason::Overflow,
                        },
                        None,
                    ),
                    _ => return Err(PlanError::InvalidCurrent),
                },
                _ => return Err(PlanError::InvalidCurrent),
            };
            Ok(HookEvent::BeforeCompaction(Box::new(
                BeforeCompactionEvent {
                    lane: current.operation.lane.clone(),
                    run_id: current.operation.operation_id.clone(),
                    reason,
                    preparation: CompactionPreparation {
                        messages_to_summarize: messages_to_summarize.clone(),
                        turn_prefix_messages: turn_prefix_messages.clone(),
                        retained_tail: retained_tail.clone(),
                        is_split_turn: *is_split_turn,
                        tokens_before: *tokens_before,
                        previous_summary: previous_summary.clone(),
                        file_ops: file_ops.clone(),
                        settings: settings.clone(),
                    },
                    custom_instructions,
                },
            )))
        }
        (
            DurableStructuralPreparation::BranchSummary {
                messages,
                file_ops,
                total_tokens,
            },
            OperationState::Navigation(NavigationState::Summarized(n)),
        ) => Ok(HookEvent::BeforeNavigation(Box::new(
            BeforeNavigationEvent {
                lane: current.operation.lane.clone(),
                run_id: current.operation.operation_id.clone(),
                target_id: n.target_id.clone(),
                preparation: BranchPreparation {
                    messages: messages.clone(),
                    file_ops: file_ops.clone(),
                    total_tokens: *total_tokens,
                },
                custom_instructions: n.custom_instructions.clone(),
            },
        ))),
        _ => Err(PlanError::InvalidInput),
    }
}
pub(crate) fn failed_result(
    current: &CurrentOperation,
    error: Option<OperationError>,
    aborted: bool,
) -> Result<OperationResult, PlanError> {
    Ok(match &current.state {
        OperationState::Compaction(_) => {
            let leaf_id = current.leaf_id.clone().ok_or(PlanError::InvalidCurrent)?;
            OperationResult::Compaction(if aborted {
                CompactionOutcome::Aborted(CompactionAborted {
                    kind: Default::default(),
                    leaf_id,
                })
            } else if let Some(error) = error {
                CompactionOutcome::Failed(CompactionFailed {
                    kind: Default::default(),
                    leaf_id,
                    error,
                })
            } else {
                CompactionOutcome::Declined(CompactionDeclined {
                    kind: Default::default(),
                    leaf_id,
                })
            })
        }
        OperationState::Navigation(_) => {
            let leaf_id = current.leaf_id.clone();
            OperationResult::Navigation(if aborted {
                NavigationOutcome::Aborted(NavigationAborted {
                    kind: Default::default(),
                    leaf_id,
                })
            } else if let Some(error) = error {
                NavigationOutcome::Failed(NavigationFailed {
                    kind: Default::default(),
                    leaf_id,
                    error,
                })
            } else {
                NavigationOutcome::Declined(NavigationDeclined {
                    kind: Default::default(),
                    leaf_id,
                })
            })
        }
        _ => return Err(PlanError::InvalidCurrent),
    })
}

pub(crate) fn last_result(
    current: &CurrentOperation,
    result: &OperationResult,
) -> Result<LaneLastResult, PlanError> {
    let operation_id = current.operation.operation_id.clone();
    macro_rules! simple {
        ($variant:ident,$ty:ident,$leaf:expr) => {
            LaneLastResult::$variant($ty {
                operation_id,
                kind: Default::default(),
                leaf_id: $leaf,
                outcome: Default::default(),
            })
        };
    }
    Ok(match (&current.operation.intent, result) {
        (OperationIntent::Compaction(_), OperationResult::Compaction(o)) => match o {
            CompactionOutcome::Completed(o) => simple!(
                CompactionCompleted,
                CompactionCompletedLastResult,
                Some(o.leaf_id.clone())
            ),
            CompactionOutcome::Declined(o) => simple!(
                CompactionDeclined,
                CompactionDeclinedLastResult,
                Some(o.leaf_id.clone())
            ),
            CompactionOutcome::Aborted(o) => simple!(
                CompactionAborted,
                CompactionAbortedLastResult,
                Some(o.leaf_id.clone())
            ),
            CompactionOutcome::Failed(o) => {
                LaneLastResult::CompactionFailed(CompactionFailedLastResult {
                    operation_id,
                    kind: Default::default(),
                    leaf_id: Some(o.leaf_id.clone()),
                    outcome: Default::default(),
                    error: o.error.clone(),
                })
            }
            _ => return Err(PlanError::InvalidInput),
        },
        (OperationIntent::Navigation(_), OperationResult::Navigation(o)) => match o {
            NavigationOutcome::Completed(o) => simple!(
                NavigationCompleted,
                NavigationCompletedLastResult,
                o.new_leaf_id.clone()
            ),
            NavigationOutcome::Declined(o) => simple!(
                NavigationDeclined,
                NavigationDeclinedLastResult,
                o.leaf_id.clone()
            ),
            NavigationOutcome::Aborted(o) => simple!(
                NavigationAborted,
                NavigationAbortedLastResult,
                o.leaf_id.clone()
            ),
            NavigationOutcome::Failed(o) => {
                LaneLastResult::NavigationFailed(NavigationFailedLastResult {
                    operation_id,
                    kind: Default::default(),
                    leaf_id: o.leaf_id.clone(),
                    outcome: Default::default(),
                    error: o.error.clone(),
                })
            }
            _ => return Err(PlanError::InvalidInput),
        },
        _ => return Err(PlanError::InvalidInput),
    })
}
fn new_entry(entry: &Entry) -> Result<NewEntry, PlanError> {
    Ok(match entry {
        Entry::Compaction(e) => NewEntry::Compaction(NewCompactionEntry {
            id: e.id.clone(),
            parent_id: e.parent_id.clone(),
            entry_type: Default::default(),
            summary: e.summary.clone(),
            retained_tail: e.retained_tail.clone(),
            tokens_before: e.tokens_before,
            details: e.details.clone(),
            usage: e.usage.clone(),
            from_hook: e.from_hook,
        }),
        Entry::BranchSummary(e) => NewEntry::BranchSummary(NewBranchSummaryEntry {
            id: e.id.clone(),
            parent_id: e.parent_id.clone(),
            entry_type: Default::default(),
            from_id: e.from_id.clone(),
            summary: e.summary.clone(),
            details: e.details.clone(),
            usage: e.usage.clone(),
            from_hook: e.from_hook,
        }),
        _ => return Err(PlanError::InvalidInput),
    })
}
/// Complete semantic result checked against caller-supplied backend-prepared metadata.
pub fn completed_result(
    current: &CurrentOperation,
    semantic: &SummarySuccessResult,
    prepared: &Entry,
    from_hook: bool,
) -> Result<OperationResult, PlanError> {
    let valid_source = if from_hook {
        matches!(decision(current), Some(StructuralDecision::Deciding { .. }))
    } else {
        pending(current).is_some_and(|p| p.request.is_none())
    };
    if !valid_source {
        return Err(PlanError::InvalidCurrent);
    }
    let id = match prepared {
        Entry::Compaction(e) => &e.id,
        Entry::BranchSummary(e) => &e.id,
        _ => return Err(PlanError::InvalidInput),
    };
    if current.leaf_id.as_ref() == Some(id) || current.operation.source_leaf_id.as_ref() == Some(id)
    {
        return Err(PlanError::InvalidInput);
    }
    if let Some(StructuralDecision::Generating { generation, .. }) = decision(current)
        && &context_generation(generation).0.result_entry_id != id
    {
        return Err(PlanError::InvalidInput);
    }
    match (semantic, prepared, &current.state) {
        (
            SummarySuccessResult::Compact(s),
            Entry::Compaction(e),
            OperationState::Compaction(_) | OperationState::Run(_),
        ) => {
            if e.parent_id != current.leaf_id
                || e.from_hook != from_hook
                || e.summary != s.summary
                || e.tokens_before != s.tokens_before
                || e.retained_tail != s.retained_tail
                || e.usage != s.usage
                || e.details != s.details
            {
                return Err(PlanError::InvalidInput);
            }
            Ok(OperationResult::Compaction(CompactionOutcome::Completed(
                CompactionCompleted {
                    kind: Default::default(),
                    leaf_id: e.id.clone(),
                    entry: e.clone(),
                },
            )))
        }
        (
            SummarySuccessResult::BranchSummary(s),
            Entry::BranchSummary(e),
            OperationState::Navigation(NavigationState::Summarized(n)),
        ) => {
            if e.parent_id.as_ref() != Some(&n.target_id)
                || Some(&e.from_id) != current.operation.source_leaf_id.as_ref()
                || e.from_hook != from_hook
                || e.summary != s.summary
                || e.usage != s.usage
            {
                return Err(PlanError::InvalidInput);
            }
            let mut d = std::collections::BTreeMap::new();
            d.insert(
                "readFiles".into(),
                JsonValue::Array(
                    s.read_files
                        .iter()
                        .cloned()
                        .map(JsonValue::String)
                        .collect(),
                ),
            );
            d.insert(
                "modifiedFiles".into(),
                JsonValue::Array(
                    s.modified_files
                        .iter()
                        .cloned()
                        .map(JsonValue::String)
                        .collect(),
                ),
            );
            if e.details != Presence::Value(JsonValue::Object(JsonObject::new(d))) {
                return Err(PlanError::InvalidInput);
            }
            Ok(OperationResult::Navigation(NavigationOutcome::Completed(
                Box::new(NavigationCompleted {
                    kind: Default::default(),
                    old_leaf_id: current.operation.source_leaf_id.clone(),
                    new_leaf_id: Some(e.id.clone()),
                    summary_entry: Some(e.clone()),
                }),
            )))
        }
        _ => Err(PlanError::InvalidInput),
    }
}
fn publication_prefix(
    current: &CurrentOperation,
    result: &OperationResult,
    extra: &Settlement,
) -> Result<Vec<Write>, PlanError> {
    let mut writes = vec![];
    let entry = match result {
        OperationResult::Compaction(CompactionOutcome::Completed(c)) => {
            Some(Entry::Compaction(c.entry.clone()))
        }
        OperationResult::Navigation(NavigationOutcome::Completed(n)) => {
            n.summary_entry.clone().map(Entry::BranchSummary)
        }
        _ => None,
    };
    if let Some(entry) = &entry {
        if extra.prepared_entry.as_ref() != Some(entry) {
            return Err(PlanError::MissingInput);
        }
        let (id, from_hook, used) = match entry {
            Entry::Compaction(e) => (e.id.clone(), e.from_hook, e.usage.clone()),
            Entry::BranchSummary(e) => (e.id.clone(), e.from_hook, e.usage.clone()),
            _ => return Err(PlanError::InvalidInput),
        };
        match (from_hook, used, &extra.hook_usage_id) {
            (true, Some(used), Some(uid)) if uid.as_str() != id.as_str() => {
                // This ordered usage write precedes the result entry's existence.
                writes.push(usage(uid.clone(), None, used))
            }
            (_, _, None)
                if !from_hook
                    || match entry {
                        Entry::Compaction(e) => e.usage.is_none(),
                        Entry::BranchSummary(e) => e.usage.is_none(),
                        _ => false,
                    } => {}
            _ => return Err(PlanError::InvalidInput),
        }
    } else if extra.prepared_entry.is_some() || extra.hook_usage_id.is_some() {
        return Err(PlanError::InvalidInput);
    }
    if let OperationResult::Navigation(NavigationOutcome::Completed(n)) = result {
        let target = match &current.state {
            OperationState::Navigation(NavigationState::Summarized(n)) => Some(n.target_id.clone()),
            OperationState::Navigation(NavigationState::Unsummarized(n)) => n.target_id.clone(),
            _ => return Err(PlanError::InvalidCurrent),
        };
        if n.old_leaf_id != current.operation.source_leaf_id {
            return Err(PlanError::InvalidInput);
        }
        writes.push(set(RegisterSet::LaneLeaf {
            key: current.operation.lane.as_str().into(),
            value: target,
        }));
    }
    if let Some(entry) = entry {
        let id = match &entry {
            Entry::Compaction(e) => e.id.clone(),
            Entry::BranchSummary(e) => e.id.clone(),
            _ => return Err(PlanError::InvalidInput),
        };
        writes.push(Write::Entry(new_entry(&entry)?));
        writes.push(set(RegisterSet::LaneLeaf {
            key: current.operation.lane.as_str().into(),
            value: Some(id),
        }));
    }
    if let (
        OperationIntent::Navigation(n),
        OperationResult::Navigation(NavigationOutcome::Completed(_)),
    ) = (&current.operation.intent, result)
        && let Some(label) = &n.label
    {
        writes.push(set(RegisterSet::FactLabel {
            key: n
                .target_id
                .as_ref()
                .ok_or(PlanError::InvalidCurrent)?
                .as_str()
                .into(),
            value: label.clone(),
        }));
    }
    Ok(writes)
}
/// Build final structural publication and complete cleanup under current tokens.
pub fn terminal_recipe<C: Clone>(
    current: &CurrentOperation,
    commit: &TerminalCommit<C>,
    facts: &CommitFacts,
    cleanup: &crate::terminal::TerminalFacts,
    extra: &Settlement,
) -> Result<RecipeDecision<C>, PlanError> {
    if !owned(current)
        || commit.current.operation != current.operation
        || commit.current.operation_state_seq != current.operation_state_seq
        || commit.current.lane_state_seq != current.lane_state_seq
    {
        return Ok(RecipeDecision::NoLongerExpected);
    }
    if pending(current).is_some_and(|p| p.request.is_some()) {
        return Ok(RecipeDecision::NoLongerExpected);
    }
    let aborted = matches!(
        &commit.result,
        OperationResult::Compaction(CompactionOutcome::Aborted(_))
            | OperationResult::Navigation(NavigationOutcome::Aborted(_))
    );
    if is_cancelled(current) != aborted {
        return Ok(RecipeDecision::NoLongerExpected);
    }
    match &commit.result {
        OperationResult::Compaction(CompactionOutcome::Completed(c)) => {
            if !matches!(current.state, OperationState::Compaction(_)) || c.leaf_id != c.entry.id {
                return Ok(RecipeDecision::NoLongerExpected);
            }
            let expected_hook =
                matches!(decision(current), Some(StructuralDecision::Deciding { .. }));
            let semantic = SummarySuccessResult::Compact(CompactResult {
                summary: c.entry.summary.clone(),
                tokens_before: c.entry.tokens_before,
                retained_tail: c.entry.retained_tail.clone(),
                usage: c.entry.usage.clone(),
                details: c.entry.details.clone(),
            });
            if completed_result(
                current,
                &semantic,
                &Entry::Compaction(c.entry.clone()),
                expected_hook,
            )? != commit.result
            {
                return Ok(RecipeDecision::NoLongerExpected);
            }
        }
        OperationResult::Navigation(NavigationOutcome::Completed(n)) => {
            let (target, summarized) = match &current.state {
                OperationState::Navigation(NavigationState::Summarized(s)) => {
                    (Some(s.target_id.clone()), true)
                }
                OperationState::Navigation(NavigationState::Unsummarized(s)) => {
                    (s.target_id.clone(), false)
                }
                _ => return Ok(RecipeDecision::NoLongerExpected),
            };
            if n.old_leaf_id != current.operation.source_leaf_id
                || n.summary_entry.is_some() != summarized
            {
                return Ok(RecipeDecision::NoLongerExpected);
            }
            if let Some(e) = &n.summary_entry {
                if !matches!(decision(current), Some(StructuralDecision::Deciding { .. }))
                    && pending(current).is_none()
                {
                    return Ok(RecipeDecision::NoLongerExpected);
                }
                if e.parent_id != target
                    || Some(&e.from_id) != current.operation.source_leaf_id.as_ref()
                    || n.new_leaf_id.as_ref() != Some(&e.id)
                    || e.from_hook
                        != matches!(decision(current), Some(StructuralDecision::Deciding { .. }))
                {
                    return Ok(RecipeDecision::NoLongerExpected);
                }
                if let Some(p) = pending(current)
                    && p.context.result_entry_id != e.id
                {
                    return Ok(RecipeDecision::NoLongerExpected);
                }
            } else if n.new_leaf_id != target {
                return Ok(RecipeDecision::NoLongerExpected);
            }
        }
        OperationResult::Compaction(CompactionOutcome::Declined(c))
            if Some(&c.leaf_id) == current.leaf_id.as_ref()
                && matches!(decision(current), Some(StructuralDecision::Deciding { .. })) => {}
        OperationResult::Compaction(CompactionOutcome::Aborted(c))
            if Some(&c.leaf_id) == current.leaf_id.as_ref() => {}
        OperationResult::Compaction(CompactionOutcome::Failed(c))
            if Some(&c.leaf_id) == current.leaf_id.as_ref() && pending(current).is_some() => {}
        OperationResult::Navigation(NavigationOutcome::Declined(n))
            if n.leaf_id == current.leaf_id
                && matches!(decision(current), Some(StructuralDecision::Deciding { .. })) => {}
        OperationResult::Navigation(NavigationOutcome::Aborted(n))
            if n.leaf_id == current.leaf_id => {}
        OperationResult::Navigation(NavigationOutcome::Failed(n))
            if n.leaf_id == current.leaf_id && pending(current).is_some() => {}
        _ => return Ok(RecipeDecision::NoLongerExpected),
    }
    if decision(current).is_some()
        && !cleanup.registers.iter().any(|r| {
            r.namespace == RegisterNamespace::OperationPreparation
                && preparation_key(current).is_ok_and(|key| r.key == key)
                && r.seq.is_some()
        })
    {
        return Err(PlanError::MissingInput);
    }
    let prefix = publication_prefix(current, &commit.result, extra)?;
    crate::terminal::finish_recipe(
        current,
        &commit.result,
        facts,
        cleanup,
        prefix,
        &commit.telemetry_context,
    )
}
fn state_recipe<C: Clone>(
    current: &CurrentOperation,
    next: OperationState,
    facts: &CommitFacts,
    context: &C,
) -> RecipeDecision<C> {
    let mut r = recipe(current, facts, context);
    r.transaction
        .writes
        .push(state_write(current, next.clone()));
    r.next = Some(next);
    RecipeDecision::Apply(Box::new(r))
}
fn failed_decision<C: Clone>(
    current: &CurrentOperation,
    error: Option<OperationError>,
    facts: &CommitFacts,
    context: &C,
) -> Result<RecipeDecision<C>, PlanError> {
    if let OperationState::Run(r) = &current.state {
        let RunPhase::Compaction(c) = &r.phase else {
            return Err(PlanError::InvalidCurrent);
        };
        let mut next = r.clone();
        next.phase = if is_cancelled(current) {
            checkpoint(
                c.resume_after.trigger_entry_id.clone(),
                Continuation::MayFinish {
                    include_final_assistant: r.latest_assistant_entry_id.is_some(),
                },
            )
        } else if error.is_none() && c.reason == RunCompactionReason::Threshold {
            RunPhase::Checkpoint(c.resume_after.clone())
        } else {
            RunPhase::FailureDrain(FailureDrainPhase {
                kind: Default::default(),
                error: error.unwrap_or(OperationError {
                    code: OperationErrorCode::new("compaction_declined")
                        .map_err(|_| PlanError::InvalidInput)?,
                    message: "Overflow compaction declined".into(),
                    details: Presence::Missing,
                }),
                provenance: FailureProvenance::Structural {
                    task_id: task(current)?.clone(),
                },
            })
        };
        Ok(state_recipe(
            current,
            OperationState::Run(next),
            facts,
            context,
        ))
    } else {
        Ok(RecipeDecision::Terminal(Box::new(TerminalCommit {
            current: current.clone(),
            result: failed_result(current, error, is_cancelled(current))?,
            telemetry_context: context.clone(),
        })))
    }
}
fn success<C: Clone>(
    current: &CurrentOperation,
    value: &SummarySuccessResult,
    extra: &Settlement,
    facts: &CommitFacts,
    context: &C,
    from_hook: bool,
) -> Result<RecipeDecision<C>, PlanError> {
    let prepared = extra
        .prepared_entry
        .as_ref()
        .ok_or(PlanError::MissingInput)?;
    let result = completed_result(current, value, prepared, from_hook)?;
    if let OperationState::Run(r) = &current.state {
        let RunPhase::Compaction(c) = &r.phase else {
            return Err(PlanError::InvalidCurrent);
        };
        let mut next = r.clone();
        next.phase = RunPhase::Checkpoint(c.resume_after.clone());
        let state = OperationState::Run(next);
        let mut r = recipe(current, facts, context);
        r.transaction.writes = publication_prefix(current, &result, extra)?;
        for w in &r.transaction.writes {
            match w {
                Write::Usage(u) => r.publication.push(Publication::Usage(u.id.clone())),
                Write::Entry(NewEntry::Compaction(e)) => {
                    r.publication.push(Publication::Entry(e.id.clone()))
                }
                _ => {}
            }
        }
        r.transaction
            .writes
            .push(state_write(current, state.clone()));
        r.next = Some(state);
        Ok(RecipeDecision::Apply(Box::new(r)))
    } else {
        Ok(RecipeDecision::Terminal(Box::new(TerminalCommit {
            current: current.clone(),
            result,
            telemetry_context: context.clone(),
        })))
    }
}
/// Merge structural hook or outer-summary output into the latest total state.
pub fn settle<C: Clone>(
    current: &CurrentOperation,
    commit: &SettlementCommit<C>,
    inputs: &PlannerInputs<C>,
    facts: &CommitFacts,
    extra: &Settlement,
) -> Result<RecipeDecision<C>, PlanError> {
    if let EffectPlan::Summary(outer) = &commit.plan {
        if !matches(current, outer, &facts.session_id) {
            return Ok(RecipeDecision::NoLongerExpected);
        }
        if let SettlementOutput::SummarySuspended(output) = &commit.output {
            return suspend_recipe(current, outer, output, facts);
        }
        let mut p = pending(current).ok_or(PlanError::InvalidCurrent)?;
        let live = inputs.running.get(&outer.key).is_some_and(
            |e| matches!(e,EffectPlan::Summary(p) if matches(current,p,&facts.session_id)),
        );
        let not_started =
            matches!(&commit.output,SettlementOutput::NotStarted{key} if key==&outer.key);
        if not_started && !is_cancelled(current) {
            return Ok(RecipeDecision::NoLongerExpected);
        }
        let outcome = match &commit.output {
            SettlementOutput::Summary { key, outcome } if key == &outer.key => Some(outcome),
            SettlementOutput::NotStarted { key } if key == &outer.key => None,
            _ => return Ok(RecipeDecision::NoLongerExpected),
        };
        if (!live || not_started)
            && (is_cancelled(current)
                || p.attempt.get() >= p.context.retry_policy.max_attempts.get())
            && p.request.is_some()
        {
            let request = p.request.take().ok_or(PlanError::InvalidCurrent)?;
            let mut r = recipe(current, facts, &commit.telemetry_context);
            if !not_started {
                r.transaction
                    .writes
                    .push(usage(request.usage_id.clone(), None, Usage::zero()));
                r.publication
                    .push(Publication::Usage(request.usage_id.clone()));
                p.usage_ids.push(request.usage_id);
            }
            let state = pending_state(current, p)?;
            r.transaction
                .writes
                .push(state_write(current, state.clone()));
            r.next = Some(state);
            return Ok(RecipeDecision::Apply(Box::new(r)));
        }
        if live && p.request.is_some() {
            return Ok(RecipeDecision::NoLongerExpected);
        }
        if is_cancelled(current) {
            return failed_decision(current, None, facts, &commit.telemetry_context);
        }
        let outcome = outcome.ok_or(PlanError::InvalidInput)?;
        if !live {
            if p.attempt.get() < p.context.retry_policy.max_attempts.get() {
                let next = replace(
                    current,
                    StructuralDecision::Generating {
                        task_id: p.context.task_id.clone(),
                        generation: Box::new(SummaryGeneration::Ready {
                            context: p.context,
                            next_attempt: crate::recovery::increment(p.attempt)?,
                        }),
                    },
                )?;
                return Ok(state_recipe(
                    current,
                    next,
                    facts,
                    &commit.telemetry_context,
                ));
            }
            return failed_decision(
                current,
                Some(OperationError {
                    code: OperationErrorCode::new("summary_interrupted")
                        .map_err(|_| PlanError::InvalidInput)?,
                    message: "Summary attempt was interrupted".into(),
                    details: Presence::Missing,
                }),
                facts,
                &commit.telemetry_context,
            );
        }
        if p.request.is_some() {
            return Ok(RecipeDecision::NoLongerExpected);
        }
        match outcome {
            SummaryAttemptOutcome::Success(value) => success(
                current,
                value,
                extra,
                facts,
                &commit.telemetry_context,
                false,
            ),
            SummaryAttemptOutcome::Retry(error)
                if p.attempt.get() < p.context.retry_policy.max_attempts.get() =>
            {
                let next = replace(
                    current,
                    StructuralDecision::Generating {
                        task_id: p.context.task_id.clone(),
                        generation: Box::new(SummaryGeneration::RetryWait {
                            context: p.context.clone(),
                            next_attempt: crate::recovery::increment(p.attempt)?,
                            not_before: crate::recovery::retry_deadline(
                                inputs.now,
                                p.context.retry_policy.base_delay_ms,
                                p.attempt,
                            )?,
                            error_message: error.message.clone(),
                        }),
                    },
                )?;
                Ok(state_recipe(
                    current,
                    next,
                    facts,
                    &commit.telemetry_context,
                ))
            }
            SummaryAttemptOutcome::Retry(error) | SummaryAttemptOutcome::Failure(error) => {
                failed_decision(
                    current,
                    Some(error.clone()),
                    facts,
                    &commit.telemetry_context,
                )
            }
        }
    } else {
        let StructuralDecision::Deciding { task_id } =
            decision(current).ok_or(PlanError::InvalidCurrent)?
        else {
            return Ok(RecipeDecision::NoLongerExpected);
        };
        let EffectPlan::Hook(h) = &commit.plan else {
            return Ok(RecipeDecision::NoLongerExpected);
        };
        let SettlementOutput::Hook(o) = &commit.output else {
            return Ok(RecipeDecision::NoLongerExpected);
        };
        let expected = key(
            current,
            &facts.session_id,
            EffectKind::Hook,
            task_id.as_str(),
            0,
            None,
        )?;
        if h.key != expected || o.key() != &expected || h.event != decision_event(current, inputs)?
        {
            return Ok(RecipeDecision::NoLongerExpected);
        }
        if is_cancelled(current) {
            return failed_decision(current, None, facts, &commit.telemetry_context);
        }
        let supplied = match o.result() {
            HookResult::BeforeCompaction(Some(r)) => match r.as_ref() {
                BeforeCompactionResult::DeclineTrue(_) => {
                    return failed_decision(current, None, facts, &commit.telemetry_context);
                }
                BeforeCompactionResult::Compaction(s) => {
                    Some(SummarySuccessResult::Compact(s.compaction.clone()))
                }
                _ => None,
            },
            HookResult::BeforeNavigation(Some(r)) => match r {
                BeforeNavigationResult::DeclineTrue(_) => {
                    return failed_decision(current, None, facts, &commit.telemetry_context);
                }
                BeforeNavigationResult::Summary(s) => {
                    Some(SummarySuccessResult::BranchSummary(s.summary.clone()))
                }
                _ => None,
            },
            HookResult::BeforeCompaction(None) | HookResult::BeforeNavigation(None) => None,
            _ => return Err(PlanError::InvalidInput),
        };
        if let Some(value) = supplied {
            return success(
                current,
                &value,
                extra,
                facts,
                &commit.telemetry_context,
                true,
            );
        }
        let id = extra
            .result_entry_id
            .clone()
            .ok_or(PlanError::MissingInput)?;
        if current.leaf_id.as_ref() == Some(&id)
            || current.operation.source_leaf_id.as_ref() == Some(&id)
        {
            return Err(PlanError::InvalidInput);
        }
        let (kind, reason) = match &current.state {
            OperationState::Compaction(_) => (SummaryKind::Compaction, Some(SummaryReason::Manual)),
            OperationState::Navigation(_) => (SummaryKind::BranchSummary, None),
            OperationState::Run(r) => match &r.phase {
                RunPhase::Compaction(c) => (
                    SummaryKind::Compaction,
                    Some(match c.reason {
                        RunCompactionReason::Threshold => SummaryReason::Threshold,
                        RunCompactionReason::Overflow => SummaryReason::Overflow,
                    }),
                ),
                _ => return Err(PlanError::InvalidCurrent),
            },
        };
        let next = replace(
            current,
            StructuralDecision::Generating {
                task_id: task_id.clone(),
                generation: Box::new(SummaryGeneration::Ready {
                    context: SummaryContext {
                        task_id: task_id.clone(),
                        result_entry_id: id,
                        kind,
                        configuration: current.configuration.clone(),
                        stream_options: inputs.runtime.stream_options.clone(),
                        retry_policy: inputs.runtime.retry_policy.clone(),
                        reason,
                    },
                    next_attempt: OneBasedSafeUnsigned::new(1)
                        .map_err(|_| PlanError::NumberRange)?,
                }),
            },
        )?;
        let mut r = recipe(current, facts, &commit.telemetry_context);
        r.expected.push(condition(
            RegisterNamespace::LaneConfig,
            current.operation.lane.as_str(),
            Some(current.configuration_seq),
        ));
        r.expected_settings_revision = Some(inputs.runtime.settings_revision);
        let (_, seq) = preparation(current, inputs)?;
        r.expected.push(condition(
            RegisterNamespace::OperationPreparation,
            &preparation_key(current)?,
            Some(seq),
        ));
        r.transaction
            .writes
            .push(state_write(current, next.clone()));
        r.next = Some(next);
        Ok(RecipeDecision::Apply(Box::new(r)))
    }
}
#[derive(Clone, Debug, PartialEq)]
/// Observed idle-lane facts for atomic structural acceptance.
pub struct Acceptance<C> {
    /// Provisional operation metadata.
    pub operation: Operation,
    /// Latest observed lane ownership and next-run queue.
    pub lane_state: LaneState,
    /// Observed lane-state sequence.
    pub lane_seq: SafeUnsigned,
    /// Observed lane-leaf sequence or absence.
    pub leaf_seq: Option<SafeUnsigned>,
    /// Reserved source leaf used for detached preparation.
    pub source_leaf: Option<EntryId>,
    /// Observed lane-configuration sequence.
    pub configuration_seq: SafeUnsigned,
    /// Observed process-local global settings revision.
    pub settings_revision: SafeUnsigned,
    /// Pre-acceptance identity-resolution result.
    pub identities_present: bool,
    /// Structural task identity.
    pub task_id: TaskId,
    /// Frozen structural input, absent only for empty or unsummarized work.
    pub preparation: Option<DurableStructuralPreparation>,
    /// Actual target point-read from the accepting Session.
    pub target: Option<Entry>,
    /// Opaque process-local observation context.
    pub telemetry_context: C,
}
/// Accept nonempty compaction or validated navigation as one checked transaction.
pub fn acceptance_recipe<C: Clone>(
    a: &Acceptance<C>,
) -> Result<Option<CommitRecipe<C>>, PlanError> {
    if a.lane_state.current_operation_id.is_some()
        || !a.identities_present
        || a.source_leaf != a.operation.source_leaf_id
    {
        return Err(PlanError::InvalidInput);
    }
    let structural = StructuralDecision::Deciding {
        task_id: a.task_id.clone(),
    };
    let state = match &a.operation.intent {
        OperationIntent::Compaction(i) => {
            let Some(p) = &a.preparation else {
                return Ok(None);
            };
            if a.source_leaf.is_none()
                || !matches!(p, DurableStructuralPreparation::Compaction { .. })
            {
                return Err(PlanError::InvalidInput);
            }
            OperationState::Compaction(CompactionState {
                kind: Default::default(),
                control: Control::Running(Default::default()),
                custom_instructions: i.custom_instructions.clone(),
                structural,
            })
        }
        OperationIntent::Navigation(i) => {
            crate::navigation::validate(&a.source_leaf, i, a.target.as_ref())
                .map_err(|_| PlanError::InvalidInput)?;
            if i.summarize {
                if !matches!(
                    a.preparation,
                    Some(DurableStructuralPreparation::BranchSummary { .. })
                ) {
                    return Err(PlanError::MissingInput);
                }
                OperationState::Navigation(NavigationState::Summarized(Box::new(
                    SummarizedNavigationState {
                        kind: Default::default(),
                        control: Control::Running(Default::default()),
                        target_id: i.target_id.clone().ok_or(PlanError::InvalidInput)?,
                        label: i.label.clone(),
                        custom_instructions: i.custom_instructions.clone(),
                        summarize: Default::default(),
                        phase: NavigationSummaryPhase {
                            kind: Default::default(),
                            structural,
                        },
                    },
                )))
            } else {
                if a.preparation.is_some() {
                    return Err(PlanError::InvalidInput);
                }
                OperationState::Navigation(NavigationState::Unsummarized(
                    UnsummarizedNavigationState {
                        kind: Default::default(),
                        control: Control::Running(Default::default()),
                        target_id: i.target_id.clone(),
                        label: i.label.clone(),
                        summarize: Default::default(),
                        phase: Default::default(),
                    },
                ))
            }
        }
        _ => return Err(PlanError::InvalidInput),
    };
    let op = a.operation.operation_id.as_str();
    let lane = a.operation.lane.as_str();
    let mut expected = vec![
        condition(RegisterNamespace::LaneState, lane, Some(a.lane_seq)),
        condition(RegisterNamespace::LaneLeaf, lane, a.leaf_seq),
        condition(
            RegisterNamespace::LaneConfig,
            lane,
            Some(a.configuration_seq),
        ),
        condition(RegisterNamespace::OperationMeta, op, None),
        condition(RegisterNamespace::OperationState, op, None),
    ];
    let mut writes = vec![];
    if let Some(p) = &a.preparation {
        let key = format!("{op}:{}", a.task_id.as_str());
        expected.push(condition(
            RegisterNamespace::OperationPreparation,
            &key,
            None,
        ));
        writes.push(set(RegisterSet::OperationPreparation {
            key,
            value: p.clone(),
        }));
    }
    writes.push(set(RegisterSet::OperationMeta {
        key: op.into(),
        value: a.operation.clone(),
    }));
    writes.push(set(RegisterSet::OperationState {
        key: op.into(),
        value: Box::new(state.clone()),
    }));
    let mut ls = a.lane_state.clone();
    ls.current_operation_id = Some(a.operation.operation_id.clone());
    writes.push(set(RegisterSet::LaneState {
        key: lane.into(),
        value: ls,
    }));
    Ok(Some(CommitRecipe {
        expected,
        expected_settings_revision: Some(a.settings_revision),
        expected_effect: None,
        transaction: Transaction { writes },
        next: Some(state),
        dispatch: None,
        result: None,
        publication: vec![],
        telemetry_context: a.telemetry_context.clone(),
    }))
}
