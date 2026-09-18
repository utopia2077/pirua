//! Pure threshold and specific assistant-overflow publication.
use crate::{planner::*, structural};
use pi_agent_schema::session_values::*;
use pi_agent_schema::*;
#[derive(Clone, Debug, PartialEq)]
/// Frozen compaction preparation with its newly reserved task identity.
pub struct PreparedCompaction {
    /// Structural task identity.
    pub task_id: TaskId,
    /// Frozen structural input, absent only for empty or unsummarized work.
    pub preparation: DurableStructuralPreparation,
}
#[derive(Clone, Debug, PartialEq)]
/// Detached context estimate and optional preparation for one checkpoint.
pub struct Threshold<C> {
    /// Opaque process-local observation context.
    pub telemetry_context: C,
    /// Source-equivalent estimated context tokens.
    pub context_tokens: SafeUnsigned,
    /// Current model context-window limit.
    pub context_window: SafeUnsigned,
    /// Optional nonempty prepared compaction.
    pub prepared: Option<PreparedCompaction>,
}
#[derive(Clone, Debug, PartialEq)]
/// Detached matching-error classification and optional first-overflow preparation.
pub struct Overflow {
    /// Ordinary error retry classification, subordinate to overflow.
    pub retryable: bool,
    /// Whether an Error message matches the context-limit classifier.
    pub matching_error: bool,
    /// Optional nonempty prepared compaction.
    pub prepared: Option<PreparedCompaction>,
    /// Reserved tool result identities for non-overflow classification.
    pub followers: Vec<(SafeUnsigned, EntryId)>,
}
/// Evaluate the pinned strict comparison using signed subtraction.
pub fn threshold_due(
    tokens: SafeUnsigned,
    window: SafeUnsigned,
    settings: &CompactionSettings,
) -> bool {
    settings.enabled
        && i128::from(tokens.get())
            > i128::from(window.get()) - i128::from(settings.reserve_tokens.get())
}
/// Mark the checkpoint and optionally enter prepared threshold compaction.
pub fn threshold_action<C: Clone>(
    current: &CurrentOperation,
    facts: &Threshold<C>,
) -> Result<ProcedureAction<C>, PlanError> {
    let r = run(current)?;
    let RunPhase::Checkpoint(p) = &r.phase else {
        return Err(PlanError::InvalidCurrent);
    };
    if cancelled(r) || p.threshold_checked_trigger_entry_id.as_ref() == Some(&p.trigger_entry_id) {
        return Err(PlanError::InvalidInput);
    }
    if p.skip_inbox_once != Some(true) && (!r.inbox.writes.is_empty() || !r.inbox.steer.is_empty())
    {
        return Err(PlanError::InvalidInput);
    }
    let mut marked = p.clone();
    marked.threshold_checked_trigger_entry_id = Some(p.trigger_entry_id.clone());
    let mut next = r.clone();
    if threshold_due(
        facts.context_tokens,
        facts.context_window,
        &r.settings.compaction,
    ) {
        if let Some(prepared) = &facts.prepared {
            check_preparation(prepared, &r.settings.compaction)?;
            next.phase = RunPhase::Compaction(RunCompactionPhase {
                kind: Default::default(),
                reason: RunCompactionReason::Threshold,
                structural: StructuralDecision::Deciding {
                    task_id: prepared.task_id.clone(),
                },
                resume_after: marked,
            });
        } else {
            next.phase = RunPhase::Checkpoint(marked)
        }
    } else {
        if facts.prepared.is_some() {
            return Err(PlanError::InvalidInput);
        }
        next.phase = RunPhase::Checkpoint(marked)
    }
    Ok(structural::transition(
        current,
        OperationState::Run(Box::new(next)),
        &facts.telemetry_context,
    ))
}
pub(crate) fn check_preparation(
    p: &PreparedCompaction,
    settings: &CompactionSettings,
) -> Result<(), PlanError> {
    match &p.preparation {
        DurableStructuralPreparation::Compaction {
            settings: captured, ..
        } if captured == settings => Ok(()),
        _ => Err(PlanError::InvalidInput),
    }
}
pub(crate) fn add_preparation<C>(
    current: &CurrentOperation,
    p: &PreparedCompaction,
    r: &mut CommitRecipe<C>,
) -> Result<(), PlanError> {
    let k = format!(
        "{}:{}",
        current.operation.operation_id.as_str(),
        p.task_id.as_str()
    );
    add_condition(
        &mut r.expected,
        condition(RegisterNamespace::OperationPreparation, &k, None),
    )?;
    r.transaction
        .writes
        .push(set(RegisterSet::OperationPreparation {
            key: k,
            value: p.preparation.clone(),
        }));
    Ok(())
}
/// A supplied classifier flag describes matching Error only, never a Stop counter.
pub fn is_overflow(
    message: &AssistantMessage,
    intended: SafeUnsigned,
    matching_error: bool,
) -> bool {
    (message.stop_reason == StopReason::Error && matching_error)
        || (message.stop_reason == StopReason::Length && message.usage.output < intended)
}
pub(crate) fn overflow_phase(
    current: &CurrentOperation,
    g: &PendingGeneration,
    response: &EntryId,
    prepared: Option<&PreparedCompaction>,
) -> Result<RunPhase, PlanError> {
    let r = run(current)?;
    if !g.context.overflow_recovery_used
        && let Some(p) = prepared
    {
        check_preparation(p, &r.settings.compaction)?;
        return Ok(RunPhase::Compaction(RunCompactionPhase {
            kind: Default::default(),
            reason: RunCompactionReason::Overflow,
            structural: StructuralDecision::Deciding {
                task_id: p.task_id.clone(),
            },
            resume_after: CheckpointPhase {
                kind: Default::default(),
                continuation: Continuation::NeedAssistant {
                    overflow_recovery_used: true,
                },
                trigger_entry_id: g.context.trigger_entry_id.clone(),
                threshold_checked_trigger_entry_id: None,
                skip_inbox_once: None,
            },
        }));
    }
    Ok(RunPhase::FailureDrain(FailureDrainPhase {
        kind: Default::default(),
        error: OperationError {
            code: OperationErrorCode::new("context_overflow")
                .map_err(|_| PlanError::InvalidInput)?,
            message: "Context overflow".into(),
            details: Presence::Missing,
        },
        provenance: FailureProvenance::Response {
            entry_id: response.clone(),
        },
    }))
}

/// Preserve the observed reason while labelling context-overflow classification.
pub fn normalize_overflow(message: &mut AssistantMessage, intended: Option<SafeUnsigned>) {
    let reason = if message.stop_reason == StopReason::Length {
        intended.map_or_else(
            || "length below captured output limit".to_owned(),
            |limit| {
                format!(
                    "length output {} below intended limit {}",
                    message.usage.output.get(),
                    limit.get()
                )
            },
        )
    } else {
        message
            .error_message
            .clone()
            .unwrap_or_else(|| "matching context-limit error".into())
    };
    message.stop_reason = StopReason::Error;
    message.error_message = Some(format!("Context overflow heuristic: {reason}"));
    message.deferred = None;
}
