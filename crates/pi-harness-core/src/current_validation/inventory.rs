use super::*;

#[derive(Default)]
struct Inventory {
    reads: CurrentReferences,
    pending: BTreeSet<EntryId>,
    reservations: BTreeSet<String>,
}

impl Inventory {
    fn entry(&mut self, id: &EntryId, required: bool, role: EntryConstraint) {
        let entry = self.reads.entries.entry(id.clone()).or_default();
        entry.required |= required;
        entry.constraints.insert(role);
    }

    fn optional_entry(&mut self, id: &Option<EntryId>) {
        if let Some(id) = id {
            self.entry(id, true, EntryConstraint::Entry);
        }
    }

    fn reservation(&mut self, id: &str) -> Result<()> {
        if !self.reservations.insert(id.to_owned()) {
            return Err(CurrentValidationError::ReservationCollision);
        }
        Ok(())
    }

    fn pending(&mut self, ids: &[EntryId], message: bool) -> Result<()> {
        for id in ids {
            if !self.pending.insert(id.clone()) {
                return Err(CurrentValidationError::PendingOwnership);
            }
            self.reads.registers.insert(
                (RegisterNamespace::PendingEntry, id.as_str().to_owned()),
                if message {
                    RegisterConstraint::PendingMessage
                } else {
                    RegisterConstraint::PendingWrite
                },
            );
        }
        Ok(())
    }

    fn control(&mut self, control: &Control) -> Result<bool> {
        match control {
            Control::Running(_) => Ok(true),
            Control::CancelRequested(c) => {
                self.pending(&c.drained_steer, true)?;
                self.pending(&c.drained_follow_up, true)?;
                Ok(false)
            }
        }
    }

    fn checkpoint(&mut self, checkpoint: &CheckpointPhase) {
        self.entry(&checkpoint.trigger_entry_id, true, EntryConstraint::Entry);
        self.optional_entry(&checkpoint.threshold_checked_trigger_entry_id);
    }

    fn generation(&mut self, generation: &Generation, running: bool) -> Result<()> {
        let (context, attempt) = match generation {
            Generation::Ready {
                context,
                next_attempt,
            }
            | Generation::RetryWait {
                context,
                next_attempt,
                ..
            } => (context, next_attempt),
            Generation::EffectPending {
                context,
                attempt,
                response_entry_id,
                usage_id,
                ..
            } => {
                self.reservation(response_entry_id.as_str())?;
                self.reservation(usage_id.as_str())?;
                self.entry(
                    response_entry_id,
                    false,
                    EntryConstraint::Assistant { running },
                );
                (context, attempt)
            }
        };
        self.entry(&context.trigger_entry_id, true, EntryConstraint::Entry);
        attempt_cap(attempt, &context.retry_policy)
    }

    fn tools(&mut self, batch: &ToolBatch, operation: &Operation, running: bool) -> Result<()> {
        self.entry(
            &batch.assistant_entry_id,
            true,
            EntryConstraint::Assistant { running },
        );
        let mut incomplete = false;
        for call in &batch.calls {
            let (source_index, result_entry_id, completed) = match call {
                ToolCallState::Planned {
                    source_index,
                    result_entry_id,
                } => (source_index, result_entry_id, false),
                ToolCallState::EffectPending {
                    source_index,
                    result_entry_id,
                    ..
                } => {
                    self.reads.registers.insert(
                        (
                            RegisterNamespace::OperationToolArguments,
                            format!(
                                "{}:{}:{}",
                                operation.operation_id.as_str(),
                                batch.turn_id.as_str(),
                                source_index.get()
                            ),
                        ),
                        RegisterConstraint::ToolArguments,
                    );
                    (source_index, result_entry_id, false)
                }
                ToolCallState::Completed {
                    source_index,
                    result_entry_id,
                    ..
                } => (source_index, result_entry_id, true),
            };
            let _ = source_index;
            if completed && incomplete {
                return Err(CurrentValidationError::ToolBatch);
            }
            incomplete |= !completed;
            self.reservation(result_entry_id.as_str())?;
            self.entry(result_entry_id, completed, EntryConstraint::ToolResult);
        }
        if !batch.calls.is_empty() && !incomplete {
            return Err(CurrentValidationError::ToolBatch);
        }
        Ok(())
    }

    fn deferred(&mut self, deferred: &Deferred, running: bool) -> Result<()> {
        let source = match deferred {
            Deferred::Suspended {
                source_entry_id, ..
            } => source_entry_id,
            Deferred::EffectPending {
                source_entry_id,
                response_entry_id,
                usage_id,
                ..
            } => {
                self.reservation(response_entry_id.as_str())?;
                self.reservation(usage_id.as_str())?;
                self.entry(
                    response_entry_id,
                    false,
                    EntryConstraint::Assistant { running },
                );
                source_entry_id
            }
        };
        self.entry(source, true, EntryConstraint::DeferredAssistant { running });
        Ok(())
    }

    fn structural(
        &mut self,
        structural: &StructuralDecision,
        operation: &Operation,
        kind: SummaryKind,
        reason: Option<SummaryReason>,
    ) -> Result<()> {
        let (task, generation) = match structural {
            StructuralDecision::Deciding { task_id } => (task_id, None),
            StructuralDecision::Generating {
                task_id,
                generation,
            } => (task_id, Some(generation.as_ref())),
        };
        self.reads.registers.insert(
            (
                RegisterNamespace::OperationPreparation,
                format!("{}:{}", operation.operation_id.as_str(), task.as_str()),
            ),
            match kind {
                SummaryKind::Compaction => RegisterConstraint::CompactionPreparation,
                SummaryKind::BranchSummary => RegisterConstraint::BranchSummaryPreparation,
            },
        );
        let Some(generation) = generation else {
            return Ok(());
        };
        let (context, attempt) = match generation {
            SummaryGeneration::Ready {
                context,
                next_attempt,
            }
            | SummaryGeneration::RetryWait {
                context,
                next_attempt,
                ..
            } => (context, next_attempt),
            SummaryGeneration::EffectPending {
                context,
                attempt,
                request,
                usage_ids,
            } => {
                if usage_ids.len() > 2 {
                    return Err(CurrentValidationError::Structural);
                }
                for id in usage_ids {
                    self.reservation(id.as_str())?;
                }
                if let Some(request) = request {
                    if request.index.get() > 1 || request.index.get() != usage_ids.len() as u64 {
                        return Err(CurrentValidationError::Structural);
                    }
                    self.reservation(request.usage_id.as_str())?;
                }
                (context, attempt)
            }
        };
        if &context.task_id != task || context.kind != kind || context.reason != reason {
            return Err(CurrentValidationError::Structural);
        }
        attempt_cap(attempt, &context.retry_policy)?;
        self.reservation(context.result_entry_id.as_str())?;
        let role = match kind {
            SummaryKind::Compaction => EntryConstraint::Compaction,
            SummaryKind::BranchSummary => EntryConstraint::BranchSummary {
                from_id: operation
                    .source_leaf_id
                    .clone()
                    .ok_or(CurrentValidationError::Navigation)?,
            },
        };
        self.entry(&context.result_entry_id, false, role);
        Ok(())
    }

    fn run(&mut self, run: &RunState, operation: &Operation) -> Result<()> {
        let running = self.control(&run.control)?;
        if !running && (!run.inbox.steer.is_empty() || !run.inbox.follow_up.is_empty()) {
            return Err(CurrentValidationError::Control);
        }
        self.pending(&run.inbox.steer, true)?;
        self.pending(&run.inbox.follow_up, true)?;
        self.pending(&run.inbox.writes, false)?;
        if let Some(id) = &run.latest_assistant_entry_id {
            self.entry(id, true, EntryConstraint::Assistant { running });
        }
        match &run.phase {
            RunPhase::Checkpoint(p) => {
                self.checkpoint(p);
                Ok(())
            }
            RunPhase::Assistant(p) => self.generation(&p.generation, running),
            RunPhase::Tools(p) => self.tools(&p.batch, operation, running),
            RunPhase::Deferred(p) => self.deferred(&p.deferred, running),
            RunPhase::FailureDrain(p) => {
                if let FailureProvenance::Response { entry_id } = &p.provenance {
                    self.entry(entry_id, true, EntryConstraint::Assistant { running });
                }
                Ok(())
            }
            RunPhase::Compaction(p) => {
                self.checkpoint(&p.resume_after);
                let reason = match p.reason {
                    RunCompactionReason::Threshold => {
                        if p.resume_after.threshold_checked_trigger_entry_id.as_ref()
                            != Some(&p.resume_after.trigger_entry_id)
                        {
                            return Err(CurrentValidationError::Structural);
                        }
                        SummaryReason::Threshold
                    }
                    RunCompactionReason::Overflow => {
                        if !matches!(
                            p.resume_after.continuation,
                            Continuation::NeedAssistant {
                                overflow_recovery_used: true
                            }
                        ) {
                            return Err(CurrentValidationError::Structural);
                        }
                        SummaryReason::Overflow
                    }
                };
                self.structural(
                    &p.structural,
                    operation,
                    SummaryKind::Compaction,
                    Some(reason),
                )
            }
        }
    }

    fn navigation(
        &mut self,
        state: &NavigationState,
        operation: &Operation,
        intent: &NavigationIntent,
    ) -> Result<()> {
        if intent.target_id == operation.source_leaf_id
            || (intent.target_id.is_none() && intent.label.is_some())
            || (intent.summarize
                && (intent.target_id.is_none() || operation.source_leaf_id.is_none()))
        {
            return Err(CurrentValidationError::Navigation);
        }
        self.optional_entry(&intent.target_id);
        match state {
            NavigationState::Unsummarized(s) => {
                self.control(&s.control)?;
                if intent.summarize || s.target_id != intent.target_id || s.label != intent.label {
                    return Err(CurrentValidationError::Navigation);
                }
                // Unsummarized state has no instructions field to compare.
                Ok(())
            }
            NavigationState::Summarized(s) => {
                self.control(&s.control)?;
                if !intent.summarize
                    || Some(&s.target_id) != intent.target_id.as_ref()
                    || s.label != intent.label
                    || s.custom_instructions != intent.custom_instructions
                {
                    return Err(CurrentValidationError::Navigation);
                }
                self.structural(
                    &s.phase.structural,
                    operation,
                    SummaryKind::BranchSummary,
                    None,
                )
            }
        }
    }
}

fn attempt_cap(attempt: &OneBasedSafeUnsigned, policy: &NormalizedRetryPolicy) -> Result<()> {
    if attempt.get() > policy.max_attempts.get() {
        return Err(CurrentValidationError::Attempt);
    }
    Ok(())
}

pub(super) fn collect(joined: &Joined<'_>) -> Result<CurrentReferences> {
    let mut inventory = Inventory::default();
    inventory.optional_entry(joined.leaf);
    inventory.pending(&joined.lane_state.pending_next_run, true)?;
    if let Some((operation, state, _)) = joined.active {
        inventory.optional_entry(&operation.source_leaf_id);
        match (&operation.intent, state) {
            (OperationIntent::Run(intent), OperationState::Run(run)) => {
                for id in &intent.prompt_entry_ids {
                    inventory.entry(id, true, EntryConstraint::Message);
                }
                inventory.run(run, operation)?;
            }
            (OperationIntent::Compaction(intent), OperationState::Compaction(s)) => {
                inventory.control(&s.control)?;
                if intent.custom_instructions != s.custom_instructions {
                    return Err(CurrentValidationError::Structural);
                }
                inventory.structural(
                    &s.structural,
                    operation,
                    SummaryKind::Compaction,
                    Some(SummaryReason::Manual),
                )?;
            }
            (OperationIntent::Navigation(intent), OperationState::Navigation(s)) => {
                inventory.navigation(s, operation, intent)?
            }
            _ => return Err(CurrentValidationError::RootIdentity),
        }
    }
    Ok(inventory.reads)
}
