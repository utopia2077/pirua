//! Real conditional Session publication of typed core recipes.
use crate::{
    owner::{RuntimeLane, Telemetry},
    restore::{HydratedLane, hydrate_lane},
    scheduler::{Admission, CommitPermit, MutationPermit},
};
use pi_agent_schema::*;
use pi_agent_session::{
    backend_support::{register_key, register_seq},
    session::WriteObservation,
    storage::{
        ConditionalCommit, ConditionalOutcome, ExpectedRegister, RegisterCondition, RuntimeStorage,
    },
};
use pi_harness_core::planner::{CommitFacts, CommitRecipe, Publication};

/// Native callers that already own both lines keep commit drain notification
/// until after their held-line publication scope releases both lines.
pub(crate) struct HeldPublication {
    pair: Option<(MutationPermit, MutationPermit)>,
    commit: Option<CommitPermit>,
}

impl HeldPublication {
    pub(crate) fn new(settings: MutationPermit, lane: MutationPermit) -> Self {
        Self {
            pair: Some((settings, lane)),
            commit: None,
        }
    }

    pub(crate) fn commit_slot(&mut self) -> &mut Option<CommitPermit> {
        &mut self.commit
    }

    pub(crate) fn release(&mut self) {
        if let Some((settings, lane)) = self.pair.take() {
            MutationPermit::release_pair(lane, settings);
        }
        drop(self.commit.take());
    }
}

impl Drop for HeldPublication {
    fn drop(&mut self) {
        self.release();
    }
}

impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>
    RuntimeLane<C, S, M>
{
    pub(crate) async fn load(&self) -> Result<HydratedLane, HarnessRuntimeError> {
        self.owner.observe(self.load_raw().await)
    }
    /// The caller observes a routed failure after releasing its Harness lines.
    pub(crate) async fn load_raw(&self) -> Result<HydratedLane, HarnessRuntimeError> {
        hydrate_lane(
            self.owner.session.as_ref(),
            self.lane.name.clone(),
            &self.owner.admission,
        )
        .await
    }
    pub(crate) fn facts(&self, loaded: &HydratedLane) -> Result<CommitFacts, HarnessRuntimeError> {
        self.facts_with_policy(loaded, false)
    }
    pub(crate) fn facts_raw(
        &self,
        loaded: &HydratedLane,
    ) -> Result<CommitFacts, HarnessRuntimeError> {
        self.facts_with_policy(loaded, true)
    }
    fn facts_with_policy(
        &self,
        loaded: &HydratedLane,
        raw: bool,
    ) -> Result<CommitFacts, HarnessRuntimeError> {
        Ok(CommitFacts {
            session_id: self.owner.session.metadata().base_metadata().id.clone(),
            leaf_seq: Some(register_seq(&loaded.roots.leaf)),
            meta_seq: register_seq(
                loaded
                    .roots
                    .operation_meta
                    .as_ref()
                    .ok_or_else(|| self.publication_fault(raw, "operation meta missing"))?,
            ),
        })
    }
    fn publication_fault(&self, raw: bool, message: &'static str) -> HarnessRuntimeError {
        if raw {
            crate::restore::fault(message)
        } else {
            self.owner.fail(message)
        }
    }
    /// Caller holds settings-before-lane mutation ownership. Admission and the
    /// retained task must outlive definitive commit and in-memory publication.
    pub(crate) async fn apply(
        &self,
        recipe: CommitRecipe<Telemetry>,
    ) -> Result<Option<HydratedLane>, HarnessRuntimeError> {
        self.apply_prepared(recipe, None).await
    }
    pub(crate) async fn apply_prepared(
        &self,
        recipe: CommitRecipe<Telemetry>,
        prepared: Option<pi_agent_session::storage::PreparedEntries>,
    ) -> Result<Option<HydratedLane>, HarnessRuntimeError> {
        let mut permit = None;
        self.apply_prepared_scoped(recipe, prepared, &mut permit, false)
            .await
    }
    /// The caller owns the permit until all held lines and publication work end.
    pub(crate) async fn apply_prepared_scoped(
        &self,
        recipe: CommitRecipe<Telemetry>,
        prepared: Option<pi_agent_session::storage::PreparedEntries>,
        permit_slot: &mut Option<CommitPermit>,
        raw: bool,
    ) -> Result<Option<HydratedLane>, HarnessRuntimeError> {
        self.apply_prepared_inner(recipe, prepared, permit_slot, raw, None, None)
            .await
    }
    /// Routed commits have already admitted their permit and carry their write
    /// observation out of this held-line publication call.
    pub(crate) async fn apply_routed_prepared(
        &self,
        recipe: CommitRecipe<Telemetry>,
        prepared: pi_agent_session::storage::PreparedEntries,
        permit_slot: &mut Option<CommitPermit>,
        observation_slot: &mut Option<WriteObservation>,
    ) -> Result<Option<HydratedLane>, HarnessRuntimeError> {
        debug_assert!(permit_slot.is_some());
        self.apply_prepared_inner(
            recipe,
            Some(prepared),
            permit_slot,
            true,
            Some(observation_slot),
            None,
        )
        .await
    }
    pub(crate) async fn apply_tool_publication(
        &self,
        recipe: CommitRecipe<Telemetry>,
        prepared: Option<pi_agent_session::storage::PreparedEntries>,
        permit_slot: &mut Option<CommitPermit>,
        cell: &crate::tools::ToolPublication<C>,
    ) -> Result<Option<HydratedLane>, HarnessRuntimeError> {
        self.apply_prepared_inner(recipe, prepared, permit_slot, true, None, Some(cell))
            .await
    }
    async fn apply_prepared_inner(
        &self,
        recipe: CommitRecipe<Telemetry>,
        prepared: Option<pi_agent_session::storage::PreparedEntries>,
        permit_slot: &mut Option<CommitPermit>,
        raw: bool,
        observation_slot: Option<&mut Option<WriteObservation>>,
        tool_publication: Option<&crate::tools::ToolPublication<C>>,
    ) -> Result<Option<HydratedLane>, HarnessRuntimeError> {
        if let Some(revision) = recipe.expected_settings_revision
            && self
                .owner
                .settings
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .revision
                != revision
        {
            return Ok(None);
        }
        let previous = self
            .lane
            .hydrated
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .current
            .clone();
        if permit_slot.is_none() {
            *permit_slot = Some(self.owner.admission.try_admit()?);
        }
        let telemetry = recipe.telemetry_context.clone();
        let events = self.owner.events.with_context(telemetry.clone());
        let conditions: Vec<RegisterCondition> = recipe
            .expected
            .iter()
            .map(|condition| RegisterCondition {
                register: pi_agent_session::storage::RegisterKey {
                    namespace: condition.namespace,
                    key: condition.key.clone(),
                },
                expected: condition
                    .seq
                    .map_or(ExpectedRegister::Absent, ExpectedRegister::Seq),
            })
            .collect();
        // Preserve the pure recipe exactly. Duplicate/conflicting predicates are
        // a recipe defect, never silently deduplicated by the interpreter.
        let accepted_operation = recipe
            .transaction
            .writes
            .iter()
            .find_map(|write| match write {
                Write::SetRegister(register) => match register.as_ref() {
                    RegisterSet::OperationMeta { value, .. } => Some(value.operation_id.clone()),
                    _ => None,
                },
                _ => None,
            });
        let usage_writes: Vec<_> = recipe
            .transaction
            .writes
            .iter()
            .enumerate()
            .filter_map(|(index, write)| match write {
                Write::Usage(row) => Some((index, row.clone())),
                _ => None,
            })
            .collect();
        let operation_id = recipe
            .expected
            .iter()
            .find(|c| c.namespace == RegisterNamespace::OperationMeta && c.seq.is_some())
            .and_then(|c| OperationId::new(c.key.clone()).ok());
        let args_write = tool_publication.map(|cell| {
            let matches: Vec<_> = recipe
                .transaction
                .writes
                .iter()
                .enumerate()
                .filter_map(|(index, write)| {
                    if let Write::SetRegister(register) = write
                        && let RegisterSet::OperationToolArguments { key, value } =
                            register.as_ref()
                        && key == &cell.plan.args_key
                    {
                        Some((index, value.clone()))
                    } else {
                        None
                    }
                })
                .collect();
            match matches.as_slice() {
                [(index, value)] if value == &cell.effective_args => Ok(*index),
                _ => Err("tool publication args write is not unique and exact"),
            }
        });
        let request = ConditionalCommit {
            conditions,
            transaction: recipe.transaction,
        };
        let outcome = match (prepared, observation_slot) {
            (Some(prepared), Some(slot)) => {
                let (outcome, observation) = self
                    .owner
                    .session
                    .commit_prepared_conditional_with_observation(prepared, request)
                    .await;
                *slot = observation;
                outcome
            }
            (Some(prepared), None) => {
                self.owner
                    .session
                    .commit_prepared_conditional_with_telemetry(
                        prepared,
                        request,
                        telemetry.as_ref(),
                    )
                    .await
            }
            (None, None) => {
                self.owner
                    .session
                    .commit_conditional_with_telemetry(request, telemetry.as_ref())
                    .await
            }
            (None, Some(_)) => unreachable!("routed commit requires prepared entries"),
        }
        .map_err(|_| self.publication_fault(raw, "runtime conditional publication failed"))?;
        let ConditionalOutcome::Applied(commit) = outcome else {
            return Ok(None);
        };
        if let Some(cell) = tool_publication {
            let receipt = args_write
                .expect("tool receipt write checked")
                .and_then(|index| {
                    commit
                        .seqs
                        .get(index)
                        .copied()
                        .map(|seq| crate::tools::CapturedArguments {
                            key: cell.plan.args_key.clone(),
                            value: cell.effective_args.clone(),
                            seq,
                        })
                        .ok_or("tool publication commit args sequence missing")
                });
            *cell.receipt.lock().unwrap_or_else(|p| p.into_inner()) =
                crate::tools::CommitArgumentsReceipt::Applied(receipt);
            // Interpret even a malformed host receipt only after fresh hydration
            // has established whether its full Tool K is still expected.
        }
        if accepted_operation.is_some()
            || recipe
                .publication
                .iter()
                .any(|p| matches!(p, Publication::Terminal(_)))
        {
            self.owner
                .suspensions
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .remove(&self.lane.name);
        }
        let loaded = hydrate_lane(
            self.owner.session.as_ref(),
            self.lane.name.clone(),
            &Admission::new(),
        )
        .await
        .map_err(|_| self.publication_fault(raw, "runtime committed state hydration failed"))?;
        if let Some(current) = loaded.current.as_ref() {
            self.structural_started(previous.as_ref(), current, telemetry.clone());
        }
        if let Some(run_id) = &accepted_operation
            && loaded
                .current
                .as_ref()
                .is_some_and(|c| matches!(c.operation.intent, OperationIntent::Run(_)))
        {
            events.enqueue(
                HarnessEventType::RunStart,
                &self.lane.name,
                HarnessEvent::RunStart(RunStartEvent {
                    event_type: Default::default(),
                    lane: self.lane.name.clone(),
                    recovery: None,
                    run_id: run_id.clone(),
                }),
            );
        }
        for publication in recipe.publication {
            match publication {
                Publication::Entry(id) => {
                    let entries = self
                        .owner
                        .session
                        .get_entries(std::slice::from_ref(&id))
                        .await
                        .map_err(|_| {
                            self.publication_fault(raw, "runtime committed entry read failed")
                        })?;
                    let entry = entries.get(&id).cloned().ok_or_else(|| {
                        self.publication_fault(raw, "runtime committed entry is absent")
                    })?;
                    if let Entry::Message(message) = &entry {
                        let streamed = self
                            .lane
                            .streaming
                            .lock()
                            .unwrap_or_else(|p| p.into_inner())
                            .as_ref()
                            .is_some_and(|(stream_id, _)| stream_id == &id);
                        if accepted_operation.is_some() {
                            let run_id = accepted_operation.clone().or(operation_id.clone());
                            events.enqueue(
                                HarnessEventType::MessageStart,
                                &self.lane.name,
                                HarnessEvent::MessageStart(Box::new(MessageStartEvent {
                                    event_type: Default::default(),
                                    lane: self.lane.name.clone(),
                                    recovery: None,
                                    run_id: run_id.clone(),
                                    message: LiveAgentMessage::Settled(message.message.clone()),
                                })),
                            );
                            events.enqueue(
                                HarnessEventType::MessageEnd,
                                &self.lane.name,
                                HarnessEvent::MessageEnd(Box::new(MessageEndEvent {
                                    event_type: Default::default(),
                                    lane: self.lane.name.clone(),
                                    recovery: None,
                                    run_id,
                                    message: message.message.clone(),
                                    entry_id: Some(id.clone()),
                                })),
                            );
                        }
                        if streamed {
                            let old = self
                                .lane
                                .streaming
                                .lock()
                                .unwrap_or_else(|p| p.into_inner())
                                .take();
                            drop(old);
                        }
                    }
                    events.enqueue(
                        HarnessEventType::EntryAdded,
                        &self.lane.name,
                        HarnessEvent::EntryAdded(EntryAddedEvent {
                            event_type: Default::default(),
                            lane: self.lane.name.clone(),
                            recovery: None,
                            entry,
                        }),
                    );
                }
                Publication::Usage(id) => {
                    let (index, row) = usage_writes
                        .iter()
                        .find(|(_, row)| row.id == id)
                        .ok_or_else(|| {
                            self.publication_fault(raw, "usage publication row missing")
                        })?;
                    let seq = *commit.seqs.get(*index).ok_or_else(|| {
                        self.publication_fault(raw, "usage publication sequence missing")
                    })?;
                    let totals = self
                        .owner
                        .session
                        .get_stats()
                        .await
                        .map_err(|_| self.publication_fault(raw, "usage totals read failed"))?
                        .usage;
                    events.enqueue(
                        HarnessEventType::Usage,
                        &self.lane.name,
                        HarnessEvent::Usage(UsageEvent {
                            event_type: Default::default(),
                            lane: self.lane.name.clone(),
                            row: UsageRow {
                                id: row.id.clone(),
                                seq,
                                usage: row.usage.clone(),
                                entry_id: row.entry_id.clone(),
                                adjustment: row.adjustment,
                                details: row.details.clone(),
                            },
                            totals,
                        }),
                    );
                }
                Publication::Terminal(result) => {
                    let operation_id = operation_id.clone().ok_or_else(|| {
                        self.publication_fault(raw, "terminal publication owner missing")
                    })?;
                    if matches!(*result, OperationResult::Run(_)) {
                        events.enqueue(
                            HarnessEventType::RunEnd,
                            &self.lane.name,
                            run_end(self.lane.name.clone(), operation_id, *result)?,
                        );
                    } else {
                        let current = previous.as_ref().ok_or_else(|| {
                            self.publication_fault(raw, "structural terminal owner missing")
                        })?;
                        let (kind, event) = crate::structural::end_event(
                            &current.operation,
                            crate::structural::reason(current),
                            *result,
                            false,
                        )?;
                        events.enqueue(kind, &self.lane.name, event);
                    }
                }
            }
        }
        let old = std::mem::replace(
            &mut *self.lane.hydrated.lock().unwrap_or_else(|p| p.into_inner()),
            loaded.clone(),
        );
        if crate::inbox::queue_signature(&old) != crate::inbox::queue_signature(&loaded) {
            self.queue_event(&loaded, raw)?;
        }
        drop(old);
        Ok(Some(loaded))
    }
}
/// Exact predicate for a codec-checked observed register.
pub(crate) fn observed(register: &Register) -> RegisterCondition {
    RegisterCondition {
        register: register_key(register),
        expected: ExpectedRegister::Seq(register_seq(register)),
    }
}

pub(crate) fn run_end(
    lane: LaneId,
    run_id: OperationId,
    result: OperationResult,
) -> Result<HarnessEvent, HarnessRuntimeError> {
    let OperationResult::Run(outcome) = result else {
        return Err(crate::restore::fault(
            "terminal event requires a later stage",
        ));
    };
    Ok(match *outcome {
        RunOutcome::CompletedWithAssistant(o) => {
            HarnessEvent::RunEndCompletedWithFinal(Box::new(RunEndCompletedWithFinalEvent {
                event_type: Default::default(),
                lane,
                recovery: None,
                run_id,
                leaf_id: Some(o.leaf_id),
                outcome: Default::default(),
                final_entry_id: o.final_entry_id,
                final_message: o.final_message,
            }))
        }
        RunOutcome::CompletedWithoutAssistant(o) => {
            HarnessEvent::RunEndCompletedWithoutFinal(RunEndCompletedWithoutFinalEvent {
                event_type: Default::default(),
                lane,
                recovery: None,
                run_id,
                leaf_id: Some(o.leaf_id),
                outcome: Default::default(),
            })
        }
        RunOutcome::AbortedWithAssistant(o) => {
            HarnessEvent::RunEndAbortedWithFinal(Box::new(RunEndAbortedWithFinalEvent {
                event_type: Default::default(),
                lane,
                recovery: None,
                run_id,
                leaf_id: Some(o.leaf_id),
                outcome: Default::default(),
                final_entry_id: o.final_entry_id,
                final_message: o.final_message,
            }))
        }
        RunOutcome::AbortedWithoutAssistant(o) => {
            HarnessEvent::RunEndAbortedWithoutFinal(RunEndAbortedWithoutFinalEvent {
                event_type: Default::default(),
                lane,
                recovery: None,
                run_id,
                leaf_id: Some(o.leaf_id),
                outcome: Default::default(),
            })
        }
        RunOutcome::FailedWithAssistant(o) => {
            HarnessEvent::RunEndFailedWithFinal(Box::new(RunEndFailedWithFinalEvent {
                event_type: Default::default(),
                lane,
                recovery: None,
                run_id,
                leaf_id: Some(o.leaf_id),
                outcome: Default::default(),
                final_entry_id: o.final_entry_id,
                final_message: o.final_message,
                error: o.error,
            }))
        }
        RunOutcome::FailedWithoutAssistant(o) => {
            HarnessEvent::RunEndFailedWithoutFinal(RunEndFailedWithoutFinalEvent {
                event_type: Default::default(),
                lane,
                recovery: None,
                run_id,
                leaf_id: Some(o.leaf_id),
                outcome: Default::default(),
                error: o.error,
            })
        }
        RunOutcome::Deferred(_) | RunOutcome::MissingIdentities(_) => {
            return Err(crate::restore::fault(
                "suspension cannot publish terminal event",
            ));
        }
    })
}

impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>
    RuntimeLane<C, S, M>
{
    /// Detached ephemeral lifecycle only. The caller releases its lines before
    /// delivering these events, then uses the unchanged CAS recipe for commit.
    pub(crate) fn precommit_messages(
        &self,
        recipe: &CommitRecipe<Telemetry>,
        operation: &OperationId,
        recovery: bool,
    ) -> Vec<(HarnessEventType, HarnessEvent)> {
        let streaming = self
            .lane
            .streaming
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .map(|(id, _)| id.clone());
        let mut events = Vec::new();
        for write in &recipe.transaction.writes {
            let Write::Entry(NewEntry::Message(entry)) = write else {
                continue;
            };
            if streaming.as_ref() == Some(&entry.id) {
                continue;
            }
            events.push((
                HarnessEventType::MessageStart,
                HarnessEvent::MessageStart(Box::new(MessageStartEvent {
                    event_type: Default::default(),
                    lane: self.lane.name.clone(),
                    recovery: recovery.then_some(pi_ai_core::LiteralTrue),
                    run_id: Some(operation.clone()),
                    message: LiveAgentMessage::Settled(entry.message.clone()),
                })),
            ));
            events.push((
                HarnessEventType::MessageEnd,
                HarnessEvent::MessageEnd(Box::new(MessageEndEvent {
                    event_type: Default::default(),
                    lane: self.lane.name.clone(),
                    recovery: recovery.then_some(pi_ai_core::LiteralTrue),
                    run_id: Some(operation.clone()),
                    message: entry.message.clone(),
                    entry_id: Some(entry.id.clone()),
                })),
            ));
        }
        events
    }
}

#[cfg(test)]
mod held_publication_tests {
    use super::HeldPublication;
    use crate::scheduler::{Admission, MutationLine};
    use std::{
        future::Future,
        pin::Pin,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        task::{Context, Poll, Wake, Waker},
    };

    fn poll<F: Future + ?Sized>(future: Pin<&mut F>, waker: &Waker) -> Poll<F::Output> {
        future.poll(&mut Context::from_waker(waker))
    }

    struct CheckDrain {
        settings: MutationLine,
        lane: MutationLine,
        called: Arc<AtomicBool>,
    }
    impl Wake for CheckDrain {
        fn wake(self: Arc<Self>) {
            assert!(!self.settings.held_for_test());
            assert!(!self.lane.held_for_test());
            self.called.store(true, Ordering::SeqCst);
        }
    }

    #[test]
    fn native_held_publication_releases_lines_before_last_commit_drain() {
        for exit in ["success", "refusal", "cancel", "unwind"] {
            let admission = Admission::new();
            let settings = MutationLine::new(admission.clone());
            let lane = MutationLine::new(admission.clone());
            let mut first = Box::pin(settings.acquire());
            let Poll::Ready(Ok(settings_permit)) = poll(first.as_mut(), Waker::noop()) else {
                panic!("settings permit")
            };
            let mut second = Box::pin(lane.acquire());
            let Poll::Ready(Ok(lane_permit)) = poll(second.as_mut(), Waker::noop()) else {
                panic!("lane permit")
            };
            let mut scope = HeldPublication::new(settings_permit, lane_permit);
            *scope.commit_slot() = Some(admission.try_admit().unwrap());
            let called = Arc::new(AtomicBool::new(false));
            let waker = Waker::from(Arc::new(CheckDrain {
                settings: settings.clone(),
                lane: lane.clone(),
                called: called.clone(),
            }));
            let mut drained = Box::pin(admission.drained());
            assert!(poll(drained.as_mut(), &waker).is_pending());
            admission.seal_close();
            match exit {
                "success" => scope.release(),
                "refusal" => drop(scope),
                "cancel" => {
                    let mut pending = Box::pin(async move {
                        let _scope = scope;
                        std::future::pending::<()>().await;
                    });
                    assert!(poll(pending.as_mut(), Waker::noop()).is_pending());
                    drop(pending);
                }
                "unwind" => {
                    assert!(
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            let _scope = scope;
                            panic!("publication caller unwound");
                        }))
                        .is_err()
                    );
                }
                _ => unreachable!(),
            }
            assert!(called.load(Ordering::SeqCst));
            assert!(poll(drained.as_mut(), Waker::noop()).is_ready());
        }
    }
}
