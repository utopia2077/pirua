//! Real conditional Session publication of typed core recipes.
use crate::{
    owner::{RuntimeLane, Telemetry},
    restore::{HydratedLane, hydrate_lane},
    scheduler::Admission,
};
use pi_agent_schema::*;
use pi_agent_session::{
    backend_support::{register_key, register_seq},
    storage::{
        ConditionalCommit, ConditionalOutcome, ExpectedRegister, RegisterCondition, RuntimeStorage,
    },
};
use pi_harness_core::planner::{CommitFacts, CommitRecipe, Publication};

impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>
    RuntimeLane<C, S, M>
{
    pub(crate) async fn load(&self) -> Result<HydratedLane, HarnessRuntimeError> {
        self.owner.observe(
            hydrate_lane(
                self.owner.session.as_ref(),
                self.lane.name.clone(),
                &self.owner.admission,
            )
            .await,
        )
    }
    pub(crate) fn facts(&self, loaded: &HydratedLane) -> Result<CommitFacts, HarnessRuntimeError> {
        Ok(CommitFacts {
            session_id: self.owner.session.metadata().base_metadata().id.clone(),
            leaf_seq: Some(register_seq(&loaded.roots.leaf)),
            meta_seq: register_seq(
                loaded
                    .roots
                    .operation_meta
                    .as_ref()
                    .ok_or_else(|| self.owner.fail("operation meta missing"))?,
            ),
        })
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
        let permit = self.owner.admission.try_admit()?;
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
        let request = ConditionalCommit {
            conditions,
            transaction: recipe.transaction,
        };
        let outcome = match prepared {
            Some(prepared) => {
                self.owner
                    .session
                    .commit_prepared_conditional_with_telemetry(
                        prepared,
                        request,
                        telemetry.as_ref(),
                    )
                    .await
            }
            None => {
                self.owner
                    .session
                    .commit_conditional_with_telemetry(request, telemetry.as_ref())
                    .await
            }
        }
        .map_err(|_| self.owner.fail("runtime conditional publication failed"))?;
        let ConditionalOutcome::Applied(commit) = outcome else {
            return Ok(None);
        };
        let loaded = hydrate_lane(
            self.owner.session.as_ref(),
            self.lane.name.clone(),
            &Admission::new(),
        )
        .await
        .map_err(|_| self.owner.fail("runtime committed state hydration failed"))?;
        if let Some(run_id) = &accepted_operation {
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
                        .map_err(|_| self.owner.fail("runtime committed entry read failed"))?;
                    let entry = entries
                        .get(&id)
                        .cloned()
                        .ok_or_else(|| self.owner.fail("runtime committed entry is absent"))?;
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
                        .ok_or_else(|| self.owner.fail("usage publication row missing"))?;
                    let seq = *commit
                        .seqs
                        .get(*index)
                        .ok_or_else(|| self.owner.fail("usage publication sequence missing"))?;
                    let totals = self
                        .owner
                        .session
                        .get_stats()
                        .await
                        .map_err(|_| self.owner.fail("usage totals read failed"))?
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
                    let operation_id = operation_id
                        .clone()
                        .ok_or_else(|| self.owner.fail("terminal publication owner missing"))?;
                    events.enqueue(
                        HarnessEventType::RunEnd,
                        &self.lane.name,
                        run_end(self.lane.name.clone(), operation_id, *result)?,
                    );
                }
            }
        }
        let old = std::mem::replace(
            &mut *self.lane.hydrated.lock().unwrap_or_else(|p| p.into_inner()),
            loaded.clone(),
        );
        if crate::inbox::queue_signature(&old) != crate::inbox::queue_signature(&loaded) {
            self.queue_event(&loaded)?;
        }
        drop(old);
        drop(permit);
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
