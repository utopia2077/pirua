//! Snapshots capture durable values and register an event buffer under publication order.
use crate::owner::*;
use pi_agent_schema::*;
use pi_agent_session::storage::RuntimeStorage;
use std::sync::Arc;
impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>
    RuntimeLane<C, S, M>
{
    pub(crate) async fn watch_lane(
        &self,
    ) -> Result<Arc<dyn WatchHandle<LaneSnapshot>>, HarnessRuntimeError> {
        let result = self.watch_lane_inner().await;
        self.owner.observed_watch(result)
    }
    async fn watch_lane_inner(
        &self,
    ) -> Result<Arc<dyn WatchHandle<LaneSnapshot>>, HarnessRuntimeError> {
        let _line = self.lane.line.acquire().await?;
        let h = crate::restore::hydrate_lane(
            self.owner.session.as_ref(),
            self.lane.name.clone(),
            &crate::scheduler::Admission::new(),
        )
        .await?;
        let Register::LaneLeaf { value: leaf_id, .. } = &h.roots.leaf else {
            return Err(self.owner.fail("invalid snapshot leaf"));
        };
        let Register::LaneState {
            value: lane_state, ..
        } = &h.roots.lane_state
        else {
            return Err(self.owner.fail("invalid snapshot state"));
        };
        let queued = |ids: &[EntryId]| -> Result<Vec<QueuedItem>, HarnessRuntimeError> {
            ids.iter()
                .map(|id| {
                    let Some(Register::PendingEntry { value, .. }) = h
                        .registers
                        .get(&(RegisterNamespace::PendingEntry, id.as_str().to_owned()))
                    else {
                        return Err(self.owner.fail("snapshot pending message missing"));
                    };
                    let pi_ai_core::Presence::Value(value) = &value.payload else {
                        return Err(self.owner.fail("snapshot pending payload missing"));
                    };
                    let message = serde_json::from_value(
                        serde_json::to_value(value)
                            .map_err(|_| self.owner.fail("snapshot payload conversion failed"))?,
                    )
                    .map_err(|_| self.owner.fail("snapshot message conversion failed"))?;
                    Ok(QueuedItem {
                        entry_id: id.clone(),
                        message,
                    })
                })
                .collect()
        };
        let mut queues = LaneQueuesSnapshot {
            next_run: queued(&lane_state.pending_next_run)?,
            ..Default::default()
        };
        let mut pending_writes = vec![];
        let operation = if let Some(current) = &h.current {
            let mut retry = None;
            if let OperationState::Run(run) = &current.state {
                queues.steer = queued(&run.inbox.steer)?;
                queues.follow_up = queued(&run.inbox.follow_up)?;
                for id in &run.inbox.writes {
                    let Some(Register::PendingEntry { value, .. }) = h
                        .registers
                        .get(&(RegisterNamespace::PendingEntry, id.as_str().into()))
                    else {
                        return Err(self.owner.fail("snapshot pending write missing"));
                    };
                    pending_writes.push(match value.entry_type {
                        PendingEntryType::Message => PendingWrite::Message(PendingMessageWrite {
                            entry_id: id.clone(),
                            entry_type: Default::default(),
                            message: queued(std::slice::from_ref(id))?.remove(0).message,
                        }),
                        PendingEntryType::Custom => PendingWrite::Custom(PendingCustomWrite {
                            entry_id: id.clone(),
                            entry_type: Default::default(),
                            custom_type: value.custom_type.clone(),
                            data: value.payload.clone(),
                        }),
                    });
                }
                if let RunPhase::Assistant(AssistantPhase {
                    generation:
                        Generation::RetryWait {
                            context,
                            next_attempt,
                            not_before,
                            ..
                        },
                    ..
                }) = &run.phase
                {
                    retry = Some(RetryInfo {
                        attempt: *next_attempt,
                        max_attempts: context.retry_policy.max_attempts,
                        next_attempt_at: *not_before,
                    });
                }
            }
            if let Some(StructuralDecision::Generating { generation, .. }) =
                pi_harness_core::structural::decision(current)
                && let SummaryGeneration::RetryWait {
                    context,
                    next_attempt,
                    not_before,
                    ..
                } = generation.as_ref()
            {
                retry = Some(RetryInfo {
                    attempt: *next_attempt,
                    max_attempts: context.retry_policy.max_attempts,
                    next_attempt_at: *not_before,
                });
            }
            Some(OperationSnapshot {
                id: current.operation.operation_id.clone(),
                kind: kind(current),
                status: if pi_harness_core::structural::is_cancelled(current) {
                    OperationSnapshotStatus::Aborting
                } else if *self.lane.driving.lock().unwrap_or_else(|p| p.into_inner()) {
                    OperationSnapshotStatus::Running
                } else {
                    OperationSnapshotStatus::Suspended
                },
                started_at: current.operation.started_at,
                suspended: if *self.lane.driving.lock().unwrap_or_else(|p| p.into_inner()) {
                    None
                } else {
                    self.suspended_snapshot(&h)
                },
                streaming_message: self
                    .lane
                    .streaming
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .as_ref()
                    .map(|(_, message)| message.clone()),
                running_tools: self
                    .lane
                    .running_tools
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .values()
                    .cloned()
                    .collect(),
                retry,
            })
        } else {
            None
        };
        let transcript = self
            .owner
            .session
            .lane_view(self.lane.name.clone())
            .find_entries_on_branch(BranchScan {
                stop_at_type: Some(EntryType::Compaction),
                ..Default::default()
            })
            .await
            .map_err(|_| self.owner.fail("snapshot transcript read failed"))?;
        let snapshot = LaneSnapshot {
            lane: self.lane.name.clone(),
            transcript,
            leaf_id: leaf_id.clone(),
            operation,
            queues,
            pending_writes,
            faulted: false,
        };
        Ok(self
            .owner
            .events
            .watch(snapshot, Some(self.lane.name.clone())))
    }
}
pub(crate) fn kind(current: &CurrentOperation) -> OperationKind {
    match current.operation.intent {
        OperationIntent::Run(_) => OperationKind::Run,
        OperationIntent::Compaction(_) => OperationKind::Compaction,
        OperationIntent::Navigation(_) => OperationKind::Navigation,
    }
}
