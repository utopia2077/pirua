//! Public queue admission and detached inbox preparation.
use crate::{owner::*, restore::HydratedLane};
use pi_agent_schema::*;
use pi_agent_session::{backend_support::register_seq, storage::RuntimeStorage};
use pi_ai_core::Presence;
use pi_harness_core::planner::{CommitRecipe, Publication, RegisterCondition};
#[cfg(feature = "extensions")]
use std::sync::Arc;

#[derive(Clone, Copy)]
pub(crate) enum QueueKind {
    Next,
    Steer,
    FollowUp,
}
pub(crate) fn closed() -> ClosedError {
    ClosedError {
        tag: Default::default(),
        message: "Harness is closed".into(),
    }
}
pub(crate) fn observed(r: &Register) -> RegisterCondition {
    let k = pi_agent_session::backend_support::register_key(r);
    RegisterCondition {
        namespace: k.namespace,
        key: k.key,
        seq: Some(register_seq(r)),
    }
}
pub(crate) fn recipe<C: Clone>(h: &HydratedLane, context: &C) -> CommitRecipe<C> {
    let mut expected = vec![observed(&h.roots.lane_state), observed(&h.roots.leaf)];
    for r in [&h.roots.operation_meta, &h.roots.operation_state]
        .into_iter()
        .flatten()
    {
        expected.push(observed(r));
    }
    CommitRecipe {
        expected,
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
pub(crate) fn payload(message: &AgentMessage) -> Result<PendingEntry, HarnessRuntimeError> {
    Ok(PendingEntry {
        entry_type: PendingEntryType::Message,
        custom_type: None,
        payload: Presence::Value(
            serde_json::from_value(
                serde_json::to_value(message)
                    .map_err(|_| crate::restore::fault("message encoding failed"))?,
            )
            .map_err(|_| crate::restore::fault("message encoding failed"))?,
        ),
    })
}
pub(crate) fn message(p: &PendingEntry) -> Result<AgentMessage, HarnessRuntimeError> {
    let Presence::Value(v) = &p.payload else {
        return Err(crate::restore::fault("pending message payload missing"));
    };
    serde_json::from_value(
        serde_json::to_value(v)
            .map_err(|_| crate::restore::fault("pending message conversion failed"))?,
    )
    .map_err(|_| crate::restore::fault("pending message conversion failed"))
}
pub(crate) fn placements(
    h: &HydratedLane,
    ids: &[EntryId],
) -> Result<Vec<PendingPlacement>, HarnessRuntimeError> {
    let mut parent = match &h.roots.leaf {
        Register::LaneLeaf { value, .. } => value.clone(),
        _ => return Err(crate::restore::fault("invalid lane leaf")),
    };
    ids.iter()
        .map(|id| {
            let Some(Register::PendingEntry { value, seq, .. }) = h
                .registers
                .get(&(RegisterNamespace::PendingEntry, id.as_str().into()))
            else {
                return Err(crate::restore::fault("pending payload missing"));
            };
            let entry = match value.entry_type {
                PendingEntryType::Message => NewEntry::Message(NewMessageEntry {
                    id: id.clone(),
                    parent_id: parent.clone(),
                    entry_type: Default::default(),
                    message: message(value)?,
                    terminate: None,
                }),
                PendingEntryType::Custom => NewEntry::Custom(NewCustomEntry {
                    id: id.clone(),
                    parent_id: parent.clone(),
                    entry_type: Default::default(),
                    custom_type: value
                        .custom_type
                        .clone()
                        .ok_or_else(|| crate::restore::fault("custom write type missing"))?,
                    data: value.payload.clone(),
                }),
            };
            parent = Some(id.clone());
            PendingPlacement::new(entry, value.clone(), *seq)
                .map_err(|_| crate::restore::fault("pending placement mismatch"))
        })
        .collect()
}
impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>
    RuntimeLane<C, S, M>
{
    pub(crate) fn queue_event(
        &self,
        h: &HydratedLane,
        raw: bool,
    ) -> Result<(), HarnessRuntimeError> {
        let fault = |message| {
            if raw {
                crate::restore::fault(message)
            } else {
                self.owner.fail(message)
            }
        };
        let queued = |ids: &[EntryId]| {
            ids.iter()
                .map(|id| {
                    let Some(Register::PendingEntry { value, .. }) = h
                        .registers
                        .get(&(RegisterNamespace::PendingEntry, id.as_str().into()))
                    else {
                        return Err(fault("queue payload missing"));
                    };
                    Ok(QueuedItem {
                        entry_id: id.clone(),
                        message: message(value)?,
                    })
                })
                .collect::<Result<Vec<_>, _>>()
        };
        let Register::LaneState { value, .. } = &h.roots.lane_state else {
            return Err(fault("queue lane state missing"));
        };
        let (steer, follow_up) = match h.current.as_ref().map(|c| &c.state) {
            Some(OperationState::Run(r)) => (queued(&r.inbox.steer)?, queued(&r.inbox.follow_up)?),
            _ => (vec![], vec![]),
        };
        self.owner.events.enqueue(
            HarnessEventType::QueueUpdate,
            &self.lane.name,
            HarnessEvent::QueueUpdate(QueueUpdateEvent {
                event_type: Default::default(),
                lane: self.lane.name.clone(),
                recovery: None,
                steer,
                follow_up,
                next_run: queued(&value.pending_next_run)?,
            }),
        );
        Ok(())
    }
    pub(crate) async fn queue_input(&self, message: AgentMessage, kind: QueueKind) -> QueueCall {
        self.queue_input_guarded(
            message,
            kind,
            #[cfg(feature = "extensions")]
            None,
        )
        .await
    }
    pub(crate) async fn queue_input_guarded(
        &self,
        message: AgentMessage,
        kind: QueueKind,
        #[cfg(feature = "extensions")] guard: Option<
            Arc<crate::extensions::profiles::actions::ActionGuard>,
        >,
    ) -> QueueCall {
        if let Err(e) = self.owner.admission.check() {
            return match e {
                HarnessRuntimeError::Closed(_) => Ok(Err(QueueExpectedError::Closed(closed()))),
                e => Err(e),
            };
        }
        if self
            .owner
            .session
            .codec()
            .validate_message(&message)
            .is_err()
        {
            return Ok(Err(QueueExpectedError::InvalidMessage(
                InvalidMessageError {
                    tag: Default::default(),
                    message: "invalid queue message".into(),
                    lane: self.lane.name.clone(),
                    reason: "invalid queue message".into(),
                },
            )));
        }
        let lane = self.clone();
        self.owner
            .clone()
            .retain_runtime(Box::pin(async move {
                #[cfg(feature = "extensions")]
                let held = if let Some(guard) = &guard {
                    let line = lane.lane.line.acquire().await?;
                    let h = lane.load().await?;
                    if !guard.check(&h) {
                        return Ok(Err(QueueExpectedError::NoActiveRun(NoActiveRunError {
                            tag: Default::default(),
                            message: "parent is no longer current".into(),
                            lane: lane.lane.name.clone(),
                        })));
                    }
                    Some(line)
                } else {
                    None
                };
                let pending = payload(&message)?;
                let id = EntryId::new(lane.owner.ids.next(None))
                    .map_err(|_| lane.owner.fail("queue identity failed"))?;
                #[cfg(feature = "extensions")]
                let line = match held {
                    Some(line) => line,
                    None => lane.lane.line.acquire().await?,
                };
                #[cfg(not(feature = "extensions"))]
                let line = lane.lane.line.acquire().await?;
                loop {
                    let h = lane.load().await?;
                    #[cfg(feature = "extensions")]
                    if guard.as_ref().is_some_and(|guard| !guard.check(&h)) {
                        return Ok(Err(QueueExpectedError::NoActiveRun(NoActiveRunError {
                            tag: Default::default(),
                            message: "parent is no longer current".into(),
                            lane: lane.lane.name.clone(),
                        })));
                    }
                    let mut r = recipe(&h, &lane.owner.telemetry);
                    r.expected.push(RegisterCondition {
                        namespace: RegisterNamespace::PendingEntry,
                        key: id.as_str().into(),
                        seq: None,
                    });
                    r.transaction.writes.push(Write::SetRegister(Box::new(
                        RegisterSet::PendingEntry {
                            key: id.as_str().into(),
                            value: pending.clone(),
                        },
                    )));
                    match kind {
                        QueueKind::Next => {
                            let Register::LaneState { mut value, .. } = h.roots.lane_state.clone()
                            else {
                                return Err(lane.owner.fail("queue state missing"));
                            };
                            value.pending_next_run.push(id.clone());
                            r.transaction.writes.push(Write::SetRegister(Box::new(
                                RegisterSet::LaneState {
                                    key: lane.lane.name.as_str().into(),
                                    value,
                                },
                            )));
                        }
                        _ => {
                            let Some(CurrentOperation {
                                state: OperationState::Run(mut run),
                                operation,
                                ..
                            }) = h.current.clone()
                            else {
                                return Ok(Err(QueueExpectedError::NoActiveRun(
                                    NoActiveRunError {
                                        tag: Default::default(),
                                        message: "lane has no active run".into(),
                                        lane: lane.lane.name.clone(),
                                    },
                                )));
                            };
                            if !matches!(run.control, Control::Running(_)) {
                                return Ok(Err(QueueExpectedError::NoActiveRun(
                                    NoActiveRunError {
                                        tag: Default::default(),
                                        message: "run is cancelling".into(),
                                        lane: lane.lane.name.clone(),
                                    },
                                )));
                            }
                            match kind {
                                QueueKind::Steer => run.inbox.steer.push(id.clone()),
                                QueueKind::FollowUp => run.inbox.follow_up.push(id.clone()),
                                QueueKind::Next => unreachable!(),
                            };
                            r.transaction.writes.push(Write::SetRegister(Box::new(
                                RegisterSet::OperationState {
                                    key: operation.operation_id.as_str().into(),
                                    value: Box::new(OperationState::Run(run)),
                                },
                            )));
                        }
                    }
                    if let Some(applied) = lane.apply(r).await? {
                        let _ = applied;
                        drop(line);
                        lane.owner.events.flush();
                        lane.lane.announce();
                        return Ok(Ok(QueueAdmission { entry_id: id }));
                    }
                }
            }))
            .await
    }
    pub(crate) async fn next_input(&self, message: AgentMessage) -> NextRunCall {
        match self.queue_input(message, QueueKind::Next).await? {
            Ok(v) => Ok(Ok(v)),
            Err(QueueExpectedError::Closed(e)) => Ok(Err(NextRunExpectedError::Closed(e))),
            Err(QueueExpectedError::InvalidMessage(e)) => {
                Ok(Err(NextRunExpectedError::InvalidMessage(e)))
            }
            Err(QueueExpectedError::NoActiveRun(_)) => {
                Err(self.owner.fail("next-run queue unexpectedly requires run"))
            }
        }
    }
    pub(crate) async fn cancel_input(&self, id: EntryId) -> CancelQueuedCall {
        if let Err(e) = self.owner.admission.check() {
            return match e {
                HarnessRuntimeError::Closed(_) => Ok(Err(ClosedExpectedError(closed()))),
                e => Err(e),
            };
        }
        let lane = self.clone();
        self.owner
            .clone()
            .retain_runtime(Box::pin(async move {
                let line = lane.lane.line.acquire().await?;
                loop {
                    let h = lane.load().await?;
                    let mut r = recipe(&h, &lane.owner.telemetry);
                    let Register::LaneState { mut value, .. } = h.roots.lane_state.clone() else {
                        return Err(lane.owner.fail("queue state missing"));
                    };
                    let mut found = value.pending_next_run.contains(&id);
                    if found {
                        value.pending_next_run.retain(|v| v != &id);
                        r.transaction.writes.push(Write::SetRegister(Box::new(
                            RegisterSet::LaneState {
                                key: lane.lane.name.as_str().into(),
                                value,
                            },
                        )));
                    } else if let Some(CurrentOperation {
                        state: OperationState::Run(mut run),
                        operation,
                        ..
                    }) = h.current.clone()
                    {
                        found = run.inbox.steer.contains(&id) || run.inbox.follow_up.contains(&id);
                        if found {
                            run.inbox.steer.retain(|v| v != &id);
                            run.inbox.follow_up.retain(|v| v != &id);
                            r.transaction.writes.push(Write::SetRegister(Box::new(
                                RegisterSet::OperationState {
                                    key: operation.operation_id.as_str().into(),
                                    value: Box::new(OperationState::Run(run)),
                                },
                            )));
                        }
                    }
                    if !found {
                        let entries = lane
                            .owner
                            .session
                            .get_entries(std::slice::from_ref(&id))
                            .await
                            .map_err(|_| lane.owner.fail("cancel queue entry read failed"))?;
                        return Ok(Ok(if entries.contains_key(&id) {
                            CancelQueuedOutcome::AlreadyConsumed
                        } else {
                            CancelQueuedOutcome::NotFound
                        }));
                    }
                    let pending = h
                        .registers
                        .get(&(RegisterNamespace::PendingEntry, id.as_str().into()))
                        .ok_or_else(|| lane.owner.fail("cancel queue pending payload missing"))?;
                    r.expected.push(observed(pending));
                    r.transaction
                        .writes
                        .push(Write::DeleteRegister(RegisterDelete {
                            namespace: RegisterNamespace::PendingEntry,
                            key: id.as_str().into(),
                        }));
                    if let Some(applied) = lane.apply(r).await? {
                        let _ = applied;
                        drop(line);
                        lane.owner.events.flush();
                        lane.lane.announce();
                        return Ok(Ok(CancelQueuedOutcome::Cancelled));
                    }
                }
            }))
            .await
    }
    pub(crate) async fn adjustment(&self, request: RecordUsageRequest) -> RecordUsageCall {
        if let Err(e) = self.owner.admission.check() {
            return match e {
                HarnessRuntimeError::Closed(_) => Ok(Err(ClosedExpectedError(closed()))),
                e => Err(e),
            };
        }
        let lane = self.clone();
        self.owner
            .clone()
            .retain_runtime(Box::pin(async move {
                let line = lane.lane.line.acquire().await?;
                loop {
                    let h = lane.load().await?;
                    let mut r = recipe(&h, &lane.owner.telemetry);
                    let id = UsageId::new(lane.owner.ids.next(None))
                        .map_err(|_| lane.owner.fail("adjustment identity failed"))?;
                    r.transaction.writes.push(Write::Usage(NewUsageRow {
                        id: id.clone(),
                        usage: request.usage.clone(),
                        entry_id: request.options.entry_id.clone(),
                        adjustment: true,
                        details: request.options.details.clone(),
                    }));
                    r.publication.push(Publication::Usage(id.clone()));
                    if lane.apply(r).await?.is_some() {
                        drop(line);
                        lane.owner.events.flush();
                        lane.lane.announce();
                        return Ok(Ok(RecordUsageOutcome { usage_id: id }));
                    }
                }
            }))
            .await
    }
}

pub(crate) fn queue_signature(h: &HydratedLane) -> (Vec<EntryId>, Vec<EntryId>, Vec<EntryId>) {
    let next = match &h.roots.lane_state {
        Register::LaneState { value, .. } => value.pending_next_run.clone(),
        _ => vec![],
    };
    match h.current.as_ref().map(|c| &c.state) {
        Some(OperationState::Run(r)) => (r.inbox.steer.clone(), r.inbox.follow_up.clone(), next),
        _ => (vec![], vec![], next),
    }
}
