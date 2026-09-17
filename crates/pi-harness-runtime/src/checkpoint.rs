//! Detached projection over exact backend-prepared entries.
use crate::{effects::RuntimeEffects, owner::Telemetry, restore::HydratedLane};
use pi_agent_schema::*;
use pi_agent_session::storage::{PreparedEntries, PreparedEntrySlot, RuntimeStorage};
use pi_harness_core::planner::PlanningFacts;
use std::collections::BTreeSet;
pub(crate) struct InboxPreparation {
    pub current: CurrentOperation,
    pub facts: PlanningFacts<Telemetry>,
    pub entries: Option<PreparedEntries>,
}
impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>
    RuntimeEffects<C, S, M>
{
    pub(crate) async fn prepare_inbox(
        &self,
        observed: &HydratedLane,
    ) -> Result<Option<PlanningFacts<Telemetry>>, HarnessRuntimeError> {
        let session = &self.lane.owner.session;
        let before = session
            .read_stamp()
            .await
            .map_err(|_| self.lane.owner.fail("projection stamp failed"))?;
        let h = self.lane.load().await?;
        let after = session
            .read_stamp()
            .await
            .map_err(|_| self.lane.owner.fail("projection stamp failed"))?;
        let Some(current) = &h.current else {
            return Ok(None);
        };
        if before != after || h.current != observed.current || h.roots.leaf != observed.roots.leaf {
            return Ok(None);
        }
        let Some(selection) = pi_harness_core::inbox::select(current)
            .map_err(|_| self.lane.owner.fail("projection selection failed"))?
        else {
            return Ok(None);
        };
        let placements = crate::inbox::placements(&h, &selection.ids)?;
        let cancelled = matches!(&current.state,OperationState::Run(r) if matches!(r.control,Control::CancelRequested(_)));
        let mut projecting = BTreeSet::new();
        let custom=!cancelled && placements.iter().any(|p|matches!(p.entry(),NewEntry::Custom(e) if self.lane.owner.projectors.contains_key(&e.custom_type)));
        let prepared = if custom {
            let prefix = pi_harness_core::inbox::placement_writes(current, &placements)
                .map_err(|_| self.lane.owner.fail("projection prefix failed"))?;
            let slots = prefix
                .into_iter()
                .enumerate()
                .filter_map(|(write_index, w)| match w {
                    Write::Entry(entry) => Some(PreparedEntrySlot { write_index, entry }),
                    _ => None,
                })
                .collect();
            let Some(prepared) = session
                .prepare_entries(before, slots)
                .await
                .map_err(|_| self.lane.owner.fail("entry preparation failed"))?
            else {
                return Ok(None);
            };
            for entry in prepared.entries() {
                if let Entry::Custom(entry) = entry
                    && let Some(projector) = self.lane.owner.projectors.get(&entry.custom_type)
                {
                    let mut projection = Box::pin(async { projector.project(entry.clone()).await });
                    let mut closed = Box::pin(self.lane.owner.admission.seal_signal());
                    let result = std::future::poll_fn(|cx| {
                        if closed.as_mut().poll(cx).is_ready() {
                            return std::task::Poll::Ready(None);
                        }
                        projection.as_mut().poll(cx).map(Some)
                    })
                    .await;
                    let Some(result) = result else {
                        return Err(self
                            .lane
                            .owner
                            .admission
                            .check()
                            .expect_err("sealed projector"));
                    };
                    let result =
                        result.map_err(|_| self.lane.owner.fail("custom projector failed"))?;
                    if let Some(messages) = result {
                        for message in &messages {
                            session.codec().validate_message(message).map_err(|_| {
                                self.lane
                                    .owner
                                    .fail("custom projector returned invalid message")
                            })?;
                        }
                        if !messages.is_empty() {
                            projecting.insert(entry.id.clone());
                        }
                    }
                }
            }
            Some(prepared)
        } else {
            None
        };
        if !cancelled {
            for p in &placements {
                if let NewEntry::Message(e) = p.entry()
                    && (selection.kind != pi_harness_core::inbox::InboxKind::Writes
                        || pi_harness_core::inbox::message_projects(&e.message))
                {
                    projecting.insert(e.id.clone());
                }
            }
        }
        let facts = PlanningFacts::Inbox {
            session_id: session.metadata().base_metadata().id.clone(),
            telemetry_context: self.trace.context(),
            placements,
            projecting,
        };
        let previous = self
            .inbox_preparation
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .replace(InboxPreparation {
                current: current.clone(),
                facts: facts.clone(),
                entries: prepared,
            });
        drop(previous);
        Ok(Some(facts))
    }
}
