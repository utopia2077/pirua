//! Lane tree facade; supported writes join owner admission and mutation order.
use crate::owner::RuntimeLane;
use pi_agent_schema::*;
use pi_agent_session::storage::*;
use pi_ai_core::{JsonValue, Presence};
pub(crate) struct TreeView<C, S: RuntimeStorage, M: SessionMetadataRecord>(
    pub RuntimeLane<C, S, M>,
);
fn failure() -> SessionError {
    SessionError::InvalidOperation(
        ErrorContext::new("runtime tree operation failed").expect("static context"),
    )
}
enum Mutation {
    Fact(Write),
    Message(AgentMessage),
    Custom(CustomType, Presence<JsonValue>),
}
impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord> SessionTree
    for TreeView<C, S, M>
{
    fn get_leaf_id(&self) -> BoxFuture<'_, Result<Option<EntryId>, SessionError>> {
        Box::pin(async move {
            self.0.owner.admission.check().map_err(|_| failure())?;
            self.0
                .owner
                .session
                .lane_view(self.0.lane.name.clone())
                .get_leaf_id()
                .await
        })
    }
    fn get_entry(&self, id: EntryId) -> BoxFuture<'_, Result<Option<Entry>, SessionError>> {
        Box::pin(async move {
            self.0.owner.admission.check().map_err(|_| failure())?;
            self.0
                .owner
                .session
                .lane_view(self.0.lane.name.clone())
                .get_entry(id)
                .await
        })
    }
    fn get_stats(&self) -> BoxFuture<'_, Result<SessionStats, SessionError>> {
        Box::pin(async move {
            self.0.owner.admission.check().map_err(|_| failure())?;
            self.0
                .owner
                .session
                .lane_view(self.0.lane.name.clone())
                .get_stats()
                .await
        })
    }
    fn get_name(&self) -> BoxFuture<'_, Result<Option<String>, SessionError>> {
        Box::pin(async move {
            self.0.owner.admission.check().map_err(|_| failure())?;
            self.0
                .owner
                .session
                .lane_view(self.0.lane.name.clone())
                .get_name()
                .await
        })
    }
    fn set_name(&self, name: Option<String>) -> BoxFuture<'_, Result<(), SessionError>> {
        Box::pin(async move {
            let result = self
                .mutate(Mutation::Fact(match name {
                    Some(value) => Write::SetRegister(Box::new(RegisterSet::FactName {
                        key: String::new(),
                        value,
                    })),
                    None => Write::DeleteRegister(RegisterDelete {
                        namespace: RegisterNamespace::FactName,
                        key: String::new(),
                    }),
                }))
                .await?;
            let _ = result;
            Ok(())
        })
    }
    fn get_label(&self, target_id: EntryId) -> BoxFuture<'_, Result<Option<String>, SessionError>> {
        Box::pin(async move {
            self.0.owner.admission.check().map_err(|_| failure())?;
            self.0
                .owner
                .session
                .lane_view(self.0.lane.name.clone())
                .get_label(target_id)
                .await
        })
    }
    fn set_label(
        &self,
        target_id: EntryId,
        label: Option<String>,
    ) -> BoxFuture<'_, Result<(), SessionError>> {
        Box::pin(async move {
            let result = self
                .mutate(Mutation::Fact(match label {
                    Some(value) => Write::SetRegister(Box::new(RegisterSet::FactLabel {
                        key: target_id.as_str().into(),
                        value,
                    })),
                    None => Write::DeleteRegister(RegisterDelete {
                        namespace: RegisterNamespace::FactLabel,
                        key: target_id.as_str().into(),
                    }),
                }))
                .await?;
            let _ = result;
            Ok(())
        })
    }
    fn get_custom_fact<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Option<JsonValue>, SessionError>> {
        Box::pin(async move {
            self.0.owner.admission.check().map_err(|_| failure())?;
            self.0
                .owner
                .session
                .lane_view(self.0.lane.name.clone())
                .get_custom_fact(key)
                .await
        })
    }
    fn set_custom_fact(
        &self,
        key: String,
        value: Option<JsonValue>,
    ) -> BoxFuture<'_, Result<(), SessionError>> {
        Box::pin(async move {
            let result = self
                .mutate(Mutation::Fact(match value {
                    Some(value) => {
                        Write::SetRegister(Box::new(RegisterSet::FactCustom { key, value }))
                    }
                    None => Write::DeleteRegister(RegisterDelete {
                        namespace: RegisterNamespace::FactCustom,
                        key,
                    }),
                }))
                .await?;
            let _ = result;
            Ok(())
        })
    }
    fn find_entries(&self, query: EntryQuery) -> BoxFuture<'_, Result<Vec<Entry>, SessionError>> {
        Box::pin(async move {
            self.0.owner.admission.check().map_err(|_| failure())?;
            self.0
                .owner
                .session
                .lane_view(self.0.lane.name.clone())
                .find_entries(query)
                .await
        })
    }
    fn find_entry(&self, query: EntryQuery) -> BoxFuture<'_, Result<Option<Entry>, SessionError>> {
        Box::pin(async move {
            self.0.owner.admission.check().map_err(|_| failure())?;
            self.0
                .owner
                .session
                .lane_view(self.0.lane.name.clone())
                .find_entry(query)
                .await
        })
    }
    fn find_entries_on_branch(
        &self,
        query: BranchScan,
    ) -> BoxFuture<'_, Result<Vec<Entry>, SessionError>> {
        Box::pin(async move {
            self.0.owner.admission.check().map_err(|_| failure())?;
            self.0
                .owner
                .session
                .lane_view(self.0.lane.name.clone())
                .find_entries_on_branch(query)
                .await
        })
    }
    fn find_entry_on_branch(
        &self,
        query: BranchScan,
    ) -> BoxFuture<'_, Result<Option<Entry>, SessionError>> {
        Box::pin(async move {
            self.0.owner.admission.check().map_err(|_| failure())?;
            self.0
                .owner
                .session
                .lane_view(self.0.lane.name.clone())
                .find_entry_on_branch(query)
                .await
        })
    }
    fn append_message(
        &self,
        message: AgentMessage,
    ) -> BoxFuture<'_, Result<EntryId, SessionError>> {
        Box::pin(async move {
            let result = self.mutate(Mutation::Message(message)).await?;
            result.ok_or_else(failure)
        })
    }
    fn append_custom_entry(
        &self,
        custom_type: CustomType,
        data: pi_ai_core::Presence<JsonValue>,
    ) -> BoxFuture<'_, Result<EntryId, SessionError>> {
        Box::pin(async move {
            let result = self.mutate(Mutation::Custom(custom_type, data)).await?;
            result.ok_or_else(failure)
        })
    }
}
impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>
    TreeView<C, S, M>
{
    async fn mutate(&self, mutation: Mutation) -> Result<Option<EntryId>, SessionError> {
        let lane = self.0.clone();
        lane.owner
            .clone()
            .retain(Box::pin(async move {
                let _line = lane.lane.line.acquire().await.map_err(|_| failure())?;
                let mut reserved = None;
                loop {
                    let h = lane.load().await.map_err(|_| failure())?;
                    let Register::LaneLeaf { value: leaf, .. } = h.roots.leaf.clone() else {
                        return Err(failure());
                    };
                    let mut writes = vec![];
                    match &mutation {
                        Mutation::Fact(write) => writes.push(write.clone()),
                        Mutation::Message(_) | Mutation::Custom(_, _) => {
                            if h.current.is_some() {
                                let _ = lane
                                    .owner
                                    .fail("active tree writes require a later runtime stage");
                                return Err(failure());
                            }
                            if reserved.is_none() {
                                reserved = Some(
                                    EntryId::new(lane.owner.ids.next(None))
                                        .map_err(|_| failure())?,
                                );
                            }
                            let id = reserved.clone().ok_or_else(failure)?;
                            let entry = match &mutation {
                                Mutation::Message(message) => NewEntry::Message(NewMessageEntry {
                                    id: id.clone(),
                                    parent_id: leaf,
                                    entry_type: MessageEntryType::Message,
                                    message: message.clone(),
                                    terminate: None,
                                }),
                                Mutation::Custom(custom_type, data) => {
                                    NewEntry::Custom(NewCustomEntry {
                                        id: id.clone(),
                                        parent_id: leaf,
                                        entry_type: CustomEntryType::Custom,
                                        custom_type: custom_type.clone(),
                                        data: data.clone(),
                                    })
                                }
                                Mutation::Fact(_) => unreachable!(),
                            };
                            writes.push(Write::Entry(entry));
                            writes.push(Write::SetRegister(Box::new(RegisterSet::LaneLeaf {
                                key: lane.lane.name.as_str().into(),
                                value: Some(id),
                            })));
                        }
                    }
                    let permit = lane.owner.admission.try_admit().map_err(|_| failure())?;
                    let result = lane
                        .owner
                        .session
                        .commit_conditional_with_telemetry(
                            ConditionalCommit {
                                conditions: vec![
                                    crate::publication::observed(&h.roots.lane_state),
                                    crate::publication::observed(&h.roots.leaf),
                                ],
                                transaction: Transaction { writes },
                            },
                            lane.owner.telemetry.as_ref(),
                        )
                        .await
                        .map_err(|_| {
                            let _ = lane.owner.fail("runtime tree publication failed");
                            failure()
                        })?;
                    if matches!(result, ConditionalOutcome::NotApplied) {
                        continue;
                    }
                    if let Some(id) = &reserved {
                        let entry = lane
                            .owner
                            .session
                            .get_entries(std::slice::from_ref(id))
                            .await?
                            .remove(id)
                            .ok_or_else(failure)?;
                        lane.owner.events.enqueue(
                            HarnessEventType::EntryAdded,
                            &lane.lane.name,
                            HarnessEvent::EntryAdded(EntryAddedEvent {
                                event_type: Default::default(),
                                lane: lane.lane.name.clone(),
                                recovery: None,
                                entry,
                            }),
                        );
                    }
                    drop(permit);
                    drop(_line);
                    lane.owner.events.flush();
                    lane.lane.announce();
                    return Ok(reserved);
                }
            }))
            .await
            .map_err(|_| failure())?
    }
}
