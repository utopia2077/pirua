//! Lane-bound tree reads, immediate global facts and conditional idle appends.
use crate::{
    codec::{invalid, schema, storage},
    session::{RuntimeSession, admission_error},
    storage::*,
};
use pi_agent_schema::session_values::*;
use pi_agent_schema::*;

impl<S: CheckedStorage + 'static, M: SessionMetadataRecord> RuntimeSession<S, M> {
    pub(crate) async fn lane_state(
        &self,
    ) -> Result<(Option<EntryId>, SafeUnsigned, LaneState, SafeUnsigned), SessionError> {
        let state = self
            .get_register(RegisterNamespace::LaneState, self.lane.as_str())
            .await?;
        let leaf = self
            .get_register(RegisterNamespace::LaneLeaf, self.lane.as_str())
            .await?;
        match (leaf, state) {
            (
                Some(Register::LaneLeaf {
                    value: leaf,
                    seq: leaf_seq,
                    ..
                }),
                Some(Register::LaneState {
                    value: state,
                    seq: state_seq,
                    ..
                }),
            ) => Ok((leaf, leaf_seq, state, state_seq)),
            _ => Err(invalid()),
        }
    }
    async fn fact_write(&self, write: Write) -> Result<(), SessionError> {
        self.lane_state().await?;
        self.write(Transaction {
            writes: vec![write],
        })
        .await?;
        Ok(())
    }
    async fn append(
        &self,
        message: Option<AgentMessage>,
        custom: Option<(CustomType, Presence<JsonValue>)>,
    ) -> Result<EntryId, SessionError> {
        // Allocate only after observing a valid idle lane. Retain this reservation on CAS retry.
        let mut id = None;
        loop {
            let (leaf, leaf_seq, state, state_seq) = self.lane_state().await?;
            if state.current_operation_id.is_some() {
                return Err(invalid());
            }
            let id = match &id {
                Some(id) => id,
                None => id.insert(
                    EntryId::new(crate::id::mint(self.inner.ids.as_ref(), None)?)
                        .map_err(|_| schema())?,
                ),
            }
            .clone();
            let entry = if let Some(message) = &message {
                NewEntry::Message(NewMessageEntry {
                    id: id.clone(),
                    parent_id: leaf,
                    entry_type: MessageEntryType::Message,
                    message: message.clone(),
                    terminate: None,
                })
            } else {
                let (custom_type, data) = custom.as_ref().ok_or_else(invalid)?;
                NewEntry::Custom(NewCustomEntry {
                    id: id.clone(),
                    parent_id: leaf,
                    entry_type: CustomEntryType::Custom,
                    custom_type: custom_type.clone(),
                    data: data.clone(),
                })
            };
            let conditions = [
                (RegisterNamespace::LaneState, state_seq),
                (RegisterNamespace::LaneLeaf, leaf_seq),
            ]
            .into_iter()
            .map(|(namespace, seq)| RegisterCondition {
                register: RegisterKey {
                    namespace,
                    key: self.lane.as_str().to_owned(),
                },
                expected: ExpectedRegister::Seq(seq),
            })
            .collect();
            let outcome = self
                .commit_conditional(ConditionalCommit {
                    conditions,
                    transaction: Transaction {
                        writes: vec![
                            Write::Entry(entry),
                            Write::SetRegister(Box::new(RegisterSet::LaneLeaf {
                                key: self.lane.as_str().to_owned(),
                                value: Some(id.clone()),
                            })),
                        ],
                    },
                })
                .await
                .map_err(admission_error)?;
            if matches!(outcome, ConditionalOutcome::Applied(_)) {
                return Ok(id);
            }
        }
    }
}
fn delete(namespace: RegisterNamespace, key: String) -> Write {
    Write::DeleteRegister(RegisterDelete { namespace, key })
}
fn set(register: RegisterSet) -> Write {
    Write::SetRegister(Box::new(register))
}
impl<S: CheckedStorage + 'static, M: SessionMetadataRecord> SessionTree for RuntimeSession<S, M> {
    fn get_leaf_id(&self) -> BoxFuture<'_, Result<Option<EntryId>, SessionError>> {
        Box::pin(async move { Ok(self.lane_state().await?.0) })
    }
    fn get_entry(&self, id: EntryId) -> BoxFuture<'_, Result<Option<Entry>, SessionError>> {
        Box::pin(async move {
            self.lane_state().await?;
            Ok(self
                .get_entries(std::slice::from_ref(&id))
                .await?
                .remove(&id))
        })
    }
    fn get_stats(&self) -> BoxFuture<'_, Result<SessionStats, SessionError>> {
        Box::pin(async move {
            self.lane_state().await?;
            self.inner.storage.stats().await.map_err(storage)
        })
    }
    fn get_name(&self) -> BoxFuture<'_, Result<Option<String>, SessionError>> {
        Box::pin(async move {
            self.lane_state().await?;
            Ok(
                match self.get_register(RegisterNamespace::FactName, "").await? {
                    Some(Register::FactName { value, .. }) => Some(value),
                    _ => None,
                },
            )
        })
    }
    fn set_name(&self, name: Option<String>) -> BoxFuture<'_, Result<(), SessionError>> {
        Box::pin(self.fact_write(match name {
            Some(value) => set(RegisterSet::FactName {
                key: String::new(),
                value,
            }),
            None => delete(RegisterNamespace::FactName, String::new()),
        }))
    }
    fn get_label(&self, target_id: EntryId) -> BoxFuture<'_, Result<Option<String>, SessionError>> {
        Box::pin(async move {
            self.lane_state().await?;
            Ok(
                match self
                    .get_register(RegisterNamespace::FactLabel, target_id.as_str())
                    .await?
                {
                    Some(Register::FactLabel { value, .. }) => Some(value),
                    _ => None,
                },
            )
        })
    }
    fn set_label(
        &self,
        target_id: EntryId,
        label: Option<String>,
    ) -> BoxFuture<'_, Result<(), SessionError>> {
        Box::pin(self.fact_write(match label {
            Some(value) => set(RegisterSet::FactLabel {
                key: target_id.as_str().to_owned(),
                value,
            }),
            None => delete(RegisterNamespace::FactLabel, target_id.as_str().to_owned()),
        }))
    }
    fn get_custom_fact<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Option<JsonValue>, SessionError>> {
        Box::pin(async move {
            self.lane_state().await?;
            Ok(
                match self
                    .get_register(RegisterNamespace::FactCustom, key)
                    .await?
                {
                    Some(Register::FactCustom { value, .. }) => Some(value),
                    _ => None,
                },
            )
        })
    }
    fn set_custom_fact(
        &self,
        key: String,
        value: Option<JsonValue>,
    ) -> BoxFuture<'_, Result<(), SessionError>> {
        Box::pin(self.fact_write(match value {
            Some(value) => set(RegisterSet::FactCustom { key, value }),
            None => delete(RegisterNamespace::FactCustom, key),
        }))
    }
    fn find_entries(&self, query: EntryQuery) -> BoxFuture<'_, Result<Vec<Entry>, SessionError>> {
        Box::pin(async move {
            self.lane_state().await?;
            let order = query.order.unwrap_or(ScanOrder::Descending);
            let mut scan = EntryScan {
                entry_type: query.entry_type,
                custom_type: query.custom_type,
                order: Some(order),
                limit: query.limit,
                ..Default::default()
            };
            if let Some(cursor) = query.cursor {
                match order {
                    ScanOrder::Descending => {
                        let Some(value) = cursor.seq.get().checked_sub(1) else {
                            return Ok(vec![]);
                        };
                        scan.to_seq = Some(SafeUnsigned::new(value).map_err(|_| schema())?);
                    }
                    ScanOrder::Ascending => {
                        let Ok(value) = SafeUnsigned::new(cursor.seq.get() + 1) else {
                            return Ok(vec![]);
                        };
                        scan.from_seq = Some(value);
                    }
                }
            }
            let entries = self
                .inner
                .storage
                .scan_entries(scan)
                .await
                .map_err(storage)?;
            for entry in &entries {
                self.inner.codec.validate_entry(entry)?;
            }
            Ok(entries)
        })
    }
    fn find_entry(
        &self,
        mut query: EntryQuery,
    ) -> BoxFuture<'_, Result<Option<Entry>, SessionError>> {
        Box::pin(async move {
            query.limit = Some(query.limit.map_or(SafeUnsigned::new(1).expect("one"), |n| {
                n.min(SafeUnsigned::new(1).expect("one"))
            }));
            Ok(self.find_entries(query).await?.into_iter().next())
        })
    }
    fn find_entries_on_branch(
        &self,
        mut query: BranchScan,
    ) -> BoxFuture<'_, Result<Vec<Entry>, SessionError>> {
        Box::pin(async move {
            let leaf = self.get_leaf_id().await?;
            if query.start.is_none() {
                query.start = leaf;
            }
            if query.start.is_none() {
                return Ok(vec![]);
            }
            let entries = self
                .inner
                .storage
                .scan_branch(query)
                .await
                .map_err(storage)?;
            for entry in &entries {
                self.inner.codec.validate_entry(entry)?;
            }
            Ok(entries)
        })
    }
    fn find_entry_on_branch(
        &self,
        mut query: BranchScan,
    ) -> BoxFuture<'_, Result<Option<Entry>, SessionError>> {
        Box::pin(async move {
            query.limit = Some(query.limit.map_or(SafeUnsigned::new(1).expect("one"), |n| {
                n.min(SafeUnsigned::new(1).expect("one"))
            }));
            Ok(self.find_entries_on_branch(query).await?.into_iter().next())
        })
    }
    fn append_message(
        &self,
        message: AgentMessage,
    ) -> BoxFuture<'_, Result<EntryId, SessionError>> {
        Box::pin(self.append(Some(message), None))
    }
    fn append_custom_entry(
        &self,
        custom_type: CustomType,
        data: Presence<JsonValue>,
    ) -> BoxFuture<'_, Result<EntryId, SessionError>> {
        Box::pin(self.append(None, Some((custom_type, data))))
    }
}
