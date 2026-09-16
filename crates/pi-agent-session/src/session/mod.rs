//! Generic Session ownership, checked admission and outcome-linked observation.
use crate::{
    codec::{PendingPublication, SessionCodec, schema, storage},
    storage::*,
};
use pi_agent_schema::*;
use pi_telemetry::{
    AttributeValue, FiniteTelemetryNumber, SpanOptions, SpanStatus, TelemetryContext,
};
use std::sync::Arc;

/// Generic session handle; clones and lane views share exactly one backend incarnation.
pub struct RuntimeSession<S: CheckedStorage, M: SessionMetadataRecord = SessionMetadata> {
    pub(crate) inner: Arc<Inner<S, M>>,
    pub(crate) lane: LaneId,
}
pub(crate) struct Inner<S: CheckedStorage, M: SessionMetadataRecord> {
    pub storage: Arc<S>,
    pub metadata: M,
    pub codec: Arc<SessionCodec>,
    pub ids: Arc<dyn IdGenerator>,
    pub telemetry: Arc<dyn TelemetryContext>,
}
impl<S: CheckedStorage, M: SessionMetadataRecord> Clone for RuntimeSession<S, M> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            lane: self.lane.clone(),
        }
    }
}
impl<S: CheckedStorage + 'static, M: SessionMetadataRecord> RuntimeSession<S, M> {
    /// Validate existing detached data before exposing a main view. Repository creation,
    /// version migration and writer ownership remain the caller's responsibility.
    pub async fn new(
        storage: Arc<S>,
        metadata: M,
        codec: Arc<SessionCodec>,
        ids: Arc<dyn IdGenerator>,
        telemetry: Arc<dyn TelemetryContext>,
    ) -> Result<Self, SessionError> {
        let snapshot = storage.snapshot().await.map_err(crate::codec::storage)?;
        codec.validate_snapshot(&snapshot)?;
        Ok(Self {
            inner: Arc::new(Inner {
                storage,
                metadata,
                codec,
                ids,
                telemetry,
            }),
            lane: LaneId::new("main").expect("static lane"),
        })
    }
    /// Borrow the actual custom-role codec and its associated header policy.
    pub fn codec(&self) -> &SessionCodec {
        &self.inner.codec
    }
    /// Create an inert typed lane view; existence is checked when an operation executes.
    pub fn lane_view(&self, lane: LaneId) -> Self {
        Self {
            inner: self.inner.clone(),
            lane,
        }
    }
    /// Backend-neutral checked admission. Rejections and mechanism errors remain distinct.
    /// Telemetry is retrospective, after the definitive outcome; it measures no duration.
    pub async fn commit_conditional(
        &self,
        request: ConditionalCommit,
    ) -> Result<ConditionalOutcome, CheckedCommitError<SessionError>> {
        self.commit_conditional_with_telemetry(request, self.inner.telemetry.as_ref())
            .await
    }
    /// Use a borrowed context for this call's retrospective write observation only.
    /// Validation, checked publication and outcome semantics match `commit_conditional`.
    pub async fn commit_conditional_with_telemetry(
        &self,
        request: ConditionalCommit,
        telemetry: &dyn TelemetryContext,
    ) -> Result<ConditionalOutcome, CheckedCommitError<SessionError>> {
        self.inner
            .codec
            .validate_transaction(&request.transaction)
            .map_err(CheckedCommitError::Rejected)?;
        let validator = SessionPublication {
            transaction: &request.transaction,
        };
        let count = request.transaction.writes.len();
        let kinds = request
            .transaction
            .writes
            .iter()
            .map(|write| {
                match write {
                    Write::Entry(_) => "entry",
                    Write::Usage(_) => "usage",
                    Write::SetRegister(_) | Write::DeleteRegister(_) => "register",
                }
                .to_owned()
            })
            .collect();
        let result = self
            .inner
            .storage
            .commit_checked(request.clone(), &validator)
            .await;
        match &result {
            Ok(ConditionalOutcome::Applied(commit)) => {
                Self::observe(telemetry, count, kinds, Some(commit))
            }
            Err(CheckedCommitError::Storage(_)) => Self::observe(telemetry, count, kinds, None),
            _ => (),
        }
        result
    }
    fn observe(
        telemetry: &dyn TelemetryContext,
        count: usize,
        kinds: Vec<String>,
        result: Option<&CommitResult>,
    ) {
        fn number(value: f64) -> AttributeValue {
            AttributeValue::Number(
                FiniteTelemetryNumber::new(value).expect("bounded integer is finite"),
            )
        }
        let span = telemetry.start_span(SpanOptions {
            name: "pi.session.write".to_owned(),
            attributes: [
                ("pi.session.item_count".to_owned(), number(count as f64)),
                (
                    "pi.session.item_kinds".to_owned(),
                    AttributeValue::Strings(kinds),
                ),
            ]
            .into(),
        });
        if let Some(result) = result {
            if let (Some(first), Some(last)) = (result.seqs.first(), result.seqs.last()) {
                span.set_attributes(
                    &[
                        ("first_seq".to_owned(), number(first.get() as f64)),
                        ("last_seq".to_owned(), number(last.get() as f64)),
                    ]
                    .into(),
                );
            }
            span.set_status(SpanStatus::Ok);
        } else {
            span.set_status(SpanStatus::Error { error: None });
        }
        span.finish();
    }
    pub(crate) async fn write(&self, tx: Transaction) -> Result<CommitResult, SessionError> {
        match self
            .commit_conditional(ConditionalCommit {
                conditions: vec![],
                transaction: tx,
            })
            .await
            .map_err(admission_error)?
        {
            ConditionalOutcome::Applied(result) => Ok(result),
            ConditionalOutcome::NotApplied => Err(schema()),
        }
    }
}
pub(crate) fn admission_error(error: CheckedCommitError<SessionError>) -> SessionError {
    match error {
        CheckedCommitError::Rejected(error) => error,
        CheckedCommitError::Storage(error) => storage(error),
    }
}
struct SessionPublication<'a> {
    transaction: &'a Transaction,
}
impl PublicationValidator for SessionPublication<'_> {
    type Error = SessionError;
    fn indexed(&self) -> Option<&dyn IndexedPublicationValidator<Error = Self::Error>> {
        Some(self)
    }
    fn validate(&self, snapshot: &StorageSnapshot) -> Result<(), SessionError> {
        PendingPublication.validate(snapshot)?;
        // Only newly authored provenance/label links require a local target. Imported
        // branch-summary provenance may legitimately point outside a fork's path.
        let exists = |id: &str| {
            snapshot
                .entries
                .iter()
                .any(|e| crate::storage::values::structure(e).id.as_str() == id)
        };
        for write in &self.transaction.writes {
            match write {
                Write::Entry(NewEntry::BranchSummary(e)) => {
                    let summary_seq = snapshot.entries.iter().find_map(|entry| match entry {
                        Entry::BranchSummary(row) if row.id == e.id => Some(row.seq),
                        _ => None,
                    });
                    let earlier_source = snapshot.entries.iter().any(|entry| {
                        let row = crate::storage::values::structure(entry);
                        row.id == e.from_id && summary_seq.is_some_and(|seq| row.seq < seq)
                    });
                    if !earlier_source {
                        return Err(schema());
                    }
                }
                Write::SetRegister(value) => {
                    if let RegisterSet::FactLabel { key, .. } = value.as_ref()
                        && !exists(key)
                    {
                        return Err(schema());
                    }
                }
                _ => (),
            }
        }
        Ok(())
    }
}
impl<S: CheckedStorage + 'static, M: SessionMetadataRecord> Session for RuntimeSession<S, M> {
    type Metadata = M;
    fn metadata(&self) -> M {
        self.inner.metadata.clone()
    }
    fn id_generator(&self) -> &dyn IdGenerator {
        self.inner.ids.as_ref()
    }
    fn view(&self, lane: LaneId) -> Arc<dyn SessionTree> {
        Arc::new(self.lane_view(lane))
    }
    fn commit(
        &self,
        transaction: Transaction,
    ) -> BoxFuture<'_, Result<CommitResult, SessionError>> {
        Box::pin(self.write(transaction))
    }
    fn get_entries<'a>(
        &'a self,
        ids: &'a [EntryId],
    ) -> BoxFuture<'a, Result<EntryMap, SessionError>> {
        Box::pin(async move {
            let entries = self.inner.storage.get_entries(ids).await.map_err(storage)?;
            for entry in entries.values() {
                self.inner.codec.validate_entry(entry)?;
            }
            Ok(entries)
        })
    }
    fn get_register<'a>(
        &'a self,
        namespace: RegisterNamespace,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Option<Register>, SessionError>> {
        Box::pin(async move {
            let value = self
                .inner
                .storage
                .get_register(namespace, key)
                .await
                .map_err(storage)?;
            if let Some(value) = &value {
                self.inner.codec.validate_register(value)?;
            }
            Ok(value)
        })
    }
    fn list_registers<'a>(
        &'a self,
        namespace: RegisterNamespace,
        prefix: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Vec<Register>, SessionError>> {
        Box::pin(async move {
            let values = self
                .inner
                .storage
                .list_registers(namespace, prefix)
                .await
                .map_err(storage)?;
            for value in &values {
                self.inner.codec.validate_register(value)?;
            }
            Ok(values)
        })
    }
    fn close(&self) -> BoxFuture<'_, Result<(), SessionError>> {
        Box::pin(async move { self.inner.storage.close().await.map_err(storage) })
    }
}

impl IndexedPublicationValidator for SessionPublication<'_> {
    type Error = SessionError;
    fn read_set(&self) -> IndexedReadSet {
        let mut read = IndexedReadSet {
            all_pending_keys: true,
            ..Default::default()
        };
        for write in &self.transaction.writes {
            match write {
                Write::Entry(entry) => {
                    let id = match entry {
                        NewEntry::Message(value) => &value.id,
                        NewEntry::Compaction(value) => &value.id,
                        NewEntry::BranchSummary(value) => &value.id,
                        NewEntry::Custom(value) => &value.id,
                    };
                    read.entry_ids.insert(id.to_string());
                    if let NewEntry::BranchSummary(entry) = entry {
                        read.entry_ids.insert(entry.from_id.to_string());
                    }
                }
                Write::SetRegister(value) => match value.as_ref() {
                    RegisterSet::FactLabel { key, .. } | RegisterSet::PendingEntry { key, .. } => {
                        read.entry_ids.insert(key.clone());
                    }
                    _ => (),
                },
                _ => (),
            }
        }
        read
    }
    fn validate_projection(&self, projection: &IndexedProjection) -> Result<(), SessionError> {
        self.read_set().check(projection).map_err(|_| schema())?;
        let pending = projection.pending_keys.as_ref().ok_or_else(schema)?;
        if pending
            .iter()
            .any(|key| projection.entries.get(key).is_some_and(Option::is_some))
        {
            return Err(schema());
        }
        let entry = |id: &str| projection.entries.get(id).and_then(Option::as_ref);
        for write in &self.transaction.writes {
            match write {
                Write::Entry(NewEntry::BranchSummary(value)) => {
                    let source = entry(value.from_id.as_str()).ok_or_else(schema)?;
                    let summary = entry(value.id.as_str()).ok_or_else(schema)?;
                    if source.seq >= summary.seq {
                        return Err(schema());
                    }
                }
                Write::SetRegister(value) => {
                    if let RegisterSet::FactLabel { key, .. } = value.as_ref()
                        && entry(key).is_none()
                    {
                        return Err(schema());
                    }
                }
                _ => (),
            }
        }
        Ok(())
    }
}

impl<S: RuntimeStorage + 'static, M: SessionMetadataRecord> RuntimeSession<S, M> {
    /// Claim this backend incarnation, including through independent Session wrappers.
    pub fn claim_runtime(&self) -> Result<RuntimeLease, SessionError> {
        self.inner
            .storage
            .runtime_attachment()
            .try_claim()
            .map_err(storage)
    }
    /// Bracket bounded root/hydration reads without taking a complete snapshot.
    pub fn read_stamp(&self) -> BoxFuture<'_, Result<ReadStamp, SessionError>> {
        Box::pin(async move { self.inner.storage.read_stamp().await.map_err(storage) })
    }
}
