//! Detached, transactional, one-session Memory storage.

use crate::storage::values::{materialize, register_key, register_seq, set_register, structure};
use crate::storage::*;
use pi_agent_schema::session_values::{SafeUnsigned, Usage};
use pi_agent_schema::*;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, MutexGuard, Weak};

mod query;
mod state;
use state::State;

/// An injected synchronous timestamp service, called outside canonical-state locks.
pub trait StorageClock: Send + Sync {
    /// Return a bounded Unix-millisecond timestamp or a content-free error.
    fn now(&self) -> Result<SafeUnsigned, StorageError>;
}

/// Backing authority that outlives individual open handles.
pub struct MemoryBacking {
    inner: Mutex<BackingState>,
}
struct BackingState {
    state: State,
    revision: u128,
    next_generation: u128,
    active: Option<u128>,
    handle: Weak<MemoryStorage>,
}

/// One active incarnation of a Memory session. Share the returned Arc, not new claims.
pub struct MemoryStorage {
    backing: Arc<MemoryBacking>,
    generation: u128,
    attachment: RuntimeAttachment,
    preparation: PreparedEntryAuthority,
    clock: Arc<dyn StorageClock>,
}

fn context(message: &'static str) -> ErrorContext {
    ErrorContext::new(message).expect("static bounded storage error context")
}
fn invalid(message: &'static str) -> StorageError {
    StorageError::InvalidData(context(message))
}
fn corrupt(message: &'static str) -> StorageError {
    StorageError::Corruption(context(message))
}
fn closed() -> StorageError {
    StorageError::Closed(context("storage handle is closed"))
}
fn exhausted() -> StorageError {
    invalid("storage sequence space is exhausted")
}

impl Default for MemoryBacking {
    fn default() -> Self {
        Self::new()
    }
}
impl MemoryBacking {
    /// Create empty current-version authority. Repository lane creation is separate.
    pub fn new() -> Self {
        Self::from_state(State::empty())
    }
    fn from_state(state: State) -> Self {
        Self {
            inner: Mutex::new(BackingState {
                state,
                revision: 0,
                next_generation: 0,
                active: None,
                handle: Weak::new(),
            }),
        }
    }
    /// Import complete rows without restamping them. Validates closure, identities,
    /// tokens, allocation and stats before producing any backing authority.
    pub fn from_snapshot(snapshot: StorageSnapshot) -> Result<Self, StorageError> {
        Ok(Self::from_state(State::from_snapshot(snapshot)?))
    }
    /// Detached authority read without opening or advancing a storage incarnation.
    pub(crate) fn read_snapshot(&self) -> Result<StorageSnapshot, StorageError> {
        Ok(self.lock()?.state.snapshot())
    }
    fn lock(&self) -> Result<MutexGuard<'_, BackingState>, StorageError> {
        self.inner
            .lock()
            .map_err(|_| corrupt("storage state lock is poisoned"))
    }
    /// Return the same live Arc, or create a new generation after close/final drop.
    /// A clock supplied while a handle is already live does not replace its clock.
    pub fn open(
        self: &Arc<Self>,
        clock: Arc<dyn StorageClock>,
    ) -> Result<Arc<MemoryStorage>, StorageError> {
        let mut inner = self.lock()?;
        if inner.active.is_some()
            && let Some(handle) = inner.handle.upgrade()
        {
            return Ok(handle);
        }
        let generation = inner.next_generation;
        inner.next_generation = generation
            .checked_add(1)
            .ok_or_else(|| invalid("storage handle generation exhausted"))?;
        let handle = Arc::new(MemoryStorage {
            backing: self.clone(),
            generation,
            attachment: RuntimeAttachment::new(),
            preparation: PreparedEntryAuthority::new(),
            clock,
        });
        inner.active = Some(generation);
        inner.handle = Arc::downgrade(&handle);
        Ok(handle)
    }
}

impl MemoryStorage {
    pub(crate) fn preparation_metadata(
        &self,
    ) -> Result<(ReadStamp, Option<SafeUnsigned>), StorageError> {
        let inner = self.backing.lock()?;
        self.ensure_open(&inner)?;
        Ok((
            self.attachment.stamp(inner.revision),
            inner.state.next_seq(),
        ))
    }
    fn ensure_open(&self, inner: &BackingState) -> Result<(), StorageError> {
        if inner.active == Some(self.generation) {
            Ok(())
        } else {
            Err(closed())
        }
    }
    fn read<T>(
        &self,
        read: impl FnOnce(&State) -> Result<T, StorageError>,
    ) -> Result<T, StorageError> {
        let inner = self.backing.lock()?;
        self.ensure_open(&inner)?;
        read(&inner.state)
    }
    /// Validate one complete detached stage before atomically publishing it.
    /// Validation may repeat after concurrent publication; no callback runs under
    /// the data lock, and close/generation are rechecked after every callback.
    pub fn commit_checked<'a, V: PublicationValidator + ?Sized>(
        &'a self,
        request: ConditionalCommit,
        validator: &'a V,
    ) -> BoxFuture<'a, Result<ConditionalOutcome, CheckedCommitError<V::Error>>> {
        Box::pin(async move { self.commit_sync(request, validator) })
    }
    fn commit_sync<V: PublicationValidator + ?Sized>(
        &self,
        request: ConditionalCommit,
        validator: &V,
    ) -> Result<ConditionalOutcome, CheckedCommitError<V::Error>> {
        self.commit_sync_prepared(request, validator, None)
    }
    fn commit_sync_prepared<V: PublicationValidator + ?Sized>(
        &self,
        request: ConditionalCommit,
        validator: &V,
        prepared: Option<&PreparedEntries>,
    ) -> Result<ConditionalOutcome, CheckedCommitError<V::Error>> {
        use CheckedCommitError::{Rejected, Storage as Failure};
        // No callback or canonical mutation occurs before duplicate-condition admission.
        let mut keys = BTreeSet::new();
        for condition in &request.conditions {
            if !keys.insert(condition.register.clone()) {
                return Err(Failure(invalid("duplicate register condition")));
            }
        }
        // Check serialization only: never re-decode classified operation headers.
        serde_json::to_writer(std::io::sink(), &request.transaction)
            .map_err(|_| Failure(invalid("transaction is not serializable")))?;
        loop {
            let (mut stage, revision, frozen_time) = {
                let inner = self.backing.lock().map_err(Failure)?;
                self.ensure_open(&inner).map_err(Failure)?;
                if !inner.state.matches(&request.conditions) {
                    return Ok(ConditionalOutcome::NotApplied);
                }
                let frozen_time = match prepared {
                    Some(prepared) => {
                        let Some(time) = self.preparation.timestamp(
                            prepared,
                            &self.attachment.stamp(inner.revision),
                            inner.state.next_seq(),
                        ) else {
                            return Ok(ConditionalOutcome::NotApplied);
                        };
                        self.preparation
                            .validate_inventory(prepared, &request.transaction)
                            .map_err(Failure)?;
                        Some(time)
                    }
                    None => None,
                };
                (inner.state.clone(), inner.revision, frozen_time)
            };
            let candidate = frozen_time
                .map_or_else(|| self.clock.now(), Ok)
                .and_then(|timestamp| stage.apply(request.transaction.clone(), timestamp));
            // Clock and mechanical errors also wait for incarnation and revision checks.
            let validation = match &candidate {
                Ok(_) => Some(validator.validate(&stage.snapshot())),
                Err(_) => None,
            };
            let mut inner = self.backing.lock().map_err(Failure)?;
            self.ensure_open(&inner).map_err(Failure)?;
            if inner.revision != revision {
                if prepared.is_some() {
                    return Ok(ConditionalOutcome::NotApplied);
                }
                continue;
            }
            let result = candidate.map_err(Failure)?;
            if let Some(Err(error)) = validation {
                return Err(Rejected(error));
            }
            let next_revision = inner
                .revision
                .checked_add(1)
                .ok_or_else(|| Failure(invalid("storage publication revision exhausted")))?;
            inner.state = stage;
            inner.revision = next_revision;
            return Ok(ConditionalOutcome::Applied(result));
        }
    }
}
struct Accept;
impl PublicationValidator for Accept {
    type Error = std::convert::Infallible;
    fn validate(&self, _: &StorageSnapshot) -> Result<(), Self::Error> {
        Ok(())
    }
}
impl AtomicStorage for MemoryStorage {
    fn commit_conditional(
        &self,
        request: ConditionalCommit,
    ) -> BoxFuture<'_, Result<ConditionalOutcome, StorageError>> {
        Box::pin(async move {
            match self.commit_sync(request, &Accept) {
                Ok(outcome) => Ok(outcome),
                Err(CheckedCommitError::Storage(error)) => Err(error),
                Err(CheckedCommitError::Rejected(never)) => match never {},
            }
        })
    }
    fn snapshot(&self) -> BoxFuture<'_, Result<StorageSnapshot, StorageError>> {
        Box::pin(async move { self.read(|state| Ok(state.snapshot())) })
    }
}
impl Storage for MemoryStorage {
    fn commit(
        &self,
        transaction: Transaction,
    ) -> BoxFuture<'_, Result<CommitResult, StorageError>> {
        Box::pin(async move {
            match self
                .commit_conditional(ConditionalCommit {
                    conditions: vec![],
                    transaction,
                })
                .await?
            {
                ConditionalOutcome::Applied(result) => Ok(result),
                ConditionalOutcome::NotApplied => {
                    Err(corrupt("unconditional commit did not apply"))
                }
            }
        })
    }
    fn get_entries<'a>(
        &'a self,
        ids: &'a [EntryId],
    ) -> BoxFuture<'a, Result<EntryMap, StorageError>> {
        Box::pin(async move {
            self.read(|state| {
                Ok(ids
                    .iter()
                    .filter_map(|id| {
                        state
                            .entries
                            .get(id)
                            .map(|entry| (id.clone(), entry.clone()))
                    })
                    .collect())
            })
        })
    }
    fn get_register<'a>(
        &'a self,
        namespace: RegisterNamespace,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Option<Register>, StorageError>> {
        Box::pin(async move {
            self.read(|state| {
                Ok(state
                    .registers
                    .get(&RegisterKey {
                        namespace,
                        key: key.to_owned(),
                    })
                    .cloned())
            })
        })
    }
    fn list_registers<'a>(
        &'a self,
        namespace: RegisterNamespace,
        key_prefix: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Vec<Register>, StorageError>> {
        Box::pin(async move {
            self.read(|state| {
                Ok(state
                    .registers
                    .range(
                        RegisterKey {
                            namespace,
                            key: key_prefix.unwrap_or_default().to_owned(),
                        }..,
                    )
                    .take_while(|(key, _)| {
                        key.namespace == namespace
                            && key.key.starts_with(key_prefix.unwrap_or_default())
                    })
                    .map(|(_, value)| value.clone())
                    .collect())
            })
        })
    }
    fn scan_branch(&self, query: BranchScan) -> BoxFuture<'_, Result<Vec<Entry>, StorageError>> {
        Box::pin(async move {
            self.read(|state| {
                state
                    .branch(&query)
                    .map(|rows| rows.into_iter().map(|row| state.hydrate(&row.id)).collect())
            })
        })
    }
    fn scan_branch_structure(
        &self,
        query: BranchScan,
    ) -> BoxFuture<'_, Result<Vec<EntryStructure>, StorageError>> {
        Box::pin(async move { self.read(|state| state.branch(&query)) })
    }
    fn scan_entries(&self, query: EntryScan) -> BoxFuture<'_, Result<Vec<Entry>, StorageError>> {
        Box::pin(async move { self.read(|state| Ok(state.scan_entries(&query))) })
    }
    fn scan_usage(&self, query: UsageScan) -> BoxFuture<'_, Result<Vec<UsageRow>, StorageError>> {
        Box::pin(async move { self.read(|state| Ok(state.scan_usage(&query))) })
    }
    fn stats(&self) -> BoxFuture<'_, Result<SessionStats, StorageError>> {
        Box::pin(async move { self.read(|state| Ok(state.stats.clone())) })
    }
    fn close(&self) -> BoxFuture<'_, Result<(), StorageError>> {
        Box::pin(async move {
            let mut inner = self.backing.lock()?;
            if inner.active == Some(self.generation) {
                inner.active = None;
                inner.handle = Weak::new();
            }
            Ok(())
        })
    }
}
impl Drop for MemoryStorage {
    fn drop(&mut self) {
        // Poison remains visible to future opens; Drop never recovers damaged state.
        if let Ok(mut inner) = self.backing.inner.lock()
            && inner.active == Some(self.generation)
        {
            inner.active = None;
        }
    }
}

#[cfg(test)]
mod tests;

impl CheckedStorage for MemoryStorage {
    fn commit_checked<'a, V: PublicationValidator + ?Sized>(
        &'a self,
        request: ConditionalCommit,
        validator: &'a V,
    ) -> BoxFuture<'a, Result<ConditionalOutcome, CheckedCommitError<V::Error>>> {
        MemoryStorage::commit_checked(self, request, validator)
    }
}

impl RuntimeStorage for MemoryStorage {
    fn runtime_attachment(&self) -> &RuntimeAttachment {
        &self.attachment
    }
    fn prepare_entries(
        &self,
        expected_stamp: ReadStamp,
        slots: Vec<PreparedEntrySlot>,
    ) -> BoxFuture<'_, Result<Option<PreparedEntries>, StorageError>> {
        Box::pin(async move {
            let (stamp, next) = self.preparation_metadata()?;
            if stamp != expected_stamp {
                return Ok(None);
            }
            let time = self.clock.now();
            let (after, _) = self.preparation_metadata()?;
            if after != stamp {
                return Ok(None);
            }
            self.preparation
                .prepare(stamp, next, time?, slots)
                .map(Some)
        })
    }
    fn commit_prepared_checked<'a, V: PublicationValidator + ?Sized>(
        &'a self,
        prepared: PreparedEntries,
        request: ConditionalCommit,
        validator: &'a V,
    ) -> BoxFuture<'a, Result<ConditionalOutcome, CheckedCommitError<V::Error>>> {
        Box::pin(async move { self.commit_sync_prepared(request, validator, Some(&prepared)) })
    }
    fn read_stamp(&self) -> BoxFuture<'_, Result<ReadStamp, StorageError>> {
        Box::pin(async move {
            let inner = self.backing.lock()?;
            self.ensure_open(&inner)?;
            Ok(self.attachment.stamp(inner.revision))
        })
    }
}

#[cfg(test)]
mod readonly_tests {
    use super::*;
    use crate::{
        read::{ReadBatchRequest, RepositoryRead},
        repo::MemorySessionRepo,
    };
    fn ready<T>(f: impl std::future::Future<Output = T>) -> T {
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        match std::pin::pin!(f).as_mut().poll(&mut cx) {
            std::task::Poll::Ready(v) => v,
            _ => panic!("Memory suspended"),
        }
    }
    struct Clock;
    impl StorageClock for Clock {
        fn now(&self) -> Result<SafeUnsigned, StorageError> {
            Ok(SafeUnsigned::new(1).unwrap())
        }
    }
    struct Ids;
    impl IdGenerator for Ids {
        fn next(&self, _: Option<SafeUnsigned>) -> String {
            panic!("explicit creation/read must not mint")
        }
    }
    #[test]
    fn actual_memory_repository_reads_never_advance_writer_incarnation() {
        let repo = MemorySessionRepo::new(
            SessionCodecOptions::default(),
            ProviderHeaderPolicy::default(),
            Arc::new(Ids),
            Arc::new(Clock),
            Arc::new(pi_telemetry::NoopTelemetryContext),
        )
        .unwrap();
        let root = ready(repo.create(SessionCreateOptions {
            id: Some(SessionId::new("reader").unwrap()),
            ..Default::default()
        }))
        .unwrap();
        let backing = root.inner.storage.backing.clone();
        let metadata = root.metadata();
        ready(root.close()).unwrap();
        let before = {
            let owner = backing.inner.lock().unwrap();
            (owner.next_generation, owner.active)
        };
        assert_eq!(repo.read_list(()).unwrap().len(), 1);
        let batch = repo
            .read_batch(
                &metadata,
                ReadBatchRequest {
                    cursor: None,
                    limit: SafeUnsigned::new(1).unwrap(),
                },
            )
            .unwrap();
        assert!(batch.complete && batch.entries.is_empty());
        let after = {
            let owner = backing.inner.lock().unwrap();
            (owner.next_generation, owner.active)
        };
        assert_eq!(
            after, before,
            "readonly repository must never open a MemoryStorage"
        );
    }
}
