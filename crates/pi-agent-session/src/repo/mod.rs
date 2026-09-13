//! Memory repository ownership, coherent forks and migrate-on-open assembly.

use crate::{
    codec::SessionCodec,
    memory::{MemoryBacking, MemoryStorage, StorageClock},
    migration::MigrationRegistry,
    session::RuntimeSession,
    storage::{AtomicStorage, StorageSnapshot},
};
use pi_agent_schema::session_values::*;
use pi_agent_schema::*;
use pi_telemetry::TelemetryContext;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};

mod forks;
mod migration;
#[cfg(test)]
mod tests;

/// Concrete Session returned by this repository; lane views share its incarnation.
pub type MemorySession = RuntimeSession<MemoryStorage>;
/// One catalog owner. Clones share the same authoritative records.
#[derive(Clone)]
pub struct MemorySessionRepo {
    inner: Arc<Inner>,
}
struct Inner {
    catalog: Mutex<Catalog>,
    codec: Arc<SessionCodec>,
    ids: Arc<dyn IdGenerator>,
    clock: Arc<dyn StorageClock>,
    telemetry: Arc<dyn TelemetryContext>,
    migrations: Arc<MigrationRegistry<Register>>,
    #[cfg(test)]
    hooks: tests::Hooks,
}
#[derive(Default)]
struct Catalog {
    next: u128,
    records: BTreeMap<SessionId, Record>,
}
struct Record {
    incarnation: u128,
    claim: Option<u128>,
    identity: Option<Identity>,
    backing: Option<Arc<MemoryBacking>>,
    active: Option<Arc<MemorySession>>,
}
#[derive(Clone)]
struct Identity {
    id: SessionId,
    created_at: SafeUnsigned,
    cwd: Option<String>,
    parent: Option<SessionId>,
}
impl Identity {
    fn metadata(&self, snapshot: &StorageSnapshot) -> SessionMetadata {
        SessionMetadata {
            id: self.id.clone(),
            created_at: self.created_at,
            storage_version: snapshot.storage_version,
            store_generation: snapshot.store_generation,
            cwd: self.cwd.clone(),
            parent_session_id: self.parent.clone(),
            legacy_parent_session_path: None,
        }
    }
}
type RecordAccess = (Identity, Arc<MemoryBacking>, Option<Arc<MemorySession>>);
type RawAccess = (
    Identity,
    Arc<MemoryBacking>,
    Arc<MemoryStorage>,
    Option<Arc<MemorySession>>,
);
struct Claim {
    owner: Arc<Inner>,
    id: SessionId,
    incarnation: u128,
    token: u128,
    creating: bool,
}
fn context(text: &'static str) -> ErrorContext {
    ErrorContext::new(text).expect("bounded static repository context")
}
fn busy() -> SessionError {
    SessionError::Storage(StorageError::Busy(context(
        "repository record is in progress",
    )))
}
fn invalid() -> SessionError {
    SessionError::InvalidOperation(context("invalid repository operation"))
}
fn missing() -> SessionError {
    SessionError::NotFound(context("repository record or fork target is missing"))
}
fn damaged() -> SessionError {
    SessionError::Storage(StorageError::Corruption(context(
        "repository authority is inconsistent",
    )))
}
fn storage(error: StorageError) -> SessionError {
    crate::codec::storage(error)
}
fn as_storage(error: SessionError) -> StorageError {
    match error {
        SessionError::Storage(error) => error,
        SessionError::Closed(_) => StorageError::Closed(context("repository handle closed")),
        _ => StorageError::InvalidData(context("repository migration validation failed")),
    }
}
fn migration_error(error: crate::migration::MigrationError) -> SessionError {
    use crate::migration::MigrationError;
    match error {
        MigrationError::Storage(error) => storage(error),
        MigrationError::NewerStorageVersion { .. } => {
            SessionError::NewerStorageVersion(context("stored version exceeds repository target"))
        }
        _ => SessionError::Migration(context("repository migration failed")),
    }
}
impl Inner {
    fn lock(&self) -> Result<MutexGuard<'_, Catalog>, SessionError> {
        self.catalog.lock().map_err(|_| damaged())
    }
}
impl Catalog {
    fn ticket(&mut self) -> Result<u128, SessionError> {
        let ticket = self.next;
        self.next = self.next.checked_add(1).ok_or_else(invalid)?;
        Ok(ticket)
    }
}
impl Claim {
    fn checked<'a>(&self, catalog: &'a mut Catalog) -> Result<&'a mut Record, SessionError> {
        let record = catalog.records.get_mut(&self.id).ok_or_else(missing)?;
        if record.incarnation != self.incarnation || record.claim != Some(self.token) {
            return Err(busy());
        }
        Ok(record)
    }
    fn record(&self) -> Result<RecordAccess, SessionError> {
        let mut catalog = self.owner.lock()?;
        let record = self.checked(&mut catalog)?;
        Ok((
            record.identity.clone().ok_or_else(damaged)?,
            record.backing.clone().ok_or_else(damaged)?,
            record.active.clone(),
        ))
    }
    fn raw(&self) -> Result<RawAccess, SessionError> {
        let (identity, backing, active) = self.record()?;
        let raw = backing.open(self.owner.clock.clone()).map_err(storage)?;
        let borrowed = active.filter(|session| Arc::ptr_eq(&session.inner.storage, &raw));
        Ok((identity, backing, raw, borrowed))
    }
    async fn snapshot(&self) -> Result<(Identity, StorageSnapshot), SessionError> {
        let (identity, _backing, raw, _borrowed) = self.raw()?;
        let snapshot = raw.snapshot().await.map_err(storage)?;
        Ok((identity, snapshot))
    }
    fn expose(
        &self,
        identity: Identity,
        backing: Arc<MemoryBacking>,
        session: Arc<MemorySession>,
    ) -> Result<(), SessionError> {
        let retired = {
            let mut catalog = self.owner.lock()?;
            let record = self.checked(&mut catalog)?;
            let old = (
                record.backing.replace(backing),
                record.active.replace(session),
            );
            record.identity = Some(identity);
            record.claim = None;
            old
        };
        drop(retired);
        Ok(())
    }
    fn release(&self) -> Result<(), SessionError> {
        let mut catalog = self.owner.lock()?;
        self.checked(&mut catalog)?.claim = None;
        Ok(())
    }
}
impl Drop for Claim {
    fn drop(&mut self) {
        // Cleanup belongs only to this reservation. Removed owners drop after unlock.
        let removed = {
            let Ok(mut catalog) = self.owner.catalog.lock() else {
                return;
            };
            let Some(record) = catalog.records.get_mut(&self.id) else {
                return;
            };
            if record.incarnation != self.incarnation || record.claim != Some(self.token) {
                return;
            }
            if self.creating {
                catalog.records.remove(&self.id)
            } else {
                record.claim = None;
                None
            }
        };
        drop(removed);
    }
}
impl MemorySessionRepo {
    /// Compile the explicit codec and create an empty catalog. IDs/time/telemetry
    /// are supplied host capabilities; no hidden entropy or filesystem is used.
    pub fn new(
        options: SessionCodecOptions,
        policy: ProviderHeaderPolicy,
        ids: Arc<dyn IdGenerator>,
        clock: Arc<dyn StorageClock>,
        telemetry: Arc<dyn TelemetryContext>,
    ) -> Result<Self, SessionError> {
        Ok(Self::with_codec(
            Arc::new(SessionCodec::new(options, policy)?),
            ids,
            clock,
            telemetry,
        ))
    }
    /// Reuse an already compiled codec with its retained header policy.
    pub fn with_codec(
        codec: Arc<SessionCodec>,
        ids: Arc<dyn IdGenerator>,
        clock: Arc<dyn StorageClock>,
        telemetry: Arc<dyn TelemetryContext>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                catalog: Mutex::new(Catalog::default()),
                codec,
                ids,
                clock,
                telemetry,
                migrations: Arc::new(MigrationRegistry::current()),
                #[cfg(test)]
                hooks: tests::Hooks::default(),
            }),
        }
    }
    fn reserve(&self, id: SessionId) -> Result<Arc<Claim>, SessionError> {
        let mut catalog = self.inner.lock()?;
        if let Some(record) = catalog.records.get(&id) {
            return Err(if record.claim.is_some() {
                busy()
            } else {
                invalid()
            });
        }
        let token = catalog.ticket()?;
        catalog.records.insert(
            id.clone(),
            Record {
                incarnation: token,
                claim: Some(token),
                identity: None,
                backing: None,
                active: None,
            },
        );
        Ok(Arc::new(Claim {
            owner: self.inner.clone(),
            id,
            incarnation: token,
            token,
            creating: true,
        }))
    }
    fn claim(&self, id: &SessionId) -> Result<Arc<Claim>, SessionError> {
        let mut catalog = self.inner.lock()?;
        let record = catalog.records.get(id).ok_or_else(missing)?;
        if record.claim.is_some() {
            return Err(busy());
        }
        let incarnation = record.incarnation;
        let token = catalog.ticket()?;
        catalog.records.get_mut(id).ok_or_else(missing)?.claim = Some(token);
        Ok(Arc::new(Claim {
            owner: self.inner.clone(),
            id: id.clone(),
            incarnation,
            token,
            creating: false,
        }))
    }
    fn create_id(&self, options: &SessionCreateOptions) -> Result<SessionId, SessionError> {
        match &options.id {
            Some(id) => Ok(id.clone()),
            None => SessionId::new(crate::id::mint(&*self.inner.ids, None)?).map_err(|_| invalid()),
        }
    }
    async fn destination(
        &self,
        claim: Arc<Claim>,
        options: SessionCreateOptions,
        snapshot: StorageSnapshot,
        writes: Vec<Write>,
    ) -> Result<Arc<MemorySession>, SessionError> {
        let identity = Identity {
            id: claim.id.clone(),
            created_at: self.inner.clock.now().map_err(storage)?,
            cwd: options.cwd,
            parent: options.parent_session_id,
        };
        let metadata = identity.metadata(&snapshot);
        let backing = Arc::new(MemoryBacking::from_snapshot(snapshot).map_err(storage)?);
        let raw = backing.open(self.inner.clock.clone()).map_err(storage)?;
        let session = Arc::new(
            RuntimeSession::new(
                raw,
                metadata,
                self.inner.codec.clone(),
                self.inner.ids.clone(),
                self.inner.telemetry.clone(),
            )
            .await?,
        );
        session.commit(Transaction { writes }).await?;
        claim.expose(identity, backing, session.clone())?;
        Ok(session)
    }
    fn empty(&self) -> StorageSnapshot {
        StorageSnapshot {
            entries: vec![],
            registers: vec![],
            usage: vec![],
            next_seq: Some(SafeUnsigned::new(0).expect("zero")),
            storage_version: self.inner.migrations.target(),
            store_generation: SafeUnsigned::new(0).expect("zero"),
            stats: SessionStats {
                message_count: SafeUnsigned::new(0).expect("zero"),
                usage: Usage::zero(),
            },
        }
    }
    fn require_current(&self, version: SafeUnsigned) -> Result<(), SessionError> {
        if version > self.inner.migrations.target() {
            Err(SessionError::NewerStorageVersion(context(
                "stored version exceeds repository target",
            )))
        } else if version < self.inner.migrations.target() {
            Err(SessionError::Migration(context(
                "fork source requires migration through open",
            )))
        } else {
            Ok(())
        }
    }
}
impl SessionRepo for MemorySessionRepo {
    type Metadata = SessionMetadata;
    type CreateOptions = SessionCreateOptions;
    type ListOptions = ();
    type Session = MemorySession;
    fn create(
        &self,
        options: SessionCreateOptions,
    ) -> BoxFuture<'_, Result<Arc<MemorySession>, SessionError>> {
        Box::pin(async move {
            let claim = self.reserve(self.create_id(&options)?)?;
            self.destination(
                claim,
                options,
                self.empty(),
                forks::lane_writes("main", None, None),
            )
            .await
        })
    }
    fn open(
        &self,
        metadata: SessionMetadata,
    ) -> BoxFuture<'_, Result<Arc<MemorySession>, SessionError>> {
        Box::pin(async move {
            let claim = self.claim(&metadata.id)?;
            let (_identity, _backing, raw, borrowed) = claim.raw()?;
            let snapshot = raw.snapshot().await.map_err(storage)?;
            if borrowed.is_some() && snapshot.storage_version < self.inner.migrations.target() {
                return Err(busy());
            }
            // Do not retain an unpublished raw writer across a migration pointer swap.
            drop(raw);
            let adapter = migration::MemoryMigrationStore::new(claim.clone());
            self.inner
                .migrations
                .migrate(&adapter)
                .await
                .map_err(migration_error)?;
            #[cfg(test)]
            self.inner.hooks.before_expose(&metadata.id)?;
            let (identity, backing, raw, active) = claim.raw()?;
            let snapshot = raw.snapshot().await.map_err(storage)?;
            self.inner.codec.validate_snapshot(&snapshot)?;
            if let Some(session) = active {
                claim.release()?;
                return Ok(session);
            }
            let session = Arc::new(
                RuntimeSession::new(
                    raw,
                    identity.metadata(&snapshot),
                    self.inner.codec.clone(),
                    self.inner.ids.clone(),
                    self.inner.telemetry.clone(),
                )
                .await?,
            );
            claim.expose(identity, backing, session.clone())?;
            Ok(session)
        })
    }
    fn list(&self, _options: ()) -> BoxFuture<'_, Result<Vec<SessionMetadata>, SessionError>> {
        Box::pin(async move {
            let ids: Vec<_> = {
                let catalog = self.inner.lock()?;
                catalog
                    .records
                    .iter()
                    .filter(|(_, record)| record.identity.is_some())
                    .map(|(id, _)| id.clone())
                    .collect()
            };
            let mut values = Vec::with_capacity(ids.len());
            for id in ids {
                let claim = match self.claim(&id) {
                    Ok(claim) => claim,
                    Err(SessionError::NotFound(_)) => continue,
                    Err(error) => return Err(error),
                };
                let (identity, snapshot) = claim.snapshot().await?;
                values.push(identity.metadata(&snapshot));
            }
            Ok(values)
        })
    }
    fn delete(&self, metadata: SessionMetadata) -> BoxFuture<'_, Result<(), SessionError>> {
        Box::pin(async move {
            let claim = match self.claim(&metadata.id) {
                Ok(claim) => claim,
                Err(SessionError::NotFound(_)) => return Ok(()),
                Err(error) => return Err(error),
            };
            let (_identity, _backing, raw, _borrowed) = claim.raw()?;
            raw.close().await.map_err(storage)?;
            let removed = {
                let mut catalog = self.inner.lock()?;
                claim.checked(&mut catalog)?;
                catalog.records.remove(&metadata.id)
            };
            drop(removed);
            Ok(())
        })
    }
    fn fork(
        &self,
        source: SessionMetadata,
        mut options: SessionForkRequest<SessionCreateOptions>,
    ) -> BoxFuture<'_, Result<Arc<MemorySession>, SessionError>> {
        Box::pin(async move {
            if options
                .create
                .parent_session_id
                .as_ref()
                .is_some_and(|parent| parent != &source.id)
            {
                return Err(invalid());
            }
            options.create.parent_session_id = Some(source.id.clone());
            let destination = self.reserve(self.create_id(&options.create)?)?;
            let (identity, snapshot) = {
                let source_claim = self.claim(&source.id)?;
                source_claim.snapshot().await?
            };
            self.require_current(snapshot.storage_version)?;
            self.inner.codec.validate_snapshot(&snapshot)?;
            #[cfg(test)]
            self.inner.hooks.after_fork_snapshot(&identity.id)?;
            let (snapshot, writes) =
                forks::prepare(snapshot, options.fork, self.inner.migrations.target())?;
            options.create.parent_session_id = Some(identity.id);
            self.destination(destination, options.create, snapshot, writes)
                .await
        })
    }
}

/// File-backed session repository and canonical path metadata.
pub mod jsonl;
