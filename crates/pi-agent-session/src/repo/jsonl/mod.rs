//! Actual file repository: header discovery, complete publication and owned opens.
use super::{as_storage, busy, context, damaged, invalid, migration_error, missing, storage};
use crate::{
    codec::SessionCodec,
    jsonl::{FileClaim, JsonlOptions, JsonlStorage},
    legacy_v3,
    memory::{MemoryBacking, StorageClock},
    migration::MigrationRegistry,
    session::RuntimeSession,
    storage::{AtomicStorage, StorageSnapshot},
};
use pi_agent_schema::session_values::*;
use pi_agent_schema::*;
use pi_telemetry::TelemetryContext;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard},
};
mod header;
mod migration;
#[cfg(test)]
mod tests;

/// Detached stored metadata plus the actual canonical host path.
#[derive(Clone, Debug, PartialEq)]
pub struct JsonlSessionMetadata {
    /// Required metadata, obtained from the stored header rather than the caller.
    pub base: SessionMetadata,
    /// Canonical file selected by this record, also for files outside the root.
    pub path: PathBuf,
}
impl SessionMetadataRecord for JsonlSessionMetadata {
    fn base_metadata(&self) -> &SessionMetadata {
        &self.base
    }
}
/// Actual generic Session returned by the JSONL repository.
pub type JsonlSession = RuntimeSession<JsonlStorage, JsonlSessionMetadata>;
/// One root directory and its strongly retained active Session incarnations.
#[derive(Clone)]
pub struct JsonlSessionRepo {
    inner: Arc<Inner>,
}
struct Inner {
    root: PathBuf,
    catalog: Mutex<Catalog>,
    codec: Arc<SessionCodec>,
    ids: Arc<dyn IdGenerator>,
    clock: Arc<dyn StorageClock>,
    telemetry: Arc<dyn TelemetryContext>,
    options: JsonlOptions,
    migrations: Arc<MigrationRegistry<JsonValue>>,
    #[cfg(test)]
    hooks: tests::Hooks,
}
#[derive(Default)]
struct Catalog {
    next: u128,
    records: BTreeMap<PathBuf, Record>,
    creating: BTreeMap<SessionId, u128>,
}
#[derive(Default)]
struct Record {
    pending: Option<u128>,
    creating: bool,
    active: Option<Arc<JsonlSession>>,
}
struct Reservation {
    owner: Arc<Inner>,
    path: PathBuf,
    token: u128,
    id: Option<SessionId>,
}
impl Inner {
    fn lock(&self) -> Result<MutexGuard<'_, Catalog>, SessionError> {
        self.catalog.lock().map_err(|_| damaged())
    }
}
impl Reservation {
    fn active(&self) -> Result<Option<Arc<JsonlSession>>, SessionError> {
        let mut catalog = self.owner.lock()?;
        Ok(self.checked(&mut catalog)?.active.clone())
    }
    fn checked<'a>(&self, catalog: &'a mut Catalog) -> Result<&'a mut Record, SessionError> {
        let record = catalog.records.get_mut(&self.path).ok_or_else(busy)?;
        if record.pending != Some(self.token) {
            return Err(busy());
        }
        Ok(record)
    }
    fn expose(&self, session: Arc<JsonlSession>) -> Result<(), SessionError> {
        let retired = {
            let mut catalog = self.owner.lock()?;
            let record = self.checked(&mut catalog)?;
            let old = record.active.replace(session);
            record.pending = None;
            record.creating = false;
            if let Some(id) = &self.id {
                catalog.creating.remove(id);
            }
            old
        };
        drop(retired);
        Ok(())
    }
    fn remove(&self) -> Result<(), SessionError> {
        let retired = {
            let mut catalog = self.owner.lock()?;
            self.checked(&mut catalog)?;
            catalog.records.remove(&self.path)
        };
        drop(retired);
        Ok(())
    }
}
impl Drop for Reservation {
    fn drop(&mut self) {
        let retired = {
            let Ok(mut catalog) = self.owner.catalog.lock() else {
                return;
            };
            let Some(record) = catalog.records.get_mut(&self.path) else {
                return;
            };
            if record.pending != Some(self.token) {
                return;
            }
            record.pending = None;
            record.creating = false;
            let empty = record.active.is_none();
            if let Some(id) = &self.id
                && catalog.creating.get(id) == Some(&self.token)
            {
                catalog.creating.remove(id);
            }
            if empty {
                catalog.records.remove(&self.path)
            } else {
                None
            }
        };
        drop(retired);
    }
}
fn io(error: std::io::Error) -> SessionError {
    if error.kind() == std::io::ErrorKind::NotFound {
        missing()
    } else {
        storage(StorageError::Io(context(
            "repository filesystem operation failed",
        )))
    }
}
fn path_for_missing(path: &Path) -> Result<PathBuf, SessionError> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let leaf = path.file_name().ok_or_else(invalid)?;
    Ok(std::fs::canonicalize(parent).map_err(io)?.join(leaf))
}
fn legacy_error(error: legacy_v3::LegacyError) -> SessionError {
    match error {
        legacy_v3::LegacyError::Codec(error) => error,
        legacy_v3::LegacyError::Snapshot(error) => storage(error),
        _ => storage(StorageError::Corruption(context(
            "invalid legacy session source",
        ))),
    }
}
fn is_closed(error: &StorageError) -> bool {
    matches!(error, StorageError::Closed(_))
}
struct SeedClock(SafeUnsigned);
impl StorageClock for SeedClock {
    fn now(&self) -> Result<SafeUnsigned, StorageError> {
        Ok(self.0)
    }
}
impl JsonlSessionRepo {
    /// Compile an explicit codec and bind an existing root directory. No implicit
    /// custom-role schemas, directory creation, IDs or current-directory defaults.
    pub fn new(
        root: impl AsRef<Path>,
        codec: SessionCodecOptions,
        policy: ProviderHeaderPolicy,
        ids: Arc<dyn IdGenerator>,
        clock: Arc<dyn StorageClock>,
        telemetry: Arc<dyn TelemetryContext>,
        options: JsonlOptions,
    ) -> Result<Self, SessionError> {
        Self::with_codec(
            root,
            Arc::new(SessionCodec::new(codec, policy)?),
            ids,
            clock,
            telemetry,
            options,
        )
    }
    /// Bind a previously compiled codec and supplied host capabilities.
    pub fn with_codec(
        root: impl AsRef<Path>,
        codec: Arc<SessionCodec>,
        ids: Arc<dyn IdGenerator>,
        clock: Arc<dyn StorageClock>,
        telemetry: Arc<dyn TelemetryContext>,
        options: JsonlOptions,
    ) -> Result<Self, SessionError> {
        let root = std::fs::canonicalize(root).map_err(io)?;
        if !root.is_dir() || !(0.0..=1.0).contains(&options.reclaim_threshold.get()) {
            return Err(invalid());
        }
        Ok(Self {
            inner: Arc::new(Inner {
                root,
                catalog: Mutex::new(Catalog::default()),
                codec,
                ids,
                clock,
                telemetry,
                options,
                migrations: Arc::new(MigrationRegistry::current()),
                #[cfg(test)]
                hooks: tests::Hooks::default(),
            }),
        })
    }
    /// Canonical discovery directory; explicit external opens do not extend it.
    pub fn root(&self) -> &Path {
        &self.inner.root
    }
    fn encoded_path(&self, id: &SessionId) -> PathBuf {
        let mut name = String::from("s-");
        for byte in id.as_str().as_bytes() {
            use std::fmt::Write;
            write!(&mut name, "{byte:02x}").expect("String formatting");
        }
        name.push_str(".jsonl");
        self.inner.root.join(name)
    }
    fn id(&self, options: &SessionCreateOptions) -> Result<SessionId, SessionError> {
        match &options.id {
            Some(id) => Ok(id.clone()),
            None => SessionId::new(crate::id::mint(&*self.inner.ids, None)?).map_err(|_| invalid()),
        }
    }
    fn reserve(&self, path: PathBuf, id: Option<SessionId>) -> Result<Reservation, SessionError> {
        let mut catalog = self.inner.lock()?;
        if catalog
            .records
            .get(&path)
            .is_some_and(|r| r.pending.is_some())
            || id
                .as_ref()
                .is_some_and(|id| catalog.creating.contains_key(id))
        {
            return Err(busy());
        }
        // Only an absent target may become an omitted, unpublished creator.
        // An occupied encoded filename may have a different authoritative ID.
        if id.is_some() {
            match std::fs::symlink_metadata(&path) {
                Ok(_) => return Err(invalid()),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
                Err(error) => return Err(io(error)),
            }
        }
        let token = catalog.next;
        catalog.next = token.checked_add(1).ok_or_else(invalid)?;
        let record = catalog.records.entry(path.clone()).or_default();
        record.pending = Some(token);
        record.creating = id.is_some();
        if let Some(id) = &id {
            catalog.creating.insert(id.clone(), token);
        }
        Ok(Reservation {
            owner: self.inner.clone(),
            path,
            token,
            id,
        })
    }
    fn paths(&self) -> Result<Vec<PathBuf>, SessionError> {
        let mut paths = Vec::new();
        for item in std::fs::read_dir(&self.inner.root).map_err(io)? {
            let item = item.map_err(io)?;
            if item.file_type().map_err(io)?.is_file()
                && item.path().extension().is_some_and(|v| v == "jsonl")
            {
                paths.push(std::fs::canonicalize(item.path()).map_err(io)?);
            }
        }
        paths.sort();
        Ok(paths)
    }
    fn header_at(&self, path: &Path) -> Result<SessionMetadata, SessionError> {
        let reservation = self.reserve(path.to_owned(), None)?;
        if let Some(active) = reservation.active()? {
            match active.inner.storage.metadata() {
                Ok(metadata) => return Ok(metadata),
                Err(error) if is_closed(&error) => (),
                Err(error) => return Err(storage(error)),
            }
        }
        let mut claim = FileClaim::acquire(path).map_err(storage)?;
        header::read(&mut claim)
    }
    fn discovered(&self) -> Result<Vec<JsonlSessionMetadata>, SessionError> {
        let mut values = Vec::new();
        let mut ids = BTreeSet::new();
        for path in self.paths()? {
            if self
                .inner
                .lock()?
                .records
                .get(&path)
                .is_some_and(|r| r.creating)
            {
                continue;
            }
            let base = self.header_at(&path)?;
            if !ids.insert(base.id.clone()) {
                return Err(storage(StorageError::Corruption(context(
                    "duplicate discovery header identity",
                ))));
            }
            values.push(JsonlSessionMetadata {
                base: self.resolve_parent(base, &path),
                path,
            });
        }
        Ok(values)
    }
    fn namespace(&self, id: &SessionId) -> Result<(), SessionError> {
        if self.discovered()?.iter().any(|m| &m.base.id == id) {
            return Err(invalid());
        }
        Ok(())
    }
    fn resolve_parent(&self, mut metadata: SessionMetadata, path: &Path) -> SessionMetadata {
        if let Some(parent) = &metadata.legacy_parent_session_path {
            let parent = path.parent().unwrap_or_else(|| Path::new(".")).join(parent);
            if let Ok(parent) = std::fs::canonicalize(parent) {
                let id = if parent == path {
                    Some(metadata.id.clone())
                } else {
                    self.header_at(&parent).ok().map(|m| m.id)
                };
                if let Some(id) = id {
                    metadata.parent_session_id = Some(id);
                    metadata.legacy_parent_session_path = None;
                }
            }
        }
        metadata
    }
    fn empty(&self) -> StorageSnapshot {
        let zero = SafeUnsigned::new(0).expect("zero");
        StorageSnapshot {
            entries: vec![],
            registers: vec![],
            usage: vec![],
            next_seq: Some(zero),
            storage_version: SafeUnsigned::new(1).expect("one"),
            store_generation: zero,
            stats: SessionStats {
                message_count: zero,
                usage: Usage::zero(),
            },
        }
    }
    async fn destination(
        &self,
        reservation: Reservation,
        options: SessionCreateOptions,
        snapshot: StorageSnapshot,
        writes: Vec<Write>,
    ) -> Result<Arc<JsonlSession>, SessionError> {
        let id = reservation.id.clone().ok_or_else(damaged)?;
        self.namespace(&id)?;
        let timestamp = self.inner.clock.now().map_err(storage)?;
        let base = SessionMetadata {
            id: id.clone(),
            created_at: timestamp,
            storage_version: snapshot.storage_version,
            store_generation: snapshot.store_generation,
            cwd: options.cwd,
            parent_session_id: options.parent_session_id,
            legacy_parent_session_path: None,
        };
        self.inner.codec.validate_transaction(&Transaction {
            writes: writes.clone(),
        })?;
        let scratch = Arc::new(MemoryBacking::from_snapshot(snapshot).map_err(storage)?)
            .open(Arc::new(SeedClock(timestamp)))
            .map_err(storage)?;
        scratch
            .commit(Transaction { writes })
            .await
            .map_err(storage)?;
        let prepared = scratch.snapshot().await.map_err(storage)?;
        self.inner.codec.validate_snapshot(&prepared)?;
        #[cfg(test)]
        self.inner.hooks.before_create()?;
        self.namespace(&id)?;
        let raw = JsonlStorage::create_from_snapshot(
            &reservation.path,
            base.clone(),
            prepared,
            self.inner.codec.clone(),
            self.inner.clock.clone(),
            self.inner.options,
        )?;
        #[cfg(test)]
        self.inner.hooks.after_publication()?;
        let session = self.session(raw, base).await?;
        reservation.expose(session.clone())?;
        Ok(session)
    }
    async fn session(
        &self,
        raw: Arc<JsonlStorage>,
        base: SessionMetadata,
    ) -> Result<Arc<JsonlSession>, SessionError> {
        let metadata = JsonlSessionMetadata {
            path: raw.path().to_owned(),
            base,
        };
        Ok(Arc::new(
            RuntimeSession::new(
                raw,
                metadata,
                self.inner.codec.clone(),
                self.inner.ids.clone(),
                self.inner.telemetry.clone(),
            )
            .await?,
        ))
    }
    fn normalized(
        &self,
        claim: &mut FileClaim,
        base: SessionMetadata,
    ) -> Result<legacy_v3::PreparedLegacy, SessionError> {
        let base = self.resolve_parent(base, &claim.path);
        legacy_v3::normalize(
            claim.read_source().map_err(storage)?,
            &self.inner.codec,
            base.parent_session_id,
        )
        .map_err(legacy_error)
    }
    async fn open_reserved(
        &self,
        reservation: Reservation,
        expected: Option<SessionId>,
    ) -> Result<Arc<JsonlSession>, SessionError> {
        if let Some(active) = reservation.active()? {
            match active.inner.storage.snapshot().await {
                Ok(snapshot) => {
                    let actual = active.inner.storage.metadata().map_err(storage)?;
                    if expected.as_ref().is_some_and(|id| id != &actual.id) {
                        return Err(invalid());
                    }
                    if snapshot.storage_version != self.inner.migrations.target() {
                        return Err(busy());
                    }
                    self.inner.codec.validate_snapshot(&snapshot)?;
                    return Ok(active);
                }
                Err(error) if is_closed(&error) => (),
                Err(error) => return Err(storage(error)),
            }
        }
        let mut claim = FileClaim::acquire(&reservation.path).map_err(storage)?;
        let base = header::read(&mut claim)?;
        if expected.as_ref().is_some_and(|id| id != &base.id) {
            return Err(invalid());
        }
        let raw = if header::is_legacy(&claim.read_source().map_err(storage)?)? {
            let prepared = self.normalized(&mut claim, base)?;
            claim.adopt_deferred(
                prepared.metadata().clone(),
                prepared.snapshot().clone(),
                prepared.source_bytes().to_vec(),
                self.inner.codec.clone(),
                self.inner.clock.clone(),
            )?
        } else {
            if base.storage_version > self.inner.migrations.target() {
                return Err(SessionError::NewerStorageVersion(context(
                    "stored version exceeds repository target",
                )));
            }
            let adapter = migration::JsonlMigrationStore::new(claim, self.inner.clone());
            let result = self.inner.migrations.migrate(&adapter).await;
            if let Err(error) = result {
                return Err(adapter
                    .validation_error()
                    .unwrap_or_else(|| migration_error(error)));
            }
            #[cfg(test)]
            self.inner.hooks.before_expose()?;
            adapter.into_claim()?.into_storage(
                self.inner.codec.clone(),
                self.inner.clock.clone(),
                self.inner.options,
            )?
        };
        let base = raw.metadata().map_err(storage)?;
        let session = self.session(raw, base).await?;
        reservation.expose(session.clone())?;
        Ok(session)
    }
    /// Open one explicit existing file; it need not belong to root discovery.
    pub fn open_path(
        &self,
        path: impl AsRef<Path>,
    ) -> BoxFuture<'_, Result<Arc<JsonlSession>, SessionError>> {
        let path = path.as_ref().to_owned();
        Box::pin(async move {
            let path = std::fs::canonicalize(path).map_err(io)?;
            let reservation = self.reserve(path, None)?;
            self.open_reserved(reservation, None).await
        })
    }
}
impl SessionRepo for JsonlSessionRepo {
    type Metadata = JsonlSessionMetadata;
    type CreateOptions = SessionCreateOptions;
    type ListOptions = ();
    type Session = JsonlSession;
    fn create(
        &self,
        options: SessionCreateOptions,
    ) -> BoxFuture<'_, Result<Arc<JsonlSession>, SessionError>> {
        Box::pin(async move {
            let id = self.id(&options)?;
            let reservation = self.reserve(self.encoded_path(&id), Some(id))?;
            self.destination(
                reservation,
                options,
                self.empty(),
                super::forks::lane_writes("main", None, None),
            )
            .await
        })
    }
    fn open(
        &self,
        metadata: JsonlSessionMetadata,
    ) -> BoxFuture<'_, Result<Arc<JsonlSession>, SessionError>> {
        Box::pin(async move {
            let path = std::fs::canonicalize(metadata.path).map_err(io)?;
            let reservation = self.reserve(path, None)?;
            self.open_reserved(reservation, Some(metadata.base.id))
                .await
        })
    }
    fn list(&self, _: ()) -> BoxFuture<'_, Result<Vec<JsonlSessionMetadata>, SessionError>> {
        Box::pin(async move { self.discovered() })
    }
    fn delete(&self, metadata: JsonlSessionMetadata) -> BoxFuture<'_, Result<(), SessionError>> {
        Box::pin(async move {
            let path = match std::fs::canonicalize(&metadata.path) {
                Ok(path) => path,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    match path_for_missing(&metadata.path) {
                        Ok(path) => path,
                        Err(SessionError::NotFound(_)) => return Ok(()),
                        Err(e) => return Err(e),
                    }
                }
                Err(e) => return Err(io(e)),
            };
            let reservation = self.reserve(path.clone(), None)?;
            let active = reservation.active()?;
            let mut claim = if let Some(active) = active {
                // Selection failure must not seal a different live identity.
                match active.inner.storage.metadata() {
                    Ok(actual) if actual.id != metadata.base.id => return Err(invalid()),
                    Ok(_) => (),
                    Err(error) if is_closed(&error) => (),
                    Err(error) => return Err(storage(error)),
                }
                match active.inner.storage.take_file_claim() {
                    Ok(claim) => Some(claim),
                    Err(error) if is_closed(&error) => None,
                    Err(error) => return Err(storage(error)),
                }
            } else {
                None
            };
            if claim.is_none() {
                if !path.try_exists().map_err(io)? {
                    reservation.remove()?;
                    return Ok(());
                }
                claim = Some(FileClaim::acquire(&path).map_err(storage)?);
            }
            let mut claim = claim.ok_or_else(damaged)?;
            if path.try_exists().map_err(io)? && header::read(&mut claim)?.id != metadata.base.id {
                return Err(invalid());
            }
            #[cfg(test)]
            self.inner.hooks.after_seal()?;
            claim.unlink_owned().map_err(storage)?;
            reservation.remove()?;
            drop(claim);
            Ok(())
        })
    }
    fn fork(
        &self,
        source: JsonlSessionMetadata,
        mut options: SessionForkRequest<SessionCreateOptions>,
    ) -> BoxFuture<'_, Result<Arc<JsonlSession>, SessionError>> {
        Box::pin(async move {
            if options
                .create
                .parent_session_id
                .as_ref()
                .is_some_and(|p| p != &source.base.id)
            {
                return Err(invalid());
            }
            let id = self.id(&options.create)?;
            let destination = self.reserve(self.encoded_path(&id), Some(id))?;
            let path = std::fs::canonicalize(&source.path).map_err(io)?;
            let source_reservation = self.reserve(path.clone(), None)?;
            let mut active_snapshot = None;
            if let Some(active) = source_reservation.active()? {
                match active.inner.storage.snapshot().await {
                    Ok(snapshot) => {
                        active_snapshot =
                            Some((active.inner.storage.metadata().map_err(storage)?, snapshot))
                    }
                    Err(error) if is_closed(&error) => (),
                    Err(error) => return Err(storage(error)),
                }
            }
            let (base, snapshot) = if let Some(value) = active_snapshot {
                value
            } else {
                let mut claim = FileClaim::acquire(&path).map_err(storage)?;
                let base = header::read(&mut claim)?;
                if header::is_legacy(&claim.read_source().map_err(storage)?)? {
                    let prepared = self.normalized(&mut claim, base)?;
                    (prepared.metadata().clone(), prepared.snapshot().clone())
                } else {
                    if base.storage_version != self.inner.migrations.target() {
                        return Err(if base.storage_version > self.inner.migrations.target() {
                            SessionError::NewerStorageVersion(context(
                                "stored version exceeds repository target",
                            ))
                        } else {
                            SessionError::Migration(context("fork source requires open migration"))
                        });
                    }
                    (
                        base,
                        claim
                            .read_raw()
                            .map_err(storage)?
                            .current(&self.inner.codec)?,
                    )
                }
            };
            if base.id != source.base.id {
                return Err(invalid());
            }
            if snapshot.storage_version != self.inner.migrations.target() {
                return Err(busy());
            }
            self.inner.codec.validate_snapshot(&snapshot)?;
            drop(source_reservation);
            let (snapshot, writes) =
                super::forks::prepare(snapshot, options.fork, self.inner.migrations.target())?;
            options.create.parent_session_id = Some(base.id);
            self.destination(destination, options.create, snapshot, writes)
                .await
        })
    }
}

mod read;
