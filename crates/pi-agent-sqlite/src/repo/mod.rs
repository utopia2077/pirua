//! File repository over actual fenced SQLite storage and detached readonly reads.
use crate::*;
use pi_agent_session::{id::mint, session::RuntimeSession};
mod read;
mod rewrite;

/// Actual base metadata and canonical file locator. Ancillary caller fields never
/// override the stored header; the ID is checked before any active owner is sealed.
#[derive(Clone, PartialEq, Eq)]
pub struct SqliteSessionMetadata {
    pub base: SessionMetadata,
    pub path: PathBuf,
}
impl std::fmt::Debug for SqliteSessionMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SqliteSessionMetadata([REDACTED])")
    }
}
impl SessionMetadataRecord for SqliteSessionMetadata {
    fn base_metadata(&self) -> &SessionMetadata {
        &self.base
    }
}
pub type SqliteSession = RuntimeSession<SqliteStorage, SqliteSessionMetadata>;
#[derive(Clone)]
pub struct SqliteSessionRepo {
    inner: Arc<RepoInner>,
}
struct Active {
    storage: Arc<SqliteStorage>,
    session: Arc<SqliteSession>,
}
impl Clone for Active {
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
            session: self.session.clone(),
        }
    }
}
#[derive(Default)]
struct Record {
    pending: Option<u64>,
    active: Option<Active>,
}
#[derive(Default)]
struct Catalog {
    records: BTreeMap<PathBuf, Record>,
    creating: BTreeMap<SessionId, u64>,
    next: u64,
}
struct RepoInner {
    root: PathBuf,
    codec: Arc<SessionCodec>,
    ids: Arc<dyn IdGenerator>,
    clock: Arc<dyn StorageClock>,
    telemetry: Arc<dyn TelemetryContext>,
    options: SqliteOptions,
    catalog: Mutex<Catalog>,
    #[cfg(test)]
    hook: Mutex<Option<tests::Hook>>,
}
struct Reservation {
    inner: Arc<RepoInner>,
    path: PathBuf,
    id: Option<SessionId>,
    ticket: u64,
}
fn invalid_repo() -> SessionError {
    SessionError::InvalidOperation(context("invalid SQLite repository operation"))
}
fn missing() -> SessionError {
    SessionError::NotFound(context("SQLite repository session missing"))
}
fn conflict() -> SessionError {
    as_session(StorageError::Conflict(context(
        "SQLite repository destination exists",
    )))
}
fn busy_repo() -> SessionError {
    as_session(busy())
}
impl RepoInner {
    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Catalog>, SessionError> {
        self.catalog.lock().map_err(|_| invalid_repo())
    }
    fn phase(&self, _name: &str, _path: &Path) -> Result<(), SessionError> {
        #[cfg(test)]
        {
            let hook = self.hook.lock().unwrap().clone();
            if let Some(hook) = hook {
                hook(_name, _path)?;
            }
        }
        Ok(())
    }
}
impl Reservation {
    fn active(&self) -> Result<Option<Active>, SessionError> {
        let catalog = self.inner.lock()?;
        let record = catalog.records.get(&self.path).ok_or_else(invalid_repo)?;
        if record.pending != Some(self.ticket) {
            return Err(busy_repo());
        }
        Ok(record.active.clone())
    }
    fn expose(&self, active: Active) -> Result<(), SessionError> {
        let old = {
            let mut catalog = self.inner.lock()?;
            let record = catalog
                .records
                .get_mut(&self.path)
                .ok_or_else(invalid_repo)?;
            if record.pending != Some(self.ticket) {
                return Err(busy_repo());
            }
            record.active.replace(active)
        };
        drop(old);
        Ok(())
    }
    fn clear_active(&self) -> Result<(), SessionError> {
        let old = {
            let mut c = self.inner.lock()?;
            let r = c.records.get_mut(&self.path).ok_or_else(invalid_repo)?;
            if r.pending != Some(self.ticket) {
                return Err(busy_repo());
            }
            r.active.take()
        };
        drop(old);
        Ok(())
    }
}
impl Drop for Reservation {
    fn drop(&mut self) {
        let retired = {
            let Ok(mut catalog) = self.inner.catalog.lock() else {
                return;
            };
            if let Some(record) = catalog.records.get_mut(&self.path)
                && record.pending == Some(self.ticket)
            {
                record.pending = None;
                let empty = record.active.is_none();
                if let Some(id) = &self.id
                    && catalog.creating.get(id) == Some(&self.ticket)
                {
                    catalog.creating.remove(id);
                }
                if empty {
                    catalog.records.remove(&self.path)
                } else {
                    None
                }
            } else {
                None
            }
        };
        drop(retired);
    }
}
impl SqliteSessionRepo {
    pub fn new(
        root: impl AsRef<Path>,
        codec: SessionCodecOptions,
        policy: ProviderHeaderPolicy,
        ids: Arc<dyn IdGenerator>,
        clock: Arc<dyn StorageClock>,
        telemetry: Arc<dyn TelemetryContext>,
        options: SqliteOptions,
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
    pub fn with_codec(
        root: impl AsRef<Path>,
        codec: Arc<SessionCodec>,
        ids: Arc<dyn IdGenerator>,
        clock: Arc<dyn StorageClock>,
        telemetry: Arc<dyn TelemetryContext>,
        options: SqliteOptions,
    ) -> Result<Self, SessionError> {
        options.validate().map_err(as_session)?;
        let root = std::fs::canonicalize(root).map_err(|e| as_session(io(e)))?;
        if !root.is_dir() {
            return Err(invalid_repo());
        }
        Ok(Self {
            inner: Arc::new(RepoInner {
                root,
                codec,
                ids,
                clock,
                telemetry,
                options,
                catalog: Mutex::new(Catalog::default()),
                #[cfg(test)]
                hook: Mutex::new(None),
            }),
        })
    }
    pub fn root(&self) -> &Path {
        &self.inner.root
    }
    fn destination_path(&self, id: &SessionId) -> PathBuf {
        let mut name = String::from("s-");
        for b in id.as_str().as_bytes() {
            use std::fmt::Write;
            write!(name, "{b:02x}").expect("String");
        }
        name.push_str(".sqlite");
        self.inner.root.join(name)
    }
    fn reserve(&self, path: PathBuf, id: Option<SessionId>) -> Result<Reservation, SessionError> {
        let mut c = self.inner.lock()?;
        if c.records.get(&path).is_some_and(|r| r.pending.is_some())
            || id.as_ref().is_some_and(|id| c.creating.contains_key(id))
        {
            return Err(busy_repo());
        }
        if id.is_some() {
            match std::fs::symlink_metadata(&path) {
                Ok(_) => return Err(conflict()),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
                Err(e) => return Err(as_session(io(e))),
            }
        }
        let ticket = c.next.checked_add(1).ok_or_else(invalid_repo)?;
        c.next = ticket;
        c.records.entry(path.clone()).or_default().pending = Some(ticket);
        if let Some(id) = &id {
            c.creating.insert(id.clone(), ticket);
        }
        Ok(Reservation {
            inner: self.inner.clone(),
            path,
            id,
            ticket,
        })
    }
    fn select_path(&self, path: &Path) -> Result<PathBuf, SessionError> {
        path::existing(path).map_err(|e| match e {
            StorageError::Io(_) => missing(),
            e => as_session(e),
        })
    }
    fn fresh(&self, id: SessionId) -> Result<Reservation, SessionError> {
        let path = self.destination_path(&id);
        // Occupied target is rejected before any creating marker can hide its header.
        match std::fs::symlink_metadata(&path) {
            Ok(_) => return Err(conflict()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
            Err(e) => return Err(as_session(io(e))),
        }
        if self.discovered(None)?.iter().any(|m| m.base.id == id) {
            return Err(conflict());
        }
        self.reserve(path, Some(id))
    }
    fn generated_id(&self, id: Option<SessionId>) -> Result<SessionId, SessionError> {
        match id {
            Some(id) => Ok(id),
            None => {
                SessionId::new(mint(self.inner.ids.as_ref(), None)?).map_err(|_| invalid_repo())
            }
        }
    }
    async fn expose(
        &self,
        reservation: &Reservation,
        storage: Arc<SqliteStorage>,
    ) -> Result<Arc<SqliteSession>, SessionError> {
        let base = storage.metadata().map_err(as_session)?;
        let metadata = SqliteSessionMetadata {
            base,
            path: storage.path().to_owned(),
        };
        let constructed = RuntimeSession::new(
            storage.clone(),
            metadata,
            self.inner.codec.clone(),
            self.inner.ids.clone(),
            self.inner.telemetry.clone(),
        )
        .await;
        let session = match constructed {
            Ok(s) => Arc::new(s),
            Err(e) => {
                let _ = storage.close().await;
                return Err(e);
            }
        };
        if let Err(e) = self.inner.phase("before_expose", &reservation.path) {
            let _ = storage.close().await;
            return Err(e);
        }
        reservation.expose(Active {
            storage,
            session: session.clone(),
        })?;
        Ok(session)
    }
    async fn publish(
        &self,
        reservation: Reservation,
        options: SessionCreateOptions,
        mut snapshot: StorageSnapshot,
        writes: Vec<Write>,
    ) -> Result<Arc<SqliteSession>, SessionError> {
        for write in writes {
            let Write::SetRegister(value) = write else {
                return Err(invalid_repo());
            };
            let seq = snapshot.next_seq.ok_or_else(invalid_repo)?;
            snapshot.registers.push(materialize_register(*value, seq));
            snapshot.next_seq = SafeUnsigned::new(seq.get() + 1).ok();
        }
        self.inner.codec.validate_snapshot(&snapshot)?;
        data::validate_mechanical(&snapshot).map_err(as_session)?;
        let created_at = self.inner.clock.now().map_err(as_session)?;
        let id = reservation.id.clone().ok_or_else(invalid_repo)?;
        if self
            .discovered(Some(&reservation))?
            .iter()
            .any(|m| m.base.id == id)
        {
            return Err(conflict());
        }
        let metadata = SessionMetadata {
            id,
            created_at,
            storage_version: n(1),
            store_generation: n(0),
            cwd: options.cwd,
            parent_session_id: options.parent_session_id,
            legacy_parent_session_path: None,
        };
        self.inner.phase("before_create", &reservation.path)?;
        let storage = SqliteStorage::create_from_snapshot(
            &reservation.path,
            metadata,
            snapshot,
            self.inner.codec.clone(),
            self.inner.clock.clone(),
            self.inner.options,
        )?;
        self.expose(&reservation, storage).await
    }
    pub fn open_path(
        &self,
        path: impl AsRef<Path>,
    ) -> BoxFuture<'_, Result<Arc<SqliteSession>, SessionError>> {
        let path = path.as_ref().to_owned();
        Box::pin(async move {
            let path = self.select_path(&path)?;
            let base = self.header(&path)?;
            self.open(SqliteSessionMetadata { base, path }).await
        })
    }
}
impl SessionRepo for SqliteSessionRepo {
    type Metadata = SqliteSessionMetadata;
    type CreateOptions = SessionCreateOptions;
    type ListOptions = ();
    type Session = SqliteSession;
    fn create(
        &self,
        options: SessionCreateOptions,
    ) -> BoxFuture<'_, Result<Arc<SqliteSession>, SessionError>> {
        Box::pin(async move {
            let id = self.generated_id(options.id.clone())?;
            let reserved = self.fresh(id)?;
            let snapshot = StorageSnapshot {
                entries: vec![],
                registers: vec![],
                usage: vec![],
                next_seq: Some(n(0)),
                storage_version: n(1),
                store_generation: n(0),
                stats: SessionStats {
                    message_count: n(0),
                    usage: Usage::zero(),
                },
            };
            let writes = lane_seed_writes("main", None, None);
            self.publish(reserved, options, snapshot, writes).await
        })
    }
    fn open(
        &self,
        metadata: SqliteSessionMetadata,
    ) -> BoxFuture<'_, Result<Arc<SqliteSession>, SessionError>> {
        Box::pin(async move {
            let path = self.select_path(&metadata.path)?;
            let reserved = self.reserve(path.clone(), None)?;
            if self.header(&path)?.id != metadata.base.id {
                return Err(invalid_repo());
            }
            if let Some(active) = reserved.active()? {
                match active.storage.metadata() {
                    Ok(base) => {
                        if base.id != metadata.base.id {
                            return Err(invalid_repo());
                        }
                        return Ok(active.session);
                    }
                    Err(StorageError::Closed(_)) => reserved.clear_active()?,
                    Err(e) => return Err(as_session(e)),
                }
            }
            let storage = SqliteStorage::open(
                &path,
                self.inner.codec.clone(),
                self.inner.clock.clone(),
                self.inner.options,
            )?;
            if storage.metadata().map_err(as_session)?.id != metadata.base.id {
                let _ = storage.close().await;
                return Err(invalid_repo());
            }
            self.expose(&reserved, storage).await
        })
    }
    fn list(&self, _: ()) -> BoxFuture<'_, Result<Vec<SqliteSessionMetadata>, SessionError>> {
        Box::pin(async move { self.discovered(None) })
    }
    fn delete(&self, metadata: SqliteSessionMetadata) -> BoxFuture<'_, Result<(), SessionError>> {
        Box::pin(async move {
            let path = self.select_path(&metadata.path)?;
            let reserved = self.reserve(path.clone(), None)?;
            let observed =
                SqliteStorage::read_snapshot(&path, &self.inner.codec, self.inner.options)?;
            if observed.metadata.id != metadata.base.id {
                return Err(invalid_repo());
            }
            if let Some(active) = reserved.active()? {
                match active.storage.metadata() {
                    Ok(base) => {
                        if base.id != metadata.base.id {
                            return Err(invalid_repo());
                        }
                        active.storage.close().await.map_err(as_session)?;
                    }
                    Err(StorageError::Closed(_)) => (),
                    Err(error) => return Err(as_session(error)),
                }
                reserved.clear_active()?;
            }
            owner::delete_file(
                &path,
                &observed,
                self.inner.codec.clone(),
                self.inner.clock.clone(),
                self.inner.options,
            )?;
            Ok(())
        })
    }
    fn fork(
        &self,
        source: SqliteSessionMetadata,
        mut options: SessionForkRequest<SessionCreateOptions>,
    ) -> BoxFuture<'_, Result<Arc<SqliteSession>, SessionError>> {
        Box::pin(async move {
            let path = self.select_path(&source.path)?;
            let reserved_source = self.reserve(path.clone(), None)?;
            let source_snapshot =
                SqliteStorage::read_snapshot(&path, &self.inner.codec, self.inner.options)?;
            if source_snapshot.metadata.id != source.base.id {
                return Err(invalid_repo());
            }
            drop(reserved_source);
            let (snapshot, writes) = prepare_fork(source_snapshot.snapshot, options.fork, n(1))?;
            options.create.parent_session_id = Some(source_snapshot.metadata.id);
            let id = self.generated_id(options.create.id.clone())?;
            let reserved = self.fresh(id)?;
            self.publish(reserved, options.create, snapshot, writes)
                .await
        })
    }
}
#[cfg(test)]
pub(crate) mod tests;
