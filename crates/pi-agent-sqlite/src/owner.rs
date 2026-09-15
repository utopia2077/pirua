use super::*;
use rusqlite::types::Value;
use std::{
    sync::atomic::{AtomicBool, Ordering},
    thread::JoinHandle,
};
struct Life {
    released: bool,
}
#[derive(Default)]
struct Signal {
    stop: Mutex<bool>,
    wake: Condvar,
}
impl Signal {
    fn stop(&self) {
        if let Ok(mut stop) = self.stop.lock() {
            *stop = true;
            self.wake.notify_all();
        }
    }
}
struct Inner {
    attachment: RuntimeAttachment,
    path: PathBuf,
    identity: path::Identity,
    store: StoreToken,
    owner: StoreToken,
    fence: SafeUnsigned,
    options: SqliteOptions,
    codec: Arc<SessionCodec>,
    clock: Arc<dyn StorageClock>,
    sealed: AtomicBool,
    life: Mutex<Life>,
    close_gate: Mutex<()>,
    signal: Arc<Signal>,
    worker: Mutex<Option<JoinHandle<()>>>,
}
/// A fenced SQLite writer incarnation. Every operation uses a transient connection.
pub struct SqliteStorage {
    inner: Arc<Inner>,
}
fn acquire(
    db: &Connection,
    header: &data::Header,
    owner: StoreToken,
    options: SqliteOptions,
) -> Result<SafeUnsigned, StorageError> {
    let time = now()?;
    let occupied = db
        .query_row(
            "SELECT expires_at_ms FROM writer_lease WHERE singleton=1",
            [],
            |r| r.get::<_, i64>(0),
        )
        .optional()
        .map_err(sql)?;
    if occupied.is_some_and(|expiry| expiry > time.get() as i64) {
        return Err(busy());
    }
    let fence = add(header.fence_counter, n(1))?;
    let expiry = add(time, options.lease_ttl_ms)?;
    db.execute(
        "UPDATE session SET fence_counter=?1 WHERE singleton=1",
        [fence.get() as i64],
    )
    .map_err(sql)?;
    db.execute("INSERT INTO writer_lease(singleton,owner_id,fence,expires_at_ms) VALUES(1,?1,?2,?3) ON CONFLICT(singleton) DO UPDATE SET owner_id=excluded.owner_id,fence=excluded.fence,expires_at_ms=excluded.expires_at_ms",params![owner.0.as_slice(),fence.get() as i64,expiry.get() as i64]).map_err(sql)?;
    Ok(fence)
}
fn release(
    db: &Connection,
    store: StoreToken,
    owner: StoreToken,
    fence: SafeUnsigned,
) -> Result<(), StorageError> {
    db.execute("DELETE FROM writer_lease WHERE singleton=1 AND owner_id=?1 AND fence=?2 AND EXISTS(SELECT 1 FROM session WHERE singleton=1 AND store_token=?3)",params![owner.0.as_slice(),fence.get() as i64,store.0.as_slice()]).map_err(sql)?;
    Ok(())
}
impl Inner {
    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Life>, StorageError> {
        self.life
            .lock()
            .map_err(|_| corrupt("SQLite owner lock poisoned"))
    }
    fn verify_identity(&self) -> Result<(), StorageError> {
        path::verify(&self.path, self.identity).inspect_err(|_| self.seal())
    }
    fn verify(
        &self,
        db: &Connection,
        header: &data::Header,
        time: SafeUnsigned,
    ) -> Result<(), StorageError> {
        if header.token != self.store {
            self.seal();
            return Err(closed());
        }
        let valid:bool=db.query_row("SELECT EXISTS(SELECT 1 FROM writer_lease WHERE singleton=1 AND owner_id=?1 AND fence=?2 AND expires_at_ms>?3)",params![self.owner.0.as_slice(),self.fence.get() as i64,time.get() as i64],|r|r.get(0)).map_err(sql)?;
        if !valid {
            self.seal();
            return Err(closed());
        }
        Ok(())
    }
    fn renew_sql(&self, db: &Connection, time: SafeUnsigned) -> Result<(), StorageError> {
        let expiry = add(time, self.options.lease_ttl_ms)?;
        let changed=db.execute("UPDATE writer_lease SET expires_at_ms=?1 WHERE singleton=1 AND owner_id=?2 AND fence=?3 AND expires_at_ms>?4",params![expiry.get() as i64,self.owner.0.as_slice(),self.fence.get() as i64,time.get() as i64]).map_err(sql)?;
        if changed != 1 {
            self.seal();
            return Err(closed());
        }
        Ok(())
    }
    fn renew(&self) -> Result<(), StorageError> {
        let _life = self.lock()?;
        if self.sealed.load(Ordering::SeqCst) {
            return Err(closed());
        }
        let _scope = path::Scope::acquire(&self.path, false)?;
        self.verify_identity()?;
        let mut db = path::connection(&self.path, self.options, false)?;
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        let time = now()?;
        let header = data::header(&tx)?;
        self.verify(&tx, &header, time)?;
        self.renew_sql(&tx, time)?;
        tx.commit().map_err(sql)
    }
    fn seal(&self) {
        self.sealed.store(true, Ordering::SeqCst);
        self.signal.stop();
    }
    fn read<T>(
        &self,
        operation: impl FnOnce(&Connection, &data::Header) -> Result<T, StorageError>,
    ) -> Result<T, StorageError> {
        let _life = self.lock()?;
        if self.sealed.load(Ordering::SeqCst) {
            return Err(closed());
        }
        let _scope = path::Scope::acquire(&self.path, false)?;
        self.verify_identity()?;
        let mut db = path::connection(&self.path, self.options, true)?;
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Deferred)
            .map_err(sql)?;
        let header = data::header(&tx)?;
        self.verify(&tx, &header, now()?)?;
        let result = operation(&tx, &header)?;
        tx.commit().map_err(sql)?;
        Ok(result)
    }
    fn finish_close(&self) -> Result<(), StorageError> {
        let _close = self
            .close_gate
            .lock()
            .map_err(|_| corrupt("SQLite close lock poisoned"))?;
        {
            let life = self.lock()?;
            self.sealed.store(true, Ordering::SeqCst);
            if life.released {
                return Ok(());
            }
        }
        self.signal.stop();
        let handle = self
            .worker
            .lock()
            .map_err(|_| corrupt("SQLite worker lock poisoned"))?
            .take();
        if let Some(handle) = handle {
            handle
                .join()
                .map_err(|_| StorageError::Io(context("SQLite renewal worker failed")))?;
        }
        let _scope = path::Scope::acquire(&self.path, false)?;
        if path::verify(&self.path, self.identity).is_err() {
            self.lock()?.released = true;
            return Ok(());
        }
        let mut db = path::connection(&self.path, self.options, false)?;
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        release(&tx, self.store, self.owner, self.fence)?;
        tx.commit().map_err(sql)?;
        self.lock()?.released = true;
        Ok(())
    }
}
impl Drop for SqliteStorage {
    fn drop(&mut self) {
        let _ = self.inner.finish_close();
    }
}
impl SqliteStorage {
    fn start(
        path: PathBuf,
        identity: path::Identity,
        authority: (StoreToken, StoreToken, SafeUnsigned),
        codec: Arc<SessionCodec>,
        clock: Arc<dyn StorageClock>,
        options: SqliteOptions,
    ) -> Result<Arc<Self>, SessionError> {
        let (store, owner, fence) = authority;
        let inner = Arc::new(Inner {
            attachment: RuntimeAttachment::new(),
            path,
            identity,
            store,
            owner,
            fence,
            options,
            codec,
            clock,
            sealed: AtomicBool::new(false),
            life: Mutex::new(Life { released: false }),
            close_gate: Mutex::new(()),
            signal: Arc::new(Signal::default()),
            worker: Mutex::new(None),
        });
        let storage = Arc::new(Self {
            inner: inner.clone(),
        });
        let weak: Weak<Inner> = Arc::downgrade(&inner);
        let signal = inner.signal.clone();
        let interval = Duration::from_millis(options.heartbeat_ms.get());
        let worker = std::thread::Builder::new()
            .name("pi-sqlite-renew".into())
            .spawn(move || {
                loop {
                    let Ok(stop) = signal.stop.lock() else { break };
                    if *stop {
                        break;
                    }
                    let Ok((stop, _)) = signal.wake.wait_timeout(stop, interval) else {
                        break;
                    };
                    if *stop {
                        break;
                    }
                    drop(stop);
                    let Some(owner) = weak.upgrade() else { break };
                    if owner.renew().is_err() {
                        owner.seal();
                        break;
                    }
                }
            })
            .map_err(|e| as_session(io(e)))?;
        *inner
            .worker
            .lock()
            .map_err(|_| as_session(corrupt("worker lock")))? = Some(worker);
        Ok(storage)
    }
    /// Open current version one, claim fenced ownership and validate stored authority.
    pub fn open(
        path: impl AsRef<Path>,
        codec: Arc<SessionCodec>,
        clock: Arc<dyn StorageClock>,
        options: SqliteOptions,
    ) -> Result<Arc<Self>, SessionError> {
        Self::open_impl(path.as_ref(), codec, clock, options, false)
    }
    fn open_impl(
        path: &Path,
        codec: Arc<SessionCodec>,
        clock: Arc<dyn StorageClock>,
        options: SqliteOptions,
        repairing: bool,
    ) -> Result<Arc<Self>, SessionError> {
        options.validate().map_err(as_session)?;
        let path = path::existing(path).map_err(as_session)?;
        let scope = path::Scope::acquire(&path, false).map_err(as_session)?;
        let identity = path::inspect_identity(&path).map_err(as_session)?;
        let mut db = path::connection(&path, options, false).map_err(as_session)?;
        let owner = token().map_err(as_session)?;
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)
            .map_err(as_session)?;
        data::current(&tx)?;
        let header = data::header(&tx).map_err(as_session)?;
        let fence = acquire(&tx, &header, owner, options).map_err(as_session)?;
        tx.commit().map_err(sql).map_err(as_session)?;
        let validation = (|| {
            let tx = db
                .transaction_with_behavior(TransactionBehavior::Deferred)
                .map_err(sql)
                .map_err(as_session)?;
            let header = data::header(&tx).map_err(as_session)?;
            let snapshot = data::snapshot(&tx, &header, &codec).map_err(as_session)?;
            if !repairing {
                branch::audit(&tx).map_err(as_session)?;
            }
            data::validate(&snapshot, &codec)?;
            tx.commit().map_err(sql).map_err(as_session)?;
            Ok(())
        })();
        if let Err(error) = validation {
            if let Ok(tx) = db.transaction_with_behavior(TransactionBehavior::Immediate) {
                let _ = release(&tx, header.token, owner, fence);
                let _ = tx.commit();
            }
            return Err(error);
        }
        drop(db);
        drop(scope);
        Self::start(
            path,
            identity,
            (header.token, owner, fence),
            codec,
            clock,
            options,
        )
    }
    /// Publish a complete new empty storage file. Repository seeding belongs in a
    /// complete snapshot passed to create_from_snapshot before first visibility.
    pub fn create(
        path: impl AsRef<Path>,
        metadata: SessionMetadata,
        codec: Arc<SessionCodec>,
        clock: Arc<dyn StorageClock>,
        options: SqliteOptions,
    ) -> Result<Arc<Self>, SessionError> {
        let snapshot = StorageSnapshot {
            entries: vec![],
            registers: vec![],
            usage: vec![],
            next_seq: Some(n(0)),
            storage_version: metadata.storage_version,
            store_generation: metadata.store_generation,
            stats: SessionStats {
                message_count: n(0),
                usage: Usage::zero(),
            },
        };
        Self::create_from_snapshot(path, metadata, snapshot, codec, clock, options)
    }
    /// Build/checkpoint/close a full temporary database, then atomically publish it at
    /// a fresh exclusively locked path, retaining all imported tokens and timestamps.
    pub fn create_from_snapshot(
        path: impl AsRef<Path>,
        metadata: SessionMetadata,
        snapshot: StorageSnapshot,
        codec: Arc<SessionCodec>,
        clock: Arc<dyn StorageClock>,
        options: SqliteOptions,
    ) -> Result<Arc<Self>, SessionError> {
        options.validate().map_err(as_session)?;
        if metadata.storage_version != n(1)
            || metadata.storage_version != snapshot.storage_version
            || metadata.store_generation != snapshot.store_generation
        {
            return Err(SessionError::Migration(context(
                "snapshot version/generation mismatch",
            )));
        }
        data::validate(&snapshot, &codec)?;
        let path = path::fresh(path.as_ref()).map_err(as_session)?;
        let scope = path::Scope::acquire(&path, true).map_err(as_session)?;
        match std::fs::symlink_metadata(&path) {
            Ok(_) => {
                return Err(as_session(StorageError::Conflict(context(
                    "SQLite destination exists",
                ))));
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
            Err(e) => return Err(as_session(io(e))),
        }
        let temporary = path::Temporary::new(&path).map_err(as_session)?;
        let mut db = path::connection(&temporary.path, options, false).map_err(as_session)?;
        db.pragma_update(None, "journal_mode", "WAL")
            .map_err(sql)
            .map_err(as_session)?;
        let store = token().map_err(as_session)?;
        let owner = token().map_err(as_session)?;
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)
            .map_err(as_session)?;
        data::init(&tx, &metadata, &snapshot, store).map_err(as_session)?;
        let header = data::header(&tx).map_err(as_session)?;
        let fence = acquire(&tx, &header, owner, options).map_err(as_session)?;
        tx.commit().map_err(sql).map_err(as_session)?;
        let busy: i64 = db
            .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| r.get(0))
            .map_err(sql)
            .map_err(as_session)?;
        if busy != 0 {
            return Err(as_session(StorageError::Io(context(
                "temporary WAL checkpoint incomplete",
            ))));
        }
        drop(db);
        let identity = path::inspect_identity(&temporary.path).map_err(as_session)?;
        #[cfg(test)]
        crate::tests::before_publish(&path).map_err(as_session)?;
        temporary.publish(&path).map_err(as_session)?;
        drop(scope);
        Self::start(path, identity, (store, owner, fence), codec, clock, options)
    }
    /// Canonical path retained after close.
    pub fn path(&self) -> &Path {
        &self.inner.path
    }
    /// Actual metadata from one fenced SQL read.
    pub fn metadata(&self) -> Result<SessionMetadata, StorageError> {
        self.inner.read(|_, h| Ok(h.metadata.clone()))
    }
    /// Header-only observation, without obtaining or renewing a writer lease.
    pub fn read_metadata(
        path: impl AsRef<Path>,
        options: SqliteOptions,
    ) -> Result<SessionMetadata, SessionError> {
        Self::readonly(path.as_ref(), options, |db| Ok(data::header(db)?.metadata))
    }
    fn readonly<T>(
        path: &Path,
        options: SqliteOptions,
        operation: impl FnOnce(&Connection) -> Result<T, StorageError>,
    ) -> Result<T, SessionError> {
        options.validate().map_err(as_session)?;
        let path = path::existing(path).map_err(as_session)?;
        let _scope = path::Scope::acquire(&path, false).map_err(as_session)?;
        let identity = path::inspect_identity(&path).map_err(as_session)?;
        path::verify(&path, identity).map_err(as_session)?;
        let mut db = path::connection(&path, options, true).map_err(as_session)?;
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Deferred)
            .map_err(sql)
            .map_err(as_session)?;
        let result = operation(&tx).map_err(as_session)?;
        tx.commit().map_err(sql).map_err(as_session)?;
        Ok(result)
    }
    /// Explicit coherent full snapshot for import/fork/admin consumers; no writer lease.
    pub fn read_snapshot(
        path: impl AsRef<Path>,
        codec: &SessionCodec,
        options: SqliteOptions,
    ) -> Result<SnapshotObservation, SessionError> {
        let observation = Self::readonly(path.as_ref(), options, |db| {
            if let Err(error) = data::current(db) {
                return Ok(Err(error));
            }
            let header = data::header(db)?;
            let snapshot = data::snapshot(db, &header, codec)?;
            branch::audit(db)?;
            Ok(Ok(SnapshotObservation {
                metadata: header.metadata,
                snapshot,
                store_token: header.token,
                revision: header.revision,
            }))
        })??;
        data::validate_mechanical(&observation.snapshot).map_err(as_session)?;
        Ok(observation)
    }
    /// Indexed readonly entry page using store incarnation/generation and exact seq
    /// membership. Never opens/renews a writer or enumerates the historical ledger.
    pub fn read_entry_page(
        path: impl AsRef<Path>,
        codec: &SessionCodec,
        options: SqliteOptions,
        request: pi_agent_session::read::ReadBatchRequest,
    ) -> Result<pi_agent_session::read::ReadBatch, SessionError> {
        use pi_agent_session::read::{ReadBatch, ReadCursor};
        if request.limit.get() == 0 {
            return Err(SessionError::InvalidOperation(context(
                "zero readonly page limit",
            )));
        }
        Self::readonly(path.as_ref(), options, |db| {
            if let Err(error) = data::current(db) {
                return Ok(Err(error));
            }
            let header = data::header(db)?;
            let mut witness = vec![2];
            witness.extend_from_slice(&header.token.0);
            let mut last = None;
            let mut reset = false;
            if let Some(cursor) = &request.cursor {
                if cursor.witness.len() != 33 || cursor.witness.first() != Some(&2) {
                    return Err(invalid("invalid SQLite cursor"));
                }
                let exists=match cursor.last_entry_seq{Some(seq)=>db.query_row("SELECT EXISTS(SELECT 1 FROM entries INDEXED BY ix_entry_seq WHERE seq=?1)",[seq.get() as i64],|r|r.get::<_,bool>(0)).map_err(sql)?,None=>true};
                if cursor.generation != header.metadata.store_generation
                    || cursor.witness != witness
                    || !exists
                {
                    reset = true;
                } else {
                    last = cursor.last_entry_seq;
                }
            }
            let lower = last.map_or(-1, |seq| seq.get() as i64);
            let query = format!(
                "SELECT {},e.payload FROM entries e INDEXED BY ix_entry_seq WHERE e.seq>?1 ORDER BY e.seq LIMIT ?2",
                data::ENTRY_COLUMNS
            );
            let mut statement = db.prepare(&query).map_err(sql)?;
            let entries = statement
                .query_map(params![lower, request.limit.get() as i64], |r| {
                    data::RawEntry::row(r, true)
                })
                .map_err(sql)?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(sql)?
                .into_iter()
                .map(|r| r.entry(codec))
                .collect::<Result<Vec<_>, _>>()?;
            let last = entries.last().map(|r| entry_structure(r).seq).or(last);
            let more = db
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM entries INDEXED BY ix_entry_seq WHERE seq>?1)",
                    [last.map_or(-1, |seq| seq.get() as i64)],
                    |r| r.get::<_, bool>(0),
                )
                .map_err(sql)?;
            Ok(Ok(ReadBatch {
                metadata: header.metadata.clone(),
                entries,
                next_cursor: ReadCursor {
                    generation: header.metadata.store_generation,
                    last_entry_seq: last,
                    witness,
                },
                complete: !more,
                reset,
            }))
        })?
    }
    /// Explicit derived-cache repair on this writer. Canonical rows/stats/generation
    /// remain unchanged; revision advances so previously prepared work must retry.
    pub fn repair(&self) -> BoxFuture<'_, Result<(), StorageError>> {
        Box::pin(async move {
            let _life = self.inner.lock()?;
            if self.inner.sealed.load(Ordering::SeqCst) {
                return Err(closed());
            }
            let _scope = path::Scope::acquire(&self.inner.path, false)?;
            self.inner.verify_identity()?;
            let mut db = path::connection(&self.inner.path, self.inner.options, false)?;
            let tx = db
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql)?;
            let time = now()?;
            let header = data::header(&tx)?;
            self.inner.verify(&tx, &header, time)?;
            let revision = add(header.revision, n(1))?;
            branch::repair(&tx)?;
            self.inner.renew_sql(&tx, time)?;
            tx.execute(
                "UPDATE session SET revision=?1 WHERE singleton=1",
                [revision.get() as i64],
            )
            .map_err(sql)?;
            tx.commit().map_err(sql)
        })
    }
    /// Explicit repair remains reachable for an inactive file whose damaged cache
    /// prevents normal open. Canonical data is fully validated, never auto-rewritten.
    pub fn repair_file(
        path: impl AsRef<Path>,
        codec: Arc<SessionCodec>,
        options: SqliteOptions,
    ) -> Result<(), SessionError> {
        struct Clock;
        impl StorageClock for Clock {
            fn now(&self) -> Result<SafeUnsigned, StorageError> {
                now()
            }
        }
        let storage = Self::open_impl(path.as_ref(), codec, Arc::new(Clock), options, true)?;
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        let result = match std::pin::pin!(storage.repair()).as_mut().poll(&mut cx) {
            std::task::Poll::Ready(value) => value.map_err(as_session),
            std::task::Poll::Pending => Err(as_session(corrupt("synchronous repair suspended"))),
        };
        let close = storage.inner.finish_close().map_err(as_session);
        result.and(close)
    }
    fn checked<V: PublicationValidator + ?Sized>(
        &self,
        request: ConditionalCommit,
        validator: &V,
    ) -> Result<ConditionalOutcome, CheckedCommitError<V::Error>> {
        use CheckedCommitError::Storage as Failure;
        let mut keys = BTreeSet::new();
        for condition in &request.conditions {
            if !keys.insert(condition.register.clone()) {
                return Err(Failure(invalid("duplicate register condition")));
            }
        }
        serde_json::to_writer(std::io::sink(), &request.transaction)
            .map_err(|_| Failure(invalid("transaction serialization failed")))?;
        loop {
            let indexed = validator.indexed();
            let read = indexed.map(|v| v.read_set());
            let captured = self
                .inner
                .read(|db, h| {
                    commit::capture(db, h.clone(), &request, read.as_ref(), &self.inner.codec)
                })
                .map_err(Failure)?;
            let Some(captured) = captured else {
                return Ok(ConditionalOutcome::NotApplied);
            };
            let revision = captured.header.revision;
            let prepared = (|| {
                self.inner
                    .codec
                    .validate_transaction(&request.transaction)
                    .map_err(|_| Failure(invalid("transaction fails storage codec")))?;
                let time = self.inner.clock.now().map_err(Failure)?;
                let prepared = commit::prepare(captured, request.transaction.clone(), time)
                    .map_err(Failure)?;
                match indexed {
                    Some(v) => {
                        v.validate_projection(&prepared.projection)
                            .map_err(CheckedCommitError::Rejected)?;
                    }
                    None => validator
                        .validate(prepared.full.as_ref().ok_or_else(|| {
                            Failure(corrupt("complete validator lacks full stage"))
                        })?)
                        .map_err(CheckedCommitError::Rejected)?,
                }
                Ok(prepared)
            })();
            let _life = self.inner.lock().map_err(Failure)?;
            if self.inner.sealed.load(Ordering::SeqCst) {
                return Err(Failure(closed()));
            }
            let _scope = path::Scope::acquire(&self.inner.path, false).map_err(Failure)?;
            self.inner.verify_identity().map_err(Failure)?;
            let mut db =
                path::connection(&self.inner.path, self.inner.options, false).map_err(Failure)?;
            let tx = db
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql)
                .map_err(Failure)?;
            let time = now().map_err(Failure)?;
            let header = data::header(&tx).map_err(Failure)?;
            self.inner.verify(&tx, &header, time).map_err(Failure)?;
            if header.revision != revision {
                continue;
            }
            let prepared = prepared?;
            let next = add(revision, n(1)).map_err(Failure)?;
            self.inner.renew_sql(&tx, time).map_err(Failure)?;
            #[cfg(test)]
            crate::tests::before_commit(&self.inner.path).map_err(Failure)?;
            commit::publish(&tx, &prepared, next).map_err(Failure)?;
            tx.commit().map_err(sql).map_err(Failure)?;
            return Ok(ConditionalOutcome::Applied(prepared.result));
        }
    }
}
struct Accept;
impl PublicationValidator for Accept {
    type Error = std::convert::Infallible;
    fn indexed(&self) -> Option<&dyn IndexedPublicationValidator<Error = Self::Error>> {
        Some(self)
    }
    fn validate(&self, _: &StorageSnapshot) -> Result<(), Self::Error> {
        Ok(())
    }
}
impl IndexedPublicationValidator for Accept {
    type Error = std::convert::Infallible;
    fn read_set(&self) -> IndexedReadSet {
        IndexedReadSet::default()
    }
    fn validate_projection(&self, _: &IndexedProjection) -> Result<(), Self::Error> {
        Ok(())
    }
}
impl CheckedStorage for SqliteStorage {
    fn commit_checked<'a, V: PublicationValidator + ?Sized>(
        &'a self,
        request: ConditionalCommit,
        validator: &'a V,
    ) -> BoxFuture<'a, Result<ConditionalOutcome, CheckedCommitError<V::Error>>> {
        Box::pin(async move { self.checked(request, validator) })
    }
}
impl AtomicStorage for SqliteStorage {
    fn commit_conditional(
        &self,
        request: ConditionalCommit,
    ) -> BoxFuture<'_, Result<ConditionalOutcome, StorageError>> {
        Box::pin(async move {
            match self.checked(request, &Accept) {
                Ok(v) => Ok(v),
                Err(CheckedCommitError::Storage(e)) => Err(e),
                Err(CheckedCommitError::Rejected(e)) => match e {},
            }
        })
    }
    fn snapshot(&self) -> BoxFuture<'_, Result<StorageSnapshot, StorageError>> {
        Box::pin(async move {
            self.inner.read(|db, h| {
                let snapshot = data::snapshot(db, h, &self.inner.codec)?;
                branch::audit(db)?;
                data::validate_mechanical(&snapshot)?;
                Ok(snapshot)
            })
        })
    }
}
impl Storage for SqliteStorage {
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
                ConditionalOutcome::Applied(value) => Ok(value),
                ConditionalOutcome::NotApplied => {
                    Err(corrupt("unconditional SQL commit mismatched"))
                }
            }
        })
    }
    fn get_entries<'a>(
        &'a self,
        ids: &'a [EntryId],
    ) -> BoxFuture<'a, Result<EntryMap, StorageError>> {
        Box::pin(async move {
            self.inner.read(|db, _| {
                let mut statement = db
                    .prepare(&format!(
                        "SELECT {},e.payload FROM entries e WHERE e.id=?1",
                        data::ENTRY_COLUMNS
                    ))
                    .map_err(sql)?;
                let mut values = EntryMap::new();
                for id in ids {
                    if let Some(row) = statement
                        .query_row([id.as_str()], |r| data::RawEntry::row(r, true))
                        .optional()
                        .map_err(sql)?
                    {
                        values.insert(id.clone(), row.entry(&self.inner.codec)?);
                    }
                }
                Ok(values)
            })
        })
    }
    fn get_register<'a>(
        &'a self,
        namespace: RegisterNamespace,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Option<Register>, StorageError>> {
        Box::pin(async move {
            self.inner.read(|db, _| {
                data::register(
                    db,
                    &RegisterKey {
                        namespace,
                        key: key.into(),
                    },
                    &self.inner.codec,
                )
            })
        })
    }
    fn list_registers<'a>(
        &'a self,
        namespace: RegisterNamespace,
        prefix: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Vec<Register>, StorageError>> {
        Box::pin(async move {
            self.inner.read(|db, _| {
                let mut query = String::from("SELECT key FROM registers WHERE namespace=?");
                let mut args = vec![Value::Text(scalar(&namespace)?)];
                if let Some(prefix) = prefix {
                    query.push_str(" AND key>=?");
                    args.push(Value::Text(prefix.into()));
                    if let Some(upper) = data::prefix_upper(prefix) {
                        query.push_str(" AND key<?");
                        args.push(Value::Text(upper));
                    }
                }
                query.push_str(" ORDER BY key");
                let mut statement = db.prepare(&query).map_err(sql)?;
                let keys = statement
                    .query_map(rusqlite::params_from_iter(args), |r| r.get::<_, String>(0))
                    .map_err(sql)?
                    .collect::<rusqlite::Result<Vec<_>>>()
                    .map_err(sql)?;
                keys.into_iter()
                    .map(|key| {
                        data::register(db, &RegisterKey { namespace, key }, &self.inner.codec)?
                            .ok_or_else(|| corrupt("listed register missing"))
                    })
                    .collect()
            })
        })
    }
    fn scan_branch(&self, q: BranchScan) -> BoxFuture<'_, Result<Vec<Entry>, StorageError>> {
        Box::pin(async move {
            self.inner.read(|db, _| {
                branch::scan(db, &self.inner.codec, &q, true)?
                    .into_iter()
                    .map(|v| match v {
                        branch::Selected::Entry(v) => Ok(v),
                        _ => Err(corrupt("branch payload projection mismatch")),
                    })
                    .collect()
            })
        })
    }
    fn scan_branch_structure(
        &self,
        q: BranchScan,
    ) -> BoxFuture<'_, Result<Vec<EntryStructure>, StorageError>> {
        Box::pin(async move {
            self.inner.read(|db, _| {
                branch::scan(db, &self.inner.codec, &q, false)?
                    .into_iter()
                    .map(|v| match v {
                        branch::Selected::Structure(v) => Ok(v),
                        _ => Err(corrupt("branch structure projection mismatch")),
                    })
                    .collect()
            })
        })
    }
    fn scan_entries(&self, q: EntryScan) -> BoxFuture<'_, Result<Vec<Entry>, StorageError>> {
        Box::pin(async move {
            self.inner.read(|db, _| {
                let (range, mut args) = data::range(q.from_seq, q.to_seq, "e.seq");
                let mut query = format!(
                    "SELECT {},e.payload FROM entries e INDEXED BY ix_entry_seq WHERE 1=1{range}",
                    data::ENTRY_COLUMNS
                );
                if let Some(kind) = q.entry_type {
                    query.push_str(" AND e.type=?");
                    args.push(Value::Text(scalar(&kind)?));
                }
                if let Some(custom) = q.custom_type {
                    query.push_str(" AND e.custom_type=?");
                    args.push(Value::Text(custom.to_string()));
                }
                query.push_str(if q.order == Some(ScanOrder::Descending) {
                    " ORDER BY e.seq DESC"
                } else {
                    " ORDER BY e.seq ASC"
                });
                if let Some(limit) = q.limit {
                    query.push_str(" LIMIT ?");
                    args.push(Value::Integer(limit.get() as i64));
                }
                let mut statement = db.prepare(&query).map_err(sql)?;
                statement
                    .query_map(rusqlite::params_from_iter(args), |r| {
                        data::RawEntry::row(r, true)
                    })
                    .map_err(sql)?
                    .collect::<rusqlite::Result<Vec<_>>>()
                    .map_err(sql)?
                    .into_iter()
                    .map(|r| r.entry(&self.inner.codec))
                    .collect()
            })
        })
    }
    fn scan_usage(&self, q: UsageScan) -> BoxFuture<'_, Result<Vec<UsageRow>, StorageError>> {
        Box::pin(async move {
            self.inner.read(|db,_|{let (range,mut args)=data::range(q.from_seq,q.to_seq,"seq");let mut query=format!("SELECT id,seq,entry_id,adjustment,usage,details FROM usage_ledger INDEXED BY ix_usage_seq WHERE 1=1{range}");query.push_str(if q.order==Some(ScanOrder::Descending){" ORDER BY seq DESC"}else{" ORDER BY seq ASC"});if let Some(limit)=q.limit{query.push_str(" LIMIT ?");args.push(Value::Integer(limit.get() as i64));}let mut statement=db.prepare(&query).map_err(sql)?;statement.query_map(rusqlite::params_from_iter(args),data::usage_row).map_err(sql)?.collect::<rusqlite::Result<Vec<_>>>().map_err(sql)?.into_iter().map(data::decode_usage).collect()})
        })
    }
    fn stats(&self) -> BoxFuture<'_, Result<SessionStats, StorageError>> {
        Box::pin(async move { self.inner.read(|_, h| Ok(h.stats.clone())) })
    }
    fn close(&self) -> BoxFuture<'_, Result<(), StorageError>> {
        Box::pin(async move { self.inner.finish_close() })
    }
}

/// Same-crate administrative authority; never exports a writer/fence or connection.
pub(crate) struct Administrative {
    storage: Arc<SqliteStorage>,
    pub observation: SnapshotObservation,
    permissions: std::fs::Permissions,
}
impl Administrative {
    pub(crate) fn capture(
        path: &Path,
        codec: Arc<SessionCodec>,
        clock: Arc<dyn StorageClock>,
        options: SqliteOptions,
    ) -> Result<Self, SessionError> {
        // Claim under the existing shared-scope writer protocol FIRST. A live
        // foreign lease must refuse without ever excluding its heartbeat.
        let storage = SqliteStorage::open(path, codec.clone(), clock, options)?;
        let inner = &storage.inner;
        let life = inner.lock().map_err(as_session)?;
        if inner.sealed.load(Ordering::SeqCst) {
            return Err(as_session(closed()));
        }
        // Our own renewal also takes Life, so it cannot contend on this exclusive
        // scope and misclassify the admin operation as foreign path contention.
        let scope = path::Scope::acquire(&inner.path, true).map_err(as_session)?;
        #[cfg(test)]
        crate::repo::tests::phase("admin_exclusive", &inner.path)?;
        inner.verify_identity().map_err(as_session)?;
        let permissions = std::fs::metadata(&inner.path)
            .map_err(|e| as_session(io(e)))?
            .permissions();
        let mut db = path::connection(&inner.path, options, false).map_err(as_session)?;
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)
            .map_err(as_session)?;
        data::current(&tx)?;
        let header = data::header(&tx).map_err(as_session)?;
        let snapshot = data::snapshot(&tx, &header, &codec).map_err(as_session)?;
        data::validate(&snapshot, &codec)?;
        branch::audit(&tx).map_err(as_session)?;
        let time = now().map_err(as_session)?;
        inner.verify(&tx, &header, time).map_err(as_session)?;
        inner.renew_sql(&tx, time).map_err(as_session)?;
        tx.commit().map_err(sql).map_err(as_session)?;
        drop(db);
        drop(scope);
        drop(life);
        Ok(Self {
            storage,
            observation: SnapshotObservation {
                metadata: header.metadata,
                snapshot,
                store_token: header.token,
                revision: header.revision,
            },
            permissions,
        })
    }
    pub(crate) fn finish(
        self,
        prepared: Result<StorageSnapshot, SessionError>,
    ) -> Result<SessionMetadata, SessionError> {
        // Build only a detached private file while the logical lease may renew.
        // Even a prepared error reaches the final authority recheck below.
        let prepared = prepared.and_then(|snapshot| {
            let mut metadata = self.observation.metadata.clone();
            metadata.store_generation = snapshot.store_generation;
            let temporary = path::Temporary::new(&self.storage.inner.path).map_err(as_session)?;
            let mut db = path::connection(&temporary.path, self.storage.inner.options, false)
                .map_err(as_session)?;
            db.pragma_update(None, "journal_mode", "WAL")
                .map_err(sql)
                .map_err(as_session)?;
            let tx = db
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql)
                .map_err(as_session)?;
            data::init(&tx, &metadata, &snapshot, token().map_err(as_session)?)
                .map_err(as_session)?;
            tx.commit().map_err(sql).map_err(as_session)?;
            #[cfg(test)]
            crate::repo::tests::phase("before_admin_checkpoint", &self.storage.inner.path)?;
            checkpoint(&db).map_err(as_session)?;
            db.close().map_err(|(_, e)| as_session(sql(e)))?;
            Ok((temporary, metadata))
        });
        let inner = &self.storage.inner;
        inner.signal.stop();
        let worker = inner
            .worker
            .lock()
            .map_err(|_| as_session(corrupt("admin worker lock")))?
            .take();
        if let Some(worker) = worker {
            worker
                .join()
                .map_err(|_| as_session(corrupt("admin worker join")))?;
        }
        let mut life = inner.lock().map_err(as_session)?;
        if inner.sealed.load(Ordering::SeqCst) {
            return Err(as_session(closed()));
        }
        let scope = path::Scope::acquire(&inner.path, true).map_err(as_session)?;
        inner.verify_identity().map_err(as_session)?;
        let mut db = path::connection(&inner.path, inner.options, false).map_err(as_session)?;
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)
            .map_err(as_session)?;
        data::current(&tx)?;
        let header = data::header(&tx).map_err(as_session)?;
        let time = now().map_err(as_session)?;
        inner.verify(&tx, &header, time).map_err(as_session)?;
        if header.revision != self.observation.revision {
            return Err(as_session(StorageError::Conflict(context(
                "administrative source changed",
            ))));
        }
        let (temporary, metadata) = prepared?;
        inner.renew_sql(&tx, time).map_err(as_session)?;
        tx.commit().map_err(sql).map_err(as_session)?;
        checkpoint(&db).map_err(as_session)?;
        db.close().map_err(|(_, e)| as_session(sql(e)))?;
        #[cfg(test)]
        crate::repo::tests::phase("before_admin_replace", &inner.path)?;
        temporary
            .replace(&inner.path, &scope, self.permissions)
            .map_err(as_session)?;
        inner.seal();
        life.released = true;
        drop(life);
        drop(scope);
        Ok(metadata)
    }
}
fn checkpoint(db: &Connection) -> Result<(), StorageError> {
    let busy: i64 = db
        .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| r.get(0))
        .map_err(sql)?;
    if busy != 0 {
        return Err(StorageError::Busy(context(
            "administrative checkpoint busy",
        )));
    }
    Ok(())
}
pub(crate) fn delete_file(
    path: &Path,
    observed: &SnapshotObservation,
    codec: Arc<SessionCodec>,
    clock: Arc<dyn StorageClock>,
    options: SqliteOptions,
) -> Result<(), SessionError> {
    let admin = Administrative::capture(path, codec, clock, options)?;
    if admin.observation.metadata.id != observed.metadata.id
        || admin.observation.store_token != observed.store_token
    {
        return Err(as_session(closed()));
    }
    admin.delete()
}
impl Administrative {
    fn delete(self) -> Result<(), SessionError> {
        let inner = &self.storage.inner;
        inner.signal.stop();
        let worker = inner
            .worker
            .lock()
            .map_err(|_| as_session(corrupt("admin worker lock")))?
            .take();
        if let Some(worker) = worker {
            worker
                .join()
                .map_err(|_| as_session(corrupt("admin worker join")))?;
        }
        let mut life = inner.lock().map_err(as_session)?;
        if inner.sealed.load(Ordering::SeqCst) {
            return Err(as_session(closed()));
        }
        let scope = path::Scope::acquire(&inner.path, true).map_err(as_session)?;
        inner.verify_identity().map_err(as_session)?;
        let mut db = path::connection(&inner.path, inner.options, false).map_err(as_session)?;
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)
            .map_err(as_session)?;
        data::current(&tx)?;
        let header = data::header(&tx).map_err(as_session)?;
        inner
            .verify(&tx, &header, now().map_err(as_session)?)
            .map_err(as_session)?;
        if header.revision != self.observation.revision {
            return Err(as_session(StorageError::Conflict(context(
                "administrative source changed",
            ))));
        }
        release(&tx, inner.store, inner.owner, inner.fence).map_err(as_session)?;
        tx.commit().map_err(sql).map_err(as_session)?;
        checkpoint(&db).map_err(as_session)?;
        db.close().map_err(|(_, e)| as_session(sql(e)))?;
        std::fs::remove_file(&inner.path).map_err(|e| as_session(io(e)))?;
        inner.seal();
        life.released = true;
        drop(life);
        drop(scope);
        Ok(())
    }
}

impl RuntimeStorage for SqliteStorage {
    fn runtime_attachment(&self) -> &RuntimeAttachment {
        &self.inner.attachment
    }
    fn read_stamp(&self) -> BoxFuture<'_, Result<ReadStamp, StorageError>> {
        Box::pin(async move {
            self.inner.read(|_, header| {
                Ok(self
                    .inner
                    .attachment
                    .stamp(u128::from(header.revision.get())))
            })
        })
    }
}
