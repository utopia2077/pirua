//! Format-four JSONL storage with synchronous, first-poll physical publication.
//!
//! Files are exclusively owned by the host process. Canonical paths collapse
//! symlink aliases; Unix hard-linked inputs are refused. This is not cross-process
//! fencing or an fsync/power-loss guarantee. On non-Unix hosts, exclusive single-path
//! ownership is a host prerequisite; file-object/hard-link detection is unverified.

use crate::storage::values::{register_key, register_seq};
use crate::{
    codec::SessionCodec,
    memory::{MemoryBacking, MemoryStorage, StorageClock},
    storage::*,
};
use pi_agent_schema::session_values::*;
use pi_agent_schema::*;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

mod file;
mod format;
#[cfg(test)]
mod tests;
pub(crate) use file::FileClaim;
#[cfg_attr(not(test), allow(unused_imports))]
pub(crate) use format::{RawRegisterRow, RawSnapshot};

fn context(message: &'static str) -> ErrorContext {
    ErrorContext::new(message).expect("static bounded JSONL context")
}
fn invalid(message: &'static str) -> StorageError {
    StorageError::InvalidData(context(message))
}
fn corrupt(message: &'static str) -> StorageError {
    StorageError::Corruption(context(message))
}
fn unsupported(message: &'static str) -> StorageError {
    StorageError::Unsupported(context(message))
}
fn conflict(message: &'static str) -> StorageError {
    StorageError::Conflict(context(message))
}
fn closed() -> StorageError {
    StorageError::Closed(context("JSONL handle is closed"))
}
fn io_error(_: std::io::Error) -> StorageError {
    io_failure()
}
fn io_failure() -> StorageError {
    StorageError::Io(context("JSONL filesystem operation failed"))
}
fn session_error(error: StorageError) -> SessionError {
    crate::codec::storage(error)
}
fn immediate<F: std::future::Future>(future: F) -> Result<F::Output, StorageError> {
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    match std::pin::pin!(future).as_mut().poll(&mut cx) {
        std::task::Poll::Ready(result) => Ok(result),
        std::task::Poll::Pending => Err(corrupt("private Memory operation unexpectedly suspended")),
    }
}
struct FixedClock(SafeUnsigned);
impl StorageClock for FixedClock {
    fn now(&self) -> Result<SafeUnsigned, StorageError> {
        Ok(self.0)
    }
}
fn projection(
    snapshot: StorageSnapshot,
    timestamp: SafeUnsigned,
) -> Result<Arc<MemoryStorage>, StorageError> {
    Arc::new(MemoryBacking::from_snapshot(snapshot)?).open(Arc::new(FixedClock(timestamp)))
}

/// Physical maintenance options, unrelated to durable schema versions.
#[derive(Clone, Copy, Debug)]
pub struct JsonlOptions {
    /// Compact on open only when reclaimed bytes/original bytes strictly exceeds
    /// this fraction in `[0,1]`. One disables automatic open-time compaction.
    pub reclaim_threshold: FiniteF64,
}
impl Default for JsonlOptions {
    fn default() -> Self {
        Self {
            reclaim_threshold: FiniteF64::new(0.5).expect("finite fraction"),
        }
    }
}
impl JsonlOptions {
    fn validate(self) -> Result<(), StorageError> {
        if !(0.0..=1.0).contains(&self.reclaim_threshold.get()) {
            Err(invalid("invalid compaction threshold"))
        } else {
            Ok(())
        }
    }
}
#[derive(Clone)]
enum Mode {
    Native {
        good_len: u64,
        known_len: u64,
        separator: bool,
    },
    Deferred {
        source: Arc<Vec<u8>>,
    },
}
struct Live {
    file: Option<FileClaim>,
    view: Arc<MemoryStorage>,
    metadata: SessionMetadata,
    mode: Mode,
    revision: u128,
}
/// One path-owned JSONL storage incarnation. Share its Arc; close seals this owner.
pub struct JsonlStorage {
    live: Mutex<Live>,
    path: PathBuf,
    clock: Arc<dyn StorageClock>,
}

impl JsonlStorage {
    /// Open current format4/storageVersion1 with the supplied codec. Older/unknown
    /// versions require the private FileRepo migration/normalization path.
    pub fn open(
        path: impl AsRef<Path>,
        codec: Arc<SessionCodec>,
        clock: Arc<dyn StorageClock>,
        options: JsonlOptions,
    ) -> Result<Arc<Self>, SessionError> {
        options.validate().map_err(session_error)?;
        let mut claim = FileClaim::acquire(path.as_ref()).map_err(session_error)?;
        let header = claim.read_header().map_err(session_error)?;
        if header.metadata.storage_version.get() > 1 {
            return Err(SessionError::NewerStorageVersion(context(
                "stored version is newer than current",
            )));
        }
        if header.metadata.storage_version.get() < 1 {
            return Err(SessionError::Migration(context(
                "no historical storage mapping is registered",
            )));
        }
        claim.into_storage(codec, clock, options)
    }
    /// Atomically create a new empty physical store. Session/Repo seeds main using
    /// the existing checked transaction path before exposing their own handle.
    pub fn create(
        path: impl AsRef<Path>,
        metadata: SessionMetadata,
        codec: Arc<SessionCodec>,
        clock: Arc<dyn StorageClock>,
        options: JsonlOptions,
    ) -> Result<Arc<Self>, SessionError> {
        let zero = SafeUnsigned::new(0).expect("zero");
        let snapshot = StorageSnapshot {
            entries: vec![],
            registers: vec![],
            usage: vec![],
            next_seq: Some(zero),
            storage_version: metadata.storage_version,
            store_generation: metadata.store_generation,
            stats: SessionStats {
                message_count: zero,
                usage: Usage::zero(),
            },
        };
        Self::create_from_snapshot(path, metadata, snapshot, codec, clock, options)
    }
    /// Atomically import a complete current snapshot into a NEW path, preserving
    /// original row times/tokens. Existing destinations are never replaced.
    pub fn create_from_snapshot(
        path: impl AsRef<Path>,
        metadata: SessionMetadata,
        snapshot: StorageSnapshot,
        codec: Arc<SessionCodec>,
        clock: Arc<dyn StorageClock>,
        options: JsonlOptions,
    ) -> Result<Arc<Self>, SessionError> {
        options.validate().map_err(session_error)?;
        if metadata.storage_version.get() != 1 {
            return Err(SessionError::Migration(context(
                "current snapshot import requires version one",
            )));
        }
        codec.validate_snapshot(&snapshot)?;
        let raw = format::RawSnapshot::from_current(metadata.clone(), snapshot.clone())
            .map_err(session_error)?;
        let bytes = format::compact(&raw).map_err(session_error)?;
        let view =
            projection(snapshot, SafeUnsigned::new(0).expect("zero")).map_err(session_error)?;
        let len = u64::try_from(bytes.len())
            .map_err(|_| session_error(invalid("file length overflow")))?;
        let mut claim = FileClaim::fresh(path.as_ref()).map_err(session_error)?;
        let temp = claim.prepare(&bytes).map_err(session_error)?;
        drop(claim.replace(temp).map_err(session_error)?);
        Ok(Self::assembled(
            claim,
            view,
            metadata,
            Mode::Native {
                good_len: len,
                known_len: len,
                separator: false,
            },
            clock,
        ))
    }
    fn assembled(
        claim: FileClaim,
        view: Arc<MemoryStorage>,
        metadata: SessionMetadata,
        mode: Mode,
        clock: Arc<dyn StorageClock>,
    ) -> Arc<Self> {
        let path = claim.path.clone();
        Arc::new(Self {
            live: Mutex::new(Live {
                file: Some(claim),
                view,
                metadata,
                mode,
                revision: 0,
            }),
            path,
            clock,
        })
    }
    /// Seal all old views while transferring, rather than releasing, path ownership.
    /// The repository retains this claim through its identity-checked unlink.
    pub(crate) fn take_file_claim(&self) -> Result<FileClaim, StorageError> {
        self.live
            .lock()
            .map_err(|_| corrupt("JSONL owner lock poisoned"))?
            .file
            .take()
            .ok_or_else(closed)
    }
    /// Bound canonical path, retained for metadata even after this handle closes.
    pub fn path(&self) -> &Path {
        &self.path
    }
    /// Detached metadata from this actual physical/logical authority.
    pub fn metadata(&self) -> Result<SessionMetadata, StorageError> {
        self.read(|live| Ok(live.metadata.clone()))
    }
    fn read<T>(
        &self,
        read: impl FnOnce(&Live) -> Result<T, StorageError>,
    ) -> Result<T, StorageError> {
        let live = self
            .live
            .lock()
            .map_err(|_| corrupt("JSONL owner lock poisoned"))?;
        live.file.as_ref().ok_or_else(closed)?.ensure()?;
        read(&live)
    }
    /// Coherent readonly batch source, with no writer acquisition or maintenance.
    pub(crate) fn read_snapshot(&self) -> Result<(SessionMetadata, StorageSnapshot), StorageError> {
        let (snapshot, metadata, _, _) = self.capture()?;
        Ok((metadata, snapshot))
    }
    fn capture(&self) -> Result<(StorageSnapshot, SessionMetadata, Mode, u128), StorageError> {
        self.read(|live| {
            Ok((
                immediate(live.view.snapshot())??,
                live.metadata.clone(),
                live.mode.clone(),
                live.revision,
            ))
        })
    }
    fn matches(
        snapshot: &StorageSnapshot,
        conditions: &[RegisterCondition],
    ) -> Result<bool, StorageError> {
        let mut keys = std::collections::BTreeSet::new();
        for condition in conditions {
            if !keys.insert(condition.register.clone()) {
                return Err(invalid("duplicate register condition"));
            }
        }
        Ok(conditions.iter().all(|condition| {
            let row = snapshot
                .registers
                .iter()
                .find(|row| register_key(row) == condition.register);
            match (row, condition.expected) {
                (None, ExpectedRegister::Absent) => true,
                (Some(row), ExpectedRegister::Seq(seq)) => register_seq(row) == seq,
                _ => false,
            }
        }))
    }
    fn checked<V: PublicationValidator + ?Sized>(
        &self,
        request: ConditionalCommit,
        validator: &V,
    ) -> Result<ConditionalOutcome, CheckedCommitError<V::Error>> {
        use CheckedCommitError::Storage as Failure;
        loop {
            let (snapshot, metadata, mode, revision) = self.capture().map_err(Failure)?;
            if !Self::matches(&snapshot, &request.conditions).map_err(Failure)? {
                return Ok(ConditionalOutcome::NotApplied);
            }
            let prepared = (|| {
                let timestamp = self.clock.now().map_err(Failure)?;
                let staged = projection(snapshot.clone(), timestamp).map_err(Failure)?;
                let result = immediate(staged.commit_checked(request.clone(), validator))
                    .map_err(Failure)??;
                let ConditionalOutcome::Applied(result) = result else {
                    return Err(Failure(corrupt("private condition evaluation diverged")));
                };
                let frame =
                    format::commit_frame(request.transaction.clone(), &result).map_err(Failure)?;
                let replacement = match &mode {
                    Mode::Native { .. } => None,
                    Mode::Deferred { .. } => {
                        let raw = format::RawSnapshot::from_current(metadata, snapshot)
                            .map_err(Failure)?;
                        let mut bytes = format::compact(&raw).map_err(Failure)?;
                        bytes.extend_from_slice(&frame);
                        Some(bytes)
                    }
                };
                Ok((staged, result, frame, replacement))
            })();
            let mut live = self
                .live
                .lock()
                .map_err(|_| Failure(corrupt("JSONL owner lock poisoned")))?;
            if live.file.is_none() {
                return Err(Failure(closed()));
            }
            if live.revision != revision {
                continue;
            }
            let (staged, result, frame, replacement) = prepared?;
            let next_revision = revision
                .checked_add(1)
                .ok_or_else(|| Failure(invalid("JSONL publication revision exhausted")))?;
            let operation = match mode {
                Mode::Native {
                    good_len,
                    known_len,
                    separator,
                } => {
                    match live
                        .file
                        .as_mut()
                        .ok_or_else(|| Failure(closed()))?
                        .append(&frame, good_len, known_len, separator)
                    {
                        Ok(len) => Ok((
                            Mode::Native {
                                good_len: len,
                                known_len: len,
                                separator: false,
                            },
                            None,
                        )),
                        Err(failure) => {
                            if failure.restored {
                                live.mode = Mode::Native {
                                    good_len,
                                    known_len: good_len,
                                    separator,
                                };
                            }
                            let retired = if failure.sealed {
                                live.file.take()
                            } else {
                                None
                            };
                            drop(live);
                            drop(retired);
                            return Err(Failure(failure.error));
                        }
                    }
                }
                Mode::Deferred { source } => {
                    let bytes = replacement
                        .as_ref()
                        .ok_or_else(|| Failure(corrupt("missing deferred replacement")))?;
                    let len = u64::try_from(bytes.len())
                        .map_err(|_| Failure(invalid("file length overflow")))?;
                    let file = live.file.as_mut().ok_or_else(|| Failure(closed()))?;
                    if file.read_source().map_err(Failure)?.as_slice() != source.as_slice() {
                        return Err(Failure(conflict("deferred source bytes changed")));
                    }
                    let temp = file.prepare(bytes).map_err(Failure)?;
                    let old = file.replace_bound(temp, Some(&source)).map_err(Failure)?;
                    Ok((
                        Mode::Native {
                            good_len: len,
                            known_len: len,
                            separator: false,
                        },
                        old,
                    ))
                }
            };
            let (mode, old_file): (_, Option<std::fs::File>) = operation.map_err(Failure)?;
            let old_view = std::mem::replace(&mut live.view, staged);
            live.mode = mode;
            live.revision = next_revision;
            drop(live);
            drop(old_file);
            drop(old_view);
            return Ok(ConditionalOutcome::Applied(result));
        }
    }
    /// Rewrite only live canonical rows, retaining seq/time/allocator exactly.
    /// Pending legacy conversion is not an ordinary compaction operation.
    pub fn compact(&self) -> BoxFuture<'_, Result<(), StorageError>> {
        Box::pin(async move {
            loop {
                let (snapshot, metadata, mode, revision) = self.capture()?;
                if matches!(mode, Mode::Deferred { .. }) {
                    return Err(unsupported(
                        "deferred source awaits its first successful transaction",
                    ));
                }
                let bytes =
                    format::compact(&format::RawSnapshot::from_current(metadata, snapshot)?)?;
                let len =
                    u64::try_from(bytes.len()).map_err(|_| invalid("file length overflow"))?;
                let mut live = self
                    .live
                    .lock()
                    .map_err(|_| corrupt("JSONL owner lock poisoned"))?;
                if live.file.is_none() {
                    return Err(closed());
                }
                if live.revision != revision {
                    continue;
                }
                let next_revision = revision
                    .checked_add(1)
                    .ok_or_else(|| invalid("JSONL publication revision exhausted"))?;
                let file = live.file.as_mut().ok_or_else(closed)?;
                let temp = file.prepare(&bytes)?;
                let old = file.replace(temp)?;
                live.mode = Mode::Native {
                    good_len: len,
                    known_len: len,
                    separator: false,
                };
                live.revision = next_revision;
                drop(live);
                drop(old);
                return Ok(());
            }
        })
    }
}
impl FileClaim {
    /// Version gating belongs to the FileRepo before this current-codec exposure.
    pub fn into_storage(
        mut self,
        codec: Arc<SessionCodec>,
        clock: Arc<dyn StorageClock>,
        options: JsonlOptions,
    ) -> Result<Arc<JsonlStorage>, SessionError> {
        options.validate().map_err(session_error)?;
        let bytes = self.read_source().map_err(session_error)?;
        let replay = format::replay(&bytes).map_err(session_error)?;
        let snapshot = replay.snapshot.current(&codec)?;
        let canonical = format::compact(&replay.snapshot).map_err(session_error)?;
        let view =
            projection(snapshot, SafeUnsigned::new(0).expect("zero")).map_err(session_error)?;
        let storage = JsonlStorage::assembled(
            self,
            view,
            replay.header.metadata,
            Mode::Native {
                good_len: u64::try_from(replay.good_len)
                    .map_err(|_| session_error(invalid("file length overflow")))?,
                known_len: u64::try_from(bytes.len())
                    .map_err(|_| session_error(invalid("file length overflow")))?,
                separator: replay.needs_separator,
            },
            clock,
        );
        if !bytes.is_empty()
            && (bytes.len().saturating_sub(canonical.len()) as f64) / (bytes.len() as f64)
                > options.reclaim_threshold.get()
        {
            immediate(storage.compact())
                .map_err(session_error)?
                .map_err(session_error)?;
        }
        Ok(storage)
    }
    /// Bind a validated normalized state to its exact still-unchanged source.
    /// This is a physical adoption seam; no legacy node parser/mapping runs here.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn adopt_deferred(
        mut self,
        metadata: SessionMetadata,
        snapshot: StorageSnapshot,
        source: Vec<u8>,
        codec: Arc<SessionCodec>,
        clock: Arc<dyn StorageClock>,
    ) -> Result<Arc<JsonlStorage>, SessionError> {
        if self.read_source().map_err(session_error)? != source {
            return Err(session_error(conflict(
                "prepared source bytes do not match",
            )));
        }
        let end = source
            .iter()
            .position(|b| *b == b'\n')
            .unwrap_or(source.len());
        let header: JsonValue = serde_json::from_slice(&source[..end])
            .map_err(|_| session_error(corrupt("prepared source header is invalid")))?;
        let header = serde_json::to_value(header)
            .map_err(|_| session_error(invalid("source header cannot be inspected")))?;
        if header.get("id").and_then(serde_json::Value::as_str) != Some(metadata.id.as_str()) {
            return Err(session_error(conflict(
                "prepared header identity does not match",
            )));
        }
        codec.validate_snapshot(&snapshot)?;
        format::RawSnapshot::from_current(metadata.clone(), snapshot.clone())
            .map_err(session_error)?;
        let view =
            projection(snapshot, SafeUnsigned::new(0).expect("zero")).map_err(session_error)?;
        Ok(JsonlStorage::assembled(
            self,
            view,
            metadata,
            Mode::Deferred {
                source: Arc::new(source),
            },
            clock,
        ))
    }
}
struct Accept;
impl PublicationValidator for Accept {
    type Error = std::convert::Infallible;
    fn validate(&self, _: &StorageSnapshot) -> Result<(), Self::Error> {
        Ok(())
    }
}
impl AtomicStorage for JsonlStorage {
    fn commit_conditional(
        &self,
        request: ConditionalCommit,
    ) -> BoxFuture<'_, Result<ConditionalOutcome, StorageError>> {
        Box::pin(async move {
            match self.checked(request, &Accept) {
                Ok(value) => Ok(value),
                Err(CheckedCommitError::Storage(error)) => Err(error),
                Err(CheckedCommitError::Rejected(never)) => match never {},
            }
        })
    }
    fn snapshot(&self) -> BoxFuture<'_, Result<StorageSnapshot, StorageError>> {
        Box::pin(async move { self.read(|live| immediate(live.view.snapshot())?) })
    }
}
impl CheckedStorage for JsonlStorage {
    fn commit_checked<'a, V: PublicationValidator + ?Sized>(
        &'a self,
        request: ConditionalCommit,
        validator: &'a V,
    ) -> BoxFuture<'a, Result<ConditionalOutcome, CheckedCommitError<V::Error>>> {
        Box::pin(async move { self.checked(request, validator) })
    }
}
impl Storage for JsonlStorage {
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
        Box::pin(async move { self.read(|live| immediate(live.view.get_entries(ids))?) })
    }
    fn get_register<'a>(
        &'a self,
        namespace: RegisterNamespace,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Option<Register>, StorageError>> {
        Box::pin(
            async move { self.read(|live| immediate(live.view.get_register(namespace, key))?) },
        )
    }
    fn list_registers<'a>(
        &'a self,
        namespace: RegisterNamespace,
        key_prefix: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Vec<Register>, StorageError>> {
        Box::pin(async move {
            self.read(|live| immediate(live.view.list_registers(namespace, key_prefix))?)
        })
    }
    fn scan_branch(&self, query: BranchScan) -> BoxFuture<'_, Result<Vec<Entry>, StorageError>> {
        Box::pin(async move { self.read(|live| immediate(live.view.scan_branch(query))?) })
    }
    fn scan_branch_structure(
        &self,
        query: BranchScan,
    ) -> BoxFuture<'_, Result<Vec<EntryStructure>, StorageError>> {
        Box::pin(
            async move { self.read(|live| immediate(live.view.scan_branch_structure(query))?) },
        )
    }
    fn scan_entries(&self, query: EntryScan) -> BoxFuture<'_, Result<Vec<Entry>, StorageError>> {
        Box::pin(async move { self.read(|live| immediate(live.view.scan_entries(query))?) })
    }
    fn scan_usage(&self, query: UsageScan) -> BoxFuture<'_, Result<Vec<UsageRow>, StorageError>> {
        Box::pin(async move { self.read(|live| immediate(live.view.scan_usage(query))?) })
    }
    fn stats(&self) -> BoxFuture<'_, Result<SessionStats, StorageError>> {
        Box::pin(async move { self.read(|live| immediate(live.view.stats())?) })
    }
    fn close(&self) -> BoxFuture<'_, Result<(), StorageError>> {
        Box::pin(async move {
            let retired = {
                let mut live = self
                    .live
                    .lock()
                    .map_err(|_| corrupt("JSONL owner lock poisoned"))?;
                live.file.take()
            };
            drop(retired);
            Ok(())
        })
    }
}
