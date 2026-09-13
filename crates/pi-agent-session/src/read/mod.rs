//! Detached repository reads with durable prefix cursors, never writer ownership.
use pi_agent_schema::session_values::SafeUnsigned;
use pi_agent_schema::{
    Entry, ErrorContext, SessionError, SessionMetadata, SessionMetadataRecord, StorageError,
};
use ring::{
    digest::{Context, SHA256},
    rand::{SecureRandom, SystemRandom},
};
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    sync::Mutex,
};
fn invalid() -> SessionError {
    SessionError::InvalidOperation(
        ErrorContext::new("invalid repository read request").expect("static"),
    )
}
fn io(_: std::io::Error) -> SessionError {
    SessionError::Storage(StorageError::Io(
        ErrorContext::new("repository read filesystem failure").expect("static"),
    ))
}
fn frame(hash: &mut Context, bytes: &[u8]) -> Result<(), SessionError> {
    let n = u64::try_from(bytes.len()).map_err(|_| invalid())?;
    hash.update(&n.to_be_bytes());
    hash.update(bytes);
    Ok(())
}
/// Opaque stable source scope. It is an isolation identity, not authorization.
#[derive(Clone, PartialEq, Eq)]
pub struct SourceDomain(Vec<u8>);
impl std::fmt::Debug for SourceDomain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SourceDomain([REDACTED])")
    }
}
impl SourceDomain {
    /// Durable bytes to bind a derived consumer to this repository owner.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
    /// Stable version-one file-domain digest from kind and exact canonical OS path.
    pub fn for_files(kind: &str, root: &Path) -> Result<Self, SessionError> {
        if kind.is_empty() {
            return Err(invalid());
        }
        let root = std::fs::canonicalize(root).map_err(io)?;
        if !root.is_dir() {
            return Err(invalid());
        }
        let mut hash = Context::new(&SHA256);
        hash.update(b"pi-harness/read-domain\0v1\0");
        frame(&mut hash, kind.as_bytes())?;
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStrExt;
            frame(&mut hash, b"unix")?;
            frame(&mut hash, root.as_os_str().as_bytes())?;
        }
        #[cfg(windows)]
        {
            use std::os::windows::ffi::OsStrExt;
            let bytes: Vec<_> = root
                .as_os_str()
                .encode_wide()
                .flat_map(u16::to_le_bytes)
                .collect();
            frame(&mut hash, b"windows-utf16le")?;
            frame(&mut hash, &bytes)?;
        }
        #[cfg(not(any(unix, windows)))]
        {
            frame(&mut hash, std::env::consts::OS.as_bytes())?;
            frame(&mut hash, root.as_os_str().as_encoded_bytes())?;
        }
        let mut bytes = vec![1];
        bytes.extend_from_slice(hash.finish().as_ref());
        Ok(Self(bytes))
    }
}
#[derive(Default)]
pub(crate) struct LazyDomain(Mutex<Option<SourceDomain>>);
impl LazyDomain {
    pub fn get(&self) -> Result<SourceDomain, SessionError> {
        self.get_with(|bytes| {
            SystemRandom::new().fill(bytes).map_err(|_| {
                SessionError::Storage(StorageError::Io(
                    ErrorContext::new("repository domain entropy unavailable").expect("static"),
                ))
            })
        })
    }
    fn get_with(
        &self,
        fill: impl FnOnce(&mut [u8]) -> Result<(), SessionError>,
    ) -> Result<SourceDomain, SessionError> {
        let mut current = self.0.lock().map_err(|_| invalid())?;
        if let Some(value) = &*current {
            return Ok(value.clone());
        }
        let mut bytes = vec![1; 33];
        fill(&mut bytes[1..])?;
        let domain = SourceDomain(bytes);
        *current = Some(domain.clone());
        Ok(domain)
    }
}
/// File names reserved by a repository in one canonical directory.
#[derive(Clone, PartialEq, Eq)]
pub struct ManagedFileDomain {
    root: PathBuf,
    suffix: String,
}
impl std::fmt::Debug for ManagedFileDomain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ManagedFileDomain([REDACTED])")
    }
}
impl ManagedFileDomain {
    /// Declare a nonempty ASCII filename suffix such as `.jsonl` or `.sqlite`.
    pub fn new(root: &Path, suffix: &str) -> Result<Self, SessionError> {
        if suffix.is_empty() || !suffix.is_ascii() || suffix.contains(['/', '\\']) {
            return Err(invalid());
        }
        let root = std::fs::canonicalize(root).map_err(io)?;
        if !root.is_dir() {
            return Err(invalid());
        }
        Ok(Self {
            root,
            suffix: suffix.into(),
        })
    }
    fn matches(&self, path: &Path) -> bool {
        path.parent() == Some(self.root.as_path())
            && path
                .file_name()
                .is_some_and(|name| name.as_encoded_bytes().ends_with(self.suffix.as_bytes()))
    }
    /// Check exact resolved parent/name and any existing resolved target. Errors
    /// remain errors; a missing/unreadable parent is not proof of unrelated scope.
    pub fn contains_path(&self, path: &Path) -> Result<bool, SessionError> {
        let leaf = path.file_name().ok_or_else(invalid)?;
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let lexical = std::fs::canonicalize(parent).map_err(io)?.join(leaf);
        if self.matches(&lexical) {
            return Ok(true);
        }
        match std::fs::canonicalize(path) {
            Ok(actual) => Ok(self.matches(&actual)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(io(error)),
        }
    }
}
/// Durable, backend-owned progress token. None is before the first entry, not seq0.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReadCursor {
    /// Actual source rewrite generation.
    pub generation: SafeUnsigned,
    /// Greatest processed entry sequence; register/usage gaps do not advance it.
    pub last_entry_seq: Option<SafeUnsigned>,
    /// Versioned opaque backend bytes, preserved unchanged by consumers.
    pub witness: Vec<u8>,
}
impl std::fmt::Debug for ReadCursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadCursor")
            .field("generation", &self.generation)
            .field("last_entry_seq", &self.last_entry_seq)
            .finish_non_exhaustive()
    }
}
/// Explicit positive bounded page request; zero is invalid rather than unbounded.
#[derive(Clone, Debug)]
pub struct ReadBatchRequest {
    pub cursor: Option<ReadCursor>,
    pub limit: SafeUnsigned,
}
/// One coherent canonical snapshot page, including nonmessage entries.
#[derive(Clone, Debug, PartialEq)]
pub struct ReadBatch {
    /// Actual source identity/version/generation, not caller override fields.
    pub metadata: SessionMetadata,
    /// Ascending detached entries selected from one source snapshot.
    pub entries: Vec<Entry>,
    /// Persist only together with processing these entries.
    pub next_cursor: ReadCursor,
    /// No unprocessed entries remain in this captured snapshot.
    pub complete: bool,
    /// The supplied cursor no longer identifies its indexed prefix; replace old output.
    pub reset: bool,
}
/// Synchronous read authority implemented on real repositories, independent of writers.
pub trait RepositoryRead: Send + Sync {
    /// The actual repository's metadata type.
    type Metadata: SessionMetadataRecord;
    /// The actual repository's explicit listing options.
    type ListOptions;
    /// Actual source-domain identity, lazily fallible for Memory.
    fn source_domain(&self) -> Result<SourceDomain, SessionError>;
    /// Generic managed canonical filename scope, if this is a file repository.
    fn managed_files(&self) -> Result<Option<ManagedFileDomain>, SessionError>;
    /// Authoritative complete discovery, never failed/Busy-as-empty.
    fn read_list(&self, options: Self::ListOptions) -> Result<Vec<Self::Metadata>, SessionError>;
    /// Read a source-bound detached page without acquiring/opening a Session writer.
    fn read_batch(
        &self,
        metadata: &Self::Metadata,
        request: ReadBatchRequest,
    ) -> Result<ReadBatch, SessionError>;
}
fn witness(
    metadata: &SessionMetadata,
    entries: &[Entry],
    through: Option<SafeUnsigned>,
) -> Result<Vec<u8>, SessionError> {
    let mut hash = Context::new(&SHA256);
    hash.update(b"pi-harness/read-prefix\0v1\0");
    frame(&mut hash, metadata.id.as_str().as_bytes())?;
    hash.update(&metadata.store_generation.get().to_be_bytes());
    for entry in entries {
        let seq = crate::storage::values::structure(entry).seq;
        if through.is_some_and(|last| seq <= last) {
            frame(
                &mut hash,
                &serde_json::to_vec(entry).map_err(|_| invalid())?,
            )?;
        }
    }
    let mut bytes = vec![1];
    bytes.extend_from_slice(hash.finish().as_ref());
    Ok(bytes)
}
pub(crate) fn prefix_batch(
    metadata: SessionMetadata,
    entries: Vec<Entry>,
    request: ReadBatchRequest,
) -> Result<ReadBatch, SessionError> {
    if request.limit.get() == 0 {
        return Err(invalid());
    }
    let mut reset = false;
    let mut last = None;
    if let Some(cursor) = request.cursor {
        if cursor.witness.len() != 33 || cursor.witness.first() != Some(&1) {
            return Err(invalid());
        }
        let exists = cursor.last_entry_seq.is_none_or(|seq| {
            entries
                .iter()
                .any(|e| crate::storage::values::structure(e).seq == seq)
        });
        if cursor.generation != metadata.store_generation
            || !exists
            || witness(&metadata, &entries, cursor.last_entry_seq)? != cursor.witness
        {
            reset = true;
        } else {
            last = cursor.last_entry_seq;
        }
    }
    let limit = usize::try_from(request.limit.get()).unwrap_or(usize::MAX);
    let mut remaining = entries
        .iter()
        .filter(|e| last.is_none_or(|last| crate::storage::values::structure(e).seq > last));
    let page: Vec<_> = remaining.by_ref().take(limit).cloned().collect();
    let complete = remaining.next().is_none();
    let through = page
        .last()
        .map(|e| crate::storage::values::structure(e).seq)
        .or(last);
    let next_cursor = ReadCursor {
        generation: metadata.store_generation,
        last_entry_seq: through,
        witness: witness(&metadata, &entries, through)?,
    };
    Ok(ReadBatch {
        metadata,
        entries: page,
        next_cursor,
        complete,
        reset,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn lazy_random_domain_failure_does_not_install_or_consume_next_attempt() {
        let domain = LazyDomain::default();
        assert!(domain.get_with(|_| Err(invalid())).is_err());
        let actual = domain
            .get_with(|bytes| {
                bytes.fill(7);
                Ok(())
            })
            .unwrap();
        assert_eq!(&actual.as_bytes()[1..], &[7; 32]);
        assert_eq!(
            actual,
            domain
                .get_with(|_| panic!("cached owner domain must not regenerate"))
                .unwrap()
        );
    }
}
