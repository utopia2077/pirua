//! Owned path claims and actual unbuffered filesystem publication.
use super::*;
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write as IoWrite};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

#[derive(Default)]
struct Registry {
    next: u128,
    paths: BTreeMap<PathBuf, u128>,
}
static CLAIMS: OnceLock<Mutex<Registry>> = OnceLock::new();
fn registry() -> &'static Mutex<Registry> {
    CLAIMS.get_or_init(|| Mutex::new(Registry::default()))
}
fn ticket() -> Result<u128, StorageError> {
    let mut registry = registry()
        .lock()
        .map_err(|_| corrupt("path registry lock poisoned"))?;
    let value = registry.next;
    registry.next = value
        .checked_add(1)
        .ok_or_else(|| invalid("path ownership counter exhausted"))?;
    Ok(value)
}
fn reserve(path: &Path) -> Result<u128, StorageError> {
    let mut registry = registry()
        .lock()
        .map_err(|_| corrupt("path registry lock poisoned"))?;
    if registry.paths.contains_key(path) {
        return Err(StorageError::Busy(context("JSONL path is already owned")));
    }
    let value = registry.next;
    registry.next = value
        .checked_add(1)
        .ok_or_else(|| invalid("path ownership counter exhausted"))?;
    registry.paths.insert(path.to_owned(), value);
    Ok(value)
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Identity {
    #[cfg(unix)]
    dev: u64,
    #[cfg(unix)]
    ino: u64,
}
fn identity(metadata: &fs::Metadata) -> Result<Identity, StorageError> {
    if !metadata.is_file() {
        return Err(unsupported("JSONL input must be a regular file"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.nlink() > 1 {
            return Err(unsupported("hard-linked session files are unsupported"));
        }
        Ok(Identity {
            dev: metadata.dev(),
            ino: metadata.ino(),
        })
    }
    #[cfg(not(unix))]
    {
        Ok(Identity {})
    }
}
/// Private owned file capability retained by the later FileRepo across migration/adoption.
/// Existing paths are canonicalized; no cross-process exclusion is promised.
pub(crate) struct FileClaim {
    pub path: PathBuf,
    token: u128,
    file: Option<File>,
    writable: bool,
    identity: Option<Identity>,
    #[cfg(test)]
    pub(super) hooks: Arc<tests::Hooks>,
}
impl Drop for FileClaim {
    fn drop(&mut self) {
        if let Ok(mut registry) = registry().lock()
            && registry.paths.get(&self.path) == Some(&self.token)
        {
            registry.paths.remove(&self.path);
        }
    }
}
impl FileClaim {
    pub fn acquire(path: &Path) -> Result<Self, StorageError> {
        let path = fs::canonicalize(path).map_err(io_error)?;
        let token = reserve(&path)?;
        let mut claim = Self {
            path,
            token,
            file: None,
            writable: false,
            identity: None,
            #[cfg(test)]
            hooks: Arc::new(tests::Hooks::default()),
        };
        identity(&fs::metadata(&claim.path).map_err(io_error)?)?;
        let file = OpenOptions::new()
            .read(true)
            .open(&claim.path)
            .map_err(io_error)?;
        claim.identity = Some(identity(&file.metadata().map_err(io_error)?)?);
        claim.file = Some(file);
        claim.ensure()?;
        Ok(claim)
    }
    pub fn fresh(path: &Path) -> Result<Self, StorageError> {
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let leaf = path
            .file_name()
            .ok_or_else(|| invalid("new file path lacks a leaf"))?;
        let path = fs::canonicalize(parent).map_err(io_error)?.join(leaf);
        let token = reserve(&path)?;
        let claim = Self {
            path,
            token,
            file: None,
            writable: false,
            identity: None,
            #[cfg(test)]
            hooks: Arc::new(tests::Hooks::default()),
        };
        claim.ensure()?;
        Ok(claim)
    }
    pub fn ensure(&self) -> Result<(), StorageError> {
        if registry()
            .lock()
            .map_err(|_| corrupt("path registry lock poisoned"))?
            .paths
            .get(&self.path)
            != Some(&self.token)
        {
            return Err(closed());
        }
        match self.identity {
            Some(expected) => {
                let target = fs::metadata(&self.path)
                    .map_err(|_| conflict("owned path no longer names its file"))?;
                if identity(&target)? != expected {
                    return Err(conflict("owned path names a different file"));
                }
                let file = self.file.as_ref().ok_or_else(closed)?;
                if identity(&file.metadata().map_err(io_error)?)? != expected {
                    return Err(conflict("opened file identity changed"));
                }
            }
            None => match fs::symlink_metadata(&self.path) {
                Ok(_) => return Err(conflict("destination already exists")),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
                Err(error) => return Err(io_error(error)),
            },
        }
        Ok(())
    }
    /// Remove only this still-owned file object. The caller checks its header ID.
    /// Absence is idempotent; the path token remains held until this claim drops.
    pub(crate) fn unlink_owned(&mut self) -> Result<(), StorageError> {
        match fs::symlink_metadata(&self.path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(io_error(error)),
            Ok(_) => (),
        }
        self.ensure()?;
        fs::remove_file(&self.path).map_err(io_error)
    }
    pub fn read_source(&mut self) -> Result<Vec<u8>, StorageError> {
        self.ensure()?;
        let file = self.file.as_mut().ok_or_else(closed)?;
        file.seek(SeekFrom::Start(0)).map_err(io_error)?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).map_err(io_error)?;
        self.ensure()?;
        Ok(bytes)
    }
    pub fn read_header(&mut self) -> Result<format::Header, StorageError> {
        format::header(&self.read_source()?)
    }
    pub fn read_raw(&mut self) -> Result<format::RawSnapshot, StorageError> {
        Ok(format::replay(&self.read_source()?)?.snapshot)
    }
    pub fn prepare(&self, bytes: &[u8]) -> Result<Temporary, StorageError> {
        self.ensure()?;
        #[cfg(test)]
        self.hooks.check(tests::Point::BeforeTemp)?;
        let parent = self
            .path
            .parent()
            .ok_or_else(|| invalid("claimed path lacks parent"))?;
        let path = parent.join(format!(
            ".pi-jsonl-{}-{}.tmp",
            std::process::id(),
            ticket()?
        ));
        let mut options = OpenOptions::new();
        options.read(true).write(true).create_new(true);
        // Temporary payloads start private even when replacing a broader file.
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options.open(&path).map_err(io_error)?;
        let mut temp = Temporary {
            path: Some(path),
            file: Some(file),
        };
        temp.file
            .as_mut()
            .ok_or_else(closed)?
            .write_all(bytes)
            .map_err(io_error)?;
        Ok(temp)
    }
    /// Complete rename has no fallible reopen afterward; the preopened descriptor wins.
    pub fn replace(&mut self, temp: Temporary) -> Result<Option<File>, StorageError> {
        self.replace_bound(temp, None)
    }
    pub fn replace_bound(
        &mut self,
        mut temp: Temporary,
        source: Option<&[u8]>,
    ) -> Result<Option<File>, StorageError> {
        self.ensure()?;
        let next_identity = identity(
            &temp
                .file
                .as_ref()
                .ok_or_else(closed)?
                .metadata()
                .map_err(io_error)?,
        )?;
        #[cfg(test)]
        self.hooks.check(tests::Point::BeforeRename)?;
        // Recheck after a test-scheduled identity change as well as before publication.
        self.ensure()?;
        if let Some(source) = source
            && self.read_source()?.as_slice() != source
        {
            return Err(conflict("deferred source bytes changed"));
        }
        // Copy existing Unix mode bits (or the platform permissions abstraction)
        // only after writing, before publication. No ACL/ownership promise.
        if let Some(original) = &self.file {
            let permissions = original.metadata().map_err(io_error)?.permissions();
            temp.file
                .as_ref()
                .ok_or_else(closed)?
                .set_permissions(permissions)
                .map_err(io_error)?;
        }
        fs::rename(temp.path.as_ref().ok_or_else(closed)?, &self.path).map_err(io_error)?;
        temp.path = None;
        let old = self
            .file
            .replace(temp.file.take().expect("prepared temporary file"));
        self.identity = Some(next_identity);
        self.writable = true;
        // Publication already happened. No injected post-rename error can undo it.
        #[cfg(test)]
        let _ = self.hooks.check(tests::Point::AfterRename);
        Ok(old)
    }
    pub fn append(
        &mut self,
        frame: &[u8],
        good_len: u64,
        known_len: u64,
        separator: bool,
    ) -> Result<u64, AppendFailure> {
        self.ensure().map_err(AppendFailure::unchanged)?;
        if !self.writable {
            let writable = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&self.path)
                .map_err(|e| AppendFailure::unchanged(io_error(e)))?;
            let actual = identity(
                &writable
                    .metadata()
                    .map_err(|e| AppendFailure::unchanged(io_error(e)))?,
            )
            .map_err(AppendFailure::unchanged)?;
            if Some(actual) != self.identity {
                return Err(AppendFailure::unchanged(conflict(
                    "append descriptor identity changed",
                )));
            }
            self.ensure().map_err(AppendFailure::unchanged)?;
            self.file = Some(writable);
            self.writable = true;
        }
        let file = self
            .file
            .as_mut()
            .ok_or_else(|| AppendFailure::unchanged(closed()))?;
        if file
            .metadata()
            .map_err(|e| AppendFailure::unchanged(io_error(e)))?
            .len()
            != known_len
        {
            return Err(AppendFailure::unchanged(conflict(
                "owned file length changed",
            )));
        }
        let added = u64::try_from(frame.len())
            .map_err(|_| AppendFailure::unchanged(invalid("frame length overflow")))?;
        let new_len = good_len
            .checked_add(u64::from(separator))
            .and_then(|n| n.checked_add(added))
            .ok_or_else(|| AppendFailure::unchanged(invalid("file offset overflow")))?;
        let operation = (|| {
            if known_len != good_len {
                file.set_len(good_len).map_err(io_error)?;
            }
            file.seek(SeekFrom::Start(good_len)).map_err(io_error)?;
            if separator {
                file.write_all(b"\n").map_err(io_error)?;
            }
            #[cfg(test)]
            if let Some(count) = self.hooks.append_prefix.lock().unwrap().take() {
                file.write_all(&frame[..count.min(frame.len())])
                    .map_err(io_error)?;
                return Err(io_failure());
            }
            file.write_all(frame).map_err(io_error)?;
            Ok(())
        })();
        if let Err(error) = operation {
            let restored = self.ensure().is_ok() && self.restore(good_len).is_ok();
            return Err(AppendFailure {
                error,
                restored,
                sealed: !restored,
            });
        }
        Ok(new_len)
    }
    fn restore(&mut self, len: u64) -> Result<(), StorageError> {
        #[cfg(test)]
        self.hooks.check(tests::Point::Rollback)?;
        self.file
            .as_mut()
            .ok_or_else(closed)?
            .set_len(len)
            .map_err(io_error)
    }
    /// Actual register-only physical rewrite; later FileRepo holds its outer claim.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn rewrite_registers(
        &mut self,
        from: SafeUnsigned,
        to: SafeUnsigned,
        registers: Vec<format::RawRegisterRow>,
    ) -> Result<(), StorageError> {
        let mut snapshot = self.read_raw()?;
        if snapshot.metadata.storage_version != from {
            return Err(conflict("migration version changed"));
        }
        if from.get().checked_add(1) != Some(to.get()) {
            return Err(invalid("migration version is not consecutive"));
        }
        let expected: BTreeMap<_, _> = snapshot
            .registers
            .iter()
            .map(|row| (row.key.clone(), row.seq))
            .collect();
        let actual: BTreeMap<_, _> = registers
            .iter()
            .map(|row| (row.key.clone(), row.seq))
            .collect();
        if expected != actual || registers.len() != expected.len() {
            return Err(invalid("migration register envelope changed"));
        }
        snapshot.registers = registers;
        snapshot.metadata.storage_version = to;
        let bytes = format::compact(&snapshot)?;
        let temp = self.prepare(&bytes)?;
        drop(self.replace(temp)?);
        Ok(())
    }
}
pub(crate) struct Temporary {
    path: Option<PathBuf>,
    file: Option<File>,
}
impl Drop for Temporary {
    fn drop(&mut self) {
        if let Some(path) = &self.path {
            let _ = fs::remove_file(path);
        }
    }
}
pub(crate) struct AppendFailure {
    pub error: StorageError,
    pub restored: bool,
    pub sealed: bool,
}
impl AppendFailure {
    fn unchanged(error: StorageError) -> Self {
        Self {
            error,
            restored: false,
            sealed: false,
        }
    }
}
