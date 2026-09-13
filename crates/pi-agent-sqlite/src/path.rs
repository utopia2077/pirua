//! Independently opened safe file locks: every SQL connection is nested inside one scope.
use super::*;
use std::fs::{File, OpenOptions};
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Identity {
    pub dev: u64,
    pub ino: u64,
}
pub(crate) fn identity(meta: &std::fs::Metadata) -> Result<Identity, StorageError> {
    if !meta.is_file() {
        return Err(invalid("SQLite path is not a regular file"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if meta.nlink() > 1 {
            return Err(StorageError::Unsupported(context(
                "hard-linked SQLite files are unsupported",
            )));
        }
        Ok(Identity {
            dev: meta.dev(),
            ino: meta.ino(),
        })
    }
    #[cfg(not(unix))]
    {
        let _ = meta;
        Err(StorageError::Unsupported(context(
            "SQLite physical identity is unsupported on this host",
        )))
    }
}
pub(crate) fn existing(path: &Path) -> Result<PathBuf, StorageError> {
    std::fs::canonicalize(path).map_err(io)
}
pub(crate) fn fresh(path: &Path) -> Result<PathBuf, StorageError> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let name = path
        .file_name()
        .ok_or_else(|| invalid("SQLite path lacks filename"))?;
    Ok(std::fs::canonicalize(parent).map_err(io)?.join(name))
}
fn coordination(path: &Path) -> PathBuf {
    let digest = ring::digest::digest(&ring::digest::SHA256, path.as_os_str().as_encoded_bytes());
    let hex: String = digest.as_ref().iter().map(|b| format!("{b:02x}")).collect();
    path.parent()
        .expect("canonical parent")
        .join(format!(".pi-sqlite-{hex}.lock"))
}
pub(crate) struct Scope {
    _file: File,
}
impl Drop for Scope {
    fn drop(&mut self) {
        // End this logical scope even if process creation temporarily inherited
        // the open-file description. Independent scopes use independent opens.
        let _ = self._file.unlock();
    }
}
impl Scope {
    #[cfg(test)]
    pub(crate) fn inherited_descriptor(&self) -> File {
        self._file.try_clone().unwrap()
    }
    pub fn acquire(path: &Path, exclusive: bool) -> Result<Self, StorageError> {
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options.open(coordination(path)).map_err(io)?;
        // Each scope has its own open-file description. Never clone a held lock FD.
        let result = if exclusive {
            file.try_lock()
        } else {
            file.try_lock_shared()
        };
        result.map_err(|e| match e {
            std::fs::TryLockError::WouldBlock => busy(),
            std::fs::TryLockError::Error(e) => io(e),
        })?;
        Ok(Self { _file: file })
    }
}
pub(crate) fn verify(path: &Path, expected: Identity) -> Result<(), StorageError> {
    let actual = identity(&std::fs::metadata(path).map_err(io)?)?;
    if actual != expected {
        return Err(closed());
    }
    Ok(())
}
pub(crate) fn inspect_identity(path: &Path) -> Result<Identity, StorageError> {
    // The path scope excludes cooperating replacement. Never open an unmanaged
    // database FD: closing it could cancel SQLite's process-owned POSIX locks.
    identity(&std::fs::metadata(path).map_err(io)?)
}
pub(crate) fn connection(
    path: &Path,
    options: SqliteOptions,
    readonly: bool,
) -> Result<Connection, StorageError> {
    let flags = if readonly {
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
    } else {
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
    };
    let connection =
        Connection::open_with_flags(path, flags | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX)
            .map_err(sql)?;
    connection
        .busy_timeout(Duration::from_millis(options.busy_timeout_ms.get()))
        .map_err(sql)?;
    if readonly {
        connection
            .pragma_update(None, "query_only", true)
            .map_err(sql)?;
    } else {
        connection
            .pragma_update(
                None,
                "synchronous",
                match options.synchronous {
                    Synchronous::Full => "FULL",
                    Synchronous::Normal => "NORMAL",
                },
            )
            .map_err(sql)?;
    }
    #[cfg(test)]
    let connection = {
        let connection = connection;
        connection.trace_v2(
            rusqlite::trace::TraceEventCodes::SQLITE_TRACE_STMT,
            Some(crate::tests::trace),
        );
        connection
    };
    Ok(connection)
}
pub(crate) struct Temporary {
    pub path: PathBuf,
    published: bool,
}
impl Temporary {
    pub fn new(target: &Path) -> Result<Self, StorageError> {
        let bytes = token()?;
        let hex: String = bytes.0.iter().map(|b| format!("{b:02x}")).collect();
        let path = target
            .parent()
            .expect("canonical")
            .join(format!(".pi-sqlite-temp-{hex}"));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        drop(options.open(&path).map_err(io)?);
        Ok(Self {
            path,
            published: false,
        })
    }
    pub fn publish(mut self, target: &Path) -> Result<(), StorageError> {
        match std::fs::symlink_metadata(target) {
            Ok(_) => return Err(StorageError::Conflict(context("SQLite destination exists"))),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
            Err(e) => return Err(io(e)),
        }
        std::fs::rename(&self.path, target).map_err(io)?;
        self.published = true;
        #[cfg(test)]
        crate::tests::after_publish(target);
        Ok(())
    }
}
impl Drop for Temporary {
    fn drop(&mut self) {
        if !self.published {
            let _ = std::fs::remove_file(&self.path);
            for suffix in ["-wal", "-shm"] {
                let mut name = self.path.as_os_str().to_os_string();
                name.push(suffix);
                let _ = std::fs::remove_file(PathBuf::from(name));
            }
        }
    }
}

impl Temporary {
    /// Replace only under an already-verified exclusive path scope. Every old and
    /// temporary connection is closed before entry; no old WAL authority survives.
    pub(crate) fn replace(
        mut self,
        target: &Path,
        _exclusive: &Scope,
        permissions: std::fs::Permissions,
    ) -> Result<(), StorageError> {
        std::fs::set_permissions(&self.path, permissions).map_err(io)?;
        std::fs::rename(&self.path, target).map_err(io)?;
        self.published = true;
        Ok(())
    }
}
