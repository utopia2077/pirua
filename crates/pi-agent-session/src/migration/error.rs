use super::*;

/// Invalid registry construction; no converter or store is called.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum MigrationRegistryError {
    /// The target is not a positive storage version.
    InvalidTarget,
    /// A step is zero-based, backward, equal, or skips a version.
    NonconsecutiveStep,
    /// A step exceeds the selected target.
    BeyondTarget,
    /// More than one step begins at the same version.
    DuplicateSource,
}
impl fmt::Display for MigrationRegistryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidTarget => "migration target must be positive",
            Self::NonconsecutiveStep => "migration steps must advance one positive version",
            Self::BeyondTarget => "migration step exceeds selected target",
            Self::DuplicateSource => "duplicate migration source version",
        })
    }
}
impl std::error::Error for MigrationRegistryError {}

/// Content-free converter rejection with an optional retained, Debug-redacted source.
#[derive(Clone, Default)]
pub struct MigrationConversionError {
    source: Option<SharedErrorSource>,
}
impl MigrationConversionError {
    /// Reject conversion without copying any stored content into diagnostics.
    pub fn new() -> Self {
        Self::default()
    }
    /// Retain a source chain without formatting it in operator-facing diagnostics.
    pub fn with_source(source: SharedErrorSource) -> Self {
        Self {
            source: Some(source),
        }
    }
}
impl fmt::Display for MigrationConversionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("register migration conversion rejected")
    }
}
impl fmt::Debug for MigrationConversionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("MigrationConversionError([REDACTED])")
    }
}
impl std::error::Error for MigrationConversionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn std::error::Error + 'static))
    }
}

/// Typed migration failure. Diagnostics never include keys, values or source text.
#[non_exhaustive]
pub enum MigrationError {
    /// Actual stored version is newer than the selected binary/fixture target.
    NewerStorageVersion {
        /// Stored version.
        stored: SafeUnsigned,
        /// Selected target.
        target: SafeUnsigned,
    },
    /// The complete remaining chain is unavailable; nothing was converted.
    MissingStep {
        /// First missing source.
        from: SafeUnsigned,
        /// Selected target.
        target: SafeUnsigned,
    },
    /// Store returned duplicate current keys or sequence tokens.
    InvalidSnapshot,
    /// Pure conversion failed before publication of this step.
    Conversion {
        /// Source version of the failed step.
        from: SafeUnsigned,
        /// Retained content-free conversion error.
        source: MigrationConversionError,
    },
    /// Backend acquisition/read/publication failed atomically.
    Storage(StorageError),
    /// An adapter acknowledged publication but reported a different stored version.
    StoreObservation,
}
impl MigrationError {
    /// Stable process-local category, independent of raw backend/source messages.
    pub fn category(&self) -> &'static str {
        match self {
            Self::NewerStorageVersion { .. } => "newer_storage_version",
            Self::MissingStep { .. } => "missing_step",
            Self::InvalidSnapshot => "invalid_snapshot",
            Self::Conversion { .. } => "conversion",
            Self::Storage(_) => "storage",
            Self::StoreObservation => "store_observation",
        }
    }
}
impl fmt::Display for MigrationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "migration failed: {}", self.category())
    }
}
impl fmt::Debug for MigrationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MigrationError")
            .field("category", &self.category())
            .finish_non_exhaustive()
    }
}
impl std::error::Error for MigrationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Conversion { source, .. } => Some(source),
            Self::Storage(source) => Some(source),
            _ => None,
        }
    }
}
