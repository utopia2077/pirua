//! External JSONL session import at the current Rust repository boundary.
//!
//! `JsonlSessionRepo::open_path` is intentionally used as-is. It accepts an
//! explicit external path, preserves source bytes while the session is only
//! observed, and performs legacy-v3 normalization/adoption on the first
//! checked write. The upstream application copy-into-destination behavior is
//! an explicit M4 divergence and is not reimplemented here.

use pi_agent::schema::SessionError;
use pi_agent_session::repo::jsonl::{JsonlSession, JsonlSessionRepo};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

/// Host-selected external JSONL source opened through the existing repository.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LegacyImportSource {
    path: PathBuf,
}

impl LegacyImportSource {
    /// Records a source path without reading or mutating it.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    /// Returns the caller-selected source path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Opens the source using current JSONL and legacy-v3 repository semantics.
    pub async fn open(
        &self,
        repository: &JsonlSessionRepo,
    ) -> Result<Arc<JsonlSession>, SessionError> {
        repository.open_path(&self.path).await
    }
}

/// Opens one external JSONL source without copying or mutating its bytes.
pub async fn open_external_jsonl(
    repository: &JsonlSessionRepo,
    path: impl AsRef<Path>,
) -> Result<Arc<JsonlSession>, SessionError> {
    repository.open_path(path).await
}
