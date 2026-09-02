//! Portable model-catalog storage and its in-memory reference implementation.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::sync::{Arc, Mutex};

use crate::id::ProviderId;
use crate::model::{ModelsStoreEntry, RawModel, RawModelsStoreEntry};
use crate::runtime::{BoxFuture, CancellationToken};

/// Stable model-store error category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ModelsStoreErrorCode {
    /// Cancellation won before effect admission.
    Cancelled,
    /// The backend proved the complete prior state remains authoritative.
    Failed,
}

/// Redacted failure from a model-store operation.
///
/// ```compile_fail
/// fn requires_serialize<T: serde::Serialize>() {}
/// requires_serialize::<pi_ai_core::ModelsStoreError>();
/// ```
pub struct ModelsStoreError {
    code: ModelsStoreErrorCode,
    source: Option<Box<dyn Error + Send + Sync + 'static>>,
}

impl ModelsStoreError {
    /// Creates a categorized failure without a backend source.
    pub const fn new(code: ModelsStoreErrorCode) -> Self {
        Self { code, source: None }
    }

    /// Creates a normal backend failure while retaining its source.
    pub fn with_source(source: impl Error + Send + Sync + 'static) -> Self {
        Self {
            code: ModelsStoreErrorCode::Failed,
            source: Some(Box::new(source)),
        }
    }

    /// Returns the stable failure category.
    pub const fn code(&self) -> ModelsStoreErrorCode {
        self.code
    }
}

impl fmt::Debug for ModelsStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ModelsStoreError")
            .field("code", &self.code)
            .field("source_present", &self.source.is_some())
            .finish()
    }
}

impl fmt::Display for ModelsStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self.code {
            ModelsStoreErrorCode::Cancelled => "models store operation cancelled",
            ModelsStoreErrorCode::Failed => "models store operation failed",
        })
    }
}

impl Error for ModelsStoreError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn Error + 'static))
    }
}

/// Object-safe storage for detached provider model catalogs.
///
/// Each operation's first poll is admission. Cancellation before admission
/// preserves the old value; cancellation or future drop after admission cannot
/// cancel the independently driven definitive result. A normal failure proves
/// that the complete old value remains authoritative.
pub trait ModelsStore: Send + Sync {
    /// Reads a detached raw entry so the registry can reapply its current policy.
    fn read<'a>(
        &'a self,
        provider: &'a ProviderId,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<RawModelsStoreEntry>, ModelsStoreError>>;

    /// Atomically replaces one provider's validated catalog.
    fn write<'a>(
        &'a self,
        provider: &'a ProviderId,
        entry: ModelsStoreEntry,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), ModelsStoreError>>;

    /// Atomically removes one provider's catalog.
    fn delete<'a>(
        &'a self,
        provider: &'a ProviderId,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), ModelsStoreError>>;
}

/// In-memory model store whose operations complete definitively on first poll.
///
/// ```compile_fail
/// fn requires_serialize<T: serde::Serialize>() {}
/// requires_serialize::<pi_ai_core::InMemoryModelsStore>();
/// ```
pub struct InMemoryModelsStore {
    entries: Mutex<BTreeMap<ProviderId, Arc<ModelsStoreEntry>>>,
}

impl InMemoryModelsStore {
    /// Creates an empty in-memory model store.
    pub fn new() -> Self {
        Self {
            entries: Mutex::new(BTreeMap::new()),
        }
    }
}

impl Default for InMemoryModelsStore {
    fn default() -> Self {
        Self::new()
    }
}

fn to_raw(entry: &ModelsStoreEntry) -> RawModelsStoreEntry {
    RawModelsStoreEntry {
        models: entry.models.iter().map(RawModel::from).collect(),
        last_modified: entry.last_modified,
        checked_at: entry.checked_at,
        etag: entry.etag.clone(),
    }
}

impl ModelsStore for InMemoryModelsStore {
    fn read<'a>(
        &'a self,
        provider: &'a ProviderId,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<RawModelsStoreEntry>, ModelsStoreError>> {
        Box::pin(async move {
            if cancellation.is_cancelled() {
                return Err(ModelsStoreError::new(ModelsStoreErrorCode::Cancelled));
            }
            let entry = {
                let entries = self
                    .entries
                    .lock()
                    .unwrap_or_else(|poison| poison.into_inner());
                entries.get(provider).map(Arc::clone)
            };
            Ok(entry.as_deref().map(to_raw))
        })
    }

    fn write<'a>(
        &'a self,
        provider: &'a ProviderId,
        entry: ModelsStoreEntry,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), ModelsStoreError>> {
        let entry = Arc::new(entry);
        Box::pin(async move {
            if cancellation.is_cancelled() {
                return Err(ModelsStoreError::new(ModelsStoreErrorCode::Cancelled));
            }
            let old = self
                .entries
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .insert(provider.clone(), entry);
            drop(old);
            Ok(())
        })
    }

    fn delete<'a>(
        &'a self,
        provider: &'a ProviderId,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), ModelsStoreError>> {
        Box::pin(async move {
            if cancellation.is_cancelled() {
                return Err(ModelsStoreError::new(ModelsStoreErrorCode::Cancelled));
            }
            let old = self
                .entries
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .remove(provider);
            drop(old);
            Ok(())
        })
    }
}
