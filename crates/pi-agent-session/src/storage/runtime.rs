//! Backend-incarnation ownership and bounded coherent-read observations.
use super::{
    CheckedCommitError, CheckedStorage, ConditionalCommit, ConditionalOutcome, PreparedEntries,
    PreparedEntrySlot, PublicationValidator,
};
use pi_agent_schema::{BoxFuture, ErrorContext, StorageError};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

/// Additional capability required by a concrete Harness runtime. Ordinary Storage
/// clones retain their existing authority; this claim only excludes another runtime.
pub trait RuntimeStorage: CheckedStorage {
    /// The cell owned by this actual backend incarnation, shared by all its views.
    fn runtime_attachment(&self) -> &RuntimeAttachment;
    /// Observe the protected nonwrapping publication revision after checking the
    /// same live/fenced authority as ordinary reads. Never captures a full snapshot.
    fn read_stamp(&self) -> BoxFuture<'_, Result<ReadStamp, StorageError>>;
    /// Prepare exact entry metadata without reserving sequences or publishing.
    fn prepare_entries(
        &self,
        _expected_stamp: ReadStamp,
        _slots: Vec<PreparedEntrySlot>,
    ) -> BoxFuture<'_, Result<Option<PreparedEntries>, StorageError>> {
        Box::pin(async { Err(unsupported()) })
    }
    /// Publish the final validated transaction only against its prepared prestate.
    fn commit_prepared_checked<'a, V: PublicationValidator + ?Sized>(
        &'a self,
        _prepared: PreparedEntries,
        _request: ConditionalCommit,
        _validator: &'a V,
    ) -> BoxFuture<'a, Result<ConditionalOutcome, CheckedCommitError<V::Error>>> {
        Box::pin(async { Err(CheckedCommitError::Storage(unsupported())) })
    }
}

/// One local attachment cell per backend incarnation. Deliberately not Clone.
#[derive(Default)]
pub struct RuntimeAttachment {
    identity: Arc<AtomicBool>,
}
impl RuntimeAttachment {
    /// Allocate a fresh incarnation identity, independent of logical SessionId.
    pub fn new() -> Self {
        Self::default()
    }
    /// Claim exclusive local runtime ownership until the returned lease is dropped.
    /// This does not open a closed backend or replace its storage fencing checks.
    pub fn try_claim(&self) -> Result<RuntimeLease, StorageError> {
        self.identity
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| {
                StorageError::Busy(
                    ErrorContext::new("storage runtime is already attached")
                        .expect("bounded static context"),
                )
            })?;
        Ok(RuntimeLease {
            identity: self.identity.clone(),
        })
    }
    /// Backend implementation seam: call only while observing the actual protected
    /// revision and live authority. Every publication/rewrite must advance revision
    /// without wrapping; reopening must allocate a new RuntimeAttachment.
    pub fn stamp(&self, revision: u128) -> ReadStamp {
        ReadStamp {
            identity: self.identity.clone(),
            revision,
        }
    }
}

/// Non-Clone RAII claim. Retaining a stamp does not retain the runtime claim.
#[must_use = "dropping the lease releases the runtime attachment"]
pub struct RuntimeLease {
    identity: Arc<AtomicBool>,
}
impl Drop for RuntimeLease {
    fn drop(&mut self) {
        self.identity.store(false, Ordering::Release);
    }
}

/// Opaque process-local equality token. Retaining the identity allocation prevents
/// address reuse while any earlier stamp can still be compared.
#[derive(Clone)]
pub struct ReadStamp {
    identity: Arc<AtomicBool>,
    revision: u128,
}
impl PartialEq for ReadStamp {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.identity, &other.identity) && self.revision == other.revision
    }
}
impl Eq for ReadStamp {}
impl std::fmt::Debug for ReadStamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ReadStamp([OPAQUE])")
    }
}

fn unsupported() -> StorageError {
    StorageError::Unsupported(
        ErrorContext::new("prepared publication is unsupported").expect("static context"),
    )
}
