//! Process-local atomic publication and snapshot capabilities.

use pi_agent_schema::session_values::SafeUnsigned;
use pi_agent_schema::{
    BoxFuture, CommitResult, Entry, Register, RegisterNamespace, SessionStats, Storage,
    StorageError, Transaction, UsageRow,
};
use serde::Serialize;

/// An exact register identity; empty and arbitrary Unicode keys are preserved.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct RegisterKey {
    /// Closed namespace.
    pub namespace: RegisterNamespace,
    /// Exact namespace-local key.
    pub key: String,
}

/// Expected presence, independent of the register's value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExpectedRegister {
    /// No current register, including no null-valued register.
    Absent,
    /// Exact last-set sequence; zero is a real token.
    Seq(SafeUnsigned),
}

/// One prestate predicate for an atomic conditional commit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegisterCondition {
    /// Exact namespace and key.
    pub register: RegisterKey,
    /// Expected presence or sequence.
    pub expected: ExpectedRegister,
}

/// Process-local conditions around an unchanged durable transaction.
#[derive(Clone, Debug)]
pub struct ConditionalCommit {
    /// Conditions all compare the same committed prestate.
    pub conditions: Vec<RegisterCondition>,
    /// Ordered candidate writes.
    pub transaction: Transaction,
}

/// Normal conditional mismatch is not a failed storage publication.
#[derive(Clone, Debug, PartialEq)]
pub enum ConditionalOutcome {
    /// The entire transaction was published.
    Applied(CommitResult),
    /// At least one condition did not match; no authority changed.
    NotApplied,
}

/// Complete detached store authority for repository operations and conformance.
/// This process-local snapshot is not a durable format or a public Storage scan.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct StorageSnapshot {
    /// Complete entries in sequence order, retaining original timestamps.
    pub entries: Vec<Entry>,
    /// Current registers in namespace/key order.
    pub registers: Vec<Register>,
    /// Ledger in sequence order.
    pub usage: Vec<UsageRow>,
    /// Next assignable sequence; None means MAX has already been assigned.
    pub next_seq: Option<SafeUnsigned>,
    /// Current storage schema version.
    pub storage_version: SafeUnsigned,
    /// Precise rewrite generation, distinct from a handle incarnation.
    pub store_generation: SafeUnsigned,
    /// Maintained ledger and message projection.
    pub stats: SessionStats,
}

/// Failure before publication validation versus a storage mechanism failure.
#[derive(Debug)]
pub enum CheckedCommitError<E> {
    /// Trusted pure candidate validation rejected before admission.
    Rejected(E),
    /// The storage mechanism failed without partial publication.
    Storage(StorageError),
}

/// A trusted pure staged-state validator. It may be rerun on contention.
/// No schema retrieval, provider action, or other application effect belongs here.
pub trait PublicationValidator: Send + Sync {
    /// Session-owned rejection category.
    type Error: Send;
    /// Optional equivalent structural validator; None retains complete validation.
    fn indexed(&self) -> Option<&dyn IndexedPublicationValidator<Error = Self::Error>> {
        None
    }
    /// Validate a complete detached candidate, never intermediate writes.
    fn validate(&self, staged: &StorageSnapshot) -> Result<(), Self::Error>;
}

/// Optional backend capabilities without changing the accepted Storage inventory.
pub trait AtomicStorage: Storage {
    /// Compare all conditions atomically with publication.
    fn commit_conditional(
        &self,
        request: ConditionalCommit,
    ) -> BoxFuture<'_, Result<ConditionalOutcome, StorageError>>;
    /// Obtain one coherent detached authority snapshot.
    fn snapshot(&self) -> BoxFuture<'_, Result<StorageSnapshot, StorageError>>;
}

pub(crate) mod values;

/// Backend-neutral checked publication for a generic Session consumer.
/// The generic validator keeps its method-specific rejection type outside Storage.
pub trait CheckedStorage: AtomicStorage {
    /// Validate a detached complete stage and publish only against that exact
    /// committed prestate; retry validation when concurrent publication intervenes.
    fn commit_checked<'a, V: PublicationValidator + ?Sized>(
        &'a self,
        request: ConditionalCommit,
        validator: &'a V,
    ) -> BoxFuture<'a, Result<ConditionalOutcome, CheckedCommitError<V::Error>>>;
}

mod indexed;
pub use indexed::{IndexedProjection, IndexedPublicationValidator, IndexedReadSet};

mod runtime;
pub use runtime::{ReadStamp, RuntimeAttachment, RuntimeLease, RuntimeStorage};

mod prepared;
pub use prepared::{PreparedEntries, PreparedEntryAuthority, PreparedEntrySlot};
