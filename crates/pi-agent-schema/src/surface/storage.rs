//! One-session, agent-agnostic storage interface.

use std::collections::BTreeMap;

use crate::surface::BoxFuture;
use crate::{
    BranchScan, CommitResult, Entry, EntryId, EntryScan, EntryStructure, Register,
    RegisterNamespace, SessionStats, StorageError, Transaction, UsageRow, UsageScan,
};

/// Detached entry lookup result keyed in deterministic order.
pub type EntryMap = BTreeMap<EntryId, Entry>;

/// Behavior-free one-session storage contract.
pub trait Storage: Send + Sync {
    /// Atomically commits ordered writes.
    fn commit(&self, transaction: Transaction)
    -> BoxFuture<'_, Result<CommitResult, StorageError>>;

    /// Reads detached entries by exact identity.
    fn get_entries<'a>(
        &'a self,
        ids: &'a [EntryId],
    ) -> BoxFuture<'a, Result<EntryMap, StorageError>>;

    /// Reads one current register.
    fn get_register<'a>(
        &'a self,
        namespace: RegisterNamespace,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Option<Register>, StorageError>>;

    /// Lists current registers in one namespace and optional key prefix.
    fn list_registers<'a>(
        &'a self,
        namespace: RegisterNamespace,
        key_prefix: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Vec<Register>, StorageError>>;

    /// Scans one branch with payloads.
    fn scan_branch(&self, query: BranchScan) -> BoxFuture<'_, Result<Vec<Entry>, StorageError>>;

    /// Scans one branch without payload fields.
    fn scan_branch_structure(
        &self,
        query: BranchScan,
    ) -> BoxFuture<'_, Result<Vec<EntryStructure>, StorageError>>;

    /// Scans the session-wide entry inventory.
    fn scan_entries(&self, query: EntryScan) -> BoxFuture<'_, Result<Vec<Entry>, StorageError>>;

    /// Scans append-only usage rows.
    fn scan_usage(&self, query: UsageScan) -> BoxFuture<'_, Result<Vec<UsageRow>, StorageError>>;

    /// Reads maintained session statistics.
    fn stats(&self) -> BoxFuture<'_, Result<SessionStats, StorageError>>;

    /// Idempotently closes this storage instance.
    fn close(&self) -> BoxFuture<'_, Result<(), StorageError>>;
}

/// Exact storage method inventory used by source-conformance guards.
pub const STORAGE_METHODS: &[&str] = &[
    "commit",
    "get_entries",
    "get_register",
    "list_registers",
    "scan_branch",
    "scan_branch_structure",
    "scan_entries",
    "scan_usage",
    "stats",
    "close",
];
