//! Backend-owned optimistic entry metadata, with no allocator reservation.
use super::{ReadStamp, values::materialize};
use pi_agent_schema::session_values::SafeUnsigned;
use pi_agent_schema::{Entry, ErrorContext, NewEntry, StorageError, Transaction, Write};
use std::sync::Arc;

/// An entry's absolute position among all writes in its eventual transaction.
#[derive(Clone, Debug, PartialEq)]
pub struct PreparedEntrySlot {
    /// Zero-based transaction-write ordinal, including nonentry writes.
    pub write_index: usize,
    /// Exact payload and parent that must be published at this ordinal.
    pub entry: NewEntry,
}

/// Inert, single-use backend preparation. Only exact candidate rows are exposed.
pub struct PreparedEntries {
    authority: Arc<()>,
    stamp: ReadStamp,
    next: Option<SafeUnsigned>,
    timestamp: SafeUnsigned,
    slots: Vec<PreparedEntrySlot>,
    entries: Vec<Entry>,
}
impl PreparedEntries {
    /// Candidate rows in transaction order, conditional on successful publication.
    pub fn entries(&self) -> &[Entry] {
        &self.entries
    }
}
impl std::fmt::Debug for PreparedEntries {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PreparedEntries([OPAQUE])")
    }
}

/// Backend implementation capability. Keep one private issuer per incarnation;
/// never return this issuer to ordinary callers. A caller-created issuer cannot
/// authenticate preparations against another backend's private issuer.
#[derive(Default)]
pub struct PreparedEntryAuthority(Arc<()>);
fn invalid() -> StorageError {
    StorageError::InvalidData(
        ErrorContext::new("prepared entry inventory is invalid").expect("static context"),
    )
}
impl PreparedEntryAuthority {
    /// Create an independent backend-incarnation issuer.
    pub fn new() -> Self {
        Self::default()
    }
    /// Materialize entries from metadata already observed and rechecked by backend.
    pub fn prepare(
        &self,
        stamp: ReadStamp,
        next: Option<SafeUnsigned>,
        timestamp: SafeUnsigned,
        slots: Vec<PreparedEntrySlot>,
    ) -> Result<PreparedEntries, StorageError> {
        let mut prior = None;
        let mut entries = Vec::with_capacity(slots.len());
        for slot in &slots {
            if prior.is_some_and(|index| index >= slot.write_index) {
                return Err(invalid());
            }
            prior = Some(slot.write_index);
            let offset = u64::try_from(slot.write_index).map_err(|_| invalid())?;
            let seq = next
                .and_then(|n| n.get().checked_add(offset))
                .and_then(|n| SafeUnsigned::new(n).ok())
                .ok_or_else(invalid)?;
            entries.push(materialize(slot.entry.clone(), seq, timestamp));
        }
        Ok(PreparedEntries {
            authority: self.0.clone(),
            stamp,
            next,
            timestamp,
            slots,
            entries,
        })
    }
    /// Check provenance and prestate before exposing frozen row time to a backend.
    /// None is ordinary staleness, including foreign or reopened incarnations.
    pub fn timestamp(
        &self,
        prepared: &PreparedEntries,
        stamp: &ReadStamp,
        next: Option<SafeUnsigned>,
    ) -> Option<SafeUnsigned> {
        (Arc::ptr_eq(&self.0, &prepared.authority)
            && prepared.stamp == *stamp
            && prepared.next == next)
            .then_some(prepared.timestamp)
    }
    /// Enforce complete entry inventory and all-write ordinals before staging.
    pub fn validate_inventory(
        &self,
        prepared: &PreparedEntries,
        transaction: &Transaction,
    ) -> Result<(), StorageError> {
        if !Arc::ptr_eq(&self.0, &prepared.authority) {
            return Err(invalid());
        }
        let mut slots = prepared.slots.iter();
        for (index, write) in transaction.writes.iter().enumerate() {
            if let Write::Entry(entry) = write {
                let slot = slots.next().ok_or_else(invalid)?;
                if slot.write_index != index || slot.entry != *entry {
                    return Err(invalid());
                }
            }
        }
        if slots.next().is_some() {
            return Err(invalid());
        }
        Ok(())
    }
}
