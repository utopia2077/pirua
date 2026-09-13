//! Explicit indexed read requirements and detached structural validation.
use super::*;
use pi_agent_schema::{EntryId, EntryStructure};
use std::collections::{BTreeMap, BTreeSet};
/// Complete declared entry reads and optional whole pending-key namespace.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct IndexedReadSet {
    /// Exact IDs/keys, including requested absences and legal empty pending keys.
    pub entry_ids: BTreeSet<String>,
    /// Request ALL final current pending.entry keys and an entry lookup for each.
    pub all_pending_keys: bool,
}
/// Distinct structural projection; never a sparse complete StorageSnapshot.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct IndexedProjection {
    /// Every declared exact lookup, explicitly None when absent.
    pub entries: BTreeMap<String, Option<EntryStructure>>,
    /// Some means the entire final current namespace was read; None means unrequested.
    pub pending_keys: Option<BTreeSet<String>>,
}
impl IndexedReadSet {
    /// Check explicit coverage/identity of a backend's final-state projection.
    /// The backend remains responsible for actually enumerating the whole namespace.
    pub fn check(&self, projection: &IndexedProjection) -> Result<(), StorageError> {
        let invalid = || {
            StorageError::InvalidData(
                pi_agent_schema::ErrorContext::new("incomplete indexed publication projection")
                    .expect("static"),
            )
        };
        let mut required = self.entry_ids.clone();
        if self.all_pending_keys {
            required.extend(
                projection
                    .pending_keys
                    .as_ref()
                    .ok_or_else(invalid)?
                    .iter()
                    .cloned(),
            );
        }
        if required
            .iter()
            .any(|id| !projection.entries.contains_key(id))
            || projection
                .entries
                .iter()
                .any(|(id, row)| row.as_ref().is_some_and(|r| r.id.as_str() != id))
        {
            return Err(invalid());
        }
        Ok(())
    }
    /// Project a genuinely complete snapshot, useful for reference/equivalence tests.
    pub fn project(&self, snapshot: &StorageSnapshot) -> IndexedProjection {
        let pending_keys = self.all_pending_keys.then(|| {
            snapshot
                .registers
                .iter()
                .filter_map(|r| match r {
                    Register::PendingEntry { key, .. } => Some(key.clone()),
                    _ => None,
                })
                .collect::<BTreeSet<_>>()
        });
        let mut required = self.entry_ids.clone();
        if let Some(keys) = &pending_keys {
            required.extend(keys.iter().cloned());
        }
        let entries = required
            .into_iter()
            .map(|id| {
                let row = EntryId::new(id.clone())
                    .ok()
                    .and_then(|entry_id| {
                        snapshot
                            .entries
                            .iter()
                            .find(|e| crate::storage::values::structure(e).id == entry_id)
                    })
                    .map(crate::storage::values::structure);
                (id, row)
            })
            .collect();
        IndexedProjection {
            entries,
            pending_keys,
        }
    }
}
/// Optional structural equivalent of a complete publication validator.
pub trait IndexedPublicationValidator: Send + Sync {
    /// Same error channel as the complete validator.
    type Error: Send;
    /// Read requirements against the final candidate after all ordered writes apply.
    fn read_set(&self) -> IndexedReadSet;
    /// Validate explicit final structures/absence and the complete requested namespace.
    fn validate_projection(&self, projection: &IndexedProjection) -> Result<(), Self::Error>;
}
