//! Actual private Memory publication adapter, used by Repository::open.
use super::*;
use crate::migration::*;
use crate::storage::values::{register_key, register_seq};
use std::sync::atomic::{AtomicBool, Ordering};

pub(super) struct MemoryMigrationStore {
    claim: Arc<Claim>,
    leased: Arc<AtomicBool>,
}
impl MemoryMigrationStore {
    pub(super) fn new(claim: Arc<Claim>) -> Self {
        Self {
            claim,
            leased: Arc::new(AtomicBool::new(false)),
        }
    }
}
pub(super) struct MemoryMigrationLease {
    claim: Arc<Claim>,
    leased: Arc<AtomicBool>,
}
impl Drop for MemoryMigrationLease {
    fn drop(&mut self) {
        self.leased.store(false, Ordering::Release);
    }
}
impl MigrationStore for MemoryMigrationStore {
    type Value = Register;
    type Lease = MemoryMigrationLease;
    fn acquire(&self) -> BoxFuture<'_, Result<Self::Lease, StorageError>> {
        Box::pin(async move {
            if self
                .leased
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                return Err(as_storage(busy()));
            }
            Ok(MemoryMigrationLease {
                claim: self.claim.clone(),
                leased: self.leased.clone(),
            })
        })
    }
}
impl MigrationLease for MemoryMigrationLease {
    type Value = Register;
    fn read(&mut self) -> BoxFuture<'_, Result<MigrationSnapshot<Register>, StorageError>> {
        Box::pin(async move {
            let (_, snapshot) = self.claim.snapshot().await.map_err(as_storage)?;
            Ok(MigrationSnapshot {
                version: snapshot.storage_version,
                registers: snapshot
                    .registers
                    .into_iter()
                    .map(|value| MigrationRegister {
                        key: register_key(&value),
                        seq: register_seq(&value),
                        value,
                    })
                    .collect(),
            })
        })
    }
    fn publish(
        &mut self,
        publication: MigrationPublication<Register>,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        Box::pin(async move {
            let (_, old_backing, raw, borrowed) = self.claim.raw().map_err(as_storage)?;
            if borrowed.is_some() {
                return Err(as_storage(busy()));
            }
            let mut candidate = raw.snapshot().await?;
            let (versions, registers) = publication.into_parts();
            if candidate.storage_version != versions.from {
                return Err(StorageError::Conflict(context(
                    "migration source version changed",
                )));
            }
            let expected: BTreeMap<_, _> = candidate
                .registers
                .iter()
                .map(|value| (register_key(value), register_seq(value)))
                .collect();
            let actual: BTreeMap<_, _> = registers
                .iter()
                .map(|row| (row.key.clone(), row.seq))
                .collect();
            if actual != expected
                || registers.len() != expected.len()
                || registers.iter().any(|row| {
                    register_key(&row.value) != row.key || register_seq(&row.value) != row.seq
                })
            {
                return Err(StorageError::InvalidData(context(
                    "migration register envelope changed",
                )));
            }
            candidate.registers = registers.into_iter().map(|row| row.value).collect();
            candidate.storage_version = versions.to;
            self.claim
                .owner
                .codec
                .validate_snapshot(&candidate)
                .map_err(as_storage)?;
            let next = Arc::new(MemoryBacking::from_snapshot(candidate)?);
            #[cfg(test)]
            self.claim.owner.hooks.before_swap(versions.to)?;
            raw.close().await?;
            let retired = {
                let mut catalog = self.claim.owner.lock().map_err(as_storage)?;
                let record = self.claim.checked(&mut catalog).map_err(as_storage)?;
                if !record
                    .backing
                    .as_ref()
                    .is_some_and(|backing| Arc::ptr_eq(backing, &old_backing))
                {
                    return Err(StorageError::Conflict(context("migration backing changed")));
                }
                (record.backing.replace(next), record.active.take())
            };
            drop(retired);
            #[cfg(test)]
            self.claim.owner.hooks.after_swap(versions.to);
            Ok(())
        })
    }
}
