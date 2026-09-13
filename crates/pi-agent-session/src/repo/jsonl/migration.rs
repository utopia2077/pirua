//! Actual raw FileClaim migration publication, retained until final Session exposure.
use super::*;
use crate::{jsonl::RawRegisterRow, migration::*};
use std::sync::atomic::{AtomicBool, Ordering};
pub(super) struct JsonlMigrationStore {
    state: Arc<State>,
}
struct State {
    file: Mutex<Option<FileClaim>>,
    leased: AtomicBool,
    owner: Arc<Inner>,
    validation: Mutex<Option<SessionError>>,
}
pub(super) struct JsonlMigrationLease {
    state: Arc<State>,
}
impl JsonlMigrationStore {
    pub(super) fn new(file: FileClaim, owner: Arc<Inner>) -> Self {
        Self {
            state: Arc::new(State {
                file: Mutex::new(Some(file)),
                leased: AtomicBool::new(false),
                owner,
                validation: Mutex::new(None),
            }),
        }
    }
    pub(super) fn validation_error(&self) -> Option<SessionError> {
        self.state.validation.lock().ok()?.take()
    }
    pub(super) fn into_claim(self) -> Result<FileClaim, SessionError> {
        if self.state.leased.load(Ordering::Acquire) {
            return Err(busy());
        }
        self.state
            .file
            .lock()
            .map_err(|_| damaged())?
            .take()
            .ok_or_else(damaged)
    }
}
impl Drop for JsonlMigrationLease {
    fn drop(&mut self) {
        self.state.leased.store(false, Ordering::Release);
    }
}
impl MigrationStore for JsonlMigrationStore {
    type Value = JsonValue;
    type Lease = JsonlMigrationLease;
    fn acquire(&self) -> BoxFuture<'_, Result<Self::Lease, StorageError>> {
        Box::pin(async move {
            if self
                .state
                .leased
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                return Err(as_storage(busy()));
            }
            Ok(JsonlMigrationLease {
                state: self.state.clone(),
            })
        })
    }
}
impl MigrationLease for JsonlMigrationLease {
    type Value = JsonValue;
    fn read(&mut self) -> BoxFuture<'_, Result<MigrationSnapshot<JsonValue>, StorageError>> {
        Box::pin(async move {
            let raw = self
                .state
                .file
                .lock()
                .map_err(|_| as_storage(damaged()))?
                .as_mut()
                .ok_or_else(|| as_storage(damaged()))?
                .read_raw()?;
            Ok(MigrationSnapshot {
                version: raw.metadata.storage_version,
                registers: raw
                    .registers
                    .into_iter()
                    .map(|r| MigrationRegister {
                        key: r.key,
                        seq: r.seq,
                        value: r.value,
                    })
                    .collect(),
            })
        })
    }
    fn publish(
        &mut self,
        publication: MigrationPublication<JsonValue>,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        Box::pin(async move {
            let (versions, registers) = publication.into_parts();
            let registers: Vec<_> = registers
                .into_iter()
                .map(|r| RawRegisterRow {
                    key: r.key,
                    seq: r.seq,
                    value: r.value,
                })
                .collect();
            if versions.to == self.state.owner.migrations.target() {
                let mut candidate = self
                    .state
                    .file
                    .lock()
                    .map_err(|_| as_storage(damaged()))?
                    .as_mut()
                    .ok_or_else(|| as_storage(damaged()))?
                    .read_raw()?;
                candidate.registers = registers.clone();
                candidate.metadata.storage_version = versions.to;
                if let Err(error) = candidate.current(&self.state.owner.codec) {
                    *self
                        .state
                        .validation
                        .lock()
                        .map_err(|_| as_storage(damaged()))? = Some(error);
                    return Err(as_storage(invalid()));
                }
            }
            #[cfg(test)]
            self.state
                .owner
                .hooks
                .before_migration(versions.to)
                .map_err(as_storage)?;
            self.state
                .file
                .lock()
                .map_err(|_| as_storage(damaged()))?
                .as_mut()
                .ok_or_else(|| as_storage(damaged()))?
                .rewrite_registers(versions.from, versions.to, registers)?;
            Ok(())
        })
    }
}
