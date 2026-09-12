//! Generic, process-local migrate-on-open machinery.
//!
//! Production storage is version one and has no historical mapping. Registering
//! a future operation mapping requires its schema owner's reviewed migration;
//! this engine does not authorize moving pending effects back to pre-intent.
//! Converters receive only detached registers, without time, IDs or effect ports.
//! They are trusted pure Rust implementations: captured effects cannot be
//! prevented by Rust's trait system and are outside the converter contract.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;

use crate::storage::RegisterKey;
use pi_agent_schema::session_values::SafeUnsigned;
use pi_agent_schema::{BoxFuture, SharedErrorSource, StorageError};

mod error;
pub use error::*;

/// A register envelope whose stable identity/token are owned by the engine.
/// `V` is a detached value representation chosen by the version-aware adapter;
/// it is not an invented historical OperationState or a new durable wire format.
pub struct MigrationRegister<V> {
    /// Exact namespace and key, including legal empty keys.
    pub key: RegisterKey,
    /// Retained sequence token; migration does not mint new ordinary writes.
    pub seq: SafeUnsigned,
    /// Detached version-specific payload.
    pub value: V,
}
impl<V> fmt::Debug for MigrationRegister<V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("MigrationRegister([REDACTED])")
    }
}

/// One coherent authoritative read under an exclusive migration lease.
pub struct MigrationSnapshot<V> {
    /// Actual stored version, never caller-supplied discovery metadata.
    pub version: SafeUnsigned,
    /// Every current register exactly once, across all namespaces.
    pub registers: Vec<MigrationRegister<V>>,
}
impl<V> fmt::Debug for MigrationSnapshot<V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MigrationSnapshot")
            .field("version", &self.version)
            .field("register_count", &self.registers.len())
            .finish_non_exhaustive()
    }
}

/// Consecutive source and destination versions of one complete step.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MigrationVersions {
    /// Stored source version.
    pub from: SafeUnsigned,
    /// Exactly source plus one.
    pub to: SafeUnsigned,
}

/// One atomic publication, constructible only by the validated engine.
/// The adapter must apply all values AND the target version together, preserve
/// keys/tokens/allocation/entries/usage, and acknowledge only definitive success.
pub struct MigrationPublication<V> {
    versions: MigrationVersions,
    registers: Vec<MigrationRegister<V>>,
}
impl<V> MigrationPublication<V> {
    /// Expected committed source version and new version.
    pub fn versions(&self) -> MigrationVersions {
        self.versions
    }
    /// Complete candidate register set with original keys and tokens.
    pub fn registers(&self) -> &[MigrationRegister<V>] {
        &self.registers
    }
    /// Consume the complete publication for the adapter's atomic transaction.
    pub fn into_parts(self) -> (MigrationVersions, Vec<MigrationRegister<V>>) {
        (self.versions, self.registers)
    }
}
impl<V> fmt::Debug for MigrationPublication<V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MigrationPublication")
            .field("versions", &self.versions)
            .field("register_count", &self.registers.len())
            .finish_non_exhaustive()
    }
}

/// Exclusive writer capability spanning the whole migration chain.
/// Reads return detached complete registers. Ordinary writes/open publication
/// cannot interleave. Persistent adapters retain fencing and writer ownership.
/// Dropping a waiting future must not abandon an admitted step: the adapter must
/// reach a definitive atomic result before relinquishing the writer claim.
pub trait MigrationLease: Send {
    /// Detached version-aware payload representation; no shared mutable aliases.
    type Value: Send + Sync;
    /// Read actual committed version and complete current registers coherently.
    fn read(&mut self) -> BoxFuture<'_, Result<MigrationSnapshot<Self::Value>, StorageError>>;
    /// Atomically publish only if source version and all register identities/tokens
    /// still match the lease's committed state. Error publishes nothing. No entry,
    /// usage, allocator or unrelated metadata mutation is allowed.
    fn publish(
        &mut self,
        publication: MigrationPublication<Self::Value>,
    ) -> BoxFuture<'_, Result<(), StorageError>>;
}

/// Backend capability used before a repository exposes an opened Session.
/// The repository supplies the real adapter; this is not part of Storage's ten
/// methods and grants no unrestricted backing replacement to normal callers.
pub trait MigrationStore: Send + Sync {
    /// Detached version-aware register values.
    type Value: Send + Sync;
    /// Owned exclusive writer claim, released on drop after definitive work.
    type Lease: MigrationLease<Value = Self::Value>;
    /// Obtain fenced/exclusive ownership before the first authoritative read.
    fn acquire(&self) -> BoxFuture<'_, Result<Self::Lease, StorageError>>;
}

/// Trusted deterministic field mapping, invoked once per register in a step.
/// Returning only a value prevents converter changes to the envelope's keys/seqs.
/// Every reachable historical operation variant must be handled by its future
/// schema owner; there is no identity/default fallback on conversion failure.
pub trait RegisterMigration<V>: Send + Sync {
    /// Convert this value without effects or mutation of the borrowed source.
    fn convert(&self, register: &MigrationRegister<V>) -> Result<V, MigrationConversionError>;
}

/// A supplied mapping between consecutive versions. Construction is passive;
/// the registry validates the entire declared graph before accepting it.
pub struct MigrationStep<V> {
    /// Source/destination version numbers.
    pub versions: MigrationVersions,
    /// Trusted pure mapping, retained without executing it at registration.
    pub converter: Arc<dyn RegisterMigration<V>>,
}

/// Explicit successful observation, including equal-version/no-op opens.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MigrationOutcome {
    /// Authoritative version seen after acquiring the writer claim.
    pub initial_version: SafeUnsigned,
    /// Authoritative final stored version, equal to the selected target.
    pub current_version: SafeUnsigned,
    /// Steps actually acknowledged during this call, empty for an equal version.
    pub completed_steps: Vec<MigrationVersions>,
}

/// An immutable validated step registry. No production historical mappings ship.
pub struct MigrationRegistry<V> {
    target: SafeUnsigned,
    steps: BTreeMap<SafeUnsigned, MigrationStep<V>>,
}
impl<V: Send + Sync> Default for MigrationRegistry<V> {
    fn default() -> Self {
        Self::current()
    }
}
impl<V: Send + Sync> MigrationRegistry<V> {
    /// Production version-one registry, deliberately with no historical steps.
    pub fn current() -> Self {
        Self {
            target: SafeUnsigned::new(1).expect("version one is safe"),
            steps: BTreeMap::new(),
        }
    }
    /// Construct generic machinery for an explicitly selected target. Targets
    /// beyond one require future approved mappings; tests label them SYNTHETIC.
    /// Gaps on an actual requested upgrade path are rejected before conversion.
    pub fn new(
        target: SafeUnsigned,
        steps: Vec<MigrationStep<V>>,
    ) -> Result<Self, MigrationRegistryError> {
        if target.get() == 0 {
            return Err(MigrationRegistryError::InvalidTarget);
        }
        let mut by_source = BTreeMap::new();
        for step in steps {
            let versions = step.versions;
            if versions.from.get() == 0
                || versions.from.get().checked_add(1) != Some(versions.to.get())
            {
                return Err(MigrationRegistryError::NonconsecutiveStep);
            }
            if versions.to > target {
                return Err(MigrationRegistryError::BeyondTarget);
            }
            if by_source.insert(versions.from, step).is_some() {
                return Err(MigrationRegistryError::DuplicateSource);
            }
        }
        Ok(Self {
            target,
            steps: by_source,
        })
    }
    /// Selected binary/fixture target; never substitutes for a store read.
    pub fn target(&self) -> SafeUnsigned {
        self.target
    }
    fn chain(&self, initial: SafeUnsigned) -> Result<Vec<&MigrationStep<V>>, MigrationError> {
        if initial > self.target {
            return Err(MigrationError::NewerStorageVersion {
                stored: initial,
                target: self.target,
            });
        }
        let mut version = initial;
        let mut chain = Vec::new();
        while version < self.target {
            let step = self
                .steps
                .get(&version)
                .ok_or(MigrationError::MissingStep {
                    from: version,
                    target: self.target,
                })?;
            chain.push(step);
            version = step.versions.to;
        }
        Ok(chain)
    }
    /// Run the complete known chain using actual leased state. Missing chains and
    /// newer stores return before any converter/publication. Each failed step
    /// leaves the preceding committed version as the next reopen's resume point.
    pub fn migrate<'a, S: MigrationStore<Value = V> + ?Sized>(
        &'a self,
        store: &'a S,
    ) -> BoxFuture<'a, Result<MigrationOutcome, MigrationError>> {
        Box::pin(async move {
            let mut lease = store.acquire().await.map_err(MigrationError::Storage)?;
            let mut snapshot = lease.read().await.map_err(MigrationError::Storage)?;
            let initial_version = snapshot.version;
            let chain = self.chain(initial_version)?;
            validate_snapshot(&mut snapshot)?;
            let mut completed_steps = Vec::new();
            for step in chain {
                if snapshot.version != step.versions.from {
                    return Err(MigrationError::StoreObservation);
                }
                let mut converted = Vec::with_capacity(snapshot.registers.len());
                for register in &snapshot.registers {
                    let value = step.converter.convert(register).map_err(|source| {
                        MigrationError::Conversion {
                            from: step.versions.from,
                            source,
                        }
                    })?;
                    converted.push(MigrationRegister {
                        key: register.key.clone(),
                        seq: register.seq,
                        value,
                    });
                }
                lease
                    .publish(MigrationPublication {
                        versions: step.versions,
                        registers: converted,
                    })
                    .await
                    .map_err(MigrationError::Storage)?;
                snapshot = lease.read().await.map_err(MigrationError::Storage)?;
                if snapshot.version != step.versions.to {
                    return Err(MigrationError::StoreObservation);
                }
                validate_snapshot(&mut snapshot)?;
                completed_steps.push(step.versions);
            }
            Ok(MigrationOutcome {
                initial_version,
                current_version: snapshot.version,
                completed_steps,
            })
        })
    }
}
fn validate_snapshot<V>(snapshot: &mut MigrationSnapshot<V>) -> Result<(), MigrationError> {
    let mut keys = BTreeSet::new();
    let mut seqs = BTreeSet::new();
    for register in &snapshot.registers {
        if !keys.insert(register.key.clone()) || !seqs.insert(register.seq) {
            return Err(MigrationError::InvalidSnapshot);
        }
    }
    snapshot.registers.sort_by(|a, b| a.key.cmp(&b.key));
    Ok(())
}
