//! SYNTHETIC versions/values exercising the actual production migration engine.
use pi_agent_schema::session_values::*;
use pi_agent_schema::*;
use pi_agent_session::migration::*;
use pi_agent_session::storage::RegisterKey;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

fn n(v: u64) -> SafeUnsigned {
    SafeUnsigned::new(v).unwrap()
}
fn ready<T>(future: impl std::future::Future<Output = T>) -> T {
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    match std::pin::pin!(future).as_mut().poll(&mut cx) {
        std::task::Poll::Ready(v) => v,
        _ => panic!("synthetic adapter suspended"),
    }
}
fn error() -> StorageError {
    StorageError::Io(ErrorContext::new("SYNTHETIC publication failure").unwrap())
}
fn fixture() -> serde_json::Value {
    serde_json::from_str(include_str!(
        "../../../conformance/session/migration-contract.json"
    ))
    .unwrap()
}
fn payload(version: u64) -> JsonValue {
    serde_json::from_value(serde_json::json!({"SYNTHETIC":true,"revision":version})).unwrap()
}

#[derive(Clone)]
struct Authority {
    version: SafeUnsigned,
    registers: BTreeMap<RegisterKey, (SafeUnsigned, JsonValue)>,
    entries: Vec<Entry>,
    usage: Vec<UsageRow>,
    next_seq: SafeUnsigned,
    store_generation: SafeUnsigned,
}
impl Authority {
    fn bytes(&self) -> Vec<u8> {
        let rows:Vec<_>=self.registers.iter().map(|(key,(seq,value))|serde_json::json!({"namespace":key.namespace,"key":key.key,"seq":seq,"value":value})).collect();
        serde_json::to_vec(&serde_json::json!({"version":self.version,"registers":rows,"entries":self.entries,"usage":self.usage,"nextSeq":self.next_seq,"storeGeneration":self.store_generation})).unwrap()
    }
    fn read(&self) -> MigrationSnapshot<JsonValue> {
        // Reverse incoming order so visits prove the engine's stable ordering.
        MigrationSnapshot {
            version: self.version,
            registers: self
                .registers
                .iter()
                .rev()
                .map(|(key, (seq, value))| MigrationRegister {
                    key: key.clone(),
                    seq: *seq,
                    value: value.clone(),
                })
                .collect(),
        }
    }
}
struct Inner {
    authority: Mutex<Authority>,
    leased: AtomicBool,
    failure_target: AtomicU64,
    observations: Mutex<Vec<Authority>>,
    malformed: AtomicU64,
}
#[derive(Clone)]
struct SyntheticStore(Arc<Inner>);
impl SyntheticStore {
    fn new(version: u64) -> Self {
        let f = fixture();
        let mut registers = BTreeMap::new();
        for row in f["registers"].as_array().unwrap() {
            registers.insert(
                RegisterKey {
                    namespace: serde_json::from_value(row["namespace"].clone()).unwrap(),
                    key: row["key"].as_str().unwrap().to_owned(),
                },
                (n(row["seq"].as_u64().unwrap()), payload(version)),
            );
        }
        let entry:Entry=serde_json::from_value(serde_json::json!({"id":"SYNTHETIC-entry","parentId":null,"seq":0,"timestamp":4,"type":"custom","customType":"SYNTHETIC","data":null})).unwrap();
        let usage = UsageRow {
            id: UsageId::new("SYNTHETIC-usage").unwrap(),
            seq: n(1),
            usage: Usage::zero(),
            entry_id: Some(EntryId::new("SYNTHETIC-entry").unwrap()),
            adjustment: true,
            details: Presence::Missing,
        };
        Self(Arc::new(Inner {
            authority: Mutex::new(Authority {
                version: n(version),
                registers,
                entries: vec![entry],
                usage: vec![usage],
                next_seq: n(30),
                store_generation: n(7),
            }),
            leased: AtomicBool::new(false),
            failure_target: AtomicU64::new(0),
            observations: Mutex::new(vec![]),
            malformed: AtomicU64::new(0),
        }))
    }
    fn bytes(&self) -> Vec<u8> {
        self.0.authority.lock().unwrap().bytes()
    }
}
struct SyntheticLease(Arc<Inner>);
impl Drop for SyntheticLease {
    fn drop(&mut self) {
        assert!(self.0.leased.swap(false, Ordering::SeqCst));
    }
}
impl MigrationStore for SyntheticStore {
    type Value = JsonValue;
    type Lease = SyntheticLease;
    fn acquire(&self) -> BoxFuture<'_, Result<Self::Lease, StorageError>> {
        Box::pin(async move {
            if self
                .0
                .leased
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_err()
            {
                return Err(StorageError::Busy(
                    ErrorContext::new("SYNTHETIC writer held").unwrap(),
                ));
            }
            Ok(SyntheticLease(self.0.clone()))
        })
    }
}
impl MigrationLease for SyntheticLease {
    type Value = JsonValue;
    fn read(&mut self) -> BoxFuture<'_, Result<MigrationSnapshot<JsonValue>, StorageError>> {
        Box::pin(async move {
            let mut view = self.0.authority.lock().unwrap().read();
            match self.0.malformed.load(Ordering::SeqCst) {
                1 => {
                    let first = &view.registers[0];
                    view.registers.push(MigrationRegister {
                        key: first.key.clone(),
                        seq: n(29),
                        value: first.value.clone(),
                    });
                }
                2 => {
                    view.registers[1].seq = view.registers[0].seq;
                }
                _ => {}
            }
            Ok(view)
        })
    }
    fn publish(
        &mut self,
        publication: MigrationPublication<JsonValue>,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        Box::pin(async move {
            let (versions, rows) = publication.into_parts();
            let mut authority = self.0.authority.lock().unwrap();
            if authority.version != versions.from {
                return Err(StorageError::Conflict(
                    ErrorContext::new("SYNTHETIC version mismatch").unwrap(),
                ));
            }
            let actual: BTreeMap<_, _> =
                rows.iter().map(|row| (row.key.clone(), row.seq)).collect();
            let expected: BTreeMap<_, _> = authority
                .registers
                .iter()
                .map(|(key, (seq, _))| (key.clone(), *seq))
                .collect();
            if actual != expected || rows.len() != authority.registers.len() {
                return Err(StorageError::Corruption(
                    ErrorContext::new("SYNTHETIC register envelope changed").unwrap(),
                ));
            }
            let mut stage = authority.clone();
            for row in rows {
                stage.registers.insert(row.key, (row.seq, row.value));
            }
            stage.version = versions.to;
            if self.0.failure_target.load(Ordering::SeqCst) == versions.to.get() {
                return Err(error());
            }
            // One publication for values and version; failure above touches neither.
            *authority = stage.clone();
            self.0.observations.lock().unwrap().push(stage);
            Ok(())
        })
    }
}
struct Mapping {
    target: u64,
    fail_seq: Option<u64>,
    visits: Arc<Mutex<Vec<(u64, u64)>>>,
}
impl RegisterMigration<JsonValue> for Mapping {
    fn convert(
        &self,
        row: &MigrationRegister<JsonValue>,
    ) -> Result<JsonValue, MigrationConversionError> {
        self.visits
            .lock()
            .unwrap()
            .push((self.target, row.seq.get()));
        if self.fail_seq == Some(row.seq.get()) {
            return Err(MigrationConversionError::new());
        }
        Ok(payload(self.target)) // Idempotent assignment, not revision increment.
    }
}
fn step(
    from: u64,
    to: u64,
    fail_seq: Option<u64>,
    visits: &Arc<Mutex<Vec<(u64, u64)>>>,
) -> MigrationStep<JsonValue> {
    MigrationStep {
        versions: MigrationVersions {
            from: n(from),
            to: n(to),
        },
        converter: Arc::new(Mapping {
            target: to,
            fail_seq,
            visits: visits.clone(),
        }),
    }
}
fn registry(
    failure: Option<(u64, u64)>,
    visits: &Arc<Mutex<Vec<(u64, u64)>>>,
) -> MigrationRegistry<JsonValue> {
    let fail = |target| {
        failure
            .filter(|(version, _)| *version == target)
            .map(|(_, seq)| seq)
    };
    MigrationRegistry::new(
        n(3),
        vec![step(2, 3, fail(3), visits), step(1, 2, fail(2), visits)],
    )
    .unwrap()
}

#[test]
fn synthetic_version_gate_uses_authoritative_store_and_never_calls_missing_mappings() {
    let registry = MigrationRegistry::<JsonValue>::current();
    assert_eq!(registry.target(), n(1));
    let equal = SyntheticStore::new(1);
    let before = equal.bytes();
    let outcome = ready(registry.migrate(&equal)).unwrap();
    assert_eq!(
        outcome,
        MigrationOutcome {
            initial_version: n(1),
            current_version: n(1),
            completed_steps: vec![]
        }
    );
    assert_eq!(equal.bytes(), before);
    let newer = SyntheticStore::new(3);
    let forged_caller_metadata_version = 1;
    assert_eq!(forged_caller_metadata_version, registry.target().get());
    let before = newer.bytes();
    assert!(
        matches!(ready(registry.migrate(&newer)),Err(MigrationError::NewerStorageVersion {stored,target}) if stored==n(3)&&target==n(1))
    );
    assert_eq!(newer.bytes(), before);
    let unknown = SyntheticStore::new(0);
    let before = unknown.bytes();
    assert!(
        matches!(ready(registry.migrate(&unknown)),Err(MigrationError::MissingStep {from,..}) if from==n(0))
    );
    assert_eq!(unknown.bytes(), before);
    assert!(equal.0.observations.lock().unwrap().is_empty());
    assert!(newer.0.observations.lock().unwrap().is_empty());
    assert!(unknown.0.observations.lock().unwrap().is_empty());
    assert!(!newer.0.leased.load(Ordering::SeqCst));
}

#[test]
fn synthetic_invalid_registry_and_complete_chain_are_rejected_before_conversion() {
    let visits = Arc::new(Mutex::new(vec![]));
    assert!(matches!(
        MigrationRegistry::<JsonValue>::new(n(0), vec![]),
        Err(MigrationRegistryError::InvalidTarget)
    ));
    for (from, to) in [(0, 1), (2, 1), (1, 1), (1, 3)] {
        assert!(matches!(
            MigrationRegistry::new(n(3), vec![step(from, to, None, &visits)]),
            Err(MigrationRegistryError::NonconsecutiveStep)
        ));
    }
    assert!(matches!(
        MigrationRegistry::new(n(2), vec![step(2, 3, None, &visits)]),
        Err(MigrationRegistryError::BeyondTarget)
    ));
    assert!(matches!(
        MigrationRegistry::new(
            n(3),
            vec![step(1, 2, None, &visits), step(1, 2, None, &visits)]
        ),
        Err(MigrationRegistryError::DuplicateSource)
    ));
    for steps in [
        vec![step(1, 2, None, &visits)],
        vec![step(2, 3, None, &visits)],
        vec![],
    ] {
        let registry = MigrationRegistry::new(n(3), steps).unwrap();
        let store = SyntheticStore::new(1);
        let before = store.bytes();
        assert!(matches!(
            ready(registry.migrate(&store)),
            Err(MigrationError::MissingStep { .. })
        ));
        assert_eq!(store.bytes(), before);
        assert!(store.0.observations.lock().unwrap().is_empty());
    }
    assert!(visits.lock().unwrap().is_empty());
}

#[test]
fn synthetic_every_register_is_visited_and_step_values_version_publish_together() {
    let visits = Arc::new(Mutex::new(vec![]));
    let store = SyntheticStore::new(1);
    let original = store.0.authority.lock().unwrap().clone();
    let outcome = ready(registry(None, &visits).migrate(&store)).unwrap();
    assert_eq!(
        outcome.completed_steps,
        vec![
            MigrationVersions {
                from: n(1),
                to: n(2)
            },
            MigrationVersions {
                from: n(2),
                to: n(3)
            }
        ]
    );
    let f = fixture();
    assert_eq!(
        f["scope"].as_str().unwrap().split_whitespace().next(),
        Some("SYNTHETIC")
    );
    let order: Vec<u64> =
        serde_json::from_value(f["expected_visit_seqs_per_step"].clone()).unwrap();
    let expected: Vec<_> = [2, 3]
        .into_iter()
        .flat_map(|target| order.iter().map(move |seq| (target, *seq)))
        .collect();
    assert_eq!(*visits.lock().unwrap(), expected);
    let observations = store.0.observations.lock().unwrap();
    let actual: Vec<_> = observations
        .iter()
        .map(|state| state.version.get())
        .collect();
    assert_eq!(
        actual,
        serde_json::from_value::<Vec<u64>>(f["expected_committed_versions"].clone()).unwrap()
    );
    for state in &*observations {
        assert_eq!(state.registers.len(), 12);
        for (key, (seq, value)) in &state.registers {
            assert_eq!(*seq, original.registers[key].0);
            assert_eq!(
                serde_json::to_value(value).unwrap(),
                f["expected_values_by_version"][state.version.get().to_string()]
            );
        }
        assert_eq!(state.entries, original.entries);
        assert_eq!(state.usage, original.usage);
        assert_eq!(state.next_seq, original.next_seq);
        assert_eq!(state.store_generation, original.store_generation);
    }
}

#[test]
fn synthetic_conversion_failure_at_every_position_preserves_whole_current_step() {
    for fail_target in [2, 3] {
        for seq in 10..22 {
            let visits = Arc::new(Mutex::new(vec![]));
            let store = SyntheticStore::new(1);
            let before = store.bytes();
            assert!(
                matches!(ready(registry(Some((fail_target,seq)),&visits).migrate(&store)),Err(MigrationError::Conversion {from,..}) if from==n(fail_target-1))
            );
            let state = store.0.authority.lock().unwrap();
            assert_eq!(state.version, n(fail_target - 1));
            for (_, value) in state.registers.values() {
                assert_eq!(*value, payload(fail_target - 1));
            }
            drop(state);
            if fail_target == 2 {
                assert_eq!(store.bytes(), before);
            }
            assert_eq!(
                store.0.observations.lock().unwrap().len(),
                usize::from(fail_target == 3)
            );
            assert!(!store.0.leased.load(Ordering::SeqCst));
        }
    }
}

#[test]
fn synthetic_commit_failure_resumes_from_recorded_step_and_reopen_is_idempotent() {
    for failure_target in [2, 3] {
        let visits = Arc::new(Mutex::new(vec![]));
        let registry = registry(None, &visits);
        let store = SyntheticStore::new(1);
        let before = store.bytes();
        store
            .0
            .failure_target
            .store(failure_target, Ordering::SeqCst);
        assert!(matches!(
            ready(registry.migrate(&store)),
            Err(MigrationError::Storage(StorageError::Io(_)))
        ));
        assert_eq!(
            store.0.authority.lock().unwrap().version,
            n(failure_target - 1)
        );
        if failure_target == 2 {
            assert_eq!(store.bytes(), before);
        }
        store.0.failure_target.store(0, Ordering::SeqCst);
        visits.lock().unwrap().clear();
        let outcome = ready(registry.migrate(&store)).unwrap();
        assert_eq!(outcome.initial_version, n(failure_target - 1));
        assert_eq!(outcome.current_version, n(3));
        assert!(
            visits
                .lock()
                .unwrap()
                .iter()
                .all(|(target, _)| *target >= failure_target)
        );
        assert_eq!(
            visits.lock().unwrap().len(),
            if failure_target == 2 { 24 } else { 12 }
        );
        let before = store.bytes();
        visits.lock().unwrap().clear();
        let again = ready(registry.migrate(&store)).unwrap();
        assert_eq!(again.completed_steps, vec![]);
        assert!(visits.lock().unwrap().is_empty());
        assert_eq!(store.bytes(), before);
    }
}

#[test]
fn synthetic_mapping_failure_resume_runs_only_remaining_step() {
    let visits = Arc::new(Mutex::new(vec![]));
    let store = SyntheticStore::new(1);
    assert!(ready(registry(Some((3, 15)), &visits).migrate(&store)).is_err());
    visits.lock().unwrap().clear();
    let outcome = ready(registry(None, &visits).migrate(&store)).unwrap();
    assert_eq!(outcome.initial_version, n(2));
    assert_eq!(
        outcome.completed_steps,
        vec![MigrationVersions {
            from: n(2),
            to: n(3)
        }]
    );
    assert_eq!(visits.lock().unwrap().len(), 12);
    assert!(
        visits
            .lock()
            .unwrap()
            .iter()
            .all(|(target, _)| *target == 3)
    );
}

#[test]
fn synthetic_newer_and_malformed_snapshot_call_zero_converters() {
    let visits = Arc::new(Mutex::new(vec![]));
    let registry = registry(None, &visits);
    assert!(matches!(
        ready(registry.migrate(&SyntheticStore::new(4))),
        Err(MigrationError::NewerStorageVersion { .. })
    ));
    for malformed in [1, 2] {
        let store = SyntheticStore::new(1);
        store.0.malformed.store(malformed, Ordering::SeqCst);
        let before = store.bytes();
        assert!(matches!(
            ready(registry.migrate(&store)),
            Err(MigrationError::InvalidSnapshot)
        ));
        assert_eq!(store.bytes(), before);
    }
    assert!(visits.lock().unwrap().is_empty());
}

#[test]
fn synthetic_exclusive_claim_and_unpolled_drop_do_not_publish() {
    let store = SyntheticStore::new(1);
    let visits = Arc::new(Mutex::new(vec![]));
    let registry = registry(None, &visits);
    let before = store.bytes();
    drop(registry.migrate(&store));
    assert!(!store.0.leased.load(Ordering::SeqCst));
    let lease = ready(store.acquire()).unwrap();
    assert!(matches!(
        ready(registry.migrate(&store)),
        Err(MigrationError::Storage(StorageError::Busy(_)))
    ));
    assert_eq!(store.bytes(), before);
    assert!(visits.lock().unwrap().is_empty());
    drop(lease);
    ready(registry.migrate(&store)).unwrap();
    assert!(!store.0.leased.load(Ordering::SeqCst));
}

#[test]
fn migration_diagnostics_redact_keys_values_and_retained_sources() {
    let sentinel = "SYNTHETIC-private-payload";
    let source = MigrationConversionError::with_source(Arc::new(std::io::Error::other(sentinel)));
    let error = MigrationError::Conversion { from: n(1), source };
    assert!(!format!("{error:?} {error}").contains(sentinel));
    assert!(std::error::Error::source(&error).is_some());
    let row = MigrationRegister {
        key: RegisterKey {
            namespace: RegisterNamespace::FactCustom,
            key: sentinel.to_owned(),
        },
        seq: n(0),
        value: sentinel,
    };
    assert!(!format!("{row:?}").contains(sentinel));
    assert!(
        !format!(
            "{:?}",
            MigrationSnapshot {
                version: n(1),
                registers: vec![row]
            }
        )
        .contains(sentinel)
    );
}

#[test]
fn synthetic_empty_register_set_still_publishes_every_version_step() {
    let visits = Arc::new(Mutex::new(vec![]));
    let store = SyntheticStore::new(1);
    store.0.authority.lock().unwrap().registers.clear();
    let original = store.0.authority.lock().unwrap().clone();
    let outcome = ready(registry(None, &visits).migrate(&store)).unwrap();
    assert_eq!(outcome.current_version, n(3));
    assert_eq!(outcome.completed_steps.len(), 2);
    assert!(visits.lock().unwrap().is_empty());
    let observations = store.0.observations.lock().unwrap();
    assert_eq!(
        observations
            .iter()
            .map(|state| state.version.get())
            .collect::<Vec<_>>(),
        vec![2, 3]
    );
    for state in &*observations {
        assert!(state.registers.is_empty());
        assert_eq!(state.entries, original.entries);
        assert_eq!(state.usage, original.usage);
        assert_eq!(state.next_seq, original.next_seq);
    }
}

#[test]
fn synthetic_idempotent_field_mapping_accepts_already_converted_values() {
    let visits = Arc::new(Mutex::new(vec![]));
    let store = SyntheticStore::new(1);
    for (_, value) in store
        .0
        .authority
        .lock()
        .unwrap()
        .registers
        .values_mut()
        .take(3)
    {
        *value = payload(2);
    }
    let registry = MigrationRegistry::new(n(2), vec![step(1, 2, None, &visits)]).unwrap();
    ready(registry.migrate(&store)).unwrap();
    assert_eq!(visits.lock().unwrap().len(), 12);
    for (_, value) in store.0.authority.lock().unwrap().registers.values() {
        assert_eq!(*value, payload(2));
    }
}
