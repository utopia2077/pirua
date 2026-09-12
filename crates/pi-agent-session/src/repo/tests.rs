//! Internal SYNTHETIC initialization and failure hooks for the real repository path.
use super::*;

type BeforeSwap = Arc<dyn Fn(SafeUnsigned) -> Result<(), StorageError> + Send + Sync>;
type AfterSwap = Arc<dyn Fn(SafeUnsigned) + Send + Sync>;
type RecordHook = Arc<dyn Fn(&SessionId) -> Result<(), SessionError> + Send + Sync>;
#[derive(Default)]
pub(super) struct Hooks {
    pub before_swap_hook: Mutex<Option<BeforeSwap>>,
    pub after_swap_hook: Mutex<Option<AfterSwap>>,
    pub before_expose_hook: Mutex<Option<RecordHook>>,
    pub after_fork_hook: Mutex<Option<RecordHook>>,
}
impl Hooks {
    pub fn before_swap(&self, version: SafeUnsigned) -> Result<(), StorageError> {
        let callback = self.before_swap_hook.lock().unwrap().clone();
        if let Some(callback) = callback {
            callback(version)?;
        }
        Ok(())
    }
    pub fn after_swap(&self, version: SafeUnsigned) {
        let callback = self.after_swap_hook.lock().unwrap().clone();
        if let Some(callback) = callback {
            callback(version);
        }
    }
    pub fn before_expose(&self, id: &SessionId) -> Result<(), SessionError> {
        let callback = self.before_expose_hook.lock().unwrap().clone();
        if let Some(callback) = callback {
            callback(id)?;
        }
        Ok(())
    }
    pub fn after_fork_snapshot(&self, id: &SessionId) -> Result<(), SessionError> {
        let callback = self.after_fork_hook.lock().unwrap().clone();
        if let Some(callback) = callback {
            callback(id)?;
        }
        Ok(())
    }
}

use crate::migration::*;
use crate::storage::values::{register_key, register_seq};
use std::sync::atomic::{AtomicU64, Ordering};
fn n(value: u64) -> SafeUnsigned {
    SafeUnsigned::new(value).unwrap()
}
fn sid(value: &str) -> SessionId {
    SessionId::new(value).unwrap()
}
fn eid(value: &str) -> EntryId {
    EntryId::new(value).unwrap()
}
fn ready<T>(future: impl std::future::Future<Output = T>) -> T {
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    match std::pin::pin!(future).as_mut().poll(&mut cx) {
        std::task::Poll::Ready(value) => value,
        _ => panic!("Memory repo operation suspended"),
    }
}
struct Clock;
impl StorageClock for Clock {
    fn now(&self) -> Result<SafeUnsigned, StorageError> {
        Ok(n(10))
    }
}
#[derive(Default)]
struct Ids(AtomicU64);
impl IdGenerator for Ids {
    fn next(&self, _: Option<SafeUnsigned>) -> String {
        format!(
            "00000000-0001-7000-8000-{:012x}",
            self.0.fetch_add(1, Ordering::SeqCst)
        )
    }
}
fn repo(registry: MigrationRegistry<Register>, policy: ProviderHeaderPolicy) -> MemorySessionRepo {
    let mut repo = MemorySessionRepo::new(
        SessionCodecOptions::default(),
        policy,
        Arc::new(Ids::default()),
        Arc::new(Clock),
        Arc::new(pi_telemetry::NoopTelemetryContext),
    )
    .unwrap();
    Arc::get_mut(&mut repo.inner).unwrap().migrations = Arc::new(registry);
    repo
}
fn options(id: &str) -> SessionCreateOptions {
    SessionCreateOptions {
        id: Some(sid(id)),
        ..Default::default()
    }
}
fn metadata(id: &str) -> SessionMetadata {
    SessionMetadata {
        id: sid(id),
        created_at: n(999),
        storage_version: n(999),
        store_generation: n(999),
        cwd: Some("forged".into()),
        parent_session_id: None,
        legacy_parent_session_path: None,
    }
}
fn closed_fixture() -> StorageSnapshot {
    let source = repo(
        MigrationRegistry::current(),
        ProviderHeaderPolicy::default(),
    );
    let session = ready(source.create(options("fixture"))).unwrap();
    let entry = Write::Entry(NewEntry::Custom(NewCustomEntry {
        id: eid("e"),
        parent_id: None,
        entry_type: CustomEntryType::Custom,
        custom_type: CustomType::new("SYNTHETIC").unwrap(),
        data: Presence::Missing,
    }));
    ready(session.commit(Transaction {
        writes: vec![entry],
    }))
    .unwrap();
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../conformance/schema/agent-state/durable-v1.json"
    ))
    .unwrap();
    for row in fixture["registers"].as_array().unwrap() {
        let mut row = row.clone();
        row.as_object_mut().unwrap().remove("seq");
        if row["namespace"] == "lane.leaf" {
            row["value"] = serde_json::Value::Null;
        }
        if row["namespace"] == "fact.label" {
            row["key"] = serde_json::json!("e");
        }
        if row["namespace"] == "pending.entry" {
            row["value"] = serde_json::json!({"type":"custom","customType":"SYNTHETIC"});
        }
        let set = serde_json::from_value(row).unwrap();
        ready(session.commit(Transaction {
            writes: vec![Write::SetRegister(Box::new(set))],
        }))
        .unwrap();
    }
    let mut snapshot = ready(session.inner.storage.snapshot()).unwrap();
    snapshot.store_generation = n(7);
    ready(session.close()).unwrap();
    snapshot
}
fn initialize(repo: &MemorySessionRepo, id: &str, snapshot: StorageSnapshot) {
    let backing = Arc::new(MemoryBacking::from_snapshot(snapshot).unwrap());
    let mut catalog = repo.inner.lock().unwrap();
    assert!(!catalog.records.contains_key(&sid(id)));
    let token = catalog.ticket().unwrap();
    catalog.records.insert(
        sid(id),
        Record {
            incarnation: token,
            claim: None,
            identity: Some(Identity {
                id: sid(id),
                created_at: n(8),
                cwd: Some("actual".into()),
                parent: None,
            }),
            backing: Some(backing),
            active: None,
        },
    );
}
fn actual(repo: &MemorySessionRepo, id: &str) -> StorageSnapshot {
    // Read-only observation of the actual backing, also usable under outer claim.
    let backing = {
        repo.inner.lock().unwrap().records[&sid(id)]
            .backing
            .clone()
            .unwrap()
    };
    let raw = backing.open(repo.inner.clock.clone()).unwrap();
    ready(raw.snapshot()).unwrap()
}
struct Mapping {
    target: u64,
    fail: bool,
    seen: Arc<Mutex<Vec<(u64, SafeUnsigned)>>>,
}
impl RegisterMigration<Register> for Mapping {
    fn convert(
        &self,
        row: &MigrationRegister<Register>,
    ) -> Result<Register, MigrationConversionError> {
        self.seen.lock().unwrap().push((self.target, row.seq));
        if self.fail && matches!(row.value, Register::FactCustom { .. }) {
            return Err(MigrationConversionError::new());
        }
        let mut value = row.value.clone();
        if let Register::FactCustom { value, .. } = &mut value {
            *value = JsonValue::String(format!("SYNTHETIC-{}", self.target));
        }
        Ok(value)
    }
}
fn registry(
    fail_second: bool,
    seen: &Arc<Mutex<Vec<(u64, SafeUnsigned)>>>,
) -> MigrationRegistry<Register> {
    MigrationRegistry::new(
        n(3),
        vec![
            MigrationStep {
                versions: MigrationVersions {
                    from: n(1),
                    to: n(2),
                },
                converter: Arc::new(Mapping {
                    target: 2,
                    fail: false,
                    seen: seen.clone(),
                }),
            },
            MigrationStep {
                versions: MigrationVersions {
                    from: n(2),
                    to: n(3),
                },
                converter: Arc::new(Mapping {
                    target: 3,
                    fail: fail_second,
                    seen: seen.clone(),
                }),
            },
        ],
    )
    .unwrap()
}
fn assert_preserved(before: &StorageSnapshot, after: &StorageSnapshot, version: u64) {
    assert_eq!(after.storage_version, n(version));
    assert_eq!(after.entries, before.entries);
    assert_eq!(after.usage, before.usage);
    assert_eq!(after.next_seq, before.next_seq);
    assert_eq!(after.stats, before.stats);
    assert_eq!(after.store_generation, before.store_generation);
    assert_eq!(after.registers.len(), 12);
    for row in &after.registers {
        let old = before
            .registers
            .iter()
            .find(|old| register_key(old) == register_key(row))
            .unwrap();
        assert_eq!(register_seq(row), register_seq(old));
        match row {
            Register::FactCustom { value, .. } => {
                assert_eq!(*value, JsonValue::String(format!("SYNTHETIC-{version}")))
            }
            _ => assert_eq!(row, old),
        }
    }
}
#[test]
fn actual_memory_migration_publishes_every_namespace_and_retains_outer_claim_until_exposure() {
    let before = closed_fixture();
    let seen = Arc::new(Mutex::new(vec![]));
    let repo = repo(registry(false, &seen), ProviderHeaderPolicy::default());
    initialize(&repo, "SYNTHETIC", before.clone());
    let observed = Arc::new(Mutex::new(vec![]));
    let weak = Arc::downgrade(&repo.inner);
    let log = observed.clone();
    *repo.inner.hooks.after_swap_hook.lock().unwrap() = Some(Arc::new(move |version| {
        let repo = MemorySessionRepo {
            inner: weak.upgrade().unwrap(),
        };
        let snapshot = actual(&repo, "SYNTHETIC");
        assert_eq!(snapshot.storage_version, version);
        log.lock().unwrap().push(snapshot);
    }));
    let weak = Arc::downgrade(&repo.inner);
    let checked = std::sync::atomic::AtomicBool::new(false);
    *repo.inner.hooks.before_expose_hook.lock().unwrap() = Some(Arc::new(move |id| {
        if checked.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        let repo = MemorySessionRepo {
            inner: weak.upgrade().unwrap(),
        };
        assert!(matches!(
            ready(repo.open(metadata(id.as_str()))),
            Err(SessionError::Storage(StorageError::Busy(_)))
        ));
        assert!(matches!(
            ready(repo.delete(metadata(id.as_str()))),
            Err(SessionError::Storage(StorageError::Busy(_)))
        ));
        assert!(repo.inner.lock().unwrap().records[id].active.is_none());
        Ok(())
    }));
    let session = ready(repo.open(metadata("SYNTHETIC"))).unwrap();
    assert_eq!(session.metadata().storage_version, n(3));
    assert_eq!(session.metadata().store_generation, n(7));
    assert_eq!(session.metadata().cwd.as_deref(), Some("actual"));
    assert_eq!(session.metadata().created_at, n(8));
    let observed = observed.lock().unwrap();
    assert_eq!(observed.len(), 2);
    assert_preserved(&before, &observed[0], 2);
    assert_preserved(&before, &observed[1], 3);
    assert_eq!(seen.lock().unwrap().len(), 24);
    *repo.inner.hooks.before_expose_hook.lock().unwrap() = None;
    seen.lock().unwrap().clear();
    assert!(Arc::ptr_eq(
        &session,
        &ready(repo.open(metadata("SYNTHETIC"))).unwrap()
    ));
    assert!(seen.lock().unwrap().is_empty());
}
#[test]
fn actual_memory_failed_step_resumes_from_published_authority_without_partial_values() {
    for failure_target in [2, 3] {
        let before = closed_fixture();
        let seen = Arc::new(Mutex::new(vec![]));
        let repo = repo(registry(false, &seen), ProviderHeaderPolicy::default());
        initialize(&repo, "SYNTHETIC", before.clone());
        *repo.inner.hooks.before_swap_hook.lock().unwrap() = Some(Arc::new(move |target| {
            if target == n(failure_target) {
                Err(StorageError::Io(context("SYNTHETIC publication failure")))
            } else {
                Ok(())
            }
        }));
        assert!(ready(repo.open(metadata("SYNTHETIC"))).is_err());
        let after = actual(&repo, "SYNTHETIC");
        if failure_target == 2 {
            assert_eq!(after, before);
        } else {
            assert_preserved(&before, &after, 2);
        }
        assert!(
            repo.inner.lock().unwrap().records[&sid("SYNTHETIC")]
                .active
                .is_none()
        );
        *repo.inner.hooks.before_swap_hook.lock().unwrap() = None;
        seen.lock().unwrap().clear();
        let session = ready(repo.open(metadata("SYNTHETIC"))).unwrap();
        assert_eq!(session.metadata().storage_version, n(3));
        assert_eq!(
            seen.lock().unwrap().len(),
            if failure_target == 2 { 24 } else { 12 }
        );
    }
}
#[test]
fn actual_memory_mapping_failure_keeps_step_one_and_closed_old_views_cannot_write() {
    let mut repo = repo(
        MigrationRegistry::current(),
        ProviderHeaderPolicy::default(),
    );
    initialize(&repo, "SYNTHETIC", closed_fixture());
    let old = ready(repo.open(metadata("SYNTHETIC"))).unwrap();
    let view = old.view(LaneId::new("main").unwrap());
    ready(old.close()).unwrap();
    let seen = Arc::new(Mutex::new(vec![]));
    Arc::get_mut(&mut repo.inner).unwrap().migrations = Arc::new(registry(true, &seen));
    assert!(ready(repo.open(metadata("SYNTHETIC"))).is_err());
    assert_eq!(actual(&repo, "SYNTHETIC").storage_version, n(2));
    assert!(ready(view.set_name(Some("stale".into()))).is_err());
    assert!(ready(old.get_stats()).is_err());
    Arc::get_mut(&mut repo.inner).unwrap().migrations = Arc::new(registry(false, &seen));
    seen.lock().unwrap().clear();
    let reopened = ready(repo.open(metadata("SYNTHETIC"))).unwrap();
    assert_eq!(reopened.metadata().storage_version, n(3));
    assert_eq!(seen.lock().unwrap().len(), 12);
}
#[test]
fn incompatible_active_root_or_view_blocks_real_migration() {
    let mut repo = repo(
        MigrationRegistry::current(),
        ProviderHeaderPolicy::default(),
    );
    initialize(&repo, "SYNTHETIC", closed_fixture());
    let old = ready(repo.open(metadata("SYNTHETIC"))).unwrap();
    let view = old.view(LaneId::new("main").unwrap());
    drop(old);
    let seen = Arc::new(Mutex::new(vec![]));
    Arc::get_mut(&mut repo.inner).unwrap().migrations = Arc::new(registry(false, &seen));
    let before = actual(&repo, "SYNTHETIC");
    assert!(matches!(
        ready(repo.open(metadata("SYNTHETIC"))),
        Err(SessionError::Storage(StorageError::Busy(_)))
    ));
    assert_eq!(actual(&repo, "SYNTHETIC"), before);
    assert!(seen.lock().unwrap().is_empty());
    ready(view.set_name(Some("still active".into()))).unwrap();
}
#[test]
fn authoritative_version_gate_precedes_current_codec_and_codec_failure_exposes_no_session() {
    for version in [0, 1, 2] {
        let repo = repo(
            MigrationRegistry::current(),
            ProviderHeaderPolicy::default(),
        );
        let mut snapshot = closed_fixture();
        let unknown: AgentMessage = serde_json::from_value(
            serde_json::json!({"role":"unknown-private-role","body":"payload-sentinel"}),
        )
        .unwrap();
        let seq = snapshot.next_seq.unwrap();
        snapshot.next_seq = Some(n(seq.get() + 1));
        snapshot.entries.push(Entry::Message(MessageEntry {
            id: eid("invalid-message"),
            parent_id: None,
            seq,
            timestamp: n(1),
            entry_type: MessageEntryType::Message,
            message: unknown,
            terminate: None,
        }));
        snapshot.stats.message_count = n(snapshot.stats.message_count.get() + 1);
        snapshot.storage_version = n(version);
        initialize(&repo, "versioned", snapshot.clone());
        let result = ready(repo.open(metadata("versioned")));
        match version {
            0 => assert!(matches!(result, Err(SessionError::Migration(_)))),
            1 => assert!(matches!(result, Err(SessionError::UnknownCustomRole(_)))),
            2 => assert!(matches!(result, Err(SessionError::NewerStorageVersion(_)))),
            _ => unreachable!(),
        }
        assert_eq!(actual(&repo, "versioned"), snapshot);
        assert!(
            repo.inner.lock().unwrap().records[&sid("versioned")]
                .active
                .is_none()
        );
    }
}
#[test]
fn final_exposure_failure_keeps_completed_migration_and_releases_only_outer_claim() {
    let seen = Arc::new(Mutex::new(vec![]));
    let repo = repo(registry(false, &seen), ProviderHeaderPolicy::default());
    initialize(&repo, "SYNTHETIC", closed_fixture());
    *repo.inner.hooks.before_expose_hook.lock().unwrap() = Some(Arc::new(|_| {
        Err(SessionError::Schema(context(
            "SYNTHETIC final admission failure",
        )))
    }));
    assert!(ready(repo.open(metadata("SYNTHETIC"))).is_err());
    assert_eq!(actual(&repo, "SYNTHETIC").storage_version, n(3));
    assert!(
        repo.inner.lock().unwrap().records[&sid("SYNTHETIC")]
            .active
            .is_none()
    );
    *repo.inner.hooks.before_expose_hook.lock().unwrap() = None;
    seen.lock().unwrap().clear();
    ready(repo.open(metadata("SYNTHETIC"))).unwrap();
    assert!(seen.lock().unwrap().is_empty());
}
#[test]
fn migration_rejects_mapper_changes_to_inner_envelope() {
    struct Bad;
    impl RegisterMigration<Register> for Bad {
        fn convert(
            &self,
            row: &MigrationRegister<Register>,
        ) -> Result<Register, MigrationConversionError> {
            let mut value = row.value.clone();
            if let Register::FactCustom { key, .. } = &mut value {
                *key = "changed".into();
            }
            Ok(value)
        }
    }
    let registry = MigrationRegistry::new(
        n(2),
        vec![MigrationStep {
            versions: MigrationVersions {
                from: n(1),
                to: n(2),
            },
            converter: Arc::new(Bad),
        }],
    )
    .unwrap();
    let repo = repo(registry, ProviderHeaderPolicy::default());
    let before = closed_fixture();
    initialize(&repo, "SYNTHETIC", before.clone());
    assert!(ready(repo.open(metadata("SYNTHETIC"))).is_err());
    assert_eq!(actual(&repo, "SYNTHETIC"), before);
}
#[test]
fn actual_migration_retains_classified_headers_and_rejects_changed_codec_policy() {
    let mut snapshot = closed_fixture();
    let states: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../conformance/schema/agent-state/states-v1.json"
    ))
    .unwrap();
    let mut state = states
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["name"] == "run.assistant.effect_pending")
        .unwrap()["value"]
        .clone();
    state["phase"]["generation"]["context"]["streamOptions"]["headers"] =
        serde_json::json!({"X-Public":"public-value"});
    let old = snapshot
        .registers
        .iter_mut()
        .find(|r| matches!(r, Register::OperationState { .. }))
        .unwrap();
    let mut value = serde_json::to_value(&*old).unwrap();
    value["value"] = state;
    let raw: RawRegister = serde_json::from_value(value).unwrap();
    *old = raw
        .into_durable(&ProviderHeaderPolicy::from_allowlist([
            "x-public".to_owned()
        ]))
        .unwrap();
    let seen = Arc::new(Mutex::new(vec![]));
    let allowed = repo(
        registry(false, &seen),
        ProviderHeaderPolicy::from_allowlist(["x-public".to_owned()]),
    );
    initialize(&allowed, "SYNTHETIC", snapshot.clone());
    ready(allowed.open(metadata("SYNTHETIC"))).unwrap();
    assert_preserved(&snapshot, &actual(&allowed, "SYNTHETIC"), 3);
    let denied = repo(
        MigrationRegistry::current(),
        ProviderHeaderPolicy::default(),
    );
    initialize(&denied, "denied", snapshot.clone());
    assert!(matches!(
        ready(denied.open(metadata("denied"))),
        Err(SessionError::Schema(_))
    ));
    assert_eq!(actual(&denied, "denied"), snapshot);
}
#[test]
fn fork_exhaustion_uses_copied_entries_not_unretained_source_allocator() {
    let repo = repo(
        MigrationRegistry::current(),
        ProviderHeaderPolicy::default(),
    );
    let mut snapshot = closed_fixture();
    snapshot.next_seq = None;
    snapshot.entries.push(Entry::Custom(CustomEntry {
        id: eid("maximum"),
        parent_id: None,
        seq: SafeUnsigned::MAX,
        timestamp: n(4),
        entry_type: CustomEntryType::Custom,
        custom_type: CustomType::new("SYNTHETIC").unwrap(),
        data: Presence::Missing,
    }));
    initialize(&repo, "source", snapshot.clone());
    let request = |id: &str, entry: &str| SessionForkRequest {
        fork: ForkOptions::Branch(BranchForkOptions {
            entry_id: Some(eid(entry)),
            ..Default::default()
        }),
        create: options(id),
    };
    assert!(ready(repo.fork(metadata("source"), request("overflow", "maximum"))).is_err());
    assert!(
        !repo
            .inner
            .lock()
            .unwrap()
            .records
            .contains_key(&sid("overflow"))
    );
    let fork = ready(repo.fork(metadata("source"), request("retained", "e"))).unwrap();
    assert_eq!(ready(fork.get_leaf_id()).unwrap(), Some(eid("e")));
    assert_eq!(fork.metadata().store_generation, n(0));
    assert_eq!(actual(&repo, "source"), snapshot);
}
#[test]
fn fork_snapshot_remains_one_whole_prefix_across_a_later_source_commit() {
    let repo = repo(
        MigrationRegistry::current(),
        ProviderHeaderPolicy::default(),
    );
    let source = ready(repo.create(options("source"))).unwrap();
    ready(source.commit(Transaction {
        writes: vec![
            Write::Entry(NewEntry::Custom(NewCustomEntry {
                id: eid("before"),
                parent_id: None,
                entry_type: CustomEntryType::Custom,
                custom_type: CustomType::new("note").unwrap(),
                data: Presence::Missing,
            })),
            Write::SetRegister(Box::new(RegisterSet::LaneLeaf {
                key: "main".into(),
                value: Some(eid("before")),
            })),
            Write::SetRegister(Box::new(RegisterSet::FactCustom {
                key: "stage".into(),
                value: JsonValue::String("before".into()),
            })),
        ],
    }))
    .unwrap();
    let callback_source = source.clone();
    *repo.inner.hooks.after_fork_hook.lock().unwrap() = Some(Arc::new(move |_| {
        ready(callback_source.commit(Transaction {
            writes: vec![
                Write::Entry(NewEntry::Custom(NewCustomEntry {
                    id: eid("after"),
                    parent_id: Some(eid("before")),
                    entry_type: CustomEntryType::Custom,
                    custom_type: CustomType::new("note").unwrap(),
                    data: Presence::Missing,
                })),
                Write::SetRegister(Box::new(RegisterSet::LaneLeaf {
                    key: "main".into(),
                    value: Some(eid("after")),
                })),
                Write::SetRegister(Box::new(RegisterSet::FactCustom {
                    key: "stage".into(),
                    value: JsonValue::String("after".into()),
                })),
            ],
        }))
        .unwrap();
        Ok(())
    }));
    let fork = ready(repo.fork(
        source.metadata(),
        SessionForkRequest {
            fork: ForkOptions::Branch(BranchForkOptions::default()),
            create: options("fork"),
        },
    ))
    .unwrap();
    assert_eq!(ready(fork.get_leaf_id()).unwrap(), Some(eid("before")));
    assert_eq!(
        ready(fork.get_custom_fact("stage")).unwrap(),
        Some(JsonValue::String("before".into()))
    );
    assert_eq!(ready(source.get_leaf_id()).unwrap(), Some(eid("after")));
    assert_eq!(
        ready(source.get_custom_fact("stage")).unwrap(),
        Some(JsonValue::String("after".into()))
    );
    *repo.inner.hooks.after_fork_hook.lock().unwrap() = None;
    let later = ready(repo.fork(
        source.metadata(),
        SessionForkRequest {
            fork: ForkOptions::Branch(BranchForkOptions::default()),
            create: options("later"),
        },
    ))
    .unwrap();
    assert_eq!(ready(later.get_leaf_id()).unwrap(), Some(eid("after")));
    assert_eq!(
        ready(later.get_custom_fact("stage")).unwrap(),
        Some(JsonValue::String("after".into()))
    );
}
#[test]
fn stale_claim_cannot_publish_or_remove_a_recreated_record() {
    let repo = repo(
        MigrationRegistry::current(),
        ProviderHeaderPolicy::default(),
    );
    let claim = repo.reserve(sid("same")).unwrap();
    let removed = { repo.inner.lock().unwrap().records.remove(&sid("same")) };
    drop(removed);
    let newer = ready(repo.create(options("same"))).unwrap();
    let snapshot = repo.empty();
    let error = ready(repo.destination(
        claim,
        options("same"),
        snapshot,
        forks::lane_writes("main", None, None),
    ))
    .err()
    .expect("stale claim rejects");
    assert!(matches!(
        error,
        SessionError::Storage(StorageError::Busy(_))
    ));
    assert!(Arc::ptr_eq(
        &newer,
        &ready(repo.open(metadata("same"))).unwrap()
    ));
}

#[test]
fn fork_acknowledged_snapshot_survives_source_delete_and_recreate() {
    let repo = repo(
        MigrationRegistry::current(),
        ProviderHeaderPolicy::default(),
    );
    let source = ready(repo.create(options("source"))).unwrap();
    let entry =
        ready(source.append_custom_entry(CustomType::new("note").unwrap(), Presence::Missing))
            .unwrap();
    let weak = Arc::downgrade(&repo.inner);
    *repo.inner.hooks.after_fork_hook.lock().unwrap() = Some(Arc::new(move |id| {
        let repo = MemorySessionRepo {
            inner: weak.upgrade().unwrap(),
        };
        ready(repo.delete(metadata(id.as_str()))).unwrap();
        ready(repo.create(options(id.as_str()))).unwrap();
        Ok(())
    }));
    let fork = ready(repo.fork(
        source.metadata(),
        SessionForkRequest {
            fork: ForkOptions::Branch(BranchForkOptions::default()),
            create: options("fork"),
        },
    ))
    .unwrap();
    assert_eq!(ready(fork.get_leaf_id()).unwrap(), Some(entry));
    assert!(ready(source.get_stats()).is_err());
    assert_eq!(
        ready(ready(repo.open(metadata("source"))).unwrap().get_leaf_id()).unwrap(),
        None
    );
}
