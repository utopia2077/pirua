use super::*;
use crate::storage::*;
use std::{
    fs,
    sync::atomic::{AtomicU64, Ordering},
};
type Callback = Box<dyn FnOnce() -> Result<(), SessionError> + Send>;
#[derive(Default)]
pub(super) struct Hooks {
    create: Mutex<Option<Callback>>,
    published: Mutex<Option<Callback>>,
    expose: Mutex<Option<Callback>>,
    seal: Mutex<Option<Callback>>,
    migration: Mutex<Option<(SafeUnsigned, Callback)>>,
}
fn call(slot: &Mutex<Option<Callback>>) -> Result<(), SessionError> {
    let callback = slot.lock().unwrap().take();
    callback.map_or(Ok(()), |c| c())
}
impl Hooks {
    pub fn before_create(&self) -> Result<(), SessionError> {
        call(&self.create)
    }
    pub fn after_publication(&self) -> Result<(), SessionError> {
        call(&self.published)
    }
    pub fn before_expose(&self) -> Result<(), SessionError> {
        call(&self.expose)
    }
    pub fn after_seal(&self) -> Result<(), SessionError> {
        call(&self.seal)
    }
    pub fn before_migration(&self, to: SafeUnsigned) -> Result<(), SessionError> {
        let callback = {
            let mut slot = self.migration.lock().unwrap();
            if slot.as_ref().is_some_and(|(v, _)| *v == to) {
                slot.take().map(|(_, c)| c)
            } else {
                None
            }
        };
        callback.map_or(Ok(()), |c| c())
    }
}
fn ready<T>(future: impl std::future::Future<Output = T>) -> T {
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    match std::pin::pin!(future).as_mut().poll(&mut cx) {
        std::task::Poll::Ready(v) => v,
        _ => panic!("JSONL repo must complete first poll"),
    }
}
fn n(v: u64) -> SafeUnsigned {
    SafeUnsigned::new(v).unwrap()
}
fn sid(v: &str) -> SessionId {
    SessionId::new(v).unwrap()
}
struct Dir(PathBuf);
impl Dir {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "pi-file-repo-unit-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn file(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}
impl Drop for Dir {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}
#[derive(Default)]
struct Ids(AtomicU64);
impl IdGenerator for Ids {
    fn next(&self, time: Option<SafeUnsigned>) -> String {
        let t = time.unwrap_or(n(100)).get();
        format!(
            "{:08x}-{:04x}-7000-8000-{:012x}",
            t >> 16,
            t & 65535,
            self.0.fetch_add(1, Ordering::SeqCst)
        )
    }
}
struct Clock;
impl StorageClock for Clock {
    fn now(&self) -> Result<SafeUnsigned, StorageError> {
        Ok(n(1234))
    }
}
#[derive(Default)]
struct Telemetry(AtomicU64);
impl TelemetryContext for Telemetry {
    fn start_span(&self, options: pi_telemetry::SpanOptions) -> pi_telemetry::SpanGuard {
        self.0.fetch_add(1, Ordering::SeqCst);
        pi_telemetry::NOOP_TELEMETRY_CONTEXT.start_span(options)
    }
}
fn repository(dir: &Dir) -> JsonlSessionRepo {
    JsonlSessionRepo::new(
        &dir.0,
        SessionCodecOptions::default(),
        ProviderHeaderPolicy::default(),
        Arc::new(Ids::default()),
        Arc::new(Clock),
        Arc::new(pi_telemetry::NoopTelemetryContext),
        JsonlOptions::default(),
    )
    .unwrap()
}
fn opts(id: &str) -> SessionCreateOptions {
    SessionCreateOptions {
        id: Some(sid(id)),
        ..Default::default()
    }
}
fn request(id: &str, fork: ForkOptions) -> SessionForkRequest<SessionCreateOptions> {
    SessionForkRequest {
        create: opts(id),
        fork,
    }
}
fn branch() -> ForkOptions {
    ForkOptions::Branch(BranchForkOptions::default())
}
fn tree() -> ForkOptions {
    ForkOptions::Tree(TreeForkOptions::default())
}
fn snapshot(session: &JsonlSession) -> StorageSnapshot {
    ready(session.inner.storage.snapshot()).unwrap()
}
fn set(key: &str, value: serde_json::Value) -> Write {
    Write::SetRegister(Box::new(RegisterSet::FactCustom {
        key: key.into(),
        value: serde_json::from_value(value).unwrap(),
    }))
}
fn tx(writes: Vec<Write>) -> Transaction {
    Transaction { writes }
}
fn legacy(id: &str) -> Vec<u8> {
    format!("{{\"type\":\"session\",\"version\":3,\"id\":{},\"timestamp\":\"1970-01-01T00:00:00.001Z\",\"cwd\":\"source-cwd\"}}\n{{\"type\":\"custom\",\"id\":\"old\",\"parentId\":null,\"timestamp\":\"1970-01-01T00:00:00.002Z\",\"customType\":\"note\",\"data\":{{\"opaque\":\"old\"}}}}\n",serde_json::to_string(id).unwrap()).into_bytes()
}
fn header(id: &str, version: u64) -> Vec<u8> {
    serde_json::to_vec(
        &serde_json::json!({"v":4,"kind":"header","id":id,"createdAt":1,"storageVersion":version}),
    )
    .unwrap()
}
fn idle(session: &JsonlSession) {
    assert!(matches!(
        ready(session.get_register(RegisterNamespace::LaneLeaf, "main")).unwrap(),
        Some(Register::LaneLeaf { .. })
    ));
    assert!(
        matches!(ready(session.get_register(RegisterNamespace::LaneState,"main")).unwrap(),Some(Register::LaneState {value:LaneState {current_operation_id:None,pending_next_run},..}) if pending_next_run.is_empty())
    );
    assert_eq!(
        ready(session.get_register(RegisterNamespace::LaneConfig, "main")).unwrap(),
        None
    );
}

#[test]
fn create_is_complete_at_first_visibility_without_scratch_write_telemetry() {
    let dir = Dir::new();
    let telemetry = Arc::new(Telemetry::default());
    let ids = Arc::new(Ids::default());
    let repo = JsonlSessionRepo::new(
        &dir.0,
        SessionCodecOptions::default(),
        ProviderHeaderPolicy::default(),
        ids.clone(),
        Arc::new(Clock),
        telemetry.clone(),
        JsonlOptions::default(),
    )
    .unwrap();
    let path = repo.encoded_path(&sid("../雪"));
    let checking = path.clone();
    *repo.inner.hooks.create.lock().unwrap() = Some(Box::new(move || {
        assert!(!checking.exists());
        Ok(())
    }));
    let published = path.clone();
    *repo.inner.hooks.published.lock().unwrap() = Some(Box::new(move || {
        // This is after the real final-path rename and before Session exposure.
        let bytes = fs::read(published).unwrap();
        let rows: Vec<serde_json::Value> = bytes
            .split(|b| *b == b'\n')
            .filter(|v| !v.is_empty())
            .map(|v| serde_json::from_slice(v).unwrap())
            .collect();
        assert_eq!(
            rows[0]["snapshotItems"], 2,
            "first published file must already be seeded"
        );
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[1]["namespace"], "lane.leaf");
        assert_eq!(rows[2]["namespace"], "lane.state");
        Ok(())
    }));
    let mut options = opts("../雪");
    options.cwd = Some("explicit".into());
    let session = ready(repo.create(options)).unwrap();
    idle(&session);
    assert_eq!(session.metadata().path, path);
    assert_eq!(session.metadata().base.cwd.as_deref(), Some("explicit"));
    assert_eq!(session.metadata().base.created_at, n(1234));
    assert_eq!(ids.0.load(Ordering::SeqCst), 0);
    assert_eq!(telemetry.0.load(Ordering::SeqCst), 0);
    let bytes = fs::read(path).unwrap();
    let rows: Vec<serde_json::Value> = bytes
        .split(|b| *b == b'\n')
        .filter(|v| !v.is_empty())
        .map(|v| serde_json::from_slice(v).unwrap())
        .collect();
    assert_eq!(rows[0]["snapshotItems"], 2);
    assert_eq!(rows.len(), 3);
    assert_eq!(snapshot(&session).next_seq, Some(n(2)));
    ready(session.commit(tx(vec![set("actual", serde_json::json!(1))]))).unwrap();
    assert_eq!(telemetry.0.load(Ordering::SeqCst), 1);
}
#[test]
fn header_id_namespace_rejects_differently_named_legacy_and_pending_reentry() {
    let dir = Dir::new();
    let repo = repository(&dir);
    let path = dir.file("old-name.jsonl");
    let bytes = legacy("collision");
    fs::write(&path, &bytes).unwrap();
    assert!(ready(repo.create(opts("collision"))).is_err());
    assert_eq!(fs::read(&path).unwrap(), bytes);
    assert_eq!(fs::read_dir(&dir.0).unwrap().count(), 1);
    let source = ready(repo.create(opts("source"))).unwrap();
    assert!(ready(repo.fork(source.metadata(), request("collision", branch()))).is_err());
    let other = repo.clone();
    *repo.inner.hooks.create.lock().unwrap() = Some(Box::new(move || {
        assert!(matches!(
            ready(other.create(opts("pending"))),
            Err(SessionError::Storage(StorageError::Busy(_)))
        ));
        assert_eq!(ready(other.list(())).unwrap().len(), 2);
        Ok(())
    }));
    ready(repo.create(opts("pending"))).unwrap();
}
#[test]
fn destination_rechecks_header_namespace_after_callbacks() {
    let dir = Dir::new();
    let repo = repository(&dir);
    let intruder = dir.file("late-name.jsonl");
    let copy = intruder.clone();
    *repo.inner.hooks.create.lock().unwrap() = Some(Box::new(move || {
        fs::write(copy, legacy("late")).unwrap();
        Ok(())
    }));
    assert!(ready(repo.create(opts("late"))).is_err());
    assert!(intruder.exists());
    assert!(!repo.encoded_path(&sid("late")).exists());
    assert_eq!(ready(repo.list(())).unwrap().len(), 1);
}
#[test]
fn list_is_header_only_sorted_nonrecursive_and_rejects_duplicate_identity() {
    let dir = Dir::new();
    let repo = repository(&dir);
    let a = dir.file("a.jsonl");
    let z = dir.file("z.jsonl");
    let mut bytes = header("future", 9);
    bytes.extend(b"\nnot a current body\n");
    fs::write(&z, bytes).unwrap();
    let mut bytes = legacy("legacy");
    bytes.extend(b"invalid legacy body\n");
    fs::write(&a, bytes).unwrap();
    fs::write(dir.file("ignore.tmp"), b"broken").unwrap();
    fs::create_dir(dir.file("nested")).unwrap();
    fs::write(dir.file("nested/child.jsonl"), b"broken").unwrap();
    let values = ready(repo.list(())).unwrap();
    assert_eq!(
        values
            .iter()
            .map(|v| v.base.id.as_str())
            .collect::<Vec<_>>(),
        vec!["legacy", "future"]
    );
    assert!(ready(repo.open(values[0].clone())).is_err());
    assert!(matches!(
        ready(repo.open(values[1].clone())),
        Err(SessionError::NewerStorageVersion(_))
    ));
    fs::write(dir.file("duplicate.jsonl"), header("future", 1)).unwrap();
    assert!(ready(repo.list(())).is_err());
    fs::remove_file(dir.file("duplicate.jsonl")).unwrap();
    fs::write(dir.file("broken.jsonl"), b"{}").unwrap();
    assert!(ready(repo.list(())).is_err());
    assert!(ready(repo.create(opts("new"))).is_err());
}
#[test]
fn authoritative_metadata_and_active_views_survive_reopen_delete_recreation() {
    let dir = Dir::new();
    let repo = repository(&dir);
    let root = ready(repo.create(opts("session"))).unwrap();
    let address = Arc::as_ptr(&root);
    let view = root.view(LaneId::new("main").unwrap());
    let mut metadata = root.metadata();
    metadata.base.created_at = n(0);
    metadata.base.storage_version = n(999);
    metadata.base.store_generation = n(99);
    metadata.base.cwd = Some("forged".into());
    drop(root);
    let same = ready(repo.open(metadata.clone())).unwrap();
    assert_eq!(Arc::as_ptr(&same), address);
    assert_eq!(same.metadata().base.created_at, n(1234));
    assert_eq!(same.metadata().base.cwd, None);
    let mut wrong = metadata.clone();
    wrong.base.id = sid("wrong");
    assert!(ready(repo.open(wrong)).is_err());
    let other = repository(&dir);
    assert!(matches!(
        ready(other.open_path(&metadata.path)),
        Err(SessionError::Storage(StorageError::Busy(_)))
    ));
    ready(same.close()).unwrap();
    let next = ready(repo.open(metadata.clone())).unwrap();
    assert!(!Arc::ptr_eq(&same, &next));
    assert!(ready(view.get_leaf_id()).is_err());
    ready(repo.delete(metadata.clone())).unwrap();
    assert!(ready(next.get_leaf_id()).is_err());
    let recreated = ready(repo.create(opts("session"))).unwrap();
    ready(next.close()).unwrap();
    assert!(Arc::ptr_eq(
        &recreated,
        &ready(repo.open(metadata)).unwrap()
    ));
    drop(repo);
    idle(&recreated);
}
#[test]
fn delete_transfers_live_claim_through_unlink_and_failure_keeps_file() {
    let dir = Dir::new();
    let repo = repository(&dir);
    let session = ready(repo.create(opts("delete"))).unwrap();
    let path = session.metadata().path;
    let other = repo.clone();
    let foreign = repository(&dir);
    let old = session.clone();
    let inspect = path.clone();
    *repo.inner.hooks.seal.lock().unwrap() = Some(Box::new(move || {
        assert!(ready(old.get_leaf_id()).is_err());
        assert!(matches!(
            ready(foreign.open_path(&inspect)),
            Err(SessionError::Storage(StorageError::Busy(_)))
        ));
        assert!(matches!(
            ready(other.create(opts("delete"))),
            Err(SessionError::Storage(StorageError::Busy(_)))
        ));
        Err(invalid())
    }));
    assert!(ready(repo.delete(session.metadata())).is_err());
    assert!(path.exists());
    let reopened = ready(repo.open_path(&path)).unwrap();
    idle(&reopened);
    ready(repo.delete(reopened.metadata())).unwrap();
    assert!(!path.exists());
    ready(repo.delete(reopened.metadata())).unwrap();
    let next = ready(repo.create(opts("delete"))).unwrap();
    ready(session.close()).unwrap();
    idle(&next);
}
#[test]
fn legacy_readonly_open_close_parent_lookup_and_external_path_do_not_convert() {
    let dir = Dir::new();
    let repo = repository(&dir);
    let external = Dir::new();
    let path = external.file("legacy.jsonl");
    let mut lines = String::from_utf8(legacy("legacy")).unwrap();
    lines = lines.replacen(
        "\"cwd\":\"source-cwd\"",
        "\"cwd\":\"source-cwd\",\"parentSession\":\"parent.jsonl\"",
        1,
    );
    let bytes = lines.into_bytes();
    fs::write(&path, &bytes).unwrap();
    let mut parent = header("parent-id", 8);
    parent.extend(b"\ninvalid parent body");
    fs::write(external.file("parent.jsonl"), parent).unwrap();
    let root = ready(repo.open_path(&path)).unwrap();
    assert_eq!(
        root.metadata().base.parent_session_id,
        Some(sid("parent-id"))
    );
    assert_eq!(root.metadata().base.legacy_parent_session_path, None);
    assert_eq!(snapshot(&root).usage.len(), 1);
    idle(&root);
    assert_eq!(fs::read(&path).unwrap(), bytes);
    assert!(ready(repo.list(())).unwrap().is_empty());
    let expected = snapshot(&root);
    ready(root.close()).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o400)).unwrap();
    }
    let reopened = ready(repo.open_path(&path)).unwrap();
    assert_eq!(snapshot(&reopened), expected);
    ready(reopened.close()).unwrap();
    assert_eq!(fs::read(&path).unwrap(), bytes);
    fs::remove_file(external.file("parent.jsonl")).unwrap();
    let unresolved = ready(repo.open_path(&path)).unwrap();
    assert_eq!(
        unresolved
            .metadata()
            .base
            .legacy_parent_session_path
            .as_deref(),
        Some("parent.jsonl")
    );
}
#[test]
fn real_legacy_first_empty_multiwrite_rejection_and_cas_conversion() {
    for writes in [
        vec![],
        vec![
            set("k", serde_json::json!(1)),
            set("k", serde_json::json!(2)),
            Write::DeleteRegister(RegisterDelete {
                namespace: RegisterNamespace::FactCustom,
                key: "k".into(),
            }),
        ],
    ] {
        let dir = Dir::new();
        let repo = repository(&dir);
        let path = dir.file("legacy.jsonl");
        let source = legacy("legacy");
        fs::write(&path, &source).unwrap();
        let session = ready(repo.open_path(&path)).unwrap();
        let pre = snapshot(&session);
        let bad:Write=serde_json::from_value(serde_json::json!({"kind":"entry","entry":{"id":"bad","parentId":"missing","type":"custom","customType":"note"}})).unwrap();
        assert!(ready(session.commit(tx(vec![bad]))).is_err());
        assert_eq!(fs::read(&path).unwrap(), source);
        let mismatch = ConditionalCommit {
            conditions: vec![RegisterCondition {
                register: RegisterKey {
                    namespace: RegisterNamespace::FactCustom,
                    key: "missing".into(),
                },
                expected: ExpectedRegister::Seq(n(0)),
            }],
            transaction: tx(vec![]),
        };
        assert_eq!(
            ready(session.commit_conditional(mismatch)).unwrap(),
            ConditionalOutcome::NotApplied
        );
        assert_eq!(fs::read(&path).unwrap(), source);
        let result = ready(session.commit(tx(writes.clone()))).unwrap();
        let bytes = fs::read(&path).unwrap();
        let lines: Vec<serde_json::Value> = bytes
            .split(|b| *b == b'\n')
            .filter(|v| !v.is_empty())
            .map(|v| serde_json::from_slice(v).unwrap())
            .collect();
        assert_eq!(lines[0]["v"], 4);
        assert_eq!(
            lines[0]["snapshotItems"],
            pre.entries.len() + pre.registers.len() + pre.usage.len()
        );
        let final_frame = lines.last().unwrap();
        assert_eq!(final_frame.as_array().unwrap().len(), writes.len());
        assert_eq!(result.first_seq, pre.next_seq.unwrap());
        let after = snapshot(&session);
        assert_eq!(after.usage, pre.usage);
        ready(session.close()).unwrap();
        let reopened = ready(repo.open_path(&path)).unwrap();
        assert_eq!(snapshot(&reopened), after);
    }
}

#[test]
fn active_and_inactive_forks_copy_literal_rows_with_fresh_idle_registers() {
    let dir = Dir::new();
    let repo = repository(&dir);
    let source = ready(repo.create(opts("source"))).unwrap();
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../conformance/session/repository-contract.json"
    ))
    .unwrap();
    let writes: Vec<Write> = serde_json::from_value(fixture["entry_writes"].clone()).unwrap();
    ready(source.commit(tx(writes))).unwrap();
    let leaf = |key: &str, value: &str| {
        Write::SetRegister(Box::new(RegisterSet::LaneLeaf {
            key: key.into(),
            value: Some(EntryId::new(value).unwrap()),
        }))
    };
    ready(source.commit(tx(vec![
        leaf("main", "S"),
        leaf("side", "E"),
        Write::SetRegister(Box::new(RegisterSet::LaneState {
            key: "side".into(),
            value: LaneState {
                current_operation_id: None,
                pending_next_run: vec![],
            },
        })),
        set("fact", serde_json::json!({"opaque":true})),
    ])))
    .unwrap();
    let config:LaneConfiguration=serde_json::from_value(serde_json::json!({"model":{"provider":"future","modelId":"model"},"thinkingLevel":"off","activeToolNames":[]})).unwrap();
    let mut ledger = Usage::zero();
    ledger.input = n(9);
    ledger.total_tokens = n(9);
    ready(source.commit(tx(vec![
        Write::SetRegister(Box::new(RegisterSet::LaneConfig {
            key: "main".into(),
            value: config.clone(),
        })),
        Write::SetRegister(Box::new(RegisterSet::LaneConfig {
            key: "side".into(),
            value: config,
        })),
        Write::Usage(NewUsageRow {
            id: UsageId::new("ledger").unwrap(),
            usage: ledger,
            entry_id: None,
            adjustment: true,
            details: Presence::Missing,
        }),
    ])))
    .unwrap();
    ready(source.set_name(Some("copied".into()))).unwrap();
    ready(source.set_label(EntryId::new("E").unwrap(), Some("side-label".into()))).unwrap();
    let before = snapshot(&source);
    let bytes = fs::read(&source.metadata().path).unwrap();
    for (index, scope, key) in [
        (0, branch(), "branch_ids"),
        (
            1,
            ForkOptions::Branch(BranchForkOptions {
                position: Some(ForkPosition::Before),
                ..Default::default()
            }),
            "before_ids",
        ),
        (2, tree(), "tree_ids"),
    ] {
        if index == 1 {
            ready(source.close()).unwrap();
        }
        let fork =
            ready(repo.fork(source.metadata(), request(&format!("fork-{index}"), scope))).unwrap();
        let actual = snapshot(&fork);
        let ids: Vec<_> = actual
            .entries
            .iter()
            .map(|e| crate::storage::values::structure(e).id.to_string())
            .collect();
        assert_eq!(serde_json::to_value(ids).unwrap(), fixture["expected"][key]);
        for entry in &actual.entries {
            assert!(before.entries.contains(entry));
        }
        assert!(actual.usage.is_empty());
        assert_eq!(actual.stats.usage, Usage::zero());
        assert_eq!(
            ready(fork.list_registers(RegisterNamespace::LaneConfig, None))
                .unwrap()
                .len(),
            if index == 2 { 2 } else { 1 }
        );
        assert_eq!(fork.metadata().base.parent_session_id, Some(sid("source")));
        assert_eq!(fork.metadata().base.cwd, None);
        assert!(actual.registers.iter().all(|r| {
            crate::storage::values::register_seq(r)
                > actual
                    .entries
                    .iter()
                    .map(|e| crate::storage::values::structure(e).seq)
                    .max()
                    .unwrap()
        }));
        assert_eq!(
            ready(fork.list_registers(RegisterNamespace::LaneLeaf, None))
                .unwrap()
                .len(),
            if index == 2 { 2 } else { 1 }
        );
        assert_eq!(
            ready(fork.get_register(RegisterNamespace::FactLabel, "E"))
                .unwrap()
                .is_some(),
            index == 2
        );
    }
    assert_eq!(fs::read(&source.metadata().path).unwrap(), bytes);
}
#[test]
fn legacy_active_and_inactive_forks_exclude_virtual_usage_and_never_convert_source() {
    let dir = Dir::new();
    let repo = repository(&dir);
    let path = dir.file("legacy.jsonl");
    let bytes = legacy("legacy");
    fs::write(&path, &bytes).unwrap();
    let source = ready(repo.open_path(&path)).unwrap();
    let before = snapshot(&source);
    assert_eq!(before.usage.len(), 1);
    for (i, scope) in [branch(), tree()].into_iter().enumerate() {
        if i == 1 {
            ready(source.close()).unwrap();
        }
        let fork =
            ready(repo.fork(source.metadata(), request(&format!("copy-{i}"), scope))).unwrap();
        idle(&fork);
        let actual = snapshot(&fork);
        assert_eq!(actual.entries, before.entries);
        assert!(actual.usage.is_empty());
        assert_eq!(actual.stats.usage, Usage::zero());
        assert_eq!(fork.metadata().base.parent_session_id, Some(sid("legacy")));
        assert_eq!(fs::read(&path).unwrap(), bytes);
    }
}
#[test]
fn actual_full_legacy_fixture_uses_supplied_codec_and_preserves_one_adjustment() {
    let dir = Dir::new();
    let path = dir.file("all.jsonl");
    let bytes = include_bytes!("../../../../../conformance/session/legacy-v3/all-v3.jsonl");
    fs::write(&path, bytes).unwrap();
    let rejecting = repository(&dir);
    assert!(ready(rejecting.open_path(&path)).is_err());
    assert_eq!(fs::read(&path).unwrap(), bytes);
    let repo = JsonlSessionRepo::new(
        &dir.0,
        legacy_v3::coding_message_options(),
        ProviderHeaderPolicy::default(),
        Arc::new(Ids::default()),
        Arc::new(Clock),
        Arc::new(pi_telemetry::NoopTelemetryContext),
        JsonlOptions::default(),
    )
    .unwrap();
    let session = ready(repo.open_path(&path)).unwrap();
    let expected: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../conformance/session/legacy-v3/all-v3.expected.json"
    ))
    .unwrap();
    let actual = snapshot(&session);
    assert_eq!(serde_json::to_value(&actual).unwrap(), expected["snapshot"]);
    ready(session.commit(tx(vec![]))).unwrap();
    ready(session.close()).unwrap();
    assert_eq!(snapshot(&ready(repo.open_path(&path)).unwrap()), actual);
}
fn synthetic(
    dir: &Dir,
    target: u64,
    steps: Vec<crate::migration::MigrationStep<JsonValue>>,
) -> JsonlSessionRepo {
    let mut repo = repository(dir);
    Arc::get_mut(&mut repo.inner).unwrap().migrations =
        Arc::new(MigrationRegistry::new(n(target), steps).unwrap());
    repo
}
struct Mapping {
    value: serde_json::Value,
}
impl crate::migration::RegisterMigration<JsonValue> for Mapping {
    fn convert(
        &self,
        row: &crate::migration::MigrationRegister<JsonValue>,
    ) -> Result<JsonValue, crate::migration::MigrationConversionError> {
        if row.key.namespace == RegisterNamespace::FactCustom && row.key.key == "mapped" {
            Ok(serde_json::from_value(self.value.clone()).unwrap())
        } else {
            Ok(row.value.clone())
        }
    }
}
fn step(from: u64, value: serde_json::Value) -> crate::migration::MigrationStep<JsonValue> {
    crate::migration::MigrationStep {
        versions: crate::migration::MigrationVersions {
            from: n(from),
            to: n(from + 1),
        },
        converter: Arc::new(Mapping { value }),
    }
}
fn migration_source(dir: &Dir) -> (JsonlSessionMetadata, StorageSnapshot) {
    let repo = repository(dir);
    let session = ready(repo.create(opts("migrate"))).unwrap();
    ready(session.commit(tx(vec![set("mapped", serde_json::json!({"old":1}))]))).unwrap();
    let snapshot = snapshot(&session);
    let metadata = session.metadata();
    ready(session.close()).unwrap();
    (metadata, snapshot)
}
#[test]
fn real_migration_publishes_values_versions_and_resumes_after_later_failure() {
    let dir = Dir::new();
    let (metadata, before) = migration_source(&dir);
    let repo = synthetic(
        &dir,
        3,
        vec![
            step(1, serde_json::json!({"middle":2})),
            step(2, serde_json::json!({"final":3})),
        ],
    );
    *repo.inner.hooks.migration.lock().unwrap() = Some((n(3), Box::new(|| Err(invalid()))));
    assert!(ready(repo.open(metadata.clone())).is_err());
    let mut claim = FileClaim::acquire(&metadata.path).unwrap();
    let middle = claim.read_raw().unwrap();
    assert_eq!(middle.metadata.storage_version, n(2));
    assert_eq!(
        serde_json::to_value(
            &middle
                .registers
                .iter()
                .find(|r| r.key.key == "mapped")
                .unwrap()
                .value
        )
        .unwrap(),
        serde_json::json!({"middle":2})
    );
    assert_eq!(middle.entries, before.entries);
    assert_eq!(middle.usage, before.usage);
    assert_eq!(middle.next_seq, before.next_seq);
    drop(claim);
    let session = ready(repo.open(metadata)).unwrap();
    let after = snapshot(&session);
    assert_eq!(after.storage_version, n(3));
    assert_eq!(
        serde_json::to_value(
            ready(session.get_register(RegisterNamespace::FactCustom, "mapped"))
                .unwrap()
                .unwrap()
        )
        .unwrap()["value"],
        serde_json::json!({"final":3})
    );
    assert_eq!(after.next_seq, before.next_seq);
    assert_eq!(after.store_generation, before.store_generation);
    assert_eq!(after.stats, before.stats);
}
#[test]
fn migration_outer_claim_survives_inner_lease_and_completed_exposure_failure() {
    let dir = Dir::new();
    let (metadata, _) = migration_source(&dir);
    let repo = synthetic(&dir, 2, vec![step(1, serde_json::json!({"final":2}))]);
    let path = metadata.path.clone();
    *repo.inner.hooks.expose.lock().unwrap() = Some(Box::new(move || {
        assert!(matches!(
            FileClaim::acquire(&path),
            Err(StorageError::Busy(_))
        ));
        Err(invalid())
    }));
    assert!(ready(repo.open(metadata.clone())).is_err());
    let mut claim = FileClaim::acquire(&metadata.path).unwrap();
    assert_eq!(claim.read_header().unwrap().metadata.storage_version, n(2));
    drop(claim);
    let session = ready(repo.open(metadata)).unwrap();
    assert_eq!(snapshot(&session).storage_version, n(2));
}
#[test]
fn final_migration_codec_rejection_cannot_publish_current_header() {
    use crate::migration::*;
    struct Bad;
    impl RegisterMigration<JsonValue> for Bad {
        fn convert(
            &self,
            row: &MigrationRegister<JsonValue>,
        ) -> Result<JsonValue, MigrationConversionError> {
            if row.key.namespace == RegisterNamespace::LaneState {
                Ok(JsonValue::Null)
            } else {
                Ok(row.value.clone())
            }
        }
    }
    let dir = Dir::new();
    let (metadata, _) = migration_source(&dir);
    let before = fs::read(&metadata.path).unwrap();
    let repo = synthetic(
        &dir,
        2,
        vec![MigrationStep {
            versions: MigrationVersions {
                from: n(1),
                to: n(2),
            },
            converter: Arc::new(Bad),
        }],
    );
    assert!(ready(repo.open(metadata.clone())).is_err());
    assert_eq!(fs::read(&metadata.path).unwrap(), before);
    let mut claim = FileClaim::acquire(&metadata.path).unwrap();
    assert_eq!(claim.read_header().unwrap().metadata.storage_version, n(1));
}

#[cfg(unix)]
#[test]
fn legacy_actual_io_failure_and_changed_source_do_not_convert() {
    use std::os::unix::fs::PermissionsExt;
    let dir = Dir::new();
    let repo = repository(&dir);
    let path = dir.file("legacy.jsonl");
    let bytes = legacy("legacy");
    fs::write(&path, &bytes).unwrap();
    let session = ready(repo.open_path(&path)).unwrap();
    let before = snapshot(&session);
    fs::set_permissions(&dir.0, fs::Permissions::from_mode(0o500)).unwrap();
    let failed = ready(session.commit(tx(vec![])));
    fs::set_permissions(&dir.0, fs::Permissions::from_mode(0o700)).unwrap();
    assert!(matches!(
        failed,
        Err(SessionError::Storage(StorageError::Io(_)))
    ));
    assert_eq!(fs::read(&path).unwrap(), bytes);
    assert_eq!(snapshot(&session), before);
    let changed = [bytes.as_slice(), b"\n"].concat();
    fs::write(&path, &changed).unwrap();
    assert!(matches!(
        ready(session.commit(tx(vec![]))),
        Err(SessionError::Storage(StorageError::Conflict(_)))
    ));
    assert_eq!(fs::read(&path).unwrap(), changed);
    ready(session.close()).unwrap();
    let next = ready(repo.open_path(&path)).unwrap();
    assert_eq!(snapshot(&next), before);
    ready(next.commit(tx(vec![]))).unwrap();
    assert_eq!(snapshot(&next).usage.len(), 1);
}
#[cfg(unix)]
#[test]
fn migration_real_io_failure_retains_previous_committed_step() {
    use std::os::unix::fs::PermissionsExt;
    let dir = Dir::new();
    let (metadata, _) = migration_source(&dir);
    let repo = synthetic(
        &dir,
        3,
        vec![step(1, serde_json::json!(2)), step(2, serde_json::json!(3))],
    );
    let directory = dir.0.clone();
    *repo.inner.hooks.migration.lock().unwrap() = Some((
        n(3),
        Box::new(move || {
            fs::set_permissions(directory, fs::Permissions::from_mode(0o500)).unwrap();
            Ok(())
        }),
    ));
    let failed = ready(repo.open(metadata.clone()));
    fs::set_permissions(&dir.0, fs::Permissions::from_mode(0o700)).unwrap();
    assert!(failed.is_err());
    let mut claim = FileClaim::acquire(&metadata.path).unwrap();
    assert_eq!(claim.read_header().unwrap().metadata.storage_version, n(2));
    drop(claim);
    assert_eq!(
        snapshot(&ready(repo.open(metadata)).unwrap()).storage_version,
        n(3)
    );
}
#[cfg(unix)]
#[test]
fn claimed_delete_refuses_a_replacement_file_and_symlink_open_collapses() {
    use std::os::unix::fs::symlink;
    let dir = Dir::new();
    let repo = repository(&dir);
    let session = ready(repo.create(opts("original"))).unwrap();
    let metadata = session.metadata();
    let alias = dir.file("alias-link");
    symlink(&metadata.path, &alias).unwrap();
    assert!(Arc::ptr_eq(
        &session,
        &ready(repo.open_path(alias)).unwrap()
    ));
    let replacement = header("replacement", 1);
    let other = dir.file("new-file");
    fs::write(&other, &replacement).unwrap();
    let path = metadata.path.clone();
    *repo.inner.hooks.seal.lock().unwrap() = Some(Box::new(move || {
        fs::rename(other, path).unwrap();
        Ok(())
    }));
    assert!(ready(repo.delete(metadata.clone())).is_err());
    assert_eq!(fs::read(&metadata.path).unwrap(), replacement);
    assert!(ready(session.get_leaf_id()).is_err());
    let next = ready(repo.open_path(&metadata.path)).unwrap();
    assert_eq!(next.metadata().base.id, sid("replacement"));
}
#[test]
fn cross_repository_same_destination_reservation_and_generated_ids_are_checked() {
    struct Bad;
    impl IdGenerator for Bad {
        fn next(&self, _: Option<SafeUnsigned>) -> String {
            "invalid-id".into()
        }
    }
    let dir = Dir::new();
    let bad = JsonlSessionRepo::new(
        &dir.0,
        SessionCodecOptions::default(),
        ProviderHeaderPolicy::default(),
        Arc::new(Bad),
        Arc::new(Clock),
        Arc::new(pi_telemetry::NoopTelemetryContext),
        JsonlOptions::default(),
    )
    .unwrap();
    assert!(ready(bad.create(SessionCreateOptions::default())).is_err());
    assert!(ready(bad.list(())).unwrap().is_empty());
    let repo = repository(&dir);
    let foreign = repository(&dir);
    *repo.inner.hooks.create.lock().unwrap() = Some(Box::new(move || {
        ready(foreign.create(opts("race"))).unwrap();
        Ok(())
    }));
    assert!(ready(repo.create(opts("race"))).is_err());
    let values = ready(repo.list(())).unwrap();
    assert_eq!(values.len(), 1);
    let session = ready(repo.open(values[0].clone())).unwrap();
    idle(&session);
    let generated = ready(repo.create(SessionCreateOptions::default())).unwrap();
    assert_eq!(
        crate::id::uuid_timestamp(generated.metadata().base.id.as_str()).unwrap(),
        n(100)
    );
}
#[test]
fn real_migration_reads_and_preserves_all_raw_namespace_envelopes() {
    let dir = Dir::new();
    let repo = repository(&dir);
    let session = ready(repo.create(opts("all"))).unwrap();
    let entry:Write=serde_json::from_value(serde_json::json!({"kind":"entry","entry":{"id":"entry-message","parentId":null,"type":"custom","customType":"note"}})).unwrap();
    ready(session.commit(tx(vec![entry]))).unwrap();

    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../conformance/schema/agent-state/durable-v1.json"
    ))
    .unwrap();
    for value in fixture["registers"].as_array().unwrap() {
        let mut value = value.clone();
        value.as_object_mut().unwrap().remove("seq");
        if value["namespace"] == "lane.leaf" {
            value["value"] = serde_json::Value::Null;
        }
        let write = Write::SetRegister(Box::new(serde_json::from_value(value).unwrap()));
        ready(session.commit(tx(vec![write]))).unwrap();
    }
    ready(session.commit(tx(vec![set("mapped", serde_json::json!("old"))]))).unwrap();
    let before = snapshot(&session);
    let metadata = session.metadata();
    ready(session.close()).unwrap();
    let migrated = synthetic(&dir, 2, vec![step(1, serde_json::json!("new"))]);
    let result = ready(migrated.open(metadata)).unwrap();
    let after = snapshot(&result);
    let envelopes = |snapshot: &StorageSnapshot| {
        snapshot
            .registers
            .iter()
            .map(|r| {
                (
                    crate::storage::values::register_key(r),
                    crate::storage::values::register_seq(r),
                )
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(envelopes(&after), envelopes(&before));
    assert_eq!(
        envelopes(&after)
            .iter()
            .map(|(k, _)| k.namespace)
            .collect::<BTreeSet<_>>()
            .len(),
        12
    );
    assert_eq!(after.next_seq, before.next_seq);
}

#[test]
fn actual_clock_reentry_keeps_reservation_and_failure_cleans_only_its_owner() {
    struct Reclock {
        callback: Mutex<Option<Box<dyn FnOnce() + Send>>>,
        fail: std::sync::atomic::AtomicBool,
    }
    impl StorageClock for Reclock {
        fn now(&self) -> Result<SafeUnsigned, StorageError> {
            let callback = self.callback.lock().unwrap().take();
            if let Some(callback) = callback {
                callback();
                if self.fail.swap(false, Ordering::SeqCst) {
                    return Err(StorageError::Io(context("clock unavailable")));
                }
            }
            Ok(n(10))
        }
    }
    let dir = Dir::new();
    let clock = Arc::new(Reclock {
        callback: Mutex::new(None),
        fail: std::sync::atomic::AtomicBool::new(true),
    });
    let repo = JsonlSessionRepo::new(
        &dir.0,
        SessionCodecOptions::default(),
        ProviderHeaderPolicy::default(),
        Arc::new(Ids::default()),
        clock.clone(),
        Arc::new(pi_telemetry::NoopTelemetryContext),
        JsonlOptions::default(),
    )
    .unwrap();
    let nested = repo.clone();
    *clock.callback.lock().unwrap() = Some(Box::new(move || {
        assert!(ready(nested.list(())).unwrap().is_empty());
        assert!(matches!(
            ready(nested.create(opts("outer"))),
            Err(SessionError::Storage(StorageError::Busy(_)))
        ));
        ready(nested.create(opts("other"))).unwrap();
    }));
    assert!(ready(repo.create(opts("outer"))).is_err());
    assert!(!repo.encoded_path(&sid("outer")).exists());
    assert_eq!(ready(repo.list(())).unwrap().len(), 1);
    idle(&ready(repo.create(opts("outer"))).unwrap());
}
#[test]
fn failed_exposure_keeps_a_complete_published_destination_and_its_namespace() {
    let dir = Dir::new();
    let repo = repository(&dir);
    *repo.inner.hooks.published.lock().unwrap() = Some(Box::new(|| Err(invalid())));
    assert!(ready(repo.create(opts("published"))).is_err());
    let discovered = ready(repo.list(())).unwrap();
    assert_eq!(discovered.len(), 1);
    assert!(ready(repo.create(opts("published"))).is_err());
    let reopened = ready(repo.open(discovered[0].clone())).unwrap();
    idle(&reopened);
}

#[test]
fn wrong_delete_id_cannot_seal_or_release_the_active_selected_path() {
    let dir = Dir::new();
    let repo = repository(&dir);
    let session = ready(repo.create(opts("actual"))).unwrap();
    let before = fs::read(&session.metadata().path).unwrap();
    let mut wrong = session.metadata();
    wrong.base.id = sid("wrong");
    assert!(matches!(
        ready(repo.delete(wrong)),
        Err(SessionError::InvalidOperation(_))
    ));
    assert!(
        ready(session.get_leaf_id()).is_ok(),
        "wrong ID must not seal active session"
    );
    assert_eq!(fs::read(&session.metadata().path).unwrap(), before);
    let foreign = repository(&dir);
    assert!(matches!(
        ready(foreign.open_path(&session.metadata().path)),
        Err(SessionError::Storage(StorageError::Busy(_)))
    ));
    let mut stale = session.metadata();
    stale.base.created_at = n(0);
    stale.base.storage_version = n(999);
    ready(repo.delete(stale)).unwrap();
    assert!(ready(session.get_leaf_id()).is_err());
}
#[test]
fn occupied_encoded_path_cannot_hide_its_real_header_during_clock_reentry() {
    struct Clock {
        callback: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    }
    impl StorageClock for Clock {
        fn now(&self) -> Result<SafeUnsigned, StorageError> {
            let callback = self.callback.lock().unwrap().take();
            if let Some(callback) = callback {
                callback();
            }
            Ok(n(1234))
        }
    }
    let dir = Dir::new();
    let original = dir.file("s-78.jsonl");
    let bytes = legacy("y");
    fs::write(&original, &bytes).unwrap();
    let clock = Arc::new(Clock {
        callback: Mutex::new(None),
    });
    let repo = JsonlSessionRepo::new(
        &dir.0,
        SessionCodecOptions::default(),
        ProviderHeaderPolicy::default(),
        Arc::new(Ids::default()),
        clock.clone(),
        Arc::new(pi_telemetry::NoopTelemetryContext),
        JsonlOptions::default(),
    )
    .unwrap();
    let nested = repo.clone();
    let inner_published = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let observed = inner_published.clone();
    *clock.callback.lock().unwrap() = Some(Box::new(move || {
        observed.store(ready(nested.create(opts("y"))).is_ok(), Ordering::SeqCst);
    }));
    assert!(ready(repo.create(opts("x"))).is_err());
    assert!(
        !inner_published.load(Ordering::SeqCst),
        "occupied x destination must never hide original y header"
    );
    assert_eq!(fs::read(&original).unwrap(), bytes);
    assert!(!dir.file("s-79.jsonl").exists());
    let listed = ready(repo.list(())).unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].base.id, sid("y"));
    assert!(ready(repo.create(opts("y"))).is_err());
    *clock.callback.lock().unwrap() = None;
    ready(repo.create(opts("unrelated"))).unwrap();
    assert_eq!(ready(repo.list(())).unwrap().len(), 2);
}
