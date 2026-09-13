use super::*;
use std::sync::atomic::{AtomicBool, Ordering};
#[derive(Clone, Copy)]
pub(super) enum Point {
    BeforeTemp,
    BeforeRename,
    AfterRename,
    Rollback,
}
#[derive(Default)]
pub(super) struct Hooks {
    pub before_temp: AtomicBool,
    pub before_rename: AtomicBool,
    pub after_rename: AtomicBool,
    pub rollback: AtomicBool,
    pub append_prefix: Mutex<Option<usize>>,
}
impl Hooks {
    pub fn check(&self, point: Point) -> Result<(), StorageError> {
        let enabled = match point {
            Point::BeforeTemp => &self.before_temp,
            Point::BeforeRename => &self.before_rename,
            Point::AfterRename => &self.after_rename,
            Point::Rollback => &self.rollback,
        };
        if enabled.swap(false, Ordering::SeqCst) {
            Err(io_failure())
        } else {
            Ok(())
        }
    }
}

use std::fs;
use std::sync::atomic::AtomicU64;
fn n(value: u64) -> SafeUnsigned {
    SafeUnsigned::new(value).unwrap()
}
fn ready<T>(future: impl std::future::Future<Output = T>) -> T {
    immediate(future).expect("JSONL must finish first poll")
}
fn codec() -> Arc<SessionCodec> {
    Arc::new(
        SessionCodec::new(
            SessionCodecOptions::default(),
            ProviderHeaderPolicy::default(),
        )
        .unwrap(),
    )
}
fn options() -> JsonlOptions {
    JsonlOptions {
        reclaim_threshold: FiniteF64::new(1.0).unwrap(),
    }
}
fn metadata() -> SessionMetadata {
    serde_json::from_value(
        serde_json::json!({"id":"jsonl-test","createdAt":1,"storageVersion":1,"storeGeneration":0}),
    )
    .unwrap()
}
struct Dir(std::path::PathBuf);
impl Dir {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "pi-jsonl-unit-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn path(&self) -> PathBuf {
        self.0.join("session.jsonl")
    }
}
impl Drop for Dir {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}
fn create(dir: &Dir) -> Arc<JsonlStorage> {
    JsonlStorage::create(
        dir.path(),
        metadata(),
        codec(),
        Arc::new(FixedClock(n(1234))),
        options(),
    )
    .unwrap()
}
fn open(dir: &Dir) -> Result<Arc<JsonlStorage>, SessionError> {
    JsonlStorage::open(
        dir.path(),
        codec(),
        Arc::new(FixedClock(n(1234))),
        options(),
    )
}
fn tx(writes: Vec<Write>) -> Transaction {
    Transaction { writes }
}
fn set(key: &str, value: serde_json::Value) -> Write {
    Write::SetRegister(Box::new(RegisterSet::FactCustom {
        key: key.into(),
        value: serde_json::from_value(value).unwrap(),
    }))
}
fn delete(key: &str) -> Write {
    Write::DeleteRegister(RegisterDelete {
        namespace: RegisterNamespace::FactCustom,
        key: key.into(),
    })
}
fn custom(id: &str, parent: Option<&str>) -> Write {
    Write::Entry(NewEntry::Custom(NewCustomEntry {
        id: EntryId::new(id).unwrap(),
        parent_id: parent.map(|p| EntryId::new(p).unwrap()),
        entry_type: CustomEntryType::Custom,
        custom_type: CustomType::new("test").unwrap(),
        data: Presence::Missing,
    }))
}
fn request(writes: Vec<Write>) -> ConditionalCommit {
    ConditionalCommit {
        conditions: vec![],
        transaction: tx(writes),
    }
}
fn hooks(storage: &JsonlStorage) -> Arc<Hooks> {
    storage
        .live
        .lock()
        .unwrap()
        .file
        .as_ref()
        .unwrap()
        .hooks
        .clone()
}
#[test]
fn rejected_callback_close_rechecks_live_incarnation() {
    struct Close(Arc<JsonlStorage>);
    impl PublicationValidator for Close {
        type Error = ();
        fn validate(&self, _: &StorageSnapshot) -> Result<(), ()> {
            ready(self.0.close()).unwrap();
            Err(())
        }
    }
    let dir = Dir::new();
    let storage = create(&dir);
    let before = fs::read(dir.path()).unwrap();
    assert!(matches!(
        ready(storage.commit_checked(
            request(vec![set("k", serde_json::json!(1))]),
            &Close(storage.clone())
        )),
        Err(CheckedCommitError::Storage(StorageError::Closed(_)))
    ));
    assert_eq!(fs::read(dir.path()).unwrap(), before);
}

fn source_header() -> Vec<u8> {
    b"{\"v\":4,\"kind\":\"header\",\"id\":\"jsonl-test\",\"createdAt\":1,\"storageVersion\":1}\n"
        .to_vec()
}
fn frame(seq: u64, key: &str, value: serde_json::Value) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({"kind":"register","op":"set","namespace":"fact.custom","key":key,"seq":seq,"value":value})).unwrap()
}
fn snapshot(storage: &JsonlStorage) -> StorageSnapshot {
    ready(storage.snapshot()).unwrap()
}
#[test]
fn original_ordered_frames_and_first_poll_publication() {
    let dir = Dir::new();
    let storage = create(&dir);
    let future = storage.commit(tx(vec![
        set("x", serde_json::json!("line\n雪")),
        set("x", serde_json::json!(2)),
        delete("x"),
    ]));
    let before = fs::read(dir.path()).unwrap();
    drop(future);
    assert_eq!(fs::read(dir.path()).unwrap(), before);
    let result = ready(storage.commit(tx(vec![
        set("x", serde_json::json!("line\n雪")),
        set("x", serde_json::json!(2)),
        delete("x"),
    ])))
    .unwrap();
    assert_eq!(result.seqs, vec![n(0), n(1), n(2)]);
    ready(storage.commit(tx(vec![]))).unwrap();
    let bytes = fs::read(dir.path()).unwrap();
    let lines: Vec<_> = bytes.split(|b| *b == b'\n').collect();
    assert_eq!(lines.len(), 4);
    let row: serde_json::Value = serde_json::from_slice(lines[1]).unwrap();
    assert_eq!(row.as_array().unwrap().len(), 3);
    assert_eq!(row[0]["value"], "line\n雪");
    assert_eq!(row[2]["op"], "delete");
    assert!(row[0].get("register").is_none());
    assert_eq!(lines[2], b"[]");
    let expected = snapshot(&storage);
    ready(storage.close()).unwrap();
    let reopened = open(&dir).unwrap();
    assert_eq!(snapshot(&reopened), expected);
}
#[test]
fn final_valid_no_lf_gets_separator_and_torn_array_is_discarded_whole() {
    let dir = Dir::new();
    let mut base = source_header();
    base.extend(frame(0, "base", serde_json::json!(1)));
    fs::write(dir.path(), &base).unwrap();
    let storage = open(&dir).unwrap();
    ready(storage.commit(tx(vec![set("second", serde_json::json!(2))]))).unwrap();
    ready(storage.close()).unwrap();
    let committed = fs::read(dir.path()).unwrap();
    assert_eq!(
        format::replay(&committed).unwrap().snapshot.registers.len(),
        2
    );
    let array = serde_json::to_vec(&serde_json::json!([
        serde_json::from_slice::<serde_json::Value>(&frame(2, "a", serde_json::json!("雪abc")))
            .unwrap(),
        serde_json::from_slice::<serde_json::Value>(&frame(3, "b", serde_json::json!(2))).unwrap()
    ]))
    .unwrap();
    let utf = array.windows(3).position(|w| w == "雪".as_bytes()).unwrap();
    for cut in [1, 8, utf + 1, utf + 2, array.len() - 1] {
        let mut torn = committed.clone();
        torn.extend(&array[..cut]);
        fs::write(dir.path(), &torn).unwrap();
        let storage = open(&dir).unwrap();
        assert_eq!(snapshot(&storage).registers.len(), 2, "cut {cut}");
        assert_eq!(fs::read(dir.path()).unwrap(), torn);
        ready(storage.commit(tx(vec![]))).unwrap();
        ready(storage.close()).unwrap();
        let repaired = fs::read(dir.path()).unwrap();
        assert_eq!(&repaired[..committed.len()], &committed);
        assert_eq!(&repaired[committed.len()..], b"[]\n");
    }
}
#[test]
fn only_incomplete_unterminated_tail_is_tolerated() {
    let dir = Dir::new();
    for suffix in [
        b"{bad}".as_slice(),
        b"{\"kind\":\"entry\"}",
        b"{\n",
        b"\n[]\n",
        b"[}\n",
        b"true",
        b"{}\n[]\n",
        b"\xff",
    ] {
        let mut bytes = source_header();
        bytes.extend(suffix);
        fs::write(dir.path(), bytes).unwrap();
        assert!(open(&dir).is_err(), "suffix {suffix:?}");
    }
    for bytes in [b"{\"v\":4".to_vec(), b"{\"v\":4}\n".to_vec()] {
        fs::write(dir.path(), bytes).unwrap();
        assert!(open(&dir).is_err());
    }
    let mut bytes = source_header();
    bytes.extend(b"  \r\n\t");
    fs::write(dir.path(), bytes).unwrap();
    let storage = open(&dir).unwrap();
    assert_eq!(snapshot(&storage).next_seq, Some(n(0)));
    ready(storage.close()).unwrap();
    let mut header: serde_json::Value = serde_json::from_slice(&source_header()).unwrap();
    header["snapshotItems"] = serde_json::json!(1);
    header["nextSeq"] = serde_json::json!(1);
    let mut bytes = serde_json::to_vec(&header).unwrap();
    bytes.push(b'\n');
    bytes.extend(b"{");
    fs::write(dir.path(), bytes).unwrap();
    assert!(open(&dir).is_err());
}
#[test]
fn corrupt_overwritten_history_and_prefix_envelopes_cannot_disappear() {
    let dir = Dir::new();
    for second in [0, 1] {
        let mut bytes = source_header();
        bytes.extend(frame(1, "k", serde_json::json!(1)));
        bytes.push(b'\n');
        bytes.extend(frame(second, "k", serde_json::json!(2)));
        fs::write(dir.path(), bytes).unwrap();
        assert!(open(&dir).is_err());
    }
    let mut header: serde_json::Value = serde_json::from_slice(&source_header()).unwrap();
    header["snapshotItems"] = serde_json::json!(1);
    header["nextSeq"] = serde_json::json!(2);
    let mut bytes = serde_json::to_vec(&header).unwrap();
    bytes.push(b'\n');
    bytes.extend(b"[");
    bytes.extend(frame(0, "a", serde_json::json!(0)));
    bytes.push(b',');
    bytes.extend(frame(1, "b", serde_json::json!(1)));
    bytes.extend(b"]\n");
    fs::write(dir.path(), bytes).unwrap();
    assert!(open(&dir).is_err());
    header["snapshotItems"] = serde_json::json!(2);
    let mut bytes = serde_json::to_vec(&header).unwrap();
    bytes.push(b'\n');
    bytes.extend(frame(0, "k", serde_json::json!(0)));
    bytes.push(b'\n');
    bytes.extend(frame(1, "k", serde_json::json!(1)));
    bytes.push(b'\n');
    fs::write(dir.path(), bytes).unwrap();
    assert!(open(&dir).is_err());
}
#[test]
fn compaction_preserves_deleted_highest_and_exhaustion_and_hands_off_descriptor() {
    let dir = Dir::new();
    let storage = create(&dir);
    ready(storage.commit(tx(vec![
        custom("e", None),
        set("k", serde_json::json!(1)),
        delete("k"),
    ])))
    .unwrap();
    let before = snapshot(&storage);
    ready(storage.compact()).unwrap();
    assert_eq!(snapshot(&storage), before);
    ready(storage.commit(tx(vec![set("after", serde_json::json!(1))]))).unwrap();
    ready(storage.close()).unwrap();
    let storage = open(&dir).unwrap();
    assert_eq!(snapshot(&storage).next_seq, Some(n(4)));
    ready(storage.close()).unwrap();
    let mut bytes = source_header();
    let max = 9_007_199_254_740_991;
    let mut row: serde_json::Value =
        serde_json::from_slice(&frame(max, "gone", serde_json::json!(0))).unwrap();
    row["op"] = serde_json::json!("delete");
    row.as_object_mut().unwrap().remove("value");
    bytes.extend(serde_json::to_vec(&row).unwrap());
    bytes.push(b'\n');
    fs::write(dir.path(), bytes).unwrap();
    let storage = open(&dir).unwrap();
    assert_eq!(snapshot(&storage).next_seq, None);
    ready(storage.compact()).unwrap();
    ready(storage.close()).unwrap();
    let storage = open(&dir).unwrap();
    assert_eq!(snapshot(&storage).next_seq, None);
    assert!(ready(storage.commit(tx(vec![]))).is_err());
}
#[test]
fn append_failure_restores_physical_boundary_or_seals_uncertainty() {
    let dir = Dir::new();
    let storage = create(&dir);
    ready(storage.commit(tx(vec![set("old", serde_json::json!(1))]))).unwrap();
    let before = fs::read(dir.path()).unwrap();
    let state = snapshot(&storage);
    let h = hooks(&storage);
    *h.append_prefix.lock().unwrap() = Some(8);
    assert!(matches!(
        ready(storage.commit(tx(vec![set("failed", serde_json::json!(2))]))),
        Err(StorageError::Io(_))
    ));
    assert_eq!(fs::read(dir.path()).unwrap(), before);
    assert_eq!(snapshot(&storage), state);
    *h.append_prefix.lock().unwrap() = Some(usize::MAX);
    h.rollback.store(true, Ordering::SeqCst);
    assert!(ready(storage.commit(tx(vec![set("unacknowledged", serde_json::json!(3))]))).is_err());
    assert!(matches!(
        ready(storage.snapshot()),
        Err(StorageError::Closed(_))
    ));
    assert!(ready(storage.commit(tx(vec![]))).is_err());
    let reopened = open(&dir).unwrap();
    assert_eq!(snapshot(&reopened).registers.len(), 2);
}
#[test]
fn replacement_failures_preserve_old_but_after_rename_is_success() {
    let dir = Dir::new();
    let storage = create(&dir);
    ready(storage.commit(tx(vec![
        set("k", serde_json::json!(1)),
        set("k", serde_json::json!(2)),
    ])))
    .unwrap();
    let before = fs::read(dir.path()).unwrap();
    let state = snapshot(&storage);
    let h = hooks(&storage);
    for point in [&h.before_temp, &h.before_rename] {
        point.store(true, Ordering::SeqCst);
        assert!(ready(storage.compact()).is_err());
        assert_eq!(fs::read(dir.path()).unwrap(), before);
        assert_eq!(snapshot(&storage), state);
        assert_eq!(fs::read_dir(&dir.0).unwrap().count(), 1);
    }
    h.after_rename.store(true, Ordering::SeqCst);
    ready(storage.compact()).unwrap();
    assert_eq!(snapshot(&storage), state);
    assert_ne!(fs::read(dir.path()).unwrap(), before);
    ready(storage.commit(tx(vec![set("fresh", serde_json::json!(3))]))).unwrap();
    ready(storage.close()).unwrap();
    assert_eq!(snapshot(&open(&dir).unwrap()).registers.len(), 2);
}

#[test]
fn revision_retry_and_clock_reentry_preserve_both_publications() {
    struct Reenter {
        store: Arc<JsonlStorage>,
        once: AtomicBool,
    }
    impl PublicationValidator for Reenter {
        type Error = ();
        fn validate(&self, stage: &StorageSnapshot) -> Result<(), ()> {
            if !self.once.swap(true, Ordering::SeqCst) {
                ready(
                    self.store
                        .commit(tx(vec![set("inner", serde_json::json!(1))])),
                )
                .unwrap();
                return Err(());
            }
            assert!(stage.registers.len() >= 2);
            Ok(())
        }
    }
    let dir = Dir::new();
    let storage = create(&dir);
    let validator = Reenter {
        store: storage.clone(),
        once: AtomicBool::new(false),
    };
    assert!(matches!(
        ready(storage.commit_checked(
            request(vec![set("outer", serde_json::json!(2))]),
            &validator
        )),
        Ok(ConditionalOutcome::Applied(_))
    ));
    assert_eq!(snapshot(&storage).next_seq, Some(n(2)));
    struct Reclock(Mutex<Option<Arc<JsonlStorage>>>);
    impl StorageClock for Reclock {
        fn now(&self) -> Result<SafeUnsigned, StorageError> {
            let store = self.0.lock().unwrap().take();
            if let Some(store) = store {
                ready(store.commit(tx(vec![set("clock", serde_json::json!(1))])))?;
            }
            Ok(n(22))
        }
    }
    ready(storage.close()).unwrap();
    let clock = Arc::new(Reclock(Mutex::new(None)));
    let storage = JsonlStorage::open(dir.path(), codec(), clock.clone(), options()).unwrap();
    *clock.0.lock().unwrap() = Some(storage.clone());
    ready(storage.commit(tx(vec![custom("e", None)]))).unwrap();
    assert_eq!(snapshot(&storage).next_seq, Some(n(4)));
    assert_eq!(
        serde_json::to_value(&snapshot(&storage).entries[0]).unwrap()["timestamp"],
        serde_json::json!(22)
    );
}
#[test]
fn rejected_or_mismatched_commit_preserves_torn_source_bytes() {
    struct Reject;
    impl PublicationValidator for Reject {
        type Error = ();
        fn validate(&self, _: &StorageSnapshot) -> Result<(), ()> {
            Err(())
        }
    }
    let dir = Dir::new();
    let mut bytes = source_header();
    bytes.extend(b"[{");
    fs::write(dir.path(), &bytes).unwrap();
    let storage = open(&dir).unwrap();
    assert!(matches!(
        ready(storage.commit_checked(request(vec![set("x", serde_json::json!(1))]), &Reject)),
        Err(CheckedCommitError::Rejected(()))
    ));
    let req = ConditionalCommit {
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
        ready(storage.commit_conditional(req)).unwrap(),
        ConditionalOutcome::NotApplied
    );
    assert!(ready(storage.commit(tx(vec![custom("bad", Some("missing"))]))).is_err());
    assert_eq!(fs::read(dir.path()).unwrap(), bytes);
    assert_eq!(snapshot(&storage).next_seq, Some(n(0)));
}
#[cfg(unix)]
#[test]
fn path_claims_symlinks_hardlinks_and_stale_target_are_checked() {
    use std::os::unix::fs::symlink;
    let dir = Dir::new();
    let storage = create(&dir);
    let alias = dir.0.join("alias");
    symlink(dir.path(), &alias).unwrap();
    assert!(JsonlStorage::open(&alias, codec(), Arc::new(FixedClock(n(0))), options()).is_err());
    let hard = dir.0.join("hard");
    fs::hard_link(dir.path(), &hard).unwrap();
    assert!(ready(storage.commit(tx(vec![]))).is_err());
    assert!(JsonlStorage::open(&hard, codec(), Arc::new(FixedClock(n(0))), options()).is_err());
    fs::remove_file(hard).unwrap();
    ready(storage.close()).unwrap();
    let new = open(&dir).unwrap();
    ready(storage.close()).unwrap();
    assert!(open(&dir).is_err());
    ready(new.commit(tx(vec![]))).unwrap();
    let replaced = source_header();
    let other = dir.0.join("other");
    fs::write(&other, &replaced).unwrap();
    fs::rename(other, dir.path()).unwrap();
    assert!(ready(new.commit(tx(vec![set("stale", serde_json::json!(1))]))).is_err());
    assert!(ready(new.compact()).is_err());
    assert_eq!(fs::read(dir.path()).unwrap(), replaced);
}
#[test]
fn globally_merged_prefix_and_open_reclamation_preserve_authority() {
    let dir = Dir::new();
    let storage = create(&dir);
    let usage = Write::Usage(NewUsageRow {
        id: UsageId::new("u").unwrap(),
        usage: Usage::zero(),
        entry_id: Some(EntryId::new("e").unwrap()),
        adjustment: true,
        details: Presence::Missing,
    });
    ready(storage.commit(tx(vec![
        custom("e", None),
        set("k", serde_json::json!(0)),
        usage,
        custom("child", Some("e")),
    ])))
    .unwrap();
    ready(storage.compact()).unwrap();
    let bytes = fs::read(dir.path()).unwrap();
    let lines: Vec<serde_json::Value> = bytes
        .split(|b| *b == b'\n')
        .filter(|l| !l.is_empty())
        .map(|l| serde_json::from_slice(l).unwrap())
        .collect();
    assert_eq!(
        lines
            .iter()
            .skip(1)
            .map(|l| l["seq"].as_u64().unwrap())
            .collect::<Vec<_>>(),
        vec![0, 1, 2, 3]
    );
    assert_eq!(
        lines
            .iter()
            .skip(1)
            .map(|l| l["kind"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec!["entry", "register", "usage", "entry"]
    );
    for i in 0..30 {
        ready(storage.commit(tx(vec![set(
            "k",
            serde_json::json!({"value":i,"padding":"long discarded payload"}),
        )])))
        .unwrap();
    }
    let expected = snapshot(&storage);
    ready(storage.close()).unwrap();
    let before = fs::metadata(dir.path()).unwrap().len();
    let storage = JsonlStorage::open(
        dir.path(),
        codec(),
        Arc::new(FixedClock(n(0))),
        JsonlOptions::default(),
    )
    .unwrap();
    assert_eq!(snapshot(&storage), expected);
    assert!(fs::metadata(dir.path()).unwrap().len() < before / 2);
}
#[test]
fn latest_register_codec_is_strict_but_superseded_value_is_raw() {
    let dir = Dir::new();
    let mut bytes = source_header();
    let row = serde_json::json!({"kind":"register","op":"set","namespace":"lane.leaf","key":"main","seq":0,"value":{"historical":true}});
    bytes.extend(serde_json::to_vec(&row).unwrap());
    bytes.push(b'\n');
    fs::write(dir.path(), &bytes).unwrap();
    assert!(open(&dir).is_err());
    let mut replacement = row.clone();
    replacement["seq"] = serde_json::json!(1);
    replacement["value"] = serde_json::Value::Null;
    bytes.extend(serde_json::to_vec(&replacement).unwrap());
    bytes.push(b'\n');
    fs::write(dir.path(), &bytes).unwrap();
    ready(open(&dir).unwrap().close()).unwrap();
    let mut bad = row;
    bad["value"] = serde_json::json!("missing");
    let mut bytes = source_header();
    bytes.extend(serde_json::to_vec(&bad).unwrap());
    bytes.push(b'\n');
    bytes.extend(serde_json::to_vec(&replacement).unwrap());
    fs::write(dir.path(), bytes).unwrap();
    assert!(open(&dir).is_err());
    let mut header: serde_json::Value = serde_json::from_slice(&source_header()).unwrap();
    header["storageVersion"] = serde_json::json!(2);
    let mut bytes = serde_json::to_vec(&header).unwrap();
    bytes.extend(b"\ninvalid current payload");
    fs::write(dir.path(), bytes).unwrap();
    assert!(matches!(
        open(&dir),
        Err(SessionError::NewerStorageVersion(_))
    ));
}

#[test]
fn actual_raw_rewrite_seam_preserves_envelopes_and_publishes_version_together() {
    let dir = Dir::new();
    let storage = create(&dir);
    ready(storage.commit(tx(vec![
        custom("e", None),
        set("k", serde_json::json!({"old":1})),
        delete("absent"),
    ])))
    .unwrap();
    let expected = snapshot(&storage);
    ready(storage.close()).unwrap();
    let mut claim = FileClaim::acquire(&dir.path()).unwrap();
    assert_eq!(claim.read_header().unwrap().metadata.storage_version, n(1));
    let raw: RawSnapshot = claim.read_raw().unwrap();
    let before = claim.read_source().unwrap();
    assert!(claim.rewrite_registers(n(1), n(2), vec![]).is_err());
    let mut wrong = raw.registers.clone();
    wrong[0].seq = n(0);
    assert!(claim.rewrite_registers(n(1), n(2), wrong).is_err());
    assert_eq!(claim.read_source().unwrap(), before);
    let mapped = vec![RawRegisterRow {
        value: serde_json::from_value(serde_json::json!({"new":2})).unwrap(),
        ..raw.registers[0].clone()
    }];
    claim.hooks.before_rename.store(true, Ordering::SeqCst);
    assert!(claim.rewrite_registers(n(1), n(2), mapped.clone()).is_err());
    assert_eq!(claim.read_source().unwrap(), before);
    claim.rewrite_registers(n(1), n(2), mapped).unwrap();
    let after = claim.read_raw().unwrap();
    assert_eq!(after.metadata.storage_version, n(2));
    assert_eq!(after.entries, expected.entries);
    assert_eq!(after.next_seq, expected.next_seq);
    assert_eq!(after.stats, expected.stats);
    assert_eq!(after.registers[0].seq, n(1));
    assert_eq!(
        serde_json::to_value(&after.registers[0].value).unwrap(),
        serde_json::json!({"new":2})
    );
    assert!(open(&dir).is_err());
    drop(claim);
    assert!(matches!(
        open(&dir),
        Err(SessionError::NewerStorageVersion(_))
    ));
}
#[test]
fn generic_migration_engine_uses_actual_owned_file_publication() {
    use crate::migration::*;
    // This probe adapter only transfers the real owned path claim; all read and
    // publication work executes the production FileClaim methods, with real files.
    struct Store(Arc<Mutex<Option<FileClaim>>>);
    struct Lease {
        claim: Option<FileClaim>,
        owner: Arc<Mutex<Option<FileClaim>>>,
    }
    impl Drop for Lease {
        fn drop(&mut self) {
            *self.owner.lock().unwrap() = self.claim.take();
        }
    }
    impl MigrationStore for Store {
        type Value = JsonValue;
        type Lease = Lease;
        fn acquire(&self) -> BoxFuture<'_, Result<Lease, StorageError>> {
            Box::pin(async {
                let claim = self
                    .0
                    .lock()
                    .unwrap()
                    .take()
                    .ok_or_else(|| conflict("probe already leased"))?;
                Ok(Lease {
                    claim: Some(claim),
                    owner: self.0.clone(),
                })
            })
        }
    }
    impl MigrationLease for Lease {
        type Value = JsonValue;
        fn read(&mut self) -> BoxFuture<'_, Result<MigrationSnapshot<JsonValue>, StorageError>> {
            Box::pin(async {
                let raw = self.claim.as_mut().unwrap().read_raw()?;
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
            p: MigrationPublication<JsonValue>,
        ) -> BoxFuture<'_, Result<(), StorageError>> {
            Box::pin(async move {
                let (v, rows) = p.into_parts();
                self.claim.as_mut().unwrap().rewrite_registers(
                    v.from,
                    v.to,
                    rows.into_iter()
                        .map(|r| RawRegisterRow {
                            key: r.key,
                            seq: r.seq,
                            value: r.value,
                        })
                        .collect(),
                )
            })
        }
    }
    struct Map;
    impl RegisterMigration<JsonValue> for Map {
        fn convert(
            &self,
            _: &MigrationRegister<JsonValue>,
        ) -> Result<JsonValue, MigrationConversionError> {
            Ok(serde_json::from_value(serde_json::json!({"mapped":true})).unwrap())
        }
    }
    let dir = Dir::new();
    let storage = create(&dir);
    ready(storage.commit(tx(vec![set("k", serde_json::json!(0))]))).unwrap();
    ready(storage.close()).unwrap();
    let store = Store(Arc::new(Mutex::new(Some(
        FileClaim::acquire(&dir.path()).unwrap(),
    ))));
    let registry = MigrationRegistry::new(
        n(2),
        vec![MigrationStep {
            versions: MigrationVersions {
                from: n(1),
                to: n(2),
            },
            converter: Arc::new(Map),
        }],
    )
    .unwrap();
    let result = ready(registry.migrate(&store)).unwrap();
    assert_eq!(result.current_version, n(2));
    let mut claim = store.0.lock().unwrap().take().unwrap();
    assert_eq!(claim.read_header().unwrap().metadata.storage_version, n(2));
    assert_eq!(
        serde_json::to_value(&claim.read_raw().unwrap().registers[0].value).unwrap(),
        serde_json::json!({"mapped":true})
    );
    assert!(open(&dir).is_err());
}
fn deferred(dir: &Dir) -> (Arc<JsonlStorage>, Vec<u8>, StorageSnapshot) {
    let backing = Arc::new(MemoryBacking::new());
    let view = backing.open(Arc::new(FixedClock(n(10)))).unwrap();
    ready(view.commit(tx(vec![
        custom("normalized", None),
        set("pre", serde_json::json!(1)),
        Write::Usage(NewUsageRow {
            id: UsageId::new("virtual").unwrap(),
            usage: Usage::zero(),
            entry_id: None,
            adjustment: true,
            details: Presence::Missing,
        }),
    ])))
    .unwrap();
    let snapshot = ready(view.snapshot()).unwrap();
    let source = b"{\"id\":\"jsonl-test\",\"version\":3}\nopaque legacy source\n".to_vec();
    fs::write(dir.path(), &source).unwrap();
    let claim = FileClaim::acquire(&dir.path()).unwrap();
    let storage = claim
        .adopt_deferred(
            metadata(),
            snapshot.clone(),
            source.clone(),
            codec(),
            Arc::new(FixedClock(n(20))),
        )
        .unwrap();
    (storage, source, snapshot)
}
#[test]
fn deferred_binding_writes_pre_snapshot_and_original_first_frame_exactly_once() {
    for writes in [
        vec![],
        vec![delete("pre")],
        vec![set("pre", serde_json::json!(2)), delete("pre")],
    ] {
        let dir = Dir::new();
        let (storage, source, pre) = deferred(&dir);
        assert_eq!(fs::read(dir.path()).unwrap(), source);
        assert_eq!(snapshot(&storage), pre);
        let result = ready(storage.commit(tx(writes.clone()))).unwrap();
        let bytes = fs::read(dir.path()).unwrap();
        let expected_prefix =
            format::compact(&RawSnapshot::from_current(metadata(), pre).unwrap()).unwrap();
        assert!(bytes.starts_with(&expected_prefix));
        assert_eq!(
            &bytes[expected_prefix.len()..],
            format::commit_frame(tx(writes), &result).unwrap()
        );
        assert_eq!(snapshot(&storage).usage.len(), 1);
        let expected = snapshot(&storage);
        ready(storage.close()).unwrap();
        assert_eq!(snapshot(&open(&dir).unwrap()), expected);
    }
}
#[test]
fn deferred_failure_and_source_mismatch_remain_unconverted() {
    let dir = Dir::new();
    let (storage, source, pre) = deferred(&dir);
    let h = hooks(&storage);
    assert!(ready(storage.commit(tx(vec![custom("bad", Some("missing"))]))).is_err());
    assert_eq!(fs::read(dir.path()).unwrap(), source);
    for point in [&h.before_temp, &h.before_rename] {
        point.store(true, Ordering::SeqCst);
        assert!(ready(storage.commit(tx(vec![delete("pre")]))).is_err());
        assert_eq!(fs::read(dir.path()).unwrap(), source);
        assert_eq!(snapshot(&storage), pre);
        assert!(matches!(
            storage.live.lock().unwrap().mode,
            Mode::Deferred { .. }
        ));
    }
    let changed = [source.as_slice(), b"changed"].concat();
    fs::write(dir.path(), &changed).unwrap();
    assert!(ready(storage.commit(tx(vec![]))).is_err());
    assert_eq!(fs::read(dir.path()).unwrap(), changed);
    ready(storage.close()).unwrap();
    let claim = FileClaim::acquire(&dir.path()).unwrap();
    assert!(
        claim
            .adopt_deferred(
                metadata(),
                pre.clone(),
                source,
                codec(),
                Arc::new(FixedClock(n(0)))
            )
            .is_err()
    );
    let claim = FileClaim::acquire(&dir.path()).unwrap();
    let mut wrong = metadata();
    wrong.id = SessionId::new("other").unwrap();
    assert!(
        claim
            .adopt_deferred(wrong, pre, changed, codec(), Arc::new(FixedClock(n(0))))
            .is_err()
    );
}
#[test]
fn private_claim_to_generic_runtime_and_fresh_snapshot_import_compile_and_run() {
    struct Ids;
    impl IdGenerator for Ids {
        fn next(&self, _: Option<SafeUnsigned>) -> String {
            "00000000-0001-7000-8000-000000000001".into()
        }
    }
    let dir = Dir::new();
    let (storage, _, _) = deferred(&dir);
    let runtime = ready(crate::session::RuntimeSession::new(
        storage.clone(),
        metadata(),
        codec(),
        Arc::new(Ids),
        Arc::new(pi_telemetry::NoopTelemetryContext),
    ))
    .unwrap();
    drop(runtime);
    ready(storage.commit(tx(vec![]))).unwrap();
    let expected = snapshot(&storage);
    ready(storage.close()).unwrap();
    let claim = FileClaim::acquire(&dir.path()).unwrap();
    let reopened = claim
        .into_storage(codec(), Arc::new(FixedClock(n(0))), options())
        .unwrap();
    assert_eq!(snapshot(&reopened), expected);
    let fork = Dir::new();
    let imported = JsonlStorage::create_from_snapshot(
        fork.path(),
        metadata(),
        expected.clone(),
        codec(),
        Arc::new(FixedClock(n(0))),
        options(),
    )
    .unwrap();
    assert_eq!(snapshot(&imported), expected);
    ready(imported.compact()).unwrap();
    ready(imported.close()).unwrap();
    assert_eq!(snapshot(&open(&fork).unwrap()), expected);
}

#[test]
fn literal_source_history_fixture_replays_and_compacts_without_lost_tokens() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../conformance/session/jsonl/physical-contract.json"
    ))
    .unwrap();
    assert!(
        fixture["authority"]
            .as_str()
            .unwrap()
            .starts_with("853a80d")
    );
    let dir = Dir::new();
    let mut bytes = source_header();
    bytes.extend(serde_json::to_vec(&fixture["orderedRegisterHistory"]).unwrap());
    bytes.push(b'\n');
    fs::write(dir.path(), bytes).unwrap();
    let storage = open(&dir).unwrap();
    assert_eq!(
        snapshot(&storage).registers.len() as u64,
        fixture["afterHistory"]["liveRegisters"].as_u64().unwrap()
    );
    ready(storage.compact()).unwrap();
    ready(storage.close()).unwrap();
    assert_eq!(
        snapshot(&open(&dir).unwrap()).next_seq.unwrap().get(),
        fixture["afterHistory"]["nextSeq"].as_u64().unwrap()
    );
}
#[test]
fn immutable_history_validates_forward_parent_usage_and_duplicate_ids() {
    let dir = Dir::new();
    let bad_rows = [
        serde_json::json!({"kind":"entry","id":"e","parentId":"later","type":"custom","customType":"x","seq":0,"timestamp":1}),
        serde_json::json!({"kind":"usage","id":"u","seq":0,"entryId":"later","adjustment":true,"usage":Usage::zero()}),
    ];
    for bad in bad_rows {
        let mut bytes = source_header();
        bytes.extend(serde_json::to_vec(&bad).unwrap());
        bytes.push(b'\n');
        fs::write(dir.path(), bytes).unwrap();
        assert!(open(&dir).is_err());
    }
    let mut bytes = source_header();
    let row = serde_json::json!({"kind":"entry","id":"e","parentId":null,"type":"custom","customType":"x","seq":0,"timestamp":1});
    let mut duplicate = row.clone();
    duplicate["seq"] = serde_json::json!(1);
    bytes.extend(serde_json::to_vec(&vec![row, duplicate]).unwrap());
    fs::write(dir.path(), bytes).unwrap();
    assert!(open(&dir).is_err());
}
#[test]
fn policy_classified_headers_round_trip_and_codec_errors_remain_distinct() {
    let dir = Dir::new();
    let storage = create(&dir);
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
        serde_json::json!({"X-Public":"retained"});
    let policy = ProviderHeaderPolicy::from_allowlist(["x-public".into()]);
    let raw: RawRegisterSet = serde_json::from_value(
        serde_json::json!({"namespace":"op.state","key":"classified","value":state}),
    )
    .unwrap();
    ready(storage.commit(tx(vec![Write::SetRegister(Box::new(
        raw.into_durable(&policy).unwrap(),
    ))])))
    .unwrap();
    let expected = snapshot(&storage);
    ready(storage.compact()).unwrap();
    ready(storage.close()).unwrap();
    assert!(open(&dir).is_err());
    let codec = Arc::new(SessionCodec::new(SessionCodecOptions::default(), policy).unwrap());
    let storage =
        JsonlStorage::open(dir.path(), codec, Arc::new(FixedClock(n(0))), options()).unwrap();
    assert_eq!(snapshot(&storage), expected);
}

#[cfg(unix)]
#[test]
fn readonly_file_can_be_claimed_read_and_adopted_without_write_access() {
    use std::os::unix::fs::PermissionsExt;
    let dir = Dir::new();
    fs::write(dir.path(), source_header()).unwrap();
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o400)).unwrap();
    let mut claim = FileClaim::acquire(&dir.path()).expect("reading must not request write access");
    assert_eq!(claim.read_source().unwrap(), source_header());
    let storage = claim
        .into_storage(codec(), Arc::new(FixedClock(n(0))), options())
        .unwrap();
    assert_eq!(snapshot(&storage).next_seq, Some(n(0)));
    assert!(ready(storage.commit(tx(vec![]))).is_err());
    assert_eq!(fs::read(dir.path()).unwrap(), source_header());
    ready(storage.close()).unwrap();
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o600)).unwrap();
    let (storage, source, pre) = deferred(&dir);
    ready(storage.close()).unwrap();
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o400)).unwrap();
    let claim = FileClaim::acquire(&dir.path()).unwrap();
    let storage = claim
        .adopt_deferred(
            metadata(),
            pre,
            source.clone(),
            codec(),
            Arc::new(FixedClock(n(0))),
        )
        .unwrap();
    assert_eq!(fs::read(dir.path()).unwrap(), source);
    assert_eq!(snapshot(&storage).entries.len(), 1);
}
#[cfg(unix)]
#[test]
fn replacement_retains_original_unix_permission_bits() {
    use std::os::unix::fs::PermissionsExt;
    let dir = Dir::new();
    let storage = create(&dir);
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o600)).unwrap();
    ready(storage.compact()).unwrap();
    assert_eq!(
        fs::metadata(dir.path()).unwrap().permissions().mode() & 0o7777,
        0o600
    );
    ready(storage.close()).unwrap();
    let (storage, _, _) = deferred(&dir);
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o640)).unwrap();
    ready(storage.commit(tx(vec![]))).unwrap();
    assert_eq!(
        fs::metadata(dir.path()).unwrap().permissions().mode() & 0o7777,
        0o640
    );
    ready(storage.close()).unwrap();
    let mut claim = FileClaim::acquire(&dir.path()).unwrap();
    let before = claim.read_source().unwrap();
    let temporary = claim.prepare(b"private temporary payload").unwrap();
    let path = fs::read_dir(&dir.0)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| {
            path.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with(".pi-jsonl-")
        })
        .unwrap();
    assert_eq!(
        fs::metadata(path).unwrap().permissions().mode() & 0o7777,
        0o600
    );
    drop(temporary);
    assert_eq!(claim.read_source().unwrap(), before);
    let raw = claim.read_raw().unwrap();
    claim.rewrite_registers(n(1), n(2), raw.registers).unwrap();
    assert_eq!(
        fs::metadata(dir.path()).unwrap().permissions().mode() & 0o7777,
        0o640
    );
}
