use super::*;
thread_local! { static TRACE: std::cell::RefCell<Vec<String>> = const { std::cell::RefCell::new(Vec::new()) }; static PLANS: std::cell::RefCell<Vec<(String, Vec<String>)>> = const { std::cell::RefCell::new(Vec::new()) }; }
thread_local! {static BEGIN_ACK:std::cell::RefCell<Option<std::sync::mpsc::Sender<()>>>=const{std::cell::RefCell::new(None)};}
pub(crate) fn trace(event: rusqlite::trace::TraceEvent<'_>) {
    if let rusqlite::trace::TraceEvent::Stmt(_, sql) = &event
        && *sql == "BEGIN IMMEDIATE"
    {
        BEGIN_ACK.with(|ack| {
            if let Some(send) = ack.borrow_mut().take() {
                let _ = send.send(());
            }
        });
    }
    if let rusqlite::trace::TraceEvent::Stmt(statement, _) = event {
        TRACE.with(|out| {
            out.borrow_mut()
                .push(statement.expanded_sql().unwrap_or_default())
        });
    }
}
pub(crate) fn query(sql: &str, args: &[rusqlite::types::Value], db: &Connection) {
    let plan = db
        .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
        .unwrap()
        .query_map(rusqlite::params_from_iter(args), |r| r.get::<_, String>(3))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    PLANS.with(|out| out.borrow_mut().push((sql.to_owned(), plan)));
}
type PublicationHook = Box<dyn Fn(&Path, bool)>;
thread_local! { static PUBLICATION: std::cell::RefCell<Option<PublicationHook>> = const { std::cell::RefCell::new(None) }; }
pub(crate) fn before_publish(path: &Path) -> Result<(), StorageError> {
    PUBLICATION.with(|f| {
        if let Some(f) = f.borrow().as_ref() {
            f(path, false);
        }
    });
    Ok(())
}
pub(crate) fn after_publish(path: &Path) {
    PUBLICATION.with(|f| {
        if let Some(f) = f.borrow().as_ref() {
            f(path, true);
        }
    });
}

pub(crate) fn before_commit(_path: &Path) -> Result<(), StorageError> {
    Ok(())
}

#[allow(dead_code)]
#[path = "../../../conformance/session/backend.rs"]
mod backend;
use backend::{custom, ready, tx};
struct Clock;
impl StorageClock for Clock {
    fn now(&self) -> Result<SafeUnsigned, StorageError> {
        Ok(n(1234))
    }
}
struct Fixture {
    directory: PathBuf,
    path: PathBuf,
    codec: Arc<SessionCodec>,
    storage: Arc<SqliteStorage>,
}
impl Fixture {
    fn new() -> Self {
        let directory =
            std::env::temp_dir().join(format!("pi-sqlite-unit-{:x?}", token().unwrap().0));
        std::fs::create_dir(&directory).unwrap();
        let path = directory.join("session.db");
        let codec = Arc::new(
            SessionCodec::new(
                SessionCodecOptions::default(),
                ProviderHeaderPolicy::default(),
            )
            .unwrap(),
        );
        let metadata = serde_json::from_value(
            serde_json::json!({"id":"unit","createdAt":1,"storageVersion":1,"storeGeneration":0}),
        )
        .unwrap();
        let storage = SqliteStorage::create(
            &path,
            metadata,
            codec.clone(),
            Arc::new(Clock),
            SqliteOptions::default(),
        )
        .unwrap();
        Self {
            directory,
            path,
            codec,
            storage,
        }
    }
    fn sql(&self, statement: &str) {
        Connection::open(&self.path)
            .unwrap()
            .execute_batch(statement)
            .unwrap();
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = ready(self.storage.close());
        std::fs::remove_dir_all(&self.directory).unwrap();
    }
}
#[test]
fn newer_version_precedes_payload_interpretation() {
    let f = Fixture::new();
    ready(f.storage.commit(tx(vec![custom("a", None)]))).unwrap();
    ready(f.storage.close()).unwrap();
    f.sql("UPDATE session SET storage_version=2; UPDATE entries SET payload='invalid json';");
    assert!(matches!(
        SqliteStorage::read_snapshot(&f.path, &f.codec, SqliteOptions::default()),
        Err(SessionError::NewerStorageVersion(_))
    ));
}
#[test]
fn observed_lease_loss_permanently_seals_owner() {
    let f = Fixture::new();
    f.sql("UPDATE writer_lease SET expires_at_ms=0;");
    assert!(matches!(
        ready(f.storage.stats()),
        Err(StorageError::Closed(_))
    ));
    f.sql("UPDATE writer_lease SET expires_at_ms=9007199254740991;");
    assert!(matches!(
        ready(f.storage.stats()),
        Err(StorageError::Closed(_))
    ));
}
fn branch_fixture(f: &Fixture) -> serde_json::Value {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../conformance/session/sqlite/branch-v2.json"
    ))
    .unwrap();
    for write in fixture["writes"].as_array().unwrap() {
        let id = write["entry"]["id"].as_str().unwrap();
        let db = Connection::open(&f.path).unwrap();
        let before: i64 = db
            .query_row("SELECT count(*) FROM branch_meta", [], |r| r.get(0))
            .unwrap();
        ready(
            f.storage
                .commit(tx(vec![serde_json::from_value(write.clone()).unwrap()])),
        )
        .unwrap();
        if let Some(expected) = fixture["branch_creation_observations"]
            .as_array()
            .unwrap()
            .iter()
            .find(|e| e["append"] == id)
        {
            let (segment, base): (String, i64) = db
                .query_row(
                    "SELECT branch_id,base_seq FROM branch_meta WHERE tip_entry_id=?1",
                    [id],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .unwrap();
            let copied: Vec<String> = db.prepare("SELECT entry_id FROM branch_entries WHERE branch_id=?1 AND entry_id<>?2 ORDER BY entry_seq").unwrap().query_map(params![segment,id],|r|r.get(0)).unwrap().collect::<rusqlite::Result<_>>().unwrap();
            assert_eq!(
                serde_json::json!(copied),
                expected["copied_existing_ids"],
                "{id}"
            );
            assert_eq!(serde_json::json!(base), expected["base_upper_seq"], "{id}");
            let after: i64 = db
                .query_row("SELECT count(*) FROM branch_meta", [], |r| r.get(0))
                .unwrap();
            assert_eq!(after, before + 1);
        }
        if id == "M" {
            let after: i64 = db
                .query_row("SELECT count(*) FROM branch_meta", [], |r| r.get(0))
                .unwrap();
            assert_eq!(before, after, "exact tip extends");
        }
    }
    fixture
}
#[test]
fn pinned_segment_paths_copy_bounds_and_queries() {
    let f = Fixture::new();
    let fixture = branch_fixture(&f);
    for (id, expected) in fixture["full_oldest_paths"].as_object().unwrap() {
        let q =
            serde_json::from_value(serde_json::json!({"start":id,"order":"oldestFirst"})).unwrap();
        assert_eq!(
            serde_json::json!(backend::ids(ready(f.storage.scan_branch(q)).unwrap())),
            *expected,
            "{id}"
        );
    }
    for case in fixture["queries"].as_array().unwrap() {
        let q = serde_json::from_value(case["query"].clone()).unwrap();
        assert_eq!(
            serde_json::json!(backend::ids(ready(f.storage.scan_branch(q)).unwrap())),
            case["ids"],
            "{case}"
        );
    }
}
#[test]
fn real_covering_plans_and_structure_excludes_payload() {
    let f = Fixture::new();
    branch_fixture(&f);
    f.sql("UPDATE entries SET payload='not json';");
    PLANS.with(|p| p.borrow_mut().clear());
    TRACE.with(|p| p.borrow_mut().clear());
    for query in [
        serde_json::json!({"start":"L","stopAtType":"compaction","limit":2}),
        serde_json::json!({"start":"L","type":"custom","limit":2}),
    ] {
        assert_eq!(
            ready(
                f.storage
                    .scan_branch_structure(serde_json::from_value(query).unwrap())
            )
            .unwrap()
            .len(),
            2
        );
    }
    PLANS.with(|plans| {
        let plans = plans.borrow();
        assert!(!plans.is_empty());
        for (sql, plan) in plans.iter() {
            assert!(!sql.contains("payload"));
            assert!(sql.contains("CROSS JOIN"));
            assert!(
                plan.iter()
                    .any(|p| p.contains("SEARCH b USING COVERING INDEX ix_be_")),
                "{plan:?}"
            );
            assert!(
                plan.iter()
                    .any(|p| p.contains("SEARCH e USING PRIMARY KEY")),
                "{plan:?}"
            );
            assert!(
                !plan
                    .iter()
                    .any(|p| p.contains("SCAN e") || p.contains("TEMP B-TREE")),
                "{plan:?}"
            );
        }
    });
    TRACE.with(|trace| {
        let trace = trace.borrow();
        assert!(trace.iter().any(|s| s.contains("checked_revision")));
        assert!(
            !trace.iter().any(|s| s.contains("usage_ledger")
                || s.contains("count(*)")
                || s == "SELECT branch_id FROM branch_meta"),
            "{trace:?}"
        );
    });
}
#[test]
fn filtered_cache_hole_errors_then_closed_explicit_repair_preserves_canonical() {
    let f = Fixture::new();
    branch_fixture(&f);
    let before = ready(f.storage.snapshot()).unwrap();
    f.sql("DELETE FROM branch_entries WHERE entry_id='H';");
    for query in [
        serde_json::json!({"start":"L","type":"custom"}),
        serde_json::json!({"start":"L","customType":"visible","stopAtType":"compaction"}),
        serde_json::json!({"start":"L","cursor":{"seq":11},"limit":1}),
    ] {
        assert!(matches!(
            ready(
                f.storage
                    .scan_branch(serde_json::from_value(query).unwrap())
            ),
            Err(StorageError::Corruption(_))
        ));
    }
    ready(f.storage.close()).unwrap();
    assert!(
        SqliteStorage::open(
            &f.path,
            f.codec.clone(),
            Arc::new(Clock),
            SqliteOptions::default()
        )
        .is_err()
    );
    SqliteStorage::repair_file(&f.path, f.codec.clone(), SqliteOptions::default()).unwrap();
    let reopened = SqliteStorage::open(
        &f.path,
        f.codec.clone(),
        Arc::new(Clock),
        SqliteOptions::default(),
    )
    .unwrap();
    assert_eq!(
        serde_json::to_vec(&before).unwrap(),
        serde_json::to_vec(&ready(reopened.snapshot()).unwrap()).unwrap()
    );
    assert_eq!(
        backend::ids(
            ready(
                reopened.scan_branch(
                    serde_json::from_value(
                        serde_json::json!({"start":"L","stopAtType":"compaction"})
                    )
                    .unwrap()
                )
            )
            .unwrap()
        ),
        ["L", "J", "H", "C2"]
    );
    ready(reopened.close()).unwrap();
}
#[test]
fn overlapping_path_scopes_release_independently() {
    let f = Fixture::new();
    let first = path::Scope::acquire(&f.path, false).unwrap();
    let second = path::Scope::acquire(&f.path, false).unwrap();
    let db = path::connection(&f.path, SqliteOptions::default(), true).unwrap();
    drop(first);
    assert!(matches!(
        path::Scope::acquire(&f.path, true),
        Err(StorageError::Busy(_))
    ));
    drop(db);
    drop(second);
    assert!(path::Scope::acquire(&f.path, true).is_ok());
}
#[derive(Default)]
struct Ids(std::sync::atomic::AtomicU64);
impl IdGenerator for Ids {
    fn next(&self, _: Option<SafeUnsigned>) -> String {
        format!(
            "00000001-0000-7000-8000-{:012x}",
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
        )
    }
}
fn session(f: &Fixture) -> pi_agent_session::session::RuntimeSession<SqliteStorage> {
    ready(pi_agent_session::session::RuntimeSession::new(
        f.storage.clone(),
        f.storage.metadata().unwrap(),
        f.codec.clone(),
        Arc::new(Ids::default()),
        Arc::new(NoopTelemetryContext),
    ))
    .unwrap()
}
fn pending(key: &str) -> Write {
    Write::SetRegister(Box::new(RegisterSet::PendingEntry {
        key: key.into(),
        value: PendingEntry {
            entry_type: PendingEntryType::Custom,
            custom_type: Some(CustomType::new("x").unwrap()),
            payload: Presence::Missing,
        },
    }))
}

#[test]
fn actual_runtime_session_uses_indexed_final_projection_and_all_pending_keys() {
    let f = Fixture::new();
    let session = session(&f);
    ready(f.storage.commit(tx(vec![custom("history", None)]))).unwrap();
    f.sql("UPDATE entries SET payload='invalid historical payload';");
    TRACE.with(|t| t.borrow_mut().clear());
    ready(session.commit(tx(vec![custom("new", None)]))).unwrap();
    TRACE.with(|t| {
        let trace = t.borrow();
        assert!(
            trace
                .iter()
                .any(|s| s.contains("SELECT key FROM registers WHERE namespace='pending.entry'")),
            "{trace:?}"
        );
        for s in trace
            .iter()
            .filter(|s| s.starts_with("SELECT") && s.contains(" FROM entries"))
        {
            assert!(s.contains("WHERE e.id="), "unbounded hot entry SQL: {s}");
            assert!(!s.contains("payload"));
        }
        assert!(
            !trace
                .iter()
                .any(|s| s.contains("FROM usage_ledger") && !s.contains("WHERE id="))
        );
    });
    ready(f.storage.commit(tx(vec![pending("new")]))).unwrap();
    assert!(
        ready(session.commit(tx(vec![]))).is_err(),
        "untouched pending/placed coexistence must be visible"
    );
    ready(
        session.commit(tx(vec![Write::DeleteRegister(RegisterDelete {
            namespace: RegisterNamespace::PendingEntry,
            key: "new".into(),
        })])),
    )
    .unwrap();
    ready(f.storage.commit(tx(vec![pending("queued")]))).unwrap();
    ready(session.commit(tx(vec![
        custom("queued", None),
        Write::DeleteRegister(RegisterDelete {
            namespace: RegisterNamespace::PendingEntry,
            key: "queued".into(),
        }),
    ])))
    .unwrap();
}
#[test]
fn actual_runtime_old_and_new_summary_label_guards() {
    for (writes, ok) in [
        (
            serde_json::json!([{"kind":"entry","entry":{"id":"s","parentId":null,"type":"branch_summary","fromId":"s","summary":"x","fromHook":false}}]),
            false,
        ),
        (
            serde_json::json!([{"kind":"entry","entry":{"id":"s","parentId":null,"type":"branch_summary","fromId":"missing","summary":"x","fromHook":false}}]),
            false,
        ),
    ] {
        let f = Fixture::new();
        let session = session(&f);
        assert_eq!(
            ready(session.commit(tx(serde_json::from_value(writes).unwrap()))).is_ok(),
            ok
        );
    }
    let f = Fixture::new();
    let session = session(&f);
    let old = Write::SetRegister(Box::new(RegisterSet::FactLabel {
        key: "missing".into(),
        value: "old".into(),
    }));
    ready(f.storage.commit(tx(vec![old.clone()]))).unwrap();
    ready(session.commit(tx(vec![]))).unwrap();
    assert!(ready(session.commit(tx(vec![old]))).is_err());
    ready(session.commit(tx(vec![
        custom("target", None),
        Write::SetRegister(Box::new(RegisterSet::FactLabel {
            key: "target".into(),
            value: "new".into(),
        })),
    ])))
    .unwrap();
}
#[test]
fn complete_validator_receives_actual_full_snapshot_and_repair_retries_preparation() {
    struct Full<'a> {
        f: &'a Fixture,
        calls: std::sync::atomic::AtomicUsize,
    }
    impl PublicationValidator for Full<'_> {
        type Error = ();
        fn validate(&self, s: &StorageSnapshot) -> Result<(), ()> {
            assert_eq!(s.entries.len(), 2);
            assert_eq!(s.usage.len(), 1);
            let calls = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if calls == 0 {
                ready(self.f.storage.repair()).unwrap();
            }
            Ok(())
        }
    }
    let f = Fixture::new();
    ready(f.storage.commit(tx(vec![
        custom("a", None),
        backend::usage("u", Usage::zero(), Some("a")),
    ])))
    .unwrap();
    let validator = Full {
        f: &f,
        calls: std::sync::atomic::AtomicUsize::new(0),
    };
    ready(f.storage.commit_checked(
        ConditionalCommit {
            conditions: vec![],
            transaction: tx(vec![custom("b", Some("a"))]),
        },
        &validator,
    ))
    .unwrap();
    assert_eq!(validator.calls.load(std::sync::atomic::Ordering::SeqCst), 2);
}
#[test]
fn validator_reentrant_close_invalidates_prepared_rejection() {
    struct Reject<'a>(&'a SqliteStorage);
    impl PublicationValidator for Reject<'_> {
        type Error = ();
        fn validate(&self, _: &StorageSnapshot) -> Result<(), ()> {
            ready(self.0.close()).unwrap();
            Err(())
        }
    }
    let f = Fixture::new();
    let result = ready(f.storage.commit_checked(
        ConditionalCommit {
            conditions: vec![],
            transaction: tx(vec![custom("a", None)]),
        },
        &Reject(&f.storage),
    ));
    assert!(matches!(
        result,
        Err(CheckedCommitError::Storage(StorageError::Closed(_)))
    ));
    assert!(
        SqliteStorage::read_snapshot(&f.path, &f.codec, SqliteOptions::default())
            .unwrap()
            .snapshot
            .entries
            .is_empty()
    );
}

use std::io::{BufRead, Write as IoWrite};
#[test]
#[ignore = "passive subprocess protocol driver; exercised by owning process tests"]
fn process_driver() {
    let path = PathBuf::from(std::env::var_os("PI_SQLITE_TEST_PATH").unwrap());
    let mode = std::env::var("PI_SQLITE_TEST_MODE").unwrap();
    if mode == "hold_descriptor" {
        println!("SQLITE:HELD");
        std::io::stdout().flush().unwrap();
        std::thread::park();
        return;
    }
    if mode == "probe_begin" {
        let mut db = Connection::open(&path).unwrap();
        db.busy_timeout(Duration::ZERO).unwrap();
        match db.transaction_with_behavior(TransactionBehavior::Immediate) {
            Ok(tx) => {
                tx.commit().unwrap();
                println!("SQLITE:ACQUIRED");
            }
            Err(e) => {
                assert_eq!(
                    e.sqlite_error_code(),
                    Some(rusqlite::ErrorCode::DatabaseBusy)
                );
                println!("SQLITE:BUSY");
            }
        }
        return;
    }
    let codec = Arc::new(
        SessionCodec::new(
            SessionCodecOptions::default(),
            ProviderHeaderPolicy::default(),
        )
        .unwrap(),
    );
    let options = SqliteOptions {
        lease_ttl_ms: n(1200),
        heartbeat_ms: n(100),
        ..SqliteOptions::default()
    };
    if mode == "reader" {
        let _scope = path::Scope::acquire(&path, false).unwrap();
        let mut db = path::connection(&path, options, true).unwrap();
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Deferred)
            .unwrap();
        let count: i64 = tx
            .query_row("SELECT count(*) FROM entries", [], |r| r.get(0))
            .unwrap();
        println!("SQLITE:READ:{count}");
        std::io::stdout().flush().unwrap();
        for line in std::io::stdin().lock().lines() {
            if line.unwrap() == "read" {
                let count: i64 = tx
                    .query_row("SELECT count(*) FROM entries", [], |r| r.get(0))
                    .unwrap();
                println!("SQLITE:READ:{count}");
                std::io::stdout().flush().unwrap();
            } else {
                break;
            }
        }
        tx.commit().unwrap();
        return;
    }
    let storage = match SqliteStorage::open(&path, codec, Arc::new(Clock), options) {
        Ok(s) => s,
        Err(SessionError::Storage(StorageError::Busy(_))) => {
            println!("SQLITE:BUSY");
            return;
        }
        Err(e) => panic!("{e:?}"),
    };
    println!("SQLITE:OPEN");
    std::io::stdout().flush().unwrap();
    for line in std::io::stdin().lock().lines() {
        let line = line.unwrap();
        let response = if line == "close" {
            ready(storage.close()).unwrap();
            "CLOSED".to_owned()
        } else if line == "stats" {
            format!(
                "STATS:{:?}",
                ready(storage.stats()).map(|s| s.message_count.get())
            )
        } else if let Some(id) = line.strip_prefix("commit ") {
            match ready(storage.commit(tx(vec![custom(id, None)]))) {
                Ok(r) => format!("COMMIT:Ok({})", r.first_seq.get()),
                Err(StorageError::Closed(_)) => "COMMIT:Closed".into(),
                Err(e) => format!("COMMIT:{e:?}"),
            }
        } else {
            break;
        };
        println!("SQLITE:{response}");
        std::io::stdout().flush().unwrap();
    }
}
struct Child {
    child: std::process::Child,
    input: std::process::ChildStdin,
    output: std::io::BufReader<std::process::ChildStdout>,
}
impl Child {
    fn start(path: &Path, mode: &str) -> Self {
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "tests::process_driver",
                "--ignored",
                "--nocapture",
            ])
            .env("PI_SQLITE_TEST_PATH", path)
            .env("PI_SQLITE_TEST_MODE", mode)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let input = child.stdin.take().unwrap();
        let output = std::io::BufReader::new(child.stdout.take().unwrap());
        Self {
            child,
            input,
            output,
        }
    }
    fn response(&mut self) -> String {
        loop {
            let mut line = String::new();
            assert_ne!(
                self.output.read_line(&mut line).unwrap(),
                0,
                "child ended before response"
            );
            if let Some(s) = line.strip_prefix("SQLITE:") {
                return s.trim().to_owned();
            }
        }
    }
    fn send(&mut self, s: &str) -> String {
        writeln!(self.input, "{s}").unwrap();
        self.input.flush().unwrap();
        self.response()
    }
    fn finish(&mut self) {
        let _ = writeln!(self.input, "exit");
        self.input.flush().unwrap();
        assert!(self.child.wait().unwrap().success());
    }
}
impl Drop for Child {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
#[test]
fn process_exclusion_idle_renewal_expired_takeover_and_stale_close() {
    let f = Fixture::new();
    ready(f.storage.close()).unwrap();
    let mut first = Child::start(&f.path, "writer");
    assert_eq!(first.response(), "OPEN");
    let mut refused = Child::start(&f.path, "writer");
    assert_eq!(refused.response(), "BUSY");
    assert!(refused.child.wait().unwrap().success());
    let db = Connection::open(&f.path).unwrap();
    let initial: i64 = db
        .query_row("SELECT expires_at_ms FROM writer_lease", [], |r| r.get(0))
        .unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let expiry: i64 = db
            .query_row("SELECT expires_at_ms FROM writer_lease", [], |r| r.get(0))
            .unwrap();
        if expiry > initial {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "idle heartbeat did not renew"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    let old: i64 = db
        .query_row("SELECT fence FROM writer_lease", [], |r| r.get(0))
        .unwrap();
    db.execute("UPDATE writer_lease SET expires_at_ms=0", [])
        .unwrap();
    let mut second = Child::start(&f.path, "writer");
    assert_eq!(second.response(), "OPEN");
    let new: i64 = db
        .query_row("SELECT fence FROM writer_lease", [], |r| r.get(0))
        .unwrap();
    assert!(new > old);
    let stale = first.send("commit stale");
    assert!(stale.contains("Closed"), "{stale}");
    assert_eq!(first.send("close"), "CLOSED");
    assert_eq!(
        db.query_row("SELECT fence FROM writer_lease", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        new
    );
    assert!(second.send("commit successor").contains("Ok(0)"));
    first.finish();
    second.finish();
}
#[test]
fn process_readonly_wal_snapshot_and_acknowledged_commit_survive_termination() {
    let f = Fixture::new();
    ready(f.storage.close()).unwrap();
    let mut writer = Child::start(&f.path, "writer");
    assert_eq!(writer.response(), "OPEN");
    assert!(writer.send("commit a").contains("Ok(0)"));
    let mut reader = Child::start(&f.path, "reader");
    assert_eq!(reader.response(), "READ:1");
    assert!(writer.send("commit b").contains("Ok(1)"));
    assert_eq!(reader.send("read"), "READ:1");
    assert_eq!(
        SqliteStorage::read_snapshot(&f.path, &f.codec, SqliteOptions::default())
            .unwrap()
            .snapshot
            .entries
            .len(),
        2
    );
    reader.finish();
    writer.child.kill().unwrap();
    assert!(!writer.child.wait().unwrap().success());
    let observed =
        SqliteStorage::read_snapshot(&f.path, &f.codec, SqliteOptions::default()).unwrap();
    assert_eq!(backend::ids(observed.snapshot.entries), ["a", "b"]);
    f.sql("UPDATE writer_lease SET expires_at_ms=0;");
    let reopened = SqliteStorage::open(
        &f.path,
        f.codec.clone(),
        Arc::new(Clock),
        SqliteOptions::default(),
    )
    .unwrap();
    assert_eq!(
        backend::ids(ready(reopened.scan_entries(EntryScan::default())).unwrap()),
        ["a", "b"]
    );
    ready(reopened.close()).unwrap();
}
#[test]
fn final_sequence_allocates_once_and_exhausted_empty_commit_is_typed() {
    let f = Fixture::new();
    let mut snapshot = ready(f.storage.snapshot()).unwrap();
    snapshot.next_seq = Some(n(9_007_199_254_740_991));
    let path = f.directory.join("max.db");
    let storage = SqliteStorage::create_from_snapshot(
        &path,
        f.storage.metadata().unwrap(),
        snapshot,
        f.codec.clone(),
        Arc::new(Clock),
        SqliteOptions::default(),
    )
    .unwrap();
    let before = ready(storage.snapshot())
        .unwrap_or_else(|e| TRACE.with(|t| panic!("{e:?}: {:?}", t.borrow())));
    assert!(matches!(
        ready(storage.commit(tx(vec![custom("a", None), custom("b", Some("a"))]))),
        Err(StorageError::InvalidData(_))
    ));
    assert_eq!(ready(storage.snapshot()).unwrap(), before);
    assert_eq!(
        ready(storage.commit(tx(vec![custom("a", None)])))
            .unwrap()
            .first_seq,
        n(9_007_199_254_740_991)
    );
    assert_eq!(ready(storage.snapshot()).unwrap().next_seq, None);
    assert!(matches!(
        ready(storage.commit(tx(vec![]))),
        Err(StorageError::InvalidData(_))
    ));
    ready(storage.close()).unwrap();
    let reopened = SqliteStorage::open(
        &path,
        f.codec.clone(),
        Arc::new(Clock),
        SqliteOptions::default(),
    )
    .unwrap();
    assert_eq!(ready(reopened.snapshot()).unwrap().next_seq, None);
    ready(reopened.close()).unwrap();
}
#[test]
fn usage_overflow_and_sql_failure_roll_back_every_row_and_allocator() {
    let f = Fixture::new();
    let mut huge = Usage::zero();
    huge.input = n(9_007_199_254_740_991);
    ready(f.storage.commit(tx(vec![backend::usage("u", huge, None)]))).unwrap();
    let before = ready(f.storage.snapshot()).unwrap();
    let mut one = Usage::zero();
    one.input = n(1);
    assert!(matches!(
        ready(f.storage.commit(tx(vec![
            custom("notpublished", None),
            backend::usage("v", one, None)
        ]))),
        Err(StorageError::InvalidData(_))
    ));
    assert_eq!(ready(f.storage.snapshot()).unwrap(), before);
    f.sql("CREATE TRIGGER reject_second BEFORE INSERT ON entries WHEN NEW.id='b' BEGIN SELECT RAISE(ABORT,'injected later row failure'); END;");
    assert!(
        ready(
            f.storage
                .commit(tx(vec![custom("a", None), custom("b", Some("a"))]))
        )
        .is_err()
    );
    assert_eq!(ready(f.storage.snapshot()).unwrap(), before);
    f.sql("DROP TRIGGER reject_second;");
    assert_eq!(
        ready(f.storage.commit(tx(vec![custom("a", None)])))
            .unwrap()
            .first_seq,
        before.next_seq.unwrap()
    );
}
#[test]
fn readonly_pages_keep_sparse_sequence_progress_and_reset_incarnation() {
    use pi_agent_session::read::ReadBatchRequest;
    let f = Fixture::new();
    ready(f.storage.commit(tx(vec![
        custom("a", None),
        backend::set("x", JsonValue::Null),
        custom("b", Some("a")),
    ])))
    .unwrap();
    let first = SqliteStorage::read_entry_page(
        &f.path,
        &f.codec,
        SqliteOptions::default(),
        ReadBatchRequest {
            cursor: None,
            limit: n(1),
        },
    )
    .unwrap();
    assert_eq!(backend::ids(first.entries), ["a"]);
    assert!(!first.complete);
    assert_eq!(first.next_cursor.last_entry_seq, Some(n(0)));
    let second = SqliteStorage::read_entry_page(
        &f.path,
        &f.codec,
        SqliteOptions::default(),
        ReadBatchRequest {
            cursor: Some(first.next_cursor.clone()),
            limit: n(1),
        },
    )
    .unwrap();
    assert_eq!(backend::ids(second.entries), ["b"]);
    assert!(second.complete);
    assert_eq!(second.next_cursor.last_entry_seq, Some(n(2)));
    let mut alien = first.next_cursor;
    alien.witness[1] ^= 1;
    let reset = SqliteStorage::read_entry_page(
        &f.path,
        &f.codec,
        SqliteOptions::default(),
        ReadBatchRequest {
            cursor: Some(alien),
            limit: n(1),
        },
    )
    .unwrap();
    assert!(reset.reset);
    assert_eq!(backend::ids(reset.entries), ["a"]);
}
#[cfg(unix)]
#[test]
fn canonical_aliases_busy_hardlinks_refused_and_replaced_identity_seals() {
    let f = Fixture::new();
    let alias = f.directory.join("alias.db");
    std::os::unix::fs::symlink(&f.path, &alias).unwrap();
    assert!(matches!(
        SqliteStorage::open(
            &alias,
            f.codec.clone(),
            Arc::new(Clock),
            SqliteOptions::default()
        ),
        Err(SessionError::Storage(StorageError::Busy(_)))
    ));
    let hard = f.directory.join("hard.db");
    std::fs::hard_link(&f.path, &hard).unwrap();
    assert!(SqliteStorage::read_metadata(&hard, SqliteOptions::default()).is_err());
    std::fs::remove_file(hard).unwrap();
    let original = f.directory.join("original.db");
    std::fs::rename(&f.path, &original).unwrap();
    std::fs::write(&f.path, []).unwrap();
    assert!(matches!(
        ready(f.storage.stats()),
        Err(StorageError::Closed(_))
    ));
    std::fs::remove_file(&f.path).unwrap();
    std::fs::rename(original, &f.path).unwrap();
    assert!(matches!(
        ready(f.storage.stats()),
        Err(StorageError::Closed(_))
    ));
}
#[test]
fn immediate_admission_samples_native_time_after_sql_contention() {
    struct WaitingClock {
        path: PathBuf,
        once: std::sync::atomic::AtomicBool,
        worker: Mutex<Option<std::thread::JoinHandle<SafeUnsigned>>>,
    }
    impl StorageClock for WaitingClock {
        fn now(&self) -> Result<SafeUnsigned, StorageError> {
            if !self.once.swap(true, std::sync::atomic::Ordering::SeqCst) {
                let path = self.path.clone();
                let (send, receive) = std::sync::mpsc::channel();
                let handle = std::thread::spawn(move || {
                    let mut db = Connection::open(path).unwrap();
                    let tx = db
                        .transaction_with_behavior(TransactionBehavior::Immediate)
                        .unwrap();
                    send.send(()).unwrap();
                    let until = now().unwrap().get() + 250;
                    while now().unwrap().get() < until {
                        std::thread::sleep(Duration::from_millis(2));
                    }
                    tx.commit().unwrap();
                    now().unwrap()
                });
                *self.worker.lock().unwrap() = Some(handle);
                receive.recv().unwrap();
            }
            Ok(n(1234))
        }
    }
    let f = Fixture::new();
    ready(f.storage.close()).unwrap();
    let clock = Arc::new(WaitingClock {
        path: f.path.clone(),
        once: std::sync::atomic::AtomicBool::new(false),
        worker: Mutex::new(None),
    });
    let storage = SqliteStorage::open(
        &f.path,
        f.codec.clone(),
        clock.clone(),
        SqliteOptions {
            lease_ttl_ms: n(5000),
            heartbeat_ms: n(4000),
            ..SqliteOptions::default()
        },
    )
    .unwrap();
    ready(storage.commit(tx(vec![custom("a", None)]))).unwrap();
    let released = clock.worker.lock().unwrap().take().unwrap().join().unwrap();
    let db = Connection::open(&f.path).unwrap();
    let expiry: i64 = db
        .query_row("SELECT expires_at_ms FROM writer_lease", [], |r| r.get(0))
        .unwrap();
    assert!(
        expiry >= released.get() as i64 + 4900,
        "lease used pre-contention native time"
    );
    ready(storage.close()).unwrap();
}
#[test]
fn imported_database_is_checkpointed_complete_at_first_publication() {
    let f = Fixture::new();
    ready(f.storage.commit(tx(vec![
        custom("seed", None),
        backend::set("seed", JsonValue::Null),
    ])))
    .unwrap();
    let snapshot = ready(f.storage.snapshot()).unwrap();
    let target = f.directory.join("published.db");
    let observed = std::rc::Rc::new(std::cell::Cell::new(0));
    let counts = observed.clone();
    PUBLICATION.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move |path, published| {
            if !published {
                assert!(!path.exists());
                return;
            }
            assert!(
                !path
                    .with_file_name(format!(
                        "{}-wal",
                        path.file_name().unwrap().to_str().unwrap()
                    ))
                    .exists()
            );
            assert!(
                !path
                    .with_file_name(format!(
                        "{}-shm",
                        path.file_name().unwrap().to_str().unwrap()
                    ))
                    .exists()
            );
            let db = Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
                .unwrap();
            let entries: i64 = db
                .query_row("SELECT count(*) FROM entries WHERE id='seed'", [], |r| {
                    r.get(0)
                })
                .unwrap();
            let regs: i64 = db
                .query_row("SELECT count(*) FROM registers WHERE key='seed'", [], |r| {
                    r.get(0)
                })
                .unwrap();
            assert_eq!(
                (entries, regs),
                (1, 1),
                "actual final file must already include complete seed"
            );
            counts.set(counts.get() + 1);
        }))
    });
    let created = SqliteStorage::create_from_snapshot(
        &target,
        f.storage.metadata().unwrap(),
        snapshot.clone(),
        f.codec.clone(),
        Arc::new(Clock),
        SqliteOptions::default(),
    )
    .unwrap();
    PUBLICATION.with(|h| h.borrow_mut().take());
    assert_eq!(observed.get(), 1);
    assert_eq!(ready(created.snapshot()).unwrap(), snapshot);
    ready(created.close()).unwrap();
}
#[test]
fn production_writes_are_immediate_and_deferred_upgrade_contrast_fails() {
    let f = Fixture::new();
    TRACE.with(|t| t.borrow_mut().clear());
    ready(f.storage.commit(tx(vec![custom("a", None)]))).unwrap();
    ready(f.storage.repair()).unwrap();
    ready(f.storage.close()).unwrap();
    TRACE.with(|t| {
        let t = t.borrow();
        assert!(
            t.iter().filter(|s| s.as_str() == "BEGIN IMMEDIATE").count() >= 3,
            "{t:?}"
        );
        assert!(t.iter().any(|s| s == "BEGIN DEFERRED"));
    });
    let mut reader = Connection::open(&f.path).unwrap();
    let writer = Connection::open(&f.path).unwrap();
    let tx = reader
        .transaction_with_behavior(TransactionBehavior::Deferred)
        .unwrap();
    tx.query_row("SELECT revision FROM session", [], |r| r.get::<_, i64>(0))
        .unwrap();
    writer
        .execute("UPDATE session SET revision=revision+1", [])
        .unwrap();
    let error = tx
        .execute("UPDATE session SET revision=revision+1", [])
        .unwrap_err();
    assert_eq!(
        error.sqlite_error_code(),
        Some(rusqlite::ErrorCode::DatabaseBusy)
    );
}
#[test]
fn host_clock_reentrant_close_prevents_publication() {
    struct Closing(Mutex<Option<std::sync::Weak<SqliteStorage>>>);
    impl StorageClock for Closing {
        fn now(&self) -> Result<SafeUnsigned, StorageError> {
            if let Some(s) = self
                .0
                .lock()
                .unwrap()
                .as_ref()
                .and_then(std::sync::Weak::upgrade)
            {
                ready(s.close()).unwrap();
            }
            Ok(n(1234))
        }
    }
    let f = Fixture::new();
    ready(f.storage.close()).unwrap();
    let clock = Arc::new(Closing(Mutex::new(None)));
    let storage = SqliteStorage::open(
        &f.path,
        f.codec.clone(),
        clock.clone(),
        SqliteOptions::default(),
    )
    .unwrap();
    *clock.0.lock().unwrap() = Some(Arc::downgrade(&storage));
    assert!(matches!(
        ready(storage.commit(tx(vec![custom("a", None)]))),
        Err(StorageError::Closed(_))
    ));
    assert!(
        SqliteStorage::read_snapshot(&f.path, &f.codec, SqliteOptions::default())
            .unwrap()
            .snapshot
            .entries
            .is_empty()
    );
}
#[test]
fn mechanical_snapshot_preserves_raw_pending_placement_coexistence() {
    let f = Fixture::new();
    ready(f.storage.commit(tx(vec![custom("u", None), pending("u")]))).unwrap();
    let snapshot = ready(f.storage.snapshot())
        .expect("authority snapshot must retain mechanically valid raw state");
    assert_eq!(snapshot.entries.len(), 1);
    assert_eq!(snapshot.registers.len(), 1);
    let observed = SqliteStorage::read_snapshot(&f.path, &f.codec, SqliteOptions::default())
        .expect("readonly authority snapshot must retain raw state");
    assert_eq!(snapshot, observed.snapshot);
    ready(f.storage.close()).unwrap();
    let reopened = SqliteStorage::open(
        &f.path,
        f.codec.clone(),
        Arc::new(Clock),
        SqliteOptions::default(),
    )
    .expect("raw accepted state must reopen for repair");
    assert_eq!(ready(reopened.snapshot()).unwrap(), snapshot);
    assert!(
        ready(pi_agent_session::session::RuntimeSession::new(
            reopened.clone(),
            reopened.metadata().unwrap(),
            f.codec.clone(),
            Arc::new(Ids::default()),
            Arc::new(NoopTelemetryContext)
        ))
        .is_err()
    );
    let imported = SqliteStorage::create_from_snapshot(
        f.directory.join("imported.db"),
        reopened.metadata().unwrap(),
        snapshot.clone(),
        f.codec.clone(),
        Arc::new(Clock),
        SqliteOptions::default(),
    )
    .expect("raw import preserves mechanically valid state");
    assert_eq!(ready(imported.snapshot()).unwrap(), snapshot);
    ready(imported.close()).unwrap();
    ready(reopened.close()).unwrap();
}

// Causal schedule independently supplied by reviewer546; exercised on author source.
#[test]
fn concurrent_close_waits_for_actual_lease_release() {
    let f = Fixture::new();
    let path = f.path.clone();
    let release_started = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let released = release_started.clone();
    let (locked_send, locked_recv) = std::sync::mpsc::channel();
    let (release_send, release_recv) = std::sync::mpsc::channel();
    let blocker = std::thread::spawn(move || {
        let _scope = path::Scope::acquire(&path, false).unwrap();
        let mut c = Connection::open(path).unwrap();
        let tx = c
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        locked_send.send(()).unwrap();
        // Watchdog releases a correct blocking close too; the assertion uses causal
        // release order, not a required wall-clock completion threshold.
        let _ = release_recv.recv_timeout(Duration::from_secs(2));
        released.store(true, std::sync::atomic::Ordering::SeqCst);
        tx.commit().unwrap();
    });
    locked_recv.recv_timeout(Duration::from_secs(5)).unwrap();
    let (begin_send, begin_recv) = std::sync::mpsc::channel();
    let first_storage = f.storage.clone();
    let first = std::thread::spawn(move || {
        BEGIN_ACK.with(|ack| *ack.borrow_mut() = Some(begin_send));
        ready(first_storage.close())
    });
    begin_recv.recv_timeout(Duration::from_secs(5)).unwrap();
    // First closer has sealed owner and reached actual SQL release transaction,
    // which cannot acquire while the separately held writer transaction remains.
    let second = ready(f.storage.close());
    let acknowledged_before_release = !release_started.load(std::sync::atomic::Ordering::SeqCst);
    let _ = release_send.send(());
    blocker.join().unwrap();
    first.join().unwrap().unwrap();
    second.unwrap();
    assert!(
        !acknowledged_before_release,
        "second close acknowledged while first close had not released its SQL lease"
    );
}
#[test]
fn newer_version_precedes_header_usage_decoding() {
    let f = Fixture::new();
    ready(f.storage.close()).unwrap();
    f.sql("UPDATE session SET storage_version=2,usage_payload='future-schema-payload';");
    assert!(matches!(
        SqliteStorage::open(
            &f.path,
            f.codec.clone(),
            Arc::new(Clock),
            SqliteOptions::default()
        ),
        Err(SessionError::NewerStorageVersion(_))
    ));
    assert!(matches!(
        SqliteStorage::read_snapshot(&f.path, &f.codec, SqliteOptions::default()),
        Err(SessionError::NewerStorageVersion(_))
    ));
    assert!(matches!(
        SqliteStorage::read_entry_page(
            &f.path,
            &f.codec,
            SqliteOptions::default(),
            pi_agent_session::read::ReadBatchRequest {
                cursor: None,
                limit: n(1)
            }
        ),
        Err(SessionError::NewerStorageVersion(_))
    ));
}

#[test]
fn identity_observation_does_not_release_active_sqlite_posix_lock() {
    let f = Fixture::new();
    ready(f.storage.close()).unwrap();
    let _scope = path::Scope::acquire(&f.path, false).unwrap();
    let mut db = Connection::open(&f.path).unwrap();
    db.pragma_update(None, "journal_mode", "DELETE").unwrap();
    let tx = db
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    let mut before = Child::start(&f.path, "probe_begin");
    assert_eq!(before.response(), "BUSY");
    assert!(before.child.wait().unwrap().success());
    {
        let _identity = path::inspect_identity(&f.path).unwrap();
    }
    let mut after = Child::start(&f.path, "probe_begin");
    let acquired = after.response();
    assert!(after.child.wait().unwrap().success());
    tx.commit().unwrap();
    assert_eq!(
        acquired, "BUSY",
        "identity observation released process POSIX SQLite lock"
    );
}

#[test]
fn logical_path_scope_releases_even_when_child_inherits_description() {
    let f = Fixture::new();
    let scope = path::Scope::acquire(&f.path, true).unwrap();
    struct Held(std::process::Child);
    impl Drop for Held {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let mut child = Held(
        std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "tests::process_driver",
                "--ignored",
                "--nocapture",
            ])
            .env("PI_SQLITE_TEST_PATH", &f.path)
            .env("PI_SQLITE_TEST_MODE", "hold_descriptor")
            .stdin(std::process::Stdio::from(scope.inherited_descriptor()))
            .stdout(std::process::Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let mut output = std::io::BufReader::new(child.0.stdout.take().unwrap());
    loop {
        let mut line = String::new();
        assert_ne!(output.read_line(&mut line).unwrap(), 0);
        if line.contains("SQLITE:HELD") {
            break;
        }
    }
    drop(scope);
    let next = path::Scope::acquire(&f.path, true);
    drop(child);
    assert!(
        next.is_ok(),
        "logical guard release must not await child's inherited description"
    );
}

#[test]
fn raw_custom_codec_rejects_unreadable_rows_and_registered_role_roundtrips() {
    let f = Fixture::new();
    let write:Write=serde_json::from_value(serde_json::json!({"kind":"entry","entry":{"id":"custom-message","parentId":null,"type":"message","message":{"role":"unregistered-review-role","payload":{"value":true}}}})).unwrap();
    let before = ready(f.storage.snapshot()).unwrap();
    assert!(
        ready(f.storage.commit(tx(vec![write.clone()]))).is_err(),
        "unreadable role must be rejected before acknowledgement"
    );
    assert_eq!(ready(f.storage.snapshot()).unwrap(), before);
    let codec=Arc::new(SessionCodec::new(SessionCodecOptions::from_role_schemas([("unregistered-review-role".into(),serde_json::from_value(serde_json::json!({"type":"object","required":["role","payload"],"properties":{"role":{"const":"unregistered-review-role"},"payload":{"type":"object"}},"additionalProperties":false})).unwrap())].into()).unwrap(),ProviderHeaderPolicy::default()).unwrap());
    let path = f.directory.join("custom.db");
    let storage = SqliteStorage::create(
        &path,
        f.storage.metadata().unwrap(),
        codec.clone(),
        Arc::new(Clock),
        SqliteOptions::default(),
    )
    .unwrap();
    ready(storage.commit(tx(vec![write]))).unwrap();
    assert!(
        ready(storage.get_entries(&[EntryId::new("custom-message").unwrap()]))
            .unwrap()
            .contains_key(&EntryId::new("custom-message").unwrap())
    );
    let before = ready(storage.snapshot()).unwrap();
    ready(storage.close()).unwrap();
    let reopened =
        SqliteStorage::open(&path, codec, Arc::new(Clock), SqliteOptions::default()).unwrap();
    assert_eq!(ready(reopened.snapshot()).unwrap(), before);
    ready(reopened.close()).unwrap();
}
