//! Runtime attachment and actual backend publication histories.
use pi_agent_schema::session_values::*;
use pi_agent_schema::*;
use pi_agent_session::{
    backend_support::*, codec::SessionCodec, session::RuntimeSession, storage::*,
};
use std::{
    path::PathBuf,
    sync::{
        Arc, Barrier,
        atomic::{AtomicUsize, Ordering},
    },
};
fn n(value: u64) -> SafeUnsigned {
    SafeUnsigned::new(value).unwrap()
}
fn ready<T>(future: impl std::future::Future<Output = T>) -> T {
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    match std::pin::pin!(future).as_mut().poll(&mut cx) {
        std::task::Poll::Ready(value) => value,
        _ => panic!("backend must complete its admitted operation"),
    }
}
struct Clock;
impl StorageClock for Clock {
    fn now(&self) -> Result<SafeUnsigned, StorageError> {
        Ok(n(1234))
    }
}
struct Ids;
impl IdGenerator for Ids {
    fn next(&self, _: Option<SafeUnsigned>) -> String {
        panic!("read/attachment generated an ID")
    }
}
fn metadata() -> SessionMetadata {
    serde_json::from_value(serde_json::json!({"id":"same-logical-session","createdAt":1,"storageVersion":1,"storeGeneration":0})).unwrap()
}
fn codec() -> Arc<SessionCodec> {
    Arc::new(
        SessionCodec::new(
            SessionCodecOptions::default(),
            ProviderHeaderPolicy::from_allowlist(["x-public".into()]),
        )
        .unwrap(),
    )
}
fn session<S: CheckedStorage + 'static>(s: Arc<S>, codec: Arc<SessionCodec>) -> RuntimeSession<S> {
    ready(RuntimeSession::new(
        s,
        metadata(),
        codec,
        Arc::new(Ids),
        Arc::new(NoopTelemetryContext),
    ))
    .unwrap()
}
struct Dir(PathBuf);
impl Dir {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "pi-runtime-capability-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Dir {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
fn tx(writes: Vec<Write>) -> Transaction {
    Transaction { writes }
}
fn set(key: &str) -> Write {
    Write::SetRegister(Box::new(RegisterSet::FactCustom {
        key: key.into(),
        value: JsonValue::Null,
    }))
}
fn delete(key: &str) -> Write {
    Write::DeleteRegister(RegisterDelete {
        namespace: RegisterNamespace::FactCustom,
        key: key.into(),
    })
}
fn entry() -> Write {
    Write::Entry(NewEntry::Custom(NewCustomEntry {
        id: EntryId::new("entry").unwrap(),
        parent_id: None,
        entry_type: CustomEntryType::Custom,
        custom_type: CustomType::new("example").unwrap(),
        data: Presence::Missing,
    }))
}
struct Accept;
impl PublicationValidator for Accept {
    type Error = ();
    fn validate(&self, _: &StorageSnapshot) -> Result<(), ()> {
        Ok(())
    }
}
struct Reject;
impl PublicationValidator for Reject {
    type Error = ();
    fn validate(&self, _: &StorageSnapshot) -> Result<(), ()> {
        Err(())
    }
}
fn histories<S: RuntimeStorage + 'static>(store: Arc<S>, independent: Arc<S>) {
    let c = codec();
    let first = session(store.clone(), c.clone());
    let second = session(store.clone(), c.clone());
    let other = session(independent.clone(), c.clone());
    assert_eq!(first.metadata().id, other.metadata().id);
    assert!(std::ptr::eq(first.codec(), c.as_ref()));
    assert!(std::ptr::eq(
        first.codec().header_policy(),
        c.header_policy()
    ));
    let lease = first.claim_runtime().unwrap();
    assert!(matches!(
        second.claim_runtime(),
        Err(SessionError::Storage(StorageError::Busy(_)))
    ));
    assert!(matches!(
        second
            .lane_view(LaneId::new("another").unwrap())
            .claim_runtime(),
        Err(SessionError::Storage(StorageError::Busy(_)))
    ));
    let other_lease = other.claim_runtime().unwrap();
    assert_ne!(
        ready(first.read_stamp()).unwrap(),
        ready(other.read_stamp()).unwrap()
    );
    let retained_stamp = ready(first.read_stamp()).unwrap();
    drop(lease);
    let lease = second.claim_runtime().unwrap();
    assert_eq!(
        retained_stamp,
        ready(first.read_stamp()).unwrap(),
        "claims are not publications"
    );
    drop(lease);
    drop(other_lease);

    let writes = vec![
        vec![set("root")],
        vec![set("root")],
        vec![entry()],
        vec![Write::Usage(NewUsageRow {
            id: UsageId::new("usage").unwrap(),
            usage: Usage::zero(),
            entry_id: None,
            adjustment: true,
            details: Presence::Missing,
        })],
        vec![delete("root")],
        vec![delete("already-absent")],
        vec![],
    ];
    for writes in writes {
        let before = ready(first.read_stamp()).unwrap();
        ready(store.commit(tx(writes))).unwrap();
        assert_ne!(
            before,
            ready(second.read_stamp()).unwrap(),
            "successful publication must invalidate stamp"
        );
    }
    let before = ready(store.read_stamp()).unwrap();
    ready(store.get_register(RegisterNamespace::FactCustom, "root")).unwrap();
    ready(store.get_entries(&[EntryId::new("entry").unwrap()])).unwrap();
    ready(store.list_registers(RegisterNamespace::FactCustom, None)).unwrap();
    ready(store.stats()).unwrap();
    assert_eq!(
        before,
        ready(store.read_stamp()).unwrap(),
        "reads cannot advance stamp"
    );
    assert!(matches!(
        ready(store.commit_conditional(ConditionalCommit {
            conditions: vec![RegisterCondition {
                register: RegisterKey {
                    namespace: RegisterNamespace::FactCustom,
                    key: "absent".into()
                },
                expected: ExpectedRegister::Seq(n(0))
            }],
            transaction: tx(vec![set("wrong")])
        }))
        .unwrap(),
        ConditionalOutcome::NotApplied
    ));
    assert_eq!(before, ready(store.read_stamp()).unwrap());
    assert!(matches!(
        ready(store.commit_checked(
            ConditionalCommit {
                conditions: vec![],
                transaction: tx(vec![set("rejected")])
            },
            &Reject
        )),
        Err(CheckedCommitError::Rejected(()))
    ));
    assert_eq!(before, ready(store.read_stamp()).unwrap());
    assert!(ready(store.commit(tx(vec![entry()]))).is_err());
    assert_eq!(before, ready(store.read_stamp()).unwrap());
    ready(store.commit_checked(
        ConditionalCommit {
            conditions: vec![],
            transaction: tx(vec![set("checked")]),
        },
        &Accept,
    ))
    .unwrap();
    assert_ne!(before, ready(store.read_stamp()).unwrap());
    let before = ready(store.read_stamp()).unwrap();
    ready(first.commit_conditional(ConditionalCommit {
        conditions: vec![],
        transaction: tx(vec![set("session-checked")]),
    }))
    .unwrap();
    assert_ne!(before, ready(store.read_stamp()).unwrap());

    // A real writer publishes strictly between bounded reads, without a snapshot.
    let barrier = Arc::new(Barrier::new(2));
    let writer = store.clone();
    let gate = barrier.clone();
    let join = std::thread::spawn(move || {
        gate.wait();
        ready(writer.commit(tx(vec![set("between-roots")]))).unwrap();
        gate.wait();
    });
    let before = ready(first.read_stamp()).unwrap();
    assert!(
        ready(store.get_register(RegisterNamespace::FactCustom, "between-roots"))
            .unwrap()
            .is_none()
    );
    barrier.wait();
    barrier.wait();
    assert!(
        ready(store.get_register(RegisterNamespace::FactCustom, "between-roots"))
            .unwrap()
            .is_some()
    );
    assert_ne!(
        before,
        ready(second.read_stamp()).unwrap(),
        "mixed bounded reads must be discarded"
    );
    join.join().unwrap();
}

use pi_agent_sqlite::{SqliteOptions, SqliteStorage};
#[test]
fn sqlite_publication_attachment_repair_and_reopen() {
    let dir = Dir::new();
    let path = dir.0.join("session.db");
    let store = SqliteStorage::create(
        &path,
        metadata(),
        codec(),
        Arc::new(Clock),
        SqliteOptions::default(),
    )
    .unwrap();
    let other = SqliteStorage::create(
        dir.0.join("independent.db"),
        metadata(),
        codec(),
        Arc::new(Clock),
        SqliteOptions::default(),
    )
    .unwrap();
    histories(store.clone(), other);
    let before = ready(store.read_stamp()).unwrap();
    ready(store.repair()).unwrap();
    assert_ne!(
        before,
        ready(store.read_stamp()).unwrap(),
        "derived repair invalidates prepared observations"
    );
    let before = ready(store.read_stamp()).unwrap();
    ready(store.close()).unwrap();
    assert!(matches!(
        ready(store.read_stamp()),
        Err(StorageError::Closed(_))
    ));
    let reopened =
        SqliteStorage::open(&path, codec(), Arc::new(Clock), SqliteOptions::default()).unwrap();
    assert_ne!(before, ready(reopened.read_stamp()).unwrap());
    ready(reopened.close()).unwrap();
}
#[test]
fn sqlite_stamp_checks_actual_fence_and_avoids_history_decode() {
    let dir = Dir::new();
    let path = dir.0.join("session.db");
    let store = SqliteStorage::create(
        &path,
        metadata(),
        codec(),
        Arc::new(Clock),
        SqliteOptions::default(),
    )
    .unwrap();
    ready(store.commit(tx(vec![entry()]))).unwrap();
    let stamp = ready(store.read_stamp()).unwrap();
    let db = rusqlite::Connection::open(&path).unwrap();
    // An unrelated malformed historical row distinguishes a header-only stamp from
    // complete snapshot decoding. This test database is discarded afterward.
    db.execute("UPDATE entries SET payload='{'", []).unwrap();
    assert_eq!(stamp, ready(store.read_stamp()).unwrap());
    assert!(ready(store.snapshot()).is_err());
    db.execute("UPDATE writer_lease SET fence=fence+1", [])
        .unwrap();
    assert!(matches!(
        ready(store.read_stamp()),
        Err(StorageError::Closed(_))
    ));
    ready(store.close()).unwrap();
}
#[test]
fn sqlite_stamps_are_distinct_from_memory_and_jsonl() {
    use pi_agent_session::{
        jsonl::{JsonlOptions, JsonlStorage},
        memory::MemoryBacking,
    };
    let dir = Dir::new();
    let sqlite = SqliteStorage::create(
        dir.0.join("session.db"),
        metadata(),
        codec(),
        Arc::new(Clock),
        SqliteOptions::default(),
    )
    .unwrap();
    let jsonl = JsonlStorage::create(
        dir.0.join("session.jsonl"),
        metadata(),
        codec(),
        Arc::new(Clock),
        JsonlOptions::default(),
    )
    .unwrap();
    let backing = Arc::new(MemoryBacking::new());
    let memory = backing.open(Arc::new(Clock)).unwrap();
    assert_ne!(
        ready(sqlite.read_stamp()).unwrap(),
        ready(jsonl.read_stamp()).unwrap()
    );
    assert_ne!(
        ready(sqlite.read_stamp()).unwrap(),
        ready(memory.read_stamp()).unwrap()
    );
    ready(sqlite.close()).unwrap();
    ready(jsonl.close()).unwrap();
}

#[test]
fn sqlite_revision_exhaustion_never_reuses_a_stamp() {
    let dir = Dir::new();
    let path = dir.0.join("session.db");
    let store = SqliteStorage::create(
        &path,
        metadata(),
        codec(),
        Arc::new(Clock),
        SqliteOptions::default(),
    )
    .unwrap();
    let original = ready(store.read_stamp()).unwrap();
    let db = rusqlite::Connection::open(&path).unwrap();
    db.execute("UPDATE session SET revision=9007199254740991", [])
        .unwrap();
    let exhausted = ready(store.read_stamp()).unwrap();
    assert_ne!(original, exhausted);
    assert!(ready(store.commit(tx(vec![set("must-not-publish")]))).is_err());
    assert!(ready(store.repair()).is_err());
    assert_eq!(exhausted, ready(store.read_stamp()).unwrap());
    assert!(
        ready(store.get_register(RegisterNamespace::FactCustom, "must-not-publish"))
            .unwrap()
            .is_none()
    );
    ready(store.close()).unwrap();
}
