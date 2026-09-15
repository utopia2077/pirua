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

use pi_agent_session::{
    jsonl::{JsonlOptions, JsonlStorage},
    memory::{MemoryBacking, MemoryStorage},
};
fn memory() -> (Arc<MemoryBacking>, Arc<MemoryStorage>) {
    let backing = Arc::new(MemoryBacking::new());
    let store = backing.open(Arc::new(Clock)).unwrap();
    (backing, store)
}
#[test]
fn memory_publication_attachment_and_reopen() {
    let (backing, store) = memory();
    let (_, other) = memory();
    histories(store.clone(), other);
    let stamp = ready(store.read_stamp()).unwrap();
    let lease = store.runtime_attachment().try_claim().unwrap();
    ready(store.close()).unwrap();
    assert!(matches!(
        ready(store.read_stamp()),
        Err(StorageError::Closed(_))
    ));
    let reopened = backing.open(Arc::new(Clock)).unwrap();
    assert_ne!(stamp, ready(reopened.read_stamp()).unwrap());
    let new_lease = reopened.runtime_attachment().try_claim().unwrap();
    drop(lease);
    assert!(matches!(
        reopened.runtime_attachment().try_claim(),
        Err(StorageError::Busy(_))
    ));
    drop(new_lease);
}
#[test]
fn jsonl_publication_attachment_rewrite_and_reopen() {
    let dir = Dir::new();
    let path = dir.0.join("session.jsonl");
    let store = JsonlStorage::create(
        &path,
        metadata(),
        codec(),
        Arc::new(Clock),
        JsonlOptions::default(),
    )
    .unwrap();
    let other = JsonlStorage::create(
        dir.0.join("independent.jsonl"),
        metadata(),
        codec(),
        Arc::new(Clock),
        JsonlOptions::default(),
    )
    .unwrap();
    histories(store.clone(), other);
    let before = ready(store.read_stamp()).unwrap();
    ready(store.compact()).unwrap();
    assert_ne!(
        before,
        ready(store.read_stamp()).unwrap(),
        "precise rewrite invalidates stamp even when rows are unchanged"
    );
    let before = ready(store.read_stamp()).unwrap();
    ready(store.close()).unwrap();
    assert!(matches!(
        ready(store.read_stamp()),
        Err(StorageError::Closed(_))
    ));
    let reopened =
        JsonlStorage::open(&path, codec(), Arc::new(Clock), JsonlOptions::default()).unwrap();
    assert_ne!(before, ready(reopened.read_stamp()).unwrap());
    ready(reopened.close()).unwrap();
}
#[test]
fn stamps_from_different_backends_never_compare_equal() {
    let dir = Dir::new();
    let (_, memory) = memory();
    let jsonl = JsonlStorage::create(
        dir.0.join("session.jsonl"),
        metadata(),
        codec(),
        Arc::new(Clock),
        JsonlOptions::default(),
    )
    .unwrap();
    assert_ne!(
        ready(memory.read_stamp()).unwrap(),
        ready(jsonl.read_stamp()).unwrap()
    );
    ready(jsonl.close()).unwrap();
}
#[test]
fn claim_is_atomic_across_threads_and_release_preserves_stamps() {
    let (_, store) = memory();
    let barrier = Arc::new(Barrier::new(8));
    let winners = Arc::new(AtomicUsize::new(0));
    let before = ready(store.read_stamp()).unwrap();
    let handles: Vec<_> = (0..8)
        .map(|_| {
            let store = store.clone();
            let barrier = barrier.clone();
            let winners = winners.clone();
            std::thread::spawn(move || {
                barrier.wait();
                let lease = store.runtime_attachment().try_claim().ok();
                if lease.is_some() {
                    winners.fetch_add(1, Ordering::SeqCst);
                }
                barrier.wait();
                drop(lease);
            })
        })
        .collect();
    for handle in handles {
        handle.join().unwrap();
    }
    assert_eq!(winners.load(Ordering::SeqCst), 1);
    let _lease = store.runtime_attachment().try_claim().unwrap();
    assert_eq!(before, ready(store.read_stamp()).unwrap());
}
#[test]
fn actual_codec_keeps_custom_roles_and_header_policy() {
    let (_, store) = memory();
    let options=SessionCodecOptions::from_role_schemas([("note".into(), serde_json::from_value(serde_json::json!({"type":"object","required":["text"],"properties":{"text":{"type":"string"}}})).unwrap())].into()).unwrap();
    let codec = Arc::new(
        SessionCodec::new(
            options,
            ProviderHeaderPolicy::from_allowlist(["x-public".into()]),
        )
        .unwrap(),
    );
    let session = session(store, codec.clone());
    let message: AgentMessage =
        serde_json::from_value(serde_json::json!({"role":"note","text":"kept"})).unwrap();
    assert!(session.codec().validate_message(&message).is_ok());
    assert!(self::codec().validate_message(&message).is_err());
    assert!(std::ptr::eq(session.codec(), codec.as_ref()));
    assert!(std::ptr::eq(
        session.codec().header_policy(),
        codec.header_policy()
    ));
}

// A custom RuntimeStorage can provide stamps while forbidding complete snapshots
// after Session construction. This distinguishes the additive capability from a
// Session implementation which silently falls back to AtomicStorage::snapshot.
struct Bounded<S> {
    inner: Arc<S>,
    snapshots: AtomicUsize,
    admitted_snapshots: usize,
}
impl<S: Storage> Storage for Bounded<S> {
    fn commit(&self, t: Transaction) -> BoxFuture<'_, Result<CommitResult, StorageError>> {
        self.inner.commit(t)
    }
    fn get_entries<'a>(
        &'a self,
        ids: &'a [EntryId],
    ) -> BoxFuture<'a, Result<EntryMap, StorageError>> {
        self.inner.get_entries(ids)
    }
    fn get_register<'a>(
        &'a self,
        n: RegisterNamespace,
        k: &'a str,
    ) -> BoxFuture<'a, Result<Option<Register>, StorageError>> {
        self.inner.get_register(n, k)
    }
    fn list_registers<'a>(
        &'a self,
        n: RegisterNamespace,
        p: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Vec<Register>, StorageError>> {
        self.inner.list_registers(n, p)
    }
    fn scan_branch(&self, q: BranchScan) -> BoxFuture<'_, Result<Vec<Entry>, StorageError>> {
        self.inner.scan_branch(q)
    }
    fn scan_branch_structure(
        &self,
        q: BranchScan,
    ) -> BoxFuture<'_, Result<Vec<EntryStructure>, StorageError>> {
        self.inner.scan_branch_structure(q)
    }
    fn scan_entries(&self, q: EntryScan) -> BoxFuture<'_, Result<Vec<Entry>, StorageError>> {
        self.inner.scan_entries(q)
    }
    fn scan_usage(&self, q: UsageScan) -> BoxFuture<'_, Result<Vec<UsageRow>, StorageError>> {
        self.inner.scan_usage(q)
    }
    fn stats(&self) -> BoxFuture<'_, Result<SessionStats, StorageError>> {
        panic!("bounded runtime read called stats")
    }
    fn close(&self) -> BoxFuture<'_, Result<(), StorageError>> {
        self.inner.close()
    }
}
impl<S: AtomicStorage> AtomicStorage for Bounded<S> {
    fn snapshot(&self) -> BoxFuture<'_, Result<StorageSnapshot, StorageError>> {
        assert!(
            self.snapshots.fetch_add(1, Ordering::SeqCst) < self.admitted_snapshots,
            "bounded runtime read called complete snapshot"
        );
        self.inner.snapshot()
    }
    fn commit_conditional(
        &self,
        request: ConditionalCommit,
    ) -> BoxFuture<'_, Result<ConditionalOutcome, StorageError>> {
        self.inner.commit_conditional(request)
    }
}
impl<S: CheckedStorage> CheckedStorage for Bounded<S> {
    fn commit_checked<'a, V: PublicationValidator + ?Sized>(
        &'a self,
        request: ConditionalCommit,
        validator: &'a V,
    ) -> BoxFuture<'a, Result<ConditionalOutcome, CheckedCommitError<V::Error>>> {
        self.inner.commit_checked(request, validator)
    }
}
impl<S: RuntimeStorage> RuntimeStorage for Bounded<S> {
    fn runtime_attachment(&self) -> &RuntimeAttachment {
        self.inner.runtime_attachment()
    }
    fn read_stamp(&self) -> BoxFuture<'_, Result<ReadStamp, StorageError>> {
        self.inner.read_stamp()
    }
}
#[test]
fn session_bounded_stamp_has_no_snapshot_or_stats_fallback() {
    let (_, inner) = memory();
    let backend = Arc::new(Bounded {
        inner: inner.clone(),
        snapshots: AtomicUsize::new(0),
        admitted_snapshots: 2,
    });
    let first = session(backend.clone(), codec());
    let second = session(backend.clone(), codec());
    let lease = first.claim_runtime().unwrap();
    assert!(second.claim_runtime().is_err());
    let before = ready(first.read_stamp()).unwrap();
    ready(first.get_register(RegisterNamespace::FactCustom, "root")).unwrap();
    ready(inner.commit(tx(vec![set("root")]))).unwrap();
    ready(second.get_register(RegisterNamespace::FactCustom, "root")).unwrap();
    assert_ne!(before, ready(second.read_stamp()).unwrap());
    assert_eq!(backend.snapshots.load(Ordering::SeqCst), 2);
    drop(lease);
    let _lease = second.claim_runtime().unwrap();
}

#[cfg(unix)]
#[test]
fn jsonl_stamp_rejects_replaced_physical_identity() {
    let dir = Dir::new();
    let path = dir.0.join("session.jsonl");
    let store = JsonlStorage::create(
        &path,
        metadata(),
        codec(),
        Arc::new(Clock),
        JsonlOptions::default(),
    )
    .unwrap();
    let _stamp = ready(store.read_stamp()).unwrap();
    std::fs::rename(&path, dir.0.join("retired.jsonl")).unwrap();
    std::fs::write(&path, b"replacement").unwrap();
    assert!(matches!(
        ready(store.read_stamp()),
        Err(StorageError::Conflict(_))
    ));
    assert!(matches!(
        ready(store.stats()),
        Err(StorageError::Conflict(_))
    ));
    ready(store.close()).unwrap();
}
