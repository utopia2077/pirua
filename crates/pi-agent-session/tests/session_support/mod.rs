#![allow(dead_code)]
pub use pi_agent_schema::session_values::*;
pub use pi_agent_schema::*;
pub use pi_agent_session::{codec::SessionCodec, memory::*, session::RuntimeSession, storage::*};
pub use serde_json::json;
use std::{
    future::Future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
};
pub fn n(n: u64) -> SafeUnsigned {
    SafeUnsigned::new(n).unwrap()
}
pub fn eid(s: &str) -> EntryId {
    EntryId::new(s).unwrap()
}
pub fn lane(s: &str) -> LaneId {
    LaneId::new(s).unwrap()
}
pub fn ct(s: &str) -> CustomType {
    CustomType::new(s).unwrap()
}
pub fn tx(writes: Vec<Write>) -> Transaction {
    Transaction { writes }
}
pub fn set(value: RegisterSet) -> Write {
    Write::SetRegister(Box::new(value))
}
pub fn del(namespace: RegisterNamespace, key: &str) -> Write {
    Write::DeleteRegister(RegisterDelete {
        namespace,
        key: key.into(),
    })
}
pub fn ready<F: Future>(future: F) -> F::Output {
    struct Thread(std::thread::Thread);
    impl Wake for Thread {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }
    let waker = Waker::from(Arc::new(Thread(std::thread::current())));
    let mut cx = Context::from_waker(&waker);
    let mut f = std::pin::pin!(future);
    loop {
        match f.as_mut().poll(&mut cx) {
            Poll::Ready(v) => return v,
            Poll::Pending => std::thread::park(),
        }
    }
}
pub struct Clock;
impl StorageClock for Clock {
    fn now(&self) -> Result<SafeUnsigned, StorageError> {
        Ok(n(1234))
    }
}
#[derive(Default)]
pub struct Ids(AtomicU64);
impl IdGenerator for Ids {
    fn next(&self, time: Option<SafeUnsigned>) -> String {
        let t = time.unwrap_or(n(86_400_001)).get();
        let count = self.0.fetch_add(1, Ordering::SeqCst);
        format!("{:08x}-{:04x}-7000-8000-{:012x}", t >> 16, t & 65535, count)
    }
}
pub fn user(text: &str) -> AgentMessage {
    serde_json::from_value(json!({"role":"user","content":text,"timestamp":1})).unwrap()
}
pub fn custom(role: &str, text: &str) -> AgentMessage {
    serde_json::from_value(json!({"role":role,"text":text})).unwrap()
}
pub fn codec() -> Arc<SessionCodec> {
    Arc::new(
        SessionCodec::new(
            SessionCodecOptions::default(),
            ProviderHeaderPolicy::default(),
        )
        .unwrap(),
    )
}
pub fn registered() -> Arc<SessionCodec> {
    Arc::new(SessionCodec::new(SessionCodecOptions::from_role_schemas([(String::from("note"),serde_json::from_value(json!({"type":"object","required":["text"],"properties":{"role":{"const":"note"},"text":{"type":"string","minLength":1}},"additionalProperties":false})).unwrap())].into()).unwrap(),ProviderHeaderPolicy::default()).unwrap())
}
pub fn metadata() -> SessionMetadata {
    SessionMetadata {
        id: SessionId::new("session").unwrap(),
        created_at: n(1),
        storage_version: n(1),
        store_generation: n(0),
        cwd: None,
        parent_session_id: None,
        legacy_parent_session_path: None,
    }
}
pub fn memory() -> (Arc<MemoryBacking>, Arc<MemoryStorage>) {
    let b = Arc::new(MemoryBacking::new());
    let s = b.open(Arc::new(Clock)).unwrap();
    (b, s)
}
pub fn seed<S: Storage>(s: &S, name: &str, leaf: Option<EntryId>) {
    ready(s.commit(tx(vec![
        set(RegisterSet::LaneLeaf {
            key: name.into(),
            value: leaf,
        }),
        set(RegisterSet::LaneState {
            key: name.into(),
            value: LaneState {
                current_operation_id: None,
                pending_next_run: vec![],
            },
        }),
    ])))
    .unwrap();
}
pub fn session<S: CheckedStorage + 'static>(
    s: Arc<S>,
    codec: Arc<SessionCodec>,
) -> RuntimeSession<S> {
    ready(RuntimeSession::new(
        s,
        metadata(),
        codec,
        Arc::new(Ids::default()),
        Arc::new(pi_telemetry::NoopTelemetryContext),
    ))
    .unwrap()
}
pub fn fresh() -> (
    Arc<MemoryBacking>,
    Arc<MemoryStorage>,
    RuntimeSession<MemoryStorage>,
) {
    let (b, s) = memory();
    seed(s.as_ref(), "main", None);
    let sess = session(s.clone(), codec());
    (b, s, sess)
}
pub fn custom_entry(id: &str, parent: Option<&str>) -> Write {
    Write::Entry(NewEntry::Custom(NewCustomEntry {
        id: eid(id),
        parent_id: parent.map(eid),
        entry_type: CustomEntryType::Custom,
        custom_type: ct("state"),
        data: Presence::Missing,
    }))
}
pub fn bytes<S: AtomicStorage>(s: &S) -> Vec<u8> {
    serde_json::to_vec(&ready(s.snapshot()).unwrap()).unwrap()
}

// A real generic CheckedStorage consumer with deliberate interposition. It owns no
// borrowed mutable Memory map and forwards all actual capabilities.
pub struct Probe<S> {
    pub inner: Arc<S>,
    pub before: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    pub branches: Mutex<Vec<BranchScan>>,
    pub attempts: AtomicU64,
}
impl<S> Probe<S> {
    pub fn new(inner: Arc<S>) -> Self {
        Self {
            inner,
            before: Mutex::new(None),
            branches: Mutex::new(vec![]),
            attempts: AtomicU64::new(0),
        }
    }
}
impl<S: Storage> Storage for Probe<S> {
    fn commit(&self, t: Transaction) -> BoxFuture<'_, Result<CommitResult, StorageError>> {
        self.inner.commit(t)
    }
    fn get_entries<'a>(
        &'a self,
        i: &'a [EntryId],
    ) -> BoxFuture<'a, Result<EntryMap, StorageError>> {
        self.inner.get_entries(i)
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
        self.branches.lock().unwrap().push(q.clone());
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
        self.inner.stats()
    }
    fn close(&self) -> BoxFuture<'_, Result<(), StorageError>> {
        self.inner.close()
    }
}
impl<S: AtomicStorage> AtomicStorage for Probe<S> {
    fn snapshot(&self) -> BoxFuture<'_, Result<StorageSnapshot, StorageError>> {
        self.inner.snapshot()
    }
    fn commit_conditional(
        &self,
        r: ConditionalCommit,
    ) -> BoxFuture<'_, Result<ConditionalOutcome, StorageError>> {
        self.inner.commit_conditional(r)
    }
}
impl<S: CheckedStorage> CheckedStorage for Probe<S> {
    fn commit_checked<'a, V: PublicationValidator + ?Sized>(
        &'a self,
        r: ConditionalCommit,
        v: &'a V,
    ) -> BoxFuture<'a, Result<ConditionalOutcome, CheckedCommitError<V::Error>>> {
        Box::pin(async move {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            let callback = self.before.lock().unwrap().take();
            if let Some(callback) = callback {
                callback();
            }
            self.inner.commit_checked(r, v).await
        })
    }
}
