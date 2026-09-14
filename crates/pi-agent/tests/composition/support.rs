#![allow(dead_code)]
pub use pi_agent::{EntrySearchHit, SearchQuery, SessionSearchService};
pub use pi_agent_schema::session_values::*;
pub use pi_agent_schema::*;
pub use pi_agent_search::{DefaultTextProjector, SearchError, SearchOptions, SqliteSearchService};
pub use pi_agent_session::{
    backend_support::*,
    codec::SessionCodec,
    jsonl::JsonlOptions,
    read::*,
    repo::{MemorySessionRepo, jsonl::JsonlSessionRepo},
};
pub use serde_json::{Value, json};
pub use std::{
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
pub type Metadata<R> = <R as SessionRepo>::Metadata;
pub trait Source:
    SessionRepo<CreateOptions = SessionCreateOptions, ListOptions = ()>
    + RepositoryRead<Metadata = <Self as SessionRepo>::Metadata, ListOptions = ()>
    + 'static
{
    fn physical(&self, metadata: &<Self as SessionRepo>::Metadata) -> Option<Vec<u8>>;
    fn canonical_path(metadata: &<Self as SessionRepo>::Metadata) -> Option<&Path>;
}
impl Source for MemorySessionRepo {
    fn physical(&self, _: &SessionMetadata) -> Option<Vec<u8>> {
        None
    }
    fn canonical_path(_: &SessionMetadata) -> Option<&Path> {
        None
    }
}
impl Source for JsonlSessionRepo {
    fn physical(&self, m: &pi_agent_session::repo::jsonl::JsonlSessionMetadata) -> Option<Vec<u8>> {
        Some(std::fs::read(&m.path).unwrap())
    }
    fn canonical_path(m: &pi_agent_session::repo::jsonl::JsonlSessionMetadata) -> Option<&Path> {
        Some(&m.path)
    }
}
#[cfg(feature = "sqlite")]
impl Source for pi_agent_sqlite::repo::SqliteSessionRepo {
    fn physical(&self, m: &pi_agent_sqlite::repo::SqliteSessionMetadata) -> Option<Vec<u8>> {
        Some(
            serde_json::to_vec(
                &pi_agent_sqlite::SqliteStorage::read_snapshot(
                    &m.path,
                    &codec(),
                    pi_agent_sqlite::SqliteOptions::default(),
                )
                .unwrap()
                .snapshot,
            )
            .unwrap(),
        )
    }
    fn canonical_path(m: &pi_agent_sqlite::repo::SqliteSessionMetadata) -> Option<&Path> {
        Some(&m.path)
    }
}
pub fn n(v: u64) -> SafeUnsigned {
    SafeUnsigned::new(v).unwrap()
}
pub fn sid(v: &str) -> SessionId {
    SessionId::new(v).unwrap()
}
pub fn eid(v: &str) -> EntryId {
    EntryId::new(v).unwrap()
}
pub fn ready<T>(f: impl std::future::Future<Output = T>) -> T {
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    match std::pin::pin!(f).as_mut().poll(&mut cx) {
        std::task::Poll::Ready(v) => v,
        _ => panic!("first-poll completion required"),
    }
}
#[derive(Default)]
pub struct Clock(pub AtomicU64);
impl Clock {
    pub fn set(&self, time: u64) {
        self.0.store(time, Ordering::SeqCst);
    }
}
impl StorageClock for Clock {
    fn now(&self) -> Result<SafeUnsigned, StorageError> {
        Ok(n(self.0.load(Ordering::SeqCst)))
    }
}
pub struct Ids;
impl IdGenerator for Ids {
    fn next(&self, _: Option<SafeUnsigned>) -> String {
        panic!("explicit fixtures and readonly search must not mint host IDs")
    }
}
pub struct Dir(pub PathBuf);
impl Dir {
    pub fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let p = std::env::temp_dir().join(format!(
            "pi-search-composition-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir(&p).unwrap();
        Self(p)
    }
    pub fn sub(&self, name: &str) -> PathBuf {
        let p = self.0.join(name);
        std::fs::create_dir(&p).unwrap();
        p
    }
}
impl Drop for Dir {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
pub fn fixture() -> Value {
    serde_json::from_str(include_str!(
        "../../../../conformance/search/composition/histories.json"
    ))
    .unwrap()
}
pub fn codec() -> Arc<SessionCodec> {
    let schemas =
        serde_json::from_value(fixture()["textHistory"]["customSchemas"].clone()).unwrap();
    Arc::new(
        SessionCodec::new(
            SessionCodecOptions::from_role_schemas(schemas).unwrap(),
            ProviderHeaderPolicy::default(),
        )
        .unwrap(),
    )
}
pub fn memory(clock: Arc<Clock>) -> Arc<MemorySessionRepo> {
    Arc::new(MemorySessionRepo::with_codec(
        codec(),
        Arc::new(Ids),
        clock,
        Arc::new(NoopTelemetryContext),
    ))
}
pub fn jsonl(root: &Path, clock: Arc<Clock>) -> Arc<JsonlSessionRepo> {
    Arc::new(
        JsonlSessionRepo::with_codec(
            root,
            codec(),
            Arc::new(Ids),
            clock,
            Arc::new(NoopTelemetryContext),
            JsonlOptions::default(),
        )
        .unwrap(),
    )
}
#[cfg(feature = "sqlite")]
pub fn sqlite(root: &Path, clock: Arc<Clock>) -> Arc<pi_agent_sqlite::repo::SqliteSessionRepo> {
    Arc::new(
        pi_agent_sqlite::repo::SqliteSessionRepo::with_codec(
            root,
            codec(),
            Arc::new(Ids),
            clock,
            Arc::new(NoopTelemetryContext),
            pi_agent_sqlite::SqliteOptions::default(),
        )
        .unwrap(),
    )
}
pub fn create<R: Source>(r: &R, id: &str) -> Arc<R::Session> {
    ready(r.create(SessionCreateOptions {
        id: Some(sid(id)),
        ..Default::default()
    }))
    .unwrap()
}
pub fn register(ns: RegisterNamespace, key: &str, value: Value) -> Write {
    serde_json::from_value(
        json!({"kind":"register","op":"set","namespace":ns,"key":key,"value":value}),
    )
    .unwrap()
}
pub fn message(id: &str, parent: Option<&str>, text: &str) -> Write {
    serde_json::from_value(json!({"kind":"entry","entry":{"id":id,"parentId":parent,"type":"message","message":{"role":"user","content":text,"timestamp":9000}}})).unwrap()
}
pub fn commit<S: Session + ?Sized>(s: &S, writes: Vec<Write>) -> CommitResult {
    ready(s.commit(Transaction { writes })).unwrap()
}
pub fn append<S: Session + ?Sized>(
    s: &S,
    id: &str,
    parent: Option<&str>,
    text: &str,
) -> CommitResult {
    commit(
        s,
        vec![
            message(id, parent, text),
            register(RegisterNamespace::LaneLeaf, "main", json!(id)),
        ],
    )
}
#[derive(Clone, Debug)]
pub struct ReadObservation {
    pub id: SessionId,
    pub request: Option<ReadCursor>,
    pub result: ReadBatch,
}
pub struct Observed<R: Source> {
    pub source: Arc<R>,
    pub reads: Mutex<Vec<ReadObservation>>,
}
impl<R: Source> Observed<R> {
    pub fn new(source: Arc<R>) -> Arc<Self> {
        Arc::new(Self {
            source,
            reads: Mutex::new(vec![]),
        })
    }
    pub fn take(&self) -> Vec<ReadObservation> {
        std::mem::take(&mut *self.reads.lock().unwrap())
    }
}
impl<R: Source> RepositoryRead for Observed<R> {
    type Metadata = Metadata<R>;
    type ListOptions = ();
    fn source_domain(&self) -> Result<SourceDomain, SessionError> {
        self.source.source_domain()
    }
    fn managed_files(&self) -> Result<Option<ManagedFileDomain>, SessionError> {
        self.source.managed_files()
    }
    fn read_list(&self, o: ()) -> Result<Vec<Self::Metadata>, SessionError> {
        self.source.read_list(o)
    }
    fn read_batch(
        &self,
        m: &Self::Metadata,
        q: ReadBatchRequest,
    ) -> Result<ReadBatch, SessionError> {
        let request = q.cursor.clone();
        let batch = self.source.read_batch(m, q)?;
        self.reads.lock().unwrap().push(ReadObservation {
            id: m.base_metadata().id.clone(),
            request,
            result: batch.clone(),
        });
        Ok(batch)
    }
}
pub fn service<R: Source>(
    path: &Path,
    reader: Arc<Observed<R>>,
) -> SqliteSearchService<Observed<R>> {
    SqliteSearchService::with_options(
        path,
        reader,
        (),
        SearchOptions {
            batch_size: n(2),
            debounce: Duration::ZERO,
            ..Default::default()
        },
        Arc::new(DefaultTextProjector),
    )
    .unwrap()
}
pub fn query(text: &str, limit: Option<u64>) -> SearchQuery {
    SearchQuery {
        text: text.into(),
        limit: limit.map(n),
    }
}
pub fn hits<R: RepositoryRead + 'static>(
    s: &SqliteSearchService<R>,
    text: &str,
    limit: Option<u64>,
) -> Result<Vec<EntrySearchHit>, SearchError>
where
    R::ListOptions: Clone + Send + Sync + 'static,
{
    ready(
        s.search_entries(query(text, limit))
            .expect("implemented entry search"),
    )
}
pub fn normalized(hits: Vec<EntrySearchHit>) -> Vec<Value> {
    hits.into_iter().map(|h|{assert!(h.score.as_ref().is_some_and(|s|s.get().is_finite()));json!({"sessionId":h.session_id,"entryId":h.entry_id,"timestamp":h.timestamp,"snippet":h.snippet})}).collect()
}
pub fn notify_hits<R: RepositoryRead + 'static>(
    s: &SqliteSearchService<R>,
    text: &str,
    wanted: usize,
) -> Vec<EntrySearchHit>
where
    R::ListOptions: Clone + Send + Sync + 'static,
{
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match hits(s, text, None) {
            Ok(h) if h.len() == wanted => return h,
            Ok(_) | Err(SearchError::Busy) => (),
            Err(e) => panic!("unexpected notify query failure {e:?}"),
        }
        assert!(Instant::now() < deadline, "notify watchdog");
        std::thread::sleep(Duration::from_millis(2));
    }
}
pub fn remove_index(path: &Path) {
    for suffix in ["", "-wal", "-shm"] {
        let mut name = path.as_os_str().to_os_string();
        name.push(suffix);
        match std::fs::remove_file(PathBuf::from(name)) {
            Ok(()) => (),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
            Err(e) => panic!("derived removal: {e}"),
        }
    }
}
#[derive(Debug, PartialEq)]
pub struct Canonical {
    pub metadata: SessionMetadata,
    pub entries: Vec<Entry>,
    pub registers: Vec<Register>,
    pub stats: SessionStats,
    pub physical: Option<Vec<u8>>,
}
pub fn canonical<R: Source>(r: &R, s: &R::Session) -> Canonical {
    let metadata = s.metadata();
    let mut entries = vec![];
    let mut cursor = None;
    loop {
        let b = r
            .read_batch(
                &metadata,
                ReadBatchRequest {
                    cursor,
                    limit: n(100),
                },
            )
            .unwrap();
        entries.extend(b.entries);
        if b.complete {
            break;
        }
        cursor = Some(b.next_cursor);
    }
    let mut registers = vec![];
    for ns in [
        RegisterNamespace::LaneLeaf,
        RegisterNamespace::LaneState,
        RegisterNamespace::FactName,
        RegisterNamespace::FactCustom,
    ] {
        registers.extend(ready(s.list_registers(ns, None)).unwrap());
    }
    Canonical {
        metadata: metadata.base_metadata().clone(),
        entries,
        registers,
        stats: ready(s.get_stats()).unwrap(),
        physical: r.physical(&metadata),
    }
}
