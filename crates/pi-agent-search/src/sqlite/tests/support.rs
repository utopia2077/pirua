#![allow(dead_code, unused_imports)]
pub use super::super::*;
pub use pi_agent_schema::session_values::*;
pub use pi_agent_schema::*;
pub use pi_agent_session::{
    backend_support::*,
    codec::SessionCodec,
    jsonl::JsonlOptions,
    read::*,
    repo::{MemorySessionRepo, jsonl::JsonlSessionRepo},
};
pub use serde_json::{Value, json};
use std::path::{Path, PathBuf};
pub use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
pub fn ready<T>(f: impl std::future::Future<Output = T>) -> T {
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    match std::pin::pin!(f).as_mut().poll(&mut cx) {
        std::task::Poll::Ready(v) => v,
        _ => panic!("search must complete on first poll"),
    }
}
pub fn n(v: u64) -> SafeUnsigned {
    SafeUnsigned::new(v).unwrap()
}
pub fn sid(v: &str) -> SessionId {
    SessionId::new(v).unwrap()
}
pub struct Clock;
impl StorageClock for Clock {
    fn now(&self) -> Result<SafeUnsigned, StorageError> {
        Ok(n(1234))
    }
}
#[derive(Default)]
pub struct Ids(AtomicUsize);
impl IdGenerator for Ids {
    fn next(&self, _: Option<SafeUnsigned>) -> String {
        format!(
            "00000000-0001-7000-8000-{:012x}",
            self.0.fetch_add(1, Ordering::SeqCst)
        )
    }
}
pub struct Dir(pub PathBuf);
impl Dir {
    pub fn new() -> Self {
        static N: AtomicUsize = AtomicUsize::new(0);
        let p = std::env::temp_dir().join(format!(
            "pi-search-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir(&p).unwrap();
        Self(p)
    }
}
impl Drop for Dir {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
pub fn memory() -> Arc<MemorySessionRepo> {
    Arc::new(
        MemorySessionRepo::new(
            SessionCodecOptions::default(),
            ProviderHeaderPolicy::default(),
            Arc::new(Ids::default()),
            Arc::new(Clock),
            Arc::new(NoopTelemetryContext),
        )
        .unwrap(),
    )
}
pub fn corpus() -> Value {
    serde_json::from_str(include_str!(
        "../../../../../conformance/search/corpora.json"
    ))
    .unwrap()
}
pub fn codec() -> SessionCodecOptions {
    let mut schemas = std::collections::BTreeMap::new();
    schemas.insert(
        "note".into(),
        serde_json::from_value(corpus()["textCorpus"]["customSchemas"]["note"].clone()).unwrap(),
    );
    // Explicit local schema translating pinned messages.ts BashExecutionMessage;
    // optional exitCode represents its source number|undefined field.
    schemas.insert("bashExecution".into(),serde_json::from_value(json!({"type":"object","required":["role","command","output","cancelled","truncated","timestamp"],"properties":{"role":{"const":"bashExecution"},"command":{"type":"string"},"output":{"type":"string"},"exitCode":{"type":"number"},"cancelled":{"type":"boolean"},"truncated":{"type":"boolean"},"timestamp":{"type":"integer"},"fullOutputPath":{"type":"string"},"excludeFromContext":{"type":"boolean"}},"additionalProperties":false})).unwrap());
    SessionCodecOptions::from_role_schemas(schemas).unwrap()
}
pub fn jsonl(path: &Path) -> Arc<JsonlSessionRepo> {
    Arc::new(
        JsonlSessionRepo::new(
            path,
            codec(),
            ProviderHeaderPolicy::default(),
            Arc::new(Ids::default()),
            Arc::new(Clock),
            Arc::new(NoopTelemetryContext),
            JsonlOptions::default(),
        )
        .unwrap(),
    )
}
pub fn create<R: SessionRepo<CreateOptions = SessionCreateOptions>>(
    repo: &R,
    id: &str,
) -> Arc<R::Session> {
    ready(repo.create(SessionCreateOptions {
        id: Some(sid(id)),
        ..Default::default()
    }))
    .unwrap()
}
pub fn write(id: &str, text: &str) -> Write {
    Write::Entry(NewEntry::Message(NewMessageEntry {
        id: EntryId::new(id).unwrap(),
        parent_id: None,
        entry_type: MessageEntryType::Message,
        message: serde_json::from_value(json!({"role":"user","content":text,"timestamp":9999}))
            .unwrap(),
        terminate: None,
    }))
}
pub fn append<S: Session>(s: &S, id: &str, text: &str) {
    ready(s.commit(Transaction {
        writes: vec![write(id, text)],
    }))
    .unwrap();
}
pub fn query(text: &str, limit: Option<u64>) -> crate::SearchQuery {
    crate::SearchQuery {
        text: text.into(),
        limit: limit.map(n),
    }
}
pub fn hits<R: RepositoryRead + 'static>(s: &SqliteSearchService<R>, text: &str) -> Vec<String>
where
    R::ListOptions: Clone + Send + Sync + 'static,
{
    ready(s.search_entries(query(text, None)).unwrap())
        .unwrap()
        .into_iter()
        .map(|h| h.entry_id.as_str().to_owned())
        .collect()
}
pub fn file(path: &Path, id: &str, generation: u64, entries: Vec<Value>) {
    let last = entries
        .iter()
        .map(|e| e["seq"].as_u64().unwrap())
        .max()
        .map_or(0, |n| n + 1);
    let mut rows = vec![
        json!({"v":4,"kind":"header","id":id,"createdAt":0,"storageVersion":1,"storeGeneration":generation,"snapshotItems":entries.len()+2,"nextSeq":last+2}),
    ];
    for mut e in entries {
        e["kind"] = json!("entry");
        rows.push(e);
    }
    rows.push(json!({"kind":"register","op":"set","namespace":"lane.leaf","key":"main","seq":last,"value":null}));
    rows.push(json!({"kind":"register","op":"set","namespace":"lane.state","key":"main","seq":last+1,"value":{"currentOperationId":null,"pendingNextRun":[]}}));
    let text = rows
        .iter()
        .map(|v| serde_json::to_string(v).unwrap() + "\n")
        .collect::<String>();
    std::fs::write(path, text).unwrap();
}
pub fn entry(id: &str, seq: u64, text: &str) -> Value {
    json!({"type":"message","id":id,"parentId":null,"seq":seq,"timestamp":1000+seq,"message":{"role":"user","content":text,"timestamp":9000}})
}
pub fn hook<R: RepositoryRead>(
    s: &SqliteSearchService<R>,
    f: impl Fn(&str) + Send + Sync + 'static,
) {
    *s.inner.hook.lock().unwrap() = Some(Arc::new(f));
}
pub fn clear_hook<R: RepositoryRead>(s: &SqliteSearchService<R>) {
    *s.inner.hook.lock().unwrap() = None;
}
pub fn pause<R: RepositoryRead>(
    s: &SqliteSearchService<R>,
    phase: &'static str,
) -> (std::sync::mpsc::Receiver<()>, std::sync::mpsc::Sender<()>) {
    let (arrive, arrived) = std::sync::mpsc::channel();
    let (resume, resumed) = std::sync::mpsc::channel();
    let resumed = Mutex::new(resumed);
    let once = AtomicUsize::new(0);
    hook(s, move |at| {
        if at == phase && once.fetch_add(1, Ordering::SeqCst) == 0 {
            arrive.send(()).unwrap();
            resumed
                .lock()
                .unwrap()
                .recv_timeout(std::time::Duration::from_secs(20))
                .unwrap();
        }
    });
    (arrived, resume)
}
pub fn wait(r: &std::sync::mpsc::Receiver<()>) {
    r.recv_timeout(std::time::Duration::from_secs(20)).unwrap();
}
