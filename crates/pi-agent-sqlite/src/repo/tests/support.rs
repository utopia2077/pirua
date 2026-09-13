#![allow(dead_code, unused_imports)]
pub use super::super::*;
pub use pi_agent_session::read::{ReadBatchRequest, ReadCursor, RepositoryRead};
pub use serde_json::{Value, json};
pub use std::sync::{
    Barrier,
    atomic::{AtomicUsize, Ordering},
};
pub fn ready<T>(f: impl std::future::Future<Output = T>) -> T {
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    match std::pin::pin!(f).as_mut().poll(&mut cx) {
        std::task::Poll::Ready(v) => v,
        _ => panic!("actual operation must finish in first poll"),
    }
}
pub struct Dir(pub PathBuf);
impl Dir {
    pub fn new() -> Self {
        static N: AtomicUsize = AtomicUsize::new(0);
        let p = std::env::temp_dir().join(format!(
            "pi-sqlite-repo-{}-{}",
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
pub struct Clock;
impl StorageClock for Clock {
    fn now(&self) -> Result<SafeUnsigned, StorageError> {
        Ok(n(500))
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
pub fn codec() -> Arc<SessionCodec> {
    Arc::new(
        SessionCodec::new(
            SessionCodecOptions::default(),
            ProviderHeaderPolicy::default(),
        )
        .unwrap(),
    )
}
pub fn repo(path: &Path) -> SqliteSessionRepo {
    SqliteSessionRepo::with_codec(
        path,
        codec(),
        Arc::new(Ids::default()),
        Arc::new(Clock),
        Arc::new(NoopTelemetryContext),
        SqliteOptions::default(),
    )
    .unwrap()
}
pub fn options(id: &str) -> SessionCreateOptions {
    SessionCreateOptions {
        id: Some(SessionId::new(id).unwrap()),
        ..Default::default()
    }
}
pub fn append(s: &SqliteSession, id: &str, text: &str) {
    ready(s.commit(Transaction {
        writes: vec![Write::Entry(NewEntry::Message(NewMessageEntry {
                id: EntryId::new(id).unwrap(),
                parent_id: None,
                entry_type: MessageEntryType::Message,
                message: serde_json::from_value(
                    json!({"role":"user","content":text,"timestamp":9999}),
                )
                .unwrap(),
                terminate: None,
            }))],
    }))
    .unwrap();
}
pub fn fixture() -> Value {
    serde_json::from_str(include_str!(
        "../../../../../conformance/session/sqlite/repository/frozen544.json"
    ))
    .unwrap()
}
pub fn snapshot(clean: bool) -> (SessionMetadata, StorageSnapshot) {
    let f = fixture();
    let source = &f["source_snapshot"];
    let mut registers = source["registers"].as_array().unwrap().clone();
    if clean {
        registers.retain(|v| {
            !v["namespace"].as_str().unwrap().starts_with("op.")
                && v["namespace"] != "pending.entry"
        });
    }
    let snapshot = StorageSnapshot {
        entries: serde_json::from_value(source["entries"].clone()).unwrap(),
        registers: serde_json::from_value(registers.into()).unwrap(),
        usage: serde_json::from_value(source["usage"].clone()).unwrap(),
        next_seq: Some(n(46)),
        storage_version: n(1),
        store_generation: n(0),
        stats: SessionStats {
            message_count: n(2),
            usage: serde_json::from_value(source["usage"][0]["usage"].clone()).unwrap(),
        },
    };
    (
        serde_json::from_value(source["metadata"].clone()).unwrap(),
        snapshot,
    )
}
pub fn seeded(dir: &Dir, clean: bool) -> SqliteSessionMetadata {
    let (metadata, snapshot) = snapshot(clean);
    let path = dir.0.join("arbitrary-source.sqlite");
    let s = SqliteStorage::create_from_snapshot(
        &path,
        metadata.clone(),
        snapshot,
        codec(),
        Arc::new(Clock),
        SqliteOptions::default(),
    )
    .unwrap();
    ready(s.close()).unwrap();
    SqliteSessionMetadata {
        base: metadata,
        path,
    }
}
pub fn observe(m: &SqliteSessionMetadata) -> SnapshotObservation {
    SqliteStorage::read_snapshot(&m.path, &codec(), SqliteOptions::default()).unwrap()
}
pub fn hook(
    r: &SqliteSessionRepo,
    f: impl Fn(&str, &Path) -> Result<(), SessionError> + Send + Sync + 'static,
) {
    *r.inner.hook.lock().unwrap() = Some(Arc::new(f));
}
pub fn filenames(d: &Dir) -> Vec<String> {
    let mut v: Vec<_> = std::fs::read_dir(&d.0)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    v.sort();
    v
}
pub fn changed(o: &SnapshotObservation) -> StorageSnapshot {
    let mut v = serde_json::to_value(&o.snapshot.entries).unwrap();
    v.as_array_mut()
        .unwrap()
        .retain(|e| ["A", "C", "D"].contains(&e["id"].as_str().unwrap()));
    for e in v.as_array_mut().unwrap() {
        if e["id"] == "D" {
            e["parentId"] = json!("A");
            e["message"]["content"] = json!("[redacted]");
        }
        if e["id"] == "C" {
            e["summary"] = json!("[redacted]");
            e["retainedTail"] = json!([]);
        }
    }
    let mut s = o.snapshot.clone();
    s.entries = serde_json::from_value(v).unwrap();
    s.registers
        .retain(|r| !matches!(r,Register::FactLabel{key,..}if key=="B"||key=="SIDE"));
    s
}
pub fn ids(s: &StorageSnapshot) -> Vec<String> {
    s.entries
        .iter()
        .map(|e| entry_structure(e).id.to_string())
        .collect()
}
