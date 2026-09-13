#![allow(dead_code, unused_imports)]
pub use pi_agent_schema::session_values::*;
pub use pi_agent_schema::*;
pub use pi_agent_session::{
    backend_support::*,
    codec::SessionCodec,
    jsonl::{JsonlOptions, JsonlStorage},
    memory::*,
    read::*,
    repo::{
        MemorySessionRepo,
        jsonl::{JsonlSessionMetadata, JsonlSessionRepo},
    },
    storage::*,
};
pub use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
pub fn ready<T>(f: impl std::future::Future<Output = T>) -> T {
    let mut c = std::task::Context::from_waker(std::task::Waker::noop());
    match std::pin::pin!(f).as_mut().poll(&mut c) {
        std::task::Poll::Ready(v) => v,
        _ => panic!("actual Memory/JSONL operation suspended"),
    }
}
pub fn n(v: u64) -> SafeUnsigned {
    SafeUnsigned::new(v).unwrap()
}
#[derive(Default)]
pub struct Ids(pub AtomicU64);
impl IdGenerator for Ids {
    fn next(&self, _: Option<SafeUnsigned>) -> String {
        format!(
            "00000000-0001-7000-8000-{:012x}",
            self.0.fetch_add(1, Ordering::SeqCst)
        )
    }
}
pub struct Clock;
impl StorageClock for Clock {
    fn now(&self) -> Result<SafeUnsigned, StorageError> {
        Ok(n(1234))
    }
}
pub fn memory(ids: Arc<Ids>) -> MemorySessionRepo {
    MemorySessionRepo::new(
        SessionCodecOptions::default(),
        ProviderHeaderPolicy::default(),
        ids,
        Arc::new(Clock),
        Arc::new(NoopTelemetryContext),
    )
    .unwrap()
}
pub struct Dir(pub std::path::PathBuf);
impl Dir {
    pub fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let p = std::env::temp_dir().join(format!(
            "pi-read-repo-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
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
pub fn jsonl(dir: &Dir) -> JsonlSessionRepo {
    JsonlSessionRepo::new(
        &dir.0,
        SessionCodecOptions::default(),
        ProviderHeaderPolicy::default(),
        Arc::new(Ids::default()),
        Arc::new(Clock),
        Arc::new(NoopTelemetryContext),
        JsonlOptions::default(),
    )
    .unwrap()
}
pub fn options(id: &str) -> SessionCreateOptions {
    SessionCreateOptions {
        id: Some(SessionId::new(id).unwrap()),
        ..Default::default()
    }
}
pub fn tx(writes: Vec<Write>) -> Transaction {
    Transaction { writes }
}
pub fn custom(id: &str, text: &str) -> Write {
    Write::Entry(NewEntry::Custom(NewCustomEntry {
        id: EntryId::new(id).unwrap(),
        parent_id: None,
        entry_type: CustomEntryType::Custom,
        custom_type: CustomType::new("note").unwrap(),
        data: Presence::Value(serde_json::from_value(serde_json::json!({"text":text})).unwrap()),
    }))
}
pub fn request(cursor: Option<ReadCursor>, limit: u64) -> ReadBatchRequest {
    ReadBatchRequest {
        cursor,
        limit: n(limit),
    }
}
pub fn legacy() -> Vec<u8> {
    b"{\"type\":\"session\",\"version\":3,\"id\":\"legacy\",\"timestamp\":\"1970-01-01T00:00:00Z\",\"cwd\":\"source\"}\n{\"type\":\"custom\",\"id\":\"old\",\"parentId\":null,\"timestamp\":\"1970-01-01T00:00:00.001Z\",\"customType\":\"note\",\"data\":{\"text\":\"before\"}}\n".to_vec()
}
