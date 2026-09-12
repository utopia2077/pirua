#![allow(dead_code)]
pub use pi_agent_schema::session_values::*;
pub use pi_agent_schema::*;
pub use pi_agent_session::{
    memory::StorageClock,
    repo::{MemorySession, MemorySessionRepo},
};
use pi_telemetry::*;
pub use std::sync::atomic::{AtomicU64, Ordering};
pub use std::sync::{Arc, Mutex};

pub fn ready<T>(future: impl std::future::Future<Output = T>) -> T {
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    match std::pin::pin!(future).as_mut().poll(&mut cx) {
        std::task::Poll::Ready(v) => v,
        _ => panic!("Memory repository suspended"),
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
pub fn options(id: &str) -> SessionCreateOptions {
    SessionCreateOptions {
        id: Some(sid(id)),
        ..Default::default()
    }
}
pub fn request(id: &str, fork: ForkOptions) -> SessionForkRequest<SessionCreateOptions> {
    SessionForkRequest {
        fork,
        create: options(id),
    }
}
pub fn branch() -> ForkOptions {
    ForkOptions::Branch(BranchForkOptions::default())
}
pub fn tree() -> ForkOptions {
    ForkOptions::Tree(TreeForkOptions::default())
}
#[derive(Default)]
pub struct Ids(pub AtomicU64);
impl IdGenerator for Ids {
    fn next(&self, timestamp: Option<SafeUnsigned>) -> String {
        let ts = timestamp.map_or(5, |v| v.get());
        format!(
            "{:08x}-{:04x}-7000-8000-{:012x}",
            ts >> 16,
            ts & 65535,
            self.0.fetch_add(1, Ordering::SeqCst)
        )
    }
}
pub struct Clock;
impl StorageClock for Clock {
    fn now(&self) -> Result<SafeUnsigned, StorageError> {
        Ok(n(500))
    }
}
#[derive(Default)]
pub struct Telemetry(pub Mutex<Vec<SpanOptions>>);
impl TelemetryContext for Telemetry {
    fn start_span(&self, options: SpanOptions) -> SpanGuard {
        self.0.lock().unwrap().push(options);
        NOOP_TELEMETRY_CONTEXT.start_span(SpanOptions {
            name: "noop".to_owned(),
            attributes: Default::default(),
        })
    }
}
pub fn repo() -> MemorySessionRepo {
    MemorySessionRepo::new(
        SessionCodecOptions::default(),
        ProviderHeaderPolicy::default(),
        Arc::new(Ids::default()),
        Arc::new(Clock),
        Arc::new(NoopTelemetryContext),
    )
    .unwrap()
}
pub fn custom(id: &str, parent: Option<&str>, data: Presence<JsonValue>) -> Write {
    Write::Entry(NewEntry::Custom(NewCustomEntry {
        id: eid(id),
        parent_id: parent.map(eid),
        entry_type: CustomEntryType::Custom,
        custom_type: CustomType::new("note").unwrap(),
        data,
    }))
}
pub fn leaf(lane: &str, id: Option<&str>) -> Write {
    Write::SetRegister(Box::new(RegisterSet::LaneLeaf {
        key: lane.to_owned(),
        value: id.map(eid),
    }))
}
pub fn state(lane: &str) -> Write {
    Write::SetRegister(Box::new(RegisterSet::LaneState {
        key: lane.to_owned(),
        value: LaneState {
            current_operation_id: None,
            pending_next_run: vec![],
        },
    }))
}
pub fn fact(key: &str, value: JsonValue) -> Write {
    Write::SetRegister(Box::new(RegisterSet::FactCustom {
        key: key.to_owned(),
        value,
    }))
}
pub fn commit(session: &MemorySession, writes: Vec<Write>) -> CommitResult {
    ready(session.commit(Transaction { writes })).unwrap()
}
pub fn entries(session: &MemorySession) -> Vec<Entry> {
    ready(session.find_entries(EntryQuery {
        order: Some(ScanOrder::Ascending),
        ..Default::default()
    }))
    .unwrap()
}
pub fn ids(values: &[Entry]) -> Vec<String> {
    values
        .iter()
        .map(|entry| {
            serde_json::to_value(entry).unwrap()["id"]
                .as_str()
                .unwrap()
                .to_owned()
        })
        .collect()
}
pub fn user(text: &str) -> AgentMessage {
    serde_json::from_value(serde_json::json!({"role":"user","content":text,"timestamp":3})).unwrap()
}
