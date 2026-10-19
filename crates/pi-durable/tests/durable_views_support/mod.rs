#![allow(dead_code)]
use pi_durable_session::*;
use serde_json::{Value, json};
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
pub fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
}
pub struct Temp(PathBuf);
impl Drop for Temp {
    fn drop(&mut self) {
        for path in [
            self.0.clone(),
            self.0.with_extension("sqlite-wal"),
            self.0.with_extension("sqlite-shm"),
        ] {
            let _ = std::fs::remove_file(path);
        }
    }
}
pub fn backends() -> Vec<(&'static str, Arc<dyn Storage>, Option<Temp>)> {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let temp = Temp(std::env::temp_dir().join(format!(
        "pi-durable-views-{}-{}.sqlite",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )));
    vec![
        ("memory", Arc::new(MemoryStorage::new()), None),
        (
            "sqlite",
            Arc::new(SqliteStorage::open(&temp.0).unwrap()),
            Some(temp),
        ),
    ]
}
pub fn fixture() -> Value {
    serde_json::from_str(include_str!(
        "../../../../conformance/durable-v1.4.0/fixtures/runtime.json"
    ))
    .unwrap()
}
pub fn user(text: &str) -> Value {
    json!({"role":"user", "content":text, "timestamp":1000})
}
pub fn system(text: &str) -> Value {
    json!({"role":"system", "content":text, "timestamp":1000})
}
pub fn assistant(text: &str) -> Value {
    json!({"role":"assistant", "content":[{"type":"text","text":text}], "api":"oracle-script", "provider":"oracle", "model":"scripted-1", "usage":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"totalTokens":0,"cost":{"input":0.0,"output":0.0,"cacheRead":0.0,"cacheWrite":0.0,"total":0.0}}, "stopReason":"stop", "timestamp":1000})
}
pub async fn root(session: &Session) {
    session
        .commit(|tx| tx.ensure_root_conversation())
        .await
        .unwrap();
}
pub async fn append(
    session: &Session,
    model: Vec<Value>,
    kind: &str,
    head: Option<EntryHead>,
    edits: Option<Vec<ContextEdit>>,
) -> EntryRecord {
    let draft = EntryDraft {
        kind: kind.into(),
        model: Some(model),
        data: None,
        head,
        edits,
    };
    session
        .commit(move |tx| tx.append_entry(ROOT_CONVERSATION_ID, draft))
        .await
        .unwrap()
}
pub fn definition(kind: &str, initial: Value, history: History, fork: ForkPolicy) -> DocDefinition {
    DocDefinition::new(
        kind,
        1,
        DocumentSemantics::Conversation { history, fork },
        false,
        move |_| Ok(initial.clone()),
        |_, _, _| Ok(false),
    )
    .unwrap()
}
pub fn address(kind: &str, conversation: ConversationId) -> DocumentAddress {
    DocumentAddress {
        kind: kind.into(),
        scope: Scope::Conversation {
            conversation_id: conversation,
        },
        key: None,
    }
}
pub async fn frame(
    watch: &pi_durable::view::ConversationViewWatch,
) -> pi_durable::view::ConversationViewFrame {
    tokio::time::timeout(std::time::Duration::from_secs(5), watch.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
}
pub async fn no_frame(watch: &pi_durable::view::ConversationViewWatch) {
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), watch.next())
            .await
            .is_err()
    );
}

/// Blocks the first armed baseline conversation read after the backend read succeeds.
/// Close can therefore seal while Session.observe is already hydrating, rather than
/// merely while an acquisition is queued before admission.
pub struct HydrationGate {
    inner: Arc<dyn Storage>,
    armed: std::sync::atomic::AtomicBool,
    gate: (std::sync::Mutex<(bool, bool)>, std::sync::Condvar),
}
impl HydrationGate {
    pub fn new(inner: Arc<dyn Storage>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            armed: std::sync::atomic::AtomicBool::new(false),
            gate: (
                std::sync::Mutex::new((false, false)),
                std::sync::Condvar::new(),
            ),
        })
    }
    pub fn arm(&self) {
        self.armed.store(true, Ordering::Release);
    }
    pub fn wait_entered(&self) {
        let (lock, condition) = &self.gate;
        let mut gate = lock.lock().unwrap();
        while !gate.0 {
            gate = condition.wait(gate).unwrap();
        }
    }
    pub fn release(&self) {
        let (lock, condition) = &self.gate;
        lock.lock().unwrap().1 = true;
        condition.notify_all();
    }
}
macro_rules! hydration_delegate {
    ($(fn $name:ident(&self $(,$arg:ident:$ty:ty)*) -> $ret:ty;)+) => {
        $(fn $name(&self,$($arg:$ty),*) -> $ret { self.inner.$name($($arg),*) })+
    };
}
impl Storage for HydrationGate {
    fn conversation(&self, id: ConversationId) -> Result<Option<ConversationRecord>> {
        let result = self.inner.conversation(id);
        if self.armed.swap(false, Ordering::AcqRel) {
            let (lock, condition) = &self.gate;
            let mut gate = lock.lock().unwrap();
            gate.0 = true;
            condition.notify_all();
            while !gate.1 {
                gate = condition.wait(gate).unwrap();
            }
        }
        result
    }
    hydration_delegate! {
        fn commit(&self,writes:&[StorageWrite])->Result<Seq>;
        fn mint_id(&self)->Result<u64>;
        fn scan_conversations(&self,query:&ConversationQuery,limit:usize,cursor:Option<&Cursor>)->Result<Page<ConversationRecord>>;
        fn entry(&self,id:EntryId)->Result<Option<StoredEntry>>;
        fn visible_entry(&self,conversation:ConversationId,id:EntryId)->Result<Option<StoredEntry>>;
        fn find_latest_head_marker(&self,conversation:ConversationId,cutoff:Option<EntryId>)->Result<Option<EntryRecord>>;
        fn scan_entries(&self,query:&EntryQuery,limit:usize,cursor:Option<&Cursor>)->Result<Page<EntryRecord>>;
        fn task(&self,id:TaskId)->Result<Option<TaskRecord>>;
        fn scan_tasks(&self,query:&TaskQuery,limit:usize,cursor:Option<&Cursor>)->Result<Page<TaskRecord>>;
        fn submission(&self,id:SubmissionId)->Result<Option<SubmissionRecord>>;
        fn scan_submissions(&self,query:&SubmissionQuery,limit:usize,cursor:Option<&Cursor>)->Result<Page<SubmissionRecord>>;
        fn submission_by_request(&self,conversation:ConversationId,request:&str)->Result<Option<SubmissionRecord>>;
        fn find_document(&self,address:&DocumentAddress,at:DocumentPoint)->Result<Option<DocumentRecord>>;
        fn document(&self,id:DocumentId,at:DocumentPoint)->Result<Option<StoredDocument>>;
        fn scan_documents(&self,query:&DocumentQuery,limit:usize,cursor:Option<&Cursor>)->Result<Page<DocumentRecord>>;
        fn close(&self)->Result<()>;
    }
}
