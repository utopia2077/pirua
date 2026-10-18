#![allow(dead_code)]
use pi_durable_session::*;
use std::{
    path::PathBuf,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicU8, AtomicU64, Ordering},
    },
};
pub fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
}
pub struct Temp {
    pub path: PathBuf,
}
impl Temp {
    pub fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        Self {
            path: std::env::temp_dir().join(format!(
                "pi-durable-session-{}-{}.sqlite",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            )),
        }
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        for path in [
            self.path.clone(),
            self.path.with_extension("sqlite-wal"),
            self.path.with_extension("sqlite-shm"),
        ] {
            let _ = std::fs::remove_file(path);
        }
    }
}
pub fn backends() -> Vec<(&'static str, Arc<dyn Storage>, Option<Temp>)> {
    let temp = Temp::new();
    vec![
        ("memory", Arc::new(MemoryStorage::new()), None),
        (
            "sqlite",
            Arc::new(SqliteStorage::open(&temp.path).unwrap()),
            Some(temp),
        ),
    ]
}
#[derive(Default)]
pub struct Gate {
    state: Mutex<(bool, bool)>,
    cv: Condvar,
}
impl Gate {
    pub fn enter(&self) {
        let mut s = self.state.lock().unwrap();
        s.0 = true;
        self.cv.notify_all();
        while !s.1 {
            s = self.cv.wait(s).unwrap();
        }
    }
    pub fn wait_entered(&self) {
        let mut s = self.state.lock().unwrap();
        while !s.0 {
            s = self.cv.wait(s).unwrap();
        }
    }
    pub fn release(&self) {
        let mut s = self.state.lock().unwrap();
        s.1 = true;
        self.cv.notify_all();
    }
}
pub struct Probe {
    pub inner: Arc<dyn Storage>,
    pub mode: AtomicU8,
    pub writes: Mutex<Vec<Vec<StorageWrite>>>,
    pub gate: Mutex<Option<Arc<Gate>>>,
}
impl Probe {
    pub fn new(inner: Arc<dyn Storage>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            mode: AtomicU8::new(0),
            writes: Mutex::new(Vec::new()),
            gate: Mutex::new(None),
        })
    }
    pub fn fail(&self, mode: u8) {
        self.mode.store(mode, Ordering::SeqCst);
    }
    pub fn hold(&self) -> Arc<Gate> {
        let gate = Arc::new(Gate::default());
        *self.gate.lock().unwrap() = Some(gate.clone());
        gate
    }
}
macro_rules! forward {($($name:ident($($arg:ident:$ty:ty),*)->$result:ty;)+)=>{$(fn $name(&self,$($arg:$ty),*)->$result{self.inner.$name($($arg),*)})+};}
impl Storage for Probe {
    fn commit(&self, writes: &[StorageWrite]) -> Result<Seq> {
        self.writes.lock().unwrap().push(writes.to_vec());
        let gate = self.gate.lock().unwrap().take();
        if let Some(gate) = gate {
            gate.enter();
        }
        let mode = self.mode.swap(0, Ordering::SeqCst);
        if mode == 1 {
            return Err(Error::backend("injected before commit"));
        }
        let seq = self.inner.commit(writes)?;
        if mode == 2 {
            return Err(Error::backend("injected ambiguous outcome"));
        }
        Ok(seq)
    }
    fn mint_id(&self) -> Result<u64> {
        if self.mode.load(Ordering::SeqCst) == 3 {
            return Err(Error::backend("injected mint failure"));
        }
        self.inner.mint_id()
    }
    fn conversation(&self, id: ConversationId) -> Result<Option<ConversationRecord>> {
        if self.mode.load(Ordering::SeqCst) == 4 {
            return Err(Error::backend("injected read failure"));
        }
        self.inner.conversation(id)
    }
    forward! {
        scan_conversations(q:&ConversationQuery,n:usize,c:Option<&Cursor>)->Result<Page<ConversationRecord>>;
        entry(id:EntryId)->Result<Option<StoredEntry>>;
        visible_entry(c:ConversationId,id:EntryId)->Result<Option<StoredEntry>>;
        find_latest_head_marker(c:ConversationId,at:Option<EntryId>)->Result<Option<EntryRecord>>;
        scan_entries(q:&EntryQuery,n:usize,c:Option<&Cursor>)->Result<Page<EntryRecord>>;
        task(id:TaskId)->Result<Option<TaskRecord>>;
        scan_tasks(q:&TaskQuery,n:usize,c:Option<&Cursor>)->Result<Page<TaskRecord>>;
        submission(id:SubmissionId)->Result<Option<SubmissionRecord>>;
        scan_submissions(q:&SubmissionQuery,n:usize,c:Option<&Cursor>)->Result<Page<SubmissionRecord>>;
        submission_by_request(c:ConversationId,r:&str)->Result<Option<SubmissionRecord>>;
        find_document(a:&DocumentAddress,at:DocumentPoint)->Result<Option<DocumentRecord>>;
        document(id:DocumentId,at:DocumentPoint)->Result<Option<StoredDocument>>;
        scan_documents(q:&DocumentQuery,n:usize,c:Option<&Cursor>)->Result<Page<DocumentRecord>>;
        close()->Result<()>;
    }
}
