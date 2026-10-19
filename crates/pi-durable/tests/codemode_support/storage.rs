use crate::session::*;
use std::sync::{
    Arc, Condvar, Mutex,
    atomic::{AtomicBool, Ordering},
};
pub struct CaptureStorage {
    pub inner: Arc<dyn Storage>,
    pub writes: Mutex<Vec<StorageWrite>>,
    pub batches: Mutex<Vec<(Seq, Vec<StorageWrite>)>>,
    pub committed_docs: Mutex<std::collections::BTreeMap<DocumentAddress, serde_json::Value>>,
    pub deny: AtomicBool,
    pub pause_count: AtomicBool,
    armed: AtomicBool,
    fail: AtomicBool,
    pub entered: tokio::sync::Notify,
    gate: (Mutex<bool>, Condvar),
}
impl CaptureStorage {
    pub fn new(inner: Arc<dyn Storage>) -> Self {
        Self {
            inner,
            writes: Mutex::new(Vec::new()),
            batches: Mutex::new(Vec::new()),
            committed_docs: Mutex::new(std::collections::BTreeMap::new()),
            deny: AtomicBool::new(false),
            pause_count: AtomicBool::new(false),
            armed: AtomicBool::new(false),
            fail: AtomicBool::new(false),
            entered: Default::default(),
            gate: (Mutex::new(false), Condvar::new()),
        }
    }
    pub fn arm(&self, fail: bool) {
        *self.gate.0.lock().unwrap() = false;
        self.fail.store(fail, Ordering::SeqCst);
        self.armed.store(true, Ordering::SeqCst);
    }
    pub fn release(&self) {
        *self.gate.0.lock().unwrap() = true;
        self.gate.1.notify_all();
    }
}
impl Storage for CaptureStorage {
    fn commit(&self, writes: &[StorageWrite]) -> crate::Result<Seq> {
        if self.deny.swap(false, Ordering::SeqCst) {
            return Err(crate::Error::backend("Native journal commit denied"));
        }
        let count = writes.iter().any(|w| matches!(w, StorageWrite::DocumentChange { content: DocumentContent::Delta { ops, .. }, .. } if ops.iter().any(|op| matches!(op, Op::Set(path,_) if path == &vec!["firstIssued".into()]))));
        if self.armed.swap(false, Ordering::SeqCst)
            || count && self.pause_count.swap(false, Ordering::SeqCst)
        {
            self.entered.notify_one();
            let mut released = self.gate.0.lock().unwrap();
            while !*released {
                released = self.gate.1.wait(released).unwrap();
            }
        }
        let seq = self.inner.commit(writes)?;
        for write in writes {
            let id = match write {
                StorageWrite::DocumentCreate { record, .. }
                | StorageWrite::DocumentCopy { record, .. } => Some(record.id),
                StorageWrite::DocumentChange { id, .. } => Some(*id),
                _ => None,
            };
            if let Some(id) = id
                && let Some(document) = self.inner.document(id, DocumentPoint::Current)?
            {
                self.committed_docs
                    .lock()
                    .unwrap()
                    .insert(document.record.identity.address(), document.value);
            }
        }
        self.writes.lock().unwrap().extend_from_slice(writes);
        self.batches.lock().unwrap().push((seq, writes.to_vec()));
        Ok(seq)
    }
    fn mint_id(&self) -> Result<u64> {
        self.inner.mint_id()
    }
    fn conversation(&self, id: ConversationId) -> Result<Option<ConversationRecord>> {
        self.inner.conversation(id)
    }
    fn scan_conversations(
        &self,
        query: &ConversationQuery,
        limit: usize,
        cursor: Option<&Cursor>,
    ) -> Result<Page<ConversationRecord>> {
        self.inner.scan_conversations(query, limit, cursor)
    }
    fn entry(&self, id: EntryId) -> Result<Option<StoredEntry>> {
        self.inner.entry(id)
    }
    fn visible_entry(
        &self,
        conversation: ConversationId,
        id: EntryId,
    ) -> Result<Option<StoredEntry>> {
        self.inner.visible_entry(conversation, id)
    }
    fn find_latest_head_marker(
        &self,
        conversation: ConversationId,
        cutoff: Option<EntryId>,
    ) -> Result<Option<EntryRecord>> {
        self.inner.find_latest_head_marker(conversation, cutoff)
    }
    fn scan_entries(
        &self,
        query: &EntryQuery,
        limit: usize,
        cursor: Option<&Cursor>,
    ) -> Result<Page<EntryRecord>> {
        self.inner.scan_entries(query, limit, cursor)
    }
    fn task(&self, id: TaskId) -> Result<Option<TaskRecord>> {
        self.inner.task(id)
    }
    fn scan_tasks(
        &self,
        query: &TaskQuery,
        limit: usize,
        cursor: Option<&Cursor>,
    ) -> Result<Page<TaskRecord>> {
        self.inner.scan_tasks(query, limit, cursor)
    }
    fn submission(&self, id: SubmissionId) -> Result<Option<SubmissionRecord>> {
        self.inner.submission(id)
    }
    fn scan_submissions(
        &self,
        query: &SubmissionQuery,
        limit: usize,
        cursor: Option<&Cursor>,
    ) -> Result<Page<SubmissionRecord>> {
        self.inner.scan_submissions(query, limit, cursor)
    }
    fn submission_by_request(
        &self,
        conversation: ConversationId,
        request: &str,
    ) -> Result<Option<SubmissionRecord>> {
        self.inner.submission_by_request(conversation, request)
    }
    fn find_document(
        &self,
        address: &DocumentAddress,
        at: DocumentPoint,
    ) -> Result<Option<DocumentRecord>> {
        self.inner.find_document(address, at)
    }
    fn document(&self, id: DocumentId, at: DocumentPoint) -> Result<Option<StoredDocument>> {
        self.inner.document(id, at)
    }
    fn scan_documents(
        &self,
        query: &DocumentQuery,
        limit: usize,
        cursor: Option<&Cursor>,
    ) -> Result<Page<DocumentRecord>> {
        self.inner.scan_documents(query, limit, cursor)
    }
    fn close(&self) -> Result<()> {
        self.inner.close()
    }
}
