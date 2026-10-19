//! Immutable committed observation. User delivery always happens off the Session line.
use crate::{Error, Result, delta::Op, records::*};
use serde_json::Value;
use std::{
    collections::{BTreeMap, VecDeque},
    future::Future,
    sync::{Arc, Mutex, Weak},
};
use tokio::sync::{Notify, watch};

#[derive(Clone, Debug, PartialEq)]
pub enum CommitChange {
    Table(StorageWrite),
    Document {
        record: DocumentRecord,
        conversation_id: Option<ConversationId>,
        version: Option<u32>,
        value: Arc<Value>,
        ops: Vec<Op>,
        deltas_since_base: usize,
    },
    DocumentCopy {
        record: DocumentRecord,
        conversation_id: ConversationId,
        source: DocumentCopySource,
        version: u32,
        value: Arc<Value>,
    },
}
#[derive(Clone, Debug, PartialEq)]
pub struct CommitPublication {
    pub seq: Seq,
    pub changes: Vec<CommitChange>,
}
#[derive(Clone, Debug, PartialEq)]
pub struct DocumentFrame {
    pub seq: Seq,
    pub value: Arc<Value>,
    pub ops: Vec<Op>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WatchEnd {
    Stopped,
    Cancelled,
    SessionClosed,
    Retired,
    ListenerError(String),
    SessionFailed(Error),
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SessionEnd {
    Closed,
    Failed(Error),
}

struct StateData {
    value: Arc<Value>,
    sequence: u64,
    end: Option<WatchEnd>,
    subscribers: Vec<Weak<Queue<StateFrame>>>,
}
pub(crate) struct StateInner {
    pub id: DocumentId,
    data: Mutex<StateData>,
}
#[derive(Clone, Debug, PartialEq)]
pub struct StateFrame {
    pub sequence: u64,
    pub value: Arc<Value>,
    pub ops: Vec<Op>,
}
pub struct StateSubscription {
    pub baseline: StateFrame,
    queue: Arc<Queue<StateFrame>>,
}
impl StateSubscription {
    pub fn try_next(&mut self) -> Option<Arc<StateFrame>> {
        self.queue.try_next()
    }
    pub async fn next(&mut self) -> Option<Arc<StateFrame>> {
        self.queue.next().await
    }
    pub fn stop(&self) {
        self.queue.close();
    }
}
impl Drop for StateSubscription {
    fn drop(&mut self) {
        self.stop()
    }
}
#[derive(Clone)]
pub struct DocumentState {
    pub(crate) inner: Arc<StateInner>,
}
impl DocumentState {
    pub(crate) fn new(id: DocumentId, value: Arc<Value>) -> Self {
        Self {
            inner: Arc::new(StateInner {
                id,
                data: Mutex::new(StateData {
                    value,
                    sequence: 0,
                    end: None,
                    subscribers: Vec::new(),
                }),
            }),
        }
    }
    pub fn id(&self) -> DocumentId {
        self.inner.id
    }
    pub fn value(&self) -> Arc<Value> {
        self.inner.data.lock().expect("state lock").value.clone()
    }
    pub fn sequence(&self) -> u64 {
        self.inner.data.lock().expect("state lock").sequence
    }
    pub fn end(&self) -> Option<WatchEnd> {
        self.inner.data.lock().expect("state lock").end.clone()
    }
    pub fn dispose(&self) {
        self.inner.finish(WatchEnd::Stopped)
    }
    /// Atomically attaches an exact local delivery stream, including its fixed baseline.
    pub fn subscribe(&self) -> StateSubscription {
        let mut data = self.inner.data.lock().expect("state lock");
        let baseline = StateFrame {
            sequence: data.sequence,
            value: data.value.clone(),
            ops: Vec::new(),
        };
        let queue = Queue::new();
        if data.end.is_some() {
            queue.close();
        } else {
            data.subscribers.push(Arc::downgrade(&queue));
        }
        StateSubscription { baseline, queue }
    }
}
impl StateInner {
    pub(crate) fn advance(&self, value: Arc<Value>, ops: Vec<Op>) {
        let mut data = self.data.lock().expect("state lock");
        if data.end.is_some() {
            return;
        }
        data.sequence += 1;
        data.value = value.clone();
        let frame = Arc::new(StateFrame {
            sequence: data.sequence,
            value: value.clone(),
            ops,
        });
        data.subscribers.retain(|q| q.strong_count() > 0);
        for queue in data.subscribers.iter().filter_map(Weak::upgrade) {
            queue.push(frame.clone());
            if value.is_null() {
                queue.close();
            }
        }
        if value.is_null() {
            data.end = Some(WatchEnd::Retired);
        }
    }
    pub(crate) fn finish(&self, reason: WatchEnd) {
        let mut data = self.data.lock().expect("state lock");
        if data.end.is_none() {
            data.end = Some(reason);
            for queue in data.subscribers.drain(..).filter_map(|q| q.upgrade()) {
                queue.close();
            }
        }
    }
}
struct WatchData {
    value: Arc<Value>,
    pending: VecDeque<DocumentFrame>,
    end: Option<WatchEnd>,
    retiring: bool,
    mode: u8,
}
pub(crate) struct WatchInner {
    pub id: DocumentId,
    data: Mutex<WatchData>,
    notify: Notify,
    ended: watch::Sender<Option<WatchEnd>>,
    delivery: tokio::sync::Mutex<()>,
}
#[derive(Clone)]
pub struct DocumentWatch {
    pub(crate) inner: Arc<WatchInner>,
}
impl DocumentWatch {
    pub(crate) fn new(id: DocumentId, value: Arc<Value>) -> Self {
        Self {
            inner: Arc::new(WatchInner {
                id,
                data: Mutex::new(WatchData {
                    value,
                    pending: VecDeque::new(),
                    end: None,
                    retiring: false,
                    mode: 0,
                }),
                notify: Notify::new(),
                ended: watch::channel(None).0,
                delivery: tokio::sync::Mutex::new(()),
            }),
        }
    }
    pub fn id(&self) -> DocumentId {
        self.inner.id
    }
    /// Acquisition revision until the first delivery; later the most recently delivered revision.
    pub fn value(&self) -> Arc<Value> {
        self.inner.data.lock().expect("watch lock").value.clone()
    }
    pub fn end(&self) -> Option<WatchEnd> {
        self.inner.data.lock().expect("watch lock").end.clone()
    }
    pub fn stop(&self) -> WatchEnd {
        self.inner.finish(WatchEnd::Stopped);
        self.end().expect("stopped watch")
    }
    pub fn cancel(&self) -> WatchEnd {
        self.inner.finish(WatchEnd::Cancelled);
        self.end().expect("cancelled watch")
    }
    pub async fn closed(&self) -> WatchEnd {
        let mut rx = self.inner.ended.subscribe();
        loop {
            if let Some(end) = rx.borrow_and_update().clone() {
                return end;
            }
            if rx.changed().await.is_err() {
                return WatchEnd::Stopped;
            }
        }
    }
    /// Pull delivery serializes callers. A terminal frame is delivered before retirement is reported.
    pub async fn next(&self) -> Result<Option<DocumentFrame>> {
        {
            let mut data = self.inner.data.lock().expect("watch lock");
            if data.mode == 2 {
                return Err(Error::validation("watch already has a listener"));
            }
            data.mode = 1;
        }
        let _delivery = self.inner.delivery.lock().await;
        let next = self.inner.next_raw().await;
        if next.as_ref().is_some_and(|f| f.value.is_null()) {
            self.inner.finish(WatchEnd::Retired);
        }
        Ok(next)
    }
    /// Installs one serialized async listener; its failure terminates only this watch.
    pub fn start<F, Fut>(&self, mut listener: F) -> Result<()>
    where
        F: FnMut(DocumentFrame) -> Fut + Send + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        {
            let mut data = self.inner.data.lock().expect("watch lock");
            if data.mode != 0 || data.end.is_some() {
                return Err(Error::validation("watch already started or ended"));
            }
            data.mode = 2;
        }
        let inner = self.inner.clone();
        let worker_inner = inner.clone();
        let worker = tokio::spawn(async move {
            while let Some(frame) = worker_inner.next_raw().await {
                let retired = frame.value.is_null();
                listener(frame).await?;
                if retired {
                    worker_inner.finish(WatchEnd::Retired);
                    break;
                }
            }
            Ok::<_, Error>(())
        });
        tokio::spawn(async move {
            match worker.await {
                Ok(Ok(())) => (),
                Ok(Err(error)) => inner.finish(WatchEnd::ListenerError(error.to_string())),
                Err(error) => inner.finish(WatchEnd::ListenerError(error.to_string())),
            }
        });
        Ok(())
    }
}
impl WatchInner {
    pub(crate) fn advance(&self, frame: DocumentFrame) {
        let mut data = self.data.lock().expect("watch lock");
        if data.end.is_some() || data.retiring {
            return;
        }
        if frame.value.is_null() {
            data.retiring = true;
        }
        if data.pending.len() >= 100 {
            data.pending.clear();
            data.pending.push_back(DocumentFrame {
                seq: frame.seq,
                ops: vec![Op::Replace((*frame.value).clone())],
                value: frame.value,
            });
        } else {
            data.pending.push_back(frame);
        }
        self.notify.notify_one();
    }
    pub(crate) fn finish(&self, reason: WatchEnd) {
        let mut data = self.data.lock().expect("watch lock");
        if data.end.is_some() {
            return;
        }
        data.end = Some(reason.clone());
        data.pending.clear();
        self.ended.send_replace(Some(reason));
        self.notify.notify_one();
    }
    async fn next_raw(&self) -> Option<DocumentFrame> {
        loop {
            let notified = self.notify.notified();
            {
                let mut data = self.data.lock().expect("watch lock");
                if data.end.is_some() {
                    return None;
                }
                if let Some(frame) = data.pending.pop_front() {
                    data.value = frame.value.clone();
                    return Some(frame);
                }
            }
            notified.await;
        }
    }
}

pub(crate) struct Queue<T> {
    data: Mutex<(VecDeque<Arc<T>>, bool)>,
    notify: Notify,
}
impl<T> Queue<T> {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            data: Mutex::new((VecDeque::new(), false)),
            notify: Notify::new(),
        })
    }
    pub(crate) fn push(&self, value: Arc<T>) {
        let mut data = self.data.lock().expect("subscription lock");
        if !data.1 {
            data.0.push_back(value);
            self.notify.notify_one();
        }
    }
    pub(crate) fn close(&self) {
        let mut data = self.data.lock().expect("subscription lock");
        data.1 = true;
        self.notify.notify_one();
    }
    pub(crate) fn try_next(&self) -> Option<Arc<T>> {
        self.data.lock().expect("subscription lock").0.pop_front()
    }
    pub(crate) async fn next(&self) -> Option<Arc<T>> {
        loop {
            let notified = self.notify.notified();
            {
                let mut data = self.data.lock().expect("subscription lock");
                if let Some(value) = data.0.pop_front() {
                    return Some(value);
                }
                if data.1 {
                    return None;
                }
            }
            notified.await;
        }
    }
}
pub struct CommitSubscription {
    pub(crate) queue: Arc<Queue<CommitPublication>>,
}
impl CommitSubscription {
    pub fn try_next(&mut self) -> Option<Arc<CommitPublication>> {
        self.queue.try_next()
    }
    pub async fn next(&mut self) -> Option<Arc<CommitPublication>> {
        self.queue.next().await
    }
    pub fn stop(&self) {
        self.queue.close();
    }
}
impl Drop for CommitSubscription {
    fn drop(&mut self) {
        self.stop()
    }
}

/// Fixed selection for an atomic multi-document/active-transcript attachment.
#[derive(Clone, Debug, Default)]
pub struct ObservationQuery {
    pub scopes: Vec<Scope>,
    pub addresses: Vec<DocumentAddress>,
    pub conversation: Option<ConversationId>,
    /// Include all current conversation tasks and publications attributed to that
    /// conversation, even when selected documents/entries have no change.
    /// Requires `conversation`; disabled by default.
    pub conversation_records: bool,
}
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ObservationSnapshot {
    pub conversation: Option<ConversationRecord>,
    pub documents: BTreeMap<DocumentAddress, StoredDocument>,
    pub entries: Vec<EntryRecord>,
    /// Detached current records of all task kinds/statuses in the conversation.
    /// Empty when `ObservationQuery::conversation_records` is disabled.
    pub tasks: BTreeMap<TaskId, TaskRecord>,
}
#[derive(Clone, Debug)]
pub struct ObservationFrame {
    pub publication: Arc<CommitPublication>,
    pub snapshot: Arc<ObservationSnapshot>,
}
pub struct ObservationSubscription {
    pub baseline: Arc<ObservationSnapshot>,
    pub(crate) queue: Arc<Queue<ObservationFrame>>,
}
impl ObservationSubscription {
    pub async fn next(&mut self) -> Option<Arc<ObservationFrame>> {
        self.queue.next().await
    }
    pub fn stop(&self) {
        self.queue.close();
    }
}
impl Drop for ObservationSubscription {
    fn drop(&mut self) {
        self.stop()
    }
}
pub(crate) struct Observer {
    pub query: ObservationQuery,
    pub snapshot: ObservationSnapshot,
    pub history: Vec<EntryRecord>,
    pub queue: Weak<Queue<ObservationFrame>>,
}
impl Observer {
    pub(crate) fn advance(&mut self, publication: Arc<CommitPublication>) {
        let Some(queue) = self.queue.upgrade() else {
            return;
        };
        let mut relevant = false;
        let record_conversation = self
            .query
            .conversation
            .filter(|_| self.query.conversation_records);
        for change in &publication.changes {
            match change {
                CommitChange::Document {
                    record,
                    conversation_id,
                    version,
                    value,
                    deltas_since_base,
                    ..
                } => {
                    if record_conversation.is_some() && record_conversation == *conversation_id {
                        relevant = true;
                    }
                    let address = record.identity.address();
                    if self.selected(&address) {
                        relevant = true;
                        if let Some(version) = version {
                            self.snapshot.documents.insert(
                                address,
                                StoredDocument {
                                    record: record.clone(),
                                    version: *version,
                                    value: (**value).clone(),
                                    deltas_since_base: *deltas_since_base,
                                },
                            );
                        } else if self
                            .snapshot
                            .documents
                            .get(&address)
                            .is_some_and(|d| d.record.identity.id == record.identity.id)
                        {
                            self.snapshot.documents.remove(&address);
                        }
                    }
                }
                CommitChange::DocumentCopy {
                    record,
                    conversation_id,
                    version,
                    value,
                    ..
                } => {
                    if record_conversation == Some(*conversation_id) {
                        relevant = true;
                    }
                    let address = record.identity.address();
                    if self.selected(&address) {
                        relevant = true;
                        self.snapshot.documents.insert(
                            address,
                            StoredDocument {
                                record: record.clone(),
                                version: *version,
                                value: (**value).clone(),
                                deltas_since_base: 0,
                            },
                        );
                    }
                }
                CommitChange::Table(StorageWrite::Task { value })
                    if record_conversation == Some(value.conversation_id) =>
                {
                    relevant = true;
                    self.snapshot.tasks.insert(value.id, value.clone());
                }
                CommitChange::Table(StorageWrite::Submission { value })
                    if record_conversation == Some(value.conversation_id) =>
                {
                    relevant = true;
                }
                CommitChange::Table(StorageWrite::Entry { value })
                    if self.query.conversation == Some(value.conversation_id) =>
                {
                    relevant = true;
                    self.history.push(value.clone());
                }
                CommitChange::Table(StorageWrite::Conversation { value })
                    if self.query.conversation == Some(value.id) =>
                {
                    relevant = true;
                    self.snapshot.conversation = Some(value.clone());
                }
                _ => (),
            }
        }
        if relevant {
            self.snapshot.entries = active_entries(&self.history, None);
            queue.push(Arc::new(ObservationFrame {
                publication,
                snapshot: Arc::new(self.snapshot.clone()),
            }));
        }
    }
    fn selected(&self, address: &DocumentAddress) -> bool {
        self.query.scopes.contains(&address.scope) || self.query.addresses.contains(address)
    }
}
/// Raw active transcript: newest head marker first, then visible non-head entries in its range.
pub fn active_entries(history: &[EntryRecord], cutoff: Option<EntryId>) -> Vec<EntryRecord> {
    let mut sorted = history
        .iter()
        .filter(|e| cutoff.is_none_or(|at| e.id <= at))
        .collect::<Vec<_>>();
    sorted.sort_by_key(|e| e.id);
    let marker = sorted.iter().rev().find(|e| e.head.is_some()).copied();
    let mut result = Vec::new();
    if let Some(marker) = marker {
        result.push(marker.clone());
    }
    result.extend(
        sorted
            .into_iter()
            .filter(|e| {
                e.head.is_none() && marker.is_none_or(|m| e.id >= m.head.expect("head marker"))
            })
            .cloned(),
    );
    result
}
