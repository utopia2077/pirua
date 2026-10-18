//! One mutation line, scoped operation staging and commit-before-publication.
use crate::{Error, ErrorKind, Result, apply, delta::Op, observation::*, records::*, storage::*};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::{Semaphore, watch};

type Initial = Arc<dyn Fn(&Value) -> Result<Value> + Send + Sync>;
type Checkpoint = Arc<dyn Fn(&Value, &[Op], CheckpointInfo) -> Result<bool> + Send + Sync>;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DocumentSemantics {
    Session,
    Conversation { history: History, fork: ForkPolicy },
    Task,
}
#[derive(Clone, Copy, Debug)]
pub struct CheckpointInfo {
    pub deltas_since_base: usize,
}
#[derive(Clone)]
pub struct DocDefinition {
    pub kind: String,
    pub version: u32,
    pub semantics: DocumentSemantics,
    pub family: bool,
    initial: Initial,
    checkpoint: Checkpoint,
}
impl DocDefinition {
    pub fn new(
        kind: impl Into<String>,
        version: u32,
        semantics: DocumentSemantics,
        family: bool,
        initial: impl Fn(&Value) -> Result<Value> + Send + Sync + 'static,
        checkpoint: impl Fn(&Value, &[Op], CheckpointInfo) -> Result<bool> + Send + Sync + 'static,
    ) -> Result<Self> {
        let result = Self {
            kind: kind.into(),
            version,
            semantics,
            family,
            initial: Arc::new(initial),
            checkpoint: Arc::new(checkpoint),
        };
        if result.kind.is_empty()
            || version == 0
            || matches!(
                semantics,
                DocumentSemantics::Conversation {
                    history: History::Latest,
                    fork: ForkPolicy::AsOf
                }
            )
        {
            return Err(Error::new(
                ErrorKind::Definition,
                "invalid document definition",
            ));
        }
        Ok(result)
    }
    pub fn address(&self, scope: Scope, key: Option<String>) -> Result<DocumentAddress> {
        let address = DocumentAddress {
            kind: self.kind.clone(),
            scope,
            key,
        };
        self.check_address(&address)?;
        Ok(address)
    }
    fn check_address(&self, address: &DocumentAddress) -> Result<()> {
        let scope = matches!(
            (self.semantics, &address.scope),
            (DocumentSemantics::Session, Scope::Session)
                | (
                    DocumentSemantics::Conversation { .. },
                    Scope::Conversation { .. }
                )
                | (DocumentSemantics::Task, Scope::Task { .. })
        );
        if self.version == 0
            || self.kind.is_empty()
            || matches!(
                self.semantics,
                DocumentSemantics::Conversation {
                    history: History::Latest,
                    fork: ForkPolicy::AsOf
                }
            )
            || !scope
            || address.kind != self.kind
            || self.family != address.key.is_some()
        {
            return Err(Error::new(
                ErrorKind::Definition,
                "document definition/address mismatch",
            ));
        }
        Ok(())
    }
    fn identity(&self, id: DocumentId, address: DocumentAddress) -> DocumentCreate {
        let (history, fork) = match self.semantics {
            DocumentSemantics::Conversation { history, fork } => (Some(history), Some(fork)),
            _ => (None, None),
        };
        DocumentCreate {
            id,
            kind: address.kind,
            scope: address.scope,
            key: address.key,
            history,
            fork,
        }
    }
    fn check_record(&self, record: &DocumentCreate, version: u32) -> Result<()> {
        self.check_address(&record.address())?;
        let expected = self.identity(record.id, record.address());
        if expected != *record || version != self.version {
            return Err(Error::new(
                ErrorKind::Definition,
                "stored definition version or semantics mismatch; migrations are unsupported",
            ));
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Default)]
pub struct TransactionScope {
    pub conversation_id: Option<ConversationId>,
    pub task_id: Option<TaskId>,
}
#[derive(Clone, Copy, Debug)]
pub enum ConversationOwnership {
    Ownerless,
    Task(TaskId),
}
#[derive(Clone, Copy, Debug)]
pub enum TaskOwnership {
    Conversation,
    Task(TaskId),
}
#[derive(Clone, Debug)]
pub enum EntryHead {
    Entry(EntryId),
    Own,
}
#[derive(Clone, Debug)]
pub struct EntryDraft {
    pub kind: String,
    pub model: Option<Vec<Value>>,
    pub data: Option<Value>,
    pub head: Option<EntryHead>,
    pub edits: Option<Vec<ContextEdit>>,
}
impl EntryDraft {
    pub fn new(kind: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            model: None,
            data: None,
            head: None,
            edits: None,
        }
    }
}
#[derive(Clone, Debug)]
pub struct TaskCreate {
    pub kind: String,
    pub version: u32,
    pub input: Value,
    pub checkpoint: Value,
    pub conversation_id: Option<ConversationId>,
    pub ownership: TaskOwnership,
    pub background: bool,
    pub abandon_on_restart: bool,
    pub trace: Option<Value>,
    pub links: Option<Vec<Value>>,
}
#[derive(Clone, Debug)]
pub struct SubmissionCreate {
    pub conversation_id: ConversationId,
    pub submission_type: SubmissionType,
    pub request_id: Option<String>,
    pub trace: Option<Value>,
    pub state: SubmissionState,
}
#[derive(Clone, Debug)]
pub enum SubmissionSettlement {
    Done {
        answer: EntryId,
    },
    Unanswered {
        reason: String,
        detail: Option<Value>,
    },
}

struct Observations {
    states: Vec<Weak<StateInner>>,
    watches: Vec<Weak<WatchInner>>,
    commits: Vec<Weak<Queue<CommitPublication>>>,
    observers: Vec<Observer>,
}
struct Inner {
    storage: Arc<dyn Storage>,
    line: Arc<Semaphore>,
    sealed: AtomicBool,
    failure: Mutex<Option<Error>>,
    observations: Mutex<Observations>,
    end: watch::Sender<Option<SessionEnd>>,
    now: Arc<dyn Fn() -> f64 + Send + Sync>,
}
#[derive(Clone)]
pub struct Session {
    inner: Arc<Inner>,
}
impl Session {
    pub fn open(storage: Arc<dyn Storage>) -> Result<Self> {
        Self::with_clock(storage, || {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs_f64()
                * 1000.0
        })
    }
    pub fn with_clock(
        storage: Arc<dyn Storage>,
        now: impl Fn() -> f64 + Send + Sync + 'static,
    ) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(Inner {
                storage,
                line: Arc::new(Semaphore::new(1)),
                sealed: AtomicBool::new(false),
                failure: Mutex::new(None),
                observations: Mutex::new(Observations {
                    states: Vec::new(),
                    watches: Vec::new(),
                    commits: Vec::new(),
                    observers: Vec::new(),
                }),
                end: watch::channel(None).0,
                now: Arc::new(now),
            }),
        })
    }
    pub async fn commit<T: Send + 'static>(
        &self,
        change: impl FnOnce(&mut Transaction<'_>) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        self.commit_scoped(TransactionScope::default(), change)
            .await
    }
    /// Runtime seam: the caller owns invocation-lease validation inside the callback.
    pub async fn commit_scoped<T: Send + 'static>(
        &self,
        scope: TransactionScope,
        change: impl FnOnce(&mut Transaction<'_>) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        self.inner.assert_open()?;
        let permit = self
            .inner
            .line
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::new(ErrorKind::Closed, "Session line closed"))?;
        self.inner.assert_open()?;
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let mut tx = Transaction::new(inner.storage.as_ref(), scope, inner.now.clone());
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| change(&mut tx)));
            let result = match result {
                Ok(result) => result,
                Err(_) => Err(Error::validation("transaction callback panicked")),
            };
            if let Some(error) = tx.storage_failure.clone() {
                inner.fail(error.clone());
                return Err(error);
            }
            let result = result?;
            let prepared = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| tx.prepare()));
            let prepared = match prepared {
                Ok(result) => result,
                Err(_) => Err(Error::validation(
                    "transaction preparation callback panicked",
                )),
            };
            if let Some(error) = tx.storage_failure.clone() {
                inner.fail(error.clone());
                return Err(error);
            }
            let prepared = prepared?;
            if prepared.writes.is_empty() {
                return Ok(result);
            }
            let seq = match inner.storage.commit(&prepared.writes) {
                Ok(seq) => seq,
                Err(error) => {
                    inner.fail(error.clone());
                    return Err(error);
                }
            };
            let publication = Arc::new(prepared.publication(seq));
            inner.publish(publication);
            Ok(result)
        })
        .await
        .map_err(|e| Error::backend(format!("Session operation failed: {e}")))?
    }
    async fn read<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&dyn Storage) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        self.inner.assert_open()?;
        let permit = self
            .inner
            .line
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::new(ErrorKind::Closed, "Session line closed"))?;
        self.inner.assert_open()?;
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let result = operation(inner.storage.as_ref());
            if let Err(error) = &result
                && error.fatal_read()
            {
                inner.fail(error.clone());
            }
            result
        })
        .await
        .map_err(|e| Error::backend(e.to_string()))?
    }
    pub async fn conversation(&self, id: ConversationId) -> Result<Option<ConversationRecord>> {
        self.read(move |s| s.conversation(id)).await
    }
    pub async fn entry(&self, id: EntryId) -> Result<Option<StoredEntry>> {
        self.read(move |s| s.entry(id)).await
    }
    pub async fn visible_entry(
        &self,
        c: ConversationId,
        id: EntryId,
    ) -> Result<Option<StoredEntry>> {
        self.read(move |s| s.visible_entry(c, id)).await
    }
    pub async fn task(&self, id: TaskId) -> Result<Option<TaskRecord>> {
        self.read(move |s| s.task(id)).await
    }
    pub async fn submission(&self, id: SubmissionId) -> Result<Option<SubmissionRecord>> {
        self.read(move |s| s.submission(id)).await
    }
    pub async fn submission_by_request(
        &self,
        c: ConversationId,
        request: String,
    ) -> Result<Option<SubmissionRecord>> {
        self.read(move |s| s.submission_by_request(c, &request))
            .await
    }
    pub async fn scan_conversations(
        &self,
        q: ConversationQuery,
        n: usize,
        c: Option<Cursor>,
    ) -> Result<Page<ConversationRecord>> {
        self.read(move |s| s.scan_conversations(&q, n, c.as_ref()))
            .await
    }
    pub async fn scan_entries(
        &self,
        q: EntryQuery,
        n: usize,
        c: Option<Cursor>,
    ) -> Result<Page<EntryRecord>> {
        self.read(move |s| s.scan_entries(&q, n, c.as_ref())).await
    }
    pub async fn scan_tasks(
        &self,
        q: TaskQuery,
        n: usize,
        c: Option<Cursor>,
    ) -> Result<Page<TaskRecord>> {
        self.read(move |s| s.scan_tasks(&q, n, c.as_ref())).await
    }
    pub async fn scan_submissions(
        &self,
        q: SubmissionQuery,
        n: usize,
        c: Option<Cursor>,
    ) -> Result<Page<SubmissionRecord>> {
        self.read(move |s| s.scan_submissions(&q, n, c.as_ref()))
            .await
    }
    pub async fn latest_head_marker(
        &self,
        c: ConversationId,
        at: Option<EntryId>,
    ) -> Result<Option<EntryRecord>> {
        self.read(move |s| s.find_latest_head_marker(c, at)).await
    }
    pub async fn history(
        &self,
        c: ConversationId,
        at: Option<EntryId>,
    ) -> Result<Vec<EntryRecord>> {
        self.read(move |s| history(s, c, at)).await
    }
    pub async fn active_entries(
        &self,
        c: ConversationId,
        at: Option<EntryId>,
    ) -> Result<Vec<EntryRecord>> {
        Ok(active_entries(&self.history(c, at).await?, at))
    }
    pub async fn snapshot(
        &self,
        definition: DocDefinition,
        address: DocumentAddress,
    ) -> Result<Option<Value>> {
        self.read(move |s| {
            load_defined(s, &definition, &address, DocumentPoint::Current)
                .map(|v| v.map(|d| d.value))
        })
        .await
    }
    pub async fn snapshot_as_of(
        &self,
        definition: DocDefinition,
        address: DocumentAddress,
        at: EntryId,
    ) -> Result<Option<Value>> {
        self.read(move |s| {
            if !matches!(
                definition.semantics,
                DocumentSemantics::Conversation {
                    history: History::Rewindable,
                    ..
                }
            ) {
                return Err(Error::request(
                    "snapshot_as_of requires rewindable conversation definition",
                ));
            }
            definition.check_address(&address)?;
            let Scope::Conversation { conversation_id } = address.scope else {
                return Err(Error::request("as-of scope must be conversation"));
            };
            let entry = s
                .visible_entry(conversation_id, at)?
                .ok_or_else(|| Error::request("as-of entry is not visible"))?;
            let address = DocumentAddress {
                scope: Scope::Conversation {
                    conversation_id: entry.entry.conversation_id,
                },
                ..address
            };
            load_defined(
                s,
                &definition,
                &address,
                DocumentPoint::At(entry.commit_seq),
            )
            .map(|v| v.map(|d| d.value))
        })
        .await
    }
    pub async fn document_state(
        &self,
        definition: DocDefinition,
        address: DocumentAddress,
    ) -> Result<Option<DocumentState>> {
        let inner = self.inner.clone();
        self.read(move |s| {
            let Some(doc) = load_defined(s, &definition, &address, DocumentPoint::Current)? else {
                return Ok(None);
            };
            let state = DocumentState::new(doc.record.identity.id, Arc::new(doc.value));
            let mut observations = inner.observations.lock().expect("observations lock");
            if inner.sealed.load(Ordering::Acquire) {
                state.inner.finish(inner.watch_end());
            } else {
                observations.states.push(Arc::downgrade(&state.inner));
            }
            Ok(Some(state))
        })
        .await
    }
    pub async fn watch_doc(
        &self,
        definition: DocDefinition,
        address: DocumentAddress,
    ) -> Result<Option<DocumentWatch>> {
        let inner = self.inner.clone();
        self.read(move |s| {
            let Some(doc) = load_defined(s, &definition, &address, DocumentPoint::Current)? else {
                return Ok(None);
            };
            let handle = DocumentWatch::new(doc.record.identity.id, Arc::new(doc.value));
            let mut observations = inner.observations.lock().expect("observations lock");
            if inner.sealed.load(Ordering::Acquire) {
                handle.inner.finish(inner.watch_end());
            } else {
                observations.watches.push(Arc::downgrade(&handle.inner));
            }
            Ok(Some(handle))
        })
        .await
    }
    pub async fn subscribe_commits(&self) -> Result<CommitSubscription> {
        let inner = self.inner.clone();
        self.read(move |_| {
            let queue = Queue::new();
            let mut observations = inner.observations.lock().expect("observations lock");
            if inner.sealed.load(Ordering::Acquire) {
                queue.close();
            } else {
                observations.commits.push(Arc::downgrade(&queue));
            }
            Ok(CommitSubscription { queue })
        })
        .await
    }
    /// Atomic baseline + subscription for a later runtime-owned mounted view.
    /// Final values (including definition-free copies) are captured before another commit.
    pub async fn observe(&self, query: ObservationQuery) -> Result<ObservationSubscription> {
        let inner = self.inner.clone();
        self.read(move |s| {
            let mut documents = BTreeMap::new();
            for scope in &query.scopes {
                for record in all_documents(s, scope.clone(), DocumentPoint::Current)? {
                    if let Some(doc) = s.document(record.identity.id, DocumentPoint::Current)? {
                        documents.insert(record.identity.address(), doc);
                    }
                }
            }
            for address in &query.addresses {
                if let Some(record) = s.find_document(address, DocumentPoint::Current)?
                    && let Some(doc) = s.document(record.identity.id, DocumentPoint::Current)?
                {
                    documents.insert(address.clone(), doc);
                }
            }
            let (conversation, entries) = if let Some(id) = query.conversation {
                (
                    Some(
                        s.conversation(id)?
                            .ok_or_else(|| Error::request("unknown observed conversation"))?,
                    ),
                    history(s, id, None)?,
                )
            } else {
                (None, Vec::new())
            };
            let snapshot = ObservationSnapshot {
                conversation,
                documents,
                entries: active_entries(&entries, None),
            };
            let baseline = Arc::new(snapshot.clone());
            let queue = Queue::new();
            let mut observations = inner.observations.lock().expect("observations lock");
            if inner.sealed.load(Ordering::Acquire) {
                queue.close();
            } else {
                observations.observers.push(Observer {
                    query,
                    snapshot,
                    history: entries,
                    queue: Arc::downgrade(&queue),
                });
            }
            Ok(ObservationSubscription { baseline, queue })
        })
        .await
    }
    /// Seals immediately when polled; admitted work settles independently of this wait.
    pub async fn close(&self) -> Result<()> {
        if !self.inner.sealed.swap(true, Ordering::AcqRel) {
            self.inner.seal_observations(WatchEnd::SessionClosed);
            let inner = self.inner.clone();
            tokio::spawn(async move {
                let permit = inner.line.clone().acquire_owned().await;
                match permit {
                    Ok(_permit) => {
                        // All admitted work has now settled and published. Complete
                        // commit subscribers end here, after the document seal.
                        inner.close_commit_subscriptions();
                        match inner.storage.close() {
                            Ok(()) => {
                                if inner.failure.lock().expect("failure lock").is_none() {
                                    inner.end.send_replace(Some(SessionEnd::Closed));
                                }
                            }
                            Err(error) => inner.fail(error),
                        }
                    }
                    Err(_) => inner.fail(Error::backend("Session line closed")),
                }
            });
        }
        match self.closed().await {
            SessionEnd::Closed => Ok(()),
            SessionEnd::Failed(error) => Err(error),
        }
    }
    pub async fn closed(&self) -> SessionEnd {
        let mut rx = self.inner.end.subscribe();
        loop {
            if let Some(end) = rx.borrow_and_update().clone() {
                return end;
            }
            if rx.changed().await.is_err() {
                return SessionEnd::Closed;
            }
        }
    }
    pub fn failure(&self) -> Option<Error> {
        self.inner.failure.lock().expect("failure lock").clone()
    }
}
impl Inner {
    fn assert_open(&self) -> Result<()> {
        if let Some(error) = self.failure.lock().expect("failure lock").as_ref() {
            return Err(Error::new(
                ErrorKind::Failed,
                format!("Session failed: {error}"),
            ));
        }
        if self.sealed.load(Ordering::Acquire) {
            return Err(Error::new(ErrorKind::Closed, "Session admission is sealed"));
        }
        Ok(())
    }
    fn watch_end(&self) -> WatchEnd {
        self.failure
            .lock()
            .expect("failure lock")
            .as_ref()
            .map_or(WatchEnd::SessionClosed, |e| {
                WatchEnd::SessionFailed(e.clone())
            })
    }
    fn fail(&self, error: Error) {
        {
            let mut failure = self.failure.lock().expect("failure lock");
            if failure.is_some() {
                return;
            }
            *failure = Some(error.clone());
        }
        self.sealed.store(true, Ordering::Release);
        self.seal_observations(WatchEnd::SessionFailed(error.clone()));
        self.close_commit_subscriptions();
        let _ = self.storage.close();
        self.end.send_replace(Some(SessionEnd::Failed(error)));
    }
    fn seal_observations(&self, end: WatchEnd) {
        let mut obs = self.observations.lock().expect("observations lock");
        for s in obs.states.drain(..).filter_map(|s| s.upgrade()) {
            s.finish(end.clone());
        }
        for w in obs.watches.drain(..).filter_map(|w| w.upgrade()) {
            w.finish(end.clone());
        }
        for o in obs.observers.drain(..) {
            if let Some(q) = o.queue.upgrade() {
                q.close();
            }
        }
    }
    fn close_commit_subscriptions(&self) {
        let mut obs = self.observations.lock().expect("observations lock");
        for q in obs.commits.drain(..).filter_map(|q| q.upgrade()) {
            q.close();
        }
    }
    fn publish(&self, publication: Arc<CommitPublication>) {
        let mut obs = self.observations.lock().expect("observations lock");
        if self.sealed.load(Ordering::Acquire) {
            // Normal close seals document observers immediately, while an already
            // admitted successful commit still reaches existing commit subscribers.
            obs.commits.retain(|q| q.strong_count() > 0);
            for q in obs.commits.iter().filter_map(Weak::upgrade) {
                q.push(publication.clone());
            }
            return;
        }
        obs.states.retain(|s| s.strong_count() > 0);
        obs.watches.retain(|s| s.strong_count() > 0);
        obs.commits.retain(|s| s.strong_count() > 0);
        obs.observers.retain(|o| o.queue.strong_count() > 0);
        for change in &publication.changes {
            if let CommitChange::Document {
                record, value, ops, ..
            } = change
            {
                for state in obs
                    .states
                    .iter()
                    .filter_map(Weak::upgrade)
                    .filter(|s| s.id == record.identity.id)
                {
                    state.advance(
                        value.clone(),
                        if value.is_null() {
                            vec![Op::Replace(Value::Null)]
                        } else {
                            ops.clone()
                        },
                    );
                }
                for handle in obs
                    .watches
                    .iter()
                    .filter_map(Weak::upgrade)
                    .filter(|w| w.id == record.identity.id)
                {
                    handle.advance(DocumentFrame {
                        seq: publication.seq,
                        value: value.clone(),
                        ops: if value.is_null() {
                            vec![Op::Replace(Value::Null)]
                        } else {
                            ops.clone()
                        },
                    });
                }
            }
        }
        for q in obs.commits.iter().filter_map(Weak::upgrade) {
            q.push(publication.clone());
        }
        for observer in &mut obs.observers {
            observer.advance(publication.clone());
        }
    }
}
fn load_defined(
    s: &dyn Storage,
    definition: &DocDefinition,
    address: &DocumentAddress,
    at: DocumentPoint,
) -> Result<Option<StoredDocument>> {
    definition.check_address(address)?;
    let Some(record) = s.find_document(address, at)? else {
        return Ok(None);
    };
    let Some(doc) = s.document(record.identity.id, at)? else {
        return Ok(None);
    };
    definition.check_record(&record.identity, doc.version)?;
    Ok(Some(doc))
}
fn history(s: &dyn Storage, c: ConversationId, at: Option<EntryId>) -> Result<Vec<EntryRecord>> {
    let query = EntryQuery {
        conversation_id: c,
        min_entry_id: None,
        max_entry_id: at,
        order: Some(ScanOrder::Ascending),
    };
    let mut cursor = None;
    let mut result = Vec::new();
    loop {
        let page = s.scan_entries(&query, 256, cursor.as_ref())?;
        result.extend(page.items);
        cursor = page.next;
        if cursor.is_none() {
            return Ok(result);
        }
    }
}
fn all_documents(s: &dyn Storage, scope: Scope, at: DocumentPoint) -> Result<Vec<DocumentRecord>> {
    let q = DocumentQuery {
        scope,
        at,
        kind: None,
    };
    let mut cursor = None;
    let mut result = Vec::new();
    loop {
        let page = s.scan_documents(&q, 256, cursor.as_ref())?;
        result.extend(page.items);
        cursor = page.next;
        if cursor.is_none() {
            return Ok(result);
        }
    }
}

struct StagedDocument {
    record: DocumentCreate,
    committed: Option<DocumentRecord>,
    definition: Option<DocDefinition>,
    version: u32,
    value: Value,
    ops: Vec<Op>,
    deltas: usize,
    created: bool,
    copy: Option<DocumentCopySource>,
    retire: bool,
    acquired: bool,
    checkpointed: bool,
    base: Value,
    tracked: bool,
}
struct Prepared {
    writes: Vec<StorageWrite>,
    tables: Vec<StorageWrite>,
    documents: Vec<StagedDocument>,
    conversations: BTreeMap<TaskId, ConversationId>,
}
impl Prepared {
    fn publication(self, seq: Seq) -> CommitPublication {
        let mut changes = self
            .tables
            .into_iter()
            .map(CommitChange::Table)
            .collect::<Vec<_>>();
        for doc in self.documents {
            let record = DocumentRecord {
                identity: doc.record.clone(),
                created_at: doc.committed.as_ref().map_or(seq, |r| r.created_at),
                retired_at: if doc.retire { Some(seq) } else { None },
            };
            let conversation_id = match doc.record.scope {
                Scope::Conversation { conversation_id } => Some(conversation_id),
                Scope::Task { task_id } => self.conversations.get(&task_id).copied(),
                Scope::Session => None,
            };
            if let Some(source) = doc.copy
                && !doc.acquired
                && !doc.retire
            {
                changes.push(CommitChange::DocumentCopy {
                    record,
                    conversation_id: conversation_id.expect("copy conversation"),
                    source,
                    version: doc.version,
                    value: Arc::new(doc.value),
                });
                continue;
            }
            changes.push(CommitChange::Document {
                record,
                conversation_id,
                version: if doc.retire { None } else { Some(doc.version) },
                value: Arc::new(if doc.retire { Value::Null } else { doc.value }),
                deltas_since_base: if doc.checkpointed {
                    0
                } else {
                    doc.deltas + usize::from(!doc.ops.is_empty())
                },
                ops: if doc.created || doc.retire {
                    Vec::new()
                } else {
                    doc.ops
                },
            });
        }
        CommitPublication { seq, changes }
    }
}
/// Scoped mutable staging. No reference to mutable document JSON is exposed.
pub struct Transaction<'a> {
    storage: &'a dyn Storage,
    scope: TransactionScope,
    now: Arc<dyn Fn() -> f64 + Send + Sync>,
    table_written: bool,
    tables: Vec<StorageWrite>,
    tasks: BTreeMap<TaskId, TaskRecord>,
    created_tasks: BTreeSet<TaskId>,
    submissions: BTreeMap<SubmissionId, SubmissionRecord>,
    documents: Vec<StagedDocument>,
    latest: BTreeMap<DocumentAddress, usize>,
    fork_sources: BTreeSet<ConversationId>,
    copy_sources: BTreeSet<DocumentId>,
    storage_failure: Option<Error>,
}
impl<'a> Transaction<'a> {
    fn new(
        storage: &'a dyn Storage,
        scope: TransactionScope,
        now: Arc<dyn Fn() -> f64 + Send + Sync>,
    ) -> Self {
        Self {
            storage,
            scope,
            now,
            table_written: false,
            tables: Vec::new(),
            tasks: BTreeMap::new(),
            created_tasks: BTreeSet::new(),
            submissions: BTreeMap::new(),
            documents: Vec::new(),
            latest: BTreeMap::new(),
            fork_sources: BTreeSet::new(),
            copy_sources: BTreeSet::new(),
            storage_failure: None,
        }
    }
    fn read_result<T>(&mut self, result: Result<T>) -> Result<T> {
        if let Err(error) = &result
            && error.fatal_read()
        {
            self.storage_failure.get_or_insert_with(|| error.clone());
        }
        result
    }
    fn table_read(&self) -> Result<()> {
        if self.table_written {
            Err(Error::new(
                ErrorKind::ReadAfterWrite,
                "table reads must precede table writes",
            ))
        } else {
            Ok(())
        }
    }
    fn mint(&mut self) -> Result<u64> {
        match self.storage.mint_id() {
            Ok(id) => Ok(id),
            Err(error) => {
                self.storage_failure.get_or_insert_with(|| error.clone());
                Err(error)
            }
        }
    }
    pub fn conversation(&mut self, id: ConversationId) -> Result<Option<ConversationRecord>> {
        self.table_read()?;
        self.read_result(self.storage.conversation(id))
    }
    pub fn entry(&mut self, id: EntryId) -> Result<Option<StoredEntry>> {
        self.table_read()?;
        self.read_result(self.storage.entry(id))
    }
    pub fn task(&mut self, id: TaskId) -> Result<Option<TaskRecord>> {
        self.table_read()?;
        self.read_result(self.storage.task(id))
    }
    pub fn submission(&mut self, id: SubmissionId) -> Result<Option<SubmissionRecord>> {
        self.table_read()?;
        self.read_result(self.storage.submission(id))
    }
    pub fn submission_by_request(
        &mut self,
        c: ConversationId,
        r: &str,
    ) -> Result<Option<SubmissionRecord>> {
        self.table_read()?;
        self.read_result(self.storage.submission_by_request(c, r))
    }
    pub fn scan_conversations(
        &mut self,
        q: &ConversationQuery,
        n: usize,
        c: Option<&Cursor>,
    ) -> Result<Page<ConversationRecord>> {
        self.table_read()?;
        self.read_result(self.storage.scan_conversations(q, n, c))
    }
    pub fn scan_entries(
        &mut self,
        q: &EntryQuery,
        n: usize,
        c: Option<&Cursor>,
    ) -> Result<Page<EntryRecord>> {
        self.table_read()?;
        self.read_result(self.storage.scan_entries(q, n, c))
    }
    pub fn scan_tasks(
        &mut self,
        q: &TaskQuery,
        n: usize,
        c: Option<&Cursor>,
    ) -> Result<Page<TaskRecord>> {
        self.table_read()?;
        self.read_result(self.storage.scan_tasks(q, n, c))
    }
    pub fn latest_head_marker(&mut self, c: ConversationId) -> Result<Option<EntryRecord>> {
        self.table_read()?;
        self.read_result(self.storage.find_latest_head_marker(c, None))
    }
    fn current_task(&mut self, id: TaskId) -> Result<Option<TaskRecord>> {
        if let Some(task) = self.tasks.get(&id) {
            Ok(Some(task.clone()))
        } else {
            self.read_result(self.storage.task(id))
        }
    }
    fn require_conversation(&mut self, id: ConversationId) -> Result<()> {
        if self
            .tables
            .iter()
            .any(|w| matches!(w,StorageWrite::Conversation {value} if value.id==id))
        {
            return Ok(());
        }
        if self.read_result(self.storage.conversation(id))?.is_some() {
            Ok(())
        } else {
            Err(Error::validation("conversation does not exist"))
        }
    }
    /// Idempotent reserved-root initialization, invoked by the runtime before admission.
    pub fn ensure_root_conversation(&mut self) -> Result<ConversationRecord> {
        if let Some(value) = self.tables.iter().find_map(|w| match w {
            StorageWrite::Conversation { value } if value.id == ROOT_CONVERSATION_ID => {
                Some(value.clone())
            }
            _ => None,
        }) {
            return Ok(value);
        }
        if let Some(value) = self.read_result(self.storage.conversation(ROOT_CONVERSATION_ID))? {
            return Ok(value);
        }
        self.table_written = true;
        let value = ConversationRecord {
            id: ROOT_CONVERSATION_ID,
            parent: None,
            owner: None,
        };
        self.tables.push(StorageWrite::Conversation {
            value: value.clone(),
        });
        Ok(value)
    }
    pub fn create_conversation(
        &mut self,
        ownership: ConversationOwnership,
    ) -> Result<ConversationRecord> {
        self.stage_conversation(None, ownership)
    }
    fn stage_conversation(
        &mut self,
        parent: Option<ConversationParent>,
        ownership: ConversationOwnership,
    ) -> Result<ConversationRecord> {
        self.table_written = true;
        let id = ConversationId::new(self.mint()?)?;
        let owner = match ownership {
            ConversationOwnership::Ownerless => None,
            ConversationOwnership::Task(id) => {
                let task = self
                    .current_task(id)?
                    .ok_or_else(|| Error::validation("conversation owner task missing"))?;
                Some(ConversationOwner {
                    conversation_id: task.conversation_id,
                    task_id: id,
                })
            }
        };
        let record = ConversationRecord { id, parent, owner };
        self.tables.push(StorageWrite::Conversation {
            value: record.clone(),
        });
        Ok(record)
    }
    pub fn fork_conversation(
        &mut self,
        parent: ConversationId,
        at: EntryId,
        ownership: ConversationOwnership,
    ) -> Result<ConversationRecord> {
        let entry = self
            .read_result(self.storage.visible_entry(parent, at))?
            .ok_or_else(|| Error::validation("fork entry not visible"))?;
        let child = self.stage_conversation(
            Some(ConversationParent {
                conversation_id: parent,
                at,
            }),
            ownership,
        )?;
        self.fork_sources.insert(parent);
        let sources = [
            (
                Scope::Conversation {
                    conversation_id: entry.entry.conversation_id,
                },
                DocumentPoint::At(entry.commit_seq),
                ForkPolicy::AsOf,
            ),
            (
                Scope::Conversation {
                    conversation_id: parent,
                },
                DocumentPoint::Current,
                ForkPolicy::Current,
            ),
        ];
        let mut addresses = BTreeSet::new();
        for (scope, point, policy) in sources {
            let records = self.read_result(all_documents(self.storage, scope, point))?;
            for source_record in records
                .into_iter()
                .filter(|r| r.identity.fork == Some(policy))
            {
                let stored = self
                    .read_result(self.storage.document(source_record.identity.id, point))?
                    .ok_or_else(|| Error::validation("fork source vanished"))?;
                let id = DocumentId::new(self.mint()?)?;
                let mut record = source_record.identity;
                let source = DocumentCopySource {
                    id: record.id,
                    at: point,
                };
                record.id = id;
                record.scope = Scope::Conversation {
                    conversation_id: child.id,
                };
                if !addresses.insert(record.address()) {
                    return Err(Error::validation(
                        "fork selects duplicate document addresses",
                    ));
                }
                self.copy_sources.insert(source.id);
                let index = self.documents.len();
                self.latest.insert(record.address(), index);
                self.documents.push(StagedDocument {
                    record,
                    committed: None,
                    definition: None,
                    version: stored.version,
                    value: stored.value.clone(),
                    ops: Vec::new(),
                    deltas: 0,
                    created: true,
                    copy: Some(source),
                    retire: false,
                    acquired: false,
                    checkpointed: true,
                    base: stored.value,
                    tracked: true,
                });
            }
        }
        Ok(child)
    }
    pub fn append_entry(&mut self, c: ConversationId, draft: EntryDraft) -> Result<EntryRecord> {
        self.table_written = true;
        self.require_conversation(c)?;
        let id = EntryId::new(self.mint()?)?;
        let record = EntryRecord {
            id,
            conversation_id: c,
            kind: draft.kind,
            model: draft.model,
            data: draft.data,
            head: draft.head.map(|h| match h {
                EntryHead::Entry(id) => id,
                EntryHead::Own => id,
            }),
            edits: draft.edits,
            by_task_id: self.scope.task_id,
        };
        self.tables.push(StorageWrite::Entry {
            value: record.clone(),
        });
        Ok(record)
    }
    pub fn create_task(&mut self, create: TaskCreate) -> Result<TaskId> {
        self.table_written = true;
        let owner = match create.ownership {
            TaskOwnership::Conversation => None,
            TaskOwnership::Task(id) => Some(id),
        };
        let owner_conversation = if let Some(owner) = owner {
            Some(
                self.current_task(owner)?
                    .ok_or_else(|| Error::validation("task owner missing"))?
                    .conversation_id,
            )
        } else {
            None
        };
        let conversation_id = create
            .conversation_id
            .or(owner_conversation)
            .or(self.scope.conversation_id)
            .ok_or_else(|| Error::validation("task conversation required"))?;
        if owner_conversation.is_some_and(|id| id != conversation_id) {
            return Err(Error::validation(
                "child task must share owner conversation",
            ));
        }
        self.require_conversation(conversation_id)?;
        let id = TaskId::new(self.mint()?)?;
        let record = TaskRecord {
            id,
            conversation_id,
            kind: create.kind,
            version: create.version,
            input: create.input,
            trace: create.trace,
            links: create.links,
            owner,
            background: create.background,
            abort_requested: false,
            abort_reason: None,
            abandon_on_restart: if create.abandon_on_restart {
                Some(true)
            } else {
                None
            },
            started_at: None,
            ended_at: None,
            restarts: None,
            state: TaskState::Pending {
                checkpoint: create.checkpoint,
            },
            memos: None,
        };
        self.tasks.insert(id, record);
        self.created_tasks.insert(id);
        Ok(id)
    }
    /// Low-level runtime-only replacement seam; execution/lease policy stays outside Session.
    pub fn replace_task(&mut self, mut candidate: TaskRecord) -> Result<()> {
        self.table_written = true;
        let current = self
            .current_task(candidate.id)?
            .ok_or_else(|| Error::validation("task replacement requires existing task"))?;
        if current.state.is_terminal() {
            return Err(Error::validation("terminal task cannot change"));
        }
        if current.conversation_id != candidate.conversation_id
            || current.owner != candidate.owner
            || current.background != candidate.background
            || current.kind != candidate.kind
            || current.input != candidate.input
            || current.version != candidate.version
            || current.abandon_on_restart != candidate.abandon_on_restart
            || current.trace != candidate.trace
            || current.links != candidate.links
        {
            return Err(Error::validation("task immutable fields changed"));
        }
        candidate.started_at = current.started_at.or_else(|| {
            if matches!(candidate.state, TaskState::Running { .. }) {
                Some((self.now)())
            } else {
                None
            }
        });
        candidate.ended_at = if candidate.state.is_terminal() {
            Some((self.now)())
        } else {
            None
        };
        if !candidate.state.can_own() {
            candidate.memos = None;
        }
        self.tasks.insert(candidate.id, candidate);
        Ok(())
    }
    pub fn staged_tasks(&self) -> Vec<TaskRecord> {
        self.tasks.values().cloned().collect()
    }
    pub fn create_submission(&mut self, create: SubmissionCreate) -> Result<SubmissionRecord> {
        self.table_written = true;
        self.require_conversation(create.conversation_id)?;
        let id = SubmissionId::new(self.mint()?)?;
        let record = SubmissionRecord {
            id,
            conversation_id: create.conversation_id,
            submission_type: create.submission_type,
            request_id: create.request_id,
            trace: create.trace,
            state: create.state,
        };
        record.validate()?;
        self.submissions.insert(id, record.clone());
        Ok(record)
    }
    fn current_submission(&mut self, id: SubmissionId) -> Result<SubmissionRecord> {
        if let Some(value) = self.submissions.get(&id) {
            Ok(value.clone())
        } else {
            self.read_result(self.storage.submission(id))?
                .ok_or_else(|| Error::validation("submission missing"))
        }
    }
    pub fn place_submission(&mut self, id: SubmissionId, entry: EntryId) -> Result<()> {
        self.table_written = true;
        let mut record = self.current_submission(id)?;
        if record.state.settled() {
            return Ok(());
        }
        if record.state != SubmissionState::Queued {
            return Err(Error::validation("submission is not queued"));
        }
        record.state = if record.submission_type == SubmissionType::Input {
            SubmissionState::Placed { entry }
        } else {
            SubmissionState::Done {
                entry,
                answer: None,
            }
        };
        self.submissions.insert(id, record);
        Ok(())
    }
    pub fn settle_submission(
        &mut self,
        id: SubmissionId,
        settlement: SubmissionSettlement,
    ) -> Result<()> {
        self.table_written = true;
        let mut record = self.current_submission(id)?;
        if record.state.settled() {
            return Ok(());
        }
        record.state = match settlement {
            SubmissionSettlement::Done { answer } => match record.state {
                SubmissionState::Placed { entry }
                    if record.submission_type == SubmissionType::Input =>
                {
                    SubmissionState::Done {
                        entry,
                        answer: Some(answer),
                    }
                }
                _ => return Err(Error::validation("only placed input can be answered")),
            },
            SubmissionSettlement::Unanswered { reason, detail } => SubmissionState::Unanswered {
                entry: match record.state {
                    SubmissionState::Placed { entry } => Some(entry),
                    _ => None,
                },
                reason,
                detail,
            },
        };
        record.validate()?;
        self.submissions.insert(id, record);
        Ok(())
    }
    pub fn doc(
        &mut self,
        definition: &DocDefinition,
        address: DocumentAddress,
        seed: Value,
    ) -> Result<DocumentId> {
        definition.check_address(&address)?;
        match address.scope {
            Scope::Conversation { conversation_id } => {
                self.require_conversation(conversation_id)?
            }
            Scope::Task { task_id } => {
                let task = self
                    .current_task(task_id)?
                    .ok_or_else(|| Error::validation("document task missing"))?;
                if task.state.is_terminal() {
                    return Err(Error::validation(
                        "terminal task document cannot be acquired",
                    ));
                }
            }
            Scope::Session => (),
        }
        if let Some(&index) = self.latest.get(&address) {
            let doc = &mut self.documents[index];
            if !doc.retire {
                definition.check_record(&doc.record, doc.version)?;
                doc.definition = Some(definition.clone());
                doc.acquired = true;
                return Ok(doc.record.id);
            }
        }
        let replaced = self.latest.contains_key(&address);
        let stored = if replaced {
            None
        } else {
            self.read_result(load_defined(
                self.storage,
                definition,
                &address,
                DocumentPoint::Current,
            ))?
        };
        let doc = if let Some(stored) = stored {
            StagedDocument {
                record: stored.record.identity.clone(),
                committed: Some(stored.record),
                definition: Some(definition.clone()),
                version: stored.version,
                value: stored.value.clone(),
                ops: Vec::new(),
                deltas: stored.deltas_since_base,
                created: false,
                copy: None,
                retire: false,
                acquired: true,
                checkpointed: false,
                base: stored.value,
                tracked: true,
            }
        } else {
            let value = (definition.initial)(&seed)?;
            if !value.is_object() {
                return Err(Error::validation("document initial must return object"));
            }
            let id = DocumentId::new(self.mint()?)?;
            StagedDocument {
                record: definition.identity(id, address.clone()),
                committed: None,
                definition: Some(definition.clone()),
                version: definition.version,
                value: value.clone(),
                ops: Vec::new(),
                deltas: 0,
                created: true,
                copy: None,
                retire: false,
                acquired: true,
                checkpointed: true,
                base: value,
                tracked: true,
            }
        };
        let id = doc.record.id;
        self.latest.insert(address, self.documents.len());
        self.documents.push(doc);
        Ok(id)
    }
    pub fn document_value(&self, id: DocumentId) -> Result<&Value> {
        self.documents
            .iter()
            .find(|d| d.record.id == id && d.acquired)
            .map(|d| &d.value)
            .ok_or_else(|| Error::validation("document is not acquired"))
    }
    pub fn apply(&mut self, id: DocumentId, op: Op) -> Result<()> {
        self.apply_all(id, &[op])
    }
    pub fn apply_all(&mut self, id: DocumentId, ops: &[Op]) -> Result<()> {
        self.stage_ops(id, ops, false)
    }
    fn stage_ops(&mut self, id: DocumentId, ops: &[Op], tracked: bool) -> Result<()> {
        let doc = self
            .documents
            .iter_mut()
            .find(|d| d.record.id == id && d.acquired)
            .ok_or_else(|| Error::validation("document is not acquired"))?;
        let value = apply(&doc.value, ops)?;
        if !value.is_object() {
            return Err(Error::validation("document root must remain object"));
        }
        doc.value = value;
        doc.ops.extend_from_slice(ops);
        doc.tracked &= tracked;
        Ok(())
    }
    pub fn retire_doc(
        &mut self,
        definition: &DocDefinition,
        address: DocumentAddress,
    ) -> Result<()> {
        definition.check_address(&address)?;
        if let Some(&index) = self.latest.get(&address) {
            definition
                .check_record(&self.documents[index].record, self.documents[index].version)?;
            self.documents[index].retire = true;
            return Ok(());
        }
        if let Some(stored) = self.read_result(load_defined(
            self.storage,
            definition,
            &address,
            DocumentPoint::Current,
        ))? {
            let index = self.documents.len();
            self.latest.insert(address, index);
            self.documents.push(StagedDocument {
                record: stored.record.identity.clone(),
                committed: Some(stored.record),
                definition: Some(definition.clone()),
                version: stored.version,
                value: stored.value.clone(),
                ops: Vec::new(),
                deltas: stored.deltas_since_base,
                created: false,
                copy: None,
                retire: true,
                acquired: false,
                checkpointed: false,
                base: stored.value,
                tracked: true,
            });
        }
        Ok(())
    }
    /// Scoped mutation convenience: harmless assignments and cancelled append/pop
    /// edits prepare to an empty batch; explicit `apply` preserves supplied ops.
    pub fn mutate(&mut self, id: DocumentId, op: Op) -> Result<()> {
        self.stage_ops(id, &[op], true)
    }
    fn prepare(&mut self) -> Result<Prepared> {
        for doc in &mut self.documents {
            if doc.tracked {
                doc.ops = crate::delta::prepare_mutations(&doc.base, &doc.ops)?;
            }
        }

        for task in self.tasks.values() {
            crate::storage::validate_task(task)?;
        }
        // Validate ownership against final candidates, before running checkpoint policies.
        let mut owners = Vec::new();
        for table in &self.tables {
            if let StorageWrite::Conversation { value } = table
                && let Some(owner) = &value.owner
            {
                owners.push(owner.task_id);
            }
        }
        for id in &self.created_tasks {
            if let Some(owner) = self.tasks[id].owner {
                owners.push(owner);
            }
        }
        for owner in owners {
            let task = self
                .current_task(owner)?
                .ok_or_else(|| Error::validation("owner task missing"))?;
            if !task.state.can_own() || task.abort_requested {
                return Err(Error::validation(
                    "new owned work requires live unmarked owner",
                ));
            }
        }
        let terminal = self
            .tasks
            .values()
            .filter(|t| t.state.is_terminal())
            .map(|t| t.id)
            .collect::<Vec<_>>();
        for id in terminal {
            for doc in &mut self.documents {
                if doc.record.scope == (Scope::Task { task_id: id }) {
                    doc.retire = true;
                }
            }
            for record in self.read_result(all_documents(
                self.storage,
                Scope::Task { task_id: id },
                DocumentPoint::Current,
            ))? {
                if self
                    .documents
                    .iter()
                    .any(|d| d.record.id == record.identity.id)
                {
                    continue;
                }
                let stored = self
                    .read_result(
                        self.storage
                            .document(record.identity.id, DocumentPoint::Current),
                    )?
                    .ok_or_else(|| Error::backend("task document missing"))?;
                self.documents.push(StagedDocument {
                    record: record.identity.clone(),
                    committed: Some(record),
                    definition: None,
                    version: stored.version,
                    value: stored.value.clone(),
                    ops: Vec::new(),
                    deltas: stored.deltas_since_base,
                    created: false,
                    copy: None,
                    retire: true,
                    acquired: false,
                    checkpointed: false,
                    base: stored.value,
                    tracked: true,
                });
            }
        }
        let mut writes = self.tables.clone();
        writes.extend(
            self.tasks
                .values()
                .cloned()
                .map(|value| StorageWrite::Task { value }),
        );
        writes.extend(
            self.submissions
                .values()
                .cloned()
                .map(|value| StorageWrite::Submission { value }),
        );
        let tables = writes.clone();
        let mut conversations = BTreeMap::new();
        for i in 0..self.documents.len() {
            let doc = &self.documents[i];
            if !doc.created && doc.ops.is_empty() && !doc.retire {
                continue;
            }
            if self.copy_sources.contains(&doc.record.id) {
                return Err(Error::validation(
                    "cannot modify selected fork source document",
                ));
            }
            if let Scope::Conversation { conversation_id } = doc.record.scope
                && self.fork_sources.contains(&conversation_id)
                && doc.record.fork == Some(ForkPolicy::Current)
            {
                return Err(Error::validation(
                    "cannot change current-policy source in fork transaction",
                ));
            }
            if let Scope::Task { task_id } = doc.record.scope {
                let task = self
                    .current_task(task_id)?
                    .ok_or_else(|| Error::validation("document task missing"))?;
                conversations.insert(task_id, task.conversation_id);
            }
        }
        for doc in &mut self.documents {
            if doc.created {
                if let Some(source) = &doc.copy {
                    if !doc.acquired {
                        writes.push(StorageWrite::DocumentCopy {
                            record: doc.record.clone(),
                            source: source.clone(),
                        });
                    } else {
                        writes.push(StorageWrite::DocumentCreate {
                            record: doc.record.clone(),
                            content: DocumentContent::Base {
                                version: doc.version,
                                value: doc.value.clone(),
                            },
                        });
                    }
                } else {
                    writes.push(StorageWrite::DocumentCreate {
                        record: doc.record.clone(),
                        content: DocumentContent::Base {
                            version: doc.version,
                            value: doc.value.clone(),
                        },
                    });
                }
            } else if !doc.ops.is_empty() {
                doc.checkpointed = (doc
                    .definition
                    .as_ref()
                    .expect("acquired definition")
                    .checkpoint)(
                    &doc.value,
                    &doc.ops,
                    CheckpointInfo {
                        deltas_since_base: doc.deltas,
                    },
                )?;
                let content = if doc.checkpointed {
                    DocumentContent::Base {
                        version: doc.version,
                        value: doc.value.clone(),
                    }
                } else {
                    DocumentContent::Delta {
                        version: doc.version,
                        ops: doc.ops.clone(),
                    }
                };
                writes.push(StorageWrite::DocumentChange {
                    id: doc.record.id,
                    content,
                });
            }
            if doc.retire {
                writes.push(StorageWrite::DocumentRetire { id: doc.record.id });
            }
        }
        let documents = std::mem::take(&mut self.documents)
            .into_iter()
            .filter(|d| d.created || !d.ops.is_empty() || d.retire)
            .collect();
        Ok(Prepared {
            writes,
            tables,
            documents,
            conversations,
        })
    }
}
