//! Atomic logical store and its Memory/SQLite implementations.
use crate::{Error, ErrorKind, Result, apply, records::*};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

/// Synchronous persistence/fault boundary. Session supplies serialized admission.
pub trait Storage: Send + Sync {
    fn commit(&self, writes: &[StorageWrite]) -> Result<Seq>;
    fn mint_id(&self) -> Result<u64>;
    fn conversation(&self, id: ConversationId) -> Result<Option<ConversationRecord>>;
    fn scan_conversations(
        &self,
        query: &ConversationQuery,
        limit: usize,
        cursor: Option<&Cursor>,
    ) -> Result<Page<ConversationRecord>>;
    fn entry(&self, id: EntryId) -> Result<Option<StoredEntry>>;
    fn visible_entry(
        &self,
        conversation: ConversationId,
        id: EntryId,
    ) -> Result<Option<StoredEntry>>;
    fn find_latest_head_marker(
        &self,
        conversation: ConversationId,
        cutoff: Option<EntryId>,
    ) -> Result<Option<EntryRecord>>;
    fn scan_entries(
        &self,
        query: &EntryQuery,
        limit: usize,
        cursor: Option<&Cursor>,
    ) -> Result<Page<EntryRecord>>;
    fn task(&self, id: TaskId) -> Result<Option<TaskRecord>>;
    fn scan_tasks(
        &self,
        query: &TaskQuery,
        limit: usize,
        cursor: Option<&Cursor>,
    ) -> Result<Page<TaskRecord>>;
    fn submission(&self, id: SubmissionId) -> Result<Option<SubmissionRecord>>;
    fn scan_submissions(
        &self,
        query: &SubmissionQuery,
        limit: usize,
        cursor: Option<&Cursor>,
    ) -> Result<Page<SubmissionRecord>>;
    fn submission_by_request(
        &self,
        conversation: ConversationId,
        request: &str,
    ) -> Result<Option<SubmissionRecord>>;
    fn find_document(
        &self,
        address: &DocumentAddress,
        at: DocumentPoint,
    ) -> Result<Option<DocumentRecord>>;
    fn document(&self, id: DocumentId, at: DocumentPoint) -> Result<Option<StoredDocument>>;
    fn scan_documents(
        &self,
        query: &DocumentQuery,
        limit: usize,
        cursor: Option<&Cursor>,
    ) -> Result<Page<DocumentRecord>>;
    fn close(&self) -> Result<()>;
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StoredEntry {
    pub entry: EntryRecord,
    pub commit_seq: Seq,
}
#[derive(Clone, Default, Serialize, Deserialize)]
struct Data {
    high_water: u64,
    sequence: u64,
    conversation_owner_conversation: BTreeMap<ConversationId, BTreeSet<ConversationId>>,
    conversation_owner_task: BTreeMap<TaskId, BTreeSet<ConversationId>>,
    entry_conversation: BTreeMap<ConversationId, BTreeSet<EntryId>>,
    task_conversation: BTreeMap<ConversationId, BTreeSet<TaskId>>,
    task_status: BTreeMap<String, BTreeSet<TaskId>>,
    submission_conversation: BTreeMap<ConversationId, BTreeSet<SubmissionId>>,
    submission_status: BTreeMap<String, BTreeSet<SubmissionId>>,
    submission_request: BTreeMap<(ConversationId, String), SubmissionId>,
    document_address: BTreeMap<DocumentAddress, BTreeSet<DocumentId>>,
    document_scope: BTreeMap<Scope, BTreeSet<DocumentId>>,
    types: BTreeMap<u64, String>,
    conversations: BTreeMap<ConversationId, ConversationRecord>,
    entries: BTreeMap<EntryId, StoredEntry>,
    tasks: BTreeMap<TaskId, TaskRecord>,
    submissions: BTreeMap<SubmissionId, SubmissionRecord>,
    documents: BTreeMap<DocumentId, DocumentData>,
}
#[derive(Clone, Serialize, Deserialize)]
struct DocumentData {
    record: DocumentRecord,
    revisions: Vec<Revision>,
}
#[derive(Clone, Serialize, Deserialize)]
struct Revision {
    seq: Seq,
    content: DocumentContent,
}
#[derive(Default)]
struct Action {
    create: Option<DocumentCreate>,
    content: Option<DocumentContent>,
    retire: bool,
}
impl Data {
    fn reindex(&mut self) {
        self.conversation_owner_conversation.clear();
        self.conversation_owner_task.clear();
        self.entry_conversation.clear();
        self.task_conversation.clear();
        self.task_status.clear();
        self.submission_conversation.clear();
        self.submission_status.clear();
        self.submission_request.clear();
        self.document_address.clear();
        self.document_scope.clear();
        for c in self.conversations.values() {
            if let Some(owner) = &c.owner {
                self.conversation_owner_conversation
                    .entry(owner.conversation_id)
                    .or_default()
                    .insert(c.id);
                self.conversation_owner_task
                    .entry(owner.task_id)
                    .or_default()
                    .insert(c.id);
            }
        }
        for e in self.entries.values() {
            self.entry_conversation
                .entry(e.entry.conversation_id)
                .or_default()
                .insert(e.entry.id);
        }
        for t in self.tasks.values() {
            self.task_conversation
                .entry(t.conversation_id)
                .or_default()
                .insert(t.id);
            self.task_status
                .entry(t.state.status().into())
                .or_default()
                .insert(t.id);
        }
        for s in self.submissions.values() {
            self.submission_conversation
                .entry(s.conversation_id)
                .or_default()
                .insert(s.id);
            self.submission_status
                .entry(s.state.status().into())
                .or_default()
                .insert(s.id);
            if let Some(request) = &s.request_id {
                self.submission_request
                    .insert((s.conversation_id, request.clone()), s.id);
            }
        }
        for d in self.documents.values() {
            self.document_address
                .entry(d.record.identity.address())
                .or_default()
                .insert(d.record.identity.id);
            self.document_scope
                .entry(d.record.identity.scope.clone())
                .or_default()
                .insert(d.record.identity.id);
        }
    }
    fn visible_entries(&self, conversation: ConversationId) -> Result<Vec<EntryRecord>> {
        self.require_conversation(conversation)?;
        let mut current = conversation;
        let mut cutoff = MAX_SAFE_INTEGER;
        let mut seen = BTreeSet::new();
        let mut result = Vec::new();
        loop {
            if !seen.insert(current) {
                return Err(Error::backend("conversation ancestry cycle"));
            }
            if let Some(ids) = self.entry_conversation.get(&current) {
                for id in ids.iter().filter(|id| id.get() <= cutoff) {
                    result.push(self.entries[id].entry.clone());
                }
            }
            match self
                .conversations
                .get(&current)
                .and_then(|c| c.parent.as_ref())
            {
                Some(parent) => {
                    cutoff = cutoff.min(parent.at.get());
                    current = parent.conversation_id;
                }
                None => break,
            }
        }
        result.sort_by_key(|e| e.id);
        Ok(result)
    }
    fn mint(&mut self) -> Result<u64> {
        let next = self
            .high_water
            .max(1)
            .checked_add(1)
            .filter(|n| *n <= MAX_SAFE_INTEGER)
            .ok_or_else(|| Error::backend("ID namespace exhausted"))?;
        self.high_water = next;
        Ok(next)
    }
    fn commit(&mut self, writes: &[StorageWrite]) -> Result<Seq> {
        let seq = Seq::new(
            self.sequence
                .checked_add(1)
                .ok_or_else(|| Error::backend("sequence exhausted"))?,
        )?;
        let mut next = self.clone();
        let changed = writes
            .iter()
            .filter_map(|w| match w {
                StorageWrite::DocumentCreate { record, .. }
                | StorageWrite::DocumentCopy { record, .. } => Some(record.id),
                StorageWrite::DocumentChange { id, .. } | StorageWrite::DocumentRetire { id } => {
                    Some(*id)
                }
                _ => None,
            })
            .collect::<BTreeSet<_>>();
        let mut actions: BTreeMap<DocumentId, Action> = BTreeMap::new();
        let mut claimed: BTreeMap<u64, &str> = BTreeMap::new();
        for w in writes {
            let identity = match w {
                StorageWrite::Conversation { value } => {
                    Some((value.id.get(), "conversation", false))
                }
                StorageWrite::Entry { value } => Some((value.id.get(), "entry", false)),
                StorageWrite::Task { value } => Some((value.id.get(), "task", true)),
                StorageWrite::Submission { value } => Some((value.id.get(), "submission", true)),
                StorageWrite::DocumentCreate { record, .. }
                | StorageWrite::DocumentCopy { record, .. } => {
                    Some((record.id.get(), "document", false))
                }
                _ => None,
            };
            if let Some((id, kind, replace)) = identity {
                if id == 1 && kind != "conversation" {
                    return Err(Error::validation("root ID is reserved for conversation"));
                }
                let old = self.types.get(&id).map(String::as_str);
                let earlier = claimed.insert(id, kind);
                if old.is_some_and(|old| !replace || old != kind)
                    || earlier.is_some_and(|old| !replace || old != kind)
                {
                    return Err(Error::validation(format!(
                        "global ID {id} is already owned"
                    )));
                }
                next.types.insert(id, kind.to_owned());
                next.high_water = next.high_water.max(id);
            }
            match w {
                StorageWrite::Conversation { value } => {
                    next.conversations.insert(value.id, value.clone());
                }
                StorageWrite::Entry { value } => {
                    next.entries.insert(
                        value.id,
                        StoredEntry {
                            entry: value.clone(),
                            commit_seq: seq,
                        },
                    );
                }
                StorageWrite::Task { value } => {
                    validate_task(value)?;
                    if let Some(old) = self.tasks.get(&value.id)
                        && (old.conversation_id != value.conversation_id
                            || old.kind != value.kind
                            || old.owner != value.owner
                            || old.background != value.background
                            || old.abandon_on_restart != value.abandon_on_restart
                            || old.input != value.input
                            || old.trace != value.trace
                            || old.links != value.links)
                    {
                        return Err(Error::validation("task immutable creation fields changed"));
                    }
                    next.tasks.insert(value.id, value.as_ref().clone());
                }
                StorageWrite::Submission { value } => {
                    value.validate()?;
                    if let Some(old) = self.submissions.get(&value.id)
                        && (old.conversation_id != value.conversation_id
                            || old.submission_type != value.submission_type
                            || old.request_id != value.request_id
                            || old.trace != value.trace)
                    {
                        return Err(Error::validation("submission immutable identity changed"));
                    }
                    next.submissions.insert(value.id, value.clone());
                }
                StorageWrite::DocumentCreate { record, content } => {
                    if !matches!(content, DocumentContent::Base { .. }) {
                        return Err(Error::validation("document creation requires base"));
                    }
                    add_content(
                        &mut actions,
                        record.id,
                        Some(record.clone()),
                        content.clone(),
                    )?;
                }
                StorageWrite::DocumentCopy { record, source } => {
                    if changed.contains(&source.id) {
                        return Err(Error::validation("copy source changed in same batch"));
                    }
                    let source = self
                        .document(source.id, source.at)?
                        .ok_or_else(|| Error::validation("copy source is not alive"))?;
                    let src = &source.record.identity;
                    if !matches!(src.scope, Scope::Conversation { .. })
                        || !matches!(record.scope, Scope::Conversation { .. })
                        || src.kind != record.kind
                        || src.key != record.key
                        || src.history != record.history
                        || src.fork != record.fork
                    {
                        return Err(Error::validation("copy source policy/address mismatch"));
                    }
                    add_content(
                        &mut actions,
                        record.id,
                        Some(record.clone()),
                        DocumentContent::Base {
                            version: source.version,
                            value: source.value,
                        },
                    )?;
                }
                StorageWrite::DocumentChange { id, content } => {
                    add_content(&mut actions, *id, None, content.clone())?;
                }
                StorageWrite::DocumentRetire { id } => {
                    let action = actions.entry(*id).or_default();
                    if action.retire {
                        return Err(Error::validation("duplicate document retirement"));
                    }
                    action.retire = true;
                }
            }
        }
        let mut requests = BTreeSet::new();
        for value in next.submissions.values() {
            if let Some(request) = &value.request_id
                && !requests.insert((value.conversation_id, request))
            {
                return Err(Error::validation("duplicate conversation request ID"));
            }
        }
        // Content is validated before retirement, including content that becomes unreachable.
        for (id, action) in actions {
            let mut doc = if let Some(record) = action.create {
                record.validate()?;
                DocumentData {
                    record: DocumentRecord {
                        identity: record,
                        created_at: seq,
                        retired_at: None,
                    },
                    revisions: Vec::new(),
                }
            } else {
                self.documents
                    .get(&id)
                    .cloned()
                    .ok_or_else(|| Error::validation("unknown document incarnation"))?
            };
            if doc.record.retired_at.is_some() {
                return Err(Error::validation("document incarnation already retired"));
            }
            if let Some(content) = action.content {
                if content.version() == 0 {
                    return Err(Error::validation("document version must be positive"));
                }
                match &content {
                    DocumentContent::Base { value, .. } => {
                        require_object(value)?;
                    }
                    DocumentContent::Delta { version, ops } => {
                        let current = materialize(&doc, DocumentPoint::Current)?
                            .ok_or_else(|| Error::validation("delta has no base"))?;
                        if current.version != *version {
                            return Err(Error::validation("delta crosses version boundary"));
                        }
                        require_object(&apply(&current.value, ops)?)?;
                    }
                }
                if !doc.record.rewindable() && matches!(content, DocumentContent::Base { .. }) {
                    doc.revisions.clear();
                }
                doc.revisions.push(Revision { seq, content });
            }
            if doc.revisions.is_empty() {
                return Err(Error::validation("document missing base"));
            }
            if action.retire {
                doc.record.retired_at = Some(seq);
                if !doc.record.rewindable() {
                    doc.revisions.clear();
                }
            }
            next.documents.insert(id, doc);
        }
        let mut live = BTreeSet::new();
        for doc in next.documents.values() {
            if doc.record.retired_at.is_none() && !live.insert(doc.record.identity.address()) {
                return Err(Error::validation(
                    "multiple live document incarnations at address",
                ));
            }
        }
        next.sequence = seq.get();
        next.reindex();
        *self = next;
        Ok(seq)
    }
    fn document(&self, id: DocumentId, at: DocumentPoint) -> Result<Option<StoredDocument>> {
        self.documents
            .get(&id)
            .map_or(Ok(None), |doc| materialize(doc, at))
    }
    fn visible(&self, conversation: ConversationId, id: EntryId) -> Result<bool> {
        self.require_conversation(conversation)?;
        let Some(entry) = self.entries.get(&id) else {
            return Ok(false);
        };
        let mut current = conversation;
        let mut cutoff = MAX_SAFE_INTEGER;
        let mut seen = BTreeSet::new();
        loop {
            if !seen.insert(current) {
                return Err(Error::backend("conversation ancestry cycle"));
            }
            if entry.entry.conversation_id == current && id.get() <= cutoff {
                return Ok(true);
            }
            match self
                .conversations
                .get(&current)
                .and_then(|c| c.parent.as_ref())
            {
                Some(parent) => {
                    cutoff = cutoff.min(parent.at.get());
                    current = parent.conversation_id;
                }
                None => return Ok(false),
            }
        }
    }
    fn require_conversation(&self, id: ConversationId) -> Result<()> {
        if self.conversations.contains_key(&id) {
            Ok(())
        } else {
            Err(Error::request(format!("unknown conversation {id}")))
        }
    }
}
fn require_object(value: &Value) -> Result<()> {
    if value.is_object() {
        Ok(())
    } else {
        Err(Error::validation("document root must be a JSON object"))
    }
}
pub(crate) fn validate_task(value: &TaskRecord) -> Result<()> {
    if value.version == 0
        || value.kind.is_empty()
        || value.owner.is_some() && value.background
        || value.abandon_on_restart == Some(false)
        || !value.state.can_own() && value.memos.is_some()
    {
        return Err(Error::validation("invalid task record"));
    }
    let mut times = vec![value.started_at, value.ended_at];
    if let TaskState::Sleeping { until, .. } = &value.state {
        times.push(Some(*until));
    }
    if times.into_iter().flatten().any(|t| !t.is_finite()) {
        return Err(Error::validation("nonfinite task time"));
    }
    Ok(())
}
fn add_content(
    actions: &mut BTreeMap<DocumentId, Action>,
    id: DocumentId,
    create: Option<DocumentCreate>,
    content: DocumentContent,
) -> Result<()> {
    let action = actions.entry(id).or_default();
    if action.content.is_some() || action.create.is_some() {
        return Err(Error::validation("more than one document content command"));
    }
    action.create = create;
    action.content = Some(content);
    Ok(())
}
fn materialize(doc: &DocumentData, at: DocumentPoint) -> Result<Option<StoredDocument>> {
    if matches!(at, DocumentPoint::At(_)) && !doc.record.rewindable() {
        return Err(Error::request(
            "document does not retain historical content",
        ));
    }
    if !doc.record.alive(at) {
        return Ok(None);
    }
    let revisions = doc
        .revisions
        .iter()
        .filter(|r| match at {
            DocumentPoint::Current => true,
            DocumentPoint::At(at) => r.seq <= at,
        })
        .collect::<Vec<_>>();
    let base = revisions
        .iter()
        .rposition(|r| matches!(r.content, DocumentContent::Base { .. }))
        .ok_or_else(|| Error::backend("document is missing base"))?;
    let DocumentContent::Base { version, value } = &revisions[base].content else {
        unreachable!()
    };
    let mut value = value.clone();
    for r in &revisions[base + 1..] {
        match &r.content {
            DocumentContent::Delta { version: v, ops } if v == version => {
                value = apply(&value, ops)?
            }
            _ => return Err(Error::backend("stored delta version mismatch")),
        }
    }
    require_object(&value)?;
    Ok(Some(StoredDocument {
        record: doc.record.clone(),
        version: *version,
        value,
        deltas_since_base: revisions.len() - base - 1,
    }))
}

fn page<T: Clone>(
    items: Vec<(u64, T)>,
    limit: usize,
    cursor: Option<&Cursor>,
    query: Value,
    requested: Option<ScanOrder>,
    default: ScanOrder,
) -> Result<Page<T>> {
    if limit == 0 {
        return Err(Error::request("scan limit must be positive"));
    }
    let (order, after) = if let Some(cursor) = cursor {
        let value = cursor
            .0
            .as_object()
            .ok_or_else(|| Error::request("malformed cursor"))?;
        if value.len() != 4
            || value.get("v") != Some(&json!(1))
            || value.get("query") != Some(&query)
        {
            return Err(Error::request("foreign storage cursor"));
        }
        let order: ScanOrder = serde_json::from_value(
            value
                .get("order")
                .cloned()
                .ok_or_else(|| Error::request("missing cursor order"))?,
        )
        .map_err(|_| Error::request("invalid cursor order"))?;
        let after = value
            .get("after")
            .and_then(Value::as_u64)
            .filter(|n| *n > 0 && *n <= MAX_SAFE_INTEGER)
            .ok_or_else(|| Error::request("invalid cursor position"))?;
        if requested.is_some_and(|r| r != order) {
            return Err(Error::request("cursor order conflict"));
        }
        (order, Some(after))
    } else {
        (requested.unwrap_or(default), None)
    };
    let mut items = items;
    if order == ScanOrder::Descending {
        items.reverse();
    }
    let mut selected = items
        .into_iter()
        .filter(|(id, _)| {
            after.is_none_or(|a| {
                if order == ScanOrder::Ascending {
                    *id > a
                } else {
                    *id < a
                }
            })
        })
        .take(limit.saturating_add(1))
        .collect::<Vec<_>>();
    let more = selected.len() > limit;
    selected.truncate(limit);
    let next = if more {
        selected
            .last()
            .map(|(id, _)| Cursor(json!({"v":1,"query":query,"order":order,"after":id})))
    } else {
        None
    };
    Ok(Page {
        items: selected.into_iter().map(|(_, v)| v).collect(),
        next,
    })
}

/// Reopenable in-memory backing. Each handle closes independently, like SQLite.
#[derive(Clone)]
pub struct MemoryStorage {
    data: Arc<Mutex<Data>>,
    closed: Arc<AtomicBool>,
}
impl Default for MemoryStorage {
    fn default() -> Self {
        Self {
            data: Arc::new(Mutex::new(Data::default())),
            closed: Arc::new(AtomicBool::new(false)),
        }
    }
}
impl MemoryStorage {
    pub fn new() -> Self {
        Self::default()
    }
    fn check(&self) -> Result<()> {
        if self.closed.load(Ordering::Acquire) {
            Err(Error::new(ErrorKind::Closed, "MemoryStorage is closed"))
        } else {
            Ok(())
        }
    }
    fn read<T>(&self, f: impl FnOnce(&Data) -> Result<T>) -> Result<T> {
        self.check()?;
        let data = self
            .data
            .lock()
            .map_err(|_| Error::backend("MemoryStorage lock poisoned"))?;
        self.check()?;
        f(&data)
    }
    fn mutate<T>(&self, f: impl FnOnce(&mut Data) -> Result<T>) -> Result<T> {
        self.check()?;
        let mut data = self
            .data
            .lock()
            .map_err(|_| Error::backend("MemoryStorage lock poisoned"))?;
        self.check()?;
        f(&mut data)
    }
}
fn select<'a, K: Ord, V>(
    values: &'a BTreeMap<K, V>,
    selection: Option<Option<&BTreeSet<K>>>,
) -> Vec<&'a V> {
    match selection {
        None => values.values().collect(),
        Some(None) => Vec::new(),
        Some(Some(ids)) => ids.iter().filter_map(|id| values.get(id)).collect(),
    }
}
macro_rules! storage_impl {
    ($ty:ty) => {
        impl Storage for $ty {
            fn commit(&self, writes: &[StorageWrite]) -> Result<Seq> {
                self.mutate(|data| data.commit(writes))
            }
            fn mint_id(&self) -> Result<u64> {
                self.mutate(Data::mint)
            }
            fn conversation(&self, id: ConversationId) -> Result<Option<ConversationRecord>> {
                self.read(|d| Ok(d.conversations.get(&id).cloned()))
            }
            fn scan_conversations(
                &self,
                q: &ConversationQuery,
                n: usize,
                c: Option<&Cursor>,
            ) -> Result<Page<ConversationRecord>> {
                self.read(|d| {
                    let mut k = q.clone();
                    k.order = None;
                    page(
                        select(
                            &d.conversations,
                            q.owner_task_id
                                .map(|id| d.conversation_owner_task.get(&id))
                                .or_else(|| {
                                    q.owner_conversation_id
                                        .map(|id| d.conversation_owner_conversation.get(&id))
                                }),
                        )
                        .into_iter()
                        .filter(|v| {
                            q.owner_conversation_id.is_none_or(|id| {
                                v.owner.as_ref().is_some_and(|o| o.conversation_id == id)
                            }) && q
                                .owner_task_id
                                .is_none_or(|id| v.owner.as_ref().is_some_and(|o| o.task_id == id))
                        })
                        .map(|v| (v.id.get(), v.clone()))
                        .collect(),
                        n,
                        c,
                        json!(["conversations", k]),
                        q.order,
                        ScanOrder::Ascending,
                    )
                })
            }
            fn entry(&self, id: EntryId) -> Result<Option<StoredEntry>> {
                self.read(|d| Ok(d.entries.get(&id).cloned()))
            }
            fn visible_entry(
                &self,
                conversation: ConversationId,
                id: EntryId,
            ) -> Result<Option<StoredEntry>> {
                self.read(|d| {
                    Ok(if d.visible(conversation, id)? {
                        d.entries.get(&id).cloned()
                    } else {
                        None
                    })
                })
            }
            fn find_latest_head_marker(
                &self,
                conversation: ConversationId,
                cutoff: Option<EntryId>,
            ) -> Result<Option<EntryRecord>> {
                self.read(|d| {
                    Ok(d.visible_entries(conversation)?
                        .into_iter()
                        .rev()
                        .find(|v| v.head.is_some() && cutoff.is_none_or(|max| v.id <= max)))
                })
            }
            fn scan_entries(
                &self,
                q: &EntryQuery,
                n: usize,
                c: Option<&Cursor>,
            ) -> Result<Page<EntryRecord>> {
                self.read(|d| {
                    let mut k = q.clone();
                    k.order = None;
                    let values = d
                        .visible_entries(q.conversation_id)?
                        .into_iter()
                        .filter(|v| {
                            q.min_entry_id.is_none_or(|min| v.id >= min)
                                && q.max_entry_id.is_none_or(|max| v.id <= max)
                        })
                        .map(|v| (v.id.get(), v))
                        .collect();
                    page(
                        values,
                        n,
                        c,
                        json!(["entries", k]),
                        q.order,
                        ScanOrder::Descending,
                    )
                })
            }
            fn task(&self, id: TaskId) -> Result<Option<TaskRecord>> {
                self.read(|d| Ok(d.tasks.get(&id).cloned()))
            }
            fn scan_tasks(
                &self,
                q: &TaskQuery,
                n: usize,
                c: Option<&Cursor>,
            ) -> Result<Page<TaskRecord>> {
                self.read(|d| {
                    let mut k = q.clone();
                    k.order = None;
                    page(
                        select(
                            &d.tasks,
                            q.status
                                .as_ref()
                                .map(|status| d.task_status.get(status))
                                .or_else(|| {
                                    q.conversation_id.map(|id| d.task_conversation.get(&id))
                                }),
                        )
                        .into_iter()
                        .filter(|v| {
                            q.conversation_id.is_none_or(|id| v.conversation_id == id)
                                && q.kind.as_ref().is_none_or(|s| &v.kind == s)
                                && q.status.as_ref().is_none_or(|s| v.state.status() == s)
                                && q.abort_requested.is_none_or(|b| v.abort_requested == b)
                                && q.background.is_none_or(|b| v.background == b)
                        })
                        .map(|v| (v.id.get(), v.clone()))
                        .collect(),
                        n,
                        c,
                        json!(["tasks", k]),
                        q.order,
                        ScanOrder::Ascending,
                    )
                })
            }
            fn submission(&self, id: SubmissionId) -> Result<Option<SubmissionRecord>> {
                self.read(|d| Ok(d.submissions.get(&id).cloned()))
            }
            fn scan_submissions(
                &self,
                q: &SubmissionQuery,
                n: usize,
                c: Option<&Cursor>,
            ) -> Result<Page<SubmissionRecord>> {
                self.read(|d| {
                    let mut k = q.clone();
                    k.order = None;
                    page(
                        select(
                            &d.submissions,
                            q.status
                                .as_ref()
                                .map(|status| d.submission_status.get(status))
                                .or_else(|| {
                                    q.conversation_id
                                        .map(|id| d.submission_conversation.get(&id))
                                }),
                        )
                        .into_iter()
                        .filter(|v| {
                            q.conversation_id.is_none_or(|id| v.conversation_id == id)
                                && q.status.as_ref().is_none_or(|s| v.state.status() == s)
                        })
                        .map(|v| (v.id.get(), v.clone()))
                        .collect(),
                        n,
                        c,
                        json!(["submissions", k]),
                        q.order,
                        ScanOrder::Ascending,
                    )
                })
            }
            fn submission_by_request(
                &self,
                conversation: ConversationId,
                request: &str,
            ) -> Result<Option<SubmissionRecord>> {
                self.read(|d| {
                    Ok(d.submission_request
                        .get(&(conversation, request.to_owned()))
                        .and_then(|id| d.submissions.get(id))
                        .cloned())
                })
            }
            fn find_document(
                &self,
                address: &DocumentAddress,
                at: DocumentPoint,
            ) -> Result<Option<DocumentRecord>> {
                self.read(|d| {
                    Ok(d.document_address
                        .get(address)
                        .into_iter()
                        .flatten()
                        .filter_map(|id| d.documents.get(id))
                        .find(|v| v.record.alive(at))
                        .map(|v| v.record.clone()))
                })
            }
            fn document(
                &self,
                id: DocumentId,
                at: DocumentPoint,
            ) -> Result<Option<StoredDocument>> {
                self.read(|d| d.document(id, at))
            }
            fn scan_documents(
                &self,
                q: &DocumentQuery,
                n: usize,
                c: Option<&Cursor>,
            ) -> Result<Page<DocumentRecord>> {
                self.read(|d| {
                    page(
                        d.document_scope
                            .get(&q.scope)
                            .into_iter()
                            .flatten()
                            .filter_map(|id| d.documents.get(id))
                            .filter(|v| {
                                q.kind.as_ref().is_none_or(|k| v.record.identity.kind == *k)
                                    && v.record.alive(q.at)
                            })
                            .map(|v| (v.record.identity.id.get(), v.record.clone()))
                            .collect(),
                        n,
                        c,
                        json!(["documents", q]),
                        None,
                        ScanOrder::Ascending,
                    )
                })
            }
            fn close(&self) -> Result<()> {
                self.closed.store(true, Ordering::Release);
                Ok(())
            }
        }
    };
}
storage_impl!(MemoryStorage);
mod sqlite;
pub use sqlite::SqliteStorage;
