//! Indexed SQLite records and per-incarnation base/delta history.
use super::*;
use pi_sqlite::{Database, OptionalExtension, Transaction, params};
use rusqlite::{Connection, params_from_iter, types::Value as SqlValue};
use std::path::Path;

const SCHEMA:&str="
CREATE TABLE durable_meta(singleton INTEGER PRIMARY KEY CHECK(singleton=1),high_water INTEGER NOT NULL,seq INTEGER NOT NULL) STRICT;
INSERT INTO durable_meta VALUES(1,1,0);
CREATE TABLE durable_ids(id INTEGER PRIMARY KEY,kind TEXT NOT NULL) STRICT;
CREATE TABLE durable_conversations(id INTEGER PRIMARY KEY,owner_conversation INTEGER,owner_task INTEGER,payload TEXT NOT NULL) STRICT;
CREATE INDEX durable_conversations_owner ON durable_conversations(owner_conversation,owner_task,id);
CREATE INDEX durable_conversations_task ON durable_conversations(owner_task,owner_conversation,id);
CREATE TABLE durable_entries(id INTEGER PRIMARY KEY,conversation INTEGER NOT NULL,head INTEGER,commit_seq INTEGER NOT NULL,payload TEXT NOT NULL) STRICT;
CREATE INDEX durable_entries_conversation ON durable_entries(conversation,id);
CREATE INDEX durable_entries_heads ON durable_entries(conversation,id DESC) WHERE head IS NOT NULL;
CREATE TABLE durable_tasks(id INTEGER PRIMARY KEY,conversation INTEGER NOT NULL,kind TEXT NOT NULL,status TEXT NOT NULL,abort_requested INTEGER NOT NULL,background INTEGER NOT NULL,payload TEXT NOT NULL) STRICT;
CREATE INDEX durable_tasks_conversation ON durable_tasks(conversation,status,id);
CREATE INDEX durable_tasks_status ON durable_tasks(status,id);
CREATE INDEX durable_tasks_kind ON durable_tasks(kind,id);
CREATE INDEX durable_tasks_abort ON durable_tasks(abort_requested,id);
CREATE INDEX durable_tasks_background ON durable_tasks(background,id);
CREATE TABLE durable_submissions(id INTEGER PRIMARY KEY,conversation INTEGER NOT NULL,status TEXT NOT NULL,request_id TEXT,payload TEXT NOT NULL) STRICT;
CREATE UNIQUE INDEX durable_submissions_request ON durable_submissions(conversation,request_id) WHERE request_id IS NOT NULL;
CREATE INDEX durable_submissions_status ON durable_submissions(status,id);
CREATE INDEX durable_submissions_conversation ON durable_submissions(conversation,status,id);
CREATE TABLE durable_documents(id INTEGER PRIMARY KEY,scope TEXT NOT NULL,owner INTEGER NOT NULL,kind TEXT NOT NULL,family_key TEXT NOT NULL,created_at INTEGER NOT NULL,retired_at INTEGER,payload TEXT NOT NULL) STRICT;
CREATE UNIQUE INDEX durable_documents_live ON durable_documents(scope,owner,kind,family_key) WHERE retired_at IS NULL;
CREATE INDEX durable_documents_address ON durable_documents(scope,owner,kind,family_key,created_at);
CREATE INDEX durable_documents_scope ON durable_documents(scope,owner,id);
CREATE TABLE durable_revisions(document INTEGER NOT NULL,seq INTEGER NOT NULL,kind TEXT NOT NULL,version INTEGER NOT NULL,payload TEXT NOT NULL,PRIMARY KEY(document,seq)) STRICT;
CREATE INDEX durable_revisions_base ON durable_revisions(document,kind,seq DESC);
";
/// Current-schema native SQLite backend. Open performs no all-document materialization.
pub struct SqliteStorage {
    database: Database,
    closed: AtomicBool,
}
impl SqliteStorage {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::from_database(Database::open(path)?)
    }
    pub fn from_database(database: Database) -> Result<Self> {
        database.initialize("durable-session", 1, SCHEMA)?;
        Ok(Self {
            database,
            closed: AtomicBool::new(false),
        })
    }
    fn check(&self) -> Result<()> {
        if self.closed.load(Ordering::Acquire) {
            Err(Error::new(ErrorKind::Closed, "SqliteStorage is closed"))
        } else {
            Ok(())
        }
    }
    fn read<T>(&self, f: impl FnOnce(&Transaction<'_>) -> Result<T>) -> Result<T> {
        self.check()?;
        self.database.snapshot(|tx| {
            self.check()?;
            f(tx)
        })
    }
    fn write<T>(&self, f: impl FnOnce(&Transaction<'_>) -> Result<T>) -> Result<T> {
        self.check()?;
        self.database.transaction(|tx| {
            self.check()?;
            f(tx)
        })
    }
}
fn sql(e: rusqlite::Error) -> Error {
    Error::backend(e.to_string())
}
fn encode<T: Serialize>(v: &T) -> Result<String> {
    serde_json::to_string(v).map_err(|e| Error::backend(e.to_string()))
}
fn decode<T: serde::de::DeserializeOwned>(s: &str) -> Result<T> {
    serde_json::from_str(s).map_err(|e| Error::backend(format!("corrupt durable record: {e}")))
}
fn one<T: serde::de::DeserializeOwned>(tx: &Connection, table: &str, id: u64) -> Result<Option<T>> {
    let value: Option<String> = tx
        .query_row(
            &format!("SELECT payload FROM {table} WHERE id=?1"),
            [id],
            |r| r.get(0),
        )
        .optional()
        .map_err(sql)?;
    value.map(|s| decode(&s)).transpose()
}
fn scope(scope: &Scope) -> (&'static str, u64) {
    match scope {
        Scope::Session => ("session", 0),
        Scope::Conversation { conversation_id } => ("conversation", conversation_id.get()),
        Scope::Task { task_id } => ("task", task_id.get()),
    }
}
fn key(key: &Option<String>) -> String {
    serde_json::to_string(key).expect("string key")
}
fn doc_data(tx: &Connection, id: DocumentId) -> Result<Option<DocumentData>> {
    let Some(record) = one::<DocumentRecord>(tx, "durable_documents", id.get())? else {
        return Ok(None);
    };
    let mut statement = tx
        .prepare("SELECT seq,payload FROM durable_revisions WHERE document=?1 ORDER BY seq")
        .map_err(sql)?;
    let rows = statement
        .query_map([id.get()], |r| {
            Ok((r.get::<_, u64>(0)?, r.get::<_, String>(1)?))
        })
        .map_err(sql)?;
    let mut revisions = Vec::new();
    for row in rows {
        let (seq, payload) = row.map_err(sql)?;
        revisions.push(Revision {
            seq: Seq::new(seq).map_err(|e| Error::backend(e.to_string()))?,
            content: decode(&payload)?,
        });
    }
    Ok(Some(DocumentData { record, revisions }))
}
fn document(tx: &Connection, id: DocumentId, at: DocumentPoint) -> Result<Option<StoredDocument>> {
    let Some(record) = one::<DocumentRecord>(tx, "durable_documents", id.get())? else {
        return Ok(None);
    };
    if matches!(at, DocumentPoint::At(_)) && !record.rewindable() {
        return Err(Error::request(
            "document does not retain historical content",
        ));
    }
    if !record.alive(at) {
        return Ok(None);
    }
    let upper = match at {
        DocumentPoint::Current => MAX_SAFE_INTEGER,
        DocumentPoint::At(at) => at.get(),
    };
    let base:Option<u64>=tx.query_row("SELECT seq FROM durable_revisions WHERE document=?1 AND kind='base' AND seq<=?2 ORDER BY seq DESC LIMIT 1",params![id.get(),upper],|r|r.get(0)).optional().map_err(sql)?;
    let base = base.ok_or_else(|| Error::backend("document missing required base"))?;
    let mut statement=tx.prepare("SELECT seq,payload FROM durable_revisions WHERE document=?1 AND seq>=?2 AND seq<=?3 ORDER BY seq").map_err(sql)?;
    let rows = statement
        .query_map(params![id.get(), base, upper], |r| {
            Ok((r.get::<_, u64>(0)?, r.get::<_, String>(1)?))
        })
        .map_err(sql)?;
    let mut revisions = Vec::new();
    for row in rows {
        let (seq, payload) = row.map_err(sql)?;
        revisions.push(Revision {
            seq: Seq::new(seq)?,
            content: decode(&payload)?,
        });
    }
    materialize(&DocumentData { record, revisions }, at)
}
fn cursor_start(
    cursor: Option<&Cursor>,
    query: &Value,
    requested: Option<ScanOrder>,
    default: ScanOrder,
) -> Result<(ScanOrder, Option<u64>)> {
    // Use the common validator; decoding a nonempty probe avoids interpreting backend cursor fields twice.
    let order = cursor
        .and_then(|c| c.0.get("order"))
        .map(|v| {
            serde_json::from_value::<ScanOrder>(v.clone())
                .map_err(|_| Error::request("invalid cursor order"))
        })
        .transpose()?
        .or(requested)
        .unwrap_or(default);
    page::<()>(Vec::new(), 1, cursor, query.clone(), requested, default)?;
    Ok((
        order,
        cursor
            .and_then(|c| c.0.get("after"))
            .and_then(Value::as_u64),
    ))
}
struct Scan {
    table: &'static str,
    clauses: Vec<String>,
    args: Vec<SqlValue>,
}
impl Scan {
    fn new(table: &'static str) -> Self {
        Self {
            table,
            clauses: Vec::new(),
            args: Vec::new(),
        }
    }
    fn bind(&mut self, column: &str, value: impl Into<SqlValue>) {
        self.clauses
            .push(format!("{column}=?{}", self.args.len() + 1));
        self.args.push(value.into());
    }
    fn page<T: serde::de::DeserializeOwned + Serialize + Clone>(
        mut self,
        tx: &Connection,
        query: Value,
        limit: usize,
        cursor: Option<&Cursor>,
        requested: Option<ScanOrder>,
        default: ScanOrder,
    ) -> Result<Page<T>> {
        if limit == 0 || limit > i64::MAX as usize - 1 {
            return Err(Error::request("invalid scan limit"));
        }
        let (order, after) = cursor_start(cursor, &query, requested, default)?;
        if let Some(after) = after {
            self.clauses.push(format!(
                "id{}?{}",
                if order == ScanOrder::Ascending {
                    ">"
                } else {
                    "<"
                },
                self.args.len() + 1
            ));
            self.args.push((after as i64).into());
        }
        let where_clause = if self.clauses.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", self.clauses.join(" AND "))
        };
        let query_sql = format!(
            "SELECT id,payload FROM {}{} ORDER BY id {} LIMIT {}",
            self.table,
            where_clause,
            if order == ScanOrder::Ascending {
                "ASC"
            } else {
                "DESC"
            },
            limit + 1
        );
        let mut statement = tx.prepare(&query_sql).map_err(sql)?;
        let rows = statement
            .query_map(params_from_iter(self.args.iter()), |r| {
                Ok((r.get::<_, u64>(0)?, r.get::<_, String>(1)?))
            })
            .map_err(sql)?;
        let mut values = Vec::new();
        for row in rows {
            let (id, payload) = row.map_err(sql)?;
            values.push((id, decode(&payload)?));
        }
        // Common pagination owns the opaque continuation format. Restore ascending input.
        if order == ScanOrder::Descending {
            values.reverse();
        }
        page(values, limit, cursor, query, requested, default)
    }
}
fn ancestry(
    tx: &Connection,
    conversation: ConversationId,
    cutoff: Option<EntryId>,
) -> Result<Vec<(ConversationId, u64)>> {
    let mut seen = BTreeSet::new();
    let mut result = Vec::new();
    let mut id = conversation;
    let mut upper = cutoff.map_or(MAX_SAFE_INTEGER, EntryId::get);
    loop {
        if !seen.insert(id) {
            return Err(Error::backend("conversation ancestry cycle"));
        }
        let record =
            one::<ConversationRecord>(tx, "durable_conversations", id.get())?.ok_or_else(|| {
                if result.is_empty() {
                    Error::request("unknown conversation")
                } else {
                    Error::backend("missing ancestor conversation")
                }
            })?;
        result.push((id, upper));
        match record.parent {
            Some(parent) => {
                upper = upper.min(parent.at.get());
                id = parent.conversation_id
            }
            None => return Ok(result),
        }
    }
}
fn entry_scan(tx: &Connection, q: &EntryQuery) -> Result<Scan> {
    let mut scan = Scan::new("durable_entries");
    let mut segments = Vec::new();
    for (id, upper) in ancestry(tx, q.conversation_id, q.max_entry_id)? {
        let i = scan.args.len() + 1;
        segments.push(format!("(conversation=?{i} AND id<=?{})", i + 1));
        scan.args.push((id.get() as i64).into());
        scan.args.push((upper as i64).into());
    }
    scan.clauses.push(format!("({})", segments.join(" OR ")));
    if let Some(min) = q.min_entry_id {
        scan.clauses.push(format!("id>=?{}", scan.args.len() + 1));
        scan.args.push((min.get() as i64).into());
    }
    Ok(scan)
}
impl Storage for SqliteStorage {
    fn mint_id(&self) -> Result<u64> {
        self.write(|tx| {
            let high: u64 = tx
                .query_row(
                    "SELECT high_water FROM durable_meta WHERE singleton=1",
                    [],
                    |r| r.get(0),
                )
                .map_err(sql)?;
            let next = high
                .checked_add(1)
                .filter(|n| *n <= MAX_SAFE_INTEGER)
                .ok_or_else(|| Error::backend("ID namespace exhausted"))?;
            tx.execute(
                "UPDATE durable_meta SET high_water=?1 WHERE singleton=1",
                [next],
            )
            .map_err(sql)?;
            Ok(next)
        })
    }
    fn commit(&self, writes: &[StorageWrite]) -> Result<Seq> {
        self.write(|tx|{
        let (high_water,sequence)=tx.query_row("SELECT high_water,seq FROM durable_meta WHERE singleton=1",[],|r|Ok((r.get(0)?,r.get(1)?))).map_err(sql)?;
        let mut data=Data {high_water,sequence,..Data::default()};let mut changed=BTreeSet::new();let mut load_docs=BTreeSet::new();let mut addresses=BTreeSet::new();
        for write in writes {
            let identity=match write{StorageWrite::Conversation {value}=>Some(value.id.get()),StorageWrite::Entry {value}=>Some(value.id.get()),StorageWrite::Task {value}=>{if let Some(old)=one(tx,"durable_tasks",value.id.get())?{data.tasks.insert(value.id,old);}Some(value.id.get())},StorageWrite::Submission {value}=>{
                if let Some(old)=one(tx,"durable_submissions",value.id.get())?{data.submissions.insert(value.id,old);}
                if let Some(request)=&value.request_id {let payload:Option<String>=tx.query_row("SELECT payload FROM durable_submissions WHERE conversation=?1 AND request_id=?2",params![value.conversation_id.get(),request],|r|r.get(0)).optional().map_err(sql)?;if let Some(payload)=payload{let existing:SubmissionRecord=decode(&payload)?;data.submissions.insert(existing.id,existing);}}Some(value.id.get())},
                StorageWrite::DocumentCreate {record,..}|StorageWrite::DocumentCopy {record,..}=>{changed.insert(record.id);load_docs.insert(record.id);addresses.insert(record.address());Some(record.id.get())},
                StorageWrite::DocumentChange {id,..}|StorageWrite::DocumentRetire {id}=>{changed.insert(*id);load_docs.insert(*id);None},
            };
            if let Some(id)=identity{let kind:Option<String>=tx.query_row("SELECT kind FROM durable_ids WHERE id=?1",[id],|r|r.get(0)).optional().map_err(sql)?;if let Some(kind)=kind{data.types.insert(id,kind);}}
            if let StorageWrite::DocumentCopy {source,..}=write{load_docs.insert(source.id);}
        }
        for address in addresses{if let Some(record)=find_document(tx,&address,DocumentPoint::Current)?{load_docs.insert(record.identity.id);}}
        for id in load_docs {if let Some(doc)=doc_data(tx,id)?{data.documents.insert(id,doc);}}
        let seq=data.commit(writes)?;
        // Retire address incumbents before installing replacements, independent of batch order.
        for id in &changed {if data.documents.get(id).is_some_and(|d|d.record.retired_at.is_some()){tx.execute("UPDATE durable_documents SET retired_at=?2 WHERE id=?1",params![id.get(),seq.get()]).map_err(sql)?;}}
        for write in writes {
            match write {
                StorageWrite::Conversation {value}=>{tx.execute("INSERT INTO durable_conversations VALUES(?1,?2,?3,?4)",params![value.id.get(),value.owner.as_ref().map(|o|o.conversation_id.get()),value.owner.as_ref().map(|o|o.task_id.get()),encode(value)?]).map_err(sql)?;}
                StorageWrite::Entry {value}=>{tx.execute("INSERT INTO durable_entries VALUES(?1,?2,?3,?4,?5)",params![value.id.get(),value.conversation_id.get(),value.head.map(EntryId::get),seq.get(),encode(value)?]).map_err(sql)?;}
                StorageWrite::Task {value}=>{tx.execute("INSERT INTO durable_tasks VALUES(?1,?2,?3,?4,?5,?6,?7) ON CONFLICT(id) DO UPDATE SET status=excluded.status,abort_requested=excluded.abort_requested,background=excluded.background,payload=excluded.payload",params![value.id.get(),value.conversation_id.get(),value.kind,value.state.status(),value.abort_requested,value.background,encode(value)?]).map_err(sql)?;}
                StorageWrite::Submission {value}=>{tx.execute("INSERT INTO durable_submissions VALUES(?1,?2,?3,?4,?5) ON CONFLICT(id) DO UPDATE SET status=excluded.status,payload=excluded.payload",params![value.id.get(),value.conversation_id.get(),value.state.status(),value.request_id,encode(value)?]).map_err(sql)?;}
                _=>(),
            }
        }
        for id in changed {
            let doc=&data.documents[&id];let r=&doc.record;let (scope,owner)=scope(&r.identity.scope);
            tx.execute("INSERT INTO durable_documents VALUES(?1,?2,?3,?4,?5,?6,?7,?8) ON CONFLICT(id) DO UPDATE SET retired_at=excluded.retired_at,payload=excluded.payload",params![id.get(),scope,owner,r.identity.kind,key(&r.identity.key),r.created_at.get(),r.retired_at.map(Seq::get),encode(r)?]).map_err(sql)?;
            if !r.rewindable()&&(r.retired_at.is_some()||doc.revisions.last().is_some_and(|r|r.seq==seq&&matches!(r.content,DocumentContent::Base {..}))){tx.execute("DELETE FROM durable_revisions WHERE document=?1",[id.get()]).map_err(sql)?;}
            if let Some(revision)=doc.revisions.iter().find(|r|r.seq==seq){tx.execute("INSERT INTO durable_revisions VALUES(?1,?2,?3,?4,?5)",params![id.get(),seq.get(),if matches!(revision.content,DocumentContent::Base {..}){"base"}else{"delta"},revision.content.version(),encode(&revision.content)?]).map_err(sql)?;}
        }
        for (id,kind) in data.types{tx.execute("INSERT OR IGNORE INTO durable_ids VALUES(?1,?2)",params![id,kind]).map_err(sql)?;}
        tx.execute("UPDATE durable_meta SET high_water=?1,seq=?2 WHERE singleton=1",params![data.high_water,seq.get()]).map_err(sql)?;Ok(seq)
    })
    }
    fn conversation(&self, id: ConversationId) -> Result<Option<ConversationRecord>> {
        self.read(|tx| one(tx, "durable_conversations", id.get()))
    }
    fn scan_conversations(
        &self,
        q: &ConversationQuery,
        n: usize,
        c: Option<&Cursor>,
    ) -> Result<Page<ConversationRecord>> {
        self.read(|tx| {
            let mut scan = Scan::new("durable_conversations");
            if let Some(id) = q.owner_conversation_id {
                scan.bind("owner_conversation", id.get() as i64)
            }
            if let Some(id) = q.owner_task_id {
                scan.bind("owner_task", id.get() as i64)
            }
            let mut k = q.clone();
            k.order = None;
            scan.page(
                tx,
                json!(["conversations", k]),
                n,
                c,
                q.order,
                ScanOrder::Ascending,
            )
        })
    }
    fn entry(&self, id: EntryId) -> Result<Option<StoredEntry>> {
        self.read(|tx| stored_entry(tx, id))
    }
    fn visible_entry(&self, c: ConversationId, id: EntryId) -> Result<Option<StoredEntry>> {
        self.read(|tx| {
            let chain = ancestry(tx, c, None)?;
            Ok(stored_entry(tx, id)?.filter(|entry| {
                chain
                    .iter()
                    .any(|(c, max)| entry.entry.conversation_id == *c && id.get() <= *max)
            }))
        })
    }
    fn find_latest_head_marker(
        &self,
        c: ConversationId,
        cutoff: Option<EntryId>,
    ) -> Result<Option<EntryRecord>> {
        self.read(|tx| {
            let q = EntryQuery {
                conversation_id: c,
                min_entry_id: None,
                max_entry_id: cutoff,
                order: Some(ScanOrder::Descending),
            };
            let mut scan = entry_scan(tx, &q)?;
            scan.clauses.push("head IS NOT NULL".into());
            Ok(scan
                .page(
                    tx,
                    json!(["head", c, cutoff]),
                    1,
                    None,
                    Some(ScanOrder::Descending),
                    ScanOrder::Descending,
                )?
                .items
                .into_iter()
                .next())
        })
    }
    fn scan_entries(
        &self,
        q: &EntryQuery,
        n: usize,
        c: Option<&Cursor>,
    ) -> Result<Page<EntryRecord>> {
        self.read(|tx| {
            let mut k = q.clone();
            k.order = None;
            entry_scan(tx, q)?.page(
                tx,
                json!(["entries", k]),
                n,
                c,
                q.order,
                ScanOrder::Descending,
            )
        })
    }
    fn task(&self, id: TaskId) -> Result<Option<TaskRecord>> {
        self.read(|tx| one(tx, "durable_tasks", id.get()))
    }
    fn scan_tasks(&self, q: &TaskQuery, n: usize, c: Option<&Cursor>) -> Result<Page<TaskRecord>> {
        self.read(|tx| {
            let mut scan = Scan::new("durable_tasks");
            if let Some(id) = q.conversation_id {
                scan.bind("conversation", id.get() as i64)
            }
            if let Some(kind) = &q.kind {
                scan.bind("kind", kind.clone())
            }
            if let Some(status) = &q.status {
                scan.bind("status", status.clone())
            }
            if let Some(abort) = q.abort_requested {
                scan.bind("abort_requested", i64::from(abort))
            }
            if let Some(background) = q.background {
                scan.bind("background", i64::from(background))
            }
            let mut k = q.clone();
            k.order = None;
            scan.page(tx, json!(["tasks", k]), n, c, q.order, ScanOrder::Ascending)
        })
    }
    fn submission(&self, id: SubmissionId) -> Result<Option<SubmissionRecord>> {
        self.read(|tx| one(tx, "durable_submissions", id.get()))
    }
    fn scan_submissions(
        &self,
        q: &SubmissionQuery,
        n: usize,
        c: Option<&Cursor>,
    ) -> Result<Page<SubmissionRecord>> {
        self.read(|tx| {
            let mut scan = Scan::new("durable_submissions");
            if let Some(id) = q.conversation_id {
                scan.bind("conversation", id.get() as i64)
            }
            if let Some(status) = &q.status {
                scan.bind("status", status.clone())
            }
            let mut k = q.clone();
            k.order = None;
            scan.page(
                tx,
                json!(["submissions", k]),
                n,
                c,
                q.order,
                ScanOrder::Ascending,
            )
        })
    }
    fn submission_by_request(
        &self,
        c: ConversationId,
        r: &str,
    ) -> Result<Option<SubmissionRecord>> {
        self.read(|tx|{let payload:Option<String>=tx.query_row("SELECT payload FROM durable_submissions WHERE conversation=?1 AND request_id=?2",params![c.get(),r],|row|row.get(0)).optional().map_err(sql)?;payload.map(|s|decode(&s)).transpose()})
    }
    fn find_document(
        &self,
        address: &DocumentAddress,
        at: DocumentPoint,
    ) -> Result<Option<DocumentRecord>> {
        self.read(|tx| find_document(tx, address, at))
    }
    fn document(&self, id: DocumentId, at: DocumentPoint) -> Result<Option<StoredDocument>> {
        self.read(|tx| document(tx, id, at))
    }
    fn scan_documents(
        &self,
        q: &DocumentQuery,
        n: usize,
        c: Option<&Cursor>,
    ) -> Result<Page<DocumentRecord>> {
        self.read(|tx| {
            let mut scan = Scan::new("durable_documents");
            let (scope, owner) = scope(&q.scope);
            scan.bind("scope", scope.to_owned());
            scan.bind("owner", owner as i64);
            if let Some(kind) = &q.kind {
                scan.bind("kind", kind.clone())
            }
            match q.at {
                DocumentPoint::Current => scan.clauses.push("retired_at IS NULL".into()),
                DocumentPoint::At(at) => {
                    let i = scan.args.len() + 1;
                    scan.clauses.push(format!(
                        "created_at<=?{i} AND (retired_at IS NULL OR retired_at>?{i})"
                    ));
                    scan.args.push((at.get() as i64).into());
                }
            }
            scan.page(
                tx,
                json!(["documents", q]),
                n,
                c,
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
fn stored_entry(tx: &Connection, id: EntryId) -> Result<Option<StoredEntry>> {
    let row: Option<(String, u64)> = tx
        .query_row(
            "SELECT payload,commit_seq FROM durable_entries WHERE id=?1",
            [id.get()],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()
        .map_err(sql)?;
    row.map(|(p, seq)| {
        Ok(StoredEntry {
            entry: decode(&p)?,
            commit_seq: Seq::new(seq)?,
        })
    })
    .transpose()
}
fn find_document(
    tx: &Connection,
    address: &DocumentAddress,
    at: DocumentPoint,
) -> Result<Option<DocumentRecord>> {
    let (scope, owner) = scope(&address.scope);
    let (clause, upper) = match at {
        DocumentPoint::Current => ("retired_at IS NULL", MAX_SAFE_INTEGER),
        DocumentPoint::At(at) => (
            "created_at<=?5 AND (retired_at IS NULL OR retired_at>?5)",
            at.get(),
        ),
    };
    let query = format!(
        "SELECT payload FROM durable_documents WHERE scope=?1 AND owner=?2 AND kind=?3 AND family_key=?4 AND {clause} ORDER BY id DESC LIMIT 1"
    );
    let mut statement = tx.prepare(&query).map_err(sql)?;
    let args = vec![
        SqlValue::Text(scope.into()),
        SqlValue::Integer(owner as i64),
        SqlValue::Text(address.kind.clone()),
        SqlValue::Text(key(&address.key)),
    ];
    let payload: Option<String> = if matches!(at, DocumentPoint::Current) {
        statement
            .query_row(params_from_iter(args), |r| r.get(0))
            .optional()
            .map_err(sql)?
    } else {
        let mut args = args;
        args.push(SqlValue::Integer(upper as i64));
        statement
            .query_row(params_from_iter(args), |r| r.get(0))
            .optional()
            .map_err(sql)?
    };
    payload.map(|s| decode(&s)).transpose()
}
