use super::{Inner, SearchError};
use crate::{EntrySearchHit, SearchQuery};
use pi_agent_schema::session_values::{FiniteF64, SafeUnsigned};
use pi_agent_schema::{EntryId, EntryStructure, SessionId};
use pi_agent_session::read::{ReadBatch, ReadCursor, RepositoryRead};
use rusqlite::{Connection, OpenFlags, OptionalExtension, TransactionBehavior, params};
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    time::Duration,
};

const SCHEMA: &str = "
CREATE TABLE search_control(singleton INTEGER PRIMARY KEY CHECK(singleton=1), version INTEGER NOT NULL, domain BLOB NOT NULL, projector TEXT NOT NULL, epoch INTEGER NOT NULL CHECK(epoch>=0), barrier INTEGER NOT NULL CHECK(barrier>=0));
CREATE TABLE search_cursors(session TEXT PRIMARY KEY, cursor BLOB NOT NULL);
CREATE TABLE search_documents(rowid INTEGER PRIMARY KEY, session TEXT NOT NULL, entry TEXT NOT NULL, seq INTEGER NOT NULL, timestamp INTEGER NOT NULL, text TEXT NOT NULL, UNIQUE(session,entry));
CREATE VIRTUAL TABLE search_fts USING fts5(text, tokenize='unicode61');
";
fn flags(readonly: bool) -> OpenFlags {
    if readonly {
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX
    } else {
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX
    }
}
pub(super) fn preflight<R: RepositoryRead>(
    path: &Path,
    reader: &R,
) -> Result<PathBuf, SearchError> {
    if reader
        .managed_files()?
        .is_some_and(|files| files.contains_path(path).unwrap_or(true))
    {
        return Err(SearchError::ScopeMismatch);
    }
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let parent = std::fs::canonicalize(parent).map_err(|_| SearchError::InvalidOptions)?;
    let leaf = path.file_name().ok_or(SearchError::InvalidOptions)?;
    let resolved = match std::fs::canonicalize(path) {
        Ok(path) => path,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => parent.join(leaf),
        Err(_) => return Err(SearchError::InvalidOptions),
    };
    Ok(resolved)
}
fn validate(c: &Connection, domain: &[u8], projector: &str) -> Result<bool, SearchError> {
    let count: i64 = c.query_row(
        "SELECT count(*) FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'",
        [],
        |r| r.get(0),
    )?;
    if count == 0 {
        return Ok(false);
    }
    let binding: (i64, Vec<u8>, String) = c
        .query_row(
            "SELECT version,domain,projector FROM search_control WHERE singleton=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .map_err(|_| SearchError::InvalidIndex)?;
    if binding.0 != 1 {
        return Err(SearchError::InvalidIndex);
    }
    if binding.1 != domain || binding.2 != projector {
        return Err(SearchError::ScopeMismatch);
    }
    // Prepare every required authority table before enabling mutable pragmas.
    c.prepare("SELECT epoch,barrier FROM search_control")?;
    c.prepare("SELECT session,cursor FROM search_cursors")?;
    c.prepare("SELECT rowid,session,entry,seq,timestamp,text FROM search_documents")?;
    c.prepare("SELECT rowid,text FROM search_fts")?;
    Ok(true)
}
pub(super) fn initialize(
    path: &Path,
    domain: &[u8],
    projector: &str,
    timeout: Duration,
) -> Result<(), SearchError> {
    match std::fs::metadata(path) {
        Ok(m) if m.len() > 0 => {
            let c = Connection::open_with_flags(path, flags(true))
                .map_err(|_| SearchError::InvalidIndex)?;
            let _ = validate(&c, domain, projector)?;
        }
        Ok(_) => (),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
        Err(_) => return Err(SearchError::InvalidIndex),
    }
    let mut c = Connection::open_with_flags(path, flags(false))?;
    c.busy_timeout(timeout)?;
    let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
    if !validate(&tx, domain, projector)? {
        tx.execute_batch(SCHEMA)?;
        tx.execute(
            "INSERT INTO search_control VALUES(1,1,?,?,0,0)",
            params![domain, projector],
        )?;
    }
    tx.commit()?;
    c.pragma_update(None, "journal_mode", "WAL")?;
    c.pragma_update(None, "synchronous", "FULL")?;
    Ok(())
}
#[derive(Clone)]
pub(super) struct Progress {
    pub epoch: i64,
    pub barrier: i64,
    pub cursor: Option<ReadCursor>,
}
fn progress(c: &Connection, id: Option<&SessionId>) -> Result<Progress, SearchError> {
    let (epoch, barrier): (i64, i64) = c.query_row(
        "SELECT epoch,barrier FROM search_control WHERE singleton=1",
        [],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    let bytes: Option<Vec<u8>> = if let Some(id) = id {
        c.query_row(
            "SELECT cursor FROM search_cursors WHERE session=?",
            [id.as_str()],
            |r| r.get(0),
        )
        .optional()?
    } else {
        None
    };
    let cursor = bytes
        .map(|v| serde_json::from_slice(&v).map_err(|_| SearchError::InvalidIndex))
        .transpose()?;
    Ok(Progress {
        epoch,
        barrier,
        cursor,
    })
}
fn bump(c: &Connection, old: &Progress, barrier: bool) -> Result<i64, SearchError> {
    let epoch = old.epoch.checked_add(1).ok_or(SearchError::Exhausted)?;
    let barrier = old
        .barrier
        .checked_add(i64::from(barrier))
        .ok_or(SearchError::Exhausted)?;
    c.execute(
        "UPDATE search_control SET epoch=?,barrier=? WHERE singleton=1",
        params![epoch, barrier],
    )?;
    Ok(epoch)
}
fn erase(c: &Connection, id: &str) -> Result<(), SearchError> {
    c.execute("DELETE FROM search_fts WHERE rowid IN (SELECT rowid FROM search_documents WHERE session=?)", [id])?;
    c.execute("DELETE FROM search_documents WHERE session=?", [id])?;
    c.execute("DELETE FROM search_cursors WHERE session=?", [id])?;
    Ok(())
}
pub(super) enum Published {
    Done(i64),
    Conflict,
    Discarded,
}
impl<R: RepositoryRead> Inner<R> {
    fn connection(&self) -> Result<Connection, SearchError> {
        // Never silently recreate an index removed under a live service.
        let c = Connection::open_with_flags(
            &self.path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        c.busy_timeout(self.options.busy_timeout)?;
        if !validate(&c, &self.domain, &self.projector_id)? {
            return Err(SearchError::InvalidIndex);
        }
        Ok(c)
    }
    pub(super) fn progress(&self, id: Option<&SessionId>) -> Result<Progress, SearchError> {
        let mut c = self.connection()?;
        let tx = c.transaction()?;
        let value = progress(&tx, id)?;
        tx.commit()?;
        Ok(value)
    }
    pub(super) fn publish(
        &self,
        id: &SessionId,
        expected: &Progress,
        batch: &ReadBatch,
        docs: &[(EntryStructure, String)],
    ) -> Result<Published, SearchError> {
        let mut c = self.connection()?;
        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let actual = progress(&tx, Some(id))?;
        if actual.barrier != expected.barrier {
            return Ok(Published::Discarded);
        }
        if actual.epoch != expected.epoch || actual.cursor != expected.cursor {
            return Ok(Published::Conflict);
        }
        if actual.cursor.as_ref() == Some(&batch.next_cursor)
            && !batch.reset
            && batch.entries.is_empty()
        {
            return Ok(Published::Done(actual.epoch));
        }
        if batch.reset {
            erase(&tx, id.as_str())?;
        }
        for (entry, text) in docs {
            tx.execute("DELETE FROM search_fts WHERE rowid IN (SELECT rowid FROM search_documents WHERE session=? AND entry=?)", params![id.as_str(),entry.id.as_str()])?;
            tx.execute("INSERT INTO search_documents(session,entry,seq,timestamp,text) VALUES(?,?,?,?,?) ON CONFLICT(session,entry) DO UPDATE SET seq=excluded.seq,timestamp=excluded.timestamp,text=excluded.text", params![id.as_str(),entry.id.as_str(),entry.seq.get() as i64,entry.timestamp.get() as i64,text])?;
            tx.execute("INSERT INTO search_fts(rowid,text) SELECT rowid,text FROM search_documents WHERE session=? AND entry=?", params![id.as_str(),entry.id.as_str()])?;
        }
        let bytes =
            serde_json::to_vec(&batch.next_cursor).map_err(|_| SearchError::InvalidIndex)?;
        tx.execute("INSERT INTO search_cursors VALUES(?,?) ON CONFLICT(session) DO UPDATE SET cursor=excluded.cursor", params![id.as_str(),bytes])?;
        let epoch = bump(&tx, &actual, false)?;
        self.phase("before_commit");
        tx.commit()?;
        self.phase("after_commit");
        Ok(Published::Done(epoch))
    }
    pub(super) fn remove_index(&self, id: &SessionId) -> Result<(), SearchError> {
        let mut c = self.connection()?;
        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let actual = progress(&tx, None)?;
        erase(&tx, id.as_str())?;
        bump(&tx, &actual, true)?;
        tx.commit()?;
        // A local hint queued before this acknowledgment cannot undo removal.
        self.state
            .lock()
            .map_err(|_| SearchError::Worker)?
            .queue
            .remove(id.as_str());
        Ok(())
    }
    pub(super) fn reconcile(
        &self,
        ids: &BTreeSet<String>,
        barrier: i64,
        epoch: Option<i64>,
    ) -> Result<(), SearchError> {
        self.phase("before_reconcile");
        let mut c = self.connection()?;
        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let actual = progress(&tx, None)?;
        if epoch != Some(actual.epoch) || actual.barrier != barrier {
            return Ok(());
        }
        let old: Vec<String> = tx
            .prepare("SELECT session FROM search_cursors")?
            .query_map([], |r| r.get(0))?
            .collect::<Result<_, _>>()?;
        let absent: Vec<_> = old.iter().filter(|id| !ids.contains(*id)).collect();
        if !absent.is_empty() {
            for id in absent {
                erase(&tx, id)?;
            }
            bump(&tx, &actual, true)?;
        }
        tx.commit()?;
        Ok(())
    }
    pub(super) fn query(
        &self,
        query: SearchQuery,
        sessions: bool,
    ) -> Result<Vec<EntrySearchHit>, SearchError> {
        let limit = query.limit.map_or(20, |n| n.get());
        if query.text.trim().is_empty() || limit == 0 {
            return Ok(Vec::new());
        }
        let expression = query
            .text
            .split_whitespace()
            .map(|t| format!("\"{}\"", t.replace('"', "\"\"")))
            .collect::<Vec<_>>()
            .join(" AND ");
        let c = self.connection()?;
        let mut stmt = c.prepare("SELECT d.session,d.entry,d.timestamp,d.text,bm25(search_fts) FROM search_fts JOIN search_documents d ON d.rowid=search_fts.rowid WHERE search_fts MATCH ? ORDER BY bm25(search_fts),d.session COLLATE BINARY,d.seq,d.entry COLLATE BINARY LIMIT ?")?;
        let mut rows = stmt.query(params![
            expression,
            if sessions { -1 } else { limit as i64 }
        ])?;
        let mut found = Vec::new();
        let mut seen = BTreeSet::new();
        while let Some(row) = rows.next()? {
            let session: String = row.get(0)?;
            if sessions && !seen.insert(session.clone()) {
                continue;
            }
            let entry: String = row.get(1)?;
            let timestamp: i64 = row.get(2)?;
            let text: String = row.get(3)?;
            let score: f64 = row.get(4)?;
            found.push(EntrySearchHit {
                session_id: SessionId::new(session).map_err(|_| SearchError::InvalidIndex)?,
                entry_id: EntryId::new(entry).map_err(|_| SearchError::InvalidIndex)?,
                timestamp: SafeUnsigned::new(
                    u64::try_from(timestamp).map_err(|_| SearchError::InvalidIndex)?,
                )
                .map_err(|_| SearchError::InvalidIndex)?,
                snippet: Some(text.chars().take(240).collect()),
                score: Some(FiniteF64::new(score).map_err(|_| SearchError::InvalidIndex)?),
            });
            if found.len() as u64 >= limit {
                break;
            }
        }
        Ok(found)
    }
}
