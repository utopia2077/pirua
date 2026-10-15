use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::Mutex,
};

use pi_sqlite::{Database, OptionalExtension, params};

use crate::{EntryContent, Error, SessionCommit, SessionInfo, SessionSnapshot, sqlite};

/// Atomic current-format storage. Revisions prevent stale managers from losing updates.
pub trait SessionStore: Send + Sync {
    fn create(&self, snapshot: &SessionSnapshot) -> Result<(), Error>;
    fn load(&self, id: &str) -> Result<SessionSnapshot, Error>;
    fn commit(&self, id: &str, commit: &SessionCommit) -> Result<u64, Error>;
    fn list(&self, cwd: Option<&str>) -> Result<Vec<SessionInfo>, Error>;
}

#[derive(Default)]
pub struct MemoryStore(Mutex<BTreeMap<String, SessionSnapshot>>);

impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl SessionStore for MemoryStore {
    fn create(&self, snapshot: &SessionSnapshot) -> Result<(), Error> {
        validate_snapshot(snapshot)?;
        let mut sessions = self.0.lock().map_err(|_| Error::Poisoned)?;
        if sessions.contains_key(&snapshot.header.id) {
            return Err(Error::AlreadyExists(snapshot.header.id.clone()));
        }
        sessions.insert(snapshot.header.id.clone(), snapshot.clone());
        Ok(())
    }

    fn load(&self, id: &str) -> Result<SessionSnapshot, Error> {
        self.0
            .lock()
            .map_err(|_| Error::Poisoned)?
            .get(id)
            .cloned()
            .ok_or_else(|| Error::NotFound(id.to_owned()))
    }

    fn commit(&self, id: &str, commit: &SessionCommit) -> Result<u64, Error> {
        let mut sessions = self.0.lock().map_err(|_| Error::Poisoned)?;
        let snapshot = sessions
            .get_mut(id)
            .ok_or_else(|| Error::NotFound(id.to_owned()))?;
        let revision = validate_commit(snapshot, commit)?;
        snapshot.entries.extend(commit.append.clone());
        snapshot.leaf_id = commit.leaf_id.clone();
        snapshot.revision = revision;
        Ok(revision)
    }

    fn list(&self, cwd: Option<&str>) -> Result<Vec<SessionInfo>, Error> {
        let sessions = self.0.lock().map_err(|_| Error::Poisoned)?;
        let mut result = sessions
            .values()
            .filter(|s| cwd.is_none_or(|cwd| s.header.cwd == cwd))
            .map(session_info)
            .collect::<Vec<_>>();
        sort_sessions(&mut result);
        Ok(result)
    }
}

const SCHEMA: &str = "
CREATE TABLE pirua_coding_sessions (
    id TEXT PRIMARY KEY NOT NULL,
    cwd TEXT NOT NULL,
    header TEXT NOT NULL CHECK (json_valid(header)),
    revision INTEGER NOT NULL CHECK (revision >= 0),
    leaf_id TEXT
) STRICT;
CREATE INDEX pirua_coding_sessions_cwd ON pirua_coding_sessions(cwd, id);
CREATE TABLE pirua_coding_entries (
    session_id TEXT NOT NULL REFERENCES pirua_coding_sessions(id),
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    id TEXT NOT NULL,
    record TEXT NOT NULL CHECK (json_valid(record)),
    PRIMARY KEY (session_id, id),
    UNIQUE (session_id, ordinal)
) STRICT;";

/// SQLite implementation of the same logical contract as MemoryStore.
#[derive(Clone)]
pub struct SqliteStore {
    database: Database,
}

impl SqliteStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        Self::from_database(Database::open(path)?)
    }

    pub fn from_database(database: Database) -> Result<Self, Error> {
        database.initialize("coding-session", 1, SCHEMA)?;
        Ok(Self { database })
    }

    pub fn database(&self) -> &Database {
        &self.database
    }
}

fn load(connection: &pi_sqlite::Connection, id: &str) -> Result<SessionSnapshot, Error> {
    let metadata: Option<(String, i64, Option<String>)> = connection
        .query_row(
            "SELECT header, revision, leaf_id FROM pirua_coding_sessions WHERE id = ?1",
            [id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .map_err(sqlite)?;
    let (header, revision, leaf_id) = metadata.ok_or_else(|| Error::NotFound(id.to_owned()))?;
    let mut statement = connection
        .prepare("SELECT record FROM pirua_coding_entries WHERE session_id = ?1 ORDER BY ordinal")
        .map_err(sqlite)?;
    let records = statement
        .query_map([id], |row| row.get::<_, String>(0))
        .map_err(sqlite)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(sqlite)?;
    let snapshot = SessionSnapshot {
        header: serde_json::from_str(&header)?,
        revision: u64::try_from(revision)
            .map_err(|_| Error::Invalid("negative stored session revision".to_owned()))?,
        leaf_id,
        entries: records
            .iter()
            .map(|record| serde_json::from_str(record))
            .collect::<Result<Vec<_>, _>>()?,
    };
    if snapshot.header.id != id {
        return Err(Error::Invalid(
            "stored session identity does not match its key".to_owned(),
        ));
    }
    validate_snapshot(&snapshot)?;
    Ok(snapshot)
}

fn insert_entries(
    tx: &pi_sqlite::Transaction<'_>,
    session: &str,
    start: usize,
    entries: &[crate::SessionEntry],
) -> Result<(), Error> {
    let mut statement = tx.prepare(
        "INSERT INTO pirua_coding_entries (session_id, ordinal, id, record) VALUES (?1, ?2, ?3, ?4)"
    ).map_err(sqlite)?;
    for (offset, entry) in entries.iter().enumerate() {
        let ordinal = i64::try_from(start + offset)
            .map_err(|_| Error::Invalid("too many session entries".to_owned()))?;
        statement
            .execute(params![
                session,
                ordinal,
                entry.id,
                serde_json::to_string(entry)?
            ])
            .map_err(sqlite)?;
    }
    Ok(())
}

impl SessionStore for SqliteStore {
    fn create(&self, snapshot: &SessionSnapshot) -> Result<(), Error> {
        validate_snapshot(snapshot)?;
        self.database.transaction(|tx| {
            let exists: bool = tx
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM pirua_coding_sessions WHERE id = ?1)",
                    [&snapshot.header.id],
                    |row| row.get(0),
                )
                .map_err(sqlite)?;
            if exists {
                return Err(Error::AlreadyExists(snapshot.header.id.clone()));
            }
            tx.execute(
                "INSERT INTO pirua_coding_sessions (id, cwd, header, revision, leaf_id)
                VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    snapshot.header.id,
                    snapshot.header.cwd,
                    serde_json::to_string(&snapshot.header)?,
                    i64::try_from(snapshot.revision).map_err(|_| Error::Invalid(
                        "session revision exceeds SQLite integer".to_owned()
                    ))?,
                    snapshot.leaf_id
                ],
            )
            .map_err(sqlite)?;
            insert_entries(tx, &snapshot.header.id, 0, &snapshot.entries)
        })
    }

    fn load(&self, id: &str) -> Result<SessionSnapshot, Error> {
        self.database.snapshot(|connection| load(connection, id))
    }

    fn commit(&self, id: &str, commit: &SessionCommit) -> Result<u64, Error> {
        self.database.transaction(|tx| {
            let snapshot = load(tx, id)?;
            let revision = validate_commit(&snapshot, commit)?;
            insert_entries(tx, id, snapshot.entries.len(), &commit.append)?;
            tx.execute(
                "UPDATE pirua_coding_sessions SET revision = ?1, leaf_id = ?2 WHERE id = ?3",
                params![
                    i64::try_from(revision).map_err(|_| Error::Invalid(
                        "session revision exceeds SQLite integer".to_owned()
                    ))?,
                    commit.leaf_id,
                    id
                ],
            )
            .map_err(sqlite)?;
            Ok(revision)
        })
    }

    fn list(&self, cwd: Option<&str>) -> Result<Vec<SessionInfo>, Error> {
        self.database.snapshot(|connection| {
            let mut statement = connection.prepare(
                "SELECT id FROM pirua_coding_sessions WHERE (?1 IS NULL OR cwd = ?1) ORDER BY id"
            ).map_err(sqlite)?;
            let ids = statement
                .query_map([cwd], |row| row.get::<_, String>(0))
                .map_err(sqlite)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(sqlite)?;
            let mut result = ids
                .iter()
                .map(|id| load(connection, id).map(|snapshot| session_info(&snapshot)))
                .collect::<Result<Vec<_>, Error>>()?;
            sort_sessions(&mut result);
            Ok(result)
        })
    }
}

fn sort_sessions(sessions: &mut [SessionInfo]) {
    sessions.sort_by(|a, b| {
        b.last_timestamp
            .cmp(&a.last_timestamp)
            .then_with(|| a.header.id.cmp(&b.header.id))
    });
}

fn session_info(snapshot: &SessionSnapshot) -> SessionInfo {
    SessionInfo {
        header: snapshot.header.clone(),
        name: snapshot
            .entries
            .iter()
            .rev()
            .find_map(|entry| match &entry.content {
                EntryContent::SessionInfo { name } => Some(name.trim().to_owned()),
                _ => None,
            })
            .filter(|name| !name.is_empty()),
        entry_count: snapshot.entries.len(),
        last_timestamp: snapshot
            .entries
            .last()
            .map_or(snapshot.header.timestamp, |entry| entry.timestamp),
    }
}

pub(crate) fn validate_snapshot(snapshot: &SessionSnapshot) -> Result<(), Error> {
    if snapshot.header.id.is_empty()
        || snapshot.header.cwd.is_empty()
        || snapshot.revision > i64::MAX as u64
    {
        return Err(Error::Invalid(
            "invalid session header or revision".to_owned(),
        ));
    }
    let mut ids = BTreeSet::new();
    validate_entries(&mut ids, &snapshot.entries)?;
    check_leaf(&ids, snapshot.leaf_id.as_deref())
}

fn validate_entries<'a>(
    ids: &mut BTreeSet<&'a str>,
    entries: &'a [crate::SessionEntry],
) -> Result<(), Error> {
    for entry in entries {
        if entry.id.is_empty() || ids.contains(entry.id.as_str()) {
            return Err(Error::AlreadyExists(entry.id.clone()));
        }
        if let Some(parent) = &entry.parent_id
            && !ids.contains(parent.as_str())
        {
            return Err(Error::NotFound(parent.clone()));
        }
        if let EntryContent::Message { message } = &entry.content
            && (!message.is_object()
                || message
                    .get("role")
                    .and_then(|r| r.as_str())
                    .is_none_or(str::is_empty))
        {
            return Err(Error::Invalid(
                "session message requires a nonempty role".to_owned(),
            ));
        }
        ids.insert(entry.id.as_str());
    }
    Ok(())
}

fn check_leaf(ids: &BTreeSet<&str>, leaf: Option<&str>) -> Result<(), Error> {
    if let Some(leaf) = leaf
        && !ids.contains(leaf)
    {
        return Err(Error::NotFound(leaf.to_owned()));
    }
    Ok(())
}

pub(crate) fn validate_commit(
    snapshot: &SessionSnapshot,
    commit: &SessionCommit,
) -> Result<u64, Error> {
    if snapshot.revision != commit.expected_revision {
        return Err(Error::Conflict {
            expected: commit.expected_revision,
            actual: snapshot.revision,
        });
    }
    let revision = snapshot
        .revision
        .checked_add(1)
        .filter(|r| *r <= i64::MAX as u64)
        .ok_or_else(|| Error::Invalid("session revision exhausted".to_owned()))?;
    let mut ids = snapshot
        .entries
        .iter()
        .map(|entry| entry.id.as_str())
        .collect::<BTreeSet<_>>();
    validate_entries(&mut ids, &commit.append)?;
    check_leaf(&ids, commit.leaf_id.as_deref())?;
    Ok(revision)
}
