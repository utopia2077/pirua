//! SQLite FTS5 derived index, bound to one repository and text policy.
mod database;
mod lifecycle;
mod project;

use crate::{
    EntrySearchHit, SearchQuery, SessionSearchHit, SessionSearchService, SessionSearchTopEntry,
};
use lifecycle::State;
use pi_agent_schema::session_values::SafeUnsigned;
use pi_agent_schema::{SessionError, SessionId, SessionMetadataRecord};
use pi_agent_session::read::{ReadBatchRequest, RepositoryRead};
pub use project::{DefaultTextProjector, TextProjector};
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    sync::{Arc, Condvar, Mutex},
    time::Duration,
};

/// Redacted, nonserialized derived-search failure. Source failures retain their
/// already-redacted repository error; SQLite errors never expose SQL or paths.
#[derive(Debug)]
#[non_exhaustive]
pub enum SearchError {
    Closed,
    Busy,
    InvalidOptions,
    ScopeMismatch,
    InvalidIndex,
    Database,
    Source(SessionError),
    Projection,
    Exhausted,
    Worker,
}
impl std::fmt::Display for SearchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Closed => "search service closed",
            Self::Busy => "search service busy",
            Self::InvalidOptions => "invalid search options",
            Self::ScopeMismatch => "search scope mismatch",
            Self::InvalidIndex => "invalid derived search index",
            Self::Database => "search database failure",
            Self::Source(_) => "search source failure",
            Self::Projection => "search projection failure",
            Self::Exhausted => "search counter exhausted",
            Self::Worker => "search worker failure",
        })
    }
}
impl std::error::Error for SearchError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        if let Self::Source(e) = self {
            Some(e)
        } else {
            None
        }
    }
}
impl From<SessionError> for SearchError {
    fn from(e: SessionError) -> Self {
        Self::Source(e)
    }
}
impl From<rusqlite::Error> for SearchError {
    fn from(e: rusqlite::Error) -> Self {
        match e.sqlite_error_code() {
            Some(rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked) => {
                Self::Busy
            }
            _ => Self::Database,
        }
    }
}

/// Local backend settings; query DTOs and repository listing options stay separate.
#[derive(Clone, Debug)]
pub struct SearchOptions {
    pub batch_size: SafeUnsigned,
    pub debounce: Duration,
    pub busy_timeout: Duration,
}
impl Default for SearchOptions {
    fn default() -> Self {
        Self {
            batch_size: SafeUnsigned::new(256).expect("constant"),
            debounce: Duration::from_millis(50),
            busy_timeout: Duration::from_secs(5),
        }
    }
}

#[cfg(test)]
type PhaseHook = Arc<dyn Fn(&str) + Send + Sync>;

struct Inner<R: RepositoryRead> {
    reader: Arc<R>,
    list_options: R::ListOptions,
    path: PathBuf,
    domain: Vec<u8>,
    projector_id: String,
    projector: Arc<dyn TextProjector>,
    options: SearchOptions,
    state: Mutex<State>,
    wake: Condvar,
    worker: Mutex<Option<std::thread::JoinHandle<()>>>,
    #[cfg(test)]
    hook: Mutex<Option<PhaseHook>>,
}

/// A service owns its worker; dropping it seals and drains that worker. It reads
/// only RepositoryRead and never opens or closes a canonical Session writer.
pub struct SqliteSearchService<R: RepositoryRead> {
    inner: Arc<Inner<R>>,
}
impl<R> SqliteSearchService<R>
where
    R: RepositoryRead + 'static,
    R::ListOptions: Clone + Send + Sync + 'static,
{
    pub fn new(
        path: impl AsRef<Path>,
        reader: Arc<R>,
        list_options: R::ListOptions,
    ) -> Result<Self, SearchError> {
        Self::with_options(
            path,
            reader,
            list_options,
            SearchOptions::default(),
            Arc::new(DefaultTextProjector),
        )
    }
    pub fn with_options(
        path: impl AsRef<Path>,
        reader: Arc<R>,
        list_options: R::ListOptions,
        options: SearchOptions,
        projector: Arc<dyn TextProjector>,
    ) -> Result<Self, SearchError> {
        if options.batch_size.get() == 0
            || options.busy_timeout.is_zero()
            || options.busy_timeout.as_millis() > i32::MAX as u128
            || std::time::Instant::now()
                .checked_add(options.debounce)
                .is_none()
            || projector.identity().is_empty()
        {
            return Err(SearchError::InvalidOptions);
        }
        let path = database::preflight(path.as_ref(), reader.as_ref())?;
        let domain = reader.source_domain()?.as_bytes().to_vec();
        let projector_id = projector.identity().to_owned();
        database::initialize(&path, &domain, &projector_id, options.busy_timeout)?;
        let inner = Arc::new(Inner {
            reader,
            list_options,
            path,
            domain,
            projector_id,
            projector,
            options,
            state: Mutex::new(State::default()),
            wake: Condvar::new(),
            worker: Mutex::new(None),
            #[cfg(test)]
            hook: Mutex::new(None),
        });
        let worker_inner = inner.clone();
        let worker = std::thread::Builder::new()
            .name("session-search".into())
            .spawn(move || worker_inner.run_worker())
            .map_err(|_| SearchError::Worker)?;
        *inner.worker.lock().map_err(|_| SearchError::Worker)? = Some(worker);
        Ok(Self { inner })
    }
    /// Drain bounded background failures. No query/source text or file path is retained.
    pub fn take_background_errors(&self) -> Result<Vec<SearchError>, SearchError> {
        let mut state = self.inner.state.lock().map_err(|_| SearchError::Worker)?;
        Ok(state.errors.drain(..).collect())
    }
}
impl<R: RepositoryRead> Drop for SqliteSearchService<R> {
    fn drop(&mut self) {
        let _ = self.inner.shutdown(true);
    }
}
impl<R> SessionSearchService for SqliteSearchService<R>
where
    R: RepositoryRead + 'static,
    R::ListOptions: Clone + Send + Sync + 'static,
{
    type Error = SearchError;
    fn search_sessions(
        &self,
        query: SearchQuery,
    ) -> pi_agent_schema::surface::BoxFuture<'_, Result<Vec<SessionSearchHit>, SearchError>> {
        Box::pin(async move {
            let _admission = self.inner.admit()?;
            let entries = self.inner.query(query, true)?;
            Ok(entries
                .into_iter()
                .map(|e| SessionSearchHit {
                    session_id: e.session_id,
                    score: e.score,
                    top: Some(SessionSearchTopEntry {
                        entry_id: e.entry_id,
                        snippet: e.snippet,
                        timestamp: e.timestamp,
                    }),
                })
                .collect())
        })
    }
    fn search_entries(
        &self,
        query: SearchQuery,
    ) -> Option<pi_agent_schema::surface::BoxFuture<'_, Result<Vec<EntrySearchHit>, SearchError>>>
    {
        Some(Box::pin(async move {
            let _admission = self.inner.admit()?;
            self.inner.query(query, false)
        }))
    }
    fn sync(&self) -> pi_agent_schema::surface::BoxFuture<'_, Result<(), SearchError>> {
        Box::pin(async move {
            let _admission = self.inner.admit()?;
            self.inner.sweep(None)
        })
    }
    fn notify(&self, id: &SessionId) {
        self.inner.enqueue(id);
    }
    fn remove(
        &self,
        id: SessionId,
    ) -> pi_agent_schema::surface::BoxFuture<'_, Result<(), SearchError>> {
        Box::pin(async move {
            let _admission = self.inner.admit()?;
            self.inner.remove_index(&id)
        })
    }
    fn close(&self) -> pi_agent_schema::surface::BoxFuture<'_, Result<(), SearchError>> {
        Box::pin(async move { self.inner.shutdown(false) })
    }
}
impl<R> Inner<R>
where
    R: RepositoryRead + 'static,
    R::ListOptions: Clone + Send + Sync + 'static,
{
    fn sweep(&self, only: Option<&SessionId>) -> Result<(), SearchError> {
        self.sweep_at(only, None)
    }
    fn sweep_at(
        &self,
        only: Option<&SessionId>,
        hint_barrier: Option<i64>,
    ) -> Result<(), SearchError> {
        let initial = self.progress(None)?;
        if hint_barrier.is_some_and(|barrier| barrier != initial.barrier) {
            return Ok(());
        }
        let mut sweep_epoch = Some(initial.epoch);
        self.phase("before_list");
        let listed = self.reader.read_list(self.list_options.clone())?;
        self.phase("after_list");
        let ids: BTreeSet<_> = listed
            .iter()
            .map(|m| m.base_metadata().id.as_str().to_owned())
            .collect();
        for metadata in &listed {
            if only.is_some_and(|id| id != &metadata.base_metadata().id) {
                continue;
            }
            if !self.catch_up(metadata, initial.barrier, &mut sweep_epoch)? {
                return Ok(());
            }
        }
        if only.is_none() {
            self.reconcile(&ids, initial.barrier, sweep_epoch)?;
        }
        Ok(())
    }
    fn catch_up(
        &self,
        metadata: &R::Metadata,
        barrier: i64,
        sweep_epoch: &mut Option<i64>,
    ) -> Result<bool, SearchError> {
        let id = &metadata.base_metadata().id;
        loop {
            let expected = self.progress(Some(id))?;
            if expected.barrier != barrier {
                return Ok(false);
            }
            if *sweep_epoch != Some(expected.epoch) {
                *sweep_epoch = None;
            }
            let batch = self.reader.read_batch(
                metadata,
                ReadBatchRequest {
                    cursor: expected.cursor.clone(),
                    limit: self.options.batch_size,
                },
            )?;
            self.phase("after_batch");
            if batch.metadata.id != *id
                || batch.metadata.store_generation != batch.next_cursor.generation
            {
                return Err(SearchError::InvalidIndex);
            }
            let mut documents = Vec::new();
            for entry in &batch.entries {
                // A custom policy chooses message text, never nonmessage authority.
                if matches!(entry, pi_agent_schema::Entry::Message(_))
                    && let Some(text) = self.projector.project(entry)?
                    && !text.is_empty()
                {
                    documents.push((
                        pi_agent_session::backend_support::entry_structure(entry),
                        text,
                    ));
                }
            }
            match self.publish(id, &expected, &batch, &documents)? {
                database::Published::Discarded => return Ok(false),
                database::Published::Conflict => {
                    *sweep_epoch = None;
                    continue;
                }
                database::Published::Done(epoch) => {
                    if sweep_epoch.is_some() {
                        *sweep_epoch = Some(epoch);
                    }
                    if batch.complete {
                        return Ok(true);
                    }
                    if expected.cursor.as_ref() == Some(&batch.next_cursor) {
                        return Err(SearchError::InvalidIndex);
                    }
                }
            }
        }
    }
}
impl<R: RepositoryRead> Inner<R> {
    fn phase(&self, _name: &str) {
        #[cfg(test)]
        {
            let hook = self.hook.lock().expect("test hook").clone();
            if let Some(hook) = hook {
                hook(_name);
            }
        }
    }
}
#[cfg(test)]
mod tests;
