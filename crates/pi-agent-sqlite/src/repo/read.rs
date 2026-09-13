use super::*;
use pi_agent_session::read::{
    ManagedFileDomain, ReadBatch, ReadBatchRequest, RepositoryRead, SourceDomain,
};
impl SqliteSessionRepo {
    pub(super) fn header(&self, path: &Path) -> Result<SessionMetadata, SessionError> {
        let _scope = path::Scope::acquire(path, false).map_err(as_session)?;
        path::inspect_identity(path).map_err(as_session)?;
        let mut db = path::connection(path, self.inner.options, true).map_err(as_session)?;
        let tx = db.transaction().map_err(sql).map_err(as_session)?;
        data::current(&tx)?;
        let header = data::header(&tx).map_err(as_session)?;
        tx.commit().map_err(sql).map_err(as_session)?;
        Ok(header.metadata)
    }
    pub(super) fn discovered(
        &self,
        own: Option<&Reservation>,
    ) -> Result<Vec<SqliteSessionMetadata>, SessionError> {
        let mut paths = Vec::new();
        for entry in std::fs::read_dir(&self.inner.root).map_err(|e| as_session(io(e)))? {
            let entry = entry.map_err(|e| as_session(io(e)))?;
            if entry.file_name().as_encoded_bytes().ends_with(b".sqlite") {
                paths.push(entry.path());
            }
        }
        paths.sort();
        let mut ids = BTreeSet::new();
        let mut result = Vec::new();
        for path in paths {
            let path = self.select_path(&path)?;
            {
                let c = self.inner.lock()?;
                if let Some(ticket) = c.records.get(&path).and_then(|r| r.pending)
                    && !own.is_some_and(|own| own.path == path && own.ticket == ticket)
                {
                    return Err(busy_repo());
                }
            }
            let base = self.header(&path)?;
            if !ids.insert(base.id.clone()) {
                return Err(invalid_repo());
            }
            result.push(SqliteSessionMetadata { base, path });
        }
        result.sort_by(|a, b| a.base.id.cmp(&b.base.id));
        Ok(result)
    }
}
impl RepositoryRead for SqliteSessionRepo {
    type Metadata = SqliteSessionMetadata;
    type ListOptions = ();
    fn source_domain(&self) -> Result<SourceDomain, SessionError> {
        SourceDomain::for_files("sqlite-session-v1", &self.inner.root)
    }
    fn managed_files(&self) -> Result<Option<ManagedFileDomain>, SessionError> {
        ManagedFileDomain::new(&self.inner.root, ".sqlite").map(Some)
    }
    fn read_list(&self, _: ()) -> Result<Vec<SqliteSessionMetadata>, SessionError> {
        self.discovered(None)
    }
    fn read_batch(
        &self,
        metadata: &SqliteSessionMetadata,
        request: ReadBatchRequest,
    ) -> Result<ReadBatch, SessionError> {
        let path = self.select_path(&metadata.path)?;
        let batch =
            SqliteStorage::read_entry_page(path, &self.inner.codec, self.inner.options, request)?;
        if batch.metadata.id != metadata.base.id {
            return Err(invalid_repo());
        }
        Ok(batch)
    }
}
