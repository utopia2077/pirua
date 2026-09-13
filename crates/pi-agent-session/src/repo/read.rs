use super::*;
use crate::read::*;
impl RepositoryRead for MemorySessionRepo {
    type Metadata = SessionMetadata;
    type ListOptions = ();
    fn source_domain(&self) -> Result<SourceDomain, SessionError> {
        self.inner.read_domain.get()
    }
    fn managed_files(&self) -> Result<Option<ManagedFileDomain>, SessionError> {
        Ok(None)
    }
    fn read_list(&self, _: ()) -> Result<Vec<SessionMetadata>, SessionError> {
        let ids: Vec<_> = self
            .inner
            .lock()?
            .records
            .iter()
            .filter(|(_, r)| r.identity.is_some())
            .map(|(id, _)| id.clone())
            .collect();
        let mut values = Vec::new();
        for id in ids {
            let claim = match self.claim(&id) {
                Ok(c) => c,
                Err(SessionError::NotFound(_)) => continue,
                Err(e) => return Err(e),
            };
            let (identity, backing, _) = claim.record()?;
            let snapshot = backing.read_snapshot().map_err(storage)?;
            values.push(identity.metadata(&snapshot));
        }
        Ok(values)
    }
    fn read_batch(
        &self,
        metadata: &SessionMetadata,
        request: ReadBatchRequest,
    ) -> Result<ReadBatch, SessionError> {
        let claim = self.claim(&metadata.id)?;
        let (identity, backing, _) = claim.record()?;
        let snapshot = backing.read_snapshot().map_err(storage)?;
        self.require_current(snapshot.storage_version)?;
        self.inner.codec.validate_snapshot(&snapshot)?;
        prefix_batch(identity.metadata(&snapshot), snapshot.entries, request)
    }
}
