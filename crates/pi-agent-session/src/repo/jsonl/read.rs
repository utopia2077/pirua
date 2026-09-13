use super::*;
use crate::read::*;
impl RepositoryRead for JsonlSessionRepo {
    type Metadata = JsonlSessionMetadata;
    type ListOptions = ();
    fn source_domain(&self) -> Result<SourceDomain, SessionError> {
        SourceDomain::for_files("jsonl", &self.inner.root)
    }
    fn managed_files(&self) -> Result<Option<ManagedFileDomain>, SessionError> {
        Ok(Some(ManagedFileDomain::new(&self.inner.root, ".jsonl")?))
    }
    fn read_list(&self, _: ()) -> Result<Vec<JsonlSessionMetadata>, SessionError> {
        self.discovered()
    }
    fn read_batch(
        &self,
        metadata: &JsonlSessionMetadata,
        request: ReadBatchRequest,
    ) -> Result<ReadBatch, SessionError> {
        let path = std::fs::canonicalize(&metadata.path).map_err(io)?;
        let reservation = self.reserve(path.clone(), None)?;
        let mut observed = None;
        if let Some(active) = reservation.active()? {
            match active.inner.storage.read_snapshot() {
                Ok(value) => observed = Some(value),
                Err(error) if is_closed(&error) => (),
                Err(error) => return Err(storage(error)),
            }
        }
        let (base, snapshot) = if let Some(value) = observed {
            value
        } else {
            let mut claim = FileClaim::acquire(&path).map_err(storage)?;
            let base = header::read(&mut claim)?;
            if base.id != metadata.base.id {
                return Err(invalid());
            }
            if header::is_legacy(&claim.read_source().map_err(storage)?)? {
                let prepared = self.normalized(&mut claim, base)?;
                (prepared.metadata().clone(), prepared.snapshot().clone())
            } else {
                require_current(base.storage_version, self.inner.migrations.target())?;
                (
                    base,
                    claim
                        .read_raw()
                        .map_err(storage)?
                        .current(&self.inner.codec)?,
                )
            }
        };
        if base.id != metadata.base.id {
            return Err(invalid());
        }
        require_current(base.storage_version, self.inner.migrations.target())?;
        self.inner.codec.validate_snapshot(&snapshot)?;
        prefix_batch(base, snapshot.entries, request)
    }
}
fn require_current(actual: SafeUnsigned, current: SafeUnsigned) -> Result<(), SessionError> {
    if actual > current {
        Err(SessionError::NewerStorageVersion(context(
            "stored version exceeds readonly target",
        )))
    } else if actual < current {
        Err(SessionError::Migration(context(
            "readonly source requires migration",
        )))
    } else {
        Ok(())
    }
}
