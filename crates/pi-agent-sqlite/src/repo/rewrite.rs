use super::*;
fn quiescent(snapshot: &StorageSnapshot) -> Result<(), SessionError> {
    for r in &snapshot.registers {
        match r {
            Register::LaneState { value, .. }
                if value.current_operation_id.is_some() || !value.pending_next_run.is_empty() =>
            {
                return Err(busy_repo());
            }
            Register::OperationMeta { .. }
            | Register::OperationState { .. }
            | Register::OperationToolArguments { .. }
            | Register::OperationPreparation { .. }
            | Register::PendingEntry { .. } => return Err(busy_repo()),
            _ => (),
        }
    }
    Ok(())
}
fn validate(
    source: &SnapshotObservation,
    mut candidate: StorageSnapshot,
    codec: &SessionCodec,
) -> Result<StorageSnapshot, SessionError> {
    quiescent(&candidate)?;
    let originals: BTreeMap<_, _> = source
        .snapshot
        .entries
        .iter()
        .map(|e| (entry_structure(e).id, e))
        .collect();
    let retained: BTreeSet<_> = candidate
        .entries
        .iter()
        .map(|e| entry_structure(e).id)
        .collect();
    for entry in &candidate.entries {
        let structure = entry_structure(entry);
        let original = originals.get(&structure.id).ok_or_else(invalid_repo)?;
        let old = entry_structure(original);
        let mut parent = old.parent_id.clone();
        let mut permitted = structure.parent_id.is_none();
        let mut visited = BTreeSet::new();
        while let Some(id) = parent {
            if !visited.insert(id.clone()) {
                return Err(invalid_repo());
            }
            if Some(&id) == structure.parent_id.as_ref() {
                permitted = true;
                break;
            }
            parent = originals
                .get(&id)
                .ok_or_else(invalid_repo)
                .map(|e| entry_structure(e).parent_id)?;
        }
        if !permitted
            || structure
                .parent_id
                .as_ref()
                .is_some_and(|id| !retained.contains(id))
        {
            return Err(invalid_repo());
        }
        let original = serde_json::to_value(original).map_err(|_| invalid_repo())?;
        let mut normalized = serde_json::to_value(entry).map_err(|_| invalid_repo())?;
        let mut editable = vec!["seq", "parentId"];
        match entry {
            Entry::Message(_) => editable.push("message"),
            Entry::Compaction(_) => editable.extend(["summary", "retainedTail"]),
            Entry::BranchSummary(_) => editable.push("summary"),
            Entry::Custom(_) => (),
        }
        for key in editable {
            if let Some(v) = original.get(key) {
                normalized[key] = v.clone();
            } else {
                normalized
                    .as_object_mut()
                    .ok_or_else(invalid_repo)?
                    .remove(key);
            }
        }
        if normalized != original {
            return Err(invalid_repo());
        }
    }
    for row in &candidate.usage {
        if !source.snapshot.usage.iter().any(|old| old == row) {
            return Err(invalid_repo());
        }
        if row
            .entry_id
            .as_ref()
            .is_some_and(|id| !retained.contains(id))
        {
            return Err(invalid_repo());
        }
    }
    for row in &candidate.registers {
        let key = register_key(row);
        if !source
            .snapshot
            .registers
            .iter()
            .any(|old| register_key(old) == key)
        {
            return Err(invalid_repo());
        }
        match row {
            Register::FactLabel { key, .. } if !retained.iter().any(|id| id.as_str() == key) => {
                return Err(invalid_repo());
            }
            Register::LaneLeaf {
                value: Some(id), ..
            } if !retained.contains(id) => return Err(invalid_repo()),
            _ => (),
        }
    }
    candidate.storage_version = source.metadata.storage_version;
    candidate.store_generation = add(source.metadata.store_generation, n(1)).map_err(as_session)?;
    candidate.stats = SessionStats {
        message_count: SafeUnsigned::new(
            candidate
                .entries
                .iter()
                .filter(|e| matches!(e, Entry::Message(_)))
                .count() as u64,
        )
        .map_err(|_| invalid_repo())?,
        usage: Usage::zero(),
    };
    for row in &candidate.usage {
        commit::add_usage(&mut candidate.stats.usage, &row.usage).map_err(as_session)?;
    }
    data::validate(&candidate, codec)?;
    codec.validate_snapshot(&candidate)?;
    Ok(candidate)
}
impl SqliteSessionRepo {
    /// Explicit trusted retained/redacted rewrite. The callback runs without SQL,
    /// owner or path locks; changed source/fence rejects even a prepared error.
    pub fn rewrite<'a, F>(
        &'a self,
        metadata: SqliteSessionMetadata,
        transform: F,
    ) -> BoxFuture<'a, Result<SqliteSessionMetadata, SessionError>>
    where
        F: FnOnce(&SnapshotObservation) -> Result<StorageSnapshot, SessionError> + Send + 'a,
    {
        Box::pin(async move {
            let path = self.select_path(&metadata.path)?;
            let reserved = self.reserve(path.clone(), None)?;
            if self.header(&path)?.id != metadata.base.id {
                return Err(invalid_repo());
            }
            if let Some(active) = reserved.active()? {
                match active.storage.metadata() {
                    Ok(_) => return Err(busy_repo()),
                    Err(StorageError::Closed(_)) => reserved.clear_active()?,
                    Err(e) => return Err(as_session(e)),
                }
            }
            let operation = owner::Administrative::capture(
                &path,
                self.inner.codec.clone(),
                self.inner.clock.clone(),
                self.inner.options,
            )?;
            if operation.observation.metadata.id != metadata.base.id {
                return Err(invalid_repo());
            }
            quiescent(&operation.observation.snapshot)?;
            self.inner
                .codec
                .validate_snapshot(&operation.observation.snapshot)?;
            let prepared = transform(&operation.observation).and_then(|candidate| {
                validate(&operation.observation, candidate, &self.inner.codec)
            });
            self.inner.phase("before_rewrite_finalize", &path)?;
            let base = operation.finish(prepared)?;
            self.inner.phase("after_rewrite_publish", &path)?;
            Ok(SqliteSessionMetadata { base, path })
        })
    }
}
