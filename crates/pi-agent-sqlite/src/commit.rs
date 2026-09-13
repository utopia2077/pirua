//! SQL point-read mechanical staging; no Memory canonical state or ordinary full scan.
use super::*;
#[derive(Clone)]
pub(crate) enum Committed {
    Entry(Entry),
    Usage(UsageRow),
    Set(Register),
    Delete(RegisterKey),
}
pub(crate) struct Capture {
    pub header: data::Header,
    entries: BTreeMap<String, Option<EntryStructure>>,
    ids: BTreeSet<String>,
    pending: Option<BTreeSet<String>>,
    full: Option<StorageSnapshot>,
}
pub(crate) struct Prepared {
    pub header: data::Header,
    pub result: CommitResult,
    pub writes: Vec<Committed>,
    pub projection: IndexedProjection,
    pub full: Option<StorageSnapshot>,
}
fn entry_id(entry: &NewEntry) -> &EntryId {
    match entry {
        NewEntry::Message(v) => &v.id,
        NewEntry::Custom(v) => &v.id,
        NewEntry::Compaction(v) => &v.id,
        NewEntry::BranchSummary(v) => &v.id,
    }
}
fn parent(entry: &NewEntry) -> Option<&EntryId> {
    match entry {
        NewEntry::Message(v) => v.parent_id.as_ref(),
        NewEntry::Custom(v) => v.parent_id.as_ref(),
        NewEntry::Compaction(v) => v.parent_id.as_ref(),
        NewEntry::BranchSummary(v) => v.parent_id.as_ref(),
    }
}
pub(crate) fn capture(
    db: &Connection,
    header: data::Header,
    request: &ConditionalCommit,
    read: Option<&IndexedReadSet>,
    codec: &SessionCodec,
) -> Result<Option<Capture>, StorageError> {
    for condition in &request.conditions {
        let actual = db
            .query_row(
                "SELECT seq FROM registers WHERE namespace=?1 AND key=?2",
                params![
                    scalar(&condition.register.namespace)?,
                    condition.register.key
                ],
                |r| r.get::<_, i64>(0),
            )
            .optional()
            .map_err(sql)?
            .map(data::safe)
            .transpose()?;
        let matches = match condition.expected {
            ExpectedRegister::Absent => actual.is_none(),
            ExpectedRegister::Seq(seq) => actual == Some(seq),
        };
        if !matches {
            return Ok(None);
        }
    }
    let mut requested = read.map(|r| r.entry_ids.clone()).unwrap_or_default();
    let pending = if read.is_some_and(|r| r.all_pending_keys) {
        let mut statement = db
            .prepare("SELECT key FROM registers WHERE namespace=?1 ORDER BY key")
            .map_err(sql)?;
        let keys = statement
            .query_map([scalar(&RegisterNamespace::PendingEntry)?], |r| {
                r.get::<_, String>(0)
            })
            .map_err(sql)?
            .collect::<rusqlite::Result<BTreeSet<_>>>()
            .map_err(sql)?;
        requested.extend(keys.iter().cloned());
        Some(keys)
    } else {
        None
    };
    let mut written_ids = BTreeSet::new();
    for write in &request.transaction.writes {
        match write {
            Write::Entry(entry) => {
                let id = entry_id(entry).to_string();
                written_ids.insert(id.clone());
                requested.insert(id);
                if let Some(parent) = parent(entry) {
                    requested.insert(parent.to_string());
                }
            }
            Write::Usage(row) => {
                written_ids.insert(row.id.to_string());
                requested.insert(row.id.to_string());
                if let Some(entry) = &row.entry_id {
                    requested.insert(entry.to_string());
                }
            }
            Write::SetRegister(value) => match value.as_ref() {
                RegisterSet::LaneLeaf {
                    value: Some(id), ..
                } => {
                    requested.insert(id.to_string());
                }
                RegisterSet::PendingEntry { key, .. } => {
                    requested.insert(key.clone());
                }
                _ => (),
            },
            _ => (),
        }
    }
    let entries = data::structures(db, &requested)?;
    let mut ids: BTreeSet<String> = entries
        .iter()
        .filter(|(_, value)| value.is_some())
        .map(|(id, _)| id.clone())
        .collect();
    let mut statement = db
        .prepare("SELECT id FROM usage_ledger WHERE id=?1")
        .map_err(sql)?;
    for id in written_ids {
        if statement
            .query_row([&id], |r| r.get::<_, String>(0))
            .optional()
            .map_err(sql)?
            .is_some()
        {
            ids.insert(id);
        }
    }
    let full = if read.is_none() {
        Some(data::snapshot(db, &header, codec)?)
    } else {
        None
    };
    Ok(Some(Capture {
        header,
        entries,
        ids,
        pending,
        full,
    }))
}
pub(crate) fn add_usage(a: &mut Usage, b: &Usage) -> Result<(), StorageError> {
    fn optional(
        a: Option<SafeUnsigned>,
        b: Option<SafeUnsigned>,
    ) -> Result<Option<SafeUnsigned>, StorageError> {
        match (a, b) {
            (None, None) => Ok(None),
            (Some(v), None) | (None, Some(v)) => Ok(Some(v)),
            (Some(a), Some(b)) => add(a, b).map(Some),
        }
    }
    fn cost(a: FiniteF64, b: FiniteF64) -> Result<FiniteF64, StorageError> {
        FiniteF64::new(a.get() + b.get()).map_err(|_| invalid("usage cost overflow"))
    }
    a.input = add(a.input, b.input)?;
    a.output = add(a.output, b.output)?;
    a.cache_read = add(a.cache_read, b.cache_read)?;
    a.cache_write = add(a.cache_write, b.cache_write)?;
    a.cache_write_1h = optional(a.cache_write_1h, b.cache_write_1h)?;
    a.reasoning = optional(a.reasoning, b.reasoning)?;
    a.total_tokens = add(a.total_tokens, b.total_tokens)?;
    a.cost.input = cost(a.cost.input, b.cost.input)?;
    a.cost.output = cost(a.cost.output, b.cost.output)?;
    a.cost.cache_read = cost(a.cost.cache_read, b.cost.cache_read)?;
    a.cost.cache_write = cost(a.cost.cache_write, b.cost.cache_write)?;
    a.cost.total = cost(a.cost.total, b.cost.total)?;
    Ok(())
}
pub(crate) fn prepare(
    mut capture: Capture,
    transaction: Transaction,
    timestamp: SafeUnsigned,
) -> Result<Prepared, StorageError> {
    let first_seq = capture
        .header
        .next
        .ok_or_else(|| invalid("sequence space exhausted"))?;
    let mut seqs = vec![];
    let mut writes = vec![];
    for write in transaction.writes {
        let seq = capture
            .header
            .next
            .ok_or_else(|| invalid("sequence space exhausted"))?;
        capture.header.next = SafeUnsigned::new(seq.get() + 1).ok();
        let value = match write {
            Write::Entry(entry) => {
                let entry = materialize_entry(entry, seq, timestamp);
                let structure = entry_structure(&entry);
                if !capture.ids.insert(structure.id.to_string()) {
                    return Err(corrupt("duplicate entry or usage identity"));
                }
                if structure.parent_id.as_ref().is_some_and(|parent| {
                    capture
                        .entries
                        .get(parent.as_str())
                        .and_then(Option::as_ref)
                        .is_none_or(|p| p.seq >= seq)
                }) {
                    return Err(corrupt("entry parent missing or not earlier"));
                }
                if structure.entry_type == EntryType::Message {
                    capture.header.stats.message_count =
                        add(capture.header.stats.message_count, n(1))?;
                }
                capture
                    .entries
                    .insert(structure.id.to_string(), Some(structure));
                Committed::Entry(entry)
            }
            Write::Usage(row) => {
                if !capture.ids.insert(row.id.to_string()) {
                    return Err(corrupt("duplicate entry or usage identity"));
                }
                if row.entry_id.as_ref().is_some_and(|entry| {
                    capture
                        .entries
                        .get(entry.as_str())
                        .and_then(Option::as_ref)
                        .is_none()
                }) {
                    return Err(corrupt("usage entry missing"));
                }
                add_usage(&mut capture.header.stats.usage, &row.usage)?;
                Committed::Usage(UsageRow {
                    id: row.id,
                    seq,
                    usage: row.usage,
                    entry_id: row.entry_id,
                    adjustment: row.adjustment,
                    details: row.details,
                })
            }
            Write::SetRegister(value) => {
                let row = materialize_register(*value, seq);
                if let Register::LaneLeaf {
                    value: Some(entry), ..
                } = &row
                    && capture
                        .entries
                        .get(entry.as_str())
                        .and_then(Option::as_ref)
                        .is_none_or(|entry| entry.seq >= seq)
                {
                    return Err(corrupt("leaf entry missing or not earlier"));
                }
                if let Register::PendingEntry { key, .. } = &row
                    && let Some(keys) = &mut capture.pending
                {
                    keys.insert(key.clone());
                }
                Committed::Set(row)
            }
            Write::DeleteRegister(key) => {
                if key.namespace == RegisterNamespace::PendingEntry
                    && let Some(keys) = &mut capture.pending
                {
                    keys.remove(&key.key);
                }
                Committed::Delete(RegisterKey {
                    namespace: key.namespace,
                    key: key.key,
                })
            }
        };
        if let Some(full) = &mut capture.full {
            match &value {
                Committed::Entry(row) => full.entries.push(row.clone()),
                Committed::Usage(row) => full.usage.push(row.clone()),
                Committed::Set(row) => {
                    let key = register_key(row);
                    full.registers.retain(|old| register_key(old) != key);
                    full.registers.push(row.clone());
                }
                Committed::Delete(key) => full.registers.retain(|old| register_key(old) != *key),
            }
        }
        writes.push(value);
        seqs.push(seq);
    }
    if let Some(full) = &mut capture.full {
        full.entries.sort_by_key(|e| entry_structure(e).seq);
        full.usage.sort_by_key(|r| r.seq);
        full.registers.sort_by_key(register_key);
        full.next_seq = capture.header.next;
        full.stats = capture.header.stats.clone();
    }
    Ok(Prepared {
        header: capture.header,
        result: CommitResult {
            first_seq,
            seqs,
            timestamp,
        },
        writes,
        projection: IndexedProjection {
            entries: capture.entries,
            pending_keys: capture.pending,
        },
        full: capture.full,
    })
}
pub(crate) fn publish(
    db: &Connection,
    value: &Prepared,
    revision: SafeUnsigned,
) -> Result<(), StorageError> {
    for write in &value.writes {
        match write {
            Committed::Entry(row) => {
                data::insert_entry(db, row)?;
                branch::append(db, &entry_structure(row))?;
            }
            Committed::Usage(row) => data::insert_usage(db, row)?,
            Committed::Set(row) => data::set_register(db, row)?,
            Committed::Delete(key) => {
                db.execute(
                    "DELETE FROM registers WHERE namespace=?1 AND key=?2",
                    params![scalar(&key.namespace)?, key.key],
                )
                .map_err(sql)?;
            }
        }
    }
    db.execute("UPDATE session SET next_seq=?1,message_count=?2,usage_payload=?3,revision=?4 WHERE singleton=1",params![value.header.next.map(|value| value.get() as i64),value.header.stats.message_count.get() as i64,encode(&value.header.stats.usage)?,revision.get() as i64]).map_err(sql)?;
    Ok(())
}
