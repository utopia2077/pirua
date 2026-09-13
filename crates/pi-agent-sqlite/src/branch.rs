//! Segmented derived cache. Ordinary reads never rebuild or walk canonical parents.
use super::*;
use rusqlite::types::Value;
#[derive(Clone, serde::Serialize)]
struct Cached {
    id: String,
    seq: i64,
    kind: String,
}
#[derive(Clone, serde::Serialize)]
struct Meta {
    id: String,
    tip: String,
    tip_seq: i64,
    base: Option<String>,
    base_seq: Option<i64>,
    count: i64,
    digest: Vec<u8>,
    revision: i64,
}
#[derive(Clone)]
struct Segment {
    meta: Meta,
    lower: i64,
    upper: i64,
}
fn hash<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, StorageError> {
    Ok(
        ring::digest::digest(&ring::digest::SHA256, encode(value)?.as_bytes())
            .as_ref()
            .to_vec(),
    )
}
fn xor(a: &mut [u8], b: &[u8]) -> Result<(), StorageError> {
    if a.len() != 32 || b.len() != 32 {
        return Err(corrupt("invalid branch digest"));
    }
    for (a, b) in a.iter_mut().zip(b) {
        *a ^= *b;
    }
    Ok(())
}
fn row(r: &rusqlite::Row<'_>) -> rusqlite::Result<(Cached, Vec<u8>)> {
    Ok((
        Cached {
            id: r.get(0)?,
            seq: r.get(1)?,
            kind: r.get(2)?,
        },
        r.get(3)?,
    ))
}
fn checked_row(value: (Cached, Vec<u8>)) -> Result<Cached, StorageError> {
    data::safe(value.0.seq)?;
    let _: EntryType = serde_json::from_value(value.0.kind.clone().into())
        .map_err(|_| corrupt("unknown cache entry type"))?;
    if hash(&value.0)? != value.1 {
        return Err(corrupt("branch row integrity mismatch"));
    }
    Ok(value.0)
}
fn meta(db: &Connection, id: &str) -> Result<Meta, StorageError> {
    let (value,actual_revision,digest)=db.query_row("SELECT branch_id,tip_entry_id,tip_seq,base_branch_id,base_seq,row_count,row_digest,checked_revision,cache_revision,meta_digest FROM branch_meta WHERE branch_id=?1",[id],|r|Ok((Meta{id:r.get(0)?,tip:r.get(1)?,tip_seq:r.get(2)?,base:r.get(3)?,base_seq:r.get(4)?,count:r.get(5)?,digest:r.get(6)?,revision:r.get(7)?},r.get::<_,i64>(8)?,r.get::<_,Vec<u8>>(9)?))).optional().map_err(sql)?.ok_or_else(||corrupt("branch metadata missing"))?;
    if value.revision != actual_revision
        || hash(&value)? != digest
        || value.digest.len() != 32
        || value.count <= 0
        || value.base.is_some() != value.base_seq.is_some()
    {
        return Err(corrupt("branch metadata integrity mismatch"));
    }
    data::safe(value.tip_seq)?;
    if value
        .base_seq
        .is_some_and(|base| base < 0 || base >= value.tip_seq)
    {
        return Err(corrupt("invalid branch base range"));
    }
    let tip=db.query_row("SELECT entry_id,entry_seq,entry_type,row_digest FROM branch_entries WHERE branch_id=?1 AND entry_id=?2",params![id,value.tip],row).optional().map_err(sql)?.ok_or_else(||corrupt("branch tip row missing"))?;
    if checked_row(tip)?.seq != value.tip_seq {
        return Err(corrupt("branch tip mismatch"));
    }
    Ok(value)
}
fn save(db: &Connection, value: &Meta) -> Result<(), StorageError> {
    let mut value = value.clone();
    value.revision = db
        .query_row(
            "SELECT cache_revision FROM branch_meta WHERE branch_id=?1",
            [&value.id],
            |r| r.get::<_, i64>(0),
        )
        .optional()
        .map_err(sql)?
        .unwrap_or(0);
    db.execute("INSERT INTO branch_meta(branch_id,tip_entry_id,tip_seq,base_branch_id,base_seq,row_count,row_digest,checked_revision,cache_revision,meta_digest) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?8,?9) ON CONFLICT(branch_id) DO UPDATE SET tip_entry_id=excluded.tip_entry_id,tip_seq=excluded.tip_seq,row_count=excluded.row_count,row_digest=excluded.row_digest,checked_revision=excluded.checked_revision,meta_digest=excluded.meta_digest",params![value.id,value.tip,value.tip_seq,value.base,value.base_seq,value.count,value.digest,value.revision,hash(&value)?]).map_err(sql)?;
    Ok(())
}
fn insert(db: &Connection, branch: &str, value: &Cached) -> Result<(), StorageError> {
    db.execute("INSERT INTO branch_entries(branch_id,entry_id,entry_seq,entry_type,row_digest) VALUES(?1,?2,?3,?4,?5)",params![branch,value.id,value.seq,value.kind,hash(value)?]).map_err(sql)?;
    Ok(())
}
fn covering(db: &Connection, entry: &EntryStructure) -> Result<Meta, StorageError> {
    if let Some(id) = db
        .query_row(
            "SELECT branch_id FROM branch_meta WHERE tip_entry_id=?1",
            [entry.id.as_str()],
            |r| r.get::<_, String>(0),
        )
        .optional()
        .map_err(sql)?
    {
        let value = meta(db, &id)?;
        if value.tip_seq != entry.seq.get() as i64 {
            return Err(corrupt("tip does not cover entry"));
        }
        return Ok(value);
    }
    let mut statement = db
        .prepare("SELECT branch_id FROM branch_entries INDEXED BY ix_be_entry WHERE entry_id=?1")
        .map_err(sql)?;
    let ids = statement
        .query_map([entry.id.as_str()], |r| r.get::<_, String>(0))
        .map_err(sql)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(sql)?;
    for id in ids {
        let value = meta(db, &id)?;
        let seq = entry.seq.get() as i64;
        if seq > value.base_seq.unwrap_or(-1) && seq <= value.tip_seq {
            let cached=db.query_row("SELECT entry_id,entry_seq,entry_type,row_digest FROM branch_entries WHERE branch_id=?1 AND entry_id=?2",params![id,entry.id.as_str()],row).map_err(sql)?;
            let cached = checked_row(cached)?;
            if cached.seq != seq || cached.kind != scalar(&entry.entry_type)? {
                return Err(corrupt("entry coverage row mismatch"));
            }
            return Ok(value);
        }
    }
    Err(corrupt("no branch logically covers entry"))
}
fn chain(db: &Connection, start: Meta, upper: i64) -> Result<Vec<Segment>, StorageError> {
    let mut segments = vec![];
    let mut seen = BTreeSet::new();
    let mut current = Some(start);
    let mut upper = upper;
    while let Some(value) = current {
        if !seen.insert(value.id.clone()) || upper > value.tip_seq {
            return Err(corrupt("invalid branch chain"));
        }
        let lower = value.base_seq.unwrap_or(-1);
        segments.push(Segment {
            meta: value.clone(),
            lower,
            upper,
        });
        current = match &value.base {
            Some(base) => {
                upper = upper.min(lower);
                Some(meta(db, base)?)
            }
            None => None,
        };
    }
    Ok(segments)
}
fn cached_range(
    db: &Connection,
    segment: &Segment,
    lower: i64,
) -> Result<Vec<Cached>, StorageError> {
    let mut statement=db.prepare("SELECT entry_id,entry_seq,entry_type,row_digest FROM branch_entries INDEXED BY ix_be_seq WHERE branch_id=?1 AND entry_seq>?2 AND entry_seq<=?3 ORDER BY entry_seq").map_err(sql)?;
    statement
        .query_map(
            params![segment.meta.id, segment.lower.max(lower), segment.upper],
            row,
        )
        .map_err(sql)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(sql)?
        .into_iter()
        .map(checked_row)
        .collect()
}
fn first_stop(
    db: &Connection,
    segments: &[Segment],
    q: &BranchScan,
    newest: bool,
) -> Result<Option<i64>, StorageError> {
    let indices: Vec<_> = if newest {
        (0..segments.len()).collect()
    } else {
        (0..segments.len()).rev().collect()
    };
    for i in indices {
        let segment = &segments[i];
        if segment.upper <= segment.lower {
            continue;
        }
        let mut found = vec![];
        if let Some(kind) = q.stop_at_type {
            let order = if newest { "DESC" } else { "ASC" };
            let query = format!(
                "SELECT entry_id,entry_seq,entry_type,row_digest FROM branch_entries INDEXED BY ix_be_type WHERE branch_id=?1 AND entry_type=?2 AND entry_seq>?3 AND entry_seq<=?4 ORDER BY entry_seq {order} LIMIT 1"
            );
            if let Some(value) = db
                .query_row(
                    &query,
                    params![
                        segment.meta.id,
                        scalar(&kind)?,
                        segment.lower,
                        segment.upper
                    ],
                    row,
                )
                .optional()
                .map_err(sql)?
            {
                found.push(checked_row(value)?);
            }
        }
        if let Some(id) = &q.stop_at_id
            && let Some(value)=db.query_row("SELECT entry_id,entry_seq,entry_type,row_digest FROM branch_entries WHERE branch_id=?1 AND entry_id=?2 AND entry_seq>?3 AND entry_seq<=?4",params![segment.meta.id,id.as_str(),segment.lower,segment.upper],row).optional().map_err(sql)?{found.push(checked_row(value)?);}
        if !found.is_empty() {
            let value = if newest {
                found.into_iter().max_by_key(|r| r.seq)
            } else {
                found.into_iter().min_by_key(|r| r.seq)
            }
            .expect("nonempty");
            let points = data::structures(db, &[value.id.clone()].into())?;
            let canonical = points
                .get(&value.id)
                .and_then(Option::as_ref)
                .ok_or_else(|| corrupt("stop row lacks entry"))?;
            if canonical.seq.get() as i64 != value.seq
                || scalar(&canonical.entry_type)? != value.kind
            {
                return Err(corrupt("stop cache differs from entry"));
            }
            return Ok(Some(value.seq));
        }
    }
    Ok(None)
}
pub(crate) fn append(db: &Connection, entry: &EntryStructure) -> Result<(), StorageError> {
    let value = Cached {
        id: entry.id.to_string(),
        seq: entry.seq.get() as i64,
        kind: scalar(&entry.entry_type)?,
    };
    let mut next = if let Some(parent) = &entry.parent_id {
        let parent = data::structures(db, &[parent.to_string()].into())?
            .remove(parent.as_str())
            .flatten()
            .ok_or_else(|| corrupt("append parent missing"))?;
        let covering = covering(db, &parent)?;
        if covering.tip == parent.id.as_str() {
            if covering.revision >= SafeUnsigned::MAX.get() as i64 {
                return Err(invalid("branch mutation counter exhausted"));
            }
            let mut next = covering;
            next.tip = value.id.clone();
            next.tip_seq = value.seq;
            next.count = next
                .count
                .checked_add(1)
                .ok_or_else(|| invalid("branch count exhausted"))?;
            xor(&mut next.digest, &hash(&value)?)?;
            insert(db, &next.id, &value)?;
            save(db, &next)?;
            return Ok(());
        }
        let segments = chain(db, covering.clone(), parent.seq.get() as i64)?;
        let stop = first_stop(
            db,
            &segments,
            &BranchScan {
                stop_at_type: Some(EntryType::Compaction),
                ..Default::default()
            },
            true,
        )?;
        let base_seq = stop.unwrap_or(parent.seq.get() as i64);
        let id = format!("segment/{}", entry.id.as_str());
        let mut next = Meta {
            id,
            tip: value.id.clone(),
            tip_seq: value.seq,
            base: Some(covering.id),
            base_seq: Some(base_seq),
            count: 0,
            digest: vec![0; 32],
            revision: 0,
        };
        if stop.is_some() {
            for segment in segments.iter().rev() {
                for copied in cached_range(db, segment, base_seq)? {
                    insert(db, &next.id, &copied)?;
                    next.count += 1;
                    xor(&mut next.digest, &hash(&copied)?)?;
                }
            }
        }
        next
    } else {
        Meta {
            id: format!("segment/{}", entry.id.as_str()),
            tip: value.id.clone(),
            tip_seq: value.seq,
            base: None,
            base_seq: None,
            count: 0,
            digest: vec![0; 32],
            revision: 0,
        }
    };
    insert(db, &next.id, &value)?;
    next.count += 1;
    xor(&mut next.digest, &hash(&value)?)?;
    save(db, &next)
}
/// Actual generated scan SQL; the same object is executed and inspected by tests.
pub(crate) struct Query {
    pub sql: String,
    pub args: Vec<Value>,
}
fn query(
    segment: &Segment,
    q: &BranchScan,
    stop: Option<i64>,
    newest: bool,
    payload: bool,
    limit: Option<u64>,
) -> Result<Query, StorageError> {
    let mut lower = segment.lower;
    let mut upper = segment.upper;
    if let Some(stop) = stop {
        if newest {
            lower = lower.max(stop - 1);
        } else {
            upper = upper.min(stop);
        }
    }
    if let Some(cursor) = q.cursor {
        if newest {
            upper = upper.min(cursor.seq.get() as i64 - 1);
        } else {
            lower = lower.max(cursor.seq.get() as i64);
        }
    }
    let index = if q.entry_type.is_some() {
        "ix_be_type"
    } else {
        "ix_be_seq"
    };
    let mut sql = format!(
        "SELECT {}{},b.entry_seq,b.entry_type FROM branch_entries b INDEXED BY {index} CROSS JOIN entries e ON e.id=b.entry_id WHERE b.branch_id=? AND b.entry_seq>? AND b.entry_seq<=?",
        data::ENTRY_COLUMNS,
        if payload { ",e.payload" } else { "" }
    );
    let mut args = vec![
        Value::Text(segment.meta.id.clone()),
        Value::Integer(lower),
        Value::Integer(upper),
    ];
    if let Some(kind) = q.entry_type {
        sql.push_str(" AND b.entry_type=?");
        args.push(Value::Text(scalar(&kind)?));
    }
    if let Some(kind) = &q.custom_type {
        sql.push_str(" AND e.custom_type=?");
        args.push(Value::Text(kind.to_string()));
    }
    sql.push_str(if newest {
        " ORDER BY b.entry_seq DESC"
    } else {
        " ORDER BY b.entry_seq ASC"
    });
    if let Some(limit) = limit {
        sql.push_str(" LIMIT ?");
        args.push(Value::Integer(limit as i64));
    }
    Ok(Query { sql, args })
}
pub(crate) enum Selected {
    Entry(Entry),
    Structure(EntryStructure),
}
impl Selected {
    fn structure(&self) -> EntryStructure {
        match self {
            Self::Entry(v) => entry_structure(v),
            Self::Structure(v) => v.clone(),
        }
    }
}
pub(crate) fn scan(
    db: &Connection,
    codec: &SessionCodec,
    q: &BranchScan,
    payload: bool,
) -> Result<Vec<Selected>, StorageError> {
    let start = q
        .start
        .as_ref()
        .ok_or_else(|| invalid("branch start is required"))?;
    let structure = data::structures(db, &[start.to_string()].into())?
        .remove(start.as_str())
        .flatten()
        .ok_or_else(|| corrupt("branch start missing"))?;
    let segments = chain(db, covering(db, &structure)?, structure.seq.get() as i64)?;
    let newest = q.order != Some(BranchOrder::OldestFirst);
    let stop = first_stop(db, &segments, q, newest)?;
    let mut remaining = q.limit.map(SafeUnsigned::get);
    let mut selected = vec![];
    let indices: Vec<_> = if newest {
        (0..segments.len()).collect()
    } else {
        (0..segments.len()).rev().collect()
    };
    for i in indices {
        if remaining == Some(0) {
            break;
        }
        let query = query(&segments[i], q, stop, newest, payload, remaining)?;
        #[cfg(test)]
        crate::tests::query(&query.sql, &query.args, db);
        let mut statement = db.prepare(&query.sql).map_err(sql)?;
        let values = statement
            .query_map(rusqlite::params_from_iter(query.args), |row| {
                let start = if payload { 7 } else { 6 };
                Ok((
                    data::RawEntry::row(row, payload)?,
                    row.get::<_, i64>(start)?,
                    row.get::<_, String>(start + 1)?,
                ))
            })
            .map_err(sql)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(sql)?;
        for (raw, seq, kind) in values {
            let structure = raw.structure()?;
            if structure.seq.get() as i64 != seq || scalar(&structure.entry_type)? != kind {
                return Err(corrupt("branch cache differs from canonical structure"));
            }
            selected.push(if payload {
                Selected::Entry(raw.entry(codec)?)
            } else {
                Selected::Structure(structure)
            });
            if let Some(value) = remaining.as_mut() {
                *value -= 1;
            }
        }
    }
    if q.entry_type.is_none() && q.custom_type.is_none() {
        let structures: Vec<_> = selected.iter().map(Selected::structure).collect();
        for pair in structures.windows(2) {
            let (child, parent) = if newest {
                (&pair[0], &pair[1])
            } else {
                (&pair[1], &pair[0])
            };
            if child.parent_id.as_ref() != Some(&parent.id) {
                return Err(corrupt("branch cache path has gap or wrong base"));
            }
        }
        if newest
            && q.cursor.is_none()
            && q.limit != Some(n(0))
            && structures.first().is_none_or(|r| r.id != *start)
        {
            return Err(corrupt("branch cache omits requested start"));
        }
        if !newest
            && q.cursor.is_none()
            && structures.first().is_some_and(|r| r.parent_id.is_some())
        {
            return Err(corrupt("branch cache omits root"));
        }
        let finished = q.limit.is_none() || remaining.is_some_and(|v| v > 0);
        if newest
            && stop.is_none()
            && finished
            && structures.last().is_some_and(|r| r.parent_id.is_some())
        {
            return Err(corrupt("branch cache omits prefix"));
        }
    }
    Ok(selected)
}
/// Cold integrity audit only. Never called by ordinary bounded scans/append.
pub(crate) fn audit(db: &Connection) -> Result<(), StorageError> {
    let mut statement = db
        .prepare("SELECT branch_id FROM branch_meta ORDER BY branch_id")
        .map_err(sql)?;
    let ids = statement
        .query_map([], |r| r.get::<_, String>(0))
        .map_err(sql)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(sql)?;
    for id in ids {
        let value = meta(db, &id)?;
        let mut statement=db.prepare("SELECT entry_id,entry_seq,entry_type,row_digest FROM branch_entries INDEXED BY ix_be_seq WHERE branch_id=?1").map_err(sql)?;
        let rows = statement
            .query_map([&id], row)
            .map_err(sql)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(sql)?;
        let mut digest = vec![0; 32];
        let mut seqs = BTreeSet::new();
        let count = rows.len();
        for row in rows {
            let row = checked_row(row)?;
            if row.seq <= value.base_seq.unwrap_or(-1)
                || row.seq > value.tip_seq
                || !seqs.insert(row.seq)
            {
                return Err(corrupt("invalid cached range"));
            }
            xor(&mut digest, &hash(&row)?)?;
        }
        if i64::try_from(count).ok() != Some(value.count) || digest != value.digest {
            return Err(corrupt("branch cache count/digest differs"));
        }
        chain(db, value.clone(), value.tip_seq)?;
    }
    let missing:bool=db.query_row("SELECT EXISTS(SELECT 1 FROM entries e WHERE NOT EXISTS(SELECT 1 FROM branch_entries b INDEXED BY ix_be_entry WHERE b.entry_id=e.id))",[],|r|r.get(0)).map_err(sql)?;
    if missing {
        return Err(corrupt("canonical entry has no branch cache"));
    }
    Ok(())
}
pub(crate) fn repair(db: &Connection) -> Result<(), StorageError> {
    let mut statement = db
        .prepare(&format!(
            "SELECT {} FROM entries e INDEXED BY ix_entry_seq ORDER BY e.seq",
            data::ENTRY_COLUMNS
        ))
        .map_err(sql)?;
    let rows = statement
        .query_map([], |r| data::RawEntry::row(r, false))
        .map_err(sql)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(sql)?
        .into_iter()
        .map(|r| r.structure())
        .collect::<Result<Vec<_>, _>>()?;
    drop(statement);
    let mut ids = BTreeSet::new();
    for row in &rows {
        if row.parent_id.as_ref().is_some_and(|id| !ids.contains(id)) {
            return Err(corrupt("canonical parent missing during repair"));
        }
        ids.insert(row.id.clone());
    }
    db.execute("DELETE FROM branch_meta", []).map_err(sql)?;
    db.execute("DELETE FROM branch_entries", []).map_err(sql)?;
    for row in rows {
        append(db, &row)?;
    }
    audit(db)
}
