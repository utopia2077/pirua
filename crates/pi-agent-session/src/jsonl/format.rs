//! Physical format and complete-frame replay, independent of filesystem I/O.
use super::*;
use crate::storage::values::{materialize, register_key, register_seq, set_register, structure};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Header {
    pub metadata: SessionMetadata,
    pub prefix: Option<(SafeUnsigned, Option<SafeUnsigned>)>,
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct HeaderWire {
    v: SafeUnsigned,
    kind: String,
    id: SessionId,
    created_at: SafeUnsigned,
    storage_version: SafeUnsigned,
    #[serde(default = "zero")]
    store_generation: SafeUnsigned,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cwd: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    parent_session_id: Option<SessionId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    legacy_parent_session_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    snapshot_items: Option<SafeUnsigned>,
    #[serde(default, skip_serializing_if = "Presence::is_missing")]
    next_seq: Presence<SafeUnsigned>,
}
fn zero() -> SafeUnsigned {
    SafeUnsigned::new(0).expect("zero")
}
fn json(bytes: &[u8]) -> Result<Value, serde_json::Error> {
    // Existing owned JSON admission rejects duplicate keys recursively.
    let value: JsonValue = serde_json::from_slice(bytes)?;
    serde_json::to_value(value)
}
fn object(value: Value) -> Result<Map<String, Value>, StorageError> {
    value
        .as_object()
        .cloned()
        .ok_or_else(|| corrupt("physical row must be an object"))
}
fn decode<T: serde::de::DeserializeOwned>(value: Value) -> Result<T, StorageError> {
    serde_json::from_value(value).map_err(|_| corrupt("invalid physical row shape"))
}
fn encoded<T: Serialize>(value: &T) -> Result<Value, StorageError> {
    serde_json::to_value(value).map_err(|_| invalid("value cannot be encoded"))
}
fn optional_non_null(map: &Map<String, Value>, keys: &[&str]) -> Result<(), StorageError> {
    if keys.iter().any(|key| map.get(*key) == Some(&Value::Null)) {
        Err(corrupt("optional physical field is null"))
    } else {
        Ok(())
    }
}
impl Header {
    pub fn decode(bytes: &[u8]) -> Result<Self, StorageError> {
        let map = object(json(bytes).map_err(|_| corrupt("invalid JSONL header"))?)?;
        if map.get("v").and_then(Value::as_u64) != Some(4) {
            return Err(unsupported("unsupported JSONL format"));
        }
        optional_non_null(
            &map,
            &[
                "cwd",
                "parentSessionId",
                "legacyParentSessionPath",
                "snapshotItems",
            ],
        )?;
        let wire: HeaderWire = decode(Value::Object(map))?;
        if wire.kind != "header" {
            return Err(corrupt("missing JSONL header tag"));
        }
        let prefix = match (wire.snapshot_items, wire.next_seq) {
            (None, Presence::Missing) => None,
            (Some(count), Presence::Null) => Some((count, None)),
            (Some(count), Presence::Value(next)) => Some((count, Some(next))),
            _ => return Err(corrupt("snapshot prefix and watermark must occur together")),
        };
        Ok(Self {
            metadata: SessionMetadata {
                id: wire.id,
                created_at: wire.created_at,
                storage_version: wire.storage_version,
                store_generation: wire.store_generation,
                cwd: wire.cwd,
                parent_session_id: wire.parent_session_id,
                legacy_parent_session_path: wire.legacy_parent_session_path,
            },
            prefix,
        })
    }
    pub fn encode(&self) -> Result<Vec<u8>, StorageError> {
        let (snapshot_items, next_seq) = match self.prefix {
            None => (None, Presence::Missing),
            Some((count, None)) => (Some(count), Presence::Null),
            Some((count, Some(next))) => (Some(count), Presence::Value(next)),
        };
        let m = &self.metadata;
        let wire = HeaderWire {
            v: SafeUnsigned::new(4).expect("four"),
            kind: "header".into(),
            id: m.id.clone(),
            created_at: m.created_at,
            storage_version: m.storage_version,
            store_generation: m.store_generation,
            cwd: m.cwd.clone(),
            parent_session_id: m.parent_session_id.clone(),
            legacy_parent_session_path: m.legacy_parent_session_path.clone(),
            snapshot_items,
            next_seq,
        };
        let mut bytes =
            serde_json::to_vec(&wire).map_err(|_| invalid("header cannot be encoded"))?;
        bytes.push(b'\n');
        Ok(bytes)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct RawRegisterRow {
    pub key: RegisterKey,
    pub seq: SafeUnsigned,
    pub value: JsonValue,
}
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct RawSnapshot {
    pub metadata: SessionMetadata,
    pub entries: Vec<Entry>,
    pub registers: Vec<RawRegisterRow>,
    pub usage: Vec<UsageRow>,
    pub next_seq: Option<SafeUnsigned>,
    pub stats: SessionStats,
}
impl RawSnapshot {
    pub fn from_current(
        metadata: SessionMetadata,
        snapshot: StorageSnapshot,
    ) -> Result<Self, StorageError> {
        if metadata.storage_version != snapshot.storage_version
            || metadata.store_generation != snapshot.store_generation
        {
            return Err(invalid("snapshot metadata does not match authority"));
        }
        let registers = snapshot
            .registers
            .iter()
            .map(|row| {
                let value = encoded(row)?;
                Ok(RawRegisterRow {
                    key: register_key(row),
                    seq: register_seq(row),
                    value: decode(
                        value
                            .get("value")
                            .cloned()
                            .ok_or_else(|| invalid("register lacks value"))?,
                    )?,
                })
            })
            .collect::<Result<Vec<_>, StorageError>>()?;
        Ok(Self {
            metadata,
            entries: snapshot.entries,
            registers,
            usage: snapshot.usage,
            next_seq: snapshot.next_seq,
            stats: snapshot.stats,
        })
    }
    pub fn current(&self, codec: &SessionCodec) -> Result<StorageSnapshot, SessionError> {
        let registers = self
            .registers
            .iter()
            .map(|row| {
                let mut map = Map::new();
                map.insert(
                    "namespace".into(),
                    encoded(&row.key.namespace).map_err(session_error)?,
                );
                map.insert("key".into(), Value::String(row.key.key.clone()));
                map.insert("seq".into(), encoded(&row.seq).map_err(session_error)?);
                map.insert("value".into(), encoded(&row.value).map_err(session_error)?);
                codec.decode_register(
                    &serde_json::to_vec(&map)
                        .map_err(|_| session_error(invalid("register cannot be encoded")))?,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let snapshot = StorageSnapshot {
            entries: self.entries.clone(),
            registers,
            usage: self.usage.clone(),
            next_seq: self.next_seq,
            storage_version: self.metadata.storage_version,
            store_generation: self.metadata.store_generation,
            stats: self.stats.clone(),
        };
        codec.validate_snapshot(&snapshot)?;
        MemoryBacking::from_snapshot(snapshot.clone()).map_err(session_error)?;
        Ok(snapshot)
    }
}
#[derive(Clone)]
enum Row {
    Entry(Entry),
    Usage(UsageRow),
    Set(RawRegisterRow),
    Delete(RegisterKey, SafeUnsigned),
}
impl Row {
    fn seq(&self) -> SafeUnsigned {
        match self {
            Self::Entry(e) => structure(e).seq,
            Self::Usage(u) => u.seq,
            Self::Set(r) => r.seq,
            Self::Delete(_, seq) => *seq,
        }
    }
    fn value(&self) -> Result<Value, StorageError> {
        let mut map = match self {
            Self::Entry(e) => object(encoded(e)?)?,
            Self::Usage(u) => object(encoded(u)?)?,
            Self::Set(r) => {
                let mut map = Map::new();
                map.insert("namespace".into(), encoded(&r.key.namespace)?);
                map.insert("key".into(), Value::String(r.key.key.clone()));
                map.insert("seq".into(), encoded(&r.seq)?);
                map.insert("value".into(), encoded(&r.value)?);
                map.insert("op".into(), Value::String("set".into()));
                map
            }
            Self::Delete(key, seq) => {
                let mut map = Map::new();
                map.insert("namespace".into(), encoded(&key.namespace)?);
                map.insert("key".into(), Value::String(key.key.clone()));
                map.insert("seq".into(), encoded(seq)?);
                map.insert("op".into(), Value::String("delete".into()));
                map
            }
        };
        map.insert(
            "kind".into(),
            Value::String(
                match self {
                    Self::Entry(_) => "entry",
                    Self::Usage(_) => "usage",
                    _ => "register",
                }
                .into(),
            ),
        );
        Ok(Value::Object(map))
    }
}
fn row(value: Value) -> Result<Row, StorageError> {
    let mut map = object(value)?;
    let kind = map
        .remove("kind")
        .ok_or_else(|| corrupt("physical row lacks kind"))?;
    match kind.as_str() {
        Some("entry") => decode(Value::Object(map)).map(Row::Entry),
        Some("usage") => decode(Value::Object(map)).map(Row::Usage),
        Some("register") => {
            let op = map
                .remove("op")
                .ok_or_else(|| corrupt("register row lacks operation"))?;
            let namespace = decode(
                map.remove("namespace")
                    .ok_or_else(|| corrupt("register row lacks namespace"))?,
            )?;
            let key = decode(
                map.remove("key")
                    .ok_or_else(|| corrupt("register row lacks key"))?,
            )?;
            let seq = decode(
                map.remove("seq")
                    .ok_or_else(|| corrupt("register row lacks sequence"))?,
            )?;
            let result = match op.as_str() {
                Some("set") => Row::Set(RawRegisterRow {
                    key: RegisterKey { namespace, key },
                    seq,
                    value: decode(
                        map.remove("value")
                            .ok_or_else(|| corrupt("register set lacks value"))?,
                    )?,
                }),
                Some("delete") => Row::Delete(RegisterKey { namespace, key }, seq),
                _ => return Err(corrupt("invalid register operation")),
            };
            if !map.is_empty() {
                return Err(corrupt("unknown register row field"));
            }
            Ok(result)
        }
        _ => Err(corrupt("unknown physical row kind")),
    }
}
fn rows(value: Value) -> Result<Vec<Row>, StorageError> {
    match value {
        Value::Object(_) => Ok(vec![row(value)?]),
        Value::Array(values) => values.into_iter().map(row).collect(),
        _ => Err(corrupt("transaction frame is not an object or array")),
    }
}

pub(crate) struct Replay {
    pub header: Header,
    pub snapshot: RawSnapshot,
    pub good_len: usize,
    pub needs_separator: bool,
}
pub(crate) fn header(bytes: &[u8]) -> Result<Header, StorageError> {
    let end = bytes
        .iter()
        .position(|b| *b == b'\n')
        .unwrap_or(bytes.len());
    Header::decode(&bytes[..end])
}
fn whitespace(bytes: &[u8]) -> bool {
    bytes
        .iter()
        .all(|b| matches!(b, b' ' | b'\t' | b'\r' | b'\n'))
}
fn parse_frame(bytes: &[u8]) -> Result<Option<Value>, StorageError> {
    if let Err(error) = std::str::from_utf8(bytes) {
        if error.error_len().is_none()
            && json(&bytes[..error.valid_up_to()]).is_err_and(|error| error.is_eof())
        {
            return Ok(None);
        }
        return Err(corrupt("invalid UTF-8 frame"));
    }
    match json(bytes) {
        Ok(value) => Ok(Some(value)),
        Err(error) if error.is_eof() => Ok(None),
        Err(_) => Err(corrupt("invalid JSON frame")),
    }
}
pub(crate) fn replay(bytes: &[u8]) -> Result<Replay, StorageError> {
    let header = header(bytes)?;
    let mut position = bytes
        .iter()
        .position(|b| *b == b'\n')
        .map_or(bytes.len(), |i| i + 1);
    let mut good_len = position;
    let mut prefix_left = header.prefix.map_or(0, |(count, _)| count.get());
    let mut next = header.prefix.map_or(Some(zero()), |(_, next)| next);
    let mut last = None;
    let mut entries = BTreeMap::new();
    let mut registers = BTreeMap::new();
    let mut usage = Vec::new();
    let mut ids = BTreeSet::new();
    let mut prefix_registers = BTreeSet::new();
    while position < bytes.len() {
        if whitespace(&bytes[position..]) {
            break;
        }
        let remaining = &bytes[position..];
        let lf = remaining.iter().position(|b| *b == b'\n');
        let end = lf.map_or(bytes.len(), |i| position + i);
        let line = &bytes[position..end];
        if whitespace(line) {
            return Err(corrupt("interior blank frame"));
        }
        let Some(value) = parse_frame(line)? else {
            if lf.is_some() || prefix_left > 0 {
                return Err(corrupt("incomplete completed frame or snapshot prefix"));
            }
            break;
        };
        let rows = rows(value)?;
        let in_prefix = prefix_left > 0;
        if in_prefix
            && (rows.is_empty()
                || u64::try_from(rows.len()).map_err(|_| corrupt("prefix count overflow"))?
                    > prefix_left)
        {
            return Err(corrupt("snapshot prefix ends inside a frame"));
        }
        for row in rows {
            let seq = row.seq();
            if last.is_some_and(|last| seq <= last) {
                return Err(corrupt("persisted sequence is not increasing"));
            }
            if in_prefix {
                if next.is_some_and(|next| seq >= next) {
                    return Err(corrupt("snapshot watermark does not dominate rows"));
                }
                prefix_left -= 1;
            } else {
                if next.is_none_or(|next| seq < next) {
                    return Err(corrupt("tail sequence violates allocator watermark"));
                }
                next = SafeUnsigned::new(seq.get() + 1).ok();
            }
            last = Some(seq);
            match row {
                Row::Entry(entry) => {
                    let meta = structure(&entry);
                    if !ids.insert(meta.id.as_str().to_owned()) {
                        return Err(corrupt("duplicate entry or usage ID"));
                    }
                    if meta
                        .parent_id
                        .as_ref()
                        .is_some_and(|id| !entries.contains_key(id))
                    {
                        return Err(corrupt("entry parent is missing or later"));
                    }
                    entries.insert(meta.id, entry);
                }
                Row::Usage(row) => {
                    if !ids.insert(row.id.as_str().to_owned()) {
                        return Err(corrupt("duplicate entry or usage ID"));
                    }
                    if row
                        .entry_id
                        .as_ref()
                        .is_some_and(|id| !entries.contains_key(id))
                    {
                        return Err(corrupt("usage target is missing or later"));
                    }
                    usage.push(row);
                }
                Row::Set(row) => {
                    if in_prefix && !prefix_registers.insert(row.key.clone()) {
                        return Err(corrupt("repeated snapshot register"));
                    }
                    if row.key.namespace == RegisterNamespace::LaneLeaf
                        && let JsonValue::String(id) = &row.value
                        && !entries.keys().any(|key| key.as_str() == id)
                    {
                        return Err(corrupt("leaf target is missing or later"));
                    }
                    registers.insert(row.key.clone(), row);
                }
                Row::Delete(key, _) => {
                    if in_prefix {
                        return Err(corrupt("snapshot prefix contains deletion"));
                    }
                    registers.remove(&key);
                }
            }
        }
        position = if lf.is_some() { end + 1 } else { end };
        good_len = position;
    }
    if prefix_left != 0 {
        return Err(corrupt("missing snapshot prefix rows"));
    }
    let mut entries: Vec<_> = entries.into_values().collect();
    entries.sort_by_key(|entry| structure(entry).seq);
    let stats = statistics(&entries, &usage)?;
    let snapshot = RawSnapshot {
        metadata: header.metadata.clone(),
        entries,
        registers: registers.into_values().collect(),
        usage,
        next_seq: next,
        stats,
    };
    Ok(Replay {
        header,
        snapshot,
        good_len,
        needs_separator: good_len > 0 && bytes[good_len - 1] != b'\n',
    })
}

pub(crate) fn compact(snapshot: &RawSnapshot) -> Result<Vec<u8>, StorageError> {
    let mut rows: Vec<_> = snapshot
        .entries
        .iter()
        .cloned()
        .map(Row::Entry)
        .chain(snapshot.registers.iter().cloned().map(Row::Set))
        .chain(snapshot.usage.iter().cloned().map(Row::Usage))
        .collect();
    rows.sort_by_key(Row::seq);
    let count = SafeUnsigned::new(
        u64::try_from(rows.len()).map_err(|_| invalid("snapshot count overflow"))?,
    )
    .map_err(|_| invalid("snapshot count exceeds safe range"))?;
    let mut bytes = Header {
        metadata: snapshot.metadata.clone(),
        prefix: Some((count, snapshot.next_seq)),
    }
    .encode()?;
    for row in rows {
        serde_json::to_writer(&mut bytes, &row.value()?)
            .map_err(|_| invalid("snapshot cannot be encoded"))?;
        bytes.push(b'\n');
    }
    // Reapply the exact physical checks, including raw prefix envelope/closure.
    replay(&bytes)?;
    Ok(bytes)
}
pub(crate) fn commit_frame(
    tx: Transaction,
    result: &CommitResult,
) -> Result<Vec<u8>, StorageError> {
    if tx.writes.len() != result.seqs.len() {
        return Err(invalid("commit sequence count mismatch"));
    }
    let rows = tx
        .writes
        .into_iter()
        .zip(&result.seqs)
        .map(|(write, seq)| match write {
            Write::Entry(entry) => Ok(Row::Entry(materialize(entry, *seq, result.timestamp))),
            Write::Usage(row) => Ok(Row::Usage(UsageRow {
                id: row.id,
                seq: *seq,
                usage: row.usage,
                entry_id: row.entry_id,
                adjustment: row.adjustment,
                details: row.details,
            })),
            Write::SetRegister(value) => {
                let register = set_register(*value, *seq);
                let object = encoded(&register)?;
                Ok(Row::Set(RawRegisterRow {
                    key: register_key(&register),
                    seq: *seq,
                    value: decode(
                        object
                            .get("value")
                            .cloned()
                            .ok_or_else(|| invalid("missing register value"))?,
                    )?,
                }))
            }
            Write::DeleteRegister(key) => Ok(Row::Delete(
                RegisterKey {
                    namespace: key.namespace,
                    key: key.key,
                },
                *seq,
            )),
        })
        .collect::<Result<Vec<_>, StorageError>>()?;
    let mut values = rows.iter().map(Row::value).collect::<Result<Vec<_>, _>>()?;
    let frame = if values.len() == 1 {
        values.remove(0)
    } else {
        Value::Array(values)
    };
    let mut bytes =
        serde_json::to_vec(&frame).map_err(|_| invalid("transaction frame cannot be encoded"))?;
    bytes.push(b'\n');
    Ok(bytes)
}
fn statistics(entries: &[Entry], rows: &[UsageRow]) -> Result<SessionStats, StorageError> {
    fn add(a: SafeUnsigned, b: SafeUnsigned) -> Result<SafeUnsigned, StorageError> {
        SafeUnsigned::new(a.get() + b.get()).map_err(|_| corrupt("usage integer overflow"))
    }
    fn optional(
        a: Option<SafeUnsigned>,
        b: Option<SafeUnsigned>,
    ) -> Result<Option<SafeUnsigned>, StorageError> {
        match (a, b) {
            (None, None) => Ok(None),
            (Some(a), None) | (None, Some(a)) => Ok(Some(a)),
            (Some(a), Some(b)) => add(a, b).map(Some),
        }
    }
    fn cost(a: FiniteF64, b: FiniteF64) -> Result<FiniteF64, StorageError> {
        FiniteF64::new(a.get() + b.get()).map_err(|_| corrupt("usage cost overflow"))
    }
    let mut usage = Usage::zero();
    for row in rows {
        let b = &row.usage;
        usage.input = add(usage.input, b.input)?;
        usage.output = add(usage.output, b.output)?;
        usage.cache_read = add(usage.cache_read, b.cache_read)?;
        usage.cache_write = add(usage.cache_write, b.cache_write)?;
        usage.total_tokens = add(usage.total_tokens, b.total_tokens)?;
        usage.cache_write_1h = optional(usage.cache_write_1h, b.cache_write_1h)?;
        usage.reasoning = optional(usage.reasoning, b.reasoning)?;
        usage.cost.input = cost(usage.cost.input, b.cost.input)?;
        usage.cost.output = cost(usage.cost.output, b.cost.output)?;
        usage.cost.cache_read = cost(usage.cost.cache_read, b.cost.cache_read)?;
        usage.cost.cache_write = cost(usage.cost.cache_write, b.cost.cache_write)?;
        usage.cost.total = cost(usage.cost.total, b.cost.total)?;
    }
    let count = u64::try_from(
        entries
            .iter()
            .filter(|e| matches!(e, Entry::Message(_)))
            .count(),
    )
    .map_err(|_| corrupt("message count overflow"))?;
    Ok(SessionStats {
        message_count: SafeUnsigned::new(count)
            .map_err(|_| corrupt("message count exceeds safe range"))?,
        usage,
    })
}
