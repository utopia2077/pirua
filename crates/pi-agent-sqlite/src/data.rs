use super::*;
use rusqlite::types::Value;
#[derive(Clone)]
pub(crate) struct Header {
    pub metadata: SessionMetadata,
    pub stats: SessionStats,
    pub next: Option<SafeUnsigned>,
    pub token: StoreToken,
    pub revision: SafeUnsigned,
    pub fence_counter: SafeUnsigned,
}
pub(crate) fn safe(value: i64) -> Result<SafeUnsigned, StorageError> {
    u64::try_from(value)
        .ok()
        .and_then(|v| SafeUnsigned::new(v).ok())
        .ok_or_else(|| corrupt("invalid stored safe integer"))
}
pub(crate) fn decode<T: serde::de::DeserializeOwned>(value: &str) -> Result<T, StorageError> {
    serde_json::from_str(value).map_err(|_| corrupt("invalid stored JSON"))
}
pub(crate) fn header(db: &Connection) -> Result<Header, StorageError> {
    let row=db.query_row("SELECT id,created_at,parent_session_id,storage_version,metadata,message_count,usage_payload,next_seq,store_generation,format_version,store_token,revision,fence_counter FROM session WHERE singleton=1",[],|r|Ok((r.get::<_,String>(0)?,r.get::<_,i64>(1)?,r.get::<_,Option<String>>(2)?,r.get::<_,i64>(3)?,r.get::<_,String>(4)?,r.get::<_,i64>(5)?,r.get::<_,String>(6)?,r.get::<_,Option<i64>>(7)?,r.get::<_,i64>(8)?,r.get::<_,i64>(9)?,r.get::<_,Vec<u8>>(10)?,r.get::<_,i64>(11)?,r.get::<_,i64>(12)?))).map_err(sql)?;
    if row.9 != 1 {
        return Err(StorageError::Unsupported(context(
            "unsupported SQLite physical format",
        )));
    }
    let extras: JsonObject = decode(&row.4)?;
    let mut metadata = serde_json::to_value(extras)
        .map_err(|_| corrupt("metadata JSON"))?
        .as_object()
        .cloned()
        .ok_or_else(|| corrupt("metadata object"))?;
    for key in [
        "id",
        "createdAt",
        "parentSessionId",
        "storageVersion",
        "storeGeneration",
    ] {
        if metadata.contains_key(key) {
            return Err(corrupt("duplicate session metadata authority"));
        }
    }
    metadata.insert("id".into(), row.0.into());
    metadata.insert(
        "createdAt".into(),
        serde_json::to_value(safe(row.1)?).map_err(|_| corrupt("metadata encode"))?,
    );
    if let Some(parent) = row.2 {
        metadata.insert("parentSessionId".into(), parent.into());
    }
    metadata.insert(
        "storageVersion".into(),
        serde_json::to_value(safe(row.3)?).map_err(|_| corrupt("metadata encode"))?,
    );
    metadata.insert(
        "storeGeneration".into(),
        serde_json::to_value(safe(row.8)?).map_err(|_| corrupt("metadata encode"))?,
    );
    let metadata =
        serde_json::from_value(metadata.into()).map_err(|_| corrupt("invalid SQLite metadata"))?;
    Ok(Header {
        metadata,
        stats: SessionStats {
            message_count: safe(row.5)?,
            usage: decode(&row.6)?,
        },
        next: row.7.map(safe).transpose()?,
        token: StoreToken(
            row.10
                .try_into()
                .map_err(|_| corrupt("invalid store identity"))?,
        ),
        revision: safe(row.11)?,
        fence_counter: safe(row.12)?,
    })
}
pub(crate) fn current(db: &Connection) -> Result<(), SessionError> {
    let version = safe(
        db.query_row(
            "SELECT storage_version FROM session WHERE singleton=1",
            [],
            |row| row.get(0),
        )
        .map_err(sql)
        .map_err(as_session)?,
    )
    .map_err(as_session)?;
    if version > n(1) {
        Err(SessionError::NewerStorageVersion(context(
            "stored SQLite version is newer",
        )))
    } else if version < n(1) {
        Err(SessionError::Migration(context(
            "no historical SQLite mapping",
        )))
    } else {
        Ok(())
    }
}
#[derive(Clone)]
pub(crate) struct RawEntry {
    id: String,
    parent: Option<String>,
    seq: i64,
    kind: String,
    custom: Option<String>,
    timestamp: i64,
    payload: Option<String>,
}
impl RawEntry {
    pub fn row(row: &rusqlite::Row<'_>, payload: bool) -> rusqlite::Result<Self> {
        Ok(Self {
            id: row.get(0)?,
            parent: row.get(1)?,
            seq: row.get(2)?,
            kind: row.get(3)?,
            custom: row.get(4)?,
            timestamp: row.get(5)?,
            payload: if payload { Some(row.get(6)?) } else { None },
        })
    }
    fn fields(&self) -> Result<serde_json::Map<String, serde_json::Value>, StorageError> {
        let mut fields = serde_json::Map::new();
        fields.insert("id".into(), self.id.clone().into());
        fields.insert(
            "parentId".into(),
            serde_json::to_value(&self.parent).map_err(|_| corrupt("parent encode"))?,
        );
        fields.insert(
            "seq".into(),
            serde_json::to_value(safe(self.seq)?).map_err(|_| corrupt("seq encode"))?,
        );
        fields.insert("type".into(), self.kind.clone().into());
        fields.insert(
            "timestamp".into(),
            serde_json::to_value(safe(self.timestamp)?).map_err(|_| corrupt("timestamp encode"))?,
        );
        if let Some(custom) = &self.custom {
            fields.insert("customType".into(), custom.clone().into());
        }
        Ok(fields)
    }
    pub fn structure(&self) -> Result<EntryStructure, StorageError> {
        let structure: EntryStructure = serde_json::from_value(self.fields()?.into())
            .map_err(|_| corrupt("invalid entry structure"))?;
        if (structure.entry_type == EntryType::Custom) != structure.custom_type.is_some() {
            return Err(corrupt("invalid custom entry structure"));
        }
        Ok(structure)
    }
    pub fn entry(&self, codec: &SessionCodec) -> Result<Entry, StorageError> {
        let payload = self
            .payload
            .as_ref()
            .ok_or_else(|| corrupt("missing entry payload"))?;
        let object: JsonObject = decode(payload)?;
        let mut value = serde_json::to_value(object)
            .map_err(|_| corrupt("payload encode"))?
            .as_object()
            .cloned()
            .ok_or_else(|| corrupt("entry payload is not object"))?;
        for (key, field) in self.fields()? {
            if value.insert(key, field).is_some() {
                return Err(corrupt("entry payload duplicates structure"));
            }
        }
        codec
            .decode_entry(&serde_json::to_vec(&value).map_err(|_| corrupt("entry encode"))?)
            .map_err(codec_error)
    }
}
pub(crate) const ENTRY_COLUMNS: &str = "e.id,e.parent_id,e.seq,e.type,e.custom_type,e.timestamp";
pub(crate) fn structures(
    db: &Connection,
    ids: &BTreeSet<String>,
) -> Result<BTreeMap<String, Option<EntryStructure>>, StorageError> {
    let mut statement = db
        .prepare(&format!(
            "SELECT {ENTRY_COLUMNS} FROM entries e WHERE e.id=?1"
        ))
        .map_err(sql)?;
    let mut values = BTreeMap::new();
    for id in ids {
        let row = statement
            .query_row([id], |r| RawEntry::row(r, false))
            .optional()
            .map_err(sql)?;
        values.insert(id.clone(), row.map(|v| v.structure()).transpose()?);
    }
    Ok(values)
}
pub(crate) fn insert_entry(db: &Connection, entry: &Entry) -> Result<(), StorageError> {
    let structure = entry_structure(entry);
    let mut payload = serde_json::to_value(entry)
        .map_err(|_| invalid("entry encoding failed"))?
        .as_object()
        .cloned()
        .ok_or_else(|| invalid("entry payload"))?;
    for key in ["id", "parentId", "seq", "timestamp", "type", "customType"] {
        payload.remove(key);
    }
    db.execute("INSERT INTO entries(id,parent_id,seq,type,custom_type,timestamp,payload) VALUES(?1,?2,?3,?4,?5,?6,?7)",params![structure.id.as_str(),structure.parent_id.as_ref().map(EntryId::as_str),structure.seq.get() as i64,scalar(&structure.entry_type)?,structure.custom_type.as_ref().map(CustomType::as_str),structure.timestamp.get() as i64,encode(&payload)?]).map_err(sql)?;
    Ok(())
}
pub(crate) fn register(
    db: &Connection,
    key: &RegisterKey,
    codec: &SessionCodec,
) -> Result<Option<Register>, StorageError> {
    let row = db
        .query_row(
            "SELECT seq,value FROM registers WHERE namespace=?1 AND key=?2",
            params![scalar(&key.namespace)?, key.key],
            |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)),
        )
        .optional()
        .map_err(sql)?;
    row.map(|(seq, value)| decode_register(key, safe(seq)?, &value, codec))
        .transpose()
}
fn decode_register(
    key: &RegisterKey,
    seq: SafeUnsigned,
    value: &str,
    codec: &SessionCodec,
) -> Result<Register, StorageError> {
    let value: JsonValue = decode(value)?;
    let bytes = serde_json::to_vec(
        &serde_json::json!({"namespace":key.namespace,"key":key.key,"seq":seq,"value":value}),
    )
    .map_err(|_| corrupt("register encoding"))?;
    codec.decode_register(&bytes).map_err(codec_error)
}
pub(crate) fn set_register(db: &Connection, value: &Register) -> Result<(), StorageError> {
    let key = register_key(value);
    let raw = serde_json::to_value(value).map_err(|_| invalid("register encode"))?;
    let payload = raw
        .get("value")
        .ok_or_else(|| invalid("register lacks value"))?;
    db.execute("INSERT INTO registers(namespace,key,seq,value) VALUES(?1,?2,?3,?4) ON CONFLICT(namespace,key) DO UPDATE SET seq=excluded.seq,value=excluded.value",params![scalar(&key.namespace)?,key.key,register_seq(value).get() as i64,encode(payload)?]).map_err(sql)?;
    Ok(())
}
pub(crate) fn insert_usage(db: &Connection, row: &UsageRow) -> Result<(), StorageError> {
    let details = match &row.details {
        Presence::Missing => None,
        Presence::Null => Some("null".into()),
        Presence::Value(v) => Some(encode(v)?),
    };
    db.execute("INSERT INTO usage_ledger(id,seq,entry_id,adjustment,usage,details) VALUES(?1,?2,?3,?4,?5,?6)",params![row.id.as_str(),row.seq.get() as i64,row.entry_id.as_ref().map(EntryId::as_str),row.adjustment,encode(&row.usage)?,details]).map_err(sql)?;
    Ok(())
}
type RawUsage = (String, i64, Option<String>, i64, String, Option<String>);
pub(crate) fn usage_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawUsage> {
    Ok((
        row.get(0)?,
        row.get(1)?,
        row.get(2)?,
        row.get(3)?,
        row.get(4)?,
        row.get(5)?,
    ))
}
pub(crate) fn decode_usage(row: RawUsage) -> Result<UsageRow, StorageError> {
    if ![0, 1].contains(&row.3) {
        return Err(corrupt("invalid usage adjustment"));
    }
    Ok(UsageRow {
        id: UsageId::new(row.0).map_err(|_| corrupt("usage ID"))?,
        seq: safe(row.1)?,
        entry_id: row
            .2
            .map(EntryId::new)
            .transpose()
            .map_err(|_| corrupt("usage entry ID"))?,
        adjustment: row.3 == 1,
        usage: decode(&row.4)?,
        details: match row.5 {
            None => Presence::Missing,
            Some(s) if s == "null" => Presence::Null,
            Some(s) => Presence::Value(decode(&s)?),
        },
    })
}
pub(crate) fn all_registers(
    db: &Connection,
    codec: &SessionCodec,
) -> Result<Vec<Register>, StorageError> {
    let mut statement = db
        .prepare("SELECT namespace,key,seq,value FROM registers ORDER BY namespace,key")
        .map_err(sql)?;
    let raw = statement
        .query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, i64>(2)?,
                r.get::<_, String>(3)?,
            ))
        })
        .map_err(sql)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(sql)?;
    let mut values = raw
        .into_iter()
        .map(|(ns, key, seq, value)| {
            let namespace = serde_json::from_value(ns.into())
                .map_err(|_| corrupt("unknown register namespace"))?;
            decode_register(&RegisterKey { namespace, key }, safe(seq)?, &value, codec)
        })
        .collect::<Result<Vec<_>, StorageError>>()?;
    values.sort_by_key(register_key);
    Ok(values)
}
pub(crate) fn snapshot(
    db: &Connection,
    header: &Header,
    codec: &SessionCodec,
) -> Result<StorageSnapshot, StorageError> {
    let mut statement = db
        .prepare(&format!(
            "SELECT {ENTRY_COLUMNS},e.payload FROM entries e INDEXED BY ix_entry_seq ORDER BY e.seq"
        ))
        .map_err(sql)?;
    let entries = statement
        .query_map([], |r| RawEntry::row(r, true))
        .map_err(sql)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(sql)?
        .into_iter()
        .map(|r| r.entry(codec))
        .collect::<Result<Vec<_>, _>>()?;
    let registers = all_registers(db, codec)?;
    let mut statement=db.prepare("SELECT id,seq,entry_id,adjustment,usage,details FROM usage_ledger INDEXED BY ix_usage_seq ORDER BY seq").map_err(sql)?;
    let usage = statement
        .query_map([], usage_row)
        .map_err(sql)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(sql)?
        .into_iter()
        .map(decode_usage)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(StorageSnapshot {
        entries,
        registers,
        usage,
        next_seq: header.next,
        storage_version: header.metadata.storage_version,
        store_generation: header.metadata.store_generation,
        stats: header.stats.clone(),
    })
}
pub(crate) fn validate_mechanical(snapshot: &StorageSnapshot) -> Result<(), StorageError> {
    drop(pi_agent_session::memory::MemoryBacking::from_snapshot(
        snapshot.clone(),
    )?);
    Ok(())
}
pub(crate) fn validate(
    snapshot: &StorageSnapshot,
    codec: &SessionCodec,
) -> Result<(), SessionError> {
    for entry in &snapshot.entries {
        codec.validate_entry(entry)?;
    }
    for register in &snapshot.registers {
        codec.validate_register(register)?;
    }
    drop(
        pi_agent_session::memory::MemoryBacking::from_snapshot(snapshot.clone())
            .map_err(as_session)?,
    );
    Ok(())
}
pub(crate) fn init(
    db: &Connection,
    metadata: &SessionMetadata,
    snapshot: &StorageSnapshot,
    store: StoreToken,
) -> Result<(), StorageError> {
    db.execute_batch(include_str!("schema.sql")).map_err(sql)?;
    let mut extras = serde_json::to_value(metadata)
        .map_err(|_| invalid("metadata encode"))?
        .as_object()
        .cloned()
        .ok_or_else(|| invalid("metadata object"))?;
    for key in [
        "id",
        "createdAt",
        "parentSessionId",
        "storageVersion",
        "storeGeneration",
    ] {
        extras.remove(key);
    }
    db.execute("INSERT INTO session(singleton,id,created_at,parent_session_id,storage_version,metadata,message_count,usage_payload,next_seq,store_generation,format_version,store_token,revision,fence_counter) VALUES(1,?1,?2,?3,?4,?5,?6,?7,?8,?9,1,?10,0,0)",params![metadata.id.as_str(),metadata.created_at.get() as i64,metadata.parent_session_id.as_ref().map(SessionId::as_str),metadata.storage_version.get() as i64,encode(&extras)?,snapshot.stats.message_count.get() as i64,encode(&snapshot.stats.usage)?,snapshot.next_seq.map(|value| value.get() as i64),metadata.store_generation.get() as i64,store.0.as_slice()]).map_err(sql)?;
    for entry in &snapshot.entries {
        insert_entry(db, entry)?;
        branch::append(db, &entry_structure(entry))?;
    }
    for row in &snapshot.usage {
        insert_usage(db, row)?;
    }
    for row in &snapshot.registers {
        set_register(db, row)?;
    }
    Ok(())
}
pub(crate) fn prefix_upper(prefix: &str) -> Option<String> {
    let mut chars: Vec<_> = prefix.chars().collect();
    while let Some(last) = chars.pop() {
        let mut next = u32::from(last) + 1;
        if (0xd800..=0xdfff).contains(&next) {
            next = 0xe000;
        }
        if let Some(next) = char::from_u32(next) {
            chars.push(next);
            return Some(chars.into_iter().collect());
        }
    }
    None
}
pub(crate) fn range(
    from: Option<SafeUnsigned>,
    to: Option<SafeUnsigned>,
    column: &str,
) -> (String, Vec<Value>) {
    let mut sql = String::new();
    let mut args = vec![];
    if let Some(from) = from {
        sql.push_str(&format!(" AND {column}>=?"));
        args.push(Value::Integer(from.get() as i64));
    }
    if let Some(to) = to {
        sql.push_str(&format!(" AND {column}<=?"));
        args.push(Value::Integer(to.get() as i64));
    }
    (sql, args)
}
