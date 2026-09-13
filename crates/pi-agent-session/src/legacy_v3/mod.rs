//! Pure, source-bound format-3 normalization. Physical file adoption and first
//! replacement belong to the file repository; this module never accesses a path.
mod error;
mod ids;
mod input;
mod iso;
mod schemas;
mod usage;
use crate::{codec::SessionCodec, memory::MemoryBacking, storage::StorageSnapshot};
pub use error::LegacyError;
pub use ids::{LegacyIdReminter, RemintDomain, Sha256Reminter, deterministic_id};
use input::{Kind, Node, optional_string, string};
pub use iso::parse_iso_timestamp;
use pi_agent_schema::session_values::*;
use pi_agent_schema::*;
pub use schemas::coding_message_options;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

/// Immutable evidence that one exact byte source produced one normalized state.
/// Only normalization can construct it. Getters never permit mutation of either
/// side of the source/state binding; downstream adoption must compare source bytes.
pub struct PreparedLegacy {
    source: Vec<u8>,
    metadata: SessionMetadata,
    snapshot: StorageSnapshot,
    ids: BTreeMap<String, EntryId>,
    resolved: BTreeMap<String, Option<EntryId>>,
}
impl std::fmt::Debug for PreparedLegacy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedLegacy").finish_non_exhaustive()
    }
}
impl PreparedLegacy {
    /// Exact original bytes, including whitespace and original header identity.
    pub fn source_bytes(&self) -> &[u8] {
        &self.source
    }
    /// Validated current logical metadata, retaining unresolved legacy parent path.
    pub fn metadata(&self) -> &SessionMetadata {
        &self.metadata
    }
    /// Complete validated logical snapshot, including the one virtual adjustment.
    pub fn snapshot(&self) -> &StorageSnapshot {
        &self.snapshot
    }
    /// Old retained IDs to deterministic new entry IDs.
    pub fn id_map(&self) -> &BTreeMap<String, EntryId> {
        &self.ids
    }
    /// Every old node resolved through discarded ancestors to a retained ID or root.
    pub fn retained_resolution(&self) -> &BTreeMap<String, Option<EntryId>> {
        &self.resolved
    }
    /// Pure preparation never publishes a physical format-4 file.
    pub const fn pending_physical_conversion(&self) -> bool {
        true
    }
}
/// Normalize with the frozen deterministic remint policy. A supplied parent ID is
/// the result of an external header lookup; no filesystem discovery occurs here.
pub fn normalize(
    source: Vec<u8>,
    codec: &SessionCodec,
    resolved_parent: Option<SessionId>,
) -> Result<PreparedLegacy, LegacyError> {
    normalize_with_reminter(source, codec, resolved_parent, &Sha256Reminter)
}
/// Normalize with an explicit trusted alternate ID policy. Its outputs receive
/// the same UUIDv7 timestamp and complete-domain collision validation as defaults.
pub fn normalize_with_reminter(
    source: Vec<u8>,
    codec: &SessionCodec,
    resolved_parent: Option<SessionId>,
    reminter: &dyn LegacyIdReminter,
) -> Result<PreparedLegacy, LegacyError> {
    let (header, nodes) = input::parse(&source)?;
    let header_id = SessionId::new(header.id).map_err(|_| LegacyError::InvalidData)?;
    if resolved_parent.is_some() && header.parent.is_none() {
        return Err(LegacyError::InvalidData);
    }
    let mut by_id = BTreeMap::new();
    let mut ids = BTreeMap::new();
    let mut resolved = BTreeMap::new();
    let mut unique = BTreeSet::new();
    // Original physical ancestry is validated before any node is discarded.
    for (index, node) in nodes.iter().enumerate() {
        if by_id.contains_key(&node.id)
            || node
                .parent
                .as_ref()
                .is_some_and(|parent| !by_id.contains_key(parent))
        {
            return Err(LegacyError::Ancestry);
        }
        by_id.insert(node.id.clone(), index);
    }
    for node in &nodes {
        let retained = if node.kind.retained() {
            let id = checked_id(
                reminter,
                &header_id,
                RemintDomain::Entry,
                &node.id,
                node.timestamp,
                &mut unique,
            )?;
            let id = EntryId::new(id).map_err(|_| LegacyError::Identity)?;
            ids.insert(node.id.clone(), id.clone());
            Some(id)
        } else {
            node.parent
                .as_ref()
                .and_then(|parent| resolved.get(parent))
                .cloned()
                .flatten()
        };
        resolved.insert(node.id.clone(), retained);
    }
    let remap = |old: &str| -> String {
        resolved
            .get(old)
            .and_then(Option::as_ref)
            .map_or_else(|| old.to_owned(), |id| id.as_str().to_owned())
    };
    let mut entries = Vec::new();
    let mut facts = BTreeMap::<(RegisterNamespace, String), RegisterSet>::new();
    let mut total = Usage::zero();
    for node in &nodes {
        match node.kind {
            Kind::Discard => (),
            Kind::Label => {
                // Validate the source field but deliberately target the node's parent.
                string(&node.value, "targetId")?;
                let label = optional_string(&node.value, "label")?;
                if let Some(target) = node
                    .parent
                    .as_ref()
                    .and_then(|parent| resolved.get(parent))
                    .and_then(Option::as_ref)
                {
                    let key = (RegisterNamespace::FactLabel, target.as_str().to_owned());
                    if let Some(value) = label.filter(|value| !value.is_empty()) {
                        facts.insert(key.clone(), RegisterSet::FactLabel { key: key.1, value });
                    } else {
                        facts.remove(&key);
                    }
                }
            }
            Kind::SessionInfo => {
                let name = optional_string(&node.value, "name")?;
                let key = (RegisterNamespace::FactName, String::new());
                if let Some(value) = name
                    .map(|name| iso::trim_ecmascript(&name).to_owned())
                    .filter(|name| !name.is_empty())
                {
                    facts.insert(
                        key,
                        RegisterSet::FactName {
                            key: String::new(),
                            value,
                        },
                    );
                } else {
                    facts.remove(&key);
                }
            }
            _ => {
                let mut row = node.value.clone();
                let row_object = row.as_object_mut().ok_or(LegacyError::InvalidData)?;
                row_object.remove("timestamp");
                row_object.insert(
                    "id".into(),
                    json!(ids.get(&node.id).ok_or(LegacyError::Identity)?),
                );
                let parent = node
                    .parent
                    .as_ref()
                    .and_then(|parent| resolved.get(parent))
                    .cloned()
                    .flatten();
                row_object.insert("parentId".into(), json!(parent));
                match node.kind {
                    Kind::Message => {
                        let message = message(
                            node.value
                                .get("message")
                                .cloned()
                                .ok_or(LegacyError::InvalidData)?,
                            &remap,
                            codec,
                        )?;
                        bill_message(&message, &mut total)?;
                        row_object.insert("message".into(), json!(message));
                    }
                    Kind::CustomMessage => {
                        let msg = custom_message(node, codec)?;
                        *row_object=serde_json::from_value(json!({"type":"message","id":ids.get(&node.id),"parentId":parent,"message":msg})).map_err(|_|LegacyError::InvalidData)?;
                    }
                    Kind::Compaction => {
                        let first = string(&node.value, "firstKeptEntryId")?;
                        let mut path = Vec::new();
                        let mut cursor = node.parent.as_deref();
                        let mut found = false;
                        while let Some(id) = cursor {
                            let ancestor = &nodes[*by_id.get(id).ok_or(LegacyError::Ancestry)?];
                            path.push(ancestor);
                            if id == first {
                                found = true;
                                break;
                            }
                            cursor = ancestor.parent.as_deref();
                        }
                        if !found {
                            return Err(LegacyError::CompactionBoundary);
                        }
                        path.reverse();
                        let mut tail = Vec::new();
                        for ancestor in path {
                            if let Some(message) = project(ancestor, &remap, codec)? {
                                tail.push(message);
                            }
                        }
                        row_object.remove("firstKeptEntryId");
                        row_object.insert("retainedTail".into(), json!(tail));
                        summary_fields(node, row_object, &mut total)?;
                    }
                    Kind::BranchSummary => {
                        row_object.insert(
                            "fromId".into(),
                            json!(remap(&string(&node.value, "fromId")?)),
                        );
                        summary_fields(node, row_object, &mut total)?;
                    }
                    _ => (),
                }
                let entry: NewEntry =
                    serde_json::from_value(row).map_err(|_| LegacyError::InvalidData)?;
                entries.push(crate::storage::values::materialize(
                    entry,
                    safe(entries.len())?,
                    node.timestamp,
                ));
            }
        }
    }
    let leaf = nodes
        .last()
        .and_then(|node| resolved.get(&node.id))
        .cloned()
        .flatten();
    facts.insert(
        (RegisterNamespace::LaneLeaf, "main".into()),
        RegisterSet::LaneLeaf {
            key: "main".into(),
            value: leaf,
        },
    );
    facts.insert(
        (RegisterNamespace::LaneState, "main".into()),
        RegisterSet::LaneState {
            key: "main".into(),
            value: LaneState {
                current_operation_id: None,
                pending_next_run: vec![],
            },
        },
    );
    let mut registers = Vec::new();
    for (_, value) in facts {
        let seq = safe(
            entries
                .len()
                .checked_add(registers.len())
                .ok_or(LegacyError::Overflow)?,
        )?;
        registers.push(crate::storage::values::set_register(value, seq));
    }
    let seq = safe(
        entries
            .len()
            .checked_add(registers.len())
            .ok_or(LegacyError::Overflow)?,
    )?;
    let usage_id = checked_id(
        reminter,
        &header_id,
        RemintDomain::Aggregate,
        "",
        header.timestamp,
        &mut unique,
    )?;
    let usage = vec![UsageRow {
        id: UsageId::new(usage_id).map_err(|_| LegacyError::Identity)?,
        seq,
        usage: total.clone(),
        entry_id: None,
        adjustment: true,
        details: Presence::Value(
            serde_json::from_value(json!({"source":"v3-import"}))
                .map_err(|_| LegacyError::InvalidData)?,
        ),
    }];
    let message_count = safe(
        entries
            .iter()
            .filter(|entry| matches!(entry, Entry::Message(_)))
            .count(),
    )?;
    let snapshot = StorageSnapshot {
        entries,
        registers,
        usage,
        next_seq: Some(SafeUnsigned::new(seq.get() + 1).map_err(|_| LegacyError::Overflow)?),
        storage_version: SafeUnsigned::new(1).expect("one"),
        store_generation: SafeUnsigned::new(0).expect("zero"),
        stats: SessionStats {
            message_count,
            usage: total,
        },
    };
    MemoryBacking::from_snapshot(snapshot.clone()).map_err(LegacyError::Snapshot)?;
    codec
        .validate_snapshot(&snapshot)
        .map_err(LegacyError::Codec)?;
    let metadata = SessionMetadata {
        id: header_id,
        created_at: header.timestamp,
        storage_version: snapshot.storage_version,
        store_generation: snapshot.store_generation,
        cwd: Some(header.cwd),
        parent_session_id: resolved_parent.clone(),
        legacy_parent_session_path: if resolved_parent.is_none() {
            header.parent
        } else {
            None
        },
    };
    Ok(PreparedLegacy {
        source,
        metadata,
        snapshot,
        ids,
        resolved,
    })
}
fn safe(value: usize) -> Result<SafeUnsigned, LegacyError> {
    SafeUnsigned::new(u64::try_from(value).map_err(|_| LegacyError::Overflow)?)
        .map_err(|_| LegacyError::Overflow)
}
fn checked_id(
    reminter: &dyn LegacyIdReminter,
    header: &SessionId,
    domain: RemintDomain,
    old: &str,
    time: SafeUnsigned,
    unique: &mut BTreeSet<String>,
) -> Result<String, LegacyError> {
    let id = reminter.remint(header, domain, old, time)?;
    if crate::id::uuid_timestamp(&id).map_err(|_| LegacyError::Identity)? != time
        || !unique.insert(id.clone())
    {
        return Err(LegacyError::Identity);
    }
    Ok(id)
}
fn bill_message(message: &AgentMessage, total: &mut Usage) -> Result<(), LegacyError> {
    match message {
        AgentMessage::BuiltIn(Message::Assistant(message)) => {
            usage::accumulate(total, &message.usage)
        }
        AgentMessage::BuiltIn(Message::ToolResult(message)) => match &message.usage {
            Some(value) => usage::accumulate(total, value),
            None => Ok(()),
        },
        _ => Ok(()),
    }
}
fn summary_fields(
    node: &Node,
    row: &mut serde_json::Map<String, Value>,
    total: &mut Usage,
) -> Result<(), LegacyError> {
    if !row.contains_key("fromHook") {
        row.insert("fromHook".into(), Value::Bool(false));
    }
    if let Some(value) = node.value.get("usage") {
        let value: Usage =
            serde_json::from_value(value.clone()).map_err(|_| LegacyError::InvalidData)?;
        usage::accumulate(total, &value)?;
    }
    Ok(())
}
fn message(
    mut value: Value,
    remap: &impl Fn(&str) -> String,
    codec: &SessionCodec,
) -> Result<AgentMessage, LegacyError> {
    let role = string(&value, "role")?;
    if matches!(role.as_str(), "user" | "assistant" | "toolResult")
        && value.get("content").is_none_or(Value::is_null)
    {
        value
            .as_object_mut()
            .ok_or(LegacyError::InvalidData)?
            .insert("content".into(), json!([]));
    }
    if role == "branchSummary" {
        let from = string(&value, "fromId")?;
        value
            .as_object_mut()
            .ok_or(LegacyError::InvalidData)?
            .insert("fromId".into(), json!(remap(&from)));
    }
    let message: AgentMessage =
        serde_json::from_value(value).map_err(|_| LegacyError::InvalidData)?;
    codec
        .validate_message(&message)
        .map_err(LegacyError::Codec)?;
    Ok(message)
}
fn custom_message(node: &Node, codec: &SessionCodec) -> Result<AgentMessage, LegacyError> {
    // Source node shape is mandatory even when a caller's custom-role schema is broad.
    string(&node.value, "customType")?;
    node.value
        .get("display")
        .and_then(Value::as_bool)
        .ok_or(LegacyError::InvalidData)?;
    let content = node.value.get("content").ok_or(LegacyError::InvalidData)?;
    let _: AgentMessage =
        serde_json::from_value(json!({"role":"user","content":content,"timestamp":node.timestamp}))
            .map_err(|_| LegacyError::InvalidData)?;
    let mut value = node
        .value
        .as_object()
        .ok_or(LegacyError::InvalidData)?
        .clone();
    for key in ["type", "id", "parentId"] {
        value.remove(key);
    }
    value.insert("role".into(), json!("custom"));
    value.insert("timestamp".into(), json!(node.timestamp));
    message(Value::Object(value), &str::to_owned, codec)
}
fn project(
    node: &Node,
    remap: &impl Fn(&str) -> String,
    codec: &SessionCodec,
) -> Result<Option<AgentMessage>, LegacyError> {
    let value = match node.kind {
        Kind::Message => {
            return message(
                node.value
                    .get("message")
                    .cloned()
                    .ok_or(LegacyError::InvalidData)?,
                remap,
                codec,
            )
            .map(Some);
        }
        Kind::CustomMessage => return custom_message(node, codec).map(Some),
        Kind::BranchSummary => {
            json!({"role":"branchSummary","summary":string(&node.value,"summary")?,"fromId":remap(&string(&node.value,"fromId")?),"timestamp":node.timestamp})
        }
        Kind::Compaction => {
            json!({"role":"compactionSummary","summary":string(&node.value,"summary")?,"tokensBefore":node.value.get("tokensBefore").ok_or(LegacyError::InvalidData)?,"timestamp":node.timestamp})
        }
        _ => return Ok(None),
    };
    // The branch-summary reference is already mapped in the concrete projection.
    message(value, &str::to_owned, codec).map(Some)
}
