use super::*;
use crate::storage::values::structure;
use std::collections::BTreeSet;

pub fn lane_writes(
    key: &str,
    leaf: Option<EntryId>,
    config: Option<LaneConfiguration>,
) -> Vec<Write> {
    let mut writes = Vec::new();
    if let Some(value) = config {
        writes.push(Write::SetRegister(Box::new(RegisterSet::LaneConfig {
            key: key.to_owned(),
            value,
        })));
    }
    writes.push(Write::SetRegister(Box::new(RegisterSet::LaneLeaf {
        key: key.to_owned(),
        value: leaf,
    })));
    writes.push(Write::SetRegister(Box::new(RegisterSet::LaneState {
        key: key.to_owned(),
        value: LaneState {
            current_operation_id: None,
            pending_next_run: vec![],
        },
    })));
    writes
}
pub fn prepare(
    source: StorageSnapshot,
    options: ForkOptions,
    version: SafeUnsigned,
) -> Result<(StorageSnapshot, Vec<Write>), SessionError> {
    let mut lanes = BTreeMap::new();
    let mut configs = BTreeMap::new();
    for register in &source.registers {
        match register {
            Register::LaneLeaf { key, value, .. } => {
                lanes.insert(key.clone(), value.clone());
            }
            Register::LaneConfig { key, value, .. } => {
                configs.insert(key.clone(), value.clone());
            }
            _ => {}
        }
    }
    let main = lanes.get("main").ok_or_else(damaged)?.clone();
    let by_id: BTreeMap<_, _> = source
        .entries
        .iter()
        .map(|entry| (structure(entry).id, entry))
        .collect();
    let (selected, lanes) = match options {
        ForkOptions::Tree(_) => (by_id.keys().cloned().collect::<BTreeSet<_>>(), lanes),
        ForkOptions::Branch(options) => {
            let mut leaf = options.entry_id.or(main);
            if let Some(id) = &leaf {
                let entry = by_id.get(id).ok_or_else(missing)?;
                if options.position == Some(ForkPosition::Before) {
                    leaf = structure(entry).parent_id;
                }
            }
            let mut selected = BTreeSet::new();
            let mut current = leaf.clone();
            while let Some(id) = current {
                if !selected.insert(id.clone()) {
                    return Err(damaged());
                }
                current = structure(by_id.get(&id).ok_or_else(damaged)?).parent_id;
            }
            (selected, BTreeMap::from([("main".to_owned(), leaf)]))
        }
    };
    let entries: Vec<_> = source
        .entries
        .into_iter()
        .filter(|entry| selected.contains(&structure(entry).id))
        .collect();
    let next_seq = match entries.iter().map(|entry| structure(entry).seq).max() {
        Some(max) => SafeUnsigned::new(max.get() + 1).ok(),
        None => Some(SafeUnsigned::new(0).expect("zero")),
    };
    let message_count = SafeUnsigned::new(
        u64::try_from(
            entries
                .iter()
                .filter(|entry| matches!(entry, Entry::Message(_)))
                .count(),
        )
        .map_err(|_| invalid())?,
    )
    .map_err(|_| invalid())?;
    let snapshot = StorageSnapshot {
        entries,
        registers: vec![],
        usage: vec![],
        next_seq,
        storage_version: version,
        store_generation: SafeUnsigned::new(0).expect("zero"),
        stats: SessionStats {
            message_count,
            usage: Usage::zero(),
        },
    };
    let mut writes = Vec::new();
    for (key, leaf) in lanes {
        writes.extend(lane_writes(&key, leaf, configs.remove(&key)));
    }
    for register in source.registers {
        let value = match register {
            Register::FactName { key, value, .. } => Some(RegisterSet::FactName { key, value }),
            Register::FactCustom { key, value, .. } => Some(RegisterSet::FactCustom { key, value }),
            Register::FactLabel { key, value, .. }
                if selected.iter().any(|id| id.as_str() == key) =>
            {
                Some(RegisterSet::FactLabel { key, value })
            }
            _ => None,
        };
        if let Some(value) = value {
            writes.push(Write::SetRegister(Box::new(value)));
        }
    }
    Ok((snapshot, writes))
}
