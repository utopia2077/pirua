use super::RegisterKey;
use pi_agent_schema::session_values::SafeUnsigned;
use pi_agent_schema::*;

pub(crate) fn structure(entry: &Entry) -> EntryStructure {
    macro_rules! common {
        ($v:expr, $kind:expr, $custom:expr) => {
            EntryStructure {
                id: $v.id.clone(),
                parent_id: $v.parent_id.clone(),
                seq: $v.seq,
                timestamp: $v.timestamp,
                entry_type: $kind,
                custom_type: $custom,
            }
        };
    }
    match entry {
        Entry::Message(v) => common!(v, EntryType::Message, None),
        Entry::Compaction(v) => common!(v, EntryType::Compaction, None),
        Entry::BranchSummary(v) => common!(v, EntryType::BranchSummary, None),
        Entry::Custom(v) => common!(v, EntryType::Custom, Some(v.custom_type.clone())),
    }
}

pub(crate) fn materialize(entry: NewEntry, seq: SafeUnsigned, timestamp: SafeUnsigned) -> Entry {
    match entry {
        NewEntry::Message(v) => Entry::Message(MessageEntry {
            id: v.id,
            parent_id: v.parent_id,
            seq,
            timestamp,
            entry_type: v.entry_type,
            message: v.message,
            terminate: v.terminate,
        }),
        NewEntry::Compaction(v) => Entry::Compaction(CompactionEntry {
            id: v.id,
            parent_id: v.parent_id,
            seq,
            timestamp,
            entry_type: v.entry_type,
            summary: v.summary,
            retained_tail: v.retained_tail,
            tokens_before: v.tokens_before,
            details: v.details,
            usage: v.usage,
            from_hook: v.from_hook,
        }),
        NewEntry::BranchSummary(v) => Entry::BranchSummary(BranchSummaryEntry {
            id: v.id,
            parent_id: v.parent_id,
            seq,
            timestamp,
            entry_type: v.entry_type,
            from_id: v.from_id,
            summary: v.summary,
            details: v.details,
            usage: v.usage,
            from_hook: v.from_hook,
        }),
        NewEntry::Custom(v) => Entry::Custom(CustomEntry {
            id: v.id,
            parent_id: v.parent_id,
            seq,
            timestamp,
            entry_type: v.entry_type,
            custom_type: v.custom_type,
            data: v.data,
        }),
    }
}

pub(crate) fn register_key(register: &Register) -> RegisterKey {
    match register {
        Register::LaneLeaf { key, .. } => RegisterKey {
            namespace: RegisterNamespace::LaneLeaf,
            key: key.clone(),
        },
        Register::LaneConfig { key, .. } => RegisterKey {
            namespace: RegisterNamespace::LaneConfig,
            key: key.clone(),
        },
        Register::LaneState { key, .. } => RegisterKey {
            namespace: RegisterNamespace::LaneState,
            key: key.clone(),
        },
        Register::LaneLastResult { key, .. } => RegisterKey {
            namespace: RegisterNamespace::LaneLastResult,
            key: key.clone(),
        },
        Register::OperationMeta { key, .. } => RegisterKey {
            namespace: RegisterNamespace::OperationMeta,
            key: key.clone(),
        },
        Register::OperationState { key, .. } => RegisterKey {
            namespace: RegisterNamespace::OperationState,
            key: key.clone(),
        },
        Register::OperationToolArguments { key, .. } => RegisterKey {
            namespace: RegisterNamespace::OperationToolArguments,
            key: key.clone(),
        },
        Register::OperationPreparation { key, .. } => RegisterKey {
            namespace: RegisterNamespace::OperationPreparation,
            key: key.clone(),
        },
        Register::PendingEntry { key, .. } => RegisterKey {
            namespace: RegisterNamespace::PendingEntry,
            key: key.clone(),
        },
        Register::FactName { key, .. } => RegisterKey {
            namespace: RegisterNamespace::FactName,
            key: key.clone(),
        },
        Register::FactLabel { key, .. } => RegisterKey {
            namespace: RegisterNamespace::FactLabel,
            key: key.clone(),
        },
        Register::FactCustom { key, .. } => RegisterKey {
            namespace: RegisterNamespace::FactCustom,
            key: key.clone(),
        },
    }
}
pub(crate) fn register_seq(register: &Register) -> SafeUnsigned {
    match register {
        Register::LaneLeaf { seq, .. } => *seq,
        Register::LaneConfig { seq, .. } => *seq,
        Register::LaneState { seq, .. } => *seq,
        Register::LaneLastResult { seq, .. } => *seq,
        Register::OperationMeta { seq, .. } => *seq,
        Register::OperationState { seq, .. } => *seq,
        Register::OperationToolArguments { seq, .. } => *seq,
        Register::OperationPreparation { seq, .. } => *seq,
        Register::PendingEntry { seq, .. } => *seq,
        Register::FactName { seq, .. } => *seq,
        Register::FactLabel { seq, .. } => *seq,
        Register::FactCustom { seq, .. } => *seq,
    }
}
pub(crate) fn set_register(value: RegisterSet, seq: SafeUnsigned) -> Register {
    match value {
        RegisterSet::LaneLeaf { key, value } => Register::LaneLeaf { key, value, seq },
        RegisterSet::LaneConfig { key, value } => Register::LaneConfig { key, value, seq },
        RegisterSet::LaneState { key, value } => Register::LaneState { key, value, seq },
        RegisterSet::LaneLastResult { key, value } => Register::LaneLastResult { key, value, seq },
        RegisterSet::OperationMeta { key, value } => Register::OperationMeta { key, value, seq },
        RegisterSet::OperationState { key, value } => Register::OperationState { key, value, seq },
        RegisterSet::OperationToolArguments { key, value } => {
            Register::OperationToolArguments { key, value, seq }
        }
        RegisterSet::OperationPreparation { key, value } => {
            Register::OperationPreparation { key, value, seq }
        }
        RegisterSet::PendingEntry { key, value } => Register::PendingEntry { key, value, seq },
        RegisterSet::FactName { key, value } => Register::FactName { key, value, seq },
        RegisterSet::FactLabel { key, value } => Register::FactLabel { key, value, seq },
        RegisterSet::FactCustom { key, value } => Register::FactCustom { key, value, seq },
    }
}
