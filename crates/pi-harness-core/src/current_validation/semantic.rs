use super::*;

fn entry_id(entry: &Entry) -> &EntryId {
    match entry {
        Entry::Message(e) => &e.id,
        Entry::Custom(e) => &e.id,
        Entry::Compaction(e) => &e.id,
        Entry::BranchSummary(e) => &e.id,
    }
}

fn role(entry: &Entry, role: &EntryConstraint) -> Result<()> {
    let valid = match role {
        EntryConstraint::Entry => true,
        EntryConstraint::Message => matches!(entry, Entry::Message(_)),
        EntryConstraint::Compaction => matches!(entry, Entry::Compaction(_)),
        EntryConstraint::BranchSummary { from_id } => {
            matches!(entry, Entry::BranchSummary(e) if &e.from_id == from_id)
        }
        EntryConstraint::ToolResult => {
            matches!(entry, Entry::Message(e) if matches!(&e.message, AgentMessage::BuiltIn(Message::ToolResult(_))))
        }
        EntryConstraint::Assistant { running } | EntryConstraint::DeferredAssistant { running } => {
            if let Entry::Message(e) = entry {
                if let AgentMessage::BuiltIn(Message::Assistant(m)) = &e.message {
                    if *running && m.stop_reason == StopReason::Aborted {
                        return Err(CurrentValidationError::Control);
                    }
                    m.stop_reason != StopReason::Pending
                        && (!matches!(role, EntryConstraint::DeferredAssistant { .. })
                            || (m.stop_reason == StopReason::Deferred && m.deferred.is_some()))
                } else {
                    false
                }
            } else {
                false
            }
        }
    };
    if valid {
        Ok(())
    } else {
        Err(CurrentValidationError::EntryRole)
    }
}

pub(super) fn validate(
    references: &CurrentReferences,
    entries: &BTreeMap<EntryId, Entry>,
    registers: &BTreeMap<(RegisterNamespace, String), Register>,
) -> Result<()> {
    for (id, reference) in &references.entries {
        let Some(entry) = entries.get(id) else {
            if reference.required {
                return Err(CurrentValidationError::MissingEntry);
            }
            continue;
        };
        if entry_id(entry) != id {
            return Err(CurrentValidationError::LoadedIdentity);
        }
        for constraint in &reference.constraints {
            role(entry, constraint)?;
        }
    }
    for ((namespace, key), constraint) in &references.registers {
        let register = registers
            .get(&(*namespace, key.clone()))
            .ok_or(CurrentValidationError::MissingRegister)?;
        let (actual_namespace, actual_key) = match register {
            Register::LaneLeaf { key, .. } => (RegisterNamespace::LaneLeaf, key),
            Register::LaneConfig { key, .. } => (RegisterNamespace::LaneConfig, key),
            Register::LaneState { key, .. } => (RegisterNamespace::LaneState, key),
            Register::LaneLastResult { key, .. } => (RegisterNamespace::LaneLastResult, key),
            Register::OperationMeta { key, .. } => (RegisterNamespace::OperationMeta, key),
            Register::OperationState { key, .. } => (RegisterNamespace::OperationState, key),
            Register::OperationToolArguments { key, .. } => {
                (RegisterNamespace::OperationToolArguments, key)
            }
            Register::OperationPreparation { key, .. } => {
                (RegisterNamespace::OperationPreparation, key)
            }
            Register::PendingEntry { key, .. } => (RegisterNamespace::PendingEntry, key),
            Register::FactName { key, .. } => (RegisterNamespace::FactName, key),
            Register::FactLabel { key, .. } => (RegisterNamespace::FactLabel, key),
            Register::FactCustom { key, .. } => (RegisterNamespace::FactCustom, key),
        };
        if actual_namespace != *namespace || actual_key != key {
            return Err(CurrentValidationError::LoadedIdentity);
        }
        let valid = match (constraint, register) {
            (RegisterConstraint::PendingMessage, Register::PendingEntry { value, .. }) => {
                value.entry_type == PendingEntryType::Message
            }
            (RegisterConstraint::PendingWrite, Register::PendingEntry { .. })
            | (RegisterConstraint::ToolArguments, Register::OperationToolArguments { .. }) => true,
            (
                RegisterConstraint::CompactionPreparation,
                Register::OperationPreparation { value, .. },
            ) => matches!(value, DurableStructuralPreparation::Compaction { .. }),
            (
                RegisterConstraint::BranchSummaryPreparation,
                Register::OperationPreparation { value, .. },
            ) => matches!(value, DurableStructuralPreparation::BranchSummary { .. }),
            _ => false,
        };
        if !valid {
            return Err(CurrentValidationError::RegisterRole);
        }
    }
    Ok(())
}

pub(super) fn tools(batch: &ToolBatch, entries: &BTreeMap<EntryId, Entry>) -> Result<()> {
    let Some(Entry::Message(assistant)) = entries.get(&batch.assistant_entry_id) else {
        return Err(CurrentValidationError::EntryRole);
    };
    let AgentMessage::BuiltIn(Message::Assistant(message)) = &assistant.message else {
        return Err(CurrentValidationError::EntryRole);
    };
    let calls = message
        .content
        .iter()
        .enumerate()
        .filter_map(|(index, content)| {
            if let AssistantContent::ToolCall(call) = content {
                Some((index, call))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    if calls.len() != batch.calls.len() {
        return Err(CurrentValidationError::ToolBatch);
    }
    let mut previous_seq = assistant.seq;
    for ((index, source), call) in calls.into_iter().zip(&batch.calls) {
        let (source_index, result_id, terminate) = match call {
            ToolCallState::Planned {
                source_index,
                result_entry_id,
            }
            | ToolCallState::EffectPending {
                source_index,
                result_entry_id,
                ..
            } => (source_index, result_entry_id, None),
            ToolCallState::Completed {
                source_index,
                result_entry_id,
                terminate,
            } => (source_index, result_entry_id, Some(*terminate)),
        };
        if source_index.get() != index as u64 {
            return Err(CurrentValidationError::ToolBatch);
        }
        if let Some(entry) = entries.get(result_id) {
            let Entry::Message(entry) = entry else {
                return Err(CurrentValidationError::EntryRole);
            };
            let AgentMessage::BuiltIn(Message::ToolResult(result)) = &entry.message else {
                return Err(CurrentValidationError::EntryRole);
            };
            if result.tool_call_id != source.id || result.tool_name != source.name {
                return Err(CurrentValidationError::ToolBatch);
            }
            if let Some(terminate) = terminate {
                if terminate != entry.terminate.is_some() || entry.seq <= previous_seq {
                    return Err(CurrentValidationError::ToolBatch);
                }
                previous_seq = entry.seq;
            }
        }
    }
    Ok(())
}
