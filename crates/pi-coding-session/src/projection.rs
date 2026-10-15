use std::collections::BTreeMap;

use serde_json::{Value, json};

use crate::{
    EntryContent, Error, ModelSelection, ProjectedEntry, SessionEntry, SessionProjection,
    SessionSnapshot,
};

/// Walks a validated snapshot's selected branch in root-to-leaf order.
pub(crate) fn branch(snapshot: &SessionSnapshot, leaf: Option<&str>) -> Vec<SessionEntry> {
    let index = snapshot
        .entries
        .iter()
        .map(|entry| (entry.id.as_str(), entry))
        .collect::<BTreeMap<_, _>>();
    let mut path = Vec::new();
    let mut current = leaf.and_then(|id| index.get(id).copied());
    while let Some(entry) = current {
        path.push(entry.clone());
        current = entry
            .parent_id
            .as_deref()
            .and_then(|id| index.get(id).copied());
    }
    path.reverse();
    path
}

/// Selects the latest compaction plus its retained tail and later entries.
pub fn build_context_entries(snapshot: &SessionSnapshot) -> Result<Vec<SessionEntry>, Error> {
    crate::store::validate_snapshot(snapshot)?;
    Ok(context_entries_validated(snapshot))
}

fn context_entries_validated(snapshot: &SessionSnapshot) -> Vec<SessionEntry> {
    let path = branch(snapshot, snapshot.leaf_id.as_deref());
    let Some(compaction_index) = path
        .iter()
        .rposition(|entry| matches!(entry.content, EntryContent::Compaction { .. }))
    else {
        return path;
    };
    let EntryContent::Compaction {
        first_kept_entry_id,
        ..
    } = &path[compaction_index].content
    else {
        unreachable!()
    };
    let mut result = vec![path[compaction_index].clone()];
    if let Some(kept_index) = path[..compaction_index]
        .iter()
        .position(|entry| entry.id == *first_kept_entry_id)
    {
        result.extend(path[kept_index..compaction_index].iter().filter(|entry| {
            !matches!(&entry.content, EntryContent::Message { message } if message["role"] == "system")
        }).cloned());
    }
    result.extend_from_slice(&path[compaction_index + 1..]);
    result
}

/// Converts one record to the current Coding runtime message envelope.
pub fn project_entry(entry: &SessionEntry) -> Vec<Value> {
    match &entry.content {
        EntryContent::Message { message } => vec![message.clone()],
        EntryContent::CustomMessage {
            custom_type,
            content,
            display,
            details,
        } => {
            let mut message = json!({"role":"custom", "customType":custom_type,
                "content":content, "display":display, "timestamp":entry.timestamp});
            if let Some(details) = details {
                message["details"] = details.clone();
            }
            vec![message]
        }
        EntryContent::BranchSummary {
            from_id, summary, ..
        } if !summary.is_empty() => vec![json!({
            "role":"branchSummary", "summary":summary, "fromId":from_id, "timestamp":entry.timestamp
        })],
        EntryContent::Compaction {
            summary,
            tokens_before,
            system_message,
            ..
        } => {
            let mut messages = Vec::new();
            if let Some(system) = system_message {
                messages.push(system.clone());
            }
            messages.push(json!({"role":"compactionSummary", "summary":summary,
                "tokensBefore":tokens_before, "timestamp":entry.timestamp}));
            messages
        }
        _ => Vec::new(),
    }
}

/// Produces provenance-preserving context with branch-local content edits.
pub fn project(snapshot: &SessionSnapshot) -> Result<SessionProjection, Error> {
    crate::store::validate_snapshot(snapshot)?;
    Ok(project_validated(snapshot))
}

pub(crate) fn project_validated(snapshot: &SessionSnapshot) -> SessionProjection {
    let path = branch(snapshot, snapshot.leaf_id.as_deref());
    let mut thinking_level = "off".to_owned();
    let mut model = None;
    for entry in &path {
        match &entry.content {
            EntryContent::ThinkingLevelChange {
                thinking_level: next,
            } => thinking_level = next.clone(),
            EntryContent::ModelChange { provider, model_id } => {
                model = Some(ModelSelection {
                    provider: provider.clone(),
                    model_id: model_id.clone(),
                })
            }
            EntryContent::Message { message } if message["role"] == "assistant" => {
                if let (Some(provider), Some(model_id)) =
                    (message["provider"].as_str(), message["model"].as_str())
                {
                    model = Some(ModelSelection {
                        provider: provider.to_owned(),
                        model_id: model_id.to_owned(),
                    });
                }
            }
            _ => {}
        }
    }
    let context = context_entries_validated(snapshot);
    let mut edits = BTreeMap::new();
    for entry in &context {
        if let EntryContent::ContextEdit {
            target_id,
            replacement,
        } = &entry.content
        {
            edits.insert(target_id.as_str(), replacement.as_ref());
        }
    }
    let entries = context
        .iter()
        .enumerate()
        .map(|(index, source_entry)| {
            let mut messages =
                if index > 0 && matches!(source_entry.content, EntryContent::Compaction { .. }) {
                    Vec::new()
                } else {
                    project_entry(source_entry)
                };
            if let Some(replacement) = edits.get(source_entry.id.as_str()) {
                match replacement {
                    None => messages.clear(),
                    Some(replacement) => {
                        for message in &mut messages {
                            let role = message["role"].as_str().unwrap_or_default();
                            if matches!(role, "user" | "assistant" | "toolResult" | "custom") {
                                message["content"] = if matches!(role, "assistant" | "toolResult")
                                    && replacement.content.is_string()
                                {
                                    json!([{"type":"text", "text":replacement.content}])
                                } else {
                                    replacement.content.clone()
                                };
                            }
                        }
                    }
                }
            }
            ProjectedEntry {
                source_entry: source_entry.clone(),
                messages,
            }
        })
        .collect::<Vec<_>>();
    let messages = entries
        .iter()
        .flat_map(|entry| entry.messages.iter().cloned())
        .collect();
    SessionProjection {
        entries,
        messages,
        thinking_level,
        model,
    }
}
