//! Read-only committed Durable context, independent of task execution and providers.
use pi_ai::{AssistantContent, Message, StopReason};
use pi_durable_session::{
    ContextEdit, ConversationId, EntryId, EntryRecord, Error, Result, Session,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

const MISSING_RESULT_TEXT: &str =
    "Tool result unavailable: history ends before this call completed.";

/// Raw active entries, edited per-entry contributions, and normalized provider messages.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ContextView {
    pub head: Option<EntryRecord>,
    pub entries: Vec<EntryRecord>,
    pub contributions: Vec<Vec<Message>>,
    pub messages: Vec<Message>,
}

/// Captures immutable whole history on the Session line, then derives context off it.
/// An explicit cutoff must be visible from the selected conversation.
pub async fn read_context(
    session: &Session,
    conversation: ConversationId,
    at: Option<EntryId>,
) -> Result<ContextView> {
    let history = session.history(conversation, at).await?;
    if let Some(at) = at
        && !history.iter().any(|entry| entry.id == at)
    {
        return Err(Error::request(format!(
            "Entry {at} is not visible from conversation {conversation}"
        )));
    }
    derive(history)
}

/// Orders selected messages by assistant tool calls, repairing missing results and
/// dropping orphan results. System placement and contribution filtering are unchanged.
/// Compaction passes the flattened contribution prefix of its captured ContextView.
pub fn order_tool_results(messages: &[Message]) -> Result<Vec<Message>> {
    let contributed = messages
        .iter()
        .cloned()
        .map(|message| Contributed {
            message,
            original: None,
        })
        .collect::<Vec<_>>();
    Ok(order_results(&contributed)?
        .into_iter()
        .map(|item| item.message)
        .collect())
}

// Provenance expresses the tagged source's object-identity check. Equal replacement
// bytes never acquire the original entry/model position's identity.
#[derive(Clone)]
struct Contributed {
    message: Message,
    original: Option<(EntryId, usize)>,
}

fn derive(mut history: Vec<EntryRecord>) -> Result<ContextView> {
    history.sort_by_key(|entry| entry.id);
    let head = history
        .iter()
        .rev()
        .find(|entry| entry.head.is_some())
        .cloned();
    let range = history
        .iter()
        .filter(|entry| {
            head.as_ref()
                .is_none_or(|h| entry.id >= h.head.expect("head"))
        })
        .collect::<Vec<_>>();
    let mut edits = BTreeMap::new();
    // Older head markers can carry the winning edit even though active entries omit them.
    for entry in &range {
        for edit in entry.edits.iter().flatten() {
            let target = match edit {
                ContextEdit::Omit { target } | ContextEdit::Replace { target, .. } => *target,
            };
            edits.insert(target, edit);
        }
    }
    let mut active = Vec::new();
    if let Some(head) = &head {
        active.push(head.clone());
    }
    active.extend(
        range
            .into_iter()
            .filter(|entry| head.is_none() || entry.head.is_none())
            .cloned(),
    );
    let baseline = find_baseline(head.as_ref(), &active);
    let mut contributions = Vec::with_capacity(active.len());
    let mut messages = Vec::new();
    for entry in &active {
        let (raw, original) = match edits.get(&entry.id).copied() {
            Some(ContextEdit::Omit { .. }) => (&[][..], false),
            Some(ContextEdit::Replace { messages, .. }) => (messages.as_slice(), false),
            None => (entry.model.as_deref().unwrap_or_default(), true),
        };
        let mut contribution = Vec::new();
        for (index, value) in raw.iter().enumerate() {
            let message: Message = serde_json::from_value(value.clone()).map_err(|error| {
                Error::validation(format!(
                    "Invalid model message in entry {}: {error}",
                    entry.id
                ))
            })?;
            if matches!(&message, Message::Assistant(a) if matches!(a.stop_reason,
                StopReason::Aborted | StopReason::Error | StopReason::Deferred))
            {
                continue;
            }
            contribution.push(message.clone());
            messages.push(Contributed {
                message,
                original: original.then_some((entry.id, index)),
            });
        }
        contributions.push(contribution);
    }
    let mut ordered = order_results(&messages)?;
    if let Some(index) = ordered
        .iter()
        .position(|item| matches!(item.message, Message::System(_)))
        && index > 0
        && (ordered[index]
            .original
            .is_some_and(|id| Some(id) == baseline)
            || ordered[..index]
                .iter()
                .all(|item| matches!(item.message, Message::User(_))))
    {
        let system = ordered.remove(index);
        ordered.insert(0, system);
    }
    Ok(ContextView {
        head,
        entries: active,
        contributions,
        messages: ordered.into_iter().map(|item| item.message).collect(),
    })
}

fn find_baseline(head: Option<&EntryRecord>, entries: &[EntryRecord]) -> Option<(EntryId, usize)> {
    for entry in entries {
        if head.is_some_and(|head| entry.id <= head.id) {
            continue;
        }
        if entry.kind == "pi.system" {
            return entry.model.as_ref().and_then(|messages| {
                messages
                    .iter()
                    .position(is_system)
                    .map(|index| (entry.id, index))
            });
        }
        if head.is_none()
            && (entry.model.iter().flatten().any(is_system)
                || entry.edits.iter().flatten().any(|edit| match edit {
                    ContextEdit::Replace { messages, .. } => messages.iter().any(is_system),
                    ContextEdit::Omit { .. } => false,
                }))
        {
            return None;
        }
    }
    None
}
fn is_system(value: &serde_json::Value) -> bool {
    value.get("role").and_then(serde_json::Value::as_str) == Some("system")
}

fn order_results(messages: &[Contributed]) -> Result<Vec<Contributed>> {
    let mut ordered = Vec::new();
    for (index, item) in messages.iter().enumerate() {
        if matches!(item.message, Message::ToolResult(_)) {
            continue;
        }
        ordered.push(item.clone());
        let Message::Assistant(assistant) = &item.message else {
            continue;
        };
        let mut results = BTreeMap::new();
        for candidate in &messages[index + 1..] {
            match &candidate.message {
                Message::Assistant(_) => break,
                Message::ToolResult(result) => {
                    results
                        .entry(result.tool_call_id.clone())
                        .or_insert(candidate);
                }
                _ => (),
            }
        }
        for content in &assistant.content {
            let AssistantContent::ToolCall(call) = content else {
                continue;
            };
            if let Some(result) = results.get(&call.id) {
                ordered.push((*result).clone());
            } else {
                // Construct through the accepted AI DTO decoder, retaining its timestamp domain.
                let message = serde_json::from_value(serde_json::json!({
                    "role": "toolResult", "toolCallId": call.id, "toolName": call.name,
                    "content": [{"type":"text", "text":MISSING_RESULT_TEXT}],
                    "isError":true, "details":{"reason":"missing_result"},
                    "timestamp":assistant.timestamp
                }))?;
                ordered.push(Contributed {
                    message,
                    original: None,
                });
            }
        }
    }
    Ok(ordered)
}
