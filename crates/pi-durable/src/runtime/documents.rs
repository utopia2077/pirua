use crate::{Error, Result, session::*};
use pi_ai::{FiniteF64, SafeUnsigned, Usage};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeMap;

fn conversation_doc(
    kind: &str,
    history: History,
    fork: ForkPolicy,
    initial: Value,
    checkpoint: impl Fn(&Value) -> bool + Send + Sync + 'static,
) -> DocDefinition {
    DocDefinition::new(
        kind,
        1,
        DocumentSemantics::Conversation { history, fork },
        false,
        move |_| Ok(initial.clone()),
        move |v, _, _| Ok(checkpoint(v)),
    )
    .expect("builtin definition")
}
pub fn agent_doc() -> DocDefinition {
    conversation_doc(
        "pi.agent",
        History::Rewindable,
        ForkPolicy::AsOf,
        json!({}),
        |_| true,
    )
}
pub fn live_doc() -> DocDefinition {
    conversation_doc(
        "pi.live",
        History::Latest,
        ForkPolicy::Initial,
        json!({}),
        |v| {
            v.get("generation").is_none()
                && !v
                    .get("tools")
                    .into_iter()
                    .chain(v.get("nestedTools"))
                    .flat_map(|a| a.as_array().into_iter().flatten())
                    .any(|s| s["status"] == "running")
        },
    )
}
pub fn inbox_doc() -> DocDefinition {
    conversation_doc(
        "pi.inbox",
        History::Latest,
        ForkPolicy::Initial,
        json!({"items":[]}),
        |v| v["items"].as_array().is_some_and(Vec::is_empty),
    )
}
pub fn provider_doc() -> DocDefinition {
    provider_definition(None)
}
pub(crate) fn provider_definition(identity: Option<String>) -> DocDefinition {
    DocDefinition::new(
        "pi.provider",
        1,
        DocumentSemantics::Conversation {
            history: History::Latest,
            fork: ForkPolicy::Initial,
        },
        false,
        move |_| {
            let id = match &identity {
                Some(id) => id.clone(),
                None => super::facade::session_id()?,
            };
            Ok(json!({"sessionId":id}))
        },
        |_, _, _| Ok(true),
    )
    .expect("provider definition")
}
pub fn usage_doc() -> DocDefinition {
    conversation_doc(
        "pi.usage",
        History::Latest,
        ForkPolicy::Initial,
        json!({"models":{},"tools":{}}),
        |_| true,
    )
}
pub fn nested_call_doc() -> DocDefinition {
    DocDefinition::new(
        "pi.tool.nested-call",
        1,
        DocumentSemantics::Task,
        true,
        |_| Ok(json!({})),
        |_, _, _| Ok(false),
    )
    .expect("nested definition")
}
pub(crate) fn address(kind: &str, c: ConversationId) -> DocumentAddress {
    DocumentAddress {
        kind: kind.into(),
        scope: Scope::Conversation { conversation_id: c },
        key: None,
    }
}
pub(crate) fn doc(
    tx: &mut Transaction<'_>,
    def: &DocDefinition,
    c: ConversationId,
) -> Result<(DocumentId, Value)> {
    let id = tx.doc(def, address(&def.kind, c), Value::Null)?;
    Ok((id, tx.document_value(id)?.clone()))
}
pub(crate) fn set(
    tx: &mut Transaction<'_>,
    id: DocumentId,
    path: Path,
    value: Value,
) -> Result<()> {
    tx.apply(id, Op::Set(path, value))
}
pub(crate) fn save(tx: &mut Transaction<'_>, id: DocumentId, value: Value) -> Result<()> {
    let old = tx.document_value(id)?.clone();
    let mut ops = Vec::new();
    diff(&old, &value, Vec::new(), &mut ops);
    tx.apply_all(id, &ops)
}
fn diff(old: &Value, new: &Value, path: Path, ops: &mut Vec<Op>) {
    if old == new {
        return;
    }
    match (old, new) {
        (Value::Object(a), Value::Object(b)) => {
            for k in a.keys() {
                if !b.contains_key(k) {
                    let mut p = path.clone();
                    p.push(k.as_str().into());
                    ops.push(Op::Delete(p));
                }
            }
            for (k, v) in b {
                let mut p = path.clone();
                p.push(k.as_str().into());
                if let Some(prev) = a.get(k) {
                    diff(prev, v, p, ops)
                } else {
                    ops.push(Op::Set(p, v.clone()))
                }
            }
        }
        (Value::Array(a), Value::Array(b)) => {
            if a.len() == b.len() {
                for (i, (x, y)) in a.iter().zip(b).enumerate() {
                    let mut p = path.clone();
                    p.push(i.into());
                    diff(x, y, p, ops)
                }
            } else if b.starts_with(a) {
                ops.push(Op::Splice(path, a.len(), 0, b[a.len()..].to_vec()))
            } else {
                let prefix = a.iter().zip(b).take_while(|(x, y)| x == y).count();
                let suffix = a[prefix..]
                    .iter()
                    .rev()
                    .zip(b[prefix..].iter().rev())
                    .take_while(|(x, y)| x == y)
                    .count();
                ops.push(Op::Splice(
                    path,
                    prefix,
                    a.len() - prefix - suffix,
                    b[prefix..b.len() - suffix].to_vec(),
                ))
            }
        }
        (Value::String(a), Value::String(b)) if b.starts_with(a) => {
            ops.push(Op::Append(path, b[a.len()..].into()))
        }
        _ => ops.push(Op::Set(path, new.clone())),
    }
}
pub(crate) fn ai_value<T: Serialize>(v: &T) -> Result<Value> {
    Ok(serde_json::to_value(v)?)
}
pub(crate) fn decode<T: serde::de::DeserializeOwned>(v: Value) -> Result<T> {
    Ok(serde_json::from_value(v)?)
}
pub(crate) fn timestamp(now: f64) -> Result<SafeUnsigned> {
    if !now.is_finite() || now < 0.0 {
        return Err(Error::validation(
            "clock must be nonnegative finite milliseconds",
        ));
    }
    SafeUnsigned::new(now.min(MAX_SAFE_INTEGER as f64) as u64)
        .map_err(|e| Error::validation(e.to_string()))
}
pub(crate) fn append_message(
    tx: &mut Transaction<'_>,
    c: ConversationId,
    kind: &str,
    message: &pi_ai::Message,
) -> Result<EntryRecord> {
    let mut draft = EntryDraft::new(kind);
    draft.model = Some(vec![ai_value(message)?]);
    tx.append_entry(c, draft)
}
pub(crate) fn append_assistant(
    tx: &mut Transaction<'_>,
    c: ConversationId,
    message: &pi_ai::AssistantMessage,
) -> Result<EntryRecord> {
    record_usage(
        tx,
        c,
        "models",
        &format!("{}/{}", message.provider, message.model),
        &message.usage,
    )?;
    append_message(
        tx,
        c,
        "pi.assistant",
        &pi_ai::Message::Assistant(Box::new(message.clone())),
    )
}
pub(crate) fn convert_partial(
    tx: &mut Transaction<'_>,
    c: ConversationId,
    live: &Value,
) -> Result<()> {
    if let Some(v) = live.pointer("/generation/message") {
        let mut message: pi_ai::AssistantMessage = decode(v.clone())?;
        message.stop_reason = pi_ai::StopReason::Aborted;
        append_assistant(tx, c, &message)?;
    }
    Ok(())
}
pub(crate) fn end_run(
    tx: &mut Transaction<'_>,
    live: &mut Value,
    task: TaskId,
    settlement: SubmissionSettlement,
) -> Result<()> {
    if live.pointer("/run/taskId") == Some(&json!(task)) {
        for id in live["run"]["inputs"].as_array().into_iter().flatten() {
            tx.settle_submission(decode(id.clone())?, settlement.clone())?;
        }
        live.as_object_mut().unwrap().remove("run");
    }
    for key in ["generation", "tools", "nestedTools"] {
        live.as_object_mut().unwrap().remove(key);
    }
    Ok(())
}
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct UsageState {
    pub models: BTreeMap<String, Usage>,
    pub tools: BTreeMap<String, Usage>,
}
fn add_count(a: SafeUnsigned, b: SafeUnsigned) -> Result<SafeUnsigned> {
    SafeUnsigned::new(
        a.get()
            .checked_add(b.get())
            .ok_or_else(|| Error::validation("usage overflow"))?,
    )
    .map_err(|e| Error::validation(e.to_string()))
}
pub(crate) fn add_usage(a: &mut Usage, b: &Usage) -> Result<()> {
    a.input = add_count(a.input, b.input)?;
    a.output = add_count(a.output, b.output)?;
    a.cache_read = add_count(a.cache_read, b.cache_read)?;
    a.cache_write = add_count(a.cache_write, b.cache_write)?;
    a.total_tokens = add_count(a.total_tokens, b.total_tokens)?;
    if let Some(v) = b.reasoning {
        a.reasoning = Some(add_count(
            a.reasoning.unwrap_or(SafeUnsigned::new(0).unwrap()),
            v,
        )?)
    }
    if let Some(v) = b.cache_write_1h {
        a.cache_write_1h = Some(add_count(
            a.cache_write_1h.unwrap_or(SafeUnsigned::new(0).unwrap()),
            v,
        )?)
    }
    for (x, y) in [
        (&mut a.cost.input, b.cost.input),
        (&mut a.cost.output, b.cost.output),
        (&mut a.cost.cache_read, b.cost.cache_read),
        (&mut a.cost.cache_write, b.cost.cache_write),
        (&mut a.cost.total, b.cost.total),
    ] {
        *x = FiniteF64::new(x.get() + y.get()).map_err(|e| Error::validation(e.to_string()))?;
    }
    Ok(())
}
pub(crate) fn record_usage(
    tx: &mut Transaction<'_>,
    c: ConversationId,
    bucket: &str,
    key: &str,
    usage: &Usage,
) -> Result<()> {
    let (id, mut v) = doc(tx, &usage_doc(), c)?;
    let map = v[bucket].as_object_mut().unwrap();
    if let Some(old) = map.get(key) {
        let mut sum: Usage = decode(old.clone())?;
        add_usage(&mut sum, usage)?;
        map.insert(key.into(), ai_value(&sum)?);
    } else {
        map.insert(key.into(), ai_value(usage)?);
    }
    save(tx, id, v)
}
pub(crate) fn slot_mut(live: &mut Value, task: TaskId) -> Option<&mut Value> {
    for key in ["tools", "nestedTools"] {
        if let Some(index) = live[key]
            .as_array()
            .and_then(|a| a.iter().position(|s| s["taskId"] == json!(task)))
        {
            return live[key].get_mut(index);
        }
    }
    None
}
pub(crate) fn clear_progress(slot: &mut Value) {
    for key in [
        "output",
        "droppedBytes",
        "droppedLines",
        "details",
        "diagnostics",
    ] {
        slot.as_object_mut().unwrap().remove(key);
    }
}
pub(crate) fn finish_slot(slot: &mut Value) {
    slot["status"] = json!("done");
    clear_progress(slot)
}
pub(crate) fn remove_nested(live: &mut Value, task: TaskId) {
    let mut removed = std::collections::BTreeSet::from([json!(task).as_u64().unwrap()]);
    if let Some(a) = live.get_mut("nestedTools").and_then(Value::as_array_mut) {
        for s in a.iter() {
            if removed.contains(&s["parentTaskId"].as_u64().unwrap_or(0)) {
                removed.insert(s["taskId"].as_u64().unwrap_or(0));
            }
        }
        a.retain(|s| !removed.contains(&s["parentTaskId"].as_u64().unwrap_or(0)));
        if a.is_empty() {
            live.as_object_mut().unwrap().remove("nestedTools");
        }
    }
}
