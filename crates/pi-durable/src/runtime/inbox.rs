use super::*;
use crate::{session::*, *};
use serde_json::{Value, json};
use std::sync::Arc;
pub(crate) struct Boundary {
    pub c: ConversationId,
    pub id: DocumentId,
    pub inbox: Value,
    pub head: Option<EntryId>,
    pub settings: DurableSettings,
}
pub(crate) fn prepare(
    tx: &mut Transaction<'_>,
    c: ConversationId,
    settings: DurableSettings,
) -> Result<Boundary> {
    let head = tx.latest_head_marker(c)?.and_then(|e| e.head);
    let (id, inbox) = doc(tx, &inbox_doc(), c)?;
    Ok(Boundary {
        c,
        id,
        inbox,
        head,
        settings,
    })
}
fn input_entry(
    tx: &mut Transaction<'_>,
    c: ConversationId,
    content: Value,
    entry: Option<Value>,
    now: f64,
) -> Result<EntryRecord> {
    let message = decode::<pi_ai::UserContent>(content)?;
    let entry = entry.unwrap_or(json!({"kind":"pi.user"}));
    let mut draft = EntryDraft::new(entry["kind"].as_str().unwrap_or("pi.user"));
    draft.data = entry.get("data").cloned();
    draft.model = Some(vec![
        json!({"role":"user","content":message,"timestamp":timestamp(now)?}),
    ]);
    tx.append_entry(c, draft)
}
fn entry_json(d: EntryDraft) -> Result<Value> {
    let mut v = json!({"kind":d.kind});
    if let Some(m) = d.model {
        v["model"] = json!(m)
    }
    if let Some(x) = d.data {
        v["data"] = x
    }
    if let Some(x) = d.edits {
        v["edits"] = json!(x)
    }
    if let Some(x) = d.head {
        v["head"] = match x {
            EntryHead::Own => json!("self"),
            EntryHead::Entry(id) => json!(id),
        }
    }
    Ok(v)
}
fn draft(v: &Value) -> Result<EntryDraft> {
    let mut d = EntryDraft::new(
        v["kind"]
            .as_str()
            .ok_or_else(|| Error::request("entry kind required"))?,
    );
    d.model = v.get("model").map(|v| decode(v.clone())).transpose()?;
    d.data = v.get("data").cloned();
    d.edits = v.get("edits").map(|v| decode(v.clone())).transpose()?;
    d.head = match v.get("head") {
        None => None,
        Some(Value::String(s)) if s == "self" => Some(EntryHead::Own),
        Some(v) => Some(EntryHead::Entry(decode(v.clone())?)),
    };
    Ok(d)
}
pub(crate) fn has_reset(v: &Value) -> bool {
    v["items"]
        .as_array()
        .into_iter()
        .flatten()
        .any(|i| i["mode"] == "write" && i["entry"]["head"] == "self")
}
pub(crate) fn apply(
    tx: &mut Transaction<'_>,
    mut b: Boundary,
    final_boundary: bool,
    now: f64,
) -> Result<(Vec<SubmissionId>, bool)> {
    let items = b.inbox["items"].as_array().cloned().unwrap_or_default();
    let reset = has_reset(&b.inbox);
    let final_boundary = final_boundary || reset;
    let mut selected = Vec::new();
    for mode in ["write", "steer", "followUp"] {
        if mode == "followUp" && !final_boundary {
            continue;
        }
        let mode_setting = if mode == "steer" {
            b.settings.steering_mode
        } else {
            b.settings.follow_up_mode
        };
        let indexes = items
            .iter()
            .enumerate()
            .filter(|(_, i)| i["mode"] == mode)
            .map(|(i, _)| i);
        selected.extend(if mode == "write" || mode_setting == QueueMode::All {
            indexes.collect::<Vec<_>>()
        } else {
            indexes.take(1).collect()
        });
    }
    let mut users = Vec::new();
    let mut writes = selected
        .iter()
        .copied()
        .filter(|i| items[*i]["mode"] == "write")
        .collect::<Vec<_>>();
    writes.sort_unstable();
    let mut user_indexes = selected
        .iter()
        .copied()
        .filter(|i| items[*i]["mode"] != "write")
        .collect::<Vec<_>>();
    user_indexes.sort_unstable();
    for i in writes {
        let item = &items[i];
        let id = decode(item["id"].clone())?;
        let d = draft(&item["entry"])?;
        if matches!(d.head,Some(EntryHead::Entry(h))if b.head.is_some_and(|x|h<x)) {
            tx.settle_submission(
                id,
                SubmissionSettlement::Unanswered {
                    reason: "stale".into(),
                    detail: None,
                },
            )?;
            continue;
        }
        let head = d.head.clone();
        let e = tx.append_entry(b.c, d)?;
        if let Some(head) = head {
            b.head = Some(match head {
                EntryHead::Own => e.id,
                EntryHead::Entry(h) => h,
            })
        }
        tx.place_submission(id, e.id)?;
    }
    for i in user_indexes {
        let item = &items[i];
        let id = decode(item["id"].clone())?;
        let e = input_entry(
            tx,
            b.c,
            item["content"].clone(),
            item.get("entry").cloned(),
            now,
        )?;
        tx.place_submission(id, e.id)?;
        users.push(id);
    }
    selected.sort_unstable_by(|a, b| b.cmp(a));
    for i in selected {
        tx.apply(b.id, Op::Splice(vec!["items".into()], i, 1, Vec::new()))?;
    }
    Ok((users, reset))
}
pub(crate) fn admit(
    tx: &mut Transaction<'_>,
    c: ConversationId,
    draft: SubmissionDraft,
    host: &Arc<RuntimeInner>,
    snapshot: &RuntimeRegistrySnapshot,
) -> Result<SubmissionId> {
    let (request_id, ty) = match &draft {
        SubmissionDraft::Input {
            request_id, entry, ..
        } => {
            if let Some(e) = entry
                && (e.kind.is_empty() || e.kind.starts_with("pi."))
            {
                return Err(Error::request(
                    "input entry kind must be nonempty outside pi.",
                ));
            }
            (request_id.clone(), SubmissionType::Input)
        }
        SubmissionDraft::Write { request_id, .. } => (request_id.clone(), SubmissionType::Write),
    };
    if let Some(request) = &request_id
        && let Some(s) = tx.submission_by_request(c, request)?
    {
        if s.submission_type != ty {
            return Err(Error::request(format!(
                "Request {request} already identifies a submission of type {}",
                if s.submission_type == SubmissionType::Input {
                    "input"
                } else {
                    "write"
                }
            )));
        }
        return Ok(s.id);
    }
    let now = (host.options.scheduler.now)();
    let settings = host.settings();
    let value = match &draft {
        SubmissionDraft::Write { entry, .. } => Some(entry_json(entry.clone())?),
        _ => None,
    };
    if let Some(entry) = &value
        && entry.get("model").is_none()
        && entry.get("head").is_none()
        && entry.get("edits").is_none()
    {
        let (_, inbox) = doc(tx, &inbox_doc(), c)?;
        if !has_reset(&inbox) {
            let e = tx.append_entry(c, super::inbox::draft(entry)?)?;
            return Ok(tx
                .create_submission(SubmissionCreate {
                    conversation_id: c,
                    submission_type: ty,
                    request_id,
                    trace: None,
                    state: SubmissionState::Done {
                        entry: e.id,
                        answer: None,
                    },
                })?
                .id);
        }
    }
    let (live_id, mut live) = doc(tx, &live_doc(), c)?;
    let busy = live.get("run").is_some();
    if busy
        && matches!(
            &draft,
            SubmissionDraft::Input {
                when_busy: Some(BusyPolicy::Reject),
                ..
            }
        )
    {
        return Err(Error::request(format!("Conversation {c} is busy")));
    }
    let boundary = if busy {
        None
    } else {
        Some(prepare(tx, c, settings)?)
    };
    if boundary
        .as_ref()
        .is_none_or(|b| !b.inbox["items"].as_array().unwrap().is_empty())
    {
        let id = tx
            .create_submission(SubmissionCreate {
                conversation_id: c,
                submission_type: ty,
                request_id,
                trace: None,
                state: SubmissionState::Queued,
            })?
            .id;
        let item = match draft {
            SubmissionDraft::Write { .. } => json!({"id":id,"mode":"write","entry":value.unwrap()}),
            SubmissionDraft::Input {
                content,
                when_busy,
                entry,
                ..
            } => {
                let mut v = json!({"id":id,"mode":if when_busy==Some(BusyPolicy::Steer){"steer"}else{"followUp"},"content":content});
                if let Some(e) = entry {
                    v["entry"] = ai_value(&e)?;
                }
                v
            }
        };
        let (inbox_id, inbox) = match &boundary {
            Some(b) => (b.id, b.inbox.clone()),
            None => doc(tx, &inbox_doc(), c)?,
        };
        tx.apply(
            inbox_id,
            Op::Splice(
                vec!["items".into()],
                inbox["items"].as_array().unwrap().len(),
                0,
                vec![item],
            ),
        )?;
        if let Some(mut b) = boundary {
            b.inbox = tx.document_value(inbox_id)?.clone();
            let (users, _) = apply(tx, b, true, now)?;
            if !users.is_empty() {
                super::generation::start_run(
                    tx,
                    c,
                    &mut live,
                    users,
                    Arc::downgrade(host),
                    snapshot.clone(),
                )?;
                save(tx, live_id, live)?;
            }
        }
        return Ok(id);
    }
    match draft {
        SubmissionDraft::Write { entry, .. } => {
            if matches!(entry.head,Some(EntryHead::Entry(h))if boundary.as_ref().unwrap().head.is_some_and(|x|h<x))
            {
                return Ok(tx
                    .create_submission(SubmissionCreate {
                        conversation_id: c,
                        submission_type: ty,
                        request_id,
                        trace: None,
                        state: SubmissionState::Unanswered {
                            entry: None,
                            reason: "stale".into(),
                            detail: None,
                        },
                    })?
                    .id);
            }
            let e = tx.append_entry(c, entry)?;
            Ok(tx
                .create_submission(SubmissionCreate {
                    conversation_id: c,
                    submission_type: ty,
                    request_id,
                    trace: None,
                    state: SubmissionState::Done {
                        entry: e.id,
                        answer: None,
                    },
                })?
                .id)
        }
        SubmissionDraft::Input { content, entry, .. } => {
            let e = input_entry(
                tx,
                c,
                ai_value(&content)?,
                entry.map(|e| ai_value(&e)).transpose()?,
                now,
            )?;
            let id = tx
                .create_submission(SubmissionCreate {
                    conversation_id: c,
                    submission_type: ty,
                    request_id,
                    trace: None,
                    state: SubmissionState::Placed { entry: e.id },
                })?
                .id;
            super::generation::start_run(
                tx,
                c,
                &mut live,
                vec![id],
                Arc::downgrade(host),
                snapshot.clone(),
            )?;
            save(tx, live_id, live)?;
            Ok(id)
        }
    }
}
pub(crate) fn remove(tx: &mut Transaction<'_>, c: ConversationId, id: SubmissionId) -> Result<()> {
    let (d, v) = doc(tx, &inbox_doc(), c)?;
    if let Some(i) = v["items"]
        .as_array()
        .unwrap()
        .iter()
        .position(|v| v["id"] == json!(id))
    {
        tx.apply(d, Op::Splice(vec!["items".into()], i, 1, vec![]))?;
    }
    Ok(())
}
pub(crate) fn withdraw(tx: &mut Transaction<'_>, c: ConversationId, reason: &str) -> Result<()> {
    let (id, v) = doc(tx, &inbox_doc(), c)?;
    for (i, item) in v["items"].as_array().unwrap().iter().enumerate().rev() {
        if item["mode"] != "write" {
            tx.settle_submission(
                decode(item["id"].clone())?,
                SubmissionSettlement::Unanswered {
                    reason: reason.into(),
                    detail: None,
                },
            )?;
            tx.apply(id, Op::Splice(vec!["items".into()], i, 1, vec![]))?;
        }
    }
    Ok(())
}
