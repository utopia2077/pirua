use super::*;
use crate::{session::*, *};
use pi_ai::{AssistantContent, Message, StopReason};
use serde_json::{Value, json};
use std::sync::Weak;
pub(crate) fn definition(
    owner: Weak<RuntimeInner>,
    snapshot: RuntimeRegistrySnapshot,
) -> Result<TaskDefinition> {
    let mut d = TaskDefinition::new("pi.compaction", 1, |_| {
        Ok(json!({
        "phase":"select"}
        ))
    })?;
    for name in ["select", "summarize", "retry"] {
        let o = owner.clone();
        let s = snapshot.clone();
        d = d.phase(name, move |t, i| {
            let o = o.clone();
            let s = s.clone();
            async move {
                let p = Phase::new(&o, i, s, t.conversation_id)?;
                run(t, p).await
            }
        });
    }
    Ok(d.abort(move |t, i| {
        let o = owner.clone();
        let s = snapshot.clone();
        async move {
            let p = Phase::new(&o, i, s, t.conversation_id)?;
            let c = t.conversation_id;
            let task = t.id;
            p.invocation
                .commit(move |tx, _| {
                    let (id, mut live) = doc(tx, &live_doc(), c)?;
                    remove_status(&mut live, task);
                    save(tx, id, live)?;
                    Ok(super::generation::terminal(TaskOutcome::Aborted {
                        reason: None,
                        result: None,
                    }))
                })
                .await
        }
    }))
}
pub(crate) fn create(
    tx: &mut Transaction<'_>,
    c: ConversationId,
    reason: CompactionReason,
    instructions: Option<String>,
    owner: Option<TaskId>,
    runtime: Weak<RuntimeInner>,
    snapshot: RuntimeRegistrySnapshot,
) -> Result<TaskId> {
    let background = owner.is_none() && !matches!(reason, CompactionReason::Manual);
    let mut input = json!({
    "reason":reason}
    );
    if let Some(text) = instructions {
        input["instructions"] = json!(text)
    }
    let task = crate::create_task(
        tx,
        &definition(runtime, snapshot)?,
        input,
        TaskOptions {
            conversation_id: Some(c),
            owner,
            background,
            ..Default::default()
        },
    )?;
    let (id, mut live) = doc(tx, &live_doc(), c)?;
    live.as_object_mut()
        .unwrap()
        .entry("compactions")
        .or_insert(json!([]))
        .as_array_mut()
        .unwrap()
        .push(json!({
        "taskId":task,"reason":reason,"blocking":owner.is_some(),"attempt":1}
        ));
    save(tx, id, live)?;
    Ok(task)
}
pub(crate) fn remove_status(live: &mut Value, task: TaskId) {
    if let Some(a) = live["compactions"].as_array_mut() {
        a.retain(|v| v["taskId"] != json!(task));
        if a.is_empty() {
            live.as_object_mut().unwrap().remove("compactions");
        }
    }
}
pub(crate) fn fail_status(live: &mut Value, task: TaskId, error: String) {
    if let Some(status) = live["compactions"]
        .as_array()
        .and_then(|a| a.iter().find(|v| v["taskId"] == json!(task)))
    {
        live["compactionFailure"] = json!({
        "taskId":task,"reason":status["reason"],"error":error}
        );
    }
    remove_status(live, task)
}
pub(crate) fn policy(settings: &DurableSettings, model: &ModelRef) -> CompactionPolicy {
    settings
        .compaction_overrides
        .get(&format!("{}/{}", model.provider, model.model_id))
        .unwrap_or(&settings.compaction)
        .clone()
}
pub(crate) fn select_cut(view: &crate::context::ContextView, recent: u64) -> Result<Option<usize>> {
    let start = usize::from(view.head.is_some());
    let mut candidates = Vec::new();
    for index in start..view.contributions.len() {
        let candidate = match view.contributions[index].first() {
            Some(Message::Assistant(_)) => true,
            Some(Message::User(_)) => {
                let calls = view.contributions[..index]
                    .iter()
                    .rev()
                    .flat_map(|v| v.iter().rev())
                    .find_map(|m| {
                        if let Message::Assistant(a) = m {
                            Some(
                                a.content
                                    .iter()
                                    .filter_map(|c| {
                                        if let AssistantContent::ToolCall(c) = c {
                                            Some(c.id.clone())
                                        } else {
                                            None
                                        }
                                    })
                                    .collect::<Vec<_>>(),
                            )
                        } else {
                            None
                        }
                    })
                    .unwrap_or_default();
                let mut valid = true;
                'after: for (after, messages) in view.contributions[index..].iter().enumerate() {
                    for (position, m) in messages.iter().enumerate() {
                        match m {
                            Message::Assistant(_) if after > 0 || position > 0 => break 'after,
                            Message::ToolResult(r) if calls.contains(&r.tool_call_id) => {
                                valid = false;
                                break 'after;
                            }
                            _ => (),
                        }
                    }
                }
                valid
            }
            _ => false,
        };
        if candidate {
            candidates.push(index)
        }
    }
    let mut kept = 0;
    for index in (start..view.contributions.len()).rev() {
        for m in &view.contributions[index] {
            kept += pi_ai::estimate_message_tokens(m)
                .map_err(|e| Error::validation(e.to_string()))?
                .get();
        }
        if kept >= recent {
            let cut = candidates
                .iter()
                .find(|n| **n >= index)
                .or(candidates.last())
                .copied();
            return Ok(cut.filter(|cut| {
                view.contributions[start..*cut]
                    .iter()
                    .any(|v| !v.is_empty())
            }));
        }
    }
    Ok(None)
}
pub(crate) fn estimate(view: &crate::context::ContextView, extra: &[Message]) -> Result<u64> {
    let after = view.head.as_ref().map(|h| h.id);
    let measured = view
        .entries
        .iter()
        .zip(&view.contributions)
        .rev()
        .filter(|(e, _)| after.is_none_or(|h| e.id > h))
        .flat_map(|(_, v)| v.iter().rev())
        .find_map(|m| {
            if let Message::Assistant(a) = m {
                pi_ai::calculate_context_tokens(&a.usage)
                    .ok()
                    .filter(|n| n.get() > 0)
                    .map(|n| (m, n.get()))
            } else {
                None
            }
        });
    let (mut tokens, from) = if let Some((m, n)) = measured {
        (
            n,
            view.messages
                .iter()
                .rposition(|v| v == m)
                .map_or(0, |i| i + 1),
        )
    } else {
        (0, 0)
    };
    for m in view.messages[from..].iter().chain(extra) {
        tokens += pi_ai::estimate_message_tokens(m)
            .map_err(|e| Error::validation(e.to_string()))?
            .get();
    }
    Ok(tokens)
}
async fn finish(
    p: &Phase,
    first_kept: Option<EntryId>,
    summary: Option<String>,
    spend: Option<(String, pi_ai::Usage)>,
) -> Result<()> {
    let c = p.conversation;
    let task = p.invocation.identity().task_id;
    let now = p.now()?;
    let host = p.host.clone();
    let snapshot = p.snapshot.clone();
    p.invocation.commit(move |tx, current| {
        if let Some((key, usage)) = spend { record_usage(tx, c, "models", &key, &usage)?; }
        let (id, mut live) = doc(tx, &live_doc(), c)?;
        remove_status(&mut live, task);
        let mut result = CompactionResult::default();
        if let (Some(first_kept), Some(summary)) = (first_kept, summary) {
            live.as_object_mut().unwrap().remove("compactionFailure");
            save(tx, id, live)?;
            let mut entry = EntryDraft::new("pi.compaction");
            entry.head = Some(EntryHead::Entry(first_kept));
            let content = format!("The conversation history before this point was compacted into the following summary:\n\n<summary>\n{summary}\n</summary>");
            entry.model = Some(vec![json!({"role":"user","content":[{"type":"text","text":content}],"timestamp":timestamp(now)?})]);
            entry.data = Some(json!({"reason":current.input["reason"]}));
            if current.owner.is_some() {
                result.entry_id = Some(tx.append_entry(c, entry)?.id);
            } else {
                result.submission_id = Some(super::inbox::admit(tx, c, SubmissionDraft::Write { request_id: Some(format!("compaction:{task}")), entry }, &host, &snapshot)?);
            }
        } else { save(tx, id, live)?; }
        Ok(super::generation::completed(ai_value(&result)?))
    }).await
}
async fn run(task: TaskRecord, p: Phase) -> Result<()> {
    let cp = crate::definition::checkpoint(&task.state).unwrap().clone();
    let phase = cp["phase"].as_str().unwrap();
    if phase == "retry" {
        let until = cp["until"].as_f64().unwrap();
        if p.now()? < until {
            return p.invocation.sleep_until(until, cp).await;
        }
        let mut next = cp.clone();
        next["phase"] = json!("summarize");
        next["attempt"] = json!(cp["attempt"].as_u64().unwrap() + 1);
        next.as_object_mut().unwrap().remove("until");
        let c = p.conversation;
        let id = task.id;
        return p
            .invocation
            .commit(move |tx, _| {
                let (d, mut live) = doc(tx, &live_doc(), c)?;
                if let Some(s) = live["compactions"]
                    .as_array_mut()
                    .and_then(|a| a.iter_mut().find(|s| s["taskId"] == json!(id)))
                {
                    s["attempt"] = next["attempt"].clone();
                    s.as_object_mut().unwrap().remove("retry");
                }
                save(tx, d, live)?;
                Ok(Some(TaskState::Running { checkpoint: next }))
            })
            .await;
    }
    let agent = p.agent().await?;
    let reference = if phase == "select" {
        agent.state.model.clone()
    } else {
        Some(decode(cp["model"].clone())?)
    };
    let Some(reference) = reference else {
        return fail(&p, task.id, "No model is configured".into(), "no_model").await;
    };
    let provider = pi_ai::ProviderId::new(reference.provider.clone())
        .map_err(|e| Error::request(e.to_string()))?;
    let mid = pi_ai::ModelId::new(reference.model_id.clone())
        .map_err(|e| Error::request(e.to_string()))?;
    let models = p.models()?;
    let Some(model) = models.get_model(&provider, &mid) else {
        return fail(
            &p,
            task.id,
            format!(
                "Model {}/{} is not available",
                reference.provider, reference.model_id
            ),
            "no_model",
        )
        .await;
    };
    if phase == "select" {
        let settings = p.settings();
        let policy = policy(&settings, &reference);
        let view = crate::context::read_context(&p.host.session, p.conversation, None).await?;
        let Some(cut) = select_cut(&view, policy.keep_recent_tokens)? else {
            return finish(&p, None, None, None).await;
        };
        let first_kept = view.entries[cut].id;
        let messages = crate::context::order_tool_results(&view.contributions[..cut].concat())?;
        let request = CompactionRequest {
            reason: decode(task.input["reason"].clone())?,
            entries: view.entries[..cut].to_vec(),
            messages,
            first_kept,
            instructions: task.input["instructions"].as_str().map(str::to_owned),
        };
        for h in p.hooks().await? {
            if let Some(h) = h.before_compact {
                match h(request.clone(), p.api()).await {
                    Ok(Some(CompactDecision::Decline)) => {
                        return finish(&p, None, None, None).await;
                    }
                    Ok(Some(CompactDecision::Summary(s))) => {
                        return finish(&p, Some(first_kept), Some(s), None).await;
                    }
                    Ok(None) => (),
                    Err(e) => {
                        p.check()?;
                        p.report(e)
                    }
                }
            }
        }
        let tail = view.entries.iter().map(|e| e.id).max().unwrap();
        let max_tokens = ((policy.reserve_tokens as f64 * 0.8).floor() as u64).min(
            if model.max_tokens.get() > 0 {
                model.max_tokens.get()
            } else {
                u64::MAX
            },
        );
        super::generation::classify_headers(&settings.stream, &p.host.options.header_policy)?;
        let checkpoint = json!({"phase":"summarize","attempt":1,"model":reference,"thinkingLevel":agent.thinking_level,"streamOptions":settings.stream,"maxTokens":max_tokens,"tail":tail,"firstKept":first_kept});
        return p
            .invocation
            .commit(move |_, _| Ok(Some(TaskState::Running { checkpoint })))
            .await;
    }
    let tail = decode(cp["tail"].clone())?;
    let first_kept = decode(cp["firstKept"].clone())?;
    let view = crate::context::read_context(&p.host.session, p.conversation, Some(tail)).await?;
    let cut = view
        .entries
        .iter()
        .position(|e| e.id == first_kept)
        .ok_or_else(|| Error::validation("compaction cut missing"))?;
    let messages = crate::context::order_tool_results(&view.contributions[..cut].concat())?;
    let focus = task.input["instructions"]
        .as_str()
        .map(|s| format!("\n\nAdditional focus: {s}"))
        .unwrap_or_default();
    let text = format!(
        "<conversation>\n{}\n</conversation>\n\n{}{focus}",
        serialize(&messages),
        include_str!("summary_prompt.txt")
    );
    let now = timestamp(p.now()?)?;
    let context = pi_ai::Context {
        system_prompt: None,
        messages: vec![
            Message::System(pi_ai::SystemMessage {
                role: Default::default(),
                content: pi_ai::SystemContent::Text(include_str!("summary_system.txt").into()),
                sections: None,
                tools_added: None,
                tools_removed: None,
                timestamp: now,
            }),
            Message::User(pi_ai::UserMessage {
                role: Default::default(),
                content: pi_ai::UserContent::Blocks(vec![super::tool::text(text)]),
                timestamp: now,
            }),
        ],
        tools: None,
    };
    let opts = decode(cp["streamOptions"].clone())?;
    let (overrides, options) = super::generation::stream_options(
        &p,
        &opts,
        decode(cp["thinkingLevel"].clone())?,
        p.provider_session_id().await?,
        cp["maxTokens"].as_u64(),
        true,
    )?;
    let message = models
        .complete_simple(model, context, overrides, options)
        .await;
    p.check()?;
    let summary = if message.stop_reason == StopReason::Stop
        && !message
            .content
            .iter()
            .any(|c| matches!(c, AssistantContent::ToolCall(_)))
    {
        let s = message
            .content
            .iter()
            .filter_map(|c| {
                if let AssistantContent::Text(t) = c {
                    Some(t.text.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
            .join("\n")
            .trim()
            .to_string();
        (!s.is_empty()).then_some(s)
    } else {
        None
    };
    let c = p.conversation;
    let task_id = task.id;
    let usage = message.usage.clone();
    let key = format!("{}/{}", message.provider, message.model);
    if let Some(summary) = summary {
        return finish(&p, Some(first_kept), Some(summary), Some((key, usage))).await;
    }
    let settings = p.settings();
    let attempt = cp["attempt"].as_u64().unwrap();
    let retry = message.stop_reason == StopReason::Error
        && pi_ai::is_retryable_assistant_error(&message)
        && settings.retry.enabled
        && attempt <= settings.retry.max_retries.get();
    if retry {
        let until = p.now()? + super::generation::retry_delay(&settings.retry, attempt);
        let mut next = cp;
        next["phase"] = json!("retry");
        next["until"] = json!(until);
        return p
            .invocation
            .commit(move |tx, _| {
                record_usage(tx, c, "models", &key, &usage)?;
                let (id, mut live) = doc(tx, &live_doc(), c)?;
                if let Some(s) = live["compactions"]
                    .as_array_mut()
                    .and_then(|a| a.iter_mut().find(|s| s["taskId"] == json!(task_id)))
                {
                    s["retry"] = json!({
                    "at":until,"error":message.error_message.unwrap_or_default()}
                    );
                }
                save(tx, id, live)?;
                Ok(Some(TaskState::Sleeping {
                    checkpoint: next,
                    until,
                }))
            })
            .await;
    }
    let failure = match message.stop_reason {
        StopReason::Error | StopReason::Aborted => format!(
            "Summarization failed: {}",
            message
                .error_message
                .unwrap_or_else(|| format!("{:?}", message.stop_reason).to_lowercase())
        ),
        StopReason::Length => "Summarization hit the token limit; the summary is incomplete".into(),
        _ => {
            if message
                .content
                .iter()
                .any(|c| matches!(c, AssistantContent::ToolCall(_)))
            {
                "Summarization attempted to call a tool".into()
            } else {
                "Summarization produced no text".into()
            }
        }
    };
    fail_spend(&p, task_id, failure, "model_error", Some((key, usage))).await
}
async fn fail(p: &Phase, task: TaskId, message: String, reason: &str) -> Result<()> {
    fail_spend(p, task, message, reason, None).await
}
async fn fail_spend(
    p: &Phase,
    task: TaskId,
    message: String,
    reason: &str,
    spend: Option<(String, pi_ai::Usage)>,
) -> Result<()> {
    let c = p.conversation;
    let reason = reason.to_string();
    p.invocation
        .commit(move |tx, _| {
            if let Some((key, usage)) = spend {
                record_usage(tx, c, "models", &key, &usage)?;
            }
            let (id, mut live) = doc(tx, &live_doc(), c)?;
            fail_status(&mut live, task, message.clone());
            save(tx, id, live)?;
            Ok(super::generation::terminal(TaskOutcome::Failed {
                error: TaskOutcomeError {
                    message,
                    detail: Some(json!({
                    "reason":reason}
                    )),
                },
                result: None,
            }))
        })
        .await
}
fn serialize(messages: &[Message]) -> String {
    let mut parts = vec![];
    for message in messages {
        match message {
            Message::User(u) => {
                let t = content_text(&u.content);
                if !t.is_empty() {
                    parts.push(format!("[User]: {t}"))
                }
            }
            Message::Assistant(a) => {
                let text = a
                    .content
                    .iter()
                    .filter_map(|c| {
                        if let AssistantContent::Text(t) = c {
                            Some(t.text.clone())
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>();
                let calls = a
                    .content
                    .iter()
                    .filter_map(|c| {
                        if let AssistantContent::ToolCall(c) = c {
                            let args = ai_value(&c.arguments).unwrap();
                            Some(format!(
                                "{}({})",
                                c.name,
                                args.as_object()
                                    .unwrap()
                                    .iter()
                                    .map(|(k, v)| format!("{k}={v}"))
                                    .collect::<Vec<_>>()
                                    .join(", ")
                            ))
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>();
                if !text.is_empty() {
                    parts.push(format!("[Assistant]: {}", text.join("\n")))
                }
                if !calls.is_empty() {
                    parts.push(format!("[Assistant tool calls]: {}", calls.join("; ")))
                }
            }
            Message::ToolResult(t) => {
                let text = t
                    .content
                    .iter()
                    .filter_map(|c| {
                        if let pi_ai::InputContent::Text(t) = c {
                            Some(t.text.clone())
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                if !text.is_empty() {
                    let count = text.encode_utf16().count();
                    let text = if count > 2000 {
                        let prefix = text
                            .chars()
                            .scan(0, |n, c| {
                                *n += c.len_utf16();
                                (*n <= 2000).then_some(c)
                            })
                            .collect::<String>();
                        format!(
                            "{prefix}\n\n[... {} more characters truncated]",
                            count - 2000
                        )
                    } else {
                        text
                    };
                    parts.push(format!("[Tool result]: {text}"))
                }
            }
            _ => (),
        }
    }
    parts.join("\n\n")
}
fn content_text(c: &pi_ai::UserContent) -> String {
    match c {
        pi_ai::UserContent::Text(s) => s.clone(),
        pi_ai::UserContent::Blocks(v) => v
            .iter()
            .filter_map(|c| {
                if let pi_ai::InputContent::Text(t) = c {
                    Some(t.text.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
            .join("\n"),
    }
}
