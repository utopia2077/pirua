use super::*;
use crate::{session::*, *};
use pi_ai::{AssistantContent, AssistantMessage, Message, ModelThinkingLevel, StopReason};
use serde_json::{Value, json};
use std::sync::{Arc, Weak};
pub(crate) fn definition(
    owner: Weak<RuntimeInner>,
    snapshot: RuntimeRegistrySnapshot,
) -> Result<TaskDefinition> {
    let mut d = TaskDefinition::new("pi.generation", 1, |_| {
        Ok(json!({
        "phase":"prepare","attempt":1}
        ))
    })?;
    for name in ["prepare", "request", "retry", "poll", "tools"] {
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
            abort(t, p).await
        }
    }))
}
pub(crate) fn create(
    tx: &mut Transaction<'_>,
    c: ConversationId,
    owner: Weak<RuntimeInner>,
    snapshot: RuntimeRegistrySnapshot,
) -> Result<TaskId> {
    crate::create_task(
        tx,
        &definition(owner, snapshot)?,
        json!({}),
        TaskOptions {
            conversation_id: Some(c),
            ..Default::default()
        },
    )
}
pub(crate) fn start_run(
    tx: &mut Transaction<'_>,
    c: ConversationId,
    live: &mut Value,
    inputs: Vec<SubmissionId>,
    owner: Weak<RuntimeInner>,
    snapshot: RuntimeRegistrySnapshot,
) -> Result<()> {
    live["run"] = json!({
    "taskId":create(tx,c,owner,snapshot)?,"inputs":inputs}
    );
    Ok(())
}
pub(crate) fn retry_delay(policy: &pi_ai::RetryPolicy, attempt: u64) -> f64 {
    let cap = policy.max_agent_delay_ms.map_or(60000, |n| n.get());
    let delay = policy.base_delay_ms.get().saturating_mul(
        1u64.checked_shl(attempt.saturating_sub(1).min(64) as u32)
            .unwrap_or(u64::MAX),
    );
    delay.min(MAX_SAFE_INTEGER).min(cap) as f64
}
pub(crate) fn terminal(outcome: TaskOutcome) -> Option<TaskState> {
    Some(TaskState::Terminal { outcome })
}
pub(crate) fn completed(v: Value) -> Option<TaskState> {
    terminal(TaskOutcome::Completed { result: v })
}
pub(crate) async fn fail(p: &Phase, reason: &str, message: String) -> Result<()> {
    fail_response(p, reason, message, None).await
}
async fn fail_response(
    p: &Phase,
    reason: &str,
    message: String,
    response: Option<AssistantMessage>,
) -> Result<()> {
    let c = p.conversation;
    let task = p.invocation.identity().task_id;
    let settings = p.settings();
    let owner = Arc::downgrade(&p.host);
    let snapshot = p.snapshot.clone();
    let now = p.now()?;
    let reason = reason.to_string();
    p.invocation
        .commit(move |tx, _| {
            let boundary = if settings.after_failed_run == AfterFailedRun::Continue {
                Some(super::inbox::prepare(tx, c, settings.clone())?)
            } else {
                None
            };
            let (id, mut live) = doc(tx, &live_doc(), c)?;
            if let Some(response) = response {
                append_assistant(tx, c, &response)?;
            }
            end_run(
                tx,
                &mut live,
                task,
                SubmissionSettlement::Unanswered {
                    reason: reason.clone(),
                    detail: (reason == "model_error").then(|| json!(message)),
                },
            )?;
            if settings.after_failed_run == AfterFailedRun::Withdraw {
                super::inbox::withdraw(tx, c, "run_failed")?;
            }
            if let Some(b) = boundary {
                let (users, _) = super::inbox::apply(tx, b, true, now)?;
                if !users.is_empty() {
                    start_run(tx, c, &mut live, users, owner, snapshot)?;
                }
            }
            save(tx, id, live)?;
            Ok(terminal(TaskOutcome::Failed {
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
fn no_model(model: Option<&ModelRef>) -> String {
    model.map_or_else(
        || "No model is configured".into(),
        |m| format!("Model {}/{} is not available", m.provider, m.model_id),
    )
}
async fn run(task: TaskRecord, p: Phase) -> Result<()> {
    let cp = crate::definition::checkpoint(&task.state).unwrap().clone();
    match cp["phase"].as_str().unwrap() {
        "prepare" => prepare(task, p, cp).await,
        "request" => request(p, cp).await,
        "retry" => {
            let until = cp["until"].as_f64().unwrap();
            if p.now()? < until {
                return p.invocation.sleep_until(until, cp).await;
            }
            let c = p.conversation;
            let attempt = cp["attempt"].as_u64().unwrap() + 1;
            let mut next = json!({
            "phase":"prepare","attempt":attempt}
            );
            if let Some(x) = cp.get("compacted") {
                next["compacted"] = x.clone()
            }
            p.invocation
                .commit(move |tx, _| {
                    let (id, mut live) = doc(tx, &live_doc(), c)?;
                    live["generation"] = json!({
                    "attempt":attempt}
                    );
                    save(tx, id, live)?;
                    Ok(Some(TaskState::Running { checkpoint: next }))
                })
                .await
        }
        "poll" => {
            let at = cp["pollAt"].as_f64().unwrap();
            if p.now()? < at {
                return p.invocation.sleep_until(at, cp).await;
            }
            let reference: ModelRef = decode(cp["model"].clone())?;
            let models = p.models()?;
            let provider = pi_ai::ProviderId::new(reference.provider.clone())
                .map_err(|e| Error::request(e.to_string()))?;
            let mid = pi_ai::ModelId::new(reference.model_id.clone())
                .map_err(|e| Error::request(e.to_string()))?;
            let Some(model) = models.get_model(&provider, &mid) else {
                return fail(&p, "no_model", no_model(Some(&reference))).await;
            };
            let message = models
                .fetch_deferred(
                    model,
                    decode(cp["handle"].clone())?,
                    Default::default(),
                    pi_ai::DeferredFetchOptions {
                        wire: Default::default(),
                        runtime: p.request_runtime(),
                    },
                )
                .await;
            classify(p, cp, message, None).await
        }
        "tools" => tools(p, cp).await,
        _ => Err(Error::request("unknown generation phase")),
    }
}
async fn prepare(_task: TaskRecord, p: Phase, cp: Value) -> Result<()> {
    let agent = p.agent().await?;
    let settings = p.settings();
    let Some(reference) = agent.state.model.clone() else {
        return fail(&p, "no_model", no_model(None)).await;
    };
    let models = p.models()?;
    let provider = pi_ai::ProviderId::new(reference.provider.clone())
        .map_err(|e| Error::request(e.to_string()))?;
    let mid = pi_ai::ModelId::new(reference.model_id.clone())
        .map_err(|e| Error::request(e.to_string()))?;
    let Some(model) = models.get_model(&provider, &mid) else {
        return fail(&p, "no_model", no_model(Some(&reference))).await;
    };
    if let Some(compacted) = cp.get("compacted") {
        let outcomes = p
            .invocation
            .outcomes(vec![decode(compacted.clone())?])
            .await?;
        let outcome = &outcomes[0];
        if cp.get("overflow").is_some()
            && !matches!(outcome,TaskOutcome::Completed{
result:v}
if v.get("entryId").is_some())
        {
            return fail(
                &p,
                "model_error",
                cp["overflow"].as_str().unwrap_or("Context overflow").into(),
            )
            .await;
        }
        if cp["threshold"] == true && !matches!(outcome, TaskOutcome::Completed { .. }) {
            let text = match outcome {
                TaskOutcome::Failed { error, .. } | TaskOutcome::Faulted { error, .. } => {
                    format!("Compaction failed: {}", error.message)
                }
                TaskOutcome::Orphaned { reason } => format!("Compaction failed: {reason}"),
                _ => "Compaction was aborted".into(),
            };
            return fail(&p, "model_error", text).await;
        }
    }
    let view = crate::context::read_context(&p.host.session, p.conversation, None).await?;
    let entries = super::prompt::plan(&p, &view, agent.clone()).await?;
    let policy = super::compaction::policy(&settings, &reference);
    let extra = entries
        .iter()
        .flat_map(|e| e.model.as_deref().unwrap_or_default())
        .cloned()
        .map(decode::<Message>)
        .collect::<Result<Vec<_>>>()?;
    let tokens = super::compaction::estimate(&view, &extra)?;
    let window = model.context_window.get();
    let threshold = if cp.get("compacted").is_none()
        && policy.enabled
        && window > 0
        && super::compaction::select_cut(&view, policy.keep_recent_tokens)?.is_some()
    {
        if tokens as f64 > window as f64 - policy.reserve_tokens as f64 {
            Some(true)
        } else if policy.background_tokens > 0
            && tokens as f64
                > window as f64 - policy.reserve_tokens as f64 - policy.background_tokens as f64
        {
            Some(false)
        } else {
            None
        }
    } else {
        None
    };
    let c = p.conversation;
    let owner = Arc::downgrade(&p.host);
    let snapshot = p.snapshot.clone();
    let task = p.invocation.identity().task_id;
    classify_headers(&settings.stream, &p.host.options.header_policy)?;
    p.invocation.commit(move |tx, _| {
        if threshold == Some(true) {
            let child = super::compaction::create(tx, c, CompactionReason::Threshold, None, Some(task), owner, snapshot)?;
            let checkpoint = json!({"phase":"prepare","attempt":cp["attempt"],"compacted":child,"threshold":true});
            return Ok(Some(TaskState::Waiting { checkpoint, on: vec![child], policy: JoinPolicy::AllSettled }));
        }
        let mut cutoff = tx.scan_entries(&EntryQuery::new(c), 1, None)?.items.first().map(|e|e.id);
        for entry in entries { cutoff = Some(tx.append_entry(c,entry)?.id); }
        let cutoff = cutoff.ok_or_else(||Error::request("conversation has no entries to send"))?;
        if threshold == Some(false) && doc(tx, &live_doc(), c)?.1.get("compactions").is_none() {
            super::compaction::create(tx, c, CompactionReason::Threshold, None, None, owner, snapshot)?;
        }
        let mut checkpoint = json!({"phase":"request","attempt":cp["attempt"],"model":reference,"thinkingLevel":agent.thinking_level,"streamOptions":settings.stream,"cutoff":cutoff});
        if let Some(compacted) = cp.get("compacted") { checkpoint["compacted"] = compacted.clone(); }
        Ok(Some(TaskState::Running { checkpoint }))
    }).await
}
pub(crate) fn classify_headers(
    options: &ConversationStreamOptions,
    policy: &pi_ai::ModelHeaderPolicy,
) -> Result<()> {
    if let Some(headers) = &options.headers {
        pi_ai::RawModelHeaders::new(headers.clone())
            .and_then(|h| h.classify(policy))
            .map_err(|e| Error::request(e.to_string()))?;
    }
    Ok(())
}
pub(crate) fn stream_options(
    p: &Phase,
    options: &ConversationStreamOptions,
    thinking: ModelThinkingLevel,
    session_id: String,
    max_tokens: Option<u64>,
    summary: bool,
) -> Result<(pi_ai::ModelsRequestOverrides, pi_ai::SimpleStreamOptions)> {
    classify_headers(options, &p.host.options.header_policy)?;
    let mut wire = pi_ai::SimpleStreamOptionsWire::default();
    wire.stream.transport = options.transport;
    wire.stream.timeout_ms = options.timeout_ms;
    wire.stream.max_retries = options.max_retries;
    wire.stream.max_retry_delay_ms = options.max_retry_delay_ms;
    wire.stream.metadata = options.metadata.clone();
    wire.stream.cache_retention = if summary {
        Some(pi_ai::CacheRetention::None)
    } else {
        options.cache_retention
    };
    wire.stream.session_id = Some(session_id);
    wire.stream.max_tokens = max_tokens
        .map(pi_ai::SafeUnsigned::new)
        .transpose()
        .map_err(|e| Error::request(e.to_string()))?;
    wire.reasoning = match thinking {
        ModelThinkingLevel::Off => None,
        other => Some(decode(ai_value(&other)?)?),
    };
    wire.deferred = if summary {
        None
    } else {
        options.deferred.clone()
    };
    let overrides = pi_ai::ModelsRequestOverrides {
        headers: pi_ai::ProviderHeaders::new(
            options
                .headers
                .clone()
                .unwrap_or_default()
                .into_iter()
                .map(|(k, v)| (k, Some(v)))
                .collect(),
        ),
        ..Default::default()
    };
    Ok((
        overrides,
        pi_ai::SimpleStreamOptions {
            wire,
            runtime: p.request_runtime(),
        },
    ))
}
async fn request(p: Phase, cp: Value) -> Result<()> {
    let c = p.conversation;
    let attempt = cp["attempt"].clone();
    p.invocation
        .commit(move |tx, _| {
            let (id, mut live) = doc(tx, &live_doc(), c)?;
            convert_partial(tx, c, &live)?;
            live["generation"] = json!({
            "attempt":attempt}
            );
            save(tx, id, live)?;
            Ok(None)
        })
        .await?;
    let reference: ModelRef = decode(cp["model"].clone())?;
    let models = p.models()?;
    let provider = pi_ai::ProviderId::new(reference.provider.clone())
        .map_err(|e| Error::request(e.to_string()))?;
    let mid = pi_ai::ModelId::new(reference.model_id.clone())
        .map_err(|e| Error::request(e.to_string()))?;
    let Some(model) = models.get_model(&provider, &mid) else {
        return fail(&p, "no_model", no_model(Some(&reference))).await;
    };
    let view =
        crate::context::read_context(&p.host.session, c, Some(decode(cp["cutoff"].clone())?))
            .await?;
    let original = view.messages;
    let mut messages = original.clone();
    for hooks in p.hooks().await? {
        if let Some(hook) = hooks.before_request {
            match hook(messages.clone(), p.api()).await {
                Ok(Some(v)) => messages = v,
                Ok(None) => (),
                Err(e) => {
                    p.check()?;
                    p.report(e)
                }
            }
        }
    }
    let options: ConversationStreamOptions = decode(cp["streamOptions"].clone())?;
    let thinking = decode(cp["thinkingLevel"].clone())?;
    let session = p.provider_session_id().await?;
    let (overrides, options) = stream_options(&p, &options, thinking, session, None, false)?;
    let mut stream = models.stream_simple(
        model,
        pi_ai::Context {
            system_prompt: None,
            messages,
            tools: None,
        },
        overrides,
        options,
    );
    let interval = p.settings().partial_interval_ms.max(0.0);
    let ready = Arc::new(tokio::sync::Notify::new());
    let mut timer: Option<Timer> = None;
    let mut pending = None;
    loop {
        tokio::select! {
                    event = stream.next() => match event {
                        None => break,
                        Some(event) => {
                            let partial = match event {
                                pi_ai::AssistantMessageEvent::Done{
        ..}
        |pi_ai::AssistantMessageEvent::Error{
        ..}
         => None,
                                pi_ai::AssistantMessageEvent::Start{
        partial}
        |pi_ai::AssistantMessageEvent::TextStart{
        partial,..}
        |
                                pi_ai::AssistantMessageEvent::TextDelta{
        partial,..}
        |pi_ai::AssistantMessageEvent::TextEnd{
        partial,..}
        |
                                pi_ai::AssistantMessageEvent::ThinkingStart{
        partial,..}
        |pi_ai::AssistantMessageEvent::ThinkingDelta{
        partial,..}
        |
                                pi_ai::AssistantMessageEvent::ThinkingEnd{
        partial,..}
        |pi_ai::AssistantMessageEvent::ToolCallStart{
        partial,..}
        |
                                pi_ai::AssistantMessageEvent::ToolCallDelta{
        partial,..}
        |pi_ai::AssistantMessageEvent::ToolCallEnd{
        partial,..}
         => Some(*partial),
                            }
        ;
                            if let Some(partial) = partial.filter(|m| !m.content.is_empty()) {
                                pending = Some(partial);
                                if timer.is_none() {
                                    let ready = ready.clone();
                                    timer = Some(p.invocation.set_timer(move || ready.notify_one(), interval)?);
                                }
                            }
                        }
                    }
        ,
                    _ = ready.notified(), if timer.is_some() => {
                        timer.take();
                        if let Some(partial) = pending.take() {
                            let attempt = cp["attempt"].clone();
                            if let Err(error) = p.invocation.commit(move |tx, _| {
                                let (id, mut live) = doc(tx, &live_doc(), c)?;
                                if live.get("generation").is_none() {
         live["generation"] = json!({
        "attempt":attempt}
        );
         }
                                live["generation"]["message"] = ai_value(&partial)?;
                                save(tx, id, live)?;
                                Ok(None)
                            }
        ).await
                                && !p.cancellation.is_cancelled() {
         p.report(error);
         }
                        }
                    }
                }
    }
    if let Some(timer) = timer {
        timer.cancel();
    }
    let message = stream.result().await;
    classify(p, cp, message, Some(original)).await
}
async fn classify(
    p: Phase,
    cp: Value,
    message: AssistantMessage,
    original: Option<Vec<Message>>,
) -> Result<()> {
    p.check()?;
    let c = p.conversation;
    let task = p.invocation.identity().task_id;
    if message.stop_reason == StopReason::Deferred
        && let Some(handle) = message.deferred
    {
        let poll_at = (p.now()? + handle.poll_after_ms.map_or(5000.0, |n| n.get() as f64))
            .max(cp["pollAt"].as_f64().map_or(0.0, |x| x + 1.0));
        let mut next = json!({
        "phase":"poll","attempt":cp["attempt"],"model":cp["model"],"cutoff":cp["cutoff"],"handle":handle,"pollAt":poll_at}
        );
        if let Some(v) = cp.get("compacted") {
            next["compacted"] = v.clone()
        }
        return p
            .invocation
            .commit(move |tx, _| {
                let (id, mut live) = doc(tx, &live_doc(), c)?;
                live["generation"] = json!({
                "attempt":cp["attempt"],"deferred":{
                "pollAt":poll_at}
                }
                );
                save(tx, id, live)?;
                Ok(Some(TaskState::Sleeping {
                    checkpoint: next,
                    until: poll_at,
                }))
            })
            .await;
    }
    for h in p.hooks().await? {
        if let Some(h) = h.after_response
            && let Err(e) = h(message.clone(), p.api()).await
        {
            p.check()?;
            p.report(e)
        }
    }
    let calls = message
        .content
        .iter()
        .filter_map(|c| {
            if let AssistantContent::ToolCall(c) = c {
                Some(c.clone())
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    if message.stop_reason == StopReason::ToolUse && !calls.is_empty() {
        return start_tools(p, cp, message, calls, original).await;
    }
    if matches!(
        message.stop_reason,
        StopReason::Stop | StopReason::Length | StopReason::ToolUse
    ) {
        let mut continuation = None;
        for h in p.hooks().await? {
            if continuation.is_some() {
                break;
            }
            if let Some(h) = h.on_yield {
                match h(message.clone(), p.api()).await {
                    Ok(v) => continuation = v,
                    Err(e) => {
                        p.check()?;
                        p.report(e)
                    }
                }
            }
        }
        let settings = p.settings();
        let now = p.now()?;
        let owner = Arc::downgrade(&p.host);
        let snapshot = p.snapshot.clone();
        return p
            .invocation
            .commit(move |tx, _| {
                let boundary = super::inbox::prepare(tx, c, settings)?;
                let (id, mut live) = doc(tx, &live_doc(), c)?;
                let e = append_assistant(tx, c, &message)?;
                let (users, reset) = super::inbox::apply(tx, boundary, true, now)?;
                if let Some(content) = continuation.filter(|_| users.is_empty() && !reset) {
                    append_message(
                        tx,
                        c,
                        "pi.user",
                        &Message::User(pi_ai::UserMessage {
                            role: Default::default(),
                            content,
                            timestamp: timestamp(now)?,
                        }),
                    )?;
                    live["run"]["taskId"] = json!(create(tx, c, owner, snapshot)?);
                    live.as_object_mut().unwrap().remove("generation");
                } else {
                    end_run(
                        tx,
                        &mut live,
                        task,
                        SubmissionSettlement::Done { answer: e.id },
                    )?;
                    if !users.is_empty() {
                        start_run(tx, c, &mut live, users, owner, snapshot)?;
                    }
                }
                save(tx, id, live)?;
                Ok(completed(json!({
                "entryId":e.id}
                )))
            })
            .await;
    }
    let settings = p.settings();
    let reference: ModelRef = decode(cp["model"].clone())?;
    let policy = super::compaction::policy(&settings, &reference);
    let overflow =
        message.stop_reason == StopReason::Error && pi_ai::is_context_overflow(&message, None);
    if overflow && cp.get("compacted").is_none() && policy.enabled {
        let view =
            crate::context::read_context(&p.host.session, c, Some(decode(cp["cutoff"].clone())?))
                .await?;
        if super::compaction::select_cut(&view, policy.keep_recent_tokens)?.is_some() {
            let owner = Arc::downgrade(&p.host);
            let snapshot = p.snapshot.clone();
            return p.invocation.commit(move |tx, _| {
                let (id, mut live) = doc(tx, &live_doc(), c)?;
                append_assistant(tx, c, &message)?;
                live.as_object_mut().unwrap().remove("generation");
                save(tx, id, live)?;
                let child = super::compaction::create(tx, c, CompactionReason::Overflow, None, Some(task), owner, snapshot)?;
                let checkpoint = json!({"phase":"prepare","attempt":cp["attempt"],"compacted":child,"overflow":message.error_message.unwrap_or("Context overflow".into())});
                Ok(Some(TaskState::Waiting { checkpoint, on: vec![child], policy: JoinPolicy::AllSettled }))
            }).await;
        }
    }
    let attempt = cp["attempt"].as_u64().unwrap();
    let retry = message.stop_reason == StopReason::Error
        && !overflow
        && pi_ai::is_retryable_assistant_error(&message)
        && settings.retry.enabled
        && attempt <= settings.retry.max_retries.get();
    if retry {
        let until = p.now()? + retry_delay(&settings.retry, attempt);
        return p
            .invocation
            .commit(move |tx, _| {
                let (id, mut live) = doc(tx, &live_doc(), c)?;
                append_assistant(tx, c, &message)?;
                live["generation"] = json!({
                "attempt":attempt,"retry":{
                "at":until,"error":message.error_message.unwrap_or_default()}
                }
                );
                save(tx, id, live)?;
                let mut checkpoint = json!({
                "phase":"retry","attempt":attempt,"until":until}
                );
                if let Some(v) = cp.get("compacted") {
                    checkpoint["compacted"] = v.clone()
                }
                Ok(Some(TaskState::Sleeping { checkpoint, until }))
            })
            .await;
    }
    let text = message.error_message.clone().unwrap_or_else(|| {
        format!(
            "Model response ended with stop reason {:?}",
            message.stop_reason
        )
    });
    fail_response(&p, "model_error", text, Some(message)).await
}
async fn start_tools(
    p: Phase,
    cp: Value,
    message: AssistantMessage,
    calls: Vec<pi_ai::ToolCall>,
    original: Option<Vec<Message>>,
) -> Result<()> {
    let messages = if let Some(m) = original {
        m
    } else {
        crate::context::read_context(
            &p.host.session,
            p.conversation,
            Some(decode(cp["cutoff"].clone())?),
        )
        .await?
        .messages
    };
    let offered = pi_ai::get_current_tools(&messages)
        .into_iter()
        .map(|t| t.name)
        .collect::<Vec<_>>();
    let agent = p.agent().await?;
    let sequential = p.settings().tool_execution == ToolExecutionMode::Sequential
        || calls.iter().any(|c| {
            offered.contains(&c.name)
                && agent.tools.iter().any(|t| {
                    t.name() == c.name.as_str()
                        && t.execution_mode == Some(ToolExecutionMode::Sequential)
                })
        });
    let c = p.conversation;
    let task = p.invocation.identity().task_id;
    let owner = Arc::downgrade(&p.host);
    let snapshot = p.snapshot.clone();
    let now = p.now()?;
    p.invocation
        .commit(move |tx, _| {
            let (id, mut live) = doc(tx, &live_doc(), c)?;
            let e = append_assistant(tx, c, &message)?;
            let mut tasks = Vec::new();
            let mut pending = Vec::new();
            let mut slots = Vec::new();
            for call in calls {
                if !offered.contains(&call.name) {
                    let result = super::tool::error_result(
                        "tool_unavailable",
                        format!("Tool {} is not available", call.name),
                    );
                    let entry = super::tool::append_tool_result(tx, c, &call, &result, now, None)?;
                    slots.push(json!({
                    "callId":call.id,"name":call.name,"status":"done","entry":entry.id}
                    ));
                    continue;
                }
                if sequential && !tasks.is_empty() {
                    pending.push(call.id.clone());
                    slots.push(json!({
                    "callId":call.id,"name":call.name,"status":"pending"}
                    ));
                    continue;
                }
                let def = super::tool::definition(owner.clone(), snapshot.clone())?;
                let id = crate::create_task(
                    tx,
                    &def,
                    json!({
                    "kind":"model","assistant":e.id,"callId":call.id}
                    ),
                    TaskOptions {
                        owner: Some(task),
                        ..Default::default()
                    },
                )?;
                tasks.push(id);
                slots.push(json!({
                "callId":call.id,"name":call.name,"taskId":id,"status":"pending"}
                ));
            }
            live.as_object_mut().unwrap().remove("generation");
            live["tools"] = json!(slots);
            save(tx, id, live)?;
            Ok(Some(TaskState::Waiting {
                checkpoint: json!({
                "phase":"tools","assistant":e.id,"tools":tasks,"pending":pending}
                ),
                on: tasks,
                policy: JoinPolicy::AllSettled,
            }))
        })
        .await
}
async fn tools(p: Phase, cp: Value) -> Result<()> {
    if let Some(next) = cp["pending"].as_array().and_then(|a| a.first()).cloned() {
        let c = p.conversation;
        let task = p.invocation.identity().task_id;
        let owner = Arc::downgrade(&p.host);
        let snapshot = p.snapshot.clone();
        return p
            .invocation
            .commit(move |tx, _| {
                let (id, mut live) = doc(tx, &live_doc(), c)?;
                let child = crate::create_task(
                    tx,
                    &super::tool::definition(owner, snapshot)?,
                    json!({
                    "kind":"model","assistant":cp["assistant"],"callId":next}
                    ),
                    TaskOptions {
                        owner: Some(task),
                        ..Default::default()
                    },
                )?;
                if let Some(slot) = live["tools"]
                    .as_array_mut()
                    .and_then(|a| a.iter_mut().find(|s| s["callId"] == next))
                {
                    slot["taskId"] = json!(child)
                }
                save(tx, id, live)?;
                let mut checkpoint = cp;
                checkpoint["tools"]
                    .as_array_mut()
                    .unwrap()
                    .push(json!(child));
                checkpoint["pending"].as_array_mut().unwrap().remove(0);
                Ok(Some(TaskState::Waiting {
                    checkpoint,
                    on: vec![child],
                    policy: JoinPolicy::AllSettled,
                }))
            })
            .await;
    }
    let ids = decode::<Vec<TaskId>>(cp["tools"].clone())?;
    let outcomes = p.invocation.outcomes(ids.clone()).await?;
    let mut controls = std::collections::BTreeMap::new();
    for (id, outcome) in ids.into_iter().zip(outcomes) {
        let control = match outcome {
            TaskOutcome::Completed { result: v } => v.get("control").cloned(),
            _ => None,
        };
        controls.insert(id, control);
    }
    let live = p
        .host
        .session
        .snapshot(live_doc(), address("pi.live", p.conversation))
        .await?
        .unwrap_or(json!({}));
    let slots = live["tools"].as_array().cloned().unwrap_or_default();
    let result_entries = slots
        .iter()
        .filter_map(|s| s.get("entry"))
        .cloned()
        .map(decode)
        .collect::<Result<Vec<EntryId>>>()?;
    let assistant = decode(cp["assistant"].clone())?;
    for h in p.hooks().await? {
        if let Some(h) = h.after_tools
            && let Err(e) = h(assistant, result_entries.clone(), p.api()).await
        {
            p.check()?;
            p.report(e)
        }
    }
    let terminate = !slots.is_empty()
        && slots.iter().all(|s| {
            s.get("taskId")
                .and_then(|v| decode::<TaskId>(v.clone()).ok())
                .and_then(|id| controls.get(&id))
                .and_then(|v| v.as_ref())
                .is_some_and(|v| v["terminate"] == true)
        });
    let handoff = controls
        .values()
        .flatten()
        .filter_map(|v| v["handoff"].as_str())
        .next_back()
        .map(str::to_owned);
    let added = controls
        .values()
        .flatten()
        .flat_map(|v| v["addTools"].as_array().into_iter().flatten())
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let c = p.conversation;
    let task = p.invocation.identity().task_id;
    let settings = p.settings();
    let owner = Arc::downgrade(&p.host);
    let snapshot = p.snapshot.clone();
    let now = p.now()?;
    p.invocation
        .commit(move |tx, _| {
            let mut boundary = super::inbox::prepare(tx, c, settings)?;
            if !added.is_empty() {
                add_tools(tx, c, &added)?;
            }
            let (id, mut live) = doc(tx, &live_doc(), c)?;
            if terminate || handoff.is_some() {
                if let Some(content) = handoff {
                    let mut draft = EntryDraft::new("pi.reset");
                    draft.head = Some(EntryHead::Own);
                    draft.model = Some(vec![json!({
                    "role":"user","content":content,"timestamp":timestamp(now)?}
                    )]);
                    boundary.head = Some(tx.append_entry(c, draft)?.id)
                }
                let (users, _) = super::inbox::apply(tx, boundary, true, now)?;
                end_run(
                    tx,
                    &mut live,
                    task,
                    SubmissionSettlement::Done { answer: assistant },
                )?;
                if !users.is_empty() {
                    start_run(tx, c, &mut live, users, owner, snapshot)?;
                }
            } else {
                let (users, reset) = super::inbox::apply(tx, boundary, false, now)?;
                if reset {
                    end_run(
                        tx,
                        &mut live,
                        task,
                        SubmissionSettlement::Unanswered {
                            reason: "reset".into(),
                            detail: None,
                        },
                    )?;
                    if !users.is_empty() {
                        start_run(tx, c, &mut live, users, owner, snapshot)?;
                    }
                } else {
                    live.as_object_mut().unwrap().remove("tools");
                    live.as_object_mut().unwrap().remove("nestedTools");
                    if live.pointer("/run/taskId") == Some(&json!(task)) {
                        live["run"]["inputs"]
                            .as_array_mut()
                            .unwrap()
                            .extend(users.into_iter().map(|v| json!(v)));
                        live["run"]["taskId"] = json!(create(tx, c, owner, snapshot)?);
                    }
                }
            }
            save(tx, id, live)?;
            Ok(completed(json!({
            "entryId":assistant}
            )))
        })
        .await
}
fn add_tools(tx: &mut Transaction<'_>, c: ConversationId, added: &[String]) -> Result<()> {
    let (id, mut state) = doc(tx, &agent_doc(), c)?;
    if let Some(a) = state["tools"].as_array_mut() {
        for name in added {
            if !a.contains(&json!(name)) {
                a.push(json!(name));
            }
        }
    } else if let Some(a) = state
        .pointer_mut("/tools/remove")
        .and_then(Value::as_array_mut)
    {
        a.retain(|v| !added.iter().any(|n| v == &json!(n)));
    }
    if let Some(a) = state["modelTools"].as_array_mut() {
        for name in added {
            if !a.contains(&json!(name)) {
                a.push(json!(name))
            }
        }
    } else {
        let old = state.get("modelTools").cloned().unwrap_or(json!({}));
        let remove = old["remove"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter(|v| !added.iter().any(|n| v == &json!(n)))
            .collect::<Vec<_>>();
        let mut add = old["add"].as_array().cloned().unwrap_or_default();
        for name in added {
            if !add.contains(&json!(name)) {
                add.push(json!(name))
            }
        }
        state["modelTools"] = json!({
        "add":add}
        );
        if !remove.is_empty() {
            state["modelTools"]["remove"] = json!(remove)
        }
    }
    save(tx, id, state)
}
async fn abort(task: TaskRecord, p: Phase) -> Result<()> {
    let cp = crate::definition::checkpoint(&task.state).unwrap().clone();
    if cp["phase"] == "poll" {
        let reference: ModelRef = decode(cp["model"].clone())?;
        let models = p.models()?;
        let provider = pi_ai::ProviderId::new(reference.provider)
            .map_err(|e| Error::request(e.to_string()))?;
        let mid =
            pi_ai::ModelId::new(reference.model_id).map_err(|e| Error::request(e.to_string()))?;
        if let Some(model) = models.get_model(&provider, &mid)
            && let Err(e) = models
                .cancel_deferred(
                    model,
                    decode(cp["handle"].clone())?,
                    Default::default(),
                    pi_ai::DeferredCancelOptions {
                        wire: Default::default(),
                        runtime: p.request_runtime(),
                    },
                )
                .await
        {
            p.report(Error::request(e.to_string()))
        }
    }
    let mut calls = vec![];
    if cp["phase"] == "tools" {
        let e = p
            .host
            .session
            .entry(decode(cp["assistant"].clone())?)
            .await?
            .ok_or_else(|| Error::request("assistant missing"))?;
        let message: AssistantMessage = decode(e.entry.model.unwrap()[0].clone())?;
        for content in message.content {
            if let AssistantContent::ToolCall(call) = content
                && cp["pending"].as_array().unwrap().contains(&json!(call.id))
            {
                calls.push(call)
            }
        }
    }
    let c = p.conversation;
    let id = task.id;
    let now = p.now()?;
    p.invocation
        .commit(move |tx, _| {
            let (d, mut live) = doc(tx, &live_doc(), c)?;
            convert_partial(tx, c, &live)?;
            for call in calls {
                super::tool::append_tool_result(
                    tx,
                    c,
                    &call,
                    &super::tool::error_result(
                        "aborted",
                        format!("Tool {} was aborted", call.name),
                    ),
                    now,
                    None,
                )?;
            }
            end_run(
                tx,
                &mut live,
                id,
                SubmissionSettlement::Unanswered {
                    reason: "aborted".into(),
                    detail: None,
                },
            )?;
            save(tx, d, live)?;
            Ok(terminal(TaskOutcome::Aborted {
                reason: None,
                result: None,
            }))
        })
        .await
}
