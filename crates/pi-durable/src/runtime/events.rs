//! Experimental committed agent events translated outside the Session and view locks.
use super::*;
use crate::view::{ConversationCommitAttachment, ConversationView};
use crate::{session::*, *};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeSet, VecDeque},
    future::Future,
    sync::{Arc, Mutex},
};
use tokio::sync::{Notify, watch};

/// A detached agent event. Its JSON fields follow the release event vocabulary.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AgentEvent(pub Value);
#[derive(Clone, Debug, PartialEq)]
pub struct AgentEventBatch {
    pub seq: Seq,
    pub events: Vec<AgentEvent>,
}
struct EventData {
    current: Arc<ConversationView>,
    pending: VecDeque<AgentEventBatch>,
    end: Option<WatchEnd>,
    mode: u8,
}
struct EventInner {
    data: Mutex<EventData>,
    notify: Notify,
    ended: watch::Sender<Option<WatchEnd>>,
    attachment: Mutex<Option<ConversationCommitAttachment>>,
    delivery: tokio::sync::Mutex<()>,
}
impl EventInner {
    fn finish(&self, end: WatchEnd) {
        let mut data = self.data.lock().expect("event stream lock");
        if data.end.is_some() {
            return;
        }
        data.end = Some(end.clone());
        data.pending.clear();
        self.ended.send_replace(Some(end));
        self.notify.notify_waiters();
        drop(data);
        if let Some(attachment) = self
            .attachment
            .lock()
            .expect("event attachment lock")
            .take()
        {
            attachment.stop();
        }
    }
    fn advance(&self, seq: Seq, current: Arc<ConversationView>, mut events: Vec<AgentEvent>) {
        let mut data = self.data.lock().expect("event stream lock");
        if data.end.is_some() {
            return;
        }
        data.current = current;
        if events.is_empty() {
            return;
        }
        if data.pending.len() >= 100 {
            data.pending.clear();
            events = vec![snapshot(&data.current)];
        }
        data.pending.push_back(AgentEventBatch { seq, events });
        self.notify.notify_one();
    }
    async fn next_raw(&self) -> Option<AgentEventBatch> {
        loop {
            let notified = self.notify.notified();
            {
                let mut data = self.data.lock().expect("event stream lock");
                if data.end.is_some() {
                    return None;
                }
                if let Some(batch) = data.pending.pop_front() {
                    return Some(batch);
                }
            }
            notified.await;
        }
    }
}
// Only public handles retain custody. Translation/listener/monitor tasks retain
// EventInner, and last public drop releases the attachment during a running callback.
struct EventCustody {
    inner: Arc<EventInner>,
}
impl Drop for EventCustody {
    fn drop(&mut self) {
        self.inner.finish(WatchEnd::Stopped);
    }
}
#[derive(Clone)]
pub struct AgentEventStream {
    pub snapshot: AgentEvent,
    custody: Arc<EventCustody>,
}
impl AgentEventStream {
    pub fn end(&self) -> Option<WatchEnd> {
        self.custody
            .inner
            .data
            .lock()
            .expect("event stream lock")
            .end
            .clone()
    }
    pub fn stop(&self) -> WatchEnd {
        self.custody.inner.finish(WatchEnd::Stopped);
        self.end().expect("stopped")
    }
    pub fn cancel(&self) -> WatchEnd {
        if let Some(attachment) = self
            .custody
            .inner
            .attachment
            .lock()
            .expect("event attachment lock")
            .as_ref()
        {
            attachment.cancel();
        }
        self.custody.inner.finish(WatchEnd::Cancelled);
        self.end().expect("cancelled")
    }
    pub async fn closed(&self) -> WatchEnd {
        let mut receive = self.custody.inner.ended.subscribe();
        loop {
            if let Some(end) = receive.borrow_and_update().clone() {
                return end;
            }
            if receive.changed().await.is_err() {
                return WatchEnd::Stopped;
            }
        }
    }
    pub async fn next(&self) -> Result<Option<AgentEventBatch>> {
        {
            let mut data = self.custody.inner.data.lock().expect("event stream lock");
            if data.mode == 2 {
                return Err(Error::validation("event stream already has a listener"));
            }
            data.mode = 1;
        }
        let _delivery = self.custody.inner.delivery.lock().await;
        Ok(self.custody.inner.next_raw().await)
    }
    pub fn start<F, Fut>(&self, mut listener: F) -> Result<()>
    where
        F: FnMut(AgentEventBatch) -> Fut + Send + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        {
            let mut data = self.custody.inner.data.lock().expect("event stream lock");
            if data.mode != 0 || data.end.is_some() {
                return Err(Error::validation("event stream already started or ended"));
            }
            data.mode = 2;
        }
        let inner = self.custody.inner.clone();
        let worker_inner = inner.clone();
        let worker = tokio::spawn(async move {
            while let Some(batch) = worker_inner.next_raw().await {
                listener(batch).await?;
            }
            Ok::<_, Error>(())
        });
        tokio::spawn(async move {
            match worker.await {
                Ok(Ok(())) => (),
                Ok(Err(error)) => inner.finish(WatchEnd::ListenerError(error.to_string())),
                Err(error) => inner.finish(WatchEnd::ListenerError(error.to_string())),
            }
        });
        Ok(())
    }
}
pub(crate) async fn attach(host: &RuntimeInner, c: ConversationId) -> Result<AgentEventStream> {
    let attachment = host.views.attach_commits(c).await?;
    let initial = attachment.baseline.value.clone();
    let mut held = attachment
        .baseline
        .tasks
        .values()
        .filter(|t| t.kind == "pi.generation" && matches!(t.state, TaskState::Completing { .. }))
        .map(|t| t.id)
        .collect::<BTreeSet<_>>();
    let inner = Arc::new(EventInner {
        data: Mutex::new(EventData {
            current: initial.clone(),
            pending: VecDeque::new(),
            end: None,
            mode: 0,
        }),
        notify: Notify::new(),
        ended: watch::channel(None).0,
        attachment: Mutex::new(Some(attachment.clone())),
        delivery: tokio::sync::Mutex::new(()),
    });
    let weak = Arc::downgrade(&inner);
    tokio::spawn(async move {
        while let Some(frame) = attachment.next().await {
            let Some(inner) = weak.upgrade() else {
                return;
            };
            let events = translate(
                c,
                &frame.before,
                &frame.after,
                &frame.ops,
                &frame.publication,
                &mut held,
            );
            inner.advance(frame.publication.seq, frame.after.clone(), events);
        }
        if let Some(inner) = weak.upgrade() {
            inner.finish(attachment.closed().await);
        }
    });
    Ok(AgentEventStream {
        snapshot: snapshot(&initial),
        custody: Arc::new(EventCustody { inner }),
    })
}
fn document<'a>(v: &'a ConversationView, kind: &str) -> &'a Value {
    v.docs.get(kind).unwrap_or(&Value::Null)
}
fn list(v: &Value) -> Vec<Value> {
    v.as_array().cloned().unwrap_or_default()
}
fn queued(inbox: &Value) -> Vec<Value> {
    list(&inbox["items"])
        .into_iter()
        .map(|x| json!({"id":x["id"],"mode":x["mode"]}))
        .collect()
}
fn snapshot(view: &ConversationView) -> AgentEvent {
    let live = document(view, "pi.live");
    let mut value = json!({"type":"snapshot","entries":view.entries,"tools":list(&live["tools"]),"nestedTools":list(&live["nestedTools"]),"compactions":list(&live["compactions"]),"inbox":queued(document(view,"pi.inbox")),"agent":view.docs.get("pi.agent").cloned().unwrap_or(json!({})),"usage":view.docs.get("pi.usage").cloned().unwrap_or(json!({"models":{},"tools":{}}))});

    if let Some(run) = live.get("run") {
        value["run"] = json!({"inputs":run["inputs"]});
    }
    for key in ["generation", "compactionFailure"] {
        if let Some(v) = live.get(key) {
            value[key] = v.clone();
        }
    }
    AgentEvent(value)
}
fn call(slot: &Value) -> Value {
    let mut value = json!({"toolCallId":slot["callId"],"toolName":slot["name"]});
    if let Some(v) = slot.get("taskId") {
        value["taskId"] = v.clone();
    }
    if let Some(v) = slot.get("parentCallId") {
        value["parentToolCallId"] = v.clone();
        value["parentTaskId"] = slot["parentTaskId"].clone();
    }
    value
}
fn tool_event(kind: &str, slot: &Value) -> Value {
    let mut v = call(slot);
    v["type"] = json!(kind);
    v
}
fn translate(
    c: ConversationId,
    before: &ConversationView,
    after: &ConversationView,
    ops: &[Op],
    publication: &CommitPublication,
    held: &mut BTreeSet<TaskId>,
) -> Vec<AgentEvent> {
    let mut entries = Vec::new();
    let mut tasks = Vec::new();
    let mut submissions = Vec::new();
    let mut nested_results = std::collections::BTreeMap::new();
    for change in &publication.changes {
        match change {
            CommitChange::Table(StorageWrite::Entry { value: v }) if v.conversation_id == c => {
                entries.push(v.clone())
            }
            CommitChange::Table(StorageWrite::Task { value: v }) if v.conversation_id == c => {
                tasks.push(v.clone())
            }
            CommitChange::Table(StorageWrite::Submission { value: v })
                if v.conversation_id == c =>
            {
                submissions.push(v.clone())
            }
            CommitChange::Document {
                record,
                conversation_id: Some(id),
                value,
                ..
            } if *id == c && record.identity.kind == "pi.tool.nested-call" => {
                if let (Some(task), Some(result)) = (
                    value.get("taskId").and_then(Value::as_u64),
                    value.get("result"),
                ) {
                    nested_results.insert(task, result.clone());
                }
            }
            _ => (),
        }
    }
    if ops.is_empty() && entries.is_empty() && tasks.is_empty() && submissions.is_empty() {
        return vec![];
    }
    submissions.sort_by_key(|s| s.id);
    let was = document(before, "pi.live");
    let now = document(after, "pi.live");
    let mut events = Vec::new();
    let old_slots = list(&was["tools"]);
    let slots = list(&now["tools"]);
    let old_nested = list(&was["nestedTools"]);
    let nested = list(&now["nestedTools"]);
    let mut updates = Vec::new();
    for (current, previous, nested_call) in
        [(&slots, &old_slots, false), (&nested, &old_nested, true)]
    {
        for (index, slot) in current.iter().enumerate() {
            let previous = previous.iter().find(|v| {
                v[if nested_call { "taskId" } else { "callId" }]
                    == slot[if nested_call { "taskId" } else { "callId" }]
            });
            if slot["status"] == "running" && previous.is_none_or(|v| v["status"] != "running") {
                let mut event = tool_event("tool_execution_start", slot);
                event["args"] = if nested_call {
                    slot["arguments"].clone()
                } else {
                    tasks
                        .iter()
                        .find(|t| json!(t.id) == slot["taskId"])
                        .and_then(|t| crate::definition::checkpoint(&t.state))
                        .and_then(|v| v.get("arguments"))
                        .cloned()
                        .unwrap_or(json!({}))
                };
                events.push(event);
            } else if slot["status"] == "running"
                && let Some(previous) = previous
                && let Some(update) = tool_update(
                    ops,
                    if nested_call { "nestedTools" } else { "tools" },
                    index,
                    slot,
                    previous,
                )
            {
                updates.push(update);
            }
        }
    }
    let partial = now["generation"].get("message");
    let old_partial = was["generation"].get("message");
    if let Some(message) = partial {
        if old_partial.is_none() {
            events.push(json!({"type":"message_start","message":message}));
        } else if partial != old_partial {
            events.push(json!({"type":"message_update","usage":message["usage"],"changes":message_changes(ops,message)}));
        }
    }
    events.extend(updates);
    if let Some(retry) = now["generation"].get("retry")
        && was["generation"].get("retry").is_none()
    {
        events.push(json!({"type":"auto_retry_start","attempt":now["generation"]["attempt"],"at":retry["at"],"errorMessage":retry["error"]}));
    }
    if was["generation"].get("retry").is_some() && now["generation"].get("retry").is_none() {
        events.push(json!({"type":"auto_retry_end","attempt":was["generation"]["attempt"]}));
    }
    if let Some(poll) = now["generation"]["deferred"].get("pollAt")
        && Some(poll) != was["generation"]["deferred"].get("pollAt")
    {
        events.push(json!({"type":"deferred_poll","pollAt":poll}));
    }
    let mut ends = Vec::new();
    for previous in &old_slots {
        if previous["status"] == "done" {
            continue;
        }
        let slot = slots.iter().find(|s| s["callId"] == previous["callId"]);
        if slot.is_none_or(|s| s["status"] == "done") {
            let selected = slot.unwrap_or(previous);
            let mut event = tool_event("tool_execution_end", selected);
            let entry = entries.iter().find(|e| {
                Some(json!(e.id)) == slot.and_then(|s| s.get("entry")).cloned()
                    || slot.is_none()
                        && e.model.as_ref().and_then(|m| m.first()).is_some_and(|m| {
                            m["role"] == "toolResult" && m["toolCallId"] == previous["callId"]
                        })
            });
            if let Some(entry) = entry {
                event["entry"] = json!(entry);
            }
            ends.push(event);
        }
    }
    for slot in &slots {
        if slot["status"] == "done" && !old_slots.iter().any(|s| s["callId"] == slot["callId"]) {
            let mut event = tool_event("tool_execution_end", slot);
            if let Some(entry) = entries.iter().find(|e| json!(e.id) == slot["entry"]) {
                event["entry"] = json!(entry);
            }
            ends.push(event);
        }
    }
    for previous in old_nested.iter().rev() {
        if previous["status"] == "done" {
            continue;
        }
        let slot = nested.iter().find(|s| s["taskId"] == previous["taskId"]);
        if slot.is_none_or(|s| s["status"] == "done") {
            let selected = slot.unwrap_or(previous);
            let mut event = tool_event("tool_execution_end", selected);
            if let Some(result) = selected["taskId"]
                .as_u64()
                .and_then(|id| nested_results.get(&id))
            {
                event["result"] = result.clone();
            }
            events.push(event);
        }
    }
    for slot in nested.iter().rev() {
        if slot["status"] == "done" && !old_nested.iter().any(|s| s["taskId"] == slot["taskId"]) {
            let mut event = tool_event("tool_execution_end", slot);
            if let Some(result) = slot["taskId"]
                .as_u64()
                .and_then(|id| nested_results.get(&id))
            {
                event["result"] = result.clone();
            }
            events.push(event);
        }
    }
    let mut assistant = false;
    for entry in entries {
        for end in &ends {
            if end.get("entry").is_some_and(|v| v["id"] == json!(entry.id)) {
                events.push(end.clone());
            }
        }
        let message = entry.model.as_ref().and_then(|m| m.first());
        if let Some(message) = message {
            let streamed = message["role"] == "assistant" && old_partial.is_some() && !assistant;
            if message["role"] == "assistant" {
                assistant = true;
            }
            if !streamed {
                events.push(json!({"type":"message_start","message":message}));
            }
            events.push(json!({"type":"message_end","entry":entry}));
        } else {
            events.push(json!({"type":"entry_appended","entry":entry}));
        }
    }
    events.extend(ends.into_iter().filter(|e| e.get("entry").is_none()));
    for status in list(&was["compactions"]) {
        if !list(&now["compactions"])
            .iter()
            .any(|s| s["taskId"] == status["taskId"])
        {
            let mut e = json!({"type":"compaction_end","taskId":status["taskId"],"reason":status["reason"]});
            if now["compactionFailure"]["taskId"] == status["taskId"] {
                e["error"] = now["compactionFailure"]["error"].clone();
            }
            events.push(e);
        }
    }
    let mut turn_ended = false;
    for task in &tasks {
        if task.kind == "pi.generation"
            && matches!(task.state, TaskState::Completing { .. })
            && held.insert(task.id)
        {
            turn_ended = true;
        }
        if let TaskState::Terminal { outcome } = &task.state {
            if task.kind == "pi.generation" && !held.remove(&task.id) {
                turn_ended = true;
            }
            let message = match outcome {
                TaskOutcome::Faulted { error, .. } => Some(&error.message),
                TaskOutcome::Orphaned { reason } => Some(reason),
                _ => None,
            };
            if let Some(message) = message {
                events.push(json!({"type":"task_failed","taskId":task.id,"kind":task.kind,"message":message}));
            }
        }
    }
    if turn_ended {
        events.push(json!({"type":"turn_end"}));
    }
    let run = now.get("run");
    let old_run = was.get("run");
    let changed = run.map(|r| &r["inputs"][0]) != old_run.map(|r| &r["inputs"][0]);
    if changed && let Some(run) = old_run {
        events.push(json!({"type":"run_end","inputs":run["inputs"]}));
    }
    events.extend(
        submissions
            .into_iter()
            .map(|record| json!({"type":"submission","record":record})),
    );
    if document(after, "pi.inbox") != document(before, "pi.inbox") {
        events.push(json!({"type":"inbox_update","items":queued(document(after,"pi.inbox"))}));
    }
    for (kind, event, field, initial) in [
        ("pi.agent", "agent_changed", "agent", json!({})),
        (
            "pi.usage",
            "usage_changed",
            "usage",
            json!({"models":{},"tools":{}}),
        ),
    ] {
        if document(after, kind) != document(before, kind) {
            let mut e = json!({"type":event});
            e[field] = after.docs.get(kind).cloned().unwrap_or(initial);
            events.push(e);
        }
    }
    for status in list(&now["compactions"]) {
        if !list(&was["compactions"])
            .iter()
            .any(|s| s["taskId"] == status["taskId"])
        {
            events.push(json!({"type":"compaction_start","taskId":status["taskId"],"reason":status["reason"],"blocking":status["blocking"]}));
        }
    }
    if let Some(run) = run {
        if changed {
            events.push(json!({"type":"run_start","inputs":run["inputs"]}));
        }
        if old_run.map(|r| &r["taskId"]) != Some(&run["taskId"])
            && tasks
                .iter()
                .any(|t| json!(t.id) == run["taskId"] && t.kind == "pi.generation")
        {
            events.push(json!({"type":"turn_start"}));
        }
    }
    events.into_iter().map(AgentEvent).collect()
}
fn path(op: &Op) -> Option<&Path> {
    match op {
        Op::Replace(_) => None,
        Op::Set(p, _)
        | Op::Delete(p)
        | Op::Append(p, _)
        | Op::Trim(p, _)
        | Op::Splice(p, ..)
        | Op::Permute(p, _) => Some(p),
    }
}
fn message_changes(ops: &[Op], message: &Value) -> Vec<Value> {
    let prefix: Path = vec![
        "docs".into(),
        "pi.live".into(),
        "generation".into(),
        "message".into(),
    ];
    let mut result = Vec::new();
    let mut whole = BTreeSet::new();
    for op in ops {
        let Some(path) = path(op) else {
            return vec![json!({"type":"message","message":message})];
        };
        if !path.starts_with(&prefix) {
            if prefix.starts_with(path) {
                return vec![json!({"type":"message","message":message})];
            }
            continue;
        }
        let rest = &path[prefix.len()..];
        if rest.first() == Some(&"usage".into()) {
            continue;
        }
        if rest.first() != Some(&"content".into()) {
            return vec![json!({"type":"message","message":message})];
        }
        if rest.len() == 1 {
            if let Op::Splice(_, index, 0, blocks) = op {
                for (offset, block) in blocks.iter().enumerate() {
                    let kind = match block["type"].as_str() {
                        Some("text") => "text_start",
                        Some("thinking") => "thinking_start",
                        _ => "toolcall_start",
                    };
                    result.push(json!({"type":kind,"contentIndex":index+offset,"block":block}));
                }
                continue;
            }
            return vec![json!({"type":"message","message":message})];
        }
        let index = match &rest[1] {
            Segment::Index(i) => *i,
            _ => return vec![json!({"type":"message","message":message})],
        };
        if whole.contains(&index) {
            continue;
        }
        let field = rest.get(2);
        if let Op::Append(_, delta) = op {
            if rest.len() == 3
                && (field == Some(&"text".into()) || field == Some(&"thinking".into()))
            {
                result.push(json!({"type":if field==Some(&"text".into()){"text_delta"}else{"thinking_delta"},"contentIndex":index,"delta":delta}));
                continue;
            }
            if field == Some(&"arguments".into()) {
                result.push(json!({"type":"toolcall_delta","contentIndex":index,"path":&rest[3..],"delta":delta}));
                continue;
            }
        }
        whole.insert(index);
        result.push(json!({"type":"block","contentIndex":index,"block":message["content"][index]}));
    }
    result
}
fn tool_update(
    ops: &[Op],
    kind: &str,
    index: usize,
    slot: &Value,
    previous: &Value,
) -> Option<Value> {
    let prefix: Path = vec![
        "docs".into(),
        "pi.live".into(),
        kind.into(),
        index.into(),
        "output".into(),
    ];
    let (mut trim, mut append, mut set) = (0, String::new(), false);
    for op in ops {
        if path(op).is_some_and(|p| p.starts_with(&prefix)) {
            match op {
                Op::Trim(_, n) => trim += n,
                Op::Append(_, s) => append.push_str(s),
                _ => set = true,
            }
        }
    }
    let mut e = tool_event("tool_execution_update", slot);
    if set || (slot.get("output") != previous.get("output") && trim == 0 && append.is_empty()) {
        e["output"] = json!({"set":slot["output"].as_str().unwrap_or("")});
    } else if trim > 0 || !append.is_empty() {
        e["output"] = json!({});
        if trim > 0 {
            e["output"]["trimStart"] = json!(trim)
        }
        if !append.is_empty() {
            e["output"]["append"] = json!(append)
        }
    }
    if slot.get("details") != previous.get("details") {
        e["details"] = slot.get("details").cloned().unwrap_or(Value::Null);
    }
    if slot.get("diagnostics") != previous.get("diagnostics") {
        e["diagnostics"] = slot.get("diagnostics").cloned().unwrap_or(json!([]));
    }
    let base = call(slot).as_object().unwrap().len() + 1;
    (e.as_object().unwrap().len() > base).then_some(e)
}
