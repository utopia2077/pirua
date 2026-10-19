use super::*;
use crate::{session::*, *};
use pi_ai::{InputContent, JsonObject, JsonValue, Presence};
use serde_json::{Value, json};
use std::sync::{
    Arc, Mutex, Weak,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
struct ProgressTimer {
    id: u64,
    handle: Option<Timer>,
}
struct Reported {
    output: super::output::OutputBuffer,
    details: Presence<JsonValue>,
    diagnostics: Vec<ToolDiagnostic>,
    generation: u64,
    settled: u64,
    error: Option<Error>,
    next_at: f64,
    timer: Option<ProgressTimer>,
    timer_sequence: u64,
    scheduling: bool,
    in_flight: bool,
    stopped: bool,
    written_text: String,
    written_details: Presence<JsonValue>,
    written_diagnostics: usize,
}
struct ApiInner {
    phase: Phase,
    call: ToolHookCall,
    tool: ToolRegistration,
    env: Option<pi_coding_runtime::tools::ExecutionToolContext>,
    enabled: bool,
    reported: Mutex<Reported>,
    changed: tokio::sync::Notify,
    ended: AtomicBool,
    sequence: AtomicU64,
    resumes: bool,
    admissions: AtomicU64,
}
/// A process-local lease. Clones cannot admit effects or progress after execute settles.
#[derive(Clone)]
pub struct ToolExecutionApi {
    inner: Arc<ApiInner>,
}
impl ToolExecutionApi {
    pub fn binding_id(&self) -> u64 {
        self.inner.phase.host.binding_id
    }
    pub fn identity(&self) -> ToolInvocationIdentity {
        ToolInvocationIdentity {
            invocation: self.inner.phase.invocation.identity(),
            conversation_id: self.inner.phase.conversation,
            call_id: self.inner.call.call.id.to_string(),
            parent: self.inner.call.parent.clone(),
        }
    }
    pub fn check_live(&self) -> Result<()> {
        if self.inner.ended.load(Ordering::Acquire) {
            return Err(Error::request("Tool call has settled"));
        }
        self.inner.phase.check()
    }
    pub fn signal(&self) -> pi_ai::CancellationToken {
        self.inner.phase.cancellation.clone()
    }
    pub fn registry(&self) -> RuntimeRegistrySnapshot {
        self.inner.phase.snapshot.clone()
    }
    pub fn models(&self) -> Result<Arc<pi_ai::ModelsRegistry>> {
        self.check_live()?;
        self.inner.phase.models()
    }
    pub async fn agent(&self) -> Result<Agent> {
        self.check_live()?;
        self.inner.phase.agent().await
    }
    pub fn env(&self) -> Result<Option<pi_coding_runtime::tools::ExecutionToolContext>> {
        self.check_live()?;
        Ok(self.inner.env.clone())
    }
    pub fn now(&self) -> Result<f64> {
        self.check_live()?;
        self.inner.phase.now()
    }
    pub fn progress_options(&self) -> ToolProgressOptions {
        let l = self.inner.tool.output_limits.clone();
        ToolProgressOptions {
            enabled: self.inner.enabled,
            output_limits: l.clone(),
            output_window: (l.retain == OutputRetention::Tail).then(|| OutputWindow {
                max_bytes: l.max_bytes,
                max_lines: l.max_lines,
                min_interval_ms: self.inner.phase.settings().output_interval_ms,
                bytes_per_second: 100 * 1024,
            }),
        }
    }
    fn update(&self, op: ToolProgress) -> Result<u64> {
        self.check_live()?;
        let mut r = self.inner.reported.lock().unwrap();
        if self.inner.ended.load(Ordering::Acquire) {
            return Err(Error::request("Tool call has settled"));
        }
        if let Some(e) = &r.error {
            return Err(e.clone());
        }
        match op {
            ToolProgress::Output { chunk, skipped } => {
                if !r.output.push(chunk, skipped)? {
                    return Ok(r.generation);
                }
            }
            ToolProgress::Details { value } => r.details = Presence::Value(value),
            ToolProgress::Diagnostic { diagnostic } => r.diagnostics.push(diagnostic),
        }
        r.generation += 1;
        Ok(r.generation)
    }
    fn schedule(&self) {
        if !self.inner.enabled {
            return;
        }
        {
            let mut r = self.inner.reported.lock().unwrap();
            if r.stopped
                || r.scheduling
                || r.in_flight
                || r.timer.is_some()
                || r.error.is_some()
                || r.generation == r.settled
            {
                return;
            }
            // Reserve before calling the host: a reentrant update can join this
            // capture, but cannot start a competing schedule or timer arm.
            r.scheduling = true;
        }
        let now = self.inner.phase.now();
        let mut r = self.inner.reported.lock().unwrap();
        let Ok(now) = now else {
            r.scheduling = false;
            drop(r);
            self.inner.changed.notify_waiters();
            return;
        };
        if r.stopped
            || self.inner.ended.load(Ordering::Acquire)
            || r.error.is_some()
            || r.generation == r.settled
        {
            r.scheduling = false;
            drop(r);
            self.inner.changed.notify_waiters();
            return;
        }
        let wait = r.next_at - now;
        if wait > 0.0 {
            r.timer_sequence = r.timer_sequence.wrapping_add(1);
            let id = r.timer_sequence;
            r.timer = Some(ProgressTimer { id, handle: None });
            drop(r);
            let weak = Arc::downgrade(&self.inner);
            let armed = self.inner.phase.invocation.set_timer(
                move || {
                    if let Some(inner) = weak.upgrade() {
                        let fired = {
                            let mut r = inner.reported.lock().unwrap();
                            if r.timer.as_ref().is_some_and(|timer| timer.id == id) {
                                r.timer.take()
                            } else {
                                None
                            }
                        };
                        if fired.is_some() {
                            drop(fired);
                            Self { inner }.schedule();
                        }
                    }
                },
                wait,
            );
            let (cancel, retired) = {
                let mut r = self.inner.reported.lock().unwrap();
                match armed {
                    Ok(timer) => {
                        if let Some(reservation) = r.timer.as_mut().filter(|timer| timer.id == id) {
                            reservation.handle = Some(timer);
                            (None, None)
                        } else {
                            // Stop or an early callback won while the host was arming.
                            (Some(timer), None)
                        }
                    }
                    Err(error) => {
                        let retired = if r.timer.as_ref().is_some_and(|timer| timer.id == id) {
                            r.timer.take()
                        } else {
                            None
                        };
                        r.error = Some(error);
                        (None, retired)
                    }
                }
            };
            drop(retired);
            if let Some(timer) = cancel {
                timer.cancel();
            }
            let reschedule = {
                let mut r = self.inner.reported.lock().unwrap();
                r.scheduling = false;
                !r.stopped && r.timer.is_none() && r.error.is_none()
            };
            self.inner.changed.notify_waiters();
            if reschedule {
                // A host may fire synchronously before returning its handle.
                // Defer rearming so repeated early wakes cannot recurse here.
                let api = self.clone();
                tokio::spawn(async move { api.schedule() });
            }
            return;
        }
        // Capture synchronously: a following details update belongs to a later commit.
        let snapshot = r.output.snapshot();
        let details = r.details.clone();
        let diagnostics = r.diagnostics.clone();
        let target = r.generation;
        let shared = if snapshot.text.starts_with(&r.written_text) {
            r.written_text.len()
        } else {
            0
        };
        let mut bytes = snapshot.text.len() - shared;
        if details != r.written_details {
            bytes += serde_json::to_vec(&details).map_or(0, |v| v.len());
        }
        bytes += serde_json::to_vec(&diagnostics[r.written_diagnostics..]).map_or(0, |v| v.len());
        r.in_flight = true;
        r.scheduling = false;
        drop(r);
        let api = self.clone();
        tokio::spawn(async move {
            let c = api.inner.phase.conversation;
            let task = api.inner.phase.invocation.identity().task_id;
            let written_text = snapshot.text.clone();
            let written_details = details.clone();
            let written_diagnostics = diagnostics.len();
            let result = api
                .inner
                .phase
                .invocation
                .commit(move |tx, _| {
                    let (id, mut live) = doc(tx, &live_doc(), c)?;
                    if let Some(slot) = slot_mut(&mut live, task) {
                        if !snapshot.text.is_empty() || slot.get("output").is_some() {
                            slot["output"] = json!(snapshot.text);
                        }
                        if snapshot.dropped_bytes > 0 {
                            slot["droppedBytes"] = json!(snapshot.dropped_bytes);
                        }
                        if snapshot.dropped_lines > 0 {
                            slot["droppedLines"] = json!(snapshot.dropped_lines);
                        }
                        match details {
                            Presence::Missing => (),
                            Presence::Null => slot["details"] = Value::Null,
                            Presence::Value(v) => slot["details"] = ai_value(&v)?,
                        }
                        if !diagnostics.is_empty() {
                            slot["diagnostics"] = ai_value(&diagnostics)?;
                        }
                        save(tx, id, live)?;
                    }
                    Ok(None)
                })
                .await;
            // Keep the write reserved while current host settings are read.
            let interval = api.inner.phase.settings().output_interval_ms;
            {
                let mut r = api.inner.reported.lock().unwrap();
                r.in_flight = false;
                match result {
                    Ok(()) => {
                        r.settled = r.settled.max(target);
                        r.next_at = now + interval.max(bytes as f64 * 1000.0 / (100.0 * 1024.0));
                        r.written_text = written_text;
                        r.written_details = written_details;
                        r.written_diagnostics = written_diagnostics;
                    }
                    Err(e) => {
                        r.error = Some(e);
                        r.next_at = now + interval;
                    }
                }
            }
            api.inner.changed.notify_waiters();
            api.schedule();
        });
    }
    async fn wait_progress(&self, target: u64) -> Result<()> {
        if !self.inner.enabled {
            return Ok(());
        }
        loop {
            let changed = self.inner.changed.notified();
            {
                let r = self.inner.reported.lock().unwrap();
                if let Some(e) = &r.error {
                    return Err(e.clone());
                }
                if r.settled >= target {
                    return Ok(());
                }
            }
            tokio::select! {
            _ = changed => (), _ = self.inner.phase.cancellation.cancelled() => return Err(Error::request("invocation cancelled")) }
        }
    }
    async fn stop_progress(&self) {
        let timer = {
            let mut r = self.inner.reported.lock().unwrap();
            r.stopped = true;
            r.timer.take()
        };
        if let Some(timer) = timer.and_then(|timer| timer.handle) {
            timer.cancel();
        }
        loop {
            let changed = self.inner.changed.notified();
            {
                let r = self.inner.reported.lock().unwrap();
                if !r.in_flight && !r.scheduling {
                    return;
                }
            }
            changed.await;
        }
    }
    pub fn output(&self, chunk: OutputChunk, skipped: Option<OutputSkip>) -> Result<()> {
        self.update(ToolProgress::Output { chunk, skipped })?;
        self.schedule();
        Ok(())
    }
    pub fn diagnostic(&self, diagnostic: ToolDiagnostic) -> Result<()> {
        self.update(ToolProgress::Diagnostic { diagnostic })?;
        self.schedule();
        Ok(())
    }
    pub async fn details(&self, value: JsonValue) -> Result<()> {
        let target = self.update(ToolProgress::Details { value })?;
        self.schedule();
        self.wait_progress(target).await
    }
    /// Success acknowledges a covering Session commit, never merely queued progress.
    pub async fn report_progress(&self, operation: ToolProgress) -> Result<()> {
        if !self.inner.enabled {
            return Err(Error::request("progress is disabled for this nested call"));
        }
        let target = self.update(operation)?;
        self.schedule();
        self.wait_progress(target).await
    }
    pub fn retained_output(&self) -> Result<RetainedOutput> {
        self.check_live()?;
        let r = self.inner.reported.lock().unwrap();
        let s = r.output.snapshot();
        Ok(RetainedOutput {
            text: s.text,
            truncated: s.dropped_bytes > 0,
        })
    }
    pub async fn commit<T: Send + 'static>(
        &self,
        change: impl FnOnce(&mut Transaction<'_>) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        self.check_live()?;
        let inner = self.inner.clone();
        self.inner
            .phase
            .invocation
            .transact(move |tx, _| {
                if inner.ended.load(Ordering::Acquire) {
                    return Err(Error::request("Tool call has settled"));
                }
                Ok((change(tx)?, None))
            })
            .await
    }
    pub async fn memo(&self, name: &str) -> Result<Option<Value>> {
        self.check_live()?;
        self.inner.phase.invocation.memo(name).await
    }
    pub async fn memo_or_insert(&self, name: String, candidate: Value) -> Result<Value> {
        self.check_live()?;
        self.inner
            .phase
            .invocation
            .memo_or_insert(name, candidate)
            .await
    }
    pub async fn snapshot(
        &self,
        def: DocDefinition,
        address: DocumentAddress,
    ) -> Result<Option<Value>> {
        self.check_live()?;
        self.inner.phase.host.session.snapshot(def, address).await
    }
    pub async fn snapshot_as_of(
        &self,
        def: DocDefinition,
        address: DocumentAddress,
        at: EntryId,
    ) -> Result<Option<Value>> {
        self.check_live()?;
        self.inner
            .phase
            .host
            .session
            .snapshot_as_of(def, address, at)
            .await
    }
    pub async fn watch_doc(
        &self,
        def: DocDefinition,
        address: DocumentAddress,
    ) -> Result<Option<DocumentWatch>> {
        self.check_live()?;
        self.inner.phase.host.session.watch_doc(def, address).await
    }
    pub async fn create_task(
        &self,
        definition: TaskDefinition,
        input: Value,
        mut options: TaskOptions,
    ) -> Result<TaskId> {
        self.check_live()?;
        options.conversation_id = Some(self.inner.phase.conversation);
        if options.owner.is_some() && !self.inner.resumes {
            options.abandon_on_restart = true;
        }
        self.commit(move |tx| crate::create_task(tx, &definition, input, options))
            .await
    }
    pub async fn get_task(&self, id: TaskId) -> Result<Option<TaskRecord>> {
        self.check_live()?;
        self.inner.phase.invocation.get_task(id).await
    }
    pub async fn wait_for_task(&self, id: TaskId) -> Result<TaskRecord> {
        self.check_live()?;
        self.inner.phase.invocation.wait_for_task(id).await
    }
    pub async fn conversation(&self, id: ConversationId) -> Result<Option<BoundConversation>> {
        self.check_live()?;
        Ok(self
            .inner
            .phase
            .host
            .session
            .conversation(id)
            .await?
            .map(|_| BoundConversation {
                api: self.clone(),
                id,
            }))
    }
    fn admit_nested(&self) -> Result<NestedAdmission> {
        self.check_live()?;
        let _reported = self.inner.reported.lock().unwrap();
        if self.inner.ended.load(Ordering::Acquire) {
            return Err(Error::request("Tool call has settled"));
        }
        self.inner.admissions.fetch_add(1, Ordering::AcqRel);
        Ok(NestedAdmission(self.inner.clone()))
    }
    pub async fn execute_tool(
        &self,
        name: String,
        args: JsonObject,
        options: NestedToolOptions,
    ) -> Result<NestedToolExecutionResult> {
        let admission = self.admit_nested()?;
        let key = if let Some(key) = options.key {
            if key.is_empty()
                || key.contains('/')
                || key == "__proto__"
                || key.bytes().all(|b| b.is_ascii_digit())
                    && key.starts_with(|c: char| c.is_ascii_digit() && c != '0')
            {
                return Err(Error::request("invalid nested call key"));
            }
            key
        } else {
            (self.inner.sequence.fetch_add(1, Ordering::SeqCst) + 1).to_string()
        };
        self.inner
            .phase
            .invocation
            .memo_or_insert("pi.tool.madeNestedCalls", json!(true))
            .await?;
        let phase = self.inner.phase.clone();
        let parent = phase.invocation.identity().task_id;
        let parent_call = self.inner.call.call.id.to_string();
        let family = DocumentAddress {
            kind: "pi.tool.nested-call".into(),
            scope: Scope::Task { task_id: parent },
            key: Some(key.clone()),
        };
        let call = pi_ai::ToolCall {
            content_type: Default::default(),
            id: pi_ai::ToolCallId::new(format!("{parent_call}/{key}"))
                .map_err(|e| Error::request(e.to_string()))?,
            name: pi_ai::ToolName::new(name.clone()).map_err(|e| Error::request(e.to_string()))?,
            arguments: args,
            arguments_error: None,
            thought_signature: None,
            namespace: None,
        };
        let expected_call = call.clone();
        let family_for_commit = family.clone();
        let resumes = self.inner.resumes;
        let id = self.inner.phase.invocation.transact(move |tx, _| {
            let document = tx.doc(&nested_call_doc(), family_for_commit, Value::Null)?;
            let state = tx.document_value(document)?.clone();
            if let Some(id) = state.get("taskId") {
                let id = decode(id.clone())?;
                let old = tx.task(id)?.ok_or_else(||Error::validation("nested task missing"))?;
                if old.input["call"]["name"] != json!(name) || old.input["call"]["arguments"] != ai_value(&expected_call.arguments)? {
                    return Err(Error::request(format!("Nested call {} was already made with another tool or other arguments", expected_call.id)));
                }
                return Ok((id, None));
            }
            let mut input = json!({"kind":"nested","parent":parent,"parentCallId":parent_call,"key":key,"call":expected_call});
            if options.progress == Some(false) { input["progress"] = json!(false); }
            let def = definition(Arc::downgrade(&phase.host), phase.snapshot.clone())?;
            let id = crate::create_task(tx, &def, input, TaskOptions { owner:Some(parent), abandon_on_restart:!resumes, ..Default::default() })?;
            set(tx, document, vec!["taskId".into()], json!(id))?;
            let (live_id, mut live) = doc(tx, &live_doc(), phase.conversation)?;
            let slots = live.as_object_mut().unwrap().entry("nestedTools").or_insert(json!([])).as_array_mut().unwrap();
            slots.push(json!({"callId":expected_call.id,"parentCallId":parent_call,"parentTaskId":parent,"name":name,"taskId":id,"arguments":expected_call.arguments,"status":"pending"}));
            save(tx, live_id, live)?;
            Ok((id, None))
        }).await?;
        drop(admission);
        let record = self.wait_for_task(id).await?;
        self.check_live()?;
        let stored = self
            .inner
            .phase
            .host
            .session
            .snapshot(nested_call_doc(), family)
            .await?;
        if let Some(result) = stored.and_then(|v| v.get("result").cloned()) {
            return decode(result);
        }
        let message = match record.state {
            TaskState::Terminal {
                outcome: TaskOutcome::Faulted { error, .. },
            } => format!("Tool {} failed: {}", call.name, error.message),
            TaskState::Terminal {
                outcome: TaskOutcome::Orphaned { reason },
            } => format!("Tool {} could not resume: {reason}", call.name),
            _ => format!("Tool {} ended without a result", call.name),
        };
        Ok(NestedToolExecutionResult {
            task_id: id,
            structured_output: Presence::Missing,
            is_error: true,
            details: Presence::Missing,
            diagnostics: vec![diagnostic("faulted", message)],
            usage: None,
            duration_ms: None,
        })
    }
}
struct NestedAdmission(Arc<ApiInner>);
impl Drop for NestedAdmission {
    fn drop(&mut self) {
        self.0.admissions.fetch_sub(1, Ordering::AcqRel);
        self.0.changed.notify_waiters();
    }
}
#[derive(Clone)]
pub struct BoundConversation {
    api: ToolExecutionApi,
    pub id: ConversationId,
}
impl BoundConversation {
    pub async fn submit(&self, draft: SubmissionDraft) -> Result<Submission> {
        self.api.check_live()?;
        if matches!(draft, SubmissionDraft::Write { .. }) {
            return Err(Error::request("bound conversations admit input only"));
        }
        let c = self.id;
        let host = self.api.inner.phase.host.clone();
        let snapshot = self.api.inner.phase.snapshot.clone();
        host.scheduler().resume();
        let runtime = host.clone();
        let id = self
            .api
            .commit(move |tx| super::inbox::admit(tx, c, draft, &runtime, &snapshot))
            .await?;
        Ok(Submission {
            id,
            host: Durable { inner: host },
            guard: Some(self.api.clone()),
        })
    }
    pub async fn abort(&self, background: bool) -> Result<()> {
        self.api.check_live()?;
        let c = self.id;
        let reached = self
            .api
            .commit(move |tx| {
                let graph = crate::graph::Graph::read(tx)?;
                let mut reached = Vec::new();
                for task in graph.tasks.values().filter(|task| {
                    !task.state.is_terminal() && graph.in_scope(task, Some(c), background)
                }) {
                    let mut marked = task.clone();
                    crate::scheduler::mark(&mut marked, None);
                    tx.replace_task(marked)?;
                    reached.push(task.id);
                }
                for id in graph.conversations.keys() {
                    if graph.conversation_in_scope(*id, c, background) {
                        super::inbox::withdraw(tx, *id, "aborted")?;
                    }
                }
                Ok(reached)
            })
            .await?;
        let scheduler = self.api.inner.phase.host.scheduler();
        scheduler.inner.signal_marks().await?;
        scheduler.resume();
        let signal = self.api.signal();
        tokio::select! {
                   result = async {
        if background {
        for id in reached {
        scheduler.wait_for_task(id).await?;
        }
        }
        scheduler.wait_for_idle(Some(c)).await }
        => result,
                   _ = signal.cancelled() => Err(Error::request("invocation cancelled"))
               }
    }
    pub async fn wait_for_idle(&self) -> Result<()> {
        self.api.check_live()?;
        let signal = self.api.signal();
        let scheduler = self.api.inner.phase.host.scheduler();
        scheduler.resume();
        tokio::select! {
        result = scheduler.wait_for_idle(Some(self.id)) => result, _ = signal.cancelled() => Err(Error::request("invocation cancelled")) }
    }
}
pub(crate) fn definition(
    owner: Weak<RuntimeInner>,
    snapshot: RuntimeRegistrySnapshot,
) -> Result<TaskDefinition> {
    let o = owner.clone();
    let s = snapshot.clone();
    let d = TaskDefinition::new("pi.tool", 2, |_| {
        Ok(json!({
        "phase":"call"}
        ))
    })?
    .phase("call", move |t, i| {
        let o = o.clone();
        let s = s.clone();
        async move {
            let p = Phase::new(&o, i, s, t.conversation_id)?;
            call(t, p, false).await
        }
    });
    let o = owner.clone();
    let s = snapshot.clone();
    Ok(d.phase("execute", move |t, i| {
        let o = o.clone();
        let s = s.clone();
        async move {
            let p = Phase::new(&o, i, s, t.conversation_id)?;
            call(t, p, true).await
        }
    })
    .abort(move |t, i| {
        let o = owner.clone();
        let s = snapshot.clone();
        async move {
            let p = Phase::new(&o, i, s, t.conversation_id)?;
            let call = read_call(&p, &t.input).await?;
            let executing =
                crate::definition::checkpoint(&t.state).is_some_and(|v| v["phase"] == "execute");
            let (code, message) = if t.abort_reason == Some(AbortReason::Restart) {
                if executing {
                    (
                        "interrupted",
                        format!(
                            "Tool {} was interrupted by a restart and may have partially run",
                            call.call.name
                        ),
                    )
                } else {
                    (
                        "abandoned",
                        format!(
                            "Tool {} was not started: its caller ended with a restart",
                            call.call.name
                        ),
                    )
                }
            } else {
                ("aborted", format!("Tool {} was aborted", call.call.name))
            };
            settle(
                &p,
                t.input,
                call,
                None,
                Some((code.into(), message)),
                None,
                "aborted",
            )
            .await
        }
    }))
}
async fn read_call(p: &Phase, input: &Value) -> Result<ToolHookCall> {
    if input["kind"] == "nested" {
        return Ok(ToolHookCall {
            call: decode(input["call"].clone())?,
            parent: Some(ToolParent {
                task_id: decode(input["parent"].clone())?,
                call_id: input["parentCallId"].as_str().unwrap().into(),
            }),
        });
    }
    let id = decode(input["assistant"].clone())?;
    let entry = p
        .host
        .session
        .entry(id)
        .await?
        .ok_or_else(|| Error::request("assistant entry missing"))?;
    let message: pi_ai::AssistantMessage = decode(
        entry
            .entry
            .model
            .unwrap_or_default()
            .into_iter()
            .next()
            .ok_or_else(|| Error::request("assistant message missing"))?,
    )?;
    message
        .content
        .into_iter()
        .find_map(|c| {
            if let pi_ai::AssistantContent::ToolCall(c) = c {
                (json!(c.id) == input["callId"]).then_some(ToolHookCall {
                    call: c,
                    parent: None,
                })
            } else {
                None
            }
        })
        .ok_or_else(|| Error::request("assistant has no matching tool call"))
}
async fn call(task: TaskRecord, p: Phase, recovery: bool) -> Result<()> {
    let call = read_call(&p, &task.input).await?;
    let agent = p.agent().await?;
    let tool = if task.input["kind"] == "nested" {
        agent.callable
    } else {
        agent.tools
    }
    .into_iter()
    .find(|t| t.name() == call.call.name.as_str());
    let checkpoint = crate::definition::checkpoint(&task.state).unwrap().clone();
    if recovery
        && (checkpoint["replay"] != "safe"
            || tool.as_ref().is_none_or(|t| t.replay != ReplayPolicy::Safe))
    {
        return settle(
            &p,
            task.input,
            call.clone(),
            None,
            Some((
                "interrupted".into(),
                format!(
                    "Tool {} was interrupted and may have partially run",
                    call.call.name
                ),
            )),
            None,
            "failed",
        )
        .await;
    }
    let Some(tool) = tool else {
        return settle(
            &p,
            task.input,
            call.clone(),
            Some(error_result(
                "tool_unavailable",
                format!("Tool {} is not available", call.call.name),
            )),
            None,
            None,
            "completed",
        )
        .await;
    };
    let mut args = if recovery {
        decode(checkpoint["arguments"].clone())?
    } else {
        let args = if let Some(prepare) = &tool.prepare_arguments {
            prepare(call.call.arguments.clone())
        } else {
            Ok(call.call.arguments.clone())
        };
        let args = match args.and_then(|a| validate(&tool, &call.call, a)) {
            Ok(a) => a,
            Err(e) => {
                return settle(
                    &p,
                    task.input,
                    call,
                    Some(error_result("invalid_arguments", e.message)),
                    None,
                    None,
                    "completed",
                )
                .await;
            }
        };
        let mut args = args;
        let mut block = None;
        for h in p.hooks().await? {
            if block.is_some() {
                break;
            }
            if let Some(h) = h.before_tool {
                let mut view = call.clone();
                view.call.arguments = args.clone();
                match h(view, p.api()).await {
                    Ok(Some(d)) => {
                        if let Some(reason) = d.block {
                            block = Some(reason)
                        } else if let Some(a) = d.arguments {
                            args = a
                        }
                    }
                    Ok(None) => (),
                    Err(e) => {
                        p.check()?;
                        block = Some(e.message)
                    }
                }
            }
        }
        if let Some(block) = block {
            return settle(
                &p,
                task.input,
                call,
                Some(error_result(
                    "blocked",
                    format!("Tool call blocked: {block}"),
                )),
                None,
                None,
                "completed",
            )
            .await;
        }
        match validate(&tool, &call.call, args) {
            Ok(a) => a,
            Err(e) => {
                return settle(
                    &p,
                    task.input,
                    call,
                    Some(error_result("invalid_arguments", e.message)),
                    None,
                    None,
                    "completed",
                )
                .await;
            }
        }
    };
    let c = p.conversation;
    let id = task.id;
    let intent_args = ai_value(&args)?;
    let replay = tool.replay;
    p.invocation
        .commit(move |tx, _| {
            let (d, mut live) = doc(tx, &live_doc(), c)?;
            if let Some(slot) = slot_mut(&mut live, id) {
                slot["status"] = json!("running");
                if recovery {
                    clear_progress(slot)
                }
                if slot.get("parentTaskId").is_some() {
                    slot["arguments"] = intent_args.clone();
                }
            }
            save(tx, d, live)?;
            Ok((!recovery).then(|| TaskState::Running {
                checkpoint: json!({
                "phase":"execute","arguments":intent_args,"replay":replay}
                ),
            }))
        })
        .await?;
    let env = p.env().await;
    let enabled = task.input["progress"] != false;
    let api = ToolExecutionApi {
        inner: Arc::new(ApiInner {
            phase: p.clone(),
            call: call.clone(),
            tool: tool.clone(),
            env: env.as_ref().ok().cloned().flatten(),
            enabled,
            reported: Mutex::new(Reported {
                output: super::output::OutputBuffer::new(tool.output_limits.clone()),
                details: Presence::Missing,
                diagnostics: vec![],
                generation: 0,
                settled: 0,
                error: None,
                next_at: 0.0,
                timer: None,
                timer_sequence: 0,
                scheduling: false,
                in_flight: false,
                stopped: false,
                written_text: String::new(),
                written_details: Presence::Missing,
                written_diagnostics: 0,
            }),
            changed: tokio::sync::Notify::new(),
            ended: AtomicBool::new(false),
            sequence: AtomicU64::new(0),
            resumes: tool.replay == ReplayPolicy::Safe,
            admissions: AtomicU64::new(0),
        }),
    };
    let started = env.is_ok().then(|| (p.host.options.monotonic_now)());
    let mut execute_call = call.clone();
    execute_call.call.arguments = std::mem::take(&mut args);
    let executed = match env {
        Err(e) => Err(e),
        Ok(_) => {
            (tool.execute)(ToolInvocation {
                call: execute_call,
                api: api.clone(),
            })
            .await
        }
    };
    let duration = started.and_then(|started| {
        pi_ai::SafeUnsigned::new(
            ((p.host.options.monotonic_now)() - started)
                .max(0.0)
                .round()
                .min(MAX_SAFE_INTEGER as f64) as u64,
        )
        .ok()
    });
    {
        let _reported = api.inner.reported.lock().unwrap();
        api.inner.ended.store(true, Ordering::Release);
    }
    loop {
        let changed = api.inner.changed.notified();
        if api.inner.admissions.load(Ordering::Acquire) == 0 {
            break;
        }
        changed.await;
    }
    api.stop_progress().await;
    p.check()?;
    if let Some(e) = p.host.session.failure() {
        return Err(e);
    }
    let progress_error = api.inner.reported.lock().unwrap().error.clone();
    if let Some(e) = progress_error {
        return Err(e);
    }
    let ending = if executed.is_err() {
        "failed"
    } else {
        "completed"
    };
    let mut result = executed.unwrap_or_else(|e| ToolExecutionResult {
        is_error: Some(true),
        diagnostics: Some(vec![diagnostic("tool_error", e.message)]),
        ..Default::default()
    });
    let mut retained_truncation = None;
    {
        let mut r = api.inner.reported.lock().unwrap();
        r.output.end();
        let retained = r.output.snapshot();
        if result.output.is_none() {
            result.output = Some(if retained.text.is_empty() {
                vec![]
            } else {
                vec![text(retained.text)]
            });
            if retained.dropped_bytes > 0 {
                retained_truncation = Some((retained.dropped_bytes, retained.dropped_lines));
            }
        }
        if result.details.is_missing() {
            result.details = r.details.clone()
        }
        let mut ds = r.diagnostics.clone();
        ds.extend(result.diagnostics.take().unwrap_or_default());
        result.diagnostics = Some(ds);
    }
    for h in p.hooks().await? {
        if let Some(h) = h.after_tool {
            match h(call.clone(), result.clone(), p.api()).await {
                Ok(Some(r)) => {
                    result = r;
                    retained_truncation = None;
                }
                Ok(None) => (),
                Err(e) => {
                    p.check()?;
                    p.report(e)
                }
            }
        }
    }
    if let Some((bytes, lines)) = retained_truncation {
        result.diagnostics.get_or_insert_default().push(truncated(
            bytes,
            lines,
            Some(tool.output_limits.retain),
        ));
    }
    bound_result(&tool, &task.input, &mut result, &p)?;
    let settled = settle(&p, task.input, call, Some(result), None, duration, ending).await;
    {
        let mut r = api.inner.reported.lock().unwrap();
        match &settled {
            Ok(()) => r.settled = r.generation,
            Err(e) => r.error = Some(e.clone()),
        }
    }
    api.inner.changed.notify_waiters();
    settled
}
fn validate(
    tool: &ToolRegistration,
    call: &pi_ai::ToolCall,
    args: JsonObject,
) -> Result<JsonObject> {
    let mut call = call.clone();
    call.arguments = args;
    pi_ai::validate_tool_arguments(&tool.declaration, &call)
        .map_err(|e| Error::request(pi_ai::format_tool_validation_feedback(&call, &e)))
}
pub(crate) fn text(text: String) -> InputContent {
    InputContent::Text(pi_ai::TextContent {
        content_type: Default::default(),
        text,
        text_signature: None,
    })
}
pub(crate) fn diagnostic(code: &str, message: impl Into<String>) -> ToolDiagnostic {
    ToolDiagnostic {
        severity: DiagnosticSeverity::Error,
        message: message.into(),
        code: Some(code.into()),
    }
}
pub(crate) fn error_result(code: &str, message: impl Into<String>) -> ToolExecutionResult {
    ToolExecutionResult {
        output: Some(vec![]),
        is_error: Some(true),
        diagnostics: Some(vec![diagnostic(code, message)]),
        ..Default::default()
    }
}
fn truncated(bytes: usize, lines: usize, retain: Option<OutputRetention>) -> ToolDiagnostic {
    ToolDiagnostic {
        severity: DiagnosticSeverity::Warn,
        code: Some("truncated".into()),
        message: format!(
            "Output truncated{}: {lines} lines, {bytes} bytes dropped",
            retain
                .map(|r| if r == OutputRetention::Head {
                    " to its beginning"
                } else {
                    " to its end"
                })
                .unwrap_or("")
        ),
    }
}
fn bound_result(
    tool: &ToolRegistration,
    input: &Value,
    result: &mut ToolExecutionResult,
    p: &Phase,
) -> Result<()> {
    let output = result.output.get_or_insert_default();
    let texts = output
        .iter()
        .filter_map(|x| {
            if let InputContent::Text(t) = x {
                Some(t.text.as_str())
            } else {
                None
            }
        })
        .collect::<String>();
    let bounded = super::output::bound(&texts, &tool.output_limits);
    if bounded.dropped_bytes > 0 {
        let index = if tool.output_limits.retain == OutputRetention::Head {
            output
                .iter()
                .position(|c| matches!(c, InputContent::Text(_)))
        } else {
            output
                .iter()
                .rposition(|c| matches!(c, InputContent::Text(_)))
        };
        let mut next = vec![];
        for (i, c) in output.iter().enumerate() {
            if !matches!(c, InputContent::Text(_)) {
                next.push(c.clone())
            } else if Some(i) == index {
                next.push(text(bounded.text.clone()))
            }
        }
        *output = next;
        result.diagnostics.get_or_insert_default().push(truncated(
            bounded.dropped_bytes,
            bounded.dropped_lines,
            Some(tool.output_limits.retain),
        ));
    }
    let broken = if let Some(schema) = &tool.structured_output_schema {
        if result.structured_output.is_missing() {
            (!result.is_error.unwrap_or(false))
                .then(|| format!("Tool {} returned no structuredOutput", tool.name()))
        } else {
            let value = ai_value(&result.structured_output)?;
            let schema = ai_value(schema)?;
            let validator =
                jsonschema::validator_for(&schema).map_err(|e| Error::validation(e.to_string()))?;
            (!validator.is_valid(&value)).then(|| {
                format!(
                    "Tool {} returned structuredOutput that does not match its schema",
                    tool.name()
                )
            })
        }
    } else {
        (!result.structured_output.is_missing()).then(|| {
            format!(
                "Tool {} returned structuredOutput but declares no structuredOutputSchema",
                tool.name()
            )
        })
    };
    if let Some(message) = broken {
        result.structured_output = Presence::Missing;
        if input["kind"] == "nested" {
            result.is_error = Some(true);
            result
                .diagnostics
                .get_or_insert_default()
                .push(diagnostic("invalid_structured_output", message));
        } else {
            p.report(Error::request(message));
        }
    }
    if input["kind"] == "nested" && tool.structured_output_schema.is_none() {
        let value = match result.output.as_deref().unwrap_or_default() {
            [] => json!(""),
            [InputContent::Text(t)] => json!(t.text),
            [one] => ai_value(one)?,
            many => ai_value(&many)?,
        };
        result.structured_output = decode::<JsonValue>(value).map(Presence::Value)?;
    }
    if input["kind"] == "nested"
        && tool.structured_output_schema.is_some()
        && result.is_error == Some(true)
        && result.structured_output.is_missing()
        && !result
            .diagnostics
            .as_deref()
            .unwrap_or_default()
            .iter()
            .any(|d| d.severity == DiagnosticSeverity::Error)
    {
        let text = result
            .output
            .iter()
            .flatten()
            .filter_map(|c| {
                if let InputContent::Text(t) = c {
                    Some(t.text.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
            .join("\n");
        if !text.is_empty() {
            result
                .diagnostics
                .get_or_insert_default()
                .push(diagnostic("tool_error", text))
        }
    }
    Ok(())
}
async fn settle(
    p: &Phase,
    input: Value,
    call: ToolHookCall,
    result: Option<ToolExecutionResult>,
    interrupted: Option<(String, String)>,
    duration: Option<pi_ai::SafeUnsigned>,
    ending: &str,
) -> Result<()> {
    let nested = p
        .invocation
        .owned_tasks()
        .await?
        .into_iter()
        .filter(|t| t.kind == "pi.tool" && t.input["kind"] == "nested")
        .map(|t| t.id)
        .collect::<Vec<_>>();
    let mut waits = Vec::new();
    for id in nested {
        let inv = p.invocation.clone();
        waits.push(async move { inv.abort_owned(id).await });
    }
    for result in futures_util::future::join_all(waits).await {
        result?;
    }
    let c = p.conversation;
    let task = p.invocation.identity().task_id;
    let failed_message = interrupted
        .as_ref()
        .map(|(_, message)| message.clone())
        .unwrap_or_else(|| format!("Tool {} threw", call.call.name));
    let ending = ending.to_string();
    let now = p.now()?;
    p.invocation
        .commit(move |tx, _| {
            let (id, mut live) = doc(tx, &live_doc(), c)?;
            let mut result = result.unwrap_or_default();
            if let Some((code, message)) = interrupted {
                let slot = slot_mut(&mut live, task);
                result.is_error = Some(true);
                if let Some(s) = slot {
                    result.output = Some(
                        s["output"]
                            .as_str()
                            .filter(|s| !s.is_empty())
                            .map(|s| vec![text(s.into())])
                            .unwrap_or_default(),
                    );
                    if let Some(d) = s.get("details") {
                        result.details = decode(d.clone())?
                    }
                    result.diagnostics = s
                        .get("diagnostics")
                        .map(|v| decode(v.clone()))
                        .transpose()?;
                    if let Some(bytes) = s["droppedBytes"].as_u64().filter(|v| *v > 0) {
                        result.diagnostics.get_or_insert_default().push(truncated(
                            bytes as usize,
                            s["droppedLines"].as_u64().unwrap_or(0) as usize,
                            None,
                        ));
                    }
                }
                result
                    .diagnostics
                    .get_or_insert_default()
                    .push(diagnostic(&code, message));
            }
            let receipt = if input["kind"] == "nested" {
                let nested = NestedToolExecutionResult {
                    task_id: task,
                    structured_output: result.structured_output.clone(),
                    is_error: result.is_error.unwrap_or(false),
                    details: result.details.clone(),
                    diagnostics: result.diagnostics.clone().unwrap_or_default(),
                    usage: result.usage.clone(),
                    duration_ms: duration,
                };
                if let Some(usage) = &result.usage {
                    record_usage(tx, c, "tools", call.call.name.as_str(), usage)?;
                }
                let parent = decode(input["parent"].clone())?;
                let d = tx.doc(
                    &nested_call_doc(),
                    DocumentAddress {
                        kind: "pi.tool.nested-call".into(),
                        scope: Scope::Task { task_id: parent },
                        key: Some(input["key"].as_str().unwrap().into()),
                    },
                    Value::Null,
                )?;
                if tx.document_value(d)?["taskId"] != json!(task) {
                    return Err(Error::validation("nested call index identity mismatch"));
                }
                set(tx, d, vec!["result".into()], ai_value(&nested)?)?;
                if let Some(slot) = slot_mut(&mut live, task) {
                    finish_slot(slot);
                    let mut summary = json!({
                    "isError":nested.is_error}
                    );
                    if let Some(v) = duration {
                        summary["durationMs"] = json!(v)
                    }
                    if let Some(v) = nested.usage {
                        summary["usage"] = ai_value(&v)?;
                    }
                    if nested.is_error {
                        let error = nested
                            .diagnostics
                            .iter()
                            .filter(|d| d.severity == DiagnosticSeverity::Error)
                            .map(|d| d.message.clone())
                            .collect::<Vec<_>>()
                            .join("\n");
                        if !error.is_empty() {
                            summary["error"] = json!(error.chars().take(500).collect::<String>());
                        }
                    }
                    slot["summary"] = summary;
                }
                json!({
                "kind":"nested"}
                )
            } else {
                let entry = append_tool_result(tx, c, &call.call, &result, now, duration)?;
                if let Some(slot) = slot_mut(&mut live, task) {
                    finish_slot(slot);
                    slot["entry"] = json!(entry.id);
                }
                let mut v = json!({
                "kind":"model","entryId":entry.id}
                );
                if ending == "completed"
                    && let Some(control) = result.control
                {
                    v["control"] = ai_value(&control)?;
                }
                v
            };
            remove_nested(&mut live, task);
            save(tx, id, live)?;
            let outcome = match ending.as_str() {
                "aborted" => TaskOutcome::Aborted {
                    reason: None,
                    result: Some(receipt),
                },
                "failed" => TaskOutcome::Failed {
                    error: TaskOutcomeError {
                        message: failed_message,
                        detail: None,
                    },
                    result: Some(receipt),
                },
                _ => TaskOutcome::Completed { result: receipt },
            };
            Ok(Some(TaskState::Terminal { outcome }))
        })
        .await
}
pub(crate) fn append_tool_result(
    tx: &mut Transaction<'_>,
    c: ConversationId,
    call: &pi_ai::ToolCall,
    result: &ToolExecutionResult,
    now: f64,
    duration: Option<pi_ai::SafeUnsigned>,
) -> Result<EntryRecord> {
    let diagnostics = result.diagnostics.clone().unwrap_or_default();
    let mut content = result.output.clone().unwrap_or_default();
    if !diagnostics.is_empty() {
        content.push(text(format!(
            "<harness>\n{}\n</harness>",
            diagnostics
                .iter()
                .map(|d| format!(
                    "[{}] {}",
                    match d.severity {
                        DiagnosticSeverity::Info => "info",
                        DiagnosticSeverity::Warn => "warn",
                        DiagnosticSeverity::Error => "error",
                    },
                    d.message
                ))
                .collect::<Vec<_>>()
                .join("\n")
        )));
    }
    let message = pi_ai::ToolResultMessage {
        role: Default::default(),
        tool_call_id: call.id.clone(),
        tool_name: call.name.clone(),
        content,
        details: result.details.clone(),
        usage: result.usage.clone(),
        nested_calls: None,
        duration_ms: duration,
        is_error: result.is_error.unwrap_or(false),
        timestamp: timestamp(now)?,
    };
    if let Some(usage) = &result.usage {
        record_usage(tx, c, "tools", call.name.as_str(), usage)?;
    }
    let mut d = EntryDraft::new("pi.tool-result");
    d.model = Some(vec![ai_value(&message)?]);
    d.data = Some(json!({
    "diagnostics":diagnostics}
    ));
    tx.append_entry(c, d)
}
