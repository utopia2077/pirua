use super::{
    documents::{field, read},
    *,
};
use crate::session::*;
use pi_codemode::{CallTarget, Event, ExecutionResult, HostCall, Outcome, Run, RunOptions};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
    time::Duration,
};

struct Cancel(pi_codemode::Cancellation);
impl Drop for Cancel {
    fn drop(&mut self) {
        self.0.cancel()
    }
}
enum Vm {
    Event(Event),
    Terminal(ExecutionResult),
}
struct Completed {
    call: HostCall,
    outcome: Outcome,
}
type Completion = (
    HostCall,
    Result<(Outcome, Option<NestedToolExecutionResult>)>,
);
struct Call {
    options: CodeModeOptions,
    api: ToolExecutionApi,
    start: CodeModeStart,
    journal: CodeModeJournalState,
    replayed: CodeModeJournalState,
    rerun: bool,
    replaying: bool,
    next_replayed: usize,
    ready: BTreeMap<u64, Outcome>,
    issued: u64,
    released: bool,
    waiting: VecDeque<HostCall>,
    held: Vec<HostCall>,
    durable_issued: u64,
    rows: BTreeMap<u64, Value>,
    running: tokio::task::JoinSet<Completion>,
}
fn task_scope(api: &ToolExecutionApi) -> Scope {
    Scope::Task {
        task_id: api.identity().invocation.task_id,
    }
}
fn store_scope(api: &ToolExecutionApi) -> Scope {
    Scope::Conversation {
        conversation_id: api.identity().conversation_id,
    }
}
fn text(s: impl Into<String>) -> pi_ai::InputContent {
    pi_ai::InputContent::Text(pi_ai::TextContent {
        content_type: Default::default(),
        text: s.into(),
        text_signature: None,
    })
}
fn error_result(s: impl Into<String>) -> ToolExecutionResult {
    ToolExecutionResult {
        output: Some(vec![text(s)]),
        is_error: Some(true),
        ..Default::default()
    }
}
fn args(call: &HostCall) -> Result<pi_ai::JsonObject> {
    match &call.args {
        None | Some(Value::Null) => Ok(Default::default()),
        Some(Value::Object(_)) => Ok(serde_json::from_value(call.args.clone().unwrap())?),
        _ => Err(Error::request("Tool arguments must be an object")),
    }
}
fn outcome(name: &str, structured: bool, r: &NestedToolExecutionResult) -> Result<Outcome> {
    let value = match &r.structured_output {
        pi_ai::Presence::Missing => None,
        pi_ai::Presence::Null => Some(Value::Null),
        pi_ai::Presence::Value(v) => Some(serde_json::to_value(v)?),
    };
    let value = if structured {
        value
    } else {
        Some(json!(match value {
            Some(Value::String(s)) => s,
            Some(Value::Array(a)) => a
                .iter()
                .filter(|v| v["type"] == "text")
                .filter_map(|v| v["text"].as_str())
                .collect::<Vec<_>>()
                .join("\n"),
            _ => String::new(),
        }))
    };
    if !r.is_error || structured && value.is_some() {
        return Ok(Outcome::Success { value });
    }
    let diagnostics = r
        .diagnostics
        .iter()
        .filter(|d| d.severity == DiagnosticSeverity::Error)
        .map(|d| d.message.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    let message = if !structured {
        value
            .as_ref()
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_owned)
    } else {
        None
    }
    .unwrap_or_else(|| {
        if diagnostics.is_empty() {
            format!("Tool {name} failed")
        } else {
            diagnostics
        }
    });
    Ok(Outcome::Error {
        message,
        properties: value
            .map(|v| BTreeMap::from([("structuredOutput".into(), v)]))
            .unwrap_or_default(),
    })
}
pub(super) async fn execute(
    options: CodeModeOptions,
    api: ToolExecutionApi,
    source: String,
) -> Result<ToolExecutionResult> {
    let parsed =
        pi_coding_runtime::codemode::parse_codemode_source(&source).map_err(Error::request)?;
    let agent = api.agent().await?;
    let (tools, namespaces) = super::describe::declarations(&agent.callable, &options.namespaces);
    let now = api.now()?;
    let start = CodeModeStart {
        version: CODEMODE_MODULE_VERSION,
        code: parsed.code,
        started_at: now,
        seed: 0,
        fingerprint: None,
        deadline: parsed.timeout.map(|d| now + d.as_secs_f64() * 1000.),
        tools,
        namespaces,
        globals: super::describe::globals(&options),
        max_output_tokens: parsed
            .max_output_tokens
            .unwrap_or(DEFAULT_MAX_OUTPUT_TOKENS),
        store: Default::default(),
    };
    let scope = task_scope(&api);
    let conversation = store_scope(&api);
    let seed = options.seed.clone();
    let (run, journal, rerun) = api
        .commit(move |tx| {
            let (id, mut run) = read::<CodeModeRunState>(tx, &codemode_run_doc(), scope.clone())?;
            let rerun = run.start.is_some();
            if run.result.is_none() && run.start.is_none() {
                let (_, store) =
                    read::<CodeModeStoreState>(tx, &codemode_store_doc(), conversation)?;
                let mut start = start;
                start.seed = seed()?;
                start.store = store.values;
                field(tx, id, "start", &start)?;
                run.start = Some(start);
            }
            let (_, journal) = read::<CodeModeJournalState>(tx, &codemode_journal_doc(), scope)?;
            Ok((run, journal, rerun))
        })
        .await?;
    if let Some(result) = run.result {
        return Ok(result);
    }
    let start = run
        .start
        .ok_or_else(|| Error::validation("CodeMode start missing"))?;
    let durable_issued = journal
        .entries
        .iter()
        .filter_map(|e| e.issued_after)
        .chain(journal.first_issued)
        .max()
        .unwrap_or(0);
    let mut call = Call {
        options,
        api,
        start,
        replaying: !journal.entries.is_empty(),
        rerun,
        next_replayed: 0,
        ready: Default::default(),
        issued: 0,
        released: !rerun,
        waiting: Default::default(),
        held: Vec::new(),
        durable_issued,
        replayed: journal.clone(),
        journal,
        rows: Default::default(),
        running: tokio::task::JoinSet::new(),
    };
    if call.start.version != CODEMODE_MODULE_VERSION {
        return call.decide(error_result(format!("The script cannot resume: it started under code mode version {}, this is {CODEMODE_MODULE_VERSION}.",call.start.version)),None).await;
    }
    let current = call.api.now()?;
    let remaining = call.start.deadline.map(|d| d - current);
    if remaining.is_some_and(|r| r <= 0.) {
        return call
            .decide(
                error_result("Script timed out: its deadline passed while it was interrupted."),
                None,
            )
            .await;
    }
    let mut native = RunOptions::new(call.start.started_at, call.start.seed);
    native.tools = call.start.tools.iter().map(Into::into).collect();
    native.globals = call.start.globals.clone();
    native.store = call.start.store.clone();
    native.timeout = remaining.map(|r| Duration::from_secs_f64(r / 1000.));
    call.api.check_live()?;
    #[cfg(test)]
    call.observe(json!({"kind":"start","taskId":call.api.identity().invocation.task_id,"start":call.start,"journal":call.journal}));
    let run = Arc::new(
        call.options
            .sandbox
            .start(call.start.code.clone(), native)
            .map_err(|e| Error::request(e.to_string()))?,
    );
    let _cancel = Cancel(run.cancellation());
    let (send, mut events) = tokio::sync::mpsc::unbounded_channel();
    let worker = run.clone();
    #[cfg(test)]
    let observation = call.options.observe.clone();
    let pump = tokio::task::spawn_blocking(move || {
        loop {
            if let Some(event) = worker.next_event(Duration::from_millis(10)) {
                #[cfg(test)]
                if let Some(observer) = &observation {
                    observer(match &event {
                        Event::Started { fingerprint } => {
                            json!({"kind":"started","fingerprint":fingerprint})
                        }
                        Event::Call(call) => json!({"kind":"call","call":call}),
                        Event::Idle { seq, issued } => {
                            json!({"kind":"idle","seq":seq,"issued":issued})
                        }
                    });
                }
                if send.send(Vm::Event(event)).is_err() {
                    worker.cancel();
                    break;
                }
            } else if worker.is_finished() {
                break;
            }
        }
        let result = worker.wait();
        #[cfg(test)]
        if let Some(observer) = &observation {
            observer(json!({"kind":"terminal","result":result}));
        }
        let _ = send.send(Vm::Terminal(result));
    });
    let result = call.drive(&run, &mut events).await;
    run.cancel();
    call.running.shutdown().await;
    pump.await
        .map_err(|e| Error::request(format!("CodeMode event pump: {e}")))?;
    result
}
impl Call {
    #[cfg(test)]
    fn observe(&self, value: Value) {
        if let Some(observer) = &self.options.observe {
            observer(value)
        }
    }
    fn rows(&self) -> Vec<Value> {
        self.rows
            .values()
            .cloned()
            .map(|mut r| {
                if r["status"] == "running" {
                    r["status"] = json!("cancelled")
                }
                r
            })
            .collect()
    }
    async fn decide(
        &mut self,
        mut result: ToolExecutionResult,
        writes: Option<pi_codemode::StoreWrites>,
    ) -> Result<ToolExecutionResult> {
        if result.details.is_missing() {
            result.details =
                pi_ai::Presence::Value(serde_json::from_value(json!({"calls":self.rows()}))?)
        }
        let scope = task_scope(&self.api);
        let conversation = store_scope(&self.api);
        self.api
            .commit(move |tx| {
                let (id, run) = read::<CodeModeRunState>(tx, &codemode_run_doc(), scope)?;
                if let Some(recorded) = run.result {
                    return Ok(recorded);
                }
                field(tx, id, "result", &result)?;
                if let Some(writes) = writes.filter(|w| !w.set.is_empty() || !w.delete.is_empty()) {
                    let (id, _) =
                        read::<CodeModeStoreState>(tx, &codemode_store_doc(), conversation)?;
                    for key in writes.delete {
                        if tx.document_value(id)?["values"].get(&key).is_some() {
                            tx.apply(id, Op::Delete(vec!["values".into(), key.as_str().into()]))?;
                        }
                    }
                    for (key, value) in writes.set {
                        tx.apply(
                            id,
                            Op::Set(vec!["values".into(), key.as_str().into()], value),
                        )?;
                    }
                }
                Ok(result)
            })
            .await
    }
    async fn diverge(&mut self, message: impl AsRef<str>) -> Result<ToolExecutionResult> {
        let result = error_result(format!(
            "The script took a different path when resumed after a restart: {}",
            message.as_ref()
        ));
        // Asynchronous divergence persists the rows in its first decision, then
        // returns the original host error to the outer tool invocation.
        self.decide(result.clone(), None).await?;
        Ok(result)
    }
    async fn drive(
        &mut self,
        run: &Arc<Run>,
        events: &mut tokio::sync::mpsc::UnboundedReceiver<Vm>,
    ) -> Result<ToolExecutionResult> {
        let signal = self.api.signal();
        let mut fingerprint = false;
        loop {
            tokio::select! {
                biased;
                _ = signal.cancelled() => return Err(Error::request("CodeMode invocation cancelled")),
                event = events.recv() => {
                    match event {
                        Some(Vm::Event(Event::Started {
                            fingerprint: actual,
                        })) => {
                            if let Some(recorded) = &self.start.fingerprint {
                                if recorded != &actual {
                                    return self.decide(error_result(format!("The script cannot resume: the sandbox changed since it started ({recorded} -> {actual}).")),None).await;
                                }
                            } else {
                                let scope = task_scope(&self.api);
                                let actual = actual.clone();
                                self.api
                                    .commit(move |tx| {
                                        let (id, mut run) =
                                            read::<CodeModeRunState>(tx, &codemode_run_doc(), scope)?;
                                        let start = run
                                            .start
                                            .as_mut()
                                            .ok_or_else(|| Error::validation("CodeMode start missing"))?;
                                        start.fingerprint = Some(actual);
                                        field(tx, id, "start", start)
                                    })
                                    .await?;
                            }
                            fingerprint = true;
                        }
                        Some(Vm::Event(Event::Call(call))) => {
                            // Retain the actual emitted call even if the worker
                            // already ended; only admission stops at terminal.
                            if call.target == CallTarget::Tool {
                                self.rows.entry(call.id).or_insert_with(||json!({"id":call.id,"name":call.name,"args":preview(call.args.as_ref().map(Value::to_string).unwrap_or_default(),200),"status":"running"}));
                            }
                            if run.is_finished() {
                                continue;
                            }
                            self.issued = self.issued.max(call.id);
                            if let Some(entry) = self
                                .journal
                                .entries
                                .iter()
                                .find(|e| e.id == call.id)
                                .filter(|_| self.replaying)
                            {
                                if let Some(global) = &entry.global {
                                    if call.target != global.target.clone().unwrap_or(CallTarget::Global)
                                        || call.name != global.name
                                        || call.args != global.args
                                    {
                                        return self
                                            .diverge(format!(
                                                "call {} changed its recorded call target or arguments",
                                                call.id
                                            ))
                                            .await;
                                    }
                                    let answer = global.outcome.clone();
                                    self.finish(&call, &answer, None);
                                    self.ready.insert(call.id, answer);
                                } else if call.target != CallTarget::Tool {
                                    return self
                                        .diverge(format!("call {} was a tool call", call.id))
                                        .await;
                                } else {
                                    self.launch(call);
                                }
                            } else if self.released {
                                self.waiting.push_back(call)
                            } else {
                                self.held.push(call)
                            }
                        }
                        Some(Vm::Event(Event::Idle { seq, issued })) => {
                            if run.is_finished() {
                                continue;
                            }
                            if self.rerun && seq <= self.replayed.entries.len() as u64 {
                                let recorded = if seq == 0 {
                                    self.replayed.first_issued
                                } else {
                                    self.replayed.entries[(seq - 1) as usize].issued_after
                                };
                                if recorded.is_some_and(|v| v != issued) {
                                    return self.diverge(if seq==0{format!("its first run made {issued} calls, where it had made {}",recorded.unwrap())}else{format!("after result {seq} it had made {issued} calls, where it had made {}",recorded.unwrap())}).await;
                                }
                            }
                            self.commit_batch(Vec::new(), vec![(seq, issued)], run)
                                .await?;
                            if self.replaying && seq == self.next_replayed as u64 {
                                if self
                                    .journal
                                    .entries
                                    .get(seq as usize)
                                    .is_some_and(|e| e.id > issued)
                                {
                                    return self
                                        .diverge("it waits without making a call it made before")
                                        .await;
                                }
                                if seq == self.journal.entries.len() as u64 {
                                    self.replaying = false;
                                }
                            }
                            if !self.replaying
                                && !self.released
                                && (seq == 0 || seq == self.journal.entries.len() as u64)
                                && let Some(message) = self.release().await?
                            {
                                return self.diverge(message).await;
                            }
                        }
                        Some(Vm::Terminal(result)) => {
                            self.api.check_live()?;
                            if let ExecutionResult::Error { error, .. } = &result
                                && error.kind == pi_codemode::ErrorKind::Aborted
                                && !signal.is_cancelled()
                            {
                                signal.cancelled().await;
                                return Err(Error::request(
                                    "CodeMode sandbox closed during a live invocation",
                                ));
                            }
                            let during = match &result {
                                ExecutionResult::Success { during, .. } | ExecutionResult::Error { during, .. } => {
                                    during
                                }
                            };
                            if self.rerun
                                && let Some(d) = during
                                && d.seq <= self.replayed.entries.len() as u64
                            {
                                let continued = if d.seq == 0 {
                                    !self.replayed.entries.is_empty() || self.replayed.first_issued.is_some()
                                } else {
                                    self.replayed.entries[(d.seq - 1) as usize]
                                        .issued_after
                                        .is_some()
                                };
                                if continued
                                    || self
                                        .replayed
                                        .entries
                                        .iter()
                                        .skip(d.seq as usize)
                                        .any(|e| e.id > d.issued)
                                {
                                    return self.decide(error_result(format!(
                                        "The script took a different path when resumed after a restart: it ended after result {}, where it went on before.",
                                        d.seq
                                    )), None).await;
                                }
                            }
                            let writes = match &result {
                                ExecutionResult::Success { store_writes, .. } => Some(store_writes.clone()),
                                _ => None,
                            };
                            let formatted =
                                super::output::format(&self.api, &self.start, result, self.rows()).await?;
                            return self.decide(formatted, writes).await;
                        }
                        None => return Err(Error::request("CodeMode event pump closed")),
                    }
                },
                Some(completion) = self.running.join_next(), if !self.running.is_empty() => {
                    let (call, result) =
                        completion.map_err(|e| Error::request(format!("CodeMode host callback: {e}")))?;
                    if run.is_finished() {
                        continue;
                    }
                    let (answer, nested) = match result {
                        Ok(v) => v,
                        Err(e) if e.kind == ErrorKind::Request => {
                            self.api.check_live()?;
                            return self.diverge(format!("call {}: {e}", call.id)).await;
                        }
                        Err(e) => return Err(e),
                    };
                    self.finish(&call, &answer, nested.as_ref());
                    if self.replaying && self.journal.entries.iter().any(|e| e.id == call.id) {
                        self.ready.insert(call.id, answer);
                    } else {
                        self.commit_batch(
                            vec![Completed {
                                call,
                                outcome: answer,
                            }],
                            Vec::new(),
                            run,
                        )
                        .await?;
                    }
                }
            }
            if fingerprint && !run.is_finished() {
                while self.replaying && self.next_replayed < self.journal.entries.len() {
                    let entry = &self.journal.entries[self.next_replayed];
                    if entry.id > self.issued {
                        break;
                    }
                    let Some(answer) = self.ready.remove(&entry.id) else {
                        break;
                    };
                    self.api.check_live()?;
                    #[cfg(test)] self.observe(json!({"kind":"deliver","seq":self.next_replayed as u64+1,"at":entry.at,"id":entry.id,"outcome":answer}));
                    run.deliver(self.next_replayed as u64 + 1, entry.at, entry.id, answer)
                        .map_err(|e| Error::request(e.to_string()))?;
                    self.next_replayed += 1;
                }
                self.launch_covered(run);
            }
        }
    }
    async fn release(&mut self) -> Result<Option<String>> {
        if self.rerun {
            for call in &self.held {
                let task = self.api.identity().invocation.task_id;
                let address = crate::nested_call_doc()
                    .address(Scope::Task { task_id: task }, Some(format!("c{}", call.id)))?;
                if let Some(prior) = self.api.snapshot(crate::nested_call_doc(), address).await?
                    && let Some(id) = prior.get("taskId")
                {
                    let old = self
                        .api
                        .get_task(serde_json::from_value(id.clone())?)
                        .await?
                        .ok_or_else(|| Error::validation("nested task missing"))?;
                    if old.input["kind"] == "nested"
                        && (call.target != CallTarget::Tool
                            || old.input["call"]["name"] != call.name
                            || old.input["call"]["arguments"] != serde_json::to_value(args(call)?)?)
                    {
                        return Ok(Some(format!(
                            "call {} is another target, tool or arguments than its earlier keyed call",
                            call.id
                        )));
                    }
                }
            }
        }
        self.released = true;
        self.waiting.extend(self.held.drain(..));
        Ok(None)
    }
    fn launch_covered(&mut self, run: &Run) {
        if !self.released || run.is_finished() {
            return;
        }
        let mut pending = VecDeque::new();
        while let Some(call) = self.waiting.pop_front() {
            if call.id <= self.durable_issued {
                self.launch(call)
            } else {
                pending.push_back(call)
            }
        }
        self.waiting = pending;
    }
    fn launch(&mut self, call: HostCall) {
        let api = self.api.clone();
        let options = self.options.clone();
        let start = self.start.clone();
        let copy = call.clone();
        // Each host operation must keep polling while the journal owner awaits a
        // Session commit. An inline sibling may otherwise hold its line permit
        // while suspended behind that owner. JoinSet also aborts work on drop.
        self.running.spawn(async move {
            let result = async {
                api.check_live()?;
                if call.target == CallTarget::Global {
                    if let Some(result) = super::describe::discovery(&call, &start) {
                        return Ok((result, None));
                    }
                    let Some(global) = options.globals.iter().find(|g| g.name == call.name) else {
                        return Ok((
                            Outcome::error(format!("Unknown global {}", call.name)),
                            None,
                        ));
                    };
                    let result = (global.execute)(call.args.clone(), api.clone()).await;
                    api.check_live()?;
                    return match result {
                        Ok(value) => Ok((Outcome::Success { value }, None)),
                        Err(e) if e.kind == ErrorKind::Request => {
                            Ok((Outcome::error(e.to_string()), None))
                        }
                        Err(e) => Err(e),
                    };
                }
                let arguments = match args(&call) {
                    Ok(v) => v,
                    Err(e) => return Ok((Outcome::error(e.to_string()), None)),
                };
                #[cfg(test)]
                if let Some(o) = &options.observe {
                    o(json!({"kind":"nested-enter","id":call.id}));
                }
                let result = api
                    .execute_tool(
                        call.name.clone(),
                        arguments,
                        NestedToolOptions {
                            key: Some(format!("c{}", call.id)),
                            progress: None,
                        },
                    )
                    .await?;
                #[cfg(test)]
                if let Some(o) = &options.observe {
                    o(json!({"kind":"nested-return","id":call.id,"result":result}));
                }
                let structured = start
                    .tools
                    .iter()
                    .find(|t| t.name == call.name)
                    .is_some_and(|t| t.structured);
                Ok((outcome(&call.name, structured, &result)?, Some(result)))
            }
            .await;
            (copy, result)
        });
    }
    fn finish(
        &mut self,
        call: &HostCall,
        answer: &Outcome,
        nested: Option<&NestedToolExecutionResult>,
    ) {
        let Some(row) = self.rows.get_mut(&call.id) else {
            return;
        };
        let failed = nested
            .map(|n| n.is_error)
            .unwrap_or(matches!(answer, Outcome::Error { .. }));
        row["status"] = json!(if failed { "error" } else { "ok" });
        if let Some(duration) = nested.and_then(|n| n.duration_ms) {
            row["durationMs"] = json!(duration);
        }
        if failed {
            let message = match answer {
                Outcome::Error { message, .. } => message.clone(),
                _ => nested
                    .map(|n| {
                        n.diagnostics
                            .iter()
                            .filter(|d| d.severity == DiagnosticSeverity::Error)
                            .map(|d| d.message.as_str())
                            .collect::<Vec<_>>()
                            .join("\n")
                    })
                    .unwrap_or_default(),
            };
            if !message.is_empty() {
                row["error"] = json!(preview(message, 500));
            }
        }
    }
    async fn commit_batch(
        &mut self,
        items: Vec<Completed>,
        acks: Vec<(u64, u64)>,
        run: &Run,
    ) -> Result<()> {
        let scope = task_scope(&self.api);
        let entries = items
            .iter()
            .map(|c| {
                Ok(JournalEntry {
                    id: c.call.id,
                    at: self.api.now()?,
                    issued_after: None,
                    global: (c.call.target == CallTarget::Global || args(&c.call).is_err()).then(
                        || GlobalResult {
                            target: (c.call.target == CallTarget::Tool).then_some(CallTarget::Tool),
                            name: c.call.name.clone(),
                            args: c.call.args.clone(),
                            outcome: c.outcome.clone(),
                        },
                    ),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let incoming = entries.clone();
        let ack_copy = acks.clone();
        self.api
            .commit(move |tx| {
                let (id, journal) =
                    read::<CodeModeJournalState>(tx, &codemode_journal_doc(), scope)?;
                for (seq, issued) in ack_copy {
                    if seq == 0 {
                        if journal.first_issued.is_none() {
                            field(tx, id, "firstIssued", issued)?;
                        }
                    } else if journal
                        .entries
                        .get((seq - 1) as usize)
                        .is_some_and(|e| e.issued_after.is_none())
                    {
                        tx.apply(
                            id,
                            Op::Set(
                                vec![
                                    "entries".into(),
                                    ((seq - 1) as usize).into(),
                                    "issuedAfter".into(),
                                ],
                                json!(issued),
                            ),
                        )?;
                    }
                }
                if !incoming.is_empty() {
                    tx.apply(
                        id,
                        Op::Splice(
                            vec!["entries".into()],
                            journal.entries.len(),
                            0,
                            incoming
                                .into_iter()
                                .map(serde_json::to_value)
                                .collect::<std::result::Result<_, _>>()?,
                        ),
                    )?;
                }
                Ok(())
            })
            .await?;
        for (seq, issued) in acks {
            if seq == 0 {
                self.journal.first_issued.get_or_insert(issued);
            } else if let Some(e) = self.journal.entries.get_mut((seq - 1) as usize) {
                e.issued_after.get_or_insert(issued);
            }
            self.durable_issued = self.durable_issued.max(issued);
        }
        for (entry, item) in entries.into_iter().zip(items) {
            self.journal.entries.push(entry.clone());
            if !run.is_finished() {
                self.api.check_live()?;
                #[cfg(test)] self.observe(json!({"kind":"deliver","seq":self.journal.entries.len()as u64,"at":entry.at,"id":entry.id,"outcome":item.outcome}));
                run.deliver(
                    self.journal.entries.len() as u64,
                    entry.at,
                    entry.id,
                    item.outcome,
                )
                .map_err(|e| Error::request(e.to_string()))?;
            }
        }
        Ok(())
    }
}
fn preview(s: String, max: usize) -> String {
    if s.chars().count() <= max {
        s
    } else {
        format!("{}...", s.chars().take(max - 3).collect::<String>())
    }
}
