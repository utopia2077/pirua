//! Local CodeMode tool adapter over a CodingSession's callable registry.
//!
//! The neutral sandbox owns the worker/VM. This adapter owns declaration,
//! nested-call routing, output presentation and canonical branch store commits.
use crate::mcp::{McpOutputSaver, format_size, save_output, text, text_of, to_ai_json};
use crate::{RegisteredTool, SessionError, ToolExposure, WeakCodingSession};
use futures_util::{StreamExt, stream::FuturesUnordered};
use pi_agent::{
    AgentError, AgentTool, AgentToolCallOutcome, AgentToolResult, CancellationToken, ToolInvocation,
};
use pi_ai::InputContent;
use pi_codemode::{
    CallTarget, DeclaredGlobal, DeclaredTool, Event, ExecutionResult, HostCall, Outcome,
    OutputItem, RunOptions, Sandbox, StoreWrites,
};
use pi_coding_session::{EntryContent, SessionSnapshot};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

pub const CODEMODE_TOOL_NAME: &str = "codemode";
pub const CODEMODE_STORE_ENTRY_TYPE: &str = "codemode-store";

#[derive(Clone)]
pub struct CodeModeOptions {
    /// Overall deadline, including nested calls, passed to the local worker.
    pub timeout: Duration,
    pub max_output_tokens: usize,
    pub max_concurrent_tools: usize,
    pub save_output: Option<McpOutputSaver>,
}
impl Default for CodeModeOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(300),
            max_output_tokens: 10_000,
            max_concurrent_tools: 32,
            save_output: None,
        }
    }
}
#[derive(Clone)]
pub struct CodeModeAdapter {
    session: WeakCodingSession,
    sandbox: Sandbox,
    options: CodeModeOptions,
    source: String,
}
static NEXT_ADAPTER: AtomicU64 = AtomicU64::new(1);
/// The invocation owns cancellation even while the event pump retains `Run`.
/// Dropping an async caller skips its cleanup tail, so signal both boundaries
/// here and let the neutral monitor reap the child and release admission.
struct InvocationCancellation {
    worker: pi_codemode::Cancellation,
    nested: CancellationToken,
}
impl Drop for InvocationCancellation {
    fn drop(&mut self) {
        self.nested.cancel();
        self.worker.cancel();
    }
}
struct CallProgress {
    rows: Mutex<BTreeMap<u64, Value>>,
    publish: pi_agent::ToolUpdate,
}
type CallCatalog = (
    Arc<BTreeMap<String, AgentTool>>,
    Arc<Vec<AgentTool>>,
    Arc<BTreeMap<String, String>>,
);
impl CallProgress {
    fn publish(&self) -> Result<(), AgentError> {
        let calls = crate::session::lock(&self.rows)
            .values()
            .cloned()
            .collect::<Vec<_>>();
        (self.publish)(AgentToolResult {
            details: Some(to_ai_json(json!({"calls":calls}))?),
            ..Default::default()
        });
        Ok(())
    }
    fn start(&self, call: &HostCall, parent: &str) -> Result<(), AgentError> {
        if call.target == CallTarget::Tool {
            crate::session::lock(&self.rows).insert(call.id,json!({"id":format!("{parent}/?"),"name":call.name,"args":preview(call.args.as_ref().map(|v|v.to_string()).unwrap_or_default(),200),"status":"running"}));
            self.publish()?;
        }
        Ok(())
    }
    fn finish(&self, id: u64, row: Value) -> Result<(), AgentError> {
        crate::session::lock(&self.rows).insert(id, row);
        self.publish()
    }
    fn finish_pending(&self) -> Result<(), AgentError> {
        for row in crate::session::lock(&self.rows).values_mut() {
            if row["status"] == "running" {
                row["status"] = json!("cancelled");
            }
        }
        self.publish()
    }
}
fn preview(value: String, max: usize) -> String {
    if value.chars().count() > max {
        format!(
            "{}...",
            value
                .chars()
                .take(max.saturating_sub(3))
                .collect::<String>()
        )
    } else {
        value
    }
}
impl CodeModeAdapter {
    pub fn new(
        session: WeakCodingSession,
        sandbox: Sandbox,
        options: CodeModeOptions,
    ) -> Result<Self, SessionError> {
        if options.max_concurrent_tools == 0 {
            return Err(SessionError::Invalid(
                "CodeMode requires nonzero nested tool concurrency".into(),
            ));
        }
        Ok(Self {
            session,
            sandbox,
            options,
            source: format!("codemode:{}", NEXT_ADAPTER.fetch_add(1, Ordering::Relaxed)),
        })
    }
    /// Registration rejects another owner's `codemode` tool atomically.
    pub fn attach(&self) -> Result<(), SessionError> {
        let session = self.session.upgrade().ok_or(SessionError::Disposed)?;
        session.replace_tools_for_source(&self.source, vec![self.tool()?])
    }
    pub fn retire(&self) -> Result<(), SessionError> {
        if let Some(session) = self.session.upgrade() {
            session.replace_tools_for_source(&self.source, Vec::new())?;
        }
        Ok(())
    }
    pub fn tool(&self) -> Result<RegisteredTool, SessionError> {
        let session = self.session.upgrade().ok_or(SessionError::Disposed)?;
        let deferred = session
            .all_tools()
            .into_iter()
            .filter(|t| t.exposure == ToolExposure::Deferred)
            .map(|t| t.declaration.name.to_string())
            .collect::<std::collections::BTreeSet<_>>();
        let tools = session
            .callable_tools()
            .into_iter()
            .filter(|t| {
                t.declaration.name.as_str() != CODEMODE_TOOL_NAME
                    && !deferred.contains(t.declaration.name.as_str())
            })
            .collect::<Vec<_>>();
        let description = format!(
            "Run JavaScript as an async function body in a local QuickJS worker. Input is {{code: string}}. Top-level await and return work. Use await tools.<name>({{...args}}). Text tools resolve to text; tools with an output schema resolve to structuredContent, including structured error results. Unawaited calls are cancelled when the script ends.\nGlobals: text(value), image(dataUrlOrImageBlock), console.log(...), exit(), store(key,value), load(key), ALL_TOOLS, await searchTools(query, {{limit?,namespace?}}), await describeTool(name), await describeNamespace(name). No implicit host files, network, credentials or timers. Optional first line: // @options: {{\"max_output_tokens\":10000,\"timeout_ms\":60000}}\n\n{}",
            tools
                .iter()
                .map(render_declaration)
                .collect::<Vec<_>>()
                .join("\n\n")
        );
        let declaration = serde_json::from_value(
            json!({"name":CODEMODE_TOOL_NAME,"description":description,"parameters":{"type":"object","properties":{"code":{"type":"string","description":"JavaScript async function body"}},"required":["code"],"additionalProperties":false}}),
        )?;
        let adapter = self.clone();
        let tool = AgentTool::new(
            declaration,
            Arc::new(move |invocation| {
                let adapter = adapter.clone();
                Box::pin(async move { adapter.execute(invocation).await })
            }),
        );
        Ok(RegisteredTool { tool, exposure: ToolExposure::Direct, source: Some(self.source.clone()), default_active: true, namespace: None, prompt_snippet: Some("Run JavaScript that calls other tools".into()), prompt_guidelines: vec!["Use codemode to batch independent tool calls with Promise.allSettled, chain them, or filter large output.".into()] })
    }
    async fn execute(&self, invocation: ToolInvocation) -> Result<AgentToolResult, AgentError> {
        let started = Instant::now();
        let session = self
            .session
            .upgrade()
            .ok_or_else(|| AgentError::Tool("CodingSession is no longer available".into()))?;
        if session.is_disposed() {
            return Err(AgentError::Tool("CodingSession is disposed".into()));
        }
        let arguments = serde_json::to_value(&invocation.arguments)
            .map_err(|e| AgentError::Tool(e.to_string()))?;
        let source = arguments
            .get("code")
            .and_then(Value::as_str)
            .ok_or_else(|| AgentError::Tool("CodeMode expects {code: string}".into()))?;
        let parsed = parse_codemode_source(source).map_err(AgentError::Tool)?;
        if invocation.cancellation.is_cancelled() {
            return Err(AgentError::Tool("CodeMode aborted".into()));
        }
        let cursor = session.branch_cursor();
        let snapshot = session.snapshot();
        let infos = session.all_tools();
        let ordered = Arc::new(
            session
                .callable_tools()
                .into_iter()
                .filter(|t| t.declaration.name.as_str() != CODEMODE_TOOL_NAME)
                .collect::<Vec<_>>(),
        );
        let tools = Arc::new(
            ordered
                .iter()
                .cloned()
                .map(|t| (t.declaration.name.to_string(), t))
                .collect::<BTreeMap<_, _>>(),
        );
        let declarations = ordered
            .iter()
            .map(|tool| DeclaredTool {
                name: tool.declaration.name.to_string(),
                js_name: None,
                description: render_declaration(tool),
            })
            .collect();
        let namespaces = Arc::new(
            infos
                .into_iter()
                .filter_map(|i| i.namespace.map(|ns| (i.declaration.name.to_string(), ns)))
                .collect::<BTreeMap<_, _>>(),
        );
        let now = unix_ms();
        let mut options = RunOptions::new(
            now,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .subsec_nanos(),
        );
        options.tools = declarations;
        options.globals = ["searchTools", "describeTool", "describeNamespace"]
            .into_iter()
            .map(|name| DeclaredGlobal {
                name: name.into(),
                spread: true,
            })
            .collect();
        options.store = read_codemode_store(&snapshot);
        options.timeout = Some(parsed.timeout.unwrap_or(self.options.timeout));
        let run = Arc::new(
            self.sandbox
                .start(parsed.code, options)
                .map_err(|e| AgentError::Tool(e.to_string()))?,
        );
        let nested_cancellation = CancellationToken::new();
        let _cancellation = InvocationCancellation {
            worker: run.cancellation(),
            nested: nested_cancellation.clone(),
        };
        let (event_sender, mut events) = tokio::sync::mpsc::unbounded_channel();
        let (result_sender, mut result_receiver) = tokio::sync::oneshot::channel();
        let pump_run = run.clone();
        // One blocking pump waits for the neutral worker; no executor thread is
        // blocked, and all callbacks run on the calling async runtime.
        let pump = tokio::task::spawn_blocking(move || {
            loop {
                if let Some(event) = pump_run.next_event(Duration::from_millis(10)) {
                    let _ = event_sender.send(event);
                } else if pump_run.is_finished() {
                    break;
                }
            }
            let _ = result_sender.send(pump_run.wait());
        });
        let progress = Arc::new(CallProgress {
            rows: Mutex::new(BTreeMap::new()),
            publish: invocation.on_update.clone(),
        });
        progress.publish()?;
        let mut queued = std::collections::VecDeque::<HostCall>::new();
        let mut running: FuturesUnordered<
            pi_agent::BoxFuture<'static, (u64, Result<Outcome, AgentError>)>,
        > = FuturesUnordered::new();
        let mut fingerprint = None;
        let mut seq = 0_u64;
        let mut fatal = None;
        let mut externally_cancelled = false;
        let mut events_open = true;
        let result = loop {
            while running.len() < self.options.max_concurrent_tools
                && !run.is_finished()
                && !externally_cancelled
            {
                let Some(call) = queued.pop_front() else {
                    break;
                };
                let session = session.clone();
                let cancellation = nested_cancellation.clone();
                let tools = tools.clone();
                let namespaces = namespaces.clone();
                let ordered = ordered.clone();
                let parent = invocation.call_id.clone();
                let progress = progress.clone();
                running.push(Box::pin(async move {
                    let id = call.id;
                    (
                        id,
                        execute_call(
                            session,
                            parent,
                            call,
                            cancellation,
                            (tools, ordered, namespaces),
                            progress,
                        )
                        .await,
                    )
                }));
            }
            tokio::select! {
                biased;
                _ = invocation.cancellation.cancelled(), if !externally_cancelled => { externally_cancelled = true; run.cancel(); nested_cancellation.cancel(); queued.clear(); },
                result = &mut result_receiver => break result.map_err(|e| AgentError::Tool(e.to_string()))?,
                event = events.recv(), if events_open => match event {
                    Some(Event::Started { fingerprint: value }) => fingerprint = Some(value),
                    Some(Event::Call(call)) => { if !externally_cancelled { progress.start(&call,&invocation.call_id)?; queued.push_back(call); } },
                    Some(Event::Idle { .. }) => {},
                    None => events_open = false,
                },
                Some((id, outcome)) = running.next(), if !running.is_empty() => {
                    match outcome {
                        Ok(outcome) => {
                            seq += 1;
                            if let Err(error) = run.deliver(seq, now+started.elapsed().as_secs_f64()*1000., id, outcome) {
                                // Oversized host results become catchable tool errors.
                                if let Err(error) = run.deliver(seq, now+started.elapsed().as_secs_f64()*1000., id, Outcome::error(format!("CodeMode host outcome rejected: {error}"))) { fatal = Some(AgentError::Tool(error.to_string())); run.cancel(); }
                            }
                        },
                        Err(error) => { fatal = Some(error); run.cancel(); nested_cancellation.cancel(); queued.clear(); },
                    }
                },
            }
        };
        nested_cancellation.cancel();
        queued.clear();
        // Callbacks must honor the shared Agent cancellation contract. Settle
        // them before the parent returns so no events/usage arrive afterwards.
        while let Some((_, outcome)) = running.next().await {
            if let Err(error) = outcome {
                fatal.get_or_insert(error);
            }
        }
        pump.await.map_err(|e| AgentError::Tool(e.to_string()))?;
        while let Ok(event) = events.try_recv() {
            if let Event::Started { fingerprint: value } = event {
                fingerprint = Some(value);
            }
        }
        progress.finish_pending()?;
        if let Some(error) = fatal {
            return Err(error);
        }
        if externally_cancelled || invocation.cancellation.is_cancelled() {
            return Err(AgentError::Tool("CodeMode aborted".into()));
        }
        let calls = crate::session::lock(&progress.rows)
            .values()
            .cloned()
            .collect();
        let mut result = format_result(
            result,
            started.elapsed(),
            fingerprint,
            parsed
                .max_output_tokens
                .unwrap_or(self.options.max_output_tokens),
            &self.options.save_output,
            calls,
        )
        .await?;
        if invocation.cancellation.is_cancelled() {
            return Err(AgentError::Tool("CodeMode aborted".into()));
        }
        if let Some(writes) = result.1.take() {
            // Same-branch deltas commit in completion order; session/navigation
            // changes reject this commit without replay or retries.
            session
                .append_custom_entries_if_cursor(
                    &cursor,
                    if writes.set.is_empty() && writes.delete.is_empty() {
                        Vec::new()
                    } else {
                        vec![(
                            CODEMODE_STORE_ENTRY_TYPE.into(),
                            Some(
                                serde_json::to_value(writes)
                                    .map_err(|e| AgentError::Tool(e.to_string()))?,
                            ),
                        )]
                    },
                )
                .await
                .map_err(|e| AgentError::Tool(e.to_string()))?;
        }
        Ok(result.0)
    }
}

fn unix_ms() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
        * 1000.
}
pub fn render_declaration(tool: &AgentTool) -> String {
    format!(
        "tools.{}(args: {}) -> {}\n{}",
        pi_codemode::to_identifier(tool.declaration.name.as_str()),
        serde_json::to_string(&tool.declaration.parameters).unwrap(),
        tool.output_schema
            .as_ref()
            .map(|s| serde_json::to_string(s).unwrap())
            .unwrap_or_else(|| "string".into()),
        tool.declaration.description
    )
}

async fn execute_call(
    session: crate::CodingSession,
    parent: String,
    call: HostCall,
    cancellation: CancellationToken,
    catalog: CallCatalog,
    progress: Arc<CallProgress>,
) -> Result<Outcome, AgentError> {
    let (tools, ordered, namespaces) = catalog;
    if cancellation.is_cancelled() {
        return Ok(Outcome::error("CodeMode nested call cancelled"));
    }
    if call.target == CallTarget::Global {
        return Ok(discovery(&call, &ordered, &namespaces));
    }
    let Some(tool) = tools.get(&call.name) else {
        return Ok(Outcome::error(format!("Unknown tool {:?}", call.name)));
    };
    let args_preview = preview(
        call.args
            .as_ref()
            .map(|v| v.to_string())
            .unwrap_or_default(),
        200,
    );
    let args = match call.args {
        None | Some(Value::Null) => Default::default(),
        Some(Value::Object(args)) => match serde_json::from_value(Value::Object(args)) {
            Ok(args) => args,
            Err(error) => return Ok(Outcome::error(format!("Invalid tool arguments: {error}"))),
        },
        _ => return Ok(Outcome::error("Tool arguments must be an object")),
    };
    let started = Instant::now();
    let outcome = session
        .execute_tool(parent, call.name.clone(), args, cancellation.clone(), None)
        .await
        .map_err(|e| AgentError::Callback(e.to_string()))?;
    let mut row = json!({"id":outcome.tool_call.id,"name":call.name,"args":args_preview,"status":if outcome.is_error {if cancellation.is_cancelled(){"cancelled"}else{"error"}}else{"ok"},"durationMs":started.elapsed().as_millis() as u64});
    if outcome.is_error {
        row["error"] = json!(preview(text_of(&outcome.result.content), 500));
    }
    progress.finish(call.id, row)?;
    Ok(to_script_value(tool, outcome))
}
fn to_script_value(tool: &AgentTool, outcome: AgentToolCallOutcome) -> Outcome {
    if tool.output_schema.is_some()
        && let Some(structured) = outcome.result.structured_content
    {
        return Outcome::success(
            serde_json::to_value(structured).expect("validated JSON serializes"),
        );
    }
    let text = text_of(&outcome.result.content);
    if outcome.is_error {
        Outcome::error(if text.is_empty() {
            format!("Tool {:?} failed", tool.declaration.name)
        } else {
            text
        })
    } else {
        Outcome::success(json!(text))
    }
}
fn discovery(
    call: &HostCall,
    tools: &[AgentTool],
    namespaces: &BTreeMap<String, String>,
) -> Outcome {
    let args = call
        .args
        .as_ref()
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let Some(name) = args.first().and_then(Value::as_str) else {
        return Outcome::error(format!("{}() expects a string", call.name));
    };
    match call.name.as_str() {
        "describeTool" => tools
            .iter()
            .find(|t| {
                t.declaration.name.as_str() == name
                    || pi_codemode::to_identifier(t.declaration.name.as_str()) == name
            })
            .map(|tool| Outcome::success(json!(render_declaration(tool))))
            .unwrap_or_else(Outcome::undefined),
        "describeNamespace" => {
            let matches = namespaces
                .iter()
                .filter(|(_, ns)| *ns == name || pi_codemode::to_identifier(ns) == name)
                .filter(|(tool, _)| {
                    tools
                        .iter()
                        .any(|t| t.declaration.name.as_str() == tool.as_str())
                })
                .collect::<Vec<_>>();
            if matches.is_empty() {
                Outcome::undefined()
            } else {
                Outcome::success(
                    json!({"name":matches[0].1,"tools":matches.iter().map(|(tool,_)| pi_codemode::to_identifier(tool)).collect::<Vec<_>>()}),
                )
            }
        }
        "searchTools" => {
            let limit = args
                .get(1)
                .and_then(|v| v.get("limit"))
                .unwrap_or(&Value::Null);
            let limit = if limit.is_null() {
                8
            } else if let Some(n) = safe_integer(limit).filter(|n| *n > 0) {
                usize::try_from(n).unwrap_or(usize::MAX)
            } else {
                return Outcome::error("searchTools() limit must be a positive integer");
            };
            let namespace = args
                .get(1)
                .and_then(|v| v.get("namespace"))
                .filter(|v| !v.is_null());
            if namespace.is_some_and(|v| !v.is_string()) {
                return Outcome::error("searchTools() namespace must be a string");
            }
            let candidates = tools
                .iter()
                .filter(|tool| {
                    namespace.is_none_or(|ns| {
                        namespaces
                            .get(tool.declaration.name.as_str())
                            .is_some_and(|name| {
                                name == ns.as_str().unwrap()
                                    || pi_codemode::to_identifier(name) == ns.as_str().unwrap()
                            })
                    })
                })
                .collect::<Vec<_>>();
            let ranked = rank_tools(name, &candidates, namespaces);
            Outcome::success(json!(ranked.into_iter().take(limit).map(|tool| json!({"name":pi_codemode::to_identifier(tool.declaration.name.as_str()),"description":render_declaration(tool)})).collect::<Vec<_>>()))
        }
        _ => Outcome::error("Unknown CodeMode global"),
    }
}

fn safe_integer(value: &Value) -> Option<u64> {
    value
        .as_u64()
        .filter(|v| *v <= 9_007_199_254_740_991)
        .or_else(|| {
            value
                .as_f64()
                .filter(|v| {
                    v.is_finite() && *v >= 0. && *v <= 9_007_199_254_740_991. && v.fract() == 0.
                })
                .map(|v| v as u64)
        })
}
fn tokenize(value: &str) -> Vec<String> {
    const STOP: &[&str] = &[
        "a", "an", "and", "are", "as", "at", "be", "by", "for", "from", "in", "is", "it", "of",
        "on", "or", "that", "the", "this", "to", "with",
    ];
    let chars = value.chars().collect::<Vec<_>>();
    let mut split = String::new();
    for (i, c) in chars.iter().enumerate() {
        if i > 0
            && c.is_ascii_uppercase()
            && (chars[i - 1].is_ascii_lowercase()
                || chars[i - 1].is_ascii_digit()
                || (chars[i - 1].is_ascii_uppercase()
                    && chars.get(i + 1).is_some_and(|c| c.is_ascii_lowercase())))
        {
            split.push(' ');
        }
        split.push(c.to_ascii_lowercase());
    }
    split
        .split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|s| !s.is_empty() && !STOP.contains(s))
        .map(|term| {
            if term.len() > 4 && term.ends_with("ies") {
                format!("{}y", &term[..term.len() - 3])
            } else if term.len() > 4
                && ["ches", "shes", "sses", "xes", "zes"]
                    .iter()
                    .any(|s| term.ends_with(s))
            {
                term[..term.len() - 2].into()
            } else if term.len() > 3 && term.ends_with('s') && !term.ends_with("ss") {
                term[..term.len() - 1].into()
            } else {
                term.into()
            }
        })
        .collect()
}
fn schema_text(schema: &Value, text: &mut String) {
    let Some(object) = schema.as_object() else {
        return;
    };
    if let Some(description) = object.get("description").and_then(Value::as_str) {
        text.push(' ');
        text.push_str(description);
    }
    if let Some(properties) = object.get("properties").and_then(Value::as_object) {
        for (key, value) in properties {
            text.push(' ');
            text.push_str(key);
            schema_text(value, text);
        }
    }
    if let Some(items) = object.get("items") {
        schema_text(items, text);
    }
    for key in ["anyOf", "oneOf", "allOf"] {
        if let Some(variants) = object.get(key).and_then(Value::as_array) {
            for variant in variants {
                schema_text(variant, text);
            }
        }
    }
}
/// Target Okapi BM25 (k1=1.2, b=.75), including schema text and stable ties.
fn rank_tools<'a>(
    query: &str,
    tools: &[&'a AgentTool],
    namespaces: &BTreeMap<String, String>,
) -> Vec<&'a AgentTool> {
    let terms = tokenize(query)
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>();
    if terms.is_empty() || tools.is_empty() {
        return Vec::new();
    }
    let documents = tools
        .iter()
        .map(|tool| {
            let mut text = format!(
                "{} {} {}",
                tool.declaration.name,
                tool.declaration.name.as_str().replace('_', " "),
                tool.declaration.description
            );
            schema_text(
                &serde_json::to_value(&tool.declaration.parameters)
                    .expect("validated schema serializes"),
                &mut text,
            );
            if let Some(namespace) = namespaces.get(tool.declaration.name.as_str()) {
                text.push(' ');
                text.push_str(namespace);
            }
            let mut counts = BTreeMap::<String, usize>::new();
            for term in tokenize(&text) {
                *counts.entry(term).or_default() += 1;
            }
            counts
        })
        .collect::<Vec<_>>();
    let lengths = documents
        .iter()
        .map(|d| d.values().sum::<usize>())
        .collect::<Vec<_>>();
    let average = lengths.iter().sum::<usize>() as f64 / tools.len() as f64;
    let average = if average == 0. { 1. } else { average };
    let idf = terms
        .iter()
        .map(|term| {
            let frequency = documents.iter().filter(|d| d.contains_key(term)).count() as f64;
            (
                term,
                (1. + (tools.len() as f64 - frequency + 0.5) / (frequency + 0.5)).ln(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut scores = documents
        .iter()
        .enumerate()
        .filter_map(|(i, document)| {
            let score = terms
                .iter()
                .filter_map(|term| {
                    document.get(term).map(|n| {
                        let n = *n as f64;
                        idf[term] * (n * 2.2)
                            / (n + 1.2 * (0.25 + 0.75 * lengths[i] as f64 / average))
                    })
                })
                .sum::<f64>();
            (score > 0.).then_some((score, i))
        })
        .collect::<Vec<_>>();
    scores.sort_by(|(a, i), (b, j)| b.total_cmp(a).then(i.cmp(j)));
    scores.into_iter().map(|(_, i)| tools[i]).collect()
}

/// Reconstruct only the selected canonical parent chain. Reopening executes no code.
pub fn read_codemode_store(snapshot: &SessionSnapshot) -> BTreeMap<String, Value> {
    let by_id = snapshot
        .entries
        .iter()
        .map(|entry| (entry.id.as_str(), entry))
        .collect::<BTreeMap<_, _>>();
    let mut path = Vec::new();
    let mut id = snapshot.leaf_id.as_deref();
    while let Some(entry) = id.and_then(|id| by_id.get(id)) {
        path.push(*entry);
        id = entry.parent_id.as_deref();
    }
    let mut store = BTreeMap::new();
    for entry in path.into_iter().rev() {
        if let EntryContent::Custom {
            custom_type,
            data: Some(data),
        } = &entry.content
            && custom_type == CODEMODE_STORE_ENTRY_TYPE
            && let Ok(writes) = serde_json::from_value::<StoreWrites>(data.clone())
        {
            for key in writes.delete {
                store.remove(&key);
            }
            store.extend(writes.set);
        }
    }
    store
}
pub struct ParsedCodeModeSource {
    pub code: String,
    pub timeout: Option<Duration>,
    pub max_output_tokens: Option<usize>,
}
pub fn parse_codemode_source(source: &str) -> Result<ParsedCodeModeSource, String> {
    const PREFIX: &str = "// @options:";
    if source.trim().is_empty() {
        return Err("Expected non-empty JavaScript source text".into());
    }
    for (index, line) in source.lines().enumerate().skip(1) {
        if line.trim_start().starts_with(PREFIX) {
            return Err(format!(
                "The {PREFIX} line must be the first line; found one on line {}",
                index + 1
            ));
        }
    }
    let first = source.lines().next().unwrap().trim_start();
    let mut parsed = ParsedCodeModeSource {
        code: source.into(),
        timeout: None,
        max_output_tokens: None,
    };
    if let Some(directive) = first.strip_prefix(PREFIX) {
        let fields: Value = serde_json::from_str(directive.trim())
            .map_err(|e| format!("@options must be a JSON object: {e}"))?;
        let fields = fields.as_object().ok_or("@options must be a JSON object")?;
        for (name, value) in fields {
            match name.as_str() {
                "max_output_tokens" => {
                    let n = safe_integer(value)
                        .ok_or("@options max_output_tokens must be a non-negative safe integer")?;
                    parsed.max_output_tokens = Some(
                        usize::try_from(n)
                            .map_err(|_| "@options max_output_tokens exceeds platform limit")?,
                    );
                }
                "timeout_ms" => {
                    let n = safe_integer(value)
                        .filter(|n| *n > 0 && *n <= 2_147_483_647)
                        .ok_or("@options timeout_ms must be a positive integer up to 2147483647")?;
                    parsed.timeout = Some(Duration::from_millis(n));
                }
                _ => {
                    return Err(format!(
                        "@options only supports max_output_tokens and timeout_ms; got {name:?}"
                    ));
                }
            }
        }
        let newline = source
            .find('\n')
            .ok_or("The @options line must be followed by JavaScript source")?;
        parsed.code = source[newline..].into();
        if parsed.code.trim().is_empty() {
            return Err("The @options line must be followed by JavaScript source".into());
        }
    }
    Ok(parsed)
}
async fn format_result(
    result: ExecutionResult,
    elapsed: Duration,
    fingerprint: Option<String>,
    max_tokens: usize,
    saver: &Option<McpOutputSaver>,
    call_rows: Vec<Value>,
) -> Result<(AgentToolResult, Option<StoreWrites>), AgentError> {
    let (mut output, writes, error) = match result {
        ExecutionResult::Success {
            value,
            mut output,
            store_writes,
            ..
        } => {
            if let Some(value) = value {
                output.push(OutputItem::Text {
                    text: value
                        .as_str()
                        .map(str::to_owned)
                        .unwrap_or_else(|| value.to_string()),
                    console: false,
                });
            }
            (output, Some(store_writes), None)
        }
        ExecutionResult::Error { error, output, .. } => (output, None, Some(error)),
    };
    let total = output
        .iter()
        .filter(|v| matches!(v, OutputItem::Text { console: false, .. }))
        .count();
    let mut index = 0;
    let mut console = Vec::new();
    let mut content = Vec::new();
    for item in output.drain(..) {
        match item {
            OutputItem::Text {
                text: value,
                console: true,
            } => console.push(value),
            OutputItem::Text {
                text: value,
                console: false,
            } => {
                index += 1;
                content.push(text(if total > 1 {
                    format!("==> text {index}/{total} <==\n{value}")
                } else {
                    value
                }));
            }
            OutputItem::Image { data, mime_type } => {
                content.push(InputContent::Image(pi_ai::ImageContent {
                    content_type: Default::default(),
                    data,
                    mime_type,
                }))
            }
        }
    }
    if !console.is_empty() {
        content.push(text(format!(
            "<console_output>\n{}\n</console_output>",
            console.join("\n")
        )));
    }
    if let Some(error) = &error {
        let head = match error.kind {
            pi_codemode::ErrorKind::Script => error.stack.clone().unwrap_or_else(|| {
                format!(
                    "{}: {}",
                    error.name.as_deref().unwrap_or("Error"),
                    error.message
                )
            }),
            pi_codemode::ErrorKind::Timeout => format!("Script timed out: {}", error.message),
            pi_codemode::ErrorKind::Aborted => format!("Script aborted: {}", error.message),
            pi_codemode::ErrorKind::Sandbox => format!("Script sandbox failed: {}", error.message),
        };
        let summary = if call_rows.is_empty() {
            "No tool calls were made.".into()
        } else {
            format!(
                "Tool calls made before the failure (they are not undone): {}",
                call_rows
                    .iter()
                    .map(|r| format!(
                        "{} ({})",
                        r["name"].as_str().unwrap_or(""),
                        r["status"].as_str().unwrap_or("")
                    ))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        content.push(text(format!("Script error:\n{head}\n\n{summary}")));
    }
    let mut details = json!({"calls":call_rows,"fingerprint":fingerprint});
    let combined = text_of(&content);
    let budget = max_tokens.saturating_mul(4);
    let units = combined.encode_utf16().count();
    if units > budget {
        let saved = save_output(saver, combined.as_bytes().to_vec(), ".txt".into()).await;
        let location = match saved {
            Ok(path) => {
                details["fullOutputPath"] = json!(path);
                format!("[Full output: {path} (read with offset/limit)]")
            }
            Err(error) => format!("[Could not save the full output: {error}]"),
        };
        content.retain(|v| matches!(v, InputContent::Image(_)));
        content.insert(
            0,
            text(format!(
                "Warning: truncated output (original token count: {})\nTotal output lines: {}\n\n{}\n\n{location}",
                units.div_ceil(4),combined.split('\n').count(),truncate_utf16(&combined,budget)
            )),
        );
    }
    // Save shown images through the same explicit output capability and retain
    // their model-readable image blocks.
    let mut presented = Vec::new();
    let mut labels = BTreeMap::<String, String>::new();
    for block in content {
        if let InputContent::Image(image) = &block {
            use base64::Engine;
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(&image.data)
                .map_err(|e| AgentError::Tool(format!("Invalid CodeMode image: {e}")))?;
            let extension = match image.mime_type.as_str() {
                "image/png" => ".png",
                "image/jpeg" => ".jpg",
                "image/gif" => ".gif",
                "image/webp" => ".webp",
                _ => ".bin",
            };
            let kind = format!("{}, {}", image.mime_type, format_size(bytes.len()));
            let label = if let Some(label) = labels.get(&image.data) {
                label.clone()
            } else {
                let label = match save_output(saver, bytes, extension.into()).await {
                    Ok(path) => format!("[Image saved to {path} ({kind})]"),
                    Err(error) => format!("[Image ({kind}) could not be saved: {error}]"),
                };
                labels.insert(image.data.clone(), label.clone());
                label
            };
            presented.push(text(label));
        }
        presented.push(block);
    }
    presented.insert(
        0,
        text(format!(
            "Script {}\nWall time {:.1} seconds\nOutput:\n",
            if error.is_none() {
                "completed"
            } else {
                "failed"
            },
            elapsed.as_secs_f64()
        )),
    );
    Ok((
        AgentToolResult {
            content: presented,
            details: Some(to_ai_json(details)?),
            is_error: error.map(|_| true),
            ..Default::default()
        },
        writes,
    ))
}
fn truncate_utf16(value: &str, budget: usize) -> String {
    let mut head_end = 0;
    let mut head_units = 0;
    for (i, c) in value.char_indices() {
        if head_units + c.len_utf16() > budget / 2 {
            break;
        }
        head_units += c.len_utf16();
        head_end = i + c.len_utf8();
    }
    let mut tail_start = value.len();
    let mut tail_units = 0;
    for (i, c) in value.char_indices().rev() {
        if tail_units + c.len_utf16() > budget - budget / 2 {
            break;
        }
        tail_units += c.len_utf16();
        tail_start = i;
    }
    format!(
        "{}…{} tokens truncated…{}",
        &value[..head_end],
        (value.encode_utf16().count() - head_units - tail_units).div_ceil(4),
        &value[tail_start..]
    )
}
