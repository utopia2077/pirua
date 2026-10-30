//! Detached, cursor-ordered entity projection. Runtime callbacks only enqueue.
use crate::protocol::*;
use base64::{Engine, engine::general_purpose::STANDARD};
use pi_agent::AgentEvent;
use pi_coding_runtime::{ActivityOutcome, CodingSession, LoadedResources, SessionEvent};
use pi_coding_session::{EntryContent, ProjectedEntry, SessionEntry, SessionSnapshot};
use serde_json::{Value, json};
use std::collections::BTreeMap;
pub struct Projection {
    pub epoch: String,
    pub sequence: u64,
    pub history: SessionSnapshot,
    pub state: String,
    pub stop_reason: Option<String>,
    pub entities: BTreeMap<(String, String), Value>,
    pub assistant_id: String,
    pub next_id: u64,
    pub activity: Value,
    pub settings: Value,
    pub resources: Value,
    /// The connection's advertised authentication methods as of the last login or logout.
    pub auth_methods: Value,
    pub last_outcome: ActivityOutcome,
    pub last_error: Option<String>,
    pub view: Value,
    pub native_partials: BTreeMap<String, Value>,
    pub entity_order: Vec<(String, String)>,
    active_compaction: Option<(String, Option<String>)>,
}
impl Projection {
    pub fn new(history: SessionSnapshot, loaded: &LoadedResources, settings: Value) -> Self {
        let epoch = format!(
            "{}-{}-{}",
            std::process::id(),
            history.header.id,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        Self {
            epoch,
            sequence: 0,
            history,
            state: "idle".into(),
            stop_reason: None,
            entities: BTreeMap::new(),
            assistant_id: String::new(),
            next_id: 0,
            activity: json!({"compacting":false,"retrying":false,"retryAttempt":0,"bashRunning":false}),
            settings,
            resources: resources(loaded),
            auth_methods: json!([]),
            last_outcome: ActivityOutcome::Completed,
            last_error: None,
            view: json!({}),
            native_partials: BTreeMap::new(),
            entity_order: vec![],
            active_compaction: None,
        }
    }
    pub fn id(&mut self, prefix: &str) -> String {
        self.next_id += 1;
        format!("{prefix}-{}-{}", self.epoch, self.next_id)
    }
    pub fn standard(&mut self, session_id: &str, mut update: Value, pi: bool) -> Value {
        self.materialize(&mut update);
        self.sequence += 1;
        let mut params = json!({"sessionId":session_id,"update":update});
        if pi {
            params["_meta"] = json!({CURSOR:{"epoch":self.epoch,"sequence":self.sequence}})
        }
        notification("session/update", params)
    }
    pub fn changed(
        &mut self,
        session_id: &str,
        changes: Value,
        history_change: Option<Value>,
        input: Option<Value>,
    ) -> Value {
        self.sequence += 1;
        let mut params = json!({"sessionId":session_id,"epoch":self.epoch,"sequence":self.sequence,"changes":changes});
        if let Some(h) = history_change {
            params["historyChange"] = h
        }
        if let Some(i) = input {
            params["inputResolution"] = i
        }
        notification("_pirua/session/changed", params)
    }
    pub fn state_update(&mut self, running: bool, reason: Option<String>) -> Value {
        self.state = if running { "running" } else { "idle" }.into();
        if !running {
            self.stop_reason = reason;
        } else {
            self.stop_reason = None;
            self.last_error = None;
            self.last_outcome = ActivityOutcome::Completed;
        }
        let mut v = json!({"sessionUpdate":"state_update","state":self.state});
        if !running && let Some(reason) = &self.stop_reason {
            v["stopReason"] = json!(reason);
            if reason == "error" {
                v["error"] = RpcError::failed(
                    self.last_error
                        .as_deref()
                        .unwrap_or("Native session failed"),
                )
                .value();
            }
        }
        v
    }
    fn materialize(&mut self, update: &mut Value) {
        let kind = update["sessionUpdate"].as_str().unwrap_or("").to_owned();
        let (whole, key) = match kind.as_str() {
            "agent_message_chunk" => ("agent_message", "messageId"),
            "agent_thought_chunk" => ("agent_thought", "messageId"),
            "user_message_chunk" => ("user_message", "messageId"),
            "tool_call_content_chunk" => ("tool_call_update", "toolCallId"),
            "terminal_output_chunk" => ("terminal_update", "terminalId"),
            "agent_message" | "agent_thought" | "user_message" => (kind.as_str(), "messageId"),
            "tool_call_update" => (kind.as_str(), "toolCallId"),
            "terminal_update" => (kind.as_str(), "terminalId"),
            "compaction_update" => (kind.as_str(), "compactionId"),
            _ => return,
        };
        let Some(id) = update[key].as_str() else {
            return;
        };
        let entity_key = (whole.to_owned(), id.to_owned());
        if !self.entities.contains_key(&entity_key) {
            self.entity_order.push(entity_key.clone());
        }
        let entity = self
            .entities
            .entry(entity_key)
            .or_insert_with(|| json!({"sessionUpdate":whole,key:id}));
        if kind == "terminal_output_chunk" {
            let mut bytes = entity["output"]["data"]
                .as_str()
                .and_then(|d| STANDARD.decode(d).ok())
                .unwrap_or_default();
            if let Some(d) = update["data"]
                .as_str()
                .and_then(|d| STANDARD.decode(d).ok())
            {
                bytes.extend(d);
            }
            entity["output"] = json!({"data":STANDARD.encode(bytes)});
        } else if kind.ends_with("_chunk") {
            if !entity["content"].is_array() {
                entity["content"] = json!([]);
            }
            entity["content"]
                .as_array_mut()
                .unwrap()
                .push(update["content"].clone());
            if update.get("_meta").is_some() {
                patch_meta(entity, &update["_meta"]);
            }
        } else {
            for (key, value) in update.as_object().unwrap() {
                if key == "_meta" {
                    patch_meta(entity, value);
                } else {
                    entity[key] = value.clone();
                }
            }
        }
        if (whole == "agent_message" || whole == "agent_thought")
            && let Some(native) = self.native_partials.get(id)
        {
            entity["_meta"][RENDER]["message"] = native.clone();
        }
    }
    /// Every view field, read from the live session.
    pub fn capture_view(session: &CodingSession, models: &[pi_ai::Model]) -> Value {
        Value::Object(
            VIEW.iter()
                .map(|key| {
                    let value = view_field(session, models, key).expect("view field");
                    ((*key).to_owned(), value)
                })
                .collect(),
        )
    }
    pub fn snapshot(&self, _session: &CodingSession, _models: &[pi_ai::Model]) -> Value {
        let mut snapshot = self.view.clone();
        let mut derived = Derived::default();
        for key in SNAPSHOT {
            snapshot[key] = self.field(key, &mut derived);
        }
        snapshot
    }
    /// The named snapshot fields, as `changes` for `_pirua/session/changed`, computed alone:
    /// a view field is re-read from the live session and kept in the view, and the history
    /// is projected only when a field derived from it is asked for.
    pub fn fields(
        &mut self,
        session: &CodingSession,
        models: &[pi_ai::Model],
        keys: &[&str],
    ) -> Value {
        // The view first: a derived field reads it (the context window of the model).
        let viewed = keys
            .iter()
            .map(|key| {
                let value = view_field(session, models, key);
                if let Some(value) = &value {
                    self.view[*key] = value.clone();
                }
                value
            })
            .collect::<Vec<_>>();
        let mut derived = Derived::default();
        Value::Object(
            keys.iter()
                .zip(viewed)
                .map(|(key, viewed)| {
                    let value = viewed.unwrap_or_else(|| self.field(key, &mut derived));
                    ((*key).to_owned(), value)
                })
                .collect(),
        )
    }
    /// One field the projection itself owns; anything else is null, as in a snapshot.
    fn field(&self, key: &str, derived: &mut Derived) -> Value {
        match key {
            "sessionId" => json!(self.history.header.id),
            "epoch" => json!(self.epoch),
            "sequence" => json!(self.sequence),
            "history" => json!(self.history),
            "messages" => json!(derived.context(&self.history).messages),
            "contextEntries" => json!(derived.context(&self.history).entries),
            "liveEntities" => json!(
                self.entity_order
                    .iter()
                    .filter_map(|key| self.entities.get(key).cloned())
                    .collect::<Vec<_>>()
            ),
            "state" => json!(self.state),
            "activity" => self.activity.clone(),
            "stats" => derived.statistics(&self.history, &self.view).0.clone(),
            "contextUsage" => derived.statistics(&self.history, &self.view).1.clone(),
            "settings" => self.settings.clone(),
            "resources" => self.resources.clone(),
            "authMethods" => self.auth_methods.clone(),
            _ => Value::Null,
        }
    }
    pub fn event(&mut self, event: SessionEvent) -> Vec<Value> {
        match event {
            SessionEvent::Agent(event) => self.agent(event),
            SessionEvent::AgentSettled { outcome } => {
                self.last_outcome = outcome;
                vec![]
            }
            SessionEvent::CompactionStart { .. } => {
                self.activity["compacting"] = json!(true);
                let id = self.id("compact");
                self.active_compaction = Some((id.clone(), None));
                vec![
                    json!({"sessionUpdate":"compaction_update","compactionId":id,"status":"in_progress"}),
                ]
            }
            SessionEvent::CompactionEnd {
                result,
                aborted,
                error,
                ..
            } => {
                self.activity["compacting"] = json!(false);
                let (id, entry) = self
                    .active_compaction
                    .take()
                    .unwrap_or_else(|| (self.id("compact"), None));
                let completed = !aborted && error.is_none() && result.is_some();
                let mut update = json!({"sessionUpdate":"compaction_update","compactionId":id,"status":if aborted{"cancelled"}else if error.is_some(){"failed"}else if completed{"completed"}else{"cancelled"},"summary":result.map(|r|vec![json!({"type":"text","text":r.summary})]),"error":error});
                if completed && let Some(entry) = entry {
                    update["_meta"] = json!({RENDER:{"phase":"end","entryId":entry}});
                }
                vec![update]
            }
            SessionEvent::SessionCompact { entry, .. } => {
                if let Some((_, committed)) = &mut self.active_compaction {
                    *committed = Some(entry.id);
                }
                vec![]
            }
            SessionEvent::AutoRetryStart { attempt, .. } => {
                self.activity["retrying"] = json!(true);
                self.activity["retryAttempt"] = json!(attempt);
                vec![]
            }
            SessionEvent::AutoRetryEnd { .. } => {
                self.activity["retrying"] = json!(false);
                vec![]
            }
            SessionEvent::ModelSelect { model, .. } => {
                self.view["model"] = serde_json::to_value(model).unwrap();
                vec![]
            }
            SessionEvent::ThinkingLevelSelect { level } => {
                self.view["thinkingLevel"] = json!(level);
                vec![]
            }
            SessionEvent::SessionInfoChanged { name } => {
                vec![json!({"sessionUpdate":"session_info_update","title":name})]
            }
            SessionEvent::BashExecutionUpdate { id, delta } => vec![
                json!({"sessionUpdate":"terminal_output_chunk","terminalId":id.unwrap_or_else(||"shell".into()),"data":STANDARD.encode(delta)}),
            ],
            _ => vec![],
        }
    }
    fn agent(&mut self, event: AgentEvent) -> Vec<Value> {
        match event {
            AgentEvent::MessageStart { message } if message.role() == "assistant" => {
                // Every assistant attempt commits before the next MessageStart.
                // Count the complete append log, including inactive tree branches,
                // so unrelated entries and owner reconstruction cannot reuse IDs.
                self.assistant_id =
                    format!("assistant-{}", assistant_entries(&self.history).count() + 1);
                let id = &self.assistant_id;
                let message = serde_json::to_value(message).unwrap();
                self.native_partials
                    .insert(self.assistant_id.clone(), message.clone());
                vec![
                    json!({"sessionUpdate":"agent_message","messageId":id,"content":[],"_meta":{RENDER:{"phase":"start","nativeMessageId":id,"message":message}}}),
                ]
            }
            AgentEvent::MessageUpdate {
                message,
                assistant_message_event,
            } => {
                self.native_partials.insert(
                    self.assistant_id.clone(),
                    serde_json::to_value(&message).unwrap(),
                );
                let event = serde_json::to_value(assistant_message_event).unwrap();
                let kind = event["type"].as_str().unwrap_or("");
                let id = &self.assistant_id;
                if kind == "text_delta" || kind == "thinking_delta" {
                    let thought = kind == "thinking_delta";
                    vec![
                        json!({"sessionUpdate":if thought{"agent_thought_chunk"}else{"agent_message_chunk"},"messageId":id,"content":{"type":"text","text":event["delta"]},"_meta":{RENDER:{"phase":"delta","nativeMessageId":id,"contentIndex":event["contentIndex"],"deltaType":kind}}}),
                    ]
                } else if kind == "toolcall_start"
                    || kind == "toolcall_delta"
                    || kind == "toolcall_end"
                {
                    let m = serde_json::to_value(message).unwrap();
                    m["content"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .filter(|c| c["type"] == "toolCall")
                        .map(|c| {
                            self.tool(
                                c["id"].as_str().unwrap_or("tool"),
                                c["name"].as_str().unwrap_or("tool"),
                                "pending",
                                Some(c["arguments"].clone()),
                                None,
                                None,
                            )
                        })
                        .collect()
                } else {
                    vec![]
                }
            }
            AgentEvent::MessageEnd { message } if message.role() == "assistant" => {
                let message = serde_json::to_value(message).unwrap();
                self.native_partials
                    .insert(self.assistant_id.clone(), message.clone());
                self.stop_reason = Some(
                    match message["stopReason"].as_str() {
                        Some("length") => "max_tokens",
                        Some("aborted") => "cancelled",
                        Some("error") => "error",
                        _ => "end_turn",
                    }
                    .into(),
                );
                self.last_error = message["errorMessage"].as_str().map(str::to_owned);
                message_updates(&self.assistant_id, &message, "end", None)
            }
            AgentEvent::ToolExecutionStart {
                tool_call_id,
                tool_name,
                args,
            } => vec![self.tool(
                &tool_call_id,
                &tool_name,
                "in_progress",
                Some(serde_json::to_value(args).unwrap()),
                None,
                None,
            )],
            AgentEvent::ToolExecutionUpdate {
                tool_call_id,
                tool_name,
                args,
                partial_result,
            } => vec![self.tool(
                &tool_call_id,
                &tool_name,
                "in_progress",
                Some(serde_json::to_value(args).unwrap()),
                Some(serde_json::to_value(partial_result).unwrap()),
                None,
            )],
            AgentEvent::ToolExecutionEnd {
                tool_call_id,
                tool_name,
                result,
                is_error,
                duration_ms,
            } => vec![self.tool(
                &tool_call_id,
                &tool_name,
                if is_error { "failed" } else { "completed" },
                None,
                Some(serde_json::to_value(result).unwrap()),
                duration_ms,
            )],
            _ => vec![],
        }
    }
    fn tool(
        &self,
        id: &str,
        name: &str,
        status: &str,
        input: Option<Value>,
        output: Option<Value>,
        duration: Option<u64>,
    ) -> Value {
        let parent = &self.assistant_id;
        let mut v = json!({"sessionUpdate":"tool_call_update","toolCallId":format!("{parent}:{id}"),"name":name,"title":name,"kind":match name{"read"=>"read","edit"|"write"=>"edit","bash"|"powershell"=>"execute",_=>"other"},"status":status,"_meta":{RENDER:{"phase":if status=="completed"||status=="failed"{"end"}else{"delta"},"nativeMessageId":parent,"parentMessageId":parent,"toolCallId":id,"durationMs":duration}}});
        if let Some(input) = input {
            v["rawInput"] = input;
        }
        if let Some(output) = output {
            v["content"] = json!(
                output["content"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter(|c| c["type"] == "text" || c["type"] == "image")
                    .map(|c| json!({"type":"content","content":c}))
                    .collect::<Vec<_>>()
            );
            v["rawOutput"] = output;
        }
        v
    }
}
fn patch_meta(target: &mut Value, patch: &Value) {
    if patch.is_null() {
        target["_meta"] = Value::Null;
    } else if let Some(patch) = patch.as_object() {
        if !target["_meta"].is_object() {
            target["_meta"] = json!({});
        }
        for (k, v) in patch {
            if v.is_null() {
                target["_meta"].as_object_mut().unwrap().remove(k);
            } else {
                target["_meta"][k] = v.clone();
            }
        }
    }
}

/// The fields of the live view, in snapshot order.
const VIEW: [&str; 9] = [
    "model",
    "routedModel",
    "thinkingLevel",
    "availableThinkingLevels",
    "models",
    "scopedModels",
    "configOptions",
    "queue",
    "tools",
];
/// The fields the projection owns, in snapshot order, after the view's.
const SNAPSHOT: [&str; 14] = [
    "sessionId",
    "epoch",
    "sequence",
    "history",
    "messages",
    "contextEntries",
    "liveEntities",
    "state",
    "activity",
    "stats",
    "contextUsage",
    "settings",
    "resources",
    "authMethods",
];
fn view_field(session: &CodingSession, models: &[pi_ai::Model], key: &str) -> Option<Value> {
    Some(match key {
        "model" => json!(session.model()),
        "routedModel" => json!(session.routed_model().map(|(m, _)| m)),
        "thinkingLevel" => json!(session.thinking_level()),
        "availableThinkingLevels" => json!(session.available_thinking_levels()),
        "models" => json!(models),
        "scopedModels" => json!(
            session
                .scoped_models()
                .iter()
                .map(|m| json!({"model":m.model,"thinkingLevel":m.thinking_level}))
                .collect::<Vec<_>>()
        ),
        "configOptions" => config(session, models),
        "queue" => {
            let (steer, follow) = session.queued_inputs();
            json!({"steering":steer.iter().map(queued).collect::<Vec<_>>(),"followUp":follow.iter().map(queued).collect::<Vec<_>>()})
        }
        "tools" => json!(
            session
                .all_tools()
                .into_iter()
                .map(|t| json!({"declaration":t.declaration,"exposure":match t.exposure{pi_coding_runtime::ToolExposure::Direct=>"direct",pi_coding_runtime::ToolExposure::ModelOnly=>"modelOnly",pi_coding_runtime::ToolExposure::CodeMode=>"codeMode",pi_coding_runtime::ToolExposure::Deferred=>"deferred",pi_coding_runtime::ToolExposure::Hidden=>"hidden"},"source":t.source,"namespace":t.namespace}))
                .collect::<Vec<_>>()
        ),
        _ => return None,
    })
}
/// What the fields of one request derive from the history, each computed once and only on use.
#[derive(Default)]
struct Derived {
    context: Option<pi_coding_session::SessionProjection>,
    statistics: Option<(Value, Value)>,
}
impl Derived {
    fn context(&mut self, history: &SessionSnapshot) -> &pi_coding_session::SessionProjection {
        self.context.get_or_insert_with(|| project_context(history))
    }
    fn statistics(&mut self, history: &SessionSnapshot, view: &Value) -> &(Value, Value) {
        if self.statistics.is_none() {
            let window = view["model"]["contextWindow"].as_u64().unwrap_or(0);
            let statistics = statistics(history, self.context(history), window);
            self.statistics = Some(statistics);
        }
        self.statistics.as_ref().expect("computed")
    }
}
#[cfg(test)]
thread_local! {
    /// Projections of a whole history made by this thread: the work a field costs.
    pub static PROJECTIONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}
/// The context a history projects to; the whole history is walked and copied.
pub fn project_context(history: &SessionSnapshot) -> pi_coding_session::SessionProjection {
    #[cfg(test)]
    PROJECTIONS.with(|count| count.set(count.get() + 1));
    pi_coding_session::project(history).expect("canonical observed history")
}
/// The context entries an append adds, when everything already projected stays as it is: the
/// entries continue the path from the previous leaf, and none replaces the context before it
/// (a compaction) or rewrites an earlier entry (a context edit). `None` means the whole
/// context must be sent again.
pub fn context_extension(
    previous_leaf: Option<&str>,
    appended: &[SessionEntry],
    leaf: Option<&str>,
) -> Option<Vec<ProjectedEntry>> {
    let mut parent = previous_leaf;
    for entry in appended {
        if entry.parent_id.as_deref() != parent
            || matches!(
                entry.content,
                EntryContent::Compaction { .. } | EntryContent::ContextEdit { .. }
            )
        {
            return None;
        }
        parent = Some(&entry.id);
    }
    (parent == leaf).then(|| {
        appended
            .iter()
            .map(|entry| ProjectedEntry {
                source_entry: entry.clone(),
                messages: pi_coding_session::project_entry(entry),
            })
            .collect()
    })
}

pub fn queued(q: &pi_coding_runtime::QueuedInput) -> Value {
    json!({"submissionId":q.id.to_string(),"delivery":q.delivery,"text":q.text,"images":q.images})
}
pub fn resources(loaded: &LoadedResources) -> Value {
    json!({"contextFiles":loaded.context_files,"skills":loaded.resources.skills.clone().unwrap_or_default(),"promptTemplates":loaded.resources.prompt_templates.clone().unwrap_or_default(),"diagnostics":loaded.diagnostics.iter().map(|d|json!({"path":d.path,"message":d.message,"winnerPath":d.winner_path})).collect::<Vec<_>>()})
}
pub fn message_updates(id: &str, message: &Value, phase: &str, entry: Option<&str>) -> Vec<Value> {
    let role = message["role"].as_str().unwrap_or("");
    let mut text = Vec::new();
    let mut thought = Vec::new();
    if let Some(s) = message["content"].as_str() {
        text.push(json!({"type":"text","text":s}));
    } else if let Some(content) = message["content"].as_array() {
        for c in content {
            match c["type"].as_str() {
                Some("text" | "image") => text.push(c.clone()),
                Some("thinking") => thought.push(json!({"type":"text","text":c["thinking"]})),
                _ => {}
            }
        }
    }
    let mut updates = Vec::new();
    if !thought.is_empty() {
        updates.push(json!({"sessionUpdate":"agent_thought","messageId":id,"content":thought,"_meta":{RENDER:{"phase":phase,"nativeMessageId":id,"entryId":entry,"message":message}}}));
    }
    if role == "user" || role == "assistant" {
        updates.push(json!({"sessionUpdate":if role=="user"{"user_message"}else{"agent_message"},"messageId":id,"content":text,"_meta":{RENDER:{"phase":phase,"nativeMessageId":id,"entryId":entry,"message":message}}}));
    }
    if role == "assistant" {
        for c in message["content"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|c| c["type"] == "toolCall")
        {
            updates.push(json!({"sessionUpdate":"tool_call_update","toolCallId":format!("{}:{}",id,c["id"].as_str().unwrap_or("tool")),"name":c["name"],"title":c["name"],"status":"pending","rawInput":c["arguments"],"_meta":{RENDER:{"phase":phase,"nativeMessageId":id,"parentMessageId":id,"toolCallId":c["id"]}}}));
        }
    }
    updates
}
pub fn config(session: &CodingSession, models: &[pi_ai::Model]) -> Value {
    json!([
    {"configId":"model","name":"Model","category":"model","type":"select","currentValue":format!("{}/{}",session.model().provider,session.model().id),"options":models.iter().map(|m|json!({"value":format!("{}/{}",m.provider,m.id),"name":format!("{} ({})",m.name,m.provider)})).collect::<Vec<_>>()},
    {"configId":"thinkingLevel","name":"Thinking","category":"thought_level","type":"select","currentValue":session.thinking_level(),"options":session.available_thinking_levels().iter().map(|v|json!({"value":v,"name":serde_json::to_value(v).unwrap().as_str().unwrap()})).collect::<Vec<_>>()},
    {"configId":"steeringMode","name":"Steering delivery","type":"select","currentValue":session.agent().steering_mode(),"options":[{"value":"one-at-a-time","name":"One at a time"},{"value":"all","name":"All"}]},
    {"configId":"followUpMode","name":"Follow-up delivery","type":"select","currentValue":session.agent().follow_up_mode(),"options":[{"value":"one-at-a-time","name":"One at a time"},{"value":"all","name":"All"}]},
    {"configId":"autoCompaction","name":"Automatic compaction","type":"boolean","currentValue":session.auto_compaction_enabled()},
    {"configId":"autoRetry","name":"Automatic retry","type":"boolean","currentValue":session.auto_retry_enabled()}
    ])
}
pub fn retained_branch(snapshot: &SessionSnapshot) -> Vec<pi_coding_session::SessionEntry> {
    let by_id = snapshot
        .entries
        .iter()
        .map(|e| (e.id.as_str(), e))
        .collect::<BTreeMap<_, _>>();
    let mut path = Vec::new();
    let mut id = snapshot.leaf_id.as_deref();
    while let Some(e) = id.and_then(|id| by_id.get(id)) {
        path.push((*e).clone());
        id = e.parent_id.as_deref();
    }
    path.reverse();
    path
}
pub fn replay(snapshot: &SessionSnapshot) -> Vec<Value> {
    let mut out = Vec::new();
    let mut tools = BTreeMap::new();
    let assistant_ids = assistant_entries(snapshot)
        .enumerate()
        .map(|(index, entry)| (entry.id.as_str(), format!("assistant-{}", index + 1)))
        .collect::<BTreeMap<_, _>>();
    for entry in retained_branch(snapshot) {
        if let EntryContent::Compaction { summary, .. } = &entry.content {
            out.push(json!({"sessionUpdate":"compaction_update","compactionId":entry.id,"status":"completed","summary":[{"type":"text","text":summary}],"_meta":{RENDER:{"phase":"end","nativeMessageId":entry.id,"entryId":entry.id}}}));
            continue;
        }
        if let EntryContent::BranchSummary { summary, .. } = &entry.content {
            out.push(json!({"sessionUpdate":"agent_message","messageId":entry.id,"content":[{"type":"text","text":summary}],"_meta":{RENDER:{"phase":"end","nativeMessageId":entry.id,"entryId":entry.id}}}));
            continue;
        }
        if let EntryContent::Message { message } = entry.content {
            let native_id = assistant_ids.get(entry.id.as_str()).unwrap_or(&entry.id);
            if message["role"] == "assistant" {
                for c in message["content"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter(|c| c["type"] == "toolCall")
                {
                    tools.insert(c["id"].as_str().unwrap_or("").to_owned(), native_id.clone());
                }
            }
            if message["role"] == "bashExecution" {
                out.push(json!({"sessionUpdate":"terminal_update","terminalId":format!("entry:{}",entry.id),"command":message["command"],"cwd":snapshot.header.cwd,"output":{"data":STANDARD.encode(message["output"].as_str().unwrap_or(""))},"exitStatus":{"exitCode":message["exitCode"]},"_meta":{RENDER:{"phase":"end","nativeMessageId":entry.id,"entryId":entry.id,"message":message}}}));
            } else if message["role"] == "toolResult" {
                let call = message["toolCallId"].as_str().unwrap_or("tool");
                let parent = tools.get(call).cloned().unwrap_or_default();
                out.push(json!({"sessionUpdate":"tool_call_update","toolCallId":format!("{parent}:{call}"),"name":message["toolName"],"status":if message["isError"]==true{"failed"}else{"completed"},"rawOutput":message,"content":message["content"].as_array().into_iter().flatten().filter(|c|c["type"]=="text"||c["type"]=="image").map(|c|json!({"type":"content","content":c})).collect::<Vec<_>>(),"_meta":{RENDER:{"phase":"end","nativeMessageId":parent,"parentMessageId":parent,"toolCallId":call,"entryId":entry.id}}}));
            } else {
                out.extend(message_updates(native_id, &message, "end", Some(&entry.id)));
            }
        }
    }
    out
}

fn assistant_entries(
    snapshot: &SessionSnapshot,
) -> impl Iterator<Item = &pi_coding_session::SessionEntry> {
    snapshot.entries.iter().filter(|entry| matches!(&entry.content, EntryContent::Message { message } if message["role"] == "assistant"))
}

fn statistics(
    history: &SessionSnapshot,
    context: &pi_coding_session::SessionProjection,
    window: u64,
) -> (Value, Value) {
    let mut stats = json!({"userMessages":0,"assistantMessages":0,"toolCalls":0,"toolResults":0,"totalMessages":0,"usage":null});
    let mut usage = Value::Null;
    for entry in &history.entries {
        let value = match &entry.content {
            EntryContent::Message { message } => {
                let total = stats["totalMessages"].as_u64().unwrap();
                stats["totalMessages"] = json!(total + 1);
                let key = match message["role"].as_str() {
                    Some("user") => Some("userMessages"),
                    Some("assistant") => Some("assistantMessages"),
                    Some("toolResult") => Some("toolResults"),
                    _ => None,
                };
                if let Some(key) = key {
                    stats[key] = json!(stats[key].as_u64().unwrap() + 1);
                }
                if message["role"] == "assistant" {
                    stats["toolCalls"] = json!(
                        stats["toolCalls"].as_u64().unwrap()
                            + message["content"]
                                .as_array()
                                .into_iter()
                                .flatten()
                                .filter(|b| b["type"] == "toolCall")
                                .count() as u64
                    );
                }
                message.get("usage")
            }
            EntryContent::Usage { usage, .. } => Some(usage),
            EntryContent::Compaction { usage, .. } | EntryContent::BranchSummary { usage, .. } => {
                usage.as_ref()
            }
            _ => None,
        };
        if let Some(value) = value {
            if usage.is_null() {
                usage = value.clone();
            } else {
                for key in [
                    "input",
                    "output",
                    "cacheRead",
                    "cacheWrite",
                    "totalTokens",
                    "cacheWrite1h",
                    "reasoning",
                ] {
                    if usage.get(key).is_some() || value.get(key).is_some() {
                        usage[key] = json!(
                            (usage[key].as_u64().unwrap_or(0) + value[key].as_u64().unwrap_or(0))
                                .min(pi_ai::JS_SAFE_INTEGER_MAX)
                        );
                    }
                }
                for key in ["input", "output", "cacheRead", "cacheWrite", "total"] {
                    usage["cost"][key] = json!(
                        usage["cost"][key].as_f64().unwrap_or(0.0)
                            + value["cost"][key].as_f64().unwrap_or(0.0)
                    );
                }
            }
        }
    }
    stats["usage"] = usage;
    let branch = retained_branch(history);
    let estimate = pi_coding_runtime::estimate_projected_context_tokens(context, &branch);
    let compacted = branch
        .iter()
        .rposition(|e| matches!(e.content, EntryContent::Compaction { .. }));
    let unknown = compacted.is_some_and(|i| {
        !branch[i + 1..].iter().any(|e| {
            context.entries.iter().any(|p| {
                p.source_entry.id == e.id && p.messages.iter().any(|m| m["usage"].is_object())
            })
        })
    });
    let context_usage = if window == 0 {
        Value::Null
    } else if unknown {
        json!({"tokens":null,"contextWindow":window,"percent":null})
    } else {
        json!({"tokens":estimate.tokens,"contextWindow":window,"percent":estimate.tokens as f64/window as f64*100.0})
    };
    stats["contextUsage"] = context_usage.clone();
    (stats, context_usage)
}

/// Only detached execution settings cross the protocol. Unknown host settings
/// may contain credential configuration and must never become renderer metadata.
pub fn display_settings(raw: &Value) -> Value {
    fn select(raw: &Value, template: &Value) -> Value {
        if let Some(fields) = template.as_object() {
            Value::Object(
                fields
                    .iter()
                    .map(|(key, value)| (key.clone(), select(&raw[key], value)))
                    .collect(),
            )
        } else if (template.is_string() && raw.is_string())
            || (template.is_boolean() && raw.is_boolean())
            || (template.is_number() && raw.is_number())
        {
            raw.clone()
        } else {
            template.clone()
        }
    }
    select(raw, &pi_coding_runtime::headless_settings_defaults())
}

#[cfg(test)]
mod tests {
    use super::*;
    use pi_coding_runtime::{CompactionReason, CompactionResult};
    use pi_coding_session::{MemoryStore, SessionManager};
    use std::sync::Arc;

    fn projection() -> Projection {
        let manager = SessionManager::create(Arc::new(MemoryStore::new()), "/synthetic").unwrap();
        Projection::new(
            manager.snapshot().clone(),
            &LoadedResources::default(),
            json!({}),
        )
    }

    #[test]
    fn compaction_completion_preserves_its_start_id_and_exact_committed_entry() {
        let mut p = projection();
        let start = p.event(SessionEvent::CompactionStart {
            reason: CompactionReason::Manual,
            will_retry: false,
        });
        p.id("unrelated");
        let entry = serde_json::from_value(json!({"id":"this-compaction","parentId":null,"timestamp":1,"type":"compaction","summary":"new summary","firstKeptEntryId":"kept","tokensBefore":100})).unwrap();
        p.event(SessionEvent::SessionCompact {
            entry,
            reason: CompactionReason::Manual,
            from_hook: true,
        });
        let end = p.event(SessionEvent::CompactionEnd {
            reason: CompactionReason::Manual,
            result: Some(CompactionResult {
                summary: "new summary".into(),
                first_kept_entry_id: Some("kept".into()),
                tokens_before: 100,
                details: None,
                usage: None,
            }),
            aborted: false,
            will_retry: false,
            error: None,
        });
        assert_eq!(start[0]["compactionId"], end[0]["compactionId"]);
        assert_eq!(end[0]["status"], "completed");
        assert_eq!(end[0]["_meta"][RENDER]["entryId"], "this-compaction");
        assert_eq!(
            end[0]["summary"],
            json!([{"type":"text","text":"new summary"}])
        );
        let start = p.event(SessionEvent::CompactionStart {
            reason: CompactionReason::Manual,
            will_retry: false,
        });
        let end = p.event(SessionEvent::CompactionEnd {
            reason: CompactionReason::Manual,
            result: None,
            aborted: true,
            will_retry: false,
            error: None,
        });
        assert_eq!(start[0]["compactionId"], end[0]["compactionId"]);
        assert_eq!(end[0]["status"], "cancelled");
        assert!(end[0]["_meta"][RENDER].get("entryId").is_none());
    }

    #[test]
    fn compaction_start_remains_in_the_materialized_live_projection() {
        let mut p = projection();
        let start = p.event(SessionEvent::CompactionStart {
            reason: CompactionReason::Manual,
            will_retry: false,
        });
        p.standard("synthetic", start[0].clone(), true);
        assert_eq!(
            p.entities.get(&(
                "compaction_update".into(),
                start[0]["compactionId"].as_str().unwrap().into()
            )),
            Some(&start[0])
        );
    }

    #[test]
    fn retained_branch_replay_and_new_streams_use_the_complete_assistant_log() {
        let mut manager =
            SessionManager::create(Arc::new(MemoryStore::new()), "/synthetic").unwrap();
        let user = manager
            .append_message(json!({"role":"user","content":"branch point"}))
            .unwrap();
        manager
            .append_message(
                json!({"role":"assistant","content":[{"type":"text","text":"inactive response"}]}),
            )
            .unwrap();
        manager.branch(Some(&user)).unwrap();
        let active = manager
            .append_message(
                json!({"role":"assistant","content":[{"type":"text","text":"active response"}]}),
            )
            .unwrap();
        manager
            .set_name("non-assistant entries do not change identity")
            .unwrap();
        manager.set_label(&active, Some("retained".into())).unwrap();
        let mut p = Projection::new(
            manager.snapshot().clone(),
            &LoadedResources::default(),
            json!({}),
        );
        let updates = replay(&p.history);
        let assistant = updates
            .iter()
            .find(|update| update["sessionUpdate"] == "agent_message")
            .unwrap();
        assert_eq!(assistant["messageId"], "assistant-2");
        assert_eq!(assistant["_meta"][RENDER]["entryId"], active);
        let message = pi_agent::AgentMessage::BuiltIn(pi_ai::Message::Assistant(Box::new(
            pi_agent::failure_message(
                &crate::fixture::model(),
                &pi_agent::AgentError::Callback(String::new()),
                false,
            ),
        )));
        assert_eq!(
            p.event(SessionEvent::Agent(AgentEvent::MessageStart { message }))[0]["messageId"],
            "assistant-3"
        );
    }

    #[test]
    fn live_operation_ids_do_not_repeat_when_an_owner_is_reconstructed() {
        let mut first = projection();
        let mut second = Projection::new(
            first.history.clone(),
            &LoadedResources::default(),
            json!({}),
        );
        for prefix in ["terminal", "command", "compact"] {
            assert_ne!(first.id(prefix), second.id(prefix));
        }
    }

    fn usage(input: u64) -> Value {
        json!({"input":input,"output":1,"cacheRead":0,"cacheWrite":0,"totalTokens":input + 1,"cost":{"input":0.5,"output":0.25,"cacheRead":0,"cacheWrite":0,"total":0.75}})
    }
    fn assistant(text: &str, input: u64) -> Value {
        let model = crate::fixture::model();
        let mut message = serde_json::to_value(pi_agent::failure_message(
            &model,
            &pi_agent::AgentError::Callback(String::new()),
            false,
        ))
        .unwrap();
        message["content"] = json!([{"type":"text","text":text},{"type":"toolCall","id":"c1","name":"read","arguments":{}}]);
        message["usage"] = usage(input);
        message["stopReason"] = json!("toolUse");
        message.as_object_mut().unwrap().remove("errorMessage");
        message
    }
    fn user(text: &str) -> Value {
        json!({"role":"user","content":text,"timestamp":1})
    }
    /// A history that uses every rule the projection and its statistics have: usage, tool
    /// calls, a compaction with a kept tail, a context edit, a label, a name and a summary.
    fn busy(store: Arc<dyn pi_coding_session::SessionStore>) -> SessionManager {
        let mut manager = SessionManager::create(store, "/synthetic").unwrap();
        manager.set_model("openai", "fixture").unwrap();
        let first = manager.append_message(user("one")).unwrap();
        manager.append_message(assistant("two", 10)).unwrap();
        manager.append_message(json!({"role":"toolResult","toolCallId":"c1","toolName":"read","content":[{"type":"text","text":"three"}],"isError":false,"timestamp":1})).unwrap();
        let kept = manager.append_message(user("four")).unwrap();
        manager.append_message(assistant("five", 20)).unwrap();
        manager
            .append_compaction(pi_coding_session::CompactionOptions {
                summary: "so far".into(),
                first_kept_entry_id: Some(kept),
                tokens_before: 100,
                usage: Some(usage(7)),
                ..Default::default()
            })
            .unwrap();
        manager.append_message(user("six")).unwrap();
        let last = manager.append_message(assistant("seven", 30)).unwrap();
        manager
            .edit_context(
                &last,
                Some(pi_coding_session::ContentReplacement {
                    content: json!("edited"),
                }),
            )
            .unwrap();
        manager.set_label(&first, Some("start".into())).unwrap();
        manager.set_name("busy").unwrap();
        manager.set_thinking_level("high").unwrap();
        manager
    }
    async fn live(manager: SessionManager, model: &pi_ai::Model) -> CodingSession {
        pi_coding_runtime::CodingSessionBuilder::new(
            manager,
            model.clone(),
            crate::fixture::stream(1),
        )
        .with_model_runtime(Arc::new(pi_coding_runtime::StaticModelRuntime::new(vec![
            model.clone(),
        ])))
        .with_hooks(Arc::new(crate::fixture::Hooks))
        .build()
        .await
        .unwrap()
    }
    /// The view as the whole-snapshot code built it, before each field could be read alone.
    fn reference_view(session: &CodingSession, models: &[pi_ai::Model]) -> Value {
        let (steer, follow) = session.queued_inputs();
        json!({"model":session.model(),"routedModel":session.routed_model().map(|(m,_)|m),"thinkingLevel":session.thinking_level(),"availableThinkingLevels":session.available_thinking_levels(),"models":models,"scopedModels":session.scoped_models().iter().map(|m|json!({"model":m.model,"thinkingLevel":m.thinking_level})).collect::<Vec<_>>(),"configOptions":config(session,models),"queue":{"steering":steer.iter().map(queued).collect::<Vec<_>>(),"followUp":follow.iter().map(queued).collect::<Vec<_>>()},"tools":session.all_tools().into_iter().map(|t|json!({"declaration":t.declaration,"exposure":match t.exposure{pi_coding_runtime::ToolExposure::Direct=>"direct",pi_coding_runtime::ToolExposure::ModelOnly=>"modelOnly",pi_coding_runtime::ToolExposure::CodeMode=>"codeMode",pi_coding_runtime::ToolExposure::Deferred=>"deferred",pi_coding_runtime::ToolExposure::Hidden=>"hidden"},"source":t.source,"namespace":t.namespace})).collect::<Vec<_>>()})
    }
    /// The snapshot as it was built whole, before fields could be computed one at a time.
    fn reference_snapshot(p: &Projection) -> Value {
        let context = pi_coding_session::project(&p.history).expect("canonical observed history");
        let mut snapshot = p.view.clone();
        let (stats, context_usage) = statistics(
            &p.history,
            &context,
            p.view["model"]["contextWindow"].as_u64().unwrap_or(0),
        );
        for (key,value) in json!({"sessionId":p.history.header.id,"epoch":p.epoch,"sequence":p.sequence,"history":p.history,"messages":context.messages,"contextEntries":context.entries,"liveEntities":p.entity_order.iter().filter_map(|key|p.entities.get(key).cloned()).collect::<Vec<_>>(),"state":p.state,"activity":p.activity,"stats":stats,"contextUsage":context_usage,"settings":p.settings,"resources":p.resources,"authMethods":p.auth_methods}).as_object().unwrap(){snapshot[key]=value.clone();}
        snapshot
    }
    async fn busy_projection() -> (Projection, CodingSession, Vec<pi_ai::Model>) {
        let store: Arc<dyn pi_coding_session::SessionStore> = Arc::new(MemoryStore::new());
        let manager = busy(store.clone());
        let model = crate::fixture::model();
        let models = vec![model.clone()];
        let session = live(
            SessionManager::open(store, &manager.header().id).unwrap(),
            &model,
        )
        .await;
        let mut p = Projection::new(
            manager.snapshot().clone(),
            &LoadedResources::default(),
            json!({"retry":{"enabled":true}}),
        );
        p.view = Projection::capture_view(&session, &models);
        p.auth_methods = json!([{"methodId":"openai:apiKey"}]);
        p.state = "running".into();
        p.standard(
            "synthetic",
            json!({"sessionUpdate":"agent_message_chunk","messageId":"live-1","content":{"type":"text","text":"streaming"}}),
            true,
        );
        (p, session, models)
    }

    #[tokio::test]
    async fn the_snapshot_and_its_view_are_byte_identical_to_the_whole_ones_they_replaced() {
        let (p, session, models) = busy_projection().await;
        let (view, reference) = (
            Projection::capture_view(&session, &models),
            reference_view(&session, &models),
        );
        assert!(view["tools"].is_array() && view["models"].as_array().unwrap().len() == 1);
        assert_eq!(view.to_string(), reference.to_string());
        let snapshot = p.snapshot(&session, &models);
        assert!(snapshot["stats"]["usage"]["input"].as_u64().unwrap() > 0);
        assert!(snapshot["contextUsage"].is_object());
        assert_eq!(snapshot.to_string(), reference_snapshot(&p).to_string());
    }

    #[tokio::test]
    async fn a_field_costs_only_the_work_it_needs_and_equals_its_part_of_the_snapshot() {
        let (mut p, session, models) = busy_projection().await;
        let projections = || PROJECTIONS.with(std::cell::Cell::get);
        let snapshot = p.snapshot(&session, &models);
        assert_eq!(projections(), 1, "the snapshot projects the history once");
        let before = projections();
        let changes = p.fields(
            &session,
            &models,
            &[
                "queue",
                "state",
                "activity",
                "scopedModels",
                "liveEntities",
                "settings",
            ],
        );
        assert_eq!(projections(), before, "no history is projected for these");
        let before = projections();
        let derived = p.fields(
            &session,
            &models,
            &["messages", "contextEntries", "stats", "contextUsage"],
        );
        assert_eq!(projections(), before + 1, "one projection serves all four");
        let before = projections();
        let one = p.fields(&session, &models, &["stats"]);
        assert_eq!(projections(), before + 1);
        for part in [&changes, &derived, &one] {
            for (key, value) in part.as_object().unwrap() {
                assert_eq!(value, &snapshot[key.as_str()], "{key}");
            }
        }
        // Every field a snapshot has can be asked for alone.
        for key in VIEW.iter().chain(SNAPSHOT.iter()) {
            let alone = p.fields(&session, &models, &[key]);
            assert_eq!(alone[*key], snapshot[*key], "{key}");
        }
    }

    #[test]
    fn an_append_extends_the_context_only_when_nothing_before_it_changes() {
        let mut manager =
            SessionManager::create(Arc::new(MemoryStore::new()), "/synthetic").unwrap();
        let mut observed = manager.observe_history();
        let mut history = observed.snapshot.clone();
        let mut context = pi_coding_session::project(&history).unwrap().entries;
        let mut seed = 0x9e37_79b9_7f4a_7c15_u64;
        let mut roll = |bound: usize| {
            seed = seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (seed >> 33) as usize % bound
        };
        let (mut extended, mut resent) = (0, 0);
        for step in 0..400 {
            let all = manager
                .entries()
                .iter()
                .map(|e| e.id.clone())
                .collect::<Vec<_>>();
            let path = manager
                .get_branch()
                .iter()
                .map(|e| e.id.clone())
                .collect::<Vec<_>>();
            // Some(true): the step replaces or rewrites context; Some(false): it only adds to its end.
            let (kind, restructures) = match roll(12) {
                0 | 1 => {
                    manager
                        .append_message(user(&format!("user {step}")))
                        .unwrap();
                    ("user", Some(false))
                }
                2 | 3 => {
                    manager
                        .append_message(assistant(&format!("reply {step}"), step))
                        .unwrap();
                    ("assistant", Some(false))
                }
                4 => {
                    manager
                        .set_model("openai", format!("model-{step}"))
                        .unwrap();
                    ("model", Some(false))
                }
                5 => {
                    manager.set_thinking_level("high").unwrap();
                    ("thinking", Some(false))
                }
                6 => {
                    manager.set_name(&format!("name {step}")).unwrap();
                    ("name", Some(false))
                }
                7 if !all.is_empty() => {
                    let target = all[roll(all.len())].clone();
                    let label = Some(format!("label {step}"));
                    manager.set_label(&target, label).unwrap();
                    ("label", Some(false))
                }
                8 if !all.is_empty() => {
                    let target = all[roll(all.len())].clone();
                    manager.branch(Some(&target)).unwrap();
                    ("branch", None)
                }
                9 if !all.is_empty() => {
                    let target = all[roll(all.len())].clone();
                    let summary = format!("summary {step}");
                    manager
                        .branch_with_summary(Some(&target), summary, None, None)
                        .unwrap();
                    ("summary", None)
                }
                10 if !path.is_empty() => {
                    let kept = path[roll(path.len())].clone();
                    manager
                        .append_compaction(pi_coding_session::CompactionOptions {
                            summary: format!("compacted {step}"),
                            first_kept_entry_id: Some(kept),
                            tokens_before: 10,
                            ..Default::default()
                        })
                        .unwrap();
                    ("compaction", Some(true))
                }
                11 if !path.is_empty() => {
                    let target = path[roll(path.len())].clone();
                    let replacement =
                        (roll(2) == 0).then(|| pi_coding_session::ContentReplacement {
                            content: json!(format!("edit {step}")),
                        });
                    if manager.edit_context(&target, replacement).is_err() {
                        continue;
                    }
                    ("edit", Some(true))
                }
                _ => continue,
            };
            while let Ok(change) = observed.changes.try_recv() {
                let pi_coding_session::HistoryChange::Committed {
                    appended,
                    leaf_id,
                    revision,
                    ..
                } = change
                else {
                    panic!("the manager only commits here")
                };
                let extension =
                    context_extension(history.leaf_id.as_deref(), &appended, leaf_id.as_deref());
                history.entries.extend(appended);
                history.leaf_id = leaf_id;
                history.revision = revision;
                let whole = pi_coding_session::project(&history).unwrap();
                if let Some(restructures) = restructures {
                    assert_eq!(extension.is_none(), restructures, "step {step}: {kind}");
                }
                if let Some(added) = extension {
                    // What the client holds, plus what the append says, is the whole context.
                    let merged = context.iter().cloned().chain(added).collect::<Vec<_>>();
                    assert_eq!(merged, whole.entries, "step {step}: {kind}");
                    let messages = merged
                        .iter()
                        .flat_map(|e| e.messages.clone())
                        .collect::<Vec<_>>();
                    assert_eq!(messages, whole.messages, "step {step}: {kind}");
                    extended += 1;
                } else {
                    resent += 1;
                }
                context = whole.entries;
            }
        }
        assert!(
            extended > 100 && resent > 20,
            "{extended} extended, {resent} resent"
        );
    }
}
