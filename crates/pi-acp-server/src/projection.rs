//! Detached, cursor-ordered entity projection. Runtime callbacks only enqueue.
use crate::protocol::*;
use base64::{Engine, engine::general_purpose::STANDARD};
use pi_agent::AgentEvent;
use pi_coding_runtime::{ActivityOutcome, CodingSession, LoadedResources, SessionEvent};
use pi_coding_session::{EntryContent, SessionSnapshot};
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
    pub fn capture_view(session: &CodingSession, models: &[pi_ai::Model]) -> Value {
        let (steer, follow) = session.queued_inputs();
        json!({"model":session.model(),"routedModel":session.routed_model().map(|(m,_)|m),"thinkingLevel":session.thinking_level(),"availableThinkingLevels":session.available_thinking_levels(),"models":models,"scopedModels":session.scoped_models().iter().map(|m|json!({"model":m.model,"thinkingLevel":m.thinking_level})).collect::<Vec<_>>(),"configOptions":config(session,models),"queue":{"steering":steer.iter().map(queued).collect::<Vec<_>>(),"followUp":follow.iter().map(queued).collect::<Vec<_>>()},"tools":session.all_tools().into_iter().map(|t|json!({"declaration":t.declaration,"exposure":match t.exposure{pi_coding_runtime::ToolExposure::Direct=>"direct",pi_coding_runtime::ToolExposure::ModelOnly=>"modelOnly",pi_coding_runtime::ToolExposure::CodeMode=>"codeMode",pi_coding_runtime::ToolExposure::Deferred=>"deferred",pi_coding_runtime::ToolExposure::Hidden=>"hidden"},"source":t.source,"namespace":t.namespace})).collect::<Vec<_>>()})
    }
    pub fn snapshot(&self, _session: &CodingSession, _models: &[pi_ai::Model]) -> Value {
        let context =
            pi_coding_session::project(&self.history).expect("canonical observed history");
        let mut snapshot = self.view.clone();
        let (stats, context_usage) = statistics(
            &self.history,
            &context,
            self.view["model"]["contextWindow"].as_u64().unwrap_or(0),
        );
        for (key,value) in json!({"sessionId":self.history.header.id,"epoch":self.epoch,"sequence":self.sequence,"history":self.history,"messages":context.messages,"contextEntries":context.entries,"liveEntities":self.entity_order.iter().filter_map(|key|self.entities.get(key).cloned()).collect::<Vec<_>>(),"state":self.state,"activity":self.activity,"stats":stats,"contextUsage":context_usage,"settings":self.settings,"resources":self.resources,"authMethods":self.auth_methods}).as_object().unwrap(){snapshot[key]=value.clone();}
        snapshot
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
}
