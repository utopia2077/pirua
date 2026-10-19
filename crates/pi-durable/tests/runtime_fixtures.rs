//! Frozen release inputs executed through the actual native conversation runtime.
//! Expected bytes never participate in dispatch, effect behavior, or state construction.
mod bindings_storage;
#[path = "runtime_support/provider.rs"]
mod runtime_support;
#[path = "runtime_support/control.rs"]
mod script_control;
mod support;
use pi_ai::Clock;
use pi_durable::{session::*, *};
use runtime_support::*;
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
fn j(v: impl Serialize) -> Value {
    serde_json::to_value(v).unwrap()
}
fn undefined() -> Value {
    json!({"$undefined":true})
}
fn err(e: &Error) -> Value {
    json!({"category":if e.message.starts_with("Conversation ")&&e.message.ends_with(" is busy"){"ConversationBusy"}else{"Error"},"message":e.message})
}
fn reference(v: &Value, b: &BTreeMap<String, Value>) -> Value {
    match v {
        Value::String(s) if s.starts_with('$') => {
            let mut p = s[1..].split('.');
            let mut v = b[p.next().unwrap()].clone();
            for key in p {
                v = if let Some(a) = v.as_array() {
                    a[key.parse::<usize>().unwrap()].clone()
                } else {
                    v[key].clone()
                };
            }
            v
        }
        Value::Array(a) => j(a.iter().map(|v| reference(v, b)).collect::<Vec<_>>()),
        Value::Object(o) => Value::Object(
            o.iter()
                .map(|(k, v)| (k.clone(), reference(v, b)))
                .collect(),
        ),
        _ => v.clone(),
    }
}
async fn flush(session: &Session) {
    for _ in 0..6 {
        tokio::task::yield_now().await;
        // Source SQLite settles on its JS event-loop turn. Native Session I/O
        // runs on a blocking worker, so join the shared mutation line rather
        // than advancing the controlled clock after an arbitrary real delay.
        match session.conversation(ROOT_CONVERSATION_ID).await {
            Ok(_) => (),
            Err(error) if error.kind == ErrorKind::Closed => {
                assert!(matches!(session.closed().await, SessionEnd::Closed));
            }
            Err(error) => panic!("native host read barrier failed: {error}"),
        }
    }
}
async fn eventually(session: &Session, mut check: impl AsyncFnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if check().await {
                return;
            }
            flush(session).await;
        }
    })
    .await
    .expect("native runtime fixture condition timed out");
}
fn cid(v: &Value) -> ConversationId {
    ConversationId::new(v.as_u64().unwrap_or(1)).unwrap()
}
fn tid(v: &Value) -> TaskId {
    TaskId::new(v.as_u64().unwrap()).unwrap()
}
fn sid(v: &Value) -> SubmissionId {
    SubmissionId::new(v.as_u64().unwrap()).unwrap()
}
fn entry(v: &Value) -> EntryDraft {
    let mut d = EntryDraft::new(v["kind"].as_str().unwrap());
    d.model = v
        .get("model")
        .map(|v| serde_json::from_value(v.clone()).unwrap());
    d.data = v.get("data").cloned();
    d.edits = v
        .get("edits")
        .map(|v| serde_json::from_value(v.clone()).unwrap());
    d.head = match v.get("head") {
        Some(Value::String(s)) if s == "self" => Some(EntryHead::Own),
        Some(v) => Some(EntryHead::Entry(serde_json::from_value(v.clone()).unwrap())),
        None => None,
    };
    d
}
fn draft(v: &Value) -> SubmissionDraft {
    let request_id = v["requestId"].as_str().map(str::to_owned);
    if v["type"] == "write" {
        SubmissionDraft::Write {
            request_id,
            entry: entry(&v["entry"]),
        }
    } else {
        SubmissionDraft::Input {
            request_id,
            content: serde_json::from_value(v["content"].clone()).unwrap(),
            when_busy: v
                .get("whenBusy")
                .map(|v| serde_json::from_value(v.clone()).unwrap()),
            entry: v
                .get("entry")
                .map(|v| serde_json::from_value(v.clone()).unwrap()),
        }
    }
}
fn publication(p: &CommitPublication) -> Value {
    let changes=p.changes.iter().map(|change|match change{CommitChange::Table(w)=>j(w),CommitChange::Document{record,conversation_id,version,value,ops,..}=>{let mut c=json!({"type":"document","record":record,"value":value.as_ref(),"ops":ops});
if let Some(id)=conversation_id{c["conversationId"]=j(id);
}
if let Some(version)=version{c["version"]=j(version);
}c},CommitChange::DocumentCopy{record,conversation_id,source,..}=>json!({"type":"document.copy","record":record,"conversationId":conversation_id,"source":source})}).collect::<Vec<_>>();

    json!({"seq":p.seq,"changes":changes})
}
#[derive(Clone)]
struct RecordedTimer {
    callback: Arc<dyn Fn() + Send + Sync>,
    at: f64,
    disarmed: Arc<AtomicBool>,
}
struct Host {
    clock: Arc<TestClock>,
    monotonic: Mutex<f64>,
    script: Arc<ScriptState>,
    registry: RuntimeRegistry,
    settings: Mutex<Value>,
    input: Value,
    aliases: Mutex<BTreeMap<String, Value>>,
    effects: Mutex<BTreeMap<String, u64>>,
    tool_events: Mutex<Vec<Value>>,
    hook_events: Mutex<Vec<Value>>,
    publications: Mutex<Vec<Value>>,
    reports: Mutex<Vec<Value>>,
    timers: Mutex<Vec<RecordedTimer>>,
    store: Mutex<Arc<dyn Storage>>,
    yield_count: AtomicU64,
}
impl Host {
    fn bindings(&self) -> BTreeMap<String, Value> {
        let mut b = self.aliases.lock().unwrap().clone();
        b.insert("clock".into(), json!(self.clock.now_unix_ms()));
        b
    }
    fn settings(&self) -> DurableSettings {
        let s = self.settings.lock().unwrap();
        let c = &s["compaction"];
        let p = &s["progress"];
        let mut out = DurableSettings {
            compaction: CompactionPolicy {
                enabled: c["enabled"].as_bool().unwrap_or(false),
                reserve_tokens: c["reserveTokens"].as_u64().unwrap_or(1000),
                keep_recent_tokens: c["keepRecentTokens"].as_u64().unwrap_or(10),
                background_tokens: c["backgroundTokens"].as_u64().unwrap_or(0),
            },
            partial_interval_ms: p["partialIntervalMs"].as_f64().unwrap_or(10.0),
            output_interval_ms: p["outputIntervalMs"].as_f64().unwrap_or(10.0),
            tool_execution: s
                .get("toolExecution")
                .map(|mode| serde_json::from_value(mode.clone()).unwrap())
                .unwrap_or_default(),
            ..Default::default()
        };
        if let Some(stream) = s.get("stream") {
            out.stream = serde_json::from_value(stream.clone()).unwrap();
        }
        if let Some(retry) = s.get("retry") {
            if let Some(n) = retry["maxRetries"].as_u64() {
                out.retry.max_retries = pi_ai::MaxRetries::new(n).unwrap();
            }
            if let Some(n) = retry["baseDelayMs"].as_u64() {
                out.retry.base_delay_ms = pi_ai::SafeUnsigned::new(n).unwrap();
            }
        }
        out
    }
    fn install_tool(self: &Arc<Self>, id: &str, replay: Option<&str>) {
        let d = self.input["tools"]
            .as_array()
            .unwrap()
            .iter()
            .find(|d| d["id"] == id)
            .unwrap()
            .clone();
        let name = d["name"].as_str().unwrap().to_owned();
        let tool:pi_ai::Tool=serde_json::from_value(json!({"name":name,"description":d.get("description").unwrap_or(&d["name"]),"parameters":d.get("parameters").cloned().unwrap_or(json!({"type":"object","properties":{},"additionalProperties":false}))})).unwrap();

        let host = self.clone();
        let descriptor = d.clone();
        let mut tool = ToolRegistration::new(tool, move |inv| {
            let host = host.clone();
            let d = descriptor.clone();
            Box::pin(async move { host.execute(d, inv).await })
        });
        tool.replay = if replay.or_else(|| d["replay"].as_str()) == Some("safe") {
            ReplayPolicy::Safe
        } else {
            ReplayPolicy::Unsafe
        };
        tool.structured_output_schema = d
            .get("structuredOutputSchema")
            .map(|v| serde_json::from_value(v.clone()).unwrap());
        tool.callers = d["callers"].as_array().map(|a| {
            a.iter()
                .map(|v| {
                    if v == "tools" {
                        ToolCaller::Tools
                    } else {
                        ToolCaller::Model
                    }
                })
                .collect()
        });
        if d.get("prepare").is_some() {
            let host = self.clone();
            let field = d["prepare"]["numberField"].as_str().map(str::to_owned);
            let name = name.clone();
            tool.prepare_arguments = Some(Arc::new(move |args| {
                host.tool_events
                    .lock()
                    .unwrap()
                    .push(json!({"kind":"prepareArguments","tool":name,"args":args}));
                let mut v = j(args);
                if let Some(f) = &field
                    && let Some(s) = v[f].as_str()
                {
                    v[f] = serde_json::from_str::<Value>(s).unwrap();
                }
                Ok(serde_json::from_value(v).unwrap())
            }));
        }
        self.registry
            .install(Extension {
                name: format!("tool:{name}"),
                tools: vec![tool],
                ..Default::default()
            })
            .unwrap();
    }
    async fn execute(
        self: Arc<Self>,
        d: Value,
        inv: ToolInvocation,
    ) -> Result<ToolExecutionResult> {
        let name = d["name"].as_str().unwrap();
        let run = {
            let mut e = self.effects.lock().unwrap();
            let v = e.entry(name.into()).or_default();
            *v += 1;
            *v
        };
        let task = inv.api.identity().invocation.task_id;
        let call = inv.call.call.id.to_string();
        let stored = self.store.lock().unwrap().clone().task(task)?;
        self.tool_events.lock().unwrap().push(json!({"kind":"execute","tool":name,"run":run,"taskId":task,"callId":call,"args":inv.call.call.arguments,"persisted":stored}));
        let mut local = serde_json::Map::new();
        for raw in d["actions"].as_array().into_iter().flatten() {
            let mut b = self.bindings();
            b.extend([
                ("args".into(), j(&inv.call.call.arguments)),
                ("local".into(), j(&local)),
                ("run".into(), j(run)),
                ("taskId".into(), j(task)),
                ("callId".into(), j(&call)),
            ]);
            let a = reference(raw, &b);
            match a["do"].as_str().unwrap() {
                "output" => inv
                    .api
                    .output(OutputChunk::Text(a["text"].as_str().unwrap().into()), None)?,
                "details" => {
                    inv.api
                        .details(serde_json::from_value(a["value"].clone()).unwrap())
                        .await?;
                    let live = inv
                        .api
                        .snapshot(
                            live_doc(),
                            live_doc().address(
                                Scope::Conversation {
                                    conversation_id: inv.api.identity().conversation_id,
                                },
                                None,
                            )?,
                        )
                        .await?;
                    self.tool_events.lock().unwrap().push(json!({"kind":"detailsAcknowledged","taskId":task,"value":a["value"],"persistedLive":live}));
                }
                "diagnostic" => inv
                    .api
                    .diagnostic(serde_json::from_value(a["value"].clone()).unwrap())?,
                "gate" => {
                    if a["firstRuns"].as_u64().is_none_or(|n| run <= n) {
                        self.script
                            .gate(a["name"].as_str().unwrap(), &inv.api.signal())
                            .await;
                        inv.api.check_live()?;
                    }
                }
                "nested" => {
                    let result = inv
                        .api
                        .execute_tool(
                            a["name"].as_str().unwrap().into(),
                            serde_json::from_value(a.get("args").cloned().unwrap_or(json!({})))
                                .unwrap(),
                            NestedToolOptions {
                                key: a["key"].as_str().map(str::to_owned),
                                progress: a["progress"].as_bool(),
                            },
                        )
                        .await;
                    match result {
                        Ok(value) => {
                            if let Some(key) = a["as"].as_str() {
                                local.insert(key.into(), j(&value));
                            }
                            self.tool_events.lock().unwrap().push(json!({"kind":"nestedResult","taskId":task,"key":a["key"],"value":value}));
                        }
                        Err(e) if a["catch"] == true => {
                            self.tool_events.lock().unwrap().push(
                                json!({"kind":"nestedRejected","taskId":task,"error":err(&e)}),
                            )
                        }
                        Err(e) => return Err(e),
                    }
                }
                "return" => return Ok(serde_json::from_value(a["value"].clone()).unwrap()),
                "throw" => return Err(Error::request(a["message"].as_str().unwrap())),
                op => panic!("unknown native tool action {op}"),
            }
        }
        Ok(Default::default())
    }
    fn install_hooks(self: &Arc<Self>) {
        let hooks = &self.input["hooks"];
        if hooks.get("generation").is_some() {
            let h = self.clone();
            let before = Arc::new(move |mut messages: Vec<pi_ai::Message>, api: HookApi| {
                let h = h.clone();
                Box::pin(async move {
                    h.hook_events.lock().unwrap().push(
                        json!({"kind":"beforeRequest","taskId":api.task_id(),"messages":messages}),
                    );
                    if let Some(text) = h.input["hooks"]["generation"]["appendUser"].as_str() {
                        messages.push(serde_json::from_value(json!({"role":"user","content":text,"timestamp":h.clock.now_unix_ms()})).unwrap());
                        Ok(Some(messages))
                    } else {
                        Ok(None)
                    }
                }) as RuntimeFuture<Option<Vec<pi_ai::Message>>>
            });
            let h = self.clone();
            let after = Arc::new(move |message: pi_ai::AssistantMessage, api: HookApi| {
                let h = h.clone();
                Box::pin(async move {
                    h.hook_events.lock().unwrap().push(
                        json!({"kind":"afterResponse","taskId":api.task_id(),"message":message}),
                    );
                    Ok(())
                }) as RuntimeFuture<()>
            });
            let h = self.clone();
            let on_yield = Arc::new(move |message: pi_ai::AssistantMessage, api: HookApi| {
                let h = h.clone();
                Box::pin(async move {
                    h.hook_events
                        .lock()
                        .unwrap()
                        .push(json!({"kind":"onYield","taskId":api.task_id(),"message":message}));
                    Ok(if h.yield_count.fetch_add(1, Ordering::SeqCst) == 0 {
                        h.input["hooks"]["generation"]["continueOnce"]
                            .as_str()
                            .map(|s| pi_ai::UserContent::Text(s.into()))
                    } else {
                        None
                    })
                }) as RuntimeFuture<Option<pi_ai::UserContent>>
            });
            self.registry
                .install(Extension {
                    name: "oracle:generation-hooks".into(),
                    hooks: Hooks {
                        before_request: Some(before),
                        after_response: Some(after),
                        on_yield: Some(on_yield),
                        ..Default::default()
                    },
                    ..Default::default()
                })
                .unwrap();
        }
        if hooks.get("tool").is_some() {
            let h = self.clone();
            let before = Arc::new(move |call: ToolHookCall, api: HookApi| {
                let h = h.clone();
                Box::pin(async move {
                    h.hook_events
                        .lock()
                        .unwrap()
                        .push(json!({"kind":"beforeTool","taskId":api.task_id(),"call":call}));
                    Ok(h.input["hooks"]["tool"]["before"]
                        .get(call.call.name.as_str())
                        .map(|v| BeforeToolResult {
                            arguments: v
                                .get("arguments")
                                .map(|v| serde_json::from_value(v.clone()).unwrap()),
                            block: v["block"].as_str().map(str::to_owned),
                        }))
                }) as RuntimeFuture<Option<BeforeToolResult>>
            });
            let h = self.clone();
            let after = Arc::new(
                move |call: ToolHookCall, result: ToolExecutionResult, api: HookApi| {
                    let h = h.clone();
                    Box::pin(async move {
                        h.hook_events.lock().unwrap().push(json!({"kind":"afterTool","taskId":api.task_id(),"call":call,"result":result}));
                        Ok(None)
                    }) as RuntimeFuture<Option<ToolExecutionResult>>
                },
            );
            self.registry
                .install(Extension {
                    name: "oracle:tool-hooks".into(),
                    hooks: Hooks {
                        before_tool: Some(before),
                        after_tool: Some(after),
                        ..Default::default()
                    },
                    ..Default::default()
                })
                .unwrap();
        }
        if hooks.get("compaction").is_some() {
            let h = self.clone();
            self.registry.install(Extension{name:"oracle:compaction-hooks".into(),hooks:Hooks{before_compact:Some(Arc::new(move|input,api|{let h=h.clone();
Box::pin(async move{h.hook_events.lock().unwrap().push(json!({"kind":"beforeCompact","taskId":api.task_id(),"input":input}));
let d=&h.input["hooks"]["compaction"];
Ok(if d["decline"]==true{Some(CompactDecision::Decline)}else{d["summary"].as_str().map(|s|CompactDecision::Summary(s.into()))})})})),..Default::default()},..Default::default()}).unwrap();
        }
    }
    async fn open(self: &Arc<Self>, models: Arc<pi_ai::ModelsRegistry>) -> Durable {
        let mut o = DurableOptions::new(models);
        o.registry = self.registry.clone();
        let h = self.clone();
        o.settings = Arc::new(move || h.settings());
        let h = self.clone();
        o.scheduler.now = Arc::new(move || h.clock.now_unix_ms() as f64);
        let h = self.clone();
        o.monotonic_now = Arc::new(move || *h.monotonic.lock().unwrap());
        let h = self.clone();
        o.scheduler.timer_host = Some(Arc::new(move |callback, delay| {
            let disarmed = Arc::new(AtomicBool::new(false));
            h.timers.lock().unwrap().push(RecordedTimer {
                callback,
                at: *h.monotonic.lock().unwrap() + delay,
                disarmed: disarmed.clone(),
            });
            Timer::new(move || {
                disarmed.store(true, Ordering::SeqCst);
            })
        }));
        let h = self.clone();
        o.scheduler.on_report = Some(Arc::new(move |r| {
            h.reports
                .lock()
                .unwrap()
                .push(json!({"error":err(&r.error),"type":r.kind,"taskId":r.task_id}))
        }));
        let clock = self.clock.clone();
        o.request_runtime = Arc::new(move |c| {
            pi_ai::RequestRuntime::with_noop_telemetry(c, clock.clone(), Arc::new(Spawner))
        });
        o.header_policy = pi_ai::ModelHeaderPolicy::from_allowlist(["x-fixture".into()]);
        o.conversation_created = Some(Arc::new(|tx, c| {
            let d = tx.doc(
                &provider_doc(),
                provider_doc().address(
                    Scope::Conversation {
                        conversation_id: c.id,
                    },
                    None,
                )?,
                Value::Null,
            )?;
            tx.apply(
                d,
                Op::Set(
                    vec!["sessionId".into()],
                    json!(format!("fixture-session-{}", c.id)),
                ),
            )
        }));
        let store = self.store.lock().unwrap().clone();
        let d = Durable::open(store, o).await.unwrap();
        let mut sub = d.session().subscribe_commits().await.unwrap();
        let h = self.clone();
        tokio::spawn(async move {
            while let Some(p) = sub.next().await {
                h.publications.lock().unwrap().push(publication(&p));
            }
        });
        let mut agent = json!({"model":{"provider":"oracle","modelId":"scripted-1"}});
        if let Some(a) = self.input["agent"].as_object() {
            agent.as_object_mut().unwrap().extend(a.clone());
        }
        d.root(ConversationOptions {
            agent: Some(serde_json::from_value(agent).unwrap()),
            ..Default::default()
        })
        .await
        .unwrap();
        flush(&d.session()).await;
        d
    }
}
struct Watched {
    watch: pi_durable::view::ConversationViewWatch,
    initial: Value,
    frames: Arc<Mutex<Vec<Value>>>,
    start_publication: usize,
    c: ConversationId,
}
async fn execute(case: &Value, backend: &str) -> Value {
    let temp = support::Temp::new();
    let make_store = || -> Arc<dyn Storage> {
        if backend == "memory" {
            Arc::new(MemoryStorage::new())
        } else {
            Arc::new(SqliteStorage::open(&temp.0).unwrap())
        }
    };
    let input = &case["input"];
    let clock = Arc::new(TestClock(AtomicU64::new(
        input["clock"].as_u64().unwrap_or(1000),
    )));
    let (models, script) = models(&input["provider"], clock.clone()).await;
    let host = Arc::new(Host {
        clock,
        monotonic: Mutex::new(0.0),
        script,
        registry: RuntimeRegistry::new(),
        settings: Mutex::new({
            let mut value = json!({"compaction":{"enabled":false,"reserveTokens":1000,"keepRecentTokens":10,"backgroundTokens":0},"progress":{"partialIntervalMs":10,"outputIntervalMs":10}});
            if let Some(extra) = input["settings"].as_object() {
                value.as_object_mut().unwrap().extend(extra.clone());
            }
            value
        }),
        input: input.clone(),
        aliases: Mutex::new(BTreeMap::from([("root".into(), json!(1))])),
        effects: Mutex::new(Default::default()),
        tool_events: Mutex::new(vec![]),
        hook_events: Mutex::new(vec![]),
        publications: Mutex::new(vec![]),
        reports: Mutex::new(vec![]),
        timers: Mutex::new(vec![]),
        store: Mutex::new(make_store()),
        yield_count: AtomicU64::new(0),
    });
    for tool in input["tools"].as_array().into_iter().flatten() {
        host.install_tool(tool["id"].as_str().unwrap(), None);
    }
    host.install_hooks();
    for section in input["sections"].as_array().into_iter().flatten() {
        let text = section["text"].as_str().unwrap().to_owned();
        let key = section["key"].as_str().unwrap().to_owned();
        host.registry
            .install(Extension {
                name: format!("section:{key}"),
                sections: vec![PromptSection {
                    key,
                    tag: section["tag"].as_bool().unwrap_or(false),
                    render: Arc::new(move |_| {
                        let text = text.clone();
                        Box::pin(async move { Ok(Some(text)) })
                    }),
                }],
                ..Default::default()
            })
            .unwrap();
    }
    let mut durable = host.open(models.clone()).await;
    let mut observations = vec![];
    let mut watches: BTreeMap<String, Watched> = BTreeMap::new();
    for (index, raw) in input["operations"].as_array().unwrap().iter().enumerate() {
        let op = reference(raw, &host.bindings());
        let name = op["op"].as_str().unwrap();
        if std::env::var_os("PIRUA_RUNTIME_TRACE").is_some() {
            eprintln!("{} {backend} {index}:{name}", case["id"]);
        }
        let c = cid(&op["conversationId"]);
        let result:Result<Value>=async{Ok(match name{
  "submit"=>j(durable.conversation(c).await?.unwrap().submit(draft(&op["value"])).await?.id),
  "status"=>j(durable.submission(sid(&op["id"])).await?.unwrap().status().await?),
  "wait"=>{let v=durable.submission(sid(&op["id"])).await?.unwrap().wait().await?;flush(&durable.session()).await;j(v)},
  "abortSubmission"=>{let v=durable.submission(sid(&op["id"])).await?.unwrap().abort().await?;
flush(&durable.session()).await;
json!(match v{SubmissionAbort::Aborted=>"aborted",SubmissionAbort::AlreadyPlaced=>"alreadyPlaced",SubmissionAbort::Settled=>"settled",SubmissionAbort::NotFound=>"notFound"})},
  "append"=>{let value=entry(&op["value"]);j(durable.conversation(c).await?.unwrap().commit(move|tx|tx.append_entry(c,value)).await?)},
  "configure"=>{durable.conversation(c).await?.unwrap().configure(serde_json::from_value(op["change"].clone()).unwrap()).await?;undefined()},
  "context"=>{let context=durable.conversation(c).await?.unwrap().context(op["at"].as_u64().map(|id|EntryId::new(id).unwrap())).await?;
let mut v=j(context);
if v["head"].is_null(){v.as_object_mut().unwrap().remove("head");
}v},
  "entries"=>j(durable.conversation(c).await?.unwrap().entries(EntryQuery{conversation_id:c,order:Some(ScanOrder::Ascending),..EntryQuery::new(c)},1000,None).await?),
  "docs"=>{let mut result=json!({});
for(name,def)in[("agent",agent_doc()),("inbox",inbox_doc()),("live",live_doc()),("provider",provider_doc()),("usage",usage_doc())]{let address=def.address(Scope::Conversation{conversation_id:c},None)?;
result[name]=durable.session().snapshot(def,address).await?.unwrap();
}result},
  "usage"=>j(durable.usage().await?),
  "task"=>durable.get_task(tid(&op["id"])).await?.map(j).unwrap_or_else(undefined),
  "tasks"=>j(durable.session().scan_tasks(TaskQuery{conversation_id:Some(c),..Default::default()},100,None).await?),
  "live"=>durable.session().snapshot(live_doc(),live_doc().address(Scope::Conversation{conversation_id:c},None)?).await?.unwrap(),
  "resume"=>{durable.resume();flush(&durable.session()).await;json!("resumed")},
  "idle"=>{durable.wait_for_idle().await?;flush(&durable.session()).await;undefined()},
  "requests"=>j(&*host.script.requests.lock().unwrap()),"toolEvents"=>j(&*host.tool_events.lock().unwrap()),"effects"=>j(&*host.effects.lock().unwrap()),
  "untilRequests"=>{if tokio::time::timeout(Duration::from_secs(2),host.script.requests(op["count"].as_u64().unwrap()as usize)).await.is_err(){panic!("no request; tasks={} reports={} publications={}",j(durable.session().scan_tasks(TaskQuery::default(),100,None).await?),j(&*host.reports.lock().unwrap()),j(&*host.publications.lock().unwrap()));
}flush(&durable.session()).await;
j(host.script.requests.lock().unwrap().len())},
  "untilEffects"=>{eventually(&durable.session(),async||host.effects.lock().unwrap().get(op["name"].as_str().unwrap()).copied().unwrap_or(0)>=op["count"].as_u64().unwrap()).await;
flush(&durable.session()).await;
j(&*host.effects.lock().unwrap())},
  "untilLive"=>{let address=live_doc().address(Scope::Conversation{conversation_id:c},None)?;
let path=op["path"].as_array().unwrap();
eventually(&durable.session(),async||{let live=durable.session().snapshot(live_doc(),address.clone()).await.unwrap().unwrap();
let mut selected=&live;
for key in path{selected=if let Some(i)=key.as_u64(){&selected[i as usize]}else{&selected[key.as_str().unwrap()]};
}if let Some(equal)=op.get("equals"){{let mut selected=selected.clone();
let mut equal=equal.clone();
integral(&mut selected);
integral(&mut equal);
selected==equal}}else{!selected.is_null()}}).await;
durable.session().snapshot(live_doc(),address).await?.unwrap()},
  "fireTimers"=>{let active=host.timers.lock().unwrap().iter().filter(|t|!t.disarmed.load(Ordering::SeqCst)).cloned().collect::<Vec<_>>();
let count=active.len();
for t in active{t.disarmed.store(true,Ordering::SeqCst);
{let mut mono=host.monotonic.lock().unwrap();
*mono=mono.max(t.at);
} (t.callback)();
}flush(&durable.session()).await;
j(count)},
  "settings"=>{host.settings.lock().unwrap().as_object_mut().unwrap().extend(op["change"].as_object().unwrap().clone());j(&*host.settings.lock().unwrap())},
  "advance"=>{host.clock.0.store(op["to"].as_u64().unwrap(),Ordering::SeqCst);durable.resume();flush(&durable.session()).await;op["to"].clone()},
  "wake"=>{flush(&durable.session()).await;j(durable.wake().await?)},
  "release"=>{host.script.release(op["name"].as_str().unwrap());flush(&durable.session()).await;json!("released")},
  "installTool"=>{host.install_tool(op["id"].as_str().unwrap(),op["replay"].as_str());json!("installed")},
  "reset"=>{durable.conversation(c).await?.unwrap().reset(op["handoff"].as_str().map(str::to_owned)).await?;undefined()},
  "fork"=>j(durable.conversation(c).await?.unwrap().fork(serde_json::from_value(op["at"].clone()).unwrap(),ConversationCreateOptions{ownership:ConversationOwnership::Ownerless,options:Default::default()}).await?.id),
  "compact"=>j(durable.conversation(c).await?.unwrap().compact(op["instructions"].as_str().map(str::to_owned)).await?),
  "waitTask"=>{let task=durable.wait_for_task(tid(&op["id"])).await?;flush(&durable.session()).await;j(task)},
  "abortConversation"=>{durable.conversation(c).await?.unwrap().abort(false).await?;undefined()},
  "watch"=>{let watch=durable.conversation(c).await?.unwrap().watch().await?;
let initial=j(watch.value().as_ref());
let frames=Arc::new(Mutex::new(vec![]));
let capture=frames.clone();
watch.start(move|frame|{let capture=capture.clone();
async move{capture.lock().unwrap().push(json!({"value":frame.value.as_ref(),"ops":frame.ops}));
Ok(())}})?;
watches.insert(op["name"].as_str().unwrap().into(),Watched{watch,initial:initial.clone(),frames,start_publication:host.publications.lock().unwrap().len(),c});
initial},
  "watchFrames"=>{flush(&durable.session()).await;
let w=&watches[op["name"].as_str().unwrap()];
let frames=w.frames.lock().unwrap().clone();
let mut replay=w.initial.clone();
for frame in &frames{let ops:Vec<Op>=serde_json::from_value(frame["ops"].clone()).unwrap();
replay=pi_durable::session::apply(&replay,&ops)?;
assert_eq!(replay,frame["value"],"actual native view operations must replay");
}let touching=host.publications.lock().unwrap()[w.start_publication..].iter().filter(|p|p["changes"].as_array().unwrap().iter().any(|c|c["type"]=="entry"&&c["value"]["conversationId"]==j(w.c)||c["type"]=="document"&&c["conversationId"]==j(w.c)&&pi_durable::view::MOUNTED_KINDS.contains(&c["record"]["kind"].as_str().unwrap_or(""))&&c["ops"].as_array().is_some_and(|a|!a.is_empty()))).map(|p|p["seq"].clone()).collect::<Vec<_>>();
assert_eq!(frames.len(),touching.len(),"one view frame per touching native commit");
json!({"frames":frames,"touchingCommits":touching,"final":w.watch.value().as_ref()})},
  "close"=>{durable.close().await?;json!("closed")},
  "reopen"=>{assert_eq!(backend,"sqlite");durable.close().await?;*host.store.lock().unwrap()=make_store();durable=host.open(models.clone()).await;json!("reopened paused")},
  op=>panic!("unknown native runtime host operation {op}"),
 })}.await;
        let result = match result {
            Ok(v) => v,
            Err(e) if op["errorExpected"] == true => json!({"error":err(&e)}),
            Err(e) => panic!("{} ({backend}) operation {index} {name}: {e}", case["id"]),
        };
        if let Some(alias) = op["as"].as_str() {
            host.aliases
                .lock()
                .unwrap()
                .insert(alias.into(), result.clone());
        }
        observations.push(json!({"label":op.get("label").or_else(||op.get("as")).cloned().unwrap_or(json!(format!("{index}:{name}"))),"value":result}));
    }
    flush(&durable.session()).await;
    assert!(
        host.script.responses.lock().unwrap().is_empty(),
        "unused scripted responses in {}",
        case["id"]
    );
    let result = json!({"observations":observations,"requests":*host.script.requests.lock().unwrap(),"toolEvents":*host.tool_events.lock().unwrap(),"hookEvents":*host.hook_events.lock().unwrap(),"publications":*host.publications.lock().unwrap(),"reports":*host.reports.lock().unwrap()});

    for w in watches.values() {
        w.watch.stop();
    }
    durable.close().await.unwrap();
    result
}
fn integral(v: &mut Value) {
    match v {
        Value::Number(n) => {
            if let Some(f) = n.as_f64()
                && f.fract() == 0.0
                && (0.0..=9_007_199_254_740_991.0).contains(&f)
            {
                *v = json!(f as u64);
            }
        }
        Value::Array(a) => {
            for v in a {
                integral(v)
            }
        }
        Value::Object(o) => {
            for v in o.values_mut() {
                integral(v)
            }
        }
        _ => (),
    }
}
// The accepted Session projection compares full atomic change membership and
// independently replays actual native operations; Chord proxy spelling is native.
fn committed_projection(value: &mut Value) {
    let mut previous = BTreeMap::new();
    for publication in value["publications"].as_array_mut().unwrap() {
        let changes = publication["changes"].as_array_mut().unwrap();
        for change in changes.iter_mut() {
            if change["type"] == "document" {
                let id = change["record"]["id"].as_u64().unwrap();
                let ops: Vec<Op> = serde_json::from_value(change["ops"].clone()).unwrap();
                if !change["value"].is_null() {
                    if let Some(before) = previous.get(&id) {
                        assert_eq!(
                            pi_durable::session::apply(before, &ops).unwrap(),
                            change["value"],
                            "publication operations replay"
                        );
                    }
                    previous.insert(id, change["value"].clone());
                }
                change["nonemptyOps"] = json!(!ops.is_empty());
                change.as_object_mut().unwrap().remove("ops");
            }
        }
        changes.sort_by_key(|change| {
            (
                change["type"].as_str().unwrap().to_owned(),
                change
                    .get("record")
                    .or_else(|| change.get("value"))
                    .unwrap()["id"]
                    .as_u64()
                    .unwrap(),
            )
        });
    }
    for observation in value["observations"].as_array_mut().unwrap() {
        if let Some(frames) = observation["value"]
            .get_mut("frames")
            .and_then(Value::as_array_mut)
        {
            for frame in frames {
                frame["nonemptyOps"] = json!(!frame["ops"].as_array().unwrap().is_empty());
                frame.as_object_mut().unwrap().remove("ops");
            }
        }
    }
}
// Approved design14 boundary: only the proven TypeBox/jsonschema integer
// diagnosis differs. Preserve the tool header, path/type meaning and original
// argument JSON; verify the native diagnostic's echoed value against that JSON.
fn validation_projection(value: &mut Value) {
    match value {
        Value::String(text)
            if text.contains("Validation failed for tool \"")
                && text.contains("\nReceived arguments:\n") =>
        {
            let arguments = text.split_once("\nReceived arguments:\n").unwrap().1;
            let input: Value = serde_json::Deserializer::from_str(arguments)
                .into_iter()
                .next()
                .unwrap()
                .unwrap();
            let mut changed = false;
            let lines = text
                .lines()
                .map(|line| {
                    if line == "  - n: must be integer" {
                        changed = true;
                        return "  - n [type]: expected integer".to_owned();
                    }
                    if let Some(actual) = line
                        .strip_prefix("  - n [type]: ")
                        .and_then(|s| s.strip_suffix(" is not of type \"integer\""))
                    {
                        let echoed: Value = serde_json::from_str(actual).unwrap();
                        assert_eq!(
                            echoed, input["n"],
                            "native validation echo must preserve the actual invalid input"
                        );
                        changed = true;
                        return "  - n [type]: expected integer".to_owned();
                    }
                    line.to_owned()
                })
                .collect::<Vec<_>>();
            if changed {
                *text = lines.join("\n");
            }
        }
        Value::Array(values) => {
            for value in values {
                validation_projection(value);
            }
        }
        Value::Object(object) => {
            for value in object.values_mut() {
                validation_projection(value);
            }
        }
        _ => (),
    }
}
fn first_difference(actual: &Value, expected: &Value, path: &str) -> Option<String> {
    if actual == expected {
        return None;
    }
    match (actual, expected) {
        (Value::Object(a), Value::Object(e)) => {
            for key in a.keys().chain(e.keys()) {
                match (a.get(key), e.get(key)) {
                    (Some(a), Some(e)) => {
                        if let Some(d) = first_difference(a, e, &format!("{path}/{key}")) {
                            return Some(d);
                        }
                    }
                    (a, e) => return Some(format!("{path}/{key}: native {a:?}, source {e:?}")),
                }
            }
        }
        (Value::Array(a), Value::Array(e)) => {
            for (i, (a, e)) in a.iter().zip(e).enumerate() {
                if let Some(d) = first_difference(a, e, &format!("{path}/{i}")) {
                    return Some(d);
                }
            }
            if a.len() != e.len() {
                return Some(format!(
                    "{path}: native length {}, source length {}",
                    a.len(),
                    e.len()
                ));
            }
        }
        _ => return Some(format!("{path}: native {actual}, source {expected}")),
    }
    None
}
#[test]
fn frozen_runtime_inputs_execute_on_both_native_backends() {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let fixture: Value = serde_json::from_str(include_str!(
                "../../../conformance/durable-v1.4.0/fixtures/runtime.json"
            ))
            .unwrap();
            let selected = std::env::var("PIRUA_RUNTIME_CASE").ok();
            let mut failures = vec![];
            let mut executions = 0;
            assert_eq!(fixture["cases"].as_array().unwrap().len(), 18);
            for case in fixture["cases"].as_array().unwrap() {
                if selected
                    .as_ref()
                    .is_some_and(|s| !case["id"].as_str().unwrap().contains(s))
                {
                    continue;
                }
                let backends = case
                    .get("backends")
                    .cloned()
                    .unwrap_or(json!(["memory", "sqlite"]));
                for backend in backends.as_array().unwrap() {
                    let backend = backend.as_str().unwrap();
                    assert!(["memory", "sqlite"].contains(&backend));
                    let expected = case["expected"]
                        .get(backend)
                        .expect("declared backend has an oracle result");
                    executions += 1;
                    let mut actual =
                        tokio::time::timeout(Duration::from_secs(10), execute(case, backend))
                            .await
                            .unwrap_or_else(|_| panic!("{} {backend} overall timeout", case["id"]));
                    if let Ok(directory) = std::env::var("PIRUA_RUNTIME_CAPTURE") {
                        std::fs::create_dir_all(&directory).unwrap();
                        std::fs::write(
                            format!(
                                "{directory}/{}-{backend}.json",
                                case["id"].as_str().unwrap()
                            ),
                            serde_json::to_vec_pretty(&actual).unwrap(),
                        )
                        .unwrap();
                    }
                    let mut expected = expected.clone();
                    integral(&mut actual);
                    integral(&mut expected);
                    committed_projection(&mut actual);
                    committed_projection(&mut expected);
                    validation_projection(&mut actual);
                    validation_projection(&mut expected);
                    if let Some(d) = first_difference(&actual, &expected, "") {
                        failures.push(format!("{} ({backend}) {d}", case["id"]));
                    }
                }
            }
            if selected.is_none() {
                assert_eq!(executions, 30);
            }
            eprintln!("Native runtime fixture executions: {executions}");
            assert!(failures.is_empty(), "{}", failures.join("\n"));
        });
}

#[test]
fn integer_validation_projection_preserves_input_and_other_diagnoses() {
    let source = "Validation failed for tool \"initial\":\n  - n: must be integer\n\nReceived arguments:\n{\n  \"n\": \"invalid\"\n}";
    let native = "Validation failed for tool \"initial\":\n  - n [type]: \"invalid\" is not of type \"integer\"\n\nReceived arguments:\n{\n  \"n\": \"invalid\"\n}";
    let mut expected = json!(source);
    let mut actual = json!(native);
    validation_projection(&mut expected);
    validation_projection(&mut actual);
    assert_eq!(actual, expected);
    let mut changed = json!(native.replace("\"initial\"", "\"another\""));
    validation_projection(&mut changed);
    assert_ne!(changed, expected, "tool identity retained");
    let mut changed = json!(source.replace("must be integer", "must be string"));
    let before = changed.clone();
    validation_projection(&mut changed);
    assert_eq!(changed, before, "other schema diagnoses are not projected");
    let mut changed = json!(source.replace("n: must be integer", "other: must be integer"));
    let before = changed.clone();
    validation_projection(&mut changed);
    assert_eq!(changed, before, "other paths are not projected");
    let mut changed = json!(source.replace("invalid", "other"));
    validation_projection(&mut changed);
    assert_ne!(changed, expected, "original invalid input retained");
    let mismatched = native.replacen("\"invalid\" is not", "\"changed\" is not", 1);
    assert!(
        std::panic::catch_unwind(|| validation_projection(&mut json!(mismatched))).is_err(),
        "native echoed diagnosis cannot conceal changed input"
    );
}

#[tokio::test]
async fn host_flush_joins_an_admitted_backend_write() {
    struct ReleaseOnDrop(Arc<bindings_storage::GateStorage>);
    impl Drop for ReleaseOnDrop {
        fn drop(&mut self) {
            self.0.release();
        }
    }
    for (backend, storage, _temp) in support::stores() {
        let storage = Arc::new(bindings_storage::GateStorage::new(storage));
        let _release = ReleaseOnDrop(storage.clone());
        let session = Session::open(storage.clone()).unwrap();
        storage.arm(false);
        let writer_session = session.clone();
        let writer = tokio::spawn(async move {
            writer_session
                .commit(|tx| tx.ensure_root_conversation())
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), storage.entered.notified())
            .await
            .unwrap();
        let reader_session = session.clone();
        let mut barrier = tokio::spawn(async move { flush(&reader_session).await });
        // This timeout observes the barrier while the real backend is held. It is
        // a negative assertion, not a quiescence delay in the fixture host.
        let premature = match tokio::time::timeout(Duration::from_millis(20), &mut barrier).await {
            Ok(done) => {
                done.unwrap();
                true
            }
            Err(_) => false,
        };
        storage.release();
        writer.await.unwrap().unwrap();
        if !premature {
            barrier.await.unwrap();
        }
        assert!(
            session
                .conversation(ROOT_CONVERSATION_ID)
                .await
                .unwrap()
                .is_some()
        );
        session.close().await.unwrap();
        assert!(
            !premature,
            "{backend}: flush returned while an admitted backend commit was held"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parallel_model_tools_keep_completion_order_and_call_order_at_their_boundaries() {
    for (backend, storage, _temp) in support::stores() {
        let clock = Arc::new(TestClock(AtomicU64::new(1000)));
        let (models, script) = models(&json!({"responses":[{"calls":[{"id":"first-call","name":"work","arguments":{}},{"id":"second-call","name":"work","arguments":{}}]},{"text":"done"}]}), clock).await;
        let (entered, mut started) = tokio::sync::mpsc::unbounded_channel();
        let gates = script.clone();
        let tool = ToolRegistration::new(serde_json::from_value(json!({"name":"work","description":"parallel gate","parameters":{"type":"object","properties":{}}})).unwrap(), move |inv| {
            let gates = gates.clone();
            let entered = entered.clone();
            Box::pin(async move {
                let call = inv.call.call.id.to_string();
                entered.send((call.clone(), inv.api.identity().invocation.task_id)).unwrap();
                gates.gate(&call, &inv.api.signal()).await;
                inv.api.check_live()?;
                Ok(ToolExecutionResult {
                    output: Some(vec![serde_json::from_value(json!({"type":"text","text":call})).unwrap()]),
                    ..Default::default()
                })
            })
        });
        let observed = Arc::new(Mutex::new(Vec::<Vec<EntryId>>::new()));
        let hook_observed = observed.clone();
        let options = DurableOptions::new(models);
        assert_eq!(
            (options.settings)().tool_execution,
            ToolExecutionMode::Parallel
        );
        options
            .registry
            .install(Extension {
                name: "parallel".into(),
                tools: vec![tool],
                hooks: Hooks {
                    after_tools: Some(Arc::new(move |_, results, _| {
                        let observed = hook_observed.clone();
                        Box::pin(async move {
                            observed.lock().unwrap().push(results);
                            Ok(())
                        })
                    })),
                    ..Default::default()
                },
                ..Default::default()
            })
            .unwrap();
        let durable = Durable::open(storage, options).await.unwrap();
        let root = durable
            .root(ConversationOptions {
                agent: Some(AgentChange {
                    model: pi_ai::Presence::Value(ModelRef {
                        provider: "oracle".into(),
                        model_id: "scripted-1".into(),
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            })
            .await
            .unwrap();
        let submission = root
            .submit(SubmissionDraft::input("parallel"))
            .await
            .unwrap();
        // Neither gate is released until both actual execute callbacks entered.
        let mut tasks = BTreeMap::new();
        for _ in 0..2 {
            let (call, task) = tokio::time::timeout(Duration::from_secs(5), started.recv())
                .await
                .unwrap()
                .unwrap();
            assert!(tasks.insert(call, task).is_none());
        }
        let first = tasks["first-call"];
        let second = tasks["second-call"];
        script.release("second-call");
        let completed = tokio::time::timeout(Duration::from_secs(5), durable.wait_for_task(second))
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            completed.state,
            TaskState::Terminal {
                outcome: TaskOutcome::Completed { .. }
            }
        ));
        assert!(matches!(
            durable.get_task(first).await.unwrap().unwrap().state,
            TaskState::Running { .. }
        ));
        let history = durable.session().history(root.id, None).await.unwrap();
        let partial = history
            .iter()
            .filter(|entry| entry.kind == "pi.tool-result")
            .collect::<Vec<_>>();
        assert_eq!(partial.len(), 1, "{backend}: first call is still gated");
        assert_eq!(partial[0].by_task_id, Some(second));
        assert!(
            observed.lock().unwrap().is_empty(),
            "afterTools waits for the complete round"
        );
        script.release("first-call");
        let receipt = tokio::time::timeout(Duration::from_secs(5), submission.wait())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(receipt.state, SubmissionState::Done { .. }));
        let history = durable.session().history(root.id, None).await.unwrap();
        let results = history
            .iter()
            .filter(|entry| entry.kind == "pi.tool-result")
            .collect::<Vec<_>>();
        assert_eq!(
            results
                .iter()
                .map(|entry| entry.by_task_id.unwrap())
                .collect::<Vec<_>>(),
            [second, first],
            "{backend}: raw entries retain completion order"
        );
        assert_eq!(
            *observed.lock().unwrap(),
            vec![vec![results[1].id, results[0].id]],
            "{backend}: afterTools uses call order"
        );
        let context = j(root.context(None).await.unwrap().messages);
        let tool_ids = |messages: &Value| {
            messages
                .as_array()
                .unwrap()
                .iter()
                .filter(|message| message["role"] == "toolResult")
                .map(|message| message["toolCallId"].as_str().unwrap().to_owned())
                .collect::<Vec<_>>()
        };
        assert_eq!(
            tool_ids(&context),
            ["first-call", "second-call"],
            "{backend}: normalized context uses call order"
        );
        {
            let requests = script.requests.lock().unwrap();
            assert_eq!(requests.len(), 2);
            assert_eq!(
                tool_ids(&requests[1]["context"]["messages"]),
                ["first-call", "second-call"],
                "{backend}: actual provider request uses call order"
            );
        }
        tokio::time::timeout(Duration::from_secs(5), durable.close())
            .await
            .unwrap()
            .unwrap();
    }
}
