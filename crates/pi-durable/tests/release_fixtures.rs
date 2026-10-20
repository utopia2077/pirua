//! Consume immutable source inputs through public native APIs. Expected values never drive actions.
mod support;
use futures_util::future::{Either, select};
use pi_durable::*;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use support::*;
use tokio::sync::watch;

fn undefined() -> Value {
    json!({"$undefined":true})
}
fn error(error: Error) -> Value {
    if error.kind == ErrorKind::Closed {
        json!({"category":"Closed","message":error.message})
    } else {
        json!({"category":"Error","message":error.message})
    }
}
fn ids(value: &Value) -> Vec<TaskId> {
    value
        .as_array()
        .unwrap()
        .iter()
        .map(|id| TaskId::new(id.as_u64().unwrap()).unwrap())
        .collect()
}
fn reference(value: &Value, bindings: &BTreeMap<String, Value>) -> Value {
    match value {
        Value::String(s) if s.starts_with('$') => {
            let mut path = s[1..].split('.');
            let name = path.next().unwrap();
            let mut value = bindings
                .get(name)
                .unwrap_or_else(|| panic!("missing reference {s}"))
                .clone();
            for key in path {
                value = if let Value::Array(a) = value {
                    a[key.parse::<usize>().unwrap()].clone()
                } else {
                    value
                        .get(key)
                        .unwrap_or_else(|| panic!("missing path {s}"))
                        .clone()
                };
            }
            value
        }
        Value::Array(values) => {
            Value::Array(values.iter().map(|v| reference(v, bindings)).collect())
        }
        Value::Object(object) => Value::Object(
            object
                .iter()
                .map(|(k, v)| (k.clone(), reference(v, bindings)))
                .collect(),
        ),
        _ => value.clone(),
    }
}
#[derive(Clone)]
struct RecordingTimer {
    callback: Arc<dyn Fn() + Send + Sync>,
    delay: f64,
    disarmed: Arc<AtomicBool>,
}
struct Context {
    definitions: Mutex<BTreeMap<String, TaskDefinition>>,
    descriptors: Mutex<BTreeMap<String, Value>>,
    aliases: Mutex<BTreeMap<String, Value>>,
    gates: Mutex<BTreeMap<String, watch::Sender<bool>>>,
    events: Mutex<Vec<Value>>,
    publications: Mutex<Vec<Value>>,
    reports: Mutex<Vec<Value>>,
    timers: Mutex<Vec<RecordingTimer>>,
    clock: AtomicU64,
    store: Mutex<Arc<dyn session::Storage>>,
}
impl Context {
    fn bindings(&self) -> BTreeMap<String, Value> {
        let mut b = self.aliases.lock().unwrap().clone();
        b.insert("clock".into(), json!(self.clock.load(Ordering::SeqCst)));
        b
    }
    fn event(&self, value: Value) {
        self.events.lock().unwrap().push(value);
    }
    fn gate(&self, name: &str) -> watch::Sender<bool> {
        self.gates
            .lock()
            .unwrap()
            .entry(name.into())
            .or_insert_with(|| watch::channel(false).0)
            .clone()
    }
    fn install(&self, registry: &Registry, id: &str) {
        let task = self.definitions.lock().unwrap()[id].clone();
        registry
            .install(task.kind().to_string(), vec![task])
            .unwrap();
    }
    async fn actions(
        self: Arc<Self>,
        descriptor: Value,
        actions: Value,
        record: TaskRecord,
        inv: Invocation,
        mode: &str,
    ) -> Result<()> {
        let record_json = serde_json::to_value(&record).unwrap();
        let mut local = serde_json::Map::new();
        self.event(json!({"kind":mode,"definition":descriptor["id"],"taskId":record.id,"checkpoint":record_json["state"]["checkpoint"],"now":self.clock.load(Ordering::SeqCst)}));
        for raw in actions.as_array().unwrap() {
            let mut bindings = self.bindings();
            bindings.insert("task".into(), record_json.clone());
            bindings.insert("local".into(), Value::Object(local.clone()));
            let a = reference(raw, &bindings);
            let action = a["do"].as_str().unwrap();
            let mut result = undefined();
            match action {
                "complete" => {
                    let outcome:TaskOutcome=serde_json::from_value(a.get("outcome").cloned().unwrap_or_else(||json!({"status":"completed","result":a.get("result").cloned().unwrap_or(Value::Null)}))).unwrap();
                    inv.commit(move |_, _| Ok(Some(TaskState::Terminal { outcome })))
                        .await?;
                }
                "next" => {
                    let state = serde_json::from_value(a["state"].clone()).unwrap();
                    inv.commit(move |_, _| Ok(Some(state))).await?;
                }
                "noProgressCommit" => inv.commit(|_, _| Ok(None)).await?,
                "counter" => {
                    let field = a["field"].as_str().unwrap().to_string();
                    let until = a["until"].as_u64().unwrap();
                    inv.commit(move |_, current| {
                        let mut cp = match &current.state {
                            TaskState::Running { checkpoint } => checkpoint.clone(),
                            _ => unreachable!(),
                        };
                        let count = cp[&field].as_u64().unwrap() + 1;
                        cp[&field] = json!(count);
                        Ok(Some(if count >= until {
                            complete(json!(count))
                        } else {
                            TaskState::Running { checkpoint: cp }
                        }))
                    })
                    .await?;
                }
                "sleep" => {
                    inv.sleep_until(a["until"].as_f64().unwrap(), a["checkpoint"].clone())
                        .await?
                }
                "memo" => {
                    let name = a["name"].as_str().unwrap();
                    result = if a["read"] == true {
                        inv.memo(name).await?.unwrap_or_else(undefined)
                    } else {
                        inv.memo_or_insert(name, a["candidate"].clone()).await?
                    };
                }
                "record" => {
                    let id = a
                        .get("id")
                        .and_then(Value::as_u64)
                        .map(|id| TaskId::new(id).unwrap())
                        .unwrap_or(record.id);
                    result = serde_json::to_value(inv.get_task(id).await?).unwrap();
                }
                "ownedTasks" => result = serde_json::to_value(inv.owned_tasks().await?).unwrap(),
                "outcomes" => {
                    result = serde_json::to_value(inv.outcomes(ids(&a["ids"])).await?).unwrap()
                }
                "spawn" => {
                    let definition =
                        self.definitions.lock().unwrap()[a["definition"].as_str().unwrap()].clone();
                    let input = a.get("input").cloned().unwrap_or(Value::Null);
                    let state = a
                        .get("state")
                        .cloned()
                        .map(|v| serde_json::from_value(v).unwrap());
                    result = serde_json::to_value(
                        inv.transact(move |tx, current| {
                            let id = create_task(
                                tx,
                                &definition,
                                input,
                                TaskOptions {
                                    owner: Some(current.id),
                                    ..Default::default()
                                },
                            )?;
                            Ok((id, state))
                        })
                        .await?,
                    )
                    .unwrap();
                }
                "wait" => {
                    let on = ids(&a["on"]);
                    let checkpoint = a["checkpoint"].clone();
                    let policy = if a["policy"] == "failFast" {
                        JoinPolicy::FailFast
                    } else {
                        JoinPolicy::AllSettled
                    };
                    inv.commit(move |_, _| {
                        Ok(Some(TaskState::Waiting {
                            checkpoint,
                            on,
                            policy,
                        }))
                    })
                    .await?;
                }
                "waitForTask" => {
                    result = serde_json::to_value(
                        inv.wait_for_task(TaskId::new(a["id"].as_u64().unwrap()).unwrap())
                            .await?,
                    )
                    .unwrap()
                }
                "gate" => {
                    let gate = self.gate(a["name"].as_str().unwrap());
                    let mut receiver = gate.subscribe();
                    let waiting = async {
                        if !*receiver.borrow() {
                            let _ = receiver.changed().await;
                        }
                    };
                    if a["abortable"] == false {
                        waiting.await;
                    } else if let Either::Right(_) =
                        select(Box::pin(waiting), Box::pin(inv.signal().cancelled())).await
                    {
                        let stored = self.store.lock().unwrap().task(record.id)?.unwrap();
                        self.event(json!({"kind":"signal","taskId":record.id,"abortRequested":stored.abort_requested,"status":stored.state.status()}));
                        if a["tryCommitOnSignal"] == true
                            && let Err(e) =
                                inv.commit(|_, _| Ok(Some(complete(json!("stale"))))).await
                        {
                            self.event(json!({"kind":"rejectedSignalCommit","taskId":record.id,"error":error(e)}));
                        }
                        return Err(Error::request("invocation cancelled"));
                    }
                }
                "lateCommit" => {
                    let state = serde_json::from_value(a["state"].clone()).unwrap();
                    result = match inv.commit(move |_, _| Ok(Some(state))).await {
                        Ok(()) => json!("accepted"),
                        Err(e) => json!({"error":error(e)}),
                    };
                }
                "timer" => {
                    let context = self.clone();
                    let task_id = record.id;
                    let name = a["name"].clone();
                    inv.set_timer(
                        move || {
                            context
                                .event(json!({"kind":"timerCallback","taskId":task_id,"name":name}))
                        },
                        a["delay"].as_f64().unwrap_or(1.0),
                    )?;
                    result = json!("armed");
                }
                "observe" => result = a["value"].clone(),
                "throw" => return Err(Error::request(a["message"].as_str().unwrap())),
                other => panic!("unknown handler action {other}"),
            }
            if let Some(name) = a["as"].as_str() {
                local.insert(name.into(), result.clone());
            }
            if a.get("label").is_some()
                || [
                    "memo",
                    "record",
                    "ownedTasks",
                    "outcomes",
                    "lateCommit",
                    "observe",
                ]
                .contains(&action)
            {
                self.event(json!({"kind":"handlerObservation","taskId":record.id,"label":a["label"].as_str().unwrap_or(action),"value":result}));
            }
        }
        Ok(())
    }
}
async fn flush() {
    for _ in 0..3 {
        tokio::task::yield_now().await;
    }
    tokio::time::sleep(Duration::from_millis(2)).await;
}
fn inspect_json(inspection: Inspection) -> Value {
    json!({"scheduling":inspection.scheduling,"submissions":[],"tasks":inspection.tasks.into_iter().map(|task|{
    let state=match task.state {InspectionState::Running=>json!({"kind":"running"}),InspectionState::Completing=>json!({"kind":"completing"}),InspectionState::Waiting{on}=>json!({"kind":"waiting","on":on}),InspectionState::Sleeping{until}=>json!({"kind":"sleeping","until":until}),InspectionState::Blocked{reason}=>json!({"kind":"blocked","reason":reason}),InspectionState::Ready=>json!({"kind":"ready","migrates":false})};json!({"record":task.record,"state":state})}).collect::<Vec<_>>()})
}
struct Pending {
    value: Arc<Mutex<Value>>,
    task: tokio::task::JoinHandle<Value>,
}
async fn execute(case: &Value, backend: &str) -> Value {
    let input = &case["input"];
    let temp = Temp::new();
    let make_store = || -> Arc<dyn session::Storage> {
        if backend == "memory" {
            Arc::new(session::MemoryStorage::new())
        } else {
            Arc::new(session::SqliteStorage::open(&temp.0).unwrap())
        }
    };
    let mut store = make_store();
    let mut writes = vec![session::StorageWrite::Conversation {
        value: session::ConversationRecord {
            id: session::ROOT_CONVERSATION_ID,
            parent: None,
            owner: None,
        },
    }];
    if let Some(seed) = input["seed"].as_array() {
        writes.extend(
            seed.iter()
                .map(|v| serde_json::from_value(v.clone()).unwrap()),
        );
    }
    store.commit(&writes).unwrap();
    if input["seededReopen"] == true {
        store.close().unwrap();
        store = make_store();
    }
    let context = Arc::new(Context {
        definitions: Mutex::new(BTreeMap::new()),
        descriptors: Mutex::new(BTreeMap::new()),
        aliases: Mutex::new(BTreeMap::new()),
        gates: Mutex::new(BTreeMap::new()),
        events: Mutex::new(Vec::new()),
        publications: Mutex::new(Vec::new()),
        reports: Mutex::new(Vec::new()),
        timers: Mutex::new(Vec::new()),
        clock: AtomicU64::new(input["clock"].as_u64().unwrap_or(1000)),
        store: Mutex::new(store.clone()),
    });
    let registry = Registry::new();
    for descriptor in input["tasks"].as_array().unwrap() {
        let initial = descriptor
            .get("initial")
            .cloned()
            .unwrap_or_else(|| json!({"phase":"run"}));
        let mut definition = TaskDefinition::new(
            descriptor["name"].as_str().unwrap(),
            descriptor["version"].as_u64().unwrap_or(1) as u32,
            move |input| {
                Ok(reference(
                    &initial,
                    &BTreeMap::from([("input".into(), input.clone())]),
                ))
            },
        )
        .unwrap();
        for (phase, actions) in descriptor["phases"].as_object().unwrap() {
            let context = context.clone();
            let descriptor = descriptor.clone();
            let actions = actions.clone();
            definition = definition.phase(phase, move |task, inv| {
                context
                    .clone()
                    .actions(descriptor.clone(), actions.clone(), task, inv, "phase")
            });
        }
        let cx = context.clone();
        let desc = descriptor.clone();
        let abort = descriptor.get("abort").cloned().unwrap_or_else(
            || json!([{"do":"complete","outcome":{"status":"aborted","reason":"oracle"}}]),
        );
        definition = definition.abort(move |task, inv| {
            cx.clone()
                .actions(desc.clone(), abort.clone(), task, inv, "abort")
        });
        context
            .definitions
            .lock()
            .unwrap()
            .insert(descriptor["id"].as_str().unwrap().into(), definition);
        context.descriptors.lock().unwrap().insert(
            descriptor["id"].as_str().unwrap().into(),
            descriptor.clone(),
        );
    }
    let installed = input
        .get("installed")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_else(|| {
            context
                .definitions
                .lock()
                .unwrap()
                .keys()
                .map(|k| json!(k))
                .collect()
        });
    for id in installed {
        context.install(&registry, id.as_str().unwrap());
    }
    let options = {
        let cx = context.clone();
        let timer_cx = context.clone();
        let report_cx = context.clone();
        SchedulerOptions {
            restart_limit: input["settings"]["restartLimit"].as_u64().unwrap_or(5),
            now: Arc::new(move || cx.clock.load(Ordering::SeqCst) as f64),
            on_wake: Some(Arc::new(|_| {})),
            on_report: Some(Arc::new(move |report| {
                report_cx.reports.lock().unwrap().push(
                    json!({"type":report.kind,"taskId":report.task_id,"error":error(report.error)}),
                )
            })),
            timer_host: Some(Arc::new(move |callback, delay| {
                let disarmed = Arc::new(AtomicBool::new(false));
                timer_cx.timers.lock().unwrap().push(RecordingTimer {
                    callback,
                    delay,
                    disarmed: disarmed.clone(),
                });
                Timer::new(move || {
                    disarmed.store(true, Ordering::SeqCst);
                })
            })),
            ..Default::default()
        }
    };
    let open = |store: Arc<dyn session::Storage>| {
        let cx = context.clone();
        let registry = registry.clone();
        let options = options.clone();
        let observer = context.clone();
        async move {
            let session =
                Session::with_clock(store, move || cx.clock.load(Ordering::SeqCst) as f64).unwrap();
            let scheduler = Scheduler::open(session.clone(), registry, options)
                .await
                .unwrap();
            let mut subscription = session.subscribe_commits().await.unwrap();
            tokio::spawn(async move {
                while let Some(publication) = subscription.next().await {
                    let tasks: Vec<_> = publication
                        .changes
                        .iter()
                        .filter_map(|change| match change {
                            session::CommitChange::Table(session::StorageWrite::Task { value }) => {
                                Some(value.clone())
                            }
                            _ => None,
                        })
                        .collect();
                    if !tasks.is_empty() {
                        observer
                            .publications
                            .lock()
                            .unwrap()
                            .push(json!({"seq":publication.seq,"tasks":tasks}));
                    }
                }
            });
            scheduler
        }
    };
    let mut scheduler = open(store.clone()).await;
    let mut observations = Vec::new();
    let mut pending = BTreeMap::<String, Pending>::new();
    flush().await;
    for (index, raw) in input["operations"].as_array().unwrap().iter().enumerate() {
        let op = reference(raw, &context.bindings());
        let kind = op["op"].as_str().unwrap();
        let id = || TaskId::new(op["id"].as_u64().unwrap()).unwrap();
        let mut result = undefined();
        match kind {
            "create" => {
                let definition =
                    context.definitions.lock().unwrap()[op["definition"].as_str().unwrap()].clone();
                result = serde_json::to_value(
                    scheduler
                        .create_task(
                            &definition,
                            op.get("input").cloned().unwrap_or(Value::Null),
                            TaskOptions {
                                conversation_id: Some(
                                    ConversationId::new(op["conversationId"].as_u64().unwrap_or(1))
                                        .unwrap(),
                                ),
                                owner: op["owner"].as_u64().map(|id| TaskId::new(id).unwrap()),
                                background: op["background"] == true,
                                abandon_on_restart: op["abandonOnRestart"] == true,
                                ..Default::default()
                            },
                        )
                        .await
                        .unwrap(),
                )
                .unwrap();
            }
            "resume" => {
                scheduler.resume();
                flush().await;
                result = json!("resumed");
            }
            "inspect" => {
                flush().await;
                result = inspect_json(scheduler.inspect().await.unwrap());
            }
            "wake" => {
                flush().await;
                result = serde_json::to_value(scheduler.wake().await.unwrap()).unwrap();
            }
            "get" => {
                result = serde_json::to_value(scheduler.get_task(id()).await.unwrap()).unwrap()
            }
            "wait" => {
                result = serde_json::to_value(wait(&scheduler, id()).await).unwrap();
                flush().await;
            }
            "until" => {
                tokio::time::timeout(Duration::from_secs(5), async {
                    loop {
                        let done = if let Some(status) = op["status"].as_str() {
                            scheduler
                                .get_task(id())
                                .await
                                .unwrap()
                                .is_some_and(|task| task.state.status() == status)
                        } else {
                            context.events.lock().unwrap().iter().any(|event| {
                                event["kind"] == op["event"]
                                    && (op.get("definition").is_none()
                                        || event["definition"] == op["definition"])
                            })
                        };
                        if done {
                            break;
                        }
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap_or_else(|_| panic!("{} until {}", case["id"], op));
                flush().await;
                result = if op.get("id").is_some() {
                    serde_json::to_value(scheduler.get_task(id()).await.unwrap()).unwrap()
                } else {
                    Value::Array(
                        context
                            .events
                            .lock()
                            .unwrap()
                            .iter()
                            .filter(|e| e["kind"] == op["event"])
                            .cloned()
                            .collect(),
                    )
                };
            }
            "abort" => {
                result = serde_json::to_value(scheduler.abort_task(id()).await.unwrap()).unwrap();
                flush().await;
            }
            "retry" => {
                scheduler.retry_task(id()).await.unwrap();
                flush().await;
            }
            "abortConversation" => {
                scheduler
                    .abort_conversation(
                        ConversationId::new(op["id"].as_u64().unwrap_or(1)).unwrap(),
                        op["background"] == true,
                    )
                    .await
                    .unwrap();
                flush().await;
            }
            "release" => {
                context
                    .gate(op["name"].as_str().unwrap())
                    .send_replace(true);
                flush().await;
                result = json!("released");
            }
            "advance" => {
                context
                    .clock
                    .store(op["to"].as_u64().unwrap(), Ordering::SeqCst);
                if op["resume"] != false {
                    scheduler.resume();
                }
                flush().await;
                result = op["to"].clone();
            }
            "install" => {
                context.install(&registry, op["definition"].as_str().unwrap());
                flush().await;
                result = json!("installed");
            }
            "uninstall" => {
                let task =
                    context.definitions.lock().unwrap()[op["definition"].as_str().unwrap()].clone();
                registry.uninstall(task.kind());
                flush().await;
                result = json!("uninstalled");
            }
            "events" => {
                flush().await;
                result = Value::Array(context.events.lock().unwrap().clone());
            }
            "timers" => result = Value::Array(
                context
                    .timers
                    .lock()
                    .unwrap()
                    .iter()
                    .map(
                        |t| json!({"delayMs":t.delay,"disarmed":t.disarmed.load(Ordering::SeqCst)}),
                    )
                    .collect(),
            ),
            "fireTimers" => {
                let timers = context.timers.lock().unwrap().clone();
                for timer in timers {
                    (timer.callback)();
                }
                flush().await;
                result = json!("fired retained host callbacks");
            }
            "close" => {
                close(&scheduler).await;
                result = json!("closed");
            }
            "rawTask" => result = serde_json::to_value(store.task(id()).unwrap()).unwrap(),
            "reopen" => {
                close(&scheduler).await;
                store = make_store();
                *context.store.lock().unwrap() = store.clone();
                scheduler = open(store.clone()).await;
                flush().await;
                result = json!("reopened paused");
            }
            "startWait" => {
                let sc = scheduler.clone();
                let task_id = op.get("id").map(|_| id());
                let value = Arc::new(Mutex::new(json!({"done":false})));
                let out = value.clone();
                let task = tokio::spawn(async move {
                    let result = if let Some(id) = task_id {
                        serde_json::to_value(sc.wait_for_task(id).await.unwrap()).unwrap()
                    } else {
                        sc.wait_for_idle(None).await.unwrap();
                        undefined()
                    };
                    *out.lock().unwrap() = json!({"done":true,"value":result});
                    result
                });
                flush().await;
                result = value.lock().unwrap()["done"].clone();
                pending.insert(op["name"].as_str().unwrap().into(), Pending { value, task });
            }
            "waitStatus" => {
                flush().await;
                result = pending[op["name"].as_str().unwrap()]
                    .value
                    .lock()
                    .unwrap()
                    .clone();
            }
            "joinWait" => {
                let p = pending.remove(op["name"].as_str().unwrap()).unwrap();
                result = p.task.await.unwrap();
                flush().await;
            }
            other => panic!("unknown host operation {other}"),
        }
        if let Some(name) = op["as"].as_str() {
            context
                .aliases
                .lock()
                .unwrap()
                .insert(name.into(), result.clone());
        }
        observations.push(json!({"label":op["label"].as_str().or(op["as"].as_str()).map(str::to_owned).unwrap_or_else(||format!("{index}:{kind}")),"value":result}));
    }
    flush().await;
    let events = context.events.lock().unwrap().clone();
    let publications = context.publications.lock().unwrap().clone();
    let reports = context.reports.lock().unwrap().clone();
    for gate in context.gates.lock().unwrap().values() {
        gate.send_replace(true);
    }
    close(&scheduler).await;
    context.definitions.lock().unwrap().clear();
    json!({"observations":observations,"events":events,"taskPublications":publications,"reports":reports})
}
/// Normalize representational differences only: JSON number spelling, native error
/// category, and independent task collection/event order. Per-task event order stays exact.
fn normalize(value: &mut Value) {
    match value {
        Value::Number(n) => {
            if let Some(f) = n.as_f64()
                && f.fract() == 0.0
                && (0.0..=9_007_199_254_740_991.0).contains(&f)
            {
                *value = json!(f as u64);
            }
        }
        Value::Array(a) => {
            for v in a.iter_mut() {
                normalize(v);
            }
            if !a.is_empty()
                && a.iter()
                    .all(|v| v.get("taskId").is_some() && v.get("kind").is_some())
            {
                a.sort_by_key(|v| v["taskId"].as_u64());
            }
            if a.iter()
                .all(|v| v.get("record").and_then(|r| r.get("id")).is_some())
            {
                a.sort_by_key(|v| v["record"]["id"].as_u64());
            }
        }
        Value::Object(o) => {
            for v in o.values_mut() {
                normalize(v);
            }
            if o.get("category") == Some(&json!("Error"))
                && o.get("message") == Some(&json!("Session is closed"))
            {
                o.insert("category".into(), json!("Closed"));
                o.insert("message".into(), json!("session closed"));
            }
            if o.get("category") == Some(&json!("Closed")) {
                o.insert("message".into(), json!("session closed"));
            }
            if o.contains_key("error") && o.get("kind") == Some(&json!("blocked")) {
                o.remove("error");
            }
        }
        _ => {}
    }
}
fn group_events(value: &Value) -> BTreeMap<u64, Vec<Value>> {
    let mut groups = BTreeMap::new();
    for event in value.as_array().unwrap() {
        groups
            .entry(event["taskId"].as_u64().unwrap())
            .or_insert_with(Vec::new)
            .push(event.clone());
    }
    groups
}
fn report_projection(reports: &Value, native: bool) -> BTreeMap<String, usize> {
    let mut projected = BTreeMap::new();
    for report in reports.as_array().unwrap() {
        let task_id = if native {
            report["taskId"].clone()
        } else {
            report["task"]["id"].clone()
        };
        let mut message = report["error"]["message"].as_str().unwrap().to_string();
        if report["type"] == "task_parked" {
            message = message
                .split("; it is parked until ")
                .next()
                .unwrap()
                .to_string();
        }
        let key=serde_json::to_string(&json!({"type":report["type"],"taskId":task_id,"category":report["error"]["category"],"message":message})).unwrap();
        *projected.entry(key).or_default() += 1;
    }
    projected
}
fn publications_match(
    actual: &Value,
    expected: &Value,
    existing_tasks: &std::collections::BTreeSet<u64>,
) -> std::result::Result<(), String> {
    let actual = actual.as_array().unwrap();
    let expected = expected.as_array().unwrap();
    if actual.len() != expected.len() {
        return Err("publication count changed".into());
    }
    let mut previous = 0;
    for publication in actual {
        let seq = publication["seq"].as_u64().unwrap();
        if seq <= previous {
            return Err("commit sequences must increase uniquely".into());
        }
        previous = seq;
    }
    fn batches(values: &[Value]) -> BTreeMap<String, usize> {
        let mut batches = BTreeMap::new();
        for publication in values {
            let mut tasks = publication["tasks"].as_array().unwrap().clone();
            // Object insertion order is not part of committed record identity.
            for task in &mut tasks {
                task.sort_all_objects();
            }
            tasks.sort_by_key(|t| t["id"].as_u64());
            *batches
                .entry(serde_json::to_string(&tasks).unwrap())
                .or_default() += 1;
        }
        batches
    }
    if batches(actual) != batches(expected) {
        return Err("atomic batch membership or full task fields changed".into());
    }
    fn per_task(values: &[Value]) -> BTreeMap<u64, Vec<Value>> {
        let mut tasks = BTreeMap::new();
        for publication in values {
            for task in publication["tasks"].as_array().unwrap() {
                tasks
                    .entry(task["id"].as_u64().unwrap())
                    .or_insert_with(Vec::new)
                    .push(task.clone());
            }
        }
        tasks
    }
    if per_task(actual) != per_task(expected) {
        return Err("per-task committed record order changed".into());
    }
    let mut created = BTreeMap::new();
    let mut terminals = BTreeMap::new();
    let mut owners = BTreeMap::new();
    for publication in actual {
        let seq = publication["seq"].as_u64().unwrap();
        for task in publication["tasks"].as_array().unwrap() {
            let id = task["id"].as_u64().unwrap();
            created.entry(id).or_insert(seq);
            if let Some(owner) = task["owner"].as_u64() {
                owners.insert(id, owner);
            }
            if task["state"]["status"] == "terminal" {
                terminals.insert(id, seq);
            }
        }
    }
    for publication in actual {
        let seq = publication["seq"].as_u64().unwrap();
        for task in publication["tasks"].as_array().unwrap() {
            if let Some(owner) = task["owner"].as_u64()
                && !existing_tasks.contains(&owner)
            {
                match created.get(&owner) {
                    Some(created) if *created <= seq => {}
                    Some(_) => return Err("owned work precedes owner creation".into()),
                    None => {
                        return Err(
                            "owner reference has no task creation or existing record".into()
                        );
                    }
                }
            }
            let references = task["state"]["on"].as_array().cloned().unwrap_or_default();
            for id in references {
                if created
                    .get(&id.as_u64().unwrap())
                    .is_some_and(|created| *created > seq)
                {
                    return Err("wait precedes target creation".into());
                }
            }
        }
    }
    let mut waits = BTreeMap::<u64, Vec<u64>>::new();
    for publication in actual {
        let seq = publication["seq"].as_u64().unwrap();
        for task in publication["tasks"].as_array().unwrap() {
            let id = task["id"].as_u64().unwrap();
            if task["state"]["status"] == "waiting" {
                waits.insert(
                    id,
                    task["state"]["on"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|id| id.as_u64().unwrap())
                        .collect(),
                );
            } else if task["state"]["status"] == "running"
                && let Some(on) = waits.remove(&id)
                && task["abortRequested"] != true
            {
                for target in on {
                    if created.contains_key(&target)
                        && !terminals
                            .get(&target)
                            .is_some_and(|terminal| *terminal <= seq)
                    {
                        return Err("durable waiter resumed before target terminal".into());
                    }
                }
            }
        }
    }
    for (child, parent) in owners {
        if let (Some(child), Some(parent)) = (terminals.get(&child), terminals.get(&parent))
            && child > parent
        {
            return Err("dependent parent terminal precedes child terminal".into());
        }
    }
    Ok(())
}
#[test]
fn publication_projection_preserves_fields_independent_of_object_order() {
    let expected: Value = serde_json::from_str(
        r#"[{"seq":2,"tasks":[{"id":2,"state":{"status":"pending","checkpoint":{"phase":"run","nested":{"a":1,"b":2},"ordered":[1,2]}}}]}]"#,
    )
    .unwrap();
    let actual: Value = serde_json::from_str(
        r#"[{"tasks":[{"state":{"checkpoint":{"ordered":[1,2],"nested":{"b":2,"a":1},"phase":"run"},"status":"pending"},"id":2}],"seq":2}]"#,
    )
    .unwrap();
    assert_eq!(actual, expected);
    assert!(publications_match(&actual, &expected, &Default::default()).is_ok());

    let mut changed = actual.clone();
    changed[0]["tasks"][0]["state"]["checkpoint"]["nested"]["a"] = json!(3);
    assert!(publications_match(&changed, &expected, &Default::default()).is_err());
    let mut missing = actual.clone();
    missing[0]["tasks"][0]["state"]["checkpoint"]["nested"]
        .as_object_mut()
        .unwrap()
        .remove("b");
    assert!(publications_match(&missing, &expected, &Default::default()).is_err());
    let mut reordered = actual;
    reordered[0]["tasks"][0]["state"]["checkpoint"]["ordered"] = json!([2, 1]);
    assert!(publications_match(&reordered, &expected, &Default::default()).is_err());
}
#[test]
fn publication_projection_rejects_missing_coalesced_and_reversed_dependent_batches() {
    let child = json!({"id":3,"owner":2,"state":{"status":"terminal","outcome":{"status":"completed","result":null}}});
    let parent = json!({"id":2,"state":{"status":"terminal","outcome":{"status":"completed","result":null}}});
    let original = json!([{"seq":2,"tasks":[child.clone()]},{"seq":3,"tasks":[parent.clone()]}]);
    assert!(
        publications_match(
            &json!([{"seq":2,"tasks":[child.clone()]}]),
            &original,
            &std::collections::BTreeSet::from([2, 3])
        )
        .is_err()
    );
    assert!(
        publications_match(
            &json!([{"seq":2,"tasks":[child.clone(),parent.clone()]}]),
            &original,
            &std::collections::BTreeSet::from([2, 3])
        )
        .is_err()
    );
    assert!(
        publications_match(
            &json!([{"seq":2,"tasks":[parent]},{"seq":3,"tasks":[child]}]),
            &original,
            &std::collections::BTreeSet::from([2, 3])
        )
        .is_err()
    );
}
#[test]
fn publication_projection_rejects_owner_reference_before_creation() {
    let parent = json!({"id":2,"state":{"status":"pending","checkpoint":{"phase":"run"}}});
    let child = json!({"id":3,"owner":2,"state":{"status":"pending","checkpoint":{"phase":"run"}}});
    let expected = json!([{"seq":2,"tasks":[parent.clone()]},{"seq":3,"tasks":[child.clone()]}]);
    let reversed = json!([{"seq":2,"tasks":[child.clone()]},{"seq":3,"tasks":[parent.clone()]}]);
    assert!(
        publications_match(&reversed, &expected, &Default::default()).is_err(),
        "owner-dependent creation batches must not commute"
    );
}
#[test]
fn publication_projection_rejects_actual_fixture_owner_creation_reordering() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../conformance/durable-v1.4.0/fixtures/scheduler.json"
    ))
    .unwrap();
    let case = fixture["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|case| case["id"] == "scheduler-completing-owned-work-hold")
        .unwrap();
    let expected = &case["expected"]["memory"]["taskPublications"];
    let mut reordered = expected.as_array().unwrap().clone();
    let child = reordered
        .iter()
        .position(|batch| {
            batch["tasks"]
                .as_array()
                .unwrap()
                .iter()
                .any(|task| task.get("owner").is_some() && task["state"]["status"] == "pending")
        })
        .unwrap();
    let child = reordered.remove(child);
    reordered.insert(0, child);
    for (index, batch) in reordered.iter_mut().enumerate() {
        batch["seq"] = json!(index + 2);
    }
    assert!(
        publications_match(&Value::Array(reordered), expected, &Default::default()).is_err(),
        "frozen owned-work stream lost its owner creation dependency"
    );
}
#[test]
fn publication_projection_allows_existing_owners_and_independent_creation_batches() {
    let parent = json!({"id":2,"state":{"status":"running","checkpoint":{"phase":"run"}}});
    let child = json!({"id":3,"owner":2,"state":{"status":"pending","checkpoint":{"phase":"run"}}});
    let expected = json!([{"seq":2,"tasks":[parent.clone()]},{"seq":3,"tasks":[child.clone()]}]);
    let reordered = json!([{"seq":2,"tasks":[child.clone()]},{"seq":3,"tasks":[parent.clone()]}]);
    assert!(
        publications_match(
            &reordered,
            &expected,
            &std::collections::BTreeSet::from([2])
        )
        .is_ok(),
        "an existing owner need not publish again before child creation"
    );
    let only_child = json!([{"seq":2,"tasks":[child.clone()]}]);
    assert!(
        publications_match(
            &only_child,
            &only_child,
            &std::collections::BTreeSet::from([2])
        )
        .is_ok()
    );
    let same_batch = json!([{"seq":2,"tasks":[parent.clone(),child]}]);
    assert!(
        publications_match(&same_batch, &same_batch, &Default::default()).is_ok(),
        "same-batch live owner and child admission is atomic"
    );
    let independent = json!({"id":4,"state":{"status":"pending","checkpoint":{"phase":"run"}}});
    let expected =
        json!([{"seq":2,"tasks":[parent.clone()]},{"seq":3,"tasks":[independent.clone()]}]);
    let reordered = json!([{"seq":2,"tasks":[independent]},{"seq":3,"tasks":[parent]}]);
    assert!(
        publications_match(&reordered, &expected, &Default::default()).is_ok(),
        "independent task batches can commute"
    );
}
#[test]
fn immutable_release_inputs_execute_on_native_memory_and_sqlite() {
    runtime().block_on(async {
        let fixture: Value = serde_json::from_str(include_str!(
            "../../../conformance/durable-v1.4.0/fixtures/scheduler.json"
        ))
        .unwrap();
        let mut executions = 0;
        for case in fixture["cases"].as_array().unwrap() {
            for (backend, expected) in case["expected"].as_object().unwrap() {
                let mut actual =
                    tokio::time::timeout(Duration::from_secs(12), execute(case, backend))
                        .await
                        .unwrap_or_else(|_| panic!("{} {backend}: execution timeout", case["id"]));
                std::fs::write(
                    format!(
                        "/tmp/pi-v140-native-raw-{}-{backend}.json",
                        case["id"].as_str().unwrap()
                    ),
                    serde_json::to_string_pretty(&actual).unwrap(),
                )
                .unwrap();
                let mut expected = expected.clone();
                normalize(&mut actual);
                normalize(&mut expected);
                if actual["observations"] != expected["observations"] {
                    std::fs::write(
                        format!(
                            "/tmp/pi-v140-native-{}-{backend}.json",
                            case["id"].as_str().unwrap()
                        ),
                        serde_json::to_string_pretty(&actual).unwrap(),
                    )
                    .unwrap();
                }
                assert_eq!(
                    actual["observations"], expected["observations"],
                    "{} {backend}: host observations",
                    case["id"]
                );
                assert_eq!(
                    group_events(&actual["events"]),
                    group_events(&expected["events"]),
                    "{} {backend}: per-task handler/signal order",
                    case["id"]
                );
                let existing_tasks = case["input"]["seed"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter(|write| write["type"] == "task")
                    .map(|write| write["value"]["id"].as_u64().unwrap())
                    .collect();
                publications_match(
                    &actual["taskPublications"],
                    &expected["taskPublications"],
                    &existing_tasks,
                )
                .unwrap_or_else(|error| panic!("{} {backend}: {error}", case["id"]));
                assert_eq!(
                    report_projection(&actual["reports"], true),
                    report_projection(&expected["reports"], false),
                    "{} {backend}: diagnostic class/count/task and semantic message",
                    case["id"]
                );
                executions += 1;
            }
        }
        assert_eq!(executions, 41);
    });
}
