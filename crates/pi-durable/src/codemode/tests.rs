//! Real worker / native storage evidence. Expected source state never drives execution.
use super::*;
use crate::session::*;
use pi_ai::Presence;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
#[allow(dead_code)]
#[path = "../../tests/runtime_support/provider.rs"]
mod provider;
#[allow(dead_code)]
#[path = "../../tests/codemode_support/storage.rs"]
mod storage;
use storage::CaptureStorage;
fn j(v: impl serde::Serialize) -> Value {
    serde_json::to_value(v).unwrap()
}
fn worker() -> Sandbox {
    let path = std::env::var_os("PIRUA_CODEMODE_WORKER")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            std::env::current_exe()
                .unwrap()
                .parent()
                .unwrap()
                .parent()
                .unwrap()
                .join("pi-codemode-worker")
        });
    assert!(
        path.is_file(),
        "Build matching worker first: {}",
        path.display()
    );
    Sandbox::new(pi_codemode::SandboxOptions::new(path)).unwrap()
}
async fn eventually(mut check: impl AsyncFnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if check().await {
                return;
            }
            tokio::time::sleep(Duration::from_millis(2)).await
        }
    })
    .await
    .expect("CodeMode condition timeout")
}
#[derive(Default)]
struct Events {
    files: Mutex<Vec<Value>>,
    vm: Mutex<Vec<Value>>,
    effects: Mutex<BTreeMap<String, usize>>,
    effect_events: Mutex<Vec<Value>>,
    storage: Mutex<Option<Arc<CaptureStorage>>>,
    gates: Mutex<BTreeMap<String, Arc<tokio::sync::Notify>>>,
    released: Mutex<Vec<String>>,
    code_task: AtomicU64,
}
impl Events {
    fn release(&self, name: &str) {
        self.released.lock().unwrap().push(name.into());
        if let Some(n) = self.gates.lock().unwrap().get(name) {
            n.notify_waiters()
        }
    }
    async fn wait(&self, name: &str, signal: pi_ai::CancellationToken) {
        let notify = self
            .gates
            .lock()
            .unwrap()
            .entry(name.into())
            .or_default()
            .clone();
        loop {
            let n = notify.notified();
            if self.released.lock().unwrap().iter().any(|s| s == name) {
                return;
            }
            tokio::select! {_=n=>(),_=signal.cancelled()=>return}
        }
    }
    fn count(&self, name: &str) -> usize {
        *self.effects.lock().unwrap().get(name).unwrap_or(&0)
    }
}
fn snapshot(storage: &CaptureStorage, def: DocDefinition, scope: Scope) -> Option<Value> {
    let address = def.address(scope, None).unwrap();
    match storage.find_document(&address, DocumentPoint::Current) {
        Ok(Some(record)) => storage
            .document(record.identity.id, DocumentPoint::Current)
            .unwrap()
            .map(|d| d.value),
        Ok(None) => None,
        Err(e) if e.kind == ErrorKind::Closed => storage
            .committed_docs
            .lock()
            .unwrap()
            .get(&address)
            .cloned(),
        Err(e) => panic!("native snapshot read: {e}"),
    }
}
struct Harness {
    durable: Durable,
    root: Conversation,
    options: DurableOptions,
    state: Arc<provider::ScriptState>,
    events: Arc<Events>,
    store: Arc<CaptureStorage>,
    wall: Arc<AtomicU64>,
    clock: Arc<provider::TestClock>,
    sandbox: Sandbox,
}
impl Harness {
    async fn open(
        inner: Arc<dyn Storage>,
        code: CodeModeOptions,
        tools: Vec<ToolRegistration>,
        hooks: Hooks,
    ) -> Self {
        Self::open_with(inner, code, tools, hooks, Arc::new(Events::default())).await
    }
    async fn open_with(
        inner: Arc<dyn Storage>,
        mut code: CodeModeOptions,
        tools: Vec<ToolRegistration>,
        hooks: Hooks,
        events: Arc<Events>,
    ) -> Self {
        let store = Arc::new(CaptureStorage::new(inner));
        *events.storage.lock().unwrap() = Some(store.clone());
        let captured = events.clone();
        code.seed = Arc::new(|| Ok(424242));
        code.observe = Some(Arc::new(move |mut v| {
            let observed = captured.storage.lock().unwrap().clone().unwrap();
            if v["kind"] == "start" {
                captured
                    .code_task
                    .store(v["taskId"].as_u64().unwrap(), Ordering::SeqCst);
            }
            let id = captured.code_task.load(Ordering::SeqCst);
            if id > 0 {
                let scope = Scope::Task {
                    task_id: TaskId::new(id).unwrap(),
                };
                v["taskId"] = json!(id);
                v["committedRun"] = j(snapshot(
                    observed.as_ref(),
                    codemode_run_doc(),
                    scope.clone(),
                ));
                v["committedJournal"] =
                    j(snapshot(observed.as_ref(), codemode_journal_doc(), scope));
            }
            captured.vm.lock().unwrap().push(v);
        }));
        let clock = Arc::new(provider::TestClock(AtomicU64::new(1000)));
        let (models, state) = provider::models(&json!({}), clock.clone()).await;
        let wall = Arc::new(AtomicU64::new(1000));
        let w = wall.clone();
        let mut options = DurableOptions::new(models);
        options.scheduler.now = Arc::new(move || w.load(Ordering::SeqCst) as f64);
        options.monotonic_now = Arc::new(|| 0.);
        let host_clock = clock.clone();
        options.request_runtime = Arc::new(move |c| {
            pi_ai::RequestRuntime::with_noop_telemetry(
                c,
                host_clock.clone(),
                Arc::new(provider::Spawner),
            )
        });
        options.provider_session_id = Arc::new(|| Ok("fixture-session-1".into()));
        let env_events = events.clone();
        options.env = Some(Arc::new(move |_| {
            let events = env_events.clone();
            Box::pin(async move {
                Ok(Some(pi_coding_runtime::tools::ExecutionToolContext {
                    env: Arc::new(environment::RecordingEnv {
                        local: pi_coding_runtime::tools::LocalExecutionEnv::new(
                            std::env::temp_dir(),
                        ),
                        events,
                    }),
                }))
            })
        }));
        let sandbox = code.sandbox.clone();
        options
            .registry
            .install(create_codemode_extension(code).unwrap())
            .unwrap();
        options
            .registry
            .install(Extension {
                name: "native-fixture".into(),
                tools,
                hooks,
                ..Default::default()
            })
            .unwrap();
        let durable = Durable::open(store.clone(), options.clone()).await.unwrap();
        let root = durable
            .root(ConversationOptions {
                agent: Some(AgentChange {
                    model: Presence::Value(ModelRef {
                        provider: "oracle".into(),
                        model_id: "scripted-1".into(),
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            })
            .await
            .unwrap();
        Self {
            durable,
            root,
            options,
            state,
            events,
            store,
            wall,
            clock,
            sandbox,
        }
    }
    fn advance(&self, now: u64) {
        self.wall.store(now, Ordering::SeqCst);
        self.clock.0.store(now, Ordering::SeqCst);
    }
    async fn submit(&self, source: &str) -> Submission {
        self.state.responses.lock().unwrap().extend([json!({"calls":[{"id":format!("script-{}",self.events.vm.lock().unwrap().len()),"name":"codemode","arguments":{"code":source}}]}),json!({"text":"done"})]);
        self.root
            .submit(SubmissionDraft::input("run script"))
            .await
            .unwrap()
    }
    async fn run(&self, source: &str) -> Value {
        let submission = self.submit(source).await;
        tokio::time::timeout(Duration::from_secs(10), submission.wait())
            .await
            .unwrap()
            .unwrap();
        self.result().await
    }
    async fn result(&self) -> Value {
        let context = self.root.context(None).await.unwrap();
        let result = context
            .entries
            .iter()
            .rev()
            .find(|e| e.kind == "pi.tool-result")
            .expect("tool result");
        let mut value = result.data.clone().unwrap_or_else(|| json!({}));
        if let Some(o) = result
            .model
            .as_ref()
            .and_then(|v| v.first())
            .and_then(Value::as_object)
        {
            for (k, v) in o {
                value[k] = v.clone();
            }
        }
        if value.get("content").is_some() {
            value["output"] = value["content"].clone();
        }
        value
    }
    async fn close(&self) {
        tokio::time::timeout(Duration::from_secs(10), self.durable.close())
            .await
            .unwrap()
            .unwrap();
    }
}
fn text(result: &Value) -> String {
    result["output"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|v| v["text"].as_str())
        .collect::<Vec<_>>()
        .join("\n")
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_worker_commits_start_journal_counts_and_success_store() {
    for inner in [
        Arc::new(MemoryStorage::new()) as Arc<dyn Storage>,
        sqlite().0,
    ] {
        let mut code = CodeModeOptions::new(worker());
        code.globals.push(CodeModeGlobal {
            name: "echo".into(),
            spread: false,
            execute: Arc::new(|args, api| {
                Box::pin(async move {
                    api.check_live()?;
                    Ok(args)
                })
            }),
        });
        let h = Harness::open(inner, code, Vec::new(), Default::default()).await;
        let r = h
            .run("const v=await echo(null); store('k',2); text(v===null); return [Date.now(),7]")
            .await;
        assert_eq!(r["isError"], false, "{r}");
        assert!(text(&r).contains("[1000,7]"));
        let vm = h.events.vm.lock().unwrap().clone();
        let start = vm.iter().find(|v| v["kind"] == "start").unwrap();
        assert_eq!(start["committedRun"]["start"]["seed"], 424242);
        let delivery = vm.iter().find(|v| v["kind"] == "deliver").unwrap();
        assert_eq!(delivery["committedJournal"]["firstIssued"], 1);
        assert_eq!(
            delivery["committedJournal"]["entries"][0]["global"]["args"],
            Value::Null
        );
        assert_eq!(
            delivery["committedJournal"]["entries"][0]["global"]["outcome"]["value"],
            Value::Null
        );
        assert_eq!(
            snapshot(
                h.store.as_ref(),
                codemode_store_doc(),
                Scope::Conversation {
                    conversation_id: h.root.id
                }
            )
            .unwrap()["values"]["k"],
            2
        );
        let failed = h.run("store('k',9);throw new Error('keep old')").await;
        assert_eq!(failed["isError"], true);
        assert_eq!(
            snapshot(
                h.store.as_ref(),
                codemode_store_doc(),
                Scope::Conversation {
                    conversation_id: h.root.id
                }
            )
            .unwrap()["values"]["k"],
            2
        );
        h.close().await;
        assert_eq!(h.sandbox.active_workers(), 0);
    }
}
fn sqlite() -> (Arc<dyn Storage>, PathBuf) {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    let path = std::env::temp_dir().join(format!(
        "pi-durable-codemode-{}-{}.sqlite",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::SeqCst)
    ));
    (Arc::new(SqliteStorage::open(&path).unwrap()), path)
}
fn tool(
    name: &str,
    execute: impl Fn(ToolInvocation) -> RuntimeFuture<ToolExecutionResult> + Send + Sync + 'static,
) -> ToolRegistration {
    ToolRegistration::new(serde_json::from_value(json!({"name":name,"description":name,"parameters":{"type":"object","additionalProperties":true}})).unwrap(),execute)
}
fn docs(h: &Harness) -> (Value, Value) {
    let scope = Scope::Task {
        task_id: TaskId::new(h.events.code_task.load(Ordering::SeqCst)).unwrap(),
    };
    (
        snapshot(h.store.as_ref(), codemode_run_doc(), scope.clone()).unwrap_or(Value::Null),
        snapshot(h.store.as_ref(), codemode_journal_doc(), scope).unwrap_or(Value::Null),
    )
}
async fn patch(h: &Harness, def: DocDefinition, paths: &[(&str, Value)]) {
    let scope = Scope::Task {
        task_id: TaskId::new(h.events.code_task.load(Ordering::SeqCst)).unwrap(),
    };
    let paths = paths
        .iter()
        .map(|(p, v)| (p.to_string(), v.clone()))
        .collect::<Vec<_>>();
    h.durable
        .session()
        .commit(move |tx| {
            let id = tx.doc(&def, def.address(scope, None)?, Value::Null)?;
            for (path, value) in paths {
                let parts = path
                    .split('.')
                    .map(|p| {
                        p.parse::<usize>()
                            .map(Segment::Index)
                            .unwrap_or_else(|_| Segment::Key(p.into()))
                    })
                    .collect();
                tx.apply(id, Op::Set(parts, value))?;
            }
            Ok(())
        })
        .await
        .unwrap();
}
async fn reopen(h: &mut Harness, path: &std::path::Path) -> Vec<(Seq, Vec<StorageWrite>)> {
    h.close().await;
    let prior = h.store.batches.lock().unwrap().clone();
    h.store = Arc::new(CaptureStorage::new(Arc::new(
        SqliteStorage::open(path).unwrap(),
    )));
    *h.events.storage.lock().unwrap() = Some(h.store.clone());
    h.durable = Durable::open(h.store.clone(), h.options.clone())
        .await
        .unwrap();
    h.root = h.durable.root(Default::default()).await.unwrap();
    prior
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_worker_keyed_receipts_and_arrival_replay() {
    let (inner, path) = sqlite();
    let events = Arc::new(Events::default());
    let e = events.clone();
    let mut slow = tool("slow", move |inv| {
        let e = e.clone();
        Box::pin(async move {
            let arguments = j(&inv.call.call.arguments);
            let name = arguments["gate"].as_str().unwrap();
            *e.effects.lock().unwrap().entry("slow".into()).or_default() += 1;
            e.wait(name, inv.api.signal()).await;
            Ok(ToolExecutionResult {
                output: Some(vec![
                    serde_json::from_value(json!({"type":"text","text":name})).unwrap(),
                ]),
                ..Default::default()
            })
        })
    });
    slow.replay = ReplayPolicy::Unsafe;
    let e = events.clone();
    let mut hold = tool("hold", move |inv| {
        let e = e.clone();
        Box::pin(async move {
            let run = {
                let mut n = e.effects.lock().unwrap();
                let n = n.entry("hold".into()).or_default();
                *n += 1;
                *n
            };
            if run == 1 {
                e.wait("hold", inv.api.signal()).await;
            }
            Ok(Default::default())
        })
    });
    hold.replay = ReplayPolicy::Safe;
    let mut h = Harness::open_with(
        inner,
        CodeModeOptions::new(worker()),
        vec![slow, hold],
        Default::default(),
        events,
    )
    .await;
    let input=h.submit("const order=[];await Promise.all([tools.slow({gate:'one'}).then(()=>order.push(1)),tools.slow({gate:'two'}).then(()=>order.push(2))]);await tools.hold({});return [order,Date.now()]").await;
    eventually(async || {
        if h.events.count("slow") == 2 {
            return true;
        }
        if h.state.requests.lock().unwrap().len() > 1 {
            let entries = h.root.context(None).await.unwrap().entries;
            panic!(
                "script ended early: vm={:?}; context={entries:?}",
                h.events.vm.lock().unwrap()
            );
        }
        false
    })
    .await;
    h.advance(1010);
    h.events.release("two");
    if tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if docs(&h).1["entries"][0]["issuedAfter"] == 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await
        }
    })
    .await
    .is_err()
    {
        panic!(
            "journal stall docs={:?}; vm={:?}; tasks={:?}",
            docs(&h),
            h.events.vm.lock().unwrap(),
            h.store
                .scan_tasks(&TaskQuery::default(), 1000, None)
                .unwrap()
        );
    }
    h.advance(1020);
    h.events.release("one");
    eventually(async || h.events.count("hold") == 1).await;
    let initial = docs(&h);
    assert_eq!(initial.1["entries"][0]["id"], 2);
    assert_eq!(initial.1["entries"][1]["id"], 1);
    let old_vm = h.events.vm.lock().unwrap().clone();
    reopen(&mut h, &path).await;
    h.advance(1030);
    h.durable.resume();
    tokio::time::timeout(Duration::from_secs(10), input_on(&h, input.id))
        .await
        .unwrap();
    assert_eq!(h.events.count("slow"), 2);
    assert_eq!(h.events.count("hold"), 2);
    assert!(text(&h.result().await).contains("[[2,1],1030]"));
    // The observer reads actual committed documents at each native delivery.
    let vm = h.events.vm.lock().unwrap().clone();
    let deliveries = vm
        .iter()
        .filter(|v| v["kind"] == "deliver")
        .map(|v| (v["id"].as_u64().unwrap(), v["at"].as_f64().unwrap()))
        .collect::<Vec<_>>();
    assert_eq!(
        deliveries,
        vec![(2, 1010.), (1, 1020.), (2, 1010.), (1, 1020.), (3, 1030.)]
    );
    assert!(old_vm.iter().any(|v| v["kind"] == "started"));
    for d in vm.iter().filter(|v| v["kind"] == "deliver") {
        assert_eq!(
            d["committedJournal"]["entries"][d["seq"].as_u64().unwrap() as usize - 1]["id"],
            d["id"]
        );
    }
    h.close().await;
}
async fn input_on(h: &Harness, id: SubmissionId) {
    let receipt = h.durable.submission(id).await.unwrap().unwrap();
    if tokio::time::timeout(Duration::from_secs(8), receipt.wait())
        .await
        .is_err()
    {
        panic!(
            "reopen wait timeout; docs={:?}; vm={:?}; tasks={:?}",
            docs(h),
            h.events.vm.lock().unwrap(),
            h.store
                .scan_tasks(&TaskQuery::default(), 1000, None)
                .unwrap()
        );
    }
}

mod fixtures;

mod scenarios;

mod environment;
