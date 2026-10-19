use super::*;
fn reference(v: &Value, bindings: &BTreeMap<String, Value>) -> Value {
    match v {
        Value::String(s) if s.starts_with('$') => {
            let mut parts = s[1..].split('.');
            let mut value = bindings[parts.next().unwrap()].clone();
            for part in parts {
                value = if let Some(a) = value.as_array() {
                    a[part.parse::<usize>().unwrap()].clone()
                } else {
                    value[part].clone()
                }
            }
            value
        }
        Value::Array(a) => j(a.iter().map(|v| reference(v, bindings)).collect::<Vec<_>>()),
        Value::Object(o) => Value::Object(
            o.iter()
                .map(|(k, v)| (k.clone(), reference(v, bindings)))
                .collect(),
        ),
        _ => v.clone(),
    }
}
fn record_effect(
    e: &Events,
    name: &str,
    args: &Value,
    api: &ToolExecutionApi,
    global: bool,
) -> usize {
    let run = {
        let mut counts = e.effects.lock().unwrap();
        let n = counts.entry(name.into()).or_default();
        *n += 1;
        *n
    };
    let storage = e.storage.lock().unwrap().clone().unwrap();
    let identity = api.identity();
    let owner = identity
        .parent
        .as_ref()
        .map(|p| p.task_id)
        .unwrap_or(identity.invocation.task_id);
    let scope = Scope::Task { task_id: owner };
    let mut event = json!({"kind":if global{"global"}else{"tool"},"name":name,"run":run,"args":args,"committedRun":snapshot(storage.as_ref(),codemode_run_doc(),scope.clone()),"committedJournal":snapshot(storage.as_ref(),codemode_journal_doc(),scope)});
    if !global {
        event["task"] = j(storage.task(identity.invocation.task_id).unwrap());
    }
    e.effect_events.lock().unwrap().push(event);
    run
}
async fn fixture(test: &Value, backend: &str, capture: &std::path::Path) -> Value {
    let (inner, path) = if backend == "sqlite" {
        let (s, p) = sqlite();
        (s, Some(p))
    } else {
        (Arc::new(MemoryStorage::new()) as Arc<dyn Storage>, None)
    };
    let e = Arc::new(Events::default());
    let mut tools = Vec::new();
    for descriptor in test["input"]["tools"].as_array().into_iter().flatten() {
        let d = descriptor.clone();
        let events = e.clone();
        let name = d["name"].as_str().unwrap().to_owned();
        let mut registered = tool(&name, move |inv| {
            let d = d.clone();
            let e = events.clone();
            Box::pin(async move {
                let args = j(&inv.call.call.arguments);
                let name = d["name"].as_str().unwrap();
                let run = record_effect(&e, name, &args, &inv.api, false);
                let bindings = BTreeMap::from([("args".into(), args), ("run".into(), json!(run))]);
                if let Some(g) = d
                    .get("gate")
                    .filter(|_| d["firstRuns"].as_u64().is_none_or(|n| run as u64 <= n))
                {
                    let gate = reference(g, &bindings);
                    e.wait(gate.as_str().unwrap(), inv.api.signal()).await;
                }
                if let Some(o) = d.get("output") {
                    inv.api.output(
                        OutputChunk::Text(reference(o, &bindings).as_str().unwrap().into()),
                        None,
                    )?;
                }
                Ok(serde_json::from_value(reference(
                    d.get("result")
                        .unwrap_or(&json!({"output":[{"type":"text","text":name}]})),
                    &bindings,
                ))?)
            })
        });
        registered.replay = if descriptor["replay"] == "unsafe" {
            ReplayPolicy::Unsafe
        } else {
            ReplayPolicy::Safe
        };
        if let Some(s) = descriptor.get("structuredOutputSchema") {
            registered.structured_output_schema = Some(serde_json::from_value(s.clone()).unwrap());
        }
        if let Some(s) = descriptor.get("parameters") {
            registered.declaration.parameters = serde_json::from_value(s.clone()).unwrap();
        }
        tools.push(registered);
    }
    let mut code = CodeModeOptions::new(worker());
    for descriptor in test["input"]["globals"].as_array().into_iter().flatten() {
        let d = descriptor.clone();
        let events = e.clone();
        code.globals.push(CodeModeGlobal {
            name: d["name"].as_str().unwrap().into(),
            spread: d["spread"].as_bool().unwrap_or(false),
            execute: Arc::new(move |args, api| {
                let e = events.clone();
                let d = d.clone();
                Box::pin(async move {
                    let run = record_effect(
                        &e,
                        d["name"].as_str().unwrap(),
                        &args.clone().unwrap_or(json!({"$undefined":true})),
                        &api,
                        true,
                    );
                    if let Some(g) = d
                        .get("gate")
                        .filter(|_| d["firstRuns"].as_u64().is_none_or(|n| run as u64 <= n))
                    {
                        if d["abortable"] == false {
                            let name = g.as_str().unwrap().to_string();
                            let events = e.clone();
                            let task = tokio::spawn(async move {
                                events.wait(&name, pi_ai::CancellationToken::new()).await;
                                events
                                    .effect_events
                                    .lock()
                                    .unwrap()
                                    .push(json!({"kind":"late-global-return"}));
                            });
                            let _ = task.await;
                        } else {
                            e.wait(g.as_str().unwrap(), api.signal()).await;
                        }
                    }
                    if d["denyNextCommit"] == true {
                        e.storage
                            .lock()
                            .unwrap()
                            .as_ref()
                            .unwrap()
                            .deny
                            .store(true, Ordering::SeqCst);
                    }
                    if let Some(message) = d["throw"].as_str() {
                        return Err(Error::request(message));
                    }
                    let value = d
                        .get("value")
                        .cloned()
                        .unwrap_or(json!({"$undefined":true}));
                    if value == json!({"$undefined":true}) {
                        Ok(None)
                    } else {
                        Ok(Some(reference(
                            &value,
                            &BTreeMap::from([
                                ("run".into(), json!(run)),
                                ("args".into(), args.unwrap_or(Value::Null)),
                            ]),
                        )))
                    }
                })
            }),
        });
    }
    let mut hooks = Hooks::default();
    if test["input"]["holdAfterDecision"] == true {
        let events = e.clone();
        let first = Arc::new(std::sync::atomic::AtomicBool::new(true));
        hooks.after_tool = Some(Arc::new(move |call, _, api| {
            let e = events.clone();
            let first = first.clone();
            Box::pin(async move {
                if call.call.name.as_str() == "codemode" && first.swap(false, Ordering::SeqCst) {
                    e.effect_events
                        .lock()
                        .unwrap()
                        .push(json!({"kind":"decision","taskId":api.task_id()}));
                    e.wait("after-decision", api.signal()).await;
                }
                Ok(None)
            })
        }));
    }
    let mut h = Harness::open_with(inner, code.clone(), tools, hooks, e.clone()).await;
    let mut bindings = BTreeMap::from([("root".into(), json!(1))]);
    let mut observations = Vec::new();
    let mut batches = Vec::new();
    for (index, raw) in test["input"]["operations"]
        .as_array()
        .unwrap()
        .iter()
        .enumerate()
    {
        let op = reference(raw, &bindings);
        let operation = op["op"].as_str().unwrap();
        let cid = ConversationId::new(op["conversationId"].as_u64().unwrap_or(1)).unwrap();
        let value = match operation {
            "run" => {
                let source = op["code"].as_str().unwrap();
                let id = format!("script-{index}");
                h.state.responses.lock().unwrap().push_back(
                    json!({"calls":[{"id":id,"name":"codemode","arguments":{"code":source}}]}),
                );
                if op["answer"] != false {
                    h.state
                        .responses
                        .lock()
                        .unwrap()
                        .push_back(json!({"text":"done"}));
                }
                j(h.durable
                    .conversation(cid)
                    .await
                    .unwrap()
                    .unwrap()
                    .submit(SubmissionDraft::input("run script"))
                    .await
                    .unwrap()
                    .id)
            }
            "wait" => {
                let id = serde_json::from_value(op["id"].clone()).unwrap();
                let submission = h.durable.submission(id).await.unwrap().unwrap();
                j(
                    tokio::time::timeout(Duration::from_secs(10), submission.wait())
                        .await
                        .unwrap()
                        .unwrap(),
                )
            }
            "status" => j(h
                .durable
                .submission(serde_json::from_value(op["id"].clone()).unwrap())
                .await
                .unwrap()
                .unwrap()
                .status()
                .await
                .unwrap()),
            "untilEffects" => {
                eventually(async || {
                    e.count(op["name"].as_str().unwrap()) as u64 >= op["count"].as_u64().unwrap()
                })
                .await;
                j(&*e.effects.lock().unwrap())
            }
            "untilVm" => {
                eventually(async || {
                    e.vm.lock()
                        .unwrap()
                        .iter()
                        .filter(|v| v["kind"] == op["kind"])
                        .count() as u64
                        >= op["count"].as_u64().unwrap()
                })
                .await;
                Value::Null
            }
            "untilJournal" => {
                eventually(async || {
                    let mut v = docs(&h).1;
                    for key in op["path"].as_array().unwrap() {
                        v = if let Some(k) = key.as_str() {
                            v[k].clone()
                        } else {
                            v[key.as_u64().unwrap() as usize].clone()
                        };
                    }
                    op.get("equals").map(|x| *x == v).unwrap_or(!v.is_null())
                })
                .await;
                docs(&h).1
            }
            "untilDecision" => {
                eventually(async || {
                    e.effect_events
                        .lock()
                        .unwrap()
                        .iter()
                        .any(|v| v["kind"] == "decision")
                })
                .await;
                docs(&h).0
            }
            "taskDocs" | "committedTaskDocs" => {
                let (run, journal) = docs(&h);
                json!({"run":run,"journal":journal})
            }
            "store" => j(snapshot(
                h.store.as_ref(),
                codemode_store_doc(),
                Scope::Conversation {
                    conversation_id: cid,
                },
            )),
            "storeAsOf" => j(h
                .durable
                .session()
                .snapshot_as_of(
                    codemode_store_doc(),
                    codemode_store_doc()
                        .address(
                            Scope::Conversation {
                                conversation_id: cid,
                            },
                            None,
                        )
                        .unwrap(),
                    serde_json::from_value(op["at"].clone()).unwrap(),
                )
                .await
                .unwrap()),
            "fork" => j(h
                .durable
                .conversation(cid)
                .await
                .unwrap()
                .unwrap()
                .fork(
                    serde_json::from_value(op["at"].clone()).unwrap(),
                    ConversationCreateOptions {
                        ownership: ConversationOwnership::Ownerless,
                        options: Default::default(),
                    },
                )
                .await
                .unwrap()
                .id),
            "entries" => j(h
                .durable
                .conversation(cid)
                .await
                .unwrap()
                .unwrap()
                .context(None)
                .await
                .unwrap()
                .entries),
            "effects" => {
                json!({"effects":*e.effects.lock().unwrap(),"events":*e.effect_events.lock().unwrap()})
            }
            "vmEvents" => j(&*e.vm.lock().unwrap()),
            "release" => {
                e.release(op["name"].as_str().unwrap());
                tokio::time::sleep(Duration::from_millis(5)).await;
                Value::Null
            }
            "advance" => {
                h.advance(op["to"].as_u64().unwrap());
                op["to"].clone()
            }
            "resume" => {
                h.durable.resume();
                Value::Null
            }
            "reopen" => {
                batches.extend(reopen(&mut h, path.as_ref().unwrap()).await);
                Value::Null
            }
            "patchRun" | "patchJournal" => {
                let values = op["set"]
                    .as_object()
                    .unwrap()
                    .iter()
                    .map(|(k, v)| (k.as_str(), v.clone()))
                    .collect::<Vec<_>>();
                patch(
                    &h,
                    if operation == "patchRun" {
                        codemode_run_doc()
                    } else {
                        codemode_journal_doc()
                    },
                    &values,
                )
                .await;
                Value::Null
            }
            "abort" => {
                h.root.abort(false).await.unwrap();
                Value::Null
            }
            "sandboxClose" => {
                let sandbox = h.sandbox.clone();
                tokio::task::spawn_blocking(move || sandbox.close())
                    .await
                    .unwrap();
                code.sandbox = worker();
                code.observe = None;
                // The new Sandbox is host custody; preserve the exact test observer.
                let events = e.clone();
                code.observe = Some(observer(events));
                h.options.registry.uninstall("codemode").unwrap();
                h.options
                    .registry
                    .install(create_codemode_extension(code.clone()).unwrap())
                    .unwrap();
                h.sandbox = code.sandbox.clone();
                Value::Null
            }
            "closed" => {
                tokio::time::timeout(Duration::from_secs(10), h.durable.session().closed())
                    .await
                    .unwrap();
                Value::Null
            }
            "close" => {
                let _ = tokio::time::timeout(Duration::from_secs(10), h.durable.close())
                    .await
                    .unwrap();
                Value::Null
            }
            "files" => {
                let result = h.result().await;
                let path = result["details"]["fullOutputPath"].as_str().unwrap();
                let dir = std::path::Path::new(path).parent().unwrap();
                j(std::fs::read_dir(dir)
                    .unwrap()
                    .map(|e| {
                        let p = e.unwrap().path();
                        json!({"path":p,"bytes":std::fs::read(&p).unwrap()})
                    })
                    .collect::<Vec<_>>())
            }
            _ => panic!("unhandled CodeMode fixture operation {operation}"),
        };
        if let Some(name) = op["as"].as_str() {
            bindings.insert(name.into(), value.clone());
        }
        observations.push(json!({"label":op["label"].as_str().or(op["as"].as_str()).map(str::to_owned).unwrap_or_else(||format!("{index}:{operation}")),"value":value}));
    }
    eventually(async || {
        let events = e.vm.lock().unwrap();
        events.iter().filter(|v| v["kind"] == "start").count()
            == events.iter().filter(|v| v["kind"] == "terminal").count()
    })
    .await;
    batches.extend(h.store.batches.lock().unwrap().clone());
    replay_operations(&batches);
    let actual = json!({"observations":observations,"vmEvents":*e.vm.lock().unwrap(),"effects":*e.effects.lock().unwrap(),"effectEvents":*e.effect_events.lock().unwrap(),"nativeStorageBatches":batches,"files":*e.files.lock().unwrap()});
    std::fs::write(
        capture.join(format!(
            "pirua-native-{}-{backend}.json",
            test["id"].as_str().unwrap()
        )),
        serde_json::to_vec_pretty(&actual).unwrap(),
    )
    .unwrap();
    let expected = &test["expected"][backend];
    compare_results(test["id"].as_str().unwrap(), &actual, expected);
    let deliveries = |events: &Value| {
        events
            .as_array()
            .unwrap()
            .iter()
            .filter(|v| v["kind"] == "deliver")
            .map(|v| {
                (
                    v["seq"].as_u64().unwrap(),
                    v["id"].as_u64().unwrap(),
                    v["at"].as_f64().unwrap(),
                    v["outcome"].clone(),
                )
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(
        deliveries(&actual["vmEvents"]),
        deliveries(&expected["vmEvents"]),
        "delivery order/clock/outcome {} {backend}",
        test["id"]
    );
    assert_eq!(
        actual["vmEvents"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|v| v["kind"] == "start")
            .count(),
        expected["vmEvents"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|v| v["kind"] == "start")
            .count(),
        "worker starts {}",
        test["id"]
    );
    assert_eq!(
        actual["effects"], expected["effects"],
        "{} {backend}",
        test["id"]
    );
    for (index, op) in test["input"]["operations"]
        .as_array()
        .unwrap()
        .iter()
        .enumerate()
    {
        if ["store", "storeAsOf"].contains(&op["op"].as_str().unwrap()) {
            let label = op["label"]
                .as_str()
                .or(op["as"].as_str())
                .map(str::to_owned)
                .unwrap_or_else(|| format!("{index}:{}", op["op"].as_str().unwrap()));
            let observed = actual["observations"]
                .as_array()
                .unwrap()
                .iter()
                .find(|v| v["label"] == label)
                .unwrap();
            let source = expected["observations"]
                .as_array()
                .unwrap()
                .iter()
                .find(|v| v["label"] == label)
                .unwrap();
            assert_eq!(observed["value"], source["value"], "store history {label}");
        }
    }
    let native_files = actual["files"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|v| v["kind"] == "writeFile")
        .collect::<Vec<_>>();
    let source_files = expected["files"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|v| v["kind"] == "writeFile")
        .collect::<Vec<_>>();
    assert_eq!(native_files.len(), source_files.len());
    for (native, source) in native_files.into_iter().zip(source_files) {
        let bytes = if let Some(text) = source["data"].as_str() {
            text.as_bytes().to_vec()
        } else {
            use base64::Engine;
            base64::engine::general_purpose::STANDARD
                .decode(source["data"]["base64"].as_str().unwrap())
                .unwrap()
        };
        assert_eq!(native["bytes"], j(bytes));
        assert_eq!(
            std::path::Path::new(native["path"].as_str().unwrap()).file_name(),
            std::path::Path::new(source["path"].as_str().unwrap()).file_name()
        );
        assert_eq!(
            std::path::Path::new(native["path"].as_str().unwrap())
                .parent()
                .unwrap()
                .to_str()
                .unwrap(),
            native["committedRun"]["outputDir"].as_str().unwrap()
        );
    }
    for event in actual["vmEvents"].as_array().unwrap() {
        if event["kind"] == "start" {
            assert!(event["committedRun"]["start"].is_object());
        }
        if event["kind"] == "deliver" {
            let index = event["seq"].as_u64().unwrap() as usize - 1;
            assert_eq!(
                event["committedJournal"]["entries"][index]["id"],
                event["id"]
            );
        }
    }
    for event in actual["effectEvents"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|e| e["kind"] == "tool" || e["kind"] == "global")
    {
        let journal = &event["committedJournal"];
        let covered = journal["entries"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|e| e["issuedAfter"].as_u64())
            .chain(journal["firstIssued"].as_u64())
            .max()
            .unwrap_or(0);
        assert!(covered > 0, "effect without committed count: {event}");
    }
    let _ = h.durable.close().await;
    actual
}
fn compare_results(id: &str, actual: &Value, expected: &Value) {
    fn messages(observations: &Value) -> Vec<&Value> {
        observations
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|observation| {
                let entries = &observation["value"];
                entries
                    .as_array()
                    .or_else(|| entries["items"].as_array())
                    .into_iter()
                    .flatten()
                    .filter(|entry| entry["kind"] == "pi.tool-result")
                    .flat_map(|entry| entry["model"].as_array().into_iter().flatten())
                    .filter(|message| message["toolName"] == "codemode")
            })
            .collect()
    }
    fn rows(message: &Value) -> Vec<Value> {
        message["details"]["calls"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|row| json!({"id":row["id"],"name":row["name"],"status":row["status"]}))
            .collect()
    }
    let native = messages(&actual["observations"]);
    let source = messages(&expected["observations"]);
    assert_eq!(native.len(), source.len(), "tool-result count {id}");
    for (native, source) in native.into_iter().zip(source) {
        assert_eq!(
            native["isError"], source["isError"],
            "result error flag {id}"
        );
        assert_eq!(rows(native), rows(source), "call rows {id}");
        match id {
            "codemode-local-basic" => {
                assert_eq!(native["content"][0], source["content"][0]);
                assert_eq!(native["content"].as_array().unwrap().len(), 2);
                let (body, value) = native["content"][1]["text"]
                    .as_str()
                    .unwrap()
                    .rsplit_once('\n')
                    .unwrap();
                let (source_body, source_value) = source["content"][1]["text"]
                    .as_str()
                    .unwrap()
                    .rsplit_once('\n')
                    .unwrap();
                assert_eq!(body, source_body);
                let value: Value = serde_json::from_str(value).unwrap();
                let source_value: Value = serde_json::from_str(source_value).unwrap();
                assert_eq!(value[0], source_value[0]);
                assert_eq!(value[1], source_value[1]);
                // Existing native fingerprinted mulberry32, seed 424242. The
                // source WASI RNG's distinct value remains in its raw fixture.
                assert_eq!(value[2], json!(0.1287079993635416));
            }
            "codemode-local-reject-fingerprint" => {
                let fingerprint = actual["vmEvents"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .rev()
                    .find(|v| v["kind"] == "started")
                    .unwrap()["fingerprint"]
                    .as_str()
                    .unwrap();
                assert_eq!(
                    native["content"],
                    json!([{"type":"text","text":format!("The script cannot resume: the sandbox changed since it started (another sandbox -> {fingerprint}).")}])
                )
            }
            "codemode-local-all-held-identities" => {
                assert_eq!(
                    native["content"],
                    json!([{"type":"text","text":"The script took a different path when resumed after a restart: call 2 is another target, tool or arguments than its earlier keyed call"}])
                );
                assert!(source["content"][0]["text"].as_str().unwrap().starts_with(
                    "The script took a different path when resumed after a restart: call 2 "
                ));
                assert_eq!(actual["effects"]["hold"], expected["effects"]["hold"]);
                assert!(actual["effects"].get("echo").is_none());
            }
            "codemode-local-output-image-spill" => {
                let path = native["details"]["fullOutputPath"].as_str().unwrap();
                let directory = std::path::Path::new(path)
                    .parent()
                    .unwrap()
                    .to_str()
                    .unwrap();
                let source_path = source["details"]["fullOutputPath"].as_str().unwrap();
                let source_directory = std::path::Path::new(source_path)
                    .parent()
                    .unwrap()
                    .to_str()
                    .unwrap();
                let mut content = native["content"].clone();
                for item in content.as_array_mut().unwrap() {
                    if let Some(text) = item["text"].as_str() {
                        item["text"] = json!(text.replace(directory, source_directory));
                    }
                }
                // The exact content/PNG bytes and already committed directory
                // binding are separately asserted at the real file host call.
                assert_eq!(content, source["content"]);
            }
            _ => assert_eq!(
                native["content"], source["content"],
                "terminal content {id}"
            ),
        }
    }
    // These observations are about the decision boundary, before retirement.
    for source in expected["observations"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|o| o["label"] == "shutdown-no-outcome" || o["label"] == "3:committedTaskDocs")
    {
        let native = actual["observations"]
            .as_array()
            .unwrap()
            .iter()
            .find(|o| o["label"] == source["label"])
            .unwrap();
        assert_eq!(
            native["value"]["run"]["result"], source["value"]["run"]["result"],
            "no decided result {id}"
        );
    }
}
fn observer(events: Arc<Events>) -> Arc<dyn Fn(Value) + Send + Sync> {
    Arc::new(move |mut v| {
        let observed = events.storage.lock().unwrap().clone().unwrap();
        if v["kind"] == "start" {
            events
                .code_task
                .store(v["taskId"].as_u64().unwrap(), Ordering::SeqCst);
        }
        let id = events.code_task.load(Ordering::SeqCst);
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
            v["committedJournal"] = j(snapshot(observed.as_ref(), codemode_journal_doc(), scope));
        }
        events.vm.lock().unwrap().push(v);
    })
}
fn capture_directory() -> PathBuf {
    if let Some(path) = std::env::var_os("PIRUA_CODEMODE_CAPTURE") {
        let path = PathBuf::from(path);
        std::fs::create_dir_all(&path).expect("create explicit CodeMode capture directory");
        return path;
    }
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("CodeMode capture clock")
        .as_nanos();
    loop {
        let path = std::env::temp_dir().join(format!(
            "pirua-codemode-capture-{}-{timestamp}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        // Exclusive creation reserves this run's directory across processes and
        // worktrees; an existing candidate is retried without writing into it.
        match std::fs::create_dir(&path) {
            Ok(()) => return path,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => panic!(
                "create CodeMode capture directory {}: {error}",
                path.display()
            ),
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn frozen_real_worker_cases() {
    let capture = capture_directory();
    eprintln!("native CodeMode capture directory: {}", capture.display());
    let corpus: Value = serde_json::from_str(include_str!(
        "../../../../../conformance/durable-v1.4.0/fixtures/codemode.json"
    ))
    .unwrap();
    let mut actual = Vec::new();
    for test in corpus["cases"].as_array().unwrap().iter().filter(|t| {
        t["input"]["sandbox"]["kind"] == "local"
            && t["id"] != "codemode-store-checkpoint-history-fork"
    }) {
        let backends = test
            .get("backends")
            .cloned()
            .unwrap_or(json!(["memory", "sqlite"]));
        for backend in backends.as_array().unwrap() {
            if std::env::var("PIRUA_CODEMODE_CASE")
                .ok()
                .is_some_and(|id| test["id"] != id)
            {
                continue;
            }
            let backend = backend.as_str().unwrap();
            eprintln!("native CodeMode {} {backend}", test["id"]);
            let result = fixture(test, backend, &capture).await;
            actual.push(json!({"id":test["id"],"backend":backend,"actual":result}));
        }
    }
    std::fs::write(
        capture.join("pirua-codemode-native-observations.json"),
        serde_json::to_vec_pretty(&actual).unwrap(),
    )
    .unwrap();
}

// Apply actual native operations in their emitted batch order. No source values
// initialize native records or substitute materialized state.
fn replay_operations(batches: &[(Seq, Vec<StorageWrite>)]) {
    let mut documents = BTreeMap::<DocumentId, Value>::new();
    let mut history = BTreeMap::<Seq, BTreeMap<DocumentId, Value>>::new();
    let mut kinds = BTreeMap::<DocumentId, String>::new();
    for (seq, batch) in batches {
        let mut changed_store = false;
        let mut decided = false;
        for write in batch {
            match write {
                StorageWrite::DocumentCreate { record, content } => {
                    kinds.insert(record.id, record.kind.clone());
                    match content {
                        DocumentContent::Base { value, .. } => {
                            documents.insert(record.id, value.clone());
                        }
                        DocumentContent::Delta { .. } => panic!("created delta without base"),
                    }
                }
                StorageWrite::DocumentCopy { record, source } => {
                    let data = match source.at {
                        DocumentPoint::At(at) => history.range(..=at).next_back().unwrap().1,
                        DocumentPoint::Current => &documents,
                    };
                    let value = data[&source.id].clone();
                    documents.insert(record.id, value);
                    kinds.insert(record.id, record.kind.clone());
                }
                StorageWrite::DocumentChange { id, content } => {
                    if kinds.get(id).is_some_and(|k| k == "pi.codemode.store") {
                        changed_store = true;
                    }
                    if let DocumentContent::Delta { ops, .. } = content
                        && kinds.get(id).is_some_and(|k| k == "pi.codemode.run")
                        && ops
                            .iter()
                            .any(|o| matches!(o,Op::Set(path,_) if path==&vec!["result".into()]))
                    {
                        decided = true;
                    }
                    match content {
                        DocumentContent::Base { value, .. } => {
                            documents.insert(*id, value.clone());
                        }
                        DocumentContent::Delta { ops, .. } => {
                            let value = documents.get_mut(id).expect("native delta base");
                            *value = apply(value, ops).unwrap();
                        }
                    }
                }
                _ => (),
            }
        }
        if changed_store {
            assert!(decided, "store write must share decided-result batch");
        }
        history.insert(*seq, documents.clone());
    }
}
