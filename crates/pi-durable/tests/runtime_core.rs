mod runtime_support;
#[path = "runtime_support/control.rs"]
mod script_control;
use pi_durable::{session::*, *};
use runtime_support::*;
use serde_json::json;
use std::sync::Arc;
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_native_generation_admission_and_receipts() {
    let store = Arc::new(MemoryStorage::new());
    let (d, root, state) = open(
        store,
        &json!({"responses":[{"text":"answer","gate":"busy"},{"text":"follow"}]}),
    )
    .await;
    let first = root
        .submit(SubmissionDraft::Input {
            request_id: Some("r1".into()),
            content: pi_ai::UserContent::Text("first".into()),
            when_busy: None,
            entry: None,
        })
        .await
        .unwrap();
    state.requests(1).await;
    let again = root
        .submit(SubmissionDraft::Input {
            request_id: Some("r1".into()),
            content: pi_ai::UserContent::Text("ignored".into()),
            when_busy: Some(BusyPolicy::Reject),
            entry: None,
        })
        .await
        .unwrap();
    assert_eq!(first.id, again.id);
    let follow = root.submit(SubmissionDraft::input("next")).await.unwrap();
    assert!(matches!(
        follow.status().await.unwrap().state,
        SubmissionState::Queued
    ));
    state.release("busy");
    let final_first = tokio::time::timeout(std::time::Duration::from_secs(5), first.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(final_first.state, SubmissionState::Done { .. }));
    assert!(matches!(
        follow.wait().await.unwrap().state,
        SubmissionState::Done { .. }
    ));
    let context = root.context(None).await.unwrap();
    assert_eq!(context.messages.len(), 4);
    assert_eq!(state.requests.lock().unwrap().len(), 2);
    assert_eq!(
        d.session()
            .snapshot(
                live_doc(),
                live_doc()
                    .address(
                        Scope::Conversation {
                            conversation_id: root.id
                        },
                        None
                    )
                    .unwrap()
            )
            .await
            .unwrap(),
        Some(json!({}))
    );
    d.close().await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_native_model_tool_round() {
    let clock = Arc::new(TestClock(std::sync::atomic::AtomicU64::new(1000)));
    let(models,_)=models(&json!({"responses":[{"calls":[{"id":"call","name":"work","arguments":{}}]},{"text":"done"}]}),clock).await;
    let mut options = DurableOptions::new(models);
    let started = std::time::Instant::now();
    options.scheduler.now = Arc::new(move || 1000.0 + started.elapsed().as_secs_f64() * 1000.0);
    let effects = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let e = effects.clone();
    let declaration=serde_json::from_value(json!({"name":"work","description":"local","parameters":{"type":"object","properties":{},"additionalProperties":false}})).unwrap();
    let mut tool = ToolRegistration::new(declaration, move |call| {
        let e = e.clone();
        Box::pin(async move {
            e.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            call.api.output(OutputChunk::Text("part".into()), None)?;
            call.api
                .details(serde_json::from_value(json!({"step":1})).unwrap())
                .await?;
            Ok(ToolExecutionResult::default())
        })
    });
    tool.replay = ReplayPolicy::Safe;
    options
        .registry
        .install(Extension {
            name: "local".into(),
            tools: vec![tool],
            ..Default::default()
        })
        .unwrap();
    let d = Durable::open(Arc::new(MemoryStorage::new()), options)
        .await
        .unwrap();
    let root = d
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
    let result = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        root.submit(SubmissionDraft::input("go"))
            .await
            .unwrap()
            .wait()
            .await
    })
    .await
    .unwrap()
    .unwrap();
    assert!(matches!(result.state, SubmissionState::Done { .. }));
    assert_eq!(effects.load(std::sync::atomic::Ordering::SeqCst), 1);
    let context = root.context(None).await.unwrap();
    assert!(context.entries.iter().any(|e| e.kind == "pi.tool-result"));
    d.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn after_tool_owned_replacement_controls_retained_output_provenance() {
    for replacement in [None, Some("abc"), Some("uvwxyz")] {
        let clock = Arc::new(TestClock(std::sync::atomic::AtomicU64::new(1000)));
        let (models,_)=models(&json!({"responses":[{"calls":[{"id":"call","name":"bounded","arguments":{}}]},{"text":"done"}]}),clock).await;
        let declaration=serde_json::from_value(json!({"name":"bounded","description":"bounded","parameters":{"type":"object","properties":{}}})).unwrap();
        let mut tool = ToolRegistration::new(declaration, |inv| {
            Box::pin(async move {
                inv.api.output(OutputChunk::Text("abcdef".into()), None)?;
                Ok(Default::default())
            })
        });
        tool.output_limits = OutputLimits {
            max_bytes: 3,
            max_lines: 10,
            retain: OutputRetention::Head,
        };
        let observed = Arc::new(std::sync::Mutex::new(vec![]));
        let seen = observed.clone();
        let hook = Arc::new(
            move |_: ToolHookCall, mut result: ToolExecutionResult, _: HookApi| {
                let seen = seen.clone();
                Box::pin(async move {
                    seen.lock()
                        .unwrap()
                        .push(result.diagnostics.clone().unwrap_or_default());
                    Ok(replacement.map(|text| {
                        result.output = Some(vec![
                            serde_json::from_value(json!({"type":"text","text":text})).unwrap(),
                        ]);
                        result
                    }))
                }) as RuntimeFuture<Option<ToolExecutionResult>>
            },
        );
        let options = DurableOptions::new(models);
        options
            .registry
            .install(Extension {
                name: "bounded".into(),
                tools: vec![tool],
                hooks: Hooks {
                    after_tool: Some(hook),
                    ..Default::default()
                },
                ..Default::default()
            })
            .unwrap();
        let durable = Durable::open(Arc::new(MemoryStorage::new()), options)
            .await
            .unwrap();
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
        root.submit(SubmissionDraft::input("go"))
            .await
            .unwrap()
            .wait()
            .await
            .unwrap();
        let context = root.context(None).await.unwrap();
        let entry = context
            .entries
            .iter()
            .find(|e| e.kind == "pi.tool-result")
            .unwrap();
        let diagnostics = entry.data.as_ref().unwrap()["diagnostics"]
            .as_array()
            .unwrap();
        assert!(
            observed.lock().unwrap()[0].is_empty(),
            "harness truncation follows after_tool"
        );
        assert_eq!(
            diagnostics.len(),
            usize::from(replacement != Some("abc")),
            "only None preserves retained provenance; explicit modified output bounds independently"
        );
        assert_eq!(
            entry.model.as_ref().unwrap()[0]["content"][0]["text"],
            json!(if replacement == Some("uvwxyz") {
                "uvw"
            } else {
                "abc"
            })
        );
        durable.close().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn event_attachment_uses_current_held_task_baseline_on_existing_mount() {
    let clock = Arc::new(TestClock(std::sync::atomic::AtomicU64::new(1000)));
    let (models, _) = models(&json!({"responses":[{"text":"answer"}]}), clock).await;
    let release = Arc::new(tokio::sync::Notify::new());
    let released = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let gate = release.clone();
    let flag = released.clone();
    let child = TaskDefinition::new("held-child", 1, |_| Ok(json!({"phase":"wait"})))
        .unwrap()
        .phase("wait", move |_, inv| {
            let gate = gate.clone();
            let flag = flag.clone();
            async move {
                loop {
                    let wake = gate.notified();
                    if flag.load(std::sync::atomic::Ordering::SeqCst) {
                        break;
                    }
                    wake.await;
                }
                inv.commit(|_, _| {
                    Ok(Some(TaskState::Terminal {
                        outcome: TaskOutcome::Completed {
                            result: serde_json::Value::Null,
                        },
                    }))
                })
                .await
            }
        });
    let captured = Arc::new(std::sync::Mutex::new(None));
    let seen = captured.clone();
    let definition = child.clone();
    let hook = Arc::new(move |_: pi_ai::AssistantMessage, api: HookApi| {
        let child = definition.clone();
        let seen = seen.clone();
        Box::pin(async move {
            *seen.lock().unwrap() = Some(api.task_id());
            api.create_task(
                child,
                serde_json::Value::Null,
                TaskOptions {
                    owner: Some(api.task_id()),
                    ..Default::default()
                },
            )
            .await?;
            Ok(())
        }) as RuntimeFuture<()>
    });
    let options = DurableOptions::new(models);
    options
        .registry
        .install(Extension {
            name: "held".into(),
            tasks: vec![child],
            hooks: Hooks {
                after_response: Some(hook),
                ..Default::default()
            },
            ..Default::default()
        })
        .unwrap();
    let d = Durable::open(Arc::new(MemoryStorage::new()), options)
        .await
        .unwrap();
    let c = d
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
    let mounted = c.view_state().await.unwrap();
    c.submit(SubmissionDraft::input("go"))
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    let task = captured.lock().unwrap().unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if matches!(
                d.get_task(task).await.unwrap().unwrap().state,
                TaskState::Completing { .. }
            ) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let events = c.watch_events().await.unwrap();
    assert!(events.snapshot.0.get("run").is_none());
    released.store(true, std::sync::atomic::Ordering::SeqCst);
    release.notify_waiters();
    d.wait_for_task(task).await.unwrap();
    let entry = c
        .commit(move |tx| tx.append_entry(c.id, EntryDraft::new("note")))
        .await
        .unwrap();
    let mut observed = vec![];
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let batch = events.next().await.unwrap().unwrap();
            let finished = batch
                .events
                .iter()
                .any(|e| e.0["type"] == "entry_appended" && e.0["entry"]["id"] == json!(entry.id));
            observed.extend(batch.events);
            if finished {
                break;
            }
        }
    })
    .await
    .unwrap();
    assert!(
        !observed.iter().any(|e| e.0["type"] == "turn_end"),
        "terminal release of a generation held before attachment must not end its turn twice"
    );
    assert!(!mounted.value().await.entries.is_empty());
    events.stop();
    d.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn event_record_only_fault_and_last_public_drop_during_callback() {
    let store = Arc::new(MemoryStorage::new());
    let (d, c, _) = open(store, &json!({"responses":[]})).await;
    let stream = c.watch_events().await.unwrap();
    let definition = TaskDefinition::new("fault-event", 1, |_| Ok(json!({"phase":"fail"})))
        .unwrap()
        .phase("fail", |_, _| async {
            Err(Error::request("intentional event failure"))
        });
    d.registry()
        .install(Extension {
            name: "fault-event".into(),
            tasks: vec![definition.clone()],
            ..Default::default()
        })
        .unwrap();
    let id = d
        .commit(move |tx| {
            create_task(
                tx,
                &definition,
                serde_json::Value::Null,
                TaskOptions {
                    conversation_id: Some(ROOT_CONVERSATION_ID),
                    ..Default::default()
                },
            )
        })
        .await
        .unwrap();
    d.resume();
    d.wait_for_task(id).await.unwrap();
    let batch = tokio::time::timeout(std::time::Duration::from_secs(2), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        batch
            .events
            .iter()
            .map(|e| e.0["type"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["task_failed"]
    );
    assert_eq!(
        batch.events[0].0["message"],
        json!("intentional event failure")
    );
    stream.stop();
    let stream = c.watch_events().await.unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let signal = entered.clone();
    let hold = release.clone();
    let count = calls.clone();
    stream
        .start(move |_| {
            let signal = signal.clone();
            let hold = hold.clone();
            let count = count.clone();
            async move {
                count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                signal.notify_one();
                hold.notified().await;
                Ok(())
            }
        })
        .unwrap();
    c.commit(|tx| tx.append_entry(ROOT_CONVERSATION_ID, EntryDraft::new("one")))
        .await
        .unwrap();
    entered.notified().await;
    drop(stream);
    c.commit(|tx| tx.append_entry(ROOT_CONVERSATION_ID, EntryDraft::new("two")))
        .await
        .unwrap();
    release.notify_one();
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    assert_eq!(
        calls.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "listener worker does not retain public custody"
    );
    d.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn automatic_threshold_background_and_overflow_compaction_use_real_tasks() {
    for mode in ["blocking", "background", "overflow"] {
        let clock = Arc::new(TestClock(std::sync::atomic::AtomicU64::new(1000)));
        let responses = if mode == "overflow" {
            json!([{"text":"","stopReason":"error","errorMessage":"maximum context length exceeded"},{"text":"complete"},{"text":"complete"}])
        } else {
            json!([{"text":"complete"},{"text":"complete"}])
        };
        let window = if mode == "blocking" {
            2000
        } else if mode == "background" {
            10000
        } else {
            100000
        };
        let(models,state)=models(&json!({"models":[{"id":"scripted-1","contextWindow":window,"maxTokens":1000}],"responses":responses}),clock).await;
        let mut options = DurableOptions::new(models);
        let background = if mode == "background" { 8000 } else { 0 };
        options.settings = Arc::new(move || DurableSettings {
            compaction: CompactionPolicy {
                enabled: true,
                reserve_tokens: 1000,
                keep_recent_tokens: 20,
                background_tokens: background,
            },
            ..Default::default()
        });
        let d = Durable::open(Arc::new(MemoryStorage::new()), options)
            .await
            .unwrap();
        let c = d
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
        for _ in 0..6 {
            let message = json!({"role":"user","content":"history ".repeat(100),"timestamp":1000});
            c.commit(move |tx| {
                let mut e = EntryDraft::new("history");
                e.model = Some(vec![message]);
                tx.append_entry(ROOT_CONVERSATION_ID, e)
            })
            .await
            .unwrap();
        }
        let submission = c.submit(SubmissionDraft::input("go")).await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(3), submission.wait())
            .await
            .unwrap()
            .unwrap();
        d.wait_for_idle().await.unwrap();
        let tasks = d
            .session()
            .scan_tasks(
                TaskQuery {
                    kind: Some("pi.compaction".into()),
                    ..Default::default()
                },
                100,
                None,
            )
            .await
            .unwrap();
        assert_eq!(tasks.items.len(), 1, "{mode}");
        let settled = d.wait_for_task(tasks.items[0].id).await.unwrap();
        assert_eq!(
            tasks.items[0].input["reason"],
            json!(if mode == "overflow" {
                "overflow"
            } else {
                "threshold"
            })
        );
        assert!(
            matches!(
                settled.state,
                TaskState::Terminal {
                    outcome: TaskOutcome::Completed { .. }
                }
            ),
            "{mode}: {:?}",
            settled.state
        );
        assert_eq!(tasks.items[0].background, mode == "background");
        let entries = c.entries(EntryQuery::new(c.id), 100, None).await.unwrap();
        assert_eq!(
            entries.items.iter().filter(|e| e.kind == "history").count(),
            6
        );
        assert_eq!(
            entries
                .items
                .iter()
                .filter(|e| e.kind == "pi.compaction")
                .count(),
            1,
            "{mode}"
        );
        assert_eq!(
            state.requests.lock().unwrap().len(),
            if mode == "overflow" { 3 } else { 2 }
        );
        d.close().await.unwrap();
    }
}

#[test]
fn progress_host_callbacks_reenter_without_locking_out_progress_or_close() {
    const CHILD: &str = "PIRUA_PROGRESS_REENTRANCY_CHILD";
    const NAME: &str = "progress_host_callbacks_reenter_without_locking_out_progress_or_close";
    let Ok(mode) = std::env::var(CHILD) else {
        for mode in ["settings", "clock", "timer"] {
            let mut child = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", NAME, "--nocapture"])
                .env(CHILD, mode)
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .unwrap();
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(8);
            let timed_out = loop {
                if child.try_wait().unwrap().is_some() {
                    break false;
                }
                if std::time::Instant::now() >= deadline {
                    child.kill().unwrap();
                    break true;
                }
                std::thread::sleep(std::time::Duration::from_millis(10));
            };
            let output = child.wait_with_output().unwrap();
            assert!(
                !timed_out && output.status.success(),
                "{mode}: timed_out={timed_out}, status={}, stdout={}, stderr={}",
                output.status,
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        return;
    };
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            use std::sync::{Mutex, atomic::{AtomicBool, AtomicUsize, Ordering}};
            use std::time::Duration;
            let captured: Arc<Mutex<Option<ToolExecutionApi>>> = Arc::new(Mutex::new(None));
            let reading = Arc::new(AtomicBool::new(false));
            let reads = Arc::new(AtomicUsize::new(0));
            let read: Arc<dyn Fn() + Send + Sync> = {
                let captured = captured.clone();
                let reading = reading.clone();
                let reads = reads.clone();
                Arc::new(move || {
                    // A clock read itself checks liveness through that same host clock.
                    if !reading.swap(true, Ordering::SeqCst) {
                        let api = captured.lock().unwrap().clone();
                        if let Some(api) = api {
                            eprintln!("REENTRANT_READ_BEGIN");
                            if api.retained_output().is_ok() {
                                reads.fetch_add(1, Ordering::SeqCst);
                            }
                            eprintln!("REENTRANT_READ_END");
                        }
                        reading.store(false, Ordering::SeqCst);
                    }
                })
            };
            let clock = Arc::new(TestClock(std::sync::atomic::AtomicU64::new(1000)));
            let (models, _) = models(&json!({"responses":[{"calls":[{"id":"call","name":"work","arguments":{}}]},{"text":"done"}]}), clock).await;
            let mut options = DurableOptions::new(models);
            let interval = if mode == "timer" { 1000.0 } else { 20.0 };
            let settings_read = read.clone();
            let settings_mode = mode == "settings";
            options.settings = Arc::new(move || {
                if settings_mode { settings_read(); }
                DurableSettings { output_interval_ms: interval, ..Default::default() }
            });
            let clock_read = read.clone();
            let clock_mode = mode == "clock";
            let started = std::time::Instant::now();
            options.scheduler.now = Arc::new(move || {
                if clock_mode { clock_read(); }
                1000.0 + started.elapsed().as_secs_f64() * 1000.0
            });
            let created = Arc::new(AtomicUsize::new(0));
            let cancelled = Arc::new(AtomicUsize::new(0));
            let armed = Arc::new(tokio::sync::Notify::new());
            if mode == "timer" {
                let read = read.clone();
                let created = created.clone();
                let cancelled = cancelled.clone();
                let armed = armed.clone();
                options.scheduler.timer_host = Some(Arc::new(move |callback, delay| {
                    read();
                    let index = created.fetch_add(1, Ordering::SeqCst);
                    // Exercise wake-before-handle-installation without advancing the clock.
                    let task = if index == 0 {
                        callback();
                        None
                    } else {
                        Some(tokio::spawn(async move {
                            tokio::time::sleep(Duration::from_secs_f64(delay / 1000.0)).await;
                            callback();
                        }))
                    };
                    armed.notify_waiters();
                    let read = read.clone();
                    let cancelled = cancelled.clone();
                    Timer::new(move || {
                        read();
                        cancelled.fetch_add(1, Ordering::SeqCst);
                        if let Some(task) = &task { task.abort(); }
                    })
                }));
            }
            let timer_mode = mode == "timer";
            let tool = {
                let captured = captured.clone();
                let created = created.clone();
                let armed = armed.clone();
                ToolRegistration::new(serde_json::from_value(json!({"name":"work","description":"reentrant host callbacks","parameters":{"type":"object","properties":{}}})).unwrap(), move |inv| {
                    let captured = captured.clone();
                    let created = created.clone();
                    let armed = armed.clone();
                    Box::pin(async move {
                        *captured.lock().unwrap() = Some(inv.api.clone());
                        inv.api.report_progress(ToolProgress::Output { chunk: OutputChunk::Text("first".into()), skipped: None }).await?;
                        let c = inv.api.identity().conversation_id;
                        let live = inv.api.snapshot(live_doc(), live_doc().address(Scope::Conversation { conversation_id: c }, None)?).await?.unwrap();
                        assert!(live["tools"].as_array().unwrap().iter().any(|s| s["output"] == "first"), "acknowledgement follows the covering commit");
                        let result = inv.api.execute_tool("child".into(), serde_json::from_value(json!({})).unwrap(), NestedToolOptions { key: Some("child".into()), progress: Some(false) }).await?;
                        assert!(!result.is_error);
                        if timer_mode {
                            inv.api.output(OutputChunk::Text("second".into()), None)?;
                            tokio::time::timeout(Duration::from_secs(2), async {
                                loop {
                                    let changed = armed.notified();
                                    if created.load(Ordering::SeqCst) >= 2 { break; }
                                    changed.await;
                                }
                            }).await.unwrap();
                        } else {
                            inv.api.details(serde_json::from_value(json!({"step":1})).unwrap()).await?;
                        }
                        Ok(ToolExecutionResult::default())
                    })
                })
            };
            let child = ToolRegistration::new(serde_json::from_value(json!({"name":"child","description":"nested admission","parameters":{"type":"object","properties":{}}})).unwrap(), |_| Box::pin(async { Ok(ToolExecutionResult::default()) }));
            options.registry.install(Extension { name: "reentrant".into(), tools: vec![tool, child], ..Default::default() }).unwrap();
            let durable = Durable::open(Arc::new(MemoryStorage::new()), options).await.unwrap();
            let root = durable.root(ConversationOptions { agent: Some(AgentChange { model: pi_ai::Presence::Value(ModelRef { provider: "oracle".into(), model_id: "scripted-1".into() }), ..Default::default() }), ..Default::default() }).await.unwrap();
            let result = tokio::time::timeout(Duration::from_secs(3), async { root.submit(SubmissionDraft::input("go")).await.unwrap().wait().await.unwrap() }).await.unwrap();
            assert!(matches!(result.state, SubmissionState::Done { .. }));
            assert!(reads.load(Ordering::SeqCst) > 0, "host callback actually reentered the live tool API");
            if timer_mode {
                assert!(created.load(Ordering::SeqCst) >= 2, "early timer wake was rearmed");
                assert!(cancelled.load(Ordering::SeqCst) >= 2, "both early and pending timer handles retired");
            }
            let context = root.context(None).await.unwrap();
            let result = context.entries.iter().find(|e| e.kind == "pi.tool-result").unwrap();
            assert_eq!(result.model.as_ref().unwrap()[0]["content"][0]["text"], if timer_mode { "firstsecond" } else { "first" });
            captured.lock().unwrap().take();
            tokio::time::timeout(Duration::from_secs(1), durable.close()).await.unwrap().unwrap();
        });
}
