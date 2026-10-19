use super::*;
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn emitted_unawaited_tool_keeps_cancelled_row_without_admission() {
    for inner in [
        Arc::new(MemoryStorage::new()) as Arc<dyn Storage>,
        sqlite().0,
    ] {
        let events = Arc::new(Events::default());
        let effects = events.clone();
        let echo = tool("echo", move |_| {
            let effects = effects.clone();
            Box::pin(async move {
                *effects
                    .effects
                    .lock()
                    .unwrap()
                    .entry("echo".into())
                    .or_default() += 1;
                Ok(ToolExecutionResult::default())
            })
        });
        let h = Harness::open_with(
            inner,
            CodeModeOptions::new(worker()),
            vec![echo],
            Default::default(),
            events.clone(),
        )
        .await;
        let mut code = CodeModeOptions::new(h.sandbox.clone());
        code.seed = Arc::new(|| Ok(424242));
        let sandbox = h.sandbox.clone();
        let first_call = std::sync::atomic::AtomicBool::new(true);
        let observed = events.clone();
        code.observe = Some(Arc::new(move |event| {
            if event["kind"] == "call" && first_call.swap(false, Ordering::SeqCst) {
                // Hold only publication of this real Call until the actual worker
                // has finished. No VM event or outcome is supplied by the test.
                let deadline = std::time::Instant::now() + Duration::from_secs(10);
                while sandbox.active_workers() != 0 {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "worker did not finish"
                    );
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
            observed.vm.lock().unwrap().push(event);
        }));
        h.options.registry.uninstall("codemode").unwrap();
        h.options
            .registry
            .install(create_codemode_extension(code).unwrap())
            .unwrap();
        let result = h.run("tools.echo({}); return 'ended'").await;
        assert_eq!(result["isError"], false);
        assert!(text(&result).ends_with("ended"));
        assert_eq!(
            result["details"]["calls"],
            json!([
                {"id":1,"name":"echo","args":"{}","status":"cancelled"}
            ])
        );
        assert_eq!(events.count("echo"), 0);
        assert!(events.vm.lock().unwrap().iter().any(|event| {
            event["kind"] == "terminal" && event["result"]["during"] == json!({"seq":0,"issued":1})
        }));
        let result = h.run("await tools.echo({}); return 'ended'").await;
        assert_eq!(result["isError"], false);
        assert_eq!(result["details"]["calls"][0]["id"], 1);
        assert_eq!(result["details"]["calls"][0]["name"], "echo");
        assert_eq!(result["details"]["calls"][0]["status"], "ok");
        assert_eq!(events.count("echo"), 1);
        h.close().await;
        assert_eq!(events.count("echo"), 1, "no late unawaited effect");
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_count_commit_gate_and_queued_calls_after_terminal() {
    for inner in [
        Arc::new(MemoryStorage::new()) as Arc<dyn Storage>,
        sqlite().0,
    ] {
        let events = Arc::new(Events::default());
        let e = events.clone();
        let mut options = CodeModeOptions::new(worker());
        options.globals.push(CodeModeGlobal {
            name: "effect".into(),
            spread: false,
            execute: Arc::new(move |_, _| {
                let e = e.clone();
                Box::pin(async move {
                    *e.effects
                        .lock()
                        .unwrap()
                        .entry("effect".into())
                        .or_default() += 1;
                    Ok(Some(json!(1)))
                })
            }),
        });
        let h = Harness::open_with(inner, options, Vec::new(), Default::default(), events).await;
        h.store.pause_count.store(true, Ordering::SeqCst);
        let input = h.submit("return await effect()").await;
        tokio::time::timeout(Duration::from_secs(10), h.store.entered.notified())
            .await
            .unwrap();
        assert_eq!(h.events.count("effect"), 0);
        assert!(docs(&h).1.get("firstIssued").is_none());
        assert!(
            h.events
                .vm
                .lock()
                .unwrap()
                .iter()
                .any(|v| v["kind"] == "idle")
        );
        h.store.release();
        input_on(&h, input.id).await;
        assert_eq!(h.events.count("effect"), 1);
        let _ = h.run("effect(); return 'ended'").await;
        assert_eq!(
            h.events.count("effect"),
            1,
            "unawaited queued call must not admit after actual VM terminal"
        );
        h.close().await;
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_equivalents_of_scripted_terminal_first_and_held_global() {
    for global in [false, true] {
        let (inner, path) = sqlite();
        let events = Arc::new(Events::default());
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
        let mut options = CodeModeOptions::new(worker());
        let e = events.clone();
        options.globals.push(CodeModeGlobal {
            name: "effect".into(),
            spread: false,
            execute: Arc::new(move |_, _| {
                let e = e.clone();
                Box::pin(async move {
                    *e.effects
                        .lock()
                        .unwrap()
                        .entry("effect".into())
                        .or_default() += 1;
                    Ok(Some(json!(1)))
                })
            }),
        });
        let mut h =
            Harness::open_with(inner, options, vec![hold], Default::default(), events).await;
        let script = if global {
            "if(load('k'))await effect();else await tools.hold({});return 1"
        } else {
            "if(load('k'))return 1;await tools.hold({});return 2"
        };
        let input = h.submit(script).await;
        eventually(async || h.events.count("hold") == 1).await;
        reopen(&mut h, &path).await;
        patch(
            &h,
            codemode_run_doc(),
            &[("start.store", json!({"k":true}))],
        )
        .await;
        h.durable.resume();
        input_on(&h, input.id).await;
        let result = h.result().await;
        assert_eq!(result["isError"], true, "{result}");
        assert!(text(&result).contains("different path"));
        assert_eq!(h.events.count("effect"), 0);
        if !global {
            assert!(
                h.events
                    .vm
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|v| v["kind"] == "terminal"
                        && v["result"]["during"] == json!({"seq":0,"issued":0}))
            );
        }
        h.close().await;
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_document_policies_checkpoint_history_and_asof_fork() {
    for inner in [
        Arc::new(MemoryStorage::new()) as Arc<dyn Storage>,
        sqlite().0,
    ] {
        let session = Session::open(inner.clone()).unwrap();
        let def = TaskDefinition::new("codemode-document-policy-owner", 1, |_| {
            Ok(json!({"phase":"run"}))
        })
        .unwrap()
        .phase("run", |_, _| async { Ok(()) });
        let task = session
            .commit(move |tx| {
                let root = tx.ensure_root_conversation()?;
                crate::create_task(
                    tx,
                    &def,
                    json!({}),
                    TaskOptions {
                        conversation_id: Some(root.id),
                        ..Default::default()
                    },
                )
            })
            .await
            .unwrap();
        let scope = Scope::Task { task_id: task };
        let sc = scope.clone();
        session
            .commit(move |tx| {
                for def in [codemode_run_doc(), codemode_journal_doc()] {
                    tx.doc(&def, def.address(sc.clone(), None)?, Value::Null)?;
                }
                Ok(())
            })
            .await
            .unwrap();
        for index in 0..70 {
            let sc = scope.clone();
            session
                .commit(move |tx| {
                    let (id, _) =
                        documents::read::<CodeModeRunState>(tx, &codemode_run_doc(), sc.clone())?;
                    documents::field(tx, id, "outputDir", format!("path-{index}"))?;
                    let (id, _) =
                        documents::read::<CodeModeJournalState>(tx, &codemode_journal_doc(), sc)?;
                    documents::field(tx, id, "firstIssued", index + 1)?;
                    Ok(())
                })
                .await
                .unwrap();
        }
        for def in [codemode_run_doc(), codemode_journal_doc()] {
            let record = inner
                .find_document(
                    &def.address(scope.clone(), None).unwrap(),
                    DocumentPoint::Current,
                )
                .unwrap()
                .unwrap();
            assert_eq!(
                inner
                    .document(record.identity.id, DocumentPoint::Current)
                    .unwrap()
                    .unwrap()
                    .deltas_since_base,
                70
            );
        }
        let c = Scope::Conversation {
            conversation_id: ROOT_CONVERSATION_ID,
        };
        let mut cutoff = None;
        for counter in 0..=64 {
            let scope = c.clone();
            let entry = session
                .commit(move |tx| {
                    let (id, _) =
                        documents::read::<CodeModeStoreState>(tx, &codemode_store_doc(), scope)?;
                    tx.apply(
                        id,
                        Op::Set(vec!["values".into(), "counter".into()], json!(counter)),
                    )?;
                    Ok(tx
                        .append_entry(ROOT_CONVERSATION_ID, EntryDraft::new("counter"))?
                        .id)
                })
                .await
                .unwrap();
            if counter == 32 {
                cutoff = Some(entry);
            }
        }
        let record = inner
            .find_document(
                &codemode_store_doc().address(c.clone(), None).unwrap(),
                DocumentPoint::Current,
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            inner
                .document(record.identity.id, DocumentPoint::Current)
                .unwrap()
                .unwrap()
                .deltas_since_base,
            64
        );
        let sc = c.clone();
        session
            .commit(move |tx| {
                let (id, _) = documents::read::<CodeModeStoreState>(tx, &codemode_store_doc(), sc)?;
                tx.apply(
                    id,
                    Op::Set(vec!["values".into(), "counter".into()], json!(65)),
                )
            })
            .await
            .unwrap();
        assert_eq!(
            inner
                .document(record.identity.id, DocumentPoint::Current)
                .unwrap()
                .unwrap()
                .deltas_since_base,
            0
        );
        assert_eq!(
            session
                .snapshot_as_of(
                    codemode_store_doc(),
                    codemode_store_doc().address(c.clone(), None).unwrap(),
                    cutoff.unwrap()
                )
                .await
                .unwrap()
                .unwrap()["values"]["counter"],
            32
        );
        let child = session
            .commit(move |tx| {
                tx.fork_conversation(
                    ROOT_CONVERSATION_ID,
                    cutoff.unwrap(),
                    ConversationOwnership::Ownerless,
                )
            })
            .await
            .unwrap();
        assert_eq!(
            session
                .snapshot(
                    codemode_store_doc(),
                    codemode_store_doc()
                        .address(
                            Scope::Conversation {
                                conversation_id: child.id
                            },
                            None
                        )
                        .unwrap()
                )
                .await
                .unwrap()
                .unwrap()["values"]["counter"],
            32
        );
        session
            .commit(move |tx| {
                let (id, _) = documents::read::<CodeModeStoreState>(tx, &codemode_store_doc(), c)?;
                tx.apply(
                    id,
                    Op::Set(vec!["values".into(), "counter".into()], json!(66)),
                )
            })
            .await
            .unwrap();
        assert_eq!(
            inner
                .document(record.identity.id, DocumentPoint::Current)
                .unwrap()
                .unwrap()
                .deltas_since_base,
            1
        );
        session.close().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raw_nonobject_validation_is_journaled_replayed_and_kind_checked() {
    for change_target in [false, true] {
        let (inner, path) = sqlite();
        let events = Arc::new(Events::default());
        let e = events.clone();
        let echo = tool("echo", move |_| {
            let e = e.clone();
            Box::pin(async move {
                *e.effects.lock().unwrap().entry("echo".into()).or_default() += 1;
                Ok(Default::default())
            })
        });
        let e = events.clone();
        let mut hold = tool("hold", move |inv| {
            let e = e.clone();
            Box::pin(async move {
                let count = {
                    let mut n = e.effects.lock().unwrap();
                    let n = n.entry("hold".into()).or_default();
                    *n += 1;
                    *n
                };
                if count == 1 {
                    e.wait("hold", inv.api.signal()).await;
                }
                Ok(Default::default())
            })
        });
        hold.replay = ReplayPolicy::Safe;
        let e = events.clone();
        let mut options = CodeModeOptions::new(worker());
        options.globals.push(CodeModeGlobal {
            name: "effect".into(),
            spread: false,
            execute: Arc::new(move |_, _| {
                let e = e.clone();
                Box::pin(async move {
                    *e.effects
                        .lock()
                        .unwrap()
                        .entry("effect".into())
                        .or_default() += 1;
                    Ok(None)
                })
            }),
        });
        let mut h =
            Harness::open_with(inner, options, vec![echo, hold], Default::default(), events).await;
        let input=h.submit("const errors=[];if(load('swap'))await effect();else{try{await tools.echo(7)}catch(e){errors.push(e.message)}}try{await tools.echo([1])}catch(e){errors.push(e.message)}await tools.hold({});return errors").await;
        eventually(async || h.events.count("hold") == 1).await;
        let (_, journal) = docs(&h);
        assert_eq!(journal["entries"][0]["global"]["target"], "tool");
        assert_eq!(journal["entries"][0]["global"]["args"], 7);
        assert_eq!(journal["entries"][1]["global"]["args"], json!([1]));
        let task = TaskId::new(h.events.code_task.load(Ordering::SeqCst)).unwrap();
        for key in ["c1", "c2"] {
            assert!(
                h.store
                    .find_document(
                        &nested_call_doc()
                            .address(Scope::Task { task_id: task }, Some(key.into()))
                            .unwrap(),
                        DocumentPoint::Current
                    )
                    .unwrap()
                    .is_none(),
                "validation fabricates no nested receipt"
            );
        }
        reopen(&mut h, &path).await;
        if change_target {
            patch(
                &h,
                codemode_run_doc(),
                &[("start.store", json!({"swap":true}))],
            )
            .await;
        }
        h.durable.resume();
        input_on(&h, input.id).await;
        let result = h.result().await;
        assert_eq!(h.events.count("echo"), 0);
        assert_eq!(h.events.count("effect"), 0);
        if change_target {
            assert_eq!(result["isError"], true);
            assert!(text(&result).contains("different path"));
        } else {
            assert_eq!(result["isError"], false);
            assert!(text(&result).contains("Tool arguments must be an object"));
            let vm = h.events.vm.lock().unwrap();
            assert_eq!(
                vm.iter()
                    .filter(|v| v["kind"] == "deliver" && v["id"] == 1)
                    .count(),
                2
            );
            assert_eq!(
                vm.iter()
                    .filter(|v| v["kind"] == "deliver" && v["id"] == 2)
                    .count(),
                2
            );
        }
        h.close().await;
    }
}
