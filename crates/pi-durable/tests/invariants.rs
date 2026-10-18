mod support;
use pi_durable::*;
use serde_json::json;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};
use support::*;

#[test]
fn inspection_progress_memos_and_atomic_terminal_documents() {
    runtime().block_on(async {
        for (_, store, _temp) in stores() {
            let registry = Registry::new();
            let calls = Arc::new(AtomicU64::new(0));
            let seen = Arc::new(Mutex::new(Vec::new()));
            let calls2 = calls.clone();
            let seen2 = seen.clone();
            let doc = session::DocDefinition::new(
                "task-progress",
                1,
                session::DocumentSemantics::Task,
                false,
                |_| Ok(json!({"n":0})),
                |_, _, _| Ok(false),
            )
            .unwrap();
            let task = TaskDefinition::new("counter", 1, |_| Ok(json!({"phase":"run","n":0})))
                .unwrap()
                .phase("run", move |record, inv| {
                    let calls = calls2.clone();
                    let seen = seen2.clone();
                    let doc = doc.clone();
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        assert_eq!(
                            inv.memo_or_insert("toString", json!(null)).await?,
                            json!(null)
                        );
                        assert_eq!(
                            inv.memo_or_insert("toString", json!("loser")).await?,
                            json!(null)
                        );
                        assert_eq!(inv.memo("missing").await?, None);
                        let n = match record.state {
                            TaskState::Running { checkpoint } => {
                                checkpoint["n"].as_u64().unwrap() + 1
                            }
                            _ => unreachable!(),
                        };
                        inv.commit(move |tx, current| {
                            let id = tx.doc(
                                &doc,
                                doc.address(
                                    session::Scope::Task {
                                        task_id: current.id,
                                    },
                                    None,
                                )?,
                                json!(null),
                            )?;
                            tx.apply(
                                id,
                                session::Op::Set(vec![session::Segment::Key("n".into())], json!(n)),
                            )?;
                            Ok(Some(if n == 3 {
                                complete(json!(n))
                            } else {
                                TaskState::Running {
                                    checkpoint: json!({"phase":"run","n":n}),
                                }
                            }))
                        })
                        .await?;
                        if n == 3 {
                            seen.lock().unwrap().push(inv.clone());
                            assert!(
                                inv.commit(|_, _| Ok(Some(complete(json!("stale")))))
                                    .await
                                    .is_err()
                            );
                        }
                        Ok(())
                    }
                });
            registry.install("counter", vec![task.clone()]).unwrap();
            let scheduler = scheduler(store.clone(), registry, Default::default()).await;
            let id = scheduler
                .create_task(&task, json!(null), task_options())
                .await
                .unwrap();
            assert_eq!(scheduler.inspect().await.unwrap().scheduling, "paused");
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            let receipt = wait(&scheduler, id).await;
            assert_eq!(receipt.state, complete(json!(3)));
            assert_eq!(calls.load(Ordering::SeqCst), 3);
            assert_eq!(receipt.memos, None);
            let address = session::DocumentAddress {
                kind: "task-progress".into(),
                scope: session::Scope::Task { task_id: id },
                key: None,
            };
            assert!(
                store
                    .find_document(&address, session::DocumentPoint::Current)
                    .unwrap()
                    .is_none()
            );
            let ended = seen.lock().unwrap()[0].clone();
            assert!(ended.memo_or_insert("late", json!(1)).await.is_err());
            close(&scheduler).await;
        }
    });
}

#[test]
fn no_progress_and_handler_errors_fault_without_overwriting_decided_outcomes() {
    runtime().block_on(async {
        for (_, store, _temp) in stores() {
            let registry = Registry::new();
            let idle = TaskDefinition::new("idle", 1, |_| Ok(json!({"phase":"run"})))
                .unwrap()
                .phase(
                    "run",
                    |_, inv| async move { inv.commit(|_, _| Ok(None)).await },
                );
            let throws = TaskDefinition::new("throws", 1, |_| Ok(json!({"phase":"run"})))
                .unwrap()
                .phase("run", |_, _| async { Err(Error::request("boom")) });
            let decided = TaskDefinition::new("decided", 1, |_| Ok(json!({"phase":"run"})))
                .unwrap()
                .phase("run", |_, inv| async move {
                    inv.commit(|_, _| Ok(Some(complete(json!(42))))).await?;
                    Err(Error::request("after terminal"))
                });
            registry
                .install("tasks", vec![idle.clone(), throws.clone(), decided.clone()])
                .unwrap();
            let scheduler = scheduler(store, registry, Default::default()).await;
            for task in [&idle, &throws] {
                let id = scheduler
                    .create_task(task, json!(null), task_options())
                    .await
                    .unwrap();
                assert!(matches!(
                    wait(&scheduler, id).await.state,
                    TaskState::Terminal {
                        outcome: TaskOutcome::Faulted { .. }
                    }
                ));
            }
            let id = scheduler
                .create_task(&decided, json!(null), task_options())
                .await
                .unwrap();
            assert_eq!(wait(&scheduler, id).await.state, complete(json!(42)));
            close(&scheduler).await;
        }
    });
}

#[test]
fn abort_commits_intent_before_signal_and_rejects_old_lease() {
    runtime().block_on(async {
        for (_, store, _temp) in stores() {
            let registry = Registry::new();
            let signal_seen = Arc::new(Mutex::new(None));
            let capture = signal_seen.clone();
            let raw = store.clone();
            let entered = Arc::new(AtomicU64::new(0));
            let entry = entered.clone();
            let task = TaskDefinition::new("gate", 1, |_| Ok(json!({"phase":"run"})))
                .unwrap()
                .phase("run", move |record, inv| {
                    let seen = capture.clone();
                    let raw = raw.clone();
                    let entry = entry.clone();
                    async move {
                        entry.store(1, Ordering::SeqCst);
                        inv.signal().cancelled().await;
                        let persisted = raw.task(record.id)?.unwrap();
                        let stale = inv.commit(|_, _| Ok(Some(complete(json!("stale"))))).await;
                        *seen.lock().unwrap() = Some((persisted.abort_requested, stale.is_err()));
                        Ok(())
                    }
                });
            registry.install("gate", vec![task.clone()]).unwrap();
            let scheduler = scheduler(store, registry, Default::default()).await;
            let id = scheduler
                .create_task(&task, json!(null), task_options())
                .await
                .unwrap();
            scheduler.resume();
            until(&scheduler, id, "running").await;
            while entered.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
            scheduler.abort_task(id).await.unwrap();
            let receipt = wait(&scheduler, id).await;
            assert!(matches!(
                receipt.state,
                TaskState::Terminal {
                    outcome: TaskOutcome::Aborted { .. }
                }
            ));
            assert_eq!(*signal_seen.lock().unwrap(), Some((true, true)));
            close(&scheduler).await;
        }
    });
}

#[test]
fn absolute_sleep_uses_wall_clock_and_early_wakes_do_not_dispatch() {
    runtime().block_on(async {
        for (_, store, _temp) in stores() {
            let clock = Arc::new(AtomicU64::new(1000));
            let now = clock.clone();
            let calls = Arc::new(AtomicU64::new(0));
            let count = calls.clone();
            let registry = Registry::new();
            let task = TaskDefinition::new("sleep", 1, |_| Ok(json!({"phase":"run"})))
                .unwrap()
                .phase("run", move |_, inv| {
                    let calls = count.clone();
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        inv.sleep_until(1500.0, json!({"phase":"done"})).await
                    }
                })
                .phase("done", |_, inv| async move {
                    let now = inv.now()?;
                    inv.commit(move |_, _| Ok(Some(complete(json!(now))))).await
                });
            registry.install("sleep", vec![task.clone()]).unwrap();
            let scheduler = scheduler(
                store,
                registry,
                SchedulerOptions {
                    now: Arc::new(move || now.load(Ordering::SeqCst) as f64),
                    on_wake: Some(Arc::new(|_| {})),
                    ..Default::default()
                },
            )
            .await;
            let id = scheduler
                .create_task(&task, json!(null), task_options())
                .await
                .unwrap();
            scheduler.resume();
            until(&scheduler, id, "sleeping").await;
            for _ in 0..5 {
                scheduler.resume();
                tokio::task::yield_now().await;
            }
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert_eq!(scheduler.wake().await.unwrap().at, Some(1500.0));
            clock.store(1499, Ordering::SeqCst);
            scheduler.resume();
            assert_eq!(
                scheduler
                    .get_task(id)
                    .await
                    .unwrap()
                    .unwrap()
                    .state
                    .status(),
                "sleeping"
            );
            clock.store(1500, Ordering::SeqCst);
            scheduler.resume();
            assert_eq!(wait(&scheduler, id).await.state, complete(json!(1500.0)));
            close(&scheduler).await;
        }
    });
}

#[test]
fn same_batch_new_owned_work_and_parent_outcome_rejects_then_owned_conversation_holds() {
    runtime().block_on(async {
        for (_, store, _temp) in stores() {
            let registry = Registry::new();
            let release = tokio::sync::watch::channel(false).0;
            let gate = release.clone();
            let calls = Arc::new(AtomicU64::new(0));
            let count = calls.clone();
            let captured = Arc::new(Mutex::new(Vec::new()));
            let capture = captured.clone();
            let child = TaskDefinition::new("inner", 1, |_| Ok(json!({"phase":"run"})))
                .unwrap()
                .phase("run", move |_, inv| {
                    let mut gate = gate.subscribe();
                    async move {
                        if !*gate.borrow() {
                            gate.changed().await.unwrap();
                        }
                        inv.commit(|_, _| Ok(Some(complete(json!("child"))))).await
                    }
                });
            let c = child.clone();
            let owner = TaskDefinition::new("owner", 1, |_| Ok(json!({"phase":"create"})))
                .unwrap()
                .phase("create", move |_, inv| {
                    let child = c.clone();
                    let count = count.clone();
                    let capture = capture.clone();
                    async move {
                        count.fetch_add(1, Ordering::SeqCst);
                        let child2 = child.clone();
                        let failed = inv
                            .transact(move |tx, current| {
                                let conversation = tx.create_conversation(
                                    session::ConversationOwnership::Task(current.id),
                                )?;
                                let id = create_task(
                                    tx,
                                    &child2,
                                    json!(null),
                                    TaskOptions {
                                        conversation_id: Some(conversation.id),
                                        ..Default::default()
                                    },
                                )?;
                                Ok((id, Some(complete(json!("too early")))))
                            })
                            .await;
                        assert!(failed.is_err());
                        let ids = inv
                            .transact(move |tx, current| {
                                let conversation = tx.create_conversation(
                                    session::ConversationOwnership::Task(current.id),
                                )?;
                                let id = create_task(
                                    tx,
                                    &child,
                                    json!(null),
                                    TaskOptions {
                                        conversation_id: Some(conversation.id),
                                        ..Default::default()
                                    },
                                )?;
                                Ok((
                                    (conversation.id, id),
                                    Some(TaskState::Running {
                                        checkpoint: json!({"phase":"decide"}),
                                    }),
                                ))
                            })
                            .await?;
                        capture.lock().unwrap().push(ids);
                        Ok(())
                    }
                })
                .phase("decide", |_, inv| async move {
                    inv.commit(|_, _| Ok(Some(complete(json!("owner"))))).await
                });
            registry
                .install("work", vec![owner.clone(), child])
                .unwrap();
            let scheduler = scheduler(store.clone(), registry, Default::default()).await;
            let id = scheduler
                .create_task(&owner, json!(null), task_options())
                .await
                .unwrap();
            scheduler.resume();
            until(&scheduler, id, "completing").await;
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert_eq!(
                store
                    .scan_conversations(&session::ConversationQuery::default(), 100, None)
                    .unwrap()
                    .items
                    .len(),
                2
            );
            assert_eq!(
                store
                    .scan_tasks(&session::TaskQuery::default(), 100, None)
                    .unwrap()
                    .items
                    .len(),
                2
            );
            release.send_replace(true);
            assert_eq!(wait(&scheduler, id).await.state, complete(json!("owner")));
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            close(&scheduler).await;
        }
    });
}

#[test]
fn concurrent_first_writer_memos_and_stale_timer_callbacks_are_lease_checked() {
    runtime().block_on(async {
        for (_, store, _temp) in stores() {
            let retained = Arc::new(Mutex::new(Vec::<Arc<dyn Fn() + Send + Sync>>::new()));
            let callbacks = retained.clone();
            let fires = Arc::new(AtomicU64::new(0));
            let fired = fires.clone();
            let registry = Registry::new();
            let ending = Arc::new(Mutex::new(None));
            let capture = ending.clone();
            let task = TaskDefinition::new("race", 1, |_| Ok(json!({"phase":"run"})))
                .unwrap()
                .phase("run", move |_, inv| {
                    let fired = fired.clone();
                    *capture.lock().unwrap() = Some(inv.signal());
                    async move {
                        let one = inv.clone();
                        let two = inv.clone();
                        let (a, b) = tokio_join_memos(one, two).await;
                        assert_eq!(a, b);
                        inv.set_timer(
                            move || {
                                fired.fetch_add(1, Ordering::SeqCst);
                            },
                            1.0,
                        )?;
                        inv.commit(move |_, _| Ok(Some(complete(a)))).await
                    }
                });
            registry.install("race", vec![task.clone()]).unwrap();
            let scheduler = scheduler(
                store,
                registry,
                SchedulerOptions {
                    timer_host: Some(Arc::new(move |callback, _| {
                        callbacks.lock().unwrap().push(callback);
                        Timer::new(|| {})
                    })),
                    ..Default::default()
                },
            )
            .await;
            let id = scheduler
                .create_task(&task, json!(null), task_options())
                .await
                .unwrap();
            wait(&scheduler, id).await;
            let ending = ending.lock().unwrap().clone().unwrap();
            ending.cancelled().await;
            for callback in retained.lock().unwrap().iter() {
                callback();
            }
            assert_eq!(fires.load(Ordering::SeqCst), 0);
            close(&scheduler).await;
        }
    });
}
async fn tokio_join_memos(a: Invocation, b: Invocation) -> (serde_json::Value, serde_json::Value) {
    let a = tokio::spawn(async move { a.memo_or_insert("winner", json!(1)).await.unwrap() });
    let b = tokio::spawn(async move { b.memo_or_insert("winner", json!(2)).await.unwrap() });
    (a.await.unwrap(), b.await.unwrap())
}

#[test]
fn runtime_cleanup_hooks_are_atomic_at_true_scheduler_terminal_and_rollback_on_failure() {
    runtime().block_on(async {
        for (_, store, _temp) in stores() {
            let registry = Registry::new();
            let calls = Arc::new(AtomicU64::new(0));
            let count = calls.clone();
            let cleanup_doc = session::DocDefinition::new(
                "cleanup",
                1,
                session::DocumentSemantics::Session,
                false,
                |_| Ok(json!({"ids":[]})),
                |_, _, _| Ok(false),
            )
            .unwrap();
            let doc = cleanup_doc.clone();
            let task = TaskDefinition::new("fault", 1, |_| Ok(json!({"phase":"run"})))
                .unwrap()
                .phase("run", |_, _| async { Err(Error::request("fault")) });
            registry.install("fault", vec![task.clone()]).unwrap();
            let scheduler = scheduler(
                store.clone(),
                registry,
                SchedulerOptions {
                    settle_outcome: Some(Arc::new(move |tx, record, _| {
                        count.fetch_add(1, Ordering::SeqCst);
                        assert_eq!(record.state.status(), "terminal");
                        assert!(tx.task(record.id).is_err());
                        let id = tx.doc(
                            &doc,
                            doc.address(session::Scope::Session, None)?,
                            json!(null),
                        )?;
                        tx.apply(
                            id,
                            session::Op::Set(
                                vec![session::Segment::Key("ids".into())],
                                json!([record.id]),
                            ),
                        )?;
                        Ok(())
                    })),
                    ..Default::default()
                },
            )
            .await;
            let id = scheduler
                .create_task(&task, json!(null), task_options())
                .await
                .unwrap();
            wait(&scheduler, id).await;
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            let address = cleanup_doc.address(session::Scope::Session, None).unwrap();
            let doc = store
                .find_document(&address, session::DocumentPoint::Current)
                .unwrap()
                .unwrap();
            assert_eq!(
                store
                    .document(doc.identity.id, session::DocumentPoint::Current)
                    .unwrap()
                    .unwrap()
                    .value,
                json!({"ids":[id]})
            );
            close(&scheduler).await;
        }
    });
}

#[test]
fn actual_runtime_interruption_reopens_running_record_and_invalidates_old_runtime() {
    let temp = Temp::new();
    let captured = Arc::new(Mutex::new(None));
    let capture = captured.clone();
    let registry = Registry::new();
    let task = TaskDefinition::new("interrupted", 1, |_| Ok(json!({"phase":"run"})))
        .unwrap()
        .phase("run", move |_, inv| {
            *capture.lock().unwrap() = Some(inv);
            std::future::pending::<Result<()>>()
        });
    registry.install("work", vec![task.clone()]).unwrap();
    let id = runtime().block_on(async {
        let store = Arc::new(session::SqliteStorage::open(&temp.0).unwrap());
        let scheduler = scheduler(store, registry.clone(), Default::default()).await;
        let id = scheduler
            .create_task(&task, json!(null), task_options())
            .await
            .unwrap();
        scheduler.resume();
        until(&scheduler, id, "running").await;
        while captured.lock().unwrap().is_none() {
            tokio::task::yield_now().await;
        }
        id
    });
    // Dropping the runtime cancels real scheduler/handler futures without close/reset.
    let old = captured.lock().unwrap().take().unwrap();
    runtime().block_on(async {
        let reopened = Arc::new(session::SqliteStorage::open(&temp.0).unwrap());
        let session = Session::open(reopened).unwrap();
        let scheduler = Scheduler::open(session, Registry::new(), Default::default())
            .await
            .unwrap();
        let record = scheduler.get_task(id).await.unwrap().unwrap();
        assert_eq!(record.state.status(), "pending");
        assert_eq!(record.restarts, Some(1));
        // An old process's runtime cannot be used in this runtime: Session storage is
        // closed explicitly below, while fresh task state comes solely from SQLite.
        assert!(
            old.commit(|_, _| Ok(Some(complete(json!("stale")))))
                .await
                .is_err()
        );
        close(&scheduler).await;
    });
}

#[test]
fn early_local_host_timer_rearms_against_absolute_deadline() {
    runtime().block_on(async {
        for (_, store, _temp) in stores() {
            let clock = Arc::new(AtomicU64::new(1000));
            let now = clock.clone();
            let callbacks = Arc::new(Mutex::new(Vec::<Arc<dyn Fn() + Send + Sync>>::new()));
            let host = callbacks.clone();
            let registry = Registry::new();
            let task = TaskDefinition::new("local-sleep", 1, |_| Ok(json!({"phase":"sleep"})))
                .unwrap()
                .phase("sleep", |_, inv| async move {
                    inv.sleep_until(2000.0, json!({"phase":"done"})).await
                })
                .phase("done", |_, inv| async move {
                    inv.commit(|_, _| Ok(Some(complete(json!("awake"))))).await
                });
            registry.install("local-sleep", vec![task.clone()]).unwrap();
            let scheduler = scheduler(
                store,
                registry,
                SchedulerOptions {
                    now: Arc::new(move || now.load(Ordering::SeqCst) as f64),
                    timer_host: Some(Arc::new(move |callback, _| {
                        host.lock().unwrap().push(callback);
                        Timer::new(|| {})
                    })),
                    ..Default::default()
                },
            )
            .await;
            let id = scheduler
                .create_task(&task, json!(null), task_options())
                .await
                .unwrap();
            scheduler.resume();
            until(&scheduler, id, "sleeping").await;
            while callbacks.lock().unwrap().is_empty() {
                tokio::task::yield_now().await;
            }
            let before = callbacks.lock().unwrap().len();
            let callback = callbacks.lock().unwrap().last().unwrap().clone();
            callback();
            tokio::time::timeout(std::time::Duration::from_secs(3), async {
                while callbacks.lock().unwrap().len() <= before {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("early timer must rearm");
            assert_eq!(
                scheduler
                    .get_task(id)
                    .await
                    .unwrap()
                    .unwrap()
                    .state
                    .status(),
                "sleeping"
            );
            clock.store(2000, Ordering::SeqCst);
            let callback = callbacks.lock().unwrap().last().unwrap().clone();
            callback();
            assert_eq!(wait(&scheduler, id).await.state, complete(json!("awake")));
            close(&scheduler).await;
        }
    });
}

#[test]
fn rejected_terminal_cleanup_rolls_back_and_stops_scheduling_without_handler_replay() {
    runtime().block_on(async {
        for (_, store, _temp) in stores() {
            let calls = Arc::new(AtomicU64::new(0));
            let count = calls.clone();
            let registry = Registry::new();
            let task = TaskDefinition::new("cleanup-fails", 1, |_| Ok(json!({"phase":"run"})))
                .unwrap()
                .phase("run", move |_, _| {
                    count.fetch_add(1, Ordering::SeqCst);
                    async { Err(Error::request("handler fault")) }
                });
            registry
                .install("cleanup-fails", vec![task.clone()])
                .unwrap();
            let scheduler = scheduler(
                store,
                registry,
                SchedulerOptions {
                    settle_outcome: Some(Arc::new(|tx, record, _| {
                        let mut entry = session::EntryDraft::new("cleanup");
                        entry.data = Some(json!("must roll back"));
                        tx.append_entry(record.conversation_id, entry)?;
                        Err(Error::validation("cleanup rejected"))
                    })),
                    ..Default::default()
                },
            )
            .await;
            let id = scheduler
                .create_task(&task, json!(null), task_options())
                .await
                .unwrap();
            scheduler.resume();
            let result = tokio::time::timeout(
                std::time::Duration::from_secs(3),
                scheduler.wait_for_task(id),
            )
            .await
            .unwrap();
            assert!(result.is_err());
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert!(scheduler.failure().is_some());
            let _ = scheduler.close().await;
        }
    });
}
