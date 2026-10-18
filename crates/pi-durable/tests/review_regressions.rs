//! Counterexamples from the independent scheduler review. Expected receipts come
//! from the pinned source contract; reviewer probes and original logs stay untouched.
#[path = "../../pi-durable-session/tests/support/mod.rs"]
mod session_support;
mod support;
use pi_durable::*;
use serde_json::json;
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use support::*;

async fn released(sender: &tokio::sync::watch::Sender<bool>) {
    let mut receiver = sender.subscribe();
    if !*receiver.borrow() {
        receiver.changed().await.unwrap();
    }
}

#[test]
fn abort_joins_normally_but_rejects_session_failure_before_handler_release() {
    runtime().block_on(async {
        let mut failures = Vec::new();
        for fail_session in [false, true] {
            for (name, store, _temp) in stores() {
                let probe = session_support::Probe::new(store.clone());
                let session = Session::open(probe.clone()).unwrap();
                session
                    .commit(|tx| tx.ensure_root_conversation())
                    .await
                    .unwrap();
                let entered = tokio::sync::watch::channel(false).0;
                let signalled = tokio::sync::watch::channel(false).0;
                let release = tokio::sync::watch::channel(false).0;
                let e = entered.clone();
                let s = signalled.clone();
                let r = release.clone();
                let definition =
                    TaskDefinition::new("review.abort-failure", 1, |_| Ok(json!({"phase":"run"})))
                        .unwrap()
                        .phase("run", move |record, invocation| {
                            let entered = e.clone();
                            let signalled = s.clone();
                            let release = r.clone();
                            let store = store.clone();
                            async move {
                                entered.send_replace(true);
                                invocation.signal().cancelled().await;
                                assert!(
                                    store.task(record.id)?.unwrap().abort_requested,
                                    "signal must follow committed abort intent"
                                );
                                assert!(
                                    invocation
                                        .commit(|_, _| Ok(Some(complete(json!("stale")))))
                                        .await
                                        .is_err()
                                );
                                signalled.send_replace(true);
                                released(&release).await;
                                Ok(())
                            }
                        });
                let registry = Registry::new();
                registry.install("work", vec![definition.clone()]).unwrap();
                let scheduler = Scheduler::open(session.clone(), registry, Default::default())
                    .await
                    .unwrap();
                let id = scheduler
                    .create_task(&definition, json!(null), task_options())
                    .await
                    .unwrap();
                scheduler.resume();
                released(&entered).await;
                let sc = scheduler.clone();
                let mut abort = tokio::spawn(async move { sc.abort_task(id).await });
                released(&signalled).await;
                let backend_error = if fail_session {
                    probe.fail(1);
                    let error = session
                        .commit(|tx| {
                            tx.append_entry(
                                session::ROOT_CONVERSATION_ID,
                                session::EntryDraft::new("fail"),
                            )
                        })
                        .await
                        .unwrap_err();
                    assert_eq!(
                        session.closed().await,
                        session::SessionEnd::Failed(error.clone())
                    );
                    Some(error)
                } else {
                    None
                };
                let early = tokio::time::timeout(Duration::from_millis(250), &mut abort).await;
                let early = early.ok().map(|result| result.unwrap());
                let settled_before_release = early.is_some();
                println!(
                    "{name} failed={fail_session}: abort settled before handler release={}",
                    early.is_some()
                );
                release.send_replace(true);
                let result = match early {
                    Some(result) => Some(result),
                    None => tokio::time::timeout(Duration::from_secs(2), abort)
                        .await
                        .ok()
                        .map(|result| result.unwrap()),
                };
                if let Some(error) = backend_error {
                    // Check settlement before release separately from the eventual join.
                    if !matches!(&result, Some(Err(actual)) if actual == &error) {
                        failures.push(format!(
                            "{name}: abort did not return the actual Session failure"
                        ));
                    }
                } else {
                    assert!(matches!(result, Some(Ok(AbortDisposition::Marked))));
                }
                let _ = scheduler.close().await;
                // A normal abort intentionally waits for a handler which ignores its signal.
                // Failure must settle the wait without requiring that handler release.
                // The first assertion is kept after cleanup so red runs leave no gated work.
                if fail_session && !settled_before_release {
                    failures.push(format!(
                        "{name}: abort remained pending after Session failure"
                    ));
                }
                if !fail_session {
                    assert!(
                        !settled_before_release,
                        "normal abort skipped its handler join"
                    );
                }
            }
        }
        assert!(failures.is_empty(), "{}", failures.join("; "));
    });
}

fn snapshot_signature(invocation: &Invocation) -> Vec<(String, u32)> {
    let mut signature: Vec<_> = invocation
        .registry()
        .tasks()
        .into_iter()
        .map(|task| (task.kind().to_string(), task.version()))
        .collect();
    signature.sort();
    signature
}

#[test]
fn unavailable_phase_definitions_report_once_per_resolution_and_keep_old_execution() {
    runtime().block_on(async {
        let mut missing = Vec::new();
        for (name, store, _temp) in stores() {
            let entered = tokio::sync::watch::channel(0_u64).0;
            let gates: Vec<_> = (0..6)
                .map(|_| tokio::sync::watch::channel(false).0)
                .collect();
            let seen = Arc::new(Mutex::new(Vec::new()));
            let snapshots = seen.clone();
            let task_entered = entered.clone();
            let phase_gates = gates.clone();
            let definition =
                TaskDefinition::new("review.outdated", 1, |_| Ok(json!({"phase":"run","n":0})))
                    .unwrap()
                    .phase("run", move |record, invocation| {
                        let entered = task_entered.clone();
                        let gates = phase_gates.clone();
                        let seen = snapshots.clone();
                        async move {
                            let TaskState::Running { checkpoint } = record.state else {
                                unreachable!()
                            };
                            let n = checkpoint["n"].as_u64().unwrap();
                            let before = snapshot_signature(&invocation);
                            seen.lock().unwrap().push(before.clone());
                            entered.send_replace(n + 1);
                            if n < 6 {
                                released(&gates[n as usize]).await;
                                assert_eq!(
                                    snapshot_signature(&invocation),
                                    before,
                                    "a phase snapshot changed during its handler"
                                );
                            }
                            invocation
                                .commit(move |_, _| {
                                    Ok(Some(if n == 6 {
                                        complete(json!("old definition"))
                                    } else {
                                        TaskState::Running {
                                            checkpoint: json!({"phase":"run","n":n+1}),
                                        }
                                    }))
                                })
                                .await
                        }
                    });
            let replacement_calls = Arc::new(AtomicU64::new(0));
            let make_replacement = || {
                let calls = replacement_calls.clone();
                TaskDefinition::new("review.outdated", 2, |_| Ok(json!({"phase":"run"})))
                    .unwrap()
                    .phase("run", move |_, _| {
                        calls.fetch_add(1, Ordering::SeqCst);
                        async {
                            Err(Error::request(
                                "incompatible definition must never dispatch",
                            ))
                        }
                    })
            };
            let replacement_a = make_replacement();
            let replacement_b = make_replacement();
            let marker_a =
                TaskDefinition::new("review.marker-a", 1, |_| Ok(json!({"phase":"run"}))).unwrap();
            let marker_b =
                TaskDefinition::new("review.marker-b", 1, |_| Ok(json!({"phase":"run"}))).unwrap();
            let registry = Registry::new();
            registry.install("work", vec![definition.clone()]).unwrap();
            let reports = Arc::new(Mutex::new(Vec::new()));
            let capture = reports.clone();
            let scheduler = scheduler(
                store,
                registry.clone(),
                SchedulerOptions {
                    on_report: Some(Arc::new(move |report| capture.lock().unwrap().push(report))),
                    ..Default::default()
                },
            )
            .await;
            let id = scheduler
                .create_task(&definition, json!(null), task_options())
                .await
                .unwrap();
            scheduler.resume();
            let mut current = entered.subscribe();
            for n in 0..6 {
                while *current.borrow_and_update() < n + 1 {
                    current.changed().await.unwrap();
                }
                match n {
                    0 => registry.uninstall("work"),
                    2 => registry
                        .install("work", vec![replacement_a.clone(), marker_a.clone()])
                        .unwrap(),
                    4 => registry
                        .install("work", vec![replacement_b.clone(), marker_b.clone()])
                        .unwrap(),
                    _ => {}
                }
                gates[n as usize].send_replace(true);
            }
            assert_eq!(
                wait(&scheduler, id).await.state,
                complete(json!("old definition"))
            );
            assert_eq!(replacement_calls.load(Ordering::SeqCst), 0);
            let snapshots = seen.lock().unwrap().clone();
            assert_eq!(snapshots.len(), 7);
            assert_eq!(snapshots[0], vec![("review.outdated".into(), 1)]);
            assert!(snapshots[1].is_empty());
            assert!(snapshots[2].is_empty());
            assert_eq!(
                snapshots[3],
                vec![("review.marker-a".into(), 1), ("review.outdated".into(), 2)]
            );
            assert_eq!(snapshots[4], snapshots[3]);
            assert_eq!(
                snapshots[5],
                vec![("review.marker-b".into(), 1), ("review.outdated".into(), 2)]
            );
            assert_eq!(snapshots[6], snapshots[5]);
            let reports = reports
                .lock()
                .unwrap()
                .iter()
                .map(|report| (report.kind, report.task_id, report.error.message.clone()))
                .collect::<Vec<_>>();
            println!("{name}: outdated reports={reports:?}");
            if reports
                != vec![
                    (
                        "task_outdated",
                        Some(id),
                        format!("Task {id} keeps running under its old review.outdated definition")
                    );
                    3
                ]
            {
                missing.push(name);
            }
            close(&scheduler).await;
        }
        assert!(
            missing.is_empty(),
            "once-per-resolution outdated reports missing: {missing:?}"
        );
    });
}

#[test]
fn exhausted_abort_guard_keeps_exact_receipt_and_reports_after_terminal_commit() {
    runtime().block_on(async {
        let mut mismatches = Vec::new();
        for (name, store, _temp) in stores() {
            let session = Session::with_clock(store.clone(), || 1000.0).unwrap();
            let calls = Arc::new(AtomicU64::new(0));
            let count = calls.clone();
            let definition = TaskDefinition::new("review.guard", 1, |_| Ok(json!({"phase":"run"})))
                .unwrap()
                .abort(move |_, _| {
                    count.fetch_add(1, Ordering::SeqCst);
                    async { Err(Error::request("exhausted abort must not dispatch")) }
                });
            let create = definition.clone();
            let id = session
                .commit(move |tx| {
                    tx.ensure_root_conversation()?;
                    create_task(tx, &create, json!(null), task_options())
                })
                .await
                .unwrap();
            session
                .commit(move |tx| {
                    let mut task = tx.task(id)?.unwrap();
                    task.state = TaskState::Running {
                        checkpoint: json!({"phase":"run"}),
                    };
                    task.abort_requested = true;
                    task.restarts = Some(1);
                    tx.replace_task(task)
                })
                .await
                .unwrap();
            let reports = Arc::new(Mutex::new(Vec::new()));
            let capture = reports.clone();
            let reported = tokio::sync::watch::channel(false).0;
            let signal = reported.clone();
            let raw = store.clone();
            let registry = Registry::new();
            registry.install("work", vec![definition]).unwrap();
            let scheduler = Scheduler::open(
                session,
                registry,
                SchedulerOptions {
                    restart_limit: 2,
                    now: Arc::new(|| 1000.0),
                    on_report: Some(Arc::new(move |report| {
                        let persisted = raw.task(id).unwrap().unwrap();
                        capture.lock().unwrap().push((report, persisted));
                        signal.send_replace(true);
                    })),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            let receipt = wait(&scheduler, id).await;
            let _ = tokio::time::timeout(Duration::from_millis(250), released(&reported)).await;
            let message = format!("Abort handler of task {id} (review.guard) was cut off 2 times");
            let expected = TaskState::Terminal {
                outcome: TaskOutcome::Faulted {
                    error: TaskOutcomeError {
                        message: message.clone(),
                        detail: Some(json!({"reason":"restart_limit"})),
                    },
                },
            };
            let actual_reports = reports
                .lock()
                .unwrap()
                .iter()
                .map(|(report, persisted)| {
                    (
                        report.kind,
                        report.task_id,
                        report.error.message.clone(),
                        persisted.state.clone(),
                    )
                })
                .collect::<Vec<_>>();
            println!(
                "{name}: abort guard receipt={:?}; reports={actual_reports:?}",
                receipt.state
            );
            if receipt.state != expected
                || actual_reports != vec![("task_faulted", Some(id), message, expected)]
            {
                mismatches.push(name);
            }
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            close(&scheduler).await;
        }
        assert!(
            mismatches.is_empty(),
            "abort guard receipt/report mismatch: {mismatches:?}"
        );
    });
}

#[test]
fn exhausted_abort_guard_does_not_report_an_unsuccessful_backend_commit() {
    runtime().block_on(async {
        for mode in [1, 2] { for (name, store, _temp) in stores() {
            let probe=session_support::Probe::new(store);
            let session=Session::open(probe.clone()).unwrap();
            let definition=TaskDefinition::new("review.guard-failure",1, |_|Ok(json!({"phase":"run"}))).unwrap();
            let create=definition.clone();
            let id=session.commit(move|tx|{tx.ensure_root_conversation()?;create_task(tx,&create,json!(null),task_options())}).await.unwrap();
            session.commit(move|tx|{let mut task=tx.task(id)?.unwrap();task.state=TaskState::Running{checkpoint:json!({"phase":"run"})};task.abort_requested=true;task.restarts=Some(1);tx.replace_task(task)}).await.unwrap();
            let reports=Arc::new(Mutex::new(Vec::new()));let captured=reports.clone();
            let registry=Registry::new();registry.install("work",vec![definition]).unwrap();
            let scheduler=Scheduler::open(session.clone(),registry,SchedulerOptions{restart_limit:2,on_report:Some(Arc::new(move|report|captured.lock().unwrap().push(report))),..Default::default()}).await.unwrap();
            probe.fail(mode);
            assert!(tokio::time::timeout(Duration::from_secs(3),scheduler.wait_for_task(id)).await.unwrap().is_err());
            assert!(matches!(session.closed().await,session::SessionEnd::Failed(_)));
            let _=scheduler.close().await;
            assert!(!reports.lock().unwrap().iter().any(|report|report.kind=="task_faulted"),"{name} mode{mode}: failed/ambiguous commit cannot emit a successful guard report");
        }}
    });
}
