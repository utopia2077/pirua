#[path = "../../pi-durable-session/tests/support/mod.rs"]
mod session_support;
mod support;
use pi_durable::*;
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use support::*;

async fn running_task(session: &Session) -> TaskId {
    let definition = TaskDefinition::new("close", 1, |_| Ok(json!({"phase":"run","n":0}))).unwrap();
    let id = session
        .commit(move |tx| {
            tx.ensure_root_conversation()?;
            create_task(tx, &definition, json!(null), task_options())
        })
        .await
        .unwrap();
    session
        .commit(move |tx| {
            let mut record = tx.task(id)?.unwrap();
            record.state = TaskState::Running {
                checkpoint: json!({"phase":"run","n":0}),
            };
            tx.replace_task(record)
        })
        .await
        .unwrap();
    id
}
#[test]
fn close_seals_retained_session_during_join_and_drains_admitted_publication_only() {
    runtime().block_on(async {
        for (name, store, temp) in stores() {
            let probe = session_support::Probe::new(store);
            let session = Session::open(probe.clone()).unwrap();
            let id = running_task(&session).await;
            let document = session::DocDefinition::new(
                "close-watch",
                1,
                session::DocumentSemantics::Session,
                false,
                |_| Ok(json!({})),
                |_, _, _| Ok(false),
            )
            .unwrap();
            let address = document.address(session::Scope::Session, None).unwrap();
            let d = document.clone();
            let a = address.clone();
            session
                .commit(move |tx| tx.doc(&d, a, json!(null)))
                .await
                .unwrap();
            let watch = session.watch_doc(document, address).await.unwrap().unwrap();
            let mut publications = session.subscribe_commits().await.unwrap();
            let backend_gate = probe.hold();
            let s = session.clone();
            let admitted = tokio::spawn(async move {
                s.commit(move |tx| {
                    let mut record = tx.task(id)?.unwrap();
                    record.state = TaskState::Running {
                        checkpoint: json!({"phase":"run","n":1}),
                    };
                    tx.replace_task(record)
                })
                .await
            });
            let entered = backend_gate.clone();
            tokio::task::spawn_blocking(move || entered.wait_entered())
                .await
                .unwrap();
            let join_release = tokio::sync::watch::channel(false).0;
            let mut join_gate = join_release.subscribe();
            let s = session.clone();
            let close_waiter = tokio::spawn(async move {
                s.close_with(
                    async move {
                        if !*join_gate.borrow() {
                            join_gate.changed().await.unwrap();
                        }
                    },
                    move |tx| {
                        let mut record = tx.task(id)?.unwrap();
                        if let TaskState::Running { checkpoint } = record.state {
                            record.state = TaskState::Pending { checkpoint };
                            tx.replace_task(record)?;
                        }
                        Ok(())
                    },
                )
                .await
            });
            assert_eq!(watch.closed().await, session::WatchEnd::SessionClosed);
            let calls = Arc::new(AtomicU64::new(0));
            let capture = calls.clone();
            let rejected = session
                .commit(move |_| {
                    capture.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
                .await
                .unwrap_err();
            assert_eq!(rejected.kind, ErrorKind::Closed);
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            backend_gate.release();
            admitted.await.unwrap().unwrap();
            let publication = publications.next().await.unwrap();
            assert_eq!(publication.changes.len(), 1);
            close_waiter.abort();
            join_release.send_replace(true);
            assert_eq!(session.closed().await, session::SessionEnd::Closed);
            assert!(
                publications.next().await.is_none(),
                "{name}: internal close reset must not publish"
            );
            if let Some(temp) = temp {
                let reopened = session::SqliteStorage::open(&temp.0).unwrap();
                use session::Storage;
                let record = reopened.task(id).unwrap().unwrap();
                assert!(
                    matches!(record.state,TaskState::Pending{ref checkpoint} if checkpoint["n"]==1)
                );
                assert_eq!(record.restarts, None);
                reopened.close().unwrap();
            }
        }
    });
}

#[test]
fn close_finalizer_failure_before_and_after_backend_admission_latches_without_publication() {
    runtime().block_on(async {
        for mode in 0..=2 {
            for (name, store, temp) in stores() {
                let probe = session_support::Probe::new(store);
                let session = Session::open(probe.clone()).unwrap();
                let id = running_task(&session).await;
                let mut publications = session.subscribe_commits().await.unwrap();
                if mode > 0 {
                    probe.fail(mode);
                }
                let result = session
                    .close_with(std::future::ready(()), move |tx| {
                        let mut record = tx.task(id)?.unwrap();
                        record.state = TaskState::Pending {
                            checkpoint: json!({"phase":"run","n":7}),
                        };
                        tx.replace_task(record)?;
                        if mode == 0 {
                            Err(Error::validation("close callback rollback"))
                        } else {
                            Ok(())
                        }
                    })
                    .await;
                assert!(result.is_err(), "{name} mode{mode}");
                assert!(session.failure().is_some());
                assert!(publications.next().await.is_none());
                if let Some(temp) = temp {
                    use session::Storage;
                    let reopened = session::SqliteStorage::open(&temp.0).unwrap();
                    let task = reopened.task(id).unwrap().unwrap();
                    assert_eq!(
                        task.state.status(),
                        if mode == 2 { "pending" } else { "running" }
                    );
                    reopened.close().unwrap();
                }
            }
        }
    });
}

#[test]
fn failed_session_skips_close_finalizer_and_first_close_owns_cleanup() {
    runtime().block_on(async {
        for (_, store, _temp) in stores() {
            let probe = session_support::Probe::new(store);
            let session = Session::open(probe.clone()).unwrap();
            running_task(&session).await;
            probe.fail(1);
            assert!(
                session
                    .commit(|tx| tx.create_conversation(session::ConversationOwnership::Ownerless))
                    .await
                    .is_err()
            );
            let called = Arc::new(AtomicU64::new(0));
            let count = called.clone();
            assert!(
                session
                    .close_with(std::future::ready(()), move |_| {
                        count.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    })
                    .await
                    .is_err()
            );
            assert_eq!(called.load(Ordering::SeqCst), 0);
        }
        for (_, store, _temp) in stores() {
            let session = Session::open(store).unwrap();
            let release = tokio::sync::watch::channel(false).0;
            let mut rx = release.subscribe();
            let called = Arc::new(AtomicU64::new(0));
            let first = called.clone();
            let s = session.clone();
            let first_close = tokio::spawn(async move {
                s.close_with(
                    async move {
                        if !*rx.borrow() {
                            rx.changed().await.unwrap();
                        }
                    },
                    move |_| {
                        first.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    },
                )
                .await
            });
            while session.commit(|_| Ok(())).await.is_ok() {
                tokio::task::yield_now().await;
            }
            let other = called.clone();
            let s = session.clone();
            let second_close = tokio::spawn(async move {
                s.close_with(std::future::ready(()), move |_| {
                    other.fetch_add(100, Ordering::SeqCst);
                    Ok(())
                })
                .await
            });
            release.send_replace(true);
            first_close.await.unwrap().unwrap();
            second_close.await.unwrap().unwrap();
            assert_eq!(called.load(Ordering::SeqCst), 1);
        }
    });
}

#[test]
fn scheduler_close_waiter_cancellation_does_not_cancel_join_or_pending_reset() {
    runtime().block_on(async {
        for (_, store, temp) in stores() {
            let registry = Registry::new();
            let entered = tokio::sync::watch::channel(false).0;
            let observed = entered.clone();
            let joined = tokio::sync::watch::channel(false).0;
            let join_signal = joined.clone();
            let release = tokio::sync::watch::channel(false).0;
            let gate = release.clone();
            let definition = TaskDefinition::new("close-gate", 1, |_| Ok(json!({"phase":"run"})))
                .unwrap()
                .phase("run", move |_, inv| {
                    let entered = observed.clone();
                    let joined = join_signal.clone();
                    let mut gate = gate.subscribe();
                    async move {
                        entered.send_replace(true);
                        inv.signal().cancelled().await;
                        joined.send_replace(true);
                        if !*gate.borrow() {
                            gate.changed().await.unwrap();
                        }
                        assert!(
                            inv.commit(|_, _| Ok(Some(complete(json!("stale")))))
                                .await
                                .is_err()
                        );
                        Ok(())
                    }
                });
            registry
                .install("close-gate", vec![definition.clone()])
                .unwrap();
            let session = Session::open(store).unwrap();
            session
                .commit(|tx| tx.ensure_root_conversation())
                .await
                .unwrap();
            let scheduler = Scheduler::open(session.clone(), registry, Default::default())
                .await
                .unwrap();
            let id = scheduler
                .create_task(&definition, json!(null), task_options())
                .await
                .unwrap();
            scheduler.resume();
            let mut entry = entered.subscribe();
            if !*entry.borrow() {
                entry.changed().await.unwrap();
            }
            let sc = scheduler.clone();
            let waiter = tokio::spawn(async move { sc.close().await });
            let mut join = joined.subscribe();
            if !*join.borrow() {
                join.changed().await.unwrap();
            }
            assert_eq!(
                session.commit(|_| Ok(())).await.unwrap_err().kind,
                ErrorKind::Closed
            );
            waiter.abort();
            release.send_replace(true);
            close(&scheduler).await;
            if let Some(temp) = temp {
                let reopened = Arc::new(session::SqliteStorage::open(&temp.0).unwrap());
                let session = Session::open(reopened.clone()).unwrap();
                let scheduler = Scheduler::open(session, Registry::new(), Default::default())
                    .await
                    .unwrap();
                let task = scheduler.get_task(id).await.unwrap().unwrap();
                assert_eq!(task.state.status(), "pending");
                assert_eq!(task.restarts, None);
                close(&scheduler).await;
            }
        }
    });
}
