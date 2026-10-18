mod support;
use pi_durable_session::*;
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use support::*;
fn definition() -> DocDefinition {
    DocDefinition::new(
        "counter",
        1,
        DocumentSemantics::Session,
        false,
        |_| Ok(json!({"count":0,"items":[1,2],"text":"😀x"})),
        |_, _, _| Ok(false),
    )
    .unwrap()
}
fn address() -> DocumentAddress {
    DocumentAddress {
        kind: "counter".into(),
        scope: Scope::Session,
        key: None,
    }
}
async fn create(session: &Session) -> DocumentId {
    session
        .commit(|tx| tx.doc(&definition(), address(), Value::Null))
        .await
        .unwrap()
}
async fn set(session: &Session, n: u64) {
    session
        .commit(move |tx| {
            let id = tx.doc(&definition(), address(), Value::Null)?;
            tx.mutate(id, Op::Set(vec!["count".into()], json!(n)))
        })
        .await
        .unwrap();
}
#[test]
fn safe_ids_presence_and_decoded_domain() {
    assert!(ConversationId::new(0).is_err());
    assert!(Seq::new(0).is_ok());
    assert!(TaskId::new(MAX_SAFE_INTEGER + 1).is_err());
    let entry: EntryRecord =
        serde_json::from_value(json!({"id":2,"conversationId":1,"kind":"x","data":null})).unwrap();
    assert_eq!(entry.data, Some(Value::Null));
    assert_eq!(serde_json::to_value(entry).unwrap()["data"], Value::Null);
    let missing: EntryRecord =
        serde_json::from_value(json!({"id":2,"conversationId":1,"kind":"x"})).unwrap();
    assert_eq!(missing.data, None);
    let base = json!({"text":"😀x","xs":[0]});
    assert!(apply(&base, &[Op::Trim(vec!["text".into()], 1)]).is_err());
    assert_eq!(base["text"], "😀x");
    assert_eq!(
        apply(&base, &[Op::Trim(vec!["text".into()], 2)]).unwrap()["text"],
        "x"
    );
    for raw in [
        json!(["s", ["xs", 4], true]),
        json!(["s", ["xs", "0"], true]),
        json!(["s", ["__proto__"], true]),
        json!(["m", ["xs"], [1]]),
    ] {
        let result = serde_json::from_value::<Op>(raw)
            .map_err(Error::from)
            .and_then(|op| apply(&base, &[op]));
        assert!(result.is_err());
    }
}
#[test]
fn backend_atomic_global_ids_and_content_before_retirement() {
    for (name, store, _) in backends() {
        let id = store.mint_id().unwrap();
        assert_eq!(id, 2, "{name}");
        let r:DocumentCreate=serde_json::from_value(json!({"id":2,"kind":"r","scope":{"kind":"conversation","conversationId":1},"history":"rewindable","fork":"asOf"})).unwrap();
        store
            .commit(&[StorageWrite::DocumentCreate {
                record: r,
                content: DocumentContent::Base {
                    version: 1,
                    value: json!({"x":0}),
                },
            }])
            .unwrap();
        let bad = [
            StorageWrite::Conversation {
                value: ConversationRecord {
                    id: ConversationId::new(3).unwrap(),
                    parent: None,
                    owner: None,
                },
            },
            StorageWrite::DocumentChange {
                id: DocumentId::new(2).unwrap(),
                content: DocumentContent::Delta {
                    version: 2,
                    ops: vec![Op::Set(vec!["x".into()], json!(1))],
                },
            },
            StorageWrite::DocumentRetire {
                id: DocumentId::new(2).unwrap(),
            },
        ];
        assert!(store.commit(&bad).is_err(), "{name}");
        assert!(
            store
                .conversation(ConversationId::new(3).unwrap())
                .unwrap()
                .is_none()
        );
        assert_eq!(
            store
                .document(DocumentId::new(2).unwrap(), DocumentPoint::Current)
                .unwrap()
                .unwrap()
                .value,
            json!({"x":0})
        );
        assert!(
            store
                .commit(&[StorageWrite::Conversation {
                    value: ConversationRecord {
                        id: ConversationId::new(2).unwrap(),
                        parent: None,
                        owner: None
                    }
                }])
                .is_err()
        );
        store.close().unwrap();
        assert!(store.mint_id().is_err());
        assert!(store.scan_tasks(&TaskQuery::default(), 1, None).is_err());
    }
}
#[test]
fn sqlite_allocation_only_reopen_and_schema_rejection() {
    let temp = Temp::new();
    let store = SqliteStorage::open(&temp.path).unwrap();
    let first = store.mint_id().unwrap();
    store.close().unwrap();
    let store = SqliteStorage::open(&temp.path).unwrap();
    assert!(store.mint_id().unwrap() > first);
    store.close().unwrap();
    let db = pi_sqlite::Database::open(&temp.path).unwrap();
    db.read::<_, pi_sqlite::Error>(|c| {
        c.execute(
            "UPDATE pirua_schema SET version=2 WHERE component='durable-session'",
            [],
        )?;
        Ok(())
    })
    .unwrap();
    assert!(SqliteStorage::from_database(db).is_err());
}
#[test]
fn preparation_rollback_noop_structural_and_checkpoint() {
    runtime().block_on(async {
        for (name, storage, _) in backends() {
            let probe = Probe::new(storage);
            let session = Session::open(probe.clone()).unwrap();
            create(&session).await;
            let before = session
                .snapshot(definition(), address())
                .await
                .unwrap()
                .unwrap();
            let count = probe.writes.lock().unwrap().len();
            session
                .commit(|tx| {
                    let id = tx.doc(&definition(), address(), Value::Null)?;
                    tx.mutate(id, Op::Set(vec!["count".into()], json!(0)))?;
                    tx.mutate(id, Op::Splice(vec!["items".into()], 2, 0, vec![json!(3)]))?;
                    tx.mutate(id, Op::Splice(vec!["items".into()], 2, 1, vec![]))
                })
                .await
                .unwrap();
            assert_eq!(probe.writes.lock().unwrap().len(), count, "{name}");
            session
                .commit(|tx| {
                    let id = tx.doc(&definition(), address(), Value::Null)?;
                    tx.mutate(id, Op::Splice(vec!["items".into()], 0, 1, vec![]))?;
                    tx.mutate(id, Op::Splice(vec!["items".into()], 0, 0, vec![json!(1)]))
                })
                .await
                .unwrap();
            assert_eq!(probe.writes.lock().unwrap().len(), count + 1);
            assert_eq!(
                session
                    .snapshot(definition(), address())
                    .await
                    .unwrap()
                    .unwrap(),
                before
            );
            let failing = DocDefinition::new(
                "counter",
                1,
                DocumentSemantics::Session,
                false,
                |_| unreachable!(),
                |_, _, _| Err(Error::validation("checkpoint rejected")),
            )
            .unwrap();
            assert!(
                session
                    .commit(move |tx| {
                        let id = tx.doc(&failing, address(), Value::Null)?;
                        tx.apply(id, Op::Set(vec!["count".into()], json!(3)))
                    })
                    .await
                    .is_err()
            );
            assert_eq!(probe.writes.lock().unwrap().len(), count + 1);
            assert!(session.failure().is_none());
            set(&session, 4).await;
            assert_eq!(before["count"], 0);
            assert!(
                session
                    .commit(|tx| {
                        tx.create_conversation(ConversationOwnership::Ownerless)?;
                        let id = tx.doc(&definition(), address(), Value::Null)?;
                        tx.apply(
                            id,
                            Op::Set(vec!["items".into(), 9usize.into()], json!(true)),
                        )
                    })
                    .await
                    .is_err()
            );
            assert!(session.failure().is_none());
            assert_eq!(
                session
                    .snapshot(definition(), address())
                    .await
                    .unwrap()
                    .unwrap()["count"],
                4
            );
            session.close().await.unwrap();
        }
    });
}
#[test]
fn table_read_boundary_and_immutable_records() {
    runtime().block_on(async {
        for (_, storage, _) in backends() {
            let session = Session::open(storage).unwrap();
            let c = session
                .commit(|tx| tx.create_conversation(ConversationOwnership::Ownerless))
                .await
                .unwrap();
            assert!(
                session
                    .commit(move |tx| {
                        tx.append_entry(c.id, EntryDraft::new("lost"))?;
                        tx.conversation(c.id)
                    })
                    .await
                    .is_err()
            );
            assert!(session.history(c.id, None).await.unwrap().is_empty());
            let e = session
                .commit(move |tx| tx.append_entry(c.id, EntryDraft::new("kept")))
                .await
                .unwrap();
            assert_eq!(session.history(c.id, None).await.unwrap(), vec![e]);
            assert!(
                session
                    .conversation(ConversationId::new(999).unwrap())
                    .await
                    .unwrap()
                    .is_none()
            );
            session.close().await.unwrap();
        }
    });
}
#[test]
fn commit_failure_latches_and_ambiguous_sqlite_reopen() {
    runtime().block_on(async {
        for mode in [1, 2] {
            let temp = Temp::new();
            let storage = Arc::new(SqliteStorage::open(&temp.path).unwrap());
            let probe = Probe::new(storage);
            let session = Session::open(probe.clone()).unwrap();
            create(&session).await;
            let state = session
                .document_state(definition(), address())
                .await
                .unwrap()
                .unwrap();
            let mut commits = session.subscribe_commits().await.unwrap();
            probe.fail(mode);
            assert!(
                session
                    .commit(|tx| {
                        let id = tx.doc(&definition(), address(), Value::Null)?;
                        tx.apply(id, Op::Set(vec!["count".into()], json!(8)))
                    })
                    .await
                    .is_err()
            );
            assert_eq!(state.value()["count"], 0);
            assert!(commits.next().await.is_none());
            assert!(session.commit(|_| Ok(())).await.is_err());
            assert!(matches!(session.closed().await, SessionEnd::Failed(_)));
            let reopened =
                Session::open(Arc::new(SqliteStorage::open(&temp.path).unwrap())).unwrap();
            assert_eq!(
                reopened
                    .snapshot(definition(), address())
                    .await
                    .unwrap()
                    .unwrap()["count"],
                if mode == 2 { 8 } else { 0 }
            );
            reopened.close().await.unwrap();
        }
    });
}
#[test]
fn allocation_and_read_failures_are_fatal_but_request_errors_are_not() {
    runtime().block_on(async {
        for mode in [3, 4] {
            for (_, storage, _) in backends() {
                let probe = Probe::new(storage);
                let session = Session::open(probe.clone()).unwrap();
                assert_eq!(
                    session
                        .history(ConversationId::new(999).unwrap(), None)
                        .await
                        .unwrap_err()
                        .kind,
                    ErrorKind::Request
                );
                assert!(session.failure().is_none());
                probe.fail(mode);
                if mode == 3 {
                    assert!(
                        session
                            .commit(|tx| tx.create_conversation(ConversationOwnership::Ownerless))
                            .await
                            .is_err()
                    );
                } else {
                    assert!(session.conversation(ROOT_CONVERSATION_ID).await.is_err());
                }
                assert!(session.failure().is_some());
            }
        }
    });
}
#[test]
fn caller_cancellation_and_close_settle_admitted_commit() {
    runtime().block_on(async {
        for (_, storage, _) in backends() {
            let probe = Probe::new(storage);
            let session = Session::open(probe.clone()).unwrap();
            create(&session).await;
            let gate = probe.hold();
            let s = session.clone();
            let caller = tokio::spawn(async move { set(&s, 9).await });
            let g = gate.clone();
            tokio::task::spawn_blocking(move || g.wait_entered())
                .await
                .unwrap();
            caller.abort();
            let state_before_close = session.clone();
            let close = tokio::spawn(async move { state_before_close.close().await });
            tokio::task::yield_now().await;
            gate.release();
            close.await.unwrap().unwrap();
            let writes = probe.writes.lock().unwrap();
            assert!(
                writes
                    .last()
                    .unwrap()
                    .iter()
                    .any(|w| matches!(w, StorageWrite::DocumentChange { .. }))
            );
        }
    });
}
#[test]
fn queued_waiter_cancellation_does_not_run_callback() {
    runtime().block_on(async {
        for (_, storage, _) in backends() {
            let probe = Probe::new(storage);
            let session = Session::open(probe.clone()).unwrap();
            create(&session).await;
            let gate = probe.hold();
            let s = session.clone();
            let first = tokio::spawn(async move { set(&s, 1).await });
            let g = gate.clone();
            tokio::task::spawn_blocking(move || g.wait_entered())
                .await
                .unwrap();
            let ran = Arc::new(AtomicUsize::new(0));
            let count = ran.clone();
            let s = session.clone();
            let second = tokio::spawn(async move {
                s.commit(move |_| {
                    count.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
                .await
            });
            tokio::task::yield_now().await;
            second.abort();
            let _ = second.await;
            gate.release();
            first.await.unwrap();
            assert_eq!(ran.load(Ordering::SeqCst), 0);
            session.close().await.unwrap();
        }
    });
}
#[test]
fn incarnation_observation_overflow_and_listener_isolation() {
    runtime().block_on(async {
        for (_, storage, _) in backends() {
            let session = Session::open(storage).unwrap();
            let old_id = create(&session).await;
            let state = session
                .document_state(definition(), address())
                .await
                .unwrap()
                .unwrap();
            let old = state.value();
            let watch = session
                .watch_doc(definition(), address())
                .await
                .unwrap()
                .unwrap();
            for n in 1..=101 {
                set(&session, n).await;
            }
            assert_eq!(watch.value()["count"], 0);
            let frame = watch.next().await.unwrap().unwrap();
            assert_eq!(frame.value["count"], 101);
            assert!(matches!(frame.ops.as_slice(), [Op::Replace(_)]));
            assert_eq!(state.value()["count"], 101);
            assert_eq!(old["count"], 0);
            let listener = session
                .watch_doc(definition(), address())
                .await
                .unwrap()
                .unwrap();
            listener
                .start(|_| async { Err(Error::validation("bad listener")) })
                .unwrap();
            set(&session, 102).await;
            assert!(matches!(
                listener.closed().await,
                WatchEnd::ListenerError(_)
            ));
            set(&session, 103).await;
            assert!(session.failure().is_none());
            let replacement = session
                .commit(|tx| {
                    tx.retire_doc(&definition(), address())?;
                    tx.doc(&definition(), address(), Value::Null)
                })
                .await
                .unwrap();
            assert_ne!(old_id, replacement);
            assert_eq!(state.value().as_ref(), &Value::Null);
            let _ = watch.next().await.unwrap();
            let _ = watch.next().await.unwrap();
            let retirement = watch.next().await.unwrap().unwrap();
            assert_eq!(retirement.value.as_ref(), &Value::Null);
            assert_eq!(watch.closed().await, WatchEnd::Retired);
            session.close().await.unwrap();
        }
    });
}
#[test]
fn observation_captures_atomic_multi_document_baseline_and_each_commit() {
    runtime().block_on(async {
        for (_, storage, _) in backends() {
            let session = Session::open(storage).unwrap();
            let c = session
                .commit(|tx| tx.create_conversation(ConversationOwnership::Ownerless))
                .await
                .unwrap();
            let mut observation = session
                .observe(ObservationQuery {
                    scopes: vec![Scope::Session],
                    addresses: vec![],
                    conversation: Some(c.id),
                })
                .await
                .unwrap();
            assert!(observation.baseline.documents.is_empty());
            session
                .commit(move |tx| {
                    let id = tx.doc(&definition(), address(), Value::Null)?;
                    tx.apply(id, Op::Set(vec!["count".into()], json!(1)))?;
                    tx.append_entry(c.id, EntryDraft::new("first"))?;
                    Ok(())
                })
                .await
                .unwrap();
            set(&session, 2).await;
            let first = observation.next().await.unwrap();
            let second = observation.next().await.unwrap();
            assert_eq!(first.snapshot.documents[&address()].value["count"], 1);
            assert_eq!(second.snapshot.documents[&address()].value["count"], 2);
            assert_eq!(first.snapshot.entries.len(), 1);
            assert!(observation.baseline.documents.is_empty());
            session.close().await.unwrap();
        }
    });
}

fn conversation_definition(kind: &str, history: History, fork: ForkPolicy) -> DocDefinition {
    DocDefinition::new(
        kind,
        1,
        DocumentSemantics::Conversation { history, fork },
        false,
        |_| Ok(json!({"count":0})),
        |_, _, _| Ok(false),
    )
    .unwrap()
}
fn conversation_address(kind: &str, id: ConversationId) -> DocumentAddress {
    DocumentAddress {
        kind: kind.into(),
        scope: Scope::Conversation {
            conversation_id: id,
        },
        key: None,
    }
}
#[test]
fn forks_use_entry_commit_final_state_and_definition_free_policies() {
    runtime().block_on(async {
        for (_, storage, _) in backends() {
            let session = Session::open(storage.clone()).unwrap();
            let parent = session
                .commit(|tx| tx.create_conversation(ConversationOwnership::Ownerless))
                .await
                .unwrap();
            let cutoff = session
                .commit(move |tx| {
                    for (kind, history, fork) in [
                        ("rewind", History::Rewindable, ForkPolicy::AsOf),
                        ("latest", History::Latest, ForkPolicy::Current),
                        ("initial", History::Rewindable, ForkPolicy::Initial),
                    ] {
                        let d = conversation_definition(kind, history, fork);
                        let id = tx.doc(&d, conversation_address(kind, parent.id), Value::Null)?;
                        tx.apply(id, Op::Set(vec!["count".into()], json!(1)))?;
                    }
                    let entry = tx.append_entry(parent.id, EntryDraft::new("cut"))?;
                    let d =
                        conversation_definition("rewind", History::Rewindable, ForkPolicy::AsOf);
                    let id = tx.doc(&d, conversation_address("rewind", parent.id), Value::Null)?;
                    tx.apply(id, Op::Set(vec!["count".into()], json!(2)))?;
                    Ok(entry)
                })
                .await
                .unwrap();
            session
                .commit(move |tx| {
                    for (kind, history, fork) in [
                        ("rewind", History::Rewindable, ForkPolicy::AsOf),
                        ("latest", History::Latest, ForkPolicy::Current),
                    ] {
                        let d = conversation_definition(kind, history, fork);
                        let id = tx.doc(&d, conversation_address(kind, parent.id), Value::Null)?;
                        tx.apply(id, Op::Set(vec!["count".into()], json!(3)))?;
                    }
                    tx.append_entry(parent.id, EntryDraft::new("excluded"))?;
                    Ok(())
                })
                .await
                .unwrap();
            let unknown_id = DocumentId::new(storage.mint_id().unwrap()).unwrap();
            storage
                .commit(&[StorageWrite::DocumentCreate {
                    record: DocumentCreate {
                        id: unknown_id,
                        kind: "uninstalled".into(),
                        scope: Scope::Conversation {
                            conversation_id: parent.id,
                        },
                        key: Some("x".into()),
                        history: Some(History::Latest),
                        fork: Some(ForkPolicy::Current),
                    },
                    content: DocumentContent::Base {
                        version: 7,
                        value: json!({"unknown":true}),
                    },
                }])
                .unwrap();
            let child = session
                .commit(move |tx| {
                    tx.fork_conversation(parent.id, cutoff.id, ConversationOwnership::Ownerless)
                })
                .await
                .unwrap();
            assert_eq!(
                session.history(child.id, None).await.unwrap(),
                vec![cutoff.clone()]
            );
            for (kind, history, fork, count) in [
                ("rewind", History::Rewindable, ForkPolicy::AsOf, 2),
                ("latest", History::Latest, ForkPolicy::Current, 3),
            ] {
                assert_eq!(
                    session
                        .snapshot(
                            conversation_definition(kind, history, fork),
                            conversation_address(kind, child.id)
                        )
                        .await
                        .unwrap()
                        .unwrap()["count"],
                    count
                );
            }
            assert!(
                session
                    .snapshot(
                        conversation_definition(
                            "initial",
                            History::Rewindable,
                            ForkPolicy::Initial
                        ),
                        conversation_address("initial", child.id)
                    )
                    .await
                    .unwrap()
                    .is_none()
            );
            let copied = storage
                .find_document(
                    &DocumentAddress {
                        kind: "uninstalled".into(),
                        scope: Scope::Conversation {
                            conversation_id: child.id,
                        },
                        key: Some("x".into()),
                    },
                    DocumentPoint::Current,
                )
                .unwrap()
                .unwrap();
            assert_ne!(copied.identity.id, unknown_id);
            assert_eq!(
                storage
                    .document(copied.identity.id, DocumentPoint::Current)
                    .unwrap()
                    .unwrap()
                    .version,
                7
            );
            let as_of = session
                .snapshot_as_of(
                    conversation_definition("rewind", History::Rewindable, ForkPolicy::AsOf),
                    conversation_address("rewind", child.id),
                    cutoff.id,
                )
                .await
                .unwrap()
                .unwrap();
            assert_eq!(as_of["count"], 2);
            assert!(
                session
                    .commit(move |tx| {
                        let d =
                            conversation_definition("latest", History::Latest, ForkPolicy::Current);
                        let id =
                            tx.doc(&d, conversation_address("latest", parent.id), Value::Null)?;
                        tx.apply(id, Op::Set(vec!["count".into()], json!(99)))?;
                        tx.fork_conversation(parent.id, cutoff.id, ConversationOwnership::Ownerless)
                    })
                    .await
                    .is_err()
            );
            assert!(session.failure().is_none());
            session.close().await.unwrap();
        }
    });
}
#[test]
fn incarnation_asof_membership_and_sqlite_reopen() {
    runtime().block_on(async {
        for (_, storage, temp) in backends() {
            let session = Session::open(storage.clone()).unwrap();
            let c = session
                .commit(|tx| tx.create_conversation(ConversationOwnership::Ownerless))
                .await
                .unwrap();
            let initial = session
                .commit(move |tx| {
                    let d = conversation_definition("r", History::Rewindable, ForkPolicy::AsOf);
                    let id = tx.doc(&d, conversation_address("r", c.id), Value::Null)?;
                    tx.apply(id, Op::Set(vec!["count".into()], json!(1)))?;
                    tx.append_entry(c.id, EntryDraft::new("before"))
                })
                .await
                .unwrap();
            let after = session
                .commit(move |tx| {
                    let d = conversation_definition("r", History::Rewindable, ForkPolicy::AsOf);
                    tx.retire_doc(&d, conversation_address("r", c.id))?;
                    let id = tx.doc(&d, conversation_address("r", c.id), Value::Null)?;
                    tx.apply(id, Op::Set(vec!["count".into()], json!(2)))?;
                    tx.append_entry(c.id, EntryDraft::new("after"))
                })
                .await
                .unwrap();
            assert_eq!(
                session
                    .snapshot_as_of(
                        conversation_definition("r", History::Rewindable, ForkPolicy::AsOf),
                        conversation_address("r", c.id),
                        initial.id
                    )
                    .await
                    .unwrap()
                    .unwrap()["count"],
                1
            );
            assert_eq!(
                session
                    .snapshot_as_of(
                        conversation_definition("r", History::Rewindable, ForkPolicy::AsOf),
                        conversation_address("r", c.id),
                        after.id
                    )
                    .await
                    .unwrap()
                    .unwrap()["count"],
                2
            );
            session.close().await.unwrap();
            if let Some(temp) = temp {
                let reopened =
                    Session::open(Arc::new(SqliteStorage::open(&temp.path).unwrap())).unwrap();
                assert_eq!(
                    reopened
                        .snapshot_as_of(
                            conversation_definition("r", History::Rewindable, ForkPolicy::AsOf),
                            conversation_address("r", c.id),
                            initial.id
                        )
                        .await
                        .unwrap()
                        .unwrap()["count"],
                    1
                );
                reopened.close().await.unwrap();
            }
        }
    });
}
#[test]
fn raw_scans_are_conjunctive_and_reject_foreign_cursors() {
    for (_, storage, _) in backends() {
        for (id, owner_c, owner_t) in [(2, 10, 20), (3, 10, 21), (4, 11, 20), (5, 10, 20)] {
            storage
                .commit(&[StorageWrite::Conversation {
                    value: ConversationRecord {
                        id: ConversationId::new(id).unwrap(),
                        parent: None,
                        owner: Some(ConversationOwner {
                            conversation_id: ConversationId::new(owner_c).unwrap(),
                            task_id: TaskId::new(owner_t).unwrap(),
                        }),
                    },
                }])
                .unwrap();
        }
        let q = ConversationQuery {
            owner_conversation_id: Some(ConversationId::new(10).unwrap()),
            owner_task_id: Some(TaskId::new(20).unwrap()),
            order: None,
        };
        let first = storage.scan_conversations(&q, 1, None).unwrap();
        assert_eq!(first.items[0].id.get(), 2);
        let second = storage
            .scan_conversations(&q, 1, first.next.as_ref())
            .unwrap();
        assert_eq!(second.items[0].id.get(), 5);
        assert!(second.next.is_none());
        let mut changed = q.clone();
        changed.order = Some(ScanOrder::Descending);
        assert_eq!(
            storage
                .scan_conversations(&changed, 1, first.next.as_ref())
                .unwrap_err()
                .kind,
            ErrorKind::Request
        );
        assert!(
            storage
                .scan_conversations(&ConversationQuery::default(), 1, first.next.as_ref())
                .is_err()
        );
        storage.close().unwrap();
    }
}
fn task_create(conversation_id: ConversationId) -> TaskCreate {
    TaskCreate {
        kind: "worker".into(),
        version: 1,
        input: Value::Null,
        checkpoint: json!({"phase":"start"}),
        conversation_id: Some(conversation_id),
        ownership: TaskOwnership::Conversation,
        background: false,
        abandon_on_restart: false,
        trace: Some(Value::Null),
        links: None,
    }
}
#[test]
fn root_task_lifecycle_attribution_ownership_and_document_retirement() {
    runtime().block_on(async {
        for (_, storage, _) in backends() {
            let clock = Arc::new(AtomicUsize::new(100));
            let now = clock.clone();
            let session =
                Session::with_clock(storage, move || now.fetch_add(1, Ordering::SeqCst) as f64)
                    .unwrap();
            let task = session
                .commit(|tx| {
                    let root = tx.ensure_root_conversation()?;
                    assert_eq!(root.id, ROOT_CONVERSATION_ID);
                    assert_eq!(tx.ensure_root_conversation()?, root);
                    tx.create_task(task_create(root.id))
                })
                .await
                .unwrap();
            let task_doc = DocDefinition::new(
                "output",
                1,
                DocumentSemantics::Task,
                false,
                |_| Ok(json!({"x":0})),
                |_, _, _| Ok(false),
            )
            .unwrap();
            let a = DocumentAddress {
                kind: "output".into(),
                scope: Scope::Task { task_id: task },
                key: None,
            };
            let d = task_doc.clone();
            let address = a.clone();
            session
                .commit(move |tx| {
                    let mut t = tx.task(task)?.unwrap();
                    t.state = TaskState::Running {
                        checkpoint: json!({"phase":"run"}),
                    };
                    tx.replace_task(t)?;
                    tx.doc(&d, address, Value::Null)
                })
                .await
                .unwrap();
            let state = session
                .document_state(task_doc.clone(), a.clone())
                .await
                .unwrap()
                .unwrap();
            let record = session.task(task).await.unwrap().unwrap();
            assert_eq!(record.started_at, Some(100.0));
            let entry = session
                .commit_scoped(
                    TransactionScope {
                        conversation_id: Some(ROOT_CONVERSATION_ID),
                        task_id: Some(task),
                    },
                    move |tx| tx.append_entry(ROOT_CONVERSATION_ID, EntryDraft::new("attributed")),
                )
                .await
                .unwrap();
            assert_eq!(entry.by_task_id, Some(task));
            assert!(
                session
                    .commit(move |tx| {
                        let mut t = tx.task(task)?.unwrap();
                        let mut child = task_create(ROOT_CONVERSATION_ID);
                        child.ownership = TaskOwnership::Task(task);
                        tx.create_task(child)?;
                        t.state = TaskState::Terminal {
                            outcome: TaskOutcome::Completed {
                                result: Value::Null,
                            },
                        };
                        tx.replace_task(t)
                    })
                    .await
                    .is_err()
            );
            assert!(session.failure().is_none());
            session
                .commit(move |tx| {
                    let mut t = tx.task(task)?.unwrap();
                    t.state = TaskState::Terminal {
                        outcome: TaskOutcome::Completed {
                            result: Value::Null,
                        },
                    };
                    tx.replace_task(t)
                })
                .await
                .unwrap();
            let record = session.task(task).await.unwrap().unwrap();
            assert_eq!(record.started_at, Some(100.0));
            assert!(record.ended_at.unwrap() >= 101.0);
            assert!(record.memos.is_none());
            assert!(state.value().is_null());
            assert!(
                session
                    .commit(move |tx| tx.doc(&task_doc, a, Value::Null))
                    .await
                    .is_err()
            );
            assert!(session.failure().is_none());
            session.close().await.unwrap();
        }
    });
}
#[test]
fn copy_frames_capture_values_before_future_mutations() {
    runtime().block_on(async {
        for (_, storage, _) in backends() {
            let session = Session::open(storage).unwrap();
            let c = session
                .commit(|tx| tx.create_conversation(ConversationOwnership::Ownerless))
                .await
                .unwrap();
            let e = session
                .commit(move |tx| {
                    let d = conversation_definition("r", History::Rewindable, ForkPolicy::AsOf);
                    let id = tx.doc(&d, conversation_address("r", c.id), Value::Null)?;
                    tx.apply(id, Op::Set(vec!["count".into()], json!(4)))?;
                    tx.append_entry(c.id, EntryDraft::new("cut"))
                })
                .await
                .unwrap();
            let child_id = ConversationId::new(e.id.get() + 1).unwrap();
            let mut observe = session
                .observe(ObservationQuery {
                    scopes: vec![Scope::Conversation {
                        conversation_id: child_id,
                    }],
                    ..ObservationQuery::default()
                })
                .await
                .unwrap();
            let child = session
                .commit(move |tx| {
                    tx.fork_conversation(c.id, e.id, ConversationOwnership::Ownerless)
                })
                .await
                .unwrap();
            assert_eq!(child.id, child_id);
            session
                .commit(move |tx| {
                    let d = conversation_definition("r", History::Rewindable, ForkPolicy::AsOf);
                    let id = tx.doc(&d, conversation_address("r", child.id), Value::Null)?;
                    tx.apply(id, Op::Set(vec!["count".into()], json!(8)))
                })
                .await
                .unwrap();
            let copy = observe.next().await.unwrap();
            let changed = observe.next().await.unwrap();
            assert_eq!(
                copy.snapshot.documents[&conversation_address("r", child.id)].value["count"],
                4
            );
            assert_eq!(
                changed.snapshot.documents[&conversation_address("r", child.id)].value["count"],
                8
            );
            session.close().await.unwrap();
        }
    });
}
#[test]
fn close_seal_retains_state_while_admitted_backend_finishes() {
    runtime().block_on(async {
        for (_, storage, _) in backends() {
            let probe = Probe::new(storage);
            let session = Session::open(probe.clone()).unwrap();
            create(&session).await;
            let state = session
                .document_state(definition(), address())
                .await
                .unwrap()
                .unwrap();
            let watch = session
                .watch_doc(definition(), address())
                .await
                .unwrap()
                .unwrap();
            let gate = probe.hold();
            let s = session.clone();
            let write = tokio::spawn(async move { set(&s, 7).await });
            let g = gate.clone();
            tokio::task::spawn_blocking(move || g.wait_entered())
                .await
                .unwrap();
            let s = session.clone();
            let close = tokio::spawn(async move { s.close().await });
            assert_eq!(watch.closed().await, WatchEnd::SessionClosed);
            gate.release();
            write.await.unwrap();
            close.await.unwrap().unwrap();
            assert_eq!(state.value()["count"], 0);
            assert_eq!(watch.value()["count"], 0);
            assert_eq!(state.end(), Some(WatchEnd::SessionClosed));
        }
    });
}

#[test]
fn normal_close_drains_complete_publication_of_admitted_commit() {
    runtime().block_on(async {
        let mut evidence = Vec::new();
        for (name, storage, temporary) in backends() {
            let probe = Probe::new(storage);
            let session = Session::open(probe.clone()).unwrap();
            let (document_id, conversation) = session
                .commit(|tx| {
                    let document_id = tx.doc(&definition(), address(), Value::Null)?;
                    let conversation = tx.create_conversation(ConversationOwnership::Ownerless)?;
                    Ok((document_id, conversation))
                })
                .await
                .unwrap();
            let state = session
                .document_state(definition(), address())
                .await
                .unwrap()
                .unwrap();
            let watch = session
                .watch_doc(definition(), address())
                .await
                .unwrap()
                .unwrap();
            let mut commits = session.subscribe_commits().await.unwrap();

            let gate = probe.hold();
            let writer = session.clone();
            let commit = tokio::spawn(async move {
                writer
                    .commit(move |tx| {
                        let id = tx.doc(&definition(), address(), Value::Null)?;
                        tx.apply(id, Op::Set(vec!["count".into()], json!(7)))?;
                        let mut entry = EntryDraft::new("closing");
                        entry.data = Some(json!({"complete": true}));
                        tx.append_entry(conversation.id, entry)
                    })
                    .await
            });
            let entered = gate.clone();
            tokio::task::spawn_blocking(move || entered.wait_entered())
                .await
                .unwrap();
            let closer = session.clone();
            let close = tokio::spawn(async move { closer.close().await });
            // This terminal signal proves the close seal occurred while the
            // admitted backend operation was still gated, without timing guesses.
            assert_eq!(watch.closed().await, WatchEnd::SessionClosed);
            assert_eq!(state.end(), Some(WatchEnd::SessionClosed));
            assert_eq!(state.value()["count"], 0);
            assert_eq!(watch.value()["count"], 0);

            gate.release();
            let entry = commit.await.unwrap().unwrap();
            close.await.unwrap().unwrap();
            let mut publications = Vec::new();
            while let Some(publication) = commits.next().await {
                publications.push(publication);
            }
            assert_eq!(state.value()["count"], 0);
            assert_eq!(watch.value()["count"], 0);
            assert_eq!(session.closed().await, SessionEnd::Closed);
            assert!(probe.inner.mint_id().is_err());

            if let Some(temporary) = temporary {
                let reopened = SqliteStorage::open(&temporary.path).unwrap();
                assert_eq!(
                    reopened
                        .document(document_id, DocumentPoint::Current)
                        .unwrap()
                        .unwrap()
                        .value["count"],
                    7
                );
                assert_eq!(reopened.entry(entry.id).unwrap().unwrap().entry, entry);
                reopened.close().unwrap();
            }
            evidence.push((name, document_id, entry, publications));
        }

        eprintln!(
            "admitted close publications: {:?}",
            evidence
                .iter()
                .map(|(name, _, _, publications)| (
                    name,
                    publications.iter().map(|p| p.seq.get()).collect::<Vec<_>>()
                ))
                .collect::<Vec<_>>()
        );
        for (name, document_id, entry, publications) in evidence {
            assert_eq!(publications.len(), 1, "{name}: successful admitted batch");
            let publication = &publications[0];
            assert_eq!(publication.seq.get(), 2);
            assert_eq!(publication.changes.len(), 2, "{name}: complete mixed batch");
            assert!(publication.changes.iter().any(|change| matches!(
                change,
                CommitChange::Table(StorageWrite::Entry { value }) if value == &entry
            )));
            let document = publication.changes.iter().find_map(|change| match change {
                CommitChange::Document {
                    record,
                    version,
                    value,
                    ops,
                    ..
                } if record.identity.id == document_id => Some((record, version, value, ops)),
                _ => None,
            });
            let (record, version, value, ops) = document.expect("complete document publication");
            assert_eq!(record.created_at.get(), 1);
            assert_eq!(*version, Some(1));
            assert_eq!(value["count"], 7);
            assert_eq!(ops, &[Op::Set(vec!["count".into()], json!(7))]);
        }
    });
}

#[test]
fn sqlite_open_and_exact_lookup_do_not_materialize_unrelated_documents() {
    let temp = Temp::new();
    let db = pi_sqlite::Database::open(&temp.path).unwrap();
    let storage = SqliteStorage::from_database(db.clone()).unwrap();
    for id in [2, 3] {
        storage
            .commit(&[StorageWrite::DocumentCreate {
                record: DocumentCreate {
                    id: DocumentId::new(id).unwrap(),
                    kind: format!("doc{id}"),
                    scope: Scope::Session,
                    key: None,
                    history: None,
                    fork: None,
                },
                content: DocumentContent::Base {
                    version: 1,
                    value: json!({"id":id}),
                },
            }])
            .unwrap();
    }
    storage.close().unwrap();
    db.read::<_, pi_sqlite::Error>(|c| {
        c.execute(
            "UPDATE durable_documents SET payload='corrupt unrelated row' WHERE id=3",
            [],
        )?;
        Ok(())
    })
    .unwrap();
    let reopened = SqliteStorage::from_database(db).unwrap();
    assert_eq!(
        reopened
            .document(DocumentId::new(2).unwrap(), DocumentPoint::Current)
            .unwrap()
            .unwrap()
            .value,
        json!({"id":2})
    );
    assert!(
        reopened
            .document(DocumentId::new(3).unwrap(), DocumentPoint::Current)
            .is_err()
    );
    reopened.close().unwrap();
}

#[test]
fn before_and_after_commit_failures_close_both_backends_without_adoption() {
    runtime().block_on(async {
        for mode in [1, 2] {
            for (_, storage, _) in backends() {
                let probe = Probe::new(storage);
                let session = Session::open(probe.clone()).unwrap();
                create(&session).await;
                let state = session
                    .document_state(definition(), address())
                    .await
                    .unwrap()
                    .unwrap();
                let watch = session
                    .watch_doc(definition(), address())
                    .await
                    .unwrap()
                    .unwrap();
                let mut commits = session.subscribe_commits().await.unwrap();
                probe.fail(mode);
                let result = session
                    .commit(|tx| {
                        let id = tx.doc(&definition(), address(), Value::Null)?;
                        tx.apply(id, Op::Set(vec!["count".into()], json!(99)))
                    })
                    .await;
                assert!(result.is_err());
                assert_eq!(state.value()["count"], 0);
                assert_eq!(watch.value()["count"], 0);
                assert!(matches!(watch.closed().await, WatchEnd::SessionFailed(_)));
                assert!(commits.next().await.is_none());
                assert!(probe.inner.mint_id().is_err());
                assert!(session.commit(|_| Ok(())).await.is_err());
            }
        }
    });
}
