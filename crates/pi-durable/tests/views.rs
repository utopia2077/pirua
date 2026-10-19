mod durable_views_support;
use durable_views_support::*;
use pi_durable::view::*;
use pi_durable_session::*;
use serde_json::{Value, json};
use std::{collections::BTreeMap, time::Duration};

fn replay(before: &ConversationView, frame: &ConversationViewFrame) {
    let rebuilt = apply(&serde_json::to_value(before).unwrap(), &frame.ops).unwrap();
    assert_eq!(rebuilt, serde_json::to_value(frame.value.as_ref()).unwrap());
}
async fn create(session: &Session, kind: &str, value: Value) -> DocumentId {
    let def = definition(kind, value, History::Latest, ForkPolicy::Initial);
    let address = address(kind, ROOT_CONVERSATION_ID);
    session
        .commit(move |tx| tx.doc(&def, address, Value::Null))
        .await
        .unwrap()
}
fn acquire(tx: &mut Transaction<'_>, kind: &str, id: DocumentId) -> Result<()> {
    let (history, fork) = if kind == "pi.agent" {
        (History::Rewindable, ForkPolicy::AsOf)
    } else {
        (History::Latest, ForkPolicy::Initial)
    };
    let def = definition(kind, json!({}), history, fork);
    assert_eq!(
        tx.doc(&def, address(kind, ROOT_CONVERSATION_ID), Value::Null)?,
        id
    );
    Ok(())
}
async fn set(session: &Session, id: DocumentId, kind: &str, n: u64) {
    let kind = kind.to_owned();
    session
        .commit(move |tx| {
            acquire(tx, &kind, id)?;
            tx.apply(id, Op::Set(vec!["n".into()], json!(n)))
        })
        .await
        .unwrap();
}
fn strip_attribution(value: &mut Value) {
    match value {
        Value::Object(object) => {
            object.remove("byTaskId");
            for value in object.values_mut() {
                strip_attribution(value);
            }
        }
        Value::Array(values) => {
            for value in values {
                strip_attribution(value);
            }
        }
        _ => (),
    }
}

#[test]
fn frozen_source_mounted_publications_replayed_through_real_session_writes() {
    runtime().block_on(async {
        let fixture = fixture();
        let case = fixture["cases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["id"] == "runtime-tool-progress-mounted-view")
            .unwrap();
        for (backend, store, _temp) in backends() {
            let expected = &case["expected"][backend];
            let observations = expected["observations"].as_array().unwrap();
            let baseline = &observations
                .iter()
                .find(|o| o["label"] == "0:watch")
                .unwrap()["value"];
            let output = &observations
                .iter()
                .find(|o| o["label"] == "9:watchFrames")
                .unwrap()["value"];
            let publications = expected["publications"].as_array().unwrap();
            let session = Session::open(store.clone()).unwrap();
            let mut definitions = BTreeMap::new();
            let mut documents = BTreeMap::new();
            let initial = publications[0]["changes"].as_array().unwrap();
            for change in initial.iter().filter(|c| c["type"] == "document") {
                let record = &change["record"];
                let kind = record["kind"].as_str().unwrap();
                let history = serde_json::from_value(record["history"].clone()).unwrap();
                let fork = serde_json::from_value(record["fork"].clone()).unwrap();
                definitions.insert(
                    kind.to_owned(),
                    definition(kind, change["value"].clone(), history, fork),
                );
            }
            let defs = definitions.clone();
            let changes = initial.clone();
            let created = session
                .commit(move |tx| {
                    tx.ensure_root_conversation()?;
                    let mut created = BTreeMap::new();
                    for change in changes.iter().filter(|c| c["type"] == "document") {
                        let kind = change["record"]["kind"].as_str().unwrap();
                        let id = tx.doc(
                            &defs[kind],
                            address(kind, ROOT_CONVERSATION_ID),
                            Value::Null,
                        )?;
                        assert_eq!(serde_json::to_value(id).unwrap(), change["record"]["id"]);
                        created.insert(kind.to_owned(), id);
                    }
                    Ok(created)
                })
                .await
                .unwrap();
            documents.extend(created);
            let views = ConversationViews::new(session.clone());
            let watch = views.watch(ROOT_CONVERSATION_ID).await.unwrap();
            assert_eq!(
                serde_json::to_value(watch.value().as_ref()).unwrap(),
                *baseline
            );
            let mut before = watch.value();
            let mut next_id = 7;
            let mut source_sequences = Vec::new();
            for publication in &publications[1..] {
                let changes = publication["changes"].as_array().unwrap();
                let relevant = changes
                    .iter()
                    .any(|c| c["type"] == "entry" || c["type"] == "document");
                if !relevant {
                    continue;
                }
                // Project committed mount inputs only. Scheduler/submission records
                // are outside this module. Reserve their global IDs and omit entry
                // task attribution symmetrically; all mount ops/order/values remain.
                for change in changes.iter().filter(|c| c["type"] == "entry") {
                    let target = change["value"]["id"].as_u64().unwrap();
                    while next_id < target {
                        assert_eq!(store.mint_id().unwrap(), next_id);
                        next_id += 1;
                    }
                    assert_eq!(next_id, target);
                    next_id += 1;
                }
                let changes = changes.clone();
                let docs = documents.clone();
                let defs = definitions.clone();
                session
                    .commit(move |tx| {
                        for change in changes.iter().filter(|c| c["type"] == "document") {
                            let kind = change["record"]["kind"].as_str().unwrap();
                            let ops: Vec<Op> = serde_json::from_value(change["ops"].clone())?;
                            assert_eq!(
                                tx.doc(
                                    &defs[kind],
                                    address(kind, ROOT_CONVERSATION_ID),
                                    Value::Null
                                )?,
                                docs[kind]
                            );
                            tx.apply_all(docs[kind], &ops)?;
                        }
                        for change in changes.iter().filter(|c| c["type"] == "entry") {
                            let value = &change["value"];
                            let draft = EntryDraft {
                                kind: value["kind"].as_str().unwrap().into(),
                                model: value.get("model").map(|v| v.as_array().unwrap().clone()),
                                data: value.get("data").cloned(),
                                head: value.get("head").map(|v| {
                                    EntryHead::Entry(serde_json::from_value(v.clone()).unwrap())
                                }),
                                edits: value
                                    .get("edits")
                                    .map(|v| serde_json::from_value(v.clone()).unwrap()),
                            };
                            let entry = tx.append_entry(ROOT_CONVERSATION_ID, draft)?;
                            assert_eq!(serde_json::to_value(entry.id).unwrap(), value["id"]);
                        }
                        Ok(())
                    })
                    .await
                    .unwrap();
                let actual = frame(&watch).await;
                replay(&before, &actual);
                let index = source_sequences.len();
                assert_eq!(actual.seq.get(), index as u64 + 2);
                let mut actual = json!({"value":actual.value.as_ref(),"ops":actual.ops});
                let mut wanted = output["frames"][index].clone();
                strip_attribution(&mut actual);
                strip_attribution(&mut wanted);
                assert_eq!(
                    actual, wanted,
                    "{backend} source seq {}",
                    publication["seq"]
                );
                before = watch.value();
                source_sequences.push(publication["seq"].clone());
            }
            assert_eq!(json!(source_sequences), output["touchingCommits"]);
            assert_eq!(
                source_sequences.len(),
                output["frames"].as_array().unwrap().len()
            );
            let mut final_view = serde_json::to_value(before.as_ref()).unwrap();
            let mut expected_final = output["final"].clone();
            strip_attribution(&mut final_view);
            strip_attribution(&mut expected_final);
            assert_eq!(final_view, expected_final);
            no_frame(&watch).await;
            session.close().await.unwrap();
        }
    });
}

#[test]
fn absent_docs_selected_singletons_and_one_batch_with_real_ops() {
    runtime().block_on(async {
        for (_, store, _temp) in backends() {
            let session = Session::open(store).unwrap();
            root(&session).await;
            let views = ConversationViews::new(session.clone());
            let state = views.state(ROOT_CONVERSATION_ID).await.unwrap();
            let watch = views.watch(ROOT_CONVERSATION_ID).await.unwrap();
            let initial = watch.value();
            assert!(initial.docs.is_empty());
            let id = create(
                &session,
                "pi.live",
                json!({"n":0,"text":"😀x","xs":[1,2],"obj":{}}),
            )
            .await;
            let created = frame(&watch).await;
            replay(&initial, &created);
            assert_eq!(
                created.ops,
                vec![Op::Set(
                    vec!["docs".into(), "pi.live".into()],
                    json!({"n":0,"text":"😀x","xs":[1,2],"obj":{}})
                )]
            );
            let before = created.value;
            let actual_ops = vec![
                Op::Append(vec!["text".into()], "y".into()),
                Op::Trim(vec!["text".into()], 2),
                Op::Set(vec!["obj".into(), "x".into()], json!(1)),
                Op::Delete(vec!["obj".into(), "x".into()]),
                Op::Permute(vec!["xs".into()], vec![1, 0]),
                Op::Splice(vec!["xs".into()], 0, 1, vec![json!(2)]),
            ];
            let ops = actual_ops.clone();
            session
                .commit(move |tx| {
                    acquire(tx, "pi.live", id)?;
                    tx.apply_all(id, &ops)?;
                    tx.append_entry(
                        ROOT_CONVERSATION_ID,
                        EntryDraft {
                            kind: "raw".into(),
                            model: Some(vec![user("one")]),
                            data: None,
                            head: None,
                            edits: None,
                        },
                    )?;
                    tx.append_entry(ROOT_CONVERSATION_ID, EntryDraft::new("two"))?;
                    Ok(())
                })
                .await
                .unwrap();
            let next = frame(&watch).await;
            replay(&before, &next);
            assert_eq!(next.ops.len(), actual_ops.len() + 2);
            assert_eq!(next.value.docs["pi.live"]["text"], "xy");
            assert_eq!(before.entries.len(), 0);
            assert_eq!(initial.docs.len(), 0);
            let values = state.value().await;
            assert_eq!(values, next.value);
            create(&session, "third.party", json!({"n":1})).await;
            // A family with a built-in kind is also outside the fixed singleton mount.
            let def = DocDefinition::new(
                "pi.inbox",
                1,
                DocumentSemantics::Conversation {
                    history: History::Latest,
                    fork: ForkPolicy::Initial,
                },
                true,
                |_| Ok(json!({"n":0})),
                |_, _, _| Ok(false),
            )
            .unwrap();
            session
                .commit(move |tx| {
                    tx.doc(
                        &def,
                        DocumentAddress {
                            kind: "pi.inbox".into(),
                            scope: Scope::Conversation {
                                conversation_id: ROOT_CONVERSATION_ID,
                            },
                            key: Some("family".into()),
                        },
                        Value::Null,
                    )
                })
                .await
                .unwrap();
            no_frame(&watch).await;
            assert_eq!(state.value().await, next.value);
            session.close().await.unwrap();
        }
    });
}

#[test]
fn heads_keep_currently_mounted_suffix_and_forks_follow_only_own_commits() {
    runtime().block_on(async {
        for (_, store, _temp) in backends() {
            let session = Session::open(store).unwrap();
            root(&session).await;
            let first = append(&session, vec![user("one")], "raw", None, None).await;
            let second = append(&session, vec![user("two")], "raw", None, None).await;
            append(&session, vec![user("three")], "raw", None, None).await;
            let views = ConversationViews::new(session.clone());
            let state = views.state(ROOT_CONVERSATION_ID).await.unwrap();
            let watch = views.watch(ROOT_CONVERSATION_ID).await.unwrap();
            let before = watch.value();
            append(
                &session,
                vec![user("summary")],
                "head",
                Some(EntryHead::Entry(second.id)),
                None,
            )
            .await;
            let cut = frame(&watch).await;
            replay(&before, &cut);
            assert_eq!(cut.ops.len(), 1);
            assert_eq!(cut.value.entries.len(), 3);
            let raw = append(
                &session,
                vec![],
                "backward-head",
                Some(EntryHead::Entry(first.id)),
                None,
            )
            .await;
            let backward = frame(&watch).await;
            replay(&cut.value, &backward);
            assert_eq!(backward.value.entries[0].id, raw.id);
            assert!(!backward.value.entries.iter().any(|e| e.id == first.id));
            assert!(
                session
                    .active_entries(ROOT_CONVERSATION_ID, None)
                    .await
                    .unwrap()
                    .iter()
                    .any(|e| e.id == first.id)
            );
            assert_eq!(state.value().await, backward.value);
            let fresh = ConversationViews::new(session.clone())
                .state(ROOT_CONVERSATION_ID)
                .await
                .unwrap()
                .value()
                .await;
            assert!(fresh.entries.iter().any(|e| e.id == first.id));
            let child = session
                .commit(move |tx| {
                    tx.fork_conversation(
                        ROOT_CONVERSATION_ID,
                        first.id,
                        ConversationOwnership::Ownerless,
                    )
                })
                .await
                .unwrap();
            let child_watch = views.watch(child.id).await.unwrap();
            assert_eq!(child_watch.value().entries, vec![first]);
            append(&session, vec![], "parent-only", None, None).await;
            no_frame(&child_watch).await;
            session
                .commit(move |tx| tx.append_entry(child.id, EntryDraft::new("child-only")))
                .await
                .unwrap();
            let next = frame(&child_watch).await;
            assert_eq!(next.value.entries.len(), 2);
            assert_eq!(next.ops.len(), 1);
            session.close().await.unwrap();
        }
    });
}

#[test]
fn incarnation_retirement_recreation_and_fork_copies_are_immutable() {
    runtime().block_on(async {
        for (_, store, _temp) in backends() {
            let session = Session::open(store).unwrap();
            root(&session).await;
            let def = definition(
                "pi.agent",
                json!({"n":0}),
                History::Rewindable,
                ForkPolicy::AsOf,
            );
            let def2 = def.clone();
            let id = session
                .commit(move |tx| {
                    tx.doc(
                        &def2,
                        address("pi.agent", ROOT_CONVERSATION_ID),
                        Value::Null,
                    )
                })
                .await
                .unwrap();
            let entry = append(&session, vec![user("kept")], "raw", None, None).await;
            set(&session, id, "pi.agent", 1).await;
            let views = ConversationViews::new(session.clone());
            let watch = views.watch(ROOT_CONVERSATION_ID).await.unwrap();
            let before = watch.value();
            let retire_definition = def.clone();
            let replacement = session
                .commit(move |tx| {
                    tx.retire_doc(&def, address("pi.agent", ROOT_CONVERSATION_ID))?;
                    tx.doc(&def, address("pi.agent", ROOT_CONVERSATION_ID), Value::Null)
                })
                .await
                .unwrap();
            assert_ne!(replacement, id);
            let replaced = frame(&watch).await;
            replay(&before, &replaced);
            assert_eq!(replaced.value.docs["pi.agent"], json!({"n":0}));
            assert_eq!(before.docs["pi.agent"], json!({"n":1}));
            set(&session, replacement, "pi.agent", 2).await;
            let changed = frame(&watch).await;
            replay(&replaced.value, &changed);
            assert_eq!(changed.value.docs["pi.agent"], json!({"n":2}));
            let child = session
                .commit(move |tx| {
                    tx.fork_conversation(
                        ROOT_CONVERSATION_ID,
                        entry.id,
                        ConversationOwnership::Ownerless,
                    )
                })
                .await
                .unwrap();
            let child_watch = views.watch(child.id).await.unwrap();
            assert_eq!(child_watch.value().docs["pi.agent"], json!({"n":0}));
            set(&session, replacement, "pi.agent", 3).await;
            no_frame(&child_watch).await;
            session
                .commit(move |tx| {
                    tx.retire_doc(
                        &retire_definition,
                        address("pi.agent", ROOT_CONVERSATION_ID),
                    )
                })
                .await
                .unwrap();
            let _pending = frame(&watch).await;
            let retired = frame(&watch).await;
            assert_eq!(
                retired.ops,
                vec![Op::Delete(vec!["docs".into(), "pi.agent".into()])]
            );
            assert!(retired.value.docs.is_empty());
            assert!(watch.end().is_none()); // Document retirement does not retire the conversation mount.
            assert_eq!(child_watch.value().docs["pi.agent"], json!({"n":0}));
            session.close().await.unwrap();
        }
    });
}

#[test]
fn structural_noops_survive_and_101st_pending_frame_converges() {
    runtime().block_on(async {
        for (_, store, _temp) in backends() {
            let session = Session::open(store).unwrap();
            root(&session).await;
            let id = create(&session, "pi.inbox", json!({"n":0,"items":[1,2]})).await;
            let views = ConversationViews::new(session.clone());
            let state = views.state(ROOT_CONVERSATION_ID).await.unwrap();
            let watch = views.watch(ROOT_CONVERSATION_ID).await.unwrap();
            let initial = watch.value();
            session
                .commit(move |tx| {
                    acquire(tx, "pi.inbox", id)?;
                    tx.apply(id, Op::Splice(vec!["items".into()], 0, 1, vec![json!(1)]))
                })
                .await
                .unwrap();
            let noop = frame(&watch).await;
            assert_eq!(noop.value, initial);
            assert_eq!(
                noop.ops,
                vec![Op::Splice(
                    vec!["docs".into(), "pi.inbox".into(), "items".into()],
                    0,
                    1,
                    vec![json!(1)]
                )]
            );
            for n in 1..=101 {
                set(&session, id, "pi.inbox", n).await;
                assert_eq!(state.value().await.docs["pi.inbox"]["n"], n);
            }
            assert_eq!(watch.value(), initial);
            let newest = frame(&watch).await;
            assert_eq!(newest.value.docs["pi.inbox"]["n"], 101);
            assert_eq!(
                newest.ops,
                vec![Op::Replace(
                    serde_json::to_value(newest.value.as_ref()).unwrap()
                )]
            );
            replay(&initial, &newest);
            no_frame(&watch).await;
            session.close().await.unwrap();
        }
    });
}

#[test]
fn listeners_are_serialized_failures_isolated_and_close_retains_values() {
    runtime().block_on(async {
        for (_, store, _temp) in backends() {
            let session = Session::open(store).unwrap();
            root(&session).await;
            let id = create(&session, "pi.live", json!({"n":0})).await;
            let views = ConversationViews::new(session.clone());
            let state = views.state(ROOT_CONVERSATION_ID).await.unwrap();
            let failed = views.watch(ROOT_CONVERSATION_ID).await.unwrap();
            let good = views.watch(ROOT_CONVERSATION_ID).await.unwrap();
            failed
                .start(|_| async { Err(Error::request("listener rejection")) })
                .unwrap();
            set(&session, id, "pi.live", 1).await;
            assert!(matches!(
                tokio::time::timeout(Duration::from_secs(5), failed.closed())
                    .await
                    .unwrap(),
                WatchEnd::ListenerError(_)
            ));
            assert!(failed.next().await.is_err());
            assert_eq!(frame(&good).await.value.docs["pi.live"]["n"], 1);
            let listener = views.watch(ROOT_CONVERSATION_ID).await.unwrap();
            let (release_send, release_receive) = tokio::sync::oneshot::channel();
            let mut release = Some(release_receive);
            let (received_send, mut received) = tokio::sync::mpsc::unbounded_channel();
            listener
                .start(move |frame| {
                    let gate = release.take();
                    let send = received_send.clone();
                    async move {
                        send.send(frame).unwrap();
                        if let Some(gate) = gate {
                            let _ = gate.await;
                        }
                        Ok(())
                    }
                })
                .unwrap();
            set(&session, id, "pi.live", 2).await;
            let running = tokio::time::timeout(Duration::from_secs(5), received.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(running.value.docs["pi.live"]["n"], 2);
            for n in 3..=103 {
                set(&session, id, "pi.live", n).await;
                state.value().await;
            }
            assert!(received.try_recv().is_err());
            release_send.send(()).unwrap();
            let converged = tokio::time::timeout(Duration::from_secs(5), received.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(converged.value.docs["pi.live"]["n"], 103);
            assert!(matches!(converged.ops.as_slice(), [Op::Replace(_)]));
            let saved = state.value().await;
            let delivered = good.value();
            session.close().await.unwrap();
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(5), good.closed())
                    .await
                    .unwrap(),
                WatchEnd::SessionClosed
            );
            assert!(good.next().await.unwrap().is_none());
            assert_eq!(good.value(), delivered);
            assert_eq!(state.value().await, saved);
            assert_eq!(state.end(), Some(WatchEnd::SessionClosed));
            assert!(views.state(ROOT_CONVERSATION_ID).await.is_err());
            assert!(views.watch(ROOT_CONVERSATION_ID).await.is_err());
            assert_eq!(
                failed.end(),
                Some(WatchEnd::ListenerError("listener rejection".into()))
            );
        }
    });
}

#[test]
fn stop_cancel_drop_and_version_rejection() {
    runtime().block_on(async {
        for (_, store, _temp) in backends() {
            let session = Session::open(store).unwrap();
            root(&session).await;
            let views = ConversationViews::new(session.clone());
            let watch = views.watch(ROOT_CONVERSATION_ID).await.unwrap();
            let snapshot = watch.value();
            assert_eq!(watch.cancel(), WatchEnd::Cancelled);
            assert_eq!(watch.stop(), WatchEnd::Cancelled);
            assert!(watch.next().await.unwrap().is_none());
            assert_eq!(watch.value(), snapshot);
            drop(watch);
            let state = views.state(ROOT_CONVERSATION_ID).await.unwrap();
            drop(state);
            let def = DocDefinition::new(
                "pi.agent",
                2,
                DocumentSemantics::Conversation {
                    history: History::Latest,
                    fork: ForkPolicy::Initial,
                },
                false,
                |_| Ok(json!({})),
                |_, _, _| Ok(false),
            )
            .unwrap();
            session
                .commit(move |tx| {
                    tx.doc(&def, address("pi.agent", ROOT_CONVERSATION_ID), Value::Null)
                })
                .await
                .unwrap();
            assert_eq!(
                views.state(ROOT_CONVERSATION_ID).await.err().unwrap().kind,
                ErrorKind::Definition
            );
            session.close().await.unwrap();
        }
    });
}

#[test]
fn close_seals_mounts_before_admitted_commit_and_waiting_acquisition() {
    runtime().block_on(async {
        for (_, store, _temp) in backends() {
            let session = Session::open(store).unwrap();
            root(&session).await;
            let id = create(&session, "pi.live", json!({"n":0})).await;
            let views = ConversationViews::new(session.clone());
            let state = views.state(ROOT_CONVERSATION_ID).await.unwrap();
            let watch = views.watch(ROOT_CONVERSATION_ID).await.unwrap();
            let baseline = state.value().await;
            let gate = std::sync::Arc::new((
                std::sync::Mutex::new((false, false)),
                std::sync::Condvar::new(),
            ));
            let writer_gate = gate.clone();
            let writer_session = session.clone();
            let writer = tokio::spawn(async move {
                writer_session
                    .commit(move |tx| {
                        let (lock, condition) = &*writer_gate;
                        let mut state = lock.lock().unwrap();
                        state.0 = true;
                        condition.notify_all();
                        while !state.1 {
                            state = condition.wait(state).unwrap();
                        }
                        drop(state);
                        acquire(tx, "pi.live", id)?;
                        tx.apply(id, Op::Set(vec!["n".into()], json!(1)))
                    })
                    .await
            });
            {
                let (lock, condition) = &*gate;
                let mut entered = lock.lock().unwrap();
                while !entered.0 {
                    entered = condition.wait(entered).unwrap();
                }
            }
            let acquiring_views = views.clone();
            let acquiring =
                tokio::spawn(async move { acquiring_views.state(ROOT_CONVERSATION_ID).await });
            tokio::task::yield_now().await;
            let closing_session = session.clone();
            let closing = tokio::spawn(async move { closing_session.close().await });
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(5), watch.closed())
                    .await
                    .unwrap(),
                WatchEnd::SessionClosed
            );
            assert!(!writer.is_finished());
            assert_eq!(state.value().await, baseline);
            {
                let (lock, condition) = &*gate;
                let mut release = lock.lock().unwrap();
                release.1 = true;
                condition.notify_all();
            }
            writer.await.unwrap().unwrap();
            closing.await.unwrap().unwrap();
            assert!(acquiring.await.unwrap().is_err());
            assert_eq!(state.value().await, baseline);
            assert_eq!(watch.value(), baseline);
            assert!(watch.next().await.unwrap().is_none());
        }
    });
}

#[test]
fn stopped_watch_releases_last_mount_and_root_replace_prefixes_exactly() {
    runtime().block_on(async {
        for (_, store, _temp) in backends() {
            let session = Session::open(store).unwrap();
            root(&session).await;
            let first = append(&session, vec![], "first", None, None).await;
            let second = append(&session, vec![], "second", None, None).await;
            let id = create(&session, "pi.live", json!({"n":0})).await;
            let views = ConversationViews::new(session.clone());
            let old = views.watch(ROOT_CONVERSATION_ID).await.unwrap();
            append(
                &session,
                vec![],
                "cut",
                Some(EntryHead::Entry(second.id)),
                None,
            )
            .await;
            frame(&old).await;
            append(
                &session,
                vec![],
                "backward",
                Some(EntryHead::Entry(first.id)),
                None,
            )
            .await;
            let cutoff = frame(&old).await;
            assert!(!cutoff.value.entries.iter().any(|e| e.id == first.id));
            old.stop();
            // Keep the stopped handle: it must no longer keep the stale mount attached.
            let remounted = views.watch(ROOT_CONVERSATION_ID).await.unwrap();
            assert!(remounted.value().entries.iter().any(|e| e.id == first.id));
            let before = remounted.value();
            session
                .commit(move |tx| {
                    acquire(tx, "pi.live", id)?;
                    tx.apply(id, Op::Replace(json!({"n":1,"items":[]})))
                })
                .await
                .unwrap();
            let changed = frame(&remounted).await;
            assert_eq!(
                changed.ops,
                vec![Op::Set(
                    vec!["docs".into(), "pi.live".into()],
                    json!({"n":1,"items":[]})
                )]
            );
            replay(&before, &changed);
            assert_eq!(old.value(), cutoff.value);
            assert_eq!(old.end(), Some(WatchEnd::Stopped));
            session.close().await.unwrap();
        }
    });
}

#[test]
fn first_hydration_rejects_state_and_watch_when_close_seals_during_read() {
    runtime().block_on(async {
        let mut failures = Vec::new();
        for watch in [false, true] {
            for (backend, store, _temp) in backends() {
                let gated = HydrationGate::new(store);
                let session = Session::open(gated.clone()).unwrap();
                root(&session).await;
                let views = ConversationViews::new(session.clone());
                gated.arm();
                let acquiring = tokio::spawn(async move {
                    if watch {
                        views.watch(ROOT_CONVERSATION_ID).await.map(|_| ())
                    } else {
                        views.state(ROOT_CONVERSATION_ID).await.map(|_| ())
                    }
                });
                gated.wait_entered();
                let mut closing = Box::pin(session.close());
                std::future::poll_fn(|context| {
                    assert!(std::future::Future::poll(closing.as_mut(), context).is_pending());
                    std::task::Poll::Ready(())
                })
                .await;
                gated.release();
                let result = tokio::time::timeout(Duration::from_secs(5), acquiring)
                    .await
                    .unwrap()
                    .unwrap();
                if !result.is_err_and(|error| error.kind == ErrorKind::Closed) {
                    failures.push(format!(
                        "{backend}: watch={watch} first hydration admitted after close"
                    ));
                }
                closing.await.unwrap();
            }
        }
        assert!(failures.is_empty(), "{failures:?}");
    });
}

struct ListenerDropSignal(Option<tokio::sync::oneshot::Sender<()>>);
impl Drop for ListenerDropSignal {
    fn drop(&mut self) {
        if let Some(send) = self.0.take() {
            let _ = send.send(());
        }
    }
}

#[test]
fn last_public_started_watch_clone_releases_mount_while_callback_finishes() {
    runtime().block_on(async {
        let mut failures = Vec::new();
        for (backend, store, _temp) in backends() {
            let session = Session::open(store).unwrap();
            root(&session).await;
            let first = append(&session, vec![], "first", None, None).await;
            let second = append(&session, vec![], "second", None, None).await;
            let views = ConversationViews::new(session.clone());
            let old = views.watch(ROOT_CONVERSATION_ID).await.unwrap();
            let alias = old.clone();
            let (received_send, mut received) = tokio::sync::mpsc::unbounded_channel();
            let (release_send, release_receive) = tokio::sync::oneshot::channel();
            let (finished_send, finished_receive) = tokio::sync::oneshot::channel();
            let listener_lifetime = ListenerDropSignal(Some(finished_send));
            let mut release = Some(release_receive);
            let mut calls = 0;
            old.start(move |frame| {
                let _keep_listener_alive = &listener_lifetime;
                calls += 1;
                let gate = if calls == 2 { release.take() } else { None };
                let send = received_send.clone();
                async move {
                    send.send(frame).unwrap();
                    if let Some(gate) = gate {
                        let _ = gate.await;
                    }
                    Ok(())
                }
            })
            .unwrap();
            append(
                &session,
                vec![],
                "cut",
                Some(EntryHead::Entry(second.id)),
                None,
            )
            .await;
            tokio::time::timeout(Duration::from_secs(5), received.recv())
                .await
                .unwrap()
                .unwrap();
            append(
                &session,
                vec![],
                "backward",
                Some(EntryHead::Entry(first.id)),
                None,
            )
            .await;
            let backward = tokio::time::timeout(Duration::from_secs(5), received.recv())
                .await
                .unwrap()
                .unwrap();
            assert!(
                !backward
                    .value
                    .entries
                    .iter()
                    .any(|entry| entry.id == first.id)
            );
            drop(old);
            append(&session, vec![], "queued-for-surviving-clone", None, None).await;
            let shared = views.watch(ROOT_CONVERSATION_ID).await.unwrap();
            assert!(
                !shared
                    .value()
                    .entries
                    .iter()
                    .any(|entry| entry.id == first.id)
            );
            shared.stop();
            drop(shared);
            assert!(received.try_recv().is_err()); // The second callback is still running.
            drop(alias); // Worker and monitor retain inner state, but no public custody remains.
            let remounted = views.watch(ROOT_CONVERSATION_ID).await.unwrap();
            if !remounted
                .value()
                .entries
                .iter()
                .any(|entry| entry.id == first.id)
            {
                failures.push(format!(
                    "{backend}: background worker retained the stale mount"
                ));
            }
            append(&session, vec![], "after-last-public-drop", None, None).await;
            frame(&remounted).await;
            release_send.send(()).unwrap();
            if tokio::time::timeout(Duration::from_secs(2), finished_receive)
                .await
                .is_err()
            {
                failures.push(format!(
                    "{backend}: dropped watch worker did not retire after callback"
                ));
            }
            if received.try_recv().is_ok() {
                failures.push(format!(
                    "{backend}: queued callback ran after last public drop"
                ));
            }
            session.close().await.unwrap();
        }
        assert!(failures.is_empty(), "{failures:?}");
    });
}
