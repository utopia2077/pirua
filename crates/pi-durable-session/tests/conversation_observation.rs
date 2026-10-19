//! Atomic conversation-record observation over real Session/Storage APIs.
mod support;

use pi_durable_session::*;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    future::Future,
    sync::Arc,
    task::{Context, Poll, Waker},
    time::Duration,
};
use support::*;

fn query(conversation: ConversationId, records: bool) -> ObservationQuery {
    ObservationQuery {
        conversation: Some(conversation),
        conversation_records: records,
        ..Default::default()
    }
}
fn task(conversation: ConversationId, kind: &str, ownership: TaskOwnership) -> TaskCreate {
    TaskCreate {
        kind: kind.into(),
        version: 7,
        input: json!({"retained": null}),
        checkpoint: json!({"phase": "initial"}),
        conversation_id: Some(conversation),
        ownership,
        background: false,
        abandon_on_restart: false,
        trace: Some(Value::Null),
        links: None,
    }
}
fn definition(kind: &str, semantics: DocumentSemantics, family: bool) -> DocDefinition {
    DocDefinition::new(
        kind,
        1,
        semantics,
        family,
        |_| Ok(json!({"value": 0})),
        |_, _, _| Ok(false),
    )
    .unwrap()
}
fn pending(subscription: &mut ObservationSubscription) -> bool {
    let mut next = Box::pin(subscription.next());
    let mut context = Context::from_waker(Waker::noop());
    next.as_mut().poll(&mut context).is_pending()
}
async fn next(subscription: &mut ObservationSubscription) -> Arc<ObservationFrame> {
    tokio::time::timeout(Duration::from_secs(3), subscription.next())
        .await
        .expect("conversation publication was not delivered")
        .expect("conversation observation ended unexpectedly")
}
async fn full(subscription: &mut CommitSubscription) -> Arc<CommitPublication> {
    tokio::time::timeout(Duration::from_secs(3), subscription.next())
        .await
        .expect("complete publication was not delivered")
        .expect("complete commit subscription ended unexpectedly")
}

#[test]
fn record_selection_requires_conversation_and_keeps_default_disabled() {
    runtime().block_on(async {
        for (_, storage, _) in backends() {
            let session = Session::with_clock(storage, || 1234567890000.0).unwrap();
            let invalid = session
                .observe(ObservationQuery {
                    conversation_records: true,
                    ..Default::default()
                })
                .await;
            assert!(matches!(
                invalid,
                Err(Error {
                    kind: ErrorKind::Request,
                    ..
                })
            ));
            assert!(session.failure().is_none());
            let (conversation, task_id) = session
                .commit(|tx| {
                    let conversation = tx.create_conversation(ConversationOwnership::Ownerless)?;
                    let task_id = tx.create_task(task(
                        conversation.id,
                        "unregistered.extension",
                        TaskOwnership::Conversation,
                    ))?;
                    Ok((conversation, task_id))
                })
                .await
                .unwrap();
            let mut default = session.observe(ObservationQuery::default()).await.unwrap();
            let mut disabled = session
                .observe(query(conversation.id, false))
                .await
                .unwrap();
            let mut enabled = session.observe(query(conversation.id, true)).await.unwrap();
            assert!(default.baseline.tasks.is_empty());
            assert!(disabled.baseline.tasks.is_empty());
            assert_eq!(enabled.baseline.tasks.len(), 1);
            assert_eq!(
                enabled.baseline.tasks[&task_id],
                session.task(task_id).await.unwrap().unwrap()
            );
            assert_eq!(enabled.baseline.tasks[&task_id].version, 7);
            assert_eq!(enabled.baseline.tasks[&task_id].trace, Some(Value::Null));
            assert!(matches!(
                enabled.baseline.tasks[&task_id].state,
                TaskState::Pending { .. }
            ));
            let updated = session
                .commit(move |tx| {
                    let mut record = tx.task(task_id)?.unwrap();
                    record.state = TaskState::Running {
                        checkpoint: json!({"phase": "next"}),
                    };
                    record.memos = Some(serde_json::Map::from_iter([("memo".into(), Value::Null)]));
                    tx.replace_task(record)?;
                    Ok(tx
                        .staged_tasks()
                        .into_iter()
                        .find(|t| t.id == task_id)
                        .unwrap())
                })
                .await
                .unwrap();
            let frame = next(&mut enabled).await;
            assert_eq!(frame.snapshot.tasks[&task_id], updated);
            assert!(matches!(
                enabled.baseline.tasks[&task_id].state,
                TaskState::Pending { .. }
            ));
            assert!(pending(&mut default));
            assert!(pending(&mut disabled));
            assert!(pending(&mut enabled));
            session.close().await.unwrap();
            assert!(default.next().await.is_none());
            assert!(disabled.next().await.is_none());
            assert!(enabled.next().await.is_none());
        }
    });
}

#[test]
fn baseline_captures_all_task_kinds_states_pages_and_exact_conversation() {
    runtime().block_on(async {
        for (_, storage, _) in backends() {
            let session = Session::with_clock(storage, || 1234567890000.0).unwrap();
            let (conversation, unrelated, ids) = session
                .commit(|tx| {
                    let conversation = tx.create_conversation(ConversationOwnership::Ownerless)?;
                    let unrelated = tx.create_conversation(ConversationOwnership::Ownerless)?;
                    let mut ids = Vec::new();
                    for index in 0..260 {
                        let kind = if index == 0 {
                            "pi.generation"
                        } else {
                            "unknown.extension"
                        };
                        let ownership = if index == 1 {
                            TaskOwnership::Task(ids[0])
                        } else {
                            TaskOwnership::Conversation
                        };
                        ids.push(tx.create_task(task(conversation.id, kind, ownership))?);
                    }
                    tx.create_task(task(unrelated.id, "foreign", TaskOwnership::Conversation))?;
                    Ok((conversation, unrelated, ids))
                })
                .await
                .unwrap();
            let selected = ids.clone();
            session
                .commit(move |tx| {
                    let mut records = selected[..6]
                        .iter()
                        .map(|id| tx.task(*id).map(Option::unwrap))
                        .collect::<Result<Vec<_>>>()?;
                    records[0].state = TaskState::Completing {
                        outcome: TaskOutcome::Completed {
                            result: json!({"held": true}),
                        },
                    };
                    records[1].state = TaskState::Running {
                        checkpoint: json!({"phase": "run"}),
                    };
                    records[2].state = TaskState::Waiting {
                        checkpoint: json!({"phase": "wait"}),
                        on: vec![selected[1]],
                        policy: JoinPolicy::AllSettled,
                    };
                    records[3].state = TaskState::Sleeping {
                        checkpoint: json!({"phase": "sleep"}),
                        until: 1234567.0,
                    };
                    records[4].state = TaskState::Terminal {
                        outcome: TaskOutcome::Aborted {
                            reason: Some("finished".into()),
                            result: None,
                        },
                    };
                    for record in records {
                        tx.replace_task(record)?;
                    }
                    Ok(())
                })
                .await
                .unwrap();
            let observation = session.observe(query(conversation.id, true)).await.unwrap();
            assert_eq!(observation.baseline.tasks.len(), 260);
            assert_eq!(
                observation
                    .baseline
                    .tasks
                    .keys()
                    .copied()
                    .collect::<Vec<_>>(),
                ids
            );
            let mut current = BTreeMap::new();
            let mut cursor = None;
            loop {
                let page = session
                    .scan_tasks(
                        TaskQuery {
                            conversation_id: Some(conversation.id),
                            ..Default::default()
                        },
                        73,
                        cursor,
                    )
                    .await
                    .unwrap();
                current.extend(page.items.into_iter().map(|record| (record.id, record)));
                cursor = page.next;
                if cursor.is_none() {
                    break;
                }
            }
            assert_eq!(observation.baseline.tasks, current);
            for (index, status) in [
                "completing",
                "running",
                "waiting",
                "sleeping",
                "terminal",
                "pending",
            ]
            .iter()
            .enumerate()
            {
                assert_eq!(
                    observation.baseline.tasks[&ids[index]].state.status(),
                    *status
                );
            }
            assert_eq!(observation.baseline.tasks[&ids[1]].owner, Some(ids[0]));
            assert!(
                observation
                    .baseline
                    .tasks
                    .values()
                    .all(|task| task.conversation_id != unrelated.id)
            );
            session.close().await.unwrap();
        }
    });
}

#[test]
fn baseline_and_registration_share_admission_with_documents_entries_and_tasks() {
    runtime().block_on(async {
        for (_, storage, _) in backends() {
            let probe = Probe::new(storage);
            let session = Session::with_clock(probe.clone(), || 1234567890000.0).unwrap();
            let (conversation, task_id) = session
                .commit(|tx| {
                    let conversation = tx.create_conversation(ConversationOwnership::Ownerless)?;
                    let task_id = tx.create_task(task(
                        conversation.id,
                        "pi.generation",
                        TaskOwnership::Conversation,
                    ))?;
                    let d = definition("mounted", DocumentSemantics::Session, false);
                    tx.doc(&d, d.address(Scope::Session, None)?, Value::Null)?;
                    Ok((conversation, task_id))
                })
                .await
                .unwrap();
            let gate = probe.hold();
            let writer = session.clone();
            let commit = tokio::spawn(async move {
                writer
                    .commit(move |tx| {
                        let mut record = tx.task(task_id)?.unwrap();
                        record.state = TaskState::Completing {
                            outcome: TaskOutcome::Completed {
                                result: json!("held at attachment"),
                            },
                        };
                        tx.replace_task(record)?;
                        let d = definition("mounted", DocumentSemantics::Session, false);
                        let document = tx.doc(&d, d.address(Scope::Session, None)?, Value::Null)?;
                        tx.apply(document, Op::Set(vec!["value".into()], json!(7)))?;
                        tx.append_entry(conversation.id, EntryDraft::new("baseline-entry"))
                    })
                    .await
            });
            let entered = gate.clone();
            tokio::task::spawn_blocking(move || entered.wait_entered())
                .await
                .unwrap();
            let mut acquisition = Box::pin(session.observe(ObservationQuery {
                scopes: vec![Scope::Session],
                ..query(conversation.id, true)
            }));
            std::future::poll_fn(|context| {
                assert!(acquisition.as_mut().poll(context).is_pending());
                Poll::Ready(())
            })
            .await;
            gate.release();
            let entry = commit.await.unwrap().unwrap();
            let mut observation = acquisition.await.unwrap();
            let baseline = observation.baseline.clone();
            assert_eq!(baseline.entries, vec![entry]);
            assert_eq!(
                baseline.documents.values().next().unwrap().value["value"],
                7
            );
            assert!(matches!(
                baseline.tasks[&task_id].state,
                TaskState::Completing { .. }
            ));
            assert!(pending(&mut observation));
            session
                .commit(move |tx| {
                    let mut record = tx.task(task_id)?.unwrap();
                    record.state = TaskState::Terminal {
                        outcome: TaskOutcome::Completed {
                            result: json!("done"),
                        },
                    };
                    tx.replace_task(record)
                })
                .await
                .unwrap();
            let terminal = next(&mut observation).await;
            assert_eq!(terminal.publication.seq.get(), 3);
            assert!(terminal.snapshot.tasks[&task_id].state.is_terminal());
            assert!(matches!(
                baseline.tasks[&task_id].state,
                TaskState::Completing { .. }
            ));
            assert_eq!(terminal.snapshot.entries, baseline.entries);
            assert_eq!(terminal.snapshot.documents, baseline.documents);
            assert!(pending(&mut observation));
            session.close().await.unwrap();
        }
    });
}

#[test]
fn task_submission_nested_document_changes_forward_whole_publications_once() {
    runtime().block_on(async {
        for (_, storage, _) in backends() {
            let session = Session::with_clock(storage, || 1234567890000.0).unwrap();
            let (conversation, other, parent, nested, foreign) = session.commit(|tx| {
                let conversation = tx.create_conversation(ConversationOwnership::Ownerless)?;
                let other = tx.create_conversation(ConversationOwnership::Ownerless)?;
                let parent = tx.create_task(task(conversation.id, "parent", TaskOwnership::Conversation))?;
                let nested = tx.create_task(task(conversation.id, "nested", TaskOwnership::Task(parent)))?;
                let foreign = tx.create_task(task(other.id, "foreign", TaskOwnership::Conversation))?;
                Ok((conversation, other, parent, nested, foreign))
            }).await.unwrap();
            let mut selected = session.observe(query(conversation.id, true)).await.unwrap();
            let mut disabled = session.observe(query(conversation.id, false)).await.unwrap();
            let mut commits = session.subscribe_commits().await.unwrap();
            let baseline = selected.baseline.clone();
            assert_eq!(baseline.tasks.len(), 2);
            assert!(baseline.documents.is_empty());

            // A foreign-only task update does not make the selected conversation relevant.
            session.commit(move |tx| {
                let mut record = tx.task(foreign)?.unwrap();
                record.state = TaskState::Running { checkpoint: json!({"phase": "foreign"}) };
                tx.replace_task(record)
            }).await.unwrap();
            full(&mut commits).await;
            assert!(pending(&mut selected));
            assert!(pending(&mut disabled));

            // A single matching record admits the entire mixed-conversation publication.
            let entry = session.commit(move |tx| {
                let mut matching = tx.task(parent)?.unwrap();
                let mut unrelated = tx.task(foreign)?.unwrap();
                matching.state = TaskState::Running { checkpoint: json!({"phase": "parent"}) };
                unrelated.state = TaskState::Running { checkpoint: json!({"phase": "foreign-again"}) };
                tx.replace_task(matching)?;
                tx.replace_task(unrelated)?;
                tx.create_submission(SubmissionCreate {
                    conversation_id: other.id, submission_type: SubmissionType::Input,
                    request_id: Some("unrelated".into()), trace: None, state: SubmissionState::Queued,
                })?;
                let d = definition("session-unselected", DocumentSemantics::Session, false);
                tx.doc(&d, d.address(Scope::Session, None)?, Value::Null)?;
                tx.append_entry(other.id, EntryDraft::new("foreign-entry"))
            }).await.unwrap();
            let publication = full(&mut commits).await;
            let mixed = next(&mut selected).await;
            assert!(Arc::ptr_eq(&publication, &mixed.publication));
            assert_eq!(mixed.publication.changes.len(), 5);
            assert!(mixed.publication.changes.iter().any(|change| matches!(change, CommitChange::Table(StorageWrite::Entry { value }) if value == &entry)));
            assert_eq!(mixed.snapshot.tasks.len(), 2);
            assert_eq!(mixed.snapshot.tasks[&parent], session.task(parent).await.unwrap().unwrap());
            assert!(!mixed.snapshot.tasks.contains_key(&foreign));
            assert!(mixed.snapshot.entries.is_empty());
            assert!(mixed.snapshot.documents.is_empty());
            assert!(pending(&mut selected));
            assert!(pending(&mut disabled));

            let created = session.commit(move |tx| tx.create_task(task(conversation.id, "new.unregistered", TaskOwnership::Task(parent)))).await.unwrap();
            let publication = full(&mut commits).await;
            let created_frame = next(&mut selected).await;
            assert!(Arc::ptr_eq(&publication, &created_frame.publication));
            assert_eq!(created_frame.snapshot.tasks[&created], session.task(created).await.unwrap().unwrap());
            assert_eq!(created_frame.snapshot.tasks.len(), 3);
            assert!(pending(&mut disabled));

            let submission = session.commit(move |tx| tx.create_submission(SubmissionCreate {
                conversation_id: conversation.id, submission_type: SubmissionType::Input,
                request_id: Some("matching".into()), trace: Some(Value::Null), state: SubmissionState::Queued,
            })).await.unwrap();
            let publication = full(&mut commits).await;
            let submitted = next(&mut selected).await;
            assert!(Arc::ptr_eq(&publication, &submitted.publication));
            assert_eq!(submitted.snapshot.tasks, created_frame.snapshot.tasks);
            assert_eq!(submitted.publication.changes, vec![CommitChange::Table(StorageWrite::Submission { value: submission })]);
            assert!(pending(&mut disabled));

            for result in [Value::Null, json!({"status": "completed", "result": {"text": "receipt"}})] {
                let expected = result.clone();
                session.commit(move |tx| {
                    let d = definition("nested.receipt", DocumentSemantics::Task, true);
                    let id = tx.doc(&d, d.address(Scope::Task { task_id: nested }, Some("c7".into()))?, Value::Null)?;
                    tx.apply(id, Op::Set(vec!["taskId".into()], json!(created)))?;
                    tx.apply(id, Op::Set(vec!["result".into()], result))
                }).await.unwrap();
                let publication = full(&mut commits).await;
                let receipt = next(&mut selected).await;
                assert!(Arc::ptr_eq(&publication, &receipt.publication));
                assert_eq!(receipt.publication.changes.len(), 1);
                assert!(matches!(&receipt.publication.changes[0], CommitChange::Document {record, conversation_id: Some(id), value, ..}
                    if *id == conversation.id && record.identity.scope == (Scope::Task {task_id: nested}) && value["result"] == expected));
                assert!(receipt.snapshot.documents.is_empty());
                assert_eq!(receipt.snapshot.tasks, created_frame.snapshot.tasks);
                assert!(pending(&mut selected));
                assert!(pending(&mut disabled));
            }
            session.commit(move |tx| {
                let d = definition("nested.receipt", DocumentSemantics::Task, true);
                tx.retire_doc(&d, d.address(Scope::Task {task_id: nested}, Some("c7".into()))?)
            }).await.unwrap();
            let publication = full(&mut commits).await;
            let retired = next(&mut selected).await;
            assert!(Arc::ptr_eq(&publication, &retired.publication));
            assert!(matches!(&retired.publication.changes[0], CommitChange::Document {value, version: None, ..} if value.is_null()));
            assert!(pending(&mut disabled));

            session.commit(move |tx| {
                let d = definition("unmounted", DocumentSemantics::Conversation {history: History::Latest, fork: ForkPolicy::Initial}, false);
                tx.doc(&d, d.address(Scope::Conversation {conversation_id: conversation.id}, None)?, Value::Null)
            }).await.unwrap();
            let publication = full(&mut commits).await;
            let unmounted = next(&mut selected).await;
            assert!(Arc::ptr_eq(&publication, &unmounted.publication));
            assert!(unmounted.snapshot.documents.is_empty());
            assert!(pending(&mut disabled));

            let entry = session.commit(move |tx| tx.append_entry(conversation.id, EntryDraft::new("own-entry"))).await.unwrap();
            let publication = full(&mut commits).await;
            let selected_entry = next(&mut selected).await;
            let disabled_entry = next(&mut disabled).await;
            assert!(Arc::ptr_eq(&publication, &selected_entry.publication));
            assert!(Arc::ptr_eq(&publication, &disabled_entry.publication));
            assert_eq!(selected_entry.snapshot.entries, vec![entry.clone()]);
            assert_eq!(disabled_entry.snapshot.entries, vec![entry]);
            assert!(disabled_entry.snapshot.tasks.is_empty());
            assert_eq!(selected_entry.snapshot.tasks, created_frame.snapshot.tasks);
            assert!(pending(&mut selected));
            assert!(pending(&mut disabled));
            assert_eq!(baseline.tasks[&parent].state.status(), "pending");
            session.close().await.unwrap();
        }
    });
}

#[test]
fn record_observation_keeps_close_cutoff_while_complete_commits_drain() {
    runtime().block_on(async {
        for (_, storage, _) in backends() {
            let probe = Probe::new(storage);
            let session = Session::with_clock(probe.clone(), || 1234567890000.0).unwrap();
            let (conversation, task_id) = session.commit(|tx| {
                let conversation = tx.create_conversation(ConversationOwnership::Ownerless)?;
                let task_id = tx.create_task(task(conversation.id, "worker", TaskOwnership::Conversation))?;
                Ok((conversation, task_id))
            }).await.unwrap();
            let mut observation = session.observe(query(conversation.id, true)).await.unwrap();
            let mut commits = session.subscribe_commits().await.unwrap();
            let baseline = observation.baseline.clone();
            let gate = probe.hold();
            let writer = session.clone();
            let commit = tokio::spawn(async move {writer.commit(move |tx| {
                let mut record = tx.task(task_id)?.unwrap();
                record.state = TaskState::Running {checkpoint: json!({"phase": "after-seal"})};
                tx.replace_task(record)
            }).await});
            let entered = gate.clone();
            tokio::task::spawn_blocking(move || entered.wait_entered()).await.unwrap();
            let closer = session.clone();
            let close = tokio::spawn(async move {closer.close().await});
            let sealed = tokio::time::timeout(Duration::from_secs(3), observation.next()).await;
            gate.release();
            commit.await.unwrap().unwrap();
            close.await.unwrap().unwrap();
            assert!(sealed.expect("observation did not end at seal").is_none());
            assert_eq!(baseline.tasks[&task_id].state.status(), "pending");
            let publication = full(&mut commits).await;
            assert_eq!(publication.seq.get(), 2);
            assert!(matches!(&publication.changes[0], CommitChange::Table(StorageWrite::Task {value}) if value.id==task_id && value.state.status()=="running"));
            assert!(commits.next().await.is_none());
            assert!(observation.next().await.is_none());
        }
    });
}

#[test]
fn failed_record_commit_never_changes_task_snapshot_or_publishes_candidate() {
    runtime().block_on(async {
        for mode in [1, 2] {
            for (_, storage, temporary) in backends() {
                let probe = Probe::new(storage);
                let session = Session::with_clock(probe.clone(), || 1234567890000.0).unwrap();
                let (conversation, task_id) = session
                    .commit(|tx| {
                        let conversation =
                            tx.create_conversation(ConversationOwnership::Ownerless)?;
                        let task_id = tx.create_task(task(
                            conversation.id,
                            "worker",
                            TaskOwnership::Conversation,
                        ))?;
                        Ok((conversation, task_id))
                    })
                    .await
                    .unwrap();
                let mut observation = session.observe(query(conversation.id, true)).await.unwrap();
                let mut commits = session.subscribe_commits().await.unwrap();
                let baseline = observation.baseline.clone();
                probe.fail(mode);
                assert!(
                    session
                        .commit(move |tx| {
                            let mut record = tx.task(task_id)?.unwrap();
                            record.state = TaskState::Running {
                                checkpoint: json!({"phase": "uncertain"}),
                            };
                            tx.replace_task(record)
                        })
                        .await
                        .is_err()
                );
                assert!(observation.next().await.is_none());
                assert!(commits.next().await.is_none());
                assert_eq!(baseline.tasks[&task_id].state.status(), "pending");
                assert!(session.failure().is_some());
                if let Some(temporary) = temporary {
                    let reopened = SqliteStorage::open(&temporary.path).unwrap();
                    assert_eq!(
                        reopened.task(task_id).unwrap().unwrap().state.status(),
                        if mode == 1 { "pending" } else { "running" }
                    );
                    reopened.close().unwrap();
                }
            }
        }
    });
}
