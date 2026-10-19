use super::*;
use pi_durable_session::{
    ConversationOwnership, DocDefinition, DocumentSemantics, EntryDraft, EntryHead, ForkPolicy,
    History, MemoryStorage, ROOT_CONVERSATION_ID, SqliteStorage, Storage, SubmissionCreate,
    SubmissionState, SubmissionType, TaskCreate, TaskOutcome, TaskOwnership, TaskState,
};
use serde_json::json;
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
}
struct Temp(PathBuf);
impl Drop for Temp {
    fn drop(&mut self) {
        for path in [
            self.0.clone(),
            self.0.with_extension("sqlite-wal"),
            self.0.with_extension("sqlite-shm"),
        ] {
            let _ = std::fs::remove_file(path);
        }
    }
}
fn backends() -> Vec<(&'static str, Arc<dyn Storage>, Option<Temp>)> {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let temp = Temp(std::env::temp_dir().join(format!(
        "pi-durable-events-view-{}-{}.sqlite",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )));
    vec![
        ("memory", Arc::new(MemoryStorage::new()), None),
        (
            "sqlite",
            Arc::new(SqliteStorage::open(&temp.0).unwrap()),
            Some(temp),
        ),
    ]
}
async fn root(session: &Session) {
    session
        .commit(|tx| tx.ensure_root_conversation())
        .await
        .unwrap();
}
fn task_create(
    conversation: Option<ConversationId>,
    owner: TaskOwnership,
    kind: &str,
) -> TaskCreate {
    TaskCreate {
        kind: kind.into(),
        version: 1,
        input: json!({}),
        checkpoint: json!({"phase":"start"}),
        conversation_id: conversation,
        ownership: owner,
        background: false,
        abandon_on_restart: false,
        trace: None,
        links: None,
    }
}
async fn create_task(session: &Session, conversation: ConversationId, kind: &str) -> TaskId {
    let create = task_create(Some(conversation), TaskOwnership::Conversation, kind);
    session
        .commit(move |tx| tx.create_task(create))
        .await
        .unwrap()
}
async fn transition(session: &Session, id: TaskId, state: TaskState) {
    session
        .commit(move |tx| {
            let mut task = tx.task(id)?.unwrap();
            task.state = state;
            tx.replace_task(task)
        })
        .await
        .unwrap();
}
async fn next(attachment: &ConversationCommitAttachment) -> Arc<ConversationCommitFrame> {
    tokio::time::timeout(Duration::from_secs(5), attachment.next())
        .await
        .unwrap()
        .unwrap()
}
async fn no_frame(attachment: &ConversationCommitAttachment) {
    assert!(
        tokio::time::timeout(Duration::from_millis(20), attachment.next())
            .await
            .is_err()
    );
}
async fn append(session: &Session, kind: &str, head: Option<EntryHead>) -> EntryRecord {
    let draft = EntryDraft {
        kind: kind.into(),
        model: None,
        data: None,
        head,
        edits: None,
    };
    session
        .commit(move |tx| tx.append_entry(ROOT_CONVERSATION_ID, draft))
        .await
        .unwrap()
}

#[test]
fn reused_mount_attachment_captures_current_task_and_view_in_one_driver_command() {
    runtime().block_on(async {
        for (backend, store, _temp) in backends() {
            let session = Session::open(store).unwrap();
            root(&session).await;
            let generation = create_task(&session, ROOT_CONVERSATION_ID, "pi.generation").await;
            let views = ConversationViews::new(session.clone());
            let state = views.state(ROOT_CONVERSATION_ID).await.unwrap();
            let original = state.value().await;
            session
                .commit(move |tx| {
                    let mut task = tx.task(generation)?.unwrap();
                    let def = DocDefinition::new(
                        "pi.live",
                        1,
                        DocumentSemantics::Conversation {
                            history: History::Latest,
                            fork: ForkPolicy::Initial,
                        },
                        false,
                        |_| Ok(json!({"n":1})),
                        |_, _, _| Ok(false),
                    )?;
                    tx.doc(
                        &def,
                        DocumentAddress {
                            kind: "pi.live".into(),
                            scope: Scope::Conversation {
                                conversation_id: ROOT_CONVERSATION_ID,
                            },
                            key: None,
                        },
                        Value::Null,
                    )?;
                    task.state = TaskState::Completing {
                        outcome: TaskOutcome::Completed {
                            result: json!("held"),
                        },
                    };
                    tx.replace_task(task)?;
                    tx.append_entry(ROOT_CONVERSATION_ID, EntryDraft::new("current"))?;
                    Ok(())
                })
                .await
                .unwrap();
            let attachment = views.attach_commits(ROOT_CONVERSATION_ID).await.unwrap();
            assert!(original.docs.is_empty());
            assert!(original.entries.is_empty());
            assert_eq!(
                attachment.baseline.value.docs["pi.live"],
                json!({"n":1}),
                "{backend}"
            );
            assert_eq!(attachment.baseline.value.entries[0].kind, "current");
            assert_eq!(attachment.baseline.tasks.len(), 1);
            assert!(matches!(
                attachment.baseline.tasks[&generation].state,
                TaskState::Completing { .. }
            ));
            no_frame(&attachment).await;
            let mut publications = session.subscribe_commits().await.unwrap();
            session
                .commit(move |tx| {
                    let mut task = tx.task(generation)?.unwrap();
                    task.state = TaskState::Terminal {
                        outcome: TaskOutcome::Completed {
                            result: json!("held"),
                        },
                    };
                    tx.replace_task(task)?;
                    tx.append_entry(ROOT_CONVERSATION_ID, EntryDraft::new("next"))?;
                    Ok(())
                })
                .await
                .unwrap();
            let frame = next(&attachment).await;
            assert!(Arc::ptr_eq(&frame.before, &attachment.baseline.value));
            assert_eq!(frame.after.entries.len(), 2);
            assert_eq!(frame.ops.len(), 1);
            assert_eq!(frame.publication, publications.next().await.unwrap());
            assert_eq!(
                pi_durable_session::apply(
                    &serde_json::to_value(frame.before.as_ref()).unwrap(),
                    &frame.ops
                )
                .unwrap(),
                serde_json::to_value(frame.after.as_ref()).unwrap()
            );
            assert!(matches!(
                attachment.baseline.tasks[&generation].state,
                TaskState::Completing { .. }
            ));
            let current = views.attach_commits(ROOT_CONVERSATION_ID).await.unwrap();
            assert_eq!(current.baseline.value, frame.after);
            assert!(matches!(
                current.baseline.tasks[&generation].state,
                TaskState::Terminal { .. }
            ));
            assert_eq!(current.stop(), WatchEnd::Stopped);
            assert_eq!(current.closed().await, WatchEnd::Stopped);
            session.close().await.unwrap();
        }
    });
}

#[test]
fn complete_task_submission_and_nested_document_publications_reach_only_attachments() {
    runtime().block_on(async {
        for (backend, store, _temp) in backends() {
            let session = Session::open(store).unwrap(); root(&session).await;
            let parent = create_task(&session,ROOT_CONVERSATION_ID,"host.parent").await;
            let child = session.commit(move |tx|tx.create_task(task_create(None,TaskOwnership::Task(parent),"host.child"))).await.unwrap();
            let foreign = session.commit(|tx|tx.create_conversation(ConversationOwnership::Ownerless)).await.unwrap();
            let other = create_task(&session,foreign.id,"host.foreign").await;
            let views = ConversationViews::new(session.clone());
            let attachment = views.attach_commits(ROOT_CONVERSATION_ID).await.unwrap();
            let ordinary = views.watch(ROOT_CONVERSATION_ID).await.unwrap();
            assert_eq!(attachment.baseline.tasks.len(),2); assert!(!attachment.baseline.tasks.contains_key(&other));
            let mut publications = session.subscribe_commits().await.unwrap();
            transition(&session,child,TaskState::Running {checkpoint:json!({"phase":"execute"})}).await;
            let task = next(&attachment).await;
            assert!(task.ops.is_empty()); assert!(Arc::ptr_eq(&task.before,&task.after));
            assert_eq!(task.publication,publications.next().await.unwrap());
            assert!(matches!(&task.publication.changes[0],CommitChange::Table(StorageWrite::Task {value}) if value.id==child));
            let task_only_current=views.attach_commits(ROOT_CONVERSATION_ID).await.unwrap();
            assert!(matches!(task_only_current.baseline.tasks[&child].state,TaskState::Running {..}));
            assert_eq!(task_only_current.baseline.value,task.after);
            task_only_current.stop();
            session.commit(|tx|tx.create_submission(SubmissionCreate {conversation_id:ROOT_CONVERSATION_ID,submission_type:SubmissionType::Input,request_id:Some("event-only".into()),trace:None,state:SubmissionState::Queued})).await.unwrap();
            let submission = next(&attachment).await;
            assert!(submission.ops.is_empty()); assert_eq!(submission.before,task.after);
            assert_eq!(submission.publication,publications.next().await.unwrap());
            assert!(matches!(&submission.publication.changes[0],CommitChange::Table(StorageWrite::Submission {..})));
            let definition = DocDefinition::new("pi.nested-call",1,DocumentSemantics::Task,true,move |_|Ok(json!({"taskId":child,"result":{"output":"captured"}})),|_,_,_|Ok(false)).unwrap();
            let address = DocumentAddress {kind:"pi.nested-call".into(),scope:Scope::Task {task_id:child},key:Some("nested".into())};
            let d = definition.clone(); let a = address.clone();
            session.commit(move |tx|tx.doc(&d,a,Value::Null)).await.unwrap();
            let nested = next(&attachment).await;
            assert!(nested.ops.is_empty()); assert_eq!(nested.publication,publications.next().await.unwrap());
            assert!(matches!(&nested.publication.changes[0],CommitChange::Document {conversation_id:Some(id),value,..} if *id==ROOT_CONVERSATION_ID && value["result"]["output"]=="captured"));
            session.commit(move |tx|tx.retire_doc(&definition,address)).await.unwrap();
            let retired = next(&attachment).await;
            assert!(retired.ops.is_empty()); assert_eq!(retired.publication,publications.next().await.unwrap());
            assert!(matches!(&nested.publication.changes[0],CommitChange::Document {value,..} if value["result"]["output"]=="captured"));
            session.commit(move |tx| {
                let mut task=tx.task(child)?.unwrap();task.state=TaskState::Sleeping {checkpoint:json!({"phase":"later"}),until:5000.0};tx.replace_task(task)?;
                tx.create_submission(SubmissionCreate {conversation_id:foreign.id,submission_type:SubmissionType::Input,request_id:None,trace:None,state:SubmissionState::Queued})?;
                Ok(())
            }).await.unwrap();
            let complete = next(&attachment).await;
            assert!(complete.ops.is_empty()); assert_eq!(complete.publication,publications.next().await.unwrap());
            assert_eq!(complete.publication.changes.len(),2,"{backend}: complete mixed-conversation publication");
            let seqs=[task.publication.seq,submission.publication.seq,nested.publication.seq,retired.publication.seq,complete.publication.seq];
            assert!(seqs.windows(2).all(|seq|seq[0]<seq[1]));
            transition(&session,other,TaskState::Running {checkpoint:json!({})}).await;
            no_frame(&attachment).await;
            assert!(tokio::time::timeout(Duration::from_millis(20),ordinary.next()).await.is_err());
            assert!(matches!(attachment.baseline.tasks[&child].state,TaskState::Pending {..}));
            session.close().await.unwrap();
            assert_eq!(attachment.closed().await,WatchEnd::SessionClosed);assert!(attachment.next().await.is_none());
        }
    });
}

#[test]
fn attachment_public_custody_controls_raw_head_remount_and_first_end_reason() {
    runtime().block_on(async {
        for (_, store, _temp) in backends() {
            let session = Session::open(store).unwrap();
            root(&session).await;
            let first = append(&session, "first", None).await;
            let second = append(&session, "second", None).await;
            let views = ConversationViews::new(session.clone());
            let original = views.attach_commits(ROOT_CONVERSATION_ID).await.unwrap();
            let alias = original.clone();
            append(&session, "cut", Some(EntryHead::Entry(second.id))).await;
            next(&original).await;
            append(&session, "backward", Some(EntryHead::Entry(first.id))).await;
            let backward = next(&original).await;
            assert!(
                !backward
                    .after
                    .entries
                    .iter()
                    .any(|entry| entry.id == first.id)
            );
            drop(original);
            let shared = views.attach_commits(ROOT_CONVERSATION_ID).await.unwrap();
            assert!(
                !shared
                    .baseline
                    .value
                    .entries
                    .iter()
                    .any(|entry| entry.id == first.id)
            );
            assert_eq!(shared.cancel(), WatchEnd::Cancelled);
            assert_eq!(shared.stop(), WatchEnd::Cancelled);
            assert_eq!(shared.end(), Some(WatchEnd::Cancelled));
            assert!(shared.next().await.is_none());
            drop(alias);
            let fresh = views.attach_commits(ROOT_CONVERSATION_ID).await.unwrap();
            assert!(
                fresh
                    .baseline
                    .value
                    .entries
                    .iter()
                    .any(|entry| entry.id == first.id)
            );
            assert_eq!(shared.closed().await, WatchEnd::Cancelled);
            assert_eq!(shared.baseline.value, backward.after);
            session.close().await.unwrap();
            assert_eq!(fresh.closed().await, WatchEnd::SessionClosed);
            assert!(views.attach_commits(ROOT_CONVERSATION_ID).await.is_err());
        }
    });
}

#[test]
fn close_seals_attachments_and_pending_acquisition_before_admitted_publication() {
    runtime().block_on(async {
        for (_,store,_temp) in backends() {
            let session=Session::open(store).unwrap();root(&session).await;
            let views=ConversationViews::new(session.clone());
            let attachment=views.attach_commits(ROOT_CONVERSATION_ID).await.unwrap();
            let mut publications=session.subscribe_commits().await.unwrap();
            let gate=Arc::new((Mutex::new((false,false)),std::sync::Condvar::new()));
            let worker_gate=gate.clone();let writer_session=session.clone();
            let writer=tokio::spawn(async move {writer_session.commit(move |tx| {
                let (lock,condition)=&*worker_gate;let mut gate=lock.lock().unwrap();gate.0=true;condition.notify_all();while !gate.1 {gate=condition.wait(gate).unwrap();}drop(gate);
                tx.append_entry(ROOT_CONVERSATION_ID,EntryDraft::new("admitted-after-seal"))
            }).await});
            {let (lock,condition)=&*gate;let mut entered=lock.lock().unwrap();while !entered.0 {entered=condition.wait(entered).unwrap();}}
            let mut acquiring=Box::pin(views.attach_commits(ROOT_CONVERSATION_ID));
            std::future::poll_fn(|context| {assert!(std::future::Future::poll(acquiring.as_mut(),context).is_pending());std::task::Poll::Ready(())}).await;
            let mut closing=Box::pin(session.close());
            std::future::poll_fn(|context| {assert!(std::future::Future::poll(closing.as_mut(),context).is_pending());std::task::Poll::Ready(())}).await;
            assert_eq!(tokio::time::timeout(Duration::from_secs(5),attachment.closed()).await.unwrap(),WatchEnd::SessionClosed);
            assert!(attachment.next().await.is_none());assert!(attachment.baseline.value.entries.is_empty());
            {let (lock,condition)=&*gate;lock.lock().unwrap().1=true;condition.notify_all();}
            writer.await.unwrap().unwrap();assert!(acquiring.await.is_err());closing.await.unwrap();
            let complete=publications.next().await.unwrap();assert!(matches!(&complete.changes[0],CommitChange::Table(StorageWrite::Entry {value}) if value.kind=="admitted-after-seal"));
            assert!(attachment.next().await.is_none());assert!(attachment.baseline.value.entries.is_empty());
        }
    });
}
