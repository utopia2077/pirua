mod support;

use pi_durable_session::*;
use serde_json::{Value, json};
use support::*;

// A valid fractional millisecond value observed at the Session publication boundary.
// Its JSON encoding must recover the same binary value, including after reopen.
const FRACTION: f64 = 1_792_695_896_535.238_5;

fn assert_number(value: &Value, context: &str) {
    assert_eq!(
        value.as_f64().unwrap().to_bits(),
        FRACTION.to_bits(),
        "{context}: {value} must preserve {FRACTION}"
    );
}

#[test]
fn fractional_records_and_documents_survive_publication_storage_and_reopen() {
    runtime().block_on(async {
        for (name, storage, temporary) in backends() {
            let session = Session::with_clock(storage.clone(), || FRACTION).unwrap();
            session
                .commit(|tx| tx.ensure_root_conversation())
                .await
                .unwrap();
            let mut publications = session.subscribe_commits().await.unwrap();
            let (task, document) = session
                .commit(|tx| {
                    let task = tx.create_task(TaskCreate {
                        kind: "fractional-record".into(),
                        version: 1,
                        input: json!({"number": FRACTION}),
                        checkpoint: json!({"phase": "start", "number": FRACTION}),
                        conversation_id: Some(ROOT_CONVERSATION_ID),
                        ownership: TaskOwnership::Conversation,
                        background: false,
                        abandon_on_restart: false,
                        trace: None,
                        links: None,
                    })?;
                    let definition = DocDefinition::new(
                        "fractional-document",
                        1,
                        DocumentSemantics::Session,
                        false,
                        |_| Ok(json!({"number": FRACTION})),
                        |_, _, _| Ok(false),
                    )?;
                    let document = tx.doc(
                        &definition,
                        definition.address(Scope::Session, None)?,
                        Value::Null,
                    )?;
                    Ok((task, document))
                })
                .await
                .unwrap();
            let publication = publications.next().await.unwrap();
            let created = publication
                .changes
                .iter()
                .find_map(|change| match change {
                    CommitChange::Table(StorageWrite::Task { value }) => Some(value),
                    _ => None,
                })
                .unwrap();
            assert_number(&created.input["number"], "published input");
            assert_eq!(
                session.task(task).await.unwrap().as_ref(),
                Some(created),
                "{name}"
            );
            assert_number(
                &storage
                    .document(document, DocumentPoint::Current)
                    .unwrap()
                    .unwrap()
                    .value["number"],
                name,
            );

            session
                .commit(move |tx| {
                    let mut record = tx.task(task)?.unwrap();
                    record.state = TaskState::Running {
                        checkpoint: json!({"phase": "running", "number": FRACTION}),
                    };
                    tx.replace_task(record)
                })
                .await
                .unwrap();
            let publication = publications.next().await.unwrap();
            let running = publication
                .changes
                .iter()
                .find_map(|change| match change {
                    CommitChange::Table(StorageWrite::Task { value }) => Some(value),
                    _ => None,
                })
                .unwrap();
            assert_eq!(running.started_at.unwrap().to_bits(), FRACTION.to_bits());
            assert_eq!(
                session.task(task).await.unwrap().as_ref(),
                Some(running),
                "{name}"
            );
            session.close().await.unwrap();

            if let Some(temporary) = temporary {
                let reopened = SqliteStorage::open(&temporary.path).unwrap();
                assert_eq!(reopened.task(task).unwrap().as_ref(), Some(running));
                assert_number(
                    &reopened
                        .document(document, DocumentPoint::Current)
                        .unwrap()
                        .unwrap()
                        .value["number"],
                    "reopened document",
                );
                reopened.close().unwrap();
            }
        }
    });
}
