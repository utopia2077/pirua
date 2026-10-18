mod support;
use pi_durable::*;
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use support::*;
fn document() -> session::DocDefinition {
    session::DocDefinition::new(
        "initialized",
        1,
        session::DocumentSemantics::Conversation {
            history: session::History::Rewindable,
            fork: session::ForkPolicy::AsOf,
        },
        false,
        |_| Ok(json!({"value":0,"initializations":0})),
        |_, _, _| Ok(false),
    )
    .unwrap()
}
#[test]
fn raw_root_new_and_fork_initialization_share_atomic_transaction_and_follow_copies() {
    runtime().block_on(async {
        for (_, store, _temp) in stores() {
            let calls = Arc::new(AtomicU64::new(0));
            let count = calls.clone();
            let session = Session::with_options(
                store.clone(),
                session::SessionOptions {
                    conversation_initializer: Some(Arc::new(move |tx, record| {
                        count.fetch_add(1, Ordering::SeqCst);
                        assert_eq!(tx.staged_conversations().last(), Some(record));
                        assert_eq!(
                            tx.scan_tasks(&session::TaskQuery::default(), 1, None)
                                .unwrap_err()
                                .kind,
                            ErrorKind::ReadAfterWrite
                        );
                        let def = document();
                        let id = tx.doc(
                            &def,
                            def.address(
                                session::Scope::Conversation {
                                    conversation_id: record.id,
                                },
                                None,
                            )?,
                            json!(null),
                        )?;
                        let previous = tx.document_value(id)?["initializations"].as_u64().unwrap();
                        tx.apply(
                            id,
                            session::Op::Set(vec!["initializations".into()], json!(previous + 1)),
                        )?;
                        Ok(())
                    })),
                    ..Default::default()
                },
            )
            .unwrap();
            session
                .commit(|tx| tx.ensure_root_conversation())
                .await
                .unwrap();
            session
                .commit(|tx| tx.ensure_root_conversation())
                .await
                .unwrap();
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            let independent = session
                .commit(|tx| tx.create_conversation(session::ConversationOwnership::Ownerless))
                .await
                .unwrap();
            assert_eq!(calls.load(Ordering::SeqCst), 2);
            let at = session
                .commit(|tx| {
                    let def = document();
                    let id = tx.doc(
                        &def,
                        def.address(
                            session::Scope::Conversation {
                                conversation_id: session::ROOT_CONVERSATION_ID,
                            },
                            None,
                        )?,
                        json!(null),
                    )?;
                    tx.apply(id, session::Op::Set(vec!["value".into()], json!(9)))?;
                    tx.append_entry(
                        session::ROOT_CONVERSATION_ID,
                        session::EntryDraft::new("fork-point"),
                    )
                })
                .await
                .unwrap();
            let fork = session
                .commit(move |tx| {
                    tx.fork_conversation(
                        session::ROOT_CONVERSATION_ID,
                        at.id,
                        session::ConversationOwnership::Ownerless,
                    )
                })
                .await
                .unwrap();
            assert_eq!(calls.load(Ordering::SeqCst), 3);
            for (id, value, initializations) in [
                (session::ROOT_CONVERSATION_ID, 9, 1),
                (independent.id, 0, 1),
                (fork.id, 9, 2),
            ] {
                let address = document()
                    .address(
                        session::Scope::Conversation {
                            conversation_id: id,
                        },
                        None,
                    )
                    .unwrap();
                let record = store
                    .find_document(&address, session::DocumentPoint::Current)
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    store
                        .document(record.identity.id, session::DocumentPoint::Current)
                        .unwrap()
                        .unwrap()
                        .value,
                    json!({"value":value,"initializations":initializations})
                );
            }
            session.close().await.unwrap();
        }
    });
}
#[test]
fn initializer_error_rolls_back_conversation_and_documents_without_poisoning_session() {
    runtime().block_on(async {
        for (_, store, _temp) in stores() {
            let reject = Arc::new(AtomicBool::new(true));
            let failing = reject.clone();
            let session = Session::with_options(
                store.clone(),
                session::SessionOptions {
                    conversation_initializer: Some(Arc::new(move |tx, record| {
                        let def = document();
                        tx.doc(
                            &def,
                            def.address(
                                session::Scope::Conversation {
                                    conversation_id: record.id,
                                },
                                None,
                            )?,
                            json!(null),
                        )?;
                        if failing.load(Ordering::SeqCst) {
                            Err(Error::validation("initializer rejected"))
                        } else {
                            Ok(())
                        }
                    })),
                    ..Default::default()
                },
            )
            .unwrap();
            assert!(
                session
                    .commit(|tx| tx.ensure_root_conversation())
                    .await
                    .is_err()
            );
            assert!(session.failure().is_none());
            assert!(
                store
                    .conversation(session::ROOT_CONVERSATION_ID)
                    .unwrap()
                    .is_none()
            );
            assert!(
                store
                    .scan_documents(
                        &session::DocumentQuery {
                            scope: session::Scope::Conversation {
                                conversation_id: session::ROOT_CONVERSATION_ID
                            },
                            at: session::DocumentPoint::Current,
                            kind: None
                        },
                        10,
                        None
                    )
                    .unwrap()
                    .items
                    .is_empty()
            );
            reject.store(false, Ordering::SeqCst);
            session
                .commit(|tx| tx.ensure_root_conversation())
                .await
                .unwrap();
            assert!(
                store
                    .conversation(session::ROOT_CONVERSATION_ID)
                    .unwrap()
                    .is_some()
            );
            session.close().await.unwrap();
        }
    });
}
