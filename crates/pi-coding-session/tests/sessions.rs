use std::{path::PathBuf, sync::Arc};

use pi_coding_session::{
    CompactionOptions, ContentReplacement, EntryContent, Error, MemoryStore, SessionCommit,
    SessionEntry, SessionHeader, SessionManager, SessionSnapshot, SessionStore, SqliteStore,
};
use pi_sqlite::{Database, params};
use serde_json::{Value, json};

fn stores() -> Vec<Arc<dyn SessionStore>> {
    vec![
        Arc::new(MemoryStore::new()),
        Arc::new(SqliteStore::from_database(Database::memory().unwrap()).unwrap()),
    ]
}

fn header(id: &str) -> SessionHeader {
    SessionHeader {
        id: id.to_owned(),
        cwd: "/work/project".to_owned(),
        timestamp: 100,
        parent_session: None,
    }
}

fn user(content: &str) -> Value {
    json!({"role":"user", "content":content, "timestamp":110})
}
fn assistant(content: &str) -> Value {
    json!({"role":"assistant", "content":[{"type":"text","text":content}],
        "provider":"openai", "model":"test-model", "timestamp":120})
}

#[test]
fn detached_projection_rejects_cycles_and_missing_selected_entries() {
    let mut snapshot = SessionSnapshot {
        header: header("invalid"),
        revision: 0,
        leaf_id: Some("cycle".into()),
        entries: vec![SessionEntry {
            id: "cycle".into(),
            parent_id: Some("cycle".into()),
            timestamp: 100,
            content: EntryContent::Message {
                message: user("cycle"),
            },
        }],
    };
    assert!(pi_coding_session::project(&snapshot).is_err());
    assert!(pi_coding_session::build_context_entries(&snapshot).is_err());
    snapshot.entries.clear();
    assert!(pi_coding_session::project(&snapshot).is_err());
}

#[test]
fn forks_preserve_resolved_label_order_and_original_timestamps() {
    for store in stores() {
        let mut session = SessionManager::create(store.clone(), "/work").unwrap();
        let root = session.append_message(user("root")).unwrap();
        let child = session.append_message(user("child")).unwrap();
        session
            .append_at(
                EntryContent::Label {
                    target_id: child.clone(),
                    label: Some("child".into()),
                },
                1234,
            )
            .unwrap();
        session
            .append_at(
                EntryContent::Label {
                    target_id: root.clone(),
                    label: Some("root".into()),
                },
                2345,
            )
            .unwrap();
        session
            .append_at(
                EntryContent::Label {
                    target_id: child.clone(),
                    label: Some("renamed".into()),
                },
                3456,
            )
            .unwrap();
        let labels = |fork: &SessionManager| {
            fork.entries()
                .iter()
                .filter_map(|entry| {
                    if let EntryContent::Label {
                        target_id,
                        label: Some(label),
                    } = &entry.content
                    {
                        Some((target_id.clone(), label.clone(), entry.timestamp))
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>()
        };
        let fork = session.fork(session.leaf_id().unwrap()).unwrap();
        assert_eq!(
            labels(&fork),
            vec![
                (child.clone(), "renamed".into(), 3456),
                (root.clone(), "root".into(), 2345)
            ]
        );
        session
            .append_at(
                EntryContent::Label {
                    target_id: child.clone(),
                    label: None,
                },
                4567,
            )
            .unwrap();
        session
            .append_at(
                EntryContent::Label {
                    target_id: child.clone(),
                    label: Some("readded".into()),
                },
                5678,
            )
            .unwrap();
        let fork = session.fork(session.leaf_id().unwrap()).unwrap();
        let reopened = SessionManager::open(store, &fork.header().id).unwrap();
        assert_eq!(
            labels(&reopened),
            vec![(root, "root".into(), 2345), (child, "readded".into(), 5678)]
        );
    }
}

#[test]
fn memory_and_sqlite_preserve_messages_metadata_and_selected_branch() {
    for store in stores() {
        let mut session =
            SessionManager::create_with_header(store.clone(), header("conversation")).unwrap();
        session.set_model("deepseek", "deepseek-chat").unwrap();
        session.set_thinking_level("high").unwrap();
        let first = session.append_message(user("question")).unwrap();
        let reply = session.append_message(assistant("answer")).unwrap();
        session.set_name("  Project\r\nreview\n ").unwrap();
        session
            .set_label(&reply, Some("checkpoint".to_owned()))
            .unwrap();
        session.branch(Some(&first)).unwrap();
        let before = session.snapshot().clone();
        let reopened = SessionManager::open(store.clone(), "conversation").unwrap();
        assert_eq!(*reopened.snapshot(), before);
        assert_eq!(reopened.projection().messages, vec![user("question")]);
        assert_eq!(reopened.projection().thinking_level, "high");
        assert_eq!(reopened.projection().model.unwrap().provider, "deepseek");
        assert_eq!(reopened.label(&reply), Some("checkpoint"));
        let list = store.list(Some("/work/project")).unwrap();
        assert_eq!(list[0].name.as_deref(), Some("Project review"));
        assert!(store.list(Some("/other")).unwrap().is_empty());
    }
}

#[test]
fn context_edits_are_branch_local_and_keep_source_provenance() {
    for store in stores() {
        let mut session = SessionManager::create(store, "/work").unwrap();
        let input = session.append_message(user("secret")).unwrap();
        let output = session.append_message(assistant("original")).unwrap();
        session.edit_context(&input, None).unwrap();
        session
            .edit_context(
                &output,
                Some(ContentReplacement {
                    content: json!("redacted"),
                }),
            )
            .unwrap();
        let projected = session.projection();
        assert_eq!(projected.messages.len(), 1);
        assert_eq!(
            projected.messages[0]["content"],
            json!([{"type":"text","text":"redacted"}])
        );
        assert_eq!(projected.entries[0].source_entry.id, input);
        assert!(projected.entries[0].messages.is_empty());
        assert_eq!(
            session.entries()[0].content,
            EntryContent::Message {
                message: user("secret")
            }
        );
        session.branch(Some(&output)).unwrap();
        assert_eq!(
            session.projection().messages,
            vec![user("secret"), assistant("original")]
        );
        let sibling = session.append_message(user("sibling")).unwrap();
        session.branch(Some(&input)).unwrap();
        assert!(session.edit_context(&sibling, None).is_err());
    }
}

#[test]
fn newest_compaction_keeps_tail_and_its_system_checkpoint_only() {
    for store in stores() {
        let mut session = SessionManager::create(store, "/work").unwrap();
        session
            .append_message(json!({"role":"system","content":"initial","timestamp":101}))
            .unwrap();
        session.append_message(user("old")).unwrap();
        let tail = session.append_message(user("keep")).unwrap();
        session
            .append_message(json!({"role":"system","content":"patch","timestamp":102}))
            .unwrap();
        session.append_message(assistant("tail reply")).unwrap();
        let system = json!({"role":"system","content":"folded","toolsAdded":[],"timestamp":130});
        let compaction = session
            .append_compaction(CompactionOptions {
                summary: "first summary".to_owned(),
                first_kept_entry_id: Some(tail),
                tokens_before: 1000,
                system_message: Some(system.clone()),
                ..Default::default()
            })
            .unwrap();
        let projected = session.projection();
        assert_eq!(projected.messages[0], system);
        assert_eq!(projected.messages[1]["role"], "compactionSummary");
        assert_eq!(projected.messages[2], user("keep"));
        assert_eq!(projected.messages[3], assistant("tail reply"));
        assert_eq!(projected.messages.len(), 4);
        session.append_message(user("after first summary")).unwrap();
        session
            .append_compaction(CompactionOptions {
                summary: "second summary".to_owned(),
                first_kept_entry_id: Some(compaction),
                system_message: Some(system),
                ..Default::default()
            })
            .unwrap();
        let messages = session.projection().messages;
        assert_eq!(
            messages
                .iter()
                .filter(|m| m["role"] == "compactionSummary")
                .count(),
            1
        );
        assert_eq!(messages.last().unwrap(), &user("after first summary"));
        assert_eq!(messages.len(), 3);
    }
}

#[test]
fn compaction_without_retained_tail_uses_its_own_entry_id() {
    for store in stores() {
        let mut session = SessionManager::create(store, "/work").unwrap();
        session.append_message(user("old")).unwrap();
        let id = session
            .append_compaction(CompactionOptions {
                summary: "summary".to_owned(),
                ..Default::default()
            })
            .unwrap();
        let EntryContent::Compaction {
            first_kept_entry_id,
            ..
        } = &session.get_entry(&id).unwrap().content
        else {
            panic!()
        };
        assert_eq!(first_kept_entry_id, &id);
        assert_eq!(session.projection().messages.len(), 1);
    }
}

#[test]
fn custom_messages_and_branch_summary_project_but_state_entries_do_not() {
    for store in stores() {
        let mut session = SessionManager::create(store, "/work").unwrap();
        let root = session.append_message(user("root")).unwrap();
        let abandoned = session.append_message(assistant("discarded")).unwrap();
        session
            .branch_with_summary(Some(&root), "other branch".to_owned(), None, None)
            .unwrap();
        session
            .append(EntryContent::Custom {
                custom_type: "codemode-store".to_owned(),
                data: Some(json!({"x":1})),
            })
            .unwrap();
        session
            .append(EntryContent::CustomMessage {
                custom_type: "note".to_owned(),
                content: json!("note"),
                display: false,
                details: None,
            })
            .unwrap();
        let messages = session.projection().messages;
        assert_eq!(messages.len(), 3);
        assert_eq!(messages[1]["fromId"], abandoned);
        assert_eq!(messages[2]["role"], "custom");
        assert_eq!(messages[2]["content"], "note");
    }
}

#[test]
fn stale_writers_cannot_publish_or_lose_another_managers_append() {
    for store in stores() {
        let mut first =
            SessionManager::create_with_header(store.clone(), header("shared")).unwrap();
        let mut stale = SessionManager::open(store.clone(), "shared").unwrap();
        first.append_message(user("accepted")).unwrap();
        let stale_before = stale.snapshot().clone();
        assert!(matches!(
            stale.append_message(user("lost")),
            Err(Error::Conflict {
                expected: 0,
                actual: 1
            })
        ));
        assert_eq!(stale.snapshot(), &stale_before);
        assert_eq!(store.load("shared").unwrap().entries.len(), 1);
        stale.reload().unwrap();
        stale.append_message(user("after reload")).unwrap();
        assert_eq!(store.load("shared").unwrap().entries.len(), 2);
    }
}

#[test]
fn invalid_atomic_batch_leaves_store_and_manager_unchanged() {
    for store in stores() {
        let mut session =
            SessionManager::create_with_header(store.clone(), header("atomic")).unwrap();
        let before = session.snapshot().clone();
        let valid = SessionEntry {
            id: "first".to_owned(),
            parent_id: None,
            timestamp: 1,
            content: EntryContent::Message {
                message: user("one"),
            },
        };
        let invalid = SessionEntry {
            id: "second".to_owned(),
            parent_id: Some("missing".to_owned()),
            timestamp: 2,
            content: EntryContent::Message {
                message: user("two"),
            },
        };
        assert!(
            session
                .commit(vec![valid, invalid], Some("second".to_owned()))
                .is_err()
        );
        assert_eq!(session.snapshot(), &before);
        assert_eq!(store.load("atomic").unwrap(), before);
    }
}

#[test]
fn sqlite_failure_after_first_insert_rolls_back_records_and_leaf() {
    let database = Database::memory().unwrap();
    let store = Arc::new(SqliteStore::from_database(database.clone()).unwrap());
    let mut session = SessionManager::create_with_header(store.clone(), header("atomic")).unwrap();
    database
        .read::<_, pi_sqlite::Error>(|connection| {
            connection.execute_batch(
                "CREATE TRIGGER reject_second BEFORE INSERT ON pirua_coding_entries
            WHEN NEW.id = 'second' BEGIN SELECT RAISE(ABORT, 'injected storage failure'); END;",
            )?;
            Ok(())
        })
        .unwrap();
    let before = session.snapshot().clone();
    let entries = vec![
        SessionEntry {
            id: "first".to_owned(),
            parent_id: None,
            timestamp: 1,
            content: EntryContent::Message {
                message: user("one"),
            },
        },
        SessionEntry {
            id: "second".to_owned(),
            parent_id: Some("first".to_owned()),
            timestamp: 2,
            content: EntryContent::Message {
                message: user("two"),
            },
        },
    ];
    assert!(session.commit(entries, Some("second".to_owned())).is_err());
    assert_eq!(session.snapshot(), &before);
    assert_eq!(store.load("atomic").unwrap(), before);
}

#[test]
fn fork_preserves_resolved_labels_and_rechains_label_nodes() {
    for store in stores() {
        let mut session = SessionManager::create(store, "/work").unwrap();
        let root = session.append_message(user("root")).unwrap();
        let label = session
            .set_label(&root, Some("root-label".to_owned()))
            .unwrap();
        session.append_message(user("tail")).unwrap();
        let compaction = session
            .append_compaction(CompactionOptions {
                summary: "summary".to_owned(),
                first_kept_entry_id: Some(label),
                ..Default::default()
            })
            .unwrap();
        let fork = session.fork(&compaction).unwrap();
        assert_ne!(fork.header().id, session.header().id);
        assert_eq!(
            fork.header().parent_session.as_deref(),
            Some(session.header().id.as_str())
        );
        assert_eq!(fork.label(&root), Some("root-label"));
        assert_eq!(fork.projection().messages, session.projection().messages);
        assert_eq!(fork.tree().len(), 1);
    }
}

#[test]
fn storage_rejects_duplicates_cycles_missing_leaf_and_bad_message_envelopes() {
    for store in stores() {
        let snapshot = SessionSnapshot {
            header: header("bad"),
            revision: 0,
            leaf_id: None,
            entries: Vec::new(),
        };
        store.create(&snapshot).unwrap();
        assert!(matches!(
            store.create(&snapshot),
            Err(Error::AlreadyExists(_))
        ));
        assert!(
            store
                .commit(
                    "bad",
                    &SessionCommit {
                        expected_revision: 0,
                        append: vec![],
                        leaf_id: Some("missing".to_owned())
                    }
                )
                .is_err()
        );
        for (parent, message) in [
            (Some("self".to_owned()), user("x")),
            (None, json!({"content":"missing role"})),
        ] {
            assert!(
                store
                    .commit(
                        "bad",
                        &SessionCommit {
                            expected_revision: 0,
                            leaf_id: Some("self".to_owned()),
                            append: vec![SessionEntry {
                                id: "self".to_owned(),
                                parent_id: parent,
                                timestamp: 0,
                                content: EntryContent::Message { message }
                            }]
                        }
                    )
                    .is_err()
            );
        }
        assert_eq!(store.load("bad").unwrap(), snapshot);
    }
}

struct TestFile(PathBuf);

#[test]
fn tree_keeps_equal_timestamp_siblings_in_append_order_and_parent_label_time() {
    for store in stores() {
        let mut session = SessionManager::create(store, "/work").unwrap();
        let root = session
            .append_at(
                EntryContent::Message {
                    message: user("root"),
                },
                10,
            )
            .unwrap();
        session
            .append_at(
                EntryContent::Label {
                    target_id: root.clone(),
                    label: Some("marked".to_owned()),
                },
                20,
            )
            .unwrap();
        session.branch(Some(&root)).unwrap();
        let first = session
            .append_at(
                EntryContent::Message {
                    message: user("first"),
                },
                30,
            )
            .unwrap();
        session.branch(Some(&root)).unwrap();
        let second = session
            .append_at(
                EntryContent::Message {
                    message: user("second"),
                },
                30,
            )
            .unwrap();
        let tree = session.tree();
        assert_eq!(tree[0].label_timestamp, Some(20));
        let children = tree[0]
            .children
            .iter()
            .filter(|child| matches!(child.entry.content, EntryContent::Message { .. }))
            .map(|child| child.entry.id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(children, vec![first.as_str(), second.as_str()]);
    }
}

impl Drop for TestFile {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", self.0.display()));
        }
    }
}

#[test]
fn sqlite_file_reopens_and_rejects_foreign_version_without_conversion() {
    let file = TestFile(std::env::temp_dir().join(
        format!("pirua-coding-session-{}-{}.sqlite", std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()),
    ));
    let before;
    {
        let store = Arc::new(SqliteStore::open(&file.0).unwrap());
        let mut session = SessionManager::create_with_header(store, header("disk")).unwrap();
        session.append_message(user("persisted")).unwrap();
        before = session.snapshot().clone();
    }
    let database = Database::open(&file.0).unwrap();
    let reopened = SqliteStore::from_database(database.clone()).unwrap();
    assert_eq!(reopened.load("disk").unwrap(), before);
    database
        .read::<_, pi_sqlite::Error>(|connection| {
            connection.execute(
                "UPDATE pirua_schema SET version = ?1 WHERE component = 'coding-session'",
                params![2],
            )?;
            Ok(())
        })
        .unwrap();
    assert!(matches!(
        SqliteStore::from_database(database.clone()),
        Err(Error::Database(pi_sqlite::Error::SchemaVersion {
            actual: 2,
            expected: 1,
            ..
        }))
    ));
    database
        .read::<_, pi_sqlite::Error>(|connection| {
            assert_eq!(
                connection.query_row(
                    "SELECT version FROM pirua_schema WHERE component='coding-session'",
                    [],
                    |row| row.get::<_, i64>(0)
                )?,
                2
            );
            assert_eq!(
                connection.query_row("SELECT COUNT(*) FROM pirua_coding_entries", [], |row| row
                    .get::<_, i64>(0))?,
                1
            );
            Ok(())
        })
        .unwrap();
}
