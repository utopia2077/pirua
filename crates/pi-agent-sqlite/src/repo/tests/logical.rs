use super::support::*;
#[test]
fn actual_five_methods_empty_seed_metadata_and_closed_delete() {
    let d = Dir::new();
    let r = repo(&d.0);
    let root = ready(r.create(options("new"))).unwrap();
    let metadata = root.metadata();
    let initial = observe(&metadata);
    assert!(initial.snapshot.entries.is_empty());
    assert_eq!(initial.snapshot.registers.len(), 2);
    assert_eq!(initial.snapshot.next_seq, Some(n(2)));
    assert_eq!(initial.metadata.created_at, n(500));
    let view = root.view(LaneId::new("main").unwrap());
    let mut forged = metadata.clone();
    forged.base.cwd = Some("forged".into());
    forged.base.storage_version = n(99);
    assert!(Arc::ptr_eq(&root, &ready(r.open(forged)).unwrap()));
    assert_eq!(
        ready(r.list(())).unwrap().as_slice(),
        std::slice::from_ref(&metadata)
    );
    let empty_fork = ready(r.fork(
        metadata.clone(),
        SessionForkRequest {
            fork: serde_json::from_value(json!({})).unwrap(),
            create: options("unconfigured-fork"),
        },
    ))
    .unwrap();
    let empty = observe(&empty_fork.metadata());
    assert!(empty.snapshot.entries.is_empty());
    assert_eq!(empty.snapshot.registers.len(), 2);
    assert_eq!(empty.snapshot.next_seq, Some(n(2)));
    assert!(
        !empty
            .snapshot
            .registers
            .iter()
            .any(|r| matches!(r, Register::LaneConfig { .. }))
    );
    let fm = empty_fork.metadata();
    ready(empty_fork.close()).unwrap();
    ready(r.delete(fm)).unwrap();
    drop(root);
    assert!(ready(view.get_leaf_id()).is_ok());
    let root = ready(r.open(metadata.clone())).unwrap();
    ready(root.close()).unwrap();
    ready(r.delete(metadata)).unwrap();
    assert!(ready(r.list(())).unwrap().is_empty());
}
#[test]
fn literal_five_forks_preserve_rows_configuration_and_fresh_tokens() {
    let d = Dir::new();
    let r = repo(&d.0);
    let source = seeded(&d, false);
    let original = observe(&source);
    let root = ready(r.open(source.clone())).unwrap();
    let view = root.view(LaneId::new("main").unwrap());
    let data = fixture();
    for (i, case) in data["forks"].as_array().unwrap().iter().enumerate() {
        let value = case.get("options").cloned().unwrap_or(json!({}));
        let fork = serde_json::from_value(value).unwrap();
        let dest = ready(r.fork(
            source.clone(),
            SessionForkRequest {
                fork,
                create: options(&format!("fork-{i}")),
            },
        ))
        .unwrap();
        let observation = observe(&dest.metadata());
        let wanted: Vec<_> = case["entryIds"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_owned())
            .collect();
        assert_eq!(ids(&observation.snapshot), wanted);
        for e in &observation.snapshot.entries {
            assert!(original.snapshot.entries.contains(e));
        }
        assert!(observation.snapshot.usage.is_empty());
        assert_eq!(observation.snapshot.stats.usage, Usage::zero());
        assert_eq!(
            observation.snapshot.stats.message_count.get(),
            case["messageCount"].as_u64().unwrap()
        );
        assert_eq!(
            observation.snapshot.next_seq.unwrap().get(),
            case["nextSeq"].as_u64().unwrap()
        );
        assert_eq!(
            observation.metadata.parent_session_id,
            Some(source.base.id.clone())
        );
        assert_eq!(observation.metadata.store_generation, n(0));
        for row in &observation.snapshot.registers {
            assert!(register_seq(row) < n(30));
            assert!(!matches!(
                row,
                Register::PendingEntry { .. }
                    | Register::OperationToolArguments { .. }
                    | Register::LaneLastResult { .. }
            ));
        }
        for (lane, leaf) in case["leaves"].as_object().unwrap() {
            let row = observation
                .snapshot
                .registers
                .iter()
                .find(|r| matches!(r,Register::LaneLeaf{key,..}if key==lane))
                .unwrap();
            if let Register::LaneLeaf { value, .. } = row {
                assert_eq!(value.as_ref().map(|v| v.as_str()), leaf.as_str());
            }
        }
        for (lane, source_lane) in case["configSource"].as_object().unwrap() {
            let wanted = original
                .snapshot
                .registers
                .iter()
                .find_map(|r| match r {
                    Register::LaneConfig { key, value, .. }
                        if key == source_lane.as_str().unwrap() =>
                    {
                        Some(value)
                    }
                    _ => None,
                })
                .unwrap();
            let got = observation
                .snapshot
                .registers
                .iter()
                .find_map(|r| match r {
                    Register::LaneConfig { key, value, .. } if key == lane => Some(value),
                    _ => None,
                })
                .unwrap();
            assert_eq!(got, wanted);
        }
        let mut labels: Vec<_> = observation
            .snapshot
            .registers
            .iter()
            .filter_map(|r| match r {
                Register::FactLabel { key, .. } => Some(key.as_str()),
                _ => None,
            })
            .collect();
        labels.sort();
        let mut wanted: Vec<_> = case["labels"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect();
        wanted.sort();
        assert_eq!(labels, wanted);
        for original_fact in original.snapshot.registers.iter().filter(|r| {
            matches!(
                r,
                Register::FactName { .. }
                    | Register::FactCustom { .. }
                    | Register::FactLabel { .. }
            )
        }) {
            let key = register_key(original_fact);
            if key.namespace == RegisterNamespace::FactLabel && !wanted.contains(&key.key.as_str())
            {
                continue;
            }
            let copied = observation
                .snapshot
                .registers
                .iter()
                .find(|r| register_key(r) == key)
                .unwrap();
            let mut a = serde_json::to_value(original_fact).unwrap();
            let mut b = serde_json::to_value(copied).unwrap();
            a.as_object_mut().unwrap().remove("seq");
            b.as_object_mut().unwrap().remove("seq");
            assert_eq!(a, b);
        }
        for row in &observation.snapshot.registers {
            if let Register::LaneState { value, .. } = row {
                assert!(value.current_operation_id.is_none() && value.pending_next_run.is_empty());
            }
        }
        ready(dest.close()).unwrap();
    }
    assert_eq!(observe(&source).snapshot, original.snapshot);
    assert!(ready(view.get_leaf_id()).is_ok());
    append(&root, "after-fork", "writable");
    ready(root.close()).unwrap();
}
#[test]
fn header_namespace_occupied_path_and_wrong_id_delete_never_seal_owner() {
    let d = Dir::new();
    let r = repo(&d.0);
    let s = ready(r.create(options("taken"))).unwrap();
    let metadata = s.metadata();
    ready(s.close()).unwrap();
    let moved = d.0.join("renamed.sqlite");
    std::fs::rename(&metadata.path, &moved).unwrap();
    assert!(ready(r.create(options("taken"))).is_err());
    let actual = ready(r.open_path(&moved)).unwrap();
    let view = actual.view(LaneId::new("main").unwrap());
    let mut wrong = actual.metadata();
    wrong.base.id = SessionId::new("wrong").unwrap();
    assert!(ready(r.delete(wrong)).is_err());
    assert!(ready(view.get_leaf_id()).is_ok());
    append(&actual, "still", "usable");
    let dest = r.destination_path(&SessionId::new("occupied").unwrap());
    let (mut base, snap) = snapshot(true);
    base.id = SessionId::new("other-header").unwrap();
    let occupied = SqliteStorage::create_from_snapshot(
        &dest,
        base,
        snap,
        codec(),
        Arc::new(Clock),
        SqliteOptions::default(),
    )
    .unwrap();
    ready(occupied.close()).unwrap();
    assert!(ready(r.create(options("occupied"))).is_err());
    assert!(
        filenames(&d)
            .iter()
            .all(|n| !n.starts_with(".pi-sqlite-temp"))
    );
    ready(actual.close()).unwrap();
}
#[test]
fn readonly_pages_and_domains_preserve_active_writer_and_recreation_resets() {
    let d = Dir::new();
    let other = Dir::new();
    let r = repo(&d.0);
    let root = ready(r.create(options("s"))).unwrap();
    append(&root, "e", "text");
    let m = root.metadata();
    let reader = repo(&d.0);
    assert_eq!(r.source_domain().unwrap(), reader.source_domain().unwrap());
    assert_ne!(
        r.source_domain().unwrap(),
        repo(&other.0).source_domain().unwrap()
    );
    let request = ReadBatchRequest {
        cursor: None,
        limit: n(1),
    };
    let batch = reader.read_batch(&m, request).unwrap();
    assert_eq!(batch.next_cursor.last_entry_seq, Some(n(2)));
    assert_eq!(
        reader.read_list(()).unwrap().as_slice(),
        std::slice::from_ref(&m)
    );
    assert!(ready(root.get_leaf_id()).is_ok());
    append(&root, "later", "text2");
    ready(r.delete(m.clone())).unwrap();
    let new = ready(r.create(options("s"))).unwrap();
    append(&new, "e", "text");
    let reset = reader
        .read_batch(
            &new.metadata(),
            ReadBatchRequest {
                cursor: Some(batch.next_cursor),
                limit: n(1),
            },
        )
        .unwrap();
    assert!(reset.reset);
    assert!(
        reader
            .managed_files()
            .unwrap()
            .unwrap()
            .contains_path(&new.metadata().path)
            .unwrap()
    );
    ready(new.close()).unwrap();
}

#[test]
fn callback_rechecks_header_namespace_and_source_capture_can_outlive_deletion() {
    struct Reenter(Mutex<Option<Box<dyn Fn() + Send>>>);
    impl StorageClock for Reenter {
        fn now(&self) -> Result<SafeUnsigned, StorageError> {
            let call = self.0.lock().unwrap().take();
            if let Some(call) = call {
                call();
            }
            Ok(n(500))
        }
    }
    let d = Dir::new();
    let clock = Arc::new(Reenter(Mutex::new(None)));
    let r = SqliteSessionRepo::with_codec(
        &d.0,
        codec(),
        Arc::new(Ids::default()),
        clock.clone(),
        Arc::new(NoopTelemetryContext),
        SqliteOptions::default(),
    )
    .unwrap();
    let rc = r.clone();
    let other = d.0.join("competing.sqlite");
    *clock.0.lock().unwrap() = Some(Box::new(move || {
        assert!(ready(rc.list(())).unwrap().is_empty());
        assert!(matches!(
            ready(rc.create(options("same"))),
            Err(SessionError::Storage(StorageError::Busy(_)))
        ));
        let base = SessionMetadata {
            id: SessionId::new("same").unwrap(),
            created_at: n(1),
            storage_version: n(1),
            store_generation: n(0),
            cwd: None,
            parent_session_id: None,
            legacy_parent_session_path: None,
        };
        let raw = SqliteStorage::create(
            &other,
            base,
            codec(),
            Arc::new(Clock),
            SqliteOptions::default(),
        )
        .unwrap();
        ready(raw.close()).unwrap();
    }));
    assert!(ready(r.create(options("same"))).is_err());
    assert_eq!(ready(r.list(())).unwrap().len(), 1);
    let source = seeded(&d, true);
    let source_before = observe(&source);
    let other_repo = repo(&d.0);
    let selected = source.clone();
    *clock.0.lock().unwrap() = Some(Box::new(move || {
        ready(other_repo.delete(selected.clone())).unwrap();
    }));
    let fork = ready(r.fork(
        source,
        SessionForkRequest {
            fork: serde_json::from_value(json!({})).unwrap(),
            create: options("captured"),
        },
    ))
    .unwrap();
    assert_eq!(ids(&observe(&fork.metadata()).snapshot), ["A", "B", "D"]);
    assert_eq!(
        source_before.metadata.id,
        fork.metadata().base.parent_session_id.unwrap()
    );
    ready(fork.close()).unwrap();
}
#[test]
fn new_publication_is_complete_before_exposure_failure_and_can_reopen() {
    let d = Dir::new();
    let r = repo(&d.0);
    hook(&r, |phase, path| {
        if phase == "before_expose" {
            let observed =
                SqliteStorage::read_snapshot(path, &codec(), SqliteOptions::default()).unwrap();
            assert_eq!(observed.snapshot.registers.len(), 2);
            assert_eq!(observed.snapshot.next_seq, Some(n(2)));
            return Err(invalid_repo());
        }
        Ok(())
    });
    assert!(ready(r.create(options("published"))).is_err());
    *r.inner.hook.lock().unwrap() = None;
    let m = ready(r.list(())).unwrap().remove(0);
    let s = ready(r.open(m)).unwrap();
    assert!(ready(s.get_leaf_id()).is_ok());
    ready(s.close()).unwrap();
    assert!(
        filenames(&d)
            .iter()
            .all(|n| !n.starts_with(".pi-sqlite-temp"))
    );
}
#[test]
fn future_version_precedes_malformed_header_and_id_generator_is_validated() {
    let d = Dir::new();
    let r = repo(&d.0);
    let s = ready(r.create(options("v"))).unwrap();
    let m = s.metadata();
    ready(s.close()).unwrap();
    let scope = path::Scope::acquire(&m.path, false).unwrap();
    let db = path::connection(&m.path, SqliteOptions::default(), false).unwrap();
    db.execute(
        "UPDATE session SET storage_version=2,usage_payload='future'",
        [],
    )
    .unwrap();
    drop(db);
    drop(scope);
    assert!(matches!(
        ready(r.list(())),
        Err(SessionError::NewerStorageVersion(_))
    ));
    assert!(matches!(
        ready(r.open(m)),
        Err(SessionError::NewerStorageVersion(_))
    ));
    struct BadIds;
    impl IdGenerator for BadIds {
        fn next(&self, _: Option<SafeUnsigned>) -> String {
            "bad".into()
        }
    }
    let empty = Dir::new();
    let bad = SqliteSessionRepo::with_codec(
        &empty.0,
        codec(),
        Arc::new(BadIds),
        Arc::new(Clock),
        Arc::new(NoopTelemetryContext),
        SqliteOptions::default(),
    )
    .unwrap();
    assert!(ready(bad.create(SessionCreateOptions::default())).is_err());
    assert!(filenames(&empty).is_empty());
    let s = ready(bad.create(options("supplied-open-id"))).unwrap();
    ready(s.close()).unwrap();
}
#[test]
fn sparse_max_reader_and_retained_max_fork_fail_without_partial_destination() {
    let d = Dir::new();
    let r = repo(&d.0);
    let (mut metadata, mut s) = snapshot(true);
    metadata.id = SessionId::new("max").unwrap();
    let custom:Entry=serde_json::from_value(json!({"type":"custom","id":"MAX","parentId":"D","seq":9007199254740991u64,"timestamp":1,"customType":"opaque","data":null})).unwrap();
    s.entries.insert(0,serde_json::from_value(json!({"type":"custom","id":"ZERO","parentId":null,"seq":0,"timestamp":1,"customType":"opaque","data":null})).unwrap());
    s.entries.push(custom);
    s.next_seq = None;
    let path = d.0.join("max.sqlite");
    let raw = SqliteStorage::create_from_snapshot(
        &path,
        metadata.clone(),
        s,
        codec(),
        Arc::new(Clock),
        SqliteOptions::default(),
    )
    .unwrap();
    ready(raw.close()).unwrap();
    let m = SqliteSessionMetadata {
        base: metadata,
        path,
    };
    let first = r
        .read_batch(
            &m,
            ReadBatchRequest {
                cursor: None,
                limit: n(1),
            },
        )
        .unwrap();
    assert_eq!(first.next_cursor.last_entry_seq, Some(n(0)));
    let first = r
        .read_batch(
            &m,
            ReadBatchRequest {
                cursor: Some(first.next_cursor),
                limit: n(100),
            },
        )
        .unwrap();
    assert_eq!(first.next_cursor.last_entry_seq, Some(n(9007199254740991)));
    let end = r
        .read_batch(
            &m,
            ReadBatchRequest {
                cursor: Some(first.next_cursor),
                limit: n(1),
            },
        )
        .unwrap();
    assert!(end.complete && end.entries.is_empty());
    assert!(
        r.read_batch(
            &m,
            ReadBatchRequest {
                cursor: None,
                limit: n(0)
            }
        )
        .is_err()
    );
    let earlier = ready(r.fork(
        m.clone(),
        SessionForkRequest {
            fork: serde_json::from_value(json!({"entryId":"A"})).unwrap(),
            create: options("earlier"),
        },
    ))
    .unwrap();
    ready(earlier.close()).unwrap();
    assert!(
        ready(r.fork(
            m,
            SessionForkRequest {
                fork: serde_json::from_value(json!({"scope":"tree"})).unwrap(),
                create: options("exhausted")
            }
        ))
        .is_err()
    );
    assert!(
        !r.destination_path(&SessionId::new("exhausted").unwrap())
            .exists()
    );
}
