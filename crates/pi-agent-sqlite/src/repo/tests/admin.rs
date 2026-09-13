use super::support::*;
#[test]
fn actual_precise_rewrite_retains_literal_rows_usage_modes_and_changes_token() {
    let d = Dir::new();
    let r = repo(&d.0);
    let m = seeded(&d, true);
    let before = observe(&m);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&m.path, std::fs::Permissions::from_mode(0o640)).unwrap();
    }
    let after = ready(r.rewrite(m.clone(), |o| {
        assert_eq!(
            SqliteStorage::read_snapshot(&m.path, &codec(), SqliteOptions::default())
                .unwrap()
                .snapshot,
            o.snapshot
        );
        Ok(changed(o))
    }))
    .unwrap();
    let actual = observe(&after);
    assert_eq!(ids(&actual.snapshot), ["A", "C", "D"]);
    assert_eq!(actual.snapshot.usage, before.snapshot.usage);
    assert_eq!(actual.snapshot.stats, before.snapshot.stats);
    assert_eq!(actual.snapshot.next_seq, Some(n(46)));
    assert_eq!(actual.metadata.store_generation, n(1));
    assert_ne!(actual.store_token, before.store_token);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&m.path).unwrap().permissions().mode() & 0o777,
            0o640
        );
    }
    let opened = ready(r.open(after)).unwrap();
    assert!(ready(opened.get_leaf_id()).is_ok());
    ready(opened.close()).unwrap();
    assert!(
        filenames(&d)
            .iter()
            .all(|n| !n.starts_with(".pi-sqlite-temp"))
    );
}
#[test]
fn rewrite_rejects_active_dirty_and_invalid_retained_candidates() {
    let d = Dir::new();
    let r = repo(&d.0);
    let m = seeded(&d, true);
    let root = ready(r.open(m.clone())).unwrap();
    assert!(ready(r.rewrite(m.clone(), |_| panic!("active transform"))).is_err());
    ready(root.close()).unwrap();
    let original = observe(&m);
    for variant in 0..4 {
        assert!(
            ready(r.rewrite(m.clone(), |o| {
                let mut s = changed(o);
                match variant {
                    0 => {
                        if let Entry::Message(e) = &mut s.entries[0] {
                            e.timestamp = n(888);
                        }
                    }
                    1 => {
                        if let Entry::Message(e) = &mut s.entries[2] {
                            e.parent_id = Some(EntryId::new("C").unwrap());
                        }
                    }
                    2 => s.usage[0].usage.input = n(99),
                    _ => {
                        if let Entry::Message(e) = &mut s.entries[0] {
                            e.id = EntryId::new("new-identity").unwrap();
                        }
                    }
                }
                Ok(s)
            }))
            .is_err()
        );
        assert_eq!(observe(&m).snapshot, original.snapshot);
    }
    let dirty = Dir::new();
    let dr = repo(&dirty.0);
    let dm = seeded(&dirty, false);
    assert!(ready(dr.rewrite(dm, |_| panic!("pending transform"))).is_err());
}

#[test]
fn stale_revision_and_fence_override_even_a_prepared_rejection() {
    for rejection in [false, true] {
        let d = Dir::new();
        let r = repo(&d.0);
        let m = seeded(&d, true);
        let before = observe(&m);
        let result = ready(r.rewrite(m.clone(), |o| {
            let scope = path::Scope::acquire(&m.path, false).unwrap();
            let mut db = path::connection(&m.path, SqliteOptions::default(), false).unwrap();
            let tx = db
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .unwrap();
            tx.execute("UPDATE session SET revision=revision+1", [])
                .unwrap();
            tx.commit().unwrap();
            drop(db);
            drop(scope);
            if rejection {
                Err(SessionError::Schema(context("prepared schema rejection")))
            } else {
                Ok(changed(o))
            }
        }));
        assert!(matches!(
            result,
            Err(SessionError::Storage(StorageError::Conflict(_)))
        ));
        assert_eq!(observe(&m).snapshot, before.snapshot);
    }
    let d = Dir::new();
    let r = repo(&d.0);
    let m = seeded(&d, true);
    let successor = Arc::new(Mutex::new(None));
    let holder = successor.clone();
    let result = ready(r.rewrite(m.clone(), |o| {
        let scope = path::Scope::acquire(&m.path, false).unwrap();
        let mut db = path::connection(&m.path, SqliteOptions::default(), false).unwrap();
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        tx.execute("UPDATE writer_lease SET expires_at_ms=0", [])
            .unwrap();
        tx.commit().unwrap();
        drop(db);
        drop(scope);
        *holder.lock().unwrap() = Some(
            SqliteStorage::open(&m.path, codec(), Arc::new(Clock), SqliteOptions::default())
                .unwrap(),
        );
        Ok(changed(o))
    }));
    assert!(matches!(result, Err(SessionError::Closed(_))));
    let successor = successor.lock().unwrap().take().unwrap();
    assert!(successor.metadata().is_ok());
    assert_eq!(observe(&m).metadata.store_generation, n(0));
    ready(successor.close()).unwrap();
}
#[test]
fn rewrite_failure_before_and_after_publish_has_distinct_complete_authority() {
    let d = Dir::new();
    let r = repo(&d.0);
    let m = seeded(&d, true);
    let before = observe(&m);
    hook(&r, |phase, _| {
        if phase == "before_rewrite_finalize" {
            Err(invalid_repo())
        } else {
            Ok(())
        }
    });
    assert!(ready(r.rewrite(m.clone(), |o| Ok(changed(o)))).is_err());
    assert_eq!(observe(&m).snapshot, before.snapshot);
    hook(&r, |phase, _| {
        if phase == "after_rewrite_publish" {
            Err(invalid_repo())
        } else {
            Ok(())
        }
    });
    assert!(ready(r.rewrite(m.clone(), |o| Ok(changed(o)))).is_err());
    assert_eq!(observe(&m).metadata.store_generation, n(1));
    assert_eq!(ids(&observe(&m).snapshot), ["A", "C", "D"]);
    *r.inner.hook.lock().unwrap() = None;
    let opened = ready(r.open(m)).unwrap();
    ready(opened.close()).unwrap();
    assert!(
        filenames(&d)
            .iter()
            .all(|n| !n.starts_with(".pi-sqlite-temp"))
    );
}
#[test]
fn rewrite_final_exclusivity_waits_for_every_actual_reader_scope() {
    let d = Dir::new();
    let r = repo(&d.0);
    let m = seeded(&d, true);
    let before = observe(&m);
    let held = Arc::new(Mutex::new(None));
    let h = held.clone();
    let result = ready(r.rewrite(m.clone(), |o| {
        let first = path::Scope::acquire(&m.path, false).unwrap();
        let db1 = path::connection(&m.path, SqliteOptions::default(), true).unwrap();
        let second = path::Scope::acquire(&m.path, false).unwrap();
        let db2 = path::connection(&m.path, SqliteOptions::default(), true).unwrap();
        drop(db1);
        drop(first);
        assert!(matches!(
            path::Scope::acquire(&m.path, true),
            Err(StorageError::Busy(_))
        ));
        *h.lock().unwrap() = Some((db2, second));
        Ok(changed(o))
    }));
    assert!(matches!(
        result,
        Err(SessionError::Storage(StorageError::Busy(_)))
    ));
    assert_eq!(observe(&m).snapshot, before.snapshot);
    let (db, scope) = held.lock().unwrap().take().unwrap();
    drop(db);
    drop(scope);
    let new = ready(r.rewrite(m, |o| Ok(changed(o)))).unwrap();
    assert_eq!(new.base.store_generation, n(1));
}

#[test]
fn refusing_foreign_live_writer_never_blocks_its_heartbeat_with_exclusive_scope() {
    for deleting in [false, true] {
        let d = Dir::new();
        let m = seeded(&d, true);
        let writer = SqliteStorage::open(
            &m.path,
            codec(),
            Arc::new(Clock),
            SqliteOptions {
                heartbeat_ms: n(10),
                lease_ttl_ms: n(10000),
                ..Default::default()
            },
        )
        .unwrap();
        let r = repo(&d.0);
        let (events, received) = std::sync::mpsc::channel();
        let (resume, resumed) = std::sync::mpsc::channel();
        let resumed = Mutex::new(resumed);
        let notify = events.clone();
        let _hook = super::instrument(
            &m.path,
            Arc::new(move |phase, _| {
                if phase == "admin_exclusive" {
                    notify.send(false).unwrap();
                    resumed
                        .lock()
                        .unwrap()
                        .recv_timeout(Duration::from_secs(10))
                        .unwrap();
                }
                Ok(())
            }),
        );
        let selected = m.clone();
        let work = std::thread::spawn(move || {
            let result = if deleting {
                ready(r.delete(selected))
            } else {
                ready(r.rewrite(selected, |o| Ok(changed(o)))).map(|_| ())
            };
            events.send(true).unwrap();
            result
        });
        let event = received.recv_timeout(Duration::from_secs(10)).unwrap();
        let mut sealed = false;
        if !event {
            let deadline = std::time::Instant::now() + Duration::from_secs(3);
            while std::time::Instant::now() < deadline {
                if matches!(writer.metadata(), Err(StorageError::Closed(_))) {
                    sealed = true;
                    break;
                }
                std::thread::sleep(Duration::from_millis(1));
            }
            resume.send(()).unwrap();
        }
        let result = work.join().unwrap();
        let alive = writer.metadata().is_ok();
        ready(writer.close()).unwrap();
        assert!(matches!(
            result,
            Err(SessionError::Storage(StorageError::Busy(_)))
        ));
        assert!(
            !sealed && alive,
            "rejected admin/delete disabled the foreign writer heartbeat (delete={deleting})"
        );
    }
}

#[test]
fn actual_temp_checkpoint_and_pre_replace_failures_preserve_old_complete_snapshot() {
    for failure in ["before_admin_checkpoint", "before_admin_replace"] {
        let d = Dir::new();
        let r = repo(&d.0);
        let m = seeded(&d, true);
        let before = observe(&m);
        let _phase = super::instrument(
            &m.path,
            Arc::new(move |at, _| {
                if at == failure {
                    Err(SessionError::Storage(StorageError::Io(context(
                        "injected administrative failure",
                    ))))
                } else {
                    Ok(())
                }
            }),
        );
        assert!(matches!(
            ready(r.rewrite(m.clone(), |o| Ok(changed(o)))),
            Err(SessionError::Storage(StorageError::Io(_)))
        ));
        let after = observe(&m);
        assert_eq!(after.snapshot, before.snapshot);
        assert_eq!(after.store_token, before.store_token);
        assert!(
            filenames(&d)
                .iter()
                .all(|n| !n.starts_with(".pi-sqlite-temp"))
        );
        let open = ready(r.open(m)).unwrap();
        ready(open.close()).unwrap();
    }
}
#[test]
fn callback_physical_recreation_cannot_be_overwritten_or_released_by_old_admin() {
    let d = Dir::new();
    let r = repo(&d.0);
    let m = seeded(&d, true);
    let before = observe(&m);
    let captured = Arc::new(Mutex::new(None));
    let saved = captured.clone();
    let result = ready(r.rewrite(m.clone(), |o| {
        let replacement = d.0.join("successor.sqlite");
        let mut next = o.snapshot.clone();
        if let Entry::Message(e) = &mut next.entries[4] {
            e.message = serde_json::from_value(
                json!({"role":"user","content":"successor content","timestamp":1}),
            )
            .unwrap();
        }
        let storage = SqliteStorage::create_from_snapshot(
            &replacement,
            o.metadata.clone(),
            next,
            codec(),
            Arc::new(Clock),
            SqliteOptions::default(),
        )
        .unwrap();
        ready(storage.close()).unwrap();
        let observation =
            SqliteStorage::read_snapshot(&replacement, &codec(), SqliteOptions::default()).unwrap();
        *saved.lock().unwrap() = Some(observation);
        let scope = path::Scope::acquire(&m.path, true).unwrap();
        std::fs::rename(&replacement, &m.path).unwrap();
        drop(scope);
        Ok(changed(o))
    }));
    assert!(matches!(result, Err(SessionError::Closed(_))));
    let after = observe(&m);
    let successor = captured.lock().unwrap().take().unwrap();
    assert_eq!(after.snapshot, successor.snapshot);
    assert_eq!(after.store_token, successor.store_token);
    assert_ne!(after.store_token, before.store_token);
    let open = ready(r.open(m)).unwrap();
    append(&open, "after-recreation", "usable");
    ready(open.close()).unwrap();
}
#[test]
fn explicit_root_detachment_is_allowed_and_generation_overflow_is_atomic() {
    let d = Dir::new();
    let r = repo(&d.0);
    let m = seeded(&d, true);
    let result = ready(r.rewrite(m.clone(), |o| {
        let mut s = changed(o);
        if let Entry::Message(e) = &mut s.entries[2] {
            e.parent_id = None;
        }
        Ok(s)
    }))
    .unwrap();
    let current = observe(&result);
    assert_eq!(
        entry_structure(&current.snapshot.entries[2]).parent_id,
        None
    );
    let (mut metadata, mut snapshot) = snapshot(true);
    metadata.id = SessionId::new("exhausted-generation").unwrap();
    metadata.store_generation = n(9007199254740991);
    snapshot.store_generation = metadata.store_generation;
    let path = d.0.join("generation.sqlite");
    let raw = SqliteStorage::create_from_snapshot(
        &path,
        metadata.clone(),
        snapshot,
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
    let before = observe(&m);
    assert!(ready(r.rewrite(m.clone(), |o| Ok(o.snapshot.clone()))).is_err());
    assert_eq!(observe(&m).snapshot, before.snapshot);
}
