use super::support::*;
#[test]
fn one_sweep_publishes_new_b_and_reconciles_absent_a_without_noop_churn() {
    let d = Dir::new();
    let r = memory();
    let a = create(r.as_ref(), "A");
    append(a.as_ref(), "a-old", "oldneedle");
    let s = SqliteSearchService::new(d.0.join("index.db"), r.clone(), ()).unwrap();
    ready(s.sync()).unwrap();
    ready(r.delete(a.metadata())).unwrap();
    let b = create(r.as_ref(), "B");
    append(b.as_ref(), "b-new", "newneedle");
    ready(s.sync()).unwrap();
    assert!(hits(&s, "oldneedle").is_empty());
    assert_eq!(hits(&s, "newneedle"), ["b-new"]);
    let epoch = s.inner.progress(None).unwrap().epoch;
    ready(s.sync()).unwrap();
    assert_eq!(s.inner.progress(None).unwrap().epoch, epoch);
    ready(s.close()).unwrap();
}
#[test]
fn stale_batch_cannot_undo_acknowledged_remove_and_new_sync_can_reindex() {
    let d = Dir::new();
    let r = memory();
    let root = create(r.as_ref(), "s");
    append(root.as_ref(), "e", "needle");
    let path = d.0.join("index.db");
    let a = Arc::new(SqliteSearchService::new(&path, r.clone(), ()).unwrap());
    let b = SqliteSearchService::new(&path, r, ()).unwrap();
    let (arrived, resume) = pause(&a, "after_batch");
    let aa = a.clone();
    let work = std::thread::spawn(move || ready(aa.sync()));
    wait(&arrived);
    ready(b.remove(sid("s"))).unwrap();
    resume.send(()).unwrap();
    work.join().unwrap().unwrap();
    assert!(hits(&b, "needle").is_empty());
    clear_hook(&a);
    ready(a.sync()).unwrap();
    assert_eq!(hits(&b, "needle"), ["e"]);
    ready(a.close()).unwrap();
    ready(b.close()).unwrap();
}
#[test]
fn stale_generation_cannot_overwrite_new_generation_progress() {
    let d = Dir::new();
    let path = d.0.join("source.jsonl");
    file(&path, "s", 0, vec![entry("old", 0, "oldneedle")]);
    let r = jsonl(&d.0);
    let db = d.0.join("index.db");
    let a = Arc::new(SqliteSearchService::new(&db, r.clone(), ()).unwrap());
    let b = SqliteSearchService::new(&db, r, ()).unwrap();
    let (arrived, resume) = pause(&a, "after_batch");
    let aa = a.clone();
    let work = std::thread::spawn(move || ready(aa.sync()));
    wait(&arrived);
    file(&path, "s", 1, vec![entry("new", 0, "newneedle")]);
    ready(b.sync()).unwrap();
    resume.send(()).unwrap();
    work.join().unwrap().unwrap();
    assert!(hits(&b, "oldneedle").is_empty());
    assert_eq!(hits(&b, "newneedle"), ["new"]);
    assert_eq!(
        b.inner
            .progress(Some(&sid("s")))
            .unwrap()
            .cursor
            .unwrap()
            .generation,
        n(1)
    );
    ready(a.close()).unwrap();
    ready(b.close()).unwrap();
}
#[test]
fn epoch_captured_before_actual_listing_blocks_old_empty_sweep() {
    let d = Dir::new();
    let r = memory();
    let root = create(r.as_ref(), "s");
    append(root.as_ref(), "old", "oldneedle");
    let db = d.0.join("index.db");
    let a = Arc::new(SqliteSearchService::new(&db, r.clone(), ()).unwrap());
    let b = SqliteSearchService::new(&db, r.clone(), ()).unwrap();
    ready(b.sync()).unwrap();
    ready(r.delete(root.metadata())).unwrap();
    let (arrived, resume) = pause(&a, "after_list");
    let aa = a.clone();
    let work = std::thread::spawn(move || ready(aa.sync()));
    wait(&arrived);
    let new = create(r.as_ref(), "s");
    append(new.as_ref(), "new", "newneedle");
    ready(b.sync()).unwrap();
    resume.send(()).unwrap();
    work.join().unwrap().unwrap();
    assert_eq!(hits(&b, "newneedle"), ["new"]);
    ready(a.close()).unwrap();
    ready(b.close()).unwrap();
}
#[test]
fn busy_actual_jsonl_listing_keeps_index_and_borrowed_writer() {
    let d = Dir::new();
    let r = jsonl(&d.0);
    let root = create(r.as_ref(), "s");
    append(root.as_ref(), "e", "needle");
    let m = root.metadata();
    ready(root.close()).unwrap();
    let s = SqliteSearchService::new(d.0.join("index.db"), r, ()).unwrap();
    ready(s.sync()).unwrap();
    let foreign = jsonl(&d.0);
    let writer = ready(foreign.open(m)).unwrap();
    assert!(matches!(
        ready(s.sync()),
        Err(SearchError::Source(SessionError::Storage(
            StorageError::Busy(_)
        )))
    ));
    assert_eq!(hits(&s, "needle"), ["e"]);
    assert!(ready(writer.get_leaf_id()).is_ok());
    ready(writer.close()).unwrap();
    ready(s.close()).unwrap();
}
#[test]
fn failed_actual_listing_and_moved_locator_never_authorize_purge() {
    let d = Dir::new();
    let path = d.0.join("source.jsonl");
    file(&path, "s", 0, vec![entry("e", 0, "needle")]);
    let s = SqliteSearchService::new(d.0.join("index.db"), jsonl(&d.0), ()).unwrap();
    ready(s.sync()).unwrap();
    std::fs::write(d.0.join("bad.jsonl"), b"invalid\n").unwrap();
    assert!(ready(s.sync()).is_err());
    assert_eq!(hits(&s, "needle"), ["e"]);
    std::fs::remove_file(d.0.join("bad.jsonl")).unwrap();
    let moved = d.0.join("moved.jsonl");
    let old = path.clone();
    hook(&s, move |phase| {
        if phase == "after_list" {
            std::fs::rename(&old, &moved).unwrap();
        }
    });
    assert!(ready(s.sync()).is_err());
    assert_eq!(hits(&s, "needle"), ["e"]);
    clear_hook(&s);
    ready(s.sync()).unwrap();
    assert_eq!(hits(&s, "needle"), ["e"]);
    ready(s.close()).unwrap();
}
#[test]
fn close_seals_discards_queue_and_drains_admitted_work() {
    let d = Dir::new();
    let r = memory();
    let root = create(r.as_ref(), "s");
    append(root.as_ref(), "e", "needle");
    let db = d.0.join("index.db");
    let a = Arc::new(SqliteSearchService::new(&db, r.clone(), ()).unwrap());
    let b = SqliteSearchService::new(&db, r, ()).unwrap();
    let (arrived, resume) = pause(&a, "after_batch");
    let aa = a.clone();
    let work = std::thread::spawn(move || ready(aa.sync()));
    wait(&arrived);
    a.notify(&sid("s"));
    let aa = a.clone();
    let closer = std::thread::spawn(move || ready(aa.close()));
    {
        let mut state = a.inner.state.lock().unwrap();
        while !state.closing {
            state = a.inner.wake.wait(state).unwrap();
        }
        assert!(!state.closed);
        assert!(state.queue.is_empty());
    }
    assert!(matches!(ready(a.sync()), Err(SearchError::Closed)));
    resume.send(()).unwrap();
    work.join().unwrap().unwrap();
    closer.join().unwrap().unwrap();
    assert_eq!(hits(&b, "needle"), ["e"]);
    ready(b.sync()).unwrap();
    ready(b.close()).unwrap();
}
#[test]
fn notify_debounce_single_session_reentry_and_background_diagnostics() {
    let d = Dir::new();
    let r = memory();
    let a = create(r.as_ref(), "A");
    let b = create(r.as_ref(), "B");
    append(a.as_ref(), "a", "alpha");
    append(b.as_ref(), "b", "beta");
    let s = Arc::new(
        SqliteSearchService::with_options(
            d.0.join("index.db"),
            r,
            (),
            SearchOptions {
                debounce: std::time::Duration::from_secs(3600),
                ..Default::default()
            },
            Arc::new(DefaultTextProjector),
        )
        .unwrap(),
    );
    for _ in 0..50 {
        s.notify(&sid("A"));
    }
    assert_eq!(s.inner.state.lock().unwrap().queue.len(), 1);
    let count = Arc::new(AtomicUsize::new(0));
    let counted = count.clone();
    let weak = Arc::downgrade(&s);
    let (done, received) = std::sync::mpsc::channel();
    hook(&s, move |phase| {
        if phase == "after_commit" {
            counted.fetch_add(1, Ordering::SeqCst);
            let s = weak.upgrade().unwrap();
            assert!(matches!(ready(s.close()), Err(SearchError::Busy)));
            assert!(matches!(ready(s.sync()), Err(SearchError::Busy)));
            done.send(()).unwrap();
        }
    });
    {
        let mut state = s.inner.state.lock().unwrap();
        state.queue.get_mut("A").unwrap().deadline = std::time::Instant::now();
    }
    s.inner.wake.notify_all();
    wait(&received);
    {
        let mut state = s.inner.state.lock().unwrap();
        while state.active.is_some() {
            state = s.inner.wake.wait(state).unwrap();
        }
    }
    assert_eq!(count.load(Ordering::SeqCst), 1);
    assert_eq!(hits(&s, "alpha"), ["a"]);
    assert!(hits(&s, "beta").is_empty());
    assert!(s.inner.progress(Some(&sid("B"))).unwrap().cursor.is_none());
    clear_hook(&s);
    ready(s.sync()).unwrap();
    assert_eq!(hits(&s, "beta"), ["b"]);
    hook(&s, |phase| {
        if phase == "before_list" {
            panic!("synthetic callback failure")
        }
    });
    s.notify(&sid("A"));
    {
        let mut state = s.inner.state.lock().unwrap();
        state.queue.get_mut("A").unwrap().deadline = std::time::Instant::now();
    }
    s.inner.wake.notify_all();
    {
        let mut state = s.inner.state.lock().unwrap();
        while state.errors.is_empty() || state.active.is_some() {
            state = s.inner.wake.wait(state).unwrap();
        }
    }
    assert!(matches!(
        s.take_background_errors().unwrap().as_slice(),
        [SearchError::Worker]
    ));
    clear_hook(&s);
    ready(s.close()).unwrap();
}
#[test]
fn empty_zero_and_unknown_hint_never_read_batches() {
    let d = Dir::new();
    let r = memory();
    let s = SqliteSearchService::new(d.0.join("index.db"), r, ()).unwrap();
    hook(&s, |_| panic!("no source phase admitted"));
    assert!(hits(&s, " \n\t").is_empty());
    assert!(
        ready(s.search_entries(query("word", Some(0))).unwrap())
            .unwrap()
            .is_empty()
    );
    clear_hook(&s);
    s.inner.sweep(Some(&sid("absent"))).unwrap();
    assert!(s.inner.progress(None).unwrap().epoch == 0);
    ready(s.close()).unwrap();
}
#[test]
fn remove_discards_older_unadmitted_local_hint() {
    let d = Dir::new();
    let r = memory();
    let root = create(r.as_ref(), "s");
    append(root.as_ref(), "e", "needle");
    let s = SqliteSearchService::with_options(
        d.0.join("index.db"),
        r,
        (),
        SearchOptions {
            debounce: std::time::Duration::from_secs(3600),
            ..Default::default()
        },
        Arc::new(DefaultTextProjector),
    )
    .unwrap();
    ready(s.sync()).unwrap();
    s.notify(&sid("s"));
    assert_eq!(s.inner.state.lock().unwrap().queue.len(), 1);
    ready(s.remove(sid("s"))).unwrap();
    assert!(s.inner.state.lock().unwrap().queue.is_empty());
    assert!(hits(&s, "needle").is_empty());
    ready(s.close()).unwrap();
}
type ProjectCallback = Arc<dyn Fn(&Entry) -> Result<(), SearchError> + Send + Sync>;
struct CallbackProjector(ProjectCallback);
impl TextProjector for CallbackProjector {
    fn identity(&self) -> &str {
        "callback-v1"
    }
    fn project(&self, e: &Entry) -> Result<Option<String>, SearchError> {
        (self.0)(e)?;
        DefaultTextProjector.project(e)
    }
}
#[test]
fn real_projector_runs_outside_index_transaction_and_reentrant_admission_is_busy() {
    let d = Dir::new();
    let r = memory();
    let root = create(r.as_ref(), "s");
    append(root.as_ref(), "e", "needle");
    let db = d.0.join("index.db");
    let link = Arc::new(Mutex::new(
        None::<std::sync::Weak<SqliteSearchService<MemorySessionRepo>>>,
    ));
    let callback_link = link.clone();
    let index = db.clone();
    let observed = Arc::new(AtomicUsize::new(0));
    let seen = observed.clone();
    let projector = CallbackProjector(Arc::new(move |_| {
        let s = callback_link
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .upgrade()
            .unwrap();
        assert!(matches!(ready(s.close()), Err(SearchError::Busy)));
        assert!(matches!(ready(s.sync()), Err(SearchError::Busy)));
        let mut c = rusqlite::Connection::open(&index).unwrap();
        c.busy_timeout(std::time::Duration::ZERO).unwrap();
        let tx = c
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .unwrap();
        tx.commit().unwrap();
        seen.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }));
    let s = Arc::new(
        SqliteSearchService::with_options(
            &db,
            r,
            (),
            SearchOptions::default(),
            Arc::new(projector),
        )
        .unwrap(),
    );
    *link.lock().unwrap() = Some(Arc::downgrade(&s));
    ready(s.sync()).unwrap();
    assert_eq!(observed.load(Ordering::SeqCst), 1);
    assert_eq!(hits(&s, "needle"), ["e"]);
    ready(s.close()).unwrap();
}
#[test]
fn failed_projector_preserves_prior_atomic_batches_then_retry_completes() {
    let d = Dir::new();
    let r = memory();
    let root = create(r.as_ref(), "s");
    append(root.as_ref(), "a", "firstneedle");
    append(root.as_ref(), "b", "secondneedle");
    let before = r
        .read_batch(
            &root.metadata(),
            ReadBatchRequest {
                cursor: None,
                limit: n(100),
            },
        )
        .unwrap();
    let fail = Arc::new(AtomicUsize::new(1));
    let cb = fail.clone();
    let projector = CallbackProjector(Arc::new(move |e| {
        if entry_structure(e).id.as_str() == "b" && cb.load(Ordering::SeqCst) == 1 {
            Err(SearchError::Projection)
        } else {
            Ok(())
        }
    }));
    let s = SqliteSearchService::with_options(
        d.0.join("index.db"),
        r.clone(),
        (),
        SearchOptions {
            batch_size: n(1),
            ..Default::default()
        },
        Arc::new(projector),
    )
    .unwrap();
    assert!(matches!(ready(s.sync()), Err(SearchError::Projection)));
    assert_eq!(hits(&s, "firstneedle"), ["a"]);
    assert!(hits(&s, "secondneedle").is_empty());
    assert_eq!(
        s.inner
            .progress(Some(&sid("s")))
            .unwrap()
            .cursor
            .unwrap()
            .last_entry_seq,
        Some(n(2))
    );
    fail.store(0, Ordering::SeqCst);
    ready(s.sync()).unwrap();
    assert_eq!(hits(&s, "secondneedle"), ["b"]);
    assert_eq!(
        before,
        r.read_batch(
            &root.metadata(),
            ReadBatchRequest {
                cursor: None,
                limit: n(100)
            }
        )
        .unwrap()
    );
    ready(s.close()).unwrap();
}
#[test]
fn actual_batch_busy_after_successful_listing_preserves_index() {
    let d = Dir::new();
    let r = jsonl(&d.0);
    let root = create(r.as_ref(), "s");
    append(root.as_ref(), "e", "needle");
    let m = root.metadata();
    ready(root.close()).unwrap();
    let foreign = jsonl(&d.0);
    let s = SqliteSearchService::new(d.0.join("index.db"), r, ()).unwrap();
    ready(s.sync()).unwrap();
    let borrowed = Arc::new(Mutex::new(None));
    let saved = borrowed.clone();
    hook(&s, move |phase| {
        if phase == "after_list" {
            *saved.lock().unwrap() = Some(ready(foreign.open(m.clone())).unwrap());
        }
    });
    assert!(matches!(
        ready(s.sync()),
        Err(SearchError::Source(SessionError::Storage(
            StorageError::Busy(_)
        )))
    ));
    assert_eq!(hits(&s, "needle"), ["e"]);
    clear_hook(&s);
    ready(borrowed.lock().unwrap().take().unwrap().close()).unwrap();
    ready(s.close()).unwrap();
}
#[test]
fn checked_epoch_exhaustion_rolls_back_removal_and_bad_cursor_is_not_success() {
    let d = Dir::new();
    let r = memory();
    let root = create(r.as_ref(), "s");
    append(root.as_ref(), "e", "needle");
    let db = d.0.join("index.db");
    let s = SqliteSearchService::new(&db, r, ()).unwrap();
    ready(s.sync()).unwrap();
    let c = rusqlite::Connection::open(&db).unwrap();
    c.execute("UPDATE search_control SET epoch=?", [i64::MAX])
        .unwrap();
    assert!(matches!(
        ready(s.remove(sid("s"))),
        Err(SearchError::Exhausted)
    ));
    assert_eq!(hits(&s, "needle"), ["e"]);
    assert!(s.inner.progress(Some(&sid("s"))).unwrap().cursor.is_some());
    c.execute("UPDATE search_control SET epoch=1", []).unwrap();
    let mut cursor = s.inner.progress(Some(&sid("s"))).unwrap().cursor.unwrap();
    cursor.witness[0] = 99;
    c.execute(
        "UPDATE search_cursors SET cursor=?",
        [serde_json::to_vec(&cursor).unwrap()],
    )
    .unwrap();
    assert!(ready(s.sync()).is_err());
    assert_eq!(hits(&s, "needle"), ["e"]);
    drop(c);
    ready(s.close()).unwrap();
}
#[test]
fn last_owner_drop_stops_worker_and_leaves_source_writable() {
    let d = Dir::new();
    let r = memory();
    let root = create(r.as_ref(), "s");
    let s = SqliteSearchService::with_options(
        d.0.join("index.db"),
        r,
        (),
        SearchOptions {
            debounce: std::time::Duration::from_secs(3600),
            ..Default::default()
        },
        Arc::new(DefaultTextProjector),
    )
    .unwrap();
    let inner = Arc::downgrade(&s.inner);
    s.notify(&sid("s"));
    drop(s);
    assert!(inner.upgrade().is_none());
    append(root.as_ref(), "e", "writable");
}
#[test]
fn old_queued_hint_on_another_instance_cannot_undo_acknowledged_removal() {
    let d = Dir::new();
    let r = memory();
    let root = create(r.as_ref(), "s");
    append(root.as_ref(), "e", "needle");
    let db = d.0.join("index.db");
    let a = SqliteSearchService::with_options(
        &db,
        r.clone(),
        (),
        SearchOptions {
            debounce: std::time::Duration::from_secs(3600),
            ..Default::default()
        },
        Arc::new(DefaultTextProjector),
    )
    .unwrap();
    let b = SqliteSearchService::new(&db, r, ()).unwrap();
    ready(b.sync()).unwrap();
    a.notify(&sid("s"));
    assert_eq!(a.inner.state.lock().unwrap().queue.len(), 1);
    ready(b.remove(sid("s"))).unwrap();
    {
        let mut state = a.inner.state.lock().unwrap();
        state.queue.get_mut("s").unwrap().deadline = std::time::Instant::now();
    }
    a.inner.wake.notify_all();
    {
        let mut state = a.inner.state.lock().unwrap();
        while !state.queue.is_empty() || state.active.is_some() {
            state = a.inner.wake.wait(state).unwrap();
        }
    }
    assert!(
        hits(&b, "needle").is_empty(),
        "old hint from another instance must not resurrect removed index rows"
    );
    assert!(b.inner.progress(Some(&sid("s"))).unwrap().cursor.is_none());
    a.notify(&sid("s"));
    {
        let mut state = a.inner.state.lock().unwrap();
        state.queue.get_mut("s").unwrap().deadline = std::time::Instant::now();
    }
    a.inner.wake.notify_all();
    {
        let mut state = a.inner.state.lock().unwrap();
        while !state.queue.is_empty() || state.active.is_some() {
            state = a.inner.wake.wait(state).unwrap();
        }
    }
    assert_eq!(
        hits(&b, "needle"),
        ["e"],
        "a new explicit hint may reindex canonical content"
    );
    ready(a.close()).unwrap();
    ready(b.close()).unwrap();
}
#[test]
fn close_drains_index_only_hint_admission_before_acknowledgment() {
    let d = Dir::new();
    let r = memory();
    let s = Arc::new(SqliteSearchService::new(d.0.join("index.db"), r, ()).unwrap());
    let (arrived, resume) = pause(&s, "hint_admitted");
    let a = s.clone();
    let notify = std::thread::spawn(move || a.notify(&sid("s")));
    wait(&arrived);
    let a = s.clone();
    let closer = std::thread::spawn(move || ready(a.close()));
    {
        let mut state = s.inner.state.lock().unwrap();
        while !state.closing {
            state = s.inner.wake.wait(state).unwrap();
        }
        assert!(!state.closed);
        assert_eq!(state.hint_admissions, 1);
    }
    resume.send(()).unwrap();
    notify.join().unwrap();
    closer.join().unwrap().unwrap();
    let state = s.inner.state.lock().unwrap();
    assert_eq!(state.hint_admissions, 0);
    assert!(state.queue.is_empty());
    assert!(state.closed);
}
