mod repository_support;
use pi_telemetry::*;
use repository_support::*;

#[test]
fn root_drop_does_not_hide_live_views_and_close_reopen_seals_every_old_handle() {
    let repo = repo();
    let root = ready(repo.create(options("session"))).unwrap();
    let meta = root.metadata();
    let address = Arc::as_ptr(&root);
    let view = root.view(LaneId::new("main").unwrap());
    drop(root);
    let same = ready(repo.open(meta.clone())).unwrap();
    assert_eq!(Arc::as_ptr(&same), address);
    ready(view.append_message(user("retained"))).unwrap();
    ready(same.close()).unwrap();
    let next = ready(repo.open(meta.clone())).unwrap();
    assert!(!Arc::ptr_eq(&same, &next));
    assert!(matches!(
        ready(view.append_message(user("old"))),
        Err(SessionError::Closed(_))
    ));
    assert!(ready(same.get_leaf_id()).is_err());
    let new_view = next.view(LaneId::new("main").unwrap());
    ready(repo.delete(meta.clone())).unwrap();
    let recreated = ready(repo.create(options("session"))).unwrap();
    assert!(ready(new_view.append_message(user("deleted"))).is_err());
    assert!(ready(next.get_stats()).is_err());
    assert!(entries(&recreated).is_empty());
    assert!(
        Arc::ptr_eq(&recreated, &ready(repo.open(meta)).unwrap()),
        "metadata selects actual recreated ID, not an old handle"
    );
    drop(repo);
    ready(recreated.append_message(user("held after catalog drop"))).unwrap();
}
struct CallbackClock {
    callback: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    fail_at: u64,
    calls: AtomicU64,
}
impl StorageClock for CallbackClock {
    fn now(&self) -> Result<SafeUnsigned, StorageError> {
        let callback = self.callback.lock().unwrap().take();
        if let Some(callback) = callback {
            callback();
        }
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        if call == self.fail_at {
            Err(StorageError::Io(
                ErrorContext::new("clock unavailable").unwrap(),
            ))
        } else {
            Ok(n(900))
        }
    }
}
#[test]
fn create_reservation_is_visible_during_clock_and_failure_cleans_only_itself() {
    for fail_at in [1, 2] {
        let clock = Arc::new(CallbackClock {
            callback: Mutex::new(None),
            fail_at,
            calls: AtomicU64::new(0),
        });
        let repo = Arc::new(
            MemorySessionRepo::new(
                SessionCodecOptions::default(),
                ProviderHeaderPolicy::default(),
                Arc::new(Ids::default()),
                clock.clone(),
                Arc::new(NoopTelemetryContext),
            )
            .unwrap(),
        );
        let weak = Arc::downgrade(&repo);
        *clock.callback.lock().unwrap() = Some(Box::new(move || {
            let repo = weak.upgrade().unwrap();
            assert!(ready(repo.list(())).unwrap().is_empty());
            assert!(matches!(
                ready(repo.create(options("reserved"))),
                Err(SessionError::Storage(StorageError::Busy(_)))
            ));
        }));
        assert!(ready(repo.create(options("reserved"))).is_err());
        assert!(ready(repo.list(())).unwrap().is_empty());
        let later = ready(repo.create(options("reserved"))).unwrap();
        assert_eq!(later.metadata().id, sid("reserved"));
    }
}
struct CallbackTelemetry {
    callback: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}
impl TelemetryContext for CallbackTelemetry {
    fn start_span(&self, options: SpanOptions) -> SpanGuard {
        let callback = self.callback.lock().unwrap().take();
        if let Some(callback) = callback {
            callback();
        }
        NOOP_TELEMETRY_CONTEXT.start_span(options)
    }
}
#[test]
fn real_session_seed_telemetry_runs_before_exposure_and_outside_catalog_lock() {
    let telemetry = Arc::new(CallbackTelemetry {
        callback: Mutex::new(None),
    });
    let repo = Arc::new(
        MemorySessionRepo::new(
            SessionCodecOptions::default(),
            ProviderHeaderPolicy::default(),
            Arc::new(Ids::default()),
            Arc::new(Clock),
            telemetry.clone(),
        )
        .unwrap(),
    );
    let weak = Arc::downgrade(&repo);
    *telemetry.callback.lock().unwrap() = Some(Box::new(move || {
        let repo = weak.upgrade().unwrap();
        assert!(ready(repo.list(())).unwrap().is_empty());
        let meta = SessionMetadata {
            id: sid("seed"),
            created_at: n(0),
            storage_version: n(1),
            store_generation: n(0),
            cwd: None,
            parent_session_id: None,
            legacy_parent_session_path: None,
        };
        assert!(matches!(
            ready(repo.open(meta.clone())),
            Err(SessionError::Storage(StorageError::Busy(_)))
        ));
        assert!(matches!(
            ready(repo.delete(meta)),
            Err(SessionError::Storage(StorageError::Busy(_)))
        ));
        ready(repo.create(options("independent"))).unwrap();
    }));
    ready(repo.create(options("seed"))).unwrap();
    assert_eq!(ready(repo.list(())).unwrap().len(), 2);
}
#[test]
fn bad_generated_id_and_duplicate_destination_never_publish_partial_records() {
    struct Bad;
    impl IdGenerator for Bad {
        fn next(&self, _: Option<SafeUnsigned>) -> String {
            "bad-id".into()
        }
    }
    let repo = MemorySessionRepo::new(
        SessionCodecOptions::default(),
        ProviderHeaderPolicy::default(),
        Arc::new(Bad),
        Arc::new(Clock),
        Arc::new(NoopTelemetryContext),
    )
    .unwrap();
    assert!(ready(repo.create(SessionCreateOptions::default())).is_err());
    assert!(ready(repo.list(())).unwrap().is_empty());
    let existing = ready(repo.create(options("existing"))).unwrap();
    assert!(ready(repo.fork(existing.metadata(), request("existing", branch()))).is_err());
    assert!(Arc::ptr_eq(
        &existing,
        &ready(repo.open(existing.metadata())).unwrap()
    ));
}
