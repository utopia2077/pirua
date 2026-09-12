mod repository_support;
use repository_support::*;

#[test]
fn actual_five_repository_methods_seed_main_and_return_detached_metadata() {
    let ids = Arc::new(Ids::default());
    let telemetry = Arc::new(Telemetry::default());
    let repo = MemorySessionRepo::new(
        SessionCodecOptions::default(),
        ProviderHeaderPolicy::default(),
        ids.clone(),
        Arc::new(Clock),
        telemetry.clone(),
    )
    .unwrap();
    let mut opts = options("z");
    opts.cwd = Some("source-cwd".to_owned());
    let source = ready(repo.create(opts)).unwrap();
    assert_eq!(ids.0.load(Ordering::SeqCst), 0, "explicit ID must not mint");
    assert_eq!(ready(source.get_leaf_id()).unwrap(), None);
    assert!(
        ready(source.get_register(RegisterNamespace::LaneConfig, "main"))
            .unwrap()
            .is_none()
    );
    assert!(
        matches!(ready(source.get_register(RegisterNamespace::LaneState,"main")).unwrap(),Some(Register::LaneState {value:LaneState {current_operation_id:None,pending_next_run},seq,..}) if pending_next_run.is_empty()&&seq==n(1))
    );
    let first = telemetry.0.lock().unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].name, "pi.session.write");
    assert!(first[0].attributes.contains_key("pi.session.item_count"));
    drop(first);
    ready(repo.create(options("a"))).unwrap();
    let mut listed = ready(repo.list(())).unwrap();
    assert_eq!(
        listed.iter().map(|v| v.id.as_str()).collect::<Vec<_>>(),
        vec!["a", "z"]
    );
    listed[1].cwd = Some("mutated".to_owned());
    listed[1].storage_version = n(999);
    listed[1].store_generation = n(999);
    listed[1].created_at = n(999);
    let reopened = ready(repo.open(listed[1].clone())).unwrap();
    assert!(Arc::ptr_eq(&source, &reopened));
    assert_eq!(reopened.metadata().cwd.as_deref(), Some("source-cwd"));
    assert_eq!(reopened.metadata().created_at, n(500));
    assert_eq!(reopened.metadata().storage_version, n(1));
    assert_eq!(reopened.metadata().store_generation, n(0));
    assert!(ready(repo.create(options("z"))).is_err());
    let fork = ready(repo.fork(source.metadata(), request("fork", branch()))).unwrap();
    assert_eq!(fork.metadata().parent_session_id, Some(sid("z")));
    assert_eq!(fork.metadata().cwd, None);
    ready(repo.delete(fork.metadata())).unwrap();
    ready(repo.delete(fork.metadata())).unwrap();
    assert!(matches!(
        ready(repo.open(fork.metadata())),
        Err(SessionError::NotFound(_))
    ));
    let generated = ready(repo.create(SessionCreateOptions::default())).unwrap();
    assert_eq!(ids.0.load(Ordering::SeqCst), 1);
    assert!(pi_agent_session::id::uuid_timestamp(generated.metadata().id.as_str()).is_ok());
}

#[test]
fn repository_sessions_are_isolated_and_custom_schema_admission_is_actual() {
    let repo = repo();
    let a = ready(repo.create(options("a"))).unwrap();
    let b = ready(repo.create(options("b"))).unwrap();
    ready(a.set_custom_fact("same".into(), Some(JsonValue::Null))).unwrap();
    assert_eq!(
        ready(a.get_custom_fact("same")).unwrap(),
        Some(JsonValue::Null)
    );
    assert_eq!(ready(b.get_custom_fact("same")).unwrap(), None);
    let unknown: AgentMessage = serde_json::from_value(
        serde_json::json!({"role":"not-registered","body":"private-content"}),
    )
    .unwrap();
    let error = ready(a.append_message(unknown)).unwrap_err();
    assert!(matches!(error, SessionError::UnknownCustomRole(_)));
    assert!(!format!("{error:?} {error}").contains("private-content"));
    assert!(entries(&a).is_empty());
    let message_id = ready(a.append_message(user("hello"))).unwrap();
    assert!(ready(b.get_entry(message_id)).unwrap().is_none());
    assert_eq!(ready(a.get_stats()).unwrap().message_count, n(1));
    assert_eq!(ready(b.get_stats()).unwrap().message_count, n(0));
}

#[test]
fn fork_mints_new_metadata_time_and_registers_without_inheriting_source_options() {
    struct Advancing(AtomicU64);
    impl StorageClock for Advancing {
        fn now(&self) -> Result<SafeUnsigned, StorageError> {
            Ok(n(self.0.fetch_add(1, Ordering::SeqCst)))
        }
    }
    let clock = Arc::new(Advancing(AtomicU64::new(1000)));
    let repo = MemorySessionRepo::new(
        SessionCodecOptions::default(),
        ProviderHeaderPolicy::default(),
        Arc::new(Ids::default()),
        clock.clone(),
        Arc::new(pi_telemetry::NoopTelemetryContext),
    )
    .unwrap();
    let mut create = options("source");
    create.cwd = Some("source-only".into());
    let source = ready(repo.create(create)).unwrap();
    assert_eq!(source.metadata().created_at, n(1000));
    let fork = ready(repo.fork(source.metadata(), request("fork", branch()))).unwrap();
    assert_eq!(fork.metadata().created_at, n(1002));
    assert_eq!(fork.metadata().cwd, None);
    assert_eq!(fork.metadata().parent_session_id, Some(sid("source")));
    assert_eq!(clock.0.load(Ordering::SeqCst), 1004);
    ready(source.set_name(Some("changed later".into()))).unwrap();
    assert_eq!(ready(fork.get_name()).unwrap(), None);
}
