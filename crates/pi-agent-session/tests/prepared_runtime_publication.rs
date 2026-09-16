mod prepared_runtime_support;
use pi_agent_schema::*;
use pi_agent_session::{
    jsonl::{JsonlOptions, JsonlStorage},
    memory::MemoryBacking,
    storage::*,
};
use prepared_runtime_support::*;
use std::sync::Arc;

#[test]
fn memory_exact_rows_races_inventory_and_validation() {
    let clock = Clock::new();
    let backing = Arc::new(MemoryBacking::new());
    let store = backing.open(clock.clone()).unwrap();
    exact_and_stale(store.clone(), clock.clone());
    inventory_and_validation(store.clone());
    usage_collision(store.clone());
    validator_race(store.clone());
    clock_race(store.clone(), clock.clone());
    let other = Arc::new(MemoryBacking::new()).open(Clock::new()).unwrap();
    foreign(store.as_ref(), other.as_ref());
    let r = request(vec![Write::Entry(entry("reopened"))]);
    let p = prepare(store.as_ref(), &r);
    ready(store.close()).unwrap();
    assert!(matches!(
        ready(store.prepare_entries(ready(other.read_stamp()).unwrap(), slots(&r))),
        Err(StorageError::Closed(_))
    ));
    let reopened = backing.open(clock).unwrap();
    assert_eq!(
        ready(reopened.commit_prepared_checked(p, r, &Accept)).unwrap(),
        ConditionalOutcome::NotApplied
    );
}
#[test]
fn jsonl_exact_rows_races_inventory_and_physical_reopen() {
    let dir = Dir::new();
    let path = dir.0.join("session.jsonl");
    let clock = Clock::new();
    let store = JsonlStorage::create(
        &path,
        metadata(),
        codec(),
        clock.clone(),
        JsonlOptions::default(),
    )
    .unwrap();
    exact_and_stale(store.clone(), clock.clone());
    inventory_and_validation(store.clone());
    usage_collision(store.clone());
    validator_race(store.clone());
    clock_race(store.clone(), clock.clone());
    let other = JsonlStorage::create(
        dir.0.join("other.jsonl"),
        metadata(),
        codec(),
        Clock::new(),
        JsonlOptions::default(),
    )
    .unwrap();
    foreign(store.as_ref(), other.as_ref());
    ready(other.close()).unwrap();
    let r = request(vec![Write::Entry(entry("reopened"))]);
    let p = prepare(store.as_ref(), &r);
    let before = ready(store.snapshot()).unwrap();
    ready(store.close()).unwrap();
    assert!(matches!(
        ready(store.commit_prepared_checked(prepare_for_closed_other(), r.clone(), &Accept)),
        Err(CheckedCommitError::Storage(StorageError::Closed(_)))
    ));
    let reopened = JsonlStorage::open(&path, codec(), clock, JsonlOptions::default()).unwrap();
    assert_eq!(before, ready(reopened.snapshot()).unwrap());
    assert_eq!(
        ready(reopened.commit_prepared_checked(p, r, &Accept)).unwrap(),
        ConditionalOutcome::NotApplied
    );
    ready(reopened.close()).unwrap();
}
fn prepare_for_closed_other() -> PreparedEntries {
    let s = Arc::new(MemoryBacking::new()).open(Clock::new()).unwrap();
    prepare(s.as_ref(), &request(vec![Write::Entry(entry("reopened"))]))
}

#[test]
fn preparation_uses_backend_private_provenance_and_checked_ordinals() {
    let s = Arc::new(MemoryBacking::new()).open(Clock::new()).unwrap();
    let stamp = ready(s.read_stamp()).unwrap();
    let r = request(vec![Write::Entry(entry("forged"))]);
    let forged = PreparedEntryAuthority::new()
        .prepare(stamp.clone(), Some(n(0)), n(99999), slots(&r))
        .unwrap();
    assert_eq!(
        ready(s.commit_prepared_checked(forged, r, &Accept)).unwrap(),
        ConditionalOutcome::NotApplied
    );
    let duplicate = vec![
        PreparedEntrySlot {
            write_index: 0,
            entry: entry("one"),
        },
        PreparedEntrySlot {
            write_index: 0,
            entry: entry("two"),
        },
    ];
    assert!(ready(s.prepare_entries(stamp.clone(), duplicate)).is_err());
    assert!(
        ready(s.prepare_entries(
            stamp,
            vec![PreparedEntrySlot {
                write_index: usize::MAX,
                entry: entry("overflow")
            }]
        ))
        .is_err()
    );
}

#[test]
fn session_preparation_observes_only_definitive_applied_writes() {
    use pi_telemetry::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Parent(Arc<AtomicUsize>);
    impl TelemetryContext for Parent {
        fn start_span(&self, o: SpanOptions) -> SpanGuard {
            assert_eq!(o.name, "pi.session.write");
            self.0.fetch_add(1, Ordering::SeqCst);
            NOOP_TELEMETRY_CONTEXT.start_span(o)
        }
    }
    let raw = Arc::new(MemoryBacking::new()).open(Clock::new()).unwrap();
    let s = session(raw.clone());
    let count = Arc::new(AtomicUsize::new(0));
    let parent = Parent(count.clone());
    let r = request(vec![Write::Entry(entry("observed"))]);
    let p = ready(s.prepare_entries(ready(s.read_stamp()).unwrap(), slots(&r)))
        .unwrap()
        .unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 0);
    ready(raw.commit(Transaction {
        writes: vec![set("race")],
    }))
    .unwrap();
    assert_eq!(
        ready(s.commit_prepared_conditional_with_telemetry(p, r.clone(), &parent)).unwrap(),
        ConditionalOutcome::NotApplied
    );
    assert_eq!(count.load(Ordering::SeqCst), 0);
    let p = ready(s.prepare_entries(ready(s.read_stamp()).unwrap(), slots(&r)))
        .unwrap()
        .unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 0);
    assert!(matches!(
        ready(s.commit_prepared_conditional_with_telemetry(p, r, &parent)).unwrap(),
        ConditionalOutcome::Applied(_)
    ));
    assert_eq!(count.load(Ordering::SeqCst), 1);
}

#[test]
fn prepared_legacy_first_write_preserves_previewed_row() {
    use pi_agent_schema::session_values::*;
    use pi_agent_session::repo::jsonl::JsonlSessionRepo;
    use std::sync::atomic::{AtomicU64, Ordering};
    struct LegacyIds(AtomicU64);
    impl IdGenerator for LegacyIds {
        fn next(&self, _: Option<SafeUnsigned>) -> String {
            format!("new-{}", self.0.fetch_add(1, Ordering::SeqCst))
        }
    }
    let dir = Dir::new();
    let path = dir.0.join("legacy.jsonl");
    let bytes=b"{\"type\":\"session\",\"version\":3,\"id\":\"legacy\",\"timestamp\":\"1970-01-01T00:00:00.001Z\",\"cwd\":\"source\"}\n{\"type\":\"custom\",\"id\":\"old\",\"parentId\":null,\"timestamp\":\"1970-01-01T00:00:00.002Z\",\"customType\":\"note\",\"data\":{}}\n";
    std::fs::write(&path, bytes).unwrap();
    let clock = Clock::new();
    let repo = JsonlSessionRepo::new(
        &dir.0,
        SessionCodecOptions::default(),
        ProviderHeaderPolicy::default(),
        Arc::new(LegacyIds(AtomicU64::new(0))),
        clock.clone(),
        Arc::new(pi_telemetry::NoopTelemetryContext),
        JsonlOptions::default(),
    )
    .unwrap();
    let s = ready(repo.open_path(&path)).unwrap();
    let r = request(vec![
        set("before-entry"),
        Write::Entry(entry("prepared-legacy")),
    ]);
    let p = ready(s.prepare_entries(ready(s.read_stamp()).unwrap(), slots(&r)))
        .unwrap()
        .unwrap();
    let rows = p.entries().to_vec();
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
    clock.value.store(77777, Ordering::SeqCst);
    ready(s.commit_prepared_conditional_with_telemetry(p, r, &pi_telemetry::NoopTelemetryContext))
        .unwrap();
    let Entry::Custom(row) = &rows[0] else {
        panic!()
    };
    assert_eq!(
        ready(s.get_entries(std::slice::from_ref(&row.id))).unwrap()[&row.id],
        rows[0]
    );
    ready(s.close()).unwrap();
    let stored = std::fs::read(&path).unwrap();
    let header: serde_json::Value =
        serde_json::from_slice(stored.split(|b| *b == b'\n').next().unwrap()).unwrap();
    assert_eq!(header["v"], 4);
    let reopened = ready(repo.open_path(&path)).unwrap();
    assert_eq!(
        ready(reopened.get_entries(std::slice::from_ref(&row.id))).unwrap()[&row.id],
        rows[0]
    );
    ready(reopened.close()).unwrap();
}
