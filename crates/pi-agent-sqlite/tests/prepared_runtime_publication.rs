mod prepared_runtime_support;
use pi_agent_schema::*;
use pi_agent_session::storage::*;
use pi_agent_sqlite::{SqliteOptions, SqliteStorage};
use prepared_runtime_support::*;

#[test]
fn sqlite_exact_rows_races_inventory_and_physical_reopen() {
    let dir = Dir::new();
    let path = dir.0.join("session.db");
    let clock = Clock::new();
    let store = SqliteStorage::create(
        &path,
        metadata(),
        codec(),
        clock.clone(),
        SqliteOptions::default(),
    )
    .unwrap();
    exact_and_stale(store.clone(), clock.clone());
    inventory_and_validation(store.clone());
    usage_collision(store.clone());
    validator_race(store.clone());
    clock_race(store.clone(), clock.clone());
    let other = SqliteStorage::create(
        dir.0.join("other.db"),
        metadata(),
        codec(),
        Clock::new(),
        SqliteOptions::default(),
    )
    .unwrap();
    foreign(store.as_ref(), other.as_ref());
    ready(other.close()).unwrap();
    let r = request(vec![Write::Entry(entry("reopened"))]);
    let p = prepare(store.as_ref(), &r);
    let before = ready(store.snapshot()).unwrap();
    ready(store.close()).unwrap();
    let reopened = SqliteStorage::open(&path, codec(), clock, SqliteOptions::default()).unwrap();
    assert_eq!(before, ready(reopened.snapshot()).unwrap());
    assert_eq!(
        ready(reopened.commit_prepared_checked(p, r, &Accept)).unwrap(),
        ConditionalOutcome::NotApplied
    );
    ready(reopened.close()).unwrap();
}
#[test]
fn sqlite_preparation_is_header_only_and_expired_lease_cannot_publish() {
    let dir = Dir::new();
    let path = dir.0.join("session.db");
    let store = SqliteStorage::create(
        &path,
        metadata(),
        codec(),
        Clock::new(),
        SqliteOptions::default(),
    )
    .unwrap();
    ready(store.commit(Transaction {
        writes: vec![Write::Entry(entry("history"))],
    }))
    .unwrap();
    let stamp = ready(store.read_stamp()).unwrap();
    let db = rusqlite::Connection::open(&path).unwrap();
    db.execute("UPDATE entries SET payload='{'", []).unwrap();
    let r = request(vec![Write::Entry(entry("unpublished"))]);
    let p = ready(store.prepare_entries(stamp, slots(&r)))
        .unwrap()
        .unwrap();
    assert_eq!(p.entries().len(), 1);
    assert!(ready(store.snapshot()).is_err());
    // This test's private database: expire actual owner lease after preparation,
    // without changing publication revision or the frozen application row clock.
    db.execute("UPDATE writer_lease SET expires_at_ms=0", [])
        .unwrap();
    assert!(matches!(
        ready(store.commit_prepared_checked(p, r, &Accept)),
        Err(CheckedCommitError::Storage(StorageError::Closed(_)))
    ));
    ready(store.close()).unwrap();
}
#[test]
fn sqlite_final_publish_rechecks_fresh_lease_after_detached_validation() {
    let dir = Dir::new();
    let path = dir.0.join("session.db");
    let store = SqliteStorage::create(
        &path,
        metadata(),
        codec(),
        Clock::new(),
        SqliteOptions::default(),
    )
    .unwrap();
    struct Expire(std::path::PathBuf);
    impl PublicationValidator for Expire {
        type Error = ();
        fn validate(&self, _: &StorageSnapshot) -> Result<(), ()> {
            rusqlite::Connection::open(&self.0)
                .unwrap()
                .execute("UPDATE writer_lease SET expires_at_ms=0", [])
                .unwrap();
            Ok(())
        }
    }
    let r = request(vec![Write::Entry(entry("not-published"))]);
    let p = prepare(store.as_ref(), &r);
    assert!(matches!(
        ready(store.commit_prepared_checked(p, r, &Expire(path.clone()))),
        Err(CheckedCommitError::Storage(StorageError::Closed(_)))
    ));
    let db = rusqlite::Connection::open(path).unwrap();
    let count: i64 = db
        .query_row("SELECT count(*) FROM entries", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 0);
    ready(store.close()).unwrap();
}
