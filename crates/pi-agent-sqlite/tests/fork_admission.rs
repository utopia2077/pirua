//! Complete source admission and parent options are checked before fork projection.
use pi_agent_schema::session_values::*;
use pi_agent_schema::*;
use pi_agent_session::{backend_support::*, codec::SessionCodec, session::RuntimeSession};
use pi_agent_sqlite::{
    SqliteOptions, SqliteStorage,
    repo::{SqliteSessionMetadata, SqliteSessionRepo},
};
use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
fn n(v: u64) -> SafeUnsigned {
    SafeUnsigned::new(v).unwrap()
}
fn sid(v: &str) -> SessionId {
    SessionId::new(v).unwrap()
}
fn ready<T>(f: impl std::future::Future<Output = T>) -> T {
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    match std::pin::pin!(f).as_mut().poll(&mut cx) {
        std::task::Poll::Ready(v) => v,
        _ => panic!("first-poll completion required"),
    }
}
struct Dir(PathBuf);
impl Dir {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let p = std::env::temp_dir().join(format!(
            "pi-fork-admission-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir(&p).unwrap();
        Self(p)
    }
}
impl Drop for Dir {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
#[derive(Default)]
struct Clock(AtomicUsize);
impl StorageClock for Clock {
    fn now(&self) -> Result<SafeUnsigned, StorageError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(n(100))
    }
}
#[derive(Default)]
struct Ids(AtomicUsize);
impl IdGenerator for Ids {
    fn next(&self, _: Option<SafeUnsigned>) -> String {
        format!(
            "00000000-0001-7000-8000-{:012x}",
            self.0.fetch_add(1, Ordering::SeqCst)
        )
    }
}
fn codec() -> Arc<SessionCodec> {
    Arc::new(
        SessionCodec::new(
            SessionCodecOptions::default(),
            ProviderHeaderPolicy::default(),
        )
        .unwrap(),
    )
}
fn repo(
    path: &Path,
    codec: Arc<SessionCodec>,
    ids: Arc<Ids>,
    clock: Arc<Clock>,
) -> SqliteSessionRepo {
    SqliteSessionRepo::with_codec(
        path,
        codec,
        ids,
        clock,
        Arc::new(NoopTelemetryContext),
        SqliteOptions::default(),
    )
    .unwrap()
}
fn custom(id: &str) -> Write {
    Write::Entry(NewEntry::Custom(NewCustomEntry {
        id: EntryId::new(id).unwrap(),
        parent_id: None,
        entry_type: CustomEntryType::Custom,
        custom_type: CustomType::new("probe").unwrap(),
        data: Presence::Missing,
    }))
}
fn pending(id: &str) -> Write {
    Write::SetRegister(Box::new(RegisterSet::PendingEntry {
        key: id.into(),
        value: PendingEntry {
            entry_type: PendingEntryType::Custom,
            custom_type: Some(CustomType::new("probe").unwrap()),
            payload: Presence::Missing,
        },
    }))
}
fn metadata(id: &str) -> SessionMetadata {
    SessionMetadata {
        id: sid(id),
        created_at: n(1),
        storage_version: n(1),
        store_generation: n(0),
        cwd: None,
        parent_session_id: None,
        legacy_parent_session_path: None,
    }
}
fn source(
    dir: &Dir,
    pending_id: &str,
    codec: Arc<SessionCodec>,
) -> (Arc<SqliteStorage>, SqliteSessionMetadata) {
    let path = dir.0.join("source.sqlite");
    let base = metadata("source");
    let raw = SqliteStorage::create(
        &path,
        base.clone(),
        codec,
        Arc::new(Clock::default()),
        SqliteOptions::default(),
    )
    .unwrap();
    let mut writes = vec![custom("A"), pending(pending_id)];
    writes.extend(lane_seed_writes(
        "main",
        Some(EntryId::new("A").unwrap()),
        None,
    ));
    ready(raw.commit(Transaction { writes })).unwrap();
    (raw, SqliteSessionMetadata { base, path })
}
fn request(
    id: Option<&str>,
    parent: Option<SessionId>,
) -> SessionForkRequest<SessionCreateOptions> {
    SessionForkRequest {
        fork: ForkOptions::Branch(BranchForkOptions::default()),
        create: SessionCreateOptions {
            id: id.map(sid),
            parent_session_id: parent,
            cwd: None,
        },
    }
}
fn names(path: &Path) -> Vec<std::ffi::OsString> {
    let mut names: Vec<_> = std::fs::read_dir(path)
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect();
    names.sort();
    names
}
fn source_namespace(path: &Path) -> Vec<std::ffi::OsString> {
    // Read-only WAL access may create only these source-owned bookkeeping files.
    // Every destination/temp/coordination name remains part of the comparison.
    names(path)
        .into_iter()
        .filter(|name| name != "source.sqlite-wal" && name != "source.sqlite-shm")
        .collect()
}
#[test]
fn complete_invalid_source_rejects_before_projection_or_destination_callbacks() {
    let d = Dir::new();
    let codec = codec();
    let (raw, m) = source(&d, "A", codec.clone());
    let before = SqliteStorage::read_snapshot(&m.path, &codec, SqliteOptions::default()).unwrap();
    assert_eq!(before.snapshot.entries.len(), 1);
    assert!(
        before
            .snapshot
            .registers
            .iter()
            .any(|r| matches!(r,Register::PendingEntry{key,..}if key=="A"))
    );
    assert!(matches!(
        ready(RuntimeSession::new(
            raw.clone(),
            m.base.clone(),
            codec.clone(),
            Arc::new(Ids::default()),
            Arc::new(NoopTelemetryContext)
        )),
        Err(SessionError::Schema(_))
    ));
    ready(raw.close()).unwrap();
    let ids = Arc::new(Ids::default());
    let clock = Arc::new(Clock::default());
    let r = repo(&d.0, codec.clone(), ids.clone(), clock.clone());
    assert!(matches!(
        ready(r.open(m.clone())),
        Err(SessionError::Schema(_))
    ));
    let reopened = SqliteStorage::open(
        &m.path,
        codec.clone(),
        Arc::new(Clock::default()),
        SqliteOptions::default(),
    )
    .unwrap();
    ready(reopened.close()).unwrap();
    let files = source_namespace(&d.0);
    for id in [Some("rejected"), None] {
        let result = ready(r.fork(m.clone(), request(id, None)));
        assert!(
            matches!(result, Err(SessionError::Schema(_))),
            "complete source rejection must precede projection"
        );
        assert_eq!(source_namespace(&d.0), files);
        assert_eq!(
            SqliteStorage::read_snapshot(&m.path, &codec, SqliteOptions::default())
                .unwrap()
                .snapshot,
            before.snapshot
        );
        assert_eq!(ids.0.load(Ordering::SeqCst), 0);
        assert_eq!(clock.0.load(Ordering::SeqCst), 0);
    }
    assert_eq!(ready(r.list(())).unwrap().len(), 1);
}
#[test]
fn contradictory_parent_rejects_without_callbacks_or_source_change() {
    let d = Dir::new();
    let codec = codec();
    let ids = Arc::new(Ids::default());
    let clock = Arc::new(Clock::default());
    let r = repo(&d.0, codec.clone(), ids.clone(), clock.clone());
    let source = ready(r.create(SessionCreateOptions {
        id: Some(sid("valid")),
        ..Default::default()
    }))
    .unwrap();
    let m = source.metadata();
    let view = source.view(LaneId::new("main").unwrap());
    let before = SqliteStorage::read_snapshot(&m.path, &codec, SqliteOptions::default()).unwrap();
    let files = names(&d.0);
    let calls = clock.0.load(Ordering::SeqCst);
    for id in [Some("rejected-parent"), None] {
        assert!(matches!(
            ready(r.fork(m.clone(), request(id, Some(sid("different"))))),
            Err(SessionError::InvalidOperation(_))
        ));
        assert_eq!(names(&d.0), files);
        assert_eq!(ids.0.load(Ordering::SeqCst), 0);
        assert_eq!(clock.0.load(Ordering::SeqCst), calls);
        assert_eq!(
            SqliteStorage::read_snapshot(&m.path, &codec, SqliteOptions::default())
                .unwrap()
                .snapshot,
            before.snapshot
        );
    }
    let mut missing = m.clone();
    missing.path = d.0.join("missing.sqlite");
    assert!(
        matches!(
            ready(r.fork(missing, request(None, Some(sid("different"))))),
            Err(SessionError::InvalidOperation(_))
        ),
        "parent admission precedes source lookup too"
    );
    assert_eq!(ids.0.load(Ordering::SeqCst), 0);
    assert_eq!(clock.0.load(Ordering::SeqCst), calls);
    assert!(ready(view.get_leaf_id()).is_ok());
    assert_eq!(
        ready(source.commit(Transaction {
            writes: vec![custom("after")]
        }))
        .unwrap()
        .first_seq,
        n(2)
    );
    ready(source.close()).unwrap();
}
#[test]
fn valid_unplaced_pending_accepts_matching_and_omitted_parent() {
    let d = Dir::new();
    let codec = codec();
    let (raw, m) = source(&d, "queued", codec.clone());
    let before = SqliteStorage::read_snapshot(&m.path, &codec, SqliteOptions::default()).unwrap();
    let direct = ready(RuntimeSession::new(
        raw.clone(),
        m.base.clone(),
        codec.clone(),
        Arc::new(Ids::default()),
        Arc::new(NoopTelemetryContext),
    ))
    .unwrap();
    ready(direct.close()).unwrap();
    let ids = Arc::new(Ids::default());
    let clock = Arc::new(Clock::default());
    let r = repo(&d.0, codec.clone(), ids.clone(), clock.clone());
    let active = ready(r.open(m.clone())).unwrap();
    let view = active.view(LaneId::new("main").unwrap());
    for (id, parent) in [("omitted", None), ("matching", Some(m.base.id.clone()))] {
        let fork = ready(r.fork(m.clone(), request(Some(id), parent)))
            .expect("valid queued-only source must remain forkable");
        let fm = fork.metadata();
        assert_eq!(fm.base.parent_session_id, Some(m.base.id.clone()));
        let after =
            SqliteStorage::read_snapshot(&fm.path, &codec, SqliteOptions::default()).unwrap();
        assert_eq!(after.snapshot.entries, before.snapshot.entries);
        assert!(
            after
                .snapshot
                .registers
                .iter()
                .all(|r| !matches!(r, Register::PendingEntry { .. }))
        );
        assert!(after.snapshot.usage.is_empty());
        assert_eq!(after.snapshot.stats.usage, Usage::zero());
        assert_eq!(after.snapshot.next_seq, Some(n(3)));
        ready(fork.close()).unwrap();
    }
    assert_eq!(clock.0.load(Ordering::SeqCst), 2);
    assert_eq!(ids.0.load(Ordering::SeqCst), 0);
    assert_eq!(
        SqliteStorage::read_snapshot(&m.path, &codec, SqliteOptions::default())
            .unwrap()
            .snapshot,
        before.snapshot
    );
    assert!(ready(view.get_leaf_id()).is_ok());
    ready(active.commit(Transaction {
        writes: vec![custom("after-valid-forks")],
    }))
    .unwrap();
    ready(active.close()).unwrap();
}
#[test]
fn matching_forged_parent_does_not_skip_actual_source_header_id() {
    let d = Dir::new();
    let codec = codec();
    let ids = Arc::new(Ids::default());
    let clock = Arc::new(Clock::default());
    let r = repo(&d.0, codec.clone(), ids.clone(), clock.clone());
    let root = ready(r.create(SessionCreateOptions {
        id: Some(sid("actual")),
        ..Default::default()
    }))
    .unwrap();
    let m = root.metadata();
    let before = SqliteStorage::read_snapshot(&m.path, &codec, SqliteOptions::default()).unwrap();
    let files = names(&d.0);
    let calls = clock.0.load(Ordering::SeqCst);
    let mut forged = m.clone();
    forged.base.id = sid("forged");
    assert!(matches!(
        ready(r.fork(forged, request(None, Some(sid("forged"))))),
        Err(SessionError::InvalidOperation(_))
    ));
    assert_eq!(names(&d.0), files);
    assert_eq!(clock.0.load(Ordering::SeqCst), calls);
    assert_eq!(ids.0.load(Ordering::SeqCst), 0);
    assert_eq!(
        SqliteStorage::read_snapshot(&m.path, &codec, SqliteOptions::default())
            .unwrap()
            .snapshot,
        before.snapshot
    );
    assert!(ready(root.get_leaf_id()).is_ok());
    ready(root.close()).unwrap();
}
