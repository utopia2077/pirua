use pi_agent_schema::session_values::*;
use pi_agent_schema::*;
use pi_agent_session::{
    backend_support::*, codec::SessionCodec, session::RuntimeSession, storage::*,
};
use std::{
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};
pub fn n(v: u64) -> SafeUnsigned {
    SafeUnsigned::new(v).unwrap()
}
pub fn ready<T>(future: impl std::future::Future<Output = T>) -> T {
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    match std::pin::pin!(future).as_mut().poll(&mut cx) {
        std::task::Poll::Ready(v) => v,
        _ => panic!("synchronous storage suspended"),
    }
}
pub struct Clock {
    pub value: AtomicU64,
    pub callback: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}
impl Clock {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            value: AtomicU64::new(1234),
            callback: Mutex::new(None),
        })
    }
}
impl StorageClock for Clock {
    fn now(&self) -> Result<SafeUnsigned, StorageError> {
        let cb = self.callback.lock().unwrap().take();
        if let Some(cb) = cb {
            cb();
        }
        Ok(n(self.value.load(Ordering::SeqCst)))
    }
}
pub struct Ids;
impl IdGenerator for Ids {
    fn next(&self, _: Option<SafeUnsigned>) -> String {
        panic!("unexpected ID allocation")
    }
}
pub fn metadata() -> SessionMetadata {
    serde_json::from_value(serde_json::json!({"id":"prepared-session","createdAt":1,"storageVersion":1,"storeGeneration":0})).unwrap()
}
pub fn codec() -> Arc<SessionCodec> {
    Arc::new(
        SessionCodec::new(
            SessionCodecOptions::default(),
            ProviderHeaderPolicy::default(),
        )
        .unwrap(),
    )
}
pub fn session<S: CheckedStorage + 'static>(s: Arc<S>) -> RuntimeSession<S> {
    ready(RuntimeSession::new(
        s,
        metadata(),
        codec(),
        Arc::new(Ids),
        Arc::new(NoopTelemetryContext),
    ))
    .unwrap()
}
pub struct Dir(pub PathBuf);
impl Dir {
    pub fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let p = std::env::temp_dir().join(format!(
            "pi-prepared-{}-{}",
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
pub fn set(key: &str) -> Write {
    Write::SetRegister(Box::new(RegisterSet::FactCustom {
        key: key.into(),
        value: JsonValue::Null,
    }))
}
pub fn entry(id: &str) -> NewEntry {
    NewEntry::Custom(NewCustomEntry {
        id: EntryId::new(id).unwrap(),
        parent_id: None,
        entry_type: CustomEntryType::Custom,
        custom_type: CustomType::new("test").unwrap(),
        data: Presence::Missing,
    })
}
pub fn request(writes: Vec<Write>) -> ConditionalCommit {
    ConditionalCommit {
        conditions: vec![],
        transaction: Transaction { writes },
    }
}
pub fn slots(request: &ConditionalCommit) -> Vec<PreparedEntrySlot> {
    request
        .transaction
        .writes
        .iter()
        .enumerate()
        .filter_map(|(write_index, write)| match write {
            Write::Entry(e) => Some(PreparedEntrySlot {
                write_index,
                entry: e.clone(),
            }),
            _ => None,
        })
        .collect()
}
pub fn prepare<S: RuntimeStorage>(s: &S, r: &ConditionalCommit) -> PreparedEntries {
    ready(s.prepare_entries(ready(s.read_stamp()).unwrap(), slots(r)))
        .unwrap()
        .unwrap()
}
pub struct Accept;
impl PublicationValidator for Accept {
    type Error = ();
    fn validate(&self, _: &StorageSnapshot) -> Result<(), ()> {
        Ok(())
    }
}
struct Reject;
impl PublicationValidator for Reject {
    type Error = ();
    fn validate(&self, _: &StorageSnapshot) -> Result<(), ()> {
        Err(())
    }
}

pub fn exact_and_stale<S: RuntimeStorage + 'static>(store: Arc<S>, clock: Arc<Clock>) {
    let s = session(store.clone());
    let r = request(vec![
        set("before"),
        Write::DeleteRegister(RegisterDelete {
            namespace: RegisterNamespace::FactCustom,
            key: "missing".into(),
        }),
        Write::Entry(entry("exact")),
        set("after"),
    ]);
    let before = ready(store.read_stamp()).unwrap();
    let unused = prepare(store.as_ref(), &r);
    assert_eq!(before, ready(store.read_stamp()).unwrap());
    drop(unused);
    let p = ready(s.prepare_entries(before.clone(), slots(&r)))
        .unwrap()
        .unwrap();
    let rows = p.entries().to_vec();
    let Entry::Custom(row) = &rows[0] else {
        panic!()
    };
    assert_eq!(row.seq, n(2));
    assert_eq!(row.timestamp, n(1234));
    clock.value.store(9876, Ordering::SeqCst);
    let result =
        ready(s.commit_prepared_conditional_with_telemetry(p, r, &NoopTelemetryContext)).unwrap();
    let ConditionalOutcome::Applied(result) = result else {
        panic!()
    };
    assert_eq!(result.timestamp, n(1234));
    assert_eq!(result.first_seq, n(0));
    assert_eq!(
        ready(store.get_entries(std::slice::from_ref(&row.id))).unwrap()[&row.id],
        rows[0]
    );
    let r = request(vec![Write::Entry(entry("after-race"))]);
    let stamp = ready(store.read_stamp()).unwrap();
    let p = prepare(store.as_ref(), &r);
    ready(store.commit(Transaction {
        writes: vec![set("unrelated-lane")],
    }))
    .unwrap();
    assert!(
        ready(store.prepare_entries(stamp, slots(&r)))
            .unwrap()
            .is_none()
    );
    assert_eq!(
        ready(store.commit_prepared_checked(p, r.clone(), &Accept)).unwrap(),
        ConditionalOutcome::NotApplied
    );
    assert!(
        ready(store.get_entries(&[EntryId::new("after-race").unwrap()]))
            .unwrap()
            .is_empty()
    );
    let p = prepare(store.as_ref(), &r);
    let rows = p.entries().to_vec();
    assert!(matches!(
        ready(store.commit_prepared_checked(p, r, &Accept)).unwrap(),
        ConditionalOutcome::Applied(_)
    ));
    let Entry::Custom(row) = &rows[0] else {
        panic!()
    };
    assert_eq!(
        ready(store.get_entries(std::slice::from_ref(&row.id))).unwrap()[&row.id],
        rows[0]
    );
}
pub fn inventory_and_validation<S: RuntimeStorage + 'static>(store: Arc<S>) {
    let base = request(vec![set("first"), Write::Entry(entry("bound"))]);
    let before = ready(store.read_stamp()).unwrap();
    for mode in 0..5 {
        let p = prepare(store.as_ref(), &base);
        let mut r = base.clone();
        match mode {
            0 => {
                r.transaction.writes.insert(0, set("shift"));
            }
            1 => {
                let Write::Entry(NewEntry::Custom(e)) = &mut r.transaction.writes[1] else {
                    panic!()
                };
                e.data = Presence::Value(JsonValue::Bool(true));
            }
            2 => {
                let Write::Entry(NewEntry::Custom(e)) = &mut r.transaction.writes[1] else {
                    panic!()
                };
                e.parent_id = Some(EntryId::new("parent").unwrap());
            }
            3 => r.transaction.writes.push(Write::Entry(entry("extra"))),
            _ => {
                r.transaction.writes.pop();
            }
        }
        assert!(matches!(
            ready(store.commit_prepared_checked(p, r, &Accept)),
            Err(CheckedCommitError::Storage(StorageError::InvalidData(_)))
        ));
        assert_eq!(before, ready(store.read_stamp()).unwrap());
    }
    let p = prepare(store.as_ref(), &base);
    assert!(matches!(
        ready(store.commit_prepared_checked(p, base.clone(), &Reject)),
        Err(CheckedCommitError::Rejected(()))
    ));
    let p = prepare(store.as_ref(), &base);
    let mut r = base.clone();
    r.conditions.push(RegisterCondition {
        register: RegisterKey {
            namespace: RegisterNamespace::FactCustom,
            key: "absent".into(),
        },
        expected: ExpectedRegister::Seq(n(0)),
    });
    assert_eq!(
        ready(store.commit_prepared_checked(p, r, &Accept)).unwrap(),
        ConditionalOutcome::NotApplied
    );
    // Existing Session pending/entry exclusivity must still reject the entire final state.
    let s = session(store.clone());
    let mut r = base.clone();
    r.transaction
        .writes
        .push(Write::SetRegister(Box::new(RegisterSet::PendingEntry {
            key: "bound".into(),
            value: PendingEntry {
                entry_type: PendingEntryType::Custom,
                custom_type: Some(CustomType::new("test").unwrap()),
                payload: Presence::Missing,
            },
        })));
    let p = prepare(store.as_ref(), &r);
    assert!(
        ready(s.commit_prepared_conditional_with_telemetry(p, r, &NoopTelemetryContext)).is_err()
    );
    assert_eq!(before, ready(store.read_stamp()).unwrap());
    // All final transaction codec validation still runs, including unrelated writes.
    let mut r = base;
    let message: AgentMessage =
        serde_json::from_value(serde_json::json!({"role":"unregistered","value":1})).unwrap();
    r.transaction
        .writes
        .push(Write::Entry(NewEntry::Message(NewMessageEntry {
            id: EntryId::new("unregistered").unwrap(),
            parent_id: None,
            entry_type: MessageEntryType::Message,
            message,
            terminate: None,
        })));
    let p = prepare(store.as_ref(), &r);
    assert!(matches!(
        ready(s.commit_prepared_conditional_with_telemetry(p, r, &NoopTelemetryContext)),
        Err(CheckedCommitError::Rejected(_))
    ));
    assert_eq!(before, ready(store.read_stamp()).unwrap());
}
pub fn foreign<S: RuntimeStorage>(first: &S, second: &S) {
    let r = request(vec![Write::Entry(entry("foreign"))]);
    let p = prepare(first, &r);
    assert_eq!(
        ready(second.commit_prepared_checked(p, r, &Accept)).unwrap(),
        ConditionalOutcome::NotApplied
    );
}
pub fn validator_race<S: RuntimeStorage + 'static>(store: Arc<S>) {
    struct Reenter<S>(Arc<S>);
    impl<S: RuntimeStorage + 'static> PublicationValidator for Reenter<S> {
        type Error = ();
        fn validate(&self, _: &StorageSnapshot) -> Result<(), ()> {
            ready(self.0.commit(Transaction {
                writes: vec![set("validator-race")],
            }))
            .unwrap();
            Ok(())
        }
    }
    let r = request(vec![Write::Entry(entry("staged-but-stale"))]);
    let p = prepare(store.as_ref(), &r);
    assert_eq!(
        ready(store.commit_prepared_checked(p, r, &Reenter(store.clone()))).unwrap(),
        ConditionalOutcome::NotApplied
    );
}
pub fn clock_race<S: RuntimeStorage + 'static>(store: Arc<S>, clock: Arc<Clock>) {
    let r = request(vec![Write::Entry(entry("clock-race"))]);
    let stamp = ready(store.read_stamp()).unwrap();
    let inner = store.clone();
    *clock.callback.lock().unwrap() = Some(Box::new(move || {
        ready(inner.commit(Transaction {
            writes: vec![set("during-clock")],
        }))
        .unwrap();
    }));
    assert!(
        ready(store.prepare_entries(stamp, slots(&r)))
            .unwrap()
            .is_none()
    );
}

pub fn usage_collision<S: RuntimeStorage + 'static>(store: Arc<S>) {
    let usage = Write::Usage(NewUsageRow {
        id: UsageId::new("reserved-usage").unwrap(),
        usage: Usage::zero(),
        entry_id: None,
        adjustment: true,
        details: Presence::Missing,
    });
    ready(store.commit(Transaction {
        writes: vec![usage.clone()],
    }))
    .unwrap();
    let r = request(vec![Write::Entry(entry("collision-must-not-place")), usage]);
    let p = prepare(store.as_ref(), &r);
    let stamp = ready(store.read_stamp()).unwrap();
    let s = session(store.clone());
    assert!(
        ready(s.commit_prepared_conditional_with_telemetry(p, r, &NoopTelemetryContext)).is_err()
    );
    assert_eq!(stamp, ready(store.read_stamp()).unwrap());
    assert!(
        ready(store.get_entries(&[EntryId::new("collision-must-not-place").unwrap()]))
            .unwrap()
            .is_empty()
    );
}
