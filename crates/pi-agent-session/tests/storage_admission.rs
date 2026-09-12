mod support;
use pi_agent_schema::session_values::*;
use pi_agent_schema::*;
use pi_agent_session::{memory::*, storage::*};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex, mpsc};
use support::*;

struct NoCoexist;
impl PublicationValidator for NoCoexist {
    type Error = &'static str;
    fn validate(&self, snapshot: &StorageSnapshot) -> Result<(), Self::Error> {
        let ids = ids(snapshot.entries.clone());
        if snapshot
            .registers
            .iter()
            .any(|register| matches!(register,Register::PendingEntry {key,..} if ids.contains(key)))
        {
            Err("pending and placed coexist")
        } else {
            Ok(())
        }
    }
}
fn pending(key: &str) -> Write {
    Write::SetRegister(Box::new(RegisterSet::PendingEntry {
        key: key.to_owned(),
        value: PendingEntry {
            entry_type: PendingEntryType::Custom,
            custom_type: Some(CustomType::new("x").unwrap()),
            payload: Presence::Missing,
        },
    }))
}
fn request(writes: Vec<Write>) -> ConditionalCommit {
    ConditionalCommit {
        conditions: vec![],
        transaction: tx(writes),
    }
}

#[test]
fn checked_publication_rejects_final_coexistence_but_accepts_placement() {
    let (_, storage) = memory();
    ready(storage.commit_checked(request(vec![pending("e")]), &NoCoexist)).unwrap();
    let before = bytes(&*storage);
    assert!(matches!(
        ready(storage.commit_checked(request(vec![custom("e", None)]), &NoCoexist)),
        Err(CheckedCommitError::Rejected("pending and placed coexist"))
    ));
    assert_eq!(bytes(&*storage), before);
    let delete = Write::DeleteRegister(RegisterDelete {
        namespace: RegisterNamespace::PendingEntry,
        key: "e".to_owned(),
    });
    assert!(matches!(
        ready(storage.commit_checked(request(vec![custom("e", None), delete]), &NoCoexist))
            .unwrap(),
        ConditionalOutcome::Applied(_)
    ));
    assert!(
        ready(storage.get_register(RegisterNamespace::PendingEntry, "e"))
            .unwrap()
            .is_none()
    );
    assert_eq!(ready(storage.get_entries(&[id("e")])).unwrap().len(), 1);
}

struct WaitValidator {
    barrier: Arc<Barrier>,
    release: Mutex<mpsc::Receiver<()>>,
    once: AtomicBool,
}
impl PublicationValidator for WaitValidator {
    type Error = std::convert::Infallible;
    fn validate(&self, _: &StorageSnapshot) -> Result<(), Self::Error> {
        if !self.once.swap(true, Ordering::SeqCst) {
            self.barrier.wait();
            self.release.lock().unwrap().recv().unwrap();
        }
        Ok(())
    }
}
#[test]
fn concurrent_multi_key_cas_has_exactly_one_winner_in_both_orders() {
    for winner in 0..2 {
        let (_, storage) = memory();
        ready(storage.commit(tx(vec![
            set("a", JsonValue::Null),
            set("b", JsonValue::Null),
        ])))
        .unwrap();
        let barrier = Arc::new(Barrier::new(3));
        let mut releases = vec![];
        let mut handles = vec![];
        for contender in 0..2 {
            let (send, recv) = mpsc::channel();
            releases.push(send);
            let store = storage.clone();
            let barrier = barrier.clone();
            handles.push(Some(std::thread::spawn(move || {
                let validator = WaitValidator {
                    barrier,
                    release: Mutex::new(recv),
                    once: AtomicBool::new(false),
                };
                ready(store.commit_checked(
                    ConditionalCommit {
                        conditions: vec![
                            condition("a", ExpectedRegister::Seq(n(0))),
                            condition("b", ExpectedRegister::Seq(n(1))),
                        ],
                        transaction: tx(vec![
                            custom(&format!("winner-{contender}"), None),
                            set("a", JsonValue::Null),
                            usage(&format!("cost-{contender}"), Usage::zero(), None),
                        ]),
                    },
                    &validator,
                ))
                .unwrap()
            })));
        }
        barrier.wait();
        releases[winner].send(()).unwrap();
        assert!(matches!(
            handles[winner].take().unwrap().join().unwrap(),
            ConditionalOutcome::Applied(_)
        ));
        let before = bytes(&*storage);
        releases[1 - winner].send(()).unwrap();
        assert_eq!(
            handles[1 - winner].take().unwrap().join().unwrap(),
            ConditionalOutcome::NotApplied
        );
        assert_eq!(bytes(&*storage), before);
        assert_eq!(
            ids(ready(storage.scan_entries(EntryScan::default())).unwrap()),
            vec![format!("winner-{winner}")]
        );
    }
}

struct Reenter {
    storage: Arc<MemoryStorage>,
    calls: AtomicUsize,
}
impl PublicationValidator for Reenter {
    type Error = &'static str;
    fn validate(&self, snapshot: &StorageSnapshot) -> Result<(), Self::Error> {
        ready(self.storage.stats()).unwrap(); // Demonstrates no callback under data lock.
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            ready(self.storage.commit(tx(vec![pending("e")]))).unwrap();
        }
        NoCoexist.validate(snapshot)
    }
}
#[test]
fn publication_validator_rechecks_revision_after_reentrant_write() {
    let (_, storage) = memory();
    let validator = Reenter {
        storage: storage.clone(),
        calls: AtomicUsize::new(0),
    };
    assert!(matches!(
        ready(storage.commit_checked(request(vec![custom("e", None)]), &validator)),
        Err(CheckedCommitError::Rejected(_))
    ));
    assert_eq!(validator.calls.load(Ordering::SeqCst), 2);
    assert!(ready(storage.get_entries(&[id("e")])).unwrap().is_empty());
    assert!(
        ready(storage.get_register(RegisterNamespace::PendingEntry, "e"))
            .unwrap()
            .is_some()
    );
    assert_eq!(ready(storage.snapshot()).unwrap().next_seq, Some(n(1)));
}

struct ClosingValidator {
    storage: Arc<MemoryStorage>,
}
impl PublicationValidator for ClosingValidator {
    type Error = std::convert::Infallible;
    fn validate(&self, _: &StorageSnapshot) -> Result<(), Self::Error> {
        ready(self.storage.close()).unwrap();
        Ok(())
    }
}
#[test]
fn closing_validator_cannot_publish_into_a_sealed_handle() {
    let (backing, storage) = memory();
    let validator = ClosingValidator {
        storage: storage.clone(),
    };
    assert!(matches!(
        ready(storage.commit_checked(request(vec![custom("e", None)]), &validator)),
        Err(CheckedCommitError::Storage(StorageError::Closed(_)))
    ));
    let reopened = backing.open(Arc::new(Clock)).unwrap();
    assert!(ready(reopened.snapshot()).unwrap().entries.is_empty());
}

struct BlockingClock {
    entered: mpsc::Sender<()>,
    release: Mutex<mpsc::Receiver<()>>,
}
impl StorageClock for BlockingClock {
    fn now(&self) -> Result<SafeUnsigned, StorageError> {
        self.entered.send(()).unwrap();
        self.release.lock().unwrap().recv().unwrap();
        Ok(n(5))
    }
}
#[test]
fn close_during_clock_rejects_old_work_and_cannot_close_successor() {
    let backing = Arc::new(MemoryBacking::new());
    let (entered, notice) = mpsc::channel();
    let (release, wait) = mpsc::channel();
    let storage = backing
        .open(Arc::new(BlockingClock {
            entered,
            release: Mutex::new(wait),
        }))
        .unwrap();
    let worker = storage.clone();
    let handle = std::thread::spawn(move || ready(worker.commit(tx(vec![custom("old", None)]))));
    notice.recv().unwrap();
    ready(storage.close()).unwrap();
    let next = backing.open(Arc::new(Clock)).unwrap();
    ready(next.commit(tx(vec![custom("new", None)]))).unwrap();
    release.send(()).unwrap();
    assert!(matches!(
        handle.join().unwrap(),
        Err(StorageError::Closed(_))
    ));
    ready(storage.close()).unwrap();
    assert_eq!(
        ids(ready(next.scan_entries(EntryScan::default())).unwrap()),
        vec!["new"]
    );
}

#[test]
fn unpolled_drop_same_arc_reopen_and_all_closed_reads() {
    let (backing, storage) = memory();
    assert!(Arc::ptr_eq(
        &storage,
        &backing.open(Arc::new(Clock)).unwrap()
    ));
    drop(storage.commit(tx(vec![custom("unpolled", None)])));
    assert!(ready(storage.snapshot()).unwrap().entries.is_empty());
    ready(storage.commit(tx(vec![custom("kept", None)]))).unwrap();
    let before = ready(storage.snapshot()).unwrap();
    ready(storage.close()).unwrap();
    ready(storage.close()).unwrap();
    assert!(matches!(
        ready(storage.commit(tx(vec![]))),
        Err(StorageError::Closed(_))
    ));
    assert!(ready(storage.get_entries(&[])).is_err());
    assert!(ready(storage.get_register(RegisterNamespace::FactName, "")).is_err());
    assert!(ready(storage.list_registers(RegisterNamespace::FactName, None)).is_err());
    assert!(ready(storage.scan_branch(BranchScan::default())).is_err());
    assert!(ready(storage.scan_branch_structure(BranchScan::default())).is_err());
    assert!(ready(storage.scan_entries(EntryScan::default())).is_err());
    assert!(ready(storage.scan_usage(UsageScan::default())).is_err());
    assert!(ready(storage.stats()).is_err());
    assert!(ready(storage.snapshot()).is_err());
    let next = backing.open(Arc::new(Clock)).unwrap();
    assert!(!Arc::ptr_eq(&storage, &next));
    assert_eq!(ready(next.snapshot()).unwrap(), before);
    drop(storage);
    assert_eq!(ready(next.snapshot()).unwrap(), before);
    drop(next);
    assert_eq!(
        ready(backing.open(Arc::new(Clock)).unwrap().snapshot()).unwrap(),
        before
    );
}

#[test]
fn snapshot_is_one_whole_prefix_while_candidate_is_unpublished() {
    let (_, storage) = memory();
    let barrier = Arc::new(Barrier::new(2));
    let (send, recv) = mpsc::channel();
    let worker = storage.clone();
    let worker_barrier = barrier.clone();
    let thread = std::thread::spawn(move || {
        let validator = WaitValidator {
            barrier: worker_barrier,
            release: Mutex::new(recv),
            once: AtomicBool::new(false),
        };
        ready(worker.commit_checked(
            request(vec![
                custom("entry", None),
                set("fact", JsonValue::Null),
                usage("cost", Usage::zero(), Some("entry")),
            ]),
            &validator,
        ))
        .unwrap()
    });
    barrier.wait();
    let before = ready(storage.snapshot()).unwrap();
    assert!(before.entries.is_empty() && before.registers.is_empty() && before.usage.is_empty());
    send.send(()).unwrap();
    thread.join().unwrap();
    let after = ready(storage.snapshot()).unwrap();
    assert_eq!(
        (
            after.entries.len(),
            after.registers.len(),
            after.usage.len()
        ),
        (1, 1, 1)
    );
    assert_eq!(after.next_seq, Some(n(3)));
}

#[test]
fn generic_session_consumer_requires_checked_backend_publication() {
    struct Consumer<'a, S: CheckedStorage>(&'a S);
    impl<S: CheckedStorage> Consumer<'_, S> {
        fn place(
            &self,
            writes: Vec<Write>,
        ) -> Result<ConditionalOutcome, CheckedCommitError<&'static str>> {
            ready(self.0.commit_checked(request(writes), &NoCoexist))
        }
    }
    let (_, storage) = memory();
    let consumer = Consumer(&*storage);
    consumer.place(vec![pending("entry")]).unwrap();
    let before = bytes(&*storage);
    assert!(matches!(
        consumer.place(vec![custom("entry", None)]),
        Err(CheckedCommitError::Rejected(_))
    ));
    assert_eq!(bytes(&*storage), before);
}
