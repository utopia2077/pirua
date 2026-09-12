use super::*;
use std::cell::RefCell;
thread_local! { pub(super) static HYDRATED: RefCell<Vec<EntryId>> = const { RefCell::new(Vec::new()) }; }
fn ready<T>(future: impl std::future::Future<Output = T>) -> T {
    let waker = std::task::Waker::noop();
    let mut cx = std::task::Context::from_waker(waker);
    match std::pin::pin!(future).as_mut().poll(&mut cx) {
        std::task::Poll::Ready(v) => v,
        _ => panic!("suspended"),
    }
}
struct Clock;
impl StorageClock for Clock {
    fn now(&self) -> Result<SafeUnsigned, StorageError> {
        Ok(SafeUnsigned::new(1).unwrap())
    }
}
#[test]
fn branch_payload_hydration_stops_at_newest_compaction_and_structure_hydrates_none() {
    let backing = Arc::new(MemoryBacking::new());
    let storage = backing.open(Arc::new(Clock)).unwrap();
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../conformance/session/storage-contract.json"
    ))
    .unwrap();
    let writes = serde_json::from_value(fixture["writes"].clone()).unwrap();
    ready(storage.commit(Transaction { writes })).unwrap();
    HYDRATED.with(|seen| seen.borrow_mut().clear());
    let q = BranchScan {
        start: Some(EntryId::new("D").unwrap()),
        stop_at_type: Some(EntryType::Compaction),
        ..Default::default()
    };
    ready(storage.scan_branch_structure(q.clone())).unwrap();
    HYDRATED.with(|seen| assert!(seen.borrow().is_empty()));
    ready(storage.scan_branch(q)).unwrap();
    HYDRATED.with(|seen| {
        assert_eq!(
            seen.borrow()
                .iter()
                .map(EntryId::as_str)
                .collect::<Vec<_>>(),
            vec!["D", "C"]
        )
    });
}
#[test]
fn poisoned_state_never_silently_reopens() {
    let backing = Arc::new(MemoryBacking::new());
    let storage = backing.open(Arc::new(Clock)).unwrap();
    let poison = backing.clone();
    assert!(
        std::thread::spawn(move || {
            let _lock = poison.inner.lock().unwrap();
            panic!("controlled lock poison")
        })
        .join()
        .is_err()
    );
    assert!(matches!(
        ready(storage.stats()),
        Err(StorageError::Corruption(_))
    ));
    assert!(matches!(
        backing.open(Arc::new(Clock)),
        Err(StorageError::Corruption(_))
    ));
}
