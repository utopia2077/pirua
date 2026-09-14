//! Existing callback recheck guarantees, including independent568's three histories.
use pi_agent_schema::session_values::{JsonValue, Presence, SafeUnsigned};
use pi_agent_schema::*;
use pi_agent_session::{
    memory::{MemoryBacking, MemoryStorage, StorageClock},
    storage::*,
};
use std::sync::{
    Arc, Mutex, Weak,
    atomic::{AtomicUsize, Ordering},
};
fn n(value: u64) -> SafeUnsigned {
    SafeUnsigned::new(value).unwrap()
}
fn ready<T>(future: impl std::future::Future<Output = T>) -> T {
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    match std::pin::pin!(future).as_mut().poll(&mut context) {
        std::task::Poll::Ready(value) => value,
        _ => panic!("Memory must finish on first poll"),
    }
}
fn entry(id: &str, parent: Option<&str>) -> Write {
    Write::Entry(NewEntry::Custom(NewCustomEntry {
        id: EntryId::new(id).unwrap(),
        parent_id: parent.map(|id| EntryId::new(id).unwrap()),
        entry_type: CustomEntryType::Custom,
        custom_type: CustomType::new("clock-regression").unwrap(),
        data: Presence::Missing,
    }))
}
fn transaction(writes: Vec<Write>) -> Transaction {
    Transaction { writes }
}
fn request(writes: Vec<Write>) -> ConditionalCommit {
    ConditionalCommit {
        conditions: vec![],
        transaction: transaction(writes),
    }
}
fn condition(expected: ExpectedRegister) -> RegisterCondition {
    RegisterCondition {
        register: RegisterKey {
            namespace: RegisterNamespace::FactCustom,
            key: "gate".into(),
        },
        expected,
    }
}
#[derive(Debug)]
struct ClockCause;
impl std::fmt::Display for ClockCause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("synthetic clock cause")
    }
}
impl std::error::Error for ClockCause {}
fn failure() -> StorageError {
    StorageError::Io(
        ErrorContext::with_source("synthetic clock failure", Arc::new(ClockCause)).unwrap(),
    )
}
enum Mode {
    Close,
    Revision,
    Condition,
    Stable,
}
struct Clock {
    mode: Mode,
    target: Mutex<Weak<MemoryStorage>>,
    calls: AtomicUsize,
}
impl StorageClock for Clock {
    fn now(&self) -> Result<SafeUnsigned, StorageError> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        let target = self.target.lock().unwrap().upgrade().unwrap();
        match (&self.mode, call) {
            (Mode::Close, 0) => {
                ready(target.close()).unwrap();
                Err(failure())
            }
            (Mode::Revision, 0) => {
                let result = ready(target.commit(transaction(vec![entry("inner", None)]))).unwrap();
                assert_eq!(result.first_seq, n(0));
                assert_eq!(result.timestamp, n(101));
                Err(failure())
            }
            (Mode::Condition, 0) => {
                let result = ready(target.commit(transaction(vec![Write::SetRegister(Box::new(
                    RegisterSet::FactCustom {
                        key: "gate".into(),
                        value: JsonValue::Null,
                    },
                ))])))
                .unwrap();
                assert_eq!(result.first_seq, n(0));
                Err(failure())
            }
            (Mode::Stable, _) => Err(failure()),
            _ => Ok(n(100 + call as u64)),
        }
    }
}
struct Fixed;
impl StorageClock for Fixed {
    fn now(&self) -> Result<SafeUnsigned, StorageError> {
        Ok(n(700))
    }
}
fn setup(mode: Mode) -> (Arc<MemoryBacking>, Arc<MemoryStorage>, Arc<Clock>) {
    let backing = Arc::new(MemoryBacking::new());
    let clock = Arc::new(Clock {
        mode,
        target: Mutex::new(Weak::new()),
        calls: AtomicUsize::new(0),
    });
    let storage = backing.open(clock.clone()).unwrap();
    *clock.target.lock().unwrap() = Arc::downgrade(&storage);
    (backing, storage, clock)
}
#[derive(Default)]
struct Validator {
    stages: Mutex<Vec<StorageSnapshot>>,
}
impl PublicationValidator for Validator {
    type Error = &'static str;
    fn validate(&self, stage: &StorageSnapshot) -> Result<(), Self::Error> {
        self.stages.lock().unwrap().push(stage.clone());
        Ok(())
    }
}
fn placed(snapshot: &StorageSnapshot) -> Vec<(String, u64, u64)> {
    snapshot
        .entries
        .iter()
        .map(|e| match e {
            Entry::Custom(e) => (e.id.to_string(), e.seq.get(), e.timestamp.get()),
            _ => panic!("unexpected fixture entry"),
        })
        .collect()
}
#[test]
fn closing_clock_error_rechecks_incarnation_before_returning_error() {
    let (backing, storage, clock) = setup(Mode::Close);
    let before = ready(storage.snapshot()).unwrap();
    let validator = Validator::default();
    let result = ready(storage.commit_checked(request(vec![entry("outer", None)]), &validator));
    let next = backing.open(Arc::new(Fixed)).unwrap();
    let after = ready(next.snapshot()).unwrap();
    ready(storage.close()).unwrap();
    let successor = ready(next.commit(transaction(vec![entry("successor", None)]))).unwrap();
    ready(next.close()).unwrap();
    assert_eq!(before, after);
    assert_eq!(successor.first_seq, n(0));
    assert_eq!(clock.calls.load(Ordering::SeqCst), 1);
    assert!(validator.stages.lock().unwrap().is_empty());
    assert!(
        matches!(
            result,
            Err(CheckedCommitError::Storage(StorageError::Closed(_)))
        ),
        "close must dominate clock error: {result:?}"
    );
}
#[test]
fn revision_changing_clock_error_retries_without_validating_failed_preparation() {
    let (_backing, storage, clock) = setup(Mode::Revision);
    let validator = Validator::default();
    let result = ready(storage.commit_checked(request(vec![entry("outer", None)]), &validator));
    let after = ready(storage.snapshot()).unwrap();
    ready(storage.close()).unwrap();
    assert_eq!(
        clock.calls.load(Ordering::SeqCst),
        3,
        "outer retry follows the actual nested commit"
    );
    let ConditionalOutcome::Applied(result) = result.unwrap() else {
        panic!("unconditional transaction must publish")
    };
    assert_eq!(result.first_seq, n(1));
    assert_eq!(result.timestamp, n(102));
    assert_eq!(
        placed(&after),
        [("inner".into(), 0, 101), ("outer".into(), 1, 102)]
    );
    assert_eq!(after.next_seq, Some(n(2)));
    assert_eq!(*validator.stages.lock().unwrap(), [after]);
}
#[test]
fn stable_clock_error_preserves_original_context_and_skips_staging_and_validator() {
    let (_backing, storage, clock) = setup(Mode::Stable);
    let before = ready(storage.snapshot()).unwrap();
    let validator = Validator::default();
    // Missing parent would be a mechanical corruption error if staging took precedence.
    let result =
        ready(storage.commit_checked(request(vec![entry("outer", Some("missing"))]), &validator));
    let after = ready(storage.snapshot()).unwrap();
    ready(storage.close()).unwrap();
    let Err(CheckedCommitError::Storage(StorageError::Io(context))) = result else {
        panic!("unchanged owner must preserve clock Io")
    };
    assert_eq!(context.message(), "synthetic clock failure");
    assert!(
        context
            .source_ref()
            .unwrap()
            .downcast_ref::<ClockCause>()
            .is_some()
    );
    assert_eq!(clock.calls.load(Ordering::SeqCst), 1);
    assert!(validator.stages.lock().unwrap().is_empty());
    assert_eq!(before, after);
}
#[test]
fn retry_rechecks_changed_condition_without_another_clock_or_validator_call() {
    let (_backing, storage, clock) = setup(Mode::Condition);
    let validator = Validator::default();
    let mut request = request(vec![entry("outer", None)]);
    request.conditions.push(condition(ExpectedRegister::Absent));
    let result = ready(storage.commit_checked(request, &validator));
    let after = ready(storage.snapshot()).unwrap();
    ready(storage.close()).unwrap();
    assert_eq!(result.unwrap(), ConditionalOutcome::NotApplied);
    assert_eq!(
        clock.calls.load(Ordering::SeqCst),
        2,
        "only failed outer and successful nested clocks run"
    );
    assert!(validator.stages.lock().unwrap().is_empty());
    assert!(after.entries.is_empty());
    assert_eq!(after.next_seq, Some(n(1)));
    assert!(
        matches!(after.registers.as_slice(),[Register::FactCustom{key,seq,value:JsonValue::Null}] if key=="gate" && *seq==n(0))
    );
}
#[test]
fn initial_condition_mismatch_never_calls_failing_clock_or_validator() {
    let (_backing, storage, clock) = setup(Mode::Stable);
    let before = ready(storage.snapshot()).unwrap();
    let validator = Validator::default();
    let mut request = request(vec![entry("outer", None)]);
    request
        .conditions
        .push(condition(ExpectedRegister::Seq(n(0))));
    assert_eq!(
        ready(storage.commit_checked(request, &validator)).unwrap(),
        ConditionalOutcome::NotApplied
    );
    assert_eq!(ready(storage.snapshot()).unwrap(), before);
    ready(storage.close()).unwrap();
    assert_eq!(clock.calls.load(Ordering::SeqCst), 0);
    assert!(validator.stages.lock().unwrap().is_empty());
}
