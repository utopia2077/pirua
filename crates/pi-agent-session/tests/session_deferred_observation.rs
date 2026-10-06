#[allow(dead_code)]
mod prepared_runtime_support;
use pi_agent_schema::session_values::SafeUnsigned;
use pi_agent_schema::*;
use pi_agent_session::{
    backend_support::StorageClock, memory::MemoryBacking, session::RuntimeSession, storage::*,
};
use pi_telemetry::*;
use prepared_runtime_support::*;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

#[derive(Clone, Debug, PartialEq)]
enum Call {
    Start(SpanOptions),
    Attributes(SpanAttributes),
    Status(SpanStatus),
    Finish,
    Drop,
}

#[derive(Default)]
struct Audit {
    calls: Mutex<Vec<Call>>,
    line_held: AtomicBool,
    on_start: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    panic_on_start: AtomicBool,
    panic_on_status: AtomicBool,
}

impl Audit {
    fn calls(&self) -> Vec<Call> {
        self.calls.lock().unwrap().clone()
    }
    fn push(&self, call: Call) {
        assert!(!self.line_held.load(Ordering::SeqCst));
        self.calls.lock().unwrap().push(call);
    }
}

struct Parent(Arc<Audit>);
impl TelemetryContext for Parent {
    fn start_span(&self, options: SpanOptions) -> SpanGuard {
        assert!(!self.0.line_held.load(Ordering::SeqCst));
        self.0.push(Call::Start(options));
        let callback = self.0.on_start.lock().unwrap().take();
        if let Some(callback) = callback {
            callback();
        }
        if self.0.panic_on_start.load(Ordering::SeqCst) {
            panic!("observer panic probe");
        }
        SpanGuard::new(AuditSpan(self.0.clone()))
    }
}
struct AuditSpan(Arc<Audit>);
impl TelemetrySpan for AuditSpan {
    fn start_child(&self, options: SpanOptions) -> SpanGuard {
        Parent(self.0.clone()).start_span(options)
    }
    fn add_event(&self, _: &str, _: &SpanAttributes) {}
    fn set_attributes(&self, attrs: &SpanAttributes) {
        self.0.push(Call::Attributes(attrs.clone()));
    }
    fn set_status(&self, status: SpanStatus) {
        self.0.push(Call::Status(status));
        if self.0.panic_on_status.load(Ordering::SeqCst) {
            panic!("observer status panic probe");
        }
    }
    fn finish(&self) {
        self.0.push(Call::Finish);
    }
}
impl Drop for AuditSpan {
    fn drop(&mut self) {
        self.0.push(Call::Drop);
    }
}

fn observed_session(
    raw: Arc<pi_agent_session::memory::MemoryStorage>,
    default: Arc<Audit>,
) -> RuntimeSession<pi_agent_session::memory::MemoryStorage> {
    ready(RuntimeSession::new(
        raw,
        metadata(),
        codec(),
        Arc::new(Ids),
        Arc::new(Parent(default)),
    ))
    .unwrap()
}

fn prepared(
    session: &RuntimeSession<pi_agent_session::memory::MemoryStorage>,
    request: &ConditionalCommit,
) -> PreparedEntries {
    ready(session.prepare_entries(ready(session.read_stamp()).unwrap(), slots(request)))
        .unwrap()
        .unwrap()
}

fn number(value: f64) -> AttributeValue {
    AttributeValue::Number(FiniteTelemetryNumber::new(value).unwrap())
}

#[test]
fn deferred_applied_span_matches_immediate_parent_and_full_callback_order() {
    let deferred_raw = Arc::new(MemoryBacking::new()).open(Clock::new()).unwrap();
    let immediate_raw = Arc::new(MemoryBacking::new()).open(Clock::new()).unwrap();
    let deferred_default = Arc::new(Audit::default());
    let immediate_default = Arc::new(Audit::default());
    let deferred = observed_session(deferred_raw.clone(), deferred_default.clone());
    let immediate = observed_session(immediate_raw, immediate_default.clone());
    let deferred_log = Arc::new(Audit::default());
    let immediate_log = Arc::new(Audit::default());
    let request = request(vec![
        set("before"),
        Write::Entry(entry("written")),
        Write::DeleteRegister(RegisterDelete {
            namespace: RegisterNamespace::FactCustom,
            key: "absent".into(),
        }),
    ]);
    let token = prepared(&deferred, &request);
    deferred_log.line_held.store(true, Ordering::SeqCst);
    let (outcome, receipt) =
        ready(deferred.commit_prepared_conditional_with_observation(token, request.clone()));
    assert!(matches!(outcome, Ok(ConditionalOutcome::Applied(_))));
    assert!(deferred_log.calls().is_empty());
    let reenter = deferred_raw.clone();
    *deferred_log.on_start.lock().unwrap() = Some(Box::new(move || {
        ready(reenter.read_stamp()).unwrap();
    }));
    deferred_log.line_held.store(false, Ordering::SeqCst);
    receipt.unwrap().emit(&Parent(deferred_log.clone()));

    let immediate_token = prepared(&immediate, &request);
    assert!(matches!(
        ready(immediate.commit_prepared_conditional_with_telemetry(
            immediate_token,
            request,
            &Parent(immediate_log.clone())
        )),
        Ok(ConditionalOutcome::Applied(_))
    ));
    assert_eq!(deferred_log.calls(), immediate_log.calls());
    assert!(deferred_default.calls().is_empty());
    assert!(immediate_default.calls().is_empty());
    assert_eq!(deferred_log.calls().len(), 5);
    let Call::Start(start) = &deferred_log.calls()[0] else {
        panic!("missing start")
    };
    assert_eq!(start.name, "pi.session.write");
    assert_eq!(start.attributes["pi.session.item_count"], number(3.0));
    assert_eq!(
        start.attributes["pi.session.item_kinds"],
        AttributeValue::Strings(vec!["register".into(), "entry".into(), "register".into()])
    );
    let Call::Attributes(seqs) = &deferred_log.calls()[1] else {
        panic!("missing sequences")
    };
    assert_eq!(seqs["first_seq"], number(0.0));
    assert_eq!(seqs["last_seq"], number(2.0));
    assert_eq!(deferred_log.calls()[2], Call::Status(SpanStatus::Ok));
    assert_eq!(deferred_log.calls()[3], Call::Finish);
    assert_eq!(deferred_log.calls()[4], Call::Drop);
}

#[test]
fn refusal_and_rejections_earn_no_receipt_or_telemetry() {
    let raw = Arc::new(MemoryBacking::new()).open(Clock::new()).unwrap();
    let default = Arc::new(Audit::default());
    let session = observed_session(raw.clone(), default.clone());
    let stale_request = request(vec![Write::Entry(entry("stale"))]);
    let stale_stamp = ready(session.read_stamp()).unwrap();
    let token = prepared(&session, &stale_request);
    ready(raw.commit(Transaction {
        writes: vec![set("race")],
    }))
    .unwrap();
    assert!(
        ready(session.prepare_entries(stale_stamp, slots(&stale_request)))
            .unwrap()
            .is_none()
    );
    let (result, receipt) =
        ready(session.commit_prepared_conditional_with_observation(token, stale_request));
    assert_eq!(result.unwrap(), ConditionalOutcome::NotApplied);
    assert!(receipt.is_none());

    let mut codec_reject = request(vec![Write::Entry(entry("valid"))]);
    // Use an invalid role, which the Session codec rejects before storage.
    let message: AgentMessage =
        serde_json::from_value(serde_json::json!({"role":"unregistered","value":1})).unwrap();
    codec_reject
        .transaction
        .writes
        .push(Write::Entry(NewEntry::Message(NewMessageEntry {
            id: EntryId::new("bad-message").unwrap(),
            parent_id: None,
            entry_type: MessageEntryType::Message,
            message,
            terminate: None,
        })));
    let token = prepared(&session, &codec_reject);
    let (result, receipt) =
        ready(session.commit_prepared_conditional_with_observation(token, codec_reject));
    assert!(matches!(result, Err(CheckedCommitError::Rejected(_))));
    assert!(receipt.is_none());

    let rejection = request(vec![
        set("valid"),
        Write::SetRegister(Box::new(RegisterSet::FactLabel {
            key: "missing".into(),
            value: "label".into(),
        })),
    ]);
    let token = prepared(&session, &rejection);
    let (result, receipt) =
        ready(session.commit_prepared_conditional_with_observation(token, rejection));
    assert!(matches!(result, Err(CheckedCommitError::Rejected(_))));
    assert!(receipt.is_none());
    assert!(default.calls().is_empty());
}

#[test]
fn checked_storage_error_receipt_matches_immediate_error_span() {
    let raw = Arc::new(MemoryBacking::new()).open(Clock::new()).unwrap();
    let session = observed_session(raw, Arc::new(Audit::default()));
    let request = request(vec![
        Write::Entry(entry("first")),
        Write::Entry(entry("second")),
    ]);
    let token = prepared(&session, &request);
    let mut mismatched = request.clone();
    mismatched.transaction.writes.swap(0, 1);
    let deferred_log = Arc::new(Audit::default());
    let (result, receipt) =
        ready(session.commit_prepared_conditional_with_observation(token, mismatched.clone()));
    assert!(matches!(result, Err(CheckedCommitError::Storage(_))));
    assert!(deferred_log.calls().is_empty());
    receipt.unwrap().emit(&Parent(deferred_log.clone()));

    let token = prepared(&session, &request);
    let immediate_log = Arc::new(Audit::default());
    assert!(matches!(
        ready(session.commit_prepared_conditional_with_telemetry(
            token,
            mismatched,
            &Parent(immediate_log.clone())
        )),
        Err(CheckedCommitError::Storage(_))
    ));
    assert_eq!(deferred_log.calls(), immediate_log.calls());
    assert_eq!(deferred_log.calls().len(), 4);
    assert_eq!(
        deferred_log.calls()[1],
        Call::Status(SpanStatus::Error { error: None })
    );
}

struct FailingClock(AtomicBool);
impl StorageClock for FailingClock {
    fn now(&self) -> Result<SafeUnsigned, StorageError> {
        if self.0.load(Ordering::SeqCst) {
            Err(StorageError::Io(ErrorContext::new("clock failed").unwrap()))
        } else {
            Ok(n(1234))
        }
    }
}

#[test]
fn preparation_and_stamp_errors_use_one_codec_checked_error_receipt() {
    let clock = Arc::new(FailingClock(AtomicBool::new(false)));
    let raw = Arc::new(MemoryBacking::new()).open(clock.clone()).unwrap();
    let session = observed_session(raw.clone(), Arc::new(Audit::default()));
    let request = request(vec![set("before"), Write::Entry(entry("planned"))]);
    let stamp = ready(session.read_stamp()).unwrap();
    clock.0.store(true, Ordering::SeqCst);
    assert!(ready(session.prepare_entries(stamp, slots(&request))).is_err());
    let deferred_log = Arc::new(Audit::default());
    let receipt = session
        .write_observation_for_preparation_error(&request.transaction)
        .unwrap();
    assert!(deferred_log.calls().is_empty());
    receipt.emit(&Parent(deferred_log.clone()));
    let immediate_log = Arc::new(Audit::default());
    assert!(matches!(
        ready(
            session
                .commit_conditional_with_telemetry(request.clone(), &Parent(immediate_log.clone()))
        ),
        Err(CheckedCommitError::Storage(_))
    ));
    assert_eq!(deferred_log.calls(), immediate_log.calls());

    ready(raw.close()).unwrap();
    assert!(ready(session.read_stamp()).is_err());
    let stamp_error_log = Arc::new(Audit::default());
    session
        .write_observation_for_preparation_error(&request.transaction)
        .unwrap()
        .emit(&Parent(stamp_error_log.clone()));
    assert_eq!(stamp_error_log.calls(), immediate_log.calls());

    let invalid: Transaction = Transaction {
        writes: vec![Write::Entry(NewEntry::Message(NewMessageEntry {
            id: EntryId::new("bad").unwrap(),
            parent_id: None,
            entry_type: MessageEntryType::Message,
            message: serde_json::from_value(serde_json::json!({"role":"unregistered","value":1}))
                .unwrap(),
            terminate: None,
        }))],
    };
    assert!(
        session
            .write_observation_for_preparation_error(&invalid)
            .is_err()
    );
}

#[test]
fn dropped_or_panicking_observer_cannot_replay_receipt() {
    let raw = Arc::new(MemoryBacking::new()).open(Clock::new()).unwrap();
    let session = observed_session(raw.clone(), Arc::new(Audit::default()));
    let request = request(vec![set("first")]);
    let token = prepared(&session, &request);
    let (outcome, dropped) =
        ready(session.commit_prepared_conditional_with_observation(token, request.clone()));
    assert!(matches!(outcome, Ok(ConditionalOutcome::Applied(_))));
    drop(dropped);
    let token = prepared(&session, &request);
    let (outcome, receipt) =
        ready(session.commit_prepared_conditional_with_observation(token, request.clone()));
    assert!(matches!(outcome, Ok(ConditionalOutcome::Applied(_))));
    let committed_snapshot =
        serde_json::to_vec(&ready(raw.snapshot()).unwrap()).expect("snapshot must serialize");
    let log = Arc::new(Audit::default());
    log.panic_on_start.store(true, Ordering::SeqCst);
    let receipt = receipt.expect("applied write must earn one receipt");
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        receipt.emit(&Parent(log.clone()));
    }));
    assert!(result.is_err());
    assert_eq!(log.calls().len(), 1);
    let after_panic_snapshot =
        serde_json::to_vec(&ready(raw.snapshot()).unwrap()).expect("snapshot must serialize");
    assert_eq!(
        after_panic_snapshot, committed_snapshot,
        "observer panic must not mutate the committed durable authority"
    );
    // `WriteObservation::emit` consumes the receipt. The single callback and
    // unchanged authority together prove there was no second emission/replay.
    assert_eq!(
        log.calls(),
        vec![Call::Start(SpanOptions {
            name: "pi.session.write".to_owned(),
            attributes: [
                ("pi.session.item_count".to_owned(), number(1.0)),
                (
                    "pi.session.item_kinds".to_owned(),
                    AttributeValue::Strings(vec!["register".to_owned()]),
                ),
            ]
            .into(),
        })]
    );

    let token = prepared(&session, &request);
    let (outcome, receipt) =
        ready(session.commit_prepared_conditional_with_observation(token, request));
    assert!(matches!(outcome, Ok(ConditionalOutcome::Applied(_))));
    let status_log = Arc::new(Audit::default());
    status_log.panic_on_status.store(true, Ordering::SeqCst);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        receipt.unwrap().emit(&Parent(status_log.clone()));
    }));
    assert!(result.is_err());
    let calls = status_log.calls();
    assert_eq!(calls.len(), 5);
    assert!(matches!(calls[0], Call::Start(_)));
    assert!(matches!(calls[1], Call::Attributes(_)));
    assert_eq!(calls[2], Call::Status(SpanStatus::Ok));
    assert_eq!(calls[3], Call::Finish);
    assert_eq!(calls[4], Call::Drop);
}
