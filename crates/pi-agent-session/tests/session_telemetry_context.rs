mod session_support;
use pi_telemetry::*;
use session_support::*;
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct Log {
    starts: Mutex<Vec<SpanOptions>>,
    attrs: Mutex<Vec<SpanAttributes>>,
    statuses: Mutex<Vec<SpanStatus>>,
    finished: Mutex<usize>,
    callback: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}
struct Parent(Arc<Log>);
impl TelemetryContext for Parent {
    fn start_span(&self, options: SpanOptions) -> SpanGuard {
        self.0.starts.lock().unwrap().push(options);
        let callback = self.0.callback.lock().unwrap().take();
        if let Some(callback) = callback {
            callback();
        }
        SpanGuard::new(Observed(self.0.clone()))
    }
}
struct Observed(Arc<Log>);
impl TelemetrySpan for Observed {
    fn start_child(&self, options: SpanOptions) -> SpanGuard {
        Parent(self.0.clone()).start_span(options)
    }
    fn add_event(&self, _: &str, _: &SpanAttributes) {}
    fn set_attributes(&self, attrs: &SpanAttributes) {
        self.0.attrs.lock().unwrap().push(attrs.clone());
    }
    fn set_status(&self, status: SpanStatus) {
        self.0.statuses.lock().unwrap().push(status);
    }
    fn finish(&self) {
        *self.0.finished.lock().unwrap() += 1;
    }
}
fn request(writes: Vec<Write>) -> ConditionalCommit {
    ConditionalCommit {
        conditions: vec![],
        transaction: tx(writes),
    }
}
fn number(value: f64) -> AttributeValue {
    AttributeValue::Number(FiniteTelemetryNumber::new(value).unwrap())
}
fn assert_span(log: &Log, index: usize, kinds: &[&str], seqs: Option<(u64, u64)>) {
    let starts = log.starts.lock().unwrap();
    assert_eq!(starts[index].name, "pi.session.write");
    assert_eq!(starts[index].attributes.len(), 2);
    assert_eq!(
        starts[index].attributes["pi.session.item_count"],
        number(kinds.len() as f64)
    );
    assert_eq!(
        starts[index].attributes["pi.session.item_kinds"],
        AttributeValue::Strings(kinds.iter().map(|s| (*s).into()).collect())
    );
    if let Some((first, last)) = seqs {
        let attrs = log.attrs.lock().unwrap();
        assert_eq!(attrs[index].len(), 2);
        assert_eq!(attrs[index]["first_seq"], number(first as f64));
        assert_eq!(attrs[index]["last_seq"], number(last as f64));
    }
}
fn assert_count(log: &Log, count: usize) {
    assert_eq!(log.starts.lock().unwrap().len(), count);
    assert_eq!(log.statuses.lock().unwrap().len(), count);
    assert_eq!(*log.finished.lock().unwrap(), count);
}

#[test]
fn borrowed_parents_remain_isolated_across_interleaved_checked_commits() {
    let (_, raw) = memory();
    seed(raw.as_ref(), "main", None);
    // Reuse the established checked forwarding fixture without changing it.
    let probe = Arc::new(Probe::new(raw.clone()));
    let default = Arc::new(Log::default());
    let first = Arc::new(Log::default());
    let second = Arc::new(Log::default());
    let session = ready(RuntimeSession::new(
        probe.clone(),
        metadata(),
        codec(),
        Arc::new(Ids::default()),
        Arc::new(Parent(default.clone())),
    ))
    .unwrap();
    let nested = session.clone();
    let second_log = second.clone();
    *probe.before.lock().unwrap() = Some(Box::new(move || {
        // The outer borrowed parent is live while another parent publishes.
        let parent = Parent(second_log);
        ready(nested.commit_conditional_with_telemetry(
            request(vec![custom_entry("second", None)]),
            &parent,
        ))
        .unwrap();
        ready(nested.commit_conditional(request(vec![custom_entry("default-during", None)])))
            .unwrap();
    }));
    let snapshot_raw = raw.clone();
    *first.callback.lock().unwrap() = Some(Box::new(move || {
        let snapshot = ready(snapshot_raw.snapshot()).unwrap();
        assert_eq!(snapshot.entries.len(), 3);
        assert!(
            snapshot
                .entries
                .iter()
                .any(|e| matches!(e, Entry::Custom(e) if e.id == eid("first")))
        );
    }));
    let parent = Parent(first.clone()); // borrowed stack context; no Arc or 'static required
    let outcome = ready(session.commit_conditional_with_telemetry(
        request(vec![
            custom_entry("first", None),
            del(RegisterNamespace::FactLabel, "missing"),
        ]),
        &parent,
    ))
    .unwrap();
    assert!(matches!(outcome, ConditionalOutcome::Applied(_)));
    ready(session.commit_conditional(request(vec![custom_entry("default-after", None)]))).unwrap();
    // A lane view's ordinary Session method still uses the constructor context.
    ready(session.lane_view(lane("main")).commit(tx(vec![]))).unwrap();
    assert_count(&first, 1);
    assert_count(&second, 1);
    assert_count(&default, 3);
    assert_span(&second, 0, &["entry"], Some((2, 2)));
    assert_span(&first, 0, &["entry", "register"], Some((4, 5)));
    assert_span(&default, 0, &["entry"], Some((3, 3)));
    assert_span(&default, 1, &["entry"], Some((6, 6)));
    assert_span(&default, 2, &[], None);
    assert_eq!(default.attrs.lock().unwrap().len(), 2);
    for log in [&first, &second, &default] {
        assert!(
            log.statuses
                .lock()
                .unwrap()
                .iter()
                .all(|s| matches!(s, SpanStatus::Ok))
        );
    }
    assert_eq!(probe.attempts.load(std::sync::atomic::Ordering::SeqCst), 5);
}

#[test]
fn explicit_context_emits_nothing_for_stale_cas_codec_or_publication_rejection() {
    let (_, raw) = memory();
    seed(raw.as_ref(), "main", None);
    let probe = Arc::new(Probe::new(raw.clone()));
    let default = Arc::new(Log::default());
    let explicit = Arc::new(Log::default());
    let session = ready(RuntimeSession::new(
        probe.clone(),
        metadata(),
        codec(),
        Arc::new(Ids::default()),
        Arc::new(Parent(default.clone())),
    ))
    .unwrap();
    let parent = Parent(explicit.clone());
    let before = bytes(raw.as_ref());
    let mut stale = request(vec![custom_entry("stale", None)]);
    stale.conditions.push(RegisterCondition {
        register: RegisterKey {
            namespace: RegisterNamespace::LaneLeaf,
            key: "main".into(),
        },
        expected: ExpectedRegister::Absent,
    });
    assert_eq!(
        ready(session.commit_conditional_with_telemetry(stale, &parent)).unwrap(),
        ConditionalOutcome::NotApplied
    );
    let unknown = Write::Entry(NewEntry::Message(serde_json::from_value(json!({"id":"unknown", "parentId":null, "type":"message", "message":{"role":"unknown-role", "text":"private-payload"}})).unwrap()));
    assert!(matches!(
        ready(session.commit_conditional_with_telemetry(request(vec![unknown]), &parent)),
        Err(CheckedCommitError::Rejected(_))
    ));
    // Actual staged publication validation: a label cannot target a missing entry.
    let dangling = set(RegisterSet::FactLabel {
        key: "absent".into(),
        value: "private-label".into(),
    });
    assert!(matches!(
        ready(session.commit_conditional_with_telemetry(request(vec![dangling]), &parent)),
        Err(CheckedCommitError::Rejected(_))
    ));
    assert_eq!(bytes(raw.as_ref()), before);
    assert_eq!(probe.attempts.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert_count(&explicit, 0);
    assert_count(&default, 0);
}

#[test]
fn explicit_context_observes_actual_mechanism_error_once_without_sequences() {
    let (_, raw) = memory();
    seed(raw.as_ref(), "main", None);
    let default = Arc::new(Log::default());
    let explicit = Arc::new(Log::default());
    let session = ready(RuntimeSession::new(
        raw.clone(),
        metadata(),
        codec(),
        Arc::new(Ids::default()),
        Arc::new(Parent(default.clone())),
    ))
    .unwrap();
    let before = bytes(raw.as_ref());
    let result = ready(session.commit_conditional_with_telemetry(
        request(vec![
            custom_entry("duplicate", None),
            custom_entry("duplicate", None),
        ]),
        &Parent(explicit.clone()),
    ));
    assert!(matches!(result, Err(CheckedCommitError::Storage(_))));
    assert_eq!(bytes(raw.as_ref()), before);
    assert_count(&explicit, 1);
    assert_count(&default, 0);
    assert_span(&explicit, 0, &["entry", "entry"], None);
    assert!(explicit.attrs.lock().unwrap().is_empty());
    assert!(matches!(
        explicit.statuses.lock().unwrap()[0],
        SpanStatus::Error { error: None }
    ));
}
