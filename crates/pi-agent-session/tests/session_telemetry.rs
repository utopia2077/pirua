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
struct Spy(Arc<Log>);
impl TelemetryContext for Spy {
    fn start_span(&self, o: SpanOptions) -> SpanGuard {
        self.0.starts.lock().unwrap().push(o);
        let callback = self.0.callback.lock().unwrap().take();
        if let Some(callback) = callback {
            callback();
        }
        SpanGuard::new(Span(self.0.clone()))
    }
}
struct Span(Arc<Log>);
impl TelemetrySpan for Span {
    fn start_child(&self, o: SpanOptions) -> SpanGuard {
        Spy(self.0.clone()).start_span(o)
    }
    fn add_event(&self, _: &str, _: &SpanAttributes) {}
    fn set_attributes(&self, a: &SpanAttributes) {
        self.0.attrs.lock().unwrap().push(a.clone());
    }
    fn set_status(&self, s: SpanStatus) {
        self.0.statuses.lock().unwrap().push(s);
    }
    fn finish(&self) {
        *self.0.finished.lock().unwrap() += 1;
    }
}
#[test]
fn outcome_linked_spans_zero_on_mismatch_rejection_and_empty_seq() {
    let (_, raw) = memory();
    seed(raw.as_ref(), "main", None);
    let log = Arc::new(Log::default());
    let s = ready(RuntimeSession::new(
        raw.clone(),
        metadata(),
        codec(),
        Arc::new(Ids::default()),
        Arc::new(Spy(log.clone())),
    ))
    .unwrap();
    let request = ConditionalCommit {
        conditions: vec![RegisterCondition {
            register: RegisterKey {
                namespace: RegisterNamespace::LaneLeaf,
                key: "main".into(),
            },
            expected: ExpectedRegister::Absent,
        }],
        transaction: tx(vec![custom_entry("no", None)]),
    };
    assert_eq!(
        ready(s.commit_conditional(request)).unwrap(),
        ConditionalOutcome::NotApplied
    );
    assert!(log.starts.lock().unwrap().is_empty());
    assert!(ready(s.append_message(custom("unknown", "payload-secret"))).is_err());
    assert!(log.starts.lock().unwrap().is_empty());
    assert!(ready(s.set_label(eid("absent"), Some("secret".into()))).is_err());
    assert!(log.starts.lock().unwrap().is_empty());
    let r = raw.clone();
    *log.callback.lock().unwrap() = Some(Box::new(move || {
        let snapshot = ready(r.snapshot()).unwrap();
        assert_eq!(snapshot.entries.len(), 1); // observer sees complete publication; no lock is retained
    }));
    ready(s.append_message(user("payload-secret"))).unwrap();
    ready(s.commit(tx(vec![]))).unwrap();
    assert_eq!(log.starts.lock().unwrap().len(), 2);
    assert_eq!(*log.finished.lock().unwrap(), 2);
    assert_eq!(log.attrs.lock().unwrap().len(), 1);
    let starts = log.starts.lock().unwrap();
    assert_eq!(starts[0].name, "pi.session.write");
    assert_eq!(
        starts[0].attributes["pi.session.item_kinds"],
        AttributeValue::Strings(vec!["entry".into(), "register".into()])
    );
    assert_eq!(
        starts[1].attributes["pi.session.item_count"],
        AttributeValue::Number(FiniteTelemetryNumber::new(0.0).unwrap())
    );
    drop(starts);
    let attrs = log.attrs.lock().unwrap();
    assert_eq!(
        attrs[0]["first_seq"],
        AttributeValue::Number(FiniteTelemetryNumber::new(2.0).unwrap())
    );
    assert_eq!(
        attrs[0]["last_seq"],
        AttributeValue::Number(FiniteTelemetryNumber::new(3.0).unwrap())
    );
    drop(attrs);
    assert!(
        ready(s.commit(tx(vec![
            custom_entry("duplicate", None),
            custom_entry("duplicate", None)
        ])))
        .is_err()
    );
    assert_eq!(log.starts.lock().unwrap().len(), 3);
    assert!(matches!(
        log.statuses.lock().unwrap()[2],
        SpanStatus::Error { .. }
    ));
}
#[test]
fn close_during_checked_callback_does_not_publish() {
    let (backing, raw) = memory();
    seed(raw.as_ref(), "main", None);
    let probe = Arc::new(Probe::new(raw.clone()));
    let s = session(probe.clone(), codec());
    let r = raw.clone();
    *probe.before.lock().unwrap() = Some(Box::new(move || {
        ready(r.close()).unwrap();
    }));
    assert_eq!(
        ready(s.append_message(user("closed")))
            .unwrap_err()
            .category(),
        "closed"
    );
    let reopened = backing.open(Arc::new(Clock)).unwrap();
    assert!(ready(reopened.snapshot()).unwrap().entries.is_empty());
    ready(s.close()).unwrap();
    ready(reopened.stats()).unwrap();
}
