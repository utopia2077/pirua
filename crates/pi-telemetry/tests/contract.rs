use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use pi_telemetry::{
    AttributeValue, FiniteTelemetryNumber, NOOP_TELEMETRY_CONTEXT, NOOP_TELEMETRY_SPAN,
    NoopTelemetryContext, NoopTelemetrySpan, SpanAttributes, SpanGuard, SpanOptions, SpanStatus,
    TelemetryContext, TelemetryErrorInfo, TelemetrySpan,
};

fn options(name: &str) -> SpanOptions {
    SpanOptions {
        name: name.to_owned(),
        attributes: BTreeMap::new(),
    }
}

fn assert_send_sync<T: Send + Sync>() {}

#[test]
fn telemetry_traits_are_object_safe_and_handles_are_send_sync() {
    assert_send_sync::<Arc<dyn TelemetryContext>>();
    assert_send_sync::<Arc<dyn TelemetrySpan>>();
    assert_send_sync::<SpanGuard>();
    assert_send_sync::<NoopTelemetryContext>();
    assert_send_sync::<NoopTelemetrySpan>();

    let context: Arc<dyn TelemetryContext> = Arc::new(NoopTelemetryContext);
    let span: Arc<dyn TelemetrySpan> = Arc::new(NoopTelemetrySpan);

    span.add_event("event", &SpanAttributes::new());
    span.finish();
    context.start_span(options("root")).finish();
}

#[test]
fn telemetry_numbers_reject_every_non_finite_value() {
    for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        assert!(FiniteTelemetryNumber::new(value).is_err());
        assert!(FiniteTelemetryNumber::try_from(value).is_err());
    }

    for value in [f64::MIN, -0.0, 0.0, f64::MAX] {
        let number = FiniteTelemetryNumber::new(value).expect("finite value must be accepted");
        assert_eq!(number.get(), value);
        assert_eq!(f64::from(number), value);
    }
}

#[test]
fn attribute_values_cover_all_six_supported_shapes() {
    let one = FiniteTelemetryNumber::new(1.0).expect("finite fixture");
    let values = [
        AttributeValue::String("text".to_owned()),
        AttributeValue::Number(one),
        AttributeValue::Boolean(true),
        AttributeValue::Strings(vec!["text".to_owned()]),
        AttributeValue::Numbers(vec![one]),
        AttributeValue::Booleans(vec![true, false]),
    ];

    assert!(matches!(&values[0], AttributeValue::String(value) if value == "text"));
    assert!(matches!(&values[1], AttributeValue::Number(value) if *value == one));
    assert!(matches!(&values[2], AttributeValue::Boolean(true)));
    assert!(matches!(&values[3], AttributeValue::Strings(value) if value == &["text"]));
    assert!(matches!(&values[4], AttributeValue::Numbers(value) if value == &[one]));
    assert!(matches!(&values[5], AttributeValue::Booleans(value) if value == &[true, false]));
}

#[test]
fn noop_context_and_span_accept_the_complete_observer_surface() {
    let mut attributes = SpanAttributes::new();
    attributes.insert("enabled".to_owned(), AttributeValue::Boolean(true));

    let parent = NOOP_TELEMETRY_CONTEXT.start_span(SpanOptions {
        name: "parent".to_owned(),
        attributes: attributes.clone(),
    });
    parent.add_event("event", &attributes);
    parent.set_attributes(&attributes);
    parent.set_status(SpanStatus::Ok);

    let child = parent.start_child(options("child"));
    child.set_status(SpanStatus::Error { error: None });
    child.finish();
    parent.finish();

    NOOP_TELEMETRY_SPAN.add_event("late-event", &attributes);
    NOOP_TELEMETRY_SPAN.set_attributes(&attributes);
    NOOP_TELEMETRY_SPAN.set_status(SpanStatus::Ok);
    NOOP_TELEMETRY_SPAN.finish();
    NOOP_TELEMETRY_SPAN.finish();
}

struct FinishCounterSpan {
    finish_calls: Arc<AtomicUsize>,
}

impl TelemetrySpan for FinishCounterSpan {
    fn start_child(&self, _options: SpanOptions) -> SpanGuard {
        SpanGuard::new(NoopTelemetrySpan)
    }

    fn add_event(&self, _name: &str, _attributes: &SpanAttributes) {}

    fn set_attributes(&self, _attributes: &SpanAttributes) {}

    fn set_status(&self, _status: SpanStatus) {}

    fn finish(&self) {
        self.finish_calls.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn explicit_finish_and_drop_each_settle_their_backend_once() {
    let explicit_calls = Arc::new(AtomicUsize::new(0));
    SpanGuard::new(FinishCounterSpan {
        finish_calls: Arc::clone(&explicit_calls),
    })
    .finish();
    assert_eq!(explicit_calls.load(Ordering::SeqCst), 1);

    let drop_calls = Arc::new(AtomicUsize::new(0));
    {
        let _guard = SpanGuard::new(FinishCounterSpan {
            finish_calls: Arc::clone(&drop_calls),
        });
    }
    assert_eq!(drop_calls.load(Ordering::SeqCst), 1);
}

struct ParentSpan {
    child_starts: Arc<AtomicUsize>,
    child_finishes: Arc<AtomicUsize>,
}

impl TelemetrySpan for ParentSpan {
    fn start_child(&self, _options: SpanOptions) -> SpanGuard {
        self.child_starts.fetch_add(1, Ordering::SeqCst);
        SpanGuard::new(FinishCounterSpan {
            finish_calls: Arc::clone(&self.child_finishes),
        })
    }

    fn add_event(&self, _name: &str, _attributes: &SpanAttributes) {}

    fn set_attributes(&self, _attributes: &SpanAttributes) {}

    fn set_status(&self, _status: SpanStatus) {}

    fn finish(&self) {}
}

#[test]
fn child_spans_are_delegated_and_independently_settled() {
    let child_starts = Arc::new(AtomicUsize::new(0));
    let child_finishes = Arc::new(AtomicUsize::new(0));
    let parent = SpanGuard::new(ParentSpan {
        child_starts: Arc::clone(&child_starts),
        child_finishes: Arc::clone(&child_finishes),
    });

    let child = parent.start_child(options("child"));
    assert_eq!(child_starts.load(Ordering::SeqCst), 1);
    drop(child);
    assert_eq!(child_finishes.load(Ordering::SeqCst), 1);
    parent.finish();
}

#[test]
fn debug_output_redacts_attribute_values_and_error_messages() {
    const SECRET: &str = "telemetry-secret-sentinel";
    const SECRET_NUMBER: f64 = 123_456_789.25;

    let mut attributes = SpanAttributes::new();
    attributes.insert(
        "payload".to_owned(),
        AttributeValue::Strings(vec![SECRET.to_owned()]),
    );
    let options = SpanOptions {
        name: "safe-span-name".to_owned(),
        attributes: attributes.clone(),
    };
    let status = SpanStatus::Error {
        error: Some(TelemetryErrorInfo {
            name: "SafeErrorName".to_owned(),
            message: SECRET.to_owned(),
        }),
    };

    for rendered in [
        format!(
            "{:?}",
            FiniteTelemetryNumber::new(SECRET_NUMBER).expect("finite fixture")
        ),
        format!("{attributes:?}"),
        format!("{options:?}"),
        format!("{status:?}"),
    ] {
        assert!(!rendered.contains(SECRET), "secret leaked through Debug");
        assert!(
            !rendered.contains(&SECRET_NUMBER.to_string()),
            "numeric attribute leaked through Debug"
        );
    }
}
