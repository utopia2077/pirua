use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use pi_telemetry::{
    AttributeValue, FiniteTelemetryNumber, InMemoryTelemetryContext, NOOP_TELEMETRY_CONTEXT,
    NOOP_TELEMETRY_SPAN, NoopTelemetryContext, NoopTelemetrySpan, SpanAttributes, SpanGuard,
    SpanOptions, SpanStatus, TelemetryAttributeDefinition, TelemetryCardinality, TelemetryContext,
    TelemetryErrorInfo, TelemetryEventDefinition, TelemetryParentDefinition,
    TelemetrySchemaDefinition, TelemetrySpan, TelemetrySpanDefinition,
    TelemetryStartAttributeDefinition, TelemetryStatusDefinition,
};

fn assert_well_formed_json(input: &str) {
    struct Parser<'a> {
        bytes: &'a [u8],
        position: usize,
    }
    impl Parser<'_> {
        fn whitespace(&mut self) {
            while self
                .bytes
                .get(self.position)
                .is_some_and(|byte| matches!(byte, b' ' | b'\n' | b'\r' | b'\t'))
            {
                self.position += 1;
            }
        }
        fn consume(&mut self, expected: u8) {
            assert_eq!(self.bytes.get(self.position), Some(&expected));
            self.position += 1;
        }
        fn value(&mut self) {
            self.whitespace();
            match self.bytes.get(self.position).copied() {
                Some(b'{') => self.object(),
                Some(b'[') => self.array(),
                Some(b'"') => self.string(),
                Some(b't') => self.literal(b"true"),
                Some(b'f') => self.literal(b"false"),
                Some(b'n') => self.literal(b"null"),
                Some(b'-' | b'0'..=b'9') => self.number(),
                other => panic!("invalid JSON value start: {other:?}"),
            }
        }
        fn object(&mut self) {
            self.consume(b'{');
            self.whitespace();
            if self.bytes.get(self.position) == Some(&b'}') {
                self.position += 1;
                return;
            }
            loop {
                self.whitespace();
                self.string();
                self.whitespace();
                self.consume(b':');
                self.value();
                self.whitespace();
                match self.bytes.get(self.position).copied() {
                    Some(b',') => self.position += 1,
                    Some(b'}') => {
                        self.position += 1;
                        return;
                    }
                    other => panic!("invalid JSON object separator: {other:?}"),
                }
            }
        }
        fn array(&mut self) {
            self.consume(b'[');
            self.whitespace();
            if self.bytes.get(self.position) == Some(&b']') {
                self.position += 1;
                return;
            }
            loop {
                self.value();
                self.whitespace();
                match self.bytes.get(self.position).copied() {
                    Some(b',') => self.position += 1,
                    Some(b']') => {
                        self.position += 1;
                        return;
                    }
                    other => panic!("invalid JSON array separator: {other:?}"),
                }
            }
        }
        fn string(&mut self) {
            self.consume(b'"');
            loop {
                match self.bytes.get(self.position).copied() {
                    Some(b'"') => {
                        self.position += 1;
                        return;
                    }
                    Some(b'\\') => {
                        self.position += 1;
                        match self.bytes.get(self.position).copied() {
                            Some(b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't') => {
                                self.position += 1
                            }
                            Some(b'u') => {
                                self.position += 1;
                                for _ in 0..4 {
                                    assert!(
                                        self.bytes
                                            .get(self.position)
                                            .is_some_and(u8::is_ascii_hexdigit)
                                    );
                                    self.position += 1;
                                }
                            }
                            other => panic!("invalid JSON string escape: {other:?}"),
                        }
                    }
                    Some(byte) if byte >= 0x20 => self.position += 1,
                    other => panic!("invalid JSON string byte: {other:?}"),
                }
            }
        }
        fn literal(&mut self, literal: &[u8]) {
            assert_eq!(
                self.bytes.get(self.position..self.position + literal.len()),
                Some(literal)
            );
            self.position += literal.len();
        }
        fn number(&mut self) {
            let start = self.position;
            if self.bytes.get(self.position) == Some(&b'-') {
                self.position += 1;
            }
            while self
                .bytes
                .get(self.position)
                .is_some_and(u8::is_ascii_digit)
            {
                self.position += 1;
            }
            if self.bytes.get(self.position) == Some(&b'.') {
                self.position += 1;
                while self
                    .bytes
                    .get(self.position)
                    .is_some_and(u8::is_ascii_digit)
                {
                    self.position += 1;
                }
            }
            if self
                .bytes
                .get(self.position)
                .is_some_and(|byte| *byte == b'e' || *byte == b'E')
            {
                self.position += 1;
                if self
                    .bytes
                    .get(self.position)
                    .is_some_and(|byte| *byte == b'+' || *byte == b'-')
                {
                    self.position += 1;
                }
                while self
                    .bytes
                    .get(self.position)
                    .is_some_and(u8::is_ascii_digit)
                {
                    self.position += 1;
                }
            }
            assert!(self.position > start, "JSON number must contain digits");
        }
    }

    let mut parser = Parser {
        bytes: input.as_bytes(),
        position: 0,
    };
    parser.value();
    parser.whitespace();
    assert_eq!(parser.position, parser.bytes.len(), "trailing JSON input");
}

fn options(name: &str) -> SpanOptions {
    SpanOptions {
        links: Vec::new(),
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
        AttributeValue::String("quoted\ntext".to_owned()),
        AttributeValue::Number(one),
        AttributeValue::Boolean(true),
        AttributeValue::Strings(vec!["text".to_owned(), "quoted\"".to_owned()]),
        AttributeValue::Numbers(vec![one]),
        AttributeValue::Booleans(vec![true, false]),
    ];

    assert_eq!(values[0].to_json(), "\"quoted\\ntext\"");
    assert_eq!(values[1].to_json(), "1");
    assert_eq!(values[2].to_json(), "true");
    assert_eq!(values[3].to_json(), "[\"text\",\"quoted\\\"\"]");
    assert_eq!(values[4].to_json(), "[1]");
    assert_eq!(values[5].to_json(), "[true,false]");

    assert!(matches!(&values[0], AttributeValue::String(value) if value == "quoted\ntext"));
    assert!(matches!(&values[1], AttributeValue::Number(value) if *value == one));
    assert!(matches!(&values[2], AttributeValue::Boolean(true)));
    assert!(matches!(&values[3], AttributeValue::Strings(value) if value == &["text", "quoted\""]));
    assert!(matches!(&values[4], AttributeValue::Numbers(value) if value == &[one]));
    assert!(matches!(&values[5], AttributeValue::Booleans(value) if value == &[true, false]));
}

#[test]
fn noop_context_and_span_accept_the_complete_observer_surface() {
    let mut attributes = SpanAttributes::new();
    attributes.insert("enabled".to_owned(), AttributeValue::Boolean(true));

    let parent = NOOP_TELEMETRY_CONTEXT.start_span(SpanOptions {
        links: Vec::new(),
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
        links: Vec::new(),
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

#[test]
fn in_memory_recorder_tracks_parentage_events_attributes_status_and_end_order() {
    let context = InMemoryTelemetryContext::new();
    let mut root_attributes = SpanAttributes::new();
    root_attributes.insert("kind".to_owned(), AttributeValue::String("root".to_owned()));
    let root = context.start_span(SpanOptions {
        links: Vec::new(),
        name: "root".to_owned(),
        attributes: root_attributes,
    });
    let root_snapshot = context.get_spans();
    assert_eq!(root_snapshot.len(), 1);
    assert_eq!(root_snapshot[0].id, 1);
    assert_eq!(root_snapshot[0].parent_id, None);
    assert!(!root_snapshot[0].settled);
    assert_eq!(root_snapshot[0].end_sequence, None);

    let first_child = root.start_child(options("first-child"));
    first_child.add_event("first", &SpanAttributes::new());
    let second_child = root.start_child(options("second-child"));
    second_child.add_event("second", &SpanAttributes::new());
    second_child.set_status(SpanStatus::Error { error: None });
    second_child.finish();
    first_child.finish();
    root.finish();

    let spans = context.get_spans();
    assert_eq!(spans.len(), 3);
    assert_eq!(spans[0].name, "root");
    assert_eq!(spans[1].name, "first-child");
    assert_eq!(spans[2].name, "second-child");
    assert_eq!(spans[1].parent_id, Some(spans[0].id));
    assert_eq!(spans[2].parent_id, Some(spans[0].id));
    assert_eq!(spans[1].events[0].name, "first");
    assert_eq!(spans[2].events[0].name, "second");
    assert_eq!(spans[2].status, SpanStatus::Error { error: None });
    assert_eq!(spans[2].end_sequence, Some(1));
    assert_eq!(spans[1].end_sequence, Some(2));
    assert_eq!(spans[0].end_sequence, Some(3));
}

#[test]
fn in_memory_recorder_clones_inputs_and_snapshots_are_detached() {
    let context = InMemoryTelemetryContext::new();
    let mut start_attributes = SpanAttributes::new();
    start_attributes.insert(
        "tags".to_owned(),
        AttributeValue::Strings(vec!["initial".to_owned()]),
    );
    let root = context.start_span(SpanOptions {
        links: Vec::new(),
        name: "snapshot".to_owned(),
        attributes: start_attributes,
    });

    let mut event_attributes = SpanAttributes::new();
    event_attributes.insert(
        "value".to_owned(),
        AttributeValue::Numbers(vec![
            FiniteTelemetryNumber::new(1.0).expect("finite fixture"),
        ]),
    );
    root.add_event("event", &event_attributes);
    let mut merged_attributes = SpanAttributes::new();
    merged_attributes.insert(
        "tags".to_owned(),
        AttributeValue::Strings(vec!["merged".to_owned()]),
    );
    merged_attributes.insert(
        "count".to_owned(),
        AttributeValue::Number(FiniteTelemetryNumber::new(2.0).expect("finite fixture")),
    );
    root.set_attributes(&merged_attributes);

    event_attributes.insert(
        "value".to_owned(),
        AttributeValue::Numbers(vec![
            FiniteTelemetryNumber::new(99.0).expect("finite fixture"),
        ]),
    );
    merged_attributes.insert(
        "count".to_owned(),
        AttributeValue::Number(FiniteTelemetryNumber::new(99.0).expect("finite fixture")),
    );

    let open = context.get_spans();
    assert!(!open[0].settled);
    assert_eq!(open[0].end_sequence, None);
    assert_eq!(
        open[0].events[0].attributes["value"],
        AttributeValue::Numbers(vec![
            FiniteTelemetryNumber::new(1.0).expect("finite fixture")
        ])
    );
    assert_eq!(
        open[0].attributes["count"],
        AttributeValue::Number(FiniteTelemetryNumber::new(2.0).expect("finite fixture"))
    );

    root.finish();
    let first = context.get_spans();
    assert!(first[0].settled);
    assert_eq!(first[0].end_sequence, Some(1));
    let mut detached = first[0].clone();
    detached
        .attributes
        .insert("detached".to_owned(), AttributeValue::Boolean(true));
    detached.events[0]
        .attributes
        .insert("detached".to_owned(), AttributeValue::Boolean(true));
    let second = context.get_spans();
    assert!(!second[0].attributes.contains_key("detached"));
    assert!(!second[0].events[0].attributes.contains_key("detached"));
}

#[test]
fn telemetry_schema_is_detached_serializable_and_rejects_duplicate_span_names() {
    let one = FiniteTelemetryNumber::new(1.0).expect("finite fixture");
    let two = FiniteTelemetryNumber::new(2.0).expect("finite fixture");
    let three = FiniteTelemetryNumber::new(3.0).expect("finite fixture");
    let four = FiniteTelemetryNumber::new(4.0).expect("finite fixture");
    let mut schema = TelemetrySchemaDefinition::new(7);
    let mut span = TelemetrySpanDefinition::new(
        "operation \"description\"\n",
        TelemetryParentDefinition::Spans(vec!["parent".to_owned(), "external".to_owned()]),
        TelemetryStatusDefinition::new("operation fails\\when"),
    );
    span.start_attributes.insert(
        "a-string".to_owned(),
        TelemetryStartAttributeDefinition::required(TelemetryAttributeDefinition::string(
            "line\n\"slash\\",
        )),
    );
    span.start_attributes.insert(
        "b-number".to_owned(),
        TelemetryStartAttributeDefinition::optional(
            TelemetryAttributeDefinition::number("numeric")
                .sensitive(true)
                .cardinality(TelemetryCardinality::High)
                .with_values(vec![
                    AttributeValue::Number(one),
                    AttributeValue::Number(FiniteTelemetryNumber::new(-0.0).unwrap()),
                ])
                .with_examples(vec![AttributeValue::Number(two)]),
        ),
    );
    span.start_attributes.insert(
        "c-boolean".to_owned(),
        TelemetryStartAttributeDefinition::required(TelemetryAttributeDefinition::boolean("flag")),
    );
    span.start_attributes.insert(
        "d-string-list".to_owned(),
        TelemetryStartAttributeDefinition::optional(
            TelemetryAttributeDefinition::strings("labels")
                .with_element_values(vec![
                    AttributeValue::String("one".to_owned()),
                    AttributeValue::String("two".to_owned()),
                ])
                .with_examples(vec![AttributeValue::Strings(vec![
                    "example".to_owned(),
                    "quoted\"".to_owned(),
                ])]),
        ),
    );
    span.start_attributes.insert(
        "e-number-list".to_owned(),
        TelemetryStartAttributeDefinition::required(
            TelemetryAttributeDefinition::numbers("measurements")
                .with_element_values(vec![AttributeValue::Number(three)])
                .with_examples(vec![AttributeValue::Numbers(vec![
                    four,
                    FiniteTelemetryNumber::new(-0.0).unwrap(),
                ])]),
        ),
    );
    span.start_attributes.insert(
        "f-boolean-list".to_owned(),
        TelemetryStartAttributeDefinition::optional(
            TelemetryAttributeDefinition::booleans("switches")
                .with_element_values(vec![
                    AttributeValue::Boolean(true),
                    AttributeValue::Boolean(false),
                ])
                .with_examples(vec![AttributeValue::Booleans(vec![true, false])]),
        ),
    );
    span.end_attributes.insert(
        "ended".to_owned(),
        TelemetryAttributeDefinition::string("end marker"),
    );
    span.events.insert(
        "finished".to_owned(),
        TelemetryEventDefinition::new("operation finished"),
    );
    span.events.get_mut("finished").unwrap().attributes.insert(
        "event-flag".to_owned(),
        TelemetryStartAttributeDefinition::optional(TelemetryAttributeDefinition::boolean(
            "event flag",
        )),
    );
    schema.spans.insert("operation".to_owned(), span);

    let json = schema.to_json();
    assert_well_formed_json(&json);
    assert_eq!(
        json,
        "{\"version\":7,\"spans\":{\"operation\":{\"description\":\"operation \\\"description\\\"\\n\",\"parents\":{\"kind\":\"spans\",\"spans\":[\"parent\",\"external\"]},\"startAttributes\":{\"a-string\":{\"type\":\"string\",\"description\":\"line\\n\\\"slash\\\\\",\"required\":true},\"b-number\":{\"type\":\"number\",\"description\":\"numeric\",\"sensitive\":true,\"cardinality\":\"high\",\"values\":[1,-0],\"examples\":[2],\"required\":false},\"c-boolean\":{\"type\":\"boolean\",\"description\":\"flag\",\"required\":true},\"d-string-list\":{\"type\":\"string[]\",\"description\":\"labels\",\"elementValues\":[\"one\",\"two\"],\"examples\":[[\"example\",\"quoted\\\"\"]],\"required\":false},\"e-number-list\":{\"type\":\"number[]\",\"description\":\"measurements\",\"elementValues\":[3],\"examples\":[[4,-0]],\"required\":true},\"f-boolean-list\":{\"type\":\"boolean[]\",\"description\":\"switches\",\"elementValues\":[true,false],\"examples\":[[true,false]],\"required\":false}},\"endAttributes\":{\"ended\":{\"type\":\"string\",\"description\":\"end marker\"}},\"events\":{\"finished\":{\"description\":\"operation finished\",\"attributes\":{\"event-flag\":{\"type\":\"boolean\",\"description\":\"event flag\",\"required\":false}}}},\"status\":{\"default\":\"ok\",\"errorWhen\":\"operation fails\\\\when\"}}}}"
    );

    let duplicate = pi_telemetry::validate_unique_telemetry_span_names(&[&schema, &schema])
        .expect_err("duplicate span names must be rejected");
    assert_eq!(duplicate.names, vec!["operation".to_owned()]);
}

impl TelemetryContext for FinishCounterSpan {
    fn start_span(&self, options: SpanOptions) -> SpanGuard {
        self.start_child(options)
    }
}
impl TelemetryContext for ParentSpan {
    fn start_span(&self, options: SpanOptions) -> SpanGuard {
        self.start_child(options)
    }
}
