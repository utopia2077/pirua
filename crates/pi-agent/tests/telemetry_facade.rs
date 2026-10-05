use pi_agent::{
    AttributeValue, InMemoryTelemetryContext, SpanAttributes, SpanOptions, SpanStatus,
    TelemetryContext,
};

#[test]
fn facade_exposes_detached_telemetry_recording() {
    let context = InMemoryTelemetryContext::new();
    let root = context.start_span(SpanOptions {
        name: "root".to_owned(),
        attributes: SpanAttributes::new(),
    });
    let child = root.start_child(SpanOptions {
        name: "child".to_owned(),
        attributes: SpanAttributes::new(),
    });

    let mut event_attributes = SpanAttributes::new();
    event_attributes.insert("ok".to_owned(), AttributeValue::Boolean(true));
    child.add_event("finished", &event_attributes);
    child.set_status(SpanStatus::Ok);
    child.finish();
    root.finish();

    let spans = context.get_spans();
    assert_eq!(spans.len(), 2);
    assert_eq!(spans[0].name, "root");
    assert_eq!(spans[1].name, "child");
    assert_eq!(spans[1].parent_id, Some(spans[0].id));
    assert_eq!(spans[1].events[0].name, "finished");
    assert_eq!(spans[1].status, SpanStatus::Ok);
    assert!(spans.iter().all(|span| span.settled));
}
