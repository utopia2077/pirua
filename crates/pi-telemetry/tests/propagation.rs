use pi_telemetry::*;
use std::sync::Arc;
fn options(name: &str) -> SpanOptions {
    SpanOptions {
        name: name.into(),
        attributes: SpanAttributes::new(),
        links: Vec::new(),
    }
}
#[test]
fn links_and_parent_propagation_preserve_trace_after_settlement() {
    let local = InMemoryTelemetryContext::new();
    let root = local.start_span(options("root"));
    let parent = root.parent();
    let link = SpanLink {
        context: parent.clone(),
        attributes: SpanAttributes::from([(
            "relation".into(),
            AttributeValue::String("cause".into()),
        )]),
    };
    let mut child_options = options("child");
    child_options.links.push(link.clone());
    let child = parent.start_span(child_options);
    child.add_link(link.clone());
    child.finish();
    let mut sibling_options = options("sibling");
    sibling_options.links.push(link.clone());
    local.start_span(sibling_options).finish();
    let carrier = local.inject(parent.as_ref()).unwrap();
    root.finish();
    let remote = InMemoryTelemetryContext::new();
    let extracted = remote.extract(Some(&carrier));
    let mut remote_options = options("remote");
    remote_options.links.push(link);
    extracted.start_span(remote_options).finish();
    let spans = local.spans();
    assert_eq!(spans.len(), 3);
    assert_eq!(spans[1].parent_id, Some(spans[0].id));
    assert_eq!(spans[0].trace_id, spans[1].trace_id);
    assert_eq!(spans[1].links.len(), 2);
    assert_eq!(spans[2].links.len(), 1);
    let remote_spans = remote.spans();
    assert_eq!(remote_spans[0].trace_id, spans[0].trace_id);
    assert_eq!(remote_spans[0].parent_id, None);
    assert_eq!(
        remote_spans[0].parent_span_id,
        Some(spans[0].id.to_string())
    );
    assert_eq!(remote_spans[0].links[0].trace_id, spans[0].trace_id);
    local
        .extract(Some(&carrier))
        .start_span(options("local-extracted"))
        .finish();
    assert_eq!(local.spans()[3].parent_id, Some(spans[0].id));
}
#[test]
fn noop_propagation_does_not_invent_context_or_links() {
    assert_eq!(
        NOOP_TELEMETRY_PROPAGATION.inject(&NOOP_TELEMETRY_CONTEXT),
        None
    );
    let context = NOOP_TELEMETRY_PROPAGATION.extract(None);
    let mut span_options = options("noop");
    span_options.links.push(SpanLink {
        context: Arc::new(NOOP_TELEMETRY_CONTEXT),
        attributes: SpanAttributes::new(),
    });
    context.start_span(span_options).finish();
    assert!(
        InMemoryTelemetryContext::new()
            .inject(&NOOP_TELEMETRY_CONTEXT)
            .is_none()
    );
}
