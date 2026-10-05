//! Public Agent facade with standalone search and telemetry contracts.
pub use pi_agent_search::{
    EntrySearchHit, SearchQuery, SessionSearchHit, SessionSearchService, SessionSearchTopEntry,
};
pub use pi_telemetry::{
    AttributeValue, FiniteTelemetryNumber, InMemoryTelemetryContext, NOOP_TELEMETRY_CONTEXT,
    NOOP_TELEMETRY_SPAN, NoopTelemetryContext, NoopTelemetrySpan, RecordedTelemetryEvent,
    RecordedTelemetrySpan, SpanAttributes, SpanGuard, SpanOptions, SpanStatus,
    TelemetryAttributeDefinition, TelemetryAttributeMetadata, TelemetryAttributeType,
    TelemetryCardinality, TelemetryContext, TelemetryDefaultStatus, TelemetryErrorInfo,
    TelemetryEventAttributeDefinition, TelemetryEventDefinition, TelemetryParentDefinition,
    TelemetrySchemaDefinition, TelemetrySpan, TelemetrySpanDefinition,
    TelemetryStartAttributeDefinition, TelemetryStatusDefinition, define_telemetry_schema,
    span_attributes_to_json, validate_unique_telemetry_span_names,
};
