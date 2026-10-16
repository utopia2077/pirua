use crate::{SpanAttributes, SpanGuard, SpanOptions, SpanStatus, TelemetryContext, TelemetrySpan};

/// Stateless context used when telemetry recording is disabled.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopTelemetryContext;

impl TelemetryContext for NoopTelemetryContext {
    fn start_span(&self, _options: SpanOptions) -> SpanGuard {
        SpanGuard::new(NoopTelemetrySpan)
    }
}

/// Stateless span whose observer operations are passive and idempotent.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopTelemetrySpan;

impl TelemetrySpan for NoopTelemetrySpan {
    fn start_child(&self, _options: SpanOptions) -> SpanGuard {
        SpanGuard::new(Self)
    }

    fn add_event(&self, _name: &str, _attributes: &SpanAttributes) {}

    fn set_attributes(&self, _attributes: &SpanAttributes) {}

    fn set_status(&self, _status: SpanStatus) {}

    fn finish(&self) {}
}

/// Shared no-op parent context.
pub const NOOP_TELEMETRY_CONTEXT: NoopTelemetryContext = NoopTelemetryContext;

/// Shared no-op span implementation.
pub const NOOP_TELEMETRY_SPAN: NoopTelemetrySpan = NoopTelemetrySpan;

impl TelemetryContext for NoopTelemetrySpan {
    fn start_span(&self, _options: SpanOptions) -> SpanGuard {
        SpanGuard::new(*self)
    }
}
/// Stateless no-op propagation adapter.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopTelemetryPropagation;
impl crate::TelemetryPropagation for NoopTelemetryPropagation {
    fn inject(&self, _: &dyn TelemetryContext) -> Option<crate::TelemetryCarrier> {
        None
    }
    fn extract(&self, _: Option<&crate::TelemetryCarrier>) -> std::sync::Arc<dyn TelemetryContext> {
        std::sync::Arc::new(NoopTelemetryContext)
    }
}
/// Shared no-op propagation adapter.
pub const NOOP_TELEMETRY_PROPAGATION: NoopTelemetryPropagation = NoopTelemetryPropagation;
