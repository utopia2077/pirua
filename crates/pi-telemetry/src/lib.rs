#![deny(missing_docs)]

//! Executor-neutral telemetry contracts for the Rust harness workspace.
//!
//! This crate defines process-local observation handles, detached in-memory
//! recording, and serializable telemetry schema metadata. It does not define
//! a persistence format or exporter.

mod memory;
mod noop;
mod schema;

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::sync::Arc;

pub use memory::{
    InMemoryTelemetryContext, RecordedSpanLink, RecordedTelemetryEvent, RecordedTelemetrySpan,
};
pub use noop::{
    NOOP_TELEMETRY_CONTEXT, NOOP_TELEMETRY_PROPAGATION, NOOP_TELEMETRY_SPAN, NoopTelemetryContext,
    NoopTelemetryPropagation, NoopTelemetrySpan,
};
pub use schema::{
    DuplicateTelemetrySpanNames, TelemetryAttributeDefinition, TelemetryAttributeMetadata,
    TelemetryAttributeType, TelemetryCardinality, TelemetryDefaultStatus,
    TelemetryEventAttributeDefinition, TelemetryEventDefinition, TelemetryParentDefinition,
    TelemetrySchemaDefinition, TelemetrySpanDefinition, TelemetryStartAttributeDefinition,
    TelemetryStatusDefinition, define_telemetry_schema, span_attributes_to_json,
    validate_unique_telemetry_span_names,
};

/// A finite floating-point number accepted at the telemetry boundary.
#[derive(Clone, Copy, PartialEq, PartialOrd)]
pub struct FiniteTelemetryNumber(f64);

impl FiniteTelemetryNumber {
    /// Validates and wraps a finite floating-point value.
    pub fn new(value: f64) -> Result<Self, NonFiniteTelemetryNumber> {
        if value.is_finite() {
            Ok(Self(value))
        } else {
            Err(NonFiniteTelemetryNumber)
        }
    }

    /// Returns the validated finite value.
    pub fn get(self) -> f64 {
        self.0
    }
}

impl TryFrom<f64> for FiniteTelemetryNumber {
    type Error = NonFiniteTelemetryNumber;

    fn try_from(value: f64) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<FiniteTelemetryNumber> for f64 {
    fn from(value: FiniteTelemetryNumber) -> Self {
        value.get()
    }
}

impl fmt::Debug for FiniteTelemetryNumber {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("FiniteTelemetryNumber([REDACTED])")
    }
}

/// Error returned when a telemetry number is NaN or infinite.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NonFiniteTelemetryNumber;

impl fmt::Display for NonFiniteTelemetryNumber {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("telemetry number must be finite")
    }
}

impl Error for NonFiniteTelemetryNumber {}

/// A scalar or homogeneous-list telemetry attribute value.
#[derive(Clone, PartialEq)]
pub enum AttributeValue {
    /// A string value.
    String(String),
    /// A finite numeric value.
    Number(FiniteTelemetryNumber),
    /// A boolean value.
    Boolean(bool),
    /// A list of string values.
    Strings(Vec<String>),
    /// A list of finite numeric values.
    Numbers(Vec<FiniteTelemetryNumber>),
    /// A list of boolean values.
    Booleans(Vec<bool>),
}

impl fmt::Debug for AttributeValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::String(_) => formatter.write_str("String([REDACTED])"),
            Self::Number(_) => formatter.write_str("Number([REDACTED])"),
            Self::Boolean(_) => formatter.write_str("Boolean([REDACTED])"),
            Self::Strings(values) => formatter
                .debug_struct("Strings")
                .field("len", &values.len())
                .finish_non_exhaustive(),
            Self::Numbers(values) => formatter
                .debug_struct("Numbers")
                .field("len", &values.len())
                .finish_non_exhaustive(),
            Self::Booleans(values) => formatter
                .debug_struct("Booleans")
                .field("len", &values.len())
                .finish_non_exhaustive(),
        }
    }
}

/// Ordered attributes attached to a span or event.
pub type SpanAttributes = BTreeMap<String, AttributeValue>;

/// Inputs used to start a span.
#[derive(Clone, PartialEq)]
pub struct SpanOptions {
    /// Backend-neutral span name.
    pub name: String,
    /// Initial span attributes.
    pub attributes: SpanAttributes,
    /// Causal links independent of the parent relation.
    pub links: Vec<SpanLink>,
}

impl fmt::Debug for SpanOptions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SpanOptions")
            .field("name", &self.name)
            .field("attribute_count", &self.attributes.len())
            .finish()
    }
}

/// Optional error details associated with an error span status.
#[derive(Clone, PartialEq, Eq)]
pub struct TelemetryErrorInfo {
    /// Stable error type or category name.
    pub name: String,
    /// Human-readable error message, redacted from `Debug` output.
    pub message: String,
}

impl fmt::Debug for TelemetryErrorInfo {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TelemetryErrorInfo")
            .field("name", &self.name)
            .field("message", &"[REDACTED]")
            .finish()
    }
}

/// Terminal status recorded for a span.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SpanStatus {
    /// The observed operation completed successfully.
    Ok,
    /// The observed operation failed, with optional sanitized details.
    Error {
        /// Optional error metadata.
        error: Option<TelemetryErrorInfo>,
    },
}

/// An explicit parent context capable of starting a telemetry span.
///
/// Implementations are passive observers of authoritative work. When a
/// session's one-shot receipt invokes an observer, an observer panic propagates
/// to the receipt caller at that boundary; the committed durable authority is
/// not rolled back or replayed. Implementations must not make a second durable
/// write while observing, and [`SpanGuard`] does not catch panics.
pub trait TelemetryContext: Send + Sync {
    /// Adapter-owned propagation carrier, absent for unrecognized/root contexts.
    fn propagation_carrier(&self) -> Option<TelemetryCarrier> {
        None
    }

    /// Starts a span owned by the returned settlement guard.
    fn start_span(&self, options: SpanOptions) -> SpanGuard;
}

/// Backend behavior for one live span.
///
/// Implementations are passive observers. Calls after the span is finished are
/// no-ops, and [`finish`](Self::finish) is idempotent. Observer panics
/// propagate to the caller at the invocation boundary; [`SpanGuard`] does not
/// catch them.
pub trait TelemetrySpan: TelemetryContext {
    /// Adds a causal link while the span is live.
    fn add_link(&self, _link: SpanLink) {}

    /// Starts a child span, or returns a no-op guard when recording is unavailable.
    fn start_child(&self, options: SpanOptions) -> SpanGuard;

    /// Records an event without changing the observed operation's behavior.
    fn add_event(&self, name: &str, attributes: &SpanAttributes);

    /// Merges attributes without changing the observed operation's behavior.
    fn set_attributes(&self, attributes: &SpanAttributes);

    /// Records the current status without changing the observed operation's behavior.
    fn set_status(&self, status: SpanStatus);

    /// Idempotently finishes the span; subsequent observer operations are no-ops.
    fn finish(&self);
}

/// Unique settlement owner for a live telemetry span.
///
/// A guard is intentionally not `Clone`. Calling [`finish`](Self::finish)
/// consumes it; otherwise dropping it finishes the backend exactly once.
///
/// ```compile_fail
/// use pi_telemetry::{NOOP_TELEMETRY_CONTEXT, SpanOptions, TelemetryContext};
///
/// let guard = NOOP_TELEMETRY_CONTEXT.start_span(SpanOptions {
///     name: "operation".to_owned(),
///     attributes: Default::default(),
///     links: Vec::new(),
/// });
/// let _duplicate = guard.clone();
/// ```
pub struct SpanGuard {
    span: Option<Arc<dyn TelemetrySpan>>,
}

impl SpanGuard {
    /// Creates a guard that uniquely owns a backend span handle.
    pub fn new(span: impl TelemetrySpan + 'static) -> Self {
        Self {
            span: Some(Arc::new(span)),
        }
    }

    pub(crate) fn noop() -> Self {
        Self::new(NoopTelemetrySpan)
    }

    /// Shares this live parent without transferring its unique settlement owner.
    pub fn parent(&self) -> Arc<dyn TelemetrySpan> {
        self.span
            .as_ref()
            .cloned()
            .unwrap_or_else(|| Arc::new(NoopTelemetrySpan))
    }

    /// Adds a causal link to this span.
    pub fn add_link(&self, link: SpanLink) {
        if let Some(span) = &self.span {
            span.add_link(link);
        }
    }

    /// Starts a child of this span.
    pub fn start_child(&self, options: SpanOptions) -> SpanGuard {
        match &self.span {
            Some(span) => span.start_child(options),
            None => NOOP_TELEMETRY_CONTEXT.start_span(options),
        }
    }

    /// Delegates an event to the backend span.
    pub fn add_event(&self, name: &str, attributes: &SpanAttributes) {
        if let Some(span) = &self.span {
            span.add_event(name, attributes);
        }
    }

    /// Delegates attributes to the backend span.
    pub fn set_attributes(&self, attributes: &SpanAttributes) {
        if let Some(span) = &self.span {
            span.set_attributes(attributes);
        }
    }

    /// Delegates a status update to the backend span.
    pub fn set_status(&self, status: SpanStatus) {
        if let Some(span) = &self.span {
            span.set_status(status);
        }
    }

    /// Explicitly finishes this span exactly once.
    pub fn finish(mut self) {
        self.settle();
    }

    fn settle(&mut self) {
        if let Some(span) = self.span.take() {
            span.finish();
        }
    }
}

impl fmt::Debug for SpanGuard {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SpanGuard")
            .field("live", &self.span.is_some())
            .finish()
    }
}

impl Drop for SpanGuard {
    fn drop(&mut self) {
        self.settle();
    }
}

/// Opaque string propagation fields owned by a telemetry adapter.
pub type TelemetryCarrier = BTreeMap<String, String>;

/// Optional transport propagation capability.
pub trait TelemetryPropagation: Send + Sync {
    /// Injects a known context, including a settled span.
    fn inject(&self, context: &dyn TelemetryContext) -> Option<TelemetryCarrier>;
    /// Extracts a recognized context; absent carriers return the root.
    fn extract(&self, carrier: Option<&TelemetryCarrier>) -> Arc<dyn TelemetryContext>;
}

/// Causal link to another local or extracted context.
#[derive(Clone)]
pub struct SpanLink {
    /// Opaque linked context.
    pub context: Arc<dyn TelemetryContext>,
    /// Detached optional link attributes.
    pub attributes: SpanAttributes,
}
impl PartialEq for SpanLink {
    fn eq(&self, other: &Self) -> bool {
        self.context.propagation_carrier() == other.context.propagation_carrier()
            && self.attributes == other.attributes
    }
}
impl fmt::Debug for SpanLink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SpanLink")
            .field("attribute_count", &self.attributes.len())
            .finish_non_exhaustive()
    }
}
