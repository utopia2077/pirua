//! Detached in-memory telemetry recording.

use crate::{
    AttributeValue, SpanAttributes, SpanGuard, SpanOptions, SpanStatus, TelemetryContext,
    TelemetrySpan,
};
use std::fmt;
use std::sync::{Arc, Mutex};

struct InMemoryTelemetryState {
    spans: Vec<MutableRecordedTelemetrySpan>,
    next_span_id: u64,
    next_end_sequence: u64,
}

impl Default for InMemoryTelemetryState {
    fn default() -> Self {
        Self {
            spans: Vec::new(),
            next_span_id: 1,
            next_end_sequence: 1,
        }
    }
}

struct MutableRecordedTelemetrySpan {
    id: u64,
    parent_id: Option<u64>,
    name: String,
    attributes: SpanAttributes,
    events: Vec<RecordedTelemetryEvent>,
    status: SpanStatus,
    explicit_status: bool,
    settled: bool,
    end_sequence: Option<u64>,
}

/// An event captured by an in-memory telemetry recorder.
#[derive(Clone, PartialEq)]
pub struct RecordedTelemetryEvent {
    /// Event name.
    pub name: String,
    /// Detached event attributes.
    pub attributes: SpanAttributes,
}

impl fmt::Debug for RecordedTelemetryEvent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RecordedTelemetryEvent")
            .field("name", &self.name)
            .field("attribute_count", &self.attributes.len())
            .finish()
    }
}

/// A detached snapshot of one recorded telemetry span.
#[derive(Clone, PartialEq)]
pub struct RecordedTelemetrySpan {
    /// Monotonic id assigned at span start.
    pub id: u64,
    /// Parent span id, or `None` for a root span.
    pub parent_id: Option<u64>,
    /// Span name.
    pub name: String,
    /// Detached span attributes after all successful merges.
    pub attributes: SpanAttributes,
    /// Detached events in insertion order.
    pub events: Vec<RecordedTelemetryEvent>,
    /// Final or current span status.
    pub status: SpanStatus,
    /// Whether the span has been settled.
    pub settled: bool,
    /// Monotonic settlement sequence, present once settled.
    pub end_sequence: Option<u64>,
}

impl fmt::Debug for RecordedTelemetrySpan {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RecordedTelemetrySpan")
            .field("id", &self.id)
            .field("parent_id", &self.parent_id)
            .field("name", &self.name)
            .field("attribute_count", &self.attributes.len())
            .field("event_count", &self.events.len())
            .field("status", &self.status)
            .field("settled", &self.settled)
            .field("end_sequence", &self.end_sequence)
            .finish()
    }
}

/// Backend-neutral reference context that records detached spans in memory.
///
/// The recorder is process-local and has no persistence or exporter behavior.
/// Each context owns an independent recording scope. Snapshot methods clone all
/// strings, attributes, and events so callers cannot mutate recorder state.
#[derive(Clone, Default)]
pub struct InMemoryTelemetryContext {
    state: Arc<Mutex<InMemoryTelemetryState>>,
}

impl fmt::Debug for InMemoryTelemetryContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let span_count = self
            .state
            .lock()
            .map(|state| state.spans.len())
            .unwrap_or_default();
        formatter
            .debug_struct("InMemoryTelemetryContext")
            .field("span_count", &span_count)
            .finish()
    }
}

impl InMemoryTelemetryContext {
    /// Creates an empty, isolated in-memory recording context.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns detached snapshots in span-start order.
    pub fn get_spans(&self) -> Vec<RecordedTelemetrySpan> {
        let Ok(state) = self.state.lock() else {
            return Vec::new();
        };
        state
            .spans
            .iter()
            .map(RecordedTelemetrySpan::from)
            .collect()
    }

    /// Returns the same detached snapshots as [`Self::get_spans`].
    pub fn spans(&self) -> Vec<RecordedTelemetrySpan> {
        self.get_spans()
    }
}

impl TelemetryContext for InMemoryTelemetryContext {
    fn start_span(&self, options: SpanOptions) -> SpanGuard {
        let Ok(mut state) = self.state.lock() else {
            return SpanGuard::noop();
        };

        let id = state.next_span_id;
        state.next_span_id = state.next_span_id.saturating_add(1);
        let span = MutableRecordedTelemetrySpan {
            id,
            parent_id: None,
            name: options.name,
            attributes: options.attributes,
            events: Vec::new(),
            status: SpanStatus::Ok,
            explicit_status: false,
            settled: false,
            end_sequence: None,
        };
        state.spans.push(span);
        drop(state);
        SpanGuard::new(InMemoryTelemetrySpan {
            state: Arc::clone(&self.state),
            id,
        })
    }
}

struct InMemoryTelemetrySpan {
    state: Arc<Mutex<InMemoryTelemetryState>>,
    id: u64,
}

impl InMemoryTelemetrySpan {
    fn with_span<R>(
        &self,
        operation: impl FnOnce(&mut MutableRecordedTelemetrySpan) -> R,
    ) -> Option<R> {
        let Ok(mut state) = self.state.lock() else {
            return None;
        };
        let span = state.spans.iter_mut().find(|span| span.id == self.id)?;
        Some(operation(span))
    }

    fn start_child(&self, options: SpanOptions) -> SpanGuard {
        let Ok(mut state) = self.state.lock() else {
            return SpanGuard::noop();
        };
        let Some(parent) = state.spans.iter().find(|span| span.id == self.id) else {
            return SpanGuard::noop();
        };
        if parent.settled {
            return SpanGuard::noop();
        }
        let parent_id = parent.id;
        let id = state.next_span_id;
        state.next_span_id = state.next_span_id.saturating_add(1);
        state.spans.push(MutableRecordedTelemetrySpan {
            id,
            parent_id: Some(parent_id),
            name: options.name,
            attributes: options.attributes,
            events: Vec::new(),
            status: SpanStatus::Ok,
            explicit_status: false,
            settled: false,
            end_sequence: None,
        });
        drop(state);
        SpanGuard::new(Self {
            state: Arc::clone(&self.state),
            id,
        })
    }
}

impl TelemetrySpan for InMemoryTelemetrySpan {
    fn start_child(&self, options: SpanOptions) -> SpanGuard {
        self.start_child(options)
    }

    fn add_event(&self, name: &str, attributes: &SpanAttributes) {
        let name = name.to_owned();
        let attributes = copy_attributes(attributes);
        let _ = self.with_span(|span| {
            if !span.settled {
                span.events
                    .push(RecordedTelemetryEvent { name, attributes });
            }
        });
    }

    fn set_attributes(&self, attributes: &SpanAttributes) {
        let attributes = copy_attributes(attributes);
        let _ = self.with_span(|span| {
            if span.settled {
                return;
            }
            for (name, value) in attributes {
                span.attributes.insert(name, value);
            }
        });
    }

    fn set_status(&self, status: SpanStatus) {
        let _ = self.with_span(|span| {
            if !span.settled {
                span.status = status;
                span.explicit_status = true;
            }
        });
    }

    fn finish(&self) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        let end_sequence = state.next_end_sequence;
        let Some(span) = state.spans.iter_mut().find(|span| span.id == self.id) else {
            return;
        };
        if span.settled {
            return;
        }
        span.settled = true;
        span.end_sequence = Some(end_sequence);
        state.next_end_sequence = end_sequence.saturating_add(1);
    }
}

impl From<&MutableRecordedTelemetrySpan> for RecordedTelemetrySpan {
    fn from(span: &MutableRecordedTelemetrySpan) -> Self {
        Self {
            id: span.id,
            parent_id: span.parent_id,
            name: span.name.clone(),
            attributes: span.attributes.clone(),
            events: span.events.clone(),
            status: span.status.clone(),
            settled: span.settled,
            end_sequence: span.end_sequence,
        }
    }
}

/// Copies a span attribute map into owned storage.
#[allow(dead_code)]
fn copy_attributes(attributes: &SpanAttributes) -> SpanAttributes {
    attributes
        .iter()
        .map(|(name, value)| (name.clone(), copy_attribute_value(value)))
        .collect()
}

fn copy_attribute_value(value: &AttributeValue) -> AttributeValue {
    match value {
        AttributeValue::String(value) => AttributeValue::String(value.clone()),
        AttributeValue::Number(value) => AttributeValue::Number(*value),
        AttributeValue::Boolean(value) => AttributeValue::Boolean(*value),
        AttributeValue::Strings(values) => AttributeValue::Strings(values.clone()),
        AttributeValue::Numbers(values) => AttributeValue::Numbers(values.clone()),
        AttributeValue::Booleans(values) => AttributeValue::Booleans(values.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn settled_spans_ignore_late_operations_and_children() {
        let context = InMemoryTelemetryContext::new();
        let span = context.start_span(SpanOptions {
            name: "root".to_owned(),
            attributes: SpanAttributes::new(),
        });
        span.finish();

        let snapshots = context.get_spans();
        assert_eq!(snapshots.len(), 1);
        assert!(snapshots[0].settled);
        assert_eq!(snapshots[0].end_sequence, Some(1));
    }

    #[test]
    fn direct_settlement_is_idempotent_and_late_calls_are_inert() {
        let state = Arc::new(Mutex::new(InMemoryTelemetryState::default()));
        state
            .lock()
            .expect("state lock")
            .spans
            .push(MutableRecordedTelemetrySpan {
                id: 1,
                parent_id: None,
                name: "root".to_owned(),
                attributes: SpanAttributes::new(),
                events: Vec::new(),
                status: SpanStatus::Ok,
                explicit_status: false,
                settled: false,
                end_sequence: None,
            });
        let span = InMemoryTelemetrySpan {
            state: Arc::clone(&state),
            id: 1,
        };
        let mut attributes = SpanAttributes::new();
        attributes.insert(
            "before".to_owned(),
            AttributeValue::String("value".to_owned()),
        );
        span.set_attributes(&attributes);
        span.add_event("before", &attributes);
        span.set_status(SpanStatus::Error { error: None });
        span.finish();
        span.finish();
        span.set_attributes(&SpanAttributes::from([(
            "after".to_owned(),
            AttributeValue::Boolean(true),
        )]));
        span.add_event("after", &SpanAttributes::new());
        span.set_status(SpanStatus::Ok);
        let child = span.start_child(SpanOptions {
            name: "late-child".to_owned(),
            attributes: SpanAttributes::new(),
        });
        child.finish();

        let state = state.lock().expect("state lock");
        assert_eq!(state.spans.len(), 1);
        let recorded = &state.spans[0];
        assert_eq!(recorded.attributes, attributes);
        assert_eq!(recorded.events.len(), 1);
        assert_eq!(recorded.status, SpanStatus::Error { error: None });
        assert!(recorded.settled);
        assert_eq!(recorded.end_sequence, Some(1));
    }
}
