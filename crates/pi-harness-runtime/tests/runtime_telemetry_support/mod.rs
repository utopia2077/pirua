use pi_telemetry::*;
use std::sync::{Arc, Mutex};
#[derive(Clone)]
pub struct Row {
    pub name: String,
    pub parent: Option<usize>,
    pub attrs: SpanAttributes,
    pub status: Option<SpanStatus>,
    pub finishes: usize,
}
#[derive(Clone, Default)]
pub struct Recorder(pub Arc<Mutex<Vec<Row>>>);
impl Recorder {
    fn start(&self, parent: Option<usize>, options: SpanOptions) -> SpanGuard {
        let id = {
            let mut rows = self.0.lock().unwrap();
            let id = rows.len();
            rows.push(Row {
                name: options.name,
                parent,
                attrs: options.attributes,
                status: None,
                finishes: 0,
            });
            id
        };
        SpanGuard::new(Recorded {
            recorder: self.clone(),
            id,
        })
    }
    pub fn rows(&self) -> Vec<Row> {
        self.0.lock().unwrap().clone()
    }
    pub fn assert_finished(&self) {
        for (id, row) in self.rows().iter().enumerate() {
            assert_eq!(row.finishes, 1, "span{id} {} must finish once", row.name);
            for (key, value) in &row.attrs {
                assert!(
                    !key.contains("payload")
                        && !key.contains("prompt")
                        && !key.contains("arguments")
                        && !key.contains("message")
                );
                match value {
                    AttributeValue::String(value) => assert!(
                        !value.contains("PRIVATE_"),
                        "payload value recorded at {key}"
                    ),
                    AttributeValue::Strings(values) => assert!(
                        !values.iter().any(|v| v.contains("PRIVATE_")),
                        "payload values recorded at {key}"
                    ),
                    _ => {}
                }
            }
        }
    }
}
impl TelemetryContext for Recorder {
    fn start_span(&self, options: SpanOptions) -> SpanGuard {
        self.start(None, options)
    }
}
struct Recorded {
    recorder: Recorder,
    id: usize,
}
impl TelemetrySpan for Recorded {
    fn start_child(&self, options: SpanOptions) -> SpanGuard {
        self.recorder.start(Some(self.id), options)
    }
    fn add_event(&self, _: &str, attributes: &SpanAttributes) {
        self.set_attributes(attributes)
    }
    fn set_attributes(&self, attributes: &SpanAttributes) {
        self.recorder.0.lock().unwrap()[self.id]
            .attrs
            .extend(attributes.clone());
    }
    fn set_status(&self, status: SpanStatus) {
        self.recorder.0.lock().unwrap()[self.id].status = Some(status);
    }
    fn finish(&self) {
        self.recorder.0.lock().unwrap()[self.id].finishes += 1;
    }
}
pub fn text(row: &Row, key: &str) -> Option<String> {
    match row.attrs.get(key) {
        Some(AttributeValue::String(s)) => Some(s.clone()),
        _ => None,
    }
}
