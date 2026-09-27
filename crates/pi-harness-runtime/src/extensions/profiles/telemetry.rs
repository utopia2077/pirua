//! Native telemetry observation with an independent finite passive queue.
use pi_extension_protocol::{
    profile_live,
    profiles::{Registry, wire_v1_1 as wire},
};
use pi_telemetry::{
    AttributeValue, SpanAttributes, SpanGuard, SpanOptions, SpanStatus, TelemetryContext,
    TelemetrySpan,
};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Start,
    Event,
    Finish,
}
#[derive(Clone, Debug, PartialEq)]
pub struct Record {
    pub kind: Kind,
    pub span_id: pi_ai_core::SafeUnsigned,
    pub parent_span_id: Option<pi_ai_core::SafeUnsigned>,
    pub name: String,
    pub attributes: SpanAttributes,
    pub status: Option<SpanStatus>,
}
impl Record {
    pub fn from_json(value: &Value) -> Result<Self, super::native::ResourceError> {
        use super::native::ResourceError as E;
        let kind = match value["kind"].as_str() {
            Some("span_start") => Kind::Start,
            Some("span_event") => Kind::Event,
            Some("span_finish") => Kind::Finish,
            _ => return Err(E),
        };
        let number = |value: &Value| {
            value
                .as_u64()
                .and_then(|v| pi_ai_core::SafeUnsigned::new(v).ok())
                .ok_or(E)
        };
        let span_id = number(&value["spanId"])?;
        let parent_span_id = value.get("parentSpanId").map(number).transpose()?;
        let name = value["name"].as_str().ok_or(E)?.to_owned();
        let mut attributes = SpanAttributes::new();
        for (name, value) in value["attributes"].as_object().ok_or(E)? {
            let attribute = match value {
                Value::String(value) => AttributeValue::String(value.clone()),
                Value::Bool(value) => AttributeValue::Boolean(*value),
                Value::Number(value) => AttributeValue::Number(
                    pi_telemetry::FiniteTelemetryNumber::new(value.as_f64().ok_or(E)?)
                        .map_err(|_| E)?,
                ),
                _ => return Err(E),
            };
            attributes.insert(name.clone(), attribute);
        }
        let status = match value.get("status").and_then(Value::as_str) {
            None | Some("unset") => None,
            Some("ok") => Some(SpanStatus::Ok),
            Some("error") => Some(SpanStatus::Error { error: None }),
            _ => return Err(E),
        };
        if kind == Kind::Finish && status.is_none() || kind != Kind::Finish && status.is_some() {
            return Err(E);
        }
        Ok(Self {
            kind,
            span_id,
            parent_span_id,
            name,
            attributes,
            status,
        })
    }
}
struct State {
    queue: profile_live::Telemetry,
    registry: Registry,
}
/// Supply this ordinary native TelemetryContext through factory options. It
/// mirrors native spans while remote failures affect only its passive queue.
#[derive(Clone)]
pub struct ProjectedTelemetry {
    local: Arc<dyn TelemetryContext>,
    state: Arc<Mutex<State>>,
    parent: Option<u64>,
}
impl ProjectedTelemetry {
    pub fn new(
        local: Arc<dyn TelemetryContext>,
        capacity_bytes: u64,
        pending_limit: usize,
    ) -> Result<Self, profile_live::Error> {
        Ok(Self {
            local,
            state: Arc::new(Mutex::new(State {
                queue: profile_live::Telemetry::new(capacity_bytes, pending_limit)?,
                registry: Registry::bundled()?,
            })),
            parent: None,
        })
    }
    pub fn disabled(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .queue
            .disabled()
    }
    pub fn dropped(&self) -> u64 {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .queue
            .dropped()
    }
    pub fn take_batch(&self) -> Option<wire::TelemetryEmitBatchRequest> {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .queue
            .take_batch()
    }
    pub fn ack(
        &self,
        sequence: u64,
        ack: wire::TelemetryEmitBatchResult,
    ) -> Result<(), profile_live::Error> {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .queue
            .ack(sequence, ack)
    }
    pub fn flush_ack(&self, requested: u64, ack: u64) -> Result<(), profile_live::Error> {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .queue
            .flush(requested, ack)
    }
    pub fn disable(&self) {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .queue
            .disable();
    }
    fn observe(
        &self,
        kind: &str,
        id: Option<u64>,
        name: &str,
        attrs: &SpanAttributes,
        status: Option<&SpanStatus>,
    ) -> Option<u64> {
        let attributes: serde_json::Map<String, Value> = attrs
            .iter()
            .filter_map(|(name, value)| {
                let value = match value {
                    AttributeValue::String(v) => Value::String(v.clone()),
                    AttributeValue::Boolean(v) => Value::Bool(*v),
                    AttributeValue::Number(v)
                        if name == "pi.step.attempt"
                            && v.get() >= 0.0
                            && v.get() <= 9_007_199_254_740_991.0
                            && v.get().fract() == 0.0 =>
                    {
                        Value::from(v.get() as u64)
                    }
                    AttributeValue::Number(v) => Value::from(v.get()),
                    _ => return None,
                };
                Some((name.clone(), value))
            })
            .collect();
        let mut record =
            json!({"kind":kind,"spanId":id.unwrap_or(0),"name":name,"attributes":attributes});
        if let Some(parent) = self.parent {
            record["parentSpanId"] = Value::from(parent);
        }
        if let Some(status) = status {
            record["status"] = Value::from(if matches!(status, SpanStatus::Ok) {
                "ok"
            } else {
                "error"
            });
        }
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let State { queue, registry } = &mut *state;
        match queue.project(registry, record) {
            Ok(Some(record)) => {
                let id = match record.wire().value.as_ref()?.value.as_ref()? {
                    wire::wire_value::Value::MapValue(map) => map
                        .entries
                        .iter()
                        .find(|entry| entry.key == "spanId")
                        .and_then(|entry| entry.value.as_ref())
                        .and_then(|v| v.value.as_ref())
                        .and_then(|v| match v {
                            wire::wire_value::Value::IntegerValue(v) => u64::try_from(*v).ok(),
                            _ => None,
                        }),
                    _ => None,
                };
                if queue.enqueue(vec![record]).is_err() {
                    queue.disable();
                }
                id
            }
            Ok(None) => None,
            Err(_) => {
                queue.disable();
                None
            }
        }
    }
}
impl TelemetryContext for ProjectedTelemetry {
    fn start_span(&self, options: SpanOptions) -> SpanGuard {
        let guard = self.local.start_span(options.clone());
        let id = self.observe("span_start", None, &options.name, &options.attributes, None);
        SpanGuard::new(ProjectedSpan {
            context: self.clone(),
            guard,
            name: options.name,
            id,
            state: Mutex::new(SpanState {
                attributes: options.attributes,
                status: SpanStatus::Ok,
                finished: false,
            }),
        })
    }
}
struct SpanState {
    attributes: SpanAttributes,
    status: SpanStatus,
    finished: bool,
}
struct ProjectedSpan {
    context: ProjectedTelemetry,
    guard: SpanGuard,
    name: String,
    id: Option<u64>,
    state: Mutex<SpanState>,
}
impl TelemetrySpan for ProjectedSpan {
    fn start_child(&self, options: SpanOptions) -> SpanGuard {
        let guard = self.guard.start_child(options.clone());
        let mut context = self.context.clone();
        context.parent = self.id;
        let id = context.observe("span_start", None, &options.name, &options.attributes, None);
        SpanGuard::new(Self {
            context,
            guard,
            name: options.name,
            id,
            state: Mutex::new(SpanState {
                attributes: options.attributes,
                status: SpanStatus::Ok,
                finished: false,
            }),
        })
    }
    fn add_event(&self, name: &str, attributes: &SpanAttributes) {
        self.guard.add_event(name, attributes);
        if self
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .finished
        {
            return;
        }
        if let Some(id) = self.id {
            self.context
                .observe("span_event", Some(id), &self.name, attributes, None);
        }
    }
    fn set_attributes(&self, attributes: &SpanAttributes) {
        self.guard.set_attributes(attributes);
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if !state.finished {
            state.attributes.extend(attributes.clone());
        }
    }
    fn set_status(&self, status: SpanStatus) {
        self.guard.set_status(status.clone());
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if !state.finished {
            state.status = if matches!(status, SpanStatus::Ok) {
                SpanStatus::Ok
            } else {
                SpanStatus::Error { error: None }
            };
        }
    }
    fn finish(&self) {
        let record = {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            if state.finished {
                return;
            }
            state.finished = true;
            (state.attributes.clone(), state.status.clone())
        };
        if let Some(id) = self.id {
            self.context.observe(
                "span_finish",
                Some(id),
                &self.name,
                &record.0,
                Some(&record.1),
            );
        }
    }
}

impl<
    C: Send + Sync + 'static,
    S: pi_agent_session::storage::RuntimeStorage + 'static,
    M: pi_agent_schema::SessionMetadataRecord,
> crate::owner::RuntimeHarness<C, S, M>
{
    /// Deliver one already projected bounded batch. An acknowledgement failure
    /// disables only this observer and returns without any Effects settlement.
    pub async fn deliver_profile_telemetry(
        &self,
        connection: Arc<super::connection::ProfileConnection>,
        capability: wire::CapabilityRef,
        sink: &ProjectedTelemetry,
    ) -> bool {
        use pi_extension_protocol::{
            profile_live::{Role, encode_payload},
            profiles::Stage,
        };
        let Some(batch) = sink.take_batch() else {
            return !sink.disabled();
        };
        let result = async {
            let payload = encode_payload(&batch, connection.max_payload_bytes())
                .map_err(|_| super::fault())?;
            let policy = pi_ai_core::ModelHeaderPolicy::default();
            let mut native =
                super::native::NativeDomain::new(self.main.owner.session.codec(), &policy);
            let cancel = self.main.owner.closed.clone();
            let scope = super::connection::CallScope {
                key: None,
                role: Role::Passive,
                parent: None,
                native_parent: None,
                permit: None,
                slot: None,
                dispatch: None,
            };
            let reply = super::resources::ConsumerReply::new(
                connection.clone(),
                connection
                    .call(
                        capability,
                        "telemetry.emit_batch",
                        payload,
                        scope,
                        &mut native,
                        None,
                        &cancel,
                    )
                    .await?,
            );
            let value = reply
                .reply()
                .payload
                .as_ref()
                .filter(|p| p.stage == Stage::Terminal && reply.reply().error.is_none())
                .ok_or_else(super::fault)?;
            let ack = wire::TelemetryEmitBatchResult {
                accepted: value.fields["accepted"].as_u64().ok_or_else(super::fault)?,
                dropped: value.fields["dropped"].as_u64().ok_or_else(super::fault)?,
            };
            sink.ack(batch.sequence, ack).map_err(|_| super::fault())?;
            reply.consumed()?;
            Ok::<_, pi_agent_schema::HarnessRuntimeError>(())
        }
        .await;
        if result.is_err() {
            sink.disable();
            false
        } else {
            true
        }
    }
    /// Explicit best-effort flush of emitted telemetry, never a close or
    /// durable-commit barrier. Future/unemitted acknowledgements disable sink.
    pub async fn flush_profile_telemetry(
        &self,
        connection: Arc<super::connection::ProfileConnection>,
        capability: wire::CapabilityRef,
        sink: &ProjectedTelemetry,
        through: u64,
    ) -> bool {
        use pi_extension_protocol::profile_live::{Role, encode_payload};
        let result = async {
            let request = wire::TelemetryFlushRequest {
                through_sequence: through,
            };
            let payload = encode_payload(&request, connection.max_payload_bytes())
                .map_err(|_| super::fault())?;
            let policy = pi_ai_core::ModelHeaderPolicy::default();
            let mut native =
                super::native::NativeDomain::new(self.main.owner.session.codec(), &policy);
            let cancel = self.main.owner.closed.clone();
            let scope = super::connection::CallScope {
                key: None,
                role: Role::Passive,
                parent: None,
                native_parent: None,
                permit: None,
                slot: None,
                dispatch: None,
            };
            let reply = super::resources::ConsumerReply::new(
                connection.clone(),
                connection
                    .call(
                        capability,
                        "telemetry.flush",
                        payload,
                        scope,
                        &mut native,
                        None,
                        &cancel,
                    )
                    .await?,
            );
            let ack = reply
                .reply()
                .payload
                .as_ref()
                .filter(|_| reply.reply().error.is_none())
                .and_then(|p| p.fields["flushed_through"].as_u64())
                .ok_or_else(super::fault)?;
            sink.flush_ack(through, ack).map_err(|_| super::fault())?;
            reply.consumed()?;
            Ok::<_, pi_agent_schema::HarnessRuntimeError>(())
        }
        .await;
        if result.is_err() {
            sink.disable();
            false
        } else {
            true
        }
    }
}
