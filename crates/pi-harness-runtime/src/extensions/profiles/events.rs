//! A separate finite subscription attached directly to native publication order.
//! It never starts a trusted watch or consumes its unbounded buffer.
use super::native::NativeDomain;
use crate::extensions::transport::MonotonicClock;
use pi_agent_schema::*;
use pi_agent_session::{session::RuntimeSession, storage::RuntimeStorage};
use pi_extension_protocol::{
    activation::GrantedProfile,
    jcs,
    profile_live::{EventQueue, encode_payload},
    profiles::{Stage, wire_v1_1 as dto},
    wire::value,
};
use std::{
    collections::VecDeque,
    io::{self, Write},
    sync::{Arc, Mutex, Weak},
};

pub(crate) trait Readiness: Send + Sync {
    fn ready(&self) -> bool;
}
trait Projection: Send + Sync {
    fn snapshot(
        &self,
        contract: &GrantedProfile,
        snapshot: &LaneSnapshot,
        id: &str,
        revision: u64,
        next: u64,
        max: usize,
    ) -> Result<dto::SubscriptionSnapshot, HarnessRuntimeError>;
    fn event(
        &self,
        contract: &GrantedProfile,
        event: &HarnessEvent,
        id: &str,
        sequence: u64,
        max: usize,
    ) -> Result<Vec<u8>, HarnessRuntimeError>;
}
struct NativeProjection<S: RuntimeStorage, M: SessionMetadataRecord> {
    session: Arc<RuntimeSession<S, M>>,
}
impl<S: RuntimeStorage + 'static, M: SessionMetadataRecord> Projection for NativeProjection<S, M> {
    fn snapshot(
        &self,
        contract: &GrantedProfile,
        snapshot: &LaneSnapshot,
        id: &str,
        revision: u64,
        next: u64,
        max: usize,
    ) -> Result<dto::SubscriptionSnapshot, HarnessRuntimeError> {
        let mut writer = BoundedWriter {
            bytes: Vec::new(),
            max,
        };
        serde_json::to_writer(&mut writer, snapshot).map_err(|_| super::fault())?;
        let mut value: serde_json::Value =
            serde_json::from_slice(&writer.bytes).map_err(|_| super::fault())?;
        sanitize(&mut value);
        let snapshot = dto::SubscriptionSnapshot {
            subscription_id: id.into(),
            snapshot_revision: revision,
            next_event_sequence: next,
            filtered_snapshot: Some(
                super::bound(
                    contract.approved().registry(),
                    "event.filtered_snapshot.v1",
                    &value,
                )
                .map_err(|_| super::fault())?,
            ),
        };
        let item = dto::EventSinkItem {
            item: Some(dto::event_sink_item::Item::Snapshot(snapshot.clone())),
        };
        self.validate(
            contract,
            Stage::Progress,
            &encode_payload(&item, max).map_err(|_| super::fault())?,
            max,
        )?;
        Ok(snapshot)
    }
    fn event(
        &self,
        contract: &GrantedProfile,
        event: &HarnessEvent,
        id: &str,
        sequence: u64,
        max: usize,
    ) -> Result<Vec<u8>, HarnessRuntimeError> {
        let mut writer = BoundedWriter {
            bytes: Vec::new(),
            max,
        };
        serde_json::to_writer(&mut writer, event).map_err(|_| super::fault())?;
        let mut value: serde_json::Value =
            serde_json::from_slice(&writer.bytes).map_err(|_| super::fault())?;
        sanitize(&mut value);
        let item = dto::EventSinkItem {
            item: Some(dto::event_sink_item::Item::Event(dto::EventNotification {
                subscription_id: id.into(),
                sequence,
                event: Some(
                    super::bound(
                        contract.approved().registry(),
                        "event.filtered_event.v1",
                        &value,
                    )
                    .map_err(|_| super::fault())?,
                ),
            })),
        };
        let bytes = encode_payload(&item, max).map_err(|_| super::fault())?;
        self.validate(contract, Stage::Progress, &bytes, max)?;
        Ok(bytes)
    }
}
impl<S: RuntimeStorage + 'static, M: SessionMetadataRecord> NativeProjection<S, M> {
    fn validate(
        &self,
        contract: &GrantedProfile,
        stage: Stage,
        bytes: &[u8],
        max: usize,
    ) -> Result<(), HarnessRuntimeError> {
        let policy = pi_ai_core::ModelHeaderPolicy::default();
        let mut native = NativeDomain::new(self.session.codec(), &policy);
        contract
            .approved()
            .validate_payload(
                contract
                    .reference()
                    .profile
                    .as_ref()
                    .expect("selected profile")
                    .minor,
                "event.subscribe",
                stage,
                bytes,
                value::Limits {
                    json_bytes: max,
                    decoded_bytes: max,
                    string_bytes: max,
                    bytes_length: max,
                    elements: max,
                    depth: 64,
                },
                jcs::Limits {
                    input_bytes: max,
                    output_bytes: max,
                    decoded_bytes: max,
                    nodes: max,
                    elements: max,
                    depth: 64,
                    schema_work: max.saturating_mul(8),
                    instance_work: max.saturating_mul(8),
                },
                &mut native,
            )
            .map_err(|_| super::fault())?;
        Ok(())
    }
}
struct BoundedWriter {
    bytes: Vec<u8>,
    max: usize,
}
impl Write for BoundedWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self
            .bytes
            .len()
            .checked_add(bytes.len())
            .is_none_or(|n| n > self.max)
        {
            return Err(io::Error::other("passive projection exceeds bound"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
pub(crate) fn sanitize_snapshot(value: &mut serde_json::Value) {
    if let Some(entries) = value
        .get_mut("transcript")
        .and_then(serde_json::Value::as_array_mut)
    {
        for entry in entries {
            if let Some(message) = entry.get_mut("message") {
                sanitize_message(message);
            }
        }
    }
    if let Some(queues) = value.get_mut("queues") {
        sanitize_queues(queues);
    }
    if let Some(writes) = value
        .get_mut("pendingWrites")
        .and_then(serde_json::Value::as_array_mut)
    {
        for write in writes {
            if write["type"] == "message"
                && let Some(message) = write.get_mut("message")
            {
                sanitize_message(message);
            }
        }
    }
    if let Some(message) = value
        .get_mut("operation")
        .and_then(|operation| operation.get_mut("streamingMessage"))
    {
        sanitize_message(message);
    }
}
fn sanitize_queues(value: &mut serde_json::Value) {
    for key in ["steer", "followUp", "nextRun"] {
        if let Some(items) = value.get_mut(key).and_then(serde_json::Value::as_array_mut) {
            for item in items {
                if let Some(message) = item.get_mut("message") {
                    sanitize_message(message);
                }
            }
        }
    }
}
fn sanitize_message(value: &mut serde_json::Value) {
    if value["role"] != "assistant" {
        return;
    }
    if let Some(map) = value.as_object_mut() {
        for field in ["diagnostics", "rawStopReason", "responseId"] {
            map.remove(field);
        }
        if map.contains_key("errorMessage") {
            map.insert("errorMessage".into(), "request failed".into());
        }
    }
}
fn sanitize(value: &mut serde_json::Value) {
    if value.get("transcript").is_some() {
        sanitize_snapshot(value);
        return;
    }
    let event_type = value["type"].as_str().unwrap_or("").to_owned();
    match event_type.as_str() {
        "message_start" | "message_update" | "message_end" | "turn_end" => {
            if let Some(message) = value.get_mut("message") {
                sanitize_message(message);
            }
            if let Some(event) = value.get_mut("event") {
                for field in ["partial", "message", "error"] {
                    if let Some(message) = event.get_mut(field) {
                        sanitize_message(message);
                    }
                }
            }
        }
        "run_end" => {
            if let Some(message) = value.get_mut("finalMessage") {
                sanitize_message(message);
            }
        }
        "entry_added" => {
            if let Some(message) = value
                .get_mut("entry")
                .and_then(|entry| entry.get_mut("message"))
            {
                sanitize_message(message);
            }
        }
        "queue_update" => sanitize_queues(value),
        "run_abort" => {
            for field in ["steer", "followUp"] {
                if let Some(messages) = value
                    .get_mut(field)
                    .and_then(serde_json::Value::as_array_mut)
                {
                    for message in messages {
                        sanitize_message(message);
                    }
                }
            }
        }
        _ => {}
    }
    if let Some(map) = value.as_object_mut() {
        if event_type == "handler_error" {
            map.remove("stack");
            map.insert("error".into(), "handler_error".into());
        }
        if event_type == "fault" {
            map.insert("code".into(), "runtime_fault".into());
            map.insert("message".into(), "operation failed".into());
        }
        if matches!(
            event_type.as_str(),
            "retry_scheduled" | "retry_start" | "retry_end"
        ) {
            for field in ["error", "finalError"] {
                if map.get(field).is_some_and(serde_json::Value::is_string) {
                    map.insert(field.into(), "request failed".into());
                }
            }
        }
        if matches!(
            event_type.as_str(),
            "run_end" | "compaction_end" | "navigation_end"
        ) && let Some(error) = map
            .get_mut("error")
            .and_then(serde_json::Value::as_object_mut)
            && error.contains_key("message")
        {
            error.insert("message".into(), "operation failed".into());
        }
    }
}
pub struct EventOptions {
    pub queue_bytes: u64,
    pub gap_bytes: u64,
    pub deadline_ms: u64,
    pub max_subscriptions: std::num::NonZeroUsize,
}
struct State {
    queue: EventQueue,
    ready: VecDeque<Weak<dyn Readiness>>,
    gap_ready: Option<Weak<dyn Readiness>>,
    snapshot: Option<dto::SubscriptionSnapshot>,
    active: bool,
    epoch: u64,
}
pub struct ProjectedEvents {
    pub(crate) lane: LaneId,
    contract: GrantedProfile,
    id: String,
    generation: u64,
    max: usize,
    projection: Arc<dyn Projection>,
    clock: Arc<dyn MonotonicClock>,
    deadline: u64,
    state: Mutex<State>,
    changed: Mutex<pi_ai_core::CancellationToken>,
}
impl ProjectedEvents {
    // Keep the captured native authority and independent finite route inputs explicit.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new<S: RuntimeStorage + 'static, M: SessionMetadataRecord>(
        lane: LaneId,
        session: Arc<RuntimeSession<S, M>>,
        contract: GrantedProfile,
        id: String,
        generation: u64,
        max: usize,
        revision: u64,
        snapshot: &LaneSnapshot,
        clock: Arc<dyn MonotonicClock>,
        options: EventOptions,
    ) -> Result<Arc<Self>, HarnessRuntimeError> {
        if contract.approved().profile() != "event-sink.v1"
            || generation == 0
            || options.deadline_ms <= clock.now_ms()
        {
            return Err(super::fault());
        }
        let projection: Arc<dyn Projection> = Arc::new(NativeProjection { session });
        let baseline = projection.snapshot(&contract, snapshot, &id, revision, 0, max)?;
        let queue = EventQueue::new(
            id.clone(),
            generation,
            options.queue_bytes,
            options.gap_bytes,
            0,
            revision,
            options.deadline_ms,
        )
        .map_err(|_| super::fault())?;
        Ok(Arc::new(Self {
            lane,
            contract,
            id,
            generation,
            max,
            projection,
            clock,
            deadline: options.deadline_ms,
            state: Mutex::new(State {
                queue,
                ready: VecDeque::new(),
                gap_ready: None,
                snapshot: Some(baseline),
                active: true,
                epoch: 0,
            }),
            changed: Mutex::new(pi_ai_core::CancellationToken::new()),
        }))
    }
    pub fn subscription_id(&self) -> &str {
        &self.id
    }
    pub fn paused(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .queue
            .paused()
    }
    pub fn dropped(&self) -> u64 {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .queue
            .dropped()
    }
    pub fn active(&self) -> bool {
        self.state.lock().unwrap_or_else(|p| p.into_inner()).active
    }
    pub fn cancel(&self) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        state.active = false;
        state.queue.cancel();
        state.ready.clear();
        state.gap_ready = None;
        state.snapshot = None;
        drop(state);
        self.notify();
    }
    pub(crate) fn notify(&self) {
        let old = {
            let mut token = self.changed.lock().unwrap_or_else(|p| p.into_inner());
            std::mem::replace(&mut *token, pi_ai_core::CancellationToken::new())
        };
        old.cancel();
    }
    pub(crate) fn changed(&self) -> pi_ai_core::CancellationToken {
        self.changed
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }
    pub(crate) fn epoch(&self) -> u64 {
        self.state.lock().unwrap_or_else(|p| p.into_inner()).epoch
    }
    pub(crate) fn deadline(&self) -> u64 {
        self.deadline
    }
    pub(crate) fn clock(&self) -> Arc<dyn MonotonicClock> {
        self.clock.clone()
    }
    pub(crate) fn publish(&self, event: &HarnessEvent, revision: u64, ready: Weak<dyn Readiness>) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if !state.active {
            return;
        }
        if self.clock.now_ms() >= self.deadline {
            state.active = false;
            state.queue.cancel();
            return;
        }
        let cursor = state.queue.cursor();
        let bytes = match self
            .projection
            .event(&self.contract, event, &self.id, cursor, self.max)
        {
            Ok(bytes) => bytes,
            Err(_) => {
                state.active = false;
                state.queue.cancel();
                return;
            }
        };
        let paused = state.queue.paused();
        if state
            .queue
            .publish(
                self.generation,
                cursor,
                revision,
                bytes,
                self.clock.now_ms(),
            )
            .is_err()
        {
            state.active = false;
            state.queue.cancel();
            return;
        }
        if !paused && state.queue.paused() {
            state.ready.clear();
            state.gap_ready = Some(ready);
        } else if !state.queue.paused() {
            state.ready.push_back(ready);
        }
    }
    /// Pull only the independent bounded projection. No trusted watch exists.
    pub fn take(&self) -> Result<Option<Vec<u8>>, HarnessRuntimeError> {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if !state.active {
            return Ok(None);
        }
        if self.clock.now_ms() >= self.deadline {
            state.active = false;
            state.queue.cancel();
            return Ok(None);
        }
        if let Some(snapshot) = state.snapshot.take() {
            return encode_payload(
                &dto::EventSinkItem {
                    item: Some(dto::event_sink_item::Item::Snapshot(snapshot)),
                },
                self.max,
            )
            .map(Some)
            .map_err(|_| super::fault());
        }
        if state.queue.paused() {
            if state
                .gap_ready
                .as_ref()
                .and_then(Weak::upgrade)
                .is_some_and(|ready| !ready.ready())
            {
                return Ok(None);
            }
            if state.gap_ready.take().is_some() {
                let gap = state.queue.gap().cloned().ok_or_else(super::fault)?;
                return encode_payload(
                    &dto::EventSinkItem {
                        item: Some(dto::event_sink_item::Item::Gap(gap)),
                    },
                    self.max,
                )
                .map(Some)
                .map_err(|_| super::fault());
            }
            return Ok(None);
        }
        if state
            .ready
            .front()
            .and_then(Weak::upgrade)
            .is_some_and(|ready| !ready.ready())
        {
            return Ok(None);
        }
        let bytes = state.queue.take_event();
        if bytes.is_some() {
            state.ready.pop_front();
        }
        Ok(bytes)
    }
    pub(crate) fn resync(
        &self,
        snapshot: &LaneSnapshot,
        revision: u64,
    ) -> Result<dto::SubscriptionSnapshot, HarnessRuntimeError> {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if !state.active {
            return Err(super::fault());
        }
        let baseline = self.projection.snapshot(
            &self.contract,
            snapshot,
            &self.id,
            revision,
            state.queue.cursor(),
            self.max,
        )?;
        let cursor = state.queue.cursor();
        state
            .queue
            .resync(self.generation, revision, cursor, self.clock.now_ms())
            .map_err(|_| super::fault())?;
        state.ready.clear();
        state.gap_ready = None;
        state.snapshot = None;
        state.epoch = state.epoch.checked_add(1).ok_or_else(super::fault)?;
        drop(state);
        Ok(baseline)
    }
}
