#![cfg(feature = "extensions")]

use std::collections::VecDeque;
use std::future::{Future, poll_fn};
use std::num::{NonZeroU64, NonZeroUsize};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::task::{Context, Poll, Waker};

use pi_agent_schema::{
    EffectKey, EffectKind, EffectStepId, EntryId, LaneId, OperationId, SessionId, ToolEffectPlan,
};
use pi_ai_core::{BoxFuture, JsonObject, SafeUnsigned, SpawnError, TaskSpawner};
use pi_extension_protocol::activation::{
    AgentTool, AgentToolMode, ApprovedTool, HostAuthority, ToolReplay, ToolSchemaInputs,
};
use pi_extension_protocol::descriptor::canonical_profile_digest;
use pi_extension_protocol::jcs::Limits as JsonLimits;
use pi_extension_protocol::wire::framing::{self, EncodedFrame, FrameReader};
use pi_extension_protocol::wire::{v1_0, v1_1};
use pi_harness_runtime::extensions::tool_driver::{
    CallTiming, DriverError, OwnerLimits, SingleToolOwner, TypedTerminal,
};
use pi_harness_runtime::extensions::transport::{
    ChannelError, MonotonicClock, OrderedByteChannel, PumpConfig,
};
use serde_json::Value;

const CATALOG: &str = include_str!("../../pi-extension-protocol/schema/fixtures/host-catalog.json");
const VECTORS: &str =
    include_str!("../../pi-extension-protocol/schema/fixtures/activation-vectors.json");
const RAW: &str = include_str!("../../pi-extension-protocol/schema/fixtures/raw-vectors.json");

fn nz(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap()
}
fn hex(value: &str) -> Vec<u8> {
    let (pairs, tail) = value.as_bytes().as_chunks::<2>();
    assert!(tail.is_empty());
    pairs
        .iter()
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}
fn varint(mut number: usize) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let mut byte = (number & 127) as u8;
        number >>= 7;
        if number != 0 {
            byte |= 128;
        }
        out.push(byte);
        if number == 0 {
            return out;
        }
    }
}
fn field(number: u8, value: &[u8]) -> Vec<u8> {
    let mut out = vec![(number << 3) | 2];
    out.extend(varint(value.len()));
    out.extend(value);
    out
}
fn frame(arm: u8, body: &[u8]) -> Vec<u8> {
    let envelope = field(arm, body);
    [varint(envelope.len()), envelope].concat()
}
fn hello() -> Vec<u8> {
    let version = [0x08, 0x01, 0x18, 0x01];
    let profile = [field(1, b"tool.v1"), vec![0x10, 0x01, 0x20, 0x01]].concat();
    frame(1, &[field(1, &version), field(2, &profile)].concat())
}
fn hello_core_1_1_tool_1_0() -> Vec<u8> {
    let version = [0x08, 0x01, 0x18, 0x01];
    let profile = [field(1, b"tool.v1"), vec![0x10, 0x01]].concat();
    frame(1, &[field(1, &version), field(2, &profile)].concat())
}
fn manifest() -> Vec<u8> {
    let vectors: Value = serde_json::from_str(VECTORS).unwrap();
    frame(
        3,
        &hex(vectors["cases"]["valid_1_1"]["manifest_pb_hex"]
            .as_str()
            .unwrap()),
    )
}
fn manifest_1_0() -> Vec<u8> {
    let vectors: Value = serde_json::from_str(VECTORS).unwrap();
    frame(
        3,
        &hex(
            vectors["cases"]["forged_1_1_selected_1_0"]["manifest_pb_hex"]
                .as_str()
                .unwrap(),
        ),
    )
}
fn ready_frame() -> Vec<u8> {
    frame(5, &[])
}
fn raw(name: &str, request_id: u8) -> Vec<u8> {
    let vectors: Value = serde_json::from_str(RAW).unwrap();
    let mut bytes = hex(vectors[name]["frame_hex"].as_str().unwrap());
    assert_eq!(bytes[5], 0x08);
    assert_eq!(bytes[6], 17);
    bytes[6] = request_id;
    bytes
}

fn selected_peer_request(
    route: &pi_harness_runtime::extensions::tool_driver::ReadyToolRoute,
    request_id: u8,
    method: &str,
) -> Vec<u8> {
    let reference = route.grant().tools()[0].reference();
    let minor = route
        .grant()
        .wire()
        .selected_protocol
        .as_ref()
        .unwrap()
        .minor;
    if minor == 0 {
        let mut reader = FrameReader::<v1_0::Envelope>::new(nz(8192));
        let mut envelope = reader
            .read_one(&raw("request", request_id))
            .unwrap()
            .frame
            .unwrap()
            .envelope;
        let Some(v1_0::envelope::Frame::Request(request)) = envelope.frame.as_mut() else {
            panic!("request fixture absent")
        };
        let profile = reference.profile.as_ref().unwrap();
        request.capability_ref = Some(v1_0::CapabilityRef {
            authenticated_extension_id: reference.authenticated_extension_id.clone(),
            kind: reference.kind.clone(),
            name: reference.name.clone(),
            profile: Some(v1_0::ProfileVersion {
                name: profile.name.clone(),
                major: profile.major,
                minor: profile.minor,
            }),
            schema_digest_sha256: reference.schema_digest_sha256.clone(),
            handler_id: reference.handler_id.clone(),
        });
        request.method = method.into();
        framing::encode(&envelope, nz(8192)).unwrap().bytes
    } else {
        let mut reader = FrameReader::<v1_1::Envelope>::new(nz(8192));
        let mut envelope = reader
            .read_one(&raw("request", request_id))
            .unwrap()
            .frame
            .unwrap()
            .envelope;
        let Some(v1_1::envelope::Frame::Request(request)) = envelope.frame.as_mut() else {
            panic!("request fixture absent")
        };
        request.capability_ref = Some(reference.clone());
        request.method = method.into();
        framing::encode(&envelope, nz(8192)).unwrap().bytes
    }
}

fn peer_request_with_ref(
    route: &pi_harness_runtime::extensions::tool_driver::ReadyToolRoute,
    method: &str,
    change: impl FnOnce(&mut v1_1::CapabilityRef),
) -> Vec<u8> {
    let mut reference = route.grant().tools()[0].reference().clone();
    change(&mut reference);
    let minor = route.grant().wire().selected_protocol.unwrap().minor;
    if minor == 0 {
        let mut reader = FrameReader::<v1_0::Envelope>::new(nz(8192));
        let mut envelope = reader
            .read_one(&selected_peer_request(route, 17, method))
            .unwrap()
            .frame
            .unwrap()
            .envelope;
        let Some(v1_0::envelope::Frame::Request(request)) = envelope.frame.as_mut() else {
            panic!("request fixture absent")
        };
        request.capability_ref = Some(v1_0::CapabilityRef {
            authenticated_extension_id: reference.authenticated_extension_id,
            kind: reference.kind,
            name: reference.name,
            profile: reference.profile.map(|p| v1_0::ProfileVersion {
                name: p.name,
                major: p.major,
                minor: p.minor,
            }),
            schema_digest_sha256: reference.schema_digest_sha256,
            handler_id: reference.handler_id,
        });
        framing::encode(&envelope, nz(8192)).unwrap().bytes
    } else {
        let mut reader = FrameReader::<v1_1::Envelope>::new(nz(8192));
        let mut envelope = reader
            .read_one(&selected_peer_request(route, 17, method))
            .unwrap()
            .frame
            .unwrap()
            .envelope;
        let Some(v1_1::envelope::Frame::Request(request)) = envelope.frame.as_mut() else {
            panic!("request fixture absent")
        };
        request.capability_ref = Some(reference);
        framing::encode(&envelope, nz(8192)).unwrap().bytes
    }
}
fn selected_terminal(minor: u32, peer_error: bool) -> Vec<u8> {
    if !peer_error {
        return raw("terminal", 1);
    }
    if minor == 0 {
        framing::encode(
            &v1_0::Envelope {
                frame: Some(v1_0::envelope::Frame::StreamEnd(v1_0::StreamEnd {
                    request_id: 1,
                    terminal: Some(v1_0::stream_end::Terminal::Error(v1_0::TypedError {
                        code: v1_0::ErrorCode::HandlerError as i32,
                        safe_message: "PEER_TEXT_MUST_NOT_ESCAPE".into(),
                    })),
                })),
            },
            nz(8192),
        )
        .unwrap()
        .bytes
    } else {
        framing::encode(
            &v1_1::Envelope {
                frame: Some(v1_1::envelope::Frame::StreamEnd(v1_1::StreamEnd {
                    request_id: 1,
                    terminal: Some(v1_1::stream_end::Terminal::Error(v1_1::TypedError {
                        code: v1_1::ErrorCode::HandlerError as i32,
                        safe_message: "PEER_TEXT_MUST_NOT_ESCAPE".into(),
                    })),
                })),
            },
            nz(8192),
        )
        .unwrap()
        .bytes
    }
}

struct FakeChannel {
    reads: Mutex<VecDeque<Option<Vec<u8>>>>,
    writes: Mutex<Vec<Vec<u8>>>,
    closed: AtomicBool,
    stall: AtomicBool,
}
struct WritePause {
    entered: Barrier,
    release: Barrier,
}
impl WritePause {
    fn new() -> Self {
        Self {
            entered: Barrier::new(2),
            release: Barrier::new(2),
        }
    }
}
impl FakeChannel {
    fn new() -> Self {
        Self {
            reads: Mutex::new(VecDeque::new()),
            writes: Mutex::new(Vec::new()),
            closed: AtomicBool::new(false),
            stall: AtomicBool::new(false),
        }
    }
    fn push(&self, bytes: Vec<u8>) {
        self.reads.lock().unwrap().push_back(Some(bytes));
    }
    fn eof(&self) {
        self.reads.lock().unwrap().push_back(None);
    }
    fn writes(&self) -> Vec<Vec<u8>> {
        self.writes.lock().unwrap().clone()
    }
}
impl OrderedByteChannel for FakeChannel {
    fn read_chunk(
        &self,
        max: NonZeroUsize,
    ) -> BoxFuture<'_, Result<Option<Vec<u8>>, ChannelError>> {
        Box::pin(poll_fn(move |_| {
            if self.closed.load(Ordering::SeqCst) {
                return Poll::Ready(Err(ChannelError::Closed));
            }
            match self.reads.lock().unwrap().pop_front() {
                Some(Some(bytes)) if bytes.len() <= max.get() => Poll::Ready(Ok(Some(bytes))),
                Some(Some(_)) => Poll::Ready(Err(ChannelError::Failed)),
                Some(None) => Poll::Ready(Ok(None)),
                None => Poll::Pending,
            }
        }))
    }
    fn write_frame(&self, frame: EncodedFrame) -> BoxFuture<'_, Result<(), ChannelError>> {
        Box::pin(poll_fn(move |_| {
            if self.closed.load(Ordering::SeqCst) {
                return Poll::Ready(Err(ChannelError::Closed));
            }
            if self.stall.load(Ordering::SeqCst) {
                return Poll::Pending;
            }
            self.writes.lock().unwrap().push(frame.bytes.clone());
            Poll::Ready(Ok(()))
        }))
    }
    fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
    }
}

struct FakeClock {
    now: AtomicU64,
    jump: Mutex<Option<ClockJump>>,
    pause_now: Mutex<Option<(u32, Arc<WritePause>)>>,
}
struct ClockJump {
    skip_reads: u32,
    ingress: u64,
    final_time: u64,
}
impl FakeClock {
    fn new(now: u64) -> Self {
        Self {
            now: AtomicU64::new(now),
            jump: Mutex::new(None),
            pause_now: Mutex::new(None),
        }
    }
    fn set(&self, now: u64) {
        self.now.store(now, Ordering::SeqCst);
        *self.jump.lock().unwrap() = None;
    }
    fn jump_after_reads(&self, skip_reads: u32, ingress: u64, final_time: u64) {
        assert!(final_time >= ingress);
        self.now.store(ingress, Ordering::SeqCst);
        *self.jump.lock().unwrap() = Some(ClockJump {
            skip_reads,
            ingress,
            final_time,
        });
    }
    fn pause_after_reads(&self, skip_reads: u32, pause: Arc<WritePause>) {
        *self.pause_now.lock().unwrap() = Some((skip_reads, pause));
    }
}
impl MonotonicClock for FakeClock {
    fn now_ms(&self) -> u64 {
        let pause = {
            let mut gate = self.pause_now.lock().unwrap();
            match gate.as_mut() {
                Some((remaining, _)) if *remaining > 0 => {
                    *remaining -= 1;
                    None
                }
                Some(_) => gate.take().map(|(_, pause)| pause),
                None => None,
            }
        };
        if let Some(pause) = pause {
            pause.entered.wait();
            pause.release.wait();
        }
        let mut jump = self.jump.lock().unwrap();
        if let Some(armed) = jump.as_mut() {
            if armed.skip_reads == 0 {
                let time = armed.ingress;
                self.now.store(armed.final_time, Ordering::SeqCst);
                *jump = None;
                return time;
            }
            armed.skip_reads -= 1;
        }
        self.now.load(Ordering::SeqCst)
    }
    fn sleep_until(&self, deadline: u64) -> BoxFuture<'_, ()> {
        Box::pin(poll_fn(move |_| {
            if self.now_ms() >= deadline {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }))
    }
}

#[derive(Default)]
struct FakeSpawner {
    tasks: Mutex<Vec<BoxFuture<'static, ()>>>,
    reject: AtomicBool,
    poll_reader_during_next_spawn: AtomicBool,
}
impl TaskSpawner for FakeSpawner {
    fn spawn(&self, future: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
        if self.reject.load(Ordering::SeqCst) {
            return Err(SpawnError::new());
        }
        self.tasks.lock().unwrap().push(future);
        if self
            .poll_reader_during_next_spawn
            .swap(false, Ordering::SeqCst)
        {
            self.tick_at(1);
        }
        Ok(())
    }
}
impl FakeSpawner {
    fn tick(&self) {
        let tasks = std::mem::take(&mut *self.tasks.lock().unwrap());
        for mut task in tasks {
            if poll(task.as_mut()).is_pending() {
                self.tasks.lock().unwrap().push(task);
            }
        }
    }
    fn tick_at(&self, index: usize) {
        let mut task = self.tasks.lock().unwrap().remove(index);
        if poll(task.as_mut()).is_pending() {
            self.tasks.lock().unwrap().insert(index, task);
        }
    }
    fn drive<F: Future>(&self, future: F) -> F::Output {
        let mut future = std::pin::pin!(future);
        for _ in 0..1000 {
            if let Poll::Ready(result) = poll(future.as_mut()) {
                return result;
            }
            self.tick();
        }
        panic!("manual spawner did not settle");
    }
}
fn poll<F: Future + ?Sized>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}

fn host(generation: u64) -> HostAuthority {
    host_minor(generation, 1)
}
fn host_minor(generation: u64, minor: u32) -> HostAuthority {
    let catalog: Value = serde_json::from_str(CATALOG).unwrap();
    let entry = &catalog["entries"][0];
    let descriptor = AgentTool {
        name: "echo".into(),
        label: entry["agentTool"]["label"].as_str().unwrap().into(),
        description: entry["agentTool"]["description"].as_str().unwrap().into(),
        parameters: entry["agentTool"]["parameters"].clone(),
        execution_mode: AgentToolMode::Sequential,
        replay: ToolReplay::Safe,
    };
    let schemas = [0, 1]
        .into_iter()
        .map(|minor| {
            let set = &entry["selected_minor_schema_sets"][format!("1.{minor}")];
            ToolSchemaInputs {
                minor,
                arguments: serde_json::to_vec(&set["arguments"]["schema"]).unwrap(),
                progress: serde_json::to_vec(&set["progress"]["schema"]).unwrap(),
                raw_result: serde_json::to_vec(&set["raw_result"]["schema"]).unwrap(),
            }
        })
        .collect();
    let tool =
        ApprovedTool::new("peer-a".into(), descriptor, schemas, JsonLimits::default()).unwrap();
    let limits = v1_1::ProtocolLimits {
        pre_handshake_frame_bytes: 4096,
        frame_bytes: 8192,
        aggregate_decoded_bytes: 8192,
        value_depth: 32,
        value_elements: 1024,
        string_bytes: 4096,
        bytes_length: 4096,
        in_flight_requests: 8,
        blocking_queue_bytes: 4096,
        progress_queue_bytes: 4096,
        passive_queue_bytes: 4096,
        telemetry_queue_bytes: 4096,
        nesting_depth: 8,
        requests_per_second: 32,
        handshake_ms: 1000,
        cancellation_grace_ms: 100,
        max_stream_credit_bytes: 4096,
    };
    HostAuthority::new(
        "peer-a".into(),
        generation,
        minor,
        minor,
        limits,
        vec![tool],
        vec![],
    )
    .unwrap()
}
fn plan() -> ToolEffectPlan<()> {
    let index = SafeUnsigned::new(0).unwrap();
    ToolEffectPlan {
        key: EffectKey {
            session_id: SessionId::new("session").unwrap(),
            lane: LaneId::new("main").unwrap(),
            operation_id: OperationId::new("op").unwrap(),
            kind: EffectKind::Tool,
            step_or_task_id: EffectStepId::new("step").unwrap(),
            logical_attempt: index,
            item_index: Some(index),
        },
        assistant_entry_id: EntryId::new("assistant-entry").unwrap(),
        source_index: index,
        args_key: "op.tool_args".into(),
        telemetry_context: (),
    }
}
fn setup(
    owner: &SingleToolOwner,
    spawner: &FakeSpawner,
    generation: u64,
    now: u64,
) -> (
    Arc<FakeChannel>,
    Arc<FakeClock>,
    pi_harness_runtime::extensions::tool_driver::ReadyToolRoute,
) {
    let channel = Arc::new(FakeChannel::new());
    let clock = Arc::new(FakeClock::new(now));
    channel.push(hello());
    channel.push(manifest());
    channel.push(ready_frame());
    let route = spawner
        .drive(owner.connect(
            host(generation),
            channel.clone(),
            clock.clone(),
            PumpConfig {
                max_read_chunk: nz(8192),
                write_timeout_ms: NonZeroU64::new(10).unwrap(),
            },
        ))
        .unwrap();
    (channel, clock, route)
}
fn setup_minor(
    owner: &SingleToolOwner,
    spawner: &FakeSpawner,
    generation: u64,
    now: u64,
    minor: u32,
) -> (
    Arc<FakeChannel>,
    Arc<FakeClock>,
    pi_harness_runtime::extensions::tool_driver::ReadyToolRoute,
) {
    let channel = Arc::new(FakeChannel::new());
    let clock = Arc::new(FakeClock::new(now));
    channel.push(hello());
    channel.push(if minor == 0 {
        manifest_1_0()
    } else {
        manifest()
    });
    channel.push(ready_frame());
    let route = spawner
        .drive(owner.connect(
            host_minor(generation, minor),
            channel.clone(),
            clock.clone(),
            PumpConfig {
                max_read_chunk: nz(8192),
                write_timeout_ms: NonZeroU64::new(10).unwrap(),
            },
        ))
        .unwrap();
    (channel, clock, route)
}
fn setup_core_1_1_tool_1_0(
    owner: &SingleToolOwner,
    spawner: &FakeSpawner,
    generation: u64,
    now: u64,
) -> (
    Arc<FakeChannel>,
    Arc<FakeClock>,
    pi_harness_runtime::extensions::tool_driver::ReadyToolRoute,
) {
    let channel = Arc::new(FakeChannel::new());
    let clock = Arc::new(FakeClock::new(now));
    channel.push(hello_core_1_1_tool_1_0());
    channel.push(manifest());
    channel.push(ready_frame());
    let route = spawner
        .drive(owner.connect(
            host_minor(generation, 1),
            channel.clone(),
            clock.clone(),
            PumpConfig {
                max_read_chunk: nz(8192),
                write_timeout_ms: NonZeroU64::new(10).unwrap(),
            },
        ))
        .unwrap();
    assert_eq!(
        route
            .grant()
            .wire()
            .selected_protocol
            .as_ref()
            .unwrap()
            .minor,
        1
    );
    assert_eq!(
        route.grant().tools()[0]
            .reference()
            .profile
            .as_ref()
            .unwrap()
            .minor,
        0
    );
    (channel, clock, route)
}

#[test]
fn route_rejects_ready_grant_without_the_selected_echo() {
    let spawner = Arc::new(FakeSpawner::default());
    let owner = owner(spawner.clone());
    let channel = Arc::new(FakeChannel::new());
    let clock = Arc::new(FakeClock::new(200));
    let mut reader = FrameReader::<v1_1::Envelope>::new(nz(8192));
    let mut envelope = reader
        .read_one(&manifest())
        .unwrap()
        .frame
        .unwrap()
        .envelope;
    let Some(v1_1::envelope::Frame::Manifest(manifest)) = envelope.frame.as_mut() else {
        panic!("manifest fixture absent")
    };
    manifest.capabilities.clear();
    channel.push(hello());
    channel.push(framing::encode(&envelope, nz(8192)).unwrap().bytes);
    channel.push(ready_frame());
    let result = spawner.drive(owner.connect(
        host(7),
        channel,
        clock,
        PumpConfig {
            max_read_chunk: nz(8192),
            write_timeout_ms: NonZeroU64::new(10).unwrap(),
        },
    ));
    assert!(matches!(result, Err(DriverError::InvalidHost)));
}
fn owner(spawner: Arc<FakeSpawner>) -> SingleToolOwner {
    owner_with_write_timeout(spawner, 10)
}
fn owner_with_write_timeout(spawner: Arc<FakeSpawner>, timeout_ms: u64) -> SingleToolOwner {
    SingleToolOwner::new(
        spawner,
        OwnerLimits {
            outbound_bytes: nz(8192),
            receipt_bytes: nz(8192),
            write_timeout_ms: NonZeroU64::new(timeout_ms).unwrap(),
        },
    )
}
fn timing() -> CallTiming {
    CallTiming {
        requested_after_ms: Some(700),
        method_limit_ms: 1000,
        connection_remaining_ms: 500,
        parent_remaining_ms: Some(300),
    }
}
fn saved() -> JsonObject {
    serde_json::from_str(r#"{"text":"ping"}"#).unwrap()
}

#[test]
fn quiet_ready_exact_request_first_terminal_and_owner_slot() {
    let spawner = Arc::new(FakeSpawner::default());
    let owner = owner(spawner.clone());
    let (channel, _, route) = setup(&owner, &spawner, 7, 200);
    assert_eq!(route.grant().wire().connection_generation, 7);
    assert_eq!(channel.writes().len(), 2);
    let handle = route
        .start_planned(&plan(), &saved(), "call-1", timing())
        .unwrap();
    assert!(
        owner
            .retained_binding()
            .unwrap()
            .same_live_identity(handle.binding())
    );
    assert_eq!(
        channel.writes().len(),
        2,
        "request is queued after binding insertion"
    );
    spawner.tick();
    let written = channel.writes();
    assert_eq!(written.len(), 3);
    let mut reader = FrameReader::<v1_1::Envelope>::new(nz(8192));
    let request = reader
        .read_one(&written[2])
        .unwrap()
        .frame
        .unwrap()
        .envelope;
    let Some(v1_1::envelope::Frame::Request(request)) = request.frame else {
        panic!("InvokeTool absent")
    };
    assert_eq!(request.request_id, 1);
    assert_eq!(request.initial_stream_credit_bytes, Some(0));
    assert_eq!(request.effect_key.unwrap().logical_attempt, 0);
    channel.push(raw("terminal", 1));
    let result = spawner.drive(handle.first_terminal()).unwrap();
    let TypedTerminal::Result(value) = result else {
        panic!("typed result absent")
    };
    assert_eq!(value.content.len(), 1);
    channel.eof();
    spawner.tick();
    assert!(
        owner
            .retained_binding()
            .unwrap()
            .same_live_identity(handle.binding())
    );

    let (other_channel, _, other) = setup(&owner, &spawner, 8, 210);
    assert_eq!(
        other
            .start_planned(&plan(), &saved(), "call-1", timing())
            .err(),
        Some(DriverError::Overloaded)
    );
    assert_eq!(other_channel.writes().len(), 2);
    owner
        .retire(
            handle.binding(),
            pi_extension_protocol::tool_live::RetireReason::Committed,
        )
        .unwrap();
    assert!(owner.retained_binding().is_none());
    assert!(
        other
            .start_planned(&plan(), &saved(), "call-1", timing())
            .is_ok()
    );
}

#[test]
fn zero_credit_progress_and_unknown_id_close_without_a_receipt() {
    for (kind, id) in [("progress", 1), ("terminal", 2)] {
        let spawner = Arc::new(FakeSpawner::default());
        let owner = owner(spawner.clone());
        let (channel, _, route) = setup(&owner, &spawner, 7, 200);
        let handle = route
            .start_planned(&plan(), &saved(), "call-1", timing())
            .unwrap();
        spawner.tick();
        channel.push(raw(kind, id));
        let error = spawner.drive(handle.first_terminal()).err().unwrap();
        assert_eq!(error, DriverError::MalformedPeer);
        assert!(channel.closed.load(Ordering::SeqCst));
        assert!(
            owner
                .retained_binding()
                .unwrap()
                .same_live_identity(handle.binding())
        );
        owner.acknowledge_peer_lost(handle.binding()).unwrap();
        assert!(owner.retained_binding().is_none());
    }
}

#[test]
fn duplicate_and_goaway_cannot_replace_a_retained_first_result() {
    for goaway in [1_u8, 4] {
        let spawner = Arc::new(FakeSpawner::default());
        let owner = owner(spawner.clone());
        let (channel, _, route) = setup(&owner, &spawner, 7, 200);
        let handle = route
            .start_planned(&plan(), &saved(), "call-1", timing())
            .unwrap();
        spawner.tick();
        channel.push(raw("terminal", 1));
        assert!(matches!(
            spawner.drive(handle.first_terminal()),
            Ok(TypedTerminal::Result(_))
        ));
        channel.push(frame(12, &[0x08, goaway]));
        spawner.tick();
        channel.push(raw("terminal", 1));
        spawner.tick();
        assert!(matches!(
            spawner.drive(handle.first_terminal()),
            Ok(TypedTerminal::Result(_))
        ));
        assert!(
            owner
                .retained_binding()
                .unwrap()
                .same_live_identity(handle.binding())
        );
        owner
            .retire(
                handle.binding(),
                pi_extension_protocol::tool_live::RetireReason::NoLongerExpected,
            )
            .unwrap();
    }
}

#[test]
fn eof_silent_deadline_and_pending_io_wake_the_exact_waiter() {
    for mode in ["eof", "deadline", "pending_write", "pending_read"] {
        let spawner = Arc::new(FakeSpawner::default());
        let owner = owner(spawner.clone());
        let (channel, clock, route) = setup(&owner, &spawner, 7, 200);
        if mode == "pending_write" {
            channel.stall.store(true, Ordering::SeqCst);
        }
        let handle = route
            .start_planned(&plan(), &saved(), "call-1", timing())
            .unwrap();
        spawner.tick();
        match mode {
            "eof" => channel.eof(),
            "deadline" => clock.set(500),
            "pending_write" | "pending_read" => route.close(),
            _ => unreachable!(),
        }
        let error = spawner.drive(handle.first_terminal()).err().unwrap();
        assert_eq!(error, DriverError::PeerLost, "{mode}");
        assert!(channel.closed.load(Ordering::SeqCst));
        owner.acknowledge_peer_lost(handle.binding()).unwrap();
    }
}

#[test]
fn valid_ungranted_request_gets_ordered_fixed_denial_before_close() {
    let spawner = Arc::new(FakeSpawner::default());
    let owner = owner(spawner.clone());
    let (channel, _, route) = setup(&owner, &spawner, 7, 200);
    channel.push(selected_peer_request(&route, 17, "tool.invoke"));
    for _ in 0..5 {
        spawner.tick();
    }
    let writes = channel.writes();
    assert_eq!(writes.len(), 3);
    let mut reader = FrameReader::<v1_1::Envelope>::new(nz(8192));
    let response = reader.read_one(&writes[2]).unwrap().frame.unwrap().envelope;
    let Some(v1_1::envelope::Frame::Response(response)) = response.frame else {
        panic!("missing denial")
    };
    assert_eq!(response.request_id, 17);
    let Some(v1_1::response::Terminal::Error(error)) = response.terminal else {
        panic!("missing error")
    };
    assert_eq!(error.code, v1_1::ErrorCode::Forbidden as i32);
    assert!(error.safe_message.is_empty());
    assert!(channel.closed.load(Ordering::SeqCst));
}

#[test]
fn unknown_method_gets_fixed_denial_but_malformed_capability_gets_none() {
    for malformed in [false, true] {
        let spawner = Arc::new(FakeSpawner::default());
        let owner = owner(spawner.clone());
        let (channel, _, route) = setup(&owner, &spawner, 7, 200);
        let mut reader = FrameReader::<v1_1::Envelope>::new(nz(8192));
        let mut envelope = reader
            .read_one(&selected_peer_request(&route, 17, "tool.invoke"))
            .unwrap()
            .frame
            .unwrap()
            .envelope;
        let Some(v1_1::envelope::Frame::Request(request)) = envelope.frame.as_mut() else {
            panic!("request fixture absent")
        };
        if malformed {
            request
                .capability_ref
                .as_mut()
                .unwrap()
                .schema_digest_sha256
                .clear();
        } else {
            request.method = "unknown.action".into();
        }
        channel.push(framing::encode(&envelope, nz(8192)).unwrap().bytes);
        for _ in 0..5 {
            spawner.tick();
        }
        let writes = channel.writes();
        assert_eq!(writes.len(), if malformed { 2 } else { 3 });
        if !malformed {
            let mut reader = FrameReader::<v1_1::Envelope>::new(nz(8192));
            let response = reader.read_one(&writes[2]).unwrap().frame.unwrap().envelope;
            let Some(v1_1::envelope::Frame::Response(response)) = response.frame else {
                panic!("missing denial")
            };
            let Some(v1_1::response::Terminal::Error(error)) = response.terminal else {
                panic!("missing fixed error")
            };
            assert_eq!(error.code, v1_1::ErrorCode::UnsupportedMethod as i32);
            assert!(error.safe_message.is_empty());
        }
        assert!(channel.closed.load(Ordering::SeqCst));
    }
}

#[test]
fn route_replacement_fences_a_stale_claim_before_any_write() {
    let spawner = Arc::new(FakeSpawner::default());
    let owner = owner(spawner.clone());
    let (channel, _, route) = setup(&owner, &spawner, 7, 200);
    route.stop_new_calls();
    assert_eq!(
        route
            .start_planned(&plan(), &saved(), "call-1", timing())
            .err(),
        Some(DriverError::Closed)
    );
    spawner.tick();
    assert_eq!(channel.writes().len(), 2);
    assert!(owner.retained_binding().is_none());
}

#[test]
fn fixed_peer_error_terminal_survives_channel_loss_without_peer_text() {
    let spawner = Arc::new(FakeSpawner::default());
    let owner = owner(spawner.clone());
    let (channel, _, route) = setup(&owner, &spawner, 7, 200);
    let handle = route
        .start_planned(&plan(), &saved(), "call-1", timing())
        .unwrap();
    spawner.tick();
    let envelope = v1_1::Envelope {
        frame: Some(v1_1::envelope::Frame::StreamEnd(v1_1::StreamEnd {
            request_id: 1,
            terminal: Some(v1_1::stream_end::Terminal::Error(v1_1::TypedError {
                code: v1_1::ErrorCode::HandlerError as i32,
                safe_message: "SECRET_SENTINEL".into(),
            })),
        })),
    };
    channel.push(framing::encode(&envelope, nz(8192)).unwrap().bytes);
    assert!(matches!(
        spawner.drive(handle.first_terminal()),
        Ok(TypedTerminal::PeerError(v1_1::ErrorCode::HandlerError))
    ));
    channel.eof();
    spawner.tick();
    assert!(matches!(
        spawner.drive(handle.first_terminal()),
        Ok(TypedTerminal::PeerError(v1_1::ErrorCode::HandlerError))
    ));
    assert!(
        owner
            .retained_binding()
            .unwrap()
            .same_live_identity(handle.binding())
    );
    assert!(
        !DriverError::MalformedPeer
            .to_string()
            .contains("SECRET_SENTINEL")
    );
}

#[test]
fn monotonic_write_timeout_closes_and_wakes_a_pending_call() {
    let spawner = Arc::new(FakeSpawner::default());
    let owner = owner(spawner.clone());
    let (channel, clock, route) = setup(&owner, &spawner, 7, 200);
    channel.stall.store(true, Ordering::SeqCst);
    let handle = route
        .start_planned(&plan(), &saved(), "call-1", timing())
        .unwrap();
    spawner.tick();
    assert_eq!(channel.writes().len(), 2);
    clock.set(210);
    assert_eq!(
        spawner.drive(handle.first_terminal()).err(),
        Some(DriverError::PeerLost)
    );
    assert!(channel.closed.load(Ordering::SeqCst));
    owner.acknowledge_peer_lost(handle.binding()).unwrap();
}

#[test]
fn wrong_direction_and_unknown_goaway_close_without_a_result() {
    for bytes in [
        framing::encode(
            &v1_1::Envelope {
                frame: Some(v1_1::envelope::Frame::Credit(v1_1::Credit {
                    request_id: 1,
                    additional_bytes: 1,
                })),
            },
            nz(8192),
        )
        .unwrap()
        .bytes,
        frame(12, &[0x08, 0x7f]),
    ] {
        let spawner = Arc::new(FakeSpawner::default());
        let owner = owner(spawner.clone());
        let (channel, _, route) = setup(&owner, &spawner, 7, 200);
        let handle = route
            .start_planned(&plan(), &saved(), "call-1", timing())
            .unwrap();
        spawner.tick();
        channel.push(bytes);
        assert_eq!(
            spawner.drive(handle.first_terminal()).err(),
            Some(DriverError::MalformedPeer)
        );
        assert!(channel.closed.load(Ordering::SeqCst));
        owner.acknowledge_peer_lost(handle.binding()).unwrap();
    }
}

#[test]
fn drain_stops_admission_but_preserves_the_claimed_first_terminal() {
    let spawner = Arc::new(FakeSpawner::default());
    let owner = owner(spawner.clone());
    let (channel, _, route) = setup(&owner, &spawner, 7, 200);
    let handle = route
        .start_planned(&plan(), &saved(), "call-1", timing())
        .unwrap();
    spawner.tick();
    channel.push(frame(12, &[0x08, 0x01]));
    spawner.tick();
    assert!(!channel.closed.load(Ordering::SeqCst));
    assert_eq!(
        route
            .start_planned(&plan(), &saved(), "call-2", timing())
            .err(),
        Some(DriverError::Closed)
    );
    channel.push(raw("terminal", 1));
    assert!(matches!(
        spawner.drive(handle.first_terminal()),
        Ok(TypedTerminal::Result(_))
    ));
    assert!(channel.closed.load(Ordering::SeqCst));
    assert!(owner.retained_binding().is_some());
}

#[test]
fn replacing_an_idle_route_closes_the_old_channel() {
    let spawner = Arc::new(FakeSpawner::default());
    let owner = owner(spawner.clone());
    let (old_channel, _, old_route) = setup(&owner, &spawner, 7, 200);
    let (_new_channel, _, new_route) = setup(&owner, &spawner, 8, 210);
    assert!(old_channel.closed.load(Ordering::SeqCst));
    assert_eq!(
        old_route
            .start_planned(&plan(), &saved(), "call-1", timing())
            .err(),
        Some(DriverError::Closed)
    );
    assert!(
        new_route
            .start_planned(&plan(), &saved(), "call-1", timing())
            .is_ok()
    );
}

#[test]
fn rejected_writer_spawn_closes_the_new_and_idle_old_route() {
    let spawner = Arc::new(FakeSpawner::default());
    let owner = owner(spawner.clone());
    let (old_channel, _, old_route) = setup(&owner, &spawner, 7, 200);
    spawner.reject.store(true, Ordering::SeqCst);
    let new_channel = Arc::new(FakeChannel::new());
    let new_clock = Arc::new(FakeClock::new(210));
    new_channel.push(hello());
    new_channel.push(manifest());
    new_channel.push(ready_frame());
    assert!(matches!(
        spawner.drive(owner.connect(
            host(8),
            new_channel.clone(),
            new_clock,
            PumpConfig {
                max_read_chunk: nz(8192),
                write_timeout_ms: NonZeroU64::new(10).unwrap(),
            },
        )),
        Err(DriverError::HostFault)
    ));
    assert!(old_channel.closed.load(Ordering::SeqCst));
    assert!(new_channel.closed.load(Ordering::SeqCst));
    assert!(owner.retained_binding().is_none());
    assert_eq!(
        old_route
            .start_planned(&plan(), &saved(), "call-1", timing())
            .err(),
        Some(DriverError::Closed)
    );
    spawner.reject.store(false, Ordering::SeqCst);
    let (_recovered_channel, _, recovered) = setup(&owner, &spawner, 9, 220);
    assert!(
        recovered
            .start_planned(&plan(), &saved(), "call-1", timing())
            .is_ok()
    );
}

#[test]
fn early_terminal_waits_for_full_request_write_then_becomes_first_result() {
    let spawner = Arc::new(FakeSpawner::default());
    let owner = owner(spawner.clone());
    let (channel, _, route) = setup(&owner, &spawner, 7, 200);
    channel.stall.store(true, Ordering::SeqCst);
    let handle = route
        .start_planned(&plan(), &saved(), "call-1", timing())
        .unwrap();
    spawner.tick();
    channel.push(raw("terminal", 1));
    spawner.tick();
    assert_eq!(channel.writes().len(), 2);
    let mut first = Box::pin(handle.first_terminal());
    assert!(
        poll(first.as_mut()).is_pending(),
        "unwritten request is provisional"
    );
    channel.stall.store(false, Ordering::SeqCst);
    assert!(matches!(spawner.drive(first), Ok(TypedTerminal::Result(_))));
    assert_eq!(channel.writes().len(), 3);
    assert!(owner.retained_binding().is_some());
}

#[test]
fn early_terminal_cannot_be_retired_after_write_timeout_or_deadline() {
    for deadline in [false, true] {
        let spawner = Arc::new(FakeSpawner::default());
        let owner = owner_with_write_timeout(spawner.clone(), if deadline { 400 } else { 10 });
        let (channel, clock, route) = setup(&owner, &spawner, 7, 200);
        channel.stall.store(true, Ordering::SeqCst);
        let handle = route
            .start_planned(&plan(), &saved(), "call-1", timing())
            .unwrap();
        spawner.tick();
        channel.push(raw("terminal", 1));
        spawner.tick();
        let mut first = Box::pin(handle.first_terminal());
        assert!(poll(first.as_mut()).is_pending(), "early frame must wait");
        if deadline {
            channel.stall.store(false, Ordering::SeqCst);
            clock.set(500);
        } else {
            clock.set(210);
        }
        assert_eq!(spawner.drive(first).err(), Some(DriverError::PeerLost));
        assert!(channel.closed.load(Ordering::SeqCst));
        assert_eq!(channel.writes().len(), if deadline { 3 } else { 2 });
        assert_eq!(
            owner
                .retire(
                    handle.binding(),
                    pi_extension_protocol::tool_live::RetireReason::Committed
                )
                .err(),
            Some(DriverError::WrongBinding)
        );
        owner.acknowledge_peer_lost(handle.binding()).unwrap();
    }
}

#[test]
fn invalid_populated_profile_gets_no_denial_in_both_selected_minors() {
    let mut unexpected_denials = Vec::new();
    for minor in [0, 1] {
        for invalid_name in [false, true] {
            let spawner = Arc::new(FakeSpawner::default());
            let owner = owner(spawner.clone());
            let (channel, _, route) = setup_minor(&owner, &spawner, 7, 200, minor);
            let bytes = if minor == 0 {
                let mut reader = FrameReader::<v1_0::Envelope>::new(nz(8192));
                let mut envelope = reader
                    .read_one(&selected_peer_request(&route, 17, "tool.invoke"))
                    .unwrap()
                    .frame
                    .unwrap()
                    .envelope;
                let Some(v1_0::envelope::Frame::Request(request)) = envelope.frame.as_mut() else {
                    panic!("request fixture absent")
                };
                let profile = request
                    .capability_ref
                    .as_mut()
                    .unwrap()
                    .profile
                    .as_mut()
                    .unwrap();
                if invalid_name {
                    profile.name.clear()
                } else {
                    profile.major = 0
                }
                framing::encode(&envelope, nz(8192)).unwrap().bytes
            } else {
                let mut reader = FrameReader::<v1_1::Envelope>::new(nz(8192));
                let mut envelope = reader
                    .read_one(&selected_peer_request(&route, 17, "tool.invoke"))
                    .unwrap()
                    .frame
                    .unwrap()
                    .envelope;
                let Some(v1_1::envelope::Frame::Request(request)) = envelope.frame.as_mut() else {
                    panic!("request fixture absent")
                };
                let profile = request
                    .capability_ref
                    .as_mut()
                    .unwrap()
                    .profile
                    .as_mut()
                    .unwrap();
                if invalid_name {
                    profile.name.clear()
                } else {
                    profile.major = 0
                }
                framing::encode(&envelope, nz(8192)).unwrap().bytes
            };
            channel.push(bytes);
            for _ in 0..5 {
                spawner.tick();
            }
            if channel.writes().len() != 2 {
                unexpected_denials.push((minor, invalid_name, channel.writes().len()));
            }
            assert!(channel.closed.load(Ordering::SeqCst));
        }
    }
    assert!(unexpected_denials.is_empty(), "{unexpected_denials:?}");
}

#[test]
fn drain_while_request_write_is_pending_keeps_one_provisional_terminal() {
    let spawner = Arc::new(FakeSpawner::default());
    let owner = owner(spawner.clone());
    let (channel, _, route) = setup(&owner, &spawner, 7, 200);
    channel.stall.store(true, Ordering::SeqCst);
    let handle = route
        .start_planned(&plan(), &saved(), "call-1", timing())
        .unwrap();
    spawner.tick();
    channel.push(frame(12, &[0x08, 0x01]));
    spawner.tick();
    channel.push(raw("terminal", 1));
    spawner.tick();
    assert!(!channel.closed.load(Ordering::SeqCst));
    let mut first = Box::pin(handle.first_terminal());
    assert!(poll(first.as_mut()).is_pending());
    channel.stall.store(false, Ordering::SeqCst);
    assert!(matches!(spawner.drive(first), Ok(TypedTerminal::Result(_))));
    assert!(channel.closed.load(Ordering::SeqCst));
    assert!(owner.retained_binding().is_some());
}

#[test]
fn terminal_arriving_at_deadline_cannot_overtake_timer_or_be_retired() {
    let spawner = Arc::new(FakeSpawner::default());
    let owner = owner(spawner.clone());
    let (channel, clock, route) = setup(&owner, &spawner, 7, 200);
    let handle = route
        .start_planned(&plan(), &saved(), "call-1", timing())
        .unwrap();
    spawner.tick();
    clock.set(500);
    channel.push(raw("terminal", 1));
    assert_eq!(
        spawner.drive(handle.first_terminal()).err(),
        Some(DriverError::PeerLost)
    );
    assert!(channel.closed.load(Ordering::SeqCst));
    assert_eq!(
        owner
            .retire(
                handle.binding(),
                pi_extension_protocol::tool_live::RetireReason::Committed
            )
            .err(),
        Some(DriverError::WrongBinding)
    );
    owner.acknowledge_peer_lost(handle.binding()).unwrap();
}

#[test]
fn provisional_input_is_bounded_to_one_selected_frame() {
    let spawner = Arc::new(FakeSpawner::default());
    let owner = owner(spawner.clone());
    let (channel, _, route) = setup(&owner, &spawner, 7, 200);
    channel.stall.store(true, Ordering::SeqCst);
    let handle = route
        .start_planned(&plan(), &saved(), "call-1", timing())
        .unwrap();
    spawner.tick();
    channel.push(raw("terminal", 1));
    channel.push(raw("terminal", 1));
    assert_eq!(
        spawner.drive(handle.first_terminal()).err(),
        Some(DriverError::MalformedPeer)
    );
    assert_eq!(channel.writes().len(), 2);
    assert!(channel.closed.load(Ordering::SeqCst));
    assert_eq!(
        owner
            .retire(
                handle.binding(),
                pi_extension_protocol::tool_live::RetireReason::Committed
            )
            .err(),
        Some(DriverError::WrongBinding)
    );
    owner.acknowledge_peer_lost(handle.binding()).unwrap();
}

#[test]
fn provisional_terminal_does_not_skip_an_ordered_peer_request_denial() {
    let spawner = Arc::new(FakeSpawner::default());
    let owner = owner(spawner.clone());
    let (channel, _, route) = setup(&owner, &spawner, 7, 200);
    channel.stall.store(true, Ordering::SeqCst);
    let handle = route
        .start_planned(&plan(), &saved(), "call-1", timing())
        .unwrap();
    spawner.tick();
    channel.push(raw("terminal", 1));
    channel.push(selected_peer_request(&route, 17, "tool.invoke"));
    spawner.tick();
    assert_eq!(channel.writes().len(), 2);
    channel.stall.store(false, Ordering::SeqCst);
    assert!(matches!(
        spawner.drive(handle.first_terminal()),
        Ok(TypedTerminal::Result(_))
    ));
    for _ in 0..3 {
        spawner.tick();
    }
    let writes = channel.writes();
    assert_eq!(
        writes.len(),
        4,
        "request and denial must both finish in FIFO order"
    );
    let mut reader = FrameReader::<v1_1::Envelope>::new(nz(8192));
    let response = reader.read_one(&writes[3]).unwrap().frame.unwrap().envelope;
    let Some(v1_1::envelope::Frame::Response(response)) = response.frame else {
        panic!("reserved denial absent")
    };
    assert_eq!(response.request_id, 17);
    let Some(v1_1::response::Terminal::Error(error)) = response.terminal else {
        panic!("fixed denial absent")
    };
    assert_eq!(error.code, v1_1::ErrorCode::Forbidden as i32);
    assert!(error.safe_message.is_empty());
    assert!(channel.closed.load(Ordering::SeqCst));
    assert!(owner.retained_binding().is_some());
}

#[test]
fn final_clock_cutoff_rejects_direct_result_and_peer_error_in_both_minors() {
    for minor in [0, 1] {
        for peer_error in [false, true] {
            let spawner = Arc::new(FakeSpawner::default());
            let owner = owner(spawner.clone());
            let (channel, clock, route) = setup_minor(&owner, &spawner, 7, 200, minor);
            let handle = route
                .start_planned(&plan(), &saved(), "call-1", timing())
                .unwrap();
            spawner.tick();
            assert_eq!(channel.writes().len(), 3);
            // The pump samples once; ingress then returns 499 while moving
            // the monotonic clock to the effective deadline 500.
            clock.jump_after_reads(1, 499, 500);
            channel.push(selected_terminal(minor, peer_error));
            assert_eq!(
                spawner.drive(handle.first_terminal()).err(),
                Some(DriverError::PeerLost),
                "minor={minor} peer_error={peer_error}"
            );
            assert!(channel.closed.load(Ordering::SeqCst));
            assert_eq!(
                owner
                    .retire(
                        handle.binding(),
                        pi_extension_protocol::tool_live::RetireReason::Committed
                    )
                    .err(),
                Some(DriverError::WrongBinding)
            );
            owner.acknowledge_peer_lost(handle.binding()).unwrap();
        }
    }
}

#[test]
fn final_clock_cutoff_rejects_provisional_result_and_peer_error_in_both_minors() {
    for minor in [0, 1] {
        for peer_error in [false, true] {
            let spawner = Arc::new(FakeSpawner::default());
            let owner = owner_with_write_timeout(spawner.clone(), 400);
            let (channel, clock, route) = setup_minor(&owner, &spawner, 7, 200, minor);
            channel.stall.store(true, Ordering::SeqCst);
            let handle = route
                .start_planned(&plan(), &saved(), "call-1", timing())
                .unwrap();
            spawner.tick();
            channel.push(selected_terminal(minor, peer_error));
            spawner.tick();
            let mut first = Box::pin(handle.first_terminal());
            assert!(poll(first.as_mut()).is_pending());
            // Writer timeout poll and full-write check sample 499; the
            // confirmation ingress read moves to 500 before final admission.
            clock.jump_after_reads(2, 499, 500);
            channel.stall.store(false, Ordering::SeqCst);
            assert_eq!(
                spawner.drive(first).err(),
                Some(DriverError::PeerLost),
                "minor={minor} peer_error={peer_error}"
            );
            assert_eq!(channel.writes().len(), 3);
            assert!(channel.closed.load(Ordering::SeqCst));
            assert_eq!(
                owner
                    .retire(
                        handle.binding(),
                        pi_extension_protocol::tool_live::RetireReason::Committed
                    )
                    .err(),
                Some(DriverError::WrongBinding)
            );
            owner.acknowledge_peer_lost(handle.binding()).unwrap();
        }
    }
}

#[test]
fn final_clock_499_terminal_survives_later_timer_in_direct_and_provisional_paths() {
    for provisional in [false, true] {
        for minor in [0, 1] {
            for peer_error in [false, true] {
                let spawner = Arc::new(FakeSpawner::default());
                let owner = owner_with_write_timeout(spawner.clone(), 400);
                let (channel, clock, route) = setup_minor(&owner, &spawner, 7, 200, minor);
                channel.stall.store(provisional, Ordering::SeqCst);
                let handle = route
                    .start_planned(&plan(), &saved(), "call-1", timing())
                    .unwrap();
                spawner.tick();
                channel.push(selected_terminal(minor, peer_error));
                if provisional {
                    spawner.tick();
                    channel.stall.store(false, Ordering::SeqCst);
                }
                clock.set(499);
                let terminal = spawner.drive(handle.first_terminal()).unwrap();
                assert_eq!(matches!(terminal, TypedTerminal::PeerError(_)), peer_error);
                clock.set(500);
                spawner.tick();
                assert!(owner.retained_binding().is_some());
                owner
                    .retire(
                        handle.binding(),
                        pi_extension_protocol::tool_live::RetireReason::Committed,
                    )
                    .unwrap();
            }
        }
    }
}

#[test]
fn final_expiry_keeps_one_previously_reserved_fifo_denial() {
    let spawner = Arc::new(FakeSpawner::default());
    let owner = owner_with_write_timeout(spawner.clone(), 400);
    let (channel, clock, route) = setup(&owner, &spawner, 7, 200);
    channel.stall.store(true, Ordering::SeqCst);
    let handle = route
        .start_planned(&plan(), &saved(), "call-1", timing())
        .unwrap();
    spawner.tick();
    channel.push(raw("terminal", 1));
    channel.push(selected_peer_request(&route, 17, "tool.invoke"));
    spawner.tick();
    clock.jump_after_reads(2, 499, 500);
    channel.stall.store(false, Ordering::SeqCst);
    assert_eq!(
        spawner.drive(handle.first_terminal()).err(),
        Some(DriverError::PeerLost)
    );
    for _ in 0..3 {
        spawner.tick();
    }
    let writes = channel.writes();
    assert_eq!(
        writes.len(),
        4,
        "one FIFO denial follows the completed request"
    );
    let mut reader = FrameReader::<v1_1::Envelope>::new(nz(8192));
    let response = reader.read_one(&writes[3]).unwrap().frame.unwrap().envelope;
    assert!(matches!(
        response.frame,
        Some(v1_1::envelope::Frame::Response(_))
    ));
    assert!(channel.closed.load(Ordering::SeqCst));
    assert_eq!(
        owner
            .retire(
                handle.binding(),
                pi_extension_protocol::tool_live::RetireReason::Committed
            )
            .err(),
        Some(DriverError::WrongBinding)
    );
    owner.acknowledge_peer_lost(handle.binding()).unwrap();
}

#[test]
fn timer_first_loss_wakes_call_then_started_write_finishes_before_fifo_denial() {
    let spawner = Arc::new(FakeSpawner::default());
    let owner = owner_with_write_timeout(spawner.clone(), 400);
    let (channel, clock, route) = setup(&owner, &spawner, 7, 200);
    channel.stall.store(true, Ordering::SeqCst);
    let handle = route
        .start_planned(&plan(), &saved(), "call-1", timing())
        .unwrap();
    spawner.tick();
    channel.push(selected_peer_request(&route, 17, "tool.invoke"));
    spawner.tick();
    assert_eq!(channel.writes().len(), 2);
    clock.set(500);
    assert_eq!(
        spawner.drive(handle.first_terminal()).err(),
        Some(DriverError::PeerLost)
    );
    assert!(!channel.closed.load(Ordering::SeqCst));
    owner.acknowledge_peer_lost(handle.binding()).unwrap();
    channel.stall.store(false, Ordering::SeqCst);
    for _ in 0..3 {
        spawner.tick();
    }
    assert_eq!(
        channel.writes().len(),
        4,
        "started Request then fixed denial"
    );
    let mut reader = FrameReader::<v1_1::Envelope>::new(nz(8192));
    let response = reader
        .read_one(&channel.writes()[3])
        .unwrap()
        .frame
        .unwrap()
        .envelope;
    let Some(v1_1::envelope::Frame::Response(response)) = response.frame else {
        panic!("denial missing")
    };
    assert_eq!(response.request_id, 17);
    assert!(
        matches!(response.terminal, Some(v1_1::response::Terminal::Error(error))
        if error.code == v1_1::ErrorCode::Forbidden as i32 && error.safe_message.is_empty())
    );
    assert!(channel.closed.load(Ordering::SeqCst));
}

#[test]
fn claimed_before_timer_is_irrevocable_even_before_first_write() {
    let spawner = Arc::new(FakeSpawner::default());
    let owner = owner_with_write_timeout(spawner.clone(), 400);
    let (channel, clock, route) = setup(&owner, &spawner, 7, 200);
    let handle = route
        .start_planned(&plan(), &saved(), "call-1", timing())
        .unwrap();
    channel.push(selected_peer_request(&route, 17, "tool.invoke"));
    spawner.tick_at(1); // reserve the denial before the ordered writer runs
    let pause = Arc::new(WritePause::new());
    clock.pause_after_reads(1, pause.clone());
    std::thread::scope(|scope| {
        let writer_spawner = spawner.clone();
        let writer = scope.spawn(move || writer_spawner.tick_at(0));
        pause.entered.wait(); // claim completed; writer paused at its next clock sample
        assert_eq!(channel.writes().len(), 2);
        clock.set(500);
        assert_eq!(
            spawner.drive(handle.first_terminal()).err(),
            Some(DriverError::PeerLost)
        );
        assert_eq!(channel.writes().len(), 2, "timer must wake before a write");
        assert!(!channel.closed.load(Ordering::SeqCst));
        pause.release.wait();
        writer.join().unwrap();
    });
    for _ in 0..3 {
        spawner.tick();
    }
    assert_eq!(
        channel.writes().len(),
        4,
        "admitted InvokeTool precedes denial"
    );
    assert_eq!(
        denial_code(&channel.writes()[3], 1),
        v1_1::ErrorCode::Forbidden as i32
    );
    assert!(channel.closed.load(Ordering::SeqCst));
    owner.acknowledge_peer_lost(handle.binding()).unwrap();
}

#[test]
fn timer_first_loss_keeps_started_write_deadline_and_drops_denial_on_timeout() {
    let spawner = Arc::new(FakeSpawner::default());
    let owner = owner_with_write_timeout(spawner.clone(), 400);
    let (channel, clock, route) = setup(&owner, &spawner, 7, 200);
    channel.stall.store(true, Ordering::SeqCst);
    let handle = route
        .start_planned(&plan(), &saved(), "call-1", timing())
        .unwrap();
    spawner.tick();
    channel.push(selected_peer_request(&route, 17, "tool.invoke"));
    spawner.tick();
    clock.set(500);
    assert_eq!(
        spawner.drive(handle.first_terminal()).err(),
        Some(DriverError::PeerLost)
    );
    assert!(!channel.closed.load(Ordering::SeqCst));
    clock.set(600);
    spawner.tick();
    assert!(channel.closed.load(Ordering::SeqCst));
    assert_eq!(channel.writes().len(), 2);
}

#[test]
fn timer_first_loss_removes_unstarted_request_and_sends_only_denial() {
    let spawner = Arc::new(FakeSpawner::default());
    let owner = owner_with_write_timeout(spawner.clone(), 400);
    let (channel, clock, route) = setup(&owner, &spawner, 7, 200);
    let handle = route
        .start_planned(&plan(), &saved(), "call-1", timing())
        .unwrap();
    channel.push(selected_peer_request(&route, 17, "tool.invoke"));
    spawner.tick_at(1); // reader reserves denial before writer claims the queued request
    clock.set(500);
    spawner.tick_at(2); // invocation timer wins the owner line
    assert_eq!(
        spawner.drive(handle.first_terminal()).err(),
        Some(DriverError::PeerLost)
    );
    for _ in 0..3 {
        spawner.tick();
    }
    assert_eq!(
        channel.writes().len(),
        3,
        "expired Request must not be written"
    );
    let mut reader = FrameReader::<v1_1::Envelope>::new(nz(8192));
    let response = reader
        .read_one(&channel.writes()[2])
        .unwrap()
        .frame
        .unwrap()
        .envelope;
    assert!(matches!(
        response.frame,
        Some(v1_1::envelope::Frame::Response(_))
    ));
    assert!(channel.closed.load(Ordering::SeqCst));
}

#[test]
fn timer_first_loss_never_starts_denial_after_its_original_control_deadline() {
    let spawner = Arc::new(FakeSpawner::default());
    let owner = owner_with_write_timeout(spawner.clone(), 400);
    let (channel, clock, route) = setup(&owner, &spawner, 7, 200);
    channel.stall.store(true, Ordering::SeqCst);
    let handle = route
        .start_planned(&plan(), &saved(), "call-1", timing())
        .unwrap();
    spawner.tick();
    channel.push(selected_peer_request(&route, 17, "tool.invoke"));
    spawner.tick();
    clock.set(500);
    assert_eq!(
        spawner.drive(handle.first_terminal()).err(),
        Some(DriverError::PeerLost)
    );
    assert!(!channel.closed.load(Ordering::SeqCst));
    // The preceding full-frame write completes at 599, then the control
    // deadline becomes 600 before the FIFO denial reaches the writer.
    clock.jump_after_reads(1, 599, 600);
    channel.stall.store(false, Ordering::SeqCst);
    spawner.tick();
    assert_eq!(
        channel.writes().len(),
        3,
        "only the preceding Request may finish"
    );
    assert!(channel.closed.load(Ordering::SeqCst));
}

#[test]
fn retained_terminal_survives_deadline_while_its_denial_finishes() {
    let spawner = Arc::new(FakeSpawner::default());
    let owner = owner_with_write_timeout(spawner.clone(), 400);
    let (channel, clock, route) = setup(&owner, &spawner, 7, 200);
    let handle = route
        .start_planned(&plan(), &saved(), "call-1", timing())
        .unwrap();
    spawner.tick();
    clock.set(499);
    channel.push(raw("terminal", 1));
    assert!(matches!(
        spawner.drive(handle.first_terminal()),
        Ok(TypedTerminal::Result(_))
    ));
    channel.stall.store(true, Ordering::SeqCst);
    channel.push(selected_peer_request(&route, 17, "tool.invoke"));
    spawner.tick();
    clock.set(500);
    spawner.tick();
    assert!(matches!(
        spawner.drive(handle.first_terminal()),
        Ok(TypedTerminal::Result(_))
    ));
    assert!(!channel.closed.load(Ordering::SeqCst));
    channel.stall.store(false, Ordering::SeqCst);
    spawner.tick();
    assert_eq!(channel.writes().len(), 4);
    owner
        .retire(
            handle.binding(),
            pi_extension_protocol::tool_live::RetireReason::Committed,
        )
        .unwrap();
}

#[test]
fn immediate_close_events_preempt_a_reserved_denial() {
    for reason in [
        Some(v1_1::GoAwayReason::GoAwayClose),
        Some(v1_1::GoAwayReason::GoAwayMalformed),
        Some(v1_1::GoAwayReason::GoAwayLimit),
        None,
    ] {
        let spawner = Arc::new(FakeSpawner::default());
        let owner = owner_with_write_timeout(spawner.clone(), 400);
        let (channel, _, route) = setup(&owner, &spawner, 7, 200);
        channel.stall.store(true, Ordering::SeqCst);
        let handle = route
            .start_planned(&plan(), &saved(), "call-1", timing())
            .unwrap();
        spawner.tick();
        channel.push(selected_peer_request(&route, 17, "tool.invoke"));
        spawner.tick();
        assert!(!channel.closed.load(Ordering::SeqCst));
        if let Some(reason) = reason {
            channel.push(
                framing::encode(
                    &v1_1::Envelope {
                        frame: Some(v1_1::envelope::Frame::GoAway(v1_1::GoAway {
                            reason: reason as i32,
                            retryable: false,
                        })),
                    },
                    nz(8192),
                )
                .unwrap()
                .bytes,
            );
        } else {
            channel.eof();
        }
        spawner.tick();
        assert!(channel.closed.load(Ordering::SeqCst), "{reason:?}");
        assert_eq!(channel.writes().len(), 2, "{reason:?}");
        assert_eq!(
            spawner.drive(handle.first_terminal()).err(),
            Some(DriverError::PeerLost)
        );
    }
}

#[test]
fn malformed_generation_and_invalid_goaway_preempt_a_reserved_denial() {
    for invalid_goaway in [false, true] {
        let spawner = Arc::new(FakeSpawner::default());
        let owner = owner_with_write_timeout(spawner.clone(), 400);
        let (channel, _, route) = setup(&owner, &spawner, 7, 200);
        channel.stall.store(true, Ordering::SeqCst);
        let handle = route
            .start_planned(&plan(), &saved(), "call-1", timing())
            .unwrap();
        spawner.tick();
        channel.push(selected_peer_request(&route, 17, "tool.invoke"));
        spawner.tick();
        if invalid_goaway {
            channel.push(
                framing::encode(
                    &v1_1::Envelope {
                        frame: Some(v1_1::envelope::Frame::GoAway(v1_1::GoAway {
                            reason: 999,
                            retryable: false,
                        })),
                    },
                    nz(8192),
                )
                .unwrap()
                .bytes,
            );
        } else {
            channel.push(vec![1, 0xff]); // malformed selected Envelope, not a second Request
        }
        spawner.tick();
        assert!(channel.closed.load(Ordering::SeqCst));
        assert_eq!(channel.writes().len(), 2);
        assert!(matches!(
            spawner.drive(handle.first_terminal()).err(),
            Some(DriverError::MalformedPeer | DriverError::PeerLost)
        ));
    }
}

#[test]
fn second_coalesced_request_is_ignored_while_first_denial_remains_reserved() {
    for second_id in [18, 0] {
        let spawner = Arc::new(FakeSpawner::default());
        let owner = owner_with_write_timeout(spawner.clone(), 400);
        let (channel, _, route) = setup(&owner, &spawner, 7, 200);
        channel.stall.store(true, Ordering::SeqCst);
        let _handle = route
            .start_planned(&plan(), &saved(), "call-1", timing())
            .unwrap();
        spawner.tick();
        channel.push(
            [
                selected_peer_request(&route, 17, "tool.invoke"),
                selected_peer_request(&route, second_id, "tool.invoke"),
            ]
            .concat(),
        );
        spawner.tick();
        assert!(
            !channel.closed.load(Ordering::SeqCst),
            "second_id={second_id}"
        );
        channel.stall.store(false, Ordering::SeqCst);
        spawner.tick();
        assert_eq!(
            channel.writes().len(),
            4,
            "one outbound Request and one denial"
        );
        let mut reader = FrameReader::<v1_1::Envelope>::new(nz(8192));
        let response = reader
            .read_one(&channel.writes()[3])
            .unwrap()
            .frame
            .unwrap()
            .envelope;
        let Some(v1_1::envelope::Frame::Response(response)) = response.frame else {
            panic!("first denial missing")
        };
        assert_eq!(response.request_id, 17);
        assert!(channel.closed.load(Ordering::SeqCst));
    }
}

#[test]
fn drain_after_reserving_denial_keeps_its_finite_fifo_attempt() {
    let spawner = Arc::new(FakeSpawner::default());
    let owner = owner_with_write_timeout(spawner.clone(), 400);
    let (channel, clock, route) = setup(&owner, &spawner, 7, 200);
    channel.stall.store(true, Ordering::SeqCst);
    let handle = route
        .start_planned(&plan(), &saved(), "call-1", timing())
        .unwrap();
    spawner.tick();
    channel.push(selected_peer_request(&route, 17, "tool.invoke"));
    spawner.tick();
    clock.set(500);
    assert_eq!(
        spawner.drive(handle.first_terminal()).err(),
        Some(DriverError::PeerLost)
    );
    channel.push(
        framing::encode(
            &v1_1::Envelope {
                frame: Some(v1_1::envelope::Frame::GoAway(v1_1::GoAway {
                    reason: v1_1::GoAwayReason::GoAwayDrain as i32,
                    retryable: false,
                })),
            },
            nz(8192),
        )
        .unwrap()
        .bytes,
    );
    spawner.tick();
    assert!(!channel.closed.load(Ordering::SeqCst));
    channel.stall.store(false, Ordering::SeqCst);
    spawner.tick();
    assert_eq!(channel.writes().len(), 4);
    assert!(channel.closed.load(Ordering::SeqCst));
}

#[test]
fn late_terminal_after_timer_loss_does_not_discard_reserved_denial() {
    let spawner = Arc::new(FakeSpawner::default());
    let owner = owner_with_write_timeout(spawner.clone(), 400);
    let (channel, clock, route) = setup(&owner, &spawner, 7, 200);
    channel.stall.store(true, Ordering::SeqCst);
    let handle = route
        .start_planned(&plan(), &saved(), "call-1", timing())
        .unwrap();
    spawner.tick();
    channel.push(selected_peer_request(&route, 17, "tool.invoke"));
    spawner.tick();
    clock.set(500);
    assert_eq!(
        spawner.drive(handle.first_terminal()).err(),
        Some(DriverError::PeerLost)
    );
    channel.push(raw("terminal", 1));
    spawner.tick();
    assert!(!channel.closed.load(Ordering::SeqCst));
    channel.stall.store(false, Ordering::SeqCst);
    spawner.tick();
    assert_eq!(channel.writes().len(), 4);
    assert!(channel.closed.load(Ordering::SeqCst));
}

#[test]
fn unpublished_start_preserves_denial_reserved_during_inline_timer_spawn() {
    let spawner = Arc::new(FakeSpawner::default());
    let owner = owner_with_write_timeout(spawner.clone(), 400);
    let (channel, _, route) = setup(&owner, &spawner, 7, 200);
    channel.push(selected_peer_request(&route, 17, "tool.invoke"));
    spawner
        .poll_reader_during_next_spawn
        .store(true, Ordering::SeqCst);
    assert_eq!(
        route
            .start_planned(&plan(), &saved(), "call-1", timing())
            .err(),
        Some(DriverError::Closed),
        "a no-send claim has no published binding"
    );
    assert!(owner.retained_binding().is_none());
    assert!(!channel.closed.load(Ordering::SeqCst));
    assert_eq!(channel.writes().len(), 2);
    spawner.tick_at(2);
    assert_eq!(
        spawner.tasks.lock().unwrap().len(),
        3,
        "unpublished timer retired"
    );
    for _ in 0..5 {
        spawner.tick();
    }
    assert_eq!(channel.writes().len(), 3, "one denial, no InvokeTool frame");
    assert!(channel.closed.load(Ordering::SeqCst));
    for _ in 0..5 {
        spawner.tick();
    }
    assert_eq!(channel.writes().len(), 3, "no phantom write after cleanup");

    let (next_channel, _, next_route) = setup(&owner, &spawner, 8, 210);
    let handle = next_route
        .start_planned(&plan(), &saved(), "call-2", timing())
        .unwrap();
    spawner.tick();
    assert_eq!(next_channel.writes().len(), 3);
    next_channel.push(raw("terminal", 1));
    assert!(matches!(
        spawner.drive(handle.first_terminal()),
        Ok(TypedTerminal::Result(_))
    ));
}

#[test]
fn denial_method_lookup_uses_selected_minor_and_capability_profile() {
    for minor in [0, 1] {
        for (method, expected) in [
            ("tool.invoke", v1_1::ErrorCode::Forbidden as i32),
            ("snapshot.get", v1_1::ErrorCode::UnsupportedMethod as i32),
            ("invented.method", v1_1::ErrorCode::UnsupportedMethod as i32),
        ] {
            let spawner = Arc::new(FakeSpawner::default());
            let owner = owner(spawner.clone());
            let (channel, _, route) = setup_minor(&owner, &spawner, 7, 200, minor);
            channel.push(selected_peer_request(&route, 17, method));
            for _ in 0..5 {
                spawner.tick();
            }
            assert_eq!(channel.writes().len(), 3, "minor={minor} method={method}");
            let code = if minor == 0 {
                let mut reader = FrameReader::<v1_0::Envelope>::new(nz(8192));
                let envelope = reader
                    .read_one(&channel.writes()[2])
                    .unwrap()
                    .frame
                    .unwrap()
                    .envelope;
                let Some(v1_0::envelope::Frame::Response(response)) = envelope.frame else {
                    panic!("denial missing")
                };
                let Some(v1_0::response::Terminal::Error(error)) = response.terminal else {
                    panic!("typed error missing")
                };
                error.code
            } else {
                let mut reader = FrameReader::<v1_1::Envelope>::new(nz(8192));
                let envelope = reader
                    .read_one(&channel.writes()[2])
                    .unwrap()
                    .frame
                    .unwrap()
                    .envelope;
                let Some(v1_1::envelope::Frame::Response(response)) = envelope.frame else {
                    panic!("denial missing")
                };
                let Some(v1_1::response::Terminal::Error(error)) = response.terminal else {
                    panic!("typed error missing")
                };
                error.code
            };
            assert_eq!(code, expected, "minor={minor} method={method}");
            assert!(channel.closed.load(Ordering::SeqCst));
        }
    }
}

#[test]
fn denial_lookup_accepts_tool_1_0_on_core_1_1_connection() {
    for (method, expected) in [
        ("tool.invoke", v1_1::ErrorCode::Forbidden as i32),
        ("snapshot.get", v1_1::ErrorCode::UnsupportedMethod as i32),
    ] {
        let spawner = Arc::new(FakeSpawner::default());
        let owner = owner(spawner.clone());
        let (channel, _, route) = setup_core_1_1_tool_1_0(&owner, &spawner, 7, 200);
        channel.push(selected_peer_request(&route, 17, method));
        for _ in 0..5 {
            spawner.tick();
        }
        assert_eq!(channel.writes().len(), 3);
        let mut reader = FrameReader::<v1_1::Envelope>::new(nz(8192));
        let envelope = reader
            .read_one(&channel.writes()[2])
            .unwrap()
            .frame
            .unwrap()
            .envelope;
        let Some(v1_1::envelope::Frame::Response(response)) = envelope.frame else {
            panic!("denial missing")
        };
        let Some(v1_1::response::Terminal::Error(error)) = response.terminal else {
            panic!("typed error missing")
        };
        assert_eq!(error.code, expected, "method={method}");
    }
}

fn denial_code(bytes: &[u8], core_minor: u32) -> i32 {
    if core_minor == 0 {
        let mut reader = FrameReader::<v1_0::Envelope>::new(nz(8192));
        let envelope = reader.read_one(bytes).unwrap().frame.unwrap().envelope;
        let Some(v1_0::envelope::Frame::Response(response)) = envelope.frame else {
            panic!("denial missing")
        };
        let Some(v1_0::response::Terminal::Error(error)) = response.terminal else {
            panic!("typed denial missing")
        };
        assert_eq!(response.request_id, 17);
        assert!(error.safe_message.is_empty());
        error.code
    } else {
        let mut reader = FrameReader::<v1_1::Envelope>::new(nz(8192));
        let envelope = reader.read_one(bytes).unwrap().frame.unwrap().envelope;
        let Some(v1_1::envelope::Frame::Response(response)) = envelope.frame else {
            panic!("denial missing")
        };
        let Some(v1_1::response::Terminal::Error(error)) = response.terminal else {
            panic!("typed denial missing")
        };
        assert_eq!(response.request_id, 17);
        assert!(error.safe_message.is_empty());
        error.code
    }
}

#[test]
fn selected_reference_is_fully_checked_before_method_denial() {
    for route_kind in [0, 1, 2] {
        for mutation in 0..6 {
            let spawner = Arc::new(FakeSpawner::default());
            let owner = owner(spawner.clone());
            let (channel, _, route) = match route_kind {
                0 => setup_minor(&owner, &spawner, 7, 200, 0),
                1 => setup_minor(&owner, &spawner, 7, 200, 1),
                _ => setup_core_1_1_tool_1_0(&owner, &spawner, 7, 200),
            };
            let bytes = peer_request_with_ref(&route, "tool.invoke", |reference| match mutation {
                0 => {
                    reference.profile.as_mut().unwrap().minor =
                        1 - reference.profile.as_ref().unwrap().minor
                }
                1 => reference.profile.as_mut().unwrap().minor = 999,
                2 => reference.schema_digest_sha256[0] ^= 1,
                3 => reference.authenticated_extension_id = "other-peer".into(),
                4 => reference.kind = "host-action".into(),
                _ => reference.profile.as_mut().unwrap().major = 2,
            });
            channel.push(bytes);
            for _ in 0..5 {
                spawner.tick();
            }
            assert_eq!(
                channel.writes().len(),
                2,
                "route={route_kind} mutation={mutation}"
            );
            assert!(channel.closed.load(Ordering::SeqCst));
        }
    }
}

#[test]
fn coherent_ungranted_and_omitted_references_use_profile_scoped_denials() {
    for route_kind in [0, 1, 2] {
        for case in 0..9 {
            let spawner = Arc::new(FakeSpawner::default());
            let owner = owner(spawner.clone());
            let (channel, _, route) = match route_kind {
                0 => setup_minor(&owner, &spawner, 7, 200, 0),
                1 => setup_minor(&owner, &spawner, 7, 200, 1),
                _ => setup_core_1_1_tool_1_0(&owner, &spawner, 7, 200),
            };
            let method = match case {
                1 | 3 | 5 | 7 => "snapshot.get",
                _ => "tool.invoke",
            };
            let bytes = peer_request_with_ref(&route, method, |reference| match case {
                0 | 1 => reference.name = "ungranted".into(),
                2..=4 | 7 | 8 => {
                    let profile = reference.profile.as_mut().unwrap();
                    profile.name = "host-action.v1".into();
                    profile.minor = if case == 4 || case == 7 { 1 } else { 0 };
                    reference.kind = "host-action".into();
                    reference.name = "ungranted-action".into();
                    reference.schema_digest_sha256 =
                        canonical_profile_digest("host-action.v1", 1, profile.minor)
                            .unwrap()
                            .to_vec();
                    if case == 4 || case == 8 {
                        reference.schema_digest_sha256[0] ^= 1;
                    }
                }
                5 => {
                    reference.profile.as_mut().unwrap().name = "future.v1".into();
                    reference.kind = "future".into();
                }
                6 => {
                    reference.profile.as_mut().unwrap().name = "host-action.v1".into();
                    reference.profile.as_mut().unwrap().minor = 999;
                    reference.kind = "host-action".into();
                }
                _ => {}
            });
            channel.push(bytes);
            for _ in 0..5 {
                spawner.tick();
            }
            let expected = if case == 4 || case == 8 {
                None
            } else if matches!(case, 0 | 3 | 7) {
                Some(v1_1::ErrorCode::Forbidden as i32)
            } else {
                Some(v1_1::ErrorCode::UnsupportedMethod as i32)
            };
            assert_eq!(
                channel.writes().len(),
                if expected.is_some() { 3 } else { 2 },
                "route={route_kind} case={case}"
            );
            if let Some(expected) = expected {
                assert_eq!(
                    denial_code(&channel.writes()[2], if route_kind == 0 { 0 } else { 1 }),
                    expected
                );
            }
            assert!(channel.closed.load(Ordering::SeqCst));
        }
    }
}

#[test]
fn omitted_known_profile_without_claimed_major_descriptor_has_no_known_method() {
    for route_kind in [0, 1, 2] {
        for method in ["tool.invoke", "snapshot.get"] {
            let spawner = Arc::new(FakeSpawner::default());
            let owner = owner(spawner.clone());
            let (channel, _, route) = match route_kind {
                0 => setup_minor(&owner, &spawner, 7, 200, 0),
                1 => setup_minor(&owner, &spawner, 7, 200, 1),
                _ => setup_core_1_1_tool_1_0(&owner, &spawner, 7, 200),
            };
            channel.push(peer_request_with_ref(&route, method, |reference| {
                reference.name = "omitted-action".into();
                reference.kind = "host-action".into();
                let profile = reference.profile.as_mut().unwrap();
                profile.name = "host-action.v1".into();
                profile.major = 2;
                profile.minor = 0;
                reference.schema_digest_sha256 = vec![0x42; 32];
            }));
            for _ in 0..5 {
                spawner.tick();
            }
            assert_eq!(
                channel.writes().len(),
                3,
                "route={route_kind} method={method}"
            );
            assert_eq!(
                denial_code(&channel.writes()[2], if route_kind == 0 { 0 } else { 1 }),
                v1_1::ErrorCode::UnsupportedMethod as i32,
                "route={route_kind} method={method}"
            );
            assert!(channel.closed.load(Ordering::SeqCst));
        }
    }
}
