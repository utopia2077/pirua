#![cfg(feature = "extensions")]

use std::collections::VecDeque;
use std::future::{Future, poll_fn};
use std::num::{NonZeroU64, NonZeroUsize};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};

use pi_ai_core::{BoxFuture, JsonObject};
use pi_extension_protocol::activation::{
    AgentTool, AgentToolMode, ApprovedTool, HostAuthority, ToolReplay, ToolSchemaInputs,
};
use pi_extension_protocol::endpoint::{EndpointPhase, SelectedEnvelope};
use pi_extension_protocol::jcs::Limits as JsonLimits;
use pi_extension_protocol::tool_live::{Start, ToolInvocation};
use pi_extension_protocol::wire::framing::{EncodedFrame, FrameReader};
use pi_extension_protocol::wire::v1_1;
use pi_harness_runtime::extensions::transport::{
    ChannelError, ConnectionPump, MonotonicClock, OrderedByteChannel, PumpConfig, PumpError,
};
use pi_harness_runtime::extensions::value::encode_arguments;

const RAW: &str = include_str!("../../pi-extension-protocol/schema/fixtures/raw-vectors.json");

#[derive(Default)]
struct CountWake(AtomicUsize);

impl Wake for CountWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn poll_once<F: Future>(future: Pin<&mut F>, wake: &Arc<CountWake>) -> Poll<F::Output> {
    let waker = Waker::from(wake.clone());
    future.poll(&mut Context::from_waker(&waker))
}

fn ready<F: Future>(future: F) -> F::Output {
    let wake = Arc::new(CountWake::default());
    let mut future = Box::pin(future);
    match poll_once(future.as_mut(), &wake) {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("scripted operation unexpectedly pending"),
    }
}

#[derive(Clone)]
enum ReadAction {
    Bytes(Vec<u8>),
    Eof,
    Failed,
}

#[derive(Clone, Copy)]
enum WriteMode {
    Complete,
    Failed,
    Stall,
    PartialStall,
}

struct ChannelState {
    reads: VecDeque<ReadAction>,
    writes: Vec<Vec<u8>>,
    partial_write_bytes: Vec<u8>,
    read_waker: Option<Waker>,
    write_waker: Option<Waker>,
    mode: WriteMode,
    closed: bool,
}

impl Default for ChannelState {
    fn default() -> Self {
        Self {
            reads: VecDeque::new(),
            writes: Vec::new(),
            partial_write_bytes: Vec::new(),
            read_waker: None,
            write_waker: None,
            mode: WriteMode::Complete,
            closed: false,
        }
    }
}

#[derive(Default)]
struct FakeChannel {
    state: Mutex<ChannelState>,
    closes: AtomicUsize,
    read_buffer_len: AtomicUsize,
    extra_declared_read_scratch_bytes: usize,
}

impl FakeChannel {
    fn with_extra_declared_read_scratch_bytes(extra: usize) -> Self {
        Self {
            extra_declared_read_scratch_bytes: extra,
            ..Self::default()
        }
    }

    fn push(&self, action: ReadAction) {
        let waker = {
            let mut state = self.state.lock().unwrap();
            state.reads.push_back(action);
            state.read_waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    fn mode(&self, mode: WriteMode) {
        let waker = {
            let mut state = self.state.lock().unwrap();
            state.mode = mode;
            state.write_waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    fn writes(&self) -> Vec<Vec<u8>> {
        self.state.lock().unwrap().writes.clone()
    }

    fn is_closed(&self) -> bool {
        self.state.lock().unwrap().closed
    }

    fn partial_write_len(&self) -> usize {
        self.state.lock().unwrap().partial_write_bytes.len()
    }

    fn read_future<'a>(
        &'a self,
        buffer: &'a mut [u8],
    ) -> impl Future<Output = Result<Option<usize>, ChannelError>> + Send + 'a {
        poll_fn(move |cx| {
            self.read_buffer_len.store(buffer.len(), Ordering::SeqCst);
            let mut state = self.state.lock().unwrap();
            if state.closed {
                return Poll::Ready(Err(ChannelError::Closed));
            }
            match state.reads.pop_front() {
                Some(ReadAction::Bytes(bytes)) => {
                    if bytes.len() <= buffer.len() {
                        buffer[..bytes.len()].copy_from_slice(&bytes);
                    }
                    Poll::Ready(Ok(Some(bytes.len())))
                }
                Some(ReadAction::Eof) => Poll::Ready(Ok(None)),
                Some(ReadAction::Failed) => Poll::Ready(Err(ChannelError::Failed)),
                None => {
                    state.read_waker = Some(cx.waker().clone());
                    Poll::Pending
                }
            }
        })
    }
}

impl OrderedByteChannel for FakeChannel {
    fn max_read_scratch_bytes(&self) -> usize {
        let mut empty = [];
        let future_bytes = std::mem::size_of_val(&self.read_future(&mut empty));
        future_bytes.saturating_add(self.extra_declared_read_scratch_bytes)
    }

    fn read_chunk<'a>(
        &'a self,
        buffer: &'a mut [u8],
    ) -> BoxFuture<'a, Result<Option<usize>, ChannelError>> {
        Box::pin(self.read_future(buffer))
    }

    fn write_frame(&self, frame: EncodedFrame) -> BoxFuture<'_, Result<(), ChannelError>> {
        Box::pin(poll_fn(move |cx| {
            let mut state = self.state.lock().unwrap();
            if state.closed {
                return Poll::Ready(Err(ChannelError::Closed));
            }
            match state.mode {
                WriteMode::Complete => {
                    state.writes.push(frame.bytes.clone());
                    Poll::Ready(Ok(()))
                }
                WriteMode::Failed => Poll::Ready(Err(ChannelError::Failed)),
                WriteMode::Stall => {
                    state.write_waker = Some(cx.waker().clone());
                    Poll::Pending
                }
                WriteMode::PartialStall => {
                    if state.partial_write_bytes.is_empty() {
                        state.partial_write_bytes.push(frame.bytes[0]);
                    }
                    state.write_waker = Some(cx.waker().clone());
                    Poll::Pending
                }
            }
        }))
    }

    fn close(&self) {
        let (read, write) = {
            let mut state = self.state.lock().unwrap();
            if state.closed {
                return;
            }
            state.closed = true;
            self.closes.fetch_add(1, Ordering::SeqCst);
            (state.read_waker.take(), state.write_waker.take())
        };
        if let Some(waker) = read {
            waker.wake();
        }
        if let Some(waker) = write {
            waker.wake();
        }
    }
}

#[derive(Default)]
struct FakeClock {
    now: AtomicU64,
    sleepers: Mutex<Vec<Waker>>,
}

impl FakeClock {
    fn new(now: u64) -> Self {
        Self {
            now: AtomicU64::new(now),
            sleepers: Mutex::new(Vec::new()),
        }
    }

    fn set(&self, now: u64) {
        self.now.store(now, Ordering::SeqCst);
        for waker in std::mem::take(&mut *self.sleepers.lock().unwrap()) {
            waker.wake();
        }
    }
}

impl MonotonicClock for FakeClock {
    fn now_ms(&self) -> u64 {
        self.now.load(Ordering::SeqCst)
    }

    fn sleep_until(&self, deadline_ms: u64) -> BoxFuture<'_, ()> {
        Box::pin(poll_fn(move |cx| {
            if self.now_ms() >= deadline_ms {
                return Poll::Ready(());
            }
            self.sleepers.lock().unwrap().push(cx.waker().clone());
            Poll::Pending
        }))
    }
}

fn limits() -> v1_1::ProtocolLimits {
    v1_1::ProtocolLimits {
        pre_handshake_frame_bytes: 120,
        frame_bytes: 300,
        aggregate_decoded_bytes: 300,
        value_depth: 32,
        value_elements: 1024,
        string_bytes: 512,
        bytes_length: 512,
        in_flight_requests: 8,
        blocking_queue_bytes: 4096,
        progress_queue_bytes: 4096,
        passive_queue_bytes: 4096,
        telemetry_queue_bytes: 4096,
        nesting_depth: 8,
        requests_per_second: 32,
        handshake_ms: 100,
        cancellation_grace_ms: 10,
        max_stream_credit_bytes: 4096,
    }
}

fn pump(channel: Arc<FakeChannel>, clock: Arc<FakeClock>, chunk: usize) -> ConnectionPump {
    let host = HostAuthority::new("peer-a".into(), 7, 1, 1, limits(), vec![], vec![]).unwrap();
    ConnectionPump::new(
        host,
        channel,
        clock,
        PumpConfig {
            max_read_chunk: NonZeroUsize::new(chunk).unwrap(),
            write_timeout_ms: NonZeroU64::new(10).unwrap(),
        },
    )
    .unwrap()
}

fn tool_pump(channel: Arc<FakeChannel>, clock: Arc<FakeClock>) -> ConnectionPump {
    let catalog: serde_json::Value = serde_json::from_str(include_str!(
        "../../pi-extension-protocol/schema/fixtures/host-catalog.json"
    ))
    .unwrap();
    let entry = &catalog["entries"][0];
    let agent = &entry["agentTool"];
    let descriptor = AgentTool {
        name: "echo".into(),
        label: agent["label"].as_str().unwrap().into(),
        description: agent["description"].as_str().unwrap().into(),
        parameters: agent["parameters"].clone(),
        execution_mode: AgentToolMode::Sequential,
        replay: ToolReplay::Safe,
    };
    let inputs = [0, 1]
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
        ApprovedTool::new("peer-a".into(), descriptor, inputs, JsonLimits::default()).unwrap();
    let mut host_limits = limits();
    host_limits.pre_handshake_frame_bytes = 4096;
    host_limits.frame_bytes = 8192;
    host_limits.aggregate_decoded_bytes = 8192;
    let host =
        HostAuthority::new("peer-a".into(), 7, 1, 1, host_limits, vec![tool], vec![]).unwrap();
    ConnectionPump::new(
        host,
        channel,
        clock,
        PumpConfig {
            max_read_chunk: NonZeroUsize::new(8192).unwrap(),
            write_timeout_ms: NonZeroU64::new(10).unwrap(),
        },
    )
    .unwrap()
}

fn varint(mut value: usize) -> Vec<u8> {
    let mut bytes = Vec::new();
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        bytes.push(byte);
        if value == 0 {
            return bytes;
        }
    }
}

fn field(number: u8, value: &[u8]) -> Vec<u8> {
    let mut bytes = vec![number << 3 | 2];
    bytes.extend(varint(value.len()));
    bytes.extend(value);
    bytes
}

fn frame(arm: u8, message: &[u8]) -> Vec<u8> {
    let envelope = field(arm, message);
    [varint(envelope.len()), envelope].concat()
}

fn hello() -> Vec<u8> {
    let version = [0x08, 0x01, 0x18, 0x01];
    let profile = [field(1, b"tool.v1"), vec![0x10, 0x01, 0x20, 0x01]].concat();
    frame(1, &[field(1, &version), field(2, &profile)].concat())
}

fn manifest() -> Vec<u8> {
    let version = [0x08, 0x01, 0x18, 0x01];
    frame(
        3,
        &[field(1, b"metadata"), field(2, b"0.1"), field(3, &version)].concat(),
    )
}

fn tool_manifest() -> Vec<u8> {
    let vectors: serde_json::Value = serde_json::from_str(include_str!(
        "../../pi-extension-protocol/schema/fixtures/activation-vectors.json"
    ))
    .unwrap();
    let hex = vectors["cases"]["valid_1_1"]["manifest_pb_hex"]
        .as_str()
        .unwrap();
    let (pairs, tail) = hex.as_bytes().as_chunks::<2>();
    assert!(tail.is_empty());
    let body: Vec<u8> = pairs
        .iter()
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect();
    frame(3, &body)
}

fn ready_frame() -> Vec<u8> {
    frame(5, &[])
}

fn raw_request() -> Vec<u8> {
    let data: serde_json::Value = serde_json::from_str(RAW).unwrap();
    let hex = data["request"]["frame_hex"].as_str().unwrap();
    let (pairs, tail) = hex.as_bytes().as_chunks::<2>();
    assert!(tail.is_empty());
    pairs
        .iter()
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}

fn sent_arms(channel: &FakeChannel) -> Vec<&'static str> {
    channel
        .writes()
        .iter()
        .map(|bytes| {
            let mut reader = FrameReader::<v1_1::Envelope>::new(NonZeroUsize::new(120).unwrap());
            let step = reader.read_one(bytes).unwrap();
            assert_eq!(step.consumed, bytes.len());
            match step.frame.unwrap().envelope.frame.unwrap() {
                v1_1::envelope::Frame::HelloAck(_) => "ack",
                v1_1::envelope::Frame::Grant(_) => "grant",
                other => panic!("unexpected outbound frame: {other:?}"),
            }
        })
        .collect()
}

#[test]
fn fragments_and_coalesced_ready_switch_cap_before_active_tail() {
    let channel = Arc::new(FakeChannel::default());
    let clock = Arc::new(FakeClock::new(100));
    let mut pump = pump(channel.clone(), clock, 512);
    let hello = hello();
    let manifest = manifest();
    let request = raw_request();
    assert_eq!(request.len(), 221);
    channel.push(ReadAction::Bytes(hello[..1].to_vec()));
    channel.push(ReadAction::Bytes(hello[1..].to_vec()));
    channel.push(ReadAction::Bytes(manifest[..3].to_vec()));
    channel.push(ReadAction::Bytes(manifest[3..].to_vec()));
    channel.push(ReadAction::Bytes([ready_frame(), request.clone()].concat()));
    let active = ready(pump.next_active()).unwrap().unwrap();
    assert_eq!(active.body_len, 219);
    assert!(matches!(
        active.envelope,
        SelectedEnvelope::V1_1(v1_1::Envelope {
            frame: Some(v1_1::envelope::Frame::Request(_))
        })
    ));
    assert_eq!(pump.phase(), EndpointPhase::Active);
    assert!(pump.active_grant().is_some());
    assert_eq!(sent_arms(&channel), ["ack", "grant"]);
    channel.push(ReadAction::Eof);
    assert!(ready(pump.next_active()).unwrap().is_none());
    assert_eq!(pump.phase(), EndpointPhase::Closed);
    assert_eq!(channel.closes.load(Ordering::SeqCst), 1);
}

#[test]
fn active_coalesced_frames_are_returned_one_at_a_time() {
    let channel = Arc::new(FakeChannel::default());
    let clock = Arc::new(FakeClock::new(100));
    let mut pump = pump(channel.clone(), clock, 512);
    let request = raw_request();
    channel.push(ReadAction::Bytes(hello()));
    channel.push(ReadAction::Bytes(manifest()));
    channel.push(ReadAction::Bytes(
        [ready_frame(), request.clone(), request].concat(),
    ));
    assert_eq!(ready(pump.next_active()).unwrap().unwrap().body_len, 219);
    assert_eq!(ready(pump.next_active()).unwrap().unwrap().body_len, 219);
    assert_eq!(sent_arms(&channel), ["ack", "grant"]);
}

#[test]
fn quiet_peer_ready_is_observable_before_an_active_frame() {
    let channel = Arc::new(FakeChannel::default());
    let clock = Arc::new(FakeClock::new(100));
    let mut pump = pump(channel.clone(), clock, 512);
    channel.push(ReadAction::Bytes(hello()));
    channel.push(ReadAction::Bytes(manifest()));
    channel.push(ReadAction::Bytes(ready_frame()));
    let wake = Arc::new(CountWake::default());
    let mut future = Box::pin(pump.drive_until_ready());
    let owned = match poll_once(future.as_mut(), &wake) {
        Poll::Ready(Ok(grant)) => grant,
        _ => panic!("Ready must return before active input"),
    };
    drop(future);
    assert_eq!(pump.phase(), EndpointPhase::Active);
    assert!(pump.active_grant().is_some());
    assert_eq!(owned.wire().connection_generation, 7);
    assert_eq!(sent_arms(&channel), ["ack", "grant"]);
    assert!(!channel.is_closed());
    // A second call must not wait for another peer byte or consume an active frame.
    let repeated = ready(pump.drive_until_ready()).unwrap();
    assert_eq!(repeated.wire().connection_generation, 7);
    channel.push(ReadAction::Bytes(raw_request()));
    assert_eq!(ready(pump.next_active()).unwrap().unwrap().body_len, 219);
}

#[test]
fn ready_boundary_preserves_coalesced_active_tail_for_next_active() {
    let channel = Arc::new(FakeChannel::default());
    let clock = Arc::new(FakeClock::new(100));
    let mut pump = pump(channel.clone(), clock, 512);
    let request = raw_request();
    channel.push(ReadAction::Bytes(hello()));
    channel.push(ReadAction::Bytes(manifest()));
    channel.push(ReadAction::Bytes(
        [ready_frame(), request.clone(), request].concat(),
    ));
    let owned = ready(pump.drive_until_ready()).unwrap();
    assert_eq!(pump.phase(), EndpointPhase::Active);
    assert!(pump.active_grant().is_some());
    assert_eq!(owned.wire().connection_generation, 7);
    assert_eq!(sent_arms(&channel), ["ack", "grant"]);
    assert_eq!(ready(pump.next_active()).unwrap().unwrap().body_len, 219);
    assert_eq!(ready(pump.next_active()).unwrap().unwrap().body_len, 219);
    assert!(!channel.is_closed());
}

#[test]
fn owned_ready_grant_can_start_a_tool_while_active_read_is_pending() {
    let channel = Arc::new(FakeChannel::default());
    let clock = Arc::new(FakeClock::new(100));
    let mut pump = tool_pump(channel.clone(), clock);
    channel.push(ReadAction::Bytes(hello()));
    channel.push(ReadAction::Bytes(tool_manifest()));
    channel.push(ReadAction::Bytes(ready_frame()));
    let owned = ready(pump.drive_until_ready()).unwrap();
    assert_eq!(owned.wire().connection_generation, 7);
    assert_eq!(owned.tools().len(), 1);
    assert_eq!(channel.writes().len(), 2);

    let wake = Arc::new(CountWake::default());
    let mut pending_read = Box::pin(pump.next_active());
    assert!(poll_once(pending_read.as_mut(), &wake).is_pending());
    let saved: JsonObject = serde_json::from_str(r#"{"text":"ping"}"#).unwrap();
    let arguments = encode_arguments(&saved, &owned, "echo").unwrap();
    let (invocation, outbound) = ToolInvocation::start(
        &owned,
        "echo",
        Start {
            effect_key: v1_1::EffectKey {
                session_id: "session".into(),
                lane: "main".into(),
                operation_id: "op".into(),
                effect_kind: "tool".into(),
                step_or_task_id: "step".into(),
                logical_attempt: 0,
                item_index: Some(0),
            },
            source_call_id: "call-1".into(),
            request_id: 17,
            active_generation: 7,
            arguments,
            now_ms: 200,
            requested_deadline_after_ms: Some(700),
            method_limit_ms: 1000,
            connection_remaining_ms: 500,
            parent_remaining_ms: Some(300),
            initial_credit_bytes: Some(137),
        },
    )
    .unwrap();
    assert_eq!(invocation.binding().request_id(), 17);
    assert!(outbound.body_len > 0);

    channel.push(ReadAction::Bytes(raw_request()));
    let active = match poll_once(pending_read.as_mut(), &wake) {
        Poll::Ready(Ok(Some(frame))) => frame,
        _ => panic!("pending active read did not resume"),
    };
    assert_eq!(active.body_len, 219);
    drop(pending_read);
    assert!(!channel.is_closed());
}

#[test]
fn dropping_pending_ready_driver_closes_before_reuse() {
    let channel = Arc::new(FakeChannel::default());
    channel.mode(WriteMode::Stall);
    channel.push(ReadAction::Bytes(hello()));
    let clock = Arc::new(FakeClock::new(100));
    let mut pump = pump(channel.clone(), clock, 512);
    let wake = Arc::new(CountWake::default());
    let mut future = Box::pin(pump.drive_until_ready());
    assert!(poll_once(future.as_mut(), &wake).is_pending());
    drop(future);
    assert_eq!(pump.phase(), EndpointPhase::Closed);
    assert!(channel.is_closed());
    assert_eq!(
        ready(pump.drive_until_ready()).err(),
        Some(PumpError::Closed)
    );
}

#[test]
fn silent_peer_handshake_timer_wakes_and_closes() {
    let channel = Arc::new(FakeChannel::default());
    let clock = Arc::new(FakeClock::new(100));
    let mut pump = pump(channel.clone(), clock.clone(), 512);
    let wake = Arc::new(CountWake::default());
    let mut future = Box::pin(pump.next_active());
    assert!(poll_once(future.as_mut(), &wake).is_pending());
    clock.set(200);
    assert!(wake.0.load(Ordering::SeqCst) > 0);
    assert!(matches!(
        poll_once(future.as_mut(), &wake),
        Poll::Ready(Err(PumpError::HandshakeTimeout))
    ));
    drop(future);
    assert_eq!(pump.phase(), EndpointPhase::Closed);
    assert!(channel.is_closed());
}

#[test]
fn bad_chunks_read_failure_and_malformed_prefix_close_without_output() {
    for (action, chunk, expected) in [
        (ReadAction::Bytes(vec![]), 8, PumpError::BadReadChunk),
        (ReadAction::Bytes(vec![0; 9]), 8, PumpError::BadReadChunk),
        (ReadAction::Failed, 8, PumpError::ReadFailed),
        (ReadAction::Bytes(vec![121]), 8, PumpError::MalformedInput),
        (
            ReadAction::Bytes(vec![2, 0x7a, 0]),
            8,
            PumpError::MalformedInput,
        ),
    ] {
        let channel = Arc::new(FakeChannel::default());
        let clock = Arc::new(FakeClock::new(100));
        let mut pump = pump(channel.clone(), clock, chunk);
        channel.push(action);
        assert_eq!(ready(pump.next_active()).err(), Some(expected));
        assert!(channel.is_closed());
        assert!(channel.writes().is_empty());
        assert!(pump.active_grant().is_none());
        assert_eq!(channel.read_buffer_len.load(Ordering::SeqCst), chunk);
    }
}

#[test]
fn fake_read_declaration_covers_its_boxed_future_payload() {
    let channel = FakeChannel::default();
    let mut empty = [];
    let future = channel.read_chunk(&mut empty);
    let future_bytes = std::mem::size_of_val(future.as_ref().get_ref());
    assert!(future_bytes > 0);
    assert!(channel.max_read_scratch_bytes() >= future_bytes);
}

#[test]
fn pending_read_borrows_only_declared_capacity_and_counts_host_scratch() {
    let channel = Arc::new(FakeChannel::default());
    let mut empty = [];
    let future = channel.read_chunk(&mut empty);
    let future_bytes = std::mem::size_of_val(future.as_ref().get_ref());
    drop(future);
    let mut pump = pump(channel.clone(), Arc::new(FakeClock::new(100)), 64);
    assert_eq!(channel.max_read_scratch_bytes(), future_bytes);
    assert_eq!(
        pump.read_storage_bound_bytes(),
        pump.read_buffer_capacity_bytes() + future_bytes
    );
    assert!(pump.read_buffer_capacity_bytes() >= 64);

    let wake = Arc::new(CountWake::default());
    let mut future = Box::pin(pump.next_active());
    assert!(poll_once(future.as_mut(), &wake).is_pending());
    assert_eq!(channel.read_buffer_len.load(Ordering::SeqCst), 64);
    assert_eq!(channel.max_read_scratch_bytes(), future_bytes);
    drop(future);
    assert!(channel.is_closed());
}

#[test]
fn read_capacity_and_declared_host_scratch_must_fit_together() {
    let channel = Arc::new(FakeChannel::with_extra_declared_read_scratch_bytes(
        usize::MAX,
    ));
    let host = HostAuthority::new("peer-a".into(), 7, 1, 1, limits(), vec![], vec![]).unwrap();
    let result = ConnectionPump::new(
        host,
        channel,
        Arc::new(FakeClock::new(100)),
        PumpConfig {
            max_read_chunk: NonZeroUsize::new(1).unwrap(),
            write_timeout_ms: NonZeroU64::new(10).unwrap(),
        },
    );
    assert!(matches!(result, Err(PumpError::ReadBufferUnavailable)));
}

#[test]
fn truncated_eof_and_pre_ready_clean_eof_are_distinct() {
    for (actions, expected) in [
        (
            vec![ReadAction::Bytes(vec![2, 0x0a]), ReadAction::Eof],
            PumpError::TruncatedEof,
        ),
        (vec![ReadAction::Eof], PumpError::HandshakeEof),
    ] {
        let channel = Arc::new(FakeChannel::default());
        let clock = Arc::new(FakeClock::new(100));
        let mut pump = pump(channel.clone(), clock, 512);
        for action in actions {
            channel.push(action);
        }
        assert_eq!(ready(pump.next_active()).err(), Some(expected));
        assert!(channel.is_closed());
    }
}

#[test]
fn handshake_write_failure_and_timeout_close_generation() {
    let failed = Arc::new(FakeChannel::default());
    failed.mode(WriteMode::Failed);
    failed.push(ReadAction::Bytes(hello()));
    let mut failed_pump = pump(failed.clone(), Arc::new(FakeClock::new(100)), 512);
    assert_eq!(
        ready(failed_pump.next_active()).err(),
        Some(PumpError::WriteFailed)
    );
    assert!(failed.writes().is_empty());
    assert!(failed.is_closed());

    let stalled = Arc::new(FakeChannel::default());
    stalled.mode(WriteMode::Stall);
    stalled.push(ReadAction::Bytes(hello()));
    let clock = Arc::new(FakeClock::new(100));
    let mut pump = pump(stalled.clone(), clock.clone(), 512);
    let wake = Arc::new(CountWake::default());
    let mut future = Box::pin(pump.next_active());
    assert!(poll_once(future.as_mut(), &wake).is_pending());
    clock.set(110);
    assert!(wake.0.load(Ordering::SeqCst) > 0);
    assert!(matches!(
        poll_once(future.as_mut(), &wake),
        Poll::Ready(Err(PumpError::WriteTimeout))
    ));
    drop(future);
    assert!(stalled.writes().is_empty());
    assert!(stalled.is_closed());
}

#[test]
fn handshake_deadline_also_bounds_a_stalled_write() {
    let channel = Arc::new(FakeChannel::default());
    channel.mode(WriteMode::Stall);
    channel.push(ReadAction::Bytes(hello()));
    let clock = Arc::new(FakeClock::new(100));
    let host = HostAuthority::new("peer-a".into(), 7, 1, 1, limits(), vec![], vec![]).unwrap();
    let mut pump = ConnectionPump::new(
        host,
        channel.clone(),
        clock.clone(),
        PumpConfig {
            max_read_chunk: NonZeroUsize::new(512).unwrap(),
            write_timeout_ms: NonZeroU64::new(200).unwrap(),
        },
    )
    .unwrap();
    let wake = Arc::new(CountWake::default());
    let mut future = Box::pin(pump.next_active());
    assert!(poll_once(future.as_mut(), &wake).is_pending());
    clock.set(200);
    assert!(wake.0.load(Ordering::SeqCst) > 0);
    assert!(matches!(
        poll_once(future.as_mut(), &wake),
        Poll::Ready(Err(PumpError::HandshakeTimeout))
    ));
    drop(future);
    assert!(channel.is_closed());
}

#[test]
fn host_close_wakes_pending_read_and_is_idempotent() {
    let channel = Arc::new(FakeChannel::default());
    let mut pump = pump(channel.clone(), Arc::new(FakeClock::new(100)), 512);
    let wake = Arc::new(CountWake::default());
    let mut future = Box::pin(pump.next_active());
    assert!(poll_once(future.as_mut(), &wake).is_pending());
    channel.close();
    assert!(wake.0.load(Ordering::SeqCst) > 0);
    assert!(matches!(
        poll_once(future.as_mut(), &wake),
        Poll::Ready(Err(PumpError::Closed))
    ));
    drop(future);
    pump.close();
    pump.close();
    assert_eq!(channel.closes.load(Ordering::SeqCst), 1);
}

#[test]
fn dropping_pending_hello_ack_write_closes_generation_before_reuse() {
    let channel = Arc::new(FakeChannel::default());
    channel.mode(WriteMode::Stall);
    channel.push(ReadAction::Bytes(hello()));
    channel.push(ReadAction::Bytes(manifest()));
    channel.push(ReadAction::Bytes([ready_frame(), raw_request()].concat()));
    let mut pump = pump(channel.clone(), Arc::new(FakeClock::new(100)), 512);
    let wake = Arc::new(CountWake::default());
    let mut future = Box::pin(pump.next_active());
    assert!(poll_once(future.as_mut(), &wake).is_pending());
    assert!(channel.writes().is_empty());
    drop(future);
    assert_eq!(pump.phase(), EndpointPhase::Closed);
    assert!(channel.is_closed());
    assert!(pump.active_grant().is_none());
    channel.mode(WriteMode::Complete);
    assert_eq!(ready(pump.next_active()).err(), Some(PumpError::Closed));
}

#[test]
fn dropping_pending_grant_write_closes_generation_before_ready() {
    let channel = Arc::new(FakeChannel::default());
    channel.push(ReadAction::Bytes(hello()));
    let mut pump = pump(channel.clone(), Arc::new(FakeClock::new(100)), 512);
    let wake = Arc::new(CountWake::default());
    let mut future = Box::pin(pump.next_active());
    assert!(poll_once(future.as_mut(), &wake).is_pending());
    assert_eq!(sent_arms(&channel), ["ack"]);
    channel.mode(WriteMode::Stall);
    channel.push(ReadAction::Bytes(manifest()));
    assert!(poll_once(future.as_mut(), &wake).is_pending());
    drop(future);
    assert_eq!(pump.phase(), EndpointPhase::Closed);
    assert!(channel.is_closed());
    assert!(pump.active_grant().is_none());
    assert_eq!(sent_arms(&channel), ["ack"]);
    channel.mode(WriteMode::Complete);
    channel.push(ReadAction::Bytes([ready_frame(), raw_request()].concat()));
    assert_eq!(ready(pump.next_active()).err(), Some(PumpError::Closed));
}

#[test]
fn dropping_pending_read_closes_generation_before_reuse() {
    let channel = Arc::new(FakeChannel::default());
    let mut pump = pump(channel.clone(), Arc::new(FakeClock::new(100)), 512);
    let wake = Arc::new(CountWake::default());
    let mut future = Box::pin(pump.next_active());
    assert!(poll_once(future.as_mut(), &wake).is_pending());
    drop(future);
    assert_eq!(pump.phase(), EndpointPhase::Closed);
    assert!(channel.is_closed());
    assert!(pump.active_grant().is_none());
    channel.push(ReadAction::Bytes(hello()));
    assert_eq!(ready(pump.next_active()).err(), Some(PumpError::Closed));
}

#[test]
fn dropping_a_partially_written_frame_closes_corruptible_channel() {
    let channel = Arc::new(FakeChannel::default());
    channel.mode(WriteMode::PartialStall);
    channel.push(ReadAction::Bytes(hello()));
    let mut pump = pump(channel.clone(), Arc::new(FakeClock::new(100)), 512);
    let wake = Arc::new(CountWake::default());
    let mut future = Box::pin(pump.next_active());
    assert!(poll_once(future.as_mut(), &wake).is_pending());
    assert_eq!(channel.partial_write_len(), 1);
    assert!(channel.writes().is_empty());
    drop(future);
    assert_eq!(pump.phase(), EndpointPhase::Closed);
    assert!(channel.is_closed());
    assert_eq!(ready(pump.next_active()).err(), Some(PumpError::Closed));
}
