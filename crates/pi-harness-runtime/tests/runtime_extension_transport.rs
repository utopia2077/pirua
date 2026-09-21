#![cfg(feature = "extensions")]

use std::collections::VecDeque;
use std::future::{Future, poll_fn};
use std::num::{NonZeroU64, NonZeroUsize};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};

use pi_ai_core::BoxFuture;
use pi_extension_protocol::activation::HostAuthority;
use pi_extension_protocol::endpoint::{EndpointPhase, SelectedEnvelope};
use pi_extension_protocol::wire::framing::{EncodedFrame, FrameReader};
use pi_extension_protocol::wire::v1_1;
use pi_harness_runtime::extensions::transport::{
    ChannelError, ConnectionPump, MonotonicClock, OrderedByteChannel, PumpConfig, PumpError,
};

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
}

impl FakeChannel {
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
}

impl OrderedByteChannel for FakeChannel {
    fn read_chunk(
        &self,
        _max_bytes: NonZeroUsize,
    ) -> BoxFuture<'_, Result<Option<Vec<u8>>, ChannelError>> {
        Box::pin(poll_fn(move |cx| {
            let mut state = self.state.lock().unwrap();
            if state.closed {
                return Poll::Ready(Err(ChannelError::Closed));
            }
            match state.reads.pop_front() {
                Some(ReadAction::Bytes(bytes)) => Poll::Ready(Ok(Some(bytes))),
                Some(ReadAction::Eof) => Poll::Ready(Ok(None)),
                Some(ReadAction::Failed) => Poll::Ready(Err(ChannelError::Failed)),
                None => {
                    state.read_waker = Some(cx.waker().clone());
                    Poll::Pending
                }
            }
        }))
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
    }
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
