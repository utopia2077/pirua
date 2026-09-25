#![cfg(feature = "extensions")]

mod runtime_r1_r4_support;
#[path = "runtime_r6_retirement_support/mod.rs"]
#[allow(dead_code)]
mod runtime_r6_retirement_support;
#[path = "runtime_sqlite_tool_support/mod.rs"]
mod runtime_sqlite_tool_support;

use std::collections::VecDeque;
use std::future::{Future, poll_fn};
use std::num::{NonZeroU64, NonZeroUsize};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::task::{Context, Poll, Wake, Waker};

use pi_agent_schema::{
    AgentHarness, AgentLane, DriveMode, EffectKey, EffectKind, EffectStepId, EntryId,
    HarnessEventType, LaneId, OperationId, PromptInput, Register, Session, SessionId,
    ToolEffectPlan,
};
use pi_agent_session::storage::AtomicStorage;
use pi_agent_session::{codec::SessionCodec, session::RuntimeSession};
use pi_ai_core::{BoxFuture, JsonObject, SafeUnsigned, SpawnError, TaskSpawner};
use pi_extension_protocol::activation::{
    AgentTool, AgentToolMode, ApprovedTool, HostAuthority, ToolReplay, ToolSchemaInputs,
};
use pi_extension_protocol::descriptor::canonical_profile_digest;
use pi_extension_protocol::jcs;
use pi_extension_protocol::jcs::Limits as JsonLimits;
use pi_extension_protocol::wire::framing::{self, EncodedFrame, FrameReader};
use pi_extension_protocol::wire::{v1_0, v1_1};
use pi_harness_runtime::extensions::tool_driver::{
    CallOutcome, CallTiming, DriverError, EchoRouteTiming, LocalAbortCause, OwnerLimits,
    SingleToolOwner, TypedTerminal,
};
use pi_harness_runtime::extensions::transport::{
    ChannelError, MonotonicClock, OrderedByteChannel, PumpConfig,
};
use pi_harness_runtime::owner::RuntimeFactory;
use runtime_r1_r4_support::{Setup, Summary};
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

fn manifest_with_replay(replay: ToolReplay) -> Vec<u8> {
    if replay == ToolReplay::Safe {
        return manifest();
    }
    let mut reader = FrameReader::<v1_1::Envelope>::new(nz(8192));
    let mut envelope = reader
        .read_one(&manifest())
        .unwrap()
        .frame
        .unwrap()
        .envelope;
    let Some(v1_1::envelope::Frame::Manifest(manifest)) = envelope.frame.as_mut() else {
        panic!("manifest fixture")
    };
    assert_eq!(manifest.capabilities.len(), 1);
    manifest.capabilities[0].replay = v1_1::ReplayContract::ReplayNever as i32;
    framing::encode(&envelope, nz(8192)).unwrap().bytes
}

fn manifest_with_altered_progress() -> Vec<u8> {
    let catalog: Value = serde_json::from_str(CATALOG).unwrap();
    let entry = &catalog["entries"][0];
    let mut altered = entry["selected_minor_schema_sets"]["1.0"]["progress"]["schema"].clone();
    altered["maxProperties"] = serde_json::json!(16);
    let original = hex(
        entry["selected_minor_schema_sets"]["1.0"]["progress"]["schema_digest_sha256"]
            .as_str()
            .unwrap(),
    );
    let replacement = jcs::admit_schema(
        &serde_json::to_vec(&altered).unwrap(),
        jcs::Limits::default(),
    )
    .unwrap()
    .canonical()
    .sha256();
    let vectors: Value = serde_json::from_str(VECTORS).unwrap();
    let mut manifest = hex(vectors["cases"]["valid_1_1"]["manifest_pb_hex"]
        .as_str()
        .unwrap());
    let mut replaced = 0;
    for offset in 0..=manifest.len() - 32 {
        if manifest[offset..offset + 32] == original
            && manifest[..offset]
                .windows(b"tool.progress:echo".len())
                .rposition(|window| window == b"tool.progress:echo")
                .is_some_and(|position| offset - position < 60)
        {
            manifest[offset..offset + 32].copy_from_slice(&replacement);
            replaced += 1;
        }
    }
    assert_eq!(replaced, 2);
    frame(3, &manifest)
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

fn raw_terminal_with_usage(request_id: u64) -> Vec<u8> {
    use prost::Message as _;
    use v1_1::wire_value::Value;
    let mut reader = FrameReader::<v1_1::Envelope>::new(nz(8192));
    let mut envelope = reader
        .read_one(&raw("terminal", request_id as u8))
        .unwrap()
        .frame
        .unwrap()
        .envelope;
    let Some(v1_1::envelope::Frame::StreamEnd(end)) = envelope.frame.as_mut() else {
        panic!("terminal fixture is not StreamEnd")
    };
    let Some(v1_1::stream_end::Terminal::Result(bytes)) = end.terminal.as_mut() else {
        panic!("terminal fixture has no result")
    };
    let mut result = v1_1::InvokeToolResult::decode(bytes.as_slice()).unwrap();
    let bound = result.raw_result.as_mut().expect("schema-bound raw result");
    let Some(v1_1::WireValue {
        value: Some(Value::MapValue(map)),
    }) = bound.value.as_mut()
    else {
        panic!("terminal result is not a map")
    };
    let integer = |key: &str, number| v1_1::WireMapEntry {
        key: key.into(),
        value: Some(v1_1::WireValue {
            value: Some(Value::IntegerValue(number)),
        }),
    };
    let cost = v1_1::WireMap {
        entries: ["cacheRead", "cacheWrite", "input", "output", "total"]
            .into_iter()
            .map(|name| integer(name, 0))
            .collect(),
    };
    let usage = v1_1::WireMap {
        entries: vec![
            integer("cacheRead", 0),
            integer("cacheWrite", 0),
            v1_1::WireMapEntry {
                key: "cost".into(),
                value: Some(v1_1::WireValue {
                    value: Some(Value::MapValue(cost)),
                }),
            },
            integer("input", 5),
            integer("output", 0),
            integer("totalTokens", 5),
        ],
    };
    map.entries.push(v1_1::WireMapEntry {
        key: "usage".into(),
        value: Some(v1_1::WireValue {
            value: Some(Value::MapValue(usage)),
        }),
    });
    *bytes = result.encode_to_vec();
    framing::encode(&envelope, nz(8192)).unwrap().bytes
}

fn externally_abort_pending_echo(runtime: &Setup) {
    use pi_agent_schema::*;
    let Some(Register::LaneState { mut value, .. }) = runtime
        .pump
        .drive(
            runtime
                .session
                .get_register(RegisterNamespace::LaneState, "main"),
        )
        .unwrap()
    else {
        panic!("pending lane")
    };
    let operation = value
        .current_operation_id
        .take()
        .expect("pending operation");
    let Some(Register::OperationState { value: state, .. }) = runtime
        .pump
        .drive(
            runtime
                .session
                .get_register(RegisterNamespace::OperationState, operation.as_str()),
        )
        .unwrap()
    else {
        panic!("pending state")
    };
    let OperationState::Run(run) = state.as_ref() else {
        panic!("run state")
    };
    let RunPhase::Tools(phase) = &run.phase else {
        panic!("tool phase")
    };
    let assistant = phase.batch.assistant_entry_id.clone();
    let leaf = runtime.pump.drive(runtime.session.get_leaf_id()).unwrap();
    let terminal = LaneLastResult::RunAborted(RunAbortedLastResult {
        operation_id: operation.clone(),
        kind: Default::default(),
        leaf_id: leaf,
        final_assistant_entry_id: Some(assistant),
        outcome: Default::default(),
    });
    let mut writes = vec![
        Write::DeleteRegister(RegisterDelete {
            namespace: RegisterNamespace::OperationState,
            key: operation.as_str().into(),
        }),
        Write::DeleteRegister(RegisterDelete {
            namespace: RegisterNamespace::OperationMeta,
            key: operation.as_str().into(),
        }),
        Write::SetRegister(Box::new(RegisterSet::LaneLastResult {
            key: "main".into(),
            value: terminal,
        })),
        Write::SetRegister(Box::new(RegisterSet::LaneState {
            key: "main".into(),
            value,
        })),
    ];
    for namespace in [
        RegisterNamespace::OperationToolArguments,
        RegisterNamespace::OperationPreparation,
    ] {
        let prefix = format!("{}:", operation.as_str());
        for register in runtime
            .pump
            .drive(runtime.session.list_registers(namespace, Some(&prefix)))
            .unwrap()
        {
            let key = match register {
                Register::OperationToolArguments { key, .. }
                | Register::OperationPreparation { key, .. } => key,
                _ => unreachable!(),
            };
            writes.push(Write::DeleteRegister(RegisterDelete { namespace, key }));
        }
    }
    runtime
        .pump
        .drive(runtime.session.commit(Transaction { writes }))
        .unwrap();
}

fn assert_one_synthetic_interrupted_result(snapshot: &pi_agent_session::storage::StorageSnapshot) {
    let results: Vec<_> = snapshot
        .entries
        .iter()
        .filter_map(|entry| match entry {
            pi_agent_schema::Entry::Message(message) => match &message.message {
                pi_agent_schema::AgentMessage::BuiltIn(pi_ai_core::Message::ToolResult(result)) => {
                    Some((message.id.clone(), result.as_ref()))
                }
                _ => None,
            },
            _ => None,
        })
        .collect();
    assert_eq!(results.len(), 1, "one reserved core reconciliation result");
    let (id, result) = &results[0];
    assert!(result.is_error);
    assert!(result.usage.is_none());
    assert!(
        matches!(&result.content[..],
        [pi_ai_core::InputContent::Text(text)] if text.text == "Tool execution was interrupted"),
        "unexpected synthetic result: {result:?}"
    );
    assert!(
        !snapshot
            .usage
            .iter()
            .any(|row| row.entry_id.as_ref() == Some(id))
    );
    assert!(!snapshot.registers.iter().any(|register| matches!(
        register,
        pi_agent_schema::Register::OperationToolArguments { .. }
    )));
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

fn cancel_reason(bytes: &[u8], minor: u32) -> i32 {
    if minor == 0 {
        let mut reader = FrameReader::<v1_0::Envelope>::new(nz(8192));
        let envelope = reader.read_one(bytes).unwrap().frame.unwrap().envelope;
        let Some(v1_0::envelope::Frame::Cancel(cancel)) = envelope.frame else {
            panic!("expected Cancel")
        };
        assert_eq!(cancel.request_id, 1);
        cancel.reason
    } else {
        let mut reader = FrameReader::<v1_1::Envelope>::new(nz(8192));
        let envelope = reader.read_one(bytes).unwrap().frame.unwrap().envelope;
        let Some(v1_1::envelope::Frame::Cancel(cancel)) = envelope.frame else {
            panic!("expected Cancel")
        };
        assert_eq!(cancel.request_id, 1);
        cancel.reason
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

    fn read_future<'a>(
        &'a self,
        buffer: &'a mut [u8],
    ) -> impl Future<Output = Result<Option<usize>, ChannelError>> + Send + 'a {
        poll_fn(move |_| {
            if self.closed.load(Ordering::SeqCst) {
                return Poll::Ready(Err(ChannelError::Closed));
            }
            match self.reads.lock().unwrap().pop_front() {
                Some(Some(bytes)) if bytes.len() <= buffer.len() => {
                    buffer[..bytes.len()].copy_from_slice(&bytes);
                    Poll::Ready(Ok(Some(bytes.len())))
                }
                Some(Some(_)) => Poll::Ready(Err(ChannelError::Failed)),
                Some(None) => Poll::Ready(Ok(None)),
                None => Poll::Pending,
            }
        })
    }
}
impl OrderedByteChannel for FakeChannel {
    fn max_read_scratch_bytes(&self) -> usize {
        let mut empty = [];
        std::mem::size_of_val(&self.read_future(&mut empty))
    }

    fn read_chunk<'a>(
        &'a self,
        buffer: &'a mut [u8],
    ) -> BoxFuture<'a, Result<Option<usize>, ChannelError>> {
        Box::pin(self.read_future(buffer))
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
    on_read: Mutex<Option<Box<dyn FnOnce() + Send>>>,
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
            on_read: Mutex::new(None),
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
    fn on_next_read(&self, callback: impl FnOnce() + Send + 'static) {
        *self.on_read.lock().unwrap() = Some(Box::new(callback));
    }
}
impl MonotonicClock for FakeClock {
    fn now_ms(&self) -> u64 {
        let callback = self.on_read.lock().unwrap().take();
        if let Some(callback) = callback {
            callback();
        }
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
    poll_next_spawn_inline: AtomicBool,
    poll_then_reject_next_spawn: AtomicBool,
}
impl TaskSpawner for FakeSpawner {
    fn spawn(&self, mut future: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
        if self.reject.load(Ordering::SeqCst) {
            return Err(SpawnError::new());
        }
        if self
            .poll_then_reject_next_spawn
            .swap(false, Ordering::SeqCst)
        {
            let _ = poll(future.as_mut());
            return Err(SpawnError::new());
        }
        if !self.poll_next_spawn_inline.swap(false, Ordering::SeqCst)
            || poll(future.as_mut()).is_pending()
        {
            self.tasks.lock().unwrap().push(future);
        }
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
    host_minor_with_progress(generation, minor, false)
}
fn host_minor_with_progress(generation: u64, minor: u32, alter_progress: bool) -> HostAuthority {
    host_minor_with_replay(generation, minor, alter_progress, ToolReplay::Safe)
}
fn host_minor_with_replay(
    generation: u64,
    minor: u32,
    alter_progress: bool,
    replay: ToolReplay,
) -> HostAuthority {
    let catalog: Value = serde_json::from_str(CATALOG).unwrap();
    let entry = &catalog["entries"][0];
    let mut altered = entry["selected_minor_schema_sets"]["1.0"]["progress"]["schema"].clone();
    if alter_progress {
        altered["maxProperties"] = serde_json::json!(16);
    }
    let descriptor = AgentTool {
        name: "echo".into(),
        label: entry["agentTool"]["label"].as_str().unwrap().into(),
        description: entry["agentTool"]["description"].as_str().unwrap().into(),
        parameters: entry["agentTool"]["parameters"].clone(),
        execution_mode: AgentToolMode::Sequential,
        replay,
    };
    let schemas = [0, 1]
        .into_iter()
        .map(|minor| {
            let set = &entry["selected_minor_schema_sets"][format!("1.{minor}")];
            ToolSchemaInputs {
                minor,
                arguments: serde_json::to_vec(&set["arguments"]["schema"]).unwrap(),
                progress: serde_json::to_vec(if alter_progress {
                    &altered
                } else {
                    &set["progress"]["schema"]
                })
                .unwrap(),
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
    setup_with_replay(owner, spawner, generation, now, ToolReplay::Safe)
}
fn setup_with_replay(
    owner: &SingleToolOwner,
    spawner: &FakeSpawner,
    generation: u64,
    now: u64,
    replay: ToolReplay,
) -> (
    Arc<FakeChannel>,
    Arc<FakeClock>,
    pi_harness_runtime::extensions::tool_driver::ReadyToolRoute,
) {
    let channel = Arc::new(FakeChannel::new());
    let clock = Arc::new(FakeClock::new(now));
    channel.push(hello());
    channel.push(manifest_with_replay(replay));
    channel.push(ready_frame());
    let route = spawner
        .drive(owner.connect(
            host_minor_with_replay(generation, 1, false, replay),
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

fn setup_altered_progress(
    owner: &SingleToolOwner,
    spawner: &FakeSpawner,
    generation: u64,
) -> pi_harness_runtime::extensions::tool_driver::ReadyToolRoute {
    let channel = Arc::new(FakeChannel::new());
    let clock = Arc::new(FakeClock::new(200));
    channel.push(hello());
    channel.push(manifest_with_altered_progress());
    channel.push(ready_frame());
    spawner
        .drive(owner.connect(
            host_minor_with_progress(generation, 1, true),
            channel,
            clock,
            PumpConfig {
                max_read_chunk: nz(8192),
                write_timeout_ms: NonZeroU64::new(10).unwrap(),
            },
        ))
        .unwrap()
}

struct RouteDropProbeChannel {
    inner: FakeChannel,
    entered: std::sync::mpsc::Sender<()>,
    release: Mutex<std::sync::mpsc::Receiver<()>>,
}

impl Drop for RouteDropProbeChannel {
    fn drop(&mut self) {
        let _ = self.entered.send(());
        let _ = self
            .release
            .lock()
            .unwrap()
            .recv_timeout(std::time::Duration::from_secs(5));
    }
}

impl OrderedByteChannel for RouteDropProbeChannel {
    fn max_read_scratch_bytes(&self) -> usize {
        self.inner.max_read_scratch_bytes()
    }

    fn read_chunk<'a>(
        &'a self,
        buffer: &'a mut [u8],
    ) -> BoxFuture<'a, Result<Option<usize>, ChannelError>> {
        self.inner.read_chunk(buffer)
    }

    fn write_frame(&self, frame: EncodedFrame) -> BoxFuture<'_, Result<(), ChannelError>> {
        self.inner.write_frame(frame)
    }

    fn close(&self) {
        self.inner.close();
    }
}

#[test]
fn fake_read_declaration_covers_its_boxed_future_payload() {
    let channel = FakeChannel::new();
    let mut empty = [];
    let future = channel.read_chunk(&mut empty);
    let future_bytes = std::mem::size_of_val(future.as_ref().get_ref());
    assert!(future_bytes > 0);
    assert!(channel.max_read_scratch_bytes() >= future_bytes);
}

#[test]
fn retired_route_host_drop_does_not_hold_tools_registry() {
    use pi_agent_schema::{AgentHarness, DriveMode};
    use std::time::Duration;

    for replace_with_g2 in [false, true] {
        let runtime = Setup::new();
        let factory = RuntimeFactory::new(
            runtime.time.clone(),
            runtime.pump.clone(),
            runtime.ids.clone(),
            Arc::new(()),
            Arc::new(Summary),
        );
        let harness = runtime
            .pump
            .drive(factory.create_configured(runtime.options(DriveMode::Automatic)))
            .unwrap()
            .harness;
        let spawner = Arc::new(FakeSpawner::default());
        let driver_owner = owner(spawner.clone());
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let channel = Arc::new(RouteDropProbeChannel {
            inner: FakeChannel::new(),
            entered: entered_tx,
            release: Mutex::new(release_rx),
        });
        let clock = Arc::new(FakeClock::new(200));
        channel.inner.push(hello());
        channel.inner.push(manifest());
        channel.inner.push(ready_frame());
        let g1 = Arc::new(
            spawner
                .drive(driver_owner.connect(
                    host(7),
                    channel.clone(),
                    clock.clone(),
                    PumpConfig {
                        max_read_chunk: nz(8192),
                        write_timeout_ms: NonZeroU64::new(10).unwrap(),
                    },
                ))
                .unwrap(),
        );
        let echo = registry_echo(&g1);
        runtime
            .pump
            .drive(harness.register_ready_echo_tool(echo.clone(), g1.clone(), route_timing()))
            .unwrap()
            .unwrap();
        g1.close();
        drop(g1);
        drop(channel);
        drop(clock);
        spawner.tick();

        let g2 = replace_with_g2.then(|| Arc::new(setup(&driver_owner, &spawner, 8, 210).2));
        let mutation_harness = harness.clone();
        let mutation_pump = runtime.pump.clone();
        let mutation = std::thread::spawn(move || {
            if let Some(g2) = g2 {
                mutation_pump
                    .drive(mutation_harness.register_ready_echo_tool(echo, g2, route_timing()))
                    .unwrap()
                    .unwrap();
            } else {
                mutation_pump
                    .drive(mutation_harness.set_tools(vec![]))
                    .unwrap();
            }
        });
        entered_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("retired route channel reached Drop");
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (got_tx, got_rx) = std::sync::mpsc::channel();
        let read_harness = harness.clone();
        let read_pump = runtime.pump.clone();
        let getter = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            let result = read_pump.drive(read_harness.get_tools());
            got_tx.send(()).unwrap();
            result
        });
        started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let read_completed = got_rx.recv_timeout(Duration::from_millis(100)).is_ok();
        release_tx.send(()).unwrap();
        mutation.join().unwrap();
        getter.join().unwrap().unwrap();
        assert!(
            read_completed,
            "get_tools must complete while retired G1 channel Drop is paused; G2={replace_with_g2}"
        );
        runtime.pump.drive(harness.close()).unwrap();
    }
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
fn route_timing() -> EchoRouteTiming {
    EchoRouteTiming {
        method_limit_ms: NonZeroU64::new(1000).unwrap(),
        connection_deadline_ms: 10_000,
    }
}
fn route_timing_at(deadline: u64) -> EchoRouteTiming {
    EchoRouteTiming {
        connection_deadline_ms: deadline,
        ..route_timing()
    }
}
fn written_request(bytes: &[u8]) -> v1_1::Request {
    let mut reader = FrameReader::<v1_1::Envelope>::new(nz(8192));
    let envelope = reader.read_one(bytes).unwrap().frame.unwrap().envelope;
    let Some(v1_1::envelope::Frame::Request(request)) = envelope.frame else {
        panic!("expected outbound Request")
    };
    request
}
fn saved() -> JsonObject {
    serde_json::from_str(r#"{"text":"ping"}"#).unwrap()
}

type CommitLineCallback = Arc<dyn Fn(&'static str) -> bool + Send + Sync>;
type CommitCloseSlot = Arc<
    Mutex<Option<pi_ai_core::BoxFuture<'static, Result<(), pi_agent_schema::HarnessRuntimeError>>>>,
>;

struct CommitDrainWake {
    pump: Arc<runtime_r1_r4_support::Pump>,
    close: CommitCloseSlot,
    probe: Arc<CommitLineProbe>,
    outcome: Mutex<Option<Result<(), pi_agent_schema::HarnessRuntimeError>>>,
    fired: AtomicBool,
    receipt_before_drain: AtomicBool,
}
impl Wake for CommitDrainWake {
    fn wake(self: Arc<Self>) {
        self.fired.store(true, Ordering::SeqCst);
        self.receipt_before_drain.store(
            self.probe.first_write_done.load(Ordering::SeqCst),
            Ordering::SeqCst,
        );
        self.pump.tick();
        let mut close = self.close.lock().unwrap();
        if let Some(close) = close.as_mut()
            && let Poll::Ready(result) = poll(close.as_mut())
        {
            *self.outcome.lock().unwrap() = Some(result);
        }
    }
}

#[derive(Default)]
struct CommitLineProbe {
    callback: Mutex<Option<CommitLineCallback>>,
    stages: Mutex<Vec<&'static str>>,
    failed_stages: Mutex<Vec<&'static str>>,
    active: AtomicBool,
    first_write_done: AtomicBool,
    failed: AtomicBool,
}
impl CommitLineProbe {
    fn set(&self, callback: CommitLineCallback) {
        *self.callback.lock().unwrap() = Some(callback);
    }
    fn check(&self, stage: &'static str) {
        if !self.active.load(Ordering::SeqCst) {
            return;
        }
        let callback = self.callback.lock().unwrap().clone();
        if let Some(callback) = callback {
            if !callback(stage) {
                self.failed.store(true, Ordering::SeqCst);
                self.failed_stages.lock().unwrap().push(stage);
            }
            self.stages.lock().unwrap().push(stage);
        }
    }
}

struct CommitStorageClock {
    time: Arc<runtime_r1_r4_support::Time>,
    probe: Arc<CommitLineProbe>,
    fail: AtomicBool,
    calls: AtomicU64,
    stale_commit: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    stale_seen: AtomicBool,
}
impl pi_agent_session::memory::StorageClock for CommitStorageClock {
    fn now(&self) -> Result<SafeUnsigned, pi_agent_schema::StorageError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.probe.check("storage_clock");
        let stale_commit = self.stale_commit.lock().unwrap().take();
        if let Some(commit) = stale_commit {
            commit();
            self.stale_seen.store(true, Ordering::SeqCst);
        }
        if self.fail.load(Ordering::SeqCst) {
            return Err(pi_agent_schema::StorageError::Io(
                pi_agent_schema::ErrorContext::new("injected stable storage clock failure")
                    .unwrap(),
            ));
        }
        Ok(SafeUnsigned::new(self.time.0.load(Ordering::SeqCst)).unwrap())
    }
}

#[derive(Clone)]
struct CommitTelemetry {
    records: Arc<Mutex<Vec<CommitSpanRecord>>>,
    probe: Arc<CommitLineProbe>,
    clock: Arc<CommitStorageClock>,
    first_write_clock_calls: Arc<AtomicU64>,
    panic_write_start: Arc<AtomicBool>,
}
#[derive(Clone)]
struct CommitSpanRecord {
    name: String,
    parent: Option<usize>,
    attributes: pi_telemetry::SpanAttributes,
    status: Option<pi_telemetry::SpanStatus>,
    finishes: usize,
    drops: usize,
}
impl CommitTelemetry {
    fn new(probe: Arc<CommitLineProbe>, clock: Arc<CommitStorageClock>) -> Self {
        Self {
            records: Arc::new(Mutex::new(Vec::new())),
            probe,
            clock,
            first_write_clock_calls: Arc::new(AtomicU64::new(u64::MAX)),
            panic_write_start: Arc::new(AtomicBool::new(false)),
        }
    }
    fn start(
        &self,
        parent: Option<usize>,
        options: pi_telemetry::SpanOptions,
    ) -> pi_telemetry::SpanGuard {
        let write = options.name == "pi.session.write" && self.probe.active.load(Ordering::SeqCst);
        if write {
            let _ = self.first_write_clock_calls.compare_exchange(
                u64::MAX,
                self.clock.calls.load(Ordering::SeqCst),
                Ordering::SeqCst,
                Ordering::SeqCst,
            );
            self.probe.check("start_span");
            if self.panic_write_start.swap(false, Ordering::SeqCst) {
                panic!("injected write observer panic");
            }
        }
        let id = {
            let mut records = self.records.lock().unwrap();
            let id = records.len();
            records.push(CommitSpanRecord {
                name: options.name,
                parent,
                attributes: options.attributes,
                status: None,
                finishes: 0,
                drops: 0,
            });
            id
        };
        pi_telemetry::SpanGuard::new(CommitSpan {
            owner: self.clone(),
            id,
            write,
        })
    }
    fn writes(&self) -> Vec<CommitSpanRecord> {
        self.records
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r.name == "pi.session.write")
            .cloned()
            .collect()
    }
}
impl pi_telemetry::TelemetryContext for CommitTelemetry {
    fn start_span(&self, options: pi_telemetry::SpanOptions) -> pi_telemetry::SpanGuard {
        self.start(None, options)
    }
}
struct CommitSpan {
    owner: CommitTelemetry,
    id: usize,
    write: bool,
}
impl pi_telemetry::TelemetrySpan for CommitSpan {
    fn start_child(&self, options: pi_telemetry::SpanOptions) -> pi_telemetry::SpanGuard {
        self.owner.start(Some(self.id), options)
    }
    fn add_event(&self, _: &str, attributes: &pi_telemetry::SpanAttributes) {
        self.set_attributes(attributes);
    }
    fn set_attributes(&self, attributes: &pi_telemetry::SpanAttributes) {
        if self.write {
            self.owner.probe.check("attributes");
        }
        self.owner.records.lock().unwrap()[self.id]
            .attributes
            .extend(attributes.clone());
    }
    fn set_status(&self, status: pi_telemetry::SpanStatus) {
        if self.write {
            self.owner.probe.check("status");
        }
        self.owner.records.lock().unwrap()[self.id].status = Some(status);
    }
    fn finish(&self) {
        if self.write {
            self.owner.probe.check("finish");
        }
        self.owner.records.lock().unwrap()[self.id].finishes += 1;
    }
}
impl Drop for CommitSpan {
    fn drop(&mut self) {
        if self.write {
            self.owner.probe.check("drop");
            self.owner
                .probe
                .first_write_done
                .store(true, Ordering::SeqCst);
            self.owner.probe.active.store(false, Ordering::SeqCst);
        }
        self.owner.records.lock().unwrap()[self.id].drops += 1;
    }
}

struct RegistryTool {
    descriptor: pi_agent_schema::AgentTool,
    drop_pause: Option<Arc<WritePause>>,
    descriptor_probe: Option<Arc<dyn Fn() + Send + Sync>>,
}

struct NativeEcho {
    descriptor: pi_agent_schema::AgentTool,
    entered: Arc<std::sync::atomic::AtomicUsize>,
    complete: bool,
}

struct ResumeProbe(std::sync::atomic::AtomicUsize);
impl pi_agent_schema::HookHandler<pi_agent_schema::BeforeResume> for ResumeProbe {
    fn call(
        &self,
        _: pi_agent_schema::BeforeResumeEvent,
    ) -> pi_agent_schema::BoxFuture<'_, Result<(), pi_agent_schema::HandlerError>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(()) })
    }
}

impl pi_agent_schema::AgentHarnessTool<()> for NativeEcho {
    fn descriptor(&self) -> &pi_agent_schema::AgentTool {
        &self.descriptor
    }

    fn prepare_arguments(
        &self,
        arguments: pi_ai_core::JsonValue,
    ) -> Result<JsonObject, pi_agent_schema::ToolExecutionError> {
        let pi_ai_core::JsonValue::Object(arguments) = arguments else {
            panic!("native echo args")
        };
        Ok(arguments)
    }

    fn execute<'a>(
        &'a self,
        _: pi_ai_core::ToolCallId,
        params: JsonObject,
        _: &'a dyn pi_agent_schema::Cancellation,
        _: &'a dyn pi_agent_schema::ToolProgressSink,
        _: &'a (),
    ) -> pi_agent_schema::BoxFuture<
        'a,
        Result<pi_agent_schema::AgentToolResult, pi_agent_schema::ToolExecutionError>,
    > {
        Box::pin(async move {
            assert_eq!(params, saved());
            self.entered.fetch_add(1, Ordering::SeqCst);
            if !self.complete {
                std::future::pending::<()>().await;
            }
            Ok(pi_agent_schema::AgentToolResult {
                content: vec![],
                details: pi_ai_core::JsonValue::Null,
                usage: None,
                added_tool_names: None,
                terminate: None,
            })
        })
    }
}

fn native_echo(
    complete: bool,
    entered: Arc<std::sync::atomic::AtomicUsize>,
) -> pi_agent_schema::AgentHarnessToolHandle<()> {
    Arc::new(NativeEcho {
        descriptor: registry_echo_declaration(pi_extension_protocol::activation::ToolReplay::Safe)
            .descriptor()
            .clone(),
        entered,
        complete,
    })
}

impl Drop for RegistryTool {
    fn drop(&mut self) {
        if let Some(pause) = &self.drop_pause {
            pause.entered.wait();
            pause.release.wait();
        }
    }
}

impl pi_agent_schema::AgentHarnessTool<()> for RegistryTool {
    fn descriptor(&self) -> &pi_agent_schema::AgentTool {
        if let Some(probe) = &self.descriptor_probe {
            probe();
        }
        &self.descriptor
    }

    fn prepare_arguments(
        &self,
        arguments: pi_ai_core::JsonValue,
    ) -> Result<JsonObject, pi_agent_schema::ToolExecutionError> {
        let pi_ai_core::JsonValue::Object(arguments) = arguments else {
            panic!("registry fixture expects object arguments")
        };
        Ok(arguments)
    }

    fn execute<'a>(
        &'a self,
        _: pi_ai_core::ToolCallId,
        _: JsonObject,
        _: &'a dyn pi_agent_schema::Cancellation,
        _: &'a dyn pi_agent_schema::ToolProgressSink,
        _: &'a (),
    ) -> pi_agent_schema::BoxFuture<
        'a,
        Result<pi_agent_schema::AgentToolResult, pi_agent_schema::ToolExecutionError>,
    > {
        Box::pin(async { panic!("registry test does not execute native tools") })
    }
}

fn registry_tool(name: &str) -> pi_agent_schema::AgentHarnessToolHandle<()> {
    registry_tool_with_pause(name, None)
}

fn registry_tool_with_pause(
    name: &str,
    drop_pause: Option<Arc<WritePause>>,
) -> pi_agent_schema::AgentHarnessToolHandle<()> {
    Arc::new(RegistryTool {
        descriptor: pi_agent_schema::AgentTool {
            name: pi_ai_core::ToolName::new(name).unwrap(),
            label: name.into(),
            description: name.into(),
            parameters: serde_json::from_value(serde_json::json!({"type":"object"})).unwrap(),
            constrained_sampling: None,
            execution_mode: None,
            replay: None,
        },
        drop_pause,
        descriptor_probe: None,
    })
}

fn registry_echo(
    route: &pi_harness_runtime::extensions::tool_driver::ReadyToolRoute,
) -> pi_agent_schema::AgentHarnessToolHandle<()> {
    registry_echo_with_probe(route, None)
}

fn registry_echo_declaration(replay: ToolReplay) -> pi_agent_schema::AgentHarnessToolHandle<()> {
    let catalog: Value = serde_json::from_str(CATALOG).unwrap();
    let entry = &catalog["entries"][0]["agentTool"];
    Arc::new(RegistryTool {
        descriptor: pi_agent_schema::AgentTool {
            name: pi_ai_core::ToolName::new("echo").unwrap(),
            label: entry["label"].as_str().unwrap().into(),
            description: entry["description"].as_str().unwrap().into(),
            parameters: serde_json::from_value(entry["parameters"].clone()).unwrap(),
            constrained_sampling: None,
            execution_mode: Some(pi_agent_schema::ToolExecutionMode::Sequential),
            replay: Some(match replay {
                ToolReplay::Safe => pi_agent_schema::ToolReplay::Safe,
                ToolReplay::Never => pi_agent_schema::ToolReplay::Never,
            }),
        },
        drop_pause: None,
        descriptor_probe: None,
    })
}

fn registry_echo_with_probe(
    route: &pi_harness_runtime::extensions::tool_driver::ReadyToolRoute,
    descriptor_probe: Option<Arc<dyn Fn() + Send + Sync>>,
) -> pi_agent_schema::AgentHarnessToolHandle<()> {
    let granted = route.grant().tools()[0].descriptor();
    Arc::new(RegistryTool {
        descriptor: pi_agent_schema::AgentTool {
            name: pi_ai_core::ToolName::new(&granted.name).unwrap(),
            label: granted.label.clone(),
            description: granted.description.clone(),
            parameters: serde_json::from_value(granted.parameters.clone()).unwrap(),
            constrained_sampling: None,
            execution_mode: Some(match granted.execution_mode {
                AgentToolMode::Sequential => pi_agent_schema::ToolExecutionMode::Sequential,
                AgentToolMode::Parallel => pi_agent_schema::ToolExecutionMode::Parallel,
            }),
            replay: Some(match granted.replay {
                ToolReplay::Safe => pi_agent_schema::ToolReplay::Safe,
                ToolReplay::Never => pi_agent_schema::ToolReplay::Never,
            }),
        },
        drop_pause: None,
        descriptor_probe,
    })
}

#[test]
fn actual_memory_session_claims_ready_echo_after_durable_args_and_settles_terminal() {
    use pi_agent_schema::{OperationState, RegisterNamespace, RunPhase, ToolCallState};
    use pi_ai_core::{AssistantContent, StopReason, ToolCall, ToolCallId, ToolName};
    use prost::Message as _;
    use runtime_r1_r4_support::assistant;

    let runtime = Setup::new();
    let mut tool_response = assistant(&runtime.faux.model, StopReason::ToolUse);
    tool_response.content = vec![AssistantContent::ToolCall(ToolCall {
        content_type: Default::default(),
        id: ToolCallId::new("echo-call").unwrap(),
        name: ToolName::new("echo").unwrap(),
        arguments: saved(),
        thought_signature: None,
        namespace: None,
    })];
    *runtime.faux.responses.lock().unwrap() = vec![
        tool_response,
        assistant(&runtime.faux.model, StopReason::Stop),
    ]
    .into();
    let factory = RuntimeFactory::new(
        runtime.time.clone(),
        runtime.pump.clone(),
        runtime.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    let harness = runtime
        .pump
        .drive(factory.create_configured(runtime.options::<()>(DriveMode::Automatic)))
        .unwrap()
        .harness;
    let spawner = Arc::new(FakeSpawner::default());
    let driver_owner = owner(spawner.clone());
    let (channel, _, route) = setup(&driver_owner, &spawner, 7, 200);
    let route = Arc::new(route);
    let descriptor_armed = Arc::new(AtomicBool::new(false));
    let descriptor_all_lane_free = Arc::new(AtomicBool::new(true));
    let descriptor_calls = Arc::new(AtomicU64::new(0));
    let probe_harness = harness.clone();
    let probe = {
        let armed = descriptor_armed.clone();
        let all_lane_free = descriptor_all_lane_free.clone();
        let calls = descriptor_calls.clone();
        Arc::new(move || {
            if armed.load(Ordering::SeqCst) {
                calls.fetch_add(1, Ordering::SeqCst);
                let mut reenter = Box::pin(probe_harness.get_leaf_id());
                all_lane_free.fetch_and(
                    matches!(poll(reenter.as_mut()), Poll::Ready(Ok(_))),
                    Ordering::SeqCst,
                );
            }
        })
    };
    let echo = registry_echo_with_probe(&route, Some(probe));
    runtime
        .pump
        .drive(harness.register_ready_echo_tool(echo, route, route_timing()))
        .unwrap()
        .unwrap();
    runtime
        .pump
        .drive(harness.set_active_tools(vec![ToolName::new("echo").unwrap()]))
        .unwrap();

    let mut prompt = Box::pin(harness.prompt(PromptInput::Text {
        text: "invoke echo".into(),
        images: vec![],
    }));
    let mut request = None;
    for _ in 0..1000 {
        let _ = poll(prompt.as_mut());
        runtime.pump.tick();
        spawner.tick();
        if let Some(bytes) = channel.writes().get(2) {
            request = Some(written_request(bytes));
            break;
        }
    }
    let request = request.expect("Ready echo Request must be written");
    assert_eq!(request.method, "tool.invoke");
    assert_eq!(request.initial_stream_credit_bytes, Some(0));
    let snapshot = runtime.pump.drive(runtime.store.snapshot()).unwrap();
    assert!(snapshot.registers.iter().any(|register| matches!(register,
        Register::OperationToolArguments { value, .. } if value == &saved())));
    let Some(Register::LaneState { value: lane, .. }) = runtime
        .pump
        .drive(
            runtime
                .session
                .get_register(RegisterNamespace::LaneState, "main"),
        )
        .unwrap()
    else {
        panic!("pending lane")
    };
    let operation = lane.current_operation_id.expect("pending operation");
    let Some(Register::OperationState { value: state, .. }) = runtime
        .pump
        .drive(
            runtime
                .session
                .get_register(RegisterNamespace::OperationState, operation.as_str()),
        )
        .unwrap()
    else {
        panic!("pending state")
    };
    let OperationState::Run(run) = state.as_ref() else {
        panic!("run")
    };
    let RunPhase::Tools(tools) = &run.phase else {
        panic!("tools")
    };
    assert!(matches!(
        &tools.batch.calls[0],
        ToolCallState::EffectPending { .. }
    ));
    assert_eq!(
        request.effect_key,
        Some(v1_1::EffectKey {
            session_id: "runtime-session".into(),
            lane: "main".into(),
            operation_id: operation.as_str().into(),
            effect_kind: "tool".into(),
            step_or_task_id: tools.batch.turn_id.as_str().into(),
            logical_attempt: 0,
            item_index: Some(0),
        })
    );
    assert!(snapshot.registers.iter().any(|register| matches!(register,
        Register::OperationToolArguments { key, value, .. }
            if key == &format!("{}:{}:0", operation.as_str(), tools.batch.turn_id.as_str())
                && value == &saved())));
    let invocation = v1_1::InvokeToolRequest::decode(request.payload.as_slice()).unwrap();
    assert_eq!(invocation.tool_call_id, "echo-call");
    assert_eq!(invocation.tool_id, "echo");
    let Some(v1_1::wire_value::Value::MapValue(arguments)) = invocation
        .arguments
        .and_then(|bound| bound.value)
        .and_then(|value| value.value)
    else {
        panic!("selected wire arguments")
    };
    assert!(arguments.entries.iter().any(|entry| entry.key == "text"
        && matches!(&entry.value,
            Some(v1_1::WireValue { value: Some(v1_1::wire_value::Value::TextValue(value)) })
                if value == "ping")));

    descriptor_armed.store(true, Ordering::SeqCst);
    channel.push(raw_terminal_with_usage(request.request_id));
    let mut completed = None;
    for _ in 0..1000 {
        spawner.tick();
        runtime.pump.tick();
        if let Poll::Ready(result) = poll(prompt.as_mut()) {
            completed = Some(result);
            break;
        }
    }
    completed
        .expect("terminal must finish the Memory run")
        .unwrap()
        .unwrap();
    assert!(descriptor_calls.load(Ordering::SeqCst) > 0);
    assert!(
        descriptor_all_lane_free.load(Ordering::SeqCst),
        "application descriptor getter must run after routed pair release"
    );
    let snapshot = runtime.pump.drive(runtime.store.snapshot()).unwrap();
    let result_entry = snapshot
        .entries
        .iter()
        .find_map(|entry| match entry {
            pi_agent_schema::Entry::Message(message)
                if matches!(&message.message,
            pi_agent_schema::AgentMessage::BuiltIn(pi_ai_core::Message::ToolResult(result))
                if result.tool_call_id.as_str() == "echo-call" && !result.is_error) =>
            {
                Some(message)
            }
            _ => None,
        })
        .expect("one peer ToolResult");
    let linked: Vec<_> = snapshot
        .usage
        .iter()
        .filter(|row| row.entry_id.as_ref() == Some(&result_entry.id))
        .collect();
    assert_eq!(linked.len(), 1);
    assert_eq!(linked[0].usage.input.get(), 5);
    assert!(
        !snapshot
            .registers
            .iter()
            .any(|register| matches!(register, Register::OperationToolArguments { .. }))
    );
}

fn commit_observer_memory_route(
    fail_clock: bool,
    stale_first: bool,
    panic_observer: bool,
    fault_cleanup: bool,
) {
    use pi_agent_schema::{
        HarnessRuntimeError, RegisterNamespace, RegisterSet, Transaction, Write,
    };
    use pi_ai_core::{AssistantContent, StopReason, ToolCall, ToolCallId, ToolName};
    use runtime_r1_r4_support::assistant;

    let probe = Arc::new(CommitLineProbe::default());
    let clock_slot: Arc<Mutex<Option<Arc<CommitStorageClock>>>> = Arc::new(Mutex::new(None));
    let runtime = Setup::with_storage_clock({
        let probe = probe.clone();
        let slot = clock_slot.clone();
        move |time| {
            let clock = Arc::new(CommitStorageClock {
                time,
                probe,
                fail: AtomicBool::new(false),
                calls: AtomicU64::new(0),
                stale_commit: Mutex::new(None),
                stale_seen: AtomicBool::new(false),
            });
            *slot.lock().unwrap() = Some(clock.clone());
            clock
        }
    });
    let clock = clock_slot.lock().unwrap().take().unwrap();
    let telemetry = Arc::new(CommitTelemetry::new(probe.clone(), clock.clone()));
    let mut response = assistant(&runtime.faux.model, StopReason::ToolUse);
    response.content = vec![AssistantContent::ToolCall(ToolCall {
        content_type: Default::default(),
        id: ToolCallId::new("echo-call").unwrap(),
        name: ToolName::new("echo").unwrap(),
        arguments: saved(),
        thought_signature: None,
        namespace: None,
    })];
    *runtime.faux.responses.lock().unwrap() =
        vec![response, assistant(&runtime.faux.model, StopReason::Stop)].into();
    let factory = RuntimeFactory::new(
        runtime.time.clone(),
        runtime.pump.clone(),
        runtime.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    let mut options = runtime.options::<()>(DriveMode::Automatic);
    options.telemetry_context = telemetry.clone();
    let harness = runtime
        .pump
        .drive(factory.create_configured(options))
        .unwrap()
        .harness;
    let spawner = Arc::new(FakeSpawner::default());
    let driver_owner = owner(spawner.clone());
    let (channel, _, route) = setup(&driver_owner, &spawner, 7, 200);
    let route = Arc::new(route);
    let echo = registry_echo(&route);
    runtime
        .pump
        .drive(harness.register_ready_echo_tool(echo.clone(), route, route_timing()))
        .unwrap()
        .unwrap();
    runtime
        .pump
        .drive(harness.set_active_tools(vec![ToolName::new("echo").unwrap()]))
        .unwrap();
    let clock_settings = Arc::new(AtomicBool::new(!stale_first));
    let close_slot: CommitCloseSlot = Arc::new(Mutex::new(None));
    let drain_wake = Arc::new(CommitDrainWake {
        pump: runtime.pump.clone(),
        close: close_slot.clone(),
        probe: probe.clone(),
        outcome: Mutex::new(None),
        fired: AtomicBool::new(false),
        receipt_before_drain: AtomicBool::new(false),
    });
    probe.set({
        let harness = harness.clone();
        let echo = echo.clone();
        let clock_settings = clock_settings.clone();
        let close_slot = close_slot.clone();
        let drain_wake = drain_wake.clone();
        let driver_owner = driver_owner.clone();
        let store = runtime.store.clone();
        Arc::new(move |stage| {
            if fault_cleanup && stage != "start_span" && stage != "storage_clock" {
                return true; // Close is sealed; the earlier callback probed both free lines.
            }
            let mut lane = Box::pin(harness.get_leaf_id());
            let lane_free = matches!(poll(lane.as_mut()), Poll::Ready(Ok(_)));
            let check_settings = match stage {
                "storage_clock" => clock_settings.swap(false, Ordering::SeqCst),
                _ => true,
            };
            let settings_free = if check_settings {
                let mut settings = Box::pin(harness.set_tools(vec![echo.clone()]));
                matches!(poll(settings.as_mut()), Poll::Ready(Ok(())))
            } else {
                true
            };
            if (panic_observer || fault_cleanup) && stage == "start_span" {
                if fault_cleanup {
                    use pi_extension_protocol::tool_live::{LivePhase, RetireReason};
                    let snapshot = drain_wake.pump.drive(store.snapshot()).unwrap();
                    assert!(snapshot.entries.iter().any(|entry| matches!(entry,
                        pi_agent_schema::Entry::Message(message) if matches!(&message.message,
                            pi_agent_schema::AgentMessage::BuiltIn(pi_ai_core::Message::ToolResult(_))))));
                    let binding = driver_owner.retained_binding().expect("retained exact B1");
                    assert_eq!(
                        driver_owner.binding_phase(&binding),
                        Ok(LivePhase::TerminalReceived)
                    );
                    driver_owner
                        .retire(&binding, RetireReason::NoLongerExpected)
                        .unwrap();
                }
                let owned = harness.clone();
                let mut close: pi_ai_core::BoxFuture<'static, Result<(), HarnessRuntimeError>> =
                    Box::pin(async move { owned.close().await });
                assert!(
                    poll(close.as_mut()).is_pending(),
                    "Close waits for admitted commit"
                );
                *close_slot.lock().unwrap() = Some(close);
                if fault_cleanup {
                    let waker = Waker::from(drain_wake.clone());
                    drain_wake.pump.tick_latest_with_waker(&waker);
                    assert!(!drain_wake.fired.load(Ordering::SeqCst));
                }
            }
            lane_free && settings_free
        })
    });
    let mut prompt = Box::pin(harness.prompt(PromptInput::Text {
        text: "invoke echo".into(),
        images: vec![],
    }));
    let mut request = None;
    for _ in 0..1000 {
        let _ = poll(prompt.as_mut());
        runtime.pump.tick();
        spawner.tick();
        if let Some(bytes) = channel.writes().get(2) {
            request = Some(written_request(bytes));
            break;
        }
    }
    let request = request.expect("echo Request");
    if stale_first {
        let Some(Register::LaneState { value: lane, .. }) = runtime
            .pump
            .drive(
                runtime
                    .session
                    .get_register(RegisterNamespace::LaneState, "main"),
            )
            .unwrap()
        else {
            panic!("pending lane")
        };
        let operation = lane.current_operation_id.expect("pending operation");
        let Some(Register::OperationState { key, value, .. }) = runtime
            .pump
            .drive(
                runtime
                    .session
                    .get_register(RegisterNamespace::OperationState, operation.as_str()),
            )
            .unwrap()
        else {
            panic!("pending operation state")
        };
        let session = runtime.session.clone();
        let pump = runtime.pump.clone();
        *clock.stale_commit.lock().unwrap() = Some(Box::new(move || {
            pump.drive(session.commit(Transaction {
                writes: vec![Write::SetRegister(Box::new(RegisterSet::OperationState {
                    key,
                    value,
                }))],
            }))
            .unwrap();
        }));
    }
    let before = runtime.pump.drive(runtime.store.snapshot()).unwrap();
    let baseline_writes = telemetry.writes().len();
    let baseline_clock = clock.calls.load(Ordering::SeqCst);
    probe.active.store(true, Ordering::SeqCst);
    clock.fail.store(fail_clock, Ordering::SeqCst);
    telemetry
        .panic_write_start
        .store(panic_observer, Ordering::SeqCst);
    channel.push(raw_terminal_with_usage(request.request_id));
    let mut completion = None;
    let mut panicked = false;
    for _ in 0..2000 {
        let step = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            runtime.pump.tick();
            spawner.tick();
            poll(prompt.as_mut())
        }));
        match step {
            Err(_) => {
                panicked = true;
                break;
            }
            Ok(Poll::Ready(value)) => {
                completion = Some(value);
                break;
            }
            Ok(Poll::Pending) => {}
        }
    }
    if panic_observer {
        assert!(
            panicked || matches!(completion, Some(Err(_))),
            "injected write observer panic reaches the task boundary"
        );
        assert!(!telemetry.panic_write_start.load(Ordering::SeqCst));
        let mut close = close_slot
            .lock()
            .unwrap()
            .take()
            .expect("Close began in observer");
        let mut closed = None;
        for _ in 0..2000 {
            if let Poll::Ready(result) = poll(close.as_mut()) {
                closed = Some(result);
                break;
            }
            runtime.pump.tick();
        }
        assert!(matches!(
            closed.expect("Close drain wakes after observer panic"),
            Err(HarnessRuntimeError::Fault(_))
        ));
        assert!(!probe.failed.load(Ordering::SeqCst));
        assert_eq!(channel.writes().len(), 3, "no second B1 Request");
        return;
    }
    if fault_cleanup {
        assert!(matches!(
            completion,
            Some(Err(HarnessRuntimeError::Fault(_)))
        ));
        assert!(drain_wake.fired.load(Ordering::SeqCst));
        assert!(drain_wake.receipt_before_drain.load(Ordering::SeqCst));
        assert!(matches!(
            drain_wake.outcome.lock().unwrap().take(),
            Some(Err(HarnessRuntimeError::Fault(_)))
        ));
        assert!(!probe.failed.load(Ordering::SeqCst));
        assert_eq!(channel.writes().len(), 3, "no second B1 Request");
        assert_eq!(
            telemetry.first_write_clock_calls.load(Ordering::SeqCst) - baseline_clock,
            2
        );
        return;
    }
    let completion = completion.expect("terminal settlement finishes");
    assert!(
        !probe.failed.load(Ordering::SeqCst),
        "clock/telemetry callback saw held settings or lane at {:?}",
        *probe.failed_stages.lock().unwrap()
    );
    let stages = probe.stages.lock().unwrap().clone();
    assert!(stages.contains(&"storage_clock"));
    assert!(stages.contains(&"start_span"));
    assert!(stages.contains(&"status"));
    assert!(stages.contains(&"finish"));
    assert!(stages.contains(&"drop"));
    assert!(probe.first_write_done.load(Ordering::SeqCst));
    assert_eq!(
        telemetry.first_write_clock_calls.load(Ordering::SeqCst) - baseline_clock,
        if fail_clock {
            1
        } else if stale_first {
            3 // first preparation, independent Session write, retried preparation
        } else {
            2 // first preparation, then registry-change retry
        },
        "only outside-pair preparation may sample the Memory storage clock",
    );
    assert_eq!(clock.stale_seen.load(Ordering::SeqCst), stale_first);
    assert!(clock.calls.load(Ordering::SeqCst) > baseline_clock);
    let writes = telemetry.writes();
    let new_writes = &writes[baseline_writes..];
    let write = new_writes
        .first()
        .expect("one ToolResult write observation");
    assert!(
        write.parent.is_some(),
        "write span keeps the original Harness parent"
    );
    assert_eq!(write.finishes, 1);
    assert_eq!(write.drops, 1);
    assert!(matches!(
        write.attributes.get("pi.session.item_count"),
        Some(pi_telemetry::AttributeValue::Number(_))
    ));
    assert!(matches!(
        write.attributes.get("pi.session.item_kinds"),
        Some(pi_telemetry::AttributeValue::Strings(_))
    ));
    if fail_clock {
        assert!(matches!(completion, Err(HarnessRuntimeError::Fault(_))));
        assert_eq!(
            new_writes.len(),
            1,
            "stable clock error emits one failed write span"
        );
        assert!(matches!(
            write.status,
            Some(pi_telemetry::SpanStatus::Error { error: None })
        ));
        assert!(!write.attributes.contains_key("first_seq"));
        assert_eq!(
            runtime.pump.drive(runtime.store.snapshot()).unwrap(),
            before,
            "clock failure writes nothing"
        );
        assert_eq!(channel.writes().len(), 3, "no second B1 Request");
    } else {
        completion.unwrap().unwrap();
        assert_eq!(channel.writes().len(), 3, "no second B1 Request");
        assert_eq!(write.status, Some(pi_telemetry::SpanStatus::Ok));
        assert!(write.attributes.contains_key("first_seq"));
        assert!(runtime.pump.drive(runtime.store.snapshot()).unwrap().entries.iter().any(|entry|
            matches!(entry, pi_agent_schema::Entry::Message(message)
                if matches!(&message.message, pi_agent_schema::AgentMessage::BuiltIn(pi_ai_core::Message::ToolResult(_))))));
        assert!(
            runtime
                .pump
                .drive(
                    runtime
                        .session
                        .list_registers(RegisterNamespace::OperationToolArguments, None)
                )
                .unwrap()
                .is_empty()
        );
    }
}

#[test]
fn actual_memory_prepared_clock_and_write_observer_reenter_after_pair_release() {
    commit_observer_memory_route(false, false, false, false);
}

#[test]
fn actual_memory_stable_preparation_clock_failure_has_one_error_span() {
    commit_observer_memory_route(true, false, false, false);
}

#[test]
fn actual_memory_stale_preparation_replans_without_span_or_b1_redispatch() {
    commit_observer_memory_route(false, true, false, false);
}

#[test]
fn actual_memory_observer_panic_releases_last_commit_for_close_drain() {
    commit_observer_memory_route(false, false, true, false);
}

#[test]
fn actual_memory_cleanup_fault_precedes_last_commit_close_drain() {
    commit_observer_memory_route(false, false, false, true);
}

#[test]
fn actual_memory_peer_loss_preserves_safe_pending_key_until_g2_resume() {
    use pi_agent_schema::HarnessRuntimeError;
    use pi_ai_core::{AssistantContent, StopReason, ToolCall, ToolCallId, ToolName};
    use runtime_r1_r4_support::assistant;

    let runtime = Setup::new();
    let mut response = assistant(&runtime.faux.model, StopReason::ToolUse);
    response.content = vec![AssistantContent::ToolCall(ToolCall {
        content_type: Default::default(),
        id: ToolCallId::new("echo-call").unwrap(),
        name: ToolName::new("echo").unwrap(),
        arguments: saved(),
        thought_signature: None,
        namespace: None,
    })];
    *runtime.faux.responses.lock().unwrap() =
        vec![response, assistant(&runtime.faux.model, StopReason::Stop)].into();
    let factory = RuntimeFactory::new(
        runtime.time.clone(),
        runtime.pump.clone(),
        runtime.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    let harness = runtime
        .pump
        .drive(factory.create_configured(runtime.options::<()>(DriveMode::Automatic)))
        .unwrap()
        .harness;
    let spawner = Arc::new(FakeSpawner::default());
    let driver_owner = owner(spawner.clone());
    let (g1_channel, _, g1) = setup(&driver_owner, &spawner, 7, 200);
    let g1 = Arc::new(g1);
    let echo = registry_echo(&g1);
    runtime
        .pump
        .drive(harness.register_ready_echo_tool(echo.clone(), g1, route_timing()))
        .unwrap()
        .unwrap();
    runtime
        .pump
        .drive(harness.set_active_tools(vec![ToolName::new("echo").unwrap()]))
        .unwrap();

    let mut prompt = Box::pin(harness.prompt(PromptInput::Text {
        text: "invoke echo".into(),
        images: vec![],
    }));
    let mut g1_request = None;
    for _ in 0..1000 {
        let _ = poll(prompt.as_mut());
        runtime.pump.tick();
        spawner.tick();
        if let Some(bytes) = g1_channel.writes().get(2) {
            g1_request = Some(written_request(bytes));
            break;
        }
    }
    let g1_request = g1_request.expect("G1 Request");
    let pending = runtime.pump.drive(runtime.store.snapshot()).unwrap();
    assert!(pending.registers.iter().any(|register| matches!(register,
        Register::OperationToolArguments { value, .. } if value == &saved())));
    g1_channel.eof();
    let mut lost = None;
    for _ in 0..1000 {
        spawner.tick();
        runtime.pump.tick();
        if let Poll::Ready(value) = poll(prompt.as_mut()) {
            lost = Some(value);
            break;
        }
    }
    assert!(matches!(
        lost.expect("G1 loss must return"),
        Err(HarnessRuntimeError::PeerLost)
    ));
    let after_loss = runtime.pump.drive(runtime.store.snapshot()).unwrap();
    assert!(
        after_loss
            .registers
            .iter()
            .any(|register| matches!(register,
        Register::OperationToolArguments { value, .. } if value == &saved()))
    );
    assert!(!after_loss.entries.iter().any(|entry| matches!(entry,
        pi_agent_schema::Entry::Message(message) if matches!(&message.message,
            pi_agent_schema::AgentMessage::BuiltIn(pi_ai_core::Message::ToolResult(_))))));
    assert!(matches!(
        runtime.pump.drive(harness.resume()),
        Err(HarnessRuntimeError::PeerLost)
    ));
    assert_eq!(g1_channel.writes().len(), 3);

    let (g2_channel, _, g2) = setup(&driver_owner, &spawner, 8, 210);
    runtime
        .pump
        .drive(harness.register_ready_echo_tool(echo, Arc::new(g2), route_timing()))
        .unwrap()
        .unwrap();
    let mut resume = Box::pin(harness.resume());
    let mut g2_request = None;
    for _ in 0..1000 {
        let _ = poll(resume.as_mut());
        runtime.pump.tick();
        spawner.tick();
        if let Some(bytes) = g2_channel.writes().get(2) {
            g2_request = Some(written_request(bytes));
            break;
        }
    }
    let g2_request = g2_request.expect("G2 Request");
    assert_eq!(g1_request.effect_key, g2_request.effect_key);
    assert_eq!(g2_request.request_id, 1);
    g2_channel.push(raw("terminal", g2_request.request_id as u8));
    let mut completed = None;
    for _ in 0..1000 {
        spawner.tick();
        runtime.pump.tick();
        if let Poll::Ready(value) = poll(resume.as_mut()) {
            completed = Some(value);
            break;
        }
    }
    completed
        .expect("G2 terminal must settle")
        .unwrap()
        .unwrap();
    let final_state = runtime.pump.drive(runtime.store.snapshot()).unwrap();
    assert_eq!(
        final_state
            .entries
            .iter()
            .filter(|entry| matches!(entry,
        pi_agent_schema::Entry::Message(message) if matches!(&message.message,
            pi_agent_schema::AgentMessage::BuiltIn(pi_ai_core::Message::ToolResult(_)))))
            .count(),
        1
    );
    assert!(
        !final_state
            .registers
            .iter()
            .any(|register| matches!(register, Register::OperationToolArguments { .. }))
    );
}

#[test]
fn actual_memory_preclaim_changed_arguments_fault_before_request() {
    preclaim_arguments_race(true);
}

#[test]
fn actual_memory_preclaim_same_arguments_new_seq_fault_before_request() {
    preclaim_arguments_race(false);
}

fn preclaim_arguments_race(replace_value: bool) {
    use pi_agent_schema::{
        HarnessRuntimeError, ProviderHeaderPolicy, RegisterNamespace, RegisterSet,
        SessionCodecOptions, Transaction, Write,
    };
    use pi_ai_core::{AssistantContent, StopReason, ToolCall, ToolCallId, ToolName};
    use runtime_r1_r4_support::assistant;
    use runtime_r6_retirement_support::{AfterRawStamp, Tool};

    let runtime = Setup::new();
    let storage = Arc::new(AfterRawStamp::for_checked_settlement(
        runtime.store.clone(),
        Tool::new(),
    ));
    let session = Arc::new(
        runtime
            .pump
            .drive(RuntimeSession::new(
                storage.clone(),
                runtime.session.metadata(),
                Arc::new(
                    SessionCodec::new(
                        SessionCodecOptions::default(),
                        ProviderHeaderPolicy::default(),
                    )
                    .unwrap(),
                ),
                runtime.ids.clone(),
                Arc::new(pi_telemetry::NOOP_TELEMETRY_CONTEXT),
            ))
            .unwrap(),
    );
    let mut response = assistant(&runtime.faux.model, StopReason::ToolUse);
    response.content = vec![AssistantContent::ToolCall(ToolCall {
        content_type: Default::default(),
        id: ToolCallId::new("echo-call").unwrap(),
        name: ToolName::new("echo").unwrap(),
        arguments: saved(),
        thought_signature: None,
        namespace: None,
    })];
    *runtime.faux.responses.lock().unwrap() =
        vec![response, assistant(&runtime.faux.model, StopReason::Stop)].into();
    let factory = RuntimeFactory::new(
        runtime.time.clone(),
        runtime.pump.clone(),
        runtime.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    let harness = runtime
        .pump
        .drive(factory.create_configured(runtime.options_for::<(), _>(session, DriveMode::Manual)))
        .unwrap()
        .harness;
    let spawner = Arc::new(FakeSpawner::default());
    let driver_owner = owner(spawner.clone());
    let (channel, _, route) = setup(&driver_owner, &spawner, 7, 200);
    let route = Arc::new(route);
    let echo = registry_echo(&route);
    runtime
        .pump
        .drive(harness.register_ready_echo_tool(echo.clone(), route, route_timing()))
        .unwrap()
        .unwrap();
    runtime
        .pump
        .drive(harness.set_active_tools(vec![ToolName::new("echo").unwrap()]))
        .unwrap();
    let after = Arc::new(runtime_r6_retirement_support::AfterToolCount::default());
    let _after_subscription = harness.hooks().after_tool(None, after.clone()).unwrap();
    let ends = Arc::new(RegistryEventLog(Mutex::new(vec![])));
    let _end_subscription = harness.events().on(HarnessEventType::ToolEnd, ends.clone());
    let mut prompt = Box::pin(harness.prompt(PromptInput::Text {
        text: "invoke echo".into(),
        images: vec![],
    }));
    let mut parked = false;
    for _ in 0..2000 {
        let _ = poll(prompt.as_mut());
        runtime.pump.tick();
        spawner.tick();
        if runtime
            .pump
            .drive(harness.peek_action())
            .unwrap()
            .is_some_and(|a| a.kind == "effect_start")
            && runtime
                .pump
                .drive(
                    runtime
                        .session
                        .list_registers(RegisterNamespace::OperationToolArguments, None),
                )
                .unwrap()
                .len()
                == 1
        {
            parked = true;
            break;
        }
        if runtime.pump.drive(harness.peek_action()).unwrap().is_some() {
            runtime.pump.drive(harness.execute_action()).unwrap();
        }
    }
    assert!(parked, "durable K with saved A reached effect-start gate");
    storage.args_read_target.store(2, Ordering::SeqCst);
    runtime.pump.drive(harness.execute_action()).unwrap();
    for _ in 0..1000 {
        runtime.pump.tick();
        spawner.tick();
        if storage.args_read_entered.is_cancelled() {
            break;
        }
    }
    assert!(
        storage.args_read_entered.is_cancelled(),
        "prepared read of A parked"
    );
    let mut args = runtime
        .pump
        .drive(
            runtime
                .session
                .list_registers(RegisterNamespace::OperationToolArguments, None),
        )
        .unwrap();
    let Register::OperationToolArguments { key, .. } = args.pop().expect("saved A") else {
        panic!("saved argument register")
    };
    runtime
        .pump
        .drive(runtime.session.commit(Transaction {
            writes: vec![Write::SetRegister(Box::new(
                RegisterSet::OperationToolArguments {
                    key,
                    value: if replace_value {
                        serde_json::from_str(r#"{"text":"changed"}"#).unwrap()
                    } else {
                        saved()
                    },
                },
            ))],
        }))
        .unwrap();
    let before = runtime.pump.drive(runtime.store.snapshot()).unwrap();
    storage.args_read_release.cancel();
    let mut completion = None;
    for _ in 0..2000 {
        runtime.pump.tick();
        spawner.tick();
        if let Poll::Ready(result) = poll(prompt.as_mut()) {
            completion = Some(result);
            break;
        }
        if runtime.pump.drive(harness.peek_action()).unwrap().is_some() {
            runtime.pump.drive(harness.execute_action()).unwrap();
        }
    }
    assert_eq!(
        channel.writes().len(),
        2,
        "no Request after saved arguments or sequence changes"
    );
    assert!(matches!(
        completion,
        Some(Err(HarnessRuntimeError::Fault(_)))
    ));
    assert_eq!(
        runtime.pump.drive(runtime.store.snapshot()).unwrap(),
        before,
        "fault makes no new durable write"
    );
    assert_eq!(after.0.load(Ordering::SeqCst), 0, "no after_tool");
    assert!(ends.0.lock().unwrap().is_empty(), "no ToolEnd");
}

fn checked_refusal_memory_route(change_args: bool, retire_binding: bool, fault_after_commit: bool) {
    use pi_agent_schema::{
        ProviderHeaderPolicy, RegisterNamespace, RegisterSet, SessionCodecOptions, Transaction,
        Write,
    };
    use pi_ai_core::{AssistantContent, StopReason, ToolCall, ToolCallId, ToolName};
    use runtime_r1_r4_support::assistant;
    use runtime_r6_retirement_support::{AfterRawStamp, Tool};

    let runtime = Setup::new();
    let observation_probe = Arc::new(CommitLineProbe::default());
    let observation_clock = Arc::new(CommitStorageClock {
        time: runtime.time.clone(),
        probe: observation_probe.clone(),
        fail: AtomicBool::new(false),
        calls: AtomicU64::new(0),
        stale_commit: Mutex::new(None),
        stale_seen: AtomicBool::new(false),
    });
    let telemetry = Arc::new(CommitTelemetry::new(
        observation_probe.clone(),
        observation_clock,
    ));
    let storage = Arc::new(AfterRawStamp::for_checked_settlement(
        runtime.store.clone(),
        Tool::new(),
    ));
    let session = Arc::new(
        runtime
            .pump
            .drive(RuntimeSession::new(
                storage.clone(),
                runtime.session.metadata(),
                Arc::new(
                    SessionCodec::new(
                        SessionCodecOptions::default(),
                        ProviderHeaderPolicy::default(),
                    )
                    .unwrap(),
                ),
                runtime.ids.clone(),
                Arc::new(pi_telemetry::NOOP_TELEMETRY_CONTEXT),
            ))
            .unwrap(),
    );
    let mut response = assistant(&runtime.faux.model, StopReason::ToolUse);
    response.content = vec![AssistantContent::ToolCall(ToolCall {
        content_type: Default::default(),
        id: ToolCallId::new("echo-call").unwrap(),
        name: ToolName::new("echo").unwrap(),
        arguments: saved(),
        thought_signature: None,
        namespace: None,
    })];
    *runtime.faux.responses.lock().unwrap() =
        vec![response, assistant(&runtime.faux.model, StopReason::Stop)].into();
    let factory = RuntimeFactory::new(
        runtime.time.clone(),
        runtime.pump.clone(),
        runtime.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    let mut options = runtime.options_for::<(), _>(session, DriveMode::Automatic);
    options.telemetry_context = telemetry.clone();
    let harness = runtime
        .pump
        .drive(factory.create_configured(options))
        .unwrap()
        .harness;
    let spawner = Arc::new(FakeSpawner::default());
    let driver_owner = owner(spawner.clone());
    let (channel, _, route) = setup(&driver_owner, &spawner, 7, 200);
    let route = Arc::new(route);
    runtime
        .pump
        .drive(harness.register_ready_echo_tool(registry_echo(&route), route, route_timing()))
        .unwrap()
        .unwrap();
    runtime
        .pump
        .drive(harness.set_active_tools(vec![ToolName::new("echo").unwrap()]))
        .unwrap();

    let mut prompt = Box::pin(harness.prompt(PromptInput::Text {
        text: "invoke echo".into(),
        images: vec![],
    }));
    let mut request = None;
    for _ in 0..1000 {
        let _ = poll(prompt.as_mut());
        runtime.pump.tick();
        spawner.tick();
        if let Some(bytes) = channel.writes().get(2) {
            request = Some(written_request(bytes));
            break;
        }
    }
    let request = request.expect("one Ready Request");
    let before_write_observations = telemetry.writes().len();
    observation_probe.active.store(true, Ordering::SeqCst);
    storage.settle_armed.store(true, Ordering::SeqCst);
    channel.push(raw_terminal_with_usage(request.request_id));
    for _ in 0..1000 {
        spawner.tick();
        runtime.pump.tick();
        if storage.settle_entered.is_cancelled() {
            break;
        }
    }
    assert!(
        storage.settle_entered.is_cancelled(),
        "checked ToolResult publication parked"
    );
    let Some(Register::LaneState { value: lane, .. }) = runtime
        .pump
        .drive(
            runtime
                .session
                .get_register(RegisterNamespace::LaneState, "main"),
        )
        .unwrap()
    else {
        panic!("pending lane")
    };
    let operation = lane.current_operation_id.expect("pending operation");
    let Some(Register::OperationState { key, value, .. }) = runtime
        .pump
        .drive(
            runtime
                .session
                .get_register(RegisterNamespace::OperationState, operation.as_str()),
        )
        .unwrap()
    else {
        panic!("pending tool state")
    };
    let write = if change_args {
        let mut args = runtime
            .pump
            .drive(
                runtime
                    .session
                    .list_registers(RegisterNamespace::OperationToolArguments, None),
            )
            .unwrap();
        let Register::OperationToolArguments { key, .. } = args.pop().expect("saved args") else {
            panic!("saved args register")
        };
        Write::SetRegister(Box::new(RegisterSet::OperationToolArguments {
            key,
            value: serde_json::from_str(r#"{"text":"changed"}"#).unwrap(),
        }))
    } else {
        Write::SetRegister(Box::new(RegisterSet::OperationState { key, value }))
    };
    runtime
        .pump
        .drive(runtime.session.commit(Transaction {
            writes: vec![write],
        }))
        .unwrap();
    let before_retry = runtime.pump.drive(runtime.store.snapshot()).unwrap();
    if retire_binding {
        use pi_extension_protocol::tool_live::{LivePhase, RetireReason};
        let binding = driver_owner
            .retained_binding()
            .expect("retained B1 before CAS refusal");
        assert_eq!(
            driver_owner.binding_phase(&binding),
            Ok(LivePhase::TerminalReceived)
        );
        driver_owner
            .retire(&binding, RetireReason::NoLongerExpected)
            .unwrap();
    }
    storage
        .fail_next_post_commit_stamp
        .store(fault_after_commit, Ordering::SeqCst);
    storage.settle_release.cancel();
    let mut completed = None;
    for _ in 0..1000 {
        runtime.pump.tick();
        spawner.tick();
        if let Poll::Ready(value) = poll(prompt.as_mut()) {
            completed = Some(value);
            break;
        }
    }
    let completion = completed.expect("CAS retry must finish");
    assert_eq!(storage.settle_not_applied.load(Ordering::SeqCst), 1);
    assert_eq!(channel.writes().len(), 3, "no second external execute");
    if change_args {
        assert!(matches!(
            completion,
            Err(pi_agent_schema::HarnessRuntimeError::Fault(_))
        ));
        assert_eq!(
            runtime.pump.drive(runtime.store.snapshot()).unwrap(),
            before_retry,
            "A's retained receipt cannot settle under B's args/seq"
        );
        return;
    }
    if retire_binding {
        assert!(matches!(
            completion,
            Err(pi_agent_schema::HarnessRuntimeError::Fault(_))
        ));
        assert_eq!(
            runtime.pump.drive(runtime.store.snapshot()).unwrap(),
            before_retry,
            "CAS retry must reject a receipt whose exact B1 has left the owner ledger"
        );
        return;
    }
    if fault_after_commit {
        assert!(storage.post_commit_fault_seen.load(Ordering::SeqCst));
        let observations = telemetry.writes();
        let earned = &observations[before_write_observations..];
        assert_eq!(
            earned.len(),
            1,
            "committed write emits exactly one receipt despite hydration Fault"
        );
        assert!(earned[0].parent.is_some());
        assert_eq!(earned[0].status, Some(pi_telemetry::SpanStatus::Ok));
        assert_eq!(earned[0].finishes, 1);
        assert_eq!(earned[0].drops, 1);
        assert!(matches!(
            completion,
            Err(pi_agent_schema::HarnessRuntimeError::Fault(_))
        ));
        let snapshot = runtime.pump.drive(runtime.store.snapshot()).unwrap();
        assert_eq!(
            snapshot
                .entries
                .iter()
                .filter(|entry| matches!(entry,
            pi_agent_schema::Entry::Message(message) if matches!(&message.message,
                pi_agent_schema::AgentMessage::BuiltIn(pi_ai_core::Message::ToolResult(_)))))
                .count(),
            1,
            "the committed result remains durable after hydration faults outward"
        );
        return;
    }
    completion.unwrap().unwrap();
    let snapshot = runtime.pump.drive(runtime.store.snapshot()).unwrap();
    let result_id = snapshot
        .entries
        .iter()
        .find_map(|entry| match entry {
            pi_agent_schema::Entry::Message(message)
                if matches!(
                    &message.message,
                    pi_agent_schema::AgentMessage::BuiltIn(pi_ai_core::Message::ToolResult(_))
                ) =>
            {
                Some(message.id.clone())
            }
            _ => None,
        })
        .expect("one ToolResult");
    assert_eq!(
        snapshot
            .entries
            .iter()
            .filter(|entry| matches!(entry,
        pi_agent_schema::Entry::Message(message) if matches!(&message.message,
            pi_agent_schema::AgentMessage::BuiltIn(pi_ai_core::Message::ToolResult(_)))))
            .count(),
        1
    );
    assert_eq!(
        snapshot
            .usage
            .iter()
            .filter(|row| row.entry_id.as_ref() == Some(&result_id))
            .count(),
        1
    );
    assert!(
        !snapshot
            .registers
            .iter()
            .any(|register| matches!(register, Register::OperationToolArguments { .. }))
    );
}

#[test]
fn actual_memory_checked_refusal_retries_one_retained_echo_receipt() {
    checked_refusal_memory_route(false, false, false);
}

#[test]
fn actual_sqlite_zero_credit_route_retries_real_revision_refusal_and_reopens() {
    use pi_agent_schema::{
        AgentMessage, Entry, NewCustomEntry, NewEntry, OperationState, ProviderHeaderPolicy,
        RegisterNamespace, RegisterSet, RunPhase, SessionCodecOptions, Storage, ToolCallState,
        Transaction, Write,
    };
    use pi_agent_session::storage::{
        ConditionalCommit, ConditionalOutcome, PreparedEntrySlot, PublicationValidator,
        RuntimeStorage,
    };
    use pi_agent_sqlite::{SqliteOptions, SqliteStorage};
    use pi_ai_core::{AssistantContent, StopReason, ToolCall, ToolCallId, ToolName};
    use pi_extension_protocol::tool_live::LivePhase;
    use prost::Message as _;
    use runtime_r1_r4_support::assistant;
    use runtime_r6_retirement_support::AfterToolCount;
    use runtime_sqlite_tool_support::{CheckedSqlite, File};

    let runtime = Setup::new();
    let file = File::new();
    let codec = Arc::new(
        SessionCodec::new(
            SessionCodecOptions::default(),
            ProviderHeaderPolicy::from_allowlist(["x-public".into()]),
        )
        .unwrap(),
    );
    let probe = Arc::new(CommitLineProbe::default());
    let clock = Arc::new(CommitStorageClock {
        time: runtime.time.clone(),
        probe: probe.clone(),
        fail: AtomicBool::new(false),
        calls: AtomicU64::new(0),
        stale_commit: Mutex::new(None),
        stale_seen: AtomicBool::new(false),
    });
    let telemetry = Arc::new(CommitTelemetry::new(probe.clone(), clock.clone()));
    let sqlite = SqliteStorage::create(
        &file.0,
        runtime.session.metadata(),
        codec.clone(),
        clock.clone(),
        SqliteOptions::default(),
    )
    .unwrap();
    let checked = Arc::new(CheckedSqlite::new(sqlite.clone(), {
        let clock = clock.clone();
        Arc::new(move || clock.calls.load(Ordering::SeqCst))
    }));
    let session = Arc::new(
        runtime
            .pump
            .drive(RuntimeSession::new(
                checked.clone(),
                runtime.session.metadata(),
                codec.clone(),
                runtime.ids.clone(),
                Arc::new(pi_telemetry::NOOP_TELEMETRY_CONTEXT),
            ))
            .unwrap(),
    );
    runtime
        .pump
        .drive(session.commit(Transaction {
            writes: vec![
                Write::SetRegister(Box::new(RegisterSet::LaneLeaf {
                    key: "main".into(),
                    value: None,
                })),
                Write::SetRegister(Box::new(RegisterSet::LaneState {
                    key: "main".into(),
                    value: pi_agent_schema::LaneState {
                        current_operation_id: None,
                        pending_next_run: vec![],
                    },
                })),
            ],
        }))
        .unwrap();
    let mut tool_response = assistant(&runtime.faux.model, StopReason::ToolUse);
    tool_response.content = vec![AssistantContent::ToolCall(ToolCall {
        content_type: Default::default(),
        id: ToolCallId::new("echo-call").unwrap(),
        name: ToolName::new("echo").unwrap(),
        arguments: saved(),
        thought_signature: None,
        namespace: None,
    })];
    *runtime.faux.responses.lock().unwrap() = vec![
        tool_response,
        assistant(&runtime.faux.model, StopReason::Stop),
    ]
    .into();
    let factory = RuntimeFactory::new(
        runtime.time.clone(),
        runtime.pump.clone(),
        runtime.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    let mut options = runtime.options_for::<(), _>(session.clone(), DriveMode::Automatic);
    options.tool_execution = pi_agent_schema::ToolExecutionMode::Sequential;
    options.telemetry_context = telemetry.clone();
    let harness = runtime
        .pump
        .drive(factory.create_first_attachment(options))
        .unwrap_or_else(|error| panic!("first attachment: {error}"))
        .harness;
    let spawner = Arc::new(FakeSpawner::default());
    let driver_owner = owner(spawner.clone());
    let (channel, _, route) = setup(&driver_owner, &spawner, 7, 200);
    let route = Arc::new(route);
    let echo = registry_echo(&route);
    runtime
        .pump
        .drive(harness.register_ready_echo_tool(echo.clone(), route, route_timing()))
        .unwrap()
        .unwrap();
    runtime
        .pump
        .drive(harness.set_active_tools(vec![ToolName::new("echo").unwrap()]))
        .unwrap();
    let after = Arc::new(AfterToolCount::default());
    let _after_subscription = harness.hooks().after_tool(None, after.clone()).unwrap();
    let ends = Arc::new(RegistryEventLog(Mutex::new(vec![])));
    let _end_subscription = harness.events().on(HarnessEventType::ToolEnd, ends.clone());

    let mut prompt = Box::pin(harness.prompt(PromptInput::Text {
        text: "invoke echo".into(),
        images: vec![],
    }));
    let mut request = None;
    for _ in 0..1000 {
        let _ = poll(prompt.as_mut());
        runtime.pump.tick();
        spawner.tick();
        if let Some(bytes) = channel.writes().get(2) {
            request = Some(written_request(bytes));
            break;
        }
    }
    let request = request.expect("one outbound Ready Request");
    assert_eq!(request.method, "tool.invoke");
    assert_eq!(request.initial_stream_credit_bytes, Some(0));
    assert_eq!(channel.writes().len(), 3);
    let pending = runtime.pump.drive(sqlite.snapshot()).unwrap();
    let Some(Register::LaneState { value: lane, .. }) = runtime
        .pump
        .drive(session.get_register(RegisterNamespace::LaneState, "main"))
        .unwrap()
    else {
        panic!("pending lane")
    };
    let operation = lane.current_operation_id.expect("pending operation");
    let Some(Register::OperationState {
        key: state_key,
        value: state,
        ..
    }) = runtime
        .pump
        .drive(session.get_register(RegisterNamespace::OperationState, operation.as_str()))
        .unwrap()
    else {
        panic!("pending state")
    };
    let OperationState::Run(run) = state.as_ref() else {
        panic!("run state")
    };
    let RunPhase::Tools(tools) = &run.phase else {
        panic!("tool phase")
    };
    assert!(matches!(
        &tools.batch.calls[0],
        ToolCallState::EffectPending { .. }
    ));
    let args_key = format!("{}:{}:0", operation.as_str(), tools.batch.turn_id.as_str());
    let args: Vec<_> = pending
        .registers
        .iter()
        .filter(|register| matches!(register, Register::OperationToolArguments { .. }))
        .collect();
    assert_eq!(args.len(), 1);
    assert!(
        matches!(args[0], Register::OperationToolArguments { key, value, seq }
        if key == &args_key && value == &saved() && seq.get() > 0)
    );
    assert_eq!(
        request.effect_key,
        Some(v1_1::EffectKey {
            session_id: "runtime-session".into(),
            lane: "main".into(),
            operation_id: operation.as_str().into(),
            effect_kind: "tool".into(),
            step_or_task_id: tools.batch.turn_id.as_str().into(),
            logical_attempt: 0,
            item_index: Some(0),
        })
    );
    let invocation = v1_1::InvokeToolRequest::decode(request.payload.as_slice()).unwrap();
    assert_eq!(invocation.tool_call_id, "echo-call");
    assert_eq!(invocation.tool_id, "echo");
    let Some(v1_1::wire_value::Value::MapValue(arguments)) = invocation
        .arguments
        .and_then(|bound| bound.value)
        .and_then(|value| value.value)
    else {
        panic!("selected arguments")
    };
    assert!(arguments.entries.iter().any(|entry| entry.key == "text"
        && matches!(&entry.value, Some(v1_1::WireValue {
            value: Some(v1_1::wire_value::Value::TextValue(value))
        }) if value == "ping")));

    let clock_settings = Arc::new(AtomicBool::new(true));
    probe.set({
        let harness = harness.clone();
        let echo = echo.clone();
        let clock_settings = clock_settings.clone();
        let checked = checked.clone();
        Arc::new(move |stage| {
            if stage == "storage_clock" && !checked.preparing_result.load(Ordering::SeqCst) {
                return true;
            }
            let mut lane = Box::pin(harness.get_leaf_id());
            let lane_free = matches!(poll(lane.as_mut()), Poll::Ready(Ok(_)));
            let settings_free =
                if stage == "storage_clock" && clock_settings.swap(false, Ordering::SeqCst) {
                    let mut settings = Box::pin(harness.set_tools(vec![echo.clone()]));
                    matches!(poll(settings.as_mut()), Poll::Ready(Ok(())))
                } else {
                    true
                };
            lane_free && settings_free
        })
    });
    let prior_writes = telemetry.writes().len();
    probe.active.store(true, Ordering::SeqCst);
    channel.push(raw_terminal_with_usage(request.request_id));
    for _ in 0..1000 {
        spawner.tick();
        runtime.pump.tick();
        if checked.first_entered.is_cancelled() {
            break;
        }
    }
    assert!(
        checked.first_entered.is_cancelled(),
        "first prepared CAS parked"
    );
    let binding = driver_owner.retained_binding().expect("exact B1 retained");
    assert_eq!(
        driver_owner.binding_phase(&binding),
        Ok(LivePhase::TerminalReceived)
    );
    assert_eq!(after.0.load(Ordering::SeqCst), 1);
    assert_eq!(telemetry.writes().len(), prior_writes);
    assert_eq!(runtime.pump.drive(sqlite.snapshot()).unwrap(), pending);
    let stamp_before = runtime.pump.drive(sqlite.read_stamp()).unwrap();
    runtime
        .pump
        .drive(session.commit(Transaction {
            writes: vec![Write::SetRegister(Box::new(RegisterSet::OperationState {
                key: state_key,
                value: state,
            }))],
        }))
        .unwrap();
    let stamp_after = runtime.pump.drive(sqlite.read_stamp()).unwrap();
    assert_ne!(
        stamp_before, stamp_after,
        "real Session write advances SQLite revision"
    );
    let before_refusal = runtime.pump.drive(sqlite.snapshot()).unwrap();
    checked.first_release.cancel();
    for _ in 0..1000 {
        spawner.tick();
        runtime.pump.tick();
        if checked.second_entered.is_cancelled() {
            break;
        }
    }
    assert!(
        checked.second_entered.is_cancelled(),
        "retained terminal reaches second prepared CAS"
    );
    assert_eq!(checked.attempts.load(Ordering::SeqCst), 2);
    assert_eq!(checked.refused.load(Ordering::SeqCst), 1);
    assert_eq!(checked.applied.load(Ordering::SeqCst), 0);
    assert_eq!(
        telemetry.writes().len(),
        prior_writes,
        "refusal earns no write receipt"
    );
    assert_eq!(
        runtime.pump.drive(sqlite.snapshot()).unwrap(),
        before_refusal
    );
    assert_eq!(
        driver_owner.binding_phase(&binding),
        Ok(LivePhase::TerminalReceived)
    );
    assert_eq!(after.0.load(Ordering::SeqCst), 1);
    runtime.time.0.store(19, Ordering::SeqCst);
    let frozen_timestamp = checked.second_timestamp.lock().unwrap().unwrap();
    checked.second_release.cancel();
    let mut completed = None;
    for _ in 0..1000 {
        spawner.tick();
        runtime.pump.tick();
        if let Poll::Ready(result) = poll(prompt.as_mut()) {
            completed = Some(result);
            break;
        }
    }
    completed.expect("SQLite route completes").unwrap().unwrap();
    assert_eq!(checked.applied.load(Ordering::SeqCst), 1);
    assert_eq!(checked.clock_calls_during_commit.load(Ordering::SeqCst), 0);
    assert!(
        !clock_settings.load(Ordering::SeqCst),
        "clock callback probed settings line"
    );
    assert!(
        !probe.failed.load(Ordering::SeqCst),
        "both lines free: {:?}",
        probe.failed_stages.lock().unwrap()
    );
    assert!(probe.stages.lock().unwrap().contains(&"storage_clock"));
    let writes = telemetry.writes();
    let result_write = &writes[prior_writes];
    assert_eq!(result_write.status, Some(pi_telemetry::SpanStatus::Ok));
    assert_eq!(result_write.finishes, 1);
    assert_eq!(result_write.drops, 1);
    assert!(result_write.parent.is_some());
    assert!(
        matches!(result_write.attributes.get("pi.session.item_kinds"),
        Some(pi_telemetry::AttributeValue::Strings(kinds))
            if kinds.contains(&"entry".to_owned()) && kinds.contains(&"usage".to_owned()))
    );
    assert_eq!(channel.writes().len(), 3, "one B1 Request after handshake");
    assert_eq!(after.0.load(Ordering::SeqCst), 1);
    assert_eq!(ends.0.lock().unwrap().len(), 1);
    assert!(
        driver_owner.retained_binding().is_none(),
        "B1 retired after commit"
    );
    let final_snapshot = runtime.pump.drive(sqlite.snapshot()).unwrap();
    assert_eq!(
        final_snapshot
            .entries
            .iter()
            .filter(|entry| matches!(entry, Entry::Message(message)
                if matches!(&message.message,
                    AgentMessage::BuiltIn(pi_ai_core::Message::ToolResult(_)))))
            .count(),
        1,
        "exactly one ToolResult is durable"
    );
    let results: Vec<_> = final_snapshot
        .entries
        .iter()
        .filter_map(|entry| match entry {
            Entry::Message(message)
                if matches!(&message.message,
            AgentMessage::BuiltIn(pi_ai_core::Message::ToolResult(result))
                if result.tool_call_id.as_str() == "echo-call" && !result.is_error) =>
            {
                Some(message)
            }
            _ => None,
        })
        .collect();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].timestamp, frozen_timestamp);
    let linked: Vec<_> = final_snapshot
        .usage
        .iter()
        .filter(|row| row.entry_id.as_ref() == Some(&results[0].id))
        .collect();
    assert_eq!(linked.len(), 1);
    assert_eq!(linked[0].usage.input.get(), 5);
    assert!(
        !final_snapshot
            .registers
            .iter()
            .any(|register| matches!(register, Register::OperationToolArguments { .. }))
    );
    let result_id = results[0].id.clone();
    let usage_id = linked[0].id.clone();
    let old_stamp = runtime.pump.drive(sqlite.read_stamp()).unwrap();
    let stale_entry = NewEntry::Custom(NewCustomEntry {
        id: EntryId::new("stale-sqlite-route-probe").unwrap(),
        parent_id: None,
        entry_type: pi_agent_schema::CustomEntryType::Custom,
        custom_type: pi_agent_schema::CustomType::new("probe").unwrap(),
        data: pi_ai_core::Presence::Missing,
    });
    let stale_request = ConditionalCommit {
        conditions: vec![],
        transaction: Transaction {
            writes: vec![Write::Entry(stale_entry.clone())],
        },
    };
    let stale_prepared = runtime
        .pump
        .drive(sqlite.prepare_entries(
            old_stamp.clone(),
            vec![PreparedEntrySlot {
                write_index: 0,
                entry: stale_entry,
            }],
        ))
        .unwrap()
        .unwrap();
    runtime.pump.drive(harness.close()).unwrap();
    runtime.pump.drive(session.close()).unwrap();
    let reopened = SqliteStorage::open(&file.0, codec, clock, SqliteOptions::default()).unwrap();
    assert_ne!(
        old_stamp,
        runtime.pump.drive(reopened.read_stamp()).unwrap()
    );
    struct Accept;
    impl PublicationValidator for Accept {
        type Error = ();
        fn validate(&self, _: &pi_agent_session::storage::StorageSnapshot) -> Result<(), ()> {
            Ok(())
        }
    }
    assert_eq!(
        runtime
            .pump
            .drive(reopened.commit_prepared_checked(stale_prepared, stale_request, &Accept,))
            .unwrap(),
        ConditionalOutcome::NotApplied
    );
    let durable = runtime.pump.drive(reopened.snapshot()).unwrap();
    assert_eq!(durable, final_snapshot);
    assert!(durable.entries.iter().any(|entry| matches!(entry,
        Entry::Message(message) if message.id == result_id && message.timestamp == frozen_timestamp)));
    assert!(
        durable
            .usage
            .iter()
            .any(|row| row.id == usage_id && row.entry_id.as_ref() == Some(&result_id))
    );
    runtime.pump.drive(reopened.close()).unwrap();
}

fn crash_sqlite_codec() -> Arc<SessionCodec> {
    Arc::new(
        SessionCodec::new(
            pi_agent_schema::SessionCodecOptions::default(),
            pi_agent_schema::ProviderHeaderPolicy::from_allowlist(["x-public".into()]),
        )
        .unwrap(),
    )
}

fn crash_sqlite_options() -> pi_agent_sqlite::SqliteOptions {
    pi_agent_sqlite::SqliteOptions {
        lease_ttl_ms: SafeUnsigned::new(120_000).unwrap(),
        heartbeat_ms: SafeUnsigned::new(1_000).unwrap(),
        ..Default::default()
    }
}

fn crash_sqlite_lease(db: &rusqlite::Connection) -> (Vec<u8>, i64, i64) {
    db.query_row(
        "SELECT owner_id,fence,expires_at_ms FROM writer_lease WHERE singleton=1",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )
    .unwrap()
}

fn pending_sqlite_key_and_args(
    snapshot: &pi_agent_session::storage::StorageSnapshot,
) -> (v1_1::EffectKey, Register) {
    use pi_agent_schema::{OperationState, RunPhase, ToolCallState};
    let lane = snapshot
        .registers
        .iter()
        .find_map(|register| match register {
            Register::LaneState { key, value, .. } if key == "main" => Some(value),
            _ => None,
        })
        .expect("persisted main lane");
    let operation = lane
        .current_operation_id
        .as_ref()
        .expect("pending operation");
    let state = snapshot
        .registers
        .iter()
        .find_map(|register| match register {
            Register::OperationState { key, value, .. } if key == operation.as_str() => Some(value),
            _ => None,
        })
        .expect("persisted operation");
    let OperationState::Run(run) = state.as_ref() else {
        panic!("pending run")
    };
    let RunPhase::Tools(tools) = &run.phase else {
        panic!("pending tools")
    };
    assert!(matches!(
        tools.batch.calls.as_slice(),
        [ToolCallState::EffectPending { .. }]
    ));
    let args_key = format!("{}:{}:0", operation.as_str(), tools.batch.turn_id.as_str());
    let args = snapshot
        .registers
        .iter()
        .filter(|register| matches!(register, Register::OperationToolArguments { .. }))
        .collect::<Vec<_>>();
    assert_eq!(args.len(), 1);
    assert!(
        matches!(args[0], Register::OperationToolArguments { key, value, seq }
        if key == &args_key && value == &saved() && seq.get() > 0)
    );
    assert!(!snapshot.entries.iter().any(|entry| matches!(entry,
        pi_agent_schema::Entry::Message(message) if matches!(&message.message,
            pi_agent_schema::AgentMessage::BuiltIn(pi_ai_core::Message::ToolResult(_))))));
    (
        v1_1::EffectKey {
            session_id: "runtime-session".into(),
            lane: "main".into(),
            operation_id: operation.as_str().into(),
            effect_kind: "tool".into(),
            step_or_task_id: tools.batch.turn_id.as_str().into(),
            logical_attempt: 0,
            item_index: Some(0),
        },
        args[0].clone(),
    )
}

fn pending_schema_key(key: &v1_1::EffectKey) -> pi_agent_schema::EffectKey {
    pi_agent_schema::EffectKey {
        session_id: pi_agent_schema::SessionId::new(&key.session_id).unwrap(),
        lane: pi_agent_schema::LaneId::new(&key.lane).unwrap(),
        operation_id: pi_agent_schema::OperationId::new(&key.operation_id).unwrap(),
        kind: pi_agent_schema::EffectKind::Tool,
        step_or_task_id: pi_agent_schema::EffectStepId::new(&key.step_or_task_id).unwrap(),
        logical_attempt: pi_ai_core::SafeUnsigned::new(key.logical_attempt).unwrap(),
        item_index: key
            .item_index
            .map(|i| pi_ai_core::SafeUnsigned::new(i).unwrap()),
    }
}

#[test]
#[ignore = "spawned only by the pending SQLite process-lifecycle witness"]
fn sqlite_pending_crash_child() {
    use pi_agent_schema::{IdGenerator, RegisterSet, Transaction, Write};
    use pi_agent_sqlite::SqliteStorage;
    use pi_ai_core::{AssistantContent, StopReason, ToolCall, ToolCallId, ToolName};
    use prost::Message as _;
    use runtime_r1_r4_support::assistant;

    let path = std::path::PathBuf::from(std::env::var_os("PI_RUNTIME_SQLITE_CRASH_PATH").unwrap());
    let replay_name = std::env::var("PI_RUNTIME_SQLITE_PENDING_REPLAY").unwrap();
    let native = replay_name == "native_safe";
    let replay = match replay_name.as_str() {
        "safe" | "native_safe" => ToolReplay::Safe,
        "never" => ToolReplay::Never,
        _ => panic!("unknown pending replay fixture"),
    };
    let runtime = Setup::new();
    let codec = crash_sqlite_codec();
    let sqlite = SqliteStorage::open(
        &path,
        codec.clone(),
        runtime.time.clone(),
        crash_sqlite_options(),
    )
    .unwrap();
    let session = Arc::new(
        runtime
            .pump
            .drive(RuntimeSession::new(
                sqlite.clone(),
                sqlite.metadata().unwrap(),
                codec,
                runtime.ids.clone(),
                Arc::new(pi_telemetry::NOOP_TELEMETRY_CONTEXT),
            ))
            .unwrap(),
    );
    runtime
        .pump
        .drive(session.commit(Transaction {
            writes: vec![
                Write::SetRegister(Box::new(RegisterSet::LaneLeaf {
                    key: "main".into(),
                    value: None,
                })),
                Write::SetRegister(Box::new(RegisterSet::LaneState {
                    key: "main".into(),
                    value: pi_agent_schema::LaneState {
                        current_operation_id: None,
                        pending_next_run: vec![],
                    },
                })),
            ],
        }))
        .unwrap();
    let mut response = assistant(&runtime.faux.model, StopReason::ToolUse);
    response.content = vec![AssistantContent::ToolCall(ToolCall {
        content_type: Default::default(),
        id: ToolCallId::new("echo-call").unwrap(),
        name: ToolName::new("echo").unwrap(),
        arguments: saved(),
        thought_signature: None,
        namespace: None,
    })];
    *runtime.faux.responses.lock().unwrap() = vec![response].into();
    let factory = RuntimeFactory::new(
        runtime.time.clone(),
        runtime.pump.clone(),
        runtime.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    let mut options = runtime.options_for::<(), _>(session, DriveMode::Automatic);
    options.tool_execution = pi_agent_schema::ToolExecutionMode::Sequential;
    let native_entered = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    if native {
        options.tools = vec![native_echo(false, native_entered.clone())];
    }
    let harness = runtime
        .pump
        .drive(factory.create_first_attachment(options))
        .unwrap()
        .harness;
    if native {
        let mut prompt = Box::pin(harness.prompt(PromptInput::Text {
            text: "invoke echo".into(),
            images: vec![],
        }));
        for _ in 0..1000 {
            assert!(
                poll(prompt.as_mut()).is_pending(),
                "native child must retain pending effect"
            );
            runtime.pump.tick();
            if native_entered.load(Ordering::SeqCst) == 1 {
                break;
            }
        }
        assert_eq!(native_entered.load(Ordering::SeqCst), 1);
        let pending = runtime.pump.drive(sqlite.snapshot()).unwrap();
        pending_sqlite_key_and_args(&pending);
        let marker = runtime.ids.next(None);
        let high: u64 = marker.strip_prefix("runtime-id-").unwrap().parse().unwrap();
        use std::io::Write as _;
        println!("PI_RUNTIME_SQLITE_PENDING:v1:{replay_name}:{high}");
        std::io::stdout().flush().unwrap();
        loop {
            std::thread::park();
        }
    }
    let spawner = Arc::new(FakeSpawner::default());
    let driver_owner = owner(spawner.clone());
    let (channel, _, route) = setup_with_replay(&driver_owner, &spawner, 7, 200, replay);
    let route = Arc::new(route);
    runtime
        .pump
        .drive(harness.register_ready_echo_tool(registry_echo(&route), route, route_timing()))
        .unwrap()
        .unwrap();
    runtime
        .pump
        .drive(harness.set_active_tools(vec![ToolName::new("echo").unwrap()]))
        .unwrap();
    let mut prompt = Box::pin(harness.prompt(PromptInput::Text {
        text: "invoke echo".into(),
        images: vec![],
    }));
    let mut outbound = None;
    for _ in 0..1000 {
        assert!(
            poll(prompt.as_mut()).is_pending(),
            "child must have no terminal"
        );
        runtime.pump.tick();
        spawner.tick();
        if let Some(bytes) = channel.writes().get(2) {
            outbound = Some(written_request(bytes));
            break;
        }
    }
    let request = outbound.expect("one G1 Request");
    assert_eq!(channel.writes().len(), 3);
    assert_eq!(request.method, "tool.invoke");
    assert_eq!(request.initial_stream_credit_bytes, Some(0));
    let pending = runtime.pump.drive(sqlite.snapshot()).unwrap();
    let (key, _) = pending_sqlite_key_and_args(&pending);
    assert_eq!(request.effect_key, Some(key));
    let invocation = v1_1::InvokeToolRequest::decode(request.payload.as_slice()).unwrap();
    assert_eq!(invocation.tool_call_id, "echo-call");
    assert_eq!(invocation.tool_id, "echo");
    let Some(v1_1::wire_value::Value::MapValue(arguments)) = invocation
        .arguments
        .and_then(|bound| bound.value)
        .and_then(|value| value.value)
    else {
        panic!("selected wire arguments")
    };
    assert_eq!(
        arguments,
        v1_1::WireMap {
            entries: vec![v1_1::WireMapEntry {
                key: "text".into(),
                value: Some(v1_1::WireValue {
                    value: Some(v1_1::wire_value::Value::TextValue("ping".into())),
                }),
            }]
        }
    );
    let marker = runtime.ids.next(None);
    let high: u64 = marker.strip_prefix("runtime-id-").unwrap().parse().unwrap();
    use std::io::Write as _;
    println!("PI_RUNTIME_SQLITE_PENDING:v1:{replay_name}:{high}");
    std::io::stdout().flush().unwrap();
    loop {
        std::thread::park();
    }
}

#[test]
#[ignore = "spawned only by the post-apply process-crash witness"]
fn sqlite_post_apply_crash_child() {
    use pi_agent_schema::{
        OperationState, RegisterNamespace, RegisterSet, RunPhase, ToolCallState, Transaction, Write,
    };
    use pi_agent_sqlite::SqliteStorage;
    use pi_ai_core::{AssistantContent, StopReason, ToolCall, ToolCallId, ToolName};
    use prost::Message as _;
    use runtime_r1_r4_support::assistant;
    use runtime_sqlite_tool_support::CheckedSqlite;

    let path = std::path::PathBuf::from(std::env::var_os("PI_RUNTIME_SQLITE_CRASH_PATH").unwrap());
    let runtime = Setup::new();
    let codec = crash_sqlite_codec();
    let sqlite = SqliteStorage::open(
        &path,
        codec.clone(),
        runtime.time.clone(),
        crash_sqlite_options(),
    )
    .unwrap();
    let checked = Arc::new(CheckedSqlite::new(sqlite.clone(), Arc::new(|| 0)));
    checked.first_release.cancel();
    checked.after_applied(|| {
        use std::io::Write;
        println!("PI_RUNTIME_SQLITE_CRASH:APPLIED:v1");
        std::io::stdout().flush().unwrap();
        loop {
            std::thread::park();
        }
    });
    let session = Arc::new(
        runtime
            .pump
            .drive(RuntimeSession::new(
                checked,
                sqlite.metadata().unwrap(),
                codec,
                runtime.ids.clone(),
                Arc::new(pi_telemetry::NOOP_TELEMETRY_CONTEXT),
            ))
            .unwrap(),
    );
    runtime
        .pump
        .drive(session.commit(Transaction {
            writes: vec![
                Write::SetRegister(Box::new(RegisterSet::LaneLeaf {
                    key: "main".into(),
                    value: None,
                })),
                Write::SetRegister(Box::new(RegisterSet::LaneState {
                    key: "main".into(),
                    value: pi_agent_schema::LaneState {
                        current_operation_id: None,
                        pending_next_run: vec![],
                    },
                })),
            ],
        }))
        .unwrap();
    let mut response = assistant(&runtime.faux.model, StopReason::ToolUse);
    response.content = vec![AssistantContent::ToolCall(ToolCall {
        content_type: Default::default(),
        id: ToolCallId::new("echo-call").unwrap(),
        name: ToolName::new("echo").unwrap(),
        arguments: saved(),
        thought_signature: None,
        namespace: None,
    })];
    *runtime.faux.responses.lock().unwrap() =
        vec![response, assistant(&runtime.faux.model, StopReason::Stop)].into();
    let factory = RuntimeFactory::new(
        runtime.time.clone(),
        runtime.pump.clone(),
        runtime.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    let mut options = runtime.options_for::<(), _>(session.clone(), DriveMode::Automatic);
    options.tool_execution = pi_agent_schema::ToolExecutionMode::Sequential;
    let harness = runtime
        .pump
        .drive(factory.create_first_attachment(options))
        .unwrap()
        .harness;
    let spawner = Arc::new(FakeSpawner::default());
    let driver_owner = owner(spawner.clone());
    let (channel, _, route) = setup(&driver_owner, &spawner, 7, 200);
    let route = Arc::new(route);
    let echo = registry_echo(&route);
    runtime
        .pump
        .drive(harness.register_ready_echo_tool(echo, route, route_timing()))
        .unwrap()
        .unwrap();
    runtime
        .pump
        .drive(harness.set_active_tools(vec![ToolName::new("echo").unwrap()]))
        .unwrap();
    let mut prompt = Box::pin(harness.prompt(PromptInput::Text {
        text: "invoke echo".into(),
        images: vec![],
    }));
    let mut outbound = None;
    for _ in 0..1000 {
        let _ = poll(prompt.as_mut());
        runtime.pump.tick();
        spawner.tick();
        if let Some(bytes) = channel.writes().get(2) {
            outbound = Some(written_request(bytes));
            break;
        }
    }
    let request = outbound.expect("selected Ready Request");
    assert_eq!(channel.writes().len(), 3);
    assert_eq!(request.method, "tool.invoke");
    assert_eq!(request.initial_stream_credit_bytes, Some(0));
    let Some(Register::LaneState { value: lane, .. }) = runtime
        .pump
        .drive(session.get_register(RegisterNamespace::LaneState, "main"))
        .unwrap()
    else {
        panic!("pending lane")
    };
    let operation = lane.current_operation_id.unwrap();
    let Some(Register::OperationState { value: state, .. }) = runtime
        .pump
        .drive(session.get_register(RegisterNamespace::OperationState, operation.as_str()))
        .unwrap()
    else {
        panic!("pending state")
    };
    let OperationState::Run(run) = state.as_ref() else {
        panic!("run state")
    };
    let RunPhase::Tools(tools) = &run.phase else {
        panic!("tool phase")
    };
    assert!(matches!(
        tools.batch.calls[0],
        ToolCallState::EffectPending { .. }
    ));
    let pending = runtime.pump.drive(sqlite.snapshot()).unwrap();
    let args_key = format!("{}:{}:0", operation.as_str(), tools.batch.turn_id.as_str());
    let args: Vec<_> = pending
        .registers
        .iter()
        .filter(|register| matches!(register, Register::OperationToolArguments { .. }))
        .collect();
    assert_eq!(args.len(), 1);
    assert!(
        matches!(args[0], Register::OperationToolArguments { key, value, seq }
        if key == &args_key && value == &saved() && seq.get() > 0)
    );
    assert_eq!(
        request.effect_key,
        Some(v1_1::EffectKey {
            session_id: "runtime-session".into(),
            lane: "main".into(),
            operation_id: operation.as_str().into(),
            effect_kind: "tool".into(),
            step_or_task_id: tools.batch.turn_id.as_str().into(),
            logical_attempt: 0,
            item_index: Some(0),
        })
    );
    let invocation = v1_1::InvokeToolRequest::decode(request.payload.as_slice()).unwrap();
    assert_eq!(invocation.tool_call_id, "echo-call");
    assert_eq!(invocation.tool_id, "echo");
    let Some(v1_1::wire_value::Value::MapValue(arguments)) = invocation
        .arguments
        .and_then(|bound| bound.value)
        .and_then(|value| value.value)
    else {
        panic!("selected wire arguments")
    };
    assert_eq!(
        arguments,
        v1_1::WireMap {
            entries: vec![v1_1::WireMapEntry {
                key: "text".into(),
                value: Some(v1_1::WireValue {
                    value: Some(v1_1::wire_value::Value::TextValue("ping".into())),
                }),
            }],
        },
        "selected request must carry exactly the saved arguments"
    );
    channel.push(raw_terminal_with_usage(request.request_id));
    for _ in 0..1000 {
        spawner.tick();
        runtime.pump.tick();
    }
    panic!("post-apply gate was not reached");
}

#[test]
fn actual_sqlite_post_apply_crash_keeps_atomic_result_and_fences_dead_writer() {
    use pi_agent_schema::{
        AgentMessage, Entry, NewCustomEntry, NewEntry, SessionError, Storage, StorageError,
        Transaction, Write,
    };
    use pi_agent_session::storage::{
        ConditionalCommit, ConditionalOutcome, PreparedEntrySlot, PublicationValidator,
        RuntimeStorage,
    };
    use pi_agent_sqlite::SqliteStorage;
    use runtime_sqlite_tool_support::{CrashChild, File};

    let file = File::new();
    let runtime = Setup::new();
    let codec = crash_sqlite_codec();
    let options = crash_sqlite_options();
    let initial = SqliteStorage::create(
        &file.0,
        runtime.session.metadata(),
        codec.clone(),
        runtime.time.clone(),
        options,
    )
    .unwrap();
    let old_stamp = runtime.pump.drive(initial.read_stamp()).unwrap();
    let stale_entry = NewEntry::Custom(NewCustomEntry {
        id: EntryId::new("older-incarnation-probe").unwrap(),
        parent_id: None,
        entry_type: pi_agent_schema::CustomEntryType::Custom,
        custom_type: pi_agent_schema::CustomType::new("probe").unwrap(),
        data: pi_ai_core::Presence::Missing,
    });
    let stale_request = ConditionalCommit {
        conditions: vec![],
        transaction: Transaction {
            writes: vec![Write::Entry(stale_entry.clone())],
        },
    };
    let stale_prepared = runtime
        .pump
        .drive(initial.prepare_entries(
            old_stamp.clone(),
            vec![PreparedEntrySlot {
                write_index: 0,
                entry: stale_entry,
            }],
        ))
        .unwrap()
        .unwrap();
    runtime.pump.drive(initial.close()).unwrap();

    let mut child = CrashChild::start(&file.0);
    child.await_applied();
    let before = SqliteStorage::read_snapshot(&file.0, &codec, options).unwrap();
    let result = before
        .snapshot
        .entries
        .iter()
        .filter_map(|entry| match entry {
            Entry::Message(message)
                if matches!(&message.message, AgentMessage::BuiltIn(pi_ai_core::Message::ToolResult(result))
                    if result.tool_call_id.as_str() == "echo-call" && !result.is_error) =>
            {
                Some(message)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(result.len(), 1);
    assert_eq!(
        before
            .snapshot
            .entries
            .iter()
            .filter(|entry| matches!(entry,
        Entry::Message(message) if matches!(&message.message,
            AgentMessage::BuiltIn(pi_ai_core::Message::ToolResult(_)))))
            .count(),
        1
    );
    let linked = before
        .snapshot
        .usage
        .iter()
        .filter(|row| row.entry_id.as_ref() == Some(&result[0].id))
        .collect::<Vec<_>>();
    assert_eq!(linked.len(), 1);
    assert_eq!(linked[0].usage.input.get(), 5);
    assert!(
        !before
            .snapshot
            .registers
            .iter()
            .any(|register| matches!(register, Register::OperationToolArguments { .. }))
    );
    let result_id = result[0].id.clone();
    let result_time = result[0].timestamp;
    let usage_id = linked[0].id.clone();

    let mut db = rusqlite::Connection::open(&file.0).unwrap();
    let live_lease = crash_sqlite_lease(&db);
    child.kill_and_reap();
    let after = SqliteStorage::read_snapshot(&file.0, &codec, options).unwrap();
    assert_eq!(after.snapshot, before.snapshot);
    assert_eq!(after.revision, before.revision);
    assert_eq!(after.store_token, before.store_token);
    assert_eq!(after.metadata, before.metadata);
    assert!(after.snapshot.entries.iter().any(|entry| matches!(entry,
        Entry::Message(message) if message.id == result_id && message.timestamp == result_time)));
    assert!(
        after
            .snapshot
            .usage
            .iter()
            .any(|row| row.id == usage_id && row.entry_id.as_ref() == Some(&result_id))
    );
    let dead_lease = crash_sqlite_lease(&db);
    assert_eq!(dead_lease.0, live_lease.0);
    assert_eq!(dead_lease.1, live_lease.1);
    let now_ms = || {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    };
    assert!(
        dead_lease.2 > now_ms(),
        "dead writer lease must remain unexpired"
    );
    assert!(matches!(
        SqliteStorage::open(&file.0, codec.clone(), runtime.time.clone(), options),
        Err(SessionError::Storage(StorageError::Busy(_)))
    ));
    assert_eq!(crash_sqlite_lease(&db).0, dead_lease.0);
    assert_eq!(crash_sqlite_lease(&db).1, dead_lease.1);

    // Deterministically expire only this private dead lease; this is not natural TTL passage.
    let tx = db
        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
        .unwrap();
    let updated = tx.execute(
        "UPDATE writer_lease SET expires_at_ms=0 WHERE singleton=1 AND owner_id=?1 AND fence=?2 AND expires_at_ms>?3",
        rusqlite::params![dead_lease.0, dead_lease.1, now_ms()],
    ).unwrap();
    assert_eq!(updated, 1);
    tx.commit().unwrap();
    let reopened =
        SqliteStorage::open(&file.0, codec.clone(), runtime.time.clone(), options).unwrap();
    let successor_lease = crash_sqlite_lease(&db);
    assert_ne!(successor_lease.0, dead_lease.0);
    assert!(successor_lease.1 > dead_lease.1);
    assert_ne!(
        runtime.pump.drive(reopened.read_stamp()).unwrap(),
        old_stamp
    );
    let reopened_snapshot = SqliteStorage::read_snapshot(&file.0, &codec, options).unwrap();
    assert_eq!(reopened_snapshot.snapshot, before.snapshot);
    assert_eq!(reopened_snapshot.revision, before.revision);
    struct Accept;
    impl PublicationValidator for Accept {
        type Error = ();
        fn validate(&self, _: &pi_agent_session::storage::StorageSnapshot) -> Result<(), ()> {
            Ok(())
        }
    }
    assert_eq!(
        runtime
            .pump
            .drive(reopened.commit_prepared_checked(stale_prepared, stale_request, &Accept))
            .unwrap(),
        ConditionalOutcome::NotApplied,
        "parent-issued older-incarnation token must be refused"
    );
    let after_refusal = SqliteStorage::read_snapshot(&file.0, &codec, options).unwrap();
    assert_eq!(after_refusal.snapshot, before.snapshot);
    assert_eq!(after_refusal.revision, before.revision);
    runtime.pump.drive(reopened.close()).unwrap();
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum PendingRecoveryCase {
    Plain,
    Mismatch,
    ArgsDrift,
    Revoked,
    RevokedDuringResume,
    Cancelled,
    CancelBeforeFinalPair,
    CancelBeforeWrapper,
    Native,
}

fn sqlite_pending_replay_after_process_end(replay: ToolReplay, case: PendingRecoveryCase) {
    use pi_agent_schema::{
        AgentMessage, Entry, HarnessRuntimeError, IdGenerator, SessionError, Storage, StorageError,
    };
    use pi_agent_sqlite::SqliteStorage;
    use pi_ai_core::StopReason;
    use prost::Message as _;
    use runtime_r1_r4_support::assistant;
    use runtime_sqlite_tool_support::{CheckedSqlite, CrashChild, File};

    let replay_name = if case == PendingRecoveryCase::Native {
        "native_safe"
    } else {
        match replay {
            ToolReplay::Safe => "safe",
            ToolReplay::Never => "never",
        }
    };
    let file = File::new();
    let runtime = Setup::new();
    let codec = crash_sqlite_codec();
    let options = crash_sqlite_options();
    let initial = SqliteStorage::create(
        &file.0,
        runtime.session.metadata(),
        codec.clone(),
        runtime.time.clone(),
        options,
    )
    .unwrap();
    runtime.pump.drive(initial.close()).unwrap();

    let mut child = CrashChild::start_pending(&file.0, replay_name);
    let child_high = child.await_pending(replay_name);
    let before = SqliteStorage::read_snapshot(&file.0, &codec, options).unwrap();
    let (key, args) = pending_sqlite_key_and_args(&before.snapshot);
    let mut db = rusqlite::Connection::open(&file.0).unwrap();
    let live_lease = crash_sqlite_lease(&db);
    child.kill_and_reap();
    let after = SqliteStorage::read_snapshot(&file.0, &codec, options).unwrap();
    assert_eq!(
        after.snapshot, before.snapshot,
        "pending K/args/result/usage survive process end"
    );
    assert_eq!(after.revision, before.revision);
    assert_eq!(after.store_token, before.store_token);
    assert_eq!(after.metadata, before.metadata);
    assert_eq!(
        pending_sqlite_key_and_args(&after.snapshot),
        (key.clone(), args.clone())
    );
    let dead_lease = crash_sqlite_lease(&db);
    assert_eq!(dead_lease.0, live_lease.0);
    assert_eq!(dead_lease.1, live_lease.1);
    let now_ms = || {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    };
    assert!(dead_lease.2 > now_ms(), "dead writer lease remains live");
    assert!(matches!(
        SqliteStorage::open(&file.0, codec.clone(), runtime.time.clone(), options),
        Err(SessionError::Storage(StorageError::Busy(_)))
    ));
    let tx = db
        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
        .unwrap();
    let updated = tx.execute(
        "UPDATE writer_lease SET expires_at_ms=0 WHERE singleton=1 AND owner_id=?1 AND fence=?2 AND expires_at_ms>?3",
        rusqlite::params![dead_lease.0, dead_lease.1, now_ms()],
    ).unwrap();
    assert_eq!(updated, 1, "only the private dead lease is expired");
    tx.commit().unwrap();
    let reopened =
        SqliteStorage::open(&file.0, codec.clone(), runtime.time.clone(), options).unwrap();
    let successor_lease = crash_sqlite_lease(&db);
    assert_ne!(successor_lease.0, dead_lease.0);
    assert!(successor_lease.1 > dead_lease.1);
    let reopened_snapshot = SqliteStorage::read_snapshot(&file.0, &codec, options).unwrap();
    assert_eq!(reopened_snapshot.snapshot, before.snapshot);
    assert_eq!(reopened_snapshot.revision, before.revision);

    for expected in 0..=child_high {
        let id = runtime.ids.next(None);
        assert_eq!(id, format!("runtime-id-{expected}"));
    }
    *runtime.faux.responses.lock().unwrap() =
        vec![assistant(&runtime.faux.model, StopReason::Stop)].into();
    let tracked = Arc::new(CheckedSqlite::new(reopened.clone(), Arc::new(|| 0)));
    tracked.first_release.cancel();
    tracked.second_release.cancel();
    let session = Arc::new(
        runtime
            .pump
            .drive(RuntimeSession::new(
                tracked.clone(),
                reopened.metadata().unwrap(),
                codec,
                runtime.ids.clone(),
                Arc::new(pi_telemetry::NOOP_TELEMETRY_CONTEXT),
            ))
            .unwrap(),
    );
    let recovery_session = session.clone();
    let factory = RuntimeFactory::new(
        runtime.time.clone(),
        runtime.pump.clone(),
        runtime.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    let mut create_options = runtime.options_for::<(), _>(session, DriveMode::Automatic);
    create_options.tool_execution = pi_agent_schema::ToolExecutionMode::Sequential;
    let native_entered = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    create_options.tools = if case == PendingRecoveryCase::Native {
        vec![native_echo(true, native_entered.clone())]
    } else if replay == ToolReplay::Safe {
        Vec::new()
    } else {
        vec![registry_echo_declaration(replay)]
    };
    let harness = runtime
        .pump
        .drive(factory.create_configured(create_options))
        .unwrap()
        .harness;
    if case == PendingRecoveryCase::Native {
        runtime.pump.drive(harness.resume()).unwrap().unwrap();
        assert_eq!(
            native_entered.load(Ordering::SeqCst),
            1,
            "native Safe replays once"
        );
        let settled = runtime.pump.drive(reopened.snapshot()).unwrap();
        let results = settled
            .entries
            .iter()
            .filter(|entry| {
                matches!(entry,
            Entry::Message(message) if matches!(&message.message,
                AgentMessage::BuiltIn(pi_ai_core::Message::ToolResult(result))
                if result.tool_call_id.as_str() == "echo-call" && !result.is_error))
            })
            .count();
        assert_eq!(results, 1);
        assert!(
            !settled
                .registers
                .iter()
                .any(|register| matches!(register, Register::OperationToolArguments { .. }))
        );
        runtime.pump.drive(reopened.close()).unwrap();
        return;
    }
    let spawner = Arc::new(FakeSpawner::default());
    let driver_owner = owner(spawner.clone());

    if replay == ToolReplay::Safe {
        let before_resume = Arc::new(ResumeProbe(std::sync::atomic::AtomicUsize::new(0)));
        let _before_subscription = harness
            .hooks()
            .before_resume(
                pi_agent_schema::HandlerId::new("recovered-echo-before-resume").unwrap(),
                before_resume.clone(),
            )
            .unwrap();
        let run_resumes = Arc::new(RegistryEventLog(Mutex::new(vec![])));
        let _run_resume_subscription = harness
            .events()
            .on(HarnessEventType::RunResume, run_resumes.clone());
        let recovered_key = pending_schema_key(&key);
        let original = registry_echo_declaration(ToolReplay::Safe)
            .descriptor()
            .clone();
        let mut invalid = original.clone();
        invalid.replay = Some(pi_agent_schema::ToolReplay::Never);
        assert_eq!(
            runtime
                .pump
                .drive(harness.register_recovered_echo_pending(recovered_key.clone(), invalid))
                .unwrap(),
            Err(pi_harness_runtime::owner::RecoveredEchoRegistrationError::InvalidOriginal)
        );
        runtime
            .pump
            .drive(harness.register_recovered_echo_pending(recovered_key.clone(), original.clone()))
            .unwrap()
            .unwrap();
        runtime
            .pump
            .drive(harness.register_recovered_echo_pending(recovered_key.clone(), original.clone()))
            .unwrap()
            .unwrap();
        let mut conflicting = original.clone();
        conflicting.description.push_str(" changed");
        assert_eq!(
            runtime
                .pump
                .drive(harness.register_recovered_echo_pending(recovered_key.clone(), conflicting,))
                .unwrap(),
            Err(pi_harness_runtime::owner::RecoveredEchoRegistrationError::ConflictingIntent)
        );
        let mut stale = recovered_key.clone();
        stale.logical_attempt = pi_ai_core::SafeUnsigned::new(1).unwrap();
        assert_eq!(
            runtime
                .pump
                .drive(harness.register_recovered_echo_pending(stale, original,))
                .unwrap(),
            Err(pi_harness_runtime::owner::RecoveredEchoRegistrationError::NotCurrent)
        );
        let mut wrong_index = recovered_key.clone();
        wrong_index.item_index = Some(pi_ai_core::SafeUnsigned::new(1).unwrap());
        assert_eq!(
            runtime
                .pump
                .drive(
                    harness.register_recovered_echo_pending(
                        wrong_index,
                        registry_echo_declaration(ToolReplay::Safe)
                            .descriptor()
                            .clone(),
                    )
                )
                .unwrap(),
            Err(pi_harness_runtime::owner::RecoveredEchoRegistrationError::NotCurrent)
        );
        let mismatched_channel = if case == PendingRecoveryCase::Mismatch {
            let (channel, _, route) =
                setup_with_replay(&driver_owner, &spawner, 8, 210, ToolReplay::Never);
            let route = Arc::new(route);
            runtime
                .pump
                .drive(harness.register_ready_echo_tool(
                    registry_echo(&route),
                    route,
                    route_timing(),
                ))
                .unwrap()
                .unwrap();
            Some(channel)
        } else {
            None
        };
        let before_loss =
            SqliteStorage::read_snapshot(&file.0, crash_sqlite_codec().as_ref(), options).unwrap();
        let mut no_route = Box::pin(harness.resume());
        let mut lost = None;
        for _ in 0..1000 {
            if let Poll::Ready(value) = poll(no_route.as_mut()) {
                lost = Some(value);
                break;
            }
            runtime.pump.tick();
            spawner.tick();
        }
        assert!(
            matches!(lost, Some(Err(HarnessRuntimeError::PeerLost))),
            "Safe resume without G2 returned {lost:?}"
        );
        let after_loss =
            SqliteStorage::read_snapshot(&file.0, crash_sqlite_codec().as_ref(), options).unwrap();
        assert_eq!(
            after_loss.snapshot, before_loss.snapshot,
            "PeerLost makes no Session write"
        );
        assert_eq!(
            after_loss.revision, before_loss.revision,
            "PeerLost keeps revision"
        );
        assert_eq!(
            before_resume.0.load(Ordering::SeqCst),
            0,
            "no BeforeResume without replacement"
        );
        assert!(
            run_resumes.0.lock().unwrap().is_empty(),
            "no RunResume without replacement"
        );
        if let Some(channel) = mismatched_channel {
            assert_eq!(channel.writes().len(), 2, "mismatched G2 emits no Request");
        }
        let mut retry_without_replacement = Box::pin(harness.resume());
        let mut retry = None;
        for _ in 0..1000 {
            if let Poll::Ready(value) = poll(retry_without_replacement.as_mut()) {
                retry = Some(value);
                break;
            }
            runtime.pump.tick();
            spawner.tick();
        }
        assert!(
            matches!(retry, Some(Err(HarnessRuntimeError::PeerLost))),
            "first PeerLost must release drive reservation: {retry:?}"
        );
        assert_eq!(
            pending_sqlite_key_and_args(&after_loss.snapshot),
            (key.clone(), args.clone())
        );
        if case == PendingRecoveryCase::ArgsDrift {
            let Register::OperationToolArguments {
                key: args_key,
                value,
                ..
            } = args.clone()
            else {
                panic!("saved args")
            };
            runtime
                .pump
                .drive(recovery_session.commit(pi_agent_schema::Transaction {
                    writes: vec![pi_agent_schema::Write::SetRegister(Box::new(
                        pi_agent_schema::RegisterSet::OperationToolArguments {
                            key: args_key,
                            value,
                        },
                    ))],
                }))
                .unwrap();
            let mut resumed = Box::pin(harness.resume());
            let mut result = None;
            for _ in 0..1000 {
                if let Poll::Ready(value) = poll(resumed.as_mut()) {
                    result = Some(value);
                    break;
                }
                runtime.pump.tick();
                spawner.tick();
            }
            assert!(
                matches!(result, Some(Err(HarnessRuntimeError::Fault(_)))),
                "changed args sequence must fault: {result:?}"
            );
            runtime.pump.drive(reopened.close()).unwrap();
            return;
        }
        if case == PendingRecoveryCase::Cancelled {
            runtime.pump.drive(harness.abort()).unwrap().unwrap();
            let mut settled = None;
            for _ in 0..1000 {
                runtime.pump.tick();
                spawner.tick();
                let snapshot = runtime.pump.drive(reopened.snapshot()).unwrap();
                if snapshot.entries.iter().any(|entry| {
                    matches!(entry,
                    Entry::Message(message) if matches!(&message.message,
                        AgentMessage::BuiltIn(pi_ai_core::Message::ToolResult(_))))
                }) {
                    settled = Some(snapshot);
                    break;
                }
            }
            let settled = settled.expect("cancel reconciliation settled");
            assert_one_synthetic_interrupted_result(&settled);
            assert!(
                !settled
                    .registers
                    .iter()
                    .any(|register| matches!(register, Register::OperationToolArguments { .. }))
            );
            let mut operation_finished = false;
            for _ in 0..1000 {
                runtime.pump.tick();
                spawner.tick();
                let snapshot = runtime.pump.drive(reopened.snapshot()).unwrap();
                if snapshot.registers.iter().any(|register| {
                    matches!(register,
                    Register::LaneState { key, value, .. }
                    if key == "main" && value.current_operation_id.is_none())
                }) {
                    operation_finished = true;
                    break;
                }
            }
            assert!(
                operation_finished,
                "cancelled operation finishes before stale retry"
            );
            let (channel, _, route) =
                setup_with_replay(&driver_owner, &spawner, 8, 210, ToolReplay::Safe);
            let route = Arc::new(route);
            runtime
                .pump
                .drive(harness.register_ready_echo_tool(
                    registry_echo(&route),
                    route,
                    route_timing(),
                ))
                .unwrap()
                .unwrap();
            let before_stale =
                SqliteStorage::read_snapshot(&file.0, crash_sqlite_codec().as_ref(), options)
                    .unwrap();
            assert!(matches!(
                runtime.pump.drive(harness.resume()).unwrap(),
                Err(pi_agent_schema::ResumeExpectedError::NothingToResume(_))
            ));
            let after_stale =
                SqliteStorage::read_snapshot(&file.0, crash_sqlite_codec().as_ref(), options)
                    .unwrap();
            assert_eq!(before_stale.snapshot, after_stale.snapshot);
            assert_eq!(before_stale.revision, after_stale.revision);
            assert_eq!(channel.writes().len(), 2, "retired K emits no Request");
            runtime.pump.drive(reopened.close()).unwrap();
            return;
        }
    }

    let (g2_channel, _, g2) = setup_with_replay(
        &driver_owner,
        &spawner,
        if case == PendingRecoveryCase::Mismatch {
            9
        } else {
            8
        },
        if case == PendingRecoveryCase::Mismatch {
            220
        } else {
            210
        },
        replay,
    );
    let g2 = Arc::new(g2);
    let revoke_on_descriptor = Arc::new(AtomicBool::new(false));
    let g2_handle = if case == PendingRecoveryCase::RevokedDuringResume {
        let route = g2.clone();
        let armed = revoke_on_descriptor.clone();
        registry_echo_with_probe(
            &g2,
            Some(Arc::new(move || {
                if armed.swap(false, Ordering::SeqCst) {
                    route.stop_new_calls();
                }
            })),
        )
    } else {
        registry_echo(&g2)
    };
    runtime
        .pump
        .drive(harness.register_ready_echo_tool(g2_handle, g2.clone(), route_timing()))
        .unwrap()
        .unwrap();
    let mut g2_channel = g2_channel;
    if matches!(
        case,
        PendingRecoveryCase::Revoked | PendingRecoveryCase::RevokedDuringResume
    ) {
        if case == PendingRecoveryCase::Revoked {
            g2.stop_new_calls();
        } else {
            revoke_on_descriptor.store(true, Ordering::SeqCst);
        }
        let mut revoked = Box::pin(harness.resume());
        let mut result = None;
        for _ in 0..1000 {
            if let Poll::Ready(value) = poll(revoked.as_mut()) {
                result = Some(value);
                break;
            }
            runtime.pump.tick();
            spawner.tick();
        }
        assert!(
            matches!(result, Some(Err(HarnessRuntimeError::PeerLost))),
            "revoked route must not claim: {result:?}"
        );
        assert_eq!(
            g2_channel.writes().len(),
            2,
            "revoked route emits no Request"
        );
        let (channel, _, route) = setup_with_replay(&driver_owner, &spawner, 9, 220, replay);
        let route = Arc::new(route);
        runtime
            .pump
            .drive(harness.register_ready_echo_tool(registry_echo(&route), route, route_timing()))
            .unwrap()
            .unwrap();
        g2_channel = channel;
    }
    if matches!(
        case,
        PendingRecoveryCase::CancelBeforeFinalPair | PendingRecoveryCase::CancelBeforeWrapper
    ) {
        use pi_agent_schema::{
            CancelRequestedControl, Control, OperationState, RegisterSet, Transaction, Write,
        };
        let starts = Arc::new(RegistryEventLog(Mutex::new(vec![])));
        let _start_subscription = harness
            .events()
            .on(HarnessEventType::ToolStart, starts.clone());
        let baseline = tracked.args_reads.load(Ordering::SeqCst);
        let offset = if case == PendingRecoveryCase::CancelBeforeFinalPair {
            6
        } else {
            7
        };
        tracked
            .args_read_target
            .store(baseline + offset, Ordering::SeqCst);
        let mut resume = Box::pin(harness.resume());
        let mut completed_early = None;
        for _ in 0..1000 {
            if let Poll::Ready(value) = poll(resume.as_mut()) {
                completed_early = Some(value);
                break;
            }
            runtime.pump.tick();
            spawner.tick();
            if tracked.args_read_entered.is_cancelled() {
                break;
            }
        }
        assert!(
            tracked.args_read_entered.is_cancelled(),
            "target read not reached: baseline={baseline} offset={offset} reads={} early={completed_early:?}",
            tracked.args_reads.load(Ordering::SeqCst)
        );
        for _ in 0..10 {
            runtime.pump.tick();
            spawner.tick();
        }
        let start_count = starts.0.lock().unwrap().len();
        assert_eq!(
            start_count,
            usize::from(case == PendingRecoveryCase::CancelBeforeWrapper),
            "pause stage: baseline={baseline} offset={offset} reads={}",
            tracked.args_reads.load(Ordering::SeqCst)
        );
        let before_cancel = runtime.pump.drive(reopened.snapshot()).unwrap();
        let (operation_id, mut state) = before_cancel
            .registers
            .iter()
            .find_map(|register| {
                if let Register::OperationState { key, value, .. } = register {
                    Some((key.clone(), value.as_ref().clone()))
                } else {
                    None
                }
            })
            .expect("current pending operation state");
        let OperationState::Run(run) = &mut state else {
            panic!("run state")
        };
        run.control = Control::CancelRequested(CancelRequestedControl {
            status: Default::default(),
            requested_at: pi_ai_core::SafeUnsigned::new(7).unwrap(),
            drained_steer: vec![],
            drained_follow_up: vec![],
        });
        runtime
            .pump
            .drive(recovery_session.commit(Transaction {
                writes: vec![Write::SetRegister(Box::new(RegisterSet::OperationState {
                    key: operation_id,
                    value: Box::new(state),
                }))],
            }))
            .unwrap();
        tracked.args_read_release.cancel();
        let mut outcome = None;
        for _ in 0..1000 {
            if let Poll::Ready(value) = poll(resume.as_mut()) {
                outcome = Some(value);
                break;
            }
            runtime.pump.tick();
            spawner.tick();
        }
        assert!(
            matches!(outcome, Some(Ok(Ok(_)))),
            "cancelled recovered effect must reconcile: {outcome:?}"
        );
        assert_eq!(
            g2_channel.writes().len(),
            2,
            "cancelled effect emits no Request"
        );
        let settled = runtime.pump.drive(reopened.snapshot()).unwrap();
        let results = settled
            .entries
            .iter()
            .filter_map(|entry| match entry {
                Entry::Message(message)
                    if matches!(
                        &message.message,
                        AgentMessage::BuiltIn(pi_ai_core::Message::ToolResult(_))
                    ) =>
                {
                    Some(message)
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(results.len(), 1, "one core cancellation result");
        let AgentMessage::BuiltIn(pi_ai_core::Message::ToolResult(result)) = &results[0].message
        else {
            unreachable!()
        };
        assert!(result.is_error && result.usage.is_none());
        assert!(
            matches!(&result.content[..], [pi_ai_core::InputContent::Text(text)]
            if text.text == "Operation aborted" || text.text == "Tool execution was interrupted")
        );
        assert!(
            !settled
                .usage
                .iter()
                .any(|row| row.entry_id.as_ref() == Some(&results[0].id))
        );
        assert!(
            !settled
                .registers
                .iter()
                .any(|register| matches!(register, Register::OperationToolArguments { .. }))
        );
        runtime.pump.drive(reopened.close()).unwrap();
        return;
    }
    let mut resume = Box::pin(harness.resume());
    if replay == ToolReplay::Safe {
        let mut outbound = None;
        for _ in 0..1000 {
            let _ = poll(resume.as_mut());
            runtime.pump.tick();
            spawner.tick();
            if let Some(bytes) = g2_channel.writes().get(2) {
                outbound = Some(written_request(bytes));
                break;
            }
        }
        let request = outbound.expect("one fresh G2 Request");
        assert_eq!(g2_channel.writes().len(), 3);
        assert_eq!(
            runtime
                .pump
                .drive(
                    harness.register_recovered_echo_pending(
                        pending_schema_key(&key),
                        registry_echo_declaration(ToolReplay::Safe)
                            .descriptor()
                            .clone(),
                    )
                )
                .unwrap(),
            Err(pi_harness_runtime::owner::RecoveredEchoRegistrationError::LaneBusy)
        );
        assert_eq!(request.method, "tool.invoke");
        assert_eq!(request.initial_stream_credit_bytes, Some(0));
        assert_eq!(request.effect_key, Some(key));
        let invocation = v1_1::InvokeToolRequest::decode(request.payload.as_slice()).unwrap();
        assert_eq!(invocation.tool_call_id, "echo-call");
        assert_eq!(invocation.tool_id, "echo");
        let Some(v1_1::wire_value::Value::MapValue(arguments)) = invocation
            .arguments
            .and_then(|bound| bound.value)
            .and_then(|value| value.value)
        else {
            panic!("resumed wire arguments")
        };
        assert_eq!(
            arguments,
            v1_1::WireMap {
                entries: vec![v1_1::WireMapEntry {
                    key: "text".into(),
                    value: Some(v1_1::WireValue {
                        value: Some(v1_1::wire_value::Value::TextValue("ping".into())),
                    }),
                }]
            }
        );
        g2_channel.push(raw_terminal_with_usage(request.request_id));
        let mut completed = None;
        for _ in 0..1000 {
            spawner.tick();
            runtime.pump.tick();
            if let Poll::Ready(value) = poll(resume.as_mut()) {
                completed = Some(value);
                break;
            }
        }
        completed
            .expect("G2 terminal must settle")
            .unwrap()
            .unwrap();
        let settled = runtime.pump.drive(reopened.snapshot()).unwrap();
        let results = settled
            .entries
            .iter()
            .filter_map(|entry| match entry {
                Entry::Message(message)
                    if matches!(&message.message,
                AgentMessage::BuiltIn(pi_ai_core::Message::ToolResult(result))
                if result.tool_call_id.as_str() == "echo-call" && !result.is_error) =>
                {
                    Some(message)
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(results.len(), 1);
        let linked = settled
            .usage
            .iter()
            .filter(|row| row.entry_id.as_ref() == Some(&results[0].id))
            .collect::<Vec<_>>();
        assert_eq!(linked.len(), 1);
        assert_eq!(linked[0].usage.input.get(), 5);
        assert!(
            !settled
                .registers
                .iter()
                .any(|register| matches!(register, Register::OperationToolArguments { .. }))
        );
    } else {
        let mut completed = None;
        for _ in 0..1000 {
            if let Poll::Ready(value) = poll(resume.as_mut()) {
                completed = Some(value);
                break;
            }
            runtime.pump.tick();
            spawner.tick();
        }
        completed
            .expect("Never reconciliation must finish")
            .unwrap()
            .unwrap();
        assert_eq!(g2_channel.writes().len(), 2, "Never emits no G2 Request");
        let settled = runtime.pump.drive(reopened.snapshot()).unwrap();
        assert_one_synthetic_interrupted_result(&settled);
    }
    runtime.pump.drive(reopened.close()).unwrap();
}

#[test]
fn actual_sqlite_pending_safe_replays_after_process_end() {
    sqlite_pending_replay_after_process_end(ToolReplay::Safe, PendingRecoveryCase::Plain);
}

#[test]
fn actual_sqlite_pending_safe_waits_for_matching_replacement() {
    sqlite_pending_replay_after_process_end(ToolReplay::Safe, PendingRecoveryCase::Mismatch);
}

#[test]
fn actual_sqlite_pending_safe_rejects_changed_args_sequence() {
    sqlite_pending_replay_after_process_end(ToolReplay::Safe, PendingRecoveryCase::ArgsDrift);
}

#[test]
fn actual_sqlite_pending_safe_waits_after_route_revocation() {
    sqlite_pending_replay_after_process_end(ToolReplay::Safe, PendingRecoveryCase::Revoked);
}

#[test]
fn actual_sqlite_pending_safe_blocks_fallback_after_preflight() {
    sqlite_pending_replay_after_process_end(
        ToolReplay::Safe,
        PendingRecoveryCase::RevokedDuringResume,
    );
}

#[test]
fn actual_sqlite_pending_safe_cancel_uses_core_reconciliation() {
    sqlite_pending_replay_after_process_end(ToolReplay::Safe, PendingRecoveryCase::Cancelled);
}

#[test]
fn recovered_safe_cancel_before_final_pair_preserves_core_reconciliation() {
    sqlite_pending_replay_after_process_end(
        ToolReplay::Safe,
        PendingRecoveryCase::CancelBeforeFinalPair,
    );
}

#[test]
fn recovered_safe_cancel_before_wrapper_preserves_core_reconciliation() {
    sqlite_pending_replay_after_process_end(
        ToolReplay::Safe,
        PendingRecoveryCase::CancelBeforeWrapper,
    );
}

#[test]
fn actual_sqlite_pending_never_reconciles_without_redispatch() {
    sqlite_pending_replay_after_process_end(ToolReplay::Never, PendingRecoveryCase::Plain);
}

#[test]
fn actual_sqlite_pending_native_safe_replays_without_route_intent() {
    sqlite_pending_replay_after_process_end(ToolReplay::Safe, PendingRecoveryCase::Native);
}

#[test]
fn actual_memory_checked_refusal_rejects_changed_saved_args() {
    checked_refusal_memory_route(true, false, false);
}

#[test]
fn actual_memory_checked_refusal_rejects_retired_b1_on_retry() {
    checked_refusal_memory_route(false, true, false);
}

#[test]
fn actual_memory_post_commit_hydration_fault_seals_after_pair_release() {
    checked_refusal_memory_route(false, false, true);
}

#[test]
fn actual_memory_unadmitted_abort_reconciles_once_without_wire_q_or_c() {
    use pi_ai_core::{AssistantContent, StopReason, ToolCall, ToolCallId, ToolName};
    use runtime_r1_r4_support::assistant;

    let runtime = Setup::new();
    let mut response = assistant(&runtime.faux.model, StopReason::ToolUse);
    response.content = vec![AssistantContent::ToolCall(ToolCall {
        content_type: Default::default(),
        id: ToolCallId::new("echo-call").unwrap(),
        name: ToolName::new("echo").unwrap(),
        arguments: saved(),
        thought_signature: None,
        namespace: None,
    })];
    *runtime.faux.responses.lock().unwrap() = vec![response].into();
    let factory = RuntimeFactory::new(
        runtime.time.clone(),
        runtime.pump.clone(),
        runtime.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    let harness = runtime
        .pump
        .drive(factory.create_configured(runtime.options::<()>(DriveMode::Automatic)))
        .unwrap()
        .harness;
    let spawner = Arc::new(FakeSpawner::default());
    let driver_owner = owner(spawner.clone());
    let (channel, _, route) = setup(&driver_owner, &spawner, 7, 200);
    let route = Arc::new(route);
    runtime
        .pump
        .drive(harness.register_ready_echo_tool(registry_echo(&route), route, route_timing()))
        .unwrap()
        .unwrap();
    runtime
        .pump
        .drive(harness.set_active_tools(vec![ToolName::new("echo").unwrap()]))
        .unwrap();
    let after = Arc::new(runtime_r6_retirement_support::AfterToolCount::default());
    let _after_subscription = harness.hooks().after_tool(None, after.clone()).unwrap();
    let ends = Arc::new(RegistryEventLog(Mutex::new(vec![])));
    let _end_subscription = harness.events().on(HarnessEventType::ToolEnd, ends.clone());
    let mut prompt = Box::pin(harness.prompt(PromptInput::Text {
        text: "invoke echo".into(),
        images: vec![],
    }));
    for _ in 0..1000 {
        let _ = poll(prompt.as_mut());
        runtime.pump.tick();
        if driver_owner.retained_binding().is_some() {
            break;
        }
    }
    assert!(
        driver_owner.retained_binding().is_some(),
        "G1 must be claimed"
    );
    assert_eq!(channel.writes().len(), 2, "writer has not admitted Q");
    runtime.pump.drive(harness.abort()).unwrap().unwrap();
    let mut completed = None;
    for _ in 0..1000 {
        runtime.pump.tick();
        spawner.tick();
        if let Poll::Ready(value) = poll(prompt.as_mut()) {
            completed = Some(value);
            break;
        }
    }
    assert!(completed.is_some(), "cancelled core must reconcile");
    assert_eq!(
        channel.writes().len(),
        2,
        "unadmitted abort writes neither Q nor C"
    );
    assert!(driver_owner.retained_binding().is_none());
    let snapshot = runtime.pump.drive(runtime.store.snapshot()).unwrap();
    assert_one_synthetic_interrupted_result(&snapshot);
    assert_eq!(
        after.0.load(Ordering::SeqCst),
        0,
        "no after_tool for private abort"
    );
    assert!(
        ends.0.lock().unwrap().is_empty(),
        "no ToolEnd for private abort"
    );
}

#[test]
fn actual_memory_abort_at_effect_start_skips_wrapper_then_reconciles_once() {
    use pi_agent_schema::RegisterNamespace;
    use pi_ai_core::{AssistantContent, StopReason, ToolCall, ToolCallId, ToolName};
    use runtime_r1_r4_support::assistant;

    let runtime = Setup::new();
    let mut response = assistant(&runtime.faux.model, StopReason::ToolUse);
    response.content = vec![AssistantContent::ToolCall(ToolCall {
        content_type: Default::default(),
        id: ToolCallId::new("echo-call").unwrap(),
        name: ToolName::new("echo").unwrap(),
        arguments: saved(),
        thought_signature: None,
        namespace: None,
    })];
    *runtime.faux.responses.lock().unwrap() = vec![response].into();
    let factory = RuntimeFactory::new(
        runtime.time.clone(),
        runtime.pump.clone(),
        runtime.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    let harness = runtime
        .pump
        .drive(factory.create_configured(runtime.options::<()>(DriveMode::Manual)))
        .unwrap()
        .harness;
    let spawner = Arc::new(FakeSpawner::default());
    let driver_owner = owner(spawner.clone());
    let (channel, _, route) = setup(&driver_owner, &spawner, 7, 200);
    let route = Arc::new(route);
    runtime
        .pump
        .drive(harness.register_ready_echo_tool(registry_echo(&route), route, route_timing()))
        .unwrap()
        .unwrap();
    runtime
        .pump
        .drive(harness.set_active_tools(vec![ToolName::new("echo").unwrap()]))
        .unwrap();
    let after = Arc::new(runtime_r6_retirement_support::AfterToolCount::default());
    let _after_subscription = harness.hooks().after_tool(None, after.clone()).unwrap();
    let ends = Arc::new(RegistryEventLog(Mutex::new(vec![])));
    let _end_subscription = harness.events().on(HarnessEventType::ToolEnd, ends.clone());
    let mut prompt = Box::pin(harness.prompt(PromptInput::Text {
        text: "invoke echo".into(),
        images: vec![],
    }));
    let mut parked = false;
    for _ in 0..2000 {
        let _ = poll(prompt.as_mut());
        runtime.pump.tick();
        spawner.tick();
        if runtime
            .pump
            .drive(harness.peek_action())
            .unwrap()
            .is_some_and(|action| action.kind == "effect_start")
            && runtime
                .pump
                .drive(
                    runtime
                        .session
                        .list_registers(RegisterNamespace::OperationToolArguments, None),
                )
                .unwrap()
                .len()
                == 1
        {
            parked = true;
            break;
        }
        if runtime.pump.drive(harness.peek_action()).unwrap().is_some() {
            runtime.pump.drive(harness.execute_action()).unwrap();
        }
    }
    assert!(
        parked,
        "durably pending echo parked before loop cancellation gate"
    );
    assert!(driver_owner.retained_binding().is_none());
    runtime.pump.drive(harness.abort()).unwrap().unwrap();
    let mut completed = None;
    let mut checked_reservation = false;
    for _ in 0..1000 {
        runtime.pump.tick();
        spawner.tick();
        if let Poll::Ready(value) = poll(prompt.as_mut()) {
            completed = Some(value);
            break;
        }
        if !checked_reservation
            && runtime
                .pump
                .drive(harness.peek_action())
                .unwrap()
                .is_some_and(|action| action.kind == "commit_settlement")
        {
            assert!(matches!(
                runtime.pump.drive(harness.resume()).unwrap(),
                Err(pi_agent_schema::ResumeExpectedError::LaneBusy(_))
            ));
            checked_reservation = true;
        }
        if runtime.pump.drive(harness.peek_action()).unwrap().is_some() {
            runtime.pump.drive(harness.execute_action()).unwrap();
        }
    }
    assert!(completed.is_some());
    assert!(
        checked_reservation,
        "canceled-core iteration retains drive reservation"
    );
    assert_eq!(channel.writes().len(), 2, "skipped wrapper sends no Q or C");
    let snapshot = runtime.pump.drive(runtime.store.snapshot()).unwrap();
    assert_one_synthetic_interrupted_result(&snapshot);
    assert_eq!(
        after.0.load(Ordering::SeqCst),
        0,
        "no after_tool for private abort"
    );
    assert!(
        ends.0.lock().unwrap().is_empty(),
        "no ToolEnd for private abort"
    );
}

#[test]
fn actual_memory_admitted_abort_keeps_real_terminal_path() {
    use pi_ai_core::{AssistantContent, StopReason, ToolCall, ToolCallId, ToolName};
    use runtime_r1_r4_support::assistant;

    let runtime = Setup::new();
    let mut response = assistant(&runtime.faux.model, StopReason::ToolUse);
    response.content = vec![AssistantContent::ToolCall(ToolCall {
        content_type: Default::default(),
        id: ToolCallId::new("echo-call").unwrap(),
        name: ToolName::new("echo").unwrap(),
        arguments: saved(),
        thought_signature: None,
        namespace: None,
    })];
    *runtime.faux.responses.lock().unwrap() = vec![response].into();
    let factory = RuntimeFactory::new(
        runtime.time.clone(),
        runtime.pump.clone(),
        runtime.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    let harness = runtime
        .pump
        .drive(factory.create_configured(runtime.options::<()>(DriveMode::Automatic)))
        .unwrap()
        .harness;
    let spawner = Arc::new(FakeSpawner::default());
    let driver_owner = owner(spawner.clone());
    let (channel, _, route) = setup(&driver_owner, &spawner, 7, 200);
    let route = Arc::new(route);
    runtime
        .pump
        .drive(harness.register_ready_echo_tool(registry_echo(&route), route, route_timing()))
        .unwrap()
        .unwrap();
    runtime
        .pump
        .drive(harness.set_active_tools(vec![ToolName::new("echo").unwrap()]))
        .unwrap();
    let mut prompt = Box::pin(harness.prompt(PromptInput::Text {
        text: "invoke echo".into(),
        images: vec![],
    }));
    let mut request = None;
    for _ in 0..1000 {
        let _ = poll(prompt.as_mut());
        runtime.pump.tick();
        spawner.tick();
        if let Some(bytes) = channel.writes().get(2) {
            request = Some(written_request(bytes));
            break;
        }
    }
    let request = request.expect("Q admitted before local abort");
    runtime.pump.drive(harness.abort()).unwrap().unwrap();
    for _ in 0..1000 {
        runtime.pump.tick();
        spawner.tick();
        if channel.writes().len() >= 4 {
            break;
        }
    }
    assert_eq!(channel.writes().len(), 4, "admitted Q keeps ordered C");
    assert!(driver_owner.retained_binding().is_some());
    channel.push(raw("terminal", request.request_id as u8));
    let mut completed = None;
    for _ in 0..1000 {
        spawner.tick();
        runtime.pump.tick();
        if let Poll::Ready(value) = poll(prompt.as_mut()) {
            completed = Some(value);
            break;
        }
    }
    assert!(completed.is_some(), "validated terminal settles after C");
    let snapshot = runtime.pump.drive(runtime.store.snapshot()).unwrap();
    let result = snapshot
        .entries
        .iter()
        .find_map(|entry| match entry {
            pi_agent_schema::Entry::Message(message) => match &message.message {
                pi_agent_schema::AgentMessage::BuiltIn(pi_ai_core::Message::ToolResult(result)) => {
                    Some(result)
                }
                _ => None,
            },
            _ => None,
        })
        .expect("one terminal-derived result");
    assert!(
        !result.is_error,
        "started/admitted call cannot become synthetic abort"
    );
    assert!(driver_owner.retained_binding().is_none());
}

#[test]
fn actual_memory_key_loss_after_unadmitted_release_writes_nothing() {
    use pi_ai_core::{AssistantContent, StopReason, ToolCall, ToolCallId, ToolName};
    use runtime_r1_r4_support::assistant;

    let runtime = Setup::new();
    let mut response = assistant(&runtime.faux.model, StopReason::ToolUse);
    response.content = vec![AssistantContent::ToolCall(ToolCall {
        content_type: Default::default(),
        id: ToolCallId::new("echo-call").unwrap(),
        name: ToolName::new("echo").unwrap(),
        arguments: saved(),
        thought_signature: None,
        namespace: None,
    })];
    *runtime.faux.responses.lock().unwrap() = vec![response].into();
    let factory = RuntimeFactory::new(
        runtime.time.clone(),
        runtime.pump.clone(),
        runtime.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    let harness = runtime
        .pump
        .drive(factory.create_configured(runtime.options::<()>(DriveMode::Manual)))
        .unwrap()
        .harness;
    let spawner = Arc::new(FakeSpawner::default());
    let driver_owner = owner(spawner.clone());
    let (g1_channel, _, g1) = setup(&driver_owner, &spawner, 7, 200);
    let g1 = Arc::new(g1);
    let echo = registry_echo(&g1);
    runtime
        .pump
        .drive(harness.register_ready_echo_tool(echo.clone(), g1, route_timing()))
        .unwrap()
        .unwrap();
    runtime
        .pump
        .drive(harness.set_active_tools(vec![ToolName::new("echo").unwrap()]))
        .unwrap();
    let mut prompt = Box::pin(harness.prompt(PromptInput::Text {
        text: "invoke echo".into(),
        images: vec![],
    }));
    for _ in 0..2000 {
        let _ = poll(prompt.as_mut());
        runtime.pump.tick();
        if driver_owner.retained_binding().is_some() {
            break;
        }
        if runtime.pump.drive(harness.peek_action()).unwrap().is_some() {
            runtime.pump.drive(harness.execute_action()).unwrap();
        }
    }
    assert!(driver_owner.retained_binding().is_some());
    assert_eq!(g1_channel.writes().len(), 2);
    runtime.pump.drive(harness.abort()).unwrap().unwrap();
    let mut parked = false;
    for _ in 0..2000 {
        runtime.pump.tick();
        if driver_owner.retained_binding().is_none()
            && runtime
                .pump
                .drive(harness.peek_action())
                .unwrap()
                .is_some_and(|action| action.kind == "commit_settlement")
        {
            parked = true;
            break;
        }
        if runtime.pump.drive(harness.peek_action()).unwrap().is_some() {
            runtime.pump.drive(harness.execute_action()).unwrap();
        }
    }
    assert!(parked, "B1 released before cancelled core apply");
    assert_eq!(g1_channel.writes().len(), 2);
    externally_abort_pending_echo(&runtime);
    let external = runtime.pump.drive(runtime.store.snapshot()).unwrap();
    let (g2_channel, _, g2) = setup(&driver_owner, &spawner, 8, 210);
    runtime
        .pump
        .drive(harness.register_ready_echo_tool(echo, Arc::new(g2), route_timing()))
        .unwrap()
        .unwrap();
    runtime.pump.drive(harness.execute_action()).unwrap();
    let mut completed = None;
    for _ in 0..1000 {
        runtime.pump.tick();
        spawner.tick();
        if let Poll::Ready(value) = poll(prompt.as_mut()) {
            completed = Some(value);
            break;
        }
    }
    assert!(
        completed.is_some(),
        "external result must win cancelled CAS"
    );
    assert_eq!(
        runtime.pump.drive(runtime.store.snapshot()).unwrap(),
        external,
        "released B1 cannot cause a synthetic result after key loss"
    );
    assert_eq!(g2_channel.writes().len(), 2, "G2 is untouched");
    assert!(driver_owner.retained_binding().is_none());
}

#[test]
fn actual_memory_external_completion_fences_open_g1_without_result() {
    use pi_ai_core::{AssistantContent, StopReason, ToolCall, ToolCallId, ToolName};
    use runtime_r1_r4_support::assistant;

    let runtime = Setup::new();
    let mut response = assistant(&runtime.faux.model, StopReason::ToolUse);
    response.content = vec![AssistantContent::ToolCall(ToolCall {
        content_type: Default::default(),
        id: ToolCallId::new("echo-call").unwrap(),
        name: ToolName::new("echo").unwrap(),
        arguments: saved(),
        thought_signature: None,
        namespace: None,
    })];
    *runtime.faux.responses.lock().unwrap() = vec![response].into();
    let factory = RuntimeFactory::new(
        runtime.time.clone(),
        runtime.pump.clone(),
        runtime.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    let harness = runtime
        .pump
        .drive(factory.create_configured(runtime.options::<()>(DriveMode::Automatic)))
        .unwrap()
        .harness;
    let spawner = Arc::new(FakeSpawner::default());
    let driver_owner = owner(spawner.clone());
    let (channel, _, route) = setup(&driver_owner, &spawner, 7, 200);
    let route = Arc::new(route);
    runtime
        .pump
        .drive(harness.register_ready_echo_tool(registry_echo(&route), route, route_timing()))
        .unwrap()
        .unwrap();
    runtime
        .pump
        .drive(harness.set_active_tools(vec![ToolName::new("echo").unwrap()]))
        .unwrap();
    let mut prompt = Box::pin(harness.prompt(PromptInput::Text {
        text: "invoke echo".into(),
        images: vec![],
    }));
    for _ in 0..1000 {
        let _ = poll(prompt.as_mut());
        runtime.pump.tick();
        spawner.tick();
        if channel.writes().len() == 3 {
            break;
        }
    }
    assert_eq!(channel.writes().len(), 3, "G1 Request admitted");
    externally_abort_pending_echo(&runtime);
    let external = runtime.pump.drive(runtime.store.snapshot()).unwrap();
    channel.eof();
    let mut completed = None;
    for _ in 0..1000 {
        spawner.tick();
        runtime.pump.tick();
        if let Poll::Ready(value) = poll(prompt.as_mut()) {
            completed = Some(value);
            break;
        }
    }
    assert!(
        completed.is_some(),
        "external completion must release AwaitEffect"
    );
    assert_eq!(
        runtime.pump.drive(runtime.store.snapshot()).unwrap(),
        external,
        "late G1 completion writes no result or usage"
    );
    assert!(
        driver_owner.retained_binding().is_none(),
        "exact G1 binding is released"
    );
}

#[test]
fn actual_memory_external_completion_retires_retained_g1_before_finalize() {
    use pi_ai_core::{AssistantContent, StopReason, ToolCall, ToolCallId, ToolName};
    use runtime_r1_r4_support::assistant;

    let runtime = Setup::new();
    let mut response = assistant(&runtime.faux.model, StopReason::ToolUse);
    response.content = vec![AssistantContent::ToolCall(ToolCall {
        content_type: Default::default(),
        id: ToolCallId::new("echo-call").unwrap(),
        name: ToolName::new("echo").unwrap(),
        arguments: saved(),
        thought_signature: None,
        namespace: None,
    })];
    *runtime.faux.responses.lock().unwrap() = vec![response].into();
    let factory = RuntimeFactory::new(
        runtime.time.clone(),
        runtime.pump.clone(),
        runtime.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    let harness = runtime
        .pump
        .drive(factory.create_configured(runtime.options::<()>(DriveMode::Manual)))
        .unwrap()
        .harness;
    let spawner = Arc::new(FakeSpawner::default());
    let driver_owner = owner(spawner.clone());
    let (channel, _, route) = setup(&driver_owner, &spawner, 7, 200);
    let route = Arc::new(route);
    runtime
        .pump
        .drive(harness.register_ready_echo_tool(registry_echo(&route), route, route_timing()))
        .unwrap()
        .unwrap();
    runtime
        .pump
        .drive(harness.set_active_tools(vec![ToolName::new("echo").unwrap()]))
        .unwrap();
    let mut prompt = Box::pin(harness.prompt(PromptInput::Text {
        text: "invoke echo".into(),
        images: vec![],
    }));
    let mut request = None;
    for _ in 0..2000 {
        let _ = poll(prompt.as_mut());
        runtime.pump.tick();
        spawner.tick();
        if let Some(bytes) = channel.writes().get(2) {
            request = Some(written_request(bytes));
            break;
        }
        if runtime.pump.drive(harness.peek_action()).unwrap().is_some() {
            runtime.pump.drive(harness.execute_action()).unwrap();
        }
    }
    let request = request.expect("manual drive reaches G1 Request");
    channel.push(raw("terminal", request.request_id as u8));
    let mut parked = false;
    for _ in 0..1000 {
        spawner.tick();
        runtime.pump.tick();
        if runtime
            .pump
            .drive(harness.peek_action())
            .unwrap()
            .is_some_and(|action| action.kind == "finalize_tool")
        {
            parked = true;
            break;
        }
        if runtime.pump.drive(harness.peek_action()).unwrap().is_some() {
            runtime.pump.drive(harness.execute_action()).unwrap();
        }
    }
    assert!(parked, "terminal retained at finalize gate");
    externally_abort_pending_echo(&runtime);
    let external = runtime.pump.drive(runtime.store.snapshot()).unwrap();
    runtime.pump.drive(harness.execute_action()).unwrap();
    let mut completed = None;
    for _ in 0..1000 {
        runtime.pump.tick();
        spawner.tick();
        if let Poll::Ready(value) = poll(prompt.as_mut()) {
            completed = Some(value);
            break;
        }
        if runtime.pump.drive(harness.peek_action()).unwrap().is_some() {
            runtime.pump.drive(harness.execute_action()).unwrap();
        }
    }
    assert!(
        completed.is_some(),
        "external result must return after finalize"
    );
    assert_eq!(
        runtime.pump.drive(runtime.store.snapshot()).unwrap(),
        external
    );
    assert!(
        driver_owner.retained_binding().is_none(),
        "retained G1 receipt retired"
    );
}

#[test]
fn actual_memory_retired_b1_before_finalize_skips_after_tool() {
    use pi_ai_core::{AssistantContent, StopReason, ToolCall, ToolCallId, ToolName};
    use pi_extension_protocol::tool_live::RetireReason;
    use runtime_r1_r4_support::assistant;

    let runtime = Setup::new();
    let mut response = assistant(&runtime.faux.model, StopReason::ToolUse);
    response.content = vec![AssistantContent::ToolCall(ToolCall {
        content_type: Default::default(),
        id: ToolCallId::new("echo-call").unwrap(),
        name: ToolName::new("echo").unwrap(),
        arguments: saved(),
        thought_signature: None,
        namespace: None,
    })];
    *runtime.faux.responses.lock().unwrap() = vec![response].into();
    let factory = RuntimeFactory::new(
        runtime.time.clone(),
        runtime.pump.clone(),
        runtime.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    let harness = runtime
        .pump
        .drive(factory.create_configured(runtime.options::<()>(DriveMode::Manual)))
        .unwrap()
        .harness;
    let after = Arc::new(runtime_r6_retirement_support::AfterToolCount::default());
    let _after_subscription = harness.hooks().after_tool(None, after.clone()).unwrap();
    let spawner = Arc::new(FakeSpawner::default());
    let driver_owner = owner(spawner.clone());
    let (channel, _, route) = setup(&driver_owner, &spawner, 7, 200);
    let route = Arc::new(route);
    runtime
        .pump
        .drive(harness.register_ready_echo_tool(registry_echo(&route), route, route_timing()))
        .unwrap()
        .unwrap();
    runtime
        .pump
        .drive(harness.set_active_tools(vec![ToolName::new("echo").unwrap()]))
        .unwrap();
    let mut prompt = Box::pin(harness.prompt(PromptInput::Text {
        text: "invoke echo".into(),
        images: vec![],
    }));
    let mut request = None;
    for _ in 0..2000 {
        let _ = poll(prompt.as_mut());
        runtime.pump.tick();
        spawner.tick();
        if let Some(bytes) = channel.writes().get(2) {
            request = Some(written_request(bytes));
            break;
        }
        if runtime.pump.drive(harness.peek_action()).unwrap().is_some() {
            runtime.pump.drive(harness.execute_action()).unwrap();
        }
    }
    let request = request.expect("manual drive reaches G1 Request");
    channel.push(raw("terminal", request.request_id as u8));
    let mut parked = false;
    for _ in 0..1000 {
        spawner.tick();
        runtime.pump.tick();
        if runtime
            .pump
            .drive(harness.peek_action())
            .unwrap()
            .is_some_and(|action| action.kind == "finalize_tool")
        {
            parked = true;
            break;
        }
        if runtime.pump.drive(harness.peek_action()).unwrap().is_some() {
            runtime.pump.drive(harness.execute_action()).unwrap();
        }
    }
    assert!(parked, "terminal retained at finalize gate");
    let binding = driver_owner.retained_binding().expect("retained B1");
    driver_owner
        .retire(&binding, RetireReason::NoLongerExpected)
        .unwrap();
    let before = runtime.pump.drive(runtime.store.snapshot()).unwrap();
    runtime.pump.drive(harness.execute_action()).unwrap();
    let mut completed = None;
    for _ in 0..1000 {
        runtime.pump.tick();
        spawner.tick();
        if let Poll::Ready(value) = poll(prompt.as_mut()) {
            completed = Some(value);
            break;
        }
        if runtime.pump.drive(harness.peek_action()).unwrap().is_some() {
            runtime.pump.drive(harness.execute_action()).unwrap();
        }
    }
    assert!(matches!(
        completed,
        Some(Err(pi_agent_schema::HarnessRuntimeError::Fault(_)))
    ));
    assert_eq!(after.0.load(Ordering::SeqCst), 0);
    assert_eq!(
        runtime.pump.drive(runtime.store.snapshot()).unwrap(),
        before
    );
    assert!(
        driver_owner.retained_binding().is_none(),
        "retained G1 receipt retired"
    );
}

#[test]
fn actual_memory_retired_b1_cannot_publish_tool_raw_or_after_tool() {
    use pi_ai_core::{AssistantContent, StopReason, ToolCall, ToolCallId, ToolName};
    use pi_extension_protocol::tool_live::{LivePhase, RetireReason};
    use runtime_r1_r4_support::assistant;

    let runtime = Setup::new();
    let mut response = assistant(&runtime.faux.model, StopReason::ToolUse);
    response.content = vec![AssistantContent::ToolCall(ToolCall {
        content_type: Default::default(),
        id: ToolCallId::new("echo-call").unwrap(),
        name: ToolName::new("echo").unwrap(),
        arguments: saved(),
        thought_signature: None,
        namespace: None,
    })];
    *runtime.faux.responses.lock().unwrap() =
        vec![response, assistant(&runtime.faux.model, StopReason::Stop)].into();
    let factory = RuntimeFactory::new(
        runtime.time.clone(),
        runtime.pump.clone(),
        runtime.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    let harness = runtime
        .pump
        .drive(factory.create_configured(runtime.options::<()>(DriveMode::Automatic)))
        .unwrap()
        .harness;
    let after = Arc::new(runtime_r6_retirement_support::AfterToolCount::default());
    let _after_subscription = harness.hooks().after_tool(None, after.clone()).unwrap();
    let spawner = Arc::new(FakeSpawner::default());
    let driver_owner = owner(spawner.clone());
    let (channel, _, route) = setup(&driver_owner, &spawner, 7, 200);
    let route = Arc::new(route);
    let echo = registry_echo(&route);
    runtime
        .pump
        .drive(harness.register_ready_echo_tool(echo.clone(), route, route_timing()))
        .unwrap()
        .unwrap();
    runtime
        .pump
        .drive(harness.set_active_tools(vec![ToolName::new("echo").unwrap()]))
        .unwrap();
    let mut prompt = Box::pin(harness.prompt(PromptInput::Text {
        text: "invoke echo".into(),
        images: vec![],
    }));
    let mut request = None;
    for _ in 0..1000 {
        let _ = poll(prompt.as_mut());
        runtime.pump.tick();
        spawner.tick();
        if let Some(bytes) = channel.writes().get(2) {
            request = Some(written_request(bytes));
            break;
        }
    }
    let request = request.expect("G1 Q");
    let binding = driver_owner.retained_binding().expect("B1");
    channel.push(raw_terminal_with_usage(request.request_id));
    for _ in 0..1000 {
        spawner.tick();
        if driver_owner.binding_phase(&binding) == Ok(LivePhase::TerminalReceived) {
            break;
        }
    }
    assert_eq!(
        driver_owner.binding_phase(&binding),
        Ok(LivePhase::TerminalReceived)
    );
    driver_owner
        .retire(&binding, RetireReason::NoLongerExpected)
        .unwrap();
    let (_, _, g2) = setup(&driver_owner, &spawner, 8, 210);
    let g2 = Arc::new(g2);
    runtime
        .pump
        .drive(harness.register_ready_echo_tool(echo, g2.clone(), route_timing()))
        .unwrap()
        .unwrap();
    let g2_call = g2
        .start_planned_with_bounds(&plan(), &saved(), "other-call", None, None)
        .expect("B2 may use the owner slot after B1 retirement");
    let b2 = g2_call.binding().clone();
    let before = runtime.pump.drive(runtime.store.snapshot()).unwrap();
    let mut completion = None;
    for _ in 0..1000 {
        runtime.pump.tick();
        spawner.tick();
        if let Poll::Ready(value) = poll(prompt.as_mut()) {
            completion = Some(value);
            break;
        }
    }
    assert!(matches!(
        completion,
        Some(Err(pi_agent_schema::HarnessRuntimeError::Fault(_)))
    ));
    assert_eq!(
        after.0.load(Ordering::SeqCst),
        0,
        "retired B1 cannot reach after_tool"
    );
    assert_eq!(
        runtime.pump.drive(runtime.store.snapshot()).unwrap(),
        before,
        "retired B1 cannot publish ToolResult or linked usage"
    );
    assert!(
        driver_owner
            .retained_binding()
            .is_some_and(|live| live.same_live_identity(&b2)),
        "B1 mismatch cannot retire or fence B2"
    );
}

#[test]
fn actual_memory_tool_result_lifecycle_wakes_lane_after_settings_release() {
    use pi_agent_schema::{ProviderHeaderPolicy, SessionCodecOptions};
    use pi_ai_core::{AssistantContent, StopReason, ToolCall, ToolCallId, ToolName};
    use runtime_r1_r4_support::assistant;
    use runtime_r6_retirement_support::{AfterRawStamp, Tool};

    let runtime = Setup::new();
    let marker = Tool::new();
    let storage = Arc::new(AfterRawStamp::new(runtime.store.clone(), marker.clone()));
    let session = Arc::new(
        runtime
            .pump
            .drive(RuntimeSession::new(
                storage.clone(),
                runtime.session.metadata(),
                Arc::new(
                    SessionCodec::new(
                        SessionCodecOptions::default(),
                        ProviderHeaderPolicy::default(),
                    )
                    .unwrap(),
                ),
                runtime.ids.clone(),
                Arc::new(pi_telemetry::NOOP_TELEMETRY_CONTEXT),
            ))
            .unwrap(),
    );
    let mut response = assistant(&runtime.faux.model, StopReason::ToolUse);
    response.content = vec![AssistantContent::ToolCall(ToolCall {
        content_type: Default::default(),
        id: ToolCallId::new("echo-call").unwrap(),
        name: ToolName::new("echo").unwrap(),
        arguments: saved(),
        thought_signature: None,
        namespace: None,
    })];
    *runtime.faux.responses.lock().unwrap() =
        vec![response, assistant(&runtime.faux.model, StopReason::Stop)].into();
    let factory = RuntimeFactory::new(
        runtime.time.clone(),
        runtime.pump.clone(),
        runtime.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    let harness = runtime
        .pump
        .drive(factory.create_configured(runtime.options_for::<(), _>(session, DriveMode::Manual)))
        .unwrap()
        .harness;
    let spawner = Arc::new(FakeSpawner::default());
    let driver_owner = owner(spawner.clone());
    let (channel, _, route) = setup(&driver_owner, &spawner, 7, 200);
    let route = Arc::new(route);
    let echo = registry_echo(&route);
    runtime
        .pump
        .drive(harness.register_ready_echo_tool(echo.clone(), route, route_timing()))
        .unwrap()
        .unwrap();
    runtime
        .pump
        .drive(harness.set_active_tools(vec![ToolName::new("echo").unwrap()]))
        .unwrap();
    let mut prompt = Box::pin(harness.prompt(PromptInput::Text {
        text: "invoke echo".into(),
        images: vec![],
    }));
    let mut request = None;
    for _ in 0..2000 {
        let _ = poll(prompt.as_mut());
        runtime.pump.tick();
        spawner.tick();
        if let Some(bytes) = channel.writes().get(2) {
            request = Some(written_request(bytes));
            break;
        }
        if runtime.pump.drive(harness.peek_action()).unwrap().is_some() {
            runtime.pump.drive(harness.execute_action()).unwrap();
        }
    }
    let request = request.expect("Q before ToolResult");
    channel.push(raw_terminal_with_usage(request.request_id));
    let mut parked = false;
    for _ in 0..1000 {
        spawner.tick();
        runtime.pump.tick();
        if runtime
            .pump
            .drive(harness.peek_action())
            .unwrap()
            .is_some_and(|action| action.kind == "commit_settlement")
        {
            parked = true;
            break;
        }
        if runtime.pump.drive(harness.peek_action()).unwrap().is_some() {
            runtime.pump.drive(harness.execute_action()).unwrap();
        }
    }
    assert!(parked, "real ToolResult reached settlement gate");
    marker.raw_done.store(1, Ordering::SeqCst);
    runtime.pump.drive(harness.execute_action()).unwrap();
    for _ in 0..1000 {
        runtime.pump.tick();
        if storage.entered.is_cancelled() {
            break;
        }
    }
    assert!(
        storage.entered.is_cancelled(),
        "read_stamp parked under settlement lines"
    );
    let settings_ready = Arc::new(AtomicBool::new(false));
    let callback_harness = harness.clone();
    let callback_echo = echo.clone();
    let callback_ready = settings_ready.clone();
    let waker = Waker::from(Arc::new(OnceWake(Mutex::new(Some(Box::new(move || {
        let mut reenter = Box::pin(callback_harness.set_tools(vec![callback_echo]));
        callback_ready.store(poll(reenter.as_mut()).is_ready(), Ordering::SeqCst);
    }))))));
    let mut lane_waiter = Box::pin(harness.get_leaf_id());
    assert!(
        lane_waiter
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending(),
        "settlement owns lane while its stamp read is pending"
    );
    storage.release.cancel();
    for _ in 0..1000 {
        runtime.pump.tick();
        if settings_ready.load(Ordering::SeqCst) {
            break;
        }
    }
    assert!(
        settings_ready.load(Ordering::SeqCst),
        "ToolResult lifecycle lane wake must reenter the released settings line"
    );
    runtime.pump.drive(lane_waiter).unwrap();
    for _ in 0..1000 {
        runtime.pump.tick();
        spawner.tick();
        if let Poll::Ready(result) = poll(prompt.as_mut()) {
            result.unwrap().unwrap();
            return;
        }
        if runtime.pump.drive(harness.peek_action()).unwrap().is_some() {
            runtime.pump.drive(harness.execute_action()).unwrap();
        }
    }
    panic!("ToolResult settlement did not finish");
}

struct RegistryEventLog(Mutex<Vec<pi_agent_schema::HarnessEvent>>);

struct OnceWake(Mutex<Option<Box<dyn FnOnce() + Send>>>);

impl Wake for OnceWake {
    fn wake(self: Arc<Self>) {
        if let Some(callback) = self.0.lock().unwrap().take() {
            callback();
        }
    }
}

impl pi_agent_schema::EventListener for RegistryEventLog {
    fn on_event(
        &self,
        event: pi_agent_schema::HarnessEvent,
    ) -> pi_agent_schema::BoxFuture<'_, Result<(), pi_agent_schema::HandlerError>> {
        Box::pin(async move {
            self.0.lock().unwrap().push(event);
            Ok(())
        })
    }
}

#[test]
fn runtime_tools_events_follow_settings_publication_order() {
    use pi_agent_schema::{AgentHarness, AgentLane, DriveMode};

    let runtime = Setup::new();
    let pause = Arc::new(WritePause::new());
    let factory = RuntimeFactory::new(
        runtime.time.clone(),
        runtime.pump.clone(),
        runtime.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    let mut options = runtime.options(DriveMode::Automatic);
    options.tools = vec![registry_tool_with_pause("echo", Some(pause.clone()))];
    let harness = runtime
        .pump
        .drive(factory.create_configured(options))
        .unwrap()
        .harness;
    let watch = runtime.pump.drive(harness.watch()).unwrap();
    let events = Arc::new(RegistryEventLog(Mutex::new(Vec::new())));
    watch.start(events.clone());

    let spawner = Arc::new(FakeSpawner::default());
    let driver_owner = owner(spawner.clone());
    let (_, _, route) = setup(&driver_owner, &spawner, 7, 200);
    let route = Arc::new(route);
    let echo = registry_echo(&route);
    let thread_harness = harness.clone();
    let thread_pump = runtime.pump.clone();
    let registration = std::thread::spawn(move || {
        thread_pump.drive(thread_harness.register_ready_echo_tool(echo, route, route_timing()))
    });
    pause.entered.wait();
    runtime.pump.drive(harness.set_tools(vec![])).unwrap();
    let before_release = events
        .0
        .lock()
        .unwrap()
        .iter()
        .filter(|event| matches!(event, pi_agent_schema::HarnessEvent::GlobalConfigUpdate(_)))
        .count();
    pause.release.wait();
    registration.join().unwrap().unwrap().unwrap();
    assert_eq!(
        before_release, 2,
        "both ordered events must precede the paused flush"
    );
    runtime.pump.drive(harness.close()).unwrap();
}

#[test]
fn runtime_g2_registration_preserves_existing_echo_position() {
    use pi_agent_schema::{AgentHarness, DriveMode};

    let runtime = Setup::new();
    let native = registry_tool("native");
    let factory = RuntimeFactory::new(
        runtime.time.clone(),
        runtime.pump.clone(),
        runtime.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    let mut options = runtime.options(DriveMode::Automatic);
    options.tools = vec![native.clone()];
    let harness = runtime
        .pump
        .drive(factory.create_configured(options))
        .unwrap()
        .harness;
    let spawner = Arc::new(FakeSpawner::default());
    let driver_owner = owner(spawner.clone());
    let (_, _, g1) = setup(&driver_owner, &spawner, 7, 200);
    let g1 = Arc::new(g1);
    let echo = registry_echo(&g1);
    runtime
        .pump
        .drive(harness.register_ready_echo_tool(echo.clone(), g1, route_timing()))
        .unwrap()
        .unwrap();
    runtime
        .pump
        .drive(harness.set_tools(vec![echo.clone(), native.clone()]))
        .unwrap();

    let (_, _, g2) = setup(&driver_owner, &spawner, 8, 210);
    runtime
        .pump
        .drive(harness.register_ready_echo_tool(echo.clone(), Arc::new(g2), route_timing()))
        .unwrap()
        .unwrap();
    let handles = runtime.pump.drive(harness.get_tools()).unwrap();
    assert_eq!(handles.len(), 2);
    assert!(Arc::ptr_eq(&handles[0], &echo));
    assert!(Arc::ptr_eq(&handles[1], &native));
    runtime.pump.drive(harness.close()).unwrap();
}

#[test]
fn runtime_rejects_ready_echo_with_altered_progress_projection() {
    use pi_agent_schema::{AgentHarness, AgentLane, DriveMode};

    let runtime = Setup::new();
    let native = registry_tool("native");
    let factory = RuntimeFactory::new(
        runtime.time.clone(),
        runtime.pump.clone(),
        runtime.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    let mut options = runtime.options(DriveMode::Automatic);
    options.tools = vec![native.clone()];
    let harness = runtime
        .pump
        .drive(factory.create_configured(options))
        .unwrap()
        .harness;
    let watch = runtime.pump.drive(harness.watch()).unwrap();
    let events = Arc::new(RegistryEventLog(Mutex::new(Vec::new())));
    watch.start(events.clone());

    let spawner = Arc::new(FakeSpawner::default());
    let driver_owner = owner(spawner.clone());
    let route = Arc::new(setup_altered_progress(&driver_owner, &spawner, 7));
    let echo = registry_echo(&route);
    assert_eq!(
        runtime
            .pump
            .drive(harness.register_ready_echo_tool(echo, route, route_timing()))
            .unwrap()
            .err(),
        Some(DriverError::InvalidHost)
    );
    let tools = runtime.pump.drive(harness.get_tools()).unwrap();
    assert_eq!(tools.len(), 1);
    assert!(Arc::ptr_eq(&tools[0], &native));
    assert!(events.0.lock().unwrap().is_empty());
    runtime.pump.drive(harness.close()).unwrap();
}

#[test]
fn runtime_ready_echo_registration_drains_removed_route_and_keeps_owner_cap() {
    use pi_agent_schema::{AgentHarness, DriveMode};

    let setup_runtime = Setup::new();
    let native = registry_tool("native");
    let factory = RuntimeFactory::new(
        setup_runtime.time.clone(),
        setup_runtime.pump.clone(),
        setup_runtime.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    let mut options = setup_runtime.options(DriveMode::Automatic);
    options.tools = vec![native.clone()];
    let harness = setup_runtime
        .pump
        .drive(factory.create_configured(options))
        .unwrap()
        .harness;

    let spawner = Arc::new(FakeSpawner::default());
    let driver_owner = owner(spawner.clone());
    let (_, _, g1) = setup(&driver_owner, &spawner, 7, 200);
    let g1 = Arc::new(g1);
    let echo = registry_echo(&g1);
    assert_eq!(
        setup_runtime
            .pump
            .drive(harness.register_ready_echo_tool(native.clone(), g1.clone(), route_timing()))
            .unwrap()
            .err(),
        Some(DriverError::InvalidHost)
    );
    assert_eq!(
        setup_runtime.pump.drive(harness.get_tools()).unwrap().len(),
        1
    );
    assert_eq!(
        setup_runtime
            .pump
            .drive(harness.register_ready_echo_tool(
                registry_tool("echo"),
                g1.clone(),
                route_timing()
            ))
            .unwrap()
            .err(),
        Some(DriverError::InvalidHost),
        "a same-name handle with different schema/replay is not the grant"
    );

    setup_runtime
        .pump
        .drive(harness.register_ready_echo_tool(echo.clone(), g1.clone(), route_timing()))
        .unwrap()
        .unwrap();
    let tools = setup_runtime.pump.drive(harness.get_tools()).unwrap();
    assert_eq!(tools.len(), 2);
    assert!(Arc::ptr_eq(&tools[0], &native));
    assert!(Arc::ptr_eq(&tools[1], &echo));
    assert_eq!(
        setup_runtime
            .pump
            .drive(harness.register_ready_echo_tool(echo.clone(), g1.clone(), route_timing()))
            .unwrap()
            .err(),
        Some(DriverError::InvalidHost),
        "re-registering the same route cannot drain it and publish it again"
    );
    setup_runtime
        .pump
        .drive(harness.set_tools(vec![native.clone(), echo.clone()]))
        .unwrap();
    let first = g1.start_planned(&plan(), &saved(), "g1", timing()).unwrap();

    let other_owner = owner(spawner.clone());
    let (_, _, alien) = setup(&other_owner, &spawner, 1, 200);
    assert_eq!(
        setup_runtime
            .pump
            .drive(harness.register_ready_echo_tool(echo.clone(), Arc::new(alien), route_timing()))
            .unwrap()
            .err(),
        Some(DriverError::InvalidHost)
    );
    assert!(Arc::ptr_eq(
        &setup_runtime.pump.drive(harness.get_tools()).unwrap()[1],
        &echo
    ));

    setup_runtime
        .pump
        .drive(harness.set_tools(vec![native.clone()]))
        .unwrap();
    assert_eq!(
        g1.start_planned(&plan(), &saved(), "late-g1", timing())
            .err(),
        Some(DriverError::Closed)
    );
    setup_runtime
        .pump
        .drive(harness.set_tools(vec![native.clone(), echo.clone()]))
        .unwrap();
    assert_eq!(
        g1.start_planned(&plan(), &saved(), "readded-g1", timing())
            .err(),
        Some(DriverError::Closed)
    );

    let (_, _, g2) = setup(&driver_owner, &spawner, 8, 210);
    let g2 = Arc::new(g2);
    setup_runtime
        .pump
        .drive(harness.register_ready_echo_tool(echo.clone(), g2.clone(), route_timing()))
        .unwrap()
        .unwrap();
    assert_eq!(
        g2.start_planned(&plan(), &saved(), "g2", timing()).err(),
        Some(DriverError::Overloaded),
        "the already claimed G1 slot still counts against the same owner"
    );
    assert!(driver_owner.retained_binding().is_some());
    drop(first);
    setup_runtime.pump.drive(harness.close()).unwrap();
}

#[test]
fn registered_bounds_use_claim_time_and_keep_g1_deadline_across_g2() {
    use pi_agent_schema::{AgentHarness, DriveMode};
    use pi_extension_protocol::tool_live::RetireReason;

    let runtime = Setup::new();
    let factory = RuntimeFactory::new(
        runtime.time.clone(),
        runtime.pump.clone(),
        runtime.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    let harness = runtime
        .pump
        .drive(factory.create_configured(runtime.options(DriveMode::Automatic)))
        .unwrap()
        .harness;
    let spawner = Arc::new(FakeSpawner::default());
    let owner = owner(spawner.clone());
    let (g1_channel, g1_clock, g1) = setup(&owner, &spawner, 7, 100);
    let g1 = Arc::new(g1);
    let echo = registry_echo(&g1);

    assert_eq!(
        runtime
            .pump
            .drive(harness.register_ready_echo_tool(echo.clone(), g1.clone(), route_timing_at(100)))
            .unwrap()
            .err(),
        Some(DriverError::InvalidHost),
        "registration cannot bind an expired connection cutoff"
    );
    assert!(runtime.pump.drive(harness.get_tools()).unwrap().is_empty());
    assert_eq!(
        g1.start_planned_with_bounds(&plan(), &saved(), "unregistered", None, None)
            .err(),
        Some(DriverError::InvalidHost)
    );
    runtime
        .pump
        .drive(harness.register_ready_echo_tool(echo.clone(), g1.clone(), route_timing_at(240)))
        .unwrap()
        .unwrap();

    g1_clock.set(200);
    assert_eq!(
        g1.start_planned_with_bounds(&plan(), &saved(), "expired-caller", Some(200), None)
            .err(),
        Some(DriverError::Deadline)
    );
    assert!(owner.retained_binding().is_none());
    assert_eq!(g1_channel.writes().len(), 2);
    // The wrapper may have preflighted at 200. The only relevant sample is
    // the one made by the direct owner claim after this delay.
    g1_clock.set(225);
    let prepared = g1
        .prepare_planned_with_bounds(&plan(), &saved(), "g1", None, None)
        .unwrap();
    g1_clock.set(230);
    let first = prepared.activate().unwrap();
    assert_eq!(first.binding().request_id(), 1, "rejection burns no ID");
    spawner.tick();
    assert_eq!(
        written_request(&g1_channel.writes()[2]).deadline_after_ms,
        Some(15)
    );
    g1_clock.set(239);
    g1_channel.push(raw("terminal", 1));
    assert!(matches!(
        spawner.drive(first.first_terminal()),
        Ok(TypedTerminal::Result(_))
    ));
    assert!(owner.retained_binding().is_some());

    let (g2_channel, g2_clock, g2) = setup(&owner, &spawner, 8, 250);
    let g2 = Arc::new(g2);
    runtime
        .pump
        .drive(harness.register_ready_echo_tool(echo, g2.clone(), route_timing_at(350)))
        .unwrap()
        .unwrap();
    assert!(
        owner.retained_binding().is_some(),
        "G1 terminal survives G2 Ready"
    );
    assert_eq!(
        g2.start_planned_with_bounds(&plan(), &saved(), "too-soon", None, None)
            .err(),
        Some(DriverError::Overloaded)
    );
    owner
        .retire(first.binding(), RetireReason::Committed)
        .unwrap();
    g2_clock.set(300);
    let second = g2
        .start_planned_with_bounds(&plan(), &saved(), "g2", None, None)
        .unwrap();
    spawner.tick();
    assert_eq!(
        written_request(&g2_channel.writes()[2]).deadline_after_ms,
        Some(50)
    );
    assert_eq!(second.binding().request_id(), 1);
    g2.close();
    runtime.pump.drive(harness.close()).unwrap();
}

#[test]
fn ready_100_claim_200_writes_40_ms_request() {
    use pi_agent_schema::{AgentHarness, DriveMode};

    let runtime = Setup::new();
    let factory = RuntimeFactory::new(
        runtime.time.clone(),
        runtime.pump.clone(),
        runtime.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    let harness = runtime
        .pump
        .drive(factory.create_configured(runtime.options(DriveMode::Automatic)))
        .unwrap()
        .harness;
    let spawner = Arc::new(FakeSpawner::default());
    let owner = owner(spawner.clone());
    let (channel, clock, route) = setup(&owner, &spawner, 7, 100);
    let route = Arc::new(route);
    let echo = registry_echo(&route);
    runtime
        .pump
        .drive(harness.register_ready_echo_tool(echo, route.clone(), route_timing_at(240)))
        .unwrap()
        .unwrap();
    clock.set(200);
    let call = route
        .start_planned_with_bounds(&plan(), &saved(), "at-200", None, None)
        .unwrap();
    assert_eq!(call.binding().request_id(), 1);
    spawner.tick();
    assert_eq!(channel.writes().len(), 3);
    assert_eq!(
        written_request(&channel.writes()[2]).deadline_after_ms,
        Some(40)
    );
    runtime.pump.drive(harness.close()).unwrap();
}

#[test]
fn absolute_bounds_select_parent_minimum_and_expire() {
    use pi_agent_schema::{AgentHarness, DriveMode};

    let runtime = Setup::new();
    let factory = RuntimeFactory::new(
        runtime.time.clone(),
        runtime.pump.clone(),
        runtime.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    let harness = runtime
        .pump
        .drive(factory.create_configured(runtime.options(DriveMode::Automatic)))
        .unwrap()
        .harness;
    let spawner = Arc::new(FakeSpawner::default());
    let owner = owner(spawner.clone());
    let (channel, clock, route) = setup(&owner, &spawner, 7, 100);
    let route = Arc::new(route);
    let echo = registry_echo(&route);
    runtime
        .pump
        .drive(harness.register_ready_echo_tool(echo, route.clone(), route_timing_at(240)))
        .unwrap()
        .unwrap();
    clock.set(200);
    let first = route
        .start_planned_with_bounds(&plan(), &saved(), "bounded", Some(230), Some(220))
        .unwrap();
    spawner.tick();
    assert_eq!(
        written_request(&channel.writes()[2]).deadline_after_ms,
        Some(20)
    );
    clock.set(220);
    spawner.tick();
    assert!(matches!(
        spawner.drive(first.outcome()),
        Err(DriverError::PeerLost)
    ));
    owner.acknowledge_peer_lost(first.binding()).unwrap();
    assert!(owner.retained_binding().is_none());
    assert_eq!(channel.writes().len(), 3);
    runtime.pump.drive(harness.close()).unwrap();
}

#[test]
fn connection_cutoff_rejects_claim_without_binding_or_request() {
    use pi_agent_schema::{AgentHarness, DriveMode};

    let runtime = Setup::new();
    let factory = RuntimeFactory::new(
        runtime.time.clone(),
        runtime.pump.clone(),
        runtime.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    let harness = runtime
        .pump
        .drive(factory.create_configured(runtime.options(DriveMode::Automatic)))
        .unwrap()
        .harness;
    let spawner = Arc::new(FakeSpawner::default());
    let owner = owner(spawner.clone());
    let (channel, clock, route) = setup(&owner, &spawner, 7, 100);
    let route = Arc::new(route);
    let echo = registry_echo(&route);
    runtime
        .pump
        .drive(harness.register_ready_echo_tool(echo, route.clone(), route_timing_at(240)))
        .unwrap()
        .unwrap();
    clock.set(240);
    assert_eq!(
        route
            .start_planned_with_bounds(&plan(), &saved(), "at-cutoff", None, None)
            .err(),
        Some(DriverError::Deadline)
    );
    assert!(owner.retained_binding().is_none());
    spawner.tick();
    assert_eq!(channel.writes().len(), 2);
    runtime.pump.drive(harness.close()).unwrap();
}

#[test]
fn registration_clock_callback_can_read_harness_tools() {
    use pi_agent_schema::{AgentHarness, DriveMode};
    use std::time::Duration;

    let runtime = Setup::new();
    let factory = RuntimeFactory::new(
        runtime.time.clone(),
        runtime.pump.clone(),
        runtime.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    let harness = runtime
        .pump
        .drive(factory.create_configured(runtime.options(DriveMode::Automatic)))
        .unwrap()
        .harness;
    let spawner = Arc::new(FakeSpawner::default());
    let owner = owner(spawner.clone());
    let (_, clock, route) = setup(&owner, &spawner, 7, 100);
    let route = Arc::new(route);
    let echo = registry_echo(&route);
    let callback_harness = harness.clone();
    let callback_pump = runtime.pump.clone();
    clock.on_next_read(move || {
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            done_tx
                .send(
                    callback_pump
                        .drive(callback_harness.get_tools())
                        .unwrap()
                        .len(),
                )
                .unwrap();
        });
        assert_eq!(
            done_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
            0,
            "the route clock must run outside the tools registry mutex"
        );
    });
    runtime
        .pump
        .drive(harness.register_ready_echo_tool(echo, route, route_timing_at(240)))
        .unwrap()
        .unwrap();
    assert_eq!(runtime.pump.drive(harness.get_tools()).unwrap().len(), 1);
    runtime.pump.drive(harness.close()).unwrap();
}

#[test]
fn replacement_does_not_call_old_handle_descriptor_under_registry_lock() {
    use pi_agent_schema::{AgentHarness, DriveMode};
    use std::time::Duration;

    let runtime = Setup::new();
    let factory = RuntimeFactory::new(
        runtime.time.clone(),
        runtime.pump.clone(),
        runtime.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    let harness = runtime
        .pump
        .drive(factory.create_configured(runtime.options(DriveMode::Automatic)))
        .unwrap()
        .harness;
    let spawner = Arc::new(FakeSpawner::default());
    let driver_owner = owner(spawner.clone());
    let (_, _, g1) = setup(&driver_owner, &spawner, 7, 200);
    let g1 = Arc::new(g1);
    let armed = Arc::new(AtomicBool::new(false));
    let calls = Arc::new(AtomicU64::new(0));
    let probe = {
        let armed = armed.clone();
        let calls = calls.clone();
        let harness = harness.clone();
        Arc::new(move || {
            if !armed.load(Ordering::SeqCst) {
                return;
            }
            calls.fetch_add(1, Ordering::SeqCst);
            let (done, received) = std::sync::mpsc::channel();
            let harness = harness.clone();
            std::thread::spawn(move || {
                let mut get = Box::pin(harness.get_tools());
                let Poll::Ready(result) = poll(get.as_mut()) else {
                    panic!("get_tools pending")
                };
                done.send(result.unwrap().len()).unwrap();
            });
            assert_eq!(
                received.recv_timeout(Duration::from_secs(2)).unwrap(),
                1,
                "old descriptor must run outside registry lock"
            );
        })
    };
    let old = registry_echo_with_probe(&g1, Some(probe));
    runtime
        .pump
        .drive(harness.register_ready_echo_tool(old.clone(), g1, route_timing()))
        .unwrap()
        .unwrap();
    armed.store(true, Ordering::SeqCst);
    let (_, _, g2) = setup(&driver_owner, &spawner, 8, 210);
    let g2 = Arc::new(g2);
    runtime
        .pump
        .drive(harness.register_ready_echo_tool(registry_echo(&g2), g2, route_timing()))
        .unwrap()
        .unwrap();
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "replacement reads cached old descriptor"
    );
    old.descriptor();
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "probe itself remains operational after replacement"
    );
}

#[test]
fn absolute_bounds_near_u64_max_reject_write_overflow_without_retention() {
    use pi_agent_schema::{AgentHarness, DriveMode};

    let runtime = Setup::new();
    let factory = RuntimeFactory::new(
        runtime.time.clone(),
        runtime.pump.clone(),
        runtime.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    let harness = runtime
        .pump
        .drive(factory.create_configured(runtime.options(DriveMode::Automatic)))
        .unwrap()
        .harness;
    let spawner = Arc::new(FakeSpawner::default());
    let owner = owner(spawner.clone());
    // Handshake needs its own representable timeout; move the route clock
    // near the limit only after Ready so this isolates invocation arithmetic.
    let (channel, clock, route) = setup(&owner, &spawner, 7, 200);
    clock.set(u64::MAX - 20);
    let route = Arc::new(route);
    let echo = registry_echo(&route);
    runtime
        .pump
        .drive(harness.register_ready_echo_tool(echo, route.clone(), route_timing_at(u64::MAX)))
        .unwrap()
        .unwrap();
    clock.set(u64::MAX - 5);
    assert_eq!(
        route
            .start_planned_with_bounds(&plan(), &saved(), "overflow", None, None)
            .err(),
        Some(DriverError::Deadline)
    );
    assert!(owner.retained_binding().is_none());
    assert_eq!(channel.writes().len(), 2);
    runtime.pump.drive(harness.close()).unwrap();
}

#[test]
fn queued_local_abort_suppresses_request_with_and_without_prior_denial() {
    for denial in [false, true] {
        let spawner = Arc::new(FakeSpawner::default());
        let owner = owner_with_write_timeout(spawner.clone(), 400);
        let (g1, clock, route) = setup(&owner, &spawner, 7, 200);
        let handle = route
            .start_planned(&plan(), &saved(), "b1", timing())
            .unwrap();
        if denial {
            g1.push(selected_peer_request(&route, 17, "tool.invoke"));
            spawner.tick_at(1);
        }
        owner
            .abort_local(handle.binding(), LocalAbortCause::User)
            .unwrap();
        owner
            .abort_local(handle.binding(), LocalAbortCause::User)
            .unwrap();
        assert!(matches!(
            spawner.drive(handle.outcome()),
            Ok(CallOutcome::UnadmittedLocalAbort)
        ));
        assert_eq!(
            spawner.drive(handle.first_terminal()).err(),
            Some(DriverError::UnadmittedLocalAbort)
        );
        assert!(owner.retained_binding().is_none());
        if denial {
            clock.set(599);
        }
        spawner.tick();
        assert_eq!(g1.writes().len(), if denial { 3 } else { 2 });
        assert!(g1.closed.load(Ordering::SeqCst));
        let (_, _, g2) = setup(&owner, &spawner, 8, 210);
        assert!(g2.start_planned(&plan(), &saved(), "b2", timing()).is_ok());
    }
}

#[test]
fn admitted_and_written_request_send_one_cause_correct_cancel() {
    for minor in [0, 1] {
        for (cause, expected) in [
            (LocalAbortCause::User, v1_1::CancelReason::CancelUser),
            (LocalAbortCause::Parent, v1_1::CancelReason::CancelParent),
            (
                LocalAbortCause::Deadline,
                v1_1::CancelReason::CancelDeadline,
            ),
            (LocalAbortCause::Drain, v1_1::CancelReason::CancelDrain),
        ] {
            for admitted_only in [false, true] {
                let spawner = Arc::new(FakeSpawner::default());
                let owner = owner_with_write_timeout(spawner.clone(), 400);
                let (g1, _, route) = setup_minor(&owner, &spawner, 7, 200, minor);
                if admitted_only {
                    g1.stall.store(true, Ordering::SeqCst);
                }
                let handle = route
                    .start_planned(&plan(), &saved(), "b1", timing())
                    .unwrap();
                spawner.tick_at(0); // A, either pending or written W.
                owner.abort_local(handle.binding(), cause).unwrap();
                owner.abort_local(handle.binding(), cause).unwrap();
                if admitted_only {
                    g1.stall.store(false, Ordering::SeqCst);
                }
                spawner.tick_at(0);
                assert_eq!(g1.writes().len(), 4, "handshake, Q, and one C");
                assert_eq!(cancel_reason(&g1.writes()[3], minor), expected as i32);
                assert!(owner.retained_binding().is_some());
            }
        }
    }
}

#[test]
fn cancel_and_denial_keep_both_fifo_reservations_and_original_deadlines() {
    for cancel_first in [false, true] {
        let spawner = Arc::new(FakeSpawner::default());
        let owner = owner_with_write_timeout(spawner.clone(), 400);
        let (g1, clock, route) = setup(&owner, &spawner, 7, 200);
        g1.stall.store(true, Ordering::SeqCst);
        let handle = route
            .start_planned(&plan(), &saved(), "b1", timing())
            .unwrap();
        spawner.tick_at(0); // A is pending, so later control jobs wait behind it.
        if cancel_first {
            owner
                .abort_local(handle.binding(), LocalAbortCause::Parent)
                .unwrap();
        }
        g1.push(selected_peer_request(&route, 17, "tool.invoke"));
        spawner.tick_at(1);
        if !cancel_first {
            owner
                .abort_local(handle.binding(), LocalAbortCause::Parent)
                .unwrap();
        }
        clock.set(299);
        g1.stall.store(false, Ordering::SeqCst);
        spawner.tick_at(0);
        let writes = g1.writes();
        assert_eq!(writes.len(), if cancel_first { 5 } else { 4 });
        if cancel_first {
            assert_eq!(
                cancel_reason(&writes[3], 1),
                v1_1::CancelReason::CancelParent as i32
            );
            assert_eq!(
                denial_code(&writes[4], 1),
                v1_1::ErrorCode::Forbidden as i32
            );
        } else {
            assert_eq!(
                denial_code(&writes[3], 1),
                v1_1::ErrorCode::Forbidden as i32
            );
        }
        assert!(g1.closed.load(Ordering::SeqCst));
    }
}

#[test]
fn exact_fence_drops_queued_cancel_but_preserves_admitted_cancel_before_denial() {
    for admitted_cancel in [false, true] {
        let spawner = Arc::new(FakeSpawner::default());
        let owner = owner_with_write_timeout(spawner.clone(), 400);
        let (g1, clock, route) = setup(&owner, &spawner, 7, 200);
        let handle = route
            .start_planned(&plan(), &saved(), "b1", timing())
            .unwrap();
        spawner.tick_at(0); // W
        owner
            .abort_local(handle.binding(), LocalAbortCause::User)
            .unwrap();
        if admitted_cancel {
            g1.stall.store(true, Ordering::SeqCst);
            spawner.tick_at(0); // C is admitted but write remains pending.
        }
        g1.push(selected_peer_request(&route, 17, "tool.invoke"));
        spawner.tick_at(1); // D reserved after C.
        owner.fence_no_longer_expected(handle.binding()).unwrap();
        assert!(matches!(
            spawner.drive(handle.outcome()),
            Ok(CallOutcome::NoLongerExpected)
        ));
        let (_, _, g2) = setup(&owner, &spawner, 8, 210);
        assert!(g2.start_planned(&plan(), &saved(), "b2", timing()).is_ok());
        clock.set(599);
        g1.stall.store(false, Ordering::SeqCst);
        spawner.tick_at(0);
        let writes = g1.writes();
        assert_eq!(writes.len(), if admitted_cancel { 5 } else { 4 });
        if admitted_cancel {
            assert_eq!(
                cancel_reason(&writes[3], 1),
                v1_1::CancelReason::CancelUser as i32
            );
            assert_eq!(
                denial_code(&writes[4], 1),
                v1_1::ErrorCode::Forbidden as i32
            );
        } else {
            assert_eq!(
                denial_code(&writes[3], 1),
                v1_1::ErrorCode::Forbidden as i32
            );
        }
        assert!(g1.closed.load(Ordering::SeqCst));
        assert!(owner.retained_binding().is_some(), "G2 owns the slot");
    }
}

#[test]
fn first_terminal_and_local_cancel_serialize_on_the_owner_line() {
    for terminal_first in [false, true] {
        let spawner = Arc::new(FakeSpawner::default());
        let owner = owner(spawner.clone());
        let (g1, _, route) = setup(&owner, &spawner, 7, 200);
        let handle = route
            .start_planned(&plan(), &saved(), "b1", timing())
            .unwrap();
        spawner.tick_at(0); // W
        if terminal_first {
            g1.push(raw("terminal", 1));
            spawner.tick_at(1);
            assert_eq!(
                owner.abort_local(handle.binding(), LocalAbortCause::User),
                Err(DriverError::WrongBinding)
            );
            assert_eq!(g1.writes().len(), 3);
        } else {
            owner
                .abort_local(handle.binding(), LocalAbortCause::User)
                .unwrap();
            g1.push(raw("terminal", 1));
            spawner.tick_at(1); // T may precede the physical C write.
            spawner.tick_at(0);
            assert_eq!(g1.writes().len(), 4);
            assert_eq!(
                cancel_reason(&g1.writes()[3], 1),
                v1_1::CancelReason::CancelUser as i32
            );
        }
        assert!(matches!(
            spawner.drive(handle.outcome()),
            Ok(CallOutcome::Terminal(TypedTerminal::Result(_)))
        ));
        owner
            .retire(
                handle.binding(),
                pi_extension_protocol::tool_live::RetireReason::Committed,
            )
            .unwrap();
    }
}

#[test]
fn grace_cutoff_wakes_independently_and_final_terminal_time_is_exclusive() {
    for at_cutoff in [false, true] {
        let spawner = Arc::new(FakeSpawner::default());
        let owner = owner_with_write_timeout(spawner.clone(), 400);
        let (g1, clock, route) = setup(&owner, &spawner, 7, 200);
        let handle = route
            .start_planned(&plan(), &saved(), "b1", timing())
            .unwrap();
        spawner.tick_at(0); // W
        owner
            .abort_local(handle.binding(), LocalAbortCause::Parent)
            .unwrap();
        clock.set(if at_cutoff { 300 } else { 299 });
        if at_cutoff {
            // A stalled C and silent reader cannot delay the independent grace timer.
            g1.stall.store(true, Ordering::SeqCst);
            assert_eq!(
                spawner.drive(handle.outcome()).err(),
                Some(DriverError::PeerLost)
            );
            assert!(g1.closed.load(Ordering::SeqCst));
        } else {
            g1.push(raw("terminal", 1));
            assert!(matches!(
                spawner.drive(handle.outcome()),
                Ok(CallOutcome::Terminal(TypedTerminal::Result(_)))
            ));
            clock.set(300);
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

#[test]
fn near_u64_max_grace_caps_at_earlier_effective_deadline() {
    let spawner = Arc::new(FakeSpawner::default());
    let owner = owner_with_write_timeout(spawner.clone(), 10);
    let (g1, clock, route) = setup(&owner, &spawner, 7, 200);
    let now = u64::MAX - 80;
    clock.set(now);
    let handle = route
        .start_planned(
            &plan(),
            &saved(),
            "b1",
            CallTiming {
                requested_after_ms: Some(30),
                method_limit_ms: 30,
                connection_remaining_ms: 30,
                parent_remaining_ms: None,
            },
        )
        .unwrap(); // effective deadline is MAX-50; each write deadline is representable.
    spawner.tick_at(0); // W
    assert_eq!(
        owner.abort_local(handle.binding(), LocalAbortCause::User),
        Ok(())
    );
    spawner.tick_at(0); // C keeps its original finite write attempt.
    assert_eq!(g1.writes().len(), 4);
    assert_eq!(
        cancel_reason(&g1.writes()[3], 1),
        v1_1::CancelReason::CancelUser as i32
    );
    clock.set(u64::MAX - 51);
    g1.push(raw("terminal", 1));
    assert!(matches!(
        spawner.drive(handle.outcome()),
        Ok(CallOutcome::Terminal(TypedTerminal::Result(_)))
    ));
}

#[test]
fn terminal_entering_before_grace_but_finishing_at_cutoff_is_not_retained() {
    let spawner = Arc::new(FakeSpawner::default());
    let owner = owner_with_write_timeout(spawner.clone(), 400);
    let (g1, clock, route) = setup(&owner, &spawner, 7, 200);
    let handle = route
        .start_planned(&plan(), &saved(), "b1", timing())
        .unwrap();
    spawner.tick_at(0);
    owner
        .abort_local(handle.binding(), LocalAbortCause::User)
        .unwrap();
    clock.jump_after_reads(1, 299, 300);
    g1.push(raw("terminal", 1));
    spawner.tick_at(1);
    assert_eq!(
        spawner.drive(handle.outcome()).err(),
        Some(DriverError::PeerLost)
    );
    assert_eq!(
        owner.retire(
            handle.binding(),
            pi_extension_protocol::tool_live::RetireReason::Committed
        ),
        Err(DriverError::WrongBinding)
    );
}

#[test]
fn grace_loss_preserves_earlier_denial_for_reader_and_timer_orders() {
    for reader_first in [false, true] {
        let spawner = Arc::new(FakeSpawner::default());
        let owner = owner_with_write_timeout(spawner.clone(), 400);
        let (g1, clock, route) = setup(&owner, &spawner, 7, 200);
        let handle = route
            .start_planned(&plan(), &saved(), "b1", timing())
            .unwrap();
        spawner.tick_at(0); // W
        owner
            .abort_local(handle.binding(), LocalAbortCause::User)
            .unwrap(); // C queued
        g1.push(selected_peer_request(&route, 17, "tool.invoke"));
        spawner.tick_at(1); // D reserved behind C; original deadline 600.
        if reader_first {
            clock.jump_after_reads(1, 299, 300);
            g1.push(raw("terminal", 1));
            spawner.tick_at(1); // final receipt sample reaches grace cutoff.
        } else {
            clock.set(300);
            spawner.tick_at(3); // independent cancellation timer wins.
        }
        assert_eq!(
            spawner.drive(handle.outcome()).err(),
            Some(DriverError::PeerLost)
        );
        owner.acknowledge_peer_lost(handle.binding()).unwrap();
        assert!(!g1.closed.load(Ordering::SeqCst), "D remains reserved");
        clock.set(599);
        spawner.tick_at(0);
        let writes = g1.writes();
        assert_eq!(writes.len(), 4, "handshake, Q, and original D; no late C");
        assert_eq!(
            denial_code(&writes[3], 1),
            v1_1::ErrorCode::Forbidden as i32
        );
        assert!(g1.closed.load(Ordering::SeqCst));
    }
}

#[test]
fn graceful_drain_retains_queued_or_admitted_cancel_after_first_terminal() {
    for peer_drain in [false, true] {
        for admitted_cancel in [false, true] {
            let spawner = Arc::new(FakeSpawner::default());
            let owner = owner_with_write_timeout(spawner.clone(), 400);
            let (g1, clock, route) = setup(&owner, &spawner, 7, 200);
            let handle = route
                .start_planned(&plan(), &saved(), "b1", timing())
                .unwrap();
            spawner.tick_at(0); // W
            if peer_drain {
                let drain = v1_1::Envelope {
                    frame: Some(v1_1::envelope::Frame::GoAway(v1_1::GoAway {
                        reason: v1_1::GoAwayReason::GoAwayDrain as i32,
                        retryable: false,
                    })),
                };
                g1.push(framing::encode(&drain, nz(8192)).unwrap().bytes);
                spawner.tick_at(1);
            } else {
                route.stop_new_calls();
            }
            owner
                .abort_local(handle.binding(), LocalAbortCause::User)
                .unwrap();
            if admitted_cancel {
                g1.stall.store(true, Ordering::SeqCst);
                spawner.tick_at(0); // C claimed, write pending.
            }
            g1.push(raw("terminal", 1));
            spawner.tick_at(1);
            assert!(matches!(
                spawner.drive(handle.outcome()),
                Ok(CallOutcome::Terminal(TypedTerminal::Result(_)))
            ));
            assert!(!g1.closed.load(Ordering::SeqCst), "graceful drain keeps C");
            clock.set(599); // T was retained before grace; C keeps its original write deadline 600.
            g1.stall.store(false, Ordering::SeqCst);
            spawner.tick_at(0);
            assert_eq!(g1.writes().len(), 4, "earlier C uses its FIFO reservation");
            assert_eq!(
                cancel_reason(&g1.writes()[3], 1),
                v1_1::CancelReason::CancelUser as i32
            );
            assert!(
                g1.closed.load(Ordering::SeqCst),
                "drained route closes after C"
            );
            owner
                .retire(
                    handle.binding(),
                    pi_extension_protocol::tool_live::RetireReason::Committed,
                )
                .unwrap();
        }
    }
}

#[test]
fn checked_terminal_retirement_preserves_prior_queued_cancel_only() {
    for drain in [false, true] {
        let spawner = Arc::new(FakeSpawner::default());
        let owner = owner_with_write_timeout(spawner.clone(), 400);
        let (g1, clock, route) = setup(&owner, &spawner, 7, 200);
        let handle = route
            .start_planned(&plan(), &saved(), "b1", timing())
            .unwrap();
        spawner.tick_at(0); // W
        if drain {
            route.stop_new_calls();
        }
        owner
            .abort_local(handle.binding(), LocalAbortCause::User)
            .unwrap(); // C queued
        g1.push(raw("terminal", 1));
        spawner.tick_at(1);
        assert!(matches!(
            spawner.drive(handle.outcome()),
            Ok(CallOutcome::Terminal(TypedTerminal::Result(_)))
        ));
        owner
            .retire(
                handle.binding(),
                pi_extension_protocol::tool_live::RetireReason::Committed,
            )
            .unwrap();
        if !drain {
            assert_eq!(
                route.start_planned(&plan(), &saved(), "b2", timing()).err(),
                Some(DriverError::Overloaded)
            );
        }
        clock.set(599);
        spawner.tick_at(0);
        assert_eq!(
            g1.writes().len(),
            4,
            "checked T retirement cannot erase earlier C"
        );
        assert_eq!(
            cancel_reason(&g1.writes()[3], 1),
            v1_1::CancelReason::CancelUser as i32
        );
        assert_eq!(g1.closed.load(Ordering::SeqCst), drain);
        if !drain {
            assert!(
                route
                    .start_planned(&plan(), &saved(), "b2", timing())
                    .is_ok()
            );
        }
    }
}

#[test]
fn g2_replacement_preserves_g1_retired_cancel_through_original_deadline() {
    for retire_before_connect in [false, true] {
        for admitted_cancel in [false, true] {
            let spawner = Arc::new(FakeSpawner::default());
            let owner = owner_with_write_timeout(spawner.clone(), 400);
            let (g1, clock, route) = setup(&owner, &spawner, 7, 200);
            let handle = route
                .start_planned(&plan(), &saved(), "b1", timing())
                .unwrap();
            spawner.tick_at(0); // W
            owner
                .abort_local(handle.binding(), LocalAbortCause::User)
                .unwrap();
            if admitted_cancel {
                g1.stall.store(true, Ordering::SeqCst);
                spawner.tick_at(0); // C is writer-admitted but incomplete.
            }
            g1.push(raw("terminal", 1));
            spawner.tick_at(1);
            assert!(matches!(
                spawner.drive(handle.outcome()),
                Ok(CallOutcome::Terminal(TypedTerminal::Result(_)))
            ));
            if retire_before_connect {
                owner
                    .retire(
                        handle.binding(),
                        pi_extension_protocol::tool_live::RetireReason::Committed,
                    )
                    .unwrap();
            }
            let (g2, _, next_route) = setup(&owner, &spawner, 8, 210);
            if !retire_before_connect {
                assert_eq!(
                    next_route
                        .start_planned(&plan(), &saved(), "b2", timing())
                        .err(),
                    Some(DriverError::Overloaded)
                );
                owner
                    .retire(
                        handle.binding(),
                        pi_extension_protocol::tool_live::RetireReason::Committed,
                    )
                    .unwrap();
            }
            let next = next_route
                .start_planned(&plan(), &saved(), "b2", timing())
                .unwrap();
            assert!(
                !g1.closed.load(Ordering::SeqCst),
                "replacement must drain prior C"
            );
            clock.set(599);
            g1.stall.store(false, Ordering::SeqCst);
            spawner.tick_at(0);
            assert_eq!(g1.writes().len(), 4, "G1 C keeps its original FIFO attempt");
            assert_eq!(
                cancel_reason(&g1.writes()[3], 1),
                v1_1::CancelReason::CancelUser as i32
            );
            assert!(g1.closed.load(Ordering::SeqCst));
            assert!(!g2.closed.load(Ordering::SeqCst));
            assert!(
                owner
                    .retained_binding()
                    .unwrap()
                    .same_live_identity(next.binding())
            );
        }
    }
}

#[test]
fn admitted_cancel_timeout_preempts_later_original_denial() {
    let spawner = Arc::new(FakeSpawner::default());
    let owner = owner_with_write_timeout(spawner.clone(), 400);
    let (g1, clock, route) = setup(&owner, &spawner, 7, 200);
    let handle = route
        .start_planned(&plan(), &saved(), "b1", timing())
        .unwrap();
    spawner.tick_at(0);
    g1.stall.store(true, Ordering::SeqCst);
    owner
        .abort_local(handle.binding(), LocalAbortCause::User)
        .unwrap();
    spawner.tick_at(0); // C admitted before D but incomplete.
    g1.push(selected_peer_request(&route, 17, "tool.invoke"));
    spawner.tick_at(1);
    owner.fence_no_longer_expected(handle.binding()).unwrap();
    clock.set(600); // C and D original control deadlines were both 600.
    spawner.tick_at(0);
    assert_eq!(g1.writes().len(), 3, "failed C preempts the reserved D");
    assert!(g1.closed.load(Ordering::SeqCst));
    assert!(matches!(
        spawner.drive(handle.outcome()),
        Ok(CallOutcome::NoLongerExpected)
    ));
}

#[test]
fn local_abort_without_denial_releases_g1_slot_and_late_terminal_cannot_bind_g2() {
    let spawner = Arc::new(FakeSpawner::default());
    let owner = owner(spawner.clone());
    let (g1, _, route) = setup(&owner, &spawner, 7, 200);
    let handle = route
        .start_planned(&plan(), &saved(), "b1", timing())
        .unwrap();
    owner
        .abort_local(handle.binding(), LocalAbortCause::User)
        .unwrap();
    let (g2, _, next_route) = setup(&owner, &spawner, 8, 210);
    let next = next_route
        .start_planned(&plan(), &saved(), "b2", timing())
        .unwrap();
    g1.push(raw("terminal", 1));
    spawner.tick();
    assert!(g1.closed.load(Ordering::SeqCst));
    assert!(!g2.closed.load(Ordering::SeqCst));
    assert!(
        owner
            .retained_binding()
            .unwrap()
            .same_live_identity(next.binding())
    );
    assert!(matches!(
        spawner.drive(handle.outcome()),
        Ok(CallOutcome::UnadmittedLocalAbort)
    ));
}

#[test]
fn exact_fence_drops_queued_request_wakes_no_write_and_frees_g2_slot() {
    let spawner = Arc::new(FakeSpawner::default());
    let owner = owner(spawner.clone());
    let (g1, _, route) = setup(&owner, &spawner, 7, 200);
    let handle = route
        .start_planned(&plan(), &saved(), "b1", timing())
        .unwrap();
    assert_eq!(g1.writes().len(), 2);
    owner.fence_no_longer_expected(handle.binding()).unwrap();
    assert!(matches!(
        spawner.drive(handle.outcome()),
        Ok(CallOutcome::NoLongerExpected)
    ));
    assert_eq!(
        spawner.drive(handle.first_terminal()).err(),
        Some(DriverError::NoLongerExpected)
    );
    assert_eq!(
        owner.fence_no_longer_expected(handle.binding()).err(),
        Some(DriverError::WrongBinding)
    );
    spawner.tick();
    assert_eq!(g1.writes().len(), 2, "queued Request bytes were removed");
    assert!(g1.closed.load(Ordering::SeqCst));
    assert!(owner.retained_binding().is_none());
    let (_, _, g2) = setup(&owner, &spawner, 8, 210);
    let second = g2.start_planned(&plan(), &saved(), "b2", timing()).unwrap();
    assert_eq!(
        owner.fence_no_longer_expected(handle.binding()).err(),
        Some(DriverError::WrongBinding)
    );
    assert!(
        owner
            .retained_binding()
            .unwrap()
            .same_live_identity(second.binding())
    );
}

#[test]
fn no_write_waker_may_reenter_owner_after_exact_slot_release() {
    struct ReenterOwner {
        owner: SingleToolOwner,
        woke: AtomicBool,
    }
    impl Wake for ReenterOwner {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            assert!(self.owner.retained_binding().is_none());
            self.woke.store(true, Ordering::SeqCst);
        }
    }

    let spawner = Arc::new(FakeSpawner::default());
    let owner = owner(spawner.clone());
    let (_, _, route) = setup(&owner, &spawner, 7, 200);
    let handle = route
        .start_planned(&plan(), &saved(), "b1", timing())
        .unwrap();
    let reenter = Arc::new(ReenterOwner {
        owner: owner.clone(),
        woke: AtomicBool::new(false),
    });
    let waker = Waker::from(reenter.clone());
    let mut outcome = Box::pin(handle.outcome());
    assert!(
        outcome
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    owner.fence_no_longer_expected(handle.binding()).unwrap();
    assert!(reenter.woke.load(Ordering::SeqCst));
    assert!(matches!(
        spawner.drive(outcome),
        Ok(CallOutcome::NoLongerExpected)
    ));
}

#[test]
fn peer_request_in_no_write_waker_cannot_reserve_a_post_fence_denial() {
    struct InjectPeerRequest {
        owner: SingleToolOwner,
        spawner: Arc<FakeSpawner>,
        channel: Arc<FakeChannel>,
        frame: Mutex<Option<Vec<u8>>>,
    }
    impl Wake for InjectPeerRequest {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            assert!(self.owner.retained_binding().is_none());
            if let Some(frame) = self.frame.lock().unwrap().take() {
                self.channel.push(frame);
                self.spawner.tick_at(1);
            }
        }
    }

    let spawner = Arc::new(FakeSpawner::default());
    let owner = owner_with_write_timeout(spawner.clone(), 400);
    let (g1, _, route) = setup(&owner, &spawner, 7, 200);
    let handle = route
        .start_planned(&plan(), &saved(), "b1", timing())
        .unwrap(); // Q is still queued; no D is reserved.
    let wake = Arc::new(InjectPeerRequest {
        owner: owner.clone(),
        spawner: spawner.clone(),
        channel: g1.clone(),
        frame: Mutex::new(Some(selected_peer_request(&route, 17, "tool.invoke"))),
    });
    let waker = Waker::from(wake.clone());
    let mut outcome = Box::pin(handle.outcome());
    assert!(
        outcome
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    owner.fence_no_longer_expected(handle.binding()).unwrap();
    assert!(
        wake.frame.lock().unwrap().is_none(),
        "waker injected the peer Request"
    );
    assert!(matches!(
        spawner.drive(outcome),
        Ok(CallOutcome::NoLongerExpected)
    ));
    spawner.tick();
    assert_eq!(g1.writes().len(), 2, "a post-fence D must never be sent");
    assert!(g1.closed.load(Ordering::SeqCst));
}

#[test]
fn exact_fence_preserves_reserved_denial_after_dropping_queued_request() {
    let spawner = Arc::new(FakeSpawner::default());
    let owner = owner_with_write_timeout(spawner.clone(), 400);
    let (g1, clock, route) = setup(&owner, &spawner, 7, 200);
    let handle = route
        .start_planned(&plan(), &saved(), "b1", timing())
        .unwrap();
    g1.push(selected_peer_request(&route, 17, "tool.invoke"));
    spawner.tick_at(1); // D is reserved while Q is still unadmitted.
    owner.fence_no_longer_expected(handle.binding()).unwrap();
    assert!(matches!(
        spawner.drive(handle.outcome()),
        Ok(CallOutcome::NoLongerExpected)
    ));
    assert!(!g1.closed.load(Ordering::SeqCst));
    clock.set(599);
    spawner.tick();
    assert_eq!(g1.writes().len(), 3, "D must survive, Q must not write");
    assert!(g1.closed.load(Ordering::SeqCst));
}

#[test]
fn fence_without_denial_closes_an_admitted_incomplete_request() {
    let spawner = Arc::new(FakeSpawner::default());
    let owner = owner_with_write_timeout(spawner.clone(), 400);
    let (g1, _, route) = setup(&owner, &spawner, 7, 200);
    g1.stall.store(true, Ordering::SeqCst);
    let handle = route
        .start_planned(&plan(), &saved(), "b1", timing())
        .unwrap();
    spawner.tick(); // Writer admitted A, but has written no Request bytes.
    owner.fence_no_longer_expected(handle.binding()).unwrap();
    assert!(g1.closed.load(Ordering::SeqCst));
    assert_eq!(g1.writes().len(), 2);
    assert!(matches!(
        spawner.drive(handle.outcome()),
        Ok(CallOutcome::NoLongerExpected)
    ));
    let (_, _, g2) = setup(&owner, &spawner, 8, 210);
    assert!(g2.start_planned(&plan(), &saved(), "b2", timing()).is_ok());
}

#[test]
fn admitted_request_drains_before_original_deadline_while_g2_uses_freed_slot() {
    let spawner = Arc::new(FakeSpawner::default());
    let owner = owner_with_write_timeout(spawner.clone(), 400);
    let (g1, clock, route) = setup(&owner, &spawner, 7, 200);
    g1.stall.store(true, Ordering::SeqCst);
    let handle = route
        .start_planned(&plan(), &saved(), "b1", timing())
        .unwrap();
    spawner.tick(); // A has occurred but write_frame remains pending.
    g1.push(selected_peer_request(&route, 17, "tool.invoke"));
    spawner.tick_at(1);
    owner.fence_no_longer_expected(handle.binding()).unwrap();
    assert!(matches!(
        spawner.drive(handle.outcome()),
        Ok(CallOutcome::NoLongerExpected)
    ));
    let (_, _, g2) = setup(&owner, &spawner, 8, 210);
    assert!(
        g2.start_planned(&plan(), &saved(), "b2", timing()).is_ok(),
        "B1 must release the owner slot before G1 drains"
    );
    clock.set(599);
    g1.stall.store(false, Ordering::SeqCst);
    spawner.tick();
    assert_eq!(g1.writes().len(), 4, "admitted A then original D");
    assert!(g1.closed.load(Ordering::SeqCst));
    assert!(owner.retained_binding().is_some(), "G2 owns the one slot");
}

#[test]
fn fenced_admitted_request_and_denial_keep_their_original_deadlines() {
    for request_timed_out in [false, true] {
        let spawner = Arc::new(FakeSpawner::default());
        let owner = owner_with_write_timeout(spawner.clone(), 400);
        let (g1, clock, route) = setup(&owner, &spawner, 7, 200);
        g1.stall.store(true, Ordering::SeqCst);
        let handle = route
            .start_planned(&plan(), &saved(), "b1", timing())
            .unwrap();
        spawner.tick();
        g1.push(selected_peer_request(&route, 17, "tool.invoke"));
        spawner.tick_at(1);
        owner.fence_no_longer_expected(handle.binding()).unwrap();
        if request_timed_out {
            clock.set(600);
        } else {
            // Request completes below 600, but D's original deadline is also
            // 600 when it next reaches the FIFO head.
            clock.jump_after_reads(1, 599, 600);
        }
        g1.stall.store(false, Ordering::SeqCst);
        spawner.tick();
        assert_eq!(g1.writes().len(), if request_timed_out { 2 } else { 3 });
        assert!(g1.closed.load(Ordering::SeqCst));
        assert!(matches!(
            spawner.drive(handle.outcome()),
            Ok(CallOutcome::NoLongerExpected)
        ));
    }
}

#[test]
fn fence_rejects_wrong_binding_and_retained_terminal_uses_checked_retire() {
    let spawner = Arc::new(FakeSpawner::default());
    let owner = owner(spawner.clone());
    let (g1, _, route) = setup(&owner, &spawner, 7, 200);
    let handle = route
        .start_planned(&plan(), &saved(), "b1", timing())
        .unwrap();
    let other_spawner = Arc::new(FakeSpawner::default());
    let other_owner = owner_with_write_timeout(other_spawner.clone(), 10);
    let (_, _, other_route) = setup(&other_owner, &other_spawner, 9, 200);
    let other = other_route
        .start_planned(&plan(), &saved(), "other", timing())
        .unwrap();
    assert_eq!(
        owner.fence_no_longer_expected(other.binding()).err(),
        Some(DriverError::WrongBinding)
    );
    assert!(owner.retained_binding().is_some());
    spawner.tick();
    g1.push(raw("terminal", 1));
    assert!(matches!(
        spawner.drive(handle.first_terminal()),
        Ok(TypedTerminal::Result(_))
    ));
    assert_eq!(
        owner.fence_no_longer_expected(handle.binding()).err(),
        Some(DriverError::WrongBinding)
    );
    assert!(owner.retained_binding().is_some());
    owner
        .retire(
            handle.binding(),
            pi_extension_protocol::tool_live::RetireReason::NoLongerExpected,
        )
        .unwrap();
    assert!(owner.retained_binding().is_none());
}

#[test]
fn final_time_receipt_wins_a_concurrent_exact_fence_on_the_owner_line() {
    let spawner = Arc::new(FakeSpawner::default());
    let owner = owner(spawner.clone());
    let (g1, clock, route) = setup(&owner, &spawner, 7, 200);
    let handle = route
        .start_planned(&plan(), &saved(), "b1", timing())
        .unwrap();
    spawner.tick();
    let pause = Arc::new(WritePause::new());
    // Pump ingress and owner ingress take two samples. Hold the third, final
    // receipt sample while the receiver owns the same line as the fence.
    clock.pause_after_reads(2, pause.clone());
    g1.push(raw("terminal", 1));
    std::thread::scope(|scope| {
        let reader_spawner = spawner.clone();
        let reader = scope.spawn(move || reader_spawner.tick_at(1));
        pause.entered.wait();
        let started = Arc::new(Barrier::new(2));
        let fence_started = started.clone();
        let fence_owner = owner.clone();
        let binding = handle.binding().clone();
        let fence = scope.spawn(move || {
            fence_started.wait();
            fence_owner.fence_no_longer_expected(&binding)
        });
        started.wait();
        pause.release.wait();
        reader.join().unwrap();
        assert_eq!(fence.join().unwrap(), Err(DriverError::WrongBinding));
    });
    assert!(matches!(
        spawner.drive(handle.first_terminal()),
        Ok(TypedTerminal::Result(_))
    ));
    owner
        .retire(
            handle.binding(),
            pi_extension_protocol::tool_live::RetireReason::NoLongerExpected,
        )
        .unwrap();
}

#[test]
fn late_g1_terminal_after_fence_closes_only_g1_and_cannot_touch_g2() {
    let spawner = Arc::new(FakeSpawner::default());
    let owner = owner_with_write_timeout(spawner.clone(), 400);
    let (g1, _, route) = setup(&owner, &spawner, 7, 200);
    let handle = route
        .start_planned(&plan(), &saved(), "b1", timing())
        .unwrap();
    spawner.tick(); // W
    g1.stall.store(true, Ordering::SeqCst);
    g1.push(selected_peer_request(&route, 17, "tool.invoke"));
    spawner.tick_at(1); // reserve D before fence
    owner.fence_no_longer_expected(handle.binding()).unwrap();
    let (g2_channel, _, g2) = setup(&owner, &spawner, 8, 210);
    let second = g2.start_planned(&plan(), &saved(), "b2", timing()).unwrap();
    g1.push(raw("terminal", 1));
    spawner.tick_at(1);
    assert!(g1.closed.load(Ordering::SeqCst));
    assert!(!g2_channel.closed.load(Ordering::SeqCst));
    assert!(
        owner
            .retained_binding()
            .unwrap()
            .same_live_identity(second.binding())
    );
    assert!(matches!(
        spawner.drive(handle.outcome()),
        Ok(CallOutcome::NoLongerExpected)
    ));
    assert_eq!(g1.writes().len(), 3, "stale terminal preempts unfinished D");
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
fn claimed_request_precedes_denial_reserved_during_inline_timer_spawn() {
    let spawner = Arc::new(FakeSpawner::default());
    let owner = owner_with_write_timeout(spawner.clone(), 400);
    let (channel, _, route) = setup(&owner, &spawner, 7, 200);
    channel.push(selected_peer_request(&route, 17, "tool.invoke"));
    spawner
        .poll_reader_during_next_spawn
        .store(true, Ordering::SeqCst);
    let handle = route
        .start_planned(&plan(), &saved(), "call-1", timing())
        .unwrap();
    assert!(owner.retained_binding().is_some());
    assert!(!channel.closed.load(Ordering::SeqCst));
    assert_eq!(channel.writes().len(), 2);
    for _ in 0..5 {
        spawner.tick();
    }
    assert_eq!(channel.writes().len(), 4, "claimed Q precedes later D");
    assert_eq!(written_request(&channel.writes()[2]).request_id, 1);
    assert!(channel.closed.load(Ordering::SeqCst));
    assert_eq!(
        spawner.drive(handle.first_terminal()).err(),
        Some(DriverError::PeerLost)
    );
    owner.acknowledge_peer_lost(handle.binding()).unwrap();

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
fn prepared_inactive_head_keeps_fifo_through_g2_handoff() {
    let spawner = Arc::new(FakeSpawner::default());
    let owner = owner_with_write_timeout(spawner.clone(), 400);
    let (g1_channel, _, g1) = setup(&owner, &spawner, 7, 200);
    let prepared = g1
        .prepare_planned(&plan(), &saved(), "g1", timing())
        .unwrap();
    assert_eq!(prepared.binding().generation(), 7);
    spawner.tick();
    assert_eq!(g1_channel.writes().len(), 2, "inactive Q cannot be popped");
    g1_channel.push(selected_peer_request(&g1, 17, "tool.invoke"));
    spawner.tick();
    assert_eq!(g1_channel.writes().len(), 2, "later D cannot overtake Q");

    let (_, _, g2) = setup(&owner, &spawner, 8, 210);
    assert_eq!(
        g2.start_planned(&plan(), &saved(), "g2", timing()).err(),
        Some(DriverError::Overloaded)
    );
    let handle = prepared.activate().unwrap();
    assert_eq!(handle.binding().request_id(), 1);
    spawner.tick();
    assert_eq!(g1_channel.writes().len(), 4, "G1 Q remains ahead of D");
    assert_eq!(written_request(&g1_channel.writes()[2]).request_id, 1);
    assert!(g1_channel.closed.load(Ordering::SeqCst));
}

#[test]
fn inline_expiry_removes_inactive_q_and_wakes_later_denial() {
    struct CountWake(AtomicU64);
    impl Wake for CountWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let spawner = Arc::new(FakeSpawner::default());
    let owner = owner_with_write_timeout(spawner.clone(), 400);
    let (channel, clock, route) = setup(&owner, &spawner, 7, 200);
    let counted = Arc::new(CountWake(AtomicU64::new(0)));
    let waker = Waker::from(counted.clone());
    let mut writer = spawner.tasks.lock().unwrap().remove(0);
    assert!(
        writer
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    spawner.tasks.lock().unwrap().insert(0, writer);
    let tasks_before = spawner.tasks.lock().unwrap().len();
    let prepared = route
        .prepare_planned(&plan(), &saved(), "call", timing())
        .unwrap();
    assert_eq!(spawner.tasks.lock().unwrap().len(), tasks_before);
    assert_eq!(
        counted.0.load(Ordering::SeqCst),
        0,
        "prepare never wakes writer"
    );
    channel.push(selected_peer_request(&route, 17, "tool.invoke"));
    spawner.tick_at(1);
    let mut writer = spawner.tasks.lock().unwrap().remove(0);
    assert!(
        writer
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    spawner.tasks.lock().unwrap().insert(0, writer);
    let before = counted.0.load(Ordering::SeqCst);
    assert_eq!(channel.writes().len(), 2);

    clock.set(500);
    spawner.poll_next_spawn_inline.store(true, Ordering::SeqCst);
    let handle = prepared.activate().unwrap();
    assert_eq!(
        spawner.drive(handle.first_terminal()).err(),
        Some(DriverError::PeerLost)
    );
    assert!(
        counted.0.load(Ordering::SeqCst) > before,
        "D writer was woken"
    );
    spawner.tick();
    assert_eq!(channel.writes().len(), 3, "only D reaches the channel");
}

#[test]
fn rejected_timer_removes_prepared_q_with_and_without_denial() {
    for with_denial in [false, true] {
        let spawner = Arc::new(FakeSpawner::default());
        let owner = owner_with_write_timeout(spawner.clone(), 400);
        let (channel, _, route) = setup(&owner, &spawner, 7, 200);
        let prepared = route
            .prepare_planned(&plan(), &saved(), "call", timing())
            .unwrap();
        if with_denial {
            channel.push(selected_peer_request(&route, 17, "tool.invoke"));
            spawner.tick();
            assert_eq!(channel.writes().len(), 2);
        }
        let (next_channel, _, next_route) = setup(&owner, &spawner, 8, 210);
        spawner.reject.store(true, Ordering::SeqCst);
        assert!(matches!(prepared.activate(), Err(DriverError::HostFault)));
        spawner.reject.store(false, Ordering::SeqCst);
        assert!(owner.retained_binding().is_none());
        assert_eq!(channel.writes().len(), 2, "no rejected Q bytes");
        if with_denial {
            assert!(!channel.closed.load(Ordering::SeqCst));
            spawner.tick();
            assert_eq!(channel.writes().len(), 3, "reserved D survives");
        } else {
            assert!(channel.closed.load(Ordering::SeqCst));
        }
        let next = next_route
            .start_planned(&plan(), &saved(), "g2", timing())
            .unwrap();
        assert_eq!(next.binding().request_id(), 1);
        spawner.tick();
        assert_eq!(next_channel.writes().len(), 3, "G2 remains usable");
    }
}

#[test]
fn inline_expiry_wins_even_when_timer_spawner_reports_rejection() {
    let spawner = Arc::new(FakeSpawner::default());
    let owner = owner_with_write_timeout(spawner.clone(), 400);
    let (channel, clock, route) = setup(&owner, &spawner, 7, 200);
    let prepared = route
        .prepare_planned(&plan(), &saved(), "call", timing())
        .unwrap();
    channel.push(selected_peer_request(&route, 17, "tool.invoke"));
    spawner.tick();
    clock.set(500);
    spawner
        .poll_then_reject_next_spawn
        .store(true, Ordering::SeqCst);
    let handle = prepared
        .activate()
        .expect("earlier expiry owns the outcome");
    assert_eq!(
        spawner.drive(handle.first_terminal()).err(),
        Some(DriverError::PeerLost)
    );
    spawner.tick();
    assert_eq!(channel.writes().len(), 3, "D survives without Q bytes");
}

#[test]
fn dropping_prepared_token_releases_inactive_q_and_denial() {
    let spawner = Arc::new(FakeSpawner::default());
    let owner = owner_with_write_timeout(spawner.clone(), 400);
    let (channel, _, route) = setup(&owner, &spawner, 7, 200);
    let prepared = route
        .prepare_planned(&plan(), &saved(), "call", timing())
        .unwrap();
    channel.push(selected_peer_request(&route, 17, "tool.invoke"));
    spawner.tick();
    assert_eq!(channel.writes().len(), 2);
    drop(prepared);
    assert!(owner.retained_binding().is_none());
    spawner.tick();
    assert_eq!(channel.writes().len(), 3, "D proceeds after abandoned Q");
}

#[test]
fn phase_gap_abort_and_exact_fence_keep_private_no_write_outcomes() {
    for fence in [false, true] {
        let spawner = Arc::new(FakeSpawner::default());
        let owner = owner(spawner.clone());
        let (channel, _, route) = setup(&owner, &spawner, 7, 200);
        let prepared = route
            .prepare_planned(&plan(), &saved(), "call", timing())
            .unwrap();
        if fence {
            owner.fence_no_longer_expected(prepared.binding()).unwrap();
        } else {
            owner
                .abort_local(prepared.binding(), LocalAbortCause::User)
                .unwrap();
        }
        let handle = prepared.activate().unwrap();
        let outcome = spawner.drive(handle.outcome());
        assert!(
            matches!(outcome, Ok(CallOutcome::NoLongerExpected) if fence)
                || matches!(outcome, Ok(CallOutcome::UnadmittedLocalAbort) if !fence)
        );
        assert_eq!(channel.writes().len(), 2);
    }
}

#[test]
fn failed_prepare_does_not_reserve_request_or_advance_id() {
    let spawner = Arc::new(FakeSpawner::default());
    let owner = owner(spawner.clone());
    let (channel, _, route) = setup(&owner, &spawner, 7, 200);
    let invalid: JsonObject = serde_json::from_str(r#"{"text":42}"#).unwrap();
    assert_eq!(
        route
            .prepare_planned(&plan(), &invalid, "bad", timing())
            .err(),
        Some(DriverError::InvalidHost)
    );
    let mut bad_timing = timing();
    bad_timing.method_limit_ms = 0;
    assert!(
        route
            .prepare_planned(&plan(), &saved(), "bad", bad_timing)
            .is_err()
    );
    assert!(owner.retained_binding().is_none());
    spawner.tick();
    assert_eq!(channel.writes().len(), 2);
    let prepared = route
        .prepare_planned(&plan(), &saved(), "good", timing())
        .unwrap();
    assert_eq!(prepared.binding().request_id(), 1);
    drop(prepared);
}

#[test]
fn queue_capacity_failure_before_claim_keeps_first_id_available() {
    let spawner = Arc::new(FakeSpawner::default());
    let owner = SingleToolOwner::new(
        spawner.clone(),
        OwnerLimits {
            outbound_bytes: nz(512),
            receipt_bytes: nz(8192),
            write_timeout_ms: NonZeroU64::new(10).unwrap(),
        },
    );
    let (channel, _, route) = setup(&owner, &spawner, 7, 200);
    let large: JsonObject = serde_json::from_value(serde_json::json!({
        "text": "x".repeat(1000)
    }))
    .unwrap();
    assert_eq!(
        route
            .prepare_planned(&plan(), &large, "large", timing())
            .err(),
        Some(DriverError::Overloaded)
    );
    assert!(owner.retained_binding().is_none());
    assert_eq!(channel.writes().len(), 2);
    let prepared = route
        .prepare_planned(&plan(), &saved(), "small", timing())
        .unwrap();
    assert_eq!(prepared.binding().request_id(), 1);
    drop(prepared);
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
