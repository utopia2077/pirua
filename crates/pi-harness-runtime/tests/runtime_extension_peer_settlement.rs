#![cfg(feature = "extensions")]

mod runtime_r1_r4_support;

use std::future::{Future, poll_fn};
use std::io::{Read, Write as IoWrite};
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::task::{Poll, Waker};
use std::time::{Duration, Instant};

use pi_agent_schema::*;
use pi_agent_session::codec::SessionCodec;
use pi_agent_session::session::RuntimeSession;
use pi_agent_session::storage::*;
use pi_agent_sqlite::{SqliteOptions, SqliteStorage};
use pi_ai_core::{
    AssistantContent, BoxFuture, CancellationToken, JsonObject, SafeUnsigned, StopReason, ToolCall,
    ToolCallId, ToolName,
};
use pi_extension_protocol::activation::{
    AgentTool as WireTool, AgentToolMode, ApprovedTool, HostAuthority, ToolReplay as WireReplay,
    ToolSchemaInputs,
};
use pi_extension_protocol::jcs::Limits as JsonLimits;
use pi_extension_protocol::tool_live::LivePhase;
use pi_extension_protocol::wire::framing::{EncodedFrame, FrameReader};
use pi_extension_protocol::wire::v1_1;
use pi_harness_runtime::extensions::tool_driver::{EchoRouteTiming, OwnerLimits, SingleToolOwner};
use pi_harness_runtime::extensions::transport::{
    ChannelError, MonotonicClock, OrderedByteChannel, PumpConfig,
};
use pi_harness_runtime::owner::RuntimeFactory;
use prost::Message as _;
use runtime_r1_r4_support::{Setup, Summary, assistant, poll};
use serde_json::Value;

const CORPUS: &str =
    include_str!("../../../conformance/extension-protocol/runtime-settlement-v1.json");
const CATALOG: &str = include_str!("../../pi-extension-protocol/schema/fixtures/host-catalog.json");
const LIMIT: Duration = Duration::from_secs(15);

fn fixture() -> Value {
    serde_json::from_str(CORPUS).unwrap()
}
fn nz(n: usize) -> NonZeroUsize {
    NonZeroUsize::new(n).unwrap()
}
fn args(text: &str) -> JsonObject {
    serde_json::from_value(serde_json::json!({"text":text})).unwrap()
}
fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap()
}
fn codec() -> Arc<SessionCodec> {
    Arc::new(
        SessionCodec::new(
            SessionCodecOptions::default(),
            ProviderHeaderPolicy::default(),
        )
        .unwrap(),
    )
}
fn until(runtime: &Setup, mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + LIMIT;
    loop {
        runtime.pump.tick();
        if condition() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "finite runtime/peer wait expired"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}
fn drive<F: Future>(runtime: &Setup, future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    let mut output = None;
    until(runtime, || {
        if let Poll::Ready(value) = poll(future.as_mut()) {
            output = Some(value);
            true
        } else {
            false
        }
    });
    output.unwrap()
}

#[derive(Default)]
struct Clock {
    start: Mutex<Option<Instant>>,
}
impl MonotonicClock for Clock {
    fn now_ms(&self) -> u64 {
        self.start
            .lock()
            .unwrap()
            .get_or_insert_with(Instant::now)
            .elapsed()
            .as_millis() as u64
    }
    fn sleep_until(&self, deadline: u64) -> BoxFuture<'_, ()> {
        let ready = Arc::new(AtomicBool::new(false));
        let wake = Arc::new(Mutex::new(None::<Waker>));
        let delay = Duration::from_millis(deadline.saturating_sub(self.now_ms()));
        let (thread_ready, thread_wake) = (ready.clone(), wake.clone());
        std::thread::spawn(move || {
            std::thread::sleep(delay);
            thread_ready.store(true, Ordering::SeqCst);
            if let Some(w) = thread_wake.lock().unwrap().take() {
                w.wake();
            }
        });
        Box::pin(poll_fn(move |cx| {
            if ready.load(Ordering::SeqCst) {
                Poll::Ready(())
            } else {
                *wake.lock().unwrap() = Some(cx.waker().clone());
                if ready.load(Ordering::SeqCst) {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }
        }))
    }
}

type Reply = Arc<Mutex<(Option<Result<(), ChannelError>>, Option<Waker>)>>;
struct Channel {
    child: Mutex<Option<Child>>,
    closed: Arc<AtomicBool>,
    read: Mutex<mpsc::Receiver<Vec<u8>>>,
    read_wake: Arc<Mutex<Option<Waker>>>,
    write: mpsc::SyncSender<(Vec<u8>, Reply)>,
    writes: Arc<Mutex<Vec<Vec<u8>>>>,
    stderr: Arc<Mutex<Vec<u8>>>,
    done: Mutex<mpsc::Receiver<()>>,
    workers: Mutex<Vec<std::thread::JoinHandle<()>>>,
    release: PathBuf,
}
impl Channel {
    fn spawn(
        language: &str,
        pair: &Value,
        fault: &str,
        replay: WireReplay,
        generation: u64,
    ) -> Arc<Self> {
        let serial = NEXT.fetch_add(1, Ordering::SeqCst);
        let release = std::env::temp_dir().join(format!(
            "pi-runtime-peer1376-{}-{serial}",
            std::process::id()
        ));
        assert!(!release.exists());
        let mut command = if language == "typescript" {
            let mut c = Command::new("node");
            c.args([
                "--experimental-strip-types",
                "sdk/typescript/runtime-peer.ts",
            ]);
            c
        } else {
            let mut c = Command::new("python3");
            c.args(["-B", "sdk/python/runtime_peer.py"]);
            c
        };
        command.arg("--generation").arg(generation.to_string());
        command
            .current_dir(root())
            .args([
                "--case",
                pair["id"].as_str().unwrap(),
                "--fault",
                fault,
                "--replay",
                if replay == WireReplay::Safe {
                    "safe"
                } else {
                    "never"
                },
                "--release",
            ])
            .arg(&release);
        let mut child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("required real language actor");
        let mut input = child.stdin.take().unwrap();
        let mut output = child.stdout.take().unwrap();
        let mut errors = child.stderr.take().unwrap();
        let closed = Arc::new(AtomicBool::new(false));
        let read_wake = Arc::new(Mutex::new(None::<Waker>));
        let writes = Arc::new(Mutex::new(vec![]));
        let stderr = Arc::new(Mutex::new(vec![]));
        let (read_tx, read_rx) = mpsc::sync_channel(1);
        let (write_tx, write_rx) = mpsc::sync_channel::<(Vec<u8>, Reply)>(1);
        let (done_tx, done_rx) = mpsc::channel();
        let mut workers = vec![];
        let (stop, wake, done) = (closed.clone(), read_wake.clone(), done_tx.clone());
        workers.push(std::thread::spawn(move || {
            let mut total = 0;
            loop {
                let mut buffer = [0u8; 8192];
                let count = output.read(&mut buffer).unwrap_or(0);
                total += count;
                assert!(total <= 65536, "finite peer stdout budget");
                let mut data = buffer[..count].to_vec();
                loop {
                    match read_tx.try_send(data) {
                        Ok(()) => break,
                        Err(mpsc::TrySendError::Full(value)) => {
                            data = value;
                            if stop.load(Ordering::SeqCst) {
                                let _ = done.send(());
                                return;
                            }
                            std::thread::sleep(Duration::from_millis(1));
                        }
                        Err(mpsc::TrySendError::Disconnected(_)) => {
                            let _ = done.send(());
                            return;
                        }
                    }
                }
                if let Some(w) = wake.lock().unwrap().take() {
                    w.wake();
                }
                if count == 0 || stop.load(Ordering::SeqCst) {
                    break;
                }
            }
            let _ = done.send(());
        }));
        let (stop, captured, done) = (closed.clone(), writes.clone(), done_tx.clone());
        workers.push(std::thread::spawn(move || {
            while !stop.load(Ordering::SeqCst) {
                match write_rx.recv_timeout(Duration::from_millis(20)) {
                    Ok((data, reply)) => {
                        let result = input.write_all(&data).map_err(|_| ChannelError::Failed);
                        if result.is_ok() {
                            captured.lock().unwrap().push(data);
                        }
                        let wake = {
                            let mut state = reply.lock().unwrap();
                            state.0 = Some(result);
                            state.1.take()
                        };
                        if let Some(w) = wake {
                            w.wake();
                        }
                    }
                    Err(mpsc::RecvTimeoutError::Timeout) => {}
                    Err(mpsc::RecvTimeoutError::Disconnected) => break,
                }
            }
            let _ = done.send(());
        }));
        let (captured, done) = (stderr.clone(), done_tx);
        workers.push(std::thread::spawn(move || {
            let mut buffer = [0u8; 1024];
            loop {
                let n = errors.read(&mut buffer).unwrap_or(0);
                if n == 0 {
                    break;
                }
                let mut value = captured.lock().unwrap();
                assert!(value.len() + n <= 65536, "finite stderr budget");
                value.extend_from_slice(&buffer[..n]);
            }
            let _ = done.send(());
        }));
        Arc::new(Self {
            child: Mutex::new(Some(child)),
            closed,
            read: Mutex::new(read_rx),
            read_wake,
            write: write_tx,
            writes,
            stderr,
            done: Mutex::new(done_rx),
            workers: Mutex::new(workers),
            release,
        })
    }
    fn release(&self) {
        std::fs::write(&self.release, b"terminal").unwrap();
    }
    fn request(&self) -> Option<v1_1::Request> {
        self.writes.lock().unwrap().iter().find_map(|bytes| {
            let mut reader = FrameReader::<v1_1::Envelope>::new(nz(8192));
            let env = reader.read_one(bytes).unwrap().frame.unwrap().envelope;
            if let Some(v1_1::envelope::Frame::Request(r)) = env.frame {
                Some(r)
            } else {
                None
            }
        })
    }
    fn request_trace(&self) -> Option<Value> {
        String::from_utf8_lossy(&self.stderr.lock().unwrap())
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .find(|record| record["stage"] == "request")
    }
    fn requests(&self) -> usize {
        self.writes
            .lock()
            .unwrap()
            .iter()
            .filter(|bytes| {
                let mut reader = FrameReader::<v1_1::Envelope>::new(nz(8192));
                matches!(
                    reader
                        .read_one(bytes)
                        .unwrap()
                        .frame
                        .unwrap()
                        .envelope
                        .frame,
                    Some(v1_1::envelope::Frame::Request(_))
                )
            })
            .count()
    }
    fn finish(&self, success: bool) {
        let deadline = Instant::now() + LIMIT;
        let status = loop {
            let status = self
                .child
                .lock()
                .unwrap()
                .as_mut()
                .and_then(|child| child.try_wait().unwrap());
            if let Some(status) = status {
                break status;
            }
            assert!(Instant::now() < deadline, "finite peer process wait");
            std::thread::sleep(Duration::from_millis(1));
        };
        if success {
            assert!(
                status.success(),
                "actor failed: {}",
                String::from_utf8_lossy(&self.stderr.lock().unwrap())
            );
        }
        self.close();
        for _ in 0..3 {
            self.done
                .lock()
                .unwrap()
                .recv_timeout(LIMIT)
                .expect("finite pipe worker cleanup");
        }
        for worker in self.workers.lock().unwrap().drain(..) {
            join_worker(worker);
        }
        let records = String::from_utf8(self.stderr.lock().unwrap().clone()).unwrap();
        if success {
            assert!(records.lines().any(|line| {
                serde_json::from_str::<Value>(line).is_ok_and(|v| v["stage"] == "terminal")
            }));
        }
        println!("PEER1376 {}", records.trim());
        self.child.lock().unwrap().take();
    }
}
static NEXT: AtomicUsize = AtomicUsize::new(0);
impl Drop for Channel {
    fn drop(&mut self) {
        self.close();
        if let Some(mut child) = self.child.lock().unwrap().take() {
            let deadline = Instant::now() + LIMIT;
            loop {
                if child.try_wait().unwrap().is_some() {
                    break;
                }
                if Instant::now() >= deadline {
                    panic!("peer reap timeout");
                }
                std::thread::sleep(Duration::from_millis(1));
            }
        }
        for worker in self.workers.lock().unwrap().drain(..) {
            join_worker(worker);
        }
        if self.release.exists() {
            std::fs::remove_file(&self.release).unwrap();
        }
    }
}
impl OrderedByteChannel for Channel {
    fn max_read_scratch_bytes(&self) -> usize {
        8192
    }
    fn read_chunk<'a>(
        &'a self,
        buffer: &'a mut [u8],
    ) -> BoxFuture<'a, Result<Option<usize>, ChannelError>> {
        Box::pin(poll_fn(move |cx| {
            if self.closed.load(Ordering::SeqCst) {
                return Poll::Ready(Err(ChannelError::Closed));
            }
            *self.read_wake.lock().unwrap() = Some(cx.waker().clone());
            match self.read.lock().unwrap().try_recv() {
                Ok(data) if data.is_empty() => Poll::Ready(Ok(None)),
                Ok(data) if data.len() <= buffer.len() => {
                    buffer[..data.len()].copy_from_slice(&data);
                    Poll::Ready(Ok(Some(data.len())))
                }
                Ok(_) => Poll::Ready(Err(ChannelError::Failed)),
                Err(mpsc::TryRecvError::Empty) => Poll::Pending,
                Err(mpsc::TryRecvError::Disconnected) => Poll::Ready(Ok(None)),
            }
        }))
    }
    fn write_frame(&self, frame: EncodedFrame) -> BoxFuture<'_, Result<(), ChannelError>> {
        let reply: Reply = Arc::new(Mutex::new((None, None)));
        let mut data = Some(frame.bytes);
        Box::pin(poll_fn(move |cx| {
            if self.closed.load(Ordering::SeqCst) {
                return Poll::Ready(Err(ChannelError::Closed));
            }
            if let Some(bytes) = data.take() {
                match self.write.try_send((bytes, reply.clone())) {
                    Ok(()) => {}
                    Err(mpsc::TrySendError::Full((bytes, _))) => {
                        data = Some(bytes);
                        return Poll::Pending;
                    }
                    Err(_) => return Poll::Ready(Err(ChannelError::Closed)),
                }
            }
            let mut state = reply.lock().unwrap();
            state.1 = Some(cx.waker().clone());
            state.0.take().map_or(Poll::Pending, Poll::Ready)
        }))
    }
    fn close(&self) {
        if !self.closed.swap(true, Ordering::SeqCst) {
            if let Some(child) = self.child.lock().unwrap().as_mut() {
                let _ = child.kill();
            }
            if let Some(w) = self.read_wake.lock().unwrap().take() {
                w.wake();
            }
        }
    }
}

fn host(pair: &Value, replay: WireReplay, generation: u64) -> HostAuthority {
    let c: Value = serde_json::from_str(CATALOG).unwrap();
    let entry = &c["entries"][0];
    let descriptor = WireTool {
        name: "echo".into(),
        label: entry["agentTool"]["label"].as_str().unwrap().into(),
        description: entry["agentTool"]["description"].as_str().unwrap().into(),
        parameters: entry["agentTool"]["parameters"].clone(),
        execution_mode: AgentToolMode::Sequential,
        replay,
    };
    let schemas = [0, 1]
        .map(|minor| {
            let s = &entry["selected_minor_schema_sets"][format!("1.{minor}")];
            ToolSchemaInputs {
                minor,
                arguments: serde_json::to_vec(&s["arguments"]["schema"]).unwrap(),
                progress: serde_json::to_vec(&s["progress"]["schema"]).unwrap(),
                raw_result: serde_json::to_vec(&s["raw_result"]["schema"]).unwrap(),
            }
        })
        .into();
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
        handshake_ms: 5000,
        cancellation_grace_ms: 100,
        max_stream_credit_bytes: 4096,
    };
    HostAuthority::new(
        "peer-a".into(),
        generation,
        pair["core_minor"].as_u64().unwrap() as u32,
        pair["tool_minor"].as_u64().unwrap() as u32,
        limits,
        vec![tool],
        vec![],
    )
    .unwrap()
}
fn connect(
    runtime: &Setup,
    language: &str,
    pair: &Value,
    fault: &str,
    replay: WireReplay,
) -> (
    SingleToolOwner,
    Arc<Channel>,
    Arc<pi_harness_runtime::extensions::tool_driver::ReadyToolRoute>,
) {
    connect_generation(runtime, language, pair, fault, replay, 7)
}
fn connect_generation(
    runtime: &Setup,
    language: &str,
    pair: &Value,
    fault: &str,
    replay: WireReplay,
    generation: u64,
) -> (
    SingleToolOwner,
    Arc<Channel>,
    Arc<pi_harness_runtime::extensions::tool_driver::ReadyToolRoute>,
) {
    let channel = Channel::spawn(language, pair, fault, replay, generation);
    let owner = SingleToolOwner::new(
        runtime.pump.clone(),
        OwnerLimits {
            outbound_bytes: nz(8192),
            receipt_bytes: nz(8192),
            write_timeout_ms: NonZeroU64::new(5000).unwrap(),
        },
    );
    let route = drive(
        runtime,
        owner.connect(
            host(pair, replay, generation),
            channel.clone(),
            Arc::new(Clock::default()),
            PumpConfig {
                max_read_chunk: nz(8192),
                write_timeout_ms: NonZeroU64::new(5000).unwrap(),
            },
        ),
    )
    .unwrap();
    assert_eq!(
        route
            .grant()
            .wire()
            .selected_protocol
            .as_ref()
            .unwrap()
            .minor,
        pair["core_minor"].as_u64().unwrap() as u32
    );
    assert_eq!(
        route.grant().tools()[0]
            .reference()
            .profile
            .as_ref()
            .unwrap()
            .minor,
        pair["tool_minor"].as_u64().unwrap() as u32
    );
    (owner, channel, Arc::new(route))
}
fn timing() -> EchoRouteTiming {
    EchoRouteTiming {
        method_limit_ms: NonZeroU64::new(12000).unwrap(),
        connection_deadline_ms: 60000,
    }
}

struct Tool {
    descriptor: AgentTool,
}
impl AgentHarnessTool<()> for Tool {
    fn descriptor(&self) -> &AgentTool {
        &self.descriptor
    }
    fn prepare_arguments(
        &self,
        arguments: pi_ai_core::JsonValue,
    ) -> Result<JsonObject, ToolExecutionError> {
        if let pi_ai_core::JsonValue::Object(value) = arguments {
            Ok(value)
        } else {
            panic!("object args")
        }
    }
    fn execute<'a>(
        &'a self,
        _: ToolCallId,
        _: JsonObject,
        _: &'a dyn Cancellation,
        _: &'a dyn ToolProgressSink,
        _: &'a (),
    ) -> BoxFuture<'a, Result<AgentToolResult, ToolExecutionError>> {
        Box::pin(async { panic!("remote witness cannot fall back to native execute") })
    }
}
fn tool(replay: WireReplay) -> AgentHarnessToolHandle<()> {
    let c: Value = serde_json::from_str(CATALOG).unwrap();
    let a = &c["entries"][0]["agentTool"];
    Arc::new(Tool {
        descriptor: AgentTool {
            name: ToolName::new("echo").unwrap(),
            label: a["label"].as_str().unwrap().into(),
            description: a["description"].as_str().unwrap().into(),
            parameters: serde_json::from_value(a["parameters"].clone()).unwrap(),
            constrained_sampling: None,
            execution_mode: Some(ToolExecutionMode::Sequential),
            replay: Some(if replay == WireReplay::Safe {
                ToolReplay::Safe
            } else {
                ToolReplay::Never
            }),
        },
    })
}
struct Before(Arc<Mutex<Vec<String>>>);
impl HookHandler<BeforeTool> for Before {
    fn call(
        &self,
        event: BeforeToolEvent,
    ) -> BoxFuture<'_, Result<Option<BeforeToolResult>, HandlerError>> {
        assert_eq!(event.args, args(fixture()["input_text"].as_str().unwrap()));
        self.0.lock().unwrap().push("before".into());
        Box::pin(async {
            Ok(Some(BeforeToolResult {
                args: Some(args(fixture()["effective_text"].as_str().unwrap())),
                block: None,
            }))
        })
    }
}
struct After {
    stage: u8,
    log: Arc<Mutex<Vec<String>>>,
}
impl HookHandler<AfterTool> for After {
    fn call(
        &self,
        event: AfterToolEvent,
    ) -> BoxFuture<'_, Result<Option<AfterToolResult>, HandlerError>> {
        let expected = if self.stage == 1 {
            fixture()["effective_text"].as_str().unwrap().to_owned()
        } else {
            "after-first".into()
        };
        assert!(
            matches!(&event.content[..],[pi_ai_core::InputContent::Text(t)] if t.text==expected),
            "actual peer first terminal/source-ordered patch"
        );
        assert_eq!(
            event.args,
            args(fixture()["effective_text"].as_str().unwrap())
        );
        assert_eq!(event.usage.as_ref().unwrap().input.get(), 5);
        self.log
            .lock()
            .unwrap()
            .push(format!("after-{}", self.stage));
        let content=serde_json::from_value(serde_json::json!([{"type":"text","text":if self.stage==1{"after-first"}else{"after-second"}}])).unwrap();
        Box::pin(async move {
            Ok(Some(AfterToolResult {
                content: Some(content),
                ..Default::default()
            }))
        })
    }
}
fn response(runtime: &Setup) {
    let mut r = assistant(&runtime.faux.model, StopReason::ToolUse);
    r.content = vec![AssistantContent::ToolCall(ToolCall {
        content_type: Default::default(),
        id: ToolCallId::new(fixture()["tool_call_id"].as_str().unwrap()).unwrap(),
        name: ToolName::new("echo").unwrap(),
        arguments: args(fixture()["input_text"].as_str().unwrap()),
        thought_signature: None,
        namespace: None,
    })];
    *runtime.faux.responses.lock().unwrap() =
        vec![r, assistant(&runtime.faux.model, StopReason::Stop)].into();
}
fn pending(snapshot: &StorageSnapshot) -> (EffectKey, Register) {
    let operation = snapshot
        .registers
        .iter()
        .find_map(|r| {
            if let Register::LaneState { key, value, .. } = r {
                if key == "main" {
                    value.current_operation_id.clone()
                } else {
                    None
                }
            } else {
                None
            }
        })
        .expect("actual pending operation");
    let state = snapshot
        .registers
        .iter()
        .find_map(|r| {
            if let Register::OperationState { key, value, .. } = r {
                if key == operation.as_str() {
                    Some(value)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .unwrap();
    let OperationState::Run(run) = state.as_ref() else {
        panic!("run")
    };
    let RunPhase::Tools(tools) = &run.phase else {
        panic!("Tools phase")
    };
    assert!(matches!(
        &tools.batch.calls[..],
        [ToolCallState::EffectPending { .. }]
    ));
    let saved = snapshot
        .registers
        .iter()
        .find(|r| matches!(r, Register::OperationToolArguments { .. }))
        .unwrap()
        .clone();
    assert!(
        matches!(&saved,Register::OperationToolArguments{value,seq,..} if value==&args(fixture()["effective_text"].as_str().unwrap()) && seq.get()>0)
    );
    (
        EffectKey {
            session_id: SessionId::new("runtime-session").unwrap(),
            lane: LaneId::new("main").unwrap(),
            operation_id: operation,
            kind: EffectKind::Tool,
            step_or_task_id: EffectStepId::new(tools.batch.turn_id.as_str()).unwrap(),
            logical_attempt: SafeUnsigned::new(0).unwrap(),
            item_index: Some(SafeUnsigned::new(0).unwrap()),
        },
        saved,
    )
}
fn check_request(request: &v1_1::Request, key: &EffectKey) {
    assert_eq!(
        request.effect_key.as_ref(),
        Some(&v1_1::EffectKey {
            session_id: key.session_id.as_str().into(),
            lane: key.lane.as_str().into(),
            operation_id: key.operation_id.as_str().into(),
            effect_kind: "tool".into(),
            step_or_task_id: key.step_or_task_id.as_str().into(),
            logical_attempt: key.logical_attempt.get(),
            item_index: key.item_index.map(SafeUnsigned::get),
        }),
        "actual Request contains exact full durable K"
    );
    assert_eq!(request.initial_stream_credit_bytes, Some(0));
    let inner = v1_1::InvokeToolRequest::decode(request.payload.as_slice()).unwrap();
    assert_eq!(inner.tool_call_id, fixture()["tool_call_id"]);
}

fn result_count(snapshot: &StorageSnapshot) -> usize {
    snapshot.entries.iter().filter(|e|matches!(e,Entry::Message(m) if matches!(&m.message,AgentMessage::BuiltIn(pi_ai_core::Message::ToolResult(_))))).count()
}
fn check_settled(snapshot: &StorageSnapshot) {
    let results = snapshot
        .entries
        .iter()
        .filter_map(|e| {
            if let Entry::Message(m) = e {
                if let AgentMessage::BuiltIn(pi_ai_core::Message::ToolResult(r)) = &m.message {
                    Some((m, r))
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    assert_eq!(results.len(), 1);
    let (entry, result) = results[0];
    assert!(!result.is_error);
    assert!(
        matches!(&result.content[..],[pi_ai_core::InputContent::Text(t)] if t.text=="after-second")
    );
    assert_eq!(result.usage.as_ref().unwrap().input.get(), 5);
    assert_eq!(
        snapshot
            .usage
            .iter()
            .filter(|u| u.entry_id.as_ref() == Some(&entry.id))
            .count(),
        1
    );
    assert!(
        !snapshot
            .registers
            .iter()
            .any(|r| matches!(r, Register::OperationToolArguments { .. }))
    );
}

struct Gate<S> {
    inner: Arc<S>,
    receipts: Mutex<Vec<CommitResult>>,
    attempts: AtomicUsize,
    refused: AtomicUsize,
    applied: AtomicUsize,
    first_entered: CancellationToken,
    first_release: CancellationToken,
    second_entered: CancellationToken,
    second_release: CancellationToken,
}
impl<S: RuntimeStorage> Gate<S> {
    fn new(inner: Arc<S>) -> Self {
        Self {
            inner,
            receipts: Mutex::new(vec![]),
            attempts: AtomicUsize::new(0),
            refused: AtomicUsize::new(0),
            applied: AtomicUsize::new(0),
            first_entered: CancellationToken::new(),
            first_release: CancellationToken::new(),
            second_entered: CancellationToken::new(),
            second_release: CancellationToken::new(),
        }
    }
}
impl<S: RuntimeStorage> Storage for Gate<S> {
    fn commit(
        &self,
        transaction: Transaction,
    ) -> BoxFuture<'_, Result<CommitResult, StorageError>> {
        self.inner.commit(transaction)
    }
    fn get_entries<'a>(
        &'a self,
        ids: &'a [EntryId],
    ) -> BoxFuture<'a, Result<EntryMap, StorageError>> {
        self.inner.get_entries(ids)
    }
    fn get_register<'a>(
        &'a self,
        namespace: RegisterNamespace,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Option<Register>, StorageError>> {
        self.inner.get_register(namespace, key)
    }
    fn list_registers<'a>(
        &'a self,
        namespace: RegisterNamespace,
        prefix: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Vec<Register>, StorageError>> {
        self.inner.list_registers(namespace, prefix)
    }
    fn scan_branch(&self, query: BranchScan) -> BoxFuture<'_, Result<Vec<Entry>, StorageError>> {
        self.inner.scan_branch(query)
    }
    fn scan_branch_structure(
        &self,
        query: BranchScan,
    ) -> BoxFuture<'_, Result<Vec<EntryStructure>, StorageError>> {
        self.inner.scan_branch_structure(query)
    }
    fn scan_entries(&self, query: EntryScan) -> BoxFuture<'_, Result<Vec<Entry>, StorageError>> {
        self.inner.scan_entries(query)
    }
    fn scan_usage(&self, query: UsageScan) -> BoxFuture<'_, Result<Vec<UsageRow>, StorageError>> {
        self.inner.scan_usage(query)
    }
    fn stats(&self) -> BoxFuture<'_, Result<SessionStats, StorageError>> {
        self.inner.stats()
    }
    fn close(&self) -> BoxFuture<'_, Result<(), StorageError>> {
        self.inner.close()
    }
}
impl<S: RuntimeStorage> AtomicStorage for Gate<S> {
    fn commit_conditional(
        &self,
        request: ConditionalCommit,
    ) -> BoxFuture<'_, Result<ConditionalOutcome, StorageError>> {
        self.inner.commit_conditional(request)
    }
    fn snapshot(&self) -> BoxFuture<'_, Result<StorageSnapshot, StorageError>> {
        self.inner.snapshot()
    }
}
impl<S: RuntimeStorage> CheckedStorage for Gate<S> {
    fn commit_checked<'a, V: PublicationValidator + ?Sized>(
        &'a self,
        request: ConditionalCommit,
        validator: &'a V,
    ) -> BoxFuture<'a, Result<ConditionalOutcome, CheckedCommitError<V::Error>>> {
        self.inner.commit_checked(request, validator)
    }
}
impl<S: RuntimeStorage> RuntimeStorage for Gate<S> {
    fn runtime_attachment(&self) -> &RuntimeAttachment {
        self.inner.runtime_attachment()
    }
    fn read_stamp(&self) -> BoxFuture<'_, Result<ReadStamp, StorageError>> {
        self.inner.read_stamp()
    }
    fn prepare_entries(
        &self,
        stamp: ReadStamp,
        slots: Vec<PreparedEntrySlot>,
    ) -> BoxFuture<'_, Result<Option<PreparedEntries>, StorageError>> {
        self.inner.prepare_entries(stamp, slots)
    }
    fn commit_prepared_checked<'a, V: PublicationValidator + ?Sized>(
        &'a self,
        prepared: PreparedEntries,
        request: ConditionalCommit,
        validator: &'a V,
    ) -> BoxFuture<'a, Result<ConditionalOutcome, CheckedCommitError<V::Error>>> {
        Box::pin(async move {
            let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
            assert!(prepared.entries().iter().any(|entry| matches!(entry, Entry::Message(message) if matches!(&message.message, AgentMessage::BuiltIn(pi_ai_core::Message::ToolResult(_))))));
            match attempt {
                0 => {
                    self.first_entered.cancel();
                    self.first_release.cancelled().await;
                }
                1 => {
                    self.second_entered.cancel();
                    self.second_release.cancelled().await;
                }
                _ => panic!("unexpected extra ToolResult CAS attempt"),
            }
            let outcome = self
                .inner
                .commit_prepared_checked(prepared, request, validator)
                .await?;
            match &outcome {
                ConditionalOutcome::NotApplied => {
                    self.refused.fetch_add(1, Ordering::SeqCst);
                }
                ConditionalOutcome::Applied(receipt) => {
                    self.receipts.lock().unwrap().push(receipt.clone());
                    self.applied.fetch_add(1, Ordering::SeqCst);
                }
            }
            Ok(outcome)
        })
    }
}

#[derive(Clone, Copy, Debug)]
enum History {
    Cas,
    WrongKey,
    WrongGeneration,
    WrongArgs,
    WrongRequest,
    Duplicate,
    BadDigest,
    External,
}
fn run<S: RuntimeStorage + 'static>(
    runtime: &Setup,
    storage: Arc<S>,
    language: &str,
    pair: &Value,
    history: History,
) {
    response(runtime);
    let gate = Arc::new(Gate::new(storage.clone()));
    let session = Arc::new(
        drive(
            runtime,
            RuntimeSession::new(
                gate.clone(),
                runtime.session.metadata(),
                codec(),
                runtime.ids.clone(),
                Arc::new(pi_telemetry::NOOP_TELEMETRY_CONTEXT),
            ),
        )
        .unwrap(),
    );
    drive(
        runtime,
        session.commit(Transaction {
            writes: vec![
                Write::SetRegister(Box::new(RegisterSet::LaneLeaf {
                    key: "main".into(),
                    value: None,
                })),
                Write::SetRegister(Box::new(RegisterSet::LaneState {
                    key: "main".into(),
                    value: LaneState {
                        current_operation_id: None,
                        pending_next_run: vec![],
                    },
                })),
            ],
        }),
    )
    .unwrap();
    let factory = RuntimeFactory::new(
        runtime.time.clone(),
        runtime.pump.clone(),
        runtime.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    let mut options = runtime.options_for::<(), _>(session.clone(), DriveMode::Automatic);
    options.tool_execution = ToolExecutionMode::Sequential;
    let harness = drive(runtime, factory.create_first_attachment(options))
        .unwrap()
        .harness;
    let fault = match history {
        History::WrongRequest => "wrong_request",
        History::Duplicate => "duplicate_terminal",
        History::BadDigest => "terminal_digest_wrong",
        _ => "normal",
    };
    let (owner, channel, route) = connect(runtime, language, pair, fault, WireReplay::Safe);
    drive(
        runtime,
        harness.register_ready_echo_tool(tool(WireReplay::Safe), route, timing()),
    )
    .unwrap()
    .unwrap();
    drive(
        runtime,
        harness.set_active_tools(vec![ToolName::new("echo").unwrap()]),
    )
    .unwrap();
    let log = Arc::new(Mutex::new(vec![]));
    let _before = harness
        .hooks()
        .before_tool(None, Arc::new(Before(log.clone())))
        .unwrap();
    let _first = harness
        .hooks()
        .after_tool(
            None,
            Arc::new(After {
                stage: 1,
                log: log.clone(),
            }),
        )
        .unwrap();
    let _second = harness
        .hooks()
        .after_tool(
            None,
            Arc::new(After {
                stage: 2,
                log: log.clone(),
            }),
        )
        .unwrap();
    let mut prompt = Box::pin(harness.prompt(PromptInput::Text {
        text: "actual peer settlement".into(),
        images: vec![],
    }));
    until(runtime, || {
        assert!(poll(prompt.as_mut()).is_pending());
        channel.request().is_some()
    });
    let initial = drive(runtime, storage.snapshot()).unwrap();
    record_backend::<S>(language, pair, history, "pending", &initial);
    let (key, saved) = pending(&initial);
    check_request(&channel.request().unwrap(), &key);
    assert_eq!(result_count(&initial), 0);
    assert_eq!(*log.lock().unwrap(), ["before"]);
    if matches!(history, History::External) {
        let operation = key.operation_id.as_str().to_owned();
        let state = initial
            .registers
            .iter()
            .find_map(|r| {
                if let Register::OperationState { value, .. } = r {
                    Some(value)
                } else {
                    None
                }
            })
            .unwrap();
        let OperationState::Run(run) = state.as_ref() else {
            unreachable!()
        };
        let RunPhase::Tools(tools) = &run.phase else {
            unreachable!()
        };
        let last = LaneLastResult::RunAborted(RunAbortedLastResult {
            operation_id: key.operation_id.clone(),
            kind: Default::default(),
            leaf_id: drive(runtime, session.get_leaf_id()).unwrap(),
            final_assistant_entry_id: Some(tools.batch.assistant_entry_id.clone()),
            outcome: Default::default(),
        });
        let mut writes = vec![
            Write::DeleteRegister(RegisterDelete {
                namespace: RegisterNamespace::OperationState,
                key: operation.clone(),
            }),
            Write::DeleteRegister(RegisterDelete {
                namespace: RegisterNamespace::OperationMeta,
                key: operation,
            }),
            Write::SetRegister(Box::new(RegisterSet::LaneState {
                key: "main".into(),
                value: LaneState {
                    current_operation_id: None,
                    pending_next_run: vec![],
                },
            })),
        ];
        writes.push(Write::SetRegister(Box::new(RegisterSet::LaneLastResult {
            key: "main".into(),
            value: last,
        })));
        for r in &initial.registers {
            if let Register::OperationPreparation { key, .. } = r {
                writes.push(Write::DeleteRegister(RegisterDelete {
                    namespace: RegisterNamespace::OperationPreparation,
                    key: key.clone(),
                }));
            }
        }
        if let Register::OperationToolArguments { key, .. } = saved {
            writes.push(Write::DeleteRegister(RegisterDelete {
                namespace: RegisterNamespace::OperationToolArguments,
                key,
            }));
        }
        drive(runtime, session.commit(Transaction { writes })).unwrap();
        let finalized = drive(runtime, storage.snapshot()).unwrap();
        record_backend::<S>(language, pair, history, "external_finalized", &finalized);
        channel.release();
        let outcome = drive(runtime, prompt.as_mut());
        assert!(outcome.is_ok(), "external completion: {outcome:?}");
        assert_eq!(
            drive(runtime, storage.snapshot()).unwrap(),
            finalized,
            "external finalization no result/usage/write"
        );
        assert_eq!(*log.lock().unwrap(), ["before"]);
        assert!(owner.retained_binding().is_none());
        channel.close();
        channel.finish(false);
        drive(runtime, harness.close()).unwrap();
        return;
    }
    channel.release();
    if matches!(history, History::WrongRequest | History::BadDigest) {
        let outcome = drive(runtime, prompt.as_mut());
        assert!(
            matches!(outcome, Err(HarnessRuntimeError::PeerLost)),
            "bad peer binding/value: {outcome:?}"
        );
        assert_eq!(
            drive(runtime, storage.snapshot()).unwrap(),
            initial,
            "wrong request/digest must cause zero durable writes"
        );
        assert_eq!(*log.lock().unwrap(), ["before"]);
        assert!(owner.retained_binding().is_none());
        println!(
            "NO_WRITE1376 language={language} history={history:?} full_snapshot_unchanged=true"
        );
        channel.finish(false);
        drive(runtime, harness.close()).unwrap();
        return;
    }
    until(runtime, || {
        assert!(
            poll(prompt.as_mut()).is_pending(),
            "validated first terminal must reach checked settlement"
        );
        gate.first_entered.is_cancelled()
    });
    let binding = owner
        .retained_binding()
        .expect("actual first terminal retained");
    assert_eq!(
        owner.binding_phase(&binding),
        Ok(LivePhase::TerminalReceived)
    );
    assert_eq!(*log.lock().unwrap(), ["before", "after-1", "after-2"]);
    assert_eq!(
        drive(runtime, storage.snapshot()).unwrap(),
        initial,
        "before checked settlement"
    );
    if matches!(history, History::WrongGeneration) {
        foreign_generation_is_inert(runtime, storage.as_ref(), &owner, &binding, language, pair);
    }
    let state = initial
        .registers
        .iter()
        .find_map(|r| {
            if let Register::OperationState { key, value, .. } = r {
                Some((key.clone(), value.clone()))
            } else {
                None
            }
        })
        .unwrap();
    let change = if matches!(history, History::WrongKey) {
        let mut value = state.1.clone();
        let OperationState::Run(run) = value.as_mut() else {
            unreachable!()
        };
        let RunPhase::Tools(tools) = &mut run.phase else {
            unreachable!()
        };
        tools.batch.turn_id = StepId::new("changed-durable-step").unwrap();
        RegisterSet::OperationState {
            key: state.0,
            value,
        }
    } else if matches!(history, History::WrongArgs) {
        let Register::OperationToolArguments { key, .. } = saved else {
            unreachable!()
        };
        RegisterSet::OperationToolArguments {
            key,
            value: args("wrong-durable-args"),
        }
    } else {
        RegisterSet::OperationState {
            key: state.0,
            value: state.1,
        }
    };
    drive(
        runtime,
        session.commit(Transaction {
            writes: vec![Write::SetRegister(Box::new(change))],
        }),
    )
    .unwrap();
    let refused_snapshot = drive(runtime, storage.snapshot()).unwrap();
    record_backend::<S>(language, pair, history, "prior_cas", &refused_snapshot);
    gate.first_release.cancel();
    if matches!(history, History::WrongArgs | History::WrongKey) {
        let outcome = drive(runtime, prompt.as_mut());
        assert!(
            matches!(outcome, Err(HarnessRuntimeError::Fault(_))),
            "wrong durable args cannot borrow first receipt: {outcome:?}"
        );
        assert_eq!(gate.refused.load(Ordering::SeqCst), 1);
        assert_eq!(
            drive(runtime, storage.snapshot()).unwrap(),
            refused_snapshot,
            "wrong durable binding zero writes"
        );
        assert_eq!(channel.requests(), 1);
        println!(
            "NO_WRITE1376 language={language} history={history:?} full_snapshot_unchanged=true"
        );
        channel.close();
        channel.finish(false);
        assert!(matches!(
            drive(runtime, harness.close()),
            Err(HarnessRuntimeError::Fault(_))
        ));
        return;
    }
    until(runtime, || {
        let _ = poll(prompt.as_mut());
        gate.second_entered.is_cancelled()
    });
    assert_eq!(gate.refused.load(Ordering::SeqCst), 1);
    assert_eq!(gate.applied.load(Ordering::SeqCst), 0);
    assert!(
        owner
            .retained_binding()
            .unwrap()
            .same_live_identity(&binding)
    );
    assert_eq!(
        owner.binding_phase(&binding),
        Ok(LivePhase::TerminalReceived)
    );
    assert_eq!(
        drive(runtime, storage.snapshot()).unwrap(),
        refused_snapshot,
        "real CAS refusal earns no result/usage"
    );
    assert_eq!(channel.requests(), 1);
    assert_eq!(*log.lock().unwrap(), ["before", "after-1", "after-2"]);
    gate.second_release.cancel();
    drive(runtime, prompt.as_mut()).unwrap().unwrap();
    let settled = drive(runtime, storage.snapshot()).unwrap();
    check_settled(&settled);
    check_receipt(&settled, &gate.receipts.lock().unwrap());
    record_backend::<S>(language, pair, history, "settled", &settled);
    assert_eq!(gate.applied.load(Ordering::SeqCst), 1);
    assert!(owner.retained_binding().is_none());
    assert_eq!(channel.requests(), 1);
    let before_late = settled.clone();
    until(runtime, || {
        channel
            .child
            .lock()
            .unwrap()
            .as_mut()
            .unwrap()
            .try_wait()
            .unwrap()
            .is_some()
    });
    assert_eq!(
        drive(runtime, storage.snapshot()).unwrap(),
        before_late,
        "late duplicate/EOF cannot replace first result"
    );
    channel.finish(!matches!(history, History::Duplicate));
    drive(runtime, harness.close()).unwrap();
    println!(
        "CASE1376 language={language} pair={} history={history:?} key={key:?} refused=1 applied=1 results={} usage={}",
        pair["id"],
        result_count(&settled),
        settled.usage.len()
    );
}

#[test]
fn actual_runtime_peers_memory_and_sqlite_settlement() {
    let f = fixture();
    for language in ["typescript", "python"] {
        for pair in f["version_pairs"].as_array().unwrap() {
            let runtime = Setup::new();
            run(
                &runtime,
                runtime.store.clone(),
                language,
                pair,
                History::Cas,
            );
            let runtime = Setup::new();
            let file = File::new();
            let sqlite = SqliteStorage::create(
                &file.0,
                runtime.session.metadata(),
                codec(),
                runtime.time.clone(),
                SqliteOptions::default(),
            )
            .unwrap();
            run(&runtime, sqlite.clone(), language, pair, History::Cas);
            drive(&runtime, sqlite.close()).unwrap();
            let reopened = SqliteStorage::open(
                &file.0,
                codec(),
                runtime.time.clone(),
                SqliteOptions::default(),
            )
            .unwrap();
            check_settled(&drive(&runtime, reopened.snapshot()).unwrap());
            drive(&runtime, reopened.close()).unwrap();
        }
    }
}
#[test]
fn actual_runtime_peers_wrong_binding_and_first_terminal_no_write() {
    let f = fixture();
    let pair = &f["version_pairs"][1];
    for language in ["typescript", "python"] {
        for history in f["binding_histories"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| history(v.as_str().unwrap()))
        {
            let runtime = Setup::new();
            run(&runtime, runtime.store.clone(), language, pair, history);
            let runtime = Setup::new();
            let file = File::new();
            let sqlite = SqliteStorage::create(
                &file.0,
                runtime.session.metadata(),
                codec(),
                runtime.time.clone(),
                SqliteOptions::default(),
            )
            .unwrap();
            run(&runtime, sqlite.clone(), language, pair, history);
            drive(&runtime, sqlite.close()).unwrap();
        }
    }
}

struct File(PathBuf);
impl File {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "pi-runtime-peer1376-db-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path.join("session.sqlite"))
    }
}
impl Drop for File {
    fn drop(&mut self) {
        std::fs::remove_dir_all(self.0.parent().unwrap()).unwrap();
    }
}

fn crash_options() -> SqliteOptions {
    SqliteOptions {
        lease_ttl_ms: SafeUnsigned::new(120000).unwrap(),
        heartbeat_ms: SafeUnsigned::new(1000).unwrap(),
        ..Default::default()
    }
}
fn crash_child(path: &Path, language: &str, replay: WireReplay) -> ! {
    let runtime = Setup::new();
    response(&runtime);
    let sqlite = SqliteStorage::open(path, codec(), runtime.time.clone(), crash_options()).unwrap();
    let session = Arc::new(
        drive(
            &runtime,
            RuntimeSession::new(
                sqlite.clone(),
                sqlite.metadata().unwrap(),
                codec(),
                runtime.ids.clone(),
                Arc::new(pi_telemetry::NOOP_TELEMETRY_CONTEXT),
            ),
        )
        .unwrap(),
    );
    drive(
        &runtime,
        session.commit(Transaction {
            writes: vec![
                Write::SetRegister(Box::new(RegisterSet::LaneLeaf {
                    key: "main".into(),
                    value: None,
                })),
                Write::SetRegister(Box::new(RegisterSet::LaneState {
                    key: "main".into(),
                    value: LaneState {
                        current_operation_id: None,
                        pending_next_run: vec![],
                    },
                })),
            ],
        }),
    )
    .unwrap();
    let factory = RuntimeFactory::new(
        runtime.time.clone(),
        runtime.pump.clone(),
        runtime.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    let mut options = runtime.options_for::<(), _>(session, DriveMode::Automatic);
    options.tool_execution = ToolExecutionMode::Sequential;
    let harness = drive(&runtime, factory.create_first_attachment(options))
        .unwrap()
        .harness;
    let f = fixture();
    let pair = &f["version_pairs"][1];
    let (owner, channel, route) = connect(&runtime, language, pair, "normal", replay);
    drive(
        &runtime,
        harness.register_ready_echo_tool(tool(replay), route, timing()),
    )
    .unwrap()
    .unwrap();
    drive(
        &runtime,
        harness.set_active_tools(vec![ToolName::new("echo").unwrap()]),
    )
    .unwrap();
    let _before = harness
        .hooks()
        .before_tool(None, Arc::new(Before(Arc::new(Mutex::new(vec![])))))
        .unwrap();
    let mut prompt = Box::pin(harness.prompt(PromptInput::Text {
        text: "crash with actual pending peer".into(),
        images: vec![],
    }));
    until(&runtime, || {
        assert!(poll(prompt.as_mut()).is_pending());
        channel.request().is_some() && channel.request_trace().is_some()
    });
    let snapshot = drive(&runtime, sqlite.snapshot()).unwrap();
    let (key, saved) = pending(&snapshot);
    check_request(&channel.request().unwrap(), &key);
    assert_eq!(channel.requests(), 1);
    let binding = owner.retained_binding().expect("actual G1 live binding");
    assert_eq!(owner.binding_phase(&binding), Ok(LivePhase::Open));
    assert_eq!(binding.generation(), 7);
    assert_eq!(binding.request_id(), channel.request().unwrap().request_id);
    let peer_request = channel.request_trace().unwrap();
    assert_eq!(peer_request["generation"], 7);
    assert_eq!(
        peer_request["args"],
        serde_json::json!({"text":fixture()["effective_text"]})
    );

    // Reap the language child while keeping the runtime future and SQLite owner live.
    // Exit bypasses Rust destructors only after this exact pending clearance is observed.
    channel.close();
    channel.finish(false);
    let high = runtime
        .ids
        .next(None)
        .strip_prefix("runtime-id-")
        .unwrap()
        .parse::<u64>()
        .unwrap();
    println!(
        "PENDING1376 {}",
        serde_json::json!({"key":key_record(&key),"saved":saved,"high":high,"generation":7,"request_id":1,"requests":1,"peer_request":peer_request,"language":language,"replay":if replay==WireReplay::Safe{"safe"}else{"never"}})
    );
    std::io::stdout().flush().unwrap();
    std::process::exit(73)
}
struct Process(Option<Child>);
impl Drop for Process {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = child.kill();
            let deadline = Instant::now() + LIMIT;
            loop {
                if child.try_wait().unwrap().is_some() {
                    break;
                }
                assert!(Instant::now() < deadline, "finite crash child reap");
                std::thread::sleep(Duration::from_millis(1));
            }
        }
    }
}
fn pending_process(path: &Path, language: &str, replay: WireReplay) -> Value {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args([
            "--exact",
            "actual_sqlite_runtime_peers_process_end_safe_never",
            "--nocapture",
        ])
        .env("PI_PEER1376_CRASH_PATH", path)
        .env("PI_PEER1376_CRASH_LANGUAGE", language)
        .env(
            "PI_PEER1376_CRASH_REPLAY",
            if replay == WireReplay::Safe {
                "safe"
            } else {
                "never"
            },
        )
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut process = Process(Some(command.spawn().unwrap()));
    let deadline = Instant::now() + LIMIT;
    loop {
        if process.0.as_mut().unwrap().try_wait().unwrap().is_some() {
            break;
        }
        assert!(Instant::now() < deadline, "finite pending child exit");
        std::thread::sleep(Duration::from_millis(1));
    }
    let output = process.0.take().unwrap().wait_with_output().unwrap();
    assert_eq!(
        output.status.code(),
        Some(73),
        "crash child: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stdout.len() < 65536 && output.stderr.len() < 65536);
    let stdout = String::from_utf8(output.stdout).unwrap();
    let marker = stdout
        .lines()
        .find_map(|s| s.strip_prefix("PENDING1376 "))
        .expect("actual pending child marker");
    serde_json::from_str(marker).unwrap()
}
#[test]
fn actual_sqlite_runtime_peers_process_end_safe_never() {
    if let Some(path) = std::env::var_os("PI_PEER1376_CRASH_PATH") {
        let language = std::env::var("PI_PEER1376_CRASH_LANGUAGE").unwrap();
        let replay = if std::env::var("PI_PEER1376_CRASH_REPLAY").unwrap() == "safe" {
            WireReplay::Safe
        } else {
            WireReplay::Never
        };
        crash_child(Path::new(&path), &language, replay);
    }
    for language in ["typescript", "python"] {
        for replay in [WireReplay::Safe, WireReplay::Never] {
            let runtime = Setup::new();
            let file = File::new();
            let initial = SqliteStorage::create(
                &file.0,
                runtime.session.metadata(),
                codec(),
                runtime.time.clone(),
                crash_options(),
            )
            .unwrap();
            drive(&runtime, initial.close()).unwrap();
            let marker = pending_process(&file.0, language, replay);
            let before = SqliteStorage::read_snapshot(&file.0, &codec(), crash_options()).unwrap();
            let (key, saved) = pending(&before.snapshot);
            assert_eq!(key_record(&key), marker["key"]);
            assert_eq!(serde_json::to_value(&saved).unwrap(), marker["saved"]);
            assert_eq!(result_count(&before.snapshot), 0);
            assert!(matches!(
                SqliteStorage::open(&file.0, codec(), runtime.time.clone(), crash_options()),
                Err(SessionError::Storage(StorageError::Busy(_)))
            ));
            // Match the dead process's exact lease; change no session rows or revision.
            let mut db = rusqlite::Connection::open(&file.0).unwrap();
            let lease: (Vec<u8>, i64) = db
                .query_row(
                    "SELECT owner_id,fence FROM writer_lease WHERE singleton=1",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .unwrap();
            let tx = db
                .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
                .unwrap();
            assert_eq!(tx.execute("UPDATE writer_lease SET expires_at_ms=0 WHERE singleton=1 AND owner_id=?1 AND fence=?2",rusqlite::params![lease.0,lease.1]).unwrap(),1);
            tx.commit().unwrap();
            let sqlite =
                SqliteStorage::open(&file.0, codec(), runtime.time.clone(), crash_options())
                    .unwrap();
            let reopened =
                SqliteStorage::read_snapshot(&file.0, &codec(), crash_options()).unwrap();
            assert_eq!(before.snapshot, reopened.snapshot);
            assert_eq!(before.revision, reopened.revision);
            assert_eq!(pending(&reopened.snapshot), (key.clone(), saved.clone()));
            for n in 0..=marker["high"].as_u64().unwrap() {
                assert_eq!(runtime.ids.next(None), format!("runtime-id-{n}"));
            }
            *runtime.faux.responses.lock().unwrap() =
                vec![assistant(&runtime.faux.model, StopReason::Stop)].into();
            let session = Arc::new(
                drive(
                    &runtime,
                    RuntimeSession::new(
                        sqlite.clone(),
                        sqlite.metadata().unwrap(),
                        codec(),
                        runtime.ids.clone(),
                        Arc::new(pi_telemetry::NOOP_TELEMETRY_CONTEXT),
                    ),
                )
                .unwrap(),
            );
            let factory = RuntimeFactory::new(
                runtime.time.clone(),
                runtime.pump.clone(),
                runtime.ids.clone(),
                Arc::new(()),
                Arc::new(Summary),
            );
            let mut options = runtime.options_for::<(), _>(session, DriveMode::Automatic);
            options.tool_execution = ToolExecutionMode::Sequential;
            if replay == WireReplay::Never {
                options.tools = vec![tool(replay)];
            }
            let harness = drive(&runtime, factory.create_configured(options))
                .unwrap()
                .harness;
            let f = fixture();
            let (owner, channel, route) = connect_generation(
                &runtime,
                language,
                &f["version_pairs"][1],
                "normal",
                replay,
                8,
            );
            if replay == WireReplay::Safe {
                drive(
                    &runtime,
                    harness.register_recovered_echo_pending(
                        key.clone(),
                        tool(replay).descriptor().clone(),
                    ),
                )
                .unwrap()
                .unwrap();
            }
            drive(
                &runtime,
                harness.register_ready_echo_tool(tool(replay), route, timing()),
            )
            .unwrap()
            .unwrap();
            let log = Arc::new(Mutex::new(vec![]));
            let _one = harness
                .hooks()
                .after_tool(
                    None,
                    Arc::new(After {
                        stage: 1,
                        log: log.clone(),
                    }),
                )
                .unwrap();
            let _two = harness
                .hooks()
                .after_tool(
                    None,
                    Arc::new(After {
                        stage: 2,
                        log: log.clone(),
                    }),
                )
                .unwrap();
            let mut resume = Box::pin(harness.resume());
            if replay == WireReplay::Safe {
                until(&runtime, || {
                    assert!(poll(resume.as_mut()).is_pending());
                    channel.request().is_some()
                });
                let request = channel.request().unwrap();
                check_request(&request, &key);
                assert_eq!(
                    request.effect_key.as_ref().unwrap().operation_id,
                    key.operation_id.as_str()
                );
                assert_eq!(
                    drive(&runtime, sqlite.snapshot()).unwrap(),
                    before.snapshot,
                    "fresh binding admission preserves K/saved args"
                );
                channel.release();
                drive(&runtime, resume.as_mut()).unwrap().unwrap();
                check_settled(&drive(&runtime, sqlite.snapshot()).unwrap());
                assert_eq!(channel.requests(), 1);
                assert_eq!(*log.lock().unwrap(), ["after-1", "after-2"]);
                channel.finish(true);
            } else {
                // Never is reconciled by the existing native interrupted recipe, without peer invocation.
                drive(&runtime, resume.as_mut()).unwrap().unwrap();
                assert_eq!(
                    channel.requests(),
                    0,
                    "Never cannot send a replacement Request"
                );
                let settled = drive(&runtime, sqlite.snapshot()).unwrap();
                assert_eq!(result_count(&settled), 1);
                let result = settled
                    .entries
                    .iter()
                    .find_map(|e| {
                        if let Entry::Message(m) = e {
                            if let AgentMessage::BuiltIn(pi_ai_core::Message::ToolResult(r)) =
                                &m.message
                            {
                                Some((m, r))
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    })
                    .unwrap();
                assert!(result.1.is_error && result.1.usage.is_none());
                assert!(
                    !settled
                        .usage
                        .iter()
                        .any(|u| u.entry_id.as_ref() == Some(&result.0.id))
                );
                assert!(
                    !settled
                        .registers
                        .iter()
                        .any(|r| matches!(r, Register::OperationToolArguments { .. }))
                );
                assert!(log.lock().unwrap().is_empty());
                channel.close();
                channel.finish(false);
            }
            assert!(owner.retained_binding().is_none());
            drive(&runtime, harness.close()).unwrap();
            drive(&runtime, sqlite.close()).unwrap();
            println!(
                "CRASH1376 language={language} replay={replay:?} original_generation=7 replacement_generation=8 key={key:?} child_marker={marker}"
            );
        }
    }
}

fn key_record(key: &EffectKey) -> Value {
    serde_json::json!({"session_id":key.session_id.as_str(),"lane":key.lane.as_str(),"operation_id":key.operation_id.as_str(),"effect_kind":"tool","step_or_task_id":key.step_or_task_id.as_str(),"logical_attempt":key.logical_attempt.get(),"item_index":key.item_index.map(SafeUnsigned::get)})
}

fn foreign_generation_is_inert<S: RuntimeStorage>(
    g1_runtime: &Setup,
    g1_storage: &S,
    g1_owner: &SingleToolOwner,
    g1_binding: &pi_extension_protocol::tool_live::InvocationBinding,
    language: &str,
    pair: &Value,
) {
    use pi_extension_protocol::tool_live::RetireReason;
    use pi_harness_runtime::extensions::tool_driver::DriverError;
    let runtime = Setup::new();
    response(&runtime);
    let gate = Arc::new(Gate::new(runtime.store.clone()));
    let session = Arc::new(
        drive(
            &runtime,
            RuntimeSession::new(
                gate.clone(),
                runtime.session.metadata(),
                codec(),
                runtime.ids.clone(),
                Arc::new(pi_telemetry::NOOP_TELEMETRY_CONTEXT),
            ),
        )
        .unwrap(),
    );
    let factory = RuntimeFactory::new(
        runtime.time.clone(),
        runtime.pump.clone(),
        runtime.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    let mut options = runtime.options_for::<(), _>(session, DriveMode::Automatic);
    options.tool_execution = ToolExecutionMode::Sequential;
    let harness = drive(&runtime, factory.create_configured(options))
        .unwrap()
        .harness;
    let (g2_owner, channel, route) =
        connect_generation(&runtime, language, pair, "normal", WireReplay::Safe, 8);
    drive(
        &runtime,
        harness.register_ready_echo_tool(tool(WireReplay::Safe), route, timing()),
    )
    .unwrap()
    .unwrap();
    drive(
        &runtime,
        harness.set_active_tools(vec![ToolName::new("echo").unwrap()]),
    )
    .unwrap();
    let log = Arc::new(Mutex::new(vec![]));
    let _before = harness
        .hooks()
        .before_tool(None, Arc::new(Before(log.clone())))
        .unwrap();
    let _one = harness
        .hooks()
        .after_tool(
            None,
            Arc::new(After {
                stage: 1,
                log: log.clone(),
            }),
        )
        .unwrap();
    let _two = harness
        .hooks()
        .after_tool(
            None,
            Arc::new(After {
                stage: 2,
                log: log.clone(),
            }),
        )
        .unwrap();
    let mut prompt = Box::pin(harness.prompt(PromptInput::Text {
        text: "another actual owner/generation".into(),
        images: vec![],
    }));
    until(&runtime, || {
        assert!(poll(prompt.as_mut()).is_pending());
        channel.request().is_some()
    });
    channel.release();
    until(&runtime, || {
        let _ = poll(prompt.as_mut());
        gate.first_entered.is_cancelled()
    });
    let g2_binding = g2_owner.retained_binding().unwrap();
    assert_eq!(g1_binding.generation(), 7);
    assert_eq!(g2_binding.generation(), 8);
    assert_eq!(g1_binding.request_id(), g2_binding.request_id());
    assert_eq!(
        g1_binding.effect_key(),
        g2_binding.effect_key(),
        "same durable K cannot confer another live identity"
    );
    assert!(!g1_binding.same_live_identity(&g2_binding));
    let before1 = drive(g1_runtime, g1_storage.snapshot()).unwrap();
    let before2 = drive(&runtime, runtime.store.snapshot()).unwrap();
    assert_eq!(
        g1_owner.retire(&g2_binding, RetireReason::Committed),
        Err(DriverError::WrongBinding)
    );
    assert_eq!(
        g2_owner.retire(g1_binding, RetireReason::NoLongerExpected),
        Err(DriverError::WrongBinding)
    );
    assert_eq!(
        g1_owner.binding_phase(g1_binding),
        Ok(LivePhase::TerminalReceived)
    );
    assert_eq!(
        g2_owner.binding_phase(&g2_binding),
        Ok(LivePhase::TerminalReceived)
    );
    assert_eq!(drive(g1_runtime, g1_storage.snapshot()).unwrap(), before1);
    assert_eq!(drive(&runtime, runtime.store.snapshot()).unwrap(), before2);
    gate.first_release.cancel();
    drive(&runtime, prompt.as_mut()).unwrap().unwrap();
    check_settled(&drive(&runtime, runtime.store.snapshot()).unwrap());
    channel.finish(true);
    drive(&runtime, harness.close()).unwrap();
    println!(
        "WRONG_GENERATION1376 actual_G1=7 actual_G2=8 same_fullK=true same_request_id=true foreign_retire=WrongBinding both_snapshots_unchanged=true"
    );
}

fn history(id: &str) -> History {
    match id {
        "wrong_key" => History::WrongKey,
        "wrong_generation" => History::WrongGeneration,
        "wrong_args" => History::WrongArgs,
        "wrong_request" => History::WrongRequest,
        "duplicate_terminal" => History::Duplicate,
        "terminal_digest_wrong" => History::BadDigest,
        "external_finalization" => History::External,
        _ => panic!("unknown corpus history"),
    }
}
fn record_backend<S>(
    language: &str,
    pair: &Value,
    history: History,
    stage: &str,
    snapshot: &StorageSnapshot,
) {
    println!(
        "BACKEND1376 {}",
        serde_json::json!({"backend":std::any::type_name::<S>(),"language":language,"pair":pair["id"],"history":format!("{history:?}"),"stage":stage,"snapshot":snapshot})
    );
}
fn check_receipt(snapshot: &StorageSnapshot, receipts: &[CommitResult]) {
    assert_eq!(receipts.len(), 1);
    let (entry, row) = snapshot
        .entries
        .iter()
        .find_map(|e| {
            if let Entry::Message(m) = e {
                if matches!(
                    &m.message,
                    AgentMessage::BuiltIn(pi_ai_core::Message::ToolResult(_))
                ) {
                    Some((
                        m,
                        snapshot
                            .usage
                            .iter()
                            .find(|u| u.entry_id.as_ref() == Some(&m.id))
                            .unwrap(),
                    ))
                } else {
                    None
                }
            } else {
                None
            }
        })
        .unwrap();
    assert!(receipts[0].seqs.contains(&entry.seq));
    assert!(receipts[0].seqs.contains(&row.seq));
    println!(
        "COMMIT1376 {}",
        serde_json::json!({"receipt":receipts[0],"result_entry_id":entry.id,"result_seq":entry.seq,"usage_id":row.id,"usage_seq":row.seq})
    );
}

fn join_worker(worker: std::thread::JoinHandle<()>) {
    let deadline = Instant::now() + LIMIT;
    while !worker.is_finished() {
        assert!(Instant::now() < deadline, "finite pipe worker join");
        std::thread::sleep(Duration::from_millis(1));
    }
    worker.join().unwrap();
}
