//! Real Node/Python protocol-only echo against Rust Endpoint and ToolInvocation.
use std::io::{Read, Write};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use pi_extension_protocol::activation::{
    AgentTool, AgentToolMode, ApprovedTool, HostAuthority, ToolReplay, ToolSchemaInputs,
};
use pi_extension_protocol::endpoint::{Endpoint, EndpointError, EndpointEvent, EndpointPhase};
use pi_extension_protocol::jcs::{self, Limits as JsonLimits};
use pi_extension_protocol::tool_live::{
    DomainDecision, LiveError, LivePhase, ReceiveOutcome, SelectedArguments, Start, TerminalKind,
    ToolInvocation,
};
use pi_extension_protocol::wire::{v1_0, v1_1};
use prost::Message;
use ring::digest::{SHA256, digest};
use serde_json::Value;

const FIX: &str = include_str!("../../../conformance/extension-protocol/echo-v1.json");
const CAT: &str = include_str!("../schema/fixtures/host-catalog.json");
const DIGESTS: &str = include_str!("../../../sdk/typescript/digests.json");
const PHASE: Duration = Duration::from_secs(3);
const WHOLE: Duration = Duration::from_secs(15);
const REAP: Duration = Duration::from_secs(2);
const MAX_FRAMES: usize = 8;

fn root() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}
fn sha(raw: &[u8]) -> Vec<u8> {
    digest(&SHA256, raw).as_ref().to_vec()
}
fn unhex(s: &str) -> Vec<u8> {
    let (pairs, tail) = s.as_bytes().as_chunks::<2>();
    assert!(tail.is_empty());
    pairs
        .iter()
        .map(|p| u8::from_str_radix(std::str::from_utf8(p).unwrap(), 16).unwrap())
        .collect()
}
fn canonical_profile(minor: u32) -> Vec<u8> {
    let raw: &[u8] = if minor == 0 {
        include_bytes!("../schema/descriptors/tool-1.0.pb")
    } else {
        include_bytes!("../schema/descriptors/tool-1.1.pb")
    };
    let mut pre = b"tool.v1\0".to_vec();
    pre.extend_from_slice(format!("1.{minor}").as_bytes());
    pre.push(0);
    pre.extend_from_slice(raw);
    sha(&pre)
}
fn catalog() -> Value {
    serde_json::from_str::<Value>(CAT).unwrap()["entries"][0].clone()
}
fn schemas(minor: u32) -> Vec<(String, Vec<u8>)> {
    let entry = catalog();
    let sets = &entry["selected_minor_schema_sets"][format!("1.{minor}")];
    ["arguments", "progress", "raw_result"]
        .into_iter()
        .map(|slot| {
            let schema = serde_json::to_vec(&sets[slot]["schema"]).unwrap();
            let admitted = jcs::admit_schema(&schema, JsonLimits::default()).unwrap();
            let actual = admitted.canonical().sha256().to_vec();
            assert_eq!(
                actual,
                unhex(sets[slot]["schema_digest_sha256"].as_str().unwrap()),
                "catalog derivative"
            );
            (slot.into(), actual)
        })
        .collect()
}
fn preflight_derivatives() {
    let d: Value = serde_json::from_str(DIGESTS).unwrap();
    assert_eq!(d["format"], "s12-echo-digests/1");
    for minor in [0, 1] {
        let version = format!("1.{minor}");
        assert_eq!(
            canonical_profile(minor),
            unhex(d["profile"][&version].as_str().unwrap())
        );
        for (slot, actual) in schemas(minor) {
            assert_eq!(
                actual,
                unhex(d["schema"][&version][&slot].as_str().unwrap())
            );
        }
    }
}
#[cfg(unix)]
fn signal_process_group(pid: u32, deadline: Instant) -> Result<(), String> {
    let mut signal = Command::new("/usr/bin/kill")
        .args(["-KILL", "--", &format!("-{pid}")])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|e| format!("could not signal process group: {e}"))?;
    loop {
        match signal.try_wait() {
            Ok(Some(status)) => {
                return if status.success() {
                    Ok(())
                } else {
                    Err(format!("process-group signal failed: {status}"))
                };
            }
            Ok(None) if Instant::now() < deadline => thread::sleep(Duration::from_millis(5)),
            Ok(None) => {
                let _ = signal.kill();
                let reaped = reap_until(&mut signal, Instant::now() + REAP);
                return Err(format!(
                    "process-group signal deadline; helper reap {reaped:?}"
                ));
            }
            Err(e) => return Err(format!("could not poll process-group signal: {e}")),
        }
    }
}
#[cfg(unix)]
fn reap_until(child: &mut Child, deadline: Instant) -> Result<std::process::ExitStatus, String> {
    loop {
        match child.try_wait() {
            Ok(Some(status)) => return Ok(status),
            Ok(None) if Instant::now() < deadline => thread::sleep(Duration::from_millis(5)),
            Ok(None) => return Err("child was not reaped within cleanup deadline".into()),
            Err(e) => return Err(format!("could not poll child exit: {e}")),
        }
    }
}
#[cfg(unix)]
fn run_gate(mut command: Command, timeout: Duration) -> Result<(), String> {
    use std::os::unix::process::CommandExt;
    command.process_group(0);
    let mut child = command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|e| format!("required executable absent: {e}"))?;
    let deadline = Instant::now() + timeout;
    loop {
        match child.try_wait() {
            Ok(Some(status)) if status.success() && Instant::now() <= deadline => return Ok(()),
            Ok(Some(status)) if status.success() => {
                let _ = signal_process_group(child.id(), Instant::now() + REAP);
                return Err("required preflight/runtime timeout after child exit".into());
            }
            Ok(Some(status)) => {
                // A failed checker may still have descendants in its isolated group.
                let _ = signal_process_group(child.id(), Instant::now() + REAP);
                return Err(format!("required preflight/runtime failed: {status}"));
            }
            Ok(None) if Instant::now() < deadline => thread::sleep(Duration::from_millis(10)),
            Ok(None) => {
                let cleanup_deadline = Instant::now() + REAP;
                let signal = signal_process_group(child.id(), cleanup_deadline);
                let reaped = reap_until(&mut child, cleanup_deadline);
                return Err(format!(
                    "required preflight/runtime timeout; group signal {signal:?}; direct child reap {reaped:?}"
                ));
            }
            Err(e) => {
                let cleanup_deadline = Instant::now() + REAP;
                let signal = signal_process_group(child.id(), cleanup_deadline);
                let reaped = reap_until(&mut child, cleanup_deadline);
                return Err(format!(
                    "required preflight/runtime poll error {e}; group signal {signal:?}; direct child reap {reaped:?}"
                ));
            }
        }
    }
}
#[cfg(not(unix))]
fn run_gate(_command: Command, _timeout: Duration) -> Result<(), String> {
    Err("isolated process-group cleanup requires Unix".into())
}
fn prerequisite() {
    let mut codegen = Command::new("cargo");
    codegen
        .current_dir(root())
        .env_remove("CARGO_TARGET_DIR")
        .args([
            "run",
            "--manifest-path",
            "crates/pi-extension-protocol/codegen/Cargo.toml",
            "--locked",
            "--offline",
        ]);
    run_gate(codegen, Duration::from_secs(120)).unwrap();
    let mut node = Command::new("node");
    node.args([
        "--experimental-strip-types",
        "--eval",
        "let x: number = 1; if (x !== 1) process.exit(2)",
    ]);
    run_gate(node, PHASE).unwrap();
    let mut python = Command::new("python3");
    python.args(["-B", "-c", "import google.protobuf"]);
    run_gate(python, PHASE).unwrap();
}

// Workers own all potentially blocking pipe operations; the controller only uses deadlines.
enum ReadEvent {
    Frame(Vec<u8>),
    Eof,
    Failed(String),
}
fn read_worker(mut out: impl Read, tx: SyncSender<ReadEvent>) {
    let mut total = 0usize;
    for frame_index in 0..MAX_FRAMES {
        let mut prefix = Vec::new();
        let mut len = 0usize;
        let mut shift = 0;
        loop {
            let mut b = [0u8; 1];
            match out.read(&mut b) {
                Ok(0) if prefix.is_empty() => {
                    let _ = tx.send(ReadEvent::Eof);
                    return;
                }
                Ok(0) => {
                    let _ = tx.send(ReadEvent::Failed("partial prefix".into()));
                    return;
                }
                Ok(_) => {}
                Err(e) => {
                    let _ = tx.send(ReadEvent::Failed(e.to_string()));
                    return;
                }
            }
            prefix.push(b[0]);
            if prefix.len() > 10 {
                let _ = tx.send(ReadEvent::Failed("prefix cap".into()));
                return;
            }
            len |= ((b[0] & 127) as usize)
                .checked_shl(shift)
                .unwrap_or(usize::MAX);
            if b[0] < 128 {
                break;
            }
            shift += 7;
        }
        if prefix.len() != varint(len).len()
            || len == 0
            || len > 8192
            || (frame_index < 3 && len > 4096)
        {
            let _ = tx.send(ReadEvent::Failed("nonshort or over-limit frame".into()));
            return;
        }
        total += prefix.len() + len;
        if total > 65536 {
            let _ = tx.send(ReadEvent::Failed("stdout budget".into()));
            return;
        }
        let mut raw = prefix;
        let target = raw.len() + len;
        while raw.len() < target {
            let mut b = [0u8; 1024];
            let want = (target - raw.len()).min(b.len());
            match out.read(&mut b[..want]) {
                Ok(0) => {
                    let _ = tx.send(ReadEvent::Failed("partial body".into()));
                    return;
                }
                Ok(n) => raw.extend_from_slice(&b[..n]),
                Err(e) => {
                    let _ = tx.send(ReadEvent::Failed(e.to_string()));
                    return;
                }
            }
        }
        if tx.send(ReadEvent::Frame(raw)).is_err() {
            return;
        }
    }
    let mut extra = [0u8; 1];
    match out.read(&mut extra) {
        Ok(0) => {
            let _ = tx.send(ReadEvent::Eof);
        }
        _ => {
            let _ = tx.send(ReadEvent::Failed("frame count exceeded".into()));
        }
    }
}
fn varint(mut n: usize) -> Vec<u8> {
    let mut v = Vec::new();
    while n >= 128 {
        v.push((n as u8) | 128);
        n >>= 7;
    }
    v.push(n as u8);
    v
}
fn writer_worker(mut input: impl Write, rx: Receiver<Vec<u8>>) -> Result<(), String> {
    while let Ok(raw) = rx.recv() {
        let mut at = 0;
        while at < raw.len() {
            match input.write(&raw[at..]) {
                Ok(0) => return Err("writer zero-byte write".into()),
                Ok(n) => at += n,
                Err(e) => return Err(format!("writer error: {e}")),
            }
        }
    }
    Ok(())
}
fn drain_worker(mut err: impl Read) -> Result<String, String> {
    let mut bytes = Vec::new();
    let mut buf = [0u8; 1024];
    loop {
        match err.read(&mut buf) {
            Ok(0) => return Ok(String::from_utf8_lossy(&bytes).into_owned()),
            Ok(n) => {
                bytes.extend_from_slice(&buf[..n]);
                if bytes.len() > 16384 {
                    return Err("stderr budget".into());
                }
            }
            Err(e) => return Err(format!("stderr read: {e}")),
        }
    }
}
struct Shutdown {
    status: Option<std::process::ExitStatus>,
    stderr: Option<String>,
    problems: Vec<String>,
}
struct Transport {
    child: Option<Child>,
    input: Option<SyncSender<Vec<u8>>>,
    output: Receiver<ReadEvent>,
    reader: Option<JoinHandle<()>>,
    writer: Option<JoinHandle<Result<(), String>>>,
    diagnostic: Option<JoinHandle<Result<String, String>>>,
    started: Instant,
    seen: usize,
    eof: bool,
    settled: bool,
    completed_workers: Arc<AtomicUsize>,
}
impl Transport {
    fn spawn(cmd: Command) -> Result<Self, String> {
        Self::spawn_inner(cmd, None, Arc::new(AtomicUsize::new(0)))
    }
    fn spawn_inner(
        mut cmd: Command,
        fail_after: Option<usize>,
        completed_workers: Arc<AtomicUsize>,
    ) -> Result<Self, String> {
        let child = cmd
            .current_dir(root())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| format!("required peer unavailable: {e}"))?;
        let (ot, output) = mpsc::sync_channel(16);
        let (input, it) = mpsc::sync_channel(4);
        // The guard exists before the first worker spawn. Any later Err drops it,
        // kills/reaps the child, and checks whichever workers already started.
        let mut guard = Self {
            child: Some(child),
            input: Some(input),
            output,
            reader: None,
            writer: None,
            diagnostic: None,
            started: Instant::now(),
            seen: 0,
            eof: false,
            settled: false,
            completed_workers,
        };
        let pid = guard.child.as_ref().unwrap().id();
        if fail_after == Some(0) {
            return Err(format!("injected spawn failure pid {pid}"));
        }
        let stdout = guard.child.as_mut().unwrap().stdout.take().unwrap();
        let stdin = guard.child.as_mut().unwrap().stdin.take().unwrap();
        let stderr = guard.child.as_mut().unwrap().stderr.take().unwrap();
        let reader_done = Arc::clone(&guard.completed_workers);
        guard.reader = Some(
            thread::Builder::new()
                .name("echo stdout".into())
                .spawn(move || {
                    read_worker(stdout, ot);
                    reader_done.fetch_add(1, Ordering::SeqCst);
                })
                .map_err(|e| format!("stdout worker spawn: {e}"))?,
        );
        if fail_after == Some(1) {
            return Err(format!("injected spawn failure pid {pid}"));
        }
        let writer_done = Arc::clone(&guard.completed_workers);
        guard.writer = Some(
            thread::Builder::new()
                .name("echo stdin".into())
                .spawn(move || {
                    let outcome = writer_worker(stdin, it);
                    writer_done.fetch_add(1, Ordering::SeqCst);
                    outcome
                })
                .map_err(|e| format!("stdin worker spawn: {e}"))?,
        );
        if fail_after == Some(2) {
            return Err(format!("injected spawn failure pid {pid}"));
        }
        let diagnostic_done = Arc::clone(&guard.completed_workers);
        guard.diagnostic = Some(
            thread::Builder::new()
                .name("echo stderr".into())
                .spawn(move || {
                    let outcome = drain_worker(stderr);
                    diagnostic_done.fetch_add(1, Ordering::SeqCst);
                    outcome
                })
                .map_err(|e| format!("stderr worker spawn: {e}"))?,
        );
        Ok(guard)
    }
    fn send(&self, raw: Vec<u8>) -> Result<(), String> {
        if raw.len() > 8192 {
            return Err("writer frame budget".into());
        }
        self.input
            .as_ref()
            .ok_or("stdin already closed")?
            .try_send(raw)
            .map_err(|e| format!("writer queue: {e}"))
    }
    fn close_input(&mut self) {
        self.input.take();
    }
    fn recv(&mut self) -> Result<Option<Vec<u8>>, String> {
        let phase_deadline = Instant::now() + PHASE;
        let remaining = WHOLE
            .checked_sub(self.started.elapsed())
            .ok_or("whole-child deadline")?;
        if remaining.is_zero() {
            return Err("whole-child deadline".into());
        }
        let wait = PHASE.min(remaining);
        match self.output.recv_timeout(wait) {
            Ok(ReadEvent::Frame(raw)) => {
                if self.started.elapsed() > WHOLE {
                    return Err("whole-child deadline".into());
                }
                if Instant::now() > phase_deadline {
                    return Err("phase deadline".into());
                }
                self.seen += 1;
                if self.seen > MAX_FRAMES {
                    Err("frame count exceeded".into())
                } else {
                    Ok(Some(raw))
                }
            }
            Ok(ReadEvent::Eof) => {
                if self.started.elapsed() > WHOLE {
                    return Err("whole-child deadline".into());
                }
                if Instant::now() > phase_deadline {
                    return Err("phase deadline".into());
                }
                self.eof = true;
                Ok(None)
            }
            Ok(ReadEvent::Failed(reason)) => Err(format!("bounded stdout: {reason}")),
            Err(mpsc::RecvTimeoutError::Timeout) if self.started.elapsed() >= WHOLE => {
                Err("whole-child deadline".into())
            }
            Err(mpsc::RecvTimeoutError::Timeout) => Err("phase deadline".into()),
            Err(mpsc::RecvTimeoutError::Disconnected) => Err("stdout worker channel closed".into()),
        }
    }
    fn workers_finished(&self) -> bool {
        self.reader.as_ref().is_none_or(JoinHandle::is_finished)
            && self.writer.as_ref().is_none_or(JoinHandle::is_finished)
            && self.diagnostic.as_ref().is_none_or(JoinHandle::is_finished)
    }
    fn shutdown(&mut self, force: bool) -> Shutdown {
        self.input.take();
        let mut problems = Vec::new();
        let whole_deadline = self.started + WHOLE;
        let now = Instant::now();
        let mut deadline = if force {
            whole_deadline.min(now + REAP)
        } else {
            whole_deadline.min(now + PHASE)
        };
        let mut emergency = false;
        if now >= whole_deadline {
            problems.push("whole-child deadline".into());
            deadline = now + REAP;
            emergency = true;
        }
        if force && let Some(child) = self.child.as_mut() {
            let _ = child.kill();
        }
        let mut status = None;
        loop {
            if status.is_none()
                && let Some(child) = self.child.as_mut()
            {
                match child.try_wait() {
                    Ok(found) => status = found,
                    Err(e) => {
                        problems.push(format!("child poll: {e}"));
                        let _ = child.kill();
                    }
                }
            }
            if status.is_some() && self.workers_finished() {
                let finished_at = Instant::now();
                if finished_at > whole_deadline {
                    problems.push("whole-child deadline".into());
                } else if finished_at > deadline {
                    problems.push(if force {
                        "cleanup deadline".into()
                    } else {
                        "phase deadline while finishing child".into()
                    });
                }
                break;
            }
            if Instant::now() >= deadline {
                if emergency {
                    problems.push("cleanup deadline: child or worker did not complete".into());
                    break;
                }
                problems.push(if whole_deadline <= deadline {
                    "whole-child deadline".into()
                } else {
                    "phase deadline while finishing child".into()
                });
                if let Some(child) = self.child.as_mut() {
                    let _ = child.kill();
                }
                deadline = Instant::now() + REAP;
                emergency = true;
            }
            thread::sleep(Duration::from_millis(5));
        }
        if status.is_some() {
            self.child.take();
        }
        if self.reader.as_ref().is_some_and(JoinHandle::is_finished) {
            if self.reader.take().unwrap().join().is_err() {
                problems.push("stdout worker panic".into());
            }
        } else if self.reader.is_some() {
            problems.push("stdout worker incomplete".into());
        }
        if self.writer.as_ref().is_some_and(JoinHandle::is_finished) {
            match self.writer.take().unwrap().join() {
                Ok(Ok(())) => {}
                Ok(Err(e)) => problems.push(e),
                Err(_) => problems.push("stdin worker panic".into()),
            }
        } else if self.writer.is_some() {
            problems.push("stdin worker incomplete".into());
        }
        let mut stderr = None;
        if self
            .diagnostic
            .as_ref()
            .is_some_and(JoinHandle::is_finished)
        {
            match self.diagnostic.take().unwrap().join() {
                Ok(Ok(text)) => stderr = Some(text),
                Ok(Err(e)) => problems.push(e),
                Err(_) => problems.push("stderr worker panic".into()),
            }
        } else if self.diagnostic.is_some() {
            problems.push("stderr worker incomplete".into());
        }
        if status.is_none() {
            problems.push("child unreaped".into());
        }
        Shutdown {
            status,
            stderr,
            problems,
        }
    }
    fn finish(mut self, success: bool) -> Result<(), String> {
        let observation = if self.eof { Ok(None) } else { self.recv() };
        let mut problems = match observation {
            Ok(None) => Vec::new(),
            Ok(Some(_)) => vec!["unexpected later peer frame".into()],
            Err(e) => vec![e],
        };
        let shutdown = self.shutdown(!problems.is_empty());
        self.settled = true;
        problems.extend(shutdown.problems);
        if shutdown
            .status
            .is_some_and(|status| status.success() != success)
        {
            problems.push(format!(
                "peer status: {}",
                shutdown.stderr.unwrap_or_default()
            ));
        }
        if problems.is_empty() {
            Ok(())
        } else {
            Err(problems.join("; "))
        }
    }
}
impl Drop for Transport {
    fn drop(&mut self) {
        if self.settled {
            return;
        }
        let shutdown = self.shutdown(true);
        self.settled = true;
        if !shutdown.problems.is_empty() {
            let message = format!("echo child cleanup: {}", shutdown.problems.join("; "));
            if thread::panicking() {
                eprintln!("{message}");
            } else {
                panic!("{message}");
            }
        }
    }
}
fn peer(language: &str, case: &str, fault: &str) -> Transport {
    let mut cmd = if language == "typescript" {
        let mut c = Command::new("node");
        c.arg("--experimental-strip-types")
            .arg("sdk/typescript/peer.ts");
        c
    } else {
        let mut c = Command::new("python3");
        c.arg("-B").arg("sdk/python/peer.py");
        c
    };
    cmd.args(["--case", case, "--fault", fault]);
    Transport::spawn(cmd).unwrap()
}
fn inspect(case: &str, fault: &str, stage: &str, raw: &[u8]) {
    let mut cmd = Command::new("python3");
    cmd.args([
        "-B",
        "sdk/python/peer.py",
        "--case",
        case,
        "--fault",
        fault,
        "--inspect-wire",
        stage,
    ]);
    let mut transport = Transport::spawn(cmd).unwrap();
    transport.send(raw.to_vec()).unwrap();
    transport.close_input();
    transport.finish(true).unwrap();
}
fn limits() -> v1_1::ProtocolLimits {
    v1_1::ProtocolLimits {
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
        max_stream_credit_bytes: 256,
    }
}
fn authority(pair: &Value, item: &Value) -> HostAuthority {
    let entry = catalog();
    let agent = &entry["agentTool"];
    let desc = AgentTool {
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
    let approved = ApprovedTool::new("peer-a".into(), desc, inputs, JsonLimits::default()).unwrap();
    HostAuthority::new(
        "peer-a".into(),
        item["generation"].as_u64().unwrap(),
        pair["core_minor"].as_u64().unwrap() as u32,
        pair["tool_minor"].as_u64().unwrap() as u32,
        limits(),
        vec![approved],
        vec![],
    )
    .unwrap()
}
fn key(item: &Value) -> v1_1::EffectKey {
    let k = &item["effect_key"];
    v1_1::EffectKey {
        session_id: k["session_id"].as_str().unwrap().into(),
        lane: k["lane"].as_str().unwrap().into(),
        operation_id: k["operation_id"].as_str().unwrap().into(),
        effect_kind: k["effect_kind"].as_str().unwrap().into(),
        step_or_task_id: k["step_or_task_id"].as_str().unwrap().into(),
        logical_attempt: k["logical_attempt"].as_u64().unwrap(),
        item_index: Some(k["item_index"].as_u64().unwrap()),
    }
}
fn arguments(item: &Value, tool: u32) -> SelectedArguments {
    let value = v1_1::WireValue {
        value: Some(v1_1::wire_value::Value::MapValue(v1_1::WireMap {
            entries: vec![v1_1::WireMapEntry {
                key: "text".into(),
                value: Some(v1_1::WireValue {
                    value: Some(v1_1::wire_value::Value::TextValue(
                        item["text"].as_str().unwrap().into(),
                    )),
                }),
            }],
        })),
    };
    let bound = v1_1::SchemaBoundValue {
        projection_id: "tool.arguments:echo".into(),
        schema_digest_sha256: schemas(tool).remove(0).1,
        value: Some(value),
    };
    if tool == 0 {
        SelectedArguments::V1_0(
            v1_0::SchemaBoundValue::decode(bound.encode_to_vec().as_slice()).unwrap(),
        )
    } else {
        SelectedArguments::V1_1(bound)
    }
}
fn decoded_request(raw: &[u8], core: u32) -> v1_1::Request {
    let mut at = 0;
    while raw[at] & 128 != 0 {
        at += 1;
    }
    at += 1;
    let body = &raw[at..];
    if core == 0 {
        let env = v1_0::Envelope::decode(body).unwrap();
        let v1_0::envelope::Frame::Request(req) = env.frame.unwrap() else {
            panic!("request arm")
        };
        v1_1::Request::decode(req.encode_to_vec().as_slice()).unwrap()
    } else {
        let env = v1_1::Envelope::decode(body).unwrap();
        let v1_1::envelope::Frame::Request(req) = env.frame.unwrap() else {
            panic!("request arm")
        };
        req
    }
}
fn assert_request(
    req: &v1_1::Request,
    item: &Value,
    grant: &pi_extension_protocol::activation::ActiveGrant,
    tool: u32,
) {
    assert_eq!(req.request_id, item["request_id"].as_u64().unwrap());
    assert_eq!(req.method, "tool.invoke");
    assert!(req.parent_request_id.is_none() && req.initial_stream_credit_bytes.is_none());
    let expected_key = key(item);
    let actual_key = req.effect_key.as_ref().unwrap();
    assert_eq!(actual_key.session_id, expected_key.session_id);
    assert_eq!(actual_key.lane, expected_key.lane);
    assert_eq!(actual_key.operation_id, expected_key.operation_id);
    assert_eq!(actual_key.effect_kind, expected_key.effect_kind);
    assert_eq!(actual_key.step_or_task_id, expected_key.step_or_task_id);
    assert_eq!(actual_key.logical_attempt, expected_key.logical_attempt);
    assert_eq!(actual_key.item_index, expected_key.item_index);
    assert_eq!(
        req.capability_ref.as_ref().unwrap(),
        grant.tools()[0].reference()
    );
    let inner = v1_1::InvokeToolRequest::decode(req.payload.as_slice()).unwrap();
    assert_eq!(inner.tool_id, "echo");
    assert_eq!(inner.tool_call_id, item["tool_call_id"].as_str().unwrap());
    let arg = inner.arguments.unwrap();
    assert_eq!(arg.projection_id, "tool.arguments:echo");
    assert_eq!(arg.schema_digest_sha256, schemas(tool).remove(0).1);
    let Some(v1_1::wire_value::Value::MapValue(map)) = arg.value.unwrap().value else {
        panic!("argument map")
    };
    assert_eq!(map.entries.len(), 1);
    assert_eq!(map.entries[0].key, "text");
    assert!(
        matches!(map.entries[0].value.as_ref().unwrap().value.as_ref(),Some(v1_1::wire_value::Value::TextValue(s)) if s==item["text"].as_str().unwrap())
    );
}
fn run_case(language: &str, pair: &Value, item: &Value, fault: &str) {
    let case = format!(
        "{}/{}",
        pair["id"].as_str().unwrap(),
        item["id"].as_str().unwrap()
    );
    assert_eq!(item["source_index"], item["effect_key"]["item_index"]);
    let core = pair["core_minor"].as_u64().unwrap() as u32;
    let tool = pair["tool_minor"].as_u64().unwrap() as u32;
    let mut peer = peer(language, &case, fault);
    let mut endpoint = Endpoint::new(authority(pair, item), 100);
    let hello = peer.recv().unwrap().unwrap();
    inspect(&case, fault, "hello", &hello);
    let step = endpoint.read_one(&hello, 101).unwrap();
    let EndpointEvent::Send(ack) = step.event.unwrap() else {
        panic!("hello ack")
    };
    assert_eq!(step.consumed, hello.len());
    assert_eq!(endpoint.phase(), EndpointPhase::AwaitManifest);
    peer.send(ack.bytes).unwrap();
    let manifest = peer.recv().unwrap().unwrap();
    inspect(&case, fault, "manifest", &manifest);
    if fault.starts_with("manifest_") {
        let error = endpoint.read_one(&manifest, 102).unwrap_err();
        assert!(
            matches!(error,EndpointError::Activation(ref a) if a.reason=="required_capability_denied"),
            "{error:?}"
        );
        assert_eq!(endpoint.phase(), EndpointPhase::Closed);
        peer.close_input();
        peer.finish(true).unwrap();
        return;
    }
    let step = endpoint.read_one(&manifest, 102).unwrap();
    let EndpointEvent::Send(grant_frame) = step.event.unwrap() else {
        panic!("grant")
    };
    peer.send(grant_frame.bytes).unwrap();
    let ready = peer.recv().unwrap().unwrap();
    inspect(&case, fault, "ready", &ready);
    let step = endpoint.read_one(&ready, 103).unwrap();
    assert!(matches!(step.event, Some(EndpointEvent::Ready)));
    assert_eq!(endpoint.phase(), EndpointPhase::Active);
    let grant = endpoint.active_grant().unwrap();
    assert_eq!(grant.wire().selected_protocol.as_ref().unwrap().minor, core);
    assert_eq!(grant.tools().len(), 1);
    assert_eq!(
        grant.tools()[0].reference().profile.as_ref().unwrap().minor,
        tool
    );
    let start = Start {
        effect_key: key(item),
        source_call_id: item["tool_call_id"].as_str().unwrap().into(),
        request_id: item["request_id"].as_u64().unwrap(),
        active_generation: item["generation"].as_u64().unwrap(),
        arguments: arguments(item, tool),
        now_ms: 200,
        requested_deadline_after_ms: Some(700),
        method_limit_ms: 1000,
        connection_remaining_ms: 500,
        parent_remaining_ms: Some(300),
        initial_credit_bytes: None,
    };
    let (mut invocation, request_frame) = ToolInvocation::start(grant, "echo", start).unwrap();
    let request = decoded_request(&request_frame.bytes, core);
    assert_request(&request, item, grant, tool);
    assert_eq!(invocation.binding().effect_key(), &key(item));
    assert_eq!(
        invocation.binding().request_id(),
        item["request_id"].as_u64().unwrap()
    );
    assert_eq!(
        invocation.binding().generation(),
        item["generation"].as_u64().unwrap()
    );
    assert_eq!(
        invocation.source_call_id(),
        item["tool_call_id"].as_str().unwrap()
    );
    assert_eq!(invocation.credit(), 0);
    peer.send(request_frame.bytes).unwrap();
    let terminal = peer.recv().unwrap().unwrap();
    inspect(&case, fault, "stream_end", &terminal);
    let step = endpoint.read_one(&terminal, 210).unwrap();
    let Some(EndpointEvent::Active(frame)) = step.event else {
        panic!("active terminal")
    };
    let mut callbacks = 0;
    let outcome =
        invocation.receive_checked(item["generation"].as_u64().unwrap(), &frame, 210, |_| {
            callbacks += 1;
            DomainDecision::Accept
        });
    if fault == "terminal_projection_wrong" || fault == "terminal_digest_wrong" {
        assert!(matches!(
            outcome,
            Err(LiveError::Value(
                pi_extension_protocol::wire::value::Error::ProjectionMismatch
            ))
        ));
        assert_eq!(invocation.phase(), LivePhase::PeerLost);
        assert!(invocation.receipt().is_none());
        assert_eq!(callbacks, 0);
        peer.close_input();
        peer.finish(true).unwrap();
        endpoint.finish().unwrap();
        return;
    }
    assert!(matches!(outcome, Ok(ReceiveOutcome::TerminalReceived)));
    assert_eq!(callbacks, 1);
    assert_eq!(invocation.phase(), LivePhase::TerminalReceived);
    let receipt = invocation.receipt().unwrap();
    let retained_json = receipt.result_json().unwrap().to_vec();
    assert_eq!(receipt.kind(), TerminalKind::Result);
    let receipt_key = receipt.binding().effect_key();
    let expected_key = key(item);
    assert_eq!(receipt_key.session_id, expected_key.session_id);
    assert_eq!(receipt_key.lane, expected_key.lane);
    assert_eq!(receipt_key.operation_id, expected_key.operation_id);
    assert_eq!(receipt_key.effect_kind, expected_key.effect_kind);
    assert_eq!(receipt_key.step_or_task_id, expected_key.step_or_task_id);
    assert_eq!(receipt_key.logical_attempt, expected_key.logical_attempt);
    assert_eq!(receipt_key.item_index, expected_key.item_index);
    assert_eq!(
        receipt.binding().generation(),
        item["generation"].as_u64().unwrap()
    );
    assert_eq!(
        receipt.binding().request_id(),
        item["request_id"].as_u64().unwrap()
    );
    assert!(receipt.binding().same_live_identity(invocation.binding()));
    let json: Value = serde_json::from_slice(receipt.result_json().unwrap()).unwrap();
    assert!(matches_expected_terminal(&json, item));
    let all_inputs = serde_json::from_str::<Value>(FIX).unwrap();
    let opposite = all_inputs["inputs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|other| other["id"] != item["id"])
        .unwrap();
    assert_ne!(
        json["content"][0]["text"], opposite["expected_terminal_text"],
        "a canned result from the opposite input must fail"
    );
    if fault == "duplicate_terminal" {
        let second = peer.recv().unwrap().unwrap();
        assert_eq!(second, terminal);
        inspect(&case, fault, "stream_end", &second);
        let step = endpoint.read_one(&second, 211).unwrap();
        let Some(EndpointEvent::Active(second_frame)) = step.event else {
            panic!("duplicate active")
        };
        assert!(matches!(
            invocation.receive_checked(
                item["generation"].as_u64().unwrap(),
                &second_frame,
                211,
                |_| {
                    callbacks += 1;
                    DomainDecision::Accept
                }
            ),
            Ok(ReceiveOutcome::Stale)
        ));
        assert_eq!(callbacks, 1);
        assert_eq!(
            invocation.receipt().unwrap().result_json().unwrap(),
            retained_json.as_slice()
        );
    }
    peer.close_input();
    peer.finish(true).unwrap();
    endpoint.finish().unwrap();
}
fn matches_expected_terminal(actual: &Value, item: &Value) -> bool {
    actual["content"][0]["text"] == item["expected_terminal_text"]
        && actual["details"] == serde_json::json!({})
}

#[test]
fn a_canned_first_result_fails_the_second_input() {
    let fix: Value = serde_json::from_str(FIX).unwrap();
    let first = &fix["inputs"][0];
    let second = &fix["inputs"][1];
    let canned = serde_json::json!({"content":[{"type":"text","text":first["text"]}],"details":{}});
    assert!(matches_expected_terminal(&canned, first));
    assert!(!matches_expected_terminal(&canned, second));
}

#[test]
fn real_cross_language_echo() {
    prerequisite();
    preflight_derivatives();
    let fix: Value = serde_json::from_str(FIX).unwrap();
    assert_eq!(fix["format"], "s12-echo-transcript/1");
    let inputs = fix["inputs"].as_array().unwrap();
    assert_eq!(inputs.len(), 2);
    assert_ne!(inputs[0]["text"], inputs[1]["text"]);
    for language in ["typescript", "python"] {
        for pair in fix["version_pairs"].as_array().unwrap() {
            for item in inputs {
                run_case(language, pair, item, "normal");
            }
        }
        let pair = &fix["version_pairs"][1];
        let item = &inputs[0];
        for fault in fix["faults"].as_array().unwrap() {
            let id = fault["id"].as_str().unwrap();
            let stage = fault["stage"].as_str().unwrap();
            let expected = if id.starts_with("manifest_") {
                "manifest"
            } else if id == "duplicate_terminal" {
                "duplicate"
            } else {
                "terminal"
            };
            assert_eq!(stage, expected, "fixture fault stage");
            run_case(language, pair, item, id);
        }
    }
}

#[test]
fn failure_path_inspector_nonzero_still_completes_cleanup() {
    let mut cmd = Command::new("python3");
    cmd.args([
        "-B",
        "-c",
        "import sys; sys.stderr.write('bad wire'); sys.exit(2)",
    ]);
    let mut transport = Transport::spawn(cmd).unwrap();
    transport.close_input();
    let completed = Arc::clone(&transport.completed_workers);
    let result = transport.finish(true);
    assert!(format!("{result:?}").contains("peer status"));
    assert_eq!(completed.load(Ordering::SeqCst), 3);
}

#[test]
fn failure_path_stderr_overflow_still_completes_cleanup() {
    let mut cmd = Command::new("python3");
    cmd.args(["-B", "-c", "import sys; sys.stderr.write('x'*17000)"]);
    let mut transport = Transport::spawn(cmd).unwrap();
    transport.close_input();
    let completed = Arc::clone(&transport.completed_workers);
    let result = transport.finish(true);
    assert!(format!("{result:?}").contains("stderr budget"));
    assert_eq!(completed.load(Ordering::SeqCst), 3);
}

#[test]
fn eof_does_not_extend_whole_child_deadline() {
    let mut cmd = Command::new("python3");
    cmd.args(["-B", "-c", "import os,time; os.close(1); time.sleep(1)"]);
    let mut transport = Transport::spawn(cmd).unwrap();
    transport.started = Instant::now() - WHOLE + Duration::from_millis(100);
    transport.close_input();
    let completed = Arc::clone(&transport.completed_workers);
    let before_finish = Instant::now();
    let result = transport.finish(true);
    assert!(
        before_finish.elapsed() < Duration::from_millis(500),
        "EOF extended whole-child deadline"
    );
    assert!(format!("{result:?}").contains("whole-child deadline"));
    assert_eq!(completed.load(Ordering::SeqCst), 3);
}

#[test]
fn phase_timeout_drop_reaps_all_pipe_workers() {
    let mut cmd = Command::new("python3");
    cmd.args(["-B", "-c", "import time; time.sleep(30)"]);
    let mut transport = Transport::spawn(cmd).unwrap();
    let completed = Arc::clone(&transport.completed_workers);
    assert!(transport.recv().unwrap_err().contains("phase deadline"));
    drop(transport);
    assert_eq!(completed.load(Ordering::SeqCst), 3);
}

#[test]
fn unexpected_frame_and_writer_error_are_reported_after_cleanup() {
    let mut extra = Command::new("python3");
    extra.args(["-B", "-c", "import os; os.write(1,bytes([1,8,1,8]))"]);
    let mut transport = Transport::spawn(extra).unwrap();
    let completed = Arc::clone(&transport.completed_workers);
    transport.close_input();
    assert_eq!(transport.recv().unwrap().unwrap(), vec![1, 8]);
    assert!(
        transport
            .finish(true)
            .unwrap_err()
            .contains("unexpected later peer frame")
    );
    assert_eq!(completed.load(Ordering::SeqCst), 3);

    let mut broken = Command::new("python3");
    broken.args([
        "-B",
        "-c",
        "import os,time; os.close(0); os.close(1); time.sleep(0.2)",
    ]);
    let mut transport = Transport::spawn(broken).unwrap();
    let completed = Arc::clone(&transport.completed_workers);
    thread::sleep(Duration::from_millis(50));
    transport.send(vec![0; 8192]).unwrap();
    transport.close_input();
    assert!(transport.finish(true).unwrap_err().contains("writer error"));
    assert_eq!(completed.load(Ordering::SeqCst), 3);
}

#[cfg(target_os = "linux")]
fn process_running(pid: u32) -> bool {
    let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat")) else {
        return false;
    };
    let Some((_, after)) = stat.rsplit_once(") ") else {
        return true;
    };
    !matches!(after.chars().next(), Some('Z' | 'X'))
}

#[cfg(target_os = "linux")]
#[test]
fn worker_spawn_early_return_reaps_child_and_started_workers() {
    for n in 0..=2 {
        let mut cmd = Command::new("python3");
        cmd.args(["-B", "-c", "import time; time.sleep(30)"]);
        let completed = Arc::new(AtomicUsize::new(0));
        let err = Transport::spawn_inner(cmd, Some(n), Arc::clone(&completed))
            .err()
            .unwrap();
        let pid: u32 = err.rsplit_once(' ').unwrap().1.parse().unwrap();
        assert!(!process_running(pid), "spawn cleanup left child {pid}");
        assert_eq!(completed.load(Ordering::SeqCst), n);
    }
}

#[cfg(target_os = "linux")]
#[test]
fn preflight_timeout_terminates_checker_descendants() {
    let path = std::env::temp_dir().join(format!("e1279-child-{}", std::process::id()));
    let mut cmd = Command::new("python3");
    cmd.args(["-B", "-c", "import subprocess,sys,time; p=subprocess.Popen(['sleep','30'],stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL); open(sys.argv[1],'w').write(str(p.pid)); time.sleep(30)"]).arg(&path);
    let error = run_gate(cmd, Duration::from_millis(350)).unwrap_err();
    assert!(error.contains("timeout"), "{error}");
    let pid: u32 = std::fs::read_to_string(&path).unwrap().parse().unwrap();
    let deadline = Instant::now() + REAP;
    while process_running(pid) && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(5));
    }
    let alive = process_running(pid);
    if alive {
        // Avoid leaving the injected descendant behind even when the assertion fails.
        let mut signal = Command::new("/usr/bin/kill")
            .args(["-KILL", &pid.to_string()])
            .spawn()
            .unwrap();
        let _ = reap_until(&mut signal, Instant::now() + REAP);
    }
    let _ = std::fs::remove_file(path);
    assert!(!alive, "timed-out checker descendant remained alive: {pid}");
}

#[test]
fn reader_failure_and_closed_writer_queue_drop_join_workers() {
    let mut malformed = Command::new("python3");
    malformed.args(["-B", "-c", "import os; os.write(1,bytes([128]))"]);
    let mut transport = Transport::spawn(malformed).unwrap();
    let completed = Arc::clone(&transport.completed_workers);
    assert!(transport.recv().unwrap_err().contains("partial prefix"));
    drop(transport);
    assert_eq!(completed.load(Ordering::SeqCst), 3);

    let mut sleeping = Command::new("python3");
    sleeping.args(["-B", "-c", "import time; time.sleep(30)"]);
    let mut transport = Transport::spawn(sleeping).unwrap();
    let completed = Arc::clone(&transport.completed_workers);
    transport.close_input();
    assert!(
        transport
            .send(vec![1])
            .unwrap_err()
            .contains("stdin already closed")
    );
    drop(transport);
    assert_eq!(completed.load(Ordering::SeqCst), 3);
}
