use pi_ai_core::BoxFuture;
use pi_extension_protocol::profiles::wire_v1_1 as dto;
use pi_extension_protocol::wire::{
    framing::{self, EncodedFrame, FrameReader},
    v1_1,
};
use pi_harness_runtime::extensions::transport::{ChannelError, MonotonicClock, OrderedByteChannel};
use prost::Message;
use serde_json::Value;
use std::{
    collections::BTreeMap,
    future::{Future, poll_fn},
    io::{Read, Write},
    num::NonZeroUsize,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        mpsc,
    },
    task::{Poll, Waker},
    time::{Duration, Instant},
};
pub const LIMIT: Duration = Duration::from_secs(15);

/// A bounded source-owned stack probe for the existing child-process worker
/// threads.  The addresses are sampled from actual stack locals at I/O phase
/// boundaries; no configured stack size or process RSS is used.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct StackSnapshot {
    pub workers: usize,
    pub samples: usize,
    pub minimum_address: usize,
    pub maximum_address: usize,
    pub span_bytes: usize,
    pub worker_names: Vec<String>,
    pub worker_snapshots: Vec<WorkerStackSnapshot>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WorkerStackSnapshot {
    pub worker: String,
    pub samples: usize,
    pub minimum_address: usize,
    pub maximum_address: usize,
    pub span_bytes: usize,
}

#[derive(Default)]
struct StackProbe {
    workers: Mutex<BTreeMap<&'static str, WorkerStack>>,
}

#[derive(Default)]
struct WorkerStack {
    samples: usize,
    minimum_address: usize,
    maximum_address: usize,
}

impl StackProbe {
    fn sample(&self, worker: &'static str, address: usize) {
        let mut workers = self.workers.lock().unwrap();
        let state = workers.entry(worker).or_default();
        state.samples += 1;
        if state.minimum_address == 0 || address < state.minimum_address {
            state.minimum_address = address;
        }
        state.maximum_address = state.maximum_address.max(address);
    }

    fn snapshot(&self) -> StackSnapshot {
        let workers = self.workers.lock().unwrap();
        let mut minimum_address = usize::MAX;
        let mut maximum_address = 0;
        let mut samples = 0;
        let worker_snapshots = workers
            .iter()
            .map(|(name, worker)| WorkerStackSnapshot {
                worker: (*name).to_owned(),
                samples: worker.samples,
                minimum_address: worker.minimum_address,
                maximum_address: worker.maximum_address,
                span_bytes: worker
                    .maximum_address
                    .saturating_sub(worker.minimum_address),
            })
            .collect::<Vec<_>>();
        for worker in workers.values() {
            samples += worker.samples;
            minimum_address = minimum_address.min(worker.minimum_address);
            maximum_address = maximum_address.max(worker.maximum_address);
        }
        if workers.is_empty() {
            minimum_address = 0;
        }
        StackSnapshot {
            workers: workers.len(),
            samples,
            minimum_address,
            maximum_address,
            span_bytes: maximum_address.saturating_sub(minimum_address),
            worker_names: workers.keys().map(|name| (*name).to_owned()).collect(),
            worker_snapshots,
        }
    }
}

/// Keep the sample in a real worker frame and descend a bounded number of
/// frames.  This is a stack-address probe, not a synthetic stack-size value.
#[inline(never)]
fn sample_worker_stack(probe: &StackProbe, worker: &'static str, depth: u8) {
    let marker = [0_u8; 128];
    std::hint::black_box(&marker);
    probe.sample(worker, marker.as_ptr() as usize);
    if depth > 0 {
        sample_worker_stack(probe, worker, depth - 1);
    }
    std::hint::black_box(marker[0]);
}

static LAST_STACK_SNAPSHOT: OnceLock<Mutex<Option<StackSnapshot>>> = OnceLock::new();

fn publish_stack_snapshot(snapshot: StackSnapshot) {
    *LAST_STACK_SNAPSHOT
        .get_or_init(|| Mutex::new(None))
        .lock()
        .unwrap() = Some(snapshot);
}

/// Return the most recently completed child-process worker stack observation.
pub fn last_stack_snapshot() -> Option<StackSnapshot> {
    LAST_STACK_SNAPSHOT
        .get()
        .and_then(|value| value.lock().unwrap().clone())
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TransportSnapshot {
    pub language: String,
    pub core: u32,
    pub minor: u32,
    pub command: String,
    pub read_capacity: usize,
    pub read_peak: usize,
    pub write_capacity: usize,
    pub write_peak: usize,
    pub topology: Vec<String>,
}

static LAST_TRANSPORT_SNAPSHOT: OnceLock<Mutex<Option<TransportSnapshot>>> = OnceLock::new();

fn publish_transport_snapshot(snapshot: TransportSnapshot) {
    *LAST_TRANSPORT_SNAPSHOT
        .get_or_init(|| Mutex::new(None))
        .lock()
        .unwrap() = Some(snapshot);
}

/// Return actual occupancy peaks from the most recently completed process
/// channel. Configured channel capacity is retained alongside observed
/// occupancy so a declaration cannot stand in for a peak.
pub fn last_transport_snapshot() -> Option<TransportSnapshot> {
    LAST_TRANSPORT_SNAPSHOT
        .get()
        .and_then(|value| value.lock().unwrap().clone())
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct CpuMeasurement {
    pub clock_hz: u64,
    pub start_cpu_ns: u128,
    pub end_cpu_ns: u128,
    pub cpu_ns: u128,
    pub wall_ns: u128,
    pub budget_ns: u128,
    pub headroom_ns: u128,
}

/// A process CPU-clock sample paired with the existing finite worker deadline.
pub struct CpuProbe {
    start_cpu_ns: u128,
    started: Instant,
    budget_ns: u128,
}

impl CpuProbe {
    pub fn start() -> Self {
        Self {
            start_cpu_ns: process_cpu_ns(),
            started: Instant::now(),
            budget_ns: LIMIT.as_nanos(),
        }
    }

    pub fn finish(self) -> CpuMeasurement {
        let end_cpu_ns = process_cpu_ns();
        let wall_ns = self.started.elapsed().as_nanos();
        let cpu_ns = end_cpu_ns.saturating_sub(self.start_cpu_ns);
        CpuMeasurement {
            clock_hz: cpu_clock_hz(),
            start_cpu_ns: self.start_cpu_ns,
            end_cpu_ns,
            cpu_ns,
            wall_ns,
            budget_ns: self.budget_ns,
            headroom_ns: self.budget_ns.saturating_sub(cpu_ns),
        }
    }
}

static CPU_CLOCK_HZ: OnceLock<u64> = OnceLock::new();

fn cpu_clock_hz() -> u64 {
    *CPU_CLOCK_HZ.get_or_init(|| {
        let output = Command::new("getconf")
            .arg("CLK_TCK")
            .output()
            .expect("host getconf for process CPU clock");
        assert!(
            output.status.success(),
            "host getconf CLK_TCK failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout)
            .expect("getconf CLK_TCK is UTF-8")
            .trim()
            .parse()
            .expect("getconf CLK_TCK is an integer")
    })
}

fn process_cpu_ticks() -> u64 {
    let stat = std::fs::read_to_string("/proc/self/stat").expect("process CPU clock stat");
    let fields = stat
        .rsplit_once(')')
        .expect("process CPU clock stat has comm field")
        .1
        .split_whitespace()
        .collect::<Vec<_>>();
    let user = fields
        .get(11)
        .expect("process CPU clock has utime")
        .parse::<u64>()
        .expect("process CPU utime is an integer");
    let system = fields
        .get(12)
        .expect("process CPU clock has stime")
        .parse::<u64>()
        .expect("process CPU stime is an integer");
    user.saturating_add(system)
}

fn process_cpu_ns() -> u128 {
    let ticks = process_cpu_ticks() as u128;
    ticks.saturating_mul(1_000_000_000) / cpu_clock_hz() as u128
}

fn nz(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap()
}
pub fn root() -> PathBuf {
    std::env::var_os("PI_PROFILE_PEER_REPO")
        .map(PathBuf::from)
        .unwrap_or_else(|| Path::new(env!("CARGO_MANIFEST_DIR")).join("../.."))
}
pub fn drive<F: Future>(
    setup: &super::super::runtime_r1_r4_support::Setup,
    future: F,
) -> F::Output {
    let mut future = std::pin::pin!(future);
    let deadline = Instant::now() + LIMIT;
    loop {
        if let Poll::Ready(result) = super::super::runtime_r1_r4_support::poll(future.as_mut()) {
            return result;
        }
        setup.pump.tick();
        assert!(
            Instant::now() < deadline,
            "finite native process completion"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}
pub struct RealClock(Instant);
impl Default for RealClock {
    fn default() -> Self {
        Self(Instant::now())
    }
}
impl MonotonicClock for RealClock {
    fn now_ms(&self) -> u64 {
        self.0.elapsed().as_millis() as u64
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
fn join_worker(worker: std::thread::JoinHandle<()>) {
    let deadline = Instant::now() + LIMIT;
    while !worker.is_finished() {
        assert!(Instant::now() < deadline, "finite process worker join");
        std::thread::sleep(Duration::from_millis(1));
    }
    worker.join().unwrap();
}
fn verify_inputs() {
    let files = [
        (
            "sdk/typescript/profile-peer.ts",
            "ae7c79933bbc474692da34c948aa54a1dd9243358229a87b5e4a528c6a056440",
        ),
        (
            "sdk/typescript/profiles/wire.ts",
            "b16ca93b977c550499cb0f2397b3cd3ed34684a58961e40f407e2a6394093366",
        ),
        (
            "sdk/typescript/profiles/shapes.ts",
            "33e79bcc760031e842821b6371883ec75adf45f57f3ba0dbca6df4f06978f440",
        ),
        (
            "sdk/python/profile_peer.py",
            "02be004c6cc76c4fb4e636c22089a63fda27f5e07ea96b90f5d0115fed3f5419",
        ),
        (
            "sdk/python/profiles/wire.py",
            "4065df088d15bf674ebf5991f647767caf7152029660ffba686d4407b14ad392",
        ),
        (
            "sdk/python/profiles/shapes.py",
            "4fb125a5065f937009abdb8ea931e26304244da28fdad7b2aee91c2c83779fb5",
        ),
        (
            "conformance/extension-protocol/profiles-runtime-v1.json",
            "6612858802bb0a00db60eced0aa6046ccf74a8990f45f669a26bb221413f520c",
        ),
    ];
    for (file, hash) in files {
        let output = Command::new("sha256sum")
            .arg(root().join(file))
            .output()
            .unwrap();
        assert!(output.status.success());
        assert_eq!(
            String::from_utf8(output.stdout)
                .unwrap()
                .split_whitespace()
                .next(),
            Some(hash),
            "accepted source pin {file}"
        );
    }
}
type Reply = Arc<Mutex<(Option<Result<(), ChannelError>>, Option<Waker>)>>;
pub struct Channel {
    child: Mutex<Option<Child>>,
    closed: Arc<AtomicBool>,
    read: Mutex<mpsc::Receiver<Vec<u8>>>,
    read_wake: Arc<Mutex<Option<Waker>>>,
    write: mpsc::SyncSender<(Vec<u8>, Reply)>,
    writes: Arc<Mutex<Vec<Vec<u8>>>>,
    incoming: Arc<Mutex<Vec<u8>>>,
    stderr: Arc<Mutex<Vec<u8>>>,
    done: Mutex<mpsc::Receiver<()>>,
    workers: Mutex<Vec<std::thread::JoinHandle<()>>>,
    stack: Arc<StackProbe>,
    release: PathBuf,
    corrupt_lane: AtomicBool,
    fail_cancel_write: AtomicBool,
    read_queued: Arc<AtomicUsize>,
    read_peak: Arc<AtomicUsize>,
    write_queued: Arc<AtomicUsize>,
    write_peak: Arc<AtomicUsize>,
    language: String,
    core: u32,
    minor: u32,
    command: String,
    topology: Arc<Mutex<Vec<String>>>,
}

/// A process-backed channel that turns the first Cancel write into a real
/// held future and publishes clean EOF to the reader while that future is
/// pending. It is used only by the physical cancellation boundary test.
pub struct HeldCancelChannel {
    inner: Arc<Channel>,
    expected_cancel_id: AtomicU64,
    gate_claimed: AtomicBool,
    released: AtomicBool,
    release_permits: AtomicUsize,
    eof: AtomicBool,
    eof_read: AtomicBool,
    closes: AtomicUsize,
    early_close: AtomicBool,
    cancel_complete: AtomicBool,
    pending_cancel: AtomicUsize,
    cancel_count: AtomicUsize,
    cancel_id: Mutex<Option<u64>>,
    cancel_bytes: Mutex<Option<Vec<u8>>>,
    write_wake: Mutex<Option<Waker>>,
    read_wake: Mutex<Option<Waker>>,
}

impl HeldCancelChannel {
    pub fn new(inner: Arc<Channel>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            expected_cancel_id: AtomicU64::new(0),
            gate_claimed: AtomicBool::new(false),
            released: AtomicBool::new(false),
            release_permits: AtomicUsize::new(0),
            eof: AtomicBool::new(false),
            eof_read: AtomicBool::new(false),
            closes: AtomicUsize::new(0),
            early_close: AtomicBool::new(false),
            cancel_complete: AtomicBool::new(false),
            pending_cancel: AtomicUsize::new(0),
            cancel_count: AtomicUsize::new(0),
            cancel_id: Mutex::new(None),
            cancel_bytes: Mutex::new(None),
            write_wake: Mutex::new(None),
            read_wake: Mutex::new(None),
        })
    }

    pub fn set_expected_cancel_id(&self, id: u64) {
        self.expected_cancel_id.store(id, Ordering::SeqCst);
    }

    pub fn cancel_id(&self) -> Option<u64> {
        *self.cancel_id.lock().unwrap()
    }

    pub fn cancel_count(&self) -> usize {
        self.cancel_count.load(Ordering::SeqCst)
    }

    pub fn cancel_bytes(&self) -> Option<Vec<u8>> {
        self.cancel_bytes.lock().unwrap().clone()
    }

    pub fn close_count(&self) -> usize {
        self.closes.load(Ordering::SeqCst)
    }

    pub fn early_close(&self) -> bool {
        self.early_close.load(Ordering::SeqCst)
    }

    pub fn cancel_complete(&self) -> bool {
        self.cancel_complete.load(Ordering::SeqCst)
    }

    pub fn reader_eof_published(&self) -> bool {
        self.eof.load(Ordering::SeqCst)
    }

    pub fn reader_eof_read(&self) -> bool {
        self.eof_read.load(Ordering::SeqCst)
    }

    pub fn release(&self) {
        self.released.store(true, Ordering::SeqCst);
        self.release_permits.fetch_add(1, Ordering::SeqCst);
        if let Some(waker) = self.write_wake.lock().unwrap().take() {
            waker.wake();
        }
    }
}
impl Channel {
    pub fn spawn(language: &str, core: u32, minor: u32) -> Arc<Self> {
        Self::spawn_scenario(language, core, minor, "none")
    }
    pub fn spawn_scenario(language: &str, core: u32, minor: u32, scenario: &str) -> Arc<Self> {
        verify_inputs();
        let release = std::env::temp_dir().join(format!(
            "profile-peer1412-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        let mut command = if language == "typescript" {
            let mut c = Command::new("node");
            c.args([
                "--experimental-strip-types",
                "sdk/typescript/profile-peer.ts",
            ]);
            c
        } else {
            let mut c = Command::new("python3");
            c.args(["-B", "sdk/python/profile_peer.py"]);
            c
        };
        let repo = root();
        let command_line = if language == "typescript" {
            format!(
                "node --experimental-strip-types sdk/typescript/profile-peer.ts --serve {} {} {} {}",
                repo.display(),
                core,
                minor,
                scenario
            )
        } else {
            format!(
                "python3 -B sdk/python/profile_peer.py --serve {} {} {} {}",
                repo.display(),
                core,
                minor,
                scenario
            )
        };
        command
            .current_dir(&repo)
            .env_clear()
            .env("PATH", "/usr/local/bin:/usr/bin:/bin")
            .env("LANG", "C.UTF-8")
            .env("PI_PROFILE_PEER_SIGNAL", &release)
            .arg("--serve")
            .arg(&repo)
            .arg(core.to_string())
            .arg(minor.to_string())
            .arg(scenario);
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
        let incoming = Arc::new(Mutex::new(vec![]));
        let stderr = Arc::new(Mutex::new(vec![]));
        let (read_tx, read_rx) = mpsc::sync_channel(1);
        let (write_tx, write_rx) = mpsc::sync_channel::<(Vec<u8>, Reply)>(1);
        let (done_tx, done_rx) = mpsc::channel();
        let mut workers = vec![];
        let stack = Arc::new(StackProbe::default());
        let read_queued = Arc::new(AtomicUsize::new(0));
        let read_peak = Arc::new(AtomicUsize::new(0));
        let write_queued = Arc::new(AtomicUsize::new(0));
        let write_peak = Arc::new(AtomicUsize::new(0));
        let topology = Arc::new(Mutex::new(vec![
            "ChildTransport/read".into(),
            "ChildTransport/write".into(),
            "ChildTransport/stderr".into(),
        ]));
        let (stop, wake, done, captured) = (
            closed.clone(),
            read_wake.clone(),
            done_tx.clone(),
            incoming.clone(),
        );
        let stack_probe = stack.clone();
        let read_queued_worker = read_queued.clone();
        let read_peak_worker = read_peak.clone();
        let topology_worker = topology.clone();
        workers.push(std::thread::spawn(move || {
            let mut total = 0;
            loop {
                sample_worker_stack(&stack_probe, "stdout", 2);
                let mut buffer = [0u8; 4096];
                let count = output.read(&mut buffer).unwrap_or(0);
                sample_worker_stack(&stack_probe, "stdout", 2);
                total += count;
                assert!(total <= 1048576, "finite peer stdout budget");
                captured.lock().unwrap().extend_from_slice(&buffer[..count]);
                let mut data = buffer[..count].to_vec();
                loop {
                    let queued = read_queued_worker.fetch_add(1, Ordering::SeqCst) + 1;
                    read_peak_worker.fetch_max(queued, Ordering::SeqCst);
                    match read_tx.try_send(data) {
                        Ok(()) => {
                            read_peak_worker.fetch_max(1, Ordering::SeqCst);
                            topology_worker
                                .lock()
                                .unwrap()
                                .push("ChildTransport/read/enqueue".into());
                            break;
                        }
                        Err(mpsc::TrySendError::Full(value)) => {
                            read_queued_worker.fetch_sub(1, Ordering::SeqCst);
                            data = value;
                            if stop.load(Ordering::SeqCst) {
                                let _ = done.send(());
                                return;
                            }
                            std::thread::sleep(Duration::from_millis(1));
                        }
                        Err(mpsc::TrySendError::Disconnected(_)) => {
                            read_queued_worker.fetch_sub(1, Ordering::SeqCst);
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
        let stack_probe = stack.clone();
        let write_queued_worker = write_queued.clone();
        let topology_worker = topology.clone();
        workers.push(std::thread::spawn(move || {
            while !stop.load(Ordering::SeqCst) {
                sample_worker_stack(&stack_probe, "stdin", 2);
                match write_rx.recv_timeout(Duration::from_millis(20)) {
                    Ok((data, reply)) => {
                        topology_worker
                            .lock()
                            .unwrap()
                            .push("ChildTransport/write/pop".into());
                        write_queued_worker.fetch_sub(1, Ordering::SeqCst);
                        let result = input.write_all(&data).map_err(|_| ChannelError::Failed);
                        sample_worker_stack(&stack_probe, "stdin", 2);
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
        let stack_probe = stack.clone();
        workers.push(std::thread::spawn(move || {
            let mut buffer = [0u8; 1024];
            loop {
                sample_worker_stack(&stack_probe, "stderr", 2);
                let n = errors.read(&mut buffer).unwrap_or(0);
                sample_worker_stack(&stack_probe, "stderr", 2);
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
            incoming,
            stderr,
            done: Mutex::new(done_rx),
            workers: Mutex::new(workers),
            stack,
            release,
            corrupt_lane: AtomicBool::new(false),
            fail_cancel_write: AtomicBool::new(false),
            read_queued,
            read_peak,
            write_queued,
            write_peak,
            language: language.to_owned(),
            core,
            minor,
            command: command_line,
            topology,
        })
    }
    pub fn spawn_cancel_write_failure(language: &str, core: u32, minor: u32) -> Arc<Self> {
        let peer = Self::spawn_scenario(language, core, minor, "runtime-native-host-abort-current");
        peer.fail_cancel_write.store(true, Ordering::SeqCst);
        peer
    }
    pub fn spawn_invalid_lane(language: &str, core: u32, minor: u32) -> Arc<Self> {
        let peer = Self::spawn_scenario(language, core, minor, "runtime-native");
        peer.corrupt_lane.store(true, Ordering::SeqCst);
        peer
    }
    pub fn stderr_text(&self) -> String {
        String::from_utf8_lossy(&self.stderr.lock().unwrap()).into_owned()
    }
    pub fn snapshot_result_round_trip_diagnostic(&self) -> String {
        let body = self
            .writes
            .lock()
            .unwrap()
            .iter()
            .filter_map(|bytes| {
                let mut reader = FrameReader::<v1_1::Envelope>::new(nz(262144));
                let envelope = reader.read_one(bytes).ok()?.frame?.envelope;
                match envelope.frame {
                    Some(v1_1::envelope::Frame::Response(response))
                        if response.request_id == 1000 =>
                    {
                        match response.terminal {
                            Some(v1_1::response::Terminal::Result(body)) => Some(body),
                            _ => None,
                        }
                    }
                    _ => None,
                }
            })
            .next();
        let Some(body) = body else {
            return "snapshot_result_round_trip_bytes=unavailable".into();
        };
        let input_hex = body
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let rust_hex = match dto::SnapshotGetResult::decode(body.as_slice()) {
            Ok(result) => result
                .encode_to_vec()
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>(),
            Err(error) => format!("decode-error:{error}"),
        };
        let node_hex = if self.language == "typescript" {
            let script = r#"
import { pathToFileURL } from "node:url";
const [repo, minorText, inputHex] = process.argv.slice(1);
const { Codec } = await import(pathToFileURL(`${repo}/sdk/typescript/profiles/wire.ts`).href);
function sortedMaps(obj) {
  if (Array.isArray(obj)) return obj.map(sortedMaps);
  if (!obj || typeof obj !== "object" || Buffer.isBuffer(obj)) return obj;
  const out = {};
  for (const [key, value] of Object.entries(obj)) out[key] = sortedMaps(value);
  if (out.map_value)
    out.map_value.entries.sort((a, b) => a.key < b.key ? -1 : a.key > b.key ? 1 : 0);
  return out;
}
const codec = new Codec(repo, Number(minorText));
const decoded = codec.decode("SnapshotGetResult", Buffer.from(inputHex, "hex"));
process.stdout.write(codec.encode("SnapshotGetResult", sortedMaps(decoded)).toString("hex"));
"#;
            let output = Command::new("node")
                .args([
                    "--experimental-strip-types",
                    "--input-type=module",
                    "-e",
                    script,
                ])
                .arg(root())
                .arg(self.minor.to_string())
                .arg(&input_hex)
                .output();
            match output {
                Ok(output) if output.status.success() => {
                    String::from_utf8_lossy(&output.stdout).trim().to_owned()
                }
                Ok(output) => format!(
                    "node-error:status={};stderr={}",
                    output.status,
                    String::from_utf8_lossy(&output.stderr).trim()
                ),
                Err(error) => format!("node-error:{error}"),
            }
        } else {
            "not-run-language-is-not-typescript".into()
        };
        format!(
            "snapshot_result_input_hex={input_hex}; \
             snapshot_result_rust_roundtrip_hex={rust_hex}; \
             snapshot_result_typescript_roundtrip_hex={node_hex}"
        )
    }
    pub fn signal(&self, phase: &str) {
        assert!(matches!(
            phase,
            "actions"
                | "provider"
                | "resource-changed"
                | "resource-second"
                | "queue-denials"
                | "queue-wrong-lane"
                | "inbox-follow-up"
                | "inbox-denials"
                | "inbox-wrong-lane"
                | "event-subscribe"
                | "event-credit"
                | "event-resync"
                | "tool"
        ));
        std::fs::write(&self.release, phase).unwrap();
    }
    pub fn stage(&self, stage: &str) -> Option<Value> {
        String::from_utf8_lossy(&self.stderr.lock().unwrap())
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .find(|record| record["stage"] == stage)
    }
    fn request(&self) -> Option<v1_1::Request> {
        self.writes.lock().unwrap().iter().find_map(|bytes| {
            let mut reader = FrameReader::<v1_1::Envelope>::new(nz(4096));
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
    pub fn requests(&self) -> usize {
        self.writes
            .lock()
            .unwrap()
            .iter()
            .filter(|bytes| {
                let mut reader = FrameReader::<v1_1::Envelope>::new(nz(4096));
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
    /// Decode every actual host-to-peer Request retained by this process
    /// channel, preserving wire order for same-connection native assertions.
    pub fn request_frames(&self) -> Vec<v1_1::Request> {
        self.writes
            .lock()
            .unwrap()
            .iter()
            .filter_map(|bytes| {
                let mut reader = FrameReader::<v1_1::Envelope>::new(nz(4096));
                let envelope = reader.read_one(bytes).unwrap().frame.unwrap().envelope;
                match envelope.frame {
                    Some(v1_1::envelope::Frame::Request(request)) => Some(request),
                    _ => None,
                }
            })
            .collect()
    }
    pub fn response_frames(&self) -> Vec<v1_1::Response> {
        self.writes
            .lock()
            .unwrap()
            .iter()
            .filter_map(|bytes| {
                let mut reader = FrameReader::<v1_1::Envelope>::new(nz(4096));
                let envelope = reader.read_one(bytes).unwrap().frame.unwrap().envelope;
                match envelope.frame {
                    Some(v1_1::envelope::Frame::Response(response)) => Some(response),
                    _ => None,
                }
            })
            .collect()
    }
    pub fn action_control_order(&self, action_id: u64, parent_id: u64) -> Vec<String> {
        self.writes
            .lock()
            .unwrap()
            .iter()
            .filter_map(|bytes| {
                let mut reader = FrameReader::<v1_1::Envelope>::new(nz(4096));
                let envelope = reader.read_one(bytes).unwrap().frame.unwrap().envelope;
                match envelope.frame {
                    Some(v1_1::envelope::Frame::Response(response))
                        if response.request_id == action_id =>
                    {
                        Some(format!("action-response:{:?}", response.terminal))
                    }
                    Some(v1_1::envelope::Frame::Cancel(cancel))
                        if cancel.request_id == parent_id =>
                    {
                        Some(format!("parent-cancel:{}", cancel.reason))
                    }
                    _ => None,
                }
            })
            .collect()
    }
    pub fn stream_item_frames(&self) -> Vec<v1_1::StreamItem> {
        self.writes
            .lock()
            .unwrap()
            .iter()
            .filter_map(|bytes| {
                let mut reader = FrameReader::<v1_1::Envelope>::new(nz(4096));
                let envelope = reader.read_one(bytes).unwrap().frame.unwrap().envelope;
                match envelope.frame {
                    Some(v1_1::envelope::Frame::StreamItem(item)) => Some(item),
                    _ => None,
                }
            })
            .collect()
    }
    pub fn peer_credit_frames(&self) -> Vec<v1_1::Credit> {
        let bytes = self.incoming.lock().unwrap().clone();
        let mut offset = 0;
        let mut reader = FrameReader::<v1_1::Envelope>::new(nz(262144));
        let mut credits = Vec::new();
        while offset < bytes.len() {
            let step = reader.read_one(&bytes[offset..]).unwrap();
            assert!(step.consumed > 0, "captured peer frame must advance");
            offset += step.consumed;
            if let Some(frame) = step.frame
                && let Some(v1_1::envelope::Frame::Credit(credit)) = frame.envelope.frame
            {
                credits.push(credit);
            }
        }
        credits
    }
    pub fn peer_request_frames(&self) -> Vec<v1_1::Request> {
        let bytes = self.incoming.lock().unwrap().clone();
        let mut offset = 0;
        let mut reader = FrameReader::<v1_1::Envelope>::new(nz(262144));
        let mut requests = Vec::new();
        while offset < bytes.len() {
            let step = reader.read_one(&bytes[offset..]).unwrap();
            assert!(step.consumed > 0, "captured peer frame must advance");
            offset += step.consumed;
            if let Some(frame) = step.frame
                && let Some(v1_1::envelope::Frame::Request(request)) = frame.envelope.frame
            {
                requests.push(request);
            }
        }
        requests
    }
    pub fn peer_response_frames(&self) -> Vec<v1_1::Response> {
        let bytes = self.incoming.lock().unwrap().clone();
        let mut offset = 0;
        let mut reader = FrameReader::<v1_1::Envelope>::new(nz(262144));
        let mut responses = Vec::new();
        while offset < bytes.len() {
            let step = reader.read_one(&bytes[offset..]).unwrap();
            assert!(step.consumed > 0, "captured peer frame must advance");
            offset += step.consumed;
            if let Some(frame) = step.frame
                && let Some(v1_1::envelope::Frame::Response(response)) = frame.envelope.frame
            {
                responses.push(response);
            }
        }
        responses
    }
    pub fn peer_stream_end_frames(&self) -> Vec<v1_1::StreamEnd> {
        let bytes = self.incoming.lock().unwrap().clone();
        let mut offset = 0;
        let mut reader = FrameReader::<v1_1::Envelope>::new(nz(262144));
        let mut ends = Vec::new();
        while offset < bytes.len() {
            let step = reader.read_one(&bytes[offset..]).unwrap();
            assert!(step.consumed > 0, "captured peer frame must advance");
            offset += step.consumed;
            if let Some(frame) = step.frame
                && let Some(v1_1::envelope::Frame::StreamEnd(end)) = frame.envelope.frame
            {
                ends.push(end);
            }
        }
        ends
    }
    fn publish_transport_snapshot(&self) {
        let topology = self.topology.lock().unwrap().clone();
        publish_transport_snapshot(TransportSnapshot {
            language: self.language.clone(),
            core: self.core,
            minor: self.minor,
            command: self.command.clone(),
            read_capacity: 1,
            read_peak: self.read_peak.load(Ordering::SeqCst),
            write_capacity: 1,
            write_peak: self.write_peak.load(Ordering::SeqCst),
            topology,
        });
    }
    pub fn finish(&self, success: bool) {
        self.closed.store(true, Ordering::SeqCst);
        let deadline = Instant::now() + LIMIT;
        let status = loop {
            let status = self
                .child
                .lock()
                .unwrap()
                .as_mut()
                .and_then(|child| child.try_wait().unwrap());
            if let Some(status) = status {
                self.topology
                    .lock()
                    .unwrap()
                    .push("PeerProcess/terminal".into());
                break status;
            }
            assert!(Instant::now() < deadline, "finite peer process wait");
            std::thread::sleep(Duration::from_millis(1));
        };
        if success {
            assert!(
                status.success(),
                "actor failed ({status}): {}",
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
            self.topology
                .lock()
                .unwrap()
                .push("PeerProcess/worker_join".into());
        }
        self.topology
            .lock()
            .unwrap()
            .push("PeerProcess/cleanup".into());
        publish_stack_snapshot(self.stack.snapshot());
        self.publish_transport_snapshot();
        let records = String::from_utf8(self.stderr.lock().unwrap().clone()).unwrap();
        println!("E1412_PROCESS {}", records.trim());
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
                    self.topology
                        .lock()
                        .unwrap()
                        .push("PeerProcess/terminal".into());
                    break;
                }
                if Instant::now() >= deadline {
                    panic!("peer reap timeout");
                }
                std::thread::sleep(Duration::from_millis(1));
            }
        }
        let mut joined = false;
        for worker in self.workers.lock().unwrap().drain(..) {
            join_worker(worker);
            joined = true;
            self.topology
                .lock()
                .unwrap()
                .push("PeerProcess/worker_join".into());
        }
        if joined {
            self.topology
                .lock()
                .unwrap()
                .push("PeerProcess/cleanup".into());
        }
        publish_stack_snapshot(self.stack.snapshot());
        self.publish_transport_snapshot();
        if self.release.exists() {
            std::fs::remove_file(&self.release).unwrap();
        }
    }
}
impl OrderedByteChannel for Channel {
    fn max_read_scratch_bytes(&self) -> usize {
        4096
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
                    self.topology
                        .lock()
                        .unwrap()
                        .push("ChildTransport/read/pop".into());
                    self.read_queued.fetch_sub(1, Ordering::SeqCst);
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
        let mut bytes = frame.bytes;
        let cancel = {
            let mut reader = FrameReader::<v1_1::Envelope>::new(nz(262144));
            matches!(
                reader
                    .read_one(&bytes)
                    .ok()
                    .and_then(|step| step.frame)
                    .and_then(|frame| frame.envelope.frame),
                Some(v1_1::envelope::Frame::Cancel(_))
            )
        };
        if cancel && self.fail_cancel_write.load(Ordering::SeqCst) {
            return Box::pin(std::future::ready(Err(ChannelError::Failed)));
        }
        if self.corrupt_lane.load(Ordering::SeqCst) {
            let mut reader = FrameReader::<v1_1::Envelope>::new(nz(262144));
            let mut envelope = reader.read_one(&bytes).unwrap().frame.unwrap().envelope;
            if let Some(v1_1::envelope::Frame::Request(request)) = envelope.frame.as_mut()
                && request.method == "before_run"
            {
                let mut hook = dto::InvokeHookRequest::decode(request.payload.as_slice()).unwrap();
                fn child<'a>(value: &'a mut v1_1::WireValue, key: &str) -> &'a mut v1_1::WireValue {
                    let Some(v1_1::wire_value::Value::MapValue(map)) = value.value.as_mut() else {
                        panic!("expected projected map")
                    };
                    map.entries
                        .iter_mut()
                        .find(|entry| entry.key == key)
                        .unwrap()
                        .value
                        .as_mut()
                        .unwrap()
                }
                let root = hook.input.as_mut().unwrap().value.as_mut().unwrap();
                let lane = child(child(root, "event"), "lane");
                lane.value = Some(v1_1::wire_value::Value::TextValue("wrong-lane".into()));
                request.payload = hook.encode_to_vec();
                bytes = framing::encode(&envelope, nz(262144)).unwrap().bytes;
            }
        }
        let mut data = Some(bytes);
        Box::pin(poll_fn(move |cx| {
            if self.closed.load(Ordering::SeqCst) {
                return Poll::Ready(Err(ChannelError::Closed));
            }
            if let Some(bytes) = data.take() {
                let queued = self.write_queued.fetch_add(1, Ordering::SeqCst) + 1;
                self.write_peak.fetch_max(queued, Ordering::SeqCst);
                match self.write.try_send((bytes, reply.clone())) {
                    Ok(()) => {}
                    Err(mpsc::TrySendError::Full((bytes, _))) => {
                        self.write_queued.fetch_sub(1, Ordering::SeqCst);
                        data = Some(bytes);
                        return Poll::Pending;
                    }
                    Err(_) => {
                        self.write_queued.fetch_sub(1, Ordering::SeqCst);
                        return Poll::Ready(Err(ChannelError::Closed));
                    }
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

impl OrderedByteChannel for HeldCancelChannel {
    fn max_read_scratch_bytes(&self) -> usize {
        self.inner.max_read_scratch_bytes()
    }

    fn read_chunk<'a>(
        &'a self,
        buffer: &'a mut [u8],
    ) -> BoxFuture<'a, Result<Option<usize>, ChannelError>> {
        let mut read = self.inner.read_chunk(buffer);
        Box::pin(poll_fn(move |cx| {
            if self.eof.load(Ordering::SeqCst) {
                self.eof_read.store(true, Ordering::SeqCst);
                return Poll::Ready(Ok(None));
            }
            *self.read_wake.lock().unwrap() = Some(cx.waker().clone());
            read.as_mut().poll(cx)
        }))
    }

    fn write_frame(&self, frame: EncodedFrame) -> BoxFuture<'_, Result<(), ChannelError>> {
        let mut reader = FrameReader::<v1_1::Envelope>::new(nz(262144));
        let cancel_id = reader
            .read_one(&frame.bytes)
            .ok()
            .and_then(|step| step.frame)
            .and_then(|frame| match frame.envelope.frame {
                Some(v1_1::envelope::Frame::Cancel(cancel)) => Some(cancel.request_id),
                _ => None,
            });
        let Some(cancel_id) = cancel_id else {
            return self.inner.write_frame(frame);
        };
        let expected = self.expected_cancel_id.load(Ordering::SeqCst);
        let should_hold = expected != 0
            && expected == cancel_id
            && !self.gate_claimed.swap(true, Ordering::SeqCst);
        if !should_hold {
            return self.inner.write_frame(frame);
        }
        {
            *self.cancel_id.lock().unwrap() = Some(cancel_id);
            *self.cancel_bytes.lock().unwrap() = Some(frame.bytes.clone());
            self.eof.store(true, Ordering::SeqCst);
            if let Some(waker) = self.read_wake.lock().unwrap().take() {
                waker.wake();
            }
        }
        self.cancel_count.fetch_add(1, Ordering::SeqCst);
        let mut pending_frame = Some(frame);
        let mut delegated: Option<BoxFuture<'_, Result<(), ChannelError>>> = None;
        let mut waiting = false;
        Box::pin(poll_fn(move |cx| {
            if let Some(delegate) = delegated.as_mut() {
                return match delegate.as_mut().poll(cx) {
                    Poll::Ready(result) => {
                        self.cancel_complete.store(true, Ordering::SeqCst);
                        Poll::Ready(result)
                    }
                    Poll::Pending => Poll::Pending,
                };
            }
            if self
                .release_permits
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |permits| {
                    if permits > 0 { Some(permits - 1) } else { None }
                })
                .is_err()
            {
                if !waiting {
                    self.pending_cancel.fetch_add(1, Ordering::SeqCst);
                    waiting = true;
                }
                *self.write_wake.lock().unwrap() = Some(cx.waker().clone());
                return Poll::Pending;
            }
            if waiting {
                self.pending_cancel.fetch_sub(1, Ordering::SeqCst);
                waiting = false;
            }
            if delegated.is_none() {
                delegated = Some(self.inner.write_frame(pending_frame.take().unwrap()));
            }
            match delegated.as_mut().unwrap().as_mut().poll(cx) {
                Poll::Ready(result) => {
                    self.cancel_complete.store(true, Ordering::SeqCst);
                    Poll::Ready(result)
                }
                Poll::Pending => Poll::Pending,
            }
        }))
    }

    fn close(&self) {
        if !self.released.load(Ordering::SeqCst) || self.pending_cancel.load(Ordering::SeqCst) > 0 {
            self.early_close.store(true, Ordering::SeqCst);
            return;
        }
        if self
            .closes
            .compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            self.inner.close();
        }
    }
}
