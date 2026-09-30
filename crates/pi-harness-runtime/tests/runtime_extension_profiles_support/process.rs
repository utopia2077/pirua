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
    future::{Future, poll_fn},
    io::{Read, Write},
    num::NonZeroUsize,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    },
    task::{Poll, Waker},
    time::{Duration, Instant},
};
pub const LIMIT: Duration = Duration::from_secs(15);
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
            "3fed2d1036091261f47d91aab3fdb53f65dc867305c7d5bd11099dd7df359876",
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
            "0432520b6cd6c11b41fc2ef9afeba575aecfb77ac4376a03db2f4604355dcc1b",
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
    release: PathBuf,
    corrupt_lane: AtomicBool,
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
        let (stop, wake, done, captured) = (
            closed.clone(),
            read_wake.clone(),
            done_tx.clone(),
            incoming.clone(),
        );
        workers.push(std::thread::spawn(move || {
            let mut total = 0;
            loop {
                let mut buffer = [0u8; 4096];
                let count = output.read(&mut buffer).unwrap_or(0);
                total += count;
                assert!(total <= 1048576, "finite peer stdout budget");
                captured.lock().unwrap().extend_from_slice(&buffer[..count]);
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
            incoming,
            stderr,
            done: Mutex::new(done_rx),
            workers: Mutex::new(workers),
            release,
            corrupt_lane: AtomicBool::new(false),
        })
    }
    pub fn spawn_invalid_lane(language: &str, core: u32, minor: u32) -> Arc<Self> {
        let peer = Self::spawn_scenario(language, core, minor, "runtime-native");
        peer.corrupt_lane.store(true, Ordering::SeqCst);
        peer
    }
    pub fn stderr_text(&self) -> String {
        String::from_utf8_lossy(&self.stderr.lock().unwrap()).into_owned()
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
        }
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
