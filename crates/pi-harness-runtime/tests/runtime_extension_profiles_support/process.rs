use pi_ai_core::BoxFuture;
use pi_extension_protocol::wire::{
    framing::{EncodedFrame, FrameReader},
    v1_1,
};
use pi_harness_runtime::extensions::transport::{ChannelError, MonotonicClock, OrderedByteChannel};
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
            "0ae4270e1ef451b5d1993c84410b2c0233674014733e235586060a2dbadf1ff6",
        ),
        (
            "sdk/typescript/profiles/wire.ts",
            "195fbfa6810902639e428207c60443d02baa331543523f59463e7bc8013f0a9c",
        ),
        (
            "sdk/typescript/profiles/shapes.ts",
            "33e79bcc760031e842821b6371883ec75adf45f57f3ba0dbca6df4f06978f440",
        ),
        (
            "sdk/python/profile_peer.py",
            "4b7f18001b089ffc9b5cfc8b609bc479d33204afd9f93c5e7f2cc334b739bb34",
        ),
        (
            "sdk/python/profiles/wire.py",
            "feb928833a07a3c375cd039f5806750d3b25001c650e4c381bf0021c1e5aa571",
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
    stderr: Arc<Mutex<Vec<u8>>>,
    done: Mutex<mpsc::Receiver<()>>,
    workers: Mutex<Vec<std::thread::JoinHandle<()>>>,
    release: PathBuf,
}
impl Channel {
    pub fn spawn(language: &str, core: u32, minor: u32) -> Arc<Self> {
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
            .arg("--serve")
            .arg(&repo)
            .arg(core.to_string())
            .arg(minor.to_string())
            .arg("none");
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
                let mut buffer = [0u8; 4096];
                let count = output.read(&mut buffer).unwrap_or(0);
                total += count;
                assert!(total <= 1048576, "finite peer stdout budget");
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
