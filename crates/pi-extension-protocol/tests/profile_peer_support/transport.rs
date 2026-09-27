use pi_extension_protocol::wire::v1_1;
use prost::Message;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
const WAIT: Duration = Duration::from_secs(3);
const CAP: usize = 262_144;

pub fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}
pub fn command(language: &str) -> Command {
    let mut c = Command::new(if language == "node" {
        "node"
    } else {
        "python3"
    });
    if language == "node" {
        c.args([
            "--disable-warning=MODULE_TYPELESS_PACKAGE_JSON",
            "--experimental-strip-types",
            "sdk/typescript/profile-peer.ts",
        ]);
    } else {
        c.args(["-B", "sdk/python/profile_peer.py"]);
    }
    c.current_dir(root());
    c
}
pub fn prefix(mut n: usize) -> Vec<u8> {
    let mut b = Vec::new();
    loop {
        b.push((n as u8 & 127) | if n > 127 { 128 } else { 0 });
        n >>= 7;
        if n == 0 {
            return b;
        }
    }
}
pub fn frame(arm: v1_1::envelope::Frame) -> Vec<u8> {
    let b = v1_1::Envelope { frame: Some(arm) }.encode_to_vec();
    let mut out = prefix(b.len());
    out.extend(b);
    out
}
fn read_frame(out: &mut impl Read) -> Result<Option<Vec<u8>>, String> {
    let mut head = Vec::new();
    let mut n = 0usize;
    for shift in (0..70).step_by(7) {
        let mut b = [0];
        match out.read_exact(&mut b) {
            Ok(()) => {}
            Err(e) if head.is_empty() && e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Ok(None);
            }
            Err(e) => return Err(e.to_string()),
        };
        head.push(b[0]);
        if shift >= usize::BITS as usize && b[0] & 127 != 0 {
            return Err("prefix overflow".into());
        }
        n |= usize::from(b[0] & 127)
            .checked_shl(shift as u32)
            .unwrap_or(0);
        if b[0] < 128 {
            if n == 0 || n > CAP || head != prefix(n) {
                return Err("frame bound".into());
            }
            let old = head.len();
            head.resize(old + n, 0);
            out.read_exact(&mut head[old..])
                .map_err(|e| e.to_string())?;
            return Ok(Some(head));
        }
    }
    Err("prefix cap".into())
}
fn bounded_bytes(mut pipe: impl Read, cap: usize) -> Result<Vec<u8>, String> {
    let mut b = Vec::new();
    pipe.by_ref()
        .take((cap + 1) as u64)
        .read_to_end(&mut b)
        .map_err(|e| e.to_string())?;
    if b.len() > cap {
        return Err("pipe cap".into());
    }
    Ok(b)
}
fn reap(child: &mut Child, force: bool) -> Result<std::process::ExitStatus, String> {
    if force {
        let _ = child.kill();
    }
    let until = Instant::now() + WAIT;
    loop {
        if let Some(s) = child.try_wait().map_err(|e| e.to_string())? {
            return Ok(s);
        }
        if Instant::now() >= until {
            kill_group(child.id());
            let _ = child.kill();
            let cleanup = Instant::now() + WAIT;
            loop {
                if let Some(s) = child.try_wait().map_err(|e| e.to_string())? {
                    return Ok(s);
                }
                if Instant::now() >= cleanup {
                    return Err("child cleanup deadline".into());
                }
                thread::sleep(Duration::from_millis(5));
            }
        }
        thread::sleep(Duration::from_millis(5));
    }
}
fn kill_group(pid: u32) {
    if let Ok(mut child) = Command::new("/usr/bin/kill")
        .args(["-KILL", "--", &format!("-{pid}")])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
    {
        let end = Instant::now() + WAIT;
        while let Ok(None) = child.try_wait() {
            if Instant::now() >= end {
                let _ = child.kill();
                break;
            }
            thread::sleep(Duration::from_millis(5));
        }
        let end = Instant::now() + WAIT;
        while let Ok(None) = child.try_wait() {
            if Instant::now() >= end {
                break;
            }
            thread::sleep(Duration::from_millis(5));
        }
    }
}
pub struct Peer {
    child: Option<Child>,
    input: Option<SyncSender<Vec<u8>>>,
    output: Receiver<Result<Option<Vec<u8>>, String>>,
    reader: Option<JoinHandle<()>>,
    writer: Option<JoinHandle<()>>,
    stderr: Option<JoinHandle<Result<Vec<u8>, String>>>,
    pub sent: Vec<Vec<u8>>,
    pub received: Vec<Vec<u8>>,
    pub pid: u32,
    started: Instant,
    label: String,
}
impl Peer {
    pub fn spawn(language: &str, core: u32, minor: u32, scenario: &str) -> Self {
        use std::os::unix::process::CommandExt;
        let mut c = command(language);
        c.args([
            "--serve",
            root().to_str().unwrap(),
            &core.to_string(),
            &minor.to_string(),
            scenario,
        ]);
        c.process_group(0);
        let mut child = c
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let pid = child.id();
        let (mut stdout, mut stdin, stderr) = (
            child.stdout.take().unwrap(),
            child.stdin.take().unwrap(),
            child.stderr.take().unwrap(),
        );
        let (tx, output) = mpsc::channel();
        let (input, rx) = mpsc::sync_channel::<Vec<u8>>(4);
        let mut guard = Self {
            child: Some(child),
            input: Some(input),
            output,
            reader: None,
            writer: None,
            stderr: None,
            sent: vec![],
            received: vec![],
            pid,
            started: Instant::now(),
            label: format!("{language}-{core}-{minor}-{scenario}"),
        };
        guard.reader = Some(
            thread::Builder::new()
                .name("profile stdout".into())
                .spawn(move || {
                    let mut total = 0usize;
                    for _ in 0..128 {
                        let mut r = read_frame(&mut stdout);
                        if let Ok(Some(raw)) = &r {
                            total += raw.len();
                            if total > 4_194_304 {
                                r = Err("stdout aggregate cap".into());
                            }
                        }
                        let end = !matches!(r, Ok(Some(_)));
                        if tx.send(r).is_err() || end {
                            return;
                        }
                    }
                    let _ = tx.send(Err("frame count".into()));
                })
                .unwrap(),
        );
        guard.writer = Some(
            thread::Builder::new()
                .name("profile stdin".into())
                .spawn(move || {
                    for raw in rx {
                        if stdin.write_all(&raw).is_err() {
                            return;
                        }
                    }
                })
                .unwrap(),
        );
        guard.stderr = Some(
            thread::Builder::new()
                .name("profile stderr".into())
                .spawn(move || bounded_bytes(stderr, 16_384))
                .unwrap(),
        );
        guard
    }
    pub fn send(&mut self, b: Vec<u8>) {
        assert!(
            b.len() <= CAP + 10
                && self.sent.len() < 128
                && self.sent.iter().map(Vec::len).sum::<usize>() + b.len() <= 4_194_304
        );
        self.sent.push(b.clone());
        self.input.as_ref().unwrap().try_send(b).unwrap();
    }
    pub fn recv(&mut self) -> Vec<u8> {
        assert!(self.started.elapsed() < Duration::from_secs(20));
        let b = self
            .output
            .recv_timeout(WAIT)
            .expect("peer phase deadline")
            .expect("bounded peer frame")
            .expect("peer EOF");
        self.received.push(b.clone());
        b
    }
    pub fn try_recv(&mut self, ms: u64) -> Option<Vec<u8>> {
        match self.output.recv_timeout(Duration::from_millis(ms)) {
            Ok(Ok(Some(b))) => {
                self.received.push(b.clone());
                Some(b)
            }
            Err(mpsc::RecvTimeoutError::Timeout) => None,
            x => panic!("unexpected peer output: {x:?}"),
        }
    }
    pub fn finish(mut self, name: &str, force: bool) {
        self.input.take();
        let status = reap(self.child.as_mut().unwrap(), force).unwrap();
        kill_group(self.pid);
        self.child.take();
        self.reader.take().unwrap().join().unwrap();
        self.writer.take().unwrap().join().unwrap();
        let err = self.stderr.take().unwrap().join().unwrap().unwrap();
        if !force {
            assert!(
                status.success(),
                "peer {name}: {status}, {}",
                String::from_utf8_lossy(&err)
            );
        }
        if let Ok(base) = std::env::var("PI_PROFILE_PEER_EVIDENCE") {
            let p = PathBuf::from(base).join(name);
            std::fs::create_dir_all(&p).unwrap();
            std::fs::write(p.join("stderr.log"), err).unwrap();
            std::fs::write(p.join("inbound.bin"), self.received.concat()).unwrap();
            std::fs::write(p.join("outbound.bin"), self.sent.concat()).unwrap();
            std::fs::write(p.join("cleanup.json"),format!("{{\"pid\":{},\"reaped\":true,\"group_cleanup\":true,\"status\":{},\"forced\":{}}}\n",self.pid,status.code().map_or("null".into(),|n|n.to_string()),force)).unwrap();
        }
    }
}
impl Drop for Peer {
    fn drop(&mut self) {
        self.input.take();
        let mut status = None;
        if let Some(mut child) = self.child.take() {
            kill_group(self.pid);
            status = reap(&mut child, true).ok();
        }
        if let Some(h) = self.reader.take() {
            let _ = h.join();
        }
        if let Some(h) = self.writer.take() {
            let _ = h.join();
        }
        let err = self
            .stderr
            .take()
            .and_then(|h| h.join().ok())
            .and_then(Result::ok)
            .unwrap_or_default();
        if status.is_some()
            && let Ok(base) = std::env::var("PI_PROFILE_PEER_EVIDENCE")
        {
            let p = PathBuf::from(base).join(format!("{}-failure", self.label));
            let _ = std::fs::create_dir_all(&p);
            let _ = std::fs::write(p.join("stderr.log"), err);
            let _ = std::fs::write(p.join("inbound.bin"), self.received.concat());
            let _ = std::fs::write(p.join("outbound.bin"), self.sent.concat());
            let _ = std::fs::write(
                p.join("cleanup.json"),
                format!(
                    "{{\"pid\":{},\"reaped\":true,\"group_cleanup\":true,\"failure_cleanup\":true}}\n",
                    self.pid
                ),
            );
        }
    }
}
struct ProbeGuard {
    child: Option<Child>,
    out: Option<JoinHandle<Result<Vec<u8>, String>>>,
    err: Option<JoinHandle<Result<Vec<u8>, String>>>,
}
impl Drop for ProbeGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            kill_group(child.id());
            let _ = reap(&mut child, true);
        }
        if let Some(worker) = self.out.take() {
            let _ = worker.join();
        }
        if let Some(worker) = self.err.take() {
            let _ = worker.join();
        }
    }
}
pub fn probe(language: &str) -> serde_json::Value {
    use std::os::unix::process::CommandExt;
    let mut c = command(language);
    c.args(["--probe", root().to_str().unwrap()]);
    c.process_group(0);
    let mut child = c
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let pid = child.id();
    let out = child.stdout.take().unwrap();
    let err = child.stderr.take().unwrap();
    let mut guard = ProbeGuard {
        child: Some(child),
        out: None,
        err: None,
    };
    guard.out = Some(
        thread::Builder::new()
            .name("profile probe stdout".into())
            .spawn(move || bounded_bytes(out, 2_000_000))
            .unwrap(),
    );
    guard.err = Some(
        thread::Builder::new()
            .name("profile probe stderr".into())
            .spawn(move || bounded_bytes(err, 16_384))
            .unwrap(),
    );
    let status = reap(guard.child.as_mut().unwrap(), false).unwrap();
    kill_group(pid);
    guard.child.take();
    let out = guard.out.take().unwrap().join().unwrap().unwrap();
    let err = guard.err.take().unwrap().join().unwrap().unwrap();
    assert!(status.success(), "probe: {}", String::from_utf8_lossy(&err));
    if let Ok(base) = std::env::var("PI_PROFILE_PEER_EVIDENCE") {
        let p = PathBuf::from(base).join(format!("probe-{language}"));
        std::fs::create_dir_all(&p).unwrap();
        std::fs::write(p.join("stdout.json"), &out).unwrap();
        std::fs::write(p.join("stderr.log"), &err).unwrap();
        std::fs::write(
            p.join("cleanup.json"),
            format!("{{\"pid\":{pid},\"reaped\":true,\"group_cleanup\":true}}\n"),
        )
        .unwrap();
    }
    serde_json::from_slice(&out).unwrap()
}
