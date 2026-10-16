use super::{Transport, TransportEvent};
use crate::{
    BoxFuture, CancellationToken, DEFAULT_MAX_MESSAGE_BYTES, Error, Result, validate_message,
};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    path::PathBuf,
    process::Stdio,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU32, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    process::{Child, ChildStdin, Command},
    sync::{Mutex as AsyncMutex, mpsc},
};

#[derive(Clone, Debug)]
pub struct StdioOptions {
    pub command: PathBuf,
    pub args: Vec<String>,
    pub cwd: Option<PathBuf>,
    pub env: BTreeMap<String, String>,
    pub inherit_env: bool,
    pub inherit_stderr: bool,
    pub max_message_bytes: usize,
    pub max_stderr_bytes: usize,
    pub close_timeout: Duration,
}
impl StdioOptions {
    pub fn new(command: impl Into<PathBuf>) -> Self {
        Self {
            command: command.into(),
            args: Vec::new(),
            cwd: None,
            env: BTreeMap::new(),
            inherit_env: true,
            inherit_stderr: false,
            max_message_bytes: DEFAULT_MAX_MESSAGE_BYTES,
            max_stderr_bytes: 64 * 1024,
            close_timeout: Duration::from_secs(2),
        }
    }
}
struct Inner {
    options: StdioOptions,
    started: AtomicBool,
    closed: AtomicBool,
    close_started: AtomicBool,
    closing: CancellationToken,
    close_done: CancellationToken,
    pid: AtomicU32,
    stdin: AsyncMutex<Option<ChildStdin>>,
    events: Mutex<Option<mpsc::UnboundedSender<TransportEvent>>>,
    stderr: Mutex<Vec<u8>>,
}
#[derive(Clone)]
pub struct StdioTransport(Arc<Inner>, Arc<()>);
impl StdioTransport {
    pub fn new(options: StdioOptions) -> Self {
        Self(
            Arc::new(Inner {
                options,
                started: AtomicBool::new(false),
                closed: AtomicBool::new(false),
                close_started: AtomicBool::new(false),
                closing: CancellationToken::new(),
                close_done: CancellationToken::new(),
                pid: AtomicU32::new(0),
                stdin: AsyncMutex::new(None),
                events: Mutex::new(None),
                stderr: Mutex::new(Vec::new()),
            }),
            Arc::new(()),
        )
    }
    pub fn pid(&self) -> Option<u32> {
        match self.0.pid.load(Ordering::Acquire) {
            0 => None,
            pid => Some(pid),
        }
    }
    pub fn stderr(&self) -> String {
        String::from_utf8_lossy(&self.0.stderr.lock().unwrap()).into_owned()
    }
}
impl Drop for StdioTransport {
    fn drop(&mut self) {
        if Arc::strong_count(&self.1) == 1 {
            self.0.closing.cancel();
        }
    }
}
fn emit(inner: &Inner, event: TransportEvent) {
    if let Some(events) = inner.events.lock().unwrap().as_ref() {
        let _ = events.send(event);
    }
}
fn emit_close(inner: &Inner) {
    if !inner.closed.swap(true, Ordering::AcqRel) {
        emit(inner, TransportEvent::Closed);
    }
}

#[cfg(unix)]
fn signal_group(pid: u32, force: bool) {
    if let Some(pid) = rustix::process::Pid::from_raw(pid as i32) {
        let _ = rustix::process::kill_process_group(
            pid,
            if force {
                rustix::process::Signal::KILL
            } else {
                rustix::process::Signal::TERM
            },
        );
    }
}
#[cfg(not(unix))]
fn signal_group(pid: u32, _force: bool) {
    let _ = std::process::Command::new("taskkill")
        .args(["/PID", &pid.to_string(), "/T", "/F"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn();
}
async fn supervise(
    inner: Arc<Inner>,
    mut child: Child,
    pid: u32,
    stdout: tokio::task::JoinHandle<()>,
    stderr: Option<tokio::task::JoinHandle<()>>,
) {
    tokio::select! {
        _ = child.wait() => {},
        _ = inner.closing.cancelled() => {
            inner.stdin.lock().await.take();
            let grace = Duration::from_millis(500).min(inner.options.close_timeout);
            if tokio::time::timeout(grace, child.wait()).await.is_err() {
                signal_group(pid, false);
                if tokio::time::timeout(inner.options.close_timeout, child.wait()).await.is_err() {
                    signal_group(pid, true);
                    let _ = child.kill().await;
                    let _ = child.wait().await;
                }
            }
        }
    }
    // Also terminate descendants that kept running after the direct child exited.
    signal_group(pid, false);
    tokio::time::sleep(inner.options.close_timeout.min(Duration::from_millis(50))).await;
    signal_group(pid, true);
    inner.pid.store(0, Ordering::Release);
    inner.stdin.lock().await.take();
    // Drain final frames before emitting Closed. A child may exit immediately
    // after writing its answer. Bound draining for descendants that detached
    // from the owned process group while retaining inherited output pipes.
    let _ = tokio::time::timeout(Duration::from_secs(1), stdout).await;
    if let Some(stderr) = stderr {
        let _ = tokio::time::timeout(Duration::from_secs(1), stderr).await;
    }
    emit_close(&inner);
    inner.close_done.cancel();
}
impl Transport for StdioTransport {
    fn start(&self) -> BoxFuture<'_, Result<mpsc::UnboundedReceiver<TransportEvent>>> {
        Box::pin(async {
            if self.0.closed.load(Ordering::Acquire) {
                return Err(Error::Closed);
            }
            if self.0.started.swap(true, Ordering::AcqRel) {
                return Err(Error::invalid("stdio already started"));
            }
            let options = &self.0.options;
            let mut command = Command::new(&options.command);
            command
                .args(&options.args)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(if options.inherit_stderr {
                    Stdio::inherit()
                } else {
                    Stdio::piped()
                })
                .kill_on_drop(true);
            if !options.inherit_env {
                command.env_clear();
            }
            command.envs(&options.env);
            if let Some(cwd) = &options.cwd {
                command.current_dir(cwd);
            }
            #[cfg(unix)]
            command.process_group(0);
            let mut child = match command.spawn() {
                Ok(child) => child,
                Err(error) => {
                    emit_close(&self.0);
                    self.0.close_done.cancel();
                    return Err(error.into());
                }
            };
            let pid = child.id().ok_or(Error::Closed)?;
            self.0.pid.store(pid, Ordering::Release);
            *self.0.stdin.lock().await = child.stdin.take();
            let (events, receiver) = mpsc::unbounded_channel();
            *self.0.events.lock().unwrap() = Some(events);
            let mut stdout = child.stdout.take().unwrap();
            let inner = self.0.clone();
            let stdout_task = tokio::spawn(async move {
                let mut buffer = Vec::new();
                let mut chunk = [0_u8; 8192];
                let mut discarding = false;
                loop {
                    let read = tokio::select! {
                        _ = inner.close_done.cancelled() => break,
                        read = stdout.read(&mut chunk) => read,
                    };
                    let size = match read {
                        Ok(0) => break,
                        Ok(size) => size,
                        Err(error) => {
                            emit(&inner, TransportEvent::Error(error.into()));
                            break;
                        }
                    };
                    for &byte in &chunk[..size] {
                        if byte == b'\n' {
                            if !discarding && !buffer.iter().all(u8::is_ascii_whitespace) {
                                match serde_json::from_slice(&buffer)
                                    .map_err(Error::from)
                                    .and_then(|message| {
                                        validate_message(&message)?;
                                        Ok(message)
                                    }) {
                                    Ok(message) => emit(&inner, TransportEvent::Message(message)),
                                    Err(error) => emit(&inner, TransportEvent::Error(error)),
                                }
                            }
                            buffer.clear();
                            discarding = false;
                        } else if !discarding {
                            buffer.push(byte);
                            if buffer.len() > inner.options.max_message_bytes {
                                buffer.clear();
                                discarding = true;
                                emit(
                                    &inner,
                                    TransportEvent::Error(Error::invalid(
                                        "stdio message exceeds byte limit",
                                    )),
                                );
                            }
                        }
                    }
                }
                if !buffer.iter().all(u8::is_ascii_whitespace) {
                    emit(
                        &inner,
                        TransportEvent::Error(Error::invalid("incomplete stdio message at EOF")),
                    );
                }
                // EOF on stdout is terminal even if the process still runs.
                inner.closing.cancel();
            });
            let stderr_task = if let Some(mut stderr) = child.stderr.take() {
                let inner = self.0.clone();
                Some(tokio::spawn(async move {
                    let mut chunk = [0_u8; 4096];
                    loop {
                        let read = tokio::select! { _ = inner.close_done.cancelled() => break, read = stderr.read(&mut chunk) => read };
                        match read {
                            Ok(0) | Err(_) => break,
                            Ok(size) => {
                                let mut buffer = inner.stderr.lock().unwrap();
                                buffer.extend_from_slice(&chunk[..size]);
                                let excess =
                                    buffer.len().saturating_sub(inner.options.max_stderr_bytes);
                                buffer.drain(..excess);
                            }
                        }
                    }
                }))
            } else {
                None
            };
            tokio::spawn(supervise(
                self.0.clone(),
                child,
                pid,
                stdout_task,
                stderr_task,
            ));
            Ok(receiver)
        })
    }
    fn send(&self, message: Value, cancellation: CancellationToken) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            if self.0.closed.load(Ordering::Acquire) || self.0.closing.is_cancelled() {
                return Err(Error::Closed);
            }
            validate_message(&message)?;
            let mut data = serde_json::to_vec(&message)?;
            data.push(b'\n');
            let mut stdin = tokio::select! { _ = cancellation.cancelled() => return Err(Error::Cancelled), stdin = self.0.stdin.lock() => stdin };
            if cancellation.is_cancelled() {
                return Err(Error::Cancelled);
            }
            // Once a frame begins it is written atomically with respect to other
            // frames. Cancelling a partial write would corrupt the next message.
            let stdin = stdin.as_mut().ok_or(Error::Closed)?;
            tokio::select! {
                _ = self.0.closing.cancelled() => Err(Error::Closed),
                result = async { stdin.write_all(&data).await?; stdin.flush().await } => result.map_err(Error::from),
            }
        })
    }
    fn close(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async {
            if !self.0.started.load(Ordering::Acquire) {
                emit_close(&self.0);
                return Ok(());
            }
            if !self.0.close_started.swap(true, Ordering::AcqRel) {
                self.0.closing.cancel();
            }
            self.0.close_done.cancelled().await;
            Ok(())
        })
    }
}
