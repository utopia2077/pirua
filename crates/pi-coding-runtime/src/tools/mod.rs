//! Coding-agent filesystem and process tools.
//!
//! The tools deliberately depend on the [`ExecutionEnv`] port.  Hosts can
//! inject a deterministic environment while `LocalExecutionEnv` provides the
//! normal local filesystem and shell implementation.

mod edits;
mod image;
mod output;

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;

use pi_agent_schema::{AgentError, AgentTool, AgentToolResult, ToolInvocation};
use pi_ai::{
    BoxFuture, CancellationToken, ImageContent, ImageContentType, InputContent, JsonObject,
    JsonValue, TextContent, TextContentType, Tool,
};
use regex::Regex;
use serde_json::json;

pub const DEFAULT_MAX_LINES: usize = 2_000;
pub const DEFAULT_MAX_BYTES: usize = 50 * 1024;
pub const GREP_MAX_LINE_LENGTH: usize = 500;

/// Detached output of a coding environment operation.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ToolOutput {
    pub content: Vec<InputContent>,
    pub details: Option<JsonValue>,
    pub usage: Option<pi_ai::Usage>,
    pub terminate: Option<bool>,
    pub structured_content: Option<JsonValue>,
    pub is_error: Option<bool>,
}
impl From<ToolOutput> for AgentToolResult {
    fn from(value: ToolOutput) -> Self {
        Self {
            content: value.content,
            details: value.details,
            usage: value.usage,
            terminate: value.terminate,
            structured_content: value.structured_content,
            is_error: value.is_error,
        }
    }
}
/// Detached progress callback. No scheduler identity is captured by the operation.
pub type ToolOutputUpdate = Arc<dyn Fn(ToolOutput) + Send + Sync>;

/// Reusable environment operation shared by transient and durable adapters.
pub trait CodingTool: Send + Sync {
    fn descriptor(&self) -> &Tool;
    fn output_schema(&self) -> Option<pi_ai::JsonSchema> {
        None
    }
    fn prepare_arguments(&self, args: JsonValue) -> EnvResult<JsonObject>;
    fn execute<'a>(
        &'a self,
        arguments: JsonObject,
        cancellation: &'a CancellationToken,
        on_update: &'a ToolOutputUpdate,
        context: &'a ExecutionToolContext,
    ) -> BoxFuture<'a, EnvResult<ToolOutput>>;
}

/// Bind one operation to the Agent callback contract using explicit host capabilities.
pub fn bind_agent_tool(operation: Arc<dyn CodingTool>, context: ExecutionToolContext) -> AgentTool {
    let declaration = operation.descriptor().clone();
    let output_schema = operation.output_schema();
    let prepare = operation.clone();
    let mut tool = AgentTool::new(
        declaration,
        Arc::new(move |invocation: ToolInvocation| {
            let operation = operation.clone();
            let context = context.clone();
            Box::pin(async move {
                let on_update: ToolOutputUpdate =
                    Arc::new(move |output| (invocation.on_update)(output.into()));
                operation
                    .execute(
                        invocation.arguments,
                        &invocation.cancellation,
                        &on_update,
                        &context,
                    )
                    .await
                    .map(Into::into)
                    .map_err(|error| AgentError::Tool(error.message))
            })
        }),
    );
    tool.output_schema = output_schema;
    tool.prepare_arguments = Some(Arc::new(move |arguments| {
        prepare
            .prepare_arguments(JsonValue::Object(arguments))
            .map_err(|error| AgentError::Tool(error.message))
    }));
    tool
}

/// Result of a fallible host operation.
pub type EnvResult<T> = Result<T, EnvError>;

/// Stable environment error returned by an injected host.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnvError {
    /// Human-readable error.
    pub message: String,
    pub io_kind: Option<std::io::ErrorKind>,
}

impl EnvError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            io_kind: None,
        }
    }
}
impl From<std::io::Error> for EnvError {
    fn from(error: std::io::Error) -> Self {
        Self {
            message: error.to_string(),
            io_kind: Some(error.kind()),
        }
    }
}
impl fmt::Display for EnvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}
impl std::error::Error for EnvError {}

/// Metadata for one addressed filesystem object.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileInfo {
    /// Basename.
    pub name: String,
    /// Addressed path.
    pub path: String,
    /// True for directories.
    pub is_dir: bool,
}

/// Process output returned by an [`ExecutionEnv`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProcessOutput {
    /// Captured standard output.
    pub stdout: String,
    /// Captured standard error.
    pub stderr: String,
    /// Optional merged arrival-ordered stream supplied by the host.
    pub merged: Option<String>,
    /// Exit status, or -1 when unavailable.
    pub exit_code: i32,
}

/// Host-produced image content after optional conversion/resizing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProcessedImage {
    /// Base64 data.
    pub data: String,
    /// Resulting MIME type.
    pub mime_type: String,
    /// Human-readable processing hints.
    pub hints: Vec<String>,
}

/// Process stream callback.
pub type ProcessData = Arc<dyn Fn(&[u8]) + Send + Sync>;

/// Process options supplied by a coding tool.
#[derive(Clone, Default)]
pub struct ProcessOptions {
    /// Working directory.
    pub cwd: Option<String>,
    /// Optional timeout.
    pub timeout: Option<Duration>,
    /// Environment overrides.
    pub env: BTreeMap<String, String>,
    /// Shared cancellation flag a host process runner may observe.
    pub abort_signal: Option<CancellationToken>,
    /// Arrival-ordered byte chunks; a host using this callback need not retain full output.
    pub on_data: Option<ProcessData>,
    /// Optional host-selected shell executable.
    pub executable: Option<PathBuf>,
}

/// Filesystem search policy shared by native and injected environments.
#[derive(Clone)]
pub struct SearchOptions {
    pub require_git: bool,
    pub include_directories: bool,
    pub cancellation: CancellationToken,
}

/// Host port consumed by all coding tools.
pub trait ExecutionEnv: Send + Sync {
    /// Optional native walker. Returning None selects the portable injected-port walker.
    fn walk_files<'a>(
        &'a self,
        _root: &'a str,
        _options: SearchOptions,
    ) -> Option<BoxFuture<'a, EnvResult<Vec<FileInfo>>>> {
        None
    }
    /// Resolve a path without requiring it to exist.
    fn absolute_path<'a>(&'a self, path: &'a str) -> BoxFuture<'a, EnvResult<String>>;
    /// Return whether a path exists.
    fn exists<'a>(&'a self, path: &'a str) -> BoxFuture<'a, EnvResult<bool>>;
    /// Read binary bytes.
    fn read_binary_file<'a>(&'a self, path: &'a str) -> BoxFuture<'a, EnvResult<Vec<u8>>>;
    /// Optionally process an image (conversion/resizing remains a host concern).
    fn process_image<'a>(
        &'a self,
        _bytes: &'a [u8],
        _mime_type: &'a str,
        options: &'a ReadToolOptions,
    ) -> BoxFuture<'a, EnvResult<Option<ProcessedImage>>> {
        Box::pin(async move { image::process(_bytes, _mime_type, options) })
    }
    /// Read UTF-8 text.
    fn read_text_file<'a>(&'a self, path: &'a str) -> BoxFuture<'a, EnvResult<String>>;
    /// Write bytes or UTF-8 text, creating parent directories.
    fn write_file<'a>(&'a self, path: &'a str, content: &'a [u8]) -> BoxFuture<'a, EnvResult<()>>;
    /// List direct children.
    fn list_dir<'a>(&'a self, path: &'a str) -> BoxFuture<'a, EnvResult<Vec<FileInfo>>>;
    /// Return metadata.
    fn file_info<'a>(&'a self, path: &'a str) -> BoxFuture<'a, EnvResult<FileInfo>>;
    /// Canonicalize an existing path.
    fn canonical_path<'a>(&'a self, path: &'a str) -> BoxFuture<'a, EnvResult<String>>;
    /// Execute a process through a host-selected shell.
    fn exec<'a>(
        &'a self,
        shell: &'a str,
        command: &'a str,
        options: ProcessOptions,
    ) -> BoxFuture<'a, EnvResult<ProcessOutput>>;
    /// Create a temporary file and return its path.
    fn create_temp_file<'a>(
        &'a self,
        prefix: &'a str,
        content: &'a [u8],
    ) -> BoxFuture<'a, EnvResult<String>>;
}

/// Explicit environment capabilities captured for a tool invocation.
#[derive(Clone)]
pub struct ExecutionToolContext {
    /// Injected filesystem/process environment.
    pub env: Arc<dyn ExecutionEnv>,
}

/// Local filesystem and shell implementation of [`ExecutionEnv`].
#[derive(Clone, Debug)]
pub struct LocalExecutionEnv {
    cwd: PathBuf,
}
impl LocalExecutionEnv {
    /// Create an environment rooted at `cwd`.
    pub fn new(cwd: impl Into<PathBuf>) -> Self {
        let cwd = cwd.into();
        let cwd = if cwd.is_absolute() {
            cwd
        } else {
            std::env::current_dir().map_or_else(|_| cwd.clone(), |current| current.join(&cwd))
        };
        Self { cwd }
    }
    /// Return the configured working directory.
    pub fn cwd(&self) -> &Path {
        &self.cwd
    }
}

fn env_error(error: impl fmt::Display) -> EnvError {
    EnvError::new(error.to_string())
}
impl ExecutionEnv for LocalExecutionEnv {
    fn walk_files<'a>(
        &'a self,
        root: &'a str,
        options: SearchOptions,
    ) -> Option<BoxFuture<'a, EnvResult<Vec<FileInfo>>>> {
        let root = root.to_owned();
        Some(Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                let mut builder = ignore::WalkBuilder::new(&root);
                let require_git = options.require_git
                    || Path::new(&root)
                        .ancestors()
                        .any(|dir| dir.join(".git").exists());
                builder
                    .hidden(false)
                    .require_git(require_git)
                    .follow_links(false)
                    .add_custom_ignore_filename(if options.include_directories {
                        ".fdignore"
                    } else {
                        ".rgignore"
                    })
                    .filter_entry(|entry| entry.file_name() != ".git");
                let mut files = Vec::new();
                for entry in builder.build() {
                    check_cancelled(&options.cancellation)?;
                    let entry = entry.map_err(env_error)?;
                    if entry.depth() == 0 {
                        continue;
                    }
                    let is_dir = entry.file_type().is_some_and(|kind| kind.is_dir());
                    if !options.include_directories
                        && entry.file_type().is_some_and(|kind| kind.is_symlink())
                    {
                        continue;
                    }
                    if !is_dir || options.include_directories {
                        files.push(FileInfo {
                            name: entry.file_name().to_string_lossy().into_owned(),
                            path: entry.path().to_string_lossy().into_owned(),
                            is_dir,
                        });
                    }
                }
                Ok(files)
            })
            .await
            .map_err(env_error)?
        }))
    }
    fn absolute_path<'a>(&'a self, path: &'a str) -> BoxFuture<'a, EnvResult<String>> {
        Box::pin(async move {
            let path: String = path
                .chars()
                .map(|ch| match ch {
                    '\u{00a0}' | '\u{2000}'..='\u{200a}' | '\u{202f}' | '\u{205f}' | '\u{3000}' => {
                        ' '
                    }
                    ch => ch,
                })
                .collect();
            let path = path.strip_prefix('@').unwrap_or(&path);
            Ok(absolute_local_path(Path::new(path), &self.cwd)?
                .to_string_lossy()
                .into_owned())
        })
    }
    fn exists<'a>(&'a self, path: &'a str) -> BoxFuture<'a, EnvResult<bool>> {
        Box::pin(async move { Ok(Path::new(path).exists()) })
    }
    fn read_binary_file<'a>(&'a self, path: &'a str) -> BoxFuture<'a, EnvResult<Vec<u8>>> {
        Box::pin(async move { std::fs::read(path).map_err(env_error) })
    }
    fn process_image<'a>(
        &'a self,
        bytes: &'a [u8],
        mime_type: &'a str,
        options: &'a ReadToolOptions,
    ) -> BoxFuture<'a, EnvResult<Option<ProcessedImage>>> {
        let bytes = bytes.to_vec();
        let mime_type = mime_type.to_owned();
        let options = options.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || image::process(&bytes, &mime_type, &options))
                .await
                .map_err(env_error)?
        })
    }
    fn read_text_file<'a>(&'a self, path: &'a str) -> BoxFuture<'a, EnvResult<String>> {
        Box::pin(async move { std::fs::read_to_string(path).map_err(env_error) })
    }
    fn write_file<'a>(&'a self, path: &'a str, content: &'a [u8]) -> BoxFuture<'a, EnvResult<()>> {
        Box::pin(async move {
            if let Some(parent) = Path::new(path).parent() {
                std::fs::create_dir_all(parent).map_err(env_error)?;
            }
            std::fs::write(path, content).map_err(env_error)
        })
    }
    fn list_dir<'a>(&'a self, path: &'a str) -> BoxFuture<'a, EnvResult<Vec<FileInfo>>> {
        Box::pin(async move {
            let mut entries = Vec::new();
            for entry in std::fs::read_dir(path).map_err(env_error)? {
                let entry = entry.map_err(env_error)?;
                let p = entry.path();
                entries.push(FileInfo {
                    name: entry.file_name().to_string_lossy().into_owned(),
                    path: p.to_string_lossy().into_owned(),
                    is_dir: p.is_dir(),
                });
            }
            Ok(entries)
        })
    }
    fn file_info<'a>(&'a self, path: &'a str) -> BoxFuture<'a, EnvResult<FileInfo>> {
        Box::pin(async move {
            let p = Path::new(path);
            let metadata = std::fs::metadata(p).map_err(env_error)?;
            Ok(FileInfo {
                name: p
                    .file_name()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .into_owned(),
                path: path.to_owned(),
                is_dir: metadata.is_dir(),
            })
        })
    }
    fn canonical_path<'a>(&'a self, path: &'a str) -> BoxFuture<'a, EnvResult<String>> {
        Box::pin(async move {
            std::fs::canonicalize(path)
                .map(|p| p.to_string_lossy().into_owned())
                .map_err(EnvError::from)
        })
    }
    fn exec<'a>(
        &'a self,
        shell: &'a str,
        command: &'a str,
        options: ProcessOptions,
    ) -> BoxFuture<'a, EnvResult<ProcessOutput>> {
        let cwd = options
            .cwd
            .clone()
            .unwrap_or_else(|| self.cwd.to_string_lossy().into_owned());
        Box::pin(async move {
            let powershell =
                shell.eq_ignore_ascii_case("powershell") || shell.eq_ignore_ascii_case("pwsh");
            let executable = options.executable.clone().unwrap_or_else(|| {
                PathBuf::from(if powershell {
                    if cfg!(windows) { "powershell" } else { "pwsh" }
                } else {
                    "bash"
                })
            });
            let mut command_builder = tokio::process::Command::new(executable);
            if powershell {
                command_builder.args(["-NoProfile", "-NonInteractive", "-Command", command]);
            } else {
                command_builder.args(["-c", command]);
            }
            #[cfg(unix)]
            {
                use std::os::unix::process::CommandExt;
                command_builder.as_std_mut().process_group(0);
            }
            command_builder
                .current_dir(cwd)
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .kill_on_drop(true);
            for name in [
                "PI_SESSION_ID",
                "PI_SESSION_FILE",
                "PI_PROVIDER",
                "PI_MODEL",
                "PI_REASONING_LEVEL",
            ] {
                command_builder.env_remove(name);
            }
            command_builder.envs(&options.env);
            if options
                .abort_signal
                .as_ref()
                .is_some_and(CancellationToken::is_cancelled)
            {
                return Err(EnvError::new("Command aborted"));
            }
            let mut child = command_builder.spawn().map_err(env_error)?;
            let pid = child.id().expect("spawned child has an ID");
            let (send, mut receive) = tokio::sync::mpsc::channel(32);
            let mut readers = ProcessReaders(vec![
                spawn_process_reader(
                    child.stdout.take().expect("piped stdout"),
                    false,
                    send.clone(),
                ),
                spawn_process_reader(
                    child.stderr.take().expect("piped stderr"),
                    true,
                    send.clone(),
                ),
            ]);
            drop(send);
            let started = std::time::Instant::now();
            let mut stdout = Vec::new();
            let mut stderr = Vec::new();
            let mut merged = Vec::new();
            let mut status = None;
            let mut failure = None;
            let mut idle_since = None;
            let mut pipes_closed = false;
            loop {
                if failure.is_none() {
                    if options
                        .abort_signal
                        .as_ref()
                        .is_some_and(CancellationToken::is_cancelled)
                    {
                        failure = Some(EnvError::new("Command aborted"));
                    } else if let Some(timeout) = options
                        .timeout
                        .filter(|timeout| started.elapsed() >= *timeout)
                    {
                        failure = Some(EnvError::new(format!(
                            "Command timed out after {} seconds",
                            timeout.as_secs_f64()
                        )));
                    }
                    if failure.is_some() {
                        kill_process_tree(&mut child, pid);
                    }
                }
                if pipes_closed {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                } else {
                    match tokio::time::timeout(Duration::from_millis(5), receive.recv()).await {
                        Ok(Some(Ok((is_stderr, bytes)))) => {
                            if let Some(on_data) = &options.on_data {
                                on_data(&bytes);
                            } else {
                                if is_stderr {
                                    stderr.extend_from_slice(&bytes);
                                } else {
                                    stdout.extend_from_slice(&bytes);
                                }
                                merged.extend_from_slice(&bytes);
                            }
                            if status.is_some() {
                                idle_since = Some(std::time::Instant::now());
                            }
                        }
                        Ok(Some(Err(error))) => {
                            if failure.is_none() {
                                failure = Some(error);
                                kill_process_tree(&mut child, pid);
                            }
                        }
                        Ok(None) => pipes_closed = true,
                        Err(_) => {}
                    }
                }
                if status.is_none()
                    && let Some(exit) = child.try_wait().map_err(env_error)?
                {
                    status = Some(exit);
                    idle_since = Some(std::time::Instant::now());
                }
                // A successful shell may leave descendants holding its pipes. Match
                // child-process.ts: close readers after 100ms of output idleness;
                // every chunk renews that grace period. Only cancellation or timeout
                // terminates the process tree.
                if status.is_some()
                    && (pipes_closed
                        || idle_since.is_some_and(|at| at.elapsed() >= Duration::from_millis(100)))
                {
                    break;
                }
            }
            readers.stop().await;
            if let Some(error) = failure {
                return Err(error);
            }
            let status = match status {
                Some(status) => status,
                None => child.wait().await.map_err(env_error)?,
            };
            #[cfg(unix)]
            let exit_code = {
                use std::os::unix::process::ExitStatusExt;
                status
                    .code()
                    .unwrap_or_else(|| 128 + status.signal().unwrap_or(0))
            };
            #[cfg(not(unix))]
            let exit_code = status.code().unwrap_or(1);
            Ok(ProcessOutput {
                stdout: String::from_utf8_lossy(&stdout).into_owned(),
                stderr: String::from_utf8_lossy(&stderr).into_owned(),
                merged: Some(String::from_utf8_lossy(&merged).into_owned()),
                exit_code,
            })
        })
    }
    fn create_temp_file<'a>(
        &'a self,
        prefix: &'a str,
        content: &'a [u8],
    ) -> BoxFuture<'a, EnvResult<String>> {
        Box::pin(async move {
            let mut path = std::env::temp_dir();
            let unique = format!(
                "{}-{}-{}",
                prefix,
                std::process::id(),
                TEMP_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            );
            path.push(unique);
            std::fs::write(&path, content).map_err(env_error)?;
            Ok(path.to_string_lossy().into_owned())
        })
    }
}
/// Shared local path resolution. Tool-specific @/space normalization happens before this.
pub(crate) fn absolute_local_path(path: &Path, cwd: &Path) -> EnvResult<PathBuf> {
    let text = path.to_string_lossy();
    let expanded =
        if text == "~" || text.starts_with("~/") || (cfg!(windows) && text.starts_with("~\\")) {
            let home = std::env::var_os(if cfg!(windows) { "USERPROFILE" } else { "HOME" })
                .ok_or_else(|| EnvError::new("home directory is unavailable"))?;
            PathBuf::from(home).join(text.get(2..).unwrap_or(""))
        } else if text.starts_with("file://") {
            url::Url::parse(&text)
                .map_err(env_error)?
                .to_file_path()
                .map_err(|_| EnvError::new("invalid file URL"))?
        } else {
            path.to_owned()
        };
    let absolute = if expanded.is_absolute() {
        expanded
    } else {
        cwd.join(expanded)
    };
    let mut normalized = PathBuf::new();
    for component in absolute.components() {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                normalized.pop();
            }
            component => normalized.push(component.as_os_str()),
        }
    }
    Ok(normalized)
}

struct ProcessReaders(Vec<tokio::task::JoinHandle<()>>);
impl ProcessReaders {
    async fn stop(&mut self) {
        for reader in self.0.drain(..) {
            reader.abort();
            let _ = reader.await;
        }
    }
}
impl Drop for ProcessReaders {
    fn drop(&mut self) {
        for reader in &self.0 {
            reader.abort();
        }
    }
}
fn spawn_process_reader(
    mut stream: impl tokio::io::AsyncRead + Unpin + Send + 'static,
    is_stderr: bool,
    send: tokio::sync::mpsc::Sender<EnvResult<(bool, Vec<u8>)>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        use tokio::io::AsyncReadExt;
        let mut buffer = [0u8; 8192];
        loop {
            match stream.read(&mut buffer).await {
                Ok(0) => break,
                Ok(count) => {
                    if send
                        .send(Ok((is_stderr, buffer[..count].to_vec())))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                Err(error) => {
                    let _ = send.send(Err(env_error(error))).await;
                    break;
                }
            }
        }
    })
}
fn kill_process_tree(child: &mut tokio::process::Child, pid: u32) {
    #[cfg(unix)]
    {
        let _ = std::process::Command::new("kill")
            .args(["-KILL", "--", &format!("-{pid}")])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
    }
    #[cfg(windows)]
    {
        let _ = std::process::Command::new("taskkill")
            .args(["/PID", &pid.to_string(), "/T", "/F"])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
    }
    let _ = child.start_kill();
}

static TEMP_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Truncation metadata retained in tool details.
#[derive(Clone, Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Truncation {
    /// Whether truncation occurred.
    pub truncated: bool,
    /// Limit that won.
    pub truncated_by: Option<&'static str>,
    /// Total lines.
    pub total_lines: usize,
    /// Total bytes.
    pub total_bytes: usize,
    /// Output lines.
    pub output_lines: usize,
    /// Output bytes.
    pub output_bytes: usize,
    /// First line exceeded byte limit.
    pub first_line_exceeds_limit: bool,
    /// Last line was partial.
    pub last_line_partial: bool,
    /// Maximum lines.
    pub max_lines: usize,
    /// Maximum bytes.
    pub max_bytes: usize,
}

fn split_lines(content: &str) -> Vec<&str> {
    if content.is_empty() {
        return Vec::new();
    }
    let mut lines: Vec<_> = content.split('\n').collect();
    if content.ends_with('\n') {
        lines.pop();
    }
    lines
}
pub fn truncate_head(content: &str) -> (String, Truncation) {
    truncate_head_lines(content, DEFAULT_MAX_LINES)
}
fn search_line_limit() -> usize {
    usize::try_from(9_007_199_254_740_991_u64).unwrap_or(usize::MAX)
}
fn truncate_head_lines(content: &str, max_lines: usize) -> (String, Truncation) {
    let lines = split_lines(content);
    let bytes = content.len();
    if lines.len() <= max_lines && bytes <= DEFAULT_MAX_BYTES {
        return (
            content.to_owned(),
            Truncation {
                truncated: false,
                truncated_by: None,
                total_lines: lines.len(),
                total_bytes: bytes,
                output_lines: lines.len(),
                output_bytes: bytes,
                first_line_exceeds_limit: false,
                last_line_partial: false,
                max_lines,
                max_bytes: DEFAULT_MAX_BYTES,
            },
        );
    }
    if lines
        .first()
        .is_some_and(|line| line.len() > DEFAULT_MAX_BYTES)
    {
        return (
            String::new(),
            Truncation {
                truncated: true,
                truncated_by: Some("bytes"),
                total_lines: lines.len(),
                total_bytes: bytes,
                output_lines: 0,
                output_bytes: 0,
                first_line_exceeds_limit: true,
                last_line_partial: false,
                max_lines,
                max_bytes: DEFAULT_MAX_BYTES,
            },
        );
    }
    let mut out = Vec::new();
    let mut size = 0;
    let mut by = "lines";
    for line in lines.iter().take(max_lines) {
        let extra = line.len() + usize::from(!out.is_empty());
        if size + extra > DEFAULT_MAX_BYTES {
            by = "bytes";
            break;
        }
        out.push(*line);
        size += extra;
    }
    let text = out.join("\n");
    (
        text.clone(),
        Truncation {
            truncated: true,
            truncated_by: Some(by),
            total_lines: lines.len(),
            total_bytes: bytes,
            output_lines: out.len(),
            output_bytes: text.len(),
            first_line_exceeds_limit: false,
            last_line_partial: false,
            max_lines,
            max_bytes: DEFAULT_MAX_BYTES,
        },
    )
}
pub fn truncate_tail(content: &str) -> (String, Truncation) {
    let lines = split_lines(content);
    let bytes = content.len();
    if lines.len() <= DEFAULT_MAX_LINES && bytes <= DEFAULT_MAX_BYTES {
        return (
            content.to_owned(),
            Truncation {
                truncated: false,
                truncated_by: None,
                total_lines: lines.len(),
                total_bytes: bytes,
                output_lines: lines.len(),
                output_bytes: bytes,
                first_line_exceeds_limit: false,
                last_line_partial: false,
                max_lines: DEFAULT_MAX_LINES,
                max_bytes: DEFAULT_MAX_BYTES,
            },
        );
    }
    let mut out = Vec::new();
    let mut size = 0;
    let mut by = "lines";
    let mut partial = false;
    for line in lines.iter().rev().take(DEFAULT_MAX_LINES) {
        let extra = line.len() + usize::from(!out.is_empty());
        if size + extra > DEFAULT_MAX_BYTES {
            by = "bytes";
            if out.is_empty() {
                let target = line.len().saturating_sub(DEFAULT_MAX_BYTES);
                let start = line
                    .char_indices()
                    .find(|(index, _)| *index >= target)
                    .map_or(0, |(index, _)| index);
                out.push(&line[start..]);
                partial = true;
            }
            break;
        }
        out.push(*line);
        size += extra;
    }
    out.reverse();
    let text = out.join("\n");
    (
        text.clone(),
        Truncation {
            truncated: true,
            truncated_by: Some(by),
            total_lines: lines.len(),
            total_bytes: bytes,
            output_lines: out.len(),
            output_bytes: text.len(),
            first_line_exceeds_limit: false,
            last_line_partial: partial,
            max_lines: DEFAULT_MAX_LINES,
            max_bytes: DEFAULT_MAX_BYTES,
        },
    )
}
fn details(value: serde_json::Value) -> JsonValue {
    serde_json::from_value(value).unwrap_or(JsonValue::Null)
}
fn text_result(text: impl Into<String>, detail: Option<serde_json::Value>) -> ToolOutput {
    ToolOutput {
        content: vec![InputContent::Text(TextContent {
            content_type: TextContentType::Text,
            text: text.into(),
            text_signature: None,
        })],
        details: detail.map(details),
        usage: None,
        terminate: None,
        structured_content: None,
        is_error: None,
    }
}
fn image_result(text: String, data: String, mime_type: String) -> ToolOutput {
    ToolOutput {
        content: vec![
            InputContent::Text(TextContent {
                content_type: TextContentType::Text,
                text: text.clone(),
                text_signature: None,
            }),
            InputContent::Image(ImageContent {
                content_type: ImageContentType::Image,
                data: data.clone(),
                mime_type: mime_type.clone(),
            }),
        ],
        details: None,
        usage: None,
        terminate: None,
        structured_content: Some(details(
            json!({"type":"image", "data":data, "mimeType":mime_type, "note":text}),
        )),
        is_error: None,
    }
}
fn tool_error(message: impl Into<String>) -> EnvError {
    EnvError::new(message)
}
fn parse_obj(args: JsonValue) -> Result<JsonObject, EnvError> {
    match args {
        JsonValue::Object(o) => Ok(o),
        _ => Err(tool_error("tool arguments must be an object")),
    }
}
fn arg_string(obj: &JsonObject, key: &str) -> Result<String, EnvError> {
    obj.as_map()
        .get(key)
        .and_then(|v| {
            if let JsonValue::String(s) = v {
                Some(s.clone())
            } else {
                None
            }
        })
        .ok_or_else(|| tool_error(format!("missing string argument: {key}")))
}
fn arg_u64(obj: &JsonObject, key: &str) -> Option<u64> {
    obj.as_map().get(key).and_then(|v| {
        if let JsonValue::Number(n) = v {
            serde_json::to_value(n).ok()?.as_u64()
        } else {
            None
        }
    })
}
fn arg_f64(obj: &JsonObject, key: &str) -> Option<f64> {
    obj.as_map()
        .get(key)
        .and_then(|value| serde_json::to_value(value).ok()?.as_f64())
}
fn arg_bool(obj: &JsonObject, key: &str) -> bool {
    obj.as_map()
        .get(key)
        .and_then(|v| {
            if let JsonValue::Bool(b) = v {
                Some(*b)
            } else {
                None
            }
        })
        .unwrap_or(false)
}
fn descriptor(name: &str, _label: &str, description: &str, schema: serde_json::Value) -> Tool {
    Tool {
        name: name.parse().unwrap(),
        description: description.into(),
        parameters: serde_json::from_value(schema).unwrap(),
        constrained_sampling: Some(pi_ai::ConstrainedSampling::Config(
            pi_ai::ConstrainedSamplingConfig::JsonSchema {
                strict: pi_ai::JsonSchemaStrictness::Prefer,
            },
        )),
    }
}
fn check_cancelled(cancel: &CancellationToken) -> Result<(), EnvError> {
    if cancel.is_cancelled() {
        Err(tool_error("Operation aborted"))
    } else {
        Ok(())
    }
}
fn update(progress: &ToolOutputUpdate, result: ToolOutput) {
    progress(result);
}

async fn resolve_read_path(env: &Arc<dyn ExecutionEnv>, raw: &str) -> EnvResult<String> {
    use unicode_normalization::UnicodeNormalization;
    let path = env.absolute_path(raw).await?;
    if env.exists(&path).await? {
        return Ok(path);
    }
    let am_pm = Regex::new(r"(?i) (AM|PM)\.")
        .expect("static screenshot pattern")
        .replace_all(&path, "\u{202f}$1.")
        .into_owned();
    let nfd: String = path.nfd().collect();
    for variant in [
        am_pm,
        nfd.clone(),
        path.replace('\'', "’"),
        nfd.replace('\'', "’"),
    ] {
        if variant != path && env.exists(&variant).await? {
            return Ok(variant);
        }
    }
    Ok(path)
}

fn format_size(bytes: usize) -> String {
    if bytes < 1024 {
        format!("{bytes}B")
    } else if bytes < 1024 * 1024 {
        format!("{:.1}KB", bytes as f64 / 1024.0)
    } else {
        format!("{:.1}MB", bytes as f64 / (1024.0 * 1024.0))
    }
}

fn image_mime(bytes: &[u8]) -> Option<&'static str> {
    if bytes.starts_with(&[0xff, 0xd8, 0xff]) {
        Some("image/jpeg")
    } else if bytes.starts_with(&[0x89, b'P', b'N', b'G', b'\r', b'\n', 0x1a, b'\n']) {
        Some("image/png")
    } else if bytes.starts_with(b"GIF") {
        Some("image/gif")
    } else if bytes.starts_with(b"RIFF") && bytes.get(8..12) == Some(b"WEBP") {
        Some("image/webp")
    } else if bytes.starts_with(b"BM") && bytes.len() >= 26 {
        Some("image/bmp")
    } else {
        None
    }
}

fn lock_for(
    env: &Arc<dyn ExecutionEnv>,
    path: &str,
) -> BoxFuture<'static, EnvResult<MutationGuard>> {
    let env = Arc::clone(env);
    let path = path.to_owned();
    Box::pin(async move {
        let registration = MUTATION_REGISTRATION
            .get_or_init(|| tokio::sync::Mutex::new(()))
            .lock()
            .await;
        let canonical = match env.canonical_path(&path).await {
            Ok(canonical) => canonical,
            Err(error)
                if matches!(
                    error.io_kind,
                    Some(std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory)
                ) =>
            {
                path.clone()
            }
            Err(error) => return Err(error),
        };
        let locks = MUTATION_LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
        let lock = {
            let mut map = locks.lock().unwrap();
            let lock = map
                .get(&canonical)
                .and_then(Weak::upgrade)
                .unwrap_or_else(|| Arc::new(tokio::sync::Mutex::new(())));
            map.insert(canonical.clone(), Arc::downgrade(&lock));
            lock
        };
        drop(registration);
        let guard = lock.lock_owned().await;
        Ok(MutationGuard {
            guard: Some(guard),
            key: canonical,
        })
    })
}
struct MutationGuard {
    guard: Option<tokio::sync::OwnedMutexGuard<()>>,
    key: String,
}
impl Drop for MutationGuard {
    fn drop(&mut self) {
        drop(self.guard.take());
        if let Some(locks) = MUTATION_LOCKS.get() {
            let mut locks = locks.lock().unwrap();
            if locks
                .get(&self.key)
                .is_some_and(|lock| lock.strong_count() == 0)
            {
                locks.remove(&self.key);
            }
        }
    }
}
static MUTATION_REGISTRATION: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
static MUTATION_LOCKS: OnceLock<Mutex<HashMap<String, Weak<tokio::sync::Mutex<()>>>>> =
    OnceLock::new();

/// Vision normalization preferences for the read operation.
#[derive(Clone, Debug)]
pub struct ReadToolOptions {
    pub auto_resize: bool,
    pub resize: Option<pi_ai::ModelImageResizeOptions>,
    pub supports_images: Option<bool>,
}
impl ReadToolOptions {
    /// Capture model input preferences without retaining a model registry or session.
    pub fn for_model(model: &pi_ai::Model) -> Self {
        Self {
            auto_resize: true,
            resize: model
                .input_limits
                .as_ref()
                .and_then(|limits| limits.images.as_ref())
                .and_then(|images| images.resize.clone()),
            supports_images: Some(model.input.contains(&pi_ai::ModelInput::Image)),
        }
    }
}
impl Default for ReadToolOptions {
    fn default() -> Self {
        Self {
            auto_resize: true,
            resize: None,
            supports_images: None,
        }
    }
}

struct ReadTool {
    descriptor: Tool,
    options: ReadToolOptions,
}
impl ReadTool {
    fn new() -> Self {
        Self::with_options(ReadToolOptions::default())
    }
    fn with_options(options: ReadToolOptions) -> Self {
        Self {
            options,
            descriptor: descriptor(
                "read",
                "read",
                "Read file contents, including supported images. Text output is truncated to 2000 lines or 50KB; use offset and limit for large files.",
                json!({"type":"object","properties":{"path":{"type":"string"},"offset":{"type":"number"},"limit":{"type":"number"}},"required":["path"]}),
            ),
        }
    }
}
impl CodingTool for ReadTool {
    fn output_schema(&self) -> Option<pi_ai::JsonSchema> {
        Some(serde_json::from_value(json!({"anyOf":[{"type":"string"},{"type":"object","properties":{"type":{"const":"image"},"data":{"type":"string"},"mimeType":{"type":"string"},"note":{"type":"string"}},"required":["type","data","mimeType","note"]}]})).expect("static output schema"))
    }
    fn descriptor(&self) -> &Tool {
        &self.descriptor
    }
    fn prepare_arguments(&self, args: JsonValue) -> Result<JsonObject, EnvError> {
        parse_obj(args)
    }
    fn execute<'a>(
        &'a self,
        p: JsonObject,
        c: &'a CancellationToken,
        progress: &'a ToolOutputUpdate,
        ctx: &'a ExecutionToolContext,
    ) -> BoxFuture<'a, Result<ToolOutput, EnvError>> {
        Box::pin(async move {
            check_cancelled(c)?;
            let raw = arg_string(&p, "path")?;
            let path = resolve_read_path(&ctx.env, &raw).await?;
            let bytes = ctx
                .env
                .read_binary_file(&path)
                .await
                .map_err(|e| tool_error(e.message))?;
            check_cancelled(c)?;
            if let Some(mime) = image_mime(&bytes) {
                if let Some(processed) = ctx
                    .env
                    .process_image(&bytes, mime, &self.options)
                    .await
                    .map_err(|e| tool_error(e.message))?
                {
                    check_cancelled(c)?;
                    let mut hints = if processed.hints.is_empty() {
                        String::new()
                    } else {
                        format!("\n{}", processed.hints.join("\n"))
                    };
                    if self.options.supports_images == Some(false) {
                        hints.push_str("\n[Current model does not support images. The image will be omitted from this request.]");
                    }
                    return Ok(image_result(
                        format!("Read image file [{}]{}", processed.mime_type, hints),
                        processed.data,
                        processed.mime_type,
                    ));
                }
                let reason = if mime == "image/bmp" {
                    "[Image omitted: could not be converted to a supported inline image format.]"
                } else {
                    "[Image omitted: could not be resized below the inline image size limit.]"
                };
                let mut note = format!("Read image file [{mime}]\n{reason}");
                if self.options.supports_images == Some(false) {
                    note.push_str("\n[Current model does not support images. The image will be omitted from this request.]");
                }
                let mut result = text_result(note.clone(), None);
                result.structured_content = Some(JsonValue::String(note));
                return Ok(result);
            }
            let text = String::from_utf8_lossy(&bytes);
            let lines: Vec<_> = text.split('\n').collect();
            let start = arg_u64(&p, "offset").unwrap_or(1).max(1) as usize - 1;
            if start >= lines.len() {
                return Err(tool_error(format!(
                    "Offset {} is beyond end of file ({} lines total)",
                    start + 1,
                    lines.len()
                )));
            }
            let limit = arg_u64(&p, "limit").map(|v| v as usize);
            let end = limit
                .map(|n| start.saturating_add(n).min(lines.len()))
                .unwrap_or(lines.len());
            let selected = lines[start..end].join("\n");
            let (mut out, tr) = truncate_head(&selected);
            let mut detail = None;
            if tr.first_line_exceeds_limit {
                out = format!(
                    "[Line {} is {}, exceeds 50.0KB limit. Use bash: sed -n '{}p' {} | head -c 51200]",
                    start + 1,
                    format_size(lines[start].len()),
                    start + 1,
                    raw
                );
                detail = Some(json!({"truncation":tr}));
            } else if tr.truncated {
                let shown = start + tr.output_lines;
                out.push_str(&format!(
                    "\n\n[Showing lines {}-{} of {}{}. Use offset={} to continue.]",
                    start + 1,
                    shown,
                    lines.len(),
                    if tr.truncated_by == Some("bytes") {
                        " (50.0KB limit)"
                    } else {
                        ""
                    },
                    shown + 1
                ));
                detail = Some(json!({"truncation":tr}));
            } else if end < lines.len() {
                out.push_str(&format!(
                    "\n\n[{} more lines in file. Use offset={} to continue.]",
                    lines.len() - end,
                    end + 1
                ));
            }
            update(progress, text_result("", None));
            let mut result = text_result(out.clone(), detail);
            result.structured_content = Some(JsonValue::String(out));
            Ok(result)
        })
    }
}

struct WriteTool {
    descriptor: Tool,
}
impl WriteTool {
    fn new() -> Self {
        Self {
            descriptor: descriptor(
                "write",
                "write",
                "Write content to a file, creating parent directories.",
                json!({"type":"object","properties":{"path":{"type":"string"},"content":{"type":"string"}},"required":["path","content"]}),
            ),
        }
    }
}
impl CodingTool for WriteTool {
    fn descriptor(&self) -> &Tool {
        &self.descriptor
    }
    fn prepare_arguments(&self, args: JsonValue) -> Result<JsonObject, EnvError> {
        parse_obj(args)
    }
    fn execute<'a>(
        &'a self,
        p: JsonObject,
        c: &'a CancellationToken,
        _: &'a ToolOutputUpdate,
        ctx: &'a ExecutionToolContext,
    ) -> BoxFuture<'a, Result<ToolOutput, EnvError>> {
        Box::pin(async move {
            check_cancelled(c)?;
            let raw = arg_string(&p, "path")?;
            let content = arg_string(&p, "content")?;
            let path = ctx
                .env
                .absolute_path(&raw)
                .await
                .map_err(|e| tool_error(e.message))?;
            let _guard = lock_for(&ctx.env, &path)
                .await
                .map_err(|e| tool_error(e.message))?;
            check_cancelled(c)?;
            ctx.env
                .write_file(&path, content.as_bytes())
                .await
                .map_err(|e| tool_error(e.message))?;
            check_cancelled(c)?;
            Ok(text_result(
                format!("Successfully wrote {} bytes to {}", content.len(), raw),
                None,
            ))
        })
    }
}

#[derive(Clone)]
struct EditTool {
    descriptor: Tool,
}
impl EditTool {
    fn new() -> Self {
        Self {
            descriptor: descriptor(
                "edit",
                "edit",
                "Edit a file using exact, unique, non-overlapping text replacements.",
                json!({"type":"object","properties":{"path":{"type":"string"},"edits":{"type":"array","items":{"type":"object","properties":{"oldText":{"type":"string"},"newText":{"type":"string"}},"required":["oldText","newText"]}}},"required":["path","edits"]}),
            ),
        }
    }
}
fn json_array_edits(obj: &JsonObject) -> Result<Vec<(String, String)>, EnvError> {
    let v = obj
        .as_map()
        .get("edits")
        .ok_or_else(|| tool_error("edits must contain at least one replacement"))?;
    let JsonValue::Array(items) = v else {
        return Err(tool_error("edits must contain at least one replacement"));
    };
    if items.is_empty() {
        return Err(tool_error("edits must contain at least one replacement"));
    };
    items
        .iter()
        .map(|v| {
            let JsonValue::Object(o) = v else {
                return Err(tool_error("invalid edit"));
            };
            Ok((arg_string(o, "oldText")?, arg_string(o, "newText")?))
        })
        .collect()
}

fn single_edit_value(value: &JsonValue) -> bool {
    matches!(value, JsonValue::Object(object)
        if matches!(object.as_map().get("oldText"), Some(JsonValue::String(_)))
        && matches!(object.as_map().get("newText"), Some(JsonValue::String(_))))
}

fn normalize_edit_arguments(args: JsonValue) -> Result<JsonObject, EnvError> {
    let mut object = parse_obj(args)?.into_map();
    if let Some(JsonValue::String(encoded)) = object.get("edits") {
        if let Ok(value) = serde_json::from_str::<serde_json::Value>(encoded) {
            let parsed: JsonValue =
                serde_json::from_value(value).map_err(|e| tool_error(e.to_string()))?;
            let parsed = if single_edit_value(&parsed) {
                JsonValue::Array(vec![parsed])
            } else {
                parsed
            };
            object.insert("edits".to_owned(), parsed);
        }
    } else if let Some(value) = object.get("edits")
        && single_edit_value(value)
    {
        object.insert("edits".to_owned(), JsonValue::Array(vec![value.clone()]));
    }
    let legacy = match (object.remove("oldText"), object.remove("newText")) {
        (Some(JsonValue::String(old_text)), Some(JsonValue::String(new_text))) => {
            Some((old_text, new_text))
        }
        (old, new) => {
            if let Some(value) = old {
                object.insert("oldText".to_owned(), value);
            }
            if let Some(value) = new {
                object.insert("newText".to_owned(), value);
            }
            None
        }
    };
    if let Some((old_text, new_text)) = legacy {
        let edit = JsonValue::Object(JsonObject::new(BTreeMap::from([
            ("oldText".to_owned(), JsonValue::String(old_text)),
            ("newText".to_owned(), JsonValue::String(new_text)),
        ])));
        match object.get_mut("edits") {
            Some(JsonValue::Array(edits)) => edits.push(edit),
            _ => {
                object.insert("edits".to_owned(), JsonValue::Array(vec![edit]));
            }
        }
    }
    Ok(JsonObject::new(object))
}
impl CodingTool for EditTool {
    fn descriptor(&self) -> &Tool {
        &self.descriptor
    }
    fn prepare_arguments(&self, args: JsonValue) -> Result<JsonObject, EnvError> {
        normalize_edit_arguments(args)
    }
    fn execute<'a>(
        &'a self,
        p: JsonObject,
        c: &'a CancellationToken,
        _progress: &'a ToolOutputUpdate,
        ctx: &'a ExecutionToolContext,
    ) -> BoxFuture<'a, Result<ToolOutput, EnvError>> {
        Box::pin(async move {
            check_cancelled(c)?;
            let raw = arg_string(&p, "path")?;
            let edits = json_array_edits(&p)?;
            let path = ctx
                .env
                .absolute_path(&raw)
                .await
                .map_err(|e| tool_error(e.message))?;
            let _guard = lock_for(&ctx.env, &path)
                .await
                .map_err(|e| tool_error(e.message))?;
            check_cancelled(c)?;
            let original =
                ctx.env.read_text_file(&path).await.map_err(|e| {
                    tool_error(format!("Could not edit file: {raw}. {}", e.message))
                })?;
            check_cancelled(c)?;
            let bom = original.strip_prefix('\u{feff}').map_or("", |_| "\u{feff}");
            let body = original.strip_prefix('\u{feff}').unwrap_or(&original);
            let crlf = body
                .find("\r\n")
                .is_some_and(|crlf| body.find('\n').is_some_and(|lf| crlf < lf));
            let normalized = body.replace("\r\n", "\n").replace('\r', "\n");
            let (base, new) = edits::apply_edits(&normalized, &edits, &raw).map_err(tool_error)?;
            let final_body = if crlf {
                new.replace('\n', "\r\n")
            } else {
                new.clone()
            };
            check_cancelled(c)?;
            ctx.env
                .write_file(&path, format!("{bom}{final_body}").as_bytes())
                .await
                .map_err(|e| tool_error(e.message))?;
            check_cancelled(c)?;
            let (diff, patch, first_changed_line) = edits::generate_diff(&raw, &base, &new);
            Ok(text_result(
                format!("Successfully replaced {} block(s) in {}.", edits.len(), raw),
                Some(json!({"diff": diff, "patch": patch, "firstChangedLine":first_changed_line})),
            ))
        })
    }
}

/// Explicit shell customizations. Host hooks can additionally wrap ExecutionEnv.
#[derive(Clone, Debug, Default)]
pub struct ShellToolOptions {
    pub command_prefix: Option<String>,
    pub shell_path: Option<PathBuf>,
    pub environment: BTreeMap<String, String>,
}

struct ShellTool {
    descriptor: Tool,
    options: ShellToolOptions,
    shell: &'static str,
}
impl ShellTool {
    fn new(name: &str, shell: &'static str) -> Self {
        Self::with_options(name, shell, ShellToolOptions::default())
    }
    fn with_options(name: &str, shell: &'static str, options: ShellToolOptions) -> Self {
        Self {
            options,
            descriptor: descriptor(
                name,
                name,
                &format!(
                    "Execute a {name} command and return stdout and stderr. Output is truncated to the last 2000 lines or 50KB.",
                ),
                json!({"type":"object","properties":{"command":{"type":"string"},"timeout":{"type":"number"}},"required":["command"]}),
            ),
            shell,
        }
    }
}
impl CodingTool for ShellTool {
    fn output_schema(&self) -> Option<pi_ai::JsonSchema> {
        Some(serde_json::from_value(json!({"type":"object","properties":{"output":{"type":"string"},"truncated":{"type":"boolean"},"full_output_path":{"type":"string"},"exit_code":{"type":"number"},"wall_time_seconds":{"type":"number"}},"required":["output","truncated","exit_code","wall_time_seconds"]})).expect("static output schema"))
    }
    fn descriptor(&self) -> &Tool {
        &self.descriptor
    }
    fn prepare_arguments(&self, args: JsonValue) -> Result<JsonObject, EnvError> {
        parse_obj(args)
    }
    fn execute<'a>(
        &'a self,
        p: JsonObject,
        c: &'a CancellationToken,
        progress: &'a ToolOutputUpdate,
        ctx: &'a ExecutionToolContext,
    ) -> BoxFuture<'a, Result<ToolOutput, EnvError>> {
        Box::pin(async move {
            check_cancelled(c)?;
            let command = arg_string(&p, "command")?;
            let command = if let Some(prefix) = self
                .options
                .command_prefix
                .as_ref()
                .filter(|prefix| !prefix.is_empty())
            {
                format!("{prefix}\n{command}")
            } else {
                command
            };
            let command = if self.shell == "powershell" {
                format!(
                    "try {{ [Console]::OutputEncoding=[System.Text.Encoding]::UTF8 }} catch {{}}\n{command}"
                )
            } else {
                command
            };
            let timeout = match p.as_map().get("timeout") {
                None => None,
                Some(_) => {
                    let seconds = arg_f64(&p, "timeout").ok_or_else(|| {
                        tool_error("Invalid timeout: must be a finite number of seconds")
                    })?;
                    if !seconds.is_finite() || seconds <= 0.0 {
                        return Err(tool_error(
                            "Invalid timeout: must be a finite number of seconds",
                        ));
                    }
                    if seconds > 2_147_483.647 {
                        return Err(tool_error(
                            "Invalid timeout: maximum is 2147483.647 seconds",
                        ));
                    }
                    Some(Duration::from_secs_f64(seconds))
                }
            };
            let accumulator = Arc::new(Mutex::new(output::OutputAccumulator::new()));
            let capture = accumulator.clone();
            let accepting_output = Arc::new(output::OutputAcceptance::new());
            let _retirement = accepting_output.retirement();
            let callback_accepting_output = accepting_output.clone();
            let progress_callback = progress.clone();
            update(progress, ToolOutput::default());
            let started = std::time::Instant::now();
            let process = ctx
                .env
                .exec(
                    self.shell,
                    &command,
                    ProcessOptions {
                        cwd: None,
                        timeout,
                        env: self.options.environment.clone(),
                        executable: self.options.shell_path.clone(),
                        abort_signal: Some(c.clone()),
                        on_data: Some(Arc::new(move |bytes| {
                            let Some(_admission) = callback_accepting_output.admit() else {
                                return;
                            };
                            let mut capture = capture.lock().unwrap();
                            capture.append(bytes);
                            let update = if capture.should_update() {
                                let (body, tr) = capture.snapshot();
                                Some(text_result(
                                    body,
                                    tr.truncated.then(
                                        || json!({"truncation":tr,"fullOutputPath":capture.path()}),
                                    ),
                                ))
                            } else {
                                None
                            };
                            drop(capture);
                            if let Some(update) = update {
                                progress_callback(update);
                            }
                        })),
                    },
                )
                .await;
            // Close admission on both success and failure, then yield until every
            // already admitted callback finishes publishing outside internal locks.
            accepting_output.close().await;
            let mut capture = accumulator.lock().unwrap();
            if let Ok(output) = &process {
                // Hosts may return captured output instead of streaming. It is admitted
                // only if they did not already publish bytes through on_data.
                if capture.snapshot().1.total_bytes == 0 {
                    let combined = output
                        .merged
                        .clone()
                        .unwrap_or_else(|| format!("{}{}", output.stdout, output.stderr));
                    capture.append(combined.as_bytes());
                }
            }
            capture.finish()?;
            let (mut body, tr) = capture.snapshot();
            let mut detail = None;
            if tr.truncated {
                let full = capture
                    .path()
                    .ok_or_else(|| tool_error("truncated output has no spill file"))?;
                let start = tr.total_lines.saturating_sub(tr.output_lines) + 1;
                let notice = if tr.last_line_partial {
                    format!(
                        "Showing last {} of line {} (line is {}).",
                        format_size(tr.output_bytes),
                        tr.total_lines,
                        format_size(capture.last_line_bytes())
                    )
                } else if tr.truncated_by == Some("lines") {
                    format!(
                        "Showing lines {start}-{} of {}.",
                        tr.total_lines, tr.total_lines
                    )
                } else {
                    format!(
                        "Showing lines {start}-{} of {} (50.0KB limit).",
                        tr.total_lines, tr.total_lines
                    )
                };
                body.push_str(&format!("\n\n[{notice} Full output: {full}]"));
                detail = Some(json!({"truncation":tr,"fullOutputPath":full}));
            }
            let output = process.map_err(|error| {
                tool_error(if body.is_empty() {
                    error.message.clone()
                } else {
                    format!("{body}\n\n{}", error.message)
                })
            })?;
            if body.is_empty() {
                body = "(no output)".into();
            }
            let (full, truncated) = capture.full_output(1024 * 1024)?;
            let mut structured = json!({"output": full, "truncated": truncated, "exit_code":output.exit_code,"wall_time_seconds":(started.elapsed().as_secs_f64()*10.0).round()/10.0});
            if truncated {
                structured["full_output_path"] = json!(capture.path());
            }
            drop(capture);
            let mut result = text_result(body, detail);
            result.structured_content = Some(details(structured));
            if output.exit_code != 0 {
                if let InputContent::Text(text) = &mut result.content[0] {
                    text.text.push_str(&format!(
                        "\n\nCommand exited with code {}",
                        output.exit_code
                    ));
                }
                result.is_error = Some(true);
            }
            update(progress, result.clone());
            Ok(result)
        })
    }
}

struct LsTool {
    descriptor: Tool,
}
impl LsTool {
    fn new() -> Self {
        Self {
            descriptor: descriptor(
                "ls",
                "ls",
                "List directory contents sorted alphabetically; directories have a trailing slash.",
                json!({"type":"object","properties":{"path":{"type":"string"},"limit":{"type":"integer"}}}),
            ),
        }
    }
}
impl CodingTool for LsTool {
    fn descriptor(&self) -> &Tool {
        &self.descriptor
    }
    fn prepare_arguments(&self, args: JsonValue) -> Result<JsonObject, EnvError> {
        parse_obj(args)
    }
    fn execute<'a>(
        &'a self,
        p: JsonObject,
        c: &'a CancellationToken,
        _: &'a ToolOutputUpdate,
        ctx: &'a ExecutionToolContext,
    ) -> BoxFuture<'a, Result<ToolOutput, EnvError>> {
        Box::pin(async move {
            check_cancelled(c)?;
            let raw = p
                .as_map()
                .get("path")
                .and_then(|v| {
                    if let JsonValue::String(s) = v {
                        Some(s.as_str())
                    } else {
                        None
                    }
                })
                .unwrap_or(".");
            let limit = arg_u64(&p, "limit").unwrap_or(500) as usize;
            let path = ctx
                .env
                .absolute_path(raw)
                .await
                .map_err(|e| tool_error(e.message))?;
            let info = ctx
                .env
                .file_info(&path)
                .await
                .map_err(|e| tool_error(e.message))?;
            if !info.is_dir {
                return Err(tool_error(format!("Not a directory: {path}")));
            }
            let mut entries = ctx
                .env
                .list_dir(&path)
                .await
                .map_err(|e| tool_error(e.message))?;
            entries.sort_by_key(|e| e.name.to_lowercase());
            let limited = entries.len() > limit;
            let out = entries
                .into_iter()
                .take(limit)
                .map(|e| {
                    if e.is_dir {
                        format!("{}/", e.name)
                    } else {
                        e.name
                    }
                })
                .collect::<Vec<_>>()
                .join("\n");
            if out.is_empty() {
                return Ok(text_result("(empty directory)", None));
            }
            let (mut out, truncation) = truncate_head_lines(&out, search_line_limit());
            let mut notices = Vec::new();
            if limited {
                notices.push(format!(
                    "{} entries limit reached. Use limit={} for more",
                    limit,
                    limit * 2
                ));
            }
            if truncation.truncated {
                notices.push("50.0KB limit reached".to_owned());
            }
            if !notices.is_empty() {
                out.push_str(&format!("\n\n[{}]", notices.join(". ")));
            }
            let mut detail = serde_json::Map::new();
            if limited {
                detail.insert("entryLimitReached".into(), json!(limit));
            }
            if truncation.truncated {
                detail.insert("truncation".into(), json!(truncation));
            }
            let detail = (!detail.is_empty()).then_some(serde_json::Value::Object(detail));
            Ok(text_result(out, detail))
        })
    }
}

struct GrepTool {
    descriptor: Tool,
}
impl GrepTool {
    fn new() -> Self {
        Self {
            descriptor: descriptor(
                "grep",
                "grep",
                "Search file contents for regex or literal patterns; respects common ignored directories.",
                json!({"type":"object","properties":{"pattern":{"type":"string"},"path":{"type":"string"},"glob":{"type":"string"},"ignoreCase":{"type":"boolean"},"literal":{"type":"boolean"},"context":{"type":"integer"},"limit":{"type":"integer"}},"required":["pattern"]}),
            ),
        }
    }
}
fn glob_matches(pattern: &str, name: &str) -> bool {
    glob_matches_case(pattern, name, false)
}
fn glob_matches_case(pattern: &str, name: &str, insensitive: bool) -> bool {
    let mut expression = String::from("^");
    let chars: Vec<_> = pattern.chars().collect();
    let mut index = 0;
    let mut braces = 0;
    while index < chars.len() {
        match chars[index] {
            '*' if chars.get(index + 1) == Some(&'*') => {
                index += 1;
                if chars.get(index + 1) == Some(&'/') {
                    expression.push_str("(?:.*/)?");
                    index += 1;
                } else {
                    expression.push_str(".*");
                }
            }
            '*' => expression.push_str("[^/]*"),
            '?' => expression.push_str("[^/]"),
            '[' => {
                if let Some(end) = chars[index + 1..]
                    .iter()
                    .position(|ch| *ch == ']')
                    .map(|end| index + 1 + end)
                {
                    expression.push('[');
                    let mut start = index + 1;
                    if chars.get(start) == Some(&'!') {
                        expression.push('^');
                        start += 1;
                    }
                    for ch in &chars[start..end] {
                        if *ch == '\\' {
                            expression.push('\\');
                        }
                        expression.push(*ch);
                    }
                    expression.push(']');
                    index = end;
                } else {
                    expression.push_str("\\[");
                }
            }
            '{' => {
                expression.push_str("(?:");
                braces += 1;
            }
            '}' if braces > 0 => {
                expression.push(')');
                braces -= 1;
            }
            ',' if braces > 0 => expression.push('|'),
            '\\' => {
                index += 1;
                if let Some(ch) = chars.get(index) {
                    expression.push_str(&regex::escape(&ch.to_string()));
                }
            }
            ch => expression.push_str(&regex::escape(&ch.to_string())),
        }
        index += 1;
    }
    expression.push('$');
    regex::RegexBuilder::new(&expression)
        .case_insensitive(insensitive)
        .build()
        .map(|regex| regex.is_match(name))
        .unwrap_or(false)
}
fn find_glob_matches(pattern: &str, file: &FileInfo) -> bool {
    let insensitive = !pattern.chars().any(|ch| ch.is_uppercase());
    if pattern.contains('/') {
        let effective = if pattern.starts_with('/') || pattern.starts_with("**/") || pattern == "**"
        {
            pattern.into()
        } else {
            format!("**/{pattern}")
        };
        glob_matches_case(&effective, &file.path.replace('\\', "/"), insensitive)
    } else {
        glob_matches_case(pattern, &file.name, insensitive)
    }
}

fn ignored(path: &str, is_dir: bool, patterns: &[(String, String)]) -> bool {
    let mut bases = Vec::new();
    for (base, _) in patterns {
        if !bases.contains(base) {
            bases.push(base.clone());
        }
    }
    let mut ignored = false;
    for base in bases {
        if !Path::new(path).starts_with(&base) {
            continue;
        }
        let mut builder = ignore::gitignore::GitignoreBuilder::new(&base);
        for (scope, line) in patterns {
            if scope == &base {
                let _ = builder.add_line(None, line);
            }
        }
        if let Ok(matcher) = builder.build() {
            let matched = matcher.matched_path_or_any_parents(path, is_dir);
            if matched.is_ignore() {
                ignored = true;
            } else if matched.is_whitelist() {
                ignored = false;
            }
        }
    }
    ignored
}
pub(crate) fn resource_path_ignored(path: &Path, rules: &[(String, String)]) -> bool {
    ignored(&path.to_string_lossy(), path.is_dir(), rules)
}

fn collect_files(
    env: &Arc<dyn ExecutionEnv>,
    root: &str,
    options: SearchOptions,
) -> BoxFuture<'static, EnvResult<Vec<FileInfo>>> {
    let env = Arc::clone(env);
    let root = root.to_owned();
    Box::pin(async move {
        if let Some(walker) = env.walk_files(&root, options.clone()) {
            return walker.await;
        }
        let mut files = Vec::new();
        let mut stack = vec![(root.clone(), Vec::<(String, String)>::new())];
        let mut visited = std::collections::BTreeSet::new();
        while let Some((dir, mut patterns)) = stack.pop() {
            check_cancelled(&options.cancellation)?;
            let canonical = env
                .canonical_path(&dir)
                .await
                .unwrap_or_else(|_| dir.clone());
            if !visited.insert(canonical) {
                continue;
            }
            if dir != root && env.exists(&format!("{dir}/.git")).await? {
                patterns.clear();
            }
            if let Ok(ignore) = env.read_text_file(&format!("{dir}/.gitignore")).await {
                patterns.extend(
                    ignore
                        .lines()
                        .map(|pattern| (dir.clone(), pattern.to_owned())),
                );
            }
            for entry in env.list_dir(&dir).await? {
                if entry.name == ".git" {
                    continue;
                }
                if !ignored(&entry.path, entry.is_dir, &patterns) {
                    if entry.is_dir {
                        if options.include_directories {
                            files.push(entry.clone());
                        }
                        stack.push((entry.path, patterns.clone()));
                    } else {
                        files.push(entry);
                    }
                }
            }
        }
        Ok(files)
    })
}

fn truncate_match_line(line: &str) -> (String, bool) {
    let mut units = line.encode_utf16();
    let prefix: Vec<_> = units.by_ref().take(GREP_MAX_LINE_LENGTH).collect();
    let text = String::from_utf16_lossy(&prefix);
    let truncated = units.next().is_some();
    if truncated {
        (format!("{text}... [truncated]"), true)
    } else {
        (text, false)
    }
}

impl CodingTool for GrepTool {
    fn descriptor(&self) -> &Tool {
        &self.descriptor
    }
    fn prepare_arguments(&self, args: JsonValue) -> Result<JsonObject, EnvError> {
        parse_obj(args)
    }
    fn execute<'a>(
        &'a self,
        p: JsonObject,
        c: &'a CancellationToken,
        _: &'a ToolOutputUpdate,
        ctx: &'a ExecutionToolContext,
    ) -> BoxFuture<'a, Result<ToolOutput, EnvError>> {
        Box::pin(async move {
            check_cancelled(c)?;
            let pattern = arg_string(&p, "pattern")?;
            let raw = p
                .as_map()
                .get("path")
                .and_then(|v| {
                    if let JsonValue::String(s) = v {
                        Some(s.as_str())
                    } else {
                        None
                    }
                })
                .unwrap_or(".");
            let root = ctx
                .env
                .absolute_path(raw)
                .await
                .map_err(|e| tool_error(e.message))?;
            let limit = arg_u64(&p, "limit").unwrap_or(100).max(1) as usize;
            let literal = arg_bool(&p, "literal");
            let ignore = arg_bool(&p, "ignoreCase");
            let regex = if literal {
                None
            } else {
                let regex_pattern = if ignore {
                    format!("(?i){pattern}")
                } else {
                    pattern.clone()
                };
                Some(Regex::new(&regex_pattern).map_err(|e| tool_error(e.to_string()))?)
            };
            let root_info = ctx
                .env
                .file_info(&root)
                .await
                .map_err(|e| tool_error(e.message))?;
            let search_root = if !root_info.is_dir {
                Path::new(&root)
                    .parent()
                    .unwrap_or(Path::new("."))
                    .to_string_lossy()
                    .into_owned()
            } else {
                root.clone()
            };
            let files = if root_info.is_dir {
                collect_files(
                    &ctx.env,
                    &root,
                    SearchOptions {
                        require_git: true,
                        include_directories: false,
                        cancellation: c.clone(),
                    },
                )
                .await
                .map_err(|e| tool_error(e.message))?
            } else {
                vec![root_info]
            };
            let glob = p.as_map().get("glob").and_then(|v| {
                if let JsonValue::String(s) = v {
                    Some(s.as_str())
                } else {
                    None
                }
            });
            let context = arg_u64(&p, "context").unwrap_or(0) as usize;
            let mut result = Vec::new();
            let mut count = 0;
            let mut lines_truncated = false;
            for file in files {
                if let Some(glob) = glob.filter(|glob| !glob.is_empty()) {
                    let (excluded, pattern) = glob
                        .strip_prefix('!')
                        .map_or((false, glob), |pattern| (true, pattern));
                    let candidate = if pattern.contains('/') {
                        file.path
                            .strip_prefix(&format!("{search_root}/"))
                            .unwrap_or(&file.path)
                    } else {
                        &file.name
                    };
                    if glob_matches(pattern, candidate) == excluded {
                        continue;
                    }
                }
                let text = match ctx.env.read_text_file(&file.path).await {
                    Ok(t) => t,
                    Err(_) => continue,
                };
                let context_lines = (context > 0).then(|| {
                    text.replace("\r\n", "\n")
                        .replace('\r', "\n")
                        .split('\n')
                        .map(str::to_owned)
                        .collect::<Vec<_>>()
                });
                let lines: Vec<_> = text
                    .replace("\r\n", "\n")
                    .split('\n')
                    .map(str::to_owned)
                    .collect();
                for (idx, line) in lines.iter().enumerate() {
                    let matched = if let Some(re) = &regex {
                        re.is_match(line)
                    } else if ignore {
                        line.to_lowercase().contains(&pattern.to_lowercase())
                    } else {
                        line.contains(&pattern)
                    };
                    if !matched {
                        continue;
                    }
                    count += 1;
                    if count > limit {
                        break;
                    }
                    let display_lines = context_lines.as_deref().unwrap_or(&lines);
                    let start = idx.saturating_sub(context);
                    let end = (idx + context + 1).min(display_lines.len());
                    for (n, line) in display_lines.iter().enumerate().take(end).skip(start) {
                        let (shown, was_truncated) = truncate_match_line(&line.replace('\r', ""));
                        let relative = Path::new(&file.path)
                            .strip_prefix(&search_root)
                            .unwrap_or(Path::new(&file.path))
                            .to_string_lossy()
                            .replace('\\', "/");
                        if was_truncated {
                            lines_truncated = true;
                        }
                        result.push(if n == idx {
                            format!("{}:{}: {}", relative, n + 1, shown)
                        } else {
                            format!("{}-{}- {}", relative, n + 1, shown)
                        });
                    }
                }
                if count >= limit {
                    break;
                }
            }
            let raw_output = result.join("\n");
            let (mut out, truncation) = truncate_head_lines(&raw_output, search_line_limit());
            if out.is_empty() {
                out = "No matches found".into()
            }
            let reached = count >= limit;
            let byte_truncated = truncation.truncated;
            let mut notices = Vec::new();
            if reached {
                notices.push(format!(
                    "{limit} matches limit reached. Use limit={} for more, or refine pattern",
                    limit * 2
                ));
            }
            if byte_truncated {
                notices.push("50.0KB limit reached".to_owned());
            }
            if lines_truncated {
                notices.push(format!("Some lines truncated to {GREP_MAX_LINE_LENGTH} chars. Use read tool to see full lines"));
            }
            if !notices.is_empty() && out != "No matches found" {
                out.push_str(&format!("\n\n[{}]", notices.join(". ")));
            }
            let mut detail = serde_json::Map::new();
            if reached {
                detail.insert("matchLimitReached".into(), json!(limit));
            }
            if byte_truncated {
                detail.insert("truncation".into(), json!(truncation));
            }
            if lines_truncated {
                detail.insert("linesTruncated".into(), json!(true));
            }
            let detail = (!detail.is_empty()).then_some(serde_json::Value::Object(detail));
            Ok(text_result(out, detail))
        })
    }
}

struct FindTool {
    descriptor: Tool,
}
impl FindTool {
    fn new() -> Self {
        Self {
            descriptor: descriptor(
                "find",
                "find",
                "Find files by glob pattern, respecting common ignored directories.",
                json!({"type":"object","properties":{"pattern":{"type":"string"},"path":{"type":"string"},"limit":{"type":"integer"}},"required":["pattern"]}),
            ),
        }
    }
}
impl CodingTool for FindTool {
    fn descriptor(&self) -> &Tool {
        &self.descriptor
    }
    fn prepare_arguments(&self, args: JsonValue) -> Result<JsonObject, EnvError> {
        parse_obj(args)
    }
    fn execute<'a>(
        &'a self,
        p: JsonObject,
        c: &'a CancellationToken,
        _: &'a ToolOutputUpdate,
        ctx: &'a ExecutionToolContext,
    ) -> BoxFuture<'a, Result<ToolOutput, EnvError>> {
        Box::pin(async move {
            check_cancelled(c)?;
            let pattern = arg_string(&p, "pattern")?;
            let raw = p
                .as_map()
                .get("path")
                .and_then(|v| {
                    if let JsonValue::String(s) = v {
                        Some(s.as_str())
                    } else {
                        None
                    }
                })
                .unwrap_or(".");
            let root = ctx
                .env
                .absolute_path(raw)
                .await
                .map_err(|e| tool_error(e.message))?;
            let limit = arg_u64(&p, "limit").unwrap_or(1000) as usize;
            let mut files = collect_files(
                &ctx.env,
                &root,
                SearchOptions {
                    require_git: false,
                    include_directories: true,
                    cancellation: c.clone(),
                },
            )
            .await
            .map_err(|e| tool_error(e.message))?;
            files.sort_by(|a, b| a.path.cmp(&b.path));
            let mut paths = Vec::new();
            for file in files {
                let rel = Path::new(&file.path)
                    .strip_prefix(&root)
                    .unwrap_or(Path::new(&file.path))
                    .to_string_lossy()
                    .replace('\\', "/");
                if find_glob_matches(&pattern, &file) {
                    paths.push(rel);
                    if limit > 0 && paths.len() >= limit {
                        break;
                    }
                }
            }
            if paths.is_empty() {
                return Ok(text_result("No files found matching pattern", None));
            }
            let reached = paths.len() >= limit;
            let raw_output = paths.join("\n");
            let (mut out, truncation) = truncate_head_lines(&raw_output, search_line_limit());
            let mut notices = Vec::new();
            if reached {
                notices.push(format!(
                    "{limit} results limit reached. Use limit={} for more, or refine pattern",
                    limit * 2
                ));
            }
            if truncation.truncated {
                notices.push("50.0KB limit reached".to_owned());
            }
            if !notices.is_empty() {
                out.push_str(&format!("\n\n[{}]", notices.join(". ")));
            }
            let mut detail = serde_json::Map::new();
            if reached {
                detail.insert("resultLimitReached".into(), json!(limit));
            }
            if truncation.truncated {
                detail.insert("truncation".into(), json!(truncation));
            }
            let detail = (!detail.is_empty()).then_some(serde_json::Value::Object(detail));
            Ok(text_result(out, detail))
        })
    }
}

/// All built-in coding tool names.
pub const TOOL_NAMES: &[&str] = &[
    "read",
    "write",
    "edit",
    "bash",
    "powershell",
    "grep",
    "find",
    "ls",
];
/// Create a tool by model-visible name.
pub fn coding_tool(name: &str) -> Arc<dyn CodingTool> {
    match name {
        "read" => Arc::new(ReadTool::new()),
        "write" => Arc::new(WriteTool::new()),
        "edit" => Arc::new(EditTool::new()),
        "bash" => Arc::new(ShellTool::new("bash", "bash")),
        "powershell" => Arc::new(ShellTool::new("powershell", "powershell")),
        "grep" => Arc::new(GrepTool::new()),
        "find" => Arc::new(FindTool::new()),
        "ls" => Arc::new(LsTool::new()),
        _ => panic!("unknown coding tool: {name}"),
    }
}
/// Create one built-in Agent tool using explicit environment capabilities.
pub fn create_tool(name: &str, context: ExecutionToolContext) -> AgentTool {
    bind_agent_tool(coding_tool(name), context)
}
/// Create all builtin Agent tools in stable order.
pub fn create_tools(context: ExecutionToolContext) -> Vec<AgentTool> {
    TOOL_NAMES
        .iter()
        .map(|name| create_tool(name, context.clone()))
        .collect()
}
/// Prompt metadata supplied by the built-in tools.
pub fn tool_prompt_metadata(name: &str) -> (&'static str, &'static [&'static str]) {
    match name {
        "read" => (
            "Read file contents",
            &["Use read to examine files instead of cat or sed."],
        ),
        "write" => (
            "Create or overwrite files",
            &["Use write only for new files or complete rewrites."],
        ),
        "edit" => (
            "Make precise file edits with exact text replacement, including multiple disjoint edits in one call",
            &[
                "Use edit for precise changes (edits[].oldText must match exactly)",
                "When changing multiple separate locations in one file, use one edit call with multiple entries in edits[] instead of multiple edit calls",
                "Each edits[].oldText is matched against the original file, not after earlier edits are applied. Do not emit overlapping or nested edits. Merge nearby changes into one edit.",
                "Keep edits[].oldText as small as possible while still being unique in the file. Do not pad with large unchanged regions.",
            ],
        ),
        "bash" => (
            "Execute bash commands (ls, grep, find, etc.)",
            &["You can inspect PI_* environment variables for current model and session details."],
        ),
        "powershell" => (
            "Execute PowerShell commands",
            &["You can inspect PI_* environment variables for current model and session details."],
        ),
        "grep" => (
            "Search file contents for patterns (respects .gitignore)",
            &[],
        ),
        "find" => ("Find files by glob pattern (respects .gitignore)", &[]),
        "ls" => ("List directory contents", &[]),
        _ => ("", &[]),
    }
}

/// Bind the builtin read operation to Agent.
pub fn create_read_tool(context: ExecutionToolContext) -> AgentTool {
    create_tool("read", context)
}
/// Reusable read operation with explicit image preferences.
pub fn read_operation(options: ReadToolOptions) -> Arc<dyn CodingTool> {
    Arc::new(ReadTool::with_options(options))
}
/// Bind a customized read operation to Agent.
pub fn create_read_tool_with_options(
    context: ExecutionToolContext,
    options: ReadToolOptions,
) -> AgentTool {
    bind_agent_tool(read_operation(options), context)
}
/// Reusable shell operation with explicit executable, prefix and environment values.
pub fn shell_operation(powershell: bool, options: ShellToolOptions) -> Arc<dyn CodingTool> {
    let name = if powershell { "powershell" } else { "bash" };
    Arc::new(ShellTool::with_options(name, name, options))
}
/// Bind a customized bash operation to Agent.
pub fn create_bash_tool_with_options(
    context: ExecutionToolContext,
    options: ShellToolOptions,
) -> AgentTool {
    bind_agent_tool(shell_operation(false, options), context)
}
/// Bind a customized PowerShell operation to Agent.
pub fn create_powershell_tool_with_options(
    context: ExecutionToolContext,
    options: ShellToolOptions,
) -> AgentTool {
    bind_agent_tool(shell_operation(true, options), context)
}

/// Bind the builtin write operation to Agent.
pub fn create_write_tool(context: ExecutionToolContext) -> AgentTool {
    create_tool("write", context)
}
/// Bind the builtin edit operation to Agent.
pub fn create_edit_tool(context: ExecutionToolContext) -> AgentTool {
    create_tool("edit", context)
}
/// Bind the builtin bash operation to Agent.
pub fn create_bash_tool(context: ExecutionToolContext) -> AgentTool {
    create_tool("bash", context)
}
/// Bind the builtin powershell operation to Agent.
pub fn create_powershell_tool(context: ExecutionToolContext) -> AgentTool {
    create_tool("powershell", context)
}
/// Bind the builtin grep operation to Agent.
pub fn create_grep_tool(context: ExecutionToolContext) -> AgentTool {
    create_tool("grep", context)
}
/// Bind the builtin find operation to Agent.
pub fn create_find_tool(context: ExecutionToolContext) -> AgentTool {
    create_tool("find", context)
}
/// Bind the builtin ls operation to Agent.
pub fn create_ls_tool(context: ExecutionToolContext) -> AgentTool {
    create_tool("ls", context)
}

/// File paths observed while projecting coding tool calls.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FileOperations {
    /// Paths read by `read`.
    pub read: std::collections::BTreeSet<String>,
    /// Paths written by `write`.
    pub written: std::collections::BTreeSet<String>,
    /// Paths edited by `edit`.
    pub edited: std::collections::BTreeSet<String>,
}

/// Sort and classify paths as read-only or modified, matching compaction output.
pub fn classify_file_operations(operations: &FileOperations) -> (Vec<String>, Vec<String>) {
    let modified: std::collections::BTreeSet<_> = operations
        .written
        .iter()
        .chain(operations.edited.iter())
        .cloned()
        .collect();
    let read_only = operations
        .read
        .iter()
        .filter(|path| !modified.contains(*path))
        .cloned()
        .collect();
    (read_only, modified.into_iter().collect())
}
