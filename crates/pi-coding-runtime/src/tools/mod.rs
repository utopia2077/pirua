//! Coding-agent filesystem and process tools.
//!
//! The tools deliberately depend on the [`ExecutionEnv`] port.  Hosts can
//! inject a deterministic environment while `LocalExecutionEnv` provides the
//! normal local filesystem and shell implementation.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use base64::Engine;
use pi_agent::schema::{
    AgentHarnessTool, AgentTool, AgentToolResult, BoxFuture, Cancellation, ErrorContext,
    ToolExecutionError, ToolProgressSink,
};
use pi_ai::{
    ImageContent, ImageContentType, InputContent, JsonObject, JsonValue, TextContent,
    TextContentType, ToolCallId,
};
use regex::Regex;
use serde_json::json;

pub const DEFAULT_MAX_LINES: usize = 2_000;
pub const DEFAULT_MAX_BYTES: usize = 50 * 1024;
pub const GREP_MAX_LINE_LENGTH: usize = 500;

/// Result of a fallible host operation.
pub type EnvResult<T> = Result<T, EnvError>;

/// Stable environment error returned by an injected host.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnvError {
    /// Human-readable error.
    pub message: String,
}

impl EnvError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
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

/// Process options supplied by a coding tool.
#[derive(Clone, Debug, Default)]
pub struct ProcessOptions {
    /// Working directory.
    pub cwd: Option<String>,
    /// Optional timeout.
    pub timeout: Option<Duration>,
    /// Environment overrides.
    pub env: BTreeMap<String, String>,
    /// Shared cancellation flag a host process runner may observe.
    pub abort_signal: Option<Arc<std::sync::atomic::AtomicBool>>,
}

/// Host port consumed by all coding tools.
pub trait ExecutionEnv: Send + Sync {
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
        _auto_resize: bool,
    ) -> BoxFuture<'a, EnvResult<Option<ProcessedImage>>> {
        Box::pin(async { Ok(None) })
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

/// Tool context captured for a Harness turn.
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
        Self { cwd: cwd.into() }
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
    fn absolute_path<'a>(&'a self, path: &'a str) -> BoxFuture<'a, EnvResult<String>> {
        Box::pin(async move {
            let path = path.replace('\u{00a0}', " ");
            let p = Path::new(&path);
            Ok(if p.is_absolute() {
                p.to_path_buf()
            } else {
                self.cwd.join(p)
            }
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
            let metadata = std::fs::symlink_metadata(p).map_err(env_error)?;
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
                .map_err(env_error)
        })
    }
    fn exec<'a>(
        &'a self,
        shell: &'a str,
        command: &'a str,
        options: ProcessOptions,
    ) -> BoxFuture<'a, EnvResult<ProcessOutput>> {
        Box::pin(async move {
            let cwd = options
                .cwd
                .clone()
                .unwrap_or_else(|| self.cwd.to_string_lossy().into_owned());
            let mut cmd =
                if shell.eq_ignore_ascii_case("powershell") || shell.eq_ignore_ascii_case("pwsh") {
                    let executable = if cfg!(windows) { "powershell" } else { "pwsh" };
                    let mut c = std::process::Command::new(executable);
                    c.args(["-NoProfile", "-NonInteractive", "-Command", command]);
                    c
                } else {
                    let mut c = std::process::Command::new("bash");
                    c.args(["-lc", command]);
                    c
                };
            if let Some(timeout) = options.timeout.filter(|value| !value.is_zero()) {
                #[cfg(unix)]
                {
                    let executable = if shell.eq_ignore_ascii_case("powershell")
                        || shell.eq_ignore_ascii_case("pwsh")
                    {
                        if cfg!(windows) { "powershell" } else { "pwsh" }
                    } else {
                        "bash"
                    };
                    let seconds = format!("{:.3}", timeout.as_secs_f64());
                    let mut timed = std::process::Command::new("timeout");
                    timed.args(["--signal=KILL", &seconds, executable]);
                    if executable == "bash" {
                        timed.args(["-lc", command]);
                    } else {
                        timed.args(["-NoProfile", "-NonInteractive", "-Command", command]);
                    }
                    cmd = timed;
                }
            }
            cmd.current_dir(cwd);
            for (key, value) in options.env {
                cmd.env(key, value);
            }
            cmd.stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped());
            let abort_signal = options.abort_signal.clone();
            let mut child = cmd.spawn().map_err(env_error)?;
            let stdout = child.stdout.take();
            let stderr = child.stderr.take();
            let stdout_reader = std::thread::spawn(move || {
                stdout
                    .map(|mut stream| {
                        let mut bytes = Vec::new();
                        let _ = std::io::Read::read_to_end(&mut stream, &mut bytes);
                        bytes
                    })
                    .unwrap_or_default()
            });
            let stderr_reader = std::thread::spawn(move || {
                stderr
                    .map(|mut stream| {
                        let mut bytes = Vec::new();
                        let _ = std::io::Read::read_to_end(&mut stream, &mut bytes);
                        bytes
                    })
                    .unwrap_or_default()
            });
            let started = std::time::Instant::now();
            let status = loop {
                if abort_signal
                    .as_ref()
                    .is_some_and(|flag| flag.load(std::sync::atomic::Ordering::Relaxed))
                {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(EnvError::new("command aborted"));
                }
                if let Some(status) = child.try_wait().map_err(env_error)? {
                    break status;
                }
                if options
                    .timeout
                    .is_some_and(|timeout| started.elapsed() >= timeout)
                {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(EnvError::new(format!(
                        "command timed out after {} seconds",
                        options.timeout.unwrap().as_secs_f64()
                    )));
                }
                std::thread::sleep(Duration::from_millis(5));
            };
            let stdout = stdout_reader.join().unwrap_or_default();
            let stderr = stderr_reader.join().unwrap_or_default();
            if status.code().is_none()
                && let Some(timeout) = options.timeout
            {
                return Err(EnvError::new(format!(
                    "command timed out after {} seconds",
                    timeout.as_secs_f64()
                )));
            }
            let output = std::process::Output {
                status,
                stdout,
                stderr,
            };
            Ok(ProcessOutput {
                stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
                stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
                merged: None,
                exit_code: output.status.code().unwrap_or(-1),
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
fn truncate_head(content: &str) -> (String, Truncation) {
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
                max_lines: DEFAULT_MAX_LINES,
                max_bytes: DEFAULT_MAX_BYTES,
            },
        );
    }
    let mut out = Vec::new();
    let mut size = 0;
    let mut by = "lines";
    for line in lines.iter().take(DEFAULT_MAX_LINES) {
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
            max_lines: DEFAULT_MAX_LINES,
            max_bytes: DEFAULT_MAX_BYTES,
        },
    )
}
fn truncate_tail(content: &str) -> (String, Truncation) {
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
fn text_result(text: impl Into<String>, detail: Option<serde_json::Value>) -> AgentToolResult {
    AgentToolResult {
        content: vec![InputContent::Text(TextContent {
            content_type: TextContentType::Text,
            text: text.into(),
            text_signature: None,
        })],
        details: detail.map(details).unwrap_or(JsonValue::Null),
        usage: None,
        added_tool_names: None,
        terminate: None,
    }
}
fn image_result(text: String, data: String, mime_type: String) -> AgentToolResult {
    AgentToolResult {
        content: vec![
            InputContent::Text(TextContent {
                content_type: TextContentType::Text,
                text,
                text_signature: None,
            }),
            InputContent::Image(ImageContent {
                content_type: ImageContentType::Image,
                data,
                mime_type,
            }),
        ],
        details: JsonValue::Null,
        usage: None,
        added_tool_names: None,
        terminate: None,
    }
}
fn tool_error(message: impl Into<String>) -> ToolExecutionError {
    ToolExecutionError::new(
        ErrorContext::new(message.into())
            .unwrap_or_else(|_| ErrorContext::new("tool failed").unwrap()),
    )
}
fn parse_obj(args: JsonValue) -> Result<JsonObject, ToolExecutionError> {
    match args {
        JsonValue::Object(o) => Ok(o),
        _ => Err(tool_error("tool arguments must be an object")),
    }
}
fn arg_string(obj: &JsonObject, key: &str) -> Result<String, ToolExecutionError> {
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
fn descriptor(name: &str, label: &str, description: &str, schema: serde_json::Value) -> AgentTool {
    AgentTool {
        name: name.parse().unwrap(),
        label: label.into(),
        description: description.into(),
        parameters: serde_json::from_value(schema).unwrap(),
        constrained_sampling: None,
        execution_mode: None,
        replay: None,
    }
}
fn check_cancelled(cancel: &dyn Cancellation) -> Result<(), ToolExecutionError> {
    if cancel.is_cancelled() {
        Err(tool_error("Operation aborted"))
    } else {
        Ok(())
    }
}
fn update(
    progress: &dyn ToolProgressSink,
    result: AgentToolResult,
) -> BoxFuture<'_, Result<(), pi_agent::schema::HarnessRuntimeError>> {
    progress.update(result)
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
        let canonical = env.canonical_path(&path).await.unwrap_or(path.clone());
        let locks = MUTATION_LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
        let lock = {
            let mut map = locks.lock().unwrap();
            map.entry(canonical.clone())
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
                .clone()
        };
        let guard = lock.lock_owned().await;
        Ok(MutationGuard { _guard: guard })
    })
}
struct MutationGuard {
    _guard: tokio::sync::OwnedMutexGuard<()>,
}
static MUTATION_LOCKS: OnceLock<Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>> =
    OnceLock::new();

struct ReadTool {
    descriptor: AgentTool,
}
impl ReadTool {
    fn new() -> Self {
        Self {
            descriptor: descriptor(
                "read",
                "read",
                "Read file contents, including supported images. Text output is truncated to 2000 lines or 50KB; use offset and limit for large files.",
                json!({"type":"object","properties":{"path":{"type":"string"},"offset":{"type":"integer"},"limit":{"type":"integer"}},"required":["path"]}),
            ),
        }
    }
}
impl AgentHarnessTool<ExecutionToolContext> for ReadTool {
    fn descriptor(&self) -> &AgentTool {
        &self.descriptor
    }
    fn prepare_arguments(&self, args: JsonValue) -> Result<JsonObject, ToolExecutionError> {
        parse_obj(args)
    }
    fn execute<'a>(
        &'a self,
        _: ToolCallId,
        p: JsonObject,
        c: &'a dyn Cancellation,
        progress: &'a dyn ToolProgressSink,
        ctx: &'a ExecutionToolContext,
    ) -> BoxFuture<'a, Result<AgentToolResult, ToolExecutionError>> {
        Box::pin(async move {
            check_cancelled(c)?;
            let raw = arg_string(&p, "path")?;
            let path = ctx
                .env
                .absolute_path(&raw)
                .await
                .map_err(|e| tool_error(e.message))?;
            let bytes = ctx
                .env
                .read_binary_file(&path)
                .await
                .map_err(|e| tool_error(e.message))?;
            check_cancelled(c)?;
            if let Some(mime) = image_mime(&bytes) {
                if let Some(processed) = ctx
                    .env
                    .process_image(&bytes, mime, true)
                    .await
                    .map_err(|e| tool_error(e.message))?
                {
                    let hints = if processed.hints.is_empty() {
                        String::new()
                    } else {
                        format!("\n{}", processed.hints.join("\n"))
                    };
                    return Ok(image_result(
                        format!("Read image file [{}]{}", processed.mime_type, hints),
                        processed.data,
                        processed.mime_type,
                    ));
                }
                if mime == "image/bmp" {
                    return Ok(text_result(
                        format!(
                            "Read image file [{mime}]\n[Image omitted: BMP conversion is unavailable.]"
                        ),
                        None,
                    ));
                }
                return Ok(image_result(
                    format!("Read image file [{mime}]"),
                    base64::engine::general_purpose::STANDARD.encode(bytes),
                    mime.into(),
                ));
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
                .map(|n| (start + n).min(lines.len()))
                .unwrap_or(lines.len());
            let selected = lines[start..end].join("\n");
            let (mut out, tr) = truncate_head(&selected);
            let mut detail = None;
            if tr.first_line_exceeds_limit {
                out = format!(
                    "[Line {} exceeds 50.0KB limit. Use bash to inspect the line.]",
                    start + 1
                );
                detail = Some(json!({"truncation":tr}));
            } else if tr.truncated {
                let shown = start + tr.output_lines;
                out.push_str(&format!(
                    "\n\n[Showing lines {}-{} of {}. Use offset={} to continue.]",
                    start + 1,
                    shown,
                    lines.len(),
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
            let _ = update(progress, text_result("", None)).await;
            Ok(text_result(out, detail))
        })
    }
}

struct WriteTool {
    descriptor: AgentTool,
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
impl AgentHarnessTool<ExecutionToolContext> for WriteTool {
    fn descriptor(&self) -> &AgentTool {
        &self.descriptor
    }
    fn prepare_arguments(&self, args: JsonValue) -> Result<JsonObject, ToolExecutionError> {
        parse_obj(args)
    }
    fn execute<'a>(
        &'a self,
        _: ToolCallId,
        p: JsonObject,
        c: &'a dyn Cancellation,
        _: &'a dyn ToolProgressSink,
        ctx: &'a ExecutionToolContext,
    ) -> BoxFuture<'a, Result<AgentToolResult, ToolExecutionError>> {
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
    descriptor: AgentTool,
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
fn json_array_edits(obj: &JsonObject) -> Result<Vec<(String, String)>, ToolExecutionError> {
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

fn normalize_edit_arguments(args: JsonValue) -> Result<JsonObject, ToolExecutionError> {
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
fn normalize_piece(ch: char) -> char {
    match ch {
        '\u{2018}' | '\u{2019}' | '\u{201a}' | '\u{201b}' => '\'',
        '\u{201c}' | '\u{201d}' | '\u{201e}' | '\u{201f}' => '"',
        '\u{2010}'..='\u{2015}' | '\u{2212}' => '-',
        '\u{00a0}' | '\u{2002}'..='\u{200a}' | '\u{202f}' | '\u{205f}' | '\u{3000}' => ' ',
        value => value,
    }
}

fn normalize_with_map(content: &str) -> (String, Vec<(usize, usize)>) {
    use unicode_normalization::{UnicodeNormalization, char::is_combining_mark};
    let source: Vec<_> = content.char_indices().collect();
    let mut output = String::new();
    let mut map = Vec::new();
    let mut index = 0;
    while index < source.len() {
        let start = source[index].0;
        let ch = source[index].1;
        let mut end_index = index + 1;
        if ch == '\r' && source.get(end_index).is_some_and(|(_, next)| *next == '\n') {
            end_index += 1;
        } else {
            while end_index < source.len() && is_combining_mark(source[end_index].1) {
                end_index += 1;
            }
        }
        let end = source
            .get(end_index)
            .map_or(content.len(), |(offset, _)| *offset);
        let piece = if ch == '\r' && end_index == index + 2 {
            "\n".to_owned()
        } else {
            content[start..end].nfkc().collect()
        };
        for normalized in piece.chars().map(normalize_piece) {
            output.push(normalized);
            map.push((start, end));
        }
        index = end_index;
    }
    // Match the pinned trailing-whitespace normalization while retaining source ranges.
    let mut keep = vec![true; map.len()];
    let output_chars: Vec<char> = output.chars().collect();
    let mut line_start = 0;
    for index in 0..=output_chars.len() {
        if index == output_chars.len() || output_chars[index] == '\n' {
            let mut end = index;
            while end > line_start && output_chars[end - 1].is_whitespace() {
                keep[end - 1] = false;
                end -= 1;
            }
            line_start = index + 1;
        }
    }
    let mut normalized = String::new();
    let mut normalized_map = Vec::new();
    for (index, ch) in output_chars.into_iter().enumerate() {
        if keep[index] {
            normalized.push(ch);
            normalized_map.push(map[index]);
        }
    }
    (normalized, normalized_map)
}

fn normalize_fuzzy(s: &str) -> String {
    normalize_with_map(s).0
}

fn apply_edits(
    content: &str,
    edits: &[(String, String)],
    path: &str,
) -> Result<(String, String), String> {
    let (normalized_content, content_map) = normalize_with_map(content);
    let mut fuzzy = false;
    let mut matches = Vec::new();
    for (i, (old, new)) in edits.iter().enumerate() {
        if old.is_empty() {
            return Err(format!("edits[{i}].oldText must not be empty in {path}."));
        }
        let normalized_old = normalize_fuzzy(old);
        let exact = content
            .match_indices(old)
            .map(|(i, _)| i)
            .collect::<Vec<_>>();
        let mut idx = exact;
        let mut match_length = old.len();
        if idx.is_empty() {
            fuzzy = true;
            idx = normalized_content
                .match_indices(&normalized_old)
                .map(|(i, _)| i)
                .collect();
            match_length = normalized_old.len();
        }
        if idx.is_empty() {
            return Err(format!(
                "Could not find edits[{i}] in {path}. The oldText must match exactly."
            ));
        }
        if idx.len() > 1 {
            return Err(format!(
                "Found {} occurrences of edits[{i}] in {path}. Each oldText must be unique.",
                idx.len()
            ));
        }
        matches.push((
            idx[0],
            match_length,
            new.replace("\r\n", "\n").replace('\r', "\n"),
        ));
    }
    if fuzzy {
        matches.clear();
        for (i, (old, new)) in edits.iter().enumerate() {
            let normalized_old = normalize_fuzzy(old);
            let indices = normalized_content
                .match_indices(&normalized_old)
                .map(|(index, _)| index)
                .collect::<Vec<_>>();
            if indices.is_empty() {
                return Err(format!(
                    "Could not find edits[{i}] in {path}. The oldText must match exactly."
                ));
            }
            if indices.len() > 1 {
                return Err(format!(
                    "Found {} occurrences of edits[{i}] in {path}. Each oldText must be unique.",
                    indices.len()
                ));
            }
            matches.push((
                indices[0],
                normalized_old.len(),
                new.replace("\r\n", "\n").replace('\r', "\n"),
            ));
        }
    }
    matches.sort_by_key(|m| m.0);
    for w in matches.windows(2) {
        if w[0].0 + w[0].1 > w[1].0 {
            return Err(format!(
                "edits overlap in {path}. Merge them into one edit."
            ));
        }
    }
    let mut mapped = Vec::new();
    for (index, length, new) in matches {
        if fuzzy {
            let start_char = normalized_content[..index].chars().count();
            let end_char = start_char + normalized_content[index..index + length].chars().count();
            let Some(&(original_start, _)) = content_map.get(start_char) else {
                return Err(format!("Could not map edit in {path}."));
            };
            let Some(&(_, original_end)) = content_map.get(end_char.saturating_sub(1)) else {
                return Err(format!("Could not map edit in {path}."));
            };
            mapped.push((original_start, original_end - original_start, new));
        } else {
            mapped.push((index, length, new));
        }
    }
    mapped.sort_by_key(|replacement| replacement.0);
    for window in mapped.windows(2) {
        if window[0].0 + window[0].1 > window[1].0 {
            return Err(format!(
                "edits overlap in {path}. Merge them into one edit."
            ));
        }
    }
    let mut out = content.to_owned();
    for (idx, len, new) in mapped.into_iter().rev() {
        out.replace_range(idx..idx + len, &new);
    }
    if out == content {
        return Err(format!("No changes made to {path}."));
    }
    Ok((content.to_owned(), out))
}

fn unified_diff(path: &str, old: &str, new: &str) -> (String, String) {
    let old_lines: Vec<_> = old.lines().collect();
    let new_lines: Vec<_> = new.lines().collect();
    let mut table = vec![vec![0usize; new_lines.len() + 1]; old_lines.len() + 1];
    for old_index in (0..old_lines.len()).rev() {
        for new_index in (0..new_lines.len()).rev() {
            table[old_index][new_index] = if old_lines[old_index] == new_lines[new_index] {
                table[old_index + 1][new_index + 1] + 1
            } else {
                table[old_index + 1][new_index].max(table[old_index][new_index + 1])
            };
        }
    }
    let mut rows = Vec::new();
    let (mut old_index, mut new_index) = (0, 0);
    while old_index < old_lines.len() || new_index < new_lines.len() {
        if old_index < old_lines.len()
            && new_index < new_lines.len()
            && old_lines[old_index] == new_lines[new_index]
        {
            rows.push((' ', old_index, new_index, old_lines[old_index]));
            old_index += 1;
            new_index += 1;
        } else if new_index < new_lines.len()
            && (old_index == old_lines.len()
                || table[old_index][new_index + 1] >= table[old_index + 1][new_index])
        {
            rows.push(('+', old_index, new_index, new_lines[new_index]));
            new_index += 1;
        } else {
            rows.push(('-', old_index, new_index, old_lines[old_index]));
            old_index += 1;
        }
    }
    let changed: Vec<_> = rows
        .iter()
        .enumerate()
        .filter(|(_, row)| row.0 != ' ')
        .map(|(index, _)| index)
        .collect();
    if changed.is_empty() {
        return (String::new(), String::new());
    }
    let start = changed[0].saturating_sub(4);
    let end = (changed.last().copied().unwrap_or(start) + 5).min(rows.len());
    let mut old_start = rows[start].1 + 1;
    let mut new_start = rows[start].2 + 1;
    if rows[start].0 == '+' {
        old_start = old_start.saturating_sub(1);
    }
    if rows[start].0 == '-' {
        new_start = new_start.saturating_sub(1);
    }
    let old_count = rows[start..end].iter().filter(|row| row.0 != '+').count();
    let new_count = rows[start..end].iter().filter(|row| row.0 != '-').count();
    let mut patch = format!(
        "--- {path}\n+++ {path}\n@@ -{old_start},{old_count} +{new_start},{new_count} @@\n"
    );
    let mut display = format!("@@ -{old_start},{old_count} +{new_start},{new_count} @@\n");
    for (kind, _, _, line) in &rows[start..end] {
        patch.push(*kind);
        patch.push_str(line);
        patch.push('\n');
        if *kind == ' ' {
            display.push_str("  ");
        } else {
            display.push(*kind);
        }
        display.push_str(line);
        display.push('\n');
    }
    // Keep the display-oriented diff and standard patch distinct, as in pi-mono.
    (display, patch)
}

impl AgentHarnessTool<ExecutionToolContext> for EditTool {
    fn descriptor(&self) -> &AgentTool {
        &self.descriptor
    }
    fn prepare_arguments(&self, args: JsonValue) -> Result<JsonObject, ToolExecutionError> {
        normalize_edit_arguments(args)
    }
    fn execute<'a>(
        &'a self,
        _id: ToolCallId,
        p: JsonObject,
        c: &'a dyn Cancellation,
        _progress: &'a dyn ToolProgressSink,
        ctx: &'a ExecutionToolContext,
    ) -> BoxFuture<'a, Result<AgentToolResult, ToolExecutionError>> {
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
            let original =
                ctx.env.read_text_file(&path).await.map_err(|e| {
                    tool_error(format!("Could not edit file: {raw}. {}", e.message))
                })?;
            let bom = original.strip_prefix('\u{feff}').map_or("", |_| "\u{feff}");
            let body = original.strip_prefix('\u{feff}').unwrap_or(&original);
            let crlf = body.contains("\r\n");
            let normalized = body.replace("\r\n", "\n").replace('\r', "\n");
            let (base, new) = apply_edits(&normalized, &edits, &raw).map_err(tool_error)?;
            let final_body = if crlf {
                new.replace('\n', "\r\n")
            } else {
                new.clone()
            };
            ctx.env
                .write_file(&path, format!("{bom}{final_body}").as_bytes())
                .await
                .map_err(|e| tool_error(e.message))?;
            check_cancelled(c)?;
            let (diff, patch) = unified_diff(&raw, &base, &new);
            Ok(text_result(
                format!("Successfully replaced {} block(s) in {}.", edits.len(), raw),
                Some(json!({"diff": diff, "patch": patch})),
            ))
        })
    }
}

struct ShellTool {
    descriptor: AgentTool,
    shell: &'static str,
}
impl ShellTool {
    fn new(name: &str, shell: &'static str) -> Self {
        Self {
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
impl AgentHarnessTool<ExecutionToolContext> for ShellTool {
    fn descriptor(&self) -> &AgentTool {
        &self.descriptor
    }
    fn prepare_arguments(&self, args: JsonValue) -> Result<JsonObject, ToolExecutionError> {
        parse_obj(args)
    }
    fn execute<'a>(
        &'a self,
        _: ToolCallId,
        p: JsonObject,
        c: &'a dyn Cancellation,
        progress: &'a dyn ToolProgressSink,
        ctx: &'a ExecutionToolContext,
    ) -> BoxFuture<'a, Result<AgentToolResult, ToolExecutionError>> {
        Box::pin(async move {
            check_cancelled(c)?;
            let command = arg_string(&p, "command")?;
            let command = if self.shell == "powershell" {
                format!(
                    "try {{ [Console]::OutputEncoding=[System.Text.Encoding]::UTF8 }} catch {{}}\n{command}"
                )
            } else {
                command
            };
            let timeout = arg_f64(&p, "timeout")
                .filter(|seconds| seconds.is_finite() && *seconds > 0.0)
                .map(Duration::from_secs_f64);
            let abort_signal = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let output = ctx
                .env
                .exec(
                    self.shell,
                    &command,
                    ProcessOptions {
                        cwd: None,
                        timeout,
                        env: BTreeMap::new(),
                        abort_signal: Some(Arc::clone(&abort_signal)),
                    },
                )
                .await
                .map_err(|e| tool_error(e.message))?;
            check_cancelled(c)?;
            let combined = if let Some(merged) = output.merged {
                merged
            } else if output.stderr.is_empty() {
                output.stdout
            } else if output.stdout.is_empty() {
                output.stderr
            } else {
                format!("{}{}", output.stdout, output.stderr)
            };
            let (mut body, tr) = truncate_tail(&combined);
            let mut detail = None;
            if tr.truncated {
                let full = ctx
                    .env
                    .create_temp_file(
                        if self.shell == "powershell" {
                            "pi-powershell"
                        } else {
                            "pi-bash"
                        },
                        combined.as_bytes(),
                    )
                    .await
                    .map_err(|e| tool_error(e.message))?;
                body.push_str(&format!(
                    "\n\n[Showing last {} lines of {}. Full output: {}]",
                    tr.output_lines, tr.total_lines, full
                ));
                detail = Some(json!({"truncation":tr,"fullOutputPath":full}));
            }
            let _ = update(progress, text_result(body.clone(), detail.clone())).await;
            if output.exit_code != 0 {
                return Err(tool_error(format!(
                    "{}\n\nCommand exited with code {}",
                    body, output.exit_code
                )));
            }
            Ok(text_result(
                if body.is_empty() {
                    "(no output)".into()
                } else {
                    body
                },
                detail,
            ))
        })
    }
}

struct LsTool {
    descriptor: AgentTool,
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
impl AgentHarnessTool<ExecutionToolContext> for LsTool {
    fn descriptor(&self) -> &AgentTool {
        &self.descriptor
    }
    fn prepare_arguments(&self, args: JsonValue) -> Result<JsonObject, ToolExecutionError> {
        parse_obj(args)
    }
    fn execute<'a>(
        &'a self,
        _: ToolCallId,
        p: JsonObject,
        c: &'a dyn Cancellation,
        _: &'a dyn ToolProgressSink,
        ctx: &'a ExecutionToolContext,
    ) -> BoxFuture<'a, Result<AgentToolResult, ToolExecutionError>> {
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
            let limit = arg_u64(&p, "limit").unwrap_or(500).max(1) as usize;
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
            let mut out = entries
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
                out = "(empty directory)".into()
            }
            let (mut out, truncation) = truncate_head(&out);
            let mut notices = Vec::new();
            if limited {
                notices.push(format!(
                    "{} entries limit reached. Use limit={} for more",
                    limit,
                    limit * 2
                ));
            }
            if truncation.truncated {
                notices.push("50.0KB output limit reached".to_owned());
            }
            if !notices.is_empty() {
                out.push_str(&format!("\n\n[{}]", notices.join(". ")));
            }
            let detail = if limited || truncation.truncated {
                Some(
                    json!({"entryLimitReached": limited.then_some(limit), "truncation": truncation}),
                )
            } else {
                None
            };
            Ok(text_result(out, detail))
        })
    }
}

struct GrepTool {
    descriptor: AgentTool,
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
    let mut r = String::from("^");
    let chars: Vec<_> = pattern.chars().collect();
    let mut index = 0;
    while index < chars.len() {
        let ch = chars[index];
        match ch {
            '*' if index + 1 < chars.len() && chars[index + 1] == '*' => {
                index += 1;
                if index + 1 < chars.len() && chars[index + 1] == '/' {
                    r.push_str("(?:.*/)?");
                    index += 1;
                } else {
                    r.push_str(".*");
                }
            }
            '*' => r.push_str("[^/]*"),
            '?' => r.push('.'),
            '.' => r.push_str("\\."),
            c => r.push_str(&regex::escape(&c.to_string())),
        }
        index += 1;
    }
    r.push('$');
    Regex::new(&r).map(|re| re.is_match(name)).unwrap_or(false)
}
fn ignore_pattern_matches(pattern: &str, relative: &str) -> bool {
    let mut pattern = pattern.trim();
    if pattern.is_empty() || pattern.starts_with('#') {
        return false;
    }
    if pattern.starts_with('!') {
        pattern = &pattern[1..];
    }
    let directory = pattern.ends_with('/');
    pattern = pattern.trim_start_matches('/').trim_end_matches('/');
    if pattern.is_empty() {
        return false;
    }
    let candidate = relative.trim_start_matches("./").trim_end_matches('/');
    if directory {
        return candidate == pattern || candidate.starts_with(&format!("{pattern}/"));
    }
    glob_matches(pattern, candidate)
        || (!pattern.contains('/')
            && glob_matches(
                pattern,
                Path::new(candidate)
                    .file_name()
                    .and_then(|v| v.to_str())
                    .unwrap_or(candidate),
            ))
}

fn ignored(path: &str, patterns: &[(String, String)]) -> bool {
    let mut result = false;
    for (base, pattern) in patterns {
        let scoped = Path::new(path)
            .strip_prefix(base)
            .unwrap_or(Path::new(path))
            .to_string_lossy()
            .replace('\\', "/");
        if ignore_pattern_matches(pattern, &scoped) {
            result = !pattern.trim_start().starts_with('!');
        }
    }
    result
}

fn collect_files(
    env: &Arc<dyn ExecutionEnv>,
    root: &str,
) -> BoxFuture<'static, EnvResult<Vec<FileInfo>>> {
    let env = Arc::clone(env);
    let root = root.to_owned();
    Box::pin(async move {
        let mut files = Vec::new();
        let mut stack = vec![(root.clone(), Vec::<(String, String)>::new())];
        while let Some((dir, mut patterns)) = stack.pop() {
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
                if entry.is_dir {
                    let is_ignored = ignored(&entry.path, &patterns);
                    let can_reinclude = patterns
                        .iter()
                        .any(|(_, pattern)| pattern.trim_start().starts_with('!'));
                    if !is_ignored || can_reinclude {
                        stack.push((entry.path, patterns.clone()));
                    }
                } else if !ignored(&entry.path, &patterns) {
                    files.push(entry);
                }
            }
        }
        Ok(files)
    })
}

fn truncate_match_line(line: &str) -> (String, bool) {
    let mut chars = line.chars();
    let text: String = chars.by_ref().take(GREP_MAX_LINE_LENGTH).collect();
    let truncated = chars.next().is_some();
    if truncated {
        (format!("{text}... [truncated]"), true)
    } else {
        (text, false)
    }
}

impl AgentHarnessTool<ExecutionToolContext> for GrepTool {
    fn descriptor(&self) -> &AgentTool {
        &self.descriptor
    }
    fn prepare_arguments(&self, args: JsonValue) -> Result<JsonObject, ToolExecutionError> {
        parse_obj(args)
    }
    fn execute<'a>(
        &'a self,
        _: ToolCallId,
        p: JsonObject,
        c: &'a dyn Cancellation,
        _: &'a dyn ToolProgressSink,
        ctx: &'a ExecutionToolContext,
    ) -> BoxFuture<'a, Result<AgentToolResult, ToolExecutionError>> {
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
                collect_files(&ctx.env, &root)
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
                if let Some(glob) = glob
                    && !glob_matches(glob, &file.name)
                {
                    continue;
                }
                let text = match ctx.env.read_text_file(&file.path).await {
                    Ok(t) => t,
                    Err(_) => continue,
                };
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
                    let start = idx.saturating_sub(context);
                    let end = (idx + context + 1).min(lines.len());
                    for (n, line) in lines.iter().enumerate().take(end).skip(start) {
                        let (shown, was_truncated) = truncate_match_line(line);
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
            let (mut out, truncation) = truncate_head(&raw_output);
            if out.is_empty() {
                out = "No matches found".into()
            }
            let reached = count >= limit;
            let byte_truncated = truncation.truncated;
            let mut notices = Vec::new();
            if reached {
                notices.push(format!("{} matches limit reached", limit));
            }
            if byte_truncated {
                notices.push("50.0KB output limit reached".to_owned());
            }
            if lines_truncated {
                notices.push("some lines truncated".to_owned());
            }
            if !notices.is_empty() && out != "No matches found" {
                out.push_str(&format!("\n\n[{}]", notices.join(". ")));
            }
            let detail = if reached || byte_truncated || lines_truncated {
                Some(
                    json!({"matchLimitReached": reached.then_some(limit), "truncation": byte_truncated.then_some(truncation), "linesTruncated": lines_truncated}),
                )
            } else {
                None
            };
            Ok(text_result(out, detail))
        })
    }
}

struct FindTool {
    descriptor: AgentTool,
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
impl AgentHarnessTool<ExecutionToolContext> for FindTool {
    fn descriptor(&self) -> &AgentTool {
        &self.descriptor
    }
    fn prepare_arguments(&self, args: JsonValue) -> Result<JsonObject, ToolExecutionError> {
        parse_obj(args)
    }
    fn execute<'a>(
        &'a self,
        _: ToolCallId,
        p: JsonObject,
        c: &'a dyn Cancellation,
        _: &'a dyn ToolProgressSink,
        ctx: &'a ExecutionToolContext,
    ) -> BoxFuture<'a, Result<AgentToolResult, ToolExecutionError>> {
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
            let limit = arg_u64(&p, "limit").unwrap_or(1000).max(1) as usize;
            let mut files = collect_files(&ctx.env, &root)
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
                if glob_matches(&pattern, &rel) || glob_matches(&pattern, &file.name) {
                    paths.push(rel);
                    if paths.len() >= limit {
                        break;
                    }
                }
            }
            if paths.is_empty() {
                return Ok(text_result("No files found matching pattern", None));
            }
            let reached = paths.len() >= limit;
            let raw_output = paths.join("\n");
            let (mut out, truncation) = truncate_head(&raw_output);
            let mut notices = Vec::new();
            if reached {
                notices.push(format!("{} results limit reached", limit));
            }
            if truncation.truncated {
                notices.push("50.0KB output limit reached".to_owned());
            }
            if !notices.is_empty() {
                out.push_str(&format!("\n\n[{}]", notices.join(". ")));
            }
            let detail = if reached || truncation.truncated {
                Some(
                    json!({"resultLimitReached": reached.then_some(limit), "truncation": truncation}),
                )
            } else {
                None
            };
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
pub fn create_tool(name: &str) -> Arc<dyn AgentHarnessTool<ExecutionToolContext>> {
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
/// Create all built-in coding tools in stable order.
pub fn create_tools() -> Vec<Arc<dyn AgentHarnessTool<ExecutionToolContext>>> {
    TOOL_NAMES.iter().map(|name| create_tool(name)).collect()
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

/// Create the built-in read tool binding.
pub fn create_read_tool() -> Arc<dyn AgentHarnessTool<ExecutionToolContext>> {
    Arc::new(ReadTool::new())
}
/// Create the built-in write tool binding.
pub fn create_write_tool() -> Arc<dyn AgentHarnessTool<ExecutionToolContext>> {
    Arc::new(WriteTool::new())
}
/// Create the built-in edit tool binding.
pub fn create_edit_tool() -> Arc<dyn AgentHarnessTool<ExecutionToolContext>> {
    Arc::new(EditTool::new())
}
/// Create the built-in bash tool binding.
pub fn create_bash_tool() -> Arc<dyn AgentHarnessTool<ExecutionToolContext>> {
    Arc::new(ShellTool::new("bash", "bash"))
}
/// Create the built-in PowerShell tool binding.
pub fn create_powershell_tool() -> Arc<dyn AgentHarnessTool<ExecutionToolContext>> {
    Arc::new(ShellTool::new("powershell", "powershell"))
}
/// Create the built-in grep tool binding.
pub fn create_grep_tool() -> Arc<dyn AgentHarnessTool<ExecutionToolContext>> {
    Arc::new(GrepTool::new())
}
/// Create the built-in find tool binding.
pub fn create_find_tool() -> Arc<dyn AgentHarnessTool<ExecutionToolContext>> {
    Arc::new(FindTool::new())
}
/// Create the built-in ls tool binding.
pub fn create_ls_tool() -> Arc<dyn AgentHarnessTool<ExecutionToolContext>> {
    Arc::new(LsTool::new())
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
