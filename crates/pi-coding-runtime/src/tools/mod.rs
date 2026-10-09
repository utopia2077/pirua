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
    /// Exit status, or -1 when unavailable.
    pub exit_code: i32,
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
}

/// Host port consumed by all coding tools.
pub trait ExecutionEnv: Send + Sync {
    /// Resolve a path without requiring it to exist.
    fn absolute_path<'a>(&'a self, path: &'a str) -> BoxFuture<'a, EnvResult<String>>;
    /// Return whether a path exists.
    fn exists<'a>(&'a self, path: &'a str) -> BoxFuture<'a, EnvResult<bool>>;
    /// Read binary bytes.
    fn read_binary_file<'a>(&'a self, path: &'a str) -> BoxFuture<'a, EnvResult<Vec<u8>>>;
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
            cmd.current_dir(cwd);
            for (key, value) in options.env {
                cmd.env(key, value);
            }
            let output = cmd.output().map_err(env_error)?;
            Ok(ProcessOutput {
                stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
                stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
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
fn normalize_fuzzy(s: &str) -> String {
    s.replace("\r\n", "\n")
        .replace('\r', "\n")
        .chars()
        .map(|c| match c {
            '\u{2018}' | '\u{2019}' | '\u{201a}' | '\u{201b}' => '\'',
            '\u{201c}' | '\u{201d}' | '\u{201e}' | '\u{201f}' => '"',
            '\u{2010}'..='\u{2015}' | '\u{2212}' => '-',
            '\u{00a0}' | '\u{2002}'..='\u{200a}' | '\u{202f}' | '\u{205f}' | '\u{3000}' => ' ',
            c => c,
        })
        .collect::<String>()
        .split('\n')
        .map(str::trim_end)
        .collect::<Vec<_>>()
        .join("\n")
}
fn apply_edits(
    content: &str,
    edits: &[(String, String)],
    path: &str,
) -> Result<(String, String), String> {
    let mut matches = Vec::new();
    let mut fuzzy = false;
    for (i, (old, new)) in edits.iter().enumerate() {
        if old.is_empty() {
            return Err(format!("edits[{i}].oldText must not be empty in {path}."));
        }
        let mut idx = content
            .match_indices(old)
            .map(|(i, _)| i)
            .collect::<Vec<_>>();
        if idx.is_empty() {
            fuzzy = true;
            let norm = normalize_fuzzy(content);
            let no = normalize_fuzzy(old);
            idx = norm.match_indices(&no).map(|(i, _)| i).collect();
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
        matches.push((idx[0], old.len(), new.clone()));
    }
    matches.sort_by_key(|m| m.0);
    for w in matches.windows(2) {
        if w[0].0 + w[0].1 > w[1].0 {
            return Err(format!(
                "edits overlap in {path}. Merge them into one edit."
            ));
        }
    }
    let mut out = if fuzzy {
        normalize_fuzzy(content)
    } else {
        content.to_owned()
    };
    for (idx, len, new) in matches.into_iter().rev() {
        out.replace_range(idx..idx + len, &new);
    }
    if out == content {
        return Err(format!("No changes made to {path}."));
    }
    Ok((content.to_owned(), out))
}
impl AgentHarnessTool<ExecutionToolContext> for EditTool {
    fn descriptor(&self) -> &AgentTool {
        &self.descriptor
    }
    fn prepare_arguments(&self, args: JsonValue) -> Result<JsonObject, ToolExecutionError> {
        parse_obj(args)
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
            let diff = format!("--- {raw}\n+++ {raw}\n@@\n-{base}\n+{new}");
            Ok(text_result(
                format!("Successfully replaced {} block(s) in {}.", edits.len(), raw),
                Some(json!({"diff": diff, "patch": diff})),
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
            let timeout = arg_u64(&p, "timeout").map(Duration::from_secs);
            let output = ctx
                .env
                .exec(
                    self.shell,
                    &command,
                    ProcessOptions {
                        cwd: None,
                        timeout,
                        env: BTreeMap::new(),
                    },
                )
                .await
                .map_err(|e| tool_error(e.message))?;
            check_cancelled(c)?;
            let combined = if output.stderr.is_empty() {
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
            if limited {
                out.push_str(&format!(
                    "\n\n[{} entries limit reached. Use limit={} for more]",
                    limit,
                    limit * 2
                ));
            }
            Ok(text_result(
                out,
                limited.then(|| json!({"entryLimitReached":limit})),
            ))
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
    for ch in pattern.chars() {
        match ch {
            '*' => r.push_str(".*"),
            '?' => r.push('.'),
            '.' => r.push_str("\\."),
            c => r.push_str(&regex::escape(&c.to_string())),
        }
    }
    r.push('$');
    Regex::new(&r).map(|re| re.is_match(name)).unwrap_or(false)
}
fn collect_files(
    env: &Arc<dyn ExecutionEnv>,
    root: &str,
) -> BoxFuture<'static, EnvResult<Vec<FileInfo>>> {
    let env = Arc::clone(env);
    let root = root.to_owned();
    Box::pin(async move {
        let mut files = Vec::new();
        let mut stack = vec![root];
        while let Some(dir) = stack.pop() {
            for entry in env.list_dir(&dir).await? {
                if entry.name == ".git" || entry.name == "node_modules" {
                    continue;
                }
                if entry.is_dir {
                    stack.push(entry.path);
                } else {
                    files.push(entry);
                }
            }
        }
        Ok(files)
    })
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
            let limit = arg_u64(&p, "limit").unwrap_or(100) as usize;
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
            let files = if ctx
                .env
                .file_info(&root)
                .await
                .map_err(|e| tool_error(e.message))?
                .is_dir
            {
                collect_files(&ctx.env, &root)
                    .await
                    .map_err(|e| tool_error(e.message))?
            } else {
                vec![
                    ctx.env
                        .file_info(&root)
                        .await
                        .map_err(|e| tool_error(e.message))?,
                ]
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
                        let mut shown = line.clone();
                        if shown.len() > GREP_MAX_LINE_LENGTH {
                            shown.truncate(GREP_MAX_LINE_LENGTH);
                            shown.push_str("... [truncated]");
                        }
                        result.push(if n == idx {
                            format!("{}:{}: {}", file.path, n + 1, shown)
                        } else {
                            format!("{}-{}- {}", file.path, n + 1, shown)
                        });
                    }
                }
                if count > limit {
                    break;
                }
            }
            let mut out = result.join("\n");
            if out.is_empty() {
                out = "No matches found".into()
            }
            let reached = count > limit;
            if reached {
                out.push_str(&format!("\n\n[{} matches limit reached]", limit));
            }
            Ok(text_result(
                out,
                reached.then(|| json!({"matchLimitReached":limit})),
            ))
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
            let limit = arg_u64(&p, "limit").unwrap_or(1000) as usize;
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
            let mut out = paths.join("\n");
            if reached {
                out.push_str(&format!("\n\n[{} results limit reached]", limit));
            }
            Ok(text_result(
                out,
                reached.then(|| json!({"resultLimitReached":limit})),
            ))
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
