use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeMap, fmt, path::PathBuf, time::Duration};

pub const MAX_STORE_VALUE_CHARS: usize = 256 * 1024;
pub const MAX_STORE_TOTAL_CHARS: usize = 1024 * 1024;
pub const MAX_OUTPUT_CHARS: usize = 16 * 1024 * 1024;
pub const MAX_OUTPUT_ITEMS: usize = 100_000;
pub const MAX_CALLS: usize = 4096;
/// Maximum nested JSON arrays/objects in a boundary value, excluding wire envelopes.
pub const MAX_JSON_DEPTH: usize = 256;
pub const MAX_JSON_BYTES: usize = 1024 * 1024;
pub const MAX_BRIDGE_BYTES: usize = 16 * 1024 * 1024;
pub(crate) const MAX_WIRE_BYTES: usize = 128 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct SandboxOptions {
    /// Explicit path to the matching `pi-codemode-worker` binary. No PATH search.
    pub worker_executable: PathBuf,
    pub max_workers: usize,
    pub memory_limit_bytes: usize,
    pub max_stack_bytes: usize,
    pub timeout: Duration,
    pub max_concurrent_tools: usize,
}
impl SandboxOptions {
    pub fn new(worker_executable: impl Into<PathBuf>) -> Self {
        Self {
            worker_executable: worker_executable.into(),
            max_workers: 4,
            memory_limit_bytes: 64 * 1024 * 1024,
            max_stack_bytes: 256 * 1024,
            timeout: Duration::from_secs(300),
            max_concurrent_tools: 32,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeclaredTool {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub js_name: Option<String>,
    #[serde(default)]
    pub description: String,
}
impl DeclaredTool {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            js_name: None,
            description: String::new(),
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeclaredGlobal {
    pub name: String,
    #[serde(default)]
    pub spread: bool,
}
#[derive(Clone, Debug)]
pub struct RunOptions {
    pub started_at: f64,
    pub seed: u32,
    pub tools: Vec<DeclaredTool>,
    pub globals: Vec<DeclaredGlobal>,
    pub store: BTreeMap<String, Value>,
    /// None disables the deadline for controlled runs.
    pub timeout: Option<Duration>,
    pub cancellation: Cancellation,
}
impl RunOptions {
    pub fn new(started_at: f64, seed: u32) -> Self {
        Self {
            started_at,
            seed,
            tools: Vec::new(),
            globals: Vec::new(),
            store: BTreeMap::new(),
            timeout: None,
            cancellation: Cancellation::default(),
        }
    }
}
#[derive(Clone, Debug, Default)]
pub struct ExecuteOptions {
    pub tools: Vec<DeclaredTool>,
    pub globals: Vec<DeclaredGlobal>,
    pub store: BTreeMap<String, Value>,
    pub timeout: Option<Duration>,
    pub cancellation: Cancellation,
}

/// A cancellation signal shared with the caller or a host tool. Completion also
/// cancels this signal so unawaited host calls can release their resources.
#[derive(Clone, Debug, Default)]
pub struct Cancellation(pub(crate) std::sync::Arc<std::sync::atomic::AtomicBool>);
impl Cancellation {
    pub fn cancel(&self) {
        self.0.store(true, std::sync::atomic::Ordering::Release);
    }
    pub fn is_cancelled(&self) -> bool {
        self.0.load(std::sync::atomic::Ordering::Acquire)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum CallTarget {
    Tool,
    Global,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct HostCall {
    pub id: u64,
    pub target: CallTarget,
    pub name: String,
    /// None is JavaScript undefined, distinct from JSON null.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_value"
    )]
    pub args: Option<Value>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(from = "OutcomeJson", into = "OutcomeJson")]
pub enum Outcome {
    Success {
        value: Option<Value>,
    },
    Error {
        message: String,
        properties: BTreeMap<String, Value>,
    },
}
impl Outcome {
    pub fn success(value: Value) -> Self {
        Self::Success { value: Some(value) }
    }
    pub fn undefined() -> Self {
        Self::Success { value: None }
    }
    pub fn error(message: impl Into<String>) -> Self {
        Self::Error {
            message: message.into(),
            properties: BTreeMap::new(),
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum OutputItem {
    Text {
        text: String,
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        console: bool,
    },
    Image {
        data: String,
        #[serde(rename = "mimeType")]
        mime_type: String,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum CallStatus {
    Ok,
    Error,
    Cancelled,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Call {
    pub name: String,
    pub status: CallStatus,
    pub duration_ms: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ErrorKind {
    Script,
    Timeout,
    Aborted,
    Sandbox,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExecutionError {
    pub kind: ErrorKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stack: Option<String>,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct StoreWrites {
    pub set: BTreeMap<String, Value>,
    pub delete: Vec<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct During {
    pub seq: u64,
    pub issued: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(try_from = "ResultJson", into = "ResultJson")]
pub enum ExecutionResult {
    Success {
        value: Option<Value>,
        output: Vec<OutputItem>,
        calls: Vec<Call>,
        store_writes: StoreWrites,
        during: Option<During>,
    },
    Error {
        error: ExecutionError,
        output: Vec<OutputItem>,
        calls: Vec<Call>,
        during: Option<During>,
    },
}
impl ExecutionResult {
    pub fn is_ok(&self) -> bool {
        matches!(self, Self::Success { .. })
    }
}
#[derive(Clone, Debug, PartialEq)]
pub enum Event {
    Started { fingerprint: String },
    Call(HostCall),
    Idle { seq: u64, issued: u64 },
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundaryError(pub String);
impl fmt::Display for BoundaryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for BoundaryError {}

pub fn to_identifier(name: &str) -> String {
    let result: String = name
        .chars()
        .enumerate()
        .map(|(i, c)| {
            if c.is_ascii_alphabetic() || c == '_' || c == '$' || (i > 0 && c.is_ascii_digit()) {
                c
            } else {
                '_'
            }
        })
        .collect();
    if result.is_empty() {
        "_".to_owned()
    } else {
        result
    }
}

#[derive(Serialize, Deserialize)]
struct OutcomeJson {
    ok: bool,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_value"
    )]
    value: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    message: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    properties: BTreeMap<String, Value>,
}
impl From<Outcome> for OutcomeJson {
    fn from(value: Outcome) -> Self {
        match value {
            Outcome::Success { value } => Self {
                ok: true,
                value,
                message: None,
                properties: BTreeMap::new(),
            },
            Outcome::Error {
                message,
                properties,
            } => Self {
                ok: false,
                value: None,
                message: Some(message),
                properties,
            },
        }
    }
}
impl From<OutcomeJson> for Outcome {
    fn from(value: OutcomeJson) -> Self {
        if value.ok {
            Self::Success { value: value.value }
        } else {
            Self::Error {
                message: value.message.unwrap_or_default(),
                properties: value.properties,
            }
        }
    }
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ResultJson {
    ok: bool,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_value"
    )]
    value: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    error: Option<ExecutionError>,
    output: Vec<OutputItem>,
    calls: Vec<Call>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    store_writes: Option<StoreWrites>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    during: Option<During>,
}
impl From<ExecutionResult> for ResultJson {
    fn from(value: ExecutionResult) -> Self {
        match value {
            ExecutionResult::Success {
                value,
                output,
                calls,
                store_writes,
                during,
            } => Self {
                ok: true,
                value,
                error: None,
                output,
                calls,
                store_writes: Some(store_writes),
                during,
            },
            ExecutionResult::Error {
                error,
                output,
                calls,
                during,
            } => Self {
                ok: false,
                value: None,
                error: Some(error),
                output,
                calls,
                store_writes: None,
                during,
            },
        }
    }
}
impl TryFrom<ResultJson> for ExecutionResult {
    type Error = String;
    fn try_from(value: ResultJson) -> Result<Self, String> {
        if value.ok {
            Ok(Self::Success {
                value: value.value,
                output: value.output,
                calls: value.calls,
                store_writes: value.store_writes.ok_or("Missing successful storeWrites")?,
                during: value.during,
            })
        } else {
            Ok(Self::Error {
                error: value.error.ok_or("Missing failed error")?,
                output: value.output,
                calls: value.calls,
                during: value.during,
            })
        }
    }
}

// A present JSON null is a value; only an absent field denotes JS undefined.
pub(crate) fn optional_value<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Value>, D::Error> {
    Value::deserialize(deserializer).map(Some)
}
