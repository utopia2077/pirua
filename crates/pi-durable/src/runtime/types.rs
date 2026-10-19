use super::ToolExecutionApi;
use crate::{InvocationIdentity, Result, TaskDefinition, session::*};
use pi_ai::{
    AssistantMessage, BoxFuture, InputContent, JsonObject, JsonSchema, JsonValue, Message,
    ModelThinkingLevel, Presence, SafeUnsigned, Tool, ToolCall, Usage, UserContent,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub type RuntimeFuture<T> = BoxFuture<'static, Result<T>>;
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ModelRef {
    pub provider: String,
    pub model_id: String,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum BusyPolicy {
    Steer,
    FollowUp,
    Reject,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct InputEntry {
    pub kind: String,
    #[serde(default, skip_serializing_if = "Presence::is_missing")]
    pub data: Presence<JsonValue>,
}
#[derive(Clone, Debug)]
pub enum SubmissionDraft {
    Input {
        request_id: Option<String>,
        content: UserContent,
        when_busy: Option<BusyPolicy>,
        entry: Option<InputEntry>,
    },
    Write {
        request_id: Option<String>,
        entry: EntryDraft,
    },
}
impl SubmissionDraft {
    pub fn input(content: impl Into<String>) -> Self {
        Self::Input {
            request_id: None,
            content: UserContent::Text(content.into()),
            when_busy: None,
            entry: None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SubmissionAbort {
    Aborted,
    AlreadyPlaced,
    Settled,
    NotFound,
}
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentState {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<ModelRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thinking_level: Option<ModelThinkingLevel>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extensions: Option<NameSelection>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tools: Option<ToolSelection>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_tools: Option<NameSelection>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instructions: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum NameSelection {
    Exact(Vec<String>),
    Edit {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        add: Option<Vec<String>>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        remove: Option<Vec<String>>,
    },
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ToolSelection {
    Exact(Vec<String>),
    Remove { remove: Vec<String> },
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentChange {
    #[serde(default, skip_serializing_if = "Presence::is_missing")]
    pub model: Presence<ModelRef>,
    #[serde(default, skip_serializing_if = "Presence::is_missing")]
    pub thinking_level: Presence<ModelThinkingLevel>,
    #[serde(default, skip_serializing_if = "Presence::is_missing")]
    pub extensions: Presence<NameSelection>,
    #[serde(default, skip_serializing_if = "Presence::is_missing")]
    pub tools: Presence<ToolSelection>,
    #[serde(default, skip_serializing_if = "Presence::is_missing")]
    pub model_tools: Presence<NameSelection>,
    #[serde(default, skip_serializing_if = "Presence::is_missing")]
    pub instructions: Presence<String>,
    #[serde(default, skip_serializing_if = "Presence::is_missing")]
    pub cwd: Presence<String>,
}
#[derive(Clone)]
pub struct Agent {
    pub state: AgentState,
    pub thinking_level: ModelThinkingLevel,
    pub extensions: Vec<Extension>,
    pub tools: Vec<ToolRegistration>,
    pub callable: Vec<ToolRegistration>,
    pub deferred: Vec<ToolRegistration>,
    pub sections: Vec<PromptSection>,
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ReplayPolicy {
    Safe,
    #[default]
    Unsafe,
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ToolExecutionMode {
    #[default]
    Parallel,
    Sequential,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ToolCaller {
    Model,
    Tools,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ToolParent {
    pub task_id: TaskId,
    pub call_id: String,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ToolHookCall {
    #[serde(flatten)]
    pub call: ToolCall,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<ToolParent>,
}
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ToolControl {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub add_tools: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminate: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub handoff: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DiagnosticSeverity {
    Info,
    Warn,
    Error,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolDiagnostic {
    pub severity: DiagnosticSeverity,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
}
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ToolExecutionResult {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<Vec<InputContent>>,
    #[serde(default, skip_serializing_if = "Presence::is_missing")]
    pub structured_output: Presence<JsonValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_error: Option<bool>,
    #[serde(default, skip_serializing_if = "Presence::is_missing")]
    pub details: Presence<JsonValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostics: Option<Vec<ToolDiagnostic>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<Usage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub control: Option<ToolControl>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NestedToolExecutionResult {
    pub task_id: TaskId,
    #[serde(default, skip_serializing_if = "Presence::is_missing")]
    pub structured_output: Presence<JsonValue>,
    pub is_error: bool,
    #[serde(default, skip_serializing_if = "Presence::is_missing")]
    pub details: Presence<JsonValue>,
    pub diagnostics: Vec<ToolDiagnostic>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<Usage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<SafeUnsigned>,
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OutputRetention {
    #[default]
    Head,
    Tail,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OutputLimits {
    pub max_bytes: usize,
    pub max_lines: usize,
    pub retain: OutputRetention,
}
impl Default for OutputLimits {
    fn default() -> Self {
        Self {
            max_bytes: 50 * 1024,
            max_lines: 2000,
            retain: OutputRetention::Head,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OutputSkip {
    pub bytes: usize,
    pub newlines: usize,
    pub ends_with_newline: bool,
}
#[derive(Clone, Debug)]
pub enum OutputChunk {
    Text(String),
    Bytes(Vec<u8>),
}
#[derive(Clone, Debug)]
pub enum ToolProgress {
    Output {
        chunk: OutputChunk,
        skipped: Option<OutputSkip>,
    },
    Details {
        value: JsonValue,
    },
    Diagnostic {
        diagnostic: ToolDiagnostic,
    },
}
#[derive(Clone, Debug)]
pub struct ToolProgressOptions {
    pub enabled: bool,
    pub output_limits: OutputLimits,
    pub output_window: Option<OutputWindow>,
}
#[derive(Clone, Debug)]
pub struct OutputWindow {
    pub max_bytes: usize,
    pub max_lines: usize,
    pub min_interval_ms: f64,
    pub bytes_per_second: usize,
}
#[derive(Clone, Debug)]
pub struct RetainedOutput {
    pub text: String,
    pub truncated: bool,
}
#[derive(Clone, Debug)]
pub struct ToolInvocationIdentity {
    pub invocation: InvocationIdentity,
    pub conversation_id: ConversationId,
    pub call_id: String,
    pub parent: Option<ToolParent>,
}
#[derive(Clone, Debug, Default)]
pub struct NestedToolOptions {
    pub key: Option<String>,
    pub progress: Option<bool>,
}
#[derive(Clone)]
pub struct ToolInvocation {
    pub call: ToolHookCall,
    pub api: ToolExecutionApi,
}
pub type ToolExecutor =
    Arc<dyn Fn(ToolInvocation) -> RuntimeFuture<ToolExecutionResult> + Send + Sync>;
#[derive(Clone)]
pub struct ToolRegistration {
    pub declaration: Tool,
    pub replay: ReplayPolicy,
    pub execution_mode: Option<ToolExecutionMode>,
    pub callers: Option<Vec<ToolCaller>>,
    pub deferred: bool,
    pub output_limits: OutputLimits,
    pub structured_output_schema: Option<JsonSchema>,
    pub prepare_arguments: Option<Arc<dyn Fn(JsonObject) -> Result<JsonObject> + Send + Sync>>,
    pub execute: ToolExecutor,
}
impl ToolRegistration {
    pub fn new(
        declaration: Tool,
        execute: impl Fn(ToolInvocation) -> RuntimeFuture<ToolExecutionResult> + Send + Sync + 'static,
    ) -> Self {
        Self {
            declaration,
            replay: ReplayPolicy::Unsafe,
            execution_mode: None,
            callers: None,
            deferred: false,
            output_limits: Default::default(),
            structured_output_schema: None,
            prepare_arguments: None,
            execute: Arc::new(execute),
        }
    }
    pub fn name(&self) -> &str {
        self.declaration.name.as_str()
    }
}
#[derive(Clone)]
pub struct PromptInput {
    pub conversation_id: ConversationId,
    pub agent: Agent,
    pub shown: Vec<(String, String)>,
    pub api: HookApi,
}
#[derive(Clone)]
pub struct PromptSection {
    pub key: String,
    pub tag: bool,
    pub render: Arc<dyn Fn(PromptInput) -> RuntimeFuture<Option<String>> + Send + Sync>,
}
#[derive(Clone)]
pub enum Wrap {
    Tool {
        name: String,
        wrap: Arc<dyn Fn(ToolRegistration) -> Result<ToolRegistration> + Send + Sync>,
    },
    Section {
        key: String,
        wrap: Arc<dyn Fn(PromptSection) -> Result<PromptSection> + Send + Sync>,
    },
}
#[derive(Clone, Default)]
pub struct Extension {
    pub name: String,
    pub tools: Vec<ToolRegistration>,
    pub sections: Vec<PromptSection>,
    pub hooks: Hooks,
    pub wraps: Vec<Wrap>,
    pub tasks: Vec<TaskDefinition>,
}
#[derive(Clone)]
pub struct HookApi {
    pub(crate) phase: super::facade::Phase,
}
#[derive(Clone, Debug, Default)]
pub struct BeforeToolResult {
    pub arguments: Option<JsonObject>,
    pub block: Option<String>,
}
#[derive(Clone, Debug)]
pub enum CompactDecision {
    Decline,
    Summary(String),
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CompactionReason {
    Manual,
    Threshold,
    Overflow,
}
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CompactionRequest {
    pub reason: CompactionReason,
    pub entries: Vec<EntryRecord>,
    pub messages: Vec<Message>,
    pub first_kept: EntryId,
    pub instructions: Option<String>,
}
pub type BeforeRequestHook =
    Arc<dyn Fn(Vec<Message>, HookApi) -> RuntimeFuture<Option<Vec<Message>>> + Send + Sync>;
pub type AfterResponseHook =
    Arc<dyn Fn(AssistantMessage, HookApi) -> RuntimeFuture<()> + Send + Sync>;
pub type OnYieldHook =
    Arc<dyn Fn(AssistantMessage, HookApi) -> RuntimeFuture<Option<UserContent>> + Send + Sync>;
pub type AfterToolsHook =
    Arc<dyn Fn(EntryId, Vec<EntryId>, HookApi) -> RuntimeFuture<()> + Send + Sync>;
pub type BeforeToolHook =
    Arc<dyn Fn(ToolHookCall, HookApi) -> RuntimeFuture<Option<BeforeToolResult>> + Send + Sync>;
pub type AfterToolHook = Arc<
    dyn Fn(ToolHookCall, ToolExecutionResult, HookApi) -> RuntimeFuture<Option<ToolExecutionResult>>
        + Send
        + Sync,
>;
pub type BeforeCompactHook =
    Arc<dyn Fn(CompactionRequest, HookApi) -> RuntimeFuture<Option<CompactDecision>> + Send + Sync>;
#[derive(Clone, Default)]
pub struct Hooks {
    pub before_request: Option<BeforeRequestHook>,
    pub after_response: Option<AfterResponseHook>,
    pub on_yield: Option<OnYieldHook>,
    pub after_tools: Option<AfterToolsHook>,
    pub before_tool: Option<BeforeToolHook>,
    pub after_tool: Option<AfterToolHook>,
    pub before_compact: Option<BeforeCompactHook>,
}
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq)]
pub enum QueueMode {
    All,
    #[default]
    OneAtATime,
}
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub enum AfterFailedRun {
    #[default]
    Keep,
    Continue,
    Withdraw,
}
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ConversationStreamOptions {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transport: Option<pi_ai::Transport>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<SafeUnsigned>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_retries: Option<SafeUnsigned>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_retry_delay_ms: Option<SafeUnsigned>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub headers: Option<std::collections::BTreeMap<String, String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<pi_ai::JsonObject>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_retention: Option<pi_ai::CacheRetention>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deferred: Option<pi_ai::DeferredRequest>,
}
#[derive(Clone, Debug)]
pub struct CompactionPolicy {
    pub enabled: bool,
    pub reserve_tokens: u64,
    pub keep_recent_tokens: u64,
    pub background_tokens: u64,
}
impl Default for CompactionPolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            reserve_tokens: 16384,
            keep_recent_tokens: 20000,
            background_tokens: 32768,
        }
    }
}
#[derive(Clone)]
pub struct DurableSettings {
    pub extensions: Option<Vec<String>>,
    pub stream: ConversationStreamOptions,
    pub retry: pi_ai::RetryPolicy,
    pub compaction: CompactionPolicy,
    pub compaction_overrides: std::collections::BTreeMap<String, CompactionPolicy>,
    pub partial_interval_ms: f64,
    pub output_interval_ms: f64,
    pub tool_execution: ToolExecutionMode,
    pub steering_mode: QueueMode,
    pub follow_up_mode: QueueMode,
    pub after_failed_run: AfterFailedRun,
}
impl Default for DurableSettings {
    fn default() -> Self {
        Self {
            extensions: None,
            stream: Default::default(),
            retry: pi_ai::RetryPolicy {
                enabled: true,
                max_retries: pi_ai::MaxRetries::new(3).unwrap(),
                base_delay_ms: SafeUnsigned::new(2000).unwrap(),
                max_agent_delay_ms: Some(SafeUnsigned::new(60000).unwrap()),
            },
            compaction: Default::default(),
            compaction_overrides: Default::default(),
            partial_interval_ms: 100.0,
            output_interval_ms: 100.0,
            tool_execution: Default::default(),
            steering_mode: Default::default(),
            follow_up_mode: Default::default(),
            after_failed_run: Default::default(),
        }
    }
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CompactionResult {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entry_id: Option<EntryId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<SubmissionId>,
}
pub type ConversationInit =
    Arc<dyn Fn(&mut Transaction<'_>, ConversationId) -> Result<()> + Send + Sync>;
#[derive(Clone, Default)]
pub struct ConversationOptions {
    pub agent: Option<AgentChange>,
    pub init: Option<ConversationInit>,
}
#[derive(Clone)]
pub struct ConversationCreateOptions {
    pub ownership: ConversationOwnership,
    pub options: ConversationOptions,
}
