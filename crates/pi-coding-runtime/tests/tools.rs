use std::sync::Arc;

use pi_agent::schema::{
    AgentHarnessTool, AgentToolResult, BoxFuture, Cancellation, HarnessRuntimeError,
    ToolProgressSink,
};
use pi_ai::ToolCallId;
use pi_coding_runtime::{
    EnvError, ExecutionEnv, ExecutionToolContext, FileInfo, LocalExecutionEnv, ProcessOptions,
    ProcessOutput, create_bash_tool, create_edit_tool, create_find_tool, create_grep_tool,
    create_ls_tool, create_powershell_tool, create_read_tool, create_write_tool,
};
use serde_json::json;
use std::path::PathBuf;

struct NeverCancel;
impl Cancellation for NeverCancel {
    fn is_cancelled(&self) -> bool {
        false
    }
    fn cancelled(&self) -> BoxFuture<'_, ()> {
        Box::pin(std::future::pending())
    }
}
struct Progress;
impl ToolProgressSink for Progress {
    fn update(&self, _partial: AgentToolResult) -> BoxFuture<'_, Result<(), HarnessRuntimeError>> {
        Box::pin(async { Ok(()) })
    }
}
fn args(value: serde_json::Value) -> pi_ai::JsonObject {
    serde_json::from_value(value).unwrap()
}
async fn run(
    tool: &dyn AgentHarnessTool<ExecutionToolContext>,
    value: serde_json::Value,
    ctx: &ExecutionToolContext,
) -> AgentToolResult {
    tool.execute(
        "call".parse::<ToolCallId>().unwrap(),
        args(value),
        &NeverCancel,
        &Progress,
        ctx,
    )
    .await
    .unwrap()
}
fn text(result: &AgentToolResult) -> String {
    result
        .content
        .iter()
        .filter_map(|block| match block {
            pi_ai::InputContent::Text(value) => Some(value.text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn local_file_tools_read_write_edit_search_and_list() {
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(async_local_file_tools_read_write_edit_search_and_list());
}
#[test]
fn local_bash_returns_process_output_and_nonzero_errors() {
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(async_local_bash_returns_process_output_and_nonzero_errors());
}
#[test]
fn powershell_uses_injected_process_port_when_unavailable() {
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(async_powershell_uses_injected_process_port_when_unavailable());
}

async fn async_local_file_tools_read_write_edit_search_and_list() {
    let dir = TempDir::new();
    let env: Arc<dyn ExecutionEnv> = Arc::new(LocalExecutionEnv::new(dir.path()));
    let ctx = ExecutionToolContext { env };
    let written = run(
        create_write_tool().as_ref(),
        json!({"path":"src/a.txt","content":"alpha\nbeta\n"}),
        &ctx,
    )
    .await;
    assert!(text(&written).contains("Successfully wrote"));
    let read = run(
        create_read_tool().as_ref(),
        json!({"path":"src/a.txt","offset":2,"limit":1}),
        &ctx,
    )
    .await;
    assert!(text(&read).contains("beta"));
    let edited = run(
        create_edit_tool().as_ref(),
        json!({"path":"src/a.txt","edits":[{"oldText":"beta","newText":"gamma"}]}),
        &ctx,
    )
    .await;
    assert!(text(&edited).contains("Successfully replaced"));
    let grep = run(
        create_grep_tool().as_ref(),
        json!({"pattern":"gamma","path":"src","literal":true}),
        &ctx,
    )
    .await;
    assert!(text(&grep).contains("gamma"));
    let find = run(
        create_find_tool().as_ref(),
        json!({"pattern":"*.txt","path":"src"}),
        &ctx,
    )
    .await;
    assert!(text(&find).contains("a.txt"));
    let ls = run(create_ls_tool().as_ref(), json!({"path":"src"}), &ctx).await;
    assert!(text(&ls).contains("a.txt"));
}

async fn async_local_bash_returns_process_output_and_nonzero_errors() {
    let dir = TempDir::new();
    let ctx = ExecutionToolContext {
        env: Arc::new(LocalExecutionEnv::new(dir.path())),
    };
    let result = run(
        create_bash_tool().as_ref(),
        json!({"command":"printf hello"}),
        &ctx,
    )
    .await;
    assert_eq!(text(&result), "hello");
    let error = create_bash_tool()
        .execute(
            "call".parse().unwrap(),
            args(json!({"command":"exit 7"})),
            &NeverCancel,
            &Progress,
            &ctx,
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("exited with code 7"));
}

struct InjectedProcessEnv {
    inner: LocalExecutionEnv,
}
impl ExecutionEnv for InjectedProcessEnv {
    fn absolute_path<'a>(&'a self, path: &'a str) -> BoxFuture<'a, Result<String, EnvError>> {
        self.inner.absolute_path(path)
    }
    fn exists<'a>(&'a self, path: &'a str) -> BoxFuture<'a, Result<bool, EnvError>> {
        self.inner.exists(path)
    }
    fn read_binary_file<'a>(&'a self, path: &'a str) -> BoxFuture<'a, Result<Vec<u8>, EnvError>> {
        self.inner.read_binary_file(path)
    }
    fn read_text_file<'a>(&'a self, path: &'a str) -> BoxFuture<'a, Result<String, EnvError>> {
        self.inner.read_text_file(path)
    }
    fn write_file<'a>(
        &'a self,
        path: &'a str,
        content: &'a [u8],
    ) -> BoxFuture<'a, Result<(), EnvError>> {
        self.inner.write_file(path, content)
    }
    fn list_dir<'a>(&'a self, path: &'a str) -> BoxFuture<'a, Result<Vec<FileInfo>, EnvError>> {
        self.inner.list_dir(path)
    }
    fn file_info<'a>(&'a self, path: &'a str) -> BoxFuture<'a, Result<FileInfo, EnvError>> {
        self.inner.file_info(path)
    }
    fn canonical_path<'a>(&'a self, path: &'a str) -> BoxFuture<'a, Result<String, EnvError>> {
        self.inner.canonical_path(path)
    }
    fn exec<'a>(
        &'a self,
        shell: &'a str,
        command: &'a str,
        _options: ProcessOptions,
    ) -> BoxFuture<'a, Result<ProcessOutput, EnvError>> {
        Box::pin(async move {
            Ok(ProcessOutput {
                stdout: format!("{shell}:{command}"),
                stderr: String::new(),
                exit_code: 0,
            })
        })
    }
    fn create_temp_file<'a>(
        &'a self,
        prefix: &'a str,
        content: &'a [u8],
    ) -> BoxFuture<'a, Result<String, EnvError>> {
        self.inner.create_temp_file(prefix, content)
    }
}

async fn async_powershell_uses_injected_process_port_when_unavailable() {
    let dir = TempDir::new();
    let env: Arc<dyn ExecutionEnv> = Arc::new(InjectedProcessEnv {
        inner: LocalExecutionEnv::new(dir.path()),
    });
    let ctx = ExecutionToolContext { env };
    let result = run(
        create_powershell_tool().as_ref(),
        json!({"command":"Write-Output ok"}),
        &ctx,
    )
    .await;
    assert_eq!(text(&result), "powershell:Write-Output ok");
}

#[test]
fn descriptors_publish_all_tool_bindings_and_prompt_metadata() {
    let names: Vec<_> = pi_coding_runtime::TOOL_NAMES
        .iter()
        .map(|name| create_tool_name(name))
        .collect();
    assert_eq!(
        names,
        [
            "read",
            "write",
            "edit",
            "bash",
            "powershell",
            "grep",
            "find",
            "ls"
        ]
    );
    assert!(pi_coding_runtime::tool_prompt_metadata("edit").1.len() >= 2);
}
fn create_tool_name(name: &str) -> &str {
    name
}

static TEMP_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
struct TempDir {
    path: PathBuf,
}
impl TempDir {
    fn new() -> Self {
        let serial = TEMP_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("pi-coding-test-{}-{}", std::process::id(), serial));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();
        Self { path }
    }
    fn path(&self) -> &std::path::Path {
        &self.path
    }
}
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}
