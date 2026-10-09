use std::sync::Arc;

use pi_agent::schema::{
    AgentHarnessTool, AgentToolResult, BoxFuture, Cancellation, HarnessRuntimeError,
    ToolProgressSink,
};
use pi_ai::ToolCallId;
use pi_coding_runtime::{
    EnvError, ExecutionEnv, ExecutionToolContext, FileInfo, LocalExecutionEnv, ProcessOptions,
    ProcessOutput, ProcessedImage, create_bash_tool, create_edit_tool, create_find_tool,
    create_grep_tool, create_ls_tool, create_powershell_tool, create_read_tool, create_write_tool,
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
struct Cancelled;
impl Cancellation for Cancelled {
    fn is_cancelled(&self) -> bool {
        true
    }
    fn cancelled(&self) -> BoxFuture<'_, ()> {
        Box::pin(async {})
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
    fn process_image<'a>(
        &'a self,
        _bytes: &'a [u8],
        mime_type: &'a str,
        auto_resize: bool,
    ) -> BoxFuture<'a, Result<Option<ProcessedImage>, EnvError>> {
        Box::pin(async move {
            Ok(Some(ProcessedImage {
                data: "processed".into(),
                mime_type: mime_type.into(),
                hints: vec![format!("autoResize={auto_resize}")],
            }))
        })
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
                merged: (command == "order").then(|| "stderr-first\nstdout-second".to_owned()),
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
    assert!(text(&result).contains("powershell:try { [Console]::OutputEncoding"));
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
    assert!(
        pi_coding_runtime::tool_prompt_metadata("edit")
            .1
            .iter()
            .any(|item| item.contains("original file"))
    );
}
fn create_tool_name(name: &str) -> &str {
    name
}

#[test]
fn searches_honor_gitignore_relative_paths_limits_and_truncation() {
    let dir = TempDir::new();
    std::fs::write(dir.path().join(".gitignore"), "ignored.txt\nignored-dir/\n").unwrap();
    std::fs::write(dir.path().join("visible.txt"), "needle\n").unwrap();
    std::fs::write(dir.path().join("ignored.txt"), "needle\n").unwrap();
    std::fs::create_dir_all(dir.path().join("ignored-dir")).unwrap();
    std::fs::write(dir.path().join("ignored-dir/no.txt"), "needle\n").unwrap();
    let ctx = ExecutionToolContext {
        env: Arc::new(LocalExecutionEnv::new(dir.path())),
    };
    let grep = tokio::runtime::Runtime::new().unwrap().block_on(async {
        run(
            create_grep_tool().as_ref(),
            json!({"pattern":"needle","path":".","literal":true,"limit":0}),
            &ctx,
        )
        .await
    });
    let grep_text = text(&grep);
    assert!(grep_text.contains("visible.txt"));
    assert!(!grep_text.contains("ignored.txt"));
    assert_eq!(
        serde_json::to_value(&grep.details)
            .unwrap()
            .get("matchLimitReached")
            .and_then(|v| v.as_u64()),
        Some(1)
    );
    let find = tokio::runtime::Runtime::new().unwrap().block_on(async {
        run(
            create_find_tool().as_ref(),
            json!({"pattern":"*.txt","path":".","limit":0}),
            &ctx,
        )
        .await
    });
    let find_text = text(&find);
    assert!(find_text.contains("visible.txt"));
    assert!(!find_text.contains("ignored.txt"));
}

#[test]
fn ls_records_byte_truncation_details() {
    let dir = TempDir::new();
    for index in 0..300 {
        std::fs::write(
            dir.path()
                .join(format!("{}-{}.txt", "x".repeat(190), index)),
            "x",
        )
        .unwrap();
    }
    let ctx = ExecutionToolContext {
        env: Arc::new(LocalExecutionEnv::new(dir.path())),
    };
    let result = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(async { run(create_ls_tool().as_ref(), json!({"path":"."}), &ctx).await });
    let details = serde_json::to_value(&result.details).unwrap();
    assert!(details.get("truncation").is_some());
}

#[test]
fn edit_prepare_compatibility_and_fuzzy_matching_preserve_untouched_bytes() {
    let dir = TempDir::new();
    std::fs::write(dir.path().join("edit.txt"), "wide　line  \nkeep  \n").unwrap();
    let ctx = ExecutionToolContext {
        env: Arc::new(LocalExecutionEnv::new(dir.path())),
    };
    let tool = create_edit_tool();
    let prepared = tool
        .prepare_arguments(pi_ai::JsonValue::Object(args(
            json!({"path":"edit.txt","oldText":"wide line","newText":"changed"}),
        )))
        .unwrap();
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        tool.execute(
            "call".parse().unwrap(),
            prepared,
            &NeverCancel,
            &Progress,
            &ctx,
        )
        .await
        .unwrap();
    });
    assert_eq!(
        std::fs::read_to_string(dir.path().join("edit.txt")).unwrap(),
        "changed\nkeep  \n"
    );
    let prepared_json = tool
        .prepare_arguments(pi_ai::JsonValue::Object(args(
            json!({"path":"edit.txt","edits":"[{\"oldText\":\"changed\",\"newText\":\"done\"}]"}),
        )))
        .unwrap();
    assert!(matches!(
        prepared_json.as_map().get("edits"),
        Some(pi_ai::JsonValue::Array(_))
    ));
}

#[test]
fn read_uses_host_image_processing_and_bash_honors_timeout() {
    let dir = TempDir::new();
    std::fs::write(
        dir.path().join("image.bmp"),
        [
            b'B', b'M', 0, 0, 0, 0, 0, 0, 0, 0, 54, 0, 0, 0, 40, 0, 0, 0, 1, 0, 1, 0, 24, 0, 0, 0,
        ],
    )
    .unwrap();
    let ctx = ExecutionToolContext {
        env: Arc::new(InjectedProcessEnv {
            inner: LocalExecutionEnv::new(dir.path()),
        }),
    };
    let result = tokio::runtime::Runtime::new().unwrap().block_on(async {
        run(
            create_read_tool().as_ref(),
            json!({"path":"image.bmp"}),
            &ctx,
        )
        .await
    });
    assert!(text(&result).contains("autoResize=true"));
    let local_ctx = ExecutionToolContext {
        env: Arc::new(LocalExecutionEnv::new(dir.path())),
    };
    let error = tokio::runtime::Runtime::new().unwrap().block_on(async {
        create_bash_tool()
            .execute(
                "call".parse().unwrap(),
                args(json!({"command":"sleep 1","timeout":0.05})),
                &NeverCancel,
                &Progress,
                &local_ctx,
            )
            .await
            .unwrap_err()
    });
    println!("TIMEOUT_ERROR={}", error);
    assert!(error.to_string().contains("timed out"));
}

#[test]
fn cancelled_mutation_is_rejected_before_host_write() {
    let dir = TempDir::new();
    let ctx = ExecutionToolContext {
        env: Arc::new(LocalExecutionEnv::new(dir.path())),
    };
    let result = tokio::runtime::Runtime::new().unwrap().block_on(async {
        create_write_tool()
            .execute(
                "call".parse().unwrap(),
                args(json!({"path":"cancelled.txt","content":"must not write"})),
                &Cancelled,
                &Progress,
                &ctx,
            )
            .await
    });
    assert!(result.is_err());
    assert!(!dir.path().join("cancelled.txt").exists());
}

#[test]
fn shell_preserves_host_merged_output_order() {
    let dir = TempDir::new();
    let ctx = ExecutionToolContext {
        env: Arc::new(InjectedProcessEnv {
            inner: LocalExecutionEnv::new(dir.path()),
        }),
    };
    let result = tokio::runtime::Runtime::new().unwrap().block_on(async {
        run(
            create_bash_tool().as_ref(),
            json!({"command":"order"}),
            &ctx,
        )
        .await
    });
    assert_eq!(text(&result), "stderr-first\nstdout-second");
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
