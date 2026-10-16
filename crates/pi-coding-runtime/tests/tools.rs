use std::sync::Arc;

use pi_ai::{BoxFuture, CancellationToken};
use pi_coding_runtime::{
    CodingTool, EnvError, ExecutionEnv, ExecutionToolContext, FileInfo, LocalExecutionEnv,
    ProcessOptions, ProcessOutput, ProcessedImage, ToolOutput as AgentToolResult, ToolOutputUpdate,
    coding_tool,
};
use serde_json::json;
use std::path::PathBuf;

fn progress() -> ToolOutputUpdate {
    Arc::new(|_| {})
}
fn cancelled() -> CancellationToken {
    let token = CancellationToken::new();
    token.cancel();
    token
}
fn create_read_tool() -> Arc<dyn CodingTool> {
    coding_tool("read")
}
fn create_write_tool() -> Arc<dyn CodingTool> {
    coding_tool("write")
}
fn create_edit_tool() -> Arc<dyn CodingTool> {
    coding_tool("edit")
}
fn create_bash_tool() -> Arc<dyn CodingTool> {
    coding_tool("bash")
}
fn create_powershell_tool() -> Arc<dyn CodingTool> {
    coding_tool("powershell")
}
fn create_grep_tool() -> Arc<dyn CodingTool> {
    coding_tool("grep")
}
fn create_find_tool() -> Arc<dyn CodingTool> {
    coding_tool("find")
}
fn create_ls_tool() -> Arc<dyn CodingTool> {
    coding_tool("ls")
}

fn args(value: serde_json::Value) -> pi_ai::JsonObject {
    serde_json::from_value(value).unwrap()
}
async fn run(
    tool: &dyn CodingTool,
    value: serde_json::Value,
    ctx: &ExecutionToolContext,
) -> AgentToolResult {
    tool.execute(args(value), &CancellationToken::new(), &progress(), ctx)
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
    let details = serde_json::to_value(&edited.details).unwrap();
    assert_ne!(details.get("diff"), details.get("patch"));
    assert!(
        details
            .get("diff")
            .and_then(|value| value.as_str())
            .unwrap()
            .contains("-2 beta")
    );
    assert!(
        details
            .get("diff")
            .and_then(|value| value.as_str())
            .unwrap()
            .contains(" 1 alpha")
    );
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
            args(json!({"command":"exit 7"})),
            &CancellationToken::new(),
            &progress(),
            &ctx,
        )
        .await
        .unwrap();
    assert_eq!(error.is_error, Some(true));
    assert!(text(&error).contains("exited with code 7"));
    assert_eq!(
        serde_json::to_value(error.structured_content).unwrap()["exit_code"],
        7
    );
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
        options: &'a pi_coding_runtime::ReadToolOptions,
    ) -> BoxFuture<'a, Result<Option<ProcessedImage>, EnvError>> {
        Box::pin(async move {
            Ok(Some(ProcessedImage {
                data: "processed".into(),
                mime_type: mime_type.into(),
                hints: vec![format!("autoResize={}", options.auto_resize)],
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

struct BlockingWriteEnv {
    inner: LocalExecutionEnv,
    started: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
    block_first: std::sync::atomic::AtomicBool,
    cancel_on_canonical: Option<CancellationToken>,
    canonical_observed: Option<Arc<tokio::sync::Notify>>,
}
impl ExecutionEnv for BlockingWriteEnv {
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
        let block = self
            .block_first
            .swap(false, std::sync::atomic::Ordering::Relaxed);
        let started = Arc::clone(&self.started);
        let release = Arc::clone(&self.release);
        let inner = &self.inner;
        Box::pin(async move {
            if block {
                started.notify_one();
                release.notified().await;
            }
            inner.write_file(path, content).await
        })
    }
    fn list_dir<'a>(&'a self, path: &'a str) -> BoxFuture<'a, Result<Vec<FileInfo>, EnvError>> {
        self.inner.list_dir(path)
    }
    fn file_info<'a>(&'a self, path: &'a str) -> BoxFuture<'a, Result<FileInfo, EnvError>> {
        self.inner.file_info(path)
    }
    fn canonical_path<'a>(&'a self, path: &'a str) -> BoxFuture<'a, Result<String, EnvError>> {
        Box::pin(async move {
            let result = self.inner.canonical_path(path).await;
            if let Some(observed) = &self.canonical_observed {
                observed.notify_one();
            }
            if let Some(cancel) = &self.cancel_on_canonical {
                cancel.cancel();
            }
            result
        })
    }
    fn exec<'a>(
        &'a self,
        shell: &'a str,
        command: &'a str,
        options: ProcessOptions,
    ) -> BoxFuture<'a, Result<ProcessOutput, EnvError>> {
        self.inner.exec(shell, command, options)
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
    std::fs::create_dir(dir.path().join(".git")).unwrap();
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
fn nested_gitignore_rules_are_scoped_and_negations_reinclude_descendants() {
    let dir = TempDir::new();
    std::fs::create_dir_all(dir.path().join("one/nested")).unwrap();
    std::fs::create_dir_all(dir.path().join("two/nested")).unwrap();
    std::fs::write(
        dir.path().join("one/.gitignore"),
        "nested/*\n!nested/keep.txt\n",
    )
    .unwrap();
    std::fs::write(dir.path().join("one/nested/drop.txt"), "needle\n").unwrap();
    std::fs::write(dir.path().join("one/nested/keep.txt"), "needle\n").unwrap();
    std::fs::write(dir.path().join("two/nested/sibling.txt"), "needle\n").unwrap();
    let ctx = ExecutionToolContext {
        env: Arc::new(LocalExecutionEnv::new(dir.path())),
    };
    let result = tokio::runtime::Runtime::new().unwrap().block_on(async {
        run(
            create_find_tool().as_ref(),
            json!({"pattern":"**/*.txt","path":"."}),
            &ctx,
        )
        .await
    });
    let output = text(&result);
    assert!(output.contains("one/nested/keep.txt"));
    assert!(!output.contains("one/nested/drop.txt"));
    assert!(output.contains("two/nested/sibling.txt"));
}

#[test]
fn grep_root_file_uses_basename_and_unicode_line_truncation_is_safe() {
    let dir = TempDir::new();
    let content = format!("{} needle", "界".repeat(600));
    std::fs::write(dir.path().join("unicode.txt"), content).unwrap();
    let ctx = ExecutionToolContext {
        env: Arc::new(LocalExecutionEnv::new(dir.path())),
    };
    let result = tokio::runtime::Runtime::new().unwrap().block_on(async {
        run(
            create_grep_tool().as_ref(),
            json!({"pattern":"needle","path":"unicode.txt","literal":true}),
            &ctx,
        )
        .await
    });
    let output = text(&result);
    assert!(output.starts_with("unicode.txt:1: "));
    assert!(output.contains("[truncated]"));
    assert!(
        serde_json::to_value(&result.details)
            .unwrap()
            .get("linesTruncated")
            .is_some()
    );
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
        tool.execute(prepared, &CancellationToken::new(), &progress(), &ctx)
            .await
            .unwrap();
    });
    assert_eq!(
        std::fs::read_to_string(dir.path().join("edit.txt")).unwrap(),
        "changed\nkeep  \n"
    );
    std::fs::write(dir.path().join("nfkc.txt"), "ﬀ  tail\n").unwrap();
    let nfkc_args = tool
        .prepare_arguments(pi_ai::JsonValue::Object(args(json!({
            "path":"nfkc.txt", "oldText":"ff", "newText":"new　 text"
        }))))
        .unwrap();
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        tool.execute(nfkc_args, &CancellationToken::new(), &progress(), &ctx)
            .await
            .unwrap();
    });
    assert_eq!(
        std::fs::read_to_string(dir.path().join("nfkc.txt")).unwrap(),
        "new　 text  tail\n"
    );
    std::fs::write(dir.path().join("composed.txt"), "e\u{301} before\n").unwrap();
    let composed_args = tool
        .prepare_arguments(pi_ai::JsonValue::Object(args(json!({
            "path":"composed.txt", "oldText":"é", "newText":"E"
        }))))
        .unwrap();
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        tool.execute(composed_args, &CancellationToken::new(), &progress(), &ctx)
            .await
            .unwrap();
    });
    assert_eq!(
        std::fs::read_to_string(dir.path().join("composed.txt")).unwrap(),
        "E before\n"
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
                args(json!({"command":"sleep 1","timeout":0.05})),
                &CancellationToken::new(),
                &progress(),
                &local_ctx,
            )
            .await
            .unwrap_err()
    });
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
                args(json!({"path":"cancelled.txt","content":"must not write"})),
                &cancelled(),
                &progress(),
                &ctx,
            )
            .await
    });
    assert!(result.is_err());
    assert!(!dir.path().join("cancelled.txt").exists());
}

#[test]
fn cancellation_is_rechecked_after_waiting_for_mutation_lock() {
    let dir = TempDir::new();
    let cancellation = CancellationToken::new();
    let ctx = ExecutionToolContext {
        env: Arc::new(BlockingWriteEnv {
            inner: LocalExecutionEnv::new(dir.path()),
            started: Arc::new(tokio::sync::Notify::new()),
            release: Arc::new(tokio::sync::Notify::new()),
            block_first: std::sync::atomic::AtomicBool::new(false),
            cancel_on_canonical: Some(cancellation.clone()),
            canonical_observed: None,
        }),
    };
    let result = tokio::runtime::Runtime::new().unwrap().block_on(async {
        create_write_tool()
            .execute(
                args(json!({"path":"queued.txt","content":"must not write"})),
                &cancellation,
                &progress(),
                &ctx,
            )
            .await
    });
    assert!(result.is_err());
    assert!(!dir.path().join("queued.txt").exists());
}

#[test]
fn queued_cancellation_waits_on_same_path_lock_then_aborts_before_write() {
    let dir = TempDir::new();
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let env = Arc::new(BlockingWriteEnv {
        inner: LocalExecutionEnv::new(dir.path()),
        started: Arc::clone(&started),
        release: Arc::clone(&release),
        block_first: std::sync::atomic::AtomicBool::new(true),
        cancel_on_canonical: None,
        canonical_observed: None,
    });
    let ctx = ExecutionToolContext { env };
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let first_tool = create_write_tool();
        let second_tool = create_write_tool();
        let first_ctx = ctx.clone();
        let second_ctx = ctx.clone();
        let first_cancel = Arc::new(CancellationToken::new());
        let second_cancel = Arc::new(CancellationToken::new());
        let first_progress = Arc::new(progress());
        let second_progress = Arc::new(progress());
        let second_cancel_for_flip = Arc::clone(&second_cancel);
        let first_progress_for_task = Arc::clone(&first_progress);
        let second_progress_for_task = Arc::clone(&second_progress);
        let first = tokio::spawn(async move {
            first_tool
                .execute(
                    args(json!({"path":"same.txt","content":"first"})),
                    first_cancel.as_ref(),
                    first_progress_for_task.as_ref(),
                    &first_ctx,
                )
                .await
        });
        started.notified().await;
        let second = tokio::spawn(async move {
            second_tool
                .execute(
                    args(json!({"path":"same.txt","content":"second"})),
                    second_cancel.as_ref(),
                    second_progress_for_task.as_ref(),
                    &second_ctx,
                )
                .await
        });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        second_cancel_for_flip.cancel();
        release.notify_one();
        assert!(first.await.unwrap().is_ok());
        assert!(second.await.unwrap().is_err());
    });
    assert_eq!(
        std::fs::read_to_string(dir.path().join("same.txt")).unwrap(),
        "first"
    );
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

#[cfg(unix)]
#[test]
fn mutation_alias_queue_keeps_inflight_cancelled_write_and_allows_other_paths() {
    let dir = TempDir::new();
    std::fs::write(dir.path().join("same.txt"), "initial").unwrap();
    std::os::unix::fs::symlink("same.txt", dir.path().join("alias.txt")).unwrap();
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let canonical = Arc::new(tokio::sync::Notify::new());
    let context = ExecutionToolContext {
        env: Arc::new(BlockingWriteEnv {
            inner: LocalExecutionEnv::new(dir.path()),
            started: started.clone(),
            release: release.clone(),
            block_first: std::sync::atomic::AtomicBool::new(true),
            cancel_on_canonical: None,
            canonical_observed: Some(canonical.clone()),
        }),
    };
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let cancel = CancellationToken::new();
        let first_cancel = cancel.clone();
        let first_context = context.clone();
        let first = tokio::spawn(async move {
            coding_tool("write")
                .execute(
                    args(json!({"path":"same.txt","content":"first"})),
                    &first_cancel,
                    &progress(),
                    &first_context,
                )
                .await
        });
        started.notified().await;
        canonical.notified().await;
        cancel.cancel();
        let other = coding_tool("write");
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            other.execute(
                args(json!({"path":"other.txt","content":"parallel"})),
                &CancellationToken::new(),
                &progress(),
                &context,
            ),
        )
        .await
        .unwrap()
        .unwrap();
        canonical.notified().await;
        let second_context = context.clone();
        let second = tokio::spawn(async move {
            coding_tool("edit")
                .execute(
                    args(
                        json!({"path":"alias.txt","edits":[{"oldText":"first","newText":"final"}]}),
                    ),
                    &CancellationToken::new(),
                    &progress(),
                    &second_context,
                )
                .await
        });
        canonical.notified().await;
        assert!(!second.is_finished());
        assert_eq!(
            std::fs::read_to_string(dir.path().join("same.txt")).unwrap(),
            "initial"
        );
        release.notify_one();
        assert!(first.await.unwrap().is_err());
        assert!(second.await.unwrap().is_ok());
    });
    assert_eq!(
        std::fs::read_to_string(dir.path().join("same.txt")).unwrap(),
        "final"
    );
    assert_eq!(
        std::fs::read_to_string(dir.path().join("other.txt")).unwrap(),
        "parallel"
    );
}
