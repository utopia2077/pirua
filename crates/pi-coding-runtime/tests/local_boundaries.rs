use pi_agent_schema::{AgentTool, AgentToolResult, ToolInvocation};
use pi_ai::{CancellationToken, InputContent};
use pi_coding_runtime::*;
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
struct TempDir(PathBuf);
impl TempDir {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "pirua-local-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
    fn path(&self) -> &Path {
        &self.0
    }
    fn write(&self, path: &str, text: &str) {
        let path = self.0.join(path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, text).unwrap();
    }
    fn context(&self) -> ExecutionToolContext {
        ExecutionToolContext {
            env: Arc::new(LocalExecutionEnv::new(&self.0)),
        }
    }
}
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn text(result: &AgentToolResult) -> String {
    result
        .content
        .iter()
        .filter_map(|value| {
            if let InputContent::Text(text) = value {
                Some(text.text.as_str())
            } else {
                None
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}
async fn invoke(
    tool: &AgentTool,
    args: Value,
) -> Result<AgentToolResult, pi_agent_schema::AgentError> {
    let args = serde_json::from_value(args).unwrap();
    let arguments = if let Some(prepare) = &tool.prepare_arguments {
        prepare(args)?
    } else {
        args
    };
    (tool.execute)(ToolInvocation {
        call_id: "detached-test".into(),
        arguments,
        cancellation: CancellationToken::new(),
        on_update: Arc::new(|_| {}),
    })
    .await
}
fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().unwrap()
}

#[test]
fn agent_factories_capture_only_explicit_environment_and_return_release_shapes() {
    let dir = TempDir::new();
    let context = dir.context();
    runtime().block_on(async {
        let write = create_write_tool(context.clone());
        let result = invoke(&write, json!({"path":"a.txt","content":"alpha\nbeta\n"}))
            .await
            .unwrap();
        assert!(text(&result).contains("Successfully wrote"));
        let edit = create_edit_tool(context.clone());
        invoke(
            &edit,
            json!({"path":"a.txt","edits":"{\"oldText\":\"beta\",\"newText\":\"gamma\"}"}),
        )
        .await
        .unwrap();
        let read = create_read_tool(context.clone());
        assert!(read.output_schema.is_some());
        let result = invoke(&read, json!({"path":"a.txt"})).await.unwrap();
        assert_eq!(text(&result), "alpha\ngamma\n");
        assert_eq!(
            serde_json::to_value(result.structured_content).unwrap(),
            json!("alpha\ngamma\n")
        );
        for tool in create_tools(context) {
            assert!(tool.declaration.constrained_sampling.is_some());
        }
    });
}

#[cfg(unix)]
#[test]
fn local_shell_cancellation_kills_children_and_settles_before_delayed_effect() {
    let dir = TempDir::new();
    let tool = create_bash_tool(dir.context());
    runtime().block_on(async {
        let cancellation = CancellationToken::new();
        let cancel = cancellation.clone();
        let started = Instant::now();
        let result = (tool.execute)(ToolInvocation {
            call_id: "cancel".into(),
            arguments: serde_json::from_value(
                json!({"command":"(sleep 0.5; printf leaked > marker) & printf ready; wait"}),
            )
            .unwrap(),
            cancellation,
            on_update: Arc::new(move |result| {
                if text(&result).contains("ready") {
                    cancel.cancel();
                }
            }),
        })
        .await;
        assert!(result.unwrap_err().to_string().contains("aborted"));
        assert!(started.elapsed() < Duration::from_secs(2));
        tokio::time::sleep(Duration::from_millis(650)).await;
        assert!(!dir.path().join("marker").exists());
    });
}

#[cfg(unix)]
#[test]
fn successful_shell_grace_tracks_pipe_idleness_and_preserves_background_work() {
    let dir = TempDir::new();
    let tool = create_bash_tool(dir.context());
    runtime().block_on(async {
        let result = invoke(
            &tool,
            json!({"command":"(for n in 1 2 3 4 5 6; do printf x; sleep 0.04; done) &"}),
        )
        .await
        .unwrap();
        assert_eq!(text(&result), "xxxxxx");
        let started = Instant::now();
        let result = invoke(
            &tool,
            json!({"command":"(sleep 0.5; printf done > background-marker) &"}),
        )
        .await
        .unwrap();
        assert_eq!(text(&result), "(no output)");
        assert!(started.elapsed() < Duration::from_millis(450));
        tokio::time::sleep(Duration::from_millis(550)).await;
        assert_eq!(
            std::fs::read_to_string(dir.path().join("background-marker")).unwrap(),
            "done"
        );
    });
}

#[cfg(unix)]
#[test]
fn shell_streams_ordered_bytes_spills_full_output_and_bounds_structured_content() {
    let dir = TempDir::new();
    let tool = create_bash_tool(dir.context());
    runtime().block_on(async {
        let order = invoke(
            &tool,
            json!({"command":"printf first >&2; sleep 0.03; printf second"}),
        )
        .await
        .unwrap();
        assert_eq!(text(&order), "firstsecond");
        let updates = Arc::new(Mutex::new(Vec::new()));
        let capture = updates.clone();
        let result = (tool.execute)(ToolInvocation {
            call_id: "large".into(),
            arguments: serde_json::from_value(
                json!({"command":"head -c 1200000 /dev/zero | tr '\\0' x"}),
            )
            .unwrap(),
            cancellation: CancellationToken::new(),
            on_update: Arc::new(move |result| capture.lock().unwrap().push(result)),
        })
        .await
        .unwrap();
        assert!(updates.lock().unwrap().len() >= 2);
        let detail = serde_json::to_value(result.details).unwrap();
        let path = detail["fullOutputPath"].as_str().unwrap();
        assert_eq!(std::fs::metadata(path).unwrap().len(), 1_200_000);
        assert_eq!(std::fs::read(path).unwrap(), vec![b'x'; 1_200_000]);
        let structured = serde_json::to_value(result.structured_content).unwrap();
        assert_eq!(structured["exit_code"], 0);
        assert_eq!(structured["truncated"], true);
        let output = structured["output"].as_str().unwrap();
        assert!(output.contains("151424 bytes omitted"));
        assert!(output.len() < 1_049_000);
        assert!(
            text(&AgentToolResult {
                content: result.content,
                ..Default::default()
            })
            .len()
                < 52_000
        );
        std::fs::remove_file(path).unwrap();
    });
}

#[cfg(unix)]
#[test]
fn shell_timeout_validation_and_process_deadline_cross_real_boundary() {
    let dir = TempDir::new();
    let tool = create_bash_tool(dir.context());
    runtime().block_on(async {
        for timeout in [json!(0), json!(-1), json!(2147484), json!("bad")] {
            assert!(
                invoke(
                    &tool,
                    json!({"command":"touch forbidden","timeout":timeout})
                )
                .await
                .unwrap_err()
                .to_string()
                .contains("Invalid timeout")
            );
        }
        assert!(!dir.path().join("forbidden").exists());
        let error = invoke(
            &tool,
            json!({"command":"sleep 0.3; printf late > marker","timeout":0.02}),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("timed out after 0.02 seconds"));
        tokio::time::sleep(Duration::from_millis(350)).await;
        assert!(!dir.path().join("marker").exists());
    });
}

#[test]
fn read_paths_normalize_at_spaces_dot_segments_file_urls_and_screenshot_aliases() {
    use unicode_normalization::UnicodeNormalization;
    let dir = TempDir::new();
    dir.write("a b.txt", "spaces");
    dir.write("Screenshot 1\u{202f}PM.png", "screenshot");
    dir.write(&"décomposé.txt".nfd().collect::<String>(), "nfd");
    dir.write("Capture d’écran.txt", "quote");
    runtime().block_on(async {
        let read = create_read_tool(dir.context());
        for (path, expected) in [
            ("@./sub/../a\u{2007}b.txt".to_owned(), "spaces"),
            (
                url::Url::from_file_path(dir.path().join("a b.txt"))
                    .unwrap()
                    .to_string(),
                "spaces",
            ),
            ("Screenshot 1 PM.png".into(), "screenshot"),
            ("décomposé.txt".into(), "nfd"),
            ("Capture d'écran.txt".into(), "quote"),
        ] {
            assert_eq!(
                text(&invoke(&read, json!({"path":path})).await.unwrap()),
                expected
            );
        }
        assert_eq!(
            dir.context().env.absolute_path("~").await.unwrap(),
            std::env::var(if cfg!(windows) { "USERPROFILE" } else { "HOME" }).unwrap()
        );
    });
}

#[test]
fn local_bmp_conversion_and_resize_preserve_vision_content_and_metadata() {
    let dir = TempDir::new();
    let bmp = image::DynamicImage::new_rgb8(2, 1);
    bmp.save_with_format(dir.path().join("a.bmp"), image::ImageFormat::Bmp)
        .unwrap();
    let large = image::DynamicImage::new_rgb8(2100, 10);
    large
        .save_with_format(dir.path().join("large.png"), image::ImageFormat::Png)
        .unwrap();
    runtime().block_on(async {
        let read = create_read_tool(dir.context());
        let bmp = invoke(&read, json!({"path":"a.bmp"})).await.unwrap();
        assert!(bmp.content.iter().any(
            |block| matches!(block,InputContent::Image(image) if image.mime_type=="image/png")
        ));
        let large = invoke(&read, json!({"path":"large.png"})).await.unwrap();
        assert!(text(&large).contains("original 2100x10, displayed at 2000x10"));
        let structured = serde_json::to_value(large.structured_content).unwrap();
        assert_eq!(structured["type"], "image");
        assert_eq!(structured["mimeType"], "image/png");
    });
}

#[test]
fn model_image_profiles_apply_without_session_state_and_report_omissions() {
    let dir = TempDir::new();
    image::DynamicImage::new_rgb8(16, 8)
        .save_with_format(dir.path().join("a.png"), image::ImageFormat::Png)
        .unwrap();
    image::DynamicImage::new_rgb8(2, 1)
        .save_with_format(dir.path().join("a.bmp"), image::ImageFormat::Bmp)
        .unwrap();
    let mut model: pi_ai::Model = serde_json::from_value(json!({"provider":"custom","id":"profile","name":"Profile","api":"openai-completions","baseUrl":"https://example.invalid","reasoning":false,"input":["text"],"cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0},"contextWindow":1000,"maxTokens":100,"inputLimits":{"images":{"resize":{"maxWidth":4,"maxHeight":4}}}})).unwrap();
    runtime().block_on(async {
        let read = create_read_tool_with_options(dir.context(), ReadToolOptions::for_model(&model));
        let result = invoke(&read, json!({"path":"a.png"})).await.unwrap();
        assert!(text(&result).contains("displayed at 4x2"));
        assert!(text(&result).contains("Current model does not support images"));
        assert!(
            result
                .content
                .iter()
                .any(|block| matches!(block, InputContent::Image(_)))
        );
        model.input_limits =
            Some(serde_json::from_value(json!({"images":{"resize":{"maxWidth":0}}})).unwrap());
        let options = ReadToolOptions::for_model(&model);
        let read = create_read_tool_with_options(dir.context(), options.clone());
        let result = invoke(&read, json!({"path":"a.png"})).await.unwrap();
        assert!(text(&result).contains("Image omitted: could not be resized"));
        assert!(
            result
                .content
                .iter()
                .all(|block| matches!(block, InputContent::Text(_)))
        );
        assert_eq!(
            serde_json::to_value(&result.structured_content).unwrap(),
            json!(text(&result))
        );
        let read = create_read_tool_with_options(
            dir.context(),
            ReadToolOptions {
                auto_resize: false,
                ..options
            },
        );
        let result = invoke(&read, json!({"path":"a.bmp"})).await.unwrap();
        assert!(result.content.iter().any(
            |block| matches!(block, InputContent::Image(image) if image.mime_type=="image/png")
        ));
    });
}

#[test]
fn local_resource_discovery_honors_precedence_ignores_collisions_and_frontmatter() {
    let dir = TempDir::new();
    dir.write("global/AGENTS.md", "\u{feff}global");
    dir.write("project/AGENTS.md", "project");
    dir.write("project/sub/AGENTS.override.md", "override");
    dir.write("project/sub/AGENTS.md", "shadowed");
    dir.write(
        "global/skills/a/SKILL.md",
        "---\nname: same\ndescription: global\n---\nGlobal instructions",
    );
    dir.write(
        "project/sub/.pi/skills/a/SKILL.md",
        "---\nname: same\ndescription: project collision\n---\nProject instructions",
    );
    dir.write("project/sub/.pi/skills/.ignore", "ignored/\n");
    dir.write(
        "project/sub/.pi/skills/ignored/SKILL.md",
        "---\ndescription: ignored\n---\nIgnored",
    );
    dir.write("project/sub/.pi/skills/b/SKILL.md","---\nname: bad--name\ndescription: |\n  multiline description\n  next line\ndisable-model-invocation: true\n---\nBody");
    dir.write(
        "global/prompts/run.md",
        "---\ndescription: run test\nargument-hint: <command>\n---\nRun $1 ${2:-default}",
    );
    dir.write("project/sub/.pi/prompts/run.md", "Project duplicate");
    let loaded = load_resources(&ResourceLoaderOptions {
        cwd: dir.path().join("project/sub"),
        agent_dir: dir.path().join("global"),
        skill_paths: Vec::new(),
        prompt_paths: Vec::new(),
        include_defaults: true,
    });
    let contexts: Vec<_> = loaded
        .context_files
        .iter()
        .map(|file| file.content.as_str())
        .collect();
    assert!(contexts.ends_with(&["project", "override"]));
    assert_eq!(contexts[0], "global");
    let skills = loaded.resources.skills.as_ref().unwrap();
    assert_eq!(skills.len(), 2);
    assert_eq!(skills[0].description, "global");
    assert_eq!(skills[1].disable_model_invocation, Some(true));
    assert!(
        loaded
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.winner_path.is_some())
    );
    assert!(
        loaded
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.message.contains("consecutive hyphens"))
    );
    let templates = loaded.resources.prompt_templates.as_ref().unwrap();
    assert_eq!(templates.len(), 1);
    assert_eq!(templates[0].argument_hint.as_deref(), Some("<command>"));
    assert_eq!(
        templates[0].file_path,
        dir.path().join("global/prompts/run.md").to_string_lossy()
    );
    let explicit = load_resources(&ResourceLoaderOptions {
        cwd: dir.path().join("project/sub/../sub"),
        agent_dir: dir.path().join("global"),
        skill_paths: Vec::new(),
        prompt_paths: vec![PathBuf::from(
            url::Url::from_file_path(dir.path().join("global/prompts/run.md"))
                .unwrap()
                .as_str(),
        )],
        include_defaults: false,
    });
    assert_eq!(
        explicit.resources.prompt_templates.as_ref().unwrap(),
        templates
    );
    assert_eq!(
        expand_prompt_template("/run 'a b'", templates),
        "Run a b default"
    );
    assert_eq!(
        parse_frontmatter("\u{feff}---\r\nname: x\r\n---\r\n body \r\n").unwrap(),
        (json!({"name":"x"}), "body".into())
    );
}

#[test]
fn local_settings_merge_preserves_nested_fields_arrays_and_error_locations() {
    let dir = TempDir::new();
    dir.write("global.json","{\"retry\":{\"baseDelayMs\":40,\"provider\":{\"maxRetryDelayMs\":5}},\"models\":[\"global\"]}");
    dir.write(
        "project.json",
        "{\"retry\":{\"maxRetries\":1},\"models\":[\"project\"]}",
    );
    let settings = load_settings(
        &dir.path().join("global.json"),
        Some(&dir.path().join("project.json")),
        &json!({"retry":{"provider":{"maxRetryDelayMs":9}}}),
    );
    assert!(settings.diagnostics.is_empty());
    assert_eq!(settings.values["retry"]["baseDelayMs"], 40);
    assert_eq!(settings.values["retry"]["maxRetries"], 1);
    assert_eq!(settings.values["retry"]["maxAgentDelayMs"], 60000);
    assert_eq!(settings.values["retry"]["provider"]["maxRetryDelayMs"], 9);
    assert_eq!(settings.values["models"], json!(["project"]));
    dir.write("project.json", "[]");
    let invalid = load_settings(
        &dir.path().join("global.json"),
        Some(&dir.path().join("project.json")),
        &json!({}),
    );
    assert_eq!(invalid.diagnostics[0].path, dir.path().join("project.json"));
}

#[cfg(unix)]
#[test]
fn explicit_shell_identity_overrides_stale_pi_values_and_keeps_host_environment() {
    let dir = TempDir::new();
    let identity = ShellIdentity {
        session_id: "session".into(),
        session_file: "sqlite:session".into(),
        provider: "openai".into(),
        model: "gpt-test".into(),
        reasoning_level: "high".into(),
    };
    let context = with_shell_identity(dir.context().env, identity);
    runtime().block_on(async {
        let output=context.env.exec("bash","printf '%s|%s|%s|%s|%s|%s' \"$PI_SESSION_ID\" \"$PI_SESSION_FILE\" \"$PI_PROVIDER\" \"$PI_MODEL\" \"$PI_REASONING_LEVEL\" \"$HOST_VALUE\"",ProcessOptions {env:[("PI_MODEL".into(),"stale".into()),("HOST_VALUE".into(),"kept".into())].into_iter().collect(),..Default::default()}).await.unwrap();
        assert_eq!(output.stdout,"session|sqlite:session|openai|gpt-test|high|kept");
    });
}

#[test]
fn nested_worktree_context_shadows_the_main_repository_copy() {
    let dir = TempDir::new();
    dir.write("main/AGENTS.md", "main duplicate");
    dir.write("main/.git/worktrees/feature/commondir", "../..\n");
    dir.write(
        "main/worktrees/feature/.git",
        &format!(
            "gitdir: {}\n",
            dir.path().join("main/.git/worktrees/feature").display()
        ),
    );
    dir.write("main/worktrees/feature/AGENTS.md", "feature");
    dir.write("global/AGENTS.md", "global");
    let files = load_project_context_files(
        &dir.path().join("main/worktrees/feature"),
        &dir.path().join("global"),
    );
    assert!(!files.iter().any(|file| file.content == "main duplicate"));
    assert!(files.iter().any(|file| file.content == "feature"));
    assert_eq!(files[0].content, "global");
}

#[cfg(unix)]
#[test]
fn shell_customizations_are_explicit_and_image_resize_can_be_disabled() {
    let dir = TempDir::new();
    let image = image::DynamicImage::new_rgb8(2100, 10);
    image
        .save_with_format(dir.path().join("large.png"), image::ImageFormat::Png)
        .unwrap();
    runtime().block_on(async {
        let bash = pi_coding_runtime::tools::create_bash_tool_with_options(
            dir.context(),
            pi_coding_runtime::tools::ShellToolOptions {
                command_prefix: Some("export PREFIX_VALUE=prefix".into()),
                shell_path: Some(PathBuf::from("/bin/bash")),
                environment: [("HOST_VALUE".into(), "host".into())].into_iter().collect(),
            },
        );
        assert_eq!(
            text(
                &invoke(
                    &bash,
                    json!({"command":"printf '%s|%s' \"$PREFIX_VALUE\" \"$HOST_VALUE\""})
                )
                .await
                .unwrap()
            ),
            "prefix|host"
        );
        let read = pi_coding_runtime::tools::create_read_tool_with_options(
            dir.context(),
            pi_coding_runtime::tools::ReadToolOptions {
                auto_resize: false,
                ..Default::default()
            },
        );
        let result = invoke(&read, json!({"path":"large.png"})).await.unwrap();
        assert!(!text(&result).contains("displayed at"));
    });
}

#[test]
fn native_search_honors_parent_ignores_nested_repositories_and_glob_syntax() {
    let dir = TempDir::new();
    dir.write("repo/.git/config", "");
    dir.write("repo/.gitignore", "*.tmp\nblocked/\n!blocked/keep.txt\n");
    dir.write("repo/src/ignored.tmp", "needle");
    dir.write("repo/src/Visible.RS", "needle");
    dir.write("repo/src/a.txt", "needle");
    dir.write("repo/blocked/keep.txt", "needle");
    dir.write("repo/nested/.git/config", "");
    dir.write("repo/nested/keep.tmp", "needle");
    runtime().block_on(async {
        let context = ExecutionToolContext {
            env: Arc::new(LocalExecutionEnv::new(dir.path().join("repo"))),
        };
        let find = create_find_tool(context.clone());
        let root = invoke(&find, json!({"pattern":"**/*"})).await.unwrap();
        let listing = text(&root);
        assert!(!listing.contains("ignored.tmp"));
        assert!(!listing.contains("blocked/keep.txt"));
        assert!(listing.contains("nested/keep.tmp"));
        let glob = invoke(&find, json!({"pattern":"src/*.{rs,txt}"}))
            .await
            .unwrap();
        assert!(text(&glob).contains("src/Visible.RS"));
        assert!(text(&glob).contains("src/a.txt"));
        let scoped = invoke(&find, json!({"pattern":"*","path":"src"}))
            .await
            .unwrap();
        assert!(!text(&scoped).contains("ignored.tmp"));
        let grep = create_grep_tool(context);
        let matches = invoke(&grep, json!({"pattern":"needle","path":"src"}))
            .await
            .unwrap();
        assert!(!text(&matches).contains("ignored.tmp"));
    });
}

#[test]
fn listing_and_search_use_byte_and_explicit_count_limits_without_read_line_cap() {
    let dir = TempDir::new();
    dir.write(".git/config", "");
    for index in 0..2005 {
        dir.write(&format!("a{index:04}.txt"), "x");
    }
    runtime().block_on(async {
        let context = dir.context();
        let ls = invoke(&create_ls_tool(context.clone()), json!({"limit":3000}))
            .await
            .unwrap();
        assert_eq!(text(&ls).lines().count(), 2006);
        let find = invoke(
            &create_find_tool(context.clone()),
            json!({"pattern":"*.txt","limit":3000}),
        )
        .await
        .unwrap();
        assert_eq!(text(&find).lines().count(), 2005);
        let grep = invoke(
            &create_grep_tool(context),
            json!({"pattern":"x","limit":3000}),
        )
        .await
        .unwrap();
        assert_eq!(text(&grep).lines().count(), 2005);
    });
}

#[test]
fn search_limit_notices_details_omission_exclusion_globs_and_zero_limits_match_release() {
    let dir = TempDir::new();
    dir.write("a.txt", "needle");
    dir.write("b.log", "needle");
    runtime().block_on(async {
        let context = dir.context();
        let ls = create_ls_tool(context.clone());
        let limited = invoke(&ls, json!({"limit":1})).await.unwrap();
        assert_eq!(text(&limited), "a.txt\n\n[1 entries limit reached. Use limit=2 for more]");
        assert_eq!(serde_json::to_value(limited.details).unwrap(), json!({"entryLimitReached":1}));
        assert!(invoke(&ls, json!({"limit":2})).await.unwrap().details.is_none());
        let empty = invoke(&ls, json!({"limit":0})).await.unwrap();
        assert_eq!(text(&empty), "(empty directory)");
        assert!(empty.details.is_none());
        let find = create_find_tool(context.clone());
        let limited = invoke(&find, json!({"pattern":"*.txt","limit":1})).await.unwrap();
        assert_eq!(text(&limited), "a.txt\n\n[1 results limit reached. Use limit=2 for more, or refine pattern]");
        assert_eq!(serde_json::to_value(limited.details).unwrap(), json!({"resultLimitReached":1}));
        let all = invoke(&find, json!({"pattern":"*","limit":0})).await.unwrap();
        assert_eq!(text(&all), "a.txt\nb.log\n\n[0 results limit reached. Use limit=0 for more, or refine pattern]");
        assert_eq!(serde_json::to_value(all.details).unwrap(), json!({"resultLimitReached":0}));
        let grep = create_grep_tool(context);
        let excluded = invoke(&grep, json!({"pattern":"needle","glob":"!*.txt"})).await.unwrap();
        assert_eq!(text(&excluded), "b.log:1: needle");
        assert!(excluded.details.is_none());
        let limited = invoke(&grep, json!({"pattern":"needle","glob":"*.txt","limit":0})).await.unwrap();
        assert_eq!(text(&limited), "a.txt:1: needle\n\n[1 matches limit reached. Use limit=2 for more, or refine pattern]");
        assert_eq!(serde_json::to_value(limited.details).unwrap(), json!({"matchLimitReached":1}));
        dir.write("a.txt", &format!("needle {}", "😀".repeat(260)));
        let long = invoke(&grep, json!({"pattern":"needle","glob":"*.txt"})).await.unwrap();
        assert_eq!(text(&long), format!("a.txt:1: needle {}�... [truncated]\n\n[Some lines truncated to 500 chars. Use read tool to see full lines]", "😀".repeat(246)));
        assert_eq!(serde_json::to_value(long.details).unwrap(), json!({"linesTruncated":true}));
    });
}

struct RetainedOutputEnv {
    local: LocalExecutionEnv,
    retained: Mutex<Option<ProcessData>>,
    initial: Vec<u8>,
    fail: bool,
    catch_progress_panic: bool,
    ready: tokio::sync::Notify,
    release: Option<Arc<tokio::sync::Notify>>,
}
impl ExecutionEnv for RetainedOutputEnv {
    fn absolute_path<'a>(&'a self, path: &'a str) -> pi_ai::BoxFuture<'a, EnvResult<String>> {
        self.local.absolute_path(path)
    }
    fn exists<'a>(&'a self, path: &'a str) -> pi_ai::BoxFuture<'a, EnvResult<bool>> {
        self.local.exists(path)
    }
    fn read_binary_file<'a>(&'a self, path: &'a str) -> pi_ai::BoxFuture<'a, EnvResult<Vec<u8>>> {
        self.local.read_binary_file(path)
    }
    fn read_text_file<'a>(&'a self, path: &'a str) -> pi_ai::BoxFuture<'a, EnvResult<String>> {
        self.local.read_text_file(path)
    }
    fn write_file<'a>(
        &'a self,
        path: &'a str,
        content: &'a [u8],
    ) -> pi_ai::BoxFuture<'a, EnvResult<()>> {
        self.local.write_file(path, content)
    }
    fn list_dir<'a>(&'a self, path: &'a str) -> pi_ai::BoxFuture<'a, EnvResult<Vec<FileInfo>>> {
        self.local.list_dir(path)
    }
    fn file_info<'a>(&'a self, path: &'a str) -> pi_ai::BoxFuture<'a, EnvResult<FileInfo>> {
        self.local.file_info(path)
    }
    fn canonical_path<'a>(&'a self, path: &'a str) -> pi_ai::BoxFuture<'a, EnvResult<String>> {
        self.local.canonical_path(path)
    }
    fn create_temp_file<'a>(
        &'a self,
        prefix: &'a str,
        content: &'a [u8],
    ) -> pi_ai::BoxFuture<'a, EnvResult<String>> {
        self.local.create_temp_file(prefix, content)
    }
    fn exec<'a>(
        &'a self,
        _shell: &'a str,
        _command: &'a str,
        options: ProcessOptions,
    ) -> pi_ai::BoxFuture<'a, EnvResult<ProcessOutput>> {
        Box::pin(async move {
            let callback = options.on_data.expect("shell supplies output callback");
            *self.retained.lock().unwrap() = Some(callback.clone());
            if !self.initial.is_empty() {
                if self.catch_progress_panic {
                    assert!(
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| callback(
                            &self.initial
                        )))
                        .is_err()
                    );
                } else {
                    callback(&self.initial);
                }
            }
            self.ready.notify_one();
            if let Some(release) = &self.release {
                release.notified().await;
            }
            if self.fail {
                Err(EnvError::new("injected exec failure"))
            } else {
                Ok(ProcessOutput {
                    stdout: String::new(),
                    stderr: String::new(),
                    merged: None,
                    exit_code: 0,
                })
            }
        })
    }
}
fn operation_text(output: &ToolOutput) -> String {
    text(&output.clone().into())
}
fn output_path(output: &ToolOutput) -> Option<String> {
    serde_json::to_value(&output.details).unwrap()["fullOutputPath"]
        .as_str()
        .map(str::to_owned)
}

#[test]
fn shell_retained_callbacks_cannot_publish_or_mutate_spills_after_success_or_error() {
    let dir = TempDir::new();
    runtime().block_on(async {
        for fail in [false, true] {
            for initial in [Vec::new(), vec![b'x'; 51_201]] {
                let env = Arc::new(RetainedOutputEnv {
                    local: LocalExecutionEnv::new(dir.path()),
                    retained: Mutex::new(None),
                    initial: initial.clone(),
                    fail,
                    catch_progress_panic: false,
                    ready: tokio::sync::Notify::new(),
                    release: None,
                });
                let updates = Arc::new(Mutex::new(Vec::<ToolOutput>::new()));
                let capture = updates.clone();
                let progress: ToolOutputUpdate =
                    Arc::new(move |output| capture.lock().unwrap().push(output));
                let result = coding_tool("bash")
                    .execute(
                        serde_json::from_value(json!({"command":"injected"})).unwrap(),
                        &CancellationToken::new(),
                        &progress,
                        &ExecutionToolContext { env: env.clone() },
                    )
                    .await;
                assert_eq!(result.is_err(), fail);
                let before = updates.lock().unwrap().clone();
                let path = before.iter().find_map(output_path);
                if let Some(path) = &path {
                    assert_eq!(std::fs::read(path).unwrap(), initial);
                }
                let callback = env.retained.lock().unwrap().clone().unwrap();
                callback(&vec![b'L'; 60_000]);
                let after = updates.lock().unwrap().clone();
                let spill_after = path.as_ref().map(|path| std::fs::read(path).unwrap());
                // Clean up either implementation's diagnostic output before asserting.
                for path in before
                    .iter()
                    .chain(&after)
                    .filter_map(output_path)
                    .collect::<std::collections::BTreeSet<_>>()
                {
                    std::fs::remove_file(path).unwrap();
                }
                assert!(
                    after == before,
                    "late progress after fail={fail}, initial={}",
                    initial.len()
                );
                if let Some(bytes) = spill_after {
                    assert_eq!(bytes, initial, "late spill mutation after fail={fail}");
                }
            }
        }
    });
}

#[test]
fn shell_settlement_waits_for_racing_progress_before_closing_output() {
    let dir = TempDir::new();
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            for fail in [false, true] {
                let release_exec = Arc::new(tokio::sync::Notify::new());
                let env = Arc::new(RetainedOutputEnv {
                    local: LocalExecutionEnv::new(dir.path()),
                    retained: Mutex::new(None),
                    initial: Vec::new(),
                    fail,
                    catch_progress_panic: false,
                    ready: tokio::sync::Notify::new(),
                    release: Some(release_exec.clone()),
                });
                let entered = Arc::new(tokio::sync::Notify::new());
                let progress_entered = entered.clone();
                let (release_progress, progress_release) = std::sync::mpsc::sync_channel(1);
                let progress_release = Mutex::new(progress_release);
                let first = std::sync::atomic::AtomicBool::new(true);
                let updates = Arc::new(Mutex::new(Vec::new()));
                let capture = updates.clone();
                let progress: ToolOutputUpdate = Arc::new(move |output| {
                    let body = operation_text(&output);
                    let label = if body == "race"
                        && first.swap(false, std::sync::atomic::Ordering::SeqCst)
                    {
                        progress_entered.notify_one();
                        progress_release
                            .lock()
                            .unwrap()
                            .recv_timeout(Duration::from_secs(2))
                            .unwrap();
                        "stream"
                    } else if body.is_empty() {
                        "start"
                    } else {
                        "final"
                    };
                    capture.lock().unwrap().push(label);
                });
                let context = ExecutionToolContext { env: env.clone() };
                let mut task = tokio::spawn(async move {
                    coding_tool("bash")
                        .execute(
                            serde_json::from_value(json!({"command":"injected"})).unwrap(),
                            &CancellationToken::new(),
                            &progress,
                            &context,
                        )
                        .await
                });
                env.ready.notified().await;
                let callback = env.retained.lock().unwrap().clone().unwrap();
                let worker_callback = callback.clone();
                let worker = std::thread::spawn(move || worker_callback(b"race"));
                entered.notified().await;
                release_exec.notify_one();
                let early = tokio::time::timeout(Duration::from_millis(100), &mut task).await;
                let completed_early = early.is_ok();
                release_progress.send(()).unwrap();
                worker.join().unwrap();
                let result = match early {
                    Ok(result) => result.unwrap(),
                    Err(_) => task.await.unwrap(),
                };
                assert_eq!(result.is_err(), fail);
                assert!(
                    !completed_early,
                    "settled before racing publication finished, fail={fail}"
                );
                assert_eq!(
                    *updates.lock().unwrap(),
                    if fail {
                        vec!["start", "stream"]
                    } else {
                        vec!["start", "stream", "final"]
                    }
                );
                let before = updates.lock().unwrap().clone();
                callback(b"LATE");
                assert_eq!(*updates.lock().unwrap(), before);
            }
        });
}

#[cfg(unix)]
#[test]
fn shell_decoder_finish_crosses_display_limit_and_preserves_raw_spill() {
    let dir = TempDir::new();
    runtime().block_on(async {
        let result = invoke(
            &create_bash_tool(dir.context()),
            json!({"command":"head -c 51199 /dev/zero | tr '\\0' x; printf '\\342'"}),
        )
        .await
        .unwrap();
        let details = serde_json::to_value(&result.details).unwrap();
        assert_eq!(details["truncation"]["truncated"], true);
        assert_eq!(details["truncation"]["totalBytes"], 51_202);
        let path = details["fullOutputPath"].as_str().unwrap();
        let mut expected_raw = vec![b'x'; 51_199];
        expected_raw.push(0xe2);
        assert_eq!(std::fs::read(path).unwrap(), expected_raw);
        let structured = serde_json::to_value(&result.structured_content).unwrap();
        assert_eq!(structured["output"], format!("{}�", "x".repeat(51_199)));
        assert_eq!(structured["truncated"], false);
        assert_eq!(structured["exit_code"], 0);
        assert!(structured.get("full_output_path").is_none());
        assert!(text(&result).contains('�'));
        std::fs::remove_file(path).unwrap();
    });
}

#[test]
fn grep_sanitizes_cr_display_without_changing_match_lines() {
    let dir = TempDir::new();
    dir.write("cr.txt", "hit\rnext\n");
    dir.write("crlf.txt", "before\r\nhit\r\nafter\r\n");
    dir.write("lf.txt", "before\nhit\nafter\n");
    runtime().block_on(async {
        let grep = create_grep_tool(dir.context());
        for (path, context, expected) in [
            ("cr.txt", 0, "cr.txt:1: hitnext"),
            ("cr.txt", 1, "cr.txt:1: hit\ncr.txt-2- next"),
            ("crlf.txt", 0, "crlf.txt:2: hit"),
            (
                "crlf.txt",
                1,
                "crlf.txt-1- before\ncrlf.txt:2: hit\ncrlf.txt-3- after",
            ),
            ("lf.txt", 0, "lf.txt:2: hit"),
            (
                "lf.txt",
                1,
                "lf.txt-1- before\nlf.txt:2: hit\nlf.txt-3- after",
            ),
        ] {
            let result = invoke(
                &grep,
                json!({"pattern":"hit","path":path,"context":context}),
            )
            .await
            .unwrap();
            assert_eq!(text(&result), expected);
            assert!(result.details.is_none());
        }
        assert_eq!(
            text(
                &invoke(&grep, json!({"pattern":"hitnext","path":"cr.txt"}))
                    .await
                    .unwrap()
            ),
            "No matches found"
        );
    });
}

#[test]
fn shell_progress_can_reenter_retained_data_callback() {
    let dir = TempDir::new();
    let path = dir.path().to_owned();
    let (complete, completed) = std::sync::mpsc::sync_channel(1);
    let worker = std::thread::spawn(move || {
        let env = Arc::new(RetainedOutputEnv {
            local: LocalExecutionEnv::new(path),
            retained: Mutex::new(None),
            initial: b"FIRST".to_vec(),
            fail: false,
            catch_progress_panic: false,
            ready: tokio::sync::Notify::new(),
            release: None,
        });
        let weak_env = Arc::downgrade(&env);
        let reentered = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let did_reenter = reentered.clone();
        let updates = Arc::new(Mutex::new(Vec::new()));
        let capture = updates.clone();
        let progress: ToolOutputUpdate = Arc::new(move |output| {
            let body = operation_text(&output);
            if body == "FIRST" && !did_reenter.swap(true, std::sync::atomic::Ordering::SeqCst) {
                let callback = weak_env
                    .upgrade()
                    .unwrap()
                    .retained
                    .lock()
                    .unwrap()
                    .clone()
                    .unwrap();
                callback(b"NESTED");
            }
            capture.lock().unwrap().push(body);
        });
        let result = runtime().block_on(coding_tool("bash").execute(
            serde_json::from_value(json!({"command":"injected"})).unwrap(),
            &CancellationToken::new(),
            &progress,
            &ExecutionToolContext { env: env.clone() },
        ));
        let before = updates.lock().unwrap().clone();
        let callback = env.retained.lock().unwrap().clone().unwrap();
        callback(b"LATE");
        let unchanged = *updates.lock().unwrap() == before;
        complete
            .send((
                result,
                reentered.load(std::sync::atomic::Ordering::SeqCst),
                unchanged,
            ))
            .unwrap();
    });
    let (result, reentered, unchanged) = completed
        .recv_timeout(Duration::from_secs(3))
        .expect("reentrant callback must not deadlock");
    worker.join().unwrap();
    assert!(reentered);
    assert_eq!(operation_text(&result.unwrap()), "FIRSTNESTED");
    assert!(unchanged);
}

#[test]
fn shell_caught_progress_panic_releases_admission_without_poisoning_retirement() {
    let dir = TempDir::new();
    runtime().block_on(async {
        for fail in [false, true] {
            let env = Arc::new(RetainedOutputEnv {
                local: LocalExecutionEnv::new(dir.path()),
                retained: Mutex::new(None),
                initial: b"PANIC".to_vec(),
                fail,
                catch_progress_panic: true,
                ready: tokio::sync::Notify::new(),
                release: None,
            });
            let first = std::sync::atomic::AtomicBool::new(true);
            let updates = Arc::new(Mutex::new(Vec::new()));
            let capture = updates.clone();
            let progress: ToolOutputUpdate = Arc::new(move |output| {
                if operation_text(&output) == "PANIC"
                    && first.swap(false, std::sync::atomic::Ordering::SeqCst)
                {
                    panic!("injected foreign progress panic");
                }
                capture.lock().unwrap().push(operation_text(&output));
            });
            let context = ExecutionToolContext { env: env.clone() };
            let result = tokio::spawn(async move {
                coding_tool("bash")
                    .execute(
                        serde_json::from_value(json!({"command":"injected"})).unwrap(),
                        &CancellationToken::new(),
                        &progress,
                        &context,
                    )
                    .await
            })
            .await
            .expect("caught foreign panic must not poison output state");
            assert_eq!(result.is_err(), fail);
            let before = updates.lock().unwrap().clone();
            let callback = env.retained.lock().unwrap().clone().unwrap();
            assert!(
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| callback(b"LATE")))
                    .is_ok()
            );
            assert_eq!(*updates.lock().unwrap(), before);
        }
    });
}

#[test]
fn dropping_shell_future_retires_retained_data_and_existing_spill() {
    let dir = TempDir::new();
    runtime().block_on(async {
        for initial in [Vec::new(), vec![b'x'; 51_201]] {
            let env = Arc::new(RetainedOutputEnv {
                local: LocalExecutionEnv::new(dir.path()),
                retained: Mutex::new(None),
                initial: initial.clone(),
                fail: false,
                catch_progress_panic: false,
                ready: tokio::sync::Notify::new(),
                release: Some(Arc::new(tokio::sync::Notify::new())),
            });
            let updates = Arc::new(Mutex::new(Vec::<ToolOutput>::new()));
            let capture = updates.clone();
            let progress: ToolOutputUpdate =
                Arc::new(move |output| capture.lock().unwrap().push(output));
            let context = ExecutionToolContext { env: env.clone() };
            let task = tokio::spawn(async move {
                coding_tool("bash")
                    .execute(
                        serde_json::from_value(json!({"command":"injected"})).unwrap(),
                        &CancellationToken::new(),
                        &progress,
                        &context,
                    )
                    .await
            });
            env.ready.notified().await;
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
            let before = updates.lock().unwrap().clone();
            let callback = env.retained.lock().unwrap().clone().unwrap();
            callback(&vec![b'L'; 60_000]);
            let after = updates.lock().unwrap().clone();
            let path = before.iter().find_map(output_path);
            let spill = path.as_ref().map(|path| std::fs::read(path).unwrap());
            for path in before
                .iter()
                .chain(&after)
                .filter_map(output_path)
                .collect::<std::collections::BTreeSet<_>>()
            {
                std::fs::remove_file(path).unwrap();
            }
            assert!(before == after, "future drop must retire later progress");
            if let Some(spill) = spill {
                assert!(spill == initial, "future drop must prevent spill mutation");
            }
        }
    });
}
