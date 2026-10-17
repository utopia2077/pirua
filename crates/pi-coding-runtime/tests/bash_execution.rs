mod session_support;
use pi_agent::{AgentError, BoxFuture};
use pi_coding_runtime::*;
use pi_coding_session::{EntryContent, MemoryStore, SessionManager};
use serde_json::{Value, json};
use session_support::*;
use std::sync::{Arc, Mutex};

struct ChunkEnv {
    base: LocalExecutionEnv,
    chunks: Vec<Vec<u8>>,
    wait: bool,
    entered: Arc<tokio::sync::Notify>,
    saved: Mutex<Option<ProcessData>>,
    cancelled: Arc<tokio::sync::Notify>,
    cleanup_release: Option<Arc<tokio::sync::Notify>>,
}
impl ExecutionEnv for ChunkEnv {
    fn absolute_path<'a>(&'a self, path: &'a str) -> BoxFuture<'a, EnvResult<String>> {
        self.base.absolute_path(path)
    }
    fn exists<'a>(&'a self, path: &'a str) -> BoxFuture<'a, EnvResult<bool>> {
        self.base.exists(path)
    }
    fn read_binary_file<'a>(&'a self, path: &'a str) -> BoxFuture<'a, EnvResult<Vec<u8>>> {
        self.base.read_binary_file(path)
    }
    fn read_text_file<'a>(&'a self, path: &'a str) -> BoxFuture<'a, EnvResult<String>> {
        self.base.read_text_file(path)
    }
    fn write_file<'a>(&'a self, path: &'a str, content: &'a [u8]) -> BoxFuture<'a, EnvResult<()>> {
        self.base.write_file(path, content)
    }
    fn list_dir<'a>(&'a self, path: &'a str) -> BoxFuture<'a, EnvResult<Vec<FileInfo>>> {
        self.base.list_dir(path)
    }
    fn file_info<'a>(&'a self, path: &'a str) -> BoxFuture<'a, EnvResult<FileInfo>> {
        self.base.file_info(path)
    }
    fn canonical_path<'a>(&'a self, path: &'a str) -> BoxFuture<'a, EnvResult<String>> {
        self.base.canonical_path(path)
    }
    fn create_temp_file<'a>(
        &'a self,
        prefix: &'a str,
        content: &'a [u8],
    ) -> BoxFuture<'a, EnvResult<String>> {
        self.base.create_temp_file(prefix, content)
    }
    fn exec<'a>(
        &'a self,
        _: &'a str,
        _: &'a str,
        options: ProcessOptions,
    ) -> BoxFuture<'a, EnvResult<ProcessOutput>> {
        Box::pin(async move {
            let callback = options.on_data.unwrap();
            *self.saved.lock().unwrap() = Some(callback.clone());
            for bytes in &self.chunks {
                callback(bytes);
            }
            self.entered.notify_one();
            if self.wait {
                options.abort_signal.unwrap().cancelled().await;
                self.cancelled.notify_one();
                if let Some(release) = &self.cleanup_release {
                    release.notified().await;
                }
                return Err(EnvError::new("cancelled"));
            }
            Ok(ProcessOutput {
                stdout: String::new(),
                stderr: String::new(),
                merged: None,
                exit_code: 7,
            })
        })
    }
}
fn chunk_env(chunks: Vec<Vec<u8>>, wait: bool) -> Arc<ChunkEnv> {
    Arc::new(ChunkEnv {
        base: LocalExecutionEnv::new("/tmp"),
        chunks,
        wait,
        entered: Arc::new(tokio::sync::Notify::new()),
        saved: Mutex::new(None),
        cancelled: Arc::new(tokio::sync::Notify::new()),
        cleanup_release: None,
    })
}
#[test]
fn exact_release_stream_sanitizer_and_decoder_observations() {
    runtime().block_on(async {
        let cases: Value = serde_json::from_str(include_str!(
            "../../../conformance/coding-session-runtime-v1.4.0/cases.json"
        ))
        .unwrap();
        let trace: Value = serde_json::from_str(include_str!(
            "../../../conformance/coding-session-runtime-v1.4.0/trace.json"
        ))
        .unwrap();
        for case in cases
            .as_array()
            .unwrap()
            .iter()
            .filter(|case| case["kind"] == "bash-sanitize")
        {
            let (stream, _) = scripted(vec![]);
            let session = CodingSessionBuilder::new(
                SessionManager::create(Arc::new(MemoryStore::new()), "/tmp").unwrap(),
                model(),
                stream,
            )
            .with_tools(vec![])
            .build()
            .await
            .unwrap();
            let chunks = Arc::new(Mutex::new(Vec::new()));
            let capture = chunks.clone();
            let environment = chunk_env(
                serde_json::from_value(case["chunks"].clone()).unwrap(),
                false,
            );
            let result = session
                .execute_bash(
                    "fixture",
                    Some(Arc::new(move |chunk| {
                        capture.lock().unwrap().push(chunk);
                        Box::pin(async { Ok(()) })
                    })),
                    ExecuteBashOptions {
                        environment: Some(environment),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            let mut actual = serde_json::to_value(&result).unwrap();
            actual.as_object_mut().unwrap().remove("fullOutputPath");
            actual["chunks"] = json!(*chunks.lock().unwrap());
            assert_eq!(
                actual,
                trace["observations"][case["id"].as_str().unwrap()],
                "{}",
                case["id"]
            );
            assert_eq!(session.projection().messages[0]["output"], result.output);
        }
    });
}
#[test]
fn local_bash_records_original_command_exit_status_exclusion_and_spill() {
    runtime().block_on(async {
        let (stream, _) = scripted(vec![]);
        let session = CodingSessionBuilder::new(
            SessionManager::create(Arc::new(MemoryStore::new()), "/tmp").unwrap(),
            model(),
            stream,
        )
        .with_tools(vec![])
        .build()
        .await
        .unwrap();
        let command = "printf '\\033[31mhello\\033[0m\\r\\n'; exit 7";
        let result = session
            .execute_bash(command, None, ExecuteBashOptions::default())
            .await
            .unwrap();
        assert_eq!(result.output, "hello\n");
        assert_eq!(result.exit_code, Some(7));
        assert!(!result.cancelled);
        let message = &session.projection().messages[0];
        assert_eq!(message["role"], "bashExecution");
        assert_eq!(message["command"], command);
        assert!(
            serde_json::to_value(convert_to_llm(session.messages().unwrap()).unwrap()).unwrap()[0]
                ["content"][0]["text"]
                .as_str()
                .unwrap()
                .contains("hello")
        );
        session
            .execute_bash(
                "printf secret",
                None,
                ExecuteBashOptions {
                    exclude_from_context: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(session.messages().unwrap().len(), 2);
        assert_eq!(
            convert_to_llm(session.messages().unwrap()).unwrap().len(),
            1
        );
        let result = session
            .execute_bash("printf '%060000d' 1", None, ExecuteBashOptions::default())
            .await
            .unwrap();
        assert!(result.truncated);
        let path = result.full_output_path.unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 60000);
        std::fs::remove_file(path).unwrap();
        assert!(!session.is_bash_running());
        session.wait_for_bash().await;
    });
}
#[test]
fn shell_records_wait_until_tool_batch_settles() {
    runtime().block_on(async {
        let (stream,requests)=scripted(vec![json!({"stopReason":"toolUse","content":[{"type":"toolCall","id":"call","name":"record","arguments":{}}]}),json!({})]);
        let session=CodingSessionBuilder::new(SessionManager::create(Arc::new(MemoryStore::new()),"/tmp").unwrap(),model(),stream).with_tools(vec![]).with_system_prompt("defer").build().await.unwrap();let weak=session.downgrade();
        session.register_tool(RegisteredTool::direct(tool("record",Arc::new(move|_|{let session=weak.upgrade().unwrap();Box::pin(async move {session.record_bash_result("host",BashResult{output:"recorded".into(),exit_code:Some(0),cancelled:false,truncated:false,full_output_path:None},false).await.map_err(|e|AgentError::Callback(e.to_string()))?;assert!(session.has_pending_bash_messages());assert!(!session.projection().messages.iter().any(|message|message["role"]=="bashExecution"));Ok(text_result("ok"))})})))).unwrap();session.prompt("go",PromptOptions::default()).await.unwrap();
        assert!(!serde_json::to_value(requests.lock().unwrap()[1].context.messages()).unwrap().as_array().unwrap().iter().any(|message|message["role"]=="user"&&message["content"].as_str().is_some_and(|text|text.contains("recorded"))));
        let projection=session.projection();let shell=projection.messages.iter().position(|message|message["role"]=="bashExecution").unwrap();let result=projection.messages.iter().position(|message|message["role"]=="toolResult").unwrap();assert!(shell>result);assert!(!session.has_pending_bash_messages());assert!(session.snapshot().entries.iter().any(|entry|matches!(&entry.content,EntryContent::Message{message}if message["role"]=="bashExecution")));
    });
}
#[test]
fn shell_cancel_drop_and_dispose_retire_output_and_settle_ownership() {
    runtime().block_on(async {
        let (stream, _) = scripted(vec![]);
        let session = CodingSessionBuilder::new(
            SessionManager::create(Arc::new(MemoryStore::new()), "/tmp").unwrap(),
            model(),
            stream,
        )
        .with_tools(vec![])
        .build()
        .await
        .unwrap();
        let environment = chunk_env(vec![b"before".to_vec()], true);
        let run = tokio::spawn({
            let session = session.clone();
            let environment = environment.clone();
            async move {
                session
                    .execute_bash(
                        "wait",
                        None,
                        ExecuteBashOptions {
                            environment: Some(environment),
                            ..Default::default()
                        },
                    )
                    .await
            }
        });
        environment.entered.notified().await;
        assert!(session.is_bash_running());
        assert!(session.is_idle());
        session.abort_bash();
        let result = run.await.unwrap().unwrap();
        assert!(result.cancelled);
        assert_eq!(result.exit_code, None);
        assert_eq!(result.output, "before");
        session.wait_for_bash().await;
        let before = session.snapshot();
        let environment = chunk_env(vec![b"abandoned".to_vec()], true);
        let run = tokio::spawn({
            let session = session.clone();
            let environment = environment.clone();
            async move {
                session
                    .execute_bash(
                        "drop",
                        None,
                        ExecuteBashOptions {
                            environment: Some(environment),
                            ..Default::default()
                        },
                    )
                    .await
            }
        });
        environment.entered.notified().await;
        run.abort();
        assert!(run.await.unwrap_err().is_cancelled());
        session.wait_for_bash().await;
        environment.saved.lock().unwrap().as_ref().unwrap()(b"late output");
        assert_eq!(session.snapshot(), before);
        let environment = chunk_env(vec![], true);
        let run = tokio::spawn({
            let session = session.clone();
            let environment = environment.clone();
            async move {
                session
                    .execute_bash(
                        "dispose",
                        None,
                        ExecuteBashOptions {
                            environment: Some(environment),
                            ..Default::default()
                        },
                    )
                    .await
            }
        });
        environment.entered.notified().await;
        session.dispose().await.unwrap();
        assert!(session.is_disposed());
        assert!(!session.is_bash_running());
        let _ = run.await.unwrap();
        assert!(
            session
                .execute_bash("late", None, Default::default())
                .await
                .is_err()
        );
    });
}

struct HoldStart {
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}
impl SessionHooks for HoldStart {
    fn before_agent_start(
        &self,
        _: BeforeAgentStart,
        _: pi_agent::CancellationToken,
    ) -> BoxFuture<'_, Result<BeforeAgentStartResult, AgentError>> {
        Box::pin(async move {
            self.entered.notify_one();
            self.release.notified().await;
            Ok(Default::default())
        })
    }
}
#[test]
fn records_held_during_startup_precede_the_starting_message() {
    runtime().block_on(async {
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let (stream, requests) = scripted(vec![json!({})]);
        let session = CodingSessionBuilder::new(
            SessionManager::create(Arc::new(MemoryStore::new()), "/tmp").unwrap(),
            model(),
            stream,
        )
        .with_tools(vec![])
        .with_system_prompt("startup")
        .with_hooks(Arc::new(HoldStart {
            entered: entered.clone(),
            release: release.clone(),
        }))
        .build()
        .await
        .unwrap();
        let run = tokio::spawn({
            let session = session.clone();
            async move { session.prompt("starting", PromptOptions::default()).await }
        });
        entered.notified().await;
        session
            .record_bash_result(
                "host",
                BashResult {
                    output: "shell before".into(),
                    exit_code: Some(0),
                    cancelled: false,
                    truncated: false,
                    full_output_path: None,
                },
                false,
            )
            .await
            .unwrap();
        session
            .send_custom_message("held", json!("custom before"), false, None, None, false)
            .await
            .unwrap();
        assert!(session.has_pending_bash_messages());
        release.notify_one();
        run.await.unwrap().unwrap();
        let request = serde_json::to_value(requests.lock().unwrap()[0].context.messages()).unwrap();
        let messages = request.as_array().unwrap();
        let texts = messages
            .iter()
            .map(|message| pi_coding_runtime::messages::content_text(&message["content"], ""))
            .collect::<Vec<_>>();
        let shell = texts
            .iter()
            .position(|text| text.contains("shell before"))
            .unwrap();
        let custom = texts
            .iter()
            .position(|text| text == "custom before")
            .unwrap();
        let starting = texts.iter().position(|text| text == "starting").unwrap();
        assert!(shell < custom && custom < starting);
        assert!(!session.has_pending_bash_messages());
    });
}

#[test]
fn disposal_closes_admission_and_survives_dropped_and_concurrent_waiters() {
    runtime().block_on(async {
        let release = Arc::new(tokio::sync::Notify::new());
        let environment = Arc::new(ChunkEnv {
            base: LocalExecutionEnv::new("/tmp"),
            chunks: vec![],
            wait: true,
            entered: Arc::new(tokio::sync::Notify::new()),
            saved: Mutex::new(None),
            cancelled: Arc::new(tokio::sync::Notify::new()),
            cleanup_release: Some(release.clone()),
        });
        let (stream, requests) = scripted(vec![json!({})]);
        let session = CodingSessionBuilder::new(
            SessionManager::create(Arc::new(MemoryStore::new()), "/tmp").unwrap(),
            model(),
            stream,
        )
        .with_tools(vec![])
        .with_system_prompt("dispose")
        .build()
        .await
        .unwrap();
        let shell = tokio::spawn({
            let session = session.clone();
            let environment = environment.clone();
            async move {
                session
                    .execute_bash(
                        "hold",
                        None,
                        ExecuteBashOptions {
                            environment: Some(environment),
                            ..Default::default()
                        },
                    )
                    .await
            }
        });
        environment.entered.notified().await;
        let disposing = tokio::spawn({
            let session = session.clone();
            async move { session.dispose().await }
        });
        environment.cancelled.notified().await;
        assert!(matches!(
            session.prompt("during dispose", Default::default()).await,
            Err(SessionError::Disposed)
        ));
        assert!(matches!(
            session
                .execute_bash("during dispose", None, Default::default())
                .await,
            Err(SessionError::Disposed)
        ));
        assert!(requests.lock().unwrap().is_empty());
        disposing.abort();
        assert!(disposing.await.unwrap_err().is_cancelled());
        assert!(matches!(
            session
                .prompt("after dropped dispose", Default::default())
                .await,
            Err(SessionError::Disposed)
        ));
        let finish = tokio::spawn({
            let session = session.clone();
            async move { session.dispose().await }
        });
        let concurrent = tokio::spawn({
            let session = session.clone();
            async move { session.dispose().await }
        });
        tokio::task::yield_now().await;
        assert!(!finish.is_finished() && !concurrent.is_finished());
        release.notify_one();
        assert!(shell.await.unwrap().unwrap().cancelled);
        finish.await.unwrap().unwrap();
        concurrent.await.unwrap().unwrap();
        session.dispose().await.unwrap();
        assert!(session.is_disposed() && !session.is_streaming() && !session.is_bash_running());
        assert!(requests.lock().unwrap().is_empty());
    });
}
