//! Deterministic transports and interactions exercise the production factories.
use pi_acp_server::{
    Application, Options, composition::compose_registry, credentials::Credentials,
};
use pi_ai::*;
use pi_ai_transport::*;
use serde_json::json;
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};
struct Spawner(tokio::runtime::Handle);
impl TaskSpawner for Spawner {
    fn spawn(&self, f: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
        self.0.spawn(f);
        Ok(())
    }
}
struct Context;
impl AuthContext for Context {
    fn env<'a>(
        &'a self,
        _: &'a str,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<String>, AuthContextError>> {
        Box::pin(async { Ok(None) })
    }
    fn file_exists<'a>(
        &'a self,
        _: &'a str,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<bool, AuthContextError>> {
        Box::pin(async { Ok(false) })
    }
}
struct Interaction;
impl AuthInteraction for Interaction {
    fn prompt<'a>(
        &'a self,
        prompt: AuthPrompt,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<String, AuthInteractionError>> {
        assert!(matches!(prompt, AuthPrompt::Secret { .. }));
        Box::pin(async { Ok("synthetic-production-key".into()) })
    }
    fn notify(&self, _: AuthEvent) -> Result<(), AuthInteractionError> {
        Ok(())
    }
}
struct Body(Option<Vec<u8>>);
impl ResponseBody for Body {
    fn next<'a>(
        &'a mut self,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, TransportError>> {
        let b = self.0.take();
        Box::pin(async move { Ok(b) })
    }
    fn close(&mut self) {
        self.0 = None;
    }
}
struct Raw(Arc<Mutex<Vec<Vec<u8>>>>);
impl RawHttpClient for Raw {
    fn send<'a>(
        &'a self,
        request: &'a HttpRequest,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<HttpResponse, TransportError>> {
        Box::pin(async move {
            self.0.lock().unwrap().push(request.body.clone());
            assert!(
                request
                    .headers
                    .iter_for_wire()
                    .any(|(k, v)| k.eq_ignore_ascii_case("authorization")
                        && v == "Bearer synthetic-production-key")
            );
            let mut headers = HttpHeaders::new();
            headers.append("content-type", "text/event-stream")?;
            let bytes=b"data: {\"id\":\"fixture\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"deepseek-chat\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Production port response\"},\"finish_reason\":null}]}\n\ndata: {\"id\":\"fixture\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"deepseek-chat\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n".to_vec();
            Ok(HttpResponse {
                status: 200,
                status_text: "OK".into(),
                headers,
                body: Box::new(Body(Some(bytes))),
            })
        })
    }
}
struct Jitter;
impl RetryJitter for Jitter {
    fn sample(&self) -> f64 {
        0.5
    }
}
fn directory() -> PathBuf {
    std::env::temp_dir().join(format!(
        "pirua-acp-production-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}
#[test]
fn native_registry_auth_persistence_stream_and_logout_use_real_ports() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let root = directory();
        std::fs::create_dir_all(&root).unwrap();
        let credentials = Arc::new(Credentials::open(&root.join("auth.sqlite")).unwrap());
        let runtime = RequestRuntime::with_noop_telemetry(
            CancellationToken::new(),
            Arc::new(TokioClock::new(rt.handle().clone())),
            Arc::new(Spawner(rt.handle().clone())),
        );
        let observed = Arc::new(Mutex::new(vec![]));
        let transport = Arc::new(HttpTransport::new(
            Arc::new(Raw(observed.clone())),
            Arc::new(Jitter),
        ));
        let registry = compose_registry(
            credentials.clone(),
            Arc::new(Context),
            transport,
            &runtime,
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            registry
                .get_providers()
                .iter()
                .map(|p| p.id().as_str())
                .collect::<Vec<_>>(),
            ["anthropic", "deepseek", "google", "openai"]
        );
        let provider = ProviderId::new("deepseek").unwrap();
        let token = CancellationToken::new();
        assert!(
            registry
                .check_auth(&provider, &token)
                .await
                .unwrap()
                .is_none()
        );
        registry
            .login(&provider, AuthType::ApiKey, &Interaction, &token)
            .await
            .unwrap();
        assert!(
            registry
                .check_auth(&provider, &token)
                .await
                .unwrap()
                .is_some()
        );
        let reopened = Credentials::open(&root.join("auth.sqlite")).unwrap();
        assert!(reopened.read(&provider, &token).await.unwrap().is_some());
        let model = registry
            .get_models(Some(&provider))
            .into_iter()
            .find(|m| m.id.as_str() == "deepseek-v4-pro")
            .unwrap();
        let stream = registry.stream_simple(
            model,
            serde_json::from_value(
                json!({"messages":[{"role":"user","content":"hello","timestamp":1}]}),
            )
            .unwrap(),
            ModelsRequestOverrides::default(),
            SimpleStreamOptions {
                wire: Default::default(),
                runtime: runtime.clone(),
            },
        );
        let message = stream.result().await;
        assert_eq!(message.stop_reason, StopReason::Stop);
        assert!(
            serde_json::to_string(&message)
                .unwrap()
                .contains("Production port response")
        );
        assert_eq!(observed.lock().unwrap().len(), 1);
        registry.logout(&provider, &token).await.unwrap();
        assert!(credentials.read(&provider, &token).await.unwrap().is_none());
        assert!(
            Credentials::open(&root.join("auth.sqlite"))
                .unwrap()
                .read(&provider, &token)
                .await
                .unwrap()
                .is_none()
        );
        std::fs::remove_dir_all(root).unwrap();
    });
}
#[test]
fn production_entrypoint_never_selects_fixture_and_builds_with_default_settings() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let root = directory();
        let options = Options::parse([
            "--data-dir".into(),
            root.join("data").to_string_lossy().into_owned(),
            "--config-dir".into(),
            root.join("config").to_string_lossy().into_owned(),
        ])
        .unwrap();
        assert!(!options.fixture_provider);
        let app = Application::new(options).await.unwrap();
        assert!(app.registry.is_some());
        assert!(app.models.len() > 4);
        assert_ne!(app.default_model.id.as_str(), "fixture");
        let manager =
            pi_coding_session::SessionManager::create(app.store.clone(), root.to_string_lossy())
                .unwrap();
        let (session, _) = app.build(manager).await.unwrap();
        assert!(session.callable_tool_names().contains(&"bash".into()));
        session.dispose().await.unwrap();
        std::fs::remove_dir_all(root).unwrap();
    });
}

struct ToolRaw(Arc<Mutex<Vec<serde_json::Value>>>);
impl RawHttpClient for ToolRaw {
    fn send<'a>(
        &'a self,
        request: &'a HttpRequest,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<HttpResponse, TransportError>> {
        Box::pin(async move {
            assert!(
                request
                    .headers
                    .iter_for_wire()
                    .any(|(k, v)| k.eq_ignore_ascii_case("authorization")
                        && v == "Bearer synthetic-production-key")
            );
            let index = {
                let mut requests = self.0.lock().unwrap();
                requests.push(serde_json::from_slice(&request.body).unwrap());
                requests.len() - 1
            };
            let (delta, finish) = if index == 0 {
                (
                    json!({"tool_calls":[{"index":0,"id":"native-bash","type":"function","function":{"name":"bash","arguments":json!({"command":"printf 'native tool marker' > native-marker.txt; cat native-marker.txt"}).to_string()}}]}),
                    "tool_calls",
                )
            } else {
                (
                    json!({"content":"Native provider confirmed the tool result"}),
                    "stop",
                )
            };
            let first = json!({"id":"native-turn","object":"chat.completion.chunk","created":1,"model":"deepseek-v4-pro","choices":[{"index":0,"delta":delta,"finish_reason":null}]});
            let last = json!({"id":"native-turn","object":"chat.completion.chunk","created":1,"model":"deepseek-v4-pro","choices":[{"index":0,"delta":{},"finish_reason":finish}]});
            let bytes = format!("data: {first}\n\ndata: {last}\n\ndata: [DONE]\n\n").into_bytes();
            let mut headers = HttpHeaders::new();
            headers.append("content-type", "text/event-stream")?;
            Ok(HttpResponse {
                status: 200,
                status_text: "OK".into(),
                headers,
                body: Box::new(Body(Some(bytes))),
            })
        })
    }
}

#[test]
fn composed_native_provider_declares_tools_executes_them_and_receives_results() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let root = directory();
        let options = Options::parse([
            "--data-dir".into(),
            root.join("data").to_string_lossy().into_owned(),
            "--config-dir".into(),
            root.join("config").to_string_lossy().into_owned(),
            "--model".into(),
            "deepseek/deepseek-v4-pro".into(),
        ])
        .unwrap();
        let mut app = Application::new(options).await.unwrap();
        let credentials = Arc::new(Credentials::open(&root.join("auth.sqlite")).unwrap());
        let observed = Arc::new(Mutex::new(Vec::new()));
        let transport = Arc::new(HttpTransport::new(
            Arc::new(ToolRaw(observed.clone())),
            Arc::new(Jitter),
        ));
        let registry = compose_registry(
            credentials,
            Arc::new(Context),
            transport,
            &app.runtime,
            None,
        )
        .await
        .unwrap();
        registry
            .login(
                &ProviderId::new("deepseek").unwrap(),
                AuthType::ApiKey,
                &Interaction,
                &CancellationToken::new(),
            )
            .await
            .unwrap();
        app.models = registry.get_models(None);
        app.registry = Some(registry);
        let manager =
            pi_coding_session::SessionManager::create(app.store.clone(), root.to_string_lossy())
                .unwrap();
        let (session, _) = app.build(manager).await.unwrap();
        let active = session.active_tool_names();
        assert_eq!(active, ["read", "bash", "edit", "write"]);
        let executions = Arc::new(Mutex::new(Vec::new()));
        let capture = executions.clone();
        session.subscribe(Arc::new(move |event| {
            if let pi_coding_runtime::SessionEvent::Agent(
                pi_agent::AgentEvent::ToolExecutionEnd {
                    tool_call_id,
                    tool_name,
                    result,
                    is_error,
                    ..
                },
            ) = event
            {
                capture
                    .lock()
                    .unwrap()
                    .push((tool_call_id, tool_name, result, is_error));
            }
            Box::pin(async { Ok(()) })
        }));
        session
            .prompt("inspect the requested artifact", Default::default())
            .await
            .unwrap();
        let requests = observed.lock().unwrap().clone();
        let declared = |body: &serde_json::Value| {
            body["tools"]
                .as_array()
                .map(|tools| {
                    tools
                        .iter()
                        .filter_map(|tool| tool["function"]["name"].as_str().map(str::to_owned))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default()
        };
        assert_eq!(
            declared(&requests[0]),
            active,
            "The first actual native HTTP request must declare the active coding tools"
        );
        assert_eq!(
            requests.len(),
            2,
            "The native tool result must drive a second provider request"
        );
        assert_eq!(declared(&requests[1]), active);
        assert_eq!(
            std::fs::read_to_string(root.join("native-marker.txt")).unwrap(),
            "native tool marker",
            "The ordinary native local bash tool must execute"
        );
        let tool = requests[1]["messages"]
            .as_array()
            .unwrap()
            .iter()
            .find(|message| message["role"] == "tool")
            .expect("Native HTTP result turn");
        assert_eq!(tool["tool_call_id"], "native-bash");
        assert_eq!(tool["content"], "native tool marker");
        let executions = executions.lock().unwrap().clone();
        assert_eq!(executions.len(), 1);
        assert_eq!(executions[0].0, "native-bash");
        assert_eq!(executions[0].1, "bash");
        assert!(!executions[0].3);
        let messages = session.messages().unwrap();
        assert_eq!(
            pi_ai::get_current_tools(
                &messages
                    .iter()
                    .filter_map(|message| {
                        if let pi_agent::AgentMessage::BuiltIn(message) = message {
                            Some(message.clone())
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>()
            )
            .iter()
            .map(|tool| tool.name.to_string())
            .collect::<Vec<_>>(),
            active,
            "Canonical context must retain its tool declarations"
        );
        assert!(
            serde_json::to_string(&messages)
                .unwrap()
                .contains("Native provider confirmed the tool result")
        );
        session
            .prompt("continue from the retained result", Default::default())
            .await
            .unwrap();
        let requests = observed.lock().unwrap().clone();
        assert_eq!(requests.len(), 3);
        assert_eq!(declared(&requests[2]), active);
        assert!(
            requests[2]["messages"]
                .as_array()
                .unwrap()
                .iter()
                .any(|message| message["role"] == "tool"
                    && message["content"] == "native tool marker")
        );
        session.dispose().await.unwrap();
        std::fs::remove_dir_all(root).unwrap();
    });
}
