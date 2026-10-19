//! Real Durable admission, model/tool tasks and committed receipts without networking.
use pi_ai::*;
use pi_durable::{session::MemoryStorage, *};
use serde_json::json;
use std::{sync::Arc, time::Duration};

struct ExampleClock;
impl Clock for ExampleClock {
    fn now_unix_ms(&self) -> u64 {
        1
    }
    fn sleep<'a>(
        &'a self,
        duration: Duration,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, std::result::Result<(), Cancelled>> {
        Box::pin(async move {
            tokio::select! { _ = tokio::time::sleep(duration) => Ok(()), _ = cancellation.cancelled() => Err(Cancelled) }
        })
    }
}
struct ExampleSpawner;
impl TaskSpawner for ExampleSpawner {
    fn spawn(&self, task: BoxFuture<'static, ()>) -> std::result::Result<(), SpawnError> {
        tokio::spawn(task);
        Ok(())
    }
}
struct NoCredentials;
impl AuthContext for NoCredentials {
    fn env<'a>(
        &'a self,
        _: &'a str,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, std::result::Result<Option<String>, AuthContextError>> {
        Box::pin(async { Ok(None) })
    }
    fn file_exists<'a>(
        &'a self,
        _: &'a str,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, std::result::Result<bool, AuthContextError>> {
        Box::pin(async { Ok(false) })
    }
}
struct OfflineAdapter {
    api: ApiId,
}
impl ApiAdapter for OfflineAdapter {
    fn api_id(&self) -> &ApiId {
        &self.api
    }
    fn stream(&self, request: TextStreamRequest) -> AssistantStream {
        self.stream_simple(SimpleStreamRequest {
            resolved: request.resolved,
            context: request.context,
            auth: request.auth,
            options: SimpleStreamOptions {
                wire: SimpleStreamOptionsWire {
                    stream: request.options.wire,
                    ..Default::default()
                },
                runtime: request.options.runtime,
            },
        })
    }
    fn fetch_deferred(
        &self,
        _: DeferredFetchRequest,
    ) -> std::result::Result<AssistantStream, CapabilityError> {
        Err(CapabilityError::Unsupported)
    }
    fn cancel_deferred<'a>(
        &'a self,
        _: DeferredCancelRequest,
    ) -> BoxFuture<'a, std::result::Result<(), ModelsError>> {
        Box::pin(async { Err(ModelsError::new(ModelsErrorCode::Provider)) })
    }
    fn stream_simple(&self, request: SimpleStreamRequest) -> AssistantStream {
        let finished_tool = request
            .context
            .messages()
            .iter()
            .any(|m| matches!(m, Message::ToolResult(_)));
        let model = request.resolved.model;
        let (stream, producer) = create_assistant_message_event_stream(AssistantStreamInit {
            api: model.api.clone(),
            provider: model.provider.clone(),
            model: model.id.clone(),
            clock: request.options.runtime.clock.clone(),
            cancellation: request.options.runtime.cancellation,
        });
        let answer: AssistantMessage = serde_json::from_value(json!({
            "role":"assistant", "api":model.api, "provider":model.provider, "model":model.id,
            "content": if finished_tool { json!([{"type":"text","text":"The local status is ready."}]) } else { json!([{"type":"toolCall","id":"status-1","name":"status","arguments":{}}]) },
            "usage":Usage::zero(), "stopReason":if finished_tool { "stop" } else { "toolUse" },
            "timestamp":request.options.runtime.clock.now_unix_ms(), "durationMs":0
        })).expect("static offline response");
        producer
            .push(AssistantMessageEvent::Done {
                reason: if finished_tool {
                    DoneReason::Stop
                } else {
                    DoneReason::ToolUse
                },
                message: Box::new(answer),
            })
            .expect("one terminal event");
        stream
    }
}
struct OfflineProvider {
    id: ProviderId,
    auth: ProviderAuth,
    model: Model,
    adapter: Arc<OfflineAdapter>,
}
impl Provider for OfflineProvider {
    fn id(&self) -> &ProviderId {
        &self.id
    }
    fn name(&self) -> &str {
        "Offline Durable example"
    }
    fn auth(&self) -> &ProviderAuth {
        &self.auth
    }
    fn models(&self) -> std::result::Result<Arc<[Model]>, ModelsError> {
        Ok(vec![self.model.clone()].into())
    }
    fn adapter(&self, api: &ApiId) -> Option<Arc<dyn ApiAdapter>> {
        (api == &self.adapter.api).then(|| self.adapter.clone() as Arc<dyn ApiAdapter>)
    }
}
fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    tokio::runtime::Builder::new_current_thread().enable_all().build()?.block_on(async {
        let models = Arc::new(ModelsRegistry::new(ModelsRegistryOptions { identity: None, credentials: Arc::new(InMemoryCredentialStore::new()), models_store: Box::new(InMemoryModelsStore::new()), auth_context: Arc::new(NoCredentials), clock: Arc::new(ExampleClock), spawner: Arc::new(ExampleSpawner), model_header_policy: Default::default() }));
        let model = serde_json::from_value(json!({"id":"offline", "name":"Offline", "api":"offline-script", "provider":"offline", "baseUrl":"http://localhost:0", "reasoning":false, "input":["text"], "cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0}, "contextWindow":100000, "maxTokens":1000}))?;
        models.set_provider(Arc::new(OfflineProvider { id: ProviderId::new("offline")?, auth: ProviderAuth::no_auth(), model, adapter: Arc::new(OfflineAdapter { api: ApiId::new("offline-script")? }) })).await?;
        let options = DurableOptions::new(models);
        let mut status = ToolRegistration::new(serde_json::from_value(json!({"name":"status", "description":"Read local status", "parameters":{"type":"object","properties":{},"additionalProperties":false}}))?, |invocation| Box::pin(async move {
            invocation.api.report_progress(ToolProgress::Output { chunk: OutputChunk::Text("ready".into()), skipped: None }).await?;
            Ok(ToolExecutionResult::default())
        }));
        status.replay = ReplayPolicy::Safe;
        options.registry.install(Extension { name: "example.status".into(), tools: vec![status], ..Default::default() })?;
        let durable = Durable::open(Arc::new(MemoryStorage::new()), options).await?;
        let conversation = durable.root(ConversationOptions { agent: Some(AgentChange { model: Presence::Value(ModelRef { provider: "offline".into(), model_id: "offline".into() }), instructions: Presence::Value("Use the local status tool.".into()), ..Default::default() }), ..Default::default() }).await?;
        let receipt = conversation.submit(SubmissionDraft::input("Check the status.")).await?.wait().await?;
        durable.wait_for_idle().await?;
        println!("Submission {}: {:?}", receipt.id, receipt.state);
        println!("Durable context has {} messages.", conversation.context(None).await?.messages.len());
        durable.close().await?;
        Ok::<_, Box<dyn std::error::Error>>(())
    })
}
