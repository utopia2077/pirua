//! A complete offline prompt/tool/save/reopen/fork/compact flow.
use pi_agent::{AgentError, AgentTool, BoxFuture, CancellationToken, StreamFn};
use pi_ai::{
    AssistantMessageEvent, AssistantStreamInit, Cancelled, Clock, DoneReason, Model, SafeUnsigned,
    StopReason,
};
use pi_coding_runtime::*;
use pi_coding_session::{SessionManager, SqliteStore};
use serde_json::json;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
struct OfflineClock;
impl Clock for OfflineClock {
    fn now_unix_ms(&self) -> u64 {
        1
    }
    fn sleep<'a>(
        &'a self,
        _: Duration,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), Cancelled>> {
        Box::pin(async { Ok(()) })
    }
}
struct OfflineHooks;
impl SessionHooks for OfflineHooks {
    fn before_compact(
        &self,
        p: CompactionPreparation,
        _: CompactionReason,
        _: CancellationToken,
    ) -> BoxFuture<'_, Result<BeforeCompactionResult, AgentError>> {
        Box::pin(async move {
            Ok(BeforeCompactionResult {
                cancel: false,
                compaction: Some(CompactionResult {
                    summary: "The offline fixture looked up the answer and saved its result."
                        .into(),
                    first_kept_entry_id: Some(p.first_kept_entry_id),
                    tokens_before: p.tokens_before,
                    details: Some(p.file_ops.details()),
                    usage: None,
                }),
            })
        })
    }
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(run())
}
async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let model: Model = serde_json::from_value(
        json!({"id":"offline","name":"Offline fixture","api":"openai-responses","provider":"openai","baseUrl":"https://example.invalid/v1","reasoning":false,"input":["text"],"cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0},"contextWindow":100000,"maxTokens":1000}),
    )?;
    let calls = Arc::new(AtomicUsize::new(0));
    let capture = calls.clone();
    let stream: StreamFn = Arc::new(move |request| {
        let calls = capture.clone();
        Box::pin(async move {
            let (stream, producer) =
                pi_ai::create_assistant_message_event_stream(AssistantStreamInit {
                    api: request.model.api.clone(),
                    provider: request.model.provider.clone(),
                    model: request.model.id.clone(),
                    clock: Arc::new(OfflineClock),
                    cancellation: request.cancellation,
                });
            let mut message = pi_agent::failure_message(
                &request.model,
                &AgentError::Callback(String::new()),
                false,
            );
            message.error_message = None;
            let first = calls.fetch_add(1, Ordering::Relaxed) == 0;
            message.content = serde_json::from_value(if first {
                json!([{"type":"toolCall","id":"lookup-1","name":"lookup","arguments":{}}])
            } else {
                json!([{"type":"text","text":"The answer is 42."}])
            })
            .unwrap();
            message.stop_reason = if first {
                StopReason::ToolUse
            } else {
                StopReason::Stop
            };
            producer
                .push(AssistantMessageEvent::Done {
                    reason: if first {
                        DoneReason::ToolUse
                    } else {
                        DoneReason::Stop
                    },
                    message: Box::new(message),
                })
                .unwrap();
            Ok(stream)
        })
    });
    let tool = AgentTool::new(
        serde_json::from_value(
            json!({"name":"lookup","description":"Look up the offline answer","parameters":{"type":"object","properties":{}}}),
        )?,
        Arc::new(|_| {
            Box::pin(async {
                Ok(pi_agent::AgentToolResult {
                    content: serde_json::from_value(json!([{"type":"text","text":"42"}])).unwrap(),
                    ..Default::default()
                })
            })
        }),
    );
    let path = std::env::temp_dir().join(format!(
        "pirua-headless-{}-{}.sqlite",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos()
    ));
    let store = Arc::new(SqliteStore::open(&path)?);
    let manager =
        SessionManager::create(store.clone(), std::env::current_dir()?.to_string_lossy())?;
    let settings = CodingSettingsSnapshot::new(
        ModelRef::new(model.provider.clone(), model.id.clone()),
        ThinkingLevel::Off,
        vec!["lookup".to_owned().try_into()?],
        pi_ai::RetryPolicy {
            enabled: false,
            max_retries: pi_ai::MaxRetries::new(0)?,
            base_delay_ms: SafeUnsigned::new(0)?,
            max_agent_delay_ms: None,
        },
        CompactionSettings {
            enabled: false,
            keep_recent_tokens: SafeUnsigned::new(1)?,
            ..Default::default()
        },
        Default::default(),
        QueueMode::OneAtATime,
        QueueMode::OneAtATime,
    );
    let session = CodingSessionBuilder::new(manager, model.clone(), stream.clone())
        .with_settings(settings)
        .with_tools(vec![tool])
        .with_hooks(Arc::new(OfflineHooks))
        .with_system_prompt("Use the offline lookup tool.")
        .build()
        .await?;
    session
        .prompt("What is the answer?", PromptOptions::default())
        .await?;
    session.set_name("Offline example").await?;
    let id = session.session_id();
    let leaf = session.snapshot().leaf_id.unwrap();
    let saved_count = session.stats().total_messages;
    let reopened =
        CodingSessionBuilder::new(SessionManager::open(store.clone(), &id)?, model, stream)
            .with_tools(vec![])
            .with_hooks(Arc::new(OfflineHooks))
            .with_system_prompt("Continue the saved conversation.")
            .build()
            .await?;
    assert_eq!(reopened.stats().total_messages, saved_count);
    assert_eq!(calls.load(Ordering::Relaxed), 2);
    reopened.fork(&leaf).await?;
    // The original settings snapshot is host-owned; configure the fork's compaction tail explicitly.
    reopened.set_compaction_settings(CompactionSettings {
        enabled: false,
        keep_recent_tokens: SafeUnsigned::new(1)?,
        ..Default::default()
    })?;
    let summary = reopened.compact(None).await?;
    println!(
        "Saved {saved_count} messages in {id}; fork {} compacted {} context tokens.",
        reopened.session_id(),
        summary.tokens_before
    );
    reopened.dispose().await?;
    session.dispose().await?;
    drop(reopened);
    drop(session);
    drop(store);
    std::fs::remove_file(path)?;
    Ok(())
}
