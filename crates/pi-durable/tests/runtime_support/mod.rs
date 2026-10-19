mod provider;
use pi_ai::{Presence, RequestRuntime};
pub use provider::*;
use serde_json::Value;
use std::sync::{Arc, atomic::AtomicU64};

pub async fn open(
    storage: Arc<dyn pi_durable::session::Storage>,
    provider: &Value,
) -> (
    pi_durable::Durable,
    pi_durable::Conversation,
    Arc<ScriptState>,
) {
    let clock = Arc::new(TestClock(AtomicU64::new(1000)));
    let (models, state) = models(provider, clock.clone()).await;
    let mut options = pi_durable::DurableOptions::new(models);
    let started = std::time::Instant::now();
    options.scheduler.now = Arc::new(move || 1000.0 + started.elapsed().as_secs_f64() * 1000.0);
    options.provider_session_id = Arc::new(|| Ok("fixture-session-1".into()));
    options.request_runtime =
        Arc::new(move |c| RequestRuntime::with_noop_telemetry(c, clock.clone(), Arc::new(Spawner)));
    let d = pi_durable::Durable::open(storage, options).await.unwrap();
    let root = d
        .root(pi_durable::ConversationOptions {
            agent: Some(pi_durable::AgentChange {
                model: Presence::Value(pi_durable::ModelRef {
                    provider: "oracle".into(),
                    model_id: "scripted-1".into(),
                }),
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .unwrap();
    (d, root, state)
}
