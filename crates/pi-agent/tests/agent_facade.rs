use std::sync::{Arc, Mutex};
use std::time::Duration;

use pi_agent::{
    Agent, AgentListener, AgentOptions, LegacyAgentError, LegacyConvertFn, LegacyStreamFn,
};
use pi_agent_schema::{AgentEvent, AgentMessage};
use pi_ai_core::{Message, Model};

fn model() -> Model {
    serde_json::from_value(serde_json::json!({
        "id": "model",
        "name": "Model",
        "api": "api",
        "provider": "provider",
        "baseUrl": "https://example.invalid",
        "reasoning": false,
        "input": ["text"],
        "cost": {"input": 0.0, "output": 0.0, "cacheRead": 0.0, "cacheWrite": 0.0},
        "contextWindow": 1000,
        "maxTokens": 100
    }))
    .expect("valid model")
}

fn user(text: &str) -> AgentMessage {
    serde_json::from_value(serde_json::json!({
        "role": "user",
        "content": text,
        "timestamp": 0
    }))
    .expect("valid user")
}

fn assistant(text: &str) -> pi_agent_schema::SettledAssistantMessage {
    serde_json::from_value(serde_json::json!({
        "role": "assistant",
        "content": [{"type": "text", "text": text}],
        "api": "api",
        "provider": "provider",
        "model": "model",
        "usage": {
            "input": 0,
            "output": 0,
            "cacheRead": 0,
            "cacheWrite": 0,
            "totalTokens": 0,
            "cost": {"input": 0.0, "output": 0.0, "cacheRead": 0.0, "cacheWrite": 0.0, "total": 0.0}
        },
        "stopReason": "stop",
        "timestamp": 0
    }))
    .expect("valid assistant")
}

fn converter() -> LegacyConvertFn {
    Arc::new(|messages| {
        Ok(messages
            .into_iter()
            .filter_map(|message| match message {
                AgentMessage::BuiltIn(message) => Some(message),
                AgentMessage::Custom(_) => None,
            })
            .collect())
    })
}

fn options(stream: LegacyStreamFn) -> AgentOptions {
    AgentOptions {
        model: model(),
        system_prompt: Some("system".to_owned()),
        stream,
        convert_to_provider: converter(),
        transform_context: None,
        tools: Vec::new(),
    }
}

#[test]
fn prompt_reduces_state_and_listener_order_is_awaited() {
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let listener_log = Arc::new(Mutex::new(Vec::<&'static str>::new()));
        let first_log = listener_log.clone();
        let first: AgentListener = Arc::new(move |event, _| {
            let first_log = first_log.clone();
            Box::pin(async move {
                if matches!(event, AgentEvent::AgentEnd { .. }) {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                first_log.lock().unwrap().push("first");
                Ok(())
            })
        });
        let second_log = listener_log.clone();
        let second: AgentListener = Arc::new(move |_event, _| {
            let second_log = second_log.clone();
            Box::pin(async move {
                second_log.lock().unwrap().push("second");
                Ok(())
            })
        });
        let stream: LegacyStreamFn = Arc::new(|_, _| Box::pin(async { Ok(assistant("done")) }));
        let agent = Agent::new(options(stream));
        agent.subscribe(first);
        agent.subscribe(second);

        agent
            .prompt(vec![user("hello")])
            .await
            .expect("prompt succeeds");
        let state = agent.state();
        assert!(!state.is_streaming);
        assert_eq!(state.messages.len(), 2);
        let log = listener_log.lock().unwrap();
        assert_eq!(log.len(), 16);
        assert!(log.chunks(2).all(|chunk| chunk == ["first", "second"]));
    });
}

#[test]
fn abort_propagates_signal_and_reset_clears_transcript() {
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let stream: LegacyStreamFn = Arc::new(|_, cancellation| {
            Box::pin(async move {
                cancellation.cancelled().await;
                Err(LegacyAgentError::Provider("cancelled".to_owned()))
            })
        });
        let agent = Agent::new(options(stream));
        let running = {
            let agent = agent.clone();
            tokio::spawn(async move { agent.prompt(vec![user("wait")]).await })
        };
        let idle_waiter = {
            let agent = agent.clone();
            tokio::spawn(async move { agent.wait_for_idle().await })
        };
        for _ in 0..1000 {
            if agent.state().is_streaming {
                break;
            }
            tokio::task::yield_now().await;
        }
        if !agent.state().is_streaming {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert!(agent.state().is_streaming);
        agent.abort();
        running
            .await
            .expect("task joins")
            .expect("abort is in-band");
        idle_waiter.await.expect("idle waiter joins");
        assert!(!agent.state().is_streaming);
        assert!(agent.state().messages.iter().any(|message| {
            matches!(
                message,
                AgentMessage::BuiltIn(Message::Assistant(message))
                    if message.stop_reason == pi_ai_core::StopReason::Aborted
            )
        }));
        agent.reset().expect("idle reset succeeds");
        assert!(agent.state().messages.is_empty());
    });
}

#[test]
fn reentrancy_is_rejected_while_a_run_is_active() {
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let stream: LegacyStreamFn = Arc::new(|_, cancellation| {
            Box::pin(async move {
                cancellation.cancelled().await;
                Err(LegacyAgentError::Provider("cancelled".to_owned()))
            })
        });
        let agent = Agent::new(options(stream));
        let running = {
            let agent = agent.clone();
            tokio::spawn(async move { agent.prompt(vec![user("first")]).await })
        };
        for _ in 0..100 {
            if agent.state().is_streaming {
                break;
            }
            tokio::task::yield_now().await;
        }
        let error = agent
            .prompt(vec![user("second")])
            .await
            .expect_err("second prompt must be rejected");
        assert!(matches!(error, LegacyAgentError::InvalidContext(_)));
        agent.abort();
        running
            .await
            .expect("task joins")
            .expect("abort is in-band");
    });
}
