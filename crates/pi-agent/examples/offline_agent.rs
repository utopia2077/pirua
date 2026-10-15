//! Run the compatibility Agent loop with a deterministic, local response.
//! No provider account, network access, or upstream checkout is needed.

use std::sync::Arc;

use pi_agent::schema::{AgentMessage, SettledAssistantMessage};
use pi_agent::{Agent, AgentOptions};
use pi_ai_core::Model;
use serde_json::json;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tokio::runtime::Builder::new_current_thread()
        .build()?
        .block_on(async {
            let agent = Agent::new(local_options());
            agent.prompt(vec![user("Hello, Pirua!")]).await?;

            let state = agent.state();
            println!("{}", serde_json::to_string_pretty(&state.messages)?);
            Ok(())
        })
}

fn local_options() -> AgentOptions {
    AgentOptions {
        model: local_model(),
        system_prompt: Some("You are a helpful assistant.".to_owned()),
        stream: Arc::new(|_, _| Box::pin(async { Ok(local_response()) })),
        convert_to_provider: Arc::new(|messages| {
            Ok(messages
                .into_iter()
                .filter_map(|message| match message {
                    AgentMessage::BuiltIn(message) => Some(message),
                    AgentMessage::Custom(_) => None,
                })
                .collect())
        }),
        transform_context: None,
        tools: Vec::new(),
        hooks: None,
    }
}

fn local_model() -> Model {
    serde_json::from_value(json!({
        "id": "local-demo",
        "name": "Local demo",
        "api": "example",
        "provider": "example",
        "baseUrl": "https://example.invalid",
        "reasoning": false,
        "input": ["text"],
        "cost": {"input": 0.0, "output": 0.0, "cacheRead": 0.0, "cacheWrite": 0.0},
        "contextWindow": 4096,
        "maxTokens": 256
    }))
    .expect("the local example model is valid")
}

fn user(text: &str) -> AgentMessage {
    serde_json::from_value(json!({"role": "user", "content": text, "timestamp": 0}))
        .expect("the local example user message is valid")
}

fn local_response() -> SettledAssistantMessage {
    serde_json::from_value(json!({
        "role": "assistant",
        "content": [{"type": "text", "text": "Hello from the Rust agent loop."}],
        "api": "example",
        "provider": "example",
        "model": "local-demo",
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
    .expect("the local example assistant message is valid")
}
