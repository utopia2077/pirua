#![allow(dead_code)]
use pi_coding_runtime::*;
use pi_coding_session::{MemoryStore, SessionManager};
use pi_mcp::{
    ClientOptions, McpClient,
    transport::{TransportEvent, memory_transport_pair},
};
use serde_json::{Value, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
#[path = "../session_support/mod.rs"]
pub mod session;

pub async fn coding(
    tools: Vec<pi_agent::AgentTool>,
    hooks: Arc<dyn SessionHooks>,
) -> CodingSession {
    let (stream, _) = session::scripted(vec![]);
    let names = tools
        .iter()
        .map(|t| t.declaration.name.to_string())
        .collect();
    let session = CodingSessionBuilder::new(
        manager_with_parent(Arc::new(MemoryStore::new())),
        session::model(),
        stream,
    )
    .with_tools(tools)
    .with_hooks(hooks)
    .with_system_prompt("test")
    .build()
    .await
    .unwrap();
    session.set_active_tools(names).unwrap();
    session
}
/// `execute_tool` is a nested-call API and needs an originating assistant.
/// Prompt integration tests construct their actual model-issued parent call.
pub fn manager_with_parent(store: Arc<dyn pi_coding_session::SessionStore>) -> SessionManager {
    let mut manager = SessionManager::create(store, "/tmp").unwrap();
    let mut assistant = pi_agent::failure_message(
        &session::model(),
        &pi_agent::AgentError::Tool(String::new()),
        false,
    );
    assistant.stop_reason = pi_ai::StopReason::Stop;
    assistant.error_message = None;
    assistant.content =
        serde_json::from_value(json!([{"type":"text","text":"adapter probe context"}])).unwrap();
    manager
        .append(pi_coding_session::EntryContent::Message {
            message: serde_json::to_value(assistant).unwrap(),
        })
        .unwrap();
    manager
}
pub async fn invoke(
    session: &CodingSession,
    name: &str,
    args: Value,
) -> pi_agent::AgentToolCallOutcome {
    session
        .execute_tool(
            "parent",
            name,
            serde_json::from_value(args).unwrap(),
            pi_agent::CancellationToken::new(),
            None,
        )
        .await
        .unwrap()
}
pub fn source_cases() -> Vec<Value> {
    serde_json::from_str(include_str!(
        "../../../../conformance/coding-tool-adapters-v1.4.0/cases.json"
    ))
    .unwrap()
}
pub fn source_trace() -> Value {
    serde_json::from_str(include_str!(
        "../../../../conformance/coding-tool-adapters-v1.4.0/trace.json"
    ))
    .unwrap()
}
pub struct Peer {
    pub tools: Mutex<Vec<Value>>,
    pub calls: Mutex<Vec<Value>>,
    pub cancelled: AtomicUsize,
}
pub async fn client(tools: Vec<Value>) -> (McpClient, Arc<Peer>) {
    let state = Arc::new(Peer {
        tools: Mutex::new(tools),
        calls: Mutex::new(Vec::new()),
        cancelled: AtomicUsize::new(0),
    });
    let capture = state.clone();
    let (transport, mut peer) = memory_transport_pair();
    tokio::spawn(async move {
        while let Some(message) = peer.incoming.recv().await {
            let result = match message["method"].as_str().unwrap_or("") {
                "initialize" => {
                    json!({"protocolVersion":"2025-11-25","capabilities":{"tools":{"listChanged":true}},"serverInfo":{"name":"fixture","version":"1"}})
                }
                "tools/list" => json!({"tools":capture.tools.lock().unwrap().clone()}),
                "tools/call" => {
                    capture.calls.lock().unwrap().push(message.clone());
                    if message["params"]["name"] == "wait" {
                        continue;
                    }
                    peer.outgoing.send(TransportEvent::Message(json!({"jsonrpc":"2.0","method":"notifications/progress","params":{"progressToken":message["params"]["_meta"]["progressToken"],"progress":1,"total":2,"message":"halfway"}}))).unwrap();
                    json!({"content":[{"type":"text","text":"MCP reply"},{"type":"image","data":"iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR4nGP4z8DwHwAFAAH/iZk9HQAAAABJRU5ErkJggg==","mimeType":"image/png"}],"structuredContent":{"n":7},"_meta":{"private":"omit"}})
                }
                "notifications/cancelled" => {
                    capture.cancelled.fetch_add(1, Ordering::SeqCst);
                    continue;
                }
                _ => continue,
            };
            if !message["id"].is_null() {
                let _ = peer.outgoing.send(TransportEvent::Message(
                    json!({"jsonrpc":"2.0","id":message["id"],"result":result}),
                ));
            }
        }
    });
    (
        McpClient::connect(ClientOptions::new("coding-test", "1"), Arc::new(transport))
            .await
            .unwrap(),
        state,
    )
}
pub fn mcp_tool(name: &str) -> Value {
    json!({"name":name,"description":"fixture tool","inputSchema":{"type":"object","properties":{"value":{"type":"integer"}},"required":["value"]}})
}
pub fn sandbox() -> pi_codemode::Sandbox {
    let path = std::env::var_os("PI_CODEMODE_TEST_WORKER")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            std::path::PathBuf::from(
                std::env::var_os("CARGO_TARGET_DIR")
                    .expect("source build-env.sh and build pi-codemode-worker before this test"),
            )
            .join("debug/pi-codemode-worker")
        });
    pi_codemode::Sandbox::new(pi_codemode::SandboxOptions::new(path)).unwrap()
}
pub fn text_of(result: &pi_agent::AgentToolResult) -> String {
    result
        .content
        .iter()
        .filter_map(|c| {
            if let pi_ai::InputContent::Text(t) = c {
                Some(t.text.as_str())
            } else {
                None
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}
pub fn sqlite_path(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "pi-adapter-{label}-{}-{}.sqlite",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}
