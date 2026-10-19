//! Durable tools over a host-owned, already initialized neutral McpClient.
//! The embedding host drives reconnect/authentication and list-change refreshes.
use super::{json as ai_json, text};
use crate::*;
use base64::Engine;
use pi_ai::{InputContent, Presence};
use pi_mcp::{CallToolResult, McpClient, RequestOptions};
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

pub type OutputSaver = Arc<dyn Fn(Vec<u8>, String) -> RuntimeFuture<String> + Send + Sync>;
#[derive(Clone)]
pub struct McpBindingOptions {
    pub timeout: Duration,
    pub replay: ReplayPolicy,
    pub deferred: bool,
    pub save_output: Option<OutputSaver>,
    pub resource_reader: Option<String>,
}
impl Default for McpBindingOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(30),
            replay: ReplayPolicy::Unsafe,
            deferred: true,
            save_output: None,
            resource_reader: None,
        }
    }
}
struct State {
    runtime: WeakDurable,
    conversation: ConversationId,
    client: McpClient,
    retired: pi_mcp::CancellationToken,
    live: AtomicBool,
}
/// Owns registrations and per-invocation cancellation, never the MCP connection.
/// Drop retires captured tools but leaves the supplied client open.
pub struct McpToolAttachment {
    state: Arc<State>,
    name: String,
}
impl McpToolAttachment {
    pub async fn attach(
        runtime: &Durable,
        conversation: ConversationId,
        server: impl Into<String>,
        client: McpClient,
        options: McpBindingOptions,
        cancellation: pi_ai::CancellationToken,
    ) -> Result<Self> {
        runtime.check_open()?;
        let server = server.into();
        if server.is_empty()
            || !client.is_connected()
            || options.timeout.is_zero()
            || runtime.conversation(conversation).await?.is_none()
        {
            return Err(Error::request(
                "invalid MCP server, timeout, connection or conversation",
            ));
        }
        let token = pi_mcp::CancellationToken::new();
        let request = client.list_tools(RequestOptions {
            cancellation: token.clone(),
            timeout: Some(options.timeout),
            progress: None,
        });
        tokio::pin!(request);
        let tools = tokio::select! { biased; _=cancellation.cancelled()=>{token.cancel();return Err(Error::request("MCP discovery cancelled"));}, result=&mut request=>result.map_err(mcp_error)? };
        runtime.check_open()?;
        let state = Arc::new(State {
            runtime: runtime.downgrade(),
            conversation,
            client: client.clone(),
            retired: pi_mcp::CancellationToken::new(),
            live: AtomicBool::new(true),
        });
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let name = format!(
            "durable-mcp:{}:{}",
            runtime.binding_id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        );
        let mut extension = Extension {
            name: name.clone(),
            ..Default::default()
        };
        let mut raw = BTreeSet::new();
        let mut taken = BTreeSet::new();
        for tool in tools {
            if !raw.insert(tool.name.clone()) {
                return Err(Error::validation(
                    "MCP discovery returned duplicate tool names",
                ));
            }
            let name = pi_coding_runtime::mcp::create_mcp_tool_name(&server, &tool.name, |n| {
                taken.contains(n)
            });
            if !taken.insert(name.clone()) {
                return Err(Error::validation("MCP mapped tool names collide"));
            }
            extension
                .tools
                .push(bind(Arc::downgrade(&state), &server, tool, name, &options)?);
        }
        runtime.registry().install(extension)?;
        Ok(Self { state, name })
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    /// Retire then attach again when the host observes tools/list_changed or a
    /// new initialized client. An old snapshot retains only retired callbacks.
    pub fn close(&self) {
        if self.state.live.swap(false, Ordering::AcqRel) {
            self.state.retired.cancel();
            if let Ok(runtime) = self.state.runtime.upgrade() {
                let _ = runtime.registry().uninstall(&self.name);
            }
        }
    }
}
impl Drop for McpToolAttachment {
    fn drop(&mut self) {
        self.close();
    }
}
impl State {
    fn check(&self, api: &ToolExecutionApi) -> Result<()> {
        self.runtime.upgrade()?.check_open()?;
        api.check_live()?;
        if !self.live.load(Ordering::Acquire)
            || !self.client.is_connected()
            || api.binding_id() != self.runtime.binding_id()
            || api.identity().conversation_id != self.conversation
        {
            return Err(Error::request("foreign or retired MCP tool invocation"));
        }
        Ok(())
    }
}
fn mcp_error(error: impl std::fmt::Display) -> Error {
    Error::request(format!("MCP: {error}"))
}
fn bind(
    weak: Weak<State>,
    server: &str,
    tool: pi_mcp::Tool,
    name: String,
    options: &McpBindingOptions,
) -> Result<ToolRegistration> {
    let mut parameters = tool.input_schema.clone();
    parameters.entry("type").or_insert(json!("object"));
    parameters.entry("properties").or_insert(json!({}));
    let declaration = serde_json::from_value(
        json!({"name":name,"description":tool.description.as_deref().filter(|s|!s.trim().is_empty()).or(tool.title.as_deref()).unwrap_or(&tool.name),"parameters":parameters}),
    )?;
    let schema = pi_coding_runtime::mcp::create_mcp_result_schema(tool.extra.get("outputSchema"))
        .map_err(mcp_error)?;
    let server = server.to_owned();
    let original = tool.name;
    let captured = options.clone();
    let mut registration = ToolRegistration::new(declaration, move |invocation| {
        let weak = weak.clone();
        let server = server.clone();
        let original = original.clone();
        let options = captured.clone();
        Box::pin(async move {
            let state = weak
                .upgrade()
                .ok_or_else(|| Error::request("MCP attachment retired"))?;
            state.check(&invocation.api)?;
            let token = pi_mcp::CancellationToken::new();
            let (progress, mut receiver) = tokio::sync::mpsc::unbounded_channel();
            let call = state.client.call_tool(
                &original,
                serde_json::to_value(invocation.call.call.arguments)?,
                RequestOptions {
                    cancellation: token.clone(),
                    timeout: Some(options.timeout),
                    progress: Some(progress),
                },
            );
            tokio::pin!(call);
            let signal = invocation.api.signal();
            let result = loop {
                tokio::select! {biased;
                    _=signal.cancelled()=>{token.cancel();return Err(Error::request("MCP invocation cancelled"));},
                    _=state.retired.cancelled()=>{token.cancel();return Err(Error::request("MCP attachment retired"));},
                    Some(progress)=receiver.recv()=>{
                        state.check(&invocation.api)?;
                        let message=progress.message.unwrap_or_else(||format!("Progress {}{}",progress.progress,progress.total.map(|t|format!("/{t}")).unwrap_or_default()));
                        let details=ai_json(json!({"server":server,"tool":original,"progress":message}))?;
                        invocation.api.details(details).await?;
                    },
                    result=&mut call=>break result.map_err(mcp_error)?,
                }
            };
            // The neutral client settles its progress sender before completing
            // the call; consume already accepted notifications before returning.
            while let Ok(progress) = receiver.try_recv() {
                state.check(&invocation.api)?;
                invocation.api.details(ai_json(json!({"server":server,"tool":original,"progress":progress.message,"value":progress.progress,"total":progress.total}))?).await?;
            }
            state.check(&invocation.api)?;
            let result = convert_result(&server, &original, result, &options).await?;
            state.check(&invocation.api)?;
            Ok(result)
        })
    });
    registration.replay = options.replay;
    registration.deferred = options.deferred;
    registration.structured_output_schema = Some(schema);
    Ok(registration)
}
/// Model-facing content and full program-facing result remain separate. Unknown
/// MCP fields survive in structured output; only top-level _meta is omitted.
pub async fn convert_result(
    server: &str,
    tool: &str,
    result: CallToolResult,
    options: &McpBindingOptions,
) -> Result<ToolExecutionResult> {
    let mut output = Vec::new();
    let mut diagnostics = Vec::new();
    for block in &result.content {
        let f = &block.fields;
        match block.kind.as_str() {
            "text" => {
                if let Some(t) = f.get("text").and_then(Value::as_str) {
                    output.push(text(t));
                }
            }
            "image" => {
                if let (Some(data), Some(mime)) = (
                    f.get("data").and_then(Value::as_str),
                    f.get("mimeType").and_then(Value::as_str),
                ) {
                    output.push(image(data, mime));
                }
            }
            "resource_link" => {
                let uri = f.get("uri").and_then(Value::as_str).unwrap_or("");
                let label = f
                    .get("title")
                    .or_else(|| f.get("name"))
                    .and_then(Value::as_str)
                    .unwrap_or("");
                let read = options
                    .resource_reader
                    .as_ref()
                    .map(|r| format!("; read with {r} (server {server})"))
                    .unwrap_or_default();
                output.push(text(format!("[Resource {uri} {label}{read}]")));
            }
            "resource" => {
                if let Some(r) = f.get("resource") {
                    let mime = r["mimeType"].as_str().unwrap_or("");
                    if let Some(t) = r["text"].as_str() {
                        output.push(text(t));
                    } else if let Some(blob) = r["blob"].as_str() {
                        if mime.starts_with("image/") {
                            output.push(image(blob, mime));
                        } else {
                            let bytes = base64::engine::general_purpose::STANDARD
                                .decode(blob)
                                .map_err(mcp_error)?;
                            let kind = mime
                                .split(';')
                                .next()
                                .unwrap_or("")
                                .trim()
                                .to_ascii_lowercase();
                            if kind.starts_with("text/")
                                || kind == "application/json"
                                || kind.ends_with("+json")
                                || kind.ends_with("+xml")
                            {
                                output.push(text(String::from_utf8_lossy(&bytes)));
                            } else if let Some(save) = &options.save_output {
                                let path = save(bytes, ".bin".into()).await?;
                                output.push(text(format!(
                                    "[Binary resource {} saved to {path}]",
                                    r["uri"].as_str().unwrap_or("")
                                )));
                            } else {
                                diagnostics.push(ToolDiagnostic{severity:DiagnosticSeverity::Info,message:format!("Binary resource {} ({mime}) is retained in structured output",r["uri"].as_str().unwrap_or("")),code:Some("mcp_binary_resource".into())});
                            }
                        }
                    }
                }
            }
            "audio" => output.push(text(format!(
                "[audio {} omitted]",
                f.get("mimeType")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown type")
            ))),
            other => output.push(text(format!("[unsupported MCP content {other}]"))),
        }
    }
    if result.content.is_empty()
        && let Some(value) = &result.structured_content
    {
        output.push(text(serde_json::to_string_pretty(value)?));
    }
    if result.is_error == Some(true)
        && !output
            .iter()
            .any(|c| matches!(c,InputContent::Text(t) if !t.text.is_empty()))
    {
        output.push(text(format!("MCP tool {server}/{tool} returned an error")));
    }
    let is_error = result.is_error;
    let mut raw = serde_json::to_value(result)?;
    raw.as_object_mut()
        .ok_or_else(|| Error::validation("MCP result is not an object"))?
        .remove("_meta");
    Ok(ToolExecutionResult {
        output: Some(output),
        structured_output: Presence::Value(ai_json(raw)?),
        details: Presence::Value(ai_json(json!({"server":server,"tool":tool}))?),
        is_error,
        diagnostics: (!diagnostics.is_empty()).then_some(diagnostics),
        ..Default::default()
    })
}
fn image(data: &str, mime: &str) -> InputContent {
    InputContent::Image(pi_ai::ImageContent {
        content_type: Default::default(),
        data: data.into(),
        mime_type: mime.into(),
    })
}
