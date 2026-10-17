//! Coding adapters for explicitly configured, initialized MCP clients.
//!
//! A registry owns only its own registrations. The host owns client transports,
//! authentication, reconnects and the notification-driving loop.
use crate::{RegisteredTool, SessionError, ToolExposure, WeakCodingSession};
use base64::Engine;
use pi_agent::{AgentError, AgentTool, AgentToolResult, CancellationToken};
use pi_ai::{InputContent, JsonSchema, TextContent};
use pi_mcp::{CallToolResult, ClientEvent, McpClient, RequestOptions};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

pub const MCP_OUTPUT_MAX_BYTES: usize = 20 * 1024;
pub type McpOutputSaver = Arc<
    dyn Fn(Vec<u8>, String) -> pi_agent::BoxFuture<'static, Result<String, AgentError>>
        + Send
        + Sync,
>;

/// Model output may spill text/binary resources; script results remain complete.
#[derive(Clone, Default)]
pub struct McpResultOptions {
    pub save_output: Option<McpOutputSaver>,
    /// Set only when the host has installed a resource reader with this name.
    pub resource_reader: Option<String>,
}
#[derive(Clone)]
pub struct McpServerOptions {
    pub exposure: ToolExposure,
    pub default_active: bool,
    pub timeout: Duration,
    pub result: McpResultOptions,
}
impl Default for McpServerOptions {
    fn default() -> Self {
        Self {
            exposure: ToolExposure::Deferred,
            default_active: true,
            timeout: Duration::from_secs(30),
            result: McpResultOptions::default(),
        }
    }
}
#[derive(Clone)]
struct Server {
    client: McpClient,
    options: McpServerOptions,
    source: String,
    names: BTreeMap<String, String>,
    retired: pi_mcp::CancellationToken,
    epoch: u64,
    ready: bool,
}
/// Explicit discovery/refresh/retirement lifecycle, with atomic collision checks.
pub struct McpToolRegistry {
    session: WeakCodingSession,
    id: u64,
    servers: Mutex<BTreeMap<String, Server>>,
}
static NEXT_REGISTRY: AtomicU64 = AtomicU64::new(1);
static NEXT_GENERATION: AtomicU64 = AtomicU64::new(1);
struct AttachGuard<'a> {
    registry: &'a McpToolRegistry,
    server: String,
    epoch: u64,
}
impl Drop for AttachGuard<'_> {
    fn drop(&mut self) {
        let mut servers = crate::session::lock(&self.registry.servers);
        if servers
            .get(&self.server)
            .is_some_and(|state| state.epoch == self.epoch && !state.ready)
            && let Some(state) = servers.remove(&self.server)
        {
            state.retired.cancel();
        }
    }
}
impl McpToolRegistry {
    pub fn new(session: WeakCodingSession) -> Self {
        Self {
            session,
            id: NEXT_REGISTRY.fetch_add(1, Ordering::Relaxed),
            servers: Default::default(),
        }
    }
    /// Does not connect, authenticate, discover credentials, or replace a server.
    pub async fn attach(
        &self,
        server: impl Into<String>,
        client: McpClient,
        options: McpServerOptions,
        cancellation: CancellationToken,
    ) -> Result<Vec<String>, SessionError> {
        let server = server.into();
        if server.is_empty() {
            return Err(SessionError::Invalid(
                "MCP server name must not be empty".into(),
            ));
        }
        let state = Server {
            client,
            options,
            source: format!("mcp:{}:{server}", self.id),
            names: BTreeMap::new(),
            retired: pi_mcp::CancellationToken::new(),
            epoch: NEXT_GENERATION.fetch_add(1, Ordering::Relaxed),
            ready: false,
        };
        {
            let mut servers = crate::session::lock(&self.servers);
            if servers.contains_key(&server) {
                return Err(SessionError::Invalid(format!(
                    "MCP server {server:?} is already attached"
                )));
            }
            servers.insert(server.clone(), state.clone());
        }
        let _guard = AttachGuard {
            registry: self,
            server: server.clone(),
            epoch: state.epoch,
        };
        let tools = Self::discover(&state, cancellation).await?;
        let mut servers = crate::session::lock(&self.servers);
        let current = servers
            .get_mut(&server)
            .filter(|current| current.epoch == state.epoch)
            .ok_or_else(|| {
                SessionError::Invalid("MCP registration changed during discovery".into())
            })?;
        self.install(&server, current, tools)
    }
    /// A failed discovery or collision preserves the complete previous loadout.
    pub async fn refresh(
        &self,
        server: &str,
        cancellation: CancellationToken,
    ) -> Result<Vec<String>, SessionError> {
        let state = crate::session::lock(&self.servers)
            .get(server)
            .filter(|state| state.ready)
            .cloned()
            .ok_or_else(|| {
                SessionError::Invalid(format!("MCP server {server:?} is not attached"))
            })?;
        let tools = Self::discover(&state, cancellation).await?;
        let mut servers = crate::session::lock(&self.servers);
        let current = servers
            .get_mut(server)
            .filter(|current| current.epoch == state.epoch)
            .ok_or_else(|| {
                SessionError::Invalid("MCP registration changed during discovery".into())
            })?;
        self.install(server, current, tools)
    }
    async fn discover(
        state: &Server,
        cancellation: CancellationToken,
    ) -> Result<Vec<pi_mcp::Tool>, SessionError> {
        let request_cancel = pi_mcp::CancellationToken::new();
        let request = state.client.list_tools(RequestOptions {
            cancellation: request_cancel.clone(),
            timeout: Some(state.options.timeout),
            progress: None,
        });
        tokio::pin!(request);
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => { request_cancel.cancel(); Err(SessionError::Invalid("MCP discovery cancelled".into())) },
            _ = state.retired.cancelled() => {request_cancel.cancel(); Err(SessionError::Invalid("MCP registration retired during discovery".into()))},
            result = &mut request => result.map_err(|e| SessionError::Invalid(e.to_string())),
        }
    }
    fn install(
        &self,
        server: &str,
        state: &mut Server,
        tools: Vec<pi_mcp::Tool>,
    ) -> Result<Vec<String>, SessionError> {
        let session = self.session.upgrade().ok_or(SessionError::Disposed)?;
        let mut taken: BTreeSet<String> = state
            .names
            .iter()
            .filter(|(raw, _)| tools.iter().any(|tool| &tool.name == *raw))
            .map(|(_, name)| name.clone())
            .collect();
        let mut names = BTreeMap::new();
        let retired = pi_mcp::CancellationToken::new();
        let mut registrations = Vec::new();
        // Reuse prior names, so a discovery reorder cannot rename a tool.
        for tool in &tools {
            if names.contains_key(&tool.name) {
                return Err(SessionError::Invalid(format!(
                    "MCP server {server:?} discovered duplicate tool {:?}",
                    tool.name
                )));
            }
            let retained = state.names.get(&tool.name);
            let name = retained.cloned().unwrap_or_else(|| {
                create_mcp_tool_name(server, &tool.name, |name| taken.contains(name))
            });
            if retained.is_none() && !taken.insert(name.clone()) {
                return Err(SessionError::Invalid(format!(
                    "MCP tool name collision: {name}"
                )));
            }
            names.insert(tool.name.clone(), name.clone());
            registrations.push(bind_mcp_tool(
                server,
                tool,
                name,
                state.client.clone(),
                &state.options,
                &state.source,
                retired.clone(),
            )?);
        }
        session.replace_tools_for_source(&state.source, registrations)?;
        state.retired.cancel();
        state.retired = retired;
        state.names = names;
        state.epoch = NEXT_GENERATION.fetch_add(1, Ordering::Relaxed);
        state.ready = true;
        Ok(state.names.values().cloned().collect())
    }
    /// Remove this owner's tools and cancel its outstanding requests. The client
    /// stays open because transport ownership belongs to the host.
    pub async fn retire(&self, server: &str) -> Result<(), SessionError> {
        let mut servers = crate::session::lock(&self.servers);
        if let Some(state) = servers.get(server) {
            if let Some(session) = self.session.upgrade() {
                session.replace_tools_for_source(&state.source, Vec::new())?;
            }
            state.retired.cancel();
        }
        servers.remove(server);
        Ok(())
    }
    /// Host-driven notifications avoid an implicit background task lifetime.
    /// Returns whether this event changed the registered loadout.
    pub async fn handle_event(
        &self,
        server: &str,
        event: ClientEvent,
        cancellation: CancellationToken,
    ) -> Result<bool, SessionError> {
        match event {
            ClientEvent::Notification { method, .. }
                if method == "notifications/tools/list_changed" =>
            {
                self.refresh(server, cancellation).await?;
                Ok(true)
            }
            ClientEvent::Closed => {
                self.retire(server).await?;
                Ok(true)
            }
            _ => Ok(false),
        }
    }
}
impl Drop for McpToolRegistry {
    fn drop(&mut self) {
        for state in self
            .servers
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .values()
        {
            state.retired.cancel();
            if let Some(session) = self.session.upgrade() {
                let _ = session.replace_tools_for_source(&state.source, Vec::new());
            }
        }
    }
}

/// Target naming algorithm: ASCII sanitization, 64 byte maximum, SHA-256 suffix.
pub fn create_mcp_tool_name(server: &str, tool: &str, is_taken: impl Fn(&str) -> bool) -> String {
    let name: String = format!("mcp__{server}__{tool}")
        .encode_utf16()
        .map(|c| {
            if c <= 127 && ((c as u8).is_ascii_alphanumeric() || c == u16::from(b'_')) {
                char::from(c as u8)
            } else {
                '_'
            }
        })
        .collect();
    if name.len() <= 64 && !is_taken(&name) {
        return name;
    }
    let digest = ring::digest::digest(
        &ring::digest::SHA256,
        format!("{server}\0{tool}").as_bytes(),
    );
    let suffix: String = digest.as_ref()[..4]
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    format!("{}_{}", &name[..name.len().min(55)], suffix)
}

fn bind_mcp_tool(
    server: &str,
    tool: &pi_mcp::Tool,
    name: String,
    client: McpClient,
    options: &McpServerOptions,
    source: &str,
    retired: pi_mcp::CancellationToken,
) -> Result<RegisteredTool, SessionError> {
    let server = server.to_owned();
    let original = tool.name.clone();
    let mut parameters = tool.input_schema.clone();
    parameters.entry("type").or_insert(json!("object"));
    parameters.entry("properties").or_insert(json!({}));
    let declaration = serde_json::from_value(
        json!({"name":name,"description":tool.description.as_deref().filter(|s| !s.trim().is_empty()).or(tool.title.as_deref()).unwrap_or(&tool.name),"parameters":parameters}),
    )?;
    let result_options = options.result.clone();
    let timeout = options.timeout;
    let label = format!("{server}/{original}");
    let namespace = server.clone();
    let mut bound = AgentTool::new(
        declaration,
        Arc::new(move |invocation| {
            let client = client.clone();
            let server = server.clone();
            let original = original.clone();
            let retired = retired.clone();
            let result_options = result_options.clone();
            Box::pin(async move {
                let cancellation = pi_mcp::CancellationToken::new();
                let (progress, mut receiver) = tokio::sync::mpsc::unbounded_channel();
                let arguments = serde_json::to_value(invocation.arguments)
                    .map_err(|e| AgentError::Tool(e.to_string()))?;
                let call = client.call_tool(
                    &original,
                    arguments,
                    RequestOptions {
                        cancellation: cancellation.clone(),
                        timeout: Some(timeout),
                        progress: Some(progress),
                    },
                );
                tokio::pin!(call);
                let result = loop {
                    tokio::select! {
                        biased;
                        _ = invocation.cancellation.cancelled() => { cancellation.cancel(); return Err(AgentError::Tool("MCP tool call cancelled".into())); },
                        _ = retired.cancelled() => { cancellation.cancel(); return Err(AgentError::Tool("MCP tool registration retired".into())); },
                        Some(progress) = receiver.recv() => {
                            let message = progress.message.unwrap_or_else(|| format!("Progress {}{}", progress.progress, progress.total.map(|total| format!("/{total}")).unwrap_or_default()));
                            (invocation.on_update)(AgentToolResult { content: vec![text(message)], details: Some(to_ai_json(json!({"server":server,"tool":original}))?), ..Default::default() });
                        },
                        result = &mut call => break result.map_err(|e| AgentError::Tool(e.to_string()))?,
                    }
                };
                convert_mcp_result(&server, &original, result, &result_options).await
            })
        }),
    );
    bound.label = label;
    bound.output_schema = Some(create_mcp_result_schema(tool.extra.get("outputSchema"))?);
    Ok(RegisteredTool {
        tool: bound,
        exposure: options.exposure,
        source: Some(source.into()),
        default_active: options.default_active,
        namespace: Some(namespace),
        prompt_snippet: None,
        prompt_guidelines: Vec::new(),
    })
}

pub fn create_mcp_result_schema(structured: Option<&Value>) -> Result<JsonSchema, SessionError> {
    let mut schema = json!({"type":"object","properties":{"content":{"type":"array","items":{"type":"object","properties":{"type":{"enum":["text","image","audio","resource_link","resource"]},"text":{"type":"string"},"data":{"type":"string"},"mimeType":{"type":"string"}},"required":["type"],"additionalProperties":true}},"structuredContent":{},"isError":{"type":"boolean"}},"required":["content"],"additionalProperties":true});
    if let Some(structured) = structured {
        schema["properties"]["structuredContent"] =
            relocate_schema(structured, "#/properties/structuredContent");
    }
    Ok(serde_json::from_value(schema)?)
}
fn relocate_schema(value: &Value, pointer: &str) -> Value {
    match value {
        Value::Array(items) => {
            Value::Array(items.iter().map(|v| relocate_schema(v, pointer)).collect())
        }
        Value::Object(fields) => Value::Object(
            fields
                .iter()
                .map(|(key, value)| {
                    let relocated = match key.as_str() {
                        "$ref"
                            if value
                                .as_str()
                                .is_some_and(|s| s == "#" || s.starts_with("#/")) =>
                        {
                            json!(format!("{pointer}{}", &value.as_str().unwrap()[1..]))
                        }
                        "const" | "enum" | "default" | "examples" => value.clone(),
                        "properties" | "patternProperties" | "dependentSchemas"
                        | "dependencies" | "$defs" | "definitions"
                            if value.is_object() =>
                        {
                            Value::Object(
                                value
                                    .as_object()
                                    .unwrap()
                                    .iter()
                                    .map(|(name, schema)| {
                                        (name.clone(), relocate_schema(schema, pointer))
                                    })
                                    .collect(),
                            )
                        }
                        _ => relocate_schema(value, pointer),
                    };
                    (key.clone(), relocated)
                })
                .collect(),
        ),
        _ => value.clone(),
    }
}
pub(crate) fn text(value: impl Into<String>) -> InputContent {
    InputContent::Text(TextContent {
        content_type: Default::default(),
        text: value.into(),
        text_signature: None,
    })
}
fn image(data: &str, mime_type: &str) -> InputContent {
    InputContent::Image(pi_ai::ImageContent {
        content_type: Default::default(),
        data: data.into(),
        mime_type: mime_type.into(),
    })
}
pub(crate) fn text_of(content: &[InputContent]) -> String {
    content
        .iter()
        .filter_map(|v| {
            if let InputContent::Text(t) = v {
                Some(t.text.as_str())
            } else {
                None
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}
pub(crate) fn to_ai_json(value: Value) -> Result<pi_ai::JsonValue, AgentError> {
    serde_json::from_value(value)
        .map_err(|e| AgentError::Tool(format!("Invalid tool JSON boundary: {e}")))
}
pub(crate) async fn save_output(
    saver: &Option<McpOutputSaver>,
    data: Vec<u8>,
    extension: String,
) -> Result<String, AgentError> {
    if let Some(saver) = saver {
        return saver(data, extension).await;
    }
    tokio::task::spawn_blocking(move || {
        use std::io::Write;
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "pi-tool-output-{}-{stamp}-{}{extension}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|e| AgentError::Tool(e.to_string()))?;
        file.write_all(&data)
            .map_err(|e| AgentError::Tool(e.to_string()))?;
        Ok(path.to_string_lossy().into_owned())
    })
    .await
    .map_err(|e| AgentError::Tool(e.to_string()))?
}
/// Model projection follows target content semantics; structured content retains
/// unknown protocol fields, and only the result's top-level `_meta` is omitted.
pub async fn convert_mcp_result(
    server: &str,
    tool: &str,
    result: CallToolResult,
    options: &McpResultOptions,
) -> Result<AgentToolResult, AgentError> {
    let mut content = Vec::new();
    for block in &result.content {
        let fields = &block.fields;
        match block.kind.as_str() {
            "text" => {
                if let Some(value) = fields.get("text").and_then(Value::as_str) {
                    content.push(text(value));
                }
            }
            "image" => {
                if let (Some(data), Some(mime)) = (
                    fields.get("data").and_then(Value::as_str),
                    fields.get("mimeType").and_then(Value::as_str),
                ) {
                    content.push(image(data, mime));
                }
            }
            "resource_link" => {
                let uri = fields.get("uri").and_then(Value::as_str).unwrap_or("");
                let title = fields
                    .get("title")
                    .or_else(|| fields.get("name"))
                    .and_then(Value::as_str)
                    .unwrap_or("");
                let description = fields
                    .get("description")
                    .and_then(Value::as_str)
                    .map(|v| format!(": {v}"))
                    .unwrap_or_default();
                let read = options
                    .resource_reader
                    .as_ref()
                    .map(|name| format!(". Read it with {name} (server {server:?})"))
                    .unwrap_or_default();
                let mut details = Vec::new();
                if let Some(mime) = fields.get("mimeType").and_then(Value::as_str) {
                    details.push(mime.into());
                }
                if let Some(size) = fields.get("size").and_then(Value::as_u64) {
                    details.push(format_size(size as usize));
                }
                let details = if details.is_empty() {
                    String::new()
                } else {
                    format!(" ({})", details.join(", "))
                };
                content.push(text(format!(
                    "[Resource {uri} {title:?}{details}{description}{read}]"
                )));
            }
            "resource" => {
                if let Some(resource) = fields.get("resource") {
                    let uri = resource["uri"].as_str().unwrap_or("");
                    let mime = resource["mimeType"].as_str().unwrap_or("");
                    if let Some(value) = resource["text"].as_str() {
                        content.push(text(value));
                    } else if let Some(blob) = resource["blob"].as_str() {
                        if mime.starts_with("image/") {
                            content.push(image(blob, mime));
                        } else {
                            let data = base64::engine::general_purpose::STANDARD
                                .decode(blob)
                                .map_err(|e| {
                                    AgentError::Tool(format!("Invalid MCP resource base64: {e}"))
                                })?;
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
                                content.push(text(String::from_utf8_lossy(&data)));
                            } else {
                                let size = format_size(data.len());
                                let extension = resource_extension(uri);
                                let saved =
                                    save_output(&options.save_output, data, extension).await;
                                content.push(text(match saved { Ok(path) => format!("[Binary resource {uri} ({}, {size}) saved to {path}]", if mime.is_empty() { "unknown type" } else { mime }), Err(error) => format!("[Binary resource {uri} ({mime}, {size}) could not be saved: {error}]") }));
                            }
                        }
                    }
                }
            }
            "audio" => content.push(text(format!(
                "[audio {} omitted]",
                fields
                    .get("mimeType")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown type")
            ))),
            _ => content.push(text(format!("[unsupported MCP content {}]", block.kind))),
        }
    }
    if result.content.is_empty()
        && let Some(value) = &result.structured_content
    {
        content.push(text(serde_json::to_string_pretty(value).unwrap()));
    }
    if result.is_error == Some(true) && text_of(&content).is_empty() {
        content.push(text(format!("MCP tool {server}/{tool} returned an error")));
    }
    let mut details = json!({"server":server,"tool":tool});
    let combined = text_of(&content);
    if combined.len() > MCP_OUTPUT_MAX_BYTES {
        let saved = save_output(
            &options.save_output,
            combined.as_bytes().to_vec(),
            ".txt".into(),
        )
        .await;
        let where_ = match saved {
            Ok(path) => {
                details["fullOutputPath"] = json!(path);
                format!("[Full output: {path} (read it with offset/limit)]")
            }
            Err(error) => format!("[Could not save the full output: {error}]"),
        };
        let limited = truncate_middle(&combined, MCP_OUTPUT_MAX_BYTES);
        content.retain(|c| matches!(c, InputContent::Image(_)));
        content.insert(0, text(format!("Warning: truncated output (original token count: {})\nTotal output lines: {}\n\n{limited}\n\n{where_}", combined.len().div_ceil(4), combined.lines().count())));
    }
    let is_error = result.is_error.filter(|v| *v);
    let mut script = serde_json::to_value(result).map_err(|e| AgentError::Tool(e.to_string()))?;
    script.as_object_mut().unwrap().remove("_meta");
    Ok(AgentToolResult {
        content,
        details: Some(to_ai_json(details)?),
        structured_content: Some(to_ai_json(script)?),
        is_error,
        ..Default::default()
    })
}
fn resource_extension(uri: &str) -> String {
    let parsed = url::Url::parse(uri).ok();
    let path = parsed.as_ref().map(|u| u.path()).unwrap_or(uri);
    if let Some((_, ext)) = path.rsplit_once('.')
        && !ext.is_empty()
        && ext.len() <= 8
        && ext.bytes().all(|b| b.is_ascii_alphanumeric())
    {
        return format!(".{ext}");
    }
    ".bin".into()
}
pub(crate) fn truncate_middle(value: &str, bytes: usize) -> String {
    if value.len() <= bytes {
        return value.into();
    }
    let mut head = bytes / 2;
    while !value.is_char_boundary(head) {
        head -= 1;
    }
    let mut tail = value.len() - (bytes - bytes / 2);
    while !value.is_char_boundary(tail) {
        tail += 1;
    }
    format!(
        "{}…{} chars truncated…{}",
        &value[..head],
        value[head..tail].chars().count(),
        &value[tail..]
    )
}
pub(crate) fn format_size(bytes: usize) -> String {
    if bytes < 1024 {
        format!("{bytes}B")
    } else if bytes < 1024 * 1024 {
        format!("{:.1}KB", bytes as f64 / 1024.)
    } else {
        format!("{:.1}MB", bytes as f64 / (1024. * 1024.))
    }
}
