//! Optional client for Pi's server-side Agent stream proxy protocol.
//! HTTP, execution and authentication are supplied explicitly by the host.
use futures_util::{FutureExt, future::poll_fn};
use pi_ai_core::{
    AssistantContent, AssistantMessage, AssistantMessageEvent, AssistantStream,
    AssistantStreamInit, AssistantStreamProducer, BoxFuture, CancellationToken, DoneReason,
    ErrorReason, JsonObject, JsonValue, Model, ProviderHeaders, RequestEndpoint, RequestRuntime,
    SafeUnsigned, SecretString, SimpleStreamOptionsWire, StopReason, StreamSetupFailure,
    TextContent, ThinkingContent, ToolCall, TranscriptContext, Usage,
    create_assistant_message_event_stream, parse_streaming_json,
};
use pi_ai_transport::{
    HttpHeaders, HttpRequest, RawHttpClient, ResponseBody, ResponseBodyPresence, SseLimits,
    TransportError,
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap, fmt, future::Future, panic::AssertUnwindSafe, sync::Arc, task::Poll,
};

/// Invalid native proxy configuration. The URL and token stay out of errors.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProxyConfigError;
impl fmt::Display for ProxyConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("invalid proxy base URL")
    }
}
impl std::error::Error for ProxyConfigError {}

/// Explicit proxy-server credentials and provider-facing serializable options.
/// Neither the token nor the proxy URL is included in the JSON request body.
/// Identity, provider API keys, callbacks and unsupported wire options are never
/// inferred or forwarded. `headers` is an explicit provider-header override.
#[derive(Clone)]
pub struct ProxyOptions {
    auth_token: SecretString,
    endpoint: Arc<RequestEndpoint>,
    pub stream_options: SimpleStreamOptionsWire,
    pub headers: Option<ProviderHeaders>,
}
impl ProxyOptions {
    /// Appends `/api/stream` to an HTTP(S) base URL (including its path). A
    /// trailing slash is normalized; credentials/query/fragment are rejected so
    /// the route cannot accidentally become part of a query or an auth source.
    pub fn new(auth_token: SecretString, base_url: &str) -> Result<Self, ProxyConfigError> {
        let mut url = url::Url::parse(base_url).map_err(|_| ProxyConfigError)?;
        if !matches!(url.scheme(), "http" | "https")
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(ProxyConfigError);
        }
        let path = format!("{}/api/stream", url.path().trim_end_matches('/'));
        url.set_path(&path);
        Ok(Self {
            auth_token,
            endpoint: Arc::new(
                RequestEndpoint::new_resolved(url.as_str()).map_err(|_| ProxyConfigError)?,
            ),
            stream_options: SimpleStreamOptionsWire::default(),
            headers: None,
        })
    }
    pub fn endpoint(&self) -> &RequestEndpoint {
        &self.endpoint
    }
}
impl fmt::Debug for ProxyOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ProxyOptions([REDACTED])")
    }
}

/// Reconstructs an assistant stream from the v1.4.0 proxy's `data: ` JSON lines.
/// The host supplies a one-attempt HTTP client and an explicit request runtime.
/// Cancellation settles through the core stream's single terminal authority and
/// closes an acquired response body; observed partial content is retained.
pub fn stream_proxy(
    model: Model,
    context: TranscriptContext,
    options: ProxyOptions,
    http: Arc<dyn RawHttpClient>,
    runtime: RequestRuntime,
) -> AssistantStream {
    let (stream, producer) = create_assistant_message_event_stream(AssistantStreamInit {
        api: model.api.clone(),
        provider: model.provider.clone(),
        model: model.id.clone(),
        clock: runtime.clock.clone(),
        cancellation: runtime.cancellation.clone(),
    });
    let worker = producer.clone();
    let task_runtime = runtime.clone();
    let task = Box::pin(async move {
        let mut partial = initial_message(&model, &task_runtime);
        let _ = worker.publish_cancellation_snapshot(partial.clone());
        let result = AssertUnwindSafe(run_proxy(
            &model,
            &context,
            &options,
            http.as_ref(),
            &task_runtime,
            &worker,
            &mut partial,
        ))
        .catch_unwind()
        .await;
        let error = match result {
            Ok(Ok(())) => None,
            Ok(Err(error)) => Some(error),
            Err(_) => Some("Proxy stream task panicked".into()),
        };
        if let Some(error) = error {
            let reason = if task_runtime.cancellation.is_cancelled() {
                ErrorReason::Aborted
            } else {
                ErrorReason::Error
            };
            partial.stop_reason = if reason == ErrorReason::Aborted {
                StopReason::Aborted
            } else {
                StopReason::Error
            };
            partial.error_message = Some(error);
            let _ = worker.push(AssistantMessageEvent::Error {
                reason,
                error: Box::new(partial),
            });
        }
    });
    if runtime.spawner.spawn(task).is_err() {
        let _ = producer.fail_setup(StreamSetupFailure::Spawn);
    }
    stream
}

/// Agent adapter. Only the allowlisted wire options cross the proxy boundary;
/// the AgentRequest's provider `api_key` and process-local callbacks are dropped.
pub fn proxy_stream_fn(
    options: ProxyOptions,
    http: Arc<dyn RawHttpClient>,
    runtime: RequestRuntime,
) -> crate::StreamFn {
    Arc::new(move |request| {
        let (mut options, http, mut runtime) = (options.clone(), http.clone(), runtime.clone());
        options.stream_options = request.options;
        runtime.cancellation = request.cancellation;
        Box::pin(async move {
            Ok(stream_proxy(
                request.model,
                request.context,
                options,
                http,
                runtime,
            ))
        })
    })
}

fn initial_message(model: &Model, runtime: &RequestRuntime) -> AssistantMessage {
    AssistantMessage {
        role: Default::default(),
        content: vec![],
        api: model.api.clone(),
        provider: model.provider.clone(),
        model: model.id.clone(),
        response_model: None,
        response_id: None,
        provider_thinking_level: None,
        thinking_level: None,
        duration_ms: None,
        diagnostics: None,
        usage: Usage::zero(),
        stop_reason: StopReason::Pending,
        deferred: None,
        error_message: None,
        raw_stop_reason: None,
        end_turn: None,
        timestamp: SafeUnsigned::new(
            runtime
                .clock
                .now_unix_ms()
                .min(pi_ai_core::JS_SAFE_INTEGER_MAX),
        )
        .expect("saturated timestamp"),
    }
}

fn request(
    model: &Model,
    context: &TranscriptContext,
    options: &ProxyOptions,
) -> Result<HttpRequest, String> {
    let mut forwarded = serde_json::to_value(&options.stream_options)
        .map_err(|_| "Invalid proxy request options")?;
    forwarded
        .as_object_mut()
        .expect("options are an object")
        .retain(|key, _| {
            matches!(
                key.as_str(),
                "temperature"
                    | "samplingParams"
                    | "maxTokens"
                    | "reasoning"
                    | "cacheRetention"
                    | "sessionId"
                    | "metadata"
                    | "transport"
                    | "thinkingBudgets"
                    | "maxRetryDelayMs"
            )
        });
    if let Some(headers) = &options.headers {
        forwarded["headers"] =
            serde_json::to_value(headers).map_err(|_| "Invalid proxy request headers")?;
    }
    let body = serde_json::to_vec(&json!({"model":model,"context":context,"options":forwarded}))
        .map_err(|_| "Invalid proxy request body")?;
    let mut headers = HttpHeaders::new();
    headers
        .append(
            "Authorization",
            format!("Bearer {}", options.auth_token.expose_secret()),
        )
        .map_err(|_| "Invalid proxy authorization header")?;
    headers
        .append("Content-Type", "application/json")
        .expect("static header");
    Ok(HttpRequest {
        endpoint: RequestEndpoint::new_resolved(options.endpoint.expose_for_transport())
            .expect("validated proxy endpoint"),
        method: "POST".into(),
        headers,
        body,
        proxy: None,
    })
}

async fn wait_io<T>(
    mut operation: BoxFuture<'_, Result<T, TransportError>>,
    cancellation: &CancellationToken,
) -> Result<T, String> {
    let mut cancelled = Box::pin(cancellation.cancelled());
    poll_fn(|cx| {
        if cancelled.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err("Request aborted by user".into()));
        }
        let result = operation.as_mut().poll(cx);
        if cancellation.is_cancelled() {
            Poll::Ready(Err("Request aborted by user".into()))
        } else {
            result.map(|result| result.map_err(|error| error.to_string()))
        }
    })
    .await
}
struct Body(Box<dyn ResponseBody>);
impl Drop for Body {
    fn drop(&mut self) {
        self.0.close();
    }
}

async fn run_proxy(
    model: &Model,
    context: &TranscriptContext,
    options: &ProxyOptions,
    http: &dyn RawHttpClient,
    runtime: &RequestRuntime,
    producer: &AssistantStreamProducer,
    partial: &mut AssistantMessage,
) -> Result<(), String> {
    if runtime.cancellation.is_cancelled() {
        return Err("Request aborted by user".into());
    }
    let request = request(model, context, options)?;
    // Install response ownership inside the I/O future, before the post-poll
    // cancellation check can discard a concurrently acquired response.
    let (status, status_text, mut body) = wait_io(
        Box::pin(async {
            let response = http.send(&request, runtime).await?;
            Ok((response.status, response.status_text, Body(response.body)))
        }),
        &runtime.cancellation,
    )
    .await?;
    let limits = SseLimits::default();
    if !(200..300).contains(&status) {
        let fallback = format!("Proxy error: {status} {status_text}");
        let mut bytes = vec![];
        while let Ok(Some(chunk)) = wait_io(body.0.next(runtime), &runtime.cancellation).await {
            if chunk.len() > limits.max_buffer_bytes.saturating_sub(bytes.len()) {
                return Err(fallback);
            }
            bytes.extend_from_slice(&chunk);
        }
        let detail = serde_json::from_slice::<Value>(&bytes)
            .ok()
            .and_then(|value| {
                value
                    .get("error")
                    .and_then(Value::as_str)
                    .filter(|text| !text.is_empty())
                    .map(str::to_owned)
            });
        return Err(detail.map_or(fallback, |detail| format!("Proxy error: {detail}")));
    }
    if body.0.presence() == ResponseBodyPresence::Absent {
        return Err("Proxy response body is absent".into());
    }
    let mut lines = Lines::new(limits);
    let mut arguments = BTreeMap::new();
    loop {
        let chunk = wait_io(body.0.next(runtime), &runtime.cancellation).await?;
        let ended = chunk.is_none();
        if let Some(chunk) = chunk {
            lines.push(&chunk)?;
        }
        while let Some(line) = lines.next(ended)? {
            let Some(data) = line.strip_prefix("data: ") else {
                continue;
            };
            let data = data.trim_matches(ecma_whitespace);
            if data.is_empty() {
                continue;
            }
            let raw: Value = serde_json::from_str(data).map_err(|_| "Invalid proxy event JSON")?;
            if !raw.get("type").is_some_and(Value::is_string) {
                continue;
            }
            let event: ProxyEvent =
                serde_json::from_value(raw).map_err(|_| "Invalid proxy event fields")?;
            if let Some(event) = process(event, partial, &mut arguments)? {
                let terminal = matches!(
                    event,
                    AssistantMessageEvent::Done { .. } | AssistantMessageEvent::Error { .. }
                );
                if !terminal {
                    let _ = producer.publish_cancellation_snapshot(partial.clone());
                }
                // Core owns first-terminal arbitration, including cancellation.
                if producer.push(event).is_err() || terminal {
                    return Ok(());
                }
            }
        }
        if ended {
            return Err("Connection closed by proxy server before the response completed".into());
        }
    }
}

/// Proxy framing is individual LF-delimited data lines, not SSE multiline frame
/// merging. Holding undecoded bytes through LF preserves fragmented UTF-8 and
/// TextDecoder's replacement behavior, including final incomplete sequences.
struct Lines {
    bytes: Vec<u8>,
    limits: SseLimits,
    first: bool,
}
impl Lines {
    fn new(limits: SseLimits) -> Self {
        Self {
            bytes: vec![],
            limits,
            first: true,
        }
    }
    fn push(&mut self, chunk: &[u8]) -> Result<(), String> {
        if chunk.len()
            > self
                .limits
                .max_buffer_bytes
                .saturating_sub(self.bytes.len())
        {
            return Err("Proxy stream buffer limit exceeded".into());
        }
        self.bytes.extend_from_slice(chunk);
        Ok(())
    }
    fn next(&mut self, ended: bool) -> Result<Option<String>, String> {
        let (length, delimiter) = match self.bytes.iter().position(|byte| *byte == b'\n') {
            Some(index) => (index, 1),
            None if ended && !self.bytes.is_empty() => (self.bytes.len(), 0),
            None => {
                if self.bytes.len() > self.limits.max_frame_bytes {
                    return Err("Proxy data line limit exceeded".into());
                }
                return Ok(None);
            }
        };
        if length > self.limits.max_frame_bytes {
            return Err("Proxy data line limit exceeded".into());
        }
        let line = String::from_utf8_lossy(&self.bytes[..length]).into_owned();
        self.bytes.drain(..length + delimiter);
        let line = if self.first {
            self.first = false;
            line.strip_prefix('\u{feff}').unwrap_or(&line).to_owned()
        } else {
            line
        };
        Ok(Some(line))
    }
}
fn ecma_whitespace(c: char) -> bool {
    matches!(c,'\u{0009}'..='\u{000d}'|'\u{0020}'|'\u{00a0}'|'\u{1680}'|'\u{2000}'..='\u{200a}'|'\u{2028}'|'\u{2029}'|'\u{202f}'|'\u{205f}'|'\u{3000}'|'\u{feff}')
}

#[derive(Deserialize)]
#[serde(tag = "type")]
enum ProxyEvent {
    #[serde(rename = "start")]
    Start,
    #[serde(rename = "text_start")]
    TextStart {
        #[serde(rename = "contentIndex")]
        index: SafeUnsigned,
    },
    #[serde(rename = "text_delta")]
    TextDelta {
        #[serde(rename = "contentIndex")]
        index: SafeUnsigned,
        delta: String,
    },
    #[serde(rename = "text_end")]
    TextEnd {
        #[serde(rename = "contentIndex")]
        index: SafeUnsigned,
        #[serde(rename = "contentSignature")]
        signature: Option<String>,
    },
    #[serde(rename = "thinking_start")]
    ThinkingStart {
        #[serde(rename = "contentIndex")]
        index: SafeUnsigned,
    },
    #[serde(rename = "thinking_delta")]
    ThinkingDelta {
        #[serde(rename = "contentIndex")]
        index: SafeUnsigned,
        delta: String,
    },
    #[serde(rename = "thinking_end")]
    ThinkingEnd {
        #[serde(rename = "contentIndex")]
        index: SafeUnsigned,
        #[serde(rename = "contentSignature")]
        signature: Option<String>,
    },
    #[serde(rename = "toolcall_start")]
    ToolStart {
        #[serde(rename = "contentIndex")]
        index: SafeUnsigned,
        id: pi_ai_core::ToolCallId,
        #[serde(rename = "toolName")]
        name: pi_ai_core::ToolName,
    },
    #[serde(rename = "toolcall_delta")]
    ToolDelta {
        #[serde(rename = "contentIndex")]
        index: SafeUnsigned,
        delta: String,
    },
    #[serde(rename = "toolcall_end")]
    ToolEnd {
        #[serde(rename = "contentIndex")]
        index: SafeUnsigned,
        #[serde(rename = "toolCall")]
        call: ToolCall,
    },
    #[serde(rename = "done")]
    Done {
        reason: DoneReason,
        usage: Usage,
        #[serde(rename = "providerThinkingLevel")]
        thinking: Option<String>,
    },
    #[serde(rename = "error")]
    Error {
        reason: ErrorReason,
        usage: Usage,
        #[serde(rename = "errorMessage")]
        message: Option<String>,
        #[serde(rename = "providerThinkingLevel")]
        thinking: Option<String>,
    },
    #[serde(other)]
    Other,
}
fn index(value: SafeUnsigned) -> Result<usize, String> {
    usize::try_from(value.get()).map_err(|_| "Invalid proxy content index".into())
}
fn replace(
    partial: &mut AssistantMessage,
    index: usize,
    content: AssistantContent,
) -> Result<(), String> {
    if index == partial.content.len() {
        partial.content.push(content);
    } else if let Some(slot) = partial.content.get_mut(index) {
        *slot = content;
    } else {
        return Err("Proxy content index is not contiguous".into());
    }
    Ok(())
}
fn process(
    event: ProxyEvent,
    partial: &mut AssistantMessage,
    arguments: &mut BTreeMap<usize, String>,
) -> Result<Option<AssistantMessageEvent>, String> {
    let event = match event {
        ProxyEvent::Start => AssistantMessageEvent::Start {
            partial: Box::new(partial.clone()),
        },
        ProxyEvent::TextStart { index: i } => {
            replace(
                partial,
                index(i)?,
                AssistantContent::Text(TextContent {
                    content_type: Default::default(),
                    text: String::new(),
                    text_signature: None,
                }),
            )?;
            AssistantMessageEvent::TextStart {
                content_index: i,
                partial: Box::new(partial.clone()),
            }
        }
        ProxyEvent::TextDelta { index: i, delta } => {
            let Some(AssistantContent::Text(content)) = partial.content.get_mut(index(i)?) else {
                return Err("Received text_delta for non-text content".into());
            };
            content.text.push_str(&delta);
            AssistantMessageEvent::TextDelta {
                content_index: i,
                delta,
                partial: Box::new(partial.clone()),
            }
        }
        ProxyEvent::TextEnd {
            index: i,
            signature,
        } => {
            let Some(AssistantContent::Text(content)) = partial.content.get_mut(index(i)?) else {
                return Err("Received text_end for non-text content".into());
            };
            content.text_signature = signature;
            let text = content.text.clone();
            AssistantMessageEvent::TextEnd {
                content_index: i,
                content: text,
                partial: Box::new(partial.clone()),
            }
        }
        ProxyEvent::ThinkingStart { index: i } => {
            replace(
                partial,
                index(i)?,
                AssistantContent::Thinking(ThinkingContent {
                    content_type: Default::default(),
                    thinking: String::new(),
                    thinking_signature: None,
                    redacted: None,
                }),
            )?;
            AssistantMessageEvent::ThinkingStart {
                content_index: i,
                partial: Box::new(partial.clone()),
            }
        }
        ProxyEvent::ThinkingDelta { index: i, delta } => {
            let Some(AssistantContent::Thinking(content)) = partial.content.get_mut(index(i)?)
            else {
                return Err("Received thinking_delta for non-thinking content".into());
            };
            content.thinking.push_str(&delta);
            AssistantMessageEvent::ThinkingDelta {
                content_index: i,
                delta,
                partial: Box::new(partial.clone()),
            }
        }
        ProxyEvent::ThinkingEnd {
            index: i,
            signature,
        } => {
            let Some(AssistantContent::Thinking(content)) = partial.content.get_mut(index(i)?)
            else {
                return Err("Received thinking_end for non-thinking content".into());
            };
            content.thinking_signature = signature;
            let thinking = content.thinking.clone();
            AssistantMessageEvent::ThinkingEnd {
                content_index: i,
                content: thinking,
                partial: Box::new(partial.clone()),
            }
        }
        ProxyEvent::ToolStart { index: i, id, name } => {
            let slot = index(i)?;
            arguments.insert(slot, String::new());
            replace(
                partial,
                slot,
                AssistantContent::ToolCall(ToolCall {
                    content_type: Default::default(),
                    id,
                    name,
                    arguments: JsonObject::default(),
                    arguments_error: None,
                    thought_signature: None,
                    namespace: None,
                }),
            )?;
            AssistantMessageEvent::ToolCallStart {
                content_index: i,
                partial: Box::new(partial.clone()),
            }
        }
        ProxyEvent::ToolDelta { index: i, delta } => {
            let slot = index(i)?;
            let Some(AssistantContent::ToolCall(content)) = partial.content.get_mut(slot) else {
                return Err("Received toolcall_delta for non-toolCall content".into());
            };
            let raw = arguments.entry(slot).or_default();
            if delta.len()
                > SseLimits::default()
                    .max_frame_bytes
                    .saturating_sub(raw.len())
            {
                return Err("Proxy tool arguments limit exceeded".into());
            }
            raw.push_str(&delta);
            content.arguments = match parse_streaming_json(Some(raw)) {
                JsonValue::Object(args) => args,
                _ => JsonObject::default(),
            };
            AssistantMessageEvent::ToolCallDelta {
                content_index: i,
                delta,
                partial: Box::new(partial.clone()),
            }
        }
        ProxyEvent::ToolEnd { index: i, call } => {
            let slot = index(i)?;
            let Some(AssistantContent::ToolCall(content)) = partial.content.get_mut(slot) else {
                return Ok(None);
            };
            *content = call.clone();
            arguments.remove(&slot);
            AssistantMessageEvent::ToolCallEnd {
                content_index: i,
                tool_call: call,
                partial: Box::new(partial.clone()),
            }
        }
        ProxyEvent::Done {
            reason,
            usage,
            thinking,
        } => {
            partial.stop_reason = match reason {
                DoneReason::Stop => StopReason::Stop,
                DoneReason::Length => StopReason::Length,
                DoneReason::ToolUse => StopReason::ToolUse,
                DoneReason::Deferred => return Err("Unsupported proxy done reason".into()),
            };
            partial.usage = usage;
            if thinking.is_some() {
                partial.provider_thinking_level = thinking;
            }
            AssistantMessageEvent::Done {
                reason,
                message: Box::new(partial.clone()),
            }
        }
        ProxyEvent::Error {
            reason,
            usage,
            message,
            thinking,
        } => {
            partial.stop_reason = if reason == ErrorReason::Aborted {
                StopReason::Aborted
            } else {
                StopReason::Error
            };
            partial.usage = usage;
            partial.error_message = Some(
                message
                    .filter(|text| !text.is_empty())
                    .unwrap_or_else(|| "Proxy server returned an error".into()),
            );
            if thinking.is_some() {
                partial.provider_thinking_level = thinking;
            }
            AssistantMessageEvent::Error {
                reason,
                error: Box::new(partial.clone()),
            }
        }
        ProxyEvent::Other => return Ok(None),
    };
    Ok(Some(event))
}
