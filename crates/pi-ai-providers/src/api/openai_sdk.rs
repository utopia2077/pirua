//! Private OpenAI SDK-equivalent execution; no family registration or payload policy.
use super::openai_responses_shared::{
    OpenAIResponsesStreamOptions, ResponsesError, ResponsesErrorKind, ResponsesEventSource,
    process_responses_stream,
};
use super::openai_sdk_strings;
use super::runtime::{AdapterResponseHead, AdapterServices, cancellable};
use super::wire_json::stringify_wire_json;
use pi_ai_core::*;
use pi_ai_transport::{
    HttpRequest, HttpResponse, ProviderAttemptFailure, ProviderRetryError, ResponseBody,
    RetryProfile, SseDecoder, SseProfile, TimeoutProfile, TransportError, TransportErrorKind,
    read_body_limited,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::VecDeque;
use std::fmt;
use std::time::Duration;

#[derive(Clone, Copy)]
pub(super) enum Family {
    #[cfg(feature = "wire-openai-responses")]
    Standard,
}
impl Family {
    fn error_prefix(self) -> &'static str {
        match self {
            #[cfg(feature = "wire-openai-responses")]
            Self::Standard => "OpenAI API error",
        }
    }
    fn failure_text(self) -> &'static str {
        match self {
            #[cfg(feature = "wire-openai-responses")]
            Self::Standard => "OpenAI Responses request failed",
        }
    }
    fn diagnostic_type(self) -> &'static str {
        match self {
            #[cfg(feature = "wire-openai-responses")]
            Self::Standard => "openai_responses_failure",
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OpenAIResponsesReasoningSummary {
    Auto,
    Detailed,
    Concise,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OpenAIResponsesToolChoiceMode {
    None,
    Auto,
    Required,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OpenAIResponsesAllowedToolsMode {
    Auto,
    Required,
}

/// Selected hosted-tool selectors from OpenAI 7.19.0; image generation is excluded.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OpenAIResponsesHostedTool {
    FileSearch,
    WebSearchPreview,
    Computer,
    ComputerUsePreview,
    ComputerUse,
    #[serde(rename = "web_search_preview_2025_03_11")]
    WebSearchPreview20250311,
    CodeInterpreter,
    Mcp,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum OpenAIResponsesToolSelector {
    AllowedTools {
        mode: OpenAIResponsesAllowedToolsMode,
        tools: Vec<JsonObject>,
    },
    Function {
        name: String,
    },
    Custom {
        name: String,
    },
    Mcp {
        server_label: String,
        #[serde(default, skip_serializing_if = "Presence::is_missing")]
        name: Presence<String>,
    },
    ApplyPatch {},
    Shell {},
}

/// Strings and every SDK object alternative; explicit root null is invalid.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged, deny_unknown_fields)]
pub enum OpenAIResponsesToolChoice {
    Mode(OpenAIResponsesToolChoiceMode),
    Selector(OpenAIResponsesToolSelector),
    Hosted {
        #[serde(rename = "type")]
        tool_type: OpenAIResponsesHostedTool,
    },
}

fn sdk_attempt<'a>(
    http: &'a HttpRequest,
    request: &'a TextStreamRequest,
    services: &'a AdapterServices,
    output: &'a AssistantMessage,
    streaming: bool,
    family: Family,
) -> BoxFuture<'a, Result<(HttpResponse, Value), ProviderAttemptFailure<Failure>>> {
    Box::pin(async move {
        let runtime = &request.options.runtime;
        let mut response = services
            .transport
            .send_fetch(
                http,
                runtime,
                RetryProfile::None,
                TimeoutProfile::Headers(Duration::from_millis(
                    request
                        .options
                        .wire
                        .timeout_ms
                        .map(SafeUnsigned::get)
                        .unwrap_or(600_000),
                )),
            )
            .await
            .map_err(|error| {
                let kind = if error.is_custom_fetch_rejection()
                    && !matches!(
                        error.kind(),
                        TransportErrorKind::Cancelled | TransportErrorKind::Timeout
                    ) {
                    TransportErrorKind::Connection
                } else {
                    error.kind()
                };
                let error = Failure::Transport(kind);
                if matches!(
                    kind,
                    TransportErrorKind::Connection | TransportErrorKind::Timeout
                ) {
                    ProviderAttemptFailure::Provider {
                        status: None,
                        headers: None,
                        error,
                    }
                } else {
                    ProviderAttemptFailure::Other(error)
                }
            })?;
        if (200..300).contains(&response.status) {
            let buffered = if streaming {
                Value::Null
            } else {
                sdk_decode_response(&mut response, request, services)
                    .await
                    .map_err(ProviderAttemptFailure::Other)?
            };
            return Ok((response, buffered));
        }
        // The SDK catches response.text() rejection and uses its message as the
        // HTTP error body. Native typed failures use reviewed lexical equivalents.
        let bytes = match read_body_limited(
            response.body.as_mut(),
            runtime,
            services.max_error_body_bytes,
        )
        .await
        {
            Ok(bytes) => bytes,
            Err(error)
                if error.kind() == TransportErrorKind::Connection
                    || error.kind() == TransportErrorKind::Closed =>
            {
                b"terminated".to_vec()
            }
            Err(error) if error.kind() == TransportErrorKind::Timeout => {
                b"Request timed out.".to_vec()
            }
            Err(error) => {
                return Err(ProviderAttemptFailure::Other(Failure::Transport(
                    error.kind(),
                )));
            }
        };
        Err(ProviderAttemptFailure::Provider {
            status: Some(response.status),
            headers: Some(response.headers.clone()),
            error: Failure::Http(
                response.status,
                classify_sdk_error_for_family(Some(response.status), &bytes, output, family),
            ),
        })
    })
}

/// The non-streaming default parser and Responses.create's _thenUnwrap both
/// finish before withResponse resolves. Decode failures are not provider errors
/// and must not retry or invoke response middleware.
async fn sdk_decode_response(
    response: &mut HttpResponse,
    request: &TextStreamRequest,
    services: &AdapterServices,
) -> Result<Value, Failure> {
    let content_type = response
        .headers
        .get_for_fetch("content-type")
        .unwrap_or_default();
    let media = openai_sdk_strings::trim(content_type.split(';').next().unwrap_or_default());
    let json = media.contains("application/json") || media.ends_with("+json");
    // defaultParseResponse returns null/undefined here without reading the body.
    if response.status == 204
        || (json && response.headers.get_for_fetch("content-length").as_deref() == Some("0"))
    {
        return Err(Failure::Setup("invalid_response"));
    }
    let bytes = read_body_limited(
        response.body.as_mut(),
        &request.options.runtime,
        services.max_error_body_bytes,
    )
    .await?;
    let decoded = String::from_utf8_lossy(&bytes);
    let decoded = decoded.strip_prefix('\u{feff}').unwrap_or(&decoded);
    let mut value = if json {
        serde_json::from_str(decoded).map_err(|_| Failure::Setup("invalid_response"))?
    } else {
        Value::String(decoded.to_owned())
    };
    if !value.is_object() && !value.is_array() {
        // The SDK's `in` guard echoes this finite decoded primitive, not the
        // entire response body. Retain its prefix: it defeats anchored overflow
        // and veto patterns that classifying the bare operand would preserve.
        // This text is classification-only and never enters public diagnostics.
        let operand = match &value {
            Value::String(text) => text.clone(),
            _ => stringify_wire_json(&value).map_err(|_| Failure::Setup("invalid_response"))?,
        };
        let mut probe = empty_message(request);
        probe.stop_reason = StopReason::Error;
        probe.error_message = Some(format!(
            "Cannot use 'in' operator to search for 'object' in {operand}"
        ));
        let mut classified = classify_message(&probe);
        classified.kind = ResponsesErrorKind::InvalidWire;
        return Err(Failure::Responses(classified));
    }
    sdk_unwrap_response(&mut value)?;
    Ok(value)
}

fn sdk_unwrap_response(value: &mut Value) -> Result<(), Failure> {
    // `'object' in rsp` throws for every primitive, including null. JSON cannot
    // supply inherited object/iterator properties: __proto__ is an own data key.
    if !value.is_object() && !value.is_array() {
        return Err(Failure::Setup("invalid_response"));
    }
    if value.get("object").and_then(Value::as_str) != Some("response") {
        return Ok(());
    }
    let mut text = String::new();
    match value.get("output") {
        // Strings are iterable, but their characters have no message type.
        Some(Value::String(_)) => {}
        Some(Value::Array(outputs)) => {
            for output in outputs {
                if output.is_null() {
                    return Err(Failure::Setup("invalid_response"));
                }
                if output.get("type").and_then(Value::as_str) != Some("message") {
                    continue;
                }
                match output.get("content") {
                    Some(Value::String(_)) => {}
                    Some(Value::Array(contents)) => {
                        for content in contents {
                            if content.is_null() {
                                return Err(Failure::Setup("invalid_response"));
                            }
                            if content.get("type").and_then(Value::as_str) == Some("output_text") {
                                sdk_join_text(content.get("text"), &mut text)?;
                            }
                        }
                    }
                    _ => return Err(Failure::Setup("invalid_response")),
                }
            }
        }
        _ => return Err(Failure::Setup("invalid_response")),
    }
    // addOutputText overwrites an existing own output_text, including null.
    value["output_text"] = Value::String(text);
    Ok(())
}

/// Array.join's conversion over the finite JSON domain used by addOutputText.
/// A JSON object's own non-callable toString shadows Object.prototype.toString
/// and makes conversion throw; arbitrary executable coercion hooks are absent.
fn sdk_join_text(value: Option<&Value>, text: &mut String) -> Result<(), Failure> {
    match value {
        None | Some(Value::Null) => {}
        Some(Value::String(value)) => text.push_str(value),
        Some(Value::Array(values)) => {
            for (index, value) in values.iter().enumerate() {
                if index != 0 {
                    text.push(',');
                }
                sdk_join_text(Some(value), text)?;
            }
        }
        Some(Value::Object(value)) => {
            if value.contains_key("toString") {
                return Err(Failure::Setup("invalid_response"));
            }
            text.push_str("[object Object]");
        }
        Some(value) => text
            .push_str(&stringify_wire_json(value).map_err(|_| Failure::Setup("invalid_response"))?),
    }
    Ok(())
}

fn retry_failure(error: ProviderRetryError<Failure>) -> Failure {
    match error {
        ProviderRetryError::Attempt(
            ProviderAttemptFailure::Provider { error, .. } | ProviderAttemptFailure::Other(error),
        ) => error,
        ProviderRetryError::Cancelled { .. } => Failure::Transport(TransportErrorKind::Cancelled),
        ProviderRetryError::Policy { error, previous } => {
            let kind = error.kind();
            match previous {
                Some(ProviderAttemptFailure::Provider {
                    error: Failure::Http(status, classified),
                    ..
                }) => Failure::RetryPolicy(
                    kind,
                    Some(status),
                    if kind == TransportErrorKind::RetryDelayExceeded {
                        classified.retry_cap
                    } else {
                        classified.normal
                    },
                ),
                Some(
                    ProviderAttemptFailure::Provider { error, .. }
                    | ProviderAttemptFailure::Other(error),
                ) => error,
                None => Failure::Transport(kind),
            }
        }
    }
}

pub(super) struct Execution<'a> {
    pub streaming: bool,
    pub max_stream_bytes: usize,
    pub stream_options: OpenAIResponsesStreamOptions<'a>,
    pub family: Family,
}

pub(super) async fn execute(
    http: &HttpRequest,
    request: &TextStreamRequest,
    services: &AdapterServices,
    execution: Execution<'_>,
    producer: &AssistantStreamProducer,
    output: &mut AssistantMessage,
) -> Result<(), Failure> {
    let Execution {
        streaming,
        max_stream_bytes,
        stream_options,
        family,
    } = execution;
    let runtime = &request.options.runtime;
    let (mut response, buffered) = services
        .transport
        .retry_provider_request(
            runtime,
            RetryProfile::SharedProvider {
                max_retries: request
                    .options
                    .wire
                    .max_retries
                    .map(SafeUnsigned::get)
                    .unwrap_or(0),
                max_retry_delay_ms: request
                    .options
                    .wire
                    .max_retry_delay_ms
                    .map(SafeUnsigned::get),
            },
            || sdk_attempt(http, request, services, output, streaming, family),
        )
        .await
        .map_err(retry_failure)?;
    if let Some(middleware) = &services.response {
        let head = AdapterResponseHead::from_http(response.status, &response.headers)?;
        cancellable(
            middleware.on_response(request, &head),
            &runtime.cancellation,
        )
        .await
        .map_err(|_| Failure::Transport(TransportErrorKind::Cancelled))?
        .map_err(|_| Failure::Setup("response_middleware"))?;
        super::request_lifecycle::record(services, &head);
    } else {
        super::request_lifecycle::observe_only(services, response.status, &response.headers);
    }
    producer
        .publish_cancellation_snapshot(output.clone())
        .map_err(|_| Failure::Setup("producer"))?;
    producer
        .push(AssistantMessageEvent::Start {
            partial: Box::new(output.clone()),
        })
        .map_err(|_| Failure::Setup("producer"))?;
    let mut source: Box<dyn ResponsesEventSource + '_> = if !streaming {
        // Payload replacement may disable SDK streaming. JSON arrays remain
        // iterable; ordinary JSON response objects fail at reducer entry.
        match buffered {
            Value::Array(events) => Box::new(BufferedEvents(events.into())),
            _ => return Err(Failure::Setup("non_streaming_response")),
        }
    } else {
        if matches!(response.status, 204 | 205)
            || response.body.presence() == pi_ai_transport::ResponseBodyPresence::Absent
        {
            response.body.close();
            return Err(Failure::Setup("no_body"));
        }
        Box::new(SseSource {
            body: response.body,
            runtime,
            decoder: SseDecoder::new(SseProfile::OpenAI, services.sse_limits),
            ended: false,
            done: false,
            bytes: 0,
            max_bytes: max_stream_bytes,
            template: output.clone(),
            family,
        })
    };
    process_responses_stream(
        source.as_mut(),
        output,
        producer,
        &request.resolved.model,
        &stream_options,
        &runtime.cancellation,
    )
    .await?;
    if runtime.cancellation.is_cancelled() {
        return Err(Failure::Transport(TransportErrorKind::Cancelled));
    }
    let reason = match output.stop_reason {
        StopReason::Stop => DoneReason::Stop,
        StopReason::Length => DoneReason::Length,
        StopReason::ToolUse => DoneReason::ToolUse,
        _ => return Err(Failure::Responses(classify_message(output))),
    };
    producer
        .push(AssistantMessageEvent::Done {
            reason,
            message: Box::new(output.clone()),
        })
        .map_err(|_| Failure::Setup("producer"))?;
    Ok(())
}
fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(v) => *v,
        Value::Number(v) => v.as_f64() != Some(0.0),
        Value::String(v) => !v.is_empty(),
        _ => true,
    }
}

struct SseSource<'a> {
    body: Box<dyn ResponseBody>,
    runtime: &'a RequestRuntime,
    decoder: SseDecoder,
    ended: bool,
    done: bool,
    bytes: usize,
    max_bytes: usize,
    template: AssistantMessage,
    family: Family,
}
struct BufferedEvents(VecDeque<Value>);
impl ResponsesEventSource for BufferedEvents {
    fn next(&mut self) -> BoxFuture<'_, Result<Option<Value>, ResponsesError>> {
        Box::pin(async { Ok(self.0.pop_front()) })
    }
}
impl ResponsesEventSource for SseSource<'_> {
    fn next(&mut self) -> BoxFuture<'_, Result<Option<Value>, ResponsesError>> {
        Box::pin(async move {
            loop {
                while let Some(frame) = self.decoder.next_frame().map_err(source_transport_error)? {
                    if self.done {
                        continue;
                    }
                    if frame.data.starts_with("[DONE]") {
                        self.done = true;
                        continue;
                    }
                    let data: Value = serde_json::from_str(&frame.data)
                        .map_err(|_| ResponsesError::new(ResponsesErrorKind::InvalidWire))?;
                    if frame
                        .event
                        .as_deref()
                        .is_some_and(|v| v.starts_with("thread."))
                    {
                        return Ok(Some(
                            json!({"event":frame.event,"data":data,"type":"sdk.thread"}),
                        ));
                    }
                    if data.get("error").is_some_and(truthy) {
                        let bytes = serde_json::to_vec(&json!({"error":data["error"]}))
                            .map_err(|_| ResponsesError::new(ResponsesErrorKind::InvalidWire))?;
                        return Err(classify_sdk_error_for_family(
                            None,
                            &bytes,
                            &self.template,
                            self.family,
                        )
                        .normal);
                    }
                    return Ok(Some(data));
                }
                if self.ended {
                    return Ok(None);
                }
                match self
                    .body
                    .next(self.runtime)
                    .await
                    .map_err(source_transport_error)?
                {
                    Some(chunk) => {
                        self.bytes = self
                            .bytes
                            .checked_add(chunk.len())
                            .filter(|v| *v <= self.max_bytes)
                            .ok_or(ResponsesError::new(ResponsesErrorKind::ResourceLimit))?;
                        self.decoder.push(&chunk).map_err(source_transport_error)?;
                    }
                    None => {
                        self.ended = true;
                        self.decoder.end_input();
                    }
                }
            }
        })
    }
}
impl Drop for SseSource<'_> {
    fn drop(&mut self) {
        self.body.close();
    }
}
fn source_transport_error(error: TransportError) -> ResponsesError {
    match error.kind() {
        TransportErrorKind::Cancelled => ResponsesError::new(ResponsesErrorKind::Cancelled),
        TransportErrorKind::LimitExceeded => ResponsesError::new(ResponsesErrorKind::ResourceLimit),
        TransportErrorKind::Connection
        | TransportErrorKind::Timeout
        | TransportErrorKind::RetryDelayExceeded => ResponsesError {
            kind: ResponsesErrorKind::Provider,
            retryable: true,
            context_overflow: false,
        },
        _ => ResponsesError::new(ResponsesErrorKind::InvalidWire),
    }
}
pub(super) fn empty_message(request: &TextStreamRequest) -> AssistantMessage {
    AssistantMessage {
        role: AssistantRole::Assistant,
        content: Vec::new(),
        api: request.resolved.model.api.clone(),
        provider: request.resolved.model.provider.clone(),
        model: request.resolved.model.id.clone(),
        response_model: None,
        provider_thinking_level: None,
        thinking_level: None,
        duration_ms: None,
        response_id: None,
        diagnostics: None,
        usage: Usage::zero(),
        stop_reason: StopReason::Pending,
        deferred: None,
        error_message: None,
        raw_stop_reason: None,
        end_turn: None,
        timestamp: SafeUnsigned::new(request.options.runtime.clock.now_unix_ms())
            .unwrap_or(SafeUnsigned::MAX),
    }
}
fn classify_message(message: &AssistantMessage) -> ResponsesError {
    ResponsesError {
        kind: ResponsesErrorKind::Provider,
        retryable: is_retryable_assistant_error(message),
        context_overflow: is_context_overflow(message, None),
    }
}
pub(super) struct SdkErrorClassification {
    normal: ResponsesError,
    retry_cap: ResponsesError,
}

fn classify_sdk_error_for_family(
    status: Option<u16>,
    bytes: &[u8],
    template: &AssistantMessage,
    family: Family,
) -> SdkErrorClassification {
    let raw = String::from_utf8_lossy(bytes);
    let raw = raw.strip_prefix('\u{feff}').unwrap_or(&raw);
    let parsed = serde_json::from_str::<Value>(raw).ok();
    let error = parsed.as_ref().and_then(|v| v.get("error"));
    let error_text = error.filter(|v| truthy(v)).map(|v| {
        v.get("message")
            .filter(|v| truthy(v))
            .map(|message| {
                message
                    .as_str()
                    .map(str::to_owned)
                    .unwrap_or_else(|| stringify_wire_json(message).unwrap_or_default())
            })
            .unwrap_or_else(|| stringify_wire_json(v).unwrap_or_default())
    });
    // SDK passes text for failed parsing and falsy JSON. Truthy non-error JSON
    // has no body, which matters for Cerebras's status/no-body overflow rule.
    let msg = error_text
        .or_else(|| {
            parsed
                .as_ref()
                .is_none_or(|value| !truthy(value))
                .then(|| raw.to_owned())
        })
        .filter(|v| !v.is_empty());
    let mut text = match (status, msg) {
        (Some(status), Some(msg)) => format!("{status} {msg}"),
        (Some(status), None) => format!("{status} status code (no body)"),
        (None, Some(msg)) => msg,
        (None, None) => "(no status code or body)".to_owned(),
    };
    let mut probe = template.clone();
    probe.stop_reason = StopReason::Error;
    // A reviewed lexical projection of the fixed retry-cap prefix; classify
    // the original SDK message before body normalization/truncation discards it.
    probe.error_message = Some(format!("Server requested retry delay. {text}"));
    let retry_cap = classify_message(&probe);
    if let Some(error) = error.filter(|v| v.as_object().is_some_and(|v| !v.is_empty())) {
        let body = stringify_wire_json(error).unwrap_or_default();
        let body = body.trim_matches(ecma_whitespace);
        let length = body.encode_utf16().count();
        let short = if length > 4000 {
            format!(
                "{}... [truncated {} chars]",
                String::from_utf16_lossy(&body.encode_utf16().take(4000).collect::<Vec<_>>()),
                length - 4000
            )
        } else {
            body.to_owned()
        };
        if !text.contains(&short) && status.is_some() {
            text = short;
        }
    }
    if let Some(status) = status {
        text = format!("{} ({status}): {text}", family.error_prefix());
    }
    probe.error_message = Some(text);
    SdkErrorClassification {
        normal: classify_message(&probe),
        retry_cap,
    }
}

pub(super) enum Failure {
    Setup(&'static str),
    Transport(TransportErrorKind),
    Responses(ResponsesError),
    Http(u16, SdkErrorClassification),
    RetryPolicy(TransportErrorKind, Option<u16>, ResponsesError),
}
impl From<TransportError> for Failure {
    fn from(error: TransportError) -> Self {
        Self::Transport(error.kind())
    }
}
impl From<ResponsesError> for Failure {
    fn from(error: ResponsesError) -> Self {
        Self::Responses(error)
    }
}
pub(super) fn publish_failure(
    request: &TextStreamRequest,
    producer: &AssistantStreamProducer,
    mut output: AssistantMessage,
    error: Failure,
    family: Family,
) {
    let aborted = request.options.runtime.cancellation.is_cancelled();
    let (kind, status, classified) = match error {
        Failure::Setup(kind) => (
            kind.to_owned(),
            None,
            ResponsesError::new(ResponsesErrorKind::Conversion),
        ),
        Failure::Transport(kind) => (
            format!("transport_{kind:?}"),
            None,
            source_transport_error(TransportError::new(kind)),
        ),
        Failure::Responses(mut error) => {
            // The pinned missing-terminal message is itself retryable.
            if error.kind == ResponsesErrorKind::MissingTerminal {
                error.retryable = true;
            }
            (format!("responses_{:?}", error.kind), None, error)
        }
        Failure::Http(status, error) => ("http".to_owned(), Some(status), error.normal),
        Failure::RetryPolicy(kind, status, error) => {
            (format!("retry_policy_{kind:?}"), status, error)
        }
    };
    output.stop_reason = if aborted {
        StopReason::Aborted
    } else {
        StopReason::Error
    };
    output.error_message = Some(
        if aborted {
            pi_ai_core::REQUEST_CANCELLED_MESSAGE
        } else {
            match (classified.context_overflow, classified.retryable) {
                (true, true) => "prompt is too long; server error",
                (true, false) => "prompt is too long",
                (false, true) => "server error",
                _ => family.failure_text(),
            }
        }
        .to_owned(),
    );
    if !aborted {
        let details = serde_json::from_value(json!({"kind":kind,"status":status,"retryable":classified.retryable,"contextOverflow":classified.context_overflow})).expect("safe diagnostic");
        output.diagnostics = Some(vec![AssistantMessageDiagnostic {
            diagnostic_type: family
                .diagnostic_type()
                .parse()
                .expect("constant diagnostic"),
            timestamp: output.timestamp,
            error: None,
            details: Some(details),
        }]);
    }
    let _ = producer.push(AssistantMessageEvent::Error {
        reason: if aborted {
            ErrorReason::Aborted
        } else {
            ErrorReason::Error
        },
        error: Box::new(output),
    });
}

impl fmt::Debug for Failure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OpenAIResponsesFailure([REDACTED])")
    }
}

fn ecma_whitespace(c: char) -> bool {
    matches!(c, '\u{9}'..='\u{d}' | '\u{20}' | '\u{a0}' | '\u{1680}' | '\u{2000}'..='\u{200a}' | '\u{2028}' | '\u{2029}' | '\u{202f}' | '\u{205f}' | '\u{3000}' | '\u{feff}')
}
#[cfg(all(test, feature = "wire-openai-responses"))]
fn classify_sdk_error(
    status: Option<u16>,
    bytes: &[u8],
    template: &AssistantMessage,
) -> SdkErrorClassification {
    classify_sdk_error_for_family(status, bytes, template, Family::Standard)
}
#[cfg(all(test, feature = "wire-openai-responses"))]
mod cap_prefix_tests {
    use super::*;
    #[test]
    fn sdk_decode_stage_matches_installed_sdk_output_text_processing() {
        let corpus: Value = serde_json::from_str(include_str!(
            "../../../../conformance/provider-traces/openai-responses/decode-stage.json"
        ))
        .unwrap();
        for case in corpus["cases"].as_array().unwrap() {
            let mut value: Value = if case["contentType"] == "text/plain" {
                case["body"].clone()
            } else {
                serde_json::from_str(case["body"].as_str().unwrap()).unwrap()
            };
            let result = sdk_unwrap_response(&mut value);
            assert_eq!(
                result.is_ok(),
                case["decoded"]["ok"].as_bool().unwrap(),
                "{}",
                case["id"]
            );
            if result.is_ok() {
                assert_eq!(
                    value.get("output_text"),
                    case["decoded"]["data"].get("output_text"),
                    "{}",
                    case["id"]
                );
            }
        }
    }
    #[test]
    fn fixed_classifier_prefix_matches_actual_sdk_cap_prefix_for_anchors_and_vetoes() {
        let fixture: Value = serde_json::from_str(include_str!(
            "../../../../conformance/provider-traces/openai-responses/retry-cap.json"
        ))
        .unwrap();
        let trace: Value = serde_json::from_str(include_str!(
            "../../../../conformance/provider-traces/openai-responses/trace.json"
        ))
        .unwrap();
        let template: AssistantMessage =
            serde_json::from_value(trace["cases"][0]["events"][0]["partial"].clone()).unwrap();
        for case in fixture["prefixEquivalenceCases"].as_array().unwrap() {
            let bytes =
                serde_json::to_vec(&json!({"error":{"message":case["sdkMessage"]}})).unwrap();
            let classified = classify_sdk_error(None, &bytes, &template);
            assert_eq!(
                (
                    classified.normal.context_overflow,
                    classified.normal.retryable
                ),
                (
                    case["original"]["contextOverflow"].as_bool().unwrap(),
                    case["original"]["retryable"].as_bool().unwrap()
                )
            );
            assert_eq!(
                (
                    classified.retry_cap.context_overflow,
                    classified.retry_cap.retryable
                ),
                (
                    case["contextOverflow"].as_bool().unwrap(),
                    case["retryable"].as_bool().unwrap()
                )
            );
        }
    }
}
